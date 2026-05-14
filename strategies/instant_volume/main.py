#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安即时刷量策略 (移植自 OKX 同名策略, 架构 1:1 对齐)

策略逻辑: 卖一价限价买入 → 买一价限价卖出 → 达到 max_volume 后清仓退出
增强: 限价单 + 超时撤单 / 部分成交 / executionReport 驱动 / REST 余额兜底 / 热加载 / 退出清仓
SDK: binance-sdk-spot (REST + WS API + WS Streams 全官方)
认证: Ed25519 (.env)
"""
import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger

# 使项目根目录可 import strategies.common
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from strategies.common import load_credentials, make_spot_client
from strategies.common.ws_errors import parse_ws_error


class PositionStatus(Enum):
    OPENING = "opening"   # 买单已下，等待成交
    OPEN = "open"         # 买单已成交，持仓中
    CLOSING = "closing"   # 卖单已下，等待成交
    CLOSED = "closed"


@dataclass
class InstantPosition:
    buy_cl_ord_id: str
    symbol: str
    quantity: str
    buy_price: str = ""
    buy_order_id: int = 0
    sell_cl_ord_id: str = ""
    sell_order_id: int = 0
    filled_size: str = "0"
    sell_filled_size: str = "0"
    buy_time: Optional[datetime] = None
    sell_time: Optional[datetime] = None
    pnl: Decimal = Decimal("0")
    status: PositionStatus = PositionStatus.OPENING
    last_update_time: int = 0


@dataclass
class StrategyConfig:
    strategy_id: str
    symbol: str
    quantity: str
    order_interval: float
    max_volume: float
    order_timeout: int = 30
    max_spread: float = 0  # BBO 价差上限 (绝对值, USDT), 0=不限


class InstantVolumeStrategy:
    def __init__(self, config: StrategyConfig, config_path: str = None):
        self.config = config
        self.config_path = Path(config_path) if config_path else None
        self._last_config_mtime = (
            self.config_path.stat().st_mtime if self.config_path and self.config_path.exists() else 0
        )
        creds = load_credentials()
        self.client = make_spot_client(creds)

        # 市场数据
        self.latest_bid_price = Decimal("0")
        self.latest_ask_price = Decimal("0")
        self.latest_bid_size = Decimal("0")
        self.latest_ask_size = Decimal("0")
        self.last_price_update: Optional[datetime] = None
        self.max_spread_d = Decimal(str(config.max_spread)) if config.max_spread else Decimal("0")

        # 交易规则
        self.tick_size = Decimal("0.01")
        self.lot_size = Decimal("0.001")
        self.min_size = Decimal("0")
        self.min_notional = Decimal("0")
        self.base_currency = ""
        self.quote_currency = ""

        # 持仓
        self.current_position: Optional[InstantPosition] = None
        self.position_lock = asyncio.Lock()

        # 统计
        self.total_buy_volume = Decimal("0")
        self.total_sell_volume = Decimal("0")
        self.total_pnl = Decimal("0")
        self.total_fee_base = Decimal("0")
        self.total_fee_quote = Decimal("0")
        self.order_count = 0

        # SDK 连接句柄
        self.ws_api_conn = None
        self.ws_streams_conn = None

        # 控制
        self.order_id_counter = 0
        self.should_stop = False
        self._tasks: list[asyncio.Task] = []

    # ─── 工具函数 ─────────────────────────────────────────────

    def _generate_order_id(self, prefix: str = "b") -> str:
        self.order_id_counter += 1
        timestamp = int(datetime.now().timestamp() * 1000) % 100000000
        return f"{prefix}{timestamp}{self.order_id_counter % 100:02d}"

    def _adjust_price(self, price: Decimal) -> str:
        if self.tick_size <= 0:
            return str(price)
        return str((price / self.tick_size).quantize(Decimal("1")) * self.tick_size)

    def _adjust_size(self, size: Decimal) -> str:
        """向下对齐 lot_size, 永不超出原数量 (避免卖单 > 实际持仓)"""
        if self.lot_size <= 0:
            return str(size)
        return str((size / self.lot_size).quantize(Decimal("1"), rounding=ROUND_DOWN) * self.lot_size)

    def _safe_decimal(self, value) -> Decimal:
        if isinstance(value, Decimal):
            return value
        if value is None or value == "":
            return Decimal("0")
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _accumulate_fee(self, fee: Decimal, fee_ccy: str):
        if fee <= 0 or not fee_ccy:
            return
        if fee_ccy.upper() in ("USDT", "USDC", "USD", "FDUSD", "TUSD", "BUSD"):
            self.total_fee_quote += fee
        else:
            self.total_fee_base += fee

    # ─── 状态文件 ─────────────────────────────────────────────

    @staticmethod
    def _state_file() -> Path:
        """volume_state.json 位于项目根目录 (与 OKX / tools/stats_summary.py 对齐)"""
        return Path(__file__).resolve().parents[2] / "volume_state.json"

    async def _load_state(self):
        state_file = self._state_file()
        if not state_file.exists():
            return
        try:
            all_data = json.loads(state_file.read_text())
            state = all_data.get(self.config.symbol, {})
            self.total_buy_volume = Decimal(str(state.get("global_buy_volume", 0)))
            self.total_sell_volume = Decimal(str(state.get("global_sell_volume", 0)))
            self.total_pnl = Decimal(str(state.get("global_pnl", 0)))
            self.total_fee_base = Decimal(str(state.get("global_fee_base", 0)))
            self.total_fee_quote = Decimal(str(state.get("global_fee_quote", 0)))
            self.order_count = state.get("order_count", 0)
            logger.info(
                f"✅ 加载状态 [{self.config.symbol}] | "
                f"买入:{self.total_buy_volume} 卖出:{self.total_sell_volume} | "
                f"手续费 base={self.total_fee_base} quote={self.total_fee_quote}"
            )
        except Exception as e:
            logger.warning(f"⚠️ 加载状态失败: {e}")

    async def _save_state(self):
        state_file = self._state_file()
        try:
            all_data = json.loads(state_file.read_text()) if state_file.exists() else {}
            all_data[self.config.symbol] = {
                "global_buy_volume": str(self.total_buy_volume),
                "global_sell_volume": str(self.total_sell_volume),
                "global_pnl": str(self.total_pnl),
                "global_fee_base": str(self.total_fee_base),
                "global_fee_quote": str(self.total_fee_quote),
                "order_count": self.order_count,
                "updated_at": datetime.now().isoformat(),
            }
            state_file.write_text(json.dumps(all_data, indent=2))
        except Exception as e:
            logger.error(f"❌ 保存状态失败: {e}")

    # ─── 交易规则 ─────────────────────────────────────────────

    async def _get_instrument_info(self):
        resp = await asyncio.to_thread(self.client.rest_api.exchange_info, symbol=self.config.symbol)
        data = resp.data()
        if not data.symbols:
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")
        sym = data.symbols[0]
        self.base_currency = sym.base_asset
        self.quote_currency = getattr(sym, "quote_asset", "") or getattr(sym, "quoteAsset", "") or ""
        for f in sym.filters:
            ft = getattr(f, "filter_type", None) or getattr(f, "filterType", None)
            if ft == "PRICE_FILTER":
                self.tick_size = Decimal(str(f.tick_size))
            elif ft == "LOT_SIZE":
                self.lot_size = Decimal(str(f.step_size))
                min_q = getattr(f, "min_qty", None) or getattr(f, "minQty", None)
                if min_q:
                    self.min_size = Decimal(str(min_q))
            elif ft == "NOTIONAL":
                mn = getattr(f, "min_notional", None) or getattr(f, "minNotional", None)
                if mn:
                    self.min_notional = Decimal(str(mn))
        qty = self._safe_decimal(self.config.quantity)
        if self.min_size > 0 and qty < self.min_size:
            raise Exception(f"quantity={qty} < min_size={self.min_size}, 请提高 quantity 或换交易对")
        logger.info(
            f"✅ 交易规则 | tick={self.tick_size} lot={self.lot_size} min={self.min_size} | "
            f"base={self.base_currency} quote={self.quote_currency} | minNotional={self.min_notional}"
        )

    # ─── WS 初始化 ─────────────────────────────────────────────

    async def _init_ws_api(self):
        """建立 WS API 连接并 session_logon (User Data 共用此会话签名)"""
        self.ws_api_conn = await self.client.websocket_api.create_connection()
        await self.ws_api_conn.session_logon()
        logger.info("✅ WS API 已连接并认证")

    async def _init_ws_streams(self):
        """订阅 bookTicker (含 BBO 量)"""
        sym = self.config.symbol.lower()
        self.ws_streams_conn = await self.client.websocket_streams.create_connection()
        h = await self.client.websocket_streams.book_ticker(symbol=sym)
        h.on("message", self._on_book_ticker)
        logger.info(f"✅ WS Streams 已订阅 | {self.config.symbol}@bookTicker")

    async def _init_user_data_stream(self):
        """通过 WS API session_logon + 签名订阅 User Data Stream"""
        result = await self.ws_api_conn.user_data_stream_subscribe_signature()
        result.stream.on("message", self._on_user_data)
        logger.info("✅ User Data Stream 已订阅 (WS API session)")

    async def _init_ws(self):
        await self._init_ws_api()
        await self._init_ws_streams()
        await self._init_user_data_stream()

    # ─── WS 回调 ─────────────────────────────────────────────

    def _on_book_ticker(self, msg):
        """bookTicker 回调: b=bidPx B=bidQty a=askPx A=askQty"""
        try:
            b = getattr(msg, "b", None) or (msg.get("b") if isinstance(msg, dict) else None)
            a = getattr(msg, "a", None) or (msg.get("a") if isinstance(msg, dict) else None)
            bid_sz = getattr(msg, "B", None) or (msg.get("B") if isinstance(msg, dict) else None)
            ask_sz = getattr(msg, "A", None) or (msg.get("A") if isinstance(msg, dict) else None)
            if b and a:
                self.latest_bid_price = self._safe_decimal(b)
                self.latest_ask_price = self._safe_decimal(a)
                if bid_sz is not None:
                    self.latest_bid_size = self._safe_decimal(bid_sz)
                if ask_sz is not None:
                    self.latest_ask_size = self._safe_decimal(ask_sz)
                self.last_price_update = datetime.now()
        except Exception as e:
            logger.debug(f"解析 bookTicker 失败: {e}")

    def _on_user_data(self, event):
        """User Data Stream 回调 → executionReport"""
        try:
            from binance_sdk_spot.websocket_api.models import ExecutionReport
            actual = getattr(event, "actual_instance", event)
            if isinstance(actual, ExecutionReport):
                asyncio.create_task(self._handle_execution_report(actual))
        except Exception as e:
            logger.error(f"处理 UserData 回调失败: {e}")

    # ─── executionReport 路由 ──────────────────────────────────

    async def _handle_execution_report(self, msg):
        """processes executionReport - 字段说明:
        s: symbol  c: clientOrderId  i: orderId  X: status
        z: cumFilledQty  Z: cumQuoteQty  L: lastFilledPrice  p: orderPrice
        n: commission  N: commissionAsset  T: transactionTime
        """
        try:
            symbol = getattr(msg, "s", "") or ""
            if symbol != self.config.symbol:
                return

            cl_ord_id = getattr(msg, "c", "") or ""
            order_id = getattr(msg, "i", 0) or 0
            status_x = getattr(msg, "X", "") or ""
            cum_filled = self._safe_decimal(getattr(msg, "z", None))
            cum_quote = self._safe_decimal(getattr(msg, "Z", None))
            last_price = self._safe_decimal(getattr(msg, "L", None))
            price = self._safe_decimal(getattr(msg, "p", None))
            commission = self._safe_decimal(getattr(msg, "n", None))
            commission_asset = getattr(msg, "N", "") or ""
            txn_time = int(getattr(msg, "T", 0) or 0)

            avg_px = (cum_quote / cum_filled) if cum_filled > 0 else (last_price or price)

            async with self.position_lock:
                pos = self.current_position
                if not pos:
                    return
                if cl_ord_id not in (pos.buy_cl_ord_id, pos.sell_cl_ord_id):
                    return
                # 乱序检测
                if txn_time > 0 and txn_time < pos.last_update_time:
                    logger.debug(f"忽略乱序消息 {cl_ord_id} T={txn_time} < {pos.last_update_time}")
                    return
                if txn_time > 0:
                    pos.last_update_time = txn_time

                # ─── 买单事件 ───
                if cl_ord_id == pos.buy_cl_ord_id:
                    pos.buy_order_id = order_id or pos.buy_order_id
                    if status_x == "FILLED" or (status_x in ("CANCELED", "EXPIRED") and cum_filled > 0):
                        # 实际可卖 = 全成交 - base 手续费
                        actual_sz = cum_filled
                        if commission_asset == self.base_currency and commission > 0:
                            actual_sz = max(cum_filled - commission, Decimal("0"))
                        pos.filled_size = str(actual_sz)
                        pos.buy_price = str(avg_px)
                        pos.buy_time = datetime.now()
                        pos.status = PositionStatus.OPEN
                        self._accumulate_fee(commission, commission_asset)
                        fee_info = f" 手续费={commission} {commission_asset}" if commission > 0 else ""
                        label = "✅ 买单成交" if status_x == "FILLED" else "⚠️ 买单部分成交后取消"
                        logger.info(
                            f"{label} | {cl_ord_id} | 成交价={avg_px} 量={cum_filled} "
                            f"(可卖={actual_sz}){fee_info}"
                        )
                    elif status_x in ("CANCELED", "EXPIRED", "REJECTED"):
                        pos.status = PositionStatus.CLOSED
                        self.current_position = None
                        logger.info(f"⚠️ 买单全量取消 | {cl_ord_id} ({status_x})")

                # ─── 卖单事件 ───
                elif cl_ord_id == pos.sell_cl_ord_id:
                    pos.sell_order_id = order_id or pos.sell_order_id
                    if status_x == "FILLED":
                        pos.sell_filled_size = str(cum_filled)
                        buy_price = self._safe_decimal(pos.buy_price)
                        size = self._safe_decimal(pos.filled_size)
                        pnl = (avg_px - buy_price) * size if size > 0 else Decimal("0")
                        pos.pnl = pnl
                        pos.sell_time = datetime.now()
                        self.total_buy_volume += buy_price * size
                        self.total_sell_volume += avg_px * size
                        self.total_pnl += pnl
                        self.order_count += 1
                        self._accumulate_fee(commission, commission_asset)
                        pos.status = PositionStatus.CLOSED
                        self.current_position = None
                        fee_info = f" 手续费={commission} {commission_asset}" if commission > 0 else ""
                        logger.info(
                            f"✅ 卖单成交 | {cl_ord_id} | 价={avg_px} 盈亏={pnl:+.6f}"
                            f"{fee_info} 累计={self.total_pnl:+.6f}"
                        )
                    elif status_x in ("CANCELED", "EXPIRED", "REJECTED"):
                        if cum_filled > 0:
                            # 部分卖出已成交 → 累入统计, 仅更新 sell_filled_size, 不动 filled_size
                            partial_size = cum_filled
                            buy_price = self._safe_decimal(pos.buy_price)
                            partial_pnl = (avg_px - buy_price) * partial_size
                            self.total_buy_volume += buy_price * partial_size
                            self.total_sell_volume += avg_px * partial_size
                            self.total_pnl += partial_pnl
                            self._accumulate_fee(commission, commission_asset)
                            pos.sell_filled_size = str(partial_size)
                            remaining = self._safe_decimal(pos.filled_size) - partial_size
                            logger.warning(f"⚠️ 卖单部分取消 已卖={partial_size} 剩余={remaining}")
                        else:
                            logger.warning(f"⚠️ 卖单全量取消 | {cl_ord_id} ({status_x})")
        except Exception as e:
            logger.error(f"❌ 处理订单推送失败: {e}", exc_info=True)

    # ─── 下单 / 撤单 ──────────────────────────────────────────

    async def _place_limit_order(self, side: str, price: Decimal, size: Decimal, cl_ord_id: str) -> dict:
        try:
            resp = await asyncio.wait_for(
                self.ws_api_conn.order_place(
                    symbol=self.config.symbol,
                    side=side.upper(),
                    type="LIMIT",
                    time_in_force="GTC",
                    quantity=self._adjust_size(size),
                    price=self._adjust_price(price),
                    new_client_order_id=cl_ord_id,
                ),
                timeout=10,
            )
            data = resp.data()
            err_code, err_msg = parse_ws_error(data)
            if err_code:
                return {"ok": False, "code": err_code, "msg": err_msg}
            result = getattr(data, "result", None) or data
            oid = getattr(result, "order_id", None) or getattr(result, "orderId", None)
            return {"ok": True, "order_id": oid}
        except asyncio.TimeoutError:
            return {"ok": False, "code": -999, "msg": "timeout"}
        except Exception as e:
            err_code, err_msg = parse_ws_error({"error": str(e)})
            return {"ok": False, "code": err_code or -1, "msg": err_msg or str(e)}

    async def _place_market_order(self, side: str, size: Decimal, cl_ord_id: str) -> dict:
        try:
            resp = await asyncio.wait_for(
                self.ws_api_conn.order_place(
                    symbol=self.config.symbol,
                    side=side.upper(),
                    type="MARKET",
                    quantity=self._adjust_size(size),
                    new_client_order_id=cl_ord_id,
                ),
                timeout=10,
            )
            data = resp.data()
            err_code, err_msg = parse_ws_error(data)
            if err_code:
                return {"ok": False, "code": err_code, "msg": err_msg}
            result = getattr(data, "result", None) or data
            oid = getattr(result, "order_id", None) or getattr(result, "orderId", None)
            return {"ok": True, "order_id": oid}
        except asyncio.TimeoutError:
            return {"ok": False, "code": -999, "msg": "timeout"}
        except Exception as e:
            err_code, err_msg = parse_ws_error({"error": str(e)})
            return {"ok": False, "code": err_code or -1, "msg": err_msg or str(e)}

    async def _cancel_order(self, order_id: int = None, cl_ord_id: str = None) -> dict:
        try:
            kwargs = {"symbol": self.config.symbol}
            if order_id:
                kwargs["order_id"] = order_id
            elif cl_ord_id:
                kwargs["orig_client_order_id"] = cl_ord_id
            else:
                return {"ok": False, "msg": "no id"}
            await asyncio.wait_for(self.ws_api_conn.order_cancel(**kwargs), timeout=5)
            return {"ok": True}
        except asyncio.TimeoutError:
            return {"ok": False, "msg": "timeout"}
        except Exception as e:
            err_code, err_msg = parse_ws_error({"error": str(e)})
            return {"ok": False, "code": err_code or -1, "msg": err_msg or str(e)}

    # ─── REST ────────────────────────────────────────────────

    async def _query_base_balance(self) -> Decimal:
        """REST 查询 base 币种可用余额"""
        if not self.base_currency:
            return Decimal("0")
        try:
            resp = await asyncio.to_thread(self.client.rest_api.get_account)
            data = resp.data()
            for b in data.balances or []:
                if b.asset == self.base_currency:
                    return Decimal(str(b.free))
        except Exception as e:
            logger.warning(f"查询 {self.base_currency} 余额失败: {e}")
        return Decimal("0")

    # ─── 主循环 ────────────────────────────────────────────

    async def _wait_position(self, predicate, timeout: float) -> bool:
        start = datetime.now()
        while (datetime.now() - start).total_seconds() < timeout:
            async with self.position_lock:
                if predicate(self.current_position):
                    return True
            await asyncio.sleep(0.3)
        return False

    async def execute_one_cycle(self):
        try:
            # 价格新鲜度检查
            if self.latest_ask_price <= 0 or self.latest_bid_price <= 0:
                logger.warning("⚠️ 等待价格数据...")
                await asyncio.sleep(1)
                return
            if self.last_price_update and (datetime.now() - self.last_price_update).total_seconds() > 30:
                logger.warning("⚠️ 价格数据陈旧 (>30s),跳过本轮")
                await asyncio.sleep(2)
                return

            # BBO 价差过滤
            if self.max_spread_d > 0:
                spread = self.latest_ask_price - self.latest_bid_price
                if spread > self.max_spread_d:
                    logger.info(
                        f"⏭️ 价差 {spread} > max_spread {self.max_spread_d}, 跳过 "
                        f"(bid={self.latest_bid_price} ask={self.latest_ask_price})"
                    )
                    return

            quantity = self._safe_decimal(self.config.quantity)

            # BBO 挂单量过滤
            if (self.latest_bid_size > 0 and self.latest_bid_size < quantity) or \
               (self.latest_ask_size > 0 and self.latest_ask_size < quantity):
                logger.info(
                    f"⏭️ BBO 量不足 (bid_sz={self.latest_bid_size} ask_sz={self.latest_ask_size}) "
                    f"< quantity={quantity}, 跳过"
                )
                return
            if self.min_size > 0 and quantity < self.min_size:
                logger.error(f"❌ quantity {quantity} < min_size {self.min_size}, 终止")
                self.should_stop = True
                return

            # ─── 1. 下买单 ───
            buy_cl_ord_id = self._generate_order_id("b")
            async with self.position_lock:
                self.current_position = InstantPosition(
                    buy_cl_ord_id=buy_cl_ord_id,
                    symbol=self.config.symbol,
                    quantity=str(quantity),
                )

            logger.info(f"📊 下买单 | 价={self.latest_ask_price} 量={quantity}")
            buy_resp = await self._place_limit_order("BUY", self.latest_ask_price, quantity, buy_cl_ord_id)
            if not buy_resp["ok"]:
                logger.error(f"❌ 买单失败 | code={buy_resp.get('code')} msg={buy_resp.get('msg')}")
                async with self.position_lock:
                    self.current_position = None
                return

            async with self.position_lock:
                if self.current_position:
                    self.current_position.buy_order_id = buy_resp.get("order_id") or 0

            # ─── 2. 等买单 OPEN/CLOSED ───
            await self._wait_position(
                lambda p: p is None or p.status in (PositionStatus.OPEN, PositionStatus.CLOSED),
                self.config.order_timeout,
            )

            # 检查是否需要撤单
            async with self.position_lock:
                pos = self.current_position
                if pos is None:
                    return  # 全量取消
                need_cancel = (pos.status == PositionStatus.OPENING)
                cancel_oid = pos.buy_order_id if need_cancel else None
                cancel_cl = pos.buy_cl_ord_id if need_cancel else None

            if need_cancel:
                logger.warning(f"⚠️ 买单超时,撤单 {cancel_oid or cancel_cl}")
                await self._cancel_order(order_id=cancel_oid, cl_ord_id=cancel_cl)
                await asyncio.sleep(0.8)
                async with self.position_lock:
                    pos = self.current_position
                    if pos is None or pos.status == PositionStatus.CLOSED:
                        if self.current_position is not None:
                            self.current_position = None
                        return
                    # 否则有部分持仓继续走平仓流程

            # 等 200ms 让结算完成 (避免 availBal 刚成交时延迟)
            await asyncio.sleep(0.2)

            # ─── 3. 准备卖单数量: min(内存 filled_size, REST availBal) 防超卖 ───
            avail = await self._query_base_balance()
            async with self.position_lock:
                pos = self.current_position
                if pos is None:
                    return
                memory_size = self._safe_decimal(pos.filled_size)
                filled_size = min(memory_size, avail) if avail > 0 else memory_size
                if self.min_size > 0 and filled_size < self.min_size:
                    logger.warning(
                        f"⚠️ 可卖 {filled_size} < min_size (mem={memory_size}, avail={avail}), 放弃"
                    )
                    self.current_position = None
                    return
                sell_cl_ord_id = self._generate_order_id("s")
                pos.sell_cl_ord_id = sell_cl_ord_id
                pos.status = PositionStatus.CLOSING

            # ─── 4. 下限价卖单 ───
            logger.info(
                f"📊 下卖单 | 价={self.latest_bid_price} 量={filled_size} "
                f"(mem={memory_size} avail={avail})"
            )
            sell_resp = await self._place_limit_order("SELL", self.latest_bid_price, filled_size, sell_cl_ord_id)
            sell_ok = sell_resp["ok"]
            if sell_ok:
                async with self.position_lock:
                    if self.current_position:
                        self.current_position.sell_order_id = sell_resp.get("order_id") or 0

                await self._wait_position(lambda p: p is None, self.config.order_timeout)
                async with self.position_lock:
                    if self.current_position is None:
                        return  # 卖单成交完成

            # ─── 5. 卖单失败 / 超时 → 撤单 + 市价兜底 ───
            async with self.position_lock:
                pos = self.current_position
                if pos is None:
                    return
                sell_oid = pos.sell_order_id
                sell_cl = pos.sell_cl_ord_id

            if sell_ok and (sell_oid or sell_cl):
                logger.warning(f"⚠️ 卖单超时,撤单 {sell_oid or sell_cl}")
                await self._cancel_order(order_id=sell_oid, cl_ord_id=sell_cl)
                await asyncio.sleep(0.8)

            # REST 真实可用余额兜底
            avail = await self._query_base_balance()
            qty_str = self._adjust_size(avail)
            qty = self._safe_decimal(qty_str)
            if self.min_size > 0 and qty < self.min_size:
                logger.warning(f"⚠️ REST 余额 {avail} 不足 min_size {self.min_size}, 放弃")
                async with self.position_lock:
                    self.current_position = None
                return

            async with self.position_lock:
                pos = self.current_position
                if pos is None:
                    return
                market_cl_ord_id = self._generate_order_id("m")
                pos.sell_cl_ord_id = market_cl_ord_id

            logger.warning(f"🧹 市价平仓 量={qty} (REST availBal={avail})")
            market_resp = await self._place_market_order("SELL", qty, market_cl_ord_id)
            if not market_resp["ok"]:
                logger.error(f"❌ 市价卖单失败: {market_resp}")
                async with self.position_lock:
                    self.current_position = None
                return

            await self._wait_position(lambda p: p is None, 10)
            async with self.position_lock:
                if self.current_position is not None:
                    logger.warning("⚠️ 市价单 10s 内未推送成交, 强制清理 position")
                    self.current_position = None

        except Exception as e:
            logger.error(f"❌ 执行循环错误: {e}", exc_info=True)
            async with self.position_lock:
                self.current_position = None

    # ─── 热加载 ─────────────────────────────────────────────

    async def _reload_hot_config_once(self):
        if not self.config_path or not self.config_path.exists():
            return
        mtime = self.config_path.stat().st_mtime
        if mtime <= self._last_config_mtime:
            return
        with open(self.config_path, "r") as f:
            data = yaml.safe_load(f) or {}

        hot_fields = {
            "quantity":       lambda v: str(v),
            "order_interval": lambda v: float(v),
            "order_timeout":  lambda v: int(v),
            "max_volume":     lambda v: float(v),
            "max_spread":     lambda v: float(v or 0),
        }
        changed = {}
        for k, caster in hot_fields.items():
            if k not in data:
                continue
            try:
                new = caster(data[k])
            except Exception:
                continue
            old = getattr(self.config, k)
            if old != new:
                setattr(self.config, k, new)
                changed[k] = f"{old} -> {new}"
                if k == "max_spread":
                    self.max_spread_d = Decimal(str(new)) if new else Decimal("0")

        for k in ("strategy_id", "symbol"):
            if k in data and data[k] != getattr(self.config, k):
                logger.warning(f"配置热加载忽略 {k} 变更: {getattr(self.config, k)} -> {data[k]}")

        self._last_config_mtime = mtime
        if changed:
            logger.info("🔄 热加载配置成功 | " + " | ".join(f"{k}: {v}" for k, v in changed.items()))

    async def reload_hot_config_loop(self):
        logger.info("🔁 配置热加载监控已启动")
        while not self.should_stop:
            try:
                await self._reload_hot_config_once()
            except Exception as e:
                logger.warning(f"⚠️ 热加载配置失败: {e}")
            await asyncio.sleep(5)

    # ─── 统计 ──────────────────────────────────────────────

    @property
    def _total_volume(self) -> Decimal:
        """双边累计成交额: 买入额 + 卖出额"""
        return self.total_buy_volume + self.total_sell_volume

    def _volume_progress(self) -> float:
        cap = self.config.max_volume
        if cap <= 0:
            return 0.0
        return float(self._total_volume / Decimal(str(cap)) * 100)

    def _max_volume_reached(self) -> bool:
        cap = self.config.max_volume
        return cap > 0 and self._total_volume >= Decimal(str(cap))

    async def print_stats(self):
        while not self.should_stop:
            await asyncio.sleep(30)
            net_pnl = self.total_pnl - self.total_fee_quote - (self.total_fee_base * self.latest_bid_price)
            logger.info(
                f"\n{'='*80}\n"
                f"📊 统计报告\n"
                f"累计买入: {self.total_buy_volume:.2f} USDT\n"
                f"累计卖出: {self.total_sell_volume:.2f} USDT\n"
                f"总交易量: {self._total_volume:.2f} USDT | 目标 {self.config.max_volume:.2f} | 进度: {self._volume_progress():.1f}%\n"
                f"订单数:   {self.order_count}\n"
                f"价差盈亏: {self.total_pnl:+.6f} USDT (不含手续费)\n"
                f"手续费:   base={self.total_fee_base} {self.base_currency} | quote={self.total_fee_quote} {self.quote_currency}\n"
                f"净盈亏:   {net_pnl:+.6f} USDT\n"
                f"价格:     买一={self.latest_bid_price} 卖一={self.latest_ask_price}\n"
                f"{'='*80}"
            )
            await self._save_state()

    # ─── WS 健康监控 ──────────────────────────────────────────

    async def _ws_health_monitor(self):
        """Binance SDK 内置自动重连, 此处仅监控价格新鲜度并告警"""
        while not self.should_stop:
            await asyncio.sleep(15)
            if self.last_price_update is None:
                continue
            elapsed = (datetime.now() - self.last_price_update).total_seconds()
            if elapsed > 60:
                logger.warning(f"⚠️ 价格 {elapsed:.0f}s 未更新 (SDK 应自动重连)")

    # ─── 退出清仓 ────────────────────────────────────────────

    async def _market_close_position(self):
        """市价平掉当前持仓: REST 查真实余额, 5 次重试"""
        if not self.base_currency:
            return
        for attempt in range(5):
            avail = await self._query_base_balance()
            qty_pre = self._adjust_size(avail)
            qty = self._safe_decimal(qty_pre)
            if qty <= 0 or (self.min_size > 0 and qty < self.min_size):
                if attempt == 0:
                    logger.info(f"无 {self.base_currency} 持仓需清仓 (avail={avail})")
                else:
                    logger.info(f"✅ 清仓完成 | 剩余 {avail} {self.base_currency} (< min_size 视为碎片)")
                return
            cl_ord_id = self._generate_order_id("e")
            logger.info(f"正在清仓: {qty_pre} {self.base_currency}")
            resp = await self._place_market_order("SELL", qty, cl_ord_id)
            if not resp["ok"]:
                logger.warning(f"清仓失败: {resp}")
                break
            await asyncio.sleep(2)
        logger.info("清仓完成")

    async def _shutdown_close(self, reason: str = ""):
        """退出清仓 (mm_v2 同款流程):
        1. WS 撤本地单 + REST 批量撤残留
        2. sleep 0.5
        3. 市价清仓 (REST 查余额 + 5 次重试)
        """
        logger.info(f"正在关闭策略 ({reason})...")

        # 1. WS 撤本地已知单
        try:
            async with self.position_lock:
                pos = self.current_position
                buy_id = pos.buy_order_id if pos else None
                buy_cl = pos.buy_cl_ord_id if pos else None
                sell_id = pos.sell_order_id if pos else None
                sell_cl = pos.sell_cl_ord_id if pos else None
                pos_status = pos.status if pos else None
            if buy_id and pos_status == PositionStatus.OPENING:
                await asyncio.wait_for(
                    self._cancel_order(order_id=buy_id, cl_ord_id=buy_cl), timeout=3.0
                )
            if sell_id and sell_cl and not sell_cl.startswith("m"):
                await asyncio.wait_for(
                    self._cancel_order(order_id=sell_id, cl_ord_id=sell_cl), timeout=3.0
                )
        except Exception as e:
            logger.warning(f"WS 撤单失败: {e}")

        # 2. REST 批量撤残留
        try:
            await asyncio.to_thread(
                self.client.rest_api.delete_open_orders, symbol=self.config.symbol
            )
            logger.info("REST 批量撤单完成")
        except Exception as e:
            logger.warning(f"REST 撤单失败: {e}")

        await asyncio.sleep(0.5)

        # 3. 市价清仓
        await self._market_close_position()

        # 4. 清状态
        async with self.position_lock:
            self.current_position = None
        await self._save_state()

    def _log_final_summary(self, reason: str):
        net_pnl = self.total_pnl - self.total_fee_quote - (self.total_fee_base * self.latest_bid_price)
        logger.info(
            f"\n{'='*80}\n"
            f"✅ 策略结束 ({reason})\n"
            f"总买入: {self.total_buy_volume:.2f} USDT\n"
            f"总卖出: {self.total_sell_volume:.2f} USDT\n"
            f"总交易量: {self._total_volume:.2f} USDT (目标 {self.config.max_volume:.2f}, 进度 {self._volume_progress():.1f}%)\n"
            f"订单数: {self.order_count}\n"
            f"价差盈亏: {self.total_pnl:+.6f} USDT\n"
            f"手续费:   base={self.total_fee_base} {self.base_currency} | quote={self.total_fee_quote} {self.quote_currency}\n"
            f"净盈亏:   {net_pnl:+.6f} USDT\n"
            f"{'='*80}"
        )

    # ─── 入口 ─────────────────────────────────────────────

    async def run(self):
        try:
            await self._load_state()
            await self._get_instrument_info()
            await self._init_ws()

            logger.info("⏳ 等待 BBO 数据...")
            for _ in range(30):
                if self.latest_ask_price > 0:
                    break
                await asyncio.sleep(0.5)
            logger.info("✅ WS 就绪, 策略启动")

            self._tasks = [
                asyncio.create_task(self.print_stats()),
                asyncio.create_task(self._ws_health_monitor()),
                asyncio.create_task(self.reload_hot_config_loop()),
            ]

            while not self.should_stop:
                if self._max_volume_reached():
                    logger.warning(
                        f"🎯 达到目标 max_volume {self.config.max_volume} "
                        f"(累计 {self._total_volume:.2f}), 触发退出"
                    )
                    self.should_stop = True
                    break
                await self.execute_one_cycle()
                await asyncio.sleep(self.config.order_interval)

        except Exception as e:
            logger.error(f"❌ 策略运行错误: {e}", exc_info=True)
        finally:
            self.should_stop = True
            reason = "达到 max_volume" if self._max_volume_reached() else "策略退出"
            # 1. 清仓
            try:
                await self._shutdown_close(reason)
            except Exception as e:
                logger.error(f"❌ 退出清仓异常: {e}")
            # 2. 取消后台任务
            for t in self._tasks:
                t.cancel()
            for t in self._tasks:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            # 3. 关 WS
            for conn in (self.ws_api_conn, self.ws_streams_conn):
                if conn:
                    try:
                        await conn.close_connection()
                    except Exception:
                        pass
            # 4. 最终保存 + 总结
            await self._save_state()
            self._log_final_summary(reason)


async def main():
    config_path = os.getenv("CONFIG_FILE", str(Path(__file__).parent / "config.yaml"))
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    config = StrategyConfig(
        strategy_id=cfg["strategy_id"],
        symbol=cfg["symbol"],
        quantity=str(cfg["quantity"]),
        order_interval=float(cfg["order_interval"]),
        max_volume=float(cfg["max_volume"]),
        order_timeout=int(cfg.get("order_timeout", 30)),
        max_spread=float(cfg.get("max_spread", 0) or 0),
    )

    logger.remove()
    logger.add(lambda m: print(m, end=""), level="INFO")
    logger.add(
        f"logs/{config.strategy_id}_instant.log",
        rotation="00:00",
        retention="7 days",
        level="INFO",
    )

    logger.info(
        f"\n{'='*80}\n"
        f"🚀 币安即时刷量策略\n"
        f"策略ID: {config.strategy_id}\n"
        f"交易对: {config.symbol}\n"
        f"数量: {config.quantity}\n"
        f"间隔: {config.order_interval}s\n"
        f"目标 max_volume: {config.max_volume} USDT (买+卖累计上限)\n"
        f"模式: {'🧪 模拟盘' if os.getenv('BINANCE_DEMO', '0') == '1' else '💰 实盘'}\n"
        f"{'='*80}"
    )

    strategy = InstantVolumeStrategy(config, config_path)

    def signal_handler(_sig, _frame):
        logger.warning("\n⚠️ 收到停止信号, 安全退出...")
        strategy.should_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await strategy.run()
    except KeyboardInterrupt:
        logger.warning("⚠️ 键盘中断")
        strategy.should_stop = True
    finally:
        await strategy._save_state()
        logger.info("👋 策略已退出")


if __name__ == "__main__":
    asyncio.run(main())
