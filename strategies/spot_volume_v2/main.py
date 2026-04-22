#!/usr/bin/env python3
"""币安现货刷量策略V2 - 买侧网格 + 止盈止损 + 波动率熔断 (SDK 版)"""
import asyncio
import itertools
import json
import math
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

import yaml
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from strategies.common import load_credentials, make_spot_client
from strategies.common.ws_errors import parse_ws_error


# ==================== 数据模型 ====================

class PositionStatus(Enum):
    OPENING = "opening"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class Position:
    buy_cl_ord_id: str
    symbol: str
    quantity: str
    buy_price: str = ""
    buy_order_id: int = 0
    tp_cl_ord_id: str = ""
    tp_order_id: int = 0
    sl_cl_ord_id: str = ""
    timeout_cl_ord_id: str = ""
    emergency_cl_ord_id: str = ""
    emergency_submitted: bool = False
    status: PositionStatus = PositionStatus.OPENING
    created_time: datetime = field(default_factory=datetime.now)
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    filled_size: str = "0"
    tp_filled_size: str = "0"
    take_profit_price: str = ""
    stop_loss_price: str = ""
    pnl: Decimal = Decimal("0")
    close_reason: str = ""

    def is_timeout(self, timeout_seconds: int) -> bool:
        return bool(self.open_time and (datetime.now() - self.open_time).total_seconds() > timeout_seconds)

    def is_closing_stuck(self, max_closing_seconds: int = 300) -> bool:
        if self.status == PositionStatus.CLOSING:
            ref_time = self.close_time or self.open_time or self.created_time
            return (datetime.now() - ref_time).total_seconds() > max_closing_seconds
        return False


@dataclass
class StrategyConfig:
    strategy_id: str = "SPOT_V2_001"
    symbol: str = "XAUTUSDT"
    quantity: float = 0.3
    take_profit_offset: float = 0.01
    stop_loss_offset: float = 0.02
    take_profit_timeout: int = 60
    stop_loss_check_interval: float = 1.0
    step: float = 0.01
    start_level: int = 0
    num_levels: int = 5
    size_growth_factor: float = 0
    max_net_buy: float = 10
    order_interval: float = 1.0
    max_order_groups: int = 10
    max_volume: float = 100000
    auto_close_on_exit: bool = True
    vol_enabled: bool = True
    vol_safe_range_pct: float = 0.0001
    vol_max_range_pct: float = 0
    vol_pause_multiplier: float = 3.0
    vol_resume_multiplier: float = 1.5
    vol_close_on_pause: bool = True

    @classmethod
    def from_yaml(cls, file_path: str) -> "StrategyConfig":
        with open(file_path) as f:
            data = yaml.safe_load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self):
        assert self.symbol and self.quantity > 0, "交易对和数量必须有效"
        assert self.stop_loss_offset > 0, "必须设置止损参数"


# ==================== 统计 ====================

class Stats:
    def __init__(self, strategy_id: str, symbol: str):
        self.strategy_id, self.symbol = strategy_id, symbol
        self.state_file = Path(__file__).parent / "volume_state.json"
        self.lock = asyncio.Lock()
        self.session_buy_volume = self.session_sell_volume = self.session_pnl = Decimal("0")
        self.session_fee = Decimal("0")
        self.session_order_count = 0
        self.global_buy_volume = self.global_sell_volume = self.global_pnl = Decimal("0")
        self.global_fee = Decimal("0")
        self.dirty = False
        self.last_save_time = time.time()
        self._load_state()

    def _load_state(self):
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text()).get(self.symbol, {})
                self.global_buy_volume = Decimal(data.get("global_buy_volume", "0"))
                self.global_sell_volume = Decimal(data.get("global_sell_volume", "0"))
                self.global_pnl = Decimal(data.get("global_pnl", "0"))
                self.global_fee = Decimal(data.get("global_fee", "0"))
                logger.info(f"加载统计 [{self.symbol}] | 买入={self.global_buy_volume} | 卖出={self.global_sell_volume}")
            except Exception as e:
                logger.warning(f"加载统计失败: {e}")

    async def save_state(self):
        async with self.lock:
            if self.dirty:
                await self._save_now()

    async def add_volume(self, buy_volume: Decimal = None, sell_volume: Decimal = None,
                         pnl: Decimal = None, fee: Decimal = None):
        async with self.lock:
            if buy_volume:
                self.session_buy_volume += buy_volume
                self.global_buy_volume += buy_volume
                self.session_order_count += 1
            if sell_volume:
                self.session_sell_volume += sell_volume
                self.global_sell_volume += sell_volume
            if pnl is not None:
                self.session_pnl += pnl
                self.global_pnl += pnl
            if fee:
                self.session_fee += fee
                self.global_fee += fee
            self.dirty = True
            if time.time() - self.last_save_time > 5.0 or self.session_order_count % 10 == 0:
                await self._save_now()

    async def _save_now(self):
        try:
            all_data = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
            all_data[self.symbol] = {
                "global_buy_volume": str(self.global_buy_volume),
                "global_sell_volume": str(self.global_sell_volume),
                "global_pnl": str(self.global_pnl),
                "global_fee": str(self.global_fee),
                "updated_at": datetime.now().isoformat(),
            }
            self.state_file.write_text(json.dumps(all_data, indent=2))
            self.dirty = False
            self.last_save_time = time.time()
        except Exception as e:
            logger.error(f"保存统计失败: {e}")

    def is_max_volume_reached(self, max_volume: float) -> bool:
        return self.global_buy_volume + self.global_sell_volume >= Decimal(str(max_volume))

    async def get_summary(self) -> dict:
        async with self.lock:
            return {
                "session": {
                    "buy": float(self.session_buy_volume), "sell": float(self.session_sell_volume),
                    "total": float(self.session_buy_volume + self.session_sell_volume),
                    "pnl": float(self.session_pnl), "fee": float(self.session_fee),
                    "count": self.session_order_count,
                },
                "global": {
                    "buy": float(self.global_buy_volume), "sell": float(self.global_sell_volume),
                    "total": float(self.global_buy_volume + self.global_sell_volume),
                    "pnl": float(self.global_pnl), "fee": float(self.global_fee),
                },
            }


# ==================== 波动率 ====================

class VolatilityTracker:
    def __init__(self, window: int = 60, baseline_samples: int = 60):
        self._prices: deque[float] = deque(maxlen=window)
        self._range_history: deque[float] = deque(maxlen=baseline_samples)
        self.current_range: float = 0
        self.baseline: float = 0
        self.last_price: float = 0
        self._last_snapshot: float = 0

    def on_price(self, price: float):
        self._prices.append(price)
        self.last_price = price
        if len(self._prices) >= 10:
            self.current_range = max(self._prices) - min(self._prices)
        now = time.time()
        if now - self._last_snapshot >= 60:
            if self.current_range > 0:
                self._range_history.append(self.current_range)
            if len(self._range_history) >= 5:
                self.baseline = sorted(self._range_history)[len(self._range_history) // 2]
            self._last_snapshot = now
            logger.info(f"[波动率] 振幅={self.current_range:.6f} 基准={self.baseline:.6f} "
                        f"倍数={self.ratio:.1f}x 比例={self.range_pct:.6f} 样本数={len(self._range_history)}")

    @property
    def ratio(self) -> float:
        return self.current_range / self.baseline if self.baseline > 0 else 0

    @property
    def range_pct(self) -> float:
        return self.current_range / self.last_price if self.last_price > 0 else 0

    @property
    def ready(self) -> bool:
        return self.baseline > 0


# ==================== 策略主类 ====================

class SpotVolumeV2:
    def __init__(self, config: StrategyConfig, config_path: str = None):
        self.config = config
        self.config_path = Path(config_path) if config_path else None
        self._last_config_mtime = self.config_path.stat().st_mtime if self.config_path and self.config_path.exists() else 0

        creds = load_credentials()
        self.client = make_spot_client(creds)

        # 市场数据
        self.bid_price = self.ask_price = Decimal("0")
        self.tick_size = Decimal("0.01")
        self.step_size = Decimal("0.001")
        self.min_notional = Decimal("5")
        self.base_asset = ""

        # 持仓管理
        self.positions: Dict[str, Position] = {}
        self.positions_lock = asyncio.Lock()
        self.order_cl_map: Dict[str, str] = {}
        self.order_id_map: Dict[int, str] = {}
        self.stats = Stats(config.strategy_id, config.symbol)
        self.order_counter = itertools.count(1)

        # SDK 连接句柄
        self.ws_api_conn = None
        self.ws_streams_conn = None

        # 波动率
        self.vol_tracker = VolatilityTracker()
        self._vol_paused = False

        # 控制
        self.should_stop = False
        self.strategy_tasks = []

    def _align_price(self, price: Decimal) -> str:
        if self.tick_size <= 0:
            return str(price)
        return str((price / self.tick_size).to_integral_value() * self.tick_size)

    def _align_qty(self, qty: Decimal) -> str:
        if self.step_size <= 0:
            return str(qty)
        return str((qty / self.step_size).to_integral_value() * self.step_size)

    @staticmethod
    def _short_id(order_id) -> str:
        s = str(order_id)
        return s[-8:] if len(s) > 8 else s

    @staticmethod
    def _generate_order_id(prefix: str) -> str:
        return f"{prefix}{int(time.time() * 1000) % 10000000000000}"

    @staticmethod
    def _safe_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
        if value is None or value == "":
            return default
        try:
            return Decimal(str(value))
        except (ValueError, TypeError, ArithmeticError):
            return default

    def _check_min_order_value(self, size: str, price: Decimal = None) -> tuple:
        estimate_price = price or self.bid_price
        order_value = self._safe_decimal(size) * estimate_price
        return order_value >= self.min_notional, order_value

    # ==================== REST (SDK) ====================

    async def fetch_exchange_info(self):
        resp = await asyncio.to_thread(self.client.rest_api.exchange_info, symbol=self.config.symbol)
        data = resp.data()
        if not data.symbols:
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")
        sym = data.symbols[0]
        self.base_asset = sym.base_asset
        for f in sym.filters:
            ft = getattr(f, "filter_type", None) or getattr(f, "filterType", None)
            if ft == "PRICE_FILTER":
                self.tick_size = Decimal(str(f.tick_size))
            elif ft == "LOT_SIZE":
                self.step_size = Decimal(str(f.step_size))
            elif ft == "NOTIONAL":
                mn = getattr(f, "min_notional", None) or getattr(f, "minNotional", None)
                if mn:
                    self.min_notional = Decimal(str(mn))
        logger.info(f"交易规则 | {self.config.symbol} | base={self.base_asset} | "
                    f"tick={self.tick_size} | step={self.step_size} | minNotional={self.min_notional}")

    async def rest_get_balance(self, asset: str) -> Decimal:
        resp = await asyncio.to_thread(self.client.rest_api.get_account)
        data = resp.data()
        for b in data.balances or []:
            if b.asset == asset:
                return Decimal(str(b.free))
        return Decimal("0")

    async def rest_place_market_sell(self, qty: str) -> dict:
        try:
            resp = await asyncio.to_thread(
                self.client.rest_api.new_order,
                symbol=self.config.symbol, side="SELL", type="MARKET", quantity=str(qty),
            )
            data = resp.data()
            order_id = getattr(data, "order_id", None)
            return {"orderId": order_id, "status": getattr(data, "status", "")}
        except Exception as e:
            logger.error(f"市价卖出失败: {e}")
            return {}

    async def rest_cancel_open_orders(self):
        try:
            await asyncio.to_thread(self.client.rest_api.delete_open_orders, symbol=self.config.symbol)
        except Exception as e:
            logger.warning(f"REST 批量撤单失败: {e}")

    # ==================== WS API (SDK) ====================

    async def _init_ws_api(self):
        """建立 WS API 连接并 session_logon"""
        self.ws_api_conn = await self.client.websocket_api.create_connection()
        await self.ws_api_conn.session_logon()
        logger.info("WS API 已连接并认证")

    async def ws_place_order(self, side: str, ord_type: str, quantity: str,
                             price: str = None, client_order_id: str = None,
                             time_in_force: str = None) -> dict:
        """WS API 下单，返回 {result: {orderId, status}, error: ...}"""
        try:
            tif = time_in_force or ("GTC" if ord_type == "LIMIT" else None)
            resp = await asyncio.wait_for(
                self.ws_api_conn.order_place(
                    symbol=self.config.symbol,
                    side=side,
                    type=ord_type,
                    quantity=str(quantity),
                    price=str(price) if price else None,
                    new_client_order_id=client_order_id,
                    time_in_force=tif,
                ),
                timeout=10,
            )
            data = resp.data()
            result = data.result
            if result is None:
                return {"error": "no result"}
            return {"result": {"orderId": result.order_id, "status": result.status}}
        except asyncio.TimeoutError:
            return {"error": "timeout"}
        except Exception as e:
            err_code, err_msg = parse_ws_error({"error": str(e)})
            return {"error": {"code": err_code, "msg": err_msg or str(e)}}

    async def ws_cancel_order(self, order_id: int = None, client_order_id: str = None) -> dict:
        """WS API 撤单"""
        try:
            resp = await asyncio.wait_for(
                self.ws_api_conn.order_cancel(
                    symbol=self.config.symbol,
                    order_id=order_id,
                    orig_client_order_id=client_order_id,
                ),
                timeout=5,
            )
            data = resp.data()
            return {"result": {"orderId": data.result.order_id if data.result else None}}
        except asyncio.TimeoutError:
            logger.warning("撤单超时")
            return {"error": "timeout"}
        except Exception as e:
            err_code, err_msg = parse_ws_error({"error": str(e)})
            return {"error": {"code": err_code, "msg": err_msg or str(e)}}

    # ==================== WS Streams (SDK) ====================

    def _on_book_ticker(self, msg):
        """bookTicker 回调 (BookTickerResponse Pydantic 或 dict)"""
        try:
            b = getattr(msg, "b", None) or (msg.get("b") if isinstance(msg, dict) else None)
            a = getattr(msg, "a", None) or (msg.get("a") if isinstance(msg, dict) else None)
            if b and a:
                self.bid_price = Decimal(str(b))
                self.ask_price = Decimal(str(a))
        except Exception as e:
            logger.debug(f"解析 bookTicker 失败: {e}")

    def _on_ticker(self, msg):
        """24hr ticker 回调，用于波动率追踪 (TickerResponse Pydantic 或 dict)"""
        try:
            # SDK Pydantic 字段 c = lastPrice, dict 也用 c
            c = getattr(msg, "c", None) or (msg.get("c") if isinstance(msg, dict) else None)
            if c:
                last = float(c)
                if last > 0:
                    self.vol_tracker.on_price(last)
        except Exception as e:
            logger.debug(f"解析 ticker 失败: {e}")

    async def _init_ws_streams(self):
        """订阅 bookTicker + ticker"""
        sym = self.config.symbol.lower()
        await self.client.websocket_streams.create_connection()
        h1 = await self.client.websocket_streams.book_ticker(symbol=sym)
        h1.on("message", self._on_book_ticker)
        h2 = await self.client.websocket_streams.ticker(symbol=sym)
        h2.on("message", self._on_ticker)
        self.ws_streams_conn = self.client.websocket_streams
        logger.info(f"WS Streams 已订阅 | {self.config.symbol}@bookTicker + @ticker")

    # ==================== User Data Stream (SDK via WS API) ====================

    def _on_user_data(self, event):
        """User Data Stream 回调 (UserDataStreamEventsResponse Pydantic)"""
        try:
            from binance_sdk_spot.websocket_api.models import ExecutionReport
            actual = getattr(event, "actual_instance", event)
            if isinstance(actual, ExecutionReport):
                asyncio.create_task(self._handle_execution_report(actual))
        except Exception as e:
            logger.error(f"处理 UserData 回调失败: {e}")

    async def _init_user_data_stream(self):
        """通过 WS API session_logon + 签名订阅 User Data Stream"""
        result = await self.ws_api_conn.user_data_stream_subscribe_signature()
        result.stream.on("message", self._on_user_data)
        logger.info("User Data Stream 已订阅 (WS API session)")

    # ==================== executionReport 处理 ====================

    async def _handle_execution_report(self, msg):
        """处理 ExecutionReport Pydantic 模型"""
        try:
            symbol = getattr(msg, "s", "") or ""
            if symbol != self.config.symbol:
                return

            cl_ord_id = getattr(msg, "c", "") or ""
            order_id = getattr(msg, "i", 0) or 0
            order_status = getattr(msg, "X", "") or ""
            cum_filled_qty = self._safe_decimal(getattr(msg, "z", None))
            last_filled_price = self._safe_decimal(getattr(msg, "L", None))
            price = self._safe_decimal(getattr(msg, "p", None))
            commission = self._safe_decimal(getattr(msg, "n", None))
            commission_asset = getattr(msg, "N", "") or ""
            cum_quote_qty = self._safe_decimal(getattr(msg, "Z", None))

            position = await self._find_position(cl_ord_id, order_id)

            # 清仓单成交统计
            if not position:
                if cl_ord_id.startswith("x") and order_status == "FILLED":
                    sell_volume = cum_quote_qty
                    await self.stats.add_volume(sell_volume=sell_volume, pnl=Decimal("0"), fee=commission)
                    logger.info(f"[清仓] 卖单成交 | price={last_filled_price} | qty={cum_filled_qty} | amount={sell_volume:.2f}")
                return

            # ========== 买单 ==========
            if cl_ord_id == position.buy_cl_ord_id:
                if order_status == "FILLED":
                    actual_filled = cum_filled_qty
                    if commission_asset == self.base_asset and commission > 0:
                        actual_filled = max(cum_filled_qty - commission, Decimal("0"))
                    avg_price = cum_quote_qty / cum_filled_qty if cum_filled_qty > 0 else price
                    position.buy_price = self._align_price(avg_price)
                    position.filled_size = self._align_qty(actual_filled)
                    position.buy_order_id = order_id
                    position.open_time = datetime.now()
                    position.status = PositionStatus.OPEN
                    bp = Decimal(position.buy_price)
                    position.take_profit_price = self._align_price(bp + Decimal(str(self.config.take_profit_offset)))
                    position.stop_loss_price = self._align_price(bp - Decimal(str(self.config.stop_loss_offset)))
                    async with self.positions_lock:
                        self._register_order_mapping(order_id, position.buy_cl_ord_id)
                    buy_volume = cum_quote_qty
                    await self.stats.add_volume(buy_volume=buy_volume, fee=commission)
                    logger.info(f"[#{self._short_id(cl_ord_id)}] 买单成交 | price={position.buy_price} | "
                                f"qty={cum_filled_qty} | actual={position.filled_size} | amount={buy_volume:.2f}")
                    await self.place_take_profit_order(position)

                elif order_status == "CANCELED":
                    if cum_filled_qty > 0:
                        actual_filled = cum_filled_qty
                        if commission_asset == self.base_asset and commission > 0:
                            actual_filled = max(cum_filled_qty - commission, Decimal("0"))
                        avg_price = cum_quote_qty / cum_filled_qty if cum_filled_qty > 0 else price
                        position.buy_price = self._align_price(avg_price)
                        position.filled_size = self._align_qty(actual_filled)
                        position.status = PositionStatus.CLOSING
                        buy_volume = cum_quote_qty
                        await self.stats.add_volume(buy_volume=buy_volume, fee=commission)
                        logger.warning(f"[#{self._short_id(cl_ord_id)}] 买单部分成交后取消 | filled={cum_filled_qty}")
                        await self._submit_emergency_sell(position, position.filled_size, self._generate_order_id("e"))
                    else:
                        logger.info(f"[#{self._short_id(cl_ord_id)}] 买单取消（未成交）")
                        async with self.positions_lock:
                            self._delete_position(position)

            # ========== 卖单 ==========
            elif cl_ord_id in (position.tp_cl_ord_id, position.sl_cl_ord_id,
                               position.timeout_cl_ord_id, position.emergency_cl_ord_id):
                reason_map = {
                    position.tp_cl_ord_id: "take_profit",
                    position.sl_cl_ord_id: "stop_loss",
                    position.timeout_cl_ord_id: "timeout",
                    position.emergency_cl_ord_id: "emergency",
                }
                reason = reason_map.get(cl_ord_id, "unknown")

                if order_status == "FILLED":
                    await self._handle_sell_filled(position, msg, reason)
                elif order_status == "PARTIALLY_FILLED":
                    if cl_ord_id == position.tp_cl_ord_id:
                        position.tp_filled_size = str(cum_filled_qty)
                elif order_status == "CANCELED":
                    if cum_filled_qty > 0 and cl_ord_id == position.tp_cl_ord_id:
                        position.tp_filled_size = str(cum_filled_qty)
                        avg_px = cum_quote_qty / cum_filled_qty if cum_filled_qty > 0 else Decimal("0")
                        buy_px = self._safe_decimal(position.buy_price)
                        sell_volume = cum_quote_qty
                        pnl = (avg_px - buy_px) * cum_filled_qty
                        await self.stats.add_volume(sell_volume=sell_volume, pnl=pnl, fee=commission)
                        logger.warning(f"[#{self._short_id(cl_ord_id)}] 止盈单部分成交后取消 | filled={cum_filled_qty}")
                        if position.status != PositionStatus.CLOSING:
                            remaining = self._safe_decimal(position.filled_size) - cum_filled_qty
                            if remaining > 0:
                                await self._submit_emergency_sell(position, self._align_qty(remaining),
                                                                  self._generate_order_id("ec"), mark_submitted=True)
                    else:
                        if position.status == PositionStatus.CLOSING:
                            pass
                        else:
                            filled_size = self._safe_decimal(position.filled_size)
                            if filled_size > 0:
                                logger.warning(f"[#{self._short_id(cl_ord_id)}] 止盈单被取消，紧急平仓")
                                await self._submit_emergency_sell(
                                    position, str(filled_size),
                                    self._generate_order_id("ec"), mark_submitted=True)

        except Exception as e:
            logger.error(f"处理 executionReport 失败: {e}", exc_info=True)

    async def _handle_sell_filled(self, position: Position, msg, reason: str):
        cum_filled_qty = self._safe_decimal(getattr(msg, "z", None))
        cum_quote_qty = self._safe_decimal(getattr(msg, "Z", None))
        commission = self._safe_decimal(getattr(msg, "n", None))
        buy_price = self._safe_decimal(position.buy_price)
        avg_px = cum_quote_qty / cum_filled_qty if cum_filled_qty > 0 else Decimal("0")
        pnl = (avg_px - buy_price) * cum_filled_qty
        sell_volume = cum_quote_qty
        await self.stats.add_volume(sell_volume=sell_volume, pnl=pnl, fee=commission)
        position.pnl = pnl
        position.status = PositionStatus.CLOSED
        position.close_time = datetime.now()
        position.close_reason = reason
        reason_tag = {"take_profit": "TP", "stop_loss": "SL", "timeout": "TO", "emergency": "EM"}.get(reason, reason)
        logger.info(f"[#{self._short_id(position.buy_cl_ord_id)}] [{reason_tag}] 卖单成交 | "
                    f"price={avg_px:.6f} | qty={cum_filled_qty} | pnl={pnl:+.6f}")
        async with self.positions_lock:
            self._delete_position(position)

    # ==================== 持仓管理 ====================

    def _register_order_mapping(self, order_id, buy_cl_ord_id: str):
        if isinstance(order_id, int) and order_id > 0:
            self.order_id_map[order_id] = buy_cl_ord_id
        elif isinstance(order_id, str) and order_id:
            self.order_cl_map[order_id] = buy_cl_ord_id

    def _cleanup_position_maps(self, position: Position):
        for oid in [position.buy_cl_ord_id, position.tp_cl_ord_id,
                    position.timeout_cl_ord_id, position.emergency_cl_ord_id]:
            if oid:
                self.order_cl_map.pop(oid, None)
        for oid in [position.buy_order_id, position.tp_order_id]:
            if oid:
                self.order_id_map.pop(oid, None)

    def _delete_position(self, position: Position):
        self._cleanup_position_maps(position)
        self.positions.pop(position.buy_cl_ord_id, None)

    async def _find_position(self, cl_ord_id: str, order_id: int) -> Optional[Position]:
        async with self.positions_lock:
            buy_cl = self.order_cl_map.get(cl_ord_id) or self.order_id_map.get(order_id)
            if buy_cl:
                return self.positions.get(buy_cl)
            return self.positions.get(cl_ord_id)

    # ==================== 紧急平仓 ====================

    async def _submit_emergency_sell(self, position: Position, size: str, order_id: str, mark_submitted: bool = False):
        try:
            is_valid, order_value = self._check_min_order_value(size)
            if not is_valid:
                logger.warning(f"[#{self._short_id(position.buy_cl_ord_id)}] 紧急单金额不足 ({order_value:.2f})，跳过")
                position.status = PositionStatus.CLOSED
                async with self.positions_lock:
                    self._delete_position(position)
                return
            position.emergency_cl_ord_id = order_id
            if mark_submitted:
                position.emergency_submitted = True
            async with self.positions_lock:
                self._register_order_mapping(order_id, position.buy_cl_ord_id)
            await self.ws_place_order("SELL", "MARKET", size, client_order_id=order_id)
            position.status = PositionStatus.CLOSING
            logger.warning(f"[#{self._short_id(position.buy_cl_ord_id)}] 紧急市价单已提交 | size={size}")
        except Exception as e:
            logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] 紧急市价单失败: {e}")

    async def _emergency_close(self, position: Position):
        try:
            is_valid, order_value = self._check_min_order_value(position.filled_size)
            if not is_valid:
                position.status = PositionStatus.CLOSED
                async with self.positions_lock:
                    self._delete_position(position)
                return
            emergency_id = self._generate_order_id("em")
            position.emergency_cl_ord_id = emergency_id
            async with self.positions_lock:
                self._register_order_mapping(emergency_id, position.buy_cl_ord_id)
            await self.ws_place_order("SELL", "MARKET", position.filled_size, client_order_id=emergency_id)
            position.status = PositionStatus.CLOSING
            logger.warning(f"[#{self._short_id(position.buy_cl_ord_id)}] 紧急平仓已提交")
        except Exception as e:
            logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] 紧急平仓失败: {e}")

    # ==================== 止盈下单 ====================

    async def place_take_profit_order(self, position: Position):
        try:
            if position.tp_cl_ord_id:
                return
            tp_cl_ord_id = self._generate_order_id("s")
            position.tp_cl_ord_id = tp_cl_ord_id
            async with self.positions_lock:
                self._register_order_mapping(tp_cl_ord_id, position.buy_cl_ord_id)

            tp_price = self._safe_decimal(position.take_profit_price)
            buy_price = self._safe_decimal(position.buy_price)
            sell_price = position.buy_price if tp_price <= buy_price else position.take_profit_price
            logger.info(f"[#{self._short_id(position.buy_cl_ord_id)}] 挂止盈单 | price={sell_price} | qty={position.filled_size}")
            resp = await self.ws_place_order("SELL", "LIMIT", position.filled_size, sell_price, tp_cl_ord_id)
            result = resp.get("result", {})
            if result.get("orderId"):
                position.tp_order_id = result["orderId"]
                async with self.positions_lock:
                    self._register_order_mapping(position.tp_order_id, position.buy_cl_ord_id)
            elif resp.get("error"):
                raise RuntimeError(f"止盈下单失败: {resp['error']}")
        except Exception as e:
            logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] 止盈下单失败: {e}")
            await self._emergency_close(position)

    # ==================== 买单网格 ====================

    async def buy_grid_loop(self):
        logger.info("买单网格循环已启动")
        while not self.should_stop:
            try:
                if self.stats.is_max_volume_reached(self.config.max_volume):
                    logger.warning(f"累计成交额达到上限: {self.config.max_volume}")
                    os.kill(os.getpid(), signal.SIGINT)
                    break

                await self._check_volatility()
                if self._vol_paused:
                    await asyncio.sleep(1)
                    continue

                if self.bid_price <= 0:
                    await asyncio.sleep(1)
                    continue

                step = Decimal(str(self.config.step))
                base_qty = Decimal(str(self.config.quantity))
                growth = Decimal(str(self.config.size_growth_factor))

                proposed = []
                for i in range(self.config.num_levels):
                    px = self._align_price(self.bid_price - step * (i + self.config.start_level))
                    qty = self._align_qty(base_qty * (1 + growth * i))
                    if Decimal(px) > 0 and Decimal(qty) > 0:
                        proposed.append((px, qty))

                async with self.positions_lock:
                    opening_by_price = {p.buy_price: p for p in self.positions.values()
                                        if p.status == PositionStatus.OPENING}
                    active_count = sum(1 for p in self.positions.values()
                                       if p.status in (PositionStatus.OPENING, PositionStatus.OPEN, PositionStatus.CLOSING))
                    net_buy = sum(Decimal(p.filled_size) for p in self.positions.values()
                                  if p.status in (PositionStatus.OPEN, PositionStatus.CLOSING))

                if self.config.max_net_buy > 0:
                    max_net = Decimal(str(self.config.max_net_buy))
                    ratio = float(max(1 - net_buy / max_net, 0))
                    proposed = proposed[:math.ceil(len(proposed) * ratio)]

                target_prices = {px for px, _ in proposed}
                stale_count = 0
                for price_str, pos in opening_by_price.items():
                    if price_str not in target_prices and pos.buy_order_id:
                        try:
                            await self.ws_cancel_order(order_id=pos.buy_order_id)
                            logger.info(f"[#{self._short_id(pos.buy_cl_ord_id)}] 取消偏离档位 | price={price_str}")
                            stale_count += 1
                        except Exception as e:
                            logger.warning(f"[#{self._short_id(pos.buy_cl_ord_id)}] 取消买单失败: {e}")

                existing_prices = set(opening_by_price.keys()) & target_prices
                available_slots = self.config.max_order_groups - active_count + stale_count

                for idx, (px, qty) in enumerate(proposed):
                    if available_slots <= 0:
                        break
                    if px in existing_prices:
                        continue

                    buy_cl_ord_id = self._generate_order_id("b")
                    tp_price = self._align_price(Decimal(px) + Decimal(str(self.config.take_profit_offset)))
                    sl_price = self._align_price(Decimal(px) - Decimal(str(self.config.stop_loss_offset)))

                    position = Position(
                        buy_cl_ord_id=buy_cl_ord_id, symbol=self.config.symbol, quantity=qty,
                        buy_price=px, take_profit_price=tp_price, stop_loss_price=sl_price,
                    )
                    async with self.positions_lock:
                        self.positions[buy_cl_ord_id] = position
                        self._register_order_mapping(buy_cl_ord_id, buy_cl_ord_id)

                    order_num = next(self.order_counter)
                    logger.info(f"[#{order_num}] 买单提交 L{idx} | price={px} | qty={qty} | tp={tp_price} | sl={sl_price}")
                    try:
                        resp = await self.ws_place_order("BUY", "LIMIT", qty, px, buy_cl_ord_id)
                        result = resp.get("result", {})
                        if result.get("orderId"):
                            position.buy_order_id = result["orderId"]
                            async with self.positions_lock:
                                self._register_order_mapping(position.buy_order_id, buy_cl_ord_id)
                        elif resp.get("error"):
                            raise RuntimeError(str(resp["error"]))
                        available_slots -= 1
                    except Exception as e:
                        logger.error(f"[#{order_num}] 买单提交失败: {e}")
                        async with self.positions_lock:
                            self.positions.pop(buy_cl_ord_id, None)

                await asyncio.sleep(self.config.order_interval)

            except Exception as e:
                logger.error(f"买单网格错误: {e}")
                await asyncio.sleep(3)

    # ==================== 持仓监控（止损/超时） ====================

    async def monitor_positions(self):
        logger.info("持仓监控已启动")
        while not self.should_stop:
            try:
                async with self.positions_lock:
                    snapshot = list(self.positions.values())

                for position in snapshot:
                    if position.status != PositionStatus.OPEN or not position.open_time:
                        continue

                    sl_price = self._safe_decimal(position.stop_loss_price)
                    if self.bid_price > 0 and sl_price > 0 and self.bid_price <= sl_price:
                        await self._close_position(position, "stop_loss", "sl")
                        continue

                    if position.take_profit_price and self.ask_price > 0:
                        if self._safe_decimal(position.take_profit_price) == self.ask_price:
                            async with self.positions_lock:
                                position.open_time = datetime.now()
                            continue

                    if position.is_timeout(self.config.take_profit_timeout):
                        await self._close_position(position, "timeout", "timeout")

                await asyncio.sleep(self.config.stop_loss_check_interval)
            except Exception as e:
                logger.error(f"持仓监控错误: {e}")
                await asyncio.sleep(1)

    async def _close_position(self, position: Position, reason: str, suffix: str):
        try:
            position.status = PositionStatus.CLOSING
            cl_ord_id = self._generate_order_id("c")
            setattr(position, f"{suffix}_cl_ord_id", cl_ord_id)
            async with self.positions_lock:
                self._register_order_mapping(cl_ord_id, position.buy_cl_ord_id)

            cancel_success = True
            if position.tp_order_id:
                try:
                    resp = await self.ws_cancel_order(order_id=position.tp_order_id)
                    if resp.get("error"):
                        cancel_success = False
                        logger.warning(f"[#{self._short_id(position.buy_cl_ord_id)}] 取消止盈单失败: {resp['error']}")
                    else:
                        await asyncio.sleep(0.3)
                except Exception as e:
                    cancel_success = False
                    logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] 取消止盈单异常: {e}")

            filled_size = self._safe_decimal(position.filled_size)
            tp_filled_size = self._safe_decimal(position.tp_filled_size)
            remaining = filled_size - tp_filled_size

            if remaining > 0:
                if position.emergency_submitted:
                    return
                if not cancel_success:
                    await asyncio.sleep(1)
                    tp_filled_size = self._safe_decimal(position.tp_filled_size)
                    remaining = filled_size - tp_filled_size
                    if remaining <= 0:
                        position.status = PositionStatus.CLOSED
                        async with self.positions_lock:
                            self._delete_position(position)
                        return

                is_valid, _ = self._check_min_order_value(str(remaining))
                if not is_valid:
                    position.status = PositionStatus.CLOSED
                    async with self.positions_lock:
                        self._delete_position(position)
                    return

                qty_str = self._align_qty(remaining)
                resp = await self.ws_place_order("SELL", "MARKET", qty_str, client_order_id=cl_ord_id)
                tag = "SL" if reason == "stop_loss" else "TO"
                if resp.get("result", {}).get("orderId"):
                    logger.warning(f"[#{self._short_id(position.buy_cl_ord_id)}] [{tag}] 市价单已提交 | qty={qty_str}")
                elif resp.get("error"):
                    logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] [{tag}] 市价单失败: {resp['error']}")
            else:
                position.status = PositionStatus.CLOSED
                async with self.positions_lock:
                    self._delete_position(position)

        except Exception as e:
            logger.error(f"[#{self._short_id(position.buy_cl_ord_id)}] {reason}执行失败: {e}")

    # ==================== 波动率熔断 ====================

    async def _check_volatility(self):
        if not self.config.vol_enabled:
            if self._vol_paused:
                self._vol_paused = False
                logger.info("波动率熔断已关闭(vol_enabled=false)，恢复运行")
            return

        range_pct = self.vol_tracker.range_pct
        ratio = self.vol_tracker.ratio
        safe_range = self.config.vol_safe_range_pct
        max_range = self.config.vol_max_range_pct

        abs_safe = range_pct <= safe_range
        abs_ok = max_range <= 0 or range_pct <= max_range
        rel_ok = abs_safe or not self.vol_tracker.ready or ratio <= (
            self.config.vol_resume_multiplier if self._vol_paused else self.config.vol_pause_multiplier)

        if not self._vol_paused and not (abs_ok and rel_ok):
            self._vol_paused = True
            reasons = []
            if not abs_ok:
                reasons.append(f"振幅{range_pct:.6f}>{max_range}")
            if not rel_ok:
                reasons.append(f"{ratio:.1f}x>{self.config.vol_pause_multiplier}x基准")
            logger.warning(f"波动率熔断: {', '.join(reasons)}")
            await self._volatility_close_all()

        if self._vol_paused and abs_ok and rel_ok:
            self._vol_paused = False
            logger.info(f"波动率熔断解除: {ratio:.1f}x基准, 振幅{range_pct:.6f}")

    async def _volatility_close_all(self):
        async with self.positions_lock:
            for pos in self.positions.values():
                if pos.status in (PositionStatus.OPENING, PositionStatus.OPEN):
                    pos.status = PositionStatus.CLOSING
        await self.rest_cancel_open_orders()
        if self.config.vol_close_on_pause:
            await self._market_close_rest()

    async def _market_close_rest(self):
        for attempt in range(5):
            try:
                avail = await self.rest_get_balance(self.base_asset)
            except Exception as e:
                logger.warning(f"清仓查余额失败: {e}")
                avail = Decimal("0")

            if avail < self.step_size:
                if attempt < 4:
                    await asyncio.sleep(2)
                    continue
                break

            qty = self._align_qty(avail)
            if Decimal(qty) < self.step_size:
                break

            is_valid, _ = self._check_min_order_value(qty)
            if not is_valid:
                break

            logger.info(f"清仓: {qty} {self.base_asset} (attempt {attempt + 1})")
            try:
                result = await self.rest_place_market_sell(qty)
                if result.get("orderId"):
                    logger.info(f"清仓成功 | orderId={result['orderId']}")
                else:
                    logger.warning(f"清仓响应: {result}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"清仓失败: {e}")
                break
        logger.info("清仓流程结束")

    # ==================== 统计打印 ====================

    async def print_stats(self):
        logger.info("统计打印已启动")
        while not self.should_stop:
            await asyncio.sleep(30)
            try:
                s = await self.stats.get_summary()
                async with self.positions_lock:
                    opening_cnt = sum(1 for p in self.positions.values() if p.status == PositionStatus.OPENING)
                    open_cnt = sum(1 for p in self.positions.values() if p.status == PositionStatus.OPEN)
                    closing_cnt = sum(1 for p in self.positions.values() if p.status == PositionStatus.CLOSING)

                logger.info(
                    f"\n{'='*60}\n"
                    f"本次 | 买入: {s['session']['buy']:.2f} | 卖出: {s['session']['sell']:.2f} | "
                    f"总计: {s['session']['total']:.2f} | 订单: {s['session']['count']}\n"
                    f"本次盈亏 | {s['session']['pnl']:.6f} | 手续费: {s['session']['fee']:.6f}\n"
                    f"全局 | 买入: {s['global']['buy']:.2f} | 卖出: {s['global']['sell']:.2f} | "
                    f"总计: {s['global']['total']:.2f}\n"
                    f"全局盈亏 | {s['global']['pnl']:.6f} | 手续费: {s['global']['fee']:.6f}\n"
                    f"持仓 | 活跃: {opening_cnt + open_cnt + closing_cnt}/{self.config.max_order_groups} "
                    f"(等待:{opening_cnt} 开仓:{open_cnt} 平仓:{closing_cnt})\n"
                    f"价格 | bid={self.bid_price} | ask={self.ask_price}\n"
                    f"波动率 | 振幅={self.vol_tracker.current_range:.6f} 基准={self.vol_tracker.baseline:.6f} "
                    f"{self.vol_tracker.ratio:.1f}x{' [已熔断]' if self._vol_paused else ''}\n"
                    f"{'='*60}"
                )
                await self.stats.save_state()
            except Exception as e:
                logger.error(f"打印统计失败: {e}")

    # ==================== 持仓清理 ====================

    async def cleanup_closed_positions(self):
        logger.info("持仓清理已启动")
        while not self.should_stop:
            await asyncio.sleep(180)
            try:
                async with self.positions_lock:
                    removed = stuck = 0
                    for cl_id, p in list(self.positions.items()):
                        if (p.status == PositionStatus.CLOSED and p.close_time and
                                (datetime.now() - p.close_time).total_seconds() > 300):
                            self._delete_position(p)
                            removed += 1
                        elif p.is_closing_stuck():
                            logger.warning(f"[#{self._short_id(cl_id)}] CLOSING卡住，强制删除")
                            self._delete_position(p)
                            stuck += 1
                if removed:
                    logger.info(f"清理了 {removed} 个已关闭持仓")
                if stuck:
                    logger.warning(f"清理了 {stuck} 个卡住的CLOSING持仓")
            except Exception as e:
                logger.error(f"清理持仓失败: {e}")

    # ==================== 热加载配置 ====================

    async def reload_hot_config_loop(self):
        logger.info("配置热加载监控已启动")
        while not self.should_stop:
            try:
                await self._reload_hot_config_once()
            except Exception as e:
                logger.warning(f"热加载配置失败: {e}")
            await asyncio.sleep(5)

    async def _reload_hot_config_once(self):
        if not self.config_path or not self.config_path.exists():
            return
        mtime = self.config_path.stat().st_mtime
        if mtime <= self._last_config_mtime:
            return
        updated = StrategyConfig.from_yaml(str(self.config_path))
        hot_fields = [
            "quantity", "take_profit_offset", "stop_loss_offset", "order_interval",
            "max_volume", "max_order_groups", "take_profit_timeout", "stop_loss_check_interval",
            "auto_close_on_exit", "step", "start_level", "num_levels", "size_growth_factor",
            "max_net_buy", "vol_enabled", "vol_safe_range_pct", "vol_max_range_pct",
            "vol_pause_multiplier", "vol_resume_multiplier", "vol_close_on_pause",
        ]
        changed = {}
        for f in hot_fields:
            old, new = getattr(self.config, f), getattr(updated, f)
            if old != new:
                setattr(self.config, f, new)
                changed[f] = f"{old} -> {new}"
        self._last_config_mtime = mtime
        if changed:
            logger.info("热加载配置成功 | " + " | ".join(f"{k}: {v}" for k, v in changed.items()))

    # ==================== 生命周期 ====================

    async def initialize(self):
        logger.info(f"初始化策略: {self.config.strategy_id}")
        await self.fetch_exchange_info()
        await self._init_ws_api()
        await self._init_ws_streams()
        await self._init_user_data_stream()

        logger.info("等待 BBO 数据...")
        for _ in range(30):
            if self.bid_price > 0:
                break
            await asyncio.sleep(0.5)
        if self.bid_price <= 0:
            logger.warning("BBO 数据未就绪，继续运行...")
        logger.info("初始化完成，策略启动")

    async def run(self):
        try:
            await self.initialize()
            self.strategy_tasks = [
                asyncio.create_task(self.buy_grid_loop()),
                asyncio.create_task(self.monitor_positions()),
                asyncio.create_task(self.print_stats()),
                asyncio.create_task(self.cleanup_closed_positions()),
                asyncio.create_task(self.reload_hot_config_loop()),
            ]
            await asyncio.gather(*self.strategy_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"策略运行错误: {e}")
        finally:
            if self.strategy_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.strategy_tasks, return_exceptions=True), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
            await self.shutdown()

    async def shutdown(self):
        logger.info("正在关闭策略...")
        async with self.positions_lock:
            for pos in self.positions.values():
                if pos.status in (PositionStatus.OPENING, PositionStatus.OPEN):
                    pos.status = PositionStatus.CLOSING

        await self.rest_cancel_open_orders()
        await asyncio.sleep(0.5)

        if self.config.auto_close_on_exit and self.base_asset:
            try:
                avail = await self.rest_get_balance(self.base_asset)
                qty = self._align_qty(avail)
                if Decimal(qty) > 0:
                    is_valid, _ = self._check_min_order_value(qty)
                    if is_valid:
                        logger.info(f"清仓卖出: {qty} {self.base_asset}")
                        await self.rest_place_market_sell(qty)
            except Exception as e:
                logger.warning(f"清仓失败: {e}")

        try:
            await asyncio.wait_for(self.stats.save_state(), timeout=2.0)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

        # 关闭 SDK 连接
        for conn in (self.ws_api_conn, self.ws_streams_conn):
            if conn:
                try:
                    await conn.close_connection()
                except Exception:
                    pass

        logger.info("策略已安全关闭")


# ==================== 入口 ====================

def setup_logger(strategy_id: str):
    logger.remove()
    logger.add(sys.stdout,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
               level="INFO")
    log_file = Path(__file__).parent / f"logs/{strategy_id}_spot_v2.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
               level="DEBUG", rotation="1 day", retention="7 days")


def handle_exception(_loop, context):
    msg = context.get("exception", context["message"])
    if "ConnectionClosed" in str(msg) or "no close frame" in str(msg):
        return
    logger.error(f"未捕获的异步异常: {msg}", exc_info=context.get("exception"))


async def main():
    config_path = os.getenv("CONFIG_FILE", str(Path(__file__).parent / "config.yaml"))
    config = StrategyConfig.from_yaml(config_path)
    config.validate()
    setup_logger(config.strategy_id)

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)

    logger.info(
        f"\n{'='*70}\n"
        f"币安现货刷量策略V2 - 买侧网格 + 止盈止损 (SDK 版)\n"
        f"策略ID: {config.strategy_id} | 交易对: {config.symbol}\n"
        f"数量: {config.quantity} | 止盈: +{config.take_profit_offset} | 止损: -{config.stop_loss_offset}\n"
        f"网格: step={config.step} levels={config.num_levels} start={config.start_level} growth={config.size_growth_factor}\n"
        f"熔断: pause={config.vol_pause_multiplier}x resume={config.vol_resume_multiplier}x close={config.vol_close_on_pause}\n"
        f"最大订单组: {config.max_order_groups} | 累计上限: {config.max_volume}\n"
        f"{'='*70}"
    )

    strategy = SpotVolumeV2(config, config_path)

    def signal_handler(_sig, _frame):
        logger.warning("收到停止信号，正在安全退出...")
        strategy.should_stop = True
        for task in strategy.strategy_tasks:
            task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await strategy.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        strategy.should_stop = True
    except Exception as e:
        logger.error(f"策略异常退出: {e}")
        raise
    finally:
        logger.info("程序已退出")


if __name__ == "__main__":
    asyncio.run(main())
