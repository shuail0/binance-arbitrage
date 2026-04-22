#!/usr/bin/env python3
"""币安现货双向做市策略V2 - 买卖双侧网格 + 库存偏斜 + OBI信号 + 波动率熔断
从 OKX market_maker_v2 移植，通信架构复用 spot_volume_v2 (WS Streams + WS API + REST)
"""
import asyncio
import csv
import json
import math
import os
import signal
import sys
import time
import uuid
from base64 import b64encode
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Optional

import aiohttp
import websockets
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from loguru import logger


# ─── 数据结构 ────────────────────────────────────────────────────────────────────

@dataclass
class ActiveOrder:
    cl_ord_id: str
    order_id: str
    side: str           # BUY / SELL
    price: Decimal
    quantity: Decimal
    filled_qty: Decimal = Decimal("0")
    cum_fee: Decimal = Decimal("0")
    fee_ccy: str = ""
    pending: bool = False   # 下单/撤单请求已发出但未确认
    sent_at: float = 0


@dataclass
class TargetOrder:
    side: str           # BUY / SELL
    price: Decimal
    quantity: Decimal


# ─── 统计 ────────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self, instrument_id: str):
        self.instrument_id = instrument_id
        self.state_file = Path(__file__).parent / "volume_state.json"
        self.session_buy_volume = self.session_sell_volume = self.session_pnl = Decimal("0")
        self.session_fee_base = self.session_fee_quote = Decimal("0")
        self.session_order_count = 0
        self.global_buy_volume = self.global_sell_volume = self.global_pnl = Decimal("0")
        self.global_fee_base = self.global_fee_quote = Decimal("0")
        self.dirty = False
        self.last_save_time = time.time()
        self._load()

    def _load(self):
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text()).get(self.instrument_id, {})
            self.global_buy_volume = Decimal(data.get("global_buy_volume", "0"))
            self.global_sell_volume = Decimal(data.get("global_sell_volume", "0"))
            self.global_pnl = Decimal(data.get("global_pnl", "0"))
            self.global_fee_base = Decimal(data.get("global_fee_base", "0"))
            self.global_fee_quote = Decimal(data.get("global_fee_quote", "0"))
            logger.info(f"已加载全局统计 [{self.instrument_id}]: "
                        f"买入={self.global_buy_volume:.2f} | 卖出={self.global_sell_volume:.2f} | "
                        f"P&L={self.global_pnl:.6f}")
        except Exception as e:
            logger.warning(f"加载统计失败: {e}")

    def _save(self):
        try:
            all_data = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
            all_data[self.instrument_id] = {
                "global_buy_volume": str(self.global_buy_volume),
                "global_sell_volume": str(self.global_sell_volume),
                "global_pnl": str(self.global_pnl),
                "global_fee_base": str(self.global_fee_base),
                "global_fee_quote": str(self.global_fee_quote),
                "updated_at": datetime.now().isoformat(),
            }
            self.state_file.write_text(json.dumps(all_data, indent=2))
            self.dirty = False
            self.last_save_time = time.time()
        except Exception as e:
            logger.warning(f"保存统计失败: {e}")

    def add(self, *, buy_vol: Decimal = None, sell_vol: Decimal = None,
            pnl: Decimal = None, fee: Decimal = None, fee_ccy: str = ""):
        if buy_vol:
            self.session_buy_volume += buy_vol
            self.global_buy_volume += buy_vol
            self.session_order_count += 1
        if sell_vol:
            self.session_sell_volume += sell_vol
            self.global_sell_volume += sell_vol
        if pnl is not None:
            self.session_pnl += pnl
            self.global_pnl += pnl
        if fee and fee_ccy:
            if fee_ccy.upper() in ("USDT", "USDC", "USD"):
                self.session_fee_quote += fee
                self.global_fee_quote += fee
            else:
                self.session_fee_base += fee
                self.global_fee_base += fee
        self.dirty = True
        if time.time() - self.last_save_time > 5 or self.session_order_count % 10 == 0:
            self._save()

    def save(self):
        if self.dirty:
            self._save()

    def is_max_reached(self, max_vol: float) -> bool:
        return max_vol > 0 and self.global_buy_volume + self.global_sell_volume >= Decimal(str(max_vol))


# ─── 波动率 ──────────────────────────────────────────────────────────────────────

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


# ─── 策略主类 ────────────────────────────────────────────────────────────────────

class MarketMaker:
    def __init__(self, instrument_id: str, api_key: str, private_key_path: str, demo: bool = False):
        self.instrument_id = instrument_id
        self.api_key = api_key
        self.demo = demo
        self.should_stop = False

        # 私钥
        if not private_key_path or not os.path.exists(private_key_path):
            raise FileNotFoundError(f"私钥文件不存在: {private_key_path}")
        self._private_key: Ed25519PrivateKey = load_pem_private_key(
            Path(private_key_path).read_bytes(), password=None)

        # URL
        if demo:
            self.rest_base = "https://demo-api.binance.com"
            self.ws_api_url = "wss://demo-ws-api.binance.com:443/ws-api/v3"
            self.ws_stream_url = "wss://demo-stream.binance.com:9443"
        else:
            self.rest_base = "https://api.binance.com"
            self.ws_api_url = "wss://ws-api.binance.com:443/ws-api/v3"
            self.ws_stream_url = "wss://stream.binance.com:9443"

        # 市场数据
        self.bid_price = self.ask_price = Decimal("0")
        self.tick_size = Decimal("0.01")
        self.step_size = Decimal("0.001")
        self.min_notional = Decimal("5")
        self.base_asset = ""
        self.quote_asset = ""

        # 深度数据 (OBI 计算)
        self.depth_bids: list = []  # [[price_str, qty_str], ...]
        self.depth_asks: list = []

        # 订单管理: cl_ord_id → ActiveOrder
        self.orders: dict[str, ActiveOrder] = {}

        # 持仓跟踪 (现金流法)
        self.net_filled_qty = Decimal("0")   # 净持仓 (买+卖-)
        self.buy_volume = Decimal("0")       # 累计买入成交额(USDT)
        self.sell_volume = Decimal("0")      # 累计卖出成交额(USDT)
        self.available_qty = Decimal("0")    # 账户可用base余额(REST校准)

        # 统计
        self.stats: Optional[Stats] = None
        self._csv_file = None
        self._csv_writer = None

        # WS API
        self.ws_api: Optional[websockets.WebSocketClientProtocol] = None
        self.ws_api_responses: dict[str, asyncio.Future] = {}
        self.ws_api_id_counter = 0

        # HTTP session (持久化, 复用 TCP+TLS)
        self._http_session: Optional[aiohttp.ClientSession] = None

        # 波动率
        self.vol_tracker = VolatilityTracker()
        self._vol_paused = False

        # User Data Stream
        self.listen_key: Optional[str] = None

        # 参数 (热加载)
        self.params: dict = {}
        self._params_path = Path(__file__).parent / "params.yaml"
        self._params_mtime: float = 0

        # 控制
        self.strategy_tasks: list = []
        self._loop_count = 0
        self._start_time = time.time()

    # ─── 签名 ────────────────────────────────────────────────────────────────

    def _sign(self, payload: str) -> str:
        return b64encode(self._private_key.sign(payload.encode())).decode()

    def _sign_params(self, params: dict) -> dict:
        params["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        params["signature"] = self._sign(query)
        return params

    # ─── 工具函数 ────────────────────────────────────────────────────────────

    def _align_price(self, price: Decimal, side: str = "BUY") -> Decimal:
        if self.tick_size <= 0:
            return price
        rounding = ROUND_DOWN if side == "BUY" else ROUND_UP
        return (price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size

    def _align_qty(self, qty: Decimal) -> Decimal:
        if self.step_size <= 0:
            return qty
        return (qty / self.step_size).to_integral_value(rounding=ROUND_DOWN) * self.step_size

    def _price_str(self, price: Decimal) -> str:
        return str(price)

    def _qty_str(self, qty: Decimal) -> str:
        return str(qty)

    @staticmethod
    def _safe_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
        if value is None or value == "":
            return default
        try:
            return Decimal(str(value))
        except (ValueError, TypeError, ArithmeticError):
            return default

    @staticmethod
    def _short_id(cid: str) -> str:
        return cid[-8:] if len(cid) > 8 else cid

    def _new_cid(self) -> str:
        return f"mm{uuid.uuid4().hex[:16]}"

    def _check_min_notional(self, qty: Decimal, price: Decimal = None) -> bool:
        return (qty * (price or self.bid_price)) >= self.min_notional

    # ─── 参数加载 (热加载) ───────────────────────────────────────────────────

    def _load_params(self):
        try:
            mtime = self._params_path.stat().st_mtime
            if mtime <= self._params_mtime:
                return
            with open(self._params_path) as f:
                raw = yaml.safe_load(f)
            old_params = self.params.copy()
            self.params = raw.get("strategy", {})
            self._params_mtime = mtime
            if old_params:
                changed = {k: f"{old_params.get(k)} -> {v}" for k, v in self.params.items()
                           if old_params.get(k) != v}
                if changed:
                    logger.info("热加载配置 | " + " | ".join(f"{k}: {v}" for k, v in changed.items()))
        except Exception as e:
            logger.warning(f"加载参数失败: {e}")

    # ─── REST ────────────────────────────────────────────────────────────────

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """懒加载持久 HTTP session, 复用 TCP+TLS 连接"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def _rest_request(self, method: str, path: str, params: dict = None, signed: bool = False) -> dict:
        url = f"{self.rest_base}{path}"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = params or {}
        if signed:
            params = self._sign_params(params)
        session = await self._get_http_session()
        async with session.request(method, url, params=params, headers=headers) as resp:
            return await resp.json()

    async def _fetch_exchange_info(self):
        data = await self._rest_request("GET", "/api/v3/exchangeInfo", {"symbol": self.instrument_id})
        if not data.get("symbols"):
            raise RuntimeError(f"交易对 {self.instrument_id} 未找到")
        sym = data["symbols"][0]
        self.base_asset = sym["baseAsset"]
        self.quote_asset = sym["quoteAsset"]
        for f in sym["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self.tick_size = Decimal(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                self.step_size = Decimal(f["stepSize"])
            elif f["filterType"] == "NOTIONAL":
                self.min_notional = Decimal(f.get("minNotional", "5"))
        logger.info(f"交易规则 | {self.instrument_id} | base={self.base_asset} quote={self.quote_asset} | "
                    f"tick={self.tick_size} | step={self.step_size} | minNotional={self.min_notional}")

    async def _rest_get_account(self) -> dict:
        return await self._rest_request("GET", "/api/v3/account", signed=True)

    async def _rest_get_balance(self, asset: str) -> tuple[Decimal, Decimal]:
        """返回 (free, locked)"""
        data = await self._rest_get_account()
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return Decimal(b["free"]), Decimal(b.get("locked", "0"))
        return Decimal("0"), Decimal("0")

    async def _rest_place_market_sell(self, qty: str) -> dict:
        return await self._rest_request("POST", "/api/v3/order", {
            "symbol": self.instrument_id, "side": "SELL", "type": "MARKET", "quantity": qty,
        }, signed=True)

    async def _rest_cancel_open_orders(self):
        return await self._rest_request("DELETE", "/api/v3/openOrders", {
            "symbol": self.instrument_id,
        }, signed=True)

    async def _rest_get_open_orders(self) -> list:
        data = await self._rest_request("GET", "/api/v3/openOrders", {
            "symbol": self.instrument_id,
        }, signed=True)
        return data if isinstance(data, list) else []

    # ─── Listen Key ──────────────────────────────────────────────────────────

    async def _get_listen_key(self) -> str:
        data = await self._rest_request("POST", "/api/v3/userDataStream", signed=False)
        return data["listenKey"]

    async def _keepalive_listen_key_loop(self):
        while not self.should_stop:
            try:
                await asyncio.sleep(30 * 60)
                if self.listen_key:
                    await self._rest_request("PUT", "/api/v3/userDataStream", {"listenKey": self.listen_key})
                    logger.debug("listenKey keepalive 完成")
            except Exception as e:
                logger.warning(f"listenKey keepalive 失败: {e}")

    # ─── WS Streams ──────────────────────────────────────────────────────────

    async def _run_market_streams(self):
        """bookTicker + depth5@100ms + ticker"""
        sym = self.instrument_id.lower()
        url = f"{self.ws_stream_url}/stream?streams={sym}@bookTicker/{sym}@depth5@100ms/{sym}@ticker"
        while not self.should_stop:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info("WS Streams 已连接 | bookTicker + depth5 + ticker")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        stream = msg.get("stream", "")
                        if "bookTicker" in stream:
                            self.bid_price = Decimal(data["b"])
                            self.ask_price = Decimal(data["a"])
                        elif "depth5" in stream:
                            self.depth_bids = data.get("bids", [])
                            self.depth_asks = data.get("asks", [])
                        elif "ticker" in stream:
                            last = float(data.get("c", 0))
                            if last > 0:
                                self.vol_tracker.on_price(last)
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"WS Streams 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    async def _run_user_data_stream(self):
        while not self.should_stop:
            try:
                self.listen_key = await self._get_listen_key()
                url = f"{self.ws_stream_url}/ws/{self.listen_key}"
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info("User Data Stream 已连接")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        if msg.get("e") == "executionReport":
                            asyncio.create_task(self._handle_execution_report(msg))
                        elif msg.get("e") == "outboundAccountPosition":
                            self._handle_account_update(msg)
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"User Data Stream 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    def _handle_account_update(self, msg: dict):
        """outboundAccountPosition 更新可用余额"""
        for b in msg.get("B", []):
            if b.get("a") == self.base_asset:
                free = Decimal(b.get("f", "0"))
                locked = Decimal(b.get("l", "0"))
                self.available_qty = free + locked

    # ─── WS API ──────────────────────────────────────────────────────────────

    async def _run_ws_api(self):
        while not self.should_stop:
            try:
                async with websockets.connect(self.ws_api_url, ping_interval=20, max_size=2**22) as ws:
                    self.ws_api = ws
                    logger.info("WS API 已连接")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        req_id = msg.get("id")
                        if req_id and req_id in self.ws_api_responses:
                            fut = self.ws_api_responses.pop(req_id)
                            if not fut.done():
                                fut.set_result(msg)
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                self.ws_api = None
                logger.warning(f"WS API 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    async def _ws_api_request(self, method: str, params: dict, timeout: float = 10) -> dict:
        if not self.ws_api:
            raise RuntimeError("WS API 未连接")
        self.ws_api_id_counter += 1
        req_id = f"req_{self.ws_api_id_counter}"
        params = self._sign_params(params)
        payload = {"id": req_id, "method": method, "params": {**params, "apiKey": self.api_key}}
        fut = asyncio.get_running_loop().create_future()
        self.ws_api_responses[req_id] = fut
        await self.ws_api.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.ws_api_responses.pop(req_id, None)
            raise

    # ─── 下单/撤单 ──────────────────────────────────────────────────────────

    async def _place_order(self, side: str, price: Decimal, qty: Decimal, cl_ord_id: str) -> Optional[dict]:
        params = {
            "symbol": self.instrument_id,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self._qty_str(qty),
            "price": self._price_str(price),
            "newClientOrderId": cl_ord_id,
        }
        try:
            resp = await self._ws_api_request("order.place", params)
            return resp
        except asyncio.TimeoutError:
            logger.warning(f"下单超时 | {cl_ord_id}")
            return None
        except Exception as e:
            logger.error(f"下单异常 | {cl_ord_id} | {e}")
            return None

    async def _cancel_order(self, order: ActiveOrder) -> bool:
        params = {"symbol": self.instrument_id}
        if order.order_id:
            params["orderId"] = order.order_id
        else:
            params["origClientOrderId"] = order.cl_ord_id
        try:
            resp = await self._ws_api_request("order.cancel", params, timeout=5)
            return resp.get("status") != -1
        except asyncio.TimeoutError:
            logger.warning(f"撤单超时 | {self._short_id(order.cl_ord_id)}")
            return False
        except Exception as e:
            logger.warning(f"撤单异常 | {self._short_id(order.cl_ord_id)} | {e}")
            return False

    async def _cancel_all_orders(self):
        """撤销所有策略挂单"""
        orders_to_cancel = [o for o in self.orders.values() if not o.pending]
        if not orders_to_cancel:
            return
        tasks = [self._cancel_order(o) for o in orders_to_cancel]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _cancel_all_rest(self):
        """REST 批量撤单兜底"""
        try:
            await self._rest_cancel_open_orders()
            logger.info("REST 批量撤单完成")
        except Exception as e:
            logger.warning(f"REST 批量撤单失败: {e}")

    # ─── executionReport 处理 ────────────────────────────────────────────────

    async def _handle_execution_report(self, msg: dict):
        try:
            symbol = msg.get("s", "")
            if symbol != self.instrument_id:
                return

            cl_ord_id = msg.get("c", "")
            order_id = str(msg.get("i", ""))
            exec_type = msg.get("x", "")       # NEW / TRADE / CANCELED / EXPIRED
            order_status = msg.get("X", "")     # NEW / PARTIALLY_FILLED / FILLED / CANCELED
            side = msg.get("S", "")             # BUY / SELL
            cum_filled_qty = self._safe_decimal(msg.get("z"))
            last_filled_price = self._safe_decimal(msg.get("L"))
            last_filled_qty = self._safe_decimal(msg.get("l"))
            commission = self._safe_decimal(msg.get("n"))
            commission_asset = msg.get("N", "")
            cum_quote_qty = self._safe_decimal(msg.get("Z"))

            order = self.orders.get(cl_ord_id)

            # 不属于本策略的订单 (如清仓单)
            if not order:
                if cl_ord_id.startswith("lq") and exec_type == "TRADE" and last_filled_qty > 0:
                    vol = last_filled_price * last_filled_qty
                    self.sell_volume += vol
                    self.net_filled_qty -= last_filled_qty
                    self.stats.add(sell_vol=vol, fee=commission, fee_ccy=commission_asset)
                    logger.info(f"[清仓] 成交 | price={last_filled_price} qty={last_filled_qty} amount={vol:.2f}")
                    self._save_state()
                return

            # NEW: 确认活跃
            if exec_type == "NEW":
                order.order_id = order_id
                order.pending = False
                return

            # TRADE: 部分或全部成交
            if exec_type == "TRADE" and last_filled_qty > 0:
                self._update_position(order, last_filled_qty, last_filled_price,
                                      commission, commission_asset, cum_quote_qty, cum_filled_qty)

            # 终态
            if order_status in ("FILLED", "CANCELED", "EXPIRED"):
                order.filled_qty = cum_filled_qty
                order.pending = False
                # 终态统计
                if cum_filled_qty > 0:
                    vol = cum_quote_qty
                    fee_info = f"{order.cum_fee} {order.fee_ccy}" if order.cum_fee > 0 else ""
                    if side == "BUY":
                        self.stats.add(buy_vol=vol, fee=order.cum_fee, fee_ccy=order.fee_ccy)
                        logger.info(f"[#{self._short_id(cl_ord_id)}] 买单{order_status} | "
                                    f"price={cum_quote_qty / cum_filled_qty if cum_filled_qty else 0:.6f} | "
                                    f"qty={cum_filled_qty} | amount={vol:.2f} | {fee_info}")
                    else:
                        # 卖单盈亏用现金流法
                        pnl = Decimal("0")  # 单笔pnl在总P&L中体现
                        self.stats.add(sell_vol=vol, pnl=pnl, fee=order.cum_fee, fee_ccy=order.fee_ccy)
                        logger.info(f"[#{self._short_id(cl_ord_id)}] 卖单{order_status} | "
                                    f"price={cum_quote_qty / cum_filled_qty if cum_filled_qty else 0:.6f} | "
                                    f"qty={cum_filled_qty} | amount={vol:.2f} | {fee_info}")
                    self._write_trade_csv(cl_ord_id, side, order_status, cum_filled_qty, cum_quote_qty, order.cum_fee, order.fee_ccy)
                self.orders.pop(cl_ord_id, None)
                self._save_state()

        except Exception as e:
            logger.error(f"处理 executionReport 失败: {e}", exc_info=True)

    def _update_position(self, order: ActiveOrder, fill_qty: Decimal, fill_price: Decimal,
                         commission: Decimal, commission_asset: str, cum_quote_qty: Decimal, cum_filled_qty: Decimal):
        """更新净持仓和统计"""
        fee_incr = max(commission - order.cum_fee, Decimal("0"))
        order.cum_fee = commission
        order.fee_ccy = commission_asset

        actual_qty = fill_qty
        # 现货买入时 base 扣手续费
        if order.side == "BUY" and fee_incr > 0 and commission_asset == self.base_asset:
            actual_qty = max(fill_qty - fee_incr, Decimal("0"))

        if order.side == "BUY":
            self.net_filled_qty += actual_qty
            self.buy_volume += fill_price * fill_qty
        else:
            self.net_filled_qty -= fill_qty
            self.sell_volume += fill_price * fill_qty

        order.filled_qty = cum_filled_qty

    # ─── 盈亏计算 (现金流法) ────────────────────────────────────────────────

    def _calc_pnl(self) -> tuple[Decimal, Decimal, Decimal]:
        """返回 (realized_pnl, unrealized_pnl, total_pnl)"""
        realized = self.sell_volume - self.buy_volume
        mid = (self.bid_price + self.ask_price) / 2 if self.bid_price > 0 and self.ask_price > 0 else Decimal("0")
        unrealized = self.net_filled_qty * mid
        return realized, unrealized, realized + unrealized

    def _can_profitable_exit(self) -> bool:
        """现金流法: 如果全部按ask卖出能盈利"""
        if self.net_filled_qty <= 0 or self.ask_price <= 0:
            return False
        realized = self.sell_volume - self.buy_volume
        return (realized + self.net_filled_qty * self.ask_price) >= 0

    # ─── OBI 计算 ────────────────────────────────────────────────────────────

    def _calc_imbalance(self, depth: int = 5) -> float:
        """前N档 bid/ask 量比，>1偏多头，<1偏空头"""
        bid_vol = sum(float(b[1]) for b in self.depth_bids[:depth]) if self.depth_bids else 0
        ask_vol = sum(float(a[1]) for a in self.depth_asks[:depth]) if self.depth_asks else 0
        return bid_vol / ask_vol if ask_vol > 0 else 1.0

    # ─── 网格计算 + Diff ────────────────────────────────────────────────────

    def _decide_orders(self) -> tuple[list[TargetOrder], list[TargetOrder]]:
        """计算目标买卖网格，返回 (target_buys, target_sells)"""
        if self.bid_price <= 0 or self.ask_price <= 0:
            return [], []

        step = Decimal(str(self.params.get("step", "0.01")))
        start_level = int(self.params.get("start_level", 0))
        num_orders = int(self.params.get("num_of_order_each_side", 5))
        max_net_buy = Decimal(str(self.params.get("maximum_net_buy", "10")))
        max_net_sell = Decimal(str(self.params.get("maximum_net_sell", "10")))
        single_size = Decimal(str(self.params.get("single_size", "0.3")))
        size_growth = Decimal(str(self.params.get("size_growth_factor", "0")))
        imbalance_depth = int(self.params.get("imbalance_depth", 5))
        imbalance_threshold = float(self.params.get("imbalance_threshold", 1.5))
        imbalance_level_scale = float(self.params.get("imbalance_level_scale", 0.6))
        exit_on_profit = self.params.get("exit_on_profit", False)
        exit_net_pct = Decimal(str(self.params.get("exit_net_pct", 0)))

        single_size = max(self._align_qty(single_size), self.step_size)

        # 库存偏斜
        buy_levels = num_orders
        sell_levels = num_orders
        if self.net_filled_qty > 0 and max_net_buy > 0:
            buy_levels = math.ceil(num_orders * float(max(1 - self.net_filled_qty / max_net_buy, 0)))
        if self.net_filled_qty < 0 and max_net_sell > 0:
            sell_levels = math.ceil(num_orders * float(max(1 + self.net_filled_qty / max_net_sell, 0)))

        # OBI 偏斜
        imbalance = self._calc_imbalance(imbalance_depth)
        if imbalance > imbalance_threshold:
            sell_levels = max(1, int(sell_levels * imbalance_level_scale))
        elif imbalance_threshold > 0 and imbalance < 1 / imbalance_threshold:
            buy_levels = max(1, int(buy_levels * imbalance_level_scale))

        # 加速出货判断
        profitable_exit = self._can_profitable_exit()
        overloaded = (exit_net_pct > 0 and max_net_buy > 0 and
                      self.net_filled_qty > 0 and self.net_filled_qty / max_net_buy >= exit_net_pct)
        exit_all = (exit_on_profit and profitable_exit) or overloaded

        # 买侧网格
        target_buys = []
        for i in range(buy_levels):
            level = i + start_level
            px = self._align_price(self.bid_price - step * level, "BUY")
            qty = self._align_qty(single_size * (1 + size_growth * i))
            if px > 0 and qty >= self.step_size and self._check_min_notional(qty, px):
                target_buys.append(TargetOrder(side="BUY", price=px, quantity=qty))

        # 卖侧网格
        target_sells = []
        if exit_all and self.net_filled_qty > 0:
            # 加速出货: 集中挂卖一
            sell_px = self._align_price(self.ask_price + step * start_level, "SELL")
            sell_qty = self._align_qty(min(self.net_filled_qty, self.available_qty))
            if sell_qty >= self.step_size and self._check_min_notional(sell_qty, sell_px):
                target_sells.append(TargetOrder(side="SELL", price=sell_px, quantity=sell_qty))
        else:
            remaining_avail = self.available_qty
            # 扣除已挂卖单的冻结量 (available_qty 已含 locked)
            for i in range(sell_levels):
                level = i + start_level
                px = self._align_price(self.ask_price + step * level, "SELL")
                qty = self._align_qty(single_size * (1 + size_growth * i))
                # 卖单不超过可用余额
                qty = min(qty, self._align_qty(remaining_avail))
                if qty < self.step_size or not self._check_min_notional(qty, px):
                    break
                target_sells.append(TargetOrder(side="SELL", price=px, quantity=qty))
                remaining_avail -= qty
                if remaining_avail < self.step_size:
                    break

        return target_buys, target_sells

    def _diff_orders(self, proposed: list[TargetOrder], side: str) -> tuple[list[TargetOrder], list[ActiveOrder]]:
        """
        Diff 算法 (不支持 amend，直接撤+补):
        Pass 1: 精确匹配 (price + remaining qty) → 保留
        Pass 2: 剩余 current → 全部撤单
        Pass 3: 剩余 proposed → 全部新下单
        返回 (to_place, to_cancel)
        """
        current = [o for o in self.orders.values() if o.side == side and not o.pending]

        # Pass 1: 精确匹配
        proposed_copy = list(proposed)
        matched_current = set()
        matched_proposed = set()
        for ci, cur in enumerate(current):
            remaining = cur.quantity - cur.filled_qty
            remaining = self._align_qty(remaining)
            for pi, prop in enumerate(proposed_copy):
                if pi in matched_proposed:
                    continue
                if cur.price == prop.price and remaining == prop.quantity:
                    matched_current.add(ci)
                    matched_proposed.add(pi)
                    break

        # Pass 2 + 3
        to_cancel = [cur for ci, cur in enumerate(current) if ci not in matched_current]
        to_place = [prop for pi, prop in enumerate(proposed_copy) if pi not in matched_proposed]

        return to_place, to_cancel

    # ─── 策略主循环 ─────────────────────────────────────────────────────────

    async def _strategy_loop(self):
        while not self.should_stop:
            try:
                self._loop_count += 1
                self._load_params()
                loop_interval = float(self.params.get("loop_interval", 0.5))

                # 最大成交额检查
                max_vol = float(self.params.get("max_volume", 0))
                if self.stats.is_max_reached(max_vol):
                    logger.warning(f"累计成交额达到上限: {max_vol}, 触发退出")
                    os.kill(os.getpid(), signal.SIGINT)
                    break

                # 等待 BBO
                if self.bid_price <= 0 or self.ask_price <= 0:
                    await asyncio.sleep(1)
                    continue

                # 波动率熔断
                if self.params.get("vol_enabled", True):
                    self._check_volatility()
                    if self._vol_paused:
                        await asyncio.sleep(loop_interval)
                        continue
                elif self._vol_paused:
                    self._vol_paused = False
                    logger.info("波动率熔断已关闭(vol_enabled=false)，恢复运行")

                # 计算目标
                target_buys, target_sells = self._decide_orders()

                # Diff 买侧
                buy_place, buy_cancel = self._diff_orders(target_buys, "BUY")
                # Diff 卖侧
                sell_place, sell_cancel = self._diff_orders(target_sells, "SELL")

                # 先撤后下
                all_cancel = buy_cancel + sell_cancel
                all_place = buy_place + sell_place

                if all_cancel:
                    cancel_tasks = []
                    for order in all_cancel:
                        order.pending = True
                        cancel_tasks.append(self._cancel_order(order))
                    results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    for order, result in zip(all_cancel, results):
                        if result is True:
                            # 撤单成功, 从本地移除
                            self.orders.pop(order.cl_ord_id, None)
                        else:
                            # 撤单失败或异常: 保留订单, 清 pending 以便下轮重试撤单
                            order.pending = False

                if all_place:
                    place_cids = []
                    place_tasks = []
                    for target in all_place:
                        cid = self._new_cid()
                        place_cids.append(cid)
                        order = ActiveOrder(
                            cl_ord_id=cid, order_id="", side=target.side,
                            price=target.price, quantity=target.quantity,
                            pending=True, sent_at=time.time(),
                        )
                        self.orders[cid] = order
                        place_tasks.append(self._place_order(target.side, target.price, target.quantity, cid))
                    results = await asyncio.gather(*place_tasks, return_exceptions=True)
                    for cid, target, result in zip(place_cids, all_place, results):
                        if result is None or isinstance(result, Exception):
                            self.orders.pop(cid, None)
                        elif isinstance(result, dict):
                            r = result.get("result", {})
                            if r.get("orderId") and cid in self.orders:
                                self.orders[cid].order_id = str(r["orderId"])
                                self.orders[cid].pending = False
                            elif result.get("error"):
                                err = result["error"]
                                logger.debug(f"下单跳过 | {target.side} {target.quantity}@{target.price} | {err.get('msg', err)}")
                                self.orders.pop(cid, None)

                if all_cancel or all_place:
                    bc = sum(1 for o in self.orders.values() if o.side == "BUY")
                    sc = sum(1 for o in self.orders.values() if o.side == "SELL")
                    logger.debug(f"BBO: {self.bid_price}/{self.ask_price} | "
                                 f"挂单: {bc}B/{sc}S | 下{len(all_place)}撤{len(all_cancel)}")

                await asyncio.sleep(loop_interval)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.should_stop:
                    return
                logger.error(f"策略循环异常: {e}", exc_info=True)
                try:
                    await self._cancel_all_rest()
                except Exception:
                    pass
                self.orders.clear()
                await asyncio.sleep(10)

    # ─── 波动率熔断 ─────────────────────────────────────────────────────────

    def _check_volatility(self):
        max_range_pct = float(self.params.get("vol_max_range_pct", 0))
        pause_mult = float(self.params.get("vol_pause_multiplier", 3.0))
        resume_mult = float(self.params.get("vol_resume_multiplier", 1.5))
        safe_range_pct = float(self.params.get("vol_safe_range_pct", 0.001))
        range_pct = self.vol_tracker.range_pct
        ratio = self.vol_tracker.ratio

        abs_safe = range_pct <= safe_range_pct
        abs_ok = max_range_pct <= 0 or range_pct <= max_range_pct
        rel_ok = abs_safe or not self.vol_tracker.ready or ratio <= (
            resume_mult if self._vol_paused else pause_mult)

        if not self._vol_paused and not (abs_ok and rel_ok):
            self._vol_paused = True
            reasons = []
            if not abs_ok:
                reasons.append(f"振幅{range_pct:.6f}>{max_range_pct}")
            if not rel_ok:
                reasons.append(f"{ratio:.1f}x>{pause_mult}x基准")
            vol_close = self.params.get("vol_close_on_pause", False)
            logger.warning(f"波动率熔断: {', '.join(reasons)}" + ("，清仓" if vol_close else ""))
            asyncio.create_task(self._volatility_close_all())

        if self._vol_paused and abs_ok and rel_ok:
            self._vol_paused = False
            logger.info(f"波动率熔断解除: {ratio:.1f}x基准, 振幅{range_pct:.6f}")

    async def _volatility_close_all(self):
        """熔断: 撤单 + 可选市价清仓"""
        try:
            await self._cancel_all_rest()
        except Exception as e:
            logger.warning(f"熔断撤单失败: {e}")
        self.orders.clear()

        if self.params.get("vol_close_on_pause", False):
            await self._market_close_rest()

    async def _market_close_rest(self):
        """REST 查余额 → 市价清仓"""
        for attempt in range(5):
            try:
                free, locked = await self._rest_get_balance(self.base_asset)
                avail = free
            except Exception as e:
                logger.warning(f"清仓查余额失败: {e}")
                avail = Decimal("0")

            if avail < self.step_size:
                if attempt < 4:
                    await asyncio.sleep(2)
                    continue
                break

            qty = self._align_qty(avail)
            if qty < self.step_size or not self._check_min_notional(qty):
                break

            logger.info(f"清仓: {qty} {self.base_asset} (attempt {attempt + 1})")
            try:
                cid = f"lq{uuid.uuid4().hex[:16]}"
                result = await self._rest_place_market_sell(self._qty_str(qty))
                if result.get("orderId"):
                    logger.info(f"清仓成功 | orderId={result['orderId']}")
                else:
                    logger.warning(f"清仓响应: {result}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"清仓失败: {e}")
                break
        logger.info("清仓流程结束")

    # ─── 订单校准 ────────────────────────────────────────────────────────────

    async def _reconcile_orders(self):
        """REST 查询真实挂单，清理本地幽灵订单"""
        try:
            exchange_orders = await self._rest_get_open_orders()
            exchange_ids = {str(o["orderId"]) for o in exchange_orders}
            exchange_cids = {o.get("clientOrderId", "") for o in exchange_orders}

            stale = [cid for cid, o in self.orders.items()
                     if not o.pending and o.order_id and o.order_id not in exchange_ids
                     and o.cl_ord_id not in exchange_cids
                     and time.time() - o.sent_at > 10]
            for cid in stale:
                logger.warning(f"清理幽灵订单 | {self._short_id(cid)}")
                self.orders.pop(cid, None)

            if stale:
                logger.info(f"订单校准: 清理 {len(stale)} 笔幽灵订单")
        except Exception as e:
            logger.warning(f"订单校准失败: {e}")

    # ─── REST 轮询 (余额校准) ───────────────────────────────────────────────

    async def _rest_polling_loop(self):
        """定期获取余额 + 订单校准"""
        while not self.should_stop:
            try:
                free, locked = await self._rest_get_balance(self.base_asset)
                self.available_qty = free + locked

                # 每 5 分钟对账一次
                if self._loop_count % 600 == 0:
                    await self._reconcile_orders()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"REST 轮询异常: {e}")
            await asyncio.sleep(10)

    # ─── 统计打印 ────────────────────────────────────────────────────────────

    async def _print_stats_loop(self):
        while not self.should_stop:
            await asyncio.sleep(30)
            try:
                s = self.stats
                s_total = s.session_buy_volume + s.session_sell_volume
                g_total = s.global_buy_volume + s.global_sell_volume
                bc = sum(1 for o in self.orders.values() if o.side == "BUY")
                sc = sum(1 for o in self.orders.values() if o.side == "SELL")
                realized, unrealized, total_pnl = self._calc_pnl()

                elapsed = int(time.time() - self._start_time)
                h, m, sec = elapsed // 3600, elapsed % 3600 // 60, elapsed % 60
                runtime = f"{h}h{m:02d}m{sec:02d}s" if h else f"{m}m{sec:02d}s"

                logger.info(
                    f"\n{'='*60}\n"
                    f"===== 统计 ({runtime}) =====\n"
                    f"本次 | 买入: {s.session_buy_volume:.2f} | 卖出: {s.session_sell_volume:.2f} | "
                    f"总计: {s_total:.2f} | 订单: {s.session_order_count}\n"
                    f"手续费 | {s.session_fee_base:.8f} {self.base_asset} + {s.session_fee_quote:.6f} {self.quote_asset}\n"
                    f"全局 | 买入: {s.global_buy_volume:.2f} | 卖出: {s.global_sell_volume:.2f} | "
                    f"总计: {g_total:.2f}\n"
                    f"P&L | 已实现: {realized:.4f} | 未实现: {unrealized:.4f} | 总计: {total_pnl:.4f}\n"
                    f"挂单 | 买: {bc} | 卖: {sc} | 净持仓: {self.net_filled_qty}\n"
                    f"余额 | {self.base_asset}: {self.available_qty}\n"
                    f"价格 | bid={self.bid_price} ask={self.ask_price}\n"
                    f"OBI | {self._calc_imbalance():.2f} | "
                    f"波动率: {self.vol_tracker.ratio:.1f}x (pct={self.vol_tracker.range_pct:.6f})"
                    f"{' [已熔断]' if self._vol_paused else ''}\n"
                    f"{'='*60}"
                )
                s.save()
            except Exception as e:
                logger.debug(f"打印统计失败: {e}")

    # ─── 健康监控 ────────────────────────────────────────────────────────────

    async def _health_monitor(self):
        _no_bbo_count = 0
        while not self.should_stop:
            await asyncio.sleep(30)
            if self.bid_price > 0:
                _no_bbo_count = 0
                # 清理超时的 pending 订单
                stale_pending = [cid for cid, o in self.orders.items()
                                 if o.pending and time.time() - o.sent_at > 30]
                for cid in stale_pending:
                    logger.warning(f"清理超时pending | {self._short_id(cid)}")
                    self.orders.pop(cid, None)
            else:
                _no_bbo_count += 1
                if _no_bbo_count >= 3:
                    logger.warning(f"BBO 持续 {_no_bbo_count * 30}s 未收到")
                    _no_bbo_count = 0

    # ─── 成交记录 CSV ───────────────────────────────────────────────────────

    def _init_trade_csv(self):
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        csv_path = logs_dir / f"trades_{self.instrument_id}_{datetime.now():%Y%m%d_%H%M%S}.csv"
        self._csv_file = open(csv_path, "a", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow([
            "time", "cl_ord_id", "side", "status", "qty", "quote_qty", "fee", "fee_ccy",
            "net_filled_qty", "buy_volume", "sell_volume",
        ])
        logger.info(f"成交记录: {csv_path}")

    def _write_trade_csv(self, cl_ord_id: str, side: str, status: str,
                         qty: Decimal, quote_qty: Decimal, fee: Decimal, fee_ccy: str):
        if not self._csv_writer:
            return
        self._csv_writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            cl_ord_id, side, status, f"{qty}", f"{quote_qty:.6f}",
            f"{fee:.8f}", fee_ccy,
            f"{self.net_filled_qty}", f"{self.buy_volume:.2f}", f"{self.sell_volume:.2f}",
        ])
        self._csv_file.flush()

    # ─── 状态持久化 ─────────────────────────────────────────────────────────

    def _state_file_path(self) -> Path:
        return Path(__file__).parent / f"mm_state_{self.instrument_id}.json"

    def _save_state(self):
        try:
            self._state_file_path().write_text(json.dumps({
                "net_filled_qty": str(self.net_filled_qty),
                "buy_volume": str(self.buy_volume),
                "sell_volume": str(self.sell_volume),
                "available_qty": str(self.available_qty),
            }, indent=2))
        except Exception as e:
            logger.warning(f"保存状态失败: {e}")

    def _restore_state(self):
        f = self._state_file_path()
        if not f.exists():
            return
        try:
            data = json.loads(f.read_text())
            self.net_filled_qty = Decimal(data.get("net_filled_qty", "0"))
            self.buy_volume = Decimal(data.get("buy_volume", "0"))
            self.sell_volume = Decimal(data.get("sell_volume", "0"))
            self.available_qty = Decimal(data.get("available_qty", "0"))
            logger.info(f"已恢复状态 | 净持仓={self.net_filled_qty} | 买入额={self.buy_volume:.2f} | 卖出额={self.sell_volume:.2f}")
        except Exception as e:
            logger.warning(f"恢复状态失败: {e}")

    async def _reconcile_position(self):
        """启动时对齐净持仓与真实余额"""
        if not self.base_asset:
            return
        try:
            free, locked = await self._rest_get_balance(self.base_asset)
            actual = free + locked
            self.available_qty = actual
            diff = abs(self.net_filled_qty - actual)
            baseline = max(abs(self.net_filled_qty), abs(actual), self.step_size)
            if diff > self.step_size and (self.net_filled_qty == 0 or actual == 0 or diff / baseline > Decimal("0.1")):
                logger.warning(f"净持仓对账修正 | 记录={self.net_filled_qty} | 实际={actual}")
                self.net_filled_qty = actual
                self._save_state()
            else:
                logger.info(f"对账通过 | 净持仓={self.net_filled_qty} | 实际余额={actual}")
        except Exception as e:
            logger.warning(f"对账失败: {e}")

    # ─── 生命周期 ────────────────────────────────────────────────────────────

    async def initialize(self):
        logger.info(f"初始化 | {self.instrument_id}")
        await self._fetch_exchange_info()
        self._load_params()
        self.stats = Stats(self.instrument_id)
        self._restore_state()
        await self._reconcile_position()
        self._init_trade_csv()

        # 启动 WS
        asyncio.create_task(self._run_market_streams())
        asyncio.create_task(self._run_ws_api())
        asyncio.create_task(self._run_user_data_stream())
        asyncio.create_task(self._keepalive_listen_key_loop())

        # 等待连接就绪
        logger.info("等待 WS 连接...")
        for _ in range(30):
            if self.ws_api and self.bid_price > 0:
                break
            await asyncio.sleep(0.5)

        if not self.ws_api:
            raise RuntimeError("WS API 连接超时")
        if self.bid_price <= 0:
            logger.warning("BBO 数据未就绪，继续等待...")

        logger.info("初始化完成")

    async def run(self):
        self._start_time = time.time()
        try:
            await self.initialize()
            self.strategy_tasks = [
                asyncio.create_task(self._strategy_loop()),
                asyncio.create_task(self._rest_polling_loop()),
                asyncio.create_task(self._print_stats_loop()),
                asyncio.create_task(self._health_monitor()),
            ]
            await asyncio.gather(*self.strategy_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"策略运行错误: {e}")
        finally:
            for task in self.strategy_tasks:
                if not task.done():
                    task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.strategy_tasks, return_exceptions=True), timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            try:
                await asyncio.wait_for(self.shutdown(), timeout=15.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("关闭超时，强制退出")

    async def shutdown(self):
        logger.info("正在关闭策略...")

        # 1. 撤销所有挂单
        try:
            await self._cancel_all_rest()
        except Exception as e:
            logger.warning(f"撤单失败: {e}")
        self.orders.clear()
        await asyncio.sleep(0.5)

        # 2. 清仓
        if self.params.get("auto_close_on_exit", False):
            await self._market_close_rest()

        # 3. 保存状态
        try:
            self._save_state()
            self.stats.save()
        except Exception as e:
            logger.warning(f"保存统计失败: {e}")

        # 4. 关闭 CSV
        if self._csv_file:
            self._csv_file.close()

        # 5. 关闭 HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

        logger.info("策略已安全关闭")


# ─── 入口 ────────────────────────────────────────────────────────────────────────

def setup_logger(instrument_id: str):
    logger.remove()
    logger.add(sys.stdout,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
               level="INFO")
    log_file = Path(__file__).parent / f"logs/{instrument_id}_market_maker_v2.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
               level="DEBUG", rotation="1 day", retention="7 days")


def handle_exception(_loop, context):
    msg = context.get("exception", context["message"])
    if "ConnectionClosed" in str(msg) or "no close frame" in str(msg):
        return
    logger.error(f"未捕获的异步异常: {msg}", exc_info=context.get("exception"))


async def main():
    load_dotenv()

    params_path = Path(__file__).parent / "params.yaml"
    with open(params_path) as f:
        root_params = yaml.safe_load(f)

    instrument_id = root_params.get("instrument_id", "XAUTUSDT")
    api_key = os.getenv("BINANCE_API_KEY", "")
    private_key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    demo = os.getenv("BINANCE_DEMO", "0") == "1"
    assert api_key and private_key_path, "请在 .env 中配置 BINANCE_API_KEY 和 BINANCE_PRIVATE_KEY_PATH"

    setup_logger(instrument_id)
    asyncio.get_running_loop().set_exception_handler(handle_exception)

    mm = MarketMaker(instrument_id, api_key, private_key_path, demo)

    def signal_handler(_sig, _frame):
        logger.warning("收到停止信号, 正在安全退出...")
        mm.should_stop = True
        for task in mm.strategy_tasks:
            task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    mode = "Demo" if demo else "Live"
    strategy_params = root_params.get("strategy", {})
    logger.info(
        f"\n{'='*70}\n"
        f"币安现货双向做市策略V2\n"
        f"交易对: {instrument_id} | 模式: {mode}\n"
        f"网格: step={strategy_params.get('step')} levels={strategy_params.get('num_of_order_each_side')} "
        f"size={strategy_params.get('single_size')} growth={strategy_params.get('size_growth_factor')}\n"
        f"库存限制: buy={strategy_params.get('maximum_net_buy')} sell={strategy_params.get('maximum_net_sell')}\n"
        f"熔断: pause={strategy_params.get('vol_pause_multiplier')}x "
        f"resume={strategy_params.get('vol_resume_multiplier')}x "
        f"close={strategy_params.get('vol_close_on_pause')}\n"
        f"{'='*70}"
    )

    try:
        await mm.run()
    except KeyboardInterrupt:
        mm.should_stop = True
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("程序已退出")


if __name__ == "__main__":
    asyncio.run(main())
