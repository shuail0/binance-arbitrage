#!/usr/bin/env python3
"""币安现货刷量策略V2 - 买侧网格 + 止盈止损 + 波动率熔断
从 OKX spot_volume_v2 移植，通信架构参考 instant_volume
"""
import asyncio
import itertools
import json
import math
import os
import signal
import sys
import time
from base64 import b64encode
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import websockets
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from loguru import logger


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
    # 运行时注入
    api_key: str = ""
    private_key_path: str = ""
    demo: bool = False

    @classmethod
    def from_yaml(cls, file_path: str) -> "StrategyConfig":
        with open(file_path) as f:
            return cls(**{k: v for k, v in yaml.safe_load(f).items() if k in cls.__annotations__})

    def validate(self):
        assert self.api_key and self.private_key_path, "API凭证不能为空"
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
        self._private_key: Optional[Ed25519PrivateKey] = None
        self._load_private_key()

        # URL
        if config.demo:
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
        self.min_notional = Decimal("5")  # 币安最小名义价值
        self.base_asset = ""

        # 持仓管理
        self.positions: Dict[str, Position] = {}
        self.positions_lock = asyncio.Lock()
        self.order_cl_map: Dict[str, str] = {}  # 所有订单ID → buy_cl_ord_id
        self.order_id_map: Dict[int, str] = {}  # orderId(int) → buy_cl_ord_id
        self.stats = Stats(config.strategy_id, config.symbol)
        self.order_counter = itertools.count(1)

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

        # 控制
        self.should_stop = False
        self.strategy_tasks = []

    def _load_private_key(self):
        pem_path = self.config.private_key_path
        if not pem_path or not os.path.exists(pem_path):
            raise FileNotFoundError(f"私钥文件不存在: {pem_path}")
        self._private_key = load_pem_private_key(Path(pem_path).read_bytes(), password=None)

    def _sign(self, payload: str) -> str:
        return b64encode(self._private_key.sign(payload.encode())).decode()

    def _sign_params(self, params: dict) -> dict:
        params["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        params["signature"] = self._sign(query)
        return params

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

    # ==================== REST ====================

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """懒加载持久 HTTP session, 复用 TCP+TLS 连接"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def _rest_request(self, method: str, path: str, params: dict = None, signed: bool = False) -> dict:
        url = f"{self.rest_base}{path}"
        headers = {"X-MBX-APIKEY": self.config.api_key}
        params = params or {}
        if signed:
            params = self._sign_params(params)
        session = await self._get_http_session()
        async with session.request(method, url, params=params, headers=headers) as resp:
            return await resp.json()

    async def fetch_exchange_info(self):
        data = await self._rest_request("GET", "/api/v3/exchangeInfo", {"symbol": self.config.symbol})
        if not data.get("symbols"):
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")
        sym = data["symbols"][0]
        self.base_asset = sym["baseAsset"]
        for f in sym["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self.tick_size = Decimal(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                self.step_size = Decimal(f["stepSize"])
            elif f["filterType"] == "NOTIONAL":
                self.min_notional = Decimal(f.get("minNotional", "5"))
        logger.info(f"交易规则 | {self.config.symbol} | base={self.base_asset} | "
                    f"tick={self.tick_size} | step={self.step_size} | minNotional={self.min_notional}")

    async def rest_get_balance(self, asset: str) -> Decimal:
        data = await self._rest_request("GET", "/api/v3/account", signed=True)
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return Decimal(b["free"])
        return Decimal("0")

    async def rest_place_market_sell(self, qty: str) -> dict:
        return await self._rest_request("POST", "/api/v3/order", {
            "symbol": self.config.symbol, "side": "SELL", "type": "MARKET", "quantity": qty,
        }, signed=True)

    async def rest_cancel_open_orders(self):
        """REST 取消该交易对所有挂单"""
        return await self._rest_request("DELETE", "/api/v3/openOrders", {
            "symbol": self.config.symbol,
        }, signed=True)

    # ==================== Listen Key ====================

    async def _get_listen_key(self) -> str:
        data = await self._rest_request("POST", "/api/v3/userDataStream", signed=False)
        return data["listenKey"]

    async def _keepalive_listen_key(self):
        if self.listen_key:
            await self._rest_request("PUT", "/api/v3/userDataStream", {"listenKey": self.listen_key})

    async def _keepalive_listen_key_loop(self):
        while not self.should_stop:
            try:
                await asyncio.sleep(30 * 60)  # 每30分钟
                await self._keepalive_listen_key()
                logger.debug("listenKey keepalive 完成")
            except Exception as e:
                logger.warning(f"listenKey keepalive 失败: {e}")

    # ==================== WS Streams ====================

    async def _run_market_streams(self):
        """bookTicker + ticker（波动率数据源）"""
        sym = self.config.symbol.lower()
        url = f"{self.ws_stream_url}/stream?streams={sym}@bookTicker/{sym}@ticker"
        while not self.should_stop:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info(f"WS Streams 已连接 | bookTicker + ticker")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        stream = msg.get("stream", "")
                        if "bookTicker" in stream:
                            if "b" in data and "a" in data:
                                self.bid_price = Decimal(data["b"])
                                self.ask_price = Decimal(data["a"])
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
        """User Data Stream: executionReport 推送"""
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
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"User Data Stream 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    # ==================== WS API ====================

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
        payload = {"id": req_id, "method": method, "params": {**params, "apiKey": self.config.api_key}}
        fut = asyncio.get_running_loop().create_future()
        self.ws_api_responses[req_id] = fut
        await self.ws_api.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.ws_api_responses.pop(req_id, None)
            raise

    # ==================== 订单操作 ====================

    async def ws_place_order(self, side: str, ord_type: str, quantity: str,
                             price: str = None, client_order_id: str = None,
                             time_in_force: str = None) -> dict:
        """WS API 下单"""
        params = {
            "symbol": self.config.symbol,
            "side": side,
            "type": ord_type,
            "quantity": quantity,
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        if price:
            params["price"] = price
        if time_in_force:
            params["timeInForce"] = time_in_force
        elif ord_type == "LIMIT":
            params["timeInForce"] = "GTC"
        resp = await self._ws_api_request("order.place", params)
        return resp

    async def ws_cancel_order(self, order_id: int = None, client_order_id: str = None) -> dict:
        """WS API 撤单"""
        params = {"symbol": self.config.symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        try:
            return await self._ws_api_request("order.cancel", params, timeout=5)
        except asyncio.TimeoutError:
            logger.warning("撤单超时")
            return {"status": -1}

    # ==================== 持仓管理 ====================

    def _register_order_mapping(self, order_id, buy_cl_ord_id: str):
        """注册订单映射（调用者需持有锁）"""
        if isinstance(order_id, int) and order_id > 0:
            self.order_id_map[order_id] = buy_cl_ord_id
        elif isinstance(order_id, str) and order_id:
            self.order_cl_map[order_id] = buy_cl_ord_id

    def _cleanup_position_maps(self, position: Position):
        """清理持仓的所有映射（调用者需持有锁）"""
        for oid in [position.buy_cl_ord_id, position.tp_cl_ord_id,
                    position.timeout_cl_ord_id, position.emergency_cl_ord_id]:
            if oid:
                self.order_cl_map.pop(oid, None)
        for oid in [position.buy_order_id, position.tp_order_id]:
            if oid:
                self.order_id_map.pop(oid, None)

    def _delete_position(self, position: Position):
        """删除持仓并清理映射（调用者需持有锁）"""
        self._cleanup_position_maps(position)
        self.positions.pop(position.buy_cl_ord_id, None)

    async def _find_position(self, cl_ord_id: str, order_id: int) -> Optional[Position]:
        async with self.positions_lock:
            buy_cl = self.order_cl_map.get(cl_ord_id) or self.order_id_map.get(order_id)
            if buy_cl:
                return self.positions.get(buy_cl)
            return self.positions.get(cl_ord_id)

    # ==================== executionReport 处理 ====================

    async def _handle_execution_report(self, msg: dict):
        """处理 User Data Stream 的 executionReport"""
        try:
            symbol = msg.get("s", "")
            if symbol != self.config.symbol:
                return

            cl_ord_id = msg.get("c", "")
            order_id = msg.get("i", 0)
            exec_type = msg.get("x", "")       # NEW/TRADE/CANCELED/EXPIRED
            order_status = msg.get("X", "")     # NEW/PARTIALLY_FILLED/FILLED/CANCELED
            side = msg.get("S", "")
            cum_filled_qty = self._safe_decimal(msg.get("z"))  # accFillSz 等价
            last_filled_price = self._safe_decimal(msg.get("L"))
            last_filled_qty = self._safe_decimal(msg.get("l"))
            price = self._safe_decimal(msg.get("p"))
            commission = self._safe_decimal(msg.get("n"))
            commission_asset = msg.get("N", "")
            cum_quote_qty = self._safe_decimal(msg.get("Z"))  # 累计成交额

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
                    # 买单成交 → OPEN → 挂止盈
                    actual_filled = cum_filled_qty
                    if commission_asset == self.base_asset and commission > 0:
                        actual_filled = max(cum_filled_qty - commission, Decimal("0"))

                    # 使用加权均价: cumQuoteQty / cumFilledQty
                    avg_price = cum_quote_qty / cum_filled_qty if cum_filled_qty > 0 else price
                    position.buy_price = self._align_price(avg_price)
                    position.filled_size = self._align_qty(actual_filled)
                    position.buy_order_id = order_id
                    position.open_time = datetime.now()
                    position.status = PositionStatus.OPEN

                    # 重新计算止盈/止损价
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
                        # 部分成交后取消 → 紧急市价卖出
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
                        # 完全未成交取消
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
                        # 止盈单部分成交后取消
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
                        # 止盈单被完全取消
                        if position.status == PositionStatus.CLOSING:
                            pass  # 程序主动取消，等市价单
                        else:
                            # 交易所自动取消 → 紧急平仓
                            filled_size = self._safe_decimal(position.filled_size)
                            if filled_size > 0:
                                logger.warning(f"[#{self._short_id(cl_ord_id)}] 止盈单被取消，紧急平仓")
                                await self._submit_emergency_sell(
                                    position, str(filled_size),
                                    self._generate_order_id("ec"), mark_submitted=True)

        except Exception as e:
            logger.error(f"处理 executionReport 失败: {e}", exc_info=True)

    async def _handle_sell_filled(self, position: Position, msg: dict, reason: str):
        """卖单成交，计算盈亏"""
        cum_filled_qty = self._safe_decimal(msg.get("z"))
        cum_quote_qty = self._safe_decimal(msg.get("Z"))
        commission = self._safe_decimal(msg.get("n"))
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

    async def _submit_emergency_sell(self, position: Position, size: str, order_id: str, mark_submitted: bool = False):
        """紧急市价卖出"""
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

    # ==================== 止盈下单 ====================

    async def place_take_profit_order(self, position: Position):
        try:
            if position.tp_cl_ord_id:
                return
            tp_cl_ord_id = self._generate_order_id("s")
            position.tp_cl_ord_id = tp_cl_ord_id

            async with self.positions_lock:
                self._register_order_mapping(tp_cl_ord_id, position.buy_cl_ord_id)

            # 如果止盈偏移=0，直接挂在买入价
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

    # ==================== 买单网格 ====================

    async def buy_grid_loop(self):
        logger.info("买单网格循环已启动")
        while not self.should_stop:
            try:
                # 1. 最大成交额检查
                if self.stats.is_max_volume_reached(self.config.max_volume):
                    logger.warning(f"累计成交额达到上限: {self.config.max_volume}")
                    os.kill(os.getpid(), signal.SIGINT)
                    break

                # 2. 波动率熔断
                await self._check_volatility()
                if self._vol_paused:
                    await asyncio.sleep(1)
                    continue

                # 3. 等待价格
                if self.bid_price <= 0:
                    await asyncio.sleep(1)
                    continue

                # 4. 计算目标网格
                step = Decimal(str(self.config.step))
                base_qty = Decimal(str(self.config.quantity))
                growth = Decimal(str(self.config.size_growth_factor))

                proposed = []
                for i in range(self.config.num_levels):
                    px = self._align_price(self.bid_price - step * (i + self.config.start_level))
                    qty = self._align_qty(base_qty * (1 + growth * i))
                    if Decimal(px) > 0 and Decimal(qty) > 0:
                        proposed.append((px, qty))

                # 5. 收集当前状态
                async with self.positions_lock:
                    opening_by_price = {p.buy_price: p for p in self.positions.values()
                                        if p.status == PositionStatus.OPENING}
                    active_count = sum(1 for p in self.positions.values()
                                       if p.status in (PositionStatus.OPENING, PositionStatus.OPEN, PositionStatus.CLOSING))
                    net_buy = sum(Decimal(p.filled_size) for p in self.positions.values()
                                  if p.status in (PositionStatus.OPEN, PositionStatus.CLOSING))

                # 5.5 库存偏斜
                if self.config.max_net_buy > 0:
                    max_net = Decimal(str(self.config.max_net_buy))
                    ratio = float(max(1 - net_buy / max_net, 0))
                    proposed = proposed[:math.ceil(len(proposed) * ratio)]

                # 6. 取消偏离档位（币安不支持改价，直接撤单）
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

                # 7. 补下新买单
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

                    # 止损
                    if self.bid_price > 0 and sl_price > 0 and self.bid_price <= sl_price:
                        await self._close_position(position, "stop_loss", "sl")
                        continue

                    # 超时前检查止盈价是否在卖一价 → 重置计时
                    if position.take_profit_price and self.ask_price > 0:
                        if self._safe_decimal(position.take_profit_price) == self.ask_price:
                            async with self.positions_lock:
                                position.open_time = datetime.now()
                            continue

                    # 超时
                    if position.is_timeout(self.config.take_profit_timeout):
                        await self._close_position(position, "timeout", "timeout")

                await asyncio.sleep(self.config.stop_loss_check_interval)
            except Exception as e:
                logger.error(f"持仓监控错误: {e}")
                await asyncio.sleep(1)

    async def _close_position(self, position: Position, reason: str, suffix: str):
        """取消止盈单 → 市价卖出剩余"""
        try:
            position.status = PositionStatus.CLOSING
            cl_ord_id = self._generate_order_id("c")
            setattr(position, f"{suffix}_cl_ord_id", cl_ord_id)
            async with self.positions_lock:
                self._register_order_mapping(cl_ord_id, position.buy_cl_ord_id)

            # 1. 取消止盈单
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

            # 2. 计算剩余数量
            filled_size = self._safe_decimal(position.filled_size)
            tp_filled_size = self._safe_decimal(position.tp_filled_size)
            remaining = filled_size - tp_filled_size

            # 3. 市价卖出
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
        """熔断: 撤单 + 可选市价清仓"""
        # 1. 标记所有活跃持仓为 CLOSING
        async with self.positions_lock:
            for pos in self.positions.values():
                if pos.status in (PositionStatus.OPENING, PositionStatus.OPEN):
                    pos.status = PositionStatus.CLOSING

        # 2. REST 批量撤单
        try:
            await self.rest_cancel_open_orders()
            logger.info("熔断撤单完成")
        except Exception as e:
            logger.warning(f"熔断撤单失败: {e}")

        # 3. 市价清仓
        if self.config.vol_close_on_pause:
            await self._market_close_rest()

    async def _market_close_rest(self):
        """REST 查余额 → 市价清仓"""
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
            if hasattr(self, "strategy_tasks") and self.strategy_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.strategy_tasks, return_exceptions=True), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
            await self.shutdown()

    async def shutdown(self):
        logger.info("正在关闭策略...")

        # 1. 标记所有活跃持仓为CLOSING
        async with self.positions_lock:
            for pos in self.positions.values():
                if pos.status in (PositionStatus.OPENING, PositionStatus.OPEN):
                    pos.status = PositionStatus.CLOSING

        # 2. REST 批量撤单
        try:
            await self.rest_cancel_open_orders()
            logger.info("批量撤单完成")
        except Exception as e:
            logger.warning(f"批量撤单失败: {e}")

        await asyncio.sleep(0.5)

        # 3. 清仓
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

        # 4. 保存状态
        try:
            await asyncio.wait_for(self.stats.save_state(), timeout=2.0)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

        # 5. 关闭 HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

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
    load_dotenv()
    config_path = os.getenv("CONFIG_FILE", str(Path(__file__).parent / "config.yaml"))
    config = StrategyConfig.from_yaml(config_path)
    config.api_key = os.getenv("BINANCE_API_KEY", "")
    config.private_key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    config.demo = os.getenv("BINANCE_DEMO", "0") == "1"
    config.validate()
    setup_logger(config.strategy_id)

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)

    mode = "Demo" if config.demo else "Live"
    logger.info(
        f"\n{'='*70}\n"
        f"币安现货刷量策略V2 - 买侧网格 + 止盈止损\n"
        f"策略ID: {config.strategy_id} | 交易对: {config.symbol}\n"
        f"数量: {config.quantity} | 止盈: +{config.take_profit_offset} | 止损: -{config.stop_loss_offset}\n"
        f"网格: step={config.step} levels={config.num_levels} start={config.start_level} growth={config.size_growth_factor}\n"
        f"熔断: pause={config.vol_pause_multiplier}x resume={config.vol_resume_multiplier}x close={config.vol_close_on_pause}\n"
        f"最大订单组: {config.max_order_groups} | 累计上限: {config.max_volume}\n"
        f"模式: {mode}\n"
        f"{'='*70}"
    )

    strategy = SpotVolumeV2(config, config_path)

    def signal_handler(_sig, _frame):
        logger.warning("收到停止信号，正在安全退出...")
        strategy.should_stop = True
        if hasattr(strategy, "strategy_tasks"):
            for task in strategy.strategy_tasks:
                task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await strategy.run()
    except KeyboardInterrupt:
        strategy.should_stop = True
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"策略异常退出: {e}")
        raise
    finally:
        logger.info("程序已退出")


if __name__ == "__main__":
    asyncio.run(main())
