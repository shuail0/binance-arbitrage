#!/usr/bin/env python3
"""币安合约双向做市/刷量策略 — 从 Go futures_market_maker 移植
通信: WS Streams (depth + aggTrade + userData) + REST (下单/撤单/改单/持仓/余额)
"""
import asyncio
import csv
import json
import math
import os
import signal
import sys
import time
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
    order_id: int = 0
    side: str = ""          # BUY / SELL
    price: Decimal = Decimal("0")
    quantity: Decimal = Decimal("0")
    pending: bool = False
    sent_at: float = 0


@dataclass
class TargetOrder:
    side: str
    price: Decimal
    quantity: Decimal


@dataclass
class ReplaceOp:
    order_id: int
    side: str
    new_price: Decimal
    new_qty: Decimal


# ─── 波动率追踪 ──────────────────────────────────────────────────────────────────

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
                        f"倍数={self.ratio:.1f}x 比例={self.range_pct:.6f} 样本={len(self._range_history)}")

    @property
    def ratio(self) -> float:
        return self.current_range / self.baseline if self.baseline > 0 else 0

    @property
    def range_pct(self) -> float:
        return self.current_range / self.last_price if self.last_price > 0 else 0

    @property
    def ready(self) -> bool:
        return self.baseline > 0

    def get_state(self) -> tuple[float, float, float, bool]:
        """返回 (range_pct, ratio, direction, ready)"""
        direction = (self._prices[-1] - self._prices[0]) if len(self._prices) >= 2 else 0
        return self.range_pct, self.ratio, direction, self.ready


# ─── 活跃度追踪 ──────────────────────────────────────────────────────────────────

class ActivityTracker:
    def __init__(self, window_min: int = 3):
        self.window_min = max(window_min, 1)
        self._trade_times: list[int] = []  # unix millis
        self._start_time = time.time()

    @property
    def ready(self) -> bool:
        return time.time() - self._start_time >= (self.window_min + 1) * 60

    def on_trade(self, trade_time_ms: int):
        self._trade_times.append(trade_time_ms)
        cutoff = int(time.time() * 1000) - (self.window_min + 1) * 60_000
        while self._trade_times and self._trade_times[0] < cutoff:
            self._trade_times.pop(0)

    def trade_stats(self) -> tuple[float, float, float]:
        """返回 (min_trades, max_trades, min_coverage) 最近N个自然分钟"""
        cur_min = int(time.time() // 60) * 60_000
        counts = [0] * self.window_min
        bits = [0] * self.window_min
        for t in reversed(self._trade_times):
            if t >= cur_min:
                continue
            idx = (cur_min - t - 1) // 60_000
            if idx >= self.window_min:
                break
            counts[idx] += 1
            bits[idx] |= 1 << ((t % 60_000) // 1000)
        covs = [bin(b).count('1') / 60.0 for b in bits]
        if not counts:
            return 0, 0, 0
        return float(min(counts)), float(max(counts)), min(covs)


# ─── 统计 ────────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.state_file = Path(__file__).parent / "volume_state.json"
        self.buy_volume = self.sell_volume = self.total_pnl = Decimal("0")
        self.order_count = 0
        self._load()

    def _load(self):
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text()).get(self.symbol, {})
            self.buy_volume = Decimal(data.get("buy_volume", "0"))
            self.sell_volume = Decimal(data.get("sell_volume", "0"))
            self.total_pnl = Decimal(data.get("total_pnl", "0"))
            if not self.buy_volume.is_zero() or not self.sell_volume.is_zero():
                logger.info(f"恢复统计 | 买入={self.buy_volume} | 卖出={self.sell_volume} | PnL={self.total_pnl}")
        except Exception as e:
            logger.warning(f"加载统计失败: {e}")

    def save(self):
        try:
            all_data = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
            all_data[self.symbol] = {
                "buy_volume": str(self.buy_volume),
                "sell_volume": str(self.sell_volume),
                "total_pnl": str(self.total_pnl),
                "updated_at": datetime.now().isoformat(),
            }
            self.state_file.write_text(json.dumps(all_data, indent=2))
        except Exception as e:
            logger.warning(f"保存统计失败: {e}")

    def add_buy(self, vol: Decimal):
        self.buy_volume += vol
        self.order_count += 1

    def add_sell(self, vol: Decimal, pnl: Decimal):
        self.sell_volume += vol
        self.total_pnl += pnl
        self.order_count += 1

    @property
    def total_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume


# ─── 策略主类 ────────────────────────────────────────────────────────────────────

class SwapVolume:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.symbol: str = cfg["symbol"]
        self.strategy_id: str = cfg.get("strategyId", "SV_001")
        self.api_key: str = cfg["apiKey"]
        self.demo: bool = cfg.get("demo", False)
        self.should_stop = False

        # 私钥
        pk_path = cfg["privateKeyPath"]
        if not pk_path or not os.path.exists(pk_path):
            raise FileNotFoundError(f"私钥文件不存在: {pk_path}")
        self._private_key: Ed25519PrivateKey = load_pem_private_key(
            Path(pk_path).read_bytes(), password=None)

        # URL
        if self.demo:
            self.rest_base = "https://demo-fapi.binance.com"
            self.ws_stream_url = "wss://fstream.binancefuture.com/stream"
            self.spot_rest_base = "https://demo-api.binance.com"
        else:
            self.rest_base = "https://fapi.binance.com"
            self.ws_stream_url = "wss://fstream.binance.com/stream"
            self.spot_rest_base = "https://api.binance.com"

        # 市场数据
        self.bid_price = self.ask_price = Decimal("0")
        self.tick_size = Decimal("0.01")
        self.step_size = Decimal("0.001")
        self.min_qty = Decimal("0.001")

        # 深度 (OBI)
        self.depth_bids: list = []
        self.depth_asks: list = []

        # 订单: order_key -> ActiveOrder
        # key = "p:<clOrdId>" (pending) 或 "<orderId>" (confirmed)
        self.active_orders: dict[str, ActiveOrder] = {}

        # 持仓
        self.position_amt = Decimal("0")  # 正=多, 负=空
        self.entry_price = Decimal("0")

        # 统计 + CSV
        self.stats: Optional[Stats] = None
        self._csv_file = None
        self._csv_writer = None

        # 波动率 + 活跃度
        self.vol_tracker = VolatilityTracker()
        self.act_tracker: Optional[ActivityTracker] = None
        self._vol_paused = False
        self._act_paused = False

        # User Data Stream
        self.listen_key: Optional[str] = None

        # HTTP session (持久化, 复用 TCP+TLS; 合约和现货 BNB 共用)
        self._http_session: Optional[aiohttp.ClientSession] = None

        # 控制
        self._order_seq = 0
        self._last_reconcile = 0.0
        self._loop_count = 0
        self._start_time = time.time()
        self._config_mtime: float = 0
        self._config_path = Path(__file__).parent / "config.yaml"
        self._current_leverage = 0
        self.strategy_tasks: list = []

    # ─── 签名 ────────────────────────────────────────────────────────────────

    def _sign(self, payload: str) -> str:
        return b64encode(self._private_key.sign(payload.encode())).decode()

    def _sign_params(self, params: dict) -> dict:
        params["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        params["signature"] = self._sign(query)
        return params

    # ─── 工具函数 ─────────────────────────────────────────────────────────────

    def _align_price(self, price: Decimal, side: str = "BUY") -> Decimal:
        if self.tick_size <= 0:
            return price
        return (price / self.tick_size).to_integral_value(
            rounding=ROUND_DOWN if side == "BUY" else ROUND_UP) * self.tick_size

    def _align_qty(self, qty: Decimal) -> Decimal:
        if self.step_size <= 0:
            return qty
        return (qty / self.step_size).to_integral_value(rounding=ROUND_DOWN) * self.step_size

    def _new_cid(self) -> str:
        self._order_seq += 1
        return f"fmm_{int(time.time()*1000)}_{self._order_seq}"

    @staticmethod
    def _short_id(cid: str) -> str:
        return cid[-8:] if len(cid) > 8 else cid

    # ─── 配置热加载 ──────────────────────────────────────────────────────────

    @property
    def s(self) -> dict:
        return self.cfg.get("strategy", {})

    def _reload_config(self):
        try:
            mtime = self._config_path.stat().st_mtime
            if mtime <= self._config_mtime:
                return
            with open(self._config_path) as f:
                hot = yaml.safe_load(f)
            old_s = self.cfg.get("strategy", {}).copy()
            self.cfg["strategy"] = hot.get("strategy", {})
            if hot.get("leverage", 0) > 0:
                self.cfg["leverage"] = hot["leverage"]
            self._set_defaults()
            self._config_mtime = mtime
            changed = {k: f"{old_s.get(k)} -> {v}" for k, v in self.s.items() if old_s.get(k) != v}
            if changed:
                logger.info("热加载 | " + " | ".join(f"{k}: {v}" for k, v in changed.items()))
        except Exception as e:
            logger.warning(f"热加载失败: {e}")

    def _set_defaults(self):
        s = self.cfg.setdefault("strategy", {})
        s.setdefault("imbalanceDepth", 5)
        s.setdefault("imbalanceThreshold", 1.5)
        s.setdefault("imbalanceLevelScale", 0.6)
        if not s.get("volPauseMultiplier"):
            s["volPauseMultiplier"] = 2.0
        if not s.get("volResumeMultiplier"):
            s["volResumeMultiplier"] = 1.5
        if not s.get("volPositionThreshold") or s["volPositionThreshold"] <= 0:
            s["volPositionThreshold"] = 0.3
        if not s.get("bnbMinBalance") or s["bnbMinBalance"] <= 0:
            s["bnbMinBalance"] = 0.02
        if not s.get("bnbBuyAmount") or s["bnbBuyAmount"] <= 0:
            s["bnbBuyAmount"] = 0.2

    # ─── REST (合约) ─────────────────────────────────────────────────────────

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """懒加载持久 HTTP session, 复用 TCP+TLS 连接"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def _futures_rest(self, method: str, path: str, params: dict = None,
                            signed: bool = True) -> dict | list:
        url = f"{self.rest_base}{path}"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = dict(params or {})
        if signed:
            params = self._sign_params(params)
        session = await self._get_http_session()
        async with session.request(method, url, params=params, headers=headers) as resp:
            return await resp.json()

    async def _spot_rest(self, method: str, path: str, params: dict = None,
                         signed: bool = True) -> dict | list:
        url = f"{self.spot_rest_base}{path}"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = dict(params or {})
        if signed:
            params = self._sign_params(params)
        session = await self._get_http_session()
        async with session.request(method, url, params=params, headers=headers) as resp:
            return await resp.json()

    # ─── 初始化 ──────────────────────────────────────────────────────────────

    async def initialize(self):
        self._set_defaults()
        self.stats = Stats(self.symbol)
        self.act_tracker = ActivityTracker(self.s.get("activityWindow", 3) or 3)

        # 交易规则
        data = await self._futures_rest("GET", "/fapi/v1/exchangeInfo", signed=False)
        sym = next((s for s in data.get("symbols", []) if s["symbol"] == self.symbol), None)
        if not sym:
            raise RuntimeError(f"交易对 {self.symbol} 不存在")
        for f in sym.get("filters", []):
            if f["filterType"] == "PRICE_FILTER":
                self.tick_size = Decimal(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                self.step_size = Decimal(f["stepSize"])
                self.min_qty = Decimal(f.get("minQty", "0.001"))
        logger.info(f"交易规则 | {self.symbol} | tick={self.tick_size} step={self.step_size} minQty={self.min_qty}")

        # 设置持仓模式 + 保证金 + 杠杆
        await self._setup_account()

        # 清除残余订单 + 同步持仓
        await self._cancel_all_orders()
        await self._sync_position()

        # CSV
        self._init_trade_csv()

    async def _setup_account(self):
        # 单向持仓
        try:
            await self._futures_rest("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        except Exception:
            pass

        # 保证金类型
        margin_type = self.cfg.get("marginType", "CROSSED")
        try:
            await self._futures_rest("POST", "/fapi/v1/marginType",
                                     {"symbol": self.symbol, "marginType": margin_type})
        except Exception:
            pass

        # 杠杆
        leverage = self.cfg.get("leverage", 5) or 5
        try:
            result = await self._futures_rest("POST", "/fapi/v1/leverage",
                                              {"symbol": self.symbol, "leverage": str(leverage)})
            self._current_leverage = leverage
            logger.info(f"杠杆={leverage}x | maxNotional={result.get('maxNotionalValue', '?')}")
        except Exception as e:
            logger.warning(f"设置杠杆失败: {e}")

    # ─── Listen Key ──────────────────────────────────────────────────────────

    async def _get_listen_key(self) -> str:
        # USER_STREAM 类型接口: 仅需 API Key header, 不需要签名
        data = await self._futures_rest("POST", "/fapi/v1/listenKey", signed=False)
        return data["listenKey"]

    async def _keepalive_listen_key_loop(self):
        while not self.should_stop:
            try:
                await asyncio.sleep(30 * 60)
                if self.listen_key:
                    await self._futures_rest("PUT", "/fapi/v1/listenKey", signed=False)
                    logger.debug("listenKey keepalive 完成")
            except Exception as e:
                logger.warning(f"listenKey keepalive 失败: {e}")

    # ─── WS Streams ──────────────────────────────────────────────────────────

    async def _run_market_streams(self):
        """depth + aggTrade"""
        sym = self.symbol.lower()
        depth_levels = 5
        imb_depth = self.s.get("imbalanceDepth", 5)
        if imb_depth > 5:
            depth_levels = 10
        if imb_depth > 10:
            depth_levels = 20
        streams = f"{sym}@depth{depth_levels}@500ms/{sym}@aggTrade"
        url = f"{self.ws_stream_url}?streams={streams}"
        while not self.should_stop:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info(f"WS Streams 已连接 | depth{depth_levels} + aggTrade")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        stream = msg.get("stream", "")
                        if "depth" in stream:
                            self.depth_bids = data.get("b", data.get("bids", []))
                            self.depth_asks = data.get("a", data.get("asks", []))
                            if self.depth_bids:
                                self.bid_price = Decimal(self.depth_bids[0][0])
                            if self.depth_asks:
                                self.ask_price = Decimal(self.depth_asks[0][0])
                        elif "aggTrade" in stream:
                            price = float(data.get("p", 0))
                            if price > 0:
                                self.vol_tracker.on_price(price)
                            trade_time = data.get("T", 0)
                            if trade_time and self.act_tracker:
                                self.act_tracker.on_trade(trade_time)
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"WS Streams 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    async def _run_user_data_stream(self):
        while not self.should_stop:
            try:
                self.listen_key = await self._get_listen_key()
                url = f"{self.ws_stream_url.replace('/stream', '')}/ws/{self.listen_key}"
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info("User Data Stream 已连接")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        if msg.get("e") == "ORDER_TRADE_UPDATE":
                            self._handle_order_update(msg)
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"User Data Stream 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    # ─── ORDER_TRADE_UPDATE 处理 ─────────────────────────────────────────────

    def _handle_order_update(self, msg: dict):
        try:
            o = msg.get("o", {})
            symbol = o.get("s", "")
            if symbol != self.symbol:
                return
            cl_ord_id = o.get("c", "")
            order_id = o.get("i", 0)
            status = o.get("X", "")       # NEW / PARTIALLY_FILLED / FILLED / CANCELED / EXPIRED
            side = o.get("S", "")
            order_key = str(order_id)
            pending_key = f"p:{cl_ord_id}"

            if status == "NEW":
                # pending -> active
                self.active_orders.pop(pending_key, None)
                price = Decimal(o.get("p", "0"))
                qty = Decimal(o.get("q", "0"))
                self.active_orders[order_key] = ActiveOrder(
                    cl_ord_id=cl_ord_id, order_id=order_id,
                    side=side, price=price, quantity=qty)

            elif status in ("PARTIALLY_FILLED", "FILLED"):
                self._update_position(o)
                if status == "FILLED":
                    self.active_orders.pop(order_key, None)
                    self.active_orders.pop(pending_key, None)

            elif status in ("CANCELED", "EXPIRED"):
                self.active_orders.pop(order_key, None)
                self.active_orders.pop(pending_key, None)

        except Exception as e:
            logger.error(f"处理 ORDER_TRADE_UPDATE 失败: {e}", exc_info=True)

    def _update_position(self, o: dict):
        last_qty = Decimal(o.get("l", "0"))
        last_px = Decimal(o.get("L", "0"))
        if last_qty.is_zero() or last_px.is_zero():
            return

        volume = last_qty * last_px
        realized_pnl = Decimal(o.get("rp", "0"))
        side = o.get("S", "")

        if side == "BUY":
            self.position_amt += last_qty
            self.stats.add_buy(volume)
        else:
            self.position_amt -= last_qty
            self.stats.add_sell(volume, realized_pnl)

        self._save_state()
        self._write_trade_csv(side, o.get("X", ""), last_px, last_qty, volume, realized_pnl)

    # ─── 网格计算 + 库存偏斜 ────────────────────────────────────────────────

    def _max_long(self) -> float:
        return float(self.s.get("maximumLong", 0.01))

    def _max_short(self) -> float:
        return float(self.s.get("maximumShort", 0.01))

    def _calculate_levels(self) -> tuple[int, int]:
        n = int(self.s.get("numOrdersEachSide", 5))
        buy_levels = sell_levels = n
        pos = float(self.position_amt)

        # 多头 -> 减少买层
        if (ml := self._max_long()) > 0 and pos > 0:
            buy_levels = math.ceil(n * max(1 - pos / ml, 0))
        # 空头 -> 减少卖层
        if (ms := self._max_short()) > 0 and pos < 0:
            sell_levels = math.ceil(n * max(1 + pos / ms, 0))

        return buy_levels, sell_levels

    def _calc_imbalance(self, depth: int = 5) -> float:
        bid_vol = sum(float(b[1]) for b in self.depth_bids[:depth]) if self.depth_bids else 0
        ask_vol = sum(float(a[1]) for a in self.depth_asks[:depth]) if self.depth_asks else 0
        return bid_vol / ask_vol if ask_vol > 0 else 1.0

    def _calculate_target_orders(self, bid: Decimal, ask: Decimal) -> tuple[list[TargetOrder], list[TargetOrder]]:
        buy_levels, sell_levels = self._calculate_levels()
        single_size = self._align_qty(Decimal(str(self.s.get("quantity", "0.001"))))
        if single_size < self.step_size:
            single_size = self.step_size
        growth = Decimal(str(self.s.get("sizeGrowthFactor", 0)))
        start_level = int(self.s.get("startLevel", 0))

        # 步长
        step_abs = self._align_price(Decimal(str(self.s.get("stepSize", "1.0"))))
        if step_abs < self.tick_size:
            step_abs = self.tick_size

        # OBI 偏斜
        imbalance = self._calc_imbalance(int(self.s.get("imbalanceDepth", 5)))
        threshold = float(self.s.get("imbalanceThreshold", 1.5))
        scale = float(self.s.get("imbalanceLevelScale", 0.6))
        buy_qty_scale = sell_qty_scale = Decimal("1")

        if self.s.get("imbalanceMode") == "levels":
            if imbalance > threshold:
                sell_levels = max(1, int(sell_levels * scale))
            elif threshold > 0 and imbalance < 1 / threshold:
                buy_levels = max(1, int(buy_levels * scale))
        else:
            if imbalance > threshold:
                sell_qty_scale = Decimal(str(scale))
            elif threshold > 0 and imbalance < 1 / threshold:
                buy_qty_scale = Decimal(str(scale))

        buys, sells = [], []
        for i in range(buy_levels):
            level = Decimal(str(i + start_level))
            px = self._align_price(bid - step_abs * level, "BUY")
            qty = self._align_qty(single_size * (1 + growth * i) * buy_qty_scale)
            if px > 0 and qty > 0:
                buys.append(TargetOrder("BUY", px, qty))

        for i in range(sell_levels):
            level = Decimal(str(i + start_level))
            px = self._align_price(ask + step_abs * level, "SELL")
            qty = self._align_qty(single_size * (1 + growth * i) * sell_qty_scale)
            if px > 0 and qty > 0:
                sells.append(TargetOrder("SELL", px, qty))

        return buys, sells

    # ─── Diff 算法 (撤/改/下) ────────────────────────────────────────────────

    def _diff_side(self, active: list[ActiveOrder], targets: list[TargetOrder],
                   price_tolerance: Decimal) -> tuple[list[int], list[ReplaceOp], list[TargetOrder]]:
        """返回 (to_cancel_ids, to_replace, to_place)"""
        used_active = set()
        used_target = set()

        # Pass 1: 精确价格匹配
        for i, t in enumerate(targets):
            for j, ao in enumerate(active):
                if j in used_active or ao.pending:
                    continue
                if ao.price == t.price:
                    used_active.add(j)
                    used_target.add(i)
                    if ao.quantity != t.quantity:
                        used_active.discard(j)
                        used_target.discard(i)
                        # 改单
                    else:
                        break
                    break

        # Pass 2: 剩余 target 贪心匹配剩余 active -> 改单
        to_replace = []
        for i, t in enumerate(targets):
            if i in used_target:
                continue
            best_j, best_diff = -1, Decimal("999999999")
            for j, ao in enumerate(active):
                if j in used_active or ao.pending:
                    continue
                diff = abs(ao.price - t.price)
                if diff < best_diff:
                    best_diff = diff
                    best_j = j
            if best_j >= 0:
                used_active.add(best_j)
                used_target.add(i)
                ao = active[best_j]
                if abs(ao.price - t.price) > price_tolerance or ao.quantity != t.quantity:
                    to_replace.append(ReplaceOp(ao.order_id, t.side, t.price, t.quantity))

        to_cancel = [active[j].order_id for j in range(len(active))
                     if j not in used_active and not active[j].pending]
        to_place = [targets[i] for i in range(len(targets)) if i not in used_target]

        return to_cancel, to_replace, to_place

    # ─── 批量执行 (REST) ────────────────────────────────────────────────────

    async def _execute_batch_cancels(self, order_ids: list[int]):
        if not order_ids:
            return
        # 立即移除
        for oid in order_ids:
            self.active_orders.pop(str(oid), None)
        # 每批最多10单
        for i in range(0, len(order_ids), 10):
            batch = order_ids[i:i+10]
            try:
                params = {"symbol": self.symbol, "orderIdList": json.dumps(batch)}
                results = await self._futures_rest("DELETE", "/fapi/v1/batchOrders", params)
                if isinstance(results, list):
                    for r in results:
                        if isinstance(r, dict) and r.get("code") and "Unknown order" not in str(r.get("msg", "")):
                            logger.warning(f"撤单失败: {r.get('msg')}")
            except Exception as e:
                logger.warning(f"批量撤单请求失败: {e}")

    async def _execute_batch_modifies(self, ops: list[ReplaceOp]):
        if not ops:
            return
        # 每批最多5单
        for i in range(0, len(ops), 5):
            batch = ops[i:i+5]
            items = [{"symbol": self.symbol, "orderId": op.order_id, "side": op.side,
                       "price": str(op.new_price), "quantity": str(op.new_qty)} for op in batch]
            try:
                params = {"batchOrders": json.dumps(items)}
                results = await self._futures_rest("PUT", "/fapi/v1/batchOrders", params)
                if isinstance(results, list):
                    for j, r in enumerate(results):
                        if isinstance(r, dict) and r.get("code"):
                            msg = r.get("msg", "")
                            if "does not exist" in msg:
                                self.active_orders.pop(str(batch[j].order_id), None)
                            elif "No need to modify" not in msg:
                                logger.warning(f"改单跳过(orderID={batch[j].order_id}): {msg}")
                        elif isinstance(r, dict) and r.get("orderId"):
                            key = str(batch[j].order_id)
                            if key in self.active_orders:
                                self.active_orders[key].price = batch[j].new_price
                                self.active_orders[key].quantity = batch[j].new_qty
            except Exception as e:
                logger.warning(f"批量改单请求失败: {e}")

    async def _execute_batch_places(self, targets: list[TargetOrder]):
        if not targets:
            return

        post_only = self.s.get("postOnly", False)
        tif = "GTX" if post_only else "GTC"
        gtx_rejects = 0

        # GTX 价格保护
        if post_only:
            filtered = []
            for t in targets:
                if t.side == "SELL" and t.price <= self.bid_price:
                    continue
                if t.side == "BUY" and t.price >= self.ask_price:
                    continue
                filtered.append(t)
            targets = filtered
            if not targets:
                return

        # 每批最多5单
        for i in range(0, len(targets), 5):
            if tif == "GTX" and gtx_rejects >= 3:
                tif = "GTC"
                logger.warning(f"GTX 被拒 {gtx_rejects} 次，本轮降级 GTC")

            batch = targets[i:i+5]
            items = []
            cl_ord_ids = []
            for t in batch:
                cid = self._new_cid()
                cl_ord_ids.append(cid)
                items.append({
                    "symbol": self.symbol, "side": t.side, "type": "LIMIT",
                    "timeInForce": tif, "price": str(t.price),
                    "quantity": str(t.quantity), "newClientOrderId": cid,
                })

            # 预注册 pending
            for j, cid in enumerate(cl_ord_ids):
                self.active_orders[f"p:{cid}"] = ActiveOrder(
                    cl_ord_id=cid, side=batch[j].side,
                    price=batch[j].price, quantity=batch[j].quantity,
                    pending=True, sent_at=time.time())

            try:
                params = {"batchOrders": json.dumps(items)}
                results = await self._futures_rest("POST", "/fapi/v1/batchOrders", params)
                if isinstance(results, list):
                    for j, r in enumerate(results):
                        cid = cl_ord_ids[j]
                        if isinstance(r, dict) and r.get("code"):
                            msg = r.get("msg", "")
                            if "Post Only" in msg:
                                gtx_rejects += 1
                            elif "would immediately" not in msg:
                                logger.warning(f"下单失败: {msg}")
                            self.active_orders.pop(f"p:{cid}", None)
                        # 成功的不需要处理, ORDER_TRADE_UPDATE 会 pending -> active
            except Exception as e:
                logger.warning(f"批量下单请求失败: {e}")
                for cid in cl_ord_ids:
                    self.active_orders.pop(f"p:{cid}", None)

    # ─── decideOrders 主逻辑 ─────────────────────────────────────────────────

    def _decide_orders(self):
        if self.bid_price <= 0 or self.ask_price <= 0:
            return

        # 定期校准
        now = time.time()
        if now - self._last_reconcile >= 5:
            asyncio.ensure_future(self._reconcile_orders())
            self._last_reconcile = now

        target_buys, target_sells = self._calculate_target_orders(self.bid_price, self.ask_price)

        # 清理超时 pending
        stale = [k for k, ao in self.active_orders.items()
                 if ao.pending and time.time() - ao.sent_at > 5]
        for k in stale:
            self.active_orders.pop(k, None)

        active_buys = [ao for ao in self.active_orders.values() if ao.side == "BUY" and not ao.pending]
        active_sells = [ao for ao in self.active_orders.values() if ao.side == "SELL" and not ao.pending]

        price_tol = self.tick_size * int(self.s.get("priceTolerance", 0))
        buy_cancel, buy_replace, buy_place = self._diff_side(active_buys, target_buys, price_tol)
        sell_cancel, sell_replace, sell_place = self._diff_side(active_sells, target_sells, price_tol)

        all_cancels = buy_cancel + sell_cancel
        all_replaces = buy_replace + sell_replace
        all_places = buy_place + sell_place

        # 先撤 -> 再改 -> 后下
        if all_cancels or all_replaces or all_places:
            asyncio.ensure_future(self._execute_cycle(all_cancels, all_replaces, all_places))

    async def _execute_cycle(self, cancels: list[int], replaces: list[ReplaceOp], places: list[TargetOrder]):
        await self._execute_batch_cancels(cancels)
        await self._execute_batch_modifies(replaces)
        await self._execute_batch_places(places)

    # ─── 波动率熔断 (方向感知) ───────────────────────────────────────────────

    def _check_volatility(self):
        """返回 True 表示应跳过下单"""
        if not self.s.get("volEnabled", True):
            if self._vol_paused:
                self._vol_paused = False
            return False

        range_pct, ratio, direction, vol_ready = self.vol_tracker.get_state()
        max_range = float(self.s.get("volMaxRangePct", 0))
        safe_range = float(self.s.get("volSafeRangePct", 0))
        mult = float(self.s.get("volResumeMultiplier", 1.3) if self._vol_paused
                      else self.s.get("volPauseMultiplier", 3))

        abs_safe = safe_range > 0 and range_pct <= safe_range
        abs_ok = max_range <= 0 or range_pct <= max_range
        rel_ok = abs_safe or not vol_ready or ratio <= mult

        # 触发熔断
        if not self._vol_paused and not (abs_ok and rel_ok):
            should_trigger = True

            if self.s.get("volDirectionAware", False):
                pos = float(self.position_amt)
                max_pos = self._max_long() if pos >= 0 else self._max_short()
                pos_ratio = abs(pos) / max_pos if max_pos > 0 else 0
                favorable = pos * direction > 0
                threshold = float(self.s.get("volPositionThreshold", 0.3))
                if pos_ratio >= threshold and favorable:
                    should_trigger = False
                    logger.info(f"波动率高但方向有利(跳过): ratio={ratio:.1f}x dir={direction:.4f} "
                                f"pos={pos:.4f} posRatio={pos_ratio*100:.0f}%")

            if should_trigger:
                self._vol_paused = True
                asyncio.ensure_future(self._cancel_all_orders())
                if self.s.get("volCloseOnPause", True):
                    logger.warning(f"波动率熔断: rangePct={range_pct:.6f} ratio={ratio:.1f}x, 撤单+清仓")
                    asyncio.ensure_future(self._close_position())
                else:
                    logger.warning(f"波动率熔断: rangePct={range_pct:.6f} ratio={ratio:.1f}x, 撤单等待")

        # 解除熔断
        if self._vol_paused and abs_ok and rel_ok:
            self._vol_paused = False
            asyncio.ensure_future(self._cancel_all_orders())
            asyncio.ensure_future(self._sync_position())
            logger.info(f"波动率熔断解除: ratio={ratio:.1f}x rangePct={range_pct:.6f}")

        return self._vol_paused

    # ─── 活跃度熔断 ──────────────────────────────────────────────────────────

    def _check_activity(self):
        """返回 True 表示应跳过下单"""
        min_t = float(self.s.get("activityMinTrades", 0))
        if min_t <= 0 or not self.act_tracker or not self.act_tracker.ready:
            return self._act_paused

        min_tpm, max_tpm, min_cov = self.act_tracker.trade_stats()
        cfg_cov = float(self.s.get("activityMinCoverage", 0))
        inactive = max_tpm < min_t or (cfg_cov > 0 and min_cov < cfg_cov)

        if not self._act_paused and inactive:
            self._act_paused = True
            asyncio.ensure_future(self._cancel_all_orders())
            if self.s.get("activityCloseOnPause", True):
                logger.warning(f"成交清淡熔断: max={max_tpm:.0f} min={min_tpm:.0f} "
                               f"覆盖{min_cov*100:.0f}%, 撤单+清仓")
                asyncio.ensure_future(self._close_position())
            else:
                logger.warning(f"成交清淡熔断: max={max_tpm:.0f} min={min_tpm:.0f}, 撤单等待")
        elif self._act_paused:
            resume_t = float(self.s.get("activityResumeTrades", 0)) or min_t * 2
            if min_tpm >= resume_t and (cfg_cov <= 0 or min_cov >= cfg_cov):
                self._act_paused = False
                asyncio.ensure_future(self._cancel_all_orders())
                asyncio.ensure_future(self._sync_position())
                logger.info(f"成交活跃度恢复: min={min_tpm:.0f} max={max_tpm:.0f} 覆盖{min_cov*100:.0f}%")

        return self._act_paused

    # ─── 校准 + 持仓同步 ────────────────────────────────────────────────────

    async def _sync_position(self):
        try:
            positions = await self._futures_rest("GET", "/fapi/v2/positionRisk",
                                                 {"symbol": self.symbol})
            if isinstance(positions, list):
                for p in positions:
                    if p.get("symbol") == self.symbol and p.get("positionSide", "BOTH") in ("BOTH", ""):
                        self.position_amt = Decimal(p.get("positionAmt", "0"))
                        self.entry_price = Decimal(p.get("entryPrice", "0"))
                        logger.info(f"持仓同步 | 持仓={self.position_amt} | 入场价={self.entry_price}")
                        break
        except Exception as e:
            logger.warning(f"持仓查询失败: {e}")

    async def _reconcile_orders(self):
        try:
            orders = await self._futures_rest("GET", "/fapi/v1/openOrders", {"symbol": self.symbol})
            if not isinstance(orders, list):
                return

            await self._sync_position()

            real_orders: dict[str, ActiveOrder] = {}
            rest_buys = rest_sells = 0
            for o in orders:
                cid = o.get("clientOrderId", "")
                if not cid.startswith("fmm_"):
                    continue
                oid = o["orderId"]
                key = str(oid)
                price = Decimal(o["price"])
                qty = Decimal(o["origQty"])
                side = o["side"]
                if side == "BUY":
                    rest_buys += 1
                else:
                    rest_sells += 1
                real_orders[key] = ActiveOrder(cl_ord_id=cid, order_id=oid,
                                               side=side, price=price, quantity=qty)

            old_buys = sum(1 for ao in self.active_orders.values() if ao.side == "BUY" and not ao.pending)
            old_sells = sum(1 for ao in self.active_orders.values() if ao.side == "SELL" and not ao.pending)

            # 保留未超时 pending
            for k, ao in self.active_orders.items():
                if ao.pending and time.time() - ao.sent_at < 5:
                    real_orders[k] = ao
            self.active_orders = real_orders

            if old_buys != rest_buys or old_sells != rest_sells:
                logger.info(f"校准 | 跟踪: {old_buys}B+{old_sells}S -> 实际: {rest_buys}B+{rest_sells}S")

        except Exception as e:
            logger.warning(f"订单校准失败: {e}")

    async def _cancel_all_orders(self):
        try:
            await self._futures_rest("DELETE", "/fapi/v1/allOpenOrders", {"symbol": self.symbol})
            logger.info("批量撤单完成")
        except Exception as e:
            if "Unknown order" not in str(e):
                logger.warning(f"批量撤单失败: {e}")
        self.active_orders.clear()

    # ─── 平仓 ────────────────────────────────────────────────────────────────

    async def _close_position(self):
        await self._sync_position()
        qty = self._align_qty(abs(self.position_amt))
        if qty <= 0 or qty < self.min_qty:
            logger.info("无持仓需清理")
            return

        # 最小名义检查
        if self.bid_price > 0 and qty * self.bid_price < 5:
            logger.warning(f"持仓过小(notional={qty * self.bid_price:.2f} < 5), 跳过平仓")
            return

        side = "SELL" if self.position_amt > 0 else "BUY"
        cid = f"fmm_close_{int(time.time()*1000)}"
        logger.info(f"市价平仓 | {side} {qty}")
        try:
            result = await self._futures_rest("POST", "/fapi/v1/order", {
                "symbol": self.symbol, "side": side, "type": "MARKET",
                "quantity": str(qty), "newClientOrderId": cid,
            })
            if result.get("orderId"):
                logger.info(f"平仓成功 | orderId={result['orderId']} avgPrice={result.get('avgPrice', '?')}")
            else:
                logger.warning(f"平仓响应: {result}")
        except Exception as e:
            logger.error(f"平仓失败: {e} | 请手动平仓 {side} {qty}")

    # ─── BNB 手续费管理 ──────────────────────────────────────────────────────

    async def _monitor_bnb_loop(self):
        while not self.should_stop:
            await asyncio.sleep(60)
            try:
                await self._check_bnb()
            except Exception as e:
                logger.warning(f"BNB 检查失败: {e}")

    async def _check_bnb(self):
        min_bal = float(self.s.get("bnbMinBalance", 0))
        if min_bal <= 0:
            return

        # 查合约 BNB 余额
        balances = await self._futures_rest("GET", "/fapi/v2/balance")
        bnb_balance = Decimal("0")
        if isinstance(balances, list):
            for b in balances:
                if b.get("asset") == "BNB":
                    bnb_balance = Decimal(b.get("balance", "0"))
                    break

        if bnb_balance >= Decimal(str(min_bal)):
            return

        # 退出阈值检查
        exit_bal = float(self.s.get("bnbExitBalance", 0))
        if exit_bal > 0 and bnb_balance < Decimal(str(exit_bal)):
            logger.error(f"[BNB] 余额 {bnb_balance} < 退出阈值 {exit_bal}，停止策略")
            self.should_stop = True
            return

        buy_amt = str(self.s.get("bnbBuyAmount", 0.2))
        logger.info(f"[BNB] 合约余额 {bnb_balance} < {min_bal}，补充 {buy_amt} BNB...")

        # 现货市价买入
        try:
            await self._spot_rest("POST", "/api/v3/order", {
                "symbol": "BNBUSDT", "side": "BUY", "type": "MARKET", "quantity": buy_amt,
            })
            logger.info(f"[BNB] 现货买入成功")
        except Exception as e:
            logger.warning(f"[BNB] 现货买入失败: {e}")
            return

        # 划转到合约
        try:
            await self._spot_rest("POST", "/sapi/v1/asset/transfer", {
                "type": "MAIN_UMFUTURE", "asset": "BNB", "amount": buy_amt,
            })
            logger.info(f"[BNB] 划转 {buy_amt} BNB 到合约成功")
        except Exception as e:
            logger.warning(f"[BNB] 划转失败: {e}")

    # ─── CSV + 状态持久化 ────────────────────────────────────────────────────

    def _init_trade_csv(self):
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        csv_path = logs_dir / f"trades_{self.symbol}_{datetime.now():%Y%m%d_%H%M%S}.csv"
        self._csv_file = open(csv_path, "a", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow(["time", "side", "status", "price", "qty", "volume", "realized_pnl", "position"])
        logger.info(f"成交记录: {csv_path}")

    def _write_trade_csv(self, side: str, status: str, price: Decimal,
                         qty: Decimal, volume: Decimal, pnl: Decimal):
        if not self._csv_writer:
            return
        self._csv_writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            side, status, str(price), str(qty), str(volume), str(pnl), str(self.position_amt),
        ])
        self._csv_file.flush()

    def _state_file_path(self) -> Path:
        return Path(__file__).parent / f"fmm_state_{self.symbol}.json"

    def _save_state(self):
        try:
            self._state_file_path().write_text(json.dumps({
                "position_amt": str(self.position_amt),
                "entry_price": str(self.entry_price),
                "buy_volume": str(self.stats.buy_volume),
                "sell_volume": str(self.stats.sell_volume),
                "total_pnl": str(self.stats.total_pnl),
            }, indent=2))
        except Exception as e:
            logger.warning(f"保存状态失败: {e}")

    def _load_state(self):
        f = self._state_file_path()
        if not f.exists():
            return
        try:
            data = json.loads(f.read_text())
            self.stats.buy_volume = Decimal(data.get("buy_volume", "0"))
            self.stats.sell_volume = Decimal(data.get("sell_volume", "0"))
            self.stats.total_pnl = Decimal(data.get("total_pnl", "0"))
            if not self.stats.buy_volume.is_zero():
                logger.info(f"恢复状态 | 买入额={self.stats.buy_volume} | 卖出额={self.stats.sell_volume} | PnL={self.stats.total_pnl}")
        except Exception as e:
            logger.warning(f"恢复状态失败: {e}")

    # ─── 统计打印 ─────────────────────────────────────────────────────────────

    async def _print_stats_loop(self):
        while not self.should_stop:
            await asyncio.sleep(30)
            try:
                self._print_stats()
            except Exception as e:
                logger.debug(f"打印统计失败: {e}")

    def _print_stats(self):
        st = self.stats
        elapsed = int(time.time() - self._start_time)
        h, m, sec = elapsed // 3600, elapsed % 3600 // 60, elapsed % 60
        runtime = f"{h}h{m:02d}m{sec:02d}s" if h else f"{m}m{sec:02d}s"

        bc = sum(1 for ao in self.active_orders.values() if ao.side == "BUY" and not ao.pending)
        sc = sum(1 for ao in self.active_orders.values() if ao.side == "SELL" and not ao.pending)

        # 未实现 PnL
        unrealized = Decimal("0")
        if not self.position_amt.is_zero() and self.bid_price > 0:
            mid = (self.bid_price + self.ask_price) / 2
            unrealized = self.position_amt * (mid - self.entry_price)

        range_pct, ratio, direction, vol_ready = self.vol_tracker.get_state()
        imb = self._calc_imbalance(int(self.s.get("imbalanceDepth", 5)))
        vol_status = " [已熔断]" if self._vol_paused else ""
        act_status = " [已暂停]" if self._act_paused else ""

        buy_lvl, sell_lvl = self._calculate_levels()
        n = int(self.s.get("numOrdersEachSide", 5))

        logger.info(
            f"\n{'='*60}\n"
            f"===== 统计 ({runtime}) =====\n"
            f"BBO | bid={self.bid_price} ask={self.ask_price} spread={self.ask_price - self.bid_price}\n"
            f"交易 | 买入额={st.buy_volume:.2f} 卖出额={st.sell_volume:.2f} 总额={st.total_volume:.2f} 笔数={st.order_count}\n"
            f"PnL | 已实现={st.total_pnl:.4f} 未实现={unrealized:.4f} 总计={st.total_pnl + unrealized:.4f}\n"
            f"挂单 | 买={bc} 卖={sc} | 持仓={self.position_amt} 入场价={self.entry_price}\n"
            f"偏斜 | 买层={buy_lvl}/{n} 卖层={sell_lvl}/{n}\n"
            f"信号 | OBI={imb:.2f} 波动率={ratio:.1f}x(pct={range_pct:.6f}){vol_status}\n"
            f"活跃度{act_status}\n"
            f"{'='*60}"
        )
        st.save()

    # ─── 主循环 ──────────────────────────────────────────────────────────────

    async def _strategy_loop(self):
        # 等待初始深度
        for _ in range(40):
            if self.bid_price > 0:
                break
            await asyncio.sleep(0.5)

        interval = float(self.s.get("orderInterval", 1))
        while not self.should_stop:
            try:
                self._loop_count += 1
                self._reload_config()

                # 最大成交额
                max_vol = float(self.s.get("maxVolume", 0))
                if max_vol > 0 and self.stats.total_volume >= Decimal(str(max_vol)):
                    logger.warning(f"累计成交额 {self.stats.total_volume} >= {max_vol}, 停止")
                    self.should_stop = True
                    break

                # 热更新杠杆
                lev = self.cfg.get("leverage", 5)
                if lev > 0 and lev != self._current_leverage:
                    try:
                        result = await self._futures_rest("POST", "/fapi/v1/leverage",
                                                          {"symbol": self.symbol, "leverage": str(lev)})
                        self._current_leverage = lev
                        logger.info(f"杠杆热更新 | {lev}x | maxNotional={result.get('maxNotionalValue', '?')}")
                    except Exception as e:
                        logger.warning(f"杠杆热更新失败: {e}")

                # 热更新 interval
                new_interval = float(self.s.get("orderInterval", 1))
                if new_interval != interval:
                    interval = new_interval

                # 热更新 activity_window
                if self.act_tracker and (w := self.s.get("activityWindow", 0)):
                    self.act_tracker.window_min = max(w, 1)

                # 等待 BBO
                if self.bid_price <= 0 or self.ask_price <= 0:
                    await asyncio.sleep(1)
                    continue

                # 波动率熔断
                if self._check_volatility():
                    await asyncio.sleep(interval)
                    continue

                # 活跃度熔断
                if self._check_activity():
                    await asyncio.sleep(interval)
                    continue

                self._decide_orders()
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.should_stop:
                    return
                logger.error(f"策略循环异常: {e}", exc_info=True)
                try:
                    await self._cancel_all_orders()
                except Exception:
                    pass
                await asyncio.sleep(10)

    # ─── 生命周期 ─────────────────────────────────────────────────────────────

    async def run(self):
        self._start_time = time.time()
        try:
            await self.initialize()
            self._load_state()
            self._config_mtime = self._config_path.stat().st_mtime

            # 启动 WS
            asyncio.create_task(self._run_market_streams())
            asyncio.create_task(self._run_user_data_stream())
            asyncio.create_task(self._keepalive_listen_key_loop())

            logger.info("等待 WS 连接...")
            for _ in range(30):
                if self.bid_price > 0:
                    break
                await asyncio.sleep(0.5)
            if self.bid_price <= 0:
                logger.warning("BBO 数据未就绪，继续等待...")

            logger.info("初始化完成")

            self.strategy_tasks = [
                asyncio.create_task(self._strategy_loop()),
                asyncio.create_task(self._print_stats_loop()),
                asyncio.create_task(self._monitor_bnb_loop()),
            ]
            await asyncio.gather(*self.strategy_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"策略运行错误: {e}", exc_info=True)
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

        # 撤单
        try:
            await self._cancel_all_orders()
        except Exception as e:
            logger.warning(f"撤单失败: {e}")
        await asyncio.sleep(0.5)

        # 清仓
        if self.s.get("autoCloseOnExit", False):
            await self._close_position()

        # 保存
        self._save_state()
        self.stats.save()
        self._print_stats()

        if self._csv_file:
            self._csv_file.close()

        # 关闭 HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

        logger.info("策略已安全关闭")


# ─── 入口 ────────────────────────────────────────────────────────────────────────

def setup_logger(symbol: str):
    logger.remove()
    logger.add(sys.stdout,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
               level="INFO")
    log_file = Path(__file__).parent / f"logs/{symbol}_swap_volume.log"
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
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    # 凭证从 .env 读取 (与现货策略统一)
    cfg["apiKey"] = os.getenv("BINANCE_API_KEY", "")
    cfg["privateKeyPath"] = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    cfg["demo"] = os.getenv("BINANCE_DEMO", "0") == "1"

    assert cfg["apiKey"] and cfg["privateKeyPath"], \
        "请在 .env 配置 BINANCE_API_KEY 和 BINANCE_PRIVATE_KEY_PATH"

    symbol = cfg.get("symbol", "BTCUSDT")
    demo = cfg["demo"]
    setup_logger(symbol)
    asyncio.get_running_loop().set_exception_handler(handle_exception)

    s = cfg.get("strategy", {})
    mode = "Demo" if demo else "Live"
    logger.info(
        f"\n{'='*70}\n"
        f"币安合约做市/刷量策略 (从 Go futures_market_maker 移植)\n"
        f"交易对: {symbol} | 模式: {mode} | 杠杆: {cfg.get('leverage', 5)}x | 保证金: {cfg.get('marginType', 'CROSSED')}\n"
        f"网格: step={s.get('stepSize')} levels={s.get('numOrdersEachSide')} "
        f"qty={s.get('quantity')} growth={s.get('sizeGrowthFactor')}\n"
        f"持仓限制: long={s.get('maximumLong')} short={s.get('maximumShort')}\n"
        f"波动率: pause={s.get('volPauseMultiplier')}x resume={s.get('volResumeMultiplier')}x "
        f"close={s.get('volCloseOnPause')} dirAware={s.get('volDirectionAware')}\n"
        f"GTX: {s.get('postOnly', False)}\n"
        f"{'='*70}"
    )

    sv = SwapVolume(cfg)

    def signal_handler(_sig, _frame):
        logger.warning("收到停止信号, 正在安全退出...")
        sv.should_stop = True
        for task in sv.strategy_tasks:
            task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await sv.run()
    except KeyboardInterrupt:
        sv.should_stop = True
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("程序已退出")


if __name__ == "__main__":
    asyncio.run(main())
