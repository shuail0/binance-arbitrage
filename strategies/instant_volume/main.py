#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""币安即时刷量策略

ask 限价买 → bid 限价卖 → 累计 max_volume 退出
事件驱动: executionReport 推送 → tracker.done.set() → 主循环继续 (零轮询)
"""
import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from strategies.common import load_credentials, make_spot_client
from strategies.common.ws_errors import parse_ws_error

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUOTE_FIATS = {"USDT", "USDC", "USD", "FDUSD", "TUSD", "BUSD"}


@dataclass
class Config:
    strategy_id: str
    symbol: str
    quantity: Decimal
    order_interval: float
    max_volume: Decimal
    order_timeout: int = 30
    max_spread: Decimal = Decimal("0")


@dataclass
class OrderTracker:
    """单笔订单的累计成交状态, 由 executionReport 累加, Event 通知终态."""
    cl_id: str
    cum_filled: Decimal = Decimal("0")
    cum_quote: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    fee_ccy: str = ""
    status: str = ""
    done: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def avg_price(self) -> Decimal:
        return self.cum_quote / self.cum_filled if self.cum_filled > 0 else Decimal("0")


class InstantVolume:
    def __init__(self, config: Config, config_path: Path):
        self.config = config
        self.config_path = config_path
        self._cfg_mtime = config_path.stat().st_mtime if config_path.exists() else 0

        creds = load_credentials()
        self.client = make_spot_client(creds)
        self.ws_api = None
        self.ws_streams = None

        # 行情
        self.bid = self.ask = Decimal("0")
        self.bid_qty = self.ask_qty = Decimal("0")
        self.last_tick: Optional[datetime] = None

        # 交易规则 (由 _fetch_rules 填)
        self.tick = self.lot = self.min_qty = self.min_notional = Decimal("0")
        self.base = self.quote = ""

        # 状态机: 单一 tracker 引用, 主循环串行设置
        self.tracker: Optional[OrderTracker] = None

        # 控制
        self.stop = False
        self.tasks: list[asyncio.Task] = []
        self._cl_counter = 0
        self.start_time = datetime.now()

        # 统计
        self.state_file = PROJECT_ROOT / "volume_state.json"
        self.buy_vol = self.sell_vol = self.pnl = Decimal("0")
        self.fee_base = self.fee_quote = Decimal("0")
        self.order_cnt = 0

    # ─── 工具 ─────────────────────────────────────

    def _next_cl(self, prefix: str) -> str:
        self._cl_counter += 1
        return f"{prefix}{int(datetime.now().timestamp() * 1000):x}{self._cl_counter:02x}"

    def _align_price(self, p: Decimal) -> str:
        return str((p / self.tick).quantize(Decimal("1")) * self.tick)

    def _align_qty(self, q: Decimal) -> str:
        return str((q / self.lot).quantize(Decimal("1"), rounding=ROUND_DOWN) * self.lot)

    # ─── 状态持久化 ────────────────────────────────

    def _load(self):
        if not self.state_file.exists():
            return
        try:
            d = json.loads(self.state_file.read_text()).get(self.config.symbol, {})
            self.buy_vol = Decimal(d.get("global_buy_volume", "0"))
            self.sell_vol = Decimal(d.get("global_sell_volume", "0"))
            self.pnl = Decimal(d.get("global_pnl", "0"))
            self.fee_base = Decimal(d.get("global_fee_base", "0"))
            self.fee_quote = Decimal(d.get("global_fee_quote", "0"))
            self.order_cnt = d.get("order_count", 0)
            logger.info(f"加载 [{self.config.symbol}] 买:{self.buy_vol} 卖:{self.sell_vol} 单:{self.order_cnt}")
        except Exception as e:
            logger.warning(f"加载状态失败: {e}")

    def _save(self):
        try:
            all_d = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
            all_d[self.config.symbol] = {
                "global_buy_volume": str(self.buy_vol),
                "global_sell_volume": str(self.sell_vol),
                "global_pnl": str(self.pnl),
                "global_fee_base": str(self.fee_base),
                "global_fee_quote": str(self.fee_quote),
                "order_count": self.order_cnt,
                "updated_at": datetime.now().isoformat(),
            }
            self.state_file.write_text(json.dumps(all_d, indent=2))
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

    # ─── REST 初始化 ──────────────────────────────

    async def _fetch_rules(self):
        from binance_sdk_spot.rest_api.models import PriceFilter, LotSizeFilter, NotionalFilter

        resp = await asyncio.to_thread(self.client.rest_api.exchange_info, symbol=self.config.symbol)
        data = resp.data()
        if not data.symbols:
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")
        sym = data.symbols[0]
        self.base, self.quote = sym.base_asset, sym.quote_asset

        for w in sym.filters:
            f = w.actual_instance
            if isinstance(f, PriceFilter):
                self.tick = Decimal(f.tick_size)
            elif isinstance(f, LotSizeFilter):
                self.lot = Decimal(f.step_size)
                self.min_qty = Decimal(f.min_qty)
            elif isinstance(f, NotionalFilter):
                self.min_notional = Decimal(f.min_notional)

        missing = [n for n, v in (("PRICE_FILTER", self.tick), ("LOT_SIZE.step", self.lot),
                                  ("LOT_SIZE.min_qty", self.min_qty),
                                  ("NOTIONAL.min_notional", self.min_notional)) if v <= 0]
        if missing:
            raise RuntimeError(f"{self.config.symbol} 缺失必要 filter: {missing}")
        if self.config.quantity < self.min_qty:
            raise RuntimeError(f"quantity={self.config.quantity} < min_qty={self.min_qty}")

        logger.info(
            f"规则 | tick={self.tick} step={self.lot} min_qty={self.min_qty} "
            f"min_notional={self.min_notional} | {self.base}/{self.quote}"
        )

    # ─── WS 初始化 ────────────────────────────────

    async def _setup_ws(self):
        self.ws_api = await self.client.websocket_api.create_connection()
        await self.ws_api.session_logon()

        self.ws_streams = await self.client.websocket_streams.create_connection()
        h = await self.client.websocket_streams.book_ticker(symbol=self.config.symbol.lower())
        h.on("message", self._on_book_ticker)

        r = await self.ws_api.user_data_stream_subscribe()
        r.stream.on("message", self._on_user_data)
        logger.info("WS API + Streams + UserData 已就绪")

    # ─── WS 回调 ──────────────────────────────────

    def _on_book_ticker(self, msg):
        if msg.b and msg.a:
            self.bid = Decimal(msg.b)
            self.ask = Decimal(msg.a)
            self.bid_qty = Decimal(msg.B or "0")
            self.ask_qty = Decimal(msg.A or "0")
            self.last_tick = datetime.now()

    def _on_user_data(self, ev):
        from binance_sdk_spot.websocket_api.models import (
            ExecutionReport, EventStreamTerminated,
        )
        x = ev.actual_instance
        if isinstance(x, ExecutionReport):
            self._update_tracker(x)
        elif isinstance(x, EventStreamTerminated):
            logger.warning("UserData 流被服务端终止, 停机")
            self.stop = True

    def _update_tracker(self, m):
        t = self.tracker
        if not t or m.c != t.cl_id:
            return
        t.cum_filled = Decimal(m.z or "0")
        t.cum_quote = Decimal(m.Z or "0")
        if m.n:
            t.fee += Decimal(m.n)
            t.fee_ccy = m.N or t.fee_ccy
        t.status = m.X or ""
        if t.status in ("FILLED", "CANCELED", "EXPIRED", "REJECTED"):
            t.done.set()

    # ─── 下单 / 撤单 / 余额 ───────────────────────

    async def _place(self, side: str, type_: str, qty: Decimal,
                     price: Optional[Decimal], cl_id: str) -> bool:
        try:
            kwargs = dict(symbol=self.config.symbol, side=side, type=type_,
                          quantity=self._align_qty(qty), new_client_order_id=cl_id)
            if type_ == "LIMIT":
                kwargs.update(time_in_force="GTC", price=self._align_price(price))
            resp = await asyncio.wait_for(self.ws_api.order_place(**kwargs), timeout=10)
            err, msg = parse_ws_error(resp.data())
            if err:
                logger.error(f"下单失败 {side} {type_} | code={err} msg={msg}")
                return False
            return True
        except asyncio.TimeoutError:
            logger.error(f"下单超时 {side} {type_}")
            return False
        except Exception as e:
            logger.error(f"下单异常 {side} {type_} | {getattr(e, 'status_code', '?')} {e}")
            return False

    async def _cancel(self, cl_id: str):
        try:
            await asyncio.wait_for(
                self.ws_api.order_cancel(symbol=self.config.symbol, orig_client_order_id=cl_id),
                timeout=5,
            )
        except Exception as e:
            logger.debug(f"撤单异常 {cl_id}: {e}")

    async def _balance(self) -> Decimal:
        try:
            resp = await asyncio.to_thread(self.client.rest_api.get_account)
            for b in resp.data().balances or []:
                if b.asset == self.base:
                    return Decimal(str(b.free))
        except Exception as e:
            logger.warning(f"查 {self.base} 余额失败: {e}")
        return Decimal("0")

    # ─── 核心: 单边成交 ────────────────────────────

    async def _fill_side(self, side: str, price: Decimal, qty: Decimal) -> OrderTracker:
        """下限价单 + 等成交. 超时撤单, 部分成交也算结束.
        失败时仍等 1s 防响应丢失但服务端已接受 → 不漏成交.
        """
        cl_id = self._next_cl(side[0].lower())
        self.tracker = OrderTracker(cl_id=cl_id)
        placed = await self._place(side, "LIMIT", qty, price, cl_id)
        wait_timeout = self.config.order_timeout if placed else 1.0
        try:
            await asyncio.wait_for(self.tracker.done.wait(), timeout=wait_timeout)
        except asyncio.TimeoutError:
            if placed:
                await self._cancel(cl_id)
                try:
                    await asyncio.wait_for(self.tracker.done.wait(), timeout=3)
                except asyncio.TimeoutError:
                    logger.warning(f"撤单后未收到推送 {cl_id}")
        t, self.tracker = self.tracker, None
        return t

    async def _market_sell(self, qty: Decimal, prefix: str = "m") -> OrderTracker:
        cl_id = self._next_cl(prefix)
        self.tracker = OrderTracker(cl_id=cl_id)
        ok = await self._place("SELL", "MARKET", qty, None, cl_id)
        if ok:
            try:
                await asyncio.wait_for(self.tracker.done.wait(), timeout=10)
            except asyncio.TimeoutError:
                logger.warning(f"市价单 10s 未推送 {cl_id}")
        t, self.tracker = self.tracker, None
        return t

    # ─── 主循环 ────────────────────────────────────

    def _can_trade(self) -> bool:
        if self.bid <= 0 or self.ask <= 0:
            return False
        if self.last_tick and (datetime.now() - self.last_tick).total_seconds() > 30:
            logger.warning("价格陈旧, 跳过")
            return False
        if self.config.max_spread > 0 and (self.ask - self.bid) > self.config.max_spread:
            logger.info(f"价差 {self.ask - self.bid} > {self.config.max_spread}, 跳过")
            return False
        q = self.config.quantity
        if (0 < self.bid_qty < q) or (0 < self.ask_qty < q):
            logger.info(f"BBO 量不足 bid={self.bid_qty} ask={self.ask_qty} < {q}, 跳过")
            return False
        return True

    async def _cycle(self):
        if not self._can_trade():
            return

        qty = self.config.quantity
        ask, bid = self.ask, self.bid

        # 1. 买
        logger.info(f"下买单 @ {ask} × {qty}")
        buy = await self._fill_side("BUY", ask, qty)
        if buy.cum_filled <= 0:
            return
        sellable = buy.cum_filled
        if buy.fee_ccy == self.base:
            sellable = max(sellable - buy.fee, Decimal("0"))
        self._record_buy(buy)

        # 2. 等结算 + REST 余额校正
        await asyncio.sleep(0.2)
        avail = await self._balance()
        sellable = min(sellable, avail) if avail > 0 else sellable
        if sellable < self.min_qty:
            logger.warning(f"可卖 {sellable} < min_qty, 跳过卖单")
            return

        # 3. 卖
        logger.info(f"下卖单 @ {bid} × {sellable}")
        sell = await self._fill_side("SELL", bid, sellable)
        if sell.cum_filled > 0:
            self._record_sell(buy.avg_price, sell)

        # 4. 部分未卖 → 市价兜底
        remaining = sellable - sell.cum_filled
        if remaining < self.min_qty:
            return
        avail = await self._balance()
        qty_close = min(remaining, avail) if avail > 0 else remaining
        if qty_close < self.min_qty:
            return
        logger.warning(f"市价兜底 {qty_close} (剩余 {remaining}, 余额 {avail})")
        m = await self._market_sell(qty_close)
        if m.cum_filled > 0:
            self._record_sell(buy.avg_price, m)

    def _record_buy(self, t: OrderTracker):
        self.buy_vol += t.cum_quote
        self._record_fee(t)
        logger.info(f"买成 {t.cl_id} @ {t.avg_price} × {t.cum_filled} 费={t.fee}{t.fee_ccy}")

    def _record_sell(self, buy_avg: Decimal, t: OrderTracker):
        self.sell_vol += t.cum_quote
        delta = (t.avg_price - buy_avg) * t.cum_filled
        self.pnl += delta
        self.order_cnt += 1
        self._record_fee(t)
        logger.info(
            f"卖成 {t.cl_id} @ {t.avg_price} × {t.cum_filled} "
            f"pnl={delta:+.6f} 累计={self.pnl:+.6f}"
        )

    def _record_fee(self, t: OrderTracker):
        if t.fee <= 0:
            return
        if t.fee_ccy.upper() in QUOTE_FIATS:
            self.fee_quote += t.fee
        else:
            self.fee_base += t.fee

    # ─── 后台: 统计 + 热加载 ──────────────────────

    async def _stats_loop(self):
        while not self.stop:
            await asyncio.sleep(30)
            self._print_stats("周期")
            self._save()

    # ─── 显示格式化工具 ──────────────────────────

    def _fmt_runtime(self) -> str:
        secs = int((datetime.now() - self.start_time).total_seconds())
        d, rem = divmod(secs, 86400)
        h, rem = divmod(rem, 3600)
        m, s = divmod(rem, 60)
        if d:
            return f"{d}d{h:02d}h{m:02d}m"
        if h:
            return f"{h}h{m:02d}m{s:02d}s"
        if m:
            return f"{m}m{s:02d}s"
        return f"{s}s"

    @staticmethod
    def _bar(pct: float, width: int = 24) -> str:
        pct = max(0.0, min(100.0, pct))
        filled = int(pct / 100 * width)
        return "█" * filled + "░" * (width - filled)

    @staticmethod
    def _fmt_eta(remaining: Decimal, per_min: Decimal) -> str:
        if per_min <= 0 or remaining <= 0:
            return "—"
        mins = int(remaining / per_min)
        if mins >= 1440:
            d, rem = divmod(mins, 1440)
            return f"{d}d{rem // 60:02d}h"
        if mins >= 60:
            h, m = divmod(mins, 60)
            return f"{h}h{m:02d}m"
        return f"{mins}m"

    def _fmt_price(self, p: Decimal) -> str:
        if self.tick <= 0:
            return str(p)
        dp = max(0, -self.tick.as_tuple().exponent)
        return f"{p:.{dp}f}"

    def _print_stats(self, tag: str = "周期"):
        elapsed = max((datetime.now() - self.start_time).total_seconds() / 60, 0.001)
        total = self.buy_vol + self.sell_vol
        cap = self.config.max_volume
        progress = float(total / cap * 100) if cap > 0 else 0.0
        remaining = max(cap - total, Decimal("0"))
        vol_per_min = Decimal(str(float(total) / elapsed))
        orders_per_min = self.order_cnt / elapsed
        # base 手续费按当前 bid 估值合并到 quote 计净 PnL
        fee_quote_total = self.fee_quote + self.fee_base * (self.bid or Decimal("0"))
        net = self.pnl - fee_quote_total
        spread = self.ask - self.bid if (self.ask > 0 and self.bid > 0) else Decimal("0")
        tick_age = (datetime.now() - self.last_tick).total_seconds() if self.last_tick else None
        sep = "═" * 78
        lines = [
            sep,
            f"  [{tag}] {self.config.strategy_id} · {self.config.symbol} · 已运行 {self._fmt_runtime()}",
            sep,
            f"  成交   买 {float(self.buy_vol):>12,.2f}    卖 {float(self.sell_vol):>12,.2f}    "
            f"总 {float(total):>12,.2f} {self.quote}",
            f"  进度   {self._bar(progress)}  {progress:5.1f}%",
            f"  目标   剩 {float(remaining):>10,.2f} / {float(cap):,.2f} {self.quote}    "
            f"ETA {self._fmt_eta(remaining, vol_per_min)}",
            f"  速率   {orders_per_min:>6.2f} 单/分    {float(vol_per_min):>9.2f} {self.quote}/分",
            f"  订单   {self.order_cnt:>6,d} 笔",
            f"  盈亏   价差 {float(self.pnl):+12.6f}    费 {float(fee_quote_total):>8.4f}    "
            f"净 {float(net):+12.6f} {self.quote}",
            f"  行情   bid {self._fmt_price(self.bid)}    ask {self._fmt_price(self.ask)}    "
            f"spread {self._fmt_price(spread)}",
        ]
        if tick_age is not None:
            lines.append(f"  心跳   {tick_age:.1f}s 前")
        lines.append(sep)
        logger.info("\n" + "\n".join(lines))

    async def _hot_reload(self):
        casters = {
            "quantity": lambda v: Decimal(str(v)),
            "order_interval": float,
            "order_timeout": int,
            "max_volume": lambda v: Decimal(str(v)),
            "max_spread": lambda v: Decimal(str(v or 0)),
        }
        while not self.stop:
            await asyncio.sleep(5)
            if not self.config_path.exists():
                continue
            mt = self.config_path.stat().st_mtime
            if mt <= self._cfg_mtime:
                continue
            self._cfg_mtime = mt
            try:
                d = yaml.safe_load(self.config_path.read_text()) or {}
                for k, cast in casters.items():
                    if k not in d:
                        continue
                    new, old = cast(d[k]), getattr(self.config, k)
                    if new != old:
                        setattr(self.config, k, new)
                        logger.info(f"热加载 {k}: {old} -> {new}")
            except Exception as e:
                logger.warning(f"热加载失败: {e}")

    # ─── 退出清仓 ──────────────────────────────────

    async def _shutdown(self):
        logger.info("退出清仓...")
        try:
            await asyncio.to_thread(
                self.client.rest_api.delete_open_orders, symbol=self.config.symbol
            )
        except Exception as e:
            if getattr(e, "status_code", None) != -2011:
                logger.warning(f"REST 撤单: {e}")
        await asyncio.sleep(0.5)
        avail = await self._balance()
        if avail >= self.min_qty:
            m = await self._market_sell(avail, prefix="e")
            if m.cum_filled > 0:
                logger.info(f"清仓卖出 {m.cum_filled} @ {m.avg_price}")
        self._save()
        self._print_stats("退出")

    # ─── 入口 ──────────────────────────────────────

    async def run(self):
        # 初始化阶段失败直接退出, 无状态可清
        try:
            self._load()
            await self._fetch_rules()
            await self._setup_ws()
        except Exception as e:
            logger.opt(exception=True).error(f"初始化失败: {e}")
            return

        logger.info("等待 BBO...")
        for _ in range(30):
            if self.ask > 0:
                break
            await asyncio.sleep(0.5)
        logger.info("策略启动")

        self.tasks = [
            asyncio.create_task(self._stats_loop()),
            asyncio.create_task(self._hot_reload()),
        ]
        try:
            while not self.stop:
                if (self.config.max_volume > 0
                        and self.buy_vol + self.sell_vol >= self.config.max_volume):
                    logger.warning(f"达到 max_volume={self.config.max_volume}, 退出")
                    break
                await self._cycle()
                await asyncio.sleep(self.config.order_interval)
        except Exception as e:
            logger.opt(exception=True).error(f"策略运行错误: {e}")
        finally:
            self.stop = True
            try:
                await self._shutdown()
            except Exception as e:
                logger.error(f"退出清仓异常: {e}")
            for t in self.tasks:
                t.cancel()
            for conn in (self.ws_api, self.ws_streams):
                if conn:
                    try:
                        await conn.close_connection()
                    except Exception:
                        pass


async def main():
    cfg_path = Path(os.getenv("CONFIG_FILE", str(Path(__file__).parent / "config.yaml")))
    raw = yaml.safe_load(cfg_path.read_text())
    config = Config(
        strategy_id=raw["strategy_id"],
        symbol=raw["symbol"],
        quantity=Decimal(str(raw["quantity"])),
        order_interval=float(raw["order_interval"]),
        max_volume=Decimal(str(raw["max_volume"])),
        order_timeout=int(raw.get("order_timeout", 30)),
        max_spread=Decimal(str(raw.get("max_spread", 0) or 0)),
    )

    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level="INFO")
    logger.add(
        log_dir / f"{config.strategy_id}_instant.log",
        rotation="00:00", retention="7 days", level="INFO",
    )

    sep = "═" * 78
    mode = "🧪 模拟盘" if os.getenv("BINANCE_DEMO", "0") == "1" else "💰 实盘"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(
        "\n" + "\n".join([
            sep,
            f"  币安即时刷量策略 · 启动 · {now_str}",
            sep,
            f"  策略     {config.strategy_id}",
            f"  交易对   {config.symbol}",
            f"  模式     {mode}",
            f"  数量     {config.quantity}    间隔 {config.order_interval}s    "
            f"超时 {config.order_timeout}s    价差上限 {config.max_spread}",
            f"  目标     {config.max_volume:,} (买+卖双边累计)",
            sep,
        ])
    )

    strat = InstantVolume(config, cfg_path)

    def _stop(*_):
        strat.stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        await strat.run()
    except KeyboardInterrupt:
        strat.stop = True
    finally:
        logger.info("策略已退出")


if __name__ == "__main__":
    asyncio.run(main())
