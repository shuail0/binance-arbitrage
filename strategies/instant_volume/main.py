#!/usr/bin/env python3
"""币安即时刷量策略 (SDK 版)

策略逻辑: 卖一价 FOK 买入 → 买一价 FOK 卖出 → 达到 maxVolume 后清仓退出
SDK: binance-sdk-spot (REST + WS API + WS Streams 全走官方)
"""
import asyncio
import json
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger

# 使项目根目录可 import strategies.common
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from strategies.common import load_credentials, make_spot_client
from strategies.common.ws_errors import parse_ws_error


logger.remove()
logger.add(lambda msg: print(msg, end=""), level="INFO")


@dataclass
class StrategyConfig:
    strategy_id: str
    symbol: str
    quantity: str
    max_spread: str
    order_interval: float
    max_volume: float
    order_timeout: int = 30


class InstantVolumeStrategy:
    def __init__(self, config: StrategyConfig):
        self.config = config
        creds = load_credentials()
        self.client = make_spot_client(creds)

        # 市场数据
        self.bid_price = Decimal("0")
        self.ask_price = Decimal("0")
        self.tick_size = Decimal("0.01")
        self.step_size = Decimal("0.001")
        self.base_asset = ""

        # 统计
        self.total_buy_volume = Decimal("0")
        self.total_sell_volume = Decimal("0")
        self.total_pnl = Decimal("0")
        self.order_count = 0

        # SDK 连接句柄
        self.ws_api_conn = None
        self.ws_streams_conn = None

        # 控制
        self.should_stop = False

    # ==================== 价格对齐 ====================

    def _align_price(self, price: Decimal) -> Decimal:
        if self.tick_size <= 0:
            return price
        return (price / self.tick_size).to_integral_value() * self.tick_size

    def _align_qty(self, qty: Decimal) -> Decimal:
        if self.step_size <= 0:
            return qty
        return (qty / self.step_size).to_integral_value() * self.step_size

    # ==================== REST ====================

    async def fetch_exchange_info(self):
        """REST 获取 exchangeInfo"""
        resp = await asyncio.to_thread(
            self.client.rest_api.exchange_info, symbol=self.config.symbol
        )
        data = resp.data()
        if not data.symbols:
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")

        sym = data.symbols[0]
        self.base_asset = sym.base_asset
        for f in sym.filters:
            # SDK 返回 Pydantic 模型, 过滤器是 Union 类型, 用属性访问
            ftype = getattr(f, "filter_type", None) or getattr(f, "filterType", None)
            if ftype == "PRICE_FILTER":
                self.tick_size = Decimal(str(f.tick_size))
            elif ftype == "LOT_SIZE":
                self.step_size = Decimal(str(f.step_size))

        logger.info(
            f"交易规则 | {self.config.symbol} | baseAsset={self.base_asset} | "
            f"tickSize={self.tick_size} | stepSize={self.step_size}"
        )

    async def rest_get_balance(self, asset: str) -> Decimal:
        """REST 查询可用余额"""
        resp = await asyncio.to_thread(self.client.rest_api.get_account)
        data = resp.data()
        for b in data.balances or []:
            if b.asset == asset:
                return Decimal(str(b.free))
        return Decimal("0")

    async def rest_place_market_sell(self, qty: Decimal):
        """REST 市价卖出 (清仓用)"""
        try:
            resp = await asyncio.to_thread(
                self.client.rest_api.new_order,
                symbol=self.config.symbol,
                side="SELL",
                type="MARKET",
                quantity=float(qty),
            )
            result = resp.data()
            order_id = getattr(result, "order_id", None) or getattr(result, "orderId", None)
            logger.info(f"市价卖出成功 | orderId={order_id}")
        except Exception as e:
            logger.error(f"市价卖出失败: {e}")

    # ==================== WS Streams (bookTicker) ====================

    async def subscribe_book_ticker(self):
        """订阅 bookTicker 实时 BBO (SDK 内置自动重连)"""
        self.ws_streams_conn = await self.client.websocket_streams.create_connection()
        handle = await self.ws_streams_conn.book_ticker(symbol=self.config.symbol.lower())
        handle.on("message", self._on_book_ticker)
        logger.info(f"WS Streams 已订阅 | {self.config.symbol}@bookTicker")

    def _on_book_ticker(self, msg):
        """bookTicker 回调 (SDK 返回 dict)"""
        try:
            if isinstance(msg, str):
                msg = json.loads(msg)
            # 字段可能在顶层或 data 子对象
            data = msg.get("data", msg) if isinstance(msg, dict) else msg
            b = data.get("b") if isinstance(data, dict) else None
            a = data.get("a") if isinstance(data, dict) else None
            if b and a:
                self.bid_price = Decimal(str(b))
                self.ask_price = Decimal(str(a))
        except Exception as e:
            logger.debug(f"解析 bookTicker 失败: {e}")

    # ==================== WS API (order_place FOK) ====================

    async def init_ws_api(self):
        """建立 WS API 连接 (SDK 自动签名 + 自动重连)"""
        self.ws_api_conn = await self.client.websocket_api.create_connection()
        logger.info("WS API 已连接")

    async def ws_place_fok_order(self, side: str, price: Decimal, quantity: Decimal) -> dict:
        """WS API 下 FOK 限价单

        Returns:
            {'ok': bool, 'result': Pydantic 模型, 'error_code': int, 'error_msg': str}
        """
        try:
            resp = await asyncio.wait_for(
                self.ws_api_conn.order_place(
                    symbol=self.config.symbol,
                    side=side,
                    type="LIMIT",
                    time_in_force="FOK",
                    price=float(price),
                    quantity=float(quantity),
                ),
                timeout=self.config.order_timeout,
            )
            data = resp.data()
            # 检查是否是错误响应
            err_code, err_msg = parse_ws_error(data)
            if err_code:
                return {"ok": False, "error_code": err_code, "error_msg": err_msg}
            return {"ok": True, "result": data}
        except asyncio.TimeoutError:
            return {"ok": False, "error_code": -999, "error_msg": "timeout"}
        except Exception as e:
            return {"ok": False, "error_code": -1, "error_msg": str(e)}

    # ==================== 状态持久化 ====================

    async def _load_state(self):
        state_file = Path(__file__).parent / "volume_state.json"
        if state_file.exists():
            try:
                all_data = json.loads(state_file.read_text())
                state = all_data.get(self.config.symbol, {})
                self.total_buy_volume = Decimal(str(state.get("global_buy_volume", 0)))
                self.total_sell_volume = Decimal(str(state.get("global_sell_volume", 0)))
                self.total_pnl = Decimal(str(state.get("global_pnl", 0)))
                self.order_count = state.get("order_count", 0)
                logger.info(
                    f"加载状态 [{self.config.symbol}] | 买入={self.total_buy_volume} | "
                    f"卖出={self.total_sell_volume}"
                )
            except Exception as e:
                logger.warning(f"加载状态失败: {e}")

    async def _save_state(self):
        state_file = Path(__file__).parent / "volume_state.json"
        try:
            all_data = json.loads(state_file.read_text()) if state_file.exists() else {}
            all_data[self.config.symbol] = {
                "global_buy_volume": str(self.total_buy_volume),
                "global_sell_volume": str(self.total_sell_volume),
                "global_pnl": str(self.total_pnl),
                "order_count": self.order_count,
                "updated_at": datetime.now().isoformat(),
            }
            state_file.write_text(json.dumps(all_data, indent=2))
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

    # ==================== 响应解析 ====================

    @staticmethod
    def _get_attr(obj, *names, default=None):
        """兼容 Pydantic 属性 / dict key 两种访问方式"""
        for n in names:
            if obj is None:
                return default
            v = getattr(obj, n, None)
            if v is not None:
                return v
            if isinstance(obj, dict):
                v = obj.get(n)
                if v is not None:
                    return v
        return default

    def _extract_fill_price(self, result, fallback: Decimal) -> Decimal:
        """从订单结果提取加权成交均价 (兼容 Pydantic 和 dict)"""
        fills = self._get_attr(result, "fills") or []
        if fills:
            total_qty, total_quote = Decimal("0"), Decimal("0")
            for f in fills:
                p = Decimal(str(self._get_attr(f, "price", default="0")))
                q = Decimal(str(self._get_attr(f, "qty", default="0")))
                total_quote += p * q
                total_qty += q
            if total_qty > 0:
                return total_quote / total_qty
        # fallback: price 字段
        price = self._get_attr(result, "price")
        if price:
            d = Decimal(str(price))
            if d > 0:
                return d
        return fallback

    # ==================== 核心循环 ====================

    async def execute_one_cycle(self):
        """一次买卖循环"""
        try:
            if self.ask_price <= 0 or self.bid_price <= 0:
                logger.warning("等待价格数据...")
                return

            spread = self.ask_price - self.bid_price
            max_spread = Decimal(self.config.max_spread)
            if spread > max_spread:
                logger.info(f"价差过大 | 当前={spread} | 最大={max_spread} | 跳过")
                return

            qty = Decimal(self.config.quantity)
            buy_price = self._align_price(self.ask_price)
            sell_price = self._align_price(self.bid_price)
            aligned_qty = self._align_qty(qty)

            # 1. FOK 买单 @ ask
            logger.info(f"下买单 | {aligned_qty} @ {buy_price} | 价差={spread}")
            buy_resp = await self.ws_place_fok_order("BUY", buy_price, aligned_qty)
            if not buy_resp["ok"]:
                logger.warning(
                    f"买单失败 | code={buy_resp['error_code']} msg={buy_resp['error_msg']}"
                )
                return

            buy_result = buy_resp["result"]
            buy_status = self._get_attr(buy_result, "status")
            if buy_status != "FILLED":
                logger.warning(f"买单未成交 | status={buy_status}")
                return

            buy_fill_price = self._extract_fill_price(buy_result, buy_price)
            buy_fill_qty = Decimal(
                str(self._get_attr(buy_result, "executed_qty", "executedQty",
                                   default=str(aligned_qty)))
            )
            buy_vol = buy_fill_price * buy_fill_qty
            self.total_buy_volume += buy_vol
            logger.info(
                f"买单成交 | {buy_fill_qty} @ {buy_fill_price} | 成交额={buy_vol:.2f}"
            )

            # 2. FOK 卖单 @ bid
            sell_qty = self._align_qty(buy_fill_qty)
            logger.info(f"下卖单 | {sell_qty} @ {sell_price}")
            sell_resp = await self.ws_place_fok_order("SELL", sell_price, sell_qty)
            if not sell_resp["ok"]:
                logger.error(
                    f"卖单失败 | code={sell_resp['error_code']} msg={sell_resp['error_msg']}，市价清仓"
                )
                await self.rest_place_market_sell(sell_qty)
                return

            sell_result = sell_resp["result"]
            sell_status = self._get_attr(sell_result, "status")
            if sell_status != "FILLED":
                logger.warning(f"卖单未成交 | status={sell_status}，市价清仓")
                await self.rest_place_market_sell(sell_qty)
                return

            sell_fill_price = self._extract_fill_price(sell_result, sell_price)
            sell_fill_qty = Decimal(
                str(self._get_attr(sell_result, "executed_qty", "executedQty",
                                   default=str(sell_qty)))
            )
            sell_vol = sell_fill_price * sell_fill_qty
            pnl = (sell_fill_price - buy_fill_price) * sell_fill_qty

            self.total_sell_volume += sell_vol
            self.total_pnl += pnl
            self.order_count += 1

            pnl_str = f"+{pnl}" if pnl >= 0 else str(pnl)
            logger.info(
                f"卖单成交 | {sell_fill_qty} @ {sell_fill_price} | 盈亏={pnl_str} | "
                f"累计={self.total_pnl:+.6f}"
            )

        except Exception as e:
            logger.error(f"执行循环错误: {e}")

    async def print_stats(self):
        """定时统计报告"""
        while not self.should_stop:
            await asyncio.sleep(30)
            total = self.total_buy_volume + self.total_sell_volume
            progress = (
                (total / (self.config.max_volume * 2) * 100)
                if self.config.max_volume > 0 else 0
            )
            logger.info(
                f"\n{'='*70}\n"
                f"统计报告\n"
                f"买入额: {self.total_buy_volume:.2f} | 卖出额: {self.total_sell_volume:.2f}\n"
                f"总交易量: {total:.2f} USDT | 进度: {progress:.1f}%\n"
                f"订单数: {self.order_count} | 累计盈亏: {self.total_pnl:+.6f}\n"
                f"当前价格: bid={self.bid_price} | ask={self.ask_price}\n"
                f"{'='*70}"
            )
            await self._save_state()

    async def close_all_and_exit(self):
        """清仓并退出"""
        logger.warning("达到 maxVolume，开始清仓...")
        if self.base_asset:
            try:
                free = await self.rest_get_balance(self.base_asset)
                aligned = self._align_qty(free)
                if aligned > 0:
                    logger.info(f"清仓卖出 | {aligned} {self.base_asset}")
                    await self.rest_place_market_sell(aligned)
            except Exception as e:
                logger.error(f"清仓失败: {e}")
        await self._save_state()
        total = self.total_buy_volume + self.total_sell_volume
        logger.info(
            f"\n{'='*70}\n"
            f"策略完成\n"
            f"买入额: {self.total_buy_volume:.2f} | 卖出额: {self.total_sell_volume:.2f}\n"
            f"总交易量: {total:.2f} USDT | 订单数: {self.order_count}\n"
            f"总盈亏: {self.total_pnl:+.6f}\n"
            f"{'='*70}"
        )
        self.should_stop = True

    async def initialize(self):
        """初始化: REST exchangeInfo + 加载状态 + 启动 WS 连接"""
        await self.fetch_exchange_info()
        await self._load_state()

        await self.init_ws_api()
        await self.subscribe_book_ticker()

        # 等待 bookTicker 第一条数据
        logger.info("等待 BBO 数据...")
        for _ in range(30):
            if self.ask_price > 0:
                break
            await asyncio.sleep(0.5)
        if self.ask_price <= 0:
            logger.warning("BBO 数据未就绪，继续等待...")

        logger.info("初始化完成，策略启动")

    async def run(self):
        try:
            await self.initialize()
            asyncio.create_task(self.print_stats())

            while not self.should_stop:
                total = self.total_buy_volume + self.total_sell_volume
                if self.config.max_volume > 0 and total >= Decimal(str(self.config.max_volume)) * 2:
                    await self.close_all_and_exit()
                    break
                await self.execute_one_cycle()
                await asyncio.sleep(self.config.order_interval)
        except Exception as e:
            logger.error(f"策略运行错误: {e}")
        finally:
            await self._save_state()
            # 关闭 SDK 连接
            for conn in (self.ws_api_conn, self.ws_streams_conn):
                if conn:
                    try:
                        await conn.close_connection()
                    except Exception:
                        pass

    async def shutdown(self):
        """安全退出"""
        logger.warning("收到停止信号，安全退出...")
        self.should_stop = True
        await self.close_all_and_exit()


async def main():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    config = StrategyConfig(
        strategy_id=cfg["strategy_id"],
        symbol=cfg["symbol"],
        quantity=str(cfg["quantity"]),
        max_spread=str(cfg.get("maxSpread", "0.01")),
        order_interval=cfg.get("orderInterval", 1),
        max_volume=cfg.get("maxVolume", 0),
        order_timeout=cfg.get("orderTimeout", 30),
    )

    logger.add(
        f"logs/{config.strategy_id}_instant.log",
        rotation="00:00",
        retention="7 days",
        level="INFO",
    )

    logger.info(
        f"\n{'='*70}\n"
        f"币安即时刷量策略 (SDK 版)\n"
        f"策略ID: {config.strategy_id} | 交易对: {config.symbol}\n"
        f"数量: {config.quantity} | 价差上限: {config.max_spread}\n"
        f"间隔: {config.order_interval}s | 目标: {config.max_volume} USDT\n"
        f"{'='*70}"
    )

    strategy = InstantVolumeStrategy(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(strategy.shutdown()))

    await strategy.run()
    logger.info("策略已退出")


if __name__ == "__main__":
    asyncio.run(main())
