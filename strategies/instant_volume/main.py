#!/usr/bin/env python3
"""币安即时刷量策略 - 极简版
策略逻辑：卖一价FOK买入 → 买一价FOK卖出 → 达到maxVolume后清仓退出
通信：WS Streams(bookTicker) + WS API(order_place) + REST(exchangeInfo)
"""
import asyncio
import hashlib
import hmac
import json
import os
import signal
import time as time_mod
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import aiohttp
import websockets
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

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
    api_key: str = ""
    api_secret: str = ""  # PEM 文件路径
    demo: bool = False


class InstantVolumeStrategy:
    def __init__(self, config: StrategyConfig):
        self.config = config
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

        # WS API
        self.ws_api: Optional[websockets.WebSocketClientProtocol] = None
        self.ws_api_responses: dict[str, asyncio.Future] = {}
        self.ws_api_id_counter = 0

        # 控制
        self.should_stop = False

    def _load_private_key(self):
        """加载 Ed25519 私钥"""
        pem_path = self.config.api_secret
        if not pem_path or not os.path.exists(pem_path):
            raise FileNotFoundError(f"私钥文件不存在: {pem_path}")
        pem_data = Path(pem_path).read_bytes()
        self._private_key = load_pem_private_key(pem_data, password=None)

    def _sign(self, payload: str) -> str:
        """Ed25519 签名"""
        signature = self._private_key.sign(payload.encode())
        return b64encode(signature).decode()

    def _sign_params(self, params: dict) -> dict:
        """对参数进行签名，添加 timestamp + signature"""
        params["timestamp"] = str(int(time_mod.time() * 1000))
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

    # ==================== REST ====================

    async def fetch_exchange_info(self):
        """REST 获取 exchangeInfo"""
        url = f"{self.rest_base}/api/v3/exchangeInfo"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"symbol": self.config.symbol}) as resp:
                data = await resp.json()

        if not data.get("symbols"):
            raise RuntimeError(f"交易对 {self.config.symbol} 未找到")

        sym = data["symbols"][0]
        self.base_asset = sym["baseAsset"]
        for f in sym["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self.tick_size = Decimal(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                self.step_size = Decimal(f["stepSize"])

        logger.info(f"交易规则 | {self.config.symbol} | baseAsset={self.base_asset} | tickSize={self.tick_size} | stepSize={self.step_size}")

    async def rest_get_balance(self, asset: str) -> Decimal:
        """REST 查询可用余额"""
        url = f"{self.rest_base}/api/v3/account"
        params = self._sign_params({})
        headers = {"X-MBX-APIKEY": self.config.api_key}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as resp:
                data = await resp.json()
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return Decimal(b["free"])
        return Decimal("0")

    async def rest_place_market_sell(self, qty: str):
        """REST 市价卖出（清仓用）"""
        url = f"{self.rest_base}/api/v3/order"
        params = self._sign_params({
            "symbol": self.config.symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": qty,
        })
        headers = {"X-MBX-APIKEY": self.config.api_key}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, headers=headers) as resp:
                result = await resp.json()
                if resp.status == 200:
                    logger.info(f"市价卖出成功 | orderId={result.get('orderId')}")
                else:
                    logger.error(f"市价卖出失败: {result}")

    # ==================== WS Streams (bookTicker) ====================

    async def _run_book_ticker(self):
        """订阅 bookTicker 实时 BBO"""
        stream = f"{self.config.symbol.lower()}@bookTicker"
        url = f"{self.ws_stream_url}/ws/{stream}"
        while not self.should_stop:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info(f"WS Streams 已连接 | {stream}")
                    async for raw in ws:
                        if self.should_stop:
                            break
                        msg = json.loads(raw)
                        if "b" in msg and "a" in msg:
                            self.bid_price = Decimal(msg["b"])
                            self.ask_price = Decimal(msg["a"])
            except (websockets.ConnectionClosed, Exception) as e:
                if self.should_stop:
                    break
                logger.warning(f"WS Streams 断线: {e}，3秒后重连...")
                await asyncio.sleep(3)

    # ==================== WS API (order_place) ====================

    async def _run_ws_api(self):
        """维护 WS API 连接"""
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
        """发送 WS API 请求并等待响应"""
        if not self.ws_api:
            raise RuntimeError("WS API 未连接")

        self.ws_api_id_counter += 1
        req_id = f"req_{self.ws_api_id_counter}"

        # 签名
        params = self._sign_params(params)

        payload = {
            "id": req_id,
            "method": method,
            "params": {**params, "apiKey": self.config.api_key},
        }

        fut = asyncio.get_event_loop().create_future()
        self.ws_api_responses[req_id] = fut

        await self.ws_api.send(json.dumps(payload))

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.ws_api_responses.pop(req_id, None)
            raise

    async def ws_place_fok_order(self, side: str, price: str, quantity: str) -> dict:
        """WS API 下 FOK 限价单"""
        params = {
            "symbol": self.config.symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "FOK",
            "price": price,
            "quantity": quantity,
        }
        resp = await self._ws_api_request("order.place", params, timeout=self.config.order_timeout)
        return resp

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
                logger.info(f"加载状态 [{self.config.symbol}] | 买入={self.total_buy_volume} | 卖出={self.total_sell_volume}")
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

    # ==================== 核心逻辑 ====================

    async def execute_one_cycle(self):
        """一次买卖循环"""
        try:
            if self.ask_price <= 0 or self.bid_price <= 0:
                logger.warning("等待价格数据...")
                return

            # 价差检查
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
            try:
                buy_resp = await self.ws_place_fok_order("BUY", buy_price, aligned_qty)
            except asyncio.TimeoutError:
                logger.error("买单超时")
                return

            buy_result = buy_resp.get("result", {})
            buy_status = buy_result.get("status", "")
            if buy_status != "FILLED":
                logger.warning(f"买单未成交 | status={buy_status}")
                return

            # 提取买单成交信息
            buy_fill_price = self._extract_fill_price(buy_result, Decimal(buy_price))
            buy_fill_qty = Decimal(buy_result.get("executedQty", aligned_qty))
            buy_vol = buy_fill_price * buy_fill_qty
            self.total_buy_volume += buy_vol

            logger.info(f"买单成交 | {buy_fill_qty} @ {buy_fill_price} | 成交额={buy_vol:.2f}")

            # 2. FOK 卖单 @ bid
            sell_qty = self._align_qty(buy_fill_qty)
            logger.info(f"下卖单 | {sell_qty} @ {sell_price}")
            try:
                sell_resp = await self.ws_place_fok_order("SELL", sell_price, sell_qty)
            except asyncio.TimeoutError:
                logger.error("卖单超时，市价清仓")
                await self.rest_place_market_sell(sell_qty)
                return

            sell_result = sell_resp.get("result", {})
            sell_status = sell_result.get("status", "")
            if sell_status != "FILLED":
                logger.warning(f"卖单未成交 | status={sell_status}，市价清仓")
                await self.rest_place_market_sell(sell_qty)
                return

            # 提取卖单成交信息
            sell_fill_price = self._extract_fill_price(sell_result, Decimal(sell_price))
            sell_vol = sell_fill_price * buy_fill_qty
            pnl = (sell_fill_price - buy_fill_price) * buy_fill_qty

            self.total_sell_volume += sell_vol
            self.total_pnl += pnl
            self.order_count += 1

            pnl_str = f"+{pnl}" if pnl >= 0 else str(pnl)
            logger.info(f"卖单成交 | {sell_qty} @ {sell_fill_price} | 盈亏={pnl_str} | 累计={self.total_pnl:+.6f}")

        except Exception as e:
            logger.error(f"执行循环错误: {e}")

    def _extract_fill_price(self, result: dict, fallback: Decimal) -> Decimal:
        """从订单结果提取加权成交均价"""
        fills = result.get("fills", [])
        if fills:
            total_qty, total_quote = Decimal("0"), Decimal("0")
            for f in fills:
                p, q = Decimal(f["price"]), Decimal(f["qty"])
                total_quote += p * q
                total_qty += q
            if total_qty > 0:
                return total_quote / total_qty
        # fallback: 取 price 字段
        price_str = result.get("price", "")
        if price_str and Decimal(price_str) > 0:
            return Decimal(price_str)
        return fallback

    async def print_stats(self):
        """定时统计报告"""
        while not self.should_stop:
            await asyncio.sleep(30)
            total = self.total_buy_volume + self.total_sell_volume
            progress = (total / (self.config.max_volume * 2) * 100) if self.config.max_volume > 0 else 0
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
                if Decimal(aligned) > 0:
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
        """初始化：REST exchangeInfo + 加载状态 + 启动 WS"""
        await self.fetch_exchange_info()
        await self._load_state()

        # 启动 WS Streams 和 WS API
        asyncio.create_task(self._run_book_ticker())
        asyncio.create_task(self._run_ws_api())

        # 等待连接就绪
        logger.info("等待 WS 连接...")
        for _ in range(30):
            if self.ws_api and self.ask_price > 0:
                break
            await asyncio.sleep(0.5)

        if not self.ws_api:
            raise RuntimeError("WS API 连接超时")
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
        api_key=os.getenv("BINANCE_API_KEY", ""),
        api_secret=os.getenv("BINANCE_PRIVATE_KEY_PATH", ""),
        demo=os.getenv("BINANCE_DEMO", "0") == "1",
    )

    logger.add(
        f"logs/{config.strategy_id}_instant.log",
        rotation="00:00",
        retention="7 days",
        level="INFO",
    )

    mode = "Demo" if config.demo else "Live"
    logger.info(
        f"\n{'='*70}\n"
        f"币安即时刷量策略\n"
        f"策略ID: {config.strategy_id} | 交易对: {config.symbol}\n"
        f"数量: {config.quantity} | 价差上限: {config.max_spread}\n"
        f"间隔: {config.order_interval}s | 目标: {config.max_volume} USDT\n"
        f"模式: {mode}\n"
        f"{'='*70}"
    )

    strategy = InstantVolumeStrategy(config)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(strategy.shutdown()))

    await strategy.run()
    logger.info("策略已退出")


if __name__ == "__main__":
    asyncio.run(main())
