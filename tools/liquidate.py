#!/usr/bin/env python3
"""批量清仓工具：查询余额 → 撤单 → 市价卖出（币安现货）"""
import os
import sys
import json
import time
import base64
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from datetime import datetime
from urllib.parse import urlencode

import aiohttp
import asyncio
from dotenv import load_dotenv
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

load_dotenv()

BASE_URL = "https://api.binance.com"
# 与现货策略保持一致 (币安账号内 Demo, 而非独立的 testnet.binance.vision)
DEMO_URL = "https://demo-api.binance.com"


class Liquidator:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper().replace('-', '')
        self.base_ccy = self._parse_base(self.symbol)

        self.api_key = os.getenv("BINANCE_API_KEY", "")
        key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
        self.demo = os.getenv("BINANCE_DEMO", "0") == "1"
        self.base_url = DEMO_URL if self.demo else BASE_URL

        if not self.api_key:
            raise ValueError("缺少 BINANCE_API_KEY，请检查 .env")
        if not key_path:
            raise ValueError("缺少 BINANCE_PRIVATE_KEY_PATH，请检查 .env")
        if not Path(key_path).exists():
            raise FileNotFoundError(f"私钥文件不存在: {key_path}")

        pk = load_pem_private_key(Path(key_path).read_bytes(), password=None)
        if not isinstance(pk, Ed25519PrivateKey):
            raise ValueError(f"不支持的密钥类型: {type(pk).__name__}，币安 API 仅支持 Ed25519")
        self.private_key = pk

        # 交易对精度信息
        self.step_size = Decimal('0')
        self.min_qty = Decimal('0')
        self.tick_size = Decimal('0')
        self.session = None

    def _parse_base(self, symbol: str) -> str:
        """从交易对推导基础币（简单规则：去掉常见报价币后缀）"""
        for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"):
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[:-len(quote)]
        return symbol[:3]

    def _sign(self, params: dict) -> dict:
        """Ed25519 签名"""
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = self.private_key.sign(query.encode())
        params["signature"] = base64.b64encode(sig).decode()
        return params

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    async def _get(self, path: str, params: dict = None, signed=False) -> dict:
        params = params or {}
        if signed:
            params = self._sign(params)
        url = f"{self.base_url}{path}"
        async with self.session.get(url, params=params, headers=self._headers()) as resp:
            return await resp.json()

    async def _post(self, path: str, params: dict = None, signed=True) -> dict:
        params = params or {}
        if signed:
            params = self._sign(params)
        url = f"{self.base_url}{path}"
        async with self.session.post(url, params=params, headers=self._headers()) as resp:
            return await resp.json()

    async def _delete(self, path: str, params: dict = None, signed=True) -> dict:
        params = params or {}
        if signed:
            params = self._sign(params)
        url = f"{self.base_url}{path}"
        async with self.session.delete(url, params=params, headers=self._headers()) as resp:
            return await resp.json()

    async def _load_exchange_info(self):
        """获取交易对精度"""
        data = await self._get("/api/v3/exchangeInfo", {"symbol": self.symbol})
        for sym in data.get("symbols", []):
            if sym["symbol"] == self.symbol:
                for f in sym.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        self.step_size = Decimal(f["stepSize"])
                        self.min_qty = Decimal(f["minQty"])
                    elif f["filterType"] == "PRICE_FILTER":
                        self.tick_size = Decimal(f["tickSize"])
                return
        raise ValueError(f"交易对 {self.symbol} 不存在")

    async def _get_balance(self) -> Decimal:
        """查询基础币可用余额"""
        data = await self._get("/api/v3/account", signed=True)
        for b in data.get("balances", []):
            if b["asset"] == self.base_ccy:
                return Decimal(b["free"])
        return Decimal('0')

    async def _cancel_open_orders(self) -> int:
        """撤销所有挂单"""
        result = await self._delete("/api/v3/openOrders", {"symbol": self.symbol})
        if isinstance(result, list):
            return len(result)
        # 没有挂单时可能返回 error
        return 0

    def _adjust_qty(self, qty: Decimal) -> Decimal:
        """对齐到 stepSize 精度"""
        if self.step_size > 0:
            qty = (qty // self.step_size) * self.step_size
        return qty.quantize(self.step_size, rounding=ROUND_DOWN)

    async def _market_sell(self, qty: Decimal) -> dict:
        """市价卖出"""
        params = {
            "symbol": self.symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(qty),
        }
        return await self._post("/api/v3/order", params)

    async def run(self):
        """执行清仓流程"""
        async with aiohttp.ClientSession() as session:
            self.session = session

            print(f"\n{'='*60}")
            print(f"  清仓工具 - {self.symbol} {'(模拟盘)' if self.demo else '(实盘)'}")
            print(f"{'='*60}\n")

            # 1. 获取交易对信息
            print(f"[1/4] 获取交易对 {self.symbol} 信息...")
            await self._load_exchange_info()
            print(f"      stepSize={self.step_size}, minQty={self.min_qty}")

            # 2. 撤销所有挂单
            print(f"\n[2/4] 撤销 {self.symbol} 所有挂单...")
            cancelled = await self._cancel_open_orders()
            print(f"      已撤销 {cancelled} 笔挂单")

            # 3. 查询余额
            print(f"\n[3/4] 查询 {self.base_ccy} 可用余额...")
            balance = await self._get_balance()
            print(f"      可用余额: {balance} {self.base_ccy}")

            # 4. 市价卖出
            adjusted = self._adjust_qty(balance)
            if adjusted < self.min_qty:
                print(f"\n[4/4] 可卖数量 {adjusted} 小于最小交易量 {self.min_qty}，跳过卖出")
                print(f"\n{'='*60}")
                print(f"  清仓完成（无需卖出）")
                print(f"{'='*60}\n")
                return

            print(f"\n[4/4] 市价卖出 {adjusted} {self.base_ccy}...")
            result = await self._market_sell(adjusted)

            if result.get("orderId"):
                fill_qty = result.get("executedQty", "0")
                fill_quote = result.get("cummulativeQuoteQty", "0")
                status = result.get("status", "")
                print(f"      ✅ 订单已提交: orderId={result['orderId']}, status={status}")
                print(f"      成交数量: {fill_qty} {self.base_ccy}")
                print(f"      成交金额: {fill_quote} USDT")

                # 保存记录
                os.makedirs('data', exist_ok=True)
                filename = f"data/liquidate_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                Path(filename).write_text(json.dumps(result, ensure_ascii=False, indent=2))
                print(f"      💾 记录已保存: {filename}")
            else:
                print(f"      ❌ 下单失败: {result.get('msg', result)}")

            print(f"\n{'='*60}")
            print(f"  清仓完成")
            print(f"{'='*60}\n")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="币安现货清仓工具")
    parser.add_argument("symbol", nargs="?", help="交易对（如 XAUTUSDT）")
    args = parser.parse_args()

    symbol = args.symbol or input("交易对 (如 XAUTUSDT): ").strip()
    if not symbol:
        print("❌ 未输入交易对")
        sys.exit(1)

    try:
        asyncio.run(Liquidator(symbol).run())
    except Exception as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
