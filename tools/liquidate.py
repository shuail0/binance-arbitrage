#!/usr/bin/env python3
"""批量清仓工具：查询余额 → 撤单 → 市价卖出（币安现货）"""
import sys
import json
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.common.sdk_clients import load_credentials, make_spot_client


class Liquidator:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper().replace('-', '')
        self.base_ccy = self._parse_base(self.symbol)

        creds = load_credentials()
        self.demo = creds.demo
        self.client = make_spot_client(creds, with_ws_api=False, with_ws_streams=False)

        self.step_size = Decimal('0')
        self.min_qty = Decimal('0')

    def _parse_base(self, symbol: str) -> str:
        for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"):
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[:-len(quote)]
        return symbol[:3]

    def _load_exchange_info(self):
        data = self.client.rest_api.exchange_info(symbol=self.symbol).data()
        sym = next((s for s in data.symbols if s.symbol == self.symbol), None)
        if not sym:
            raise ValueError(f"交易对 {self.symbol} 不存在")
        for f in sym.filters:
            ft = getattr(f, "filter_type", None) or getattr(f, "filterType", None)
            if ft == "LOT_SIZE":
                self.step_size = Decimal(str(getattr(f, "step_size", None) or getattr(f, "stepSize", "0")))
                self.min_qty = Decimal(str(getattr(f, "min_qty", None) or getattr(f, "minQty", "0")))

    def _get_balance(self) -> Decimal:
        data = self.client.rest_api.get_account().data()
        for b in getattr(data, "balances", []):
            if getattr(b, "asset", None) == self.base_ccy:
                return Decimal(str(getattr(b, "free", "0") or "0"))
        return Decimal('0')

    def _cancel_open_orders(self) -> int:
        try:
            data = self.client.rest_api.cancel_all_open_orders_on_a_symbol(symbol=self.symbol).data()
            return len(data) if isinstance(data, list) else 1
        except Exception:
            return 0

    def _adjust_qty(self, qty: Decimal) -> Decimal:
        if self.step_size > 0:
            qty = (qty // self.step_size) * self.step_size
        return qty.quantize(self.step_size, rounding=ROUND_DOWN)

    def _market_sell(self, qty: Decimal):
        return self.client.rest_api.new_order(
            symbol=self.symbol, side="SELL", type="MARKET", quantity=float(qty)
        ).data()

    def run(self):
        print(f"\n{'='*60}")
        print(f"  清仓工具 - {self.symbol} {'(模拟盘)' if self.demo else '(实盘)'}")
        print(f"{'='*60}\n")

        print(f"[1/4] 获取交易对 {self.symbol} 信息...")
        self._load_exchange_info()
        print(f"      stepSize={self.step_size}, minQty={self.min_qty}")

        print(f"\n[2/4] 撤销 {self.symbol} 所有挂单...")
        cancelled = self._cancel_open_orders()
        print(f"      已撤销 {cancelled} 笔挂单")

        print(f"\n[3/4] 查询 {self.base_ccy} 可用余额...")
        balance = self._get_balance()
        print(f"      可用余额: {balance} {self.base_ccy}")

        adjusted = self._adjust_qty(balance)
        if adjusted < self.min_qty:
            print(f"\n[4/4] 可卖数量 {adjusted} 小于最小交易量 {self.min_qty}，跳过卖出")
            print(f"\n{'='*60}\n  清仓完成（无需卖出）\n{'='*60}\n")
            return

        print(f"\n[4/4] 市价卖出 {adjusted} {self.base_ccy}...")
        result = self._market_sell(adjusted)

        order_id = getattr(result, "order_id", None) or getattr(result, "orderId", None)
        if order_id:
            fill_qty = getattr(result, "executed_qty", None) or getattr(result, "executedQty", "0")
            fill_quote = getattr(result, "cummulative_quote_qty", None) or getattr(result, "cummulativeQuoteQty", "0")
            status = getattr(result, "status", "")
            print(f"      订单已提交: orderId={order_id}, status={status}")
            print(f"      成交数量: {fill_qty} {self.base_ccy}")
            print(f"      成交金额: {fill_quote} USDT")

            Path("data").mkdir(exist_ok=True)
            filename = f"data/liquidate_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            try:
                result_dict = result.model_dump()
            except Exception:
                result_dict = {"orderId": str(order_id)}
            Path(filename).write_text(json.dumps(result_dict, ensure_ascii=False, indent=2))
            print(f"      记录已保存: {filename}")
        else:
            print(f"      下单失败: {result}")

        print(f"\n{'='*60}\n  清仓完成\n{'='*60}\n")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="币安现货清仓工具")
    parser.add_argument("symbol", nargs="?", help="交易对（如 XAUTUSDT）")
    args = parser.parse_args()

    symbol = args.symbol or input("交易对 (如 XAUTUSDT): ").strip()
    if not symbol:
        print("未输入交易对")
        sys.exit(1)

    try:
        Liquidator(symbol).run()
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
