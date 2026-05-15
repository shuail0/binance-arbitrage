#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量清仓工具: 撤单 → 市价卖出 (币安现货, 本机直连 API)"""
import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from strategies.common.sdk_clients import load_credentials, make_spot_client

QUOTE_CCYS = ("USDT", "USDC", "FDUSD", "TUSD", "BUSD", "BTC", "ETH", "BNB")


def parse_base(symbol: str) -> str:
    for q in QUOTE_CCYS:
        if symbol.endswith(q) and len(symbol) > len(q):
            return symbol[:-len(q)]
    return symbol[:3]


def run(symbol: str):
    from binance_sdk_spot.rest_api.models import LotSizeFilter

    symbol = symbol.upper().replace("-", "")
    base = parse_base(symbol)
    creds = load_credentials()
    client = make_spot_client(creds, with_ws_api=False, with_ws_streams=False)

    sep = "=" * 60
    print(f"\n{sep}\n  清仓工具 - {symbol} {'(模拟盘)' if creds.demo else '(实盘)'}\n{sep}\n")

    # 1. 交易规则
    print(f"[1/4] 获取交易规则 {symbol}...")
    data = client.rest_api.exchange_info(symbol=symbol).data()
    sym = next((s for s in data.symbols if s.symbol == symbol), None)
    if not sym:
        raise ValueError(f"交易对 {symbol} 不存在")
    step = min_qty = Decimal("0")
    for w in sym.filters:
        f = w.actual_instance
        if isinstance(f, LotSizeFilter):
            step = Decimal(f.step_size)
            min_qty = Decimal(f.min_qty)
            break
    if step <= 0:
        raise RuntimeError(f"{symbol} 缺 LOT_SIZE filter")
    print(f"      step={step}  min_qty={min_qty}")

    # 2. 撤单
    print(f"\n[2/4] 撤所有 {symbol} 挂单...")
    try:
        canceled = client.rest_api.cancel_all_open_orders_on_a_symbol(symbol=symbol).data()
        n = len(canceled) if isinstance(canceled, list) else 1
    except Exception:
        n = 0
    print(f"      撤 {n} 笔")

    # 3. 余额
    print(f"\n[3/4] 查询 {base} 可用余额...")
    bal_data = client.rest_api.get_account().data()
    balance = next((Decimal(str(b.free)) for b in bal_data.balances if b.asset == base), Decimal("0"))
    print(f"      余额: {balance} {base}")

    # 4. 市价卖出
    qty = (balance / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step
    if qty < min_qty:
        print(f"\n[4/4] 可卖 {qty} < min_qty {min_qty}, 跳过")
        print(f"\n{sep}\n  清仓完成 (无需卖出)\n{sep}\n")
        return

    print(f"\n[4/4] 市价卖出 {qty} {base}...")
    result = client.rest_api.new_order(
        symbol=symbol, side="SELL", type="MARKET", quantity=str(qty),
    ).data()
    order_id = result.order_id
    print(f"      orderId={order_id} status={result.status}")
    print(f"      成交: {result.executed_qty} {base} = {result.cummulative_quote_qty} USDT")

    # 落盘记录
    Path("data").mkdir(exist_ok=True)
    fp = Path(f"data/liquidate_{symbol}_{datetime.now():%Y%m%d_%H%M%S}.json")
    fp.write_text(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))
    print(f"      记录: {fp}")
    print(f"\n{sep}\n  清仓完成\n{sep}\n")


def main():
    p = argparse.ArgumentParser(description="币安现货清仓工具")
    p.add_argument("symbol", nargs="?", help="交易对 (如 XAUTUSDT)")
    args = p.parse_args()
    symbol = args.symbol or input("交易对 (如 XAUTUSDT): ").strip()
    if not symbol:
        print("未输入交易对")
        sys.exit(1)
    try:
        run(symbol)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
