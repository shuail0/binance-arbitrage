#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交易统计汇总 - 读 volume_state.json (项目根目录) 输出各交易对统计"""
import json
from decimal import Decimal
from pathlib import Path


def main():
    state_file = Path(__file__).resolve().parents[1] / "volume_state.json"
    if not state_file.exists():
        print(f"未找到 {state_file}")
        return

    data = json.loads(state_file.read_text())
    sep = "=" * 60
    print(f"{sep}\n{'交易统计汇总':^50}\n{sep}")

    total_buy = total_sell = total_pnl = Decimal("0")
    for symbol, stats in sorted(data.items()):
        buy = Decimal(str(stats.get("global_buy_volume", "0")))
        sell = Decimal(str(stats.get("global_sell_volume", "0")))
        pnl = Decimal(str(stats.get("global_pnl", "0")))
        print(f"\n📊 {symbol}")
        print(f"   买:{buy:.2f} 卖:{sell:.2f} 总:{buy+sell:.2f}  PnL:{pnl:+.6f}")
        print(f"   更新: {stats.get('updated_at', 'N/A')}")
        total_buy += buy
        total_sell += sell
        total_pnl += pnl

    print(f"\n{sep}")
    print(f"汇总 | 买:{total_buy:.2f} 卖:{total_sell:.2f} 总:{total_buy+total_sell:.2f}")
    print(f"总盈亏: {total_pnl:+.6f}")
    print(sep)


if __name__ == "__main__":
    main()
