#!/usr/bin/env python3
"""交易统计汇总工具 - 读取本地 volume_state.json 输出统计"""
import json
from pathlib import Path
from decimal import Decimal


def main():
    state_file = Path("volume_state.json")
    if not state_file.exists():
        print("未找到 volume_state.json")
        return

    data = json.loads(state_file.read_text())
    print(f"{'='*60}")
    print(f"{'交易统计汇总':^50}")
    print(f"{'='*60}")

    total_buy = total_sell = total_pnl = Decimal('0')
    for symbol, stats in sorted(data.items()):
        buy = Decimal(str(stats.get('global_buy_volume', '0')))
        sell = Decimal(str(stats.get('global_sell_volume', '0')))
        pnl = Decimal(str(stats.get('global_pnl', '0')))
        total = buy + sell
        updated = stats.get('updated_at', 'N/A')

        print(f"\n📊 {symbol}")
        print(f"   买入: {buy:.2f} | 卖出: {sell:.2f} | 总计: {total:.2f}")
        print(f"   盈亏: {pnl:+.6f}")
        print(f"   更新: {updated}")

        total_buy += buy
        total_sell += sell
        total_pnl += pnl

    print(f"\n{'='*60}")
    print(f"汇总 | 买入: {total_buy:.2f} | 卖出: {total_sell:.2f} | 总计: {total_buy + total_sell:.2f}")
    print(f"总盈亏: {total_pnl:+.6f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
