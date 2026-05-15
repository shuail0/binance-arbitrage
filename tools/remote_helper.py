#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""远端助手脚本 - 由 strategy_manager.py SSH 调用

子命令:
    balance --accounts funding,spot,futures [--assets BTC,USDT]
    buy --symbol BNBUSDT --quantity 0.5
    buy --symbol BNBUSDT --quote 100

输出: JSON 到 stdout (单行)
凭证: 复用部署阶段写入的 .env (BINANCE_API_KEY / BINANCE_PRIVATE_KEY_PATH / BINANCE_DEMO)
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from strategies.common import (  # noqa: E402
    load_credentials, make_spot_client, make_futures_client,
)


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


def _sort_assets(items):
    items.sort(key=lambda x: (0, x["asset"]) if x["asset"] == "USDT" else (1, x["asset"]))
    return items


def _filter_items(raw_items, asset_filter):
    """raw_items: [{asset, free, locked}, ...]"""
    out = []
    for r in raw_items:
        a = (r.get("asset") or "").upper()
        if not a:
            continue
        if asset_filter and a not in asset_filter:
            continue
        free = str(r.get("free", "0") or "0")
        locked = str(r.get("locked", "0") or "0")
        if not asset_filter and _dec(free) + _dec(locked) <= 0:
            continue
        out.append({"asset": a, "free": free, "locked": locked})
    return _sort_assets(out)


def query_funding(spot, asset_filter):
    """资金账户 SAPI POST /sapi/v3/asset/getUserAsset

    绕过 SDK 的 send_signed_request (其在 Python 3.12 触发
    'function object not subscriptable' bug, send_request[T] 子脚本失效),
    直接调用底层 binance_common.utils.send_request.
    """
    try:
        from binance_common.utils import send_request
        resp = send_request(
            spot.rest_api._session,
            spot.rest_api.configuration,
            "POST",
            "/sapi/v3/asset/getUserAsset",
            payload={},
            is_signed=True,
            signer=spot.rest_api._signer,
        )
        raw = resp.data() if hasattr(resp, "data") else resp
        if not isinstance(raw, list):
            return {"err": f"unexpected response type: {type(raw).__name__}"}
        items = [
            {"asset": r.get("asset", ""), "free": r.get("free", "0"),
             "locked": r.get("locked", "0")}
            for r in raw
        ]
        return {"items": _filter_items(items, asset_filter)}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:280]}"}


def query_spot(spot, asset_filter):
    """现货账户 GET /api/v3/account"""
    try:
        resp = spot.rest_api.get_account()
        data = resp.data()
        items = [
            {"asset": b.asset, "free": str(b.free), "locked": str(b.locked)}
            for b in (data.balances or [])
        ]
        return {"items": _filter_items(items, asset_filter)}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:280]}"}


def query_futures(fut, asset_filter):
    """合约账户 GET /fapi/v3/balance, 用 availableBalance 作 free, balance-available 作 locked"""
    try:
        resp = fut.rest_api.futures_account_balance_v3()
        data = resp.data() or []
        items = []
        for r in data:
            asset = (r.asset or "").upper()
            if not asset:
                continue
            if asset_filter and asset not in asset_filter:
                continue
            balance = _dec(r.balance)
            avail = _dec(r.available_balance)
            locked = max(balance - avail, Decimal(0))
            if not asset_filter and balance <= 0:
                continue
            items.append({"asset": asset, "free": str(avail), "locked": f"{locked:.8f}"})
        return {"items": _sort_assets(items)}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:280]}"}


def cmd_balance(args):
    creds = load_credentials()
    spot = make_spot_client(creds, with_ws_api=False, with_ws_streams=False)
    fut = make_futures_client(creds, with_ws_streams=False)
    accounts = set(args.accounts.split(","))
    asset_filter = (
        {a.upper() for a in args.assets.split(",") if a.strip()}
        if args.assets else None
    )

    res = {"queriedAt": time.strftime("%Y-%m-%dT%H:%M:%S")}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {}
        if "funding" in accounts:
            futs["funding"] = ex.submit(query_funding, spot, asset_filter)
        if "spot" in accounts:
            futs["spot"] = ex.submit(query_spot, spot, asset_filter)
        if "futures" in accounts:
            futs["futures"] = ex.submit(query_futures, fut, asset_filter)
        for k, f in futs.items():
            res[k] = f.result()

    res["ok"] = any(
        "items" in (res.get(k) or {}) for k in ("funding", "spot", "futures")
    )
    print(json.dumps(res, ensure_ascii=False))


def cmd_buy(args):
    creds = load_credentials()
    spot = make_spot_client(creds, with_ws_api=False, with_ws_streams=False)

    cl_id = f"buy{int(time.time() * 1000) % 10**10:x}"
    kwargs = {
        "symbol": args.symbol.upper(),
        "side": "BUY",
        "type": "MARKET",
        "new_client_order_id": cl_id,
    }
    if args.quote:
        kwargs["quote_order_qty"] = str(args.quote)
        mode, param = "quoteOrderQty", str(args.quote)
    else:
        kwargs["quantity"] = str(args.quantity)
        mode, param = "quantity", str(args.quantity)

    res = {
        "ok": False, "symbol": args.symbol.upper(), "side": "BUY",
        "mode": mode, "param": param, "clientOrderId": cl_id,
    }
    t0 = time.time()
    try:
        resp = spot.rest_api.new_order(**kwargs)
        data = resp.data()
        res["ok"] = True
        res["orderId"] = getattr(data, "order_id", None)
        res["status"] = getattr(data, "status", None) or ""
        res["executedQty"] = str(getattr(data, "executed_qty", "") or "")
        res["cumQuoteQty"] = str(getattr(data, "cummulative_quote_qty", "") or "")

        # 计算加权均价 + 累计费 (base 类 + 其他类)
        fills = getattr(data, "fills", None) or []
        total_qty = total_quote = Decimal(0)
        fee_bnb = Decimal(0)
        fee_other = Decimal(0)
        other_ccy = ""
        for f in fills:
            p = _dec(getattr(f, "price", None))
            q = _dec(getattr(f, "qty", None))
            c = _dec(getattr(f, "commission", None))
            ca = (getattr(f, "commission_asset", None) or "").upper()
            total_qty += q
            total_quote += p * q
            if ca == "BNB":
                fee_bnb += c
            elif c > 0:
                fee_other += c
                other_ccy = ca
        if total_qty > 0:
            res["avgPrice"] = f"{total_quote / total_qty:.4f}"
        if fee_bnb > 0:
            res["feeBNB"] = str(fee_bnb)
        if fee_other > 0 and other_ccy:
            res["feeOther"] = f"{fee_other} {other_ccy}"
    except Exception as e:
        res["err"] = f"{type(e).__name__}: {str(e)[:280]}"

    res["timeMs"] = int((time.time() - t0) * 1000)
    print(json.dumps(res, ensure_ascii=False))


def main():
    p = argparse.ArgumentParser(description="远端助手: 余额查询 / 市价买入")
    sub = p.add_subparsers(dest="cmd", required=True)

    pb = sub.add_parser("balance", help="查询资金/现货/合约账户余额")
    pb.add_argument("--accounts", default="funding,spot,futures",
                    help="逗号分隔: funding,spot,futures")
    pb.add_argument("--assets", default="",
                    help="逗号分隔资产过滤, 留空=所有非零")
    pb.set_defaults(func=cmd_balance)

    pby = sub.add_parser("buy", help="现货市价买入")
    pby.add_argument("--symbol", default="BNBUSDT")
    g = pby.add_mutually_exclusive_group(required=True)
    g.add_argument("--quantity", help="按基础币数量买入")
    g.add_argument("--quote", help="按报价币 (USDT) 金额买入 quoteOrderQty")
    pby.set_defaults(func=cmd_buy)

    args = p.parse_args()
    try:
        args.func(args)
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "err": f"{type(e).__name__}: {str(e)[:300]}",
        }, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
