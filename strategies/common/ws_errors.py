"""WebSocket API 错误响应解析工具

**背景 (坑 #1)**:
官方 SDK 在 WS API 层把错误吞成了字符串:
    {"error": "Error received from server: {'code': -5022, 'msg': '...'}"}
而不是:
    {"error": {"code": -5022, "msg": "..."}}

本模块提供正则 helper 从字符串里抽出 error code 和 msg,
供业务层判断 GTX 拒单 (-5022)、已成交 (-2011) 等情况.
"""
import re
from typing import Any

# 匹配形如: 'code': -5022, 'msg': 'Order would immediately match and take.'
_ERR_RE = re.compile(r"'code':\s*(-?\d+),\s*'msg':\s*'([^']*)'")


def parse_ws_error(resp_data: Any) -> tuple[int, str]:
    """从 SDK WS API 响应中抽出 (error_code, error_msg)

    Args:
        resp_data: SDK WebsocketApiResponse.data() 的返回值,
                   正常成功时是 Pydantic 模型, 失败时可能是 dict 或带 error 字符串的对象

    Returns:
        (code, msg): 若无错误或解析失败返回 (0, "")
                     典型 code: -5022 (Post Only 拒单), -2011 (订单已成交/撤单)
    """
    if resp_data is None:
        return 0, ""

    # SDK 成功响应是 Pydantic 模型, 没有 'error' 属性
    err_str = None
    if isinstance(resp_data, dict):
        err_str = resp_data.get("error")
    elif hasattr(resp_data, "error"):
        err_str = getattr(resp_data, "error", None)
    elif hasattr(resp_data, "get"):
        err_str = resp_data.get("error")

    if not err_str:
        return 0, ""

    m = _ERR_RE.search(str(err_str))
    if m:
        return int(m.group(1)), m.group(2)
    return 0, str(err_str)


def is_error_response(resp_data: Any) -> bool:
    """快速判断响应是否是错误"""
    code, _ = parse_ws_error(resp_data)
    return code != 0


# ─── 常见 Binance 错误码 (便于业务层命名判断) ────────────────────────────────

ERR_POST_ONLY_REJECT = -5022      # GTX 拒单 (会立即成交)
ERR_UNKNOWN_ORDER = -2011         # 订单已成交/已取消/不存在
ERR_DUPLICATE_ORDER = -4045       # clOrdId 重复
ERR_INSUFFICIENT_MARGIN = -2019   # 保证金不足
ERR_MIN_NOTIONAL = -1013          # 最小名义金额不足


def is_post_only_reject(code: int) -> bool:
    return code == ERR_POST_ONLY_REJECT


def is_order_gone(code: int) -> bool:
    """订单不存在(已成交/已撤/从未存在)"""
    return code == ERR_UNKNOWN_ORDER
