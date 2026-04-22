"""币安官方 SDK 客户端工厂

统一从 .env 读凭证, 构造 Spot / Futures SDK 客户端.
所有策略共享此工厂, 避免重复配置.
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass
class Credentials:
    """API 凭证 (从 .env 统一读取)"""
    api_key: str
    private_key_path: str
    demo: bool = False

    def validate(self) -> None:
        if not self.api_key:
            raise ValueError("BINANCE_API_KEY 未配置, 请检查 .env")
        if not self.private_key_path:
            raise ValueError("BINANCE_PRIVATE_KEY_PATH 未配置, 请检查 .env")
        if not Path(self.private_key_path).exists():
            raise FileNotFoundError(f"私钥文件不存在: {self.private_key_path}")


def load_credentials() -> Credentials:
    """从 .env 加载凭证 (所有策略入口统一调用)"""
    load_dotenv()
    creds = Credentials(
        api_key=os.getenv("BINANCE_API_KEY", ""),
        private_key_path=os.getenv("BINANCE_PRIVATE_KEY_PATH", ""),
        demo=os.getenv("BINANCE_DEMO", "0") == "1",
    )
    creds.validate()
    return creds


# ─── URL 常量 ──────────────────────────────────────────────────────────────
# 现货
SPOT_REST_PROD = "https://api.binance.com"
SPOT_REST_DEMO = "https://demo-api.binance.com"
SPOT_WS_API_PROD = "wss://ws-api.binance.com:443/ws-api/v3"
SPOT_WS_API_DEMO = "wss://demo-ws-api.binance.com:443/ws-api/v3"
SPOT_WS_STREAMS_PROD = "wss://stream.binance.com:9443"
SPOT_WS_STREAMS_DEMO = "wss://demo-stream.binance.com:9443"

# 合约
FUTURES_REST_PROD = "https://fapi.binance.com"
FUTURES_REST_DEMO = "https://demo-fapi.binance.com"
FUTURES_WS_API_PROD = "wss://ws-fapi.binance.com/ws-fapi/v1"
FUTURES_WS_API_DEMO = "wss://testnet.binancefuture.com/ws-fapi/v1"  # 需实测
FUTURES_WS_STREAMS_PROD = "wss://fstream.binance.com"
FUTURES_WS_STREAMS_DEMO = "wss://fstream.binancefuture.com"


def make_spot_client(creds: Credentials, *,
                     with_ws_api: bool = True,
                     with_ws_streams: bool = True):
    """构造现货 SDK 客户端 (REST + 可选 WS API + 可选 WS Streams)

    Args:
        creds: API 凭证 (调用前已 validate)
        with_ws_api: 是否需要 WS API (下撤单)
        with_ws_streams: 是否需要 WS Streams (行情订阅)

    Returns:
        Spot 客户端实例. 通过 .rest_api / .websocket_api / .websocket_streams 访问.
    """
    from binance_sdk_spot import Spot
    from binance_common.configuration import (
        ConfigurationRestAPI, ConfigurationWebSocketAPI, ConfigurationWebSocketStreams,
    )

    rest_base = SPOT_REST_DEMO if creds.demo else SPOT_REST_PROD
    ws_api_url = SPOT_WS_API_DEMO if creds.demo else SPOT_WS_API_PROD
    ws_streams_url = SPOT_WS_STREAMS_DEMO if creds.demo else SPOT_WS_STREAMS_PROD

    rest_cfg = ConfigurationRestAPI(
        api_key=creds.api_key,
        private_key=creds.private_key_path,
        base_path=rest_base,
    )
    ws_api_cfg = ConfigurationWebSocketAPI(
        api_key=creds.api_key,
        private_key=creds.private_key_path,
        stream_url=ws_api_url,
    ) if with_ws_api else None
    ws_streams_cfg = ConfigurationWebSocketStreams(
        stream_url=ws_streams_url,
    ) if with_ws_streams else None

    return Spot(
        config_rest_api=rest_cfg,
        config_ws_api=ws_api_cfg,
        config_ws_streams=ws_streams_cfg,
    )


def make_futures_client(creds: Credentials, *,
                        with_ws_streams: bool = True):
    """构造 U 本位合约 SDK 客户端

    合约当前只用 REST (批量下单) + WS Streams (行情 + User Data).
    合约 WS API 批量不可用, 一般不需要 (单笔 order.place/cancel 如有需要再加).

    Args:
        creds: API 凭证
        with_ws_streams: 是否需要 WS Streams (含 User Data listenKey)
    """
    from binance_sdk_derivatives_trading_usds_futures import (
        DerivativesTradingUsdsFutures,
    )
    from binance_common.configuration import (
        ConfigurationRestAPI, ConfigurationWebSocketStreams,
    )

    rest_base = FUTURES_REST_DEMO if creds.demo else FUTURES_REST_PROD
    ws_streams_url = FUTURES_WS_STREAMS_DEMO if creds.demo else FUTURES_WS_STREAMS_PROD

    rest_cfg = ConfigurationRestAPI(
        api_key=creds.api_key,
        private_key=creds.private_key_path,
        base_path=rest_base,
    )
    ws_streams_cfg = ConfigurationWebSocketStreams(
        stream_url=ws_streams_url,
    ) if with_ws_streams else None

    return DerivativesTradingUsdsFutures(
        config_rest_api=rest_cfg,
        config_ws_streams=ws_streams_cfg,
    )
