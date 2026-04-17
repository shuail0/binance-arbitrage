# CLAUDE.md

## 项目概述

币安交易策略 Python 项目，使用官方 SDK (`binance-sdk-spot`)，以 WebSocket 为主、REST 为辅。
从 OKX 项目 (okx-arbitrage) 移植，保持相同的代码范式和策略命名。

## 策略映射

| 策略目录 | 说明 | OKX 对应 | Go 对应 |
|---------|------|---------|---------|
| `instant_volume` | 盘口即时刷量 | `instant_volume` | `volume_maker` |
| `spot_volume_v2` | 现货网格刷量 | `spot_volume_v2` | `market_making_volume_v2` |
| `market_maker_v2` | 现货双向做市 | `market_maker_v2` | `market_maker` |
| `swap_volume` | 合约做市/刷量 | `swap_volume` | `futures_market_maker` |

## 通信架构

- **WebSocket Streams**: 行情推送 (bookTicker/depth/ticker) + User Data Stream (订单/余额)
- **WebSocket API**: 下单/撤单/改单（低延迟，替代 REST 交易接口）
- **REST API**: 辅助操作 (exchangeInfo/listenKey/账户查询)

## 环境配置

- API 密钥通过 `.env` 文件配置（参考 `.env.example`）
- `BINANCE_DEMO=1` 切换模拟盘
- 策略参数通过各自目录的 `config.yaml` / `params.yaml` 配置

## 运行

```bash
pip install -r requirements.txt
./run.sh iv            # 即时刷量
./run.sh spot          # 现货刷量
./run.sh mm2           # 做市
./run.sh swap          # 合约
```

## 开发规范

- Python 3.12+, asyncio
- 日志: loguru
- 数值: Decimal (避免浮点精度问题)
- 配置: YAML + .env
- 统计持久化: volume_state.json (orjson)
- 中文注释
- 极简实现，一行能解决绝不写两行
