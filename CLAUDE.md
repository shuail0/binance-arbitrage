# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

币安交易策略 Python 项目，从 OKX 项目 (okx-arbitrage) 移植，保持相同代码范式。
无第三方 SDK，直接使用 websockets + aiohttp 原生对接币安 API。

## 运行

```bash
pip install -r requirements.txt
./run.sh iv            # 即时刷量（前台）
./run.sh spot          # 现货刷量
./run.sh mm2           # 做市
./run.sh swap          # 合约
./run.sh spot start    # 后台运行 (screen)
./run.sh spot stop     # 停止
```

工具脚本：
```bash
python tools/stats_summary.py         # 统计汇总（读 volume_state.json）
python tools/liquidate.py             # 批量清仓
python tools/strategy_manager.py      # 远程策略管理（SSH 部署/启停）
```

## 通信架构

三层通信，所有策略共用同一模式：

- **WS Streams**: 行情推送 (bookTicker/depth/ticker/aggTrade) + User Data Stream (executionReport/ORDER_TRADE_UPDATE)
- **WS API** (仅现货): 下单/撤单（低延迟，替代 REST）
- **REST API**: exchangeInfo、listenKey 管理、账户查询、合约下单/撤单/改单

认证方式: Ed25519 私钥签名（PEM 文件），非 HMAC。

## 策略架构

每个策略是独立的单文件 (`strategies/<name>/main.py`)，内含完整的：策略类 + 数据模型 + Stats 统计 + VolatilityTracker 波动率 + 入口函数。无共享基类。

| 策略 | 配置文件 | 说明 | 特有机制 |
|------|---------|------|---------|
| `instant_volume` | `config.yaml` | FOK 买@ask → FOK 卖@bid | 最简单，达 maxVolume 后清仓退出 |
| `spot_volume_v2` | `config.yaml` | 买侧网格 + 止盈止损 | Position 状态机(OPENING→OPEN→CLOSING→CLOSED)，User Data Stream 驱动 |
| `market_maker_v2` | `params.yaml` | 买卖双侧网格 | Diff 算法(撤+补)、OBI 信号、现金流法 PnL、CSV 成交记录 |
| `swap_volume` | `config.yaml` | 合约双侧做市 | REST 批量下单/改单/撤单、活跃度熔断、BNB 手续费管理、方向感知熔断 |

### 共性模式

- **波动率熔断**: 滑动窗口计算振幅，与历史基准中位数比较，超过倍数触发熔断（撤单+可选清仓），回落后自动恢复
- **配置热加载**: 监控 config/params.yaml 的 mtime，变更时自动重载可热更新字段
- **状态持久化**: 各策略目录下 `volume_state.json`（全局统计）和 `*_state_*.json`（持仓/PnL），JSON 格式
- **安全退出**: SIGINT/SIGTERM → should_stop=True → 撤单 → 可选清仓 → 保存状态

### 现货 vs 合约差异

- 现货策略: WS API 下单（低延迟），User Data Stream 接收 `executionReport`
- 合约策略: REST 批量下单/改单（`/fapi/v1/batchOrders`），支持 GTX(Post Only)，User Data Stream 接收 `ORDER_TRADE_UPDATE`
- 合约额外功能: 杠杆设置、保证金模式、持仓模式(单向)、BNB 自动补充

## 环境配置

所有策略（现货 3 个 + 合约 1 个）统一从项目根目录 `.env` 读取凭证：
- `BINANCE_API_KEY`
- `BINANCE_PRIVATE_KEY_PATH`（Ed25519 PEM 文件的绝对路径）
- `BINANCE_DEMO=1` 切换模拟盘

各策略的 `config.yaml` / `params.yaml` 只保留业务参数，不放凭证。

### Demo / Testnet URL 映射

| 产品 | REST | WS Stream | 账号体系 |
|------|------|-----------|---------|
| 现货 demo | `demo-api.binance.com` | `demo-stream.binance.com:9443` | demo.binance.com 独立账号 |
| 合约 demo | `demo-fapi.binance.com` | `fstream.binancefuture.com` | 同上（历史遗留域名混用，官方确认正确）|
| 清仓工具 liquidate.py | `demo-api.binance.com` | — | 仅支持现货，保持与现货策略一致 |

注意币安还有一个独立的公开 Spot Test Network (`testnet.binance.vision`)，需独立注册，项目代码**不使用**它。

## 远程部署（strategy_manager.py）

Excel（`keys/币安API.xlsx`）驱动的多账号部署：
- 每台服务器一个账号，从 Excel 读 `ip / api_key / 密钥路径`
- sync / deploy 时在远程生成 `.env`，同时 scp 上传 `.pem` 私钥文件
- SSH 密码不再硬编码，可通过 `SSH_PASSWORD` 环境变量或 `getpass()` 交互输入

## 开发规范

- Python 3.12+, asyncio, 全异步
- 日志: loguru（stdout + 按日轮转文件）
- 数值: Decimal（价格/数量/统计），避免浮点
- 价格/数量对齐: `_align_price()`(tickSize) / `_align_qty()`(stepSize)，下单前必须对齐
- 统计持久化: volume_state.json (json, 按 symbol 分 key)
- 中文注释、极简实现
- HTTP session 复用: 每个策略持有一个 `self._http_session: aiohttp.ClientSession`，懒加载 + shutdown 时关闭
- 事件循环: `asyncio.get_running_loop()` 而非已弃用的 `get_event_loop()`

## 已知字段命名碎片化

4 个策略因历史原因（来自不同项目移植）使用不一致的 YAML 字段命名：

| 策略 | 层级 | 命名 |
|------|------|------|
| `instant_volume` | 根级 | 混用（`strategy_id` snake, `maxVolume` camel）|
| `spot_volume_v2` | 根级 | 全 snake_case |
| `market_maker_v2` | `strategy.*` | 全 snake_case |
| `swap_volume` | `strategy.*` | 全 camelCase |

`strategy_manager.py` 的 `CONFIG_FIELD_PATHS` 抽象了这些差异。**新加策略请统一用 `strategy.xxx_yyy`**（嵌套 + snake_case，与 `market_maker_v2` 对齐）。
