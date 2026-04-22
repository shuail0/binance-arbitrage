#!/bin/bash
# 币安交易策略快速启动脚本
# 用法:
#   ./run.sh iv            - 前台运行即时刷量
#   ./run.sh spot          - 前台运行现货刷量V2
#   ./run.sh mm2           - 前台运行做市V2
#   ./run.sh swap          - 前台运行合约刷量
#   ./run.sh spot start    - 后台运行
#   ./run.sh spot stop     - 停止
#   ./run.sh spot status   - 查看状态
#   ./run.sh spot logs     - 查看日志

set -e

GREEN='\033[0;32m' YELLOW='\033[1;33m' RED='\033[0;31m' BLUE='\033[0;34m' NC='\033[0m'

case "$1" in
    iv)
        MODE="iv"
        SCRIPT="strategies/instant_volume/main.py"
        SCREEN_NAME="bnc-iv"
        shift
        ;;
    spot)
        MODE="spot"
        SCRIPT="strategies/spot_volume_v2/main.py"
        SCREEN_NAME="bnc-spot"
        shift
        ;;
    mm2)
        MODE="mm2"
        SCRIPT="strategies/market_maker_v2/main.py"
        SCREEN_NAME="bnc-mm2"
        shift
        ;;
    swap)
        MODE="swap"
        SCRIPT="strategies/swap_volume/main.py"
        SCREEN_NAME="bnc-swap"
        shift
        ;;
    *)
        echo -e "${YELLOW}用法: $0 {iv|spot|mm2|swap} [start|stop|status|logs]${NC}"
        echo ""
        echo "策略:"
        echo "  iv    - 即时刷量 (instant_volume)"
        echo "  spot  - 现货刷量V2 (spot_volume_v2)"
        echo "  mm2   - 做市V2 (market_maker_v2)"
        echo "  swap  - 合约刷量 (swap_volume)"
        echo ""
        echo "操作:"
        echo "  (无)   - 前台运行"
        echo "  start  - 后台运行 (screen)"
        echo "  stop   - 停止"
        echo "  status - 查看状态"
        echo "  logs   - 查看日志"
        exit 1
        ;;
esac

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

ACTION="${1:-foreground}"

case "$ACTION" in
    start)
        if screen -ls | grep -q "$SCREEN_NAME"; then
            echo -e "${YELLOW}⚠️  $SCREEN_NAME 已在运行${NC}"
            exit 1
        fi
        screen -dmS "$SCREEN_NAME" python "$SCRIPT"
        echo -e "${GREEN}✅ $SCREEN_NAME 已在后台启动${NC}"
        ;;
    stop)
        if screen -ls | grep -q "$SCREEN_NAME"; then
            # 发送 SIGINT 触发策略 shutdown (撤单 + 清仓 + 保存状态)
            # swap_volume shutdown 最长可达 15s, 这里等 20s 留余量
            screen -S "$SCREEN_NAME" -X stuff $'\003'
            for i in $(seq 1 20); do
                screen -ls | grep -q "$SCREEN_NAME" || break
                sleep 1
            done
            screen -ls | grep -q "$SCREEN_NAME" && screen -S "$SCREEN_NAME" -X quit
            echo -e "${GREEN}✅ $SCREEN_NAME 已停止${NC}"
        else
            echo -e "${YELLOW}⚠️  $SCREEN_NAME 未在运行${NC}"
        fi
        ;;
    status)
        if screen -ls | grep -q "$SCREEN_NAME"; then
            echo -e "${GREEN}✅ $SCREEN_NAME 运行中${NC}"
        else
            echo -e "${RED}❌ $SCREEN_NAME 未运行${NC}"
        fi
        ;;
    logs)
        if screen -ls | grep -q "$SCREEN_NAME"; then
            screen -r "$SCREEN_NAME"
        else
            echo -e "${YELLOW}⚠️  $SCREEN_NAME 未在运行${NC}"
            ls -t logs/${MODE}_*.log 2>/dev/null | head -1 | xargs tail -50 2>/dev/null || echo "无日志文件"
        fi
        ;;
    foreground)
        echo -e "${BLUE}🚀 前台运行 $MODE ...${NC}"
        python "$SCRIPT"
        ;;
    *)
        echo -e "${RED}未知操作: $ACTION${NC}"
        exit 1
        ;;
esac
