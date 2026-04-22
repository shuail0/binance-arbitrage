#!/usr/bin/env python3
"""策略管理工具 - 统一管理远程策略（启动、停止、查看、配置、同步、部署）"""

import subprocess
import sys
import yaml
import tempfile
import os
import time
import random
import shlex
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from openpyxl import load_workbook
from getpass import getpass

MAX_RETRIES = 3
RETRY_DELAY = 2
print_lock = Lock()

# ==================== 配置 ====================
RANDOM_RANGE = {
    "quantity": None,       # (min, max) 或 None 不随机
    "max_volume": None,
}

EXCEL_PATH = str(Path(__file__).resolve().parents[1] / "keys" / "币安API.xlsx")
PROJECT_DIR = Path(__file__).resolve().parent.parent
REMOTE_DIR = "/home/ubuntu/binance"
SSH_USER = "ubuntu"
# SSH 密码不硬编码。可通过环境变量 SSH_PASSWORD 注入 (如 export 或 .env), 否则交互式 getpass
DEFAULT_SSH_PASSWORD = os.getenv("SSH_PASSWORD", "")
CONDA_ENV = "binance"

STRATEGY_MAP = {
    "1": ("iv",   "instant_volume",   "即时刷量"),
    "2": ("spot", "spot_volume_v2",   "现货刷量V2"),
    "3": ("mm2",  "market_maker_v2",  "做市策略V2"),
    "4": ("swap", "swap_volume",      "合约刷量"),
}

CONFIG_MAP = {
    "instant_volume":   "strategies/instant_volume/config.yaml",
    "spot_volume_v2":   "strategies/spot_volume_v2/config.yaml",
    "market_maker_v2":  "strategies/market_maker_v2/params.yaml",
    "swap_volume":      "strategies/swap_volume/config.yaml",
}

STRATEGY_CONFIGS = {k: (name, CONFIG_MAP[name], desc) for k, (_, name, desc) in STRATEGY_MAP.items()}

# 支持热加载的策略
HOT_RELOAD_STRATEGIES = {"market_maker_v2", "swap_volume"}

# 各策略字段到 YAML 路径的映射
CONFIG_FIELD_PATHS = {
    "instant_volume": {
        "strategy_id": ("strategy_id",),
        "symbol": ("symbol",),
        "quantity": ("quantity",),
        "max_volume": ("maxVolume",),
        "order_interval": ("orderInterval",),
        "max_spread": ("maxSpread",),
    },
    "spot_volume_v2": {
        "strategy_id": ("strategy_id",),
        "symbol": ("symbol",),
        "quantity": ("quantity",),
        "max_volume": ("max_volume",),
        "order_interval": ("order_interval",),
        "step": ("step",),
        "num_levels": ("num_levels",),
        "max_net_buy": ("max_net_buy",),
    },
    "market_maker_v2": {
        "symbol": ("instrument_id",),
        "single_size": ("strategy", "single_size"),
        "step": ("strategy", "step"),
        "num_each_side": ("strategy", "num_of_order_each_side"),
        "max_net_buy": ("strategy", "maximum_net_buy"),
        "max_net_sell": ("strategy", "maximum_net_sell"),
        "max_volume": ("strategy", "max_volume"),
    },
    "swap_volume": {
        "strategy_id": ("strategyId",),
        "symbol": ("symbol",),
        "quantity": ("strategy", "quantity"),
        "max_volume": ("strategy", "maxVolume"),
        "order_interval": ("strategy", "orderInterval"),
        "step_size": ("strategy", "stepSize"),
        "num_each_side": ("strategy", "numOrdersEachSide"),
        "leverage": ("leverage",),
        "margin_type": ("marginType",),
    },
}


# ==================== 工具函数 ====================
def get_nested(data, path, default=None):
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def set_nested(data, path, value):
    current = data
    for key in path[:-1]:
        current = current.setdefault(key, {})
    current[path[-1]] = value


def get_field_path(strategy_name, field_name):
    return CONFIG_FIELD_PATHS.get(strategy_name, {}).get(field_name)


def get_config_value(config, strategy_name, field_name, default=None):
    path = get_field_path(strategy_name, field_name)
    return get_nested(config, path, default) if path else default


def load_yaml_config(path):
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def normalize_server_id(value):
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


# ==================== 服务器管理 ====================
def read_servers(excel_path):
    """读取服务器列表（自动匹配列名）"""
    def normalize(text):
        return str(text).strip().lower() if text is not None else ""

    def pick_index(header_map, *candidates):
        for c in candidates:
            key = normalize(c)
            if key in header_map:
                return header_map[key]
        for c in candidates:
            key = normalize(c)
            for hk, idx in header_map.items():
                if key in hk:
                    return idx
        return None

    wb = load_workbook(excel_path, data_only=True)
    for ws in wb.worksheets:
        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not header_row:
            continue

        header_map = {normalize(t): i for i, t in enumerate(header_row) if t}
        ip_idx = pick_index(header_map, "IP", "服务器IP", "ip地址")
        if ip_idx is None:
            continue

        id_idx = pick_index(header_map, "序号", "编号", "id")
        name_idx = pick_index(header_map, "名称", "备注", "服务器名称")
        api_idx = pick_index(header_map, "API key", "api key", "apikey")
        key_path_idx = pick_index(header_map, "密钥路径", "密钥", "key path", "private key")
        enabled_idx = pick_index(header_map, "是否可用", "可用", "启用", "是否启用")

        servers = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            ip = str(row[ip_idx] or "").strip() if ip_idx is not None and ip_idx < len(row) else ""
            if not ip:
                continue

            if enabled_idx is not None and enabled_idx < len(row):
                val = normalize(row[enabled_idx])
                if val in ("否", "不可用", "停用", "no", "n", "false", "0"):
                    continue

            def cell(idx):
                return str(row[idx] or "").strip() if idx is not None and idx < len(row) else ""

            servers.append({
                "id": normalize_server_id(row[id_idx] if id_idx is not None and id_idx < len(row) else len(servers) + 1),
                "name": cell(name_idx),
                "api_key": cell(api_idx),
                "key_path": cell(key_path_idx),
                "ip": ip,
            })

        if servers:
            return servers

    return []


def run_ssh(ip, password, command, timeout=30, retry=False):
    """执行 SSH 命令"""
    max_attempts = MAX_RETRIES if retry else 1
    for attempt in range(1, max_attempts + 1):
        try:
            cmd = [
                "sshpass", "-p", password, "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                "-o", "ServerAliveInterval=5",
                f"{SSH_USER}@{ip}", "bash", "-s",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=command)
            if result.returncode == 0 or attempt == max_attempts:
                return result.returncode == 0, result.stdout, result.stderr
            with print_lock:
                print(f"   ⚠️  第 {attempt} 次尝试失败，{RETRY_DELAY}秒后重试...")
            time.sleep(RETRY_DELAY)
        except subprocess.TimeoutExpired:
            if attempt == max_attempts:
                return False, "", f"SSH命令执行超时（>{timeout}秒）"
            with print_lock:
                print(f"   ⚠️  第 {attempt} 次超时，{RETRY_DELAY}秒后重试...")
            time.sleep(RETRY_DELAY)
    return False, "", "重试耗尽"


def upload_files(ip, password, local_path, remote_path, retry=True):
    """上传文件"""
    max_attempts = MAX_RETRIES if retry else 1
    for attempt in range(1, max_attempts + 1):
        cmd = [
            "sshpass", "-p", password, "scp",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-r", str(local_path),
            f"{SSH_USER}@{ip}:{remote_path}",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            return True, result.stderr
        if attempt < max_attempts:
            with print_lock:
                print(f"   ⚠️  第 {attempt} 次尝试失败，{RETRY_DELAY}秒后重试...")
            time.sleep(RETRY_DELAY)
    return False, result.stderr


def select_servers(servers):
    print("\n可用服务器列表：")
    for s in servers:
        print(f"  {s['id']}. {s['ip']} ({s['name']})")

    server_input = input("\n请输入服务器编号（多个用逗号分隔，或 'all' 选择所有）: ").strip()
    if server_input.lower() == 'all':
        return servers
    selected_ids = {x.strip() for x in server_input.split(",") if x.strip()}
    return [s for s in servers if s["id"] in selected_ids]


def select_strategy():
    print("\n可用策略：")
    for k, (_, name, desc) in sorted(STRATEGY_MAP.items()):
        print(f"  {k}. {desc} ({name})")
    choice = input(f"请选择策略 ({'/'.join(sorted(STRATEGY_MAP.keys()))}): ").strip()
    return STRATEGY_MAP.get(choice)


def get_password():
    if DEFAULT_SSH_PASSWORD:
        print("\n✅ 使用预设 SSH 密码")
        return DEFAULT_SSH_PASSWORD
    return getpass("\n请输入 SSH 密码: ")


def randomize_value(key, fallback):
    r = RANDOM_RANGE.get(key)
    return random.randint(r[0], r[1]) if r else fallback


# ==================== 同步 & 部署 ====================
def sync_server(server, password, strategies_to_sync, rand_qty=False, rand_vol=False):
    """同步单台服务器的配置（.env + 策略配置）"""
    ip, server_id, server_name = server["ip"], server["id"], server["name"]
    api_key = server.get("api_key", "")
    key_path = server.get("key_path", "")

    print(f"\n{'='*60}")
    print(f"同步服务器 {server_id}: {ip} ({server_name})")
    print(f"{'='*60}")

    total_steps = 1 + len(strategies_to_sync)

    # 1. 同步 .env
    print(f"[1/{total_steps}] 同步 .env（API 密钥）...")
    key_filename = os.path.basename(key_path) if key_path else ""
    remote_key_path = f"{REMOTE_DIR}/{key_filename}" if key_filename else ""

    env_content = (
        f"BINANCE_API_KEY={api_key}\n"
        f"BINANCE_PRIVATE_KEY_PATH={remote_key_path}\n"
        f"BINANCE_DEMO=0\n"
    )
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write(env_content)
        tmp_env = Path(f.name)
    success, error = upload_files(ip, password, tmp_env, f"{REMOTE_DIR}/.env", retry=True)
    tmp_env.unlink(missing_ok=True)
    if not success:
        print(f"❌ 同步 .env 失败: {error}")
        return False
    print(f"✅ .env 已同步")

    # 上传密钥文件
    if key_path and os.path.exists(key_path):
        success, error = upload_files(ip, password, key_path, f"{REMOTE_DIR}/")
        if not success:
            print(f"⚠️  上传密钥文件失败: {error}")

    # 2. 同步策略配置
    for step, (strategy_name, config_path, desc) in enumerate(strategies_to_sync, 2):
        print(f"[{step}/{total_steps}] 同步 {desc} 配置...")
        local_path = PROJECT_DIR / config_path
        if not local_path.exists():
            print(f"⚠️  配置文件不存在: {local_path}")
            continue

        # 确保远程目录存在
        remote_dir = f"{REMOTE_DIR}/{os.path.dirname(config_path)}"
        run_ssh(ip, password, f"mkdir -p {shlex.quote(remote_dir)}", retry=False)

        if rand_qty or rand_vol:
            config = load_yaml_config(local_path)

            if strategy_name == "market_maker_v2":
                s = config.get("strategy", {})
                if rand_qty and "single_size" in s:
                    orig = s["single_size"]
                    s["single_size"] = randomize_value("quantity", orig)
                    print(f"   📊 随机 single_size: {orig} → {s['single_size']}")
                if rand_vol and s.get("max_volume"):
                    orig = s["max_volume"]
                    s["max_volume"] = randomize_value("max_volume", orig)
                    print(f"   📈 随机 max_volume: {orig} → {s['max_volume']}")
            elif strategy_name == "swap_volume":
                s = config.get("strategy", {})
                if rand_qty and "quantity" in s:
                    orig = s["quantity"]
                    s["quantity"] = str(randomize_value("quantity", orig))
                    print(f"   📊 随机 quantity: {orig} → {s['quantity']}")
                if rand_vol and s.get("maxVolume"):
                    orig = s["maxVolume"]
                    s["maxVolume"] = randomize_value("max_volume", orig)
                    print(f"   📈 随机 maxVolume: {orig} → {s['maxVolume']}")
            else:
                if rand_qty and "quantity" in config:
                    orig = config["quantity"]
                    config["quantity"] = randomize_value("quantity", orig)
                    print(f"   📊 随机 quantity: {orig} → {config['quantity']}")
                if rand_vol and config.get("max_volume", config.get("maxVolume")):
                    vol_key = "maxVolume" if "maxVolume" in config else "max_volume"
                    orig = config[vol_key]
                    config[vol_key] = randomize_value("max_volume", orig)
                    print(f"   📈 随机 {vol_key}: {orig} → {config[vol_key]}")

            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                tmp_config = Path(f.name)
            success, error = upload_files(ip, password, tmp_config, f"{REMOTE_DIR}/{config_path}", retry=True)
            tmp_config.unlink(missing_ok=True)
        else:
            success, error = upload_files(ip, password, local_path, f"{REMOTE_DIR}/{config_path}", retry=True)

        if not success:
            print(f"❌ 同步 {desc} 失败: {error}")
            return False
        print(f"✅ {desc} 配置已同步")

    print(f"✅ 服务器 {server_id} 配置同步完成")
    return True


def deploy_server(server, password, full_setup=False):
    """部署单台服务器"""
    ip, server_id, server_name = server["ip"], server["id"], server["name"]
    total_steps = 5 if full_setup else 4

    with print_lock:
        print(f"\n{'='*60}")
        print(f"[{server_id}] 开始部署: {ip} ({server_name})")
        print(f"{'='*60}")

    # 1. 创建远程目录
    with print_lock:
        print(f"[{server_id}] [1/{total_steps}] 创建远程目录...")
    success, _, error = run_ssh(ip, password, f"mkdir -p {REMOTE_DIR}/logs {REMOTE_DIR}/strategies {REMOTE_DIR}/tools", retry=True)
    if not success:
        with print_lock:
            print(f"[{server_id}] ❌ 创建目录失败: {error}")
        return False, server, f"创建目录失败: {error}"

    # 2. 上传代码 (rsync 排除配置和日志)
    with print_lock:
        print(f"[{server_id}] [2/{total_steps}] 上传代码...")
    exclude_args = [
        "--exclude=.env", "--exclude=__pycache__", "--exclude=logs/",
        "--exclude=.git", "--exclude=keys/",
    ]
    for name in ("strategies", "tools"):
        local = PROJECT_DIR / name
        if not local.exists():
            continue
        # 列表参数 + shell=False, 避免密码/IP 中特殊字符被 shell 注入
        cmd = [
            "sshpass", "-p", password,
            "rsync", "-az", *exclude_args,
            "-e", "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10",
            f"{local}/", f"{SSH_USER}@{ip}:{REMOTE_DIR}/{name}/",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            with print_lock:
                print(f"[{server_id}] ❌ 上传 {name} 失败: {result.stderr}")
            return False, server, f"上传 {name} 失败"
        with print_lock:
            print(f"[{server_id}]    ✅ {name} 已更新")

    # 上传 run.sh + requirements.txt
    for fname in ("run.sh", "requirements.txt"):
        local = PROJECT_DIR / fname
        if local.exists():
            upload_files(ip, password, local, f"{REMOTE_DIR}/{fname}")

    # 3. 生成并上传 .env
    with print_lock:
        print(f"[{server_id}] [3/{total_steps}] 生成并上传 .env...")
    api_key = server.get("api_key", "")
    key_path = server.get("key_path", "")
    key_filename = os.path.basename(key_path) if key_path else ""
    remote_key_path = f"{REMOTE_DIR}/{key_filename}" if key_filename else ""

    env_content = f"BINANCE_API_KEY={api_key}\nBINANCE_PRIVATE_KEY_PATH={remote_key_path}\nBINANCE_DEMO=0\n"
    tmp_env = Path("/tmp") / f".env_bnc_{ip}"
    tmp_env.write_text(env_content)
    success, error = upload_files(ip, password, tmp_env, f"{REMOTE_DIR}/.env", retry=True)
    tmp_env.unlink(missing_ok=True)
    if not success:
        with print_lock:
            print(f"[{server_id}] ❌ 上传 .env 失败: {error}")
        return False, server, f"上传 .env 失败: {error}"

    # 上传密钥
    if key_path and os.path.exists(key_path):
        upload_files(ip, password, key_path, f"{REMOTE_DIR}/")

    # 4. 设置执行权限
    with print_lock:
        print(f"[{server_id}] [4/{total_steps}] 设置执行权限...")
    run_ssh(ip, password, f"chmod +x {REMOTE_DIR}/run.sh")

    # 5. 可选：安装 conda 环境
    if full_setup:
        with print_lock:
            print(f"[{server_id}] [5/{total_steps}] 安装 conda 环境...")
        run_ssh(ip, password, (
            'test -d $HOME/miniconda3 || '
            '(curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/mc.sh '
            '&& bash /tmp/mc.sh -b -p $HOME/miniconda3 && rm /tmp/mc.sh '
            '&& $HOME/miniconda3/bin/conda init bash)'
        ), timeout=300, retry=True)
        run_ssh(ip, password, (
            f'source $HOME/miniconda3/etc/profile.d/conda.sh && '
            f'(conda env list | grep -q "^{CONDA_ENV} " || conda create -n {CONDA_ENV} python=3.12 -y) && '
            f'conda activate {CONDA_ENV} && '
            f'pip install -r {REMOTE_DIR}/requirements.txt'
        ), timeout=300, retry=True)

    with print_lock:
        print(f"[{server_id}] ✅ 部署成功")
    return True, server, None


# ==================== 功能模块 ====================
def start_strategy(server, mode, name, desc, password):
    """启动策略"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"在服务器 {server_id} ({ip}) 启动 {desc}")
    print(f"{'='*60}")

    # 检查是否已运行
    check_cmd = f'screen -list 2>/dev/null | grep "bnc-{mode}" || true'
    _, stdout, _ = run_ssh(ip, password, check_cmd)
    if f"bnc-{mode}" in stdout:
        pid_cmd = f'pgrep -f "python.*strategies/{name}/main.py" || true'
        _, pid_out, _ = run_ssh(ip, password, pid_cmd, timeout=5)
        if pid_out.strip():
            print(f"⚠️  策略已在运行中（screen: bnc-{mode}）")
            return True
        run_ssh(ip, password, f'screen -S "bnc-{mode}" -X quit 2>/dev/null', timeout=5)

    # 启动
    launch_cmd = f"""
command -v screen >/dev/null || sudo apt install -y screen
screen -dmS "bnc-{mode}" bash -c '
    source $HOME/miniconda3/etc/profile.d/conda.sh 2>/dev/null
    conda activate {CONDA_ENV} 2>/dev/null
    cd {REMOTE_DIR}
    python strategies/{name}/main.py
'
"""
    run_ssh(ip, password, launch_cmd, timeout=10)

    time.sleep(2)
    verify_cmd = f'screen -list 2>/dev/null | grep "bnc-{mode}" && echo "OK" || echo "FAILED"'
    _, stdout, _ = run_ssh(ip, password, verify_cmd, timeout=5)

    if "OK" in stdout:
        print(f"✅ 策略已启动（screen: bnc-{mode}）")
    else:
        print(f"❌ 启动失败")
    return "OK" in stdout


def stop_strategy(server, mode, name, desc, password):
    """停止策略（SIGINT 优雅退出 → 超时强制 kill）"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"在服务器 {server_id} ({ip}) 停止 {desc}")
    print(f"{'='*60}")

    stop_cmd = f"""
PID=""
for p in $(pgrep -f "strategies/{name}/main.py" 2>/dev/null); do
    case "$(cat /proc/$p/comm 2>/dev/null)" in
        python*) PID=$p; break;;
    esac
done
if [ -n "$PID" ]; then
    echo "发送 SIGINT 到进程 $PID..."
    kill -INT $PID
    for i in $(seq 1 30); do
        kill -0 $PID 2>/dev/null || break
        sleep 1
    done
    if kill -0 $PID 2>/dev/null; then
        echo "优雅退出超时，强制终止..."
        kill -9 $PID 2>/dev/null
    fi
    echo "STOPPED"
else
    echo "NOT_RUNNING"
fi
screen -S "bnc-{mode}" -X quit 2>/dev/null
"""
    _, stdout, stderr = run_ssh(ip, password, stop_cmd, timeout=60)

    if "NOT_RUNNING" in stdout:
        print(f"⚠️  策略未运行")
    elif "STOPPED" in stdout:
        print(f"✅ 策略已停止")
    else:
        print(f"❌ 停止失败: {stderr.strip()}")


def view_status(server, mode, name, desc, password):
    """查看运行状态"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"服务器 {server_id} ({ip}) - {desc}")
    print(f"{'='*60}")

    # 1. 检查进程
    cmd = f'ps aux | grep "python.*strategies/{name}/main.py" | grep -v grep'
    _, stdout, _ = run_ssh(ip, password, cmd)
    if stdout.strip():
        print("✅ 策略正在运行：")
        print(stdout.rstrip())
    else:
        print("❌ 策略未运行")

    # 2. screen 会话
    cmd = f'screen -list 2>/dev/null | grep "bnc-{mode}" || echo "无 screen 会话"'
    _, stdout, _ = run_ssh(ip, password, cmd)
    print(f"\nScreen: {stdout.strip()}")

    # 3. 读取策略配置
    config_path = f"{REMOTE_DIR}/{CONFIG_MAP[name]}"
    _, config_stdout, _ = run_ssh(ip, password, f"cat {shlex.quote(config_path)} 2>/dev/null")

    config = {}
    max_volume = 0
    if config_stdout.strip():
        try:
            config = yaml.safe_load(config_stdout) or {}
            max_volume = get_config_value(config, name, "max_volume", 0) or 0
        except Exception:
            pass

    # 4. 读取 volume_state.json
    state_cmd = f"cat {REMOTE_DIR}/volume_state.json 2>/dev/null"
    success, state_stdout, _ = run_ssh(ip, password, state_cmd)

    if success and state_stdout.strip():
        try:
            import json
            all_state = json.loads(state_stdout)
            symbol = get_config_value(config, name, "symbol", "")
            data = all_state.get(symbol, {})
            if not data and all_state:
                data = next(iter(all_state.values())) if isinstance(next(iter(all_state.values()), None), dict) else {}

            if data:
                buy = float(data.get('global_buy_volume', 0))
                sell = float(data.get('global_sell_volume', 0))
                volume = buy + sell
                pnl = float(data.get('global_pnl', 0))
                updated_at = data.get('updated_at', 'N/A')

                print(f"\n{'='*60}")
                print(f"📊 全局统计数据" + (f"（{symbol}）" if symbol else ""))
                print(f"{'='*60}")
                print(f"💰 累计总成交额: {volume:,.2f} USDT")
                print(f"📈 买入成交额:   {buy:,.2f} USDT")
                print(f"📉 卖出成交额:   {sell:,.2f} USDT")

                if pnl > 0:
                    print(f"💎 累计盈亏:     +{pnl:,.6f} USDT (盈利)")
                elif pnl < 0:
                    print(f"📊 累计盈亏:     {pnl:,.6f} USDT (亏损)")
                else:
                    print(f"⚖️  累计盈亏:     0 USDT")

                print(f"🕐 最后更新:     {updated_at}")

                if max_volume and max_volume > 0:
                    progress = (volume / float(max_volume)) * 100
                    print(f"\n📍 进度: {volume:,.2f} / {float(max_volume):,.2f} USDT ({progress:.1f}%)")
                    bar_length = 40
                    filled = int(bar_length * min(progress, 100) / 100)
                    print(f"[{'█' * filled}{'░' * (bar_length - filled)}] {progress:.1f}%")
            else:
                print(f"\n📊 未找到 {symbol or '该策略'} 的统计数据")

        except Exception as e:
            print(f"\n⚠️  解析统计数据失败: {e}")
    else:
        print(f"\n📊 volume_state.json 不存在（首次运行或未产生交易）")


def view_logs(server, mode, name, desc, password):
    """查看日志（最后50行）"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"服务器 {server_id} ({ip}) - {desc} 日志（最后50行）")
    print(f"{'='*60}")

    cmd = f'ls -t {REMOTE_DIR}/logs/*{mode}*.log 2>/dev/null | head -1'
    _, log_file, _ = run_ssh(ip, password, cmd)
    log_file = log_file.strip()

    if not log_file:
        cmd = f'ls -t {REMOTE_DIR}/logs/*.log 2>/dev/null | head -1'
        _, log_file, _ = run_ssh(ip, password, cmd)
        log_file = log_file.strip()

    if not log_file:
        print("⚠️  日志文件不存在")
        return

    _, stdout, _ = run_ssh(ip, password, f"tail -50 {shlex.quote(log_file)}")
    if stdout.strip():
        print(f"📄 日志文件: {log_file}")
        print(stdout)
    else:
        print("⚠️  日志为空")


def view_config(server, mode, name, desc, password, display_mode='formatted'):
    """查看配置"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"服务器 {server_id} ({ip}) - {desc} 配置")
    print(f"{'='*60}")

    config_path = f"{REMOTE_DIR}/{CONFIG_MAP[name]}"
    _, stdout, _ = run_ssh(ip, password, f"cat {shlex.quote(config_path)} 2>/dev/null || echo 'FILE_NOT_FOUND'")

    if "FILE_NOT_FOUND" in stdout:
        print(f"❌ 配置文件不存在: {config_path}")
        return

    if display_mode == 'raw':
        print(f"\n原始 YAML：")
        print(stdout)
    else:
        try:
            config = yaml.safe_load(stdout) or {}
            print(f"\n当前配置：")
            print(f"{'='*60}")

            if name == "market_maker_v2":
                print(f"\n📋 基本信息：")
                print(f"  instrument_id: {config.get('instrument_id', 'N/A')}")
                s = config.get('strategy', {})
                print(f"\n💰 交易参数：")
                print(f"  single_size:           {s.get('single_size', 'N/A')}")
                print(f"  step:                  {s.get('step', 'N/A')}")
                print(f"  num_of_order_each_side:{s.get('num_of_order_each_side', 'N/A')}")
                print(f"  maximum_net_buy:       {s.get('maximum_net_buy', 'N/A')}")
                print(f"  maximum_net_sell:      {s.get('maximum_net_sell', 'N/A')}")
                print(f"  max_volume:            {s.get('max_volume', 'N/A')}")
                print(f"  auto_close_on_exit:    {s.get('auto_close_on_exit', 'N/A')}")
            elif name == "swap_volume":
                print(f"\n📋 基本信息：")
                print(f"  strategyId:  {config.get('strategyId', 'N/A')}")
                print(f"  symbol:      {config.get('symbol', 'N/A')}")
                print(f"  leverage:    {config.get('leverage', 'N/A')}x")
                print(f"  marginType:  {config.get('marginType', 'N/A')}")
                s = config.get('strategy', {})
                print(f"\n💰 交易参数：")
                print(f"  quantity:       {s.get('quantity', 'N/A')}")
                print(f"  stepSize:       {s.get('stepSize', 'N/A')}")
                print(f"  numEachSide:    {s.get('numOrdersEachSide', 'N/A')}")
                print(f"  maxVolume:      {s.get('maxVolume', 'N/A')}")
                print(f"  orderInterval:  {s.get('orderInterval', 'N/A')}s")
                print(f"  postOnly:       {s.get('postOnly', False)}")
            else:
                print(f"\n📋 基本信息：")
                print(f"  strategy_id: {config.get('strategy_id', 'N/A')}")
                print(f"  symbol:      {config.get('symbol', 'N/A')}")
                print(f"\n💰 交易参数：")
                print(f"  quantity:       {config.get('quantity', 'N/A')}")
                print(f"  orderInterval:  {config.get('orderInterval', config.get('order_interval', 'N/A'))}s")
                vol = config.get('maxVolume', config.get('max_volume', 'N/A'))
                print(f"  maxVolume:      {vol}")

            print(f"\n{'='*60}")
        except Exception as e:
            print(f"❌ YAML 解析失败: {e}")
            print(stdout)

    # .env（脱敏显示）
    print(f"\n📋 .env 配置：")
    _, env_stdout, _ = run_ssh(ip, password, f"cat {REMOTE_DIR}/.env 2>/dev/null || echo 'FILE_NOT_FOUND'")
    if "FILE_NOT_FOUND" in env_stdout:
        print("  .env 文件不存在")
    else:
        for line in env_stdout.splitlines():
            if "=" in line and not line.startswith("#"):
                key, val = line.split("=", 1)
                key = key.strip()
                if key == "BINANCE_API_KEY" and len(val) > 10:
                    print(f"  {key}={val[:6]}...{val[-4:]}")
                elif key == "BINANCE_PRIVATE_KEY_PATH":
                    print(f"  {key}={os.path.basename(val)}")
                else:
                    print(f"  {line}")


def update_config(server, mode, name, desc, password):
    """修改配置"""
    ip, server_id = server['ip'], server['id']

    print(f"\n{'='*60}")
    print(f"修改服务器 {server_id} ({ip}) - {desc} 配置")
    print(f"{'='*60}")

    config_path = f"{REMOTE_DIR}/{CONFIG_MAP[name]}"
    success, stdout, stderr = run_ssh(ip, password, f"cat {shlex.quote(config_path)}")
    if not success:
        print(f"❌ 读取配置失败: {stderr}")
        return

    try:
        config = yaml.safe_load(stdout) or {}
    except Exception as e:
        print(f"❌ YAML 解析失败: {e}")
        return

    print("\n当前配置：")
    print(yaml.dump(config, allow_unicode=True, default_flow_style=False))

    print("修改模式：")
    print("  1. 快速修改（quantity / maxVolume）")
    print("  2. 高级修改（任意参数）")
    mode_choice = input("请选择模式 (1/2): ").strip()
    updates = {}

    if mode_choice == "1":
        print("\n请输入要修改的配置（留空跳过）：")
        if name == "market_maker_v2":
            s = config.get("strategy", {})
            v = input(f"  single_size [当前: {s.get('single_size')}]: ").strip()
            if v: updates["strategy.single_size"] = v
            v = input(f"  max_volume [当前: {s.get('max_volume')}]: ").strip()
            if v: updates["strategy.max_volume"] = v
        elif name == "swap_volume":
            s = config.get("strategy", {})
            v = input(f"  quantity [当前: {s.get('quantity')}]: ").strip()
            if v: updates["strategy.quantity"] = v
            v = input(f"  maxVolume [当前: {s.get('maxVolume')}]: ").strip()
            if v: updates["strategy.maxVolume"] = v
        else:
            v = input(f"  quantity [当前: {config.get('quantity')}]: ").strip()
            if v: updates["quantity"] = v
            v = input(f"  maxVolume [当前: {config.get('maxVolume', config.get('max_volume'))}]: ").strip()
            if v: updates["maxVolume" if "maxVolume" in config else "max_volume"] = v

    elif mode_choice == "2":
        print("\n请输入要修改的参数（格式: key=value，每行一个，空行结束）：")
        print("嵌套参数用点号：strategy.quantity=500")
        while True:
            line = input("> ").strip()
            if not line:
                break
            if '=' not in line:
                print("  ⚠️  格式错误，请使用 key=value 格式")
                continue
            key, value = line.split('=', 1)
            updates[key.strip()] = value.strip()

    if not updates:
        print("\n⚠️  未修改任何参数")
        return

    # 应用修改
    for key, value in updates.items():
        if isinstance(value, str):
            if value.lower() == 'true': value = True
            elif value.lower() == 'false': value = False
            else:
                try:
                    value = int(value) if '.' not in str(value) else float(value)
                except ValueError:
                    pass

        parts = key.split('.')
        target = config
        for p in parts[:-1]:
            target = target.setdefault(p, {})
        target[parts[-1]] = value

    # 上传
    print("\n正在上传新配置...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        tmp_path = Path(f.name)
    try:
        success, error = upload_files(ip, password, tmp_path, config_path)
        if success:
            print("✅ 配置已更新")
            for key, value in updates.items():
                print(f"  {key}: {value}")
            if name in HOT_RELOAD_STRATEGIES:
                print("\n💡 提示：策略支持热加载，无需重启即可生效")
            else:
                print("\n💡 提示：配置已更新，需要重启才能生效")
        else:
            print(f"❌ 上传配置失败: {error}")
    finally:
        tmp_path.unlink(missing_ok=True)


# ==================== 主菜单 ====================
def main():
    result = subprocess.run("which sshpass", shell=True, capture_output=True)
    if result.returncode != 0:
        print("❌ 未安装 sshpass，请先安装：brew install hudochenkov/sshpass/sshpass")
        sys.exit(1)

    print("=" * 60)
    print("         Binance Python 策略管理工具 v1.0")
    print("=" * 60)
    print("\n可用操作：")
    print("  1. 启动策略")
    print("  2. 停止策略")
    print("  3. 查看运行状态")
    print("  4. 查看日志（最后50行）")
    print("  5. 查看配置")
    print("  6. 修改配置")
    print("  7. 同步配置（.env + 策略配置）")
    print("  8. 部署代码")

    action = input("\n请选择操作 (1-8): ").strip()
    if action not in [str(i) for i in range(1, 9)]:
        print("❌ 无效的操作选择")
        sys.exit(1)

    servers = read_servers(EXCEL_PATH)
    if not servers:
        print("❌ 未找到任何可用服务器")
        sys.exit(1)

    selected_servers = select_servers(servers)
    if not selected_servers:
        print("❌ 未选择任何服务器")
        sys.exit(1)

    # 同步配置
    if action == "7":
        print("\n当前本地配置：")
        for _, (sname, spath, sdesc) in STRATEGY_CONFIGS.items():
            lp = PROJECT_DIR / spath
            if lp.exists():
                cfg = load_yaml_config(lp)
                symbol = get_config_value(cfg, sname, "symbol")
                vol = get_config_value(cfg, sname, "max_volume")
                print(f"  {sdesc}: symbol={symbol} max_volume={vol}")

        print("\n选择同步的策略：")
        for k, (sname, _, sdesc) in STRATEGY_CONFIGS.items():
            print(f"  {k}. {sdesc} ({sname})")
        print("  all. 全部策略")
        sc = input("请选择（多个用逗号分隔，或 all）: ").strip()

        if sc == "all":
            strategies_to_sync = list(STRATEGY_CONFIGS.values())
        else:
            strategies_to_sync = [STRATEGY_CONFIGS[x.strip()] for x in sc.split(",") if x.strip() in STRATEGY_CONFIGS]
        if not strategies_to_sync:
            print("❌ 无效选择")
            sys.exit(1)

        rand_qty = input("\n随机化 quantity? (y/N): ").strip().lower() == "y"
        rand_vol = input("随机化 max_volume? (y/N): ").strip().lower() == "y"
        password = get_password()

        success_count, failed_count = 0, 0
        for server in selected_servers:
            if sync_server(server, password, strategies_to_sync, rand_qty=rand_qty, rand_vol=rand_vol):
                success_count += 1
            else:
                failed_count += 1

        print(f"\n{'='*60}")
        print(f"同步完成！成功: {success_count}，失败: {failed_count}")
        print(f"{'='*60}")
        return

    # 部署代码
    if action == "8":
        full_setup = input("\n是否完整安装（含 conda 环境）？(y/N): ").strip().lower() == "y"
        password = get_password()

        print(f"\n{'='*60}")
        print(f"🚀 开始并行部署 {len(selected_servers)} 台服务器...")
        print(f"{'='*60}")

        success_list, failed_list = [], []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=min(10, len(selected_servers))) as executor:
            futures = {executor.submit(deploy_server, s, password, full_setup): s for s in selected_servers}
            for future in as_completed(futures):
                try:
                    success, srv, error = future.result()
                    (success_list if success else failed_list).append(srv if success else (srv, error))
                except Exception as e:
                    s = futures[future]
                    failed_list.append((s, str(e)))
                    with print_lock:
                        print(f"[{s['id']}] ❌ 部署异常: {e}")

        print(f"\n{'='*60}")
        print(f"部署完成！成功: {len(success_list)}，失败: {len(failed_list)} (耗时 {time.time() - start_time:.1f}s)")
        print(f"{'='*60}")
        if failed_list:
            print(f"\n❌ 失败的服务器:")
            for s, error in failed_list:
                print(f"   {s['id']}. {s['ip']} - {error}")
        return

    # 操作 1-6 需要选择策略
    strategy = select_strategy()
    if not strategy:
        print("❌ 无效的策略选择")
        sys.exit(1)

    mode, name, desc = strategy
    password = get_password()

    display_mode = None
    if action == "5":
        print("\n选择显示模式：")
        print("  1. 美观格式（分组显示）")
        print("  2. 原始 YAML")
        mode_choice = input("请选择 (1/2，默认1): ").strip()
        display_mode = 'raw' if mode_choice == '2' else 'formatted'

    action_map = {
        "1": start_strategy,
        "2": stop_strategy,
        "3": view_status,
        "4": view_logs,
        "5": view_config,
        "6": update_config,
    }

    handler = action_map[action]
    for server in selected_servers:
        if action == "5":
            handler(server, mode, name, desc, password, display_mode)
        else:
            handler(server, mode, name, desc, password)

    print(f"\n{'='*60}")
    print("操作完成！")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
