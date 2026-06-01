#!/bin/bash
# ============================================
#  股票量化分析系统 - 一键启动脚本
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT=${PORT:-8888}
LOG_FILE=${LOG_FILE:-/tmp/stock_server.log}
PID_FILE="$SCRIPT_DIR/server.pid"

echo "============================================"
echo "  股票量化分析系统"
echo "============================================"

# 检查 Python3
if ! command -v python3 &>/dev/null; then
    echo "❌ 未找到 python3，请先安装 Python 3.9+"
    exit 1
fi

echo "✓ Python: $(python3 --version)"

# 创建虚拟环境（可选）
if [ ! -d "venv" ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv venv
fi

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
echo "📦 安装依赖..."
pip install -q -r requirements.txt

# 停止旧进程
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "🛑 停止旧进程 (PID: $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

# 启动服务
echo "🚀 启动服务 (端口: $PORT)..."
nohup python server.py > "$LOG_FILE" 2>&1 < /dev/null &
PID=$!
echo $PID > "$PID_FILE"

sleep 2

# 验证启动
if kill -0 "$PID" 2>/dev/null; then
    echo ""
    echo "============================================"
    echo "  ✅ 服务启动成功!"
    echo "============================================"
    echo "  访问地址: http://localhost:$PORT/stock_analysis"
    echo "  进程 PID: $PID"
    echo "  日志文件: $LOG_FILE"
    echo "  停止服务: bash stop.sh"
    echo "============================================"
else
    echo "❌ 启动失败，请查看日志: $LOG_FILE"
    exit 1
fi
