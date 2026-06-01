#!/bin/bash
# ============================================
#  股票量化分析系统 - 停止脚本
# ============================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/server.pid"
PORT=${PORT:-8888}

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "🛑 停止服务 (PID: $PID)..."
        kill "$PID"
        sleep 1
        if kill -0 "$PID" 2>/dev/null; then
            kill -9 "$PID"
        fi
        rm -f "$PID_FILE"
        echo "✅ 服务已停止"
    else
        echo "⚠️ 进程 $PID 不存在，清理 PID 文件"
        rm -f "$PID_FILE"
    fi
else
    # 尝试通过端口查找
    PID=$(lsof -ti:$PORT 2>/dev/null)
    if [ -n "$PID" ]; then
        echo "🛑 通过端口 $PORT 找到进程 $PID，正在停止..."
        kill $PID 2>/dev/null
        echo "✅ 服务已停止"
    else
        echo "⚠️ 未找到运行中的服务"
    fi
fi
