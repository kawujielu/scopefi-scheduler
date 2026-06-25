#!/bin/bash
# 运行 Python 脚本，日志追加到 /app/logs/<脚本名>.log，同时输出到 stdout
set -euo pipefail
SCRIPT_PATH="${1:?用法: run_python.sh /path/to/script.py [args...]}"
shift
WORKDIR="$(dirname "$SCRIPT_PATH")"
SCRIPT="$(basename "$SCRIPT_PATH")"
LOG="/app/logs/${SCRIPT%.py}.log"
mkdir -p /app/logs
cd "$WORKDIR"
{
  echo "========== $(date '+%Y-%m-%d %H:%M:%S %Z') pid=$$ =========="
  python -u "$SCRIPT" "$@"
} 2>&1 | tee -a "$LOG"
