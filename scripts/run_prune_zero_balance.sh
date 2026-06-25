#!/bin/bash
set -euo pipefail
exec /app/scripts/run_python.sh /app/scopefi-score/prune_long_short_ratio_zero_balance.py "$@"
