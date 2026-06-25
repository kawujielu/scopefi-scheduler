#!/bin/bash
set -euo pipefail
exec /app/scripts/run_python.sh /app/scopefi-score/ch_score_to_long_short_ratio.py --write
