#!/bin/bash
set -euo pipefail
cd /app/scopefi-score
python ch_score_to_long_short_ratio.py --write
