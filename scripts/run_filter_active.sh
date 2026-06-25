#!/bin/bash
set -euo pipefail
exec /app/scripts/run_python.sh /app/strategy-data-main/ch_filter_active_addresses.py
