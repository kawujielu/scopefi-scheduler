#!/bin/bash
set -euo pipefail
cd /app/strategy-data-main
python ch_filter_active_addresses.py
