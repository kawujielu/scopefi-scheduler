#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段二：读取 fills_data/ 下各地址本地 CSV，按交易对（coin）做增强版评分。

评分逻辑（同 scopefi/strategy-data-main/tpinghua_mul.py，仅增强版）：
  1. 仅保留 dir 含 long/short 的成交
  2. 按时间排序，对 closedPnl 做累计
  3. 线性回归：X=成交序号，y=累计盈亏 → 得到 R²
  4. score = R² * (mean_pnl / std_pnl) * sqrt(N)
     - std_pnl <= 0 或 NaN 时记为 0
     - 仅当 MIN_TRADES <= N < MAX_TRADES（默认 100 <= N < 3000）才打分输出
     - coin 以 xyz: 开头的不参与打分

  输出字段：address、coin、N、score、total_pnl、status
  status：高手(score>1.5) / 新手(score<-1.5) / 其他

两阶段流程:
  1. python fetch_fills_batch.py     # Hydromancer → fills_data/{address}.csv
  2. python score_fills_by_symbol.py # 读本地 CSV 打分 → scores_by_symbol.csv

依赖: pip install pandas numpy scikit-learn pyarrow
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

# ---------- 固定配置 ----------
PARQUET_FILE_NAME = "active_perp_addresses_filtered.parquet"
OUTPUT_CSV = "scores_by_symbol.csv"
MAX_ADDRESSES = 0

MIN_TRADES = 100
MAX_TRADES = 3000   # 有效成交笔数须 < 此值
COIN_SKIP_PREFIX = "xyz:"
SCORE_GAOSHOU = 1.5
SCORE_XINSHOU = -1.5
# ------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PARQUET_FILE = SCRIPT_DIR / PARQUET_FILE_NAME


def classify_score(score: float) -> str:
    if score > SCORE_GAOSHOU:
        return "高手"
    if score < SCORE_XINSHOU:
        return "新手"
    return "其他"


def score_coin_trades(df_coin: pd.DataFrame) -> dict | None:
    coin = str(df_coin["coin"].iloc[0]) if len(df_coin) else ""
    if coin.startswith(COIN_SKIP_PREFIX):
        return None

    df = df_coin[df_coin["dir"].str.contains("long|short", case=False, na=False)].copy()
    df = df.sort_values("date").reset_index(drop=True)

    n = len(df)
    if n < MIN_TRADES or n >= MAX_TRADES:
        return None

    df["cum_pnl"] = df["closed_pnl"].cumsum()
    x = np.arange(1, n + 1).reshape(-1, 1)
    y = df["cum_pnl"].values

    model = LinearRegression().fit(x, y)
    r_sq = float(model.score(x, y))

    mean_pnl = float(df["closed_pnl"].mean())
    std_pnl = float(df["closed_pnl"].std(ddof=1))
    if std_pnl > 0 and not np.isnan(std_pnl):
        score = float(r_sq * (mean_pnl / std_pnl) * np.sqrt(n))
    else:
        score = 0.0

    return {
        "coin": coin,
        "N": n,
        "score": score,
        "total_pnl": float(df["closed_pnl"].sum()),
        "status": classify_score(score),
    }


def score_address_by_coin(df: pd.DataFrame, address: str) -> pd.DataFrame:
    rows: list[dict] = []
    for coin, grp in df.groupby("coin", sort=True):
        row = score_coin_trades(grp)
        if row is not None:
            row["address"] = address
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["address", "coin", "N", "score", "total_pnl", "status"])
    return pd.DataFrame(rows).sort_values("score", ascending=False, kind="mergesort")


def print_summary(result: pd.DataFrame, *, stats: dict) -> None:
    print(f"\n{'=' * 72}")
    print("打分汇总（阶段二 · 本地 CSV）")
    print(f"{'=' * 72}")
    print(f"parquet 地址数:   {stats['total_addrs']:,}")
    print(f"有本地文件:       {stats['has_file']:,}")
    print(f"无文件(跳过):     {stats['no_file']:,}")
    print(f"有成交记录:       {stats['with_fills']:,}")
    print(f"有打分结果:       {stats['scored_addrs']:,}  (coin 满足 {MIN_TRADES}<=N<{MAX_TRADES}，且非 xyz:)")
    print(f"输出行数(coin级): {len(result):,}")

    if result.empty:
        print("\n无满足条件的打分结果")
        return

    for label in ("高手", "新手", "其他"):
        cnt = int((result["status"] == label).sum())
        print(f"  {label}: {cnt:,} 条")

    print(f"\n--- 前 20 条（按 score 降序）---")
    header = f"{'address':<44} {'coin':<12} {'N':>6} {'score':>12} {'total_pnl':>12} {'status':<6}"
    print(header)
    print("-" * len(header))
    for _, r in result.head(20).iterrows():
        print(
            f"{str(r['address']):<44} "
            f"{str(r['coin']):<12} "
            f"{int(r['N']):>6} "
            f"{float(r['score']):>12.6f} "
            f"{float(r['total_pnl']):>12.4f} "
            f"{str(r['status']):<6}"
        )


def main() -> int:
    from address_loader import load_addresses
    from fills_local_store import (
        FILLS_DATA_DIR,
        csv_path_for_address,
        fills_csv_to_score_df,
    )

    data_dir = SCRIPT_DIR / FILLS_DATA_DIR
    if not data_dir.is_dir():
        print(f"未找到目录 {data_dir}，请先运行 fetch_fills_batch.py", file=sys.stderr)
        return 1

    addresses, addr_col = load_addresses(PARQUET_FILE)
    if MAX_ADDRESSES > 0:
        addresses = addresses[:MAX_ADDRESSES]

    stats = {
        "total_addrs": len(addresses),
        "has_file": 0,
        "no_file": 0,
        "with_fills": 0,
        "scored_addrs": 0,
    }
    all_rows: list[dict] = []

    print(f"地址文件: {PARQUET_FILE.name}  列: {addr_col}  共 {len(addresses)} 个地址")
    print(f"数据目录: {data_dir}")
    print(f"MIN_TRADES={MIN_TRADES}  MAX_TRADES={MAX_TRADES}  跳过 coin 前缀={COIN_SKIP_PREFIX!r}\n")

    for idx, addr in enumerate(addresses, start=1):
        csv_path = csv_path_for_address(data_dir, addr)
        if not csv_path.is_file():
            stats["no_file"] += 1
            continue

        stats["has_file"] += 1
        try:
            df = fills_csv_to_score_df(csv_path)
        except ValueError as exc:
            print(f"  跳过 {addr}: {exc}", file=sys.stderr)
            continue

        if df.empty:
            continue

        stats["with_fills"] += 1
        scored = score_address_by_coin(df, addr)
        if scored.empty:
            continue

        stats["scored_addrs"] += 1
        all_rows.extend(scored.to_dict(orient="records"))

        if idx % 200 == 0:
            print(f"  已处理 {idx}/{len(addresses)} ...")

    result = pd.DataFrame(all_rows)
    if not result.empty:
        result = result.sort_values("score", ascending=False, kind="mergesort")
        out_path = SCRIPT_DIR / OUTPUT_CSV
        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n已写入 {out_path} ({len(result):,} 行)")

    print_summary(result, stats=stats)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("[error]", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(1) from None
