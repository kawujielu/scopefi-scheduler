#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 ClickHouse 读取 active_perp_addresses，批量查 ScopeFi 资金后过滤，写入 active_addresses。

过滤规则同 filter_active_address.py：每地址最新 create_time、balance、active_days_30d、
last_active_day、trades_all_time。balance 仅用于内存过滤，不写入 ClickHouse；
最终筛选结果另存为 CSV（含 address、balance）。

需在 scopefi/.env 配置 SCOPEFI_TRACK_BASE_URL 等（同 query_wallet_balance.py）。

用法:
    python ch_filter_active_addresses.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import date
from pathlib import Path

import clickhouse_connect
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
SCOPEFI_DIR = SCRIPT_DIR.parent
if str(SCOPEFI_DIR) not in sys.path:
    sys.path.insert(0, str(SCOPEFI_DIR))

from query_wallet_balance import (  # noqa: E402
    _chunked,
    fetch_balances_map,
    load_dotenv,
    normalize_wallet,
    ScopeFiSettings,
)

# ======================== ClickHouse 配置 ========================
CLICKHOUSE_HOST = "192.168.112.239"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "writer_pro"
CLICKHOUSE_PASSWORD = "Y14%s-^X5U=@FkH_Ga"
CLICKHOUSE_DATABASE = "pro"

SOURCE_TABLE = "active_perp_addresses"
TARGET_TABLE = "active_addresses"
INSERT_BATCH = 10000
PREVIEW_ROWS = 10
OUTPUT_CSV = SCRIPT_DIR / "active_addresses_filtered.csv"

# ======================== 过滤规则（同 filter_active_address.py）========================
USER_COL = "user"
CREATE_TIME_COL = "create_time"
BALANCE_COL = "balance"
TRADES_COL = "trades_all_time"
ACTIVE_DAYS_COL = "active_days_30d"
LAST_ACTIVE_COL = "last_active_day"

BALANCE_MIN = 200
BALANCE_MAX = 2_000_000
ACTIVE_DAYS_MIN = 5
LAST_ACTIVE_MAX_DAYS_AGO = 3
TRADES_MIN = 50
TRADES_MAX = 3_000

# ScopeFi 批量查资金（同 query_wallet_balance.py）
BALANCE_BATCH_SIZE = 1000
BALANCE_BATCH_INTERVAL_SEC = 1.0

# 非真实/占位地址，不写入 active_addresses
INVALID_ADDRESSES = frozenset(
    {
        "0x0000000000000000000000000000000000000000",
    }
)
# ==============================================================================


def _configure_stdio() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _print_step(step: str, remaining: int, *, removed: int | None = None) -> None:
    line = f"  {step} → 剩余 {remaining:,} 条"
    if removed is not None and removed > 0:
        line += f"  (本步剔除 {removed:,})"
    print(line)


def is_valid_trader_address(addr: object) -> bool:
    """42 位 0x 地址，且非零地址等占位地址。"""
    s = str(addr).strip()
    if len(s) != 42 or not s.startswith("0x"):
        return False
    lower = s.lower()
    if lower in INVALID_ADDRESSES:
        return False
    hex_part = lower[2:]
    if len(hex_part) != 40 or not all(c in "0123456789abcdef" for c in hex_part):
        return False
    if int(hex_part, 16) == 0:
        return False
    return True


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def fetch_latest_per_user(client) -> pd.DataFrame:
    """等价于 GROUP BY user + argMax(..., create_time)。"""
    # 子查询将 create_time 重命名为 ts，避免输出别名 create_time 与 argMax 第二参数冲突 (code 184)
    sql = f"""
    SELECT
        user,
        argMax(volume_30d, ts) AS volume_30d,
        argMax(fills_30d, ts) AS fills_30d,
        argMax(active_days_30d, ts) AS active_days_30d,
        argMax(last_active_day, ts) AS last_active_day,
        max(ts) AS create_time,
        argMax(pnl_all_time, ts) AS pnl_all_time,
        argMax(win_rate_all_time, ts) AS win_rate_all_time,
        argMax(trades_all_time, ts) AS trades_all_time
    FROM (
        SELECT
            user,
            volume_30d,
            fills_30d,
            active_days_30d,
            last_active_day,
            create_time AS ts,
            pnl_all_time,
            win_rate_all_time,
            trades_all_time
        FROM {SOURCE_TABLE}
        WHERE lower(user) NOT IN ({",".join(repr(a) for a in INVALID_ADDRESSES)})
    )
    GROUP BY user
    """
    print(f"读取 {CLICKHOUSE_DATABASE}.{SOURCE_TABLE}（每地址保留最新 create_time）...")
    result = client.query(sql)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    print(f"  拉取完成: {len(df):,} 条（唯一地址）")
    return df


def _collect_wallet_addresses(df: pd.DataFrame) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in df[USER_COL]:
        if not is_valid_trader_address(raw):
            continue
        try:
            w = normalize_wallet(str(raw))
        except ValueError:
            continue
        key = w.lower()
        if key not in seen:
            seen.add(key)
            out.append(w)
    return out


async def attach_balances(df: pd.DataFrame) -> pd.DataFrame:
    """批量查询 ScopeFi 账户净值，写入 balance 列（仅内存，不落库）。"""
    addresses = _collect_wallet_addresses(df)
    if not addresses:
        work = df.copy()
        work[BALANCE_COL] = pd.NA
        return work

    batches = _chunked(addresses, BALANCE_BATCH_SIZE)
    settings = ScopeFiSettings.from_env()
    total_batches = len(batches)

    print("\n批量查询 ScopeFi 资金（仅用于过滤）")
    print(f"  API: {settings.position_url}")
    print(
        f"  待查地址: {len(addresses):,}  "
        f"批次数: {total_batches}  每批: {BALANCE_BATCH_SIZE}  "
        f"间隔: {BALANCE_BATCH_INTERVAL_SEC}s"
    )

    balance_by_user: dict[str, float | None] = {}
    for i, batch in enumerate(batches, start=1):
        print(f"\n[balance batch {i}/{total_batches}] 查询 {len(batch)} 个地址 ...")
        fetched = await fetch_balances_map(batch)
        balance_by_user.update(fetched)
        found = sum(1 for v in fetched.values() if v is not None)
        print(f"  返回 {found}/{len(batch)} 个有效账户")
        if i < total_batches and BALANCE_BATCH_INTERVAL_SEC > 0:
            await asyncio.sleep(BALANCE_BATCH_INTERVAL_SEC)

    work = df.copy()

    def _lookup_balance(raw: object) -> float | None:
        try:
            key = normalize_wallet(str(raw)).lower()
        except ValueError:
            return None
        return balance_by_user.get(key)

    work[BALANCE_COL] = work[USER_COL].map(_lookup_balance)
    queried = work[BALANCE_COL].notna().sum()
    print(f"\n[balance] 已查询 {queried:,}/{len(work):,} 行有 balance 值")
    return work


def filter_addresses(df: pd.DataFrame) -> pd.DataFrame:
    for col in (USER_COL, TRADES_COL, ACTIVE_DAYS_COL, LAST_ACTIVE_COL, BALANCE_COL):
        if col not in df.columns:
            raise ValueError(f"缺少列 {col!r}，现有: {df.columns.tolist()}")

    print("\n过滤规则:")
    print(f"  balance 范围: [{BALANCE_MIN:,}, {BALANCE_MAX:,}]")
    print(f"  {ACTIVE_DAYS_COL} 须 > {ACTIVE_DAYS_MIN}")
    print(f"  {LAST_ACTIVE_COL} 距今天 < {LAST_ACTIVE_MAX_DAYS_AGO} 天")
    print(f"  {TRADES_COL} 范围: [{TRADES_MIN:,}, {TRADES_MAX:,}]")
    print("\n过滤步骤:")

    n0 = len(df)
    _print_step("1) 源表去重后", n0)

    mask_valid_addr = df[USER_COL].map(is_valid_trader_address)
    df0 = df.loc[mask_valid_addr].copy()
    n0b = len(df0)
    _print_step("2) 剔除非真实/占位地址", n0b, removed=n0 - n0b)

    bal = pd.to_numeric(df0[BALANCE_COL], errors="coerce")
    mask_balance = bal.notna() & (bal >= BALANCE_MIN) & (bal <= BALANCE_MAX)
    df1 = df0.loc[mask_balance].copy()
    n1 = len(df1)
    _print_step(
        f"3) balance 过滤 [{BALANCE_MIN:,}, {BALANCE_MAX:,}]",
        n1,
        removed=n0b - n1,
    )

    active_days = pd.to_numeric(df1[ACTIVE_DAYS_COL], errors="coerce")
    mask_active_days = active_days.notna() & (active_days > ACTIVE_DAYS_MIN)
    df2 = df1.loc[mask_active_days].copy()
    n2 = len(df2)
    _print_step(
        f"4) {ACTIVE_DAYS_COL} 过滤 (> {ACTIVE_DAYS_MIN})",
        n2,
        removed=n1 - n2,
    )

    today = pd.Timestamp(date.today())
    last_active = pd.to_datetime(df2[LAST_ACTIVE_COL], errors="coerce").dt.normalize()
    days_ago = (today - last_active).dt.days
    mask_last_active = (
        last_active.notna()
        & (days_ago >= 0)
        & (days_ago < LAST_ACTIVE_MAX_DAYS_AGO)
    )
    df3 = df2.loc[mask_last_active].copy()
    n3 = len(df3)
    _print_step(
        f"5) {LAST_ACTIVE_COL} 过滤 (距今天 < {LAST_ACTIVE_MAX_DAYS_AGO} 天)",
        n3,
        removed=n2 - n3,
    )

    trades = pd.to_numeric(df3[TRADES_COL], errors="coerce")
    mask_trades = trades.notna() & (trades >= TRADES_MIN) & (trades <= TRADES_MAX)
    df4 = df3.loc[mask_trades].copy()
    n4 = len(df4)
    _print_step(
        f"6) {TRADES_COL} 过滤 [{TRADES_MIN:,}, {TRADES_MAX:,}]",
        n4,
        removed=n3 - n4,
    )

    print("\n汇总:")
    print(f"  初始: {n0:,} 条")
    print(f"  最终剩余: {n4:,} 条")
    print(f"  累计剔除: {n0 - n4:,} 条")
    return df4


def _normalize_trader(addr: str) -> str | None:
    if not is_valid_trader_address(addr):
        return None
    return str(addr).strip()


def preview_before_insert(df: pd.DataFrame) -> None:
    total = len(df)
    print(f"\n=== 写入前预览（共 {total:,} 条，显示前 {min(PREVIEW_ROWS, total)} 条）===")
    if total == 0:
        print("(无符合条件地址，跳过写入)")
        return

    show_cols = [
        c
        for c in (
            USER_COL,
            BALANCE_COL,
            ACTIVE_DAYS_COL,
            LAST_ACTIVE_COL,
            TRADES_COL,
            "volume_30d",
            "pnl_all_time",
        )
        if c in df.columns
    ]
    preview = df[show_cols].head(PREVIEW_ROWS)
    print("\t".join(show_cols))
    for _, row in preview.iterrows():
        print("\t".join(str(row[c]) for c in show_cols))
    print()


def save_filtered_to_csv(df: pd.DataFrame) -> int:
    """将最终筛选结果写入 CSV，含 address 与查询得到的 balance。"""
    if df.empty:
        print("\n[csv] 无符合条件地址，跳过 CSV 导出")
        return 0

    export = df.copy()
    export = export.rename(columns={USER_COL: "address"})

    cols = ["address", BALANCE_COL]
    for c in (
        ACTIVE_DAYS_COL,
        LAST_ACTIVE_COL,
        TRADES_COL,
        CREATE_TIME_COL,
        "volume_30d",
        "fills_30d",
        "pnl_all_time",
        "win_rate_all_time",
    ):
        if c in export.columns and c not in cols:
            cols.append(c)

    export = export[cols]
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    export.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"\n[ok] 已写入 CSV: {OUTPUT_CSV} ({len(export):,} 行)")
    return len(export)


def save_to_active_addresses(client, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    version = int(time.time())
    rows: list[tuple[str, int]] = []
    skipped = 0
    for addr in df[USER_COL]:
        trader = _normalize_trader(addr)
        if trader is None:
            skipped += 1
            continue
        rows.append((trader, version))

    if skipped:
        print(f"[警告] {skipped:,} 个地址长度非 42，已跳过")

    if not rows:
        print("[error] 无有效地址可写入")
        return 0

    fq = f"{CLICKHOUSE_DATABASE}.{TARGET_TABLE}"
    print(f"\n写入 {fq}（version={version}，共 {len(rows):,} 条）...")

    for i in range(0, len(rows), INSERT_BATCH):
        batch = rows[i : i + INSERT_BATCH]
        client.insert(
            table=TARGET_TABLE,
            data=batch,
            column_names=["trader", "version"],
        )
        print(f"  已插入 {min(i + INSERT_BATCH, len(rows)):,} / {len(rows):,}")

    print(f"[ok] 已写入 {len(rows):,} 条 → {fq}")
    return len(rows)


async def run_pipeline(client) -> int:
    df = fetch_latest_per_user(client)
    df = await attach_balances(df)
    filtered = filter_addresses(df)
    save_filtered_to_csv(filtered)
    preview_before_insert(filtered)
    save_to_active_addresses(client, filtered)
    return 0


def main() -> int:
    _configure_stdio()
    env_path = load_dotenv()
    if env_path:
        print(f"[env] 已加载 {env_path.name}", flush=True)

    print(
        f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}",
        flush=True,
    )

    try:
        client = get_client()
    except Exception as e:
        print(f"[error] 连接 ClickHouse 失败: {e}", file=sys.stderr)
        return 1

    try:
        return asyncio.run(run_pipeline(client))
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n已取消", file=sys.stderr)
        raise SystemExit(130) from None
