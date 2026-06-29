#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 ClickHouse user_fill_v2 按交易对打分，写入 long_short_ratio。

打分规则与 score_fills_by_symbol.py 一致（增强版 score = R² * mean/std * sqrt(N)）。
写入 long_short_ratio 时额外要求：高手须 total_pnl > 0，新手须 total_pnl < 0。
ADDRESS_OVERALL_SIDE_FILTER 开启时，写表/CSV 前再按 hyper_portfolio.pnl_history 最新项过滤：
  pnl>0 只保留高手池，pnl<0 只保留新手池。
最终高手/新手地址另存 CSV（全部 coin，含 coin、balance、总盈亏、交易笔数、得分）。
balance 经 ScopeFi API 查询；balance 为空或 0 的地址不写入 CSV 与 long_short_ratio。
仅统计 active_addresses 表中的地址（与 ch_filter_active_addresses.py 输出一致）。

用法:
    python ch_score_to_long_short_ratio.py           # 默认只预览不写库
    python ch_score_to_long_short_ratio.py --write   # 写入 long_short_ratio
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from score_fills_by_symbol import (
    COIN_SKIP_PREFIX,
    MAX_TRADES,
    MIN_TRADES,
    SCORE_GAOSHOU,
    SCORE_XINSHOU,
    classify_score,
)

SCRIPT_DIR = Path(__file__).resolve().parent


def _scopefi_dir() -> Path:
    for p in (SCRIPT_DIR.parent / "scopefi", SCRIPT_DIR, SCRIPT_DIR.parent):
        if (p / "query_wallet_balance.py").is_file():
            return p
    return SCRIPT_DIR.parent / "scopefi"


SCOPEFI_DIR = _scopefi_dir()


def _ensure_scopefi_path() -> None:
    if str(SCOPEFI_DIR) not in sys.path:
        sys.path.insert(0, str(SCOPEFI_DIR))


def _bind_clickhouse_config(globals_ns: dict[str, Any]) -> None:
    """从 .env 注入 CLICKHOUSE_*；兼容旧版 query_wallet_balance（无 bind_clickhouse_config）。"""
    _ensure_scopefi_path()
    from query_wallet_balance import load_dotenv  # noqa: E402

    load_dotenv()
    try:
        from query_wallet_balance import bind_clickhouse_config  # noqa: E402

        bind_clickhouse_config(globals_ns)
        return
    except ImportError:
        pass
    try:
        from query_wallet_balance import ClickHouseSettings  # noqa: E402

        s = ClickHouseSettings.from_env()
        globals_ns["CLICKHOUSE_HOST"] = s.host
        globals_ns["CLICKHOUSE_PORT"] = s.port
        globals_ns["CLICKHOUSE_USER"] = s.user
        globals_ns["CLICKHOUSE_PASSWORD"] = s.password
        globals_ns["CLICKHOUSE_DATABASE"] = s.database
        return
    except ImportError:
        import os

        def _env(key: str, *, default: str = "", required: bool = False) -> str:
            val = (os.environ.get(key) or default).strip().strip('"').strip("'")
            if required and not val:
                raise ValueError(f"环境变量 {key} 未配置（请在 .env 中设置）")
            return val

        globals_ns["CLICKHOUSE_HOST"] = _env("CLICKHOUSE_HOST", required=True)
        globals_ns["CLICKHOUSE_PORT"] = int(_env("CLICKHOUSE_PORT", default="8123") or "8123")
        globals_ns["CLICKHOUSE_USER"] = _env("CLICKHOUSE_USER", required=True)
        globals_ns["CLICKHOUSE_PASSWORD"] = _env("CLICKHOUSE_PASSWORD", required=True)
        globals_ns["CLICKHOUSE_DATABASE"] = _env("CLICKHOUSE_DATABASE", default="pro") or "pro"

# ClickHouse 见 scopefi/.env（main 中 bind_clickhouse_config 注入 CLICKHOUSE_*）
CLICKHOUSE_HOST = ""
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = ""
CLICKHOUSE_PASSWORD = ""
CLICKHOUSE_DATABASE = "pro"

FILLS_TABLE = "user_fill_v2"
TARGET_TABLE = "long_short_ratio"
ACTIVE_ADDRESSES_TABLE = "active_addresses"
HYPER_PORTFOLIO_TABLE = "hyper_portfolio"

# long_short_ratio 写入列（version 由表 DEFAULT now64(6) 自动填充，不写入）
LONG_SHORT_RATIO_INSERT_COLUMNS: tuple[str, ...] = (
    "coin",
    "gaoshou_count",
    "gaoshou_longCount",
    "gaoshou_shortCount",
    "gaoshou_longRatio",
    "gaoshou_shortRatio",
    "gaoshou_addresses",
    "xinshou_count",
    "xinshou_longCount",
    "xinshou_shortCount",
    "xinshou_longRatio",
    "xinshou_shortRatio",
    "xinshou_addresses",
    "timestamp",
)
# 与 pro.long_short_ratio 一致；显式指定类型可避免 insert_df 的 DESCRIBE（需 SHOW COLUMNS）
LONG_SHORT_RATIO_COLUMN_TYPES: tuple[str, ...] = (
    "String",
    "UInt32",
    "UInt32",
    "UInt32",
    "UInt32",
    "UInt32",
    "Array(String)",
    "UInt32",
    "UInt32",
    "UInt32",
    "UInt32",
    "UInt32",
    "Array(String)",
    "DateTime64(3)",
)

LOOKBACK_DAYS = 0  # 0 = 不限制时间；>0 则仅统计最近 N 天成交
INVALID_TRADER = "0x0000000000000000000000000000000000000000"
PREVIEW_COUNT = 3
TARGET_PREVIEW_ROWS = 10
OUTPUT_SCORED_CSV = SCRIPT_DIR / "long_short_scored_addresses.csv"
INSPECT_TARGET_TABLE = True  # 启动时 DESCRIBE/预览目标表；测试库权限不足时可设为 False
# 写表/CSV 前按地址全 coin 合计 pnl 过滤高手/新手池
ADDRESS_OVERALL_SIDE_FILTER = True
# 非空时只统计这些 coin（None=从库中取全部 coin）
SCORE_COINS: tuple[str, ...] | None = ("BTC", "ETH", "SOL", "HYPE")

BALANCE_BATCH_SIZE = 1000
BALANCE_BATCH_INTERVAL_SEC = 1.0

_START_POS_CANDIDATES = ("start_pos", "start_position", "startPosition")
_CLOSED_PNL_CANDIDATES = ("closed_pnl", "closedPnl")
# user_fill_v2 已知列；测试环境可跳过 DESCRIBE（避免无权限或多余查询）
_FILLS_KNOWN_COLUMNS = frozenset(
    {
        "trader",
        "coin",
        "time",
        "dir",
        "sz",
        "start_pos",
        "start_position",
        "startPosition",
        "closed_pnl",
        "closedPnl",
    }
)
DESCRIBE_FILLS_AT_STARTUP = True  # 测试脚本设为 False，直接用 _FILLS_KNOWN_COLUMNS
_table_cols_cache: frozenset[str] | None = None
# ===========================================================


def _configure_stdio() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def get_client():
    _ensure_scopefi_path()
    from query_wallet_balance import get_clickhouse_client  # noqa: E402

    return get_clickhouse_client()


def _fmt_preview_cell(v: object, *, max_len: int = 120) -> str:
    s = str(v)
    if len(s) <= max_len:
        return s
    return f"{s[: max_len - 3]}..."


def _target_table_fq() -> str:
    return f"{CLICKHOUSE_DATABASE}.{TARGET_TABLE}"


def _ch_query_warn(label: str, exc: Exception) -> None:
    print(f"[warn] {label}: {exc}", flush=True)


def _ch_query(client, label: str, sql: str, **kwargs):
    t0 = time.perf_counter()
    try:
        return client.query(sql, **kwargs)
    finally:
        print(f"[ch] {label}: {time.perf_counter() - t0:.2f}s", flush=True)


def _should_inspect_target(explicit: bool | None) -> bool:
    """非 pro 库（如 default 测试库）默认不探查目标表，避免 DESCRIBE 无权限。"""
    if explicit is not None:
        return explicit
    if CLICKHOUSE_DATABASE != "pro":
        return False
    return INSPECT_TARGET_TABLE


def inspect_target_table(client) -> None:
    """启动时查看 long_short_ratio 表结构、总条数、最新数据。"""
    fq = _target_table_fq()
    tbl = TARGET_TABLE

    print(f"\n=== 表结构 {fq} ===", flush=True)
    try:
        result = _ch_query(client, f"DESCRIBE {tbl}", f"DESCRIBE TABLE {tbl}")
        cols = result.column_names
        rows = result.result_rows
        if not rows:
            print("(无字段信息)", flush=True)
        else:
            print("\t".join(str(c) for c in cols), flush=True)
            for row in rows:
                print("\t".join(str(v) for v in row), flush=True)
            print(f"共 {len(rows)} 个字段", flush=True)
    except Exception as e:
        _ch_query_warn(f"无法 DESCRIBE {fq}（可能无 SHOW COLUMNS 权限）", e)

    try:
        count_result = _ch_query(client, f"count {tbl}", f"SELECT count() FROM {tbl}")
        total = int(count_result.result_rows[0][0])
    except Exception as e:
        _ch_query_warn(f"无法统计 {fq} 行数", e)
        print(flush=True)
        return

    print(f"\n=== {fq} 数据总量 ===", flush=True)
    print(f"共 {total:,} 条", flush=True)

    print(
        f"\n=== {fq} 最新 {TARGET_PREVIEW_ROWS} 条 "
        f"(ORDER BY version DESC, timestamp DESC, coin ASC) ===",
        flush=True,
    )
    if total == 0:
        print("(无数据)\n", flush=True)
        return

    try:
        latest = _ch_query(
            client,
            f"preview {tbl}",
            f"SELECT * FROM {tbl} "
            f"ORDER BY version DESC, timestamp DESC, coin ASC "
            f"LIMIT {int(TARGET_PREVIEW_ROWS)}",
        )
    except Exception as e:
        _ch_query_warn(f"无法预览 {fq} 数据", e)
        print(flush=True)
        return

    preview_cols = latest.column_names
    preview_rows = latest.result_rows
    print("\t".join(str(c) for c in preview_cols), flush=True)
    for row in preview_rows:
        print("\t".join(_fmt_preview_cell(v) for v in row), flush=True)
    shown = len(preview_rows)
    if total > shown:
        print(f"\n... 表内共 {total:,} 条，仅显示最新 {shown} 条", flush=True)
    print(f"共显示 {shown} 条\n", flush=True)


def _decode_trader(raw: object) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8").strip("\x00").strip()
    s = str(raw).strip()
    if s.startswith("b'") and s.endswith("'"):
        s = s[2:-1]
    return s


def _table_columns(client) -> frozenset[str]:
    global _table_cols_cache
    if _table_cols_cache is not None:
        return _table_cols_cache
    if not DESCRIBE_FILLS_AT_STARTUP:
        _table_cols_cache = _FILLS_KNOWN_COLUMNS
        return _table_cols_cache
    try:
        result = _ch_query(client, f"DESCRIBE {FILLS_TABLE}", f"DESCRIBE TABLE {FILLS_TABLE}")
        cols = frozenset(str(row[0]) for row in result.result_rows)
        if cols:
            _table_cols_cache = cols
            return cols
    except Exception as e:
        _ch_query_warn(f"无法 DESCRIBE {FILLS_TABLE}，使用已知列名", e)
    _table_cols_cache = _FILLS_KNOWN_COLUMNS
    return _table_cols_cache


def _pick_col(client, candidates: tuple[str, ...], label: str) -> str:
    cols = _table_columns(client)
    for name in candidates:
        if name in cols:
            return name
    raise ValueError(
        f"{FILLS_TABLE} 缺少 {label} 列；候选 {candidates}；"
        f"实际列 {sorted(cols)}"
    )


def _sql_ident(name: str) -> str:
    return f"`{name.replace('`', '')}`"


def _sql_float_expr(col: str, alias: str, table: str = "") -> str:
    col_sql = _sql_ident(col)
    if table:
        col_sql = f"{table}.{col_sql}"
    return f"toFloat64({col_sql}) AS {alias}"


def _fills_time_filter_sql(table_alias: str = "") -> str:
    time_col = f"{table_alias}.time" if table_alias else "time"
    if LOOKBACK_DAYS > 0:
        return (
            f"AND fromUnixTimestamp64Milli({time_col}) >= "
            f"now() - INTERVAL {int(LOOKBACK_DAYS)} DAY"
        )
    return ""


def _empty_fills_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["trader", "coin", "dir", "time", "closed_pnl", "start_pos", "sz"]
    )


def _scorable_traders_subquery() -> str:
    """long|short 成交笔数在 [MIN_TRADES, MAX_TRADES) 内的白名单地址。"""
    return f"""
    SELECT lower(toString(f2.trader)) AS trader
    FROM {FILLS_TABLE} AS f2
    INNER JOIN {ACTIVE_ADDRESSES_TABLE} AS a
        ON lower(toString(f2.trader)) = lower(a.trader)
    WHERE f2.coin = %(coin)s
      AND lower(toString(f2.trader)) != %(invalid)s
      AND match(f2.dir, '(?i)long|short')
      {_fills_time_filter_sql('f2')}
    GROUP BY trader
    HAVING count() >= {int(MIN_TRADES)} AND count() < {int(MAX_TRADES)}
    """


def _fills_query_result_to_df(result) -> pd.DataFrame:
    if not result.result_rows:
        return _empty_fills_df()
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    df["trader"] = df["trader"].map(_decode_trader)
    df["closed_pnl"] = pd.to_numeric(df["closed_pnl"], errors="coerce").fillna(0.0)
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df["start_pos"] = pd.to_numeric(df["start_pos"], errors="coerce").fillna(0.0)
    df["sz"] = pd.to_numeric(df["sz"], errors="coerce").fillna(0.0)
    df["coin"] = df["coin"].astype(str)
    df["dir"] = df["dir"].astype(str)
    df = df.dropna(subset=["time"])
    df["date"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    return df


def fetch_distinct_coins(client) -> list[str]:
    """从 user_fill_v2 读取白名单地址涉及的全部 coin（排除 xyz: 前缀）。"""
    sql = f"""
    SELECT DISTINCT coin
    FROM {FILLS_TABLE}
    WHERE lower(toString(trader)) != %(invalid)s
      AND lower(toString(trader)) IN (
          SELECT lower(trader) FROM {ACTIVE_ADDRESSES_TABLE}
      )
      {_fills_time_filter_sql()}
    ORDER BY coin
    """
    result = _ch_query(client, "distinct coins", sql, parameters={"invalid": INVALID_TRADER})
    coins = [str(row[0]) for row in result.result_rows]
    return [c for c in coins if c and not c.startswith(COIN_SKIP_PREFIX)]


def print_address_whitelist_stats(client, coins: list[str]) -> None:
    """统计白名单过滤：fills 中有但不在 active 的地址；active 中但无 fills 的地址。"""
    if not coins:
        print(f"\n--- 地址白名单过滤统计: 无 coin ---", flush=True)
        return

    coins_sql = ", ".join(f"'{c.replace(chr(39), '')}'" for c in coins)
    params = {"invalid": INVALID_TRADER}
    time_tail = _fills_time_filter_sql()

    print(f"\n--- 地址白名单过滤统计 ({FILLS_TABLE} × {ACTIVE_ADDRESSES_TABLE}) ---", flush=True)

    for coin in coins:
        filtered = int(
            _ch_query(
                client,
                f"whitelist/{coin}/out",
                f"""
                SELECT count(DISTINCT lower(toString(trader)))
                FROM {FILLS_TABLE}
                WHERE coin = %(coin)s
                  AND lower(toString(trader)) != %(invalid)s
                  AND lower(toString(trader)) NOT IN (
                      SELECT lower(trader) FROM {ACTIVE_ADDRESSES_TABLE}
                  )
                  {time_tail}
                """,
                parameters={**params, "coin": coin},
            ).result_rows[0][0]
        )
        no_fills = int(
            _ch_query(
                client,
                f"whitelist/{coin}/idle",
                f"""
                SELECT count()
                FROM {ACTIVE_ADDRESSES_TABLE} AS a
                WHERE lower(a.trader) NOT IN (
                    SELECT DISTINCT lower(toString(trader))
                    FROM {FILLS_TABLE}
                    WHERE coin = %(coin)s
                      AND lower(toString(trader)) != %(invalid)s
                      {time_tail}
                )
                """,
                parameters={**params, "coin": coin},
            ).result_rows[0][0]
        )
        print(
            f"  {coin}: 已过滤 {filtered:,} 个地址（fills 有、不在 active）"
            f" | 白名单无成交 {no_fills:,} 个",
            flush=True,
        )

    filtered_all = int(
        _ch_query(
            client,
            "whitelist/out_total",
            f"""
            SELECT count(DISTINCT lower(toString(trader)))
            FROM {FILLS_TABLE}
            WHERE coin IN ({coins_sql})
              AND lower(toString(trader)) != %(invalid)s
              AND lower(toString(trader)) NOT IN (
                  SELECT lower(trader) FROM {ACTIVE_ADDRESSES_TABLE}
              )
              {time_tail}
            """,
            parameters=params,
        ).result_rows[0][0]
    )
    no_fills_all = int(
        _ch_query(
            client,
            "whitelist/idle_total",
            f"""
            SELECT count()
            FROM {ACTIVE_ADDRESSES_TABLE} AS a
            WHERE lower(a.trader) NOT IN (
                SELECT DISTINCT lower(toString(trader))
                FROM {FILLS_TABLE}
                WHERE coin IN ({coins_sql})
                  AND lower(toString(trader)) != %(invalid)s
                  {time_tail}
            )
            """,
            parameters=params,
        ).result_rows[0][0]
    )
    print(
        f"  合计({len(coins)} coins): 已过滤 {filtered_all:,} 个地址（去重）"
        f" | 白名单无成交 {no_fills_all:,} 个",
        flush=True,
    )
    print(flush=True)


def fetch_scorable_trader_count(client, coin: str) -> int:
    """阶段一：统计 long|short 笔数在 [MIN_TRADES, MAX_TRADES) 的白名单地址数。"""
    sql = f"SELECT count() FROM ({_scorable_traders_subquery()}) AS scorable"
    result = _ch_query(
        client,
        f"{coin}/scorable_count",
        sql,
        parameters={"coin": coin, "invalid": INVALID_TRADER},
    )
    return int(result.result_rows[0][0])


def fetch_fills_by_coin(client, coin: str) -> pd.DataFrame:
    """两阶段读取某 coin 成交：先筛可打分地址，再拉明细（无全局 ORDER BY）。"""
    scorable_n = fetch_scorable_trader_count(client, coin)
    print(
        f"  可打分地址 ({MIN_TRADES}<=N<{MAX_TRADES}): {scorable_n:,}",
        flush=True,
    )
    if scorable_n == 0:
        return _empty_fills_df()

    start_col = _pick_col(client, _START_POS_CANDIDATES, "start_pos")
    pnl_col = _pick_col(client, _CLOSED_PNL_CANDIDATES, "closed_pnl")
    sql = f"""
    SELECT
        lower(toString(f.trader)) AS trader,
        f.coin,
        f.dir,
        f.time,
        {_sql_float_expr(pnl_col, "closed_pnl", "f")},
        {_sql_float_expr(start_col, "start_pos", "f")},
        toFloat64(f.sz) AS sz
    FROM {FILLS_TABLE} AS f
    INNER JOIN ({_scorable_traders_subquery()}) AS e
        ON lower(toString(f.trader)) = e.trader
    WHERE f.coin = %(coin)s
      AND lower(toString(f.trader)) != %(invalid)s
      {_fills_time_filter_sql('f')}
    """
    result = _ch_query(
        client,
        f"{coin}/fills",
        sql,
        parameters={"coin": coin, "invalid": INVALID_TRADER},
    )
    return _fills_query_result_to_df(result)


def score_coin_verbose(df_coin: pd.DataFrame) -> dict | None:
    """与 score_fills_by_symbol.score_coin_trades 相同规则，额外返回 R²/mean/std。"""
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
        "r_sq": r_sq,
        "mean_pnl": mean_pnl,
        "std_pnl": std_pnl if not np.isnan(std_pnl) else 0.0,
        "score": score,
        "total_pnl": float(df["closed_pnl"].sum()),
        "status": classify_score(score),
    }


def score_traders_for_coin(df: pd.DataFrame, coin: str) -> pd.DataFrame:
    rows: list[dict] = []
    for trader, grp in df.groupby("trader", sort=False):
        row = score_coin_verbose(grp)
        if row is None:
            continue
        row["address"] = _decode_trader(trader)
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _norm_addr(addr: str) -> str:
    return str(addr).strip().lower()


def fetch_overall_pnl_by_address(client, _pnl_col: str = "") -> dict[str, float]:
    """白名单地址在 hyper_portfolio.pnl_history 中时间戳最大项的 pnl。"""
    sql = f"""
    SELECT
        lower(toString(trader)) AS addr,
        toFloat64(argMax(tupleElement(x, 2), tupleElement(x, 1))) AS pnl
    FROM {HYPER_PORTFOLIO_TABLE}
    ARRAY JOIN pnl_history AS x
    WHERE lower(toString(trader)) IN (
        SELECT lower(trader) FROM {ACTIVE_ADDRESSES_TABLE}
    )
    GROUP BY addr
    """
    result = _ch_query(client, "hyper_portfolio/pnl", sql)
    return {str(row[0]): float(row[1]) for row in result.result_rows}


def passes_total_pnl_gate(status: str, total_pnl: float) -> bool:
    """单 coin：高手须该 coin 盈利，新手须该 coin 亏损。"""
    if status == "高手":
        return total_pnl > 0
    if status == "新手":
        return total_pnl < 0
    return False


def row_eligible_for_output(status: str, coin_pnl: float) -> bool:
    return passes_total_pnl_gate(status, coin_pnl)


def _classify_addrs_by_pos(
    addrs: list[str], pos_map: dict[str, float]
) -> tuple[list[str], list[str], list[str]]:
    long_, short_, flat_ = [], [], []
    for a in addrs:
        ep = pos_map.get(_norm_addr(a), 0.0)
        if ep > 0:
            long_.append(_norm_addr(a))
        elif ep < 0:
            short_.append(_norm_addr(a))
        else:
            flat_.append(_norm_addr(a))
    return long_, short_, flat_


def _pack_long_short_row(
    coin: str,
    gs_long: list[str],
    gs_short: list[str],
    gs_flat: list[str],
    xs_long: list[str],
    xs_short: list[str],
    xs_flat: list[str],
    ts: datetime,
) -> dict[str, Any] | None:
    gs_total = len(gs_long) + len(gs_short) + len(gs_flat)
    xs_total = len(xs_long) + len(xs_short) + len(xs_flat)
    if gs_total + xs_total == 0:
        return None
    gs_lr, gs_sr = _ratio(len(gs_long), len(gs_long) + len(gs_short))
    xs_lr, xs_sr = _ratio(len(xs_long), len(xs_long) + len(xs_short))
    return {
        "coin": coin,
        "gaoshou_count": gs_total,
        "gaoshou_longCount": len(gs_long),
        "gaoshou_shortCount": len(gs_short),
        "gaoshou_longRatio": gs_lr,
        "gaoshou_shortRatio": gs_sr,
        "gaoshou_addresses": gs_long + gs_short + gs_flat,
        "xinshou_count": xs_total,
        "xinshou_longCount": len(xs_long),
        "xinshou_shortCount": len(xs_short),
        "xinshou_longRatio": xs_lr,
        "xinshou_shortRatio": xs_sr,
        "xinshou_addresses": xs_short + xs_long + xs_flat,
        "timestamp": ts,
    }


def filter_rows_by_overall_pnl(
    rows: list[dict[str, Any]],
    overall_pnl: dict[str, float],
    pos_by_coin: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    """写表前：整体盈利地址只留高手池，整体亏损地址只留新手池。"""
    if not overall_pnl:
        return rows
    out: list[dict[str, Any]] = []
    drop_gs = drop_xs = 0
    for row in rows:
        coin = row["coin"]
        pos_map = pos_by_coin.get(coin, {})
        gs = [
            _norm_addr(a)
            for a in row["gaoshou_addresses"]
            if overall_pnl.get(_norm_addr(a), 0.0) > 0
        ]
        xs = [
            _norm_addr(a)
            for a in row["xinshou_addresses"]
            if overall_pnl.get(_norm_addr(a), 0.0) < 0
        ]
        drop_gs += len(row["gaoshou_addresses"]) - len(gs)
        drop_xs += len(row["xinshou_addresses"]) - len(xs)
        gl, gsh, gf = _classify_addrs_by_pos(gs, pos_map)
        xl, xsh, xf = _classify_addrs_by_pos(xs, pos_map)
        packed = _pack_long_short_row(coin, gl, gsh, gf, xl, xsh, xf, row["timestamp"])
        if packed:
            out.append(packed)
    print(
        f"\n[filter] 写表前整体 pnl 过滤: 从高手池剔除 {drop_gs:,} 地址，"
        f"从新手池剔除 {drop_xs:,} 地址",
        flush=True,
    )
    return out


def filter_eligible_by_overall_pnl(
    df: pd.DataFrame, overall_pnl: dict[str, float]
) -> pd.DataFrame:
    """CSV 导出前：整体盈利只保留高手行，整体亏损只保留新手行。"""
    if df.empty or not overall_pnl:
        return df

    def _ok(r: pd.Series) -> bool:
        pnl = overall_pnl.get(_norm_addr(str(r["address"])), 0.0)
        if pnl > 0:
            return str(r["status"]) == "高手"
        if pnl < 0:
            return str(r["status"]) == "新手"
        return False

    out = df.loc[df.apply(_ok, axis=1)].copy()
    print(
        f"[filter] CSV 整体 pnl 过滤: {len(df):,} → {len(out):,} 行",
        flush=True,
    )
    return out


def end_pos_from_row(r: pd.Series) -> float:
    d = str(r["dir"])
    sp = float(r["start_pos"])
    sz = float(r["sz"])
    if d == "Open Long":
        return sp + sz
    if d == "Close Long":
        return sp - sz
    if d == "Open Short":
        return sp - sz
    if d == "Close Short":
        return sp + sz
    if d == "Long > Short":
        return sp - sz
    if d == "Short > Long":
        return sp + sz
    return sp


def latest_end_pos(df: pd.DataFrame) -> dict[str, float]:
    """trader -> 该 coin 最新一笔 fill 推算的 end_pos（地址小写）。"""
    out: dict[str, float] = {}
    for trader, grp in df.groupby("trader", sort=False):
        last = grp.sort_values("time").iloc[-1]
        out[_norm_addr(_decode_trader(trader))] = end_pos_from_row(last)
    return out


def _ratio(long_count: int, total: int) -> tuple[int, int]:
    if total <= 0:
        return 0, 0
    lr = int(long_count / total * 100)
    return lr, 100 - lr


def build_long_short_rows(
    scored_by_coin: dict[str, pd.DataFrame],
    pos_by_coin: dict[str, dict[str, float]],
    ts: datetime,
    *,
    balance_ok: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for coin in sorted(scored_by_coin.keys()):
        scored = scored_by_coin.get(coin, pd.DataFrame())
        pos_map = pos_by_coin.get(coin, {})

        gs_long: list[str] = []
        gs_short: list[str] = []
        gs_flat: list[str] = []
        xs_long: list[str] = []
        xs_short: list[str] = []
        xs_flat: list[str] = []

        if not scored.empty:
            selected = scored[scored["status"].isin(("高手", "新手"))]
            for _, r in selected.iterrows():
                addr = _norm_addr(str(r["address"]))
                if not row_eligible_for_output(str(r["status"]), float(r["total_pnl"])):
                    continue
                if balance_ok is not None and addr not in balance_ok:
                    continue
                ep = pos_map.get(addr, 0.0)
                if r["status"] == "高手":
                    if ep > 0:
                        gs_long.append(addr)
                    elif ep < 0:
                        gs_short.append(addr)
                    else:
                        gs_flat.append(addr)
                else:
                    if ep > 0:
                        xs_long.append(addr)
                    elif ep < 0:
                        xs_short.append(addr)
                    else:
                        xs_flat.append(addr)

        packed = _pack_long_short_row(
            coin, gs_long, gs_short, gs_flat, xs_long, xs_short, xs_flat, ts
        )
        if packed:
            rows.append(packed)
    return rows


def collect_eligible_scored(scored_by_coin: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """收集写入 long_short_ratio / CSV 的高手/新手行（单 coin 门槛）。"""
    parts: list[pd.DataFrame] = []
    for _coin, scored in scored_by_coin.items():
        if scored.empty:
            continue
        sel = scored[scored["status"].isin(("高手", "新手"))].copy()
        mask = sel.apply(
            lambda r: row_eligible_for_output(str(r["status"]), float(r["total_pnl"])),
            axis=1,
        )
        eligible = sel.loc[mask, ["address", "status", "coin", "total_pnl", "N", "score"]]
        if not eligible.empty:
            parts.append(eligible)

    if not parts:
        return pd.DataFrame(
            columns=["address", "status", "coin", "total_pnl", "N", "score", "balance"]
        )
    return pd.concat(parts, ignore_index=True)


async def attach_balances_for_export(df: pd.DataFrame) -> pd.DataFrame:
    """为导出结果批量查询 ScopeFi balance。"""
    work = df.copy()
    if work.empty:
        work["balance"] = pd.NA
        return work

    _ensure_scopefi_path()
    from query_wallet_balance import (  # noqa: E402
        _chunked,
        fetch_balances_map,
        load_dotenv,
        normalize_wallet,
        ScopeFiSettings,
    )

    env_path = load_dotenv()
    if env_path:
        print(f"[env] 已加载 {env_path.name}", flush=True)

    seen: set[str] = set()
    addresses: list[str] = []
    for raw in work["address"]:
        try:
            w = normalize_wallet(str(raw))
        except ValueError:
            continue
        key = w.lower()
        if key not in seen:
            seen.add(key)
            addresses.append(w)

    if not addresses:
        work["balance"] = pd.NA
        return work

    batches = _chunked(addresses, BALANCE_BATCH_SIZE)
    settings = ScopeFiSettings.from_env()
    print(f"\n批量查询 ScopeFi balance（导出 CSV，共 {len(addresses):,} 地址）")
    print(f"  API: {settings.position_url}", flush=True)

    balance_by_user: dict[str, float | None] = {}
    for i, batch in enumerate(batches, start=1):
        print(f"[balance batch {i}/{len(batches)}] 查询 {len(batch)} 个地址 ...", flush=True)
        fetched = await fetch_balances_map(batch)
        balance_by_user.update(fetched)
        if i < len(batches) and BALANCE_BATCH_INTERVAL_SEC > 0:
            await asyncio.sleep(BALANCE_BATCH_INTERVAL_SEC)

    def _lookup_balance(raw: object) -> float | None:
        try:
            key = normalize_wallet(str(raw)).lower()
        except ValueError:
            return None
        return balance_by_user.get(key)

    work["balance"] = work["address"].map(_lookup_balance)
    queried = work["balance"].notna().sum()
    print(f"[balance] 已查询 {queried:,}/{len(work):,} 行有 balance 值", flush=True)
    return work


def filter_nonzero_balance(df: pd.DataFrame) -> pd.DataFrame:
    """剔除 balance 为空或 0 的行。"""
    if df.empty or "balance" not in df.columns:
        return df
    bal = pd.to_numeric(df["balance"], errors="coerce")
    return df.loc[bal.notna() & (bal != 0)].copy()


def balance_ok_addresses(df: pd.DataFrame) -> frozenset[str]:
    """balance 非空且非 0 的地址集合（小写）。"""
    if df.empty:
        return frozenset()
    bal = pd.to_numeric(df["balance"], errors="coerce")
    ok = df.loc[bal.notna() & (bal != 0), "address"].astype(str).map(_norm_addr)
    return frozenset(ok.unique())


def _warn_pool_address_overlap(rows: list[dict[str, Any]]) -> None:
    gs_all: set[str] = set()
    xs_all: set[str] = set()
    for row in rows:
        gs_all.update(_norm_addr(a) for a in row.get("gaoshou_addresses") or [])
        xs_all.update(_norm_addr(a) for a in row.get("xinshou_addresses") or [])
    overlap = gs_all & xs_all
    if overlap:
        sample = ", ".join(sorted(overlap)[:5])
        suffix = " ..." if len(overlap) > 5 else ""
        print(
            f"\n[warn] 高手/新手池地址重叠 {len(overlap)} 个: {sample}{suffix}",
            flush=True,
        )


def save_scored_addresses_csv(df: pd.DataFrame) -> int:
    """将最终高手/新手地址写入 CSV。"""
    if df.empty:
        print("\n[csv] 无符合条件的高手/新手，跳过 CSV 导出")
        return 0

    export = df.rename(
        columns={"total_pnl": "总盈亏", "N": "交易笔数", "score": "得分"}
    )
    cols = ["address", "status", "coin", "balance", "总盈亏", "交易笔数", "得分"]
    export = export[cols].sort_values(
        ["coin", "status", "得分"],
        ascending=[True, True, False],
        kind="mergesort",
    )
    OUTPUT_SCORED_CSV.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig 带 BOM，Excel 在 Windows 下才能正确识别中文（避免「新手」→「鏂版墜」）
    export.to_csv(OUTPUT_SCORED_CSV, index=False, encoding="utf-8-sig")
    gs = int((export["status"] == "高手").sum())
    xs = int((export["status"] == "新手").sum())
    print(f"\n[ok] 已写入 CSV: {OUTPUT_SCORED_CSV} ({len(export):,} 行，高手 {gs:,} / 新手 {xs:,})")
    return len(export)


def collect_all_scored(scored_by_coin: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts = [df for df in scored_by_coin.values() if not df.empty]
    if not parts:
        return pd.DataFrame()
    all_scored = pd.concat(parts, ignore_index=True)
    return all_scored.sort_values("score", ascending=False, kind="mergesort")


def print_preview_samples(all_scored: pd.DataFrame) -> None:
    """打印 3 条样例：各规则分量 + 最终 score。"""
    print(f"\n{'=' * 88}")
    print(f"打分预览（规则: {MIN_TRADES}<=N<{MAX_TRADES}, 高手 score>{SCORE_GAOSHOU}, 新手 score<{SCORE_XINSHOU}）")
    print("写入表附加: 高手 total_pnl>0 | 新手 total_pnl<0 | balance 非空且 != 0")
    print(f"{'=' * 88}")

    if all_scored.empty:
        print("(无满足 MIN/MAX_TRADES 的打分结果)")
        return

    selected = all_scored[all_scored["status"].isin(("高手", "新手"))]
    pool = selected if not selected.empty else all_scored
    show = pool.head(PREVIEW_COUNT)

    for i, (_, r) in enumerate(show.iterrows(), start=1):
        print(f"\n--- 样例 [{i}] ---")
        print(f"  address:   {r['address']}")
        print(f"  coin:      {r['coin']}")
        print(f"  N:         {int(r['N'])}  ({MIN_TRADES}<=N<{MAX_TRADES})")
        print(f"  R²:        {float(r['r_sq']):.6f}")
        print(f"  mean_pnl:  {float(r['mean_pnl']):.6f}")
        print(f"  std_pnl:   {float(r['std_pnl']):.6f}")
        print(
            f"  score:     {float(r['score']):.6f}  "
            f"(= R² × mean_pnl/std_pnl × sqrt(N))"
        )
        print(f"  total_pnl: {float(r['total_pnl']):.4f}")
        print(f"  status:    {r['status']}")

    gs = int((all_scored["status"] == "高手").sum())
    xs = int((all_scored["status"] == "新手").sum())
    other = int((all_scored["status"] == "其他").sum())
    gs_ok = int(
        ((all_scored["status"] == "高手") & (all_scored["total_pnl"] > 0)).sum()
    )
    xs_ok = int(
        ((all_scored["status"] == "新手") & (all_scored["total_pnl"] < 0)).sum()
    )
    print(f"\n汇总: 高手 {gs:,} 条 | 新手 {xs:,} 条 | 其他 {other:,} 条 | 合计 {len(all_scored):,} 条")
    print(f"写入表: 高手 {gs_ok:,} 条 (total_pnl>0) | 新手 {xs_ok:,} 条 (total_pnl<0)")


def print_long_short_summary(rows: list[dict[str, Any]]) -> None:
    print(f"\n--- 待写入 {TARGET_TABLE} ---")
    for r in rows:
        print(
            f"  {r['coin']}: 高手 {r['gaoshou_count']} (多{r['gaoshou_longCount']}/空{r['gaoshou_shortCount']}) "
            f"新手 {r['xinshou_count']} (多{r['xinshou_longCount']}/空{r['xinshou_shortCount']}) "
            f"ts={r['timestamp']}"
        )


def print_coin_stats_ranked(rows: list[dict[str, Any]]) -> None:
    """按 coin 高手+新手总数从多到少排序展示。"""
    if not rows:
        print("\n--- 各 coin 统计（按地址总数排序）---")
        print("(无数据)")
        return

    ranked = sorted(
        rows,
        key=lambda r: int(r["gaoshou_count"]) + int(r["xinshou_count"]),
        reverse=True,
    )
    print(f"\n--- 各 coin 统计（按高手+新手地址总数降序，共 {len(ranked)} 个 coin）---")
    for r in ranked:
        gs = int(r["gaoshou_count"])
        xs = int(r["xinshou_count"])
        print(
            f"  {r['coin']}: 高手 {gs} (多{r['gaoshou_longCount']}/空{r['gaoshou_shortCount']}) "
            f"新手 {xs} (多{r['xinshou_longCount']}/空{r['xinshou_shortCount']})，"
            f"总共地址数({gs}+{xs})"
        )


def insert_long_short_ratio_rows(client, rows: list[dict[str, Any]]) -> int:
    """写入 long_short_ratio；显式列类型，不 DESCRIBE 目标表。"""
    if not rows:
        return 0
    fq_table = _target_table_fq()
    data = [[r[col] for col in LONG_SHORT_RATIO_INSERT_COLUMNS] for r in rows]
    client.insert(
        fq_table,
        data,
        column_names=LONG_SHORT_RATIO_INSERT_COLUMNS,
        column_type_names=LONG_SHORT_RATIO_COLUMN_TYPES,
    )
    return len(rows)


def save_rows(client, rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("\n[skip] 无数据可写入")
        return
    n = insert_long_short_ratio_rows(client, rows)
    print(f"\n[ok] 已插入 {n} 条 → {CLICKHOUSE_DATABASE}.{TARGET_TABLE}")


def run(*, dry_run: bool = True, inspect_target: bool | None = None) -> int:
    ts = datetime.now().replace(minute=0, second=0, microsecond=0)

    print(
        f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}",
        flush=True,
    )
    print(
        f"lookback_days={LOOKBACK_DAYS or '全部'}  "
        f"MIN_TRADES={MIN_TRADES}  MAX_TRADES={MAX_TRADES}",
        flush=True,
    )

    client = get_client()
    if SCORE_COINS:
        coins = [c.strip().upper() for c in SCORE_COINS if c and str(c).strip()]
        print(f"待统计 coin 数: {len(coins):,}（指定 SCORE_COINS）", flush=True)
        print(f"  coins: {', '.join(coins)}", flush=True)
    else:
        coins = fetch_distinct_coins(client)
        print(f"待统计 coin 数: {len(coins):,}", flush=True)
        if coins:
            preview = ", ".join(coins[:20])
            suffix = " ..." if len(coins) > 20 else ""
            print(f"  coins: {preview}{suffix}", flush=True)
    active_n = int(
        _ch_query(
            client,
            f"count {ACTIVE_ADDRESSES_TABLE}",
            f"SELECT count() FROM {ACTIVE_ADDRESSES_TABLE}",
        ).result_rows[0][0]
    )
    print(f"地址白名单 {ACTIVE_ADDRESSES_TABLE}: {active_n:,} 个", flush=True)
    do_inspect = _should_inspect_target(inspect_target)
    if do_inspect:
        inspect_target_table(client)
    else:
        print(
            f"\n[skip] 跳过目标表探查 {_target_table_fq()} "
            f"(inspect_target={inspect_target}, db={CLICKHOUSE_DATABASE})",
            flush=True,
        )
    cols = _table_columns(client)
    start_col = _pick_col(client, _START_POS_CANDIDATES, "start_pos")
    pnl_col = _pick_col(client, _CLOSED_PNL_CANDIDATES, "closed_pnl")
    overall_pnl: dict[str, float] = {}
    if ADDRESS_OVERALL_SIDE_FILTER:
        overall_pnl = fetch_overall_pnl_by_address(client, pnl_col)
        n_profit = sum(1 for v in overall_pnl.values() if v > 0)
        n_loss = sum(1 for v in overall_pnl.values() if v < 0)
        print(
            f"\n[filter] hyper_portfolio 最新 pnl: 盈利 {n_profit:,} / 亏损 {n_loss:,} "
            f"（写表前再按整体 pnl 过滤池子）",
            flush=True,
        )
    print(
        f"user_fill_v2 列映射: start_pos<-{start_col}  closed_pnl<-{pnl_col}  "
        f"(共 {len(cols)} 列)",
        flush=True,
    )
    print_address_whitelist_stats(client, coins)
    scored_by_coin: dict[str, pd.DataFrame] = {}
    pos_by_coin: dict[str, dict[str, float]] = {}

    for coin in coins:
        print(f"\n读取 {FILLS_TABLE} coin={coin} ...", flush=True)
        df = fetch_fills_by_coin(client, coin)
        print(f"  成交行数: {len(df):,}  地址数: {df['trader'].nunique() if not df.empty else 0:,}", flush=True)
        pos_by_coin[coin] = latest_end_pos(df) if not df.empty else {}
        scored = score_traders_for_coin(df, coin)
        scored_by_coin[coin] = scored
        if not scored.empty:
            gs = int((scored["status"] == "高手").sum())
            xs = int((scored["status"] == "新手").sum())
            print(f"  打分结果: {len(scored):,} 条 (高手 {gs:,} / 新手 {xs:,})", flush=True)

    all_scored = collect_all_scored(scored_by_coin)
    print_preview_samples(all_scored)

    eligible = collect_eligible_scored(scored_by_coin)
    eligible = asyncio.run(attach_balances_for_export(eligible))
    n_before_bal = len(eligible)
    eligible = filter_nonzero_balance(eligible)
    dropped_bal = n_before_bal - len(eligible)
    if n_before_bal:
        print(
            f"\n[balance] 过滤空/0: 剔除 {dropped_bal:,} 行，"
            f"保留 {len(eligible):,} 行（{len(balance_ok_addresses(eligible)):,} 个地址）",
            flush=True,
        )

    if overall_pnl:
        eligible = filter_eligible_by_overall_pnl(eligible, overall_pnl)

    ok_addrs = balance_ok_addresses(eligible)
    rows = build_long_short_rows(
        scored_by_coin,
        pos_by_coin,
        ts,
        balance_ok=ok_addrs,
    )
    if overall_pnl:
        rows = filter_rows_by_overall_pnl(rows, overall_pnl, pos_by_coin)
    _warn_pool_address_overlap(rows)
    print_long_short_summary(rows)

    save_scored_addresses_csv(eligible)
    print_coin_stats_ranked(rows)

    if dry_run:
        print("\n[dry-run] 未写入 ClickHouse")
        return 0

    save_rows(client, rows)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="user_fill_v2 按 coin 打分 → long_short_ratio")
    p.add_argument(
        "--write",
        action="store_true",
        help="写入 long_short_ratio 表（默认仅预览不写库）",
    )
    return p.parse_args()


def main() -> int:
    _configure_stdio()
    _ensure_scopefi_path()
    from query_wallet_balance import load_dotenv, log_error_exc  # noqa: E402

    env_path = load_dotenv()
    _bind_clickhouse_config(globals())
    if env_path:
        print(f"[env] 已加载 {env_path.name}", flush=True)
    args = parse_args()
    try:
        return run(dry_run=not args.write)
    except Exception:
        log_error_exc()
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n已取消", file=sys.stderr)
        raise SystemExit(130) from None
