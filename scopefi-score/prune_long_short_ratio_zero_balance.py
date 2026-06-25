#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""读 long_short_ratio 各 coin 最新行，查 ScopeFi 资金，剔除 balance=0 地址。

默认写入剔除后的新快照；加 --dry-run 仅预览。
ScopeFi 查资金同 ch_filter_active_addresses.py（query_wallet_balance + .env）。

用法:
    python prune_long_short_ratio_zero_balance.py           # 写入 pro.long_short_ratio
    python prune_long_short_ratio_zero_balance.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent


def _scopefi_dir() -> Path:
    for p in (SCRIPT_DIR.parent, SCRIPT_DIR.parent / "scopefi", SCRIPT_DIR):
        if (p / "query_wallet_balance.py").is_file():
            return p
    return SCRIPT_DIR.parent


SCOPEFI_DIR = _scopefi_dir()
if str(SCOPEFI_DIR) not in sys.path:
    sys.path.insert(0, str(SCOPEFI_DIR))

from query_wallet_balance import (  # noqa: E402
    _chunked,
    bind_clickhouse_config,
    fetch_balances_map,
    get_clickhouse_client,
    load_dotenv,
    normalize_wallet,
    ScopeFiSettings,
)

# ClickHouse 见 scopefi/.env（main 中 bind_clickhouse_config 注入 CLICKHOUSE_*）
CLICKHOUSE_HOST = ""
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = ""
CLICKHOUSE_PASSWORD = ""
CLICKHOUSE_DATABASE = "pro"
TARGET_TABLE = "long_short_ratio"

# ScopeFi 批量查资金（同 ch_filter_active_addresses.py）
BALANCE_BATCH_SIZE = 1000
BALANCE_BATCH_INTERVAL_SEC = 1.0

INVALID_ADDRESSES = frozenset({"0x0000000000000000000000000000000000000000"})

INSERT_COLS = (
    "coin", "gaoshou_count", "gaoshou_longCount", "gaoshou_shortCount",
    "gaoshou_longRatio", "gaoshou_shortRatio", "gaoshou_addresses",
    "xinshou_count", "xinshou_longCount", "xinshou_shortCount",
    "xinshou_longRatio", "xinshou_shortRatio", "xinshou_addresses", "timestamp",
)
INSERT_TYPES = (
    "String", "UInt32", "UInt32", "UInt32", "UInt32", "UInt32", "Array(String)",
    "UInt32", "UInt32", "UInt32", "UInt32", "UInt32", "Array(String)", "DateTime64(3)",
)


def _configure_stdio() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _coerce_address(raw: object) -> str:
    """ClickHouse Array(String) 可能返回 bytes，需 decode 后再校验。"""
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw).decode("utf-8").strip()
    return str(raw).strip()


def is_valid_trader_address(addr: object) -> bool:
    s = _coerce_address(addr)
    if len(s) != 42 or not s.startswith("0x"):
        return False
    lower = s.lower()
    if lower in INVALID_ADDRESSES:
        return False
    hex_part = lower[2:]
    if len(hex_part) != 40 or not all(c in "0123456789abcdef" for c in hex_part):
        return False
    return int(hex_part, 16) != 0


def _norm(raw: object) -> str:
    return _coerce_address(raw).lower()


def _ratio(n_long: int, total: int) -> tuple[int, int]:
    if total <= 0:
        return 0, 0
    lr = int(n_long / total * 100)
    return lr, 100 - lr


def _pack_row(
    coin: str, gl: list[str], gsh: list[str], gf: list[str],
    xl: list[str], xsh: list[str], xf: list[str], ts: datetime,
) -> dict[str, Any] | None:
    gs = len(gl) + len(gsh) + len(gf)
    xs = len(xl) + len(xsh) + len(xf)
    if gs + xs == 0:
        return None
    glr, gsr = _ratio(len(gl), len(gl) + len(gsh))
    xlr, xsr = _ratio(len(xl), len(xl) + len(xsh))
    return {
        "coin": coin,
        "gaoshou_count": gs, "gaoshou_longCount": len(gl), "gaoshou_shortCount": len(gsh),
        "gaoshou_longRatio": glr, "gaoshou_shortRatio": gsr,
        "gaoshou_addresses": gl + gsh + gf,
        "xinshou_count": xs, "xinshou_longCount": len(xl), "xinshou_shortCount": len(xsh),
        "xinshou_longRatio": xlr, "xinshou_shortRatio": xsr,
        "xinshou_addresses": xsh + xl + xf,
        "timestamp": ts,
    }


def _split_pools(row: dict[str, Any]) -> tuple[list[str], ...]:
    gs = [_norm(a) for a in row["gaoshou_addresses"]]
    xs = [_norm(a) for a in row["xinshou_addresses"]]
    ngl, ngs = int(row["gaoshou_longCount"]), int(row["gaoshou_shortCount"])
    gl, gsh, gf = gs[:ngl], gs[ngl : ngl + ngs], gs[ngl + ngs :]
    xsh, nxl = int(row["xinshou_shortCount"]), int(row["xinshou_longCount"])
    return gl, gsh, gf, xs[xsh : xsh + nxl], xs[:xsh], xs[xsh + nxl :]


def _client():
    return get_clickhouse_client()


def _collect_wallet_addresses(addrs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in addrs:
        if not is_valid_trader_address(raw):
            continue
        try:
            w = normalize_wallet(_coerce_address(raw))
        except ValueError:
            continue
        key = w.lower()
        if key not in seen:
            seen.add(key)
            out.append(w)
    return out


async def fetch_balances(addrs: list[str]) -> dict[str, float | None]:
    """批量查询 ScopeFi 账户净值（同 ch_filter_active_addresses.attach_balances）。"""
    addresses = _collect_wallet_addresses(addrs)
    if not addresses:
        return {}

    batches = _chunked(addresses, BALANCE_BATCH_SIZE)
    settings = ScopeFiSettings.from_env()
    total_batches = len(batches)

    print("\n批量查询 ScopeFi 资金")
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

    print(f"\n[balance] 共 {sum(1 for v in balance_by_user.values() if v is not None):,}/{len(balance_by_user):,} 个地址有 balance")
    return balance_by_user


def _lookup_balance(raw: object, balance_by_user: dict[str, float | None]) -> float | None:
    try:
        key = normalize_wallet(_coerce_address(raw)).lower()
    except ValueError:
        return None
    return balance_by_user.get(key)


def _insert(client, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    data = [[r[c] for c in INSERT_COLS] for r in rows]
    client.insert(
        f"{CLICKHOUSE_DATABASE}.{TARGET_TABLE}",
        data, column_names=INSERT_COLS, column_type_names=INSERT_TYPES,
    )
    return len(rows)


async def run(write: bool) -> int:
    client = _client()
    r = client.query(
        f"SELECT * FROM {TARGET_TABLE} ORDER BY version DESC, timestamp DESC LIMIT 1 BY coin"
    )
    rows = [dict(zip(r.column_names, row)) for row in r.result_rows]
    if not rows:
        print("[skip] 无数据")
        return 0

    addrs = [a for row in rows for a in row["gaoshou_addresses"] + row["xinshou_addresses"]]
    print(f"coin 数: {len(rows)}  地址数: {len(addrs)}")
    bal = await fetch_balances(addrs)
    ok = {k for k, v in bal.items() if v is not None and float(v) != 0}

    print("\n=== 地址与资金 ===")
    for row in rows:
        print(f"\n[{row['coin']}]")
        for pool, xs in (("高手", row["gaoshou_addresses"]), ("新手", row["xinshou_addresses"])):
            for a in xs:
                v = _lookup_balance(a, bal)
                print(f"  {pool} {_coerce_address(a)}  balance={'N/A' if v is None else f'{float(v):,.4f}'}")

    zero = sorted(k for k, v in bal.items() if v is None or float(v) == 0)
    print("\n=== 待删除 ===")
    for a in zero:
        v = bal.get(a)
        print(f"  {a}  balance={'N/A' if v is None else f'{float(v):,.4f}'}")
    print(f"共 {len(zero)} 个" if zero else "  (无)")

    ts = datetime.now().replace(minute=0, second=0, microsecond=0)
    new_rows = []
    for row in rows:
        gl, gsh, gf, xl, xsh, xf = _split_pools(row)
        packed = _pack_row(
            row["coin"],
            [a for a in gl if a in ok], [a for a in gsh if a in ok], [a for a in gf if a in ok],
            [a for a in xl if a in ok], [a for a in xsh if a in ok], [a for a in xf if a in ok],
            ts,
        )
        if packed:
            new_rows.append(packed)

    print(f"\n重建: {len(new_rows)} 行，剔除 {len(zero)} 地址")
    if not write:
        print("[dry-run] 未写入（去掉 --dry-run 则写入）")
        return 0
    if new_rows:
        print(f"[ok] 已插入 {_insert(client, new_rows)} 条")
    return 0


def main() -> int:
    _configure_stdio()
    env_path = load_dotenv()
    bind_clickhouse_config(globals())
    if env_path:
        print(f"[env] 已加载 {env_path.name}", flush=True)

    p = argparse.ArgumentParser(
        description="剔除 long_short_ratio 零资金地址并写入新快照",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="只预览，不写 ClickHouse（默认写入）",
    )
    try:
        return asyncio.run(run(write=not p.parse_args().dry_run))
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
