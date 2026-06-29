#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查询指定钱包 ScopeFi 账户资金量（单文件自包含）。

从脚本同目录 `.env` 读取 SCOPEFI_TRACK_BASE_URL、SCOPEFI_POSITION_PATH 等。

用法:
  python query_wallet_balance.py --wallet 0xb25fa75338b89966958d7d7fcc7132e9e5674993
  python query_wallet_balance.py --wallet 0xaaa... --wallet 0xbbb...
  python query_wallet_balance.py --json-out data/wallet_balance.json

  # 从 parquet 批量查资金（每批 1000，间隔 1s，默认先查 3 批）
  python query_wallet_balance.py --parquet active_perp_addresses.parquet
  python query_wallet_balance.py --parquet active_perp_addresses.parquet --max-batches 3

也可修改下方 CONFIG.WALLET 后直接: python query_wallet_balance.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# =============================================================================
# CONFIG — 无命令行参数时使用
# =============================================================================

WALLET: str = ""  # 例: "0xb25fa75338b89966958d7d7fcc7132e9e5674993"
JSON_OUT: str = ""  # 例: "data/wallet_balance.json"

DEFAULT_PARQUET = "active_perp_addresses.parquet"
DEFAULT_PARQUET_OUT = "active_perp_addresses_with_balance.parquet"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_BATCHES = 3
DEFAULT_BATCH_INTERVAL_SEC = 1.0
USER_COL = "user"

# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# import scopefi_log  # noqa: F401, E402

MAX_POSITION_USERS = 1000


def env_log(message: str) -> None:
    print(f"[env] {message}", flush=True)


def _configure_stdio() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def log_error_exc(prefix: str = "[error]") -> None:
    print(prefix, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)


def _http_error(status: int, body: str) -> RuntimeError:
    return RuntimeError(f"HTTP {status}: {body[:500]}")


def _env_blank(raw: str | None) -> bool:
    if raw is None:
        return True
    s = str(raw).strip().strip('"').strip("'")
    return not s or s.lower() in ("none", "null", "~")


def _env_str(key: str, *, default: str = "", required: bool = False) -> str:
    raw = os.environ.get(key)
    if _env_blank(raw):
        if required:
            raise ValueError(f"环境变量 {key} 未配置（请在同目录 .env 中设置）")
        return default
    return str(raw).strip().strip('"').strip("'")


def _env_float(key: str, *, default: float) -> float:
    raw = os.environ.get(key)
    if _env_blank(raw):
        return default
    return float(str(raw).strip())


def load_dotenv() -> Path | None:
    """加载脚本同目录 .env（不覆盖已有环境变量）。"""
    path = SCRIPT_DIR / ".env"
    if not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and not _env_blank(val):
            os.environ.setdefault(key, val)
    return path


def normalize_wallet(raw: str) -> str:
    s = raw.strip()
    if len(s) != 42 or not s.startswith("0x"):
        raise ValueError(f"钱包地址须为 42 字符 0x 开头: {s!r}")
    return s


def _env_int(key: str, *, default: int) -> int:
    raw = os.environ.get(key)
    if _env_blank(raw):
        return default
    return int(str(raw).strip())


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> ClickHouseSettings:
        return cls(
            host=_env_str("CLICKHOUSE_HOST", required=True),
            port=_env_int("CLICKHOUSE_PORT", default=8123),
            user=_env_str("CLICKHOUSE_USER", required=True),
            password=_env_str("CLICKHOUSE_PASSWORD", required=True),
            database=_env_str("CLICKHOUSE_DATABASE", default="pro"),
        )


def get_clickhouse_client(settings: ClickHouseSettings | None = None):
    import clickhouse_connect

    s = settings or ClickHouseSettings.from_env()
    return clickhouse_connect.get_client(
        host=s.host,
        port=s.port,
        username=s.user,
        password=s.password,
        database=s.database,
    )


def bind_clickhouse_config(ns: dict[str, Any]) -> ClickHouseSettings:
    """加载 scopefi/.env 并写入调用模块的 CLICKHOUSE_* 变量。"""
    load_dotenv()
    s = ClickHouseSettings.from_env()
    ns["CLICKHOUSE_HOST"] = s.host
    ns["CLICKHOUSE_PORT"] = s.port
    ns["CLICKHOUSE_USER"] = s.user
    ns["CLICKHOUSE_PASSWORD"] = s.password
    ns["CLICKHOUSE_DATABASE"] = s.database
    return s


@dataclass(frozen=True)
class ScopeFiSettings:
    base_url: str
    position_path: str
    timeout_sec: float
    api_key: str | None

    @classmethod
    def from_env(cls) -> ScopeFiSettings:
        api_key = _env_str("SCOPEFI_API_KEY") or None
        return cls(
            base_url=_env_str("SCOPEFI_TRACK_BASE_URL", required=True).rstrip("/"),
            position_path=_env_str(
                "SCOPEFI_POSITION_PATH", default="/api/track/user/position"
            ),
            timeout_sec=_env_float("SCOPEFI_POSITION_TIMEOUT_SEC", default=15.0),
            api_key=api_key,
        )

    @property
    def position_url(self) -> str:
        return f"{self.base_url}{self.position_path}"


@dataclass
class WalletBalance:
    user: str
    account_value: float
    withdrawable: float
    total_ntl_pos: float
    raw: dict[str, Any]


def parse_wallet_balance(data: dict[str, Any]) -> WalletBalance:
    margin = data.get("marginSummary") or {}
    cross = data.get("crossMarginSummary") or {}
    account_value = float(margin.get("accountValue", 0)) + float(
        cross.get("accountValue", 0)
    )
    total_ntl = float(margin.get("totalNtlPos", 0))
    withdrawable = float(data.get("withdrawable", 0) or 0)
    return WalletBalance(
        user=str(data.get("user", "")),
        account_value=account_value,
        withdrawable=withdrawable,
        total_ntl_pos=total_ntl,
        raw=data,
    )


def index_balances_by_user(records: list[dict[str, Any]]) -> dict[str, WalletBalance]:
    out: dict[str, WalletBalance] = {}
    for rec in records:
        parsed = parse_wallet_balance(rec)
        if parsed.user:
            out[parsed.user.lower()] = parsed
    return out


async def _post_json(
    url: str,
    *,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_sec: float,
) -> dict[str, Any]:
    """POST JSON；优先 httpx.AsyncClient，否则 sync Client 或 stdlib urllib。"""
    import urllib.error
    import urllib.request

    req_headers = dict(headers)
    req_headers.setdefault("Content-Type", "application/json")

    def _via_urllib() -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers=req_headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise _http_error(e.code, body) from e

    try:
        import httpx
    except ImportError:
        return await asyncio.to_thread(_via_urllib)

    if hasattr(httpx, "AsyncClient"):
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            r = await client.post(url, json=payload, headers=headers)
            if r.is_error:
                raise _http_error(r.status_code, r.text)
            return r.json()

    if hasattr(httpx, "Client"):
        def _via_httpx_sync() -> dict[str, Any]:
            with httpx.Client(timeout=timeout_sec) as client:
                r = client.post(url, json=payload, headers=headers)
                if r.is_error:
                    raise _http_error(r.status_code, r.text)
                return r.json()

        return await asyncio.to_thread(_via_httpx_sync)

    return await asyncio.to_thread(_via_urllib)


async def fetch_user_positions(
    users: list[str],
    settings: ScopeFiSettings | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not users:
        return [], {}
    if len(users) > MAX_POSITION_USERS:
        raise ValueError(f"单次请求 users 不能超过 {MAX_POSITION_USERS}")

    s = settings or ScopeFiSettings.from_env()
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if s.api_key:
        headers["Authorization"] = f"Bearer {s.api_key}"

    try:
        body = await _post_json(
            s.position_url,
            payload={"users": users},
            headers=headers,
            timeout_sec=s.timeout_sec,
        )

        code = body.get("code")
        if code != 0:
            raw = json.dumps(body, ensure_ascii=False)[:500]
            raise RuntimeError(
                f"ScopeFi API code={code} msg={body.get('msg', '')} raw={raw}"
            )

        data = body.get("data")
        if not isinstance(data, list):
            raise RuntimeError(
                f"ScopeFi API 响应 data 不是数组 raw={json.dumps(body, ensure_ascii=False)[:500]}"
            )
        return data, body
    except Exception as exc:
        from lark_notice import notify_rest_error  # noqa: E402

        notify_rest_error(exc, api_url=s.position_url)
        log_error_exc("[rest error]")
        raise


def build_balance_report(
    wallet: str,
    bal: WalletBalance | None,
    *,
    api_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if bal is None:
        out: dict[str, Any] = {"found": False, "wallet": wallet}
    else:
        out = {
            "found": True,
            "wallet": bal.user,
            "account_value": bal.account_value,
            "withdrawable": bal.withdrawable,
            "total_ntl_pos": bal.total_ntl_pos,
            "raw": bal.raw,
        }
    if api_response is not None:
        out["api_response"] = api_response
    return out


async def fetch_wallet_balance(wallet: str) -> dict[str, Any]:
    w = normalize_wallet(wallet)
    records, api_body = await fetch_user_positions([w])
    by_user = index_balances_by_user(records)
    bal = by_user.get(w.lower())
    if bal is None and records:
        bal = parse_wallet_balance(records[0])
    return build_balance_report(w, bal, api_response=api_body)


async def fetch_wallet_balances(wallets: list[str]) -> list[dict[str, Any]]:
    normalized = [normalize_wallet(w) for w in wallets]
    records, api_body = await fetch_user_positions(normalized)
    by_user = index_balances_by_user(records)
    return [
        build_balance_report(w, by_user.get(w.lower()), api_response=api_body)
        for w in normalized
    ]


async def fetch_balances_map(wallets: list[str]) -> dict[str, float | None]:
    """批量查询，返回 address(lower) -> account_value；未返回的为 None。"""
    if not wallets:
        return {}
    reports = await fetch_wallet_balances(wallets)
    out: dict[str, float | None] = {}
    for rep in reports:
        w = str(rep.get("wallet", "")).lower()
        if rep.get("found"):
            out[w] = float(rep.get("account_value") or 0.0)
        else:
            out[w] = None
    return out


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


async def batch_query_parquet(
    *,
    parquet_in: Path,
    parquet_out: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: int | None = DEFAULT_MAX_BATCHES,
    interval_sec: float = DEFAULT_BATCH_INTERVAL_SEC,
) -> int:
    import pandas as pd

    if not parquet_in.is_file():
        raise FileNotFoundError(f"未找到 parquet 文件: {parquet_in}")

    df = pd.read_parquet(parquet_in)
    if USER_COL not in df.columns:
        raise ValueError(f"parquet 缺少 {USER_COL!r} 列，现有: {df.columns.tolist()}")

    addresses = [normalize_wallet(str(a)) for a in df[USER_COL].tolist()]
    batches = _chunked(addresses, batch_size)
    if max_batches is not None:
        batches = batches[:max_batches]

    total_batches = len(batches)
    total_addrs = sum(len(b) for b in batches)
    settings = ScopeFiSettings.from_env()

    print("batch_query_parquet")
    print(f"  输入: {parquet_in}")
    print(f"  输出: {parquet_out}")
    print(f"  API: {settings.position_url}")
    print(f"  总行数: {len(df):,}  本次查询: {total_addrs:,} 地址 / {total_batches} 批")
    print(f"  每批: {batch_size}  间隔: {interval_sec}s")

    balance_by_user: dict[str, float | None] = {}
    for i, batch in enumerate(batches, start=1):
        print(f"\n[batch {i}/{total_batches}] 查询 {len(batch)} 个地址 ...")
        fetched = await fetch_balances_map(batch)
        balance_by_user.update(fetched)
        found = sum(1 for v in fetched.values() if v is not None)
        print(f"  返回 {found}/{len(batch)} 个有效账户")

        if i < total_batches and interval_sec > 0:
            await asyncio.sleep(interval_sec)

    df = df.copy()
    df["balance"] = df[USER_COL].map(
        lambda u: balance_by_user.get(normalize_wallet(str(u)).lower())
    )

    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_out, index=False)

    queried = df["balance"].notna().sum()
    print(f"\n[ok] 已写入 {parquet_out}")
    print(f"  已查询并写入 balance: {queried:,} 行")
    print(f"  未查询（balance 为空）: {len(df) - queried:,} 行")
    return 0


def print_balance_section(
    report: dict[str, Any],
    *,
    index: int | None = None,
    show_api_response: bool = True,
) -> None:
    print("\n" + "=" * 60)
    title = "钱包资金量（ScopeFi Track API）"
    if index is not None:
        title += f"  [{index}]"
    print(title)
    print("=" * 60)
    print(f"wallet: {report.get('wallet', '-')}")
    if not report.get("found"):
        print("[未返回数据] 请检查地址或 SCOPEFI_TRACK_BASE_URL")
        if show_api_response and report.get("api_response"):
            print("\n--- 接口完整响应 ---")
            print(
                json.dumps(
                    report["api_response"],
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )
            )
        return
    print(f"账户净值 account_value: {report.get('account_value'):,.4f} USDC")
    print(f"可提余额 withdrawable: {report.get('withdrawable'):,.4f} USDC")
    print(f"总名义持仓 total_ntl_pos: {report.get('total_ntl_pos'):,.4f} USDC")
    if report.get("raw"):
        print("\n--- 用户原始数据 (data[i]) ---")
        print(
            json.dumps(
                report["raw"],
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )
    if show_api_response and report.get("api_response"):
        print("\n--- 接口完整响应 ---")
        print(
            json.dumps(
                report["api_response"],
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="查询指定钱包 ScopeFi 账户资金量")
    p.add_argument(
        "--wallet",
        action="append",
        dest="wallets",
        metavar="0x...",
        help="钱包地址，可重复指定多个",
    )
    p.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="将结果写入 JSON 文件",
    )
    p.add_argument(
        "--parquet",
        nargs="?",
        const=Path(DEFAULT_PARQUET),
        type=Path,
        default=None,
        metavar="FILE",
        help=f"从 parquet 批量查资金（省略 FILE 时默认 {DEFAULT_PARQUET}）",
    )
    p.add_argument(
        "--parquet-out",
        type=Path,
        default=None,
        metavar="FILE",
        help=f"带 balance 列的输出 parquet（默认 {DEFAULT_PARQUET_OUT}）",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批查询地址数（默认 {DEFAULT_BATCH_SIZE}）",
    )
    p.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_MAX_BATCHES,
        help=f"最多查询批次数，0 表示全部（默认 {DEFAULT_MAX_BATCHES}）",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_BATCH_INTERVAL_SEC,
        help=f"每批查询间隔秒数（默认 {DEFAULT_BATCH_INTERVAL_SEC}）",
    )
    return p.parse_args()


async def _run(args: argparse.Namespace) -> int:
    if args.parquet is not None:
        parquet_in = Path(args.parquet)
        if not parquet_in.is_absolute():
            parquet_in = SCRIPT_DIR / parquet_in
        parquet_out = Path(args.parquet_out) if args.parquet_out else (SCRIPT_DIR / DEFAULT_PARQUET_OUT)
        if not parquet_out.is_absolute():
            parquet_out = SCRIPT_DIR / parquet_out
        max_batches = args.max_batches if args.max_batches > 0 else None
        return await batch_query_parquet(
            parquet_in=parquet_in,
            parquet_out=parquet_out,
            batch_size=args.batch_size,
            max_batches=max_batches,
            interval_sec=args.interval,
        )

    wallets: list[str] = []
    if args.wallets:
        wallets = [normalize_wallet(w) for w in args.wallets]
    elif WALLET.strip():
        wallets = [normalize_wallet(WALLET)]
    else:
        print(
            "[error] 请指定 --wallet 0x...、--parquet FILE，或在脚本顶部 CONFIG 设置 WALLET",
            file=sys.stderr,
        )
        return 1

    settings = ScopeFiSettings.from_env()
    print("query_wallet_balance")
    print(f"  API: {settings.position_url}")
    print(f"  查询 {len(wallets)} 个地址")

    if len(wallets) == 1:
        report_list = [await fetch_wallet_balance(wallets[0])]
    else:
        report_list = await fetch_wallet_balances(wallets)

    for i, report in enumerate(report_list, start=1):
        print_balance_section(
            report,
            index=i if len(report_list) > 1 else None,
            show_api_response=(len(report_list) == 1 or i == 1),
        )

    json_out = args.json_out
    if json_out is None and JSON_OUT.strip():
        json_out = Path(JSON_OUT.strip())
    if json_out is not None:
        out_path = json_out if json_out.is_absolute() else (SCRIPT_DIR / json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"wallets": wallets, "balances": report_list}
        out_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"\n[ok] JSON 已写入 {out_path}")

    return 0


def main() -> int:
    _configure_stdio()
    env_path = load_dotenv()
    if env_path:
        env_log(f"已加载 {env_path.name}")
    args = parse_args()
    try:
        return asyncio.run(_run(args))
    except Exception:
        log_error_exc()
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n已取消", file=sys.stderr)
        raise SystemExit(130) from None
