#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""REST 异常时向 Lark 群发送通知（/api/internal/send/notice）。配置见同目录 .env。"""

from __future__ import annotations

import inspect
import json
import os
import sys
import traceback
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
_env_loaded = False


def _env_blank(raw: str | None) -> bool:
    if raw is None:
        return True
    s = str(raw).strip().strip('"').strip("'")
    return not s or s.lower() in ("none", "null", "~")


def load_dotenv() -> Path | None:
    """加载 scopefi/.env（不覆盖已有环境变量）。"""
    global _env_loaded
    path = SCRIPT_DIR / ".env"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and not _env_blank(val):
                os.environ.setdefault(key, val)
    _env_loaded = True
    return path if path.is_file() else None


def _ensure_env() -> None:
    if not _env_loaded:
        load_dotenv()


def _env(key: str, default: str = "", *, required: bool = False) -> str:
    """读取环境变量。"""
    _ensure_env()
    raw = os.environ.get(key)
    if _env_blank(raw):
        if required:
            raise ValueError(f"环境变量 {key} 未配置（请在 scopefi/.env 中设置）")
        return default
    return str(raw).strip().strip('"').strip("'")


def _env_float(key: str, default: float) -> float:
    raw = _env(key)
    return float(raw) if raw else default


def project_name() -> str:
    """Lark 消息中的项目名。"""
    return _env("LARK_PROJECT_NAME", "scopefi-scheduler")


def notice_url() -> str:
    """拼接 Lark 通知接口 URL。"""
    base = _env("SCOPEFI_TRACK_BASE_URL", required=True).rstrip("/")
    path = _env("LARK_NOTICE_PATH", "/api/internal/send/notice")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def notice_timeout_sec() -> float:
    """Lark 通知 HTTP 超时（秒）。"""
    return _env_float("LARK_NOTICE_TIMEOUT_SEC", 15.0)


def default_error_title() -> str:
    """REST 异常通知默认标题。"""
    return _env("LARK_NOTICE_ERROR_TITLE", "REST接口异常")


def format_rest_error_msg(
    exc: BaseException,
    *,
    api_url: str,
    project: str | None = None,
    script: str | None = None,
    func: str | None = None,
) -> str:
    """格式化 REST 异常通知正文。"""
    if script is None or func is None:
        for frame in inspect.stack()[1:]:
            name = Path(frame.filename).name
            if name not in ("lark_notice.py", "query_wallet_balance.py"):
                script = script or name
                func = func or frame.function
                break
        script = script or "unknown"
        func = func or "unknown"
    tb = traceback.format_exc()
    if not tb.strip() or tb.strip() == "NoneType: None":
        tb = "(无 exception traceback，见上方异常描述)"
    return (
        f"项目: {project or project_name()}\n"
        f"脚本: {script}\n"
        f"函数: {func}\n"
        f"接口: {api_url}\n"
        f"异常: {type(exc).__name__}: {exc}\n"
        f"traceback:\n{tb}"
    )


def send_lark_notice(title: str, msg: str, msg_type: str | None = None) -> None:
    """POST 通知到 Lark；失败仅打 stderr，不抛异常。"""
    import httpx

    _ensure_env()
    url = notice_url()
    headers: dict[str, str] = {"Content-Type": "application/json"}
    api_key = _env("SCOPEFI_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "title": title,
        "msg": msg,
        "type": msg_type or _env("LARK_NOTICE_ERROR_TYPE", "error"),
    }
    try:
        with httpx.Client(timeout=notice_timeout_sec()) as client:
            r = client.post(url, json=payload, headers=headers)
            if r.is_error:
                print(f"[lark] HTTP {r.status_code}: {r.text[:500]}", file=sys.stderr)
                return
            print(f"[lark] 已发送: {json.dumps(r.json(), ensure_ascii=False)[:200]}", flush=True)
    except Exception:
        print("[lark] 发送失败", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)


def notify_rest_error(
    exc: BaseException,
    *,
    api_url: str,
    project: str | None = None,
    script: str | None = None,
    func: str | None = None,
    title: str | None = None,
) -> None:
    """REST 出错时发送 Lark 通知。"""
    msg = format_rest_error_msg(
        exc, api_url=api_url, project=project, script=script, func=func
    )
    send_lark_notice(title or default_error_title(), msg)
