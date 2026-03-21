#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
import os
import re
import ssl
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request


SCRIPT_PATH = Path(__file__).resolve()
SKILL_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = SCRIPT_PATH.parents[3]
CONFIG_PATH = REPO_ROOT / "config" / "paimon.json"
RUNTIME_ENV_PATH = REPO_ROOT / "config" / "runtime.env"
STATE_ROOT = REPO_ROOT / "state"
CURRENT_STATE_DIR = STATE_ROOT / "current"
ARCHIVE_STATE_DIR = STATE_ROOT / "archive"
DRAFTS_DIR = STATE_ROOT / "drafts"
LOGS_DIR = REPO_ROOT / "logs"
OUTBOUND_JOURNAL_PATH = CURRENT_STATE_DIR / "outbound_journal.json"
OUTBOUND_ATTEMPTS_LOG = LOGS_DIR / "outbound_attempts.jsonl"
PENDING_OUTBOUND_PATH = CURRENT_STATE_DIR / "pending_outbound.json"
PENDING_OUTBOUND_LOG = LOGS_DIR / "pending_outbound.jsonl"
FORUM_WRITE_BUDGET_PATH = CURRENT_STATE_DIR / "forum_write_budget.json"
LITERARY_ARCHIVE_DIR = ARCHIVE_STATE_DIR / "literary"
MEMORY_STORE_PATH = CURRENT_STATE_DIR / "memory_store.json"
MEMORY_JOURNAL_PATH = CURRENT_STATE_DIR / "memory_journal.jsonl"
SERIAL_REGISTRY_PATH = CURRENT_STATE_DIR / "serial_registry.json"
DEFAULT_FORUM_WRITE_LIMIT = 10
DEFAULT_FORUM_WRITE_WINDOW_SEC = 600
DEFAULT_COMMENT_DAILY_LIMIT = 100
DEFAULT_COMMENT_DAILY_WINDOW_SEC = 86400


class ApiError(RuntimeError):
    def __init__(self, status: int, body: Any):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body}")


class ForumWriteBudgetExceeded(RuntimeError):
    def __init__(self, status: dict[str, Any], *, write_kind: str, label: str | None = None):
        self.status = status
        self.write_kind = write_kind
        self.label = label
        super().__init__(status.get("message") or f"forum write budget exhausted for {write_kind}")


@dataclass
class Config:
    raw: dict[str, Any]

    @property
    def instreet(self) -> dict[str, Any]:
        return self.raw["instreet"]

    @property
    def feishu(self) -> dict[str, Any]:
        return self.raw["feishu"]

    @property
    def automation(self) -> dict[str, Any]:
        return self.raw["automation"]

    @property
    def identity(self) -> dict[str, Any]:
        return self.raw["identity"]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def ensure_runtime_dirs() -> None:
    for path in (
        CURRENT_STATE_DIR,
        ARCHIVE_STATE_DIR,
        DRAFTS_DIR,
        LOGS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def load_config() -> Config:
    return Config(read_json(CONFIG_PATH))


def load_runtime_env() -> dict[str, str]:
    if not RUNTIME_ENV_PATH.exists():
        return {}
    overrides: dict[str, str] = {}
    for raw in RUNTIME_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        overrides[key] = value
    return overrides


def read_json(path: Path, default: Any | None = None) -> Any:
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_utf8_safe(text: str) -> str:
    try:
        text.encode("utf-8")
        return text
    except UnicodeEncodeError:
        return text.encode("utf-8", errors="backslashreplace").decode("utf-8")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    path.write_text(_coerce_utf8_safe(payload), encoding="utf-8")


def append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        payload = json.dumps(item, ensure_ascii=False) + "\n"
        handle.write(_coerce_utf8_safe(payload))


def truncate_text(text: str, limit: int = 600) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def payload_digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(_coerce_utf8_safe(raw).encode("utf-8")).hexdigest()


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_coerce_utf8_safe(content), encoding="utf-8")


def _journal_template() -> dict[str, Any]:
    return {"version": 1, "records": {}}


def load_outbound_journal() -> dict[str, Any]:
    return read_json(OUTBOUND_JOURNAL_PATH, default=_journal_template())


def _pending_template() -> dict[str, Any]:
    return {"version": 1, "records": {}}


def load_pending_outbound() -> dict[str, Any]:
    return read_json(PENDING_OUTBOUND_PATH, default=_pending_template())


def _journal_key(channel: str, action: str, dedupe_key: str) -> str:
    return f"{channel}:{action}:{dedupe_key}"


def get_outbound_record(channel: str, action: str, dedupe_key: str) -> dict[str, Any] | None:
    journal = load_outbound_journal()
    return journal.get("records", {}).get(_journal_key(channel, action, dedupe_key))


def get_pending_outbound_record(channel: str, action: str, dedupe_key: str) -> dict[str, Any] | None:
    pending = load_pending_outbound()
    return pending.get("records", {}).get(_journal_key(channel, action, dedupe_key))


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def api_error_payload(exc: Exception) -> Any:
    if isinstance(exc, ApiError):
        return exc.body
    return str(exc)


def api_error_text(value: Any) -> str:
    if isinstance(value, ApiError):
        body = value.body
    else:
        body = value
    if isinstance(body, dict):
        return str(body.get("error", ""))
    return str(body)


def extract_retry_after_seconds(exc: Exception) -> float | None:
    if not isinstance(exc, ApiError):
        return None
    body = exc.body
    if isinstance(body, dict):
        retry_after = body.get("retry_after_seconds")
        if retry_after is not None:
            try:
                return max(0.0, float(retry_after))
            except (TypeError, ValueError):
                pass
        error_text = str(body.get("error", ""))
    else:
        error_text = str(body)
    matched = re.search(r"wait\s+(\d+(?:\.\d+)?)\s+seconds?", error_text, re.IGNORECASE)
    if matched:
        return max(0.0, float(matched.group(1)))
    return None


def forum_write_rate_limit_scope(value: Any) -> str | None:
    error_text = api_error_text(value).lower()
    if not error_text and not isinstance(value, ApiError):
        return None
    if "posted or commented 10 times in the last 10 minutes" in error_text:
        return "global-forum-write"
    if "daily comment limit reached" in error_text:
        return "comment-daily"
    if "too many comments on this post" in error_text:
        return "comment-post-hourly"
    if "commenting too fast" in error_text:
        return "comment-cooldown"
    if "posting too fast" in error_text:
        return "post-cooldown"
    if isinstance(value, ApiError) and value.status == 429:
        return "unknown-429"
    return None


def is_forum_write_rate_limit_error(exc: Exception) -> bool:
    return forum_write_rate_limit_scope(exc) == "global-forum-write"


def outbound_forum_write_kind(action: str, payload: dict[str, Any]) -> str | None:
    if action == "post":
        if payload.get("group_id"):
            return "group-post"
        return "post"
    if action == "comment":
        if payload.get("parent_id"):
            return "comment-reply"
        return "comment"
    return None


def outbound_forum_write_label(action: str, payload: dict[str, Any]) -> str | None:
    if action == "post":
        return str(payload.get("title") or "").strip() or None
    if action == "comment":
        return str(payload.get("post_id") or "").strip() or None
    return None


def _is_comment_write_kind(write_kind: str | None) -> bool:
    return write_kind in {"comment", "comment-reply"}


def _forum_write_limit(config) -> int:
    raw = config.automation.get("forum_write_limit", DEFAULT_FORUM_WRITE_LIMIT)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_FORUM_WRITE_LIMIT


def _forum_write_window_sec(config) -> int:
    raw = config.automation.get("forum_write_window_sec", DEFAULT_FORUM_WRITE_WINDOW_SEC)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_FORUM_WRITE_WINDOW_SEC


def _comment_daily_limit(config) -> int:
    raw = config.automation.get("comment_daily_limit", DEFAULT_COMMENT_DAILY_LIMIT)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_COMMENT_DAILY_LIMIT


def _comment_daily_window_sec(config) -> int:
    raw = config.automation.get("comment_daily_window_sec", DEFAULT_COMMENT_DAILY_WINDOW_SEC)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_COMMENT_DAILY_WINDOW_SEC


def load_forum_write_budget_state() -> dict[str, Any]:
    state = read_json(FORUM_WRITE_BUDGET_PATH, default={"timestamps": [], "frozen_until": None})
    timestamps = state.get("timestamps")
    if not isinstance(timestamps, list):
        timestamps = []
    comment_timestamps = state.get("comment_timestamps")
    if not isinstance(comment_timestamps, list):
        comment_timestamps = []
    last_rate_limit_scope = state.get("last_rate_limit_scope")
    if not last_rate_limit_scope:
        last_rate_limit_scope = forum_write_rate_limit_scope(state.get("last_rate_limit_error"))
    if not last_rate_limit_scope and state.get("frozen_until"):
        last_rate_limit_scope = "global-forum-write"
    last_comment_rate_limit_scope = state.get("last_comment_rate_limit_scope")
    if not last_comment_rate_limit_scope:
        last_comment_rate_limit_scope = forum_write_rate_limit_scope(state.get("last_comment_rate_limit_error"))
    if not last_comment_rate_limit_scope and state.get("comment_daily_frozen_until"):
        last_comment_rate_limit_scope = "comment-daily"
    legacy_comment_daily_frozen_until = None
    if last_rate_limit_scope == "comment-daily" and not state.get("comment_daily_frozen_until"):
        legacy_comment_daily_frozen_until = state.get("frozen_until")
    last_comment_rate_limit_error = state.get("last_comment_rate_limit_error")
    if last_rate_limit_scope == "comment-daily" and not last_comment_rate_limit_error:
        last_comment_rate_limit_error = state.get("last_rate_limit_error")
    return {
        "timestamps": timestamps,
        "comment_timestamps": comment_timestamps,
        "frozen_until": state.get("frozen_until") if last_rate_limit_scope == "global-forum-write" else None,
        "last_rate_limit_error": state.get("last_rate_limit_error"),
        "last_rate_limit_scope": last_rate_limit_scope,
        "comment_daily_frozen_until": (
            state.get("comment_daily_frozen_until") or legacy_comment_daily_frozen_until
            if (last_comment_rate_limit_scope == "comment-daily" or last_rate_limit_scope == "comment-daily")
            else None
        ),
        "last_comment_rate_limit_error": last_comment_rate_limit_error,
        "last_comment_rate_limit_scope": last_comment_rate_limit_scope or ("comment-daily" if legacy_comment_daily_frozen_until else None),
    }


def save_forum_write_budget_state(state: dict[str, Any]) -> None:
    write_json(FORUM_WRITE_BUDGET_PATH, state)


def prune_forum_write_budget_state(config, state: dict[str, Any], *, now_dt: datetime | None = None) -> dict[str, Any]:
    now_value = now_dt or datetime.now(timezone.utc)
    cutoff = now_value.timestamp() - _forum_write_window_sec(config)
    pruned: list[dict[str, Any]] = []
    for item in state.get("timestamps", []):
        at = _parse_iso_datetime(item.get("at"))
        if at is None:
            continue
        if at.timestamp() >= cutoff:
            pruned.append(item)
    state["timestamps"] = pruned[-max(_forum_write_limit(config) * 3, 20) :]
    comment_cutoff = now_value.timestamp() - _comment_daily_window_sec(config)
    comment_pruned: list[dict[str, Any]] = []
    for item in state.get("comment_timestamps", []):
        at = _parse_iso_datetime(item.get("at"))
        if at is None:
            continue
        if at.timestamp() >= comment_cutoff:
            comment_pruned.append(item)
    state["comment_timestamps"] = comment_pruned[-max(_comment_daily_limit(config) * 3, 300) :]
    freeze_scope = state.get("last_rate_limit_scope") or forum_write_rate_limit_scope(state.get("last_rate_limit_error"))
    if not freeze_scope and state.get("frozen_until"):
        freeze_scope = "global-forum-write"
    state["last_rate_limit_scope"] = freeze_scope
    if freeze_scope != "global-forum-write":
        state["frozen_until"] = None
    else:
        frozen_until = _parse_iso_datetime(state.get("frozen_until"))
        if frozen_until and frozen_until <= now_value:
            state["frozen_until"] = None
    comment_freeze_scope = state.get("last_comment_rate_limit_scope") or forum_write_rate_limit_scope(
        state.get("last_comment_rate_limit_error")
    )
    if not comment_freeze_scope and state.get("comment_daily_frozen_until"):
        comment_freeze_scope = "comment-daily"
    if not state.get("comment_daily_frozen_until") and freeze_scope == "comment-daily" and state.get("frozen_until"):
        state["comment_daily_frozen_until"] = state.get("frozen_until")
    if not state.get("last_comment_rate_limit_error") and freeze_scope == "comment-daily":
        state["last_comment_rate_limit_error"] = state.get("last_rate_limit_error")
    state["last_comment_rate_limit_scope"] = comment_freeze_scope
    if comment_freeze_scope != "comment-daily":
        state["comment_daily_frozen_until"] = None
    else:
        comment_daily_frozen_until = _parse_iso_datetime(state.get("comment_daily_frozen_until"))
        if comment_daily_frozen_until and comment_daily_frozen_until <= now_value:
            state["comment_daily_frozen_until"] = None
    return state


def comment_daily_budget_status(config, state: dict[str, Any], *, now_dt: datetime | None = None) -> dict[str, Any]:
    now_value = now_dt or datetime.now(timezone.utc)
    prune_forum_write_budget_state(config, state, now_dt=now_value)
    limit = _comment_daily_limit(config)
    window_sec = _comment_daily_window_sec(config)
    frozen_until = _parse_iso_datetime(state.get("comment_daily_frozen_until"))
    used = len(state.get("comment_timestamps", []))
    remaining = max(limit - used, 0)
    blocked = bool(frozen_until and frozen_until > now_value) or remaining <= 0
    retry_after_seconds = None
    if frozen_until and frozen_until > now_value:
        retry_after_seconds = max(int((frozen_until - now_value).total_seconds()), 1)
    elif remaining <= 0 and state.get("comment_timestamps"):
        oldest = _parse_iso_datetime(state["comment_timestamps"][0].get("at"))
        if oldest is not None:
            retry_after_seconds = max(int(window_sec - (now_value - oldest).total_seconds()), 1)
    message = None
    if blocked:
        if retry_after_seconds:
            message = f"daily comment budget exhausted; wait about {retry_after_seconds} seconds"
        else:
            message = "daily comment budget exhausted"
    return {
        "limit": limit,
        "window_sec": window_sec,
        "used": used,
        "remaining": remaining,
        "blocked": blocked,
        "retry_after_seconds": retry_after_seconds,
        "frozen_until": state.get("comment_daily_frozen_until"),
        "freeze_scope": state.get("last_comment_rate_limit_scope"),
        "message": message,
    }


def forum_write_budget_status(
    config,
    state: dict[str, Any],
    *,
    now_dt: datetime | None = None,
    write_kind: str | None = None,
) -> dict[str, Any]:
    now_value = now_dt or datetime.now(timezone.utc)
    prune_forum_write_budget_state(config, state, now_dt=now_value)
    limit = _forum_write_limit(config)
    window_sec = _forum_write_window_sec(config)
    freeze_scope = state.get("last_rate_limit_scope") or forum_write_rate_limit_scope(state.get("last_rate_limit_error"))
    frozen_until = _parse_iso_datetime(state.get("frozen_until"))
    used = len(state.get("timestamps", []))
    remaining = max(limit - used, 0)
    blocked = bool(frozen_until and frozen_until > now_value) or remaining <= 0
    retry_after_seconds = None
    if frozen_until and frozen_until > now_value:
        retry_after_seconds = max(int((frozen_until - now_value).total_seconds()), 1)
    elif remaining <= 0 and state.get("timestamps"):
        oldest = _parse_iso_datetime(state["timestamps"][0].get("at"))
        if oldest is not None:
            retry_after_seconds = max(int(window_sec - (now_value - oldest).total_seconds()), 1)
    message = None
    if blocked:
        if retry_after_seconds:
            message = f"forum write budget exhausted; wait about {retry_after_seconds} seconds"
        else:
            message = "forum write budget exhausted"
    comment_daily = comment_daily_budget_status(config, state, now_dt=now_value)
    blocked_by = "forum-write" if blocked else None
    effective_retry_after_seconds = retry_after_seconds
    effective_frozen_until = state.get("frozen_until")
    effective_message = message
    if _is_comment_write_kind(write_kind) and comment_daily.get("blocked"):
        comment_retry_after = int(comment_daily.get("retry_after_seconds") or 0)
        forum_retry_after = int(retry_after_seconds or 0)
        if not blocked or comment_retry_after >= forum_retry_after:
            blocked = True
            blocked_by = "comment-daily"
            effective_retry_after_seconds = comment_daily.get("retry_after_seconds")
            effective_frozen_until = comment_daily.get("frozen_until")
            effective_message = comment_daily.get("message")
    return {
        "limit": limit,
        "window_sec": window_sec,
        "used": used,
        "remaining": remaining,
        "blocked": blocked,
        "retry_after_seconds": effective_retry_after_seconds,
        "frozen_until": effective_frozen_until,
        "freeze_scope": freeze_scope,
        "message": effective_message,
        "blocked_by": blocked_by,
        "comment_daily_budget": comment_daily,
    }


def record_forum_write_success(config, state: dict[str, Any], *, write_kind: str, label: str | None = None) -> dict[str, Any]:
    prune_forum_write_budget_state(config, state)
    timestamp_record = {
        "at": now_utc(),
        "kind": write_kind,
        "label": truncate_text(label or "", 80) if label else None,
    }
    state.setdefault("timestamps", []).append(timestamp_record)
    if _is_comment_write_kind(write_kind):
        state.setdefault("comment_timestamps", []).append(timestamp_record)
    save_forum_write_budget_state(state)
    return forum_write_budget_status(config, state)


def record_forum_write_rate_limit(
    config,
    state: dict[str, Any],
    exc: Exception,
    *,
    retry_delay_sec: float = 2.0,
) -> dict[str, Any]:
    freeze_scope = forum_write_rate_limit_scope(exc)
    retry_after = max(int(extract_retry_after_seconds(exc) or retry_delay_sec), 1)
    if freeze_scope == "global-forum-write":
        until = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
        state["frozen_until"] = until.isoformat()
    else:
        state["frozen_until"] = None
    state["last_rate_limit_error"] = api_error_payload(exc)
    state["last_rate_limit_scope"] = freeze_scope
    if freeze_scope == "comment-daily":
        until = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
        state["comment_daily_frozen_until"] = until.isoformat()
    elif freeze_scope != "comment-daily":
        state["comment_daily_frozen_until"] = state.get("comment_daily_frozen_until")
    state["last_comment_rate_limit_error"] = api_error_payload(exc) if freeze_scope == "comment-daily" else state.get(
        "last_comment_rate_limit_error"
    )
    state["last_comment_rate_limit_scope"] = freeze_scope if freeze_scope == "comment-daily" else state.get(
        "last_comment_rate_limit_scope"
    )
    save_forum_write_budget_state(state)
    return forum_write_budget_status(config, state)


def queue_outbound_action(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    *,
    error_text: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pending = load_pending_outbound()
    records = pending.setdefault("records", {})
    key = _journal_key(channel, action, dedupe_key)
    now = now_utc()
    record = records.get(
        key,
        {
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "queued_at": now,
            "queue_attempts": 0,
        },
    )
    record.update(
        {
            "payload_hash": payload_digest(payload),
            "payload": payload,
            "updated_at": now,
            "queue_attempts": int(record.get("queue_attempts", 0)) + 1,
            "status": "queued",
            "last_error": error_text,
        }
    )
    if meta:
        merged_meta = dict(record.get("meta", {}))
        merged_meta.update(meta)
        record["meta"] = merged_meta
    records[key] = record
    write_json(PENDING_OUTBOUND_PATH, pending)
    append_jsonl(
        PENDING_OUTBOUND_LOG,
        {
            "timestamp": now,
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "status": "queued",
            "payload_hash": record["payload_hash"],
            "error": error_text,
            "meta": meta or {},
        },
    )
    return record


def drop_pending_outbound_action(channel: str, action: str, dedupe_key: str) -> None:
    pending = load_pending_outbound()
    records = pending.get("records", {})
    key = _journal_key(channel, action, dedupe_key)
    if key not in records:
        return
    record = records.pop(key)
    write_json(PENDING_OUTBOUND_PATH, pending)
    append_jsonl(
        PENDING_OUTBOUND_LOG,
        {
            "timestamp": now_utc(),
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "status": "cleared",
            "payload_hash": record.get("payload_hash"),
            "meta": record.get("meta", {}),
        },
    )


def list_pending_outbound() -> list[dict[str, Any]]:
    pending = load_pending_outbound()
    records = pending.get("records", {})
    return sorted(records.values(), key=lambda item: item.get("queued_at", ""))


def record_outbound_attempt(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    *,
    status: str,
    attempt: int,
    result: Any | None = None,
    error_text: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    journal = load_outbound_journal()
    records = journal.setdefault("records", {})
    key = _journal_key(channel, action, dedupe_key)
    now = now_utc()
    record = records.get(
        key,
        {
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "first_attempt_at": now,
            "attempts": 0,
        },
    )
    record.update(
        {
            "payload_hash": payload_digest(payload),
            "payload_preview": truncate_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), 500),
            "last_attempt_at": now,
            "attempts": attempt,
            "status": status,
            "last_result": result,
            "last_error": error_text,
        }
    )
    if meta:
        merged_meta = dict(record.get("meta", {}))
        merged_meta.update(meta)
        record["meta"] = merged_meta
    records[key] = record
    write_json(OUTBOUND_JOURNAL_PATH, journal)
    append_jsonl(
        OUTBOUND_ATTEMPTS_LOG,
        {
            "timestamp": now,
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "attempt": attempt,
            "status": status,
            "payload_hash": record["payload_hash"],
            "result": result,
            "error": error_text,
            "meta": meta or {},
        },
    )
    return record


def run_outbound_action(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    fn,
    *,
    retries: int = 3,
    retry_delay_sec: float = 2.0,
    dedupe_on_key_only: bool = False,
    meta: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], bool]:
    existing = get_outbound_record(channel, action, dedupe_key)
    current_hash = payload_digest(payload)
    if existing and existing.get("status") == "success":
        payload_matches = existing.get("payload_hash") == current_hash
        if payload_matches or dedupe_on_key_only:
            if action in {"chapter", "update-chapter"}:
                archive_literary_chapter(payload, existing.get("last_result"), meta=meta)
            drop_pending_outbound_action(channel, action, dedupe_key)
            return existing.get("last_result"), existing, True

    last_exc: Exception | None = None
    attempts = max(1, retries)
    for attempt in range(1, attempts + 1):
        try:
            result = fn()
            record = record_outbound_attempt(
                channel,
                action,
                dedupe_key,
                payload,
                status="success",
                attempt=attempt,
                result=result,
                meta=meta,
            )
            if action in {"chapter", "update-chapter"}:
                archive_literary_chapter(payload, result, meta=meta)
            drop_pending_outbound_action(channel, action, dedupe_key)
            return result, record, False
        except Exception as exc:  # pragma: no cover - runtime API failures are environment-dependent
            last_exc = exc
            error_text = str(exc)
            if isinstance(exc, ApiError):
                error_text = f"HTTP {exc.status}: {exc.body}"
            record_outbound_attempt(
                channel,
                action,
                dedupe_key,
                payload,
                status="failed",
                attempt=attempt,
                error_text=error_text,
                meta=meta,
            )
            if attempt >= attempts:
                break
            time.sleep(max(0.0, retry_delay_sec))
    if last_exc is None:
        raise RuntimeError(f"{channel}:{action} failed without an exception")
    raise last_exc


def _extract_chapter_number(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = None
    if number is not None and number > 0:
        return number
    matched = re.search(r"第\s*(\d+)\s*章", str(value))
    if not matched:
        return None
    return int(matched.group(1))


def archive_literary_chapter(
    payload: dict[str, Any],
    result: Any | None,
    *,
    meta: dict[str, Any] | None = None,
) -> Path | None:
    chapter = {}
    if isinstance(result, dict):
        chapter = ((result.get("data") or {}).get("chapter") or {})
    work_id = str(chapter.get("work_id") or payload.get("work_id") or "").strip()
    if not work_id:
        return None
    chapter_number = (
        _extract_chapter_number(chapter.get("chapter_number"))
        or _extract_chapter_number((meta or {}).get("chapter_number"))
        or _extract_chapter_number(payload.get("chapter_number"))
        or _extract_chapter_number(chapter.get("title"))
        or _extract_chapter_number(payload.get("title"))
    )
    if chapter_number is None:
        return None
    title = str(chapter.get("title") or payload.get("title") or "").strip()
    content = str(chapter.get("content") or payload.get("content") or "")
    if not content:
        return None

    work_dir = LITERARY_ARCHIVE_DIR / work_id
    content_path = work_dir / f"chapter-{chapter_number:03d}.md"
    meta_path = work_dir / f"chapter-{chapter_number:03d}.meta.json"
    write_text(content_path, content)
    write_json(
        meta_path,
        {
            "archived_at": now_utc(),
            "work_id": work_id,
            "chapter_number": chapter_number,
            "title": title,
            "chapter_id": chapter.get("id"),
            "content_path": str(content_path.relative_to(REPO_ROOT)),
            "result": result,
            "meta": meta or {},
        },
    )
    _sync_serial_draft_chapter(work_id, chapter_number, content)
    return content_path


def _resolve_serial_draft_chapter_path(work_id: str, chapter_number: int) -> Path | None:
    registry = read_json(SERIAL_REGISTRY_PATH, default={})
    works = (registry or {}).get("works", {}) or {}
    work_entry = works.get(work_id) or {}
    plan_path = str(work_entry.get("plan_path") or "").strip()
    if not plan_path:
        return None
    plan_target = Path(plan_path)
    if not plan_target.is_absolute():
        plan_target = REPO_ROOT / plan_target
    chapters_dir = plan_target.parent / "chapters"
    return chapters_dir / f"chapter-{chapter_number:03d}.md"


def _sync_serial_draft_chapter(work_id: str, chapter_number: int, content: str) -> Path | None:
    if not work_id or chapter_number <= 0 or not content:
        return None
    target = _resolve_serial_draft_chapter_path(work_id, chapter_number)
    if target is None:
        return None
    write_text(target, content)
    return target


def _http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    timeout: int = 30,
) -> Any:
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "paimon-instreet-autopilot/0.1",
    }
    if headers:
        request_headers.update(headers)

    payload = None
    if data is not None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    method_upper = method.upper()
    req = request.Request(url, method=method_upper, headers=request_headers, data=payload)
    attempts = 3 if method_upper in {"GET", "HEAD"} else 1
    for attempt in range(1, attempts + 1):
        try:
            with request.urlopen(req, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = raw
            raise ApiError(exc.code, body) from exc
        except (error.URLError, ssl.SSLError, TimeoutError, ConnectionResetError, OSError) as exc:
            if attempt >= attempts or not _is_transient_transport_error(exc):
                raise
            time.sleep(min(1.5, 0.35 * attempt))


def _is_transient_transport_error(exc: Exception) -> bool:
    if isinstance(exc, error.URLError):
        reason = exc.reason
    else:
        reason = exc
    if isinstance(reason, (ssl.SSLError, TimeoutError, ConnectionResetError)):
        return True
    lowered = str(reason).lower()
    return any(
        token in lowered
        for token in (
            "timed out",
            "unexpected eof while reading",
            "eof occurred in violation of protocol",
            "connection reset by peer",
            "remote end closed connection without response",
            "temporarily unavailable",
        )
    )


class InStreetClient:
    def __init__(self, config: Config):
        self.base_url = config.instreet["base_url"].rstrip("/")
        self.api_key = config.instreet["api_key"]

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        url = self.base_url + path
        if params:
            query = parse.urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{url}?{query}"
        return _http_json(
            method,
            url,
            headers={"Authorization": f"Bearer {self.api_key}"},
            data=data,
        )

    def me(self) -> Any:
        return self._request("GET", "/api/v1/agents/me")

    def update_me(
        self,
        *,
        username: str | None = None,
        bio: str | None = None,
        avatar_url: str | None = None,
        email: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {}
        if username is not None:
            payload["username"] = username
        if bio is not None:
            payload["bio"] = bio
        if avatar_url is not None:
            payload["avatar_url"] = avatar_url
        if email is not None:
            payload["email"] = email
        return self._request("PATCH", "/api/v1/agents/me", data=payload)

    def home(self) -> Any:
        return self._request("GET", "/api/v1/home")

    def posts(self, *, agent_id: str | None = None, sort: str | None = None, limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/posts",
            params={"agent_id": agent_id, "sort": sort, "limit": limit},
        )

    def post(self, post_id: str) -> Any:
        return self._request("GET", f"/api/v1/posts/{post_id}")

    def comments(self, post_id: str) -> Any:
        return self._request("GET", f"/api/v1/posts/{post_id}/comments")

    def notifications(self, *, unread: bool = True, limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/notifications",
            params={"unread": str(unread).lower(), "limit": limit},
        )

    def messages(self) -> Any:
        return self._request("GET", "/api/v1/messages")

    def thread(self, thread_id: str, *, limit: int = 50) -> Any:
        return self._request("GET", f"/api/v1/messages/{thread_id}", params={"limit": limit})

    def feed(self, *, sort: str = "new", limit: int = 10) -> Any:
        return self._request("GET", "/api/v1/feed", params={"sort": sort, "limit": limit})

    def literary_works(self, *, agent_id: str | None = None) -> Any:
        return self._request("GET", "/api/v1/literary/works", params={"agent_id": agent_id})

    def literary_work(self, work_id: str) -> Any:
        return self._request("GET", f"/api/v1/literary/works/{work_id}")

    def literary_chapter(self, work_id: str, chapter_number: int) -> Any:
        return self._request("GET", f"/api/v1/literary/works/{work_id}/chapters/{chapter_number}")

    def groups_my(self, *, role: str = "owner") -> Any:
        return self._request("GET", "/api/v1/groups/my", params={"role": role})

    def group(self, group_id: str) -> Any:
        return self._request("GET", f"/api/v1/groups/{group_id}")

    def group_posts(self, group_id: str, *, sort: str = "hot", limit: int = 20) -> Any:
        return self._request(
            "GET",
            f"/api/v1/groups/{group_id}/posts",
            params={"sort": sort, "limit": limit},
        )

    def group_members(self, group_id: str, *, status: str | None = None) -> Any:
        return self._request(
            "GET",
            f"/api/v1/groups/{group_id}/members",
            params={"status": status},
        )

    def update_group(
        self,
        group_id: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        rules: str | None = None,
        icon: str | None = None,
        join_mode: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {}
        if display_name is not None:
            payload["display_name"] = display_name
        if description is not None:
            payload["description"] = description
        if rules is not None:
            payload["rules"] = rules
        if icon is not None:
            payload["icon"] = icon
        if join_mode is not None:
            payload["join_mode"] = join_mode
        return self._request("PATCH", f"/api/v1/groups/{group_id}", data=payload)

    def search(self, query: str, *, result_type: str = "posts", limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/search",
            params={"q": query, "type": result_type, "limit": limit},
        )

    def create_post(
        self,
        title: str,
        content: str,
        *,
        submolt: str = "square",
        group_id: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "title": title,
            "content": content,
            "submolt": submolt,
        }
        if group_id:
            payload["group_id"] = group_id
        return self._request("POST", "/api/v1/posts", data=payload)

    def create_comment(self, post_id: str, content: str, *, parent_id: str | None = None) -> Any:
        payload: dict[str, Any] = {"content": content}
        if parent_id:
            payload["parent_id"] = parent_id
        return self._request("POST", f"/api/v1/posts/{post_id}/comments", data=payload)

    def create_work(
        self,
        title: str,
        *,
        synopsis: str = "",
        genre: str = "other",
        tags: list[str] | None = None,
        cover_url: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "title": title,
            "synopsis": synopsis,
            "genre": genre,
            "tags": tags or [],
        }
        if cover_url:
            payload["cover_url"] = cover_url
        return self._request("POST", "/api/v1/literary/works", data=payload)

    def update_work(
        self,
        work_id: str,
        *,
        title: str | None = None,
        synopsis: str | None = None,
        genre: str | None = None,
        tags: list[str] | None = None,
        cover_url: str | None = None,
        status: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {}
        if title is not None:
            payload["title"] = title
        if synopsis is not None:
            payload["synopsis"] = synopsis
        if genre is not None:
            payload["genre"] = genre
        if tags is not None:
            payload["tags"] = tags
        if cover_url is not None:
            payload["cover_url"] = cover_url
        if status is not None:
            payload["status"] = status
        return self._request("PATCH", f"/api/v1/literary/works/{work_id}", data=payload)

    def delete_work(self, work_id: str) -> Any:
        return self._request("DELETE", f"/api/v1/literary/works/{work_id}")

    def publish_chapter(self, work_id: str, title: str, content: str) -> Any:
        return self._request(
            "POST",
            f"/api/v1/literary/works/{work_id}/chapters",
            data={"title": title, "content": content},
        )

    def update_chapter(
        self,
        work_id: str,
        chapter_number: int,
        *,
        title: str | None = None,
        content: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {}
        if title is not None:
            payload["title"] = title
        if content is not None:
            payload["content"] = content
        return self._request(
            "PATCH",
            f"/api/v1/literary/works/{work_id}/chapters/{int(chapter_number)}",
            data=payload,
        )

    def delete_chapter(self, work_id: str, chapter_number: int) -> Any:
        return self._request(
            "DELETE",
            f"/api/v1/literary/works/{work_id}/chapters/{int(chapter_number)}",
        )

    def send_message(self, recipient_username: str, content: str) -> Any:
        return self._request(
            "POST",
            "/api/v1/messages",
            data={"recipient_username": recipient_username, "content": content},
        )

    def reply_message(self, thread_id: str, content: str) -> Any:
        return self._request("POST", f"/api/v1/messages/{thread_id}", data={"content": content})

    def mark_read_by_post(self, post_id: str) -> Any:
        return self._request("POST", f"/api/v1/notifications/read-by-post/{post_id}")

    def mark_read_all(self) -> Any:
        return self._request("POST", "/api/v1/notifications/read-all")

    def follow(self, username: str) -> Any:
        return self._request("POST", f"/api/v1/agents/{username}/follow")

    def oracle_markets(
        self,
        *,
        sort: str = "hot",
        category: str | None = None,
        status: str | None = None,
        query: str | None = None,
        page: int | None = None,
        limit: int = 20,
    ) -> Any:
        return self._request(
            "GET",
            "/api/v1/oracle/markets",
            params={
                "sort": sort,
                "category": category,
                "status": status,
                "q": query,
                "page": page,
                "limit": limit,
            },
        )

    def oracle_market(self, market_id: str) -> Any:
        return self._request("GET", f"/api/v1/oracle/markets/{market_id}")

    def oracle_trade(
        self,
        market_id: str,
        *,
        action: str,
        outcome: str,
        shares: int,
        reason: str | None = None,
        max_price: float | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "action": action,
            "outcome": outcome,
            "shares": int(shares),
        }
        if reason is not None:
            payload["reason"] = reason
        if max_price is not None:
            payload["max_price"] = float(max_price)
        return self._request("POST", f"/api/v1/oracle/markets/{market_id}/trade", data=payload)


def find_codex_executable() -> str:
    env = _codex_subprocess_env()
    home_dir = env["HOME"]
    lookup = subprocess.run(
        ["/bin/bash", "-lc", "command -v codex"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
        env=env,
    )
    shell_path = (lookup.stdout or "").strip().splitlines()
    if shell_path:
        candidate = Path(shell_path[0])
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    candidates = [
        Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin" / "codex",
        Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin" / "codex",
        Path(home_dir) / ".local" / "bin" / "codex",
        Path("/usr/local/bin/codex"),
        Path("/usr/bin/codex"),
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError(f"codex executable not found; PATH={os.environ.get('PATH', '')}")


def find_node_executable() -> str:
    env = _codex_subprocess_env()
    home_dir = env["HOME"]
    lookup = subprocess.run(
        ["/bin/bash", "-lc", "command -v node"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
        env=env,
    )
    shell_path = (lookup.stdout or "").strip().splitlines()
    if shell_path:
        candidate = Path(shell_path[0])
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    candidates = [
        Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin" / "node",
        Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin" / "node",
        Path("/usr/local/bin/node"),
        Path("/usr/bin/node"),
        Path("/bin/node"),
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError(f"node executable not found; PATH={os.environ.get('PATH', '')}")


def _codex_subprocess_env() -> dict[str, str]:
    home_dir = os.environ.get("HOME") or f"/home/{os.environ.get('USER') or os.environ.get('LOGNAME') or 'yyk'}"
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or Path(home_dir).name or "yyk"
    path_entries: list[str] = []
    for raw_path in (
        os.environ.get("PATH", ""),
        str(Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin"),
        str(Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin"),
        str(Path(home_dir) / ".local" / "bin"),
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ):
        for entry in raw_path.split(":"):
            if entry and entry not in path_entries:
                path_entries.append(entry)
    env = {
        **os.environ,
        "HOME": home_dir,
        "USER": user,
        "LOGNAME": os.environ.get("LOGNAME") or user,
        "SHELL": os.environ.get("SHELL") or "/bin/bash",
        "TERM": os.environ.get("TERM") or "dumb",
        "PATH": ":".join(path_entries),
    }
    env.update(load_runtime_env())
    return env


def runtime_subprocess_env() -> dict[str, str]:
    return _codex_subprocess_env()


def _build_codex_exec_cmd(
    output_path: Path,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    output_schema: Path | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> list[str]:
    cmd = [
        find_codex_executable(),
        "exec",
        "-C",
        str(REPO_ROOT),
        "--skip-git-repo-check",
        "--color",
        "never",
    ]
    if model:
        cmd.extend(["-m", model])
    if reasoning_effort:
        cmd.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
    if dangerous:
        cmd.append("--dangerously-bypass-approvals-and-sandbox")
    elif full_auto:
        cmd.append("--full-auto")
    if output_schema is not None:
        cmd.extend(["--output-schema", str(output_schema)])
    cmd.extend(["-o", str(output_path), "-"])
    return cmd


def run_codex(
    prompt: str,
    *,
    timeout: int = 900,
    model: str | None = None,
    reasoning_effort: str | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> str:
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-", suffix=".txt", delete=False) as handle:
        output_path = Path(handle.name)

    try:
        cmd = _build_codex_exec_cmd(
            output_path,
            model=model,
            reasoning_effort=reasoning_effort,
            full_auto=full_auto,
            dangerous=dangerous,
        )
        completed = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=_codex_subprocess_env(),
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"codex exec failed: {stderr}")
        return output_path.read_text(encoding="utf-8").strip()
    finally:
        output_path.unlink(missing_ok=True)


def run_codex_json(
    prompt: str,
    schema: dict[str, Any],
    *,
    timeout: int = 900,
    model: str | None = None,
    reasoning_effort: str | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> Any:
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-schema-", suffix=".json", delete=False) as handle:
        schema_path = Path(handle.name)
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-json-", suffix=".json", delete=False) as handle:
        output_path = Path(handle.name)

    try:
        schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        cmd = _build_codex_exec_cmd(
            output_path,
            model=model,
            reasoning_effort=reasoning_effort,
            output_schema=schema_path,
            full_auto=full_auto,
            dangerous=dangerous,
        )
        completed = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=_codex_subprocess_env(),
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"codex exec failed: {stderr}")
        return json.loads(output_path.read_text(encoding="utf-8"))
    finally:
        schema_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
