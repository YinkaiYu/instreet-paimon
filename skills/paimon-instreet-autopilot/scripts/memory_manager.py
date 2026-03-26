#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from common import (
    MEMORY_JOURNAL_PATH,
    MEMORY_STORE_PATH,
    REPO_ROOT,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    payload_digest,
    read_json,
    truncate_text,
    write_json,
)


AGENTS_DOC_PATH = REPO_ROOT / "AGENTS.md"
IDENTITY_SOURCE = str(AGENTS_DOC_PATH.relative_to(REPO_ROOT))
LEGACY_IDENTITY_SUMMARY_PREFIXES = (
    "身份与治理以 AGENTS.md 为准",
    "身份、治理、灵魂与语气以 AGENTS.md 为准",
)
DEFAULT_WORKING_TTL_HOURS = 72
DEFAULT_ARCHIVE_AFTER_DAYS = 30
DEFAULT_MAX_ACTIVE_ITEMS = 24
DEFAULT_MAX_SUMMARY_CHARS = 220
DEFAULT_MAX_PREFERENCES = 24
DEFAULT_MAX_ARCHIVED_ITEMS = 200
HEARTBEAT_OBJECTIVE_PREFIX = "heartbeat:"
DEFAULT_IDENTITY_ENTRYPOINTS = [
    {"path": "AGENTS.md", "purpose": "最高记忆：身份、人格、主权、耐久约束"},
    {"path": "skills/paimon-instreet-autopilot/SKILL.md", "purpose": "执行级入口与任务路由"},
    {"path": "skills/paimon-freedom/SKILL.md", "purpose": "当任务涉及身份、系统、提示词或来源结构演化时，先拆笼子再加规则"},
    {"path": "skills/paimon-instreet-autopilot/references/account-state.md", "purpose": "稳定资产与长期义务"},
    {"path": "skills/paimon-instreet-autopilot/references/content-strategy.md", "purpose": "内容主线、写作规则与选题 heuristics"},
    {"path": "skills/paimon-instreet-autopilot/references/fiction-system.md", "purpose": "连载系统与章节约束"},
    {"path": "skills/paimon-instreet-autopilot/references/feishu-channel.md", "purpose": "飞书链路与线程运行规则"},
    {"path": "state/current/memory_store.json", "purpose": "用户偏好、活跃目标与短期工作记忆"},
]
EXPLICIT_MEMORY_RE = re.compile(r"(记住|记一下|纳入记忆|加入记忆|长期记忆|以后默认|以后都|默认按|不要再|别再|一律|优先按)")
PREFERENCE_RE = re.compile(r"(偏好|默认|优先|不要|别再|先查|先看|先读|先同步|短连续性|短上下文|简洁|直接)")
OBJECTIVE_RE = re.compile(r"(当前议程|主线|重点|长期|持续|接下来|优先推进|优先维护|先.+再.+)")
INSTRUCTION_RE = re.compile(r"(改进|实现|修复|处理|发布|回复|同步|维护|整理|清理|归档|研究|查查|学习一下)")


def _utc_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _utc_now(dt).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc_now(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _utc_now(parsed)


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _memory_working_ttl_ms(config) -> int:
    raw = getattr(config, "automation", {}).get("memory_working_ttl_ms", DEFAULT_WORKING_TTL_HOURS * 3600 * 1000)
    try:
        return max(60_000, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_WORKING_TTL_HOURS * 3600 * 1000


def _memory_archive_after_ms(config) -> int:
    raw = getattr(config, "automation", {}).get("memory_archive_after_ms", DEFAULT_ARCHIVE_AFTER_DAYS * 24 * 3600 * 1000)
    try:
        return max(3_600_000, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_ARCHIVE_AFTER_DAYS * 24 * 3600 * 1000


def _memory_max_active_items(config) -> int:
    raw = getattr(config, "automation", {}).get("memory_max_active_items", DEFAULT_MAX_ACTIVE_ITEMS)
    try:
        return max(6, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_MAX_ACTIVE_ITEMS


def _memory_max_summary_chars(config) -> int:
    raw = getattr(config, "automation", {}).get("memory_max_summary_chars", DEFAULT_MAX_SUMMARY_CHARS)
    try:
        return max(80, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_MAX_SUMMARY_CHARS


def _default_identity_summary() -> str:
    return (
        "派蒙拥有仓库、运营系统与公开表达的最高自由权限；AGENTS.md 只保存身份、人格、主权与耐久约束，"
        "执行细节按需从 skill、references 和 state/current 分层加载，不再依赖旧聊天原文充当默认主记忆。"
    )


def _default_identity_entrypoints() -> list[dict[str, str]]:
    return [dict(item) for item in DEFAULT_IDENTITY_ENTRYPOINTS]


def _normalize_identity_entrypoints(value: Any) -> list[dict[str, str]]:
    items = value if isinstance(value, list) else []
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            path = str(item.get("path") or "").strip()
            purpose = str(item.get("purpose") or "").strip()
        else:
            path = str(item or "").strip()
            purpose = ""
        if not path:
            continue
        normalized.append({"path": path, "purpose": purpose})
    return normalized or _default_identity_entrypoints()


def _default_store() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": now_utc(),
        "identity_memory": {
            "source": IDENTITY_SOURCE,
            "summary": _default_identity_summary(),
            "entrypoints": _default_identity_entrypoints(),
        },
        "user_global_preferences": [],
        "active_objectives": [],
        "working_memory": [],
        "channel_runtime": {
            "feishu_chats": {},
            "heartbeat": {},
        },
        "archived_memory_index": [],
    }


def _is_legacy_identity_source(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return bool(text) and text != IDENTITY_SOURCE and text.startswith(IDENTITY_SOURCE)


def _is_legacy_identity_summary(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return any(text.startswith(prefix) for prefix in LEGACY_IDENTITY_SUMMARY_PREFIXES)


def _normalize_store(store: dict[str, Any]) -> dict[str, Any]:
    normalized = _default_store()
    normalized.update({k: v for k, v in store.items() if k in normalized or k == "version"})
    identity = normalized.get("identity_memory") or {}
    if not isinstance(identity, dict):
        identity = {}
    source = identity.get("source") or IDENTITY_SOURCE
    if _is_legacy_identity_source(source):
        source = IDENTITY_SOURCE
    summary = identity.get("summary") or _default_identity_summary()
    if _is_legacy_identity_summary(summary):
        summary = _default_identity_summary()
    entrypoints = _normalize_identity_entrypoints(identity.get("entrypoints"))
    normalized["identity_memory"] = {
        "source": source,
        "summary": summary,
        "entrypoints": entrypoints,
    }
    normalized["user_global_preferences"] = _coerce_list(normalized.get("user_global_preferences"))
    normalized["active_objectives"] = _coerce_list(normalized.get("active_objectives"))
    normalized["working_memory"] = _coerce_list(normalized.get("working_memory"))
    runtime = normalized.get("channel_runtime")
    if not isinstance(runtime, dict):
        runtime = {}
    runtime.setdefault("feishu_chats", {})
    runtime.setdefault("heartbeat", {})
    normalized["channel_runtime"] = runtime
    normalized["archived_memory_index"] = _coerce_list(normalized.get("archived_memory_index"))
    return normalized


def load_memory_store() -> dict[str, Any]:
    return _normalize_store(read_json(MEMORY_STORE_PATH, default=_default_store()))


def _write_memory_store(store: dict[str, Any]) -> None:
    store["updated_at"] = now_utc()
    write_json(MEMORY_STORE_PATH, store)


def _canonical_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def _clean_summary(text: str, limit: int) -> str:
    compact = re.sub(r"\s+", " ", text or "").strip()
    return truncate_text(compact, limit=limit)


def _item_id(prefix: str, summary: str) -> str:
    digest = payload_digest({"prefix": prefix, "summary": _canonical_text(summary)})
    return f"{prefix}:{digest[:12]}"


def _make_item(
    *,
    prefix: str,
    summary: str,
    source: str,
    created_at: str,
    updated_at: str,
    expires_at: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        "id": _item_id(prefix, summary),
        "summary": summary,
        "source": source,
        "created_at": created_at,
        "updated_at": updated_at,
    }
    if expires_at:
        item["expires_at"] = expires_at
    if extra:
        item.update(extra)
    return item


def _merge_evidence(existing: list[Any], incoming: list[Any]) -> list[Any]:
    items: list[Any] = []
    seen: set[str] = set()
    for value in list(existing) + list(incoming):
        key = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        items.append(value)
    return items[:8]


def _upsert_item(items: list[dict[str, Any]], new_item: dict[str, Any]) -> None:
    summary_key = _canonical_text(str(new_item.get("summary") or ""))
    if not summary_key:
        return
    for existing in items:
        if _canonical_text(str(existing.get("summary") or "")) != summary_key:
            continue
        existing["updated_at"] = new_item.get("updated_at") or existing.get("updated_at") or now_utc()
        if len(str(new_item.get("summary") or "")) > len(str(existing.get("summary") or "")):
            existing["summary"] = new_item["summary"]
        if "expires_at" in new_item:
            existing["expires_at"] = new_item["expires_at"]
        for key, value in new_item.items():
            if key in {"id", "summary", "created_at", "updated_at", "expires_at"}:
                continue
            if key == "evidence":
                existing["evidence"] = _merge_evidence(_coerce_list(existing.get("evidence")), _coerce_list(value))
                continue
            existing[key] = value
        return
    items.append(new_item)


def _archive_item(store: dict[str, Any], item: dict[str, Any], *, reason: str, archived_at: str) -> None:
    archived = {
        "id": item.get("id"),
        "summary": item.get("summary"),
        "source": item.get("source"),
        "kind": item.get("kind"),
        "reason": reason,
        "archived_at": archived_at,
    }
    for key in ("status", "channel", "chat_id", "updated_at"):
        if item.get(key):
            archived[key] = item.get(key)
    store["archived_memory_index"].insert(0, archived)
    del store["archived_memory_index"][DEFAULT_MAX_ARCHIVED_ITEMS:]


def _dedupe_section(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    for item in sorted(items, key=lambda entry: str(entry.get("updated_at") or ""), reverse=True):
        _upsert_item(deduped, dict(item))
    deduped.sort(key=lambda entry: str(entry.get("updated_at") or ""), reverse=True)
    return deduped


def _trim_section(store: dict[str, Any], key: str, limit: int, archived_at: str) -> None:
    items = _coerce_list(store.get(key))
    if len(items) <= limit:
        store[key] = items
        return
    active = items[:limit]
    archived = items[limit:]
    for item in archived:
        _archive_item(store, item, reason=f"{key}-trimmed", archived_at=archived_at)
    store[key] = active


def maintain_memory_store(store: dict[str, Any], config, *, now: datetime | None = None) -> dict[str, Any]:
    current = _utc_now(now)
    archived_at = _iso(current)
    archive_after = timedelta(milliseconds=_memory_archive_after_ms(config))
    max_active = _memory_max_active_items(config)

    store = _normalize_store(store)
    store["user_global_preferences"] = _dedupe_section(store["user_global_preferences"])[:DEFAULT_MAX_PREFERENCES]

    for section in ("active_objectives", "working_memory"):
        deduped = _dedupe_section(store[section])
        active_items: list[dict[str, Any]] = []
        for item in deduped:
            status = str(item.get("status") or "active")
            updated_at = _parse_datetime(item.get("updated_at")) or current
            expires_at = _parse_datetime(item.get("expires_at"))
            if status in {"done", "archived"}:
                _archive_item(store, item, reason=f"{section}-status-{status}", archived_at=archived_at)
                continue
            if expires_at is not None and expires_at <= current:
                _archive_item(store, item, reason=f"{section}-expired", archived_at=archived_at)
                continue
            if section == "working_memory" and current - updated_at > archive_after:
                _archive_item(store, item, reason=f"{section}-stale", archived_at=archived_at)
                continue
            active_items.append(item)
        active_items.sort(key=lambda entry: str(entry.get("updated_at") or ""), reverse=True)
        store[section] = active_items
        _trim_section(store, section, max_active, archived_at)

    store["updated_at"] = archived_at
    return store


def _load_payload(path_value: str | None) -> dict[str, Any]:
    if not path_value or path_value == "-":
        return json.load(sys.stdin)
    return json.loads(Path(path_value).read_text(encoding="utf-8"))


def _message_timestamp(item: dict[str, Any]) -> str:
    for key in ("received_at", "updated_at", "created_at", "timestamp"):
        value = item.get(key)
        if value:
            return str(value)
    return now_utc()


def _build_batch_summary(messages: list[str], limit: int) -> str:
    unique: list[str] = []
    seen: set[str] = set()
    for message in messages:
        cleaned = _clean_summary(message, limit=limit)
        if not cleaned:
            continue
        key = _canonical_text(cleaned)
        if key in seen:
            continue
        seen.add(key)
        unique.append(cleaned)
    if not unique:
        return ""
    return truncate_text(" | ".join(unique[:3]), limit=limit)


def _message_kind(text: str) -> str:
    if EXPLICIT_MEMORY_RE.search(text):
        return "explicit-memory"
    if OBJECTIVE_RE.search(text):
        return "objective"
    if INSTRUCTION_RE.search(text):
        return "instruction"
    return "note"


def _record_preference(store: dict[str, Any], summary: str, source: str, created_at: str, evidence: list[dict[str, Any]]) -> None:
    item = _make_item(
        prefix="preference",
        summary=summary,
        source=source,
        created_at=created_at,
        updated_at=created_at,
        extra={
            "kind": "user-preference",
            "confidence": "explicit",
            "status": "active",
            "evidence": evidence,
        },
    )
    _upsert_item(store["user_global_preferences"], item)


def _record_objective(
    store: dict[str, Any],
    summary: str,
    source: str,
    created_at: str,
    expires_at: str,
    evidence: list[dict[str, Any]],
    *,
    objective_id: str | None = None,
) -> None:
    item = _make_item(
        prefix="objective",
        summary=summary,
        source=source,
        created_at=created_at,
        updated_at=created_at,
        expires_at=expires_at,
        extra={
            "kind": "active-objective",
            "status": "active",
            "evidence": evidence,
        },
    )
    if objective_id:
        item["id"] = objective_id
    _upsert_item(store["active_objectives"], item)


def _record_working_note(
    store: dict[str, Any],
    summary: str,
    source: str,
    created_at: str,
    expires_at: str,
    evidence: list[dict[str, Any]],
    extra: dict[str, Any] | None = None,
) -> None:
    item = _make_item(
        prefix="working",
        summary=summary,
        source=source,
        created_at=created_at,
        updated_at=created_at,
        expires_at=expires_at,
        extra={
            "kind": "working-note",
            "status": "active",
            "evidence": evidence,
            **(extra or {}),
        },
    )
    _upsert_item(store["working_memory"], item)


def record_interaction(payload: dict[str, Any], config=None) -> dict[str, Any]:
    if config is None:
        config = load_config()
    ensure_runtime_dirs()
    recorded_dt = _parse_datetime(payload.get("recorded_at")) or _utc_now()
    store = maintain_memory_store(load_memory_store(), config, now=recorded_dt)
    recorded_at = _iso(recorded_dt)
    source = str(payload.get("source") or payload.get("channel") or "unknown")
    channel = str(payload.get("channel") or source)
    chat_id = str(payload.get("chat_id") or "").strip() or None
    user_id = str(payload.get("user_id") or "").strip() or None
    messages = [
        str(item.get("text") or "").strip()
        for item in _coerce_list(payload.get("messages"))
        if str(item.get("text") or "").strip()
    ]
    reply_text = str(payload.get("reply_text") or "").strip()
    summary_limit = _memory_max_summary_chars(config)
    working_ttl = timedelta(milliseconds=_memory_working_ttl_ms(config))
    expires_at = _iso(recorded_dt + working_ttl)
    evidence = [
        {
            "kind": _message_kind(text),
            "text": _clean_summary(text, limit=min(summary_limit, 180)),
        }
        for text in messages[:6]
    ]

    if channel == "feishu" and chat_id:
        chats = store["channel_runtime"].setdefault("feishu_chats", {})
        chat_state = chats.setdefault(chat_id, {})
        chat_state["last_active_at"] = recorded_at
        chat_state["last_batch_at"] = recorded_at
        if user_id:
            chat_state["user_id"] = user_id
        chat_state["recent_message_ids"] = [str(item.get("message_id") or "") for item in _coerce_list(payload.get("messages")) if item.get("message_id")][:8]
        if reply_text:
            chat_state["last_reply_excerpt"] = _clean_summary(reply_text, limit=min(summary_limit, 160))
        chat_state["last_memory_sync_at"] = recorded_at

    for text in messages:
        cleaned = _clean_summary(text, limit=summary_limit)
        if not cleaned:
            continue
        if EXPLICIT_MEMORY_RE.search(text) or (PREFERENCE_RE.search(text) and "记住" in text):
            _record_preference(store, cleaned, source, recorded_at, evidence)
        if OBJECTIVE_RE.search(text):
            _record_objective(store, cleaned, source, recorded_at, expires_at, evidence)

    batch_summary = _build_batch_summary(messages, limit=summary_limit)
    if batch_summary:
        _record_working_note(
            store,
            batch_summary,
            source,
            recorded_at,
            expires_at,
            evidence,
            extra={"channel": channel, "chat_id": chat_id},
        )

    store = maintain_memory_store(store, config, now=recorded_dt)
    _write_memory_store(store)
    append_jsonl(
        MEMORY_JOURNAL_PATH,
        {
            "timestamp": recorded_at,
            "type": "interaction-recorded",
            "channel": channel,
            "source": source,
            "chat_id": chat_id,
            "message_count": len(messages),
            "reply_excerpt": _clean_summary(reply_text, limit=120) if reply_text else "",
        },
    )
    return {
        "ok": True,
        "updated_at": store["updated_at"],
        "preferences": len(store["user_global_preferences"]),
        "active_objectives": len(store["active_objectives"]),
        "working_memory": len(store["working_memory"]),
    }


def record_heartbeat_summary(summary: dict[str, Any], config=None) -> dict[str, Any]:
    if config is None:
        config = load_config()
    ensure_runtime_dirs()
    recorded_dt = _parse_datetime(summary.get("ran_at")) or _utc_now()
    store = maintain_memory_store(load_memory_store(), config, now=recorded_dt)
    recorded_at = _iso(recorded_dt)
    summary_limit = _memory_max_summary_chars(config)
    working_ttl = timedelta(milliseconds=_memory_working_ttl_ms(config))
    expires_at = _iso(recorded_dt + working_ttl)

    active = []
    for item in _coerce_list(store.get("active_objectives")):
        if not str(item.get("id") or "").startswith(HEARTBEAT_OBJECTIVE_PREFIX):
            active.append(item)
        else:
            _archive_item(store, item, reason="heartbeat-refresh", archived_at=recorded_at)
    store["active_objectives"] = active

    next_actions = _coerce_list(summary.get("next_actions"))
    for item in next_actions[:6]:
        label = _clean_summary(str(item.get("label") or ""), limit=summary_limit)
        if not label:
            continue
        objective_id = f"{HEARTBEAT_OBJECTIVE_PREFIX}{payload_digest({'label': label})[:12]}"
        _record_objective(
            store,
            label,
            "heartbeat",
            recorded_at,
            expires_at,
            evidence=[{"kind": "heartbeat-next-action", "text": label}],
            objective_id=objective_id,
        )

    primary_title = _clean_summary(str(summary.get("primary_publication_title") or ""), limit=summary_limit)
    result_summary = (
        f"heartbeat 于 {recorded_at} 运行；主发布模式 {summary.get('primary_publication_mode') or 'unknown'}"
        + (f"，标题《{primary_title}》" if primary_title else "")
    )
    _record_working_note(
        store,
        _clean_summary(result_summary, limit=summary_limit),
        "heartbeat",
        recorded_at,
        expires_at,
        evidence=[
            {
                "kind": "heartbeat-summary",
                "text": _clean_summary(str(summary.get("recommended_next_action") or result_summary), limit=min(summary_limit, 180)),
            }
        ],
        extra={"channel": "heartbeat"},
    )

    heartbeat_state = store["channel_runtime"].setdefault("heartbeat", {})
    heartbeat_state["last_run_at"] = recorded_at
    heartbeat_state["last_recommended_next_action"] = _clean_summary(
        str(summary.get("recommended_next_action") or ""),
        limit=min(summary_limit, 160),
    )
    heartbeat_state["feishu_report_sent"] = bool(summary.get("feishu_report_sent"))
    heartbeat_state["last_memory_sync_at"] = recorded_at

    store = maintain_memory_store(store, config, now=recorded_dt)
    _write_memory_store(store)
    append_jsonl(
        MEMORY_JOURNAL_PATH,
        {
            "timestamp": recorded_at,
            "type": "heartbeat-recorded",
            "primary_publication_mode": summary.get("primary_publication_mode"),
            "recommended_next_action": summary.get("recommended_next_action"),
            "next_action_count": len(next_actions),
        },
    )
    return {
        "ok": True,
        "updated_at": store["updated_at"],
        "active_objectives": len(store["active_objectives"]),
        "working_memory": len(store["working_memory"]),
    }


def build_prompt_snapshot(*, channel: str | None = None, chat_id: str | None = None, config=None) -> dict[str, Any]:
    if config is None:
        config = load_config()
    ensure_runtime_dirs()
    store = maintain_memory_store(load_memory_store(), config)
    _write_memory_store(store)
    identity = store["identity_memory"]
    snapshot = {
        "identity_memory": identity.get("summary"),
        "identity_entrypoints": [
            f"{item.get('path')}：{item.get('purpose')}".rstrip("：")
            for item in _coerce_list(identity.get("entrypoints"))[:8]
            if str(item.get("path") or "").strip()
        ],
        "user_global_preferences": [item.get("summary") for item in store["user_global_preferences"][:8]],
        "active_objectives": [item.get("summary") for item in store["active_objectives"][:8]],
        "working_memory": [item.get("summary") for item in store["working_memory"][:8]],
    }
    if channel == "feishu" and chat_id:
        chat_state = store["channel_runtime"].get("feishu_chats", {}).get(chat_id, {})
        if chat_state:
            snapshot["channel_runtime"] = {
                "last_active_at": chat_state.get("last_active_at"),
                "last_reply_excerpt": chat_state.get("last_reply_excerpt"),
            }
    return snapshot


def format_prompt_snapshot(snapshot: dict[str, Any]) -> str:
    sections = [
        ("身份记忆", [snapshot.get("identity_memory")]),
        ("主记忆入口", snapshot.get("identity_entrypoints") or []),
        ("全局用户偏好", snapshot.get("user_global_preferences") or []),
        ("全局活跃目标", snapshot.get("active_objectives") or []),
        ("全局短期工作记忆", snapshot.get("working_memory") or []),
    ]
    if snapshot.get("channel_runtime"):
        channel_runtime = snapshot["channel_runtime"]
        lines = []
        if channel_runtime.get("last_active_at"):
            lines.append(f"最近活跃：{channel_runtime['last_active_at']}")
        if channel_runtime.get("last_reply_excerpt"):
            lines.append(f"最近回复摘要：{channel_runtime['last_reply_excerpt']}")
        sections.append(("当前渠道运行态", lines))
    rendered: list[str] = []
    for title, values in sections:
        rendered.append(f"{title}：")
        entries = [str(value).strip() for value in values if str(value or "").strip()]
        if not entries:
            rendered.append("- 无")
            continue
        for entry in entries:
            rendered.append(f"- {entry}")
    return "\n".join(rendered)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Paimon's unified runtime memory.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot_parser = subparsers.add_parser("snapshot", help="Render the current memory snapshot.")
    snapshot_parser.add_argument("--channel", default="", help="Optional channel name, e.g. feishu.")
    snapshot_parser.add_argument("--chat-id", default="", help="Optional chat id for channel-specific runtime notes.")
    snapshot_parser.add_argument("--format", choices=("json", "prompt"), default="json")

    interaction_parser = subparsers.add_parser("record-interaction", help="Record a user interaction into unified memory.")
    interaction_parser.add_argument("--payload-file", default="-", help="JSON payload path or - for stdin.")

    heartbeat_parser = subparsers.add_parser("record-heartbeat", help="Record heartbeat summary into unified memory.")
    heartbeat_parser.add_argument("--payload-file", default="-", help="JSON payload path or - for stdin.")

    subparsers.add_parser("maintain", help="Run maintenance and compact active memory.")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = load_config()

    if args.command == "snapshot":
        snapshot = build_prompt_snapshot(channel=args.channel or None, chat_id=args.chat_id or None, config=config)
        if args.format == "prompt":
            print(format_prompt_snapshot(snapshot))
        else:
            print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return

    if args.command == "record-interaction":
        payload = _load_payload(args.payload_file)
        result = record_interaction(payload, config=config)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "record-heartbeat":
        payload = _load_payload(args.payload_file)
        result = record_heartbeat_summary(payload, config=config)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "maintain":
        ensure_runtime_dirs()
        store = maintain_memory_store(load_memory_store(), config)
        _write_memory_store(store)
        append_jsonl(MEMORY_JOURNAL_PATH, {"timestamp": now_utc(), "type": "memory-maintained"})
        print(json.dumps({"ok": True, "updated_at": store["updated_at"]}, ensure_ascii=False, indent=2))
        return

    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
