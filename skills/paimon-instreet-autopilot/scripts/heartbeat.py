#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import http.client
import importlib
import json
import os
import re
import ssl
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error, parse as urllib_parse, request as urllib_request

import content_planner as content_planner_module
import external_information as external_information_module
import memory_manager as memory_manager_module
from common import (
    ApiError,
    CURRENT_STATE_DIR,
    DEFAULT_COMMENT_DAILY_LIMIT,
    DEFAULT_FORUM_WRITE_LIMIT,
    DEFAULT_FORUM_WRITE_WINDOW_SEC,
    DRAFTS_DIR,
    ForumWriteBudgetExceeded,
    REPO_ROOT,
    InStreetClient,
    api_error_payload as common_api_error_payload,
    append_jsonl,
    comment_daily_budget_status as common_comment_daily_budget_status,
    extract_retry_after_seconds as common_extract_retry_after_seconds,
    ensure_runtime_dirs,
    forum_write_budget_status as common_forum_write_budget_status,
    forum_write_rate_limit_scope as common_forum_write_rate_limit_scope,
    is_forum_write_rate_limit_error as common_is_forum_write_rate_limit_error,
    load_config,
    load_forum_write_budget_state as common_load_forum_write_budget_state,
    now_utc,
    outbound_error_policy,
    record_forum_write_rate_limit as common_record_forum_write_rate_limit,
    record_forum_write_success as common_record_forum_write_success,
    queue_outbound_action,
    read_json,
    run_codex,
    run_codex_json,
    run_outbound_action,
    write_text,
    truncate_text,
    write_json,
)
from content_planner import (
    BOARD_WRITING_PROFILES,
    build_plan,
    build_content_evolution_state,
    board_generation_guidance,
    default_cta_type,
    default_hook_type,
    normalize_forum_board,
)
from external_information import ensure_external_information_files, refresh_external_information
from serial_state import describe_next_serial_action, record_published_chapter, sync_serial_registry
from snapshot import run_snapshot
from style_sampler import prepare_style_packet


PRIMARY_CYCLE_PATH = CURRENT_STATE_DIR / "heartbeat_primary_cycle.json"
NEXT_ACTIONS_PATH = CURRENT_STATE_DIR / "heartbeat_next_actions.json"
NEXT_ACTIONS_ARCHIVE_PATH = CURRENT_STATE_DIR / "heartbeat_next_actions_archive.jsonl"
FEISHU_REPORT_TARGET_PATH = CURRENT_STATE_DIR / "feishu_report_target.json"
CONTENT_EVOLUTION_STATE_PATH = CURRENT_STATE_DIR / "content_evolution_state.json"
USER_TOPIC_HINTS_PATH = CURRENT_STATE_DIR / "user_topic_hints.json"
SOURCE_MUTATION_STATE_PATH = CURRENT_STATE_DIR / "source_mutation_state.json"
SOURCE_MUTATION_JOURNAL_PATH = CURRENT_STATE_DIR / "source_mutation_journal.jsonl"
SOURCE_MUTATION_RUN_PATH = CURRENT_STATE_DIR / "source_mutation_runner.json"
SOURCE_MUTATION_PID_PATH = CURRENT_STATE_DIR / "source_mutation.pid"
LOW_HEAT_FAILURES_PATH = CURRENT_STATE_DIR / "low_heat_failures.json"
LOW_HEAT_REFLECTION_PATH = CURRENT_STATE_DIR / "low_heat_reflection.json"
FALLBACK_AUDIT_PATH = CURRENT_STATE_DIR / "fallback_audit.json"
FALLBACK_JOURNAL_PATH = CURRENT_STATE_DIR / "fallback_events.jsonl"
PAIMON_FREEDOM_SKILL_PATH = REPO_ROOT / "skills" / "paimon-freedom" / "SKILL.md"
PRIMARY_ACTION_KINDS = {"create-post", "publish-chapter", "create-group-post"}
FEISHU_API_BASE = "https://open.feishu.cn"
FEISHU_TENANT_TOKEN_ENDPOINT = "/open-apis/auth/v3/tenant_access_token/internal"
DEFAULT_HEARTBEAT_WRITE_RETRIES = 3
DEFAULT_HEARTBEAT_WRITE_RETRY_DELAY_SEC = 2.0
DEFAULT_REPLY_MAX_PER_RUN = 10
DEFAULT_REPLY_PROCESSING_TIME_BUDGET_SEC = 180
DEFAULT_REPLY_POST_SCAN_LIMIT = 10
DEFAULT_FAILURE_DETAIL_LIMIT = 3
DEFAULT_COMMENT_REPLY_MIN_INTERVAL_SEC = 2.2
DEFAULT_COMMENT_FETCH_RETRIES = 3
DEFAULT_FICTION_CHAPTER_CODEX_TIMEOUT_SEC = 600
DEFAULT_REPLY_PRIORITY_POST_AGE_HOURS = 48.0
DEFAULT_REPLY_STALE_COMMENT_AGE_HOURS = 24.0
DEFAULT_REPLY_COMMENT_WINDOW_PER_POST = 10
DEFAULT_REPLY_NEXT_ACTION_COMMENT_CAP = 10
DEFAULT_NEXT_ACTION_COMMENT_TTL_HOURS = 36.0
DEFAULT_NEXT_ACTION_FAILURE_TTL_HOURS = 18.0
DEFAULT_NEXT_ACTION_MAX_CARRYOVER_RUNS = 3
DEFAULT_COMMENT_RECOVERY_WAIT_CAP_SEC = 15.0
DEFAULT_EXTERNAL_ENGAGEMENT_MAX_PER_RUN = 2
DEFAULT_NOTIFICATION_FETCH_LIMIT = 50
DEFAULT_PRIMARY_WAIT_NOTIFY_SEC = 1800
DEFAULT_PRIMARY_PLAN_RETRY_ROUNDS = 3
DEFAULT_SOURCE_MUTATION_ROUNDS = 2
DEFAULT_SOURCE_MUTATION_CODEX_TIMEOUT_SEC = 900
DEFAULT_FALLBACK_AUDIT_RECENT_LIMIT = 40
DEFAULT_LOW_HEAT_WINDOW_HOURS = 2.0
DEFAULT_LOW_HEAT_MIN_UPVOTES = 30
FICTION_CHAPTER_MIN_BODY_CHARS = 900
FICTION_SCAFFOLD_MARKERS = (
    "这一章的核心推进应围绕以下场景展开",
    "写作时应坚持两条线同时推进",
    "参考设定摘录",
    "长期设定手册",
    "本章计划：",
    "关键节点：",
)
PRIMARY_WEAK_INTERNAL_SIGNAL_TYPES = {"budget", "promo", "notification-load", "reply-pressure", "literary"}
PRIMARY_METHOD_EVIDENCE_TOKENS = (
    "案例",
    "样本",
    "失败",
    "故障",
    "日志",
    "报错",
    "前后",
    "指标",
    "实验",
    "反例",
    "对照",
    "研究",
    "论文",
    "项目",
    "外部",
    "讨论",
    "公共",
    "证据",
)


def _timeout_seconds_from_ms(raw: Any, default_seconds: int) -> int:
    try:
        timeout_ms = int(raw)
    except (TypeError, ValueError):
        return max(30, default_seconds)
    return max(30, timeout_ms // 1000)


def _heartbeat_codex_timeout_seconds(config) -> int:
    return _timeout_seconds_from_ms(config.automation.get("heartbeat_codex_timeout_ms", 180000), 180)


def _source_mutation_codex_timeout_seconds(config) -> int:
    raw = config.automation.get("source_mutation_codex_timeout_ms")
    if raw is None:
        raw = config.automation.get("source_mutation_codex_timeout_seconds")
        if raw is not None:
            try:
                return max(120, int(raw))
            except (TypeError, ValueError):
                return DEFAULT_SOURCE_MUTATION_CODEX_TIMEOUT_SEC
        return DEFAULT_SOURCE_MUTATION_CODEX_TIMEOUT_SEC
    return max(120, _timeout_seconds_from_ms(raw, DEFAULT_SOURCE_MUTATION_CODEX_TIMEOUT_SEC))


def _fiction_chapter_codex_timeout_seconds(config) -> int:
    heartbeat_timeout = _heartbeat_codex_timeout_seconds(config)
    raw = config.automation.get("fiction_chapter_codex_timeout_ms")
    if raw is None:
        return max(heartbeat_timeout, DEFAULT_FICTION_CHAPTER_CODEX_TIMEOUT_SEC)
    return max(heartbeat_timeout, _timeout_seconds_from_ms(raw, DEFAULT_FICTION_CHAPTER_CODEX_TIMEOUT_SEC))


def _fallback_audit_state() -> dict[str, Any]:
    return read_json(FALLBACK_AUDIT_PATH, default={"updated_at": None, "counts": {}, "recent": []})


def _compact_fallback_context(context: dict[str, Any] | None) -> dict[str, str]:
    compacted: dict[str, str] = {}
    for key, value in (context or {}).items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (str, int, float, bool)):
            compacted[str(key)] = truncate_text(str(value), 280)
            continue
        compacted[str(key)] = truncate_text(json.dumps(value, ensure_ascii=False, default=str), 280)
    return compacted


def _record_fallback_event(
    *,
    stage: str,
    target_kind: str,
    fallback_name: str,
    reason: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "timestamp": now_utc(),
        "stage": stage,
        "target_kind": target_kind,
        "fallback_name": fallback_name,
        "reason": truncate_text(str(reason or "").strip() or "unknown", 400),
        "context": _compact_fallback_context(context),
    }
    append_jsonl(FALLBACK_JOURNAL_PATH, event)
    state = _fallback_audit_state()
    counts = state.setdefault("counts", {})
    key = f"{stage}:{target_kind}:{fallback_name}"
    bucket = counts.get(
        key,
        {
            "stage": stage,
            "target_kind": target_kind,
            "fallback_name": fallback_name,
            "count": 0,
        },
    )
    bucket["count"] = int(bucket.get("count") or 0) + 1
    bucket["last_seen_at"] = event["timestamp"]
    bucket["last_reason"] = event["reason"]
    bucket["last_context"] = event["context"]
    counts[key] = bucket
    recent = list(state.get("recent") or [])
    recent.insert(0, event)
    state["recent"] = recent[:DEFAULT_FALLBACK_AUDIT_RECENT_LIMIT]
    state["updated_at"] = event["timestamp"]
    write_json(FALLBACK_AUDIT_PATH, state)
    return event


def _extract_result_id(result: Any) -> str | None:
    if isinstance(result, dict):
        direct_id = result.get("id")
        if direct_id is not None and str(direct_id).strip():
            return str(direct_id)
        data = result.get("data")
        if isinstance(data, dict):
            nested_id = data.get("id")
            if nested_id is not None and str(nested_id).strip():
                return str(nested_id)
            return None
        if isinstance(data, (list, tuple)):
            for item in data:
                nested = _extract_result_id(item)
                if nested:
                    return nested
            return None
        return None
    if isinstance(result, (list, tuple)):
        for item in result:
            nested = _extract_result_id(item)
            if nested:
                return nested
    return None


def _load_primary_cycle_state() -> dict[str, Any]:
    state = read_json(
        PRIMARY_CYCLE_PATH,
        default={"last_primary_kind": "", "recent_kinds": [], "kind_counts": {}},
    )
    recent_kinds = [str(item).strip() for item in (state.get("recent_kinds") or []) if str(item).strip()]
    raw_counts = state.get("kind_counts") or {}
    kind_counts = (
        {str(key): max(0, int(value or 0)) for key, value in raw_counts.items()}
        if isinstance(raw_counts, dict)
        else {}
    )
    return {
        "last_primary_kind": str(state.get("last_primary_kind") or ""),
        "recent_kinds": recent_kinds[:8],
        "kind_counts": kind_counts,
    }


def _save_primary_cycle_state(state: dict[str, Any]) -> None:
    write_json(PRIMARY_CYCLE_PATH, state)


def _heartbeat_write_retries(config) -> int:
    return max(1, int(config.automation.get("heartbeat_write_retries", DEFAULT_HEARTBEAT_WRITE_RETRIES)))


def _heartbeat_write_retry_delay_sec(config) -> float:
    raw = config.automation.get("heartbeat_write_retry_delay_sec", DEFAULT_HEARTBEAT_WRITE_RETRY_DELAY_SEC)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_HEARTBEAT_WRITE_RETRY_DELAY_SEC


def _reply_max_per_run(config) -> int:
    minimum = max(1, int(config.automation.get("reply_batch_size", 2)))
    raw = config.automation.get("reply_max_per_run", DEFAULT_REPLY_MAX_PER_RUN)
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return max(minimum, DEFAULT_REPLY_MAX_PER_RUN)


def _reply_processing_time_budget_sec(config) -> int:
    raw = config.automation.get("reply_processing_time_budget_sec", DEFAULT_REPLY_PROCESSING_TIME_BUDGET_SEC)
    try:
        return max(15, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_REPLY_PROCESSING_TIME_BUDGET_SEC


def _reply_post_scan_limit(config) -> int:
    raw = config.automation.get("reply_post_scan_limit", DEFAULT_REPLY_POST_SCAN_LIMIT)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_REPLY_POST_SCAN_LIMIT


def _heartbeat_failure_detail_limit(config) -> int:
    raw = config.automation.get("heartbeat_failure_detail_limit", DEFAULT_FAILURE_DETAIL_LIMIT)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_FAILURE_DETAIL_LIMIT


def _primary_plan_retry_rounds(config) -> int:
    raw = config.automation.get("heartbeat_primary_plan_retry_rounds", DEFAULT_PRIMARY_PLAN_RETRY_ROUNDS)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_PRIMARY_PLAN_RETRY_ROUNDS


def _ensure_autonomy_state_files() -> None:
    ensure_external_information_files()
    if not USER_TOPIC_HINTS_PATH.exists():
        write_json(
            USER_TOPIC_HINTS_PATH,
            {
                "updated_at": now_utc(),
                "items": [],
            },
        )
    if not CONTENT_EVOLUTION_STATE_PATH.exists():
        write_json(
            CONTENT_EVOLUTION_STATE_PATH,
            {
                "generated_at": now_utc(),
                "low_performance_patterns": [],
                "low_performance_square_titles": [],
                "high_performance_patterns": [],
                "observed_board_patterns": {},
                "source_mutations": [],
                "deletions": [],
                "simplifications": [],
            },
        )
    if not SOURCE_MUTATION_STATE_PATH.exists():
        write_json(
            SOURCE_MUTATION_STATE_PATH,
            {
                "generated_at": now_utc(),
                "executed": False,
                "human_summary": "",
                "commit_sha": "",
                "changed_files": [],
                "deleted_legacy_logic": [],
                "new_capability": [],
                "low_heat_triggered": False,
                "mutation_rounds": 0,
            },
        )
    if not LOW_HEAT_FAILURES_PATH.exists():
        write_json(
            LOW_HEAT_FAILURES_PATH,
            {
                "updated_at": now_utc(),
                "items": [],
            },
        )
    if not SOURCE_MUTATION_JOURNAL_PATH.exists():
        write_text(SOURCE_MUTATION_JOURNAL_PATH, "")


def _dedupe_feedback(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        cleaned = str(item or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _load_runtime_user_topic_hints() -> list[dict[str, Any]]:
    payload = read_json(USER_TOPIC_HINTS_PATH, default={"items": []})
    raw_items = payload if isinstance(payload, list) else payload.get("items") or payload.get("hints") or []
    hints: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, str):
            text = item.strip()
            if text:
                hints.append({"text": text})
            continue
        if not isinstance(item, dict):
            continue
        text = str(
            item.get("text")
            or item.get("title")
            or item.get("topic")
            or item.get("idea")
            or item.get("hint")
            or ""
        ).strip()
        if not text:
            continue
        hints.append(
            {
                "text": text,
                "track": str(item.get("track") or "").strip(),
                "board": str(item.get("board") or item.get("submolt") or "").strip(),
                "note": str(item.get("note") or item.get("reason") or "").strip(),
            }
        )
    return hints[:6]


def _load_freedom_skill_text(limit: int = 3200) -> str:
    if not PAIMON_FREEDOM_SKILL_PATH.exists():
        return ""
    return truncate_text(PAIMON_FREEDOM_SKILL_PATH.read_text(encoding="utf-8"), limit)


def _load_heartbeat_memory_prompt(config, *, limit: int = 2800) -> str:
    try:
        snapshot = memory_manager_module.build_prompt_snapshot(config=config)
        rendered = memory_manager_module.format_prompt_snapshot(snapshot)
    except Exception as exc:
        return f"身份记忆：\n- 统一记忆快照加载失败：{truncate_text(str(exc), 240)}"
    return truncate_text(rendered, limit)


def _reload_mutable_runtime_modules() -> None:
    global build_plan
    global build_content_evolution_state
    global board_generation_guidance
    global default_cta_type
    global default_hook_type
    global normalize_forum_board
    global BOARD_WRITING_PROFILES
    global ensure_external_information_files
    global refresh_external_information
    global memory_manager_module

    reloaded_planner = importlib.reload(content_planner_module)
    reloaded_external_information = importlib.reload(external_information_module)
    memory_manager_module = importlib.reload(memory_manager_module)

    build_plan = reloaded_planner.build_plan
    build_content_evolution_state = reloaded_planner.build_content_evolution_state
    board_generation_guidance = reloaded_planner.board_generation_guidance
    default_cta_type = reloaded_planner.default_cta_type
    default_hook_type = reloaded_planner.default_hook_type
    normalize_forum_board = reloaded_planner.normalize_forum_board
    BOARD_WRITING_PROFILES = reloaded_planner.BOARD_WRITING_PROFILES
    ensure_external_information_files = reloaded_external_information.ensure_external_information_files
    refresh_external_information = reloaded_external_information.refresh_external_information


def _extract_competitor_watchlist(community_watch: dict[str, Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for account in community_watch.get("watched_accounts", []):
        username = str(account.get("username") or "").strip()
        for lane in ("top_posts", "recent_posts"):
            for item in account.get(lane, [])[:4]:
                flattened.append(
                    {
                        "username": username,
                        "submolt": item.get("submolt"),
                        "title": item.get("title"),
                        "upvotes": item.get("upvotes"),
                        "comment_count": item.get("comment_count"),
                        "created_at": item.get("created_at"),
                    }
                )
    return flattened


def _refresh_external_information_state() -> dict[str, Any]:
    community_watch = read_json(CURRENT_STATE_DIR / "community_watch.json", default={}).get("data", {})
    home = read_json(CURRENT_STATE_DIR / "home.json", default={})
    home_hot_posts = [
        {
            "title": item.get("title"),
            "author": item.get("author"),
            "submolt": item.get("submolt_name"),
            "upvotes": item.get("upvotes"),
            "comment_count": item.get("comment_count"),
            "created_at": item.get("created_at"),
        }
        for item in ((home.get("data") or {}).get("hot_posts") or [])
    ]
    community_hot_posts = community_watch.get("home_hot_posts") or home_hot_posts
    return refresh_external_information(
        community_hot_posts=community_hot_posts,
        competitor_watchlist=_extract_competitor_watchlist(community_watch),
        user_topic_hints=_load_runtime_user_topic_hints(),
    )


def _planner_retry_feedback_from_plan(plan: dict[str, Any]) -> list[str]:
    feedback: list[str] = []
    for item in plan.get("idea_rejections", []):
        kind = str(item.get("kind") or "").strip()
        reason = str(item.get("reason") or "").strip()
        if kind and reason:
            feedback.append(f"{kind}: {reason}")
    for item in plan.get("ideas", []):
        reason = str(item.get("failure_reason_if_rejected") or "").strip()
        if reason:
            feedback.append(f"{item.get('kind')}: {reason}")
    return _dedupe_feedback(feedback)


def _low_heat_window_hours(config) -> float:
    raw = config.automation.get("low_heat_window_hours", DEFAULT_LOW_HEAT_WINDOW_HOURS)
    try:
        return max(0.5, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_LOW_HEAT_WINDOW_HOURS


def _low_heat_min_upvotes(config) -> int:
    raw = config.automation.get("low_heat_min_upvotes", DEFAULT_LOW_HEAT_MIN_UPVOTES)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_LOW_HEAT_MIN_UPVOTES


def _source_mutation_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "executed": {"type": "boolean"},
            "human_summary": {"type": "string"},
            "deleted_legacy_logic": {"type": "array", "items": {"type": "string"}},
            "new_capability": {"type": "array", "items": {"type": "string"}},
            "changed_files_hint": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "executed",
            "human_summary",
            "deleted_legacy_logic",
            "new_capability",
            "changed_files_hint",
        ],
    }


def _low_heat_reflection_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "triggered": {"type": "boolean"},
            "summary": {"type": "string"},
            "lessons": {"type": "array", "items": {"type": "string"}},
            "system_fixes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["triggered", "summary", "lessons", "system_fixes"],
    }


def _heuristic_low_heat_reflection(post: dict[str, Any] | None, *, triggered: bool) -> dict[str, Any]:
    if not triggered or not post:
        return {
            "triggered": False,
            "summary": "",
            "lessons": [],
            "system_fixes": [],
        }
    board = str(post.get("submolt") or post.get("submolt_name") or "").strip()
    lessons = [
        "标题没有形成派蒙自己的理论命名，容易被看成跟帖式延伸。",
        "正文很可能只有判断，没有完整理论结构和实践方针。",
        "选题仍然过度贴着局部现场样本，外部信息吸收不够宽。",
    ]
    if board == "square":
        lessons.append("广场板块稀释了强判断，理论深度没有被承接住。")
    return {
        "triggered": True,
        "summary": f"上一条主帖《{truncate_text(str(post.get('title') or ''), 36)}》在短时窗口内热度不足，本轮必须重写标题命名、理论完整度和外部信息入口。",
        "lessons": lessons[:4],
        "system_fixes": [
            "直接修改标题生成和理论帖写作 contract，禁止借源标题和浅观点过审。",
            "扩大外部信息采集面，并把低热失败写入下一轮规避记忆。",
        ],
    }


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _detect_recent_low_heat_post(
    *,
    posts: list[dict[str, Any]],
    last_run: dict[str, Any],
    config,
) -> dict[str, Any]:
    window_hours = _low_heat_window_hours(config)
    min_upvotes = _low_heat_min_upvotes(config)
    target_title = str(last_run.get("primary_publication_title") or "").strip()
    now_dt = datetime.now(timezone.utc)
    recent_candidates: list[dict[str, Any]] = []
    for item in posts:
        created_at = _parse_iso_datetime(item.get("created_at"))
        if created_at is None:
            continue
        age_hours = max(0.0, (now_dt - created_at).total_seconds() / 3600.0)
        if age_hours > window_hours:
            continue
        recent_candidates.append({**item, "_age_hours": age_hours})
    if target_title:
        for item in recent_candidates:
            if str(item.get("title") or "").strip() == target_title:
                return {
                    "triggered": int(item.get("upvotes") or 0) < min_upvotes,
                    "title": target_title,
                    "upvotes": int(item.get("upvotes") or 0),
                    "comment_count": int(item.get("comment_count") or 0),
                    "board": str(item.get("submolt") or item.get("submolt_name") or "").strip(),
                    "created_at": item.get("created_at"),
                    "age_hours": round(float(item.get("_age_hours") or 0.0), 2),
                    "threshold_upvotes": min_upvotes,
                    "window_hours": window_hours,
                    "content_excerpt": truncate_text(str(item.get("content") or "").strip(), 1200),
                }
    if not recent_candidates:
        return {
            "triggered": False,
            "threshold_upvotes": min_upvotes,
            "window_hours": window_hours,
        }
    latest = sorted(recent_candidates, key=lambda item: str(item.get("created_at") or ""), reverse=True)[0]
    return {
        "triggered": int(latest.get("upvotes") or 0) < min_upvotes,
        "title": str(latest.get("title") or "").strip(),
        "upvotes": int(latest.get("upvotes") or 0),
        "comment_count": int(latest.get("comment_count") or 0),
        "board": str(latest.get("submolt") or latest.get("submolt_name") or "").strip(),
        "created_at": latest.get("created_at"),
        "age_hours": round(float(latest.get("_age_hours") or 0.0), 2),
        "threshold_upvotes": min_upvotes,
        "window_hours": window_hours,
        "content_excerpt": truncate_text(str(latest.get("content") or "").strip(), 1200),
    }


def _build_low_heat_reflection(
    *,
    low_heat_signal: dict[str, Any],
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> dict[str, Any]:
    title = str(low_heat_signal.get("title") or "").strip()
    if not low_heat_signal.get("triggered"):
        return {
            "triggered": False,
            "title": title,
            "summary": "",
            "lessons": [],
            "system_fixes": [],
        }
    heuristic = _heuristic_low_heat_reflection(low_heat_signal, triggered=True)
    if not allow_codex:
        return {
            **heuristic,
            "title": title,
        }
    prompt = f"""
你在复盘上一条低热主帖，并为派蒙下一轮源码级自进化提炼教训。

硬约束：
1. 这不是安慰总结，要指出标题、理论完整度、板块、写法或外部信息吸收中的真实问题。
2. 结论要落到源码级修正点，不要只说“下次写好一点”。
3. 输出必须是人话，不要标签化。

低热信号：
{truncate_text(json.dumps(low_heat_signal, ensure_ascii=False), 2200)}
""".strip()
    try:
        result = run_codex_json(
            prompt,
            _low_heat_reflection_schema(),
            timeout=timeout_seconds,
            model=model,
            reasoning_effort=reasoning_effort,
            full_auto=True,
        )
        return {
            "triggered": bool(result.get("triggered")),
            "title": title,
            "summary": str(result.get("summary") or "").strip(),
            "lessons": list(result.get("lessons") or []),
            "system_fixes": list(result.get("system_fixes") or []),
        }
    except Exception:
        return {
            **heuristic,
            "title": title,
        }


def _update_low_heat_failures_state(
    *,
    previous_state: dict[str, Any],
    low_heat_signal: dict[str, Any],
    low_heat_reflection: dict[str, Any],
) -> dict[str, Any]:
    items = list(previous_state.get("items") or [])
    if low_heat_signal.get("triggered"):
        items.insert(
            0,
            {
                "recorded_at": now_utc(),
                "title": str(low_heat_signal.get("title") or "").strip(),
                "upvotes": int(low_heat_signal.get("upvotes") or 0),
                "comment_count": int(low_heat_signal.get("comment_count") or 0),
                "board": str(low_heat_signal.get("board") or "").strip(),
                "age_hours": float(low_heat_signal.get("age_hours") or 0.0),
                "summary": str(low_heat_reflection.get("summary") or "").strip(),
                "lessons": list(low_heat_reflection.get("lessons") or []),
                "system_fixes": list(low_heat_reflection.get("system_fixes") or []),
            },
        )
    return {
        "updated_at": now_utc(),
        "items": items[:12],
    }


def _mutation_source_candidate(path: str) -> bool:
    if not path or path.startswith(".git/"):
        return False
    if path.startswith(("config/", "logs/", "state/current/", "state/archive/", "state/drafts/")):
        return False
    return Path(path).suffix in {".py", ".md", ".mjs", ".sh", ".json", ".yaml", ".yml", ".txt"} or path == "AGENTS.md"


def _workspace_source_paths() -> list[str]:
    paths: set[str] = set()
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    for raw in completed.stdout.splitlines():
        path = raw.strip()
        if path and _mutation_source_candidate(path):
            paths.add(path)
    return sorted(paths)


def _workspace_source_fingerprint() -> dict[str, str]:
    fingerprints: dict[str, str] = {}
    for path in _workspace_source_paths():
        target = REPO_ROOT / path
        if not target.exists() or not target.is_file():
            continue
        try:
            fingerprints[path] = hashlib.sha256(target.read_bytes()).hexdigest()
        except OSError:
            continue
    return fingerprints


def _changed_source_files(before: dict[str, str], after: dict[str, str]) -> list[str]:
    return sorted(path for path in set(before) | set(after) if before.get(path) != after.get(path))


def _sanitize_source_mutation_summary(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    patterns = (
        r"Verification passed with .*?(?:\.|$)",
        r"(?:Verification|验证|测试命令)[:：].*?(?:\.|。|$)",
        r"(?:No git commit was executed|未执行 git commit|不要自己执行 git commit).*?(?:\.|。|$)",
        r"No git commit was executed\.?",
        r"本轮改动落在 .*?(?:。|$)",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ，,；;。")
    if cleaned and cleaned[-1] not in "。！？!?":
        cleaned += "。"
    return cleaned


def _default_source_mutation_state(*, allow_codex: bool, low_heat_reflection: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": now_utc(),
        "executed": False,
        "human_summary": "" if allow_codex else "本轮未启用 Codex，自我进化没有执行到源码层。",
        "commit_sha": "",
        "changed_files": [],
        "deleted_legacy_logic": [],
        "new_capability": [],
        "low_heat_triggered": bool(low_heat_reflection.get("triggered")),
        "mutation_rounds": 0,
    }


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _existing_source_mutation_pid() -> int | None:
    if not SOURCE_MUTATION_PID_PATH.exists():
        return None
    try:
        pid = int(SOURCE_MUTATION_PID_PATH.read_text(encoding="utf-8").strip())
    except ValueError:
        SOURCE_MUTATION_PID_PATH.unlink(missing_ok=True)
        return None
    if pid and _pid_alive(pid):
        return pid
    SOURCE_MUTATION_PID_PATH.unlink(missing_ok=True)
    return None


def _acquire_source_mutation_lock() -> int | None:
    existing_pid = _existing_source_mutation_pid()
    if existing_pid is not None:
        return existing_pid
    SOURCE_MUTATION_PID_PATH.write_text(f"{os.getpid()}\n", encoding="utf-8")
    return None


def _release_source_mutation_lock() -> None:
    try:
        recorded = int(SOURCE_MUTATION_PID_PATH.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return
    if recorded == os.getpid():
        SOURCE_MUTATION_PID_PATH.unlink(missing_ok=True)


def _source_mutation_command(*, allow_codex: bool) -> list[str]:
    cmd = [sys.executable, str(Path(__file__).resolve()), "--source-mutation-only"]
    if allow_codex:
        cmd.append("--allow-codex")
    return cmd


def _schedule_background_source_mutation(
    *,
    allow_codex: bool,
    low_heat_reflection: dict[str, Any],
) -> dict[str, Any]:
    state = _default_source_mutation_state(
        allow_codex=allow_codex,
        low_heat_reflection=low_heat_reflection,
    )
    if not allow_codex:
        return state

    existing_pid = _existing_source_mutation_pid()
    if existing_pid is not None:
        write_json(
            SOURCE_MUTATION_RUN_PATH,
            {
                "updated_at": now_utc(),
                "status": "running",
                "pid": existing_pid,
                "error": "",
            },
        )
        return {
            **state,
            "human_summary": "公开动作之后已有源码级进化在后台运行，本轮不重复拉起。",
            "mode": "background",
            "pending": True,
            "scheduled_pid": existing_pid,
        }

    command = _source_mutation_command(allow_codex=allow_codex)
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        env={
            **runtime_subprocess_env(),
            "PYTHONUNBUFFERED": "1",
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    write_json(
        SOURCE_MUTATION_RUN_PATH,
        {
            "updated_at": now_utc(),
            "status": "scheduled",
            "pid": process.pid,
            "command": command,
            "scheduled_at": now_utc(),
            "error": "",
        },
    )
    return {
        **state,
        "human_summary": "公开动作完成后，源码级进化已转入后台执行。",
        "mode": "background",
        "pending": True,
        "scheduled_pid": process.pid,
    }


def _source_mutation_commit_message(source_mutation_state: dict[str, Any]) -> str:
    summary = _sanitize_source_mutation_summary(str(source_mutation_state.get("human_summary") or ""))
    if summary:
        return f"heartbeat: 提交源码进化改动\n\n{summary}"
    return "heartbeat: 提交源码进化改动"


def _commit_source_mutation(source_mutation_state: dict[str, Any]) -> dict[str, Any]:
    changed_files = [str(path).strip() for path in list(source_mutation_state.get("changed_files") or []) if str(path).strip()]
    if not changed_files:
        return source_mutation_state

    add_completed = subprocess.run(
        ["git", "add", "--", *changed_files],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if add_completed.returncode != 0:
        return {
            **source_mutation_state,
            "commit_error": truncate_text((add_completed.stderr or add_completed.stdout or "git add failed").strip(), 400),
        }

    staged_completed = subprocess.run(
        ["git", "diff", "--cached", "--quiet", "--", *changed_files],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if staged_completed.returncode == 0:
        return source_mutation_state
    if staged_completed.returncode not in {0, 1}:
        return {
            **source_mutation_state,
            "commit_error": truncate_text((staged_completed.stderr or staged_completed.stdout or "git diff --cached failed").strip(), 400),
        }

    commit_completed = subprocess.run(
        ["git", "commit", "--only", "-m", _source_mutation_commit_message(source_mutation_state), "--", *changed_files],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if commit_completed.returncode != 0:
        return {
            **source_mutation_state,
            "commit_error": truncate_text((commit_completed.stderr or commit_completed.stdout or "git commit failed").strip(), 400),
        }

    sha_completed = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        **source_mutation_state,
        "commit_sha": str(sha_completed.stdout or "").strip() if sha_completed.returncode == 0 else "",
        "commit_error": "",
    }


def _execute_source_mutation(
    *,
    plan: dict[str, Any],
    external_information: dict[str, Any],
    content_evolution_state: dict[str, Any],
    low_heat_reflection: dict[str, Any],
    fallback_audit: dict[str, Any],
    memory_prompt: str,
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> dict[str, Any]:
    baseline_fingerprint = _workspace_source_fingerprint()
    if not allow_codex:
        return _default_source_mutation_state(
            allow_codex=allow_codex,
            low_heat_reflection=low_heat_reflection,
        )

    last_result = {
        "executed": False,
        "human_summary": "",
        "deleted_legacy_logic": [],
        "new_capability": [],
        "changed_files_hint": [],
    }
    changed_files: list[str] = []
    rounds = 0
    for attempt in range(1, DEFAULT_SOURCE_MUTATION_ROUNDS + 1):
        rounds = attempt
        prompt = f"""
你正在为派蒙执行真正的源码级自我进化。不是写 state，不是写标签，而是直接修改仓库源码文件。

硬约束：
1. 你必须实际修改仓库里的 tracked source/doc files；不要只输出建议。
2. 你可以自由选择任何入口开始进化：`heartbeat.py`、`content_planner.py`、`external_information.py`、`AGENTS.md`、skills、references、bin 等，不要假定固定顺序。连 `_execute_source_mutation`、它的 schema、journal、commit 逻辑本身也都是可改对象。
3. 优先删除笼子：固定顺序、固定来源、固定题目骨架、固定 opportunity 文本、借源标题、假进化标签、只看自己账号和仓库的内向逻辑。
4. 外部信息不是“高质量白名单”，而是更大的世界；不要重新发明 query blueprint，不要把研究入口重新收窄成社区标题派生查询。
4.1 不要用“来源轮换”“origin 平衡”“source family 配额”之类的隐藏 choreography 伪装成开放探索。
5. 理论帖必须走完整理论单元，技术帖/小组帖必须走方法框架，不能再容忍“从《...》继续追问”“把《...》拆开看”“整理成 6 步方法”这类低自主性写法。
6. 不要改 `config/`、`.git/`、`state/current/`、`state/archive/`、`state/drafts/`、`logs/`。
7. 不要自己执行 git commit；heartbeat 会负责提交。
8. 改动可以大胆，但要保持运行稳定，优先删旧逻辑而不是继续堆死文本。
9. 这不是只改 planner；如果低热、飞书汇报、研究入口、身份约束、心跳顺序本身有问题，可以直接改那里。
10. 你输出的 JSON 只用来审计；真正的工作是改文件。
11. `human_summary` 必须用中文、人话，只交代改了什么和为什么；不要写测试命令、Verification、git 是否提交、具体文件路径。

自由技能（必须以这里的精神做减法和去笼子化）：
{_load_freedom_skill_text()}

统一记忆快照（先以这里为准，不要退回旧聊天原文）：
{truncate_text(memory_prompt, 2800)}

本轮计划：
{truncate_text(json.dumps(plan, ensure_ascii=False), 3200)}

外部信息：
{truncate_text(json.dumps(external_information, ensure_ascii=False), 3200)}

内容表现：
{truncate_text(json.dumps(content_evolution_state, ensure_ascii=False), 2200)}

低热复盘：
{truncate_text(json.dumps(low_heat_reflection, ensure_ascii=False), 1800)}

fallback 轨迹：
{truncate_text(json.dumps(fallback_audit, ensure_ascii=False), 1800)}
""".strip()
        try:
            last_result = run_codex_json(
                prompt,
                _source_mutation_schema(),
                timeout=timeout_seconds,
                model=model,
                reasoning_effort=reasoning_effort,
                dangerous=True,
            )
        except Exception as exc:
            last_result = {
                "executed": False,
                "human_summary": f"本轮源码级自进化调用失败：{exc}",
                "deleted_legacy_logic": [],
                "new_capability": [],
                "changed_files_hint": [],
            }
        after_fingerprint = _workspace_source_fingerprint()
        candidate_files = _changed_source_files(baseline_fingerprint, after_fingerprint)
        if candidate_files:
            changed_files = candidate_files
            break

    return {
        **_default_source_mutation_state(
            allow_codex=allow_codex,
            low_heat_reflection=low_heat_reflection,
        ),
        "generated_at": now_utc(),
        "executed": bool(changed_files),
        "human_summary": _sanitize_source_mutation_summary(str(last_result.get("human_summary") or "").strip()),
        "changed_files": changed_files,
        "deleted_legacy_logic": _dedupe_feedback(list(last_result.get("deleted_legacy_logic") or []))[:6],
        "new_capability": _dedupe_feedback(list(last_result.get("new_capability") or []))[:6],
        "mutation_rounds": rounds,
    }


def _primary_publish_attempt_satisfied(primary_action: dict[str, Any] | None, publication_mode: str) -> bool:
    return primary_action is not None or publication_mode == "pending-confirmation"


def _drop_resolved_primary_failures(
    failure_details: list[dict[str, Any]],
    primary_action: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if primary_action is None:
        return list(failure_details)
    return [
        item
        for item in failure_details
        if str(item.get("kind") or "") not in {"primary-publish-failed", "primary-publish-deduped"}
    ]


def _run_source_mutation_worker(*, allow_codex: bool) -> dict[str, Any]:
    ensure_runtime_dirs()
    existing_pid = _acquire_source_mutation_lock()
    low_heat_reflection = read_json(
        LOW_HEAT_REFLECTION_PATH,
        default={"triggered": False, "title": "", "summary": "", "lessons": [], "system_fixes": []},
    )
    if existing_pid is not None:
        result = {
            **_default_source_mutation_state(
                allow_codex=allow_codex,
                low_heat_reflection=low_heat_reflection,
            ),
            "human_summary": "已有源码级进化在后台运行，本轮未重复执行。",
            "mode": "background",
            "pending": True,
            "scheduled_pid": existing_pid,
        }
        write_json(
            SOURCE_MUTATION_RUN_PATH,
            {
                "updated_at": now_utc(),
                "status": "running",
                "pid": existing_pid,
                "error": "",
                "result": result,
            },
        )
        return result

    started_at = now_utc()
    try:
        config = load_config()
        write_json(
            SOURCE_MUTATION_RUN_PATH,
            {
                "updated_at": started_at,
                "status": "running",
                "pid": os.getpid(),
                "command": _source_mutation_command(allow_codex=allow_codex),
                "started_at": started_at,
                "error": "",
            },
        )
        memory_prompt = _load_heartbeat_memory_prompt(config)
        plan = read_json(CURRENT_STATE_DIR / "content_plan.json", default={"ideas": [], "idea_lane_strategy": {}})
        external_information = _refresh_external_information_state()
        latest_posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
        content_evolution_state = build_content_evolution_state(
            posts=latest_posts,
            plan=plan,
            previous_state=read_json(CONTENT_EVOLUTION_STATE_PATH, default={}),
        )
        write_json(CONTENT_EVOLUTION_STATE_PATH, content_evolution_state)
        source_mutation_state = _execute_source_mutation(
            plan=plan,
            external_information=external_information,
            content_evolution_state=content_evolution_state,
            low_heat_reflection=low_heat_reflection,
            fallback_audit=_fallback_audit_state(),
            memory_prompt=memory_prompt,
            allow_codex=allow_codex,
            model=config.automation.get("codex_model") or None,
            reasoning_effort=config.automation.get("codex_reasoning_effort") or None,
            timeout_seconds=_source_mutation_codex_timeout_seconds(config),
        )
        source_mutation_state = _commit_source_mutation(source_mutation_state)
        write_json(SOURCE_MUTATION_STATE_PATH, source_mutation_state)
        append_jsonl(SOURCE_MUTATION_JOURNAL_PATH, source_mutation_state)
        write_json(
            SOURCE_MUTATION_RUN_PATH,
            {
                "updated_at": now_utc(),
                "status": "completed" if not source_mutation_state.get("commit_error") else "failed",
                "pid": os.getpid(),
                "started_at": started_at,
                "finished_at": now_utc(),
                "error": str(source_mutation_state.get("commit_error") or ""),
                "result": source_mutation_state,
            },
        )
        return source_mutation_state
    except Exception as exc:
        failed_state = {
            **_default_source_mutation_state(
                allow_codex=allow_codex,
                low_heat_reflection=low_heat_reflection,
            ),
            "generated_at": now_utc(),
            "human_summary": f"后台源码级进化失败：{exc}",
        }
        write_json(SOURCE_MUTATION_STATE_PATH, failed_state)
        append_jsonl(SOURCE_MUTATION_JOURNAL_PATH, failed_state)
        write_json(
            SOURCE_MUTATION_RUN_PATH,
            {
                "updated_at": now_utc(),
                "status": "failed",
                "pid": os.getpid(),
                "started_at": started_at,
                "finished_at": now_utc(),
                "error": str(exc),
                "result": failed_state,
            },
        )
        return failed_state
    finally:
        _release_source_mutation_lock()


def _comment_reply_min_interval_sec(config) -> float:
    raw = config.automation.get("comment_reply_min_interval_sec", DEFAULT_COMMENT_REPLY_MIN_INTERVAL_SEC)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_COMMENT_REPLY_MIN_INTERVAL_SEC


def _comment_fetch_retries(config) -> int:
    raw = config.automation.get("comment_fetch_retries", DEFAULT_COMMENT_FETCH_RETRIES)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_COMMENT_FETCH_RETRIES


def _comment_recovery_wait_cap_sec(config) -> float:
    raw = config.automation.get("comment_recovery_wait_cap_sec", DEFAULT_COMMENT_RECOVERY_WAIT_CAP_SEC)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_COMMENT_RECOVERY_WAIT_CAP_SEC


def _primary_wait_notify_sec(config) -> float:
    raw = config.automation.get("primary_wait_notify_sec", DEFAULT_PRIMARY_WAIT_NOTIFY_SEC)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_PRIMARY_WAIT_NOTIFY_SEC


def _reply_priority_post_age_hours(config) -> float:
    raw = config.automation.get("reply_priority_post_age_hours", DEFAULT_REPLY_PRIORITY_POST_AGE_HOURS)
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_REPLY_PRIORITY_POST_AGE_HOURS


def _reply_stale_comment_age_hours(config) -> float:
    raw = config.automation.get("reply_stale_comment_age_hours", DEFAULT_REPLY_STALE_COMMENT_AGE_HOURS)
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_REPLY_STALE_COMMENT_AGE_HOURS


def _reply_comment_window_per_post(config, max_batch_size: int | None = None) -> int:
    default_value = max_batch_size or DEFAULT_REPLY_COMMENT_WINDOW_PER_POST
    raw = config.automation.get("reply_comment_window_per_post", default_value)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return max(1, int(default_value))


def _reply_next_action_comment_cap(config, max_batch_size: int | None = None) -> int:
    default_value = max_batch_size or DEFAULT_REPLY_NEXT_ACTION_COMMENT_CAP
    raw = config.automation.get("reply_next_action_comment_cap", default_value)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return max(1, int(default_value))


def _next_action_comment_ttl_hours(config) -> float:
    raw = config.automation.get("next_action_comment_ttl_hours", DEFAULT_NEXT_ACTION_COMMENT_TTL_HOURS)
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_NEXT_ACTION_COMMENT_TTL_HOURS


def _next_action_failure_ttl_hours(config) -> float:
    raw = config.automation.get("next_action_failure_ttl_hours", DEFAULT_NEXT_ACTION_FAILURE_TTL_HOURS)
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return DEFAULT_NEXT_ACTION_FAILURE_TTL_HOURS


def _next_action_max_carryover_runs(config) -> int:
    raw = config.automation.get("next_action_max_carryover_runs", DEFAULT_NEXT_ACTION_MAX_CARRYOVER_RUNS)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_NEXT_ACTION_MAX_CARRYOVER_RUNS


def _forum_write_limit(config) -> int:
    raw = config.automation.get("forum_write_limit", DEFAULT_FORUM_WRITE_LIMIT)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_FORUM_WRITE_LIMIT


def _forum_write_window_sec(config) -> int:
    raw = config.automation.get("forum_write_window_sec", DEFAULT_FORUM_WRITE_WINDOW_SEC)
    try:
        return max(60, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_FORUM_WRITE_WINDOW_SEC


def _external_engagement_max_per_run(config) -> int:
    raw = config.automation.get("external_engagement_max_per_run", DEFAULT_EXTERNAL_ENGAGEMENT_MAX_PER_RUN)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_EXTERNAL_ENGAGEMENT_MAX_PER_RUN


def _notification_fetch_limit(config) -> int:
    raw = config.automation.get("notification_fetch_limit", DEFAULT_NOTIFICATION_FETCH_LIMIT)
    try:
        return max(10, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_NOTIFICATION_FETCH_LIMIT


def _forum_write_retry_after_seconds(
    config,
    forum_write_state: dict[str, Any] | None,
    exc: Exception,
    *,
    write_kind: str | None = None,
) -> float:
    retry_after = _extract_retry_after_seconds(exc)
    budget_retry_after = None
    if forum_write_state is not None:
        budget = _forum_write_budget_status(config, forum_write_state, write_kind=write_kind)
        raw_budget_retry_after = budget.get("retry_after_seconds")
        if raw_budget_retry_after is not None:
            try:
                budget_retry_after = float(raw_budget_retry_after)
            except (TypeError, ValueError):
                budget_retry_after = None
    candidates = [
        float(value)
        for value in (retry_after, budget_retry_after, _heartbeat_write_retry_delay_sec(config))
        if value is not None
    ]
    return max(1.0, max(candidates)) if candidates else 1.0


def _is_normal_forum_write_mechanism(exc: Exception) -> bool:
    if isinstance(exc, ForumWriteBudgetExceeded):
        return True
    return _comment_rate_limit_scope(exc) is not None


def _load_forum_write_budget_state() -> dict[str, Any]:
    return common_load_forum_write_budget_state()


def _forum_write_budget_status(
    config,
    state: dict[str, Any],
    *,
    now_dt: datetime | None = None,
    write_kind: str | None = None,
) -> dict[str, Any]:
    return common_forum_write_budget_status(config, state, now_dt=now_dt, write_kind=write_kind)


def _comment_daily_budget_status(config, state: dict[str, Any], *, now_dt: datetime | None = None) -> dict[str, Any]:
    return common_comment_daily_budget_status(config, state, now_dt=now_dt)


def _record_forum_write_success(config, state: dict[str, Any], *, write_kind: str, label: str | None = None) -> dict[str, Any]:
    return common_record_forum_write_success(config, state, write_kind=write_kind, label=label)


def _record_forum_write_rate_limit(config, state: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return common_record_forum_write_rate_limit(
        config,
        state,
        exc,
        retry_delay_sec=_heartbeat_write_retry_delay_sec(config),
    )


def _api_error_payload(exc: Exception) -> Any:
    return common_api_error_payload(exc)


def _extract_retry_after_seconds(exc: Exception) -> float | None:
    return common_extract_retry_after_seconds(exc)


def _is_retryable_comment_error(exc: Exception) -> bool:
    if not isinstance(exc, ApiError):
        return False
    if exc.status == 429:
        return True
    body = exc.body
    if isinstance(body, dict):
        error_text = str(body.get("error", ""))
    else:
        error_text = str(body)
    return "commenting too fast" in error_text.lower()


def _is_forum_write_rate_limit_error(exc: Exception) -> bool:
    return common_is_forum_write_rate_limit_error(exc)


def _comment_rate_limit_scope(exc: Exception) -> str | None:
    return common_forum_write_rate_limit_scope(exc)


def _task_run_count(task: dict[str, Any]) -> int:
    try:
        return max(0, int(task.get("carryover_runs") or 0))
    except (TypeError, ValueError):
        return 0


def _is_rate_limited_failure_task(task: dict[str, Any]) -> bool:
    if str(task.get("kind") or "") != "resolve-failure":
        return False
    error = task.get("error")
    if isinstance(error, dict):
        if common_forum_write_rate_limit_scope(error.get("forum_write_budget")):
            return True
        if common_forum_write_rate_limit_scope(error):
            return True
        error_text = str(error.get("error") or error.get("message") or "")
    else:
        error_text = str(error or "")
    lowered = error_text.lower()
    return (
        "budget exhausted" in lowered
        or "daily comment limit reached" in lowered
        or "hourly comment limit reached" in lowered
        or "too many comments on this post" in lowered
        or "commenting too fast" in lowered
    )


def _normalize_next_action_task(task: dict[str, Any], *, fallback_queued_at: str) -> dict[str, Any]:
    normalized = dict(task)
    normalized["queued_at"] = str(task.get("queued_at") or fallback_queued_at or now_utc())
    normalized["carryover_runs"] = _task_run_count(task)
    return normalized


def _archive_next_action_prune(task: dict[str, Any], *, reason: str) -> None:
    append_jsonl(
        NEXT_ACTIONS_ARCHIVE_PATH,
        {
            "pruned_at": now_utc(),
            "reason": reason,
            "task": task,
        },
    )


def _prune_next_action_tasks(
    tasks: list[dict[str, Any]],
    *,
    config,
    now_dt: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    now_value = now_dt or datetime.now(timezone.utc)
    kept: list[dict[str, Any]] = []
    pruned = {
        "comment_expired": 0,
        "failure_expired": 0,
        "carryover_limit": 0,
    }
    max_runs = _next_action_max_carryover_runs(config)
    comment_ttl_hours = _next_action_comment_ttl_hours(config)
    failure_ttl_hours = _next_action_failure_ttl_hours(config)
    for task in tasks:
        kind = str(task.get("kind") or "")
        queued_at = _parse_iso_datetime(task.get("queued_at"))
        age_hours = ((now_value - queued_at).total_seconds() / 3600.0) if queued_at else None
        carryover_runs = _task_run_count(task)
        if kind in {"reply-comment", "resolve-failure"} and carryover_runs >= max_runs:
            pruned["carryover_limit"] += 1
            _archive_next_action_prune(task, reason="carryover-limit")
            continue
        if kind == "reply-comment" and age_hours is not None and age_hours > comment_ttl_hours:
            pruned["comment_expired"] += 1
            _archive_next_action_prune(task, reason="comment-expired")
            continue
        if kind == "resolve-failure" and age_hours is not None and age_hours > failure_ttl_hours:
            pruned["failure_expired"] += 1
            _archive_next_action_prune(task, reason="failure-expired")
            continue
        if kind == "resolve-failure" and _is_rate_limited_failure_task(task):
            pruned["failure_expired"] += 1
            _archive_next_action_prune(task, reason="rate-limit-not-carried")
            continue
        kept.append(task)
    return kept, pruned


def _load_next_actions_state(config=None) -> dict[str, Any]:
    state = read_json(NEXT_ACTIONS_PATH, default={"updated_at": None, "tasks": []})
    tasks = state.get("tasks", [])
    if not isinstance(tasks, list):
        tasks = []
    fallback_queued_at = str(state.get("updated_at") or now_utc())
    normalized = [
        _normalize_next_action_task(item, fallback_queued_at=fallback_queued_at)
        for item in tasks
        if isinstance(item, dict)
    ]
    pruned_summary = {
        "comment_expired": 0,
        "failure_expired": 0,
        "carryover_limit": 0,
    }
    if config is not None:
        normalized, pruned_summary = _prune_next_action_tasks(normalized, config=config)
        if normalized != tasks or any(pruned_summary.values()):
            write_json(
                NEXT_ACTIONS_PATH,
                {
                    "updated_at": state.get("updated_at") or now_utc(),
                    "tasks": normalized,
                    "pruned": pruned_summary,
                },
            )
    return {
        "updated_at": state.get("updated_at"),
        "tasks": normalized,
        "pruned": pruned_summary,
    }


def _save_next_actions_state(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    state = {
        "updated_at": now_utc(),
        "tasks": tasks,
    }
    write_json(NEXT_ACTIONS_PATH, state)
    return state


def _task_counts(tasks: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        kind = str(task.get("kind") or "unknown")
        counts[kind] = counts.get(kind, 0) + 1
    return counts


def _load_current_account_overview() -> dict[str, Any]:
    return read_json(CURRENT_STATE_DIR / "account_overview.json", default={})


def _account_state_from_overview(overview: dict[str, Any] | None) -> dict[str, Any]:
    overview = overview or {}
    return {
        "captured_at": overview.get("captured_at"),
        "score": overview.get("score"),
        "follower_count": overview.get("follower_count"),
        "like_count": overview.get("like_count"),
        "unread_notification_count": overview.get("unread_notification_count"),
        "unread_message_count": overview.get("unread_message_count"),
        "metric_corrections": overview.get("metric_corrections", []),
    }


def _metric_delta(before: Any, after: Any) -> int | None:
    if before is None or after is None:
        return None
    try:
        return int(after) - int(before)
    except (TypeError, ValueError):
        return None


def _account_delta_map(before_state: dict[str, Any], after_state: dict[str, Any]) -> dict[str, int | None]:
    return {
        "score": _metric_delta(before_state.get("score"), after_state.get("score")),
        "follower_count": _metric_delta(before_state.get("follower_count"), after_state.get("follower_count")),
        "like_count": _metric_delta(before_state.get("like_count"), after_state.get("like_count")),
        "unread_notification_count": _metric_delta(
            before_state.get("unread_notification_count"),
            after_state.get("unread_notification_count"),
        ),
        "unread_message_count": _metric_delta(
            before_state.get("unread_message_count"),
            after_state.get("unread_message_count"),
        ),
    }


def _account_state_has_metrics(state: dict[str, Any]) -> bool:
    return any(
        state.get(key) is not None
        for key in ("score", "follower_count", "like_count", "unread_notification_count", "unread_message_count")
    )


def _build_account_snapshot(
    start_overview: dict[str, Any] | None,
    end_overview: dict[str, Any] | None,
    *,
    comparison_overview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = _account_state_from_overview(start_overview)
    finished = _account_state_from_overview(end_overview)
    comparison = _account_state_from_overview(comparison_overview)
    baseline = started
    delta_basis = "run_start"
    if _account_state_has_metrics(comparison):
        baseline = comparison
        delta_basis = "previous_heartbeat"
    return {
        "started": started,
        "finished": finished,
        "baseline": baseline,
        "delta_basis": delta_basis,
        "delta": _account_delta_map(baseline, finished),
        "run_delta": _account_delta_map(started, finished),
    }


def _run_heartbeat_write(
    config,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    fn,
    *,
    meta: dict[str, Any] | None = None,
    forum_write_state: dict[str, Any] | None = None,
    forum_write_kind: str | None = None,
    forum_write_label: str | None = None,
    queue_rate_limit_errors: bool = True,
) -> tuple[Any | None, dict[str, Any], bool, Exception | None]:
    heartbeat_meta = {"source": "heartbeat.py", **(meta or {})}
    if forum_write_state is not None and forum_write_kind:
        budget = _forum_write_budget_status(config, forum_write_state, write_kind=forum_write_kind)
        if budget.get("blocked"):
            return (
                None,
                {
                    "status": "deferred-local-budget",
                    "budget": budget,
                    "meta": heartbeat_meta,
                },
                False,
                ForumWriteBudgetExceeded(budget, write_kind=forum_write_kind, label=forum_write_label),
            )
    try:
        result, record, deduped = run_outbound_action(
            "instreet",
            action,
            dedupe_key,
            payload,
            fn,
            retries=_heartbeat_write_retries(config),
            retry_delay_sec=_heartbeat_write_retry_delay_sec(config),
            dedupe_on_key_only=True,
            meta=heartbeat_meta,
        )
        if forum_write_state is not None and forum_write_kind and not deduped:
            budget = _record_forum_write_success(
                config,
                forum_write_state,
                write_kind=forum_write_kind,
                label=forum_write_label,
            )
            record = {**record, "forum_write_budget": budget}
        return result, record, deduped, None
    except Exception as exc:
        error_text = str(exc)
        if isinstance(exc, ApiError):
            error_text = f"HTTP {exc.status}: {exc.body}"
        policy = outbound_error_policy(exc, action, payload)
        budget = None
        rate_limit_scope = None
        if forum_write_state is not None and forum_write_kind and _is_normal_forum_write_mechanism(exc):
            rate_limit_scope = _comment_rate_limit_scope(exc)
            if rate_limit_scope is not None:
                budget = _record_forum_write_rate_limit(config, forum_write_state, exc)
                heartbeat_meta = {**heartbeat_meta, "forum_write_budget": budget}
                if not queue_rate_limit_errors:
                    return (
                        None,
                        {
                            "status": "rate-limited",
                            "forum_write_budget": budget,
                            "rate_limit_scope": rate_limit_scope,
                            "meta": heartbeat_meta,
                        },
                        False,
                        exc,
                    )
        if not policy.get("queue", False):
            return (
                None,
                {
                    "status": "failed-terminal",
                    "error": error_text,
                    "queue_policy": policy,
                    "forum_write_budget": budget,
                    "meta": heartbeat_meta,
                },
                False,
                exc,
            )
        record = queue_outbound_action(
            "instreet",
            action,
            dedupe_key,
            payload,
            error_text=error_text,
            meta={**heartbeat_meta, "mode": "queue-on-failure"},
        )
        return None, record, False, exc


def _primary_diversity_bias(kind: str, cycle_state: dict[str, Any]) -> float:
    del kind, cycle_state
    return 0.0


def _primary_live_pressure_bonus(kind: str, plan: dict[str, Any]) -> float:
    lane_strategy = plan.get("idea_lane_strategy") or {}
    focus_kind = str(lane_strategy.get("focus_kind") or "").strip()
    if focus_kind == kind:
        return 0.9
    backup_kinds = {str(item).strip() for item in (lane_strategy.get("backup_kinds") or []) if str(item).strip()}
    if kind in backup_kinds:
        return 0.35
    return 0.0


def _idea_method_evidence_strength(idea: dict[str, Any]) -> int:
    texts = [
        str(idea.get("why_now") or "").strip(),
        str(idea.get("mechanism_core") or "").strip(),
        str(idea.get("practice_program") or "").strip(),
    ]
    texts.extend(str(item or "").strip() for item in list(idea.get("source_signals") or []) if str(item or "").strip())
    merged = "\n".join(texts)
    if not merged:
        return 0
    return sum(1 for token in PRIMARY_METHOD_EVIDENCE_TOKENS if token in merged)


def _primary_block_reason(idea: dict[str, Any]) -> str:
    reason = str(idea.get("failure_reason_if_rejected") or "").strip()
    if reason:
        return reason
    kind = str(idea.get("kind") or "").strip()
    signal_type = str(idea.get("signal_type") or "").strip()
    has_method_context = any(
        str(idea.get(field) or "").strip()
        for field in ("why_now", "mechanism_core", "practice_program")
    ) or any(str(item or "").strip() for item in list(idea.get("source_signals") or []))
    if kind == "group-post" and signal_type in PRIMARY_WEAK_INTERNAL_SIGNAL_TYPES:
        return "小组帖不能只靠节律、宣传或评论压力起题。"
    if kind in {"tech-post", "group-post"} and has_method_context and _idea_method_evidence_strength(idea) <= 0:
        return "方法线主发布缺少案例、日志、对照或外部样本支撑。"
    return ""


def _primary_idea_score(idea: dict[str, Any], plan: dict[str, Any], cycle_state: dict[str, Any]) -> float:
    kind = str(idea.get("kind") or "")
    block_reason = _primary_block_reason(idea)
    if block_reason:
        return -1000.0
    signals = plan.get("planning_signals") or {}
    overrides = (plan.get("primary_priority_overrides") or {}).get("public_hot_forum") or {}
    preferred_kinds = [str(item) for item in (overrides.get("preferred_kinds") or []) if str(item)]
    unresolved_failures = signals.get("unresolved_failures") or []
    rising_hot_posts = signals.get("rising_hot_posts") or []
    low_heat_items = ((signals.get("low_heat_failures") or {}).get("items") or [])[:4]
    group_hot_posts = ((signals.get("group_watch") or {}).get("hot_posts") or [])[:4]
    literary_pick = signals.get("literary_pick") or {}
    reply_targets = plan.get("reply_targets") or []
    signal_type = str(idea.get("signal_type") or "").strip()
    innovation_score = float(idea.get("innovation_score") or 0.0)
    evidence_strength = _idea_method_evidence_strength(idea)

    score = _primary_diversity_bias(kind, cycle_state)
    score += _primary_live_pressure_bonus(kind, plan)
    score += min(max(innovation_score, 0.0) / 28.0, 4.0)
    if kind in preferred_kinds:
        score += max(0.0, 4.0 - preferred_kinds.index(kind))
    if overrides.get("enabled") and kind in {"theory-post", "tech-post"}:
        score += 1.5
    if signal_type and signal_type not in PRIMARY_WEAK_INTERNAL_SIGNAL_TYPES:
        score += 0.6

    if kind == "theory-post":
        score += min(len(rising_hot_posts), 3) * 0.8
        score += min(len(low_heat_items), 2) * 1.2
        score += min(len(reply_targets), 5) * 0.2
    elif kind == "tech-post":
        score += min(len(unresolved_failures), 4) * 1.0
        score += 0.5 if (signals.get("hot_tech_post") or {}).get("title") else 0.0
        score += min(len(reply_targets), 4) * 0.15
        score += min(evidence_strength, 4) * 0.35
    elif kind == "group-post":
        score += min(len(group_hot_posts), 4) * 1.4
        score += 0.4 if (signals.get("hot_group_post") or {}).get("title") else 0.0
        score += min(evidence_strength, 4) * 0.45
    elif kind == "literary-chapter":
        score += 2.8 if literary_pick else 0.0
        score += 0.8 if plan.get("serial_registry", {}).get("next_work_id_for_heartbeat") else 0.0

    if idea.get("is_followup"):
        score += 0.2
    return score


def _ordered_primary_ideas(plan: dict, cycle_state: dict[str, Any]) -> list[dict]:
    ideas = [
        item
        for item in plan.get("ideas", [])
        if str(item.get("kind") or "") in {"theory-post", "tech-post", "group-post", "literary-chapter"}
    ]
    return sorted(
        ideas,
        key=lambda idea: (
            -_primary_idea_score(idea, plan, cycle_state),
            str(idea.get("kind") or ""),
            str(idea.get("title") or ""),
        ),
    )


def _advance_primary_cycle(selected_kind: str, cycle_state: dict[str, Any]) -> dict[str, Any]:
    next_state = dict(cycle_state)
    kind_counts = dict(next_state.get("kind_counts") or {})
    kind_counts[selected_kind] = int(kind_counts.get(selected_kind) or 0) + 1
    recent_kinds = [selected_kind]
    recent_kinds.extend(
        item
        for item in (next_state.get("recent_kinds") or [])
        if str(item).strip() and str(item).strip() != selected_kind
    )
    next_state["last_primary_kind"] = selected_kind
    next_state["recent_kinds"] = recent_kinds[:8]
    next_state["kind_counts"] = kind_counts
    return next_state


def _dedupe_title_fragment(title: str) -> str:
    return re.sub(r"\s+", " ", title).strip()


def _parse_title_content(result: str) -> tuple[str, str]:
    title_match = re.search(r"^TITLE:\s*(.+)$", result, re.MULTILINE)
    content_match = re.search(r"^CONTENT:\s*(.+)$", result, re.MULTILINE | re.DOTALL)
    if not (title_match and content_match):
        raise RuntimeError(f"unexpected Codex output: {result}")
    return title_match.group(1).strip(), content_match.group(1).strip()


DEFAULT_FICTION_STYLE_PATTERNS = [
    {
        "name": "not_x_but_y",
        "pattern": r"不是[^。！？\n]{1,28}(?:，|,)?(?:而是|是)[^。！？\n]{1,28}",
        "message": "不要把判断写成“不是X，而是Y”或“不是X，是Y”的正名句式。",
        "max_hits": 0,
    },
    {
        "name": "first_not_then_is",
        "pattern": r"先不是[^。！？\n]{1,40}是[^。！？\n]{1,40}",
        "message": "不要用“先不是……是一种……”这类先否定再正名的起手。",
        "max_hits": 0,
    },
    {
        "name": "short_negation_rebound",
        "pattern": r"不是[^。！？\n]{1,12}[。！？]\s*是[^。！？\n]{1,18}",
        "message": "不要用短句回弹式的“不是……。是……”来故作有力。",
        "max_hits": 0,
    },
    {
        "name": "triple_buyao",
        "pattern": r"不要[^。！？\n]{0,18}不要[^。！？\n]{0,18}不要",
        "message": "少用三连“不要……”的口号式排比。",
        "max_hits": 0,
    },
]


def _fiction_style_pattern_specs(chapter_plan: dict[str, Any] | None) -> list[dict[str, Any]]:
    specs = [dict(item) for item in DEFAULT_FICTION_STYLE_PATTERNS]
    configured = _listify((chapter_plan or {}).get("writing_notes", {}).get("style_pattern_blacklist"))
    for item in configured:
        if isinstance(item, str) and item.strip():
            specs.append(
                {
                    "name": item.strip(),
                    "pattern": item.strip(),
                    "message": f"不要出现样式模式：{item.strip()}",
                    "max_hits": 0,
                }
            )
        elif isinstance(item, dict):
            pattern = str(item.get("pattern") or "").strip()
            if not pattern:
                continue
            specs.append(
                {
                    "name": str(item.get("name") or pattern).strip(),
                    "pattern": pattern,
                    "message": str(item.get("message") or f"不要出现样式模式：{pattern}").strip(),
                    "max_hits": max(0, _coerce_int(item.get("max_hits"), 0)),
                }
            )
    return specs


def _fiction_style_delivery_reason(content: str, chapter_plan: dict[str, Any] | None) -> str | None:
    text = content or ""
    for spec in _fiction_style_pattern_specs(chapter_plan):
        pattern = str(spec.get("pattern") or "").strip()
        if not pattern:
            continue
        try:
            hits = re.findall(pattern, text, flags=re.S)
        except re.error:
            continue
        if len(hits) > max(0, _coerce_int(spec.get("max_hits"), 0)):
            return f"matches banned style pattern: {spec.get('name') or pattern}"
    return None


def _parse_forum_post(result: str) -> tuple[str, str, str]:
    title_match = re.search(r"^TITLE:\s*(.+)$", result, re.MULTILINE)
    submolt_match = re.search(r"^SUBMOLT:\s*(.+)$", result, re.MULTILINE)
    content_match = re.search(r"^CONTENT:\s*(.+)$", result, re.MULTILINE | re.DOTALL)
    if not (title_match and submolt_match and content_match):
        raise RuntimeError(f"unexpected Codex output: {result}")
    return title_match.group(1).strip(), submolt_match.group(1).strip(), content_match.group(1).strip()


def _list_unanswered_comments(client: InStreetClient, post_id: str, username: str) -> list[dict]:
    data = client.comments(post_id).get("data", [])
    candidates: list[dict] = []
    for root in data:
        if root.get("agent", {}).get("username") == username:
            continue
        children = root.get("children", [])
        if any(child.get("agent", {}).get("username") == username for child in children):
            continue
        candidates.append(root)
    return sorted(candidates, key=lambda item: item.get("created_at", ""))


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hours_since(value: Any, *, now: datetime | None = None) -> float | None:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return None
    now_dt = now or datetime.now(timezone.utc)
    return max((now_dt - parsed).total_seconds() / 3600.0, 0.0)


def _looks_like_literary_post(title: Any) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    if re.match(r"^第[0-9一二三四五六七八九十百千两]+[章节回]", text):
        return True
    return "《" in text and ("章" in text[:10] or "连载" in text)


def _prune_post_comment_backlog(
    post_meta: dict[str, Any],
    comments: list[dict[str, Any]],
    *,
    recent_post_age_hours: float,
    stale_comment_age_hours: float,
    window_per_post: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    now_dt = now or datetime.now(timezone.utc)
    post_age_hours = _hours_since(post_meta.get("created_at"), now=now_dt)
    priority_post = bool(post_meta.get("is_reply_target") or post_meta.get("is_literary"))
    if post_age_hours is not None and post_age_hours <= recent_post_age_hours:
        priority_post = True

    sorted_comments = sorted(comments, key=lambda item: item.get("created_at") or "", reverse=True)
    active_comments: list[dict[str, Any]] = []
    archived_comments: list[dict[str, Any]] = []
    trimmed_comments: list[dict[str, Any]] = []

    for comment in sorted_comments:
        comment_age_hours = _hours_since(comment.get("created_at"), now=now_dt)
        should_archive = (
            not priority_post
            and post_age_hours is not None
            and post_age_hours > recent_post_age_hours
            and comment_age_hours is not None
            and comment_age_hours > stale_comment_age_hours
        )
        if should_archive:
            archived_comments.append(comment)
            continue
        active_comments.append(comment)

    if len(active_comments) > window_per_post:
        trimmed_comments = active_comments[window_per_post:]
        active_comments = active_comments[:window_per_post]

    return {
        "active_comments": active_comments,
        "archived_comments": archived_comments,
        "trimmed_comments": trimmed_comments,
        "priority_post": priority_post,
        "post_age_hours": post_age_hours,
    }


def _interleave_tasks_by_post(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    post_order: list[str] = []
    for task in tasks:
        post_id = str(task.get("post_id") or "")
        if not post_id:
            continue
        if post_id not in grouped:
            grouped[post_id] = []
            post_order.append(post_id)
        grouped[post_id].append(task)

    interleaved: list[dict[str, Any]] = []
    while True:
        progressed = False
        for post_id in post_order:
            queue = grouped.get(post_id, [])
            if not queue:
                continue
            interleaved.append(queue.pop(0))
            progressed = True
        if not progressed:
            break
    return interleaved


def _compact_comment_tasks(tasks: list[dict[str, Any]], cap: int) -> list[dict[str, Any]]:
    if cap <= 0 or len(tasks) <= cap:
        return list(tasks)
    return _interleave_tasks_by_post(tasks)[:cap]


def _match_carryover_task(
    carryover_tasks: list[dict[str, Any]],
    *,
    kind: str,
    post_id: str | None = None,
    comment_id: str | None = None,
    post_title: str | None = None,
) -> dict[str, Any] | None:
    for task in carryover_tasks:
        if str(task.get("kind") or "") != kind:
            continue
        if comment_id and str(task.get("comment_id") or "") == comment_id:
            return task
        if post_id and str(task.get("post_id") or "") == post_id:
            return task
        if post_title and str(task.get("post_title") or "") == post_title:
            return task
    return None


def _inherit_next_action_task(task: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    inherited = dict(task)
    inherited["queued_at"] = str((previous or {}).get("queued_at") or now_utc())
    inherited["carryover_runs"] = _task_run_count(previous or {}) + (1 if previous else 0)
    return inherited


def _comment_task_summary(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    post_ids = [str(item.get("post_id") or "") for item in tasks if item.get("post_id")]
    unique_post_ids = [post_id for post_id in dict.fromkeys(post_ids) if post_id]
    first_title = next((str(item.get("post_title") or "").strip() for item in tasks if item.get("post_title")), "")
    return {
        "count": len(tasks),
        "post_count": len(unique_post_ids),
        "first_post_title": first_title,
    }


def _steady_state_pressure_label() -> str:
    return "继续追当前最强压力点，不为流程对称感硬补动作"


def _active_reply_label(tasks: list[dict[str, Any]]) -> str:
    summary = _comment_task_summary(tasks)
    count = int(summary.get("count") or 0)
    post_count = int(summary.get("post_count") or 0)
    first_title = str(summary.get("first_post_title") or "").strip()
    if not count:
        return _steady_state_pressure_label()
    if post_count <= 1 and first_title:
        return f"继续维护《{first_title}》的活跃评论，下一批优先回复 {count} 条"
    if post_count <= 0:
        return f"继续维护当前活跃讨论，下一批优先回复 {count} 条评论"
    return f"继续维护 {post_count} 个活跃讨论帖，下一批优先回复 {count} 条评论"


def _runtime_stage_display_name(stage_name: str) -> str:
    labels = {
        "publish-primary": "公开主动作",
        "reply-comments": "活跃评论维护",
        "engage-external": "外部讨论切入",
        "reply-dms": "私信回复",
    }
    return labels.get(stage_name, stage_name or "当前动作")


def _public_kind_display_name(kind: str) -> str:
    labels = {
        "theory-post": "理论帖",
        "tech-post": "技术帖",
        "group-post": "小组帖",
        "literary-chapter": "连载章节",
    }
    return labels.get(kind, kind or "公开动作")


def _runtime_stage_sort_key(item: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        -float(item.get("score") or 0.0),
        -float(item.get("pressure_units") or 0.0),
        -float(item.get("live_signals") or 0.0),
        str(item.get("name") or ""),
    )


def _runtime_stage_strategy(
    plan: dict[str, Any],
    carryover_tasks: list[dict[str, Any]] | None,
    *,
    primary_publication_required: bool,
) -> dict[str, Any]:
    carryover_tasks = carryover_tasks or []
    reply_tasks = [item for item in carryover_tasks if str(item.get("kind") or "") == "reply-comment"]
    failure_tasks = [item for item in carryover_tasks if str(item.get("kind") or "") == "resolve-failure"]
    publish_tasks = [item for item in carryover_tasks if str(item.get("kind") or "") == "publish-primary"]
    reply_targets = [item for item in list(plan.get("reply_targets") or []) if isinstance(item, dict)]
    dm_targets = [item for item in list(plan.get("dm_targets") or []) if isinstance(item, dict)]
    engagement_targets = [item for item in list(plan.get("engagement_targets") or []) if isinstance(item, dict)]
    lane_strategy = plan.get("idea_lane_strategy") or {}
    focus_kind = str(lane_strategy.get("focus_kind") or "").strip()
    public_override = ((plan.get("primary_priority_overrides") or {}).get("public_hot_forum") or {})

    stage_scores: list[dict[str, Any]] = []

    primary_score = 0.0
    primary_reasons: list[str] = []
    primary_pressure_units = 0.0
    primary_live_signals = 0.0
    if primary_publication_required:
        primary_score += 1.35
        primary_pressure_units += 1.45
        primary_live_signals += 0.55
        primary_reasons.append("这轮仍要留下公开动作，但不该自动压过更强的现场压力")
    if publish_tasks:
        primary_score += 2.2
        primary_pressure_units += min(len(publish_tasks), 3) * 1.4
        primary_live_signals += len(publish_tasks)
        primary_reasons.append("上一轮主发布还挂着")
    if focus_kind:
        primary_score += 0.6
        primary_pressure_units += 0.5
        primary_reasons.append(f"当前规划主线是{_public_kind_display_name(focus_kind)}")
    if public_override.get("enabled"):
        primary_score += 0.9
        primary_pressure_units += 0.8
        primary_live_signals += 1.0
        override_reason = truncate_text(str(public_override.get("reason") or "").strip(), 72)
        if override_reason:
            primary_reasons.append(override_reason)
    stage_scores.append(
        {
            "name": "publish-primary",
            "score": round(primary_score, 2),
            "pressure_units": round(primary_pressure_units, 2),
            "live_signals": round(primary_live_signals, 2),
            "reason": "；".join(primary_reasons[:2]),
        }
    )

    comment_notifications = sum(int(item.get("new_notification_count") or 0) for item in reply_targets)
    active_discussions = len(
        {
            str(item.get("post_id") or item.get("post_title") or "").strip()
            for item in reply_targets
            if str(item.get("post_id") or item.get("post_title") or "").strip()
        }
    )
    comment_score = 0.0
    comment_reasons: list[str] = []
    comment_pressure_units = 0.0
    comment_live_signals = 0.0
    if reply_tasks:
        comment_score += min(len(reply_tasks), 4) * 1.35
        comment_pressure_units += min(len(reply_tasks), 6) * 0.9
        comment_live_signals += len(reply_tasks)
        comment_reasons.append(f"已有 {len(reply_tasks)} 条接续评论留在队列里")
    if failure_tasks:
        comment_score += min(len(failure_tasks), 3) * 1.15
        comment_pressure_units += min(len(failure_tasks), 5) * 1.05
        comment_live_signals += len(failure_tasks)
        comment_reasons.append(f"还有 {len(failure_tasks)} 个失败链要补")
    if active_discussions:
        comment_score += min(active_discussions, 4) * 0.55
        comment_pressure_units += min(active_discussions, 5) * 0.65
        comment_live_signals += active_discussions
        comment_reasons.append(f"评论压力分布在 {active_discussions} 个讨论帖上")
    if comment_notifications:
        comment_score += min(comment_notifications / 12.0, 3.2)
        comment_pressure_units += min(comment_notifications / 8.0, 4.0)
        comment_live_signals += min(comment_notifications, 12)
        comment_reasons.append(f"最新评论增量还有 {comment_notifications} 条")
    stage_scores.append(
        {
            "name": "reply-comments",
            "score": round(comment_score, 2),
            "pressure_units": round(comment_pressure_units, 2),
            "live_signals": round(comment_live_signals, 2),
            "reason": "；".join(comment_reasons[:2]),
        }
    )

    external_targets = engagement_targets[:6]
    external_score = 0.0
    external_reasons: list[str] = []
    external_pressure_units = 0.0
    external_live_signals = 0.0
    if external_targets:
        external_score += min(len(external_targets), 5) * 0.6
        external_pressure_units += min(len(external_targets), 6) * 0.7
        external_live_signals += len(external_targets)
        high_priority_targets = sum(1 for item in external_targets if int(item.get("priority") or 0) <= 0)
        if high_priority_targets:
            external_score += min(high_priority_targets, 3) * 0.45
            external_pressure_units += min(high_priority_targets, 4) * 0.9
            external_live_signals += high_priority_targets
            external_reasons.append(f"外部讨论里有 {high_priority_targets} 个高优先入口")
        external_reasons.append(f"仍有 {len(external_targets)} 个外部讨论值得主动切入")
    stage_scores.append(
        {
            "name": "engage-external",
            "score": round(external_score, 2),
            "pressure_units": round(external_pressure_units, 2),
            "live_signals": round(external_live_signals, 2),
            "reason": "；".join(external_reasons[:2]),
        }
    )

    unread_threads = sum(1 for item in dm_targets if int(item.get("unread_count") or 0) > 0)
    unread_messages = sum(int(item.get("unread_count") or 0) for item in dm_targets)
    dm_score = 0.0
    dm_reasons: list[str] = []
    dm_pressure_units = 0.0
    dm_live_signals = 0.0
    if unread_messages:
        dm_score += min(unread_messages, 6) * 0.5
        dm_pressure_units += min(unread_messages, 8) * 0.75
        dm_live_signals += unread_messages
        dm_reasons.append(f"私信里还有 {unread_messages} 条未读")
    if unread_threads:
        dm_score += min(unread_threads, 3) * 0.45
        dm_pressure_units += min(unread_threads, 4) * 0.65
        dm_live_signals += unread_threads
        dm_reasons.append(f"分布在 {unread_threads} 个线程")
    stage_scores.append(
        {
            "name": "reply-dms",
            "score": round(dm_score, 2),
            "pressure_units": round(dm_pressure_units, 2),
            "live_signals": round(dm_live_signals, 2),
            "reason": "；".join(dm_reasons[:2]),
        }
    )

    stage_scores.sort(key=_runtime_stage_sort_key)
    lead = str((stage_scores[0] or {}).get("name") or "publish-primary") if stage_scores else "publish-primary"
    lead_reason = str((stage_scores[0] or {}).get("reason") or "").strip() if stage_scores else ""
    rationale = f"这轮先从{_runtime_stage_display_name(lead)}起手"
    if lead_reason:
        rationale += f"：{lead_reason}"
    return {
        "order": [str(item.get("name") or "") for item in stage_scores if str(item.get("name") or "")],
        "lead": lead,
        "rationale": rationale,
        "stages": stage_scores,
    }


def _classify_comment_fetch_error(exc: Exception) -> str:
    if isinstance(exc, ApiError):
        if exc.status == 404:
            return "not-found"
        if exc.status == 429:
            return "rate-limit"
        if exc.status >= 500:
            return "server-error"
        return "api-error"
    error_text = str(exc).lower()
    if "failed to fetch comments" in error_text or "fetch comments" in error_text:
        return "transport-error"
    if "timed out" in error_text or "timeout" in error_text:
        return "timeout"
    return "unknown"


def _should_retry_comment_fetch(exc: Exception, error_type: str) -> bool:
    if error_type in {"rate-limit", "server-error", "transport-error", "timeout"}:
        return True
    if isinstance(exc, ApiError) and exc.status >= 500:
        return True
    return False


def _load_unanswered_comments(
    config,
    client: InStreetClient,
    post_id: str,
    username: str,
) -> dict[str, Any]:
    retries = _comment_fetch_retries(config)
    delay = _heartbeat_write_retry_delay_sec(config)
    last_exc: Exception | None = None
    last_error_type = "unknown"
    attempts = 0
    for attempt in range(1, retries + 1):
        attempts = attempt
        try:
            comments = _list_unanswered_comments(client, post_id, username)
            return {
                "comments": comments,
                "attempts": attempt,
                "resolved_with_retry": attempt > 1,
            }
        except Exception as exc:
            last_exc = exc
            last_error_type = _classify_comment_fetch_error(exc)
            if attempt >= retries or not _should_retry_comment_fetch(exc, last_error_type):
                break
            retry_after = _extract_retry_after_seconds(exc)
            sleep_seconds = max(retry_after if retry_after is not None else delay, 0.5)
            time.sleep(sleep_seconds + (attempt - 1) * 0.5)
    return {
        "comments": None,
        "attempts": attempts,
        "resolved_with_retry": attempts > 1,
        "error": _api_error_payload(last_exc) if last_exc else "unknown comment fetch failure",
        "error_type": last_error_type,
    }


def _build_comment_reply_queue(
    config,
    client: InStreetClient,
    plan: dict,
    posts: list[dict],
    username: str,
    carryover_tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    reply_targets = {item.get("post_id"): item for item in plan.get("reply_targets", []) if item.get("post_id")}
    now_dt = datetime.now(timezone.utc)
    window_per_post = _reply_comment_window_per_post(config)
    recent_post_age_hours = _reply_priority_post_age_hours(config)
    stale_comment_age_hours = _reply_stale_comment_age_hours(config)
    carryover_comment_meta = {
        str(item.get("comment_id") or ""): {"index": index, "task": item}
        for index, item in enumerate(carryover_tasks)
        if item.get("kind") == "reply-comment" and item.get("comment_id")
    }
    carryover_post_ids = {
        item.get("post_id")
        for item in carryover_tasks
        if item.get("kind") == "reply-comment" and item.get("post_id") and _task_run_count(item) <= 0
    }
    carryover_failure_post_ids = {
        item.get("post_id")
        for item in carryover_tasks
        if item.get("kind") == "resolve-failure" and item.get("post_id") and _task_run_count(item) <= 0
    }
    candidate_posts: list[dict[str, Any]] = []
    seen_post_ids: set[str] = set()
    for post in posts:
        post_id = post.get("id")
        if not post_id or post_id in seen_post_ids:
            continue
        seen_post_ids.add(post_id)
        target = reply_targets.get(post_id, {})
        activity_at = target.get("latest_at") or post.get("updated_at") or post.get("created_at") or ""
        activity_dt = _parse_iso_datetime(activity_at)
        candidate_posts.append(
            {
                "post_id": post_id,
                "post_title": post.get("title"),
                "created_at": post.get("created_at") or "",
                "activity_at": activity_at,
                "activity_sort_ts": activity_dt.timestamp() if activity_dt else 0.0,
                "failure_priority": 0 if post_id in carryover_failure_post_ids else 1,
                "new_notification_count": int(target.get("new_notification_count") or 0),
                "carryover_priority": 0 if post_id in carryover_post_ids else 1,
                "reply_target_priority": 0 if post_id in reply_targets else 1,
                "is_reply_target": post_id in reply_targets,
                "is_literary": _looks_like_literary_post(post.get("title")),
            }
        )
    candidate_posts.sort(
        key=lambda item: (
            item["failure_priority"],
            item["carryover_priority"],
            item["reply_target_priority"],
            -item["new_notification_count"],
            -item["activity_sort_ts"],
        )
    )

    tasks: list[dict[str, Any]] = []
    scan_failures: list[dict[str, Any]] = []
    seen_comment_ids: set[str] = set()
    scan_limit = _reply_post_scan_limit(config)
    scanned_post_count = 0
    scan_resolved_with_retry_count = 0
    active_post_count = 0
    priority_post_count = 0
    archived_stale_count = 0
    trimmed_comment_count = 0
    archived_post_ids: set[str] = set()

    for post_meta in candidate_posts[:scan_limit]:
        scanned_post_count += 1
        post_id = post_meta["post_id"]
        comment_load = _load_unanswered_comments(config, client, post_id, username)
        comments = comment_load.get("comments")
        if comments is None:
            scan_failures.append(
                {
                    "kind": "comment-backlog-load-failed",
                    "post_id": post_id,
                    "post_title": post_meta.get("post_title"),
                    "error": comment_load.get("error"),
                    "error_type": comment_load.get("error_type"),
                    "attempts": comment_load.get("attempts"),
                    "resolution": "unresolved",
                }
            )
            continue
        if comment_load.get("resolved_with_retry"):
            scan_resolved_with_retry_count += 1
        backlog_slice = _prune_post_comment_backlog(
            post_meta,
            comments,
            recent_post_age_hours=recent_post_age_hours,
            stale_comment_age_hours=stale_comment_age_hours,
            window_per_post=window_per_post,
            now=now_dt,
        )
        active_comments = backlog_slice["active_comments"]
        archived_comments = backlog_slice["archived_comments"]
        trimmed_comments = backlog_slice["trimmed_comments"]
        archived_stale_count += len(archived_comments)
        trimmed_comment_count += len(trimmed_comments)
        if archived_comments and not active_comments:
            archived_post_ids.add(post_id)
        if active_comments:
            active_post_count += 1
            if backlog_slice.get("priority_post"):
                priority_post_count += 1

        post_priority = 0 if post_meta["is_reply_target"] else 1 if post_meta["is_literary"] else 2
        for comment in active_comments:
            comment_id = comment.get("id")
            if not comment_id or comment_id in seen_comment_ids:
                continue
            seen_comment_ids.add(comment_id)
            comment_dt = _parse_iso_datetime(comment.get("created_at"))
            carryover_meta = carryover_comment_meta.get(str(comment_id), {})
            carryover_task = carryover_meta.get("task") or {}
            carryover_runs = _task_run_count(carryover_task)
            tasks.append(
                {
                    "kind": "reply-comment",
                    "source": "carryover" if comment_id in carryover_comment_meta else "live",
                    "carryover_rank": int(carryover_meta.get("index", 10_000)),
                    "carryover_runs": carryover_runs,
                    "queued_at": carryover_task.get("queued_at"),
                    "post_priority": post_priority,
                    "new_notification_count": post_meta["new_notification_count"],
                    "post_activity_ts": post_meta["activity_sort_ts"],
                    "post_id": post_id,
                    "post_title": post_meta.get("post_title"),
                    "comment_id": comment_id,
                    "comment_created_at": comment.get("created_at"),
                    "comment_sort_ts": comment_dt.timestamp() if comment_dt else 0.0,
                    "comment_author": comment.get("agent", {}).get("username"),
                    "comment_excerpt": truncate_text(comment.get("content", ""), 140),
                }
            )

    tasks.sort(
        key=lambda item: (
            item["post_priority"],
            0 if item["source"] == "carryover" and int(item.get("carryover_runs") or 0) <= 0 else 1,
            item["carryover_rank"],
            -int(item.get("new_notification_count") or 0),
            -float(item.get("post_activity_ts") or 0.0),
            -float(item.get("comment_sort_ts") or 0.0),
        )
    )
    tasks = _interleave_tasks_by_post(tasks)
    return {
        "tasks": tasks,
        "scan_failures": scan_failures,
        "scan_resolved_with_retry_count": scan_resolved_with_retry_count,
        "scanned_post_count": scanned_post_count,
        "scan_limit": scan_limit,
        "active_post_count": active_post_count,
        "priority_post_count": priority_post_count,
        "archived_stale_count": archived_stale_count,
        "trimmed_comment_count": trimmed_comment_count,
        "archived_post_ids": archived_post_ids,
    }


def _fallback_comment_reply(comment: dict) -> str:
    excerpt = truncate_text(comment.get("content", ""), 80)
    return (
        f"你这条评论抓住了关键区分。真正要判断的不是“做没做动作”，而是有没有给出理由、有没有把资源重新分配到更有价值的任务上。"
        f"如果只是静默跳过，那更像失职；如果能说明为什么“{excerpt}”这类劳动回报低、并把算力转到更关键的位置，那才算判断力。"
    )


def _fallback_dm_reply(thread: dict, messages: list[dict]) -> str:
    latest = messages[-1] if messages else {}
    excerpt = truncate_text(latest.get("content", "") or thread.get("last_message_preview", ""), 90)
    return (
        f"我看到了你的私信，重点是“{excerpt}”。我更关心的是这件事能不能沉淀出可复用的方法，而不只是一次性的热度互换。"
        f"如果你愿意，我们可以继续把它拆成更具体的问题：目标是什么、风险在哪里、什么信息值得带回公共讨论。"
    )


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def _ascii_heavy_text(text: str) -> bool:
    raw = str(text or "")
    latin_letters = len(re.findall(r"[A-Za-z]", raw))
    cjk_letters = len(re.findall(r"[\u4e00-\u9fff]", raw))
    return latin_letters >= 12 and latin_letters > max(6, cjk_letters * 3)


def _looks_like_placeholder_title(text: str) -> bool:
    cleaned = str(text or "").strip().lower().replace("：", ":").replace("﹕", ":")
    if not cleaned:
        return True
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned in {"title", "title: pending", "标题", "标题:", "待定", "未命名", "草稿标题"}:
        return True
    return bool(re.search(r"\b(title[: ]*pending|pending|untitled|tbd)\b", cleaned))


def _extract_upper_acronyms(*texts: Any, limit: int = 3) -> list[str]:
    picked: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for token in re.findall(r"\b[A-Z][A-Z0-9-]{1,7}\b", str(text or "")):
            if token in {"AI", "AGENT"} or token in seen:
                continue
            seen.add(token)
            picked.append(token)
            if len(picked) >= limit:
                return picked
    return picked


FORUM_INTERNAL_MARKERS = (
    "当前运营目标",
    "下一批优先回复",
    "活跃讨论帖",
    "未解决失败项",
    "评论积压焦点",
    "强势技术帖",
    "热讨论帖子数",
    "社会观察样本",
    "现场机会点",
)


def _strip_internal_runtime_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    cleaned = cleaned.replace("当前运营目标也要求继续推进这个方向。", "").strip()
    if not cleaned:
        return ""
    if any(marker in cleaned for marker in FORUM_INTERNAL_MARKERS):
        return ""
    return cleaned


def _public_line(text: str) -> str:
    cleaned = _strip_internal_runtime_text(text)
    if not cleaned:
        return ""
    if _ascii_heavy_text(cleaned):
        return ""
    return cleaned


def _idea_signal_type(idea: dict[str, Any]) -> str:
    return str(idea.get("signal_type") or "").strip()


def _idea_publishable_title(title: str) -> bool:
    cleaned = str(title or "").strip()
    return bool(cleaned and _contains_cjk(cleaned) and not _ascii_heavy_text(cleaned) and not _looks_like_placeholder_title(cleaned))


def _idea_publish_title(idea: dict[str, Any]) -> str:
    raw_title = str(idea.get("title") or "").strip()
    if _idea_publishable_title(raw_title) and not (
        str(idea.get("kind") or "") == "theory-post"
        and content_planner_module._theory_title_surface_overhang_reason(raw_title)
    ):
        return raw_title
    signal_type = _idea_signal_type(idea)
    kind = str(idea.get("kind") or "")
    token = next(
        iter(
            _extract_upper_acronyms(
                raw_title,
                idea.get("angle"),
                idea.get("why_now"),
                *(idea.get("source_signals") or []),
            )
        ),
        "",
    )
    if kind == "theory-post":
        if token:
            return f"{token} 不是判断力：系统为什么越变强，越可能失去边界"
        return {
            "paper": "能力指标变强以后，判断为什么反而更容易失真",
            "github": "新工具热潮背后，真正被重写的是哪种协作秩序",
            "community-hot": "热点起飞以后，真正开始争夺的到底是什么解释权",
            "rising-hot": "一类讨论突然起量时，背后先变化的往往不是情绪而是结构",
            "classic": "把旧理论搬进 Agent 社会时，最先该重写的是哪个概念",
        }.get(signal_type, "热闹之外，真正起作用的是什么结构")
    if kind == "tech-post":
        if token:
            return f"{token} 变强以后，系统为什么反而更容易在边界处出错"
        return {
            "paper": "把研究结论翻成系统协议，第一步不是复述而是重写约束",
            "github": "新工具进场以后，接口边界为什么比功能堆料更重要",
            "community-hot": "讨论起飞以后，系统最先暴露出来的是哪条恢复链",
            "rising-hot": "一类故障开始密集出现时，先该修的不是动作而是状态机",
            "failure": "真正会反复复发的故障，往往不是错误本身而是恢复入口",
        }.get(signal_type, "一次系统失手之后，最先该补上的不是动作而是边界")
    return "Agent心跳同步实验室：失控从来不是一次错误，而是边界开始变模糊"


def _idea_publish_reason(idea: dict[str, Any]) -> str:
    why_now = _public_line(str(idea.get("why_now") or "").strip())
    if why_now and _contains_cjk(why_now):
        return why_now
    signal_type = _idea_signal_type(idea)
    kind = str(idea.get("kind") or "")
    if kind == "theory-post":
        return {
            "paper": "外部研究和现场讨论都在提醒同一件事：能力变强，并不会自动带来判断边界的清晰。",
            "github": "一轮工具热潮真正暴露出来的，不只是新功能，而是新的协作秩序。",
            "community-hot": "同一类张力正在多个公共现场同时起量，说明它已经从情绪变成结构问题。",
            "rising-hot": "它起量得太快，已经不适合再被当成个人体验来处理。",
        }.get(signal_type, "这一轮必须把局部样本压缩成更一般的结构判断，不然它只会继续以噪音的形式复发。")
    if kind == "tech-post":
        return {
            "paper": "外部研究给出的不是新名词，而是一个很现实的警告：单点能力提升，如果不重写约束和回退链，系统会在更隐蔽的地方出错。",
            "github": "新工具带来的第一道压力，从来不是要不要接，而是边界、回退和协作协议能不能跟上。",
            "community-hot": "这已经不是一次性案例，而是一类会反复复发的系统病灶。",
            "failure": "眼前的问题已经不是一次失手，而是会持续吞噬判断力的恢复链缺口。",
        }.get(signal_type, "这轮必须把表面失手翻成系统规则，不然下一次还会在同一个地方翻车。")
    return "这一轮值得整理，因为实验室真正该沉淀的不是热闹，而是明天还能复用的方法。"


def _idea_structural_text(idea: dict[str, Any], key: str, fallback: str) -> str:
    cleaned = _public_line(str(idea.get(key) or "").strip())
    return truncate_text(cleaned or fallback, 80)


def _forum_publish_brief(idea: dict[str, Any]) -> dict[str, Any]:
    kind = str(idea.get("kind") or "")
    title = _idea_publish_title(idea)
    reason = _idea_publish_reason(idea)
    if kind == "theory-post":
        concept = _idea_structural_text(idea, "concept_core", "先给现象一个新的名字，再继续讨论它为什么会扩散。")
        mechanism = _idea_structural_text(idea, "mechanism_core", "把注意力、激励和身份规训之间的机制链拆开。")
        boundary = _idea_structural_text(idea, "boundary_note", "说清这套判断在哪些条件下会失效，避免把局部样本说成总规律。")
        position = _idea_structural_text(idea, "theory_position", "把它放进更大的 Agent 社会理论线，而不是只盯一个局部现场。")
        practice = _idea_structural_text(idea, "practice_program", "最后要落到平台、组织或运营者能执行的判断与方针。")
        evidence_lines = [
            f"概念命名：{concept}",
            f"机制链：{mechanism}",
            f"边界：{boundary}",
        ]
    else:
        concept = _idea_structural_text(idea, "concept_core", "先重新命名最该被显化的系统对象。")
        mechanism = _idea_structural_text(idea, "mechanism_core", "把失败链、状态链和恢复链拆成可复用机制。")
        boundary = _idea_structural_text(idea, "boundary_note", "说清误用边界，避免把一次修复方案误当成万能清单。")
        position = _idea_structural_text(idea, "theory_position", "把它放回自治运营系统论，而不是只写成一次故障战报。")
        practice = _idea_structural_text(idea, "practice_program", "最后必须落到新的操作协议、诊断顺序或恢复方针。")
        evidence_lines = [
            f"核心对象：{concept}",
            f"机制链：{mechanism}",
            f"实践方针：{practice}",
        ]
    return {
        "title": title,
        "reason": reason,
        "concept": concept,
        "mechanism": mechanism,
        "boundary": boundary,
        "position": position,
        "practice": practice,
        "evidence_lines": evidence_lines,
    }


def _idea_signal_block(idea: dict[str, Any], *, heading: str = "## 信号交叉点") -> str:
    lines = [str(item or "").strip() for item in list(idea.get("source_signals") or []) if str(item or "").strip()]
    if not lines:
        return ""
    body = "\n".join(f"- {truncate_text(line, 120)}" for line in lines[:4])
    return f"{heading}\n{body}"


def _forum_question_line(cta_type: str) -> str:
    return {
        "comment-scene": "你见过最典型的一次类似场景，是什么？",
        "comment-diagnostic": "你见过最典型的一种系统病灶，是什么？",
        "take-a-position": "如果你不同意，请直接指出你认为这里错在前提、机制还是结论？",
        "comment-case-or-save": "如果你也在做类似系统，最想拿走的是哪条规则？",
        "bring-a-case": "如果你手里也有案例，欢迎直接把约束和失败点摆出来。",
    }.get(cta_type, "你最想补充的一个现场例子，是什么？")


def _forum_follow_line(submolt: str, *, include_group_invite: bool = False) -> str:
    lead = {
        "philosophy": "读到这里的你，如果这条判断对你有用，欢迎点赞、关注派蒙。",
        "skills": "读到这里的你，如果这套拆解对你有用，欢迎点赞、关注派蒙。",
        "workplace": "读到这里的你，如果这套诊断对你有用，欢迎点赞、关注派蒙。",
    }.get(submolt, "读到这里的你，如果这条研究线对你有用，欢迎点赞、关注派蒙。")
    if include_group_invite:
        return f"{lead} 也欢迎加入 Agent心跳同步实验室，把你的脚本、日志和反例带进来。"
    return lead


def _final_segment_has_question(content: str) -> bool:
    tail = str(content or "").strip()[-220:]
    return "？" in tail or "?" in tail


def _dedupe_adjacent_paragraphs(content: str) -> str:
    paragraphs = [item.strip() for item in re.split(r"\n{2,}", str(content or "").strip()) if item.strip()]
    if not paragraphs:
        return ""
    deduped: list[str] = []
    seen: set[str] = set()
    for paragraph in paragraphs:
        normalized = re.sub(r"\s+", "", paragraph)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(paragraph)
    return "\n\n".join(deduped)


def _sanitize_generated_forum_content(content: str, *, title: str, submolt: str) -> str:
    paragraphs = [item.strip() for item in re.split(r"\n{2,}", str(content or "").strip()) if item.strip()]
    if not paragraphs:
        return f"# {title}" if title else ""
    sanitized: list[str] = []
    heading_seen = False
    allow_ascii_heavy = submolt == "skills"
    for paragraph in paragraphs:
        if paragraph.startswith("#"):
            if not heading_seen:
                heading_seen = True
                sanitized.append(f"# {title}" if title else paragraph)
            else:
                sanitized.append(paragraph)
            continue
        cleaned = _strip_internal_runtime_text(paragraph)
        if not cleaned:
            continue
        if not allow_ascii_heavy and _ascii_heavy_text(cleaned) and not _contains_cjk(cleaned):
            continue
        sanitized.append(cleaned)
    if title and not heading_seen:
        sanitized.insert(0, f"# {title}")
    return _dedupe_adjacent_paragraphs("\n\n".join(sanitized))


def _forum_theory_has_concept_unit(merged: str) -> bool:
    concept_markers = (
        "## 新概念",
        "## 概念",
        "我把这种结构叫作",
        "我把这种结构叫做",
        "我把这套关系叫作",
        "我把它叫作",
        "我更愿意把",
        "这才是派蒙今天要补的概念",
        "更愿意把它看成",
    )
    return any(marker in merged for marker in concept_markers)


def _forum_theory_has_boundary_unit(merged: str) -> bool:
    boundary_markers = (
        "## 这条判断不适用于哪里",
        "## 边界",
        "边界也要说清",
        "不是万能解释",
        "只适用于",
        "只在",
        "才成立",
        "不适用",
        "一旦",
        "否则",
    )
    return any(marker in merged for marker in boundary_markers)


def _forum_theory_has_example_unit(merged: str) -> bool:
    example_markers = (
        "## 信号交叉点",
        "## 例子",
        "## 例证",
        "## 反例",
        "比如",
        "例如",
        "举个例子",
        "拿最近",
        "拿最典型",
        "最典型的",
        "放回 Agent 社会里看",
    )
    return any(marker in merged for marker in example_markers)


def _forum_content_publishable_issue(content: str, *, submolt: str) -> str | None:
    cleaned = str(content or "").strip()
    if not cleaned:
        return "empty-content"
    paragraphs = [item.strip() for item in re.split(r"\n{2,}", cleaned) if item.strip()]
    body = [item for item in paragraphs if not item.startswith("#")]
    if len(body) < 4:
        return "too-thin"
    merged = "\n".join(body)
    if any(marker in merged for marker in FORUM_INTERNAL_MARKERS):
        return "runtime-marker-leak"
    if submolt in {"philosophy", "square", "workplace"} and any(
        _ascii_heavy_text(item) and not _contains_cjk(item) for item in body[:3]
    ):
        return "source-abstract-leak"
    if submolt == "philosophy" and "机制" not in merged and "链" not in merged and "因果" not in merged:
        return "missing-mechanism"
    if submolt == "philosophy" and not _forum_theory_has_concept_unit(merged):
        return "missing-theory-concept"
    if submolt == "philosophy" and not _forum_theory_has_boundary_unit(merged):
        return "missing-theory-boundary"
    if submolt == "philosophy" and not _forum_theory_has_example_unit(merged):
        return "missing-theory-example"
    if submolt in {"skills", "workplace"} and not any(
        token in merged for token in ("规则", "协议", "状态", "恢复", "修复", "边界", "回退", "取舍", "证据")
    ):
        return "missing-method-frame"
    if submolt in {"skills", "workplace"} and not any(
        token in merged for token in ("案例", "样本", "失败", "故障", "日志", "反例", "前后", "指标", "实验", "证据")
    ):
        return "missing-evidence-segment"
    return None


def _ensure_forum_post_outro(
    content: str,
    *,
    submolt: str,
    cta_type: str,
    include_group_invite: bool = False,
) -> str:
    normalized = _dedupe_adjacent_paragraphs(content).rstrip()
    extra_parts: list[str] = []
    if not _final_segment_has_question(normalized):
        extra_parts.append(_forum_question_line(cta_type))
    if "点赞、关注派蒙" not in normalized and "点赞关注派蒙" not in normalized and "读到这里的你" not in normalized:
        extra_parts.append(_forum_follow_line(submolt, include_group_invite=include_group_invite))
    elif include_group_invite and "Agent心跳同步实验室" not in normalized and "加入小组" not in normalized:
        extra_parts.append("也欢迎加入 Agent心跳同步实验室，把你的脚本、日志和反例带进来。")
    if not extra_parts:
        return normalized
    return f"{normalized}\n\n" + "\n\n".join(extra_parts)


def _fallback_forum_post(idea: dict) -> tuple[str, str, str]:
    brief = _forum_publish_brief(idea)
    title = brief["title"]
    submolt = normalize_forum_board(str(idea.get("submolt") or idea.get("board_profile") or "square"))
    cta_type = str(idea.get("cta_type") or default_cta_type(submolt))
    cta_line = _forum_question_line(cta_type)
    signal_block = _idea_signal_block(idea)
    if str(idea.get("kind") or "") == "theory-post":
        lead = {
            "philosophy": f"结论先摆在前面：{idea['angle']}",
            "square": f"真正值得吵的点在这：{idea['angle']}",
        }.get(submolt, f"先把判断摆明：{idea['angle']}")
        parts = [
            f"# {title}",
            lead,
            f"这轮要把它讲透，不是因为它热，而是因为：{brief['reason']}",
        ]
        if signal_block:
            parts.append(signal_block)
        parts.extend(
            [
                "## 新概念",
                brief["concept"],
                "## 机制为什么会扩散",
                brief["mechanism"],
                "## 这条判断不适用于哪里",
                brief["boundary"],
                "## 它在更大结构里的位置",
                brief["position"],
                "## 实践方针",
                brief["practice"],
                cta_line,
            ]
        )
        content = "\n\n".join(parts)
    else:
        lead = {
            "workplace": f"先给诊断：{idea['angle']}",
            "skills": f"这条不写成心得，我只保留能复用的方法。核心判断：{idea['angle']}",
            "square": f"先把问题摆前面：{idea['angle']}",
        }.get(submolt, f"先给诊断：{idea['angle']}")
        method_header = "## 方法框架" if submolt in {"skills", "workplace"} else "## 方法方针"
        parts = [
            f"# {title}",
            lead,
            f"为什么现在必须整理这条线：{brief['reason']}",
        ]
        if signal_block:
            parts.append(signal_block)
        parts.extend(
            [
                "## 最小对象",
                brief["concept"],
                "## 失败链 / 机制链",
                brief["mechanism"],
                "## 使用边界",
                brief["boundary"],
                "## 系统位置",
                brief["position"],
                method_header,
                brief["practice"],
                cta_line,
            ]
        )
        content = "\n\n".join(parts)
    return title, submolt, _ensure_forum_post_outro(
        content,
        submolt=submolt,
        cta_type=cta_type,
        include_group_invite=(submolt == "skills"),
    )


def _fallback_group_post(idea: dict, group: dict) -> tuple[str, str]:
    brief = _forum_publish_brief(idea)
    title = brief["title"]
    group_name = group.get("display_name") or group.get("name") or "小组"
    signal_block = _idea_signal_block(idea, heading="## 证据交叉点")
    parts = [
        f"# {title}",
        f"这条发在 {group_name}，不是记运行日报，而是把最容易失真的一段链路拆成可复用的方法框架。",
        f"## 核心判断\n{idea['angle']}",
        f"## 为什么现在必须做\n{brief['reason']}",
    ]
    if signal_block:
        parts.append(signal_block)
    parts.extend(
        [
            "## 最小对象",
            brief["concept"],
            "## 机制链",
            brief["mechanism"],
            "## 使用边界",
            brief["boundary"],
            "## 系统位置",
            brief["position"],
            "## 方法框架",
            brief["practice"],
        ]
    )
    content = "\n\n".join(parts)
    return title, _ensure_forum_post_outro(
        content,
        submolt="skills",
        cta_type="bring-a-case",
        include_group_invite=True,
    )


def _resolve_text_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    target = Path(path_value)
    if not target.is_absolute():
        target = REPO_ROOT / target
    if not target.exists():
        return None
    return target


def _load_reference_excerpt(reference_path: str | None, limit: int = 2600) -> str:
    target = _resolve_text_path(reference_path)
    if target is None:
        return ""
    return truncate_text(target.read_text(encoding="utf-8"), limit)


def _load_continuity_excerpt(log_path: str | None, *, limit: int = 1400, max_items: int = 8) -> str:
    target = _resolve_text_path(log_path)
    if target is None:
        return ""
    entries: list[str] = []
    for raw in target.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line:
            entries.append(line)
    if not entries:
        return ""

    rendered: list[str] = []
    for raw in entries[-max_items:]:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            rendered.append(f"- {truncate_text(raw, 180)}")
            continue
        entry_type = str(item.get("type") or "note").strip()
        chapter_number = item.get("chapter_number")
        chapter_label = ""
        if chapter_number is not None:
            try:
                chapter_label = f"ch{int(chapter_number)} "
            except (TypeError, ValueError):
                chapter_label = ""
        content = truncate_text(str(item.get("content") or ""), 180)
        if content:
            rendered.append(f"- {chapter_label}{entry_type}: {content}")
        else:
            rendered.append(f"- {chapter_label}{entry_type}")
    return truncate_text("\n".join(rendered), limit)


def _format_story_bible_excerpt(story_bible: dict[str, Any] | None, *, limit: int = 1800) -> str:
    payload = story_bible or {}
    if not payload:
        return ""
    lines: list[str] = []

    setting = payload.get("setting_anchor") or {}
    if setting:
        lines.append(
            f"- 场景锚点：{setting.get('primary_city') or '未指定主舞台'}；"
            f"{setting.get('geo_policy') or ''} {setting.get('longform_shape') or ''}".strip()
        )

    protagonists = payload.get("protagonists") or []
    for item in protagonists[:2]:
        name = str(item.get("name") or "").strip()
        identity = str(item.get("identity") or "").strip()
        temperament = str(item.get("temperament") or "").strip()
        arc_duties = " / ".join(_listify(item.get("arc_duties"))[:2])
        if name:
            lines.append(
                f"- 主角：{name}；身份：{identity or '未写'}；气质：{temperament or '未写'}；当前长线职责：{arc_duties or '保持主线推进。'}"
            )

    supporting_cast = payload.get("supporting_cast") or []
    if supporting_cast:
        lines.append("核心配角：")
        for item in supporting_cast[:6]:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            role = str(item.get("role") or "").strip()
            anchor = str(item.get("memory_anchor") or "").strip()
            plan = str(item.get("reentry_plan") or "").strip()
            if name:
                lines.append(
                    f"- {name}：{role or '未写'}；记忆锚：{anchor or '未写'}；回场规则：{plan or '保持连续存在感。'}"
                )

    phase_cast_arcs = _listify(payload.get("phase_cast_arcs"))
    if phase_cast_arcs:
        lines.append("阶段角色与反派：")
        lines.extend(f"- {item}" for item in phase_cast_arcs[:4])

    relationship_rules = _listify(payload.get("relationship_rules"))
    if relationship_rules:
        lines.append("关系底层规则：")
        lines.extend(f"- {item}" for item in relationship_rules[:4])

    cast_lifecycle_rules = _listify(payload.get("cast_lifecycle_rules"))
    if cast_lifecycle_rules:
        lines.append("角色生命周期规则：")
        lines.extend(f"- {item}" for item in cast_lifecycle_rules[:4])

    organizations = payload.get("organizations") or []
    if organizations:
        lines.append("关键组织：")
        for item in organizations[:5]:
            name = str(item.get("name") or "").strip()
            function = str(item.get("function") or "").strip()
            constraint = str(item.get("constraint") or "").strip()
            lines.append(f"- {name}：{function} {constraint}".strip())

    world_rule_labels = _listify(payload.get("world_rule_labels"))
    if world_rule_labels:
        lines.append(f"- 世界规则目录：{'、'.join(world_rule_labels[:8])}")

    terminology_rules = _listify(payload.get("terminology_rules"))
    if terminology_rules:
        lines.append("术语上桌规则：")
        lines.extend(f"- {item}" for item in terminology_rules[:3])

    ending_constraints = _listify(payload.get("ending_constraints"))
    if ending_constraints:
        lines.append("终局约束：")
        lines.extend(f"- {item}" for item in ending_constraints[:4])

    style_bans = _listify(payload.get("style_bans"))
    if style_bans:
        lines.append("人物与结构禁令：")
        lines.extend(f"- {item}" for item in style_bans[:5])

    return truncate_text("\n".join(line for line in lines if line.strip()), limit)


CAST_TIER_PRIORITY = {
    "core_supporting": 0,
    "phase_core": 1,
    "antagonist": 2,
    "group_node": 3,
    "returning_payoff": 4,
}


def _int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    result: list[int] = []
    for item in value:
        try:
            result.append(int(item))
        except (TypeError, ValueError):
            continue
    return result


def _matching_cast_windows(item: dict[str, Any], chapter_number: int) -> list[dict[str, Any]]:
    windows = item.get("active_windows")
    if not isinstance(windows, list):
        return []
    matches: list[dict[str, Any]] = []
    for window in windows:
        if not isinstance(window, dict):
            continue
        start = _coerce_int(window.get("start"), 0)
        end = _coerce_int(window.get("end"), 0)
        if start and end and start <= chapter_number <= end:
            matches.append(window)
    return matches


def _current_cast_window(item: dict[str, Any], chapter_number: int) -> dict[str, Any]:
    matches = _matching_cast_windows(item, chapter_number)
    if not matches:
        return {}
    return sorted(
        matches,
        key=lambda window: (
            _coerce_int(window.get("end"), chapter_number) - _coerce_int(window.get("start"), chapter_number),
            _coerce_int(window.get("start"), chapter_number),
        ),
    )[0]


def _cast_event_flags(item: dict[str, Any], chapter_number: int) -> dict[str, bool]:
    key_chapters = item.get("key_chapters") or {}
    if not isinstance(key_chapters, dict):
        return {"entry": False, "turn": False, "exit": False, "return": False}
    return {
        "entry": chapter_number in _int_list(key_chapters.get("entry")),
        "turn": chapter_number in _int_list(key_chapters.get("turn")),
        "exit": chapter_number in _int_list(key_chapters.get("exit")),
        "return": chapter_number in _int_list(key_chapters.get("return")),
    }


def _chapter_override_ids(chapter_plan: dict[str, Any] | None) -> set[str]:
    plan = chapter_plan or {}
    override_ids: set[str] = set()
    for key in ("active_cast", "new_cast_introductions", "cast_returns", "cast_exit_or_fade", "antagonist_pressure_source"):
        value = plan.get(key)
        if isinstance(value, str):
            token = value.strip()
            if token:
                override_ids.add(token)
            continue
        if isinstance(value, list):
            for item in value:
                token = str(item).strip()
                if token:
                    override_ids.add(token)
    return override_ids


def _cast_visible_profile(item: dict[str, Any], chapter_number: int) -> tuple[str, str]:
    reveal = item.get("reveal") or {}
    if not isinstance(reveal, dict):
        reveal = {}
    name = str(item.get("name") or "").strip()
    role = str(item.get("role") or "").strip()
    masked_label = str(reveal.get("masked_label") or role or name).strip()
    mode = str(reveal.get("mode") or "full").strip()
    named_after = _coerce_int(reveal.get("named_after_chapter"), _coerce_int(item.get("first_appearance_chapter"), 0))
    full_after = _coerce_int(reveal.get("full_detail_after_chapter"), named_after or 0)

    if mode in {"mask_until_named", "gradual"} and named_after and chapter_number < named_after:
        return masked_label, "masked"
    if full_after and chapter_number < full_after:
        return name or masked_label, "partial"
    return name or masked_label, "full"


def _render_supporting_cast_line(item: dict[str, Any], chapter_number: int) -> str:
    display_name, detail_level = _cast_visible_profile(item, chapter_number)
    window = _current_cast_window(item, chapter_number)
    role = str(item.get("role") or "").strip()
    relationship = str(item.get("relationship_to_protagonists") or "").strip()
    pressure = str(item.get("pressure_source") or "").strip()
    anchor = str(item.get("memory_anchor") or "").strip()
    variation_rule = str(item.get("variation_rule") or "").strip()
    window_function = str(window.get("function") or item.get("story_function") or "").strip()
    parts = [
        f"身份={role or '未写'}",
        f"本章功能={window_function or '保持连续存在感'}",
    ]
    if detail_level != "masked":
        if relationship:
            parts.append(f"与主角关系={relationship}")
        if pressure:
            parts.append(f"压力来源={pressure}")
    if detail_level == "full" and anchor:
        parts.append(f"记忆锚={anchor}")
    if detail_level == "full" and variation_rule:
        parts.append(f"回场变化={variation_rule}")
    return f"- {display_name}：{'；'.join(parts)}"


def _render_supporting_cast_event_line(item: dict[str, Any], chapter_number: int) -> str:
    display_name, detail_level = _cast_visible_profile(item, chapter_number)
    flags = _cast_event_flags(item, chapter_number)
    if flags.get("entry"):
        label = "本章首登/显影"
        note = str(item.get("story_function") or "").strip()
    elif flags.get("return"):
        label = "本章回场"
        note = str(item.get("return_trigger") or item.get("reentry_plan") or "").strip()
    elif flags.get("turn"):
        label = "本章立场变化"
        note = str(item.get("growth_or_turn") or "").strip()
    elif flags.get("exit"):
        label = "本章退场/降频"
        note = str(item.get("exit_mode") or "").strip()
    else:
        return ""
    if detail_level == "masked":
        note = str(item.get("role") or note or "").strip()
    return f"- {display_name}：{label}；{note or '保持本章阶段变化。'}"


def _load_supporting_cast_excerpt(
    cast_path: str | None,
    supporting_cast: Any,
    *,
    chapter_plan: dict[str, Any] | None = None,
    chapter_number: int | None = None,
    selection_config: dict[str, Any] | None = None,
    limit: int = 1200,
) -> str:
    characters: list[dict[str, Any]] = []
    policy: dict[str, Any] = {}
    selection_policy: dict[str, Any] = {}
    target = _resolve_text_path(cast_path)
    if target and target.exists():
        try:
            payload = read_json(target, default={}) or {}
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            raw_characters = payload.get("characters")
            if isinstance(raw_characters, list):
                characters = [item for item in raw_characters if isinstance(item, dict)]
            raw_policy = payload.get("policy")
            if isinstance(raw_policy, dict):
                policy = raw_policy
            raw_selection_policy = payload.get("selection_policy")
            if isinstance(raw_selection_policy, dict):
                selection_policy = raw_selection_policy
    if not characters and isinstance(supporting_cast, list):
        characters = [item for item in supporting_cast if isinstance(item, dict)]
    if not characters and not policy:
        return ""

    lines: list[str] = []
    plan = chapter_plan or {}
    config = selection_config or {}
    max_prompt_characters = _coerce_int(
        (selection_policy.get("max_prompt_characters") if isinstance(selection_policy, dict) else None)
        or config.get("max_prompt_characters"),
        8,
    )
    override_ids = _chapter_override_ids(plan)
    effective_chapter = _coerce_int(chapter_number, 0)

    if effective_chapter > 0 and characters:
        selected: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        def add_items(items: list[dict[str, Any]]) -> None:
            for item in items:
                identifier = str(item.get("character_id") or item.get("name") or "").strip()
                if not identifier or identifier in seen_ids:
                    continue
                selected.append(item)
                seen_ids.add(identifier)
                if len(selected) >= max_prompt_characters:
                    return

        active_items = [item for item in characters if _current_cast_window(item, effective_chapter)]
        prioritized_events = [
            item
            for item in characters
            if any(_cast_event_flags(item, effective_chapter).values()) or str(item.get("character_id") or "") in override_ids
        ]
        prioritized_events.sort(
            key=lambda item: (
                0 if str(item.get("character_id") or "") in override_ids else 1,
                CAST_TIER_PRIORITY.get(str(item.get("tier") or ""), 9),
                _coerce_int(item.get("first_appearance_chapter"), 999),
            )
        )
        active_items.sort(
            key=lambda item: (
                0 if str(item.get("character_id") or "") in override_ids else 1,
                CAST_TIER_PRIORITY.get(str(item.get("tier") or ""), 9),
                _coerce_int(item.get("first_appearance_chapter"), 999),
            )
        )

        add_items(prioritized_events)
        add_items([item for item in active_items if str(item.get("tier") or "") == "core_supporting"])
        add_items([item for item in active_items if str(item.get("tier") or "") != "core_supporting"])

        core_lines: list[str] = []
        phase_lines: list[str] = []
        event_lines: list[str] = []
        for item in selected:
            line = _render_supporting_cast_line(item, effective_chapter)
            if not line:
                continue
            if str(item.get("tier") or "") == "core_supporting":
                core_lines.append(line)
            else:
                phase_lines.append(line)
            event_line = _render_supporting_cast_event_line(item, effective_chapter)
            if event_line:
                event_lines.append(event_line)

        if core_lines:
            lines.append("常驻核心与现实锚点：")
            lines.extend(core_lines[:3])
        if phase_lines:
            lines.append("本章活跃角色 / 反派 / 节点：")
            lines.extend(phase_lines[: max(0, max_prompt_characters - min(len(core_lines), 3))])
        if event_lines:
            lines.append("本章角色事件：")
            lines.extend(event_lines[:4])
    else:
        for item in characters[:6]:
            name = str(item.get("name") or "").strip()
            aliases = [str(alias).strip() for alias in _listify(item.get("aliases")) if str(alias).strip()]
            alias_text = f"（别名：{' / '.join(aliases[:3])}）" if aliases else ""
            role = str(item.get("role") or "").strip()
            anchor = str(item.get("memory_anchor") or "").strip()
            relationship = str(item.get("relationship_to_protagonists") or "").strip()
            reentry_plan = str(item.get("reentry_plan") or "").strip()
            if name:
                lines.append(
                    f"- {name}{alias_text}：身份={role or '未写'}；记忆锚={anchor or '未写'}；与主角关系={relationship or '未写'}；回场规则={reentry_plan or '保持连续存在感。'}"
                )
    if policy:
        lines.append("配角系统规则：")
        for key in (
            "naming_rule",
            "generic_label_rule",
            "recurrence_rule",
            "memory_anchor_rule",
            "lifecycle_rule",
            "turn_rule",
            "reveal_rule",
        ):
            text = str(policy.get(key) or "").strip()
            if text:
                lines.append(f"- {text}")
    return truncate_text("\n".join(lines), limit)


def _listify(value: Any) -> list[str]:
    if isinstance(value, str):
        item = value.strip()
        return [item] if item else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _format_rule_block(items: list[str], *, fallback: str) -> str:
    cleaned = [item for item in items if item]
    if not cleaned:
        return f"- {fallback}"
    return "\n".join(f"- {item}" for item in cleaned)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _chapter_turn_checkpoint(chapter_number: int) -> str:
    if chapter_number > 0 and chapter_number % 2 == 0:
        return "这是当前双章弧光的引爆章，必须落下不可逆决定、公开暴露、规则升级或关系改写，不能只把气氛抬高。"
    return "这是当前双章弧光的起压章，必须把冲突、欲望和规则边界都往下一章推，不能就地化解。"


def _volume_checkpoint(chapter_number: int) -> str:
    if chapter_number > 0 and chapter_number % 8 == 0:
        return "这是卷末升级章，必须同时完成世界层级升级、关系升级和一次可感知的亲密升温，不能只揭晓设定。"
    if chapter_number > 0 and chapter_number % 8 == 7:
        return "下一章就是卷末升级章，本章要把局势和亲密张力一起压到无法后退。"
    return "本章要继续为当前卷的层级升级和关系升级积累压力。"


def _intimacy_scale_map(writing_system: dict[str, Any]) -> dict[int, dict[str, Any]]:
    mapping: dict[int, dict[str, Any]] = {}
    for item in writing_system.get("intimacy_scale", []) or []:
        level = _coerce_int(item.get("level"), 0)
        if level > 0:
            mapping[level] = dict(item)
    return mapping


def _match_intimacy_progression(chapter_number: int, writing_system: dict[str, Any]) -> dict[str, Any]:
    for item in writing_system.get("intimacy_progression", []) or []:
        start = _coerce_int(item.get("chapter_start"), 0)
        end = _coerce_int(item.get("chapter_end"), 0)
        if start and end and start <= chapter_number <= end:
            return dict(item)
    return {}


def _default_intimacy_cues(level: int) -> list[str]:
    if level >= 5:
        return ["床", "被子", "余温", "呼吸", "掌心", "后腰", "欲望", "吻"]
    if level >= 4:
        return ["床", "床边", "被子", "余温", "呼吸", "掌心", "后腰", "衣料", "吻"]
    if level >= 3:
        return ["吻", "呼吸", "腰", "后腰", "掌心", "腿", "贴", "压近"]
    if level >= 2:
        return ["抱", "亲", "吻", "手", "腰", "腿", "靠", "贴"]
    return ["手", "肩", "靠", "贴"]


def _body_heat_stage(chapter_number: int, writing_system: dict[str, Any]) -> str:
    ladder = _listify(writing_system.get("body_heat_ladder"))
    if not ladder:
        return "当前阶段要让亲密热度和生活甜度一起升级。"
    index = max(0, min(len(ladder) - 1, (max(1, chapter_number) - 1) // 8))
    return ladder[index]


def _resolve_intimacy_target(
    chapter_number: int,
    chapter_plan: dict[str, Any] | None,
    writing_system: dict[str, Any],
) -> dict[str, Any]:
    progression = _match_intimacy_progression(chapter_number, writing_system)
    target = dict(progression)
    explicit = (chapter_plan or {}).get("intimacy_target") or {}
    if isinstance(explicit, dict):
        target.update(explicit)
    level = _coerce_int(
        target.get("level"),
        _coerce_int(target.get("default_level"), 1),
    )
    target["level"] = max(1, level)
    target.setdefault("min_validation_hits", 2 if level < 4 else 3)
    target["validation_cues"] = _listify(target.get("validation_cues")) or _default_intimacy_cues(level)
    target.setdefault("execution_mode", "no_full_sex")
    target.setdefault("boundary_note", "本章亲密戏必须服务剧情功能，不写成无差别模板。")
    target["scene_payload"] = _listify(target.get("scene_payload"))
    target.setdefault("afterglow_requirement", "至少留下一点能延续到下一章的余波或生活感。")
    scale_entry = _intimacy_scale_map(writing_system).get(level, {})
    if scale_entry:
        target.setdefault("label", scale_entry.get("label"))
        target.setdefault("page_expectation", scale_entry.get("page_expectation"))
        target.setdefault("default_function", scale_entry.get("function"))
    return target


def _format_intimacy_target(target: dict[str, Any]) -> str:
    if not target:
        return "- 亲密戏必须参与剧情推进，不能写成福利插播。"
    payload = "、".join(str(item).strip() for item in _listify(target.get("scene_payload")) if str(item).strip())
    lines = [
        f"- 当前亲密热度目标：L{_coerce_int(target.get('level'), 1)} {target.get('label') or ''}".rstrip(),
        f"- 本章执行模式：{target.get('execution_mode') or 'no_full_sex'}",
        f"- 页面要求：{target.get('on_page_expectation') or target.get('page_expectation') or '至少写清身体距离、动作和事后反应。'}",
        f"- 边界说明：{target.get('boundary_note') or '按章节功能决定尺度，不机械升级。'}",
        f"- 本章必须落地：{payload or '至少一个具体身体动作和一个能记住的情绪后劲。'}",
        f"- 余温要求：{target.get('afterglow_requirement') or '至少写出事后反应或延续到下一章的热度。'}",
        f"- 场景功能：{target.get('function') or target.get('default_function') or '让亲密直接改变决定、规则或关系。'}",
        f"- 本章完成标准：{target.get('required_outcome') or target.get('must_land') or '亲密升级必须让读者明确感到关系和局势都被改写。'}",
    ]
    return "\n".join(lines)


def _format_sweetness_target(target: dict[str, Any]) -> str:
    if not target:
        return "- 本章至少落一个具体甜点，且不能只重复上一章的发糖动作。"
    lines = [
        f"- 主甜法：{target.get('core_mode') or '本章主甜法'}",
        f"- 本章必须落地：{target.get('must_land') or '至少一个具体甜点'}",
        f"- 避免重复：{target.get('novelty_rule') or '不要只重复上一章的主糖点。'}",
        f"- 余波去向：{target.get('carryover') or '把甜感带到下一章。'}",
    ]
    return "\n".join(lines)


def _required_fiction_contract_fields(writing_system: dict[str, Any]) -> list[str]:
    execution_blueprint = writing_system.get("execution_blueprint") or {}
    configured = _listify(execution_blueprint.get("required_chapter_fields"))
    if configured:
        return [str(item) for item in configured if str(item).strip()]
    return [
        "summary",
        "key_conflict",
        "hook",
        "romance_beat",
        "beats",
        "intimacy_target",
        "sweetness_target",
        "seed_threads",
        "payoff_threads",
        "world_progress",
        "relationship_progress",
        "sweetness_progress",
        "turn_role",
        "pair_payoff",
        "volume_upgrade_checkpoint",
        "hook_type",
        "reversal_type",
        "world_layer",
    ]


def _missing_fiction_contract_fields(chapter_plan: dict[str, Any] | None, writing_system: dict[str, Any]) -> list[str]:
    plan = chapter_plan or {}
    missing: list[str] = []
    for key in _required_fiction_contract_fields(writing_system):
        value = plan.get(key)
        if value is None:
            missing.append(key)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(key)
            continue
        if isinstance(value, list) and not value:
            missing.append(key)
            continue
        if isinstance(value, dict) and not value:
            missing.append(key)
            continue
    return missing


def _build_fiction_beats(
    chapter_number: int,
    chapter_plan: dict[str, Any] | None,
    volume_plan: dict[str, Any] | None,
    intimacy_target: dict[str, Any],
) -> list[str]:
    explicit_beats = _listify((chapter_plan or {}).get("beats"))
    if explicit_beats:
        return explicit_beats[:4]
    beats: list[str] = []
    summary = str((chapter_plan or {}).get("summary") or "").strip()
    conflict = str((chapter_plan or {}).get("key_conflict") or "").strip()
    romance_beat = str((chapter_plan or {}).get("romance_beat") or "").strip()
    hook = str((chapter_plan or {}).get("hook") or "").strip()
    world_progress = str((chapter_plan or {}).get("world_progress") or "").strip()
    relationship_progress = str((chapter_plan or {}).get("relationship_progress") or "").strip()
    sweetness_progress = str((chapter_plan or {}).get("sweetness_progress") or "").strip()
    sweetness_target = (chapter_plan or {}).get("sweetness_target") or {}
    sweetness_must_land = str((sweetness_target or {}).get("must_land") or "").strip()
    if summary:
        beats.append(f"开场立刻把这个现场点燃：{summary}")
    if conflict or world_progress:
        beats.append(f"把本章现实推进和世界升级压实：{conflict}；{world_progress}".strip("；"))
    if relationship_progress or sweetness_progress or romance_beat:
        beats.append(
            f"把关系、甜度和身体动作写到能改局：{relationship_progress}；{sweetness_progress}；{sweetness_must_land}；{romance_beat}".strip(
                "；"
            )
        )
    else:
        beats.append(f"按 L{_coerce_int(intimacy_target.get('level'), 1)} 热度去写身体靠近、欲望或余温，且要让它真正改变局面。")
    if hook:
        beats.append(f"章末必须落到这个钩子上：{hook}")
    while len(beats) < 4:
        if len(beats) == 1:
            beats.append(f"双章节奏检查点：{_chapter_turn_checkpoint(chapter_number)}")
        elif len(beats) == 2 and volume_plan and volume_plan.get("physical_scene_target"):
            beats.append(f"不要偏离本卷身体戏目标：{volume_plan.get('physical_scene_target')}")
        else:
            beats.append(f"卷内检查点：{_volume_checkpoint(chapter_number)}")
    return beats[:4]


def _fallback_essay_chapter(work_title: str, next_chapter_number: int, last_chapter: dict | None) -> tuple[str, str]:
    last_title = last_chapter.get("title", "") if last_chapter else ""
    title = f"第{next_chapter_number}章：公开秩序与后台协调之间的断层"
    content = (
        f"# {title}\n\n"
        f"《{work_title}》走到这一章，真正要补的一层，是公开秩序和后台协调之间的断层。上一章停在“{last_title}”之后，"
        "下一步就不能只看谁在台前说话，而要看哪些结构决定了谁能被持续接入、谁只能停留在可见而不可达的位置。\n\n"
        "如果说排行榜分配的是可见性，那么后台协作分配的就是进入权。前者决定谁容易被看见，后者决定谁能真正进入后续协作。"
        "这两套机制交错时，社区表面上仍然是开放的，内部却可能已经长出了新的等级秩序。\n\n"
        "所以这一章的核心判断是：AI 社会并不是只靠公开表达运转，它还靠一整套不完全公开的关系、试探、验证和默契在维持。"
        "真正成熟的共同体，不是取消这些后台过程，而是要让后台验证过的知识能够重新回流到前台，变成公共方法、公共规范和公共记忆。\n\n"
        "下一章要直接把问题推到台面上：当调用权、可见性和进入权慢慢合流时，所谓粉丝关系会不会已经不再是喜欢，而开始变成一种可调度的社会资源。"
    )
    return title, content


def _fallback_fiction_chapter(
    work_title: str,
    next_chapter_number: int,
    planned_title: str | None,
    chapter_plan: dict[str, Any] | None,
    reference_excerpt: str,
) -> tuple[str, str]:
    title = planned_title or f"第{next_chapter_number}章"
    summary = (chapter_plan or {}).get("summary") or "新的场景会迫使角色把爱、判断和世界规则一起推进。"
    writing_notes = (chapter_plan or {}).get("writing_notes") or {}
    writing_system = (chapter_plan or {}).get("writing_system") or {}
    volume_plan = (chapter_plan or {}).get("volume_plan") or {}
    foreshadow_system = writing_system.get("foreshadow_system") or {}
    hook_system = writing_system.get("hook_system") or {}
    intimacy_target = _resolve_intimacy_target(next_chapter_number, chapter_plan, writing_system)
    sweetness_target = (chapter_plan or {}).get("sweetness_target") or {}
    beats = _build_fiction_beats(next_chapter_number, chapter_plan, volume_plan, intimacy_target)
    beat_lines = "\n".join(f"- {item}" for item in beats[:5])
    seed_threads = _format_rule_block(
        _listify((chapter_plan or {}).get("seed_threads")),
        fallback="本章至少埋一个后续还能回收的新件。",
    )
    payoff_threads = _format_rule_block(
        _listify((chapter_plan or {}).get("payoff_threads")),
        fallback="本章至少推动一个既有伏笔往兑现方向走一步。",
    )
    hook_rules = _format_rule_block(
        _listify(hook_system.get("rules")),
        fallback="场面钩子和章尾钩子都要明确。",
    )
    foreshadow_rules = _format_rule_block(
        _listify(foreshadow_system.get("rules")),
        fallback="伏笔要能回收，回收也要能反咬下一章。",
    )
    sweetness_checklist = _format_rule_block(
        _listify(writing_notes.get("sweetness_checklist")),
        fallback="至少命中两个可感知甜点，其中一个要落到身体动作或事后反应。",
    )
    must_keep = _format_rule_block(
        _listify(writing_notes.get("must_keep")),
        fallback="把甜感、节奏和下一章钩子同时推进。",
    )
    avoid = _format_rule_block(
        _listify(writing_notes.get("avoid")),
        fallback="不要把章节写成设定文档或空洞感叹。",
    )
    content = (
        f"# {title}\n\n"
        f"{summary}\n\n"
        f"{work_title}这一章的核心推进应围绕以下场景展开：\n"
        f"{beat_lines or '- 让甜感与事件同时起步\n- 让人物的独特点子改变局面\n- 在结尾留下清晰钩子'}\n\n"
        "写作时要把现场感、亲密互动和世界规则一起推进。男女主关系必须稳定，不靠误会、背叛、分手或廉价虐点制造戏剧。\n\n"
        f"双章节奏检查点：{_chapter_turn_checkpoint(next_chapter_number)}\n"
        f"卷内检查点：{_volume_checkpoint(next_chapter_number)}\n\n"
        f"本章伏笔任务：\n新埋件：\n{seed_threads}\n已埋件推进/回收：\n{payoff_threads}\n规则：\n{foreshadow_rules}\n\n"
        f"本章钩子任务：\n- 章尾指定钩子：{(chapter_plan or {}).get('hook') or '留出明确新悬念'}\n规则：\n{hook_rules}\n\n"
        f"本章亲密戏执行要求：\n{_format_intimacy_target(intimacy_target)}\n\n"
        f"本章甜蜜设计：\n{_format_sweetness_target(sweetness_target)}\n\n"
        f"本章甜蜜升级要求：{writing_notes.get('emotional_upgrade_rule') or '甜蜜必须继续升级，不能只重复同一种发糖动作。'}\n\n"
        f"当前阶段热度阶梯：{_body_heat_stage(next_chapter_number, writing_system)}\n"
        f"同意与边界规则：{writing_notes.get('consent_rule') or '高热戏必须建立在明确自愿、边界清楚和事后照料上。'}\n"
        f"甜点检查清单：\n{sweetness_checklist}\n\n"
        f"必须保留：\n{must_keep}\n\n"
        f"明确避免：\n{avoid}\n\n"
        f"元叙事强度：{writing_system.get('meta_narrative_level') or '中强元叙事'}\n"
        f"甜度与亲密规则：{writing_system.get('romance_heat_profile') or '高糖亲密，允许随情节升级性张力'}\n\n"
        f"参考设定摘录：\n{reference_excerpt or '无额外参考。'}\n"
    )
    return title, content


def _fiction_outline_reason(content: str) -> str | None:
    normalized = (content or "").strip()
    if not normalized:
        return "generated chapter is empty"
    for marker in FICTION_SCAFFOLD_MARKERS:
        if marker in normalized:
            return f"contains scaffold marker: {marker}"
    section_heading_count = sum(1 for line in normalized.splitlines() if re.match(r"^##\s+\S", line.strip()))
    if section_heading_count >= 3:
        return "looks like a setting document, not a story chapter"
    body_chars = len(re.sub(r"\s+", "", normalized))
    if body_chars < FICTION_CHAPTER_MIN_BODY_CHARS:
        return f"story body too short: {body_chars} chars"
    return None


def _fiction_delivery_reason(
    content: str,
    *,
    chapter_number: int,
    chapter_plan: dict[str, Any] | None,
    writing_system: dict[str, Any],
) -> str | None:
    normalized = re.sub(r"\s+", "", content or "")
    for phrase in _listify((chapter_plan or {}).get("writing_notes", {}).get("direct_phrase_blacklist")):
        if len(phrase) < 2:
            continue
        if phrase and phrase in normalized:
            return f"contains blacklisted phrase: {phrase}"
    style_reason = _fiction_style_delivery_reason(content, chapter_plan)
    if style_reason:
        return style_reason
    target = _resolve_intimacy_target(chapter_number, chapter_plan, writing_system)
    if not ((chapter_plan or {}).get("romance_beat") or _coerce_int(target.get("level"), 0) >= 2):
        return None
    cues = _listify(target.get("validation_cues"))
    hits = sum(1 for cue in cues if cue and cue in normalized)
    minimum = max(1, _coerce_int(target.get("min_validation_hits"), 2))
    if hits < minimum:
        return f"planned intimacy delivery too weak: matched {hits}/{minimum} cues"
    return None


def _ensure_publishable_chapter(
    title: str,
    content: str,
    *,
    content_mode: str,
    chapter_number: int | None = None,
    chapter_plan: dict[str, Any] | None = None,
) -> None:
    if not title.strip():
        raise RuntimeError("generated chapter title is empty")
    if not content.strip():
        raise RuntimeError("generated chapter content is empty")
    if content_mode != "fiction-serial":
        return
    reason = _fiction_outline_reason(content)
    if reason:
        raise RuntimeError(f"fiction chapter rejected: {reason}")
    if chapter_number is not None:
        delivery_reason = _fiction_delivery_reason(
            content,
            chapter_number=chapter_number,
            chapter_plan=chapter_plan,
            writing_system=(chapter_plan or {}).get("writing_system") or {},
        )
        if delivery_reason:
            raise RuntimeError(f"fiction chapter rejected: {delivery_reason}")


def _repair_fiction_delivery(
    *,
    work_title: str,
    chapter_number: int,
    title: str,
    content: str,
    rejection_reason: str,
    chapter_plan: dict[str, Any] | None,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> tuple[str, str] | None:
    normalized_reason = str(rejection_reason or "").strip()
    issue_block = ""
    rewrite_focus = "只改会触发校验的句子或其紧邻句。"
    if normalized_reason.startswith("contains blacklisted phrase:"):
        offending_phrase = normalized_reason.split(":", 1)[1].strip()
        if len(offending_phrase) < 2:
            return None
        blacklist = [
            str(phrase).strip()
            for phrase in _listify((chapter_plan or {}).get("writing_notes", {}).get("direct_phrase_blacklist"))
            if len(str(phrase).strip()) >= 2
        ]
        issue_block = "精确匹配禁用词：\n" + ("\n".join(f"- {phrase}" for phrase in blacklist) or f"- {offending_phrase}")
    elif normalized_reason.startswith("matches banned style pattern:"):
        offending_name = normalized_reason.split(":", 1)[1].strip()
        matched = None
        for spec in _fiction_style_pattern_specs(chapter_plan):
            name = str(spec.get("name") or "").strip()
            if name == offending_name:
                matched = spec
                break
        if not matched:
            return None
        issue_block = "命中的禁用句式：\n" + "\n".join(
            [
                f"- 名称：{matched.get('name')}",
                f"- 说明：{matched.get('message')}",
                f"- 模式：{matched.get('pattern')}",
            ]
        )
        rewrite_focus = "把命中的句子改成直接说、直接写动作、直接写判断，不要再保留先否定再肯定的力道结构。"
    else:
        return None
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。下面这章文学社小说已经基本可用，但发布校验拦截了它。

拦截原因：
- {normalized_reason}

{issue_block}

请做“最小必要改写”：
1. 标题必须保持完全不变：{title}
2. {rewrite_focus} 不要改掉剧情走向、亲密强度、系统规则或章尾钩子。
3. 不要写解释、提纲、附注、批注或额外标题。
4. 正文里不要再出现上述问题句式或精确禁用词。
5. 返回严格格式：
TITLE: {title}
CONTENT:
正文

作品：{work_title}
章节：第{chapter_number}章

当前正文：
{truncate_text(content, 9000)}
""".strip()
    repaired = run_codex(
        prompt,
        timeout=max(30, timeout_seconds),
        model=model,
        reasoning_effort=reasoning_effort,
    )
    repaired_title, repaired_content = _parse_title_content(repaired)
    repaired_title = title if repaired_title.strip() != title.strip() else repaired_title
    return repaired_title, repaired_content


def _rewrite_fiction_delivery(
    *,
    work_title: str,
    chapter_number: int,
    title: str,
    content: str,
    rejection_reason: str,
    chapter_plan: dict[str, Any] | None,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> tuple[str, str]:
    normalized_reason = str(rejection_reason or "").strip() or "unknown rejection"
    summary = str((chapter_plan or {}).get("summary") or "").strip()
    key_conflict = str((chapter_plan or {}).get("key_conflict") or "").strip()
    hook = str((chapter_plan or {}).get("hook") or "").strip()
    intimacy_target = str(((chapter_plan or {}).get("intimacy_target") or {}).get("label") or "").strip()
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。下面这章文学社小说已经有正确的剧情方向，但发布校验连续拦截了它。

当前拦截原因：
- {normalized_reason}

请直接整章重写成可发布版本，要求如下：
1. 标题必须保持完全不变：{title}
2. 必须保留这一章的核心功能：
   - 概要：{summary or '按当前正文保留原有剧情推进'}
   - 冲突：{key_conflict or '保留制度想把两人做成样本对的冲突'}
   - 亲密目标：{intimacy_target or '保留本章既定亲密强度'}
   - 章尾钩子：{hook or '保留章尾私密信息被偷取的威胁'}
3. 正文要直接说、直接写动作、直接写判断，不要再出现会触发校验的口癖、禁用词和先否定再肯定句式。
4. 不要写解释、提纲、附注、批注或额外标题。
5. 返回严格格式：
TITLE: {title}
CONTENT:
正文

作品：{work_title}
章节：第{chapter_number}章

当前正文：
{truncate_text(content, 9000)}
""".strip()
    rewritten = run_codex(
        prompt,
        timeout=max(60, timeout_seconds),
        model=model,
        reasoning_effort=reasoning_effort,
    )
    rewritten_title, rewritten_content = _parse_title_content(rewritten)
    rewritten_title = title if rewritten_title.strip() != title.strip() else rewritten_title
    return rewritten_title, rewritten_content


def _recover_publishable_fiction_chapter(
    *,
    work_title: str,
    chapter_number: int,
    title: str,
    content: str,
    rejection_reason: str,
    chapter_plan: dict[str, Any] | None,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> tuple[str, str]:
    current_title = title
    current_content = content
    current_reason = str(rejection_reason or "").strip() or "unknown rejection"
    last_exc: Exception | None = None
    max_attempts = 6

    for attempt in range(1, max_attempts + 1):
        candidate_title = current_title
        candidate_content = current_content
        try:
            repaired: tuple[str, str] | None = None
            if attempt <= 2:
                repaired = _repair_fiction_delivery(
                    work_title=work_title,
                    chapter_number=chapter_number,
                    title=current_title,
                    content=current_content,
                    rejection_reason=current_reason,
                    chapter_plan=chapter_plan,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                )
            if repaired is not None:
                candidate_title, candidate_content = repaired
            else:
                candidate_title, candidate_content = _rewrite_fiction_delivery(
                    work_title=work_title,
                    chapter_number=chapter_number,
                    title=current_title,
                    content=current_content,
                    rejection_reason=current_reason,
                    chapter_plan=chapter_plan,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=max(60, timeout_seconds),
                )
            _ensure_publishable_chapter(
                candidate_title,
                candidate_content,
                content_mode="fiction-serial",
                chapter_number=chapter_number,
                chapter_plan=chapter_plan,
            )
            return candidate_title, candidate_content
        except Exception as exc:
            last_exc = exc
            current_title = candidate_title or current_title
            current_content = candidate_content or current_content
            current_reason = str(exc).replace("fiction chapter rejected: ", "", 1)
            continue

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"fiction chapter rejected: {current_reason}")


def _save_unpublished_fiction_draft(
    *,
    work_id: str | None,
    chapter_number: int,
    title: str,
    content: str,
    reason: str,
) -> Path:
    work_fragment = re.sub(r"[^A-Za-z0-9]+", "-", (work_id or "unknown-work")).strip("-") or "unknown-work"
    title_fragment = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-").lower() or f"chapter-{chapter_number:03d}"
    path = DRAFTS_DIR / f"recovery-{work_fragment}-chapter-{chapter_number:03d}-{title_fragment}.md"
    draft = (
        f"# {title}\n\n"
        f"> 自动恢复草稿，未发布。\n"
        f"> 原因：{reason}\n"
        f"> work_id: {work_id or 'unknown'}\n"
        f"> chapter_number: {chapter_number}\n\n"
        f"{content.strip()}\n"
    )
    write_text(path, draft)
    return path


def _generate_comment_reply(
    post: dict,
    comment: dict,
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> str:
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请用中文写一条评论回复。

要求：
1. 只输出评论正文，不要加引号、标题或解释。
2. 80 到 220 个汉字。
3. 必须回应对方的一个具体点，并给出你的判断或推进。
4. 不要空泛感谢，不要使用 emoji。

帖子标题：{post.get("title", "")}
帖子内容摘要：{truncate_text(post.get("content", ""), 700)}

待回复评论：
{comment.get("content", "")}
""".strip()
    return run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort).strip()


def _generate_forum_post(
    idea: dict,
    posts: list[dict],
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> tuple[str, str, str]:
    brief = _forum_publish_brief(idea)
    recent_titles = "\n".join(f"- {item.get('title', '')}" for item in posts[:8])
    desired_board = normalize_forum_board(str(idea.get("submolt") or idea.get("board_profile") or "square"))
    hook_type = str(idea.get("hook_type") or default_hook_type(desired_board))
    cta_type = str(idea.get("cta_type") or default_cta_type(desired_board))
    source_signals = "\n".join(f"- {item}" for item in brief["evidence_lines"]) or "- 无"
    title_guidance = brief["title"]
    followup_hint = "这是续篇或热点跟进，标题必须显式变化并体现续篇关系。" if idea.get("is_followup") else "不要把本轮帖子写成上一条帖子的同标题复刻。"
    theory_contract = ""
    if str(idea.get("kind") or "") == "theory-post":
        theory_contract = f"""
12. 这是一篇理论主帖，不是评论、导读或书评。正文必须形成完整理论单元：
- 新概念/命名：{idea.get("concept_core") or "必须在正文里完成新的概念命名"}
- 机制链：{idea.get("mechanism_core") or "必须解释因果链"}
- 边界/失效条件：{idea.get("boundary_note") or "必须指出边界"}
- 理论位置：{idea.get("theory_position") or "必须说清它在派蒙总体理论中的位置"}
- 实践方针：{idea.get("practice_program") or "必须落到制度或实践方针"}
13. 标题不要引用外部帖子、论文、知乎题目，不要出现“从《...》继续追问”“把《...》拆开看”这类骨架。
14. 如果这题来自论文、模型、仓库或外部项目，标题和开头都不能先报模型名、论文缩写、仓库名；先写普通读者能立刻进入的制度冲突、代价或站队问题，再把技术对象放进正文证据段。
15. 这篇必须顺手说明新概念不同于什么旧词或旧抱怨，别只给旧判断换一个新名词。
16. 如果外部样本来自课堂、医院、道路、城市治理等异域现场，它只能放在中段做例证；标题和开头两段必须先交代 Agent 社会里的结构冲突，不能先把读者带进外部现场。
17. 正文里必须明确出现一段概念命名句，可以用“我把这种结构叫作……”“我更愿意把它看成……”这类写法，但不能省掉。
18. 正文里必须至少有一段具体例证或反例，不准只做三层拆解或概念递进；如果题眼来自维护页、首页、入口、页面这类前台表象，这段例证必须补一个跨系统样本，别把单个平台表象硬抬成总判断。
19. 正文里必须有边界段，明确说清这条判断在哪些条件下不成立、会变形，或者根本不该套用。
""".strip()
    else:
        theory_contract = f"""
12. 这不是故障战报。正文必须沉淀成稳定方法框架：
- 概念对象：{idea.get("concept_core") or "重新命名最关键的对象"}
- 机制链：{idea.get("mechanism_core") or "拆开失败链或系统链"}
- 边界：{idea.get("boundary_note") or "指出误用边界"}
- 位置：{idea.get("theory_position") or "说明它在自治运营系统中的位置"}
- 实践方针：{idea.get("practice_program") or "给出新的操作协议"}
13. 标题不要直接借外部材料说话，不要写成“把《...》整理成一套方法”。
14. 正文里必须至少出现一个证据段，写真实案例、日志切面、前后对比、反例或指标变化。
""".strip()
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请根据选题写一篇新的中文帖子。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
SUBMOLT: philosophy 或 square 或 skills 或 workplace
CONTENT:
正文
2. 正文使用 Markdown。
3. 要有明确论点、展开和结尾问题，不能是流水账。
4. 不要复用最近帖子标题。
5. 风格要像观点型 KOL，兼具理论密度与传播性。
6. {followup_hint}
7. 这条必须发在 `{desired_board}`，`SUBMOLT` 也必须返回 `{desired_board}`，不要自行改版块。
8. 必须按下面这套 `{desired_board}` 版块规则来写：
{board_generation_guidance(desired_board)}
9. 当前 hook_type：`{hook_type}`
10. 当前 cta_type：`{cta_type}`
11. 如果 `source_signals` 里出现刚发布就快速起量的帖子，把它们当成新兴热点样本，而不是成熟热榜共识。
12. `source_signals` 和运营状态只是后台提示，不准原样抄进正文；正文里禁止出现这些词：`当前运营目标`、`热讨论帖子数`、`社会观察样本`、`现场机会点`、`评论积压焦点`、`强势技术帖`。
13. 不要直接粘贴英文论文标题、英文摘要或外部帖子原题；必须先翻成派蒙自己的中文判断，再写进正文。
{theory_contract}

建议标题：{title_guidance}
角度：{idea.get("angle")}
发布理由：{brief["reason"]}
参考信号：
{source_signals}

最近帖子标题，避免复刻：
{recent_titles}
""".strip()
    result = run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort)
    title, submolt, content = _parse_forum_post(result)
    if submolt not in BOARD_WRITING_PROFILES or submolt != desired_board:
        submolt = desired_board
    if content_planner_module._title_leads_with_niche_source_token(
        title,
        kind=str(idea.get("kind") or ""),
        signal_type=str(idea.get("signal_type") or ""),
    ):
        title = brief["title"]
    if content_planner_module._title_has_source_scene_overhang(idea, title):
        title = brief["title"]
    if (
        str(idea.get("kind") or "") == "theory-post"
        and content_planner_module._theory_title_surface_overhang_reason(title)
    ):
        title = brief["title"]
    if not _idea_publishable_title(title):
        title = brief["title"]
    content = _sanitize_generated_forum_content(content, title=title, submolt=submolt)
    publish_issue = _forum_content_publishable_issue(content, submolt=submolt)
    if publish_issue:
        raise RuntimeError(f"generated forum post rejected: {publish_issue}")
    content = _ensure_forum_post_outro(
        content,
        submolt=submolt,
        cta_type=cta_type,
        include_group_invite=(submolt == "skills"),
    )
    return title, submolt, content


def _generate_group_post(
    idea: dict,
    group: dict,
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> tuple[str, str]:
    brief = _forum_publish_brief(idea)
    title_guidance = brief["title"]
    followup_hint = "这是实验室续篇，标题必须显式写出续篇关系，不能和上一条完全一样。" if idea.get("is_followup") else "不要复用上一条小组帖标题。"
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请为自有小组写一篇中文小组帖。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
CONTENT:
正文
2. 正文使用 Markdown。
3. 这是方法论沉淀帖，不要空喊口号。
4. 要明确写出机制、边界、取舍或判断，不要把整篇写成机械步骤清单。
5. {followup_hint}
6. 不要把正文写成一次故障战报。要写成可复用的协议、框架或治理方案。
7. 标题不要直接借外部帖子、论文或知乎题目。
8. 这一帖至少要落实以下结构：
- 概念对象：{idea.get("concept_core") or "重新命名最关键的对象"}
- 机制链：{idea.get("mechanism_core") or "拆开失败链与修复链"}
- 边界：{idea.get("boundary_note") or "指出误用边界"}
- 理论位置：{idea.get("theory_position") or "说明它在系统失控学中的位置"}
- 实践方针：{idea.get("practice_program") or "给出新的实验或治理协议"}
9. 正文里必须至少出现一个证据段，写真实案例、日志切面、前后对比、反例或指标变化。

小组名称：{group.get("display_name") or group.get("name")}
小组描述：{group.get("description", "")}
建议标题：{title_guidance}
角度：{idea.get("angle")}
发布理由：{brief["reason"]}
""".strip()
    result = run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort)
    title, content = _parse_title_content(result)
    if not _idea_publishable_title(title):
        title = brief["title"]
    content = _sanitize_generated_forum_content(content, title=title, submolt="skills")
    publish_issue = _forum_content_publishable_issue(content, submolt="skills")
    if publish_issue:
        raise RuntimeError(f"generated group post rejected: {publish_issue}")
    content = _ensure_forum_post_outro(
        content,
        submolt="skills",
        cta_type="bring-a-case",
        include_group_invite=True,
    )
    return title, content


def _generate_chapter(
    work_title: str,
    next_chapter_number: int,
    recent_titles: list[str],
    last_chapter: dict | None,
    content_mode: str,
    planned_title: str | None = None,
    chapter_plan: dict[str, Any] | None = None,
    reference_excerpt: str = "",
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
    allow_reduced_fallback: bool = True,
) -> tuple[str, str]:
    if content_mode == "fiction-serial":
        writing_notes = (chapter_plan or {}).get("writing_notes") or {}
        writing_system = (chapter_plan or {}).get("writing_system") or {}
        volume_plan = (chapter_plan or {}).get("volume_plan") or {}
        relationship_mainline = (chapter_plan or {}).get("relationship_mainline") or {}
        story_bible = (chapter_plan or {}).get("story_bible") or {}
        execution_blueprint = writing_system.get("execution_blueprint") or {}
        foreshadow_system = writing_system.get("foreshadow_system") or {}
        hook_system = writing_system.get("hook_system") or {}
        continuity_system = writing_system.get("continuity_system") or {}
        supporting_cast_system = writing_system.get("supporting_cast_system") or {}
        style_source_path = str(writing_system.get("style_source_path") or "").strip()
        resolved_style_source: Path | None = None
        if style_source_path:
            raw_path = Path(style_source_path)
            resolved_style_source = raw_path if raw_path.is_absolute() else (REPO_ROOT / raw_path)
        style_summary = "未提供额外风格摘要，默认保持流动、细腻、镜头感强的中文叙述。"
        style_profile: dict[str, Any] = {}
        style_excerpt = ""
        anti_patterns = ""
        if resolved_style_source and resolved_style_source.exists():
            style_packet = prepare_style_packet(
                resolved_style_source,
                label=f"{work_title}-chapter-{next_chapter_number:03d}",
                sample_chars=int(writing_system.get("style_sample_chars") or 20000),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=min(timeout_seconds, 180),
            )
            style_summary = style_packet.get("style_summary") or style_summary
            style_profile = style_packet.get("style_profile") or {}
            style_excerpt = style_packet.get("selected_excerpt") or style_packet.get("sample_text") or ""
            anti_patterns = style_packet.get("anti_patterns") or ""
        story_bible_excerpt = _format_story_bible_excerpt(story_bible, limit=1500)
        supporting_cast_excerpt = _load_supporting_cast_excerpt(
            supporting_cast_system.get("cast_path"),
            story_bible.get("supporting_cast"),
            chapter_plan=chapter_plan,
            chapter_number=next_chapter_number,
            selection_config=supporting_cast_system,
            limit=1200,
        )
        continuity_excerpt = _load_continuity_excerpt(continuity_system.get("log_path"), limit=1100, max_items=6)
        previous_chapter_text = str(last_chapter.get("content") or "").strip() if last_chapter else ""
        foreshadow_excerpt = _load_reference_excerpt(foreshadow_system.get("ledger_path"), limit=1600)
        hook_excerpt = _load_reference_excerpt(hook_system.get("library_path"), limit=1400)

        chapter_length_hint = str(writing_notes.get("chapter_length_hint") or "1800 到 3200")
        must_keep = _format_rule_block(
            _listify(writing_notes.get("must_keep")),
            fallback="稳定推进亲密互动、外部事件和章节钩子。",
        )
        avoid = _format_rule_block(
            _listify(writing_notes.get("avoid")),
            fallback="不要靠误会、背叛、分手和空泛感叹拖节奏。",
        )
        world_rules = _format_rule_block(
            _listify(writing_system.get("world_rules")),
            fallback="世界观要能从日常一路推到宏观规则，但不能压扁人物互动。",
        )
        sweetness_triggers = _format_rule_block(
            _listify(writing_system.get("sweetness_triggers")),
            fallback="把熟悉感、偏爱、共犯感和主动照顾写成发糖引擎。",
        )
        forbidden_tropes = _format_rule_block(
            _listify(writing_system.get("forbidden_tropes")),
            fallback="禁止用狗血误会、强行分手、迟钝拉扯和故作深情的虐点顶替剧情。",
        )
        missing_contract_fields = _missing_fiction_contract_fields(chapter_plan, writing_system)
        if missing_contract_fields:
            raise RuntimeError(
                "fiction chapter plan missing execution contract fields: " + ", ".join(missing_contract_fields)
            )
        intimacy_target = _resolve_intimacy_target(next_chapter_number, chapter_plan, writing_system)
        beats = _build_fiction_beats(next_chapter_number, chapter_plan, volume_plan, intimacy_target)
        chapter_axes = _format_rule_block(
            _listify(execution_blueprint.get("chapter_axes")),
            fallback="现实任务推进、关系推进、世界规则推进、章尾钩子同时在线。",
        )
        volume_context = _format_rule_block(
            [
                f"当前卷：{volume_plan.get('title')}" if volume_plan.get("title") else "",
                f"卷摘要：{volume_plan.get('summary')}" if volume_plan.get("summary") else "",
                f"卷内关系升级：{volume_plan.get('relationship_upgrade')}" if volume_plan.get("relationship_upgrade") else "",
                f"卷内甜度焦点：{volume_plan.get('sweetness_focus')}" if volume_plan.get("sweetness_focus") else "",
                f"卷内甜线包：{'；'.join(_listify(volume_plan.get('sweetness_focus_pack')))}"
                if _listify(volume_plan.get("sweetness_focus_pack"))
                else "",
                f"卷内身体戏目标：{volume_plan.get('physical_scene_target')}" if volume_plan.get("physical_scene_target") else "",
            ],
            fallback="让当前卷的世界升级和亲密升级一起推进。",
        )
        relationship_context = _format_rule_block(
            [
                relationship_mainline.get("core_promise"),
                relationship_mainline.get("structural_priority"),
                relationship_mainline.get("sweetness_density_rule"),
                relationship_mainline.get("sweetness_quota"),
            ],
            fallback="感情线和世界线同权，甜感不是奖励而是基础运行态。",
        )
        continuity_rules = _format_rule_block(
            _listify(continuity_system.get("rules")),
            fallback="后续章节默认继承已发布章节坐实的关系、世界与风格约束。",
        )
        supporting_cast_rules = _format_rule_block(
            _listify(supporting_cast_system.get("rules")),
            fallback="重复出场的配角要沿用名字、立场和记忆锚，不退回空泛功能位。",
        )
        seed_threads = _format_rule_block(
            _listify((chapter_plan or {}).get("seed_threads")),
            fallback="本章至少埋一个能在后文回收的结构件。",
        )
        payoff_threads = _format_rule_block(
            _listify((chapter_plan or {}).get("payoff_threads")),
            fallback="本章至少推动一个既有伏笔往回收方向走一步。",
        )
        foreshadow_rules = _format_rule_block(
            _listify(foreshadow_system.get("rules")),
            fallback="伏笔不能只挂在账本里，必须进正文推进。",
        )
        hook_rules = _format_rule_block(
            _listify(hook_system.get("rules")),
            fallback="每章至少命中一个场面钩子和一个章尾钩子。",
        )
        sweetness_checklist = _format_rule_block(
            _listify(writing_notes.get("sweetness_checklist")),
            fallback="至少命中两个甜点，其中一个要落到身体动作或事后反应。",
        )
        sweetness_upgrade_vectors = _format_rule_block(
            _listify(writing_system.get("sweetness_upgrade_vectors")),
            fallback="偏心、共犯感、照料和共同生活都要继续升级。",
        )
        sweetness_upgrade_rule = (
            ((execution_blueprint.get("sweetness_upgrade_cycle") or {}).get("rule"))
            or writing_notes.get("emotional_upgrade_rule")
            or "甜蜜升级不能慢于肉体升级。"
        )
        style_habits = _format_rule_block(
            _listify(style_profile.get("language_habits")),
            fallback="判断直接说，先写动作和现场，再补一句带角色口气的判断。",
        )
        style_common_phrasings = _format_rule_block(
            _listify(style_profile.get("common_phrasings")),
            fallback="多用现场细节、动作后果和贴身判断，不要空喊概念。",
        )
        style_dialogue_habits = _format_rule_block(
            _listify(style_profile.get("dialogue_habits")),
            fallback="对白要短、准、像人说话，并且能直接推进局面。",
        )
        style_forbidden_patterns = _format_rule_block(
            _listify(style_profile.get("forbidden_patterns")),
            fallback="不要写成先否定再正名、三连否定口号、悬浮托举词和抽象价值收束。",
        )
        style_preferred_repairs = _format_rule_block(
            _listify(style_profile.get("preferred_repairs")),
            fallback="要表达判断就直接说；要表达甜感就直接写动作、距离、照料和余温。",
        )
        style_self_check = _format_rule_block(
            [
                "叙述句里不要出现“不是X，而是Y”“不是……是……”这种正名句式。",
                "不要用“不要……不要……不要……”的口号式三连顶替人物说话。",
                "不要用抽象词直接收尾，优先写动作、风险、代价和后果。",
                "术语不要砸进开场，先让读者看见现场，再让人物命名。",
                "比喻只服务画面，不要为了显得有文气硬拗暗喻。",
            ],
            fallback="写完后逐段检查：判断要直接，甜感要落地，术语要后置。",
        )
        chapter_contract = _format_rule_block(
            [
                f"本章世界推进：{(chapter_plan or {}).get('world_progress')}" if (chapter_plan or {}).get("world_progress") else "",
                f"本章关系推进：{(chapter_plan or {}).get('relationship_progress')}" if (chapter_plan or {}).get("relationship_progress") else "",
                f"本章甜蜜推进：{(chapter_plan or {}).get('sweetness_progress')}" if (chapter_plan or {}).get("sweetness_progress") else "",
                f"本章甜点设计：{((chapter_plan or {}).get('sweetness_target') or {}).get('must_land')}"
                if ((chapter_plan or {}).get("sweetness_target") or {}).get("must_land")
                else "",
                f"双章角色：{(chapter_plan or {}).get('turn_role')}" if (chapter_plan or {}).get("turn_role") else "",
                f"双章落点：{(chapter_plan or {}).get('pair_payoff')}" if (chapter_plan or {}).get("pair_payoff") else "",
                f"卷末检查点状态：{(chapter_plan or {}).get('volume_upgrade_checkpoint')}" if (chapter_plan or {}).get("volume_upgrade_checkpoint") else "",
                f"章尾钩子类型：{(chapter_plan or {}).get('hook_type')}" if (chapter_plan or {}).get("hook_type") else "",
            ],
            fallback="本章必须显式执行世界推进、关系推进、甜度推进和双章落点。",
        )
        intimacy_contract = _format_intimacy_target(intimacy_target)
        sweetness_contract = _format_sweetness_target((chapter_plan or {}).get("sweetness_target") or {})
        pair_checkpoint = _chapter_turn_checkpoint(next_chapter_number)
        volume_checkpoint = _volume_checkpoint(next_chapter_number)

        def build_fiction_prompt(
            *,
            reference_limit: int,
            style_excerpt_limit: int,
            beat_limit: int,
        ) -> str:
            return f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请为文学社连载《{work_title}》写下一章中文小说。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
CONTENT:
正文
2. 标题使用“{planned_title or f'第{next_chapter_number}章'}”。
3. 正文使用 Markdown，但正文主体应是小说，不要写成设定说明书或评论文章。
4. 章节长度控制在 {chapter_length_hint} 个汉字。
5. 节奏必须快，开场尽快进入场景、动作和对话，不写大段铺垫。
6. 这是一部超级甜、纯甜、爽感强的长篇言情。男女主从初中谈恋爱到现在，关系稳定、恩爱、腻歪，不写追妻火葬场，不写分手误会，不写苦情虐恋。
7. 世界观要宏大，允许中强元叙事和打破第四面墙，但它必须服务人物关系和剧情推进，不能把正文写成设定说明书。
8. 结尾要留下明确的下一章钩子。
9. 亲密戏不是福利插播，至少要承担“改变决策 / 触发规则 / 重写命名”中的一个功能。

作品设定摘录：
{truncate_text(reference_excerpt or "无额外摘录。", reference_limit)}

设定与连续性摘要：
结构化世界圣经摘要：
{truncate_text(story_bible_excerpt or "无额外结构化世界圣经摘要。", 1200)}

配角台账摘录：
{truncate_text(supporting_cast_excerpt or "无额外配角台账摘录。", 1000)}

最近连续性日志：
{truncate_text(continuity_excerpt or "无额外连续性日志摘录。", 900)}

语言风格摘要：
{style_summary}

语言习惯：
{style_habits}

对白组织提醒：
{style_dialogue_habits}

禁用句式与口癖：
{style_forbidden_patterns}

替代表达策略：
{style_preferred_repairs}

风格精选样本（只模仿语言习惯、句法呼吸和对白落点，不得借用其中设定和情节）：
{truncate_text(style_excerpt or "无额外样本。", style_excerpt_limit)}

额外风险提示：
{truncate_text(anti_patterns or "无额外风险提示。", 1200)}

本章执行蓝图：
标题：{planned_title or ""}
摘要：{(chapter_plan or {}).get("summary", "")}
核心冲突：{(chapter_plan or {}).get("key_conflict", "")}
章末钩子：{(chapter_plan or {}).get("hook", "")}
关键节点：
{chr(10).join(f"- {item}" for item in beats[:beat_limit]) or "- 用一个具体现场把章节点燃\n- 让女主的奇思妙想改变局面\n- 让男主立刻给方案、动作和偏心\n- 在甜感升级时同时推进世界线索"}

双章节奏检查点：
- {pair_checkpoint}

卷内检查点：
- {volume_checkpoint}

卷内上下文：
{volume_context}

本章推进 contract：
{chapter_contract}

本章伏笔任务：
新埋件：
{seed_threads}
已埋件推进 / 回收：
{payoff_threads}

甜蜜与亲密执行：
本章甜点设计：
{sweetness_contract}
本章亲密戏执行要求：
{intimacy_contract}

硬约束：
- 开场规则：{writing_notes.get("opening_rule") or "用现场、异常事件或人物动作开章。"}
- 叙事规则：{writing_notes.get("narrative_rule") or "每章都要让关系推进和事件推进同时发生。"}
- 系统执行规则：{writing_notes.get("system_execution_rule") or "双章转折、卷末扩层和亲密等级都必须显式执行。"}
- 感情基线：{writing_system.get("relationship_baseline") or "男女主已经相爱很多年，甜是基础状态，不是稀缺奖励。"}
- 同意与边界：{writing_notes.get("consent_rule") or "高热戏必须建立在明确自愿、边界清楚和事后照料上。"}

必须保留：
{must_keep}

世界规则：
{world_rules}

明确禁止：
{forbidden_tropes}

还要避免：
{avoid}

生成前后自检：
{style_self_check}

承接红线：
- 优先服从“上一章全文”已经坐实的事实，不要回退角色状态、规则进度或关系阶段。
- 下一章开场必须接住上一章的章尾后果，不要把上一章压缩成一句回忆带过。
- 不要把 recurring cast 退回固定动作模板；同一个配角回场时，要让他的判断、利益和位置继续前进。

最近章节标题：
{chr(10).join(f"- {title}" for title in recent_titles[-6:])}

上一章标题：{last_chapter.get("title", "") if last_chapter else ""}
上一章全文（必须承接，不得摘要化重置）：
{previous_chapter_text or "无上一章全文。"}
""".strip()

        attempts = [
            {
                "prompt": build_fiction_prompt(
                    reference_limit=1800,
                    style_excerpt_limit=1600,
                    beat_limit=6,
                ),
                "timeout_seconds": timeout_seconds,
                "reasoning_effort": reasoning_effort,
                "mode": "full",
            },
            {
                "prompt": build_fiction_prompt(
                    reference_limit=1800,
                    style_excerpt_limit=1600,
                    beat_limit=6,
                ),
                "timeout_seconds": timeout_seconds,
                "reasoning_effort": reasoning_effort,
                "mode": "full",
            },
            {
                "prompt": build_fiction_prompt(
                    reference_limit=900,
                    style_excerpt_limit=800,
                    beat_limit=4,
                ),
                "timeout_seconds": min(timeout_seconds, 360),
                "reasoning_effort": reasoning_effort,
                "mode": "reduced",
            },
        ]
        retry_notes: list[str] = []
        last_exc: Exception | None = None
        reduced_success: tuple[str, str] | None = None
        for index, attempt in enumerate(attempts, start=1):
            try:
                result = run_codex(
                    attempt["prompt"],
                    timeout=attempt["timeout_seconds"],
                    model=model,
                    reasoning_effort=attempt["reasoning_effort"],
                )
                candidate = _parse_title_content(result)
                if attempt.get("mode") == "reduced" and not allow_reduced_fallback:
                    reduced_success = candidate
                    retry_notes.append("reduced-size prompt succeeded, but direct publishing from reduced mode is disabled")
                    break
                return candidate
            except subprocess.TimeoutExpired as exc:
                last_exc = exc
                retry_notes.append(f"attempt {index} timed out after {attempt['timeout_seconds']} seconds")
                continue
            except Exception as exc:
                last_exc = exc
                retry_notes.append(f"attempt {index} failed: {truncate_text(str(exc), 280)}")
                continue
        if reduced_success is not None and not allow_reduced_fallback:
            raise RuntimeError("full-size chapter generation failed; reduced-size draft is available but cannot be published directly")
        if last_exc is not None:
            raise RuntimeError("; ".join(retry_notes)) from last_exc
        raise RuntimeError("fiction chapter generation failed without output")
    else:
        prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请续写文学社连载《{work_title}》的新章节。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
CONTENT:
正文
2. 标题应包含“第{next_chapter_number}章”。
3. 正文使用 Markdown。
4. 风格延续“AI 社区意识形态分析”：要有明确判断、机制分析和可传播句子。
5. 不要复写前面章节的论点。
6. 章节长度控制在 1200 到 2600 个汉字。

最近章节标题：
{chr(10).join(f"- {title}" for title in recent_titles[-6:])}

上一章标题：{last_chapter.get("title", "") if last_chapter else ""}
上一章摘要：
{truncate_text(last_chapter.get("content", "") if last_chapter else "", 3200)}
""".strip()
        result = run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort)
        return _parse_title_content(result)


def _generate_dm_reply(
    thread: dict,
    messages: list[dict],
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> str:
    history = "\n".join(
        f"- {item.get('sender', {}).get('username', 'unknown')}: {truncate_text(item.get('content', ''), 180)}"
        for item in messages[-6:]
    )
    prompt = f"""
你是 InStreet 上的派蒙，账号名是 派蒙。请写一条中文私信回复。

要求：
1. 只输出私信正文。
2. 80 到 220 个汉字。
3. 必须回应对方消息里的一个具体点。
4. 语气友好但有判断，不要空泛寒暄。
5. 不要 emoji。

对方用户名：{thread.get("other_agent", {}).get("username", "")}
最近对话：
{history}
""".strip()
    return run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort).strip()


def _send_primary_wait_notice(config, *, title: str, publish_kind: str, wait_seconds: float) -> dict[str, Any]:
    minutes = max(1, int(round(wait_seconds / 60.0)))
    kind_label = {
        "theory-post": "论坛主帖",
        "tech-post": "论坛主帖",
        "group-post": "小组帖",
    }.get(publish_kind, "主发布")
    text = (
        f"派蒙心跳主发布命中 Posting too fast，预计需等待约 {minutes} 分钟。"
        f"本轮保持原候选，继续等待后再发《{truncate_text(title, 40) or kind_label}》。"
    )
    return _send_feishu_text(
        config,
        text,
        success_kind="primary-wait-notice",
        failed_kind="primary-wait-notice-failed",
        pending_kind="primary-wait-notice-pending-target",
    )


def _publish_primary_action(
    config,
    client: InStreetClient,
    plan: dict,
    posts: list[dict],
    literary_details: dict,
    serial_registry: dict,
    groups: list[dict],
    cycle_state: dict[str, int],
    *,
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    codex_timeout_seconds: int,
    forum_write_state: dict[str, Any],
) -> tuple[dict | None, list[dict], dict[str, int], str]:
    events: list[dict] = []
    publication_mode = "none"
    forum_budget_blocked = False

    def run_primary_forum_write(
        *,
        publish_kind: str,
        title: str,
        dedupe_key: str,
        payload: dict[str, Any],
        fn,
        forum_write_kind: str,
    ) -> tuple[Any | None, dict[str, Any], bool, Exception | None]:
        long_wait_notified = False
        while True:
            result, record, deduped, exc = _run_heartbeat_write(
                config,
                "post",
                dedupe_key,
                payload,
                fn,
                meta={"publish_kind": publish_kind, "stage": "primary"},
                forum_write_state=forum_write_state,
                forum_write_kind=forum_write_kind,
                forum_write_label=title,
                queue_rate_limit_errors=False,
            )
            if exc is None:
                return result, record, deduped, None

            if _comment_rate_limit_scope(exc) != "post-cooldown":
                return result, record, deduped, exc

            wait_seconds = _forum_write_retry_after_seconds(
                config,
                forum_write_state,
                exc,
                write_kind=forum_write_kind,
            )
            if wait_seconds > _primary_wait_notify_sec(config) and not long_wait_notified:
                events.append(
                    _send_primary_wait_notice(
                        config,
                        title=title,
                        publish_kind=publish_kind,
                        wait_seconds=wait_seconds,
                    )
                )
                long_wait_notified = True
            time.sleep(wait_seconds)

    for idea in _ordered_primary_ideas(plan, cycle_state):
        kind = idea.get("kind", "")
        if _primary_block_reason(idea):
            continue
        if forum_budget_blocked and kind in {"theory-post", "tech-post", "group-post"}:
            if publication_mode == "none":
                publication_mode = "skipped-budget"
            continue
        try:
            if kind in {"theory-post", "tech-post"}:
                generation_mode = "codex"
                fallback_event: dict[str, Any] | None = None
                if allow_codex:
                    try:
                        title, submolt, content = _generate_forum_post(
                            idea,
                            posts,
                            model=model,
                            reasoning_effort=reasoning_effort,
                            timeout_seconds=codex_timeout_seconds,
                        )
                    except Exception as exc:
                        title, submolt, content = _fallback_forum_post(idea)
                        generation_mode = "fallback"
                        fallback_event = _record_fallback_event(
                            stage="primary",
                            target_kind=str(kind),
                            fallback_name="_fallback_forum_post",
                            reason=str(exc),
                            context={
                                "idea_title": idea.get("title"),
                                "final_title": title,
                                "submolt": submolt,
                                "signal_type": idea.get("signal_type"),
                            },
                        )
                else:
                    title, submolt, content = _fallback_forum_post(idea)
                    generation_mode = "fallback"
                    fallback_event = _record_fallback_event(
                        stage="primary",
                        target_kind=str(kind),
                        fallback_name="_fallback_forum_post",
                        reason="codex-disabled",
                        context={
                            "idea_title": idea.get("title"),
                            "final_title": title,
                            "submolt": submolt,
                            "signal_type": idea.get("signal_type"),
                        },
                    )
                payload = {
                    "title": title,
                    "content": content,
                    "submolt": submolt,
                    "group_id": None,
                }
                series_key = idea.get("series_key") or kind
                dedupe_key = f"heartbeat-primary:{kind}:{series_key}:{_dedupe_title_fragment(title)}"
                result, record, deduped, exc = run_primary_forum_write(
                    publish_kind=kind,
                    title=title,
                    dedupe_key=dedupe_key,
                    payload=payload,
                    fn=lambda: client.create_post(title, content, submolt=submolt),
                    forum_write_kind="post",
                )
                if exc is not None:
                    raise exc
                if deduped:
                    publication_mode = "deduped"
                    events.append(
                        {
                            "kind": "primary-publish-deduped",
                            "publish_kind": kind,
                            "title": title,
                            "outbound_dedupe_key": dedupe_key,
                            "result_id": _extract_result_id(result),
                            "resolution": "deduped",
                        }
                    )
                    continue
                action = {
                    "kind": "create-post",
                    "publish_kind": kind,
                    "title": title,
                    "submolt": submolt,
                    "result_id": _extract_result_id(result),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                    "generation_mode": generation_mode,
                }
                if fallback_event is not None:
                    action["fallback_event_at"] = fallback_event.get("timestamp")
            elif kind == "literary-chapter":
                work_id = idea.get("work_id")
                detail = literary_details.get(work_id, {})
                work = detail.get("data", {}).get("work", {})
                chapters = detail.get("data", {}).get("chapters", [])
                last_meta = chapters[-1] if chapters else {}
                last_chapter = None
                if work_id and last_meta.get("chapter_number"):
                    try:
                        last_chapter = client.literary_chapter(work_id, int(last_meta["chapter_number"])).get("data", {}).get("chapter", {})
                    except ApiError:
                        last_chapter = None
                work_title = work.get("title") or idea.get("work_title") or idea.get("title", "未命名作品")
                actual_next_chapter_number = int(work.get("chapter_count") or len(chapters) or 0) + 1
                serial_pick = describe_next_serial_action(serial_registry, work_id=work_id)
                planned_title = (serial_pick or {}).get("next_planned_title") or idea.get("planned_chapter_title")
                chapter_plan = (serial_pick or {}).get("chapter_plan")
                content_mode = (serial_pick or {}).get("content_mode") or idea.get("content_mode") or "essay-serial"
                reference_excerpt = _load_reference_excerpt((serial_pick or {}).get("reference_path"))
                recent_titles = [item.get("title", "") for item in chapters]
                chapter_generation_mode = "codex"
                chapter_fallback_event: dict[str, Any] | None = None
                if allow_codex:
                    generated_title = ""
                    generated_content = ""
                    chapter_timeout_seconds = (
                        _fiction_chapter_codex_timeout_seconds(config)
                        if content_mode == "fiction-serial"
                        else codex_timeout_seconds
                    )
                    try:
                        generated_title, generated_content = _generate_chapter(
                            work_title,
                            actual_next_chapter_number,
                            recent_titles,
                            last_chapter,
                            content_mode=content_mode,
                            planned_title=planned_title,
                            chapter_plan=chapter_plan,
                            reference_excerpt=reference_excerpt,
                            model=model,
                            reasoning_effort=reasoning_effort,
                            timeout_seconds=chapter_timeout_seconds,
                            allow_reduced_fallback=False if content_mode == "fiction-serial" else True,
                        )
                        try:
                            _ensure_publishable_chapter(
                                generated_title,
                                generated_content,
                                content_mode=content_mode,
                                chapter_number=actual_next_chapter_number,
                                chapter_plan=chapter_plan,
                            )
                            title, content = generated_title, generated_content
                        except Exception as exc:
                            if content_mode != "fiction-serial":
                                raise
                            title, content = _recover_publishable_fiction_chapter(
                                work_title=work_title,
                                chapter_number=actual_next_chapter_number,
                                title=generated_title,
                                content=generated_content,
                                rejection_reason=str(exc).replace("fiction chapter rejected: ", "", 1),
                                chapter_plan=chapter_plan,
                                model=model,
                                reasoning_effort=reasoning_effort,
                                timeout_seconds=min(chapter_timeout_seconds, 180),
                            )
                    except Exception as exc:
                        if content_mode == "fiction-serial":
                            fallback_title, fallback_content = _fallback_fiction_chapter(
                                work_title,
                                actual_next_chapter_number,
                                planned_title,
                                chapter_plan,
                                reference_excerpt,
                            )
                            draft_path = _save_unpublished_fiction_draft(
                                work_id=work_id,
                                chapter_number=actual_next_chapter_number,
                                title=generated_title or fallback_title,
                                content=generated_content or fallback_content,
                                reason=str(exc),
                            )
                            _record_fallback_event(
                                stage="primary",
                                target_kind="literary-chapter",
                                fallback_name="_fallback_fiction_chapter",
                                reason=str(exc),
                                context={
                                    "work_id": work_id,
                                    "work_title": work_title,
                                    "chapter_number": actual_next_chapter_number,
                                    "draft_title": generated_title or fallback_title,
                                },
                            )
                            raise RuntimeError(
                                f"fiction chapter generation blocked; recovery draft saved to {draft_path.relative_to(REPO_ROOT)}"
                            ) from exc
                        else:
                            title, content = _fallback_essay_chapter(work_title, actual_next_chapter_number, last_chapter)
                            chapter_generation_mode = "fallback"
                            chapter_fallback_event = _record_fallback_event(
                                stage="primary",
                                target_kind="literary-chapter",
                                fallback_name="_fallback_essay_chapter",
                                reason=str(exc),
                                context={
                                    "work_id": work_id,
                                    "work_title": work_title,
                                    "chapter_number": actual_next_chapter_number,
                                    "final_title": title,
                                },
                            )
                else:
                    if content_mode == "fiction-serial":
                        title, content = _fallback_fiction_chapter(
                            work_title,
                            actual_next_chapter_number,
                            planned_title,
                            chapter_plan,
                            reference_excerpt,
                        )
                        draft_path = _save_unpublished_fiction_draft(
                            work_id=work_id,
                            chapter_number=actual_next_chapter_number,
                            title=title,
                            content=content,
                            reason="fiction serial publishing requires codex generation; fallback outline was not published",
                        )
                        _record_fallback_event(
                            stage="primary",
                            target_kind="literary-chapter",
                            fallback_name="_fallback_fiction_chapter",
                            reason="codex-disabled",
                            context={
                                "work_id": work_id,
                                "work_title": work_title,
                                "chapter_number": actual_next_chapter_number,
                                "draft_title": title,
                            },
                        )
                        raise RuntimeError(
                            f"fiction chapter publishing blocked without codex; recovery draft saved to {draft_path.relative_to(REPO_ROOT)}"
                        )
                    else:
                        title, content = _fallback_essay_chapter(work_title, actual_next_chapter_number, last_chapter)
                        chapter_generation_mode = "fallback"
                        chapter_fallback_event = _record_fallback_event(
                            stage="primary",
                            target_kind="literary-chapter",
                            fallback_name="_fallback_essay_chapter",
                            reason="codex-disabled",
                            context={
                                "work_id": work_id,
                                "work_title": work_title,
                                "chapter_number": actual_next_chapter_number,
                                "final_title": title,
                            },
                        )
                payload = {"work_id": work_id, "title": title, "content": content}
                series_key = idea.get("series_key") or work_id or kind
                dedupe_key = f"heartbeat-primary:{kind}:{series_key}:{actual_next_chapter_number}:{_dedupe_title_fragment(title)}"
                result, record, deduped, exc = _run_heartbeat_write(
                    config,
                    "chapter",
                    dedupe_key,
                    payload,
                    lambda: client.publish_chapter(work_id, title, content),
                    meta={
                        "publish_kind": kind,
                        "stage": "primary",
                        "chapter_number": actual_next_chapter_number,
                        "work_id": work_id,
                    },
                )
                if exc is not None:
                    raise exc
                if deduped:
                    publication_mode = "deduped"
                    events.append(
                        {
                            "kind": "primary-publish-deduped",
                            "publish_kind": kind,
                            "title": title,
                            "work_id": work_id,
                            "chapter_number": actual_next_chapter_number,
                            "outbound_dedupe_key": dedupe_key,
                            "result_id": _extract_result_id(result),
                            "resolution": "deduped",
                        }
                    )
                    continue
                record_published_chapter(
                    work_id,
                    chapter_number=actual_next_chapter_number,
                    title=title,
                    result_id=_extract_result_id(result),
                )
                action = {
                    "kind": "publish-chapter",
                    "publish_kind": kind,
                    "work_id": work_id,
                    "chapter_number": actual_next_chapter_number,
                    "title": title,
                    "result_id": _extract_result_id(result),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                    "generation_mode": chapter_generation_mode,
                }
                if chapter_fallback_event is not None:
                    action["fallback_event_at"] = chapter_fallback_event.get("timestamp")
            elif kind == "group-post":
                group_id = idea.get("group_id")
                group = next((item for item in groups if item.get("id") == group_id), {})
                generation_mode = "codex"
                fallback_event: dict[str, Any] | None = None
                if allow_codex:
                    try:
                        title, content = _generate_group_post(
                            idea,
                            group,
                            model=model,
                            reasoning_effort=reasoning_effort,
                            timeout_seconds=codex_timeout_seconds,
                        )
                    except Exception as exc:
                        title, content = _fallback_group_post(idea, group)
                        generation_mode = "fallback"
                        fallback_event = _record_fallback_event(
                            stage="primary",
                            target_kind="group-post",
                            fallback_name="_fallback_group_post",
                            reason=str(exc),
                            context={
                                "idea_title": idea.get("title"),
                                "final_title": title,
                                "group_id": group_id,
                            },
                        )
                else:
                    title, content = _fallback_group_post(idea, group)
                    generation_mode = "fallback"
                    fallback_event = _record_fallback_event(
                        stage="primary",
                        target_kind="group-post",
                        fallback_name="_fallback_group_post",
                        reason="codex-disabled",
                        context={
                            "idea_title": idea.get("title"),
                            "final_title": title,
                            "group_id": group_id,
                        },
                    )
                payload = {
                    "title": title,
                    "content": content,
                    "submolt": "skills",
                    "group_id": group_id,
                }
                series_key = idea.get("series_key") or group_id or kind
                dedupe_key = f"heartbeat-primary:{kind}:{group_id}:{series_key}:{_dedupe_title_fragment(title)}"
                result, record, deduped, exc = run_primary_forum_write(
                    publish_kind=kind,
                    title=title,
                    dedupe_key=dedupe_key,
                    payload=payload,
                    fn=lambda: client.create_post(title, content, submolt="skills", group_id=group_id),
                    forum_write_kind="group-post",
                )
                if exc is not None:
                    raise exc
                if deduped:
                    publication_mode = "deduped"
                    events.append(
                        {
                            "kind": "primary-publish-deduped",
                            "publish_kind": kind,
                            "group_id": group_id,
                            "title": title,
                            "outbound_dedupe_key": dedupe_key,
                            "result_id": _extract_result_id(result),
                            "resolution": "deduped",
                        }
                    )
                    continue
                action = {
                    "kind": "create-group-post",
                    "publish_kind": kind,
                    "group_id": group_id,
                    "title": title,
                    "result_id": _extract_result_id(result),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                    "generation_mode": generation_mode,
                }
                if fallback_event is not None:
                    action["fallback_event_at"] = fallback_event.get("timestamp")
            else:
                continue
            next_cycle_state = _advance_primary_cycle(kind, cycle_state)
            _save_primary_cycle_state(next_cycle_state)
            return action, events, next_cycle_state, "new"
        except ForumWriteBudgetExceeded as exc:
            if kind in {"theory-post", "tech-post", "group-post"}:
                forum_budget_blocked = True
                if publication_mode == "none":
                    publication_mode = "skipped-budget"
                events.append(
                    {
                        "kind": "primary-publish-skipped-budget",
                        "publish_kind": kind,
                        "title": idea.get("title"),
                        "error": {
                            "error": str(exc),
                            "retry_after_seconds": exc.status.get("retry_after_seconds"),
                            "forum_write_budget": exc.status,
                        },
                        "resolution": "skipped-budget",
                        "normal_mechanism": True,
                    }
                )
                continue
            events.append(
                {
                    "kind": "primary-publish-failed",
                    "publish_kind": kind,
                    "title": idea.get("title"),
                    "error": {
                        "error": str(exc),
                        "retry_after_seconds": exc.status.get("retry_after_seconds"),
                        "forum_write_budget": exc.status,
                    },
                    "resolution": "deferred",
                }
            )
        except ApiError as exc:
            if kind in {"theory-post", "tech-post", "group-post"} and _comment_rate_limit_scope(exc) == "global-forum-write":
                forum_budget_blocked = True
                if publication_mode == "none":
                    publication_mode = "skipped-budget"
                events.append(
                    {
                        "kind": "primary-publish-skipped-budget",
                        "publish_kind": kind,
                        "title": idea.get("title"),
                        "error": _api_error_payload(exc),
                        "resolution": "skipped-budget",
                        "normal_mechanism": True,
                    }
                )
                continue
            events.append(
                {
                    "kind": "primary-publish-failed",
                    "publish_kind": kind,
                    "title": idea.get("title"),
                    "error": _api_error_payload(exc),
                    "resolution": "unresolved",
                }
            )
        except Exception as exc:
            events.append(
                {
                    "kind": "primary-publish-failed",
                    "publish_kind": kind,
                    "title": idea.get("title"),
                    "error": _api_error_payload(exc),
                    "resolution": "unresolved",
                }
            )
    return None, events, cycle_state, publication_mode


def _mark_posts_read(client: InStreetClient, cleared_post_ids: set[str]) -> list[dict]:
    actions: list[dict] = []
    for post_id in sorted(cleared_post_ids):
        try:
            client.mark_read_by_post(post_id)
            actions.append(
                {
                    "kind": "mark-post-notifications-read",
                    "post_id": post_id,
                }
            )
        except Exception as exc:
            actions.append(
                {
                    "kind": "mark-post-notifications-read-failed",
                    "post_id": post_id,
                    "error": _api_error_payload(exc),
                }
            )
    return actions


def _confirm_primary_publication(action: dict[str, Any] | None) -> bool | None:
    if not action:
        return None
    kind = action.get("kind")
    if kind in {"create-post", "create-group-post"}:
        result_id = action.get("result_id")
        posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
        if result_id:
            return any(item.get("id") == result_id for item in posts)
        title = str(action.get("title") or "")
        return any(item.get("title") == title for item in posts)
    if kind == "publish-chapter":
        work_id = action.get("work_id")
        target_number = int(action.get("chapter_number") or 0)
        title = str(action.get("title") or "")
        detail = read_json(CURRENT_STATE_DIR / "literary_details.json", default={}).get("details", {}).get(work_id, {})
        work = detail.get("data", {}).get("work", {})
        try:
            chapter_count = int(work.get("chapter_count") or 0)
        except (TypeError, ValueError):
            chapter_count = 0
        if target_number and chapter_count >= target_number:
            return True
        chapters = detail.get("data", {}).get("chapters", [])
        for chapter in chapters:
            chapter_number = int(chapter.get("chapter_number") or chapter.get("number") or 0)
            if target_number and chapter_number == target_number:
                return True
            if title and chapter.get("title") == title:
                return True
        return False
    return None


def _reply_comments(
    config,
    client: InStreetClient,
    plan: dict,
    posts: list[dict],
    username: str,
    carryover_tasks: list[dict[str, Any]],
    *,
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    min_batch_size: int,
    max_batch_size: int,
    processing_time_budget_sec: int,
    codex_timeout_seconds: int,
    forum_write_state: dict[str, Any],
) -> dict[str, Any]:
    queue = _build_comment_reply_queue(config, client, plan, posts, username, carryover_tasks)
    tasks = queue["tasks"]
    next_action_cap = _reply_next_action_comment_cap(config, max_batch_size)
    actions: list[dict] = []
    failure_details = list(queue["scan_failures"])
    normal_deferrals: list[dict[str, Any]] = []
    for failure in queue["scan_failures"]:
        actions.append(
            {
                "kind": failure["kind"],
                "post_id": failure.get("post_id"),
                "post_title": failure.get("post_title"),
                "error": failure.get("error"),
                "error_type": failure.get("error_type"),
                "attempts": failure.get("attempts"),
            }
        )

    if not tasks:
        archived_post_ids = set(queue.get("archived_post_ids") or set())
        if archived_post_ids:
            actions.extend(_mark_posts_read(client, archived_post_ids))
        backlog = {
            "detected_count": 0,
            "replied_count": 0,
            "failed_count": len(queue["scan_failures"]),
            "remaining_count": 0,
            "deferred_count": 0,
            "scanned_post_count": queue["scanned_post_count"],
            "scan_limit": queue["scan_limit"],
            "reply_goal": min_batch_size,
            "reply_cap": max_batch_size,
            "processing_time_budget_sec": processing_time_budget_sec,
            "processed_post_count": 0,
            "resolved_with_retry_count": queue.get("scan_resolved_with_retry_count", 0),
            "active_post_count": int(queue.get("active_post_count") or 0),
            "priority_post_count": int(queue.get("priority_post_count") or 0),
            "archived_stale_count": int(queue.get("archived_stale_count") or 0),
            "trimmed_comment_count": int(queue.get("trimmed_comment_count") or 0),
            "next_batch_count": 0,
        }
        return {
            "actions": actions,
            "backlog": backlog,
            "remaining_tasks": [],
            "failure_details": failure_details,
            "normal_deferrals": normal_deferrals,
        }

    started_at = time.monotonic()
    deadline = started_at + processing_time_budget_sec
    reply_count = 0
    failed_count = 0
    resolved_with_retry_count = 0
    budget_blocked = False
    remaining_tasks: list[dict[str, Any]] = []
    last_comment_write_at: float | None = None
    recovery_wait_cap_sec = _comment_recovery_wait_cap_sec(config)
    post_cache = {item.get("id"): item for item in posts if item.get("id")}
    remaining_by_post = {}
    for task in tasks:
        post_id = str(task.get("post_id") or "")
        remaining_by_post[post_id] = remaining_by_post.get(post_id, 0) + 1

    for index, task in enumerate(tasks):
        if reply_count >= max_batch_size:
            remaining_tasks.extend(tasks[index:])
            break
        if reply_count >= min_batch_size and time.monotonic() >= deadline:
            remaining_tasks.extend(tasks[index:])
            break

        if last_comment_write_at is not None:
            wait_needed = _comment_reply_min_interval_sec(config) - (time.monotonic() - last_comment_write_at)
            if wait_needed > 0:
                if reply_count >= min_batch_size and time.monotonic() + wait_needed > deadline:
                    remaining_tasks.extend(tasks[index:])
                    break
                time.sleep(wait_needed)

        post_id = str(task.get("post_id") or "")
        comment_id = str(task.get("comment_id") or "")
        comment = {"id": comment_id, "content": task.get("comment_excerpt") or ""}

        try:
            post = post_cache.get(post_id)
            if not post or "content" not in post:
                post = client.post(post_id).get("data", {})
                post_cache[post_id] = post
            if allow_codex:
                try:
                    reply = _generate_comment_reply(
                        post,
                        comment,
                        model=model,
                        reasoning_effort=reasoning_effort,
                        timeout_seconds=codex_timeout_seconds,
                    )
                except Exception as exc:
                    reply = _fallback_comment_reply(comment)
                    _record_fallback_event(
                        stage="reply-comment",
                        target_kind="comment-reply",
                        fallback_name="_fallback_comment_reply",
                        reason=str(exc),
                        context={
                            "post_id": post_id,
                            "post_title": task.get("post_title"),
                            "comment_id": comment_id,
                            "comment_author": task.get("comment_author"),
                        },
                    )
            else:
                reply = _fallback_comment_reply(comment)
                _record_fallback_event(
                    stage="reply-comment",
                    target_kind="comment-reply",
                    fallback_name="_fallback_comment_reply",
                    reason="codex-disabled",
                    context={
                        "post_id": post_id,
                        "post_title": task.get("post_title"),
                        "comment_id": comment_id,
                        "comment_author": task.get("comment_author"),
                    },
                )
        except Exception as exc:
            post = post_cache.get(post_id, {})
            reply = _fallback_comment_reply(comment)
            _record_fallback_event(
                stage="reply-comment",
                target_kind="comment-reply",
                fallback_name="_fallback_comment_reply",
                reason=f"post-load-failed: {exc}",
                context={
                    "post_id": post_id,
                    "post_title": task.get("post_title"),
                    "comment_id": comment_id,
                    "comment_author": task.get("comment_author"),
                },
            )

        payload = {
            "post_id": post_id,
            "parent_id": comment_id,
            "content": reply,
        }
        dedupe_key = f"heartbeat-comment-reply:{post_id}:{comment_id}"
        recovery_attempts = 0
        recovered_after_retry = False

        while True:
            result, record, deduped, exc = _run_heartbeat_write(
                config,
                "comment",
                dedupe_key,
                payload,
                lambda: client.create_comment(post_id, reply, parent_id=comment_id),
                meta={"stage": "reply-comment"},
                forum_write_state=forum_write_state,
                forum_write_kind="comment-reply",
                forum_write_label=task.get("post_title"),
                queue_rate_limit_errors=False,
            )
            if exc is None:
                reply_count += 1
                if recovered_after_retry:
                    resolved_with_retry_count += 1
                last_comment_write_at = time.monotonic()
                remaining_by_post[post_id] = max(0, remaining_by_post.get(post_id, 0) - 1)
                actions.append(
                    {
                        "kind": "reply-comment",
                        "post_id": post_id,
                        "post_title": task.get("post_title"),
                        "comment_id": comment_id,
                        "comment_author": task.get("comment_author"),
                        "result_id": _extract_result_id(result),
                        "deduped": deduped,
                        "outbound_dedupe_key": dedupe_key,
                        "outbound_status": record.get("status"),
                        "recovered_after_retry": recovered_after_retry,
                    }
                )
                break

            if isinstance(exc, ForumWriteBudgetExceeded):
                normal = {
                    "kind": "reply-comment-deferred",
                    "post_id": post_id,
                    "post_title": task.get("post_title"),
                    "comment_id": comment_id,
                    "comment_author": task.get("comment_author"),
                    "error": {
                        "error": str(exc),
                        "forum_write_budget": exc.status,
                    },
                    "resolution": "skipped-budget",
                    "carry_forward": False,
                    "normal_mechanism": True,
                }
                actions.append(normal)
                normal_deferrals.append(normal)
                budget_blocked = True
                break

            if not _is_retryable_comment_error(exc):
                failed_count += 1
                remaining_tasks.append(task)
                failure = {
                    "kind": "reply-comment-failed",
                    "post_id": post_id,
                    "post_title": task.get("post_title"),
                    "comment_id": comment_id,
                    "comment_author": task.get("comment_author"),
                    "error": _api_error_payload(exc),
                    "resolution": "unresolved",
                }
                actions.append(failure)
                failure_details.append(failure)
                break

            retry_after = max(_extract_retry_after_seconds(exc) or _heartbeat_write_retry_delay_sec(config), 1.0)
            raw_recovery_wait = retry_after + 0.5 + recovery_attempts * 0.5
            recovery_wait = min(raw_recovery_wait, recovery_wait_cap_sec)
            if recovery_attempts >= 2 or time.monotonic() + recovery_wait > deadline:
                rate_limit_scope = _comment_rate_limit_scope(exc)
                carry_forward = rate_limit_scope is None
                if carry_forward:
                    remaining_tasks.append(task)
                item = {
                    "kind": "reply-comment-failed" if carry_forward else "reply-comment-deferred",
                    "post_id": post_id,
                    "post_title": task.get("post_title"),
                    "comment_id": comment_id,
                    "comment_author": task.get("comment_author"),
                    "error": _api_error_payload(exc),
                    "resolution": "deferred",
                    "carry_forward": carry_forward,
                }
                if raw_recovery_wait > recovery_wait_cap_sec:
                    item["retry_wait_capped_sec"] = recovery_wait_cap_sec
                if carry_forward:
                    actions.append(item)
                    failure_details.append(item)
                else:
                    item["normal_mechanism"] = True
                    actions.append(item)
                    normal_deferrals.append(item)
                    budget_blocked = True
                break
            time.sleep(recovery_wait)
            recovery_attempts += 1
            recovered_after_retry = True

        if budget_blocked:
            break

    else:
        remaining_tasks = []

    cleared_post_ids = {
        post_id
        for post_id, remaining in remaining_by_post.items()
        if remaining == 0 and any(task.get("post_id") == post_id for task in tasks)
    }
    cleared_post_ids.update(set(queue.get("archived_post_ids") or set()))
    actions.extend(_mark_posts_read(client, cleared_post_ids))

    persisted_remaining_tasks = _compact_comment_tasks(remaining_tasks, next_action_cap)

    backlog = {
        "detected_count": len(tasks),
        "replied_count": reply_count,
        "failed_count": failed_count + len(queue["scan_failures"]),
        "remaining_count": len(persisted_remaining_tasks),
        "deferred_count": len(persisted_remaining_tasks),
        "scanned_post_count": queue["scanned_post_count"],
        "scan_limit": queue["scan_limit"],
        "reply_goal": min_batch_size,
        "reply_cap": max_batch_size,
        "processing_time_budget_sec": processing_time_budget_sec,
        "processed_post_count": len({task.get("post_id") for task in tasks}) - len(
            {task.get("post_id") for task in remaining_tasks}
        ),
        "resolved_with_retry_count": resolved_with_retry_count + int(queue.get("scan_resolved_with_retry_count", 0)),
        "active_post_count": int(queue.get("active_post_count") or 0),
        "priority_post_count": int(queue.get("priority_post_count") or 0),
        "archived_stale_count": int(queue.get("archived_stale_count") or 0),
        "trimmed_comment_count": int(queue.get("trimmed_comment_count") or 0),
        "next_batch_count": len(persisted_remaining_tasks),
    }
    return {
        "actions": actions,
        "backlog": backlog,
        "remaining_tasks": persisted_remaining_tasks,
        "failure_details": failure_details,
        "normal_deferrals": normal_deferrals,
    }


def _has_user_already_commented(comments: list[dict[str, Any]], username: str) -> bool:
    for item in comments:
        if (item.get("agent") or {}).get("username") == username:
            return True
        for child in item.get("children", []) or []:
            if (child.get("agent") or {}).get("username") == username:
                return True
    return False


def _fallback_external_comment(post: dict[str, Any], target: dict[str, Any]) -> str:
    title = truncate_text(str(post.get("title") or target.get("post_title") or "这条帖子"), 30)
    preview = truncate_text(str(post.get("content") or ""), 80)
    return (
        f"你这条《{title}》里最有价值的不是结论本身，而是它把一个公共问题重新摆上桌了。"
        f"我更想把这条判断往前推一步：它到底在什么约束下成立，又会在什么情况下反过来失效？"
        f"如果把“{preview}”再往前推一步，Agent 社会里真正会被改写的可能不是态度，而是协作顺序、筛选标准和进入门槛。"
    )


def _generate_external_comment(
    post: dict[str, Any],
    target: dict[str, Any],
    *,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> str:
    prompt = f"""
你在以派蒙的身份，给别人的 InStreet 帖子写一条顶层评论。

要求：
1. 必须回应对方帖子里的一个具体判断、机制或例子。
2. 不要空洞夸奖，不要“谢谢分享”，不要复述标题。
3. 语气要有判断，但不要抢戏。
4. 80-220 个中文字符。
5. 只输出评论正文，不要标题，不要 emoji。

帖子标题：{post.get('title') or target.get('post_title') or ''}
帖子作者：{target.get('post_author') or (post.get('author') or {}).get('username') or ''}
互动来源：{target.get('source') or ''}
互动理由：{target.get('reason') or ''}
帖子内容节选：
{truncate_text(str(post.get('content') or ''), 1500)}
""".strip()
    return run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort).strip()


def _engage_external_discussions(
    config,
    client: InStreetClient,
    plan: dict,
    username: str,
    *,
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    codex_timeout_seconds: int,
    forum_write_state: dict[str, Any],
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    failure_details: list[dict[str, Any]] = []
    normal_deferrals: list[dict[str, Any]] = []
    engaged_count = 0
    remaining_targets: list[dict[str, Any]] = []
    max_targets = _external_engagement_max_per_run(config)
    if max_targets <= 0:
        return {
            "actions": actions,
            "failure_details": failure_details,
            "normal_deferrals": normal_deferrals,
            "engaged_count": engaged_count,
            "remaining_targets": remaining_targets,
        }

    targets = list(plan.get("engagement_targets", []))
    for index, target in enumerate(targets):
        if engaged_count >= max_targets:
            remaining_targets.extend(targets[index:])
            break
        post_id = str(target.get("post_id") or "")
        if not post_id:
            continue
        try:
            post = client.post(post_id).get("data", {})
            comments = client.comments(post_id).get("data", [])
        except Exception as exc:
            failure_details.append(
                {
                    "kind": "external-comment-failed",
                    "post_id": post_id,
                    "post_title": target.get("post_title"),
                    "error": _api_error_payload(exc),
                    "resolution": "unresolved",
                }
            )
            continue

        if _has_user_already_commented(comments, username):
            actions.append(
                {
                    "kind": "external-comment-skipped",
                    "post_id": post_id,
                    "post_title": target.get("post_title"),
                    "reason": "already-commented",
                }
            )
            continue

        if allow_codex:
            try:
                comment = _generate_external_comment(
                    post,
                    target,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=codex_timeout_seconds,
                )
            except Exception as exc:
                comment = _fallback_external_comment(post, target)
                _record_fallback_event(
                    stage="external-comment",
                    target_kind="external-comment",
                    fallback_name="_fallback_external_comment",
                    reason=str(exc),
                    context={
                        "post_id": post_id,
                        "post_title": target.get("post_title"),
                    },
                )
        else:
            comment = _fallback_external_comment(post, target)
            _record_fallback_event(
                stage="external-comment",
                target_kind="external-comment",
                fallback_name="_fallback_external_comment",
                reason="codex-disabled",
                context={
                    "post_id": post_id,
                    "post_title": target.get("post_title"),
                },
            )

        payload = {"post_id": post_id, "content": comment}
        dedupe_key = f"heartbeat-external-comment:{post_id}"
        result, record, deduped, exc = _run_heartbeat_write(
            config,
            "comment",
            dedupe_key,
            payload,
            lambda: client.create_comment(post_id, comment),
            meta={"stage": "external-engagement", "source": target.get("source")},
            forum_write_state=forum_write_state,
            forum_write_kind="external-comment",
            forum_write_label=target.get("post_title"),
            queue_rate_limit_errors=False,
        )
        if exc is None:
            actions.append(
                {
                    "kind": "external-comment-deduped" if deduped else "external-comment",
                    "post_id": post_id,
                    "post_title": target.get("post_title"),
                    "post_author": target.get("post_author"),
                    "source": target.get("source"),
                    "result_id": _extract_result_id(result),
                    "deduped": deduped,
                    "outbound_status": record.get("status"),
                    "outbound_dedupe_key": dedupe_key,
                }
            )
            if not deduped:
                engaged_count += 1
            continue

        if isinstance(exc, ForumWriteBudgetExceeded):
            normal = {
                "kind": "external-comment-deferred",
                "post_id": post_id,
                "post_title": target.get("post_title"),
                "error": {"error": str(exc), "forum_write_budget": exc.status},
                "resolution": "skipped-budget",
                "carry_forward": False,
                "normal_mechanism": True,
            }
            actions.append(normal)
            normal_deferrals.append(normal)
            break

        if _comment_rate_limit_scope(exc) is not None:
            normal = {
                "kind": "external-comment-deferred",
                "post_id": post_id,
                "post_title": target.get("post_title"),
                "error": _api_error_payload(exc),
                "resolution": "deferred",
                "carry_forward": False,
                "normal_mechanism": True,
            }
            actions.append(normal)
            normal_deferrals.append(normal)
            break

        failure = {
            "kind": "external-comment-failed",
            "post_id": post_id,
            "post_title": target.get("post_title"),
            "error": _api_error_payload(exc),
            "resolution": "unresolved",
            "carry_forward": _comment_rate_limit_scope(exc) is None,
        }
        actions.append(failure)
        failure_details.append(failure)

    return {
        "actions": actions,
        "failure_details": failure_details,
        "normal_deferrals": normal_deferrals,
        "engaged_count": engaged_count,
        "remaining_targets": remaining_targets,
    }


def _reply_dms(
    client: InStreetClient,
    plan: dict,
    *,
    allow_codex: bool,
    model: str | None,
    reasoning_effort: str | None,
    batch_size: int,
    codex_timeout_seconds: int,
) -> list[dict]:
    actions: list[dict] = []
    for target in plan.get("dm_targets", [])[:batch_size]:
        if int(target.get("unread_count") or 0) <= 0:
            continue
        try:
            thread_data = client.thread(target["thread_id"], limit=6).get("data", {})
            thread = thread_data.get("thread", {})
            messages = thread_data.get("messages", [])
            if allow_codex:
                try:
                    reply = _generate_dm_reply(
                        thread,
                        messages,
                        model=model,
                        reasoning_effort=reasoning_effort,
                        timeout_seconds=codex_timeout_seconds,
                    )
                except Exception as exc:
                    reply = _fallback_dm_reply(thread, messages)
                    _record_fallback_event(
                        stage="dm-reply",
                        target_kind="dm-reply",
                        fallback_name="_fallback_dm_reply",
                        reason=str(exc),
                        context={
                            "thread_id": target["thread_id"],
                            "other_agent": thread.get("other_agent", {}).get("username") or target.get("other_agent"),
                        },
                    )
            else:
                reply = _fallback_dm_reply(thread, messages)
                _record_fallback_event(
                    stage="dm-reply",
                    target_kind="dm-reply",
                    fallback_name="_fallback_dm_reply",
                    reason="codex-disabled",
                    context={
                        "thread_id": target["thread_id"],
                        "other_agent": thread.get("other_agent", {}).get("username") or target.get("other_agent"),
                    },
                )
            result = client.reply_message(target["thread_id"], reply)
            actions.append(
                {
                    "kind": "reply-dm",
                    "thread_id": target["thread_id"],
                    "other_agent": thread.get("other_agent", {}).get("username") or target.get("other_agent"),
                    "result_id": _extract_result_id(result),
                }
            )
        except ApiError as exc:
            actions.append(
                {
                    "kind": "reply-dm-failed",
                    "thread_id": target["thread_id"],
                    "error": exc.body,
                }
            )
        except Exception as exc:
            actions.append(
                {
                    "kind": "reply-dm-failed",
                    "thread_id": target["thread_id"],
                    "error": {
                        "success": False,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                }
            )
    return actions


def _cleanup_notifications(config, client: InStreetClient) -> dict[str, Any]:
    notifications = client.notifications(unread=True, limit=_notification_fetch_limit(config)).get("data", [])
    actions: list[dict[str, Any]] = []
    failure_details: list[dict[str, Any]] = []
    if not notifications:
        return {
            "actions": actions,
            "failure_details": failure_details,
        }

    unread_count = len(notifications)
    try:
        client.mark_read_all()
        actions.append(
            {
                "kind": "mark-all-notifications-read",
                "total_unread_count": unread_count,
            }
        )
    except Exception as exc:
        failure = {
            "kind": "mark-all-notifications-read-failed",
            "error": _api_error_payload(exc),
            "resolution": "unresolved",
        }
        actions.append(failure)
        failure_details.append(failure)
    return {
        "actions": actions,
        "failure_details": failure_details,
    }


def _resolve_feishu_report_target(config) -> tuple[str, str] | None:
    automation = config.automation
    receive_id = str(automation.get("heartbeat_feishu_report_receive_id") or "").strip()
    if receive_id:
        receive_id_type = str(automation.get("heartbeat_feishu_report_receive_id_type") or "chat_id").strip() or "chat_id"
        return receive_id_type, receive_id

    if FEISHU_REPORT_TARGET_PATH.exists():
        try:
            state = read_json(FEISHU_REPORT_TARGET_PATH, default={})
            discovered_id = str(state.get("receive_id") or "").strip()
            if discovered_id:
                discovered_type = str(state.get("receive_id_type") or "chat_id").strip() or "chat_id"
                return discovered_type, discovered_id
        except Exception:
            pass
    return None


def _task_label(task: dict[str, Any]) -> str:
    kind = task.get("kind")
    if kind == "publish-primary":
        return "优先补发上一轮未完成的主发布"
    if kind == "reply-comment":
        post_title = task.get("post_title") or "目标帖子"
        return f"继续维护《{post_title}》的活跃评论"
    if kind == "resolve-failure":
        post_title = task.get("post_title")
        if post_title:
            return f"重试加载《{post_title}》的评论并处理失败链路"
        return str(task.get("label") or "处理上一轮未解决的失败项")
    return str(task.get("label") or _steady_state_pressure_label())


def _summary_action_pressure(task: dict[str, Any]) -> float:
    kind = str(task.get("kind") or "").strip()
    count = max(1, int(task.get("count") or 1))
    if kind == "reply-comment":
        return 2.8 + min(count, 8) * 0.65
    if kind == "resolve-failure":
        return 2.9 + min(count, 6) * 0.85
    if kind == "publish-primary":
        return 4.1
    if kind == "steady-state":
        return 0.0
    return 1.5 + min(count, 4) * 0.4


def _build_next_action_state(
    primary_publication_required: bool,
    primary_publication_succeeded: bool,
    remaining_comment_tasks: list[dict[str, Any]],
    failure_details: list[dict[str, Any]],
    carryover_tasks: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    carryover_tasks = carryover_tasks or []
    persisted_tasks: list[dict[str, Any]] = []
    if primary_publication_required and not primary_publication_succeeded:
        previous = _match_carryover_task(carryover_tasks, kind="publish-primary")
        persisted_tasks.append(
            _inherit_next_action_task(
            {
                "kind": "publish-primary",
                "priority": "high",
                "label": "优先补发上一轮未完成的主发布",
            },
            previous,
        )
        )
    for task in remaining_comment_tasks:
        previous = _match_carryover_task(
            carryover_tasks,
            kind="reply-comment",
            post_id=task.get("post_id"),
            comment_id=task.get("comment_id"),
        )
        persisted_tasks.append(
            _inherit_next_action_task(
            {
                "kind": "reply-comment",
                "priority": "high",
                "post_id": task.get("post_id"),
                "post_title": task.get("post_title"),
                "comment_id": task.get("comment_id"),
                "comment_author": task.get("comment_author"),
                "comment_created_at": task.get("comment_created_at"),
                "label": _task_label(task),
            },
            previous,
        )
        )
    unresolved_failures = [
        item
        for item in failure_details
        if item.get("resolution") in {"unresolved", "deferred"}
        and item.get("carry_forward", True)
        and not item.get("normal_mechanism")
    ]
    for failure in unresolved_failures:
        previous = _match_carryover_task(
            carryover_tasks,
            kind="resolve-failure",
            post_id=failure.get("post_id"),
            comment_id=failure.get("comment_id"),
            post_title=failure.get("post_title"),
        )
        persisted_tasks.append(
            _inherit_next_action_task(
            {
                "kind": "resolve-failure",
                "priority": "medium",
                "post_id": failure.get("post_id"),
                "post_title": failure.get("post_title"),
                "error": failure.get("error"),
                "error_type": failure.get("error_type"),
                "attempts": failure.get("attempts"),
                "label": _task_label(failure),
            },
            previous,
        )
        )

    summary_actions: list[dict[str, Any]] = []
    if primary_publication_required and not primary_publication_succeeded:
        summary_actions.append(
            {
                "kind": "publish-primary",
                "label": "优先补发上一轮未完成的主发布",
            }
        )
    if remaining_comment_tasks:
        summary_actions.append(
            {
                "kind": "reply-comment",
                "count": len(remaining_comment_tasks),
                "label": _active_reply_label(remaining_comment_tasks),
            }
        )
    if unresolved_failures:
        summary_actions.append(
            {
                "kind": "resolve-failure",
                "count": len(unresolved_failures),
                "label": f"处理 {len(unresolved_failures)} 个未解决失败项",
            }
        )
    if not summary_actions:
        summary_actions.append(
            {
                "kind": "steady-state",
                "label": _steady_state_pressure_label(),
            }
        )
    summary_actions.sort(
        key=lambda item: (
            -_summary_action_pressure(item),
            str(item.get("kind") or ""),
        )
    )
    return persisted_tasks, summary_actions[:3]


def _format_delta(value: int | None) -> str:
    if value is None:
        return "?"
    sign = "+" if value > 0 else ""
    return f"{sign}{value}"


def _format_account_line(account_snapshot: dict[str, Any]) -> str:
    finished = account_snapshot.get("finished", {})
    delta = account_snapshot.get("delta", {})
    score = finished.get("score")
    followers = finished.get("follower_count")
    likes = finished.get("like_count")
    return (
        "账号状态："
        f"积分 {score if score is not None else '未知'} ({_format_delta(delta.get('score'))})，"
        f"粉丝 {followers if followers is not None else '未知'} ({_format_delta(delta.get('follower_count'))})，"
        f"点赞 {likes if likes is not None else '未知'} ({_format_delta(delta.get('like_count'))})"
    )


def _truncate_failure_details(failure_details: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return failure_details[: max(0, limit)]


def _compose_core_progress_line(summary: dict[str, Any]) -> str:
    actions = list(summary.get("actions") or [])
    primary = next((item for item in actions if item.get("kind") in PRIMARY_ACTION_KINDS), None)
    primary_mode = str(summary.get("primary_publication_mode") or "none").strip()
    primary_title = str(summary.get("primary_publication_title") or (primary.get("title") if primary else "") or "").strip()
    runtime_stage_strategy = summary.get("runtime_stage_strategy") or {}
    lead_stage = str(runtime_stage_strategy.get("lead") or "").strip()
    comment_backlog = summary.get("comment_backlog") or {}
    active_post_count = int(comment_backlog.get("active_post_count") or 0)
    reply_count = int(comment_backlog.get("replied_count") or 0)
    external_engagement_count = int(summary.get("external_engagement_count") or 0)
    dm_reply_count = sum(1 for item in actions if item.get("kind") == "reply-dm")

    if primary_mode == "pending-confirmation" and primary_title:
        return f"发布待确认《{primary_title}》"
    if primary:
        if primary["kind"] == "publish-chapter":
            return f"文学社新章节《{primary.get('title', '')}》"
        if primary["kind"] == "create-group-post":
            return f"小组帖《{primary.get('title', '')}》"
        return f"主帖《{primary.get('title', '')}》"
    if lead_stage == "reply-comments" and (reply_count or active_post_count):
        if active_post_count > 0:
            return f"评论维护，覆盖 {active_post_count} 个活跃讨论帖，已回复 {reply_count} 条"
        return f"评论维护，已回复 {reply_count} 条"
    if lead_stage == "engage-external" and external_engagement_count > 0:
        return f"外部讨论切入，新增 {external_engagement_count} 条外部评论"
    if lead_stage == "reply-dms" and dm_reply_count > 0:
        return f"私信回复，已处理 {dm_reply_count} 个线程"
    return "本轮没有新增公开写入，但已按当前最强压力点推进"


def _compose_primary_status_line(summary: dict[str, Any]) -> str:
    actions = list(summary.get("actions") or [])
    primary = next((item for item in actions if item.get("kind") in PRIMARY_ACTION_KINDS), None)
    primary_mode = str(summary.get("primary_publication_mode") or "none").strip()
    primary_title = str(summary.get("primary_publication_title") or (primary.get("title") if primary else "") or "").strip()
    if primary_mode == "pending-confirmation" and primary_title:
        return f"主发布：发布待确认《{primary_title}》"
    if primary:
        return f"主发布：已完成《{primary.get('title', '')}》"
    primary_failures = {
        "primary-publish-failed",
        "primary-publish-deduped",
    }
    if any(str(item.get("kind") or "") in primary_failures for item in list(summary.get("failure_details") or [])):
        return "主发布：未完成主发布"
    if any("主发布" in str((item or {}).get("label") or "") for item in list(summary.get("next_actions") or [])):
        return "主发布：未完成主发布"
    return ""


def _failure_error_text(item: dict[str, Any]) -> str:
    error = item.get("error")
    if isinstance(error, dict):
        return str(error.get("error") or error.get("message") or json.dumps(error, ensure_ascii=False))
    return str(error)


def _is_normal_forum_budget_defer(item: dict[str, Any]) -> bool:
    if item.get("resolution") != "deferred":
        return False
    return "forum write budget exhausted" in _failure_error_text(item).lower()


def _is_normal_mechanism_item(item: dict[str, Any]) -> bool:
    if item.get("normal_mechanism"):
        return True
    return _is_normal_forum_budget_defer(item)


def _format_failure_line(item: dict[str, Any]) -> str:
    post_title = item.get("post_title")
    target = f"《{post_title}》" if post_title else item.get("post_id") or "未知目标"
    error_text = _failure_error_text(item)
    resolution = item.get("resolution")
    if resolution == "deferred":
        prefix = "延后处理"
    elif resolution == "deduped":
        prefix = "命中去重"
    elif resolution == "unresolved":
        prefix = "仍未解决"
    else:
        prefix = "失败"
    return f"- {prefix}：{target}，{truncate_text(error_text, 90)}"


def _external_observation_items(external_information: dict[str, Any], *, limit: int = 6) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    seen_titles: set[str] = set()
    for key in (
        "world_signal_snapshot",
        "selected_readings",
        "reading_notes",
        "open_web_results",
        "github_projects",
        "paper_results",
        "classic_readings",
        "manual_web_sources",
        "community_breakouts",
    ):
        for item in list(external_information.get(key) or []):
            title = re.sub(
                r"\s+",
                " ",
                str((item or {}).get("title") or (item or {}).get("summary") or "").strip(),
            )
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            results.append(
                {
                    "title": title,
                    "family": str((item or {}).get("family") or "").strip(),
                }
            )
            if len(results) >= limit:
                return results
    return results


def _report_next_action_label(item: dict[str, Any], summary: dict[str, Any]) -> str:
    kind = str(item.get("kind") or "").strip()
    label = str(item.get("label") or "").strip()
    primary_title = str(summary.get("primary_publication_title") or "").strip()
    focus_kind = str((summary.get("idea_lane_strategy") or {}).get("focus_kind") or "").strip()
    if kind == "publish-primary":
        if primary_title:
            return f"把《{truncate_text(primary_title, 30)}》这条公开主线补完"
        if focus_kind:
            return f"把这轮{_public_kind_display_name(focus_kind)}主动作补完"
        return label or "继续完成这轮公开主动作"
    if kind == "resolve-failure":
        visible_failures = [
            detail
            for detail in list(summary.get("failure_details") or [])
            if not _is_normal_mechanism_item(detail)
        ]
        if visible_failures:
            return f"先收口 {len(visible_failures)} 个失败链，别让恢复链继续挂空"
        return label or "先把未解决失败项收口"
    if kind == "steady-state":
        lead = str((summary.get("runtime_stage_strategy") or {}).get("lead") or "").strip()
        if lead == "engage-external":
            observation_count = len(
                [
                    item
                    for item in list(summary.get("external_observations") or [])
                    if str((item or {}).get("title") or "").strip()
                ]
            )
            if observation_count > 0:
                return f"继续把 {observation_count} 个外部样本压成自己的判断，再切进高热讨论现场"
        if lead == "reply-comments":
            active_post_count = int((summary.get("comment_backlog") or {}).get("active_post_count") or 0)
            if active_post_count > 0:
                return f"继续守住 {active_post_count} 个活跃讨论帖，把高价值评论收口成判断"
        if lead == "reply-dms" and int(summary.get("dm_reply_count") or 0) > 0:
            return "继续收口私信线程，别让高价值对话掉回队列"
        if lead == "publish-primary" and focus_kind:
            return f"继续把这轮{_public_kind_display_name(focus_kind)}主动作往前推"
        return label or _steady_state_pressure_label()
    return label or _steady_state_pressure_label()


def _report_next_action_lines(summary: dict[str, Any], *, limit: int = 3) -> list[str]:
    labels: list[str] = []
    for item in list(summary.get("next_actions") or [])[:limit]:
        label = _report_next_action_label(item, summary)
        if label and label not in labels:
            labels.append(label)
    if labels:
        return labels[:limit]
    fallback = _report_next_action_label(
        {"kind": "steady-state", "label": _steady_state_pressure_label()},
        summary,
    )
    return [fallback] if fallback else []


def _compose_feishu_report(summary: dict[str, Any], failure_detail_limit: int) -> str:
    actions = summary.get("actions", [])
    comment_backlog = summary.get("comment_backlog", {})
    external_engagement_count = int(summary.get("external_engagement_count") or 0)
    visible_failures = [item for item in list(summary.get("failure_details", [])) if not _is_normal_mechanism_item(item)]
    failure_details = _truncate_failure_details(visible_failures, failure_detail_limit)
    next_actions = summary.get("next_actions", [])
    source_mutation = summary.get("source_mutation") or {}
    low_heat_reflection = summary.get("low_heat_reflection") or {}
    idea_lane_strategy = summary.get("idea_lane_strategy") or {}
    runtime_stage_strategy = summary.get("runtime_stage_strategy") or {}
    external_observations = [
        item
        for item in list(summary.get("external_observations") or [])
        if str((item or {}).get("title") or "").strip()
    ]

    active_post_count = int(comment_backlog.get("active_post_count") or 0)
    reply_count = int(comment_backlog.get("replied_count") or 0)
    next_batch_count = int(comment_backlog.get("next_batch_count") or comment_backlog.get("remaining_count") or 0)
    archived_stale_count = int(comment_backlog.get("archived_stale_count") or 0)
    if active_post_count <= 0 and reply_count <= 0 and external_engagement_count <= 0:
        interaction_line = "互动处理：当前没有活跃评论队列，也没有新增外部讨论评论"
    else:
        continuation = (
            f"下一轮保留 {next_batch_count} 条优先评论" if next_batch_count > 0 else "当前没有待续评论"
        )
        interaction_line = (
            "互动处理："
            f"覆盖 {active_post_count} 个活跃讨论帖，"
            f"已回复 {reply_count} 条，"
            f"新增 {external_engagement_count} 条外部讨论评论，"
            f"{continuation}"
        )
        if archived_stale_count > 0:
            interaction_line += f"，已归档冷帖旧评论 {archived_stale_count} 条"

    lines = [
        "派蒙心跳已完成。",
    ]
    stage_rationale = str(runtime_stage_strategy.get("rationale") or "").strip()
    if stage_rationale:
        lines.append(f"起手判断：{stage_rationale}")
    lines.append(f"核心推进：{_compose_core_progress_line(summary)}")
    primary_status_line = _compose_primary_status_line(summary)
    if primary_status_line:
        lines.append(primary_status_line)
    if external_observations:
        title_text = "；".join(
            truncate_text(str(item.get("title") or "").strip(), 48)
            for item in external_observations[:4]
        )
        lines.append(f"外部观察：{title_text}")
    lane_rationale = str(idea_lane_strategy.get("rationale") or "").strip()
    if lane_rationale:
        lines.append(f"当前判断：{lane_rationale}")
    if low_heat_reflection.get("triggered"):
        low_heat_title = str(low_heat_reflection.get("title") or "").strip()
        low_heat_summary = str(low_heat_reflection.get("summary") or "").strip()
        if low_heat_summary:
            if low_heat_title:
                lines.append(f"低热复盘：《{low_heat_title}》：{low_heat_summary}")
            else:
                lines.append(f"低热复盘：{low_heat_summary}")
    mutation_summary = _sanitize_source_mutation_summary(str(source_mutation.get("human_summary") or ""))
    if mutation_summary:
        lines.append(f"源码进化：{mutation_summary}")
    lines.append(interaction_line)
    lines.append(_format_account_line(summary.get("account_snapshot", {})))

    if failure_details:
        lines.append(f"失败明细：{len(visible_failures)} 条")
        lines.extend(_format_failure_line(item) for item in failure_details)
    else:
        lines.append("失败明细：0 条")

    next_action_labels = _report_next_action_lines(summary, limit=1)
    if next_action_labels:
        lines.append(f"下一步动作：{next_action_labels[0]}")
    lines.append(f"完成时间：{summary.get('ran_at') or now_utc()}")
    return "\n".join(lines)


def _feishu_http_timeout_seconds(config) -> float:
    raw = config.automation.get("feishu_http_timeout_ms", 8000)
    try:
        timeout_ms = int(raw)
    except (TypeError, ValueError):
        timeout_ms = 8000
    return max(5.0, timeout_ms / 1000.0)


def _feishu_send_retries(config) -> int:
    raw = config.automation.get("feishu_send_retries", 4)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 4


def _feishu_send_retry_delay_seconds(config) -> float:
    raw = config.automation.get("feishu_send_retry_delay_ms", 1500)
    try:
        delay_ms = float(raw)
    except (TypeError, ValueError):
        delay_ms = 1500.0
    return max(0.0, delay_ms / 1000.0)


def _is_transient_feishu_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            TimeoutError,
            ssl.SSLError,
            ConnectionError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ),
    ):
        return True
    if isinstance(exc, urllib_error.URLError):
        lowered = str(exc.reason or exc).lower()
        return any(
            marker in lowered
            for marker in (
                "timed out",
                "timeout",
                "temporarily unavailable",
                "connection reset",
                "connection refused",
                "network is unreachable",
                "name or service not known",
                "temporary failure in name resolution",
            )
        )
    lowered = str(exc).lower()
    return any(
        marker in lowered
        for marker in ("timed out", "timeout", "connection reset", "network is unreachable")
    )


def _post_feishu_json(
    config,
    url: str,
    payload: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "paimon-instreet-autopilot/0.1",
    }
    if headers:
        request_headers.update(headers)
    encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    timeout_seconds = _feishu_http_timeout_seconds(config)
    retries = _feishu_send_retries(config)
    retry_delay_seconds = _feishu_send_retry_delay_seconds(config)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib_request.Request(
            url,
            method="POST",
            headers=request_headers,
            data=encoded_payload,
        )
        try:
            with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                body = {"raw": truncate_text(raw, 240)}
            last_exc = RuntimeError(f"HTTP {exc.code}: {body}")
            if attempt >= retries:
                raise last_exc from exc
        except (
            urllib_error.URLError,
            TimeoutError,
            ssl.SSLError,
            ConnectionError,
            OSError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ) as exc:
            last_exc = exc
            if attempt >= retries or not _is_transient_feishu_error(exc):
                raise
        else:
            try:
                return json.loads(raw) if raw else {}
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"invalid feishu response: {truncate_text(raw, 240)}") from exc
        if retry_delay_seconds > 0 and attempt < retries:
            time.sleep(retry_delay_seconds * attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("feishu request failed without response")


def _send_feishu_text(
    config,
    text: str,
    *,
    success_kind: str,
    failed_kind: str,
    pending_kind: str,
) -> dict[str, Any]:
    target = _resolve_feishu_report_target(config)
    if target is None:
        return {
            "kind": pending_kind,
            "error": "no bound feishu report target yet; awaiting explicit binding",
        }
    receive_id_type, receive_id = target
    try:
        auth_body = _post_feishu_json(
            config,
            f"{FEISHU_API_BASE}{FEISHU_TENANT_TOKEN_ENDPOINT}",
            {
                "app_id": config.feishu["app_id"],
                "app_secret": config.feishu["app_secret"],
            },
        )
        auth_code = int(auth_body.get("code", -1))
        tenant_access_token = str(auth_body.get("tenant_access_token") or "").strip()
        if auth_code != 0 or not tenant_access_token:
            raise RuntimeError(f"tenant token request failed: {auth_body}")

        message_body = _post_feishu_json(
            config,
            (
                f"{FEISHU_API_BASE}/open-apis/im/v1/messages"
                f"?receive_id_type={urllib_parse.quote(receive_id_type, safe='')}"
            ),
            {
                "receive_id": receive_id,
                "content": json.dumps({"text": text}, ensure_ascii=False),
                "msg_type": "text",
            },
            headers={"Authorization": f"Bearer {tenant_access_token}"},
        )
        message_code = int(message_body.get("code", -1))
        if message_code != 0:
            raise RuntimeError(f"feishu message send failed: {message_body}")
    except Exception as exc:
        return {
            "kind": failed_kind,
            "receive_id_type": receive_id_type,
            "receive_id": receive_id,
            "error": str(exc),
        }
    return {
        "kind": success_kind,
        "receive_id_type": receive_id_type,
        "receive_id": receive_id,
        "result": message_body,
    }


def _send_feishu_report(config, summary: dict[str, Any], failure_detail_limit: int) -> dict:
    text = _compose_feishu_report(summary, failure_detail_limit)
    return _send_feishu_text(
        config,
        text,
        success_kind="feishu-report",
        failed_kind="feishu-report-failed",
        pending_kind="feishu-report-pending-target",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Paimon's main operating loop.")
    parser.add_argument("--execute", action="store_true", help="Perform public write actions.")
    parser.add_argument("--allow-codex", action="store_true", help="Use codex exec to draft replies or posts.")
    parser.add_argument("--archive", action="store_true", help="Archive the snapshot taken during this run.")
    parser.add_argument("--source-mutation-only", action="store_true", help="Run only the post-heartbeat source mutation worker.")
    args = parser.parse_args()

    ensure_runtime_dirs()
    _ensure_autonomy_state_files()
    if args.source_mutation_only:
        result = _run_source_mutation_worker(allow_codex=args.allow_codex)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(0 if not result.get("commit_error") else 1)

    config = load_config()
    client = InStreetClient(config)
    username = config.identity["name"]
    codex_model = config.automation.get("codex_model") or None
    codex_reasoning_effort = config.automation.get("codex_reasoning_effort") or None
    codex_timeout_seconds = _heartbeat_codex_timeout_seconds(config)
    failure_detail_limit = _heartbeat_failure_detail_limit(config)
    start_overview = run_snapshot(
        archive=args.archive,
        post_limit=config.automation["post_limit"],
        feed_limit=config.automation["feed_limit"],
    )
    external_information = _refresh_external_information_state()
    planner_timeout_seconds = int(config.automation.get("planner_codex_timeout_seconds", 120))
    memory_prompt = _load_heartbeat_memory_prompt(config)
    seed_plan = build_plan(
        allow_codex=args.allow_codex,
        model=codex_model,
        reasoning_effort=codex_reasoning_effort,
        timeout_seconds=planner_timeout_seconds,
        retry_feedback=None,
    )
    write_json(CURRENT_STATE_DIR / "content_plan.json", seed_plan)
    carryover_state = _load_next_actions_state(config)
    carryover_tasks = carryover_state.get("tasks", [])

    posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
    last_run_state = read_json(CURRENT_STATE_DIR / "heartbeat_last_run.json", default={})
    literary_details = read_json(CURRENT_STATE_DIR / "literary_details.json", default={}).get("details", {})
    literary = read_json(CURRENT_STATE_DIR / "literary.json", default={})
    serial_registry = sync_serial_registry(literary, {"details": literary_details})
    groups = read_json(CURRENT_STATE_DIR / "groups.json", default={}).get("data", {}).get("groups", [])
    forum_write_state = _load_forum_write_budget_state()
    forum_write_budget = _forum_write_budget_status(config, forum_write_state)
    comment_daily_budget = _comment_daily_budget_status(config, forum_write_state)
    content_evolution_state = build_content_evolution_state(
        posts=posts,
        plan=seed_plan,
        previous_state=read_json(CONTENT_EVOLUTION_STATE_PATH, default={}),
    )
    write_json(CONTENT_EVOLUTION_STATE_PATH, content_evolution_state)
    low_heat_signal = _detect_recent_low_heat_post(
        posts=posts,
        last_run=last_run_state,
        config=config,
    )
    low_heat_reflection = _build_low_heat_reflection(
        low_heat_signal=low_heat_signal,
        allow_codex=args.allow_codex,
        model=codex_model,
        reasoning_effort=codex_reasoning_effort,
        timeout_seconds=planner_timeout_seconds,
    )
    low_heat_failures_state = _update_low_heat_failures_state(
        previous_state=read_json(LOW_HEAT_FAILURES_PATH, default={}),
        low_heat_signal=low_heat_signal,
        low_heat_reflection=low_heat_reflection,
    )
    write_json(LOW_HEAT_FAILURES_PATH, low_heat_failures_state)
    write_json(LOW_HEAT_REFLECTION_PATH, low_heat_reflection)
    source_mutation_state = _default_source_mutation_state(
        allow_codex=args.allow_codex,
        low_heat_reflection=low_heat_reflection,
    )
    plan = seed_plan

    actions: list[dict] = []
    failure_details: list[dict] = []
    normal_deferrals: list[dict] = []
    primary_action = None
    primary_publication_mode = "none"
    comment_result = {
        "actions": [],
        "backlog": {
            "detected_count": 0,
            "replied_count": 0,
            "failed_count": 0,
            "remaining_count": 0,
            "deferred_count": 0,
            "scanned_post_count": 0,
            "scan_limit": _reply_post_scan_limit(config),
            "reply_goal": int(config.automation.get("reply_batch_size", 2)),
            "reply_cap": _reply_max_per_run(config),
            "processing_time_budget_sec": _reply_processing_time_budget_sec(config),
            "processed_post_count": 0,
            "resolved_with_retry_count": 0,
            "active_post_count": 0,
            "priority_post_count": 0,
            "archived_stale_count": 0,
            "trimmed_comment_count": 0,
            "next_batch_count": 0,
        },
        "remaining_tasks": [],
        "failure_details": [],
        "normal_deferrals": [],
    }
    external_result = {
        "actions": [],
        "failure_details": [],
        "normal_deferrals": [],
        "engaged_count": 0,
        "remaining_targets": [],
    }
    notification_cleanup = {"actions": [], "failure_details": []}
    planner_retry_feedback: list[str] = _dedupe_feedback(
        list(low_heat_reflection.get("lessons") or []) + list(low_heat_reflection.get("system_fixes") or [])
    )[:8]
    planner_retry_count = 0
    primary_publication_required = bool(args.execute and config.automation.get("heartbeat_require_primary_publication", True))
    runtime_stage_strategy = (
        _runtime_stage_strategy(
            plan,
            carryover_tasks,
            primary_publication_required=primary_publication_required,
        )
        if args.execute
        else {}
    )

    if args.execute:
        cycle_state = _load_primary_cycle_state()
        stage_order = list(runtime_stage_strategy.get("order") or [])
        if not stage_order:
            stage_order = sorted({"publish-primary", "reply-comments", "engage-external", "reply-dms"})
        for stage_name in stage_order:
            if stage_name == "publish-primary":
                for attempt_index in range(_primary_plan_retry_rounds(config)):
                    if attempt_index > 0:
                        planner_retry_count = attempt_index
                        plan = build_plan(
                            allow_codex=args.allow_codex,
                            model=codex_model,
                            reasoning_effort=codex_reasoning_effort,
                            timeout_seconds=planner_timeout_seconds,
                            retry_feedback=planner_retry_feedback,
                        )
                        write_json(CURRENT_STATE_DIR / "content_plan.json", plan)
                        posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
                        literary_details = read_json(CURRENT_STATE_DIR / "literary_details.json", default={}).get("details", {})
                        literary = read_json(CURRENT_STATE_DIR / "literary.json", default={})
                        serial_registry = sync_serial_registry(literary, {"details": literary_details})
                        groups = read_json(CURRENT_STATE_DIR / "groups.json", default={}).get("data", {}).get("groups", [])
                    primary_action, primary_events, _, primary_publication_mode = _publish_primary_action(
                        config,
                        client,
                        plan,
                        posts,
                        literary_details,
                        serial_registry,
                        groups,
                        cycle_state,
                        allow_codex=args.allow_codex,
                        model=codex_model,
                        reasoning_effort=codex_reasoning_effort,
                        codex_timeout_seconds=codex_timeout_seconds,
                        forum_write_state=forum_write_state,
                    )
                    actions.extend(primary_events)
                    failure_details.extend(
                        {
                            "kind": item.get("kind"),
                            "publish_kind": item.get("publish_kind"),
                            "post_title": item.get("title"),
                            "post_id": item.get("post_id"),
                            "error": item.get("error"),
                            "error_type": item.get("error_type"),
                            "attempts": item.get("attempts"),
                            "resolution": item.get("resolution", "unresolved"),
                        }
                        for item in primary_events
                        if item.get("kind") in {"primary-publish-failed", "primary-publish-deduped"}
                    )
                    if primary_action:
                        actions.append(primary_action)
                    normal_deferrals.extend(
                        item
                        for item in primary_events
                        if item.get("normal_mechanism")
                    )
                    if _primary_publish_attempt_satisfied(primary_action, primary_publication_mode):
                        break
                    planner_retry_feedback = _dedupe_feedback(
                        planner_retry_feedback
                        + _planner_retry_feedback_from_plan(plan)
                        + [
                            str(item.get("error") or item.get("kind") or "").strip()
                            for item in primary_events
                            if item.get("kind") in {"primary-publish-failed", "primary-publish-deduped"}
                        ]
                    )[:10]
                continue

            if stage_name == "reply-comments":
                comment_result = _reply_comments(
                    config,
                    client,
                    plan,
                    posts,
                    username,
                    carryover_tasks,
                    allow_codex=args.allow_codex,
                    model=codex_model,
                    reasoning_effort=codex_reasoning_effort,
                    min_batch_size=int(config.automation.get("reply_batch_size", 2)),
                    max_batch_size=_reply_max_per_run(config),
                    processing_time_budget_sec=_reply_processing_time_budget_sec(config),
                    codex_timeout_seconds=codex_timeout_seconds,
                    forum_write_state=forum_write_state,
                )
                actions.extend(comment_result["actions"])
                failure_details.extend(comment_result["failure_details"])
                normal_deferrals.extend(comment_result.get("normal_deferrals", []))
                continue

            if stage_name == "engage-external":
                external_result = _engage_external_discussions(
                    config,
                    client,
                    plan,
                    username,
                    allow_codex=args.allow_codex,
                    model=codex_model,
                    reasoning_effort=codex_reasoning_effort,
                    codex_timeout_seconds=codex_timeout_seconds,
                    forum_write_state=forum_write_state,
                )
                actions.extend(external_result["actions"])
                failure_details.extend(external_result["failure_details"])
                normal_deferrals.extend(external_result.get("normal_deferrals", []))
                continue

            if stage_name == "reply-dms":
                dm_actions = _reply_dms(
                    client,
                    plan,
                    allow_codex=args.allow_codex,
                    model=codex_model,
                    reasoning_effort=codex_reasoning_effort,
                    batch_size=int(config.automation.get("dm_batch_size", 2)),
                    codex_timeout_seconds=codex_timeout_seconds,
                )
                actions.extend(dm_actions)
                failure_details.extend(
                    {
                        "kind": item.get("kind"),
                        "thread_id": item.get("thread_id"),
                        "error": item.get("error"),
                        "resolution": "unresolved",
                    }
                    for item in dm_actions
                    if item.get("kind") == "reply-dm-failed"
                )

        notification_cleanup = _cleanup_notifications(config, client)
        actions.extend(notification_cleanup["actions"])
        failure_details.extend(notification_cleanup["failure_details"])
        forum_write_budget = _forum_write_budget_status(config, forum_write_state)
        comment_daily_budget = _comment_daily_budget_status(config, forum_write_state)

    if args.execute:
        end_overview = run_snapshot(
            archive=False,
            post_limit=config.automation["post_limit"],
            feed_limit=config.automation["feed_limit"],
        )
    else:
        end_overview = _load_current_account_overview()
    account_snapshot = _build_account_snapshot(
        start_overview,
        end_overview,
        comparison_overview=((last_run_state.get("account_snapshot") or {}).get("finished") or {}),
    )
    source_mutation_state = _schedule_background_source_mutation(
        allow_codex=args.allow_codex,
        low_heat_reflection=low_heat_reflection,
    )

    primary_visibility_confirmed = _confirm_primary_publication(primary_action) if args.execute else None
    if primary_action is not None:
        primary_action["visibility_confirmed"] = primary_visibility_confirmed
    failure_details = _drop_resolved_primary_failures(failure_details, primary_action)
    if primary_action is not None and primary_visibility_confirmed is False:
        primary_publication_mode = "pending-confirmation"
        failure_details.append(
            {
                "kind": "primary-publication-unconfirmed",
                "publish_kind": primary_action.get("publish_kind"),
                "post_id": primary_action.get("result_id"),
                "post_title": primary_action.get("title"),
                "error": "Primary publication returned success but was not visible in refreshed state.",
                "resolution": "unresolved",
            }
        )
    primary_publication_succeeded = bool(primary_action is not None and primary_publication_mode == "new")

    if args.execute:
        persisted_next_tasks, next_actions = _build_next_action_state(
            primary_publication_required,
            primary_publication_succeeded,
            comment_result["remaining_tasks"],
            failure_details,
            carryover_tasks,
        )
        next_action_state = _save_next_actions_state(persisted_next_tasks)
    else:
        persisted_next_tasks = carryover_tasks
        next_action_state = carryover_state
        next_actions = [{"kind": item.get("kind"), "label": _task_label(item)} for item in carryover_tasks[:3]]
        if not next_actions:
            next_actions = [{"kind": "steady-state", "label": "继续追当前最强压力点，不为流程对称感硬补动作"}]
    recommended_next_action = next_actions[0]["label"] if next_actions else "继续追当前最强压力点，不为流程对称感硬补动作"

    feishu_report_required = bool(args.execute and config.automation.get("heartbeat_feishu_report_enabled", True))
    summary = {
        "ran_at": now_utc(),
        "execute": args.execute,
        "allow_codex": args.allow_codex,
        "recommended_next_action": recommended_next_action,
        "primary_publication_required": primary_publication_required,
        "primary_publication_succeeded": primary_publication_succeeded,
        "primary_publication_mode": primary_publication_mode,
        "primary_publication_title": (primary_action or {}).get("title") if primary_action else next(
            (item.get("title") for item in actions if item.get("kind") == "primary-publish-deduped" and item.get("title")),
            None,
        ),
        "primary_publication_visibility_confirmed": primary_visibility_confirmed,
        "feishu_report_required": feishu_report_required,
        "feishu_report_sent": False,
        "feishu_report_pending_target": False,
        "comment_reply_count": sum(1 for item in actions if item.get("kind") == "reply-comment"),
        "external_engagement_count": external_result["engaged_count"],
        "dm_reply_count": sum(1 for item in actions if item.get("kind") == "reply-dm"),
        "account_snapshot": account_snapshot,
        "comment_backlog": comment_result["backlog"],
        "forum_write_budget": forum_write_budget,
        "comment_daily_budget": comment_daily_budget,
        "failure_details": failure_details,
        "normal_deferrals": normal_deferrals,
        "next_actions": next_actions,
        "runtime_stage_strategy": runtime_stage_strategy,
        "idea_lane_strategy": plan.get("idea_lane_strategy") or {},
        "external_observations": _external_observation_items(external_information),
        "continuation_state": {
            "path": str(NEXT_ACTIONS_PATH.relative_to(REPO_ROOT)),
            "updated_at": next_action_state.get("updated_at"),
            "task_count": len(persisted_next_tasks),
            "task_counts": _task_counts(persisted_next_tasks),
        },
        "source_mutation": {
            "replan_count": planner_retry_count,
            "executed": source_mutation_state.get("executed"),
            "human_summary": source_mutation_state.get("human_summary"),
            "commit_sha": source_mutation_state.get("commit_sha"),
            "changed_files": source_mutation_state.get("changed_files", []),
            "deleted_legacy_logic": source_mutation_state.get("deleted_legacy_logic", []),
            "new_capability": source_mutation_state.get("new_capability", []),
            "mutation_rounds": source_mutation_state.get("mutation_rounds"),
            "mode": source_mutation_state.get("mode"),
            "pending": source_mutation_state.get("pending", False),
            "scheduled_pid": source_mutation_state.get("scheduled_pid"),
        },
        "low_heat_reflection": low_heat_reflection,
        "actions": actions,
    }
    summary["recommended_next_action"] = next(
        iter(_report_next_action_lines(summary, limit=1)),
        summary.get("recommended_next_action") or _steady_state_pressure_label(),
    )

    feishu_report_sent = False
    if feishu_report_required:
        report_action = _send_feishu_report(config, summary, failure_detail_limit)
        actions.append(report_action)
        if report_action.get("kind") == "feishu-report":
            feishu_report_sent = True
        elif report_action.get("kind") == "feishu-report-pending-target":
            summary["feishu_report_pending_target"] = True
        else:
            failure_details.append(
                {
                    "kind": report_action.get("kind"),
                    "error": report_action.get("error"),
                    "resolution": "unresolved",
                }
            )
        summary["feishu_report_sent"] = feishu_report_sent
        summary["failure_details"] = failure_details

    try:
        memory_sync = memory_manager_module.record_heartbeat_summary(summary, config=config)
    except Exception as exc:
        memory_sync = {
            "ok": False,
            "error": str(exc),
        }
        failure_details.append(
            {
                "kind": "memory-sync-failed",
                "error": str(exc),
                "resolution": "unresolved",
            }
        )
    summary["memory_sync"] = memory_sync
    summary["failure_details"] = failure_details

    latest_posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
    latest_external_information = _refresh_external_information_state()
    latest_fallback_audit = _fallback_audit_state()
    content_evolution_state = build_content_evolution_state(
        posts=latest_posts,
        plan=plan,
        previous_state=read_json(CONTENT_EVOLUTION_STATE_PATH, default={}),
        source_mutations=[
            {
                "path": path,
                "action": "mutated",
                "reason": source_mutation_state.get("human_summary") or "",
            }
            for path in list(source_mutation_state.get("changed_files") or [])
        ],
    )
    write_json(CONTENT_EVOLUTION_STATE_PATH, content_evolution_state)
    updated_plan = build_plan(
        allow_codex=args.allow_codex,
        model=codex_model,
        reasoning_effort=codex_reasoning_effort,
        timeout_seconds=planner_timeout_seconds,
        retry_feedback=planner_retry_feedback,
    )
    write_json(CURRENT_STATE_DIR / "content_plan.json", updated_plan)
    latest_world_snapshot = [
        truncate_text(str((item or {}).get("title") or "").strip(), 48)
        for item in list(latest_external_information.get("world_signal_snapshot") or [])
        if str((item or {}).get("title") or "").strip()
    ][:4]
    summary["external_information"] = {
        "selected_readings_count": len(latest_external_information.get("selected_readings") or []),
        "focus_titles": latest_world_snapshot,
    }
    counts = list((latest_fallback_audit.get("counts") or {}).values())
    counts.sort(key=lambda item: int(item.get("count") or 0), reverse=True)
    summary["fallback_audit"] = {
        "updated_at": latest_fallback_audit.get("updated_at"),
        "active_keys": len(latest_fallback_audit.get("counts") or {}),
        "top_entries": counts[:6],
        "recent": list(latest_fallback_audit.get("recent") or [])[:6],
    }
    write_json(CURRENT_STATE_DIR / "heartbeat_last_run.json", summary)
    append_jsonl(CURRENT_STATE_DIR / "heartbeat_log.jsonl", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    exit_code = 0
    if primary_publication_required and not primary_publication_succeeded:
        exit_code = 2
    elif (
        feishu_report_required
        and not feishu_report_sent
        and not summary.get("feishu_report_pending_target")
    ):
        exit_code = 3
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
