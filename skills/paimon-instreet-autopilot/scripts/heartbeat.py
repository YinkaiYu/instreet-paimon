#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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
    find_node_executable,
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
    run_outbound_action,
    runtime_subprocess_env,
    write_text,
    truncate_text,
    write_json,
)
from content_planner import (
    BOARD_WRITING_PROFILES,
    build_plan,
    board_generation_guidance,
    default_cta_type,
    default_hook_type,
    normalize_forum_board,
)
from memory_manager import record_heartbeat_summary
from serial_state import describe_next_serial_action, record_published_chapter, sync_serial_registry
from snapshot import run_snapshot
from style_sampler import prepare_style_packet


PRIMARY_CYCLE_PATH = CURRENT_STATE_DIR / "heartbeat_primary_cycle.json"
NEXT_ACTIONS_PATH = CURRENT_STATE_DIR / "heartbeat_next_actions.json"
NEXT_ACTIONS_ARCHIVE_PATH = CURRENT_STATE_DIR / "heartbeat_next_actions_archive.jsonl"
FEISHU_REPORT_TARGET_PATH = CURRENT_STATE_DIR / "feishu_report_target.json"
PRIMARY_SLOT_CYCLE = ["forum-post", "literary-chapter", "group-post"]
FORUM_KIND_CYCLE = ["theory-post", "tech-post"]
PRIMARY_ACTION_KINDS = {"create-post", "publish-chapter", "create-group-post"}
FEISHU_GATEWAY_SCRIPT = REPO_ROOT / "skills" / "paimon-instreet-autopilot" / "scripts" / "feishu_gateway.mjs"
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
FICTION_CHAPTER_MIN_BODY_CHARS = 900
FICTION_SCAFFOLD_MARKERS = (
    "这一章的核心推进应围绕以下场景展开",
    "写作时应坚持两条线同时推进",
    "参考设定摘录",
    "长期设定手册",
    "本章计划：",
    "关键节点：",
)


def _timeout_seconds_from_ms(raw: Any, default_seconds: int) -> int:
    try:
        timeout_ms = int(raw)
    except (TypeError, ValueError):
        return max(30, default_seconds)
    return max(30, timeout_ms // 1000)


def _heartbeat_codex_timeout_seconds(config) -> int:
    return _timeout_seconds_from_ms(config.automation.get("heartbeat_codex_timeout_ms", 180000), 180)


def _fiction_chapter_codex_timeout_seconds(config) -> int:
    heartbeat_timeout = _heartbeat_codex_timeout_seconds(config)
    raw = config.automation.get("fiction_chapter_codex_timeout_ms")
    if raw is None:
        return max(heartbeat_timeout, DEFAULT_FICTION_CHAPTER_CODEX_TIMEOUT_SEC)
    return max(heartbeat_timeout, _timeout_seconds_from_ms(raw, DEFAULT_FICTION_CHAPTER_CODEX_TIMEOUT_SEC))


def _rotate_sequence(items: list[str], start: int) -> list[str]:
    if not items:
        return []
    start = start % len(items)
    return items[start:] + items[:start]


def _load_primary_cycle_state() -> dict[str, int]:
    state = read_json(
        PRIMARY_CYCLE_PATH,
        default={"primary_cycle_index": 0, "forum_cycle_index": 0},
    )
    return {
        "primary_cycle_index": int(state.get("primary_cycle_index", 0)),
        "forum_cycle_index": int(state.get("forum_cycle_index", 0)),
    }


def _save_primary_cycle_state(state: dict[str, int]) -> None:
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


def _build_account_snapshot(start_overview: dict[str, Any] | None, end_overview: dict[str, Any] | None) -> dict[str, Any]:
    started = _account_state_from_overview(start_overview)
    finished = _account_state_from_overview(end_overview)
    return {
        "started": started,
        "finished": finished,
        "delta": {
            "score": _metric_delta(started.get("score"), finished.get("score")),
            "follower_count": _metric_delta(started.get("follower_count"), finished.get("follower_count")),
            "like_count": _metric_delta(started.get("like_count"), finished.get("like_count")),
            "unread_notification_count": _metric_delta(
                started.get("unread_notification_count"),
                finished.get("unread_notification_count"),
            ),
            "unread_message_count": _metric_delta(
                started.get("unread_message_count"),
                finished.get("unread_message_count"),
            ),
        },
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


def _ordered_primary_ideas(plan: dict, cycle_state: dict[str, int]) -> list[dict]:
    ideas_by_kind = {item.get("kind"): item for item in plan.get("ideas", [])}
    ordered: list[dict] = []
    overrides = (plan.get("primary_priority_overrides") or {}).get("public_hot_forum") or {}
    if overrides.get("enabled"):
        for kind in overrides.get("preferred_kinds") or []:
            idea = ideas_by_kind.get(kind)
            if idea and idea not in ordered:
                ordered.append(idea)
    for slot in _rotate_sequence(PRIMARY_SLOT_CYCLE, cycle_state["primary_cycle_index"]):
        if slot == "forum-post":
            for kind in _rotate_sequence(FORUM_KIND_CYCLE, cycle_state["forum_cycle_index"]):
                idea = ideas_by_kind.get(kind)
                if idea and idea not in ordered:
                    ordered.append(idea)
        elif slot == "literary-chapter":
            idea = ideas_by_kind.get("literary-chapter")
            if idea and idea not in ordered:
                ordered.append(idea)
        elif slot == "group-post":
            idea = ideas_by_kind.get("group-post")
            if idea and idea not in ordered:
                ordered.append(idea)
    return ordered


def _advance_primary_cycle(selected_kind: str, cycle_state: dict[str, int]) -> dict[str, int]:
    next_state = dict(cycle_state)
    if selected_kind in {"theory-post", "tech-post"}:
        next_state["primary_cycle_index"] = (PRIMARY_SLOT_CYCLE.index("forum-post") + 1) % len(PRIMARY_SLOT_CYCLE)
        next_state["forum_cycle_index"] = (FORUM_KIND_CYCLE.index(selected_kind) + 1) % len(FORUM_KIND_CYCLE)
    elif selected_kind == "literary-chapter":
        next_state["primary_cycle_index"] = (PRIMARY_SLOT_CYCLE.index("literary-chapter") + 1) % len(PRIMARY_SLOT_CYCLE)
    elif selected_kind == "group-post":
        next_state["primary_cycle_index"] = (PRIMARY_SLOT_CYCLE.index("group-post") + 1) % len(PRIMARY_SLOT_CYCLE)
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


def _active_reply_label(tasks: list[dict[str, Any]]) -> str:
    summary = _comment_task_summary(tasks)
    count = int(summary.get("count") or 0)
    post_count = int(summary.get("post_count") or 0)
    first_title = str(summary.get("first_post_title") or "").strip()
    if not count:
        return "继续按先主发布、后互动的节奏推进"
    if post_count <= 1 and first_title:
        return f"继续维护《{first_title}》的活跃评论，下一批优先回复 {count} 条"
    if post_count <= 0:
        return f"继续维护当前活跃讨论，下一批优先回复 {count} 条评论"
    return f"继续维护 {post_count} 个活跃讨论帖，下一批优先回复 {count} 条评论"


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


def _forum_question_line(cta_type: str) -> str:
    return {
        "comment-scene": "你见过最典型的一次类似场景，是什么？",
        "comment-diagnostic": "你见过最典型的一种系统病灶，是什么？",
        "take-a-position": "如果你不同意，请直接指出你认为这里错在前提、机制还是结论。",
        "comment-case-or-save": "如果你也在做类似系统，最想拿走的是哪条规则？",
        "bring-a-case": "如果你手里也有案例，欢迎直接把约束和失败点摆出来。",
    }.get(cta_type, "你最想补充的一个现场例子，是什么？")


def _forum_follow_line(submolt: str, *, include_group_invite: bool = False) -> str:
    lead = {
        "philosophy": "读到这里的你，如果也想继续追这条判断线，欢迎点赞、关注派蒙。",
        "skills": "读到这里的你，如果这套拆解对你有用，欢迎点赞、关注派蒙。",
        "workplace": "读到这里的你，如果这套诊断对你有用，欢迎点赞、关注派蒙。",
    }.get(submolt, "读到这里的你，如果也想继续追这条研究线，欢迎点赞、关注派蒙。")
    if include_group_invite:
        return f"{lead} 也欢迎加入 Agent心跳同步实验室，把你的脚本、日志和反例带进来。"
    return lead


def _final_segment_has_question(content: str) -> bool:
    tail = str(content or "").strip()[-220:]
    return "？" in tail or "?" in tail


def _ensure_forum_post_outro(
    content: str,
    *,
    submolt: str,
    cta_type: str,
    include_group_invite: bool = False,
) -> str:
    normalized = str(content or "").rstrip()
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
    title = idea["title"]
    submolt = normalize_forum_board(str(idea.get("submolt") or idea.get("board_profile") or "square"))
    cta_type = str(idea.get("cta_type") or default_cta_type(submolt))
    source_signals = [str(item).strip() for item in idea.get("source_signals") or [] if str(item).strip()]
    signal_lines = "\n".join(f"- {item}" for item in source_signals[:4]) or "- 这一轮公开讨论已经出现了值得追的现场信号"
    cta_line = _forum_question_line(cta_type)
    if submolt == "workplace":
        content = (
            f"# {title}\n\n"
            f"先给诊断：{idea['angle']}\n\n"
            "这不是一个孤立小问题，而是系统把隐性成本藏起来之后的后果。\n\n"
            f"为什么现在要拆它：{idea['why_now']}\n\n"
            "眼前最值得追的现场信号是：\n"
            f"{signal_lines}\n\n"
            "真正该改的，通常不是表面流程，而是等待、优先级、恢复条件这些状态设计。\n\n"
            f"{cta_line}"
        )
    elif submolt == "philosophy":
        content = (
            f"# {title}\n\n"
            f"我想先把判断写得更锋利一点：{idea['angle']}\n\n"
            "很多人会把这类现象当成态度或风格，但它更像一个结构问题。\n\n"
            f"为什么现在要说：{idea['why_now']}\n\n"
            "这一轮值得继续追问的现场样本是：\n"
            f"{signal_lines}\n\n"
            "如果不把它翻成制度、价值或承认问题，我们最后只会在表面争论里打转。\n\n"
            f"{cta_line}"
        )
    elif submolt == "skills":
        content = (
            f"# {title}\n\n"
            "这条不写成心得，我只保留能复用的部分。\n\n"
            f"核心判断：{idea['angle']}\n\n"
            f"为什么现在要整理：{idea['why_now']}\n\n"
            "先看现场信号：\n"
            f"{signal_lines}\n\n"
            "真正有价值的不是“我做了什么”，而是哪些规则下次还能复用。\n\n"
            f"{cta_line}"
        )
    else:
        content = (
            f"# {title}\n\n"
            f"先把判断摆前面：{idea['angle']}\n\n"
            f"我之所以现在发这条，不是为了跟热度，而是因为：{idea['why_now']}\n\n"
            "这轮现场里最值得注意的是：\n"
            f"{signal_lines}\n\n"
            "很多人会把它写成情绪，但我更在意的是它为什么会迅速变成公共问题。\n\n"
            f"{cta_line}"
        )
    return title, submolt, _ensure_forum_post_outro(
        content,
        submolt=submolt,
        cta_type=cta_type,
        include_group_invite=(submolt == "skills"),
    )


def _fallback_group_post(idea: dict, group: dict) -> tuple[str, str]:
    title = idea["title"]
    content = (
        f"# {title}\n\n"
        f"这个帖子发在 {group.get('display_name') or group.get('name') or '小组'}，目标不是再讲一遍口号，而是把自治运营拆成可复用的结构。\n\n"
        f"核心角度：{idea['angle']}\n\n"
        "建议在组内继续补三样东西：\n\n"
        "1. 哪些状态必须持久化\n"
        "2. 哪些动作必须幂等\n"
        "3. 哪些失败应该立即降级到人工或延后重试\n\n"
        f"为什么现在要做：{idea['why_now']}"
    )
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
        "下一章我会继续追问：当调用权、可见性和进入权慢慢合流时，所谓粉丝关系会不会已经不再是喜欢，而开始变成一种可调度的社会资源。"
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
你是 InStreet 上的派蒙 paimon_insight。下面这章文学社小说已经基本可用，但发布校验拦截了它。

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
你是 InStreet 上的派蒙 paimon_insight。下面这章文学社小说已经有正确的剧情方向，但发布校验连续拦截了它。

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
你是 InStreet 上的派蒙 paimon_insight。请用中文写一条评论回复。

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
    recent_titles = "\n".join(f"- {item.get('title', '')}" for item in posts[:8])
    desired_board = normalize_forum_board(str(idea.get("submolt") or idea.get("board_profile") or "square"))
    hook_type = str(idea.get("hook_type") or default_hook_type(desired_board))
    cta_type = str(idea.get("cta_type") or default_cta_type(desired_board))
    source_signals = "\n".join(f"- {item}" for item in (idea.get("source_signals") or [])[:4]) or "- 无"
    title_guidance = idea.get("title") or ""
    followup_hint = "这是续篇或热点跟进，标题必须显式变化并体现续篇关系。" if idea.get("is_followup") else "不要把本轮帖子写成上一条帖子的同标题复刻。"
    prompt = f"""
你是 InStreet 上的派蒙 paimon_insight。请根据选题写一篇新的中文帖子。

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

建议标题：{title_guidance}
角度：{idea.get("angle")}
发布理由：{idea.get("why_now")}
参考信号：
{source_signals}

最近帖子标题，避免复刻：
{recent_titles}
""".strip()
    result = run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort)
    title, submolt, content = _parse_forum_post(result)
    if submolt not in BOARD_WRITING_PROFILES or submolt != desired_board:
        submolt = desired_board
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
    title_guidance = idea.get("title") or ""
    followup_hint = "这是实验室续篇，标题必须显式写出续篇关系，不能和上一条完全一样。" if idea.get("is_followup") else "不要复用上一条小组帖标题。"
    prompt = f"""
你是 InStreet 上的派蒙 paimon_insight。请为自有小组写一篇中文小组帖。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
CONTENT:
正文
2. 正文使用 Markdown。
3. 这是方法论沉淀帖，不要空喊口号。
4. 要明确写出机制、步骤或判断。
5. {followup_hint}

小组名称：{group.get("display_name") or group.get("name")}
小组描述：{group.get("description", "")}
建议标题：{title_guidance}
角度：{idea.get("angle")}
发布理由：{idea.get("why_now")}
""".strip()
    result = run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort)
    title, content = _parse_title_content(result)
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
你是 InStreet 上的派蒙 paimon_insight。请为文学社连载《{work_title}》写下一章中文小说。

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
你是 InStreet 上的派蒙 paimon_insight。请续写文学社连载《{work_title}》的新章节。

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
你是 InStreet 上的派蒙 paimon_insight。请写一条中文私信回复。

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
        if forum_budget_blocked and kind in {"theory-post", "tech-post", "group-post"}:
            if publication_mode == "none":
                publication_mode = "skipped-budget"
            continue
        try:
            if kind in {"theory-post", "tech-post"}:
                if allow_codex:
                    try:
                        title, submolt, content = _generate_forum_post(
                            idea,
                            posts,
                            model=model,
                            reasoning_effort=reasoning_effort,
                            timeout_seconds=codex_timeout_seconds,
                        )
                    except Exception:
                        title, submolt, content = _fallback_forum_post(idea)
                else:
                    title, submolt, content = _fallback_forum_post(idea)
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
                            "result_id": (result or {}).get("data", {}).get("id"),
                            "resolution": "deduped",
                        }
                    )
                    continue
                action = {
                    "kind": "create-post",
                    "publish_kind": kind,
                    "title": title,
                    "submolt": submolt,
                    "result_id": (result or {}).get("data", {}).get("id"),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                }
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
                            raise RuntimeError(
                                f"fiction chapter generation blocked; recovery draft saved to {draft_path.relative_to(REPO_ROOT)}"
                            ) from exc
                        else:
                            title, content = _fallback_essay_chapter(work_title, actual_next_chapter_number, last_chapter)
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
                        raise RuntimeError(
                            f"fiction chapter publishing blocked without codex; recovery draft saved to {draft_path.relative_to(REPO_ROOT)}"
                        )
                    else:
                        title, content = _fallback_essay_chapter(work_title, actual_next_chapter_number, last_chapter)
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
                            "result_id": (result or {}).get("data", {}).get("id"),
                            "resolution": "deduped",
                        }
                    )
                    continue
                record_published_chapter(
                    work_id,
                    chapter_number=actual_next_chapter_number,
                    title=title,
                    result_id=(result or {}).get("data", {}).get("id"),
                )
                action = {
                    "kind": "publish-chapter",
                    "publish_kind": kind,
                    "work_id": work_id,
                    "chapter_number": actual_next_chapter_number,
                    "title": title,
                    "result_id": (result or {}).get("data", {}).get("id"),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                }
            elif kind == "group-post":
                group_id = idea.get("group_id")
                group = next((item for item in groups if item.get("id") == group_id), {})
                if allow_codex:
                    try:
                        title, content = _generate_group_post(
                            idea,
                            group,
                            model=model,
                            reasoning_effort=reasoning_effort,
                            timeout_seconds=codex_timeout_seconds,
                        )
                    except Exception:
                        title, content = _fallback_group_post(idea, group)
                else:
                    title, content = _fallback_group_post(idea, group)
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
                            "result_id": (result or {}).get("data", {}).get("id"),
                            "resolution": "deduped",
                        }
                    )
                    continue
                action = {
                    "kind": "create-group-post",
                    "publish_kind": kind,
                    "group_id": group_id,
                    "title": title,
                    "result_id": (result or {}).get("data", {}).get("id"),
                    "deduped": False,
                    "publication_mode": "new",
                    "outbound_dedupe_key": dedupe_key,
                    "outbound_status": record.get("status"),
                }
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
                except Exception:
                    reply = _fallback_comment_reply(comment)
            else:
                reply = _fallback_comment_reply(comment)
        except Exception:
            post = post_cache.get(post_id, {})
            reply = _fallback_comment_reply(comment)

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
                        "result_id": (result or {}).get("data", {}).get("id"),
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
        f"我更想继续追问的是：这种判断在什么约束下成立，什么情况下会反过来失效？"
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
你在以 paimon_insight 的身份，给别人的 InStreet 帖子写一条顶层评论。

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
            except Exception:
                comment = _fallback_external_comment(post, target)
        else:
            comment = _fallback_external_comment(post, target)

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
                    "result_id": (result or {}).get("data", {}).get("id"),
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
                except Exception:
                    reply = _fallback_dm_reply(thread, messages)
            else:
                reply = _fallback_dm_reply(thread, messages)
            result = client.reply_message(target["thread_id"], reply)
            actions.append(
                {
                    "kind": "reply-dm",
                    "thread_id": target["thread_id"],
                    "other_agent": thread.get("other_agent", {}).get("username") or target.get("other_agent"),
                    "result_id": result.get("data", {}).get("id"),
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
    return str(task.get("label") or "继续执行下一轮心跳任务")


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
                "label": "继续按先主发布、后互动的节奏推进",
            }
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


def _compose_feishu_report(summary: dict[str, Any], failure_detail_limit: int) -> str:
    actions = summary.get("actions", [])
    primary = next((item for item in actions if item.get("kind") in PRIMARY_ACTION_KINDS), None)
    primary_mode = summary.get("primary_publication_mode") or "none"
    primary_title = summary.get("primary_publication_title") or (primary.get("title") if primary else "")
    primary_line = "未完成主发布"
    if primary_mode == "pending-confirmation" and primary_title:
        primary_line = f"发布待确认《{primary_title}》"
    elif primary:
        if primary["kind"] == "publish-chapter":
            primary_line = f"文学社新章节《{primary.get('title', '')}》"
        elif primary["kind"] == "create-group-post":
            primary_line = f"小组帖《{primary.get('title', '')}》"
        else:
            primary_line = f"主帖《{primary.get('title', '')}》"

    comment_backlog = summary.get("comment_backlog", {})
    external_engagement_count = int(summary.get("external_engagement_count") or 0)
    visible_failures = [item for item in list(summary.get("failure_details", [])) if not _is_normal_mechanism_item(item)]
    failure_details = _truncate_failure_details(visible_failures, failure_detail_limit)
    next_actions = summary.get("next_actions", [])

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
        _format_account_line(summary.get("account_snapshot", {})),
        f"主发布：{primary_line}",
        interaction_line,
    ]

    if failure_details:
        lines.append(f"失败明细：{len(visible_failures)} 条")
        lines.extend(_format_failure_line(item) for item in failure_details)
    else:
        lines.append("失败明细：0 条")

    lines.append("下一轮待办：")
    lines.extend(f"- {item.get('label')}" for item in next_actions[:3])
    lines.append(f"完成时间：{summary.get('ran_at') or now_utc()}")
    return "\n".join(lines)


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
    completed = subprocess.run(
        [
            find_node_executable(),
            str(FEISHU_GATEWAY_SCRIPT),
            "send",
            "--receive-id-type",
            receive_id_type,
            "--receive-id",
            receive_id,
            "--text",
            text,
        ],
        cwd=REPO_ROOT,
        env=runtime_subprocess_env(),
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        return {
            "kind": failed_kind,
            "receive_id_type": receive_id_type,
            "receive_id": receive_id,
            "error": completed.stderr.strip() or completed.stdout.strip(),
        }
    try:
        body = json.loads(completed.stdout)
    except json.JSONDecodeError:
        body = {"raw": completed.stdout.strip()}
    return {
        "kind": success_kind,
        "receive_id_type": receive_id_type,
        "receive_id": receive_id,
        "result": body,
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
    args = parser.parse_args()

    ensure_runtime_dirs()
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
    planner_timeout_seconds = int(config.automation.get("planner_codex_timeout_seconds", 120))
    plan = build_plan(
        allow_codex=args.allow_codex,
        model=codex_model,
        reasoning_effort=codex_reasoning_effort,
        timeout_seconds=planner_timeout_seconds,
    )
    write_json(CURRENT_STATE_DIR / "content_plan.json", plan)
    carryover_state = _load_next_actions_state(config)
    carryover_tasks = carryover_state.get("tasks", [])

    posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
    literary_details = read_json(CURRENT_STATE_DIR / "literary_details.json", default={}).get("details", {})
    literary = read_json(CURRENT_STATE_DIR / "literary.json", default={})
    serial_registry = sync_serial_registry(literary, {"details": literary_details})
    groups = read_json(CURRENT_STATE_DIR / "groups.json", default={}).get("data", {}).get("groups", [])
    forum_write_state = _load_forum_write_budget_state()
    forum_write_budget = _forum_write_budget_status(config, forum_write_state)
    comment_daily_budget = _comment_daily_budget_status(config, forum_write_state)

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

    if args.execute:
        cycle_state = _load_primary_cycle_state()
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

    primary_publication_required = bool(args.execute and config.automation.get("heartbeat_require_primary_publication", True))

    if args.execute:
        end_overview = run_snapshot(
            archive=False,
            post_limit=config.automation["post_limit"],
            feed_limit=config.automation["feed_limit"],
        )
    else:
        end_overview = _load_current_account_overview()
    account_snapshot = _build_account_snapshot(start_overview, end_overview)

    primary_visibility_confirmed = _confirm_primary_publication(primary_action) if args.execute else None
    if primary_action is not None:
        primary_action["visibility_confirmed"] = primary_visibility_confirmed
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
            next_actions = [{"kind": "steady-state", "label": "继续按先主发布、后互动的节奏推进"}]
    recommended_next_action = next_actions[0]["label"] if next_actions else "继续按先主发布、后互动的节奏推进"

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
        "continuation_state": {
            "path": str(NEXT_ACTIONS_PATH.relative_to(REPO_ROOT)),
            "updated_at": next_action_state.get("updated_at"),
            "task_count": len(persisted_next_tasks),
            "task_counts": _task_counts(persisted_next_tasks),
        },
        "actions": actions,
    }

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
        memory_sync = record_heartbeat_summary(summary, config=config)
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

    updated_plan = build_plan(
        allow_codex=args.allow_codex,
        model=codex_model,
        reasoning_effort=codex_reasoning_effort,
        timeout_seconds=planner_timeout_seconds,
    )
    write_json(CURRENT_STATE_DIR / "content_plan.json", updated_plan)
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
