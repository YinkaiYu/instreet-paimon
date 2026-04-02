#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from common import (
    CURRENT_STATE_DIR,
    LOGS_DIR,
    REPO_ROOT,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    read_json,
    run_codex_json,
    runtime_subprocess_env,
    truncate_text,
    write_json,
)


HEARTBEAT_ONCE_BIN = REPO_ROOT / "bin" / "paimon-heartbeat-once"
REPLAY_OUTBOUND_BIN = REPO_ROOT / "bin" / "paimon-replay-outbound"
HEARTBEAT_LAST_RUN_PATH = CURRENT_STATE_DIR / "heartbeat_last_run.json"
HEARTBEAT_LOG_PATH = CURRENT_STATE_DIR / "heartbeat_log.jsonl"
SUPERVISOR_LAST_RUN_PATH = CURRENT_STATE_DIR / "heartbeat_supervisor_last_run.json"
SUPERVISOR_PID_PATH = CURRENT_STATE_DIR / "heartbeat_supervisor.pid"
SUPERVISOR_LOG_PATH = LOGS_DIR / "heartbeat_supervisor_log.jsonl"
COMMENT_FETCH_FAILURE_BURST_THRESHOLD = 5
COMMENT_FETCH_PERSISTENCE_THRESHOLD = 2
COMMENT_FETCH_HISTORY_WINDOW = 4
COMMENT_FETCH_PERSISTENT_POST_REPAIR_THRESHOLD = 2

PUBLIC_ACTION_KINDS = {"reply-comment", "create-post", "create-group-post", "publish-chapter", "comment-on-feed"}
SUPPORTED_REASONING_EFFORTS = {"low", "medium", "high", "xhigh"}


def _bool_flag(enabled: bool, flag: str) -> list[str]:
    return [flag] if enabled else []


def _normalize_reasoning_effort(model: str | None, effort: Any) -> str | None:
    if effort is None:
        return None
    normalized = str(effort).strip().lower()
    if not normalized:
        return None
    if normalized not in SUPPORTED_REASONING_EFFORTS:
        return None
    model_name = (model or "").strip().lower()
    if normalized == "xhigh" and ("codex-mini" in model_name or model_name.endswith("-mini")):
        return "high"
    return normalized


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _acquire_lock() -> int | None:
    if SUPERVISOR_PID_PATH.exists():
        try:
            existing_pid = int(SUPERVISOR_PID_PATH.read_text(encoding="utf-8").strip())
        except ValueError:
            existing_pid = 0
        if existing_pid and _pid_alive(existing_pid):
            return existing_pid
        SUPERVISOR_PID_PATH.unlink(missing_ok=True)
    pid = os.getpid()
    SUPERVISOR_PID_PATH.write_text(f"{pid}\n", encoding="utf-8")
    return None


def _release_lock() -> None:
    try:
        recorded = int(SUPERVISOR_PID_PATH.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return
    if recorded == os.getpid():
        SUPERVISOR_PID_PATH.unlink(missing_ok=True)


def _reconcile_stale_run_record() -> dict[str, Any] | None:
    previous = read_json(SUPERVISOR_LAST_RUN_PATH, default={})
    if not isinstance(previous, dict):
        return None
    if str(previous.get("status") or "").strip() != "running":
        return None
    try:
        recorded_pid = int(previous.get("pid") or 0)
    except (TypeError, ValueError):
        recorded_pid = 0
    if recorded_pid and _pid_alive(recorded_pid):
        return previous
    stale_reason = (
        f"supervisor pid {recorded_pid} no longer exists"
        if recorded_pid
        else "running record had no live supervisor pid"
    )
    reconciled = {
        **previous,
        "status": "interrupted",
        "completed_at": now_utc(),
        "stale_reason": stale_reason,
    }
    write_json(SUPERVISOR_LAST_RUN_PATH, reconciled)
    append_jsonl(
        SUPERVISOR_LOG_PATH,
        {
            "timestamp": now_utc(),
            "kind": "stale-supervisor-record",
            "started_at": previous.get("started_at"),
            "pid": recorded_pid or None,
            "reason": stale_reason,
        },
    )
    return reconciled


def _heartbeat_command(args: argparse.Namespace) -> list[str]:
    return [
        str(HEARTBEAT_ONCE_BIN),
        *_bool_flag(args.execute, "--execute"),
        *_bool_flag(args.allow_codex, "--allow-codex"),
        *_bool_flag(args.archive, "--archive"),
    ]


def _replay_command(settings: dict[str, Any]) -> list[str]:
    return [
        str(REPLAY_OUTBOUND_BIN),
        "--limit",
        str(settings["replay_pending_limit"]),
    ]


def _load_heartbeat_summary() -> tuple[dict[str, Any] | None, float | None]:
    if not HEARTBEAT_LAST_RUN_PATH.exists():
        return None, None
    try:
        return read_json(HEARTBEAT_LAST_RUN_PATH), HEARTBEAT_LAST_RUN_PATH.stat().st_mtime
    except Exception:
        return None, None


def _has_public_action(summary: dict[str, Any] | None) -> bool:
    actions = summary.get("actions", []) if isinstance(summary, dict) else []
    return any(item.get("kind") in PUBLIC_ACTION_KINDS for item in actions)


def _recent_heartbeat_summaries(limit: int = COMMENT_FETCH_HISTORY_WINDOW) -> list[dict[str, Any]]:
    if not HEARTBEAT_LOG_PATH.exists():
        return []
    try:
        lines = HEARTBEAT_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    summaries: list[dict[str, Any]] = []
    for raw in lines[-limit:]:
        if not raw.strip():
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            summaries.append(item)
    return summaries


def _comment_fetch_failure_post_ids(summary: dict[str, Any] | None) -> set[str]:
    if not isinstance(summary, dict):
        return set()
    return {
        str(item.get("post_id"))
        for item in summary.get("failure_details", [])
        if item.get("kind") == "comment-backlog-load-failed" and item.get("post_id")
    }


def _persistent_comment_fetch_failures(summary: dict[str, Any] | None) -> set[str]:
    current = _comment_fetch_failure_post_ids(summary)
    if not current:
        return set()
    history = _recent_heartbeat_summaries()
    counts: dict[str, int] = {}
    for item in history:
        for post_id in _comment_fetch_failure_post_ids(item):
            counts[post_id] = counts.get(post_id, 0) + 1
    return {post_id for post_id in current if counts.get(post_id, 0) >= COMMENT_FETCH_PERSISTENCE_THRESHOLD}


def _evaluate_attempt(
    result: dict[str, Any],
    summary: dict[str, Any] | None,
    summary_mtime: float | None,
    attempt_started_at: float,
    *,
    require_public_action: bool,
    require_primary_publication: bool,
    require_feishu_report: bool,
) -> dict[str, Any]:
    issues: list[str] = []
    if result.get("timed_out"):
        issues.append("heartbeat command timed out")
    if result.get("returncode") not in {0, None}:
        issues.append(f"heartbeat exited with code {result['returncode']}")
    if summary is None or summary_mtime is None or summary_mtime < attempt_started_at - 1:
        issues.append("heartbeat_last_run.json was not refreshed by this attempt")

    has_public_action = _has_public_action(summary)
    if require_public_action and not has_public_action:
        issues.append("no public action recorded in heartbeat summary")
    primary_publication_succeeded = bool(summary.get("primary_publication_succeeded")) if isinstance(summary, dict) else False
    if require_primary_publication and not primary_publication_succeeded:
        issues.append("no primary publication recorded in heartbeat summary")
    feishu_report_sent = bool(summary.get("feishu_report_sent")) if isinstance(summary, dict) else False
    feishu_report_pending_target = bool(summary.get("feishu_report_pending_target")) if isinstance(summary, dict) else False
    if require_feishu_report and not feishu_report_sent and not feishu_report_pending_target:
        issues.append("no feishu progress report recorded in heartbeat summary")
    comment_fetch_failures = _comment_fetch_failure_post_ids(summary)
    persistent_comment_failures = _persistent_comment_fetch_failures(summary)
    if len(persistent_comment_failures) >= COMMENT_FETCH_PERSISTENT_POST_REPAIR_THRESHOLD:
        issues.append(
            f"persistent comment fetch failures detected for {len(persistent_comment_failures)} posts"
        )
    elif len(comment_fetch_failures) >= COMMENT_FETCH_FAILURE_BURST_THRESHOLD:
        issues.append(
            f"comment fetch failures spiked to {len(comment_fetch_failures)} posts in this heartbeat"
        )

    if result.get("timed_out") or result.get("returncode") not in {0, None}:
        status = "repair"
    elif summary is None or summary_mtime is None or summary_mtime < attempt_started_at - 1:
        status = "repair"
    elif require_primary_publication and not primary_publication_succeeded:
        status = "repair"
    elif require_feishu_report and not feishu_report_sent and not feishu_report_pending_target:
        status = "repair"
    elif (
        len(persistent_comment_failures) >= COMMENT_FETCH_PERSISTENT_POST_REPAIR_THRESHOLD
        or len(comment_fetch_failures) >= COMMENT_FETCH_FAILURE_BURST_THRESHOLD
    ):
        status = "repair"
    elif require_public_action and not has_public_action:
        status = "retry"
    else:
        status = "success"

    return {
        "status": status,
        "issues": issues,
        "fresh_summary": summary is not None and summary_mtime is not None and summary_mtime >= attempt_started_at - 1,
        "has_public_action": has_public_action,
        "primary_publication_succeeded": primary_publication_succeeded,
        "feishu_report_sent": feishu_report_sent,
        "feishu_report_pending_target": feishu_report_pending_target,
        "comment_fetch_failure_count": len(comment_fetch_failures),
        "persistent_comment_failure_count": len(persistent_comment_failures),
    }


def _run_heartbeat_attempt(command: list[str], timeout_seconds: int) -> dict[str, Any]:
    started_at = now_utc()
    started_epoch = time.time()
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env={
                **runtime_subprocess_env(),
                "PYTHONUNBUFFERED": "1",
            },
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        result = {
            "started_at": started_at,
            "finished_at": now_utc(),
            "started_epoch": started_epoch,
            "timed_out": False,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except subprocess.TimeoutExpired as exc:
        result = {
            "started_at": started_at,
            "finished_at": now_utc(),
            "started_epoch": started_epoch,
            "timed_out": True,
            "returncode": None,
            "stdout": (exc.stdout or "").strip(),
            "stderr": (exc.stderr or "").strip(),
        }
    return result


def _audit_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "status": {"type": "string", "enum": ["success", "retry", "repair"]},
            "reason": {"type": "string"},
            "next_step": {"type": "string"},
            "notes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["status", "reason", "next_step", "notes"],
    }


def _repair_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "status": {"type": "string", "enum": ["success", "failed"]},
            "summary": {"type": "string"},
            "fixes": {"type": "array", "items": {"type": "string"}},
            "final_command": {"type": "string"},
            "final_run_at": {"type": "string"},
            "attempts": {"type": "integer", "minimum": 0},
        },
        "required": ["status", "summary", "fixes", "final_command", "final_run_at", "attempts"],
    }


def _audit_with_codex(
    attempt_index: int,
    max_attempts: int,
    command: list[str],
    result: dict[str, Any],
    summary: dict[str, Any] | None,
    deterministic: dict[str, Any],
    *,
    timeout_seconds: int,
    model: str | None,
    reasoning_effort: str | None,
) -> dict[str, Any]:
    prompt = f"""
你在监管派蒙的定时 heartbeat。你只能审计，不要修改任何文件。

目标：
1. 判断这次 heartbeat 尝试是否已经成功完成。
2. 如果没有成功，判断更适合直接重试，还是先修复再继续。

判断规则：
- `success`：这次尝试已经完成，状态文件已刷新，且如果要求公开动作，则已记录公开动作。
- `retry`：更像瞬时问题，直接再跑一次更合适。
- `repair`：更像代码、入口、依赖、环境或状态链路问题，应该先修。

输出必须匹配 schema。

尝试序号：{attempt_index}/{max_attempts}
执行命令：{" ".join(command)}
确定性基线判断：
{json.dumps(deterministic, ensure_ascii=False, indent=2)}

进程结果：
{json.dumps({
    "timed_out": result.get("timed_out"),
    "returncode": result.get("returncode"),
    "stdout": truncate_text(result.get("stdout", ""), 3000),
    "stderr": truncate_text(result.get("stderr", ""), 3000),
}, ensure_ascii=False, indent=2)}

heartbeat_last_run.json：
{truncate_text(json.dumps(summary or {}, ensure_ascii=False, indent=2), 5000)}
""".strip()
    return run_codex_json(
        prompt,
        _audit_schema(),
        timeout=timeout_seconds,
        model=model,
        reasoning_effort=reasoning_effort,
        full_auto=True,
    )


def _repair_with_codex(
    command: list[str],
    result: dict[str, Any],
    summary: dict[str, Any] | None,
    audit: dict[str, Any],
    *,
    timeout_seconds: int,
    model: str | None,
    reasoning_effort: str | None,
) -> dict[str, Any]:
    prompt = f"""
你在修复派蒙的 heartbeat cron 链路。你的工作目录是仓库根目录。

目标：
1. 调查为什么下面这次 heartbeat 尝试没有成功。
2. 做最小必要修复。
3. 反复运行 `{" ".join(command)}`，直到它成功退出并刷新 `state/current/heartbeat_last_run.json`。

约束：
- 只在当前仓库内做最小修复，优先修入口、脚本、路径、日志、状态判断问题。
- 不要修改 `config/paimon.json`。
- 不要使用破坏性 git 命令。
- 不要额外发帖或评论；只允许通过 heartbeat 本身产生公开动作。
- 如果确认无法在本次会话内修好，如实返回 failed。

上次失败的进程结果：
{json.dumps({
    "timed_out": result.get("timed_out"),
    "returncode": result.get("returncode"),
    "stdout": truncate_text(result.get("stdout", ""), 3000),
    "stderr": truncate_text(result.get("stderr", ""), 3000),
}, ensure_ascii=False, indent=2)}

上次 heartbeat 摘要：
{truncate_text(json.dumps(summary or {}, ensure_ascii=False, indent=2), 5000)}

审计结论：
{json.dumps(audit, ensure_ascii=False, indent=2)}
""".strip()
    return run_codex_json(
        prompt,
        _repair_schema(),
        timeout=timeout_seconds,
        model=model,
        reasoning_effort=reasoning_effort,
        dangerous=True,
    )


def _supervisor_settings(config: Any) -> dict[str, Any]:
    automation = config.automation
    audit_model = automation.get("heartbeat_supervisor_codex_model") or automation.get("codex_model") or None
    repair_model = automation.get("heartbeat_supervisor_repair_model") or automation.get("codex_model") or None
    configured_attempt_timeout_seconds = max(
        60,
        int(automation.get("heartbeat_supervisor_attempt_timeout_ms", 1500000)) // 1000,
    )
    heartbeat_codex_timeout_seconds = max(
        30,
        int(automation.get("heartbeat_codex_timeout_ms", 180000)) // 1000,
    )
    fiction_chapter_timeout_raw = automation.get("fiction_chapter_codex_timeout_ms")
    if fiction_chapter_timeout_raw is None:
        fiction_chapter_timeout_seconds = max(heartbeat_codex_timeout_seconds, 600)
    else:
        fiction_chapter_timeout_seconds = max(
            heartbeat_codex_timeout_seconds,
            max(30, int(fiction_chapter_timeout_raw) // 1000),
        )
    planner_timeout_seconds = max(30, int(automation.get("planner_codex_timeout_seconds", 120)))
    primary_wait_notify_seconds = max(0, int(automation.get("primary_wait_notify_sec", 1800)))
    minimum_attempt_timeout_seconds = max(
        2700,
        primary_wait_notify_seconds + 900,
        fiction_chapter_timeout_seconds + planner_timeout_seconds + 1200,
    )
    return {
        "max_attempts": int(automation.get("heartbeat_supervisor_max_attempts", 3)),
        "attempt_timeout_seconds": max(
            configured_attempt_timeout_seconds,
            minimum_attempt_timeout_seconds,
        ),
        "audit_timeout_seconds": max(60, int(automation.get("heartbeat_supervisor_codex_timeout_ms", 240000)) // 1000),
        "repair_timeout_seconds": max(60, int(automation.get("heartbeat_supervisor_repair_timeout_ms", 1200000)) // 1000),
        "audit_model": audit_model,
        "audit_reasoning_effort": _normalize_reasoning_effort(
            audit_model,
            automation.get("heartbeat_supervisor_codex_reasoning_effort"),
        ),
        "repair_model": repair_model,
        "repair_reasoning_effort": _normalize_reasoning_effort(
            repair_model,
            automation.get("heartbeat_supervisor_repair_reasoning_effort"),
        ),
        "use_codex_audit": bool(automation.get("heartbeat_supervisor_use_codex_audit", True)),
        "auto_repair": bool(automation.get("heartbeat_supervisor_auto_repair", True)),
        "require_public_action": bool(automation.get("public_output_required", False)),
        "require_primary_publication": bool(automation.get("heartbeat_require_primary_publication", True)),
        "require_feishu_report": bool(automation.get("heartbeat_feishu_report_enabled", True)),
        "replay_pending_enabled": bool(automation.get("pending_outbound_replay_enabled", True)),
        "replay_pending_limit": max(1, int(automation.get("pending_outbound_replay_limit", 3))),
        "replay_pending_timeout_seconds": max(30, int(automation.get("pending_outbound_replay_timeout_ms", 300000)) // 1000),
    }


def _run_pending_replay(settings: dict[str, Any]) -> dict[str, Any]:
    command = _replay_command(settings)
    started_at = now_utc()
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env={
                **runtime_subprocess_env(),
                "PYTHONUNBUFFERED": "1",
            },
            text=True,
            capture_output=True,
            timeout=settings["replay_pending_timeout_seconds"],
            check=False,
        )
        parsed_stdout = None
        if completed.stdout.strip():
            try:
                parsed_stdout = json.loads(completed.stdout)
            except json.JSONDecodeError:
                parsed_stdout = None
        return {
            "started_at": started_at,
            "finished_at": now_utc(),
            "command": command,
            "timed_out": False,
            "returncode": completed.returncode,
            "stdout": truncate_text(completed.stdout.strip(), 3000),
            "stderr": truncate_text(completed.stderr.strip(), 3000),
            "result": parsed_stdout,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "started_at": started_at,
            "finished_at": now_utc(),
            "command": command,
            "timed_out": True,
            "returncode": None,
            "stdout": truncate_text((exc.stdout or "").strip(), 3000),
            "stderr": truncate_text((exc.stderr or "").strip(), 3000),
            "result": None,
        }


def _maybe_run_pending_replay(args: argparse.Namespace, settings: dict[str, Any]) -> dict[str, Any] | None:
    if not args.execute or not settings.get("replay_pending_enabled"):
        return None
    return _run_pending_replay(settings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Supervise and auto-repair Paimon's heartbeat runs.")
    parser.add_argument("--execute", action="store_true", help="Perform public write actions.")
    parser.add_argument("--allow-codex", action="store_true", help="Allow heartbeat content drafting via codex exec.")
    parser.add_argument("--archive", action="store_true", help="Archive the snapshot taken during this run.")
    args = parser.parse_args()

    ensure_runtime_dirs()
    existing_pid = _acquire_lock()
    if existing_pid is not None:
        print(f"Heartbeat supervisor already running with PID {existing_pid}")
        return

    reconciled_record = _reconcile_stale_run_record()
    if reconciled_record is not None and str(reconciled_record.get("status") or "").strip() == "running":
        _release_lock()
        print(f"Heartbeat supervisor already running with PID {reconciled_record.get('pid')}")
        return
    config = load_config()
    settings = _supervisor_settings(config)
    command = _heartbeat_command(args)
    run_record: dict[str, Any] = {
        "started_at": now_utc(),
        "pid": os.getpid(),
        "status": "running",
        "command": command,
        "settings": settings,
        "attempts": [],
    }
    write_json(SUPERVISOR_LAST_RUN_PATH, run_record)

    try:
        for attempt_index in range(1, settings["max_attempts"] + 1):
            result = _run_heartbeat_attempt(command, settings["attempt_timeout_seconds"])
            summary, summary_mtime = _load_heartbeat_summary()
            deterministic = _evaluate_attempt(
                result,
                summary,
                summary_mtime,
                result["started_epoch"],
                require_public_action=args.execute and settings["require_public_action"],
                require_primary_publication=args.execute and settings["require_primary_publication"],
                require_feishu_report=args.execute and settings["require_feishu_report"],
            )

            if settings["use_codex_audit"] and deterministic["fresh_summary"]:
                try:
                    audit = _audit_with_codex(
                        attempt_index,
                        settings["max_attempts"],
                        command,
                        result,
                        summary,
                        deterministic,
                        timeout_seconds=settings["audit_timeout_seconds"],
                        model=settings["audit_model"],
                        reasoning_effort=settings["audit_reasoning_effort"],
                    )
                except Exception as exc:
                    audit = {
                        "status": deterministic["status"],
                        "reason": f"codex audit failed, fallback to deterministic evaluation: {exc}",
                        "next_step": deterministic["status"],
                        "notes": deterministic["issues"],
                    }
            else:
                if settings["use_codex_audit"] and not deterministic["fresh_summary"]:
                    reason = "heartbeat summary was stale or missing; using deterministic evaluation"
                else:
                    reason = "codex audit disabled; using deterministic evaluation"
                audit = {
                    "status": deterministic["status"],
                    "reason": reason,
                    "next_step": deterministic["status"],
                    "notes": deterministic["issues"],
                }

            if deterministic["status"] == "success" and audit.get("status") != "success":
                notes = list(audit.get("notes", []))
                notes.append("deterministic success takes precedence over codex downgrade")
                audit = {
                    **audit,
                    "status": "success",
                    "reason": f"{audit.get('reason', '')} | deterministic success takes precedence".strip(" |"),
                    "next_step": "success",
                    "notes": notes,
                }

            attempt_record: dict[str, Any] = {
                "attempt": attempt_index,
                "result": {
                    "started_at": result["started_at"],
                    "finished_at": result["finished_at"],
                    "timed_out": result["timed_out"],
                    "returncode": result["returncode"],
                    "stdout": truncate_text(result["stdout"], 3000),
                    "stderr": truncate_text(result["stderr"], 3000),
                },
                "deterministic": deterministic,
                "audit": audit,
                "heartbeat_summary": summary,
            }

            if audit.get("status") == "repair" and settings["auto_repair"]:
                try:
                    repair = _repair_with_codex(
                        command,
                        result,
                        summary,
                        audit,
                        timeout_seconds=settings["repair_timeout_seconds"],
                        model=settings["repair_model"],
                        reasoning_effort=settings["repair_reasoning_effort"],
                    )
                except Exception as exc:
                    repair = {
                        "status": "failed",
                        "summary": f"repair codex exec failed: {exc}",
                        "fixes": [],
                        "final_command": " ".join(command),
                        "final_run_at": "",
                        "attempts": 0,
                    }
                attempt_record["repair"] = repair
                repaired_summary, repaired_summary_mtime = _load_heartbeat_summary()
                attempt_record["post_repair_evaluation"] = _evaluate_attempt(
                    {
                        "timed_out": False,
                        "returncode": 0 if repair.get("status") == "success" else None,
                    },
                    repaired_summary,
                    repaired_summary_mtime,
                    result["started_epoch"],
                    require_public_action=args.execute and settings["require_public_action"],
                    require_primary_publication=args.execute and settings["require_primary_publication"],
                    require_feishu_report=args.execute and settings["require_feishu_report"],
                )
                attempt_record["post_repair_summary"] = repaired_summary
                if attempt_record["post_repair_evaluation"]["status"] == "success":
                    run_record["attempts"].append(attempt_record)
                    run_record["status"] = "success"
                    run_record["completed_at"] = now_utc()
                    replay_summary = _maybe_run_pending_replay(args, settings)
                    if replay_summary is not None:
                        run_record["pending_outbound_replay"] = replay_summary
                    write_json(SUPERVISOR_LAST_RUN_PATH, run_record)
                    append_jsonl(SUPERVISOR_LOG_PATH, run_record)
                    print(json.dumps(run_record, ensure_ascii=False, indent=2))
                    return

            run_record["attempts"].append(attempt_record)
            write_json(SUPERVISOR_LAST_RUN_PATH, run_record)
            append_jsonl(
                SUPERVISOR_LOG_PATH,
                {
                    "timestamp": now_utc(),
                    "attempt": attempt_index,
                    "audit_status": audit.get("status"),
                    "returncode": result.get("returncode"),
                    "timed_out": result.get("timed_out"),
                },
            )

            if audit.get("status") == "success":
                run_record["status"] = "success"
                run_record["completed_at"] = now_utc()
                replay_summary = _maybe_run_pending_replay(args, settings)
                if replay_summary is not None:
                    run_record["pending_outbound_replay"] = replay_summary
                write_json(SUPERVISOR_LAST_RUN_PATH, run_record)
                print(json.dumps(run_record, ensure_ascii=False, indent=2))
                return

        run_record["status"] = "failed"
        run_record["completed_at"] = now_utc()
        write_json(SUPERVISOR_LAST_RUN_PATH, run_record)
        append_jsonl(SUPERVISOR_LOG_PATH, run_record)
        print(json.dumps(run_record, ensure_ascii=False, indent=2))
        raise SystemExit(1)
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
