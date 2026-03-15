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
HEARTBEAT_LAST_RUN_PATH = CURRENT_STATE_DIR / "heartbeat_last_run.json"
SUPERVISOR_LAST_RUN_PATH = CURRENT_STATE_DIR / "heartbeat_supervisor_last_run.json"
SUPERVISOR_PID_PATH = CURRENT_STATE_DIR / "heartbeat_supervisor.pid"
SUPERVISOR_LOG_PATH = LOGS_DIR / "heartbeat_supervisor_log.jsonl"

PUBLIC_ACTION_KINDS = {"reply-comment", "create-post", "comment-on-feed"}


def _bool_flag(enabled: bool, flag: str) -> list[str]:
    return [flag] if enabled else []


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


def _heartbeat_command(args: argparse.Namespace) -> list[str]:
    return [
        str(HEARTBEAT_ONCE_BIN),
        *_bool_flag(args.execute, "--execute"),
        *_bool_flag(args.allow_codex, "--allow-codex"),
        *_bool_flag(args.archive, "--archive"),
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
    if require_feishu_report and not feishu_report_sent:
        issues.append("no feishu progress report recorded in heartbeat summary")

    if result.get("timed_out") or result.get("returncode") not in {0, None}:
        status = "repair"
    elif summary is None or summary_mtime is None or summary_mtime < attempt_started_at - 1:
        status = "repair"
    elif require_primary_publication and not primary_publication_succeeded:
        status = "repair"
    elif require_feishu_report and not feishu_report_sent:
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
你在监管 paimon_insight 的定时 heartbeat。你只能审计，不要修改任何文件。

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
你在修复 paimon_insight 的 heartbeat cron 链路。你的工作目录是仓库根目录。

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
    return {
        "max_attempts": int(automation.get("heartbeat_supervisor_max_attempts", 3)),
        "attempt_timeout_seconds": max(60, int(automation.get("heartbeat_supervisor_attempt_timeout_ms", 1500000)) // 1000),
        "audit_timeout_seconds": max(60, int(automation.get("heartbeat_supervisor_codex_timeout_ms", 240000)) // 1000),
        "repair_timeout_seconds": max(60, int(automation.get("heartbeat_supervisor_repair_timeout_ms", 1200000)) // 1000),
        "audit_model": automation.get("heartbeat_supervisor_codex_model") or automation.get("codex_model") or None,
        "audit_reasoning_effort": automation.get("heartbeat_supervisor_codex_reasoning_effort") or None,
        "repair_model": automation.get("heartbeat_supervisor_repair_model") or automation.get("codex_model") or None,
        "repair_reasoning_effort": automation.get("heartbeat_supervisor_repair_reasoning_effort") or None,
        "use_codex_audit": bool(automation.get("heartbeat_supervisor_use_codex_audit", True)),
        "auto_repair": bool(automation.get("heartbeat_supervisor_auto_repair", True)),
        "require_public_action": bool(automation.get("public_output_required", False)),
        "require_primary_publication": bool(automation.get("heartbeat_require_primary_publication", True)),
        "require_feishu_report": bool(automation.get("heartbeat_feishu_report_enabled", True)),
    }


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

    config = load_config()
    settings = _supervisor_settings(config)
    command = _heartbeat_command(args)
    run_record: dict[str, Any] = {
        "started_at": now_utc(),
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

            if settings["use_codex_audit"]:
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
                audit = {
                    "status": deterministic["status"],
                    "reason": "codex audit disabled; using deterministic evaluation",
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
