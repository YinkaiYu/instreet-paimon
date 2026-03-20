#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from common import REPO_ROOT, read_json


REQUIRED_WRITING_SYSTEM_KEYS = [
    "execution_blueprint",
    "intimacy_scale",
    "intimacy_progression",
    "foreshadow_system",
    "hook_system",
    "continuity_system",
    "sweetness_upgrade_vectors",
]
REQUIRED_STORY_BIBLE_KEYS = [
    "setting_anchor",
    "protagonists",
    "relationship_rules",
    "organizations",
    "terminology_rules",
    "longline_threads",
    "ending_constraints",
    "style_bans",
]
DEFAULT_LOOKAHEAD_REQUIRED_CHAPTER_KEYS = [
    "summary",
    "key_conflict",
    "hook",
    "romance_beat",
    "beats",
    "intimacy_target",
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
VALID_TURN_ROLES = {"ignite", "detonate"}
VALID_HOOK_TYPES = {"rule", "reveal", "threat", "choice", "payoff", "reversal"}


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _listify(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _resolve_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _chapter_number(chapter: dict[str, Any]) -> int:
    return _coerce_int(chapter.get("chapter_number") or chapter.get("number"), 0)


def _progression_for_chapter(chapter_number: int, writing_system: dict[str, Any]) -> dict[str, Any]:
    for item in _listify(writing_system.get("intimacy_progression")):
        start = _coerce_int(item.get("chapter_start"), 0)
        end = _coerce_int(item.get("chapter_end"), 0)
        if start and end and start <= chapter_number <= end:
            return dict(item)
    return {}


def _missing_required_keys(payload: dict[str, Any], keys: list[str]) -> list[str]:
    missing: list[str] = []
    for key in keys:
        value = payload.get(key)
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


def _required_chapter_keys(plan: dict[str, Any]) -> list[str]:
    execution_blueprint = (plan.get("writing_system", {}) or {}).get("execution_blueprint", {}) or {}
    configured = execution_blueprint.get("required_chapter_fields")
    if isinstance(configured, list) and configured:
        return [str(item) for item in configured if str(item).strip()]
    return list(DEFAULT_LOOKAHEAD_REQUIRED_CHAPTER_KEYS)


def _chapters_dir_for_plan(plan_path: Path | None) -> Path | None:
    if plan_path is None:
        return None
    return plan_path.parent / "chapters"


def audit_plan(plan: dict[str, Any], *, lookahead: int, plan_path: Path | None = None) -> dict[str, Any]:
    work = plan.get("work", {}) or {}
    writing_notes = plan.get("writing_notes", {}) or {}
    writing_system = plan.get("writing_system", {}) or {}
    story_bible = plan.get("story_bible", {}) or {}
    issues: list[str] = []
    warnings: list[str] = []
    required_chapter_keys = _required_chapter_keys(plan)
    chapters_dir = _chapters_dir_for_plan(plan_path)

    for label, path_value in (
        ("synopsis", work.get("synopsis_path")),
        ("story bible", work.get("story_bible_path") or story_bible.get("source_path")),
        ("style guide", writing_notes.get("style_guide_path")),
    ):
        target = _resolve_path(path_value)
        if target is None or not target.exists():
            issues.append(f"{label} path missing or unreadable: {path_value}")

    for key in REQUIRED_WRITING_SYSTEM_KEYS:
        if not writing_system.get(key):
            issues.append(f"writing_system missing required key: {key}")

    for label, key in (
        ("foreshadow ledger", "foreshadow_system"),
        ("hook library", "hook_system"),
        ("continuity log", "continuity_system"),
    ):
        payload = writing_system.get(key, {}) or {}
        if key == "foreshadow_system":
            path_key = "ledger_path"
        elif key == "hook_system":
            path_key = "library_path"
        else:
            path_key = "log_path"
        target = _resolve_path(payload.get(path_key))
        if target is None or not target.exists():
            issues.append(f"{label} path missing or unreadable: {payload.get(path_key)}")

    for key in REQUIRED_STORY_BIBLE_KEYS:
        if not story_bible.get(key):
            issues.append(f"story_bible missing required key: {key}")

    chapters = sorted(_listify(plan.get("chapters")), key=_chapter_number)
    chapter_map = {_chapter_number(chapter): chapter for chapter in chapters}
    plan_threads = {
        str(item).strip()
        for chapter in chapters
        for field in ("seed_threads", "payoff_threads")
        for item in _listify(chapter.get(field))
        if str(item).strip()
    }

    for item in _listify(story_bible.get("longline_threads")):
        label = str(item.get("label") or "").strip()
        aliases = [str(alias).strip() for alias in _listify(item.get("thread_aliases")) if str(alias).strip()]
        if not label:
            issues.append("story_bible longline thread missing label")
            continue
        if not aliases:
            issues.append(f"story_bible longline thread missing aliases: {label}")
            continue
        if not plan_threads.intersection(aliases):
            issues.append(f"story_bible longline thread not mapped into plan threads: {label}")

    planned_chapters = [chapter for chapter in chapters if str(chapter.get("status") or "planned") != "published"]
    lookahead_chapters = planned_chapters[:lookahead]
    lookahead_reports: list[dict[str, Any]] = []

    for chapter in lookahead_chapters:
        chapter_number = _chapter_number(chapter)
        missing = _missing_required_keys(chapter, required_chapter_keys)
        progression = _progression_for_chapter(chapter_number, writing_system)
        target = chapter.get("intimacy_target", {}) or {}
        explicit_level = _coerce_int(target.get("level"), 0)
        default_level = _coerce_int(progression.get("default_level"), 0)
        if explicit_level and default_level and explicit_level < default_level:
            warnings.append(
                f"chapter {chapter_number} intimacy_target.level={explicit_level} is below progression default {default_level}"
            )
        if missing:
            issues.append(f"chapter {chapter_number} missing structured fields: {', '.join(missing)}")
        beats = _listify(chapter.get("beats"))
        if beats and len(beats) != 4:
            issues.append(f"chapter {chapter_number} beats must contain exactly 4 items")
        turn_role = str(chapter.get("turn_role") or "").strip()
        if turn_role and turn_role not in VALID_TURN_ROLES:
            issues.append(f"chapter {chapter_number} invalid turn_role: {turn_role}")
        expected_turn_role = "ignite" if chapter_number % 2 == 1 else "detonate"
        if turn_role and turn_role != expected_turn_role:
            issues.append(f"chapter {chapter_number} turn_role={turn_role} expected {expected_turn_role}")
        hook_type = str(chapter.get("hook_type") or "").strip()
        if hook_type and hook_type not in VALID_HOOK_TYPES:
            issues.append(f"chapter {chapter_number} invalid hook_type: {hook_type}")
        checkpoint = str(chapter.get("volume_upgrade_checkpoint") or "").strip()
        if chapter_number % 8 == 0 and checkpoint != "required":
            issues.append(
                f"chapter {chapter_number} volume_upgrade_checkpoint={checkpoint or 'missing'} expected required"
            )
        if chapter_number % 2 == 0:
            previous = chapter_map.get(chapter_number - 1) or {}
            if previous and str(previous.get("pair_payoff") or "").strip() != str(chapter.get("pair_payoff") or "").strip():
                issues.append(f"chapter pair {chapter_number - 1}-{chapter_number} pair_payoff mismatch")
        lookahead_reports.append(
            {
                "chapter_number": chapter_number,
                "title": chapter.get("display_title") or chapter.get("title"),
                "missing_fields": missing,
                "intimacy_level": explicit_level or default_level or None,
                "seed_threads": len(_listify(chapter.get("seed_threads"))),
                "payoff_threads": len(_listify(chapter.get("payoff_threads"))),
            }
        )

    published_without_romance = [
        _chapter_number(chapter)
        for chapter in chapters
        if str(chapter.get("status") or "") == "published" and not str(chapter.get("romance_beat") or "").strip()
    ]
    if published_without_romance:
        warnings.append(
            "published chapters missing romance_beat metadata: "
            + ", ".join(str(number) for number in published_without_romance)
        )

    published_without_local_file = []
    for chapter in chapters:
        chapter_number = _chapter_number(chapter)
        if str(chapter.get("status") or "") != "published":
            continue
        if chapters_dir is None:
            local_path = None
        else:
            local_path = chapters_dir / f"chapter-{chapter_number:03d}.md"
        if local_path is None or not local_path.exists():
            published_without_local_file.append(chapter_number)
    if published_without_local_file:
        warnings.append(
            "published chapters missing local markdown archive: "
            + ", ".join(str(number) for number in published_without_local_file)
        )

    return {
        "ok": not issues,
        "issue_count": len(issues),
        "warning_count": len(warnings),
        "issues": issues,
        "warnings": warnings,
        "lookahead": lookahead_reports,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit fiction series-plan execution readiness.")
    parser.add_argument(
        "--plan",
        default="state/drafts/serials/quanyuzhou-relian/series-plan.json",
        help="Path to series-plan.json relative to repo root.",
    )
    parser.add_argument("--lookahead", type=int, default=10, help="How many upcoming planned chapters to inspect.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a text report.")
    args = parser.parse_args()

    plan_path = _resolve_path(args.plan)
    if plan_path is None or not plan_path.exists():
        raise SystemExit(f"plan not found: {args.plan}")

    plan = read_json(plan_path, default={})
    report = audit_plan(plan, lookahead=max(1, args.lookahead), plan_path=plan_path)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1

    print(f"Plan audit: ok={report['ok']} issues={report['issue_count']} warnings={report['warning_count']}")
    if report["issues"]:
        print("Issues:")
        for item in report["issues"]:
            print(f"- {item}")
    if report["warnings"]:
        print("Warnings:")
        for item in report["warnings"]:
            print(f"- {item}")
    if report["lookahead"]:
        print("Lookahead:")
        for item in report["lookahead"]:
            missing = ", ".join(item["missing_fields"]) if item["missing_fields"] else "none"
            print(
                f"- ch{item['chapter_number']}: intimacy={item['intimacy_level']} "
                f"seed_threads={item['seed_threads']} payoff_threads={item['payoff_threads']} missing={missing}"
            )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
