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
    "supporting_cast_system",
    "sweetness_upgrade_vectors",
]
REQUIRED_STORY_BIBLE_KEYS = [
    "setting_anchor",
    "protagonists",
    "supporting_cast",
    "relationship_rules",
    "organizations",
    "terminology_rules",
    "longline_threads",
    "ending_constraints",
    "style_bans",
]
SUPPORTING_CAST_REQUIRED_KEYS = [
    "name",
    "role",
    "memory_anchor",
    "relationship_to_protagonists",
    "first_appearance_chapter",
    "reentry_plan",
]
SUPPORTING_CAST_FILE_REQUIRED_KEYS = [
    "character_id",
    "name",
    "tier",
    "faction",
    "role",
    "memory_anchor",
    "relationship_to_protagonists",
    "first_appearance_chapter",
    "active_windows",
    "growth_or_turn",
    "exit_mode",
    "reveal",
    "reentry_plan",
]
DEFAULT_LOOKAHEAD_REQUIRED_CHAPTER_KEYS = [
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
VALID_TURN_ROLES = {"ignite", "detonate"}
VALID_HOOK_TYPES = {"rule", "reveal", "threat", "choice", "payoff", "reversal"}
VALID_INTIMACY_EXECUTION_MODES = {"no_full_sex", "optional_full_sex", "must_full_sex", "afterglow_only"}
REQUIRED_SWEETNESS_TARGET_KEYS = ["core_mode", "must_land", "novelty_rule", "carryover"]
REQUIRED_INTIMACY_TARGET_KEYS = ["level", "label", "execution_mode", "boundary_note", "scene_payload", "afterglow_requirement", "on_page_expectation"]
LEGACY_INTIMACY_TEMPLATE = "同时满足：允许正面写到上床、做爱推进和事后余温，必须有连续动作、身体反应和情绪递进，不靠黑屏跳过。"
CAST_CHAPTER_DIRECTIVE_KEYS = [
    "active_cast",
    "new_cast_introductions",
    "cast_returns",
    "cast_exit_or_fade",
    "antagonist_pressure_source",
]


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _listify(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


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


def _chapter_cast_directive_values(chapter: dict[str, Any]) -> dict[str, list[str]]:
    directives: dict[str, list[str]] = {}
    for key in CAST_CHAPTER_DIRECTIVE_KEYS:
        raw_value = chapter.get(key)
        values: list[str] = []
        if isinstance(raw_value, str):
            token = raw_value.strip()
            if token:
                values.append(token)
        elif isinstance(raw_value, list):
            values.extend(str(item).strip() for item in raw_value if str(item).strip())
        if values:
            directives[key] = values
    return directives


def _known_cast_identifiers(items: list[dict[str, Any]]) -> set[str]:
    identifiers: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in ("character_id", "name"):
            token = str(item.get(key) or "").strip()
            if token:
                identifiers.add(token)
    return identifiers


def _cast_is_active_for_chapter(item: dict[str, Any], chapter_number: int) -> bool:
    windows = _dict_list(item.get("active_windows"))
    for window in windows:
        start = _coerce_int(window.get("start"), 0)
        end = _coerce_int(window.get("end"), 0)
        if start and end and start <= chapter_number <= end:
            return True
    key_chapters = item.get("key_chapters") or {}
    if isinstance(key_chapters, dict):
        for key in ("entry", "turn", "exit", "return"):
            if chapter_number in [_coerce_int(value, 0) for value in _listify(key_chapters.get(key))]:
                return True
    return False


def _expected_cast_identifiers(items: list[dict[str, Any]], chapter_number: int) -> list[str]:
    expected: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not _cast_is_active_for_chapter(item, chapter_number):
            continue
        identifier = str(item.get("character_id") or item.get("name") or "").strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        expected.append(identifier)
    return expected


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
    file_cast: list[dict[str, Any]] = []
    known_cast_ids: set[str] = set()

    for label, path_value in (
        ("synopsis", work.get("synopsis_path")),
        ("story bible", work.get("story_bible_path") or story_bible.get("source_path")),
        ("style guide", writing_notes.get("style_guide_path")),
        ("supporting cast", work.get("supporting_cast_path") or (writing_system.get("supporting_cast_system") or {}).get("cast_path")),
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
        ("supporting cast", "supporting_cast_system"),
    ):
        payload = writing_system.get(key, {}) or {}
        if key == "foreshadow_system":
            path_key = "ledger_path"
        elif key == "hook_system":
            path_key = "library_path"
        elif key == "supporting_cast_system":
            path_key = "cast_path"
        else:
            path_key = "log_path"
        target = _resolve_path(payload.get(path_key))
        if target is None or not target.exists():
            issues.append(f"{label} path missing or unreadable: {payload.get(path_key)}")

    for key in REQUIRED_STORY_BIBLE_KEYS:
        if not story_bible.get(key):
            issues.append(f"story_bible missing required key: {key}")
    for optional_key in ("phase_cast_arcs", "cast_lifecycle_rules"):
        if not story_bible.get(optional_key):
            warnings.append(f"story_bible missing recommended key: {optional_key}")

    support_cast = _dict_list(story_bible.get("supporting_cast"))
    if support_cast:
        seen_names: set[str] = set()
        for item in support_cast:
            missing = _missing_required_keys(item, SUPPORTING_CAST_REQUIRED_KEYS)
            name = str(item.get("name") or "").strip()
            if missing:
                issues.append(
                    f"story_bible supporting_cast item missing fields"
                    + (f" for {name}" if name else "")
                    + f": {', '.join(missing)}"
                )
            if name:
                if name in seen_names:
                    issues.append(f"story_bible supporting_cast duplicate name: {name}")
                seen_names.add(name)
            if _coerce_int(item.get("first_appearance_chapter"), 0) <= 0:
                issues.append(f"story_bible supporting_cast first_appearance_chapter invalid for {name or 'unknown'}")
    cast_path = _resolve_path(
        work.get("supporting_cast_path") or (writing_system.get("supporting_cast_system") or {}).get("cast_path")
    )
    if cast_path and cast_path.exists():
        cast_payload = read_json(cast_path, default={}) or {}
        file_cast = _dict_list(cast_payload.get("characters"))
        selection_policy = cast_payload.get("selection_policy") or {}
        if not file_cast:
            issues.append("supporting cast file missing characters list")
        else:
            story_names = {str(item.get("name") or "").strip() for item in support_cast if str(item.get("name") or "").strip()}
            file_names = {str(item.get("name") or "").strip() for item in file_cast if str(item.get("name") or "").strip()}
            if story_names and file_names and not story_names.issubset(file_names):
                issues.append("story_bible supporting_cast must be a subset of supporting-cast file characters")
            known_cast_ids = _known_cast_identifiers(file_cast)
            if not isinstance(selection_policy, dict) or not selection_policy:
                warnings.append("supporting cast file missing selection_policy")
            has_group_node = False
            for item in file_cast:
                missing = _missing_required_keys(item, SUPPORTING_CAST_FILE_REQUIRED_KEYS)
                name = str(item.get("name") or "").strip()
                if missing:
                    issues.append(
                        f"supporting cast file item missing fields"
                        + (f" for {name}" if name else "")
                        + f": {', '.join(missing)}"
                    )
                tier = str(item.get("tier") or "").strip()
                if tier in {"group_node", "returning_payoff"}:
                    has_group_node = True
                active_windows = _dict_list(item.get("active_windows"))
                if not active_windows:
                    issues.append(f"supporting cast file item missing active_windows: {name or 'unknown'}")
                for window in active_windows:
                    start = _coerce_int(window.get("start"), 0)
                    end = _coerce_int(window.get("end"), 0)
                    if start <= 0 or end <= 0 or end < start:
                        issues.append(f"supporting cast file invalid active_window for {name or 'unknown'}")
                        break
                reveal = item.get("reveal") or {}
                if not isinstance(reveal, dict):
                    issues.append(f"supporting cast file reveal must be an object for {name or 'unknown'}")
                else:
                    named_after = _coerce_int(reveal.get("named_after_chapter"), 0)
                    full_after = _coerce_int(reveal.get("full_detail_after_chapter"), 0)
                    if named_after and full_after and full_after < named_after:
                        issues.append(
                            f"supporting cast file reveal order invalid for {name or 'unknown'}: full_detail_after_chapter < named_after_chapter"
                        )
            if not has_group_node:
                warnings.append("supporting cast file has no group_node/returning_payoff entries")

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
        cast_directives = _chapter_cast_directive_values(chapter)
        progression = _progression_for_chapter(chapter_number, writing_system)
        target = chapter.get("intimacy_target", {}) or {}
        sweetness_target = chapter.get("sweetness_target", {}) or {}
        explicit_level = _coerce_int(target.get("level"), 0)
        default_level = _coerce_int(progression.get("default_level"), 0)
        if explicit_level and default_level and explicit_level < default_level:
            warnings.append(
                f"chapter {chapter_number} intimacy_target.level={explicit_level} is below progression default {default_level}"
            )
        if missing:
            issues.append(f"chapter {chapter_number} missing structured fields: {', '.join(missing)}")
        intimacy_missing = _missing_required_keys(target, REQUIRED_INTIMACY_TARGET_KEYS) if isinstance(target, dict) else REQUIRED_INTIMACY_TARGET_KEYS
        if intimacy_missing:
            issues.append(f"chapter {chapter_number} intimacy_target missing fields: {', '.join(intimacy_missing)}")
        execution_mode = str(target.get("execution_mode") or "").strip()
        if execution_mode and execution_mode not in VALID_INTIMACY_EXECUTION_MODES:
            issues.append(f"chapter {chapter_number} invalid intimacy_target.execution_mode: {execution_mode}")
        on_page_expectation = str(target.get("on_page_expectation") or "")
        if LEGACY_INTIMACY_TEMPLATE in on_page_expectation:
            issues.append(f"chapter {chapter_number} still uses legacy intimacy template")
        sweetness_missing = (
            _missing_required_keys(sweetness_target, REQUIRED_SWEETNESS_TARGET_KEYS)
            if isinstance(sweetness_target, dict)
            else REQUIRED_SWEETNESS_TARGET_KEYS
        )
        if sweetness_missing:
            issues.append(f"chapter {chapter_number} sweetness_target missing fields: {', '.join(sweetness_missing)}")
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
        if known_cast_ids:
            referenced_ids = {
                token
                for values in cast_directives.values()
                for token in values
            }
            unknown_ids = sorted(token for token in referenced_ids if token not in known_cast_ids)
            if unknown_ids:
                issues.append(
                    f"chapter {chapter_number} references unknown cast identifiers: {', '.join(unknown_ids)}"
                )
            if not cast_directives:
                warnings.append(
                    f"chapter {chapter_number} missing cast execution fields: {', '.join(CAST_CHAPTER_DIRECTIVE_KEYS)}"
                )
            expected_ids = _expected_cast_identifiers(file_cast, chapter_number)
            missing_expected = [token for token in expected_ids if token not in referenced_ids]
            if missing_expected:
                warnings.append(
                    f"chapter {chapter_number} cast directives miss active/reentry characters: {', '.join(missing_expected[:4])}"
                )
        lookahead_reports.append(
            {
                "chapter_number": chapter_number,
                "title": chapter.get("display_title") or chapter.get("title"),
                "missing_fields": missing,
                "intimacy_level": explicit_level or default_level or None,
                "intimacy_execution_mode": execution_mode or None,
                "seed_threads": len(_listify(chapter.get("seed_threads"))),
                "payoff_threads": len(_listify(chapter.get("payoff_threads"))),
                "cast_directives": sum(len(values) for values in cast_directives.values()),
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
                f"- ch{item['chapter_number']}: intimacy={item['intimacy_level']} mode={item.get('intimacy_execution_mode') or 'n/a'} "
                f"seed_threads={item['seed_threads']} payoff_threads={item['payoff_threads']} missing={missing}"
            )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
