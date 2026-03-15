#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common import CURRENT_STATE_DIR, REPO_ROOT, now_utc, read_json, write_json


SERIAL_REGISTRY_PATH = CURRENT_STATE_DIR / "serial_registry.json"


def _registry_template() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": now_utc(),
        "literary_queue": [],
        "next_work_id_for_heartbeat": None,
        "manual_override_work_id": None,
        "manual_override_reason": None,
        "manual_override_requested_at": None,
        "manual_override_expire_at": None,
        "works": {},
    }


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_details_map(literary_details: dict[str, Any]) -> dict[str, Any]:
    if "details" in literary_details:
        return literary_details.get("details", {})
    return literary_details


def _infer_content_mode(title: str, genre: str, existing: dict[str, Any] | None = None) -> str:
    if existing and existing.get("content_mode"):
        return str(existing["content_mode"])
    if genre in {"sci-fi", "fantasy", "romance", "mystery", "realism"}:
        return "fiction-serial"
    if genre == "prose-poetry":
        return "lyrical-serial"
    if any(keyword in title for keyword in ("分析", "方法", "机制", "观察", "手册", "笔记")):
        return "essay-serial"
    return "essay-serial"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_override_active(registry: dict[str, Any]) -> bool:
    work_id = registry.get("manual_override_work_id")
    if not work_id:
        return False
    expire_at = _parse_iso(registry.get("manual_override_expire_at"))
    if expire_at is None:
        return True
    return expire_at > datetime.now(timezone.utc)


def _chapter_display_title(chapter: dict[str, Any]) -> str | None:
    display_title = str(chapter.get("display_title") or "").strip()
    if display_title:
        return display_title
    number = _coerce_int(chapter.get("chapter_number") or chapter.get("number"), 0)
    title = str(chapter.get("title") or "").strip()
    if title.startswith("第"):
        return title
    if number and title:
        return f"第{number}章：{title}"
    if number:
        return f"第{number}章"
    if title:
        return title
    return None


def load_serial_registry() -> dict[str, Any]:
    registry = read_json(SERIAL_REGISTRY_PATH, default=_registry_template())
    registry.setdefault("literary_queue", [])
    registry.setdefault("works", {})
    registry.setdefault("next_work_id_for_heartbeat", None)
    registry.setdefault("manual_override_work_id", None)
    registry.setdefault("manual_override_reason", None)
    registry.setdefault("manual_override_requested_at", None)
    registry.setdefault("manual_override_expire_at", None)
    return registry


def save_serial_registry(registry: dict[str, Any]) -> dict[str, Any]:
    registry["updated_at"] = now_utc()
    write_json(SERIAL_REGISTRY_PATH, registry)
    return registry


def resolve_repo_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def relative_repo_path(path: str | Path | None) -> str | None:
    if path is None:
        return None
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (REPO_ROOT / resolved).resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def load_work_plan(plan_path: str | None) -> dict[str, Any] | None:
    target = resolve_repo_path(plan_path)
    if target is None:
        return None
    return read_json(target, default=None)


def _find_plan_chapter(work_plan: dict[str, Any], chapter_number: int | None) -> dict[str, Any] | None:
    chapters = work_plan.get("chapters", [])
    if chapter_number:
        for chapter in chapters:
            if _coerce_int(chapter.get("chapter_number") or chapter.get("number"), 0) == chapter_number:
                return chapter
    for chapter in chapters:
        if str(chapter.get("status") or "planned") != "published":
            return chapter
    return None


def get_next_chapter_plan(work_entry: dict[str, Any]) -> dict[str, Any] | None:
    work_plan = load_work_plan(work_entry.get("plan_path"))
    if not work_plan:
        return None
    target_number = _coerce_int(work_entry.get("next_planned_chapter_number"), 0) or None
    chapter = _find_plan_chapter(work_plan, target_number)
    if not chapter:
        return None
    planned = dict(chapter)
    planned["display_title"] = _chapter_display_title(chapter)
    planned["plan_path"] = work_entry.get("plan_path")
    planned["reference_path"] = work_entry.get("reference_path")
    planned["work_title"] = work_plan.get("work", {}).get("title") or work_entry.get("title")
    planned["content_mode"] = work_plan.get("work", {}).get("content_mode") or work_entry.get("content_mode")
    planned["series_brief"] = work_plan.get("work", {}).get("series_brief") or work_entry.get("series_brief")
    planned["writing_notes"] = work_plan.get("writing_notes", {})
    return planned


def describe_next_serial_action(
    registry: dict[str, Any] | None = None,
    *,
    work_id: str | None = None,
    available_work_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    registry = registry or load_serial_registry()
    work_entry: dict[str, Any] | None
    if work_id:
        work_entry = registry.get("works", {}).get(work_id)
    else:
        work_entry = select_heartbeat_work(registry, available_work_ids=available_work_ids)
    if not work_entry:
        return None
    chapter_plan = get_next_chapter_plan(work_entry)
    next_title = work_entry.get("next_planned_title")
    if chapter_plan and chapter_plan.get("display_title"):
        next_title = chapter_plan["display_title"]
    return {
        "work_id": work_entry.get("work_id"),
        "work_title": work_entry.get("title"),
        "content_mode": work_entry.get("content_mode"),
        "series_brief": work_entry.get("series_brief"),
        "plan_path": work_entry.get("plan_path"),
        "reference_path": work_entry.get("reference_path"),
        "next_planned_chapter_number": _coerce_int(work_entry.get("next_planned_chapter_number"), 1),
        "next_planned_title": next_title,
        "chapter_plan": chapter_plan,
    }


def _eligible_work_ids(registry: dict[str, Any], *, available_work_ids: set[str] | None = None) -> list[str]:
    works = registry.get("works", {})
    ordered_ids: list[str] = []
    seen: set[str] = set()
    for work_id in registry.get("literary_queue", []) + list(works.keys()):
        if work_id in seen:
            continue
        seen.add(work_id)
        entry = works.get(work_id, {})
        if not entry or not entry.get("heartbeat_enabled", True):
            continue
        if str(entry.get("status") or "ongoing") == "completed":
            continue
        if available_work_ids is not None and work_id not in available_work_ids:
            continue
        ordered_ids.append(work_id)
    return ordered_ids


def select_heartbeat_work(
    registry: dict[str, Any],
    *,
    available_work_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    works = registry.get("works", {})
    eligible_ids = _eligible_work_ids(registry, available_work_ids=available_work_ids)
    if not eligible_ids:
        return None
    if _is_override_active(registry):
        override_id = registry.get("manual_override_work_id")
        if override_id in eligible_ids:
            return works.get(override_id)
    next_work_id = registry.get("next_work_id_for_heartbeat")
    if next_work_id in eligible_ids:
        return works.get(next_work_id)
    return works.get(eligible_ids[0])


def _next_queue_target(registry: dict[str, Any], current_work_id: str | None) -> str | None:
    eligible_ids = _eligible_work_ids(registry)
    if not eligible_ids:
        return None
    if current_work_id not in eligible_ids:
        return eligible_ids[0]
    index = eligible_ids.index(current_work_id)
    return eligible_ids[(index + 1) % len(eligible_ids)]


def sync_serial_registry(literary: dict[str, Any], literary_details: dict[str, Any]) -> dict[str, Any]:
    registry = load_serial_registry()
    works = registry.setdefault("works", {})
    queue = list(registry.get("literary_queue", []))
    detail_map = _resolve_details_map(literary_details)

    available_work_ids: list[str] = []
    for work in literary.get("data", {}).get("works", []):
        work_id = work.get("id")
        if not work_id:
            continue
        available_work_ids.append(work_id)
        detail = detail_map.get(work_id, {})
        work_payload = detail.get("data", {}).get("work", {}) or work
        chapters = detail.get("data", {}).get("chapters", [])
        last_meta = chapters[-1] if chapters else {}
        chapter_count = _coerce_int(work_payload.get("chapter_count"), len(chapters))
        existing = works.get(work_id, {})

        next_planned_chapter_number = _coerce_int(existing.get("next_planned_chapter_number"), chapter_count + 1)
        if next_planned_chapter_number <= chapter_count:
            next_planned_chapter_number = chapter_count + 1

        entry = {
            "work_id": work_id,
            "title": work_payload.get("title") or existing.get("title") or "未命名作品",
            "genre": work_payload.get("genre") or existing.get("genre") or "other",
            "status": work_payload.get("status") or existing.get("status") or "ongoing",
            "launch_source": existing.get("launch_source") or "snapshot-discovered",
            "priority_mode": existing.get("priority_mode") or "rotation",
            "heartbeat_enabled": bool(existing.get("heartbeat_enabled", True)),
            "manual_bump_allowed": bool(existing.get("manual_bump_allowed", True)),
            "content_mode": _infer_content_mode(
                work_payload.get("title") or existing.get("title") or "",
                work_payload.get("genre") or existing.get("genre") or "other",
                existing,
            ),
            "plan_path": existing.get("plan_path"),
            "reference_path": existing.get("reference_path"),
            "series_brief": existing.get("series_brief"),
            "next_planned_chapter_number": next_planned_chapter_number,
            "next_planned_title": existing.get("next_planned_title"),
            "last_published_chapter_number": _coerce_int(last_meta.get("chapter_number"), chapter_count),
            "last_published_title": last_meta.get("title") or existing.get("last_published_title"),
            "last_published_at": last_meta.get("published_at") or existing.get("last_published_at"),
            "last_planning_updated_at": existing.get("last_planning_updated_at") or now_utc(),
        }
        planned = get_next_chapter_plan(entry)
        if planned and planned.get("display_title"):
            entry["next_planned_title"] = planned["display_title"]
        works[work_id] = entry
        if work_id not in queue:
            queue.append(work_id)

    registry["literary_queue"] = [work_id for work_id in queue if work_id in works]
    available_set = set(available_work_ids)
    if registry.get("next_work_id_for_heartbeat") not in _eligible_work_ids(registry, available_work_ids=available_set):
        next_entry = select_heartbeat_work(registry, available_work_ids=available_set)
        registry["next_work_id_for_heartbeat"] = next_entry.get("work_id") if next_entry else None
    if not _is_override_active(registry):
        registry["manual_override_work_id"] = None
        registry["manual_override_reason"] = None
        registry["manual_override_requested_at"] = None
        registry["manual_override_expire_at"] = None
    return save_serial_registry(registry)


def upsert_serial_work(
    work_id: str,
    *,
    title: str | None = None,
    genre: str | None = None,
    status: str | None = None,
    launch_source: str | None = None,
    heartbeat_enabled: bool | None = None,
    manual_bump_allowed: bool | None = None,
    priority_mode: str | None = None,
    content_mode: str | None = None,
    plan_path: str | None = None,
    reference_path: str | None = None,
    series_brief: str | None = None,
    next_planned_chapter_number: int | None = None,
    next_planned_title: str | None = None,
    queue_position: str = "back",
    set_next: bool = False,
) -> dict[str, Any]:
    registry = load_serial_registry()
    existing = registry.setdefault("works", {}).get(work_id, {})
    entry = {
        "work_id": work_id,
        "title": title or existing.get("title") or "未命名作品",
        "genre": genre or existing.get("genre") or "other",
        "status": status or existing.get("status") or "ongoing",
        "launch_source": launch_source or existing.get("launch_source") or "manual-configured",
        "priority_mode": priority_mode or existing.get("priority_mode") or "rotation",
        "heartbeat_enabled": existing.get("heartbeat_enabled", True) if heartbeat_enabled is None else heartbeat_enabled,
        "manual_bump_allowed": existing.get("manual_bump_allowed", True)
        if manual_bump_allowed is None
        else manual_bump_allowed,
        "content_mode": content_mode or existing.get("content_mode") or _infer_content_mode(title or "", genre or "other"),
        "plan_path": relative_repo_path(plan_path) if plan_path else existing.get("plan_path"),
        "reference_path": relative_repo_path(reference_path) if reference_path else existing.get("reference_path"),
        "series_brief": series_brief or existing.get("series_brief"),
        "next_planned_chapter_number": next_planned_chapter_number
        if next_planned_chapter_number is not None
        else _coerce_int(existing.get("next_planned_chapter_number"), 1),
        "next_planned_title": next_planned_title or existing.get("next_planned_title"),
        "last_published_chapter_number": _coerce_int(existing.get("last_published_chapter_number"), 0),
        "last_published_title": existing.get("last_published_title"),
        "last_published_at": existing.get("last_published_at"),
        "last_planning_updated_at": now_utc(),
    }
    planned = get_next_chapter_plan(entry)
    if planned and planned.get("display_title"):
        entry["next_planned_title"] = planned["display_title"]
        entry["next_planned_chapter_number"] = _coerce_int(planned.get("chapter_number") or planned.get("number"), entry["next_planned_chapter_number"])
    registry["works"][work_id] = entry

    queue = [item for item in registry.get("literary_queue", []) if item != work_id]
    if queue_position == "front":
        queue.insert(0, work_id)
    elif queue_position == "keep" and work_id in registry.get("literary_queue", []):
        original_index = registry.get("literary_queue", []).index(work_id)
        queue.insert(min(original_index, len(queue)), work_id)
    else:
        queue.append(work_id)
    registry["literary_queue"] = queue
    if set_next or not registry.get("next_work_id_for_heartbeat"):
        registry["next_work_id_for_heartbeat"] = work_id
    return save_serial_registry(registry)


def set_manual_override(work_id: str, *, reason: str, expire_at: str | None = None) -> dict[str, Any]:
    registry = load_serial_registry()
    registry["manual_override_work_id"] = work_id
    registry["manual_override_reason"] = reason
    registry["manual_override_requested_at"] = now_utc()
    registry["manual_override_expire_at"] = expire_at
    return save_serial_registry(registry)


def clear_manual_override() -> dict[str, Any]:
    registry = load_serial_registry()
    registry["manual_override_work_id"] = None
    registry["manual_override_reason"] = None
    registry["manual_override_requested_at"] = None
    registry["manual_override_expire_at"] = None
    return save_serial_registry(registry)


def record_published_chapter(
    work_id: str,
    *,
    chapter_number: int,
    title: str,
    published_at: str | None = None,
    result_id: str | None = None,
    advance_queue: bool = True,
) -> dict[str, Any]:
    registry = load_serial_registry()
    entry = registry.setdefault("works", {}).get(work_id)
    if not entry:
        raise KeyError(f"unknown serial work id: {work_id}")

    entry["last_published_chapter_number"] = chapter_number
    entry["last_published_title"] = title
    entry["last_published_at"] = published_at or now_utc()
    entry["last_planning_updated_at"] = now_utc()
    if result_id:
        entry["last_result_id"] = result_id

    next_chapter_number = chapter_number + 1
    entry["next_planned_chapter_number"] = next_chapter_number
    entry["next_planned_title"] = None

    work_plan = load_work_plan(entry.get("plan_path"))
    if work_plan:
        changed = False
        for chapter in work_plan.get("chapters", []):
            number = _coerce_int(chapter.get("chapter_number") or chapter.get("number"), 0)
            if number != chapter_number:
                continue
            chapter["status"] = "published"
            chapter["published_at"] = entry["last_published_at"]
            chapter["published_title"] = title
            changed = True
            break
        next_chapter = _find_plan_chapter(work_plan, next_chapter_number)
        if next_chapter:
            entry["next_planned_chapter_number"] = _coerce_int(
                next_chapter.get("chapter_number") or next_chapter.get("number"),
                next_chapter_number,
            )
            entry["next_planned_title"] = _chapter_display_title(next_chapter)
        work_plan["updated_at"] = now_utc()
        if changed:
            target = resolve_repo_path(entry.get("plan_path"))
            if target:
                write_json(target, work_plan)

    registry["works"][work_id] = entry
    if advance_queue:
        registry["next_work_id_for_heartbeat"] = _next_queue_target(registry, work_id)
        if registry.get("manual_override_work_id") == work_id:
            registry["manual_override_work_id"] = None
            registry["manual_override_reason"] = None
            registry["manual_override_requested_at"] = None
            registry["manual_override_expire_at"] = None
    return save_serial_registry(registry)
