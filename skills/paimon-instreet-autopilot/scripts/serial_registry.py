#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from common import CURRENT_STATE_DIR, read_json
from serial_state import (
    clear_manual_override,
    describe_next_serial_action,
    load_serial_registry,
    record_published_chapter,
    retire_serial_work,
    set_manual_override,
    sync_serial_registry,
    upsert_serial_work,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and update the literary serial registry.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("sync")

    show_next = subparsers.add_parser("next")
    show_next.add_argument("--work-id")

    configure = subparsers.add_parser("configure")
    configure.add_argument("--work-id", required=True)
    configure.add_argument("--title")
    configure.add_argument("--genre")
    configure.add_argument("--status")
    configure.add_argument("--launch-source")
    configure.add_argument("--priority-mode")
    configure.add_argument("--content-mode")
    configure.add_argument("--plan-path")
    configure.add_argument("--reference-path")
    configure.add_argument("--series-brief")
    configure.add_argument("--next-chapter-number", type=int)
    configure.add_argument("--next-chapter-title")
    configure.add_argument("--queue-position", choices=["front", "back", "keep"], default="back")
    configure.add_argument("--set-next", action="store_true")
    configure.add_argument("--enable-heartbeat", action="store_true")
    configure.add_argument("--disable-heartbeat", action="store_true")
    configure.add_argument("--allow-manual-bump", action="store_true")
    configure.add_argument("--disallow-manual-bump", action="store_true")

    mark = subparsers.add_parser("mark-published")
    mark.add_argument("--work-id", required=True)
    mark.add_argument("--chapter-number", required=True, type=int)
    mark.add_argument("--title", required=True)
    mark.add_argument("--published-at")
    mark.add_argument("--result-id")
    mark.add_argument("--no-advance-queue", action="store_true")

    override = subparsers.add_parser("override")
    override.add_argument("--work-id", required=True)
    override.add_argument("--reason", required=True)
    override.add_argument("--expire-at")

    retire = subparsers.add_parser("retire")
    retire.add_argument("--work-id", required=True)
    retire.add_argument("--status", default="hiatus")
    retire.add_argument("--drop-entry", action="store_true")

    subparsers.add_parser("clear-override")
    return parser


def _sync() -> dict:
    literary = read_json(CURRENT_STATE_DIR / "literary.json", default={})
    literary_details = read_json(CURRENT_STATE_DIR / "literary_details.json", default={})
    return sync_serial_registry(literary, literary_details)


def _bool_choice(enabled_flag: bool, disabled_flag: bool) -> bool | None:
    if enabled_flag and disabled_flag:
        raise ValueError("conflicting boolean flags")
    if enabled_flag:
        return True
    if disabled_flag:
        return False
    return None


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "sync":
        output = _sync()
    elif args.command == "next":
        output = describe_next_serial_action(load_serial_registry(), work_id=args.work_id)
    elif args.command == "configure":
        heartbeat_enabled = _bool_choice(args.enable_heartbeat, args.disable_heartbeat)
        manual_bump_allowed = _bool_choice(args.allow_manual_bump, args.disallow_manual_bump)
        output = upsert_serial_work(
            args.work_id,
            title=args.title,
            genre=args.genre,
            status=args.status,
            launch_source=args.launch_source,
            heartbeat_enabled=heartbeat_enabled,
            manual_bump_allowed=manual_bump_allowed,
            priority_mode=args.priority_mode,
            content_mode=args.content_mode,
            plan_path=args.plan_path,
            reference_path=args.reference_path,
            series_brief=args.series_brief,
            next_planned_chapter_number=args.next_chapter_number,
            next_planned_title=args.next_chapter_title,
            queue_position=args.queue_position,
            set_next=args.set_next,
        )
    elif args.command == "mark-published":
        output = record_published_chapter(
            args.work_id,
            chapter_number=args.chapter_number,
            title=args.title,
            published_at=args.published_at,
            result_id=args.result_id,
            advance_queue=not args.no_advance_queue,
        )
    elif args.command == "override":
        output = set_manual_override(args.work_id, reason=args.reason, expire_at=args.expire_at)
    elif args.command == "retire":
        output = retire_serial_work(args.work_id, status=args.status, drop_entry=args.drop_entry)
    elif args.command == "clear-override":
        output = clear_manual_override()
    else:  # pragma: no cover
        raise ValueError(f"unknown command: {args.command}")

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
