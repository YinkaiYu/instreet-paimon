#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any, Callable

from common import (
    InStreetClient,
    ensure_runtime_dirs,
    list_pending_outbound,
    load_config,
    now_utc,
    queue_outbound_action,
    run_outbound_action,
)


def _build_action(client: InStreetClient, action: str, payload: dict[str, Any]) -> Callable[[], Any]:
    if action == "post":
        return lambda: client.create_post(
            payload["title"],
            payload["content"],
            submolt=payload.get("submolt", "square"),
            group_id=payload.get("group_id"),
        )
    if action == "comment":
        return lambda: client.create_comment(
            payload["post_id"],
            payload["content"],
            parent_id=payload.get("parent_id"),
        )
    if action == "message":
        if payload.get("thread_id"):
            return lambda: client.reply_message(payload["thread_id"], payload["content"])
        return lambda: client.send_message(payload["recipient_username"], payload["content"])
    if action == "update-profile":
        return lambda: client.update_me(
            username=payload.get("username"),
            bio=payload.get("bio"),
            avatar_url=payload.get("avatar_url"),
            email=payload.get("email"),
        )
    if action == "chapter":
        return lambda: client.publish_chapter(
            payload["work_id"],
            payload["title"],
            payload["content"],
        )
    if action == "update-group":
        return lambda: client.update_group(
            payload["group_id"],
            display_name=payload.get("display_name"),
            description=payload.get("description"),
            rules=payload.get("rules"),
            icon=payload.get("icon"),
            join_mode=payload.get("join_mode"),
        )
    if action == "delete-chapter":
        return lambda: client.delete_chapter(
            payload["work_id"],
            payload["chapter_number"],
        )
    if action == "follow":
        return lambda: client.follow(payload["username"])
    if action == "mark-read":
        return lambda: client.mark_read_by_post(payload["post_id"])
    raise ValueError(f"unsupported outbound action: {action}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay locally queued outbound InStreet actions.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum queued actions to replay.")
    parser.add_argument("--retries", type=int, default=3, help="Retry attempts for each replayed action.")
    parser.add_argument("--retry-delay-sec", type=float, default=2.0, help="Delay between retries.")
    args = parser.parse_args()

    ensure_runtime_dirs()
    client = InStreetClient(load_config())
    items = list_pending_outbound()
    results: list[dict[str, Any]] = []

    for item in items[: max(0, args.limit)]:
        channel = item["channel"]
        action = item["action"]
        dedupe_key = item["dedupe_key"]
        payload = item["payload"]
        try:
            result, record, deduped = run_outbound_action(
                channel,
                action,
                dedupe_key,
                payload,
                _build_action(client, action, payload),
                retries=args.retries,
                retry_delay_sec=args.retry_delay_sec,
                meta={
                    "source": "replay_outbound.py",
                    "chapter_number": payload.get("chapter_number"),
                    "work_id": payload.get("work_id"),
                },
            )
            results.append(
                {
                    "channel": channel,
                    "action": action,
                    "dedupe_key": dedupe_key,
                    "status": "success",
                    "deduped": deduped,
                    "result": result,
                    "record": record,
                }
            )
        except Exception as exc:  # pragma: no cover - runtime API failures are environment-dependent
            queue_outbound_action(
                channel,
                action,
                dedupe_key,
                payload,
                error_text=str(exc),
                meta={"source": "replay_outbound.py"},
            )
            results.append(
                {
                    "channel": channel,
                    "action": action,
                    "dedupe_key": dedupe_key,
                    "status": "failed",
                    "error": str(exc),
                }
            )

    print(
        json.dumps(
            {
                "processed_at": now_utc(),
                "attempted": min(len(items), max(0, args.limit)),
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
