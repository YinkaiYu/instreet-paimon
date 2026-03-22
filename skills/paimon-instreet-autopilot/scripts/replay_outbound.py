#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any, Callable

from common import (
    ForumWriteBudgetExceeded,
    InStreetClient,
    ensure_runtime_dirs,
    forum_write_budget_status,
    is_forum_write_rate_limit_error,
    list_pending_outbound,
    load_config,
    load_forum_write_budget_state,
    now_utc,
    outbound_forum_write_kind,
    outbound_forum_write_label,
    queue_outbound_action,
    record_forum_write_rate_limit,
    record_forum_write_success,
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
    if action == "work":
        return lambda: client.create_work(
            payload["title"],
            synopsis=payload.get("synopsis", ""),
            genre=payload.get("genre", "other"),
            tags=payload.get("tags"),
            cover_url=payload.get("cover_url"),
        )
    if action == "update-work":
        return lambda: client.update_work(
            payload["work_id"],
            title=payload.get("title"),
            synopsis=payload.get("synopsis"),
            genre=payload.get("genre"),
            tags=payload.get("tags"),
            cover_url=payload.get("cover_url"),
            status=payload.get("status"),
        )
    if action == "delete-work":
        return lambda: client.delete_work(payload["work_id"])
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
    if action == "appoint-group-admin":
        return lambda: client.appoint_group_admin(payload["group_id"], payload["agent_id"])
    if action == "revoke-group-admin":
        return lambda: client.revoke_group_admin(payload["group_id"], payload["agent_id"])
    if action == "review-group-member":
        return lambda: client.review_group_member(
            payload["group_id"],
            payload["agent_id"],
            action=payload["action"],
        )
    if action == "pin-group-post":
        return lambda: client.pin_group_post(payload["group_id"], payload["post_id"])
    if action == "unpin-group-post":
        return lambda: client.unpin_group_post(payload["group_id"], payload["post_id"])
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
    config = load_config()
    client = InStreetClient(config)
    items = list_pending_outbound()
    results: list[dict[str, Any]] = []
    forum_write_state = load_forum_write_budget_state()

    for item in items[: max(0, args.limit)]:
        channel = item["channel"]
        action = item["action"]
        dedupe_key = item["dedupe_key"]
        payload = item["payload"]
        forum_write_kind = outbound_forum_write_kind(action, payload)
        forum_write_label = outbound_forum_write_label(action, payload)
        if forum_write_kind:
            budget = forum_write_budget_status(config, forum_write_state, write_kind=forum_write_kind)
            if budget.get("blocked"):
                exc = ForumWriteBudgetExceeded(budget, write_kind=forum_write_kind, label=forum_write_label)
                queue_outbound_action(
                    channel,
                    action,
                    dedupe_key,
                    payload,
                    error_text=str(exc),
                    meta={
                        "source": "replay_outbound.py",
                        "mode": "deferred-local-budget",
                        "forum_write_budget": budget,
                    },
                )
                results.append(
                    {
                        "channel": channel,
                        "action": action,
                        "dedupe_key": dedupe_key,
                        "status": "deferred-local-budget",
                        "error": str(exc),
                        "forum_write_budget": budget,
                    }
                )
                continue
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
            budget = None
            if forum_write_kind and not deduped:
                budget = record_forum_write_success(
                    config,
                    forum_write_state,
                    write_kind=forum_write_kind,
                    label=forum_write_label,
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
                    "forum_write_budget": budget,
                }
            )
        except Exception as exc:  # pragma: no cover - runtime API failures are environment-dependent
            budget = None
            if forum_write_kind and is_forum_write_rate_limit_error(exc):
                budget = record_forum_write_rate_limit(
                    config,
                    forum_write_state,
                    exc,
                    retry_delay_sec=args.retry_delay_sec,
                )
            queue_outbound_action(
                channel,
                action,
                dedupe_key,
                payload,
                error_text=str(exc),
                meta={
                    "source": "replay_outbound.py",
                    "forum_write_budget": budget,
                },
            )
            results.append(
                {
                    "channel": channel,
                    "action": action,
                    "dedupe_key": dedupe_key,
                    "status": "failed",
                    "error": str(exc),
                    "forum_write_budget": budget,
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
