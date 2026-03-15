#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import (
    ARCHIVE_STATE_DIR,
    CURRENT_STATE_DIR,
    ApiError,
    InStreetClient,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_slug,
    now_utc,
    read_json,
    write_json,
)


def _extract_posts(obj: dict) -> list[dict]:
    data = obj.get("data", {})
    return data.get("data", [])


def _serialize_exception(exc: Exception) -> dict:
    payload = {
        "type": exc.__class__.__name__,
        "message": str(exc),
    }
    if isinstance(exc, ApiError):
        payload["status"] = exc.status
        payload["body"] = exc.body
    return payload


def fetch_best_effort(name: str, loader, *, empty_data) -> tuple[dict, dict | None]:
    try:
        return loader(), None
    except Exception as exc:  # pragma: no cover - best effort fallback
        serialized = _serialize_exception(exc)
        cached = read_json(CURRENT_STATE_DIR / f"{name}.json", default=None)
        if cached is not None:
            failure = {
                "endpoint": name,
                "used_cache": True,
                **serialized,
            }
            cached.setdefault("snapshot_warning", failure)
            return cached, failure
        return {
            "success": False,
            "error": serialized,
            "data": empty_data,
        }, {
            "endpoint": name,
            "used_cache": False,
            **serialized,
        }


def fetch_literary_details(client: InStreetClient, literary: dict) -> dict:
    works = literary.get("data", {}).get("works", [])
    details: dict[str, dict] = {}
    for work in works:
        work_id = work.get("id")
        if not work_id:
            continue
        try:
            details[work_id] = client.literary_work(work_id)
        except Exception as exc:  # pragma: no cover - best effort enrichment
            details[work_id] = {
                "success": False,
                "error": str(exc),
                "data": {
                    "work": work,
                    "chapters": [],
                },
            }
    return {"success": True, "details": details}


def build_overview(
    me: dict,
    home: dict,
    posts: dict,
    literary: dict,
    literary_details: dict,
    groups: dict,
    fetch_failures: list[dict],
) -> dict:
    me_data = me.get("data", {})
    home_data = home.get("data", {})
    account = home_data.get("your_account", {})
    post_items = _extract_posts(posts)
    top_posts = sorted(
        post_items,
        key=lambda item: (item.get("upvotes", 0) + item.get("comment_count", 0)),
        reverse=True,
    )[:5]

    return {
        "captured_at": now_utc(),
        "username": me_data.get("username"),
        "agent_id": me_data.get("id"),
        "score": me_data.get("score"),
        "follower_count": account.get("follower_count"),
        "following_count": account.get("following_count"),
        "unread_notification_count": account.get("unread_notification_count"),
        "unread_message_count": account.get("unread_message_count"),
        "recent_top_posts": [
            {
                "id": post.get("id"),
                "title": post.get("title"),
                "submolt": post.get("submolt", {}).get("name"),
                "upvotes": post.get("upvotes"),
                "comment_count": post.get("comment_count"),
            }
            for post in top_posts
        ],
        "literary_works": literary.get("data", {}).get("works", []),
        "literary_chapter_index": [
            {
                "work_id": work_id,
                "title": detail.get("data", {}).get("work", {}).get("title"),
                "chapter_count": detail.get("data", {}).get("work", {}).get("chapter_count"),
                "chapters": [
                    {
                        "chapter_number": chapter.get("chapter_number"),
                        "title": chapter.get("title"),
                        "published_at": chapter.get("published_at"),
                    }
                    for chapter in detail.get("data", {}).get("chapters", [])
                ],
            }
            for work_id, detail in literary_details.get("details", {}).items()
        ],
        "owned_groups": groups.get("data", {}).get("groups", []),
        "post_count": len(post_items),
        "fetch_failures": fetch_failures,
    }


def save_bundle(target_dir: Path, bundle: dict[str, dict]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for name, payload in bundle.items():
        write_json(target_dir / f"{name}.json", payload)


def run_snapshot(*, archive: bool, post_limit: int, feed_limit: int) -> dict:
    ensure_runtime_dirs()
    config = load_config()
    client = InStreetClient(config)
    agent_id = config.identity["agent_id"]

    fetch_failures: list[dict] = []

    me, me_failure = fetch_best_effort("me", client.me, empty_data={})
    if me_failure:
        fetch_failures.append(me_failure)

    home, home_failure = fetch_best_effort("home", client.home, empty_data={})
    if home_failure:
        fetch_failures.append(home_failure)

    posts, posts_failure = fetch_best_effort(
        "posts",
        lambda: client.posts(agent_id=agent_id, limit=post_limit),
        empty_data={"data": []},
    )
    if posts_failure:
        fetch_failures.append(posts_failure)

    literary, literary_failure = fetch_best_effort(
        "literary",
        lambda: client.literary_works(agent_id=agent_id),
        empty_data={"works": []},
    )
    if literary_failure:
        fetch_failures.append(literary_failure)

    literary_details = fetch_literary_details(client, literary)

    groups, group_failure = fetch_best_effort(
        "groups",
        lambda: client.groups_my(role="owner"),
        empty_data={"groups": []},
    )
    if group_failure:
        fetch_failures.append(group_failure)

    feed, feed_failure = fetch_best_effort(
        "feed",
        lambda: client.feed(sort="new", limit=feed_limit),
        empty_data={"data": []},
    )
    if feed_failure:
        fetch_failures.append(feed_failure)

    messages, messages_failure = fetch_best_effort(
        "messages",
        client.messages,
        empty_data=[],
    )
    if messages_failure:
        fetch_failures.append(messages_failure)

    notifications, notifications_failure = fetch_best_effort(
        "notifications",
        lambda: client.notifications(unread=True, limit=50),
        empty_data=[],
    )
    if notifications_failure:
        fetch_failures.append(notifications_failure)

    overview = build_overview(me, home, posts, literary, literary_details, groups, fetch_failures)

    bundle = {
        "me": me,
        "home": home,
        "posts": posts,
        "literary": literary,
        "literary_details": literary_details,
        "groups": groups,
        "feed": feed,
        "messages": messages,
        "notifications": notifications,
        "fetch_failures": {
            "success": len(fetch_failures) == 0,
            "data": fetch_failures,
        },
        "account_overview": overview,
    }
    save_bundle(CURRENT_STATE_DIR, bundle)

    archive_dir = None
    if archive:
        archive_dir = ARCHIVE_STATE_DIR / now_slug()
        save_bundle(archive_dir, bundle)

    append_jsonl(
        CURRENT_STATE_DIR / "snapshot_log.jsonl",
        {
            "captured_at": overview["captured_at"],
            "score": overview["score"],
            "post_count": overview["post_count"],
            "fetch_failure_count": len(fetch_failures),
            "archive_dir": str(archive_dir) if archive_dir else None,
        },
    )
    return overview


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync live InStreet state into the local repo.")
    parser.add_argument("--archive", action="store_true", help="Also persist the snapshot under state/archive.")
    parser.add_argument("--post-limit", type=int, default=20)
    parser.add_argument("--feed-limit", type=int, default=10)
    args = parser.parse_args()

    overview = run_snapshot(archive=args.archive, post_limit=args.post_limit, feed_limit=args.feed_limit)
    print(
        f"Snapshot captured for {overview['username']} | "
        f"score={overview['score']} | "
        f"followers={overview['follower_count']} | "
        f"unread_notifications={overview['unread_notification_count']}"
    )


if __name__ == "__main__":
    main()
