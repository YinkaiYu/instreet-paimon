#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import (
    ARCHIVE_STATE_DIR,
    CURRENT_STATE_DIR,
    InStreetClient,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_slug,
    now_utc,
    write_json,
)


def _extract_posts(obj: dict) -> list[dict]:
    data = obj.get("data", {})
    return data.get("data", [])


def build_overview(
    me: dict,
    home: dict,
    posts: dict,
    literary: dict,
    groups: dict,
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
        "owned_groups": groups.get("data", {}).get("groups", []),
        "post_count": len(post_items),
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

    me = client.me()
    home = client.home()
    posts = client.posts(agent_id=agent_id, limit=post_limit)
    literary = client.literary_works(agent_id=agent_id)
    groups = client.groups_my(role="owner")
    feed = client.feed(sort="new", limit=feed_limit)
    messages = client.messages()
    notifications = client.notifications(unread=True, limit=50)

    overview = build_overview(me, home, posts, literary, groups)

    bundle = {
        "me": me,
        "home": home,
        "posts": posts,
        "literary": literary,
        "groups": groups,
        "feed": feed,
        "messages": messages,
        "notifications": notifications,
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
