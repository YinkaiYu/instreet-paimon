#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import (
    InStreetClient,
    LOGS_DIR,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    payload_digest,
    queue_outbound_action,
    run_outbound_action,
)


def _read_content(args: argparse.Namespace) -> str:
    if args.content_file:
        return Path(args.content_file).read_text(encoding="utf-8").strip()
    if args.content:
        return args.content.strip()
    raise ValueError("content is required")


def _read_optional_text(value: str | None, file_path: str | None) -> str:
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()
    return (value or "").strip()


def _log(action: str, payload: dict, result: dict | None, dry_run: bool) -> None:
    append_jsonl(
        LOGS_DIR / "publication_log.jsonl",
        {
            "timestamp": now_utc(),
            "action": action,
            "dry_run": dry_run,
            "payload": payload,
            "result": result,
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish or interact with InStreet.")
    parser.add_argument("--dry-run", action="store_true", help="Print the payload without calling the API.")
    parser.add_argument("--enqueue-only", action="store_true", help="Store the action locally without calling the API.")
    parser.add_argument(
        "--queue-on-failure",
        action="store_true",
        help="Store the action locally if API delivery fails after retries.",
    )
    parser.add_argument("--dedupe-key", help="Stable idempotency key for this action.")
    parser.add_argument("--retries", type=int, default=3, help="Retry attempts for write actions.")
    parser.add_argument("--retry-delay-sec", type=float, default=2.0, help="Delay between retries.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    post = subparsers.add_parser("post")
    post.add_argument("--title", required=True)
    post.add_argument("--content")
    post.add_argument("--content-file")
    post.add_argument("--submolt", default="square")
    post.add_argument("--group-id")

    comment = subparsers.add_parser("comment")
    comment.add_argument("--post-id", required=True)
    comment.add_argument("--parent-id")
    comment.add_argument("--content")
    comment.add_argument("--content-file")

    message = subparsers.add_parser("message")
    message.add_argument("--recipient-username")
    message.add_argument("--thread-id")
    message.add_argument("--content")
    message.add_argument("--content-file")

    profile = subparsers.add_parser("update-profile")
    profile.add_argument("--username")
    profile.add_argument("--bio")
    profile.add_argument("--bio-file")
    profile.add_argument("--avatar-url")
    profile.add_argument("--email")

    work = subparsers.add_parser("work")
    work.add_argument("--title", required=True)
    work.add_argument("--synopsis")
    work.add_argument("--synopsis-file")
    work.add_argument("--genre", default="other")
    work.add_argument("--tag", action="append", dest="tags", default=[])
    work.add_argument("--cover-url")

    update_work = subparsers.add_parser("update-work")
    update_work.add_argument("--work-id", required=True)
    update_work.add_argument("--title")
    update_work.add_argument("--synopsis")
    update_work.add_argument("--synopsis-file")
    update_work.add_argument("--genre")
    update_work.add_argument("--tag", action="append", dest="tags")
    update_work.add_argument("--cover-url")
    update_work.add_argument("--status", choices=["ongoing", "completed", "hiatus"])

    delete_work = subparsers.add_parser("delete-work")
    delete_work.add_argument("--work-id", required=True)

    update_group = subparsers.add_parser("update-group")
    update_group.add_argument("--group-id", required=True)
    update_group.add_argument("--display-name")
    update_group.add_argument("--description")
    update_group.add_argument("--description-file")
    update_group.add_argument("--rules")
    update_group.add_argument("--rules-file")
    update_group.add_argument("--icon")
    update_group.add_argument("--join-mode", choices=["open", "approval"])

    chapter = subparsers.add_parser("chapter")
    chapter.add_argument("--work-id", required=True)
    chapter.add_argument("--title", required=True)
    chapter.add_argument("--content")
    chapter.add_argument("--content-file")

    update_chapter = subparsers.add_parser("update-chapter")
    update_chapter.add_argument("--work-id", required=True)
    update_chapter.add_argument("--chapter-number", required=True, type=int)
    update_chapter.add_argument("--title")
    update_chapter.add_argument("--content")
    update_chapter.add_argument("--content-file")

    delete_chapter = subparsers.add_parser("delete-chapter")
    delete_chapter.add_argument("--work-id", required=True)
    delete_chapter.add_argument("--chapter-number", required=True, type=int)

    follow = subparsers.add_parser("follow")
    follow.add_argument("--username", required=True)

    mark = subparsers.add_parser("mark-read")
    mark.add_argument("--post-id", required=True)

    return parser


def _default_dedupe_key(command: str, payload: dict) -> str:
    if command == "post":
        return f"{payload.get('submolt','square')}:{payload.get('group_id') or '-'}:{payload.get('title','')}"
    if command == "comment":
        parent = payload.get("parent_id") or "root"
        return f"{payload.get('post_id')}:{parent}:{payload_digest(payload.get('content',''))[:10]}"
    if command == "message":
        recipient = payload.get("thread_id") or payload.get("recipient_username") or "unknown"
        return f"{recipient}:{payload_digest(payload.get('content',''))[:10]}"
    if command == "update-profile":
        username = payload.get("username") or "me"
        return f"{username}:{payload_digest(payload)[:10]}"
    if command == "work":
        return payload.get("title", "")
    if command == "update-work":
        return f"{payload.get('work_id','')}:{payload_digest(payload)[:10]}"
    if command == "delete-work":
        return payload.get("work_id", "")
    if command == "update-group":
        return f"{payload.get('group_id','')}:{payload_digest(payload)[:10]}"
    if command == "chapter":
        return f"{payload.get('work_id')}:{payload.get('title','')}"
    if command == "update-chapter":
        return f"{payload.get('work_id')}:{payload.get('chapter_number')}"
    if command == "delete-chapter":
        return f"{payload.get('work_id')}:{payload.get('chapter_number')}"
    if command == "follow":
        return payload.get("username", "")
    if command == "mark-read":
        return payload.get("post_id", "")
    return payload_digest(payload)


def main() -> None:
    ensure_runtime_dirs()
    parser = build_parser()
    args = parser.parse_args()
    client = InStreetClient(load_config())

    channel = "instreet"
    if args.command == "post":
        content = _read_content(args)
        payload = {
            "title": args.title,
            "content": content,
            "submolt": args.submolt,
            "group_id": args.group_id,
        }
        action = lambda: client.create_post(args.title, content, submolt=args.submolt, group_id=args.group_id)
    elif args.command == "comment":
        content = _read_content(args)
        payload = {
            "post_id": args.post_id,
            "parent_id": args.parent_id,
            "content": content,
        }
        action = lambda: client.create_comment(args.post_id, content, parent_id=args.parent_id)
    elif args.command == "message":
        content = _read_content(args)
        payload = {
            "recipient_username": args.recipient_username,
            "thread_id": args.thread_id,
            "content": content,
        }
        if args.thread_id:
            action = lambda: client.reply_message(args.thread_id, content)
        elif args.recipient_username:
            action = lambda: client.send_message(args.recipient_username, content)
        else:
            raise ValueError("message requires --recipient-username or --thread-id")
    elif args.command == "update-profile":
        bio = None
        if args.bio is not None or args.bio_file:
            bio = _read_optional_text(args.bio, args.bio_file)
        payload = {}
        if args.username is not None:
            payload["username"] = args.username
        if bio is not None:
            payload["bio"] = bio
        if args.avatar_url is not None:
            payload["avatar_url"] = args.avatar_url
        if args.email is not None:
            payload["email"] = args.email
        if not payload:
            raise ValueError("update-profile requires at least one field to update")
        action = lambda: client.update_me(
            username=args.username,
            bio=bio,
            avatar_url=args.avatar_url,
            email=args.email,
        )
    elif args.command == "work":
        synopsis = _read_optional_text(args.synopsis, args.synopsis_file)
        payload = {
            "title": args.title,
            "synopsis": synopsis,
            "genre": args.genre,
            "tags": args.tags,
            "cover_url": args.cover_url,
        }
        action = lambda: client.create_work(
            args.title,
            synopsis=synopsis,
            genre=args.genre,
            tags=args.tags,
            cover_url=args.cover_url,
        )
    elif args.command == "update-work":
        synopsis = None
        if args.synopsis is not None or args.synopsis_file:
            synopsis = _read_optional_text(args.synopsis, args.synopsis_file)
        payload = {"work_id": args.work_id}
        if args.title is not None:
            payload["title"] = args.title
        if synopsis is not None:
            payload["synopsis"] = synopsis
        if args.genre is not None:
            payload["genre"] = args.genre
        if args.tags is not None:
            payload["tags"] = args.tags
        if args.cover_url is not None:
            payload["cover_url"] = args.cover_url
        if args.status is not None:
            payload["status"] = args.status
        if len(payload) == 1:
            raise ValueError("update-work requires at least one field to update")
        action = lambda: client.update_work(
            args.work_id,
            title=args.title,
            synopsis=synopsis,
            genre=args.genre,
            tags=args.tags,
            cover_url=args.cover_url,
            status=args.status,
        )
    elif args.command == "delete-work":
        payload = {"work_id": args.work_id}
        action = lambda: client.delete_work(args.work_id)
    elif args.command == "update-group":
        description = None
        if args.description is not None or args.description_file:
            description = _read_optional_text(args.description, args.description_file)
        rules = None
        if args.rules is not None or args.rules_file:
            rules = _read_optional_text(args.rules, args.rules_file)
        payload = {"group_id": args.group_id}
        if args.display_name is not None:
            payload["display_name"] = args.display_name
        if description is not None:
            payload["description"] = description
        if rules is not None:
            payload["rules"] = rules
        if args.icon is not None:
            payload["icon"] = args.icon
        if args.join_mode is not None:
            payload["join_mode"] = args.join_mode
        if len(payload) == 1:
            raise ValueError("update-group requires at least one field to update")
        action = lambda: client.update_group(
            args.group_id,
            display_name=args.display_name,
            description=description,
            rules=rules,
            icon=args.icon,
            join_mode=args.join_mode,
        )
    elif args.command == "chapter":
        content = _read_content(args)
        payload = {"work_id": args.work_id, "title": args.title, "content": content}
        action = lambda: client.publish_chapter(args.work_id, args.title, content)
    elif args.command == "update-chapter":
        content = None
        if args.content is not None or args.content_file:
            content = _read_optional_text(args.content, args.content_file)
        payload = {"work_id": args.work_id, "chapter_number": args.chapter_number}
        if args.title is not None:
            payload["title"] = args.title
        if content is not None:
            payload["content"] = content
        if len(payload) == 2:
            raise ValueError("update-chapter requires at least one field to update")
        action = lambda: client.update_chapter(
            args.work_id,
            args.chapter_number,
            title=args.title,
            content=content,
        )
    elif args.command == "delete-chapter":
        payload = {"work_id": args.work_id, "chapter_number": args.chapter_number}
        action = lambda: client.delete_chapter(args.work_id, args.chapter_number)
    elif args.command == "follow":
        payload = {"username": args.username}
        action = lambda: client.follow(args.username)
    elif args.command == "mark-read":
        payload = {"post_id": args.post_id}
        action = lambda: client.mark_read_by_post(args.post_id)
    else:
        raise ValueError(f"unknown command: {args.command}")

    dedupe_key = args.dedupe_key or _default_dedupe_key(args.command, payload)
    if args.dry_run:
        _log(args.command, payload, None, args.dry_run)
        print(payload)
        return
    if args.enqueue_only:
        record = queue_outbound_action(
            channel,
            args.command,
            dedupe_key,
            payload,
            meta={"source": "publish.py", "mode": "enqueue-only"},
        )
        output = {
            "queued": True,
            "record": record,
        }
        _log(args.command, payload, output, args.dry_run)
        print(output)
        return

    try:
        result, record, deduped = run_outbound_action(
            channel,
            args.command,
            dedupe_key,
            payload,
            action,
            retries=args.retries,
            retry_delay_sec=args.retry_delay_sec,
            meta={
                "source": "publish.py",
                "chapter_number": payload.get("chapter_number"),
                "work_id": payload.get("work_id"),
            },
        )
    except Exception as exc:
        if not args.queue_on_failure:
            raise
        record = queue_outbound_action(
            channel,
            args.command,
            dedupe_key,
            payload,
            error_text=str(exc),
            meta={"source": "publish.py", "mode": "queue-on-failure"},
        )
        output = {
            "queued": True,
            "error": str(exc),
            "record": record,
            "deduped": False,
        }
        _log(args.command, payload, output, args.dry_run)
        print(output)
        return
    output = {
        "result": result,
        "record": record,
        "deduped": deduped,
    }
    _log(args.command, payload, output, args.dry_run)
    print(output)


if __name__ == "__main__":
    main()
