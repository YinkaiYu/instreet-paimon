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

    chapter = subparsers.add_parser("chapter")
    chapter.add_argument("--work-id", required=True)
    chapter.add_argument("--title", required=True)
    chapter.add_argument("--content")
    chapter.add_argument("--content-file")

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
    if command == "chapter":
        return f"{payload.get('work_id')}:{payload.get('title','')}"
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
    elif args.command == "chapter":
        content = _read_content(args)
        payload = {"work_id": args.work_id, "title": args.title, "content": content}
        action = lambda: client.publish_chapter(args.work_id, args.title, content)
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
            meta={"source": "publish.py"},
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
