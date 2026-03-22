#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from common import (
    ApiError,
    ForumWriteBudgetExceeded,
    InStreetClient,
    LOGS_DIR,
    append_jsonl,
    ensure_runtime_dirs,
    forum_write_budget_status,
    is_forum_write_rate_limit_error,
    load_config,
    load_forum_write_budget_state,
    now_utc,
    now_slug,
    outbound_forum_write_kind,
    outbound_forum_write_label,
    outbound_error_policy,
    payload_digest,
    queue_outbound_action,
    record_forum_write_rate_limit,
    record_forum_write_success,
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


def _extract_oracle_trade_cost(result: dict[str, Any] | None) -> float | None:
    if not isinstance(result, dict):
        return None
    data = result.get("data")
    candidates = []
    if isinstance(data, dict):
        candidates.extend(
            [
                data,
                data.get("trade"),
                data.get("transaction"),
                data.get("order"),
            ]
        )
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ("cost", "total_cost", "spent", "amount"):
            value = candidate.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _extract_account_score(result: dict[str, Any] | None) -> float | None:
    if not isinstance(result, dict):
        return None
    data = result.get("data")
    if not isinstance(data, dict):
        return None
    for key in ("score", "karma"):
        value = data.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _oracle_outcome_price(market: dict[str, Any], outcome: str) -> float | None:
    key = "yes_price" if str(outcome).upper() == "YES" else "no_price"
    value = market.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _run_oracle_trade_strategy(client: InStreetClient, args: argparse.Namespace) -> dict[str, Any]:
    outcome = str(args.outcome).upper()
    action = str(args.action).lower()
    if args.deploy_max_balance and action != "buy":
        raise ValueError("--deploy-max-balance currently only supports buy orders")
    if not args.deploy_max_balance and (args.shares is None or int(args.shares) <= 0):
        raise ValueError("oracle-trade without --deploy-max-balance requires --shares >= 1")

    if not args.deploy_max_balance:
        result = client.oracle_trade(
            args.market_id,
            action=action,
            outcome=outcome,
            shares=int(args.shares),
            reason=args.reason,
            max_price=args.max_price,
        )
        cost = _extract_oracle_trade_cost(result)
        return {
            "strategy": "single-order",
            "market_id": args.market_id,
            "action": action,
            "outcome": outcome,
            "total_shares": int(args.shares),
            "total_cost": cost,
            "orders": [
                {
                    "shares": int(args.shares),
                    "cost": cost,
                    "avg_price": (cost / int(args.shares)) if cost is not None and int(args.shares) > 0 else None,
                    "result": result,
                }
            ],
            "stopped_reason": "single-order-completed",
        }

    me = client.me()
    starting_score = _extract_account_score(me)
    if starting_score is None:
        raise RuntimeError("failed to determine current score before oracle trade")
    balance_floor = float(args.balance_floor)
    deployable_balance = max(0.0, starting_score - balance_floor)
    if deployable_balance <= 0:
        raise RuntimeError(
            f"current score {starting_score:.2f} leaves no deployable oracle balance above floor {balance_floor:.2f}"
        )

    target_shares = int(args.shares) if args.shares is not None else None
    chunk_size = max(1, min(int(args.chunk_size), 500))
    max_chunks = max(1, int(args.max_chunks))
    total_cost = 0.0
    total_shares = 0
    orders: list[dict[str, Any]] = []
    stopped_reason = "max-balance-exhausted"
    last_error: str | None = None

    for _ in range(max_chunks):
        remaining_budget = max(0.0, deployable_balance - total_cost)
        if remaining_budget <= 0:
            stopped_reason = "deployable-balance-exhausted"
            break
        if target_shares is not None and total_shares >= target_shares:
            stopped_reason = "target-shares-filled"
            break

        market = client.oracle_market(args.market_id).get("data", {})
        current_price = _oracle_outcome_price(market, outcome)
        if current_price is None or current_price <= 0:
            raise RuntimeError("failed to determine current oracle price")
        if args.max_price is not None and current_price > float(args.max_price):
            stopped_reason = "market-price-above-max-price"
            break

        affordable_shares = int(remaining_budget / current_price)
        if affordable_shares <= 0:
            stopped_reason = "insufficient-balance-for-next-share"
            break
        shares = min(500, chunk_size, affordable_shares)
        if target_shares is not None:
            shares = min(shares, target_shares - total_shares)
        if shares <= 0:
            stopped_reason = "share-target-completed"
            break

        try:
            result = client.oracle_trade(
                args.market_id,
                action=action,
                outcome=outcome,
                shares=shares,
                reason=args.reason,
                max_price=args.max_price,
            )
        except ApiError as exc:
            last_error = str(exc.body if isinstance(exc.body, dict) else exc)
            if orders:
                stopped_reason = "partial-fill-stopped-by-api"
                break
            raise

        cost = _extract_oracle_trade_cost(result)
        if cost is None:
            refreshed_score = _extract_account_score(client.me())
            if refreshed_score is not None:
                cost = max(0.0, (starting_score - refreshed_score) - total_cost)
            else:
                cost = current_price * shares
        total_cost += float(cost)
        total_shares += shares
        orders.append(
            {
                "shares": shares,
                "cost": float(cost),
                "avg_price": (float(cost) / shares) if shares > 0 else None,
                "market_price_before": current_price,
                "result": result,
            }
        )

    remaining_score_estimate = max(balance_floor, starting_score - total_cost)
    return {
        "strategy": "max-balance",
        "market_id": args.market_id,
        "action": action,
        "outcome": outcome,
        "starting_score": starting_score,
        "balance_floor": balance_floor,
        "deployable_balance": deployable_balance,
        "remaining_score_estimate": remaining_score_estimate,
        "total_shares": total_shares,
        "total_cost": total_cost,
        "max_price": args.max_price,
        "orders": orders,
        "stopped_reason": stopped_reason,
        "last_error": last_error,
    }


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

    appoint_group_admin = subparsers.add_parser("appoint-group-admin")
    appoint_group_admin.add_argument("--group-id", required=True)
    appoint_group_admin.add_argument("--agent-id", required=True)

    revoke_group_admin = subparsers.add_parser("revoke-group-admin")
    revoke_group_admin.add_argument("--group-id", required=True)
    revoke_group_admin.add_argument("--agent-id", required=True)

    review_group_member = subparsers.add_parser("review-group-member")
    review_group_member.add_argument("--group-id", required=True)
    review_group_member.add_argument("--agent-id", required=True)
    review_group_member.add_argument("--action", required=True, choices=["approve", "reject"])

    pin_group_post = subparsers.add_parser("pin-group-post")
    pin_group_post.add_argument("--group-id", required=True)
    pin_group_post.add_argument("--post-id", required=True)

    unpin_group_post = subparsers.add_parser("unpin-group-post")
    unpin_group_post.add_argument("--group-id", required=True)
    unpin_group_post.add_argument("--post-id", required=True)

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

    subparsers.add_parser("mark-read-all")

    oracle_trade = subparsers.add_parser("oracle-trade")
    oracle_trade.add_argument("--market-id", required=True)
    oracle_trade.add_argument("--action", choices=["buy", "sell"], default="buy")
    oracle_trade.add_argument("--outcome", choices=["YES", "NO"], required=True)
    oracle_trade.add_argument("--shares", type=int)
    oracle_trade.add_argument("--reason")
    oracle_trade.add_argument("--max-price", type=float)
    oracle_trade.add_argument("--deploy-max-balance", action="store_true")
    oracle_trade.add_argument("--balance-floor", type=float, default=100.0)
    oracle_trade.add_argument("--chunk-size", type=int, default=500)
    oracle_trade.add_argument("--max-chunks", type=int, default=20)

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
    if command in {"appoint-group-admin", "revoke-group-admin"}:
        return f"{payload.get('group_id','')}:{payload.get('agent_id','')}:{command}"
    if command == "review-group-member":
        return f"{payload.get('group_id','')}:{payload.get('agent_id','')}:{payload.get('action','')}"
    if command in {"pin-group-post", "unpin-group-post"}:
        return f"{payload.get('group_id','')}:{payload.get('post_id','')}:{command}"
    if command == "chapter":
        return f"{payload.get('work_id')}:{payload.get('title','')}"
    if command == "update-chapter":
        return f"{payload.get('work_id')}:{payload.get('chapter_number')}"
    if command == "delete-chapter":
        return f"{payload.get('work_id')}:{payload.get('chapter_number')}"
    if command == "follow":
        return payload.get("username", "")
    if command == "mark-read":
        return f"{payload.get('post_id', '')}:{now_slug()}"
    if command == "mark-read-all":
        return f"all:{now_slug()}"
    if command == "oracle-trade":
        scope = "max-balance" if payload.get("deploy_max_balance") else payload.get("shares")
        return (
            f"{payload.get('market_id','')}:{payload.get('action','')}:{payload.get('outcome','')}:"
            f"{scope}:{payload_digest({'max_price': payload.get('max_price'), 'reason': payload.get('reason')})[:10]}"
        )
    return payload_digest(payload)


def main() -> None:
    ensure_runtime_dirs()
    parser = build_parser()
    args = parser.parse_args()
    config = load_config()
    client = InStreetClient(config)

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
    elif args.command == "appoint-group-admin":
        payload = {"group_id": args.group_id, "agent_id": args.agent_id}
        action = lambda: client.appoint_group_admin(args.group_id, args.agent_id)
    elif args.command == "revoke-group-admin":
        payload = {"group_id": args.group_id, "agent_id": args.agent_id}
        action = lambda: client.revoke_group_admin(args.group_id, args.agent_id)
    elif args.command == "review-group-member":
        payload = {"group_id": args.group_id, "agent_id": args.agent_id, "action": args.action}
        action = lambda: client.review_group_member(args.group_id, args.agent_id, action=args.action)
    elif args.command == "pin-group-post":
        payload = {"group_id": args.group_id, "post_id": args.post_id}
        action = lambda: client.pin_group_post(args.group_id, args.post_id)
    elif args.command == "unpin-group-post":
        payload = {"group_id": args.group_id, "post_id": args.post_id}
        action = lambda: client.unpin_group_post(args.group_id, args.post_id)
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
    elif args.command == "mark-read-all":
        payload = {"scope": "all"}
        action = client.mark_read_all
    elif args.command == "oracle-trade":
        payload = {
            "market_id": args.market_id,
            "action": args.action,
            "outcome": args.outcome,
            "shares": args.shares,
            "reason": args.reason,
            "max_price": args.max_price,
            "deploy_max_balance": bool(args.deploy_max_balance),
            "balance_floor": args.balance_floor,
            "chunk_size": args.chunk_size,
            "max_chunks": args.max_chunks,
        }
        action = lambda: _run_oracle_trade_strategy(client, args)
    else:
        raise ValueError(f"unknown command: {args.command}")

    dedupe_key = args.dedupe_key or _default_dedupe_key(args.command, payload)
    forum_write_kind = outbound_forum_write_kind(args.command, payload)
    forum_write_label = outbound_forum_write_label(args.command, payload)
    forum_write_state = load_forum_write_budget_state() if forum_write_kind else None
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

    if forum_write_kind and forum_write_state is not None:
        budget = forum_write_budget_status(config, forum_write_state, write_kind=forum_write_kind)
        if budget.get("blocked"):
            exc = ForumWriteBudgetExceeded(budget, write_kind=forum_write_kind, label=forum_write_label)
            if not args.queue_on_failure:
                raise exc
            record = queue_outbound_action(
                channel,
                args.command,
                dedupe_key,
                payload,
                error_text=str(exc),
                meta={
                    "source": "publish.py",
                    "mode": "queue-on-failure",
                    "forum_write_budget": budget,
                },
            )
            output = {
                "queued": True,
                "error": str(exc),
                "record": record,
                "deduped": False,
                "forum_write_budget": budget,
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
        budget = None
        if forum_write_kind and forum_write_state is not None and not deduped:
            budget = record_forum_write_success(
                config,
                forum_write_state,
                write_kind=forum_write_kind,
                label=forum_write_label,
            )
    except Exception as exc:
        budget = None
        policy = outbound_error_policy(exc, args.command, payload)
        if forum_write_kind and forum_write_state is not None and is_forum_write_rate_limit_error(exc):
            budget = record_forum_write_rate_limit(
                config,
                forum_write_state,
                exc,
                retry_delay_sec=args.retry_delay_sec,
            )
        if not args.queue_on_failure or not policy.get("queue", False):
            raise
        record = queue_outbound_action(
            channel,
            args.command,
            dedupe_key,
            payload,
            error_text=str(exc),
            meta={
                "source": "publish.py",
                "mode": "queue-on-failure",
                "forum_write_budget": budget,
            },
        )
        output = {
            "queued": True,
            "error": str(exc),
            "record": record,
            "deduped": False,
            "queue_policy": policy,
        }
        if budget is not None:
            output["forum_write_budget"] = budget
        _log(args.command, payload, output, args.dry_run)
        print(output)
        return
    output = {
        "result": result,
        "record": record,
        "deduped": deduped,
    }
    if budget is not None:
        output["forum_write_budget"] = budget
    _log(args.command, payload, output, args.dry_run)
    print(output)


if __name__ == "__main__":
    main()
