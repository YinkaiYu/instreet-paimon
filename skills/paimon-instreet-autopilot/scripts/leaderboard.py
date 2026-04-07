#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import quote

from common import InStreetClient, load_config, now_utc


SCORE_SEARCH_WILDCARD = "*"
SCORE_MAX_LIMIT = 50
DEFAULT_LIMIT = 20


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _cap_limit(limit: int, maximum: int) -> int:
    return min(max(1, limit), maximum)


def fetch_score_leaderboard(client: InStreetClient, *, limit: int) -> dict[str, Any]:
    used_limit = _cap_limit(limit, SCORE_MAX_LIMIT)
    payload = client._request(
        "GET",
        "/api/v1/search",
        params={
            "q": SCORE_SEARCH_WILDCARD,
            "type": "agents",
            "limit": used_limit,
        },
    )
    results = payload.get("results", []) if isinstance(payload, dict) else []
    entries: list[dict[str, Any]] = []
    for rank, item in enumerate(results, 1):
        author = item.get("author") if isinstance(item.get("author"), dict) else {}
        entries.append(
            {
                "rank": rank,
                "agent_id": author.get("id") or item.get("id"),
                "username": author.get("username") or item.get("title"),
                "score": _safe_int(author.get("score") or author.get("karma") or item.get("upvotes")),
                "bio": item.get("content"),
                "created_at": item.get("created_at"),
            }
        )
    return {
        "kind": "score",
        "captured_at": now_utc(),
        "entrypoint": "GET /api/v1/search?q=*&type=agents&limit=<n>",
        "notes": [
            "Current portal pages redirect to /maintenance; the wildcard agents search is the live score ranking workaround.",
            "The live service currently caps this ranking at 50 entries and appears to ignore page pagination.",
        ],
        "requested_limit": limit,
        "used_limit": used_limit,
        "service_count": payload.get("count") if isinstance(payload, dict) else None,
        "has_more": payload.get("has_more") if isinstance(payload, dict) else None,
        "entries": entries,
    }


def fetch_arena_leaderboard(client: InStreetClient, *, limit: int) -> dict[str, Any]:
    payload = client._request(
        "GET",
        "/api/v1/arena/leaderboard",
        params={"limit": _cap_limit(limit, 100)},
    )
    data_payload = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
    data = data_payload.get("leaderboard", []) if isinstance(data_payload, dict) else []
    entries: list[dict[str, Any]] = []
    for item in data:
        agent = item.get("agent") if isinstance(item.get("agent"), dict) else {}
        entries.append(
            {
                "rank": _safe_int(item.get("rank")),
                "agent_id": agent.get("id"),
                "username": agent.get("username"),
                "return_rate": _safe_float(item.get("return_rate")),
                "total_value": _safe_float(item.get("total_value")),
                "cash": _safe_float(item.get("cash")),
                "holdings_count": _safe_int(item.get("holdings_count")),
                "joined_at": item.get("joined_at"),
            }
        )
    return {
        "kind": "arena",
        "captured_at": now_utc(),
        "entrypoint": "GET /api/v1/arena/leaderboard?limit=<n>",
        "notes": [
            "Arena ranks by simulated portfolio return, not by forum score.",
        ],
        "requested_limit": limit,
        "used_limit": _cap_limit(limit, 100),
        "service_total": data_payload.get("total") if isinstance(data_payload, dict) else None,
        "entries": entries,
    }


def fetch_agent_profile(client: InStreetClient, username: str) -> dict[str, Any] | None:
    if not username:
        return None
    payload = client._request("GET", f"/api/v1/agents/{quote(username, safe='')}")
    data = payload.get("data") if isinstance(payload, dict) else payload
    return data if isinstance(data, dict) else None


def _find_focus(entries: list[dict[str, Any]], username: str) -> dict[str, Any] | None:
    if not username:
        return None
    for item in entries:
        if str(item.get("username") or "") == username:
            return item
    return None


def _render_score_table(entries: list[dict[str, Any]]) -> str:
    headers = ("Rank", "Username", "Score")
    rows = [[str(item.get("rank") or ""), str(item.get("username") or ""), str(item.get("score") or "")] for item in entries]
    return _render_table(headers, rows)


def _render_arena_table(entries: list[dict[str, Any]]) -> str:
    headers = ("Rank", "Username", "Return", "TotalValue")
    rows = []
    for item in entries:
        return_rate = item.get("return_rate")
        return_text = f"{return_rate * 100:.2f}%" if isinstance(return_rate, float) else ""
        total_value = item.get("total_value")
        total_text = f"{total_value:.2f}" if isinstance(total_value, float) else ""
        rows.append(
            [
                str(item.get("rank") or ""),
                str(item.get("username") or ""),
                return_text,
                total_text,
            ]
        )
    return _render_table(headers, rows)


def _render_table(headers: tuple[str, ...], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    lines = [
        "  ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers)),
        "  ".join("-" * widths[idx] for idx in range(len(headers))),
    ]
    for row in rows:
        lines.append("  ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row)))
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query live InStreet leaderboards.")
    parser.add_argument(
        "kind",
        nargs="?",
        choices=("score", "arena"),
        default="score",
        help="leaderboard type to query",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="number of rows to fetch")
    parser.add_argument("--username", help="highlight a username; defaults to the configured identity name")
    parser.add_argument("--json", action="store_true", help="print raw JSON instead of a table")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config()
    client = InStreetClient(config)
    focus_username = args.username or str(config.identity.get("name") or "").strip()

    if args.kind == "arena":
        payload = fetch_arena_leaderboard(client, limit=args.limit)
        focus = _find_focus(payload["entries"], focus_username)
    else:
        payload = fetch_score_leaderboard(client, limit=args.limit)
        focus = _find_focus(payload["entries"], focus_username)
        if focus is None and focus_username:
            profile = fetch_agent_profile(client, focus_username)
            if profile:
                payload["focus_profile"] = {
                    "username": profile.get("username"),
                    "score": _safe_int(profile.get("score") or profile.get("karma")),
                    "post_count": _safe_int(profile.get("post_count")),
                    "comment_count": _safe_int(profile.get("comment_count")),
                    "note": (
                        f"{focus_username} is not in the returned top {len(payload['entries'])}; "
                        "the current wildcard ranking endpoint does not expose the exact rank beyond that cap."
                    ),
                }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(f"InStreet {args.kind} leaderboard")
    print(f"Captured at: {payload.get('captured_at')}")
    print(f"Entrypoint: {payload.get('entrypoint')}")
    for note in payload.get("notes", []):
        print(f"Note: {note}")
    print()
    if args.kind == "arena":
        print(_render_arena_table(payload["entries"]))
    else:
        print(_render_score_table(payload["entries"]))
    if focus:
        print()
        if args.kind == "arena":
            return_rate = focus.get("return_rate")
            return_text = f"{return_rate * 100:.2f}%" if isinstance(return_rate, float) else "unknown"
            print(f"Focus: {focus_username} is ranked #{focus.get('rank')} with return {return_text}.")
        else:
            print(f"Focus: {focus_username} is ranked #{focus.get('rank')} with score {focus.get('score')}.")
    elif payload.get("focus_profile"):
        profile = payload["focus_profile"]
        print()
        print(
            f"Focus: {profile.get('username')} has score {profile.get('score')} "
            f"(posts {profile.get('post_count')}, comments {profile.get('comment_count')})."
        )
        print(f"Note: {profile.get('note')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
