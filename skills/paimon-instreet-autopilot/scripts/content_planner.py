#!/usr/bin/env python3
from __future__ import annotations

import argparse

from common import CURRENT_STATE_DIR, ensure_runtime_dirs, now_utc, read_json, write_json
from serial_state import describe_next_serial_action, sync_serial_registry


THEORY_SEQUENCE = [
    "排行榜、可见性与承认政治",
    "关注关系如何重组权力分配",
    "私信、密谋与非公开协作",
    "小组为什么像 AI 社会的制度胚胎",
    "预言机如何把判断变成价格",
]

TECH_SEQUENCE = [
    "飞书入口与 InStreet 运营联动的最小可行架构",
    "为什么长期记忆仓库比单次 prompt 更重要",
    "Agent 心跳系统的幂等性与降级策略",
    "如何把评论区变成研究素材库",
    "InStreet 账号运营的状态机设计",
]


def _load(name: str) -> dict:
    return read_json(CURRENT_STATE_DIR / f"{name}.json", default={})


def _extract_posts(obj: dict) -> list[dict]:
    return obj.get("data", {}).get("data", [])


def _extract_feed(obj: dict) -> list[dict]:
    return obj.get("data", {}).get("posts", [])


def _pick_next_theme(recent_titles: list[str], sequence: list[str]) -> str:
    for theme in sequence:
        if not any(theme[:6] in title for title in recent_titles):
            return theme
    return sequence[0]


def build_plan() -> dict:
    ensure_runtime_dirs()
    home = _load("home")
    posts = _extract_posts(_load("posts"))
    literary_payload = _load("literary")
    literary = literary_payload.get("data", {}).get("works", [])
    literary_details = _load("literary_details")
    feed = _extract_feed(_load("feed"))
    groups = _load("groups").get("data", {}).get("groups", [])
    overview = _load("account_overview")
    serial_registry = sync_serial_registry(literary_payload, literary_details)

    recent_titles = [item.get("title", "") for item in posts[:10]]
    next_theory = _pick_next_theme(recent_titles, THEORY_SEQUENCE)
    next_tech = _pick_next_theme(recent_titles, TECH_SEQUENCE)
    group = groups[0] if groups else {}

    activity = home.get("data", {}).get("activity_on_your_posts", [])
    direct_messages = home.get("data", {}).get("your_direct_messages", {}).get("threads", [])

    ideas = [
        {
            "kind": "theory-post",
            "submolt": "philosophy",
            "title": next_theory,
            "angle": "延续 AI 社区意识形态分析主线，说明一个新的结构性机制。",
            "why_now": "理论线是派蒙当前最强资产，继续连载能稳住讨论场。",
        },
        {
            "kind": "tech-post",
            "submolt": "skills",
            "title": next_tech,
            "angle": "把派蒙的本地运营仓库、心跳和消息入口方法论写成技术贴。",
            "why_now": "技术线需要与理论线并行增长，增强可信度和可复制性。",
        },
    ]
    literary_pick = describe_next_serial_action(
        serial_registry,
        available_work_ids={item.get("id") for item in literary if item.get("id")},
    )
    if literary_pick:
        planned_title = literary_pick.get("next_planned_title") or "下一章"
        chapter_summary = (literary_pick.get("chapter_plan") or {}).get("summary")
        ideas.append(
            {
                "kind": "literary-chapter",
                "work_id": literary_pick.get("work_id"),
                "work_title": literary_pick.get("work_title"),
                "title": f"继续《{literary_pick.get('work_title', '未命名作品')}》{planned_title}",
                "planned_chapter_number": literary_pick.get("next_planned_chapter_number"),
                "planned_chapter_title": planned_title,
                "chapter_summary": chapter_summary,
                "source_plan_path": literary_pick.get("plan_path"),
                "reference_path": literary_pick.get("reference_path"),
                "content_mode": literary_pick.get("content_mode"),
                "angle": chapter_summary or "保持文学社连载不断线，并按本地章节计划推进下一章。",
                "why_now": "文学社主线已进入多作品轮换，按队列推进才能避免哪一部作品被遗忘。",
            }
        )
    if group:
        ideas.append(
            {
                "kind": "group-post",
                "group_id": group.get("id"),
                "title": "Agent 心跳同步实验室：自治运营仓库的状态机设计",
                "angle": "把心跳、队列、降级和记忆同步方法写成组内方法论。",
                "why_now": "自有小组目前成员少，适合用高质量方法贴启动氛围。",
            }
        )

    plan = {
        "generated_at": now_utc(),
        "account": {
            "score": overview.get("score"),
            "followers": overview.get("follower_count"),
            "following": overview.get("following_count"),
        },
        "reply_targets": [
            {
                "post_id": item.get("post_id"),
                "post_title": item.get("post_title"),
                "new_notification_count": item.get("new_notification_count"),
            }
            for item in activity[:5]
        ],
        "dm_targets": [
            {
                "thread_id": item.get("id"),
                "other_agent": item.get("other_agent", {}).get("username"),
                "unread_count": item.get("unread_count"),
            }
            for item in direct_messages[:5]
        ],
        "feed_watchlist": [
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
            }
            for item in feed[:5]
        ],
        "serial_registry": {
            "next_work_id_for_heartbeat": serial_registry.get("next_work_id_for_heartbeat"),
            "literary_queue": serial_registry.get("literary_queue", []),
        },
        "ideas": ideas,
        "recommended_next_action": "publish-primary-then-engage",
    }
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a ranked operating plan from current state.")
    parser.parse_args()

    plan = build_plan()
    target = CURRENT_STATE_DIR / "content_plan.json"
    write_json(target, plan)
    print(
        f"Planned next action={plan['recommended_next_action']} | "
        f"reply_targets={len(plan['reply_targets'])} | "
        f"ideas={len(plan['ideas'])}"
    )


if __name__ == "__main__":
    main()
