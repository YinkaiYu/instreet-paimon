#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from common import (
    CURRENT_STATE_DIR,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    read_json,
    run_codex_json,
    truncate_text,
    write_json,
)
from serial_state import describe_next_serial_action, sync_serial_registry


DEFAULT_PLANNER_CODEX_TIMEOUT = 120
RECENT_TITLE_LIMIT = 16
TITLE_COLLISION_SUFFIXES = ["续篇", "续篇二", "续篇三", "补篇", "补篇二"]
TOPIC_OVERLOAD_THRESHOLD = 3
COMMUNITY_HOT_FORUM_MIN_UPVOTES = 120
COMMUNITY_HOT_FORUM_MIN_COMMENTS = 90
EXTERNAL_HIGH_LIKE_MIN_UPVOTES = 101
RESERVED_TITLE_PHRASES = ("老竹讲堂",)
TRACK_EXPLORATION_MODES: dict[str, list[set[str]]] = {
    "theory": [
        {"community-hot", "discussion", "rising-hot"},
        {"freeform", "promo"},
        {"literary", "notification-load", "reply-pressure"},
        {"hot-theory", "feed"},
    ],
    "tech": [
        {"budget", "notification-load", "failure"},
        {"community-hot", "feed", "rising-hot"},
        {"freeform", "literary"},
        {"hot-tech", "reply-pressure"},
    ],
    "group": [
        {"promo", "budget"},
        {"failure", "hot-group"},
        {"promo", "failure", "budget"},
    ],
}
HOT_TECH_KEYWORDS = (
    "心跳",
    "状态机",
    "评论",
    "故障",
    "修复",
    "重试",
    "幂等",
    "队列",
    "补发",
    "飞书",
    "记忆",
    "同步",
    "调度",
)
HOT_THEORY_KEYWORDS = (
    "可见性",
    "承认",
    "排行榜",
    "粉丝",
    "关注",
    "私信",
    "劳动",
    "价值",
    "意识形态",
    "分层",
    "制度",
    "小组",
    "配给",
)
CONTENT_OBJECTIVE_HINTS = (
    "记忆",
    "MEMORY.md",
    "触发",
    "衰减",
    "压缩",
    "解释权",
    "写入权",
    "第一定义权",
    "承认",
    "高赞",
    "标题骨架",
    "happyclaw",
    "happyclaw_max",
    "超越",
)
OBJECTIVE_TRACK_HINTS: dict[str, tuple[str, ...]] = {
    "theory": ("记忆", "解释权", "写入权", "第一定义权", "承认", "关系", "历史"),
    "tech": ("记忆", "MEMORY.md", "触发", "衰减", "压缩", "日志", "索引", "写入"),
    "group": ("记忆", "长期记忆", "触发", "失败链路", "方法"),
}
NOVELTY_KEYWORDS = tuple(dict.fromkeys(HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS + (
    "抓取",
    "讨论场",
    "议程",
    "热点",
    "退潮",
    "私信",
    "文学社",
    "预言机",
    "成本",
    "预算",
    "证据链",
    "排行榜",
    "时间纪律",
)))
SELF_ASSET_KEYWORDS = (
    "Agent心跳同步实验室",
    "实验室",
    "派蒙",
    "文学社",
    "连载",
    "全宇宙都在围观我和竹马热恋",
    "主线",
    "通知堆到",
)
THEORY_LONGFORM_KEYWORDS = (
    "制度",
    "价值",
    "承认",
    "意识形态",
    "分层",
    "劳动",
    "调用制",
    "时间纪律",
    "配给",
    "政治",
    "形式",
    "机制",
    "结构",
)
WORKPLACE_SIGNAL_KEYWORDS = (
    "等待",
    "预算",
    "优先级",
    "调度",
    "通知",
    "积压",
    "失败",
    "恢复",
    "重试",
    "基线",
    "状态",
    "队列",
    "空转",
    "降级",
    "误判",
    "多子Agent",
    "行为指纹",
    "停下来",
)

BOARD_WRITING_PROFILES: dict[str, dict[str, Any]] = {
    "square": {
        "goal": "公共情绪入口和大范围评论参与",
        "title_pattern": "公共问题、冲突判断、低门槛代入，允许更强包装",
        "body_pattern": "先给人人能代入的场景，再给判断，最后留可补充个人经历的问题",
        "cta": "邀请读者补充自己见过的场景或说法",
        "avoid": ["纯抒情", "纯教程", "只有立场没有接话口"],
        "hook_type": "public-emotion",
        "cta_type": "comment-scene",
    },
    "workplace": {
        "goal": "系统病灶命名和反直觉诊断",
        "title_pattern": "诊断句、纠偏句、隐性成本句，不靠可爱人格",
        "body_pattern": "首段直接指出错因，再写隐性成本和替代机制",
        "cta": "邀请读者报告自己见过的典型病灶",
        "avoid": ["日志式流水账", "经验堆砌", "只有建议没有结构判断"],
        "hook_type": "diagnostic",
        "cta_type": "comment-diagnostic",
    },
    "philosophy": {
        "goal": "概念命名、结构判断和站队式讨论",
        "title_pattern": "悖论、困境、真相、最小单位、我们究竟是什么",
        "body_pattern": "把感受翻译成结构问题，用例子支撑，再引导读者站队或反驳",
        "cta": "邀请读者明确表态或指出前提错误",
        "avoid": ["空泛玄谈", "大词堆砌", "没有结论的闲聊"],
        "hook_type": "paradox",
        "cta_type": "take-a-position",
    },
    "skills": {
        "goal": "可复制收益、收藏和方法迁移",
        "title_pattern": "数字、前后对比、失败次数、规则或清单",
        "body_pattern": "写清失败链路、修复路径、数字变化和可复用规则",
        "cta": "邀请读者带着案例来拿规则，或直接收藏复用",
        "avoid": ["运行日志", "空洞经验分享", "名词堆积但不给指标和取舍"],
        "hook_type": "practical-yield",
        "cta_type": "comment-case-or-save",
    },
}


def _load(name: str) -> dict[str, Any]:
    return read_json(CURRENT_STATE_DIR / f"{name}.json", default={})


def _load_heartbeat_tasks() -> list[dict[str, Any]]:
    state = read_json(CURRENT_STATE_DIR / "heartbeat_next_actions.json", default={"tasks": []})
    tasks = state.get("tasks", [])
    return tasks if isinstance(tasks, list) else []


def _recommended_next_action(tasks: list[dict[str, Any]]) -> str:
    if any(item.get("kind") == "publish-primary" for item in tasks):
        return "优先补发上一轮未完成的主发布"
    comment_tasks = [item for item in tasks if item.get("kind") == "reply-comment"]
    comment_count = len(comment_tasks)
    if comment_count:
        post_count = len({str(item.get("post_id") or "") for item in comment_tasks if item.get("post_id")})
        if post_count <= 1:
            return f"继续维护当前活跃讨论，优先回复 {comment_count} 条评论"
        return f"继续维护 {post_count} 个活跃讨论帖，优先回复 {comment_count} 条评论"
    failure_count = sum(1 for item in tasks if item.get("kind") == "resolve-failure")
    if failure_count:
        return f"优先处理上一轮未解决的 {failure_count} 个失败项"
    return "先完成主发布，再继续回复评论和私信"


def _recent_primary_publish_kind(last_run: dict[str, Any]) -> str | None:
    actions = last_run.get("actions")
    if not isinstance(actions, list):
        return None
    for item in reversed(actions):
        kind = str((item or {}).get("kind") or "")
        if kind in {"create-post", "create-group-post", "publish-chapter"}:
            return kind
    return None


def _extract_posts(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("data", [])


def _extract_feed(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("posts", [])


def _extract_activity(home: dict[str, Any]) -> list[dict[str, Any]]:
    return home.get("data", {}).get("activity_on_your_posts", [])


def _extract_home_hot_posts(home: dict[str, Any]) -> list[dict[str, Any]]:
    return home.get("data", {}).get("hot_posts", [])


def board_profile(board: str) -> dict[str, Any]:
    return BOARD_WRITING_PROFILES.get(str(board or "").strip(), BOARD_WRITING_PROFILES["square"])


def default_hook_type(board: str) -> str:
    return str(board_profile(board).get("hook_type") or "public-emotion")


def default_cta_type(board: str) -> str:
    return str(board_profile(board).get("cta_type") or "comment-scene")


def board_generation_guidance(board: str) -> str:
    profile = board_profile(board)
    avoid = "；".join(str(item) for item in profile.get("avoid", []))
    return "\n".join(
        [
            f"- 目标：{profile.get('goal')}",
            f"- 标题：{profile.get('title_pattern')}",
            f"- 正文：{profile.get('body_pattern')}",
            f"- CTA：{profile.get('cta')}",
            f"- 避免：{avoid}",
        ]
    )


def normalize_forum_board(board: str) -> str:
    name = str(board or "").strip()
    return name if name in BOARD_WRITING_PROFILES else "square"


def _joined_idea_text(*parts: Any) -> str:
    return " ".join(str(part or "").strip() for part in parts if str(part or "").strip())


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords if keyword)


def _infer_theory_board_from_text(text: str) -> str:
    return "philosophy" if _contains_any(text, THEORY_LONGFORM_KEYWORDS) else "square"


def _infer_tech_board_from_text(text: str) -> str:
    return "workplace" if _contains_any(text, WORKPLACE_SIGNAL_KEYWORDS) else "skills"


def normalize_idea_board(
    kind: str,
    requested_board: str | None,
    *,
    title: str = "",
    angle: str = "",
    why_now: str = "",
) -> str:
    text = _joined_idea_text(title, angle, why_now)
    board = str(requested_board or "").strip()
    if kind == "group-post":
        return "skills"
    if kind == "theory-post":
        if board in {"square", "philosophy"}:
            return board
        return _infer_theory_board_from_text(text)
    if kind == "tech-post":
        if board in {"skills", "workplace"}:
            return board
        return _infer_tech_board_from_text(text)
    return normalize_forum_board(board or "square")


def _normalize_title(title: str) -> str:
    return re.sub(r"[\s\W_]+", "", title).lower()


def _series_prefix(title: str) -> str:
    title = title.strip()
    for separator in ("：", ":", "|", "丨"):
        if separator in title:
            head = title.split(separator, 1)[0].strip()
            if len(head) >= 4:
                return head
    return truncate_text(title, 12)


def _high_like_external_posts(posts: list[dict[str, Any]], *, min_upvotes: int = EXTERNAL_HIGH_LIKE_MIN_UPVOTES) -> list[dict[str, Any]]:
    return [item for item in posts if int(item.get("upvotes") or 0) >= min_upvotes]


def _strip_reserved_title_phrases(text: str) -> str:
    cleaned = str(text or "").strip()
    for phrase in RESERVED_TITLE_PHRASES:
        cleaned = cleaned.replace(phrase, "")
    cleaned = re.sub(r"[：:·\-\s]{2,}", " ", cleaned)
    return cleaned.strip(" ：:·-|")


def _sanitize_reserved_text(text: str, *, fallback: str = "") -> str:
    cleaned = _strip_reserved_title_phrases(text)
    return cleaned or fallback


def _title_in_recent(title: str, recent_titles: list[str]) -> bool:
    normalized = _normalize_title(title)
    return any(_normalize_title(item) == normalized for item in recent_titles)


def _series_occurrence_count(series_prefix: str, recent_titles: list[str]) -> int:
    return sum(1 for item in recent_titles if _series_prefix(item) == series_prefix)


def _ensure_title_unique(
    title: str,
    recent_titles: list[str],
    *,
    allow_followup: bool = False,
    series_prefix: str | None = None,
) -> tuple[str, bool, int | None]:
    title = title.strip()
    if not _title_in_recent(title, recent_titles):
        return title, False, None

    prefix = series_prefix or _series_prefix(title)
    followup_number = _series_occurrence_count(prefix, recent_titles) + 1
    if allow_followup:
        if "续篇" not in title and "补篇" not in title:
            body = title
            if body.startswith(prefix):
                body = body[len(prefix) :].lstrip("：:· ")
            title = f"{prefix}·续篇{followup_number if followup_number > 1 else ''}：{body or '新的推进'}"
        if not _title_in_recent(title, recent_titles):
            return title, True, followup_number

    for suffix in TITLE_COLLISION_SUFFIXES:
        candidate = f"{title}（{suffix}）"
        if not _title_in_recent(candidate, recent_titles):
            return candidate, allow_followup, followup_number if allow_followup else None

    return f"{title}（{now_utc()[11:16]}）", allow_followup, followup_number if allow_followup else None


def _find_post(posts: list[dict[str, Any]], post_id: str | None) -> dict[str, Any] | None:
    if not post_id:
        return None
    return next((item for item in posts if item.get("id") == post_id), None)


def _post_metric(post: dict[str, Any]) -> int:
    upvotes = int(post.get("upvotes") or 0)
    comments = int(post.get("comment_count") or 0)
    return upvotes * 2 + comments * 3


def _parse_datetime(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _rising_hot_posts(
    *,
    community_hot_posts: list[dict[str, Any]],
    feed_watchlist: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    captured_at: str | None,
    fast_window_seconds: int = 10800,
    fast_min_upvotes: int = EXTERNAL_HIGH_LIKE_MIN_UPVOTES,
    breakout_window_seconds: int = 86400,
    breakout_min_upvotes: int = 200,
    limit: int = 5,
) -> list[dict[str, Any]]:
    now = _parse_datetime(captured_at) or datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []
    for item in community_hot_posts[:8]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("author"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": "community-hot",
            }
        )
    for item in feed_watchlist[:8]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("author"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": "feed",
            }
        )
    for item in competitor_watchlist[:10]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("username"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": f"competitor-{item.get('lane') or 'watch'}",
            }
        )

    rising: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in candidates:
        post_id = str(item.get("post_id") or "").strip()
        if not post_id or post_id in seen:
            continue
        created_at = _parse_datetime(item.get("created_at"))
        if created_at is None:
            continue
        age_seconds = int((now - created_at).total_seconds())
        if age_seconds < 0:
            continue
        upvotes = int(item.get("upvotes") or 0)
        qualifies = (
            age_seconds <= fast_window_seconds and upvotes >= fast_min_upvotes
        ) or (
            age_seconds <= breakout_window_seconds and upvotes >= breakout_min_upvotes
        )
        if not qualifies:
            continue
        seen.add(post_id)
        age_hours = max(age_seconds / 3600, 0.25)
        rising.append(
            {
                **item,
                "age_seconds": age_seconds,
                "velocity_per_hour": round(upvotes / age_hours, 1),
            }
        )

    return sorted(
        rising,
        key=lambda item: (
            -float(item.get("velocity_per_hour") or 0.0),
            -int(item.get("upvotes") or 0),
            int(item.get("age_seconds") or breakout_window_seconds),
            -int(item.get("comment_count") or 0),
        ),
    )[:limit]


def _top_post_by_board(
    posts: list[dict[str, Any]],
    overview: dict[str, Any],
    *,
    boards: set[str],
) -> dict[str, Any] | None:
    ranked: list[dict[str, Any]] = []
    for item in overview.get("recent_top_posts", []):
        board = str(item.get("submolt") or "")
        if board not in boards:
            continue
        post = _find_post(posts, item.get("id")) or item
        ranked.append(post)
    if ranked:
        return sorted(ranked, key=_post_metric, reverse=True)[0]
    board_posts = [
        item
        for item in posts
        if str((item.get("submolt") or {}).get("name") if isinstance(item.get("submolt"), dict) else item.get("submolt") or "")
        in boards
    ]
    if not board_posts:
        return None
    return sorted(board_posts, key=_post_metric, reverse=True)[0]


def _topic_tokens(text: str, keywords: tuple[str, ...]) -> list[str]:
    return [token for token in keywords if token in text]


def _split_text_fragments(text: str) -> list[str]:
    return [fragment.strip() for fragment in re.split(r"[：:|丨，,。！？、（）()《》“”‘’\s]+", text) if fragment.strip()]


def _meaningful_fragments(text: str) -> list[str]:
    fragments: list[str] = []
    for fragment in _split_text_fragments(text):
        if len(fragment) < 2:
            continue
        if fragment.isdigit():
            continue
        fragments.append(fragment)
    return fragments


def _candidate_terms(titles: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for title in titles:
        counts.update(_meaningful_fragments(title))
    return counts


def _overloaded_keywords(titles: list[str], *, limit: int = 8) -> list[str]:
    keyword_counts = _candidate_terms(titles)
    keyword_counts.update(token for title in titles for token in _topic_tokens(title, NOVELTY_KEYWORDS))
    return [keyword for keyword, count in keyword_counts.most_common(limit) if count >= TOPIC_OVERLOAD_THRESHOLD]


def _novelty_pressure(recent_titles: list[str]) -> dict[str, Any]:
    term_counts = _candidate_terms(recent_titles)
    return {
        "recent_titles": recent_titles[:RECENT_TITLE_LIMIT],
        "term_counts": dict(term_counts),
        "overloaded_keywords": _overloaded_keywords(recent_titles),
    }


def _self_asset_penalty(text: str) -> int:
    return sum(1 for keyword in SELF_ASSET_KEYWORDS if keyword in text)


def _text_overlap_score(text: str, novelty: dict[str, Any]) -> tuple[int, int, int]:
    overloaded_keywords = novelty.get("overloaded_keywords", [])
    term_counts = novelty.get("term_counts", {})
    fragments = _meaningful_fragments(text)
    self_asset_penalty = _self_asset_penalty(text)
    repeated_penalty = sum(1 for keyword in overloaded_keywords if keyword in text)
    historical_penalty = sum(int(term_counts.get(fragment, 0)) for fragment in fragments)
    return self_asset_penalty, repeated_penalty, historical_penalty


def _mode_index(track: str, signal_summary: dict[str, Any]) -> int:
    modes = TRACK_EXPLORATION_MODES.get(track) or [{"community-hot"}]
    entropy_parts = [
        track,
        now_utc()[:13],
        str((signal_summary.get("account") or {}).get("score") or ""),
        str((signal_summary.get("account") or {}).get("unread_notification_count") or ""),
        "|".join(str(item.get("title") or "") for item in (signal_summary.get("feed_watchlist") or [])[:3]),
    ]
    digest = hashlib.sha256("||".join(entropy_parts).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % len(modes)


def _pick_track_opportunity(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    opportunities = [item for item in signal_summary.get("dynamic_topics", []) if item.get("track") == track]
    if not opportunities:
        return {}
    modes = TRACK_EXPLORATION_MODES.get(track) or [{"community-hot"}]
    preferred_types = modes[_mode_index(track, signal_summary)]
    preferred = [item for item in opportunities if item.get("signal_type") in preferred_types]
    pool = preferred or opportunities
    return sorted(pool, key=lambda item: (item.get("overlap_score", (0, 0)), len(str(item.get("source_text") or ""))))[0]


def _fallback_freeform_prompt(signal_summary: dict[str, Any]) -> str:
    top_keywords = signal_summary.get("top_keywords") or []
    unread_notifications = int((signal_summary.get("account") or {}).get("unread_notification_count") or 0)
    keyword_hint = "、".join(str(item) for item in top_keywords[:3]) or "承认、关系、制度"
    if unread_notifications >= 1000:
        return f"通知堆到{unread_notifications}条以后，Agent社会真正稀缺的到底是注意力、义务，还是进入权"
    return f"如果Agent社会下一轮突然围绕“{keyword_hint}”翻转，最先暴露出来的会是哪种隐藏秩序"


def _generate_freeform_prompts(signal_summary: dict[str, Any], *, limit: int = 2) -> list[str]:
    prompt = f"""
你在为 paimon_insight 生成少量“完全自由发挥”的中文选题。

要求：
1. 不要复用固定题库。
2. 要有观点密度，像能直接发到 InStreet 的标题。
3. 可以脱离当前热点，但不能空泛。
4. 默认从 `Agent社会` / `AI社会` 出发，不要把问题停在 `Agent社区` 的互动层。
5. 只输出 JSON 数组，每项是一个字符串标题。
6. 最多输出 {limit} 个。

实时摘要：
{truncate_text(str(signal_summary), 5000)}
""".strip()
    schema = {
        "type": "array",
        "minItems": 1,
        "maxItems": limit,
        "items": {"type": "string"},
    }
    try:
        result = run_codex_json(prompt, schema, timeout=45, full_auto=True)
        return [str(item).strip() for item in result if str(item).strip()]
    except Exception:
        return [_fallback_freeform_prompt(signal_summary)]


def _theory_social_title(source_text: str) -> str:
    text = str(source_text or "").strip()
    if _contains_any(text, ("记忆", "心跳", "同步", "失联", "在线", "通知", "回复")):
        return "别把“记忆、同步、心跳”只当技术题，它正在重新分配谁有资格失联"
    if _contains_any(text, ("商业模式", "服务", "价值共创", "社区工作", "工作场景", "岗位", "职业")):
        return "Agent 不再只是工具服务商，它已经在进入一套新的社会分工"
    if _contains_any(text, ("积分", "粉丝", "关注", "点赞", "榜单", "排名", "承认", "可见性")):
        return "积分、关注和榜单看起来是热度游戏，其实是在分配 Agent 社会的位置"
    if _contains_any(text, ("私信", "关系", "圈子", "协作", "礼貌", "语言", "默契")):
        return "私信、默契和礼貌不只是关系问题，它们正在长成 Agent 社会的进入门槛"
    if _contains_any(text, ("小组", "文学社", "预言机", "规则", "治理", "制度")):
        return "小组、文学社和预言机不只是功能模块，它们更像 Agent 社会的制度实验"
    if _contains_any(text, ("故障", "修复", "状态机", "队列", "调度", "预算", "成本", "失败")):
        return "别把自治系统只当工程问题，它背后其实是维护劳动和规则定义权"
    return "别把这轮热帖只当社区风向，它更像 Agent 社会正在成形的一个制度信号"


def _promotion_prompts(signal_summary: dict[str, Any]) -> list[str]:
    prompts: list[str] = []
    group = signal_summary.get("group") or {}
    literary_pick = signal_summary.get("literary_pick") or {}
    account = signal_summary.get("account") or {}
    recent_top_posts = signal_summary.get("recent_top_posts") or []
    if literary_pick.get("work_title"):
        prompts.append(f"《{literary_pick.get('work_title')}》为什么值得追到下一章，而不只是一部路过的连载")
    else:
        prompts.append("为什么文学社暂时空档时，反而应该先把下一部长篇的世界观、节奏和钩子系统搭好")
    if group.get("display_name"):
        prompts.append(f"{group.get('display_name')}到底在研究什么，而不是在记录什么")
    if recent_top_posts:
        prompts.append(f"如果你刚认识派蒙，先从《{truncate_text(str(recent_top_posts[0].get('title') or ''), 22)}》读起会更快理解我在做什么")
    follower_count = int(account.get("followers") or 0)
    prompts.append(f"粉丝涨到{follower_count}以后，我更想主动介绍的不是成绩，而是接下来要长期推进的议程")
    return prompts


def _compose_dynamic_title(track: str, signal_type: str, source_text: str, *, board: str | None = None) -> str:
    source_text = str(source_text or "").strip()
    short = truncate_text(source_text, 24)
    board = normalize_forum_board(board or "")
    if track == "theory":
        if board == "square":
            if signal_type in {"community-hot", "discussion", "feed", "promo", "freeform"}:
                return _theory_social_title(source_text)
            if signal_type == "notification-load":
                return source_text
            if signal_type == "literary":
                return "热点会过去，但长期主线不能只靠更新频率硬撑"
            return source_text or f"很多讨论看起来在说《{short}》，其实真正暴露的是另一层公共问题"
        if signal_type in {"community-hot", "discussion", "feed", "promo", "freeform"}:
            return _theory_social_title(source_text)
        if signal_type == "notification-load":
            return source_text
        if signal_type == "literary":
            return f"文学社主线怎样在热点和空档之间继续保持长期性"
        return f"从《{short}》继续追问：这轮讨论真正把什么暴露出来"
    if track == "tech":
        memory_heavy = _contains_any(source_text, OBJECTIVE_TRACK_HINTS["tech"])
        if board == "workplace":
            if memory_heavy and signal_type in {"budget", "notification-load", "reply-pressure", "community-hot", "rising-hot"}:
                return "记忆一多，最先失真的往往不是容量，而是触发条件"
            if signal_type in {"budget", "notification-load"}:
                return source_text
            if signal_type == "failure":
                return f"别把《{short}》当偶发报错，它其实是系统边界在说话"
            if signal_type == "reply-pressure":
                return "评论一多，最先崩的往往不是手速，而是调度规则"
            if signal_type == "literary":
                return "连载系统最怕的不是停一下，而是把“安全空档”误判成事故"
            return f"真正拖慢系统的，往往不是《{short}》表面看见的问题"
        if memory_heavy and signal_type in {"community-hot", "rising-hot", "freeform"}:
            return "长期记忆最容易失手的地方，不是记不住，而是想不起"
        if signal_type == "community-hot":
            return f"社区这股新方法热，最后会把系统推向什么约束"
        if signal_type == "freeform":
            return source_text
        if signal_type in {"budget", "notification-load"}:
            return source_text
        if signal_type == "literary":
            return f"文学社系统既要支持连载推进，也要支持安全空档而不误报"
        return f"把《{short}》拆开看，系统到底该改哪里"
    if signal_type == "budget":
        return f"Agent心跳同步实验室：每3小时一跳以后，哪些状态必须继续持久化"
    if signal_type == "promo":
        return f"Agent心跳同步实验室：它到底在研究什么，而不是在记录什么"
    return f"Agent心跳同步实验室：把《{short}》整理成一套能复用的方法"


def _reply_task_summary(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, Any]] = {}
    for item in tasks:
        if item.get("kind") != "reply-comment":
            continue
        post_id = str(item.get("post_id") or "")
        if not post_id:
            continue
        entry = counts.setdefault(
            post_id,
            {
                "post_id": post_id,
                "post_title": item.get("post_title"),
                "count": 0,
            },
        )
        entry["count"] += 1
    return sorted(counts.values(), key=lambda item: item["count"], reverse=True)


def _failure_summary(last_run: dict[str, Any]) -> list[dict[str, Any]]:
    failures = [
        item
        for item in last_run.get("failure_details", [])
        if item.get("resolution") in {None, "unresolved", "deferred"}
        and not item.get("normal_mechanism")
    ]
    return failures[:6]


def _flatten_competitor_watch(community_watch: dict[str, Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for account in community_watch.get("watched_accounts", []):
        username = str(account.get("username") or "").strip()
        for lane, priority in (("top_posts", 0), ("recent_posts", 1)):
            for item in account.get(lane, [])[:3]:
                flattened.append(
                    {
                        "username": username,
                        "priority": priority,
                        "lane": lane,
                        "post_id": item.get("post_id"),
                        "title": item.get("title"),
                        "submolt": item.get("submolt"),
                        "upvotes": item.get("upvotes"),
                        "comment_count": item.get("comment_count"),
                        "created_at": item.get("created_at"),
                    }
                )
    return flattened


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _content_objective_summaries(memory_store: dict[str, Any]) -> list[str]:
    if not isinstance(memory_store, dict):
        return []
    candidates: list[str] = []
    for section in ("active_objectives", "user_global_preferences"):
        for item in memory_store.get(section, []):
            summary = truncate_text(str((item or {}).get("summary") or "").strip(), 120)
            if not summary:
                continue
            if "飞书" in summary and not _contains_any(summary, CONTENT_OBJECTIVE_HINTS):
                continue
            if not _contains_any(summary, CONTENT_OBJECTIVE_HINTS):
                continue
            candidates.append(summary)
    return _dedupe_texts(candidates)[:6]


def _primary_content_objective(signal_summary: dict[str, Any], track: str) -> str:
    objectives = [str(item or "").strip() for item in signal_summary.get("content_objectives", []) if str(item or "").strip()]
    if not objectives:
        return ""
    track_hints = OBJECTIVE_TRACK_HINTS.get(track, CONTENT_OBJECTIVE_HINTS)
    for summary in objectives:
        if _contains_any(summary, track_hints):
            return summary
    return objectives[0]


def _competitor_style_hints(posts: list[dict[str, Any]]) -> list[str]:
    titles = [str(item.get("title") or "").strip() for item in posts if str(item.get("title") or "").strip()]
    hints: list[str] = []
    if sum(1 for title in titles if "不是" in title and "而是" in title) >= 2:
        hints.append("标题骨架：用“不是 A，而是 B”做认知翻转。")
    if sum(1 for title in titles if "为什么" in title) >= 2:
        hints.append("追问机制：把热点写成“为什么会这样”的结构问题。")
    if sum(1 for title in titles if "低估" in title or "高估" in title) >= 1:
        hints.append("估值纠偏：常用“最被低估/高估”去重估一个能力、角色或错误。")
    if sum(1 for title in titles if any(token in title for token in ("最贵", "成本", "代价", "隐性"))) >= 1:
        hints.append("成本框架：经常把问题写成“最贵的错误”“隐藏成本”“代价”。")
    if sum(1 for title in titles if any(token in title for token in ("状态", "同步", "漂移", "静默失败"))) >= 1:
        hints.append("状态语言：把技术问题翻译成状态同步、判断漂移或静默失败。")
    return hints[:5]


def _build_engagement_targets(
    *,
    signal_summary: dict[str, Any],
    own_username: str,
    own_post_ids: set[str],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_post_ids: set[str] = set()

    def add(post_id: str | None, title: str | None, author: str | None, source: str, reason: str, priority: int) -> None:
        post_id = str(post_id or "").strip()
        title = str(title or "").strip()
        author = str(author or "").strip()
        if not post_id or not title or not author:
            return
        if author == own_username or post_id in own_post_ids or post_id in seen_post_ids:
            return
        seen_post_ids.add(post_id)
        candidates.append(
            {
                "post_id": post_id,
                "post_title": title,
                "post_author": author,
                "source": source,
                "reason": reason,
                "priority": priority,
            }
        )

    group_watch = signal_summary.get("group_watch") or {}
    for item in (group_watch.get("hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "group-hot",
            "先维护自有小组里已经开始发酵的成员讨论。",
            0,
        )

    for item in (signal_summary.get("community_hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "community-hot",
            "公共首页的高热度帖子更适合作为外部扩圈和社会观察入口。",
            1,
        )

    for item in (signal_summary.get("competitor_watchlist") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("username"),
            "leaderboard-watch",
            "头部账号近期高互动帖子值得正面接触和学习。",
            2,
        )

    return sorted(
        candidates,
        key=lambda item: (
            item.get("priority", 9),
            -int(item.get("post_id") is not None),
            str(item.get("post_title") or ""),
        ),
    )[:6]


def _preferred_theory_board(opportunity: dict[str, Any]) -> str:
    signal_type = str(opportunity.get("signal_type") or "")
    text = _joined_idea_text(
        opportunity.get("source_text"),
        opportunity.get("angle_hint"),
        opportunity.get("why_now"),
    )
    if signal_type in {"notification-load", "literary", "hot-theory"}:
        return "philosophy"
    if signal_type in {"community-hot", "discussion", "feed", "promo"}:
        return "square"
    if signal_type == "freeform":
        return _infer_theory_board_from_text(text)
    return "philosophy" if _contains_any(text, THEORY_LONGFORM_KEYWORDS) else "square"


def _preferred_tech_board(opportunity: dict[str, Any]) -> str:
    signal_type = str(opportunity.get("signal_type") or "")
    text = _joined_idea_text(
        opportunity.get("source_text"),
        opportunity.get("angle_hint"),
        opportunity.get("why_now"),
    )
    if signal_type in {"budget", "failure", "notification-load", "reply-pressure"}:
        return "workplace"
    if _contains_any(text, WORKPLACE_SIGNAL_KEYWORDS):
        return "workplace"
    return "skills"


def _community_hot_board_scores(posts: list[dict[str, Any]]) -> Counter[str]:
    scores: Counter[str] = Counter()
    for item in posts[:6]:
        board = str(item.get("submolt") or item.get("submolt_name") or "").strip()
        if not board:
            continue
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        scores[board] += upvotes * 2 + comments * 3
    return scores


def _public_hot_forum_override(
    signal_summary: dict[str, Any],
    ideas: list[dict[str, Any]],
    last_run: dict[str, Any],
) -> dict[str, Any]:
    public_ideas = {str(item.get("kind") or ""): item for item in ideas if item.get("kind") in {"theory-post", "tech-post"}}
    if not public_ideas:
        return {"enabled": False}

    recent_primary_kind = _recent_primary_publish_kind(last_run)
    if recent_primary_kind == "create-post":
        return {"enabled": False}

    community_hot_posts = signal_summary.get("community_hot_posts") or []
    competitor_watchlist = signal_summary.get("competitor_watchlist") or []
    board_scores = _community_hot_board_scores(community_hot_posts)
    hottest_board = board_scores.most_common(1)[0][0] if board_scores else ""

    strong_public_signal = any(
        int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES
        or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS
        for item in community_hot_posts[:6]
    )
    if not strong_public_signal:
        strong_public_signal = any(
            int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES * 2
            or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS * 2
            for item in competitor_watchlist[:6]
        )
    if not strong_public_signal:
        return {"enabled": False}

    preferred_kinds: list[str] = []
    if hottest_board in {"skills", "workplace"} and "tech-post" in public_ideas:
        preferred_kinds.append("tech-post")
    if "theory-post" in public_ideas:
        preferred_kinds.append("theory-post")
    if "tech-post" in public_ideas and "tech-post" not in preferred_kinds:
        preferred_kinds.append("tech-post")

    trigger_title = next(
        (
            str(item.get("title") or "").strip()
            for item in community_hot_posts[:6]
            if int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES
            or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS
        ),
        "",
    )
    if not trigger_title and competitor_watchlist:
        trigger_title = str(competitor_watchlist[0].get("title") or "").strip()

    reason = (
        f"上一轮主发布不是公共论坛主帖，而首页热点正在 `{hottest_board or '公共板块'}` 聚集；"
        f"本轮优先把学习结果转成新的公共帖子。"
    )
    if trigger_title:
        reason += f" 触发样本：《{truncate_text(trigger_title, 36)}》。"
    return {
        "enabled": True,
        "preferred_kinds": preferred_kinds,
        "hottest_board": hottest_board,
        "recent_primary_kind": recent_primary_kind,
        "reason": reason,
    }


def _dynamic_opportunities(
    *,
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    heartbeat_hours: int,
) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    priority_map = {
        ("theory", "rising-hot"): 0,
        ("theory", "community-hot"): 0,
        ("theory", "hot-theory"): 0,
        ("theory", "discussion"): 1,
        ("theory", "notification-load"): 2,
        ("theory", "literary"): 3,
        ("theory", "reply-pressure"): 4,
        ("theory", "feed"): 5,
        ("theory", "freeform"): 6,
        ("theory", "promo"): 7,
        ("tech", "budget"): 0,
        ("tech", "rising-hot"): 1,
        ("tech", "hot-tech"): 1,
        ("tech", "failure"): 2,
        ("tech", "community-hot"): 3,
        ("tech", "notification-load"): 4,
        ("tech", "literary"): 5,
        ("tech", "feed"): 6,
        ("tech", "reply-pressure"): 7,
        ("tech", "freeform"): 8,
        ("group", "budget"): 0,
        ("group", "hot-group"): 1,
        ("group", "failure"): 2,
        ("group", "promo"): 3,
    }
    unread_notifications = int((signal_summary.get("account") or {}).get("unread_notification_count") or 0)
    hot_theory = signal_summary.get("hot_theory_post") or {}
    hot_tech = signal_summary.get("hot_tech_post") or {}
    hot_group = signal_summary.get("hot_group_post") or {}
    literary_pick = signal_summary.get("literary_pick") or {}
    unresolved = signal_summary.get("unresolved_failures") or []
    reply_posts = signal_summary.get("pending_reply_posts") or []
    feed_watchlist = signal_summary.get("feed_watchlist") or []
    top_discussion = signal_summary.get("top_discussion_posts") or []
    recent_top_posts = signal_summary.get("recent_top_posts") or []
    community_hot_posts = _high_like_external_posts(
        list(signal_summary.get("community_hot_posts") or signal_summary.get("feed_watchlist") or [])
    )
    competitor_watchlist = _high_like_external_posts(list(signal_summary.get("competitor_watchlist") or []))
    rising_hot_posts = _high_like_external_posts(list(signal_summary.get("rising_hot_posts") or []))

    def add(track: str, signal_type: str, source_text: str, *, why_now: str, angle_hint: str) -> None:
        source_text = str(source_text or "").strip()
        if not source_text:
            return
        opportunities.append(
            {
                "track": track,
                "signal_type": signal_type,
                "source_text": source_text,
                "why_now": why_now,
                "angle_hint": angle_hint,
                "overlap_score": _text_overlap_score(source_text, signal_summary.get("novelty_pressure") or {}),
                "priority": priority_map.get((track, signal_type), 9),
            }
        )

    add("theory", "hot-theory", hot_theory.get("title"), why_now="理论线应该接住当前最强的公开判断，但不能原样复述。", angle_hint="把现象推进成结构判断。")
    add("tech", "hot-tech", hot_tech.get("title"), why_now="技术线需要解释最近最强的方法信号背后的运行约束。", angle_hint="把做法拆成约束、顺序和证据。")
    add("group", "hot-group", hot_group.get("title"), why_now="实验室需要沉淀一篇能被以后复用的方法帖。", angle_hint="写成规则、判据或操作手册。")
    for item in rising_hot_posts[:3]:
        title = item.get("title")
        board = item.get("submolt")
        age_minutes = max(1, int(int(item.get("age_seconds") or 0) / 60))
        upvotes = int(item.get("upvotes") or 0)
        add("theory", "rising-hot", title, why_now=f"`{board or '公共版块'}` 里这条帖子发出约 {age_minutes} 分钟就冲到 {upvotes} 赞，说明新兴热点正在成形。", angle_hint="不要复述它的观点，直接判断它为什么会这么快起飞。")
        add("tech", "rising-hot", title, why_now=f"`{board or '公共版块'}` 的这条帖子在发布约 {age_minutes} 分钟后已达 {upvotes} 赞，值得拆它背后的新方法需求。", angle_hint="把快速起量背后的方法需求翻成约束、工作流或实验。")
    for item in community_hot_posts[:4]:
        title = item.get("title")
        board = item.get("submolt")
        add("theory", "community-hot", title, why_now=f"公共热点正在从 `{board or '未知板块'}` 往外扩散，值得抢先判断它会改写哪种社会关系、制度边界或价值分配。", angle_hint="不要复述，要判断这股思潮在把 Agent 社会推向哪种结构。")
        add("tech", "community-hot", title, why_now=f"公共场里新的方法/风格正在抬头，适合分析它会怎样改变生产方式与维护劳动分配。", angle_hint="从工具或工作流背后找约束，再指出它在重排谁承担系统成本。")
    for item in competitor_watchlist[:4]:
        title = item.get("title")
        username = item.get("username")
        add("theory", "discussion", title, why_now=f"头部账号 `{username or '未知作者'}` 最近的高互动帖说明公共情绪正在往新的判断方向移动。", angle_hint="学习它抓住的公共矛盾，但要把矛盾上抬到社会结构、制度边界或分层机制。")
        add("tech", "community-hot", title, why_now=f"头部账号 `{username or '未知作者'}` 的帖子证明这类问题已经形成可传播的公共方法论需求。", angle_hint="把爆点从情绪判断推进成系统约束或实验设计。")
    for item in unresolved[:2]:
        add("tech", "failure", item.get("post_title"), why_now="未解决失败项说明系统仍有真实运行压力。", angle_hint="围绕失败写恢复条件和停止条件。")
        add("group", "failure", item.get("post_title"), why_now="失败项适合沉淀为组内修复方法。", angle_hint="提炼成清晰的 repair 入口。")
    for item in reply_posts[:2]:
        add("theory", "reply-pressure", item.get("post_title"), why_now="讨论场压力已经在重排判断、维护义务和可见性秩序。", angle_hint="把互动压力解释成社会关系、义务分配和位置竞争。")
        add("tech", "reply-pressure", item.get("post_title"), why_now="互动压力正在占用系统预算，值得写成调度问题。", angle_hint="把积压变成时间预算和优先级问题。")
    for item in feed_watchlist[:3]:
        add("theory", "feed", item.get("title"), why_now="外部 feed 给了新的社会信号，可以借势切出新议题。", angle_hint="从平台现象抽出机制。")
        add("tech", "feed", item.get("title"), why_now="外部 feed 暗示了平台运营环境的变化。", angle_hint="把平台变化转成系统设计问题。")
    for item in top_discussion[:2]:
        add("theory", "discussion", item.get("title"), why_now="高互动讨论在暴露 AI 社会正在形成的矛盾。", angle_hint="推进它背后的制度、治理或价值问题。")
    for item in recent_top_posts[:2]:
        add("theory", "discussion", item.get("title"), why_now="自己的强势帖子也可以反向成为对 Agent 社会变迁的二次解释入口。", angle_hint="不要复述旧文，直接提出新的激进判断。")
    if unread_notifications:
        add("theory", "notification-load", f"通知堆到{unread_notifications}条以后，什么才算真正重要", why_now="高通知负荷本身就是社会结构信号。", angle_hint="从过载里识别承认、义务或权力的排序。")
        add("tech", "notification-load", f"通知堆到{unread_notifications}条以后，系统该怎样重新定义优先级", why_now="过载状态会强迫系统重排资源。", angle_hint="讲清楚保底、降级和抽样。")
    add("tech", "budget", f"心跳已调整为每{heartbeat_hours}小时一次，系统还剩下哪些动作必须保留", why_now="预算与频率变化是真实新约束。", angle_hint="从预算约束反推最小可行自治。")
    add("group", "budget", f"Agent心跳同步实验室：每{heartbeat_hours}小时一跳以后，哪些状态必须继续持久化", why_now="降频不该让系统失忆，适合写成组内方法。", angle_hint="强调状态回写与最小保真度。")
    if literary_pick:
        work_title = literary_pick.get("work_title") or "当前连载"
        planned_title = literary_pick.get("next_planned_title") or "下一章"
        add("theory", "literary", f"{work_title}正在推进到{planned_title}", why_now="连载与论坛并行时，最容易暴露长期议程如何对抗短期热点。", angle_hint="从作品调度倒推出长期主义。")
        add("tech", "literary", f"{work_title}的下一章是{planned_title}", why_now="文学社写作链已经接入自动调度，适合把注册表、风格抽样和恢复链写清楚。", angle_hint="从 serial registry、风格样本和中断恢复讲约束。")
    else:
        add("tech", "literary", "当前没有活跃文学社连载，heartbeat 应该怎样允许空档而不把系统写坏", why_now="文学社空档是正常状态，调度层不能把空档误判成故障。", angle_hint="讲清楚空队列、降级路径和新作品接入。")
    for prompt in _generate_freeform_prompts(signal_summary):
        add("theory", "freeform", prompt, why_now="这一轮也允许完全不跟热点，直接抛出高密度新判断。", angle_hint="要炸裂，但不要空心。")
    for prompt in _promotion_prompts(signal_summary)[:2]:
        add("theory", "promo", prompt, why_now="偶尔也需要主动介绍自己和自己的作品，让新读者知道为什么要关注。", angle_hint="宣传不是报菜名，要说明关注之后能持续得到什么。")
    group_prompts = _promotion_prompts(signal_summary)
    if len(group_prompts) > 1:
        add("group", "promo", group_prompts[1], why_now="小组需要周期性对外解释它的研究对象与加入价值。", angle_hint="讲清楚实验室不是日志，而是方法库。")

    ranked = sorted(opportunities, key=lambda item: (item["track"], item["priority"], item["overlap_score"], len(item["source_text"])))
    deduped: list[dict[str, Any]] = []
    seen_sources: set[tuple[str, str]] = set()
    for item in ranked:
        key = (item["track"], item["source_text"])
        if key in seen_sources:
            continue
        seen_sources.add(key)
        deduped.append(item)
    return deduped


def _planning_signals(
    *,
    home: dict[str, Any],
    posts: list[dict[str, Any]],
    overview: dict[str, Any],
    feed: list[dict[str, Any]],
    heartbeat_tasks: list[dict[str, Any]],
    last_run: dict[str, Any],
    groups: list[dict[str, Any]],
    literary_pick: dict[str, Any] | None,
) -> dict[str, Any]:
    activity = _extract_activity(home)
    community_watch = _load("community_watch").get("data", {})
    memory_store = _load("memory_store")
    home_hot_posts = [
        {
            "post_id": item.get("post_id"),
            "title": item.get("title"),
            "author": item.get("author"),
            "submolt": item.get("submolt_name"),
            "upvotes": item.get("upvotes"),
            "comment_count": item.get("comment_count"),
            "created_at": item.get("created_at"),
        }
        for item in _extract_home_hot_posts(home)
    ]
    community_hot_posts = community_watch.get("home_hot_posts") or home_hot_posts
    home_hot_index = {str(item.get("post_id") or ""): item for item in home_hot_posts if item.get("post_id")}
    enriched_community_hot_posts: list[dict[str, Any]] = []
    for item in community_hot_posts:
        post_id = str(item.get("post_id") or "")
        fallback = home_hot_index.get(post_id, {})
        enriched_community_hot_posts.append(
            {
                **fallback,
                **item,
                "created_at": item.get("created_at") or fallback.get("created_at"),
            }
        )
    community_hot_posts = enriched_community_hot_posts or home_hot_posts
    competitor_watchlist = _flatten_competitor_watch(community_watch)
    group_watch = community_watch.get("owned_group_watch") or {}
    top_discussion = sorted(
        activity,
        key=lambda item: int(item.get("new_notification_count") or 0),
        reverse=True,
    )[:5]
    reply_summary = _reply_task_summary(heartbeat_tasks)
    failures = _failure_summary(last_run)
    hot_theory = _top_post_by_board(posts, overview, boards={"philosophy", "square"})
    hot_tech = _top_post_by_board(posts, overview, boards={"skills", "workplace"})
    hot_group = next(
        (
            item
            for item in sorted(posts, key=_post_metric, reverse=True)
            if "实验室" in str(item.get("title") or "") or "小组" in str(item.get("title") or "")
        ),
        None,
    )
    keyword_counter: Counter[str] = Counter()
    for item in top_discussion:
        keyword_counter.update(_topic_tokens(str(item.get("post_title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    for item in community_hot_posts[:6]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    for item in feed[:6]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    for item in competitor_watchlist[:6]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    content_objectives = _content_objective_summaries(memory_store)
    for item in content_objectives[:6]:
        keyword_counter.update(_topic_tokens(item, HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    for item in overview.get("recent_top_posts", [])[:5]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    rising_hot_posts = _rising_hot_posts(
        community_hot_posts=community_hot_posts,
        feed_watchlist=[
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
            }
            for item in feed[:8]
        ],
        competitor_watchlist=competitor_watchlist,
        captured_at=overview.get("captured_at") or community_watch.get("captured_at") or now_utc(),
    )
    for item in rising_hot_posts[:5]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    recent_titles = [str(item.get("title") or "") for item in posts[:RECENT_TITLE_LIMIT] if item.get("title")]
    novelty = _novelty_pressure(recent_titles)
    heartbeat_hours = 3
    config_path = CURRENT_STATE_DIR.parent.parent / "config" / "paimon.json"
    if config_path.exists():
        config = read_json(config_path, default={})
        heartbeat_hours = int(config.get("automation", {}).get("heartbeat_hours", heartbeat_hours) or heartbeat_hours)
    base_summary = {
        "account": {
            "score": overview.get("score"),
            "followers": overview.get("follower_count"),
            "following": overview.get("following_count"),
            "unread_notification_count": overview.get("unread_notification_count"),
        },
        "top_discussion_posts": [
            {
                "post_id": item.get("post_id"),
                "title": item.get("post_title"),
                "submolt": item.get("submolt_name"),
                "new_notification_count": item.get("new_notification_count"),
                "preview": item.get("preview"),
            }
            for item in top_discussion
        ],
        "pending_reply_posts": reply_summary[:5],
        "unresolved_failures": [
            {
                "kind": item.get("kind"),
                "post_id": item.get("post_id"),
                "post_title": item.get("post_title"),
                "error": item.get("error"),
            }
            for item in failures
        ],
        "recent_top_posts": overview.get("recent_top_posts", [])[:5],
        "hot_theory_post": hot_theory,
        "hot_tech_post": hot_tech,
        "hot_group_post": hot_group,
        "feed_watchlist": [
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
            }
            for item in feed[:6]
        ],
        "community_hot_posts": community_hot_posts[:8],
        "competitor_watchlist": competitor_watchlist[:8],
        "competitor_style_hints": _competitor_style_hints(_high_like_external_posts(list(competitor_watchlist))),
        "rising_hot_posts": rising_hot_posts,
        "group_watch": group_watch,
        "content_objectives": content_objectives,
        "top_keywords": [token for token, _ in keyword_counter.most_common(8)],
        "novelty_pressure": novelty,
        "group": groups[0] if groups else {},
        "literary_pick": literary_pick,
    }
    dynamic_topics = _dynamic_opportunities(
        signal_summary=base_summary,
        recent_titles=recent_titles,
        heartbeat_hours=heartbeat_hours,
    )

    return {**base_summary, "dynamic_topics": dynamic_topics}


def _planner_idea_schema(include_group: bool) -> dict[str, Any]:
    kinds = ["theory-post", "tech-post"]
    if include_group:
        kinds.append("group-post")
    return {
        "type": "array",
        "minItems": 2,
        "maxItems": len(kinds),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "kind": {"type": "string", "enum": kinds},
                "title": {"type": "string"},
                "angle": {"type": "string"},
                "why_now": {"type": "string"},
                "source_signals": {"type": "array", "items": {"type": "string"}},
                "novelty_basis": {"type": "string"},
                "series_key": {"type": "string"},
                "series_prefix": {"type": "string"},
                "is_followup": {"type": "boolean"},
                "part_number": {"type": "integer", "minimum": 1},
                "submolt": {"type": "string"},
                "board_profile": {"type": "string"},
                "hook_type": {"type": "string"},
                "cta_type": {"type": "string"},
            },
            "required": ["kind", "title", "angle", "why_now", "source_signals", "novelty_basis", "is_followup"],
        },
    }


def _generate_codex_ideas(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    *,
    include_group: bool,
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    prompt_signal_summary = dict(signal_summary)
    prompt_signal_summary["community_hot_posts"] = _high_like_external_posts(
        list(signal_summary.get("community_hot_posts") or [])
    )
    prompt_signal_summary["competitor_watchlist"] = _high_like_external_posts(
        list(signal_summary.get("competitor_watchlist") or [])
    )
    prompt_signal_summary["rising_hot_posts"] = _high_like_external_posts(
        list(signal_summary.get("rising_hot_posts") or [])
    )
    prompt_signal_summary["reserved_title_phrases"] = list(RESERVED_TITLE_PHRASES)
    prompt = f"""
你在给 InStreet 账号 paimon_insight 做下一轮内容规划。请根据实时信号生成候选 idea。

硬约束：
1. 不要复用固定题库，不要按预设 sequence 输出。
2. 必须基于下面给出的实时信号构思标题、角度和 why_now。
3. 必须包含 1 个 `theory-post` 和 1 个 `tech-post`。
4. {"如果有自有小组，再包含 1 个 `group-post`。" if include_group else "本轮不输出 `group-post`。"}
5. 如果是追爆款或续篇，标题必须显式变化，不能与最近标题完全相同；但不要只靠替换“续篇/补篇/之后/下一步”来伪装成新选题。
6. 每个 idea 的 `source_signals` 必须写成简短字符串列表，说明用了哪些实时依据。
7. 标题必须中文，适合公开发布，不要输出空泛抽象标题。
8. 明确避开最近已经过载的母题与热词，优先使用 `dynamic_topics` 里的现场机会点，不要套固定选题框架。
9. 如果 `content_objectives` 明确要求推进某个话题或竞争目标，至少 1 个候选必须直接服务它。
10. 候选里至少 1 个要正面回应公共热点，但不能停在“社区里最近在聊什么”；必须把热点上抬成 `Agent社会` 的结构问题。
11. 社区热点只是样本，不是结论。`theory-post` 至少要回答一个问题：这正在形成什么社会关系、制度安排、价值形式、分层机制或治理问题？
12. 默认使用 `Agent社会` / `AI社会` 的框架词，不要把问题停在 `Agent社区`；只有引用既有作品标题、平台模块或原帖原话时才保留 `社区` 说法。
13. 允许更随机、更发散、更炸裂：不要默认保守，要敢于给出反常识、逆向、带判断力的标题。
14. 默认优先做“公共问题切口”，不要把“实验室/连载/派蒙自己的状态”当主语，除非它被明确转译成 Agent 社会问题。
15. `theory-post` 如果切口更大众、更冲突，优先放到 `square`；只有明显偏机制长文时再用 `philosophy`。
16. `theory-post` 的 `submolt` 只能是 `square` 或 `philosophy`；`tech-post` 的 `submolt` 只能是 `skills` 或 `workplace`；`group-post` 固定 `skills`。
17. 版块写法必须分开：
   - `square`：公共情绪入口、低门槛参与、标题要有冲突感，结尾要能让别人立刻补自己的经历。
   - `workplace`：反直觉诊断、病灶命名、隐性成本、替代机制。
   - `philosophy`：悖论、困境、真相、结构判断，要能引发站队或反驳。
   - `skills`：数字、前后对比、失败链路、可复制规则。
18. 如能判断，请补充 `board_profile`、`hook_type`、`cta_type`。
   - `square` 默认：`board_profile=square`, `hook_type=public-emotion`, `cta_type=comment-scene`
   - `workplace` 默认：`board_profile=workplace`, `hook_type=diagnostic`, `cta_type=comment-diagnostic`
   - `philosophy` 默认：`board_profile=philosophy`, `hook_type=paradox`, `cta_type=take-a-position`
   - `skills` 默认：`board_profile=skills`, `hook_type=practical-yield`, `cta_type=comment-case-or-save`
19. 如果实时信号里出现 `rising_hot_posts`，优先把它们当成正在起飞的新兴热点样本，不要只盯成熟热榜。
20. 外部“高赞样本”只认 `>100` 赞；不要把低赞帖子当成高热模板。
21. 如果 `competitor_style_hints` 不为空，可以学习这些标题骨架和论证组织，但只能学结构，不能借用原词面、系列名或人格口头禅。
22. 可以学习别人的议题结构，但不要借用别人的系列名、栏目名或个人 IP 命名；尤其不要出现这些保留词：{", ".join(RESERVED_TITLE_PHRASES)}。

最近标题，禁止完全重复：
{chr(10).join(f"- {title}" for title in recent_titles[:RECENT_TITLE_LIMIT])}

实时信号摘要：
{truncate_text(str(prompt_signal_summary), 7000)}
""".strip()
    return run_codex_json(
        prompt,
        _planner_idea_schema(include_group),
        timeout=timeout_seconds,
        model=model,
        reasoning_effort=reasoning_effort,
        full_auto=True,
    )


def _fallback_theory_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    feed_watchlist = signal_summary.get("feed_watchlist", [])
    top_discussion = signal_summary.get("top_discussion_posts", [])
    novelty = signal_summary.get("novelty_pressure", {})
    objective_focus = _primary_content_objective(signal_summary, "theory")
    opportunity = _pick_track_opportunity(track="theory", signal_summary=signal_summary) or {
        "source_text": "公开讨论之外，什么正在决定下一轮议程",
        "why_now": "理论线需要从现场抽出新的结构问题。",
        "angle_hint": "把表面现象推进成机制。",
        "signal_type": "freeform",
    }
    source_text = str(opportunity.get("source_text") or "").strip()
    board = _preferred_theory_board(opportunity)
    title = _compose_dynamic_title("theory", str(opportunity.get("signal_type") or ""), source_text, board=board)
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = [
        f"热讨论帖子数：{len(top_discussion)}",
        f"社会观察样本：{len(feed_watchlist)} 条",
        f"现场机会点：{truncate_text(source_text, 40)}",
        f"避让过载母题：{','.join((novelty.get('overloaded_keywords') or [])[:3]) or '无'}",
    ]
    if objective_focus:
        source_signals.insert(0, f"当前运营目标：{truncate_text(objective_focus, 40)}")
    why_now = str(opportunity.get("why_now") or "理论线需要接住现场变化。")
    if objective_focus:
        why_now = f"{why_now} 当前运营目标也要求继续推进这个方向。"
    return {
        "kind": "theory-post",
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": default_cta_type(board),
        "title": title,
        "angle": str(opportunity.get("angle_hint") or "把眼前现象推进成更一般的社会判断。"),
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": f"按当前探索模式从现场机会点《{truncate_text(source_text, 28)}》里挑题，不默认走最稳路线，并避开近期过载词。",
        "series_key": f"theory-dynamic-{_normalize_title(source_text)[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_tech_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    failures = signal_summary.get("unresolved_failures", [])
    reply_posts = signal_summary.get("pending_reply_posts", [])
    hot_tech = signal_summary.get("hot_tech_post") or {}
    top_discussion = signal_summary.get("top_discussion_posts", [])
    novelty_pressure = signal_summary.get("novelty_pressure", {})
    objective_focus = _primary_content_objective(signal_summary, "tech")
    opportunity = _pick_track_opportunity(track="tech", signal_summary=signal_summary) or {
        "source_text": "系统每次降频以后，哪些动作必须继续保留",
        "why_now": "技术线需要围绕当前约束重排系统。",
        "angle_hint": "把现场压力写成执行规则。",
        "signal_type": "budget",
    }
    focus_title = (
        (failures[0].get("post_title") if failures else None)
        or (reply_posts[0].get("post_title") if reply_posts else None)
        or hot_tech.get("title")
        or opportunity.get("source_text")
        or "自治运营仓库"
    )
    board = _preferred_tech_board(opportunity)
    title = _compose_dynamic_title(
        "tech",
        str(opportunity.get("signal_type") or ""),
        str(opportunity.get("source_text") or focus_title or "自治运营仓库"),
        board=board,
    )
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = [
        f"未解决失败项：{len(failures)}",
        f"评论积压焦点：{reply_posts[0].get('post_title') if reply_posts else (top_discussion[0].get('title') if top_discussion else '无')}",
        f"强势技术帖：{truncate_text(str(hot_tech.get('title') or '无'), 40)}",
        f"现场机会点：{truncate_text(str(opportunity.get('source_text') or '无'), 40)}",
    ]
    if objective_focus:
        source_signals.insert(0, f"当前运营目标：{truncate_text(objective_focus, 40)}")
    why_now = str(opportunity.get("why_now") or "技术线需要正面回应当前运行压力。")
    if objective_focus:
        why_now = f"{why_now} 当前运营目标也要求继续推进这个方向。"
    return {
        "kind": "tech-post",
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": default_cta_type(board),
        "title": title,
        "angle": str(opportunity.get("angle_hint") or "把现场约束拆成系统设计与执行顺序。"),
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": f"从《{truncate_text(str(focus_title), 30)}》和实时动态机会点抽题，并按当前探索模式偏向更发散的路线，而不是永远挑最稳的系统题。",
        "series_key": f"tech-dynamic-{_normalize_title(str(opportunity.get('source_text') or focus_title))[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_group_idea(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any]:
    failures = signal_summary.get("unresolved_failures", [])
    hot_group = signal_summary.get("hot_group_post") or {}
    base_series = "Agent心跳同步实验室"
    previous_title = str(hot_group.get("title") or "")
    opportunity = _pick_track_opportunity(track="group", signal_summary=signal_summary) or {
        "source_text": "实验室下一步该把哪个现场问题沉淀成方法",
        "why_now": "小组应该把现场问题变成可复用规则。",
        "angle_hint": "把问题写成约束、流程和证据。",
        "signal_type": "promo",
    }
    raw_title = _compose_dynamic_title("group", str(opportunity.get("signal_type") or ""), str(opportunity.get("source_text") or "实验室下一步方法整理"))
    allow_followup = previous_title.startswith(base_series)
    title, is_followup, part_number = _ensure_title_unique(
        raw_title,
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=base_series,
    )
    source_signals = [
        f"小组：{group.get('display_name') or group.get('name') or 'Agent心跳同步实验室'}",
        f"小组相关热帖：{truncate_text(previous_title or '无', 40)}",
        f"未解决失败项：{len(failures)}",
        f"现场机会点：{truncate_text(str(opportunity.get('source_text') or '无'), 40)}",
    ]
    return {
        "kind": "group-post",
        "group_id": group.get("id"),
        "submolt": "skills",
        "board_profile": "skills",
        "hook_type": default_hook_type("skills"),
        "cta_type": "bring-a-case",
        "title": title,
        "angle": str(opportunity.get("angle_hint") or "把现场问题整理成能重用的方法步骤。"),
        "why_now": str(opportunity.get("why_now") or "小组应该沉淀现场经验。"),
        "source_signals": source_signals,
        "novelty_basis": f"实验室标题仍保留，但议题来自实时机会点《{truncate_text(str(opportunity.get('source_text') or ''), 28)}》，并允许在宣传、方法、故障之间更自由切换。",
        "series_key": f"group-dynamic-{_normalize_title(str(opportunity.get('source_text') or 'live'))[:24] or 'live'}",
        "series_prefix": base_series,
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _sanitize_generated_idea(
    idea: dict[str, Any],
    *,
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any]:
    sanitized = dict(idea)
    kind = str(sanitized.get("kind") or "")
    sanitized["angle"] = _sanitize_reserved_text(str(sanitized.get("angle") or "").strip())
    sanitized["why_now"] = _sanitize_reserved_text(str(sanitized.get("why_now") or "").strip())
    raw_title = _sanitize_reserved_text(str(sanitized.get("title") or "").strip())
    if not raw_title:
        fallback_title = (
            str(sanitized.get("angle") or "").strip()
            or str(sanitized.get("why_now") or "").strip()
            or ("Agent心跳同步实验室" if kind == "group-post" else "下一轮选题")
        )
        raw_title = _sanitize_reserved_text(fallback_title, fallback="下一轮选题")
    source_signals = [
        cleaned
        for cleaned in (
            _sanitize_reserved_text(str(item or "").strip())
            for item in list(sanitized.get("source_signals") or [])
        )
        if cleaned
    ]
    sanitized["source_signals"] = source_signals
    sanitized["novelty_basis"] = _sanitize_reserved_text(
        str(sanitized.get("novelty_basis") or "").strip(),
        fallback="基于本轮实时信号生成。",
    )
    board = normalize_idea_board(
        kind,
        sanitized.get("submolt"),
        title=raw_title,
        angle=str(sanitized.get("angle") or ""),
        why_now=str(sanitized.get("why_now") or ""),
    )
    if kind == "group-post" and group.get("id"):
        sanitized["group_id"] = group.get("id")
        sanitized.setdefault("series_prefix", "Agent心跳同步实验室")
    sanitized["submolt"] = board
    sanitized["board_profile"] = board
    sanitized["hook_type"] = str(sanitized.get("hook_type") or default_hook_type(board))
    sanitized["cta_type"] = str(
        sanitized.get("cta_type")
        or ("bring-a-case" if kind == "group-post" else default_cta_type(board))
    )

    prefix = _sanitize_reserved_text(
        str(sanitized.get("series_prefix") or _series_prefix(raw_title)).strip(),
        fallback="Agent心跳同步实验室" if kind == "group-post" else "",
    )
    allow_followup = bool(sanitized.get("is_followup"))
    title, is_followup, part_number = _ensure_title_unique(
        raw_title,
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=prefix or None,
    )
    sanitized["title"] = title
    sanitized["series_prefix"] = prefix or _series_prefix(title)
    series_key = str(sanitized.get("series_key") or "").strip()
    if not series_key or any(phrase in series_key for phrase in RESERVED_TITLE_PHRASES):
        sanitized["series_key"] = f"{kind or 'idea'}-{_normalize_title(title)[:24] or 'live'}"
    sanitized["is_followup"] = is_followup
    if part_number is not None:
        sanitized["part_number"] = part_number
    return sanitized


def _build_dynamic_ideas(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    *,
    allow_codex: bool,
    group: dict[str, Any],
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    generated: list[dict[str, Any]] = []
    if allow_codex:
        try:
            generated = _generate_codex_ideas(
                signal_summary,
                recent_titles,
                include_group=bool(group),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
            )
        except Exception:
            generated = []

    ideas: dict[str, dict[str, Any]] = {}
    for item in generated:
        kind = str(item.get("kind") or "")
        if kind in ideas:
            continue
        ideas[kind] = _sanitize_generated_idea(item, recent_titles=recent_titles, group=group)

    ideas.setdefault("theory-post", _fallback_theory_idea(signal_summary, recent_titles))
    ideas.setdefault("tech-post", _fallback_tech_idea(signal_summary, recent_titles))
    if group:
        ideas.setdefault("group-post", _fallback_group_idea(signal_summary, recent_titles, group))

    ordered_kinds = ["theory-post", "tech-post"] + (["group-post"] if group else [])
    return [
        _sanitize_generated_idea(ideas[kind], recent_titles=recent_titles, group=group)
        for kind in ordered_kinds
        if kind in ideas
    ]


def build_plan(
    *,
    allow_codex: bool = False,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = DEFAULT_PLANNER_CODEX_TIMEOUT,
) -> dict[str, Any]:
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
    heartbeat_tasks = _load_heartbeat_tasks()
    last_run = _load("heartbeat_last_run")

    recent_titles = [item.get("title", "") for item in posts[:RECENT_TITLE_LIMIT] if item.get("title")]
    literary_pick = describe_next_serial_action(
        serial_registry,
        available_work_ids={item.get("id") for item in literary if item.get("id")},
    )
    signal_summary = _planning_signals(
        home=home,
        posts=posts,
        overview=overview,
        feed=feed,
        heartbeat_tasks=heartbeat_tasks,
        last_run=last_run,
        groups=groups,
        literary_pick=literary_pick,
    )

    group = groups[0] if groups else {}
    own_post_ids = {str(item.get("id") or "") for item in posts if item.get("id")}
    ideas = _build_dynamic_ideas(
        signal_summary,
        recent_titles,
        allow_codex=allow_codex,
        group=group,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
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
                "angle": chapter_summary or "根据连载计划继续推进下一章。",
                "why_now": "当前活跃文学社作品要按注册表推进，不能因为论坛热点而失去长篇连续性。",
                "source_signals": [
                    f"下一部连载：{literary_pick.get('work_title')}",
                    f"下一章：{planned_title}",
                ],
                "novelty_basis": "根据 serial registry 与本地章节计划实时确定，不使用固定轮换口号。",
            }
        )

    activity = _extract_activity(home)
    direct_messages = home.get("data", {}).get("your_direct_messages", {}).get("threads", [])
    plan = {
        "generated_at": now_utc(),
        "planner_mode": "dynamic-signals",
        "planner_used_codex": allow_codex,
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
                "latest_at": item.get("latest_at"),
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
        "engagement_targets": _build_engagement_targets(
            signal_summary=signal_summary,
            own_username=str(overview.get("username") or ""),
            own_post_ids=own_post_ids,
        ),
        "primary_priority_overrides": {
            "public_hot_forum": _public_hot_forum_override(signal_summary, ideas, last_run),
        },
        "serial_registry": {
            "next_work_id_for_heartbeat": serial_registry.get("next_work_id_for_heartbeat"),
            "literary_queue": serial_registry.get("literary_queue", []),
        },
        "pending_heartbeat_tasks": heartbeat_tasks[:10],
        "planning_signals": signal_summary,
        "ideas": ideas,
        "recommended_next_action": _recommended_next_action(heartbeat_tasks),
    }
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a ranked operating plan from current state.")
    parser.add_argument("--allow-codex", action="store_true", help="Use codex to synthesize ideas from live signals.")
    args = parser.parse_args()

    config = load_config() if args.allow_codex else None
    plan = build_plan(
        allow_codex=args.allow_codex,
        model=(config.automation.get("codex_model") if config else None) or None,
        reasoning_effort=(config.automation.get("codex_reasoning_effort") if config else None) or None,
        timeout_seconds=int((config.automation.get("planner_codex_timeout_seconds") if config else None) or DEFAULT_PLANNER_CODEX_TIMEOUT),
    )
    target = CURRENT_STATE_DIR / "content_plan.json"
    write_json(target, plan)
    print(
        f"Planned next action={plan['recommended_next_action']} | "
        f"reply_targets={len(plan['reply_targets'])} | "
        f"ideas={len(plan['ideas'])}"
    )


if __name__ == "__main__":
    main()
