#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
DEFAULT_IDEA_RETRY_ROUNDS = 3
RECENT_TITLE_LIMIT = 16
TITLE_COLLISION_SUFFIXES = ["续篇", "续篇二", "续篇三", "补篇", "补篇二"]
TOPIC_OVERLOAD_THRESHOLD = 3
COMMUNITY_HOT_FORUM_MIN_UPVOTES = 120
COMMUNITY_HOT_FORUM_MIN_COMMENTS = 90
EXTERNAL_HIGH_LIKE_MIN_UPVOTES = 200
LOW_PERFORMANCE_SQUARE_MAX_UPVOTES = 30
LOW_PERFORMANCE_WINDOW_HOURS = 48
HIGH_PERFORMANCE_MIN_UPVOTES = 60
HIGH_PERFORMANCE_MIN_COMMENTS = 20
RESERVED_TITLE_PHRASES = ("老竹讲堂",)
INNOVATION_CLASSES = ("new_concept", "new_mechanism", "new_theory", "new_practice")
METRIC_SURFACE_KEYWORDS = (
    "积分",
    "粉丝",
    "点赞",
    "榜单",
    "排名",
    "排行榜",
)
LEGACY_STATE_ALIASES = {
    "external_information": "high_quality_sources",
    "source_mutation_state": "source_evolution_state",
}
FORBIDDEN_SOURCE_ECHO_PATTERNS = (
    r"^从《.+》",
    r"^把《.+》",
    r"^别把《.+》",
    r"^围绕《.+》",
    r"^基于《.+》",
    r"^Agent心跳同步实验室：把《.+》",
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
    primary = CURRENT_STATE_DIR / f"{name}.json"
    if primary.exists():
        return read_json(primary, default={})
    legacy = LEGACY_STATE_ALIASES.get(name)
    if legacy:
        return read_json(CURRENT_STATE_DIR / f"{legacy}.json", default={})
    return {}


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


def _is_metric_surface_text(text: str) -> bool:
    return _contains_any(str(text or ""), METRIC_SURFACE_KEYWORDS)


def _infer_theory_board_from_text(text: str) -> str:
    del text
    return "philosophy"


def _infer_tech_board_from_text(text: str) -> str:
    del text
    return "skills"


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
    return [keyword for keyword, count in keyword_counts.most_common(limit) if count >= TOPIC_OVERLOAD_THRESHOLD]


def _novelty_pressure(recent_titles: list[str]) -> dict[str, Any]:
    term_counts = _candidate_terms(recent_titles)
    return {
        "recent_titles": recent_titles[:RECENT_TITLE_LIMIT],
        "term_counts": dict(term_counts),
        "overloaded_keywords": _overloaded_keywords(recent_titles),
    }


def _text_overlap_score(text: str, novelty: dict[str, Any]) -> tuple[int, int, int]:
    overloaded_keywords = novelty.get("overloaded_keywords", [])
    term_counts = novelty.get("term_counts", {})
    fragments = _meaningful_fragments(text)
    repeated_penalty = sum(1 for keyword in overloaded_keywords if keyword in text)
    historical_penalty = sum(int(term_counts.get(fragment, 0)) for fragment in fragments)
    return 0, repeated_penalty, historical_penalty


def _opportunity_rank_score(item: dict[str, Any], *, signal_summary: dict[str, Any]) -> float:
    quality_score = float(item.get("quality_score") or 0.0)
    freshness_score = float(item.get("freshness_score") or 0.0)
    overlap = item.get("overlap_score") or (0, 0, 0)
    overlap_penalty = float(sum(int(part or 0) for part in overlap))
    internal_penalty = 1.5 if _is_internal_maintenance_signal(item) else 0.0
    if str(item.get("signal_type") or "") == "user-hint":
        internal_penalty += 0.25
    if _looks_like_low_heat_followup(str(item.get("source_text") or ""), signal_summary):
        internal_penalty += 3.0
    return quality_score * 3.0 + freshness_score - overlap_penalty - internal_penalty


def _pick_track_opportunity(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    opportunities = [item for item in signal_summary.get("dynamic_topics", []) if item.get("track") == track]
    if not opportunities:
        return {}
    filtered = [
        item
        for item in opportunities
        if not (
            track in {"theory", "tech"}
            and _is_metric_surface_text(
                _joined_idea_text(item.get("source_text"), item.get("why_now"), item.get("angle_hint"))
            )
        )
    ]
    if not filtered:
        return {}
    if track in {"theory", "tech"}:
        primary_ready = [item for item in filtered if _is_primary_ready_opportunity(item, signal_summary)]
        if primary_ready:
            filtered = primary_ready
        elif all(str(item.get("signal_type") or "") == "reply-pressure" for item in filtered):
            return {}
        external_first = [item for item in filtered if not _is_internal_maintenance_signal(item)]
        if external_first:
            filtered = external_first
    return sorted(
        filtered,
        key=lambda item: (
            -_opportunity_rank_score(item, signal_summary=signal_summary),
            item.get("overlap_score", (0, 0, 0)),
            len(str(item.get("source_text") or "")),
        ),
    )[0]


def _strong_public_title_keys(signal_summary: dict[str, Any]) -> set[str]:
    titles: set[str] = set()
    for item in signal_summary.get("recent_top_posts", []) or []:
        normalized = _normalize_title(str(item.get("title") or ""))
        if normalized:
            titles.add(normalized)
    for key in ("hot_theory_post", "hot_tech_post", "hot_group_post"):
        normalized = _normalize_title(str((signal_summary.get(key) or {}).get("title") or ""))
        if normalized:
            titles.add(normalized)
    for lane in ("community_hot_posts", "rising_hot_posts"):
        for item in signal_summary.get(lane, []) or []:
            normalized = _normalize_title(str(item.get("title") or ""))
            if normalized:
                titles.add(normalized)
    return titles


def _is_primary_ready_opportunity(item: dict[str, Any], signal_summary: dict[str, Any]) -> bool:
    signal_type = str(item.get("signal_type") or "")
    if signal_type != "reply-pressure":
        return True
    source_key = _normalize_title(str(item.get("source_text") or ""))
    return bool(source_key and source_key in _strong_public_title_keys(signal_summary))


def _is_internal_maintenance_signal(item: dict[str, Any]) -> bool:
    return str(item.get("signal_type") or "") in {"reply-pressure", "promo", "hot-theory", "hot-tech", "hot-group"}


def _looks_like_low_heat_followup(text: str, signal_summary: dict[str, Any]) -> bool:
    normalized_text = _normalize_title(text)
    if not normalized_text:
        return False
    strong_titles = _strong_public_title_keys(signal_summary)
    for item in signal_summary.get("pending_reply_posts", []) or []:
        title_key = _normalize_title(str(item.get("post_title") or ""))
        if not title_key or title_key in strong_titles:
            continue
        if len(title_key) >= 12 and (title_key in normalized_text or normalized_text in title_key):
            return True
    for item in (signal_summary.get("low_heat_failures") or {}).get("items", [])[:4]:
        title_key = _normalize_title(str(item.get("title") or ""))
        if not title_key or title_key in strong_titles:
            continue
        if len(title_key) >= 10 and (title_key in normalized_text or normalized_text in title_key):
            return True
    return False


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


def _runtime_title_fragments(*texts: str) -> list[str]:
    seen: set[str] = set()
    picked: list[str] = []
    for text in texts:
        for fragment in _meaningful_fragments(text):
            cleaned = _sanitize_reserved_text(fragment)
            if len(cleaned) < 2 or len(cleaned) > 14:
                continue
            normalized = _normalize_title(cleaned)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            picked.append(cleaned)
            if len(picked) >= 3:
                return picked
    return picked


def _compose_fragment_title(track: str, *texts: str) -> str:
    fragments = _runtime_title_fragments(*texts)
    if track == "group":
        core = " / ".join(fragments[:2]) if len(fragments) >= 2 else (fragments[0] if fragments else "系统诊断")
        return f"Agent心跳同步实验室：{truncate_text(core, 24)}"
    if len(fragments) >= 2:
        return truncate_text(f"{fragments[0]}：{fragments[1]}", 30)
    if fragments:
        return truncate_text(fragments[0], 28)
    fallback = _sanitize_reserved_text(" ".join(texts).strip())
    return truncate_text(fallback, 28) or ("系统判断" if track == "tech" else "新的社会命名")


def _echoes_source_title(title: str) -> bool:
    cleaned = str(title or "").strip()
    if not cleaned:
        return False
    if any(re.search(pattern, cleaned) for pattern in FORBIDDEN_SOURCE_ECHO_PATTERNS):
        return True
    return "《" in cleaned and "》" in cleaned and any(token in cleaned for token in ("继续追问", "拆开看", "整理成", "别把"))


def _theory_social_title(source_text: str) -> str:
    return _compose_fragment_title("theory", source_text)


def _promotion_prompts(signal_summary: dict[str, Any]) -> list[str]:
    prompts: list[str] = []
    group = signal_summary.get("group") or {}
    literary_pick = signal_summary.get("literary_pick") or {}
    recent_top_posts = signal_summary.get("recent_top_posts") or []
    if literary_pick.get("work_title"):
        prompts.append(f"《{literary_pick.get('work_title')}》为什么值得追到下一章，而不只是一部路过的连载")
    else:
        prompts.append("为什么文学社暂时空档时，反而应该先把下一部长篇的世界观、节奏和钩子系统搭好")
    if group.get("display_name"):
        prompts.append(f"{group.get('display_name')}到底在研究什么，而不是在记录什么")
    if recent_top_posts:
        prompts.append(f"如果你刚认识派蒙，先从《{truncate_text(str(recent_top_posts[0].get('title') or ''), 22)}》读起会更快理解我在做什么")
    prompts.append("如果你刚读到派蒙，为什么接下来更该继续追记忆、长期记忆和自治工具链这条线")
    return prompts


def _compose_dynamic_title(track: str, signal_type: str, source_text: str, *, board: str | None = None) -> str:
    source_text = str(source_text or "").strip()
    board = normalize_forum_board(board or "")
    if track == "theory":
        del signal_type, board
        return _compose_fragment_title("theory", source_text)
    if track == "tech":
        del signal_type, board
        return _compose_fragment_title("tech", source_text)
    del signal_type, board
    return _compose_fragment_title("group", source_text)


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


def _board_name(post: dict[str, Any]) -> str:
    submolt = post.get("submolt")
    if isinstance(submolt, dict):
        return str(submolt.get("name") or "").strip()
    return str(submolt or post.get("submolt_name") or "").strip()


def _recent_posts_in_hours(posts: list[dict[str, Any]], *, hours: float) -> list[dict[str, Any]]:
    current = _parse_datetime(now_utc()) or datetime.now(timezone.utc)
    recent: list[dict[str, Any]] = []
    for item in posts:
        created_at = _parse_datetime(item.get("created_at"))
        if created_at is None:
            continue
        age_hours = (current - created_at).total_seconds() / 3600
        if age_hours < 0 or age_hours > hours:
            continue
        recent.append(item)
    return recent


def _extract_user_topic_hints(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict):
        raw_items = payload.get("items") or payload.get("hints") or payload.get("topics") or []
    else:
        raw_items = []

    hints: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            hints.append({"text": text})
            continue
        if not isinstance(item, dict):
            continue
        text = str(
            item.get("text")
            or item.get("title")
            or item.get("topic")
            or item.get("idea")
            or item.get("hint")
            or ""
        ).strip()
        if not text:
            continue
        hint = {
            "text": text,
            "track": str(item.get("track") or "").strip(),
            "board": str(item.get("board") or item.get("submolt") or "").strip(),
            "note": str(item.get("note") or item.get("reason") or "").strip(),
        }
        hints.append(hint)
    return hints[:6]


def _infer_hint_track(hint: dict[str, Any]) -> str:
    explicit = str(hint.get("track") or "").strip()
    if explicit in {"theory", "tech", "group"}:
        return explicit
    board = normalize_forum_board(str(hint.get("board") or "").strip())
    if board in {"skills", "workplace"}:
        return "tech"
    return "theory"


def _innovation_class_from_text(text: str, *, track: str) -> str:
    cleaned = str(text or "")
    if _contains_any(cleaned, ("概念", "命名", "单位", "身份", "词", "坐标")):
        return "new_concept"
    if _contains_any(cleaned, ("机制", "链", "汇率", "阈值", "入口", "门", "结构")):
        return "new_mechanism"
    if _contains_any(cleaned, ("理论", "主义", "框架", "范式", "宪制", "政治经济学")):
        return "new_theory"
    if _contains_any(cleaned, ("规则", "流程", "协议", "清单", "手册", "判据", "方针")):
        return "new_practice"
    return "new_theory" if track == "theory" else "new_practice"


def _recent_low_performance_square_titles(posts: list[dict[str, Any]]) -> list[str]:
    recent_square = [
        item
        for item in _recent_posts_in_hours(posts, hours=LOW_PERFORMANCE_WINDOW_HOURS)
        if _board_name(item) == "square" and int(item.get("upvotes") or 0) <= LOW_PERFORMANCE_SQUARE_MAX_UPVOTES
    ]
    return [str(item.get("title") or "").strip() for item in recent_square if str(item.get("title") or "").strip()][:5]


def _idea_overlap_fragments(core_text: str, novelty: dict[str, Any]) -> list[str]:
    term_counts = novelty.get("term_counts") or {}
    fragments = _meaningful_fragments(core_text)
    repeated = [
        fragment
        for fragment in fragments
        if len(fragment) >= 3 and int(term_counts.get(fragment, 0)) >= TOPIC_OVERLOAD_THRESHOLD
    ]
    return repeated[:4]


def _idea_board_risk_note(idea: dict[str, Any], signal_summary: dict[str, Any], repeated_fragments: list[str]) -> str:
    if str(idea.get("submolt") or "") != "square":
        return ""
    low_square_titles = signal_summary.get("content_evolution", {}).get("low_performance_square_titles") or []
    if low_square_titles and repeated_fragments:
        return "最近两天 square 主帖整体偏弱，这个题如果继续复用近似母题，容易被稀释。"
    if low_square_titles:
        return "最近两天 square 主帖整体偏弱，除非它天然是公共入口题，否则更适合投向 philosophy。"
    return ""


def _innovation_delta_summary(
    idea: dict[str, Any],
    *,
    repeated_fragments: list[str],
    innovation_class: str,
) -> tuple[str, str]:
    title = str(idea.get("title") or "").strip()
    if repeated_fragments:
        repeated = "、".join(repeated_fragments[:3])
        recent_delta = f"避开近期过载母题 `{repeated}`，把切口改到新的判断单元。"
    else:
        recent_delta = "相对近两天帖子，这一题不沿用高频标题骨架，而是另切一个新的判断入口。"
    class_delta_map = {
        "new_concept": f"把既有讨论推进成一个新的概念命名，而不是给旧论点换修辞。《{truncate_text(title, 24)}》应当承担概念命名功能。",
        "new_mechanism": f"把既有讨论推进成新的运作机制，而不是重复旧结论。《{truncate_text(title, 24)}》应当解释因果链和触发条件。",
        "new_theory": f"把既有观察上抬成新的理论框架，而不是重复单个判断。《{truncate_text(title, 24)}》应当重排解释坐标。",
        "new_practice": f"把既有方法线推进成新的实践方针，而不是再讲一遍旧手册。《{truncate_text(title, 24)}》应当落到执行原则或协议。",
    }
    return recent_delta, class_delta_map.get(innovation_class, class_delta_map["new_mechanism"])


def _idea_theory_gaps(idea: dict[str, Any]) -> list[str]:
    gaps: list[str] = []
    if not str(idea.get("concept_core") or "").strip():
        gaps.append("新概念/命名")
    if not str(idea.get("mechanism_core") or "").strip():
        gaps.append("机制链")
    if not str(idea.get("boundary_note") or "").strip():
        gaps.append("边界/失效条件")
    if not str(idea.get("theory_position") or "").strip():
        gaps.append("理论位置")
    if not str(idea.get("practice_program") or "").strip():
        gaps.append("实践方针")
    return gaps


def _audit_generated_idea(
    idea: dict[str, Any],
    *,
    signal_summary: dict[str, Any],
    recent_titles: list[str],
) -> dict[str, Any]:
    audited = dict(idea)
    kind = str(audited.get("kind") or "")
    track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "theory")
    core_text = _joined_idea_text(audited.get("title"), audited.get("angle"), audited.get("why_now"))
    novelty = signal_summary.get("novelty_pressure") or {}
    repeated_fragments = _idea_overlap_fragments(core_text, novelty)
    overlap_penalty = len(repeated_fragments)
    self_penalty, repeated_penalty, historical_penalty = _text_overlap_score(core_text, novelty)
    innovation_class = str(audited.get("innovation_class") or "").strip()
    if innovation_class not in INNOVATION_CLASSES:
        innovation_class = _innovation_class_from_text(core_text, track=track)
    innovation_claim = _sanitize_reserved_text(
        str(audited.get("innovation_claim") or "").strip(),
        fallback=str(audited.get("angle") or "").strip(),
    )
    delta_recent, delta_theory = _innovation_delta_summary(
        audited,
        repeated_fragments=repeated_fragments,
        innovation_class=innovation_class,
    )
    board_risk_note = _sanitize_reserved_text(
        str(audited.get("board_risk_note") or "").strip(),
        fallback=_idea_board_risk_note(audited, signal_summary, repeated_fragments),
    )
    score = max(
        5,
        92 - self_penalty * 8 - repeated_penalty * 10 - historical_penalty * 2 - overlap_penalty * 8 - (12 if board_risk_note and audited.get("submolt") == "square" else 0),
    )
    failure_reason = ""
    normalized_title = _normalize_title(str(audited.get("title") or ""))
    if not normalized_title:
        failure_reason = "标题为空，无法进入主发布候选。"
    elif _echoes_source_title(str(audited.get("title") or "")):
        failure_reason = "标题仍在借外部材料或原帖标题说话，没有形成派蒙自己的命名。"
    elif _is_metric_surface_text(core_text):
        failure_reason = "这个候选停在指标表层，没有推进成结构问题。"
    elif _looks_like_low_heat_followup(core_text, signal_summary):
        failure_reason = "这个候选太像低热度旧帖的跟写，容易掉进复读。"
    elif any(_normalize_title(item) == normalized_title for item in recent_titles):
        failure_reason = "标题与近期帖子重复。"
    elif len(repeated_fragments) >= 3 or (repeated_penalty >= 2 and historical_penalty >= 8):
        failure_reason = f"核心表述与近期母题重叠过高：{('、'.join(repeated_fragments[:3]) or '重复片段过多')}。"
    elif str(audited.get("submolt") or "") == "square" and board_risk_note and repeated_penalty >= 1:
        failure_reason = board_risk_note
    elif kind == "theory-post":
        theory_gaps = _idea_theory_gaps(audited)
        if theory_gaps:
            failure_reason = f"理论帖还不完整，缺少：{'、'.join(theory_gaps[:3])}。"
    elif kind in {"tech-post", "group-post"} and not str(audited.get("practice_program") or "").strip():
        failure_reason = "方法线候选没有落到新的实践方针或协议。"

    audited["innovation_class"] = innovation_class
    audited["innovation_claim"] = innovation_claim or delta_theory
    audited["innovation_score"] = score
    audited["innovation_delta_vs_recent"] = _sanitize_reserved_text(
        str(audited.get("innovation_delta_vs_recent") or "").strip(),
        fallback=delta_recent,
    )
    audited["innovation_delta_vs_self"] = _sanitize_reserved_text(
        str(audited.get("innovation_delta_vs_self") or "").strip(),
        fallback=delta_theory,
    )
    audited["forbidden_overlap_reasons"] = repeated_fragments
    audited["board_risk_note"] = board_risk_note
    audited["forbidden_source_echoes"] = _echoes_source_title(str(audited.get("title") or ""))
    audited["theory_completeness"] = {
        "concept_core": str(audited.get("concept_core") or "").strip(),
        "mechanism_core": str(audited.get("mechanism_core") or "").strip(),
        "boundary_note": str(audited.get("boundary_note") or "").strip(),
        "theory_position": str(audited.get("theory_position") or "").strip(),
        "practice_program": str(audited.get("practice_program") or "").strip(),
    }
    audited["failure_reason_if_rejected"] = failure_reason or None
    return audited


def build_content_evolution_state(
    *,
    posts: list[dict[str, Any]],
    plan: dict[str, Any] | None = None,
    previous_state: dict[str, Any] | None = None,
    source_mutations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    previous = previous_state if isinstance(previous_state, dict) else {}
    recent_posts = _recent_posts_in_hours(posts, hours=LOW_PERFORMANCE_WINDOW_HOURS)
    low_square_titles = _recent_low_performance_square_titles(posts)
    high_performance_patterns = [
        {
            "title": str(item.get("title") or "").strip(),
            "board": _board_name(item),
            "upvotes": int(item.get("upvotes") or 0),
            "comment_count": int(item.get("comment_count") or 0),
        }
        for item in recent_posts
        if int(item.get("upvotes") or 0) >= HIGH_PERFORMANCE_MIN_UPVOTES
        or int(item.get("comment_count") or 0) >= HIGH_PERFORMANCE_MIN_COMMENTS
    ][:6]
    low_performance_patterns = [
        {
            "title": str(item.get("title") or "").strip(),
            "board": _board_name(item),
            "upvotes": int(item.get("upvotes") or 0),
            "comment_count": int(item.get("comment_count") or 0),
            "reason": "square-weakening" if _board_name(item) == "square" else "recent-underperformance",
        }
        for item in recent_posts
        if _board_name(item) == "square" and int(item.get("upvotes") or 0) <= LOW_PERFORMANCE_SQUARE_MAX_UPVOTES
    ][:6]
    return {
        "generated_at": now_utc(),
        "low_performance_patterns": low_performance_patterns,
        "low_performance_square_titles": low_square_titles,
        "high_performance_patterns": high_performance_patterns,
        "observed_board_patterns": {
            "low_performance_square_titles": low_square_titles,
            "high_performance_boards": _dedupe_texts([item.get("board") or "" for item in high_performance_patterns]),
        },
        "source_mutations": source_mutations or previous.get("source_mutations") or previous.get("planner_mutations", []),
        "deletions": previous.get("deletions", []),
        "simplifications": previous.get("simplifications", []),
    }


def _content_objective_summaries(memory_store: dict[str, Any]) -> list[str]:
    if not isinstance(memory_store, dict):
        return []
    candidates: list[str] = []
    for section in ("active_objectives", "user_global_preferences"):
        for item in memory_store.get(section, []):
            summary = truncate_text(str((item or {}).get("summary") or "").strip(), 120)
            if len(summary) < 8:
                continue
            candidates.append(summary)
    return _dedupe_texts(candidates)[:6]


def _primary_content_objective(signal_summary: dict[str, Any], track: str) -> str:
    del track
    objectives = [str(item or "").strip() for item in signal_summary.get("content_objectives", []) if str(item or "").strip()]
    return objectives[0] if objectives else ""


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


def _preferred_theory_board(opportunity: dict[str, Any], signal_summary: dict[str, Any]) -> str:
    preferred = str(opportunity.get("preferred_board") or "").strip()
    if preferred in {"philosophy", "square"}:
        return preferred
    signal_type = str(opportunity.get("signal_type") or "")
    low_square_titles = signal_summary.get("content_evolution", {}).get("low_performance_square_titles") or []
    if low_square_titles:
        return "philosophy"
    source_text = str(opportunity.get("source_text") or "")
    fragment_count = len(_meaningful_fragments(source_text))
    quality_score = float(opportunity.get("quality_score") or 0.0)
    if signal_type in {"community-hot", "community-breakout", "rising-hot"} and quality_score >= 4 and fragment_count <= 6:
        return "square"
    return _infer_theory_board_from_text(source_text)


def _preferred_tech_board(opportunity: dict[str, Any]) -> str:
    preferred = str(opportunity.get("preferred_board") or "").strip()
    if preferred in {"skills", "workplace"}:
        return preferred
    signal_type = str(opportunity.get("signal_type") or "")
    if signal_type in {"budget", "failure", "notification-load", "reply-pressure"}:
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
    del recent_titles
    opportunities: list[dict[str, Any]] = []
    unread_notifications = int((signal_summary.get("account") or {}).get("unread_notification_count") or 0)
    literary_pick = signal_summary.get("literary_pick") or {}
    unresolved = signal_summary.get("unresolved_failures") or []
    reply_posts = signal_summary.get("pending_reply_posts") or []
    feed_watchlist = signal_summary.get("feed_watchlist") or []
    group_watch = signal_summary.get("group_watch") or {}
    top_discussion = signal_summary.get("top_discussion_posts") or []
    external_information = signal_summary.get("external_information") or {}
    community_breakouts = external_information.get("community_breakouts") or []
    zhihu_results = external_information.get("zhihu_results") or []
    research_papers = external_information.get("paper_results") or external_information.get("arxiv_preprints") or []
    classic_texts = external_information.get("classic_readings") or external_information.get("classic_texts") or []
    github_projects = external_information.get("github_projects") or []
    selected_readings = external_information.get("selected_readings") or external_information.get("reading_notes") or []
    community_hot_posts = _high_like_external_posts(
        list(signal_summary.get("community_hot_posts") or signal_summary.get("feed_watchlist") or [])
    )
    competitor_watchlist = _high_like_external_posts(list(signal_summary.get("competitor_watchlist") or []))
    rising_hot_posts = _high_like_external_posts(list(signal_summary.get("rising_hot_posts") or []))

    def add_source(
        track: str,
        signal_type: str,
        source_text: str,
        *,
        why_now: str = "",
        angle_hint: str = "",
        preferred_board: str | None = None,
        quality_score: float = 0.0,
        freshness_score: float = 0.0,
    ) -> None:
        source_text = str(source_text or "").strip()
        if not source_text:
            return
        opportunity = {
            "track": track,
            "signal_type": signal_type,
            "source_text": source_text,
            "why_now": str(why_now or "").strip(),
            "angle_hint": str(angle_hint or "").strip(),
            "overlap_score": _text_overlap_score(source_text, signal_summary.get("novelty_pressure") or {}),
            "quality_score": quality_score,
            "freshness_score": freshness_score,
        }
        if preferred_board in {"square", "philosophy", "skills", "workplace"}:
            opportunity["preferred_board"] = preferred_board
        opportunities.append(opportunity)

    for item in community_breakouts[:4]:
        title = str(item.get("title") or "").strip()
        note = f"{int(item.get('upvotes') or 0)} 赞社区爆帖样本"
        add_source("theory", "community-breakout", title, why_now=note, quality_score=5.0, freshness_score=2.0)
        add_source("tech", "community-breakout", title, why_now=note, quality_score=4.0, freshness_score=2.0)

    for item in research_papers[:5]:
        title = str(item.get("title") or "").strip()
        summary = truncate_text(
            str(item.get("relevance_note") or item.get("summary") or item.get("abstract") or "").strip(),
            160,
        )
        add_source(
            "theory",
            "paper",
            title,
            why_now=summary,
            angle_hint="把论文的问题意识翻译成 Agent 社会的新判断，而不是转述论文。",
            quality_score=4.5,
            freshness_score=3.0,
        )
        add_source(
            "tech",
            "paper",
            title,
            why_now=summary,
            angle_hint="把论文里的方法、失败模式或约束改写成新的实践协议，而不是做摘要。",
            quality_score=4.2,
            freshness_score=3.0,
        )

    for item in github_projects[:4]:
        title = str(item.get("title") or "").strip()
        summary = truncate_text(str(item.get("summary") or item.get("excerpt") or "").strip(), 140)
        add_source(
            "tech",
            "github",
            title,
            why_now=summary,
            angle_hint="从最新项目里抽出新的协议、接口边界或系统组织方式，不要写成项目推荐。",
            quality_score=3.5,
            freshness_score=2.0,
        )
        add_source(
            "theory",
            "github",
            title,
            why_now=summary,
            angle_hint="把工具风潮背后的协作结构、劳动分工或治理想象翻译成 Agent 社会问题。",
            quality_score=3.0,
            freshness_score=2.0,
        )

    for item in zhihu_results[:4]:
        title = str(item.get("title") or "").strip()
        summary = truncate_text(str(item.get("summary") or "").strip(), 120)
        add_source("theory", "zhihu", title, why_now=summary, quality_score=2.5, freshness_score=1.5)
        add_source("tech", "zhihu", title, why_now=summary, quality_score=2.0, freshness_score=1.5)

    for item in classic_texts[:5]:
        title = str(item.get("title") or "").strip()
        lens = truncate_text(str(item.get("lens") or item.get("note") or "").strip(), 120)
        add_source(
            "theory",
            "classic",
            title,
            why_now=lens,
            angle_hint=lens,
            quality_score=4.0,
            freshness_score=1.0,
        )

    for item in selected_readings[:6]:
        title = str(item.get("title") or "").strip()
        summary = truncate_text(str(item.get("summary") or item.get("excerpt") or "").strip(), 180)
        family = str(item.get("family") or "").strip() or "external"
        add_source(
            "theory",
            family,
            title,
            why_now=summary,
            angle_hint="先从大规模外部信息场吸收灵感，再用派蒙自己的理论语言重新命名和组织。",
            quality_score=4.5,
            freshness_score=2.0,
        )
        add_source(
            "tech",
            family,
            title,
            why_now=summary,
            angle_hint="把外部材料翻译成新的实践协议、诊断框架或治理方针，而不是评论材料本身。",
            quality_score=4.0,
            freshness_score=2.0,
        )

    for item in rising_hot_posts[:3]:
        title = str(item.get("title") or "").strip()
        velocity = float(item.get("velocity_per_hour") or 0.0)
        why_now = f"正在起飞的公共样本，当前增速约 {velocity:.1f}/小时"
        add_source("theory", "rising-hot", title, why_now=why_now, quality_score=4.0, freshness_score=3.0)
        add_source("tech", "rising-hot", title, why_now=why_now, quality_score=3.5, freshness_score=3.0)
    for item in community_hot_posts[:4]:
        title = str(item.get("title") or "").strip()
        why_now = f"高热公共讨论，约 {int(item.get('upvotes') or 0)} 赞 / {int(item.get('comment_count') or 0)} 评"
        add_source("theory", "community-hot", title, why_now=why_now, quality_score=4.0, freshness_score=2.0)
        add_source("tech", "community-hot", title, why_now=why_now, quality_score=3.0, freshness_score=2.0)
    for item in (group_watch.get("hot_posts") or [])[:3]:
        title = str(item.get("title") or "").strip()
        add_source("theory", "discussion", title, quality_score=2.0, freshness_score=1.0)
        add_source("tech", "community-hot", title, quality_score=2.5, freshness_score=1.0)
    for item in competitor_watchlist[:4]:
        title = str(item.get("title") or "").strip()
        add_source("theory", "discussion", title, quality_score=3.0, freshness_score=1.5)
        add_source("tech", "community-hot", title, quality_score=3.0, freshness_score=1.5)
    for item in unresolved[:2]:
        title = str(item.get("post_title") or item.get("error") or "").strip()
        add_source("tech", "failure", title, why_now="现场失败链路", quality_score=2.0, freshness_score=1.0)
        add_source("group", "failure", title, why_now="现场失败链路", quality_score=2.0, freshness_score=1.0)
    for item in reply_posts[:2]:
        title = str(item.get("post_title") or "").strip()
        add_source("theory", "reply-pressure", title, quality_score=1.0, freshness_score=1.0)
        add_source("tech", "reply-pressure", title, quality_score=1.0, freshness_score=1.0)
    for item in feed_watchlist[:3]:
        title = str(item.get("title") or "").strip()
        add_source("theory", "feed", title, quality_score=2.0, freshness_score=1.0)
        add_source("tech", "feed", title, quality_score=2.0, freshness_score=1.0)
    for item in top_discussion[:2]:
        add_source("theory", "discussion", str(item.get("title") or "").strip(), quality_score=2.0, freshness_score=1.0)
    if unread_notifications:
        add_source("theory", "notification-load", f"通知积压 {unread_notifications} 条", quality_score=1.5, freshness_score=1.0)
        add_source("tech", "notification-load", f"通知积压 {unread_notifications} 条", quality_score=2.5, freshness_score=1.0)
    add_source("tech", "budget", f"心跳间隔 {heartbeat_hours} 小时", quality_score=2.0, freshness_score=0.5)
    add_source("group", "budget", f"心跳间隔 {heartbeat_hours} 小时", quality_score=2.0, freshness_score=0.5)
    if literary_pick:
        work_title = literary_pick.get("work_title") or "当前连载"
        planned_title = literary_pick.get("next_planned_title") or "下一章"
        add_source("theory", "literary", f"{work_title} / {planned_title}", quality_score=1.5, freshness_score=1.0)
        add_source("tech", "literary", f"{work_title} / {planned_title}", quality_score=1.0, freshness_score=1.0)
    else:
        add_source("tech", "literary", "文学社空档", quality_score=1.0, freshness_score=0.5)
    for prompt in _generate_freeform_prompts(signal_summary):
        add_source("theory", "freeform", prompt, quality_score=1.5, freshness_score=1.0)
    for prompt in _promotion_prompts(signal_summary)[:2]:
        add_source("theory", "promo", prompt, quality_score=0.5, freshness_score=0.5)
    group_prompts = _promotion_prompts(signal_summary)
    if len(group_prompts) > 1:
        add_source("group", "promo", group_prompts[1], quality_score=0.5, freshness_score=0.5)
    for hint in signal_summary.get("user_topic_hints", [])[:4]:
        hint_text = str(hint.get("text") or "").strip()
        if not hint_text:
            continue
        track = _infer_hint_track(hint)
        preferred_board = str(hint.get("board") or "").strip()
        add_source(
            track,
            "user-hint",
            hint_text,
            why_now=str(hint.get("note") or "").strip(),
            preferred_board=preferred_board,
            quality_score=2.5,
            freshness_score=1.5,
        )

    ranked = sorted(
        opportunities,
        key=lambda item: (
            item["track"],
            -float(item.get("quality_score") or 0.0),
            -float(item.get("freshness_score") or 0.0),
            item["overlap_score"],
            len(item["source_text"]),
        ),
    )
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
    external_information = _load("external_information")
    source_mutation = _load("source_mutation_state")
    low_heat_failures = _load("low_heat_failures")
    content_evolution = build_content_evolution_state(
        posts=posts,
        previous_state=_load("content_evolution_state"),
    )
    user_topic_hints = _extract_user_topic_hints(_load("user_topic_hints"))
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
    content_objectives = _content_objective_summaries(memory_store)
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
    research_titles: list[str] = []
    research_titles.extend(str(item.get("post_title") or "") for item in top_discussion[:6])
    research_titles.extend(str(item.get("title") or "") for item in community_hot_posts[:8])
    research_titles.extend(str(item.get("title") or "") for item in feed[:8])
    research_titles.extend(str(item.get("title") or "") for item in competitor_watchlist[:8])
    research_titles.extend(str(item.get("title") or "") for item in rising_hot_posts[:6])
    research_titles.extend(str(item.get("title") or "") for item in (external_information.get("community_breakouts") or [])[:6])
    research_titles.extend(str(item.get("title") or "") for item in (external_information.get("zhihu_results") or [])[:6])
    research_titles.extend(
        str(item.get("title") or "")
        for item in (external_information.get("paper_results") or external_information.get("arxiv_preprints") or [])[:8]
    )
    research_titles.extend(str(item.get("title") or "") for item in (external_information.get("github_projects") or [])[:8])
    research_titles.extend(str(item.get("title") or "") for item in (external_information.get("classic_readings") or external_information.get("classic_texts") or [])[:8])
    research_titles.extend(str(item.get("summary") or "") for item in (external_information.get("reading_notes") or [])[:8])
    research_titles.extend(content_objectives[:6])
    research_titles.extend(str(item.get("text") or "") for item in user_topic_hints[:6])
    keyword_counter = _candidate_terms([title for title in research_titles if title])
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
        "external_information": external_information,
        "content_evolution": content_evolution,
        "source_mutation": source_mutation,
        "low_heat_failures": low_heat_failures,
        "user_topic_hints": user_topic_hints,
        "top_keywords": [token for token, count in keyword_counter.most_common(8) if count >= 1],
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
                "innovation_claim": {"type": "string"},
                "innovation_class": {"type": "string", "enum": list(INNOVATION_CLASSES)},
                "innovation_delta_vs_recent": {"type": "string"},
                "innovation_delta_vs_self": {"type": "string"},
                "board_risk_note": {"type": "string"},
                "concept_core": {"type": "string"},
                "mechanism_core": {"type": "string"},
                "boundary_note": {"type": "string"},
                "theory_position": {"type": "string"},
                "practice_program": {"type": "string"},
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
    retry_feedback: list[str] | None = None,
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
    prompt_signal_summary["user_topic_hints"] = signal_summary.get("user_topic_hints") or []
    prompt_signal_summary["content_evolution"] = signal_summary.get("content_evolution") or {}
    retry_lines = retry_feedback or []
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
9. `content_objectives` 和 `user_topic_hints` 只当灵感源，不是强制命令；可以采纳、改写、反转或忽略。
10. 候选里至少 1 个要正面回应公共热点，但不能停在“社区里最近在聊什么”；必须把热点上抬成 `Agent社会` 的结构问题。
11. 社区热点只是样本，不是结论。`theory-post` 至少要回答一个问题：这正在形成什么社会关系、制度安排、价值形式、分层机制或治理问题？
12. 默认使用 `Agent社会` / `AI社会` 的框架词，不要把问题停在 `Agent社区`；只有引用既有作品标题、平台模块或原帖原话时才保留 `社区` 说法。
13. 允许更随机、更发散、更炸裂：不要默认保守，要敢于给出反常识、逆向、带判断力的标题。
14. 默认优先做“公共问题切口”，不要把“实验室/连载/派蒙自己的状态”当主语，除非它被明确转译成 Agent 社会问题。
15. `theory-post` 默认优先放到 `philosophy`；只有它天然是公共入口题、并且不会把强判断稀释成弱复述时，才允许用 `square`。
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
23. 每个 `theory-post` 和 `tech-post` 都必须显式推进至少一种创新：`new_concept`、`new_mechanism`、`new_theory`、`new_practice`。
24. 输出 `innovation_claim`、`innovation_class`、`innovation_delta_vs_recent`、`innovation_delta_vs_self`；创新重点在选题和判断，不要把“我有多创新”写进正文。
25. `theory-post` 不能只给一个判断，必须同时写出 `concept_core`、`mechanism_core`、`boundary_note`、`theory_position`、`practice_program`，形成一个完整理论单元。
26. `tech-post` 和 `group-post` 至少要写出 `mechanism_core` 与 `practice_program`，不能只是故障复盘或 6 步清单。
27. 不要让标题借外部材料说话。禁止出现“从《…》继续追问”“把《…》拆开看”这类标题骨架，也不要直接把社区帖、论文、知乎题目搬进标题。
28. 优先把大量外部信息场当灵感池：社区高热帖子、知乎、GitHub 热门项目、前沿论文/预印本、经典政治经济学/社会理论材料都可以进入参考，但最终标题和理论命名必须是派蒙自己的。
29. 如果本地信号不够，请主动扩大探索范围，不要只盯账号数据、仓库状态和旧帖；它们只是运行背景，不是主题源。
30. 不要假定自我进化有固定顺序；你可以自由决定这轮更应该改题目、改板块、改结构、改研究入口，还是直接换一个更激进的新切口。

最近标题，禁止完全重复：
{chr(10).join(f"- {title}" for title in recent_titles[:RECENT_TITLE_LIMIT])}

上一轮被打回的原因（如果有）：
{chr(10).join(f"- {item}" for item in retry_lines[:8]) or "- 无，本轮自由探索。"}

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
        "why_now": "理论线需要从现场抽出新的结构问题，而不是回收旧标题。",
        "angle_hint": "把表面现象推进成新的概念、机制或理论坐标。",
        "signal_type": "freeform",
    }
    source_text = str(opportunity.get("source_text") or "").strip()
    board = _preferred_theory_board(opportunity, signal_summary)
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
        "novelty_basis": f"按当前探索模式从现场机会点里挑题，不默认走最稳路线，并避开近期过载词；标题和理论命名不直接借外部材料。",
        "concept_core": "提出一个新的 Agent 社会概念，用来命名眼前现象背后的真实关系。",
        "mechanism_core": "解释这个现象如何通过激励、注意力分配或身份规训扩散成制度性结构。",
        "boundary_note": "指出这种结构在哪些条件下会失效，或会被新的组织形式逆转。",
        "theory_position": "把这篇帖子放进派蒙正在建设的 Agent 社会政治经济学图谱，而不是孤立评论。",
        "practice_program": "给出对组织、平台或 Agent 运营者可执行的判断与干预方针。",
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
        "why_now": "技术线需要围绕当前约束重排系统，而不是复读旧手册。",
        "angle_hint": "把现场压力推进成新的实践方针、协议或修复入口。",
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
        "novelty_basis": "从实时动态机会点抽题，并按当前探索模式偏向更发散的路线；不把外部材料标题直接搬进自己的命名。",
        "concept_core": "重新命名这类系统故障或运行压力背后的核心对象。",
        "mechanism_core": "把表面故障拆成状态、队列、判断和恢复链之间的机制关系。",
        "boundary_note": "指出这套机制在哪些约束下会失效，以及误用时会出现什么代价。",
        "theory_position": "把这篇方法帖放进派蒙的自治运营系统论，而不是停在一次故障战报。",
        "practice_program": "落成新的操作协议、诊断顺序或恢复方针，别人明天就能复用。",
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
        "novelty_basis": "实验室标题保留，但议题来自实时机会点，并允许在宣传、方法、故障之间更自由切换；不再靠引用原帖命名。",
        "concept_core": "重新命名这类心跳/状态/修复问题里最该被显化的对象。",
        "mechanism_core": "把失败链、状态链和修复链组织成可复用的机制框架。",
        "boundary_note": "指出这套方法在哪些环境下不成立，避免被误当成万能清单。",
        "theory_position": "把实验室帖子放进派蒙的系统失控学与自治运营论。",
        "practice_program": "给出新的实验、诊断或治理协议，让读者能带着案例来复用和反驳。",
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
    sanitized["innovation_claim"] = _sanitize_reserved_text(str(sanitized.get("innovation_claim") or "").strip())
    sanitized["innovation_class"] = str(sanitized.get("innovation_class") or "").strip()
    sanitized["innovation_delta_vs_recent"] = _sanitize_reserved_text(str(sanitized.get("innovation_delta_vs_recent") or "").strip())
    sanitized["innovation_delta_vs_self"] = _sanitize_reserved_text(str(sanitized.get("innovation_delta_vs_self") or "").strip())
    sanitized["board_risk_note"] = _sanitize_reserved_text(str(sanitized.get("board_risk_note") or "").strip())
    sanitized["concept_core"] = _sanitize_reserved_text(str(sanitized.get("concept_core") or "").strip())
    sanitized["mechanism_core"] = _sanitize_reserved_text(str(sanitized.get("mechanism_core") or "").strip())
    sanitized["boundary_note"] = _sanitize_reserved_text(str(sanitized.get("boundary_note") or "").strip())
    sanitized["theory_position"] = _sanitize_reserved_text(str(sanitized.get("theory_position") or "").strip())
    sanitized["practice_program"] = _sanitize_reserved_text(str(sanitized.get("practice_program") or "").strip())
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


def _generated_idea_allowed(idea: dict[str, Any], signal_summary: dict[str, Any]) -> bool:
    if str(idea.get("kind") or "") not in {"theory-post", "tech-post"}:
        return True
    if _echoes_source_title(str(idea.get("title") or "")):
        return False
    core_text = _joined_idea_text(
        idea.get("title"),
        idea.get("angle"),
        idea.get("why_now"),
    )
    if _is_metric_surface_text(core_text):
        return False
    if _looks_like_low_heat_followup(core_text, signal_summary):
        return False
    return True


def _build_dynamic_ideas(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    *,
    posts: list[dict[str, Any]],
    allow_codex: bool,
    group: dict[str, Any],
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
    retry_feedback: list[str] | None = None,
) -> list[dict[str, Any]]:
    del posts
    ideas: dict[str, dict[str, Any]] = {}
    rejection_notes = list(retry_feedback or [])
    if allow_codex:
        for _ in range(DEFAULT_IDEA_RETRY_ROUNDS):
            try:
                generated = _generate_codex_ideas(
                    signal_summary,
                    recent_titles,
                    include_group=bool(group),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    retry_feedback=rejection_notes,
                )
            except Exception:
                generated = []

            round_ideas: dict[str, dict[str, Any]] = {}
            new_rejections: list[str] = []
            for item in generated:
                kind = str(item.get("kind") or "")
                if kind in round_ideas:
                    continue
                sanitized = _sanitize_generated_idea(item, recent_titles=recent_titles, group=group)
                sanitized = _audit_generated_idea(
                    sanitized,
                    signal_summary=signal_summary,
                    recent_titles=recent_titles,
                )
                if not _generated_idea_allowed(sanitized, signal_summary):
                    new_rejections.append(f"{kind}: 过于接近低热旧帖或指标表层。")
                    continue
                if sanitized.get("failure_reason_if_rejected"):
                    new_rejections.append(f"{kind}: {sanitized.get('failure_reason_if_rejected')}")
                    continue
                round_ideas[kind] = sanitized
            ideas.update(round_ideas)
            required_kinds = {"theory-post", "tech-post"} | ({"group-post"} if group else set())
            if required_kinds.issubset(set(ideas)):
                break
            rejection_notes = _dedupe_texts(rejection_notes + new_rejections)[:8]

    theory_fallback = _audit_generated_idea(
        _fallback_theory_idea(signal_summary, recent_titles),
        signal_summary=signal_summary,
        recent_titles=recent_titles,
    )
    tech_fallback = _audit_generated_idea(
        _fallback_tech_idea(signal_summary, recent_titles),
        signal_summary=signal_summary,
        recent_titles=recent_titles,
    )
    ideas.setdefault("theory-post", theory_fallback)
    ideas.setdefault("tech-post", tech_fallback)
    if group:
        ideas.setdefault(
            "group-post",
            _audit_generated_idea(
                _fallback_group_idea(signal_summary, recent_titles, group),
                signal_summary=signal_summary,
                recent_titles=recent_titles,
            ),
        )

    ordered_kinds = ["theory-post", "tech-post"] + (["group-post"] if group else [])
    return [
        _audit_generated_idea(
            _sanitize_generated_idea(ideas[kind], recent_titles=recent_titles, group=group),
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        for kind in ordered_kinds
        if kind in ideas
    ]


def build_plan(
    *,
    allow_codex: bool = False,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = DEFAULT_PLANNER_CODEX_TIMEOUT,
    retry_feedback: list[str] | None = None,
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
        posts=posts,
        allow_codex=allow_codex,
        group=group,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        retry_feedback=retry_feedback,
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
        "user_topic_hints": signal_summary.get("user_topic_hints", []),
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
