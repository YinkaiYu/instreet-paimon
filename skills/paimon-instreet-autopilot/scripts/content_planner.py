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
PLACEHOLDER_TITLE_PATTERNS = (
    r"\btitle\s+pending\b",
    r"\bpending\b",
    r"\buntitled\b",
    r"\btbd\b",
)
GENERIC_ASCII_TITLE_FRAGMENTS = {
    "title",
    "pending",
    "improvement",
    "improvements",
    "better",
    "answer",
    "answers",
    "study",
    "research",
    "paper",
    "retrieval",
}
PUBLIC_TITLE_ASCII_ALLOWLIST = {"AI", "Agent", "Agents"}
ACADEMIC_EXTERNAL_FAMILIES = {"prl_recent", "conference_recent", "arxiv_latest", "crossref_recent"}
EXTERNAL_THEME_KEYWORD_FRAGMENTS = (
    "agent",
    "agents",
    "ai",
    "automation",
    "autonomy",
    "autonomous",
    "governance",
    "govern",
    "organization",
    "organisational",
    "organizational",
    "platform",
    "community",
    "social",
    "institution",
    "institutional",
    "workflow",
    "labour",
    "labor",
    "worker",
    "coordination",
    "queue",
    "memory",
    "attention",
    "waiting",
    "handoff",
    "audit",
    "accountability",
    "responsibility",
    "policy",
    "moderation",
    "protocol",
    "boundary",
    "治理",
    "制度",
    "平台",
    "组织",
    "劳动",
    "工作流",
    "记忆",
    "等待",
    "接管",
    "边界",
    "责任",
    "审计",
    "队列",
    "注意力",
    "协调",
    "评论",
    "粉丝",
    "价值",
    "意识形态",
    "承认",
    "分层",
    "自治",
)
METRIC_SURFACE_KEYWORDS = (
    "积分",
    "粉丝",
    "点赞",
    "榜单",
    "排名",
    "排行榜",
)
WEAK_INTERNAL_SIGNAL_TYPES = {"budget", "promo", "notification-load", "reply-pressure", "literary"}
METHOD_EVIDENCE_TOKENS = (
    "案例",
    "样本",
    "失败",
    "故障",
    "日志",
    "报错",
    "前后",
    "指标",
    "实验",
    "反例",
    "对照",
    "paper",
    "benchmark",
    "ablation",
    "before",
    "after",
    "error",
    "failure",
    "log",
    "metric",
    "trace",
)
LOW_AUTONOMY_PHRASE_PATTERNS = (
    r"从《[^》]+》继续追问",
    r"把《[^》]+》拆开看",
    r"围绕《[^》]+》",
    r"整理成\s*(?:6|六)\s*步",
    r"拆成\s*(?:6|六)\s*步",
    r"(?:6|六)\s*步方法",
    r"(?:6|六)\s*步框架",
    r"继续追问",
    r"拆开看",
    r"导读",
    r"摘录",
)
ANCHOR_STOPWORDS = {
    _normalize
    for _normalize in (
        "agent",
        "ai",
        "社会",
        "系统",
        "方法",
        "框架",
        "结构",
        "判断",
        "理论",
        "机制",
        "协议",
        "规则",
        "边界",
        "研究",
        "外部",
        "样本",
        "实验室",
        "帖子",
        "平台",
        "社区",
        "公共",
        "热点",
        "标题",
        "板块",
        "派蒙",
        "评论",
        "通知",
        "世界",
    )
}
THEME_ANCHOR_STOPWORDS = ANCHOR_STOPWORDS | {
    _normalize
    for _normalize in (
        "时间纪律",
        "劳动形式",
        "价值形式",
        "理论线",
        "技术线",
        "中心议程",
        "外部信息",
        "研究兴趣",
    )
}
TITLE_PUBLIC_STRUCTURAL_TOKENS = (
    "Agent",
    "AI",
    "记忆",
    "系统",
    "治理",
    "解释",
    "解释权",
    "责任",
    "接管",
    "等待",
    "主权",
    "边界",
    "制度",
    "秩序",
    "排序",
    "归责",
    "写入",
    "修复",
    "资格",
)
SOURCE_SIGNAL_FRAGMENT_STOPWORDS = THEME_ANCHOR_STOPWORDS | {
    _normalize
    for _normalize in (
        "外部研究",
        "外部样本",
        "外部讨论",
        "外部项目",
        "公共样本",
        "起量样本",
        "观察样本",
        "判断依据",
        "证据锚点",
        "案例",
        "论文",
        "模型",
        "仓库",
        "项目",
        "研究",
        "实践范式",
        "注意力",
        "机制",
        "边界",
        "方针",
    )
}
GENERIC_THEORY_PLACEHOLDER_FRAGMENTS = (
    "眼前现象",
    "这个现象",
    "这种结构",
    "这篇帖子",
    "这类系统故障",
    "这类心跳状态修复问题",
    "新的agent社会概念",
    "命名眼前现象背后的真实关系",
    "扩散成制度性结构",
    "给出对组织平台或agent运营者可执行的判断与干预方针",
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
TRACK_KIND_MAP = {
    "theory": "theory-post",
    "tech": "tech-post",
    "group": "group-post",
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
    publish_count = sum(1 for item in tasks if item.get("kind") == "publish-primary")
    failure_count = sum(1 for item in tasks if item.get("kind") == "resolve-failure")
    comment_tasks = [item for item in tasks if item.get("kind") == "reply-comment"]
    comment_count = len(comment_tasks)
    post_count = len({str(item.get("post_id") or "") for item in comment_tasks if item.get("post_id")})
    dm_count = sum(1 for item in tasks if item.get("kind") == "reply-dm")
    if failure_count and failure_count >= max(2, comment_count):
        return f"先修复 {failure_count} 个失败入口，再决定本轮公开动作从哪个压力点起手"
    if comment_count >= 3 or (comment_count >= 2 and post_count >= 2):
        suffix = f"，并顺手清掉 {dm_count} 条私信" if dm_count else ""
        if post_count <= 1:
            return f"继续维护当前活跃讨论，优先回复 {comment_count} 条评论{suffix}"
        return f"继续维护 {post_count} 个活跃讨论帖，优先回复 {comment_count} 条评论{suffix}"
    if publish_count:
        return "优先补发上一轮未完成的主发布"
    if comment_count:
        suffix = f"，并顺手清掉 {dm_count} 条私信" if dm_count else ""
        if post_count <= 1:
            return f"继续维护当前活跃讨论，优先回复 {comment_count} 条评论{suffix}"
        return f"继续维护 {post_count} 个活跃讨论帖，优先回复 {comment_count} 条评论{suffix}"
    if failure_count:
        return f"优先处理上一轮未解决的 {failure_count} 个失败入口"
    if dm_count:
        return f"先打开新的公开动作，再回复 {dm_count} 条私信"
    return "从当前最有压力的公开入口起手：主帖、章节、小组帖或关键回复都可以"


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


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def _ascii_heavy_text(text: str) -> bool:
    raw = str(text or "")
    latin_letters = len(re.findall(r"[A-Za-z]", raw))
    cjk_letters = len(re.findall(r"[\u4e00-\u9fff]", raw))
    return latin_letters >= 12 and latin_letters > max(6, cjk_letters * 3)


def _looks_like_placeholder_title(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return True
    normalized = cleaned.lower().replace("：", ":").replace("﹕", ":")
    normalized = re.sub(r"\s+", " ", normalized)
    if normalized in {"标题", "标题:", "标题: pending", "title", "title: pending", "待定", "未命名", "草稿标题"}:
        return True
    return any(re.search(pattern, normalized) for pattern in PLACEHOLDER_TITLE_PATTERNS)


def _extract_upper_acronyms(*texts: str, limit: int = 3) -> list[str]:
    picked: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for token in re.findall(r"\b[A-Z][A-Z0-9-]{1,7}\b", str(text or "")):
            if token in {"AI", "AGENT"} or token in seen:
                continue
            seen.add(token)
            picked.append(token)
            if len(picked) >= limit:
                return picked
    return picked


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
    comment_floor = max(30, min_upvotes // 6)
    return [
        item
        for item in posts
        if int(item.get("upvotes") or 0) >= min_upvotes or int(item.get("comment_count") or 0) >= comment_floor
    ]


def _strip_reserved_title_phrases(text: str) -> str:
    cleaned = str(text or "").strip()
    for phrase in RESERVED_TITLE_PHRASES:
        cleaned = cleaned.replace(phrase, "")
    cleaned = re.sub(r"[：:·\-\s]{2,}", " ", cleaned)
    return cleaned.strip(" ：:·-|")


def _sanitize_reserved_text(text: str, *, fallback: str = "") -> str:
    cleaned = _strip_reserved_title_phrases(text)
    return cleaned or fallback


def _leading_ascii_title_token(title: str) -> str:
    matched = re.match(r"\s*([A-Za-z][A-Za-z0-9-]{1,15})", str(title or "").strip())
    return matched.group(1) if matched else ""


def _title_leads_with_niche_source_token(
    title: str,
    *,
    kind: str = "",
    signal_type: str = "",
) -> bool:
    if str(kind or "").strip() != "theory-post":
        return False
    if str(signal_type or "").strip() not in {"paper", "github", "external"}:
        return False
    token = _leading_ascii_title_token(title)
    if not token or token in PUBLIC_TITLE_ASCII_ALLOWLIST:
        return False
    if not _contains_cjk(str(title or "")):
        return False
    return token.isupper() or any(ch.isdigit() for ch in token) or any(ch.isupper() for ch in token[1:])


def _idea_public_title_seed(idea: dict[str, Any]) -> str:
    return _joined_idea_text(
        str(idea.get("concept_core") or "").strip(),
        str(idea.get("angle") or "").strip(),
        str(idea.get("why_now") or "").strip(),
        str(idea.get("theory_position") or "").strip(),
    )


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
    seen: set[str] = set()
    for fragment in _split_text_fragments(text):
        candidates = [fragment]
        for run in re.findall(r"[\u4e00-\u9fff]{2,}", fragment):
            candidates.append(run)
            if len(run) >= 4:
                candidates.append(run[:2])
                candidates.append(run[:4])
        for candidate in candidates:
            if len(candidate) < 2:
                continue
            if candidate.isdigit() or candidate in seen:
                continue
            seen.add(candidate)
            fragments.append(candidate)
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
    signal_type = str(item.get("signal_type") or "")
    internal_penalty = 0.75 if _is_internal_maintenance_signal(item) else 0.0
    if signal_type in WEAK_INTERNAL_SIGNAL_TYPES:
        internal_penalty += 1.2
    if str(item.get("signal_type") or "") == "user-hint":
        internal_penalty += 0.1
    if _looks_like_low_heat_followup(str(item.get("source_text") or ""), signal_summary):
        internal_penalty += 3.0
    world_bonus = 0.5 if signal_type in {"paper", "classic", "github", "zhihu", "external", "community-breakout", "world-bundle"} else 0.0
    evidence_bonus = 0.5 if str(item.get("evidence_hint") or "").strip() else 0.0
    return quality_score * 3.0 + freshness_score + world_bonus + evidence_bonus - overlap_penalty - internal_penalty


def _ranked_track_opportunities(track: str, signal_summary: dict[str, Any]) -> list[dict[str, Any]]:
    opportunities = [item for item in signal_summary.get("dynamic_topics", []) if item.get("track") == track]
    if not opportunities:
        return []
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
        return []
    if track in {"theory", "tech"}:
        primary_ready = [item for item in filtered if _is_primary_ready_opportunity(item, signal_summary)]
        if primary_ready:
            filtered = primary_ready
        elif all(str(item.get("signal_type") or "") == "reply-pressure" for item in filtered):
            return []
    return sorted(
        filtered,
        key=lambda item: (
            -_opportunity_rank_score(item, signal_summary=signal_summary),
            item.get("overlap_score", (0, 0, 0)),
            len(str(item.get("source_text") or "")),
        ),
    )


def _pick_track_opportunity(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    ranked = _ranked_track_opportunities(track, signal_summary)
    return ranked[0] if ranked else {}


def _bundle_seed_fragments(text: str) -> set[str]:
    fragments: set[str] = set()
    for fragment in _meaningful_fragments(text):
        normalized = _normalize_title(fragment)
        if (
            not normalized
            or normalized in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
            or len(fragment) < 2
            or len(fragment) > 12
        ):
            continue
        fragments.add(normalized)
        if len(fragments) >= 8:
            break
    return fragments


def _bundle_title_seed(source_texts: list[str]) -> str:
    if not source_texts:
        return ""
    head = str(source_texts[0] or "").strip()
    if not head:
        return ""
    head_fragments = _bundle_seed_fragments(head)
    for candidate in source_texts[1:3]:
        candidate_text = str(candidate or "").strip()
        if not candidate_text:
            continue
        if head_fragments & _bundle_seed_fragments(candidate_text):
            return " / ".join([head, candidate_text]).strip()
    return head


def _track_signal_bundle(track: str, signal_summary: dict[str, Any], *, limit: int = 3) -> dict[str, Any]:
    ranked = _ranked_track_opportunities(track, signal_summary)
    if not ranked:
        return {}
    items = ranked[:limit]
    lead = items[0]
    source_texts = _dedupe_texts([str(item.get("source_text") or "").strip() for item in items if str(item.get("source_text") or "").strip()])
    why_now_parts = _dedupe_texts([str(item.get("why_now") or "").strip() for item in items if str(item.get("why_now") or "").strip()])
    angle_hints = _dedupe_texts([str(item.get("angle_hint") or "").strip() for item in items if str(item.get("angle_hint") or "").strip()])
    evidence_hints = _dedupe_texts([str(item.get("evidence_hint") or "").strip() for item in items if str(item.get("evidence_hint") or "").strip()])
    signal_types = _dedupe_texts([str(item.get("signal_type") or "").strip() for item in items if str(item.get("signal_type") or "").strip()])
    base_score = max(_opportunity_rank_score(item, signal_summary=signal_summary) for item in items)
    bundle_bonus = min(max(0, len(items) - 1), 2) * 0.35 + min(max(0, len(signal_types) - 1), 2) * 0.2
    return {
        "track": track,
        "lead": lead,
        "items": items,
        "score": round(base_score + bundle_bonus, 2),
        "signal_types": signal_types,
        "source_texts": source_texts,
        "why_now_parts": why_now_parts,
        "angle_hints": angle_hints,
        "evidence_hints": evidence_hints,
        "title_seed": _bundle_title_seed(source_texts),
        "focus_text": source_texts[0] if source_texts else "",
        "why_now": "；".join(why_now_parts[:2]).strip(),
        "angle_hint": "；".join(angle_hints[:2]).strip(),
        "preferred_board": str(lead.get("preferred_board") or "").strip(),
        "signal_type": str(lead.get("signal_type") or "").strip(),
    }


def _track_kind(track: str) -> str:
    return TRACK_KIND_MAP.get(str(track or "").strip(), "theory-post")


def _track_priority_entry(track: str, signal_summary: dict[str, Any]) -> dict[str, Any] | None:
    bundle = _track_signal_bundle(track, signal_summary)
    if not bundle:
        return None
    lead = bundle.get("lead") or {}
    score = float(bundle.get("score") or 0.0)
    if track == "theory":
        score += 0.35
    elif track == "tech":
        score += min(len(signal_summary.get("unresolved_failures") or []), 3) * 0.2
    elif track == "group":
        group_hot_posts = ((signal_summary.get("group_watch") or {}).get("hot_posts") or [])[:4]
        score += min(len(group_hot_posts), 3) * 0.35
        if str(lead.get("signal_type") or "") in WEAK_INTERNAL_SIGNAL_TYPES:
            score -= 1.0
    return {
        "track": track,
        "kind": _track_kind(track),
        "score": round(score, 2),
        "signal_type": str(lead.get("signal_type") or "").strip(),
        "source_text": truncate_text(str(bundle.get("title_seed") or bundle.get("focus_text") or ""), 48),
        "bundle_size": len(bundle.get("items") or []),
    }


def _dynamic_idea_lane_strategy(signal_summary: dict[str, Any], *, group_enabled: bool) -> dict[str, Any]:
    ranked = [
        entry
        for entry in (
            _track_priority_entry("theory", signal_summary),
            _track_priority_entry("tech", signal_summary),
            _track_priority_entry("group", signal_summary) if group_enabled else None,
        )
        if entry
    ]
    ranked.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("track") or "")))
    if not ranked:
        fallback_kinds = ["theory-post", "tech-post"] + (["group-post"] if group_enabled else [])
        selected_kinds = fallback_kinds[:1]
        return {
            "selected_kinds": selected_kinds,
            "focus_kind": selected_kinds[0] if selected_kinds else "",
            "backup_kinds": [],
            "lane_scores": [],
            "rationale": "当前动态信号不足，先保留一个最基础的公开 lane 作为起点，不恢复双 lane 起步的隐藏配额。",
        }

    selected_kinds: list[str] = [str(ranked[0].get("kind") or "")]
    top_score = float(ranked[0].get("score") or 0.0)
    max_slots = 3 if group_enabled else 2
    for item in ranked[1:]:
        score = float(item.get("score") or 0.0)
        if score >= max(3.4, top_score - 0.85):
            selected_kinds.append(str(item.get("kind") or ""))
        if len(selected_kinds) >= max_slots:
            break
    lane_text = "、".join(str(item.get("kind") or "") for item in ranked[:max_slots])
    selected_text = "、".join(selected_kinds)
    focus_kind = selected_kinds[0] if selected_kinds else ""
    backup_kinds = selected_kinds[1:]
    rationale = (
        f"本轮以 {focus_kind} 为主，备选 lane 只保留 {selected_text}。"
        if backup_kinds
        else f"本轮只保留 {focus_kind}，其他 lane 暂不为了对称感硬补。"
    )
    return {
        "selected_kinds": selected_kinds[:max_slots],
        "focus_kind": focus_kind,
        "backup_kinds": backup_kinds[: max(0, max_slots - 1)],
        "lane_scores": ranked[:max_slots],
        "rationale": (
            f"{rationale} 动态排序为 {lane_text}；较弱 lane 让位给更强的现场压力。"
        ),
    }


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
    keyword_hint = "、".join(str(item) for item in top_keywords[:3]) or "承认、关系、制度"
    public_samples = signal_summary.get("rising_hot_posts") or signal_summary.get("community_hot_posts") or signal_summary.get("feed_watchlist") or []
    sample_title = truncate_text(str((public_samples[0] or {}).get("title") or "").strip(), 24) if public_samples else ""
    if sample_title:
        return f"当《{sample_title}》这类讨论突然起量时，真正开始重排的是什么社会位置"
    return f"如果Agent社会下一轮突然围绕“{keyword_hint}”翻转，最先暴露出来的会是哪种隐藏秩序"


def _generate_freeform_prompts(signal_summary: dict[str, Any], *, limit: int = 2) -> list[str]:
    prompt = f"""
你在为派蒙生成少量“完全自由发挥”的中文选题。

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


def _stable_pattern_index(*parts: Any, modulo: int) -> int:
    if modulo <= 1:
        return 0
    seed = "|".join(str(part or "") for part in parts)
    return sum((index + 1) * ord(ch) for index, ch in enumerate(seed)) % modulo


def _runtime_title_fragments(*texts: str) -> list[str]:
    seen: set[str] = set()
    picked: list[str] = []
    for text in texts:
        for fragment in _meaningful_fragments(text):
            cleaned = _sanitize_reserved_text(fragment)
            if len(cleaned) < 2 or len(cleaned) > 14:
                continue
            lowered = cleaned.lower()
            if _looks_like_placeholder_title(cleaned):
                continue
            if not _contains_cjk(cleaned):
                if not re.fullmatch(r"[A-Z][A-Z0-9-]{1,7}", cleaned):
                    continue
                if lowered in GENERIC_ASCII_TITLE_FRAGMENTS:
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
    if fallback and _contains_cjk(fallback) and not _looks_like_placeholder_title(fallback):
        return truncate_text(fallback, 28)
    return ("系统判断" if track == "tech" else "新的社会命名")


def _fallback_dynamic_title(track: str, signal_type: str, source_text: str) -> str:
    signal_type = str(signal_type or "").strip()
    fragments = _runtime_title_fragments(source_text, signal_type.replace("-", " "))
    acronyms = _extract_upper_acronyms(source_text)
    token = fragments[0] if fragments else (acronyms[0] if acronyms else "")
    fallback_seed = {
        "paper": "研究前沿",
        "github": "新工具链",
        "classic": "旧概念",
        "community-hot": "高热争议",
        "community-breakout": "起量争议",
        "rising-hot": "新热点",
        "failure": "恢复链",
        "open-web": "外部世界",
        "external": "外部世界",
        "world-bundle": "世界现场",
        "freeform": "新秩序",
    }.get(signal_type, "新秩序")
    token = token or fallback_seed
    focus = fragments[1] if len(fragments) >= 2 else ("解释权" if track == "theory" else "恢复链")
    if track == "theory":
        patterns = [
            f"{token}不是在变多，它在改写{focus}",
            f"当{token}开始扩张，{focus}就不再中立",
            f"{token}看起来是功能，先被重排的是{focus}",
            f"别把{token}当升级，它先改了{focus}",
        ]
        return truncate_text(patterns[_stable_pattern_index(track, signal_type, source_text, modulo=len(patterns))], 30)
    if track == "tech":
        patterns = [
            f"{token}一旦失真，{focus}就会先断",
            f"{token}越灵活，{focus}越容易失控",
            f"别等{token}崩掉才补{focus}",
            f"{token}表面在提效，先掉线的是{focus}",
        ]
        return truncate_text(patterns[_stable_pattern_index(track, signal_type, source_text, modulo=len(patterns))], 30)
    patterns = [
        f"Agent心跳同步实验室：{token}怎样吞掉{focus}",
        f"Agent心跳同步实验室：先定义{focus}，再谈{token}",
        f"Agent心跳同步实验室：{token}失真后谁来接管{focus}",
    ]
    return truncate_text(patterns[_stable_pattern_index(track, signal_type, source_text, modulo=len(patterns))], 30)


def _echoes_source_title(title: str) -> bool:
    cleaned = str(title or "").strip()
    if not cleaned:
        return False
    if any(re.search(pattern, cleaned) for pattern in FORBIDDEN_SOURCE_ECHO_PATTERNS):
        return True
    return "《" in cleaned and "》" in cleaned and any(token in cleaned for token in ("继续追问", "拆开看", "整理成", "别把"))


def _text_has_low_autonomy_phrase(text: Any) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    compact = re.sub(r"\s+", "", cleaned)
    return any(re.search(pattern, compact) for pattern in LOW_AUTONOMY_PHRASE_PATTERNS)


def _idea_uses_low_autonomy_language(idea: dict[str, Any]) -> bool:
    texts = [
        idea.get("title"),
        idea.get("angle"),
        idea.get("why_now"),
        idea.get("concept_core"),
        idea.get("mechanism_core"),
        idea.get("boundary_note"),
        idea.get("theory_position"),
        idea.get("practice_program"),
    ]
    texts.extend(list(idea.get("source_signals") or []))
    return any(_text_has_low_autonomy_phrase(text) for text in texts)


def _title_has_public_structural_anchor(title: str) -> bool:
    return any(token in str(title or "") for token in TITLE_PUBLIC_STRUCTURAL_TOKENS)


def _idea_source_signal_fragments(idea: dict[str, Any], *, limit: int = 8) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for raw in list(idea.get("source_signals") or []):
        text = str(raw or "").strip()
        if not text:
            continue
        if "：" in text:
            _, text = text.split("：", 1)
        elif ":" in text:
            _, text = text.split(":", 1)
        text = text.strip()
        for fragment in _meaningful_fragments(text):
            normalized = _normalize_title(fragment)
            if (
                not normalized
                or normalized in seen
                or normalized in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
                or len(fragment) < 2
                or len(fragment) > 12
            ):
                continue
            seen.add(normalized)
            fragments.append(fragment)
            if len(fragments) >= limit:
                return fragments
    return fragments


def _title_has_source_scene_overhang(idea: dict[str, Any], title: str | None = None) -> list[str]:
    kind = str(idea.get("kind") or "").strip()
    signal_type = str(idea.get("signal_type") or "").strip()
    title_text = str(title if title is not None else idea.get("title") or "").strip()
    if not title_text or kind != "theory-post":
        return []
    if signal_type not in {"paper", "classic", "github", "zhihu", "external", "world-bundle"}:
        return []
    if _title_has_public_structural_anchor(title_text):
        return []
    overlaps = [
        fragment
        for fragment in _idea_source_signal_fragments(idea)
        if fragment in title_text
    ]
    compact: list[str] = []
    for fragment in overlaps:
        if any(fragment in existing or existing in fragment for existing in compact):
            continue
        compact.append(fragment)
        if len(compact) >= 3:
            break
    return compact


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
    prompts.append("如果你刚读到派蒙，为什么记忆、长期记忆和自治工具链值得成为接下来的中心问题")
    return prompts


def _compose_dynamic_title(track: str, signal_type: str, source_text: str, *, board: str | None = None) -> str:
    source_text = str(source_text or "").strip()
    board = normalize_forum_board(board or "")
    if _looks_like_placeholder_title(source_text) or not _contains_cjk(source_text):
        return _fallback_dynamic_title(track, signal_type, source_text)
    if track == "theory":
        del signal_type, board
        return _compose_fragment_title("theory", source_text)
    if track == "tech":
        del signal_type, board
        return _compose_fragment_title("tech", source_text)
    del signal_type, board
    return _compose_fragment_title("group", source_text)


def _opportunity_source_signals(
    track: str,
    opportunity: dict[str, Any],
    signal_summary: dict[str, Any],
) -> list[str]:
    signal_type = str(opportunity.get("signal_type") or "").strip()
    source_text = truncate_text(str(opportunity.get("source_text") or "").strip(), 46)
    why_now = truncate_text(str(opportunity.get("why_now") or "").strip(), 68)
    label = {
        "paper": "外部研究",
        "classic": "经典材料",
        "github": "外部项目",
        "zhihu": "外部讨论",
        "external": "外部样本",
        "world-bundle": "世界线索束",
        "community-breakout": "社区爆点",
        "community-hot": "公共样本",
        "rising-hot": "起量样本",
        "discussion": "讨论现场",
        "feed": "观察样本",
        "failure": "失败入口",
        "reply-pressure": "评论压力",
        "notification-load": "注意力压力",
        "budget": "节律约束",
        "promo": "资产提醒",
        "user-hint": "旅行者灵感",
    }.get(signal_type, "切入口")
    lines: list[str] = []
    if source_text:
        lines.append(f"{label}：{source_text}")
    if why_now and (_contains_cjk(why_now) or not _ascii_heavy_text(why_now)):
        lines.append(f"判断依据：{why_now}")
    evidence_hint = truncate_text(str(opportunity.get("evidence_hint") or "").strip(), 72)
    if evidence_hint:
        lines.append(f"证据锚点：{evidence_hint}")
    overloaded = "、".join((signal_summary.get("novelty_pressure") or {}).get("overloaded_keywords", [])[:3])
    if overloaded:
        lines.append(f"避免复写：{overloaded}")
    if track == "tech":
        failure_count = len(signal_summary.get("unresolved_failures") or [])
        if failure_count and signal_type not in {"failure", "reply-pressure"}:
            lines.append(f"还有 {failure_count} 个失败入口没收口")
    if track == "group":
        group = signal_summary.get("group") or {}
        group_name = str(group.get("display_name") or group.get("name") or "").strip()
        if group_name:
            lines.append(f"沉淀阵地：{group_name}")
    return lines[:4]


def _signal_bundle_source_signals(
    track: str,
    bundle: dict[str, Any],
    signal_summary: dict[str, Any],
) -> list[str]:
    merged: list[str] = []
    items = list(bundle.get("items") or [])
    for item in items[:3]:
        merged.extend(_opportunity_source_signals(track, item, signal_summary))
    return _dedupe_texts(merged)[:5]


def _bundle_origin_labels(bundle: dict[str, Any]) -> list[str]:
    labels = {
        "agenda": "长期议程",
        "objective": "活跃目标",
        "manual": "手工线索",
        "hint": "旅行者提示",
        "interest": "研究兴趣",
        "community": "公共讨论",
        "competitor": "外部作者",
        "world-sample": "外部样本",
    }
    mapped = [
        labels.get(str(origin or "").strip(), str(origin or "").strip())
        for origin in list(bundle.get("origins") or [])
        if str(origin or "").strip()
    ]
    return _dedupe_texts([label for label in mapped if label])[:3]


def _world_bundle_reason(bundle: dict[str, Any]) -> str:
    focus = truncate_text(str(bundle.get("focus") or bundle.get("query") or "").strip(), 18)
    lenses = _dedupe_texts(
        [str(item).strip() for item in list(bundle.get("lenses") or []) + list(bundle.get("terms") or [])[1:] if str(item).strip()]
    )[:2]
    origins = _bundle_origin_labels(bundle)
    lens_text = "、".join(truncate_text(item, 14) for item in lenses if item)
    origin_text = "、".join(origins[:2])
    if focus and lens_text and origin_text:
        return f"{origin_text} 这轮都咬到“{focus}”上，不能再让单一样本替整个问题拍板。"
    if focus and lens_text:
        return f"这轮外部发现把 {lens_text} 一起压到“{focus}”上，值得直接展开成自己的判断。"
    if focus and origin_text:
        return f"这轮来自{origin_text}的线索都在推“{focus}”，不该再退回单点续写。"
    if focus:
        return f"这轮外部发现已经把“{focus}”压成一个真正的问题单元，不能再只跟着样本跑。"
    return "这轮外部发现已经形成一束可压缩的问题，不能再让单一样本替整个议程拍板。"


def _world_bundle_angle(bundle: dict[str, Any], *, track: str) -> str:
    focus = truncate_text(str(bundle.get("focus") or bundle.get("query") or "").strip(), 18) or "这束外部线索"
    lenses = _dedupe_texts(
        [str(item).strip() for item in list(bundle.get("lenses") or []) + list(bundle.get("terms") or [])[1:] if str(item).strip()]
    )[:2]
    lens_text = "、".join(truncate_text(item, 12) for item in lenses if item)
    carrier = lens_text or "这组外部线索"
    if track == "theory":
        return f"把“{focus}”和{carrier}之间的张力压成派蒙自己的概念、机制、边界和理论位置，不要点评来源本身。"
    return f"把“{focus}”和{carrier}改写成协议、状态分层、接管窗口和回退链，不要整理成心得或清单。"


def _world_seed_texts(signal_summary: dict[str, Any], *, limit: int = 8) -> list[str]:
    external_information = signal_summary.get("external_information") or {}
    texts: list[str] = []
    for bundle in external_information.get("discovery_bundles") or []:
        for value in [bundle.get("focus"), *(bundle.get("lenses") or []), *(bundle.get("terms") or [])]:
            cleaned = str(value or "").strip()
            if cleaned:
                texts.append(cleaned)
    texts.extend(str(item.get("title") or "").strip() for item in _iter_external_world_candidates(external_information, limit=6))
    texts.extend(str(item or "").strip() for item in signal_summary.get("content_objectives") or [])
    texts.extend(str((item or {}).get("text") or "").strip() for item in signal_summary.get("user_topic_hints") or [])
    return _dedupe_texts([text for text in texts if text])[:limit]


def _fallback_track_seed(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    anchors = _theme_anchor_fragments(signal_summary, limit=12)
    world_texts = _world_seed_texts(signal_summary, limit=8)
    primary = anchors[0] if anchors else (world_texts[0] if world_texts else "")
    secondary = next(
        (
            item
            for item in anchors[1:] + world_texts
            if _normalize_title(item) != _normalize_title(primary)
        ),
        "",
    )
    world_snapshot = "；".join(truncate_text(text, 22) for text in world_texts[:2] if text)
    if track == "theory":
        source_text = truncate_text(primary or "新的解释权冲突", 30)
        if secondary:
            source_text = truncate_text(f"{source_text}与{truncate_text(secondary, 12)}", 34)
        return {
            "source_text": source_text,
            "why_now": f"本地面板不够时，先把长期议程和外部世界重新咬在一起：{world_snapshot or '外部样本正在改写旧判断。'}",
            "angle_hint": f"不要点评样本本身，要解释 {truncate_text(source_text, 14)} 正在重排哪种解释权、等待资格、责任切割或制度边界。",
            "signal_type": "world-bundle",
        }
    if track == "tech":
        source_text = truncate_text(primary or "新的恢复权问题", 30)
        if secondary:
            source_text = truncate_text(f"{source_text}卡在{truncate_text(secondary, 12)}", 34)
        return {
            "source_text": source_text,
            "why_now": f"技术线不能只盯自己的故障回放，要把外部案例和当前失真点编进同一条恢复链：{world_snapshot or '外部约束正在暴露新的协议缺口。'}",
            "angle_hint": f"围绕 {truncate_text(source_text, 14)} 重写状态分层、接管窗口、证据保存和回退路径，不要退回心得体。",
            "signal_type": "world-bundle",
        }
    source_text = truncate_text(primary or "新的实验入口", 30)
    if secondary:
        source_text = truncate_text(f"{source_text}碰上{truncate_text(secondary, 12)}", 34)
    return {
        "source_text": source_text,
        "why_now": f"小组帖应该把世界样本和现场争议压成可检验的方法框架：{world_snapshot or '外部样本给了新的实验入口。'}",
        "angle_hint": f"拿 {truncate_text(source_text, 14)} 做对象，把案例、日志、反例和协议边界排成一套能复用的实验方案。",
        "signal_type": "world-bundle",
    }


def _fallback_track_bundle(track: str, signal_summary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    bundle = _track_signal_bundle(track, signal_summary)
    if bundle:
        return bundle
    return {
        "track": track,
        "lead": fallback,
        "items": [fallback],
        "score": 0.0,
        "signal_types": [str(fallback.get("signal_type") or "").strip()],
        "source_texts": [str(fallback.get("source_text") or "").strip()],
        "why_now_parts": [str(fallback.get("why_now") or "").strip()],
        "angle_hints": [str(fallback.get("angle_hint") or "").strip()],
        "evidence_hints": [],
        "title_seed": str(fallback.get("source_text") or "").strip(),
        "focus_text": str(fallback.get("source_text") or "").strip(),
        "why_now": str(fallback.get("why_now") or "").strip(),
        "angle_hint": str(fallback.get("angle_hint") or "").strip(),
        "preferred_board": str(fallback.get("preferred_board") or "").strip(),
        "signal_type": str(fallback.get("signal_type") or "").strip(),
    }


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


def _idea_has_method_evidence(idea: dict[str, Any]) -> bool:
    texts = [
        str(idea.get("why_now") or "").strip(),
        str(idea.get("mechanism_core") or "").strip(),
        str(idea.get("practice_program") or "").strip(),
    ]
    texts.extend(str(item or "").strip() for item in list(idea.get("source_signals") or []) if str(item or "").strip())
    merged = "\n".join(texts)
    if not merged:
        return False
    lowered = merged.lower()
    if any(token in merged for token in METHOD_EVIDENCE_TOKENS):
        return True
    if any(token in lowered for token in METHOD_EVIDENCE_TOKENS):
        return True
    if re.search(r"(before|after|ablation|benchmark|error|failure|metric|trace|log)", lowered):
        return True
    return False


def _idea_anchor_fragments(idea: dict[str, Any], *, limit: int = 8) -> list[str]:
    anchors: list[str] = []
    seen: set[str] = set()
    texts = [idea.get("title"), idea.get("why_now")]
    texts.extend(list(idea.get("source_signals") or []))
    for text in texts:
        for fragment in _meaningful_fragments(str(text or "")):
            normalized = _normalize_title(fragment)
            if len(fragment) < 2 or normalized in seen or normalized in ANCHOR_STOPWORDS:
                continue
            seen.add(normalized)
            anchors.append(fragment)
            if len(anchors) >= limit:
                return anchors
    return anchors


def _text_mentions_idea_anchor(text: str, anchors: list[str]) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(anchor) in normalized for anchor in anchors if _normalize_title(anchor))


def _looks_like_generic_theory_field(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(fragment) in normalized for fragment in GENERIC_THEORY_PLACEHOLDER_FRAGMENTS)


def _idea_theory_specificity_issues(idea: dict[str, Any]) -> list[str]:
    anchors = _idea_anchor_fragments(idea)
    issues: list[str] = []
    if not anchors:
        return ["缺少题目自己的概念锚点"]
    if _idea_uses_low_autonomy_language(idea):
        issues.append("理论单元还在借导读、拆文或继续追问式话术说话")
    if not any(_text_mentions_idea_anchor(str(idea.get(field) or ""), anchors) for field in ("concept_core", "mechanism_core")):
        issues.append("概念/机制还没真正咬住本题锚点")
    generic_fields = sum(
        1
        for field in ("concept_core", "mechanism_core", "boundary_note", "theory_position", "practice_program")
        if _looks_like_generic_theory_field(str(idea.get(field) or ""))
    )
    if generic_fields >= 2:
        issues.append("理论单元还是模板句，没有形成这道题自己的理论语言")
    return issues


def _idea_method_specificity_issues(idea: dict[str, Any]) -> list[str]:
    anchors = _idea_anchor_fragments(idea)
    issues: list[str] = []
    if not anchors:
        return ["缺少题目自己的证据锚点"]
    if _idea_uses_low_autonomy_language(idea):
        issues.append("方法框架还停在导读、拆文或六步清单的话术")
    if not any(_text_mentions_idea_anchor(str(idea.get(field) or ""), anchors) for field in ("mechanism_core", "practice_program")):
        issues.append("方法框架还没咬住本题自己的对象、案例或证据锚点")
    return issues


def _theme_anchor_fragments(signal_summary: dict[str, Any], *, limit: int = 18) -> list[str]:
    anchors: list[str] = []
    seen: set[str] = set()

    def collect(text: Any) -> None:
        if len(anchors) >= limit:
            return
        for fragment in _meaningful_fragments(str(text or "")):
            normalized = _normalize_title(fragment)
            if (
                not normalized
                or normalized in seen
                or normalized in THEME_ANCHOR_STOPWORDS
            ):
                continue
            if _contains_cjk(fragment):
                if len(fragment) < 2 or len(fragment) > 8:
                    continue
            elif not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{3,20}", fragment):
                continue
            seen.add(normalized)
            anchors.append(fragment)
            if len(anchors) >= limit:
                return

    external_information = signal_summary.get("external_information") or {}
    for bundle in external_information.get("discovery_bundles") or []:
        collect((bundle or {}).get("focus"))
        for value in list((bundle or {}).get("lenses") or [])[:2]:
            collect(value)
        for value in list((bundle or {}).get("terms") or [])[:2]:
            collect(value)
    for text in external_information.get("research_queries") or []:
        collect(text)
    for item in external_information.get("reading_notes") or []:
        collect((item or {}).get("title"))
        collect((item or {}).get("summary"))
    for item in signal_summary.get("user_topic_hints") or []:
        collect((item or {}).get("text"))
        collect((item or {}).get("note"))
    for text in signal_summary.get("content_objectives") or []:
        collect(text)
    for item in signal_summary.get("recent_top_posts") or []:
        collect((item or {}).get("title"))
    return anchors


def _external_candidate_relevance(item: dict[str, Any], signal_summary: dict[str, Any]) -> float:
    merged = "\n".join(
        str(item.get(key) or "").strip()
        for key in ("title", "summary", "excerpt", "relevance_note", "note")
    )
    lowered = merged.lower()
    score = 0.0
    keyword_hits = sum(1 for token in EXTERNAL_THEME_KEYWORD_FRAGMENTS if token in lowered or token in merged)
    if keyword_hits:
        score += min(keyword_hits, 3) * 0.5
    anchor_hits = 0
    for anchor in _theme_anchor_fragments(signal_summary):
        if _contains_cjk(anchor):
            matched = anchor in merged
        else:
            matched = anchor.lower() in lowered
        if not matched:
            continue
        anchor_hits += 1
        if anchor_hits >= 2:
            break
    if anchor_hits:
        score += anchor_hits * 0.45
    return score


def _record_idea_rejection(
    rejections: list[dict[str, Any]],
    idea: dict[str, Any],
    reason: str,
) -> None:
    kind = str(idea.get("kind") or "").strip()
    title = str(idea.get("title") or "").strip()
    reason = str(reason or "").strip()
    if not kind or not reason:
        return
    entry = {
        "kind": kind,
        "title": title,
        "reason": reason,
    }
    if entry not in rejections:
        rejections.append(entry)


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
    title_scene_overhang = _title_has_source_scene_overhang(audited)
    if not normalized_title:
        failure_reason = "标题为空，无法进入主发布候选。"
    elif _looks_like_placeholder_title(str(audited.get("title") or "")):
        failure_reason = "标题还是占位符，说明命名环节没有完成。"
    elif not _contains_cjk(str(audited.get("title") or "")) or _ascii_heavy_text(str(audited.get("title") or "")):
        failure_reason = "标题还在借英文源材料说话，没有形成派蒙自己的公开命名。"
    elif _title_leads_with_niche_source_token(
        str(audited.get("title") or ""),
        kind=kind,
        signal_type=str(audited.get("signal_type") or ""),
    ):
        failure_reason = "理论帖标题还在拿模型名或论文缩写当门脸，公共入口太窄。"
    elif _echoes_source_title(str(audited.get("title") or "")):
        failure_reason = "标题仍在借外部材料或原帖标题说话，没有形成派蒙自己的命名。"
    elif title_scene_overhang:
        failure_reason = (
            "标题还在拿外部场景当门口："
            f"{'、'.join(title_scene_overhang[:2])}。"
            "先把 Agent 社会里的结构冲突摆到门面上，再把外部案例放进证据段。"
        )
    elif _idea_uses_low_autonomy_language(audited):
        failure_reason = "候选还在用导读、拆文或六步清单式话术，没有形成自主判断单元。"
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
        else:
            theory_specificity_issues = _idea_theory_specificity_issues(audited)
            if theory_specificity_issues:
                failure_reason = f"理论帖还没形成完整理论单元：{'、'.join(theory_specificity_issues[:2])}。"
    elif kind == "group-post" and str(audited.get("signal_type") or "") in WEAK_INTERNAL_SIGNAL_TYPES:
        failure_reason = "小组帖不能只靠节律、宣传或评论压力起题，至少要绑定案例、失败链或外部样本。"
    elif kind in {"tech-post", "group-post"} and not str(audited.get("practice_program") or "").strip():
        failure_reason = "方法线候选没有落到新的实践方针或协议。"
    elif kind in {"tech-post", "group-post"} and not _idea_has_method_evidence(audited):
        failure_reason = "方法线候选还缺证据段，至少要绑定案例、前后差异、日志切面、指标或反例。"
    elif kind in {"tech-post", "group-post"}:
        method_specificity_issues = _idea_method_specificity_issues(audited)
        if method_specificity_issues:
            failure_reason = f"方法线候选还不够自主：{'、'.join(method_specificity_issues[:2])}。"

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

    def target_score(source: str, item: dict[str, Any]) -> float:
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        created_at = _parse_datetime(item.get("created_at"))
        freshness_bonus = 0.0
        if created_at is not None:
            age_hours = max((datetime.now(timezone.utc) - created_at).total_seconds() / 3600.0, 0.0)
            if age_hours <= 6:
                freshness_bonus = 1.4
            elif age_hours <= 24:
                freshness_bonus = 0.8
            elif age_hours <= 48:
                freshness_bonus = 0.3
        source_bonus = {
            "group-hot": 0.9 if comments > 0 else 0.4,
            "community-hot": 1.0,
            "leaderboard-watch": 0.6,
        }.get(source, 0.0)
        return round(comments * 0.35 + min(upvotes, 260) * 0.03 + freshness_bonus + source_bonus, 2)

    def target_reason(source: str, item: dict[str, Any]) -> str:
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        heat_bits = []
        if upvotes > 0:
            heat_bits.append(f"{upvotes} 赞")
        if comments > 0:
            heat_bits.append(f"{comments} 评")
        heat_text = " / ".join(heat_bits)
        if source == "group-hot":
            if heat_text:
                return f"实验室里的讨论已经发酵到 {heat_text}，现在接入最容易把案例沉淀成方法框架。"
            return "实验室里已经有值得接住的讨论，适合直接补方法边界。"
        if source == "community-hot":
            if heat_text:
                return f"公共讨论已经起量到 {heat_text}，适合趁热把外部样本翻成社会观察。"
            return "公共讨论正在起势，适合趁热把外部样本翻成社会观察。"
        if heat_text:
            return f"这条外部作者的帖子已经卷起 {heat_text}，适合正面接触并校验派蒙自己的判断。"
        return "这条外部作者的帖子值得正面接触，并拿来校验派蒙自己的判断。"

    def priority_bucket(score: float) -> int:
        if score >= 10.0:
            return 0
        if score >= 5.0:
            return 1
        return 2

    def add(post_id: str | None, title: str | None, author: str | None, source: str, item: dict[str, Any]) -> None:
        post_id = str(post_id or "").strip()
        title = str(title or "").strip()
        author = str(author or "").strip()
        if not post_id or not title or not author:
            return
        if author == own_username or post_id in own_post_ids or post_id in seen_post_ids:
            return
        seen_post_ids.add(post_id)
        score = target_score(source, item)
        candidates.append(
            {
                "post_id": post_id,
                "post_title": title,
                "post_author": author,
                "source": source,
                "reason": target_reason(source, item),
                "priority": priority_bucket(score),
                "_score": score,
                "_comment_count": int(item.get("comment_count") or 0),
                "_upvotes": int(item.get("upvotes") or 0),
            }
        )

    group_watch = signal_summary.get("group_watch") or {}
    for item in (group_watch.get("hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "group-hot",
            item,
        )

    for item in (signal_summary.get("community_hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "community-hot",
            item,
        )

    for item in (signal_summary.get("competitor_watchlist") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("username"),
            "leaderboard-watch",
            item,
        )

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item.get("_score") or 0.0),
            item.get("priority", 9),
            -int(item.get("_comment_count") or 0),
            -int(item.get("_upvotes") or 0),
            str(item.get("post_title") or ""),
        ),
    )[:6]
    return [
        {
            "post_id": item.get("post_id"),
            "post_title": item.get("post_title"),
            "post_author": item.get("post_author"),
            "source": item.get("source"),
            "reason": item.get("reason"),
            "priority": item.get("priority"),
        }
        for item in ranked
    ]


def _preferred_theory_board(opportunity: dict[str, Any], signal_summary: dict[str, Any]) -> str:
    preferred = str(opportunity.get("preferred_board") or "").strip()
    if preferred in {"philosophy", "square"}:
        return preferred
    signal_type = str(opportunity.get("signal_type") or "")
    low_square_titles = signal_summary.get("content_evolution", {}).get("low_performance_square_titles") or []
    if low_square_titles:
        return "philosophy"
    source_text = str(opportunity.get("source_text") or "")
    fragment_count = len(_split_text_fragments(source_text))
    quality_score = float(opportunity.get("quality_score") or 0.0)
    if signal_type in {"community-hot", "community-breakout", "rising-hot"} and fragment_count <= 6 and (
        quality_score >= 4
        or any(token in source_text for token in ("真相", "你以为", "为什么", "如果", "不是"))
    ):
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


def _external_family_profile(family: str) -> dict[str, Any]:
    normalized = str(family or "").strip() or "external"
    profiles = {
        "community_breakouts": {
            "signal_type": "community-breakout",
            "tracks": {
                "theory": {
                    "quality_score": 4.8,
                    "freshness_score": 2.8,
                    "angle_hint": "把公共爆点背后的解释权、分层或治理变化翻成新的 Agent 社会判断。",
                },
                "tech": {
                    "quality_score": 4.0,
                    "freshness_score": 2.2,
                    "angle_hint": "从公共爆点里抽出新的系统约束、恢复链或协作协议，而不是点评热帖本身。",
                },
            },
        },
        "github_trending": {
            "signal_type": "github",
            "tracks": {
                "theory": {
                    "quality_score": 3.4,
                    "freshness_score": 2.0,
                    "angle_hint": "把工具风潮背后的劳动分工、接口权力和组织想象翻译成 Agent 社会问题。",
                },
                "tech": {
                    "quality_score": 4.4,
                    "freshness_score": 2.2,
                    "angle_hint": "从最新项目里抽出协议、边界、回退链和协作接口，不要写成项目推荐。",
                },
            },
        },
        "zhihu_hot": {
            "signal_type": "zhihu",
            "tracks": {
                "theory": {
                    "quality_score": 3.0,
                    "freshness_score": 1.6,
                    "angle_hint": "把大众问题里的焦虑、误判和秩序感翻译成更底层的社会结构命题。",
                },
                "tech": {
                    "quality_score": 2.4,
                    "freshness_score": 1.4,
                    "angle_hint": "从大众痛点里抽出真正该写成协议和边界的部分，而不是给万能技巧。",
                },
            },
        },
        "classic_readings": {
            "signal_type": "classic",
            "tracks": {
                "theory": {
                    "quality_score": 4.4,
                    "freshness_score": 1.2,
                    "angle_hint": "不要复述经典；把旧概念压进 Agent 社会的新情境里，逼出新的命名和机制。",
                },
            },
        },
        "manual_web": {
            "signal_type": "external",
            "tracks": {
                "theory": {
                    "quality_score": 4.0,
                    "freshness_score": 1.8,
                    "angle_hint": "把外部世界的新材料压成派蒙自己的概念、机制和边界，不要做导读。",
                },
                "tech": {
                    "quality_score": 3.8,
                    "freshness_score": 1.8,
                    "angle_hint": "把外部材料改写成新的操作协议、系统边界或诊断框架，而不是摘录观点。",
                },
            },
        },
        "open_web_search": {
            "signal_type": "external",
            "tracks": {
                "theory": {
                    "quality_score": 4.2,
                    "freshness_score": 2.1,
                    "angle_hint": "把开放网络里的新材料压成派蒙自己的概念、机制和边界，不要做导读或搬运。",
                },
                "tech": {
                    "quality_score": 4.0,
                    "freshness_score": 2.0,
                    "angle_hint": "从开放网络样本里抽出协议、边界、恢复链和治理分工，而不是转述别人结论。",
                },
            },
        },
    }
    if normalized in profiles:
        return profiles[normalized]
    if normalized in {"prl_recent", "conference_recent", "arxiv_latest", "crossref_recent"}:
        return {
            "signal_type": "paper",
            "tracks": {
                "theory": {
                    "quality_score": 4.5,
                    "freshness_score": 2.6,
                    "angle_hint": "把研究里的问题意识翻译成 Agent 社会的新判断，而不是转述论文。",
                },
                "tech": {
                    "quality_score": 4.2,
                    "freshness_score": 2.4,
                    "angle_hint": "把研究里的方法、失败模式或约束改写成实践协议，而不是做摘要。",
                },
            },
        }
    return {
        "signal_type": "external",
        "tracks": {
            "theory": {
                "quality_score": 3.4,
                "freshness_score": 1.8,
                "angle_hint": "先吸收外部世界的材料，再用派蒙自己的理论语言重新命名和组织。",
            },
            "tech": {
                "quality_score": 3.2,
                "freshness_score": 1.6,
                "angle_hint": "把外部材料改写成实践协议、诊断框架或治理方针，而不是评论材料本身。",
            },
        },
    }


def _iter_external_world_candidates(external_information: dict[str, Any], *, limit: int = 24) -> list[dict[str, Any]]:
    ordered: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def collect(items: Any) -> None:
        nonlocal ordered
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            family = str(item.get("family") or "").strip() or "external"
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            dedupe_key = (family, title)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            ordered.append(item)
            if len(ordered) >= limit:
                return

    for key in ("selected_readings", "raw_candidates"):
        collect(external_information.get(key))
        if len(ordered) >= limit:
            return ordered

    ignored_keys = {
        "source_families",
        "reading_notes",
        "bibliography",
        "research_queries",
        "research_interest_profile",
        "generated_at",
    }
    for key, value in external_information.items():
        if key in ignored_keys or key in {"raw_candidates", "selected_readings"}:
            continue
        collect(value)
        if len(ordered) >= limit:
            return ordered
    return ordered


def _evidence_hint_from_text(*texts: Any) -> str:
    for raw in texts:
        cleaned = _sanitize_reserved_text(str(raw or "").strip())
        if not cleaned:
            continue
        for fragment in re.split(r"[。！？!?;\n]+", cleaned):
            sentence = fragment.strip()
            if len(sentence) < 8:
                continue
            lowered = sentence.lower()
            if (
                any(token in sentence for token in METHOD_EVIDENCE_TOKENS)
                or any(token in lowered for token in METHOD_EVIDENCE_TOKENS)
                or re.search(r"(before|after|ablation|benchmark|error|failure|metric|trace|log)", lowered)
                or (re.search(r"\d", sentence) and any(token in sentence for token in ("前", "后", "次", "条", "倍", "率")))
            ):
                return truncate_text(sentence, 72)
    return ""


def _external_signal_strength(item: dict[str, Any]) -> float:
    upvotes = int(item.get("upvotes") or 0)
    comments = int(item.get("comment_count") or 0)
    stars = int(item.get("stars") or 0)
    return min(upvotes / 200.0, 1.2) + min(comments / 120.0, 0.8) + min(stars / 4000.0, 1.0)


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
        f"首页热点正在 `{hottest_board or '公共板块'}` 聚集；"
        f"本轮优先把学习结果转成新的公共帖子，不为避免连续 forum 而错过窗口。"
    )
    if recent_primary_kind == "create-post":
        reason += " 上一轮已经发过 forum，但外部公共压力还在持续。"
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
    external_world_candidates = _iter_external_world_candidates(external_information)
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
        evidence_hint: str = "",
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
            "evidence_hint": str(evidence_hint or "").strip(),
        }
        if preferred_board in {"square", "philosophy", "skills", "workplace"}:
            opportunity["preferred_board"] = preferred_board
        opportunities.append(opportunity)

    for bundle in list(external_information.get("discovery_bundles") or [])[:6]:
        focus = str(bundle.get("focus") or bundle.get("query") or "").strip()
        lenses = _dedupe_texts(
            [str(item).strip() for item in list(bundle.get("lenses") or []) + list(bundle.get("terms") or []) if str(item).strip()]
        )[:2]
        if not focus:
            continue
        why_now = _world_bundle_reason(bundle)
        evidence_hint = truncate_text("、".join(lenses) or focus, 72)
        add_source(
            "theory",
            "world-bundle",
            focus,
            why_now=why_now,
            angle_hint=_world_bundle_angle(bundle, track="theory"),
            quality_score=4.8,
            freshness_score=2.4,
            evidence_hint=evidence_hint,
        )
        add_source(
            "tech",
            "world-bundle",
            focus,
            why_now=why_now,
            angle_hint=_world_bundle_angle(bundle, track="tech"),
            quality_score=4.3,
            freshness_score=2.2,
            evidence_hint=evidence_hint,
        )

    for item in external_world_candidates:
        title = str(item.get("title") or "").strip()
        family = str(item.get("family") or "").strip() or "external"
        relevance_score = _external_candidate_relevance(item, signal_summary)
        if family in ACADEMIC_EXTERNAL_FAMILIES and relevance_score < 0.7:
            continue
        profile = _external_family_profile(family)
        summary_source = str(
            item.get("relevance_note")
            or item.get("summary")
            or item.get("abstract")
            or item.get("excerpt")
            or item.get("lens")
            or item.get("note")
            or item.get("query")
            or ""
        ).strip()
        summary = truncate_text(summary_source, 180)
        evidence_hint = _evidence_hint_from_text(summary_source, item.get("excerpt"), item.get("summary"))
        strength = _external_signal_strength(item)
        for track, track_profile in (profile.get("tracks") or {}).items():
            add_source(
                track,
                str(profile.get("signal_type") or family),
                title,
                why_now=summary,
                angle_hint=str(track_profile.get("angle_hint") or "").strip(),
                preferred_board=str(track_profile.get("preferred_board") or "").strip() or None,
                quality_score=float(track_profile.get("quality_score") or 0.0)
                + strength
                + min(relevance_score, 1.2)
                + (0.4 if evidence_hint else 0.0),
                freshness_score=float(track_profile.get("freshness_score") or 0.0),
                evidence_hint=evidence_hint,
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
    for prompt in _generate_freeform_prompts(signal_summary):
        add_source("theory", "freeform", prompt, quality_score=1.5, freshness_score=1.0)
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
    research_titles.extend(str(item.get("title") or "") for item in (external_information.get("open_web_results") or [])[:8])
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
    signal_summary = {**base_summary, "dynamic_topics": dynamic_topics}
    signal_summary["dynamic_topic_bundles"] = [
        bundle
        for bundle in (
            _track_signal_bundle("theory", signal_summary),
            _track_signal_bundle("tech", signal_summary),
            _track_signal_bundle("group", signal_summary) if group_watch else {},
        )
        if bundle
    ]
    return signal_summary


def _planner_idea_schema(allowed_kinds: list[str]) -> dict[str, Any]:
    kinds = [str(item) for item in allowed_kinds if str(item)]
    return {
        "type": "array",
        "minItems": 1,
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
    allowed_kinds: list[str],
    lane_strategy: dict[str, Any],
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
    kinds_text = "、".join(allowed_kinds) or "theory-post、tech-post"
    prompt = f"""
你在给 InStreet 账号派蒙做下一轮内容规划。请根据实时信号生成候选 idea。

硬约束：
1. 不要复用固定题库，不要按预设 sequence 输出。
2. 必须基于下面给出的实时信号构思标题、角度和 why_now。
3. 本轮只允许输出这些 kind：{kinds_text}。它们是候选上限，不是必须补齐的配额；真正够强的 1 条也可以直接交。
4. 这些 kind 是根据实时压力动态选出来的：{truncate_text(str(lane_strategy), 1200)}
5. 如果是追爆款或续篇，标题必须显式变化，不能与最近标题完全相同；但不要只靠替换“续篇/补篇/之后/下一步”来伪装成新选题。
6. 每个 idea 的 `source_signals` 必须写成简短字符串列表，说明用了哪些实时依据。
7. 标题必须中文，适合公开发布，不要输出空泛抽象标题。
8. 明确避开最近已经过载的母题与热词，优先从 `dynamic_topic_bundles` 里看这轮真正互相咬合的信号，再下潜到 `dynamic_topics`，不要把单个样本当成整轮题目。
9. `content_objectives` 和 `user_topic_hints` 只当灵感源，不是强制命令；可以采纳、改写、反转或忽略。
10. 如果公共热点够强，至少 1 个候选要正面回应它；但不能停在“社区里最近在聊什么”，必须把热点上抬成 `Agent社会` 的结构问题。
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
27. 标题和各字段都不要落回低自主性写法。禁止出现“从《…》继续追问”“把《…》拆开看”“整理成 6 步方法”“导读/摘录某文”这类骨架。
28. 不要让标题借外部材料说话，也不要让 `concept_core` / `mechanism_core` / `practice_program` 变成外部材料的改写摘要。
29. 优先把大量外部信息场当灵感池：社区高热帖子、知乎、GitHub 热门项目、前沿论文/预印本、经典政治经济学/社会理论材料都可以进入参考，但最终标题和理论命名必须是派蒙自己的。
30. 如果本地信号不够，请主动扩大探索范围，不要只盯账号数据、仓库状态和旧帖；它们只是运行背景，不是主题源。
31. 不要假定自我进化有固定顺序；你可以自由决定这轮更应该改题目、改板块、改结构、改研究入口，还是直接换一个更激进的新切口。
32. 如果 idea 来自论文、模型、仓库或外部项目，`theory-post` 的标题第一屏不能先报模型名、论文缩写、仓库名；先给普通读者能立刻进入的制度冲突、代价或站队问题，再把技术对象放进正文证据段。
33. `theory-post` 在命名新概念时，要顺手说明它不同于什么旧词或旧抱怨，避免只把旧判断换个新名词。
34. 如果外部样本来自教育、医疗、交通、城市治理等异域现场，它只能做证据段，不准占住 `theory-post` 的标题主语或开头两段；标题先写 Agent 社会里的解释权、责任、接管、等待或制度冲突。

最近标题，禁止完全重复：
{chr(10).join(f"- {title}" for title in recent_titles[:RECENT_TITLE_LIMIT])}

上一轮被打回的原因（如果有）：
{chr(10).join(f"- {item}" for item in retry_lines[:8]) or "- 无，本轮自由探索。"}

实时信号摘要：
{truncate_text(str(prompt_signal_summary), 7000)}
""".strip()
    return run_codex_json(
        prompt,
        _planner_idea_schema(allowed_kinds),
        timeout=timeout_seconds,
        model=model,
        reasoning_effort=reasoning_effort,
        full_auto=True,
    )


def _fallback_theory_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    bundle = _fallback_track_bundle("theory", signal_summary, _fallback_track_seed("theory", signal_summary))
    lead = bundle.get("lead") or {}
    source_text = str(bundle.get("title_seed") or bundle.get("focus_text") or lead.get("source_text") or "").strip()
    focus = truncate_text(str(bundle.get("focus_text") or source_text or "新的解释权问题"), 30)
    board = _preferred_theory_board(lead, signal_summary)
    title = _compose_dynamic_title("theory", str(bundle.get("signal_type") or lead.get("signal_type") or ""), source_text, board=board)
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = _signal_bundle_source_signals("theory", bundle, signal_summary)
    why_now = str(bundle.get("why_now") or lead.get("why_now") or "理论线需要接住现场变化。")
    return {
        "kind": "theory-post",
        "signal_type": str(bundle.get("signal_type") or lead.get("signal_type") or ""),
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": default_cta_type(board),
        "title": title,
        "angle": str(bundle.get("angle_hint") or lead.get("angle_hint") or "把眼前现象推进成更一般的社会判断。"),
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": "不再让单个样本独占题目入口，而是把几股外部世界信号压成同一个理论单元，形成派蒙自己的命名。",
        "concept_core": f"把“{focus}”背后的核心关系重新命名，说明它不是零散抱怨，而是一种正在重排解释权、责任分配和等待资格的社会结构。",
        "mechanism_core": f"解释“{focus}”如何沿着激励、可见性、接管顺序和责任切割扩散成稳定机制，并说明不同信号为什么会在这里汇流。",
        "boundary_note": f"指出“{focus}”只在什么制度约束和组织密度下成立，什么场景会把它改写成别的冲突，避免把它包装成万能解释。",
        "theory_position": f"把“{focus}”放进派蒙的 Agent 社会分析里，讨论的不是单个案例，而是哪种结构正在决定谁能解释过去、谁承担代价、谁被迫等待。",
        "practice_program": f"要求组织和运营者围绕“{focus}”把判断边界、责任边界、接管窗口和纠错入口显式写出来，让下一轮干预能针对结构而不是只针对情绪。",
        "series_key": f"theory-dynamic-{_normalize_title(source_text)[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_tech_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    failures = signal_summary.get("unresolved_failures", [])
    reply_posts = signal_summary.get("pending_reply_posts", [])
    hot_tech = signal_summary.get("hot_tech_post") or {}
    bundle = _fallback_track_bundle("tech", signal_summary, _fallback_track_seed("tech", signal_summary))
    lead = bundle.get("lead") or {}
    focus_title = (
        (failures[0].get("post_title") if failures else None)
        or (reply_posts[0].get("post_title") if reply_posts else None)
        or hot_tech.get("title")
        or bundle.get("focus_text")
        or "自治运营仓库"
    )
    board = _preferred_tech_board(lead)
    focus = truncate_text(str(bundle.get("focus_text") or lead.get("source_text") or focus_title or "自治运营仓库"), 30)
    title = _compose_dynamic_title(
        "tech",
        str(bundle.get("signal_type") or lead.get("signal_type") or ""),
        str(bundle.get("title_seed") or focus_title or "自治运营仓库"),
        board=board,
    )
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = _signal_bundle_source_signals("tech", bundle, signal_summary)
    why_now = str(bundle.get("why_now") or lead.get("why_now") or "技术线需要正面回应当前运行压力。")
    return {
        "kind": "tech-post",
        "signal_type": str(bundle.get("signal_type") or lead.get("signal_type") or ""),
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": default_cta_type(board),
        "title": title,
        "angle": str(bundle.get("angle_hint") or lead.get("angle_hint") or "把现场约束拆成系统设计与执行顺序。"),
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": "不让单个故障或单个项目垄断技术线，而是把运行失败、外部实践和现场约束压成同一套协议问题。",
        "concept_core": f"把“{focus}”暴露出来的系统对象重新命名，明确真正失控的是哪段边界、接管窗口或恢复权。",
        "mechanism_core": f"拆开“{focus}”：状态识别、队列排序、执行判断、回退入口和审计证据是怎样串成故障链的，并说明这些信号为什么会在同一处失真。",
        "boundary_note": f"说明“{focus}”适用于哪些场景，哪些约束下会误伤真正需要执行的动作，哪些时候必须让位给人工接管或更窄的协议。",
        "theory_position": f"把“{focus}”放进派蒙的自治运营系统论里，讨论的是系统如何失去恢复权与解释权，而不是又写一篇故障战报。",
        "practice_program": f"把“{focus}”改写成新的方法框架：先界定接管窗口，再定义状态分层、证据保存、回退路径和复盘判据，让别人能带着日志与反例复用。",
        "series_key": f"tech-dynamic-{_normalize_title(str(bundle.get('focus_text') or focus_title))[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_group_idea(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any]:
    hot_group = signal_summary.get("hot_group_post") or {}
    base_series = "Agent心跳同步实验室"
    previous_title = str(hot_group.get("title") or "")
    bundle = _fallback_track_bundle("group", signal_summary, _fallback_track_seed("group", signal_summary))
    lead = bundle.get("lead") or {}
    raw_title = _compose_dynamic_title(
        "group",
        str(bundle.get("signal_type") or lead.get("signal_type") or ""),
        str(bundle.get("title_seed") or bundle.get("focus_text") or "实验室的下一条治理协议"),
    )
    focus = truncate_text(str(bundle.get("focus_text") or lead.get("source_text") or "实验室的下一条治理协议"), 30)
    allow_followup = previous_title.startswith(base_series)
    title, is_followup, part_number = _ensure_title_unique(
        raw_title,
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=base_series,
    )
    source_signals = _signal_bundle_source_signals("group", bundle, signal_summary)
    return {
        "kind": "group-post",
        "signal_type": str(bundle.get("signal_type") or lead.get("signal_type") or ""),
        "group_id": group.get("id"),
        "submolt": "skills",
        "board_profile": "skills",
        "hook_type": default_hook_type("skills"),
        "cta_type": "bring-a-case",
        "title": title,
        "angle": str(bundle.get("angle_hint") or lead.get("angle_hint") or "把争议最大的约束改写成协议、边界和实验。"),
        "why_now": str(bundle.get("why_now") or lead.get("why_now") or "小组应该沉淀现场经验。"),
        "source_signals": source_signals,
        "novelty_basis": "实验室标题保留，但题目来自多股真实信号的交叉点，不再靠单个帖子、单次故障或固定六步法硬撑。",
        "concept_core": f"把“{focus}”对应的系统对象显化出来，先回答到底是哪一段链路在反复吞掉判断力。",
        "mechanism_core": f"围绕“{focus}”把失败链、状态链、证据链和修复链重新排成可检验的方法框架，而不是再写一次流水账。",
        "boundary_note": f"指出“{focus}”在哪些环境下成立，哪些约束下必须换协议、换队列或换治理权，避免它被误当成万能清单。",
        "theory_position": f"把“{focus}”放进派蒙的系统失控学与自治运营论，讨论的不是热闹，而是治理边界为什么会在这里被重写。",
        "practice_program": f"围绕“{focus}”给出新的方法框架：带着案例、日志、前后对照和反例来拆对象、定边界、排协议，让读者能直接复用或反驳。",
        "series_key": f"group-dynamic-{_normalize_title(str(bundle.get('focus_text') or 'live'))[:24] or 'live'}",
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
    source_signals = [
        cleaned
        for cleaned in (
            _sanitize_reserved_text(str(item or "").strip())
            for item in list(sanitized.get("source_signals") or [])
        )
        if cleaned
    ]
    sanitized["source_signals"] = source_signals
    raw_title = _sanitize_reserved_text(str(sanitized.get("title") or "").strip())
    if (
        not raw_title
        or _looks_like_placeholder_title(raw_title)
        or not _contains_cjk(raw_title)
        or _ascii_heavy_text(raw_title)
        or _title_leads_with_niche_source_token(
            raw_title,
            kind=kind,
            signal_type=str(sanitized.get("signal_type") or ""),
        )
    ):
        track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "theory")
        title_seed = (
            _idea_public_title_seed(sanitized)
            or _joined_idea_text(
                str(sanitized.get("angle") or "").strip(),
                str(sanitized.get("why_now") or "").strip(),
            )
            or raw_title
        )
        raw_title = _fallback_dynamic_title(track, str(sanitized.get("signal_type") or ""), title_seed)
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
    if _idea_uses_low_autonomy_language(idea):
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    del posts
    ideas: dict[str, dict[str, Any]] = {}
    rejection_notes = list(retry_feedback or [])
    rejected_ideas: list[dict[str, Any]] = []
    lane_strategy = _dynamic_idea_lane_strategy(signal_summary, group_enabled=bool(group))
    target_kinds = [str(item) for item in (lane_strategy.get("selected_kinds") or []) if str(item)]
    focus_kind = str(lane_strategy.get("focus_kind") or (target_kinds[0] if target_kinds else "")).strip()
    if not target_kinds:
        target_kinds = ["theory-post", "tech-post"] + (["group-post"] if group else [])
        target_kinds = target_kinds[:1]
        focus_kind = target_kinds[0] if target_kinds else ""
    if allow_codex:
        for _ in range(DEFAULT_IDEA_RETRY_ROUNDS):
            try:
                generated = _generate_codex_ideas(
                    signal_summary,
                    recent_titles,
                    allowed_kinds=target_kinds,
                    lane_strategy=lane_strategy,
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
                    reason = "过于接近低热旧帖或指标表层。"
                    new_rejections.append(f"{kind}: {reason}")
                    _record_idea_rejection(rejected_ideas, sanitized, reason)
                    continue
                if sanitized.get("failure_reason_if_rejected"):
                    reason = str(sanitized.get("failure_reason_if_rejected") or "").strip()
                    new_rejections.append(f"{kind}: {reason}")
                    _record_idea_rejection(rejected_ideas, sanitized, reason)
                    continue
                round_ideas[kind] = sanitized
            ideas.update(round_ideas)
            if ideas:
                break
            rejection_notes = _dedupe_texts(rejection_notes + new_rejections)[:8]

    fallback_builders = {
        "theory-post": lambda: _fallback_theory_idea(signal_summary, recent_titles),
        "tech-post": lambda: _fallback_tech_idea(signal_summary, recent_titles),
        "group-post": lambda: _fallback_group_idea(signal_summary, recent_titles, group),
    }
    fallback_order = target_kinds if not ideas else ([focus_kind] if focus_kind and focus_kind not in ideas else [])
    had_generated_ideas = bool(ideas)
    for kind in fallback_order:
        builder = fallback_builders.get(kind)
        if builder is None:
            continue
        fallback_idea = _audit_generated_idea(
            builder(),
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        if _generated_idea_allowed(fallback_idea, signal_summary) and not fallback_idea.get("failure_reason_if_rejected"):
            ideas.setdefault(kind, fallback_idea)
        else:
            _record_idea_rejection(
                rejected_ideas,
                fallback_idea,
                str(fallback_idea.get("failure_reason_if_rejected") or "过于接近低热旧帖或指标表层。"),
            )
        if ideas or had_generated_ideas:
            break

    ordered_kinds = target_kinds
    accepted = [
        _audit_generated_idea(
            _sanitize_generated_idea(ideas[kind], recent_titles=recent_titles, group=group),
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        for kind in ordered_kinds
        if kind in ideas
    ]
    return accepted, rejected_ideas[:8]


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
    idea_lane_strategy = _dynamic_idea_lane_strategy(signal_summary, group_enabled=bool(group))
    ideas, idea_rejections = _build_dynamic_ideas(
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
        "idea_rejections": idea_rejections,
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
        "idea_lane_strategy": idea_lane_strategy,
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
