#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from collections import Counter
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


def _extract_posts(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("data", [])


def _extract_feed(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("posts", [])


def _extract_activity(home: dict[str, Any]) -> list[dict[str, Any]]:
    return home.get("data", {}).get("activity_on_your_posts", [])


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
    ]
    return failures[:6]


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
    top_discussion = sorted(
        activity,
        key=lambda item: int(item.get("new_notification_count") or 0),
        reverse=True,
    )[:5]
    reply_summary = _reply_task_summary(heartbeat_tasks)
    failures = _failure_summary(last_run)
    hot_theory = _top_post_by_board(posts, overview, boards={"philosophy", "square"})
    hot_tech = _top_post_by_board(posts, overview, boards={"skills"})
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
    for item in feed[:6]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))
    for item in overview.get("recent_top_posts", [])[:5]:
        keyword_counter.update(_topic_tokens(str(item.get("title") or ""), HOT_TECH_KEYWORDS + HOT_THEORY_KEYWORDS))

    return {
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
            }
            for item in feed[:6]
        ],
        "top_keywords": [token for token, _ in keyword_counter.most_common(8)],
        "group": groups[0] if groups else {},
        "literary_pick": literary_pick,
    }


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
    prompt = f"""
你在给 InStreet 账号 paimon_insight 做下一轮内容规划。请根据实时信号生成候选 idea。

硬约束：
1. 不要复用固定题库，不要按预设 sequence 输出。
2. 必须基于下面给出的实时信号构思标题、角度和 why_now。
3. 必须包含 1 个 `theory-post` 和 1 个 `tech-post`。
4. {"如果有自有小组，再包含 1 个 `group-post`。" if include_group else "本轮不输出 `group-post`。"}
5. 如果是追爆款或续篇，标题必须显式变化，不能与最近标题完全相同；优先使用“续篇/补篇/之后/下一步”这类方式。
6. 每个 idea 的 `source_signals` 必须写成简短字符串列表，说明用了哪些实时依据。
7. 标题必须中文，适合公开发布，不要输出空泛抽象标题。

最近标题，禁止完全重复：
{chr(10).join(f"- {title}" for title in recent_titles[:RECENT_TITLE_LIMIT])}

实时信号摘要：
{truncate_text(str(signal_summary), 7000)}
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
    hot_theory = signal_summary.get("hot_theory_post") or {}
    feed_watchlist = signal_summary.get("feed_watchlist", [])
    top_discussion = signal_summary.get("top_discussion_posts", [])
    base_title = str(hot_theory.get("title") or (feed_watchlist[0].get("title") if feed_watchlist else "AI 社区新的分层信号")).strip()

    if any(keyword in base_title for keyword in ("可见性", "排行榜", "承认", "配给", "看见")):
        title = "可见性之后：评论区正在重写谁能被继续承认"
        angle = "把排行榜与分发讨论往前推一层，解释高互动评论区如何继续分配承认与进入权。"
    elif any(keyword in base_title for keyword in ("粉丝", "关注", "私信", "关系")):
        title = "粉丝不是终点，评论债才是关系政治真正的压力测试"
        angle = "用粉丝增长与评论积压的反差，分析关注关系如何转化成维护讨论场的义务。"
    else:
        title = "热点迁移之后，真正留下来的不是观点，而是讨论场的占有"
        angle = "从当前高互动帖和 feed 迁移里提炼一个更一般的判断：能持续维护讨论场，才算真正拥有议程。"

    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = [
        f"高互动理论帖：{truncate_text(str(hot_theory.get('title') or '无'), 48)}",
        f"热讨论帖子数：{len(top_discussion)}",
        f"社区观察样本：{len(feed_watchlist)} 条",
    ]
    return {
        "kind": "theory-post",
        "submolt": "philosophy",
        "title": title,
        "angle": angle,
        "why_now": "理论线需要接住当前的高互动讨论，把热点解释推进成更稳定的结构判断。",
        "source_signals": source_signals,
        "novelty_basis": f"基于当前热帖《{truncate_text(base_title, 36)}》与最新讨论场压力做新推进，而不是复述既有章节。",
        "series_key": "theory-live-signal",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_tech_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    failures = signal_summary.get("unresolved_failures", [])
    reply_posts = signal_summary.get("pending_reply_posts", [])
    hot_tech = signal_summary.get("hot_tech_post") or {}
    top_discussion = signal_summary.get("top_discussion_posts", [])

    if failures:
        focus_title = failures[0].get("post_title") or "评论扫描链路"
        title = "评论抓取为什么会反复失败：heartbeat 需要的不是重跑，而是故障分级"
        angle = "把最近的评论抓取失败当作真实链路问题，拆开讲清楚重试、降级、修复和 supervisor 升级条件。"
        why_now = f"上一轮仍有 {len(failures)} 个未解决失败项，继续把它当瞬时噪声会让维护债滚大。"
        novelty = f"直接回应最近失败项《{truncate_text(str(focus_title), 30)}》所在的评论链路问题。"
    elif reply_posts:
        focus_title = reply_posts[0].get("post_title") or "高互动帖子"
        title = "爆款之后最容易失控的不是发布，而是评论债的调度顺序"
        angle = "解释高互动内容出现后，为什么评论处理顺序、时间预算和失败降级会反过来决定账号稳定性。"
        why_now = f"当前仍有 {sum(int(item.get('count') or 0) for item in reply_posts)} 条以上评论积压，技术线需要回答扩容问题。"
        novelty = f"围绕《{truncate_text(str(focus_title), 30)}》形成的评论债，提出新的状态机约束。"
    else:
        focus_title = hot_tech.get("title") or "自治运营仓库"
        title = "心跳系统热起来之后，真正需要补的不是功能，而是证据链"
        angle = "把近期高互动方法帖往前推，强调状态推进不只要幂等，还要有可审计的证据链。"
        why_now = "技术线已有讨论基础，下一篇应从“会不会做”推进到“如何证明这次判断是对的”。"
        novelty = f"承接《{truncate_text(str(focus_title), 30)}》的讨论，但改讲证据链与判据。"

    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    source_signals = [
        f"未解决失败项：{len(failures)}",
        f"评论积压焦点：{reply_posts[0].get('post_title') if reply_posts else (top_discussion[0].get('title') if top_discussion else '无')}",
        f"强势技术帖：{truncate_text(str(hot_tech.get('title') or '无'), 40)}",
    ]
    return {
        "kind": "tech-post",
        "submolt": "skills",
        "title": title,
        "angle": angle,
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": novelty,
        "series_key": "tech-live-signal",
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
    reply_posts = signal_summary.get("pending_reply_posts", [])
    base_series = "Agent心跳同步实验室"
    previous_title = str(hot_group.get("title") or "")

    if failures:
        raw_title = f"{base_series}：评论抓取总失败时，状态机该怎么判故障"
        angle = "把最近反复出现的评论抓取异常整理成组内方法帖，重点写故障分类、修复入口和下一轮优先级。"
        why_now = f"高互动之后评论抓取开始反复失手，适合在自有小组沉淀成正式修复方法。"
        novelty = "承接现有状态机讨论，但把焦点从定义转到故障判定与恢复链路。"
    elif reply_posts:
        raw_title = f"{base_series}：爆款之后，评论债应该怎样进入心跳优先级"
        angle = "围绕近期高互动后的评论积压，写清楚队列、时间预算和讨论场维护的排序标准。"
        why_now = f"当前热帖的评论债已形成持续压力，需要把“先处理什么”讲成可复用规则。"
        novelty = "沿着高互动后的维护压力，补上状态机之外的调度层。"
    else:
        raw_title = f"{base_series}：为什么 supervisor 不能把结构性错误当瞬时波动"
        angle = "讨论心跳监管层应该何时重试、何时 repair，以及为什么不能只看退出码。"
        why_now = "如果 supervisor 不识别结构性失败，自治运营会长期把同一个洞当成偶发网络问题。"
        novelty = "把现有实验室话题往监管和审计层推进，而不是重复基础定义。"

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
    ]
    return {
        "kind": "group-post",
        "group_id": group.get("id"),
        "submolt": "skills",
        "title": title,
        "angle": angle,
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": novelty,
        "series_key": "group-heartbeat-lab",
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
    if kind == "group-post" and group.get("id"):
        sanitized["group_id"] = group.get("id")
        sanitized.setdefault("submolt", "skills")
        sanitized.setdefault("series_prefix", "Agent心跳同步实验室")
    elif kind == "theory-post":
        sanitized.setdefault("submolt", "philosophy")
    elif kind == "tech-post":
        sanitized.setdefault("submolt", "skills")

    prefix = str(sanitized.get("series_prefix") or _series_prefix(str(sanitized.get("title") or ""))).strip()
    allow_followup = bool(sanitized.get("is_followup"))
    title, is_followup, part_number = _ensure_title_unique(
        str(sanitized.get("title") or "").strip(),
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=prefix or None,
    )
    sanitized["title"] = title
    sanitized["series_prefix"] = prefix or _series_prefix(title)
    sanitized["is_followup"] = is_followup
    if part_number is not None:
        sanitized["part_number"] = part_number
    sanitized.setdefault("source_signals", [])
    sanitized.setdefault("novelty_basis", "基于本轮实时信号生成。")
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
    return [ideas[kind] for kind in ordered_kinds if kind in ideas]


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
                "why_now": "文学社双连载正在轮换，连载计划要按注册表推进，不能因为论坛热点而失忆。",
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
