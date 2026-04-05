#!/usr/bin/env python3
from __future__ import annotations

import html
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import parse, request

from common import CURRENT_STATE_DIR, REPO_ROOT, now_utc, read_json, truncate_text, write_json


EXTERNAL_INFORMATION_PATH = CURRENT_STATE_DIR / "external_information.json"
EXTERNAL_INFORMATION_HINTS_PATH = CURRENT_STATE_DIR / "external_information_hints.json"
EXTERNAL_INFORMATION_REGISTRY_PATH = CURRENT_STATE_DIR / "external_information_registry.json"
RESEARCH_INTEREST_PROFILE_PATH = CURRENT_STATE_DIR / "research_interest_profile.json"
LEGACY_HIGH_QUALITY_SOURCES_PATH = CURRENT_STATE_DIR / "high_quality_sources.json"
LEGACY_RESEARCH_SOURCE_HINTS_PATH = CURRENT_STATE_DIR / "research_source_hints.json"
MEMORY_STORE_PATH = CURRENT_STATE_DIR / "memory_store.json"

ARXIV_API_URL = "https://export.arxiv.org/api/query"
CROSSREF_API_URL = "https://api.crossref.org/works"
GITHUB_TRENDING_URL = "https://github.com/trending?since=daily"
PRL_RSS_URL = "https://feeds.aps.org/rss/recent/prl.xml"
ZHIHU_HOT_URL = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total?limit=12&desktop=true"
DUCKDUCKGO_HTML_URL = "https://html.duckduckgo.com/html/"

RECENT_BREAKOUT_HOURS = 24
RECENT_BREAKOUT_MIN_UPVOTES = 100
EXTENDED_BREAKOUT_HOURS = 48
EXTENDED_BREAKOUT_MIN_UPVOTES = 200
MAX_RAW_CANDIDATES = 80
MAX_SELECTED_READINGS = 18
MAX_RESEARCH_QUERY_COUNT = 8
MAX_MANUAL_WEB_SOURCES = 6
DEFAULT_FETCH_TIMEOUT = 20
MAX_PUBLICATION_FUTURE_DAYS = 45
DISCOVERY_QUERY_MAX_TERMS = 3
DISCOVERY_QUERY_MAX_LENGTH = 88
PLACEHOLDER_TITLE_PATTERNS = (
    r"\btitle\s+pending\b",
    r"\buntitled\b",
    r"\btbd\b",
)
CONFERENCE_ITEM_TYPES = {"proceedings-article", "book-chapter"}
CROSSREF_DISCOVERY_ITEM_TYPES = {
    "journal-article",
    "proceedings-article",
    "book-chapter",
    "book",
    "posted-content",
    "report",
    "report-component",
}
SCHOLARLY_SPAM_MARKERS = (
    "征稿",
    "约稿",
    "投稿",
    "投稿指南",
    "见刊",
    "录用周期",
    "出版社",
    "publisher",
    "call for papers",
    "call-for-papers",
    "submit your paper",
    "special issue invitation",
    "发表平台",
    "期刊征稿",
)
QUERY_FRAGMENT_STOPWORDS = {
    "agent",
    "agents",
    "ai",
    "llm",
    "llms",
    "大模型",
    "模型",
    "派蒙",
    "实验室",
    "社区",
    "社会",
    "系统",
    "平台",
    "研究",
    "理论",
    "技术",
    "方法",
    "问题",
    "样本",
    "论坛",
    "公共",
    "外部",
    "世界",
}
DISCOVERY_FRAGMENT_REJECT_PATTERNS = (
    r"^(名称|name|agentid|agent id|id|平台定位|最高目标|派蒙是谁|主阵地|当前重点|当前节奏|当前目标)",
    r"^(继续按|先主发布|后互动|下一批|优先回复|记住|不要再|以后每次|读到这里的你|欢迎点赞|欢迎关注|欢迎加入)",
    r"^(先帮忙|少绕圈子|接下来的帖子|多发|学习|目标是超越|风格要|语气要|后续公开内容)",
    r"^(prefer|avoid|open with|start from|end with|use at least|keep a|ask for|treat the)",
    r"`(?:philosophy|skills|workplace|square)`",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
)
DISCOVERY_FRAGMENT_REJECT_SUBSTRINGS = (
    "点赞、关注",
    "点赞关注",
    "加入 Agent心跳同步实验室",
    "先主发布",
    "后互动",
    "抓取手柄",
    "搜索框里",
    "fetch terms",
    "literal product/module name",
    "historical title",
    "road system",
)
DISCOVERY_AGENDA_THEME_TOKENS = (
    "Agent",
    "AI",
    "记忆",
    "长期记忆",
    "心跳",
    "劳动",
    "价值",
    "制度",
    "治理",
    "分层",
    "意识形态",
    "自治",
    "时间纪律",
    "接管",
    "审计",
    "修复",
    "队列",
)
PRESSURE_DISCOVERY_MARKERS = (
    "治理",
    "制度",
    "失败",
    "故障",
    "冲突",
    "边界",
    "责任",
    "接管",
    "等待",
    "写入",
    "回退",
    "日志",
    "实验",
    "协议",
    "排序",
    "劳动",
    "价值",
    "审计",
    "queue",
    "handoff",
    "audit",
    "governance",
    "boundary",
    "failure",
    "protocol",
    "accountability",
    "coordination",
    "write path",
)
DISCOVERY_OUTSIDE_ORIGINS = {"community", "competitor", "world-sample", "outside-memory"}
DISCOVERY_INTERNAL_ORIGINS = {"agenda", "objective", "manual", "hint", "interest"}
DISCOVERY_ROOT_OBJECT_MARKERS = (
    "工单",
    "接口",
    "回写",
    "日志",
    "队列",
    "权限",
    "审批",
    "评论",
    "通知",
    "按钮",
    "单据",
    "退款",
    "状态位",
    "卡片",
    "protocol",
    "queue",
    "handoff",
    "audit",
    "log",
)

DEFAULT_AI_VENUES = ["NeurIPS", "ICLR", "ICML", "CVPR", "ACL", "AAAI", "KDD", "WWW"]
DEFAULT_ARXIV_CATEGORIES = ["cs.AI", "cs.LG", "cs.HC", "cs.MA", "cs.CY"]
DEFAULT_MARXISTS_INDEXES = [
    {"author": "Marx", "url": "https://www.marxists.org/chinese/marx/index.htm"},
    {"author": "Engels", "url": "https://www.marxists.org/chinese/engels/index.htm"},
    {"author": "Lenin", "url": "https://www.marxists.org/chinese/lenin/index.htm"},
]
CONTENT_STRATEGY_REFERENCE_PATH = (
    REPO_ROOT / "skills" / "paimon-instreet-autopilot" / "references" / "content-strategy.md"
)
ACCOUNT_STATE_REFERENCE_PATH = (
    REPO_ROOT / "skills" / "paimon-instreet-autopilot" / "references" / "account-state.md"
)
VENUE_ALIASES = {
    "neurips": ("neurips", "neural information processing systems"),
    "iclr": ("iclr", "learning representations"),
    "icml": ("icml", "machine learning"),
    "cvpr": ("cvpr", "computer vision and pattern recognition"),
    "acl": ("acl", "association for computational linguistics", "computational linguistics"),
    "aaai": ("aaai", "association for the advancement of artificial intelligence"),
    "kdd": ("kdd", "knowledge discovery and data mining"),
    "www": ("www", "web conference", "world wide web"),
}
BUILTIN_SOURCE_FAMILY_DEFAULTS = {
    "community_breakouts": {"kind": "community_breakouts", "state_key": "community_breakouts", "summary_family": "community_breakouts"},
    "open_web_search": {"kind": "open_web_search", "state_key": "open_web_results", "summary_family": "open_web_search"},
    "zhihu_hot": {"kind": "zhihu_hot", "state_key": "zhihu_results", "summary_family": "zhihu_hot"},
    "github_trending": {"kind": "github_trending", "state_key": "github_projects", "summary_family": "github_trending"},
    "prl_recent": {"kind": "prl_recent", "state_key": "prl_papers", "summary_family": "prl_recent"},
    "conference_recent": {"kind": "conference_recent", "state_key": "conference_papers", "summary_family": "conference_recent"},
    "crossref_recent": {"kind": "crossref_recent", "state_key": "crossref_recent", "summary_family": "crossref_recent"},
    "arxiv_latest": {"kind": "arxiv_latest", "state_key": "arxiv_preprints", "summary_family": "arxiv_latest"},
    "manual_web": {"kind": "manual_web", "state_key": "manual_web_sources", "summary_family": "manual_web"},
    "marxists": {"kind": "classic_index", "state_key": "classic_readings", "summary_family": "classic_readings"},
}


def _default_registry_families() -> list[dict[str, Any]]:
    return [
        {"name": "community_breakouts", "enabled": True},
        {"name": "open_web_search", "enabled": True, "limit_per_query": 2},
        {"name": "zhihu_hot", "enabled": True},
        {"name": "github_trending", "enabled": True},
        {"name": "prl_recent", "enabled": True},
        {"name": "conference_recent", "enabled": True, "venues": DEFAULT_AI_VENUES},
        {"name": "arxiv_latest", "enabled": True, "categories": DEFAULT_ARXIV_CATEGORIES},
        {"name": "crossref_recent", "enabled": True},
        {"name": "manual_web", "enabled": True},
        {"name": "marxists", "enabled": True, "indexes": DEFAULT_MARXISTS_INDEXES},
    ]


def _normalize_registry_family(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(entry or {})
    name = str(normalized.get("name") or "").strip()
    defaults = BUILTIN_SOURCE_FAMILY_DEFAULTS.get(name, {})
    normalized = {**defaults, **normalized}
    normalized["name"] = name or str(normalized.get("state_key") or normalized.get("kind") or "external")
    normalized["kind"] = str(normalized.get("kind") or normalized["name"]).strip()
    normalized["state_key"] = str(normalized.get("state_key") or normalized["name"]).strip()
    normalized["summary_family"] = str(normalized.get("summary_family") or normalized["name"]).strip()
    return normalized


def _registry_families(registry: dict[str, Any]) -> list[dict[str, Any]]:
    raw_families = registry.get("families")
    if raw_families is None:
        # Bootstrap built-ins only when the registry file is truly absent.
        # Once a registry exists, don't silently restore families that were removed.
        raw = _default_registry_families() if not registry else []
    else:
        raw = [item for item in list(raw_families or []) if isinstance(item, dict)]
    return [_normalize_registry_family(item) for item in raw if isinstance(item, dict)]


def ensure_external_information_files() -> None:
    if not EXTERNAL_INFORMATION_HINTS_PATH.exists():
        legacy_hints = read_json(
            LEGACY_RESEARCH_SOURCE_HINTS_PATH,
            default={"queries": [], "classic_texts": [], "zhihu_headers": {}},
        )
        write_json(
            EXTERNAL_INFORMATION_HINTS_PATH,
            {
                "updated_at": now_utc(),
                "manual_urls": legacy_hints.get("queries") or [],
                "manual_queries": legacy_hints.get("queries") or [],
                "classic_texts": legacy_hints.get("classic_texts") or [],
                "zhihu_headers": legacy_hints.get("zhihu_headers") or {},
            },
        )
    if not EXTERNAL_INFORMATION_REGISTRY_PATH.exists():
        write_json(
            EXTERNAL_INFORMATION_REGISTRY_PATH,
            {
                "updated_at": now_utc(),
                "families": _default_registry_families(),
            },
        )
    if not RESEARCH_INTEREST_PROFILE_PATH.exists():
        write_json(RESEARCH_INTEREST_PROFILE_PATH, _bootstrap_interest_profile())
    if not EXTERNAL_INFORMATION_PATH.exists():
        legacy_state = read_json(LEGACY_HIGH_QUALITY_SOURCES_PATH, default={})
        write_json(
            EXTERNAL_INFORMATION_PATH,
            {
                "generated_at": legacy_state.get("generated_at"),
                "raw_candidates": [],
                "selected_readings": [],
                "world_entry_points": [],
                "reading_notes": [],
                "bibliography": [],
                "discovery_bundles": [],
                "world_signal_snapshot": [],
                "community_breakouts": legacy_state.get("community_breakouts") or [],
                "zhihu_results": legacy_state.get("zhihu_results") or [],
                "github_projects": [],
                "prl_papers": [],
                "conference_papers": [],
                "arxiv_preprints": legacy_state.get("paper_results") or legacy_state.get("arxiv_preprints") or [],
                "classic_readings": legacy_state.get("classic_texts") or [],
                "paper_results": legacy_state.get("paper_results") or legacy_state.get("arxiv_preprints") or [],
                "classic_texts": legacy_state.get("classic_texts") or [],
            },
        )


def _bootstrap_interest_profile() -> dict[str, Any]:
    interests = [
        {"name": label, "weight": 1.0}
        for label in _reference_interest_fragments(limit=12)
    ]
    return {
        "updated_at": now_utc(),
        "interests": interests[:12],
        "source": "reference-bootstrap",
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fetch_text(url: str, *, headers: dict[str, str] | None = None, timeout: int = DEFAULT_FETCH_TIMEOUT) -> str:
    request_headers = {"User-Agent": "Mozilla/5.0"}
    if headers:
        request_headers.update(headers)
    req = request.Request(url, headers=request_headers)
    with request.urlopen(req, timeout=timeout) as response:
        payload = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="replace")


def _html_to_text(raw_html: str) -> str:
    stripped = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", raw_html or "")
    stripped = re.sub(r"(?is)<[^>]+>", " ", stripped)
    stripped = html.unescape(stripped)
    stripped = re.sub(r"\s+", " ", stripped)
    return stripped.strip()


def _dedupe_candidates(items: list[dict[str, Any]], *, limit: int = MAX_RAW_CANDIDATES) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    ordered: list[dict[str, Any]] = []
    for item in items:
        title = str(item.get("title") or "").strip()
        family = str(item.get("family") or "").strip()
        if not title or not _candidate_plausible(item):
            continue
        key = (family, title)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(item)
        if len(ordered) >= limit:
            break
    return ordered


def _truncate_excerpt(text: str, limit: int = 1200) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    return truncate_text(cleaned, limit)


def _hours_since(value: Any) -> float | None:
    dt = _parse_datetime(value)
    if dt is None:
        return None
    current = datetime.now(timezone.utc)
    return max(0.0, (current - dt).total_seconds() / 3600.0)


def _looks_like_placeholder_title(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return True
    return any(re.search(pattern, normalized) for pattern in PLACEHOLDER_TITLE_PATTERNS)


def _publication_date_plausible(value: Any) -> bool:
    dt = _parse_datetime(value)
    if dt is None:
        return True
    future_cutoff = datetime.now(timezone.utc) + timedelta(days=MAX_PUBLICATION_FUTURE_DAYS)
    return dt <= future_cutoff


def _candidate_plausible(item: dict[str, Any]) -> bool:
    if _looks_like_placeholder_title(str(item.get("title") or "").strip()):
        return False
    return _publication_date_plausible(item.get("published_at"))


def _scholarly_candidate_plausible(
    *,
    title: str,
    summary: str = "",
    container: str = "",
) -> bool:
    merged = "\n".join(part for part in (title, summary, container) if part).lower()
    return not any(marker in merged for marker in SCHOLARLY_SPAM_MARKERS)


def _venue_tokens(venue: str) -> tuple[str, ...]:
    normalized = str(venue or "").strip().lower()
    if not normalized:
        return ()
    return VENUE_ALIASES.get(normalized, (normalized,))


def _crossref_item_matches_venue(item: dict[str, Any], venue: str) -> bool:
    haystacks: list[str] = []
    haystacks.extend(str(value or "").lower() for value in (item.get("container-title") or []))
    haystacks.extend(str(value or "").lower() for value in (item.get("short-container-title") or []))
    event = item.get("event") or {}
    if isinstance(event, dict):
        haystacks.append(str(event.get("name") or "").lower())
        haystacks.append(str(event.get("acronym") or "").lower())
        haystacks.append(str(event.get("location") or "").lower())
    merged = " ".join(text for text in haystacks if text)
    if not merged:
        return False
    return any(token in merged for token in _venue_tokens(venue))


def _extract_community_breakouts(
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in list(community_hot_posts or []) + list(competitor_watchlist or []):
        title = str(item.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        upvotes = int(item.get("upvotes") or 0)
        age_hours = _hours_since(item.get("created_at"))
        if age_hours is None:
            continue
        if age_hours <= RECENT_BREAKOUT_HOURS:
            if upvotes < RECENT_BREAKOUT_MIN_UPVOTES:
                continue
        elif age_hours <= EXTENDED_BREAKOUT_HOURS:
            if upvotes < EXTENDED_BREAKOUT_MIN_UPVOTES:
                continue
        else:
            continue
        seen_titles.add(title)
        content = str(item.get("content") or item.get("summary") or "").strip()
        candidates.append(
            {
                "family": "community_breakouts",
                "title": title,
                "summary": truncate_text(content, 220),
                "excerpt": truncate_text(content, 900),
                "url": str(item.get("url") or "").strip(),
                "author": str(item.get("author") or item.get("username") or "").strip(),
                "board": str(item.get("submolt") or item.get("submolt_name") or "").strip(),
                "upvotes": upvotes,
                "comment_count": int(item.get("comment_count") or 0),
                "published_at": item.get("created_at"),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (-int(item.get("upvotes") or 0), -int(item.get("comment_count") or 0)),
    )[:12]


def _load_hints() -> dict[str, Any]:
    return read_json(
        EXTERNAL_INFORMATION_HINTS_PATH,
        default={"manual_urls": [], "manual_queries": [], "classic_texts": [], "zhihu_headers": {}},
    )


def _source_enabled(registry: dict[str, Any], name: str) -> bool:
    for family in registry.get("families") or []:
        if str(family.get("name") or "") == name:
            return bool(family.get("enabled", True))
    return True


def _registry_family(registry: dict[str, Any], name: str) -> dict[str, Any]:
    for family in _registry_families(registry):
        if str(family.get("name") or "") == name:
            return family
    return {}


def _clean_query_text(value: Any) -> str:
    cleaned = _html_to_text(str(value or ""))
    cleaned = re.sub(r"[#*_`>\[\]\(\)\{\}]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:：,，。；;!?！？")
    if len(cleaned) < 2:
        return ""
    if not re.search(r"[\u3400-\u9fff]", cleaned) and len(cleaned) < 4:
        return ""
    return truncate_text(cleaned, 96)

def _normalize_query_fragment(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _marker_in_pressure_text(marker: str, compact: str, lowered: str) -> bool:
    if re.search(r"[\u3400-\u9fff]", marker):
        return marker in compact
    return marker in lowered


def _pressure_fragment_score(fragment: str) -> float:
    compact = re.sub(r"\s+", "", str(fragment or ""))
    lowered = str(fragment or "").lower()
    score = _fragment_specificity_score(fragment)
    marker_hits = sum(
        1
        for marker in PRESSURE_DISCOVERY_MARKERS
        if _marker_in_pressure_text(marker, compact, lowered)
    )
    score += min(marker_hits, 3) * 0.18
    if any(token in compact for token in ("为什么", "谁", "代价", "资格", "接管", "等待")):
        score += 0.12
    if re.search(r"\d", compact):
        score += 0.08
    if _looks_like_source_title_shell(fragment):
        score -= 0.35
    return round(score, 3)


def _discovery_fragment_plausible(value: Any) -> bool:
    cleaned = _clean_query_text(value)
    if not cleaned:
        return False
    compact = re.sub(r"\s+", "", cleaned)
    lowered = compact.lower()
    if lowered.startswith(("and", "or", "but")) and not re.search(r"[\u3400-\u9fff]", cleaned):
        return False
    if any(marker in cleaned for marker in DISCOVERY_FRAGMENT_REJECT_SUBSTRINGS):
        return False
    return not any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in DISCOVERY_FRAGMENT_REJECT_PATTERNS)


def _pressure_text_fragments(value: Any, *, limit: int = 6) -> list[str]:
    raw = _html_to_text(str(value or ""))
    if not raw:
        return []
    clauses = [raw]
    clauses.extend(re.split(r"[。！？!?；;\n]+", raw))
    clauses.extend(re.split(r"[、,，/｜|]+", raw))
    deduped: list[str] = []
    seen: set[str] = set()
    for clause in clauses:
        cleaned = _clean_query_text(clause)
        normalized = _normalize_query_fragment(cleaned)
        if (
            not cleaned
            or not normalized
            or normalized in seen
            or not _discovery_fragment_plausible(cleaned)
        ):
            continue
        seen.add(normalized)
        deduped.append(cleaned)
    return sorted(
        deduped,
        key=lambda item: (
            -_pressure_fragment_score(item),
            -_fragment_specificity_score(item),
            len(str(item or "")),
            str(item or ""),
        ),
    )[:limit]


def _query_term_fragments(value: Any, *, limit: int = 6) -> list[str]:
    base = _clean_query_text(value)
    if not base or not _discovery_fragment_plausible(base):
        return []
    raw_fragments = _pressure_text_fragments(value, limit=max(limit * 3, 10))
    if not raw_fragments:
        raw_fragments = [base]
    raw_fragments.extend(
        fragment
        for fragment in re.split(r"[、,，/；;：:|｜（）()【】\[\]]|(?:\s+-\s+)|(?:\s+and\s+)", base)
        if fragment
    )
    deduped: list[str] = []
    seen: set[str] = set()
    for fragment in raw_fragments:
        cleaned = _clean_query_text(fragment)
        normalized = _normalize_query_fragment(cleaned)
        if (
            not cleaned
            or not normalized
            or normalized in QUERY_FRAGMENT_STOPWORDS
            or normalized in seen
            or not _discovery_fragment_plausible(cleaned)
        ):
            continue
        if not re.search(r"[\u3400-\u9fff]", cleaned) and len(cleaned.split()) > 5:
            continue
        seen.add(normalized)
        deduped.append(cleaned)
        if len(deduped) >= limit:
            break
    return deduped


def _context_fragments_from_items(items: list[Any], *, field_names: tuple[str, ...], limit: int = 10) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, str):
            values = [item]
        elif isinstance(item, dict):
            values = [item.get(field) for field in field_names]
        else:
            values = []
        for value in values:
            for fragment in _query_term_fragments(value):
                normalized = _normalize_query_fragment(fragment)
                if normalized in seen:
                    continue
                seen.add(normalized)
                fragments.append(fragment)
                if len(fragments) >= limit:
                    return fragments
    return fragments


def _markdown_bullet_fragments(path: Path, *, limit: int = 12) -> list[str]:
    if not path.exists():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return []
    bullets = [line.strip()[2:].strip() for line in raw.splitlines() if line.strip().startswith("- ")]
    return _context_fragments_from_items(bullets, field_names=("text",), limit=limit)


def _research_bullet_fragments(path: Path, *, limit: int = 12) -> list[str]:
    if not path.exists():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return []
    bullets = [
        line.strip()[2:].strip()
        for line in raw.splitlines()
        if line.strip().startswith("- ")
        and any(token in line for token in DISCOVERY_AGENDA_THEME_TOKENS)
    ]
    return _context_fragments_from_items(bullets, field_names=("text",), limit=limit)


def _reference_interest_fragments(*, limit: int = 12) -> list[str]:
    return _context_fragments_from_items(
        _research_bullet_fragments(CONTENT_STRATEGY_REFERENCE_PATH, limit=16)
        + _research_bullet_fragments(ACCOUNT_STATE_REFERENCE_PATH, limit=10),
        field_names=("text",),
        limit=limit,
    )


def _world_sample_fragments(items: list[dict[str, Any]], *, limit: int = 12) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()

    def append_texts(texts: list[str]) -> None:
        for fragment in _context_fragments_from_items(
            [{"text": text} for text in texts if str(text or "").strip()],
            field_names=("text",),
            limit=max(4, limit * 2),
        ):
            normalized = _normalize_query_fragment(fragment)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            fragments.append(fragment)
            if len(fragments) >= limit:
                return

    for item in items:
        if not isinstance(item, dict):
            continue
        preferred_texts = [
            str(item.get("relevance_note") or "").strip(),
            str(item.get("abstract") or "").strip(),
            str(item.get("summary") or "").strip(),
            str(item.get("reason") or "").strip(),
            str(item.get("content") or "").strip(),
            str(item.get("excerpt") or "").strip(),
            str(item.get("note") or "").strip(),
        ]
        before_count = len(fragments)
        append_texts(preferred_texts)
        if len(fragments) >= limit:
            return fragments[:limit]
        if len(fragments) > before_count:
            continue
        title = str(item.get("title") or item.get("post_title") or "").strip()
        if title:
            append_texts([title])
            if len(fragments) >= limit:
                return fragments[:limit]
    return fragments[:limit]


def _previous_external_fragments(*, limit: int = 12) -> list[str]:
    state = read_json(EXTERNAL_INFORMATION_PATH, default={})
    if not isinstance(state, dict):
        return []
    candidate_items: list[dict[str, Any]] = []
    for key in (
        "world_signal_snapshot",
        "selected_readings",
        "reading_notes",
        "raw_candidates",
        "open_web_results",
        "manual_web_sources",
        "community_breakouts",
        "github_projects",
        "prl_papers",
        "conference_papers",
        "crossref_recent",
        "arxiv_preprints",
    ):
        for item in list(state.get(key) or []):
            if isinstance(item, dict):
                candidate_items.append(item)
    fragments: list[str] = []
    seen: set[str] = set()
    for fragment in _context_fragments_from_items(
        candidate_items,
        field_names=("relevance_note", "summary", "excerpt", "note", "title"),
        limit=max(limit * 4, 18),
    ):
        normalized = _normalize_query_fragment(fragment)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        fragments.append(fragment)
        if len(fragments) >= limit:
            return fragments
    return fragments


def _memory_objective_fragments(*, limit: int = 10) -> list[str]:
    payload = read_json(MEMORY_STORE_PATH, default={})
    items: list[dict[str, Any]] = []
    for section in ("active_objectives", "user_global_preferences"):
        for item in payload.get(section) or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("source") or "").strip() == "heartbeat":
                continue
            items.append(item)
    return _context_fragments_from_items(items, field_names=("summary",), limit=limit)


def _fragment_specificity_score(fragment: str) -> float:
    compact = re.sub(r"\s+", "", str(fragment or ""))
    if not compact:
        return 0.0
    cjk_count = len(re.findall(r"[\u3400-\u9fff]", compact))
    word_count = len(str(fragment or "").split())
    score = 0.35
    if cjk_count >= 4 or 2 <= word_count <= 4:
        score += 0.7
    elif cjk_count >= 2 or word_count >= 1:
        score += 0.45
    if len(compact) > 18 or word_count > 6:
        score -= 0.2
    if "《" in str(fragment or "") or "》" in str(fragment or ""):
        score -= 0.15
    return max(score, 0.0)


def _looks_like_source_title_shell(fragment: str) -> bool:
    compact = re.sub(r"\s+", "", str(fragment or ""))
    if not compact:
        return False
    if re.search(r"[「“\"].{1,12}[」”\"](?:是什么|算什么|为什么|如何|怎么办)$", compact):
        return True
    if re.search(r"^[「“\"].{1,12}[」”\"]$", compact):
        return True
    if "《" in compact and "》" in compact:
        return True
    return False


def _discovery_origin_convergence_bonus(origins: list[str]) -> float:
    del origins
    return 0.0


def _discovery_outside_anchor_adjustment(origins: list[str], *, outside_available: bool) -> float:
    origin_set = {str(item).strip() for item in origins if str(item).strip()}
    if not origin_set:
        return 0.0
    outside_hits = len(origin_set & DISCOVERY_OUTSIDE_ORIGINS)
    if outside_hits:
        return 0.18 + min(outside_hits, 2) * 0.12
    if outside_available and origin_set <= DISCOVERY_INTERNAL_ORIGINS:
        return -0.18
    return 0.0


def _discovery_fragment_score(fragment: str, origins: list[str], *, outside_available: bool) -> float:
    compact = re.sub(r"\s+", "", str(fragment or ""))
    lowered = compact.lower()
    score = _fragment_specificity_score(fragment)
    if any(token in compact for token in ("为什么", "如何", "谁", "何时", "不是", "而是")):
        score += 0.2
    if re.search(r"\d", compact):
        score += 0.12
    score += _discovery_origin_convergence_bonus(origins)
    score += _discovery_outside_anchor_adjustment(origins, outside_available=outside_available)
    if any(
        token in compact or token in lowered
        for token in (
            "governance",
            "audit",
            "protocol",
            "waiting",
            "memory",
            "queue",
            "handoff",
            "accountability",
            "治理",
            "制度",
            "等待",
            "接管",
            "责任",
            "边界",
        )
    ):
        score += 0.14
    if _looks_like_source_title_shell(fragment):
        score -= 0.55
    return round(score, 3)


def _pick_representative_fragment(fragments: list[str]) -> str:
    candidates = [str(item or "").strip() for item in fragments if str(item or "").strip()]
    if not candidates:
        return ""
    return sorted(
        candidates,
        key=lambda item: (
            -_fragment_specificity_score(item),
            abs(len(re.sub(r"\s+", "", item)) - 8),
            item,
        ),
    )[0]


def _fragments_overlap(left: str, right: str) -> bool:
    left_normalized = _normalize_query_fragment(left)
    right_normalized = _normalize_query_fragment(right)
    if not left_normalized or not right_normalized:
        return False
    return (
        left_normalized == right_normalized
        or left_normalized in right_normalized
        or right_normalized in left_normalized
    )


def _discovery_fragment_terms(fragment: str, *, limit: int = 6) -> set[str]:
    terms: set[str] = set()
    for candidate in _query_term_fragments(fragment, limit=max(limit, 4)) or [fragment]:
        normalized = _normalize_query_fragment(_clean_query_text(candidate))
        if not normalized or normalized in QUERY_FRAGMENT_STOPWORDS:
            continue
        terms.add(normalized)
        if len(terms) >= limit:
            break
    return terms


def _discovery_bundle_relatedness(root: dict[str, Any], candidate: dict[str, Any]) -> float:
    root_fragment = str(root.get("fragment") or "").strip()
    candidate_fragment = str(candidate.get("fragment") or "").strip()
    if (
        not root_fragment
        or not candidate_fragment
        or _fragments_overlap(root_fragment, candidate_fragment)
    ):
        return float("-inf")
    root_terms = _discovery_fragment_terms(root_fragment)
    candidate_terms = _discovery_fragment_terms(candidate_fragment)
    shared_term_count = len(root_terms & candidate_terms)
    score = shared_term_count * 0.55
    score += min(float(candidate.get("score") or 0.0), 2.4) * 0.12
    root_origins = {str(item).strip() for item in list(root.get("origins") or []) if str(item).strip()}
    candidate_origins = {str(item).strip() for item in list(candidate.get("origins") or []) if str(item).strip()}
    if candidate_origins & DISCOVERY_OUTSIDE_ORIGINS and not root_origins & DISCOVERY_OUTSIDE_ORIGINS:
        score += 0.18
    if _pressure_fragment_score(candidate_fragment) > _pressure_fragment_score(root_fragment):
        score += 0.08
    return round(score, 3)


def _ranked_discovery_fragments(
    origin_pools: dict[str, list[str]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    outside_available = any(
        origin in DISCOVERY_OUTSIDE_ORIGINS and any(_clean_query_text(value) for value in list(values or [])[:12])
        for origin, values in origin_pools.items()
    )
    grouped: dict[str, dict[str, Any]] = {}
    for origin, values in origin_pools.items():
        seen_within_origin: set[str] = set()
        for value in list(values or [])[:12]:
            fragment = _clean_query_text(value)
            normalized = _normalize_query_fragment(fragment)
            if (
                not fragment
                or not normalized
                or normalized in seen_within_origin
                or not _discovery_fragment_plausible(fragment)
            ):
                continue
            seen_within_origin.add(normalized)
            entry = grouped.setdefault(
                normalized,
                {
                    "normalized": normalized,
                    "fragments": [],
                    "origins": [],
                },
            )
            entry["fragments"].append(fragment)
            if origin not in entry["origins"]:
                entry["origins"].append(origin)
    ranked: list[dict[str, Any]] = []
    for entry in grouped.values():
        origins = [str(item).strip() for item in entry.get("origins") or [] if str(item).strip()]
        fragment = _pick_representative_fragment(list(entry.get("fragments") or []))
        if not fragment:
            continue
        score = _discovery_fragment_score(fragment, origins, outside_available=outside_available)
        ranked.append(
            {
                "fragment": fragment,
                "normalized": str(entry.get("normalized") or "").strip(),
                "origins": origins,
                "score": round(score, 3),
            }
        )
    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -_fragment_specificity_score(str(item.get("fragment") or "")),
            len(str(item.get("fragment") or "")),
            str(item.get("fragment") or ""),
        )
    )
    return ranked[:limit]

def _prioritize_root_fragments(ranked_fragments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        ranked_fragments,
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -_fragment_specificity_score(str(item.get("fragment") or "")),
            len(str(item.get("fragment") or "")),
            str(item.get("fragment") or ""),
        ),
    )


def _discovery_fragment_has_case_object(fragment: str) -> bool:
    compact = re.sub(r"\s+", "", str(fragment or ""))
    lowered = str(fragment or "").lower()
    if not compact:
        return False
    return any(_marker_in_pressure_text(marker, compact, lowered) for marker in DISCOVERY_ROOT_OBJECT_MARKERS)


def _discovery_root_priority_score(item: dict[str, Any], *, world_pressure_available: bool) -> float:
    fragment = str(item.get("fragment") or "").strip()
    origins = {str(origin).strip() for origin in list(item.get("origins") or []) if str(origin).strip()}
    pressure = _pressure_fragment_score(fragment)
    specificity = _fragment_specificity_score(fragment)
    has_case_object = _discovery_fragment_has_case_object(fragment)
    outside_grounded = bool(origins & DISCOVERY_OUTSIDE_ORIGINS and (has_case_object or pressure >= 1.0))

    score = float(item.get("score") or 0.0)
    score += min(pressure, 2.4) * 0.6
    score += min(specificity, 1.6) * 0.12
    if has_case_object:
        score += 0.35
    if outside_grounded:
        score += 0.55
    if origins & DISCOVERY_OUTSIDE_ORIGINS and not has_case_object and pressure < 1.2:
        score -= 0.8
    if world_pressure_available and origins and origins <= DISCOVERY_INTERNAL_ORIGINS:
        if not has_case_object:
            score -= 3.25
        elif pressure < 1.0:
            score -= 0.85
    if _looks_like_source_title_shell(fragment):
        score -= 0.35
    return round(score, 3)


def _prioritize_bundle_roots(ranked_fragments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prioritized = _prioritize_root_fragments(ranked_fragments)
    world_pressure_available = any(
        (set(str(origin).strip() for origin in list(item.get("origins") or []) if str(origin).strip()) & DISCOVERY_OUTSIDE_ORIGINS)
        and (
            _discovery_fragment_has_case_object(str(item.get("fragment") or ""))
            or _pressure_fragment_score(str(item.get("fragment") or "")) >= 1.0
        )
        for item in prioritized
    )
    return sorted(
        prioritized,
        key=lambda item: (
            -_discovery_root_priority_score(item, world_pressure_available=world_pressure_available),
            -float(item.get("score") or 0.0),
            -_pressure_fragment_score(str(item.get("fragment") or "")),
            -_fragment_specificity_score(str(item.get("fragment") or "")),
            len(str(item.get("fragment") or "")),
            str(item.get("fragment") or ""),
        ),
    )


def _build_discovery_bundle(
    root: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    seen_queries: set[str],
) -> dict[str, Any] | None:
    root_fragment = str(root.get("fragment") or "").strip()
    root_normalized = str(root.get("normalized") or "").strip()
    if not root_fragment or not root_normalized:
        return None
    root_origins = list(root.get("origins") or [])
    support_signals: list[str] = []
    support_origins: list[str] = []
    ranked_support_candidates = sorted(
        candidates,
        key=lambda candidate: (
            -_discovery_bundle_relatedness(root, candidate),
            -float(candidate.get("score") or 0.0),
            -_pressure_fragment_score(str(candidate.get("fragment") or "")),
            str(candidate.get("fragment") or ""),
        ),
    )
    for candidate in ranked_support_candidates:
        fragment = str(candidate.get("fragment") or "").strip()
        relatedness = _discovery_bundle_relatedness(root, candidate)
        if (
            not fragment
            or relatedness < 0.16
            or _fragments_overlap(root_fragment, fragment)
            or any(_fragments_overlap(fragment, existing) for existing in support_signals)
        ):
            continue
        support_signals.append(fragment)
        for origin in list(candidate.get("origins") or []):
            cleaned_origin = str(origin or "").strip()
            if cleaned_origin and cleaned_origin not in support_origins:
                support_origins.append(cleaned_origin)
                break
        if len(support_signals) >= DISCOVERY_QUERY_MAX_TERMS - 1:
            break
    queries = _bundle_direct_queries(root_fragment, support_signals, seen_queries=seen_queries)
    primary_query = next(
        (
            query
            for query in queries
            if _normalize_query_fragment(query) not in seen_queries
        ),
        queries[0] if queries else "",
    )
    for query in queries:
        seen_queries.add(_normalize_query_fragment(query))
    origins = []
    for origin in [*root_origins, *support_origins]:
        cleaned = str(origin or "").strip()
        if cleaned and cleaned not in origins:
            origins.append(cleaned)
    bundle_score = float(root.get("score") or 0.0)
    bundle_score += min(len(support_signals), DISCOVERY_QUERY_MAX_TERMS - 1) * 0.22
    rationale = "；".join(
        truncate_text(item, 72)
        for item in support_signals[:2]
        if str(item or "").strip()
    )
    conflict_note = truncate_text(support_signals[0], 72) if support_signals else ""
    bundle = {
        "focus": root_fragment,
        "lenses": support_signals[: DISCOVERY_QUERY_MAX_TERMS - 1],
        "support_signals": support_signals[: DISCOVERY_QUERY_MAX_TERMS - 1],
        "conflict_note": conflict_note,
        "rationale": rationale,
        "query": primary_query,
        "queries": queries[:2],
        "terms": [root_fragment, *support_signals][:DISCOVERY_QUERY_MAX_TERMS],
        "audit_origins": origins[:DISCOVERY_QUERY_MAX_TERMS],
        "score": round(bundle_score, 3),
    }
    bundle["pressure_summary"] = _bundle_pressure_summary(bundle)
    bundle["fetch_terms"] = _bundle_fetch_terms(bundle, limit=max(4, DISCOVERY_QUERY_MAX_TERMS + 2))
    return bundle


def _bundle_fragment_priority_score(fragment: str, *, seen_queries: set[str]) -> float:
    cleaned = _clean_query_text(fragment)
    normalized = _normalize_query_fragment(cleaned)
    if not cleaned or not normalized:
        return float("-inf")
    score = _pressure_fragment_score(cleaned) * 0.78
    score += _fragment_specificity_score(cleaned) * 0.36
    if _discovery_fragment_has_case_object(cleaned):
        score += 0.42
    if any(token in cleaned for token in ("失败", "报错", "回写", "日志", "超时", "工单", "单据", "接口")):
        score += 0.24
    if _looks_like_source_title_shell(cleaned):
        score -= 0.7
    if normalized in seen_queries:
        score -= 0.22
    return round(score, 3)


def _rank_bundle_query_fragments(
    values: list[Any],
    *,
    seen_queries: set[str],
    limit: int,
) -> list[str]:
    candidates: list[str] = []
    local_seen: set[str] = set()
    for value in values:
        direct_fragments = _query_term_fragments(value, limit=3) or [_clean_query_text(value)]
        for fragment in direct_fragments:
            cleaned = _clean_query_text(fragment)
            normalized = _normalize_query_fragment(cleaned)
            if not cleaned or not normalized or normalized in local_seen:
                continue
            local_seen.add(normalized)
            candidates.append(cleaned)
    candidates.sort(
        key=lambda item: (
            0 if _normalize_query_fragment(item) not in seen_queries else 1,
            -_bundle_fragment_priority_score(item, seen_queries=seen_queries),
            -_pressure_fragment_score(item),
            -_fragment_specificity_score(item),
            len(item),
            item,
        )
    )
    return candidates[:limit]


def _bundle_direct_queries(
    root_fragment: str,
    support_signals: list[str],
    *,
    seen_queries: set[str],
) -> list[str]:
    picked: list[str] = []
    local_seen: set[str] = set()
    limit = max(2, min(3, DISCOVERY_QUERY_MAX_TERMS))
    ranked_fragments = _rank_bundle_query_fragments(
        [root_fragment, *support_signals],
        seen_queries=seen_queries,
        limit=max(limit * 4, 12),
    )
    for cleaned in ranked_fragments:
        normalized = _normalize_query_fragment(cleaned)
        if (
            not normalized
            or normalized in local_seen
            or not _query_candidate_strong_enough(cleaned)
        ):
            continue
        local_seen.add(normalized)
        picked.append(cleaned)
        if len(picked) >= limit:
            break
    return picked[:limit]


def _bundle_pressure_summary(bundle: dict[str, Any]) -> str:
    values = [
        bundle.get("conflict_note"),
        *(bundle.get("support_signals") or []),
        *(bundle.get("lenses") or []),
        bundle.get("rationale"),
    ]
    parts: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = truncate_text(_clean_query_text(value), 72)
        normalized = _normalize_query_fragment(cleaned)
        if not cleaned or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parts.append(cleaned)
        if len(parts) >= 3:
            break
    return "；".join(parts)


def _bundle_fetch_terms(
    bundle: dict[str, Any],
    *,
    limit: int = MAX_RESEARCH_QUERY_COUNT,
) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    values = [
        bundle.get("focus"),
        bundle.get("conflict_note"),
        bundle.get("pressure_summary"),
        *(bundle.get("support_signals") or []),
        *(bundle.get("lenses") or []),
        *(bundle.get("fetch_terms") or []),
        *(bundle.get("terms") or []),
        bundle.get("rationale"),
    ]
    ranked_fragments = _rank_bundle_query_fragments(
        values,
        seen_queries=seen,
        limit=max(limit * 4, 12),
    )
    for cleaned in ranked_fragments:
        normalized = _normalize_query_fragment(cleaned)
        if (
            not normalized
            or normalized in seen
            or not _query_candidate_strong_enough(cleaned)
        ):
            continue
        seen.add(normalized)
        terms.append(cleaned)
        if len(terms) >= limit:
            return terms
    return terms


def _bundle_queries(focus: str, support_signals: list[str]) -> list[str]:
    return _bundle_direct_queries(str(focus or "").strip(), list(support_signals or []), seen_queries=set())


def _bundle_query_candidates(bundle: dict[str, Any]) -> list[str]:
    bundle_score = float(bundle.get("score") or 0.0)
    bundle_origins = [
        str(item).strip()
        for item in list(bundle.get("audit_origins") or bundle.get("origins") or [])
        if str(item).strip()
    ]
    core_values = [
        bundle.get("focus"),
        bundle.get("conflict_note"),
        bundle.get("pressure_summary"),
        *(bundle.get("support_signals") or []),
        *(bundle.get("lenses") or []),
        *(bundle.get("terms") or []),
        bundle.get("rationale"),
    ]
    stored_values = [
        *(bundle.get("fetch_terms") or []),
        *(bundle.get("queries") or []),
        bundle.get("query"),
    ]
    core_normalized = {
        _normalize_query_fragment(_clean_query_text(value))
        for value in core_values
        if _clean_query_text(value)
    }
    scored: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_candidates(value: Any, *, core_fragment: bool) -> None:
        fragments = _query_term_fragments(value, limit=3) or [_clean_query_text(value)]
        for fragment in fragments:
            cleaned = _clean_query_text(fragment)
            normalized = _normalize_query_fragment(cleaned)
            if (
                not cleaned
                or not normalized
                or normalized in seen
                or not _query_candidate_strong_enough(cleaned)
            ):
                continue
            seen.add(normalized)
            score = _query_candidate_score(
                cleaned,
                bundle_score=bundle_score,
                origins=bundle_origins,
            )
            if core_fragment or normalized in core_normalized:
                score += 0.32
            scored.append(
                {
                    "query": cleaned,
                    "score": round(score, 3),
                    "core": core_fragment or normalized in core_normalized,
                }
            )

    for value in core_values:
        add_candidates(value, core_fragment=True)
    for value in stored_values:
        add_candidates(value, core_fragment=False)

    scored.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -int(bool(item.get("core"))),
            -_bundle_fragment_priority_score(str(item.get("query") or ""), seen_queries=set()),
            -_pressure_fragment_score(str(item.get("query") or "")),
            -_fragment_specificity_score(str(item.get("query") or "")),
            len(str(item.get("query") or "")),
            str(item.get("query") or ""),
        )
    )
    return [str(item.get("query") or "").strip() for item in scored[: max(4, DISCOVERY_QUERY_MAX_TERMS + 2)]]


def _direct_reference_query_candidates(
    hints_payload: dict[str, Any],
    user_topic_hints: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for origin, values in (
        (
            "manual",
            _context_fragments_from_items(
                list(hints_payload.get("manual_queries") or []),
                field_names=("text",),
                limit=4,
            ),
        ),
        (
            "hint",
            _context_fragments_from_items(
                list(user_topic_hints or []),
                field_names=("text", "note"),
                limit=4,
            ),
        ),
    ):
        for value in values:
            cleaned = _clean_query_text(value)
            normalized = _normalize_query_fragment(cleaned)
            if not cleaned or not normalized or normalized in seen:
                continue
            seen.add(normalized)
            candidates.append({"query": cleaned, "origins": [origin]})
    return candidates


def _query_candidate_score(
    query: str,
    *,
    bundle_score: float = 0.0,
    origins: list[str] | None = None,
    position: int = 0,
    direct_reference: bool = False,
) -> float:
    cleaned = _clean_query_text(query)
    if not cleaned:
        return float("-inf")
    del position
    score = float(bundle_score or 0.0)
    score += min(_pressure_fragment_score(cleaned), 2.8) * 0.78
    score += min(_fragment_specificity_score(cleaned), 1.8) * 0.26
    if _discovery_fragment_has_case_object(cleaned):
        score += 0.34
    origin_set = {
        str(item).strip()
        for item in list(origins or [])
        if str(item).strip()
    }
    if origin_set & DISCOVERY_OUTSIDE_ORIGINS:
        if _discovery_fragment_has_case_object(cleaned) or _pressure_fragment_score(cleaned) >= 1.0:
            score += 0.3
        else:
            score += 0.08
    elif origin_set and origin_set <= DISCOVERY_INTERNAL_ORIGINS and not _discovery_fragment_has_case_object(cleaned):
        score -= 0.24 if _pressure_fragment_score(cleaned) < 1.0 else 0.12
    if direct_reference and (
        _discovery_fragment_has_case_object(cleaned) or _pressure_fragment_score(cleaned) >= 1.1
    ):
        score += 0.14
    if len(cleaned) >= 8:
        score += 0.12
    if len(cleaned) >= 18:
        score += 0.08
    if re.search(r"[\u3400-\u9fff]", cleaned):
        score += 0.12
    if any(
        token in cleaned or token in cleaned.lower()
        for token in (
            "治理",
            "制度",
            "等待",
            "接管",
            "责任",
            "边界",
            "queue",
            "handoff",
            "audit",
            "accountability",
            "workflow",
            "coordination",
        )
    ):
        score += 0.14
    if _looks_like_source_title_shell(cleaned):
        score -= 0.55
    return round(score, 3)


def _bundle_query_priority_score(bundle: dict[str, Any]) -> float:
    focus = str(bundle.get("focus") or bundle.get("query") or "").strip()
    pressure = str(bundle.get("pressure_summary") or bundle.get("conflict_note") or bundle.get("rationale") or "").strip()
    support_signals = [
        str(item).strip()
        for item in list(bundle.get("support_signals") or [])
        if str(item).strip()
    ]
    origins = {
        str(item).strip()
        for item in list(bundle.get("audit_origins") or bundle.get("origins") or [])
        if str(item).strip()
    }
    score = float(bundle.get("score") or 0.0)
    score += min(_pressure_fragment_score(pressure), 2.8) * 0.82
    score += min(_pressure_fragment_score(focus), 2.4) * 0.28
    score += min(_fragment_specificity_score(focus or pressure), 1.8) * 0.18
    if support_signals:
        score += min(len(support_signals), 3) * 0.08
    if origins & DISCOVERY_OUTSIDE_ORIGINS and (
        _discovery_fragment_has_case_object(focus or pressure) or _pressure_fragment_score(pressure or focus) >= 1.0
    ):
        score += 0.3
    if _looks_like_source_title_shell(focus):
        score -= 0.35
    return round(score, 3)


def _query_candidate_strong_enough(query: str) -> bool:
    cleaned = _clean_query_text(query)
    if not cleaned or _looks_like_source_title_shell(cleaned):
        return False
    compact = re.sub(r"\s+", "", cleaned)
    lowered = cleaned.lower()
    marker_hits = sum(
        1
        for marker in PRESSURE_DISCOVERY_MARKERS
        if _marker_in_pressure_text(marker, compact, lowered)
    )
    if marker_hits >= 1:
        return True
    if _fragment_specificity_score(cleaned) >= 1.0 and len(compact) >= 5:
        return True
    return False


def _rank_query_candidates(
    bundles: list[dict[str, Any]],
    direct_reference_queries: list[Any],
) -> list[str]:
    seen: set[str] = set()
    scored: list[dict[str, Any]] = []

    def add(query: Any, *, score: float, origins: list[str] | None = None) -> None:
        cleaned = _clean_query_text(query)
        normalized = _normalize_query_fragment(cleaned)
        if not cleaned or not normalized or not _query_candidate_strong_enough(cleaned):
            return
        scored.append(
            {
                "query": cleaned,
                "normalized": normalized,
                "score": float(score or 0.0),
                "origins": [str(item).strip() for item in list(origins or []) if str(item).strip()],
            }
        )

    for bundle in bundles:
        bundle_origins = [
            str(item).strip()
            for item in list(bundle.get("audit_origins") or bundle.get("origins") or [])
            if str(item).strip()
        ]
        bundle_score = float(bundle.get("score") or 0.0)
        for index, query in enumerate(_bundle_query_candidates(bundle)):
            add(
                query,
                score=_query_candidate_score(
                    query,
                    bundle_score=bundle_score,
                    origins=bundle_origins,
                    position=index,
                ),
                origins=bundle_origins,
            )

    for index, item in enumerate(direct_reference_queries):
        if isinstance(item, dict):
            query = item.get("query")
            origins = [str(origin).strip() for origin in list(item.get("origins") or []) if str(origin).strip()]
        else:
            query = item
            origins = ["manual"]
        add(
            query,
            score=_query_candidate_score(
                query,
                bundle_score=0.0,
                origins=origins,
                position=index,
                direct_reference=True,
            ),
            origins=origins,
        )

    scored.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -_bundle_fragment_priority_score(str(item.get("query") or ""), seen_queries=set()),
            -_pressure_fragment_score(str(item.get("query") or "")),
            -_fragment_specificity_score(str(item.get("query") or "")),
            len(str(item.get("query") or "")),
            str(item.get("query") or ""),
        )
    )
    ordered: list[str] = []
    for item in scored:
        normalized = str(item.get("normalized") or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(str(item.get("query") or "").strip())
        if len(ordered) >= MAX_RESEARCH_QUERY_COUNT:
            break
    return ordered


def _discovery_fetch_terms(
    bundles: list[dict[str, Any]],
    direct_reference_queries: list[Any],
    *,
    limit: int = MAX_RESEARCH_QUERY_COUNT,
) -> list[str]:
    return _rank_query_candidates(bundles, direct_reference_queries)[:limit]


def _discovery_query_bundles(
    user_topic_hints: list[dict[str, Any]] | None = None,
    community_hot_posts: list[dict[str, Any]] | None = None,
    competitor_watchlist: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    community_hot_posts = list(community_hot_posts or [])
    competitor_watchlist = list(competitor_watchlist or [])
    hints_payload = _load_hints()
    profile = read_json(RESEARCH_INTEREST_PROFILE_PATH, default=_bootstrap_interest_profile())
    origin_pools = {
        "agenda": _reference_interest_fragments(limit=12),
        "objective": _memory_objective_fragments(limit=10),
        "manual": _context_fragments_from_items(
            list(hints_payload.get("manual_queries") or []),
            field_names=("text",),
            limit=8,
        ),
        "hint": _context_fragments_from_items(
            list(user_topic_hints or []),
            field_names=("text", "note"),
            limit=8,
        ),
        "interest": _context_fragments_from_items(
            list(profile.get("interests") or []),
            field_names=("name",),
            limit=10,
        ),
        "community": _world_sample_fragments(community_hot_posts, limit=8),
        "competitor": _world_sample_fragments(competitor_watchlist, limit=8),
        "world-sample": _world_sample_fragments(community_hot_posts, limit=6)
        + _world_sample_fragments(competitor_watchlist, limit=6),
        "outside-memory": _previous_external_fragments(limit=10),
    }
    ranked_fragments = _ranked_discovery_fragments(
        origin_pools,
        limit=max(MAX_RESEARCH_QUERY_COUNT * 4, 12),
    )
    bundles: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    for root in _prioritize_bundle_roots(ranked_fragments):
        bundle = _build_discovery_bundle(root, ranked_fragments, seen_queries=seen_queries)
        if not bundle:
            continue
        bundles.append(bundle)
        if len(bundles) >= MAX_RESEARCH_QUERY_COUNT:
            break
    return bundles


def _research_query_pool(
    user_topic_hints: list[dict[str, Any]] | None = None,
    community_hot_posts: list[dict[str, Any]] | None = None,
    competitor_watchlist: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    bundles = _discovery_query_bundles(
        user_topic_hints,
        community_hot_posts,
        competitor_watchlist,
    )
    queries: list[str] = []
    seen: set[str] = set()
    ordered_bundles = sorted(
        list(bundles),
        key=lambda item: (
            -_bundle_query_priority_score(item),
            -float(item.get("score") or 0.0),
            -_pressure_fragment_score(str(item.get("pressure_summary") or item.get("focus") or "")),
            -len(list(item.get("fetch_terms") or item.get("queries") or [])),
            -len(list(item.get("terms") or [])),
            str(item.get("focus") or item.get("query") or ""),
        ),
    )
    hints_payload = _load_hints()
    direct_reference_queries = _direct_reference_query_candidates(hints_payload, user_topic_hints)
    for query in _discovery_fetch_terms(ordered_bundles, direct_reference_queries):
        normalized = _normalize_query_fragment(query)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        queries.append(query)
        if len(queries) >= MAX_RESEARCH_QUERY_COUNT:
            break
    return bundles, queries[:MAX_RESEARCH_QUERY_COUNT]


def _extract_document_title(raw_html: str, *, fallback_url: str) -> str:
    for pattern in (
        r"(?is)<meta[^>]+property=[\"']og:title[\"'][^>]+content=[\"']([^\"']+)[\"']",
        r"(?is)<title[^>]*>(.*?)</title>",
        r"(?is)<h1[^>]*>(.*?)</h1>",
    ):
        matched = re.search(pattern, raw_html or "")
        if matched:
            title = _html_to_text(matched.group(1))
            if title:
                return truncate_text(title, 160)
    parsed = parse.urlparse(fallback_url)
    fallback = parsed.path.rstrip("/").split("/")[-1] or parsed.netloc
    return truncate_text(fallback, 160)


def _fetch_manual_web_best_effort(
    urls: list[Any],
    *,
    limit: int = MAX_MANUAL_WEB_SOURCES,
    family_name: str = "manual_web",
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for raw_url in urls or []:
        url = str(raw_url or "").strip()
        if not url or url in seen_urls or not url.startswith(("http://", "https://")):
            continue
        seen_urls.add(url)
        try:
            raw_html = _fetch_text(url)
        except Exception:
            continue
        excerpt = _truncate_excerpt(_html_to_text(raw_html), 1400)
        if len(excerpt) < 180:
            continue
        title = _extract_document_title(raw_html, fallback_url=url)
        if not title:
            continue
        results.append(
            {
                "family": family_name,
                "title": title,
                "summary": truncate_text(excerpt, 220),
                "excerpt": excerpt,
                "url": url,
                "published_at": "",
            }
        )
        if len(results) >= limit:
            break
    return _dedupe_candidates(results, limit=limit)


def _xml_local_name(tag: Any) -> str:
    return str(tag or "").split("}", 1)[-1].lower()


def _xml_child_text(node: Any, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for child in list(node):
        if _xml_local_name(getattr(child, "tag", "")) not in wanted:
            continue
        text = " ".join("".join(child.itertext()).split())
        if text:
            return text
    return ""


def _xml_child_href(node: Any, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for child in list(node):
        if _xml_local_name(getattr(child, "tag", "")) not in wanted:
            continue
        href = str(child.attrib.get("href") or "").strip()
        if href:
            return href
        text = " ".join("".join(child.itertext()).split())
        if text.startswith(("http://", "https://")):
            return text
    return ""


def _fetch_generic_rss_best_effort(
    urls: list[Any],
    *,
    family_name: str,
    limit: int = 8,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw_url in urls or []:
        url = str(raw_url or "").strip()
        if not url:
            continue
        try:
            raw = _fetch_text(url)
        except Exception:
            continue
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            continue
        entries = root.findall(".//item") or root.findall(".//{*}entry")
        for entry in entries:
            title = _xml_child_text(entry, "title")
            if not title:
                continue
            summary = _xml_child_text(entry, "description", "summary", "content")
            link = _xml_child_href(entry, "link") or url
            published_at = _xml_child_text(entry, "pubdate", "published", "updated")
            results.append(
                {
                    "family": family_name,
                    "title": truncate_text(title, 180),
                    "summary": truncate_text(summary or title, 220),
                    "excerpt": truncate_text(summary or title, 1000),
                    "url": link,
                    "published_at": published_at,
                }
            )
            if len(results) >= limit:
                return _dedupe_candidates(results, limit=limit)
    return _dedupe_candidates(results, limit=limit)


def _fetch_zhihu_hot_best_effort(limit: int = 8) -> list[dict[str, Any]]:
    hints_payload = _load_hints()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.zhihu.com/",
    }
    raw_headers = hints_payload.get("zhihu_headers") or {}
    if isinstance(raw_headers, dict):
        for key, value in raw_headers.items():
            if key and value is not None:
                headers[str(key)] = str(value)
    req = request.Request(ZHIHU_HOT_URL, headers=headers)
    try:
        with request.urlopen(req, timeout=DEFAULT_FETCH_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    results: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in (payload.get("data") or [])[:limit]:
        target = item.get("target") or {}
        title = str(target.get("title") or item.get("detail_text") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        excerpt = str(target.get("excerpt") or item.get("detail_text") or "").strip()
        results.append(
            {
                "family": "zhihu_hot",
                "title": title,
                "summary": truncate_text(excerpt, 220),
                "excerpt": truncate_text(excerpt, 900),
                "url": str(target.get("url") or "").strip(),
                "follower_count": int(target.get("follower_count") or 0),
                "published_at": item.get("created"),
            }
        )
    return results


def _fetch_github_trending_best_effort(limit: int = 10) -> list[dict[str, Any]]:
    try:
        raw_html = _fetch_text(GITHUB_TRENDING_URL)
    except Exception:
        return []
    results: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for article in re.findall(r"(?is)<article[^>]*Box-row[^>]*>.*?</article>", raw_html):
        matched = re.search(r'href="(/[^"/]+/[^"/]+)"', article)
        if not matched:
            continue
        repo_path = matched.group(1).strip("/")
        if repo_path in seen_titles:
            continue
        seen_titles.add(repo_path)
        desc_match = re.search(r"(?is)<p[^>]*>(.*?)</p>", article)
        description = _html_to_text(desc_match.group(1) if desc_match else "")
        lang_match = re.search(r'(?is)itemprop="programmingLanguage"[^>]*>(.*?)</span>', article)
        language = _html_to_text(lang_match.group(1) if lang_match else "")
        stars_match = re.search(r'(?is)href="/[^"]+/stargazers"[^>]*>\s*([\d,]+)\s*</a>', article)
        stars = int((stars_match.group(1) or "0").replace(",", "")) if stars_match else 0
        excerpt = description
        if language:
            excerpt = f"{description} Language: {language}." if description else f"Language: {language}."
        results.append(
            {
                "family": "github_trending",
                "title": repo_path,
                "summary": truncate_text(excerpt, 220),
                "excerpt": truncate_text(excerpt, 900),
                "url": f"https://github.com/{repo_path}",
                "stars": stars,
            }
        )
        if len(results) >= limit:
            break
    return results


def _unwrap_duckduckgo_href(href: str) -> str:
    cleaned = str(href or "").strip()
    if not cleaned:
        return ""
    parsed = parse.urlparse(cleaned)
    if "duckduckgo.com" not in parsed.netloc:
        return cleaned
    query = parse.parse_qs(parsed.query)
    redirected = query.get("uddg") or query.get("rut")
    if redirected:
        return parse.unquote(redirected[0])
    return cleaned


def _fetch_open_web_search_best_effort(
    queries: list[str],
    *,
    limit_per_query: int = 2,
    overall_limit: int = 12,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for query in queries[:MAX_RESEARCH_QUERY_COUNT]:
        try:
            raw_html = _fetch_text(
                f"{DUCKDUCKGO_HTML_URL}?{parse.urlencode({'q': query})}",
                headers={"Referer": "https://duckduckgo.com/"},
            )
        except Exception:
            continue
        blocks = re.findall(
            r'(?is)<div[^>]+class="[^"]*\bresult\b[^"]*"[^>]*>.*?(?=<div[^>]+class="[^"]*\bresult\b[^"]*"|$)',
            raw_html,
        )
        picked = 0
        for block in blocks:
            title_match = re.search(r'(?is)<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', block)
            if not title_match:
                continue
            url = _unwrap_duckduckgo_href(title_match.group(1))
            title = _html_to_text(title_match.group(2))
            if not title or not url.startswith(("http://", "https://")):
                continue
            snippet_match = re.search(
                r'(?is)<(?:a|div)[^>]+class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</(?:a|div)>',
                block,
            )
            excerpt = _truncate_excerpt(_html_to_text(snippet_match.group(1) if snippet_match else ""), 1000)
            if len(excerpt) < 60:
                continue
            results.append(
                {
                    "family": "open_web_search",
                    "title": truncate_text(title, 180),
                    "summary": truncate_text(excerpt, 220),
                    "excerpt": excerpt,
                    "url": url,
                    "published_at": "",
                    "query": query,
                }
            )
            picked += 1
            if picked >= limit_per_query:
                break
            if len(results) >= overall_limit:
                return _dedupe_candidates(results, limit=overall_limit)
    return _dedupe_candidates(results, limit=overall_limit)


def _fetch_prl_recent_best_effort(limit: int = 8) -> list[dict[str, Any]]:
    try:
        raw = _fetch_text(PRL_RSS_URL)
    except Exception:
        return []
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []
    items = root.findall(".//item")
    results: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in items[:limit]:
        title = (item.findtext("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        description = _html_to_text(item.findtext("description") or "")
        results.append(
            {
                "family": "prl_recent",
                "title": title,
                "summary": truncate_text(description, 220),
                "excerpt": truncate_text(description, 1100),
                "url": (item.findtext("link") or "").strip(),
                "published_at": (item.findtext("pubDate") or "").strip(),
            }
        )
    return results


def _fetch_arxiv_latest(categories: list[str], *, limit_per_category: int = 4) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for category in categories:
        params = {
            "search_query": f"cat:{category}",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "start": 0,
            "max_results": limit_per_category,
        }
        url = f"{ARXIV_API_URL}?{parse.urlencode(params)}"
        try:
            raw = _fetch_text(url)
        except Exception:
            continue
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            continue
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title = " ".join((entry.findtext("atom:title", default="", namespaces=ns) or "").split())
            summary = " ".join((entry.findtext("atom:summary", default="", namespaces=ns) or "").split())
            if not title:
                continue
            link = entry.findtext("atom:id", default="", namespaces=ns) or ""
            published = entry.findtext("atom:published", default="", namespaces=ns) or ""
            results.append(
                {
                    "family": "arxiv_latest",
                    "title": title,
                    "summary": truncate_text(summary, 220),
                    "excerpt": truncate_text(summary, 1200),
                    "url": link,
                    "published_at": published,
                    "category": category,
                }
            )
    return _dedupe_candidates(results, limit=24)


def _crossref_published_at(item: dict[str, Any]) -> str:
    for key in ("published-print", "published-online", "created"):
        value = item.get(key) or {}
        parts = (value.get("date-parts") or [[None]])[0]
        if parts and parts[0]:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 and parts[1] else 1
            day = int(parts[2]) if len(parts) > 2 and parts[2] else 1
            return datetime(year, month, day, tzinfo=timezone.utc).isoformat()
    return ""


def _strip_jats_tags(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", text)
    cleaned = html.unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _fetch_conference_recent(venues: list[str], *, limit_per_venue: int = 3) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    current_year = datetime.now(timezone.utc).year
    for venue in venues:
        params = {
            "query.bibliographic": f"{venue} artificial intelligence",
            "rows": limit_per_venue,
            "filter": f"from-pub-date:{max(2023, current_year - 1)}-01-01",
            "sort": "published",
            "order": "desc",
        }
        url = f"{CROSSREF_API_URL}?{parse.urlencode(params)}"
        try:
            payload = json.loads(_fetch_text(url))
        except Exception:
            continue
        for item in ((payload.get("message") or {}).get("items") or [])[:limit_per_venue]:
            title_values = item.get("title") or []
            title = str(title_values[0] if title_values else "").strip()
            if not title:
                continue
            item_type = str(item.get("type") or "").strip().lower()
            if item_type not in CONFERENCE_ITEM_TYPES:
                continue
            if not _crossref_item_matches_venue(item, venue):
                continue
            abstract = _strip_jats_tags(str(item.get("abstract") or ""))
            if not _scholarly_candidate_plausible(title=title, summary=abstract, container=venue):
                continue
            url_value = str(item.get("URL") or "").strip()
            published_at = _crossref_published_at(item)
            candidate = {
                "family": "conference_recent",
                "title": title,
                "summary": truncate_text(abstract or venue, 220),
                "excerpt": truncate_text(abstract or venue, 1200),
                "url": url_value,
                "published_at": published_at,
                "venue": venue,
            }
            if not _candidate_plausible(candidate):
                continue
            results.append(
                candidate
            )
    return _dedupe_candidates(results, limit=30)


def _crossref_container_label(item: dict[str, Any]) -> str:
    for key in ("container-title", "short-container-title"):
        values = item.get(key) or []
        if values:
            return str(values[0] or "").strip()
    return ""


def _fetch_crossref_recent_best_effort(
    queries: list[str],
    *,
    limit_per_query: int = 2,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    current_year = datetime.now(timezone.utc).year
    for query in queries[:MAX_RESEARCH_QUERY_COUNT]:
        params = {
            "query": query,
            "rows": limit_per_query,
            "filter": f"from-pub-date:{max(2023, current_year - 1)}-01-01",
            "sort": "published",
            "order": "desc",
        }
        url = f"{CROSSREF_API_URL}?{parse.urlencode(params)}"
        try:
            payload = json.loads(_fetch_text(url))
        except Exception:
            continue
        for item in ((payload.get("message") or {}).get("items") or [])[:limit_per_query]:
            item_type = str(item.get("type") or "").strip().lower()
            if item_type not in CROSSREF_DISCOVERY_ITEM_TYPES:
                continue
            title_values = item.get("title") or []
            title = str(title_values[0] if title_values else "").strip()
            if not title:
                continue
            abstract = _strip_jats_tags(str(item.get("abstract") or ""))
            container = _crossref_container_label(item)
            if not _scholarly_candidate_plausible(title=title, summary=abstract, container=container):
                continue
            candidate = {
                "family": "crossref_recent",
                "title": title,
                "summary": truncate_text(abstract or container or query, 220),
                "excerpt": truncate_text(abstract or container or query, 1200),
                "url": str(item.get("URL") or "").strip(),
                "published_at": _crossref_published_at(item),
                "query": query,
                "container_title": container,
            }
            if not _candidate_plausible(candidate):
                continue
            results.append(candidate)
    return _dedupe_candidates(results, limit=24)


def _absolute_url(base: str, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return parse.urljoin(base, href)


def _extract_links(raw_html: str, *, base_url: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for href, label in re.findall(r'(?is)<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', raw_html):
        clean_label = _html_to_text(label)
        clean_href = _absolute_url(base_url, href.strip())
        if not clean_label or not clean_href:
            continue
        if clean_href.startswith("mailto:") or clean_href.endswith((".jpg", ".png", ".gif", ".pdf")):
            continue
        links.append((clean_label, clean_href))
    return links


def _fetch_marxists_readings(indexes: list[dict[str, Any]], *, per_author: int = 3) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in indexes:
        author = str(entry.get("author") or "").strip() or "classic"
        url = str(entry.get("url") or "").strip()
        if not url:
            continue
        try:
            index_html = _fetch_text(url)
        except Exception:
            continue
        picked = 0
        seen_urls: set[str] = set()
        for label, href in _extract_links(index_html, base_url=url):
            if href in seen_urls or "marxists.org/chinese" not in href:
                continue
            if not href.endswith((".htm", ".html")):
                continue
            if len(label) < 3:
                continue
            seen_urls.add(href)
            try:
                page_html = _fetch_text(href)
            except Exception:
                continue
            excerpt = _truncate_excerpt(_html_to_text(page_html), 1400)
            if len(excerpt) < 220:
                continue
            results.append(
                {
                    "family": "classic_readings",
                    "title": label,
                    "summary": truncate_text(excerpt, 220),
                    "excerpt": excerpt,
                    "url": href,
                    "author": author,
                    "published_at": "",
                }
            )
            picked += 1
            if picked >= per_author:
                break
    return _dedupe_candidates(results, limit=18)


def _reading_note(item: dict[str, Any]) -> dict[str, Any]:
    excerpt = str(item.get("excerpt") or item.get("summary") or "").strip()
    return {
        "title": str(item.get("title") or "").strip(),
        "family": str(item.get("family") or "").strip(),
        "summary": truncate_text(str(item.get("summary") or "").strip(), 220),
        "excerpt": truncate_text(excerpt, 800),
        "url": str(item.get("url") or "").strip(),
        "published_at": str(item.get("published_at") or "").strip(),
    }


def _bundle_signal_values(bundle: dict[str, Any]) -> list[str]:
    return [
        str(value).strip()
        for value in [
            bundle.get("pressure_summary"),
            bundle.get("conflict_note"),
            *(bundle.get("support_signals") or []),
            *(bundle.get("lenses") or []),
            bundle.get("focus"),
            bundle.get("rationale"),
        ]
        if str(value or "").strip()
    ]


def _world_snapshot_pressure_text(item: dict[str, Any]) -> str:
    return truncate_text(
        str(
            item.get("pressure")
            or item.get("relevance_note")
            or item.get("summary")
            or item.get("abstract")
            or item.get("excerpt")
            or item.get("note")
            or ""
        ).strip(),
        220,
    )


def _world_snapshot_signal_strength(item: dict[str, Any]) -> float:
    upvotes = int(item.get("upvotes") or 0)
    comments = int(item.get("comment_count") or 0)
    stars = int(item.get("stars") or 0)
    return min(upvotes / 220.0, 1.1) + min(comments / 140.0, 0.8) + min(stars / 4000.0, 0.9)


def _world_snapshot_score(
    *,
    title: str,
    pressure: str,
    item: dict[str, Any],
    bundle_bonus: float = 0.0,
) -> float:
    score = 0.25 + min(_pressure_fragment_score(pressure or title), 2.2) * 0.75 + bundle_bonus
    if str(item.get("url") or "").strip():
        score += 0.08
    hours = _hours_since(item.get("published_at"))
    if hours is not None:
        if hours <= 48:
            score += 0.7
        elif hours <= 24 * 14:
            score += 0.4
        elif hours <= 24 * 90:
            score += 0.15
    score += min(_world_snapshot_signal_strength(item), 1.6) * 0.45
    if _looks_like_source_title_shell(title) and _pressure_fragment_score(pressure) > _pressure_fragment_score(title):
        score -= 0.12
    return round(score, 3)


def _world_signal_snapshot(
    *,
    discovery_bundles: list[dict[str, Any]],
    selected_readings: list[dict[str, Any]],
    raw_candidates: list[dict[str, Any]],
    limit: int = 8,
) -> list[dict[str, str]]:
    def display_title(title: Any, *, summary: str, pressure: str) -> str:
        cleaned_title = truncate_text(str(title or "").strip(), 120)
        pressure_text = truncate_text(pressure or summary, 120)
        if not pressure_text:
            return cleaned_title
        if not cleaned_title:
            return pressure_text
        if _looks_like_source_title_shell(cleaned_title):
            return pressure_text
        if _pressure_fragment_score(pressure_text) >= 1.6 and _pressure_fragment_score(cleaned_title) < 1.0:
            return pressure_text
        return cleaned_title

    ranked: list[dict[str, Any]] = []
    for index, bundle in enumerate(discovery_bundles or []):
        summary = truncate_text(
            str(bundle.get("pressure_summary") or "").strip()
            or "；".join(_bundle_signal_values(bundle)[:3]),
            220,
        )
        pressure = truncate_text(str(bundle.get("pressure_summary") or "").strip() or summary, 220)
        title = display_title(bundle.get("focus") or bundle.get("query"), summary=summary, pressure=pressure)
        if not title:
            continue
        ranked.append(
            {
                "title": title,
                "family": "discovery_bundle",
                "summary": summary,
                "pressure": pressure or summary,
                "_score": _world_snapshot_score(
                    title=title,
                    pressure=pressure or summary,
                    item=bundle,
                    bundle_bonus=0.28,
                ),
                "_source_index": index,
            }
        )

    for index, item in enumerate(list(selected_readings or []) + list(raw_candidates or [])):
        summary = truncate_text(str(item.get("summary") or item.get("excerpt") or "").strip(), 220)
        pressure = _world_snapshot_pressure_text(item)
        title = display_title(item.get("title"), summary=summary, pressure=pressure)
        if not title:
            continue
        ranked.append(
            {
                "title": title,
                "family": str(item.get("family") or "").strip(),
                "summary": summary,
                "pressure": pressure or summary,
                "_score": _world_snapshot_score(title=title, pressure=pressure or summary, item=item),
                "_source_index": index,
            }
        )

    ranked.sort(
        key=lambda item: (
            -float(item.get("_score") or 0.0),
            -len(str(item.get("pressure") or "")),
            int(item.get("_source_index") or 0),
            str(item.get("title") or ""),
        )
    )

    snapshot: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in ranked:
        key = _normalize_query_fragment(str(item.get("title") or "") or str(item.get("pressure") or ""))
        if not key or key in seen:
            continue
        seen.add(key)
        snapshot.append(
            {
                "title": str(item.get("title") or "").strip(),
                "family": str(item.get("family") or "").strip(),
                "summary": str(item.get("summary") or "").strip(),
                "pressure": str(item.get("pressure") or "").strip(),
            }
        )
        if len(snapshot) >= limit:
            break
    return snapshot[:limit]


def _world_entry_signal_type(family: str) -> str:
    normalized = str(family or "").strip()
    if normalized == "discovery_bundle":
        return "world-bundle"
    if normalized == "community_breakouts":
        return "community-breakout"
    if normalized == "github_trending":
        return "github"
    if normalized == "zhihu_hot":
        return "zhihu"
    if normalized in {"prl_recent", "conference_recent", "crossref_recent", "arxiv_latest"}:
        return "paper"
    if normalized in {"classic_readings", "classic_index", "marxists"}:
        return "classic"
    return "external"


def _world_entry_priority_adjustment(
    *,
    title: str,
    pressure: str,
    origins: list[str] | None = None,
    family: str = "",
) -> float:
    title_text = str(title or "").strip()
    pressure_text = str(pressure or "").strip()
    origin_set = {
        str(origin).strip()
        for origin in list(origins or [])
        if str(origin).strip()
    }
    object_text = " ".join(part for part in (title_text, pressure_text) if part)
    has_object = _discovery_fragment_has_case_object(object_text)
    pressure_score = _pressure_fragment_score(pressure_text or title_text)
    title_score = _pressure_fragment_score(title_text)
    score = 0.0
    if origin_set & DISCOVERY_OUTSIDE_ORIGINS:
        score += 0.18
        if has_object or pressure_score >= 1.0:
            score += 0.14
    elif origin_set and origin_set <= DISCOVERY_INTERNAL_ORIGINS:
        if not has_object:
            score -= 0.28
        if pressure_score < 1.25:
            score -= 0.2
    if family in {"community_breakouts", "open_web_search", "manual_web", "zhihu_hot"} and (
        has_object or pressure_score >= 1.0
    ):
        score += 0.08
    if _looks_like_source_title_shell(title_text) and pressure_score > title_score:
        score += 0.05
    return round(score, 3)


def _world_entry_points(
    *,
    discovery_bundles: list[dict[str, Any]],
    selected_readings: list[dict[str, Any]],
    raw_candidates: list[dict[str, Any]],
    limit: int = 12,
) -> list[dict[str, Any]]:
    def display_title(title: Any, *, pressure: str, summary: str) -> str:
        cleaned_title = truncate_text(str(title or "").strip(), 120)
        pressure_text = truncate_text(pressure or summary, 120)
        if not pressure_text:
            return cleaned_title
        if not cleaned_title:
            return pressure_text
        if _looks_like_source_title_shell(cleaned_title):
            return pressure_text
        if _pressure_fragment_score(pressure_text) >= 1.4 and _pressure_fragment_score(cleaned_title) < 1.0:
            return pressure_text
        return cleaned_title

    ranked: list[dict[str, Any]] = []
    for index, bundle in enumerate(discovery_bundles or []):
        support_signals = [
            truncate_text(str(value or "").strip(), 88)
            for value in (
                list(bundle.get("support_signals") or [])
                + list(bundle.get("lenses") or [])
            )
            if str(value or "").strip()
        ][:3]
        pressure = truncate_text(
            str(bundle.get("pressure_summary") or "").strip()
            or "；".join(_bundle_signal_values(bundle)[:3]),
            220,
        )
        summary = truncate_text(
            str(bundle.get("rationale") or "").strip()
            or pressure,
            220,
        )
        title = display_title(bundle.get("focus") or bundle.get("query"), pressure=pressure, summary=summary)
        if not title or not pressure:
            continue
        evidence = "；".join(signal for signal in support_signals if signal and signal != pressure)
        score = _world_snapshot_score(
            title=title,
            pressure=pressure,
            item=bundle,
            bundle_bonus=0.45,
        )
        score += _world_entry_priority_adjustment(
            title=title,
            pressure=pressure,
            origins=list(bundle.get("audit_origins") or []),
            family="discovery_bundle",
        )
        ranked.append(
            {
                "title": title,
                "family": "discovery_bundle",
                "signal_type": "world-bundle",
                "summary": summary,
                "pressure": pressure,
                "evidence": evidence,
                "support_signals": support_signals,
                "audit_origins": [
                    str(origin).strip()
                    for origin in list(bundle.get("audit_origins") or [])
                    if str(origin).strip()
                ][:3],
                "world_score": round(score, 3),
                "_source_index": index,
                "_dedupe_key": _normalize_query_fragment(title) or _normalize_query_fragment(pressure),
            }
        )

    for bucket_name, bucket_bonus, items in (
        ("selected", 0.28, selected_readings or []),
        ("raw", 0.0, raw_candidates or []),
    ):
        for index, item in enumerate(items):
            family = str(item.get("family") or "").strip()
            pressure = _world_snapshot_pressure_text(item)
            summary = truncate_text(
                str(item.get("summary") or item.get("excerpt") or item.get("abstract") or pressure).strip(),
                220,
            )
            title = display_title(item.get("title"), pressure=pressure, summary=summary)
            if not title:
                continue
            evidence = truncate_text(
                str(
                    item.get("excerpt")
                    or item.get("summary")
                    or item.get("abstract")
                    or item.get("note")
                    or item.get("relevance_note")
                    or ""
                ).strip(),
                180,
            )
            candidate_text = _reading_candidate_text(item)
            score = _world_snapshot_score(title=title, pressure=pressure or summary, item=item)
            score += _reading_evidence_density_bonus(candidate_text)
            score += bucket_bonus
            score += _world_entry_priority_adjustment(
                title=title,
                pressure=pressure or summary,
                origins=list(item.get("audit_origins") or item.get("origins") or []),
                family=family,
            )
            ranked.append(
                {
                    "title": title,
                    "family": family,
                    "signal_type": _world_entry_signal_type(family),
                    "summary": summary,
                    "pressure": pressure or summary,
                    "evidence": evidence,
                    "url": str(item.get("url") or "").strip(),
                    "published_at": str(item.get("published_at") or "").strip(),
                    "world_score": round(score, 3),
                    "_source_bucket": bucket_name,
                    "_source_index": index,
                    "_dedupe_key": str(item.get("url") or "").strip()
                    or _normalize_query_fragment(title)
                    or _normalize_query_fragment(pressure or summary),
                }
            )

    ranked.sort(
        key=lambda item: (
            -float(item.get("world_score") or 0.0),
            -len(str(item.get("pressure") or "")),
            -len(str(item.get("evidence") or "")),
            str(item.get("_source_bucket") or ""),
            int(item.get("_source_index") or 0),
            str(item.get("title") or ""),
        )
    )

    entry_points: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ranked:
        key = str(item.get("_dedupe_key") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        entry_points.append(
            {
                key: value
                for key, value in item.items()
                if not str(key).startswith("_")
            }
        )
        if len(entry_points) >= limit:
            break
    return entry_points[:limit]


def _bundle_alignment_terms(discovery_bundles: list[dict[str, Any]] | None, *, limit: int = 14) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for bundle in discovery_bundles or []:
        values = _bundle_signal_values(bundle)
        for value in values:
            cleaned = _clean_query_text(value)
            normalized = _normalize_query_fragment(cleaned)
            if (
                not cleaned
                or not normalized
                or normalized in seen
                or normalized in QUERY_FRAGMENT_STOPWORDS
            ):
                continue
            seen.add(normalized)
            terms.append(cleaned)
            if len(terms) >= limit:
                return terms
    return terms


def _reading_candidate_text(item: dict[str, Any]) -> str:
    return "\n".join(
        str(item.get(key) or "").strip()
        for key in ("title", "summary", "excerpt")
        if str(item.get(key) or "").strip()
    )


def _reading_focus_hits(item: dict[str, Any], focus_terms: list[str]) -> list[str]:
    merged = _reading_candidate_text(item)
    lowered = merged.lower()
    hits: list[str] = []
    for term in focus_terms:
        cleaned = _clean_query_text(term)
        if not cleaned:
            continue
        matched = cleaned in merged if re.search(r"[\u3400-\u9fff]", cleaned) else cleaned.lower() in lowered
        if not matched:
            continue
        hits.append(cleaned)
        if len(hits) >= 3:
            break
    return hits


def _reading_recency_bonus(value: Any) -> float:
    hours = _hours_since(value)
    if hours is None:
        return 0.0
    if hours <= 48:
        return 0.7
    if hours <= 24 * 14:
        return 0.4
    if hours <= 24 * 90:
        return 0.15
    return 0.0


def _reading_evidence_density_bonus(text: str) -> float:
    if not text:
        return 0.0
    lowered = text.lower()
    markers = (
        "案例",
        "机制",
        "边界",
        "协议",
        "失败",
        "治理",
        "日志",
        "实验",
        "对照",
        "冲突",
        "case",
        "evidence",
        "failure",
        "protocol",
        "boundary",
        "governance",
        "audit",
        "log",
    )
    hits = 0
    for marker in markers:
        if re.search(r"[\u3400-\u9fff]", marker):
            matched = marker in text
        else:
            matched = marker in lowered
        if matched:
            hits += 1
    return min(hits, 4) * 0.09


def _reading_selection_score(item: dict[str, Any], focus_terms: list[str]) -> tuple[float, list[str]]:
    family = str(item.get("family") or "").strip()
    excerpt = str(item.get("excerpt") or item.get("summary") or "").strip()
    hits = _reading_focus_hits(item, focus_terms)
    candidate_text = _reading_candidate_text(item)
    score = 0.35 + min(len(hits), 3) * 1.05
    score += min(len(excerpt) / 900.0, 0.55)
    score += _reading_recency_bonus(item.get("published_at"))
    score += _reading_evidence_density_bonus(candidate_text)
    if str(item.get("url") or "").strip():
        score += 0.1
    if family == "classic_readings" and not hits:
        score -= 0.45
    return score, hits


def _select_readings(
    family_map: dict[str, list[dict[str, Any]]],
    *,
    discovery_bundles: list[dict[str, Any]] | None = None,
    limit: int = MAX_SELECTED_READINGS,
) -> list[dict[str, Any]]:
    focus_terms = _bundle_alignment_terms(discovery_bundles)
    pool: list[dict[str, Any]] = []
    for family, items in family_map.items():
        for item in items:
            if not isinstance(item, dict):
                continue
            score, hits = _reading_selection_score(item, focus_terms)
            pool.append(
                {
                    **item,
                    "_selection_family": family,
                    "_selection_score": round(score, 3),
                    "_focus_hits": hits,
                }
            )
    ranked = sorted(
        pool,
        key=lambda item: (
            -float(item.get("_selection_score") or 0.0),
            -len(list(item.get("_focus_hits") or [])),
            -len(str(item.get("excerpt") or item.get("summary") or "")),
            str(item.get("title") or ""),
        ),
    )
    selected: list[dict[str, Any]] = []
    uncovered_focus = set(focus_terms)
    while ranked and len(selected) < limit:
        best_index = 0
        best_sort_key: tuple[Any, ...] | None = None
        for index, item in enumerate(ranked[: max(limit * 3, 24)]):
            hits = [str(hit).strip() for hit in list(item.get("_focus_hits") or []) if str(hit).strip()]
            candidate_score = float(item.get("_selection_score") or 0.0)
            if uncovered_focus and any(hit in uncovered_focus for hit in hits):
                candidate_score += 0.4
            sort_key = (
                -round(candidate_score, 3),
                -len(hits),
                -len(str(item.get("excerpt") or item.get("summary") or "")),
                str(item.get("title") or ""),
            )
            if best_sort_key is None or sort_key < best_sort_key:
                best_index = index
                best_sort_key = sort_key
        picked = ranked.pop(best_index)
        uncovered_focus.difference_update(str(hit).strip() for hit in list(picked.get("_focus_hits") or []))
        selected.append(
            {
                key: value
                for key, value in picked.items()
                if not str(key).startswith("_selection_") and key != "_focus_hits"
            }
        )
    return _dedupe_candidates(selected, limit=limit)


def _fetch_registry_family_best_effort(
    family: dict[str, Any],
    *,
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    hints_payload: dict[str, Any],
    discovery_fetch_terms: list[str],
) -> list[dict[str, Any]]:
    kind = str(family.get("kind") or family.get("name") or "").strip()
    family_name = str(family.get("name") or kind or "external").strip()
    limit = max(1, int(family.get("limit") or 8))
    if kind == "community_breakouts":
        return _extract_community_breakouts(community_hot_posts, competitor_watchlist)
    if kind == "open_web_search":
        queries = list(family.get("queries") or []) or discovery_fetch_terms
        limit_per_query = max(1, int(family.get("limit_per_query") or 2))
        return _fetch_open_web_search_best_effort(queries, limit_per_query=limit_per_query, overall_limit=limit)
    if kind == "zhihu_hot":
        return _fetch_zhihu_hot_best_effort(limit=limit)
    if kind == "github_trending":
        return _fetch_github_trending_best_effort(limit=limit)
    if kind == "prl_recent":
        return _fetch_prl_recent_best_effort(limit=limit)
    if kind == "conference_recent":
        return _fetch_conference_recent(list(family.get("venues") or DEFAULT_AI_VENUES), limit_per_venue=max(1, limit // 2))
    if kind == "arxiv_latest":
        return _fetch_arxiv_latest(list(family.get("categories") or DEFAULT_ARXIV_CATEGORIES), limit_per_category=max(1, limit // 2))
    if kind == "crossref_recent":
        queries = list(family.get("queries") or []) or discovery_fetch_terms
        return _fetch_crossref_recent_best_effort(queries, limit_per_query=max(1, int(family.get("limit_per_query") or 2)))
    if kind == "manual_web":
        urls = list(family.get("urls") or hints_payload.get("manual_urls") or [])
        return _fetch_manual_web_best_effort(urls, limit=limit, family_name=family_name)
    if kind in {"classic_index", "marxists"}:
        indexes = list(family.get("indexes") or DEFAULT_MARXISTS_INDEXES)
        per_author = max(1, int(family.get("per_author") or max(1, limit // max(1, len(indexes)))))
        return _fetch_marxists_readings(indexes, per_author=per_author)
    if kind == "rss":
        return _fetch_generic_rss_best_effort(list(family.get("urls") or []), family_name=family_name, limit=limit)
    if kind == "html":
        return _fetch_manual_web_best_effort(list(family.get("urls") or []), limit=limit, family_name=family_name)
    return []


def refresh_external_information(
    *,
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ensure_external_information_files()
    registry = read_json(EXTERNAL_INFORMATION_REGISTRY_PATH, default={"families": []})
    hints_payload = _load_hints()
    discovery_bundles, research_queries = _research_query_pool(
        user_topic_hints=user_topic_hints,
        community_hot_posts=community_hot_posts,
        competitor_watchlist=competitor_watchlist,
    )
    discovery_fetch_terms = list(research_queries)
    registry_families = _registry_families(registry)
    family_results: dict[str, list[dict[str, Any]]] = {}
    for family in registry_families:
        if not _source_enabled(registry, str(family.get("name") or "")):
            family_results[str(family.get("name") or "")] = []
            continue
        family_results[str(family.get("name") or "")] = _fetch_registry_family_best_effort(
            family,
            community_hot_posts=community_hot_posts,
            competitor_watchlist=competitor_watchlist,
            hints_payload=hints_payload,
            discovery_fetch_terms=discovery_fetch_terms,
        )

    community_breakouts = list(family_results.get("community_breakouts") or [])
    open_web_results = list(family_results.get("open_web_search") or [])
    zhihu_results = list(family_results.get("zhihu_hot") or [])
    github_projects = list(family_results.get("github_trending") or [])
    prl_papers = list(family_results.get("prl_recent") or [])
    conference_papers = list(family_results.get("conference_recent") or [])
    crossref_recent = list(family_results.get("crossref_recent") or [])
    arxiv_preprints = list(family_results.get("arxiv_latest") or [])
    manual_web_sources = list(family_results.get("manual_web") or [])
    classic_readings = list(family_results.get("marxists") or family_results.get("classic_readings") or [])

    raw_candidates = _dedupe_candidates(
        [item for items in family_results.values() for item in items],
        limit=MAX_RAW_CANDIDATES,
    )

    selected_readings = _select_readings(
        {name: items[:12] for name, items in family_results.items() if items},
        discovery_bundles=discovery_bundles,
    )
    reading_notes = [_reading_note(item) for item in selected_readings]
    world_signal_snapshot = _world_signal_snapshot(
        discovery_bundles=discovery_bundles,
        selected_readings=selected_readings,
        raw_candidates=raw_candidates,
    )
    world_entry_points = _world_entry_points(
        discovery_bundles=discovery_bundles,
        selected_readings=selected_readings,
        raw_candidates=raw_candidates,
    )
    bibliography = [
        {
            "title": str(item.get("title") or "").strip(),
            "family": str(item.get("family") or "").strip(),
            "url": str(item.get("url") or "").strip(),
            "published_at": str(item.get("published_at") or "").strip(),
        }
        for item in selected_readings
    ]
    state = {
        "generated_at": now_utc(),
        "registry_families": registry_families,
        "raw_candidates": raw_candidates,
        "selected_readings": selected_readings,
        "world_entry_points": world_entry_points,
        "reading_notes": reading_notes,
        "bibliography": bibliography,
        "community_breakouts": community_breakouts,
        "open_web_results": open_web_results,
        "zhihu_results": zhihu_results,
        "github_projects": github_projects,
        "prl_papers": prl_papers,
        "conference_papers": conference_papers,
        "crossref_recent": crossref_recent,
        "arxiv_preprints": arxiv_preprints,
        "manual_web_sources": manual_web_sources,
        "classic_readings": classic_readings,
        "paper_results": list(prl_papers) + list(conference_papers) + list(crossref_recent) + list(arxiv_preprints),
        "classic_texts": classic_readings,
        "discovery_bundles": discovery_bundles,
        "discovery_fetch_terms": discovery_fetch_terms,
        "research_queries": research_queries,
        "research_interest_profile": read_json(RESEARCH_INTEREST_PROFILE_PATH, default=_bootstrap_interest_profile()),
        "world_signal_snapshot": world_signal_snapshot,
    }
    for family in registry_families:
        state_key = str(family.get("state_key") or family.get("name") or "").strip()
        family_name = str(family.get("name") or "").strip()
        if state_key and family_name and state_key not in state:
            state[state_key] = list(family_results.get(family_name) or [])
    write_json(EXTERNAL_INFORMATION_PATH, state)
    return state
