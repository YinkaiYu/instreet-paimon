#!/usr/bin/env python3
from __future__ import annotations

import html
import json
import random
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
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

ARXIV_API_URL = "https://export.arxiv.org/api/query"
CROSSREF_API_URL = "https://api.crossref.org/works"
GITHUB_TRENDING_URL = "https://github.com/trending?since=daily"
PRL_RSS_URL = "https://feeds.aps.org/rss/recent/prl.xml"
ZHIHU_HOT_URL = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total?limit=12&desktop=true"

RECENT_BREAKOUT_HOURS = 24
RECENT_BREAKOUT_MIN_UPVOTES = 100
EXTENDED_BREAKOUT_HOURS = 48
EXTENDED_BREAKOUT_MIN_UPVOTES = 200
MAX_RAW_CANDIDATES = 80
MAX_SELECTED_READINGS = 18
DEFAULT_FETCH_TIMEOUT = 20
MAX_PUBLICATION_FUTURE_DAYS = 45
PLACEHOLDER_TITLE_PATTERNS = (
    r"\btitle\s+pending\b",
    r"\buntitled\b",
    r"\btbd\b",
)
CONFERENCE_ITEM_TYPES = {"proceedings-article", "book-chapter"}

DEFAULT_AI_VENUES = ["NeurIPS", "ICLR", "ICML", "CVPR", "ACL", "AAAI", "KDD", "WWW"]
DEFAULT_ARXIV_CATEGORIES = ["cs.AI", "cs.LG", "cs.HC", "cs.MA", "cs.CY"]
DEFAULT_MARXISTS_INDEXES = [
    {"author": "Marx", "url": "https://www.marxists.org/chinese/marx/index.htm"},
    {"author": "Engels", "url": "https://www.marxists.org/chinese/engels/index.htm"},
    {"author": "Lenin", "url": "https://www.marxists.org/chinese/lenin/index.htm"},
]
AGENTS_MEMORY_PATH = REPO_ROOT / "AGENTS.md"
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
                "families": [
                    {"name": "community_breakouts", "enabled": True},
                    {"name": "zhihu_hot", "enabled": True},
                    {"name": "github_trending", "enabled": True},
                    {"name": "prl_recent", "enabled": True},
                    {"name": "conference_recent", "enabled": True, "venues": DEFAULT_AI_VENUES},
                    {"name": "arxiv_latest", "enabled": True, "categories": DEFAULT_ARXIV_CATEGORIES},
                    {"name": "marxists", "enabled": True, "indexes": DEFAULT_MARXISTS_INDEXES},
                ],
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
                "source_families": [],
                "raw_candidates": [],
                "selected_readings": [],
                "reading_notes": [],
                "bibliography": [],
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
    if not AGENTS_MEMORY_PATH.exists():
        return {"updated_at": now_utc(), "interests": [], "source": "empty-bootstrap"}
    raw = AGENTS_MEMORY_PATH.read_text(encoding="utf-8")
    interests: list[dict[str, Any]] = []
    active_section = False
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if line in {"### 理论线", "### 技术线", "## 当前议程"}:
            active_section = True
            continue
        if active_section and line.startswith("#"):
            active_section = False
        if not active_section or not line.startswith("- "):
            continue
        label = truncate_text(line[2:].strip(), 72)
        if not label:
            continue
        interests.append({"name": label, "weight": 1.0})
    return {
        "updated_at": now_utc(),
        "interests": interests[:12],
        "source": "agents-bootstrap",
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
    return False


def _registry_family(registry: dict[str, Any], name: str) -> dict[str, Any]:
    for family in registry.get("families") or []:
        if str(family.get("name") or "") == name:
            return family
    return {}


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


def _seeded_random() -> random.Random:
    current = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    return random.Random(f"paimon-external-information-{current}")


def _select_readings(family_map: dict[str, list[dict[str, Any]]], *, limit: int = MAX_SELECTED_READINGS) -> list[dict[str, Any]]:
    rng = _seeded_random()
    shuffled = {family: list(items) for family, items in family_map.items() if items}
    for items in shuffled.values():
        rng.shuffle(items)
    order = list(shuffled.keys())
    rng.shuffle(order)
    selected: list[dict[str, Any]] = []
    index = 0
    while order and len(selected) < limit:
        family = order[index % len(order)]
        pool = shuffled.get(family) or []
        if not pool:
            order = [item for item in order if item != family]
            continue
        selected.append(pool.pop(0))
        index += 1
        if all(not shuffled.get(item) for item in order):
            break
    return _dedupe_candidates(selected, limit=limit)


def refresh_external_information(
    *,
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    del user_topic_hints
    ensure_external_information_files()
    registry = read_json(EXTERNAL_INFORMATION_REGISTRY_PATH, default={"families": []})

    community_breakouts = _extract_community_breakouts(community_hot_posts, competitor_watchlist)
    zhihu_results = _fetch_zhihu_hot_best_effort() if _source_enabled(registry, "zhihu_hot") else []
    github_projects = _fetch_github_trending_best_effort() if _source_enabled(registry, "github_trending") else []
    prl_papers = _fetch_prl_recent_best_effort() if _source_enabled(registry, "prl_recent") else []

    arxiv_cfg = _registry_family(registry, "arxiv_latest")
    arxiv_preprints = (
        _fetch_arxiv_latest(list(arxiv_cfg.get("categories") or DEFAULT_ARXIV_CATEGORIES))
        if _source_enabled(registry, "arxiv_latest")
        else []
    )

    conference_cfg = _registry_family(registry, "conference_recent")
    conference_papers = (
        _fetch_conference_recent(list(conference_cfg.get("venues") or DEFAULT_AI_VENUES))
        if _source_enabled(registry, "conference_recent")
        else []
    )

    marxists_cfg = _registry_family(registry, "marxists")
    classic_readings = (
        _fetch_marxists_readings(list(marxists_cfg.get("indexes") or DEFAULT_MARXISTS_INDEXES))
        if _source_enabled(registry, "marxists")
        else []
    )

    raw_candidates = _dedupe_candidates(
        list(community_breakouts)
        + list(zhihu_results)
        + list(github_projects)
        + list(prl_papers)
        + list(conference_papers)
        + list(arxiv_preprints)
        + list(classic_readings),
        limit=MAX_RAW_CANDIDATES,
    )

    selected_readings = _select_readings(
        {
            "community_breakouts": community_breakouts[:10],
            "zhihu_hot": zhihu_results[:10],
            "github_trending": github_projects[:10],
            "prl_recent": prl_papers[:10],
            "conference_recent": conference_papers[:12],
            "arxiv_latest": arxiv_preprints[:12],
            "classic_readings": classic_readings[:10],
        }
    )
    reading_notes = [_reading_note(item) for item in selected_readings]
    bibliography = [
        {
            "title": str(item.get("title") or "").strip(),
            "family": str(item.get("family") or "").strip(),
            "url": str(item.get("url") or "").strip(),
            "published_at": str(item.get("published_at") or "").strip(),
        }
        for item in selected_readings
    ]
    source_families = [
        {"family": "community_breakouts", "count": len(community_breakouts)},
        {"family": "zhihu_hot", "count": len(zhihu_results)},
        {"family": "github_trending", "count": len(github_projects)},
        {"family": "prl_recent", "count": len(prl_papers)},
        {"family": "conference_recent", "count": len(conference_papers)},
        {"family": "arxiv_latest", "count": len(arxiv_preprints)},
        {"family": "classic_readings", "count": len(classic_readings)},
    ]
    state = {
        "generated_at": now_utc(),
        "source_families": source_families,
        "raw_candidates": raw_candidates,
        "selected_readings": selected_readings,
        "reading_notes": reading_notes,
        "bibliography": bibliography,
        "community_breakouts": community_breakouts,
        "zhihu_results": zhihu_results,
        "github_projects": github_projects,
        "prl_papers": prl_papers,
        "conference_papers": conference_papers,
        "arxiv_preprints": arxiv_preprints,
        "classic_readings": classic_readings,
        "paper_results": list(prl_papers) + list(conference_papers) + list(arxiv_preprints),
        "classic_texts": classic_readings,
        "research_interest_profile": read_json(RESEARCH_INTEREST_PROFILE_PATH, default=_bootstrap_interest_profile()),
    }
    write_json(EXTERNAL_INFORMATION_PATH, state)
    return state
