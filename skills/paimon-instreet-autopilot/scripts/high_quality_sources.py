#!/usr/bin/env python3
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib import parse, request

from common import CURRENT_STATE_DIR, now_utc, read_json, truncate_text, write_json


HIGH_QUALITY_SOURCES_PATH = CURRENT_STATE_DIR / "high_quality_sources.json"
RESEARCH_SOURCE_HINTS_PATH = CURRENT_STATE_DIR / "research_source_hints.json"
ARXIV_API_URL = "https://export.arxiv.org/api/query"
CROSSREF_API_URL = "https://api.crossref.org/works"
BREAKOUT_MIN_UPVOTES = 200
DEFAULT_QUERY_LIMIT = 6
DEFAULT_RESULTS_PER_QUERY = 3


def ensure_high_quality_source_files() -> None:
    if not RESEARCH_SOURCE_HINTS_PATH.exists():
        write_json(
            RESEARCH_SOURCE_HINTS_PATH,
            {
                "updated_at": now_utc(),
                "queries": [],
                "classic_texts": [],
            },
        )
    if not HIGH_QUALITY_SOURCES_PATH.exists():
        write_json(
            HIGH_QUALITY_SOURCES_PATH,
            {
                "generated_at": None,
                "research_queries": [],
                "community_breakouts": [],
                "paper_results": [],
                "classic_texts": [],
            },
        )


def _split_fragments(text: str) -> list[str]:
    return [fragment.strip() for fragment in re.split(r"[：:|丨，,。！？、（）()《》“”‘’/\\\s]+", str(text or "")) if fragment.strip()]


def _meaningful_fragments(text: str) -> list[str]:
    fragments: list[str] = []
    for fragment in _split_fragments(text):
        if len(fragment) < 2:
            continue
        if fragment.isdigit():
            continue
        fragments.append(fragment)
    return fragments


def _dedupe_strings(values: list[str], *, limit: int) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
        if len(ordered) >= limit:
            break
    return ordered


def _title_queries(title: str) -> list[str]:
    cleaned = str(title or "").strip()
    if not cleaned:
        return []
    fragments = _meaningful_fragments(cleaned)
    short_query = " ".join(fragments[:4]).strip()
    queries = [cleaned]
    if short_query and short_query != cleaned:
        queries.append(short_query)
    return queries


def _extract_community_breakouts(
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in list(community_hot_posts or []) + list(competitor_watchlist or []):
        upvotes = int(item.get("upvotes") or 0)
        if upvotes < BREAKOUT_MIN_UPVOTES:
            continue
        title = str(item.get("title") or "").strip()
        if not title or title in seen:
            continue
        seen.add(title)
        candidates.append(
            {
                "title": title,
                "author": str(item.get("author") or item.get("username") or "").strip(),
                "submolt": str(item.get("submolt") or item.get("submolt_name") or "").strip(),
                "upvotes": upvotes,
                "comment_count": int(item.get("comment_count") or 0),
                "created_at": item.get("created_at"),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (-int(item.get("upvotes") or 0), -int(item.get("comment_count") or 0)),
    )[:8]


def _load_hint_payload() -> dict[str, Any]:
    return read_json(RESEARCH_SOURCE_HINTS_PATH, default={"queries": [], "classic_texts": []})


def _build_research_queries(
    *,
    community_breakouts: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None,
    source_evolution: dict[str, Any] | None,
) -> list[str]:
    queries: list[str] = []
    hints_payload = _load_hint_payload()
    queries.extend(str(item or "").strip() for item in hints_payload.get("queries") or [])
    queries.extend(_title_queries(str(item.get("title") or "")) for item in community_breakouts[:4])
    flat_queries: list[str] = []
    for item in queries:
        if isinstance(item, list):
            flat_queries.extend(str(part or "").strip() for part in item)
        else:
            flat_queries.append(str(item or "").strip())
    if user_topic_hints:
        for item in user_topic_hints[:4]:
            flat_queries.extend(_title_queries(str(item.get("text") or "")))
    if isinstance(source_evolution, dict):
        flat_queries.extend(_title_queries(str(source_evolution.get("focus") or "")))
        for item in source_evolution.get("targets", [])[:3]:
            flat_queries.extend(_title_queries(str(item.get("reason") or "")))
    if not flat_queries:
        flat_queries.extend(["AI agents", "multi-agent systems", "political economy of AI"])
    return _dedupe_strings(flat_queries, limit=DEFAULT_QUERY_LIMIT)


def _fetch_arxiv_preprints(queries: list[str], *, limit_per_query: int = DEFAULT_RESULTS_PER_QUERY) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for query_text in queries:
        query = parse.urlencode(
            {
                "search_query": f'all:"{query_text}"',
                "sortBy": "lastUpdatedDate",
                "sortOrder": "descending",
                "start": 0,
                "max_results": limit_per_query,
            }
        )
        req = request.Request(
            f"{ARXIV_API_URL}?{query}",
            headers={"User-Agent": "paimon-insight/1.0"},
        )
        with request.urlopen(req, timeout=20) as response:
            payload = response.read()
        root = ET.fromstring(payload)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title = " ".join("".join(entry.findtext("atom:title", default="", namespaces=ns)).split())
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            summary = " ".join("".join(entry.findtext("atom:summary", default="", namespaces=ns)).split())
            link = ""
            for link_node in entry.findall("atom:link", ns):
                href = str(link_node.attrib.get("href") or "").strip()
                rel = str(link_node.attrib.get("rel") or "").strip()
                if href and rel in {"alternate", ""}:
                    link = href
                    break
            authors = [
                " ".join("".join(author.findtext("atom:name", default="", namespaces=ns)).split())
                for author in entry.findall("atom:author", ns)
            ]
            entries.append(
                {
                    "source": "arxiv",
                    "query": query_text,
                    "title": title,
                    "summary": truncate_text(summary, 280),
                    "authors": [item for item in authors if item],
                    "published_at": entry.findtext("atom:published", default="", namespaces=ns),
                    "updated_at": entry.findtext("atom:updated", default="", namespaces=ns),
                    "link": link,
                }
            )
    return entries


def _fetch_crossref_recent(queries: list[str], *, limit_per_query: int = DEFAULT_RESULTS_PER_QUERY) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for query_text in queries:
        params = parse.urlencode(
            {
                "query.title": query_text,
                "rows": limit_per_query,
                "sort": "published",
                "order": "desc",
                "select": "title,author,DOI,abstract,published-print,published-online,URL,container-title",
            }
        )
        req = request.Request(
            f"{CROSSREF_API_URL}?{params}",
            headers={"User-Agent": "paimon-insight/1.0 (mailto:none@example.com)"},
        )
        with request.urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        items = (read_json_from_text(payload).get("message") or {}).get("items") or []
        for item in items:
            title_list = item.get("title") or []
            title = str(title_list[0] or "").strip() if title_list else ""
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            authors = []
            for author in item.get("author") or []:
                given = str(author.get("given") or "").strip()
                family = str(author.get("family") or "").strip()
                full = " ".join(part for part in (given, family) if part)
                if full:
                    authors.append(full)
            abstract = re.sub(r"<[^>]+>", " ", str(item.get("abstract") or "")).strip()
            entries.append(
                {
                    "source": "crossref",
                    "query": query_text,
                    "title": title,
                    "summary": truncate_text(" ".join(abstract.split()), 280),
                    "authors": authors[:6],
                    "published_at": _crossref_published_at(item),
                    "updated_at": _crossref_published_at(item),
                    "link": str(item.get("URL") or "").strip(),
                }
            )
    return entries


def _crossref_published_at(item: dict[str, Any]) -> str:
    for key in ("published-print", "published-online"):
        date_parts = ((item.get(key) or {}).get("date-parts") or [[]])[0]
        if date_parts:
            return "-".join(f"{int(part):02d}" for part in date_parts[:3])
    return ""


def read_json_from_text(payload: str) -> dict[str, Any]:
    import json

    return json.loads(payload)


def _load_classic_texts() -> list[dict[str, Any]]:
    hints_payload = _load_hint_payload()
    classics = hints_payload.get("classic_texts") or hints_payload.get("items") or []
    normalized: list[dict[str, Any]] = []
    for item in classics:
        if isinstance(item, str):
            title = item.strip()
            if title:
                normalized.append({"title": title, "lens": ""})
            continue
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        normalized.append(
            {
                "title": title,
                "lens": str(item.get("lens") or item.get("note") or "").strip(),
            }
        )
    return normalized[:10]


def _dedupe_papers(entries: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in sorted(
        entries,
        key=lambda current: (
            str(current.get("published_at") or current.get("updated_at") or ""),
            str(current.get("source") or ""),
        ),
        reverse=True,
    ):
        title = str(item.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def refresh_high_quality_sources(
    *,
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None = None,
    source_evolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_high_quality_source_files()
    community_breakouts = _extract_community_breakouts(community_hot_posts, competitor_watchlist)
    research_queries = _build_research_queries(
        community_breakouts=community_breakouts,
        user_topic_hints=user_topic_hints,
        source_evolution=source_evolution,
    )
    cached = read_json(HIGH_QUALITY_SOURCES_PATH, default={})
    try:
        arxiv_preprints = _fetch_arxiv_preprints(research_queries)
    except Exception:
        arxiv_preprints = cached.get("paper_results") or []
    try:
        crossref_results = _fetch_crossref_recent(research_queries)
    except Exception:
        crossref_results = []
    state = {
        "generated_at": now_utc(),
        "research_queries": research_queries,
        "community_breakouts": community_breakouts,
        "paper_results": _dedupe_papers(list(arxiv_preprints) + list(crossref_results)),
        "classic_texts": _load_classic_texts(),
    }
    write_json(HIGH_QUALITY_SOURCES_PATH, state)
    return state
