#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib import parse, request

from common import CURRENT_STATE_DIR, now_utc, read_json, run_codex_json, truncate_text, write_json


HIGH_QUALITY_SOURCES_PATH = CURRENT_STATE_DIR / "high_quality_sources.json"
RESEARCH_SOURCE_HINTS_PATH = CURRENT_STATE_DIR / "research_source_hints.json"
ARXIV_API_URL = "https://export.arxiv.org/api/query"
CROSSREF_API_URL = "https://api.crossref.org/works"
ZHIHU_HOT_URL = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total?limit=8&desktop=true"
BREAKOUT_MIN_UPVOTES = 200
DEFAULT_QUERY_LIMIT = 6
DEFAULT_RESULTS_PER_QUERY = 3
DEFAULT_QUERY_BLUEPRINT_TIMEOUT = 90
MIN_PAPER_RELEVANCE_SCORE = 3
MAX_FUTURE_PUBLICATION_DAYS = 180


def ensure_high_quality_source_files() -> None:
    if not RESEARCH_SOURCE_HINTS_PATH.exists():
        write_json(
            RESEARCH_SOURCE_HINTS_PATH,
            {
                "updated_at": now_utc(),
                "queries": [],
                "classic_texts": [],
                "zhihu_headers": {},
            },
        )
    if not HIGH_QUALITY_SOURCES_PATH.exists():
        write_json(
            HIGH_QUALITY_SOURCES_PATH,
            {
                "generated_at": None,
                "research_queries": [],
                "research_query_specs": [],
                "community_breakouts": [],
                "zhihu_results": [],
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


def _fetch_zhihu_hot_best_effort(limit: int = 5) -> list[dict[str, Any]]:
    hints_payload = _load_hint_payload()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.zhihu.com/",
    }
    raw_headers = hints_payload.get("zhihu_headers") or {}
    if isinstance(raw_headers, dict):
        for key, value in raw_headers.items():
            if not key or value is None:
                continue
            headers[str(key)] = str(value)
    req = request.Request(ZHIHU_HOT_URL, headers=headers)
    try:
        with request.urlopen(req, timeout=20) as response:
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
                "title": title,
                "summary": truncate_text(excerpt, 180),
                "url": str(target.get("url") or "").strip(),
                "follower_count": int(target.get("follower_count") or 0),
            }
        )
    return results


def _research_query_blueprint_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "query_specs": {
                "type": "array",
                "minItems": 1,
                "maxItems": DEFAULT_QUERY_LIMIT,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query": {"type": "string"},
                        "intent": {"type": "string"},
                        "track": {"type": "string"},
                        "bridge_terms": {
                            "type": "array",
                            "maxItems": 4,
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["query", "intent", "track", "bridge_terms"],
                },
            },
            "classic_texts": {
                "type": "array",
                "maxItems": 4,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "title": {"type": "string"},
                        "lens": {"type": "string"},
                    },
                    "required": ["title", "lens"],
                },
            },
        },
        "required": ["query_specs", "classic_texts"],
    }


def _fallback_query_blueprint(
    *,
    community_breakouts: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    candidates = [str(item.get("title") or "").strip() for item in community_breakouts[:4]]
    if user_topic_hints:
        candidates.extend(str(item.get("text") or "").strip() for item in user_topic_hints[:3])
    query_specs: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        fragments = _meaningful_fragments(candidate)
        query = " ".join(fragments[:4]).strip() or candidate
        if query in seen_queries:
            continue
        seen_queries.add(query)
        query_specs.append(
            {
                "query": query,
                "intent": candidate,
                "track": "theory",
                "bridge_terms": fragments[:4],
            }
        )
        if len(query_specs) >= DEFAULT_QUERY_LIMIT:
            break
    if not query_specs:
        query_specs = [
            {
                "query": "AI agents memory governance",
                "intent": "从 AI agents、memory、governance 这些更通用的问题意识切入。",
                "track": "theory",
                "bridge_terms": ["AI", "agents", "memory", "governance"],
            }
        ]
    return {"query_specs": query_specs, "classic_texts": []}


def _build_query_blueprint(
    *,
    community_breakouts: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None,
    source_evolution: dict[str, Any] | None,
) -> dict[str, Any]:
    hints_payload = _load_hint_payload()
    manual_queries = [str(item or "").strip() for item in hints_payload.get("queries") or [] if str(item or "").strip()]
    manual_classics = hints_payload.get("classic_texts") or []
    prompt = f"""
你在给派蒙生成一轮“外部研究查询蓝图”。目标不是复述社区标题，而是把现场样本抽象成更适合抓论文/预印本/经典文本的问题意识。

要求：
1. 输出 4-6 个查询对象，每个对象包含 `query`、`intent`、`track`、`bridge_terms`。
2. `query` 应该是适合学术检索的关键词串，不要直接复制整句爆帖标题。
3. `intent` 说明这个查询真正要追的机制、概念或矛盾。
4. `bridge_terms` 只保留最关键的 2-4 个词，用于后续过滤检索结果。
5. 可以中英混合，但要服务于当前 Agent / AI 社会议程。
6. 额外给出 0-4 条适合这轮问题意识的经典著作线索，写成 `title` + `lens`。
7. 不要把查询锁死在固定主题；要从这轮现场样本里长出来。

社区爆帖样本：
{truncate_text(json.dumps(community_breakouts, ensure_ascii=False), 2200)}

用户参考题：
{truncate_text(json.dumps(user_topic_hints or [], ensure_ascii=False), 1200)}

当前进化焦点：
{truncate_text(json.dumps(source_evolution or {}, ensure_ascii=False), 1200)}

人工补充查询：
{truncate_text(json.dumps(manual_queries, ensure_ascii=False), 800)}

人工补充经典：
{truncate_text(json.dumps(manual_classics, ensure_ascii=False), 800)}
""".strip()
    try:
        blueprint = run_codex_json(
            prompt,
            _research_query_blueprint_schema(),
            timeout=DEFAULT_QUERY_BLUEPRINT_TIMEOUT,
            full_auto=True,
        )
    except Exception:
        blueprint = _fallback_query_blueprint(
            community_breakouts=community_breakouts,
            user_topic_hints=user_topic_hints,
        )
    return blueprint


def _normalize_query_specs(raw_specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    for item in raw_specs:
        if not isinstance(item, dict):
            continue
        query = str(item.get("query") or "").strip()
        intent = str(item.get("intent") or "").strip()
        track = str(item.get("track") or "").strip() or "theory"
        bridge_terms = _dedupe_strings(
            [str(term or "").strip() for term in list(item.get("bridge_terms") or [])],
            limit=4,
        )
        if not query or query in seen_queries:
            continue
        seen_queries.add(query)
        specs.append(
            {
                "query": query,
                "intent": intent or query,
                "track": track,
                "bridge_terms": bridge_terms,
            }
        )
        if len(specs) >= DEFAULT_QUERY_LIMIT:
            break
    return specs


def _normalize_classic_texts(items: list[dict[str, Any]] | list[str]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in items:
        if isinstance(item, str):
            title = item.strip()
            lens = ""
        elif isinstance(item, dict):
            title = str(item.get("title") or "").strip()
            lens = str(item.get("lens") or item.get("note") or "").strip()
        else:
            continue
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        normalized.append({"title": title, "lens": lens})
        if len(normalized) >= 10:
            break
    return normalized


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


def _crossref_published_at(item: dict[str, Any]) -> str:
    for key in ("published-print", "published-online"):
        date_parts = ((item.get(key) or {}).get("date-parts") or [[]])[0]
        if date_parts:
            return "-".join(f"{int(part):02d}" for part in date_parts[:3])
    return ""


def _fetch_arxiv_preprints(query_specs: list[dict[str, Any]], *, limit_per_query: int = DEFAULT_RESULTS_PER_QUERY) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for spec in query_specs:
        query_text = str(spec.get("query") or "").strip()
        if not query_text:
            continue
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
                    "intent": str(spec.get("intent") or "").strip(),
                    "track": str(spec.get("track") or "").strip(),
                    "bridge_terms": list(spec.get("bridge_terms") or []),
                    "title": title,
                    "summary": truncate_text(summary, 280),
                    "authors": [item for item in authors if item],
                    "published_at": entry.findtext("atom:published", default="", namespaces=ns),
                    "updated_at": entry.findtext("atom:updated", default="", namespaces=ns),
                    "link": link,
                }
            )
    return entries


def _fetch_crossref_recent(query_specs: list[dict[str, Any]], *, limit_per_query: int = DEFAULT_RESULTS_PER_QUERY) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for spec in query_specs:
        query_text = str(spec.get("query") or "").strip()
        if not query_text:
            continue
        params = parse.urlencode(
            {
                "query.bibliographic": query_text,
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
        items = (json.loads(payload).get("message") or {}).get("items") or []
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
                    "intent": str(spec.get("intent") or "").strip(),
                    "track": str(spec.get("track") or "").strip(),
                    "bridge_terms": list(spec.get("bridge_terms") or []),
                    "title": title,
                    "summary": truncate_text(" ".join(abstract.split()), 280),
                    "authors": authors[:6],
                    "published_at": _crossref_published_at(item),
                    "updated_at": _crossref_published_at(item),
                    "link": str(item.get("URL") or "").strip(),
                }
            )
    return entries


def _reference_terms(
    *,
    query_specs: list[dict[str, Any]],
    community_breakouts: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None,
) -> list[str]:
    terms: list[str] = []
    for spec in query_specs:
        terms.extend(_meaningful_fragments(str(spec.get("intent") or "")))
        terms.extend(_meaningful_fragments(str(spec.get("query") or "")))
        terms.extend(str(term or "").strip() for term in list(spec.get("bridge_terms") or []))
    for item in community_breakouts[:4]:
        terms.extend(_meaningful_fragments(str(item.get("title") or "")))
    for item in user_topic_hints or []:
        terms.extend(_meaningful_fragments(str(item.get("text") or "")))
    return _dedupe_strings(terms, limit=40)


def _paper_relevance_score(item: dict[str, Any], *, reference_terms: list[str]) -> tuple[int, list[str]]:
    title = str(item.get("title") or "").strip()
    summary = str(item.get("summary") or "").strip()
    core_text = f"{title} {summary}"
    reasons: list[str] = []
    score = 0
    matched_terms = [term for term in reference_terms if term and term in core_text]
    if matched_terms:
        title_hits = [term for term in matched_terms if term in title]
        score += len(title_hits) * 3 + (len(matched_terms) - len(title_hits))
        reasons.append("命中了桥接词：" + "、".join(matched_terms[:4]))
    bridge_terms = [str(term or "").strip() for term in list(item.get("bridge_terms") or []) if str(term or "").strip()]
    bridge_hits = [term for term in bridge_terms if term in core_text]
    if bridge_hits:
        score += len(bridge_hits) * 2
        reasons.append("命中了查询桥：" + "、".join(bridge_hits[:3]))
    if str(item.get("source") or "") == "arxiv":
        score += 1
    if len(summary) >= 80:
        score += 1
    published_at = _parse_datetime(item.get("published_at")) or _parse_datetime(item.get("updated_at"))
    if published_at and published_at > datetime.now(timezone.utc) + timedelta(days=MAX_FUTURE_PUBLICATION_DAYS):
        score -= 6
        reasons.append("发布时间过于超前")
    return score, reasons


def _filter_paper_results(
    entries: list[dict[str, Any]],
    *,
    reference_terms: list[str],
    limit: int = 12,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in entries:
        title = str(item.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        relevance_score, relevance_reasons = _paper_relevance_score(item, reference_terms=reference_terms)
        if relevance_score < MIN_PAPER_RELEVANCE_SCORE:
            continue
        ranked.append(
            {
                **item,
                "relevance_score": relevance_score,
                "relevance_note": "；".join(relevance_reasons[:2]) or str(item.get("intent") or "").strip(),
            }
        )
    ranked.sort(
        key=lambda item: (
            -int(item.get("relevance_score") or 0),
            str(item.get("published_at") or item.get("updated_at") or ""),
            str(item.get("source") or ""),
        ),
        reverse=False,
    )
    ranked.sort(
        key=lambda item: (
            -int(item.get("relevance_score") or 0),
            str(item.get("published_at") or item.get("updated_at") or ""),
        ),
        reverse=True,
    )
    return ranked[:limit]


def refresh_high_quality_sources(
    *,
    community_hot_posts: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    user_topic_hints: list[dict[str, Any]] | None = None,
    source_evolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_high_quality_source_files()
    community_breakouts = _extract_community_breakouts(community_hot_posts, competitor_watchlist)
    query_blueprint = _build_query_blueprint(
        community_breakouts=community_breakouts,
        user_topic_hints=user_topic_hints,
        source_evolution=source_evolution,
    )
    query_specs = _normalize_query_specs(list(query_blueprint.get("query_specs") or []))
    if not query_specs:
        query_blueprint = _fallback_query_blueprint(
            community_breakouts=community_breakouts,
            user_topic_hints=user_topic_hints,
        )
        query_specs = _normalize_query_specs(list(query_blueprint.get("query_specs") or []))
    reference_terms = _reference_terms(
        query_specs=query_specs,
        community_breakouts=community_breakouts,
        user_topic_hints=user_topic_hints,
    )
    cached = read_json(HIGH_QUALITY_SOURCES_PATH, default={})
    try:
        arxiv_preprints = _fetch_arxiv_preprints(query_specs)
    except Exception:
        arxiv_preprints = cached.get("paper_results") or []
    try:
        crossref_results = _fetch_crossref_recent(query_specs)
    except Exception:
        crossref_results = []
    classic_payload = list(_load_hint_payload().get("classic_texts") or []) + list(query_blueprint.get("classic_texts") or [])
    state = {
        "generated_at": now_utc(),
        "research_queries": [str(item.get("query") or "").strip() for item in query_specs],
        "research_query_specs": query_specs,
        "community_breakouts": community_breakouts,
        "zhihu_results": _fetch_zhihu_hot_best_effort(),
        "paper_results": _filter_paper_results(
            list(arxiv_preprints) + list(crossref_results),
            reference_terms=reference_terms,
        ),
        "classic_texts": _normalize_classic_texts(classic_payload),
    }
    write_json(HIGH_QUALITY_SOURCES_PATH, state)
    return state
