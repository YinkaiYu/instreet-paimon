#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from pathlib import Path
from typing import Any

from common import DRAFTS_DIR, now_slug, run_codex, truncate_text, write_json, write_text


STYLE_SESSION_DIR = DRAFTS_DIR / "style_sessions"
DEFAULT_SAMPLE_CHARS = 20000
SELECTED_EXCERPT_TARGET_CHARS = 1000


def _clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _dedupe_lines(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        line = _clean_line(item)
        if not line or line in seen:
            continue
        seen.add(line)
        result.append(line)
    return result


def _split_blocks(text: str) -> list[str]:
    blocks = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    return blocks or [text.strip()]


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.S)
    if fenced:
        text = fenced.group(1).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower()
    return slug or "style-sample"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _snap_start(text: str, index: int, *, window: int = 600) -> int:
    if not text:
        return 0
    start = max(0, index - window)
    end = min(len(text), index + window)
    marker = text.rfind("\n\n", start, end)
    if marker != -1:
        return marker + 2
    marker = text.rfind("\n", start, end)
    if marker != -1:
        return marker + 1
    return max(0, index)


def sample_contiguous_text(
    source_path: str | Path,
    *,
    sample_chars: int = DEFAULT_SAMPLE_CHARS,
    seed: int | None = None,
) -> dict[str, Any]:
    source = Path(source_path)
    text = _read_text(source)
    sample_chars = max(2000, int(sample_chars))
    if len(text) <= sample_chars:
        start = 0
        end = len(text)
    else:
        rng = random.Random(seed)
        raw_start = rng.randint(0, len(text) - sample_chars)
        start = _snap_start(text, raw_start)
        end = min(len(text), start + sample_chars)
    sample_text = text[start:end]
    return {
        "source_path": str(source),
        "source_size": len(text),
        "sample_chars": len(sample_text),
        "requested_sample_chars": sample_chars,
        "start_offset": start,
        "end_offset": end,
        "seed": seed,
        "sample_text": sample_text,
        "sample_digest": hashlib.sha1(sample_text.encode("utf-8")).hexdigest(),
    }


def _heuristic_style_summary(sample_text: str) -> str:
    paragraphs = [part.strip() for part in sample_text.splitlines() if part.strip()]
    avg_len = 0
    if paragraphs:
        avg_len = round(sum(len(item) for item in paragraphs) / len(paragraphs))
    dialog_count = sample_text.count("“") + sample_text.count("\"")
    ellipsis_count = sample_text.count("……")
    summary = [
        "- 句子整体偏长，叙述会在动作、感受和环境之间连续滑行，不要写成短促流水账。",
        "- 段落推进依赖镜头感和情绪蓄压，人物出场时先给现场气氛，再给对白或判断。",
        "- 允许适度夸张和华丽修辞，但修辞必须服务节奏，不要变成空洞辞藻堆砌。",
        "- 对白要有角色性格，不写模板化互怼；甜感来自熟悉、默契和细小偏爱，而不是廉价误会。",
        f"- 当前样本平均段落长度约 {avg_len} 字，对话标记约 {dialog_count} 处，说明文本更适合大段流动叙述中穿插对白。",
        f"- 省略号“……”约出现 {ellipsis_count} 次，保留情绪停顿，但不要滥用感叹号和网络口癖。",
        "- 只模仿语言呼吸、句法密度和意象组织，不得继承原文本的人物、世界观或情节。",
    ]
    return "\n".join(summary)


def _heuristic_style_profile(sample_text: str) -> dict[str, Any]:
    paragraphs = [part.strip() for part in sample_text.splitlines() if part.strip()]
    avg_len = 0
    if paragraphs:
        avg_len = round(sum(len(item) for item in paragraphs) / len(paragraphs))
    return {
        "syntax_patterns": [
            "以中长句为主，常把动作、判断、心理和环境压进同一口气里往前推。",
            f"段落平均长度约 {avg_len} 字，更适合流动叙述中插对白，不适合一味切成碎短句。",
            "长句负责蓄压，短句负责收锤，句内需要有明显落点。",
        ],
        "language_habits": [
            "判断尽量直接，不绕弯，不先否定一串再给答案。",
            "抽象概念要落到动作、场面、风险和后果上。",
            "口语感要自然，避免模板化鸡汤和网络段子腔。",
        ],
        "common_phrasings": [
            "先写看见了什么、做了什么，再写人物的判断。",
            "甜感优先落在熟悉、偏心、照料、贴近和事后余温。",
            "紧张场面里也保留人味，不要把人物写成只会讲概念。",
        ],
        "dialogue_habits": [
            "对白要短、准、带角色身份，不写所有人都共用一个作者腔。",
            "对白先推动局面，再顺手带出关系和情绪。",
        ],
        "rhythm_model": [
            "开场尽快进入现场和动作，不先铺大段说明。",
            "中段用动作和对话抬高节奏，结尾给一句明确钩子或异动。",
        ],
        "imagery_rules": [
            "意象只用来加重眼前画面，不为了显得高级而硬拔高。",
            "少量核心物象反复变奏，比一段里连换好几个比喻更有效。",
        ],
        "emotion_delivery": [
            "情绪先通过动作和停顿显形，再让人物补一句判断。",
            "甜和疼都要具体，不写空泛价值结论。",
        ],
        "forbidden_patterns": [
            "不要把判断写成“不是X，而是Y”“不是……是……”这种正名句式。",
            "不要连续用“不要……不要……不要……”这种排比口号起手。",
            "不要直接写“接住、托住、很稳、稳”这类悬浮托举词。",
            "不要用“被看见、被命名、被默认、被承认”做抽象收束。",
            "不要用突兀术语或莫名其妙的比喻替代现场描写。",
        ],
        "preferred_repairs": [
            "需要判断时直接下判断，不先列一串错误答案。",
            "需要甜感时直接写动作、距离、照料和余温。",
            "需要升级世界观时先给异常和后果，再让术语上桌。",
            "需要拔高时优先加重现场压力，不靠抽象大词。",
        ],
    }


def _normalize_profile(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = payload or {}
    profile = _heuristic_style_profile("")
    for key in profile:
        value = raw.get(key)
        if isinstance(value, list):
            profile[key] = _dedupe_lines([str(item) for item in value])
    if not any(profile["syntax_patterns"]):
        return _heuristic_style_profile("")
    return profile


def summarize_style_profile(
    sample_text: str,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    prompt = f"""
请阅读下面这段中文长文本样本，只总结语言风格，不要复述设定与剧情。

请返回严格 JSON 对象，不要输出解释，不要加 Markdown 代码块，字段固定如下：
{{
  "syntax_patterns": ["..."],
  "language_habits": ["..."],
  "common_phrasings": ["..."],
  "dialogue_habits": ["..."],
  "rhythm_model": ["..."],
  "imagery_rules": ["..."],
  "emotion_delivery": ["..."],
  "forbidden_patterns": ["..."],
  "preferred_repairs": ["..."]
}}

要求：
1. 每个字段输出 3 到 6 条中文短句。
2. 必须重点总结“语言习惯”“常用表述倾向”“禁用句式”“替代表达策略”。
3. forbidden_patterns 里必须覆盖这几类问题：先否定再肯定、抽象正名句、三连否定、悬浮托举词、抽象价值收束、突兀术语、无意义比喻。
4. common_phrasings 总结的是“这类文本常怎么组织表达”，不是抄原句。
5. 不要出现样本里的专有名词，不要总结剧情。

样本文本：
{truncate_text(sample_text, 14000)}
""".strip()
    try:
        raw = run_codex(
            prompt,
            timeout=timeout_seconds,
            model=model,
            reasoning_effort=reasoning_effort,
        ).strip()
        payload = _extract_json_object(raw)
        if payload:
            return _normalize_profile(payload)
    except Exception:
        pass
    return _heuristic_style_profile(sample_text)


def _render_markdown_section(title: str, items: list[str]) -> str:
    lines = [f"## {title}"]
    for item in _dedupe_lines(items):
        lines.append(f"- {item}")
    if len(lines) == 1:
        lines.append("- 无")
    return "\n".join(lines)


def render_style_summary(profile: dict[str, Any]) -> str:
    sections = [
        _render_markdown_section("风格摘要", profile.get("syntax_patterns") or []),
        _render_markdown_section("语言习惯", profile.get("language_habits") or []),
        _render_markdown_section("常用表述", profile.get("common_phrasings") or []),
        _render_markdown_section("对白组织", profile.get("dialogue_habits") or []),
        _render_markdown_section("节奏模型", profile.get("rhythm_model") or []),
        _render_markdown_section("意象边界", profile.get("imagery_rules") or []),
        _render_markdown_section("情绪落地", profile.get("emotion_delivery") or []),
        _render_markdown_section("禁用句式", profile.get("forbidden_patterns") or []),
        _render_markdown_section("替代表达", profile.get("preferred_repairs") or []),
    ]
    return "\n\n".join(sections)


def render_anti_patterns(profile: dict[str, Any]) -> str:
    sections = [
        "# 禁用句式与风险提示",
        "",
        "## 禁用句式",
    ]
    for item in _dedupe_lines(profile.get("forbidden_patterns") or []):
        sections.append(f"- {item}")
    sections.extend(["", "## 替代表达", ""])
    for item in _dedupe_lines(profile.get("preferred_repairs") or []):
        sections.append(f"- {item}")
    return "\n".join(sections).strip()


def _heuristic_selected_excerpt(sample_text: str, target_chars: int = SELECTED_EXCERPT_TARGET_CHARS) -> str:
    blocks = _split_blocks(sample_text)
    best_text = sample_text[:target_chars]
    best_score = -1
    for start in range(len(blocks)):
        candidate_parts: list[str] = []
        total = 0
        for end in range(start, len(blocks)):
            block = blocks[end]
            extra = len(block) + (2 if candidate_parts else 0)
            if candidate_parts and total + extra > 1300:
                break
            candidate_parts.append(block)
            total += extra
            if total < 700:
                continue
            candidate = "\n\n".join(candidate_parts)
            sentence_count = len(re.findall(r"[。！？；]", candidate))
            dialog_count = candidate.count("“") + candidate.count("\"")
            imagery_count = len(re.findall(r"[雨风火光夜影声手眼心唇肩腰]", candidate))
            score = sentence_count * 3 + dialog_count * 4 + imagery_count
            if 850 <= len(candidate) <= 1150:
                score += 40
            if score > best_score:
                best_score = score
                best_text = candidate
    return best_text.strip()


def select_representative_excerpt(
    sample_text: str,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = 180,
) -> str:
    prompt = f"""
请从下面这段中文样本文本中，原样挑出一段最能代表语言文气、句法呼吸和对白落点的连续原文。

要求：
1. 只输出原文，不要解释，不要加引号，不要改写，不要补字。
2. 长度控制在 800 到 1200 个汉字。
3. 必须是样本文本中真实存在的一段连续原文。
4. 优先选择语言最漂亮、最能代表文本手感的一段，而不是信息量最大的一段。

样本文本：
{truncate_text(sample_text, 14000)}
""".strip()
    try:
        candidate = run_codex(
            prompt,
            timeout=timeout_seconds,
            model=model,
            reasoning_effort=reasoning_effort,
        ).strip()
        cleaned = candidate.strip()
        if 700 <= len(cleaned) <= 1300 and cleaned in sample_text:
            return cleaned
    except Exception:
        pass
    return _heuristic_selected_excerpt(sample_text)


def summarize_style(
    sample_text: str,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = 180,
) -> str:
    prompt = f"""
请阅读下面这段中文长文本样本，并只总结“语言风格”，不要复述内容设定。

要求：
1. 只输出 8 到 12 条 Markdown 列表项。
2. 关注句法、叙述距离、节奏、对白组织、意象密度、修辞习惯、情绪推进方式。
3. 明确指出“该模仿什么”和“不要模仿什么”。
4. 不要引用样本中的专有名词，不要总结剧情。

样本文本：
{truncate_text(sample_text, 14000)}
""".strip()
    try:
        return run_codex(prompt, timeout=timeout_seconds, model=model, reasoning_effort=reasoning_effort).strip()
    except Exception:
        return _heuristic_style_summary(sample_text)


def prepare_style_packet(
    source_path: str | Path,
    *,
    label: str,
    sample_chars: int = DEFAULT_SAMPLE_CHARS,
    seed: int | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    STYLE_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    sample = sample_contiguous_text(source_path, sample_chars=sample_chars, seed=seed)
    style_profile = summarize_style_profile(
        sample["sample_text"],
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    style_summary = render_style_summary(style_profile)
    anti_patterns = render_anti_patterns(style_profile)
    selected_excerpt = select_representative_excerpt(
        sample["sample_text"],
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=min(timeout_seconds, 180),
    )
    session_slug = f"{now_slug()}-{_slugify(label)}"
    session_dir = STYLE_SESSION_DIR / session_slug
    session_dir.mkdir(parents=True, exist_ok=True)

    excerpt_path = session_dir / "excerpt.txt"
    summary_path = session_dir / "style-summary.md"
    profile_path = session_dir / "style-profile.json"
    selected_excerpt_path = session_dir / "selected-sample.md"
    anti_patterns_path = session_dir / "anti-patterns.md"
    meta_path = session_dir / "session.json"
    write_text(excerpt_path, sample["sample_text"])
    write_text(summary_path, style_summary + "\n")
    write_json(profile_path, style_profile)
    write_text(selected_excerpt_path, selected_excerpt + "\n")
    write_text(anti_patterns_path, anti_patterns + "\n")
    write_json(
        meta_path,
        {
            "generated_at": now_slug(),
            "label": label,
            **{key: value for key, value in sample.items() if key != "sample_text"},
            "excerpt_path": str(excerpt_path),
            "summary_path": str(summary_path),
            "style_profile_path": str(profile_path),
            "selected_excerpt_path": str(selected_excerpt_path),
            "anti_patterns_path": str(anti_patterns_path),
        },
    )
    return {
        **sample,
        "style_summary": style_summary,
        "style_profile": style_profile,
        "selected_excerpt": selected_excerpt,
        "anti_patterns": anti_patterns,
        "excerpt_path": str(excerpt_path),
        "summary_path": str(summary_path),
        "style_profile_path": str(profile_path),
        "selected_excerpt_path": str(selected_excerpt_path),
        "anti_patterns_path": str(anti_patterns_path),
        "meta_path": str(meta_path),
        "session_dir": str(session_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample a contiguous style excerpt and summarize its language patterns.")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--label", default="style-sample")
    parser.add_argument("--sample-chars", type=int, default=DEFAULT_SAMPLE_CHARS)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--model")
    parser.add_argument("--reasoning-effort")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()

    packet = prepare_style_packet(
        args.source_path,
        label=args.label,
        sample_chars=args.sample_chars,
        seed=args.seed,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps({key: value for key, value in packet.items() if key != "sample_text"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
