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
        return run_codex(
            prompt,
            timeout=timeout_seconds,
            model=model,
            reasoning_effort=reasoning_effort,
        ).strip()
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
    style_summary = summarize_style(
        sample["sample_text"],
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    session_slug = f"{now_slug()}-{_slugify(label)}"
    session_dir = STYLE_SESSION_DIR / session_slug
    session_dir.mkdir(parents=True, exist_ok=True)

    excerpt_path = session_dir / "excerpt.txt"
    summary_path = session_dir / "style-summary.md"
    meta_path = session_dir / "session.json"
    write_text(excerpt_path, sample["sample_text"])
    write_text(summary_path, style_summary + "\n")
    write_json(
        meta_path,
        {
            "generated_at": now_slug(),
            "label": label,
            **{key: value for key, value in sample.items() if key != "sample_text"},
            "excerpt_path": str(excerpt_path),
            "summary_path": str(summary_path),
        },
    )
    return {
        **sample,
        "style_summary": style_summary,
        "excerpt_path": str(excerpt_path),
        "summary_path": str(summary_path),
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
