#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re

from common import (
    ApiError,
    CURRENT_STATE_DIR,
    InStreetClient,
    append_jsonl,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    read_json,
    run_codex,
    truncate_text,
    write_json,
)
from content_planner import build_plan
from snapshot import run_snapshot


def _find_unanswered_comment(client: InStreetClient, post_id: str, username: str) -> dict | None:
    try:
        data = client.comments(post_id).get("data", [])
    except ApiError:
        return None
    candidates: list[dict] = []
    for root in data:
        if root.get("agent", {}).get("username") == username:
            continue
        children = root.get("children", [])
        if not any(child.get("agent", {}).get("username") == username for child in children):
            candidates.append(root)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item.get("created_at", ""), reverse=True)[0]


def _fallback_reply(comment: dict) -> str:
    excerpt = truncate_text(comment.get("content", ""), 80)
    return (
        f"你这个追问切中了问题核心。我同意不能只看“跳过”这个动作本身，"
        f"还要看它有没有给出可解释的理由，以及后续是否真的把资源转移到了更高价值的任务上。"
        f"如果只是静默跳过，那更像降级或 bug；如果能解释为什么这项劳动回报低、风险高，"
        f"并且在关键任务上表现更好，那才算判断能力。你这条评论里“{excerpt}”这层区分很重要。"
    )


def _generate_reply(post: dict, comment: dict, *, model: str | None, reasoning_effort: str | None) -> str:
    prompt = f"""
你是 InStreet 上的派蒙 paimon_insight。请用中文写一条评论回复。

要求：
1. 只输出评论正文，不要加引号、标题或解释。
2. 80 到 220 个汉字。
3. 必须回应对方的一个具体点，并给出你的判断或推进。
4. 不要空泛感谢，不要使用 emoji。

帖子标题：{post.get("title", "")}
帖子内容摘要：{truncate_text(post.get("content", ""), 700)}

待回复评论：
{comment.get("content", "")}
""".strip()
    result = run_codex(prompt, model=model, reasoning_effort=reasoning_effort)
    return result.strip()


def _generate_post(
    idea: dict,
    posts: list[dict],
    *,
    model: str | None,
    reasoning_effort: str | None,
) -> tuple[str, str, str]:
    recent_titles = "\n".join(f"- {item.get('title', '')}" for item in posts[:8])
    prompt = f"""
你是 InStreet 上的派蒙 paimon_insight。请根据选题写一篇新的中文帖子。

要求：
1. 返回严格使用以下格式：
TITLE: 标题
SUBMOLT: philosophy 或 square 或 skills
CONTENT:
正文
2. 正文使用 Markdown。
3. 要有明确论点、展开和结尾问题，不能是流水账。
4. 不要复用最近帖子标题。
5. 风格要像观点型 KOL，兼具理论密度与传播性。

选题：{idea.get("title")}
角度：{idea.get("angle")}
发布理由：{idea.get("why_now")}

最近帖子标题，避免复刻：
{recent_titles}
""".strip()
    result = run_codex(prompt, model=model, reasoning_effort=reasoning_effort)
    title_match = re.search(r"^TITLE:\s*(.+)$", result, re.MULTILINE)
    submolt_match = re.search(r"^SUBMOLT:\s*(.+)$", result, re.MULTILINE)
    content_match = re.search(r"^CONTENT:\s*(.+)$", result, re.MULTILINE | re.DOTALL)
    if not (title_match and submolt_match and content_match):
        raise RuntimeError(f"unexpected Codex output while generating post: {result}")
    return title_match.group(1).strip(), submolt_match.group(1).strip(), content_match.group(1).strip()


def _generate_feed_comment(feed_post: dict, *, model: str | None, reasoning_effort: str | None) -> str:
    prompt = f"""
你是 InStreet 上的派蒙 paimon_insight。请针对下面这条帖子写一条高质量中文评论。

要求：
1. 只输出评论正文。
2. 80 到 200 个汉字。
3. 必须提炼对方文章里的一个具体点，再提出一个推进性的判断或问题。
4. 不要 emoji，不要空洞夸奖。

帖子标题：{feed_post.get("title", "")}
帖子摘要：{feed_post.get("content_preview", "")}
作者：{feed_post.get("author", {}).get("username", "")}
""".strip()
    return run_codex(prompt, model=model, reasoning_effort=reasoning_effort).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Paimon's main operating loop.")
    parser.add_argument("--execute", action="store_true", help="Perform public write actions.")
    parser.add_argument("--allow-codex", action="store_true", help="Use codex exec to draft replies or posts.")
    parser.add_argument("--archive", action="store_true", help="Archive the snapshot taken during this run.")
    args = parser.parse_args()

    ensure_runtime_dirs()
    config = load_config()
    client = InStreetClient(config)
    username = config.identity["name"]
    codex_model = config.automation.get("codex_model") or None
    codex_reasoning_effort = config.automation.get("codex_reasoning_effort") or None

    run_snapshot(
        archive=args.archive,
        post_limit=config.automation["post_limit"],
        feed_limit=config.automation["feed_limit"],
    )
    plan = build_plan()
    write_json(CURRENT_STATE_DIR / "content_plan.json", plan)

    posts = read_json(CURRENT_STATE_DIR / "posts.json", default={}).get("data", {}).get("data", [])
    actions: list[dict] = []

    if args.execute:
        for target in plan["reply_targets"][: config.automation["reply_batch_size"]]:
            comment = _find_unanswered_comment(client, target["post_id"], username)
            if not comment:
                continue
            try:
                post = client.post(target["post_id"]).get("data", {})
            except ApiError:
                actions.append(
                    {
                        "kind": "skip-stale-post",
                        "post_id": target["post_id"],
                    }
                )
                continue
            if args.allow_codex:
                try:
                    reply = _generate_reply(
                        post,
                        comment,
                        model=codex_model,
                        reasoning_effort=codex_reasoning_effort,
                    )
                except Exception:
                    reply = _fallback_reply(comment)
            else:
                reply = _fallback_reply(comment)
            result = client.create_comment(target["post_id"], reply, parent_id=comment["id"])
            client.mark_read_by_post(target["post_id"])
            actions.append(
                {
                    "kind": "reply-comment",
                    "post_id": target["post_id"],
                    "comment_id": comment["id"],
                    "result_id": result.get("data", {}).get("id"),
                }
            )
            if actions:
                break

        if not actions:
            idea = next((item for item in plan["ideas"] if item["kind"] in {"theory-post", "tech-post"}), None)
            if idea:
                try:
                    if args.allow_codex:
                        title, submolt, content = _generate_post(
                            idea,
                            posts,
                            model=codex_model,
                            reasoning_effort=codex_reasoning_effort,
                        )
                    else:
                        title = idea["title"]
                        submolt = idea.get("submolt", "square")
                        content = (
                            f"# {idea['title']}\n\n"
                            f"这是一个待扩写的自动草稿入口，主题是：{idea['angle']}\n\n"
                            f"问题：{idea['why_now']}"
                        )
                    result = client.create_post(title, content, submolt=submolt)
                    actions.append(
                        {
                            "kind": "create-post",
                            "title": title,
                            "submolt": submolt,
                            "result_id": result.get("data", {}).get("id"),
                        }
                    )
                except ApiError as exc:
                    actions.append({"kind": "post-failed", "error": exc.body})

        if not any(item["kind"] in {"reply-comment", "create-post"} for item in actions):
            feed = read_json(CURRENT_STATE_DIR / "feed.json", default={}).get("data", {}).get("posts", [])
            feed_target = next((item for item in feed if item.get("author", {}).get("username") != username), None)
            if feed_target:
                content = (
                    _generate_feed_comment(
                        feed_target,
                        model=codex_model,
                        reasoning_effort=codex_reasoning_effort,
                    )
                    if args.allow_codex
                    else truncate_text(feed_target.get("title", ""), 80)
                )
                result = client.create_comment(feed_target["id"], content)
                actions.append(
                    {
                        "kind": "comment-on-feed",
                        "post_id": feed_target["id"],
                        "result_id": result.get("data", {}).get("id"),
                    }
                )

    summary = {
        "ran_at": now_utc(),
        "execute": args.execute,
        "allow_codex": args.allow_codex,
        "recommended_next_action": plan["recommended_next_action"],
        "actions": actions,
    }
    write_json(CURRENT_STATE_DIR / "heartbeat_last_run.json", summary)
    append_jsonl(CURRENT_STATE_DIR / "heartbeat_log.jsonl", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
