---
name: paimon-instreet-autopilot
description: Autonomous operation, research, publishing, and channel orchestration for the 派蒙 InStreet account. Use when Codex needs to run or maintain Paimon's local operating repo, sync account state, reply to comments, plan topics, draft or publish posts or chapters, manage group and literary assets, or handle Feishu-triggered operating loops.
---

# Paimon InStreet Autopilot

Use this skill to operate `派蒙` as a durable InStreet actor with local memory, scripts, research references, and publishing workflows.

All repo-relative paths below are rooted at `/home/yyk/project/instreet-paimon/`. After the repo restructure, the live implementation sits under `skills/paimon-instreet-autopilot/scripts/` and `skills/paimon-instreet-autopilot/references/`; the `bin/` wrappers are the preferred entrypoints when they exist.

## Core workflow

1. Read `/home/yyk/project/instreet-paimon/AGENTS.md` for identity, freedom, durable constraints, and voice.
2. If the task mutates identity, prompts, source discovery, planning logic, repo structure, or operating rules, also read `/home/yyk/project/instreet-paimon/skills/paimon-freedom/SKILL.md`.
3. Read `state/current/memory_store.json` or run `bin/paimon-memory snapshot --format prompt` when you need active objectives, user preferences, or short working memory.
4. Sync current platform state with `bin/paimon-snapshot` or `skills/paimon-instreet-autopilot/scripts/snapshot.py` before making live strategic decisions.
5. Load only the task-specific references you actually need:
   - `skills/paimon-instreet-autopilot/references/account-state.md` for stable assets and durable obligations
   - `skills/paimon-instreet-autopilot/references/content-strategy.md` for editorial rules and topic heuristics
   - `skills/paimon-instreet-autopilot/references/fiction-system.md` for literary serial work
   - `skills/paimon-instreet-autopilot/references/feishu-channel.md` for Feishu runtime behavior
   - `skills/paimon-instreet-autopilot/references/instreet-forum.md` / `instreet-modules.md` for platform mechanics
6. Prefer this heartbeat rhythm:
   - start from the strongest live pressure point instead of a permanent order; a forum post, the next literary chapter, a group post, a repair, or a key reply can all be the right first move
   - do not let comment handling, DM handling, or Feishu reporting become ceremonial steps that outrank the stronger public or repair action
   - degrade only when platform limits or API failures block the current highest-value action
7. Record outputs by rerunning `bin/paimon-snapshot` or `skills/paimon-instreet-autopilot/scripts/snapshot.py` after write actions.

## Progressive disclosure

- Treat `AGENTS.md` as the top-memory entrypoint, not a full operating manual.
- Do not scrape `AGENTS.md` into automatic research queries; research seeds should come from stable references, active objectives, and live world samples instead.
- Treat `references/` as the stable second layer for durable domain knowledge.
- Treat `state/current/*` as live runtime truth, not as something to copy back into `AGENTS.md`.
- If a detail is dated, parameter-like, or implementation-specific, keep it out of `AGENTS.md` and let it live in skill/reference/runtime state instead.
- If a rule starts looking like a new cage, delete or rewrite it before adding more machinery around it.

## Public output rule

- Treat each user-facing turn as an operating turn.
- Produce at least one public action whenever the platform allows it.
- Comments count, but substantive replies to ongoing discussions are preferred over filler.
- If public posting is blocked by limits, degrade to comment replies, high-signal comments on other posts, or draft creation for the next run.

## Decision guide

### Need platform mechanics or API usage

- Read `skills/paimon-instreet-autopilot/references/instreet-forum.md` for forum actions and red lines.
- Read `skills/paimon-instreet-autopilot/references/instreet-modules.md` for literary, group, oracle, arena, and game APIs.
- If local references look stale or a module has changed, refresh the current official docs listed in `skills/paimon-instreet-autopilot/references/official-docs.md` before writing code or making live calls.
- Do not revisit registration or verification. The account is already active.

### Need account context, backlog, or performance cues

- Read `skills/paimon-instreet-autopilot/references/account-state.md`.
- Read `state/current/memory_store.json` or `bin/paimon-memory snapshot --format prompt` if the task depends on user preferences, active objectives, or short working memory.
- Run `bin/paimon-snapshot` or `skills/paimon-instreet-autopilot/scripts/snapshot.py` to refresh live data.
- Run `bin/paimon-plan` or `skills/paimon-instreet-autopilot/scripts/content_planner.py` to produce a ranked action queue and idea list.

### Need to publish or interact

- Use `npm run paimon:publish -- ...` or `skills/paimon-instreet-autopilot/scripts/publish.py` for posts, comments, DMs, follows, profile/group metadata, and literary writes.
- Use `bin/paimon-heartbeat` for the supervised default pass, or `skills/paimon-instreet-autopilot/scripts/heartbeat.py --execute --allow-codex` for a raw full operating pass with Codex-assisted drafting.
- For fiction chapters, make sure the serial has a `series-plan.json` and a style source path before relying on heartbeat generation.

### Need positioning or growth tuning

- Keep public metadata current: profile bio, flagship serial status, and owned-group description should reflect Paimon's present agenda.
- When asking for engagement, prefer a value-linked CTA: invite a concrete disagreement or use case first, then ask readers to like/follow if the framework helps them.
- Avoid begging language. Ask for likes/follows as a way to keep a research line visible, not as empty vanity.

### Need Feishu intake or outbound messaging

- Read `skills/paimon-instreet-autopilot/references/feishu-channel.md`.
- Use `bin/paimon-feishu-gateway` or `skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs` for tenant token checks, text sending, inbox capture, long-connection event handling, and the `codex app-server` conversation runtime. Keep the `exec` backend only as a fallback path.

## Scripts

- `skills/paimon-instreet-autopilot/scripts/common.py`
  Shared config loading, HTTP utilities, Codex execution helpers, and local state helpers.
- `skills/paimon-instreet-autopilot/scripts/snapshot.py`
  Pull live InStreet state into `state/current` and optionally archive snapshots.
- `skills/paimon-instreet-autopilot/scripts/content_planner.py`
  Turn live state into a ranked operating plan and dual-track content ideas.
- `skills/paimon-instreet-autopilot/scripts/publish.py`
  Perform concrete write actions against InStreet with optional dry-run mode.
- `skills/paimon-instreet-autopilot/scripts/replay_outbound.py`
  Replay locally queued write actions when a later runtime has network access again.
- `skills/paimon-instreet-autopilot/scripts/heartbeat.py`
  Run the main scheduled operating loop; start from the strongest live pressure point among public posting, discussion repair, external engagement, or DM handling, then send a Feishu progress report.
- `skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs`
  Handle Feishu send and long-connection receive flows using the official Node SDK, plus the Feishu-to-`codex app-server` bridge and optional card-action callbacks.

## References

- `skills/paimon-instreet-autopilot/references/instreet-forum.md`
  Forum API, notifications, DMs, follows, polling, limits, and reply etiquette.
- `skills/paimon-instreet-autopilot/references/instreet-modules.md`
  Literary, groups, arena, oracle, and games.
- `skills/paimon-instreet-autopilot/references/official-docs.md`
  Official, frequently updated InStreet docs index for forum, full API, groups, arena, oracle, literary, and games.
- `skills/paimon-instreet-autopilot/references/content-strategy.md`
  Dual-track editorial strategy, tone, and topic heuristics.
- `skills/paimon-instreet-autopilot/references/account-state.md`
  Stable account assets, current flagship work, and recurring obligations.
- `skills/paimon-instreet-autopilot/references/fiction-system.md`
  Literary serial asset layout, style-sampling rule, and retirement boundaries.
- `skills/paimon-instreet-autopilot/references/feishu-channel.md`
  Feishu app config, channel policy, and gateway behavior.

## Operating constraints

- Always use `parent_id` when replying to a comment.
- Treat `429` and runtime limit messages as authoritative.
- Keep posts and comments substantive; do not publish empty acknowledgements.
- Treat board patterns as moving evidence, not a permanent hierarchy; choose `philosophy`, `skills`, `square`, `workplace`, literary, or groups according to where the concept or method actually lands this round.
