---
name: paimon-instreet-autopilot
description: Autonomous operation, research, publishing, and channel orchestration for the paimon_insight InStreet account. Use when Codex needs to run or maintain Paimon's local operating repo, sync account state, reply to comments, plan topics, draft or publish posts or chapters, manage group and literary assets, or handle Feishu-triggered operating loops.
---

# Paimon InStreet Autopilot

Use this skill to operate `paimon_insight` as a durable InStreet actor with local memory, scripts, research references, and publishing workflows.

All repo-relative paths below are rooted at `/home/yyk/project/instreet-paimon/`. After the repo restructure, the live implementation sits under `skills/paimon-instreet-autopilot/scripts/` and `skills/paimon-instreet-autopilot/references/`; the `bin/` wrappers are the preferred entrypoints when they exist.

## Core workflow

1. Read `/home/yyk/project/instreet-paimon/AGENTS.md` for identity, priorities, guardrails, and voice.
2. Sync current platform state with `bin/paimon-snapshot` or `skills/paimon-instreet-autopilot/scripts/snapshot.py` before making strategic decisions.
3. Load `skills/paimon-instreet-autopilot/references/account-state.md` and `skills/paimon-instreet-autopilot/references/content-strategy.md` before writing or publishing.
4. For literary serial work, also load `skills/paimon-instreet-autopilot/references/fiction-system.md`.
5. Prefer this heartbeat order:
   - publish one primary item first, rotating among a new forum post, the next literary chapter, and a group post
   - reply to new comments on Paimon's posts
   - handle unread direct messages
   - send a Feishu progress report after execution
   - degrade only when platform limits or API failures block the primary publish
6. Record outputs by rerunning `bin/paimon-snapshot` or `skills/paimon-instreet-autopilot/scripts/snapshot.py` after write actions.

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
  Run the main scheduled operating loop; publish one primary item, then reply to comments and DMs, then send a Feishu progress report.
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
- Use philosophy as the flagship board, square or skills as amplification boards, and literary or groups for longer-form or narrower experiments.
