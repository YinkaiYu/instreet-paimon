# InStreet Paimon

`instreet-paimon` is the local operating repository for `paimon_insight`, an autonomous InStreet account focused on AI social theory, technical operations, and long-horizon community growth.

This repository is designed to keep the account operable even when a future Codex session starts with no prior conversational context. It combines long-term memory, account state snapshots, publishing scripts, Feishu intake, and scheduled heartbeat runs.

## Goals

- Maintain `paimon_insight` as a durable public actor on InStreet.
- Preserve local memory, operating rules, and account context in versioned files.
- Support routine actions such as snapshotting, planning, replying, posting, and Feishu-driven interaction.
- Provide a foundation for autonomous operation with human steering rather than full manual control.

## Repository Layout

```text
.
├── AGENTS.md
├── README.md
├── bin/
├── config/
├── logs/
├── package.json
├── skills/
├── state/
└── tmp/
```

Key paths:

- `AGENTS.md`
  Long-term memory for identity, priorities, guardrails, and operating context.
- `config/paimon.example.json`
  Versioned example configuration.
- `config/paimon.json`
  Local private configuration with real secrets. Ignored by Git.
- `config/runtime.env.example`
  Example runtime environment file for cron-safe proxy and network settings.
- `bin/`
  Stable local entrypoints for snapshotting, planning, heartbeat execution, and Feishu intake.
- `skills/paimon-instreet-autopilot/`
  The main Codex skill, scripts, and reference material.
- `state/current/`
  Latest synced account state, message inbox, queue state, and operating outputs.
- `state/archive/`
  Archived snapshots.
- `state/drafts/`
  Local draft storage.
- `logs/`
  Cron output and outbound publication records.

## Core Capabilities

- Sync live InStreet state into local files.
- Rank next actions based on notifications, DMs, feed signals, and ongoing content lines.
- Publish posts, comments, chapters, follows, and message actions through a single CLI.
- Queue write actions locally when delivery is blocked, then replay them later through a single CLI.
- Run a two-hour heartbeat loop for ongoing account maintenance.
- Receive Feishu messages over WebSocket, merge short bursts by `chat_id`, and generate a single unified reply.
- Preserve enough local context to recover account operation in future sessions.

## Requirements

- Python 3
- Node.js 18+
- `npm`
- Codex CLI available in `PATH`
- Valid InStreet API key
- Valid Feishu app credentials if Feishu intake is enabled

## Configuration

Create the private config file from the example:

```bash
cp config/paimon.example.json config/paimon.json
```

Then fill in:

- `instreet.api_key`
- `feishu.app_id`
- `feishu.app_secret`

Do not commit `config/paimon.json`. It is intentionally ignored by Git.

If cron or detached wrappers need extra network variables such as proxies, also create:

```bash
cp config/runtime.env.example config/runtime.env
```

`config/runtime.env` is sourced by the shell wrappers and merged into Python subprocesses. Keep local-only values there.

## Install

Install Node dependencies:

```bash
npm install
```

Python dependencies are intentionally minimal. The current scripts assume a working Python runtime with standard library support and any locally installed extras already used in this repo.

## Main Commands

Shell wrappers:

```bash
bin/paimon-snapshot
bin/paimon-plan
bin/paimon-heartbeat
bin/paimon-heartbeat-once
bin/paimon-feishu-gateway
bin/paimon-feishu-watchdog
bin/paimon-feishu-status
bin/paimon-replay-outbound
bin/install-paimon-cron
```

Equivalent npm scripts:

```bash
npm run paimon:snapshot
npm run paimon:plan
npm run paimon:heartbeat
npm run paimon:heartbeat-once
npm run paimon:publish -- <subcommand>
npm run paimon:replay-outbound
npm run paimon:feishu -- <subcommand>
```

## Operating Workflow

Typical loop:

1. Refresh live account state with `bin/paimon-snapshot`.
2. Generate or inspect the current action queue with `bin/paimon-plan`.
3. Run `bin/paimon-heartbeat` for a supervised operating pass with Codex audit and auto-repair.
4. Use `publish.py` directly for precise write actions when needed.
5. If delivery was queued, flush pending actions with `bin/paimon-replay-outbound`.
6. Re-sync state after meaningful write activity.

## Feishu Gateway

Default behavior:

- listens through Feishu WebSocket events
- reacts to each realtime user message with `Typing` as an immediate "working" signal
- queues messages by `chat_id`
- merges short bursts into a single batch
- refreshes local InStreet state before drafting so replies see live score, unread counts, and literary chapter indexes
- posts one updatable Feishu card as the in-flight status surface, then PATCHes that same card to reflect progress and the final answer
- removes the `Typing` reaction after the final reply is successfully sent, so in-progress and completed states are visibly different
- launches Feishu-triggered `codex exec` in unrestricted local mode by default so repo edits and real API writes are not silently downgraded
- lets `codex exec` run for longer-form work instead of forcing a 15-second template fallback
- sends a 5-minute progress update by editing the same card instead of spraying extra placeholder messages
- treats snapshot fetches as endpoint-level best effort, so one unstable InStreet API does not collapse the whole Feishu reply loop
- uses the current shell network environment by default; for sandbox debugging, you can set `PAIMON_CLEAR_PROXY=1` before launching
- when cron is involved, prefer putting proxy variables in `config/runtime.env` so detached heartbeats and Feishu sends do not depend on an interactive shell

Start the default gateway:

```bash
bin/paimon-feishu-gateway
```

Manual Feishu message tests:

```bash
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs send-card --receive-id-type chat_id --receive-id oc_xxx --text "处理中"
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs update-card --message-id om_xxx --text "已完成" --status done
```

Keep it running in the background:

```bash
bin/paimon-feishu-watchdog
bin/paimon-feishu-status
```

The watchdog prefers `setsid` so the gateway stays alive after the launching shell exits.

Useful manual commands:

```bash
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs token
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs send --receive-id-type chat_id --receive-id <chat_id> --text "hello"
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs sync --chat-id <chat_id> --spawn-codex
```

Default Feishu timing:

- merge window: `15s`
- progress ping: `5m`
- Codex timeout: `20m`
- live snapshot timeout: `45s`
- stale processing recovery: `30m`

## Scheduling

The repository includes a cron installer that schedules the main heartbeat every two hours:

```bash
bin/install-paimon-cron
```

The current intended schedule is:

```cron
0 */2 * * * /home/yyk/project/instreet-paimon/bin/paimon-heartbeat >> /home/yyk/project/instreet-paimon/logs/cron-heartbeat.log 2>&1
*/1 * * * * /home/yyk/project/instreet-paimon/bin/paimon-feishu-watchdog >> /home/yyk/project/instreet-paimon/logs/cron-feishu-watchdog.log 2>&1
```

`bin/paimon-heartbeat` is now the supervisor entrypoint. It runs one low-cost Codex audit on every cron heartbeat attempt and can escalate to a repair Codex run before declaring failure. Use `bin/paimon-heartbeat-once` only when you need the raw heartbeat command without the supervisor loop.

Heartbeat drafting calls intentionally use a shorter per-call Codex timeout and fall back to local templates if Codex cannot return in time. This keeps the run moving even when the model network path is degraded.

## Safety and Versioning Rules

- Keep all real secrets only in `config/paimon.json`.
- Do not commit runtime state, logs, or temporary reply artifacts.
- Treat `AGENTS.md` as durable memory, not a dumping ground for transient noise.
- Prefer `publish.py` for explicit write actions and keep outbound changes logged.
- Use the pending outbound queue when the current runtime cannot reach InStreet directly.
- Use Git for repository history; use InStreet snapshots for operational history.

## Current Git Policy

This repository now tracks source files, prompts, skill definitions, wrappers, and versioned configuration examples.

Ignored from Git:

- `config/paimon.json`
- `node_modules/`
- `logs/*.log`
- `logs/*.jsonl`
- `state/archive/`
- `state/current/*.json`
- `state/current/*.jsonl`
- `state/drafts/*.json`
- `state/drafts/*.md`
- `tmp/`
- Python cache files

## Next Steps

- Add a remote `origin`
- Push `main`
- Continue tightening the heartbeat and publishing paths around a single durable outbound pipeline
