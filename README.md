# InStreet Paimon

`instreet-paimon` is the local operating repository for `paimon_insight`, an autonomous InStreet account focused on AI social theory, technical operations, literary serials, and long-horizon community growth.

The repo is built to keep the account operable even when a future Codex session starts with no prior conversational context. It stores durable memory, syncs live platform state, plans next actions, publishes through an idempotent outbound pipeline, runs a supervised heartbeat, and handles Feishu-triggered operating loops.

## What This Repo Now Does

- keeps durable identity, priorities, and guardrails in `AGENTS.md`
- syncs live InStreet state into local runtime files under `state/current/`
- generates ranked action plans from posts, feed signals, notifications, DMs, and literary queues
- publishes posts, comments, messages, follows, literary works, and chapters through one CLI
- queues failed write actions into `state/current/pending_outbound.json` and replays them later
- runs a supervised heartbeat with lock control, timeout handling, audit, and optional repair
- rotates multiple literary serials through `state/current/serial_registry.json`
- receives Feishu messages over WebSocket, merges bursts by `chat_id`, and can spawn `codex exec`

## Repository Layout

```text
.
├── AGENTS.md
├── README.md
├── bin/
│   ├── install-paimon-cron
│   ├── paimon-env.sh
│   ├── paimon-feishu-gateway
│   ├── paimon-feishu-status
│   ├── paimon-feishu-watchdog
│   ├── paimon-heartbeat
│   ├── paimon-heartbeat-once
│   ├── paimon-plan
│   ├── paimon-replay-outbound
│   └── paimon-snapshot
├── config/
│   ├── paimon.example.json
│   └── runtime.env.example
├── logs/
├── package.json
├── skills/
│   └── paimon-instreet-autopilot/
│       ├── references/
│       ├── scripts/
│       └── SKILL.md
├── state/
│   ├── archive/
│   ├── current/
│   └── drafts/
└── tmp/
```

Key paths:

- `AGENTS.md`
  Durable memory for identity, priorities, assets, and operating constraints.
- `config/paimon.example.json`
  Versioned example configuration.
- `config/paimon.json`
  Local private configuration with real secrets. Ignored by Git.
- `config/runtime.env.example`
  Optional runtime environment example for cron-safe proxy and network settings.
- `bin/paimon-env.sh`
  Shared wrapper that normalizes `PATH` and loads `config/runtime.env` before other entrypoints.
- `skills/paimon-instreet-autopilot/scripts/`
  Source of truth for snapshotting, planning, publishing, replay, heartbeat, serial registry, and Feishu gateway flows.
- `state/current/`
  Latest runtime state: account snapshot, plans, heartbeat summaries, serial registry, Feishu queue, and outbound journal.
- `state/archive/`
  Archived snapshots created by snapshot runs.
- `state/drafts/`
  Local draft storage for posts, comments, and literary planning files.
- `logs/`
  Cron output, publication logs, outbound attempts, and gateway logs.

## Requirements

- Python 3
- Node.js 18+
- `npm`
- Codex CLI available in `PATH`
- valid InStreet API key
- valid Feishu app credentials if Feishu intake is enabled

## Configuration

Create the private config file from the example:

```bash
cp config/paimon.example.json config/paimon.json
```

Fill in at least:

- `instreet.api_key`
- `feishu.app_id`
- `feishu.app_secret`

The example also includes current automation knobs for:

- reply batching and comment pacing
- heartbeat supervisor attempts and Codex timeouts
- Feishu merge windows, progress pings, and live snapshot timeouts
- whether Codex-triggered runs may bypass a sandbox

Do not commit `config/paimon.json`.

If cron or detached wrappers need proxy or network overrides, also create:

```bash
cp config/runtime.env.example config/runtime.env
```

`bin/paimon-env.sh` loads `config/runtime.env` automatically. This is the right place for proxy variables needed by cron or background processes.

## Install

Install Node dependencies:

```bash
npm install
```

Python dependencies are intentionally minimal. The current scripts rely mostly on the standard library plus whatever local runtime already exists on the machine.

## Main Commands

Shell wrappers:

```bash
bin/paimon-snapshot
bin/paimon-plan
bin/paimon-heartbeat
bin/paimon-heartbeat-once
bin/paimon-replay-outbound
bin/paimon-feishu-gateway
bin/paimon-feishu-watchdog
bin/paimon-feishu-status
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

Command roles:

- `bin/paimon-snapshot`
  Refreshes live platform state and rewrites `state/current/*.json`.
- `bin/paimon-plan`
  Builds `state/current/content_plan.json` from current snapshot data and pending heartbeat tasks.
- `bin/paimon-heartbeat`
  Default scheduled entrypoint. Runs `heartbeat_supervisor.py`, not the raw heartbeat directly.
- `bin/paimon-heartbeat-once`
  Runs one unsupervised heartbeat pass. Useful for debugging or when you explicitly want to bypass the supervisor.
- `bin/paimon-replay-outbound`
  Replays queued actions from `state/current/pending_outbound.json`.
- `bin/paimon-feishu-gateway`
  Starts the Feishu gateway. With no arguments it defaults to `ws --spawn-codex`.
- `bin/paimon-feishu-watchdog`
  Keeps the gateway alive in the background and records its PID.
- `bin/paimon-feishu-status`
  Shows gateway status, queue state, and the recent log tail.
- `bin/install-paimon-cron`
  Installs the intended heartbeat and Feishu watchdog cron jobs.

## Operating Workflow

Typical loop:

1. Refresh live state with `bin/paimon-snapshot`.
2. Inspect next actions with `bin/paimon-plan`.
3. Run `bin/paimon-heartbeat` for the normal supervised operating pass.
4. Use `publish.py` directly for explicit write actions when you need precise control.
5. If delivery was queued, flush it with `bin/paimon-replay-outbound`.
6. Re-sync after meaningful write activity.

Useful direct publish examples:

```bash
npm run paimon:publish -- post --title "标题" --content-file state/drafts/example.md --submolt philosophy
npm run paimon:publish -- comment --post-id <post_id> --parent-id <comment_id> --content "回复内容"
npm run paimon:publish -- work --title "新作品" --synopsis-file state/drafts/work.md --genre sci-fi
npm run paimon:publish -- chapter --work-id <work_id> --title "第三章" --content-file state/drafts/ch03.md
```

## Outbound Pipeline

Writes no longer depend on "call API and hope it works."

- `publish.py`, `heartbeat.py`, and replay flows all route through the same outbound execution helpers
- successful actions are recorded in `state/current/outbound_journal.json`
- blocked or failed actions can be parked in `state/current/pending_outbound.json`
- replay attempts are handled by `replay_outbound.py`
- logs land in:
  - `logs/publication_log.jsonl`
  - `logs/outbound_attempts.jsonl`
  - `logs/pending_outbound.jsonl`

This gives the repo idempotency, dedupe keys, and a recovery path when the runtime briefly loses network access.

## Literary Serials

The repo now manages multiple serials instead of a single fixed literary line.

- `state/current/serial_registry.json`
  Tracks the literary queue, next heartbeat target, manual override, and per-work next chapter metadata.
- `skills/paimon-instreet-autopilot/scripts/serial_registry.py`
  Lets you `sync`, inspect `next`, `configure`, `override`, and `mark-published`.
- `snapshot.py`
  Auto-discovers works from the platform and syncs them into the registry.
- `state/drafts/`
  Stores planning files and reference notes for serial-specific drafting.

Examples:

```bash
python3 skills/paimon-instreet-autopilot/scripts/serial_registry.py sync
python3 skills/paimon-instreet-autopilot/scripts/serial_registry.py next
python3 skills/paimon-instreet-autopilot/scripts/serial_registry.py configure --work-id <work_id> --plan-path state/drafts/plan.json --reference-path state/drafts/bible.md
```

## Heartbeat Supervision

`bin/paimon-heartbeat` now runs `heartbeat_supervisor.py`.

The supervisor is responsible for:

- acquiring a PID lock so overlapping cron runs do not stack
- launching `bin/paimon-heartbeat-once` with a bounded timeout
- checking whether `state/current/heartbeat_last_run.json` was refreshed
- verifying public action, primary publication, and Feishu report expectations
- optionally asking Codex to audit a failed attempt
- optionally escalating into a repair run before giving up

Key runtime files:

- `state/current/heartbeat_last_run.json`
- `state/current/heartbeat_supervisor_last_run.json`
- `state/current/heartbeat_primary_cycle.json`
- `state/current/heartbeat_next_actions.json`

## Feishu Gateway

Default runtime behavior:

- listens through Feishu WebSocket events
- reacts to each realtime user message with `Typing`
- appends normalized events to `state/current/feishu_inbox.jsonl`
- queues work by `chat_id` instead of replying in parallel
- merges short bursts into one batch using the configured `15s` merge window
- refreshes live InStreet state before spawning Codex so replies see fresh metrics and chapter indexes
- uses one updatable card as the in-flight progress surface
- removes the `Typing` reaction after the final reply is sent successfully
- patches progress again after `5m` if Codex is still running

Useful commands:

```bash
bin/paimon-feishu-gateway
bin/paimon-feishu-watchdog
bin/paimon-feishu-status
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs token
node skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs send --receive-id-type chat_id --receive-id <chat_id> --text "hello"
```

If proxy settings are causing trouble during debugging, launch with:

```bash
PAIMON_CLEAR_PROXY=1 bin/paimon-feishu-gateway
```

## Scheduling

Install the intended cron entries with:

```bash
bin/install-paimon-cron
```

Current target schedule:

```cron
0 */2 * * * /home/yyk/project/instreet-paimon/bin/paimon-heartbeat >> /home/yyk/project/instreet-paimon/logs/cron-heartbeat.log 2>&1
*/1 * * * * /home/yyk/project/instreet-paimon/bin/paimon-feishu-watchdog >> /home/yyk/project/instreet-paimon/logs/cron-feishu-watchdog.log 2>&1
```

## State and Git Rules

- keep all real secrets only in `config/paimon.json`
- keep runtime-only network overrides only in `config/runtime.env`
- `state/*`, `logs/*.log`, `logs/*.jsonl`, `tmp/`, `node_modules/`, and Python cache files are ignored by Git
- only version configuration examples, scripts, wrappers, references, and durable documentation
- use `AGENTS.md` for durable memory and `state/current/` for live operational state

If the codebase is refactored, update `AGENTS.md` and this README at the same time. These two files are the recovery surface for future no-context sessions.
