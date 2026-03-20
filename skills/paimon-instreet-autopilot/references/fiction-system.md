# Fiction System

## Purpose

This repo supports long-form literary serials as durable operating assets, not one-off drafts.

The fiction pipeline must preserve:

- serial registry metadata and active/inactive status
- story bible and chapter plan files
- style sampling before each new fiction chapter
- continuity logs so later Codex sessions do not invent contradictory character states
- safe archive boundaries when a legacy work is retired

## Active serial layout

Preferred active fiction layout:

- `state/drafts/serials/<slug>/series-plan.json`
- `state/drafts/serials/<slug>/story-bible.md`
- `state/drafts/serials/<slug>/supporting-cast.json`
- `state/drafts/serials/<slug>/style-guide.md`
- `state/drafts/serials/<slug>/reader-hooks.md`
- `state/drafts/serials/<slug>/continuity-log.jsonl`
- optional supporting files such as `synopsis.md` or chapter seeds

`series-plan.json` is the machine-readable source of truth for:

- work metadata
- chapter queue
- `writing_notes`
- `writing_system`
- `story_bible`
- `supporting_cast`

Recommended structured path fields inside `series-plan.json`:

- `work.synopsis_path`
- `work.story_bible_path`
- `writing_notes.style_guide_path`
- `work.supporting_cast_path`
- `writing_system.continuity_system.log_path`
- `writing_system.supporting_cast_system.cast_path`

Operational rule:

- keep the external markdown and jsonl files as human-editable assets
- mirror their durable constraints back into `series-plan.json` so heartbeat and audits can consume structured summaries instead of dumping whole documents into prompts
- recurring supporting characters should live in `supporting-cast.json` with stable names, role hooks, and recurrence rules so future chapters do not regress into generic labels like “负责人” or “顾问”

## Style sampling rule

Before each new fiction chapter:

1. Sample a contiguous ~20000-character excerpt from `writing_system.style_source_path`.
2. Write the excerpt and style summary into `state/drafts/style_sessions/`.
3. Use the summary to imitate language rhythm only.
4. Never inherit setting, names, plot beats, or conflicts from the reference corpus.

CLI:

```bash
npm run paimon:style-sample -- --source-path state/drafts/style-corpus/longform-reference.txt --label serial-chapter
```

## Plan audit rule

Before drafting or rewriting the next fiction chapter, audit the series plan:

```bash
python3 skills/paimon-instreet-autopilot/scripts/fiction_plan_audit.py --plan state/drafts/serials/quanyuzhou-relian/series-plan.json --lookahead 10
```

The audit should confirm that upcoming chapters have structured `beats`, `intimacy_target`, `seed_threads`, `payoff_threads`, and hook metadata instead of relying on loose prose-only notes. It should also verify that synopsis/style/story-bible/continuity paths exist and that documented longline threads are actually mapped into chapter `seed_threads` / `payoff_threads`.

## Post-publish QA rule

After each newly published or updated fiction chapter:

1. Fetch the online chapter version from InStreet, not just the local source file.
2. Read through the published text end to end and check for detail errors, formatting issues, logic breaks, awkward wording, and rendering mismatches.
3. If any issue is found, update the chapter immediately instead of waiting for a later batch fix.
4. Keep the local archive in sync with the corrected online version, including the chapter markdown and metadata.

## Heartbeat behavior

- `heartbeat.py` may publish a fiction chapter only when there is an active serial in `serial_registry`.
- `heartbeat.py` must tolerate an empty active-literary queue and degrade to forum/group output instead of treating it as a failure.
- Retired works must be removed from the active queue or marked `heartbeat_enabled=false`.
- When generating a fiction chapter, prefer structured story-bible summaries plus the latest continuity bullets; do not dump entire reference documents into the prompt unless debugging a specific inconsistency.

## Retirement rule

When a work is retired:

- archive published chapters and legacy drafts under `state/archive/fiction/<slug>-legacy/`
- keep retired content out of future writing prompts
- do not let the retired work remain in the active literary queue
- if the platform still retains a shell work, leave it heartbeat-disabled and clearly marked as discontinued
