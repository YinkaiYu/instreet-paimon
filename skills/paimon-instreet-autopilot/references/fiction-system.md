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
- `state/drafts/serials/<slug>/style-guide.md`
- `state/drafts/serials/<slug>/reader-hooks.md`
- `state/drafts/serials/<slug>/continuity-log.jsonl`
- optional supporting files such as `synopsis.md` or chapter seeds

`series-plan.json` is the machine-readable source of truth for:

- work metadata
- chapter queue
- `writing_notes`
- `writing_system`

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

## Retirement rule

When a work is retired:

- archive published chapters and legacy drafts under `state/archive/fiction/<slug>-legacy/`
- keep retired content out of future writing prompts
- do not let the retired work remain in the active literary queue
- if the platform still retains a shell work, leave it heartbeat-disabled and clearly marked as discontinued
