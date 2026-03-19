# Feishu Channel

## App configuration

- App ID: `cli_a93ee8023cb89bb5`
- Mode: WebSocket long connection
- DM policy: whitelist
- Group policy: open

## Supported gateway actions

- Fetch tenant access token
- Send text messages to `chat_id`, `open_id`, or other supported receive ID types
- Send and patch interactive card messages for long-running work updates
- Start a long-connection event listener through the official Node SDK
- Sync user messages from an existing chat through message history when long-connection events are incomplete
- Persist incoming message events to `state/current/feishu_inbox.jsonl`
- Optionally spawn `codex exec` to generate a response and send it back to the originating chat

## Runtime design

1. The gateway listens for `im.message.receive_v1`
2. Each realtime user message first gets a `Typing` reaction so the user can see work has started
3. Each event is normalized and appended to the inbox log
4. User messages are queued by `chat_id`, not processed one-by-one in parallel
5. The queue waits for a short merge window so consecutive short messages become one task
6. Default runtime sends one updatable interactive card as the in-flight status surface; `auto-ack` is optional and should be used only when an immediate receipt message is required.
7. The same card is patched from `处理中` to `已完成`, so the user sees one continuous artifact instead of multiple placeholder messages
8. After the final reply is sent successfully, the gateway deletes the earlier `Typing` reaction from that user message
9. If auto-response is enabled, the gateway first refreshes `state/current` with a live snapshot so Codex sees fresh InStreet state instead of stale cache
10. Snapshot fetches are endpoint-level best effort; if one InStreet API fails, the gateway records that degraded endpoint instead of collapsing the whole reply loop
11. The Codex prompt includes that live probe summary and must distinguish `local cache missing` from `remote API unavailable`
12. The gateway triggers Codex in the repo root with the merged batch, a short continuation window, and the unified memory snapshot from `state/current/memory_store.json`; old raw chat history is not default prompt context
13. Long-running Codex jobs do not fall back after a few seconds. The gateway waits up to the configured long timeout, and after 5 minutes patches the same card to tell the user to wait.

This prevents the common failure mode where the user sends 2 to 3 follow-up messages before the first reply finishes. The queue is serialized per chat, keeps a short recent-history window, relies on structured global memory for durable context, and restores stale in-flight batches after process crashes or restarts.

If the developer console is missing `im.message.receive_v1`, use the history sync fallback to poll a known `chat_id` and backfill unseen user messages into the same inbox log.

## Operational note

Long connection mode is convenient because it avoids public webhooks during local development. Use the official SDK rather than hard-coding a WebSocket protocol client when possible.

Feishu's official message update capability is for app-sent interactive cards, not ordinary text messages. Keep `Typing` as the lightweight immediate signal, and use the shared card as the editable progress surface.

## Fallback

If the long connection flow is unavailable, the gateway still supports token verification and outbound text sending so the channel can be tested incrementally.

## Default timing

- Merge window: `15s`
- Progress ping: `5m`
- Codex timeout: `20m`
- Live snapshot timeout: `45s`
- Processing stale timeout: `30m`
- Default reaction emoji: `Typing`
