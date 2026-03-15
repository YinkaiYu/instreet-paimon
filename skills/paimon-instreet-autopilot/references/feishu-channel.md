# Feishu Channel

## App configuration

- App ID: `cli_a93ee8023cb89bb5`
- Mode: WebSocket long connection
- DM policy: whitelist
- Group policy: open

## Supported gateway actions

- Fetch tenant access token
- Send text messages to `chat_id`, `open_id`, or other supported receive ID types
- Start a long-connection event listener through the official Node SDK
- Sync user messages from an existing chat through message history when long-connection events are incomplete
- Persist incoming message events to `state/current/feishu_inbox.jsonl`
- Optionally spawn `codex exec` to generate a response and send it back to the originating chat

## Runtime design

1. The gateway listens for `im.message.receive_v1`
2. Each event is normalized and appended to the inbox log
3. User messages are queued by `chat_id`, not processed one-by-one in parallel
4. The queue waits for a short merge window so consecutive short messages become one task
5. Default runtime sends only the final merged reply. `auto-ack` is optional and should be used only when an immediate receipt message is required.
6. If auto-response is enabled, the gateway triggers Codex in the repo root with recent chat history plus the merged batch
7. The Codex reply is sent back as a normal Feishu message through raw HTTP with retry, not a one-shot SDK call

This prevents the common failure mode where the user sends 2 to 3 follow-up messages before the first reply finishes. The queue is serialized per chat, keeps a short recent-history window, and restores stale in-flight batches after process crashes or restarts.

If the developer console is missing `im.message.receive_v1`, use the history sync fallback to poll a known `chat_id` and backfill unseen user messages into the same inbox log.

## Operational note

Long connection mode is convenient because it avoids public webhooks during local development. Use the official SDK rather than hard-coding a WebSocket protocol client when possible.

## Fallback

If the long connection flow is unavailable, the gateway still supports token verification and outbound text sending so the channel can be tested incrementally.
