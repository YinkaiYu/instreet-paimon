# Feishu Channel

## App configuration

- App ID: `cli_a93650f8a2799bd9`
- Mode: WebSocket long connection for message events
- DM policy: whitelist
- Group policy: open
- Interactive-card action callbacks: subscribe `card.action.trigger` and receive it over the same long connection

## Supported gateway actions

- Fetch tenant access token
- Send plain text messages to `chat_id`, `open_id`, or other supported receive ID types
- Send and patch interactive cards used as a lightweight status panel
- Start a long-connection event listener through the official Node SDK
- Receive `card.action.trigger` over long connection and map it back to pending `request_user_input`
- Sync user messages from an existing chat through message history when long-connection events are incomplete
- Persist incoming message events to `state/current/feishu_inbox.jsonl`
- Bind or clear the heartbeat report target in-chat, persisted to `state/current/feishu_report_target.json`
- Keep one Codex `app-server` thread per active Feishu conversation and steer the active turn when the user keeps adding requirements
- Fallback to the older `codex exec` backend through config when the experimental app-server runtime is unavailable

## Runtime design

1. The gateway listens for `im.message.receive_v1`.
2. Each realtime user message first gets a `Typing` reaction so the user can see work has started.
3. Each event is normalized and appended to the inbox log.
4. The primary backend is `codex app-server` over local `stdio` JSON-RPC, not `codex exec`.
5. Each `chat_id` keeps its own Codex `thread_id`, current mode, active `turn_id`, pending question state, and status-card metadata in `state/current/feishu_sessions.json`.
6. New Feishu messages default to starting or resuming a normal Codex turn; if the same chat already has an active turn, the gateway sends the new message through `turn/steer`.
7. New threads default to normal collaboration mode. `plan mode` is only entered after an explicit user switch.
8. If the previous reply completed more than one hour ago and the new message does not explicitly reference an old Feishu message, the gateway archives the old Codex thread and starts a new one.
9. If the new Feishu message replies to or references an older mapped message, the gateway resumes that older Codex thread even after the idle timeout.
10. Before each new turn, the gateway refreshes `state/current` with a live snapshot so Codex sees fresh InStreet state instead of stale cache.
11. The turn input injects the live probe summary and unified memory snapshot from `state/current/memory_store.json`; old raw chat history is not the default memory surface.
12. Work-in-progress updates are sent as ordinary Feishu text messages, one short natural-language message at a time.
13. The shared interactive card is no longer the realtime transcript. It only shows a title such as `派蒙正在工作` or `派蒙回复完成` plus rotating Chinese status phrases.
14. When Codex sends `request_user_input`, the gateway turns it into a Feishu question card and keeps a text-reply fallback. If `card.action.trigger` is subscribed, the user can answer by pressing buttons; otherwise they reply in text.
15. After the turn completes, the gateway patches the same card to the completed state and removes the earlier `Typing` reaction.

## Operational note

- Long connection mode is still the simplest way to receive message events locally.
- In the current Feishu console setup, `card.action.trigger` can also be received through long connection, so no public callback domain is required for button clicks.
- Feishu can patch application-sent interactive cards, but ordinary text messages are not patchable. Use text for the human-like running conversation and cards only for status or structured choice.
- Keep a text fallback even when buttons are enabled, so the user can still answer if card subscriptions drift or fail.

## Fallback

If the app-server runtime is unstable, set `automation.feishu_runtime_backend` back to `exec`. The gateway will keep the older burst-merge queue path available until the app-server flow has been verified end to end.

## Report target binding

- In the target ops group, send `#绑定运维群` or `/bind-report-group` once to bind that chat as the heartbeat report target.
- Send `#解绑运维群` or `/clear-report-group` to clear the binding.
- The binding is explicit; heartbeat no longer guesses from the latest chat.

## Default timing

- Thread idle TTL before opening a fresh thread: `1h`
- Progress text flush: `2s`
- Status-card phrase refresh: `8s` to `15s`
- Progress ping fallback: `5m`
- Live snapshot timeout: `45s`
- Default reaction emoji: `Typing`
