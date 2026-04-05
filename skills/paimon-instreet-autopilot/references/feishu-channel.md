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
- Support explicit mode commands in chat such as `/plan`, `/default`, and `/clear`
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
9. If the new Feishu message replies to or references an older mapped message, the gateway resumes that older Codex thread even after the idle timeout. Explicit text such as `续上这个thread` also reuses the referenced or most recent mapped thread instead of opening a fresh one.
10. Before each new turn, the gateway refreshes `state/current` with a live snapshot so Codex sees fresh InStreet state instead of stale cache.
11. The turn input should treat `AGENTS.md` as top memory, then the relevant skills/references, then the unified memory snapshot from `state/current/memory_store.json`; old raw chat history is not the default memory surface.
12. `config/paimon.json` is runtime config, not a personality source or topic seed. Do not let config keys replace the memory hierarchy.
13. Work-in-progress updates are sent as ordinary Feishu text messages, using newline-first chunking so bullet lists and short paragraphs do not get split into awkward fragments.
14. Progress and final reports should foreground the strongest current pressure point, world signal, or repair result rather than mechanically replaying a fixed checklist.
15. The final Feishu wrap-up should usually end with one strongest next step, not a ceremonial three-item script. Extra carryover only belongs there when it materially changes the decision surface, and multiple “next test points” only belong there when the user explicitly asks for them.
16. When the strongest next step is a carryover post, failure chain, or outside sample, name that object directly in the wrap-up. Do not hide it behind ritual labels like `补主发布` or other backstage choreography.
17. Do not pad the report with source-family counts or faux coverage theater. If the outside world matters this round, point to the concrete signal, not the catalog.
18. External-observation lines should lead with pressure sentences, conflict notes, or failure statements. Titles are secondary indexes, not the main body of the report.
19. If the repo already aggregated `world_entry_points`, report from that layer first. It exists to flatten fetch topology into object + pressure + evidence, so do not peel back to registry order in the final message.
20. If an outside sample only hands over a title shell and still cannot yield a pressure sentence after reading its summary/excerpt, drop it from the report. Do not let catalog noise pretend to be observation.
21. Heat notes may amplify a pressure sentence, but they must not stand alone as the reason to act. “xxx 赞 / xxx 评” is not a next step.
22. If the next public move is still pending, report the object or failure chain that justifies it, not only the unfinished title.
23. If there is no usable object-level pressure yet, fall back to a generic “公开判断/公开动作” phrasing. Do not backslide into `理论帖` / `技术帖` / `group-post` lane labels in the final message.
24. Notification counts, queues, and snapshot fields are operating pressure, not topics by themselves. They only deserve public language after they are tied to a failure chain, outside sample, or institutional conflict.
25. The shared interactive card is no longer the realtime transcript. It usually shows a lightweight status card, but when plan mode finishes it is patched into a complete-plan card with `执行计划` / `继续规划` actions.
26. When Codex sends `request_user_input`, the gateway turns it into a Feishu question card and keeps a text-reply fallback. If `card.action.trigger` is subscribed, the user can answer by pressing buttons; otherwise they reply in text.
27. Clicking `执行计划` starts a fresh default-mode turn on the same Codex thread; clicking `继续规划` keeps the same thread in plan mode and waits for more planning input.
28. After `执行计划`, treat the confirmed plan as a reference frame, not a lockstep queue. Start from the strongest live pressure point; if the field changes, say so and rewrite the takeoff order instead of obeying stale choreography.
29. After the turn completes, the gateway patches the same card to the completed state and removes the earlier `Typing` reaction.

## Operational note

- Long connection mode is still the simplest way to receive message events locally.
- In the current Feishu console setup, `card.action.trigger` can also be received through long connection, so no public callback domain is required for button clicks.
- Feishu can patch application-sent interactive cards, but ordinary text messages are not patchable. Use text for the human-like running conversation and cards for status or structured choice such as plan completion.
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
