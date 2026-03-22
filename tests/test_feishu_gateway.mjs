import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

import {
  buildCodexPrompt,
  buildQuestionAnswerPayload,
  buildStatusCard,
  extractModeDirective,
  inboxEventMatchesIncomingEvent,
  listIncomingDedupKeys,
  normalizeCardActionPayload,
  shouldEnableCardCallbacks,
  shouldApplyTurnCompletionToSession,
  splitNaturalMessageChunks,
  supportsCardActions,
  tryMapTextToQuestionAnswer
} from "../skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs";

test("extractModeDirective recognizes explicit plan switch", () => {
  const result = extractModeDirective("切到 plan mode，帮我规划一下飞书重构");
  assert.equal(result.mode, "plan");
  assert.equal(result.remainder, "帮我规划一下飞书重构");
});

test("extractModeDirective recognizes explicit default switch", () => {
  const result = extractModeDirective("切回默认模式，直接实现");
  assert.equal(result.mode, "default");
  assert.equal(result.remainder, "直接实现");
});

test("splitNaturalMessageChunks emits complete sentences and keeps tail", () => {
  const result = splitNaturalMessageChunks("先看现状。再改实现", false);
  assert.deepEqual(result.chunks, ["先看现状。"]);
  assert.equal(result.remaining, "再改实现");
});

test("splitNaturalMessageChunks merges punctuation-only deltas into previous chunk", () => {
  const result = splitNaturalMessageChunks("现在查重复投递后的幂等处理，看是不是应用侧少了一层拦截\n。看到一处信号了", false);
  assert.deepEqual(result.chunks, ["现在查重复投递后的幂等处理，看是不是应用侧少了一层拦截。"]);
  assert.equal(result.remaining, "看到一处信号了");
});

test("splitNaturalMessageChunks keeps a trailing newline sentence buffered until punctuation arrives", () => {
  const result = splitNaturalMessageChunks("然后看当前会话有没有被误推进\n", false);
  assert.deepEqual(result.chunks, []);
  assert.equal(result.remaining, "然后看当前会话有没有被误推进\n");
});

test("splitNaturalMessageChunks folds delayed punctuation onto the previous newline sentence", () => {
  const result = splitNaturalMessageChunks("然后看当前会话有没有被误推进\n。", false);
  assert.deepEqual(result.chunks, ["然后看当前会话有没有被误推进。"]);
  assert.equal(result.remaining, "");
});

test("splitNaturalMessageChunks merges forced punctuation tails into previous chunk", () => {
  const result = splitNaturalMessageChunks("再看一眼接线位置\n。", true);
  assert.deepEqual(result.chunks, ["再看一眼接线位置。"]);
  assert.equal(result.remaining, "");
});

test("buildQuestionAnswerPayload maps answered question ids", () => {
  const payload = buildQuestionAnswerPayload(
    [{ id: "mode" }, { id: "thread" }],
    {
      mode: { answers: ["普通模式(Recommended)"] },
      thread: { answers: ["1小时切新线程(推荐)"] }
    }
  );
  assert.deepEqual(payload, {
    answers: {
      mode: { answers: ["普通模式(Recommended)"] },
      thread: { answers: ["1小时切新线程(推荐)"] }
    }
  });
});

test("tryMapTextToQuestionAnswer prefers matching option labels", () => {
  const answer = tryMapTextToQuestionAnswer(
    {
      id: "mode",
      options: [
        { label: "普通模式(推荐)", description: "default" },
        { label: "Plan 优先", description: "plan" }
      ]
    },
    "我选 Plan 优先"
  );
  assert.deepEqual(answer, { answers: ["Plan 优先"] });
});

test("buildStatusCard renders question buttons when card actions are enabled", () => {
  const card = buildStatusCard("停在岔路口等你拍板", {
    status: "waiting",
    chatId: "oc_test",
    requestId: "123",
    allowActions: true,
    questions: [
      {
        id: "mode",
        header: "默认模式",
        question: "飞书默认应该进哪种模式？",
        options: [
          { label: "普通模式(推荐)", description: "default" },
          { label: "Plan 优先", description: "plan" }
        ]
      }
    ]
  });
  assert.equal(card.header.title.content, "派蒙等待你的选择");
  const actionElement = card.elements.find((item) => item.tag === "action");
  assert.ok(actionElement);
  assert.equal(actionElement.actions.length, 2);
});

test("supportsCardActions only depends on callback enablement, not token or encrypt key", () => {
  const config = {
    feishu: {},
    automation: {
      feishu_card_callback_enabled: true
    }
  };
  assert.equal(shouldEnableCardCallbacks(config, {}), true);
  assert.equal(supportsCardActions(config, {}), true);
});

test("normalizeCardActionPayload accepts long-connection event wrapper", () => {
  const normalized = normalizeCardActionPayload({
    header: {
      tenant_key: "tenant-test"
    },
    event: {
      open_message_id: "om_123",
      open_id: "ou_123",
      token: "token_123",
      action: {
        tag: "button",
        value: {
          action: "request-user-input-answer",
          request_id: "req_1"
        }
      }
    }
  });
  assert.deepEqual(normalized, {
    open_id: "ou_123",
    user_id: "",
    tenant_key: "tenant-test",
    open_message_id: "om_123",
    token: "token_123",
    action: {
      tag: "button",
      value: {
        action: "request-user-input-answer",
        request_id: "req_1"
      }
    }
  });
});

test("shouldApplyTurnCompletionToSession ignores stale completions from older turns", () => {
  assert.equal(shouldApplyTurnCompletionToSession({ active_turn_id: "turn-new" }, "turn-old"), false);
  assert.equal(shouldApplyTurnCompletionToSession({ active_turn_id: "turn-old" }, "turn-old"), true);
  assert.equal(shouldApplyTurnCompletionToSession({ active_turn_id: "" }, "turn-old"), true);
});

test("buildCodexPrompt keeps the Feishu user wording consistent in exec fallback", () => {
  const prompt = buildCodexPrompt("oc_test", [], [], "- 无", "- 无");
  assert.match(prompt, /派蒙，你正在通过飞书和用户连续协作/);
  assert.doesNotMatch(prompt, /仓库主人/);
});

test("listIncomingDedupKeys includes both message id and realtime event id", () => {
  assert.deepEqual(
    listIncomingDedupKeys({
      message_id: "om_123",
      raw: {
        event_id: "evt_123"
      }
    }),
    ["om_123", "event:evt_123"]
  );
});

test("inboxEventMatchesIncomingEvent matches by event id when message ids drift", () => {
  assert.equal(
    inboxEventMatchesIncomingEvent(
      {
        message_id: "om_old",
        raw: {
          event_id: "evt_same"
        }
      },
      {
        message_id: "om_new",
        raw: {
          event_id: "evt_same"
        }
      }
    ),
    true
  );
});

test("status phrase asset contains a large rotating pool", () => {
  const raw = fs.readFileSync(
    new URL("../skills/paimon-instreet-autopilot/assets/feishu-status-phrases.json", import.meta.url),
    "utf8"
  );
  const phrases = JSON.parse(raw);
  const total = Object.values(phrases).reduce((sum, bucket) => sum + bucket.length, 0);
  assert.ok(total >= 90);
});
