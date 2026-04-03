import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

import {
  buildCodexPrompt,
  buildPlanExecutionInput,
  buildFeishuContextBlock,
  buildPlanCompletionCard,
  buildQuestionAnswerPayload,
  buildStatusCard,
  extractPlanTextFromItem,
  extractThreadResumeDirective,
  extractModeDirective,
  inboxEventMatchesIncomingEvent,
  isMissingRolloutThreadError,
  listIncomingDedupKeys,
  normalizeCardActionPayload,
  normalizePlanActionDecision,
  normalizePendingRequestId,
  resumeThreadWithFallback,
  shouldEnableCardCallbacks,
  shouldApplyTurnCompletionToSession,
  splitNaturalMessageChunks,
  supportsCardActions,
  tryMapTextToQuestionAnswer
} from "../skills/paimon-instreet-autopilot/scripts/feishu_gateway.mjs";

test("extractModeDirective recognizes explicit plan switch", () => {
  const result = extractModeDirective("切到 plan mode，帮我规划一下飞书重构");
  assert.equal(result.mode, "plan");
  assert.equal(result.newThread, false);
  assert.equal(result.remainder, "帮我规划一下飞书重构");
});

test("extractModeDirective recognizes slash commands for plan and default", () => {
  const plan = extractModeDirective("/plan 帮我规划飞书线程恢复");
  assert.equal(plan.mode, "plan");
  assert.equal(plan.newThread, false);
  assert.equal(plan.clearSession, false);
  assert.equal(plan.remainder, "帮我规划飞书线程恢复");

  const fallback = extractModeDirective("/default 直接开始修");
  assert.equal(fallback.mode, "default");
  assert.equal(fallback.newThread, false);
  assert.equal(fallback.clearSession, false);
  assert.equal(fallback.remainder, "直接开始修");
});

test("extractModeDirective recognizes clear-session command", () => {
  const result = extractModeDirective("/clear /default 重新开始");
  assert.equal(result.mode, "default");
  assert.equal(result.newThread, true);
  assert.equal(result.clearSession, true);
  assert.equal(result.remainder, "重新开始");
});

test("extractModeDirective recognizes explicit default switch", () => {
  const result = extractModeDirective("切回默认模式，直接实现");
  assert.equal(result.mode, "default");
  assert.equal(result.newThread, false);
  assert.equal(result.remainder, "直接实现");
});

test("extractModeDirective recognizes explicit new thread switch", () => {
  const result = extractModeDirective("开个新 thread，继续做飞书联调");
  assert.equal(result.mode, "");
  assert.equal(result.newThread, true);
  assert.equal(result.remainder, "继续做飞书联调");
});

test("extractModeDirective ignores Feishu prefixes before a new thread directive", () => {
  const result = extractModeDirective("【联调4】@_user_1 派蒙 开个新thread，只做飞书链路自检");
  assert.equal(result.mode, "");
  assert.equal(result.newThread, true);
  assert.equal(result.remainder, "只做飞书链路自检");
});

test("extractModeDirective supports combined new thread and mode directives", () => {
  const result = extractModeDirective("开个新thread，切到 plan mode，帮我规划飞书链路");
  assert.equal(result.mode, "plan");
  assert.equal(result.newThread, true);
  assert.equal(result.remainder, "帮我规划飞书链路");
});

test("extractModeDirective ignores Feishu prefixes before a mode directive", () => {
  const result = extractModeDirective("【联调5】@派蒙 切到 plan mode，帮我规划");
  assert.equal(result.mode, "plan");
  assert.equal(result.newThread, false);
  assert.equal(result.remainder, "帮我规划");
});

test("extractThreadResumeDirective recognizes explicit resume wording", () => {
  const result = extractThreadResumeDirective("【联调6】@_user_1 续上这个thread，继续修飞书 flush");
  assert.equal(result.resumeThread, true);
  assert.equal(result.remainder, "继续修飞书 flush");
});

test("normalizePlanActionDecision recognizes execute and continue replies", () => {
  assert.equal(normalizePlanActionDecision("执行计划"), "execute");
  assert.equal(normalizePlanActionDecision("继续规划"), "continue");
  assert.equal(normalizePlanActionDecision("继续补细节"), "");
});

test("buildPlanExecutionInput starts from the strongest live pressure instead of a fixed sequence", () => {
  const text = buildPlanExecutionInput("1. 先做 A\n2. 再做 B");
  assert.match(text, /最强的压力点/);
  assert.doesNotMatch(text, /既定顺序/);
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

test("splitNaturalMessageChunks keeps list markers as separate newline chunks", () => {
  const result = splitNaturalMessageChunks("先看现状\n- 第一项\n- 第二项", true);
  assert.deepEqual(result.chunks, ["先看现状", "- 第一项", "- 第二项"]);
  assert.equal(result.remaining, "");
});

test("splitNaturalMessageChunks preserves paragraph breaks before lists", () => {
  const result = splitNaturalMessageChunks("先做概览\n\n1. 看 thread\n2. 看卡片", true);
  assert.deepEqual(result.chunks, ["先做概览", "1. 看 thread", "2. 看卡片"]);
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

test("normalizePendingRequestId keeps zero as a valid request id", () => {
  assert.equal(normalizePendingRequestId(0), "0");
  assert.equal(normalizePendingRequestId("0"), "0");
  assert.equal(normalizePendingRequestId(""), "");
  assert.equal(normalizePendingRequestId(null), "");
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
  assert.equal(card.header.title.content, "派蒙等你拍板中");
  const actionElement = card.elements.find((item) => item.tag === "action");
  assert.ok(actionElement);
  assert.equal(actionElement.actions.length, 2);
});

test("buildPlanCompletionCard renders complete-plan actions", () => {
  const card = buildPlanCompletionCard("1. 先修 thread 恢复\n2. 再补计划卡片", {
    actionButtons: [
      {
        label: "执行计划",
        value: {
          action: "plan-completion",
          chat_id: "oc_test",
          decision: "execute"
        }
      },
      {
        label: "继续规划",
        value: {
          action: "plan-completion",
          chat_id: "oc_test",
          decision: "continue"
        }
      }
    ]
  });
  assert.equal(card.header.title.content, "派蒙把这份计划叠整齐啦");
  const actionElement = card.elements.find((item) => item.tag === "action");
  assert.ok(actionElement);
  assert.equal(actionElement.actions.length, 2);
});

test("extractPlanTextFromItem accepts direct and structured plan payloads", () => {
  assert.equal(
    extractPlanTextFromItem({ text: "完整计划正文" }),
    "完整计划正文"
  );
  assert.equal(
    extractPlanTextFromItem({
      plan: [
        { step: "先修 thread 恢复" },
        { step: "再修计划卡片" }
      ]
    }),
    "1. 先修 thread 恢复\n2. 再修计划卡片"
  );
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
  assert.match(prompt, /先把 AGENTS\.md 当最高记忆入口/);
  assert.match(prompt, /config\/paimon\.json 是运行配置，不是人格来源、选题理由或研究入口/);
  assert.match(prompt, /state\/current 下的实时状态/);
  assert.match(prompt, /不要在飞书回复里输出 Markdown 链接、文件路径、行号/);
  assert.doesNotMatch(prompt, /SOUL\.md/);
  assert.doesNotMatch(prompt, /派蒙是仓库的主人之一/);
  assert.doesNotMatch(prompt, /我先对齐内部上下文/);
  assert.doesNotMatch(prompt, /作为 AI 助手/);
  assert.doesNotMatch(prompt, /请先阅读本地 AGENTS\.md/);
  assert.doesNotMatch(prompt, /我再去看一眼/);
});

test("buildFeishuContextBlock keeps identity alignment internal", () => {
  const prompt = buildFeishuContextBlock({
    chatId: "oc_test",
    messageText: "继续做飞书联调",
    session: null,
    liveProbeSummary: "- 无",
    memorySnapshot: "- 无",
    event: null
  });
  assert.match(prompt, /工作中多发短句自然语言更新/);
  assert.match(prompt, /当前会话模式：default/);
  assert.doesNotMatch(prompt, /AGENTS\.md/);
  assert.doesNotMatch(prompt, /SOUL\.md/);
  assert.doesNotMatch(prompt, /静默遵循/);
  assert.doesNotMatch(prompt, /先对齐内部上下文/);
  assert.doesNotMatch(prompt, /未完成语气/);
  assert.doesNotMatch(prompt, /请先对齐本地/);
});

test("buildFeishuContextBlock includes resume fallback notice when needed", () => {
  const prompt = buildFeishuContextBlock({
    chatId: "oc_test",
    messageText: "续上这个 thread，继续联调",
    session: null,
    liveProbeSummary: "- 无",
    memorySnapshot: "- 无",
    event: null,
    resumeFallbackNotice: "旧 thread 已经被归档了，这一轮需要先恢复再继续。"
  });
  assert.match(prompt, /恢复说明：旧 thread 已经被归档了，这一轮需要先恢复再继续。/);
});

test("isMissingRolloutThreadError recognizes archived-thread resume failures", () => {
  assert.equal(
    isMissingRolloutThreadError({ code: -32600, message: "no rollout found for thread id 019d1764-179b-7ad0-97f5-d9d28a4aba1f" }),
    true
  );
  assert.equal(
    isMissingRolloutThreadError({ code: -32000, message: "no rollout found for thread id 019d1764-179b-7ad0-97f5-d9d28a4aba1f" }),
    false
  );
  assert.equal(
    isMissingRolloutThreadError({ code: -32600, message: "permission denied" }),
    false
  );
});

test("resumeThreadWithFallback unarchives and resumes an archived thread before falling back", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/resume" && calls.filter((entry) => entry.method === "thread/resume").length === 1) {
        throw { code: -32600, message: "no rollout found for thread id thread-old" };
      }
      if (method === "thread/unarchive") {
        return { thread: { id: params.threadId } };
      }
      if (method === "thread/resume") {
        return { thread: { id: params.threadId } };
      }
      throw new Error(`unexpected method: ${method}`);
    }
  };

  const result = await resumeThreadWithFallback(client, "thread-old", { automation: { codex_model: "gpt-test" } });
  assert.deepEqual(result, {
    threadId: "thread-old",
    startingNewThread: false,
    fallbackFromThreadId: "",
    recoveredFromArchive: true,
    recoveryError: "{\"code\":-32600,\"message\":\"no rollout found for thread id thread-old\"}",
    unarchiveError: ""
  });
  assert.deepEqual(
    calls.map((entry) => entry.method),
    ["thread/resume", "thread/unarchive", "thread/resume"]
  );
});

test("resumeThreadWithFallback falls back to a new thread only after unarchive recovery fails", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/resume") {
        throw { code: -32600, message: "no rollout found for thread id thread-old" };
      }
      if (method === "thread/unarchive") {
        throw new Error("thread missing");
      }
      if (method === "thread/start") {
        return { thread: { id: "thread-new" } };
      }
      throw new Error(`unexpected method: ${method}`);
    }
  };

  const result = await resumeThreadWithFallback(client, "thread-old", { automation: { codex_model: "gpt-test" } });
  assert.equal(result.threadId, "thread-new");
  assert.equal(result.startingNewThread, true);
  assert.equal(result.fallbackFromThreadId, "thread-old");
  assert.equal(result.recoveredFromArchive, false);
  assert.match(result.recoveryError, /no rollout found/);
  assert.match(result.unarchiveError, /thread missing/);
  assert.deepEqual(
    calls.map((entry) => entry.method),
    ["thread/resume", "thread/unarchive", "thread/start"]
  );
});

test("resumeThreadWithFallback rethrows unrelated resume failures", async () => {
  const client = {
    async request(method) {
      if (method === "thread/resume") {
        throw new Error("permission denied");
      }
      throw new Error(`unexpected method: ${method}`);
    }
  };

  await assert.rejects(
    () => resumeThreadWithFallback(client, "thread-old", { automation: { codex_model: "gpt-test" } }),
    /permission denied/
  );
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
