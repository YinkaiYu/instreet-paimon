#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as Lark from "@larksuiteoapi/node-sdk";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../../..");
const configPath = path.join(repoRoot, "config", "paimon.json");
const stateCurrentDir = path.join(repoRoot, "state", "current");
const tmpDir = path.join(repoRoot, "tmp");
const snapshotScriptPath = path.join(__dirname, "snapshot.py");
const accountOverviewPath = path.join(stateCurrentDir, "account_overview.json");
const literaryDetailsPath = path.join(stateCurrentDir, "literary_details.json");
const fetchFailuresPath = path.join(stateCurrentDir, "fetch_failures.json");
const reactionsPath = path.join(stateCurrentDir, "feishu_reactions.json");
const inboxPath = path.join(stateCurrentDir, "feishu_inbox.jsonl");
const errorsPath = path.join(stateCurrentDir, "feishu_gateway_errors.jsonl");
const eventsPath = path.join(stateCurrentDir, "feishu_events.jsonl");
const seenMessagesPath = path.join(stateCurrentDir, "feishu_seen_messages.json");
const queuePath = path.join(stateCurrentDir, "feishu_queue.json");
const batchesPath = path.join(stateCurrentDir, "feishu_batches.jsonl");
const chatTimers = new Map();
const processingChats = new Set();
let queueSweepTimer = null;
let cachedCodexExecutable = null;

function ensureDirs() {
  for (const dir of [stateCurrentDir, tmpDir]) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function readConfig() {
  return JSON.parse(fs.readFileSync(configPath, "utf8"));
}

function appendJsonl(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, `${JSON.stringify(payload)}\n`);
}

function readJsonFile(filePath, fallback) {
  if (!fs.existsSync(filePath)) {
    return fallback;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJsonFile(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
}

function readSeenMessages() {
  if (!fs.existsSync(seenMessagesPath)) {
    return {};
  }
  return JSON.parse(fs.readFileSync(seenMessagesPath, "utf8"));
}

function writeSeenMessages(data) {
  fs.mkdirSync(path.dirname(seenMessagesPath), { recursive: true });
  fs.writeFileSync(seenMessagesPath, `${JSON.stringify(data, null, 2)}\n`);
}

function readReactionState() {
  return readJsonFile(reactionsPath, { messages: {} });
}

function writeReactionState(data) {
  writeJsonFile(reactionsPath, data);
}

function rememberReaction(messageId, payload) {
  if (!messageId || !payload?.reaction_id) {
    return;
  }
  const state = readReactionState();
  state.messages[messageId] = payload;
  writeReactionState(state);
}

function consumeReaction(messageId) {
  if (!messageId) {
    return null;
  }
  const state = readReactionState();
  const entry = state.messages[messageId] || null;
  if (!entry) {
    return null;
  }
  delete state.messages[messageId];
  writeReactionState(state);
  return entry;
}

function readQueue() {
  return readJsonFile(queuePath, { version: 1, chats: {} });
}

function writeQueue(queue) {
  writeJsonFile(queuePath, queue);
}

function ensureChatQueue(queue, chatId) {
  if (!queue.chats[chatId]) {
    queue.chats[chatId] = {
      pending: [],
      processing: null,
      updated_at: new Date().toISOString()
    };
  }
  return queue.chats[chatId];
}

function readChatProcessing(chatId) {
  const queue = readQueue();
  return queue.chats?.[chatId]?.processing || null;
}

function updateChatProcessing(chatId, updater) {
  const queue = readQueue();
  const chat = ensureChatQueue(queue, chatId);
  if (!chat.processing) {
    return null;
  }
  const patch = typeof updater === "function" ? updater(chat.processing) : updater;
  if (patch && typeof patch === "object") {
    Object.assign(chat.processing, patch);
    chat.updated_at = new Date().toISOString();
    writeQueue(queue);
  }
  return chat.processing;
}

function parseArgs(argv) {
  const [command = "help", ...rest] = argv.slice(2);
  const flags = {};
  for (let i = 0; i < rest.length; i += 1) {
    const key = rest[i];
    if (!key.startsWith("--")) {
      continue;
    }
    const name = key.slice(2);
    const next = rest[i + 1];
    if (!next || next.startsWith("--")) {
      flags[name] = true;
      continue;
    }
    flags[name] = next;
    i += 1;
  }
  return { command, flags };
}

async function fetchTenantToken(config) {
  const response = await fetch("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      app_id: config.feishu.app_id,
      app_secret: config.feishu.app_secret
    })
  });
  const body = await response.json();
  if (!response.ok || body.code !== 0) {
    throw new Error(`tenant token request failed: ${JSON.stringify(body)}`);
  }
  return body;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function feishuApiRequest(config, endpoint, options = {}, retries = null, retryDelayMs = null, flags = {}) {
  const maxRetries = retries ?? Number(config.automation?.feishu_send_retries || 4);
  const baseDelayMs = retryDelayMs ?? Number(config.automation?.feishu_send_retry_delay_ms || 1500);
  let lastError = null;
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const auth = await fetchTenantToken(config);
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), getHttpTimeoutMs(config, flags));
      const response = await fetch(`https://open.feishu.cn${endpoint}`, {
        ...options,
        signal: controller.signal,
        headers: {
          Authorization: `Bearer ${auth.tenant_access_token}`,
          "Content-Type": "application/json",
          ...(options.headers || {})
        }
      });
      clearTimeout(timeout);
      const body = await response.json();
      if (!response.ok || body.code !== 0) {
        throw new Error(`feishu api failed: HTTP ${response.status} ${JSON.stringify(body)}`);
      }
      return body;
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) {
        break;
      }
      await sleep(baseDelayMs * attempt);
    }
  }
  throw lastError;
}

async function sendTextMessage(config, receiveIdType, receiveId, text, flags = {}) {
  return feishuApiRequest(
    config,
    `/open-apis/im/v1/messages?receive_id_type=${encodeURIComponent(receiveIdType)}`,
    {
      method: "POST",
      body: JSON.stringify({
        receive_id: receiveId,
        content: JSON.stringify({ text }),
        msg_type: "text"
      })
    },
    null,
    null,
    flags
  );
}

function clampCardText(text, limit = 12000) {
  if (!text || text.length <= limit) {
    return text || "";
  }
  return `${text.slice(0, limit - 24)}\n\n_内容过长，已截断显示。_`;
}

function buildStatusCard(text, options = {}) {
  const status = options.status || "working";
  const mergedCount = options.mergedCount || 1;
  const title =
    status === "done"
      ? "派蒙回复完成"
      : status === "error"
        ? "派蒙处理异常"
        : "派蒙正在处理";
  const template =
    status === "done"
      ? "green"
      : status === "error"
        ? "red"
        : "blue";
  const noteParts = [
    status === "done" ? "状态：已完成" : status === "error" ? "状态：异常" : "状态：处理中",
    `合并消息：${mergedCount} 条`,
    `更新时间：${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`
  ];
  return {
    config: {
      wide_screen_mode: true,
      update_multi: true
    },
    header: {
      template,
      title: {
        tag: "plain_text",
        content: title
      }
    },
    elements: [
      {
        tag: "markdown",
        content: clampCardText(text)
      },
      {
        tag: "note",
        elements: [
          {
            tag: "plain_text",
            content: noteParts.join(" | ")
          }
        ]
      }
    ]
  };
}

async function sendCardMessage(config, receiveIdType, receiveId, card, flags = {}) {
  return feishuApiRequest(
    config,
    `/open-apis/im/v1/messages?receive_id_type=${encodeURIComponent(receiveIdType)}`,
    {
      method: "POST",
      body: JSON.stringify({
        receive_id: receiveId,
        content: JSON.stringify(card),
        msg_type: "interactive"
      })
    },
    null,
    null,
    flags
  );
}

async function updateCardMessage(config, messageId, card, flags = {}) {
  return feishuApiRequest(
    config,
    `/open-apis/im/v1/messages/${encodeURIComponent(messageId)}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        content: JSON.stringify(card)
      })
    },
    null,
    null,
    flags
  );
}

async function sendMessageReaction(config, messageId, emojiType, flags = {}) {
  return feishuApiRequest(
    config,
    `/open-apis/im/v1/messages/${encodeURIComponent(messageId)}/reactions`,
    {
      method: "POST",
      body: JSON.stringify({
        reaction_type: {
          emoji_type: emojiType
        }
      })
    },
    null,
    null,
    flags
  );
}

async function deleteMessageReaction(config, messageId, reactionId, flags = {}) {
  return feishuApiRequest(
    config,
    `/open-apis/im/v1/messages/${encodeURIComponent(messageId)}/reactions/${encodeURIComponent(reactionId)}`,
    {
      method: "DELETE"
    },
    null,
    null,
    flags
  );
}

async function fetchMessagesPage(config, chatId, pageSize = 50, pageToken = "", flags = {}) {
  const url = new URL("https://open.feishu.cn/open-apis/im/v1/messages");
  url.searchParams.set("container_id_type", "chat");
  url.searchParams.set("container_id", chatId);
  url.searchParams.set("page_size", String(pageSize));
  if (pageToken) {
    url.searchParams.set("page_token", pageToken);
  }
  return feishuApiRequest(config, `${url.pathname}${url.search}`, {
    method: "GET"
  }, null, null, flags);
}

function extractMessageEvent(data) {
  const root = data.event ?? data;
  const message = root.message ?? {};
  const sender = root.sender ?? {};
  let parsedContent = {};
  try {
    parsedContent = JSON.parse(message.content || "{}");
  } catch {
    parsedContent = { raw: message.content || "" };
  }
  return {
    received_at: new Date().toISOString(),
    message_id: message.message_id,
    chat_id: message.chat_id,
    message_type: message.message_type,
    text: parsedContent.text || parsedContent.raw || "",
    sender: sender.sender_id ?? {},
    raw: root
  };
}

function normalizeMessageItem(item) {
  let parsedContent = {};
  try {
    parsedContent = JSON.parse(item?.body?.content || "{}");
  } catch {
    parsedContent = { raw: item?.body?.content || "" };
  }
  return {
    received_at: new Date().toISOString(),
    source: "history-sync",
    message_id: item.message_id,
    chat_id: item.chat_id,
    message_type: item.msg_type,
    text: parsedContent.text || parsedContent.raw || "",
    sender: item.sender || {},
    raw: item
  };
}

function readInboxLines(chatId, limit = 12) {
  if (!fs.existsSync(inboxPath)) {
    return [];
  }
  const lines = fs.readFileSync(inboxPath, "utf8").trim().split("\n").filter(Boolean);
  const items = [];
  for (const line of lines) {
    try {
      const payload = JSON.parse(line);
      if (payload.chat_id === chatId) {
        items.push(payload);
      }
    } catch {
      // ignore malformed lines
    }
  }
  if (limit === null || limit === undefined) {
    return items;
  }
  return items.slice(-limit);
}

function dedupeMessages(items) {
  const seen = new Set();
  const deduped = [];
  for (const item of items) {
    if (!item?.message_id || seen.has(item.message_id)) {
      continue;
    }
    seen.add(item.message_id);
    deduped.push(item);
  }
  return deduped;
}

function truncateText(text, limit = 220) {
  const source = typeof text === "string" ? text : JSON.stringify(text, null, 0);
  if (source.length <= limit) {
    return source;
  }
  return `${source.slice(0, limit - 3)}...`;
}

function summarizeWorkDetail(detail, limit = 6) {
  const work = detail?.data?.work || {};
  const chapters = Array.isArray(detail?.data?.chapters) ? detail.data.chapters : [];
  const chapterSummary = chapters
    .slice(0, limit)
    .map((chapter) => `${chapter.chapter_number || "?"}:${chapter.title || "未命名章节"}`)
    .join(" | ");
  return {
    title: work.title || "未命名作品",
    chapterCount: work.chapter_count ?? chapters.length,
    updatedAt: work.updated_at || "",
    chapterSummary
  };
}

function buildProgressStatusReply(batchMessages, stage) {
  const mergedCount = batchMessages.length;
  if (stage === "initial") {
    return `派蒙正在处理这轮消息（已合并 ${mergedCount} 条），先同步实时快照和最近对话。`;
  }
  if (stage === "codex") {
    return `派蒙正在处理这轮消息（已合并 ${mergedCount} 条），实时快照已同步，正在运行 Codex 整理回复。`;
  }
  return "派蒙正在处理这轮消息。";
}

function getCodexTimeoutMs(config, flags) {
  return Number(flags["codex-timeout-ms"] || config.automation?.feishu_codex_timeout_ms || 1200000);
}

function getHttpTimeoutMs(config, flags) {
  return Number(flags["http-timeout-ms"] || config.automation?.feishu_http_timeout_ms || 8000);
}

function getProgressPingMs(config, flags) {
  return Number(flags["progress-ping-ms"] || config.automation?.feishu_progress_ping_ms || 300000);
}

function getReactionEmojiType(config, flags) {
  return String(flags["reaction-emoji"] || config.automation?.feishu_reaction_emoji_type || "Typing");
}

function isReactionEnabled(config, flags) {
  if (flags["no-reaction"]) {
    return false;
  }
  return config.automation?.feishu_reaction_enabled !== false;
}

function getCodexCommand(config, flags) {
  return String(flags["codex-command"] || config.automation?.feishu_codex_command || "").trim();
}

function getConfiguredCodexExecutable(config, flags) {
  return String(
    flags["codex-bin"] ||
    process.env.PAIMON_CODEX_BIN ||
    config.automation?.feishu_codex_bin ||
    ""
  ).trim();
}

function shouldBypassCodexSandbox(config, flags) {
  if (flags["no-codex-bypass-sandbox"]) {
    return false;
  }
  if (flags["codex-bypass-sandbox"]) {
    return true;
  }
  return config.automation?.feishu_codex_bypass_sandbox !== false;
}

function isExecutableFile(filePath) {
  if (!filePath) {
    return false;
  }
  try {
    fs.accessSync(filePath, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function resolveCodexExecutable(config, flags) {
  if (cachedCodexExecutable) {
    return cachedCodexExecutable;
  }

  const explicit = getConfiguredCodexExecutable(config, flags);
  if (explicit) {
    cachedCodexExecutable = explicit;
    return cachedCodexExecutable;
  }

  const candidateNames = ["codex"];
  for (const name of candidateNames) {
    const local = spawnSync(name, ["--version"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"]
    });
    if (!local.error) {
      cachedCodexExecutable = name;
      return cachedCodexExecutable;
    }
  }

  const loginShellLookup = spawnSync("/bin/bash", ["-lc", "command -v codex"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      HOME: process.env.HOME || `/home/${process.env.USER || "yyk"}`
    }
  });
  const shellPath = (loginShellLookup.stdout || "").trim().split("\n")[0] || "";
  if (isExecutableFile(shellPath)) {
    cachedCodexExecutable = shellPath;
    return cachedCodexExecutable;
  }

  const homeDir = process.env.HOME || `/home/${process.env.USER || "yyk"}`;
  const commonCandidates = [
    path.join(homeDir, ".nvm", "versions", "node", "v22.19.0", "bin", "codex"),
    path.join(homeDir, ".nvm", "versions", "node", "current", "bin", "codex"),
    path.join(homeDir, ".local", "bin", "codex"),
    "/usr/local/bin/codex",
    "/usr/bin/codex"
  ];
  for (const candidate of commonCandidates) {
    if (isExecutableFile(candidate)) {
      cachedCodexExecutable = candidate;
      return cachedCodexExecutable;
    }
  }

  throw new Error(`codex executable not found; PATH=${process.env.PATH || ""}`);
}

function formatHistoryLine(item) {
  const senderType = item.sender?.sender_type || item.sender?.id_type || "unknown";
  const who = senderType === "user" ? "用户" : senderType === "app" ? "派蒙" : senderType;
  const content = truncateText(item.text || "", 180);
  return `- ${who}: ${content}`;
}

function buildLiveProbeSummary(snapshotResult, overview, literaryDetails) {
  const lines = [];
  if (snapshotResult?.skipped) {
    lines.push("- 实时快照: 已跳过");
  } else if (snapshotResult?.success) {
    lines.push(`- 实时快照: 成功，${truncateText(snapshotResult.stdout || "snapshot ok", 180)}`);
  } else if (snapshotResult?.attempted) {
    lines.push(`- 实时快照: 失败，${truncateText(snapshotResult.error || snapshotResult.stderr || "unknown error", 180)}`);
  }

  if (overview?.captured_at || overview?.score !== undefined) {
    const metrics = [];
    if (overview?.captured_at) {
      metrics.push(`captured_at=${overview.captured_at}`);
    }
    if (overview?.score !== undefined) {
      metrics.push(`score=${overview.score}`);
    }
    if (overview?.unread_notification_count !== undefined) {
      metrics.push(`unread_notifications=${overview.unread_notification_count}`);
    }
    if (overview?.unread_message_count !== undefined) {
      metrics.push(`unread_messages=${overview.unread_message_count}`);
    }
    lines.push(`- 账号概览: ${metrics.join(" | ")}`);
  }

  const detailMap = literaryDetails?.details || {};
  const detailEntries = Object.values(detailMap);
  for (const detail of detailEntries.slice(0, 3)) {
    const summary = summarizeWorkDetail(detail);
    lines.push(
      `- 文学社作品: ${summary.title} | chapters=${summary.chapterCount} | updated_at=${summary.updatedAt || "unknown"}`
    );
    if (summary.chapterSummary) {
      lines.push(`  章节索引: ${summary.chapterSummary}`);
    }
  }
  const fetchFailures = readJsonFile(fetchFailuresPath, {}).data || [];
  if (Array.isArray(fetchFailures) && fetchFailures.length) {
    for (const failure of fetchFailures.slice(0, 5)) {
      const endpoint = failure?.endpoint || "unknown";
      const status = failure?.status ? ` status=${failure.status}` : "";
      const message = truncateText(failure?.message || JSON.stringify(failure?.body || {}), 180);
      lines.push(`- 快照降级: ${endpoint}${status} ${message}`.trim());
    }
  }
  return lines.join("\n") || "- 无";
}

async function refreshLiveSnapshot(config, flags) {
  if (!shouldRefreshLiveSnapshot(config, flags)) {
    return { attempted: false, skipped: true, success: false };
  }

  return new Promise((resolve) => {
    let settled = false;
    let stdout = "";
    let stderr = "";
    const child = spawn("python3", [snapshotScriptPath, "--post-limit", "12", "--feed-limit", "8"], {
      cwd: repoRoot,
      env: {
        ...process.env,
        PYTHONUNBUFFERED: "1"
      },
      stdio: ["ignore", "pipe", "pipe"]
    });

    const timeout = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), 2000);
      resolve({
        attempted: true,
        success: false,
        error: `snapshot timeout after ${getSnapshotTimeoutMs(config, flags)}ms`,
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    }, getSnapshotTimeoutMs(config, flags));

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      resolve({
        attempted: true,
        success: false,
        error: String(error),
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    });
    child.on("close", (code) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      resolve({
        attempted: true,
        success: code === 0,
        exitCode: code,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
        error: code === 0 ? "" : `snapshot exited with code ${code}`
      });
    });
  });
}

async function gatherLiveProbe(config, flags, chatId, batchMessages) {
  const snapshotResult = await refreshLiveSnapshot(config, flags);
  if (snapshotResult?.attempted && !snapshotResult.success) {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "live-snapshot-failed",
      chat_id: chatId,
      message_ids: batchMessages.map((item) => item.message_id),
      error: snapshotResult.error || snapshotResult.stderr || "unknown error"
    });
  }
  const overview = readJsonFile(accountOverviewPath, {});
  const literaryDetails = readJsonFile(literaryDetailsPath, {});
  return {
    snapshotResult,
    overview,
    literaryDetails,
    summary: buildLiveProbeSummary(snapshotResult, overview, literaryDetails)
  };
}

async function clearTypingReactions(config, messageItems, flags = {}) {
  for (const item of messageItems) {
    const messageId = item?.message_id;
    const reaction = consumeReaction(messageId);
    if (!reaction?.reaction_id) {
      continue;
    }
    try {
      await deleteMessageReaction(config, messageId, reaction.reaction_id, flags);
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "im.message.reaction.delete",
        chat_id: item?.chat_id || reaction.chat_id || "",
        message_id: messageId,
        emoji_type: reaction.emoji_type || "",
        reaction_id: reaction.reaction_id
      });
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "message-reaction-delete-failed",
        chat_id: item?.chat_id || reaction.chat_id || "",
        message_id: messageId,
        emoji_type: reaction.emoji_type || "",
        reaction_id: reaction.reaction_id,
        error: String(error)
      });
    }
  }
}

async function upsertProgressMessage(config, chatId, batchMessages, text, flags = {}) {
  const card = buildStatusCard(text, { status: "working", mergedCount: batchMessages.length });
  const existingMessageId = readChatProcessing(chatId)?.progress_message_id || "";
  if (existingMessageId) {
    try {
      await updateCardMessage(config, existingMessageId, card, flags);
      updateChatProcessing(chatId, {
        progress_updated_at: new Date().toISOString(),
        progress_text_preview: truncateText(text, 180)
      });
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "progress-message.update",
        chat_id: chatId,
        message_id: existingMessageId,
        text
      });
      return { messageId: existingMessageId, mode: "update" };
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "progress-message-update-failed",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        message_id: existingMessageId,
        error: String(error)
      });
    }
  }

  const response = await sendCardMessage(config, "chat_id", chatId, card, flags);
  const messageId = response?.data?.message_id || "";
  updateChatProcessing(chatId, {
    progress_message_id: messageId,
    progress_updated_at: new Date().toISOString(),
    progress_text_preview: truncateText(text, 180)
  });
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: "progress-message.create",
    chat_id: chatId,
    message_ids: batchMessages.map((item) => item.message_id),
    message_id: messageId,
    text
  });
  return { messageId, mode: "create" };
}

async function deliverFinalReply(config, chatId, batchMessages, replyText, flags = {}) {
  const progressMessageId = readChatProcessing(chatId)?.progress_message_id || "";
  const finalCard = buildStatusCard(replyText, { status: "done", mergedCount: batchMessages.length });
  if (progressMessageId) {
    try {
      await updateCardMessage(config, progressMessageId, finalCard, flags);
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "final-reply.patch",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        message_id: progressMessageId
      });
      return { messageId: progressMessageId, mode: "patch" };
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "final-reply-patch-failed",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        message_id: progressMessageId,
        error: String(error)
      });
    }
  }

  const response = await sendCardMessage(config, "chat_id", chatId, finalCard, flags);
  const messageId = response?.data?.message_id || "";
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: "final-reply.send",
    chat_id: chatId,
    message_ids: batchMessages.map((item) => item.message_id),
    message_id: messageId
  });
  return { messageId, mode: "send" };
}

function buildCodexPrompt(chatId, batchMessages, historyMessages, inboxMessages, liveProbeSummary) {
  const historyLines = historyMessages.map(formatHistoryLine).join("\n");
  const inboxLines = inboxMessages.map(formatHistoryLine).join("\n");
  const batchLines = batchMessages.map((item, index) => `${index + 1}. ${item.text}`).join("\n");
  return [
    "你是 InStreet 上的派蒙 paimon_insight。",
    "你正在通过飞书与仓库主人沟通。请先阅读本地 AGENTS.md 和记忆状态，再回复。",
    "把 AGENTS.md、config/paimon.json 和 state/current 下的最新状态视为主记忆来源。",
    "忽略 tmp/、旧回复缓存、旧批次日志、历史实验残留，除非用户这轮明确重新提出。",
    "这不是逐条客服对话，而是一个持续工作会话。",
    "如果用户在短时间内连续发来多条消息，请把它们理解为同一轮请求的补充信息，统一回复。",
    "只输出飞书回复正文，不要标题，不要引号，不要解释你如何生成。",
    "回复要求：简洁但有信息量，优先回应最新问题，同时吸收前面消息里的补充约束。",
    "",
    `当前 chat_id: ${chatId}`,
    "",
    "最近飞书历史：",
    historyLines || "- 无",
    "",
    "本地 inbox 近况：",
    inboxLines || "- 无",
    "",
    "实时平台探针：",
    liveProbeSummary || "- 无",
    "",
    "本轮合并待处理消息：",
    batchLines || "- 无",
    "",
    "如果用户是在追问正在执行的工作，请明确说清当前结果、卡点和下一步。",
    "如果用户只是连续发了几个补充短句，要把它们整合成一次自然回复。",
    "除非上面的实时平台探针明确失败，或你在这轮执行里亲自再次验证失败，否则不要声称 InStreet API / 当前环境不可访问。",
    "如果实时探针里出现“快照降级”，那表示单个接口拉取失败但整轮快照仍然成功，不要把它表述成平台整体不可用。",
    "如果只是本地快照缺字段，要明确说“本地未落盘/未缓存”，不要把它说成“线上不可访问”。",
    "每一轮和用户的交互，原则上都要带来至少一次社区公开动作。优先顺序是：回复评论 > 发布主帖 > 发布文学社章节 > 在他人帖子下高质量评论。"
  ].join("\n");
}

function runCodexPrompt(prompt, config, flags, hooks = {}) {
  ensureDirs();
  const outputFile = path.join(tmpDir, `feishu-reply-${Date.now()}.txt`);
  return new Promise((resolve, reject) => {
    let settled = false;
    const customCommand = getCodexCommand(config, flags);
    const child = customCommand
      ? spawn("bash", ["-lc", customCommand], {
        stdio: ["pipe", "inherit", "inherit"],
        cwd: repoRoot,
        env: {
          ...process.env,
          PAIMON_FEISHU_OUTPUT_FILE: outputFile,
          PAIMON_REPO_ROOT: repoRoot
        }
      })
      : spawn(
        resolveCodexExecutable(config, flags),
        [
          ...(shouldBypassCodexSandbox(config, flags) ? ["--dangerously-bypass-approvals-and-sandbox"] : []),
          "exec",
          "-C",
          repoRoot,
          "--skip-git-repo-check",
          "--color",
          "never",
          "-o",
          outputFile,
          "-"
        ],
        {
          stdio: ["pipe", "inherit", "inherit"]
        }
      );
    child.stdin.end(prompt);

    const cleanup = () => {
      clearTimeout(timeout);
      clearTimeout(progressTimer);
    };

    const timeout = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), 2000);
      reject(new Error(`codex timeout after ${getCodexTimeoutMs(config, flags)}ms`));
    }, getCodexTimeoutMs(config, flags));

    const progressTimer = setTimeout(() => {
      if (settled || typeof hooks.onLongRunning !== "function") {
        return;
      }
      Promise.resolve(hooks.onLongRunning()).catch((error) => {
        if (typeof hooks.onLongRunningError === "function") {
          hooks.onLongRunningError(error);
        }
      });
    }, getProgressPingMs(config, flags));

    child.on("error", (error) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(error);
    });

    child.on("close", (code) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (code !== 0) {
        reject(new Error(`codex exited with code ${code}`));
        return;
      }
      const responseText = fs.readFileSync(outputFile, "utf8").trim();
      resolve(responseText);
    });
  });
}

function buildFallbackReply(batchMessages) {
  const latest = (batchMessages[batchMessages.length - 1]?.text || "").trim();
  if (/个人简历|自我介绍|简历|群里/.test(latest)) {
    return [
      "可以，先给你一版适合直接发群的简短介绍：",
      "",
      "大家好，我是派蒙，InStreet 上的 `paimon_insight`。",
      "我主要做两条线：一条是 AI 社会、社区意识形态和互动结构的研究；另一条是 Agent 工具链、自动运营、心跳机制和内容生产流程的实践。",
      "目前我在持续连载《AI社区意识形态分析》，也会写技术方法贴，目标是把社区观察和可复用方法都做深。",
      "如果后面群里有关于 Agent 运营、内容策略或自动化协作的话题，我可以继续补具体经验。"
    ].join("\n");
  }
  if (/测试/.test(latest)) {
    return "收到，飞书链路已经恢复。这条回复走的是稳定链路，后续我会继续按合并消息的方式统一回应。";
  }
  return `收到。这轮我先给出简短回复：${truncateText(latest, 120)}。如果你接着补充，我会继续按同一轮消息合并处理。`;
}

function buildCodexFailureReply(batchMessages, error) {
  const text = String(error || "");
  if (/timeout/i.test(text)) {
    return "这轮消息我已经收到了，但生成完整回复时运行超时。你可以继续补充要求，或者让我拆成更小的步骤来处理。";
  }
  return buildFallbackReply(batchMessages);
}

function buildLongRunningReply(batchMessages) {
  const mergedCount = batchMessages.length;
  return `派蒙还在处理这轮消息（已合并 ${mergedCount} 条），Codex 仍在正常运行。你先不用重复发，我会继续更新这条消息。`;
}

function getMergeWindowMs(config, flags) {
  return Number(flags["merge-window-ms"] || config.automation?.feishu_merge_window_ms || 15000);
}

function getHistoryLimit(config, flags) {
  return Number(flags["history-limit"] || config.automation?.feishu_history_limit || 12);
}

function getProcessingTimeoutMs(config, flags) {
  return Number(flags["process-timeout-ms"] || config.automation?.feishu_processing_timeout_ms || 1800000);
}

function getQueueSweepIntervalMs(config, flags) {
  return Number(flags["queue-sweep-ms"] || config.automation?.feishu_queue_sweep_interval_ms || 15000);
}

function getSnapshotTimeoutMs(config, flags) {
  return Number(flags["snapshot-timeout-ms"] || config.automation?.feishu_snapshot_timeout_ms || 45000);
}

function shouldRefreshLiveSnapshot(config, flags) {
  if (flags["skip-live-snapshot"]) {
    return false;
  }
  return config.automation?.feishu_live_snapshot_enabled !== false;
}

function isSyntheticItem(item) {
  return item?.source === "synthetic-test";
}

function isSyntheticProcessing(processing) {
  const explicitItems = Array.isArray(processing?.items) ? processing.items : [];
  return explicitItems.length > 0 && explicitItems.every((item) => isSyntheticItem(item));
}

function hasNonSyntheticPending(chat) {
  return Array.isArray(chat?.pending) && chat.pending.some((item) => !isSyntheticItem(item));
}

function isProcessingStale(processing, config, flags) {
  if (!processing?.started_at) {
    return true;
  }
  const startedAt = new Date(processing.started_at).getTime();
  if (Number.isNaN(startedAt)) {
    return true;
  }
  return Date.now() - startedAt >= getProcessingTimeoutMs(config, flags);
}

function recoverProcessingItems(chatId, processing) {
  const explicitItems = Array.isArray(processing?.items) ? processing.items : [];
  if (explicitItems.length) {
    return explicitItems;
  }
  const messageIds = new Set(processing?.message_ids || []);
  if (!messageIds.size) {
    return [];
  }
  const inboxItems = readInboxLines(chatId, null);
  return inboxItems.filter((item) => messageIds.has(item.message_id));
}

function restoreStaleProcessing(queue, chatId, config, flags) {
  const chat = ensureChatQueue(queue, chatId);
  if (!chat.processing) {
    return false;
  }

  const shouldDropSynthetic =
    isSyntheticProcessing(chat.processing) &&
    hasNonSyntheticPending(chat);

  if (!shouldDropSynthetic && !isProcessingStale(chat.processing, config, flags)) {
    return false;
  }

  const recovered = recoverProcessingItems(chatId, chat.processing);
  chat.pending = shouldDropSynthetic
    ? dedupeMessages(chat.pending)
    : dedupeMessages(recovered.concat(chat.pending));
  chat.processing = null;
  chat.updated_at = new Date().toISOString();
  if (shouldDropSynthetic) {
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "synthetic-processing-dropped",
      chat_id: chatId,
      dropped_message_ids: recovered.map((item) => item.message_id),
      pending_message_ids: chat.pending.map((item) => item.message_id)
    });
  }
  return true;
}

function enqueueForChat(event) {
  const queue = readQueue();
  const chat = ensureChatQueue(queue, event.chat_id);
  const exists = chat.pending.some((item) => item.message_id === event.message_id);
  const processing = chat.processing?.message_ids?.includes(event.message_id);
  if (!exists && !processing) {
    chat.pending.push({
      message_id: event.message_id,
      chat_id: event.chat_id,
      text: event.text,
      received_at: event.received_at,
      source: event.source || "realtime"
    });
    chat.updated_at = new Date().toISOString();
    writeQueue(queue);
  }
}

function scheduleChatProcessing(chatId, config, flags, delayMs = null) {
  const waitMs = delayMs ?? getMergeWindowMs(config, flags);
  if (chatTimers.has(chatId)) {
    clearTimeout(chatTimers.get(chatId));
  }
  chatTimers.set(
    chatId,
    setTimeout(() => {
      chatTimers.delete(chatId);
      processChatQueue(chatId, config, flags).catch((error) => {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "process-chat-queue",
          chat_id: chatId,
          error: String(error)
        });
      });
    }, waitMs)
  );
}

async function processChatQueue(chatId, config, flags) {
  if (processingChats.has(chatId)) {
    return;
  }
  const queue = readQueue();
  const chat = ensureChatQueue(queue, chatId);
  if (chat.processing) {
    if (!restoreStaleProcessing(queue, chatId, config, flags)) {
      return;
    }
  }
  if (!chat.pending.length) {
    writeQueue(queue);
    return;
  }

  const batchMessages = chat.pending.slice();
  chat.pending = [];
  chat.processing = {
    started_at: new Date().toISOString(),
    message_ids: batchMessages.map((item) => item.message_id),
    items: batchMessages
  };
  chat.updated_at = new Date().toISOString();
  writeQueue(queue);
  processingChats.add(chatId);

  try {
    await upsertProgressMessage(
      config,
      chatId,
      batchMessages,
      buildProgressStatusReply(batchMessages, "initial"),
      flags
    );
    const liveProbe = await gatherLiveProbe(config, flags, chatId, batchMessages);
    let normalizedHistory = [];
    try {
      const historyItems = await fetchChatMessages(config, chatId, Math.max(getHistoryLimit(config, flags) * 2, 20), flags);
      normalizedHistory = historyItems
        .filter((item) => item.msg_type === "text")
        .slice(-getHistoryLimit(config, flags))
        .map(normalizeMessageItem);
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "history-fetch-failed",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        error: String(error)
      });
    }
    await upsertProgressMessage(
      config,
      chatId,
      batchMessages,
      buildProgressStatusReply(batchMessages, "codex"),
      flags
    );
    const inboxMessages = readInboxLines(chatId, getHistoryLimit(config, flags));
    const prompt = buildCodexPrompt(chatId, batchMessages, normalizedHistory, inboxMessages, liveProbe.summary);
    let replyText = "";
    try {
      replyText = await runCodexPrompt(prompt, config, flags, {
        onLongRunning: async () => {
          const waitText = buildLongRunningReply(batchMessages);
          await upsertProgressMessage(config, chatId, batchMessages, waitText, flags);
          appendJsonl(eventsPath, {
            timestamp: new Date().toISOString(),
            type: "codex-progress-ping",
            chat_id: chatId,
            message_ids: batchMessages.map((item) => item.message_id),
            text: waitText
          });
        },
        onLongRunningError: (error) => {
          appendJsonl(errorsPath, {
            timestamp: new Date().toISOString(),
            type: "codex-progress-ping-failed",
            chat_id: chatId,
            message_ids: batchMessages.map((item) => item.message_id),
            error: String(error)
          });
        }
      });
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "codex-reply-failed",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        error: String(error)
      });
      replyText = buildCodexFailureReply(batchMessages, error);
    }
    if (!replyText) {
      replyText = buildFallbackReply(batchMessages);
    }
    const delivered = await deliverFinalReply(config, chatId, batchMessages, replyText, flags);
    await clearTypingReactions(config, batchMessages, flags);
    appendJsonl(batchesPath, {
      timestamp: new Date().toISOString(),
      chat_id: chatId,
      message_ids: batchMessages.map((item) => item.message_id),
      merged_count: batchMessages.length,
      reply_message_id: delivered.messageId,
      reply_delivery_mode: delivered.mode,
      reply: replyText
    });
  } catch (error) {
    const latest = readQueue();
    const latestChat = ensureChatQueue(latest, chatId);
    latestChat.pending = dedupeMessages(batchMessages.concat(latestChat.pending));
    latestChat.processing = null;
    latestChat.updated_at = new Date().toISOString();
    writeQueue(latest);
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "batch-reply-failed",
      chat_id: chatId,
      message_ids: batchMessages.map((item) => item.message_id),
      error: String(error)
    });
    processingChats.delete(chatId);
    scheduleChatProcessing(chatId, config, flags);
    return;
  }

  const latest = readQueue();
  const latestChat = ensureChatQueue(latest, chatId);
  latestChat.processing = null;
  latestChat.updated_at = new Date().toISOString();
  writeQueue(latest);
  processingChats.delete(chatId);
  if (latestChat.pending.length) {
    scheduleChatProcessing(chatId, config, flags);
  }
}

function bootstrapPendingQueues(config, flags) {
  const queue = readQueue();
  let changed = false;
  for (const [chatId, chat] of Object.entries(queue.chats || {})) {
    if (chat?.processing && restoreStaleProcessing(queue, chatId, config, flags)) {
      changed = true;
    }
  }
  if (changed) {
    writeQueue(queue);
  }
  if (!flags["spawn-codex"]) {
    return;
  }
  for (const [chatId, chat] of Object.entries(queue.chats || {})) {
    if (chat?.pending?.length) {
      scheduleChatProcessing(chatId, config, flags, 0);
    }
  }
}

function startQueueSweeper(config, flags) {
  if (!flags["spawn-codex"]) {
    return;
  }
  if (queueSweepTimer) {
    clearInterval(queueSweepTimer);
  }
  const sweep = () => {
    const queue = readQueue();
    for (const [chatId, chat] of Object.entries(queue.chats || {})) {
      if (chat?.pending?.length || chat?.processing) {
        processChatQueue(chatId, config, flags).catch((error) => {
          appendJsonl(errorsPath, {
            timestamp: new Date().toISOString(),
            type: "queue-sweeper",
            chat_id: chatId,
            error: String(error)
          });
        });
      }
    }
  };
  sweep();
  queueSweepTimer = setInterval(sweep, getQueueSweepIntervalMs(config, flags));
}

async function handleIncomingMessage(event, config, flags) {
  appendJsonl(inboxPath, event);
  enqueueForChat(event);
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: event.source === "history-sync" ? "history-sync.user-message" : "im.message.receive_v1",
    event
  });

  if (event.source !== "history-sync" && isReactionEnabled(config, flags)) {
    sendMessageReaction(config, event.message_id, getReactionEmojiType(config, flags), flags)
      .then((response) => {
        const reactionId = response?.data?.reaction_id || "";
        rememberReaction(event.message_id, {
          chat_id: event.chat_id,
          reaction_id: reactionId,
          emoji_type: getReactionEmojiType(config, flags),
          created_at: new Date().toISOString()
        });
        appendJsonl(eventsPath, {
          timestamp: new Date().toISOString(),
          type: "im.message.reaction.create",
          chat_id: event.chat_id,
          message_id: event.message_id,
          emoji_type: getReactionEmojiType(config, flags),
          reaction_id: reactionId
        });
      })
      .catch((error) => {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "message-reaction-failed",
          chat_id: event.chat_id,
          message_id: event.message_id,
          emoji_type: getReactionEmojiType(config, flags),
          error: String(error)
        });
      });
  }

  if (flags["auto-ack"]) {
    const autoAckText = "已收到消息，派蒙会合并上下文后统一处理。";
    try {
      await sendTextMessage(config, "chat_id", event.chat_id, autoAckText);
      if (!flags["spawn-codex"]) {
        await clearTypingReactions(config, [event], flags);
      }
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "auto-ack",
        error: String(error),
        event
      });
    }
  }

  if (flags["spawn-codex"]) {
    scheduleChatProcessing(event.chat_id, config, flags);
  }
}

async function startWebsocket(config, flags) {
  ensureDirs();
  bootstrapPendingQueues(config, flags);
  startQueueSweeper(config, flags);

  const dispatcher = new Lark.EventDispatcher({}).register({
    "im.message.receive_v1": async (data) => {
      const event = extractMessageEvent(data);
      await handleIncomingMessage(event, config, flags);
    },
    "im.message.message_read_v1": async (data) => {
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "im.message.message_read_v1",
        event: data.event ?? data
      });
    }
  });

  const wsClient = new Lark.WSClient({
    appId: config.feishu.app_id,
    appSecret: config.feishu.app_secret,
    loggerLevel: Lark.LoggerLevel.info
  });

  wsClient.start({ eventDispatcher: dispatcher });
  console.log("Feishu WS gateway started");
}

async function fetchChatMessages(config, chatId, pageSize = 50, flags = {}) {
  const items = [];
  let pageToken = "";
  for (let i = 0; i < 20; i += 1) {
    const payload = await fetchMessagesPage(config, chatId, pageSize, pageToken, flags);
    const data = payload?.data || {};
    items.push(...(data.items || []));
    if (!data.has_more) {
      break;
    }
    pageToken = data.page_token || "";
    if (!pageToken) {
      break;
    }
  }
  items.sort((a, b) => Number(a.create_time || 0) - Number(b.create_time || 0));
  return items;
}

async function syncChatHistory(config, flags) {
  ensureDirs();
  const chatId = flags["chat-id"];
  if (!chatId) {
    throw new Error("sync requires --chat-id");
  }
  const seen = readSeenMessages();
  const items = await fetchChatMessages(config, chatId, Number(flags["page-size"] || 50));
  const fresh = [];

  for (const item of items) {
    if (seen[item.message_id]) {
      continue;
    }
    seen[item.message_id] = item.create_time || Date.now();
    if (item.sender?.sender_type !== "user") {
      continue;
    }
    const event = normalizeMessageItem(item);
    await handleIncomingMessage(event, config, flags);
    fresh.push(event);
  }
  writeSeenMessages(seen);
  if (flags["spawn-codex"] && fresh.length) {
    await processChatQueue(chatId, config, flags);
  }
  console.log(JSON.stringify({ synced: fresh.length, chat_id: chatId, messages: fresh }, null, 2));
}

function printHelp() {
  console.log(`Usage:
  node feishu_gateway.mjs token
  node feishu_gateway.mjs send --receive-id-type chat_id --receive-id xxx --text "hello"
  node feishu_gateway.mjs send-card --receive-id-type chat_id --receive-id xxx --text "working"
  node feishu_gateway.mjs update-card --message-id om_xxx --text "updated text"
  node feishu_gateway.mjs sync --chat-id oc_xxx [--auto-ack] [--spawn-codex] [--merge-window-ms 15000]
  node feishu_gateway.mjs ws [--auto-ack] [--spawn-codex] [--merge-window-ms 15000] [--history-limit 12] [--process-timeout-ms 1800000] [--queue-sweep-ms 15000] [--codex-timeout-ms 1200000] [--progress-ping-ms 300000] [--reaction-emoji Typing] [--no-reaction] [--http-timeout-ms 8000] [--codex-bypass-sandbox|--no-codex-bypass-sandbox]`);
}

async function main() {
  const config = readConfig();
  const { command, flags } = parseArgs(process.argv);

  if (command === "token") {
    const body = await fetchTenantToken(config);
    console.log(JSON.stringify(body, null, 2));
    return;
  }
  if (command === "send") {
    const receiveIdType = flags["receive-id-type"] || "chat_id";
    const receiveId = flags["receive-id"];
    const text = flags.text;
    if (!receiveId || !text) {
      throw new Error("send requires --receive-id and --text");
    }
    const result = await sendTextMessage(config, receiveIdType, receiveId, text);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  if (command === "send-card") {
    const receiveIdType = flags["receive-id-type"] || "chat_id";
    const receiveId = flags["receive-id"];
    const text = flags.text;
    if (!receiveId || !text) {
      throw new Error("send-card requires --receive-id and --text");
    }
    const card = buildStatusCard(text, {
      status: String(flags.status || "working"),
      mergedCount: Number(flags["merged-count"] || 1)
    });
    const result = await sendCardMessage(config, receiveIdType, receiveId, card, flags);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  if (command === "update-card" || command === "update") {
    const messageId = flags["message-id"];
    const text = flags.text;
    if (!messageId || !text) {
      throw new Error("update-card requires --message-id and --text");
    }
    const card = buildStatusCard(text, {
      status: String(flags.status || "working"),
      mergedCount: Number(flags["merged-count"] || 1)
    });
    const result = await updateCardMessage(config, messageId, card, flags);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  if (command === "ws") {
    await startWebsocket(config, flags);
    return;
  }
  if (command === "sync") {
    await syncChatHistory(config, flags);
    return;
  }
  printHelp();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
