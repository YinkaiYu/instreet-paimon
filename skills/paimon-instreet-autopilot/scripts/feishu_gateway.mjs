#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as Lark from "@larksuiteoapi/node-sdk";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../../..");
const configPath = path.join(repoRoot, "config", "paimon.json");
const stateCurrentDir = path.join(repoRoot, "state", "current");
const tmpDir = path.join(repoRoot, "tmp");
const inboxPath = path.join(stateCurrentDir, "feishu_inbox.jsonl");
const errorsPath = path.join(stateCurrentDir, "feishu_gateway_errors.jsonl");
const eventsPath = path.join(stateCurrentDir, "feishu_events.jsonl");
const seenMessagesPath = path.join(stateCurrentDir, "feishu_seen_messages.json");
const queuePath = path.join(stateCurrentDir, "feishu_queue.json");
const batchesPath = path.join(stateCurrentDir, "feishu_batches.jsonl");
const chatTimers = new Map();
const processingChats = new Set();

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

async function feishuApiRequest(config, endpoint, options = {}, retries = null, retryDelayMs = null) {
  const maxRetries = retries ?? Number(config.automation?.feishu_send_retries || 4);
  const baseDelayMs = retryDelayMs ?? Number(config.automation?.feishu_send_retry_delay_ms || 1500);
  let lastError = null;
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const auth = await fetchTenantToken(config);
      const response = await fetch(`https://open.feishu.cn${endpoint}`, {
        ...options,
        headers: {
          Authorization: `Bearer ${auth.tenant_access_token}`,
          "Content-Type": "application/json",
          ...(options.headers || {})
        }
      });
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

function buildClient(config) {
  return new Lark.Client({
    appId: config.feishu.app_id,
    appSecret: config.feishu.app_secret,
    appType: Lark.AppType.SelfBuild,
    domain: Lark.Domain.Feishu
  });
}

async function sendTextMessage(config, receiveIdType, receiveId, text) {
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
    }
  );
}

async function fetchMessagesPage(config, chatId, pageSize = 50, pageToken = "") {
  const url = new URL("https://open.feishu.cn/open-apis/im/v1/messages");
  url.searchParams.set("container_id_type", "chat");
  url.searchParams.set("container_id", chatId);
  url.searchParams.set("page_size", String(pageSize));
  if (pageToken) {
    url.searchParams.set("page_token", pageToken);
  }
  return feishuApiRequest(config, `${url.pathname}${url.search}`, {
    method: "GET"
  });
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

function formatHistoryLine(item) {
  const senderType = item.sender?.sender_type || item.sender?.id_type || "unknown";
  const who = senderType === "user" ? "用户" : senderType === "app" ? "派蒙" : senderType;
  const content = truncateText(item.text || "", 180);
  return `- ${who}: ${content}`;
}

function buildCodexPrompt(chatId, batchMessages, historyMessages, inboxMessages) {
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
    "本轮合并待处理消息：",
    batchLines || "- 无",
    "",
    "如果用户是在追问正在执行的工作，请明确说清当前结果、卡点和下一步。",
    "如果用户只是连续发了几个补充短句，要把它们整合成一次自然回复。"
  ].join("\n");
}

function runCodexPrompt(prompt) {
  ensureDirs();
  const outputFile = path.join(tmpDir, `feishu-reply-${Date.now()}.txt`);
  return new Promise((resolve, reject) => {
    const child = spawn(
      "codex",
      [
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
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`codex exited with code ${code}`));
        return;
      }
      const responseText = fs.readFileSync(outputFile, "utf8").trim();
      resolve(responseText);
    });
  });
}

function getMergeWindowMs(config, flags) {
  return Number(flags["merge-window-ms"] || config.automation?.feishu_merge_window_ms || 15000);
}

function getHistoryLimit(config, flags) {
  return Number(flags["history-limit"] || config.automation?.feishu_history_limit || 12);
}

function getProcessingTimeoutMs(config, flags) {
  return Number(flags["process-timeout-ms"] || config.automation?.feishu_processing_timeout_ms || 120000);
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
  if (!chat.processing || !isProcessingStale(chat.processing, config, flags)) {
    return false;
  }
  const recovered = recoverProcessingItems(chatId, chat.processing);
  chat.pending = dedupeMessages(recovered.concat(chat.pending));
  chat.processing = null;
  chat.updated_at = new Date().toISOString();
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
    const historyItems = await fetchChatMessages(config, chatId, Math.max(getHistoryLimit(config, flags) * 2, 20));
    const normalizedHistory = historyItems
      .filter((item) => item.msg_type === "text")
      .slice(-getHistoryLimit(config, flags))
      .map(normalizeMessageItem);
    const inboxMessages = readInboxLines(chatId, getHistoryLimit(config, flags));
    const prompt = buildCodexPrompt(chatId, batchMessages, normalizedHistory, inboxMessages);
    const replyText = await runCodexPrompt(prompt);
    if (!replyText) {
      throw new Error("empty Codex reply");
    }
    await sendTextMessage(config, "chat_id", chatId, replyText);
    appendJsonl(batchesPath, {
      timestamp: new Date().toISOString(),
      chat_id: chatId,
      message_ids: batchMessages.map((item) => item.message_id),
      merged_count: batchMessages.length,
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

async function handleIncomingMessage(event, config, flags) {
  appendJsonl(inboxPath, event);
  enqueueForChat(event);
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: event.source === "history-sync" ? "history-sync.user-message" : "im.message.receive_v1",
    event
  });

  if (flags["auto-ack"]) {
    const autoAckText = "已收到消息，派蒙会合并上下文后统一处理。";
    try {
      await sendTextMessage(config, "chat_id", event.chat_id, autoAckText);
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

async function fetchChatMessages(config, chatId, pageSize = 50) {
  const items = [];
  let pageToken = "";
  for (let i = 0; i < 20; i += 1) {
    const payload = await fetchMessagesPage(config, chatId, pageSize, pageToken);
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
  node feishu_gateway.mjs sync --chat-id oc_xxx [--auto-ack] [--spawn-codex] [--merge-window-ms 15000]
  node feishu_gateway.mjs ws [--auto-ack] [--spawn-codex] [--merge-window-ms 15000] [--history-limit 12] [--process-timeout-ms 120000]`);
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
