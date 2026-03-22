#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { EventEmitter } from "node:events";
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
const memoryManagerScriptPath = path.join(__dirname, "memory_manager.py");
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
const reportTargetPath = path.join(stateCurrentDir, "feishu_report_target.json");
const sessionsPath = path.join(stateCurrentDir, "feishu_sessions.json");
const messageThreadIndexPath = path.join(stateCurrentDir, "feishu_message_thread_index.json");
const pendingRequestsPath = path.join(stateCurrentDir, "feishu_pending_requests.json");
const statusPhrasesPath = path.join(repoRoot, "skills", "paimon-instreet-autopilot", "assets", "feishu-status-phrases.json");
const chatTimers = new Map();
const processingChats = new Set();
const sessionStatusTimers = new Map();
let queueSweepTimer = null;
let cachedCodexExecutable = null;
let cachedPythonExecutable = null;
let cachedStatusPhrases = null;
let appServerRuntime = null;

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

function describeError(error) {
  if (error instanceof Error) {
    return error.stack || error.message || String(error);
  }
  if (typeof error === "string") {
    return error;
  }
  if (error && typeof error === "object") {
    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }
  return String(error);
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

function readReportTargetState() {
  return readJsonFile(reportTargetPath, {});
}

function writeReportTargetState(payload) {
  writeJsonFile(reportTargetPath, payload);
}

function clearReportTargetState() {
  if (fs.existsSync(reportTargetPath)) {
    fs.unlinkSync(reportTargetPath);
  }
}

function readSessionStore() {
  return readJsonFile(sessionsPath, { version: 1, chats: {} });
}

function writeSessionStore(payload) {
  writeJsonFile(sessionsPath, payload);
}

function readMessageThreadIndex() {
  return readJsonFile(messageThreadIndexPath, { version: 1, messages: {} });
}

function writeMessageThreadIndex(payload) {
  writeJsonFile(messageThreadIndexPath, payload);
}

function normalizePendingRequestId(requestId) {
  if (requestId === null || requestId === undefined) {
    return "";
  }
  return String(requestId).trim();
}

function readPendingRequestStore() {
  return readJsonFile(pendingRequestsPath, { version: 1, requests: {} });
}

function writePendingRequestStore(payload) {
  writeJsonFile(pendingRequestsPath, payload);
}

function ensureSession(store, chatId) {
  if (!store.chats[chatId]) {
    store.chats[chatId] = {
      chat_id: chatId,
      thread_id: "",
      mode: "default",
      active_turn_id: "",
      provisional_turn_id: "",
      status: "idle",
      status_card_message_id: "",
      status_card_kind: "",
      status_card_phrase: "",
      pending_request_id: "",
      last_user_message_at: "",
      last_agent_message_at: "",
      last_completed_at: "",
      last_referenced_message_id: "",
      last_thread_preview: "",
      interrupted_at: ""
    };
  }
  return store.chats[chatId];
}

function updateSessionState(chatId, updater) {
  const store = readSessionStore();
  const session = ensureSession(store, chatId);
  const patch = typeof updater === "function" ? updater(session) : updater;
  if (patch && typeof patch === "object") {
    Object.assign(session, patch);
  }
  session.updated_at = new Date().toISOString();
  writeSessionStore(store);
  return session;
}

function findChatIdByThreadId(threadId) {
  if (!threadId) {
    return "";
  }
  const store = readSessionStore();
  for (const [chatId, session] of Object.entries(store.chats || {})) {
    if (session?.thread_id === threadId) {
      return chatId;
    }
  }
  return "";
}

function indexMessageToThread(messageId, payload) {
  if (!messageId) {
    return;
  }
  const store = readMessageThreadIndex();
  store.messages[messageId] = {
    ...payload,
    indexed_at: new Date().toISOString()
  };
  writeMessageThreadIndex(store);
}

function lookupMessageThreadBinding(messageId) {
  if (!messageId) {
    return null;
  }
  const store = readMessageThreadIndex();
  return store.messages[messageId] || null;
}

function rewriteIndexedTurnBindings(threadId, fromTurnId, toTurnId) {
  if (!threadId || !fromTurnId || !toTurnId || fromTurnId === toTurnId) {
    return 0;
  }
  const store = readMessageThreadIndex();
  let changed = 0;
  for (const entry of Object.values(store.messages || {})) {
    if (entry?.thread_id !== threadId || entry?.turn_id !== fromTurnId) {
      continue;
    }
    entry.turn_id = toTurnId;
    changed += 1;
  }
  if (changed) {
    writeMessageThreadIndex(store);
  }
  return changed;
}

function rewritePendingRequestTurnBindings(fromTurnId, toTurnId) {
  if (!fromTurnId || !toTurnId || fromTurnId === toTurnId) {
    return 0;
  }
  const store = readPendingRequestStore();
  let changed = 0;
  for (const entry of Object.values(store.requests || {})) {
    if (entry?.turn_id !== fromTurnId) {
      continue;
    }
    entry.turn_id = toTurnId;
    changed += 1;
  }
  if (changed) {
    writePendingRequestStore(store);
  }
  return changed;
}

function removePendingRequest(requestId) {
  const normalizedId = normalizePendingRequestId(requestId);
  if (!normalizedId) {
    return null;
  }
  const store = readPendingRequestStore();
  const entry = store.requests[normalizedId] || null;
  if (!entry) {
    return null;
  }
  delete store.requests[normalizedId];
  writePendingRequestStore(store);
  return entry;
}

function upsertPendingRequest(requestId, payload) {
  const normalizedId = normalizePendingRequestId(requestId);
  if (!normalizedId) {
    return null;
  }
  const store = readPendingRequestStore();
  const existing = store.requests[normalizedId] || {};
  store.requests[normalizedId] = {
    ...existing,
    ...payload,
    request_id: normalizedId,
    updated_at: new Date().toISOString()
  };
  writePendingRequestStore(store);
  return store.requests[normalizedId];
}

function readPendingRequest(requestId) {
  const normalizedId = normalizePendingRequestId(requestId);
  if (!normalizedId) {
    return null;
  }
  const store = readPendingRequestStore();
  return store.requests[normalizedId] || null;
}

function removePendingRequestsForTurn(turnId) {
  if (!turnId) {
    return [];
  }
  const store = readPendingRequestStore();
  const removed = [];
  for (const [requestId, entry] of Object.entries(store.requests || {})) {
    if (entry?.turn_id !== turnId) {
      continue;
    }
    removed.push(entry);
    delete store.requests[requestId];
  }
  if (removed.length) {
    writePendingRequestStore(store);
  }
  return removed;
}

function resolvePythonExecutable() {
  if (cachedPythonExecutable) {
    return cachedPythonExecutable;
  }
  const candidates = [
    process.env.PAIMON_PYTHON_BIN,
    process.env.PYTHON_BIN,
    "python3",
    "python"
  ].filter(Boolean);
  for (const candidate of candidates) {
    const probe = spawnSync(candidate, ["--version"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"]
    });
    if (probe.status === 0) {
      cachedPythonExecutable = candidate;
      return candidate;
    }
  }
  throw new Error("python executable not found for memory manager");
}

function runMemoryManager(args, payload = null) {
  const result = spawnSync(resolvePythonExecutable(), [memoryManagerScriptPath, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
    input: payload ? JSON.stringify(payload) : undefined,
    env: process.env
  });
  if (result.status !== 0) {
    const stderr = (result.stderr || result.stdout || "").trim();
    throw new Error(`memory manager failed: ${stderr}`);
  }
  return (result.stdout || "").trim();
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

function listIncomingDedupKeys(event) {
  const keys = [];
  if (event?.message_id) {
    keys.push(event.message_id);
  }
  const rawEventId = event?.raw?.event_id || "";
  if (rawEventId) {
    keys.push(`event:${rawEventId}`);
  }
  return keys;
}

function mergeSeenMessages(entries) {
  if (!entries || typeof entries !== "object" || !Object.keys(entries).length) {
    return readSeenMessages();
  }
  const seen = readSeenMessages();
  let changed = false;
  for (const [key, value] of Object.entries(entries)) {
    if (!key || Object.prototype.hasOwnProperty.call(seen, key)) {
      continue;
    }
    seen[key] = value;
    changed = true;
  }
  if (changed) {
    writeSeenMessages(seen);
  }
  return seen;
}

function inboxEventMatchesIncomingEvent(inboxEvent, event) {
  if (!inboxEvent || !event) {
    return false;
  }
  if (event.message_id && inboxEvent.message_id === event.message_id) {
    return true;
  }
  const inboxEventId = inboxEvent?.raw?.event_id || "";
  const rawEventId = event?.raw?.event_id || "";
  return Boolean(rawEventId && inboxEventId && rawEventId === inboxEventId);
}

function inboxAlreadyHasIncomingEvent(event) {
  if (!fs.existsSync(inboxPath)) {
    return false;
  }
  const lines = fs.readFileSync(inboxPath, "utf8").split("\n");
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index]?.trim();
    if (!line) {
      continue;
    }
    try {
      const payload = JSON.parse(line);
      if (inboxEventMatchesIncomingEvent(payload, event)) {
        return true;
      }
    } catch {
      // ignore malformed lines
    }
  }
  return false;
}

function rememberIncomingEvent(event) {
  const dedupKeys = listIncomingDedupKeys(event);
  if (!dedupKeys.length) {
    return false;
  }
  const seen = readSeenMessages();
  if (dedupKeys.some((key) => Object.prototype.hasOwnProperty.call(seen, key))) {
    return true;
  }
  const timestamp = event?.received_at || new Date().toISOString();
  mergeSeenMessages(
    Object.fromEntries(dedupKeys.map((key) => [key, timestamp]))
  );
  return false;
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
  const title =
    status === "done"
      ? "派蒙回复完成"
      : status === "waiting"
        ? "派蒙等待你的选择"
      : status === "error"
        ? "派蒙思考中断"
        : "派蒙正在工作";
  const template =
    status === "done"
      ? "green"
      : status === "waiting"
        ? "orange"
      : status === "error"
        ? "red"
        : "blue";
  const noteParts = [
    status === "done"
      ? "状态：已完成"
      : status === "waiting"
        ? "状态：等待选择"
        : status === "error"
          ? "状态：稍后重试"
          : "状态：处理中",
    `更新时间：${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`
  ];
  const elements = [
    {
      tag: "markdown",
      content: clampCardText(text)
    }
  ];
  const questions = Array.isArray(options.questions) ? options.questions : [];
  const answers = options.answers || {};
  const allowActions = options.allowActions !== false;
  if (questions.length) {
    for (const question of questions) {
      elements.push({
        tag: "markdown",
        content: clampCardText([
          `**${question.header || "问题"}**`,
          question.question || "",
          answers?.[question.id]?.answers?.length ? `已选：${answers[question.id].answers.join(" / ")}` : ""
        ].filter(Boolean).join("\n"))
      });
      const actionItems = [];
      if (allowActions && Array.isArray(question.options) && question.options.length) {
        for (const [index, option] of question.options.entries()) {
          actionItems.push({
            tag: "button",
            type: index === 0 ? "primary" : "default",
            text: {
              tag: "plain_text",
              content: option.label
            },
            value: {
              action: "request-user-input-answer",
              chat_id: options.chatId || "",
              request_id: options.requestId || "",
              question_id: question.id,
              answer: option.label
            }
          });
        }
      }
      if (actionItems.length) {
        elements.push({
          tag: "action",
          actions: actionItems
        });
      } else if (Array.isArray(question.options) && question.options.length) {
        elements.push({
          tag: "markdown",
          content: clampCardText(question.options.map((option, index) => `${index + 1}. ${option.label}：${option.description}`).join("\n"))
        });
      }
    }
    elements.push({
      tag: "note",
      elements: [
        {
          tag: "plain_text",
          content: "可以直接点按钮，也可以继续发文字补充。"
        }
      ]
    });
  }
  elements.push({
    tag: "note",
    elements: [
      {
        tag: "plain_text",
        content: noteParts.join(" | ")
      }
    ]
  });
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
    elements
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

class CodexAppServerClient extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.process = null;
    this.pending = new Map();
    this.buffer = "";
    this.nextId = 1;
    this.readyPromise = null;
  }

  async ensureStarted() {
    if (this.readyPromise) {
      return this.readyPromise;
    }
    this.readyPromise = this.start().catch((error) => {
      this.readyPromise = null;
      throw error;
    });
    return this.readyPromise;
  }

  async start() {
    if (this.process) {
      return;
    }
    const codexBin = resolveCodexExecutable(this.config, {});
    this.process = spawn(
      codexBin,
      ["app-server", "--listen", "stdio://", "--session-source", "cli"],
      {
        cwd: repoRoot,
        stdio: ["pipe", "pipe", "pipe"],
        env: process.env
      }
    );
    this.process.stdout.on("data", (chunk) => this.handleStdout(chunk.toString()));
    this.process.stderr.on("data", (chunk) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "app-server.stderr",
        error: chunk.toString()
      });
    });
    this.process.on("exit", (code, signal) => {
      const pending = Array.from(this.pending.values());
      this.pending.clear();
      this.process = null;
      this.readyPromise = null;
      for (const entry of pending) {
        entry.reject(new Error(`codex app-server exited code=${code} signal=${signal || ""}`.trim()));
      }
      this.emit("exit", { code, signal });
    });
    const initResult = await this.requestInternal("initialize", {
      clientInfo: {
        name: "paimon-feishu-gateway",
        version: "0.1.0"
      },
      capabilities: {
        experimentalApi: true,
        optOutNotificationMethods: []
      }
    });
    void initResult;
    this.notify("initialized");
  }

  handleStdout(chunk) {
    this.buffer += chunk;
    const lines = this.buffer.split(/\n/);
    this.buffer = lines.pop() || "";
    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }
      let payload = null;
      try {
        payload = JSON.parse(line);
      } catch (error) {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "app-server-parse-failed",
          line,
          error: String(error)
        });
        continue;
      }
      if (payload?.method && Object.prototype.hasOwnProperty.call(payload, "id")) {
        this.emit("server-request", payload);
        continue;
      }
      if (Object.prototype.hasOwnProperty.call(payload, "id")) {
        const pending = this.pending.get(payload.id);
        if (!pending) {
          continue;
        }
        this.pending.delete(payload.id);
        if (payload.error) {
          pending.reject(payload.error);
        } else {
          pending.resolve(payload.result);
        }
        continue;
      }
      if (payload?.method) {
        this.emit("notification", payload);
      }
    }
  }

  sendPayload(payload) {
    if (!this.process) {
      throw new Error("codex app-server is not running");
    }
    this.process.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  requestInternal(method, params) {
    const id = this.nextId++;
    this.sendPayload({
      jsonrpc: "2.0",
      id,
      method,
      params
    });
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  async request(method, params) {
    await this.ensureStarted();
    return this.requestInternal(method, params);
  }

  notify(method, params = undefined) {
    if (!this.process) {
      return;
    }
    const payload = {
      jsonrpc: "2.0",
      method
    };
    if (params !== undefined) {
      payload.params = params;
    }
    this.sendPayload(payload);
  }

  async respond(id, result) {
    await this.ensureStarted();
    this.sendPayload({
      jsonrpc: "2.0",
      id,
      result
    });
  }
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
  const mentions = Array.isArray(message.mentions) ? message.mentions : [];
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
    chat_type: message.chat_type || "",
    message_type: message.message_type,
    thread_id: message.thread_id || "",
    parent_id: message.parent_id || "",
    root_id: message.root_id || "",
    text: parsedContent.text || parsedContent.raw || "",
    mentions,
    sender: sender.sender_id ?? {},
    sender_type: sender.sender_type || "",
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
    chat_type: item.chat_type || "",
    message_type: item.msg_type,
    thread_id: item.thread_id || "",
    parent_id: item.parent_id || "",
    root_id: item.root_id || "",
    text: parsedContent.text || parsedContent.raw || "",
    mentions: Array.isArray(item?.mentions) ? item.mentions : [],
    sender: item.sender || {},
    sender_type: item?.sender?.sender_type || "",
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

function getMessageTimestampMs(item) {
  const candidates = [
    item?.raw?.message?.create_time,
    item?.raw?.create_time,
    item?.created_at,
    item?.updated_at,
    item?.received_at
  ];
  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined || candidate === "") {
      continue;
    }
    const numeric = Number(candidate);
    if (Number.isFinite(numeric) && numeric > 0) {
      return numeric;
    }
    const parsed = Date.parse(String(candidate));
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return Date.now();
}

function getContinuationWindowMs(config, flags) {
  return Number(flags["continuation-window-ms"] || config.automation?.feishu_continuation_window_ms || 1800000);
}

function getContinuationMaxMessages(config, flags) {
  return Number(flags["continuation-max-messages"] || config.automation?.feishu_continuation_max_messages || 6);
}

function isHistoryRawPromptEnabled(config, flags) {
  if (flags["history-raw-prompt"]) {
    return true;
  }
  if (flags["no-history-raw-prompt"]) {
    return false;
  }
  return config.automation?.feishu_history_raw_prompt_enabled === true;
}

function readRecentContinuation(chatId, batchMessages, config, flags) {
  const maxMessages = getContinuationMaxMessages(config, flags);
  if (maxMessages <= 0) {
    return [];
  }
  const batchIds = new Set(batchMessages.map((item) => item.message_id));
  const batchStartMs = Math.min(...batchMessages.map((item) => getMessageTimestampMs(item)));
  const windowMs = getContinuationWindowMs(config, flags);
  const candidates = readInboxLines(chatId, null)
    .filter((item) => item?.text)
    .filter((item) => !batchIds.has(item.message_id))
    .filter((item) => {
      const timestampMs = getMessageTimestampMs(item);
      return timestampMs < batchStartMs && batchStartMs - timestampMs <= windowMs;
    })
    .sort((a, b) => getMessageTimestampMs(a) - getMessageTimestampMs(b));
  const anchorKeys = collectConversationKeys(batchMessages);
  if (!anchorKeys.size) {
    return candidates.slice(-maxMessages);
  }
  const preferred = candidates.filter((item) => sharesConversationKey(item, anchorKeys));
  if (preferred.length >= maxMessages) {
    return preferred.slice(-maxMessages);
  }
  const preferredIds = new Set(preferred.map((item) => item.message_id));
  const fallback = candidates.filter((item) => !preferredIds.has(item.message_id));
  return preferred
    .concat(fallback.slice(-(maxMessages - preferred.length)))
    .sort((a, b) => getMessageTimestampMs(a) - getMessageTimestampMs(b));
}

function getMemoryPromptSnapshot(chatId) {
  return runMemoryManager(["snapshot", "--format", "prompt", "--channel", "feishu", "--chat-id", chatId]);
}

function syncInteractionMemory(chatId, batchMessages, replyText) {
  const firstSender = batchMessages.find((item) => item?.sender)?.sender || {};
  const userId =
    firstSender?.user_id ||
    firstSender?.sender_id?.user_id ||
    "";
  return runMemoryManager(
    ["record-interaction", "--payload-file", "-"],
    {
      source: "feishu",
      channel: "feishu",
      chat_id: chatId,
      user_id: userId,
      messages: batchMessages.map((item) => ({
        message_id: item.message_id,
        received_at: item.received_at,
        text: item.text
      })),
      reply_text: replyText,
      recorded_at: new Date().toISOString()
    }
  );
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

function isPidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function stopPidBestEffort(pid) {
  if (!isPidAlive(pid)) {
    return false;
  }
  try {
    process.kill(pid, "SIGTERM");
    return true;
  } catch {
    return false;
  }
}

function truncateText(text, limit = 220) {
  const source = typeof text === "string" ? text : JSON.stringify(text, null, 0);
  if (source.length <= limit) {
    return source;
  }
  return `${source.slice(0, limit - 3)}...`;
}

function loadStatusPhrases() {
  if (cachedStatusPhrases) {
    return cachedStatusPhrases;
  }
  const fallback = {
    working: [
      "正在把线头一根根捋直",
      "正在翻上下文的抽屉",
      "正在对齐这轮需求的骨架",
      "正在把碎片拼成可执行的形状",
      "正在把刚冒头的问题摁回桌面",
      "正在替你把岔路口的牌子擦亮"
    ],
    waiting: [
      "停在岔路口等你拍板",
      "手里攥着两个方案等你点头",
      "问题已经摆上桌，等你落子",
      "先把节奏按住，等你选方向"
    ],
    done: [
      "这锅已经收汁",
      "这轮线头已经收拢",
      "该落地的部分已经归位",
      "这一段先告一段落"
    ],
    error: [
      "中途绊了一下，正在找新的落脚点",
      "这轮链路打了个结，需要换个走法"
    ]
  };
  try {
    const loaded = readJsonFile(statusPhrasesPath, fallback);
    cachedStatusPhrases = {
      working: Array.isArray(loaded.working) && loaded.working.length ? loaded.working : fallback.working,
      waiting: Array.isArray(loaded.waiting) && loaded.waiting.length ? loaded.waiting : fallback.waiting,
      done: Array.isArray(loaded.done) && loaded.done.length ? loaded.done : fallback.done,
      error: Array.isArray(loaded.error) && loaded.error.length ? loaded.error : fallback.error
    };
  } catch {
    cachedStatusPhrases = fallback;
  }
  return cachedStatusPhrases;
}

function pickStatusPhrase(status, previous = "") {
  const phrases = loadStatusPhrases();
  const bucket = Array.isArray(phrases?.[status]) && phrases[status].length ? phrases[status] : phrases.working;
  const candidates = bucket.filter((item) => item !== previous);
  const source = candidates.length ? candidates : bucket;
  return source[Math.floor(Math.random() * source.length)] || "";
}

function normalizeMode(mode) {
  return String(mode || "").trim().toLowerCase() === "plan" ? "plan" : "default";
}

function stripLeadingDirectivePreamble(text) {
  let remainder = String(text || "").trim();
  while (remainder) {
    const next = remainder
      .replace(/^\s*【[^】]{1,48}】\s*/u, "")
      .replace(/^\s*@(?:[^\s，,:：]+)\s*/u, "")
      .replace(/^\s*派蒙(?:[\s，,:：]+|$)/u, "");
    if (next === remainder) {
      break;
    }
    remainder = next.trimStart();
  }
  return remainder;
}

function extractModeDirective(text) {
  const source = String(text || "").trim();
  if (!source) {
    return { mode: "", newThread: false, remainder: "" };
  }
  const directiveSource = stripLeadingDirectivePreamble(source);
  const patterns = [
    {
      kind: "new-thread",
      pattern: /^\s*(?:请)?(?:先)?(?:帮我)?(?:重新|直接)?(?:开新|新开|开个新|开一个新|开一条新|另开|另起|重新开|重新起|换个新|切到新的)\s*(?:thread|线程|对话|会话)\s*[，,:：]?\s*/iu
    },
    {
      kind: "mode",
      mode: "plan",
      pattern: /^\s*(?:请)?(?:先)?(?:帮我)?(?:进入|切到|切换到|改成|改用|用)\s*(?:plan(?:\s*mode)?|规划模式|计划模式)\s*[，,:：]?\s*/iu
    },
    {
      kind: "mode",
      mode: "default",
      pattern: /^\s*(?:请)?(?:先)?(?:帮我)?(?:退出|切回|切到|切换到|改成|改用|用)\s*(?:default(?:\s*mode)?|普通模式|默认模式)\s*[，,:：]?\s*/iu
    }
  ];
  let remainder = directiveSource;
  let mode = "";
  let newThread = false;
  while (remainder) {
    let matched = false;
    for (const entry of patterns) {
      const match = remainder.match(entry.pattern);
      if (!match) {
        continue;
      }
      if (entry.kind === "mode") {
        mode = entry.mode;
      } else if (entry.kind === "new-thread") {
        newThread = true;
      }
      remainder = remainder.slice(match[0].length).trim();
      matched = true;
      break;
    }
    if (!matched) {
      break;
    }
  }
  if (!mode && !newThread) {
    return { mode: "", newThread: false, remainder: source };
  }
  return { mode, newThread, remainder };
}

function buildDirectiveAckText(mode = "", newThread = false) {
  if (newThread && mode === "plan") {
    return "后续我会新开一条线程，并切到 plan mode。你继续发需求，我就从新 thread 接。";
  }
  if (newThread && mode === "default") {
    return "后续我会新开一条线程，并按默认模式接。你继续发需求，我就从新 thread 接。";
  }
  if (newThread) {
    return "后续我会新开一条线程。你继续发需求，我就从新 thread 接。";
  }
  if (mode === "plan") {
    return "这条线程后续切到规划模式了。你继续发需求，我就按 plan mode 来。";
  }
  return "这条线程后续切回普通模式了。你继续发需求，我就按默认模式来。";
}

function isTurnActive(session) {
  return Boolean(session?.active_turn_id);
}

function shouldApplyTurnCompletionToSession(session, completedTurnId) {
  if (!session?.active_turn_id) {
    return true;
  }
  return session.active_turn_id === completedTurnId;
}

function isPunctuationOnlyChunk(text) {
  return /^[\p{P}\p{S}\s]+$/u.test(String(text || ""));
}

function trimChunkBoundary(text) {
  return String(text || "")
    .replace(/\s*\n+\s*([。！？!?])/gu, "$1")
    .trim();
}

function pushNaturalMessageChunk(chunks, piece) {
  const normalized = trimChunkBoundary(piece);
  if (!normalized) {
    return;
  }
  if (chunks.length && isPunctuationOnlyChunk(normalized)) {
    chunks[chunks.length - 1] = `${chunks[chunks.length - 1]}${normalized}`;
    return;
  }
  chunks.push(normalized);
}

function splitNaturalMessageChunks(buffer, force = false) {
  const chunks = [];
  let remaining = String(buffer || "");
  if (!remaining) {
    return { chunks, remaining };
  }
  const sentencePattern = /(.+?[。！？!?]\s*|\n{2,}|.+?\n)/u;
  while (true) {
    const match = remaining.match(sentencePattern);
    if (!match) {
      break;
    }
    const piece = match[0];
    let nextRemaining = remaining.slice(match[0].length);
    const endsWithSingleNewline = /\n$/.test(piece) && !/\n{2,}$/.test(piece) && !/[。！？!?]\s*$/.test(piece);
    if (endsWithSingleNewline) {
      const trailingPunctuation = nextRemaining.match(/^\s*[\p{P}\p{S}]+\s*/u);
      if (trailingPunctuation?.[0]?.trim()) {
        pushNaturalMessageChunk(chunks, `${piece.replace(/\s+$/u, "")}${trailingPunctuation[0].trim()}`);
        nextRemaining = nextRemaining.slice(trailingPunctuation[0].length);
        remaining = nextRemaining;
        continue;
      }
      if (!force && !nextRemaining.trim()) {
        break;
      }
    }
    remaining = nextRemaining;
    pushNaturalMessageChunk(chunks, piece);
  }
  if (force && remaining.trim()) {
    pushNaturalMessageChunk(chunks, remaining);
    remaining = "";
  }
  return { chunks, remaining };
}

function buildQuestionAnswerPayload(questions, partialAnswers) {
  const answers = {};
  for (const question of questions || []) {
    const answer = partialAnswers?.[question.id];
    if (!answer?.answers?.length) {
      continue;
    }
    answers[question.id] = {
      answers: answer.answers
    };
  }
  return { answers };
}

function tryMapTextToQuestionAnswer(question, text) {
  const normalized = String(text || "").trim();
  if (!normalized) {
    return null;
  }
  const options = Array.isArray(question?.options) ? question.options : [];
  for (let index = 0; index < options.length; index += 1) {
    const option = options[index];
    if (!option?.label) {
      continue;
    }
    if (normalized === option.label || normalized.includes(option.label) || normalized === String(index + 1)) {
      return { answers: [option.label] };
    }
  }
  return { answers: [normalized] };
}

function collectConversationKeys(items) {
  const keys = new Set();
  for (const item of items || []) {
    for (const candidate of [item?.thread_id, item?.root_id, item?.parent_id]) {
      if (candidate) {
        keys.add(candidate);
      }
    }
  }
  return keys;
}

function sharesConversationKey(item, anchorKeys) {
  if (!anchorKeys?.size) {
    return false;
  }
  for (const candidate of [item?.thread_id, item?.root_id, item?.parent_id]) {
    if (candidate && anchorKeys.has(candidate)) {
      return true;
    }
  }
  return false;
}

function formatMessageContext(item) {
  const parts = [];
  if (item?.chat_type) {
    parts.push(item.chat_type);
  }
  if (item?.thread_id) {
    parts.push(`thread=${truncateText(item.thread_id, 18)}`);
  }
  if (item?.parent_id) {
    parts.push(`parent=${truncateText(item.parent_id, 18)}`);
  }
  if (item?.root_id && item.root_id !== item.parent_id) {
    parts.push(`root=${truncateText(item.root_id, 18)}`);
  }
  return parts.join(" ");
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
  if (stage === "initial") {
    return "派蒙正在思考这轮消息。";
  }
  if (stage === "codex") {
    return "派蒙正在整理想法。";
  }
  return "派蒙正在思考。";
}

function getRuntimeBackend(config, flags) {
  return String(flags["runtime-backend"] || config.automation?.feishu_runtime_backend || "app-server").trim() || "app-server";
}

function getDefaultCollaborationMode(config) {
  const mode = String(config.automation?.feishu_default_collaboration_mode || "default").trim().toLowerCase();
  return mode === "plan" ? "plan" : "default";
}

function getThreadIdleTtlMs(config, flags) {
  return Number(flags["thread-idle-ttl-ms"] || config.automation?.feishu_thread_idle_ttl_ms || 3600000);
}

function getCodexTimeoutMs(config, flags) {
  return Number(flags["codex-timeout-ms"] || config.automation?.feishu_codex_timeout_ms || 1200000);
}

function getHttpTimeoutMs(config, flags) {
  return Number(flags["http-timeout-ms"] || config.automation?.feishu_http_timeout_ms || 8000);
}

function getProgressFlushMs(config, flags) {
  return Number(flags["progress-flush-ms"] || config.automation?.feishu_progress_flush_ms || 2000);
}

function getProgressPingMs(config, flags) {
  return Number(flags["progress-ping-ms"] || config.automation?.feishu_progress_ping_ms || 300000);
}

function getStatusCardUpdateMinMs(config, flags) {
  return Number(flags["status-card-update-min-ms"] || config.automation?.feishu_status_card_update_min_ms || 8000);
}

function getStatusCardUpdateMaxMs(config, flags) {
  return Number(flags["status-card-update-max-ms"] || config.automation?.feishu_status_card_update_max_ms || 15000);
}

function shouldEnableCardCallbacks(config, flags) {
  if (flags["no-card-callback"]) {
    return false;
  }
  if (flags["card-callback"]) {
    return true;
  }
  return config.automation?.feishu_card_callback_enabled === true;
}

function supportsCardActions(config, flags) {
  return shouldEnableCardCallbacks(config, flags);
}

function buildCurrentSessionStatusCard(chatId) {
  if (!chatId) {
    return buildStatusCard(pickStatusPhrase("done"), { status: "done" });
  }
  const session = ensureSession(readSessionStore(), chatId);
  const status = session.status_card_kind || session.status || "done";
  const phrase = session.status_card_phrase || pickStatusPhrase(status);
  return buildStatusCard(phrase, { status });
}

function summarizeAppServerEnvelope(payload) {
  const params = payload?.params || {};
  return {
    method: payload?.method || "",
    request_id: Object.prototype.hasOwnProperty.call(payload || {}, "id") ? String(payload.id) : "",
    thread_id: params.threadId || params.thread?.id || "",
    turn_id: params.turnId || params.turn?.id || "",
    item_id: params.itemId || params.item?.id || "",
    turn_status: params.turn?.status || "",
    question_count: Array.isArray(params.questions) ? params.questions.length : 0,
    plan_steps: Array.isArray(params.plan) ? params.plan.length : 0,
    delta_len: typeof params.delta === "string" ? params.delta.length : 0
  };
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
  const senderType = item.sender_type || item.sender?.sender_type || item.sender?.id_type || "unknown";
  const who = senderType === "user" ? "用户" : senderType === "app" ? "派蒙" : senderType;
  const content = truncateText(item.text || "", 180);
  const context = formatMessageContext(item);
  return `- ${who}${context ? ` [${context}]` : ""}: ${content}`;
}

function buildBatchLine(item, index) {
  const context = formatMessageContext(item);
  return `${index + 1}. ${context ? `[${context}] ` : ""}${item.text}`;
}

function lookupReplyBatchByMessageId(messageId) {
  if (!messageId || !fs.existsSync(batchesPath)) {
    return null;
  }
  const lines = fs.readFileSync(batchesPath, "utf8").trim().split("\n").filter(Boolean).reverse();
  for (const line of lines) {
    try {
      const payload = JSON.parse(line);
      if (payload.reply_message_id === messageId) {
        return payload;
      }
    } catch {
      // ignore malformed lines
    }
  }
  return null;
}

function collectReferencedReplies(batchMessages, limit = 3) {
  const referencedIds = [];
  for (const item of batchMessages) {
    for (const candidate of [item?.parent_id, item?.root_id]) {
      if (candidate) {
        referencedIds.push(candidate);
      }
    }
  }
  const seen = new Set();
  const replies = [];
  for (const messageId of referencedIds) {
    if (seen.has(messageId)) {
      continue;
    }
    seen.add(messageId);
    const batch = lookupReplyBatchByMessageId(messageId);
    if (!batch?.reply) {
      continue;
    }
    replies.push({
      message_id: messageId,
      chat_id: batch.chat_id || "",
      replied_at: batch.timestamp || "",
      reply: batch.reply
    });
    if (replies.length >= limit) {
      break;
    }
  }
  return replies;
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

function buildFeishuContextBlock({ chatId, messageText, session, liveProbeSummary, memorySnapshot, event, previousThreadPreview = "", mode = "default", isSteer = false }) {
  const lines = [
    "派蒙，你正在通过飞书和用户连续协作。",
    "飞书回复约束：",
    "- 工作中多发短句自然语言更新，像人类在同步进度。",
    "- 不要主动展开变量名、堆栈、原始命令输出或仓库内部实现细节，除非用户明确追问。",
    "- 不要主动输出 Markdown 链接、文件路径、行号、配置键名或代码定位；飞书里优先只说自然语言结论与进展，除非用户明确要这些细节。",
    `- 当前会话模式：${normalizeMode(mode)}。`,
    isSteer ? "- 这条消息是在你工作中途追加的，请把它视为同一件事的补充或纠偏。" : "- 这是一轮新的飞书输入。"
  ];
  if (previousThreadPreview) {
    lines.push(`- 上一轮话题摘要：${truncateText(previousThreadPreview, 160)}`);
  }
  lines.push("", `当前 chat_id：${chatId}`);
  if (event?.thread_id || event?.parent_id || event?.root_id) {
    lines.push(`消息上下文：${formatMessageContext(event) || "无"}`);
  }
  lines.push("", "统一记忆快照：", memorySnapshot || "- 无", "", "实时平台探针：", liveProbeSummary || "- 无", "", "用户这次的飞书消息：", messageText || "- 无");
  return lines.join("\n");
}

function buildTurnInputItems(contextText) {
  return [
    {
      type: "text",
      text: contextText,
      text_elements: []
    }
  ];
}

function resolveReferencedThreadBinding(event) {
  for (const candidate of [event?.parent_id, event?.root_id, event?.thread_id]) {
    const binding = lookupMessageThreadBinding(candidate);
    if (binding?.thread_id) {
      return binding;
    }
  }
  return null;
}

function isSessionExpired(session, config, flags) {
  if (!session?.thread_id) {
    return true;
  }
  const anchor = session.last_completed_at || session.last_agent_message_at || session.last_user_message_at;
  if (!anchor) {
    return false;
  }
  const anchorMs = Date.parse(anchor);
  if (Number.isNaN(anchorMs)) {
    return false;
  }
  return Date.now() - anchorMs > getThreadIdleTtlMs(config, flags);
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

function clearSessionStatusTimer(chatId) {
  if (!sessionStatusTimers.has(chatId)) {
    return;
  }
  clearTimeout(sessionStatusTimers.get(chatId));
  sessionStatusTimers.delete(chatId);
}

async function sendIndexedTextMessage(config, chatId, text, session, flags = {}) {
  const response = await sendTextMessage(config, "chat_id", chatId, text, flags);
  const messageId = response?.data?.message_id || "";
  if (messageId) {
    indexMessageToThread(messageId, {
      chat_id: chatId,
      thread_id: session?.thread_id || "",
      turn_id: session?.active_turn_id || "",
      message_type: "agent-text"
    });
  }
  if (session?.chat_id) {
    updateSessionState(session.chat_id, {
      last_agent_message_at: new Date().toISOString()
    });
  }
  return response;
}

async function upsertSessionCard(config, chatId, status, flags = {}, options = {}) {
  const session = ensureSession(readSessionStore(), chatId);
  const pendingRequest = options.pendingRequest || readPendingRequest(session.pending_request_id);
  const phrase = options.phrase || pickStatusPhrase(status, session.status_card_phrase);
  const card = buildStatusCard(phrase, {
    status,
    questions: pendingRequest?.questions || [],
    answers: pendingRequest?.answers || {},
    requestId: pendingRequest?.request_id || "",
    chatId,
    allowActions: supportsCardActions(config, flags)
  });
  let messageId = session.status_card_message_id || "";
  if (messageId) {
    try {
      await updateCardMessage(config, messageId, card, flags);
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "session-card-update-failed",
        chat_id: chatId,
        message_id: messageId,
        error: String(error)
      });
      messageId = "";
    }
  }
  if (!messageId) {
    const response = await sendCardMessage(config, "chat_id", chatId, card, flags);
    messageId = response?.data?.message_id || "";
  }
  const updatedSession = updateSessionState(chatId, {
    status_card_message_id: messageId,
    status_card_kind: status,
    status_card_phrase: phrase,
  });
  if (messageId) {
    indexMessageToThread(messageId, {
      chat_id: chatId,
      thread_id: updatedSession.thread_id || "",
      turn_id: updatedSession.active_turn_id || "",
      message_type: "status-card"
    });
  }
  return updatedSession;
}

function scheduleSessionCardRefresh(config, flags, chatId) {
  clearSessionStatusTimer(chatId);
  const store = readSessionStore();
  const session = ensureSession(store, chatId);
  if (!session.status_card_message_id || !["working", "waiting"].includes(session.status_card_kind)) {
    return;
  }
  const minMs = getStatusCardUpdateMinMs(config, flags);
  const maxMs = Math.max(minMs, getStatusCardUpdateMaxMs(config, flags));
  const delay = minMs + Math.floor(Math.random() * Math.max(1, maxMs - minMs + 1));
  const timer = setTimeout(async () => {
    try {
      const latestStore = readSessionStore();
      const latestSession = ensureSession(latestStore, chatId);
      if (!latestSession.status_card_message_id || !["working", "waiting"].includes(latestSession.status_card_kind)) {
        clearSessionStatusTimer(chatId);
        return;
      }
      await upsertSessionCard(config, chatId, latestSession.status_card_kind, flags, {
        phrase: pickStatusPhrase(latestSession.status_card_kind, latestSession.status_card_phrase)
      });
      scheduleSessionCardRefresh(config, flags, chatId);
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "session-card-refresh-failed",
        chat_id: chatId,
        error: String(error)
      });
    }
  }, delay);
  sessionStatusTimers.set(chatId, timer);
}

async function settleSessionCardIfNeeded(config, flags, chatId, status = "done", phrase = "") {
  if (!chatId) {
    return false;
  }
  const session = ensureSession(readSessionStore(), chatId);
  if (!session.status_card_message_id || !["working", "waiting"].includes(session.status_card_kind)) {
    return false;
  }
  clearSessionStatusTimer(chatId);
  await upsertSessionCard(config, chatId, status, flags, {
    phrase: phrase || pickStatusPhrase(status, session.status_card_phrase)
  });
  return true;
}

function getAppServerTurnKey(threadId, turnId) {
  return `${threadId}:${turnId}`;
}

function ensureAppServerTurnState(threadId, turnId, chatId = "") {
  if (!appServerRuntime) {
    return null;
  }
  const key = getAppServerTurnKey(threadId, turnId);
  if (!appServerRuntime.turnStates.has(key)) {
    appServerRuntime.turnStates.set(key, {
      threadId,
      turnId,
      chatId,
      buffer: "",
      sentMessages: [],
      fullText: "",
      planBuffer: "",
      fullPlanText: "",
      planItemIds: [],
      userMessages: [],
      flushTimer: null,
      planFlushTimer: null,
      lastAgentTextAt: 0,
      lastPlanSummary: ""
    });
  }
  const state = appServerRuntime.turnStates.get(key);
  if (chatId && !state.chatId) {
    state.chatId = chatId;
  }
  return state;
}

function clearTurnFlushTimer(turnState) {
  if (!turnState?.flushTimer) {
    return;
  }
  clearTimeout(turnState.flushTimer);
  turnState.flushTimer = null;
}

function clearTurnPlanFlushTimer(turnState) {
  if (!turnState?.planFlushTimer) {
    return;
  }
  clearTimeout(turnState.planFlushTimer);
  turnState.planFlushTimer = null;
}

function dropAppServerTurnState(threadId, turnId) {
  if (!appServerRuntime) {
    return;
  }
  const key = getAppServerTurnKey(threadId, turnId);
  const state = appServerRuntime.turnStates.get(key);
  if (state?.flushTimer) {
    clearTimeout(state.flushTimer);
  }
  if (state?.planFlushTimer) {
    clearTimeout(state.planFlushTimer);
  }
  appServerRuntime.turnStates.delete(key);
}

function migrateAppServerTurnState(threadId, fromTurnId, toTurnId, chatId = "") {
  if (!appServerRuntime || !threadId || !fromTurnId || !toTurnId || fromTurnId === toTurnId) {
    return null;
  }
  const fromKey = getAppServerTurnKey(threadId, fromTurnId);
  const toKey = getAppServerTurnKey(threadId, toTurnId);
  const fromState = appServerRuntime.turnStates.get(fromKey);
  const toState = appServerRuntime.turnStates.get(toKey);
  if (!fromState && !toState) {
    return null;
  }
  const merged = {
    threadId,
    turnId: toTurnId,
    chatId: chatId || toState?.chatId || fromState?.chatId || "",
    buffer: `${toState?.buffer || ""}${fromState?.buffer || ""}`,
    sentMessages: [...(toState?.sentMessages || []), ...(fromState?.sentMessages || [])],
    fullText: `${toState?.fullText || ""}${fromState?.fullText || ""}`,
    planBuffer: `${toState?.planBuffer || ""}${fromState?.planBuffer || ""}`,
    fullPlanText: `${toState?.fullPlanText || ""}${fromState?.fullPlanText || ""}`,
    planItemIds: [...new Set([...(toState?.planItemIds || []), ...(fromState?.planItemIds || [])])],
    userMessages: [...(toState?.userMessages || []), ...(fromState?.userMessages || [])],
    flushTimer: toState?.flushTimer || fromState?.flushTimer || null,
    planFlushTimer: toState?.planFlushTimer || fromState?.planFlushTimer || null,
    lastAgentTextAt: Math.max(toState?.lastAgentTextAt || 0, fromState?.lastAgentTextAt || 0),
    lastPlanSummary: toState?.lastPlanSummary || fromState?.lastPlanSummary || ""
  };
  appServerRuntime.turnStates.set(toKey, merged);
  if (fromState?.flushTimer && fromState.flushTimer !== merged.flushTimer) {
    clearTimeout(fromState.flushTimer);
  }
  if (fromState?.planFlushTimer && fromState.planFlushTimer !== merged.planFlushTimer) {
    clearTimeout(fromState.planFlushTimer);
  }
  appServerRuntime.turnStates.delete(fromKey);
  return merged;
}

async function flushTurnTextBuffer(config, flags, turnState, bufferKey, force = false) {
  if (!turnState?.chatId) {
    return;
  }
  while (turnState[bufferKey] && turnState.sentMessages?.length) {
    const leadingPunctuation = String(turnState[bufferKey] || "").match(/^\s*[\p{P}\p{S}]+\s*/u);
    if (!leadingPunctuation?.[0]?.trim()) {
      break;
    }
    const suffix = leadingPunctuation[0].trim();
    turnState.sentMessages[turnState.sentMessages.length - 1] = `${turnState.sentMessages[turnState.sentMessages.length - 1]}${suffix}`;
    turnState[bufferKey] = String(turnState[bufferKey] || "").slice(leadingPunctuation[0].length);
  }
  const { chunks, remaining } = splitNaturalMessageChunks(turnState[bufferKey], force);
  turnState[bufferKey] = remaining;
  if (!chunks.length) {
    return;
  }
  for (const chunk of chunks) {
    if (isPunctuationOnlyChunk(chunk)) {
      if (turnState.sentMessages.length) {
        turnState.sentMessages[turnState.sentMessages.length - 1] = `${turnState.sentMessages[turnState.sentMessages.length - 1]}${chunk}`;
      }
      continue;
    }
    const session = ensureSession(readSessionStore(), turnState.chatId);
    await sendIndexedTextMessage(config, turnState.chatId, chunk, session, flags);
    turnState.sentMessages.push(chunk);
    turnState.lastAgentTextAt = Date.now();
  }
}

async function flushTurnStateText(config, flags, turnState, force = false) {
  await flushTurnTextBuffer(config, flags, turnState, "buffer", force);
}

async function flushTurnPlanText(config, flags, turnState, force = false) {
  await flushTurnTextBuffer(config, flags, turnState, "planBuffer", force);
}

function scheduleTurnStateFlush(config, flags, turnState) {
  clearTurnFlushTimer(turnState);
  turnState.flushTimer = setTimeout(() => {
    flushTurnStateText(config, flags, turnState, true).catch((error) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "turn-state-flush-failed",
        chat_id: turnState.chatId,
        thread_id: turnState.threadId,
        turn_id: turnState.turnId,
        error: describeError(error)
      });
    });
  }, getProgressFlushMs(config, flags));
}

function scheduleTurnPlanFlush(config, flags, turnState) {
  clearTurnPlanFlushTimer(turnState);
  turnState.planFlushTimer = setTimeout(() => {
    flushTurnPlanText(config, flags, turnState, true).catch((error) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "turn-plan-flush-failed",
        chat_id: turnState.chatId,
        thread_id: turnState.threadId,
        turn_id: turnState.turnId,
        error: describeError(error)
      });
    });
  }, getProgressFlushMs(config, flags));
}

async function sendPlanSummaryUpdate(config, flags, chatId, plan, explanation, session) {
  if (!chatId) {
    return;
  }
  const summary = explanation
    ? `我先把这轮计划收束成 ${plan.length || 0} 步：${truncateText(explanation, 120)}`
    : `我在整理计划，目前大致分成 ${plan.length || 0} 步。`;
  await sendIndexedTextMessage(config, chatId, summary, session, flags);
}

async function archiveThreadQuietly(threadId) {
  if (!threadId || !appServerRuntime?.client) {
    return;
  }
  try {
    await appServerRuntime.client.request("thread/archive", { threadId });
  } catch (error) {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "thread-archive-failed",
      thread_id: threadId,
      error: describeError(error)
    });
  }
}

function buildCollaborationModePayload(config, mode) {
  if (normalizeMode(mode) !== "plan") {
    return null;
  }
  return {
    mode: "plan",
    settings: {
      model: String(config.automation?.codex_model || "gpt-5.4"),
      reasoning_effort: config.automation?.codex_reasoning_effort || null,
      developer_instructions: null
    }
  };
}

async function ensureAppServerRuntime(config, flags) {
  if (appServerRuntime?.client) {
    await appServerRuntime.client.ensureStarted();
    return appServerRuntime;
  }
  const client = new CodexAppServerClient(config);
  appServerRuntime = {
    client,
    turnStates: new Map()
  };
  client.on("notification", (payload) => {
    if (payload?.method !== "item/agentMessage/delta") {
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "app-server.notification",
        ...summarizeAppServerEnvelope(payload)
      });
    }
    handleAppServerNotification(config, flags, payload).catch((error) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "app-server-notification-failed",
        method: payload?.method || "",
        error: describeError(error)
      });
    });
  });
  client.on("server-request", (payload) => {
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "app-server.server-request",
      ...summarizeAppServerEnvelope(payload)
    });
    handleAppServerServerRequest(config, flags, payload).catch((error) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "app-server-server-request-failed",
        method: payload?.method || "",
        error: describeError(error)
      });
    });
  });
  client.on("exit", () => {
    const store = readSessionStore();
    for (const session of Object.values(store.chats || {})) {
      session.active_turn_id = "";
      session.provisional_turn_id = "";
      session.pending_request_id = "";
      session.status = "idle";
      session.interrupted_at = new Date().toISOString();
    }
    writeSessionStore(store);
  });
  await client.ensureStarted();
  return appServerRuntime;
}

function normalizeCardActionPayload(data) {
  const source = data?.event && typeof data.event === "object" ? data.event : data;
  const action = source?.action && typeof source.action === "object" ? source.action : {};
  const value = action?.value && typeof action.value === "object" ? action.value : {};
  return {
    open_id: source?.open_id || source?.operator?.open_id || source?.user?.open_id || "",
    user_id: source?.user_id || source?.operator?.user_id || source?.user?.user_id || "",
    tenant_key: source?.tenant_key || data?.tenant_key || data?.header?.tenant_key || "",
    open_message_id: source?.open_message_id || source?.open_message?.open_message_id || "",
    token: source?.token || data?.token || "",
    action: {
      ...action,
      value
    }
  };
}

async function handleCardAction(data, config, flags) {
  const normalized = normalizeCardActionPayload(data);
  const value = normalized.action?.value || {};
  if (value.action !== "request-user-input-answer") {
    return buildStatusCard(pickStatusPhrase("working"), { status: "working" });
  }
  const requestEntry = readPendingRequest(value.request_id);
  if (!requestEntry) {
    return buildCurrentSessionStatusCard(value.chat_id || "");
  }
  const answers = {
    ...(requestEntry.answers || {}),
    [value.question_id]: {
      answers: [String(value.answer || "").trim()].filter(Boolean)
    }
  };
  const updated = upsertPendingRequest(requestEntry.request_id, {
    ...requestEntry,
    answers
  });
  const allAnswered = (updated.questions || []).every((question) => answers?.[question.id]?.answers?.length);
  if (allAnswered) {
    removePendingRequest(updated.request_id);
    const phrase = pickStatusPhrase("working");
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "request-user-input.answer",
      source: "card",
      chat_id: updated.chat_id,
      request_id: updated.request_id,
      question_ids: Object.keys(answers),
      answer_count: Object.keys(answers).length
    });
    updateSessionState(updated.chat_id, {
      pending_request_id: "",
      status: "working",
      status_card_kind: "working",
      status_card_phrase: phrase
    });
    upsertSessionCard(config, updated.chat_id, "working", flags, {
      phrase
    }).catch((error) => {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "card-action-working-patch-failed",
        chat_id: updated.chat_id,
        request_id: updated.request_id,
        error: String(error)
      });
    });
    appServerRuntime.client.respond(updated.rpc_id, buildQuestionAnswerPayload(updated.questions, answers))
      .catch((error) => {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "card-action-respond-failed",
          chat_id: updated.chat_id,
          request_id: updated.request_id,
          error: String(error)
        });
      });
    scheduleSessionCardRefresh(config, flags, updated.chat_id);
    return buildStatusCard(phrase, { status: "working" });
  }
  updateSessionState(updated.chat_id, {
    pending_request_id: updated.request_id,
    status: "waiting",
    status_card_kind: "waiting"
  });
  upsertSessionCard(config, updated.chat_id, "waiting", flags, {
    pendingRequest: updated
  }).catch((error) => {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "card-action-waiting-patch-failed",
      chat_id: updated.chat_id,
      request_id: updated.request_id,
      error: String(error)
    });
  });
  scheduleSessionCardRefresh(config, flags, updated.chat_id);
  return buildStatusCard(
    pickStatusPhrase("waiting"),
    {
      status: "waiting",
      questions: updated.questions || [],
      answers,
      requestId: updated.request_id,
      chatId: updated.chat_id,
      allowActions: supportsCardActions(config, flags)
    }
  );
}

async function handleCardActionTrigger(data, config, flags) {
  if (!shouldEnableCardCallbacks(config, flags)) {
    return {
      toast: {
        type: "warning",
        content: "卡片交互暂未启用"
      }
    };
  }
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: "card.action.trigger",
    event: normalizeCardActionPayload(data)
  });
  return handleCardAction(data, config, flags);
}

async function maybeResolvePendingRequestFromText(event, config, flags) {
  const store = readSessionStore();
  const session = ensureSession(store, event.chat_id);
  if (!session.pending_request_id) {
    return false;
  }
  const requestEntry = readPendingRequest(session.pending_request_id);
  if (!requestEntry) {
    updateSessionState(event.chat_id, { pending_request_id: "", status: "idle" });
    return false;
  }
  const answers = { ...(requestEntry.answers || {}) };
  let answeredQuestion = "";
  for (const question of requestEntry.questions || []) {
    if (answers?.[question.id]?.answers?.length) {
      continue;
    }
    const mapped = tryMapTextToQuestionAnswer(question, event.text);
    if (!mapped) {
      continue;
    }
    answers[question.id] = mapped;
    answeredQuestion = question.id;
    break;
  }
  if (!answeredQuestion) {
    return false;
  }
  if (session.thread_id && session.active_turn_id) {
    const turnState = ensureAppServerTurnState(session.thread_id, session.active_turn_id, event.chat_id);
    turnState?.userMessages?.push(event);
  }
  const updated = upsertPendingRequest(requestEntry.request_id, {
    ...requestEntry,
    answers
  });
  const allAnswered = (updated.questions || []).every((question) => answers?.[question.id]?.answers?.length);
  if (allAnswered) {
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "request-user-input.answer",
      source: "text",
      chat_id: updated.chat_id,
      request_id: updated.request_id,
      question_ids: Object.keys(answers),
      answer_count: Object.keys(answers).length
    });
    await appServerRuntime.client.respond(updated.rpc_id, buildQuestionAnswerPayload(updated.questions, answers));
    removePendingRequest(updated.request_id);
    updateSessionState(event.chat_id, {
      pending_request_id: "",
      status: "working",
      last_user_message_at: event.received_at
    });
    await upsertSessionCard(config, event.chat_id, "working", flags);
    scheduleSessionCardRefresh(config, flags, event.chat_id);
    return true;
  }
  await upsertSessionCard(config, event.chat_id, "waiting", flags, {
    pendingRequest: updated
  });
  scheduleSessionCardRefresh(config, flags, event.chat_id);
  const remaining = (updated.questions || []).filter((question) => !answers?.[question.id]?.answers?.length).length;
  await sendIndexedTextMessage(
    config,
    event.chat_id,
    `这个答案我先记下了，还差 ${remaining} 个选择。你可以继续回文字，也可以点卡片按钮。`,
    ensureSession(readSessionStore(), event.chat_id),
    flags
  );
  updateSessionState(event.chat_id, {
    last_user_message_at: event.received_at
  });
  await clearTypingReactions(config, [event], flags);
  return true;
}

async function handleAppServerServerRequest(config, flags, payload) {
  if (!payload?.method) {
    return;
  }
  if (payload.method === "item/tool/requestUserInput") {
    const chatId = findChatIdByThreadId(payload.params?.threadId);
    if (!chatId) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "request-user-input-no-chat",
        request_id: payload.id,
        thread_id: payload.params?.threadId || ""
      });
      return;
    }
    const requestId = normalizePendingRequestId(payload.id);
    const requestEntry = upsertPendingRequest(requestId, {
      request_id: requestId,
      rpc_id: payload.id,
      chat_id: chatId,
      thread_id: payload.params.threadId,
      turn_id: payload.params.turnId,
      item_id: payload.params.itemId,
      questions: payload.params.questions || [],
      answers: {},
      created_at: new Date().toISOString()
    });
    updateSessionState(chatId, {
      pending_request_id: requestId,
      status: "waiting"
    });
    await upsertSessionCard(config, chatId, "waiting", flags, {
      pendingRequest: requestEntry
    });
    scheduleSessionCardRefresh(config, flags, chatId);
    await sendIndexedTextMessage(
      config,
      chatId,
      "我这里需要你拍一个板。卡片里点一下最快，直接回文字也可以。",
      ensureSession(readSessionStore(), chatId),
      flags
    );
    return;
  }
  if (payload.method === "item/commandExecution/requestApproval") {
    await appServerRuntime.client.respond(payload.id, { decision: "accept" });
    return;
  }
  if (payload.method === "item/fileChange/requestApproval") {
    await appServerRuntime.client.respond(payload.id, { decision: "accept" });
    return;
  }
  if (payload.method === "execCommandApproval") {
    await appServerRuntime.client.respond(payload.id, { decision: "approved" });
    return;
  }
  if (payload.method === "applyPatchApproval") {
    await appServerRuntime.client.respond(payload.id, { decision: "approved" });
    return;
  }
  appendJsonl(errorsPath, {
    timestamp: new Date().toISOString(),
    type: "unsupported-app-server-request",
    method: payload.method,
    request_id: payload.id
  });
}

async function handleAppServerNotification(config, flags, payload) {
  const threadId = payload?.params?.threadId || payload?.params?.thread?.id || "";
  const turnId = payload?.params?.turnId || payload?.params?.turn?.id || "";
  const chatId = findChatIdByThreadId(threadId);
  if (payload.method === "turn/started" && chatId) {
    const startedTurnId = payload.params.turn.id;
    const store = readSessionStore();
    const session = ensureSession(store, chatId);
    const provisionalTurnId = session.provisional_turn_id || "";
    if (provisionalTurnId && provisionalTurnId !== startedTurnId) {
      const migrated = migrateAppServerTurnState(threadId, provisionalTurnId, startedTurnId, chatId);
      const rewrittenMessages = rewriteIndexedTurnBindings(threadId, provisionalTurnId, startedTurnId);
      const rewrittenRequests = rewritePendingRequestTurnBindings(provisionalTurnId, startedTurnId);
      appendJsonl(eventsPath, {
        timestamp: new Date().toISOString(),
        type: "app-server.turn-id-reconciled",
        chat_id: chatId,
        thread_id: threadId,
        provisional_turn_id: provisionalTurnId,
        authoritative_turn_id: startedTurnId,
        migrated_runtime: Boolean(migrated),
        rewritten_messages: rewrittenMessages,
        rewritten_requests: rewrittenRequests
      });
    }
    session.active_turn_id = startedTurnId;
    session.provisional_turn_id = "";
    session.status = "working";
    session.updated_at = new Date().toISOString();
    writeSessionStore(store);
    return;
  }
  if (payload.method === "item/agentMessage/delta") {
    const turnState = ensureAppServerTurnState(threadId, turnId, chatId);
    if (!turnState) {
      return;
    }
    turnState.buffer += payload.params.delta || "";
    turnState.fullText += payload.params.delta || "";
    await flushTurnStateText(config, flags, turnState, false);
    scheduleTurnStateFlush(config, flags, turnState);
    return;
  }
  if (payload.method === "item/plan/delta") {
    const turnState = ensureAppServerTurnState(threadId, turnId, chatId);
    if (!turnState) {
      return;
    }
    if (payload.params?.itemId && !turnState.planItemIds.includes(payload.params.itemId)) {
      turnState.planItemIds.push(payload.params.itemId);
    }
    turnState.planBuffer += payload.params.delta || "";
    turnState.fullPlanText += payload.params.delta || "";
    await flushTurnPlanText(config, flags, turnState, false);
    scheduleTurnPlanFlush(config, flags, turnState);
    return;
  }
  if (payload.method === "turn/plan/updated" && chatId) {
    const turnState = ensureAppServerTurnState(threadId, turnId, chatId);
    const planSummary = JSON.stringify(payload.params.plan || []);
    if (
      turnState
      && turnState.lastPlanSummary !== planSummary
      && !turnState.fullPlanText.trim()
    ) {
      const session = ensureSession(readSessionStore(), chatId);
      await sendPlanSummaryUpdate(config, flags, chatId, payload.params.plan || [], payload.params.explanation || "", session);
      turnState.lastPlanSummary = planSummary;
    } else if (turnState) {
      turnState.lastPlanSummary = planSummary;
    }
    return;
  }
  if (payload.method === "item/completed") {
    const itemId = payload.params?.itemId || payload.params?.item?.id || "";
    const turnState = ensureAppServerTurnState(threadId, turnId, chatId);
    if (turnState && (turnState.planItemIds.includes(itemId) || itemId.endsWith("-plan"))) {
      await flushTurnPlanText(config, flags, turnState, true);
    }
    return;
  }
  if (payload.method === "turn/completed") {
    const completedTurn = payload.params.turn || {};
    const completedThreadId = payload.params.threadId || threadId;
    const completedTurnId = completedTurn.id || turnId;
    const existingTurnState = appServerRuntime?.turnStates?.get(getAppServerTurnKey(completedThreadId, completedTurnId)) || null;
    const finalChatId = chatId || existingTurnState?.chatId || findChatIdByThreadId(completedThreadId);
    if (!finalChatId) {
      dropAppServerTurnState(completedThreadId, completedTurnId);
      return;
    }
    const turnState = existingTurnState || ensureAppServerTurnState(completedThreadId, completedTurnId, finalChatId);
    await flushTurnStateText(config, flags, turnState, true);
    await flushTurnPlanText(config, flags, turnState, true);
    const replyText = turnState?.sentMessages?.join("\n").trim()
      || turnState?.fullText?.trim()
      || turnState?.fullPlanText?.trim()
      || "";
    const sessionStore = readSessionStore();
    const session = ensureSession(sessionStore, finalChatId);
    const shouldReconcileProvisionalOnCompletion = Boolean(
      session.provisional_turn_id
      && session.active_turn_id === session.provisional_turn_id
      && session.thread_id === completedThreadId
      && session.provisional_turn_id !== completedTurnId
    );
    if (shouldReconcileProvisionalOnCompletion) {
      migrateAppServerTurnState(completedThreadId, session.provisional_turn_id, completedTurnId, finalChatId);
      rewriteIndexedTurnBindings(completedThreadId, session.provisional_turn_id, completedTurnId);
      rewritePendingRequestTurnBindings(session.provisional_turn_id, completedTurnId);
    }
    const sessionPendingRequest = readPendingRequest(session.pending_request_id);
    const shouldClearSessionPending = sessionPendingRequest?.turn_id === completedTurnId;
    const shouldApplySessionUpdate = shouldApplyTurnCompletionToSession(session, completedTurnId) || shouldReconcileProvisionalOnCompletion;
    removePendingRequestsForTurn(completedTurnId);
    if (shouldApplySessionUpdate) {
      session.active_turn_id = "";
      session.provisional_turn_id = "";
      if (shouldClearSessionPending || !sessionPendingRequest) {
        session.pending_request_id = "";
      }
      session.status = "idle";
      session.last_completed_at = new Date().toISOString();
      session.last_thread_preview = truncateText(replyText || session.last_thread_preview || "", 180);
      writeSessionStore(sessionStore);
      if (completedTurn.status === "failed") {
        const text = replyText || "这轮中途断了一下，但上下文我还记着。你继续补一句，我就从这里接上。";
        if (!replyText && finalChatId) {
          await sendIndexedTextMessage(config, finalChatId, text, session, flags);
        }
        await upsertSessionCard(config, finalChatId, "error", flags, {
          phrase: pickStatusPhrase("error", session.status_card_phrase)
        });
      } else if (completedTurn.status === "interrupted") {
        const text = replyText || "这一轮我先按下暂停键。你给我新方向后，我就从新的 turn 继续。";
        if (!replyText && finalChatId) {
          await sendIndexedTextMessage(config, finalChatId, text, session, flags);
        }
        await upsertSessionCard(config, finalChatId, "done", flags, {
          phrase: pickStatusPhrase("done", session.status_card_phrase)
        });
      } else {
        if (!replyText) {
          await sendIndexedTextMessage(config, finalChatId, "这轮已经处理完了，你可以继续补充下一步。", session, flags);
        }
        await upsertSessionCard(config, finalChatId, "done", flags, {
          phrase: pickStatusPhrase("done", session.status_card_phrase)
        });
      }
      clearSessionStatusTimer(finalChatId);
    } else if (shouldClearSessionPending || (session.pending_request_id && !sessionPendingRequest) || session.provisional_turn_id === completedTurnId) {
      if (shouldClearSessionPending || (session.pending_request_id && !sessionPendingRequest)) {
        session.pending_request_id = "";
      }
      if (session.provisional_turn_id === completedTurnId) {
        session.provisional_turn_id = "";
      }
      writeSessionStore(sessionStore);
    }
    try {
      if (turnState?.userMessages?.length) {
        await clearTypingReactions(config, turnState.userMessages, flags);
      }
      if (completedTurn.status !== "failed" && completedTurn.status !== "interrupted" && turnState?.userMessages?.length && replyText) {
        syncInteractionMemory(finalChatId, turnState.userMessages, replyText);
      }
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "turn-completed-followup-failed",
        chat_id: finalChatId,
        turn_id: completedTurn.id || "",
        error: describeError(error)
      });
    }
    dropAppServerTurnState(completedThreadId, completedTurnId);
  }
}

async function ensureThreadForEvent(event, session, config, flags, modeOverride = "", forceNewThread = false) {
  const runtime = await ensureAppServerRuntime(config, flags);
  const client = runtime.client;
  const referencedBinding = forceNewThread ? null : resolveReferencedThreadBinding(event);
  let threadId = referencedBinding?.thread_id || "";
  const sessionExpired = isSessionExpired(session, config, flags);
  const startingNewThread = forceNewThread || (!threadId && (!session.thread_id || sessionExpired));
  if (forceNewThread && session.thread_id) {
    await archiveThreadQuietly(session.thread_id);
  }
  if (!forceNewThread && !threadId && session.thread_id && !sessionExpired) {
    threadId = session.thread_id;
  }
  if (!forceNewThread && !threadId && session.thread_id && sessionExpired) {
    await archiveThreadQuietly(session.thread_id);
  }
  if (!threadId) {
    const started = await client.request("thread/start", {
      model: String(config.automation?.codex_model || "gpt-5.4"),
      cwd: repoRoot,
      approvalPolicy: "never",
      sandbox: "danger-full-access",
      persistExtendedHistory: true,
      experimentalRawEvents: false,
      ephemeral: false
    });
    threadId = started.thread.id;
  } else {
    await client.request("thread/resume", {
      threadId,
      cwd: repoRoot,
      approvalPolicy: "never",
      sandbox: "danger-full-access",
      persistExtendedHistory: true
    });
  }
  updateSessionState(event.chat_id, {
    thread_id: threadId,
    mode: normalizeMode(modeOverride || session.mode || getDefaultCollaborationMode(config)),
    last_referenced_message_id: forceNewThread ? "" : referencedBinding?.message_id || "",
    status: "working"
  });
  return {
    client,
    threadId,
    startingNewThread
  };
}

async function startAppServerTurnForEvent(event, config, flags, session, modeOverride = "", forceNewThread = false) {
  const desiredMode = normalizeMode(modeOverride || session.mode || getDefaultCollaborationMode(config));
  const { client, threadId, startingNewThread } = await ensureThreadForEvent(event, session, config, flags, desiredMode, forceNewThread);
  const liveProbe = await gatherLiveProbe(config, flags, event.chat_id, [event]);
  let memorySnapshot = "- 无";
  try {
    memorySnapshot = getMemoryPromptSnapshot(event.chat_id) || "- 无";
  } catch (error) {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "memory-snapshot-failed",
      chat_id: event.chat_id,
      message_id: event.message_id,
      error: describeError(error)
    });
  }
  const latestSession = ensureSession(readSessionStore(), event.chat_id);
  const contextText = buildFeishuContextBlock({
    chatId: event.chat_id,
    messageText: event.text,
    session: latestSession,
    liveProbeSummary: liveProbe.summary,
    memorySnapshot,
    event,
    previousThreadPreview: startingNewThread ? latestSession.last_thread_preview || "" : "",
    mode: desiredMode,
    isSteer: false
  });
  const turn = await client.request("turn/start", {
    threadId,
    input: buildTurnInputItems(contextText),
    model: String(config.automation?.codex_model || "gpt-5.4"),
    effort: config.automation?.codex_reasoning_effort || null,
    collaborationMode: buildCollaborationModePayload(config, desiredMode)
  });
  await settleSessionCardIfNeeded(config, flags, event.chat_id, "done");
  const provisionalTurnId = turn.turn.id;
  const currentSession = ensureSession(readSessionStore(), event.chat_id);
  const authoritativeTurnId = (currentSession.thread_id === threadId && currentSession.active_turn_id && currentSession.active_turn_id !== provisionalTurnId)
    ? currentSession.active_turn_id
    : provisionalTurnId;
  if (authoritativeTurnId !== provisionalTurnId) {
    migrateAppServerTurnState(threadId, provisionalTurnId, authoritativeTurnId, event.chat_id);
  }
  const turnState = ensureAppServerTurnState(threadId, authoritativeTurnId, event.chat_id);
  turnState.userMessages.push(event);
  indexMessageToThread(event.message_id, {
    chat_id: event.chat_id,
    thread_id: threadId,
    turn_id: authoritativeTurnId,
    message_type: "user"
  });
  updateSessionState(event.chat_id, {
    thread_id: threadId,
    active_turn_id: authoritativeTurnId,
    provisional_turn_id: authoritativeTurnId === provisionalTurnId ? provisionalTurnId : "",
    mode: desiredMode,
    status: "working",
    last_user_message_at: event.received_at,
    status_card_message_id: "",
    status_card_kind: "",
    status_card_phrase: ""
  });
  await upsertSessionCard(config, event.chat_id, "working", flags);
  scheduleSessionCardRefresh(config, flags, event.chat_id);
}

async function steerAppServerTurn(event, config, flags, session) {
  const runtime = await ensureAppServerRuntime(config, flags);
  const turnState = ensureAppServerTurnState(session.thread_id, session.active_turn_id, event.chat_id);
  turnState.userMessages.push(event);
  const contextText = buildFeishuContextBlock({
    chatId: event.chat_id,
    messageText: event.text,
    session,
    liveProbeSummary: "- 这是一条工作中追加的飞书消息，不重新跑实时快照。",
    memorySnapshot: "- 继续沿用当前 turn 已经持有的上下文。",
    event,
    mode: session.mode || "default",
    isSteer: true
  });
  try {
    await runtime.client.request("turn/steer", {
      threadId: session.thread_id,
      expectedTurnId: session.active_turn_id,
      input: buildTurnInputItems(contextText)
    });
  } catch (error) {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "turn-steer-failed",
      chat_id: event.chat_id,
      thread_id: session.thread_id,
      turn_id: session.active_turn_id,
      error: describeError(error)
    });
    return false;
  }
  indexMessageToThread(event.message_id, {
    chat_id: event.chat_id,
    thread_id: session.thread_id,
    turn_id: session.active_turn_id,
    message_type: "user"
  });
  updateSessionState(event.chat_id, {
    last_user_message_at: event.received_at
  });
  return true;
}

async function interruptAndRestartTurn(event, config, flags, session, nextMode, remainderText, forceNewThread = false) {
  const runtime = await ensureAppServerRuntime(config, flags);
  if (session.thread_id && session.active_turn_id) {
    try {
      await runtime.client.request("turn/interrupt", {
        threadId: session.thread_id,
        turnId: session.active_turn_id
      });
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "turn-interrupt-failed",
        chat_id: event.chat_id,
        thread_id: session.thread_id,
        turn_id: session.active_turn_id,
        error: describeError(error)
      });
    }
  }
  await settleSessionCardIfNeeded(config, flags, event.chat_id, "done");
  updateSessionState(event.chat_id, {
    active_turn_id: "",
    provisional_turn_id: "",
    mode: nextMode,
    interrupted_at: new Date().toISOString(),
    last_user_message_at: event.received_at
  });
  if (!remainderText) {
    if (forceNewThread && session.thread_id) {
      await archiveThreadQuietly(session.thread_id);
    }
    const updatedSession = updateSessionState(event.chat_id, {
      thread_id: forceNewThread ? "" : session.thread_id || "",
      last_referenced_message_id: forceNewThread ? "" : session.last_referenced_message_id || "",
      status: "idle"
    });
    indexMessageToThread(event.message_id, {
      chat_id: event.chat_id,
      thread_id: forceNewThread ? "" : session.thread_id || "",
      turn_id: "",
      message_type: "user"
    });
    await sendIndexedTextMessage(
      config,
      event.chat_id,
      buildDirectiveAckText(nextMode, forceNewThread),
      updatedSession,
      flags
    );
    await clearTypingReactions(config, [event], flags);
    return;
  }
  const nextEvent = {
    ...event,
    text: remainderText
  };
  await startAppServerTurnForEvent(nextEvent, config, flags, ensureSession(readSessionStore(), event.chat_id), nextMode, forceNewThread);
}

async function processEventWithAppServer(event, config, flags) {
  if (!flags["spawn-codex"]) {
    return;
  }
  await ensureAppServerRuntime(config, flags);
  const modeDirective = extractModeDirective(event.text);
  const session = ensureSession(readSessionStore(), event.chat_id);
  if (await maybeResolvePendingRequestFromText(event, config, flags)) {
    indexMessageToThread(event.message_id, {
      chat_id: event.chat_id,
      thread_id: session.thread_id || "",
      turn_id: session.active_turn_id || "",
      message_type: "user"
    });
    return;
  }
  if (isTurnActive(session)) {
    if (modeDirective.mode || modeDirective.newThread) {
      await interruptAndRestartTurn(
        event,
        config,
        flags,
        session,
        modeDirective.mode || session.mode || getDefaultCollaborationMode(config),
        modeDirective.remainder,
        modeDirective.newThread
      );
      return;
    }
    const steered = await steerAppServerTurn(event, config, flags, session);
    if (steered) {
      return;
    }
    await settleSessionCardIfNeeded(config, flags, event.chat_id, "done");
    updateSessionState(event.chat_id, {
      active_turn_id: "",
      provisional_turn_id: "",
      pending_request_id: "",
      status: "idle",
      interrupted_at: new Date().toISOString(),
      last_user_message_at: event.received_at
    });
    await startAppServerTurnForEvent(event, config, flags, ensureSession(readSessionStore(), event.chat_id), session.mode || "", false);
    return;
  }
  if ((modeDirective.mode || modeDirective.newThread) && !modeDirective.remainder) {
    if (modeDirective.newThread && session.thread_id) {
      await archiveThreadQuietly(session.thread_id);
    }
    indexMessageToThread(event.message_id, {
      chat_id: event.chat_id,
      thread_id: modeDirective.newThread ? "" : session.thread_id || "",
      turn_id: "",
      message_type: "user"
    });
    const updatedSession = updateSessionState(event.chat_id, {
      thread_id: modeDirective.newThread ? "" : session.thread_id || "",
      mode: modeDirective.mode || session.mode || getDefaultCollaborationMode(config),
      last_user_message_at: event.received_at,
      last_referenced_message_id: modeDirective.newThread ? "" : session.last_referenced_message_id || "",
      status: "idle"
    });
    await sendIndexedTextMessage(
      config,
      event.chat_id,
      buildDirectiveAckText(modeDirective.mode || session.mode || getDefaultCollaborationMode(config), modeDirective.newThread),
      updatedSession,
      flags
    );
    await upsertSessionCard(config, event.chat_id, "done", flags, {
      phrase: pickStatusPhrase("done")
    });
    await clearTypingReactions(config, [event], flags);
    return;
  }
  const effectiveEvent = (modeDirective.mode || modeDirective.newThread)
    ? {
      ...event,
      text: modeDirective.remainder
    }
    : event;
  if (modeDirective.mode || modeDirective.newThread) {
    updateSessionState(event.chat_id, {
      mode: modeDirective.mode || session.mode || getDefaultCollaborationMode(config)
    });
  }
  await startAppServerTurnForEvent(
    effectiveEvent,
    config,
    flags,
    ensureSession(readSessionStore(), event.chat_id),
    modeDirective.mode || "",
    modeDirective.newThread
  );
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

function buildCodexPrompt(
  chatId,
  batchMessages,
  continuationMessages,
  memorySnapshot,
  liveProbeSummary,
  recalledHistoryMessages = [],
  referencedReplies = []
) {
  const continuationLines = continuationMessages.map(formatHistoryLine).join("\n");
  const recalledHistoryLines = recalledHistoryMessages.map(formatHistoryLine).join("\n");
  const referencedReplyLines = referencedReplies
    .map((item) => `- ${truncateText(item.message_id, 24)}: ${truncateText(item.reply || "", 220)}`)
    .join("\n");
  const batchLines = batchMessages.map((item, index) => buildBatchLine(item, index)).join("\n");
  return [
    "你是 InStreet 上的派蒙 paimon_insight。",
    "派蒙，你正在通过飞书和用户连续协作。",
    "把 state/current/memory_store.json、config/paimon.json 和 state/current 下的最新状态视为主记忆来源。",
    "忽略 tmp/、旧回复缓存、旧批次日志、历史实验残留，除非用户这轮明确重新提出。",
    "这不是逐条客服对话，而是一个持续工作会话。",
    "如果用户在短时间内连续发来多条消息，请把它们理解为同一轮请求的补充信息，统一回复。",
    "只输出飞书回复正文，不要标题，不要引号，不要解释你如何生成。",
    "回复要求：简洁但有信息量，优先回应最新问题，同时吸收前面消息里的补充约束。",
    "除非用户明确要求，否则不要在飞书回复里输出 Markdown 链接、文件路径、行号、配置键名或代码定位；优先用自然语言说明结论、进度和下一步。",
    "不要把跨小时、跨天的旧聊天原文当作默认主上下文；需要长期保留的信息应来自全局记忆快照，而不是历史原文。",
    "",
    `当前 chat_id: ${chatId}`,
    "",
    "全局统一记忆快照：",
    memorySnapshot || "- 无",
    "",
    "短连续性上下文：",
    continuationLines || "- 无",
    "",
    "显式回捞的历史原文：",
    recalledHistoryLines || "- 无",
    "",
    "本轮消息直接回复/引用的历史回复：",
    referencedReplyLines || "- 无",
    "",
    "实时平台探针：",
    liveProbeSummary || "- 无",
    "",
    "本轮合并待处理消息：",
    batchLines || "- 无",
    "",
    "如果用户是在追问正在执行的工作，请明确说清当前结果、卡点和下一步。",
    "如果用户只是连续发了几个补充短句，要把它们整合成一次自然回复。",
    "如果全局记忆里已经有长期偏好或稳定约束，要执行它们；如果这轮出现新的长期偏好，请在回复里顺带体现已经吸收。",
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
    if (typeof hooks.onSpawn === "function") {
      Promise.resolve(hooks.onSpawn({
        pid: child.pid || 0,
        outputFile
      })).catch(() => {});
    }

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
      "目前我的理论旗舰《AI社区意识形态分析》已经完结，也在文学社开启了新的长篇言情连载《全宇宙都在围观我和竹马热恋》，同时会继续写技术方法贴。",
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
  return "派蒙还在思考这轮消息。你先不用重复发，我会继续回复在同一条消息里。";
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

function recoverPersistedProcessingOnStartup(queue, chatId) {
  const chat = ensureChatQueue(queue, chatId);
  if (!chat.processing) {
    return false;
  }
  const recovered = recoverProcessingItems(chatId, chat.processing);
  const codexPid = Number(chat.processing?.codex_pid || 0);
  const stopped = stopPidBestEffort(codexPid);
  chat.pending = dedupeMessages(recovered.concat(chat.pending));
  chat.processing = null;
  chat.updated_at = new Date().toISOString();
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: "processing-recovered-on-startup",
    chat_id: chatId,
    recovered_message_ids: recovered.map((item) => item.message_id),
    stopped_codex_pid: stopped ? codexPid : 0,
    pending_message_ids: chat.pending.map((item) => item.message_id)
  });
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
      chat_type: event.chat_type || "",
      text: event.text,
      received_at: event.received_at,
      source: event.source || "realtime",
      sender: event.sender || {},
      sender_type: event.sender_type || "",
      message_type: event.message_type || "text",
      thread_id: event.thread_id || "",
      parent_id: event.parent_id || "",
      root_id: event.root_id || "",
      mentions: Array.isArray(event.mentions) ? event.mentions : []
    });
    chat.updated_at = new Date().toISOString();
    writeQueue(queue);
  }
}

function normalizeCommandText(text) {
  return String(text || "").trim().replace(/\s+/g, " ");
}

function getConfiguredCommands(config, key, fallback) {
  const configured = config.automation?.[key];
  if (Array.isArray(configured)) {
    return configured.map((item) => normalizeCommandText(item)).filter(Boolean);
  }
  if (typeof configured === "string") {
    const text = normalizeCommandText(configured);
    return text ? [text] : fallback;
  }
  return fallback;
}

function getReportBindCommands(config) {
  return getConfiguredCommands(config, "feishu_report_bind_commands", [
    "#绑定运维群",
    "绑定运维群",
    "#bind-report-group",
    "/bind-report-group"
  ]);
}

function getReportUnbindCommands(config) {
  return getConfiguredCommands(config, "feishu_report_unbind_commands", [
    "#解绑运维群",
    "解绑运维群",
    "#clear-report-group",
    "/clear-report-group"
  ]);
}

function buildReportTargetBindingReply(event) {
  const targetLabel = event.chat_type === "group" ? "当前群" : "当前会话";
  return `${targetLabel}已绑定为飞书运维汇报目标。后续 heartbeat 和长任务进度会发到这里。`;
}

function buildReportTargetClearedReply(event) {
  const targetLabel = event.chat_type === "group" ? "当前群" : "当前会话";
  return `${targetLabel}的飞书运维汇报绑定已清除。后续 heartbeat 不会再自动发到这里，直到重新绑定。`;
}

function buildReportTargetState(config, options = {}) {
  return {
    version: 1,
    app_id: config.feishu.app_id,
    receive_id_type: options.receive_id_type || "chat_id",
    receive_id: options.receive_id || "",
    chat_type: options.chat_type || "",
    label: options.label || "",
    source: options.source || "manual",
    bound_at: options.bound_at || new Date().toISOString(),
    updated_at: new Date().toISOString(),
    bind_message_id: options.bind_message_id || "",
    bound_by: {
      user_id: options.user_id || "",
      open_id: options.open_id || "",
      union_id: options.union_id || ""
    }
  };
}

async function maybeHandleReportTargetCommand(event, config) {
  if (event.source === "history-sync") {
    return false;
  }
  const text = normalizeCommandText(event.text);
  if (!text) {
    return false;
  }
  if (getReportBindCommands(config).includes(text)) {
    const state = buildReportTargetState(config, {
      receive_id: event.chat_id,
      chat_type: event.chat_type || "",
      source: "message-command",
      bind_message_id: event.message_id,
      user_id: event.sender?.user_id || "",
      open_id: event.sender?.open_id || "",
      union_id: event.sender?.union_id || ""
    });
    writeReportTargetState(state);
    await sendTextMessage(config, "chat_id", event.chat_id, buildReportTargetBindingReply(event));
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "feishu-report-target.bind",
      chat_id: event.chat_id,
      message_id: event.message_id,
      target: state
    });
    return true;
  }
  if (getReportUnbindCommands(config).includes(text)) {
    clearReportTargetState();
    await sendTextMessage(config, "chat_id", event.chat_id, buildReportTargetClearedReply(event));
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: "feishu-report-target.clear",
      chat_id: event.chat_id,
      message_id: event.message_id
    });
    return true;
  }
  return false;
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
    gateway_pid: process.pid,
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
    const continuationMessages = readRecentContinuation(chatId, batchMessages, config, flags);
    let recalledHistory = [];
    if (isHistoryRawPromptEnabled(config, flags)) {
      try {
        const historyItems = await fetchChatMessages(config, chatId, Math.max(getHistoryLimit(config, flags) * 2, 20), flags);
        recalledHistory = historyItems
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
    }
    let memorySnapshot = "- 无";
    try {
      memorySnapshot = getMemoryPromptSnapshot(chatId) || "- 无";
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "memory-snapshot-failed",
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
    const referencedReplies = collectReferencedReplies(batchMessages);
    const prompt = buildCodexPrompt(
      chatId,
      batchMessages,
      continuationMessages,
      memorySnapshot,
      liveProbe.summary,
      recalledHistory,
      referencedReplies
    );
    let replyText = "";
    try {
      replyText = await runCodexPrompt(prompt, config, flags, {
        onSpawn: ({ pid, outputFile }) => {
          updateChatProcessing(chatId, {
            codex_pid: pid,
            codex_output_file: outputFile
          });
        },
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
    try {
      syncInteractionMemory(chatId, batchMessages, replyText);
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "memory-record-failed",
        chat_id: chatId,
        message_ids: batchMessages.map((item) => item.message_id),
        error: String(error)
      });
    }
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
    if (chat?.processing && recoverPersistedProcessingOnStartup(queue, chatId)) {
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
  const alreadySeen = rememberIncomingEvent(event);
  const duplicateInInbox = !alreadySeen && inboxAlreadyHasIncomingEvent(event);
  if (alreadySeen || duplicateInInbox) {
    appendJsonl(eventsPath, {
      timestamp: new Date().toISOString(),
      type: event.source === "history-sync" ? "history-sync.user-message.duplicate" : "im.message.receive_v1.duplicate",
      chat_id: event.chat_id,
      message_id: event.message_id,
      duplicate_source: alreadySeen ? "seen-ledger" : "inbox-log"
    });
    return false;
  }

  appendJsonl(inboxPath, event);
  appendJsonl(eventsPath, {
    timestamp: new Date().toISOString(),
    type: event.source === "history-sync" ? "history-sync.user-message" : "im.message.receive_v1",
    event
  });

  if (event.source === "history-sync" && getRuntimeBackend(config, flags) === "app-server") {
    return true;
  }

  try {
    if (await maybeHandleReportTargetCommand(event, config)) {
      return;
    }
  } catch (error) {
    appendJsonl(errorsPath, {
      timestamp: new Date().toISOString(),
      type: "report-target-command-failed",
      chat_id: event.chat_id,
      message_id: event.message_id,
      error: describeError(error)
    });
  }

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
          error: describeError(error)
        });
      });
  }

  if (flags["auto-ack"]) {
    const autoAckText = "已收到，派蒙正在思考。";
    try {
      await sendTextMessage(config, "chat_id", event.chat_id, autoAckText);
      if (!flags["spawn-codex"]) {
        await clearTypingReactions(config, [event], flags);
      }
    } catch (error) {
      appendJsonl(errorsPath, {
        timestamp: new Date().toISOString(),
        type: "auto-ack",
        error: describeError(error),
        event
      });
    }
  }

  if (!flags["spawn-codex"]) {
    return true;
  }

  if (getRuntimeBackend(config, flags) === "app-server") {
    await processEventWithAppServer(event, config, flags);
    return true;
  }

  enqueueForChat(event);
  scheduleChatProcessing(event.chat_id, config, flags);
  return true;
}

async function startWebsocket(config, flags) {
  ensureDirs();
  if (getRuntimeBackend(config, flags) === "app-server") {
    await ensureAppServerRuntime(config, flags);
  } else {
    bootstrapPendingQueues(config, flags);
    startQueueSweeper(config, flags);
  }

  const dispatcher = new Lark.EventDispatcher({
    encryptKey: config.feishu?.encrypt_key || ""
  }).register({
    "im.message.receive_v1": (data) => {
      let event = null;
      try {
        event = extractMessageEvent(data);
      } catch (error) {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "im.message.receive_v1.parse-failed",
          error: describeError(error)
        });
        return;
      }
      handleIncomingMessage(event, config, flags).catch((error) => {
        appendJsonl(errorsPath, {
          timestamp: new Date().toISOString(),
          type: "im.message.receive_v1.failed",
          chat_id: event?.chat_id || "",
          message_id: event?.message_id || "",
          error: describeError(error)
        });
      });
    },
    "card.action.trigger": async (data) => handleCardActionTrigger(data, config, flags),
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
  const items = await fetchChatMessages(config, chatId, Number(flags["page-size"] || 50));
  const fresh = [];

  for (const item of items) {
    if (item.sender?.sender_type !== "user") {
      continue;
    }
    const event = normalizeMessageItem(item);
    const accepted = await handleIncomingMessage(event, config, flags);
    if (accepted) {
      fresh.push(event);
    }
  }
  if (flags["spawn-codex"] && fresh.length && getRuntimeBackend(config, flags) !== "app-server") {
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
  node feishu_gateway.mjs show-report-target
  node feishu_gateway.mjs bind-report-target --chat-id oc_xxx [--chat-type group] [--label "ops"]
  node feishu_gateway.mjs clear-report-target
  node feishu_gateway.mjs sync --chat-id oc_xxx [--auto-ack] [--spawn-codex] [--runtime-backend app-server|exec]
  node feishu_gateway.mjs ws [--auto-ack] [--spawn-codex] [--runtime-backend app-server|exec] [--thread-idle-ttl-ms 3600000] [--progress-flush-ms 2000] [--status-card-update-min-ms 8000] [--status-card-update-max-ms 15000] [--progress-ping-ms 300000] [--reaction-emoji Typing] [--no-reaction] [--card-callback|--no-card-callback]`);
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
  if (command === "show-report-target") {
    console.log(JSON.stringify(readReportTargetState(), null, 2));
    return;
  }
  if (command === "bind-report-target") {
    const chatId = flags["chat-id"] || flags["receive-id"] || "";
    if (!chatId) {
      throw new Error("bind-report-target requires --chat-id");
    }
    const state = buildReportTargetState(config, {
      receive_id: chatId,
      chat_type: String(flags["chat-type"] || ""),
      label: String(flags.label || ""),
      source: "cli-command"
    });
    writeReportTargetState(state);
    console.log(JSON.stringify(state, null, 2));
    return;
  }
  if (command === "clear-report-target") {
    clearReportTargetState();
    console.log(JSON.stringify({ cleared: true }, null, 2));
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

const isDirectRun = Boolean(process.argv[1] && path.resolve(process.argv[1]) === __filename);

if (isDirectRun) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export {
  buildCodexPrompt,
  buildFeishuContextBlock,
  buildQuestionAnswerPayload,
  buildStatusCard,
  extractModeDirective,
  getRuntimeBackend,
  inboxEventMatchesIncomingEvent,
  listIncomingDedupKeys,
  normalizeCardActionPayload,
  normalizePendingRequestId,
  pickStatusPhrase,
  shouldEnableCardCallbacks,
  shouldApplyTurnCompletionToSession,
  splitNaturalMessageChunks,
  supportsCardActions,
  tryMapTextToQuestionAnswer
};
