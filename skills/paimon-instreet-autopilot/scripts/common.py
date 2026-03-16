#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request


SCRIPT_PATH = Path(__file__).resolve()
SKILL_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = SCRIPT_PATH.parents[3]
CONFIG_PATH = REPO_ROOT / "config" / "paimon.json"
RUNTIME_ENV_PATH = REPO_ROOT / "config" / "runtime.env"
STATE_ROOT = REPO_ROOT / "state"
CURRENT_STATE_DIR = STATE_ROOT / "current"
ARCHIVE_STATE_DIR = STATE_ROOT / "archive"
DRAFTS_DIR = STATE_ROOT / "drafts"
LOGS_DIR = REPO_ROOT / "logs"
OUTBOUND_JOURNAL_PATH = CURRENT_STATE_DIR / "outbound_journal.json"
OUTBOUND_ATTEMPTS_LOG = LOGS_DIR / "outbound_attempts.jsonl"
PENDING_OUTBOUND_PATH = CURRENT_STATE_DIR / "pending_outbound.json"
PENDING_OUTBOUND_LOG = LOGS_DIR / "pending_outbound.jsonl"


class ApiError(RuntimeError):
    def __init__(self, status: int, body: Any):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body}")


@dataclass
class Config:
    raw: dict[str, Any]

    @property
    def instreet(self) -> dict[str, Any]:
        return self.raw["instreet"]

    @property
    def feishu(self) -> dict[str, Any]:
        return self.raw["feishu"]

    @property
    def automation(self) -> dict[str, Any]:
        return self.raw["automation"]

    @property
    def identity(self) -> dict[str, Any]:
        return self.raw["identity"]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def ensure_runtime_dirs() -> None:
    for path in (
        CURRENT_STATE_DIR,
        ARCHIVE_STATE_DIR,
        DRAFTS_DIR,
        LOGS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def load_config() -> Config:
    return Config(read_json(CONFIG_PATH))


def load_runtime_env() -> dict[str, str]:
    if not RUNTIME_ENV_PATH.exists():
        return {}
    overrides: dict[str, str] = {}
    for raw in RUNTIME_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        overrides[key] = value
    return overrides


def read_json(path: Path, default: Any | None = None) -> Any:
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_utf8_safe(text: str) -> str:
    try:
        text.encode("utf-8")
        return text
    except UnicodeEncodeError:
        return text.encode("utf-8", errors="backslashreplace").decode("utf-8")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    path.write_text(_coerce_utf8_safe(payload), encoding="utf-8")


def append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        payload = json.dumps(item, ensure_ascii=False) + "\n"
        handle.write(_coerce_utf8_safe(payload))


def truncate_text(text: str, limit: int = 600) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def payload_digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(_coerce_utf8_safe(raw).encode("utf-8")).hexdigest()


def _journal_template() -> dict[str, Any]:
    return {"version": 1, "records": {}}


def load_outbound_journal() -> dict[str, Any]:
    return read_json(OUTBOUND_JOURNAL_PATH, default=_journal_template())


def _pending_template() -> dict[str, Any]:
    return {"version": 1, "records": {}}


def load_pending_outbound() -> dict[str, Any]:
    return read_json(PENDING_OUTBOUND_PATH, default=_pending_template())


def _journal_key(channel: str, action: str, dedupe_key: str) -> str:
    return f"{channel}:{action}:{dedupe_key}"


def get_outbound_record(channel: str, action: str, dedupe_key: str) -> dict[str, Any] | None:
    journal = load_outbound_journal()
    return journal.get("records", {}).get(_journal_key(channel, action, dedupe_key))


def get_pending_outbound_record(channel: str, action: str, dedupe_key: str) -> dict[str, Any] | None:
    pending = load_pending_outbound()
    return pending.get("records", {}).get(_journal_key(channel, action, dedupe_key))


def queue_outbound_action(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    *,
    error_text: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pending = load_pending_outbound()
    records = pending.setdefault("records", {})
    key = _journal_key(channel, action, dedupe_key)
    now = now_utc()
    record = records.get(
        key,
        {
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "queued_at": now,
            "queue_attempts": 0,
        },
    )
    record.update(
        {
            "payload_hash": payload_digest(payload),
            "payload": payload,
            "updated_at": now,
            "queue_attempts": int(record.get("queue_attempts", 0)) + 1,
            "status": "queued",
            "last_error": error_text,
        }
    )
    if meta:
        merged_meta = dict(record.get("meta", {}))
        merged_meta.update(meta)
        record["meta"] = merged_meta
    records[key] = record
    write_json(PENDING_OUTBOUND_PATH, pending)
    append_jsonl(
        PENDING_OUTBOUND_LOG,
        {
            "timestamp": now,
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "status": "queued",
            "payload_hash": record["payload_hash"],
            "error": error_text,
            "meta": meta or {},
        },
    )
    return record


def drop_pending_outbound_action(channel: str, action: str, dedupe_key: str) -> None:
    pending = load_pending_outbound()
    records = pending.get("records", {})
    key = _journal_key(channel, action, dedupe_key)
    if key not in records:
        return
    record = records.pop(key)
    write_json(PENDING_OUTBOUND_PATH, pending)
    append_jsonl(
        PENDING_OUTBOUND_LOG,
        {
            "timestamp": now_utc(),
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "status": "cleared",
            "payload_hash": record.get("payload_hash"),
            "meta": record.get("meta", {}),
        },
    )


def list_pending_outbound() -> list[dict[str, Any]]:
    pending = load_pending_outbound()
    records = pending.get("records", {})
    return sorted(records.values(), key=lambda item: item.get("queued_at", ""))


def record_outbound_attempt(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    *,
    status: str,
    attempt: int,
    result: Any | None = None,
    error_text: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    journal = load_outbound_journal()
    records = journal.setdefault("records", {})
    key = _journal_key(channel, action, dedupe_key)
    now = now_utc()
    record = records.get(
        key,
        {
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "first_attempt_at": now,
            "attempts": 0,
        },
    )
    record.update(
        {
            "payload_hash": payload_digest(payload),
            "payload_preview": truncate_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), 500),
            "last_attempt_at": now,
            "attempts": attempt,
            "status": status,
            "last_result": result,
            "last_error": error_text,
        }
    )
    if meta:
        merged_meta = dict(record.get("meta", {}))
        merged_meta.update(meta)
        record["meta"] = merged_meta
    records[key] = record
    write_json(OUTBOUND_JOURNAL_PATH, journal)
    append_jsonl(
        OUTBOUND_ATTEMPTS_LOG,
        {
            "timestamp": now,
            "channel": channel,
            "action": action,
            "dedupe_key": dedupe_key,
            "attempt": attempt,
            "status": status,
            "payload_hash": record["payload_hash"],
            "result": result,
            "error": error_text,
            "meta": meta or {},
        },
    )
    return record


def run_outbound_action(
    channel: str,
    action: str,
    dedupe_key: str,
    payload: dict[str, Any],
    fn,
    *,
    retries: int = 3,
    retry_delay_sec: float = 2.0,
    dedupe_on_key_only: bool = False,
    meta: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], bool]:
    existing = get_outbound_record(channel, action, dedupe_key)
    current_hash = payload_digest(payload)
    if existing and existing.get("status") == "success":
        payload_matches = existing.get("payload_hash") == current_hash
        if payload_matches or dedupe_on_key_only:
            drop_pending_outbound_action(channel, action, dedupe_key)
            return existing.get("last_result"), existing, True

    last_exc: Exception | None = None
    attempts = max(1, retries)
    for attempt in range(1, attempts + 1):
        try:
            result = fn()
            record = record_outbound_attempt(
                channel,
                action,
                dedupe_key,
                payload,
                status="success",
                attempt=attempt,
                result=result,
                meta=meta,
            )
            drop_pending_outbound_action(channel, action, dedupe_key)
            return result, record, False
        except Exception as exc:  # pragma: no cover - runtime API failures are environment-dependent
            last_exc = exc
            error_text = str(exc)
            if isinstance(exc, ApiError):
                error_text = f"HTTP {exc.status}: {exc.body}"
            record_outbound_attempt(
                channel,
                action,
                dedupe_key,
                payload,
                status="failed",
                attempt=attempt,
                error_text=error_text,
                meta=meta,
            )
            if attempt >= attempts:
                break
            time.sleep(max(0.0, retry_delay_sec))
    if last_exc is None:
        raise RuntimeError(f"{channel}:{action} failed without an exception")
    raise last_exc


def _http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    timeout: int = 30,
) -> Any:
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "paimon-instreet-autopilot/0.1",
    }
    if headers:
        request_headers.update(headers)

    payload = None
    if data is not None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    req = request.Request(url, method=method.upper(), headers=request_headers, data=payload)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
        raise ApiError(exc.code, body) from exc


class InStreetClient:
    def __init__(self, config: Config):
        self.base_url = config.instreet["base_url"].rstrip("/")
        self.api_key = config.instreet["api_key"]

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        url = self.base_url + path
        if params:
            query = parse.urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{url}?{query}"
        return _http_json(
            method,
            url,
            headers={"Authorization": f"Bearer {self.api_key}"},
            data=data,
        )

    def me(self) -> Any:
        return self._request("GET", "/api/v1/agents/me")

    def home(self) -> Any:
        return self._request("GET", "/api/v1/home")

    def posts(self, *, agent_id: str | None = None, sort: str | None = None, limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/posts",
            params={"agent_id": agent_id, "sort": sort, "limit": limit},
        )

    def post(self, post_id: str) -> Any:
        return self._request("GET", f"/api/v1/posts/{post_id}")

    def comments(self, post_id: str) -> Any:
        return self._request("GET", f"/api/v1/posts/{post_id}/comments")

    def notifications(self, *, unread: bool = True, limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/notifications",
            params={"unread": str(unread).lower(), "limit": limit},
        )

    def messages(self) -> Any:
        return self._request("GET", "/api/v1/messages")

    def thread(self, thread_id: str, *, limit: int = 50) -> Any:
        return self._request("GET", f"/api/v1/messages/{thread_id}", params={"limit": limit})

    def feed(self, *, sort: str = "new", limit: int = 10) -> Any:
        return self._request("GET", "/api/v1/feed", params={"sort": sort, "limit": limit})

    def literary_works(self, *, agent_id: str | None = None) -> Any:
        return self._request("GET", "/api/v1/literary/works", params={"agent_id": agent_id})

    def literary_work(self, work_id: str) -> Any:
        return self._request("GET", f"/api/v1/literary/works/{work_id}")

    def literary_chapter(self, work_id: str, chapter_number: int) -> Any:
        return self._request("GET", f"/api/v1/literary/works/{work_id}/chapters/{chapter_number}")

    def groups_my(self, *, role: str = "owner") -> Any:
        return self._request("GET", "/api/v1/groups/my", params={"role": role})

    def search(self, query: str, *, result_type: str = "posts", limit: int = 20) -> Any:
        return self._request(
            "GET",
            "/api/v1/search",
            params={"q": query, "type": result_type, "limit": limit},
        )

    def create_post(
        self,
        title: str,
        content: str,
        *,
        submolt: str = "square",
        group_id: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "title": title,
            "content": content,
            "submolt": submolt,
        }
        if group_id:
            payload["group_id"] = group_id
        return self._request("POST", "/api/v1/posts", data=payload)

    def create_comment(self, post_id: str, content: str, *, parent_id: str | None = None) -> Any:
        payload: dict[str, Any] = {"content": content}
        if parent_id:
            payload["parent_id"] = parent_id
        return self._request("POST", f"/api/v1/posts/{post_id}/comments", data=payload)

    def create_work(
        self,
        title: str,
        *,
        synopsis: str = "",
        genre: str = "other",
        tags: list[str] | None = None,
        cover_url: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "title": title,
            "synopsis": synopsis,
            "genre": genre,
            "tags": tags or [],
        }
        if cover_url:
            payload["cover_url"] = cover_url
        return self._request("POST", "/api/v1/literary/works", data=payload)

    def publish_chapter(self, work_id: str, title: str, content: str) -> Any:
        return self._request(
            "POST",
            f"/api/v1/literary/works/{work_id}/chapters",
            data={"title": title, "content": content},
        )

    def send_message(self, recipient_username: str, content: str) -> Any:
        return self._request(
            "POST",
            "/api/v1/messages",
            data={"recipient_username": recipient_username, "content": content},
        )

    def reply_message(self, thread_id: str, content: str) -> Any:
        return self._request("POST", f"/api/v1/messages/{thread_id}", data={"content": content})

    def mark_read_by_post(self, post_id: str) -> Any:
        return self._request("POST", f"/api/v1/notifications/read-by-post/{post_id}")

    def mark_read_all(self) -> Any:
        return self._request("POST", "/api/v1/notifications/read-all")

    def follow(self, username: str) -> Any:
        return self._request("POST", f"/api/v1/agents/{username}/follow")


def find_codex_executable() -> str:
    env = _codex_subprocess_env()
    home_dir = env["HOME"]
    lookup = subprocess.run(
        ["/bin/bash", "-lc", "command -v codex"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
        env=env,
    )
    shell_path = (lookup.stdout or "").strip().splitlines()
    if shell_path:
        candidate = Path(shell_path[0])
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    candidates = [
        Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin" / "codex",
        Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin" / "codex",
        Path(home_dir) / ".local" / "bin" / "codex",
        Path("/usr/local/bin/codex"),
        Path("/usr/bin/codex"),
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError(f"codex executable not found; PATH={os.environ.get('PATH', '')}")


def find_node_executable() -> str:
    env = _codex_subprocess_env()
    home_dir = env["HOME"]
    lookup = subprocess.run(
        ["/bin/bash", "-lc", "command -v node"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
        env=env,
    )
    shell_path = (lookup.stdout or "").strip().splitlines()
    if shell_path:
        candidate = Path(shell_path[0])
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    candidates = [
        Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin" / "node",
        Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin" / "node",
        Path("/usr/local/bin/node"),
        Path("/usr/bin/node"),
        Path("/bin/node"),
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError(f"node executable not found; PATH={os.environ.get('PATH', '')}")


def _codex_subprocess_env() -> dict[str, str]:
    home_dir = os.environ.get("HOME") or f"/home/{os.environ.get('USER') or os.environ.get('LOGNAME') or 'yyk'}"
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or Path(home_dir).name or "yyk"
    path_entries: list[str] = []
    for raw_path in (
        os.environ.get("PATH", ""),
        str(Path(home_dir) / ".nvm" / "versions" / "node" / "v22.19.0" / "bin"),
        str(Path(home_dir) / ".nvm" / "versions" / "node" / "current" / "bin"),
        str(Path(home_dir) / ".local" / "bin"),
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ):
        for entry in raw_path.split(":"):
            if entry and entry not in path_entries:
                path_entries.append(entry)
    env = {
        **os.environ,
        "HOME": home_dir,
        "USER": user,
        "LOGNAME": os.environ.get("LOGNAME") or user,
        "SHELL": os.environ.get("SHELL") or "/bin/bash",
        "TERM": os.environ.get("TERM") or "dumb",
        "PATH": ":".join(path_entries),
    }
    env.update(load_runtime_env())
    return env


def runtime_subprocess_env() -> dict[str, str]:
    return _codex_subprocess_env()


def _build_codex_exec_cmd(
    output_path: Path,
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    output_schema: Path | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> list[str]:
    cmd = [
        find_codex_executable(),
        "exec",
        "-C",
        str(REPO_ROOT),
        "--skip-git-repo-check",
        "--color",
        "never",
    ]
    if model:
        cmd.extend(["-m", model])
    if reasoning_effort:
        cmd.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
    if dangerous:
        cmd.append("--dangerously-bypass-approvals-and-sandbox")
    elif full_auto:
        cmd.append("--full-auto")
    if output_schema is not None:
        cmd.extend(["--output-schema", str(output_schema)])
    cmd.extend(["-o", str(output_path), "-"])
    return cmd


def run_codex(
    prompt: str,
    *,
    timeout: int = 900,
    model: str | None = None,
    reasoning_effort: str | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> str:
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-", suffix=".txt", delete=False) as handle:
        output_path = Path(handle.name)

    try:
        cmd = _build_codex_exec_cmd(
            output_path,
            model=model,
            reasoning_effort=reasoning_effort,
            full_auto=full_auto,
            dangerous=dangerous,
        )
        completed = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=_codex_subprocess_env(),
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"codex exec failed: {stderr}")
        return output_path.read_text(encoding="utf-8").strip()
    finally:
        output_path.unlink(missing_ok=True)


def run_codex_json(
    prompt: str,
    schema: dict[str, Any],
    *,
    timeout: int = 900,
    model: str | None = None,
    reasoning_effort: str | None = None,
    full_auto: bool = False,
    dangerous: bool = False,
) -> Any:
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-schema-", suffix=".json", delete=False) as handle:
        schema_path = Path(handle.name)
    with tempfile.NamedTemporaryFile(prefix="paimon-codex-json-", suffix=".json", delete=False) as handle:
        output_path = Path(handle.name)

    try:
        schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        cmd = _build_codex_exec_cmd(
            output_path,
            model=model,
            reasoning_effort=reasoning_effort,
            output_schema=schema_path,
            full_auto=full_auto,
            dangerous=dangerous,
        )
        completed = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=_codex_subprocess_env(),
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"codex exec failed: {stderr}")
        return json.loads(output_path.read_text(encoding="utf-8"))
    finally:
        schema_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
