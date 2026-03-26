from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

import common  # noqa: E402
import memory_manager  # noqa: E402


class MemoryManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_root = Path(self.tmpdir.name)
        self.original_common_paths = {
            "CURRENT_STATE_DIR": common.CURRENT_STATE_DIR,
            "ARCHIVE_STATE_DIR": common.ARCHIVE_STATE_DIR,
            "DRAFTS_DIR": common.DRAFTS_DIR,
            "LOGS_DIR": common.LOGS_DIR,
        }
        self.original_memory_paths = {
            "MEMORY_STORE_PATH": memory_manager.MEMORY_STORE_PATH,
            "MEMORY_JOURNAL_PATH": memory_manager.MEMORY_JOURNAL_PATH,
        }

        common.CURRENT_STATE_DIR = self.tmp_root / "current"
        common.ARCHIVE_STATE_DIR = self.tmp_root / "archive"
        common.DRAFTS_DIR = self.tmp_root / "drafts"
        common.LOGS_DIR = self.tmp_root / "logs"
        memory_manager.MEMORY_STORE_PATH = common.CURRENT_STATE_DIR / "memory_store.json"
        memory_manager.MEMORY_JOURNAL_PATH = common.CURRENT_STATE_DIR / "memory_journal.jsonl"

        self.config = SimpleNamespace(
            automation={
                "memory_working_ttl_ms": 3_600_000,
                "memory_archive_after_ms": 86_400_000,
                "memory_max_active_items": 12,
                "memory_max_summary_chars": 220,
            }
        )

    def tearDown(self) -> None:
        for key, value in self.original_common_paths.items():
            setattr(common, key, value)
        for key, value in self.original_memory_paths.items():
            setattr(memory_manager, key, value)
        self.tmpdir.cleanup()

    def test_record_interaction_merges_cli_and_feishu_preferences_into_global_memory(self) -> None:
        memory_manager.record_interaction(
            {
                "source": "cli",
                "channel": "cli",
                "messages": [{"text": "记住：以后默认先查仓库再问我。"}],
                "reply_text": "收到。",
            },
            config=self.config,
        )
        memory_manager.record_interaction(
            {
                "source": "feishu",
                "channel": "feishu",
                "chat_id": "oc_test",
                "user_id": "user-1",
                "messages": [{"text": "记住，之后默认用短连续性上下文，不要把跨天旧消息拉进来。"}],
                "reply_text": "已更新。",
            },
            config=self.config,
        )

        store = memory_manager.load_memory_store()
        preference_summaries = [item["summary"] for item in store["user_global_preferences"]]
        self.assertEqual(2, len(preference_summaries))
        self.assertTrue(any("先查仓库" in item for item in preference_summaries))
        self.assertTrue(any("短连续性上下文" in item for item in preference_summaries))

        snapshot = memory_manager.build_prompt_snapshot(channel="feishu", chat_id="oc_test", config=self.config)
        self.assertTrue(any("先查仓库" in item for item in snapshot["user_global_preferences"]))
        self.assertTrue(any("短连续性上下文" in item for item in snapshot["user_global_preferences"]))
        self.assertEqual("已更新。", snapshot["channel_runtime"]["last_reply_excerpt"])
        self.assertTrue(any("AGENTS.md" in item for item in snapshot["identity_entrypoints"]))
        self.assertTrue(any("state/current/memory_store.json" in item for item in snapshot["identity_entrypoints"]))

    def test_maintain_memory_archives_expired_working_items(self) -> None:
        store = memory_manager._default_store()
        store["working_memory"] = [
            {
                "id": "working:old",
                "summary": "已经过期的短期记忆",
                "source": "feishu",
                "kind": "working-note",
                "status": "active",
                "created_at": "2026-03-10T00:00:00+00:00",
                "updated_at": "2026-03-10T00:00:00+00:00",
                "expires_at": "2026-03-10T01:00:00+00:00",
            }
        ]

        maintained = memory_manager.maintain_memory_store(
            store,
            self.config,
            now=memory_manager._parse_datetime("2026-03-12T00:00:00+00:00"),
        )

        self.assertEqual([], maintained["working_memory"])
        self.assertEqual("已经过期的短期记忆", maintained["archived_memory_index"][0]["summary"])
        self.assertEqual("working_memory-expired", maintained["archived_memory_index"][0]["reason"])

    def test_record_heartbeat_summary_replaces_prior_heartbeat_objectives(self) -> None:
        store = memory_manager._default_store()
        store["active_objectives"] = [
            {
                "id": "heartbeat:stale",
                "summary": "旧的 heartbeat 目标",
                "source": "heartbeat",
                "kind": "active-objective",
                "status": "active",
                "created_at": "2026-03-18T00:00:00+00:00",
                "updated_at": "2026-03-18T00:00:00+00:00",
            },
            {
                "id": "objective:user",
                "summary": "用户长期要求维护全局记忆系统",
                "source": "feishu",
                "kind": "active-objective",
                "status": "active",
                "created_at": "2026-03-18T01:00:00+00:00",
                "updated_at": "2026-03-18T01:00:00+00:00",
            },
        ]
        memory_manager._write_memory_store(store)

        memory_manager.record_heartbeat_summary(
            {
                "ran_at": "2026-03-19T00:00:00+00:00",
                "primary_publication_mode": "new",
                "primary_publication_title": "记忆系统不是旧聊天回放",
                "recommended_next_action": "优先维护统一记忆与飞书入口",
                "next_actions": [
                    {"label": "优先维护统一记忆与飞书入口"},
                    {"label": "继续清理活跃讨论帖评论"},
                ],
                "feishu_report_sent": True,
            },
            config=self.config,
        )

        updated = memory_manager.load_memory_store()
        active_summaries = [item["summary"] for item in updated["active_objectives"]]
        self.assertIn("用户长期要求维护全局记忆系统", active_summaries)
        self.assertIn("优先维护统一记忆与飞书入口", active_summaries)
        self.assertIn("继续清理活跃讨论帖评论", active_summaries)
        self.assertNotIn("旧的 heartbeat 目标", active_summaries)
        self.assertTrue(any(item["summary"] == "旧的 heartbeat 目标" for item in updated["archived_memory_index"]))


if __name__ == "__main__":
    unittest.main()
