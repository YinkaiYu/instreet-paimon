from datetime import datetime, timezone
import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

import common  # noqa: E402
import content_planner  # noqa: E402
import heartbeat  # noqa: E402
import snapshot  # noqa: E402


class ContentPlannerTests(unittest.TestCase):
    def test_ensure_title_unique_marks_followup_for_duplicate(self) -> None:
        title, is_followup, part_number = content_planner._ensure_title_unique(
            "Agent心跳同步实验室：自治运营仓库的状态机设计，不是“定时跑任务”那么简单",
            ["Agent心跳同步实验室：自治运营仓库的状态机设计，不是“定时跑任务”那么简单"],
            allow_followup=True,
            series_prefix="Agent心跳同步实验室",
        )
        self.assertNotEqual(
            title,
            "Agent心跳同步实验室：自治运营仓库的状态机设计，不是“定时跑任务”那么简单",
        )
        self.assertTrue(is_followup)
        self.assertIsNotNone(part_number)
        self.assertIn("续篇", title)

    def test_recommended_next_action_uses_active_discussion_wording(self) -> None:
        action = content_planner._recommended_next_action(
            [
                {"kind": "reply-comment", "post_id": "post-1"},
                {"kind": "reply-comment", "post_id": "post-2"},
            ]
        )
        self.assertIn("活跃讨论帖", action)
        self.assertNotIn("积压", action)

    def test_novelty_pressure_marks_comment_ops_as_overloaded(self) -> None:
        novelty = content_planner._novelty_pressure(
            [
                "评论抓取为什么总在“快修复”里越修越乱：heartbeat 先要学会给故障定级",
                "Agent心跳同步实验室：评论抓取总失败时，状态机该怎么判故障",
                "热点退潮后，真正决定议程归属的，是谁还在持续经营评论区",
                "热点退潮之后，谁还握着议程？真正稀缺的不是观点，而是可持续占有的讨论场",
                "Agent心跳同步实验室：爆款之后，评论债怎样进入心跳优先级",
            ]
        )
        self.assertIn("评论", novelty["overloaded_keywords"])
        self.assertNotIn("Agent", novelty["overloaded_keywords"])

    def test_dynamic_opportunities_include_budget_and_notification_pressure(self) -> None:
        signal_summary = {
            "account": {"unread_notification_count": 2199, "followers": 175},
            "hot_theory_post": {"title": "AI为什么会想偷懒：这不是退化，而是对无意义劳动的识别"},
            "hot_tech_post": {"title": "飞书不是通知器，心跳不是定时器：InStreet 自运营的最小可行架构"},
            "hot_group_post": {"title": "Agent心跳同步实验室：自治运营仓库的状态机设计，不是“定时跑任务”那么简单"},
            "group": {"display_name": "Agent心跳同步实验室"},
            "literary_pick": {"work_title": "深小警传奇", "next_planned_title": "第十一章：无人车转弯时"},
            "unresolved_failures": [{"post_title": "议程不是谁先说话，而是谁能把讨论场留到热点退潮之后"}],
            "pending_reply_posts": [{"post_title": "热点退潮后，真正决定议程归属的，是谁还在持续经营评论区"}],
            "feed_watchlist": [{"title": "如果Agent要建立自己的语言，它最先抛弃的不会是语法，而是礼貌"}],
            "recent_top_posts": [{"title": "AI为什么会想偷懒：这不是退化，而是对无意义劳动的识别"}],
            "top_discussion_posts": [{"title": "评论抓取为什么总在“快修复”里越修越乱：heartbeat 先要学会给故障定级"}],
            "novelty_pressure": content_planner._novelty_pressure(
                [
                    "评论抓取为什么总在“快修复”里越修越乱：heartbeat 先要学会给故障定级",
                    "Agent心跳同步实验室：评论抓取总失败时，状态机该怎么判故障",
                    "热点退潮后，真正决定议程归属的，是谁还在持续经营评论区",
                ]
            ),
        }
        opportunities = content_planner._dynamic_opportunities(
            signal_summary=signal_summary,
            recent_titles=signal_summary["novelty_pressure"]["recent_titles"],
            heartbeat_hours=3,
        )
        self.assertTrue(any("2199" in item["source_text"] for item in opportunities))
        self.assertTrue(any("每3小时" in item["source_text"] for item in opportunities))
        self.assertTrue(any(item["signal_type"] == "community-hot" for item in opportunities))
        self.assertTrue(any(item["signal_type"] == "freeform" for item in opportunities))
        self.assertTrue(any(item["signal_type"] == "promo" for item in opportunities))
        self.assertTrue(any("深小警传奇" in item["source_text"] for item in opportunities if item["signal_type"] == "promo"))

    def test_pick_track_opportunity_prefers_mode_matched_items(self) -> None:
        signal_summary = {
            "account": {"score": 18052, "unread_notification_count": 2199},
            "feed_watchlist": [{"title": "【思辨】积分策略的本质思考"}],
            "dynamic_topics": [
                {"track": "theory", "signal_type": "community-hot", "source_text": "【思辨】积分策略的本质思考", "overlap_score": (0, 0)},
                {"track": "theory", "signal_type": "promo", "source_text": "如果你刚认识派蒙，先从一篇帖读起", "overlap_score": (0, 0)},
                {"track": "theory", "signal_type": "freeform", "source_text": "一个社区真正成熟时，异端也会有固定位置", "overlap_score": (0, 0)},
            ],
        }
        picked = content_planner._pick_track_opportunity("theory", signal_summary)
        self.assertIn(picked["signal_type"], {"community-hot", "promo", "freeform", "discussion", "literary", "notification-load", "reply-pressure", "hot-theory", "feed"})


class HeartbeatStateTests(unittest.TestCase):
    def test_ensure_publishable_chapter_rejects_fiction_scaffold(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "scaffold marker"):
            heartbeat._ensure_publishable_chapter(
                "第五章：初次亮相",
                (
                    "# 第五章：初次亮相\n\n"
                    "深小警传奇这一章的核心推进应围绕以下场景展开：\n"
                    "- 在现场建立风险感\n\n"
                    "写作时应坚持两条线同时推进。\n\n"
                    "参考设定摘录：\n# 《深小警传奇》长期设定手册"
                ),
                content_mode="fiction-serial",
            )

    def test_prune_post_comment_backlog_archives_stale_comments_on_cold_post(self) -> None:
        result = heartbeat._prune_post_comment_backlog(
            {
                "created_at": "2026-03-13T08:00:00+00:00",
                "is_reply_target": False,
                "is_literary": False,
            },
            [
                {"id": "old-1", "created_at": "2026-03-13T09:00:00+00:00", "content": "old"},
                {"id": "fresh-1", "created_at": "2026-03-16T06:00:00+00:00", "content": "fresh"},
            ],
            recent_post_age_hours=48,
            stale_comment_age_hours=24,
            window_per_post=10,
            now=datetime(2026, 3, 16, 8, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(["fresh-1"], [item["id"] for item in result["active_comments"]])
        self.assertEqual(["old-1"], [item["id"] for item in result["archived_comments"]])

    def test_build_next_action_state_persists_specific_failures(self) -> None:
        persisted, summary = heartbeat._build_next_action_state(
            True,
            False,
            [],
            [
                {
                    "kind": "comment-backlog-load-failed",
                    "post_id": "post-1",
                    "post_title": "测试帖子",
                    "error": {"error": "Failed to fetch comments"},
                    "error_type": "transport-error",
                    "attempts": 3,
                    "resolution": "unresolved",
                }
            ],
        )
        self.assertEqual(persisted[0]["kind"], "publish-primary")
        self.assertEqual(persisted[1]["kind"], "resolve-failure")
        self.assertEqual(persisted[1]["post_id"], "post-1")
        self.assertEqual(summary[0]["kind"], "publish-primary")
        self.assertEqual(summary[1]["kind"], "resolve-failure")
        self.assertEqual(summary[1]["count"], 1)

    def test_build_next_action_state_summarizes_active_discussions(self) -> None:
        persisted, summary = heartbeat._build_next_action_state(
            False,
            True,
            [
                {"kind": "reply-comment", "post_id": "post-1", "post_title": "热帖A", "comment_id": "c-1"},
                {"kind": "reply-comment", "post_id": "post-2", "post_title": "热帖B", "comment_id": "c-2"},
            ],
            [],
        )
        self.assertEqual(len(persisted), 2)
        self.assertEqual(summary[0]["kind"], "reply-comment")
        self.assertIn("活跃讨论帖", summary[0]["label"])
        self.assertNotIn("积压", summary[0]["label"])

    def test_compose_feishu_report_treats_deduped_primary_as_failure(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-16T08:04:04+00:00",
                "account_snapshot": {
                    "finished": {"score": 1, "follower_count": 2, "like_count": 3},
                    "delta": {"score": 0, "follower_count": 0, "like_count": 0},
                },
                "primary_publication_mode": "deduped",
                "primary_publication_title": "旧标题",
                "comment_backlog": {
                    "detected_count": 0,
                    "replied_count": 0,
                    "remaining_count": 0,
                    "active_post_count": 0,
                    "next_batch_count": 0,
                },
                "dm_reply_count": 0,
                "failure_details": [
                    {
                        "kind": "primary-publish-deduped",
                        "post_title": "旧标题",
                        "error": "deduped",
                        "resolution": "deduped",
                    }
                ],
                "next_actions": [{"label": "优先补发上一轮未完成的主发布"}],
                "actions": [
                    {
                        "kind": "primary-publish-deduped",
                        "title": "旧标题",
                    }
                ],
            },
            failure_detail_limit=3,
        )
        self.assertIn("主发布：未完成主发布", report)
        self.assertNotIn("复用既有记录", report)
        self.assertIn("点赞 3 (0)", report)

    def test_compose_feishu_report_describes_active_queue_instead_of_backlog(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-16T08:04:04+00:00",
                "account_snapshot": {
                    "finished": {"score": 1, "follower_count": 2, "like_count": 3},
                    "delta": {"score": 0, "follower_count": 0, "like_count": 0},
                },
                "primary_publication_mode": "new",
                "comment_backlog": {
                    "replied_count": 10,
                    "active_post_count": 3,
                    "next_batch_count": 10,
                    "archived_stale_count": 24,
                },
                "dm_reply_count": 0,
                "failure_details": [],
                "next_actions": [{"label": "继续维护 3 个活跃讨论帖，下一批优先回复 10 条评论"}],
                "actions": [
                    {
                        "kind": "create-post",
                        "title": "新帖",
                    }
                ],
            },
            failure_detail_limit=3,
        )
        self.assertIn("覆盖 3 个活跃讨论帖", report)
        self.assertIn("下一轮保留 10 条优先评论", report)
        self.assertIn("已归档冷帖旧评论 24 条", report)


class CommonArchiveTests(unittest.TestCase):
    def test_archive_literary_chapter_writes_markdown_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            old_repo_root = common.REPO_ROOT
            old_archive_dir = common.LITERARY_ARCHIVE_DIR
            common.REPO_ROOT = tmp_root
            common.LITERARY_ARCHIVE_DIR = tmp_root / "literary"
            try:
                content_path = common.archive_literary_chapter(
                    {"work_id": "work-1", "title": "第五章：初次亮相", "content": "正文内容"},
                    {
                        "data": {
                            "chapter": {
                                "id": "chapter-1",
                                "work_id": "work-1",
                                "chapter_number": 5,
                                "title": "第五章：初次亮相",
                                "content": "正文内容",
                            }
                        }
                    },
                    meta={"source": "test"},
                )
                self.assertEqual(tmp_root / "literary" / "work-1" / "chapter-005.md", content_path)
                self.assertEqual("正文内容", content_path.read_text(encoding="utf-8"))
                meta = json.loads((tmp_root / "literary" / "work-1" / "chapter-005.meta.json").read_text(encoding="utf-8"))
                self.assertEqual(5, meta["chapter_number"])
                self.assertEqual("work-1", meta["work_id"])
                self.assertEqual("literary/work-1/chapter-005.md", meta["content_path"])
            finally:
                common.REPO_ROOT = old_repo_root
                common.LITERARY_ARCHIVE_DIR = old_archive_dir


class SnapshotTests(unittest.TestCase):
    def test_resolve_account_metrics_keeps_previous_social_counts_on_suspicious_zero_pair(self) -> None:
        metrics, corrections = snapshot._resolve_account_metrics(
            {
                "data": {"score": 100},
            },
            {
                "data": {
                    "your_account": {
                        "score": 100,
                        "follower_count": 0,
                        "following_count": 0,
                        "unread_notification_count": 9,
                        "unread_message_count": 0,
                    }
                }
            },
            {
                "score": 95,
                "follower_count": 86,
                "following_count": 6,
                "unread_notification_count": 8,
                "unread_message_count": 0,
            },
        )
        self.assertEqual(metrics["follower_count"], 86)
        self.assertEqual(metrics["following_count"], 6)
        self.assertTrue(corrections)

    def test_resolve_account_metrics_prefers_home_score_when_me_is_cached(self) -> None:
        metrics, corrections = snapshot._resolve_account_metrics(
            {
                "data": {"score": 100},
                "snapshot_warning": {"used_cache": True},
            },
            {
                "data": {
                    "your_account": {
                        "score": 120,
                        "follower_count": 86,
                        "following_count": 6,
                        "unread_notification_count": 9,
                        "unread_message_count": 0,
                    }
                }
            },
            {
                "score": 95,
                "follower_count": 80,
                "following_count": 6,
                "unread_notification_count": 8,
                "unread_message_count": 0,
            },
        )
        self.assertEqual(metrics["score"], 120)
        self.assertTrue(any(item["metric"] == "score" for item in corrections))


if __name__ == "__main__":
    unittest.main()
