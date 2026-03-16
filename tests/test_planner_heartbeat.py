from datetime import datetime, timezone
import sys
import unittest


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

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


class HeartbeatStateTests(unittest.TestCase):
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
