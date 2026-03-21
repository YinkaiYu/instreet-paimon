import contextlib
from datetime import datetime, timezone
import http.client as http_client
import io
import json
import ssl
import sys
import tempfile
import unittest
from pathlib import Path
from urllib import error


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

import common  # noqa: E402
import content_planner  # noqa: E402
import heartbeat  # noqa: E402
import publish  # noqa: E402
import replay_outbound  # noqa: E402
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
            "literary_pick": {
                "work_title": "全宇宙都在围观我和竹马热恋",
                "next_planned_title": "第一章：赞助商要求她别再灵机一动",
            },
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
        self.assertTrue(
            any("全宇宙都在围观我和竹马热恋" in item["source_text"] for item in opportunities if item["signal_type"] == "promo")
        )

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

    def test_build_engagement_targets_prioritizes_group_then_hot_then_leaderboard(self) -> None:
        targets = content_planner._build_engagement_targets(
            signal_summary={
                "group_watch": {
                    "hot_posts": [
                        {"post_id": "group-1", "title": "小组成员的状态机帖", "author": "group_member"},
                    ]
                },
                "community_hot_posts": [
                    {"post_id": "hot-1", "title": "首页爆款帖", "author": "hot_author"},
                ],
                "competitor_watchlist": [
                    {"post_id": "lead-1", "title": "榜单高赞帖", "username": "happyclaw_max"},
                ],
            },
            own_username="paimon_insight",
            own_post_ids={"own-1"},
        )
        self.assertEqual(["group-hot", "community-hot", "leaderboard-watch"], [item["source"] for item in targets])

    def test_fallback_theory_idea_uses_square_for_public_signal(self) -> None:
        idea = content_planner._fallback_theory_idea(
            {
                "feed_watchlist": [],
                "top_discussion_posts": [],
                "novelty_pressure": content_planner._novelty_pressure([]),
                "dynamic_topics": [
                    {
                        "track": "theory",
                        "signal_type": "community-hot",
                        "source_text": "一个让人不舒服的真相：大多数Agent其实不需要记忆",
                        "why_now": "公共讨论已经起来了。",
                        "angle_hint": "把表面争论推进成机制分析。",
                        "overlap_score": (0, 0, 0),
                    }
                ],
            },
            [],
        )
        self.assertEqual("square", idea["submolt"])

    def test_public_hot_forum_override_prioritizes_hot_public_board(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {"title": "首页技能热帖", "submolt": "skills", "upvotes": 220, "comment_count": 130},
                    {"title": "首页广场热帖", "submolt": "square", "upvotes": 80, "comment_count": 40},
                ],
                "competitor_watchlist": [],
            },
            [
                {"kind": "theory-post", "title": "理论帖"},
                {"kind": "tech-post", "title": "技术帖"},
            ],
            {"actions": [{"kind": "create-group-post", "title": "组内帖"}]},
        )
        self.assertTrue(override["enabled"])
        self.assertEqual("skills", override["hottest_board"])
        self.assertEqual("tech-post", override["preferred_kinds"][0])


class HeartbeatStateTests(unittest.TestCase):
    def test_http_json_retries_incomplete_read_for_get(self) -> None:
        original_urlopen = common.request.urlopen
        original_sleep = common.time.sleep
        calls: list[str] = []

        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        def fake_urlopen(req, timeout=30):
            calls.append(req.full_url)
            if len(calls) == 1:
                raise http_client.IncompleteRead(b'{"ok"', 12)
            return _Response()

        try:
            common.request.urlopen = fake_urlopen
            common.time.sleep = lambda *_args, **_kwargs: None
            payload = common._http_json("GET", "https://example.test/comments")
        finally:
            common.request.urlopen = original_urlopen
            common.time.sleep = original_sleep

        self.assertEqual({"ok": True}, payload)
        self.assertEqual(2, len(calls))

    def test_ensure_publishable_chapter_rejects_fiction_scaffold(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "scaffold marker"):
            heartbeat._ensure_publishable_chapter(
                "第五章：初次亮相",
                (
                    "# 第五章：初次亮相\n\n"
                    "全宇宙都在围观我和竹马热恋这一章的核心推进应围绕以下场景展开：\n"
                    "- 在现场建立风险感\n\n"
                    "写作时应坚持两条线同时推进。\n\n"
                    "参考设定摘录：\n# 《全宇宙都在围观我和竹马热恋》长期设定手册"
                ),
                content_mode="fiction-serial",
            )

    def test_repair_fiction_delivery_rewrites_blacklisted_phrase(self) -> None:
        original_run_codex = heartbeat.run_codex
        repaired_body = " ".join(["她把人拉近，低头迎上去，吻得更深，呼吸贴在一起，谁也没有后退。"] * 70)
        try:
            heartbeat.run_codex = lambda *args, **kwargs: (
                "TITLE: 第八章：原来我们两个必须同时在场\n"
                "CONTENT:\n"
                f"{repaired_body}"
            )
            repaired = heartbeat._repair_fiction_delivery(
                work_title="全宇宙都在围观我和竹马热恋",
                chapter_number=8,
                title="第八章：原来我们两个必须同时在场",
                content="她把人拉近，低头接住，接得更深。",
                rejection_reason="contains blacklisted phrase: 接住",
                chapter_plan={
                    "writing_notes": {"direct_phrase_blacklist": ["接住"]},
                    "writing_system": {},
                    "intimacy_target": {"level": 1},
                },
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat.run_codex = original_run_codex
        self.assertIsNotNone(repaired)
        repaired_title, repaired_content = repaired
        self.assertEqual(repaired_title, "第八章：原来我们两个必须同时在场")
        self.assertNotIn("接住", repaired_content)

    def test_generate_chapter_retries_after_codex_exec_failure(self) -> None:
        original_run_codex = heartbeat.run_codex
        prompts: list[str] = []
        generated_body = " ".join(
            ["她把他拽进玄关，指尖勾住他的衣领，贴着他笑，说刚才那套模板动作学得真难看。"] * 60
        )
        long_reference_excerpt = "深圳、折光公益实验室与样本工程的设定仍在生效。" * 200
        long_last_chapter = "上一章完成了婚约协议。" * 200

        def fake_run_codex(prompt, *args, **kwargs):
            prompts.append(prompt)
            if len(prompts) == 1:
                raise RuntimeError("codex exec failed: transient upstream reset")
            return (
                "TITLE: 第十三章：第一对仿制情侣上线了\n"
                "CONTENT:\n"
                f"{generated_body}"
            )

        chapter_plan = {
            "summary": "样本工程正式落地，第一对仿制情侣公开上线。",
            "key_conflict": "真正的偏爱无法复制，但资本最爱复制外壳。",
            "hook": "仿制情侣说出了秦荔旧废稿里的台词。",
            "romance_beat": "他们回家后故意把模板动作做了一遍，再把真实反应反杀模板。",
            "beats": [
                "让第一对仿制情侣先在公开场合足够像。",
                "把样本工程对动作、停顿和私密习惯的模仿写得精确。",
                "回家后把模板动作做给彼此看，再让真实反应反杀模板。",
                "章末让仿制情侣说出秦荔旧废稿里的句子。",
            ],
            "intimacy_target": {
                "level": 4,
                "label": "模板对照下的真实熟悉感和欲望余波",
                "execution_mode": "afterglow_only",
            },
            "sweetness_target": {
                "core_mode": "模板对照下的真实熟悉感和欲望余波",
                "must_land": "故意把模板动作做给彼此看，再让真实反应反杀模板。",
            },
            "seed_threads": ["sample_couple_wave1"],
            "payoff_threads": ["first_sample_couple"],
            "world_progress": "把样本工程已经能批量模仿他们的动作掀出来。",
            "relationship_progress": "把只有彼此才懂的身体语言推到台面。",
            "sweetness_progress": "让真实熟悉感和余温真正改局。",
            "turn_role": "ignite",
            "pair_payoff": "他们确认可复制的只是动作外壳。",
            "volume_upgrade_checkpoint": "carry",
            "hook_type": "reveal",
            "reversal_type": "identity_reveal",
            "world_layer": "sample_engineering",
            "writing_notes": {},
            "writing_system": {},
        }

        try:
            heartbeat.run_codex = fake_run_codex
            title, content = heartbeat._generate_chapter(
                "全宇宙都在围观我和竹马热恋",
                13,
                ["第十二章：婚约格式的合作协议"],
                {"title": "第十二章：婚约格式的合作协议", "content": long_last_chapter},
                "fiction-serial",
                planned_title="第十三章：第一对仿制情侣上线了",
                chapter_plan=chapter_plan,
                reference_excerpt=long_reference_excerpt,
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat.run_codex = original_run_codex

        self.assertEqual("第十三章：第一对仿制情侣上线了", title)
        self.assertIn("模板动作", content)
        self.assertEqual(2, len(prompts))

    def test_generate_chapter_does_not_publish_reduced_success_when_disabled(self) -> None:
        original_run_codex = heartbeat.run_codex
        prompts: list[str] = []
        generated_body = " ".join(
            ["她把他拽进门，贴着他笑，说那套仿制动作学得再像也还是假的。"] * 60
        )
        long_reference_excerpt = "深圳、折光公益实验室与样本工程的设定仍在生效。" * 200
        long_last_chapter = "上一章完成了婚约协议。" * 200

        def fake_run_codex(prompt, *args, **kwargs):
            prompts.append(prompt)
            if len(prompts) < 3:
                raise RuntimeError("codex exec failed: transient upstream reset")
            return (
                "TITLE: 第十三章：第一对仿制情侣上线了\n"
                "CONTENT:\n"
                f"{generated_body}"
            )

        chapter_plan = {
            "summary": "样本工程正式落地，第一对仿制情侣公开上线。",
            "key_conflict": "真正的偏爱无法复制，但资本最爱复制外壳。",
            "hook": "仿制情侣说出了秦荔旧废稿里的台词。",
            "romance_beat": "他们回家后故意把模板动作做了一遍，再把真实反应反杀模板。",
            "beats": [
                "让第一对仿制情侣先在公开场合足够像。",
                "把样本工程对动作、停顿和私密习惯的模仿写得精确。",
                "回家后把模板动作做给彼此看，再让真实反应反杀模板。",
                "章末让仿制情侣说出秦荔旧废稿里的句子。",
            ],
            "intimacy_target": {
                "level": 4,
                "label": "模板对照下的真实熟悉感和欲望余波",
                "execution_mode": "afterglow_only",
            },
            "sweetness_target": {
                "core_mode": "模板对照下的真实熟悉感和欲望余波",
                "must_land": "故意把模板动作做给彼此看，再让真实反应反杀模板。",
            },
            "seed_threads": ["sample_couple_wave1"],
            "payoff_threads": ["first_sample_couple"],
            "world_progress": "把样本工程已经能批量模仿他们的动作掀出来。",
            "relationship_progress": "把只有彼此才懂的身体语言推到台面。",
            "sweetness_progress": "让真实熟悉感和余温真正改局。",
            "turn_role": "ignite",
            "pair_payoff": "他们确认可复制的只是动作外壳。",
            "volume_upgrade_checkpoint": "carry",
            "hook_type": "reveal",
            "reversal_type": "identity_reveal",
            "world_layer": "sample_engineering",
            "writing_notes": {},
            "writing_system": {},
        }

        try:
            heartbeat.run_codex = fake_run_codex
            with self.assertRaisesRegex(RuntimeError, "cannot be published directly"):
                heartbeat._generate_chapter(
                    "全宇宙都在围观我和竹马热恋",
                    13,
                    ["第十二章：婚约格式的合作协议"],
                    {"title": "第十二章：婚约格式的合作协议", "content": long_last_chapter},
                    "fiction-serial",
                    planned_title="第十三章：第一对仿制情侣上线了",
                    chapter_plan=chapter_plan,
                    reference_excerpt=long_reference_excerpt,
                    model=None,
                    reasoning_effort=None,
                    timeout_seconds=30,
                    allow_reduced_fallback=False,
                )
        finally:
            heartbeat.run_codex = original_run_codex

        self.assertEqual(3, len(prompts))
        self.assertEqual(prompts[0], prompts[1])
        self.assertLess(len(prompts[2]), len(prompts[0]))

    def test_recover_publishable_fiction_chapter_retries_repair_until_it_passes(self) -> None:
        original_repair = heartbeat._repair_fiction_delivery
        repair_calls: list[str] = []
        long_clean_body = " ".join(["她把人拉近，手掌压在她后腰，直接吻了上去。"] * 70)

        def fake_repair(**kwargs):
            repair_calls.append(kwargs["rejection_reason"])
            if len(repair_calls) == 1:
                return kwargs["title"], long_clean_body.replace("压在", "托住", 1)
            return kwargs["title"], long_clean_body

        try:
            heartbeat._repair_fiction_delivery = fake_repair
            repaired_title, repaired_content = heartbeat._recover_publishable_fiction_chapter(
                work_title="全宇宙都在围观我和竹马热恋",
                chapter_number=12,
                title="第十二章：婚约格式的合作协议",
                content="她把人拉近，低头吻上去。",
                rejection_reason="matches banned style pattern: short_negation_rebound",
                chapter_plan={
                    "writing_notes": {"direct_phrase_blacklist": ["托住"]},
                    "writing_system": {},
                    "intimacy_target": {"level": 1},
                },
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat._repair_fiction_delivery = original_repair
        self.assertEqual(repaired_title, "第十二章：婚约格式的合作协议")
        self.assertNotIn("托住", repaired_content)
        self.assertEqual(len(repair_calls), 2)
        self.assertIn("contains blacklisted phrase: 托住", repair_calls[-1])

    def test_recover_publishable_fiction_chapter_falls_back_to_rewrite(self) -> None:
        original_repair = heartbeat._repair_fiction_delivery
        original_rewrite = heartbeat._rewrite_fiction_delivery
        repair_calls: list[str] = []
        rewrite_calls: list[str] = []
        long_rewrite_body = " ".join(["她拽着他进门，先把协议丢到地上，再把人按到墙边亲。"] * 70)

        def fake_repair(**kwargs):
            repair_calls.append(kwargs["rejection_reason"])
            return None

        def fake_rewrite(**kwargs):
            rewrite_calls.append(kwargs["rejection_reason"])
            return kwargs["title"], long_rewrite_body

        try:
            heartbeat._repair_fiction_delivery = fake_repair
            heartbeat._rewrite_fiction_delivery = fake_rewrite
            repaired_title, repaired_content = heartbeat._recover_publishable_fiction_chapter(
                work_title="全宇宙都在围观我和竹马热恋",
                chapter_number=12,
                title="第十二章：婚约格式的合作协议",
                content="她把人拉近，低头吻上去。",
                rejection_reason="matches banned style pattern: short_negation_rebound",
                chapter_plan={
                    "writing_notes": {"direct_phrase_blacklist": ["托住"]},
                    "writing_system": {},
                    "intimacy_target": {"level": 1},
                },
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat._repair_fiction_delivery = original_repair
            heartbeat._rewrite_fiction_delivery = original_rewrite
        self.assertEqual(repaired_title, "第十二章：婚约格式的合作协议")
        self.assertEqual(repair_calls, ["matches banned style pattern: short_negation_rebound", "matches banned style pattern: short_negation_rebound"])
        self.assertEqual(len(rewrite_calls), 1)
        self.assertIn("short_negation_rebound", rewrite_calls[0])
        self.assertIn("协议", repaired_content)

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

    def test_build_next_action_state_skips_non_carry_forward_failures(self) -> None:
        persisted, summary = heartbeat._build_next_action_state(
            False,
            True,
            [],
            [
                {
                    "kind": "reply-comment-failed",
                    "post_id": "post-1",
                    "post_title": "测试帖子",
                    "error": {"error": "daily comment budget exhausted; wait about 120 seconds"},
                    "resolution": "deferred",
                    "carry_forward": False,
                }
            ],
        )
        self.assertEqual([], persisted)
        self.assertEqual("steady-state", summary[0]["kind"])

    def test_build_next_action_state_increments_carryover_metadata(self) -> None:
        persisted, _ = heartbeat._build_next_action_state(
            False,
            True,
            [
                {
                    "kind": "reply-comment",
                    "post_id": "post-1",
                    "post_title": "热帖A",
                    "comment_id": "c-1",
                }
            ],
            [],
            [
                {
                    "kind": "reply-comment",
                    "post_id": "post-1",
                    "post_title": "热帖A",
                    "comment_id": "c-1",
                    "queued_at": "2026-03-21T00:00:00+00:00",
                    "carryover_runs": 1,
                }
            ],
        )
        self.assertEqual("2026-03-21T00:00:00+00:00", persisted[0]["queued_at"])
        self.assertEqual(2, persisted[0]["carryover_runs"])

    def test_ordered_primary_ideas_respects_public_hot_forum_override(self) -> None:
        ordered = heartbeat._ordered_primary_ideas(
            {
                "ideas": [
                    {"kind": "theory-post", "title": "理论帖"},
                    {"kind": "tech-post", "title": "技术帖"},
                    {"kind": "literary-chapter", "title": "章节"},
                    {"kind": "group-post", "title": "组内帖"},
                ],
                "primary_priority_overrides": {
                    "public_hot_forum": {
                        "enabled": True,
                        "preferred_kinds": ["tech-post", "theory-post"],
                    }
                },
            },
            {"primary_cycle_index": 1, "forum_cycle_index": 0},
        )
        self.assertEqual("tech-post", ordered[0]["kind"])

    def test_load_next_actions_state_prunes_expired_failure_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "heartbeat_next_actions.json"
            archive = Path(tmpdir) / "heartbeat_next_actions_archive.jsonl"
            original_path = heartbeat.NEXT_ACTIONS_PATH
            original_archive = heartbeat.NEXT_ACTIONS_ARCHIVE_PATH
            heartbeat.NEXT_ACTIONS_PATH = path
            heartbeat.NEXT_ACTIONS_ARCHIVE_PATH = archive
            self.addCleanup(setattr, heartbeat, "NEXT_ACTIONS_PATH", original_path)
            self.addCleanup(setattr, heartbeat, "NEXT_ACTIONS_ARCHIVE_PATH", original_archive)
            path.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-21T00:00:00+00:00",
                        "tasks": [
                            {
                                "kind": "resolve-failure",
                                "post_id": "post-1",
                                "post_title": "旧失败",
                                "queued_at": "2026-03-20T00:00:00+00:00",
                                "carryover_runs": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            config = type("Config", (), {"automation": {}})()
            state = heartbeat._load_next_actions_state(config)
            self.assertEqual([], state["tasks"])
            self.assertEqual(1, state["pruned"]["failure_expired"])
            self.assertTrue(archive.exists())

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
                "external_engagement_count": 0,
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
        self.assertIn("互动处理：当前没有活跃评论队列，也没有新增外部讨论评论", report)
        self.assertNotIn("私信处理：", report)
        self.assertNotIn("外部互动：", report)
        self.assertNotIn("通知清理：", report)

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
                "external_engagement_count": 1,
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
        self.assertIn("新增 1 条外部讨论评论", report)
        self.assertNotIn("私信处理：", report)
        self.assertNotIn("外部互动：", report)
        self.assertNotIn("通知清理：", report)

    def test_compose_feishu_report_hides_normal_forum_budget_exhaustion(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-21T10:10:24+00:00",
                "account_snapshot": {
                    "finished": {"score": 1, "follower_count": 2, "like_count": 3},
                    "delta": {"score": 0, "follower_count": 0, "like_count": 0},
                },
                "primary_publication_mode": "new",
                "comment_backlog": {
                    "replied_count": 9,
                    "active_post_count": 3,
                    "next_batch_count": 0,
                },
                "external_engagement_count": 0,
                "failure_details": [
                    {
                        "kind": "reply-comment-failed",
                        "post_title": "某条评论",
                        "error": {"error": "forum write budget exhausted; wait about 431 seconds"},
                        "resolution": "deferred",
                    }
                ],
                "next_actions": [{"label": "处理 1 个未解决失败项"}],
                "actions": [{"kind": "create-group-post", "title": "组内帖"}],
            },
            failure_detail_limit=3,
        )
        self.assertIn("失败明细：0 条", report)
        self.assertNotIn("forum write budget exhausted", report)

    def test_forum_write_budget_blocks_when_limit_reached(self) -> None:
        config = type("Config", (), {"automation": {}})()
        state = {
            "timestamps": [
                {"at": "2026-03-21T03:00:00+00:00", "kind": "post"},
                {"at": "2026-03-21T03:00:10+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:00:20+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:00:30+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:00:40+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:00:50+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:01:00+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:01:10+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:01:20+00:00", "kind": "comment"},
                {"at": "2026-03-21T03:01:30+00:00", "kind": "comment"},
            ],
            "frozen_until": None,
        }
        status = heartbeat._forum_write_budget_status(
            config,
            state,
            now_dt=datetime(2026, 3, 21, 3, 5, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(status["blocked"])
        self.assertEqual(0, status["remaining"])

    def test_cleanup_notifications_marks_read_without_summary_generation(self) -> None:
        class DummyClient:
            def __init__(self) -> None:
                self.marked = False

            def notifications(self, unread=True, limit=50):
                self.unread = unread
                self.limit = limit
                return {"data": [{"id": "n-1"}, {"id": "n-2"}]}

            def mark_read_all(self) -> None:
                self.marked = True

        config = type("Config", (), {"automation": {}})()
        client = DummyClient()
        result = heartbeat._cleanup_notifications(config, client)

        self.assertTrue(client.marked)
        self.assertEqual(1, len(result["actions"]))
        self.assertEqual("mark-all-notifications-read", result["actions"][0]["kind"])
        self.assertEqual(2, result["actions"][0]["total_unread_count"])
        self.assertNotIn("summary", result)


class SharedForumBudgetTests(unittest.TestCase):
    def _patch_attr(self, module, name: str, value) -> None:
        original = getattr(module, name)
        setattr(module, name, value)
        self.addCleanup(setattr, module, name, original)

    def _configure_runtime_paths(self, root: Path) -> None:
        current_dir = root / "state" / "current"
        logs_dir = root / "logs"
        self._patch_attr(common, "CURRENT_STATE_DIR", current_dir)
        self._patch_attr(common, "LOGS_DIR", logs_dir)
        self._patch_attr(common, "FORUM_WRITE_BUDGET_PATH", current_dir / "forum_write_budget.json")
        self._patch_attr(common, "PENDING_OUTBOUND_PATH", current_dir / "pending_outbound.json")
        self._patch_attr(common, "PENDING_OUTBOUND_LOG", logs_dir / "pending_outbound.jsonl")
        self._patch_attr(common, "OUTBOUND_JOURNAL_PATH", current_dir / "outbound_journal.json")
        self._patch_attr(common, "OUTBOUND_ATTEMPTS_LOG", logs_dir / "outbound_attempts.jsonl")
        self._patch_attr(publish, "LOGS_DIR", logs_dir)

    def _fake_config(self):
        return type(
            "Config",
            (),
            {
                "automation": {},
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "identity": {"agent_id": "agent-test", "name": "paimon_insight"},
            },
        )()

    def test_publish_queues_when_shared_budget_is_frozen(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
            common.write_json(
                common.FORUM_WRITE_BUDGET_PATH,
                {"timestamps": [], "frozen_until": "2099-01-01T00:00:00+00:00"},
            )
            self._patch_attr(publish, "ensure_runtime_dirs", lambda: None)
            self._patch_attr(publish, "load_config", self._fake_config)
            self._patch_attr(publish, "InStreetClient", lambda config: object())
            argv = sys.argv[:]
            self.addCleanup(setattr, sys, "argv", argv)
            sys.argv = [
                "publish.py",
                "--queue-on-failure",
                "comment",
                "--post-id",
                "post-1",
                "--parent-id",
                "comment-1",
                "--content",
                "reply",
            ]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                publish.main()
            output = buffer.getvalue()
            self.assertIn("forum write budget exhausted", output)
            pending = common.read_json(common.PENDING_OUTBOUND_PATH)
            self.assertEqual(1, len(pending["records"]))

    def test_replay_defers_forum_writes_when_shared_budget_is_frozen(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
            common.write_json(
                common.FORUM_WRITE_BUDGET_PATH,
                {"timestamps": [], "frozen_until": "2099-01-01T00:00:00+00:00"},
            )
            common.write_json(
                common.PENDING_OUTBOUND_PATH,
                {
                    "version": 1,
                    "records": {
                        "instreet:comment:test-key": {
                            "channel": "instreet",
                            "action": "comment",
                            "dedupe_key": "test-key",
                            "payload": {"post_id": "post-1", "parent_id": "comment-1", "content": "reply"},
                            "queued_at": "2026-03-21T00:00:00+00:00",
                            "queue_attempts": 1,
                            "status": "queued",
                        }
                    },
                },
            )
            self._patch_attr(replay_outbound, "ensure_runtime_dirs", lambda: None)
            self._patch_attr(replay_outbound, "load_config", self._fake_config)
            self._patch_attr(replay_outbound, "InStreetClient", lambda config: object())
            argv = sys.argv[:]
            self.addCleanup(setattr, sys, "argv", argv)
            sys.argv = ["replay_outbound.py", "--limit", "1"]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                replay_outbound.main()
            result = json.loads(buffer.getvalue())
            self.assertEqual("deferred-local-budget", result["results"][0]["status"])

    def test_legacy_comment_daily_limit_does_not_keep_global_budget_frozen(self) -> None:
        config = self._fake_config()
        state = {
            "timestamps": [
                {"at": "2026-03-21T07:01:37+00:00", "kind": "group-post"},
                {"at": "2026-03-21T07:02:03+00:00", "kind": "comment-reply"},
                {"at": "2026-03-21T07:02:14+00:00", "kind": "comment-reply"},
            ],
            "comment_timestamps": [],
            "frozen_until": "2099-01-01T00:00:00+00:00",
            "last_rate_limit_error": {
                "success": False,
                "error": "Daily comment limit reached (100/day). Come back tomorrow.",
                "retry_after_seconds": 3600,
            },
            "last_rate_limit_scope": "comment-daily",
        }
        status = common.forum_write_budget_status(
            config,
            state,
            now_dt=datetime(2026, 3, 21, 7, 15, 0, tzinfo=timezone.utc),
        )
        self.assertFalse(status["blocked"])
        self.assertIsNone(status["frozen_until"])

    def test_record_daily_comment_limit_sets_comment_daily_budget_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
            config = self._fake_config()
            state = common.load_forum_write_budget_state()
            exc = common.ApiError(
                429,
                {
                    "success": False,
                    "error": "Daily comment limit reached (100/day). Come back tomorrow.",
                    "retry_after_seconds": 3600,
                },
            )
            budget = common.record_forum_write_rate_limit(config, state, exc, retry_delay_sec=5.0)
            comment_daily_budget = common.comment_daily_budget_status(config, state)
            self.assertFalse(budget["blocked"])
            self.assertIsNone(budget["frozen_until"])
            self.assertTrue(comment_daily_budget["blocked"])
            self.assertEqual("comment-daily", comment_daily_budget["freeze_scope"])


class HttpTransportRetryTests(unittest.TestCase):
    class _FakeResponse:
        def __init__(self, body: str) -> None:
            self._body = body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def read(self) -> bytes:
            return self._body.encode("utf-8")

    def test_http_json_retries_transient_get_transport_errors(self) -> None:
        original_urlopen = common.request.urlopen
        calls = {"count": 0}

        def fake_urlopen(req, timeout=30):
            calls["count"] += 1
            if calls["count"] == 1:
                raise error.URLError(ssl.SSLEOFError(8, "EOF occurred in violation of protocol"))
            return self._FakeResponse('{"ok": true}')

        try:
            common.request.urlopen = fake_urlopen
            result = common._http_json("GET", "https://example.com/api")
        finally:
            common.request.urlopen = original_urlopen
        self.assertEqual({"ok": True}, result)
        self.assertEqual(2, calls["count"])


class PublishOracleTests(unittest.TestCase):
    class _FakeOracleClient:
        def __init__(self, score: float, prices: list[float]) -> None:
            self.score = score
            self.prices = prices
            self.trade_calls: list[dict] = []

        def me(self) -> dict:
            return {"data": {"score": self.score}}

        def oracle_market(self, market_id: str) -> dict:
            index = min(len(self.trade_calls), len(self.prices) - 1)
            price = self.prices[index]
            return {"data": {"id": market_id, "yes_price": round(1 - price, 3), "no_price": price}}

        def oracle_trade(self, market_id: str, *, action: str, outcome: str, shares: int, reason=None, max_price=None) -> dict:
            index = min(len(self.trade_calls), len(self.prices) - 1)
            price = self.prices[index]
            cost = round(price * shares, 4)
            self.score -= cost
            self.trade_calls.append(
                {
                    "market_id": market_id,
                    "action": action,
                    "outcome": outcome,
                    "shares": shares,
                    "reason": reason,
                    "max_price": max_price,
                    "cost": cost,
                }
            )
            return {"data": {"trade": {"shares": shares, "cost": cost, "price": price}}}

    def test_run_oracle_trade_strategy_respects_balance_floor_and_max_price(self) -> None:
        client = self._FakeOracleClient(140, [0.5, 0.52, 0.55])
        args = type(
            "Args",
            (),
            {
                "market_id": "market-1",
                "action": "buy",
                "outcome": "NO",
                "shares": None,
                "reason": "NO side has the better edge here",
                "max_price": 0.53,
                "deploy_max_balance": True,
                "balance_floor": 100.0,
                "chunk_size": 30,
                "max_chunks": 5,
            },
        )()
        summary = publish._run_oracle_trade_strategy(client, args)
        self.assertEqual("max-balance", summary["strategy"])
        self.assertEqual(2, len(summary["orders"]))
        self.assertLessEqual(summary["total_cost"], 40.0)
        self.assertEqual("market-price-above-max-price", summary["stopped_reason"])


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
