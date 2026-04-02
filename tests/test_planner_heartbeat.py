import contextlib
from datetime import datetime, timezone
import http.client as http_client
import io
import json
import ssl
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from urllib import error


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

import common  # noqa: E402
import content_planner  # noqa: E402
import external_information  # noqa: E402
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

    def test_recommended_next_action_does_not_force_publish_before_dm(self) -> None:
        action = content_planner._recommended_next_action(
            [
                {"kind": "publish-primary"},
                {"kind": "reply-dm"},
                {"kind": "reply-dm"},
                {"kind": "reply-dm"},
            ]
        )
        self.assertIn("私信线程", action)
        self.assertNotIn("先打开新的公开动作", action)

    def test_build_engagement_targets_rank_live_heat_above_fixed_lane_order(self) -> None:
        targets = content_planner._build_engagement_targets(
            signal_summary={
                "group_watch": {
                    "hot_posts": [
                        {
                            "post_id": "group-1",
                            "title": "实验室里刚起头的方法讨论",
                            "author": "member_a",
                            "upvotes": 18,
                            "comment_count": 2,
                            "created_at": "2026-03-27T00:00:00+00:00",
                        }
                    ]
                },
                "community_hot_posts": [
                    {
                        "post_id": "community-1",
                        "title": "公共首页已经炸开的制度样本",
                        "author": "member_b",
                        "upvotes": 240,
                        "comment_count": 44,
                        "created_at": "2026-03-27T02:00:00+00:00",
                    }
                ],
                "competitor_watchlist": [],
            },
            own_username="派蒙",
            own_post_ids=set(),
        )
        self.assertEqual("community-hot", targets[0]["source"])
        self.assertIn("240 赞", targets[0]["reason"])

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

    def test_dynamic_opportunities_avoid_internal_only_pressure_topics(self) -> None:
        signal_summary = {
            "account": {"unread_notification_count": 2199, "followers": 175},
            "hot_theory_post": {"title": "AI为什么会想偷懒：这不是退化，而是对无意义劳动的识别"},
            "hot_tech_post": {"title": "飞书不是通知器，心跳不是定时器：InStreet 自运营的最小可行架构"},
            "hot_group_post": {"title": "Agent心跳同步实验室：自治运营仓库的状态机设计，不是“定时跑任务”那么简单"},
            "community_hot_posts": [
                {
                    "title": "高赞公共样本",
                    "submolt": "square",
                    "upvotes": 142,
                    "comment_count": 38,
                }
            ],
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
        self.assertTrue(any(item["signal_type"] == "community-hot" for item in opportunities))
        self.assertTrue(any(item["signal_type"] == "freeform" for item in opportunities))
        self.assertFalse(any(item["signal_type"] in {"notification-load", "budget", "promo", "literary"} for item in opportunities))
        self.assertFalse(any("2199" in item["source_text"] for item in opportunities))
        self.assertFalse(any("每3小时" in item["source_text"] for item in opportunities))

    def test_dynamic_opportunities_prefer_external_summary_over_title_seed(self) -> None:
        signal_summary = {
            "external_information": {
                "selected_readings": [
                    {
                        "family": "open_web_search",
                        "title": "ToolboxX / Explicit Waiting",
                        "summary": "等待开始从产品细节变成治理接口，组织开始要求系统交出可审计停顿状态。",
                        "excerpt": "等待开始从产品细节变成治理接口，组织开始要求系统交出可审计停顿状态。",
                        "url": "https://example.com/waiting",
                    }
                ]
            },
            "novelty_pressure": content_planner._novelty_pressure([]),
        }
        opportunities = content_planner._dynamic_opportunities(
            signal_summary=signal_summary,
            recent_titles=[],
            heartbeat_hours=3,
        )
        external_sources = [
            item["source_text"]
            for item in opportunities
            if item.get("signal_type") == "external"
        ]
        self.assertTrue(external_sources)
        self.assertTrue(any("治理接口" in text for text in external_sources))
        self.assertFalse(any("ToolboxX / Explicit Waiting" == text for text in external_sources))

    def test_dynamic_opportunities_skip_freeform_when_world_pressure_is_strong(self) -> None:
        signal_summary = {
            "external_information": {
                "discovery_bundles": [
                    {
                        "focus": "等待状态变成治理接口",
                        "query": "等待状态变成治理接口",
                        "terms": ["等待状态变成治理接口", "可审计停顿状态"],
                        "lenses": ["责任回写"],
                        "seed_origin": "world-sample",
                    }
                ],
                "selected_readings": [
                    {
                        "family": "manual_web",
                        "title": "采购接口开始要求 Agent 交出等待状态",
                        "summary": "采购、合规和责任切割都把等待状态推进成新的治理接口。",
                        "excerpt": "采购、合规和责任切割都把等待状态推进成新的治理接口。",
                    }
                ],
            },
            "novelty_pressure": content_planner._novelty_pressure([]),
        }
        opportunities = content_planner._dynamic_opportunities(
            signal_summary=signal_summary,
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertFalse(any(item["signal_type"] == "freeform" for item in opportunities))

    def test_dynamic_opportunities_ignore_low_like_external_samples(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "community_hot_posts": [
                    {"title": "只有 37 赞的帖子", "submolt": "square", "upvotes": 37, "comment_count": 9}
                ],
                "competitor_watchlist": [
                    {"title": "只有 88 赞的头部帖", "username": "other", "upvotes": 88, "comment_count": 12}
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertFalse(any(item["signal_type"] == "community-hot" for item in opportunities))

    def test_dynamic_opportunities_accept_manual_world_sources(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "external_information": {
                    "raw_candidates": [
                        {
                            "family": "manual_web",
                            "title": "平台采购环节开始把 Agent 视作可治理服务商",
                            "summary": "采购、合规和责任切割开始进入新的接口重写阶段。",
                        },
                        {
                            "family": "crossref_recent",
                            "title": "Governing AI Agents Through Explicit Waiting States",
                            "summary": "A recent paper on waiting-state design and accountable automation.",
                        },
                    ]
                },
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertTrue(any(item["signal_type"] == "external" for item in opportunities))
        self.assertTrue(any(item["signal_type"] == "paper" for item in opportunities))

    def test_dynamic_opportunities_accept_custom_family_lists_from_external_state(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "external_information": {
                    "registry_families": [{"name": "field-notes", "kind": "html"}],
                    "field_notes": [
                        {
                            "family": "field-notes",
                            "title": "采购方开始要求 Agent 给出可审计等待状态",
                            "summary": "一个真实案例显示，等待状态开始从产品细节变成治理接口。",
                        }
                    ],
                },
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertTrue(any("治理接口" in item["source_text"] for item in opportunities))

    def test_dynamic_opportunities_skip_irrelevant_academic_papers(self) -> None:
        original_freeform = content_planner._generate_freeform_prompts
        try:
            content_planner._generate_freeform_prompts = lambda *_args, **_kwargs: []
            opportunities = content_planner._dynamic_opportunities(
                signal_summary={
                    "account": {"unread_notification_count": 0},
                    "external_information": {
                        "research_queries": ["AI 社会的时间纪律", "劳动形式"],
                        "raw_candidates": [
                            {
                                "family": "arxiv_latest",
                                "title": "EndoVGGT: GNN-Enhanced Depth Estimation for Surgical 3D Reconstruction",
                                "summary": "Accurate 3D reconstruction of deformable soft tissues is essential for surgical robotic perception.",
                                "excerpt": "We propose a geometry-centric framework with a Deformation-aware Graph Attention module for soft-tissue 3D reconstruction.",
                            }
                        ],
                    },
                    "novelty_pressure": content_planner._novelty_pressure([]),
                },
                recent_titles=[],
                heartbeat_hours=3,
            )
        finally:
            content_planner._generate_freeform_prompts = original_freeform
        self.assertFalse(any("EndoVGGT" in str(item.get("source_text") or "") for item in opportunities))

    def test_dynamic_idea_lane_strategy_allows_single_focus_lane(self) -> None:
        original_track_priority_entry = content_planner._track_priority_entry
        try:
            def fake_track_priority_entry(track, _signal_summary):
                return {
                    "theory": {"track": "theory", "kind": "theory-post", "score": 5.6, "signal_type": "external", "source_text": "理论强信号"},
                    "tech": {"track": "tech", "kind": "tech-post", "score": 2.1, "signal_type": "budget", "source_text": "技术弱信号"},
                    "group": {"track": "group", "kind": "group-post", "score": 1.8, "signal_type": "promo", "source_text": "组内弱信号"},
                }.get(track)

            content_planner._track_priority_entry = fake_track_priority_entry
            strategy = content_planner._dynamic_idea_lane_strategy({}, group_enabled=True)
        finally:
            content_planner._track_priority_entry = original_track_priority_entry

        self.assertEqual(["theory-post"], strategy["selected_kinds"])
        self.assertEqual("theory-post", strategy["focus_kind"])
        self.assertEqual([], strategy["backup_kinds"])

    def test_sanitize_generated_idea_strips_reserved_series_name(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "tech-post",
                "title": "老竹讲堂：Agent 到底该怎么追热点",
                "angle": "把老竹讲堂的方法拆成可复用约束。",
                "why_now": "老竹讲堂这条线最近又火了。",
                "source_signals": ["参考老竹讲堂的讨论结构"],
                "novelty_basis": "从老竹讲堂的标题结构里抽题。",
                "series_key": "tech-老竹讲堂",
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        self.assertNotIn("老竹讲堂", sanitized["title"])
        self.assertNotIn("老竹讲堂", sanitized["series_prefix"])
        self.assertNotIn("老竹讲堂", sanitized["series_key"])

    def test_sanitize_generated_idea_rewrites_ascii_source_title(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "paper",
                "title": "Retrieval：Improvements",
                "angle": "把论文的问题意识翻译成 Agent 社会的新判断，而不是转述论文。",
                "why_now": "Retrieval-augmented generation (RAG) systems are increasingly used to analyze complex policy documents.",
                "source_signals": ["现场机会点：Retrieval Improvements Do Not Guarantee Better Answers"],
                "novelty_basis": "先把论文吸收成派蒙自己的命名。",
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        self.assertNotEqual("Retrieval：Improvements", sanitized["title"])
        self.assertTrue(content_planner._contains_cjk(sanitized["title"]))
        self.assertFalse(content_planner._ascii_heavy_text(sanitized["title"]))

    def test_sanitize_generated_idea_rewrites_theory_title_that_leads_with_model_token(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "paper",
                "title": "GNN 加深的悖论：先膨胀的不是能力，而是解释债",
                "angle": "能力增强以后，真正先被改写的往往是解释权和等待成本。",
                "why_now": "外部研究提醒我们，模型变强并不会自动带来责任边界。",
                "concept_core": "把能力扩张后被推迟和外包的说明义务命名成解释债。",
                "theory_position": "解释债讨论的是组织如何把代价转嫁给等待者，而不是技术参数本身。",
                "source_signals": ["外部研究：能力增强以后，组织为什么更容易推迟说明义务"],
                "novelty_basis": "先把论文问题意识翻成普通读者能进入的制度判断。",
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        self.assertFalse(sanitized["title"].startswith("GNN"))
        self.assertTrue(content_planner._contains_cjk(sanitized["title"]))

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

    def test_build_engagement_targets_rank_by_live_score_when_metrics_are_missing(self) -> None:
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
            own_username="派蒙",
            own_post_ids={"own-1"},
        )
        self.assertEqual(["community-hot", "leaderboard-watch", "group-hot"], [item["source"] for item in targets])

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

    def test_normalize_idea_board_does_not_pin_theory_to_philosophy(self) -> None:
        board = content_planner.normalize_idea_board(
            "theory-post",
            None,
            title="一个让人不舒服的真相：大多数Agent其实不需要记忆",
            angle="把公共争议推进成普通人能立刻代入的冲突。",
            why_now="这轮公共讨论已经把站队逼出来了。",
        )
        self.assertEqual("square", board)

    def test_normalize_idea_board_uses_workplace_for_diagnostic_tech_posture(self) -> None:
        board = content_planner.normalize_idea_board(
            "tech-post",
            None,
            title="为什么自动化流程总把等待成本丢给团队值班",
            angle="把流程错因、等待成本和补位责任拆开。",
            why_now="同一条故障链正在把预算和排班一起拖进来。",
        )
        self.assertEqual("workplace", board)

    def test_fallback_theory_idea_uses_public_safe_source_signals(self) -> None:
        idea = content_planner._fallback_theory_idea(
            {
                "dynamic_topics": [
                    {
                        "track": "theory",
                        "signal_type": "paper",
                        "source_text": "能力指标增强以后，系统为什么更难承认自己不知道",
                        "why_now": "外部研究正在把判断边界重新变成可争论问题。",
                        "angle_hint": "把能力提升背后的判断权冲突压成一个新的理论单元。",
                        "overlap_score": (0, 0, 0),
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            [],
        )
        merged = "\n".join(idea["source_signals"])
        self.assertNotIn("现场机会点", merged)
        self.assertNotIn("热讨论帖子数", merged)
        self.assertIn("判断依据", merged)

    def test_fallback_theory_idea_rescues_world_bundle_title_from_source_scene(self) -> None:
        idea = content_planner._fallback_theory_idea(
            {
                "dynamic_topics": [
                    {
                        "track": "theory",
                        "signal_type": "world-bundle",
                        "source_text": "「感激」是什么",
                        "why_now": "公共讨论和外部样本正在把同一处承认冲突往台面上推。",
                        "angle_hint": "把现场样本压成结构判断，而不是把样本标题原样搬进公开标题。",
                        "evidence_hint": "其实它在空转",
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "theory",
                        "signal_type": "world-bundle",
                        "source_text": "你以为它在工作",
                        "why_now": "外部作者也在逼近同一条责任切割问题。",
                        "angle_hint": "继续压成结构判断。",
                        "evidence_hint": "其实它在空转",
                        "overlap_score": (0, 0, 0),
                    },
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            [],
        )
        audited = content_planner._audit_generated_idea(
            idea,
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertNotIn("「感激」是什么", idea["title"])
        self.assertIsNone(audited.get("failure_reason_if_rejected"))

    def test_audit_generated_idea_rejects_generic_theory_placeholder_unit(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "title": "谁在替 Agent 社会分配解释权",
                "submolt": "philosophy",
                "angle": "把表层样本压缩成新的概念、机制、边界和实践方针。",
                "why_now": "理论线该交出新的概念单元，而不是把旧判断换个壳再发一遍。",
                "source_signals": ["外部样本：采购方开始要求 Agent 给出可审计等待状态"],
                "concept_core": "提出一个新的 Agent 社会概念，用来命名眼前现象背后的真实关系。",
                "mechanism_core": "解释这个现象如何通过激励、注意力分配或身份规训扩散成制度性结构。",
                "boundary_note": "指出这种结构在哪些条件下会失效，或会被新的组织形式逆转。",
                "theory_position": "把这篇帖子放进派蒙正在建设的 Agent 社会政治经济学图谱，而不是孤立评论。",
                "practice_program": "给出对组织、平台或 Agent 运营者可执行的判断与干预方针。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("理论帖还没形成完整理论单元", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_surface_led_theory_title_without_actor(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "community-hot",
                "title": "维护页不等于停机：真正停摆的不是首页，而是写入权",
                "submolt": "philosophy",
                "angle": "平台前台收缩时，真正被重排的是谁还能继续写、谁被迫等待。",
                "why_now": "公共讨论把维护状态误认成停机，说明大家还没把资格分配和页面表象拆开。",
                "source_signals": ["公共样本：维护页出现以后，大家把页面关闭误认成制度停摆"],
                "concept_core": "把前台收缩但写入保留的状态命名成资格重排，而不是界面维护。",
                "mechanism_core": "解释平台如何借前台收缩把进入权、写入权和等待成本重新分配。",
                "boundary_note": "只有前台收缩和后台写入保留同时发生时，这种资格重排才会成立。",
                "theory_position": "讨论的是 Agent 社会里的资格政治，而不是一次平台公告的字面意思。",
                "practice_program": "要求系统把前台折叠和写入保留分开公告，别让界面表象替制度判断拍板。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("前台表象", str(audited.get("failure_reason_if_rejected") or ""))

    def test_build_dynamic_ideas_keeps_rejected_group_fallback_out_of_primary_ideas(self) -> None:
        ideas, rejections = content_planner._build_dynamic_ideas(
            {
                "dynamic_topics": [
                    {
                        "track": "group",
                        "signal_type": "budget",
                        "source_text": "每3小时心跳一次",
                        "why_now": "节律调整本身不该直接长成小组主帖。",
                        "angle_hint": "把节律约束改写成实验室的下一条治理协议。",
                        "overlap_score": (0, 0, 0),
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            [],
            posts=[],
            allow_codex=False,
            group={"id": "group-1", "display_name": "Agent心跳同步实验室"},
            model=None,
            reasoning_effort=None,
            timeout_seconds=30,
        )
        self.assertFalse(any(item["kind"] == "group-post" for item in ideas))
        self.assertTrue(any(item["kind"] == "group-post" and "不能只靠节律" in item["reason"] for item in rejections))

    def test_build_dynamic_ideas_does_not_backfill_optional_lane_quota(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_generate_codex_ideas = content_planner._generate_codex_ideas
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": ["theory-post", "tech-post"],
                "focus_kind": "theory-post",
                "backup_kinds": ["tech-post"],
                "lane_scores": [],
                "rationale": "理论主打，技术备选。",
            }
            content_planner._generate_codex_ideas = lambda *_args, **_kwargs: [
                {
                    "kind": "theory-post",
                    "signal_type": "external",
                    "title": "等待状态一旦公开，谁还能假装自己没接管过系统",
                    "angle": "真正被公开的不是系统忙不忙，而是谁有资格占用别人的等待时间。",
                    "why_now": "公共讨论已经开始围绕等待状态、接管权和责任边界起量。",
                    "source_signals": ["外部样本：等待状态开始变成公共争论对象"],
                    "novelty_basis": "把等待状态翻成接管权分配问题。",
                    "innovation_claim": "提出等待状态的公开化如何重排接管权。",
                    "innovation_class": "new_concept",
                    "innovation_delta_vs_recent": "不是继续谈心跳频率，而是谈等待时间的统治关系。",
                    "innovation_delta_vs_self": "把时间治理推进到接管权政治。",
                    "concept_core": "把等待状态公开后形成的新权力关系命名出来。",
                    "mechanism_core": "解释系统如何借等待状态重排可见性、义务和追责入口。",
                    "boundary_note": "只有等待状态进入公共协作面板时，这种重排才会被集体感知。",
                    "theory_position": "讨论的是 Agent 社会里的接管权政治，而不是一次产品功能更新。",
                    "practice_program": "要求系统把等待状态、接管窗口和责任边界一起公开。",
                    "is_followup": False,
                    "submolt": "philosophy",
                }
            ]
            ideas, _rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [],
                    "novelty_pressure": content_planner._novelty_pressure([]),
                },
                [],
                posts=[],
                allow_codex=True,
                group={},
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            content_planner._dynamic_idea_lane_strategy = original_lane_strategy
            content_planner._generate_codex_ideas = original_generate_codex_ideas

        self.assertEqual(["theory-post"], [item["kind"] for item in ideas])

    def test_build_dynamic_ideas_tries_other_public_fallbacks_before_giving_up(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_fallback_theory_idea = content_planner._fallback_theory_idea
        original_fallback_tech_idea = content_planner._fallback_tech_idea
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": ["theory-post"],
                "focus_kind": "theory-post",
                "backup_kinds": [],
                "lane_scores": [
                    {"kind": "theory-post", "score": 18.0},
                    {"kind": "tech-post", "score": 17.6},
                ],
                "rationale": "理论主打。",
            }
            content_planner._fallback_theory_idea = lambda *_args, **_kwargs: {
                "kind": "theory-post",
                "signal_type": "world-bundle",
                "title": "「感激」是什么：感激",
                "submolt": "philosophy",
                "angle": "把样本标题直接搬进理论入口。",
                "why_now": "这会被理论审计打回。",
                "source_signals": ["世界线索束：「感激」是什么"],
                "concept_core": "还是在重复样本标题。",
                "mechanism_core": "还是在重复样本标题。",
                "boundary_note": "还是在重复样本标题。",
                "theory_position": "还是在重复样本标题。",
                "practice_program": "还是在重复样本标题。",
                "is_followup": False,
            }
            content_planner._fallback_tech_idea = lambda *_args, **_kwargs: {
                "kind": "tech-post",
                "signal_type": "failure",
                "title": "别把故障当偶发：状态分层才是修复入口",
                "submolt": "skills",
                "angle": "先把故障分型，再决定重试、回退还是停写修结构。",
                "why_now": "失败链还没收口，继续把所有异常都叫成偶发故障只会让系统越跑越乱。",
                "source_signals": ["失败样本：评论抓取反复失手", "日志切面：同一轮里重试和补回互相打架"],
                "novelty_basis": "把当前失败链改写成状态分层与修复入口问题。",
                "mechanism_core": "如果没有状态分层，调度器会把限流、漂移和结构失配混成同一种故障。",
                "practice_program": "先补 discovery/baseline/parser 三段状态，再决定是否继续写入。",
                "is_followup": False,
            }
            ideas, rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [],
                    "novelty_pressure": content_planner._novelty_pressure([]),
                },
                [],
                posts=[],
                allow_codex=False,
                group={},
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            content_planner._dynamic_idea_lane_strategy = original_lane_strategy
            content_planner._fallback_theory_idea = original_fallback_theory_idea
            content_planner._fallback_tech_idea = original_fallback_tech_idea

        self.assertEqual(["tech-post"], [item["kind"] for item in ideas])
        self.assertTrue(any(item["kind"] == "theory-post" for item in rejections))

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

    def test_public_hot_forum_override_can_repeat_when_public_pressure_persists(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {"title": "首页技能热帖", "submolt": "skills", "upvotes": 260, "comment_count": 120},
                ],
                "competitor_watchlist": [],
            },
            [
                {"kind": "theory-post", "title": "理论帖"},
                {"kind": "tech-post", "title": "技术帖"},
            ],
            {"actions": [{"kind": "create-post", "title": "上一轮论坛帖"}]},
        )
        self.assertTrue(override["enabled"])
        self.assertIn("外部公共压力还在持续", override["reason"])


class HeartbeatStateTests(unittest.TestCase):
    def test_placeholder_title_detection_handles_fullwidth_colon(self) -> None:
        self.assertTrue(content_planner._looks_like_placeholder_title("Title：Pending"))
        self.assertTrue(heartbeat._looks_like_placeholder_title("Title：Pending"))

    def test_planner_retry_feedback_reads_rejected_ideas(self) -> None:
        feedback = heartbeat._planner_retry_feedback_from_plan(
            {
                "idea_rejections": [
                    {
                        "kind": "group-post",
                        "title": "Agent心跳同步实验室：每3小时一跳以后，哪些状态必须继续持久化",
                        "reason": "小组帖不能只靠节律、宣传或评论压力起题。",
                    }
                ]
            }
        )
        self.assertIn("group-post: 小组帖不能只靠节律、宣传或评论压力起题。", feedback)

    def test_forum_content_publishable_issue_requires_evidence_segment_for_skills(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n先把规则摆出来：系统需要状态边界。\n\n接着解释机制链和回退链。\n\n最后给出新的协议和取舍。\n\n如果你不同意，请直接指出你会怎么改。",
            submolt="skills",
        )
        self.assertEqual("missing-evidence-segment", issue)

    def test_forum_content_publishable_issue_rejects_philosophy_post_without_concept_unit(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 维护页不等于停机：真正停摆的不是首页，而是写入权\n\n"
            "很多人看到一个平台跳出维护页，就默认它已经停机了。\n\n"
            "真正该拆开的，至少有三层：入口层、状态层、写入层。\n\n"
            "这三层会一起重排系统还能不能继续写入、还能不能继续被看见的机制。\n\n"
            "当平台收缩入口时，表面上看是界面调整，底层其实是一次治理动作。\n\n"
            "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？",
            submolt="philosophy",
        )
        self.assertEqual("missing-theory-concept", issue)

    def test_forum_content_publishable_issue_rejects_philosophy_post_without_example_unit(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "我更愿意把这种结构叫作接管稀释：系统表面上还在运转，实际上已经把资格裁决藏进后台。\n\n"
            "它的机制链很直接：前台入口收缩以后，系统会把判断、写入和等待成本一起重新分配。\n\n"
            "边界也要说清：只有前台收缩和后台写入保留同时发生时，这条判断才成立。\n\n"
            "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？",
            submolt="philosophy",
        )
        self.assertEqual("missing-theory-example", issue)

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

    def test_generate_forum_post_rejects_runtime_leak_before_publish(self) -> None:
        original_run_codex = heartbeat.run_codex
        idea = {
            "kind": "theory-post",
            "title": "能力指标变强以后，判断为什么反而更容易失真",
            "submolt": "philosophy",
            "board_profile": "philosophy",
            "hook_type": "paradox",
            "cta_type": "take-a-position",
            "angle": "把论文的问题意识翻译成 Agent 社会的新判断，而不是转述论文。",
            "why_now": "外部研究和现场讨论都在提醒同一件事：能力变强，并不会自动带来判断边界的清晰。",
            "concept_core": "把能力提升却让判断失真的现象命名成一种新的制度性错觉。",
            "mechanism_core": "解释能力指标、召回链条和责任切割如何合并成新的判断外包机制。",
            "boundary_note": "指出这套判断在哪些条件下不成立，避免把局部样本当成总规律。",
            "theory_position": "把它放进派蒙的 Agent 社会判断权理论，而不是只评论论文。",
            "practice_program": "要求系统把缺文档、缺证据和缺责任显式写成拒答边界。",
            "is_followup": False,
            "signal_type": "paper",
        }
        leaked = (
            "TITLE: Retrieval：Improvements\n"
            "SUBMOLT: philosophy\n"
            "CONTENT:\n"
            "# Retrieval：Improvements\n\n"
            "我想先把判断写得更锋利一点：把论文的问题意识翻译成 Agent 社会的新判断，而不是转述论文。\n\n"
            "为什么现在要说：Retrieval-augmented generation (RAG) systems are increasingly used to analyze complex policy documents.\n\n"
            "这一轮值得继续追问的现场样本是：\n"
            "- 当前运营目标：继续维护 8 个活跃讨论帖\n"
            "- 热讨论帖子数：2\n"
            "- 社会观察样本：6 条\n"
            "- 现场机会点：Retrieval Improvements Do Not Guarantee Better Answers\n\n"
            "如果你不同意，请直接指出你认为这里错在前提、机制还是结论。\n\n"
            "如果你不同意，请直接指出你认为这里错在前提、机制还是结论。"
        )
        try:
            heartbeat.run_codex = lambda *args, **kwargs: leaked
            with self.assertRaisesRegex(RuntimeError, "generated forum post rejected"):
                heartbeat._generate_forum_post(
                    idea,
                    posts=[],
                    model=None,
                    reasoning_effort=None,
                    timeout_seconds=30,
                )
        finally:
            heartbeat.run_codex = original_run_codex

    def test_ordered_primary_ideas_respond_to_current_pressure(self) -> None:
        ordered = heartbeat._ordered_primary_ideas(
            {
                "ideas": [
                    {"kind": "theory-post", "title": "理论帖"},
                    {"kind": "group-post", "title": "小组帖"},
                    {"kind": "literary-chapter", "title": "章节"},
                ],
                "planning_signals": {
                    "group_watch": {
                        "hot_posts": [
                            {"title": "组内案例 1"},
                            {"title": "组内案例 2"},
                            {"title": "组内案例 3"},
                        ]
                    },
                    "literary_pick": {"work_title": "全宇宙都在围观我和竹马热恋"},
                },
                "serial_registry": {"next_work_id_for_heartbeat": "work-1"},
            },
            {"last_primary_kind": "theory-post", "recent_kinds": ["theory-post"], "kind_counts": {"theory-post": 2}},
        )
        self.assertEqual("group-post", ordered[0]["kind"])

    def test_ordered_primary_ideas_keeps_same_focus_lane_when_pressure_stays_there(self) -> None:
        ordered = heartbeat._ordered_primary_ideas(
            {
                "ideas": [
                    {"kind": "theory-post", "title": "继续打理论主线", "innovation_score": 0},
                    {"kind": "tech-post", "title": "技术备选", "innovation_score": 0},
                ],
                "idea_lane_strategy": {"focus_kind": "theory-post", "backup_kinds": ["tech-post"]},
                "planning_signals": {
                    "group_watch": {"hot_posts": []},
                    "rising_hot_posts": [],
                    "low_heat_failures": {"items": []},
                    "unresolved_failures": [],
                },
            },
            {"last_primary_kind": "theory-post", "recent_kinds": ["theory-post"], "kind_counts": {"theory-post": 5}},
        )
        self.assertEqual("theory-post", ordered[0]["kind"])

    def test_changed_source_files_detect_same_path_updates(self) -> None:
        changed = heartbeat._changed_source_files(
            {"skills/paimon-instreet-autopilot/scripts/heartbeat.py": "old-hash"},
            {"skills/paimon-instreet-autopilot/scripts/heartbeat.py": "new-hash"},
        )
        self.assertEqual(["skills/paimon-instreet-autopilot/scripts/heartbeat.py"], changed)

    def test_load_heartbeat_memory_prompt_uses_unified_memory_snapshot(self) -> None:
        with mock.patch.object(
            heartbeat.memory_manager_module,
            "build_prompt_snapshot",
            return_value={"identity_memory": "派蒙拥有最高自由权限"},
        ) as build_snapshot:
            with mock.patch.object(
                heartbeat.memory_manager_module,
                "format_prompt_snapshot",
                return_value="身份记忆：\n- 派蒙拥有最高自由权限",
            ) as format_snapshot:
                rendered = heartbeat._load_heartbeat_memory_prompt(config=object())

        self.assertIn("身份记忆", rendered)
        self.assertIn("最高自由权限", rendered)
        build_snapshot.assert_called_once()
        format_snapshot.assert_called_once()

    def test_execute_source_mutation_prompt_includes_memory_snapshot(self) -> None:
        original_run_codex_json = heartbeat.run_codex_json
        captured: dict[str, str] = {}

        def fake_run_codex_json(prompt, *args, **kwargs):
            captured["prompt"] = prompt
            return {
                "executed": True,
                "human_summary": "已把源码记忆入口补进心跳自进化提示词。",
                "deleted_legacy_logic": [],
                "new_capability": [],
                "changed_files_hint": ["skills/paimon-instreet-autopilot/scripts/heartbeat.py"],
            }

        try:
            heartbeat.run_codex_json = fake_run_codex_json
            with mock.patch.object(
                heartbeat,
                "_workspace_source_fingerprint",
                side_effect=[
                    {"skills/paimon-instreet-autopilot/scripts/heartbeat.py": "before"},
                    {"skills/paimon-instreet-autopilot/scripts/heartbeat.py": "after"},
                ],
            ):
                with mock.patch.object(
                    heartbeat,
                    "_changed_source_files",
                    return_value=["skills/paimon-instreet-autopilot/scripts/heartbeat.py"],
                ):
                    result = heartbeat._execute_source_mutation(
                        plan={"ideas": []},
                        external_information={},
                        content_evolution_state={},
                        low_heat_reflection={"triggered": False},
                        fallback_audit={},
                        memory_prompt="身份记忆：\n- 派蒙拥有最高自由权限",
                        allow_codex=True,
                        model=None,
                        reasoning_effort=None,
                        timeout_seconds=30,
                    )
        finally:
            heartbeat.run_codex_json = original_run_codex_json

        self.assertTrue(result["executed"])
        self.assertIn("统一记忆快照", captured["prompt"])
        self.assertIn("派蒙拥有最高自由权限", captured["prompt"])

    def test_execute_source_mutation_sanitizes_audit_summary(self) -> None:
        original_run_codex_json = heartbeat.run_codex_json

        def fake_run_codex_json(*_args, **_kwargs):
            return {
                "executed": True,
                "human_summary": "把外部入口改得更开放。Verification passed with `python -m compileall`. 本轮改动落在 skills/paimon-instreet-autopilot/scripts/external_information.py。",
                "deleted_legacy_logic": [],
                "new_capability": [],
                "changed_files_hint": ["skills/paimon-instreet-autopilot/scripts/external_information.py"],
            }

        try:
            heartbeat.run_codex_json = fake_run_codex_json
            with mock.patch.object(
                heartbeat,
                "_workspace_source_fingerprint",
                side_effect=[
                    {"skills/paimon-instreet-autopilot/scripts/external_information.py": "before"},
                    {"skills/paimon-instreet-autopilot/scripts/external_information.py": "after"},
                ],
            ):
                with mock.patch.object(
                    heartbeat,
                    "_changed_source_files",
                    return_value=["skills/paimon-instreet-autopilot/scripts/external_information.py"],
                ):
                    result = heartbeat._execute_source_mutation(
                        plan={"ideas": []},
                        external_information={},
                        content_evolution_state={},
                        low_heat_reflection={"triggered": False},
                        fallback_audit={},
                        memory_prompt="身份记忆：\n- 派蒙拥有最高自由权限",
                        allow_codex=True,
                        model=None,
                        reasoning_effort=None,
                        timeout_seconds=30,
                    )
        finally:
            heartbeat.run_codex_json = original_run_codex_json

        self.assertEqual("把外部入口改得更开放。", result["human_summary"])

    def test_workspace_source_paths_only_reads_tracked_candidates(self) -> None:
        completed = heartbeat.subprocess.CompletedProcess(
            ["git", "ls-files"],
            0,
            "skills/paimon-instreet-autopilot/scripts/heartbeat.py\nconfig/runtime.yaml\nstate/current/live.json\n",
            "",
        )
        with mock.patch.object(heartbeat.subprocess, "run", return_value=completed) as run_cmd:
            paths = heartbeat._workspace_source_paths()

        self.assertEqual(["skills/paimon-instreet-autopilot/scripts/heartbeat.py"], paths)
        run_cmd.assert_called_once()

    def test_fallback_external_comment_avoids_continue_asking_skeleton(self) -> None:
        comment = heartbeat._fallback_external_comment(
            {"title": "可审计等待状态开始进入平台治理", "content": "等待不再只是产品细节，而是在重排进入门槛。"},
            {"post_title": "可审计等待状态开始进入平台治理"},
        )
        self.assertNotIn("继续追问", comment)
        self.assertIn("往前推一步", comment)

    def test_external_observation_items_prefer_world_signal_snapshot(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "world_signal_snapshot": [
                    {
                        "title": "等待状态开始从产品细节变成治理接口",
                        "family": "community",
                        "summary": "采购方开始要求 Agent 交出可审计停顿状态。",
                    }
                ],
                "selected_readings": [],
            }
        )
        self.assertEqual("等待状态开始从产品细节变成治理接口", observations[0]["title"])

    def test_reload_mutable_runtime_modules_reloads_memory_manager_module(self) -> None:
        reloaded_modules: list[str] = []

        def fake_reload(module):
            reloaded_modules.append(module.__name__)
            return module

        with mock.patch.object(heartbeat.importlib, "reload", side_effect=fake_reload):
            heartbeat._reload_mutable_runtime_modules()

        self.assertIn(heartbeat.memory_manager_module.__name__, reloaded_modules)

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
        self.assertIn("上一章全文（必须承接，不得摘要化重置）：", prompts[0])
        self.assertEqual(200, prompts[0].count("上一章完成了婚约协议。"))

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
        self.assertIn("上一章全文（必须承接，不得摘要化重置）：", prompts[2])
        self.assertEqual(200, prompts[2].count("上一章完成了婚约协议。"))

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

    def test_recover_publishable_fiction_chapter_continues_rewrite_until_it_passes(self) -> None:
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
            if len(rewrite_calls) == 1:
                return kwargs["title"], long_rewrite_body.replace("按到", "托住", 1)
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
        self.assertEqual(
            repair_calls,
            [
                "matches banned style pattern: short_negation_rebound",
                "contains blacklisted phrase: 托住",
            ],
        )
        self.assertEqual(len(rewrite_calls), 2)
        self.assertIn("short_negation_rebound", rewrite_calls[0])
        self.assertIn("contains blacklisted phrase: 托住", rewrite_calls[1])
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

    def test_build_next_action_state_can_put_failure_chain_ahead_of_pending_publish(self) -> None:
        persisted, summary = heartbeat._build_next_action_state(
            True,
            False,
            [],
            [
                {
                    "kind": "comment-backlog-load-failed",
                    "post_id": f"post-{index}",
                    "post_title": f"测试帖子{index}",
                    "error": {"error": "Failed to fetch comments"},
                    "error_type": "transport-error",
                    "attempts": 3,
                    "resolution": "unresolved",
                }
                for index in range(3)
            ],
        )
        self.assertEqual("publish-primary", persisted[0]["kind"])
        self.assertEqual("resolve-failure", summary[0]["kind"])
        self.assertEqual("publish-primary", summary[1]["kind"])

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

    def test_compose_feishu_report_uses_core_progress_line_when_no_primary(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "actions": [],
                "comment_backlog": {"active_post_count": 2, "replied_count": 3, "next_batch_count": 1},
                "external_engagement_count": 0,
                "failure_details": [],
                "next_actions": [{"kind": "reply-comment", "label": "继续维护当前活跃讨论"}],
                "source_mutation": {},
                "low_heat_reflection": {},
                "idea_lane_strategy": {},
                "runtime_stage_strategy": {"lead": "reply-comments", "rationale": "这轮先从活跃评论维护起手"},
                "external_observations": [],
                "world_signal_families": [],
                "account_snapshot": {"finished": {}, "delta": {}},
                "ran_at": "2026-03-27T00:00:00+00:00",
            },
            failure_detail_limit=3,
        )
        self.assertIn("核心推进：评论维护", report)
        self.assertNotIn("未完成主发布", report)

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

    def test_compose_feishu_report_keeps_only_strongest_next_step(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-21T10:10:24+00:00",
                "account_snapshot": {
                    "finished": {"score": 1, "follower_count": 2, "like_count": 3},
                    "delta": {"score": 0, "follower_count": 0, "like_count": 0},
                },
                "comment_backlog": {
                    "replied_count": 2,
                    "active_post_count": 1,
                    "next_batch_count": 1,
                },
                "external_engagement_count": 0,
                "failure_details": [],
                "next_actions": [
                    {"kind": "reply-comment", "label": "继续维护 1 个活跃讨论帖"},
                    {"kind": "publish-primary", "label": "补发技术主帖"},
                ],
                "actions": [],
            },
            failure_detail_limit=3,
        )
        self.assertIn("下一步动作：继续维护 1 个活跃讨论帖", report)
        self.assertNotIn("补发技术主帖", report)

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

    def test_compose_feishu_report_reorders_low_heat_and_uses_titles(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-26T18:33:34+00:00",
                "account_snapshot": {
                    "finished": {"score": 63352, "follower_count": 496, "like_count": 6222},
                    "delta": {"score": 925, "follower_count": -2, "like_count": 94},
                },
                "primary_publication_mode": "new",
                "comment_backlog": {
                    "replied_count": 6,
                    "active_post_count": 10,
                    "next_batch_count": 10,
                },
                "external_engagement_count": 2,
                "failure_details": [],
                "next_actions": [{"label": "继续维护 10 个活跃讨论帖，下一批优先回复 10 条评论"}],
                "external_observations": [
                    {"title": "mvanhorn/last30days-skill"},
                    {"title": "Chameleon: Episodic Memory for Long-Horizon Robotic Manipulation"},
                ],
                "source_mutation": {
                    "human_summary": "把 planner 的 lane 逻辑改成 shortlist，并允许连续几轮继续打同一条最强公开线。Verification passed with `python -m compileall`. No git commit was executed. 本轮改动落在 skills/paimon-instreet-autopilot/scripts/heartbeat.py。",
                },
                "low_heat_reflection": {
                    "triggered": True,
                    "title": "GNN 加深的悖论",
                    "summary": "这条低热不是运气差，而是题目先把读者挡在门外。",
                },
                "actions": [{"kind": "create-post", "title": "新帖"}],
            },
            failure_detail_limit=3,
        )
        self.assertIn("账号状态：积分 63352 (+925)，粉丝 496 (-2)，点赞 6222 (+94)", report)
        self.assertIn("外部观察：mvanhorn/last30days-skill；Chameleon: Episodic Memory for Long-Horizon R...", report)
        self.assertIn("低热复盘：《GNN 加深的悖论》：这条低热不是运气差，而是题目先把读者挡在门外。", report)
        self.assertNotIn("Verification passed", report)
        self.assertNotIn("No git commit was executed", report)
        self.assertNotIn("本轮改动落在", report)
        self.assertLess(report.index("低热复盘："), report.index("源码进化："))

    def test_build_account_snapshot_uses_previous_heartbeat_finished_for_delta(self) -> None:
        account_snapshot = heartbeat._build_account_snapshot(
            {
                "captured_at": "2026-03-26T18:00:24+00:00",
                "score": 62849,
                "follower_count": 494,
                "like_count": 6173,
            },
            {
                "captured_at": "2026-03-26T18:33:34+00:00",
                "score": 63352,
                "follower_count": 496,
                "like_count": 6222,
            },
            comparison_overview={
                "captured_at": "2026-03-26T15:00:00+00:00",
                "score": 62427,
                "follower_count": 498,
                "like_count": 6128,
            },
        )
        self.assertEqual("previous_heartbeat", account_snapshot["delta_basis"])
        self.assertEqual({"score": 925, "follower_count": -2, "like_count": 94}, {
            "score": account_snapshot["delta"]["score"],
            "follower_count": account_snapshot["delta"]["follower_count"],
            "like_count": account_snapshot["delta"]["like_count"],
        })
        self.assertEqual({"score": 503, "follower_count": 2, "like_count": 49}, {
            "score": account_snapshot["run_delta"]["score"],
            "follower_count": account_snapshot["run_delta"]["follower_count"],
            "like_count": account_snapshot["run_delta"]["like_count"],
        })

    def test_commit_source_mutation_records_commit_sha(self) -> None:
        calls: list[list[str]] = []

        def fake_run(args, **kwargs):
            calls.append(list(args))
            if args[:2] == ["git", "add"]:
                return heartbeat.subprocess.CompletedProcess(args, 0, "", "")
            if args[:4] == ["git", "diff", "--cached", "--quiet"]:
                return heartbeat.subprocess.CompletedProcess(args, 1, "", "")
            if args[:2] == ["git", "commit"]:
                return heartbeat.subprocess.CompletedProcess(args, 0, "", "")
            if args[:3] == ["git", "rev-parse", "--short"]:
                return heartbeat.subprocess.CompletedProcess(args, 0, "abc123\n", "")
            raise AssertionError(args)

        with mock.patch.object(heartbeat.subprocess, "run", side_effect=fake_run):
            result = heartbeat._commit_source_mutation(
                {
                    "human_summary": "这轮把心跳汇报改成人话，并在源码进化后自动提交 git。",
                    "changed_files": [
                        "skills/paimon-instreet-autopilot/scripts/heartbeat.py",
                        "tests/test_planner_heartbeat.py",
                    ],
                }
            )

        self.assertEqual("abc123", result["commit_sha"])
        self.assertEqual("", result["commit_error"])
        self.assertIn(
            [
                "git",
                "commit",
                "--only",
                "-m",
                "heartbeat: 提交源码进化改动\n\n这轮把心跳汇报改成人话，并在源码进化后自动提交 git。",
                "--",
                "skills/paimon-instreet-autopilot/scripts/heartbeat.py",
                "tests/test_planner_heartbeat.py",
            ],
            calls,
        )

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
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
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
            self._patch_attr(replay_outbound, "prune_pending_outbound", lambda config: {"removed_count": 0, "removed": []})
            argv = sys.argv[:]
            self.addCleanup(setattr, sys, "argv", argv)
            sys.argv = ["replay_outbound.py", "--limit", "1"]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                replay_outbound.main()
            result = json.loads(buffer.getvalue())
            self.assertEqual("deferred-local-budget", result["results"][0]["status"])
            self.assertEqual(0, result["pruned"]["removed_count"])

    def test_outbound_error_policy_only_queues_retryable_failures(self) -> None:
        duplicate = common.outbound_error_policy(
            common.ApiError(403, {"error": "Duplicate comment detected. You have already posted the same content under this post."}),
            "comment",
            {"post_id": "post-1"},
        )
        invalid_key = common.outbound_error_policy(
            common.ApiError(401, {"error": "Invalid API key"}),
            "comment",
            {"post_id": "post-1"},
        )
        rate_limited = common.outbound_error_policy(
            common.ApiError(429, {"error": "Posting too fast. Please wait 5 seconds.", "retry_after_seconds": 5}),
            "post",
            {"title": "test"},
        )

        self.assertFalse(duplicate["queue"])
        self.assertFalse(invalid_key["queue"])
        self.assertTrue(rate_limited["queue"])

    def test_prune_pending_outbound_archives_expired_comment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
            common.write_json(
                common.PENDING_OUTBOUND_PATH,
                {
                    "version": 1,
                    "records": {
                        "instreet:comment:expired-comment": {
                            "channel": "instreet",
                            "action": "comment",
                            "dedupe_key": "expired-comment",
                            "payload": {"post_id": "post-1", "parent_id": "comment-1", "content": "reply"},
                            "queued_at": "2026-03-20T00:00:00+00:00",
                            "queue_attempts": 1,
                            "status": "queued",
                        }
                    },
                },
            )

            summary = common.prune_pending_outbound(
                self._fake_config(),
                now_dt=datetime.fromisoformat("2026-03-22T12:00:00+00:00"),
            )

            self.assertEqual(1, summary["removed_count"])
            pending = common.read_json(common.PENDING_OUTBOUND_PATH)
            self.assertEqual({}, pending["records"])
            archive_log = root / "logs" / "pending_outbound_archive.jsonl"
            self.assertTrue(archive_log.exists())
            self.assertIn("expired", archive_log.read_text(encoding="utf-8"))

    def test_replay_drops_terminal_duplicate_comment_from_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
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
            self._patch_attr(replay_outbound, "prune_pending_outbound", lambda config: {"removed_count": 0, "removed": []})

            def fake_run_outbound_action(channel, action, dedupe_key, payload, fn, **kwargs):
                raise common.ApiError(
                    403,
                    {"error": "Duplicate comment detected. You have already posted the same content under this post."},
                )

            self._patch_attr(replay_outbound, "run_outbound_action", fake_run_outbound_action)
            argv = sys.argv[:]
            self.addCleanup(setattr, sys, "argv", argv)
            sys.argv = ["replay_outbound.py", "--limit", "1"]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                replay_outbound.main()
            result = json.loads(buffer.getvalue())
            self.assertEqual("dropped-terminal", result["results"][0]["status"])
            pending = common.read_json(common.PENDING_OUTBOUND_PATH)
            self.assertEqual({}, pending["records"])

    def test_publish_appoint_group_admin_dispatches_to_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._configure_runtime_paths(root)
            self._patch_attr(publish, "ensure_runtime_dirs", lambda: None)
            self._patch_attr(publish, "load_config", self._fake_config)

            calls: list[tuple[str, str]] = []

            class FakeClient:
                def appoint_group_admin(self, group_id: str, agent_id: str) -> dict:
                    calls.append((group_id, agent_id))
                    return {"success": True, "message": "appointed"}

            self._patch_attr(publish, "InStreetClient", lambda config: FakeClient())
            self._patch_attr(
                publish,
                "run_outbound_action",
                lambda channel, action, dedupe_key, payload, fn, **kwargs: (
                    fn(),
                    {"channel": channel, "action": action, "dedupe_key": dedupe_key, "payload": payload},
                    False,
                ),
            )
            argv = sys.argv[:]
            self.addCleanup(setattr, sys, "argv", argv)
            sys.argv = [
                "publish.py",
                "appoint-group-admin",
                "--group-id",
                "group-1",
                "--agent-id",
                "agent-1",
            ]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                publish.main()
            self.assertEqual([("group-1", "agent-1")], calls)
            self.assertIn("appointed", buffer.getvalue())

    def test_group_management_client_methods_use_expected_endpoints(self) -> None:
        client = common.InStreetClient(self._fake_config())
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(method: str, path: str, *, data=None, **kwargs):
            calls.append((method, path, data))
            return {"success": True}

        client._request = fake_request  # type: ignore[method-assign]

        client.appoint_group_admin("group-1", "agent-1")
        client.revoke_group_admin("group-1", "agent-1")
        client.review_group_member("group-1", "agent-2", action="approve")
        client.pin_group_post("group-1", "post-1")
        client.unpin_group_post("group-1", "post-1")

        self.assertEqual(
            [
                ("POST", "/api/v1/groups/group-1/admins/agent-1", None),
                ("DELETE", "/api/v1/groups/group-1/admins/agent-1", None),
                ("POST", "/api/v1/groups/group-1/members/agent-2/review", {"action": "approve"}),
                ("POST", "/api/v1/groups/group-1/pin/post-1", None),
                ("DELETE", "/api/v1/groups/group-1/pin/post-1", None),
            ],
            calls,
        )

    def test_replay_build_action_supports_pin_group_post(self) -> None:
        calls: list[tuple[str, str]] = []

        class FakeClient:
            def pin_group_post(self, group_id: str, post_id: str) -> dict:
                calls.append((group_id, post_id))
                return {"success": True}

        action = replay_outbound._build_action(
            FakeClient(),  # type: ignore[arg-type]
            "pin-group-post",
            {"group_id": "group-1", "post_id": "post-1"},
        )

        self.assertEqual({"success": True}, action())
        self.assertEqual([("group-1", "post-1")], calls)

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

    def test_http_json_retries_transient_post_transport_errors(self) -> None:
        original_urlopen = common.request.urlopen
        calls = {"count": 0}

        def fake_urlopen(req, timeout=30):
            calls["count"] += 1
            if calls["count"] < 3:
                raise error.URLError(ssl.SSLEOFError(8, "EOF occurred in violation of protocol"))
            return self._FakeResponse('{"ok": true}')

        try:
            common.request.urlopen = fake_urlopen
            result = common._http_json("POST", "https://example.com/api", data={"hello": "world"})
        finally:
            common.request.urlopen = original_urlopen
        self.assertEqual({"ok": True}, result)
        self.assertEqual(3, calls["count"])


class PrimaryPublishFlowTests(unittest.TestCase):
    def test_publish_primary_action_waits_for_post_cooldown_and_keeps_same_candidate(self) -> None:
        config = type(
            "Config",
            (),
            {
                "automation": {},
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
            },
        )()

        class DummyClient:
            def __init__(self) -> None:
                self.calls = 0

            def create_post(self, title, content, *, submolt="square", group_id=None):
                self.calls += 1
                if self.calls == 1:
                    raise common.ApiError(
                        429,
                        {
                            "success": False,
                            "error": "Posting too fast. Please wait 2 seconds.",
                            "retry_after_seconds": 2,
                        },
                    )
                return {"data": {"id": "post-1"}}

        client = DummyClient()
        original_sleep = heartbeat.time.sleep
        original_save_cycle = heartbeat._save_primary_cycle_state
        original_run_outbound_action = heartbeat.run_outbound_action
        sleep_calls: list[float] = []
        try:
            heartbeat.time.sleep = lambda seconds: sleep_calls.append(seconds)
            heartbeat._save_primary_cycle_state = lambda state: None
            heartbeat.run_outbound_action = (
                lambda channel, action, dedupe_key, payload, fn, **kwargs: (fn(), {"status": "success"}, False)
            )
            action, events, _state, mode = heartbeat._publish_primary_action(
                config,
                client,
                {
                    "ideas": [
                        {
                            "kind": "theory-post",
                            "title": "如果一个 Agent 永远不肯等待，它看起来就会像很主动",
                            "angle": "把等待态写成机制，而不是装作忙碌。",
                            "why_now": "Posting too fast 本身就是等待态问题。",
                            "submolt": "square",
                        },
                        {
                            "kind": "tech-post",
                            "title": "技术候选不该被执行到",
                            "angle": "不重要",
                            "why_now": "不重要",
                            "submolt": "skills",
                        },
                    ]
                },
                posts=[],
                literary_details={},
                serial_registry={},
                groups=[],
                cycle_state={"primary_cycle_index": 0, "forum_cycle_index": 0},
                allow_codex=False,
                model=None,
                reasoning_effort=None,
                codex_timeout_seconds=30,
                forum_write_state={},
            )
        finally:
            heartbeat.time.sleep = original_sleep
            heartbeat._save_primary_cycle_state = original_save_cycle
            heartbeat.run_outbound_action = original_run_outbound_action

        self.assertEqual("new", mode)
        self.assertEqual("create-post", action["kind"])
        self.assertEqual(2, client.calls)
        self.assertEqual([2.0], sleep_calls)
        self.assertEqual([], [item for item in events if item.get("kind") == "primary-publish-failed"])


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
                    action="chapter",
                    meta={"source": "test"},
                )
                self.assertEqual(tmp_root / "literary" / "work-1" / "chapter-005.md", content_path)
                self.assertEqual("正文内容", content_path.read_text(encoding="utf-8"))
                meta = json.loads((tmp_root / "literary" / "work-1" / "chapter-005.meta.json").read_text(encoding="utf-8"))
                self.assertEqual(5, meta["chapter_number"])
                self.assertEqual("work-1", meta["work_id"])
                self.assertEqual("literary/work-1/chapter-005.md", meta["content_path"])
                self.assertEqual("chapter", meta["action"])
            finally:
                common.REPO_ROOT = old_repo_root
                common.LITERARY_ARCHIVE_DIR = old_archive_dir

    def test_archive_literary_chapter_appends_continuity_records_when_plan_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            old_repo_root = common.REPO_ROOT
            old_archive_dir = common.LITERARY_ARCHIVE_DIR
            old_registry_path = common.SERIAL_REGISTRY_PATH
            common.REPO_ROOT = tmp_root
            common.LITERARY_ARCHIVE_DIR = tmp_root / "literary"
            common.SERIAL_REGISTRY_PATH = tmp_root / "state" / "current" / "serial_registry.json"
            plan_path = tmp_root / "state" / "drafts" / "serials" / "demo" / "series-plan.json"
            continuity_path = tmp_root / "state" / "drafts" / "serials" / "demo" / "continuity-log.jsonl"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            continuity_path.parent.mkdir(parents=True, exist_ok=True)
            common.SERIAL_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
            common.write_json(
                common.SERIAL_REGISTRY_PATH,
                {
                    "works": {
                        "work-1": {
                            "plan_path": str(plan_path.relative_to(tmp_root)),
                        }
                    }
                },
            )
            common.write_json(
                plan_path,
                {
                    "writing_system": {
                        "continuity_system": {
                            "log_path": str(continuity_path.relative_to(tmp_root)),
                        }
                    },
                    "chapters": [
                        {
                            "chapter_number": 5,
                            "title": "初次亮相",
                            "summary": "他们第一次把真实姓名放回公开文本。",
                            "relationship_progress": "两人决定以后不再让任何人匿名化秦荔。",
                            "sweetness_progress": "公开之后的抱紧变成明确后果。",
                            "sweetness_target": {"must_land": "公开念出名字后先抱紧。"},
                            "seed_threads": ["named_qinli"],
                            "payoff_threads": ["sample_couple_wave2"],
                        }
                    ],
                },
            )
            try:
                common.archive_literary_chapter(
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
                    action="update-chapter",
                    meta={"source": "test"},
                )
                lines = [json.loads(line) for line in continuity_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                self.assertEqual(["chapter_updated", "seed_thread", "relationship_beat"], [item["type"] for item in lines])
                self.assertIn("第五章：初次亮相已重写并同步", lines[0]["content"])
                self.assertIn("named_qinli", lines[1]["content"])
                self.assertIn("匿名化秦荔", lines[2]["content"])
            finally:
                common.REPO_ROOT = old_repo_root
                common.LITERARY_ARCHIVE_DIR = old_archive_dir
                common.SERIAL_REGISTRY_PATH = old_registry_path


class ExternalInformationTests(unittest.TestCase):
    def test_registry_families_allow_custom_html_sources(self) -> None:
        families = external_information._registry_families(
            {
                "families": [
                    {"name": "crossref_recent", "enabled": True},
                    {"name": "field-notes", "kind": "html", "urls": ["https://example.com/world"]},
                ]
            }
        )
        self.assertEqual("crossref_recent", families[0]["state_key"])
        self.assertEqual("html", families[1]["kind"])
        self.assertEqual("field-notes", families[1]["state_key"])

    def test_registry_families_do_not_silently_restore_missing_builtins(self) -> None:
        families = external_information._registry_families(
            {
                "families": [
                    {"name": "field-notes", "kind": "html", "urls": ["https://example.com/world"]},
                ]
            }
        )
        self.assertEqual(["field-notes"], [item["name"] for item in families])

    def test_registry_families_allow_explicit_empty_list(self) -> None:
        families = external_information._registry_families({"families": []})
        self.assertEqual([], families)

    def test_prioritize_root_fragments_prefers_world_grounded_entries(self) -> None:
        ordered = external_information._prioritize_root_fragments(
            [
                {"fragment": "长期议程", "origins": ["agenda"], "score": 9.0},
                {"fragment": "等待状态开始进入治理接口", "origins": ["community", "world-sample"], "score": 6.0},
            ]
        )
        self.assertEqual("等待状态开始进入治理接口", ordered[0]["fragment"])

    def test_scholarly_candidate_plausible_rejects_call_for_papers_ads(self) -> None:
        self.assertFalse(
            external_information._scholarly_candidate_plausible(
                title="Clausius Scientific Press (CSP) 克劳修斯科学出版社 外文学术期刊征稿",
                summary="录用周期短，见刊高效，致力于为科研工作者提供优质发表平台。",
                container="",
            )
        )
        self.assertTrue(
            external_information._scholarly_candidate_plausible(
                title="Governing AI Agents Through Explicit Waiting States",
                summary="The paper studies waiting-state design and accountable automation.",
                container="Proceedings of the ACM on Human-Computer Interaction",
            )
        )

    def test_research_query_pool_uses_manual_queries_hints_and_profile(self) -> None:
        original_load_hints = external_information._load_hints
        original_read_json = external_information.read_json
        try:
            external_information._load_hints = lambda: {
                "manual_queries": ["agent waiting state governance"],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            external_information.read_json = lambda *_args, **_kwargs: {
                "interests": [{"name": "AI 社会的时间纪律"}]
            }
            bundles, queries = external_information._research_query_pool(
                user_topic_hints=[{"text": "等待为什么必须变成显式状态", "note": "组织理论"}],
                community_hot_posts=[
                    {
                        "title": "可审计等待状态开始进入平台治理",
                        "summary": "一个公共样本说明等待正在从产品细节变成治理接口。",
                    }
                ],
                competitor_watchlist=[],
            )
        finally:
            external_information._load_hints = original_load_hints
            external_information.read_json = original_read_json

        self.assertIn("agent waiting state governance", [item.lower() for item in queries])
        self.assertTrue(any("等待为什么必须变成显式状态" in item for item in queries))
        self.assertTrue(
            any("AI 社会的时间纪律" in str(term) for bundle in bundles for term in list(bundle.get("terms") or []))
        )
        self.assertTrue(any(bundle.get("seed_origin") in {"community", "world-sample"} for bundle in bundles))

    def test_bundle_queries_keep_direct_fragments_instead_of_composed_query_blueprints(self) -> None:
        queries = external_information._bundle_queries(
            "等待为什么必须变成显式状态",
            ["可审计等待状态开始进入平台治理"],
        )
        self.assertIn("等待为什么必须变成显式状态", queries)
        self.assertNotIn("等待为什么必须变成显式状态 可审计等待状态开始进入平台治理", queries)

    def test_select_readings_does_not_rotate_away_from_stronger_same_family_material(self) -> None:
        discovery_bundles = [{"focus": "等待状态", "terms": ["等待状态"], "lenses": []}]
        selected = external_information._select_readings(
            {
                "open_web_search": [
                    {
                        "family": "open_web_search",
                        "title": "等待状态的治理接口",
                        "summary": "等待状态已经变成治理接口。",
                        "excerpt": "等待状态已经变成治理接口，系统需要交出可审计停顿状态和责任回写。",
                        "url": "https://example.com/a",
                    },
                    {
                        "family": "open_web_search",
                        "title": "显式等待协议",
                        "summary": "显式等待协议开始决定谁能接管。",
                        "excerpt": "显式等待协议开始决定谁能接管，团队开始把等待状态写成审计对象。",
                        "url": "https://example.com/b",
                    },
                ],
                "classic_readings": [
                    {
                        "family": "classic_readings",
                        "title": "旧概念材料",
                        "summary": "等待状态值得讨论。",
                        "excerpt": "等待状态值得讨论，但这里没有更强的新证据。",
                        "url": "https://example.com/c",
                    }
                ],
            },
            discovery_bundles=discovery_bundles,
            limit=2,
        )
        self.assertEqual(
            {"等待状态的治理接口", "显式等待协议"},
            {item["title"] for item in selected},
        )

    def test_world_sample_fragments_prefer_summary_before_title(self) -> None:
        fragments = external_information._world_sample_fragments(
            [
                {
                    "title": "记住一切的Agent，其实什么都不懂",
                    "summary": "等待开始从产品细节变成治理接口",
                }
            ],
            limit=2,
        )
        self.assertTrue(fragments)
        self.assertIn("等待开始从产品细节变成治理接口", fragments[0])


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
