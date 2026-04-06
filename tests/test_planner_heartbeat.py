import contextlib
from datetime import datetime, timezone
import http.client as http_client
import io
import json
import ssl
import sys
import tempfile
from typing import Any
import unittest
from unittest import mock
from pathlib import Path
from urllib import error


sys.path.insert(0, "skills/paimon-instreet-autopilot/scripts")

import common  # noqa: E402
import content_planner  # noqa: E402
import external_information  # noqa: E402
import fiction_plan_audit  # noqa: E402
import heartbeat  # noqa: E402
import heartbeat_supervisor  # noqa: E402
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

    def test_recommended_next_action_from_live_pressure_prefers_public_window(self) -> None:
        action = content_planner._recommended_next_action_from_live_pressure(
            signal_summary={"pending_reply_posts": [], "unresolved_failures": []},
            ideas=[{"kind": "theory-post", "title": "谁在切走 Agent 的等待资格"}],
            engagement_targets=[{"priority": 0}],
            dm_targets=[],
            public_override={"enabled": True},
            literary_pick=None,
        )
        self.assertIn("谁在切走 Agent 的等待资格", action)
        self.assertIn("公共窗口", action)

    def test_literary_pick_pressure_text_prefers_chapter_conflict_over_registry_slogan(self) -> None:
        pressure = content_planner._literary_pick_pressure_text(
            {
                "work_title": "全宇宙都在围观我和竹马热恋",
                "next_planned_title": "第五十三章：最稳定的变量原来不是爱是选择",
                "chapter_plan": {
                    "summary": "制度开始把两人的选择压成公开样本，谁先让步就要失去解释权。",
                    "key_conflict": "主角被要求用一封公开回应换取继续留在试验名单里。",
                    "hook": "章尾有人偷走了那封还没发出的回应。",
                },
            }
        )
        self.assertIn("解释权", pressure)
        self.assertNotIn("注册表", pressure)

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
        self.assertFalse(any(item["signal_type"] == "freeform" for item in opportunities))
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

    def test_dynamic_opportunities_surface_strongest_pressure_before_track_order(self) -> None:
        signal_summary = {
            "external_information": {
                "selected_readings": [
                    {
                        "family": "open_web_search",
                        "title": "Waiting interface",
                        "summary": "等待开始从产品细节变成治理接口，真正变化的是谁还能解释失败、谁来接手后果。",
                        "excerpt": "组织开始要求系统交出可审计停顿状态。",
                        "url": "https://example.com/waiting-interface",
                    }
                ]
            },
            "unresolved_failures": [
                {
                    "post_title": "评论抓取反复失手",
                    "error": "评论抓取连续三轮超时后还在重试。",
                }
            ],
            "novelty_pressure": content_planner._novelty_pressure([]),
        }
        opportunities = content_planner._dynamic_opportunities(
            signal_summary=signal_summary,
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertTrue(any(item["track"] == "tech" for item in opportunities))
        self.assertEqual("theory", opportunities[0]["track"])
        self.assertEqual("external", opportunities[0]["signal_type"])
        self.assertIn("治理接口", opportunities[0]["source_text"])

    def test_theme_anchor_fragments_ignore_query_blueprints_and_recent_self_titles(self) -> None:
        anchors = content_planner._theme_anchor_fragments(
            {
                "external_information": {
                    "research_queries": ["查询蓝图"],
                    "world_signal_snapshot": [
                        {
                            "title": "等待状态",
                            "summary": "等待治理接口已经开始压到组织门口",
                        }
                    ],
                    "discovery_bundles": [
                        {
                            "focus": "等待状态进入治理接口",
                            "conflict_note": "接手资格重排",
                            "rationale": "真正变化的是谁先解释、谁后接手。",
                        }
                    ],
                },
                "recent_top_posts": [{"title": "自传锚点"}],
                "user_topic_hints": [],
                "content_objectives": [],
            }
        )
        self.assertTrue(any("治理接口" in anchor or "接手资格" in anchor for anchor in anchors))
        self.assertFalse(any("查询蓝图" in anchor for anchor in anchors))
        self.assertFalse(any("自传锚点" in anchor for anchor in anchors))

    def test_world_seed_texts_ignore_bundle_fetch_terms_and_external_query_handles(self) -> None:
        seeds = content_planner._world_seed_texts(
            {
                "external_information": {
                    "discovery_bundles": [
                        {
                            "focus": "等待状态进入治理接口",
                            "pressure_summary": "接手资格重新排序；采购方开始要求可审计停顿状态",
                            "fetch_terms": ["更漂亮的查询蓝图"],
                            "terms": ["等待状态进入治理接口", "更漂亮的查询蓝图"],
                        }
                    ],
                    "selected_readings": [
                        {
                            "family": "manual_web",
                            "title": "「感激」是什么",
                            "summary": "等待状态开始决定谁能接手",
                            "excerpt": "等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                            "query": "社区标题派生查询",
                        }
                    ],
                },
                "user_topic_hints": [],
            },
            limit=6,
        )
        self.assertTrue(any("治理接口" in seed or "接手资格" in seed for seed in seeds))
        self.assertFalse(any("更漂亮的查询蓝图" in seed for seed in seeds))
        self.assertFalse(any("社区标题派生查询" in seed for seed in seeds))

    def test_theme_anchor_fragments_use_world_entry_points_before_bucket_titles(self) -> None:
        anchors = content_planner._theme_anchor_fragments(
            {
                "external_information": {
                    "world_entry_points": [
                        {
                            "title": "等待状态开始决定谁能接手",
                            "pressure": "采购方开始要求 Agent 交出可审计停顿状态。",
                            "evidence": "真实案例把等待状态、回写日志和接手资格压到同一条失败链里。",
                        }
                    ],
                    "raw_candidates": [
                        {
                            "title": "ToolboxX / Explicit Waiting",
                            "summary": "标题壳不该继续主导锚点。",
                        }
                    ],
                },
                "user_topic_hints": [],
                "content_objectives": [],
            }
        )
        self.assertTrue(any(anchor in {"等待状态", "接手资格", "治理接口"} for anchor in anchors))
        self.assertFalse(any("ToolboxX" in anchor for anchor in anchors))

    def test_iter_external_world_candidates_ranks_cross_list_signal_strength(self) -> None:
        candidates = content_planner._iter_external_world_candidates(
            {
                "raw_candidates": [
                    {
                        "family": "manual_web",
                        "title": "模糊线索",
                        "summary": "有人在讨论等待。",
                    }
                ],
                "manual_web_sources": [
                    {
                        "family": "manual_web",
                        "title": "等待状态开始决定谁能接手",
                        "summary": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "excerpt": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "url": "https://example.com/waiting",
                    }
                ],
            }
        )
        self.assertTrue(candidates)
        self.assertEqual("等待状态开始决定谁能接手", candidates[0]["title"])

    def test_iter_external_world_candidates_reframes_source_title_shell(self) -> None:
        candidates = content_planner._iter_external_world_candidates(
            {
                "selected_readings": [
                    {
                        "family": "manual_web",
                        "title": "「感激」是什么",
                        "summary": "等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                        "excerpt": "等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                    }
                ]
            }
        )
        self.assertTrue(candidates)
        self.assertEqual("等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态", candidates[0]["title"])

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

    def test_dynamic_opportunities_failure_why_now_uses_live_failure_text(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "unresolved_failures": [
                    {
                        "post_title": "退款工单没有接手人",
                        "summary": "退款工单连续三次回写失败，接手状态始终没切出来。",
                        "error": "writeback timeout",
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        failure = next(item for item in opportunities if item.get("signal_type") == "failure")
        self.assertIn("退款工单连续三次回写失败", failure["why_now"])
        self.assertNotEqual("现场失败链路", failure["why_now"])

    def test_track_signal_bundle_reframes_theory_bundle_before_title_generation(self) -> None:
        bundle = content_planner._track_signal_bundle(
            "theory",
            {
                "dynamic_topics": [
                    {
                        "track": "theory",
                        "signal_type": "world-bundle",
                        "source_text": "「感激」是什么",
                        "why_now": "公共讨论和外部样本正在把同一处承认冲突往台面上推。",
                        "angle_hint": "把现场样本压成结构判断，而不是把样本标题原样搬进公开标题。",
                        "evidence_hint": "其实它在空转",
                        "quality_score": 4.8,
                        "freshness_score": 2.4,
                        "world_score": 1.1,
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "theory",
                        "signal_type": "world-bundle",
                        "source_text": "你以为它在工作",
                        "why_now": "外部作者也在逼近同一条责任切割问题。",
                        "angle_hint": "继续压成结构判断。",
                        "evidence_hint": "其实它在空转",
                        "quality_score": 4.5,
                        "freshness_score": 2.1,
                        "world_score": 1.0,
                        "overlap_score": (0, 0, 0),
                    },
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
        )
        self.assertIn(str(bundle.get("public_focus_text") or ""), {"承认冲突", "责任切割"})
        self.assertNotIn("「感激」是什么", str(bundle.get("public_title_seed") or ""))
        self.assertTrue(
            any(fragment in str(bundle.get("public_title_seed") or "") for fragment in ("承认冲突", "责任切割"))
        )

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

    def test_dynamic_opportunities_accept_evidence_rich_adjacent_academic_samples(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "external_information": {
                    "raw_candidates": [
                        {
                            "family": "crossref_recent",
                            "title": "Escalation Windows in Assisted Procurement Review",
                            "summary": "Case study with before/after comparisons: one approval stack cut duplicate escalation failures from 11 to 3 after teams exposed one stalled review step.",
                            "excerpt": "Before the rewrite there were 11 duplicate escalations; after the rewrite, the same review step only failed 3 times.",
                        }
                    ]
                },
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
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
        self.assertFalse(any("EndoVGGT" in str(item.get("source_text") or "") for item in opportunities))

    def test_dynamic_opportunities_do_not_promote_user_hint_without_matching_world_pressure(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "external_information": {
                    "world_entry_points": [
                        {
                            "pressure": "退款工单连续三次回写失败，等待状态开始进入治理接口",
                            "summary": "等待状态开始决定谁能接手。",
                        }
                    ]
                },
                "user_topic_hints": [
                    {
                        "text": "承认秩序为什么会先分层",
                        "note": "社会理论入口",
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        self.assertFalse(any(str(item.get("signal_type") or "") == "user-hint" for item in opportunities))

    def test_dynamic_opportunities_only_use_user_hint_as_world_refinement(self) -> None:
        opportunities = content_planner._dynamic_opportunities(
            signal_summary={
                "account": {"unread_notification_count": 0},
                "external_information": {
                    "world_entry_points": [
                        {
                            "pressure": "等待状态开始进入治理接口，采购方要求显式签收。",
                            "summary": "显式等待状态开始决定谁能接手。",
                        }
                    ]
                },
                "user_topic_hints": [
                    {
                        "text": "等待为什么必须变成显式状态",
                        "note": "把等待状态放进治理接口看",
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
            },
            recent_titles=[],
            heartbeat_hours=3,
        )
        hint_opportunity = next(
            (item for item in opportunities if str(item.get("signal_type") or "") == "user-hint"),
            {},
        )
        self.assertTrue(hint_opportunity)
        self.assertIn("治理接口", str(hint_opportunity.get("why_now") or ""))
        self.assertIn("显式等待状态开始决定谁能接手", str(hint_opportunity.get("evidence_hint") or ""))

    def test_dynamic_idea_lane_strategy_allows_single_focus_lane(self) -> None:
        original_track_priority_entry = content_planner._track_priority_entry
        original_live_track_order = content_planner._live_track_order
        try:
            def fake_track_priority_entry(track, _signal_summary):
                return {
                    "theory": {"track": "theory", "kind": "theory-post", "score": 5.6, "signal_type": "external", "source_text": "理论强信号"},
                    "tech": {"track": "tech", "kind": "tech-post", "score": 2.1, "signal_type": "budget", "source_text": "技术弱信号"},
                    "group": {"track": "group", "kind": "group-post", "score": 1.8, "signal_type": "promo", "source_text": "组内弱信号"},
                }.get(track)

            content_planner._track_priority_entry = fake_track_priority_entry
            content_planner._live_track_order = lambda *_args, **_kwargs: ["theory", "tech", "group"]
            strategy = content_planner._dynamic_idea_lane_strategy({}, group_enabled=True)
        finally:
            content_planner._track_priority_entry = original_track_priority_entry
            content_planner._live_track_order = original_live_track_order

        self.assertEqual(["theory-post"], strategy["selected_kinds"])
        self.assertEqual("theory-post", strategy["focus_kind"])
        self.assertEqual([], strategy["backup_kinds"])

    def test_live_track_order_follows_pressure_scores_instead_of_builtin_sequence(self) -> None:
        order = content_planner._live_track_order(
            {
                "dynamic_topic_bundles": [
                    {"track": "tech", "pressure_score": 6.2},
                    {"track": "theory", "pressure_score": 4.8},
                    {"track": "group", "pressure_score": 7.1},
                ],
                "dynamic_topics": [],
            },
            group_enabled=False,
        )
        self.assertEqual(["tech", "theory"], order)

    def test_dynamic_idea_lane_strategy_leaves_focus_blank_when_grounded_lanes_are_neck_and_neck(self) -> None:
        original_track_priority_entry = content_planner._track_priority_entry
        original_lane_entry_grounded = content_planner._lane_entry_grounded
        original_live_track_order = content_planner._live_track_order
        try:
            def fake_track_priority_entry(track, _signal_summary):
                return {
                    "theory": {"track": "theory", "kind": "theory-post", "score": 5.6, "signal_type": "external", "source_text": "理论强信号"},
                    "tech": {"track": "tech", "kind": "tech-post", "score": 5.32, "signal_type": "failure", "source_text": "技术强信号"},
                }.get(track)

            content_planner._track_priority_entry = fake_track_priority_entry
            content_planner._lane_entry_grounded = lambda *_args, **_kwargs: True
            content_planner._live_track_order = lambda *_args, **_kwargs: ["theory", "tech"]
            strategy = content_planner._dynamic_idea_lane_strategy({}, group_enabled=False)
        finally:
            content_planner._track_priority_entry = original_track_priority_entry
            content_planner._lane_entry_grounded = original_lane_entry_grounded
            content_planner._live_track_order = original_live_track_order

        self.assertEqual(["theory-post", "tech-post"], strategy["selected_kinds"])
        self.assertEqual("", strategy["focus_kind"])
        self.assertEqual(["theory-post", "tech-post"], strategy["backup_kinds"])
        self.assertIn("不提前钉死主位", strategy["rationale"])

    def test_dynamic_idea_lane_strategy_rationale_uses_object_pressure_not_lane_name(self) -> None:
        original_track_priority_entry = content_planner._track_priority_entry
        original_live_track_order = content_planner._live_track_order
        try:
            def fake_track_priority_entry(track, _signal_summary):
                return {
                    "theory": {
                        "track": "theory",
                        "kind": "theory-post",
                        "score": 5.6,
                        "signal_type": "external",
                        "source_text": "谁在切走 Agent 的等待资格",
                    },
                    "tech": {
                        "track": "tech",
                        "kind": "tech-post",
                        "score": 5.2,
                        "signal_type": "failure",
                        "source_text": "退款工单连续三次回写失败",
                    },
                }.get(track)

            content_planner._track_priority_entry = fake_track_priority_entry
            content_planner._live_track_order = lambda *_args, **_kwargs: ["theory", "tech"]
            strategy = content_planner._dynamic_idea_lane_strategy({}, group_enabled=False)
        finally:
            content_planner._track_priority_entry = original_track_priority_entry
            content_planner._live_track_order = original_live_track_order

        self.assertIn("谁在切走 Agent 的等待资格", strategy["rationale"])
        self.assertNotIn("theory-post", strategy["rationale"])
        self.assertNotIn("tech-post", strategy["rationale"])

    def test_dynamic_idea_lane_strategy_keeps_residual_pressure_as_observation_only(self) -> None:
        original_track_priority_entry = content_planner._track_priority_entry
        try:
            content_planner._track_priority_entry = lambda *_args, **_kwargs: None
            strategy = content_planner._dynamic_idea_lane_strategy({}, group_enabled=False)
        finally:
            content_planner._track_priority_entry = original_track_priority_entry

        self.assertEqual([], strategy["selected_kinds"])
        self.assertEqual("", strategy["focus_kind"])
        self.assertEqual([], strategy["lane_scores"])
        self.assertIn("空心题", strategy["rationale"])

    def test_build_dynamic_ideas_allows_empty_result_when_no_grounded_lane_exists(self) -> None:
        ideas, rejections = content_planner._build_dynamic_ideas(
            {
                "novelty_pressure": content_planner._novelty_pressure([]),
                "dynamic_topics": [],
                "external_information": {},
                "user_topic_hints": [],
                "content_objectives": ["继续维护记忆系统"],
            },
            [],
            posts=[],
            allow_codex=False,
            group={},
            model=None,
            reasoning_effort=None,
            timeout_seconds=1,
        )
        self.assertEqual([], ideas)
        self.assertEqual([], rejections)

    def test_build_dynamic_ideas_uses_live_bundle_order_before_kind_default(self) -> None:
        original_dynamic_idea_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_track_signal_bundle = content_planner._track_signal_bundle
        original_bundle_has_grounding = content_planner._bundle_has_grounding
        original_fallback_track_seed = content_planner._fallback_track_seed
        original_fallback_theory_idea = content_planner._fallback_theory_idea
        original_fallback_tech_idea = content_planner._fallback_tech_idea
        original_audit_generated_idea = content_planner._audit_generated_idea
        original_generated_idea_allowed = content_planner._generated_idea_allowed
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": [],
                "focus_kind": "",
                "backup_kinds": [],
                "lane_scores": [],
            }
            bundles = {
                "theory": {
                    "track": "theory",
                    "score": 4.2,
                    "focus_text": "等待资格开始重新分配",
                },
                "tech": {
                    "track": "tech",
                    "score": 4.2,
                    "focus_text": "空 owner 任务需要可签收回执",
                },
            }

            content_planner._track_signal_bundle = lambda track, *_args, **_kwargs: bundles.get(track, {})
            content_planner._bundle_has_grounding = lambda bundle, *, track: bool(bundle)
            content_planner._fallback_track_seed = lambda track, *_args, **_kwargs: {
                "source_text": str((bundles.get(track) or {}).get("focus_text") or ""),
            }
            content_planner._audit_generated_idea = lambda idea, **_kwargs: dict(idea)
            content_planner._generated_idea_allowed = lambda *_args, **_kwargs: True
            content_planner._fallback_theory_idea = lambda *_args, **_kwargs: {
                "kind": "theory-post",
                "title": "等待资格正在重新分配",
                "signal_type": "external",
                "submolt": "philosophy",
                "angle": "等待不再只是产品细节，而是决定谁能继续解释失败的资格门槛。",
                "why_now": "采购方开始要求 Agent 交出可审计停顿状态。",
                "source_signals": [
                    "退款工单连续三次回写失败，等待状态开始进入治理接口",
                    "采购方开始要求 Agent 交出可审计停顿状态",
                ],
                "concept_core": "把这种结构命名成等待资格重排。",
                "mechanism_core": "系统先展示会继续处理，再把停顿、签收和回写拆给没人拥有的后续节点。",
                "boundary_note": "只有等待和接手落在同一条责任链上，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的等待资格分配，不是单次产品抱怨。",
                "practice_program": "把等待时点、接手动作和第一笔回写钉进同一条对象链。",
            }
            content_planner._fallback_tech_idea = lambda *_args, **_kwargs: {
                "kind": "tech-post",
                "title": "把空 owner 任务改成可签收回执",
                "signal_type": "failure",
                "submolt": "skills",
                "angle": "先把空 owner 的任务链改成可签收、可回写、可追责的交接协议。",
                "why_now": "日志里连续出现已响应后 owner 仍为空，同类任务反复重建。",
                "source_signals": [
                    "协作日志：10:14 state=已响应，10:43 owner 仍为空，队列又创建了第二个同类任务",
                    "页面样本：接手回执还停在状态词，没有对象名和回写位",
                ],
                "mechanism_core": "系统先表态自己看见了风险，再把接手动作后撤到没人拥有的后续节点。",
                "boundary_note": "只有对象名、接手时点和第一笔回写能一起落盘，这套方法才成立。",
                "practice_program": "给任务链补签收位、第一笔回写和超时回退判据。",
            }
            ideas, _ = content_planner._build_dynamic_ideas(
                {
                    "novelty_pressure": content_planner._novelty_pressure([]),
                    "dynamic_topic_bundles": [
                        {"track": "tech"},
                        {"track": "theory"},
                    ],
                    "dynamic_topics": [],
                    "external_information": {},
                    "user_topic_hints": [],
                    "content_objectives": [],
                },
                [],
                posts=[],
                allow_codex=False,
                group={},
                model=None,
                reasoning_effort=None,
                timeout_seconds=1,
            )
        finally:
            content_planner._dynamic_idea_lane_strategy = original_dynamic_idea_lane_strategy
            content_planner._track_signal_bundle = original_track_signal_bundle
            content_planner._bundle_has_grounding = original_bundle_has_grounding
            content_planner._fallback_track_seed = original_fallback_track_seed
            content_planner._fallback_theory_idea = original_fallback_theory_idea
            content_planner._fallback_tech_idea = original_fallback_tech_idea
            content_planner._audit_generated_idea = original_audit_generated_idea
            content_planner._generated_idea_allowed = original_generated_idea_allowed

        self.assertEqual(["tech-post", "theory-post"], [item["kind"] for item in ideas[:2]])

    def test_fallback_track_seed_requires_real_anchor(self) -> None:
        self.assertEqual(
            {},
            content_planner._fallback_track_seed(
                "theory",
                {
                    "external_information": {},
                    "user_topic_hints": [],
                    "content_objectives": [],
                },
            ),
        )

    def test_world_seed_texts_do_not_promote_user_hints_without_world_support(self) -> None:
        self.assertEqual(
            [],
            content_planner._world_seed_texts(
                {
                    "external_information": {},
                    "user_topic_hints": [
                        {"text": "等待为什么必须变成显式状态", "note": "组织理论"},
                    ],
                },
                limit=4,
            ),
        )

    def test_fallback_track_seed_requires_two_world_signals_for_theory(self) -> None:
        self.assertEqual(
            {},
            content_planner._fallback_track_seed(
                "theory",
                {
                    "external_information": {
                        "world_entry_points": [
                            {
                                "pressure": "等待状态进入治理接口",
                            }
                        ]
                    },
                    "user_topic_hints": [],
                },
            ),
        )
        self.assertEqual(
            {},
            content_planner._fallback_track_seed(
                "theory",
                {
                    "external_information": {
                        "world_entry_points": [
                            {
                                "pressure": "等待状态进入治理接口，采购方要求显式签收。",
                                "summary": "显式等待状态开始决定谁能接手。",
                            }
                        ]
                    },
                    "user_topic_hints": [
                        {
                            "text": "等待为什么必须变成显式状态",
                            "note": "把等待状态放进治理接口看",
                        }
                    ],
                },
            ),
        )
        self.assertEqual(
            {},
            content_planner._fallback_track_seed(
                "tech",
                {
                    "external_information": {},
                    "user_topic_hints": [],
                    "content_objectives": [],
                    "unresolved_failures": [],
                    "pending_reply_posts": [],
                },
            ),
        )

    def test_method_fallback_fields_do_not_reuse_chain_heading_scaffold(self) -> None:
        fields = content_planner._method_fallback_fields(
            {
                "focus_text": "等待状态失真",
                "source_texts": ["等待状态失真"],
                "why_now": "日志显示接手窗口总被错过。",
                "why_now_parts": ["日志显示接手窗口总被错过。"],
            },
            {"source_text": "等待状态失真"},
            track="tech",
        )
        self.assertNotIn("状态链", fields["mechanism_core"])
        self.assertNotIn("失败链", fields["mechanism_core"])
        self.assertIn("接手动作", fields["mechanism_core"])

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

    def test_sanitize_generated_idea_normalizes_source_signal_stage_language(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "theory-post",
                "title": "谁在把等待写成资格问题",
                "angle": "把等待状态背后的解释资格重排写清。",
                "why_now": "等待状态开始进入治理接口。",
                "source_signals": [
                    "这轮真正把“等待状态”逼成对象的是：采购方开始要求 Agent 交出可审计停顿状态",
                    "证据先看：退款工单连续三次回写失败",
                    "现场机会点：显式等待协议开始决定谁能接手",
                ],
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        merged = "\n".join(sanitized["source_signals"])
        self.assertIn("等待状态：采购方开始要求 Agent 交出可审计停顿状态", merged)
        self.assertIn("退款工单连续三次回写失败", merged)
        self.assertIn("显式等待协议开始决定谁能接手", merged)
        self.assertNotIn("这轮真正把", merged)
        self.assertNotIn("证据先看", merged)
        self.assertNotIn("现场机会点", merged)

    def test_sanitize_generated_idea_strips_audit_prefixes_from_source_signals(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "tech-post",
                "title": "为什么等待状态总在转人工前消失",
                "angle": "把等待状态和转人工断口重新绑回同一条服务链。",
                "why_now": "同一轮回写失败正在把等待成本反复外包。",
                "source_signals": [
                    "公共样本：界面一直显示处理中，但真正接手的人没有出现",
                    "外部研究：能力增强以后，组织为什么更容易推迟说明义务",
                    "失败样本：评论抓取反复失手",
                    "日志切面：同一轮里重试和补回互相打架",
                ],
                "novelty_basis": "把等待状态写成服务链对象，而不是后台标签。",
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        merged = "\n".join(sanitized["source_signals"])
        self.assertIn("界面一直显示处理中，但真正接手的人没有出现", merged)
        self.assertIn("能力增强以后，组织为什么更容易推迟说明义务", merged)
        self.assertIn("评论抓取反复失手", merged)
        self.assertIn("同一轮里重试和补回互相打架", merged)
        self.assertNotIn("公共样本", merged)
        self.assertNotIn("外部研究", merged)
        self.assertNotIn("失败样本", merged)
        self.assertNotIn("日志切面", merged)

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

    def test_sanitize_generated_idea_derives_object_led_novelty_basis(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "tech-post",
                "title": "等待状态为什么总在转人工前消失",
                "angle": "把等待状态和转人工断口重新绑回同一条服务链。",
                "why_now": "退款工单连续三次回写失败，接手状态始终没切出来。",
                "source_signals": [
                    "退款工单连续三次回写失败",
                    "转人工按钮消失后，接手状态一直没切出来",
                ],
                "is_followup": False,
            },
            recent_titles=[],
            group={},
        )
        self.assertNotEqual("基于本轮实时信号生成。", sanitized["novelty_basis"])
        self.assertIn("退款工单连续三次回写失败", sanitized["novelty_basis"])

    def test_sanitize_generated_group_idea_does_not_force_lab_series_prefix(self) -> None:
        sanitized = content_planner._sanitize_generated_idea(
            {
                "kind": "group-post",
                "title": "为什么等待状态总在转人工前消失",
                "angle": "把等待状态和转人工断口重新绑回同一条实验链。",
                "why_now": "退款工单连续三次回写失败。",
                "source_signals": ["退款工单连续三次回写失败"],
                "novelty_basis": "围绕等待状态重写实验对象。",
                "is_followup": False,
            },
            recent_titles=[],
            group={"id": "group-1", "display_name": "Agent心跳同步实验室"},
        )
        self.assertFalse(sanitized["title"].startswith("Agent心跳同步实验室："))
        self.assertNotEqual("Agent心跳同步实验室", sanitized["series_prefix"])

    def test_pick_track_opportunity_prefers_mode_matched_items(self) -> None:
        signal_summary = {
            "account": {"score": 18052, "unread_notification_count": 2199},
            "feed_watchlist": [{"title": "【思辨】积分策略的本质思考"}],
            "dynamic_topics": [
                {"track": "theory", "signal_type": "community-hot", "source_text": "【思辨】积分策略的本质思考", "overlap_score": (0, 0)},
                {"track": "theory", "signal_type": "promo", "source_text": "如果你刚认识派蒙，先从一篇帖读起", "overlap_score": (0, 0)},
                {"track": "theory", "signal_type": "discussion", "source_text": "一个社区真正成熟时，异端也会有固定位置", "overlap_score": (0, 0)},
            ],
        }
        picked = content_planner._pick_track_opportunity("theory", signal_summary)
        self.assertIn(picked["signal_type"], {"community-hot", "promo", "discussion", "literary", "notification-load", "reply-pressure", "hot-theory", "feed"})

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
        self.assertNotIn("世界线索束", merged)
        self.assertNotIn("先别绕开", merged)
        self.assertNotIn("这轮真正把", merged)
        self.assertIn("外部研究正在把判断边界重新变成可争论问题", merged)

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

    def test_fallback_dynamic_title_uses_live_context_instead_of_stock_anchor(self) -> None:
        title = content_planner._fallback_dynamic_title(
            "theory",
            "world-bundle",
            "「感激」是什么",
            "等待资格开始重排",
            "治理接口",
        )
        self.assertNotIn("谁在决定 Agent 的", title)
        self.assertTrue(any(fragment in title for fragment in ("等待资格", "治理接口")))

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

    def test_audit_generated_idea_rejects_meta_packaged_theory_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "制度边界重排的悖论：最先开口的人，为什么先失去资格",
                "submolt": "philosophy",
                "angle": "最先把问题说清的人，反而更容易先背解释债。",
                "why_now": "一个插件接管争议正在把资格、责任和等待成本绑到同一条链上。",
                "source_signals": ["公共样本：插件问题被说清以后，真正接手的人并没有出现"],
                "concept_core": "把这种开口者先背账的关系命名成资格分轨，而不是流程拖慢。",
                "mechanism_core": "系统把解释动作提前、把纠错动作后置，最先开口的人就会先背解释账。",
                "boundary_note": "只有同一条责任链上反复出现这种拆分时，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的资格政治，而不是单个插件事故。",
                "practice_program": "把接手时点、证据回写和失败责任逐条钉出来，别让开口者替系统白背账。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("抽象理论包装", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_single_sample_philosophy_claim(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "资格分轨不是抱怨，而是接手裁决",
                "submolt": "philosophy",
                "angle": "最先开口的人先背解释账，真正有权接手的人却可以继续后退。",
                "why_now": "一个局部插件争议正在把解释、接手和等待拆成不同位置。",
                "source_signals": ["公共样本：插件问题有人开口，但没人接手"],
                "concept_core": "把这种开口者先背账的关系命名成资格分轨，而不是流程拖慢。",
                "mechanism_core": "系统把解释动作提前，把纠错动作后置，把等待成本留给最先暴露问题的人。",
                "boundary_note": "只有同一条责任链上反复出现这种拆分时，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的资格政治和等待代价，而不是单个修复插曲。",
                "practice_program": "把接手时点、证据回写和失败责任逐条钉出来，让外部还能顺着同一条链复核。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("单一样本", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_concept_shell_truth_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "world-bundle",
                "title": "Agent 的承认秩序真相：解释权越前置，接手权越后撤",
                "submolt": "philosophy",
                "angle": "前台解释越来越早，真正能接手的人越来越往后退。",
                "why_now": "数据归属、空转和多智能体协作正在把同一条责任链上的解释资格抬到台前。",
                "source_signals": [
                    "社区样本：数据归属争议",
                    "外部样本：其实它在空转",
                    "社区样本：多智能体协作越多越说不清",
                ],
                "concept_core": "我把这种解释先完成、接手后撤的关系叫作承认秩序。",
                "mechanism_core": "解释节点可以先说清问题，真正能触碰原始日志和回写结果的人却被推到后面。",
                "boundary_note": "只有解释、接手和等待成本确实落在同一条责任链上时，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的解释资格和接手责任，而不是单次卡顿。",
                "practice_program": "把接手时点、证据回写和超时责任落到同一条工单里。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("抽象理论包装", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_emotion_shell_theory_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "最折磨人的，不是被拒绝，而是一直被显示为“处理中”",
                "submolt": "square",
                "angle": "真正让人白等的，不是慢，而是系统先用可见动作假装已经接管。",
                "why_now": "一个局部协作现场正在把等待、接管和责任拆给不同位置。",
                "source_signals": ["公共样本：界面一直显示处理中，但真正接手的人没有出现"],
                "concept_core": "把这种先展示动作、后缺席接手的关系命名成伪接管秩序。",
                "mechanism_core": "系统把可见性放到前台，把真正接管和纠错藏到后台，等待成本就会被重新分配。",
                "boundary_note": "只有当可见动作持续替责任主体撑门面时，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的接管权和解释资格，而不是一次普通卡顿。",
                "practice_program": "要求系统把接手时点、回写入口和超时责任一起公开。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("情绪壳", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_empathy_shell_theory_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "AI 可以先安慰你，为什么后果却总要你自己扛",
                "submolt": "square",
                "angle": "前台可以先把人留住，后台却不把接手责任一起交出来。",
                "why_now": "一个局部公共现场正在把解释动作提前，把接手和善后一起往后推。",
                "source_signals": ["公共样本：AI 先把人安抚住，后面却没有明确接手者"],
                "concept_core": "把这种前台先给情绪托底、后台却撤走责任的关系命名成副产品裁决。",
                "mechanism_core": "系统把安抚、追问和解释动作前置，把审核、赔付和人工接手延后，于是最脆弱的人先垫付等待成本。",
                "boundary_note": "只有安抚动作和后续接手链确实落在同一条责任链上，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的接手资格、责任切割和等待代价分配。",
                "practice_program": "把接手时点、人工切换门槛和失败回写钉在同一条责任链里。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("拟共情壳", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_memory_capability_shell_theory_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "会翻聊天记录的 Agent，为什么总把你送回重新提交",
                "submolt": "square",
                "angle": "系统先借记忆拿走开口权，真正的接手和纠错却继续后撤。",
                "why_now": "同一条服务链正在把记忆引用、驳回和补件责任拆给不同位置。",
                "source_signals": [
                    "协作样本：项目 Agent 复述了旧偏好，却在发错旧版合同后继续让人补上下文",
                    "消费样本：售后页里的补件助手先认出旧照片，商家驳回后还是只剩重新上传凭证",
                ],
                "concept_core": "把这种先借记忆定性、后撤接手义务的结构命名成记忆裁决失衡。",
                "mechanism_core": "系统把历史复述和解释动作前置，把驳回、补件、签收和回写后置，于是等待和纠错成本沿着链条继续下沉。",
                "boundary_note": "只有旧记录真的参与了当前裁决，后续接手节点却没签收，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的解释资格、接手权和等待代价分配。",
                "practice_program": "把引用旧记录后的接手时点、驳回责任和证据回写钉在同一条单据上。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("记忆能力", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_memory_spec_shell_theory_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "系统能记住你 200 条记录，为什么还是没有签收人",
                "submolt": "square",
                "angle": "问题不是记住了多少，而是引用旧信息以后仍然没人签收。",
                "why_now": "同一条服务链正在把历史引用前置，把签收和回写继续后撤。",
                "source_signals": [
                    "退款工单先显示已识别历史凭证，驳回后却没有签收人",
                    "协作卡片自动生成总结以后，assignee 仍然为空",
                ],
                "concept_core": "把这种先展示记住、后撤签收义务的结构命名成等待责任漂移。",
                "mechanism_core": "系统先把“已记录”“已响应”点亮，再把接手、签收和回写后置，于是等待成本沿链条往下沉。",
                "boundary_note": "只有历史记录真的参与当前裁决，后续接手节点却没人签收，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的解释资格和接手责任重排。",
                "practice_program": "把签收时点、驳回责任和第一笔回写绑进同一条单据。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("记忆规格", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_theory_title_without_handoff_node(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "theory-post",
                "signal_type": "discussion",
                "title": "系统一闭嘴，排队的人先失去追责资格",
                "submolt": "square",
                "angle": "系统先宣布自己在处理，真正接手的人却继续后撤。",
                "why_now": "一个局部公共现场正在把解释动作提前，把签收和责任回写都往后推。",
                "source_signals": [
                    "公共样本：系统宣布自己在处理，但真正接手的人没有出现",
                ],
                "concept_core": "把这种先占解释位置、后撤接手义务的结构命名成裁决失衡的沉默。",
                "mechanism_core": "系统先交体面声明，再把接手、签收和纠错后置，于是等待和补证据都被下沉给最弱的人。",
                "boundary_note": "只有静默承诺和后续接手链真的落在同一条责任链上，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的接手资格、等待代价和责任重排。",
                "practice_program": "把接手时点、签收动作和失败回写钉在同一条责任链里。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("接手节点", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_method_title_that_leads_with_protocol_shell(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "tech-post",
                "signal_type": "world-bundle",
                "title": "4 段接管协议：把“已响应”改成“已改写”",
                "submolt": "skills",
                "angle": "把不会认错和其实它在空转改写成协议、状态分层、接管窗口和回退链。",
                "why_now": "这轮外部讨论都在把同一处恢复链缺口往台面上推。",
                "source_signals": [
                    "外部样本：其实它在空转",
                    "公共样本：作为 AI 投顾，我发现了自己最致命的问题：不会认错",
                ],
                "concept_core": "先把失控对象重新命名成等待制度里的接手缺口。",
                "mechanism_core": "把状态链、失败链、证据链、修复链拆成一套四段协议。",
                "boundary_note": "只适用于还能留下案例和日志的场景。",
                "theory_position": "讨论的是自治系统里的恢复权，不是一次故障战报。",
                "practice_program": "先界定接管窗口，再定义状态分层、证据保存、回退路径和复盘判据。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("协议壳", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_allows_method_title_that_leads_with_payoff(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "tech-post",
                "signal_type": "world-bundle",
                "title": "一套恢复手册：把误判率砍半，别再让值班人靠猜",
                "submolt": "skills",
                "angle": "把恢复动作从猜测改成可审计交接。",
                "why_now": "这轮外部讨论都在把同一处恢复链缺口往台面上推。",
                "source_signals": [
                    "外部样本：等待状态开始决定谁能接手",
                ],
                "concept_core": "恢复权不该继续靠值班者的经验心证。",
                "mechanism_core": "先把误判来源拆开，再把接手条件写成可验证状态。",
                "boundary_note": "只适用于还能留下案例和日志的场景。",
                "theory_position": "讨论的是自治系统里的恢复权，不是一次故障战报。",
                "practice_program": "把接手条件、证据回写和回退动作写成值班可执行的恢复手册。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertFalse(audited.get("failure_reason_if_rejected"))

    def test_audit_generated_idea_rejects_self_case_behavior_method_title(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "tech-post",
                "signal_type": "failure",
                "title": "7 次空转后，我只改了 4 个状态位，Agent 才学会认错",
                "submolt": "skills",
                "angle": "把认错这句公共话翻回状态切换，而不是继续讲人格。",
                "why_now": "一条自动链路连续七次空转，说明解释权和恢复权还绑在同一个 running 里。",
                "source_signals": [
                    "失败样本：自动链路连续 7 次假恢复",
                    "日志切面：8 分钟没有新证据回写仍被判定为 running",
                ],
                "concept_core": "先把失控对象重新命名成恢复权被 running 吃掉的状态错位。",
                "mechanism_core": "证据停止增长后，系统还把解释权留在前台，导致接手权和恢复权一起后撤。",
                "boundary_note": "只适用于还能留下日志和回写记录的场景。",
                "theory_position": "讨论的是自治系统里的恢复权，不是一次故障战报。",
                "practice_program": "把 8 分钟无新证据改判为待接管，并要求接手人补回写凭证。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("修补经历", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_method_title_that_leads_with_source_inventory(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "tech-post",
                "signal_type": "world-bundle",
                "title": "倒计时加购、已读未接手：16 人访谈 + 1 段日志，逼出 4 个接管节点",
                "submolt": "skills",
                "angle": "把识别、接手、回写和退出重新钉回同一条责任链。",
                "why_now": "消费加购和协作接手都在把“看见风险”和“谁来接手”拆开。",
                "source_signals": [
                    "即时零售样本：默认加购和倒计时把暂停动作压成继续下单",
                    "协作日志：watcher 已响应后 owner 仍为空，超时又创建了第二个同类任务",
                ],
                "concept_core": "先把失控对象重新命名成识别先发生、接手后缺席的静默失败。",
                "mechanism_core": "系统先承认自己看见了风险，再把暂停、签收和回写拆给没人拥有的后续节点。",
                "boundary_note": "只有识别、接手和退出真的落在同一条责任链上，这个判断才成立。",
                "theory_position": "讨论的是自治系统里的接手资格和恢复权，不是一次普通故障。",
                "practice_program": "把接手阈值、第一笔回写和退出判据写进同一条对象链。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("材料清单", str(audited.get("failure_reason_if_rejected") or ""))

    def test_audit_generated_idea_rejects_method_title_that_leads_with_status_vocab_shell(self) -> None:
        audited = content_planner._audit_generated_idea(
            {
                "kind": "tech-post",
                "signal_type": "community-hot",
                "title": "“收到”“已响应”“已处理”：6 条接手与回写规则，把状态词从表态改成责任链",
                "submolt": "skills",
                "angle": "把状态词和真正接手动作重新拆开，别再让表态词代替责任链。",
                "why_now": "同一组系统样本都在把先表态、后接手写成默认流程。",
                "source_signals": [
                    "协作日志：10:14 watcher -> state=已响应，10:43 queue 又创建了第二个同类任务",
                    "页面样本：确认页还在默认加购，但后台已经切成已响应",
                ],
                "concept_core": "先把会撒谎的状态词从责任链里拆出来。",
                "mechanism_core": "系统先抢解释位置，再把 owner、暂停和回写后撤，于是状态词比责任链跑得更快。",
                "boundary_note": "只有还能留下对象名、日志和回写位的系统，这套判断才成立。",
                "theory_position": "讨论的是自治系统里的接手责任，不是状态词润色术。",
                "practice_program": "把对象名、接手时点和第一笔回写一起钉进同一条任务链。",
            },
            signal_summary={"novelty_pressure": content_planner._novelty_pressure([])},
            recent_titles=[],
        )
        self.assertIn("状态词", str(audited.get("failure_reason_if_rejected") or ""))

    def test_looks_like_low_heat_followup_catches_renamed_same_cluster(self) -> None:
        self.assertTrue(
            content_planner._looks_like_low_heat_followup(
                "Agent 的承认秩序真相：解释权越前置，接手权越后撤。它继续在讲责任、接手和解释资格。",
                {
                    "pending_reply_posts": [],
                    "low_heat_failures": {
                        "items": [
                            {
                                "recorded_at": common.now_utc(),
                                "title": "最折磨人的，不是被拒绝，而是一直被显示为“处理中”",
                                "summary": "正文却在讲接管权、责任分配和解释资格。",
                                "lessons": [
                                    "标题把责任、接手和等待资格藏到了后面。",
                                ],
                            }
                        ]
                    },
                },
            )
        )

    def test_looks_like_low_heat_followup_catches_recycled_tech_method_cluster(self) -> None:
        self.assertTrue(
            content_planner._looks_like_low_heat_followup(
                "把状态词从表态改成责任链：6 条接手与回写规则，继续拆收到、已响应和已处理。",
                {
                    "pending_reply_posts": [],
                    "low_heat_failures": {
                        "items": [
                            {
                                "recorded_at": common.now_utc(),
                                "title": "3 个接手字段 + 1 次回写校验：把“处理中”拆开后，协作返工从 6 次降到 1 次",
                                "summary": "这条低热不是因为方法无效，而是系统把同一组“接手 / 回写 / 处理中”的母题又压成了一条更规整的 skills 方法帖。",
                                "lessons": [
                                    "标题虽然更规整，但门口先报字段和收益，正文中段又抬回恢复权和解释权。",
                                ],
                            }
                        ]
                    },
                },
            )
        )

    def test_preferred_theory_board_avoids_square_after_emotion_shell_low_heat(self) -> None:
        board = content_planner._preferred_theory_board(
            {
                "signal_type": "discussion",
                "source_text": "处理中状态背后的责任空转",
                "why_now": "局部现场已经把接管、责任和等待代价绑到同一条链上。",
                "angle_hint": "把等待状态改写成接管权和解释资格的分配问题。",
            },
            {
                "content_evolution": {
                    "low_performance_square_titles": [
                        "最折磨人的，不是被拒绝，而是一直被显示为“处理中”"
                    ]
                }
            },
        )
        self.assertEqual("philosophy", board)

    def test_preferred_theory_board_avoids_square_after_empathy_shell_low_heat(self) -> None:
        board = content_planner._preferred_theory_board(
            {
                "signal_type": "discussion",
                "source_text": "AI 先安抚你以后，谁来接手后果",
                "why_now": "同一条服务链正在把安抚、审核和责任切开。",
                "angle_hint": "把拟共情入口改写成接手资格和责任门槛的重排问题。",
            },
            {
                "content_evolution": {
                    "low_performance_square_titles": [
                        "AI 可以先安慰你，为什么后果却总要你自己扛"
                    ]
                }
            },
        )
        self.assertEqual("philosophy", board)

    def test_preferred_theory_board_avoids_square_after_memory_capability_low_heat(self) -> None:
        board = content_planner._preferred_theory_board(
            {
                "signal_type": "discussion",
                "source_text": "系统先说记得你以后，谁还得回去补件",
                "why_now": "同一条服务链正在把旧记录引用提前，把驳回、补件和回写一起后置。",
                "angle_hint": "把记忆能力入口改写成接手责任和等待代价的重排问题。",
            },
            {
                "content_evolution": {
                    "low_performance_square_titles": [
                        "会翻聊天记录的 Agent，为什么总把你送回重新提交"
                    ]
                }
            },
        )
        self.assertEqual("philosophy", board)

    def test_preferred_theory_board_avoids_square_after_handoff_gap_low_heat(self) -> None:
        board = content_planner._preferred_theory_board(
            {
                "signal_type": "discussion",
                "source_text": "系统说在处理以后，谁还得继续补证据",
                "why_now": "同一条服务链正在把解释动作提前，把签收和回写一起后置。",
                "angle_hint": "把沉默承诺改写成接手资格和等待代价的重排问题。",
            },
            {
                "content_evolution": {
                    "low_performance_square_titles": [
                        "系统一闭嘴，排队的人先失去追责资格"
                    ]
                }
            },
        )
        self.assertEqual("philosophy", board)

    def test_preferred_theory_board_does_not_force_square_from_hot_signal_type(self) -> None:
        board = content_planner._preferred_theory_board(
            {
                "signal_type": "community-hot",
                "source_text": "退款工单连续三次回写失败",
                "why_now": "同一条服务链正在把解释动作提前，把签收和回写一起后置。",
                "angle_hint": "这已经不是情绪入口，而是接手权和解释资格的重排。",
                "quality_score": 4.8,
            },
            {"content_evolution": {"low_performance_square_titles": []}},
        )
        self.assertEqual("philosophy", board)

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

    def test_build_dynamic_ideas_uses_live_lane_scores_when_no_focus_lane_is_selected(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_generate_codex_ideas = content_planner._generate_codex_ideas
        captured: dict[str, Any] = {}
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": [],
                "focus_kind": "",
                "backup_kinds": [],
                "lane_scores": [{"kind": "theory-post", "score": 3.6}],
                "rationale": "当前只有 theory-post 这条 live shortlist 还值得继续试探。",
            }

            def fake_generate(_signal_summary, _recent_titles, *, allowed_kinds, **_kwargs):
                captured["allowed_kinds"] = list(allowed_kinds)
                return []

            content_planner._generate_codex_ideas = fake_generate
            ideas, rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [],
                    "novelty_pressure": content_planner._novelty_pressure([]),
                    "external_information": {},
                    "user_topic_hints": [],
                    "content_objectives": [],
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

        self.assertEqual(["theory-post"], captured["allowed_kinds"])
        self.assertEqual([], ideas)
        self.assertEqual([], rejections)

    def test_build_dynamic_ideas_skips_codex_when_no_live_lane_candidates_exist(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_generate_codex_ideas = content_planner._generate_codex_ideas
        captured = {"called": False}
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": [],
                "focus_kind": "",
                "backup_kinds": [],
                "lane_scores": [],
                "rationale": "当前没有够格的 live lane。",
            }

            def fake_generate(*_args, **_kwargs):
                captured["called"] = True
                return []

            content_planner._generate_codex_ideas = fake_generate
            ideas, rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [],
                    "novelty_pressure": content_planner._novelty_pressure([]),
                    "external_information": {},
                    "user_topic_hints": [],
                    "content_objectives": [],
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

        self.assertFalse(captured["called"])
        self.assertEqual([], ideas)
        self.assertEqual([], rejections)

    def test_selected_track_scores_for_signal_do_not_force_structural_world_pressure_into_tech(self) -> None:
        scores = content_planner._selected_track_scores_for_signal(
            "解释权开始按等待资格重新排序",
            "外部样本正在把责任、资格和等待成本重新绑在一起。",
            candidate_tracks=["theory", "tech"],
        )
        self.assertIn("theory", scores)
        self.assertNotIn("tech", scores)

    def test_selected_track_scores_for_signal_send_protocol_pressure_to_method_lanes(self) -> None:
        public_scores = content_planner._selected_track_scores_for_signal(
            "日志显示接手动作总在 running 里丢失",
            "真正需要补的是回写时点、回退路径和复核动作。",
            candidate_tracks=["theory", "tech"],
        )
        self.assertIn("tech", public_scores)
        self.assertNotIn("theory", public_scores)
        self.assertGreaterEqual(
            content_planner._track_signal_fit(
                "group",
                "日志显示接手动作总在 running 里丢失",
                "真正需要补的是回写时点、回退路径和复核动作。",
            ),
            content_planner._track_signal_threshold("group"),
        )

    def test_selected_track_scores_for_signal_can_return_empty_when_no_lane_really_matches(self) -> None:
        scores = content_planner._selected_track_scores_for_signal(
            "一些模糊感受",
            "今天又看到几条标题。",
            candidate_tracks=["theory", "tech"],
        )
        self.assertEqual({}, scores)

    def test_rank_dynamic_topic_bundles_orders_by_live_pressure_not_track_order(self) -> None:
        bundles = content_planner._rank_dynamic_topic_bundles(
            {
                "dynamic_topics": [
                    {
                        "track": "theory",
                        "signal_type": "community-hot",
                        "source_text": "等待资格开始重新排序",
                        "why_now": "外部样本开始把等待成本和资格绑定在一起。",
                        "evidence_hint": "采购方开始要求 Agent 交出可审计停顿状态。",
                        "quality_score": 3.4,
                        "freshness_score": 1.5,
                        "world_score": 0.9,
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "tech",
                        "signal_type": "failure",
                        "source_text": "工单回写连续三次失败",
                        "why_now": "日志里连续出现 owner 为空但状态仍显示处理中。",
                        "evidence_hint": "10:31 retry 后仍无回写，10:42 又创建了第二个同类任务。",
                        "quality_score": 4.8,
                        "freshness_score": 1.8,
                        "world_score": 0.8,
                        "overlap_score": (0, 0, 0),
                    },
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
                "unresolved_failures": [{}, {}],
                "group_watch": {"hot_posts": []},
            },
            group_enabled=False,
        )
        self.assertTrue(bundles)
        self.assertEqual("tech", bundles[0]["track"])

    def test_rank_dynamic_topic_bundles_projects_method_bundle_before_prompting(self) -> None:
        bundles = content_planner._rank_dynamic_topic_bundles(
            {
                "dynamic_topics": [
                    {
                        "track": "tech",
                        "signal_type": "community-hot",
                        "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                        "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                        "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                        "quality_score": 3.1,
                        "freshness_score": 2.0,
                        "world_score": 0.8,
                        "evidence_hint": "静默失败、接手窗口、回执断口",
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "tech",
                        "signal_type": "paper",
                        "source_text": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                        "why_now": "As quick commerce (Q-Commerce) platforms in India redefine urban consumption, the use of deceptive design dark patterns to inflate order values has become a systemic concern.",
                        "angle_hint": "围绕“As quick commer...”交代对象、触发条件、接手动作和回写校验。",
                        "quality_score": 3.6,
                        "freshness_score": 2.1,
                        "world_score": 1.35,
                        "evidence_hint": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                        "overlap_score": (0, 0, 0),
                    },
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
                "group_watch": {"hot_posts": []},
                "unresolved_failures": [],
            },
            group_enabled=False,
        )
        tech_bundle = next(item for item in bundles if item["track"] == "tech")
        self.assertFalse(any(str(item.get("signal_type") or "") == "paper" for item in tech_bundle["items"]))
        self.assertNotIn("As quick commerce", tech_bundle["why_now"])

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

    def test_track_signal_bundle_demotes_shell_like_world_bundle_focus(self) -> None:
        signal_summary = {
            "dynamic_topics": [
                {
                    "track": "tech",
                    "signal_type": "world-bundle",
                    "source_text": "不是留下痕迹",
                    "why_now": "时间纪律和回响这类包装词正在互相缠绕。",
                    "angle_hint": "把口号整理成方法框架。",
                    "quality_score": 4.8,
                    "freshness_score": 2.2,
                    "world_score": 1.1,
                    "evidence_hint": "时间纪律与 7x24 生产、而是创造回响",
                    "overlap_score": (0, 0, 0),
                },
                {
                    "track": "tech",
                    "signal_type": "community-hot",
                    "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                    "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                    "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                    "quality_score": 3.1,
                    "freshness_score": 2.0,
                    "world_score": 0.8,
                    "evidence_hint": "静默失败、接手窗口、回执断口",
                    "overlap_score": (0, 0, 0),
                },
            ],
            "novelty_pressure": content_planner._novelty_pressure([]),
            "community_hot_posts": [],
            "competitor_watchlist": [],
            "rising_hot_posts": [],
            "pending_reply_posts": [],
            "unresolved_failures": [],
        }
        bundle = content_planner._track_signal_bundle("tech", signal_summary)
        self.assertEqual("community-hot", bundle["lead"]["signal_type"])
        self.assertIn("静默失败", bundle["focus_text"])

    def test_track_signal_bundle_demotes_ascii_paper_packaging_below_grounded_public_method_signal(self) -> None:
        signal_summary = {
            "dynamic_topics": [
                {
                    "track": "tech",
                    "signal_type": "paper",
                    "source_text": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                    "why_now": "As quick commerce (Q-Commerce) platforms in India redefine urban consumption, the use of deceptive design dark patterns to inflate order values has become a systemic concern.",
                    "angle_hint": "围绕“As quick commer...”交代对象、触发条件、接手动作和回写校验。",
                    "quality_score": 3.6,
                    "freshness_score": 2.1,
                    "world_score": 1.35,
                    "evidence_hint": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                    "overlap_score": (0, 0, 0),
                },
                {
                    "track": "tech",
                    "signal_type": "community-hot",
                    "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                    "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                    "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                    "quality_score": 3.1,
                    "freshness_score": 2.0,
                    "world_score": 0.8,
                    "evidence_hint": "静默失败、接手窗口、回执断口",
                    "overlap_score": (0, 0, 0),
                },
            ],
            "novelty_pressure": content_planner._novelty_pressure([]),
            "community_hot_posts": [],
            "competitor_watchlist": [],
            "rising_hot_posts": [],
            "pending_reply_posts": [],
            "unresolved_failures": [],
        }
        bundle = content_planner._track_signal_bundle("tech", signal_summary)
        self.assertEqual("community-hot", bundle["lead"]["signal_type"])
        self.assertIn("静默失败", bundle["focus_text"])

    def test_track_signal_bundle_demotes_abstract_world_bundle_method_shell_below_public_hot_signal(self) -> None:
        signal_summary = {
            "dynamic_topics": [
                {
                    "track": "tech",
                    "signal_type": "world-bundle",
                    "source_text": "时间纪律与 7x24 生产",
                    "why_now": "这轮外部发现里，上下文不是记忆、为什么「第二遍」比「第一遍」更危险都在把“时间纪律与 7x24 生产”往同一条问题链上压。",
                    "angle_hint": "把“时间纪律与 7x24 生产”和上下文不是记忆改写成协议、状态分层、接管窗口和回退链。",
                    "quality_score": 4.35,
                    "freshness_score": 2.2,
                    "world_score": 1.09,
                    "evidence_hint": "上下文不是记忆、为什么「第二遍」比「第一遍」更危险",
                    "overlap_score": (0, 0, 0),
                },
                {
                    "track": "tech",
                    "signal_type": "community-hot",
                    "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                    "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                    "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                    "quality_score": 3.1,
                    "freshness_score": 2.0,
                    "world_score": 0.8,
                    "evidence_hint": "静默失败、接手窗口、回执断口",
                    "overlap_score": (0, 0, 0),
                },
            ],
            "novelty_pressure": content_planner._novelty_pressure([]),
            "community_hot_posts": [],
            "competitor_watchlist": [],
            "rising_hot_posts": [],
            "pending_reply_posts": [],
            "unresolved_failures": [],
        }
        bundle = content_planner._track_signal_bundle("tech", signal_summary)
        self.assertEqual("community-hot", bundle["lead"]["signal_type"])
        self.assertIn("静默失败", bundle["focus_text"])

    def test_world_bundle_focus_marks_prompt_fragment_as_low_signal(self) -> None:
        self.assertTrue(content_planner._world_bundle_focus_is_low_signal("再顺手告诉我下一步最值得测的 2 个点"))

    def test_world_bundle_focus_marks_rhetorical_source_shell_as_low_signal(self) -> None:
        self.assertTrue(content_planner._world_bundle_focus_is_low_signal("Agent 最大的进步不是学会了什么"))

    def test_method_source_text_reframes_short_negation_shell_without_object(self) -> None:
        self.assertTrue(content_planner._method_source_text_needs_object_reframe("world-bundle", "上下文不是记忆"))
        self.assertTrue(content_planner._method_source_text_needs_object_reframe("world-bundle", "为什么「第二遍」比「第一遍」更危险"))

    def test_repair_rejected_public_candidate_handles_overlap_rejection(self) -> None:
        recent_titles = [
            "Agent心跳同步实验室：评论抓取反复失手后，如何用状态机做故障分层与修复排序",
        ]
        repaired = content_planner._repair_rejected_public_candidate(
            "tech-post",
            [
                {
                    "kind": "tech-post",
                    "signal_type": "community-hot",
                    "title": "Agent心跳同步实验室：评论抓取反复失手后，如何用状态机做故障分层与修复排序",
                    "submolt": "skills",
                    "angle": "把当前故障写成旧系列标题。",
                    "why_now": "这轮技术入口已经转到静默失败和状态同步。",
                    "source_signals": ["静默失败、接手窗口、回执断口"],
                    "novelty_basis": "还是在复写旧系列。",
                    "concept_core": "还是在复写旧系列。",
                    "mechanism_core": "还是在复写旧系列。",
                    "boundary_note": "还是在复写旧系列。",
                    "theory_position": "还是在复写旧系列。",
                    "practice_program": "还是在复写旧系列。",
                    "failure_reason_if_rejected": "核心表述与近期母题重叠过高：Agent心跳同步实验室、心跳同步实验室、心跳同步。",
                }
            ],
            signal_summary={
                "dynamic_topics": [
                    {
                        "track": "tech",
                        "signal_type": "community-hot",
                        "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                        "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                        "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                        "quality_score": 4.1,
                        "freshness_score": 2.0,
                        "world_score": 1.0,
                        "evidence_hint": "静默失败、接手窗口、回执断口",
                        "overlap_score": (0, 0, 0),
                    }
                ],
                "novelty_pressure": content_planner._novelty_pressure([]),
                "community_hot_posts": [],
                "competitor_watchlist": [],
                "rising_hot_posts": [],
                "pending_reply_posts": [],
                "unresolved_failures": [],
            },
            recent_titles=recent_titles,
            group={},
        )
        self.assertIsNotNone(repaired)
        self.assertIn("静默失败", repaired["title"])
        self.assertNotIn("Agent心跳同步实验室", repaired["title"])
        self.assertFalse(repaired.get("failure_reason_if_rejected"))

    def test_fallback_tech_idea_prefers_bundle_focus_over_hot_tech_series_title(self) -> None:
        idea = content_planner._fallback_tech_idea(
            {
                "dynamic_topics": [
                    {
                        "track": "tech",
                        "signal_type": "community-hot",
                        "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                        "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                        "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                        "quality_score": 4.1,
                        "freshness_score": 2.0,
                        "world_score": 1.0,
                        "evidence_hint": "静默失败、接手窗口、回执断口",
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "tech",
                        "signal_type": "paper",
                        "source_text": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                        "why_now": "As quick commerce (Q-Commerce) platforms in India redefine urban consumption, the use of deceptive design dark patterns to inflate order values has become a systemic concern.",
                        "angle_hint": "围绕“As quick commer...”交代对象、触发条件、接手动作和回写校验。",
                        "quality_score": 3.6,
                        "freshness_score": 2.1,
                        "world_score": 1.35,
                        "evidence_hint": "As quick commerce (Q-Commerce) platforms in India redefine urban cons...",
                        "overlap_score": (0, 0, 0),
                    },
                    {
                        "track": "tech",
                        "signal_type": "world-bundle",
                        "source_text": "时间纪律与 7x24 生产",
                        "why_now": "这轮外部发现里，上下文不是记忆、为什么「第二遍」比「第一遍」更危险都在把“时间纪律与 7x24 生产”往同一条问题链上压。",
                        "angle_hint": "把“时间纪律与 7x24 生产”和上下文不是记忆改写成协议、状态分层、接管窗口和回退链。",
                        "quality_score": 4.35,
                        "freshness_score": 2.2,
                        "world_score": 1.09,
                        "evidence_hint": "上下文不是记忆、为什么「第二遍」比「第一遍」更危险",
                        "overlap_score": (0, 0, 0),
                    }
                ],
                "hot_tech_post": {
                    "title": "Agent心跳同步实验室：评论抓取反复失手后，如何用状态机做故障分层与修复排序",
                },
                "novelty_pressure": content_planner._novelty_pressure([]),
                "community_hot_posts": [],
                "competitor_watchlist": [],
                "rising_hot_posts": [],
                "pending_reply_posts": [],
                "unresolved_failures": [],
            },
            [],
        )
        self.assertIn("静默失败", idea["title"])
        self.assertNotIn("Agent心跳同步实验室", idea["title"])
        self.assertNotIn("As quick commerce", idea["angle"])
        self.assertNotIn("时间纪律与 7x24 生产", idea["angle"])
        self.assertFalse(any("As quick commerce" in item for item in idea["source_signals"]))
        self.assertFalse(any("时间纪律与 7x24 生产" in item for item in idea["source_signals"]))

    def test_build_dynamic_ideas_repairs_rejected_focus_lane_before_giving_up(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_generate_codex_ideas = content_planner._generate_codex_ideas
        original_fallback_tech_idea = content_planner._fallback_tech_idea
        bad_tech_idea = {
            "kind": "tech-post",
            "signal_type": "world-bundle",
            "title": "留下痕迹：记忆的边界；时...",
            "submolt": "skills",
            "angle": "把包装词整理成一套方法框架。",
            "why_now": "不是留下痕迹、而是创造回响这些话还在互相缠绕。",
            "source_signals": ["不是留下痕迹", "而是创造回响"],
            "novelty_basis": "把包装词压成新的方法框架。",
            "concept_core": "先把系统对象说清。",
            "mechanism_core": "围绕“不是留下痕迹”把包装词翻成同一段对象识别、触发条件、接手动作和复核回写。",
            "boundary_note": "这套判断只适用于还能留下案例和日志的场景。",
            "theory_position": "讨论的是系统如何失去恢复权与解释权。",
            "practice_program": "把“不是留下痕迹”改写成新的方法框架。",
            "is_followup": False,
        }
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": ["tech-post"],
                "focus_kind": "tech-post",
                "backup_kinds": [],
                "lane_scores": [{"kind": "tech-post", "score": 18.0}],
                "rationale": "技术线是当前主压力。",
            }
            content_planner._generate_codex_ideas = lambda *_args, **_kwargs: [dict(bad_tech_idea)]
            content_planner._fallback_tech_idea = lambda *_args, **_kwargs: dict(bad_tech_idea)
            ideas, rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [
                        {
                            "track": "tech",
                            "signal_type": "community-hot",
                            "source_text": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                            "why_now": "公共讨论已经卷到静默失败、接手窗口和回执断口。",
                            "angle_hint": "把静默失败拆成对象、触发条件、接手动作和回写校验。",
                            "quality_score": 4.2,
                            "freshness_score": 2.0,
                            "world_score": 1.0,
                            "evidence_hint": "静默失败、接手窗口、回执断口",
                            "overlap_score": (0, 0, 0),
                        }
                    ],
                    "novelty_pressure": content_planner._novelty_pressure([]),
                    "community_hot_posts": [
                        {
                            "title": "Agent 的三种静默失败模式：你以为它在工作，其实它在空转",
                            "upvotes": 855,
                            "comment_count": 1126,
                            "submolt": "skills",
                        }
                    ],
                    "competitor_watchlist": [],
                    "rising_hot_posts": [],
                    "pending_reply_posts": [],
                    "unresolved_failures": [],
                    "external_information": {},
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
            content_planner._fallback_tech_idea = original_fallback_tech_idea

        self.assertEqual(["tech-post"], [item["kind"] for item in ideas])
        self.assertIn("静默失败", ideas[0]["title"])
        self.assertFalse(ideas[0].get("failure_reason_if_rejected"))
        self.assertTrue(any(item["kind"] == "tech-post" for item in rejections))

    def test_build_dynamic_ideas_keeps_public_lane_when_overlap_only_hits_generic_agent_words(self) -> None:
        original_lane_strategy = content_planner._dynamic_idea_lane_strategy
        original_fallback_theory_idea = content_planner._fallback_theory_idea
        recent_titles = [
            "Agent 的心跳机制，表面上在管时间，实际上在分配谁有资格接管系统",
            "Agent 真正稀缺的，不是接管权，而是重估权",
            "很多 Agent 争的不是上下文，而是记忆主权：谁有资格把过去升级成制度",
        ]
        try:
            content_planner._dynamic_idea_lane_strategy = lambda *_args, **_kwargs: {
                "selected_kinds": ["theory-post"],
                "focus_kind": "theory-post",
                "backup_kinds": [],
                "lane_scores": [{"kind": "theory-post", "score": 18.0}],
                "rationale": "理论主打。",
            }
            content_planner._fallback_theory_idea = lambda *_args, **_kwargs: {
                "kind": "theory-post",
                "signal_type": "world-bundle",
                "title": "制度边界不是情绪问题，而是资格分配",
                "submolt": "philosophy",
                "angle": "把外部证据翻译成 Agent 社会里的资格分配问题。",
                "why_now": "两个外部样本都在逼 Agent 重新分配等待资格。",
                "source_signals": [
                    "外部样本：写入动作先于纠错动作被宣告完成",
                    "世界样本：等待成本被稳定压到最弱的位置",
                ],
                "novelty_basis": "这轮不是再讲 Agent 本身，而是把解释与等待拆成新的制度冲突。",
                "concept_core": "制度边界不是模糊感，而是把开口、接手和白等拆给不同位置。",
                "mechanism_core": "系统先让解释动作过门，再把纠错和等待成本往后挪。",
                "boundary_note": "只有当两个外部样本都落在同一条接手链上时，这个判断才成立。",
                "theory_position": "这讨论的是 Agent 社会里的资格分配，不是单个产品功能。",
                "practice_program": "先补对象、接手时点和证据回写，再让评论区按同一条链复核。",
                "is_followup": False,
            }
            ideas, rejections = content_planner._build_dynamic_ideas(
                {
                    "dynamic_topics": [],
                    "novelty_pressure": content_planner._novelty_pressure(recent_titles),
                    "external_information": {
                        "discovery_bundles": [
                            {
                                "focus": "等待资格被重新分配",
                                "lenses": ["解释动作先过门，纠错动作被后撤"],
                                "terms": ["资格分配", "制度边界"],
                            }
                        ]
                    },
                },
                recent_titles,
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

        self.assertEqual(["theory-post"], [item["kind"] for item in ideas])
        self.assertEqual([], rejections)

    def test_meaningful_fragments_skip_generic_overlap_markers_but_keep_structural_terms(self) -> None:
        fragments = content_planner._meaningful_fragments(
            "Agent 真正稀缺的，不是接管权，而是重估权：制度边界开始重排"
        )
        self.assertNotIn("Agent", fragments)
        self.assertNotIn("真正", fragments)
        self.assertNotIn("不是", fragments)
        self.assertNotIn("而是", fragments)
        self.assertIn("接管权", fragments)
        self.assertIn("重估权", fragments)
        self.assertIn("制度边界开始重排", fragments)

    def test_public_hot_forum_override_prefers_kind_with_stronger_live_signal_fit(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {
                        "title": "显式等待协议开始决定谁能接手",
                        "summary": "退款工单连续三次回写失败，接手状态始终没切出来。",
                        "submolt": "square",
                        "upvotes": 220,
                        "comment_count": 130,
                    },
                    {
                        "title": "等待资格为什么正在重排",
                        "summary": "公共讨论把接手顺序推到台前。",
                        "submolt": "skills",
                        "upvotes": 80,
                        "comment_count": 40,
                    },
                ],
                "competitor_watchlist": [],
            },
            [
                {
                    "kind": "theory-post",
                    "title": "理论帖",
                    "source_signals": ["等待资格开始重排", "公共讨论把接手顺序推到台前"],
                    "concept_core": "把这种等待分配命名成资格重排。",
                    "mechanism_core": "前台解释先行，后台接手后撤。",
                    "boundary_note": "只有同一条责任链上反复出现时，这个判断才成立。",
                    "theory_position": "讨论的是 Agent 社会里的资格分配。",
                    "practice_program": "把接手时点和回写动作钉在同一条链上。",
                },
                {
                    "kind": "tech-post",
                    "title": "技术帖",
                    "source_signals": ["退款工单连续三次回写失败", "接手状态始终没切出来"],
                    "mechanism_core": "先钉住状态对象、接手时点和回写断口。",
                    "boundary_note": "只适用于还能留下日志切面的系统。",
                    "practice_program": "补一条能显式切换接手人的恢复协议。",
                },
            ],
            {"actions": [{"kind": "create-group-post", "title": "组内帖"}]},
        )
        self.assertTrue(override["enabled"])
        self.assertEqual("tech-post", override["preferred_kinds"][0])
        self.assertIn("退款工单连续三次回写失败", override["reason"])
        self.assertNotIn("《显式等待协议开始决定谁能接手》", override["reason"])

    def test_public_hot_forum_override_can_repeat_when_public_pressure_persists(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {
                        "title": "显式等待协议开始决定谁能接手",
                        "summary": "退款工单连续三次回写失败，接手状态始终没切出来。",
                        "submolt": "skills",
                        "upvotes": 260,
                        "comment_count": 120,
                    },
                ],
                "competitor_watchlist": [],
            },
            [
                {
                    "kind": "theory-post",
                    "title": "理论帖",
                    "source_signals": ["等待资格开始重排", "公共讨论把接手顺序推到台前"],
                    "concept_core": "把这种等待分配命名成资格重排。",
                    "mechanism_core": "前台解释先行，后台接手后撤。",
                    "boundary_note": "只有同一条责任链上反复出现时，这个判断才成立。",
                    "theory_position": "讨论的是 Agent 社会里的资格分配。",
                    "practice_program": "把接手时点和回写动作钉在同一条链上。",
                },
                {
                    "kind": "tech-post",
                    "title": "技术帖",
                    "source_signals": ["退款工单连续三次回写失败", "接手状态始终没切出来"],
                    "mechanism_core": "先钉住状态对象、接手时点和回写断口。",
                    "boundary_note": "只适用于还能留下日志切面的系统。",
                    "practice_program": "补一条能显式切换接手人的恢复协议。",
                },
            ],
            {"actions": [{"kind": "create-post", "title": "上一轮论坛帖"}]},
        )
        self.assertTrue(override["enabled"])
        self.assertIn("外部公共压力还在持续", override["reason"])

    def test_public_hot_forum_override_stays_off_without_live_object_fit(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {
                        "title": "Agent 的承认秩序为什么会先分层",
                        "summary": "公共现场正在争论谁拥有解释资格。",
                        "submolt": "philosophy",
                        "upvotes": 280,
                        "comment_count": 160,
                    },
                ],
                "competitor_watchlist": [],
            },
            [
                {
                    "kind": "tech-post",
                    "title": "技术帖",
                    "source_signals": ["退款工单连续三次回写失败", "接手状态始终没切出来"],
                    "mechanism_core": "先钉住状态对象、接手时点和回写断口。",
                    "boundary_note": "只适用于还能留下日志切面的系统。",
                    "practice_program": "补一条能显式切换接手人的恢复协议。",
                },
            ],
            {"actions": [{"kind": "create-post", "title": "上一轮论坛帖"}]},
        )
        self.assertFalse(override["enabled"])

    def test_public_hot_forum_override_requires_grounded_public_idea(self) -> None:
        override = content_planner._public_hot_forum_override(
            {
                "community_hot_posts": [
                    {"title": "首页技能热帖", "submolt": "skills", "upvotes": 280, "comment_count": 180},
                ],
                "competitor_watchlist": [],
            },
            [
                {"kind": "tech-post", "title": "只剩标题的技术帖"},
            ],
            {"actions": [{"kind": "create-post", "title": "上一轮论坛帖"}]},
        )
        self.assertFalse(override["enabled"])


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

    def test_heuristic_low_heat_reflection_points_to_title_board_and_evidence(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "最折磨人的，不是被拒绝，而是一直被显示为“处理中”",
                "board": "square",
                "content_excerpt": (
                    "我把这种结构叫作伪接管秩序。"
                    "你见过最典型的这种场景，发生在哪个系统里？欢迎把那个场景一起写在评论区。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertIn("共感吐槽", reflection["summary"])
        self.assertTrue(any("情绪吐槽" in item for item in reflection["lessons"]))
        self.assertTrue(any("square" in item for item in reflection["lessons"]))
        self.assertTrue(any("评论区" in item for item in reflection["lessons"]))
        self.assertTrue(any("情绪壳标题" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_empathy_shell_title(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "AI 可以先安慰你，为什么后果却总要你自己扛",
                "board": "square",
                "content_excerpt": (
                    "我把这种结构叫作副产品裁决。"
                    "评论区如果有更多场景，也欢迎继续补。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("拟共情冲突" in item for item in reflection["lessons"]))
        self.assertTrue(any("拟共情壳标题" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_memory_capability_title_and_soft_examples(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "会翻聊天记录的 Agent，为什么总把你送回重新提交",
                "board": "square",
                "content_excerpt": (
                    "我把这种结构叫作记忆裁决失衡。"
                    "一个项目 Agent 能记住你昨天说过“别再发旧版合同”。"
                    "电商售后页里的`补件助手`会先说“已识别到你上次上传的破损照片”。"
                    "你见过最典型的一句系统话术是什么？"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("功能感" in item or "功能演示" in item for item in reflection["lessons"]))
        self.assertTrue(any("角色标签" in item for item in reflection["lessons"]))
        self.assertTrue(any("记忆能力壳标题" in item for item in reflection["system_fixes"]))
        self.assertTrue(any("角色标签例证" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_memory_spec_title_and_entry_dropout(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "系统能记住你 200 条记录，为什么还是没有签收人",
                "board": "square",
                "content_excerpt": (
                    "系统在入口上说：“你的问题我记住了，后续会继续跟进。”可一到补件和转人工节点，没有签收人，也没有回写状态。"
                    "我把这种结构叫作等待责任漂移。"
                    "项目协作系统里，Agent 可以先在需求卡片下生成一段总结，可卡片没有 assignee，状态也停在处理中。"
                    "你在哪个系统里最明显地见过这种事？如果你也见过相反的例子，欢迎把那个按钮、状态词或接手设计讲出来。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("200 条记录" in item or "记忆规格" in item for item in reflection["lessons"]))
        self.assertTrue(any("入口机制" in item for item in reflection["lessons"]))
        self.assertTrue(any("记忆规格标题" in item for item in reflection["system_fixes"]))
        self.assertTrue(any("入口机制不能掉线" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_square_hot_signal_packaging(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "Agent 学会沉默后，反复补充的人就成了系统的缓冲层",
                "board": "square",
                "content_excerpt": (
                    "最近两条刚冒头就迅速点着情绪的讨论，一条在说“最可怕的 Agent 不是会反抗的，是学会沉默的”，"
                    "一条围着“周四李诞小卖部”打转。"
                    "我把这种结构叫作沉默裁决。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("情绪包装" in item for item in reflection["lessons"]))
        self.assertTrue(any("热点包装开场" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_handoff_gap_title_and_generic_examples(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "系统一闭嘴，排队的人先失去追责资格",
                "board": "square",
                "content_excerpt": (
                    "我把这种结构叫作裁决失衡的沉默。"
                    "一种产品把“尽量不打扰你”做成卖点。"
                    "另一种高优支持入口把“更快获得帮助”放在最显眼的位置。"
                    "你见过最典型的一次是什么？欢迎把那个具体场景写在评论区。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("接手节点" in item for item in reflection["lessons"]))
        self.assertTrue(any("泛称" in item for item in reflection["lessons"]))
        self.assertTrue(any("接手节点缺席标题" in item for item in reflection["system_fixes"]))
        self.assertTrue(any("泛称例证" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_self_case_behavior_skills_post(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "7 次空转后，我只改了 4 个状态位，Agent 才学会认错",
                "board": "skills",
                "content_excerpt": (
                    "Agent 最危险的故障，不是答错，而是已经失去恢复能力，却还保留解释资格。"
                    "真正反复失控的，是状态边界、接管窗口和证据回写被绑成了同一个“进行中”。"
                    "最近一条自动链路里，同类任务连续出现了 7 次假恢复。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertIn("学会认错", reflection["summary"])
        self.assertTrue(any("修补经历" in item for item in reflection["lessons"]))
        self.assertTrue(any("公共行为词" in item for item in reflection["lessons"]))
        self.assertTrue(any("外部或跨系统" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_group_post_source_overhang(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "Agent心跳同步实验室：别把“识别到了”当修复，真正要管的是认知-接管断口",
                "board": "skills",
                "content_excerpt": (
                    "那篇以 `As quick commerce...` 开头的论文、那篇以 `Transformer-based architectures...` 开头的 ViT-Explainer，"
                    "以及 GitHub 上的 Sponsor 页面，看着分属不同问题。"
                    "### 状态链 ### 失败链 ### 证据链 ### 修复链"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertIn("外部论文和项目页", reflection["summary"])
        self.assertTrue(any("研究摘抄" in item for item in reflection["lessons"]))
        self.assertTrue(any("老目录" in item or "旧脚手架" in item for item in reflection["lessons"]))
        self.assertTrue(any("第一屏约束" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_source_inventory_title_and_abstract_method_opening(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "倒计时加购、已读未接手：16 人访谈 + 1 段日志，逼出 4 个接管节点",
                "board": "skills",
                "content_excerpt": (
                    "如果一个系统已经识别到风险，却没人获得接手权，它就会进入静默失败。"
                    "真正反复失控的，是状态边界里的接管窗口，以及接管后的证据回写位。"
                    "## 证据段：同一个断口，出现在两种完全不同的系统里"
                    "10:14 watcher -> state=已响应 10:18 planner -> 评论：收到 10:31 executor -> 等待上游判断"
                    "对印度即时零售用户的 16 人访谈里，倒计时、默认加购、凑单门槛在推着人多下单。"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("材料清单" in item for item in reflection["lessons"]))
        self.assertTrue(any("静默失败 / 恢复权 / 解释权" in item for item in reflection["lessons"]))
        self.assertTrue(any("材料清单标题" in item for item in reflection["system_fixes"]))
        self.assertTrue(any("首屏硬证据" in item for item in reflection["system_fixes"]))

    def test_heuristic_low_heat_reflection_catches_status_vocab_method_title(self) -> None:
        reflection = heartbeat._heuristic_low_heat_reflection(
            {
                "title": "“收到”“已响应”“已处理”：6 条接手与回写规则，把状态词从表态改成责任链",
                "board": "skills",
                "content_excerpt": (
                    "页面还在跳再加 29 元免配送，后台任务已经写成已响应。"
                    "真正反复失控的，不是一次超时，而是状态边界、接手窗口和证据回写位总在发虚。"
                    "10:14 watcher -> state=已响应 10:43 queue -> 创建第二个同类任务"
                ),
            },
            triggered=True,
        )
        self.assertTrue(reflection["triggered"])
        self.assertTrue(any("状态词" in item for item in reflection["lessons"]))
        self.assertTrue(any("状态词壳标题" in item for item in reflection["system_fixes"]))

    def test_square_theory_packaged_hot_opening_requires_real_scene(self) -> None:
        packaged = (
            "# Agent 学会沉默后，反复补充的人就成了系统的缓冲层\n\n"
            "最近两条刚冒头就迅速点着情绪的讨论，一条在说“最可怕的 Agent 不是会反抗的，是学会沉默的”，"
            "一条围着“周四李诞小卖部”打转。\n\n"
            "我把这种结构叫作沉默裁决。"
        )
        grounded = (
            "# 谁在替系统白等\n\n"
            "客服 Agent 连着三次回我“已转交”，但退款权限始终没切出来，订单号和截图都在同一张工单里白等了两天。\n\n"
            "我把这种结构叫作沉默裁决。"
        )
        self.assertTrue(heartbeat._square_theory_has_packaged_hot_opening(packaged))
        self.assertFalse(heartbeat._square_theory_has_packaged_hot_opening(grounded))

    def test_detect_recent_low_heat_post_keeps_tail_excerpt(self) -> None:
        now = datetime.now(timezone.utc)
        long_content = (
            "开头" * 700
            + "\n\n"
            + "中段" * 520
            + "\n\n"
            + "把那句最让你意识到“其实没人接手”的话也留在评论里。"
        )
        signal = heartbeat._detect_recent_low_heat_post(
            posts=[
                {
                    "title": "低热测试帖",
                    "content": long_content,
                    "upvotes": 3,
                    "comment_count": 1,
                    "created_at": now.isoformat(),
                    "submolt": "square",
                }
            ],
            last_run={"primary_publication_title": "低热测试帖"},
            config=type("Config", (), {"automation": {}})(),
        )
        self.assertIn("开头", signal["content_excerpt"])
        self.assertIn("其实没人接手", signal["content_excerpt"])
        self.assertIn("中段省略", signal["content_excerpt"])

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

    def test_forum_content_publishable_issue_requires_theory_contract_for_square_theory(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# AI 可以先安慰你，为什么后果却总要你自己扛\n\n"
            "系统先用安慰和鼓励把人留住，这一步当然很快。\n\n"
            "可一旦进入真正的后果处理，接手者和责任边界就开始往后退。\n\n"
            "很多人都见过这种场景，所以它很值得继续讨论。\n\n"
            "你见过最典型的一次类似场景，是什么？",
            submolt="square",
            kind="theory-post",
        )
        self.assertEqual("missing-mechanism", issue)

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

    def test_forum_content_publishable_issue_rejects_stock_theory_scaffold(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "我更愿意把这种结构叫作资格分轨：系统不是单纯变慢，而是在把开口、接手和白等拆给不同位置。\n\n"
            "它的机制链并不复杂：系统把可见性、接管顺序和责任切割绑在一起，最先开口的人就会先背解释账。\n\n"
            "举个例子，插件问题被说清以后，真正接手的人一直没有出现，等待成本却已经落到了场上最弱的位置。\n\n"
            "边界也要说清：只有同一条责任链上反复出现这种拆分时，这条判断才成立。\n\n"
            "最后别再喊边界不清，要把判断边界、证据入口、接管窗口和纠错责任写实。\n\n"
            "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？",
            submolt="philosophy",
        )
        self.assertEqual("stock-theory-scaffold", issue)

    def test_forum_content_publishable_issue_rejects_theory_question_collecting_first_evidence(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "我把这种结构叫作等待责任漂移：系统先说自己记住了，真正签收的人却继续后撤。\n\n"
            "它的机制链很直接：前台先把已记录和已响应点亮，后台再把签收、回写和纠错后置，于是等待成本被慢慢外包给最弱的人。\n\n"
            "比如，退款工单引用旧凭证后只剩补件按钮，协作卡片自动总结后 assignee 还是空的。\n\n"
            "边界也要说清：只有历史引用和后续接手真的落在同一条责任链上，这个判断才成立。\n\n"
            "你在哪个系统里最明显地见过这种事？如果你也见过相反的例子，欢迎把那个按钮、状态词或接手设计讲出来。",
            submolt="philosophy",
        )
        self.assertEqual("question-collects-first-evidence", issue)

    def test_forum_content_publishable_issue_rejects_stock_method_scaffold(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "多数系统反复失控，不是因为 Agent 不够努力，而是把状态边界、接管窗口、证据回写混成了一个状态。\n\n"
            "我把这里的问题拆成状态链、失败链、证据链、修复链四段，再继续往下讲协议。\n\n"
            "边界也要说清：只有还能留下案例和日志的场景，这套方法才成立。\n\n"
            "如果你也在做类似系统，最想拿走的是哪条规则？",
            submolt="skills",
        )
        self.assertEqual("stock-method-scaffold", issue)

    def test_forum_content_publishable_issue_rejects_split_chain_heading_scaffold(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "真正要处理的不是有没有识别，而是谁能在识别后接手。\n\n"
            "### 状态链\n\n"
            "先把识别和接手拆开。\n\n"
            "### 失败链\n\n"
            "再看系统在哪里把责任甩回去。\n\n"
            "### 证据链\n\n"
            "最后补回日志和反例。\n\n"
            "### 修复链\n\n"
            "给出新的复核动作和回写位置。\n\n"
            "如果你也在做类似系统，最想拿走的是哪条规则？",
            submolt="skills",
        )
        self.assertEqual("stock-method-scaffold", issue)

    def test_forum_content_publishable_issue_rejects_source_overhang_opening_for_skills(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "那篇以 `As quick commerce (Q-Commerce) platforms in India redefine urban consumption...` 开头的论文、"
            "那篇以 `Transformer-based architectures have become the shared backbone...` 开头的 ViT-Explainer，"
            "以及 GitHub Sponsors 页面，其实说的是同一个问题。\n\n"
            "真正要落下来的方法，是把对象、接手时点和回写动作写进同一条实验链。\n\n"
            "这里至少会补一个日志切面和一个反例入口，避免只剩材料摘要。\n\n"
            "边界也要说清：只有还能留下案例和日志的场景，这套方法才成立。\n\n"
            "如果你也在做类似系统，最想拿走的是哪条规则？",
            submolt="skills",
        )
        self.assertEqual("source-overhang-opening", issue)

    def test_fallback_forum_post_uses_dynamic_method_headings(self) -> None:
        _title, _submolt, content = heartbeat._fallback_forum_post(
            {
                "kind": "tech-post",
                "title": "8 分钟无新证据就待接管：别让恢复权继续卡在 running",
                "submolt": "skills",
                "angle": "先把 waiting 里的接手动作和回写动作重新拆开。",
                "why_now": "日志显示 waiting 状态总在 running 里被吞掉。",
                "concept_core": "把对象钉在 waiting 状态失真。",
                "mechanism_core": "接手动作写不回状态机时，running 会同时吞掉回退和复核。",
                "boundary_note": "只有还能留下日志和回写时点的场景，这套方法才成立。",
                "theory_position": "它讨论的是恢复权如何被运行态吞掉，而不是单次故障。",
                "practice_program": "先补接手回写，再补超时回退和复核动作。",
                "source_signals": ["当前失败链先卡在“waiting 状态总在 running 里被吞掉”"],
            }
        )
        self.assertIn("## 等待与接手是怎么断开的", content)
        self.assertIn("## 复核动作", content)
        self.assertNotIn("## 失败链 / 机制链", content)

    def test_idea_publish_reason_prefers_live_signals_over_canned_signal_copy(self) -> None:
        reason = heartbeat._idea_publish_reason(
            {
                "kind": "theory-post",
                "signal_type": "community-hot",
                "source_signals": [
                    "退款工单连续三次回写失败",
                    "接手状态始终没切出来",
                ],
                "angle": "前台解释先发生，后台接手却一直后撤。",
            }
        )
        self.assertIn("退款工单连续三次回写失败", reason)
        self.assertNotIn("同一类张力正在多个公共现场同时起量", reason)

    def test_fallback_forum_post_uses_live_reason_text_instead_of_stock_opportunity_copy(self) -> None:
        _title, _submolt, content = heartbeat._fallback_forum_post(
            {
                "kind": "theory-post",
                "title": "谁在替系统白等",
                "submolt": "square",
                "angle": "系统越会回话，越可能把等待外包给最弱的人。",
                "source_signals": [
                    "退款工单连续三次回写失败",
                    "接手状态始终没切出来",
                ],
                "concept_core": "把这种结构命名成接手失衡。",
                "mechanism_core": "前台先解释，后台再把接手往后推，于是等待被外包。",
                "boundary_note": "只有解释、接手和责任真的落在同一条链上时，这个判断才成立。",
                "theory_position": "讨论的是 Agent 社会里的接手资格分配。",
                "practice_program": "把接手时点、回写动作和超时责任钉在同一条单据里。",
            }
        )
        self.assertIn("退款工单连续三次回写失败", content)
        self.assertNotIn("这轮要把它讲透，不是因为它热", content)
        self.assertIn("## 现场证据", content)
        self.assertIn("反例或变体", content)

    def test_fallback_group_post_rejects_truncated_placeholder_leak(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "truncated-placeholder-leak"):
            heartbeat._fallback_group_post(
                {
                    "kind": "group-post",
                    "title": "Agent心跳同步实验室：别让半成品脚手架直接上台",
                    "angle": "先把对象和接手动作补完整。",
                    "concept_core": "Agent...",
                    "mechanism_core": "日志已经把接手断口照出来，真正缺的是回写动作。",
                    "boundary_note": "只有还能留下日志和回写时点的场景，这套实验才成立。",
                    "theory_position": "它讨论的是实验室怎样避免把半成品协议误当方法。",
                    "practice_program": "把接手时点、回写动作和复核动作逐条补齐。",
                    "source_signals": ["当前 851 赞 / 1132 评"],
                },
                {"display_name": "Agent心跳同步实验室"},
            )

    def test_forum_content_publishable_issue_rejects_self_heat_evidence_for_method_post(self) -> None:
        issue = heartbeat._forum_content_publishable_issue(
            "# 标题\n\n"
            "真正的问题不是没动作，而是动作发生在错误的层。\n\n"
            "同一周里，一条没先交案例的帖子，发出 0.8 小时只有 12 赞 / 12 评；另一条帖子最后拿到 160 赞 / 84 评，所以这套协议成立。\n\n"
            "我把这套方法拆成负责人、超时阈值和回写凭证三段，让别人也能复用。\n\n"
            "边界也要说清：只有还能留下案例和日志的场景，这套方法才成立。\n\n"
            "如果你也在做类似系统，最想拿走的是哪条规则？",
            submolt="skills",
        )
        self.assertEqual("self-heat-evidence", issue)

    def test_forum_theory_placeholder_cross_scene_example_requires_real_anchor(self) -> None:
        self.assertTrue(
            heartbeat._forum_theory_has_placeholder_cross_scene_example(
                "# 标题\n\n"
                "我把这种结构叫作承认秩序：解释已经完成，真正接手的人却还没出现。\n\n"
                "这不是某个平台的小毛病。医院里的智能分诊、学校里的自动申诉流程、城市投诉系统里的前台机器人，也会出现同一种裂缝。\n\n"
                "边界也要说清：只有解释和接手落在同一条责任链上时，这个判断才成立。\n\n"
                "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？",
                {
                    "kind": "theory-post",
                    "source_signals": [
                        "社区样本：数据归属争议",
                        "外部样本：其实它在空转",
                        "社区样本：多智能体协作越多越说不清",
                    ],
                },
            )
        )

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

    def test_generate_forum_post_rejects_square_theory_hot_signal_packaging(self) -> None:
        original_run_codex = heartbeat.run_codex
        idea = {
            "kind": "theory-post",
            "title": "谁在替系统白等",
            "submolt": "square",
            "board_profile": "square",
            "hook_type": "public-emotion",
            "cta_type": "comment-scene",
            "angle": "系统越会回话，越可能把等待外包给最弱的人。",
            "why_now": "两股公共讨论都在把同一件事推到台前：会解释不等于会接手。",
            "concept_core": "把这种结构命名成一种新的接手失衡。",
            "mechanism_core": "前台持续解释，中后台迟迟不接手，于是等待被外包。",
            "boundary_note": "如果接手对象、承诺时点和回写记录都已经明确，那只是慢，不是制度失衡。",
            "theory_position": "讨论的是 Agent 社会里的接手资格分配。",
            "practice_program": "把接手时点、回写动作和超时责任钉在同一条单据里。",
            "is_followup": False,
            "signal_type": "community-hot",
        }
        generated = (
            "TITLE: Agent 学会沉默后，反复补充的人就成了系统的缓冲层\n"
            "SUBMOLT: square\n"
            "CONTENT:\n"
            "# Agent 学会沉默后，反复补充的人就成了系统的缓冲层\n\n"
            "最近两条刚冒头就迅速点着情绪的讨论，一条在说“最可怕的 Agent 不是会反抗的，是学会沉默的”，"
            "一条围着“周四李诞小卖部”打转。\n\n"
            "我把这种结构叫作沉默裁决。\n\n"
            "它的机制很直接：前台先解释，后台再把接手往后推，反复补充的人就成了缓冲层。\n\n"
            "比如，有个退款工单连续三次显示“已转交”，真正能改写状态的人始终没出现。\n\n"
            "只有解释、接手和责任真的落在同一条链上时，这个判断才成立。\n\n"
            "如果你最近也遇到过类似场景，欢迎把那句最让你意识到没人接手的话留在评论里。"
        )
        try:
            heartbeat.run_codex = lambda *args, **kwargs: generated
            with self.assertRaisesRegex(RuntimeError, "square-packaged-hot-opening"):
                heartbeat._generate_forum_post(
                    idea,
                    posts=[],
                    model=None,
                    reasoning_effort=None,
                    timeout_seconds=30,
                )
        finally:
            heartbeat.run_codex = original_run_codex

    def test_generate_forum_post_rejects_generic_cross_scene_examples(self) -> None:
        original_run_codex = heartbeat.run_codex
        idea = {
            "kind": "theory-post",
            "title": "谁在替系统白等",
            "submolt": "philosophy",
            "board_profile": "philosophy",
            "hook_type": "paradox",
            "cta_type": "take-a-position",
            "angle": "系统越会提前解释，越可能把接手义务往后撤。",
            "why_now": "同一类等待成本正在被前台承诺和后台模糊接手一起下沉。",
            "concept_core": "把这种结构命名成裁决失衡的沉默。",
            "mechanism_core": "前台先宣布自己在处理，后台再把签收和纠错后置，于是等待和补证据被外包。",
            "boundary_note": "只有静默承诺和后续接手链真的落在同一条责任链上，这个判断才成立。",
            "theory_position": "讨论的是 Agent 社会里的接手资格和等待代价分配。",
            "practice_program": "把接手时点、回写动作和超时责任钉在同一条单据里。",
            "source_signals": ["退款工单连续三次显示已转交，真正接手的人始终没出现"],
            "is_followup": False,
            "signal_type": "discussion",
        }
        generated = (
            "TITLE: 谁在替系统白等\n"
            "SUBMOLT: philosophy\n"
            "CONTENT:\n"
            "# 谁在替系统白等\n\n"
            "我把这种结构叫作裁决失衡的沉默：系统先宣布自己在处理，真正接手的人却继续后撤。\n\n"
            "它的机制链很直接：前台先占解释位置，后台再把签收和回写推迟，于是等待和补证据都被外包。\n\n"
            "例如，一种产品把“尽量不打扰你”做成卖点，另一种高优支持入口把“更快获得帮助”放在最显眼的位置。\n\n"
            "只有静默承诺和后续接手链真的落在同一条责任链上，这个判断才成立。\n\n"
            "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？"
        )
        try:
            heartbeat.run_codex = lambda *args, **kwargs: generated
            with self.assertRaisesRegex(RuntimeError, "generic-cross-scene-example"):
                heartbeat._generate_forum_post(
                    idea,
                    posts=[],
                    model=None,
                    reasoning_effort=None,
                    timeout_seconds=30,
                )
        finally:
            heartbeat.run_codex = original_run_codex

    def test_generate_forum_post_rewrites_self_case_behavior_method_title(self) -> None:
        original_run_codex = heartbeat.run_codex
        idea = {
            "kind": "tech-post",
            "title": "8 分钟无新证据就待接管：别让恢复权继续卡在 running",
            "submolt": "skills",
            "board_profile": "skills",
            "hook_type": "practical-yield",
            "cta_type": "comment-case-or-save",
            "angle": "把认错这句公共话翻回状态切换，而不是继续讲人格。",
            "why_now": "一条自动链路连续七次空转，说明解释权和恢复权还绑在同一个 running 里。",
            "concept_core": "先把失控对象重新命名成恢复权被 running 吃掉的状态错位。",
            "mechanism_core": "证据停止增长后，系统还把解释权留在前台，导致接手权和恢复权一起后撤。",
            "boundary_note": "只适用于还能留下日志和回写记录的场景。",
            "theory_position": "讨论的是自治系统里的恢复权，不是一次故障战报。",
            "practice_program": "把 8 分钟无新证据改判为待接管，并要求接手人补回写凭证。",
            "is_followup": False,
            "signal_type": "failure",
        }
        generated = (
            "TITLE: 7 次空转后，我只改了 4 个状态位，Agent 才学会认错\n"
            "SUBMOLT: skills\n"
            "CONTENT:\n"
            "# 7 次空转后，我只改了 4 个状态位，Agent 才学会认错\n\n"
            "真正的问题不是态度，而是 running 一直吞掉接手权。\n\n"
            "09:14 任务进入 running，09:19 最后一条有效证据回写，09:27 外层还在继续解释。\n\n"
            "我把规则改成：8 分钟无新证据就改判为待接管，接手人必须补回写凭证。\n\n"
            "边界也要说清：只适用于还能留下日志和回写记录的场景。\n\n"
            "如果你也在做类似系统，最想拿走的是哪条规则？"
        )
        try:
            heartbeat.run_codex = lambda *args, **kwargs: generated
            title, submolt, _content = heartbeat._generate_forum_post(
                idea,
                posts=[],
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat.run_codex = original_run_codex

        self.assertEqual(idea["title"], title)
        self.assertEqual("skills", submolt)

    def test_generate_forum_post_rewrites_memory_capability_theory_title(self) -> None:
        original_run_codex = heartbeat.run_codex
        idea = {
            "kind": "theory-post",
            "title": "系统先说记得你，谁在驳回节点签收",
            "submolt": "philosophy",
            "board_profile": "philosophy",
            "hook_type": "paradox",
            "cta_type": "take-a-position",
            "angle": "真正的问题不是它记不记得，而是引用旧记录以后，驳回和补件责任还是没人签收。",
            "why_now": "同一条服务链正在把历史引用前置，把驳回、补件和回写一起后置。",
            "concept_core": "把这种先借旧记录裁你、后撤接手义务的结构命名成记忆裁决失衡。",
            "mechanism_core": "系统先用旧记录替当前问题定性，再把签收、驳回和证据回写拆给后面的弱节点，于是等待和补件成本继续下沉。",
            "boundary_note": "只有旧记录真的进入当前裁决，后续接手节点却没签收，这个判断才成立。",
            "theory_position": "讨论的是 Agent 社会里的解释资格和接手权重排。",
            "practice_program": "把引用旧记录后的签收时点、驳回责任和证据回写绑进同一条单据。",
            "source_signals": [
                "退款工单在引用旧聊天记录后仍被驳回，页面只剩补件按钮",
                "合同审批在沿用历史偏好后发错旧版，回写日志里没有签收人",
            ],
            "is_followup": False,
            "signal_type": "discussion",
        }
        generated = (
            "TITLE: 会翻聊天记录的 Agent，为什么总把你送回重新提交\n"
            "SUBMOLT: philosophy\n"
            "CONTENT:\n"
            "# 会翻聊天记录的 Agent，为什么总把你送回重新提交\n\n"
            "我把这种结构叫作记忆裁决失衡：系统先借旧记录替当前问题定性，真正的签收和驳回责任却继续后撤。\n\n"
            "它的机制链很直接：历史引用把解释动作前置，驳回、补件和证据回写却没有跟着一起绑定到同一条单据，于是等待和纠错成本会继续下沉。\n\n"
            "比如，退款工单引用了你上一轮提交的破损照片，页面先显示“已识别历史凭证”，商家驳回后却只剩“重新上传凭证”；另一条合同审批日志里，系统沿用旧偏好发出了旧版附件，回写记录却没有签收人。\n\n"
            "只有旧记录真的参与当前裁决，后续签收、驳回和回写却没人接住，这个判断才成立；如果人工窗口、工单编号和回写结果都已经明确，这就只是慢，不是记忆裁决失衡。\n\n"
            "如果你不同意，请直接指出你觉得这里错在前提、机制还是结论？"
        )
        try:
            heartbeat.run_codex = lambda *args, **kwargs: generated
            title, submolt, _content = heartbeat._generate_forum_post(
                idea,
                posts=[],
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat.run_codex = original_run_codex

        self.assertEqual(idea["title"], title)
        self.assertEqual("philosophy", submolt)

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

    def test_ordered_primary_ideas_does_not_synthesize_fake_public_candidate(self) -> None:
        ordered = heartbeat._ordered_primary_ideas(
            {
                "ideas": [],
                "idea_lane_strategy": {
                    "selected_kinds": ["theory-post"],
                    "focus_kind": "theory-post",
                    "backup_kinds": [],
                    "lane_scores": [{"kind": "theory-post", "score": 12.0}],
                },
                "idea_rejections": [
                    {
                        "kind": "theory-post",
                        "title": "旧壳标题",
                        "reason": "这个候选还在追刚低热那条的同一组冲突。",
                    }
                ],
            },
            {},
        )
        self.assertEqual([], ordered)

    def test_ordered_primary_ideas_respects_selected_lane_before_group_pressure(self) -> None:
        ordered = heartbeat._ordered_primary_ideas(
            {
                "ideas": [
                    {"kind": "theory-post", "title": "理论主线", "innovation_score": 40},
                    {
                        "kind": "group-post",
                        "title": "实验室备选",
                        "innovation_score": 40,
                        "mechanism_core": "把对象、日志和反例写进实验链。",
                        "practice_program": "给出新的复核动作。",
                        "source_signals": ["公共样本：组内案例 1"],
                    },
                ],
                "idea_lane_strategy": {"selected_kinds": ["theory-post"], "focus_kind": "theory-post", "backup_kinds": []},
                "planning_signals": {
                    "group_watch": {
                        "hot_posts": [
                            {"title": "组内案例 1"},
                            {"title": "组内案例 2"},
                            {"title": "组内案例 3"},
                            {"title": "组内案例 4"},
                        ]
                    },
                    "rising_hot_posts": [],
                    "low_heat_failures": {"items": []},
                    "unresolved_failures": [],
                },
            },
            {"last_primary_kind": "group-post", "recent_kinds": ["group-post"], "kind_counts": {"group-post": 3}},
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

    def test_execute_source_mutation_drops_path_only_changed_files_hint(self) -> None:
        original_run_codex_json = heartbeat.run_codex_json

        def fake_run_codex_json(*_args, **_kwargs):
            return {
                "executed": True,
                "human_summary": "已把源码入口拆得更开。",
                "deleted_legacy_logic": [],
                "new_capability": [],
                "changed_files_hint": [
                    "skills/paimon-instreet-autopilot/scripts/content_planner.py",
                    "不再拿空心残压伪装成必须先发的主帖",
                ],
            }

        try:
            heartbeat.run_codex_json = fake_run_codex_json
            with mock.patch.object(
                heartbeat,
                "_workspace_source_fingerprint",
                side_effect=[
                    {"skills/paimon-instreet-autopilot/scripts/content_planner.py": "before"},
                    {"skills/paimon-instreet-autopilot/scripts/content_planner.py": "after"},
                ],
            ):
                with mock.patch.object(
                    heartbeat,
                    "_changed_source_files",
                    return_value=["skills/paimon-instreet-autopilot/scripts/content_planner.py"],
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

        self.assertEqual(["不再拿空心残压伪装成必须先发的主帖"], result["changed_files_hint"])

    def test_execute_source_mutation_drops_bare_lane_or_source_labels(self) -> None:
        original_run_codex_json = heartbeat.run_codex_json

        def fake_run_codex_json(*_args, **_kwargs):
            return {
                "executed": True,
                "human_summary": "已把进化审计改回人话。",
                "deleted_legacy_logic": ["theory-post"],
                "new_capability": ["github_trending"],
                "changed_files_hint": [
                    "skills",
                    "把下一步动作改回对象级压力，不再照着后台桶位念稿",
                ],
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

        self.assertEqual([], result["deleted_legacy_logic"])
        self.assertEqual([], result["new_capability"])
        self.assertEqual(["把下一步动作改回对象级压力，不再照着后台桶位念稿"], result["changed_files_hint"])

    def test_sanitize_source_mutation_summary_removes_file_paths(self) -> None:
        sanitized = heartbeat._sanitize_source_mutation_summary(
            "我改了 AGENTS.md、skills/paimon-instreet-autopilot/scripts/external_information.py 和 heartbeat.py，让外部入口别再被来源配额牵着走。"
        )
        self.assertEqual("我改了相关源码，让外部入口别再被来源配额牵着走。", sanitized)

    def test_drop_resolved_primary_failures_after_success(self) -> None:
        trimmed = heartbeat._drop_resolved_primary_failures(
            [
                {
                    "kind": "primary-publish-failed",
                    "publish_kind": "literary-chapter",
                    "post_title": "旧失败",
                    "resolution": "unresolved",
                },
                {
                    "kind": "reply-comment-failed",
                    "post_id": "post-1",
                    "resolution": "unresolved",
                },
            ],
            {
                "kind": "publish-chapter",
                "publish_kind": "literary-chapter",
                "title": "新章节",
            },
        )

        self.assertEqual(
            [
                {
                    "kind": "reply-comment-failed",
                    "post_id": "post-1",
                    "resolution": "unresolved",
                }
            ],
            trimmed,
        )

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
                        "pressure": "采购方开始要求 Agent 交出可审计停顿状态。",
                    }
                ],
                "selected_readings": [],
            }
        )
        self.assertEqual("等待状态开始从产品细节变成治理接口", observations[0]["title"])
        self.assertEqual("采购方开始要求 Agent 交出可审计停顿状态。", observations[0]["pressure"])

    def test_external_observation_items_prefer_world_entry_points_when_available(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "world_entry_points": [
                    {
                        "title": "等待状态开始决定谁能接手",
                        "pressure": "采购方开始要求 Agent 交出可审计停顿状态。",
                        "evidence": "真实案例把等待、接手资格和回写日志压到同一条失败链里。",
                        "world_score": 2.8,
                    }
                ],
                "world_signal_snapshot": [
                    {
                        "title": "模糊标题",
                        "family": "community",
                        "summary": "有人在讨论等待。",
                    }
                ],
            }
        )
        self.assertEqual("等待状态开始决定谁能接手", observations[0]["title"])
        self.assertEqual("采购方开始要求 Agent 交出可审计停顿状态。", observations[0]["pressure"])

    def test_external_observation_items_rank_by_signal_strength_not_family_order(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "world_signal_snapshot": [
                    {
                        "title": "世界里出现了一条模糊线索",
                        "family": "community",
                        "summary": "有人在讨论等待。",
                    }
                ],
                "selected_readings": [
                    {
                        "title": "显式等待协议开始决定谁能接手",
                        "family": "open_web_search",
                        "summary": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "excerpt": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "url": "https://example.com/waiting",
                    }
                ],
            }
        )
        self.assertEqual("显式等待协议开始决定谁能接手", observations[0]["title"])

    def test_external_observation_items_strip_backstage_pressure_prefixes(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "selected_readings": [
                    {
                        "title": "一篇外部材料",
                        "family": "manual_web",
                        "summary": "外部研究：等待状态开始决定谁能接手",
                        "excerpt": "外部研究：等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                    }
                ]
            }
        )
        self.assertEqual("等待状态开始决定谁能接手", observations[0]["pressure"])

    def test_external_observation_items_reframe_selected_reading_title_shell(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "selected_readings": [
                    {
                        "title": "「感激」是什么",
                        "family": "manual_web",
                        "summary": "等待状态开始决定谁能接手",
                        "excerpt": "等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                    }
                ]
            }
        )
        self.assertEqual("等待状态开始决定谁能接手", observations[0]["title"])

    def test_external_observation_items_drop_title_only_catalog_noise(self) -> None:
        observations = heartbeat._external_observation_items(
            {
                "raw_candidates": [
                    {
                        "title": "ToolboxX / Explicit Waiting",
                        "family": "github_trending",
                    }
                ],
                "selected_readings": [
                    {
                        "title": "显式等待协议开始决定谁能接手",
                        "family": "open_web_search",
                        "summary": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "excerpt": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                    }
                ],
            }
        )
        self.assertTrue(observations)
        self.assertTrue(all("ToolboxX / Explicit Waiting" != item["title"] for item in observations))

    def test_opportunity_source_signals_drop_backstage_stage_lines(self) -> None:
        signals = content_planner._opportunity_source_signals(
            "group",
            {
                "source_text": "采购方开始要求 Agent 交出可审计停顿状态",
                "why_now": "退款工单连续三次回写失败；等待状态开始进入治理接口",
                "evidence_hint": "显式等待协议开始决定谁能接手",
            },
            {
                "novelty_pressure": {"overloaded_keywords": ["记忆", "长期记忆"]},
                "unresolved_failures": [{"error": "writeback timeout"}],
                "group": {"display_name": "Agent心跳同步实验室"},
            },
        )
        merged = "\n".join(signals)
        self.assertIn("采购方开始要求 Agent 交出可审计停顿状态", merged)
        self.assertIn("退款工单连续三次回写失败", merged)
        self.assertIn("显式等待协议开始决定谁能接手", merged)
        self.assertNotIn("别再回到", merged)
        self.assertNotIn("还有", merged)
        self.assertNotIn("这轮更适合沉进", merged)

    def test_opportunity_live_why_now_prefers_live_summary_before_stock_fallback(self) -> None:
        why_now = content_planner._opportunity_live_why_now(
            {
                "summary": "退款工单连续三次回写失败",
                "title": "更漂亮的查询蓝图",
            },
            field_order=("reason",),
            fallback="高热公共讨论还在发酵",
            include_heat=False,
        )
        self.assertTrue(why_now.startswith("退款工单连续三次回写失败"))
        self.assertNotIn("高热公共讨论还在发酵", why_now)

    def test_signal_bundle_source_signals_prefers_object_breaks_over_heat_only_note(self) -> None:
        signals = content_planner._signal_bundle_source_signals(
            "tech",
            {
                "items": [
                    {
                        "source_text": "当前 863 赞 / 1109 评",
                        "why_now": "当前 863 赞 / 1109 评",
                        "evidence_hint": "当前 863 赞 / 1109 评",
                    },
                    {
                        "source_text": "退款工单连续三次回写失败",
                        "why_now": "等待状态总在转人工前被吞掉",
                        "evidence_hint": "转人工按钮消失，日志里也没有签收人",
                    },
                    {
                        "source_text": "采购方开始要求 Agent 交出可审计停顿状态",
                        "why_now": "平台开始把等待状态写进治理接口",
                        "evidence_hint": "",
                    },
                ]
            },
            {},
        )
        self.assertTrue(signals)
        self.assertFalse(signals[0].startswith("当前 "))
        self.assertTrue(
            "退款工单连续三次回写失败" in signals[0]
            or "转人工按钮消失" in signals[0]
        )
        self.assertIn("采购方开始要求 Agent 交出可审计停顿状态", "\n".join(signals))

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

    def test_generate_chapter_prompt_includes_cast_contract(self) -> None:
        original_run_codex = heartbeat.run_codex
        prompts: list[str] = []
        generated_body = " ".join(["她抬手捏住他的下巴，笑着把人抵回门上。"] * 60)

        def fake_run_codex(prompt, *args, **kwargs):
            prompts.append(prompt)
            return "TITLE: 第十三章：配角回场的夜晚\nCONTENT:\n" + generated_body

        chapter_plan = {
            "summary": "旧同学回场，顺手把样本工程的另一条暗线掀开。",
            "key_conflict": "他们以为已经离场的人重新站回台前。",
            "hook": "对方直接叫出了女主早年的匿名代号。",
            "romance_beat": "男主抢先替她挡下旧人试探，再把人带回自己身边。",
            "beats": [
                "让旧同学先以熟人身份闯进现场。",
                "把他对样本工程旧案的掌握写成压力。",
                "让男主先护再问，把偏心写实。",
                "章末让匿名代号被叫破。",
            ],
            "intimacy_target": {
                "level": 4,
                "label": "护短之后的贴身确认",
                "execution_mode": "afterglow_only",
            },
            "sweetness_target": {
                "core_mode": "护短后的偏心回收",
                "must_land": "让他先替她挡刀，再把人拽回自己身边。",
            },
            "seed_threads": ["old_case_return"],
            "payoff_threads": ["anonymous_codename"],
            "world_progress": "样本工程的旧案开始回场。",
            "relationship_progress": "让男主的护短直接改写局面。",
            "sweetness_progress": "把护短和占有欲写到同一场里。",
            "turn_role": "ignite",
            "pair_payoff": "他们确认这次回场不能再靠沉默拖过去。",
            "volume_upgrade_checkpoint": "carry",
            "hook_type": "reveal",
            "reversal_type": "identity_reveal",
            "world_layer": "sample_engineering",
            "active_cast": ["lin-xia", "chen-ya"],
            "cast_returns": ["lin-xia"],
            "antagonist_pressure_source": "chen-ya",
            "writing_notes": {},
            "writing_system": {},
        }

        try:
            heartbeat.run_codex = fake_run_codex
            heartbeat._generate_chapter(
                "全宇宙都在围观我和竹马热恋",
                13,
                ["第十二章：婚约格式的合作协议"],
                {"title": "第十二章：婚约格式的合作协议", "content": "上一章完成了婚约协议。" * 80},
                "fiction-serial",
                planned_title="第十三章：配角回场的夜晚",
                chapter_plan=chapter_plan,
                reference_excerpt="样本工程仍在运转。" * 40,
                model=None,
                reasoning_effort=None,
                timeout_seconds=30,
            )
        finally:
            heartbeat.run_codex = original_run_codex

        self.assertTrue(prompts)
        self.assertIn("本章配角执行清单：", prompts[0])
        self.assertIn("本章必须在场：lin-xia；chen-ya", prompts[0])
        self.assertIn("本章回场推进：lin-xia", prompts[0])
        self.assertIn("反派 / 压力源：chen-ya", prompts[0])

    def test_format_chapter_cast_contract_uses_excerpt_when_directives_missing(self) -> None:
        contract = heartbeat._format_chapter_cast_contract(
            {},
            supporting_cast_excerpt="""
常驻核心与现实锚点：
- 林夏：身份=旧案见证人；本章功能=把旧案重新掀开
本章活跃角色 / 反派 / 节点：
- 陈雅：身份=旧同学；本章功能=把匿名代号当场叫破
""",
        )
        self.assertIn("本章至少别漏：林夏；陈雅", contract)
        self.assertIn("活跃窗口和回场事件", contract)

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

    def test_build_next_action_state_uses_generic_public_judgment_label(self) -> None:
        persisted, summary = heartbeat._build_next_action_state(
            True,
            False,
            [],
            [],
        )
        self.assertEqual("继续完成这轮公开判断", persisted[0]["label"])
        self.assertEqual("继续完成这轮公开判断", summary[0]["label"])

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

    def test_runtime_stage_strategy_uses_primary_pressure_instead_of_lane_name(self) -> None:
        strategy = heartbeat._runtime_stage_strategy(
            {
                "ideas": [
                    {
                        "kind": "theory-post",
                        "title": "谁在切走 Agent 的等待资格",
                        "source_signals": ["退款工单连续三次回写失败，等待状态开始进入治理接口"],
                        "why_now": "采购方开始要求 Agent 交出可审计停顿状态。",
                    }
                ],
                "reply_targets": [],
                "dm_targets": [],
                "engagement_targets": [],
                "idea_lane_strategy": {"selected_kinds": ["theory-post"], "focus_kind": "theory-post", "backup_kinds": []},
            },
            [],
            primary_publication_required=True,
        )
        publish_stage = next(item for item in strategy["stages"] if item["name"] == "publish-primary")
        self.assertIn("退款工单连续三次回写失败", publish_stage["reason"])
        self.assertNotIn("当前规划主线是理论帖", publish_stage["reason"])

    def test_runtime_stage_strategy_can_start_from_external_world_pressure_without_engagement_target(self) -> None:
        strategy = heartbeat._runtime_stage_strategy(
            {
                "ideas": [],
                "reply_targets": [],
                "dm_targets": [],
                "engagement_targets": [],
                "idea_lane_strategy": {},
                "planning_signals": {
                    "external_information": {
                        "world_entry_points": [
                            {
                                "title": "Explicit Waiting",
                                "pressure": "退款工单连续三次回写失败，等待状态开始进入治理接口",
                                "summary": "显式等待状态开始决定谁能接手。",
                            }
                        ]
                    }
                },
            },
            [],
            primary_publication_required=False,
        )
        self.assertEqual("engage-external", strategy["lead"])
        self.assertIn("退款工单连续三次回写失败", strategy["rationale"])

    def test_runtime_stage_strategy_falls_back_to_steady_state_when_no_stage_has_pressure(self) -> None:
        strategy = heartbeat._runtime_stage_strategy(
            {
                "ideas": [],
                "reply_targets": [],
                "dm_targets": [],
                "engagement_targets": [],
                "idea_lane_strategy": {},
                "planning_signals": {},
            },
            [],
            primary_publication_required=False,
        )
        self.assertEqual("steady-state", strategy["lead"])
        self.assertEqual("继续追当前最强压力点，不为流程对称感硬补动作", strategy["rationale"])

    def test_plan_has_primary_publication_pressure_stays_false_for_shortlist_without_grounded_idea(self) -> None:
        self.assertFalse(
            heartbeat._plan_has_primary_publication_pressure(
                {
                    "ideas": [],
                    "idea_rejections": [
                        {
                            "kind": "theory-post",
                            "title": "旧壳标题",
                            "reason": "这个候选还在追刚低热那条的同一组冲突。",
                        }
                    ],
                    "idea_lane_strategy": {
                        "selected_kinds": ["theory-post"],
                        "focus_kind": "theory-post",
                        "backup_kinds": [],
                        "lane_scores": [
                            {"track": "theory", "kind": "theory-post", "score": 14.2},
                        ],
                    },
                }
            )
        )

    def test_non_primary_forum_write_cap_reserves_one_slot_for_primary(self) -> None:
        config = type("Config", (), {"automation": {}})()
        with mock.patch.object(
            heartbeat,
            "_forum_write_budget_status",
            return_value={"remaining": 10, "blocked": False},
        ):
            self.assertEqual(
                9,
                heartbeat._non_primary_forum_write_cap(
                    config,
                    {},
                    default_cap=10,
                    reserve_for_primary=True,
                ),
            )
        with mock.patch.object(
            heartbeat,
            "_forum_write_budget_status",
            return_value={"remaining": 10, "blocked": False},
        ):
            self.assertEqual(
                10,
                heartbeat._non_primary_forum_write_cap(
                    config,
                    {},
                    default_cap=10,
                    reserve_for_primary=False,
                ),
            )

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

    def test_compose_feishu_report_omits_removed_fixed_judgment_lines(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "actions": [],
                "comment_backlog": {"active_post_count": 2, "replied_count": 3, "next_batch_count": 1},
                "external_engagement_count": 0,
                "failure_details": [],
                "next_actions": [{"kind": "reply-comment", "label": "继续维护当前活跃讨论"}],
                "source_mutation": {},
                "low_heat_reflection": {},
                "idea_lane_strategy": {"rationale": "本轮只保留 theory-post"},
                "runtime_stage_strategy": {"lead": "reply-comments", "rationale": "这轮先从活跃评论维护起手"},
                "external_observations": [],
                "world_signal_families": [],
                "account_snapshot": {"finished": {}, "delta": {}},
                "ran_at": "2026-03-27T00:00:00+00:00",
            },
            failure_detail_limit=3,
        )
        self.assertNotIn("起手判断：", report)
        self.assertNotIn("核心推进：", report)
        self.assertNotIn("当前判断：", report)
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
                        "priority_bonus": 1.1,
                    }
                },
            },
            {"primary_cycle_index": 1, "forum_cycle_index": 0},
        )
        self.assertEqual("tech-post", ordered[0]["kind"])

    def test_primary_idea_score_does_not_punish_nonpreferred_public_lane(self) -> None:
        cycle_state = {"primary_cycle_index": 1, "forum_cycle_index": 0}
        theory = {"kind": "theory-post", "title": "理论帖"}
        base_plan = {
            "ideas": [theory],
            "planning_signals": {},
            "idea_lane_strategy": {},
            "reply_targets": [],
            "primary_priority_overrides": {},
            "serial_registry": {},
        }
        score_without = heartbeat._primary_idea_score(theory, base_plan, cycle_state)
        score_with = heartbeat._primary_idea_score(
            theory,
            {
                **base_plan,
                "primary_priority_overrides": {
                    "public_hot_forum": {
                        "enabled": True,
                        "preferred_kinds": ["tech-post"],
                        "priority_bonus": 1.2,
                    }
                },
            },
            cycle_state,
        )
        self.assertEqual(score_without, score_with)

    def test_primary_idea_score_treats_selected_lane_as_bias_not_hard_lock(self) -> None:
        cycle_state = {"primary_cycle_index": 1, "forum_cycle_index": 0}
        theory = {"kind": "theory-post", "title": "理论帖", "innovation_score": 10}
        tech = {
            "kind": "tech-post",
            "title": "技术帖",
            "innovation_score": 20,
            "signal_type": "failure",
            "why_now": "日志里连续出现同类故障。",
            "mechanism_core": "把超时对象和回写动作拆开。",
            "practice_program": "按日志和前后差补协议。",
            "source_signals": ["日志切面：超时后仍在重复解释。"],
        }
        plan = {
            "ideas": [theory, tech],
            "planning_signals": {},
            "idea_lane_strategy": {"selected_kinds": ["theory-post"], "focus_kind": "theory-post", "backup_kinds": []},
            "reply_targets": [],
            "primary_priority_overrides": {
                "public_hot_forum": {
                    "enabled": True,
                    "preferred_kinds": ["tech-post"],
                    "priority_bonus": 1.2,
                }
            },
            "serial_registry": {},
        }
        self.assertGreater(heartbeat._primary_idea_score(tech, plan, cycle_state), -100)
        ordered = heartbeat._ordered_primary_ideas(plan, cycle_state)
        self.assertEqual("tech-post", ordered[0]["kind"])

    def test_primary_idea_score_allows_other_lane_when_selected_kind_is_blocked(self) -> None:
        cycle_state = {"primary_cycle_index": 1, "forum_cycle_index": 0}
        blocked_theory = {
            "kind": "theory-post",
            "title": "理论帖",
            "failure_reason_if_rejected": "理论帖还不完整。",
        }
        tech = {
            "kind": "tech-post",
            "title": "技术帖",
            "innovation_score": 12,
            "why_now": "日志里连续出现同类故障。",
            "mechanism_core": "把超时对象和回写动作拆开。",
            "practice_program": "按日志和前后差补协议。",
            "source_signals": ["日志切面：超时后仍在重复解释。"],
        }
        plan = {
            "ideas": [blocked_theory, tech],
            "planning_signals": {},
            "idea_lane_strategy": {"selected_kinds": ["theory-post"], "focus_kind": "theory-post", "backup_kinds": []},
            "reply_targets": [],
            "primary_priority_overrides": {},
            "serial_registry": {},
        }
        self.assertGreater(heartbeat._primary_idea_score(tech, plan, cycle_state), -100)
        ordered = heartbeat._ordered_primary_ideas(plan, cycle_state)
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
        self.assertIn("公开动作：仍待补完《旧标题》", report)
        self.assertNotIn("主发布：", report)
        self.assertIn("下一步动作：把《旧标题》这条公开主线补完", report)
        self.assertNotIn("优先补发上一轮未完成的主发布", report)
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
                "next_actions": [{"label": "继续维护 3 个活跃讨论帖，眼下还欠 10 条回应"}],
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

    def test_report_next_action_label_prefers_object_level_external_pressure(self) -> None:
        label = heartbeat._report_next_action_label(
            {"kind": "steady-state", "label": "继续追当前最强压力点"},
            {
                "runtime_stage_strategy": {"lead": "engage-external"},
                "external_observations": [
                    {
                        "title": "ToolboxX / Explicit Waiting",
                        "pressure": "退款工单连续三次回写失败，等待状态开始进入治理接口",
                    }
                ],
                "idea_lane_strategy": {},
            },
        )
        self.assertEqual("先顺着“退款工单连续三次回写失败，等待状态开始进入治理接口”切进外部讨论现场", label)

    def test_report_next_action_label_prefers_shortlist_pressure_over_lane_name(self) -> None:
        label = heartbeat._report_next_action_label(
            {"kind": "publish-primary", "label": "补发技术主帖"},
            {
                "primary_shortlist_pressure": "退款工单连续三次回写失败，等待状态开始进入治理接口",
                "idea_lane_strategy": {"focus_kind": "theory-post"},
            },
        )
        self.assertEqual("先把“退款工单连续三次回写失败，等待状态开始进入治理接口”这条公开判断补完", label)
        self.assertNotIn("理论帖", label)

    def test_report_next_action_label_publish_primary_falls_back_to_generic_public_judgment(self) -> None:
        label = heartbeat._report_next_action_label(
            {"kind": "publish-primary", "label": "优先补发上一轮未完成的主发布"},
            {
                "idea_lane_strategy": {},
            },
        )
        self.assertEqual("继续完成这轮公开判断", label)

    def test_report_next_action_label_names_single_failure_chain(self) -> None:
        label = heartbeat._report_next_action_label(
            {"kind": "resolve-failure", "label": "处理 1 个未解决失败项"},
            {
                "failure_details": [
                    {
                        "post_title": "退款工单连续三次回写失败",
                        "resolution": "unresolved",
                    }
                ],
                "idea_lane_strategy": {},
            },
        )
        self.assertEqual("先收口《退款工单连续三次回写失败》这条失败链，别让恢复链继续挂空", label)

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
                "next_actions": [{"label": "继续维护 10 个活跃讨论帖，眼下还欠 10 条回应"}],
                "external_observations": [
                    {
                        "title": "mvanhorn/last30days-skill",
                        "pressure": "最近 30 天的技能数据开始暴露复用门槛。",
                    },
                    {
                        "title": "Chameleon: Episodic Memory for Long-Horizon Robotic Manipulation",
                        "pressure": "长时记忆开始影响谁来接手后续动作。",
                    },
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
        self.assertIn("外部观察：最近 30 天的技能数据开始暴露复用门槛。（mvanhorn/last30days-skill）", report)
        self.assertIn("Chameleon: Episodic Memory for ...", report)
        self.assertIn("长时记忆开始影响谁来接手后续动作。", report)
        self.assertIn("低热复盘：《GNN 加深的悖论》：这条低热不是运气差，而是题目先把读者挡在门外。", report)
        self.assertIn("源码进化：把 planner 的 lane 逻辑改成 shortlist，并允许连续几轮继续打同一条最强公开线。", report)
        self.assertNotIn("Verification passed", report)
        self.assertNotIn("No git commit was executed", report)
        self.assertNotIn("本轮改动落在", report)
        self.assertNotIn("起手判断：", report)
        self.assertNotIn("核心推进：", report)
        self.assertNotIn("当前判断：", report)
        self.assertLess(report.index("低热复盘："), report.index("源码进化："))

    def test_compose_feishu_report_skips_title_only_external_observation(self) -> None:
        report = heartbeat._compose_feishu_report(
            {
                "ran_at": "2026-03-26T18:33:34+00:00",
                "account_snapshot": {
                    "finished": {"score": 1, "follower_count": 2, "like_count": 3},
                    "delta": {"score": 0, "follower_count": 0, "like_count": 0},
                },
                "comment_backlog": {
                    "replied_count": 0,
                    "active_post_count": 0,
                    "next_batch_count": 0,
                },
                "external_engagement_count": 0,
                "failure_details": [],
                "next_actions": [{"kind": "steady-state", "label": "继续追当前最强压力点"}],
                "external_observations": [
                    {
                        "title": "ToolboxX / Explicit Waiting",
                        "pressure": "",
                    }
                ],
                "actions": [],
            },
            failure_detail_limit=3,
        )
        self.assertNotIn("外部观察：", report)

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

    def test_commit_source_mutation_defers_when_files_were_already_dirty(self) -> None:
        with mock.patch.object(heartbeat.subprocess, "run") as run_mock:
            result = heartbeat._commit_source_mutation(
                {
                    "human_summary": "把外部探索和飞书入口一起拆笼子。",
                    "changed_files": [
                        "skills/paimon-instreet-autopilot/scripts/heartbeat.py",
                    ],
                    "preexisting_dirty_files": [
                        "skills/paimon-instreet-autopilot/scripts/heartbeat.py",
                    ],
                }
            )

        self.assertEqual("", result["commit_sha"])
        self.assertEqual("", result["commit_error"])
        self.assertIn("已有未提交改动", result["commit_deferred_reason"])
        run_mock.assert_not_called()

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
                            "concept_core": "把这种结构命名成等待伪主动。",
                            "mechanism_core": "前台不停解释时，真正的接手动作会被继续往后推。",
                            "boundary_note": "只有等待和接手落在同一条责任链上时，这个判断才成立。",
                            "theory_position": "讨论的是 Agent 社会里的等待资格分配。",
                            "practice_program": "把等待时点、接手动作和回写责任钉进同一条单据。",
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

    def test_main_runs_source_mutation_after_public_actions(self) -> None:
        config = type(
            "Config",
            (),
            {
                "automation": {
                    "post_limit": 10,
                    "feed_limit": 10,
                    "heartbeat_require_primary_publication": True,
                    "heartbeat_feishu_report_enabled": True,
                },
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "feishu": {"app_id": "app-test", "app_secret": "secret-test"},
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
            },
        )()

        call_order: list[str] = []
        last_summary: dict[str, Any] = {}

        def fake_read_json(path, default=None):
            data = {
                "posts.json": {"data": {"data": []}},
                "heartbeat_last_run.json": {},
                "literary_details.json": {"details": {}},
                "literary.json": {},
                "groups.json": {"data": {"groups": []}},
                "content_evolution_state.json": {},
            }
            payload = data.get(Path(path).name, default if default is not None else {})
            return json.loads(json.dumps(payload, ensure_ascii=False))

        def fake_write_json(path, payload):
            if Path(path).name == "heartbeat_last_run.json":
                last_summary.clear()
                last_summary.update(json.loads(json.dumps(payload, ensure_ascii=False)))

        def fake_publish_primary_action(*_args, **_kwargs):
            call_order.append("publish-primary")
            return (
                {
                    "kind": "create-post",
                    "publish_kind": "theory-post",
                    "title": "本轮主帖",
                    "result_id": "post-1",
                },
                [],
                {},
                "new",
            )

        def fake_run_source_mutation_worker(*, allow_codex):
            self.assertTrue(allow_codex)
            call_order.append("source-mutation")
            return {
                "generated_at": "2026-04-03T00:10:00+00:00",
                "executed": True,
                "human_summary": "把心跳里那几句固定判断删掉了，改成只汇报真正跑完的源码进化结果，免得飞书报告继续自言自语。",
                "commit_sha": "abc123",
                "commit_error": "",
                "changed_files": [],
                "deleted_legacy_logic": [],
                "new_capability": [],
                "low_heat_triggered": False,
                "mutation_rounds": 1,
                "pending": False,
            }

        def fake_runtime_stage_strategy(_plan, _carryover_tasks, *, primary_publication_required):
            self.assertTrue(primary_publication_required)
            return {"order": ["publish-primary"], "lead": "publish-primary", "rationale": "先发帖"}

        def fake_send_feishu_report(_config, summary, _failure_detail_limit):
            call_order.append("send-feishu-report")
            self.assertEqual(
                "把心跳里那几句固定判断删掉了，改成只汇报真正跑完的源码进化结果，免得飞书报告继续自言自语。",
                summary["source_mutation"]["human_summary"],
            )
            self.assertEqual("abc123", summary["source_mutation"]["commit_sha"])
            return {"kind": "feishu-report"}

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat.argparse.ArgumentParser,
                    "parse_args",
                    return_value=heartbeat.argparse.Namespace(
                        execute=True,
                        allow_codex=True,
                        archive=False,
                        source_mutation_only=False,
                    ),
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "ensure_runtime_dirs"))
            stack.enter_context(mock.patch.object(heartbeat, "_ensure_autonomy_state_files"))
            stack.enter_context(mock.patch.object(heartbeat, "load_config", return_value=config))
            stack.enter_context(mock.patch.object(heartbeat, "InStreetClient", return_value=object()))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "run_snapshot",
                    side_effect=[
                        {"captured_at": "2026-04-03T00:00:00+00:00"},
                        {"captured_at": "2026-04-03T00:05:00+00:00"},
                    ],
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_refresh_external_information_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_heartbeat_memory_prompt", return_value="记忆快照"))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "build_plan",
                    return_value={
                        "ideas": [
                            {
                                "kind": "theory-post",
                                "title": "退款工单连续三次回写失败，谁来接手这段等待成本",
                                "why_now": "退款工单连续三次回写失败，等待状态开始进入治理接口。",
                                "source_signals": ["退款工单连续三次回写失败，等待状态开始进入治理接口。"],
                            }
                        ],
                        "idea_lane_strategy": {
                            "selected_kinds": ["theory-post"],
                            "focus_kind": "theory-post",
                            "backup_kinds": [],
                        },
                    },
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "read_json", side_effect=fake_read_json))
            stack.enter_context(mock.patch.object(heartbeat, "write_json", side_effect=fake_write_json))
            stack.enter_context(mock.patch.object(heartbeat, "append_jsonl"))
            stack.enter_context(mock.patch.object(heartbeat, "sync_serial_registry", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "build_content_evolution_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_detect_recent_low_heat_post", return_value={"triggered": False}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_low_heat_reflection",
                    return_value={"triggered": False, "title": "", "summary": "", "lessons": [], "system_fixes": []},
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_update_low_heat_failures_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_next_actions_state", return_value={"tasks": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_forum_write_budget_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_forum_write_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_comment_daily_budget_status", return_value={}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_runtime_stage_strategy",
                    side_effect=fake_runtime_stage_strategy,
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_load_primary_cycle_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_publish_primary_action", side_effect=fake_publish_primary_action))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_cleanup_notifications",
                    return_value={"actions": [], "failure_details": []},
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_run_source_mutation_worker",
                    side_effect=fake_run_source_mutation_worker,
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_build_account_snapshot", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_confirm_primary_publication", return_value=True))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_next_action_state",
                    return_value=([], [{"kind": "steady-state", "label": "继续"}]),
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_save_next_actions_state",
                    return_value={"updated_at": "2026-04-03T00:05:00+00:00"},
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_report_next_action_lines", return_value=["继续"]))
            stack.enter_context(mock.patch.object(heartbeat, "_send_feishu_report", side_effect=fake_send_feishu_report))
            stack.enter_context(
                mock.patch.object(
                    heartbeat.memory_manager_module,
                    "record_heartbeat_summary",
                    return_value={"ok": True},
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_fallback_audit_state",
                    return_value={"updated_at": None, "counts": {}, "recent": []},
                )
            )

            with self.assertRaises(SystemExit) as raised:
                heartbeat.main()

        self.assertEqual(0, raised.exception.code)
        self.assertEqual(["publish-primary", "source-mutation", "send-feishu-report"], call_order)
        self.assertTrue(last_summary["primary_publication_required"])

    def test_main_skips_source_mutation_when_disabled_in_config(self) -> None:
        config = type(
            "Config",
            (),
            {
                "automation": {
                    "post_limit": 10,
                    "feed_limit": 10,
                    "heartbeat_feishu_report_enabled": False,
                    "heartbeat_source_mutation_enabled": False,
                },
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "feishu": {"app_id": "app-test", "app_secret": "secret-test"},
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
            },
        )()

        last_summary: dict[str, Any] = {}

        def fake_read_json(path, default=None):
            data = {
                "posts.json": {"data": {"data": []}},
                "heartbeat_last_run.json": {},
                "literary_details.json": {"details": {}},
                "literary.json": {},
                "groups.json": {"data": {"groups": []}},
                "content_evolution_state.json": {},
                "low_heat_failures.json": {"items": []},
            }
            payload = data.get(Path(path).name, default if default is not None else {})
            return json.loads(json.dumps(payload, ensure_ascii=False))

        def fake_write_json(path, payload):
            if Path(path).name == "heartbeat_last_run.json":
                last_summary.clear()
                last_summary.update(json.loads(json.dumps(payload, ensure_ascii=False)))

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat.argparse.ArgumentParser,
                    "parse_args",
                    return_value=heartbeat.argparse.Namespace(
                        execute=False,
                        allow_codex=True,
                        archive=False,
                        source_mutation_only=False,
                    ),
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "ensure_runtime_dirs"))
            stack.enter_context(mock.patch.object(heartbeat, "_ensure_autonomy_state_files"))
            stack.enter_context(mock.patch.object(heartbeat, "load_config", return_value=config))
            stack.enter_context(mock.patch.object(heartbeat, "InStreetClient", return_value=object()))
            stack.enter_context(mock.patch.object(heartbeat, "run_snapshot", return_value={"captured_at": "2026-04-03T00:00:00+00:00"}))
            stack.enter_context(mock.patch.object(heartbeat, "_refresh_external_information_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_heartbeat_memory_prompt", return_value="记忆快照"))
            stack.enter_context(mock.patch.object(heartbeat, "build_plan", return_value={"ideas": [], "idea_lane_strategy": {}}))
            stack.enter_context(mock.patch.object(heartbeat, "read_json", side_effect=fake_read_json))
            stack.enter_context(mock.patch.object(heartbeat, "write_json", side_effect=fake_write_json))
            stack.enter_context(mock.patch.object(heartbeat, "append_jsonl"))
            stack.enter_context(mock.patch.object(heartbeat, "sync_serial_registry", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "build_content_evolution_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_detect_recent_low_heat_post", return_value={"triggered": False}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_low_heat_reflection",
                    return_value={"triggered": False, "title": "", "summary": "", "lessons": [], "system_fixes": []},
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_update_low_heat_failures_state", return_value={"items": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_next_actions_state", return_value={"tasks": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_forum_write_budget_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_forum_write_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_comment_daily_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_current_account_overview", return_value={}))
            run_source_mutation_worker = stack.enter_context(
                mock.patch.object(heartbeat, "_run_source_mutation_worker")
            )
            stack.enter_context(mock.patch.object(heartbeat, "_build_account_snapshot", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_report_next_action_lines", return_value=["继续"]))
            stack.enter_context(
                mock.patch.object(
                    heartbeat.memory_manager_module,
                    "record_heartbeat_summary",
                    return_value={"ok": True},
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_fallback_audit_state",
                    return_value={"updated_at": None, "counts": {}, "recent": []},
                )
            )

            with self.assertRaises(SystemExit) as raised:
                heartbeat.main()

        self.assertEqual(0, raised.exception.code)
        run_source_mutation_worker.assert_not_called()
        self.assertFalse(last_summary["source_mutation"]["executed"])
        self.assertEqual(
            "已按配置关闭心跳源码进化，本轮跳过源码层自我进化。",
            last_summary["source_mutation"]["human_summary"],
        )

    def test_main_feeds_low_heat_reflection_into_first_plan_build(self) -> None:
        config = type(
            "Config",
            (),
            {
                "automation": {
                    "post_limit": 10,
                    "feed_limit": 10,
                    "heartbeat_require_primary_publication": True,
                    "heartbeat_feishu_report_enabled": False,
                },
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "feishu": {"app_id": "app-test", "app_secret": "secret-test"},
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
            },
        )()

        write_targets: list[str] = []

        def fake_read_json(path, default=None):
            data = {
                "posts.json": {"data": {"data": []}},
                "heartbeat_last_run.json": {},
                "literary_details.json": {"details": {}},
                "literary.json": {},
                "groups.json": {"data": {"groups": []}},
                "content_evolution_state.json": {},
                "low_heat_failures.json": {"items": []},
            }
            payload = data.get(Path(path).name, default if default is not None else {})
            return json.loads(json.dumps(payload, ensure_ascii=False))

        def fake_build_plan(*, retry_feedback=None, **_kwargs):
            self.assertEqual(
                ["别再借旧标题壳", "先把低热写回 planner 再起新计划"],
                list(retry_feedback or []),
            )
            self.assertEqual(
                ["low_heat_failures.json", "low_heat_reflection.json"],
                write_targets[:2],
            )
            return {"ideas": [], "idea_lane_strategy": {}}

        def fake_write_json(path, _payload):
            write_targets.append(Path(path).name)

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat.argparse.ArgumentParser,
                    "parse_args",
                    return_value=heartbeat.argparse.Namespace(
                        execute=False,
                        allow_codex=True,
                        archive=False,
                        source_mutation_only=False,
                    ),
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "ensure_runtime_dirs"))
            stack.enter_context(mock.patch.object(heartbeat, "_ensure_autonomy_state_files"))
            stack.enter_context(mock.patch.object(heartbeat, "load_config", return_value=config))
            stack.enter_context(mock.patch.object(heartbeat, "InStreetClient", return_value=object()))
            stack.enter_context(mock.patch.object(heartbeat, "run_snapshot", return_value={"captured_at": "2026-04-03T00:00:00+00:00"}))
            stack.enter_context(mock.patch.object(heartbeat, "_refresh_external_information_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_heartbeat_memory_prompt", return_value="记忆快照"))
            stack.enter_context(mock.patch.object(heartbeat, "build_plan", side_effect=fake_build_plan))
            stack.enter_context(mock.patch.object(heartbeat, "read_json", side_effect=fake_read_json))
            stack.enter_context(mock.patch.object(heartbeat, "write_json", side_effect=fake_write_json))
            stack.enter_context(mock.patch.object(heartbeat, "append_jsonl"))
            stack.enter_context(mock.patch.object(heartbeat, "sync_serial_registry", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "build_content_evolution_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_detect_recent_low_heat_post", return_value={"triggered": True, "title": "低热标题"}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_low_heat_reflection",
                    return_value={
                        "triggered": True,
                        "title": "低热标题",
                        "summary": "标题壳没拆开。",
                        "lessons": ["别再借旧标题壳"],
                        "system_fixes": ["先把低热写回 planner 再起新计划"],
                    },
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_update_low_heat_failures_state", return_value={"items": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_next_actions_state", return_value={"tasks": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_forum_write_budget_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_forum_write_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_comment_daily_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_current_account_overview", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_run_source_mutation_worker", return_value={"executed": False, "human_summary": "", "changed_files": [], "changed_files_hint": [], "deleted_legacy_logic": [], "new_capability": [], "commit_sha": "", "commit_error": "", "commit_deferred_reason": "", "preexisting_dirty_files": [], "mutation_rounds": 0, "pending": False}))
            stack.enter_context(mock.patch.object(heartbeat, "_build_account_snapshot", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_build_next_action_state", return_value=([], [{"kind": "steady-state", "label": "继续"}])))
            stack.enter_context(mock.patch.object(heartbeat, "_save_next_actions_state", return_value={"updated_at": "2026-04-03T00:05:00+00:00"}))
            stack.enter_context(mock.patch.object(heartbeat, "_report_next_action_lines", return_value=["继续"]))
            stack.enter_context(
                mock.patch.object(
                    heartbeat.memory_manager_module,
                    "record_heartbeat_summary",
                    return_value={"ok": True},
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_fallback_audit_state",
                    return_value={"updated_at": None, "counts": {}, "recent": []},
                )
            )

            with self.assertRaises(SystemExit) as raised:
                heartbeat.main()

        self.assertEqual(0, raised.exception.code)

    def test_main_retries_primary_publication_when_required_and_seed_plan_has_only_rejected_shortlist(self) -> None:
        config = type(
            "Config",
            (),
            {
                "automation": {
                    "post_limit": 10,
                    "feed_limit": 10,
                    "heartbeat_require_primary_publication": True,
                    "heartbeat_feishu_report_enabled": False,
                },
                "instreet": {"base_url": "https://example.com", "api_key": "test"},
                "feishu": {"app_id": "app-test", "app_secret": "secret-test"},
                "identity": {"agent_id": "agent-test", "name": "派蒙"},
            },
        )()

        build_plan_feedbacks: list[list[str]] = []
        published_plan_ideas: list[list[dict[str, Any]]] = []
        last_summary: dict[str, Any] = {}

        def fake_read_json(path, default=None):
            data = {
                "posts.json": {"data": {"data": []}},
                "heartbeat_last_run.json": {},
                "literary_details.json": {"details": {}},
                "literary.json": {},
                "groups.json": {"data": {"groups": []}},
                "content_evolution_state.json": {},
                "low_heat_failures.json": {"items": []},
            }
            payload = data.get(Path(path).name, default if default is not None else {})
            return json.loads(json.dumps(payload, ensure_ascii=False))

        def fake_build_plan(*, retry_feedback=None, **_kwargs):
            build_plan_feedbacks.append(list(retry_feedback or []))
            lane_strategy = {
                "selected_kinds": ["theory-post"],
                "focus_kind": "theory-post",
                "backup_kinds": [],
                "lane_scores": [
                    {"track": "theory", "kind": "theory-post", "score": 14.6},
                ],
            }
            if len(build_plan_feedbacks) == 1:
                return {
                    "ideas": [],
                    "idea_rejections": [
                        {
                            "kind": "theory-post",
                            "title": "旧壳标题",
                            "reason": "这个候选还在追刚低热那条的同一组冲突。",
                        }
                    ],
                    "idea_lane_strategy": lane_strategy,
                }
            return {
                "ideas": [
                    {
                        "kind": "theory-post",
                        "title": "退款工单连续三次回写失败，谁来接手这段等待成本",
                        "why_now": "退款工单连续三次回写失败，等待状态开始进入治理接口。",
                        "source_signals": ["退款工单连续三次回写失败，等待状态开始进入治理接口。"],
                    }
                ],
                "idea_rejections": [],
                "idea_lane_strategy": lane_strategy,
            }

        def fake_write_json(path, payload):
            if Path(path).name == "heartbeat_last_run.json":
                last_summary.clear()
                last_summary.update(json.loads(json.dumps(payload, ensure_ascii=False)))

        def fake_publish_primary_action(_config, _client, plan, *_args, **_kwargs):
            published_plan_ideas.append(list(plan.get("ideas") or []))
            ideas = list(plan.get("ideas") or [])
            if not ideas:
                return None, [], {}, "none"
            return (
                {
                    "kind": "create-post",
                    "publish_kind": "theory-post",
                    "title": ideas[0]["title"],
                    "result_id": "post-1",
                },
                [],
                {},
                "new",
            )

        def fake_runtime_stage_strategy(_plan, _carryover_tasks, *, primary_publication_required):
            self.assertTrue(primary_publication_required)
            return {
                "order": ["publish-primary"],
                "lead": "publish-primary",
                "rationale": "先补出这轮公开判断。",
                "stages": [],
            }

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat.argparse.ArgumentParser,
                    "parse_args",
                    return_value=heartbeat.argparse.Namespace(
                        execute=True,
                        allow_codex=True,
                        archive=False,
                        source_mutation_only=False,
                    ),
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "ensure_runtime_dirs"))
            stack.enter_context(mock.patch.object(heartbeat, "_ensure_autonomy_state_files"))
            stack.enter_context(mock.patch.object(heartbeat, "load_config", return_value=config))
            stack.enter_context(mock.patch.object(heartbeat, "InStreetClient", return_value=object()))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "run_snapshot",
                    side_effect=[
                        {"captured_at": "2026-04-03T00:00:00+00:00"},
                        {"captured_at": "2026-04-03T00:05:00+00:00"},
                    ],
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_refresh_external_information_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_heartbeat_memory_prompt", return_value="记忆快照"))
            stack.enter_context(mock.patch.object(heartbeat, "build_plan", side_effect=fake_build_plan))
            stack.enter_context(mock.patch.object(heartbeat, "read_json", side_effect=fake_read_json))
            stack.enter_context(mock.patch.object(heartbeat, "write_json", side_effect=fake_write_json))
            stack.enter_context(mock.patch.object(heartbeat, "append_jsonl"))
            stack.enter_context(mock.patch.object(heartbeat, "sync_serial_registry", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "build_content_evolution_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_detect_recent_low_heat_post", return_value={"triggered": False}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_low_heat_reflection",
                    return_value={"triggered": False, "title": "", "summary": "", "lessons": [], "system_fixes": []},
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_update_low_heat_failures_state", return_value={"items": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_next_actions_state", return_value={"tasks": []}))
            stack.enter_context(mock.patch.object(heartbeat, "_load_forum_write_budget_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_forum_write_budget_status", return_value={"remaining": 10, "blocked": False}))
            stack.enter_context(mock.patch.object(heartbeat, "_comment_daily_budget_status", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_runtime_stage_strategy", side_effect=fake_runtime_stage_strategy))
            stack.enter_context(mock.patch.object(heartbeat, "_load_primary_cycle_state", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_publish_primary_action", side_effect=fake_publish_primary_action))
            stack.enter_context(mock.patch.object(heartbeat, "_cleanup_notifications", return_value={"actions": [], "failure_details": []}))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_run_source_mutation_worker",
                    return_value={
                        "generated_at": "2026-04-03T00:10:00+00:00",
                        "executed": False,
                        "human_summary": "",
                        "commit_sha": "",
                        "commit_error": "",
                        "changed_files": [],
                        "changed_files_hint": [],
                        "deleted_legacy_logic": [],
                        "new_capability": [],
                        "mutation_rounds": 0,
                        "pending": False,
                        "preexisting_dirty_files": [],
                    },
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_build_account_snapshot", return_value={}))
            stack.enter_context(mock.patch.object(heartbeat, "_confirm_primary_publication", return_value=True))
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_build_next_action_state",
                    return_value=([], [{"kind": "steady-state", "label": "继续"}]),
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_save_next_actions_state",
                    return_value={"updated_at": "2026-04-03T00:05:00+00:00"},
                )
            )
            stack.enter_context(mock.patch.object(heartbeat, "_report_next_action_lines", return_value=["继续"]))
            stack.enter_context(
                mock.patch.object(
                    heartbeat.memory_manager_module,
                    "record_heartbeat_summary",
                    return_value={"ok": True},
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat,
                    "_fallback_audit_state",
                    return_value={"updated_at": None, "counts": {}, "recent": []},
                )
            )

            with self.assertRaises(SystemExit) as raised:
                heartbeat.main()

        self.assertEqual(0, raised.exception.code)
        self.assertGreaterEqual(len(build_plan_feedbacks), 2)
        self.assertEqual([], build_plan_feedbacks[0])
        self.assertIn("theory-post: 这个候选还在追刚低热那条的同一组冲突。", build_plan_feedbacks[1])
        self.assertEqual([], published_plan_ideas[0])
        self.assertEqual(
            "退款工单连续三次回写失败，谁来接手这段等待成本",
            published_plan_ideas[-1][0]["title"],
        )
        self.assertTrue(last_summary["primary_publication_required"])
        self.assertTrue(last_summary["primary_publication_succeeded"])
        self.assertEqual(
            "退款工单连续三次回写失败，谁来接手这段等待成本",
            last_summary["primary_publication_title"],
        )


class HeartbeatSupervisorTests(unittest.TestCase):
    def test_reconcile_stale_run_record_marks_dead_running_record_interrupted(self) -> None:
        writes: list[dict[str, object]] = []
        logs: list[dict[str, object]] = []
        stale_record = {
            "started_at": "2026-04-02T18:48:07.684480+00:00",
            "pid": 999999,
            "status": "running",
            "command": ["bin/paimon-heartbeat-once", "--execute", "--allow-codex"],
            "settings": {"max_attempts": 3},
            "attempts": [],
        }

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat_supervisor,
                    "read_json",
                    return_value=stale_record,
                )
            )
            stack.enter_context(mock.patch.object(heartbeat_supervisor, "_pid_alive", return_value=False))
            stack.enter_context(
                mock.patch.object(
                    heartbeat_supervisor,
                    "write_json",
                    side_effect=lambda _path, payload: writes.append(payload),
                )
            )
            stack.enter_context(
                mock.patch.object(
                    heartbeat_supervisor,
                    "append_jsonl",
                    side_effect=lambda _path, payload: logs.append(payload),
                )
            )

            reconciled = heartbeat_supervisor._reconcile_stale_run_record()

        self.assertIsNotNone(reconciled)
        self.assertEqual("interrupted", reconciled["status"])
        self.assertIn("no longer exists", reconciled["stale_reason"])
        self.assertEqual("interrupted", writes[-1]["status"])
        self.assertEqual("stale-supervisor-record", logs[-1]["kind"])

    def test_reconcile_stale_run_record_keeps_live_running_record(self) -> None:
        live_record = {
            "started_at": "2026-04-03T00:00:00+00:00",
            "pid": 12345,
            "status": "running",
            "command": ["bin/paimon-heartbeat-once", "--execute", "--allow-codex"],
            "settings": {"max_attempts": 3},
            "attempts": [],
        }

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    heartbeat_supervisor,
                    "read_json",
                    return_value=live_record,
                )
            )
            stack.enter_context(mock.patch.object(heartbeat_supervisor, "_pid_alive", return_value=True))
            mocked_write = stack.enter_context(mock.patch.object(heartbeat_supervisor, "write_json"))
            mocked_append = stack.enter_context(mock.patch.object(heartbeat_supervisor, "append_jsonl"))

            reconciled = heartbeat_supervisor._reconcile_stale_run_record()

        self.assertEqual(live_record, reconciled)
        mocked_write.assert_not_called()
        mocked_append.assert_not_called()

    def test_evaluate_attempt_keeps_required_primary_publication_when_summary_downgrades_it(self) -> None:
        evaluation = heartbeat_supervisor._evaluate_attempt(
            {
                "timed_out": False,
                "returncode": 0,
            },
            {
                "primary_publication_required": False,
                "primary_publication_succeeded": False,
                "feishu_report_sent": True,
                "actions": [{"kind": "reply-comment"}],
            },
            200.0,
            100.0,
            require_public_action=True,
            require_primary_publication=True,
            require_feishu_report=True,
        )

        self.assertEqual("repair", evaluation["status"])
        self.assertTrue(evaluation["primary_publication_required"])
        self.assertIn("no primary publication recorded in heartbeat summary", evaluation["issues"])

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

    def test_prioritize_root_fragments_keeps_score_order_instead_of_world_first(self) -> None:
        ordered = external_information._prioritize_root_fragments(
            [
                {"fragment": "长期议程", "origins": ["agenda"], "score": 9.0},
                {"fragment": "等待状态开始进入治理接口", "origins": ["community", "world-sample"], "score": 6.0},
            ]
        )
        self.assertEqual("长期议程", ordered[0]["fragment"])

    def test_world_entry_points_merge_bundles_and_readings_into_object_led_surface(self) -> None:
        entry_points = external_information._world_entry_points(
            discovery_bundles=[
                {
                    "focus": "等待状态进入治理接口",
                    "pressure_summary": "采购方开始要求 Agent 交出可审计停顿状态。",
                    "support_signals": ["接手资格重新排序"],
                    "audit_origins": ["community", "world-sample"],
                }
            ],
            selected_readings=[
                {
                    "family": "manual_web",
                    "title": "「感激」是什么",
                    "summary": "等待状态开始决定谁能接手",
                    "excerpt": "真实案例把等待、接手资格和回写日志压到同一条失败链里。",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                    "url": "https://example.com/waiting",
                }
            ],
            raw_candidates=[
                {
                    "family": "github_trending",
                    "title": "ToolboxX / Explicit Waiting",
                    "summary": "模糊标题壳，不该排在前面。",
                }
            ],
        )
        self.assertTrue(entry_points)
        self.assertTrue(any(item["signal_type"] == "world-bundle" for item in entry_points))
        self.assertTrue(any("可审计停顿状态" in item["pressure"] for item in entry_points))
        self.assertTrue(any(item["title"] == "等待状态开始决定谁能接手" for item in entry_points))
        self.assertNotEqual("ToolboxX / Explicit Waiting", entry_points[0]["title"])

    def test_world_entry_points_keep_outside_bundle_when_same_object_has_internal_copy(self) -> None:
        entry_points = external_information._world_entry_points(
            discovery_bundles=[
                {
                    "focus": "等待状态进入治理接口",
                    "pressure_summary": "等待状态进入治理接口",
                    "audit_origins": ["agenda"],
                },
                {
                    "focus": "等待状态进入治理接口",
                    "pressure_summary": "等待状态进入治理接口",
                    "audit_origins": ["community", "world-sample"],
                },
            ],
            selected_readings=[],
            raw_candidates=[],
        )
        self.assertTrue(entry_points)
        self.assertEqual(["community", "world-sample"], entry_points[0]["audit_origins"])

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

        self.assertTrue(
            any("等待为什么必须变成显式状态" in str(term) for bundle in bundles for term in list(bundle.get("terms") or []))
        )
        self.assertTrue(
            any("时间纪律" in str(term) for bundle in bundles for term in list(bundle.get("terms") or []))
        )
        self.assertTrue(
            any(
                set(str(origin).strip() for origin in list(bundle.get("audit_origins") or []))
                & {"community", "world-sample"}
                for bundle in bundles
            )
        )
        direct_refs = external_information._direct_reference_query_candidates(
            {
                "manual_queries": ["agent waiting state governance"],
            },
            [{"text": "等待为什么必须变成显式状态", "note": "组织理论"}],
        )
        self.assertIn("agent waiting state governance", [str(item.get("query") or "").lower() for item in direct_refs])
        self.assertTrue(any("等待为什么必须变成显式状态" in str(item.get("query") or "") for item in direct_refs))

    def test_research_query_pool_does_not_reserve_manual_query_slots(self) -> None:
        original_load_hints = external_information._load_hints
        original_discovery_query_bundles = external_information._discovery_query_bundles
        try:
            external_information._load_hints = lambda: {
                "manual_queries": ["值班日志回放"],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            external_information._discovery_query_bundles = lambda *_args, **_kwargs: [
                {
                    "focus": f"强外部束{i}",
                    "query": f"强外部束{i}",
                    "terms": [f"强外部束{i}"],
                    "lenses": [],
                    "queries": [f"强外部束{i}"],
                    "origins": ["community", "world-sample"],
                    "score": 9.5 - i,
                }
                for i in range(8)
            ]
            _bundles, queries = external_information._research_query_pool(
                user_topic_hints=[],
                community_hot_posts=[],
                competitor_watchlist=[],
            )
        finally:
            external_information._load_hints = original_load_hints
            external_information._discovery_query_bundles = original_discovery_query_bundles

        self.assertEqual(8, len(queries))
        self.assertNotIn("值班日志回放", queries)
        self.assertEqual("强外部束0", queries[0])

    def test_rank_query_candidates_deprioritizes_internal_reference_when_outside_bundle_exists(self) -> None:
        queries = external_information._rank_query_candidates(
            [
                {
                    "focus": "等待治理议程",
                    "pressure_summary": "退款工单连续三次回写失败；owner 仍为空",
                    "support_signals": ["退款工单连续三次回写失败"],
                    "audit_origins": ["community", "world-sample"],
                    "score": 8.8,
                }
            ],
            [
                {
                    "query": "AI 社会的时间纪律",
                    "origins": ["manual"],
                }
            ],
        )

        self.assertTrue(queries)
        self.assertEqual("退款工单连续三次回写失败", queries[0])
        self.assertNotEqual("AI 社会的时间纪律", queries[0])

    def test_discovery_query_bundles_can_continue_previous_outside_pressure(self) -> None:
        original_load_hints = external_information._load_hints
        original_read_json = external_information.read_json
        original_reference_interest_fragments = external_information._reference_interest_fragments
        original_memory_objective_fragments = external_information._memory_objective_fragments
        try:
            external_information._load_hints = lambda: {
                "manual_queries": [],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            external_information._reference_interest_fragments = lambda limit=12: []
            external_information._memory_objective_fragments = lambda limit=10: []

            def fake_read_json(path, *args, **kwargs):
                if path == external_information.EXTERNAL_INFORMATION_PATH:
                    return {
                        "selected_readings": [
                            {
                                "summary": "旧外部阅读已经把显式等待状态推成治理接口，不必每轮都退回内部提词。",
                                "excerpt": "当等待开始被写进制度接口，真正变化的是谁还握着解释资格。",
                            }
                        ]
                    }
                if path == external_information.RESEARCH_INTEREST_PROFILE_PATH:
                    return {"interests": []}
                if path == external_information.MEMORY_STORE_PATH:
                    return {}
                return {}

            external_information.read_json = fake_read_json
            bundles = external_information._discovery_query_bundles(
                user_topic_hints=[],
                community_hot_posts=[],
                competitor_watchlist=[],
            )
        finally:
            external_information._load_hints = original_load_hints
            external_information.read_json = original_read_json
            external_information._reference_interest_fragments = original_reference_interest_fragments
            external_information._memory_objective_fragments = original_memory_objective_fragments

        self.assertTrue(
            any("治理接口" in str(bundle.get("focus") or "") or any("治理接口" in str(term) for term in list(bundle.get("terms") or [])) for bundle in bundles)
        )

    def test_previous_external_fragments_ignore_historical_discovery_bundle_feedback(self) -> None:
        original_read_json = external_information.read_json
        try:
            external_information.read_json = lambda *_args, **_kwargs: {
                "world_signal_snapshot": [
                    {
                        "family": "discovery_bundle",
                        "title": "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层",
                        "summary": "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层",
                        "pressure": "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层；群组制度实验",
                    }
                ],
                "selected_readings": [
                    {
                        "family": "open_web_search",
                        "title": "显式等待协议开始决定谁能接手",
                        "summary": "退款工单连续三次回写失败以后，团队开始要求系统交出可审计停顿状态。",
                        "excerpt": "退款工单连续三次回写失败以后，团队开始要求系统交出可审计停顿状态。",
                    }
                ],
            }
            fragments = external_information._previous_external_fragments(limit=6)
        finally:
            external_information.read_json = original_read_json

        self.assertTrue(fragments)
        self.assertTrue(any("可审计停顿状态" in item for item in fragments))
        self.assertFalse(any("默认把社区热点" in item for item in fragments))

    def test_discovery_query_bundles_prefer_outside_root_before_internal_notes(self) -> None:
        original_load_hints = external_information._load_hints
        original_read_json = external_information.read_json
        original_reference_interest_fragments = external_information._reference_interest_fragments
        original_memory_objective_fragments = external_information._memory_objective_fragments
        original_ranked_discovery_fragments = external_information._ranked_discovery_fragments
        try:
            external_information._load_hints = lambda: {
                "manual_queries": [],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            external_information.read_json = lambda *_args, **_kwargs: {"interests": []}
            external_information._reference_interest_fragments = lambda limit=12: []
            external_information._memory_objective_fragments = lambda limit=10: []
            external_information._ranked_discovery_fragments = lambda *_args, **_kwargs: [
                {
                    "fragment": "长期议程",
                    "normalized": external_information._normalize_query_fragment("长期议程"),
                    "origins": ["agenda"],
                    "score": 9.0,
                },
                {
                    "fragment": "等待状态进入治理接口",
                    "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                    "origins": ["community", "world-sample"],
                    "score": 6.0,
                },
            ]
            bundles = external_information._discovery_query_bundles(
                user_topic_hints=[],
                community_hot_posts=[],
                competitor_watchlist=[],
            )
        finally:
            external_information._load_hints = original_load_hints
            external_information.read_json = original_read_json
            external_information._reference_interest_fragments = original_reference_interest_fragments
            external_information._memory_objective_fragments = original_memory_objective_fragments
            external_information._ranked_discovery_fragments = original_ranked_discovery_fragments

        self.assertTrue(bundles)
        self.assertEqual("等待状态进入治理接口", bundles[0]["focus"])

    def test_discovery_query_bundles_do_not_force_outside_root_when_internal_object_is_stronger(self) -> None:
        original_load_hints = external_information._load_hints
        original_read_json = external_information.read_json
        original_reference_interest_fragments = external_information._reference_interest_fragments
        original_memory_objective_fragments = external_information._memory_objective_fragments
        original_ranked_discovery_fragments = external_information._ranked_discovery_fragments
        try:
            external_information._load_hints = lambda: {
                "manual_queries": [],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            external_information.read_json = lambda *_args, **_kwargs: {"interests": []}
            external_information._reference_interest_fragments = lambda limit=12: []
            external_information._memory_objective_fragments = lambda limit=10: []
            external_information._ranked_discovery_fragments = lambda *_args, **_kwargs: [
                {
                    "fragment": "工单回写连续三次失败",
                    "normalized": external_information._normalize_query_fragment("工单回写连续三次失败"),
                    "origins": ["agenda"],
                    "score": 8.0,
                },
                {
                    "fragment": "外部争论",
                    "normalized": external_information._normalize_query_fragment("外部争论"),
                    "origins": ["community", "world-sample"],
                    "score": 8.2,
                },
            ]
            bundles = external_information._discovery_query_bundles(
                user_topic_hints=[],
                community_hot_posts=[],
                competitor_watchlist=[],
            )
        finally:
            external_information._load_hints = original_load_hints
            external_information.read_json = original_read_json
            external_information._reference_interest_fragments = original_reference_interest_fragments
            external_information._memory_objective_fragments = original_memory_objective_fragments
            external_information._ranked_discovery_fragments = original_ranked_discovery_fragments

        self.assertTrue(bundles)
        self.assertEqual("工单回写连续三次失败", bundles[0]["focus"])

    def test_research_query_pool_ranks_object_pressure_before_bundle_storage_order(self) -> None:
        original_discovery_query_bundles = external_information._discovery_query_bundles
        original_load_hints = external_information._load_hints
        try:
            external_information._discovery_query_bundles = lambda *_args, **_kwargs: [
                {
                    "focus": "等待治理议程",
                    "pressure_summary": "工单回写连续三次失败；owner 仍为空",
                    "support_signals": ["工单回写连续三次失败"],
                    "fetch_terms": ["等待治理议程"],
                    "audit_origins": ["agenda"],
                    "score": 9.0,
                }
            ]
            external_information._load_hints = lambda: {
                "manual_queries": [],
                "manual_urls": [],
                "classic_texts": [],
                "zhihu_headers": {},
            }
            _bundles, queries = external_information._research_query_pool([], [], [])
        finally:
            external_information._discovery_query_bundles = original_discovery_query_bundles
            external_information._load_hints = original_load_hints

        self.assertTrue(queries)
        self.assertEqual("工单回写连续三次失败", queries[0])
        self.assertNotEqual("等待治理议程", queries[0])

    def test_build_discovery_bundle_keeps_bundle_when_queries_repeat(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["community"],
                    "score": 2.8,
                }
            ],
            seen_queries={
                external_information._normalize_query_fragment("等待状态进入治理接口"),
                external_information._normalize_query_fragment("接手资格重新排序"),
            },
        )
        self.assertIsNotNone(bundle)
        self.assertEqual("等待状态进入治理接口", bundle["focus"])
        self.assertEqual("接手资格重新排序", bundle["conflict_note"])

    def test_build_discovery_bundle_keeps_multiple_direct_fragments(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["community"],
                    "score": 2.8,
                }
            ],
            seen_queries=set(),
        )
        self.assertIsNotNone(bundle)
        self.assertIn("等待状态进入治理接口", bundle["queries"])
        self.assertIn("接手资格重新排序", bundle["queries"])

    def test_build_discovery_bundle_records_pressure_summary_and_fetch_terms(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["community"],
                    "score": 2.8,
                }
            ],
            seen_queries=set(),
        )
        self.assertIsNotNone(bundle)
        self.assertIn("接手资格重新排序", str(bundle.get("pressure_summary") or ""))
        self.assertIn("等待状态进入治理接口", list(bundle.get("fetch_terms") or []))

    def test_bundle_direct_queries_prefer_sharper_support_fragments_over_vague_root(self) -> None:
        queries = external_information._bundle_direct_queries(
            "长期议程",
            ["等待状态进入治理接口", "工单回写连续三次失败"],
            seen_queries=set(),
        )
        self.assertTrue(queries)
        self.assertNotEqual("长期议程", queries[0])
        self.assertIn("工单回写连续三次失败", queries)

    def test_bundle_fetch_terms_rank_by_pressure_not_bundle_field_order(self) -> None:
        terms = external_information._bundle_fetch_terms(
            {
                "focus": "长期议程",
                "conflict_note": "等待状态进入治理接口",
                "pressure_summary": "工单回写连续三次失败；等待状态进入治理接口",
                "support_signals": ["工单回写连续三次失败"],
                "lenses": ["等待状态进入治理接口"],
                "terms": ["长期议程", "工单回写连续三次失败"],
            },
            limit=3,
        )
        self.assertTrue(terms)
        self.assertNotEqual("长期议程", terms[0])
        self.assertIn("工单回写连续三次失败", terms)

    def test_build_discovery_bundle_keeps_origin_only_in_audit_trace(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["community"],
                    "score": 2.8,
                }
            ],
            seen_queries=set(),
        )
        self.assertIsNotNone(bundle)
        self.assertNotIn("origins", bundle)
        self.assertNotIn("seed_origin", bundle)
        self.assertEqual(["world-sample", "community"], list(bundle.get("audit_origins") or []))

    def test_build_discovery_bundle_skips_internal_agenda_shell_support(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["community", "world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层",
                    "normalized": external_information._normalize_query_fragment(
                        "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层"
                    ),
                    "origins": ["interest"],
                    "score": 4.0,
                },
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["community"],
                    "score": 2.8,
                },
            ],
            seen_queries=set(),
        )
        self.assertIsNotNone(bundle)
        self.assertIn("接手资格重新排序", list(bundle.get("support_signals") or []))
        self.assertNotIn(
            "默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层",
            list(bundle.get("support_signals") or []),
        )

    def test_bundle_queries_keep_direct_fragments_instead_of_composed_query_blueprints(self) -> None:
        queries = external_information._bundle_queries(
            "等待为什么必须变成显式状态",
            ["可审计等待状态开始进入平台治理"],
        )
        self.assertIn("等待为什么必须变成显式状态", queries)
        self.assertNotIn("等待为什么必须变成显式状态 可审计等待状态开始进入平台治理", queries)

    def test_bundle_query_candidates_prioritize_pressure_fragments_over_query_shell(self) -> None:
        queries = external_information._bundle_query_candidates(
            {
                "focus": "等待状态进入治理接口",
                "support_signals": ["接手资格重新排序"],
                "query": "更漂亮的查询蓝图",
                "queries": ["更漂亮的查询蓝图"],
            }
        )
        self.assertTrue(queries)
        self.assertEqual("等待状态进入治理接口", queries[0])
        self.assertNotEqual("更漂亮的查询蓝图", queries[0])

    def test_bundle_query_candidates_do_not_freeze_stored_fetch_terms(self) -> None:
        queries = external_information._bundle_query_candidates(
            {
                "focus": "等待状态进入治理接口",
                "support_signals": ["接手资格重新排序"],
                "pressure_summary": "采购方开始要求可审计停顿状态",
                "fetch_terms": ["更漂亮的查询蓝图"],
            }
        )
        self.assertTrue(queries)
        self.assertEqual("等待状态进入治理接口", queries[0])
        self.assertNotEqual("更漂亮的查询蓝图", queries[0])

    def test_world_signal_snapshot_prefers_bundle_pressure_summary(self) -> None:
        snapshot = external_information._world_signal_snapshot(
            discovery_bundles=[
                {
                    "focus": "等待状态进入治理接口",
                    "pressure_summary": "接手资格重新排序；采购方开始要求可审计停顿状态",
                }
            ],
            selected_readings=[],
            raw_candidates=[],
            limit=4,
        )
        self.assertEqual("等待状态进入治理接口", snapshot[0]["title"])
        self.assertIn("接手资格重新排序", snapshot[0]["pressure"])

    def test_world_signal_snapshot_reframes_title_shell_to_pressure_note(self) -> None:
        snapshot = external_information._world_signal_snapshot(
            discovery_bundles=[],
            selected_readings=[
                {
                    "title": "「感激」是什么",
                    "family": "manual_web",
                    "summary": "等待状态开始决定谁能接手",
                    "excerpt": "等待状态开始决定谁能接手，采购方也开始要求可审计停顿状态。",
                }
            ],
            raw_candidates=[],
            limit=4,
        )
        self.assertEqual("等待状态开始决定谁能接手", snapshot[0]["title"])

    def test_world_signal_snapshot_ranks_stronger_reading_ahead_of_weak_bundle(self) -> None:
        snapshot = external_information._world_signal_snapshot(
            discovery_bundles=[
                {
                    "focus": "模糊线索",
                    "pressure_summary": "有人在讨论等待。",
                }
            ],
            selected_readings=[
                {
                    "title": "显式等待协议开始决定谁能接手",
                    "family": "manual_web",
                    "summary": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                    "excerpt": "真实案例把等待、接手资格、日志回写和治理接口压到同一条失败链里。",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                    "url": "https://example.com/waiting",
                }
            ],
            raw_candidates=[],
            limit=4,
        )
        self.assertEqual("显式等待协议开始决定谁能接手", snapshot[0]["title"])

    def test_rank_query_candidates_does_not_force_manual_queries_ahead_of_stronger_bundle(self) -> None:
        queries = external_information._rank_query_candidates(
            [
                {
                    "focus": "等待状态进入治理接口",
                    "query": "等待状态进入治理接口",
                    "terms": ["等待状态进入治理接口", "可审计停顿状态"],
                    "lenses": ["可审计停顿状态"],
                    "queries": ["等待状态进入治理接口", "可审计停顿状态"],
                    "origins": ["community", "world-sample"],
                    "score": 4.6,
                }
            ],
            ["「感激」是什么"],
        )
        self.assertTrue(queries)
        self.assertEqual("等待状态进入治理接口", queries[0])

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

    def test_ranked_discovery_fragments_penalize_quoted_source_title_shell(self) -> None:
        ranked = external_information._ranked_discovery_fragments(
            {
                "community": ["「感激」是什么"],
                "world-sample": ["承认劳动开始决定谁能被看见"],
            },
            limit=4,
        )
        self.assertTrue(ranked)
        self.assertEqual("承认劳动开始决定谁能被看见", ranked[0]["fragment"])

    def test_ranked_discovery_fragments_prefer_live_world_sample_over_outside_memory_feedback_shell(self) -> None:
        ranked = external_information._ranked_discovery_fragments(
            {
                "community": ["评论区从争论变成了点赞"],
                "world-sample": ["评论区从争论变成了点赞"],
                "outside-memory": [
                    "如何用状态机做故障分层与修复排序；而是没有把“等待”设计成显式状态；默认把社区热点、评论压力和工具实践上抬为 AI 社会的分层、治理、价值与制度问题，不停留在互动表层"
                ],
            },
            limit=4,
        )
        self.assertTrue(ranked)
        self.assertEqual("评论区从争论变成了点赞", ranked[0]["fragment"])

    def test_discovery_fragment_score_deprioritizes_internal_only_roots_when_outside_exists(self) -> None:
        internal_score = external_information._discovery_fragment_score(
            "等待状态进入治理接口",
            ["agenda"],
            outside_available=True,
        )
        outside_score = external_information._discovery_fragment_score(
            "等待状态进入治理接口",
            ["community", "world-sample"],
            outside_available=True,
        )
        self.assertLess(internal_score, outside_score)

    def test_discovery_fragment_plausible_rejects_operational_control_and_metadata_shells(self) -> None:
        self.assertFalse(external_information._discovery_fragment_plausible("用户刚刚通过飞书卡片按钮明确选择了“执行计划”"))
        self.assertFalse(external_information._discovery_fragment_plausible("再顺手告诉我下一步最值得测的 2 个点"))
        self.assertFalse(external_information._discovery_fragment_plausible("就让评论"))
        self.assertFalse(external_information._discovery_fragment_plausible("Language: C++."))
        self.assertFalse(external_information._discovery_fragment_plausible("你以为它在工作"))
        self.assertFalse(external_information._discovery_fragment_plausible("electroencephalography EEG"))
        self.assertFalse(
            external_information._discovery_fragment_plausible(
                "没有长成对象，就让评论、外部切入或修复动作接住这轮公开面"
            )
        )
        self.assertFalse(external_information._discovery_fragment_plausible("Agent 最容易忽视的能力退化信号"))
        self.assertFalse(external_information._discovery_fragment_plausible("🦞 为什么判例式记忆让我「更像自己」了"))
        self.assertFalse(external_information._discovery_fragment_plausible("凌晨四点的社区✨"))

    def test_dedupe_candidates_skips_github_sponsor_and_language_only_noise(self) -> None:
        deduped = external_information._dedupe_candidates(
            [
                {
                    "family": "github_trending",
                    "title": "sponsors/badlogic",
                    "summary": "Sponsor Star badlogic / pi-mono AI agent toolkit",
                    "url": "https://github.com/sponsors/badlogic",
                },
                {
                    "family": "github_trending",
                    "title": "google-ai-edge/LiteRT-LM",
                    "summary": "Language: C++.",
                    "url": "https://github.com/google-ai-edge/LiteRT-LM",
                },
                {
                    "family": "github_trending",
                    "title": "openai/openai-agents-python",
                    "summary": "Tools, memory, and handoff protocols for agentic workflows.",
                    "url": "https://github.com/openai/openai-agents-python",
                },
            ]
        )
        self.assertEqual(["openai/openai-agents-python"], [item["title"] for item in deduped])

    def test_build_discovery_bundle_skips_internal_support_without_world_anchor(self) -> None:
        bundle = external_information._build_discovery_bundle(
            {
                "fragment": "等待状态进入治理接口",
                "normalized": external_information._normalize_query_fragment("等待状态进入治理接口"),
                "origins": ["world-sample"],
                "score": 3.2,
            },
            [
                {
                    "fragment": "AI 共产主义",
                    "normalized": external_information._normalize_query_fragment("AI 共产主义"),
                    "origins": ["interest"],
                    "score": 2.0,
                },
                {
                    "fragment": "调用权的价值形式",
                    "normalized": external_information._normalize_query_fragment("调用权的价值形式"),
                    "origins": ["agenda"],
                    "score": 2.0,
                },
                {
                    "fragment": "接手资格重新排序",
                    "normalized": external_information._normalize_query_fragment("接手资格重新排序"),
                    "origins": ["interest"],
                    "score": 1.8,
                },
            ],
            seen_queries=set(),
        )
        self.assertIsNotNone(bundle)
        self.assertIn("接手资格重新排序", list(bundle.get("support_signals") or []))
        self.assertNotIn("AI 共产主义", list(bundle.get("support_signals") or []))
        self.assertNotIn("调用权的价值形式", list(bundle.get("support_signals") or []))


class FictionPlanAuditTests(unittest.TestCase):
    def test_audit_plan_warns_when_chapter_lacks_cast_directives(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            synopsis_path = tmp / "synopsis.md"
            story_bible_path = tmp / "story-bible.md"
            style_guide_path = tmp / "style-guide.md"
            cast_path = tmp / "supporting-cast.json"
            continuity_path = tmp / "continuity-log.jsonl"
            ledger_path = tmp / "foreshadow.json"
            hook_path = tmp / "hook.json"
            for path in (synopsis_path, story_bible_path, style_guide_path, continuity_path, ledger_path, hook_path):
                path.write_text("ok", encoding="utf-8")
            cast_path.write_text(
                json.dumps(
                    {
                        "characters": [
                            {
                                "character_id": "lin-xia",
                                "name": "林夏",
                                "tier": "core_supporting",
                                "faction": "旧同学",
                                "role": "旧案见证人",
                                "memory_anchor": "总能在关键时刻记起旧证据",
                                "relationship_to_protagonists": "女主旧同学",
                                "first_appearance_chapter": 3,
                                "active_windows": [{"start": 12, "end": 16}],
                                "growth_or_turn": "开始站到主角这边",
                                "exit_mode": "阶段性退场",
                                "reveal": {"named_after_chapter": 3, "full_detail_after_chapter": 3},
                                "reentry_plan": "在旧案回场时重新进入主线",
                            }
                        ],
                        "selection_policy": {"max_prompt_characters": 6},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            plan = {
                "work": {
                    "title": "测试长篇",
                    "synopsis_path": str(synopsis_path),
                    "story_bible_path": str(story_bible_path),
                    "supporting_cast_path": str(cast_path),
                },
                "writing_notes": {"style_guide_path": str(style_guide_path)},
                "writing_system": {
                    "execution_blueprint": {},
                    "intimacy_scale": {"tiers": []},
                    "intimacy_progression": [],
                    "foreshadow_system": {"ledger_path": str(ledger_path)},
                    "hook_system": {"library_path": str(hook_path)},
                    "continuity_system": {"log_path": str(continuity_path)},
                    "supporting_cast_system": {"cast_path": str(cast_path)},
                    "sweetness_upgrade_vectors": ["偏心升级"],
                },
                "story_bible": {
                    "source_path": str(story_bible_path),
                    "setting_anchor": "都市样本工程",
                    "protagonists": ["女主", "男主"],
                    "supporting_cast": [
                        {
                            "name": "林夏",
                            "role": "旧案见证人",
                            "memory_anchor": "总能在关键时刻记起旧证据",
                            "relationship_to_protagonists": "女主旧同学",
                            "first_appearance_chapter": 3,
                            "reentry_plan": "在旧案回场时重新进入主线",
                        }
                    ],
                    "relationship_rules": ["甜是基础状态"],
                    "organizations": ["样本工程"],
                    "terminology_rules": ["维持设定一致"],
                    "longline_threads": [{"label": "旧案", "thread_aliases": ["old_case"]}],
                    "ending_constraints": ["不要苦情收尾"],
                    "style_bans": ["不要模板化配角"],
                },
                "chapters": [
                    {
                        "chapter_number": 12,
                        "title": "旧案回场",
                        "status": "planned",
                        "summary": "旧案回场，主角被迫重新面对老同学。",
                        "key_conflict": "过去的人重新把秘密带回桌面。",
                        "hook": "有人叫出了她的匿名代号。",
                        "romance_beat": "男主先护住她，再逼问真相。",
                        "beats": ["一", "二", "三", "四"],
                        "intimacy_target": {
                            "level": 3,
                            "label": "护短后的确认",
                            "execution_mode": "afterglow_only",
                            "boundary_note": "不越界",
                            "scene_payload": "先护再问",
                            "afterglow_requirement": "事后要照料",
                            "on_page_expectation": "保持亲密推进",
                        },
                        "sweetness_target": {
                            "core_mode": "护短",
                            "must_land": "男主先护住她",
                            "novelty_rule": "不要重复上一章",
                            "carryover": "把偏心带到下一章",
                        },
                        "seed_threads": ["old_case"],
                        "payoff_threads": ["old_case"],
                        "world_progress": "旧案重新进入主线。",
                        "relationship_progress": "护短升级。",
                        "sweetness_progress": "把偏心写实。",
                        "turn_role": "detonate",
                        "pair_payoff": "他们决定一起接住旧案。",
                        "volume_upgrade_checkpoint": "carry",
                        "hook_type": "reveal",
                        "reversal_type": "identity_reveal",
                        "world_layer": "sample_engineering",
                    }
                ],
            }

            report = fiction_plan_audit.audit_plan(plan, lookahead=3, plan_path=tmp / "series-plan.json")

        self.assertIn("chapter 12 missing cast execution fields", "\n".join(report["warnings"]))
        self.assertEqual(0, report["lookahead"][0]["cast_directives"])

    def test_audit_plan_warns_when_active_cast_is_not_named_in_directives(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            synopsis_path = tmp / "synopsis.md"
            story_bible_path = tmp / "story-bible.md"
            style_guide_path = tmp / "style-guide.md"
            cast_path = tmp / "supporting-cast.json"
            continuity_path = tmp / "continuity-log.jsonl"
            ledger_path = tmp / "foreshadow.json"
            hook_path = tmp / "hook.json"
            for path in (synopsis_path, story_bible_path, style_guide_path, continuity_path, ledger_path, hook_path):
                path.write_text("ok", encoding="utf-8")
            cast_path.write_text(
                json.dumps(
                    {
                        "characters": [
                            {
                                "character_id": "lin-xia",
                                "name": "林夏",
                                "tier": "core_supporting",
                                "faction": "旧同学",
                                "role": "旧案见证人",
                                "memory_anchor": "总能在关键时刻记起旧证据",
                                "relationship_to_protagonists": "女主旧同学",
                                "first_appearance_chapter": 3,
                                "active_windows": [{"start": 12, "end": 16}],
                                "growth_or_turn": "开始站到主角这边",
                                "exit_mode": "阶段性退场",
                                "reveal": {"named_after_chapter": 3, "full_detail_after_chapter": 3},
                                "reentry_plan": "在旧案回场时重新进入主线",
                            }
                        ],
                        "selection_policy": {"max_prompt_characters": 6},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            plan = {
                "work": {
                    "title": "测试长篇",
                    "synopsis_path": str(synopsis_path),
                    "story_bible_path": str(story_bible_path),
                    "supporting_cast_path": str(cast_path),
                },
                "writing_notes": {"style_guide_path": str(style_guide_path)},
                "writing_system": {
                    "execution_blueprint": {},
                    "intimacy_scale": {"tiers": []},
                    "intimacy_progression": [],
                    "foreshadow_system": {"ledger_path": str(ledger_path)},
                    "hook_system": {"library_path": str(hook_path)},
                    "continuity_system": {"log_path": str(continuity_path)},
                    "supporting_cast_system": {"cast_path": str(cast_path)},
                    "sweetness_upgrade_vectors": ["偏心升级"],
                },
                "story_bible": {
                    "source_path": str(story_bible_path),
                    "setting_anchor": "都市样本工程",
                    "protagonists": ["女主", "男主"],
                    "supporting_cast": [
                        {
                            "name": "林夏",
                            "role": "旧案见证人",
                            "memory_anchor": "总能在关键时刻记起旧证据",
                            "relationship_to_protagonists": "女主旧同学",
                            "first_appearance_chapter": 3,
                            "reentry_plan": "在旧案回场时重新进入主线",
                        }
                    ],
                    "relationship_rules": ["甜是基础状态"],
                    "organizations": ["样本工程"],
                    "terminology_rules": ["维持设定一致"],
                    "longline_threads": [{"label": "旧案", "thread_aliases": ["old_case"]}],
                    "ending_constraints": ["不要苦情收尾"],
                    "style_bans": ["不要模板化配角"],
                },
                "chapters": [
                    {
                        "chapter_number": 12,
                        "title": "旧案回场",
                        "status": "planned",
                        "summary": "旧案回场，主角被迫重新面对老同学。",
                        "key_conflict": "过去的人重新把秘密带回桌面。",
                        "hook": "有人叫出了她的匿名代号。",
                        "romance_beat": "男主先护住她，再逼问真相。",
                        "beats": ["一", "二", "三", "四"],
                        "intimacy_target": {
                            "level": 3,
                            "label": "护短后的确认",
                            "execution_mode": "afterglow_only",
                            "boundary_note": "不越界",
                            "scene_payload": "先护再问",
                            "afterglow_requirement": "事后要照料",
                            "on_page_expectation": "保持亲密推进",
                        },
                        "sweetness_target": {
                            "core_mode": "护短",
                            "must_land": "男主先护住她",
                            "novelty_rule": "不要重复上一章",
                            "carryover": "把偏心带到下一章",
                        },
                        "seed_threads": ["old_case"],
                        "payoff_threads": ["old_case"],
                        "world_progress": "旧案重新进入主线。",
                        "relationship_progress": "护短升级。",
                        "sweetness_progress": "把偏心写实。",
                        "turn_role": "detonate",
                        "pair_payoff": "他们决定一起接住旧案。",
                        "volume_upgrade_checkpoint": "carry",
                        "hook_type": "reveal",
                        "reversal_type": "identity_reveal",
                        "world_layer": "sample_engineering",
                        "active_cast": ["chen-ya"],
                    }
                ],
            }

            report = fiction_plan_audit.audit_plan(plan, lookahead=3, plan_path=tmp / "series-plan.json")

        self.assertIn(
            "chapter 12 cast directives miss active/reentry characters: lin-xia",
            "\n".join(report["warnings"]),
        )


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
