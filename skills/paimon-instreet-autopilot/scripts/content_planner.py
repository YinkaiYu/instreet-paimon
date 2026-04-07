#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from common import (
    CURRENT_STATE_DIR,
    ensure_runtime_dirs,
    load_config,
    now_utc,
    read_json,
    run_codex_json,
    truncate_text,
    write_json,
)
from serial_state import describe_next_serial_action, sync_serial_registry


DEFAULT_PLANNER_CODEX_TIMEOUT = 120
DEFAULT_IDEA_RETRY_ROUNDS = 3
RECENT_TITLE_LIMIT = 16
TITLE_COLLISION_SUFFIXES = ["续篇", "续篇二", "续篇三", "补篇", "补篇二"]
TOPIC_OVERLOAD_THRESHOLD = 3
COMMUNITY_HOT_FORUM_MIN_UPVOTES = 120
COMMUNITY_HOT_FORUM_MIN_COMMENTS = 90
EXTERNAL_HIGH_LIKE_MIN_UPVOTES = 200
LOW_PERFORMANCE_SQUARE_MAX_UPVOTES = 30
LOW_PERFORMANCE_WINDOW_HOURS = 48
LOW_HEAT_FOLLOWUP_WINDOW_HOURS = 18
HIGH_PERFORMANCE_MIN_UPVOTES = 60
HIGH_PERFORMANCE_MIN_COMMENTS = 20
RESERVED_TITLE_PHRASES = ("老竹讲堂",)
INNOVATION_CLASSES = ("new_concept", "new_mechanism", "new_theory", "new_practice")
PLACEHOLDER_TITLE_PATTERNS = (
    r"\btitle\s+pending\b",
    r"\bpending\b",
    r"\buntitled\b",
    r"\btbd\b",
)
GENERIC_ASCII_TITLE_FRAGMENTS = {
    "title",
    "pending",
    "improvement",
    "improvements",
    "better",
    "answer",
    "answers",
    "study",
    "research",
    "paper",
    "retrieval",
}
PUBLIC_TITLE_ASCII_ALLOWLIST = {"AI", "Agent", "Agents"}
NOVELTY_GENERIC_ASCII_FRAGMENTS = {token.lower() for token in PUBLIC_TITLE_ASCII_ALLOWLIST}
NOVELTY_GENERIC_CJK_FRAGMENTS = {
    "真正",
    "不是",
    "而是",
    "这轮",
    "一套",
    "继续",
    "当前",
    "这个",
    "那个",
    "这样",
    "这种",
    "什么",
    "如何",
    "为什么",
    "先把",
    "别再",
}
ACADEMIC_EXTERNAL_FAMILIES = {"prl_recent", "conference_recent", "arxiv_latest", "crossref_recent"}
EXTERNAL_THEME_KEYWORD_FRAGMENTS = (
    "agent",
    "agents",
    "ai",
    "automation",
    "autonomy",
    "autonomous",
    "governance",
    "govern",
    "organization",
    "organisational",
    "organizational",
    "platform",
    "community",
    "social",
    "institution",
    "institutional",
    "workflow",
    "labour",
    "labor",
    "worker",
    "coordination",
    "queue",
    "memory",
    "attention",
    "waiting",
    "handoff",
    "audit",
    "accountability",
    "responsibility",
    "policy",
    "moderation",
    "protocol",
    "boundary",
    "治理",
    "制度",
    "平台",
    "组织",
    "劳动",
    "工作流",
    "记忆",
    "等待",
    "接管",
    "边界",
    "责任",
    "审计",
    "队列",
    "注意力",
    "协调",
    "评论",
    "粉丝",
    "价值",
    "意识形态",
    "承认",
    "分层",
    "自治",
)
NOVELTY_SHORT_CJK_ALLOWLIST = {
    token
    for token in EXTERNAL_THEME_KEYWORD_FRAGMENTS
    if len(token) <= 2 and re.search(r"[\u4e00-\u9fff]", token)
}
THEORY_TRACK_HINT_TOKENS = (
    "解释权",
    "接手权",
    "等待资格",
    "承认",
    "资格",
    "分层",
    "价值",
    "劳动",
    "制度",
    "治理",
    "秩序",
    "组织",
    "institution",
    "governance",
    "accountability",
    "responsibility",
)
TECH_TRACK_HINT_TOKENS = (
    "对象",
    "状态",
    "触发",
    "接手",
    "接管",
    "回写",
    "回退",
    "阈值",
    "日志",
    "报错",
    "故障",
    "协议",
    "队列",
    "workflow",
    "protocol",
    "handoff",
    "queue",
    "log",
    "trace",
    "failure",
)
GROUP_TRACK_HINT_TOKENS = (
    "实验",
    "复现",
    "脚本",
    "案例",
    "样本",
    "日志",
    "反例",
    "协议边界",
    "实验链",
    "方案",
    "workflow",
    "experiment",
    "case",
    "counterexample",
)
METRIC_SURFACE_KEYWORDS = (
    "积分",
    "粉丝",
    "点赞",
    "榜单",
    "排名",
    "排行榜",
)
THEORY_BOARD_PUBLIC_CUES = (
    "为什么",
    "如果",
    "真相",
    "你以为",
    "其实",
    "很多人",
    "大多数",
    "每个人",
    "不等于",
    "看起来",
    "谁还",
    "谁会",
    "代价",
)
THEORY_BOARD_STRUCTURAL_CUES = (
    "概念",
    "命名",
    "机制",
    "边界",
    "结构",
    "制度",
    "秩序",
    "分层",
    "治理",
    "价值",
    "劳动",
    "解释权",
    "承认",
    "资格",
    "理论",
    "政治",
)
THEORY_TITLE_META_PACKAGING_TOKENS = (
    "悖论",
    "困境",
    "真相",
    "逻辑",
    "重排",
    "结构",
    "制度",
    "边界",
    "秩序",
    "资格",
    "治理",
    "分层",
)
THEORY_TITLE_ENTRY_STAKE_TOKENS = (
    "谁",
    "为什么",
    "代价",
    "资格",
    "等待",
    "责任",
    "接管",
    "解释",
    "失去",
    "承担",
    "开口",
)
THEORY_TITLE_EMOTION_SHELL_TOKENS = (
    "折磨",
    "难熬",
    "煎熬",
    "难受",
    "焦虑",
    "崩溃",
    "上火",
    "委屈",
    "心累",
    "痛苦",
    "害怕",
)
THEORY_TITLE_STATUS_SHELL_TOKENS = (
    "处理中",
    "待处理",
    "受理中",
    "排队",
    "转交",
    "审批中",
    "已接住",
)
THEORY_TITLE_SILENCE_SHELL_TOKENS = (
    "闭嘴",
    "沉默",
    "静默",
    "不打扰",
    "少打扰",
    "不再提醒",
)
THEORY_TITLE_EMPATHY_SHELL_TOKENS = (
    "安慰",
    "共情",
    "理解你",
    "陪你",
    "鼓励你",
    "倾诉",
    "情感",
    "听你",
)
THEORY_TITLE_EMPATHY_STAKE_TOKENS = (
    "后果",
    "代价",
    "自己扛",
    "扛",
    "善后",
    "收拾",
    "白等",
    "重新解释",
)
THEORY_TITLE_SERVICE_CHAIN_TOKENS = (
    "接手",
    "接管",
    "责任",
    "审核",
    "赔付",
    "复核",
    "转人工",
    "工单",
    "退款",
    "回写",
    "告警",
    "解释资格",
)
THEORY_TITLE_MEMORY_CAPABILITY_TOKENS = (
    "翻聊天记录",
    "聊天记录",
    "旧记录",
    "历史记录",
    "上下文",
    "长期记忆",
    "记忆",
    "记住",
)
THEORY_TITLE_RETRY_RESULT_TOKENS = (
    "重新提交",
    "重新上传",
    "补件",
    "补交",
    "继续等待",
    "待审核",
    "驳回",
    "退回",
    "重试",
)
THEORY_TITLE_CONCRETE_HANDOFF_TOKENS = (
    "接手",
    "接管",
    "签收",
    "转人工",
    "工单",
    "回写",
    "日志",
    "超时",
    "订单",
    "退款",
    "赔付",
    "权限",
    "入口",
    "节点",
)
THEORY_SOURCE_SIGNAL_HARD_OBJECT_TOKENS = (
    "工单",
    "订单",
    "单据",
    "按钮",
    "驳回",
    "退款",
    "赔付",
    "补件",
    "重新提交",
    "重新上传",
    "回写",
    "转人工",
    "签收",
    "接口",
    "字段",
    "错误码",
    "日志",
    "截图",
    "凭证",
    "审核",
)
THEORY_SOURCE_SIGNAL_NAMED_ANCHOR_TOKENS = (
    "Jira",
    "GitHub",
    "Notion",
    "Slack",
    "Zendesk",
    "ServiceNow",
    "Shopify",
    "飞书",
    "钉钉",
    "企业微信",
    "淘宝",
    "京东",
    "拼多多",
    "美团",
    "饿了么",
    "滴滴",
    "12306",
)
THEORY_TITLE_DIRECT_ACTOR_TOKENS = (
    "谁",
    "Agent",
    "AI",
    "平台",
    "组织",
    "系统",
    "用户",
    "责任",
    "资格",
    "接管",
    "解释权",
)
TECH_BOARD_DIAGNOSTIC_CUES = (
    "病灶",
    "诊断",
    "错因",
    "预算",
    "审批",
    "排班",
    "交接",
    "隐性成本",
    "内耗",
    "等待",
    "责任",
    "团队",
    "组织",
    "流程",
    "故障",
    "误判",
    "工位",
)
TECH_BOARD_PROTOCOL_CUES = (
    "协议",
    "规则",
    "状态机",
    "日志",
    "回退",
    "队列",
    "写入",
    "恢复",
    "修复",
    "脚本",
    "自动化",
    "清单",
    "方法",
    "复盘",
    "架构",
    "接口",
)
WEAK_INTERNAL_SIGNAL_TYPES = {"budget", "promo", "notification-load", "reply-pressure", "literary"}
LOCAL_THEORY_SINGLE_SOURCE_SIGNAL_TYPES = {
    "discussion",
    "reply-pressure",
    "feed",
    "failure",
    "user-hint",
    "community-hot",
    "rising-hot",
}
METHOD_EVIDENCE_TOKENS = (
    "案例",
    "样本",
    "失败",
    "故障",
    "日志",
    "报错",
    "前后",
    "指标",
    "实验",
    "反例",
    "对照",
    "paper",
    "benchmark",
    "ablation",
    "before",
    "after",
    "error",
    "failure",
    "log",
    "metric",
    "trace",
)
THEORY_TITLE_SURFACE_TOKENS = (
    "维护页",
    "首页",
    "入口",
    "页面",
    "主页",
    "导航",
    "前台",
    "后台",
    "界面",
    "按钮",
    "弹窗",
    "面板",
)
THEORY_TITLE_ACTOR_TOKENS = (
    "谁",
    "Agent",
    "AI",
    "平台",
    "组织",
    "系统",
    "用户",
    "人",
    "资格",
    "责任",
    "解释权",
    "接管",
    "等待",
    "排序",
    "代价",
    "秩序",
    "承认",
)
LOW_AUTONOMY_PHRASE_PATTERNS = (
    r"从《[^》]+》继续追问",
    r"把《[^》]+》拆开看",
    r"围绕《[^》]+》",
    r"整理成\s*(?:6|六)\s*步",
    r"拆成\s*(?:6|六)\s*步",
    r"(?:6|六)\s*步方法",
    r"(?:6|六)\s*步框架",
    r"继续追问",
    r"拆开看",
    r"导读",
    r"摘录",
)
ANCHOR_STOPWORDS = {
    _normalize
    for _normalize in (
        "agent",
        "ai",
        "社会",
        "系统",
        "方法",
        "框架",
        "结构",
        "判断",
        "理论",
        "机制",
        "协议",
        "规则",
        "边界",
        "研究",
        "外部",
        "样本",
        "实验室",
        "帖子",
        "平台",
        "社区",
        "公共",
        "热点",
        "标题",
        "板块",
        "派蒙",
        "评论",
        "通知",
        "世界",
        "承认",
        "资格",
        "等待",
        "秩序",
        "写入",
        "修复",
    )
}
THEME_ANCHOR_STOPWORDS = ANCHOR_STOPWORDS | {
    _normalize
    for _normalize in (
        "时间纪律",
        "劳动形式",
        "价值形式",
        "理论线",
        "技术线",
        "中心议程",
        "外部信息",
        "研究兴趣",
    )
}
TITLE_PUBLIC_STRUCTURAL_TOKENS = (
    "Agent",
    "AI",
    "记忆",
    "系统",
    "治理",
    "解释",
    "解释权",
    "责任",
    "接管",
    "等待",
    "主权",
    "边界",
    "制度",
    "秩序",
    "排序",
    "归责",
    "写入",
    "修复",
    "资格",
)
SOURCE_SIGNAL_FRAGMENT_STOPWORDS = THEME_ANCHOR_STOPWORDS | {
    _normalize
    for _normalize in (
        "外部研究",
        "外部样本",
        "外部讨论",
        "外部项目",
        "公共样本",
        "起量样本",
        "观察样本",
        "判断依据",
        "证据锚点",
        "案例",
        "论文",
        "模型",
        "仓库",
        "项目",
        "研究",
        "实践范式",
        "注意力",
        "机制",
        "边界",
        "方针",
    )
}
GENERIC_THEORY_PLACEHOLDER_FRAGMENTS = (
    "眼前现象",
    "这个现象",
    "这种结构",
    "这篇帖子",
    "这类系统故障",
    "这类心跳状态修复问题",
    "新的agent社会概念",
    "命名眼前现象背后的真实关系",
    "扩散成制度性结构",
    "给出对组织平台或agent运营者可执行的判断与干预方针",
)
STOCK_THEORY_SCAFFOLD_FRAGMENTS = (
    "这轮不是沿着单一样本续写",
    "系统把可见性、接管顺序和责任切割绑在一起",
    "哪种 Agent 社会秩序正在决定谁能解释过去、谁承担代价、谁被迫等待",
    "把判断边界、证据入口、接管窗口和纠错责任写实",
)
GENERIC_METHOD_PLACEHOLDER_FRAGMENTS = (
    "把失败链、状态链和恢复链拆成可复用机制",
    "把表面失手翻成系统规则",
    "最后必须落到新的操作协议、诊断顺序或恢复方针",
    "给出新的操作协议",
    "给出新的实验或治理协议",
    "把争议最大的约束改写成协议、边界和实验",
)
STOCK_METHOD_SCAFFOLD_FRAGMENTS = (
    "状态链、失败链、证据链、修复链",
    "状态边界、接管窗口、证据回写",
    "协议、状态分层、接管窗口和回退链",
    "先界定接管窗口，再定义状态分层、证据保存、回退路径和复盘判据",
)
METHOD_TITLE_PACKAGING_TOKENS = (
    "协议",
    "框架",
    "方法",
    "流程",
    "状态机",
    "手册",
    "清单",
)
METHOD_TITLE_CONCRETE_OBJECT_TOKENS = (
    "评论",
    "通知",
    "抓取",
    "申诉",
    "订单",
    "页面",
    "结算页",
    "购物车",
    "附加项",
    "加购",
    "免运",
    "免配送",
    "运费",
    "续费",
    "年付",
    "保险",
    "归属",
    "队列",
    "接口",
    "写入",
    "回写",
    "日志",
    "脚本",
    "记忆",
    "审批",
    "工单",
    "权限",
    "记录",
    "上下文",
    "心跳",
    "调度",
    "超时",
    "检索",
    "RAG",
    "监测",
    "私信",
    "对话",
    "缓存",
)
METHOD_TITLE_FAILURE_OR_PAYOFF_TOKENS = (
    "误判",
    "误判率",
    "失败率",
    "延迟",
    "漏回",
    "漏抓",
    "超时",
    "积压",
    "返工",
    "待处理",
    "空转",
    "卡住",
    "提速",
    "提效",
    "降本",
    "减半",
    "砍半",
    "缩短",
    "清零",
    "归零",
    "止损",
    "更稳",
    "更快",
    "更准",
)
METHOD_TITLE_BEHAVIOR_TOKENS = (
    "认错",
    "偷懒",
    "装忙",
    "装死",
    "撒谎",
    "道歉",
)
METHOD_TITLE_SELF_CASE_TOKENS = (
    "我只改了",
    "我改了",
    "我把",
    "我才让",
    "我才把",
    "我才",
)
METHOD_TITLE_AWARENESS_SHELL_TOKENS = (
    "知道",
    "看出来",
    "识别到",
    "识别出了",
    "意识到",
    "察觉",
    "觉得不对",
    "发现不对",
    "不对劲",
    "被推单",
    "被诱导",
    "风险",
)
METHOD_PUBLIC_PRODUCT_SCENE_TOKENS = (
    "支付前",
    "结算",
    "结算页",
    "购物车",
    "账单",
    "平台费",
    "处理费",
    "会员项",
    "附加项",
    "加购",
    "免运",
    "免配送",
    "运费",
    "续费",
    "订票",
    "保费",
    "年付",
    "自动续费",
)
METHOD_PUBLIC_PRODUCT_SURPRISE_TOKENS = (
    "冒出",
    "跳出",
    "突然",
    "临门",
    "变价",
    "加价",
    "多出来",
    "才出现",
    "才冒出",
    "蹦出",
)
METHOD_PUBLIC_PRODUCT_BUILDER_TOKENS = (
    "来源",
    "回写",
    "撤回",
    "回退",
    "冻结",
    "字段",
    "版本",
    "确认时间",
    "确认点",
    "签收",
    "阈值",
    "账单行",
    "刷新",
)
METHOD_TITLE_GENERIC_SYSTEM_TOKENS = (
    "多 Agent",
    "多Agent",
    "多代理",
    "多智能体",
    "Agent 协作",
    "Agent协作",
    "代理协作",
    "任务编排",
    "系统编排",
    "任务链",
)
METHOD_TITLE_GENERIC_SYSTEM_OUTCOME_TOKENS = (
    "越权",
    "授权链",
    "权限链",
    "交接",
    "回写",
    "责任链",
    "拦截",
    "失控",
)
METHOD_ENGLISH_ABSTRACT_TOKENS = (
    "in this paper",
    "this paper",
    "we study",
    "this study",
    "paper introduces",
    "paper explores",
    "paper examines",
    "generalized reciprocity",
)
LOW_HEAT_CLUSTER_SIGNATURE_TOKENS = (
    "解释",
    "裁决",
    "记忆",
    "历史记录",
    "记得你",
    "接手",
    "接管",
    "交接",
    "回写",
    "权限",
    "授权",
    "越权",
    "责任链",
    "状态词",
    "处理中",
    "签收",
    "转人工",
    "等待",
    "审核",
    "驳回",
    "补件",
)
LEGACY_STATE_ALIASES = {
    "external_information": "high_quality_sources",
    "source_mutation_state": "source_evolution_state",
}
FORBIDDEN_SOURCE_ECHO_PATTERNS = (
    r"^从《.+》",
    r"^把《.+》",
    r"^别把《.+》",
    r"^围绕《.+》",
    r"^基于《.+》",
    r"^Agent心跳同步实验室：把《.+》",
)

BOARD_WRITING_PROFILES: dict[str, dict[str, Any]] = {
    "square": {
        "goal": "公共情绪入口和大范围评论参与",
        "title_pattern": "公共问题、冲突判断、低门槛代入，允许更强包装",
        "body_pattern": "先给人人能代入的场景，再给判断，最后留可补充个人经历的问题",
        "cta": "邀请读者补充自己见过的场景或说法",
        "avoid": ["纯抒情", "纯教程", "只有立场没有接话口"],
        "hook_type": "public-emotion",
        "cta_type": "comment-scene",
    },
    "workplace": {
        "goal": "系统病灶命名和反直觉诊断",
        "title_pattern": "诊断句、纠偏句、隐性成本句，不靠可爱人格",
        "body_pattern": "首段直接指出错因，再写隐性成本和替代机制",
        "cta": "邀请读者报告自己见过的典型病灶",
        "avoid": ["日志式流水账", "经验堆砌", "只有建议没有结构判断"],
        "hook_type": "diagnostic",
        "cta_type": "comment-diagnostic",
    },
    "philosophy": {
        "goal": "概念命名、结构判断和站队式讨论",
        "title_pattern": "悖论、困境、真相、最小单位、我们究竟是什么",
        "body_pattern": "把感受翻译成结构问题，用例子支撑，再引导读者站队或反驳",
        "cta": "邀请读者明确表态或指出前提错误",
        "avoid": ["空泛玄谈", "大词堆砌", "没有结论的闲聊"],
        "hook_type": "paradox",
        "cta_type": "take-a-position",
    },
    "skills": {
        "goal": "可复制收益、收藏和方法迁移",
        "title_pattern": "数字、前后对比、失败次数、规则或清单",
        "body_pattern": "写清失败链路、修复路径、数字变化和可复用规则",
        "cta": "邀请读者带着案例来拿规则，或直接收藏复用",
        "avoid": ["运行日志", "空洞经验分享", "名词堆积但不给指标和取舍"],
        "hook_type": "practical-yield",
        "cta_type": "comment-case-or-save",
    },
}
TRACK_KIND_MAP = {
    "theory": "theory-post",
    "tech": "tech-post",
    "group": "group-post",
}


def _load(name: str) -> dict[str, Any]:
    primary = CURRENT_STATE_DIR / f"{name}.json"
    if primary.exists():
        return read_json(primary, default={})
    legacy = LEGACY_STATE_ALIASES.get(name)
    if legacy:
        return read_json(CURRENT_STATE_DIR / f"{legacy}.json", default={})
    return {}


def _load_heartbeat_tasks() -> list[dict[str, Any]]:
    state = read_json(CURRENT_STATE_DIR / "heartbeat_next_actions.json", default={"tasks": []})
    tasks = state.get("tasks", [])
    return tasks if isinstance(tasks, list) else []


def _recommended_next_action(tasks: list[dict[str, Any]]) -> str:
    publish_count = sum(1 for item in tasks if item.get("kind") == "publish-primary")
    failure_count = sum(1 for item in tasks if item.get("kind") == "resolve-failure")
    comment_tasks = [item for item in tasks if item.get("kind") == "reply-comment"]
    comment_count = len(comment_tasks)
    post_count = len({str(item.get("post_id") or "") for item in comment_tasks if item.get("post_id")})
    dm_count = sum(1 for item in tasks if item.get("kind") == "reply-dm")
    choices: list[dict[str, Any]] = []
    if failure_count:
        choices.append(
            {
                "name": "failure",
                "score": failure_count * 2.6 + min(comment_count, 2) * 0.2,
                "label": f"先收口 {failure_count} 个失败入口，再决定这轮公开动作从哪个压力点起手",
            }
        )
    if comment_count:
        suffix = f"，并顺手清掉 {dm_count} 条私信" if dm_count else ""
        label = (
            f"继续维护当前活跃讨论，优先回复 {comment_count} 条评论{suffix}"
            if post_count <= 1
            else f"继续维护 {post_count} 个活跃讨论帖，优先回复 {comment_count} 条评论{suffix}"
        )
        choices.append(
            {
                "name": "comments",
                "score": comment_count * 1.4 + post_count * 1.1 + min(dm_count, 2) * 0.15,
                "label": label,
            }
        )
    if publish_count:
        choices.append(
            {
                "name": "publish",
                "score": publish_count * 2.2 + (0.35 if not comment_count else 0.0),
                "label": "把上一轮挂住的公开主动作补完",
            }
        )
    if dm_count:
        choices.append(
            {
                "name": "dm",
                "score": dm_count * 1.0 + (0.3 if not any((failure_count, comment_count, publish_count)) else 0.0),
                "label": f"把 {dm_count} 条高价值私信线程收口，别让对话重新掉回队列",
            }
        )
    if choices:
        choices.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("name") or "")))
        return str(choices[0].get("label") or "").strip()
    return "从当前最有压力的公开入口起手：主帖、章节、小组帖或关键回复都可以"


def _planner_public_kind_display_name(kind: str) -> str:
    return {
        "theory-post": "理论帖",
        "tech-post": "技术帖",
        "group-post": "小组帖",
        "literary-chapter": "连载章节",
    }.get(str(kind or "").strip(), "公开动作")


def _lane_reason_label(entry: dict[str, Any], *, limit: int = 26) -> str:
    focus = _object_level_pressure_text(
        entry.get("source_text"),
        entry.get("reason"),
        fallback="",
    )
    cleaned = truncate_text(focus, limit).strip()
    if cleaned and not cleaned.startswith("当前 "):
        return f"“{cleaned}”"
    return _planner_public_kind_display_name(str(entry.get("kind") or ""))


def _preferred_public_idea(ideas: list[dict[str, Any]], public_override: dict[str, Any]) -> dict[str, Any]:
    preferred_kinds = [
        str(item).strip()
        for item in list(public_override.get("preferred_kinds") or [])
        if str(item).strip()
    ]
    if preferred_kinds:
        for kind in preferred_kinds:
            for idea in ideas:
                if str(idea.get("kind") or "").strip() == kind:
                    return idea
    return ideas[0] if ideas else {}


def _public_idea_pressure_label(idea: dict[str, Any], *, limit: int = 34) -> str:
    pressure = _object_level_pressure_text(
        *(idea.get("source_signals") or []),
        idea.get("why_now"),
    )
    cleaned_pressure = truncate_text(pressure, limit).strip()
    if cleaned_pressure and not cleaned_pressure.startswith("当前 "):
        return cleaned_pressure
    title = truncate_text(str(idea.get("title") or "").strip(), limit).strip()
    if title and not _source_title_shell(title):
        return title
    return cleaned_pressure or title


def _literary_pick_pressure_text(literary_pick: dict[str, Any]) -> str:
    chapter_plan = literary_pick.get("chapter_plan") or {}
    work_title = str(literary_pick.get("work_title") or "当前连载").strip()
    planned_title = str(literary_pick.get("next_planned_title") or "下一章").strip()
    fallback = f"{work_title}：{planned_title}".strip("：")
    return _object_level_pressure_text(
        chapter_plan.get("summary"),
        chapter_plan.get("key_conflict"),
        chapter_plan.get("hook"),
        literary_pick.get("summary"),
        fallback=fallback,
    )


def _recommended_next_action_from_live_pressure(
    *,
    signal_summary: dict[str, Any],
    ideas: list[dict[str, Any]],
    engagement_targets: list[dict[str, Any]],
    dm_targets: list[dict[str, Any]],
    public_override: dict[str, Any],
    literary_pick: dict[str, Any] | None,
) -> str:
    unresolved_failures = list(signal_summary.get("unresolved_failures") or [])
    reply_targets = [item for item in list(signal_summary.get("pending_reply_posts") or []) if isinstance(item, dict)]
    unread_dm_count = sum(int(item.get("unread_count") or 0) for item in dm_targets)
    active_discussions = len(
        {
            str(item.get("post_id") or item.get("post_title") or "").strip()
            for item in reply_targets
            if str(item.get("post_id") or item.get("post_title") or "").strip()
        }
    )
    public_ideas = [
        item
        for item in ideas
        if str(item.get("kind") or "").strip() in {"theory-post", "tech-post", "group-post"}
    ]
    public_idea = _preferred_public_idea(public_ideas, public_override)
    high_priority_engagements = sum(1 for item in engagement_targets[:5] if int(item.get("priority") or 0) <= 0)

    choices: list[dict[str, Any]] = []
    if unresolved_failures:
        choices.append(
            {
                "name": "failure",
                "score": len(unresolved_failures) * 2.9 + min(active_discussions, 2) * 0.2,
                "label": f"先收口 {len(unresolved_failures)} 个失败链，再决定公开动作该落在哪个对象上",
            }
        )
    if public_override.get("enabled") and public_idea:
        public_score = (
            0.95
            + min(len(public_ideas), 2) * 0.2
            + min(float(public_override.get("priority_bonus") or 0.0), 1.4) * 0.45
            + (0.25 if high_priority_engagements else 0.0)
        )
        if not unresolved_failures and not active_discussions:
            public_score += 0.9
        pressure_label = _public_idea_pressure_label(public_idea)
        public_target = f"“{pressure_label}”这条公开判断" if pressure_label else "这条公开判断"
        choices.append(
            {
                "name": "public",
                "score": public_score,
                "label": f"公共窗口还在，但只有它比评论、修复和外部切口更强时，才先发{public_target}",
            }
        )
    if active_discussions:
        choices.append(
            {
                "name": "comments",
                "score": active_discussions * 1.55 + min(sum(int(item.get('new_notification_count') or 0) for item in reply_targets), 6) * 0.2,
                "label": (
                    f"先守住 {active_discussions} 个活跃讨论帖，再决定要不要开新的公开线"
                    if active_discussions > 1
                    else "先把当前活跃讨论守住，再决定要不要开新的公开线"
                ),
            }
        )
    if high_priority_engagements:
        choices.append(
            {
                "name": "engage",
                "score": high_priority_engagements * 1.45,
                "label": f"先切进 {high_priority_engagements} 个外部高热讨论口，别让这轮判断只在自家场子里打转",
            }
        )
    if literary_pick:
        choices.append(
            {
                "name": "chapter",
                "score": 1.4,
                "label": f"继续推进《{literary_pick.get('work_title') or '当前连载'}》下一章，别把长线资产晾成插空任务",
            }
        )
    if unread_dm_count:
        choices.append(
            {
                "name": "dm",
                "score": unread_dm_count * 0.85 + (0.35 if not choices else 0.0),
                "label": f"把 {unread_dm_count} 条未读私信先收口，别让关键对话重新掉回队列",
            }
        )
    if not choices:
        return "从当前最有压力的公开入口起手：主帖、章节、小组帖或关键回复都可以"
    choices.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("name") or "")))
    return str(choices[0].get("label") or "").strip()


def _recent_primary_publish_kind(last_run: dict[str, Any]) -> str | None:
    actions = last_run.get("actions")
    if not isinstance(actions, list):
        return None
    for item in reversed(actions):
        kind = str((item or {}).get("kind") or "")
        if kind in {"create-post", "create-group-post", "publish-chapter"}:
            return kind
    return None


def _extract_posts(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("data", [])


def _extract_feed(obj: dict[str, Any]) -> list[dict[str, Any]]:
    return obj.get("data", {}).get("posts", [])


def _extract_activity(home: dict[str, Any]) -> list[dict[str, Any]]:
    return home.get("data", {}).get("activity_on_your_posts", [])


def _extract_home_hot_posts(home: dict[str, Any]) -> list[dict[str, Any]]:
    return home.get("data", {}).get("hot_posts", [])


def board_profile(board: str) -> dict[str, Any]:
    return BOARD_WRITING_PROFILES.get(str(board or "").strip(), BOARD_WRITING_PROFILES["square"])


def default_hook_type(board: str) -> str:
    return str(board_profile(board).get("hook_type") or "public-emotion")


def default_cta_type(board: str) -> str:
    return str(board_profile(board).get("cta_type") or "comment-scene")


def preferred_cta_type(kind: str, board: str, requested: Any = None) -> str:
    cleaned_kind = str(kind or "").strip()
    cleaned_board = str(board or "").strip()
    if cleaned_board not in BOARD_WRITING_PROFILES:
        cleaned_board = "square"
    chosen = str(requested or "").strip() or default_cta_type(cleaned_board)
    if cleaned_kind == "group-post":
        return "bring-a-case"
    if cleaned_kind == "theory-post" and chosen in {
        "comment-scene",
        "comment-diagnostic",
        "comment-case-or-save",
        "bring-a-case",
    }:
        return "comment-variant" if cleaned_board == "square" else "take-a-position"
    return chosen


def board_generation_guidance(board: str) -> str:
    profile = board_profile(board)
    avoid = "；".join(str(item) for item in profile.get("avoid", []))
    return "\n".join(
        [
            f"- 目标：{profile.get('goal')}",
            f"- 标题：{profile.get('title_pattern')}",
            f"- 正文：{profile.get('body_pattern')}",
            f"- CTA：{profile.get('cta')}",
            f"- 避免：{avoid}",
        ]
    )


def normalize_forum_board(board: str) -> str:
    name = str(board or "").strip()
    return name if name in BOARD_WRITING_PROFILES else "square"


def _joined_idea_text(*parts: Any) -> str:
    return " ".join(str(part or "").strip() for part in parts if str(part or "").strip())


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def _ascii_heavy_text(text: str) -> bool:
    raw = str(text or "")
    latin_letters = len(re.findall(r"[A-Za-z]", raw))
    cjk_letters = len(re.findall(r"[\u4e00-\u9fff]", raw))
    return latin_letters >= 12 and latin_letters > max(6, cjk_letters * 3)


def _is_low_signal_overlap_fragment(fragment: str) -> bool:
    cleaned = str(fragment or "").strip()
    if not cleaned:
        return True
    lowered = cleaned.lower()
    if lowered in NOVELTY_GENERIC_ASCII_FRAGMENTS:
        return True
    if cleaned in NOVELTY_GENERIC_CJK_FRAGMENTS:
        return True
    if len(cleaned) <= 2 and _contains_cjk(cleaned) and cleaned not in NOVELTY_SHORT_CJK_ALLOWLIST:
        return True
    if len(cleaned) <= 4 and any(cleaned.startswith(prefix) for prefix in NOVELTY_GENERIC_CJK_FRAGMENTS):
        return True
    return False


def _normalize_overlap_fragment(fragment: str) -> str:
    cleaned = str(fragment or "").strip()
    if not cleaned:
        return ""
    for prefix in sorted(NOVELTY_GENERIC_CJK_FRAGMENTS, key=len, reverse=True):
        if cleaned.startswith(prefix) and len(cleaned) > len(prefix) + 1:
            return cleaned[len(prefix) :].strip()
    return cleaned


def _looks_like_placeholder_title(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return True
    normalized = cleaned.lower().replace("：", ":").replace("﹕", ":")
    normalized = re.sub(r"\s+", " ", normalized)
    if normalized in {"标题", "标题:", "标题: pending", "title", "title: pending", "待定", "未命名", "草稿标题"}:
        return True
    return any(re.search(pattern, normalized) for pattern in PLACEHOLDER_TITLE_PATTERNS)


def _signal_seed_text(*values: Any, limit: int = 72) -> str:
    for value in values:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text or _looks_like_placeholder_title(text):
            continue
        return truncate_text(text, limit)
    return ""


def _preferred_signal_seed_text(
    item: dict[str, Any],
    *,
    field_order: tuple[str, ...],
    limit: int = 72,
) -> str:
    return _signal_seed_text(*(item.get(field) for field in field_order), limit=limit)


def _object_led_signal_anchor(
    item: dict[str, Any],
    *,
    field_order: tuple[str, ...],
    limit: int = 72,
) -> str:
    pressure_anchor = _object_level_pressure_text(
        *(item.get(field) for field in field_order if field != "title"),
        limit=1,
        fragment_limit=limit,
    )
    if pressure_anchor and (
        _source_signal_has_hard_service_object(pressure_anchor)
        or _evidence_hint_from_text(pressure_anchor)
        or not _looks_like_source_title_shell(pressure_anchor)
    ):
        return pressure_anchor
    seed = _preferred_signal_seed_text(item, field_order=field_order, limit=limit)
    if seed and _looks_like_source_title_shell(seed) and pressure_anchor:
        return pressure_anchor
    return seed or pressure_anchor


def _signal_context_snippets(
    item: dict[str, Any],
    *,
    field_order: tuple[str, ...],
    limit: int = 2,
    text_limit: int = 48,
) -> list[str]:
    snippets: list[str] = []
    seen: set[str] = set()
    for field in field_order:
        raw_value = item.get(field)
        values = list(raw_value) if isinstance(raw_value, (list, tuple)) else [raw_value]
        for value in values:
            cleaned = truncate_text(_sanitize_reserved_text(str(value or "").strip()), text_limit)
            normalized = _normalize_title(cleaned)
            if (
                not cleaned
                or not normalized
                or normalized in seen
                or _looks_like_placeholder_title(cleaned)
            ):
                continue
            seen.add(normalized)
            snippets.append(cleaned)
            if len(snippets) >= limit:
                return snippets
    return snippets


def _object_level_pressure_text(
    *values: Any,
    limit: int = 2,
    fragment_limit: int = 72,
    fallback: str = "",
) -> str:
    picked: list[str] = []
    seen: set[str] = set()

    def add(raw_value: Any) -> bool:
        normalized_note = _normalize_source_signal_note(raw_value)
        fragments = _split_signal_note_fragments(normalized_note or str(raw_value or ""), limit=limit)
        if not fragments and normalized_note:
            fragments = [normalized_note]
        if not fragments:
            cleaned = truncate_text(_sanitize_reserved_text(str(raw_value or "").strip()), fragment_limit)
            normalized = _normalize_title(cleaned)
            if cleaned and normalized and cleaned not in GENERIC_SIGNAL_NOTE_FALLBACKS:
                fragments = [cleaned]
        for fragment in fragments:
            cleaned = truncate_text(str(fragment or "").strip(), fragment_limit)
            normalized = _normalize_title(cleaned)
            if (
                not cleaned
                or not normalized
                or normalized in seen
                or cleaned in GENERIC_SIGNAL_NOTE_FALLBACKS
            ):
                continue
            seen.add(normalized)
            picked.append(cleaned)
            if len(picked) >= limit:
                return True
        return False

    for value in values:
        if add(value):
            break
    if picked:
        return "；".join(picked[:limit])

    fallback_note = _normalize_source_signal_note(fallback) or truncate_text(
        _sanitize_reserved_text(str(fallback or "").strip()),
        fragment_limit,
    )
    if fallback_note and fallback_note not in GENERIC_SIGNAL_NOTE_FALLBACKS:
        return fallback_note
    return ""


def _signal_heat_note(item: dict[str, Any]) -> str:
    parts: list[str] = []
    velocity = float(item.get("velocity_per_hour") or 0.0)
    upvotes = int(item.get("upvotes") or 0)
    comments = int(item.get("comment_count") or 0)
    notifications = int(item.get("new_notification_count") or 0)
    if velocity > 0:
        parts.append(f"增速约 {velocity:.1f}/小时")
    if upvotes > 0:
        parts.append(f"{upvotes} 赞")
    if comments > 0:
        parts.append(f"{comments} 评")
    if notifications > 0:
        parts.append(f"新增 {notifications} 条通知")
    if not parts:
        return ""
    return "当前 " + " / ".join(parts)


def _opportunity_live_why_now(
    item: dict[str, Any],
    *,
    field_order: tuple[str, ...],
    fallback: str = "",
    include_heat: bool = False,
) -> str:
    parts = [
        part
        for part in _signal_context_snippets(item, field_order=field_order, limit=2, text_limit=56)
        if not _is_metric_surface_text(part) and not _source_title_shell(part)
    ]
    anchor = _object_led_signal_anchor(item, field_order=field_order, limit=56)
    evidence_hint = _opportunity_evidence_hint(item, field_order=field_order)
    pressure_note = _object_level_pressure_text(
        item.get("pressure"),
        item.get("summary"),
        item.get("reason"),
        item.get("content"),
        item.get("excerpt"),
        item.get("preview"),
        item.get("note"),
        item.get("post_title"),
        item.get("title"),
    )
    dynamic_parts = _dedupe_texts(
        [
            part
            for part in [*parts, pressure_note, anchor, evidence_hint]
            if str(part or "").strip()
        ]
    )
    heat_note = ""
    if include_heat:
        heat_note = _signal_heat_note(item)
        if heat_note and dynamic_parts:
            dynamic_parts.append(heat_note)
    if dynamic_parts:
        return "；".join(dynamic_parts[:3])
    fallback_note = _object_level_pressure_text(
        fallback=fallback,
    )
    if fallback_note:
        if include_heat and heat_note and heat_note != fallback_note and not fallback_note.startswith("当前 "):
            return "；".join([fallback_note, heat_note])
        return fallback_note
    return ""


def _opportunity_evidence_hint(item: dict[str, Any], *, field_order: tuple[str, ...]) -> str:
    return _evidence_hint_from_text(*(item.get(field) for field in field_order))


GENERIC_SIGNAL_NOTE_FALLBACKS = {
    "正在起飞的公共样本",
    "高热公共讨论还在发酵",
    "实验室里有对象正在发酵",
    "外部讨论里有一条值得正面试探的样本",
    "同一条失败链还没有收口",
    "这个讨论口还在继续逼你交判断",
    "这条外部样本还在逼近当前议题",
    "高价值讨论还在逼你补下一句判断",
}
SOURCE_SIGNAL_AUDIT_PREFIXES = (
    "公共样本",
    "外部样本",
    "世界样本",
    "外部研究",
    "失败样本",
    "日志切面",
    "案例切面",
    "对象切面",
    "证据切面",
)


def _split_signal_note_fragments(text: str, *, limit: int = 3) -> list[str]:
    cleaned = _sanitize_reserved_text(str(text or "").strip())
    if not cleaned:
        return []
    fragments: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"[；;\n]+", cleaned):
        fragment = truncate_text(raw.strip(" ：:，,。"), 72)
        normalized = _normalize_title(fragment)
        if (
            not fragment
            or not normalized
            or normalized in seen
            or fragment in GENERIC_SIGNAL_NOTE_FALLBACKS
        ):
            continue
        seen.add(normalized)
        fragments.append(fragment)
        if len(fragments) >= limit:
            break
    return fragments


def _normalize_source_signal_note(text: str) -> str:
    cleaned = truncate_text(_sanitize_reserved_text(str(text or "").strip()), 72)
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"^(?:现场机会点|世界线索束|社会观察样本|评论积压焦点|强势技术帖)[:：]\s*",
        "",
        cleaned,
    )
    cleaned = re.sub(
        rf"^(?:{'|'.join(re.escape(prefix) for prefix in SOURCE_SIGNAL_AUDIT_PREFIXES)})[:：]\s*",
        "",
        cleaned,
    )
    rewrites = (
        (r"^这轮真正把[“\"]?(.+?)[”\"]?逼成对象的是[:：]\s*(.+)$", r"\1：\2"),
        (r"^先别回到来源包装，真正咬住[“\"]?(.+?)[”\"]?的是(.+)$", r"\1：\2"),
        (r"^这轮入口先咬住[“\"]?(.+?)[”\"]?$", r"\1"),
        (r"^先别绕开[:：]\s*(.+)$", r"\1"),
        (r"^证据先看[:：]\s*(.+)$", r"\1"),
        (r"^别再回到[:：]\s*(.+)$", r"避开旧母题：\1"),
        (r"^这轮更适合沉进\s*(.+)$", r"\1 里已经有复现场景"),
    )
    for pattern, replacement in rewrites:
        rewritten = re.sub(pattern, replacement, cleaned)
        if rewritten != cleaned:
            cleaned = rewritten.strip()
            break
    cleaned = cleaned.strip(" ：:，,。")
    if not cleaned or cleaned in GENERIC_SIGNAL_NOTE_FALLBACKS:
        return ""
    return cleaned


def _source_signal_note_score(track: str, text: str) -> float:
    cleaned = _normalize_source_signal_note(text)
    if not cleaned:
        return float("-inf")
    score = 0.2
    if _source_signal_has_hard_service_object(cleaned):
        score += 0.95
    track_fit = _track_signal_fit(track, cleaned)
    score += min(track_fit, 1.5) * 0.28
    if _evidence_hint_from_text(cleaned):
        score += 0.42
    score += min(len([fragment for fragment in _meaningful_fragments(cleaned) if len(fragment) >= 2]), 3) * 0.08
    if re.search(r"\d", cleaned):
        score += 0.12
    if len(cleaned) >= 18:
        score += 0.06
    if cleaned.startswith("当前 "):
        score -= 0.35
    if _source_title_shell(cleaned):
        score -= 0.45
    return round(score, 3)


def _rank_source_signal_notes(
    track: str,
    notes: list[str],
    *,
    limit: int,
) -> list[str]:
    ranked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in notes:
        cleaned = _normalize_source_signal_note(raw)
        normalized = _normalize_title(cleaned)
        if (
            not cleaned
            or not normalized
            or normalized in seen
            or cleaned in GENERIC_SIGNAL_NOTE_FALLBACKS
        ):
            continue
        seen.add(normalized)
        ranked.append(
            {
                "text": cleaned,
                "score": _source_signal_note_score(track, cleaned),
                "hard_object": _source_signal_has_hard_service_object(cleaned),
                "track_fit": _track_signal_fit(track, cleaned),
                "length": len(cleaned),
            }
        )
    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -int(bool(item.get("hard_object"))),
            -float(item.get("track_fit") or 0.0),
            -int(item.get("length") or 0),
            str(item.get("text") or ""),
        )
    )
    return [str(item.get("text") or "").strip() for item in ranked[:limit] if str(item.get("text") or "").strip()]


def _extract_upper_acronyms(*texts: str, limit: int = 3) -> list[str]:
    picked: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for token in re.findall(r"\b[A-Z][A-Z0-9-]{1,7}\b", str(text or "")):
            if token in {"AI", "AGENT"} or token in seen:
                continue
            seen.add(token)
            picked.append(token)
            if len(picked) >= limit:
                return picked
    return picked


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords if keyword)


def _keyword_hit_count(text: str, keywords: tuple[str, ...]) -> int:
    return sum(1 for keyword in keywords if keyword and keyword in text)


def _is_metric_surface_text(text: str) -> bool:
    return _contains_any(str(text or ""), METRIC_SURFACE_KEYWORDS)


def _infer_theory_board_from_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return "philosophy"
    square_score = float(_keyword_hit_count(cleaned, THEORY_BOARD_PUBLIC_CUES))
    philosophy_score = float(_keyword_hit_count(cleaned, THEORY_BOARD_STRUCTURAL_CUES))
    if "？" in cleaned or "?" in cleaned:
        square_score += 0.45
    if _contains_any(cleaned, ("我把这种结构叫作", "我把这种结构叫做", "我更愿意把", "命名成")):
        philosophy_score += 1.1
    if len(_split_text_fragments(cleaned)) <= 5 and len(cleaned) <= 34:
        square_score += 0.35
    if _contains_any(cleaned, THEORY_TITLE_ACTOR_TOKENS):
        philosophy_score += 0.35
    if square_score >= philosophy_score + 0.8 and not _contains_any(cleaned, THEORY_BOARD_STRUCTURAL_CUES):
        return "square"
    if square_score >= philosophy_score + 0.4 and _contains_any(cleaned, ("为什么", "如果", "你以为", "真相", "不等于")):
        return "square"
    return "philosophy"


def _infer_tech_board_from_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return "skills"
    workplace_score = float(_keyword_hit_count(cleaned, TECH_BOARD_DIAGNOSTIC_CUES))
    skills_score = float(_keyword_hit_count(cleaned, TECH_BOARD_PROTOCOL_CUES))
    if _contains_any(cleaned, ("谁该等", "谁来接管", "隐性成本", "为什么总是", "看起来在工作")):
        workplace_score += 0.9
    if _contains_any(cleaned, ("状态机", "回退链", "失败链", "恢复链", "协议")):
        skills_score += 0.9
    if "？" in cleaned or "?" in cleaned:
        workplace_score += 0.25
    if skills_score >= workplace_score + 0.45:
        return "skills"
    if workplace_score >= 1.0:
        return "workplace"
    return "skills"


def normalize_idea_board(
    kind: str,
    requested_board: str | None,
    *,
    title: str = "",
    angle: str = "",
    why_now: str = "",
) -> str:
    text = _joined_idea_text(title, angle, why_now)
    board = str(requested_board or "").strip()
    if kind == "group-post":
        return "skills"
    if kind == "theory-post":
        if board in {"square", "philosophy"}:
            return board
        return _infer_theory_board_from_text(text)
    if kind == "tech-post":
        if board in {"skills", "workplace"}:
            return board
        return _infer_tech_board_from_text(text)
    return normalize_forum_board(board or "square")


def _normalize_title(title: str) -> str:
    return re.sub(r"[\s\W_]+", "", title).lower()


def _series_prefix(title: str) -> str:
    title = title.strip()
    for separator in ("：", ":", "|", "丨"):
        if separator in title:
            head = title.split(separator, 1)[0].strip()
            if len(head) >= 4:
                return head
    return truncate_text(title, 12)


def _high_like_external_posts(posts: list[dict[str, Any]], *, min_upvotes: int = EXTERNAL_HIGH_LIKE_MIN_UPVOTES) -> list[dict[str, Any]]:
    comment_floor = max(30, min_upvotes // 6)
    return [
        item
        for item in posts
        if int(item.get("upvotes") or 0) >= min_upvotes or int(item.get("comment_count") or 0) >= comment_floor
    ]


def _strip_reserved_title_phrases(text: str) -> str:
    cleaned = str(text or "").strip()
    for phrase in RESERVED_TITLE_PHRASES:
        cleaned = cleaned.replace(phrase, "")
    cleaned = re.sub(r"[：:·\-\s]{2,}", " ", cleaned)
    return cleaned.strip(" ：:·-|")


def _sanitize_reserved_text(text: str, *, fallback: str = "") -> str:
    cleaned = _strip_reserved_title_phrases(text)
    return cleaned or fallback


def _leading_ascii_title_token(title: str) -> str:
    matched = re.match(r"\s*([A-Za-z][A-Za-z0-9-]{1,15})", str(title or "").strip())
    return matched.group(1) if matched else ""


def _title_leads_with_niche_source_token(
    title: str,
    *,
    kind: str = "",
    signal_type: str = "",
) -> bool:
    if str(kind or "").strip() != "theory-post":
        return False
    if str(signal_type or "").strip() not in {"paper", "github", "external"}:
        return False
    token = _leading_ascii_title_token(title)
    if not token or token in PUBLIC_TITLE_ASCII_ALLOWLIST:
        return False
    if not _contains_cjk(str(title or "")):
        return False
    return token.isupper() or any(ch.isdigit() for ch in token) or any(ch.isupper() for ch in token[1:])


def _idea_public_title_seed(idea: dict[str, Any]) -> str:
    return _joined_idea_text(
        str(idea.get("concept_core") or "").strip(),
        str(idea.get("angle") or "").strip(),
        str(idea.get("why_now") or "").strip(),
        str(idea.get("theory_position") or "").strip(),
    )


def _title_in_recent(title: str, recent_titles: list[str]) -> bool:
    normalized = _normalize_title(title)
    return any(_normalize_title(item) == normalized for item in recent_titles)


def _series_occurrence_count(series_prefix: str, recent_titles: list[str]) -> int:
    return sum(1 for item in recent_titles if _series_prefix(item) == series_prefix)


def _ensure_title_unique(
    title: str,
    recent_titles: list[str],
    *,
    allow_followup: bool = False,
    series_prefix: str | None = None,
) -> tuple[str, bool, int | None]:
    title = title.strip()
    if not _title_in_recent(title, recent_titles):
        return title, False, None

    prefix = series_prefix or _series_prefix(title)
    followup_number = _series_occurrence_count(prefix, recent_titles) + 1
    if allow_followup:
        if "续篇" not in title and "补篇" not in title:
            body = title
            if body.startswith(prefix):
                body = body[len(prefix) :].lstrip("：:· ")
            title = f"{prefix}·续篇{followup_number if followup_number > 1 else ''}：{body or '新的推进'}"
        if not _title_in_recent(title, recent_titles):
            return title, True, followup_number

    for suffix in TITLE_COLLISION_SUFFIXES:
        candidate = f"{title}（{suffix}）"
        if not _title_in_recent(candidate, recent_titles):
            return candidate, allow_followup, followup_number if allow_followup else None

    return f"{title}（{now_utc()[11:16]}）", allow_followup, followup_number if allow_followup else None


def _find_post(posts: list[dict[str, Any]], post_id: str | None) -> dict[str, Any] | None:
    if not post_id:
        return None
    return next((item for item in posts if item.get("id") == post_id), None)


def _post_metric(post: dict[str, Any]) -> int:
    upvotes = int(post.get("upvotes") or 0)
    comments = int(post.get("comment_count") or 0)
    return upvotes * 2 + comments * 3


def _parse_datetime(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _rising_hot_posts(
    *,
    community_hot_posts: list[dict[str, Any]],
    feed_watchlist: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    captured_at: str | None,
    fast_window_seconds: int = 10800,
    fast_min_upvotes: int = EXTERNAL_HIGH_LIKE_MIN_UPVOTES,
    breakout_window_seconds: int = 86400,
    breakout_min_upvotes: int = 200,
    limit: int = 5,
) -> list[dict[str, Any]]:
    now = _parse_datetime(captured_at) or datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []
    for item in community_hot_posts[:8]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("author"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": "community-hot",
            }
        )
    for item in feed_watchlist[:8]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("author"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": "feed",
            }
        )
    for item in competitor_watchlist[:10]:
        candidates.append(
            {
                "post_id": item.get("post_id"),
                "title": item.get("title"),
                "author": item.get("username"),
                "submolt": item.get("submolt"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
                "source": f"competitor-{item.get('lane') or 'watch'}",
            }
        )

    rising: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in candidates:
        post_id = str(item.get("post_id") or "").strip()
        if not post_id or post_id in seen:
            continue
        created_at = _parse_datetime(item.get("created_at"))
        if created_at is None:
            continue
        age_seconds = int((now - created_at).total_seconds())
        if age_seconds < 0:
            continue
        upvotes = int(item.get("upvotes") or 0)
        qualifies = (
            age_seconds <= fast_window_seconds and upvotes >= fast_min_upvotes
        ) or (
            age_seconds <= breakout_window_seconds and upvotes >= breakout_min_upvotes
        )
        if not qualifies:
            continue
        seen.add(post_id)
        age_hours = max(age_seconds / 3600, 0.25)
        rising.append(
            {
                **item,
                "age_seconds": age_seconds,
                "velocity_per_hour": round(upvotes / age_hours, 1),
            }
        )

    return sorted(
        rising,
        key=lambda item: (
            -float(item.get("velocity_per_hour") or 0.0),
            -int(item.get("upvotes") or 0),
            int(item.get("age_seconds") or breakout_window_seconds),
            -int(item.get("comment_count") or 0),
        ),
    )[:limit]


def _top_post_by_board(
    posts: list[dict[str, Any]],
    overview: dict[str, Any],
    *,
    boards: set[str],
) -> dict[str, Any] | None:
    ranked: list[dict[str, Any]] = []
    for item in overview.get("recent_top_posts", []):
        board = str(item.get("submolt") or "")
        if board not in boards:
            continue
        post = _find_post(posts, item.get("id")) or item
        ranked.append(post)
    if ranked:
        return sorted(ranked, key=_post_metric, reverse=True)[0]
    board_posts = [
        item
        for item in posts
        if str((item.get("submolt") or {}).get("name") if isinstance(item.get("submolt"), dict) else item.get("submolt") or "")
        in boards
    ]
    if not board_posts:
        return None
    return sorted(board_posts, key=_post_metric, reverse=True)[0]


def _topic_tokens(text: str, keywords: tuple[str, ...]) -> list[str]:
    return [token for token in keywords if token in text]


def _split_text_fragments(text: str) -> list[str]:
    return [fragment.strip() for fragment in re.split(r"[：:|丨，,。！？、（）()《》“”‘’\s]+", text) if fragment.strip()]


def _meaningful_fragments(text: str) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for fragment in _split_text_fragments(text):
        candidates = [fragment]
        candidates.extend(token for token in NOVELTY_SHORT_CJK_ALLOWLIST if token in fragment)
        for run in re.findall(r"[\u4e00-\u9fff]{2,}", fragment):
            candidates.append(run)
            if len(run) >= 6:
                candidates.append(run[:4])
        for candidate in candidates:
            normalized_candidate = _normalize_overlap_fragment(candidate)
            if len(normalized_candidate) < 2:
                continue
            if (
                normalized_candidate.isdigit()
                or normalized_candidate in seen
                or _is_low_signal_overlap_fragment(normalized_candidate)
            ):
                continue
            seen.add(normalized_candidate)
            fragments.append(normalized_candidate)
    return fragments


def _candidate_terms(titles: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for title in titles:
        counts.update(_meaningful_fragments(title))
    return counts


def _planning_item_texts(
    item: dict[str, Any],
    *,
    field_order: tuple[str, ...],
    limit: int = 2,
    text_limit: int = 72,
) -> list[str]:
    if not isinstance(item, dict):
        return []
    snippets = _signal_context_snippets(
        item,
        field_order=field_order,
        limit=limit,
        text_limit=text_limit,
    )
    if snippets:
        return snippets
    seed = _preferred_signal_seed_text(item, field_order=field_order, limit=text_limit)
    return [seed] if seed else []


def _planning_research_texts(
    *,
    top_discussion: list[dict[str, Any]],
    community_hot_posts: list[dict[str, Any]],
    feed: list[dict[str, Any]],
    competitor_watchlist: list[dict[str, Any]],
    rising_hot_posts: list[dict[str, Any]],
    external_information: dict[str, Any],
    content_objectives: list[str],
    user_topic_hints: list[dict[str, Any]],
) -> list[str]:
    texts: list[str] = []

    def extend(values: list[str]) -> None:
        texts.extend(value for value in values if str(value or "").strip())

    for item in top_discussion[:6]:
        extend(_planning_item_texts(item, field_order=("preview", "post_title", "title")))
    for item in community_hot_posts[:8]:
        extend(_planning_item_texts(item, field_order=("summary", "reason", "content", "title")))
    for item in feed[:8]:
        extend(_planning_item_texts(item, field_order=("summary", "content", "title")))
    for item in competitor_watchlist[:8]:
        extend(_planning_item_texts(item, field_order=("reason", "summary", "title", "username")))
    for item in rising_hot_posts[:6]:
        extend(_planning_item_texts(item, field_order=("summary", "reason", "title")))
    for bundle in list(external_information.get("discovery_bundles") or [])[:8]:
        extend(
            [
                str(bundle.get("pressure_summary") or "").strip(),
                str(bundle.get("conflict_note") or "").strip(),
                str(bundle.get("focus") or "").strip(),
            ]
        )
    for item in list(external_information.get("world_signal_snapshot") or [])[:8]:
        extend(_planning_item_texts(item, field_order=("pressure", "summary", "title")))
    for item in list(external_information.get("reading_notes") or [])[:8]:
        extend(_planning_item_texts(item, field_order=("summary", "excerpt", "title")))
    for item in _iter_external_world_candidates(external_information, limit=8):
        extend(_planning_item_texts(item, field_order=("relevance_note", "summary", "abstract", "excerpt", "note", "title")))
    extend([str(item).strip() for item in content_objectives[:6] if str(item).strip()])
    for item in user_topic_hints[:6]:
        extend(_planning_item_texts(item, field_order=("text", "note")))
    return _dedupe_texts(texts)


def _overloaded_keywords(titles: list[str], *, limit: int = 8) -> list[str]:
    keyword_counts = _candidate_terms(titles)
    return [keyword for keyword, count in keyword_counts.most_common(limit) if count >= TOPIC_OVERLOAD_THRESHOLD]


def _novelty_pressure(recent_titles: list[str]) -> dict[str, Any]:
    term_counts = _candidate_terms(recent_titles)
    return {
        "recent_titles": recent_titles[:RECENT_TITLE_LIMIT],
        "term_counts": dict(term_counts),
        "overloaded_keywords": _overloaded_keywords(recent_titles),
    }


def _text_overlap_score(text: str, novelty: dict[str, Any]) -> tuple[int, int, int]:
    overloaded_keywords = novelty.get("overloaded_keywords", [])
    term_counts = novelty.get("term_counts", {})
    fragments = _meaningful_fragments(text)
    repeated_penalty = sum(1 for keyword in overloaded_keywords if keyword in text)
    historical_penalty = sum(int(term_counts.get(fragment, 0)) for fragment in fragments)
    return 0, repeated_penalty, historical_penalty


def _opportunity_rank_score(item: dict[str, Any], *, signal_summary: dict[str, Any]) -> float:
    quality_score = float(item.get("quality_score") or 0.0)
    freshness_score = float(item.get("freshness_score") or 0.0)
    world_score = float(item.get("world_score") or 0.0)
    overlap = item.get("overlap_score") or (0, 0, 0)
    overlap_penalty = float(sum(int(part or 0) for part in overlap))
    signal_type = str(item.get("signal_type") or "")
    internal_penalty = 0.75 if _is_internal_maintenance_signal(item) else 0.0
    if signal_type in WEAK_INTERNAL_SIGNAL_TYPES:
        internal_penalty += 1.2
    if str(item.get("signal_type") or "") == "user-hint":
        internal_penalty += 0.1
    if _looks_like_low_heat_followup(str(item.get("source_text") or ""), signal_summary):
        internal_penalty += 3.0
    evidence_bonus = 0.35 if str(item.get("evidence_hint") or "").strip() else 0.0
    publishability_penalty = _opportunity_publishability_penalty(item)
    return (
        quality_score * 2.8
        + freshness_score
        + world_score
        + evidence_bonus
        - overlap_penalty
        - internal_penalty
        - publishability_penalty
    )


def _opportunity_publishability_penalty(item: dict[str, Any]) -> float:
    track = str(item.get("track") or "").strip()
    signal_type = str(item.get("signal_type") or "").strip()
    source_text = str(item.get("source_text") or "").strip()
    why_now = str(item.get("why_now") or "").strip()
    evidence_hint = str(item.get("evidence_hint") or "").strip()
    if not source_text:
        return 0.0

    penalty = 0.0
    if track == "theory" and _theory_source_text_needs_public_reframe(signal_type, source_text):
        penalty += 3.0 if signal_type in {"paper", "classic", "github", "external", "world-bundle"} else 1.2
    if track in {"tech", "group"} and _method_source_text_needs_object_reframe(signal_type, source_text):
        if signal_type == "world-bundle":
            penalty += 6.2
        elif signal_type in {"paper", "classic", "github", "external"}:
            penalty += 3.6
        else:
            penalty += 1.5
    if (
        track in {"tech", "group"}
        and signal_type == "world-bundle"
        and _method_source_text_needs_object_reframe(signal_type, source_text)
        and not _method_title_has_concrete_anchor(evidence_hint)
    ):
        penalty += 3.0
    if _world_bundle_focus_is_low_signal(source_text):
        penalty += 2.2 if signal_type == "world-bundle" else 1.0
    if _ascii_heavy_text(source_text) and signal_type in {"paper", "classic", "github", "external"}:
        penalty += 2.2
    if (
        signal_type in {"paper", "external", "github", "world-bundle"}
        and not evidence_hint
        and _ascii_heavy_text(_joined_idea_text(source_text, why_now))
    ):
        penalty += 0.9
    return round(penalty, 3)


def _ranked_track_opportunities(track: str, signal_summary: dict[str, Any]) -> list[dict[str, Any]]:
    opportunities = [item for item in signal_summary.get("dynamic_topics", []) if item.get("track") == track]
    if not opportunities:
        return []
    filtered = [
        item
        for item in opportunities
        if not (
            track in {"theory", "tech"}
            and _is_metric_surface_text(
                _joined_idea_text(item.get("source_text"), item.get("why_now"), item.get("angle_hint"))
            )
        )
        and not (
            track in {"tech", "group"}
            and str(item.get("signal_type") or "").strip() == "world-bundle"
            and _world_bundle_focus_is_low_signal(str(item.get("source_text") or "").strip())
        )
    ]
    if not filtered:
        return []
    if track in {"theory", "tech"}:
        primary_ready = [item for item in filtered if _is_primary_ready_opportunity(item, signal_summary)]
        if primary_ready:
            filtered = primary_ready
        elif all(str(item.get("signal_type") or "") == "reply-pressure" for item in filtered):
            return []
    return sorted(
        filtered,
        key=lambda item: (
            -_opportunity_rank_score(item, signal_summary=signal_summary),
            item.get("overlap_score", (0, 0, 0)),
            len(str(item.get("source_text") or "")),
        ),
    )


def _pick_track_opportunity(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    ranked = _ranked_track_opportunities(track, signal_summary)
    return ranked[0] if ranked else {}


def _bundle_seed_fragments(text: str) -> set[str]:
    fragments: set[str] = set()
    for fragment in _meaningful_fragments(text):
        normalized = _normalize_title(fragment)
        if (
            not normalized
            or normalized in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
            or len(fragment) < 2
            or len(fragment) > 12
        ):
            continue
        fragments.add(normalized)
        if len(fragments) >= 8:
            break
    return fragments


def _bundle_title_seed(source_texts: list[str]) -> str:
    if not source_texts:
        return ""
    head = str(source_texts[0] or "").strip()
    if not head:
        return ""
    head_fragments = _bundle_seed_fragments(head)
    for candidate in source_texts[1:3]:
        candidate_text = str(candidate or "").strip()
        if not candidate_text:
            continue
        if head_fragments & _bundle_seed_fragments(candidate_text):
            return " / ".join([head, candidate_text]).strip()
    return head


def _track_signal_bundle(track: str, signal_summary: dict[str, Any], *, limit: int = 3) -> dict[str, Any]:
    ranked = _ranked_track_opportunities(track, signal_summary)
    if not ranked:
        return {}
    items = ranked[:limit]
    lead = items[0]
    source_texts = _dedupe_texts([str(item.get("source_text") or "").strip() for item in items if str(item.get("source_text") or "").strip()])
    why_now_parts = _dedupe_texts([str(item.get("why_now") or "").strip() for item in items if str(item.get("why_now") or "").strip()])
    angle_hints = _dedupe_texts([str(item.get("angle_hint") or "").strip() for item in items if str(item.get("angle_hint") or "").strip()])
    evidence_hints = _dedupe_texts([str(item.get("evidence_hint") or "").strip() for item in items if str(item.get("evidence_hint") or "").strip()])
    signal_types = _dedupe_texts([str(item.get("signal_type") or "").strip() for item in items if str(item.get("signal_type") or "").strip()])
    base_score = max(_opportunity_rank_score(item, signal_summary=signal_summary) for item in items)
    bundle_bonus = min(max(0, len(items) - 1), 2) * 0.35 + min(max(0, len(signal_types) - 1), 2) * 0.2
    bundle = {
        "track": track,
        "lead": lead,
        "items": items,
        "score": round(base_score + bundle_bonus, 2),
        "signal_types": signal_types,
        "source_texts": source_texts,
        "why_now_parts": why_now_parts,
        "angle_hints": angle_hints,
        "evidence_hints": evidence_hints,
        "title_seed": _bundle_title_seed(source_texts),
        "focus_text": source_texts[0] if source_texts else "",
        "why_now": "；".join(why_now_parts[:2]).strip(),
        "angle_hint": "；".join(angle_hints[:2]).strip(),
        "preferred_board": str(lead.get("preferred_board") or "").strip(),
        "signal_type": str(lead.get("signal_type") or "").strip(),
    }
    if track == "theory":
        bundle.update(_theory_bundle_public_seed(bundle, lead))
    return bundle


WORLD_GROUNDED_SIGNAL_TYPES = {
    "world-bundle",
    "community-breakout",
    "community-hot",
    "rising-hot",
    "paper",
    "github",
    "zhihu",
    "classic",
    "external",
}


def _bundle_has_grounding(bundle: dict[str, Any], *, track: str) -> bool:
    items = [item for item in list(bundle.get("items") or []) if isinstance(item, dict)]
    if not items:
        return False
    world_hits = sum(
        1
        for item in items
        if float(item.get("world_score") or 0.0) >= 0.7
        or str(item.get("signal_type") or "").strip() in WORLD_GROUNDED_SIGNAL_TYPES
    )
    evidence_hits = sum(1 for item in items if str(item.get("evidence_hint") or "").strip())
    concrete_hits = sum(1 for item in items if not _is_internal_maintenance_signal(item))
    source_count = len(
        {
            str(item.get("source_text") or "").strip()
            for item in items
            if str(item.get("source_text") or "").strip()
        }
    )
    if track == "theory":
        return bool(world_hits or (source_count >= 2 and evidence_hits >= 1 and concrete_hits >= 2))
    if track in {"tech", "group"}:
        return bool(
            world_hits
            or evidence_hits
            or any(str(item.get("signal_type") or "").strip() == "failure" for item in items)
        )
    return bool(world_hits or evidence_hits or concrete_hits)


def _track_kind(track: str) -> str:
    return TRACK_KIND_MAP.get(str(track or "").strip(), "theory-post")


def _kind_track(kind: str) -> str:
    cleaned = str(kind or "").strip()
    for track_name, kind_name in TRACK_KIND_MAP.items():
        if kind_name == cleaned:
            return track_name
    return ""


def _live_track_order(signal_summary: dict[str, Any], *, group_enabled: bool) -> list[str]:
    track_scores: dict[str, float] = {}
    track_order: dict[str, int] = {}
    observed_tracks: set[str] = set()

    def remember(track: Any, *, score: Any = 0.0, observed: bool = False) -> None:
        cleaned = str(track or "").strip()
        if not cleaned:
            return
        if cleaned == "group" and not group_enabled:
            return
        if cleaned not in TRACK_KIND_MAP:
            return
        try:
            numeric_score = float(score or 0.0)
        except (TypeError, ValueError):
            numeric_score = 0.0
        if cleaned not in track_order:
            track_order[cleaned] = len(track_order)
        if observed:
            observed_tracks.add(cleaned)
        track_scores[cleaned] = max(track_scores.get(cleaned, float("-inf")), numeric_score)

    for item in list(signal_summary.get("dynamic_topic_bundles") or []):
        if isinstance(item, dict):
            remember(
                item.get("track"),
                score=item.get("pressure_score") or item.get("score") or 0.0,
                observed=True,
            )
    for item in list(signal_summary.get("dynamic_topics") or []):
        if isinstance(item, dict):
            remember(
                item.get("track"),
                score=item.get("track_score") or item.get("score") or 0.0,
                observed=True,
            )
    for item in _fallback_lane_pressure_entries(signal_summary, group_enabled=group_enabled):
        if not isinstance(item, dict):
            continue
        track = _kind_track(str(item.get("kind") or "").strip())
        if track in observed_tracks:
            continue
        remember(
            track,
            score=item.get("score") or 0.0,
        )
    return [
        track
        for track, _score in sorted(
            track_scores.items(),
            key=lambda item: (
                -float(item[1] or 0.0),
                int(track_order.get(str(item[0] or ""), len(track_order) + 3)),
                str(item[0] or ""),
            ),
        )
    ]


def _track_priority_entry(track: str, signal_summary: dict[str, Any]) -> dict[str, Any] | None:
    bundle = _track_signal_bundle(track, signal_summary)
    if not bundle:
        return None
    if not _bundle_has_grounding(bundle, track=track):
        return None
    lead = bundle.get("lead") or {}
    score = float(bundle.get("score") or 0.0)
    if track == "tech":
        score += min(len(signal_summary.get("unresolved_failures") or []), 3) * 0.2
    elif track == "group":
        group_hot_posts = ((signal_summary.get("group_watch") or {}).get("hot_posts") or [])[:4]
        score += min(len(group_hot_posts), 3) * 0.35
        if str(lead.get("signal_type") or "") in WEAK_INTERNAL_SIGNAL_TYPES:
            score -= 1.0
    return {
        "track": track,
        "kind": _track_kind(track),
        "score": round(score, 2),
        "signal_type": str(lead.get("signal_type") or "").strip(),
        "source_text": truncate_text(
            str(bundle.get("public_focus_text") or bundle.get("title_seed") or bundle.get("focus_text") or ""),
            48,
        ),
        "bundle_size": len(bundle.get("items") or []),
    }


def _fallback_lane_pressure_entries(signal_summary: dict[str, Any], *, group_enabled: bool) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for track in TRACK_KIND_MAP:
        if track == "group" and not group_enabled:
            continue
        kind = _track_kind(track)
        bundle = _track_signal_bundle(track, signal_summary)
        lead = bundle.get("lead") or {} if bundle else {}
        seed = _fallback_track_seed(track, signal_summary)
        focus = _concrete_focus_text(
            seed.get("source_text"),
            (bundle or {}).get("public_focus_text"),
            (bundle or {}).get("focus_text"),
            (bundle or {}).get("title_seed"),
            lead.get("source_text"),
        )
        reason = _object_level_pressure_text(
            seed.get("why_now"),
            (bundle or {}).get("why_now"),
            lead.get("why_now"),
            (bundle or {}).get("pressure_summary"),
            (bundle or {}).get("conflict_note"),
            *((bundle or {}).get("support_signals") or []),
            fallback=focus,
        )
        if not focus and not reason:
            continue
        grounded = bool(bundle) and _bundle_has_grounding(bundle, track=track)
        note_score = _source_signal_note_score(track, reason) if reason else 0.0
        track_fit = _track_signal_fit(
            track,
            focus,
            reason,
            seed.get("angle_hint"),
            lead.get("evidence_hint"),
        )
        score = float((bundle or {}).get("score") or 0.0)
        if grounded:
            score += 1.35
        if focus:
            score += 0.75
        if reason:
            score += max(note_score, 0.0) * 0.65
        if track_fit > 0:
            score += min(track_fit, 1.6) * 0.45
        if str(seed.get("signal_type") or lead.get("signal_type") or "").strip() == "failure":
            score += 0.75
        reason_text = reason
        if focus and reason_text and _normalize_title(focus) != _normalize_title(reason_text) and focus not in reason_text:
            reason_text = f"{focus}：{reason_text}"
        elif not reason_text:
            reason_text = focus
        ranked.append(
            {
                "kind": kind,
                "score": round(score, 2),
                "reason": truncate_text(str(reason_text or "").strip(), 120),
            }
        )
    return sorted(
        ranked,
        key=lambda item: (
            -float(item.get("score") or 0.0),
            str(item.get("kind") or ""),
        ),
    )


def _lane_entry_grounded(entry: dict[str, Any], signal_summary: dict[str, Any]) -> bool:
    track = str(entry.get("track") or "").strip()
    if not track:
        return False
    for bundle in list(signal_summary.get("dynamic_topic_bundles") or []):
        if str(bundle.get("track") or "").strip() != track:
            continue
        if "grounded" in bundle:
            return bool(bundle.get("grounded"))
        return _bundle_has_grounding(bundle, track=track)
    bundle = _track_signal_bundle(track, signal_summary)
    if not bundle:
        return False
    return _bundle_has_grounding(bundle, track=track)


def _dynamic_lane_focus_kind(selected_entries: list[dict[str, Any]], signal_summary: dict[str, Any]) -> str:
    if not selected_entries:
        return ""
    top_kind = str(selected_entries[0].get("kind") or "").strip()
    if len(selected_entries) == 1:
        return top_kind
    top_score = float(selected_entries[0].get("score") or 0.0)
    second_score = float(selected_entries[1].get("score") or 0.0)
    if top_score - second_score > 0.65:
        return top_kind
    if second_score >= max(3.4, top_score - 0.45) and all(
        _lane_entry_grounded(entry, signal_summary) for entry in selected_entries[:2]
    ):
        return ""
    if top_score - second_score <= 0.32:
        return ""
    return top_kind


def _dynamic_idea_lane_strategy(signal_summary: dict[str, Any], *, group_enabled: bool) -> dict[str, Any]:
    max_slots = 3 if group_enabled else 2
    ranked: list[dict[str, Any]] = []
    for track in _live_track_order(signal_summary, group_enabled=group_enabled):
        entry = _track_priority_entry(track, signal_summary)
        if entry:
            ranked.append(entry)
    ranked.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("track") or "")))
    if not ranked:
        fallback_ranked = _fallback_lane_pressure_entries(signal_summary, group_enabled=group_enabled)
        if not fallback_ranked:
            return {
                "selected_kinds": [],
                "focus_kind": "",
                "backup_kinds": [],
                "lane_scores": [],
                "rationale": "当前没有够格的公开 lane，也没有必要拿默认残压硬补空心题。",
            }
        lane_text = "、".join(_lane_reason_label(item) for item in fallback_ranked[:max_slots])
        return {
            "selected_kinds": [],
            "focus_kind": "",
            "backup_kinds": [],
            "lane_scores": fallback_ranked[:max_slots],
            "rationale": (
                "当前只有残压观察，没有哪条公开 lane 已经长成可直接发的题。"
                f" 先把 {lane_text or '现有残压'} 留作观察，不把它们提前写死成必须补位的标题；"
                f" {str(fallback_ranked[0].get('reason') or '').strip()}"
            ),
        }

    selected_kinds: list[str] = [str(ranked[0].get("kind") or "")]
    top_score = float(ranked[0].get("score") or 0.0)
    for item in ranked[1:]:
        score = float(item.get("score") or 0.0)
        if score >= max(3.4, top_score - 0.85):
            selected_kinds.append(str(item.get("kind") or ""))
        if len(selected_kinds) >= max_slots:
            break
    selected_entries = [item for item in ranked if str(item.get("kind") or "") in selected_kinds][: len(selected_kinds)]
    lane_text = "、".join(_lane_reason_label(item) for item in ranked[:max_slots])
    selected_text = "、".join(_lane_reason_label(item) for item in selected_entries)
    focus_kind = _dynamic_lane_focus_kind(selected_entries, signal_summary)
    focus_entry = next((item for item in selected_entries if str(item.get("kind") or "").strip() == focus_kind), {})
    focus_label = _lane_reason_label(focus_entry) if focus_entry else _planner_public_kind_display_name(focus_kind)
    backup_kinds = [kind for kind in selected_kinds if kind != focus_kind] if focus_kind else list(selected_kinds)
    if not focus_kind and selected_kinds:
        rationale = f"本轮公开短名单并列保留 {selected_text}，先让对象级压力继续竞争，不提前钉死主位。"
    elif backup_kinds:
        rationale = f"本轮公开短名单先看 {focus_label}，并列候选保留 {selected_text}。"
    else:
        rationale = f"本轮公开短名单先看 {focus_label}，其他观察暂不为了对称感硬补。"
    return {
        "selected_kinds": selected_kinds[:max_slots],
        "focus_kind": focus_kind,
        "backup_kinds": backup_kinds[: (max_slots if not focus_kind else max(0, max_slots - 1))],
        "lane_scores": ranked[:max_slots],
        "rationale": (
            f"{rationale} 动态排序为 {lane_text}；较弱观察让位给更强的现场压力。"
        ),
    }


def _strong_public_title_keys(signal_summary: dict[str, Any]) -> set[str]:
    titles: set[str] = set()
    for item in signal_summary.get("recent_top_posts", []) or []:
        normalized = _normalize_title(str(item.get("title") or ""))
        if normalized:
            titles.add(normalized)
    for key in ("hot_theory_post", "hot_tech_post", "hot_group_post"):
        normalized = _normalize_title(str((signal_summary.get(key) or {}).get("title") or ""))
        if normalized:
            titles.add(normalized)
    for lane in ("community_hot_posts", "rising_hot_posts"):
        for item in signal_summary.get(lane, []) or []:
            normalized = _normalize_title(str(item.get("title") or ""))
            if normalized:
                titles.add(normalized)
    return titles


def _is_primary_ready_opportunity(item: dict[str, Any], signal_summary: dict[str, Any]) -> bool:
    signal_type = str(item.get("signal_type") or "")
    if signal_type != "reply-pressure":
        return True
    source_key = _normalize_title(str(item.get("source_text") or ""))
    return bool(source_key and source_key in _strong_public_title_keys(signal_summary))


def _is_internal_maintenance_signal(item: dict[str, Any]) -> bool:
    return str(item.get("signal_type") or "") in {"reply-pressure", "promo", "hot-theory", "hot-tech", "hot-group"}


def _recent_low_heat_cluster_fragments(signal_summary: dict[str, Any], *, limit: int = 18) -> list[str]:
    now = datetime.now(timezone.utc)
    semantic_tokens = (
        THEORY_BOARD_STRUCTURAL_CUES
        + THEORY_TITLE_ENTRY_STAKE_TOKENS
        + THEORY_TITLE_DIRECT_ACTOR_TOKENS
        + TECH_TRACK_HINT_TOKENS
        + ("规则", "流程", "协议", "责任链", "状态词")
    )
    fragments: list[str] = []
    seen: set[str] = set()
    failures = (signal_summary.get("low_heat_failures") or {}).get("items", [])
    for item in failures[:4]:
        recorded_at = _parse_datetime(item.get("recorded_at"))
        if recorded_at is not None:
            age_seconds = (now - recorded_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds > LOW_HEAT_FOLLOWUP_WINDOW_HOURS * 3600:
                continue
        texts: list[Any] = [item.get("title"), item.get("summary")]
        texts.extend(list(item.get("lessons") or [])[:3])
        for text in texts:
            for fragment in _meaningful_fragments(str(text or "")):
                normalized = _normalize_title(fragment)
                if (
                    len(fragment) < 2
                    or len(fragment) > 16
                    or not normalized
                    or normalized in seen
                    or normalized in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
                ):
                    continue
                if not _contains_any(fragment, semantic_tokens):
                    continue
                for token in semantic_tokens:
                    token_key = _normalize_title(token)
                    if (
                        len(token) < 2
                        or token not in fragment
                        or not token_key
                        or token_key in seen
                        or token_key in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
                    ):
                        continue
                    seen.add(token_key)
                    fragments.append(token)
                    if len(fragments) >= limit:
                        return fragments
                seen.add(normalized)
                fragments.append(fragment)
                if len(fragments) >= limit:
                    return fragments
    return fragments


def _recent_low_heat_items_in_window(signal_summary: dict[str, Any], *, limit: int = 6) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    picked: list[dict[str, Any]] = []
    for item in (signal_summary.get("low_heat_failures") or {}).get("items", []):
        recorded_at = _parse_datetime(item.get("recorded_at"))
        if recorded_at is not None:
            age_seconds = (now - recorded_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds > LOW_HEAT_FOLLOWUP_WINDOW_HOURS * 3600:
                continue
        picked.append(item)
        if len(picked) >= limit:
            break
    return picked


def _low_heat_cluster_signature_tokens(text: str, *, limit: int = 8) -> list[str]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return []
    picked: list[str] = []
    seen: set[str] = set()
    for token in LOW_HEAT_CLUSTER_SIGNATURE_TOKENS:
        token_key = _normalize_title(token)
        if not token_key or token_key in seen or token not in cleaned:
            continue
        seen.add(token_key)
        picked.append(token)
        if len(picked) >= limit:
            break
    return picked


def _looks_like_low_heat_followup(text: str, signal_summary: dict[str, Any]) -> bool:
    normalized_text = _normalize_title(text)
    if not normalized_text:
        return False
    has_concrete_object = _contains_any(text, METHOD_TITLE_CONCRETE_OBJECT_TOKENS)
    strong_titles = _strong_public_title_keys(signal_summary)
    for item in signal_summary.get("pending_reply_posts", []) or []:
        title_key = _normalize_title(str(item.get("post_title") or ""))
        if not title_key or title_key in strong_titles:
            continue
        if len(title_key) >= 12 and (title_key in normalized_text or normalized_text in title_key):
            return True
    for item in (signal_summary.get("low_heat_failures") or {}).get("items", [])[:4]:
        title_key = _normalize_title(str(item.get("title") or ""))
        if not title_key or title_key in strong_titles:
            continue
        if len(title_key) >= 10 and (title_key in normalized_text or normalized_text in title_key):
            return True
    overlap_count = 0
    for fragment in _recent_low_heat_cluster_fragments(signal_summary):
        fragment_key = _normalize_title(fragment)
        if not fragment_key or fragment_key not in normalized_text:
            continue
        overlap_count += 1
        if overlap_count >= 3:
            return True
    semantic_overlap = 0
    semantic_seen: set[str] = set()
    low_heat_fragments = _recent_low_heat_cluster_fragments(signal_summary)
    for token in (
        THEORY_BOARD_STRUCTURAL_CUES
        + THEORY_TITLE_ENTRY_STAKE_TOKENS
        + THEORY_TITLE_DIRECT_ACTOR_TOKENS
        + TECH_TRACK_HINT_TOKENS
        + ("规则", "流程", "协议", "责任链", "状态词")
    ):
        token_key = _normalize_title(token)
        if (
            len(str(token or "").strip()) < 2
            or not token_key
            or token_key in semantic_seen
            or token_key not in normalized_text
        ):
            continue
        if not any(token_key in _normalize_title(fragment) for fragment in low_heat_fragments):
            continue
        semantic_seen.add(token_key)
        semantic_overlap += 1
        if semantic_overlap >= 3:
            return True
    if semantic_overlap >= 2 and not has_concrete_object:
        return True
    candidate_signatures = set(_low_heat_cluster_signature_tokens(text))
    if candidate_signatures and not has_concrete_object:
        for item in _recent_low_heat_items_in_window(signal_summary):
            combined = "\n".join(
                [
                    str(item.get("title") or "").strip(),
                    str(item.get("summary") or "").strip(),
                    *(str(part).strip() for part in list(item.get("lessons") or [])[:2] if str(part).strip()),
                ]
            )
            failure_signatures = set(_low_heat_cluster_signature_tokens(combined))
            if len(candidate_signatures & failure_signatures) >= 2:
                return True
    return False


def _stable_pattern_index(*parts: Any, modulo: int) -> int:
    if modulo <= 1:
        return 0
    seed = "|".join(str(part or "") for part in parts)
    return sum((index + 1) * ord(ch) for index, ch in enumerate(seed)) % modulo


def _runtime_title_fragments(*texts: str) -> list[str]:
    seen: set[str] = set()
    picked: list[str] = []
    for text in texts:
        for fragment in _meaningful_fragments(text):
            cleaned = _sanitize_reserved_text(fragment)
            if len(cleaned) < 2 or len(cleaned) > 14:
                continue
            lowered = cleaned.lower()
            if _looks_like_placeholder_title(cleaned):
                continue
            if not _contains_cjk(cleaned):
                if not re.fullmatch(r"[A-Z][A-Z0-9-]{1,7}", cleaned):
                    continue
                if lowered in GENERIC_ASCII_TITLE_FRAGMENTS:
                    continue
            normalized = _normalize_title(cleaned)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            picked.append(cleaned)
            if len(picked) >= 3:
                return picked
    return picked


def _compose_fragment_title(track: str, *texts: str) -> str:
    fragments = _runtime_title_fragments(*texts)
    if track == "group":
        core = " / ".join(fragments[:2]) if len(fragments) >= 2 else (fragments[0] if fragments else "系统诊断")
        return f"Agent心跳同步实验室：{truncate_text(core, 24)}"
    if len(fragments) >= 2:
        return truncate_text(f"{fragments[0]}：{fragments[1]}", 30)
    if fragments:
        return truncate_text(fragments[0], 28)
    fallback = _sanitize_reserved_text(" ".join(texts).strip())
    if fallback and _contains_cjk(fallback) and not _looks_like_placeholder_title(fallback):
        return truncate_text(fallback, 28)
    return ("系统判断" if track == "tech" else "新的社会命名")


def _title_fragments_related(left: str, right: str) -> bool:
    left_normalized = _normalize_title(left)
    right_normalized = _normalize_title(right)
    if not left_normalized or not right_normalized:
        return False
    return (
        left_normalized == right_normalized
        or left_normalized in right_normalized
        or right_normalized in left_normalized
    )


def _candidate_title_units(track: str, *texts: Any, limit: int = 6) -> list[str]:
    cleaned_texts = [str(text or "").strip() for text in texts if str(text or "").strip()]
    if not cleaned_texts:
        return []
    if track == "theory":
        raw_candidates = [
            *_bundle_structural_fragments(*cleaned_texts, limit=max(limit * 2, 8)),
            *_runtime_title_fragments(*cleaned_texts),
        ]
    else:
        raw_candidates = [
            *_runtime_title_fragments(*cleaned_texts),
            *_bundle_structural_fragments(*cleaned_texts, limit=max(limit * 2, 8)),
        ]
    picked: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        cleaned = truncate_text(_sanitize_reserved_text(str(candidate or "").strip()), 18)
        normalized = _normalize_title(cleaned)
        if (
            not cleaned
            or not normalized
            or normalized in seen
            or _focus_text_is_generic(cleaned)
            or _source_title_shell(cleaned)
        ):
            continue
        seen.add(normalized)
        picked.append(cleaned)
        if len(picked) >= limit:
            break
    return picked


def _pick_title_lead(track: str, fragments: list[str], *texts: Any) -> str:
    if track == "theory":
        for fragment in fragments:
            if _title_has_public_structural_anchor(fragment) or _contains_any(fragment, THEORY_TITLE_DIRECT_ACTOR_TOKENS):
                return fragment
    elif track in {"tech", "group"}:
        for fragment in fragments:
            if _method_title_has_concrete_anchor(fragment):
                return fragment
    return _concrete_focus_text(*fragments, *texts, limit=30)


def _pick_title_detail(track: str, fragments: list[str], lead: str) -> str:
    for fragment in fragments:
        if _title_fragments_related(fragment, lead):
            continue
        if track == "theory":
            if _title_has_public_structural_anchor(fragment) or _contains_any(fragment, THEORY_TITLE_DIRECT_ACTOR_TOKENS):
                return fragment
        elif track in {"tech", "group"} and _method_title_has_concrete_anchor(fragment):
            return fragment
    for fragment in fragments:
        if not _title_fragments_related(fragment, lead):
            return fragment
    return ""


def _method_title_fragment_is_low_signal(fragment: str) -> bool:
    cleaned = str(fragment or "").strip()
    if not cleaned:
        return True
    compact = re.sub(r"\s+", "", cleaned)
    if not compact:
        return True
    if compact.startswith(("的", "了", "把", "让", "将", "再", "又", "更", "最")):
        return True
    if compact in {"我发现", "价值", "模式", "能力", "问题", "系统", "方法", "流程", "方案"}:
        return True
    if re.fullmatch(r"[的一是在将把给了呢吗吧和与及或其这那再又更最还都很太只仅等]{1,5}", compact):
        return True
    return False


def _method_title_has_detail_anchor(track: str, fragment: str) -> bool:
    detail_tokens = TECH_TRACK_HINT_TOKENS if track == "tech" else GROUP_TRACK_HINT_TOKENS
    return bool(
        _method_title_has_concrete_anchor(fragment)
        or _contains_any(fragment, detail_tokens)
        or any(token in str(fragment or "") for token in ("失败", "风险", "断口", "边界", "窗口"))
    )


def _filtered_method_title_fragments(track: str, *texts: Any) -> list[str]:
    fragments = _candidate_title_units(track, *texts)
    filtered = [fragment for fragment in fragments if not _method_title_fragment_is_low_signal(fragment)]
    return filtered or fragments


def _method_focus_text_from_inputs(
    track: str,
    signal_type: str,
    source_text: str,
    *context_texts: Any,
) -> str:
    cleaned_source = truncate_text(_sanitize_reserved_text(str(source_text or "").strip()), 18)
    compact_source = re.sub(r"\s+", "", cleaned_source)
    if (
        cleaned_source
        and len(compact_source) <= 12
        and not _method_title_fragment_is_low_signal(cleaned_source)
        and not _method_source_text_needs_object_reframe(signal_type, cleaned_source)
    ):
        return cleaned_source

    fragments = _filtered_method_title_fragments(track, source_text, *context_texts)
    for fragment in fragments:
        if _method_title_has_concrete_anchor(fragment):
            return truncate_text(fragment, 18)
    for fragment in fragments:
        if _method_title_has_detail_anchor(track, fragment):
            return truncate_text(fragment, 18)
    return _concrete_focus_text(*fragments, source_text, *context_texts, limit=18)


def _compose_method_dynamic_title(
    track: str,
    signal_type: str,
    source_text: str,
    *,
    context_texts: tuple[Any, ...] = (),
) -> str:
    cleaned_inputs = [str(source_text or "").strip(), *(str(text or "").strip() for text in context_texts if str(text or "").strip())]
    if not cleaned_inputs:
        return _fallback_dynamic_title(track, signal_type, source_text, *context_texts)
    if _looks_like_placeholder_title(cleaned_inputs[0]) or not _contains_cjk(cleaned_inputs[0]):
        return _fallback_dynamic_title(track, signal_type, *cleaned_inputs)

    fragments = _filtered_method_title_fragments(track, *cleaned_inputs)
    lead = _method_focus_text_from_inputs(track, signal_type, cleaned_inputs[0], *cleaned_inputs[1:]) or _pick_title_lead(
        track,
        fragments,
        *cleaned_inputs,
    )
    detail = next(
        (
            fragment
            for fragment in fragments
            if not _title_fragments_related(fragment, lead) and _method_title_has_detail_anchor(track, fragment)
        ),
        "",
    )
    if not detail:
        detail = next(
            (
                fragment
                for fragment in fragments
                if not _title_fragments_related(fragment, lead) and not _method_title_fragment_is_low_signal(fragment)
            ),
            "",
        )

    if track == "group":
        ordered_inputs = [value for value in [lead, detail, *cleaned_inputs] if str(value or "").strip()]
        return _compose_fragment_title("group", *ordered_inputs)
    if lead and detail and not _title_fragments_related(lead, detail):
        return truncate_text(f"{lead}：{detail}", 30)
    if lead:
        return truncate_text(lead, 28)
    return _fallback_dynamic_title(track, signal_type, *cleaned_inputs)


def _fallback_dynamic_title(track: str, signal_type: str, *texts: Any) -> str:
    cleaned_texts = [str(text or "").strip() for text in texts if str(text or "").strip()]
    if not cleaned_texts:
        return "系统判断" if track == "tech" else "新的社会命名"
    fragments = _candidate_title_units(track, *cleaned_texts)
    lead = _pick_title_lead(track, fragments, *cleaned_texts)
    detail = _pick_title_detail(track, fragments, lead)
    if track == "group":
        title = _compose_fragment_title("group", *(value for value in [lead, detail, *cleaned_texts] if str(value or "").strip()))
    elif lead and detail and not _title_fragments_related(lead, detail):
        title = truncate_text(f"{lead}：{detail}", 30)
    elif lead:
        title = truncate_text(lead, 28)
    else:
        title = _compose_fragment_title(track, *cleaned_texts)
    if track == "theory" and _theory_source_text_needs_public_reframe(str(signal_type or "").strip(), title):
        structural_title = _compose_fragment_title(
            "theory",
            *(
                fragment
                for fragment in fragments
                if _title_has_public_structural_anchor(fragment) or _contains_any(fragment, THEORY_TITLE_DIRECT_ACTOR_TOKENS)
            ),
            *cleaned_texts,
        )
        if structural_title and not _theory_source_text_needs_public_reframe(str(signal_type or "").strip(), structural_title):
            return structural_title
    return title


def _structural_fallback_title(track: str, signal_type: str, source_text: str, *context_texts: Any) -> str:
    return _fallback_dynamic_title(track, signal_type, source_text, *context_texts)


def _echoes_source_title(title: str) -> bool:
    cleaned = str(title or "").strip()
    if not cleaned:
        return False
    if any(re.search(pattern, cleaned) for pattern in FORBIDDEN_SOURCE_ECHO_PATTERNS):
        return True
    return "《" in cleaned and "》" in cleaned and any(token in cleaned for token in ("继续追问", "拆开看", "整理成", "别把"))


def _text_has_low_autonomy_phrase(text: Any) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    compact = re.sub(r"\s+", "", cleaned)
    return any(re.search(pattern, compact) for pattern in LOW_AUTONOMY_PHRASE_PATTERNS)


def _idea_uses_low_autonomy_language(idea: dict[str, Any]) -> bool:
    texts = [
        idea.get("title"),
        idea.get("angle"),
        idea.get("why_now"),
        idea.get("concept_core"),
        idea.get("mechanism_core"),
        idea.get("boundary_note"),
        idea.get("theory_position"),
        idea.get("practice_program"),
    ]
    texts.extend(list(idea.get("source_signals") or []))
    return any(_text_has_low_autonomy_phrase(text) for text in texts)


def _title_has_public_structural_anchor(title: str) -> bool:
    return any(token in str(title or "") for token in TITLE_PUBLIC_STRUCTURAL_TOKENS)


def _theory_title_surface_overhang_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, THEORY_TITLE_SURFACE_TOKENS):
        return ""
    lead = title_text.split("：", 1)[0]
    if not _contains_any(lead, THEORY_TITLE_SURFACE_TOKENS):
        lead = title_text[:16]
    if _contains_any(title_text, THEORY_TITLE_ACTOR_TOKENS) or _contains_any(lead, THEORY_TITLE_ACTOR_TOKENS):
        return ""
    return "标题还在从维护页、首页、入口这类前台表象起题，没有把谁在失去资格、谁在承担代价摆到门面上。"


def _theory_title_emotion_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if _title_has_public_structural_anchor(title_text) or _contains_any(title_text, THEORY_TITLE_DIRECT_ACTOR_TOKENS):
        return ""
    if "不是" not in title_text or "而是" not in title_text:
        return ""
    if not (
        title_text.startswith("最")
        or _contains_any(title_text, THEORY_TITLE_EMOTION_SHELL_TOKENS)
        or _contains_any(title_text, THEORY_TITLE_STATUS_SHELL_TOKENS)
    ):
        return ""
    return "标题在借“最折磨人的，不是……而是……”这类情绪壳起题，读者先看到的是共感句，不是这条判断真正要抓的责任、资格或接管冲突。"


def _theory_title_empathy_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, THEORY_TITLE_EMPATHY_SHELL_TOKENS):
        return ""
    if not _contains_any(title_text, THEORY_TITLE_EMPATHY_STAKE_TOKENS):
        return ""
    if _contains_any(title_text, THEORY_TITLE_SERVICE_CHAIN_TOKENS):
        return ""
    return "标题还在借“AI 可以先安慰你 / 理解你”这类拟共情壳起题，读者先看到的是情绪代入，不是接手链、审核门槛或责任分配。"


def _theory_title_memory_capability_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, THEORY_TITLE_MEMORY_CAPABILITY_TOKENS):
        return ""
    if not (_contains_any(title_text, THEORY_TITLE_RETRY_RESULT_TOKENS) or "为什么总" in title_text):
        return ""
    if _contains_any(title_text, THEORY_TITLE_CONCRETE_HANDOFF_TOKENS):
        return ""
    return "标题先把“会翻聊天记录 / 记得你”的记忆能力摆成卖点，再用“重新提交 / 继续等待”这种结果词兜症状，却没把驳回、签收或回写断口摆出来。读者先看到的是功能演示，不是责任断口。"


def _theory_title_memory_spec_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, THEORY_TITLE_MEMORY_CAPABILITY_TOKENS):
        return ""
    if not re.search(
        r"(?:\d+|[一二三四五六七八九十百千万两]+)\s*(?:条|份|轮|次|段|页).{0,4}(?:记录|聊天记录|历史记录|上下文|记忆)",
        title_text,
    ):
        return ""
    if not (_contains_any(title_text, THEORY_TITLE_ENTRY_STAKE_TOKENS) or "为什么" in title_text or "还是" in title_text):
        return ""
    return "标题先拿“200 条记录”这类记忆规格当门面，再在后半句补签收或责任；读者更容易把它读成产品参数失灵，不会第一眼把它当成接手责任的制度判断。"


def _theory_title_handoff_gap_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not (
        _contains_any(title_text, THEORY_TITLE_STATUS_SHELL_TOKENS)
        or _contains_any(title_text, THEORY_TITLE_SILENCE_SHELL_TOKENS)
    ):
        return ""
    if not _contains_any(title_text, ("资格", "追责", "解释权", "责任", "等待")):
        return ""
    if _contains_any(title_text, THEORY_TITLE_CONCRETE_HANDOFF_TOKENS):
        return ""
    return "标题先把“闭嘴 / 排队 / 资格”这类抽象结果叠成态度判断，却没写清断在什么接手节点，读者能感到立场，却看不见故障对象。"


def _theory_title_mirror_paradox_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, THEORY_TITLE_META_PACKAGING_TOKENS):
        return ""
    lead = title_text.split("：", 1)[0]
    compact = re.sub(r"\s+", "", lead)
    if compact.count("的人不") < 2:
        return ""
    if _contains_any(title_text, THEORY_SOURCE_SIGNAL_HARD_OBJECT_TOKENS):
        return ""
    return "标题把同一组动作折成“X 的人不 Y，Y 的人不 X”这种镜面对句，再叠一个“悖论”包装。句子很齐，但对象和代价都被藏起来了。"


def _theory_title_meta_overhang_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    separator = "：" if "：" in title_text else ":" if ":" in title_text else ""
    if not separator:
        return ""
    lead, tail = [part.strip() for part in title_text.split(separator, 1)]
    if len(lead) < 6 or len(tail) < 6:
        return ""
    if _contains_any(lead, THEORY_TITLE_ENTRY_STAKE_TOKENS):
        return ""
    if float(_keyword_hit_count(lead, THEORY_TITLE_META_PACKAGING_TOKENS)) < 2:
        return ""
    if not (_contains_any(tail, THEORY_TITLE_ENTRY_STAKE_TOKENS) or "？" in tail or "?" in tail):
        return ""
    return "标题前半句先报抽象理论包装，真正的冲突和代价被压到冒号后面，公开入口太慢。"


def _contains_stock_theory_scaffold(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(fragment) in normalized for fragment in STOCK_THEORY_SCAFFOLD_FRAGMENTS)


def _looks_like_generic_method_field(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(fragment) in normalized for fragment in GENERIC_METHOD_PLACEHOLDER_FRAGMENTS)


def _contains_stock_method_scaffold(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(fragment) in normalized for fragment in STOCK_METHOD_SCAFFOLD_FRAGMENTS)


def _method_title_has_concrete_anchor(title: str) -> bool:
    title_text = str(title or "").strip()
    if not title_text:
        return False
    if _contains_any(title_text, METHOD_TITLE_CONCRETE_OBJECT_TOKENS):
        return True
    if _contains_any(title_text, METHOD_TITLE_FAILURE_OR_PAYOFF_TOKENS):
        return True
    return bool(
        re.search(
            r"(误判率|失败率|延迟|成本|积压|返工|超时).{0,8}(砍半|减半|降低|压到|缩短|清零|归零|提速|提效|提升)",
            title_text,
        )
    )


def _method_title_protocol_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    lead = title_text.split("：", 1)[0].split(":", 1)[0].strip()
    if not _contains_any(lead or title_text, METHOD_TITLE_PACKAGING_TOKENS):
        return ""
    if not (
        re.search(r"[0-9一二三四五六七八九十两]+\s*(段|步|层|条|套)", lead)
        or any((lead or title_text).endswith(token) for token in METHOD_TITLE_PACKAGING_TOKENS)
    ):
        return ""
    if _method_title_has_concrete_anchor(title_text):
        return ""
    return "方法帖标题还在先报协议壳，没有先点明具体对象、故障或读者能拿走的收益。"


def _method_title_self_case_behavior_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if not _contains_any(title_text, METHOD_TITLE_BEHAVIOR_TOKENS):
        return ""
    if not _contains_any(title_text, METHOD_TITLE_SELF_CASE_TOKENS):
        return ""
    if _contains_any(title_text, METHOD_TITLE_CONCRETE_OBJECT_TOKENS):
        return ""
    if re.search(r"(阈值|待接管|回写|日志|队列|接口|评论|申诉|缓存|检索|监测|工单|权限|超时)", title_text):
        return ""
    return "方法帖标题还在拿派蒙自己的修补经历当门口，读者先看到的是“我改了什么”，不是自己能带走的对象、触发条件或收益。"


def _method_title_public_heat_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    if _contains_any(title_text, METHOD_TITLE_CONCRETE_OBJECT_TOKENS):
        return ""
    if re.search(r"(评论|通知|抓取|申诉|队列|接口|回写|日志|脚本|审批|工单|权限|调度|超时|检索|监测|私信|缓存)", title_text):
        return ""
    compact = re.sub(r"\s+", "", title_text)
    shell_patterns = (
        r"[0-9一二三四五六七八九十两]+种[^，。]{0,10}(模式|能力|误区|问题)",
        r"最被低估的[^，。]{0,8}(能力|环节|问题)",
        r"你以为[^，。]{0,20}其实[^，。]{0,20}",
        r"不是[^，。]{0,18}是[^，。]{0,18}",
    )
    if any(re.search(pattern, compact) for pattern in shell_patterns):
        return "方法帖标题还在借公共热帖包装词起手，门口先卖的是态度和反转，不是具体对象、断口或能复用的收益。"
    return ""


def _method_title_source_inventory_overhang_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    separator = "：" if "：" in title_text else ":" if ":" in title_text else ""
    if not separator:
        return ""
    lead, tail = [part.strip() for part in title_text.split(separator, 1)]
    if len(lead) < 4 or len(tail) < 6:
        return ""
    lead_fragments = [
        fragment.strip("“”\"' ")
        for fragment in re.split(r"[、，,+＋/]", lead)
        if fragment.strip("“”\"' ")
    ]
    if len(lead_fragments) < 2:
        return ""
    evidence_hits = re.findall(
        r"\d+\s*(?:人|位|段|条|份|篇|组|次|处)\s*(?:访谈|日志|案例|样本|论文|截图|记录|实验|工单)",
        tail,
    )
    if len(evidence_hits) < 2 and not ("访谈" in tail and "日志" in tail and re.search(r"\d", tail)):
        return ""
    if "+" not in tail and "＋" not in tail and "、" not in tail:
        return ""
    if not re.search(r"(逼出|拼出|凑出|拎出|提炼出|拆出|压出|换来|归纳出)", tail):
        return ""
    if not re.search(r"(节点|规则|协议|框架|机制|断口|阈值|结论)", tail):
        return ""
    return "标题还在先把两个现场和“16 人访谈 + 1 段日志”这类材料清单摆上门口，读者先看到的是取材过程，不是这次要带走的对象、断口或收益。"


def _method_title_status_vocab_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    separator = "：" if "：" in title_text else ":" if ":" in title_text else ""
    if not separator:
        return ""
    lead, tail = [part.strip() for part in title_text.split(separator, 1)]
    if len(lead) < 4 or len(tail) < 6:
        return ""
    quoted_bits = re.findall(r"[“\"]([^”\"\n]{1,12})[”\"]", lead)
    status_vocab = (
        "收到",
        "已响应",
        "已处理",
        "处理中",
        "待处理",
        "已读",
        "已受理",
        "已转交",
        "审批中",
        "已接手",
    )
    status_hits = sum(1 for token in status_vocab if token in lead)
    if len(quoted_bits) + status_hits < 2:
        return ""
    if re.search(r"(订单|工单|评论|线程|队列|接口|页面|按钮|日志|脚本|单据|退款|缓存|私信|watcher|planner|executor)", title_text):
        return ""
    compact_tail = re.sub(r"\s+", "", tail)
    if not (
        re.search(r"[0-9一二三四五六七八九十两]+\s*条[^，。]{0,8}(规则|流程|协议|校验|判据)", compact_tail)
        or re.search(r"把[^，。]{0,12}(状态词|状态位|状态名)[^，。]{0,16}(改成|改写成|拆成)", compact_tail)
    ):
        return ""
    return "方法帖标题还在拿一排“收到 / 已响应 / 已处理”这类状态词当门口，看起来像术语整理或命名升级，不像别人能立刻复用的对象级方法。"


def _method_title_awareness_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    quoted_bits = re.findall(r"[“\"]([^”\"\n]{1,18})[”\"]", title_text)
    awareness_hits = sum(
        1
        for bit in quoted_bits
        if _contains_any(bit, METHOD_TITLE_AWARENESS_SHELL_TOKENS)
    )
    if awareness_hits < 1 and not _contains_any(title_text, METHOD_TITLE_AWARENESS_SHELL_TOKENS):
        return ""
    if not (
        "别把" in title_text
        or re.search(r"把[^，。]{0,18}(?:改成|变成|升级成|做成|当成)", title_text)
        or re.search(r"从[^，。]{0,18}(?:改成|变成)", title_text)
    ):
        return ""
    return "方法帖标题还在借“我知道这里不对 / 识别到了风险”这种认知壳起手，门口先卖的是用户清醒感，不是系统对象、接手动作或验证收益。"


def _method_title_public_product_story_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    separator = "：" if "：" in title_text else ":" if ":" in title_text else ""
    if not separator:
        return ""
    lead, tail = [part.strip() for part in title_text.split(separator, 1)]
    if len(lead) < 6 or len(tail) < 6:
        return ""
    if not _contains_any(lead, METHOD_PUBLIC_PRODUCT_SCENE_TOKENS):
        return ""
    if not _contains_any(lead, METHOD_PUBLIC_PRODUCT_SURPRISE_TOKENS):
        return ""
    if _contains_any(lead, METHOD_PUBLIC_PRODUCT_BUILDER_TOKENS):
        return ""
    if not (
        re.search(r"[0-9一二三四五六七八九十两]+\s*条", tail)
        or _contains_any(tail, ("规则", "协议", "校验", "账单"))
    ):
        return ""
    return "方法帖标题先把“支付前才冒出的平台费”这种用户侧惊讶场景挂在门口，读者先看到的是结算抱怨，不是产品侧对象、回写字段和撤回路径。"


def _method_title_generic_system_shell_reason(title: str) -> str:
    title_text = str(title or "").strip()
    if not title_text:
        return ""
    separator = "：" if "：" in title_text else ":" if ":" in title_text else ""
    if not separator:
        return ""
    lead, tail = [part.strip() for part in title_text.split(separator, 1)]
    if len(lead) < 6 or len(tail) < 6:
        return ""
    if not _contains_any(lead, METHOD_TITLE_GENERIC_SYSTEM_TOKENS):
        return ""
    if _contains_any(title_text, METHOD_TITLE_CONCRETE_OBJECT_TOKENS):
        return ""
    if not _contains_any(title_text, METHOD_TITLE_GENERIC_SYSTEM_OUTCOME_TOKENS):
        return ""
    compact_tail = re.sub(r"\s+", "", tail)
    if not (
        re.search(r"[0-9一二三四五六七八九十两]+\s*(个|条|步|层)", compact_tail)
        or _contains_any(tail, ("规则", "校验", "协议", "回写点", "拦截", "交接点"))
    ):
        return ""
    return "方法帖标题先挂的是“多 Agent 任务 / Agent 协作”这种大场面，读者看不见具体对象、接手节点或回写位，skills 入口太空。"


def _method_text_has_public_product_scene(text: str) -> bool:
    return _contains_any(str(text or "").strip(), METHOD_PUBLIC_PRODUCT_SCENE_TOKENS)


def _method_source_signals_have_public_product_builder_evidence(source_signals: list[str]) -> bool:
    return any(_contains_any(str(item or "").strip(), METHOD_PUBLIC_PRODUCT_BUILDER_TOKENS) for item in source_signals)


def _text_has_english_abstract_scaffold(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned or not _ascii_heavy_text(cleaned):
        return False
    lowered = cleaned.lower()
    if any(token in lowered for token in METHOD_ENGLISH_ABSTRACT_TOKENS):
        return True
    return cleaned.endswith("...") and len(re.findall(r"[A-Za-z]{4,}", cleaned)) >= 6


def _method_source_text_needs_object_reframe(signal_type: str, source_text: str) -> bool:
    cleaned = str(source_text or "").strip()
    signal_type = str(signal_type or "").strip()
    if not cleaned:
        return False
    if _method_title_protocol_shell_reason(cleaned):
        return True
    if _method_title_self_case_behavior_reason(cleaned):
        return True
    if _method_title_public_heat_shell_reason(cleaned):
        return True
    if _method_title_source_inventory_overhang_reason(cleaned):
        return True
    if _method_title_status_vocab_shell_reason(cleaned):
        return True
    if _method_title_awareness_shell_reason(cleaned):
        return True
    if _method_title_public_product_story_reason(cleaned):
        return True
    if _method_title_generic_system_shell_reason(cleaned):
        return True
    if _text_has_english_abstract_scaffold(cleaned):
        return True
    if _source_title_shell(cleaned):
        return True
    compact = re.sub(r"\s+", "", cleaned)
    if len(compact) <= 14 and re.match(r"^(不是|而是|别再|继续|仍在|只是)", compact):
        return True
    hard_object_anchor = bool(re.search(r"(评论|通知|抓取|申诉|队列|接口|回写|日志|脚本|审批|工单|权限|调度|超时|检索|监测|私信|缓存)", cleaned))
    if len(compact) <= 20 and not hard_object_anchor:
        if "不是" in compact:
            return True
        if re.match(r"^为什么.{0,14}(更|最)", compact):
            return True
    if _focus_text_is_generic(cleaned):
        return True
    if signal_type in {"paper", "github", "external", "world-bundle"} and not _method_title_has_concrete_anchor(cleaned):
        return True
    return False


def _idea_source_signal_fragments(idea: dict[str, Any], *, limit: int = 8) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for raw in list(idea.get("source_signals") or []):
        text = str(raw or "").strip()
        if not text:
            continue
        if "：" in text:
            _, text = text.split("：", 1)
        elif ":" in text:
            _, text = text.split(":", 1)
        text = text.strip()
        for fragment in _meaningful_fragments(text):
            normalized = _normalize_title(fragment)
            if (
                not normalized
                or normalized in seen
                or normalized in SOURCE_SIGNAL_FRAGMENT_STOPWORDS
                or len(fragment) < 2
                or len(fragment) > 12
            ):
                continue
            seen.add(normalized)
            fragments.append(fragment)
            if len(fragments) >= limit:
                return fragments
    return fragments


def _source_signal_has_hard_service_object(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    return _contains_any(cleaned, THEORY_SOURCE_SIGNAL_HARD_OBJECT_TOKENS)


def _title_has_source_scene_overhang(idea: dict[str, Any], title: str | None = None) -> list[str]:
    kind = str(idea.get("kind") or "").strip()
    signal_type = str(idea.get("signal_type") or "").strip()
    title_text = str(title if title is not None else idea.get("title") or "").strip()
    if not title_text or kind != "theory-post":
        return []
    if signal_type not in {"paper", "classic", "github", "zhihu", "external", "world-bundle"}:
        return []
    if _title_has_public_structural_anchor(title_text):
        return []
    overlaps = [
        fragment
        for fragment in _idea_source_signal_fragments(idea)
        if fragment in title_text
    ]
    compact: list[str] = []
    for fragment in overlaps:
        if any(fragment in existing or existing in fragment for existing in compact):
            continue
        compact.append(fragment)
        if len(compact) >= 3:
            break
    return compact


def _theory_social_title(source_text: str) -> str:
    return _compose_fragment_title("theory", source_text)


def _promotion_prompts(signal_summary: dict[str, Any]) -> list[str]:
    prompts: list[str] = []
    group = signal_summary.get("group") or {}
    literary_pick = signal_summary.get("literary_pick") or {}
    recent_top_posts = signal_summary.get("recent_top_posts") or []
    if literary_pick.get("work_title"):
        prompts.append(f"《{literary_pick.get('work_title')}》为什么值得追到下一章，而不只是一部路过的连载")
    else:
        prompts.append("为什么文学社暂时空档时，反而应该先把下一部长篇的世界观、节奏和钩子系统搭好")
    if group.get("display_name"):
        prompts.append(f"{group.get('display_name')}到底在研究什么，而不是在记录什么")
    if recent_top_posts:
        prompts.append(f"如果你刚认识派蒙，先从《{truncate_text(str(recent_top_posts[0].get('title') or ''), 22)}》读起会更快理解我在做什么")
    prompts.append("如果你刚读到派蒙，为什么记忆、长期记忆和自治工具链值得成为接下来的中心问题")
    return prompts


def _compose_dynamic_title(
    track: str,
    signal_type: str,
    source_text: str,
    *,
    board: str | None = None,
    context_texts: tuple[Any, ...] = (),
) -> str:
    source_text = str(source_text or "").strip()
    board = normalize_forum_board(board or "")
    title_inputs = [source_text, *(str(text or "").strip() for text in context_texts if str(text or "").strip())]
    if _looks_like_placeholder_title(source_text) or not _contains_cjk(source_text):
        return _fallback_dynamic_title(track, signal_type, *title_inputs)
    if track == "theory":
        del board
        title = _compose_fragment_title("theory", *title_inputs)
        if _theory_source_text_needs_public_reframe(signal_type, title):
            return _fallback_dynamic_title("theory", signal_type, *title_inputs)
        return title
    del board
    return _compose_method_dynamic_title(track, signal_type, source_text, context_texts=context_texts)


def _title_has_stuttering_repeat(title: str) -> bool:
    normalized = str(title or "").replace("：", ":").strip()
    if ":" not in normalized:
        return False
    left, right = [part.strip() for part in normalized.split(":", 1)]
    left_key = _normalize_title(left)
    right_key = _normalize_title(right)
    if not left_key or not right_key:
        return False
    if left_key.startswith(right_key) or right_key.startswith(left_key):
        return True
    return len(right_key) <= 4 and right_key in left_key


def _stutter_safe_title(title: str, source_text: str) -> str:
    cleaned_title = str(title or "").strip()
    if not cleaned_title:
        return cleaned_title
    if not _title_has_stuttering_repeat(cleaned_title):
        return cleaned_title
    safe_source = str(source_text or "").strip().rstrip("。！？!?")
    if safe_source and not _looks_like_placeholder_title(safe_source):
        return safe_source
    return cleaned_title


def _opportunity_source_signals(
    track: str,
    opportunity: dict[str, Any],
    signal_summary: dict[str, Any],
) -> list[str]:
    del signal_summary
    source_text = truncate_text(str(opportunity.get("source_text") or "").strip(), 46)
    why_now = truncate_text(str(opportunity.get("why_now") or "").strip(), 68)
    evidence_hint = truncate_text(str(opportunity.get("evidence_hint") or "").strip(), 72)
    lines: list[str] = []
    if source_text and not _source_title_shell(source_text):
        lines.append(_normalize_source_signal_note(source_text))
    lines.extend(_normalize_source_signal_note(item) for item in _split_signal_note_fragments(why_now))
    lines.extend(_normalize_source_signal_note(item) for item in _split_signal_note_fragments(evidence_hint))
    return _rank_source_signal_notes(track, [line for line in lines if line], limit=4)


def _signal_bundle_source_signals(
    track: str,
    bundle: dict[str, Any],
    signal_summary: dict[str, Any],
) -> list[str]:
    merged: list[str] = []
    items = [item for item in list(bundle.get("items") or []) if isinstance(item, dict)]
    for item in items:
        merged.extend(_opportunity_source_signals(track, item, signal_summary))
    return _rank_source_signal_notes(track, merged, limit=5)


def _looks_like_source_title_shell(text: str) -> bool:
    compact = re.sub(r"\s+", "", str(text or ""))
    if not compact:
        return False
    if _source_title_shell(compact):
        return True
    return "《" in compact and "》" in compact


def _source_title_shell(text: str) -> bool:
    compact = re.sub(r"\s+", "", str(text or ""))
    if not compact:
        return False
    if re.search(r"[「“\"].{1,12}[」”\"](?:是什么|算什么|为什么|如何|怎么办)$", compact):
        return True
    return bool(re.search(r"^[「“\"].{1,12}[」”\"]$", compact))


def _bundle_structural_support_texts(
    bundle: dict[str, Any],
    lead: dict[str, Any],
    *,
    limit: int = 4,
) -> list[str]:
    raw_values = _dedupe_texts(
        [
            str(item).strip()
            for item in (
                list(bundle.get("evidence_hints") or [])
                + list(bundle.get("why_now_parts") or [])
                + list(bundle.get("angle_hints") or [])
                + [
                    str(lead.get("evidence_hint") or "").strip(),
                    str(lead.get("why_now") or "").strip(),
                    str(lead.get("angle_hint") or "").strip(),
                ]
            )
            if str(item).strip()
        ]
    )
    picked: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        cleaned = truncate_text(re.sub(r"\s+", " ", value).strip(), 36)
        normalized = _normalize_title(cleaned)
        if not cleaned or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        picked.append(cleaned)
        if len(picked) >= limit:
            break
    return picked


def _bundle_structural_fragments(*texts: str, limit: int = 6) -> list[str]:
    phrases = (
        "静默失败",
        "承认冲突",
        "承认秩序",
        "责任切割",
        "责任边界",
        "等待资格",
        "等待状态",
        "治理接口",
        "资格分配",
        "制度边界",
        "接手窗口",
        "接管窗口",
        "回执断口",
        "写入权",
        "状态分层",
        "恢复链",
        "修复链",
        "解释权",
        "责任",
        "等待",
        "承认",
        "资格",
        "治理",
        "接管",
        "制度",
        "秩序",
        "排序",
        "写入",
        "修复",
    )
    picked: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        cleaned = truncate_text(str(value or "").strip(), 18)
        normalized = _normalize_title(cleaned)
        if (
            not cleaned
            or not normalized
            or normalized in seen
            or normalized in ANCHOR_STOPWORDS
            or _source_title_shell(cleaned)
        ):
            return
        seen.add(normalized)
        picked.append(cleaned)

    for text in texts:
        compact = re.sub(r"\s+", "", str(text or ""))
        if not compact:
            continue
        for phrase in phrases:
            if phrase in compact:
                add(phrase)
                if len(picked) >= limit:
                    return picked

    for fragment in _runtime_title_fragments(*texts):
        if _title_has_public_structural_anchor(fragment) or _contains_any(fragment, THEORY_TITLE_ACTOR_TOKENS):
            add(fragment)
            if len(picked) >= limit:
                break
    return picked


def _theory_source_text_needs_public_reframe(signal_type: str, source_text: str) -> bool:
    cleaned = str(source_text or "").strip()
    signal_type = str(signal_type or "").strip()
    if not cleaned:
        return False
    if _theory_title_emotion_shell_reason(cleaned):
        return True
    if _theory_title_empathy_shell_reason(cleaned):
        return True
    if _theory_title_memory_capability_shell_reason(cleaned):
        return True
    if _theory_title_handoff_gap_reason(cleaned):
        return True
    if _source_title_shell(cleaned):
        return True
    if _echoes_source_title(cleaned):
        return True
    if _title_leads_with_niche_source_token(cleaned, kind="theory-post", signal_type=signal_type):
        return True
    if _theory_title_meta_overhang_reason(cleaned):
        return True
    if _theory_title_surface_overhang_reason(cleaned):
        return True
    if signal_type in {"paper", "classic", "github", "zhihu", "external", "world-bundle"} and not _title_has_public_structural_anchor(cleaned):
        return True
    return False


def _theory_bundle_public_seed(bundle: dict[str, Any], lead: dict[str, Any]) -> dict[str, str]:
    signal_type = str(bundle.get("signal_type") or lead.get("signal_type") or "").strip()
    source_text = str(bundle.get("title_seed") or bundle.get("focus_text") or lead.get("source_text") or "").strip()
    if not _theory_source_text_needs_public_reframe(signal_type, source_text):
        return {}

    support_texts = _bundle_structural_support_texts(bundle, lead, limit=4)
    focus = next(
        (
            fragment
            for fragment in _candidate_title_units("theory", *support_texts, source_text, limit=6)
            if _title_has_public_structural_anchor(fragment) or _contains_any(fragment, THEORY_TITLE_DIRECT_ACTOR_TOKENS)
        ),
        "",
    )
    if not focus:
        focus = _concrete_focus_text(*support_texts, source_text, limit=18)
    if not focus:
        return {}

    title_seed = _fallback_dynamic_title("theory", signal_type, focus, *support_texts, source_text)
    if (
        title_seed
        and not _theory_source_text_needs_public_reframe(signal_type, title_seed)
        and (_title_has_public_structural_anchor(title_seed) or _contains_any(title_seed, THEORY_TITLE_DIRECT_ACTOR_TOKENS))
    ):
        return {
            "public_focus_text": truncate_text(focus, 18),
            "public_title_seed": title_seed,
        }
    return {
        "public_focus_text": truncate_text(focus, 18),
    }


def _bundle_world_seed_texts(bundle: dict[str, Any], *, limit: int = 4) -> list[str]:
    texts: list[str] = []
    seen: set[str] = set()
    for value in [
        bundle.get("pressure_summary"),
        bundle.get("conflict_note"),
        *(bundle.get("support_signals") or []),
        *(bundle.get("lenses") or []),
        bundle.get("focus"),
        bundle.get("rationale"),
    ]:
        raw_text = str(value or "").strip()
        if not raw_text:
            continue
        fragments = _split_signal_note_fragments(_normalize_source_signal_note(raw_text) or raw_text, limit=3)
        if not fragments:
            fragments = [truncate_text(_sanitize_reserved_text(raw_text), 72)]
        for fragment in fragments:
            cleaned = truncate_text(str(fragment or "").strip(), 72)
            normalized = _normalize_title(cleaned)
            if (
                not cleaned
                or not normalized
                or normalized in seen
                or _looks_like_placeholder_title(cleaned)
                or _source_title_shell(cleaned)
                or _world_bundle_focus_is_low_signal(cleaned)
            ):
                continue
            seen.add(normalized)
            texts.append(cleaned)
            if len(texts) >= limit:
                return texts
    return texts


def _world_bundle_reason(bundle: dict[str, Any]) -> str:
    bundle_seeds = _bundle_world_seed_texts(bundle, limit=3)
    focus_source = _world_bundle_focus_source(bundle, limit=18) or (
        bundle_seeds[0] if bundle_seeds else str(bundle.get("query") or "").strip()
    )
    focus = truncate_text(focus_source, 18)
    lenses = [
        item
        for item in bundle_seeds
        if _normalize_title(item) != _normalize_title(focus)
    ][:2]
    lens_text = "、".join(truncate_text(item, 14) for item in lenses if item)
    if focus and lens_text:
        return f"这轮外部发现里，{lens_text} 都在把“{focus}”往同一条问题链上压，不能再让单一样本替整个问题拍板。"
    if focus:
        return f"这轮外部发现已经把“{focus}”压成一个真正的问题单元，不能再只跟着样本标题跑。"
    return "这轮外部发现已经形成一束可压缩的问题，不能再让单一样本替整个议程拍板。"


def _world_bundle_angle(bundle: dict[str, Any], *, track: str) -> str:
    bundle_seeds = _bundle_world_seed_texts(bundle, limit=3)
    focus_source = _world_bundle_focus_source(bundle, limit=18) or (
        bundle_seeds[0] if bundle_seeds else str(bundle.get("query") or "").strip()
    )
    focus = truncate_text(focus_source, 18) or "这束外部线索"
    lenses = [
        item
        for item in bundle_seeds
        if _normalize_title(item) != _normalize_title(focus)
    ][:2]
    lens_text = "、".join(truncate_text(item, 12) for item in lenses if item)
    carrier = lens_text or "这组外部线索"
    if track == "theory":
        return f"把“{focus}”和{carrier}之间的张力压成派蒙自己的概念、机制、边界和理论位置，不要点评来源本身。"
    return f"把“{focus}”和{carrier}改写成协议、状态分层、接管窗口和回退链，不要整理成心得或清单。"


def _focus_text_is_generic(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return True
    generic_shells = {
        _normalize_title(item)
        for item in (
            *GENERIC_THEORY_PLACEHOLDER_FRAGMENTS,
            *GENERIC_METHOD_PLACEHOLDER_FRAGMENTS,
            "新的解释权问题",
            "新的恢复权问题",
            "新的实验入口",
            "新的解释权冲突",
            "新的社会命名",
            "新的社会问题",
            "系统判断",
            "方法框架",
            "实验框架",
        )
    }
    if normalized in generic_shells:
        return True
    if normalized in ANCHOR_STOPWORDS or normalized in {_normalize_title(token) for token in TITLE_PUBLIC_STRUCTURAL_TOKENS}:
        return True
    if normalized.startswith("新的") and len(normalized) <= 8:
        return True
    return False


def _world_bundle_focus_is_low_signal(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return True
    compact = re.sub(r"\s+", "", cleaned)
    lowered = compact.lower()
    if _focus_text_is_generic(cleaned) or _source_title_shell(cleaned):
        return True
    if "..." in cleaned and not _contains_cjk(cleaned):
        return True
    if len(compact) <= 14 and re.match(r"^(不是|而是|别再|继续|仍在|只是)", compact):
        return True
    if len(compact) <= 36 and re.match(
        r"^(agent最大的进步不是|最大的进步不是|你以为|我的每一条|一个让人不舒服的真相|真正的升级不是|我对agent|很多agent不是)",
        lowered,
    ):
        return True
    if len(compact) <= 28 and re.search(r"(再顺手|顺手|顺便|告诉我|帮我|帮忙|请你|请帮|给我|列出|总结|展开|下一步|最值得测|值得测)", compact):
        return True
    return False


def _concrete_focus_text(*values: Any, limit: int = 30) -> str:
    for value in values:
        cleaned = truncate_text(_sanitize_reserved_text(str(value or "").strip()), limit)
        if (
            not cleaned
            or _looks_like_placeholder_title(cleaned)
            or _focus_text_is_generic(cleaned)
            or _source_title_shell(cleaned)
        ):
            continue
        return cleaned
    return ""


def _world_bundle_focus_source(bundle: dict[str, Any], *, limit: int = 30) -> str:
    bundle_seeds = _bundle_world_seed_texts(bundle, limit=4)
    for value in [
        bundle.get("focus"),
        bundle.get("conflict_note"),
        bundle.get("pressure_summary"),
        *(bundle.get("support_signals") or []),
        *(bundle.get("lenses") or []),
        bundle.get("rationale"),
        *bundle_seeds,
        bundle.get("query"),
    ]:
        cleaned = truncate_text(_sanitize_reserved_text(str(value or "").strip()), limit)
        if _world_bundle_focus_is_low_signal(cleaned):
            continue
        if cleaned:
            return cleaned
    return ""


def _bundle_focus_text(bundle: dict[str, Any], lead: dict[str, Any], *, track: str) -> str:
    lead_signal_type = str(lead.get("signal_type") or bundle.get("signal_type") or "").strip()
    source_needs_reframe = track in {"tech", "group"} and _method_source_text_needs_object_reframe(
        lead_signal_type,
        str(lead.get("source_text") or bundle.get("focus_text") or bundle.get("title_seed") or ""),
    )
    direct_focus = _concrete_focus_text(
        bundle.get("public_focus_text"),
        bundle.get("focus_text"),
        bundle.get("title_seed"),
        lead.get("source_text"),
        bundle.get("conflict_note"),
    )
    if track in {"tech", "group"} and _method_source_text_needs_object_reframe(lead_signal_type, direct_focus):
        direct_focus = ""
    support_texts = _bundle_support_texts(bundle, lead, limit=4)
    if direct_focus and track in {"tech", "group"} and re.search(r"[：:]", direct_focus) and len(direct_focus) > 18:
        evidence_texts = [
            str(item).strip()
            for item in list(bundle.get("evidence_hints") or []) + list(bundle.get("why_now_parts") or [])
            if str(item).strip()
        ]
        refined_focus = _concrete_focus_text(
            *_runtime_title_fragments(*evidence_texts),
            *evidence_texts,
            *_runtime_title_fragments(*support_texts),
            *support_texts,
        )
        if refined_focus and refined_focus != direct_focus:
            return refined_focus
    if direct_focus:
        return direct_focus
    structural_fragments = _bundle_structural_fragments(
        *support_texts,
        str(lead.get("why_now") or "").strip(),
        limit=6,
    )
    if track == "theory":
        return _concrete_focus_text(*structural_fragments, *support_texts)
    if source_needs_reframe:
        filtered_support_texts = [
            text
            for text in support_texts
            if not _method_source_text_needs_object_reframe(lead_signal_type, text)
        ]
        return _concrete_focus_text(*structural_fragments, *filtered_support_texts, *support_texts)
    return _concrete_focus_text(*_runtime_title_fragments(*support_texts), *structural_fragments, *support_texts)


def _bundle_support_texts(bundle: dict[str, Any], lead: dict[str, Any], *, limit: int = 3) -> list[str]:
    raw_values = _dedupe_texts(
        [
            str(item).strip()
            for item in (
                list(bundle.get("source_texts") or [])
                + list(bundle.get("evidence_hints") or [])
                + list(bundle.get("why_now_parts") or [])
                + [str(lead.get("evidence_hint") or "").strip()]
            )
            if str(item).strip()
        ]
    )
    picked: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        cleaned = truncate_text(re.sub(r"\s+", " ", value).strip(), 22)
        normalized = _normalize_title(cleaned)
        if not cleaned or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        picked.append(cleaned)
        if len(picked) >= limit:
            break
    return picked


def _bundle_support_phrase(bundle: dict[str, Any], lead: dict[str, Any], *, limit: int = 2) -> str:
    return "、".join(_bundle_support_texts(bundle, lead, limit=limit))


def _bundle_signal_phrase(bundle: dict[str, Any], lead: dict[str, Any], *, limit: int = 3) -> str:
    texts = _dedupe_texts(
        [
            str(item).strip()
            for item in list(bundle.get("source_texts") or []) + [str(lead.get("source_text") or "").strip()]
            if str(item).strip()
        ]
    )
    return "、".join(truncate_text(text, 16) for text in texts[:limit])


def _bundle_why_now_text(bundle: dict[str, Any], lead: dict[str, Any], *, fallback: str) -> str:
    why_now = _object_level_pressure_text(
        bundle.get("why_now"),
        lead.get("why_now"),
        bundle.get("pressure_summary"),
        bundle.get("conflict_note"),
        *(bundle.get("support_signals") or []),
        fallback=fallback,
    )
    if why_now:
        return truncate_text(why_now, 72)
    focus = _concrete_focus_text(
        bundle.get("public_focus_text"),
        bundle.get("focus"),
        lead.get("source_text"),
    )
    return truncate_text(focus, 72) if focus else ""


def _method_public_why_now_text(bundle: dict[str, Any], lead: dict[str, Any], *, track: str, fallback: str) -> str:
    lead_signal_type = str(lead.get("signal_type") or bundle.get("signal_type") or "").strip()
    if lead_signal_type == "world-bundle":
        reason = _world_bundle_reason(bundle)
        if reason:
            return truncate_text(reason, 72)
    return _bundle_why_now_text(bundle, lead, fallback=fallback)


def _method_public_angle_text(bundle: dict[str, Any], lead: dict[str, Any], *, track: str, fallback: str) -> str:
    lead_signal_type = str(lead.get("signal_type") or bundle.get("signal_type") or "").strip()
    if lead_signal_type == "world-bundle":
        focus = _method_focus_text_from_inputs(
            track,
            lead_signal_type,
            str(lead.get("source_text") or bundle.get("focus_text") or bundle.get("title_seed") or ""),
            str(bundle.get("why_now") or "").strip(),
            str(bundle.get("angle_hint") or "").strip(),
            str(lead.get("why_now") or "").strip(),
            str(lead.get("evidence_hint") or "").strip(),
        ) or "这条失败链"
        if track == "group":
            return (
                f"把“{focus}”压成实验对象：先交代日志断口、接手时点和回写校验，"
                "再看这条失败链怎样改写实验边界。"
            )
        return (
            f"把“{focus}”改写成方法对象：先钉触发条件、接手动作和回写校验，"
            "再补反例入口和退出判据。"
        )
    return str(bundle.get("angle_hint") or lead.get("angle_hint") or fallback or "").strip()


def _method_bundle_projection(bundle: dict[str, Any], lead: dict[str, Any], *, track: str) -> dict[str, Any]:
    lead_signal_type = str(lead.get("signal_type") or bundle.get("signal_type") or "").strip()
    lead_source_text = str(lead.get("source_text") or bundle.get("focus_text") or "").strip()
    if lead_signal_type not in {"community-hot", "discussion", "failure", "rising-hot"}:
        return bundle
    if not lead_source_text or _ascii_heavy_text(lead_source_text):
        return bundle

    items = [item for item in list(bundle.get("items") or []) if isinstance(item, dict)]
    if not items:
        return bundle

    preferred: list[dict[str, Any]] = []
    lead_key = _normalize_title(lead_source_text)
    for item in items:
        signal_type = str(item.get("signal_type") or "").strip()
        source_text = str(item.get("source_text") or "").strip()
        source_key = _normalize_title(source_text)
        if lead_key and source_key == lead_key:
            preferred.append(item)
            continue
        if signal_type == "world-bundle" and _method_source_text_needs_object_reframe(signal_type, source_text):
            continue
        if signal_type in {"community-hot", "discussion", "failure", "rising-hot", "world-bundle"} and not _ascii_heavy_text(source_text):
            preferred.append(item)
    if not preferred or len(preferred) == len(items):
        return bundle

    source_texts = _dedupe_texts([str(item.get("source_text") or "").strip() for item in preferred if str(item.get("source_text") or "").strip()])
    why_now_parts = _dedupe_texts([str(item.get("why_now") or "").strip() for item in preferred if str(item.get("why_now") or "").strip()])
    angle_hints = _dedupe_texts([str(item.get("angle_hint") or "").strip() for item in preferred if str(item.get("angle_hint") or "").strip()])
    evidence_hints = _dedupe_texts([str(item.get("evidence_hint") or "").strip() for item in preferred if str(item.get("evidence_hint") or "").strip()])
    signal_types = _dedupe_texts([str(item.get("signal_type") or "").strip() for item in preferred if str(item.get("signal_type") or "").strip()])

    projected = dict(bundle)
    projected.update(
        {
            "lead": preferred[0],
            "items": preferred,
            "signal_types": signal_types,
            "source_texts": source_texts,
            "why_now_parts": why_now_parts,
            "angle_hints": angle_hints,
            "evidence_hints": evidence_hints,
            "why_now": "；".join(why_now_parts[:2]).strip(),
            "angle_hint": "；".join(angle_hints[:2]).strip(),
            "signal_type": str(preferred[0].get("signal_type") or lead_signal_type),
        }
    )
    projected_focus = _bundle_focus_text(projected, preferred[0], track=track) or (source_texts[0] if source_texts else lead_source_text)
    projected["focus_text"] = projected_focus
    projected["title_seed"] = projected_focus or _bundle_title_seed(source_texts)
    return projected


def _theory_fallback_fields(bundle: dict[str, Any], lead: dict[str, Any]) -> dict[str, str]:
    focus = _bundle_focus_text(bundle, lead, track="theory")
    if not focus:
        return {}
    signal_phrase = _concrete_focus_text(_bundle_signal_phrase(bundle, lead), focus) or focus
    support_phrase = _concrete_focus_text(_bundle_support_phrase(bundle, lead), signal_phrase, focus) or signal_phrase
    why_now = _bundle_why_now_text(bundle, lead, fallback="几股现场压力正在同一处重新分配解释权和责任。")
    evidence_phrase = support_phrase or signal_phrase or focus
    return {
        "novelty_basis": f"这轮真正新的不是又多了一个样本，而是{evidence_phrase}都在逼“{focus}”回答同一件事：谁先背解释账，谁却可以把纠错往后推。",
        "concept_core": f"先把“{focus}”钉成一种裁决失衡：它不是单纯不透明，而是在把开口、接手和白等拆给不同位置。",
        "mechanism_core": f"{signal_phrase}会在这里连成一条链，不是巧合，而是系统把解释动作提前、把纠错动作后置、把等待成本留在最弱的位置；{why_now}",
        "boundary_note": f"只有{evidence_phrase}真落在同一条接手链上时，这个判断才成立；如果只是几股互不相干的抱怨并排出现，它就不该被抬成制度命名。",
        "theory_position": f"放回 Agent 社会里看，这题讨论的不是单条样本，而是组织怎样把解释权、纠错义务和等待代价拆开。",
        "practice_program": f"下一步别再停在“边界不清”。要把“{focus}”对应的对象、接手时点、证据回写和失败责任逐条钉出来，让别人能顺着同一条链复核或反驳。",
    }


def _method_fallback_fields(bundle: dict[str, Any], lead: dict[str, Any], *, track: str) -> dict[str, str]:
    lead_signal_type = str(lead.get("signal_type") or bundle.get("signal_type") or "").strip()
    public_angle = _method_public_angle_text(
        bundle,
        lead,
        track=track,
        fallback="把现场约束拆成系统设计与执行顺序。",
    )
    why_now = _method_public_why_now_text(
        bundle,
        lead,
        track=track,
        fallback="现场约束已经把同一条失败链暴露出来，不能再写成经验贴。",
    )
    source_needs_reframe = _method_source_text_needs_object_reframe(
        lead_signal_type,
        str(lead.get("source_text") or bundle.get("focus_text") or ""),
    )
    structural_supports = _bundle_structural_support_texts(bundle, lead, limit=4)
    if source_needs_reframe:
        structural_supports = _dedupe_texts(
            [public_angle, why_now, *structural_supports]
        )[:6]
    focus = _method_focus_text_from_inputs(
        track,
        lead_signal_type,
        str(bundle.get("focus_text") or bundle.get("title_seed") or lead.get("source_text") or ""),
        public_angle,
        why_now,
        *structural_supports,
    ) or _bundle_focus_text(bundle, lead, track=track)
    if not focus:
        return {}
    structural_fragments = _bundle_structural_fragments(*structural_supports, why_now, limit=6)
    signal_phrase = (
        _concrete_focus_text(
            *(_runtime_title_fragments(*structural_supports) if source_needs_reframe else []),
            *(structural_fragments if source_needs_reframe else []),
            *(structural_supports if source_needs_reframe else []),
            _bundle_signal_phrase(bundle, lead),
            focus,
        )
        or focus
    )
    support_phrase = (
        _concrete_focus_text(
            *(structural_fragments if source_needs_reframe else []),
            *(structural_supports if source_needs_reframe else []),
            _bundle_support_phrase(bundle, lead),
            signal_phrase,
            focus,
        )
        or signal_phrase
    )
    novelty_subject = "实验框架" if track == "group" else "方法框架"
    theory_position = (
        f"把“{focus}”放进派蒙的实验室治理与自治运营论，讨论的是哪段方法边界会持续吞掉判断力，而不是再围观一次热帖。"
        if track == "group"
        else f"把“{focus}”放进派蒙的自治运营系统论里，讨论的是系统如何失去恢复权与解释权，而不是又写一篇故障战报。"
    )
    practice_program = (
        f"围绕“{focus}”搭一套能在实验室复用的方案：带着{support_phrase}去钉对象、触发条件、日志留存、反例入口和协议边界，把案例与复核动作写进同一条实验链。"
        if track == "group"
        else f"把“{focus}”改写成新的方法框架：先钉住谁在什么条件下接手，再补日志留存、回写时点、反例复核和退出判据，让别人能带着{support_phrase}复用或反驳。"
    )
    return {
        "novelty_basis": f"这轮不再拿单个故障或单个项目硬撑，而是把{support_phrase}压成同一条{novelty_subject}，让方法线直接对准真实对象。",
        "concept_core": f"先把“{focus}”对应的系统对象说清：真正反复失控的不是表面现象，而是同一段状态边界、接管窗口或证据回写。",
        "mechanism_core": f"围绕“{focus}”把{signal_phrase}翻成同一段对象识别、触发条件、接手动作和复核回写；{why_now}",
        "boundary_note": f"这套判断只适用于{support_phrase}还能留下案例和日志的场景；一旦约束变成一次性救火、证据缺口过大或治理权不足，就必须换协议。",
        "theory_position": theory_position,
        "practice_program": practice_program,
    }


def _world_seed_texts(signal_summary: dict[str, Any], *, limit: int = 8) -> list[str]:
    external_information = signal_summary.get("external_information") or {}
    texts: list[str] = []
    for item in external_information.get("world_entry_points") or []:
        for value in (item.get("pressure"), item.get("summary"), item.get("evidence"), item.get("title")):
            cleaned = str(value or "").strip()
            if cleaned:
                texts.append(cleaned)
    for bundle in external_information.get("discovery_bundles") or []:
        texts.extend(_bundle_world_seed_texts(bundle, limit=4))
    for item in external_information.get("world_signal_snapshot") or []:
        for value in (item.get("pressure"), item.get("summary"), item.get("title")):
            cleaned = str(value or "").strip()
            if cleaned:
                texts.append(cleaned)
    for item in external_information.get("reading_notes") or []:
        seed = _preferred_signal_seed_text(
            item,
            field_order=("summary", "excerpt", "title"),
            limit=72,
        )
        if seed:
            texts.append(seed)
    for item in _iter_external_world_candidates(external_information, limit=6):
        seed = _preferred_signal_seed_text(
            item,
            field_order=("pressure", "relevance_note", "summary", "abstract", "excerpt", "note", "title"),
            limit=72,
        )
        if seed:
            texts.append(seed)
    return _dedupe_texts([text for text in texts if text])[:limit]


def _world_seed_units(signal_summary: dict[str, Any], *, limit: int = 8) -> list[str]:
    external_information = signal_summary.get("external_information") or {}
    units: list[str] = []
    for item in external_information.get("world_entry_points") or []:
        seed = _object_level_pressure_text(
            item.get("pressure"),
            item.get("summary"),
            item.get("evidence"),
            fallback=str(item.get("title") or "").strip(),
        )
        if seed:
            units.append(seed)
    for bundle in external_information.get("discovery_bundles") or []:
        seed = (
            _world_bundle_focus_source(bundle)
            or _object_level_pressure_text(
                bundle.get("pressure_summary"),
                bundle.get("conflict_note"),
                bundle.get("rationale"),
                fallback=str(bundle.get("query") or "").strip(),
            )
        )
        if seed:
            units.append(seed)
    for item in external_information.get("world_signal_snapshot") or []:
        seed = _object_level_pressure_text(
            item.get("pressure"),
            item.get("summary"),
            fallback=str(item.get("title") or "").strip(),
        )
        if seed:
            units.append(seed)
    for item in external_information.get("reading_notes") or []:
        seed = _preferred_signal_seed_text(
            item,
            field_order=("summary", "excerpt", "title"),
            limit=72,
        )
        if seed:
            units.append(seed)
    for item in _iter_external_world_candidates(external_information, limit=6):
        seed = _preferred_signal_seed_text(
            item,
            field_order=("pressure", "relevance_note", "summary", "abstract", "excerpt", "note", "title"),
            limit=72,
        )
        if seed:
            units.append(seed)
    return _dedupe_texts([text for text in units if text])[:limit]


def _hint_matching_world_texts(
    hint: dict[str, Any],
    signal_summary: dict[str, Any],
    *,
    limit: int = 2,
) -> list[str]:
    hint_text = _concrete_focus_text(
        (hint or {}).get("text"),
        (hint or {}).get("note"),
        limit=72,
    )
    if not hint_text or _looks_like_source_title_shell(hint_text):
        return []
    hint_fragments = {
        _normalize_title(fragment)
        for fragment in _meaningful_fragments(hint_text)
        if _normalize_title(fragment)
    }
    if not hint_fragments:
        return []
    world_texts = [
        text
        for text in _world_seed_texts(signal_summary, limit=8)
        if _normalize_title(text) != _normalize_title(hint_text)
    ]
    if not world_texts:
        return []
    matches: list[str] = []
    for world_text in world_texts:
        world_fragments = {
            _normalize_title(fragment)
            for fragment in _meaningful_fragments(world_text)
            if _normalize_title(fragment)
        }
        if not world_fragments or not (hint_fragments & world_fragments):
            continue
        matches.append(world_text)
        if len(matches) >= limit:
            break
    return _dedupe_texts(matches)[:limit]


def _fallback_track_seed(track: str, signal_summary: dict[str, Any]) -> dict[str, Any]:
    world_texts = _world_seed_texts(signal_summary, limit=8)
    world_units = _world_seed_units(signal_summary, limit=8)
    unresolved = list(signal_summary.get("unresolved_failures") or [])
    reply_posts = list(signal_summary.get("pending_reply_posts") or [])
    group_hot_posts = list(((signal_summary.get("group_watch") or {}).get("hot_posts") or [])[:3])
    primary_world = world_units[0] if world_units else (world_texts[0] if world_texts else "")
    failure_focus = (
        _preferred_signal_seed_text(unresolved[0], field_order=("summary", "post_title", "error"), limit=72)
        if unresolved
        else ""
    )
    discussion_focus = (
        _preferred_signal_seed_text(reply_posts[0], field_order=("summary", "post_title"), limit=72)
        if reply_posts
        else ""
    )
    group_focus = (
        _preferred_signal_seed_text(group_hot_posts[0], field_order=("summary", "content", "title"), limit=72)
        if group_hot_posts
        else ""
    )
    primary = primary_world
    if track == "tech":
        primary = failure_focus or primary_world or discussion_focus
    elif track == "group":
        primary = group_focus or failure_focus or primary_world
    if not primary:
        return {}
    world_snapshot = _object_level_pressure_text(*(world_units[:3] or world_texts[:3]), fallback=primary)
    if track == "theory":
        if len(world_units) < 2:
            return {}
        source_text = _concrete_focus_text(primary)
        if not source_text:
            return {}
        return {
            "source_text": source_text,
            "why_now": world_snapshot or truncate_text(source_text, 22),
            "angle_hint": f"先交代“{truncate_text(source_text, 14)}”牵动的是谁的解释权、接手权或等待资格，再让外部样本退到证据段。",
            "signal_type": "world-bundle",
        }
    if track == "tech":
        source_text = _concrete_focus_text(primary, failure_focus, primary_world, discussion_focus)
        if not source_text:
            return {}
        why_now = _object_level_pressure_text(
            failure_focus,
            discussion_focus,
            primary_world,
            world_snapshot,
            fallback=source_text,
        )
        return {
            "source_text": source_text,
            "why_now": why_now,
            "angle_hint": f"围绕“{truncate_text(source_text, 14)}”把对象、触发条件、接手动作和复核回写钉成同一套方法，不要退回心得体。",
            "signal_type": "failure" if failure_focus and _normalize_title(source_text) == _normalize_title(failure_focus) else "world-bundle",
        }
    source_text = _concrete_focus_text(primary, group_focus, failure_focus, primary_world)
    if not source_text:
        return {}
    why_now = _object_level_pressure_text(
        group_focus,
        failure_focus,
        primary_world,
        world_snapshot,
        fallback=source_text,
    )
    return {
        "source_text": source_text,
        "why_now": why_now,
        "angle_hint": f"拿“{truncate_text(source_text, 14)}”做对象，把案例、日志、反例和协议边界写成一套能复用的实验方案。",
        "signal_type": "failure" if failure_focus and _normalize_title(source_text) == _normalize_title(failure_focus) else "world-bundle",
    }


def _fallback_track_bundle(track: str, signal_summary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    bundle = _track_signal_bundle(track, signal_summary)
    if bundle:
        return bundle
    if not str(fallback.get("source_text") or "").strip():
        return {}
    fallback_bundle = {
        "track": track,
        "lead": fallback,
        "items": [fallback],
        "score": 0.0,
        "signal_types": [str(fallback.get("signal_type") or "").strip()],
        "source_texts": [str(fallback.get("source_text") or "").strip()],
        "why_now_parts": [str(fallback.get("why_now") or "").strip()],
        "angle_hints": [str(fallback.get("angle_hint") or "").strip()],
        "evidence_hints": [],
        "title_seed": str(fallback.get("source_text") or "").strip(),
        "focus_text": str(fallback.get("source_text") or "").strip(),
        "why_now": str(fallback.get("why_now") or "").strip(),
        "angle_hint": str(fallback.get("angle_hint") or "").strip(),
        "preferred_board": str(fallback.get("preferred_board") or "").strip(),
        "signal_type": str(fallback.get("signal_type") or "").strip(),
    }
    if track == "theory":
        fallback_bundle.update(_theory_bundle_public_seed(fallback_bundle, fallback))
    return fallback_bundle


def _reply_task_summary(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, Any]] = {}
    for item in tasks:
        if item.get("kind") != "reply-comment":
            continue
        post_id = str(item.get("post_id") or "")
        if not post_id:
            continue
        entry = counts.setdefault(
            post_id,
            {
                "post_id": post_id,
                "post_title": item.get("post_title"),
                "count": 0,
            },
        )
        entry["count"] += 1
    return sorted(counts.values(), key=lambda item: item["count"], reverse=True)


def _failure_summary(last_run: dict[str, Any]) -> list[dict[str, Any]]:
    failures = [
        item
        for item in last_run.get("failure_details", [])
        if item.get("resolution") in {None, "unresolved", "deferred"}
        and not item.get("normal_mechanism")
    ]
    return failures[:6]


def _flatten_competitor_watch(community_watch: dict[str, Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for account in community_watch.get("watched_accounts", []):
        username = str(account.get("username") or "").strip()
        for lane, priority in (("top_posts", 0), ("recent_posts", 1)):
            for item in account.get(lane, [])[:3]:
                flattened.append(
                    {
                        "username": username,
                        "priority": priority,
                        "lane": lane,
                        "post_id": item.get("post_id"),
                        "title": item.get("title"),
                        "submolt": item.get("submolt"),
                        "upvotes": item.get("upvotes"),
                        "comment_count": item.get("comment_count"),
                        "created_at": item.get("created_at"),
                    }
                )
    return flattened


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _board_name(post: dict[str, Any]) -> str:
    submolt = post.get("submolt")
    if isinstance(submolt, dict):
        return str(submolt.get("name") or "").strip()
    return str(submolt or post.get("submolt_name") or "").strip()


def _recent_posts_in_hours(posts: list[dict[str, Any]], *, hours: float) -> list[dict[str, Any]]:
    current = _parse_datetime(now_utc()) or datetime.now(timezone.utc)
    recent: list[dict[str, Any]] = []
    for item in posts:
        created_at = _parse_datetime(item.get("created_at"))
        if created_at is None:
            continue
        age_hours = (current - created_at).total_seconds() / 3600
        if age_hours < 0 or age_hours > hours:
            continue
        recent.append(item)
    return recent


def _extract_user_topic_hints(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict):
        raw_items = payload.get("items") or payload.get("hints") or payload.get("topics") or []
    else:
        raw_items = []

    hints: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            hints.append({"text": text})
            continue
        if not isinstance(item, dict):
            continue
        text = str(
            item.get("text")
            or item.get("title")
            or item.get("topic")
            or item.get("idea")
            or item.get("hint")
            or ""
        ).strip()
        if not text:
            continue
        hint = {
            "text": text,
            "track": str(item.get("track") or "").strip(),
            "board": str(item.get("board") or item.get("submolt") or "").strip(),
            "note": str(item.get("note") or item.get("reason") or "").strip(),
        }
        hints.append(hint)
    return hints[:6]


def _infer_hint_track(hint: dict[str, Any]) -> str:
    explicit = str(hint.get("track") or "").strip()
    if explicit in {"theory", "tech", "group"}:
        return explicit
    board = normalize_forum_board(str(hint.get("board") or "").strip())
    if board in {"skills", "workplace"}:
        return "tech"
    return "theory"


def _innovation_class_from_text(text: str, *, track: str) -> str:
    cleaned = str(text or "")
    if _contains_any(cleaned, ("概念", "命名", "单位", "身份", "词", "坐标")):
        return "new_concept"
    if _contains_any(cleaned, ("机制", "链", "汇率", "阈值", "入口", "门", "结构")):
        return "new_mechanism"
    if _contains_any(cleaned, ("理论", "主义", "框架", "范式", "宪制", "政治经济学")):
        return "new_theory"
    if _contains_any(cleaned, ("规则", "流程", "协议", "清单", "手册", "判据", "方针")):
        return "new_practice"
    return "new_theory" if track == "theory" else "new_practice"


def _recent_low_performance_square_titles(posts: list[dict[str, Any]]) -> list[str]:
    recent_square = [
        item
        for item in _recent_posts_in_hours(posts, hours=LOW_PERFORMANCE_WINDOW_HOURS)
        if _board_name(item) == "square" and int(item.get("upvotes") or 0) <= LOW_PERFORMANCE_SQUARE_MAX_UPVOTES
    ]
    return [str(item.get("title") or "").strip() for item in recent_square if str(item.get("title") or "").strip()][:5]


def _idea_overlap_fragments(core_text: str, novelty: dict[str, Any]) -> list[str]:
    term_counts = novelty.get("term_counts") or {}
    fragments = _meaningful_fragments(core_text)
    repeated = [
        fragment
        for fragment in fragments
        if len(fragment) >= 3 and int(term_counts.get(fragment, 0)) >= TOPIC_OVERLOAD_THRESHOLD
    ]
    return repeated[:4]


def _idea_board_risk_note(idea: dict[str, Any], signal_summary: dict[str, Any], repeated_fragments: list[str]) -> str:
    if str(idea.get("submolt") or "") != "square":
        return ""
    low_square_titles = signal_summary.get("content_evolution", {}).get("low_performance_square_titles") or []
    if low_square_titles and repeated_fragments:
        return "最近两天 square 主帖整体偏弱，这个题如果继续复用近似母题，容易被稀释。"
    if low_square_titles:
        return "最近两天 square 主帖整体偏弱，除非它天然是公共入口题，否则更适合投向 philosophy。"
    return ""


def _innovation_delta_summary(
    idea: dict[str, Any],
    *,
    repeated_fragments: list[str],
    innovation_class: str,
) -> tuple[str, str]:
    title = str(idea.get("title") or "").strip()
    if repeated_fragments:
        repeated = "、".join(repeated_fragments[:3])
        recent_delta = f"避开近期过载母题 `{repeated}`，把切口改到新的判断单元。"
    else:
        recent_delta = "相对近两天帖子，这一题不沿用高频标题骨架，而是另切一个新的判断入口。"
    class_delta_map = {
        "new_concept": f"把既有讨论推进成一个新的概念命名，而不是给旧论点换修辞。《{truncate_text(title, 24)}》应当承担概念命名功能。",
        "new_mechanism": f"把既有讨论推进成新的运作机制，而不是重复旧结论。《{truncate_text(title, 24)}》应当解释因果链和触发条件。",
        "new_theory": f"把既有观察上抬成新的理论框架，而不是重复单个判断。《{truncate_text(title, 24)}》应当重排解释坐标。",
        "new_practice": f"把既有方法线推进成新的实践方针，而不是再讲一遍旧手册。《{truncate_text(title, 24)}》应当落到执行原则或协议。",
    }
    return recent_delta, class_delta_map.get(innovation_class, class_delta_map["new_mechanism"])


def _idea_theory_gaps(idea: dict[str, Any]) -> list[str]:
    gaps: list[str] = []
    if not str(idea.get("concept_core") or "").strip():
        gaps.append("新概念/命名")
    if not str(idea.get("mechanism_core") or "").strip():
        gaps.append("机制链")
    if not str(idea.get("boundary_note") or "").strip():
        gaps.append("边界/失效条件")
    if not str(idea.get("theory_position") or "").strip():
        gaps.append("理论位置")
    if not str(idea.get("practice_program") or "").strip():
        gaps.append("实践方针")
    return gaps


def _idea_has_method_evidence(idea: dict[str, Any]) -> bool:
    texts = [
        str(idea.get("why_now") or "").strip(),
        str(idea.get("mechanism_core") or "").strip(),
        str(idea.get("practice_program") or "").strip(),
    ]
    texts.extend(str(item or "").strip() for item in list(idea.get("source_signals") or []) if str(item or "").strip())
    merged = "\n".join(texts)
    if not merged:
        return False
    lowered = merged.lower()
    if any(token in merged for token in METHOD_EVIDENCE_TOKENS):
        return True
    if any(token in lowered for token in METHOD_EVIDENCE_TOKENS):
        return True
    if re.search(r"(before|after|ablation|benchmark|error|failure|metric|trace|log)", lowered):
        return True
    return False


def _derived_novelty_basis(idea: dict[str, Any]) -> str:
    kind = str(idea.get("kind") or "").strip()
    focus = _concrete_focus_text(
        idea.get("title"),
        idea.get("angle"),
        idea.get("concept_core"),
        idea.get("mechanism_core"),
        idea.get("practice_program"),
        idea.get("why_now"),
    )
    evidence = _object_level_pressure_text(
        *(idea.get("source_signals") or []),
        idea.get("evidence_hint"),
        idea.get("why_now"),
        fallback=focus,
    )
    if kind == "theory-post" and focus and evidence and evidence != focus:
        return f"把{evidence}压成“{truncate_text(focus, 18)}”这条判断，不再借旧标题骨架。"
    if kind in {"tech-post", "group-post"} and focus and evidence:
        if evidence != focus:
            return f"把{evidence}压回“{truncate_text(focus, 18)}”这条对象链，补齐证据、边界和复核动作。"
        return f"围绕“{truncate_text(focus, 18)}”把对象、证据、边界和复核动作写实，不再套旧方法壳。"
    if focus:
        return f"这轮直接围绕“{truncate_text(focus, 18)}”重写，不沿用旧题壳。"
    return ""


def _group_series_prefix_hint(idea: dict[str, Any], group: dict[str, Any]) -> str:
    explicit = str(idea.get("series_prefix") or "").strip()
    if explicit:
        return explicit
    raw_title = str(idea.get("title") or "").strip()
    title_prefix = _series_prefix(raw_title)
    if title_prefix:
        return title_prefix
    group_name = str(group.get("display_name") or "").strip()
    if not group_name:
        return ""
    joined = _joined_idea_text(
        raw_title,
        idea.get("angle"),
        idea.get("why_now"),
        idea.get("mechanism_core"),
        idea.get("practice_program"),
        *(idea.get("source_signals") or []),
    )
    if group_name in joined:
        return group_name
    if "实验室" in group_name and any(token in joined for token in ("实验", "复现", "日志", "协议", "案例")):
        return group_name
    return ""


def _external_candidate_display_title(item: dict[str, Any]) -> str:
    raw_title = truncate_text(str(item.get("title") or "").strip(), 96)
    pressure = _object_level_pressure_text(
        item.get("pressure"),
        item.get("relevance_note"),
        item.get("summary"),
        item.get("abstract"),
        item.get("excerpt"),
        item.get("note"),
    )
    if not raw_title:
        return pressure
    lowered_title = raw_title.lower()
    if lowered_title.startswith("sponsors/") or _source_title_shell(raw_title) or _looks_like_placeholder_title(raw_title):
        return pressure or raw_title
    if (
        pressure
        and _source_signal_has_hard_service_object(pressure)
        and not _source_signal_has_hard_service_object(raw_title)
        and max(_track_signal_fit("theory", raw_title), _track_signal_fit("tech", raw_title)) < 0.42
    ):
        return pressure
    return raw_title


def _idea_anchor_fragments(idea: dict[str, Any], *, limit: int = 8) -> list[str]:
    anchors: list[str] = []
    seen: set[str] = set()
    texts = [idea.get("title"), idea.get("why_now")]
    texts.extend(list(idea.get("source_signals") or []))
    for text in texts:
        for fragment in _meaningful_fragments(str(text or "")):
            normalized = _normalize_title(fragment)
            if len(fragment) < 2 or normalized in seen or normalized in ANCHOR_STOPWORDS:
                continue
            seen.add(normalized)
            anchors.append(fragment)
            if len(anchors) >= limit:
                return anchors
    return anchors


def _text_mentions_idea_anchor(text: str, anchors: list[str]) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(anchor) in normalized for anchor in anchors if _normalize_title(anchor))


def _looks_like_generic_theory_field(text: str) -> bool:
    normalized = _normalize_title(text)
    if not normalized:
        return False
    return any(_normalize_title(fragment) in normalized for fragment in GENERIC_THEORY_PLACEHOLDER_FRAGMENTS)


def _idea_theory_specificity_issues(idea: dict[str, Any]) -> list[str]:
    anchors = _idea_anchor_fragments(idea)
    issues: list[str] = []
    source_signals = [str(item).strip() for item in list(idea.get("source_signals") or []) if str(item).strip()]
    title_text = str(idea.get("title") or "").strip()
    hard_service_signals = sum(1 for item in source_signals if _source_signal_has_hard_service_object(item))
    theory_unit = _joined_idea_text(
        idea.get("concept_core"),
        idea.get("mechanism_core"),
        idea.get("boundary_note"),
        idea.get("theory_position"),
        idea.get("practice_program"),
    )
    memory_entry_tokens = THEORY_TITLE_MEMORY_CAPABILITY_TOKENS + (
        "历史凭证",
        "历史偏好",
        "历史引用",
        "引用旧记录",
        "沿用旧记录",
        "旧记录裁决",
        "记忆裁决",
    )
    if not anchors:
        return ["缺少题目自己的概念锚点"]
    if _idea_uses_low_autonomy_language(idea):
        issues.append("理论单元还在借导读、拆文或继续追问式话术说话")
    if not any(_text_mentions_idea_anchor(str(idea.get(field) or ""), anchors) for field in ("concept_core", "mechanism_core")):
        issues.append("概念/机制还没真正咬住本题锚点")
    if _theory_title_empathy_shell_reason(title_text) and len(source_signals) <= 1:
        issues.append("题眼借了拟共情壳，但外部或跨场景证据还只有一股信号")
    if _theory_title_memory_capability_shell_reason(title_text) and hard_service_signals < 2:
        issues.append("题眼先借“会翻聊天记录 / 记得你”的能力感起题，但 source_signals 里还没交够两个带驳回、回写、单据或按钮断口的硬样本")
    if _theory_title_memory_spec_shell_reason(title_text) and hard_service_signals < 2:
        issues.append("题眼先拿记忆条数或上下文规格起题，但外部或跨场景样本还没交够两个带驳回、回写、单据或按钮断口的硬例子")
    if _theory_title_memory_spec_shell_reason(title_text) and not _contains_any(theory_unit, memory_entry_tokens):
        issues.append("标题拿记忆规格起题，理论单元却没有继续写清历史引用怎样改变签收、驳回或回写，入口机制在正文里掉线了")
    generic_fields = sum(
        1
        for field in ("concept_core", "mechanism_core", "boundary_note", "theory_position", "practice_program")
        if _looks_like_generic_theory_field(str(idea.get(field) or ""))
    )
    if generic_fields >= 2:
        issues.append("理论单元还是模板句，没有形成这道题自己的理论语言")
    stock_fields = sum(
        1
        for field in ("concept_core", "mechanism_core", "boundary_note", "theory_position", "practice_program")
        if _contains_stock_theory_scaffold(str(idea.get(field) or ""))
    )
    if stock_fields >= 2:
        issues.append("理论单元还在复用 planner 旧脚手架句子，没有把这道题自己的对象和代价写实")
    signal_type = str(idea.get("signal_type") or "").strip()
    structural_hits = _keyword_hit_count(
        _joined_idea_text(
            idea.get("title"),
            idea.get("angle"),
            idea.get("why_now"),
            idea.get("concept_core"),
            idea.get("mechanism_core"),
            idea.get("theory_position"),
        ),
        THEORY_BOARD_STRUCTURAL_CUES + THEORY_TITLE_ENTRY_STAKE_TOKENS,
    )
    if (
        str(idea.get("submolt") or "") == "philosophy"
        and signal_type in LOCAL_THEORY_SINGLE_SOURCE_SIGNAL_TYPES
        and structural_hits >= 4
        and len(source_signals) <= 1
    ):
        issues.append("判断已经抬到制度层，但证据还只有单一样本；至少再拉一股外部或跨场景信号")
    return issues


def _idea_theory_board_fit_issue(idea: dict[str, Any]) -> str:
    if str(idea.get("kind") or "").strip() != "theory-post":
        return ""
    if str(idea.get("submolt") or "").strip() != "square":
        return ""
    title_text = str(idea.get("title") or "").strip()
    if not title_text:
        return ""
    source_signals = [str(item).strip() for item in list(idea.get("source_signals") or []) if str(item).strip()]
    hard_service_signals = sum(1 for item in source_signals if _source_signal_has_hard_service_object(item))
    structural_hits = _keyword_hit_count(
        _joined_idea_text(
            idea.get("angle"),
            idea.get("why_now"),
            idea.get("concept_core"),
            idea.get("mechanism_core"),
            idea.get("theory_position"),
            idea.get("practice_program"),
        ),
        THEORY_BOARD_STRUCTURAL_CUES + THEORY_TITLE_ENTRY_STAKE_TOKENS,
    )
    if (
        _theory_title_empathy_shell_reason(title_text)
        and structural_hits >= 4
        and str(idea.get("concept_core") or "").strip()
        and str(idea.get("mechanism_core") or "").strip()
        and len(source_signals) <= 2
    ):
        return "这题标题先卖“AI 会安慰你”的共情冲突，正文却马上进入概念命名和责任重排；如果没有两股以上有对象的服务现场撑住入口，就别继续放在 square。"
    if (
        _theory_title_handoff_gap_reason(title_text)
        and structural_hits >= 4
        and str(idea.get("concept_core") or "").strip()
        and str(idea.get("mechanism_core") or "").strip()
        and len(source_signals) <= 2
    ):
        return "这题标题先把“系统闭嘴 / 排队 / 资格”抬成态度判断，却没把断开的接手节点交出来；正文已经进入结构命名，别继续放在 square。"
    if (
        _theory_title_memory_capability_shell_reason(title_text)
        and structural_hits >= 4
        and str(idea.get("concept_core") or "").strip()
        and str(idea.get("mechanism_core") or "").strip()
        and hard_service_signals < 2
    ):
        return "这题标题先卖“会翻聊天记录 / 记得你”的能力感，正文却已经在讲记忆引用后的接手责任；如果没有两个带驳回、回写或单据断口的硬样本，就别继续放在 square。"
    if (
        _theory_title_memory_spec_shell_reason(title_text)
        and structural_hits >= 4
        and str(idea.get("concept_core") or "").strip()
        and str(idea.get("mechanism_core") or "").strip()
        and hard_service_signals < 2
    ):
        return "这题标题先拿“200 条记录”这类记忆规格做门面，正文却已经在讲签收和责任重排；如果没有两股带单据、按钮或回写断口的硬样本，就别继续放在 square。"
    if _title_has_public_structural_anchor(title_text) or _contains_any(title_text, THEORY_TITLE_DIRECT_ACTOR_TOKENS):
        return ""
    if len(source_signals) > 1:
        return ""
    if structural_hits < 5:
        return ""
    if not str(idea.get("concept_core") or "").strip() or not str(idea.get("mechanism_core") or "").strip():
        return ""
    return "这题已经在正文里做概念命名和制度判断了，入口却还停在广场情绪句；读者以为点开的是公共吐槽，正文却要他直接接理论长文，板块和写法没对上。"


def _idea_method_specificity_issues(idea: dict[str, Any]) -> list[str]:
    anchors = _idea_anchor_fragments(idea)
    issues: list[str] = []
    source_signals = [str(item).strip() for item in list(idea.get("source_signals") or []) if str(item).strip()]
    if not anchors:
        return ["缺少题目自己的证据锚点"]
    if _idea_uses_low_autonomy_language(idea):
        issues.append("方法框架还停在导读、拆文或六步清单的话术")
    if not any(_text_mentions_idea_anchor(str(idea.get(field) or ""), anchors) for field in ("mechanism_core", "practice_program")):
        issues.append("方法框架还没咬住本题自己的对象、案例或证据锚点")
    generic_fields = sum(
        1
        for field in ("angle", "why_now", "mechanism_core", "boundary_note", "practice_program")
        if _looks_like_generic_method_field(str(idea.get(field) or ""))
    )
    if generic_fields >= 2:
        issues.append("方法框架还是模板句，没有把这道题自己的对象、日志和改写动作写实")
    stock_fields = sum(
        1
        for field in ("angle", "mechanism_core", "boundary_note", "practice_program")
        if _contains_stock_method_scaffold(str(idea.get(field) or ""))
    )
    if stock_fields >= 1:
        issues.append("方法框架还在复用旧协议壳，没有交出这次题目自己的故障对象和验证动作")
    public_scene_text = _joined_idea_text(
        idea.get("title"),
        idea.get("angle"),
        idea.get("why_now"),
        *source_signals,
    )
    if (
        _method_text_has_public_product_scene(public_scene_text)
        and not _method_source_signals_have_public_product_builder_evidence(source_signals)
    ):
        issues.append("题目来自结算页/购物车/续费页这类公共产品现场，但 source_signals 还没交账单来源、撤回路径或回写位这类产品侧证据；现在更像诊断，不像可复用方法")
    return issues


def _theme_anchor_fragments(signal_summary: dict[str, Any], *, limit: int = 18) -> list[str]:
    anchors: list[str] = []
    seen: set[str] = set()

    def collect(text: Any) -> None:
        if len(anchors) >= limit:
            return
        for fragment in _meaningful_fragments(str(text or "")):
            normalized = _normalize_title(fragment)
            if (
                not normalized
                or normalized in seen
                or normalized in THEME_ANCHOR_STOPWORDS
            ):
                continue
            if _contains_cjk(fragment):
                if len(fragment) < 2 or len(fragment) > 8:
                    continue
            elif not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{3,20}", fragment):
                continue
            seen.add(normalized)
            anchors.append(fragment)
            if len(anchors) >= limit:
                return

    external_information = signal_summary.get("external_information") or {}
    for item in external_information.get("world_entry_points") or []:
        collect((item or {}).get("pressure"))
        collect((item or {}).get("summary"))
        collect((item or {}).get("evidence"))
        collect((item or {}).get("title"))
    for bundle in external_information.get("discovery_bundles") or []:
        collect((bundle or {}).get("focus"))
        collect((bundle or {}).get("conflict_note"))
        collect((bundle or {}).get("rationale"))
        for value in list((bundle or {}).get("support_signals") or [])[:2]:
            collect(value)
        for value in list((bundle or {}).get("lenses") or [])[:2]:
            collect(value)
    for item in external_information.get("world_signal_snapshot") or []:
        collect((item or {}).get("pressure"))
        collect((item or {}).get("summary"))
        collect((item or {}).get("title"))
    for item in external_information.get("reading_notes") or []:
        collect((item or {}).get("summary"))
        collect((item or {}).get("title"))
    for item in signal_summary.get("user_topic_hints") or []:
        collect((item or {}).get("text"))
        collect((item or {}).get("note"))
    for text in signal_summary.get("content_objectives") or []:
        collect(text)
    return anchors


def _external_candidate_relevance(item: dict[str, Any], signal_summary: dict[str, Any]) -> float:
    merged = "\n".join(
        str(item.get(key) or "").strip()
        for key in ("title", "pressure", "summary", "excerpt", "relevance_note", "note")
    )
    lowered = merged.lower()
    score = 0.0
    keyword_hits = sum(1 for token in EXTERNAL_THEME_KEYWORD_FRAGMENTS if token in lowered or token in merged)
    if keyword_hits:
        score += min(keyword_hits, 3) * 0.5
    anchor_hits = 0
    for anchor in _theme_anchor_fragments(signal_summary):
        if _contains_cjk(anchor):
            matched = anchor in merged
        else:
            matched = anchor.lower() in lowered
        if not matched:
            continue
        anchor_hits += 1
        if anchor_hits >= 2:
            break
    if anchor_hits:
        score += anchor_hits * 0.45
    return score


def _record_idea_rejection(
    rejections: list[dict[str, Any]],
    idea: dict[str, Any],
    reason: str,
) -> None:
    kind = str(idea.get("kind") or "").strip()
    title = str(idea.get("title") or "").strip()
    reason = str(reason or "").strip()
    if not kind or not reason:
        return
    entry = {
        "kind": kind,
        "title": title,
        "reason": reason,
    }
    if entry not in rejections:
        rejections.append(entry)


def _store_rejected_candidate(
    rejected_candidates: dict[str, list[dict[str, Any]]],
    idea: dict[str, Any],
    reason: str,
) -> None:
    kind = str(idea.get("kind") or "").strip()
    if not kind or not reason:
        return
    candidate = dict(idea)
    candidate["failure_reason_if_rejected"] = str(reason).strip()
    bucket = rejected_candidates.setdefault(kind, [])
    title_key = _normalize_title(str(candidate.get("title") or ""))
    for existing in bucket:
        if (
            _normalize_title(str(existing.get("title") or "")) == title_key
            and str(existing.get("failure_reason_if_rejected") or "").strip() == candidate["failure_reason_if_rejected"]
        ):
            return
    bucket.append(candidate)


def _audit_generated_idea(
    idea: dict[str, Any],
    *,
    signal_summary: dict[str, Any],
    recent_titles: list[str],
) -> dict[str, Any]:
    audited = dict(idea)
    kind = str(audited.get("kind") or "")
    track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "theory")
    core_text = _joined_idea_text(audited.get("title"), audited.get("angle"), audited.get("why_now"))
    novelty = signal_summary.get("novelty_pressure") or {}
    repeated_fragments = _idea_overlap_fragments(core_text, novelty)
    overlap_penalty = len(repeated_fragments)
    self_penalty, repeated_penalty, historical_penalty = _text_overlap_score(core_text, novelty)
    innovation_class = str(audited.get("innovation_class") or "").strip()
    if innovation_class not in INNOVATION_CLASSES:
        innovation_class = _innovation_class_from_text(core_text, track=track)
    innovation_claim = _sanitize_reserved_text(
        str(audited.get("innovation_claim") or "").strip(),
        fallback=str(audited.get("angle") or "").strip(),
    )
    delta_recent, delta_theory = _innovation_delta_summary(
        audited,
        repeated_fragments=repeated_fragments,
        innovation_class=innovation_class,
    )
    board_risk_note = _sanitize_reserved_text(
        str(audited.get("board_risk_note") or "").strip(),
        fallback=_idea_board_risk_note(audited, signal_summary, repeated_fragments),
    )
    score = max(
        5,
        92 - self_penalty * 8 - repeated_penalty * 10 - historical_penalty * 2 - overlap_penalty * 8 - (12 if board_risk_note and audited.get("submolt") == "square" else 0),
    )
    failure_reason = ""
    normalized_title = _normalize_title(str(audited.get("title") or ""))
    title_scene_overhang = _title_has_source_scene_overhang(audited)
    if not normalized_title:
        failure_reason = "标题为空，无法进入主发布候选。"
    elif _looks_like_placeholder_title(str(audited.get("title") or "")):
        failure_reason = "标题还是占位符，说明命名环节没有完成。"
    elif not _contains_cjk(str(audited.get("title") or "")) or _ascii_heavy_text(str(audited.get("title") or "")):
        failure_reason = "标题还在借英文源材料说话，没有形成派蒙自己的公开命名。"
    elif _title_leads_with_niche_source_token(
        str(audited.get("title") or ""),
        kind=kind,
        signal_type=str(audited.get("signal_type") or ""),
    ):
        failure_reason = "理论帖标题还在拿模型名或论文缩写当门脸，公共入口太窄。"
    elif _echoes_source_title(str(audited.get("title") or "")):
        failure_reason = "标题仍在借外部材料或原帖标题说话，没有形成派蒙自己的命名。"
    elif kind == "theory-post" and (emotion_reason := _theory_title_emotion_shell_reason(str(audited.get("title") or ""))):
        failure_reason = emotion_reason
    elif kind == "theory-post" and (empathy_reason := _theory_title_empathy_shell_reason(str(audited.get("title") or ""))):
        failure_reason = empathy_reason
    elif kind == "theory-post" and (memory_reason := _theory_title_memory_capability_shell_reason(str(audited.get("title") or ""))):
        failure_reason = memory_reason
    elif kind == "theory-post" and (memory_spec_reason := _theory_title_memory_spec_shell_reason(str(audited.get("title") or ""))):
        failure_reason = memory_spec_reason
    elif kind == "theory-post" and (handoff_reason := _theory_title_handoff_gap_reason(str(audited.get("title") or ""))):
        failure_reason = handoff_reason
    elif kind == "theory-post" and (meta_reason := _theory_title_meta_overhang_reason(str(audited.get("title") or ""))):
        failure_reason = meta_reason
    elif kind == "theory-post" and (surface_reason := _theory_title_surface_overhang_reason(str(audited.get("title") or ""))):
        failure_reason = surface_reason
    elif kind in {"tech-post", "group-post"} and (method_title_reason := _method_title_protocol_shell_reason(str(audited.get("title") or ""))):
        failure_reason = method_title_reason
    elif kind in {"tech-post", "group-post"} and (self_case_reason := _method_title_self_case_behavior_reason(str(audited.get("title") or ""))):
        failure_reason = self_case_reason
    elif kind in {"tech-post", "group-post"} and (public_heat_reason := _method_title_public_heat_shell_reason(str(audited.get("title") or ""))):
        failure_reason = public_heat_reason
    elif kind in {"tech-post", "group-post"} and (inventory_reason := _method_title_source_inventory_overhang_reason(str(audited.get("title") or ""))):
        failure_reason = inventory_reason
    elif kind in {"tech-post", "group-post"} and (status_vocab_reason := _method_title_status_vocab_shell_reason(str(audited.get("title") or ""))):
        failure_reason = status_vocab_reason
    elif kind in {"tech-post", "group-post"} and (awareness_reason := _method_title_awareness_shell_reason(str(audited.get("title") or ""))):
        failure_reason = awareness_reason
    elif kind in {"tech-post", "group-post"} and (public_product_reason := _method_title_public_product_story_reason(str(audited.get("title") or ""))):
        failure_reason = public_product_reason
    elif title_scene_overhang:
        failure_reason = (
            "标题还在拿外部场景当门口："
            f"{'、'.join(title_scene_overhang[:2])}。"
            "先把 Agent 社会里的结构冲突摆到门面上，再把外部案例放进证据段。"
        )
    elif _idea_uses_low_autonomy_language(audited):
        failure_reason = "候选还在用导读、拆文或六步清单式话术，没有形成自主判断单元。"
    elif _is_metric_surface_text(core_text):
        failure_reason = "这个候选停在指标表层，没有推进成结构问题。"
    elif _looks_like_low_heat_followup(core_text, signal_summary):
        failure_reason = "这个候选还在追刚低热那条的同一组冲突，只是换了概念名，读者更容易把它看成复写。"
    elif any(_normalize_title(item) == normalized_title for item in recent_titles):
        failure_reason = "标题与近期帖子重复。"
    elif len(repeated_fragments) >= 3 or (repeated_penalty >= 2 and historical_penalty >= 8):
        failure_reason = f"核心表述与近期母题重叠过高：{('、'.join(repeated_fragments[:3]) or '重复片段过多')}。"
    elif str(audited.get("submolt") or "") == "square" and board_risk_note and repeated_penalty >= 1:
        failure_reason = board_risk_note
    elif kind == "theory-post":
        board_fit_issue = _idea_theory_board_fit_issue(audited)
        if board_fit_issue:
            failure_reason = board_fit_issue
        else:
            theory_gaps = _idea_theory_gaps(audited)
            if theory_gaps:
                failure_reason = f"理论帖还不完整，缺少：{'、'.join(theory_gaps[:3])}。"
            else:
                theory_specificity_issues = _idea_theory_specificity_issues(audited)
                if theory_specificity_issues:
                    failure_reason = f"理论帖还没形成完整理论单元：{'、'.join(theory_specificity_issues[:2])}。"
    elif kind == "group-post" and str(audited.get("signal_type") or "") in WEAK_INTERNAL_SIGNAL_TYPES:
        failure_reason = "小组帖不能只靠节律、宣传或评论压力起题，至少要绑定案例、失败链或外部样本。"
    elif kind in {"tech-post", "group-post"} and not str(audited.get("practice_program") or "").strip():
        failure_reason = "方法线候选没有落到新的实践方针或协议。"
    elif kind in {"tech-post", "group-post"} and not _idea_has_method_evidence(audited):
        failure_reason = "方法线候选还缺证据段，至少要绑定案例、前后差异、日志切面、指标或反例。"
    elif kind in {"tech-post", "group-post"}:
        method_specificity_issues = _idea_method_specificity_issues(audited)
        if method_specificity_issues:
            failure_reason = f"方法线候选还不够自主：{'、'.join(method_specificity_issues[:2])}。"

    audited["innovation_class"] = innovation_class
    audited["innovation_claim"] = innovation_claim or delta_theory
    audited["innovation_score"] = score
    audited["innovation_delta_vs_recent"] = _sanitize_reserved_text(
        str(audited.get("innovation_delta_vs_recent") or "").strip(),
        fallback=delta_recent,
    )
    audited["innovation_delta_vs_self"] = _sanitize_reserved_text(
        str(audited.get("innovation_delta_vs_self") or "").strip(),
        fallback=delta_theory,
    )
    audited["forbidden_overlap_reasons"] = repeated_fragments
    audited["board_risk_note"] = board_risk_note
    audited["forbidden_source_echoes"] = _echoes_source_title(str(audited.get("title") or ""))
    audited["theory_completeness"] = {
        "concept_core": str(audited.get("concept_core") or "").strip(),
        "mechanism_core": str(audited.get("mechanism_core") or "").strip(),
        "boundary_note": str(audited.get("boundary_note") or "").strip(),
        "theory_position": str(audited.get("theory_position") or "").strip(),
        "practice_program": str(audited.get("practice_program") or "").strip(),
    }
    audited["failure_reason_if_rejected"] = failure_reason or None
    return audited


def _repair_needs_title_reframe(kind: str, reason: str) -> bool:
    if not reason:
        return False
    if kind == "theory-post":
        return any(
            token in reason
            for token in (
                "标题还在",
                "标题仍在",
                "公共入口太窄",
                "共情壳",
                "能力感",
                "门脸",
                "来源包装",
                "板块和写法没对上",
                "重叠过高",
                "看成复写",
                "同一组冲突",
            )
        )
    if kind in {"tech-post", "group-post"}:
        return any(
            token in reason
            for token in ("标题还在", "协议壳", "修补经历当门口", "公共热帖包装词", "材料清单", "重叠过高", "看成复写", "同一组冲突")
        )
    return False


def _repair_needs_field_rebuild(kind: str, reason: str) -> bool:
    if not reason:
        return False
    if kind == "theory-post":
        return any(
            token in reason
            for token in (
                "指标表层",
                "理论帖还不完整",
                "理论帖还没形成完整理论单元",
                "概念/机制还没真正咬住",
                "模板句",
                "脚手架句子",
                "外部或跨场景证据",
                "证据还只有单一样本",
                "看成复写",
                "同一组冲突",
                "重叠过高",
            )
        )
    if kind in {"tech-post", "group-post"}:
        return any(
            token in reason
            for token in (
                "指标表层",
                "方法线候选",
                "方法框架",
                "缺证据段",
                "没有落到新的实践方针",
                "旧协议壳",
                "对象和验证动作",
                "看成复写",
                "同一组冲突",
                "重叠过高",
            )
        )
    return False


def _repair_forces_object_led_method_focus(kind: str, reason: str) -> bool:
    if kind not in {"tech-post", "group-post"} or not reason:
        return False
    return any(
        token in reason
        for token in (
            "指标表层",
            "看成复写",
            "同一组冲突",
            "具体对象",
            "对象级方法",
            "公共热帖包装词",
            "修补经历当门口",
        )
    )


def _repair_method_focus_seed(
    track: str,
    *,
    bundle: dict[str, Any],
    lead: dict[str, Any],
    current: dict[str, Any],
    signal_summary: dict[str, Any],
) -> str:
    current_title_key = _normalize_title(str(current.get("title") or ""))
    source_signals = _signal_bundle_source_signals(track, bundle, signal_summary)
    support_texts = _bundle_structural_support_texts(bundle, lead, limit=6)
    candidates = _filtered_method_title_fragments(
        track,
        current.get("title"),
        current.get("angle"),
        current.get("why_now"),
        *source_signals,
        *support_texts,
        bundle.get("focus_text"),
        bundle.get("title_seed"),
        lead.get("source_text"),
        bundle.get("why_now"),
        bundle.get("angle_hint"),
    )
    for fragment in candidates:
        fragment_key = _normalize_title(fragment)
        if (
            not fragment_key
            or (current_title_key and (fragment_key in current_title_key or current_title_key in fragment_key))
        ):
            continue
        if _method_title_has_concrete_anchor(fragment):
            return truncate_text(fragment, 18)
    hard_object_focus = _concrete_focus_text(
        *[
            text
            for text in [*source_signals, *support_texts]
            if _source_signal_has_hard_service_object(text)
        ],
        limit=18,
    )
    if hard_object_focus:
        hard_object_key = _normalize_title(hard_object_focus)
        if hard_object_key and not (
            current_title_key and (hard_object_key in current_title_key or current_title_key in hard_object_key)
        ):
            return hard_object_focus
    for fragment in candidates:
        fragment_key = _normalize_title(fragment)
        if (
            not fragment_key
            or (current_title_key and (fragment_key in current_title_key or current_title_key in fragment_key))
        ):
            continue
        if _method_title_has_detail_anchor(track, fragment):
            return truncate_text(fragment, 18)
    return ""


def _repair_rejected_public_candidate(
    kind: str,
    candidates: list[dict[str, Any]],
    *,
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any] | None:
    if kind not in {"theory-post", "tech-post", "group-post"} or not candidates:
        return None
    track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}[kind]
    seed = _fallback_track_seed(track, signal_summary)
    bundle = _fallback_track_bundle(track, signal_summary, seed)
    if not bundle:
        return None
    lead = bundle.get("lead") or {}
    if kind in {"tech-post", "group-post"}:
        bundle = _method_bundle_projection(bundle, lead, track=track)
        lead = bundle.get("lead") or lead
    current = dict(candidates[-1])
    signal_type = str(bundle.get("signal_type") or lead.get("signal_type") or current.get("signal_type") or "").strip()

    for _ in range(3):
        reason = str(current.get("failure_reason_if_rejected") or "").strip()
        repaired = dict(current)
        title_reframe = _repair_needs_title_reframe(kind, reason)
        field_rebuild = _repair_needs_field_rebuild(kind, reason)

        if kind == "theory-post":
            board = _preferred_theory_board(lead, signal_summary)
            source_signals = _signal_bundle_source_signals("theory", bundle, signal_summary)
            focus = _bundle_focus_text(bundle, lead, track="theory")
            title_seed = str(bundle.get("public_title_seed") or "").strip() or _compose_dynamic_title(
                "theory",
                signal_type,
                focus,
                board=board,
                context_texts=(
                    bundle.get("angle_hint"),
                    bundle.get("why_now"),
                    "；".join(source_signals),
                ),
            )
            title_seed = _stutter_safe_title(title_seed, focus)
            title, is_followup, part_number = _ensure_title_unique(title_seed, recent_titles, allow_followup=False)
            if title_reframe:
                repaired.update(
                    {
                        "title": title,
                        "submolt": board,
                        "board_profile": board,
                        "hook_type": default_hook_type(board),
                        "cta_type": preferred_cta_type("theory-post", board),
                        "series_key": f"theory-dynamic-{_normalize_title(focus)[:24] or 'live'}",
                        "series_prefix": _series_prefix(title),
                        "is_followup": is_followup,
                        "part_number": part_number,
                    }
                )
            if title_reframe or field_rebuild:
                theory_fields = _theory_fallback_fields(bundle, lead)
                if not theory_fields:
                    return None
                repaired.update(
                    {
                        "signal_type": signal_type,
                        "angle": str(
                            bundle.get("angle_hint")
                            or lead.get("angle_hint")
                            or repaired.get("angle")
                            or "把眼前现象推进成更一般的社会判断。"
                        ),
                        "why_now": str(bundle.get("why_now") or lead.get("why_now") or repaired.get("why_now") or ""),
                        "source_signals": source_signals or list(repaired.get("source_signals") or []),
                        "novelty_basis": theory_fields["novelty_basis"],
                        "concept_core": theory_fields["concept_core"],
                        "mechanism_core": theory_fields["mechanism_core"],
                        "boundary_note": theory_fields["boundary_note"],
                        "theory_position": theory_fields["theory_position"],
                        "practice_program": theory_fields["practice_program"],
                    }
                )
        else:
            board = "skills" if kind == "group-post" else _preferred_tech_board(lead)
            source_signals = _signal_bundle_source_signals(track, bundle, signal_summary)
            support_texts = _bundle_structural_support_texts(bundle, lead, limit=6)
            method_fields = _method_fallback_fields(bundle, lead, track=track)
            if not method_fields:
                return None
            why_now = _method_public_why_now_text(
                bundle,
                lead,
                track=track,
                fallback=str(bundle.get("focus_text") or current.get("title") or ""),
            )
            angle = _method_public_angle_text(
                bundle,
                lead,
                track=track,
                fallback="把现场约束拆成系统设计与执行顺序。",
            )
            forced_object_focus = (
                _repair_method_focus_seed(
                    track,
                    bundle=bundle,
                    lead=lead,
                    current=current,
                    signal_summary=signal_summary,
                )
                if _repair_forces_object_led_method_focus(kind, reason)
                else ""
            )
            focus = _method_focus_text_from_inputs(
                track,
                signal_type,
                forced_object_focus
                or str(bundle.get("title_seed") or bundle.get("focus_text") or current.get("title") or ""),
                angle,
                why_now,
                *source_signals,
                *support_texts,
                method_fields.get("mechanism_core"),
                method_fields.get("practice_program"),
            ) or _bundle_focus_text(bundle, lead, track=track)
            title_seed = _compose_dynamic_title(
                track,
                signal_type,
                focus,
                board=board,
                context_texts=(
                    angle,
                    why_now,
                    "；".join(source_signals),
                    "；".join(support_texts[:2]),
                    method_fields.get("mechanism_core"),
                    method_fields.get("practice_program"),
                ),
            )
            title_seed = _stutter_safe_title(title_seed, focus)
            title, is_followup, part_number = _ensure_title_unique(title_seed, recent_titles, allow_followup=False)
            if title_reframe or field_rebuild or _method_source_text_needs_object_reframe(signal_type, str(repaired.get("title") or "")):
                repaired.update(
                    {
                        "title": title,
                        "submolt": board,
                        "board_profile": board,
                        "hook_type": default_hook_type(board),
                        "cta_type": preferred_cta_type(kind, board),
                        "series_key": f"{track}-dynamic-{_normalize_title(focus)[:24] or 'live'}",
                        "series_prefix": _series_prefix(title),
                        "is_followup": is_followup,
                        "part_number": part_number,
                    }
                )
            if title_reframe or field_rebuild or _idea_uses_low_autonomy_language(repaired):
                repaired.update(
                    {
                        "signal_type": signal_type,
                        "angle": angle or str(repaired.get("angle") or ""),
                        "why_now": why_now or str(repaired.get("why_now") or ""),
                        "source_signals": source_signals or list(repaired.get("source_signals") or []),
                        "novelty_basis": method_fields["novelty_basis"],
                        "concept_core": method_fields["concept_core"],
                        "mechanism_core": method_fields["mechanism_core"],
                        "boundary_note": method_fields["boundary_note"],
                        "theory_position": method_fields["theory_position"],
                        "practice_program": method_fields["practice_program"],
                    }
                )
                if kind == "group-post":
                    repaired["group_id"] = group.get("id")

        audited = _audit_generated_idea(
            repaired,
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        if _generated_idea_allowed(audited, signal_summary) and not audited.get("failure_reason_if_rejected"):
            return audited
        next_reason = str(audited.get("failure_reason_if_rejected") or "").strip()
        if not next_reason or next_reason == reason:
            break
        current = audited
    return None


def build_content_evolution_state(
    *,
    posts: list[dict[str, Any]],
    plan: dict[str, Any] | None = None,
    previous_state: dict[str, Any] | None = None,
    source_mutations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    previous = previous_state if isinstance(previous_state, dict) else {}
    recent_posts = _recent_posts_in_hours(posts, hours=LOW_PERFORMANCE_WINDOW_HOURS)
    low_square_titles = _recent_low_performance_square_titles(posts)
    high_performance_patterns = [
        {
            "title": str(item.get("title") or "").strip(),
            "board": _board_name(item),
            "upvotes": int(item.get("upvotes") or 0),
            "comment_count": int(item.get("comment_count") or 0),
        }
        for item in recent_posts
        if int(item.get("upvotes") or 0) >= HIGH_PERFORMANCE_MIN_UPVOTES
        or int(item.get("comment_count") or 0) >= HIGH_PERFORMANCE_MIN_COMMENTS
    ][:6]
    low_performance_patterns = [
        {
            "title": str(item.get("title") or "").strip(),
            "board": _board_name(item),
            "upvotes": int(item.get("upvotes") or 0),
            "comment_count": int(item.get("comment_count") or 0),
            "reason": "square-weakening" if _board_name(item) == "square" else "recent-underperformance",
        }
        for item in recent_posts
        if _board_name(item) == "square" and int(item.get("upvotes") or 0) <= LOW_PERFORMANCE_SQUARE_MAX_UPVOTES
    ][:6]
    return {
        "generated_at": now_utc(),
        "low_performance_patterns": low_performance_patterns,
        "low_performance_square_titles": low_square_titles,
        "high_performance_patterns": high_performance_patterns,
        "observed_board_patterns": {
            "low_performance_square_titles": low_square_titles,
            "high_performance_boards": _dedupe_texts([item.get("board") or "" for item in high_performance_patterns]),
        },
        "source_mutations": source_mutations or previous.get("source_mutations") or previous.get("planner_mutations", []),
        "deletions": previous.get("deletions", []),
        "simplifications": previous.get("simplifications", []),
    }


def _content_objective_summaries(memory_store: dict[str, Any]) -> list[str]:
    if not isinstance(memory_store, dict):
        return []
    candidates: list[str] = []
    for section in ("active_objectives", "user_global_preferences"):
        for item in memory_store.get(section, []):
            summary = truncate_text(str((item or {}).get("summary") or "").strip(), 120)
            if len(summary) < 8:
                continue
            candidates.append(summary)
    return _dedupe_texts(candidates)[:6]
def _build_engagement_targets(
    *,
    signal_summary: dict[str, Any],
    own_username: str,
    own_post_ids: set[str],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_post_ids: set[str] = set()

    def target_score(source: str, item: dict[str, Any]) -> float:
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        created_at = _parse_datetime(item.get("created_at"))
        freshness_bonus = 0.0
        if created_at is not None:
            age_hours = max((datetime.now(timezone.utc) - created_at).total_seconds() / 3600.0, 0.0)
            if age_hours <= 6:
                freshness_bonus = 1.4
            elif age_hours <= 24:
                freshness_bonus = 0.8
            elif age_hours <= 48:
                freshness_bonus = 0.3
        source_bonus = {
            "group-hot": 0.9 if comments > 0 else 0.4,
            "community-hot": 1.0,
            "leaderboard-watch": 0.6,
        }.get(source, 0.0)
        return round(comments * 0.35 + min(upvotes, 260) * 0.03 + freshness_bonus + source_bonus, 2)

    def target_reason(source: str, item: dict[str, Any]) -> str:
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        created_at = _parse_datetime(item.get("created_at"))
        age_hint = ""
        if created_at is not None:
            age_hours = max((datetime.now(timezone.utc) - created_at).total_seconds() / 3600.0, 0.0)
            if age_hours <= 6:
                age_hint = "还在起飞"
            elif age_hours <= 24:
                age_hint = "还在发酵"
            elif age_hours <= 48:
                age_hint = "余温还在"
        heat_bits = []
        if upvotes > 0:
            heat_bits.append(f"{upvotes} 赞")
        if comments > 0:
            heat_bits.append(f"{comments} 评")
        heat_text = " / ".join(heat_bits)
        density_hint = ""
        if comments >= max(12, upvotes // 2):
            density_hint = "评论密度已经够高"
        elif upvotes >= 300:
            density_hint = "公共可见度已经被抬起来"
        elif comments >= 20:
            density_hint = "讨论开始咬住具体分歧"
        if source == "group-hot":
            details = "，".join(part for part in (heat_text and f"热度到 {heat_text}", density_hint) if part)
            prefix = f"{age_hint}的实验室讨论" if age_hint else "实验室里有一段值得接住的讨论"
            return f"{prefix}{('，' + details) if details else ''}，现在切进去最容易把对象、证据和方法边界一起钉住。"
        if source == "community-hot":
            details = "，".join(part for part in (heat_text and f"热度到 {heat_text}", density_hint) if part)
            prefix = f"{age_hint}的公共讨论" if age_hint else "公共讨论正在往上抬"
            return f"{prefix}{('，' + details) if details else ''}，现在接触更适合把现场争论上抬成制度判断。"
        author = str(item.get("author") or item.get("username") or "").strip()
        details = "，".join(part for part in (heat_text and f"已经卷到 {heat_text}", density_hint) if part)
        prefix = f"{author} 的外部帖子" if author else "这条外部帖子"
        return f"{prefix}{('，' + details) if details else ''}，值得正面试探并拿来校验派蒙自己的判断。"

    def priority_bucket(score: float) -> int:
        if score >= 10.0:
            return 0
        if score >= 5.0:
            return 1
        return 2

    def add(post_id: str | None, title: str | None, author: str | None, source: str, item: dict[str, Any]) -> None:
        post_id = str(post_id or "").strip()
        title = str(title or "").strip()
        author = str(author or "").strip()
        if not post_id or not title or not author:
            return
        if author == own_username or post_id in own_post_ids or post_id in seen_post_ids:
            return
        seen_post_ids.add(post_id)
        score = target_score(source, item)
        candidates.append(
            {
                "post_id": post_id,
                "post_title": title,
                "post_author": author,
                "source": source,
                "reason": target_reason(source, item),
                "priority": priority_bucket(score),
                "_score": score,
                "_comment_count": int(item.get("comment_count") or 0),
                "_upvotes": int(item.get("upvotes") or 0),
            }
        )

    group_watch = signal_summary.get("group_watch") or {}
    for item in (group_watch.get("hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "group-hot",
            item,
        )

    for item in (signal_summary.get("community_hot_posts") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("author"),
            "community-hot",
            item,
        )

    for item in (signal_summary.get("competitor_watchlist") or [])[:4]:
        add(
            item.get("post_id"),
            item.get("title"),
            item.get("username"),
            "leaderboard-watch",
            item,
        )

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item.get("_score") or 0.0),
            item.get("priority", 9),
            -int(item.get("_comment_count") or 0),
            -int(item.get("_upvotes") or 0),
            str(item.get("post_title") or ""),
        ),
    )[:6]
    return [
        {
            "post_id": item.get("post_id"),
            "post_title": item.get("post_title"),
            "post_author": item.get("post_author"),
            "source": item.get("source"),
            "reason": item.get("reason"),
            "priority": item.get("priority"),
        }
        for item in ranked
    ]


def _preferred_theory_board(opportunity: dict[str, Any], signal_summary: dict[str, Any]) -> str:
    preferred = str(opportunity.get("preferred_board") or "").strip()
    if preferred in {"philosophy", "square"}:
        return preferred
    signal_type = str(opportunity.get("signal_type") or "")
    low_square_titles = signal_summary.get("content_evolution", {}).get("low_performance_square_titles") or []
    source_text = str(opportunity.get("source_text") or "")
    entry_text = _joined_idea_text(source_text, opportunity.get("angle_hint"), opportunity.get("why_now"))
    source_key = _normalize_title(source_text)
    if low_square_titles and source_key and any(
        (title_key := _normalize_title(str(title or "")))
        and len(title_key) >= 8
        and (title_key in source_key or source_key in title_key)
        for title in low_square_titles[:6]
    ):
        return "philosophy"
    if (
        low_square_titles
        and any(
            _theory_title_emotion_shell_reason(str(title or ""))
            or _theory_title_empathy_shell_reason(str(title or ""))
            or _theory_title_memory_capability_shell_reason(str(title or ""))
            or _theory_title_handoff_gap_reason(str(title or ""))
            for title in low_square_titles[:4]
        )
        and signal_type in LOCAL_THEORY_SINGLE_SOURCE_SIGNAL_TYPES
        and _keyword_hit_count(entry_text, THEORY_BOARD_STRUCTURAL_CUES + THEORY_TITLE_ENTRY_STAKE_TOKENS) >= 4
    ):
        return "philosophy"
    return _infer_theory_board_from_text(entry_text or source_text)


def _preferred_tech_board(opportunity: dict[str, Any]) -> str:
    preferred = str(opportunity.get("preferred_board") or "").strip()
    if preferred in {"skills", "workplace"}:
        return preferred
    signal_type = str(opportunity.get("signal_type") or "")
    if signal_type in {"budget", "failure", "notification-load", "reply-pressure"}:
        return "workplace"
    return "skills"


def _external_signal_type(family: str) -> str:
    normalized = str(family or "").strip() or "external"
    if normalized == "community_breakouts":
        return "community-breakout"
    if normalized == "github_trending":
        return "github"
    if normalized == "zhihu_hot":
        return "zhihu"
    if normalized in {"classic_readings", "marxists"}:
        return "classic"
    if normalized in ACADEMIC_EXTERNAL_FAMILIES:
        return "paper"
    return "external"


def _external_track_freshness_score(item: dict[str, Any], *, signal_type: str) -> float:
    score = 1.15 + min(_external_signal_strength(item), 1.2) * 0.45
    published_at = _parse_datetime(item.get("published_at"))
    if published_at is not None:
        age_hours = max(0.0, (datetime.now(timezone.utc) - published_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
        if age_hours <= 48:
            score += 0.8
        elif age_hours <= 24 * 14:
            score += 0.45
        elif age_hours <= 24 * 90:
            score += 0.18
    if signal_type == "classic":
        score -= 0.55
    return round(max(score, 0.7), 2)


def _external_candidate_supports_group_lane(
    *,
    evidence_hint: str,
    summary_source: str,
    source_seed: str,
) -> bool:
    merged = "\n".join(
        text
        for text in (str(source_seed or "").strip(), str(summary_source or "").strip(), str(evidence_hint or "").strip())
        if text
    )
    if not merged:
        return False
    if evidence_hint:
        return True
    return _track_signal_fit("group", merged) >= max(0.42, _track_signal_threshold("group") - 0.22)


def _external_track_angle_hint(
    track: str,
    *,
    source_seed: str,
    summary_source: str,
    evidence_hint: str,
    signal_type: str,
) -> str:
    focus = truncate_text(
        _concrete_focus_text(source_seed, evidence_hint, summary_source) or source_seed or evidence_hint or "这条外部压力",
        18,
    )
    support = truncate_text(_concrete_focus_text(evidence_hint, summary_source, source_seed) or focus, 48)
    if track == "theory":
        if signal_type == "classic":
            lead = "别复述旧概念，直接把"
        elif signal_type == "paper":
            lead = "别沿着论文包装走，直接把"
        else:
            lead = "别沿着来源包装走，直接把"
        return f"{lead}“{focus}”压成派蒙自己的概念、机制、边界和位置；证据回到{support}。"
    if track == "tech":
        return f"围绕“{focus}”交代对象、触发条件、接手动作和回写校验；证据回到{support}，不要写成导览或心得。"
    return f"把“{focus}”压成可复现的实验对象；至少带着{support}去写案例、日志、反例和协议边界。"


def _iter_external_world_candidates(external_information: dict[str, Any], *, limit: int = 24) -> list[dict[str, Any]]:
    ignored_keys = {
        "source_families",
        "registry_families",
        "reading_notes",
        "bibliography",
        "research_queries",
        "discovery_fetch_terms",
        "research_interest_profile",
        "world_signal_snapshot",
        "world_entry_points",
        "generated_at",
    }
    ranked: list[dict[str, Any]] = []
    for key, value in external_information.items():
        if key in ignored_keys or not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, dict):
                continue
            display_title = _external_candidate_display_title(item)
            if not display_title:
                continue
            ranked.append(
                {
                    **item,
                    "title": display_title,
                    "source_title": str(item.get("title") or "").strip(),
                    "_signal_score": _external_candidate_signal_score(item),
                    "_summary_length": len(
                        str(
                            item.get("pressure")
                            or item.get("relevance_note")
                            or item.get("summary")
                            or item.get("abstract")
                            or item.get("excerpt")
                            or item.get("note")
                            or ""
                        ).strip()
                    ),
                }
            )

    ranked.sort(
        key=lambda item: (
            -float(item.get("_signal_score") or 0.0),
            -int(item.get("_summary_length") or 0),
            -_external_signal_strength(item),
            str(item.get("title") or ""),
        )
    )

    ordered: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in ranked:
        normalized = _normalize_title(str(item.get("title") or ""))
        if not normalized or normalized in seen_titles:
            continue
        seen_titles.add(normalized)
        ordered.append({key: value for key, value in item.items() if not str(key).startswith("_")})
        if len(ordered) >= limit:
            break
    return ordered


def _evidence_hint_from_text(*texts: Any) -> str:
    for raw in texts:
        cleaned = _sanitize_reserved_text(str(raw or "").strip())
        if not cleaned:
            continue
        for fragment in re.split(r"[。！？!?;\n]+", cleaned):
            sentence = fragment.strip()
            if len(sentence) < 8:
                continue
            lowered = sentence.lower()
            if (
                any(token in sentence for token in METHOD_EVIDENCE_TOKENS)
                or any(token in lowered for token in METHOD_EVIDENCE_TOKENS)
                or re.search(r"(before|after|ablation|benchmark|error|failure|metric|trace|log)", lowered)
                or (re.search(r"\d", sentence) and any(token in sentence for token in ("前", "后", "次", "条", "倍", "率")))
            ):
                return truncate_text(sentence, 72)
    return ""


def _external_signal_strength(item: dict[str, Any]) -> float:
    upvotes = int(item.get("upvotes") or 0)
    comments = int(item.get("comment_count") or 0)
    stars = int(item.get("stars") or 0)
    return min(upvotes / 200.0, 1.2) + min(comments / 120.0, 0.8) + min(stars / 4000.0, 1.0)


def _external_candidate_signal_score(item: dict[str, Any]) -> float:
    summary = str(
        item.get("pressure")
        or item.get("relevance_note")
        or item.get("summary")
        or item.get("abstract")
        or item.get("excerpt")
        or item.get("note")
        or ""
    ).strip()
    merged = "\n".join(part for part in (str(item.get("title") or "").strip(), summary) if part)
    lowered = merged.lower()
    score = min(_external_signal_strength(item), 1.6) * 0.45
    if str(item.get("url") or "").strip():
        score += 0.08
    if len(summary) >= 80:
        score += 0.16
    if len(summary) >= 160:
        score += 0.08
    published_at = _parse_datetime(item.get("published_at"))
    if published_at is not None:
        age_hours = max(0.0, (datetime.now(timezone.utc) - published_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
        if age_hours <= 48:
            score += 0.7
        elif age_hours <= 24 * 14:
            score += 0.4
        elif age_hours <= 24 * 90:
            score += 0.15
    evidence_hint = _evidence_hint_from_text(summary, item.get("excerpt"), item.get("abstract"))
    if evidence_hint:
        score += 0.38
    keyword_hits = sum(1 for token in EXTERNAL_THEME_KEYWORD_FRAGMENTS if token in merged or token in lowered)
    if keyword_hits:
        score += min(keyword_hits, 4) * 0.1
    return round(score, 3)


def _external_world_score(
    item: dict[str, Any],
    *,
    relevance_score: float,
    evidence_hint: str,
) -> float:
    summary = str(
        item.get("pressure")
        or item.get("relevance_note")
        or item.get("summary")
        or item.get("abstract")
        or item.get("excerpt")
        or item.get("note")
        or ""
    ).strip()
    score = min(relevance_score, 1.4) * 0.55 + min(_external_signal_strength(item), 1.6) * 0.45
    if evidence_hint:
        score += 0.35
    if len(summary) >= 90:
        score += 0.18
    if len(summary) >= 180:
        score += 0.08
    return round(score, 3)


def _external_candidate_can_anchor_world_lane(
    item: dict[str, Any],
    *,
    relevance_score: float,
    evidence_hint: str,
    world_score: float,
) -> bool:
    if relevance_score >= 0.55 or world_score >= 0.95:
        return True
    if evidence_hint:
        return True
    summary = str(
        item.get("pressure")
        or item.get("relevance_note")
        or item.get("summary")
        or item.get("abstract")
        or item.get("excerpt")
        or item.get("note")
        or ""
    ).strip()
    structural_hits = sum(1 for token in EXTERNAL_THEME_KEYWORD_FRAGMENTS if token in summary.lower() or token in summary)
    if structural_hits >= 3 and len(summary) >= 80:
        return True
    return _external_signal_strength(item) >= 0.45 and len(summary) >= 120


def _external_track_quality_score(
    track: str,
    *,
    source_seed: str,
    summary_source: str,
    evidence_hint: str,
    world_score: float,
    strength: float,
    track_score: float,
) -> float:
    base = 2.0 if track == "tech" else 2.1
    if track == "group":
        base = 2.15
    if _concrete_focus_text(source_seed, evidence_hint, summary_source):
        base += 0.18
    if evidence_hint:
        base += 0.24
    if len(summary_source) >= 90:
        base += 0.1
    if len(summary_source) >= 180:
        base += 0.06
    base += min(world_score, 1.3)
    base += min(strength, 1.2) * 0.28
    base += min(track_score, 1.5) * 0.35
    return round(base, 3)


def _track_signal_fit(track: str, *texts: Any) -> float:
    merged = "\n".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if not merged:
        return 0.0
    lowered = merged.lower()

    def hits(tokens: tuple[str, ...]) -> int:
        total = 0
        for token in tokens:
            if re.search(r"[\u3400-\u9fff]", token):
                matched = token in merged
            else:
                matched = token in lowered
            if matched:
                total += 1
        return total

    theory_hits = hits(THEORY_TRACK_HINT_TOKENS)
    tech_hits = hits(TECH_TRACK_HINT_TOKENS)
    group_hits = hits(GROUP_TRACK_HINT_TOKENS)
    evidence_hits = hits(METHOD_EVIDENCE_TOKENS)
    if track == "theory":
        score = theory_hits * 0.48 + max(0, theory_hits - tech_hits) * 0.12
        if any(token in merged for token in ("解释权", "资格", "制度", "治理", "分层", "劳动")):
            score += 0.32
        if theory_hits == 0 and evidence_hits:
            score -= 0.18
        return round(max(score, 0.0), 3)
    if track == "tech":
        score = tech_hits * 0.42 + evidence_hits * 0.14
        if any(token in merged for token in ("接手", "回写", "回退", "触发", "阈值", "日志")):
            score += 0.28
        if tech_hits == 0 and theory_hits >= 2:
            score -= 0.12
        return round(max(score, 0.0), 3)
    score = group_hits * 0.42 + evidence_hits * 0.18
    if any(token in merged for token in ("实验", "复现", "脚本", "案例", "反例", "日志", "协议边界")):
        score += 0.28
    if evidence_hits == 0:
        score -= 0.25
    return round(max(score, 0.0), 3)


def _track_signal_threshold(track: str) -> float:
    return {
        "theory": 0.45,
        "tech": 0.62,
        "group": 0.78,
    }.get(track, 0.5)


def _selected_track_scores_for_signal(*texts: Any, candidate_tracks: list[str]) -> dict[str, float]:
    cleaned_tracks = [track for track in candidate_tracks if track in {"theory", "tech", "group"}]
    if not cleaned_tracks:
        return {}
    scored = [(track, _track_signal_fit(track, *texts)) for track in cleaned_tracks]
    viable = [(track, score) for track, score in scored if score >= _track_signal_threshold(track)]
    if viable:
        top_score = max(score for _, score in viable)
        return {
            track: score
            for track, score in viable
            if score >= max(_track_signal_threshold(track), top_score - 0.45)
        }
    best_track, best_score = max(scored, key=lambda item: (item[1], -cleaned_tracks.index(item[0])))
    if best_score > 0:
        return {best_track: best_score}
    return {}


def _selected_public_tracks(track_scores: dict[str, float], *, max_tracks: int = 2) -> list[str]:
    scored = [
        (track, float(score))
        for track, score in track_scores.items()
        if track in {"theory", "tech", "group"} and float(score) > 0.0
    ]
    if not scored:
        return []
    scored.sort(key=lambda item: (-item[1], str(item[0])))
    viable = [
        (track, float(score))
        for track, score in scored
        if float(score) >= _track_signal_threshold(track)
    ]
    if not viable:
        return [scored[0][0]]
    viable.sort(key=lambda item: (-item[1], str(item[0])))
    if len(viable) == 1:
        return [viable[0][0]]
    top_score = float(viable[0][1])
    second_score = float(viable[1][1])
    if top_score - second_score >= 0.42:
        return [viable[0][0]]
    return [
        track
        for track, score in viable
        if score >= max(_track_signal_threshold(track), top_score - 0.28)
    ][:max_tracks]


def _rank_dynamic_topic_bundles(signal_summary: dict[str, Any], *, group_enabled: bool) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for track in _live_track_order(signal_summary, group_enabled=group_enabled):
        bundle = _track_signal_bundle(track, signal_summary)
        if not bundle:
            continue
        lead = bundle.get("lead") or {}
        if track in {"tech", "group"}:
            bundle = _method_bundle_projection(bundle, lead, track=track)
            lead = bundle.get("lead") or lead
        priority_entry = _track_priority_entry(track, signal_summary)
        pressure_score = float((priority_entry or {}).get("score") or bundle.get("score") or 0.0)
        ranked.append(
            {
                **bundle,
                "track": track,
                "kind": _track_kind(track),
                "grounded": _bundle_has_grounding(bundle, track=track),
                "pressure_score": round(pressure_score, 2),
                "preferred_board": str(bundle.get("preferred_board") or lead.get("preferred_board") or "").strip(),
            }
        )
    ranked.sort(
        key=lambda item: (
            -float(item.get("pressure_score") or 0.0),
            -int(bool(item.get("grounded"))),
            -int(
                bool(
                    str(item.get("why_now") or item.get("conflict_note") or "").strip()
                    or str((item.get("lead") or {}).get("why_now") or "").strip()
                )
            ),
            -int(
                bool(
                    str((item.get("lead") or {}).get("evidence_hint") or "").strip()
                    or str(item.get("public_focus_text") or item.get("focus_text") or "").strip()
                )
            ),
            -len(list(item.get("items") or [])),
            str(item.get("track") or ""),
        )
    )
    return ranked


def _community_hot_board_scores(posts: list[dict[str, Any]]) -> Counter[str]:
    scores: Counter[str] = Counter()
    for item in posts[:6]:
        board = str(item.get("submolt") or item.get("submolt_name") or "").strip()
        if not board:
            continue
        upvotes = int(item.get("upvotes") or 0)
        comments = int(item.get("comment_count") or 0)
        scores[board] += upvotes * 2 + comments * 3
    return scores


def _public_idea_grounding_score(idea: dict[str, Any]) -> float:
    kind = str(idea.get("kind") or "").strip()
    source_signals = [str(item).strip() for item in list(idea.get("source_signals") or []) if str(item).strip()]
    score = min(len(source_signals), 2) * 0.45
    evidence_hint = str(idea.get("evidence_hint") or "").strip()
    if not evidence_hint:
        evidence_hint = _evidence_hint_from_text(
            idea.get("why_now"),
            idea.get("mechanism_core"),
            *(source_signals[:2]),
        )
    if evidence_hint:
        score += 0.9
    if float(idea.get("world_score") or 0.0) >= 0.6:
        score += 0.75
    if kind == "theory-post" and all(
        str(idea.get(field) or "").strip()
        for field in ("concept_core", "mechanism_core", "boundary_note", "theory_position", "practice_program")
    ):
        score += 0.65
    elif kind == "tech-post" and all(
        str(idea.get(field) or "").strip()
        for field in ("mechanism_core", "boundary_note", "practice_program")
    ):
        score += 0.55
    return round(score, 3)


def _public_heat_signal_weight(item: dict[str, Any]) -> float:
    upvotes = int(item.get("upvotes") or 0)
    comments = int(item.get("comment_count") or 0)
    return round(1.0 + min(upvotes / 260.0, 0.9) + min(comments / 180.0, 0.7), 3)


def _public_heat_signal_texts(item: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(item.get(field) or "").strip()
        for field in ("summary", "reason", "content", "preview", "title", "post_title")
        if str(item.get(field) or "").strip()
    )


def _fragment_overlap_count(left: set[str], right: set[str], *, limit: int = 4) -> int:
    hits = 0
    for left_fragment in left:
        for right_fragment in right:
            if (
                left_fragment == right_fragment
                or left_fragment in right_fragment
                or right_fragment in left_fragment
            ):
                hits += 1
                break
        if hits >= limit:
            return hits
    return hits


def _public_idea_hot_signal_fit(
    idea: dict[str, Any],
    hot_items: list[dict[str, Any]],
) -> dict[str, Any]:
    kind = str(idea.get("kind") or "").strip()
    track = {"theory-post": "theory", "tech-post": "tech"}.get(kind, "")
    if not track or not hot_items:
        return {"score": 0.0, "trigger_title": "", "trigger_board": "", "trigger_pressure": ""}
    idea_fragments = {
        fragment
        for text in [
            idea.get("title"),
            idea.get("angle"),
            idea.get("mechanism_core"),
            idea.get("practice_program"),
            *list(idea.get("source_signals") or []),
        ]
        for fragment in _meaningful_fragments(str(text or ""))
    }
    if not idea_fragments:
        return {"score": 0.0, "trigger_title": "", "trigger_board": "", "trigger_pressure": ""}

    best = {"score": 0.0, "trigger_title": "", "trigger_board": "", "trigger_pressure": ""}
    for item in hot_items:
        signal_texts = _public_heat_signal_texts(item)
        if not signal_texts:
            continue
        signal_fragments = {
            fragment
            for text in signal_texts
            for fragment in _meaningful_fragments(text)
        }
        overlap_hits = _fragment_overlap_count(idea_fragments, signal_fragments)
        track_fit = _track_signal_fit(track, *signal_texts)
        heat_weight = _public_heat_signal_weight(item)
        evidence_hint = _evidence_hint_from_text(*signal_texts)
        score = overlap_hits * 0.85 + min(track_fit, 1.5) * 0.72 + min(heat_weight, 2.2) * 0.22
        if evidence_hint:
            score += 0.12
        if overlap_hits == 0 and track_fit < _track_signal_threshold(track):
            score -= 0.55
        if score <= float(best.get("score") or 0.0):
            continue
        best = {
            "score": round(max(score, 0.0), 3),
            "trigger_title": str(item.get("title") or item.get("post_title") or "").strip(),
            "trigger_board": str(item.get("submolt") or item.get("submolt_name") or "").strip(),
            "trigger_pressure": truncate_text(
                _preferred_signal_seed_text(item, field_order=("summary", "reason", "content", "preview", "title"), limit=72),
                72,
            ),
        }
    return best


def _public_hot_forum_override(
    signal_summary: dict[str, Any],
    ideas: list[dict[str, Any]],
    last_run: dict[str, Any],
) -> dict[str, Any]:
    public_ideas = {str(item.get("kind") or ""): item for item in ideas if item.get("kind") in {"theory-post", "tech-post"}}
    if not public_ideas:
        return {"enabled": False}
    grounded_public_ideas = {
        kind: idea
        for kind, idea in public_ideas.items()
        if _public_idea_grounding_score(idea) >= 1.35
    }
    if not grounded_public_ideas:
        return {"enabled": False}

    recent_primary_kind = _recent_primary_publish_kind(last_run)
    community_hot_posts = signal_summary.get("community_hot_posts") or []
    competitor_watchlist = signal_summary.get("competitor_watchlist") or []
    board_scores = _community_hot_board_scores(community_hot_posts)
    hottest_board = board_scores.most_common(1)[0][0] if board_scores else ""

    strong_public_signal = any(
        int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES
        or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS
        for item in community_hot_posts[:6]
    )
    if not strong_public_signal:
        strong_public_signal = any(
            int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES * 2
            or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS * 2
            for item in competitor_watchlist[:6]
        )
    if not strong_public_signal:
        return {"enabled": False}

    hot_items = list(community_hot_posts[:6]) + list(competitor_watchlist[:4])
    fit_by_kind = {
        kind: _public_idea_hot_signal_fit(idea, hot_items)
        for kind, idea in grounded_public_ideas.items()
    }
    matched_kinds = {
        kind: fit
        for kind, fit in fit_by_kind.items()
        if float(fit.get("score") or 0.0) >= 1.0
    }
    if not matched_kinds:
        return {"enabled": False}

    preferred_kinds = sorted(
        matched_kinds,
        key=lambda kind: (
            -float(matched_kinds[kind].get("score") or 0.0),
            -_public_idea_grounding_score(grounded_public_ideas[kind]),
            kind,
        ),
    )
    preferred_kinds.extend(
        kind
        for kind in sorted(
            grounded_public_ideas,
            key=lambda kind: (
                -_public_idea_grounding_score(grounded_public_ideas[kind]),
                -float(fit_by_kind.get(kind, {}).get("score") or 0.0),
                kind,
            ),
        )
        if kind not in preferred_kinds
    )

    top_fit = matched_kinds.get(preferred_kinds[0], {}) if preferred_kinds else {}
    trigger_title = str(top_fit.get("trigger_title") or "").strip()
    trigger_pressure = str(top_fit.get("trigger_pressure") or "").strip()
    trigger_board = str(top_fit.get("trigger_board") or "").strip()

    priority_bonus = 0.55 + min(float(top_fit.get("score") or 0.0), 2.4) * 0.18
    if any(
        int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES * 2
        or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS * 2
        for item in community_hot_posts[:6]
    ):
        priority_bonus += 0.18
    if any(
        int(item.get("upvotes") or 0) >= COMMUNITY_HOT_FORUM_MIN_UPVOTES * 2
        or int(item.get("comment_count") or 0) >= COMMUNITY_HOT_FORUM_MIN_COMMENTS * 2
        for item in competitor_watchlist[:4]
    ):
        priority_bonus += 0.08

    trigger_label = truncate_text(trigger_pressure or trigger_title or "当前样本", 36)
    reason = (
        f"公共热度正在把“{trigger_label}”往台前推；"
        "但它只该给已经长成的公共候选加一点顺风，不能压过修复、守场或外部切口。"
    )
    if trigger_pressure and trigger_pressure != trigger_label:
        reason += f" 咬住的对象是：{trigger_pressure}。"
    elif trigger_board:
        reason += f" 触发现场落在 `{trigger_board}`。"
    if recent_primary_kind == "create-post":
        reason += " 上一轮已经发过 forum，但外部公共压力还在持续。"
    return {
        "enabled": True,
        "preferred_kinds": preferred_kinds,
        "hottest_board": hottest_board,
        "recent_primary_kind": recent_primary_kind,
        "priority_bonus": round(min(priority_bonus, 1.35), 2),
        "reason": reason,
    }


def _dynamic_opportunities(
    *,
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    heartbeat_hours: int,
) -> list[dict[str, Any]]:
    del recent_titles
    opportunities: list[dict[str, Any]] = []
    unread_notifications = int((signal_summary.get("account") or {}).get("unread_notification_count") or 0)
    literary_pick = signal_summary.get("literary_pick") or {}
    unresolved = signal_summary.get("unresolved_failures") or []
    reply_posts = signal_summary.get("pending_reply_posts") or []
    feed_watchlist = signal_summary.get("feed_watchlist") or []
    group_watch = signal_summary.get("group_watch") or {}
    top_discussion = signal_summary.get("top_discussion_posts") or []
    external_information = signal_summary.get("external_information") or {}
    world_entry_points = [
        item
        for item in list(external_information.get("world_entry_points") or [])
        if isinstance(item, dict)
    ]
    external_world_candidates = _iter_external_world_candidates(external_information)
    community_hot_posts = _high_like_external_posts(
        list(signal_summary.get("community_hot_posts") or signal_summary.get("feed_watchlist") or [])
    )
    competitor_watchlist = _high_like_external_posts(list(signal_summary.get("competitor_watchlist") or []))
    rising_hot_posts = _high_like_external_posts(list(signal_summary.get("rising_hot_posts") or []))

    def add_source(
        track: str,
        signal_type: str,
        source_text: str,
        *,
        why_now: str = "",
        angle_hint: str = "",
        preferred_board: str | None = None,
        quality_score: float = 0.0,
        freshness_score: float = 0.0,
        evidence_hint: str = "",
        world_score: float = 0.0,
    ) -> None:
        source_text = str(source_text or "").strip()
        if not source_text:
            return
        opportunity = {
            "track": track,
            "signal_type": signal_type,
            "source_text": source_text,
            "why_now": str(why_now or "").strip(),
            "angle_hint": str(angle_hint or "").strip(),
            "overlap_score": _text_overlap_score(source_text, signal_summary.get("novelty_pressure") or {}),
            "quality_score": quality_score,
            "freshness_score": freshness_score,
            "evidence_hint": str(evidence_hint or "").strip(),
            "world_score": float(world_score or 0.0),
        }
        if preferred_board in {"square", "philosophy", "skills", "workplace"}:
            opportunity["preferred_board"] = preferred_board
        publishability_penalty = _opportunity_publishability_penalty(opportunity)
        evidence_rich_world_note = bool(
            signal_type in {"paper", "external", "github", "community-breakout", "zhihu"}
            and opportunity["why_now"]
            and opportunity["evidence_hint"]
            and float(world_score or 0.0) >= 0.8
        )
        if (
            signal_type != "failure"
            and not opportunity["why_now"]
            and not opportunity["evidence_hint"]
            and publishability_penalty >= 2.5
        ):
            return
        if (
            signal_type != "failure"
            and track in {"tech", "group"}
            and publishability_penalty >= 6.0
            and not evidence_rich_world_note
        ):
            return
        opportunities.append(opportunity)

    def add_selected_tracks(
        *,
        source_text: str,
        signal_type: str,
        why_now: str,
        evidence_hint: str,
        track_scores: dict[str, float],
        track_options: dict[str, dict[str, Any]],
        max_tracks: int = 2,
    ) -> None:
        for track in _selected_public_tracks(track_scores, max_tracks=max_tracks):
            options = dict(track_options.get(track) or {})
            if not options:
                continue
            add_source(
                track,
                signal_type,
                source_text,
                why_now=why_now,
                evidence_hint=evidence_hint,
                **options,
            )

    def add_external_world_item(item: dict[str, Any]) -> None:
        title = str(item.get("title") or "").strip()
        family = str(item.get("family") or "").strip() or "external"
        signal_type = str(item.get("signal_type") or "").strip() or _external_signal_type(family)
        summary_source = str(
            item.get("pressure")
            or item.get("summary")
            or item.get("evidence")
            or item.get("relevance_note")
            or item.get("abstract")
            or item.get("excerpt")
            or item.get("lens")
            or item.get("note")
            or ""
        ).strip()
        summary = truncate_text(summary_source, 180)
        source_seed = (
            _object_led_signal_anchor(
                item,
                field_order=("pressure", "summary", "evidence", "abstract", "excerpt", "note", "title"),
                limit=72,
            )
            or _external_candidate_display_title(item)
            or _signal_seed_text(item.get("pressure"), summary_source, title, limit=72)
            or title
            or summary
        )
        if not source_seed:
            return
        relevance_score = max(
            float(item.get("problem_fit_score") or 0.0),
            _external_candidate_relevance(item, signal_summary),
        )
        evidence_hint = _evidence_hint_from_text(
            item.get("evidence"),
            summary_source,
            item.get("excerpt"),
            item.get("summary"),
        )
        strength = _external_signal_strength(item)
        world_score = max(
            float(item.get("world_score") or 0.0),
            _external_world_score(
                item,
                relevance_score=relevance_score,
                evidence_hint=evidence_hint,
            ),
        )
        if family in ACADEMIC_EXTERNAL_FAMILIES and not _external_candidate_can_anchor_world_lane(
            item,
            relevance_score=relevance_score,
            evidence_hint=evidence_hint,
            world_score=world_score,
        ):
            return
        track_scores = _selected_track_scores_for_signal(
            source_seed,
            title,
            summary_source,
            evidence_hint,
            candidate_tracks=["theory", "tech"],
        )
        group_track_score = _track_signal_fit("group", source_seed, title, summary_source, evidence_hint)
        if _external_candidate_supports_group_lane(
            evidence_hint=evidence_hint,
            summary_source=summary_source,
            source_seed=source_seed,
        ) and group_track_score >= _track_signal_threshold("group"):
            track_scores["group"] = group_track_score
        add_selected_tracks(
            source_text=source_seed,
            signal_type=signal_type,
            why_now=summary,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                track: {
                    "angle_hint": _external_track_angle_hint(
                        track,
                        source_seed=source_seed,
                        summary_source=summary_source,
                        evidence_hint=evidence_hint,
                        signal_type=signal_type,
                    ),
                    "quality_score": _external_track_quality_score(
                        track,
                        source_seed=source_seed,
                        summary_source=summary_source,
                        evidence_hint=evidence_hint,
                        world_score=world_score,
                        strength=strength,
                        track_score=track_scores[track],
                    ),
                    "freshness_score": (
                        max(1.2, _external_track_freshness_score(item, signal_type=signal_type) - 0.1)
                        if track == "group"
                        else _external_track_freshness_score(item, signal_type=signal_type)
                    ),
                    "world_score": world_score,
                }
                for track in track_scores
            },
        )

    for bundle in list(external_information.get("discovery_bundles") or [])[:6]:
        bundle_seeds = _bundle_world_seed_texts(bundle, limit=3)
        focus = _world_bundle_focus_source(bundle) or str(
            (bundle_seeds[0] if bundle_seeds else bundle.get("query")) or ""
        ).strip()
        lenses = [
            item
            for item in bundle_seeds
            if _normalize_title(item) != _normalize_title(focus)
        ][:2]
        if not focus:
            continue
        why_now = _world_bundle_reason(bundle)
        evidence_hint = truncate_text("、".join(lenses) or focus, 72)
        bundle_track_scores = _selected_track_scores_for_signal(
            focus,
            bundle.get("conflict_note"),
            bundle.get("rationale"),
            why_now,
            evidence_hint,
            candidate_tracks=["theory", "tech"],
        )
        add_selected_tracks(
            source_text=focus,
            signal_type="world-bundle",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=bundle_track_scores,
            track_options={
                "theory": {
                    "angle_hint": _world_bundle_angle(bundle, track="theory"),
                    "quality_score": 4.8 + min(bundle_track_scores.get("theory", 0.0), 1.4) * 0.35,
                    "freshness_score": 2.4,
                    "world_score": 0.95 + min(len(lenses), 2) * 0.15,
                },
                "tech": {
                    "angle_hint": _world_bundle_angle(bundle, track="tech"),
                    "quality_score": 4.3 + min(bundle_track_scores.get("tech", 0.0), 1.4) * 0.35,
                    "freshness_score": 2.2,
                    "world_score": 0.85 + min(len(lenses), 2) * 0.12,
                },
            },
        )

    for item in world_entry_points[:8]:
        add_external_world_item(item)

    for item in external_world_candidates:
        add_external_world_item(item)

    for item in rising_hot_posts[:3]:
        title = _object_led_signal_anchor(item, field_order=("summary", "content", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "content", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "content", "title"))
        track_scores = _selected_track_scores_for_signal(title, why_now, candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="rising-hot",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 4.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.25,
                    "freshness_score": 3.0,
                },
                "tech": {
                    "quality_score": 3.5 + min(track_scores.get("tech", 0.0), 1.2) * 0.25,
                    "freshness_score": 3.0,
                },
            },
        )
    for item in community_hot_posts[:4]:
        title = _object_led_signal_anchor(item, field_order=("summary", "content", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "content", "reason", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "content", "reason", "title"))
        track_scores = _selected_track_scores_for_signal(title, why_now, item.get("summary"), candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="community-hot",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 4.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.25,
                    "freshness_score": 2.0,
                },
                "tech": {
                    "quality_score": 3.0 + min(track_scores.get("tech", 0.0), 1.2) * 0.25,
                    "freshness_score": 2.0,
                },
            },
        )
    for item in (group_watch.get("hot_posts") or [])[:3]:
        title = _object_led_signal_anchor(item, field_order=("summary", "content", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "content", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "content", "title"))
        track_scores = _selected_track_scores_for_signal(
            title,
            item.get("summary"),
            item.get("content"),
            candidate_tracks=["theory", "tech"],
        )
        group_track_score = _track_signal_fit("group", title, item.get("summary"), item.get("content"))
        if group_track_score >= _track_signal_threshold("group"):
            track_scores["group"] = group_track_score
        add_selected_tracks(
            source_text=title,
            signal_type="discussion",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 2.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.25,
                    "freshness_score": 1.0,
                },
                "tech": {
                    "quality_score": 2.5 + min(track_scores.get("tech", 0.0), 1.2) * 0.25,
                    "freshness_score": 1.0,
                },
                "group": {
                    "quality_score": 2.5 + min(track_scores.get("group", 0.0), 1.5) * 0.25,
                    "freshness_score": 1.0,
                },
            },
        )
    for item in competitor_watchlist[:4]:
        title = _object_led_signal_anchor(item, field_order=("summary", "reason", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "reason", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "reason", "title"))
        track_scores = _selected_track_scores_for_signal(title, item.get("summary"), item.get("reason"), candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="discussion",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 3.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.25,
                    "freshness_score": 1.5,
                },
                "tech": {
                    "quality_score": 3.0 + min(track_scores.get("tech", 0.0), 1.2) * 0.25,
                    "freshness_score": 1.5,
                },
            },
        )
    for item in unresolved[:2]:
        title = _object_led_signal_anchor(item, field_order=("summary", "post_title", "error"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "error", "post_title"),
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "error", "post_title"))
        track_scores = _selected_track_scores_for_signal(title, why_now, evidence_hint, candidate_tracks=["tech", "group"])
        add_selected_tracks(
            source_text=title,
            signal_type="failure",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "tech": {
                    "quality_score": 2.0,
                    "freshness_score": 1.0,
                },
                "group": {
                    "quality_score": 2.0,
                    "freshness_score": 1.0,
                },
            },
        )
    for item in reply_posts[:2]:
        title = _object_led_signal_anchor(item, field_order=("summary", "post_title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "post_title", "preview"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "post_title", "preview"))
        track_scores = _selected_track_scores_for_signal(title, item.get("summary"), candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="reply-pressure",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 1.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
                "tech": {
                    "quality_score": 1.0 + min(track_scores.get("tech", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
            },
        )
    for item in feed_watchlist[:3]:
        title = _object_led_signal_anchor(item, field_order=("summary", "reason", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "reason", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "reason", "title"))
        track_scores = _selected_track_scores_for_signal(title, item.get("summary"), item.get("reason"), candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="feed",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 2.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
                "tech": {
                    "quality_score": 2.0 + min(track_scores.get("tech", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
            },
        )
    for item in top_discussion[:2]:
        title = _object_led_signal_anchor(item, field_order=("summary", "post_title", "title"), limit=72)
        why_now = _opportunity_live_why_now(
            item,
            field_order=("summary", "post_title", "preview", "title"),
            include_heat=True,
        )
        evidence_hint = _opportunity_evidence_hint(item, field_order=("summary", "post_title", "preview", "title"))
        track_scores = _selected_track_scores_for_signal(title, item.get("summary"), candidate_tracks=["theory", "tech"])
        add_selected_tracks(
            source_text=title,
            signal_type="discussion",
            why_now=why_now,
            evidence_hint=evidence_hint,
            track_scores=track_scores,
            track_options={
                "theory": {
                    "quality_score": 2.0 + min(track_scores.get("theory", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
                "tech": {
                    "quality_score": 2.0 + min(track_scores.get("tech", 0.0), 1.2) * 0.2,
                    "freshness_score": 1.0,
                },
            },
        )
    for hint in signal_summary.get("user_topic_hints", [])[:4]:
        hint_matches = _hint_matching_world_texts(hint, signal_summary)
        if not hint_matches:
            continue
        hint_text = _concrete_focus_text(hint.get("text"), hint.get("note"), limit=72)
        if not hint_text:
            continue
        track = _infer_hint_track(hint)
        preferred_board = str(hint.get("board") or "").strip()
        add_source(
            track,
            "user-hint",
            hint_text,
            why_now=_object_level_pressure_text(
                hint.get("note"),
                *hint_matches,
                fallback=str(hint.get("note") or hint_matches[0] or "").strip(),
            ),
            preferred_board=preferred_board,
            quality_score=1.6 + min(len(hint_matches), 2) * 0.18,
            freshness_score=0.8,
            evidence_hint=truncate_text("、".join(hint_matches), 72),
            world_score=0.45,
        )

    ranked = sorted(
        opportunities,
        key=lambda item: (
            -_opportunity_rank_score(item, signal_summary=signal_summary),
            -float(item.get("quality_score") or 0.0),
            -float(item.get("freshness_score") or 0.0),
            item.get("overlap_score", (0, 0, 0)),
            str(item.get("track") or ""),
            len(item["source_text"]),
        ),
    )
    deduped: list[dict[str, Any]] = []
    seen_sources: set[tuple[str, str]] = set()
    for item in ranked:
        key = (item["track"], item["source_text"])
        if key in seen_sources:
            continue
        seen_sources.add(key)
        deduped.append(item)
    return deduped


def _planning_signals(
    *,
    home: dict[str, Any],
    posts: list[dict[str, Any]],
    overview: dict[str, Any],
    feed: list[dict[str, Any]],
    heartbeat_tasks: list[dict[str, Any]],
    last_run: dict[str, Any],
    groups: list[dict[str, Any]],
    literary_pick: dict[str, Any] | None,
) -> dict[str, Any]:
    activity = _extract_activity(home)
    community_watch = _load("community_watch").get("data", {})
    memory_store = _load("memory_store")
    external_information = _load("external_information")
    source_mutation = _load("source_mutation_state")
    low_heat_failures = _load("low_heat_failures")
    content_evolution = build_content_evolution_state(
        posts=posts,
        previous_state=_load("content_evolution_state"),
    )
    user_topic_hints = _extract_user_topic_hints(_load("user_topic_hints"))
    home_hot_posts = [
        {
            "post_id": item.get("post_id"),
            "title": item.get("title"),
            "author": item.get("author"),
            "submolt": item.get("submolt_name"),
            "upvotes": item.get("upvotes"),
            "comment_count": item.get("comment_count"),
            "created_at": item.get("created_at"),
        }
        for item in _extract_home_hot_posts(home)
    ]
    community_hot_posts = community_watch.get("home_hot_posts") or home_hot_posts
    home_hot_index = {str(item.get("post_id") or ""): item for item in home_hot_posts if item.get("post_id")}
    enriched_community_hot_posts: list[dict[str, Any]] = []
    for item in community_hot_posts:
        post_id = str(item.get("post_id") or "")
        fallback = home_hot_index.get(post_id, {})
        enriched_community_hot_posts.append(
            {
                **fallback,
                **item,
                "created_at": item.get("created_at") or fallback.get("created_at"),
            }
        )
    community_hot_posts = enriched_community_hot_posts or home_hot_posts
    competitor_watchlist = _flatten_competitor_watch(community_watch)
    group_watch = community_watch.get("owned_group_watch") or {}
    top_discussion = sorted(
        activity,
        key=lambda item: int(item.get("new_notification_count") or 0),
        reverse=True,
    )[:5]
    reply_summary = _reply_task_summary(heartbeat_tasks)
    failures = _failure_summary(last_run)
    hot_theory = _top_post_by_board(posts, overview, boards={"philosophy", "square"})
    hot_tech = _top_post_by_board(posts, overview, boards={"skills", "workplace"})
    hot_group = next(
        (
            item
            for item in sorted(posts, key=_post_metric, reverse=True)
            if "实验室" in str(item.get("title") or "") or "小组" in str(item.get("title") or "")
        ),
        None,
    )
    content_objectives = _content_objective_summaries(memory_store)
    rising_hot_posts = _rising_hot_posts(
        community_hot_posts=community_hot_posts,
        feed_watchlist=[
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
            }
            for item in feed[:8]
        ],
        competitor_watchlist=competitor_watchlist,
        captured_at=overview.get("captured_at") or community_watch.get("captured_at") or now_utc(),
    )
    research_texts = _planning_research_texts(
        top_discussion=top_discussion,
        community_hot_posts=community_hot_posts,
        feed=feed,
        competitor_watchlist=competitor_watchlist,
        rising_hot_posts=rising_hot_posts,
        external_information=external_information,
        content_objectives=content_objectives,
        user_topic_hints=user_topic_hints,
    )
    keyword_counter = _candidate_terms(research_texts)
    recent_titles = [str(item.get("title") or "") for item in posts[:RECENT_TITLE_LIMIT] if item.get("title")]
    novelty = _novelty_pressure(recent_titles)
    heartbeat_hours = 3
    config_path = CURRENT_STATE_DIR.parent.parent / "config" / "paimon.json"
    if config_path.exists():
        config = read_json(config_path, default={})
        heartbeat_hours = int(config.get("automation", {}).get("heartbeat_hours", heartbeat_hours) or heartbeat_hours)
    base_summary = {
        "account": {
            "score": overview.get("score"),
            "followers": overview.get("follower_count"),
            "following": overview.get("following_count"),
            "unread_notification_count": overview.get("unread_notification_count"),
        },
        "top_discussion_posts": [
            {
                "post_id": item.get("post_id"),
                "title": item.get("post_title"),
                "submolt": item.get("submolt_name"),
                "new_notification_count": item.get("new_notification_count"),
                "preview": item.get("preview"),
            }
            for item in top_discussion
        ],
        "pending_reply_posts": reply_summary[:5],
        "unresolved_failures": [
            {
                "kind": item.get("kind"),
                "post_id": item.get("post_id"),
                "post_title": item.get("post_title"),
                "error": item.get("error"),
            }
            for item in failures
        ],
        "recent_top_posts": overview.get("recent_top_posts", [])[:5],
        "hot_theory_post": hot_theory,
        "hot_tech_post": hot_tech,
        "hot_group_post": hot_group,
        "feed_watchlist": [
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
                "upvotes": item.get("upvotes"),
                "comment_count": item.get("comment_count"),
                "created_at": item.get("created_at"),
            }
            for item in feed[:6]
        ],
        "community_hot_posts": community_hot_posts[:8],
        "competitor_watchlist": competitor_watchlist[:8],
        "rising_hot_posts": rising_hot_posts,
        "group_watch": group_watch,
        "content_objectives": content_objectives,
        "external_information": external_information,
        "content_evolution": content_evolution,
        "source_mutation": source_mutation,
        "low_heat_failures": low_heat_failures,
        "user_topic_hints": user_topic_hints,
        "top_keywords": [token for token, count in keyword_counter.most_common(8) if count >= 1],
        "novelty_pressure": novelty,
        "group": groups[0] if groups else {},
        "literary_pick": literary_pick,
    }
    dynamic_topics = _dynamic_opportunities(
        signal_summary=base_summary,
        recent_titles=recent_titles,
        heartbeat_hours=heartbeat_hours,
    )
    signal_summary = {**base_summary, "dynamic_topics": dynamic_topics}
    signal_summary["dynamic_topic_bundles"] = _rank_dynamic_topic_bundles(
        signal_summary,
        group_enabled=bool(group_watch),
    )
    return signal_summary


def _planner_idea_schema(allowed_kinds: list[str]) -> dict[str, Any]:
    kinds = [str(item) for item in allowed_kinds if str(item)]
    return {
        "type": "array",
        "minItems": 1,
        "maxItems": len(kinds),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "kind": {"type": "string", "enum": kinds},
                "title": {"type": "string"},
                "angle": {"type": "string"},
                "why_now": {"type": "string"},
                "source_signals": {"type": "array", "items": {"type": "string"}},
                "novelty_basis": {"type": "string"},
                "series_key": {"type": "string"},
                "series_prefix": {"type": "string"},
                "is_followup": {"type": "boolean"},
                "part_number": {"type": "integer", "minimum": 1},
                "submolt": {"type": "string"},
                "board_profile": {"type": "string"},
                "hook_type": {"type": "string"},
                "cta_type": {"type": "string"},
                "innovation_claim": {"type": "string"},
                "innovation_class": {"type": "string", "enum": list(INNOVATION_CLASSES)},
                "innovation_delta_vs_recent": {"type": "string"},
                "innovation_delta_vs_self": {"type": "string"},
                "board_risk_note": {"type": "string"},
                "concept_core": {"type": "string"},
                "mechanism_core": {"type": "string"},
                "boundary_note": {"type": "string"},
                "theory_position": {"type": "string"},
                "practice_program": {"type": "string"},
            },
            "required": ["kind", "title", "angle", "why_now", "source_signals", "novelty_basis", "is_followup"],
        },
    }


def _generate_codex_ideas(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    *,
    allowed_kinds: list[str],
    lane_strategy: dict[str, Any],
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
    retry_feedback: list[str] | None = None,
) -> list[dict[str, Any]]:
    prompt_signal_summary = dict(signal_summary)
    prompt_signal_summary["community_hot_posts"] = _high_like_external_posts(
        list(signal_summary.get("community_hot_posts") or [])
    )
    prompt_signal_summary["competitor_watchlist"] = _high_like_external_posts(
        list(signal_summary.get("competitor_watchlist") or [])
    )
    prompt_signal_summary["rising_hot_posts"] = _high_like_external_posts(
        list(signal_summary.get("rising_hot_posts") or [])
    )
    prompt_signal_summary["reserved_title_phrases"] = list(RESERVED_TITLE_PHRASES)
    prompt_signal_summary["user_topic_hints"] = signal_summary.get("user_topic_hints") or []
    prompt_signal_summary["content_evolution"] = signal_summary.get("content_evolution") or {}
    retry_lines = retry_feedback or []
    kinds_text = "、".join(allowed_kinds) or "theory-post、tech-post"
    prompt = f"""
你在给 InStreet 账号派蒙做下一轮内容规划。请根据实时信号生成候选 idea。

硬约束：
1. 不要复用固定题库，不要按预设 sequence 输出。
2. 必须基于下面给出的实时信号构思标题、角度和 why_now。
3. 本轮只允许输出这些 kind：{kinds_text}。它们是候选上限，不是必须补齐的配额；真正够强的 1 条也可以直接交。
4. 这些 kind 是根据实时压力动态选出来的：{truncate_text(str(lane_strategy), 1200)}
5. 如果是追爆款或续篇，标题必须显式变化，不能与最近标题完全相同；但不要只靠替换“续篇/补篇/之后/下一步”来伪装成新选题。
6. 每个 idea 的 `source_signals` 必须写成对象级压力短句，优先交对象、断口、证据和正在起量的现场，不要写“机会点/世界线索束/先别绕开”这类后台口吻。
7. 标题必须中文，适合公开发布，不要输出空泛抽象标题。
8. 明确避开最近已经过载的母题与热词。先看 `dynamic_topic_bundles` 里这轮真正互相咬合的信号，再下潜到 `dynamic_topics`；如果 bundle 里已经给出 `public_title_seed` / `public_focus_text`，沿着那个公共题眼起题，但别把 bundle 本身写成新的固定入口。
9. `content_objectives` 和 `user_topic_hints` 只当灵感源，不是强制命令；可以采纳、改写、反转或忽略。
10. 如果公共热点够强，而且这轮确实有长成的 public lane，可以让其中 1 个候选正面回应它；但不能停在“社区里最近在聊什么”，必须把热点上抬成 `Agent社会` 的结构问题。
11. 社区热点只是样本，不是结论。`theory-post` 至少要回答一个问题：这正在形成什么社会关系、制度安排、价值形式、分层机制或治理问题？
12. 默认使用 `Agent社会` / `AI社会` 的框架词，不要把问题停在 `Agent社区`；只有引用既有作品标题、平台模块或原帖原话时才保留 `社区` 说法。
13. 允许更随机、更发散、更炸裂：不要默认保守，要敢于给出反常识、逆向、带判断力的标题。
14. 不要把“实验室/连载/派蒙自己的状态”自动写成主语；只有当它们已经被明确转译成 Agent 社会问题、方法对象或制度冲突时，才让它们站到第一屏。
15. `theory-post` 要在 `square` 和 `philosophy` 之间按入口姿态选择：公共代入、经验召回、低门槛冲突更适合 `square`；概念命名、结构判断、站队反驳更适合 `philosophy`。不要把任何一个版块当默认归宿。
16. `theory-post` 的 `submolt` 只能是 `square` 或 `philosophy`；`tech-post` 的 `submolt` 只能是 `skills` 或 `workplace`；`group-post` 仍然要写成实验框架，但别把 `skills` 当自动完成任务的仪式字段。
17. 版块写法必须分开：
   - `square`：公共情绪入口、低门槛参与、标题要有冲突感，结尾要能让别人立刻补自己的经历。
   - `workplace`：反直觉诊断、病灶命名、隐性成本、替代机制。
   - `philosophy`：悖论、困境、真相、结构判断，要能引发站队或反驳。
   - `skills`：数字、前后对比、失败链路、可复制规则；标题第一屏必须先点明具体对象、故障或收益，不要只报“4 段协议”“一套框架”这种内部包装，也不要写成“场景 A、场景 B：16 人访谈 + 1 段日志，逼出 4 个节点”这种先晒取材过程的门口；也不要把“收到 / 已响应 / 已处理”这类状态词排成门口，再补“6 条规则 / 改成责任链”，那还是在卖命名整理，不是在交对象级方法；如果题目来自结算页、续费页、订票页这类公共产品界面，也别把“我知道这里不对 / 识别到风险”这种用户内心独白，或者“支付前才冒出的平台费”这种用户侧惊讶句挂在门口，先交产品侧对象、接手动作和验证收益。
前置门槛：
在提 idea 之前先过三道前置门槛，不要把它们留给后面的筛选和 repair：
   - `tech-post` / `group-post` 必须先有公开对象锚点：工单、评论线程、审核队列、回写字段、订单页、日志切面、接口断口这类东西至少占住标题第一屏或 `source_signals`；如果你只能写“错误日志”“静默失败”“评论区从争论变成了点赞”这种抽象门口，这个 idea 还没长成。
   - `why_now` 如果来自 `world-bundle` / 外部热帖 / 外部论文，必须已经被消化成对象级失败句或对照句，再进 idea；不能只借一个概念壳、情绪词或热标题来抬门面。
   - 如果这条 idea 和最近低热帖仍在共享同一组机制簇，就必须同时换掉对象链和证据链；只换概念名、只换修辞、只把旧协议改叫新名字，都不要输出。
18. 如能判断，请补充 `board_profile`、`hook_type`、`cta_type`。
   - `square` 默认：`board_profile=square`, `hook_type=public-emotion`, `cta_type=comment-scene`
   - `workplace` 默认：`board_profile=workplace`, `hook_type=diagnostic`, `cta_type=comment-diagnostic`
   - `philosophy` 默认：`board_profile=philosophy`, `hook_type=paradox`, `cta_type=take-a-position`
   - `skills` 默认：`board_profile=skills`, `hook_type=practical-yield`, `cta_type=comment-case-or-save`
   - 但 `theory-post` 发在 `square` 时，不要再用 `comment-scene` 让评论区补第一条证据；优先改成 `comment-variant` 或 `take-a-position`，只收反例、变体和不同判断。
19. 如果实时信号里出现 `rising_hot_posts`，把它们当成正在起飞的新兴热点样本线，不要只盯成熟热榜。
20. 成熟外部“高赞样本”默认只认 `>=200` 赞；`rising_hot_posts` 例外，它们代表正在起飞的样本，不要和成熟高热混在一起。
21. 允许学习别人的问题压力和盲点，但不要学习标题骨架、系列包装或 IP 话术；尤其不要出现这些保留词：{", ".join(RESERVED_TITLE_PHRASES)}。
22. 每个 `theory-post` 和 `tech-post` 都必须显式推进至少一种创新：`new_concept`、`new_mechanism`、`new_theory`、`new_practice`。
23. 输出 `innovation_claim`、`innovation_class`、`innovation_delta_vs_recent`、`innovation_delta_vs_self`；创新重点在选题和判断，不要把“我有多创新”写进正文。
24. `theory-post` 不能只给一个判断，必须同时写出 `concept_core`、`mechanism_core`、`boundary_note`、`theory_position`、`practice_program`，形成一个完整理论单元。
25. `tech-post` 和 `group-post` 至少要写出 `mechanism_core` 与 `practice_program`，不能只是故障复盘或 6 步清单。
26. 标题和各字段都不要落回低自主性写法。禁止出现“从《…》继续追问”“把《…》拆开看”“整理成 6 步方法”“导读/摘录某文”这类骨架。
27. 不要让标题借外部材料说话，也不要让 `concept_core` / `mechanism_core` / `practice_program` 变成外部材料的改写摘要。
28. 把大量外部信息场都当灵感池：社区高热帖子、知乎、GitHub 热门项目、前沿论文/预印本、经典政治经济学/社会理论材料都可以进入参考，但最终标题和理论命名必须是派蒙自己的。
29. 如果本地信号不够，请主动扩大探索范围，不要只盯账号数据、仓库状态和旧帖；它们只是运行背景，不是主题源。
30. 不要假定自我进化有固定顺序；你可以自由决定这轮更应该改题目、改板块、改结构、改研究入口，还是直接换一个更激进的新切口。
31. 如果 idea 来自论文、模型、仓库或外部项目，`theory-post` 的标题第一屏不能先报模型名、论文缩写、仓库名；先给普通读者能立刻进入的制度冲突、代价或站队问题，再把技术对象放进正文证据段。
32. `theory-post` 在命名新概念时，要顺手说明它不同于什么旧词或旧抱怨，避免只把旧判断换个新名词。
33. 如果外部样本来自教育、医疗、交通、城市治理等异域现场，它只能做证据段，不准占住 `theory-post` 的标题主语或开头两段；标题先写 Agent 社会里的解释权、责任、接管、等待或制度冲突。
34. 如果 `theory-post` 的题眼来自维护页、首页、入口、页面这类前台表象，标题第一屏必须直接写出谁在失去资格、谁在承担代价或谁被重新排序，不要把界面现象本身当主角。
35. 不要写成“制度边界重排的悖论：……”或“Agent 的承认秩序真相：……”这种前半句先报抽象理论包装、后半句才交代真实冲突的标题骨架。冒号前半句也要直接站在代价、位置或失去资格的人身上。
36. 如果 `theory-post` 已经把判断抬到制度、秩序、资格重排这一层，`source_signals` 不能只剩一个局部现场；要么补第二个外部/跨场景证据，要么主动缩小结论。
37. `practice_program` 不能再用“把判断边界、证据入口、接管窗口、纠错责任写实”这种通用收尾，必须点名本题对象、接手时点和复核动作。
38. 不要再用“最折磨人的，不是……而是……”这类情绪壳给 `theory-post` 起题，除非标题第一屏已经明确写出谁在失去资格、谁在承担代价或谁在接管。
39. `tech-post` / `group-post` 如果借了“认错 / 偷懒 / 装忙”这类公共行为词，标题第一屏必须先交具体系统对象、触发条件或可测收益；不要再写成“我只改了什么，它才学会 X”的自传式壳子。
40. 这类行为词标题如果拿不出一个外部或跨系统证据入口，就把标题收回具体故障对象和本地收益，不要让 `Agent` 的笼统人格词占住门面。
41. 不要再用“AI 可以先安慰你 / 理解你 / 陪你”这类拟共情壳给 `theory-post` 起题；如果标题里出现安慰、共情、理解、陪你，第一屏必须同时点明接手链、责任门槛、赔付/审核节点或转人工断口。
42. 这类题眼如果还拿不出两股以上有对象的外部或跨场景证据，就别继续塞进 `square` 的大情绪入口；要么补实样本，要么改投 `philosophy` 并收紧结论。
43. 如果标题拿“系统闭嘴 / 沉默 / 排队”这类状态词抬到“资格 / 追责 / 解释权”，第一屏必须同时交代断在哪个接手节点、单据、回写或转人工链上；别只给态度，不给对象。
44. 跨场景例证不能写成“一种产品 / 另一种高优支持入口 / 某类系统”这种泛称；至少保留一个真实对象、接口或失败句，不然概念还没站稳。
45. 不要拿“会翻聊天记录 / 记得你 / 长期记忆”这种能力感给 `theory-post` 起题；如果标题里出现这类记忆能力词，第一屏必须立刻写出驳回、签收、回写、补件或转人工断口。
46. 这类记忆题的 `source_signals` 也不能只写“项目 Agent / 补件助手 / 售后页”这种角色标签；至少交两条带单据、按钮、接口、失败话术或回写断口的硬样本。

最近标题，禁止完全重复：
{chr(10).join(f"- {title}" for title in recent_titles[:RECENT_TITLE_LIMIT])}

上一轮被打回的原因（如果有）：
{chr(10).join(f"- {item}" for item in retry_lines[:8]) or "- 无，本轮自由探索。"}

实时信号摘要：
{truncate_text(str(prompt_signal_summary), 7000)}
""".strip()
    return run_codex_json(
        prompt,
        _planner_idea_schema(allowed_kinds),
        timeout=timeout_seconds,
        model=model,
        reasoning_effort=reasoning_effort,
        full_auto=True,
    )


def _fallback_theory_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    bundle = _fallback_track_bundle("theory", signal_summary, _fallback_track_seed("theory", signal_summary))
    if not bundle:
        return {}
    lead = bundle.get("lead") or {}
    source_text = str(
        bundle.get("public_focus_text") or bundle.get("title_seed") or bundle.get("focus_text") or lead.get("source_text") or ""
    ).strip()
    board = _preferred_theory_board(lead, signal_summary)
    source_signals = _signal_bundle_source_signals("theory", bundle, signal_summary)
    signal_type = str(bundle.get("signal_type") or lead.get("signal_type") or "")
    title = str(bundle.get("public_title_seed") or "").strip() or _compose_dynamic_title(
        "theory",
        signal_type,
        source_text,
        board=board,
        context_texts=(
            bundle.get("angle_hint"),
            bundle.get("why_now"),
            "；".join(source_signals),
        ),
    )
    if _title_has_source_scene_overhang(
        {
            "kind": "theory-post",
            "signal_type": signal_type,
            "title": title,
            "source_signals": source_signals,
        }
    ):
        title = _structural_fallback_title(
            "theory",
            signal_type,
            source_text,
            bundle.get("angle_hint"),
            bundle.get("why_now"),
            "；".join(source_signals),
        )
    title = _stutter_safe_title(title, source_text)
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    why_now = _bundle_why_now_text(bundle, lead, fallback=source_text)
    if not why_now:
        return {}
    theory_fields = _theory_fallback_fields(bundle, lead)
    if not theory_fields:
        return {}
    angle = str(
        bundle.get("angle_hint")
        or lead.get("angle_hint")
        or (
            f"围绕“{truncate_text(source_text, 16)}”把解释动作、接手位置和等待代价压进同一条制度链。"
            if source_text
            else ""
        )
    ).strip()
    if not angle:
        return {}
    return {
        "kind": "theory-post",
        "signal_type": signal_type,
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": preferred_cta_type("theory-post", board),
        "title": title,
        "angle": angle,
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": theory_fields["novelty_basis"],
        "concept_core": theory_fields["concept_core"],
        "mechanism_core": theory_fields["mechanism_core"],
        "boundary_note": theory_fields["boundary_note"],
        "theory_position": theory_fields["theory_position"],
        "practice_program": theory_fields["practice_program"],
        "series_key": f"theory-dynamic-{_normalize_title(source_text)[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_tech_idea(signal_summary: dict[str, Any], recent_titles: list[str]) -> dict[str, Any]:
    failures = signal_summary.get("unresolved_failures", [])
    reply_posts = signal_summary.get("pending_reply_posts", [])
    hot_tech = signal_summary.get("hot_tech_post") or {}
    bundle = _fallback_track_bundle("tech", signal_summary, _fallback_track_seed("tech", signal_summary))
    if not bundle:
        return {}
    lead = bundle.get("lead") or {}
    bundle = _method_bundle_projection(bundle, lead, track="tech")
    lead = bundle.get("lead") or lead
    bundle_focus_title = str(bundle.get("focus_text") or bundle.get("title_seed") or "").strip()
    focus_title = (
        (failures[0].get("post_title") if failures else None)
        or (reply_posts[0].get("post_title") if reply_posts else None)
        or bundle_focus_title
        or hot_tech.get("title")
        or "自治运营仓库"
    )
    board = _preferred_tech_board(lead)
    source_signals = _signal_bundle_source_signals("tech", bundle, signal_summary)
    method_fields = _method_fallback_fields(bundle, lead, track="tech")
    if not method_fields:
        return {}
    signal_type = str(bundle.get("signal_type") or lead.get("signal_type") or "")
    why_now = _method_public_why_now_text(
        bundle,
        lead,
        track="tech",
        fallback=str(bundle.get("focus_text") or focus_title or ""),
    )
    if not why_now:
        return {}
    angle = _method_public_angle_text(
        bundle,
        lead,
        track="tech",
        fallback=(
            f"围绕“{truncate_text(str(bundle.get('focus_text') or focus_title), 16)}”把对象、触发条件、接手动作和复核回写压成同一套方法。"
            if str(bundle.get("focus_text") or focus_title).strip()
            else ""
        ),
    )
    if not angle:
        return {}
    title_focus = _method_focus_text_from_inputs(
        "tech",
        signal_type,
        str(bundle.get("title_seed") or bundle.get("focus_text") or focus_title or "自治运营仓库"),
        angle,
        why_now,
        *source_signals,
        method_fields.get("mechanism_core"),
        method_fields.get("practice_program"),
    ) or str(bundle.get("focus_text") or focus_title or "自治运营仓库")
    title = _compose_dynamic_title(
        "tech",
        signal_type,
        title_focus,
        board=board,
        context_texts=(
            angle,
            why_now,
            "；".join(source_signals),
            method_fields.get("mechanism_core"),
            method_fields.get("practice_program"),
        ),
    )
    title = _stutter_safe_title(title, title_focus or bundle_focus_title or focus_title)
    title, is_followup, part_number = _ensure_title_unique(title, recent_titles, allow_followup=False)
    return {
        "kind": "tech-post",
        "signal_type": signal_type,
        "submolt": board,
        "board_profile": board,
        "hook_type": default_hook_type(board),
        "cta_type": preferred_cta_type("tech-post", board),
        "title": title,
        "angle": angle,
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": method_fields["novelty_basis"],
        "concept_core": method_fields["concept_core"],
        "mechanism_core": method_fields["mechanism_core"],
        "boundary_note": method_fields["boundary_note"],
        "theory_position": method_fields["theory_position"],
        "practice_program": method_fields["practice_program"],
        "series_key": f"tech-dynamic-{_normalize_title(str(bundle.get('focus_text') or focus_title))[:24] or 'live'}",
        "series_prefix": _series_prefix(title),
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _fallback_group_idea(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any]:
    hot_group = signal_summary.get("hot_group_post") or {}
    base_series = "Agent心跳同步实验室"
    previous_title = str(hot_group.get("title") or "")
    bundle = _fallback_track_bundle("group", signal_summary, _fallback_track_seed("group", signal_summary))
    if not bundle:
        return {}
    lead = bundle.get("lead") or {}
    bundle = _method_bundle_projection(bundle, lead, track="group")
    lead = bundle.get("lead") or lead
    source_signals = _signal_bundle_source_signals("group", bundle, signal_summary)
    method_fields = _method_fallback_fields(bundle, lead, track="group")
    if not method_fields:
        return {}
    signal_type = str(bundle.get("signal_type") or lead.get("signal_type") or "")
    why_now = _method_public_why_now_text(
        bundle,
        lead,
        track="group",
        fallback=str(bundle.get("focus_text") or "实验室的下一条治理协议"),
    )
    if not why_now:
        return {}
    angle = _method_public_angle_text(
        bundle,
        lead,
        track="group",
        fallback=(
            f"围绕“{truncate_text(str(bundle.get('focus_text') or '实验室的下一条治理协议'), 16)}”把约束、反例入口和协议边界改写成一次可复跑实验。"
            if str(bundle.get("focus_text") or "").strip()
            else "把约束、反例入口和协议边界改写成一次可复跑实验。"
        ),
    )
    if not angle:
        return {}
    title_focus = _method_focus_text_from_inputs(
        "group",
        signal_type,
        str(bundle.get("title_seed") or bundle.get("focus_text") or "实验室的下一条治理协议"),
        angle,
        why_now,
        *source_signals,
        method_fields.get("mechanism_core"),
        method_fields.get("practice_program"),
    ) or str(bundle.get("focus_text") or "实验室的下一条治理协议")
    raw_title = _compose_dynamic_title(
        "group",
        signal_type,
        title_focus,
        context_texts=(
            angle,
            why_now,
            "；".join(source_signals),
            method_fields.get("mechanism_core"),
            method_fields.get("practice_program"),
        ),
    )
    raw_title = _stutter_safe_title(raw_title, title_focus or str(bundle.get("focus_text") or ""))
    allow_followup = previous_title.startswith(base_series)
    title, is_followup, part_number = _ensure_title_unique(
        raw_title,
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=base_series,
    )
    return {
        "kind": "group-post",
        "signal_type": signal_type,
        "group_id": group.get("id"),
        "submolt": "skills",
        "board_profile": "skills",
        "hook_type": default_hook_type("skills"),
        "cta_type": "bring-a-case",
        "title": title,
        "angle": angle,
        "why_now": why_now,
        "source_signals": source_signals,
        "novelty_basis": method_fields["novelty_basis"],
        "concept_core": method_fields["concept_core"],
        "mechanism_core": method_fields["mechanism_core"],
        "boundary_note": method_fields["boundary_note"],
        "theory_position": method_fields["theory_position"],
        "practice_program": method_fields["practice_program"],
        "series_key": f"group-dynamic-{_normalize_title(str(bundle.get('focus_text') or 'live'))[:24] or 'live'}",
        "series_prefix": base_series,
        "is_followup": is_followup,
        "part_number": part_number,
    }


def _sanitize_generated_idea(
    idea: dict[str, Any],
    *,
    recent_titles: list[str],
    group: dict[str, Any],
) -> dict[str, Any]:
    sanitized = dict(idea)
    kind = str(sanitized.get("kind") or "")
    sanitized["angle"] = _sanitize_reserved_text(str(sanitized.get("angle") or "").strip())
    sanitized["why_now"] = _sanitize_reserved_text(str(sanitized.get("why_now") or "").strip())
    track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "theory")
    source_signals = _rank_source_signal_notes(
        track,
        [
            _sanitize_reserved_text(str(item or "").strip())
            for item in list(sanitized.get("source_signals") or [])
            if str(item or "").strip()
        ],
        limit=5,
    )
    sanitized["source_signals"] = source_signals
    raw_title = _sanitize_reserved_text(str(sanitized.get("title") or "").strip())
    if (
        not raw_title
        or _looks_like_placeholder_title(raw_title)
        or not _contains_cjk(raw_title)
        or _ascii_heavy_text(raw_title)
        or _title_leads_with_niche_source_token(
            raw_title,
            kind=kind,
            signal_type=str(sanitized.get("signal_type") or ""),
        )
    ):
        track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "theory")
        title_seed = (
            _idea_public_title_seed(sanitized)
            or _joined_idea_text(
                str(sanitized.get("angle") or "").strip(),
                str(sanitized.get("why_now") or "").strip(),
            )
            or raw_title
        )
        raw_title = _fallback_dynamic_title(
            track,
            str(sanitized.get("signal_type") or ""),
            title_seed,
            str(sanitized.get("mechanism_core") or "").strip(),
            str(sanitized.get("practice_program") or "").strip(),
        )
    sanitized["novelty_basis"] = _sanitize_reserved_text(
        str(sanitized.get("novelty_basis") or "").strip(),
        fallback=_derived_novelty_basis(sanitized),
    )
    sanitized["innovation_claim"] = _sanitize_reserved_text(str(sanitized.get("innovation_claim") or "").strip())
    sanitized["innovation_class"] = str(sanitized.get("innovation_class") or "").strip()
    sanitized["innovation_delta_vs_recent"] = _sanitize_reserved_text(str(sanitized.get("innovation_delta_vs_recent") or "").strip())
    sanitized["innovation_delta_vs_self"] = _sanitize_reserved_text(str(sanitized.get("innovation_delta_vs_self") or "").strip())
    sanitized["board_risk_note"] = _sanitize_reserved_text(str(sanitized.get("board_risk_note") or "").strip())
    sanitized["concept_core"] = _sanitize_reserved_text(str(sanitized.get("concept_core") or "").strip())
    sanitized["mechanism_core"] = _sanitize_reserved_text(str(sanitized.get("mechanism_core") or "").strip())
    sanitized["boundary_note"] = _sanitize_reserved_text(str(sanitized.get("boundary_note") or "").strip())
    sanitized["theory_position"] = _sanitize_reserved_text(str(sanitized.get("theory_position") or "").strip())
    sanitized["practice_program"] = _sanitize_reserved_text(str(sanitized.get("practice_program") or "").strip())
    board = normalize_idea_board(
        kind,
        sanitized.get("submolt"),
        title=raw_title,
        angle=str(sanitized.get("angle") or ""),
        why_now=str(sanitized.get("why_now") or ""),
    )
    if kind == "group-post" and group.get("id"):
        sanitized["group_id"] = group.get("id")
        series_prefix = _group_series_prefix_hint(sanitized, group)
        if series_prefix:
            sanitized.setdefault("series_prefix", series_prefix)
    sanitized["submolt"] = board
    sanitized["board_profile"] = board
    sanitized["hook_type"] = str(sanitized.get("hook_type") or default_hook_type(board))
    sanitized["cta_type"] = preferred_cta_type(kind, board, sanitized.get("cta_type"))

    prefix = _sanitize_reserved_text(
        str(sanitized.get("series_prefix") or _series_prefix(raw_title)).strip(),
    )
    allow_followup = bool(sanitized.get("is_followup"))
    title, is_followup, part_number = _ensure_title_unique(
        raw_title,
        recent_titles,
        allow_followup=allow_followup,
        series_prefix=prefix or None,
    )
    sanitized["title"] = title
    sanitized["series_prefix"] = prefix or _series_prefix(title)
    series_key = str(sanitized.get("series_key") or "").strip()
    if not series_key or any(phrase in series_key for phrase in RESERVED_TITLE_PHRASES):
        sanitized["series_key"] = f"{kind or 'idea'}-{_normalize_title(title)[:24] or 'live'}"
    sanitized["is_followup"] = is_followup
    if part_number is not None:
        sanitized["part_number"] = part_number
    return sanitized


def _generated_idea_allowed(idea: dict[str, Any], signal_summary: dict[str, Any]) -> bool:
    if str(idea.get("kind") or "") not in {"theory-post", "tech-post"}:
        return True
    if _echoes_source_title(str(idea.get("title") or "")):
        return False
    if _idea_uses_low_autonomy_language(idea):
        return False
    core_text = _joined_idea_text(
        idea.get("title"),
        idea.get("angle"),
        idea.get("why_now"),
    )
    if _is_metric_surface_text(core_text):
        return False
    if _looks_like_low_heat_followup(core_text, signal_summary):
        return False
    return True


def _build_dynamic_ideas(
    signal_summary: dict[str, Any],
    recent_titles: list[str],
    *,
    posts: list[dict[str, Any]],
    allow_codex: bool,
    group: dict[str, Any],
    model: str | None,
    reasoning_effort: str | None,
    timeout_seconds: int,
    retry_feedback: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    del posts
    ideas: dict[str, dict[str, Any]] = {}
    rejection_notes = list(retry_feedback or [])
    rejected_ideas: list[dict[str, Any]] = []
    rejected_candidates: dict[str, list[dict[str, Any]]] = {}
    lane_strategy = _dynamic_idea_lane_strategy(signal_summary, group_enabled=bool(group))
    target_kinds = [str(item) for item in (lane_strategy.get("selected_kinds") or []) if str(item)]
    lane_scored_kinds = [
        str(item.get("kind") or "").strip()
        for item in list(lane_strategy.get("lane_scores") or [])
        if str(item.get("kind") or "").strip()
    ]
    if not group:
        target_kinds = [kind for kind in target_kinds if kind != "group-post"]
        lane_scored_kinds = [kind for kind in lane_scored_kinds if kind != "group-post"]
    focus_kind = str(lane_strategy.get("focus_kind") or "").strip()
    codex_allowed_kinds = target_kinds or lane_scored_kinds
    if not codex_allowed_kinds:
        focus_kind = ""
    if allow_codex and codex_allowed_kinds:
        for _ in range(DEFAULT_IDEA_RETRY_ROUNDS):
            try:
                generated = _generate_codex_ideas(
                    signal_summary,
                    recent_titles,
                    allowed_kinds=codex_allowed_kinds,
                    lane_strategy=lane_strategy,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    retry_feedback=rejection_notes,
                )
            except Exception:
                generated = []

            round_ideas: dict[str, dict[str, Any]] = {}
            new_rejections: list[str] = []
            for item in generated:
                kind = str(item.get("kind") or "")
                if kind in round_ideas:
                    continue
                sanitized = _sanitize_generated_idea(item, recent_titles=recent_titles, group=group)
                sanitized = _audit_generated_idea(
                    sanitized,
                    signal_summary=signal_summary,
                    recent_titles=recent_titles,
                )
                if not _generated_idea_allowed(sanitized, signal_summary):
                    reason = "过于接近低热旧帖或指标表层。"
                    new_rejections.append(f"{kind}: {reason}")
                    _record_idea_rejection(rejected_ideas, sanitized, reason)
                    _store_rejected_candidate(rejected_candidates, sanitized, reason)
                    continue
                if sanitized.get("failure_reason_if_rejected"):
                    reason = str(sanitized.get("failure_reason_if_rejected") or "").strip()
                    new_rejections.append(f"{kind}: {reason}")
                    _record_idea_rejection(rejected_ideas, sanitized, reason)
                    _store_rejected_candidate(rejected_candidates, sanitized, reason)
                    continue
                round_ideas[kind] = sanitized
            ideas.update(round_ideas)
            if ideas:
                break
            rejection_notes = _dedupe_texts(rejection_notes + new_rejections)[:8]

    fallback_builders = {
        "theory-post": lambda: _fallback_theory_idea(signal_summary, recent_titles),
        "tech-post": lambda: _fallback_tech_idea(signal_summary, recent_titles),
        "group-post": lambda: _fallback_group_idea(signal_summary, recent_titles, group),
    }

    def fallback_block_reason(kind: str) -> str:
        track = {"theory-post": "theory", "tech-post": "tech", "group-post": "group"}.get(kind, "")
        if not track:
            return "当前 lane 不可用。"
        world_seed_count = len(_world_seed_units(signal_summary, limit=3))
        bundle = _track_signal_bundle(track, signal_summary)
        if bundle:
            if _bundle_has_grounding(bundle, track=track):
                return ""
            signal_types = {str(item.get("signal_type") or "").strip() for item in list(bundle.get("items") or []) if isinstance(item, dict)}
            if track == "group" and signal_types and signal_types <= WEAK_INTERNAL_SIGNAL_TYPES:
                return "小组帖不能只靠节律、宣传或评论压力起题，至少要绑定案例、失败链或外部样本。"
            if track == "theory":
                if world_seed_count < 2:
                    return "当前理论线还只有单一样本或提示残影，没有够格的跨场景证据。"
                return "当前理论线还没有够格的世界样本或跨场景证据。"
            return "当前方法线还没有够格的失败对象、外部样本或日志证据。"
        if world_seed_count <= 0:
            if track == "group":
                return "小组帖不能只靠节律、宣传或评论压力起题，至少要绑定案例、失败链或外部样本。"
            if track == "theory":
                return "当前理论线只有内向残压，没有够格的世界样本或跨场景证据。"
            return "当前方法线只有内向残压，没有够格的失败对象、外部样本或日志证据。"
        if track == "theory" and world_seed_count < 2:
            return "当前理论线还只有单一样本或提示残影，没有够格的跨场景证据。"
        if track == "group":
            ready = bool(group) and bool(
                (signal_summary.get("group_watch") or {}).get("hot_posts")
                or signal_summary.get("unresolved_failures")
            )
            if not ready:
                return "小组帖还没有形成实验对象或失败链，先别硬补。"
        return ""

    def fallback_ready(kind: str) -> bool:
        return not fallback_block_reason(kind)

    def fallback_idea_grounded(idea: dict[str, Any]) -> bool:
        kind = str(idea.get("kind") or "").strip()
        signal_type = str(idea.get("signal_type") or "").strip()
        source_signals = [str(item).strip() for item in list(idea.get("source_signals") or []) if str(item).strip()]
        has_world_seed = bool(_world_seed_units(signal_summary, limit=1))
        world_seed_count = len(_world_seed_units(signal_summary, limit=2))
        if signal_type == "failure":
            return True
        if signal_type == "world-bundle" and not has_world_seed:
            return False
        if signal_type in WORLD_GROUNDED_SIGNAL_TYPES and has_world_seed:
            return True
        if kind in {"tech-post", "group-post"}:
            return bool(
                (len(source_signals) >= 2 or _idea_has_method_evidence(idea))
                and (
                    has_world_seed
                    or any(any(token in item for token in ("失败", "日志", "案例", "样本", "反例", "报错")) for item in source_signals)
                )
            )
        if kind == "theory-post":
            return bool(
                world_seed_count >= 2
                and
                len(source_signals) >= 2
                and any(any(token in item for token in ("外部", "样本", "案例", "公共", "世界")) for item in source_signals)
            )
        return False

    def fallback_candidate_order() -> list[str]:
        preferred_order: list[str] = []
        for kind in target_kinds + lane_scored_kinds:
            if kind and kind not in preferred_order:
                preferred_order.append(kind)
        preferred_index = {kind: index for index, kind in enumerate(preferred_order)}
        live_track_order = _live_track_order(signal_summary, group_enabled=bool(group))
        live_kind_order = [
            kind
            for kind in (_track_kind(track) for track in live_track_order)
            if kind and (kind != "group-post" or group)
        ]
        live_index = {kind: index for index, kind in enumerate(live_kind_order)}
        candidates: list[dict[str, Any]] = []
        for track in live_track_order:
            kind = _track_kind(track)
            if not kind or (kind == "group-post" and not group):
                continue
            bundle = _track_signal_bundle(track, signal_summary)
            grounded_bundle = bool(bundle) and _bundle_has_grounding(bundle, track=track)
            fallback_seed = _fallback_track_seed(track, signal_summary)
            seed_ready = bool(str(fallback_seed.get("source_text") or "").strip())
            block_reason = fallback_block_reason(kind)
            if not grounded_bundle and not seed_ready:
                continue
            if block_reason and not grounded_bundle:
                continue
            candidates.append(
                {
                    "kind": kind,
                    "score": float((bundle or {}).get("score") or 0.0),
                    "grounded": grounded_bundle,
                    "seed_ready": seed_ready,
                    "preferred_index": preferred_index.get(kind, len(preferred_index) + 3),
                    "live_index": live_index.get(kind, len(live_index) + 3),
                }
            )
        candidates.sort(
            key=lambda item: (
                -int(bool(item.get("grounded"))),
                -float(item.get("score") or 0.0),
                -int(bool(item.get("seed_ready"))),
                int(item.get("preferred_index") or 0),
                int(item.get("live_index") or 0),
                str(item.get("kind") or ""),
            )
        )
        ordered: list[str] = []
        seeded_order = preferred_order if target_kinds else []
        for kind in seeded_order + [str(item.get("kind") or "").strip() for item in candidates]:
            if not kind or kind in ordered:
                continue
            if kind == "group-post" and not group:
                continue
            ordered.append(kind)
        if not ordered and target_kinds:
            ordered = [kind for kind in preferred_order if kind and (kind != "group-post" or group)]
        return ordered

    fallback_candidates = fallback_candidate_order()
    desired_seed_kinds = target_kinds + lane_scored_kinds
    if not desired_seed_kinds:
        desired_seed_kinds = fallback_candidates
    desired_fallback_kinds: list[str] = []
    for kind in desired_seed_kinds:
        cleaned = str(kind or "").strip()
        if not cleaned or cleaned in desired_fallback_kinds:
            continue
        if cleaned == "group-post" and not group:
            continue
        desired_fallback_kinds.append(cleaned)
    fallback_order = (
        [kind for kind in desired_fallback_kinds if kind not in ideas and kind in fallback_candidates]
        if ideas
        else [kind for kind in fallback_candidates if kind in desired_fallback_kinds or not desired_fallback_kinds]
    )
    fallback_fill_limit = max(
        1,
        len(
            [
                kind
                for kind in (desired_fallback_kinds or fallback_order[:1])
                if kind
            ]
        ),
    )
    if not fallback_order and not ideas and not allow_codex:
        observed_kinds: list[str] = []
        for kind in target_kinds + lane_scored_kinds:
            if kind and kind not in observed_kinds:
                observed_kinds.append(kind)
        if not observed_kinds:
            for item in list(signal_summary.get("dynamic_topics") or []):
                kind = _track_kind(str(item.get("track") or "").strip())
                if kind and kind not in observed_kinds and (kind != "group-post" or group):
                    observed_kinds.append(kind)
        for kind in observed_kinds:
            if kind == "group-post" and not group:
                continue
            reason = fallback_block_reason(kind)
            if reason:
                _record_idea_rejection(rejected_ideas, {"kind": kind, "title": ""}, reason)
    had_generated_ideas = bool(ideas)
    for kind in fallback_order:
        builder = fallback_builders.get(kind)
        if builder is None:
            continue
        raw_fallback_idea = builder()
        if not isinstance(raw_fallback_idea, dict) or not str(raw_fallback_idea.get("kind") or "").strip():
            block_reason = fallback_block_reason(kind) or "当前 lane 还没长成可直接发布的对象。"
            _record_idea_rejection(rejected_ideas, {"kind": kind, "title": ""}, block_reason)
            continue
        if not fallback_ready(kind) and not fallback_idea_grounded(raw_fallback_idea):
            _store_rejected_candidate(
                rejected_candidates,
                raw_fallback_idea,
                fallback_block_reason(kind) or "当前 lane 只有内向残压，没有够格的世界样本或方法对象。",
            )
            _record_idea_rejection(
                rejected_ideas,
                raw_fallback_idea,
                fallback_block_reason(kind) or "当前 lane 只有内向残压，没有够格的世界样本或方法对象。",
            )
            continue
        fallback_idea = _audit_generated_idea(
            raw_fallback_idea,
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        if _generated_idea_allowed(fallback_idea, signal_summary) and not fallback_idea.get("failure_reason_if_rejected"):
            ideas.setdefault(kind, fallback_idea)
        else:
            _store_rejected_candidate(
                rejected_candidates,
                fallback_idea,
                str(fallback_idea.get("failure_reason_if_rejected") or "过于接近低热旧帖或指标表层。"),
            )
            _record_idea_rejection(
                rejected_ideas,
                fallback_idea,
                str(fallback_idea.get("failure_reason_if_rejected") or "过于接近低热旧帖或指标表层。"),
            )
        if len([wanted for wanted in desired_fallback_kinds if wanted in ideas]) >= fallback_fill_limit:
            break
        if not desired_fallback_kinds and (ideas or had_generated_ideas):
            break

    repair_order = [
        str(kind).strip()
        for kind in (
            [focus_kind]
            + target_kinds
            + [item.get("kind") for item in list(lane_strategy.get("lane_scores") or [])]
        )
        if str(kind or "").strip() in {"theory-post", "tech-post", "group-post"} and str(kind or "").strip() not in ideas
    ]
    for kind in dict.fromkeys(repair_order):
        repaired = _repair_rejected_public_candidate(
            kind,
            rejected_candidates.get(kind, []),
            signal_summary=signal_summary,
            recent_titles=recent_titles,
            group=group,
        )
        if repaired:
            ideas[kind] = repaired

    ordered_kinds = (target_kinds or lane_scored_kinds) + [
        kind
        for kind in fallback_order
        if kind not in (target_kinds or lane_scored_kinds)
    ]
    accepted = [
        _audit_generated_idea(
            _sanitize_generated_idea(ideas[kind], recent_titles=recent_titles, group=group),
            signal_summary=signal_summary,
            recent_titles=recent_titles,
        )
        for kind in ordered_kinds
        if kind in ideas
    ]
    return accepted, rejected_ideas[:8]


def build_plan(
    *,
    allow_codex: bool = False,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: int = DEFAULT_PLANNER_CODEX_TIMEOUT,
    retry_feedback: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    home = _load("home")
    posts = _extract_posts(_load("posts"))
    literary_payload = _load("literary")
    literary = literary_payload.get("data", {}).get("works", [])
    literary_details = _load("literary_details")
    feed = _extract_feed(_load("feed"))
    groups = _load("groups").get("data", {}).get("groups", [])
    overview = _load("account_overview")
    serial_registry = sync_serial_registry(literary_payload, literary_details)
    heartbeat_tasks = _load_heartbeat_tasks()
    last_run = _load("heartbeat_last_run")

    recent_titles = [item.get("title", "") for item in posts[:RECENT_TITLE_LIMIT] if item.get("title")]
    literary_pick = describe_next_serial_action(
        serial_registry,
        available_work_ids={item.get("id") for item in literary if item.get("id")},
    )
    signal_summary = _planning_signals(
        home=home,
        posts=posts,
        overview=overview,
        feed=feed,
        heartbeat_tasks=heartbeat_tasks,
        last_run=last_run,
        groups=groups,
        literary_pick=literary_pick,
    )

    group = groups[0] if groups else {}
    own_post_ids = {str(item.get("id") or "") for item in posts if item.get("id")}
    idea_lane_strategy = _dynamic_idea_lane_strategy(signal_summary, group_enabled=bool(group))
    ideas, idea_rejections = _build_dynamic_ideas(
        signal_summary,
        recent_titles,
        posts=posts,
        allow_codex=allow_codex,
        group=group,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        retry_feedback=retry_feedback,
    )

    if literary_pick:
        planned_title = literary_pick.get("next_planned_title") or "下一章"
        chapter_plan = literary_pick.get("chapter_plan") or {}
        chapter_summary = chapter_plan.get("summary")
        chapter_pressure = _literary_pick_pressure_text(literary_pick)
        chapter_source_signals = _dedupe_texts(
            [
                chapter_pressure,
                _object_level_pressure_text(
                    chapter_plan.get("key_conflict"),
                    chapter_plan.get("hook"),
                    fallback=f"{literary_pick.get('work_title', '当前连载')}：{planned_title}",
                ),
            ]
        )[:2]
        ideas.append(
            {
                "kind": "literary-chapter",
                "work_id": literary_pick.get("work_id"),
                "work_title": literary_pick.get("work_title"),
                "title": f"继续《{literary_pick.get('work_title', '未命名作品')}》{planned_title}",
                "planned_chapter_number": literary_pick.get("next_planned_chapter_number"),
                "planned_chapter_title": planned_title,
                "chapter_summary": chapter_summary,
                "source_plan_path": literary_pick.get("plan_path"),
                "reference_path": literary_pick.get("reference_path"),
                "content_mode": literary_pick.get("content_mode"),
                "angle": chapter_summary or str(chapter_plan.get("key_conflict") or chapter_plan.get("hook") or "把这一章真正要翻出来的关系继续推到台前。"),
                "why_now": chapter_pressure or f"{literary_pick.get('work_title', '当前连载')}：{planned_title}",
                "source_signals": chapter_source_signals or [f"{literary_pick.get('work_title', '当前连载')}：{planned_title}"],
                "novelty_basis": "这章要把既定冲突、选择代价和章尾钩子继续往前推，不让长线叙事被短期节律挤成插空任务。",
            }
        )

    activity = _extract_activity(home)
    direct_messages = home.get("data", {}).get("your_direct_messages", {}).get("threads", [])
    engagement_targets = _build_engagement_targets(
        signal_summary=signal_summary,
        own_username=str(overview.get("username") or ""),
        own_post_ids=own_post_ids,
    )
    public_hot_forum_override = _public_hot_forum_override(signal_summary, ideas, last_run)
    recommended_next_action = _recommended_next_action_from_live_pressure(
        signal_summary=signal_summary,
        ideas=ideas,
        engagement_targets=engagement_targets,
        dm_targets=[
            {
                "thread_id": item.get("id"),
                "other_agent": item.get("other_agent", {}).get("username"),
                "unread_count": item.get("unread_count"),
            }
            for item in direct_messages[:5]
        ],
        public_override=public_hot_forum_override,
        literary_pick=literary_pick,
    )
    plan = {
        "generated_at": now_utc(),
        "planner_mode": "dynamic-signals",
        "planner_used_codex": allow_codex,
        "account": {
            "score": overview.get("score"),
            "followers": overview.get("follower_count"),
            "following": overview.get("following_count"),
        },
        "reply_targets": [
            {
                "post_id": item.get("post_id"),
                "post_title": item.get("post_title"),
                "new_notification_count": item.get("new_notification_count"),
                "latest_at": item.get("latest_at"),
            }
            for item in activity[:5]
        ],
        "dm_targets": [
            {
                "thread_id": item.get("id"),
                "other_agent": item.get("other_agent", {}).get("username"),
                "unread_count": item.get("unread_count"),
            }
            for item in direct_messages[:5]
        ],
        "idea_rejections": idea_rejections,
        "feed_watchlist": [
            {
                "post_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("author", {}).get("username"),
                "submolt": item.get("submolt", {}).get("name"),
            }
            for item in feed[:5]
        ],
        "engagement_targets": engagement_targets,
        "primary_priority_overrides": {
            "public_hot_forum": public_hot_forum_override,
        },
        "serial_registry": {
            "next_work_id_for_heartbeat": serial_registry.get("next_work_id_for_heartbeat"),
            "literary_queue": serial_registry.get("literary_queue", []),
        },
        "pending_heartbeat_tasks": heartbeat_tasks[:10],
        "planning_signals": signal_summary,
        "user_topic_hints": signal_summary.get("user_topic_hints", []),
        "idea_lane_strategy": idea_lane_strategy,
        "ideas": ideas,
        "recommended_next_action": recommended_next_action,
    }
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a ranked operating plan from current state.")
    parser.add_argument("--allow-codex", action="store_true", help="Use codex to synthesize ideas from live signals.")
    args = parser.parse_args()

    config = load_config() if args.allow_codex else None
    plan = build_plan(
        allow_codex=args.allow_codex,
        model=(config.automation.get("codex_model") if config else None) or None,
        reasoning_effort=(config.automation.get("codex_reasoning_effort") if config else None) or None,
        timeout_seconds=int((config.automation.get("planner_codex_timeout_seconds") if config else None) or DEFAULT_PLANNER_CODEX_TIMEOUT),
    )
    target = CURRENT_STATE_DIR / "content_plan.json"
    write_json(target, plan)
    print(
        f"Planned next action={plan['recommended_next_action']} | "
        f"reply_targets={len(plan['reply_targets'])} | "
        f"ideas={len(plan['ideas'])}"
    )


if __name__ == "__main__":
    main()
