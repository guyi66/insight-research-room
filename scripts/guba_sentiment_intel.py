#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Guba Sentiment Intel (EastMoney Guba -> Local MySQL -> LLM/Rule analysis)

你当前已把股吧帖子（含可选全文）落到 MySQL（bettafish_guba.guba_post）。
本脚本用于“当天舆情分析”：
- 仅取【当天】帖子（默认 Asia/Shanghai 的今天）
- 判断：舆情过热/过冷/中性（基于热度+情绪+语气强度）
- 最后输出：重要帖子（作为“舆情详细信息/重要评论”）清单（含链接、时间、作者、阅读/评论数、摘要）

说明（重要）：
- 目前 DB 里存的是“帖子/发言”（post），不包含每条帖子的“楼内回复/评论内容”。
  因此本脚本在“重要评论”部分输出的是【重要帖子正文/发言内容】。
  若你后续确实需要抓“评论区回复”，需要再加一条 comment API/HTML 抓取链路。

依赖：
  pip install pymysql aiohttp requests beautifulsoup4 lxml

运行示例：
  python scripts/guba_sentiment_intel.py --config "C:\...\Guba-Crawler\config.ini" --code 600519 --llm

参数：
  --date YYYY-MM-DD   指定分析日期（默认：今天）
  --limit             当天最多取多少条帖子进入分析（默认 300）
  --top               重要帖子清单输出数量（默认 20）
  --baseline-days     用于热度基线的回看窗口（日，默认 14）
  --llm               调用 MediaEngine DeepSearchAgent 的 llm_client 做高质量归纳
"""

from __future__ import annotations

import argparse
import configparser
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymysql


# ---------- timezone helpers ----------
def _today_in_tz(tz_name: str) -> date:
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        # fallback: local date
        return datetime.now().date()


# ---------- config ----------
def _read_text_with_bom_fallback(file_path: str) -> str:
    with open(file_path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8-sig", errors="strict")
    except Exception:
        return data.decode("utf-8", errors="replace")


def load_config(config_path: str) -> configparser.ConfigParser:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    cfg = configparser.ConfigParser(strict=False)
    cfg.read_string(_read_text_with_bom_fallback(config_path))
    return cfg


# ---------- models ----------
@dataclass
class GubaPost:
    href: str
    url: str
    title: str
    author: str
    read_count: Optional[int]
    comment_count: Optional[int]
    last_update_raw: str
    last_update_dt: Optional[str]  # iso
    crawled_at: str                # iso
    full_text: str

    def text_for_sentiment(self) -> str:
        return " ".join([self.title or "", self.full_text or ""]).strip()


# ---------- filters (optional) ----------
_MARKET_QUOTE_PATTERNS = [
    r"收盘", r"开盘", r"最高价", r"最低价", r"涨跌幅", r"涨停", r"跌停",
    r"成交额", r"成交量", r"换手率", r"振幅", r"量比", r"市值",
    r"K线", r"均线", r"MACD", r"RSI", r"BOLL", r"龙虎榜", r"主力资金",
    r"资金流", r"北向资金", r"南向资金", r"融资融券", r"两融",
]
_MARKET_QUOTE_RE = re.compile("|".join(_MARKET_QUOTE_PATTERNS), flags=re.IGNORECASE)

def _is_market_quote_like(title: str, text: str) -> bool:
    return bool(_MARKET_QUOTE_RE.search(f"{title} {text}"))


# ---------- db ----------
def _mysql_conn_from_cfg(cfg: configparser.ConfigParser):
    if not cfg.has_section("mysql"):
        raise RuntimeError("config.ini 缺少 [mysql] 配置段。")
    host = cfg.get("mysql", "host", fallback="127.0.0.1")
    port = cfg.getint("mysql", "port", fallback=3306)
    user = cfg.get("mysql", "user", fallback="root")
    password = cfg.get("mysql", "password", fallback="")
    database = cfg.get("mysql", "database", fallback="bettafish_guba")
    charset = cfg.get("mysql", "charset", fallback="utf8mb4")
    return pymysql.connect(
        host=host, port=port, user=user, password=password, database=database,
        charset=charset, autocommit=True, cursorclass=pymysql.cursors.DictCursor
    )


def fetch_posts_for_date(cfg: configparser.ConfigParser, stock_code: str, day: date, limit: int = 300) -> List[GubaPost]:
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end = start + timedelta(days=1)

    sql = """
    SELECT href, COALESCE(url, href) AS url,
           COALESCE(title,'') AS title,
           COALESCE(author,'') AS author,
           read_count, comment_count,
           COALESCE(last_update_raw,'') AS last_update_raw,
           last_update_dt,
           crawled_at,
           COALESCE(full_text,'') AS full_text
      FROM guba_post
     WHERE stock_code = %s
       AND (
         (last_update_dt IS NOT NULL AND last_update_dt >= %s AND last_update_dt < %s)
         OR
         (last_update_dt IS NULL AND crawled_at >= %s AND crawled_at < %s)
       )
     ORDER BY COALESCE(comment_count,0) DESC, COALESCE(read_count,0) DESC, COALESCE(last_update_dt, crawled_at) DESC
     LIMIT %s
    """

    conn = _mysql_conn_from_cfg(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (stock_code, start, end, start, end, int(limit)))
            rows = cur.fetchall()
    finally:
        conn.close()

    out: List[GubaPost] = []
    for r in rows or []:
        out.append(GubaPost(
            href=str(r.get("href") or ""),
            url=str(r.get("url") or ""),
            title=str(r.get("title") or ""),
            author=str(r.get("author") or ""),
            read_count=r.get("read_count"),
            comment_count=r.get("comment_count"),
            last_update_raw=str(r.get("last_update_raw") or ""),
            last_update_dt=(r.get("last_update_dt").isoformat(sep=" ", timespec="seconds") if r.get("last_update_dt") else None),
            crawled_at=(r.get("crawled_at").isoformat(sep=" ", timespec="seconds") if r.get("crawled_at") else ""),
            full_text=str(r.get("full_text") or ""),
        ))
    return out


def fetch_daily_counts(cfg: configparser.ConfigParser, stock_code: str, days: int = 14) -> List[Tuple[str, int, int]]:
    """
    返回近 N 天按日聚合的 (day, post_cnt, total_comments_proxy)
    注：total_comments_proxy 用 comment_count 求和作为“讨论强度”代理
    """
    days = max(1, int(days))
    start = datetime.now() - timedelta(days=days - 1)
    # 按 last_update_dt 优先；缺失用 crawled_at
    sql = """
    SELECT DATE(COALESCE(last_update_dt, crawled_at)) AS d,
           COUNT(*) AS cnt,
           SUM(COALESCE(comment_count,0)) AS csum
      FROM guba_post
     WHERE stock_code=%s
       AND COALESCE(last_update_dt, crawled_at) >= %s
     GROUP BY DATE(COALESCE(last_update_dt, crawled_at))
     ORDER BY d ASC
    """
    conn = _mysql_conn_from_cfg(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (stock_code, start))
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for r in rows or []:
        d = r.get("d")
        out.append((str(d), int(r.get("cnt") or 0), int(r.get("csum") or 0)))
    return out


# ---------- rule-based sentiment ----------
_POS_WORDS = [
    "利好", "超预期", "看好", "买入", "加仓", "增持", "回购", "分红", "提价", "起飞",
    "主升", "突破", "反转", "牛", "强势", "向上", "稳", "护盘", "核心资产", "逻辑",
]
_NEG_WORDS = [
    "利空", "暴雷", "踩雷", "砸盘", "出货", "崩", "下跌", "割肉", "恐慌", "退市",
    "调查", "立案", "处罚", "问询", "风险", "造假", "库存", "价格倒挂", "滞销",
]
_EUPHORIA_WORDS = ["梭哈", "翻倍", "一字板", "躺赢", "闭眼买", "无脑买", "起飞", "暴涨", "抄底成功"]
_PANIC_WORDS = ["崩盘", "完了", "跑路", "爆仓", "割肉", "破位", "踩踏", "恐慌", "顶不住"]

def score_sentiment(text: str) -> Dict[str, int]:
    t = (text or "").lower()
    pos = sum(t.count(w) for w in _POS_WORDS)
    neg = sum(t.count(w) for w in _NEG_WORDS)
    eup = sum(t.count(w) for w in _EUPHORIA_WORDS)
    pan = sum(t.count(w) for w in _PANIC_WORDS)
    return {"pos": pos, "neg": neg, "euphoria": eup, "panic": pan}


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def classify_hot_cold(
    today_cnt: int,
    today_csum: int,
    baseline: List[Tuple[str, int, int]],
    sent: Dict[str, float],
) -> Tuple[str, Dict[str, Any]]:
    """
    返回 verdict + explain
    verdict ∈ {"过热","过冷","中性/分歧","材料不足"}
    """
    if today_cnt <= 0:
        return "材料不足", {"reason": "当天未抓到帖子（或 DB 未覆盖当天）。"}

    # baseline 统计（排除当天）
    # baseline: list of (d, cnt, csum)
    base_vals = [(d, c, s) for (d, c, s) in baseline]
    cnts = [c for (_, c, _) in base_vals]
    csums = [s for (_, _, s) in base_vals]
    mean_cnt = sum(cnts) / max(1, len(cnts))
    mean_csum = sum(csums) / max(1, len(csums))

    # 过热/过冷判定：热度是否显著偏离 + 情绪是否明显偏一边 + 语气强度
    heat_ratio = _safe_div(today_cnt, mean_cnt) if mean_cnt else 0.0
    discuss_ratio = _safe_div(today_csum, mean_csum) if mean_csum else 0.0

    pos_ratio = sent.get("pos_ratio", 0.0)
    neg_ratio = sent.get("neg_ratio", 0.0)
    eup_ratio = sent.get("euphoria_ratio", 0.0)
    pan_ratio = sent.get("panic_ratio", 0.0)

    explain = {
        "today_cnt": today_cnt,
        "today_comment_sum_proxy": today_csum,
        "baseline_mean_cnt": round(mean_cnt, 2),
        "baseline_mean_comment_sum_proxy": round(mean_csum, 2),
        "heat_ratio_vs_baseline": round(heat_ratio, 2),
        "discuss_ratio_vs_baseline": round(discuss_ratio, 2),
        "pos_ratio": round(pos_ratio, 2),
        "neg_ratio": round(neg_ratio, 2),
        "euphoria_ratio": round(eup_ratio, 2),
        "panic_ratio": round(pan_ratio, 2),
    }

    # heuristic thresholds
    hot_heat = (heat_ratio >= 1.5) or (discuss_ratio >= 1.8)
    cold_heat = (heat_ratio >= 1.5) or (discuss_ratio >= 1.8)

    # 情绪偏向：以差值为主
    bias = pos_ratio - neg_ratio

    if hot_heat and bias >= 0.18 and eup_ratio >= 0.08:
        return "过热", {**explain, "rule": "热度显著偏高 + 乐观占优 + 亢奋词占比偏高"}
    if cold_heat and bias <= -0.18 and pan_ratio >= 0.08:
        return "过冷", {**explain, "rule": "热度显著偏高 + 悲观占优 + 恐慌词占比偏高"}

    # 热度不高但明显负面（无人问津/冷清）
    if (heat_ratio <= 0.7) and (neg_ratio >= 0.25) and (pan_ratio >= 0.05):
        return "过冷", {**explain, "rule": "热度偏低 + 负面浓度较高（冷清且悲观）"}

    # 热度高但分歧大
    if hot_heat and abs(bias) < 0.12:
        return "中性/分歧", {**explain, "rule": "热度偏高但多空分歧较大"}

    # 默认
    return "中性/分歧", {**explain, "rule": "未触发过热/过冷阈值（倾向分歧或中性）"}


# ---------- LLM ----------
def _init_media_agent():
    """
    复用你的 MediaEngine.agent.DeepSearchAgent，拿到 llm_client。
    """
    try:
        from MediaEngine.agent import DeepSearchAgent  # type: ignore
        return DeepSearchAgent()
    except Exception as e:
        raise RuntimeError(f"Failed to init MediaEngine DeepSearchAgent: {e}")


def _call_llm(llm_client: Any, system_prompt: str, user_prompt: str) -> str:
    if hasattr(llm_client, "stream_invoke_to_string"):
        return llm_client.stream_invoke_to_string(system_prompt, user_prompt)
    if hasattr(llm_client, "invoke_to_string"):
        return llm_client.invoke_to_string(system_prompt, user_prompt)
    if hasattr(llm_client, "invoke"):
        return llm_client.invoke(system_prompt, user_prompt)
    raise RuntimeError("Unsupported LLM client interface")


SYSTEM_PROMPT = """你是A股“股吧舆情/情绪”研究员。
你只能依据我提供的【当天帖子材料】进行分析，不得引入材料之外的事实。
你的任务是判断：当前舆情是“过热/过冷/中性分歧”，并说明原因与证据。
要求：
1) 必须明确写出“分析日期”（当天）与“材料覆盖范围”（当天帖子样本量）。
2) 从消息面角度归纳当天讨论的核心议题（事件/传闻/预期/风险点）。
3) 给出“过热/过冷/中性分歧”的结论，并给出可复核的证据（来自帖子文本）。
4) 最后输出【重要舆情清单】至少10条：每条包含 标题、时间、作者、阅读/评论数、链接、摘录（不超过120字），并说明为什么重要（例如高互动/代表性观点/强烈情绪/关键事实线索）。
写作要克制、可复核，避免行情技术指标推演。"""


def build_llm_prompt(stock_code: str, day: str, stats: Dict[str, Any], posts: List[GubaPost], top_k: int) -> str:
    lines = [
        f"股票代码：{stock_code}",
        f"分析日期：{day}",
        "",
        "【当天舆情统计】",
        f"- 当天帖子数：{stats.get('today_cnt')}",
        f"- 当天评论数合计（代理）：{stats.get('today_csum')}",
        f"- 近{stats.get('baseline_days')}日帖子均值：{stats.get('baseline_mean_cnt')}",
        f"- 近{stats.get('baseline_days')}日评论数合计均值（代理）：{stats.get('baseline_mean_csum')}",
        f"- 热度比（帖子数/均值）：{stats.get('heat_ratio')}",
        f"- 讨论强度比（评论数代理/均值）：{stats.get('discuss_ratio')}",
        f"- 情绪词占比：pos={stats.get('pos_ratio')} neg={stats.get('neg_ratio')} eup={stats.get('euphoria_ratio')} panic={stats.get('panic_ratio')}",
        "",
        "【当天帖子材料（按互动强度排序，节选）】",
    ]
    for i, p in enumerate(posts[:max(top_k, 30)], 1):
        dt = p.last_update_dt or p.crawled_at
        excerpt = (p.full_text or p.title or "").strip()
        excerpt = re.sub(r"\s+", " ", excerpt)
        if len(excerpt) > 220:
            excerpt = excerpt[:219] + "…"
        lines += [
            f"{i}. 标题：{p.title[:120]}",
            f"   时间：{dt}",
            f"   作者：{p.author[:60] if p.author else ''}",
            f"   阅读/评论：{p.read_count or 0}/{p.comment_count or 0}",
            f"   链接：{p.url}",
            f"   摘录：{excerpt}",
        ]
    lines += ["", "请按照系统提示输出分析与重要舆情清单。"]
    return "\n".join(lines)


# ---------- report formatting ----------
def render_rule_report(
    stock_code: str,
    day: str,
    verdict: str,
    explain: Dict[str, Any],
    sent_summary: Dict[str, Any],
    top_posts: List[GubaPost],
    top_k: int,
) -> str:
    lines = []
    lines.append(f"### 股吧舆情（日内）分析：{stock_code}")
    lines.append(f"- 分析日期（当天）：{day}")
    lines.append(f"- 结论：**{verdict}**")
    lines.append("")
    lines.append("#### 1) 热度与情绪概览（规则统计）")
    lines.append(f"- 当天帖子数：{explain.get('today_cnt')}")
    lines.append(f"- 当天评论数合计（代理）：{explain.get('today_comment_sum_proxy')}")
    lines.append(f"- 近{sent_summary.get('baseline_days')}日帖子均值：{explain.get('baseline_mean_cnt')}")
    lines.append(f"- 近{sent_summary.get('baseline_days')}日评论数合计均值（代理）：{explain.get('baseline_mean_comment_sum_proxy')}")
    lines.append(f"- 热度比（帖子数/均值）：{explain.get('heat_ratio_vs_baseline')}")
    lines.append(f"- 讨论强度比（评论数代理/均值）：{explain.get('discuss_ratio_vs_baseline')}")
    lines.append(f"- 情绪词占比：pos={explain.get('pos_ratio')} neg={explain.get('neg_ratio')} eup={explain.get('euphoria_ratio')} panic={explain.get('panic_ratio')}")
    lines.append(f"- 规则解释：{explain.get('rule')}")
    lines.append("")
    lines.append("#### 2) 重要舆情清单（当天，按互动强度/代表性排序）")
    lines.append("> 说明：当前库中为“帖子/发言”正文，不含楼内回复文本；此处输出作为“重要评论/舆情详情”。")
    lines.append("")
    for i, p in enumerate(top_posts[:top_k], 1):
        dt = p.last_update_dt or p.crawled_at
        excerpt = (p.full_text or p.title or "").strip()
        excerpt = re.sub(r"\s+", " ", excerpt)
        if len(excerpt) > 120:
            excerpt = excerpt[:119] + "…"
        lines.append(f"{i}. **{p.title[:120]}**")
        lines.append(f"   - 时间：{dt}")
        lines.append(f"   - 作者：{p.author[:80] if p.author else ''}")
        lines.append(f"   - 阅读/评论：{p.read_count or 0}/{p.comment_count or 0}")
        lines.append(f"   - 链接：{p.url}")
        lines.append(f"   - 摘录：{excerpt}")
    return "\n".join(lines)


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="BettaFish - Guba Sentiment Intel (today only)")
    ap.add_argument("--config", required=True, help="含 [mysql] 的 config.ini（可复用 Guba-Crawler/config.ini）")
    ap.add_argument("--code", required=True, help="股票代码，例如 600519")
    ap.add_argument("--tz", default="Asia/Shanghai", help="用于“当天”判定的时区（默认 Asia/Shanghai）")
    ap.add_argument("--date", default=None, help="指定日期 YYYY-MM-DD（默认当天）")
    ap.add_argument("--limit", type=int, default=300, help="当天最多取多少条进入分析（默认 300）")
    ap.add_argument("--top", type=int, default=20, help="输出重要舆情清单条数（默认 20）")
    ap.add_argument("--baseline-days", type=int, default=14, help="热度基线回看天数（默认 14）")
    ap.add_argument("--no-filter-quote", action="store_true", help="不剔除明显行情播报类帖子（默认会剔除）")
    ap.add_argument("--llm", action="store_true", help="调用 LLM 生成高质量舆情归纳（默认关闭）")
    ap.add_argument("--out-dir", default=None, help="输出目录（默认 ./output/guba_sentiment_intel）")
    args = ap.parse_args()

    # project root bootstrap (for MediaEngine import)
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    cfg = load_config(args.config)

    if args.date:
        day = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
    else:
        day = _today_in_tz(args.tz)
    day_str = day.isoformat()

    posts = fetch_posts_for_date(cfg, args.code.strip(), day=day, limit=args.limit)

    if not args.no_filter_quote:
        posts = [p for p in posts if not _is_market_quote_like(p.title, p.full_text)]

    if not posts:
        print(f"[WARN] 当天未抽取到帖子：code={args.code}, date={day_str}。请先运行采集模块更新 DB。")
        return

    # baseline
    baseline = fetch_daily_counts(cfg, args.code.strip(), days=args.baseline_days)
    today_cnt = len(posts)
    today_csum = sum(int(p.comment_count or 0) for p in posts)

    # sentiment scoring
    agg = {"pos": 0, "neg": 0, "euphoria": 0, "panic": 0}
    for p in posts:
        s = score_sentiment(p.text_for_sentiment())
        for k in agg:
            agg[k] += int(s[k])

    # ratios
    denom = max(1, today_cnt)
    sent_summary = {
        "pos_ratio": round(agg["pos"] / denom, 4),
        "neg_ratio": round(agg["neg"] / denom, 4),
        "euphoria_ratio": round(agg["euphoria"] / denom, 4),
        "panic_ratio": round(agg["panic"] / denom, 4),
        "baseline_days": int(args.baseline_days),
    }
    sent_for_rule = {
        "pos_ratio": float(sent_summary["pos_ratio"]),
        "neg_ratio": float(sent_summary["neg_ratio"]),
        "euphoria_ratio": float(sent_summary["euphoria_ratio"]),
        "panic_ratio": float(sent_summary["panic_ratio"]),
    }

    verdict, explain = classify_hot_cold(today_cnt=today_cnt, today_csum=today_csum, baseline=baseline, sent=sent_for_rule)

    # top posts for output
    posts_sorted = sorted(
        posts,
        key=lambda p: (int(p.comment_count or 0), int(p.read_count or 0), p.last_update_dt or p.crawled_at),
        reverse=True,
    )
    top_k = max(10, int(args.top))
    top_posts = posts_sorted[:top_k]

    out_dir = Path(args.out_dir) if args.out_dir else (project_root / "output" / "guba_sentiment_intel")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # save materials json
    payload = {
        "meta": {
            "code": args.code.strip(),
            "date": day_str,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "today_posts": today_cnt,
            "today_comment_sum_proxy": today_csum,
        },
        "baseline_daily": baseline,
        "sent_lexicon": {**agg, **sent_summary},
        "rule_verdict": verdict,
        "rule_explain": explain,
        "top_posts": [asdict(p) for p in top_posts],
    }
    json_path = out_dir / f"{args.code.strip()}_{day_str}_{ts}_guba_materials.json"
    json_path.write_text(__import__("json").dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # render rule report first
    md_report = render_rule_report(
        stock_code=args.code.strip(),
        day=day_str,
        verdict=verdict,
        explain=explain,
        sent_summary=sent_summary,
        top_posts=top_posts,
        top_k=top_k,
    )

    # optional LLM enhancement
    if args.llm:
        agent = _init_media_agent()
        # build stats for prompt
        # baseline mean
        cnts = [c for (_, c, _) in baseline] or [0]
        csums = [s for (_, _, s) in baseline] or [0]
        mean_cnt = sum(cnts) / max(1, len(cnts))
        mean_csum = sum(csums) / max(1, len(csums))
        stats = {
            "today_cnt": today_cnt,
            "today_csum": today_csum,
            "baseline_days": int(args.baseline_days),
            "baseline_mean_cnt": round(mean_cnt, 2),
            "baseline_mean_csum": round(mean_csum, 2),
            "heat_ratio": round((today_cnt / mean_cnt) if mean_cnt else 0.0, 2),
            "discuss_ratio": round((today_csum / mean_csum) if mean_csum else 0.0, 2),
            **{k: float(v) for k, v in sent_summary.items() if k.endswith("_ratio")},
        }
        llm_prompt = build_llm_prompt(args.code.strip(), day_str, stats, posts_sorted, top_k=top_k)
        llm_text = _call_llm(agent.llm_client, SYSTEM_PROMPT, llm_prompt)
        # prepend rule stats + append llm output
        md_report = md_report + "\n\n---\n\n#### 3) LLM 舆情归纳（仅基于当天帖子材料）\n\n" + llm_text

    md_path = out_dir / f"{args.code.strip()}_{day_str}_{ts}_guba_report.md"
    md_path.write_text(md_report, encoding="utf-8")

    print(f"[OK] Materials saved: {json_path}")
    print(f"[OK] Report saved:    {md_path}")
    print("\n" + "=" * 90 + "\n")
    print(md_report)


if __name__ == "__main__":
    main()
