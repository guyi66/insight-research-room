#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock News Intel Pipeline (BettaFish custom extension)

目标
- 输入：A股股票代码（可选股票简称）
- 输出：
  1) 收集到的“消息面/新闻面”材料（结构化JSON，便于复用）
  2) 基于材料的LLM分析（Markdown文本）

设计约束（按你的最新要求）
1) 不采集/不输出行情类内容：收盘价、涨跌幅、成交额、换手率、融资融券、资金流、龙虎榜、技术指标等均视为“技术面”，本脚本会主动过滤。
2) 真正调用 MindSpider：默认触发 MindSpider/MediaCrawler 多平台爬取（可用 --no-crawl 关闭）。
3) days 支持“真实天数窗口”：默认 7 天，可输入任意正整数。优先使用 Bocha Web Search API 的 freshness
   日期区间（YYYY-MM-DD..YYYY-MM-DD），实现精确时间范围检索。

依赖
- 复用 MediaEngine/agent.py 的 DeepSearchAgent：用于LLM调用（以及在缺少 Bocha Key 时作为搜索兜底）
- 复用 MindSpider DeepSentimentCrawling 的 PlatformCrawler：触发社媒爬取并可选从DB抽取内容
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# -------------------------
# Path bootstrap
# -------------------------
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# -------------------------
# Data model
# -------------------------
@dataclass
class NewsItem:
    title: str
    snippet: str = ""
    url: str = ""
    published_date: str = ""
    source: str = ""

# -------------------------
# Utilities
# -------------------------
def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _safe_text(s: Any, max_len: int = 500) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s

def _dedup_by_url(items: List[NewsItem]) -> List[NewsItem]:
    seen = set()
    out = []
    for it in items:
        key = (it.url or "").strip()
        if not key:
            key = (it.title.strip(), it.snippet[:80].strip())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

# -------------------------
# Technical/quote content filtering (no market quotes)
# -------------------------
_MARKET_QUOTE_PATTERNS = [
    r"收盘", r"开盘", r"最高价", r"最低价", r"涨跌幅", r"涨停", r"跌停",
    r"成交额", r"成交量", r"换手率", r"振幅", r"量比", r"市值",
    r"股价", r"股指", r"K线", r"均线", r"MACD", r"RSI", r"BOLL", r"技术面",
    r"龙虎榜", r"主力资金", r"资金流", r"北向资金", r"南向资金",
    r"融资融券", r"融资净", r"融券", r"两融", r"大宗交易",
]
_MARKET_QUOTE_RE = re.compile("|".join(_MARKET_QUOTE_PATTERNS), flags=re.IGNORECASE)

def _is_market_quote_item(title: str, snippet: str) -> bool:
    text = f"{title} {snippet}"
    return bool(_MARKET_QUOTE_RE.search(text))

# -------------------------
# Bocha raw web search (precise day range via freshness)
# -------------------------
def _get_bocha_api_key() -> Optional[str]:
    # 1) env
    for k in ("BOCHA_API_KEY", "BOCHA_WEB_SEARCH_API_KEY"):
        v = os.getenv(k)
        if v:
            return v.strip()

    # 2) project config (if exists)
    try:
        import config  # type: ignore
        # common patterns in this repo: config.settings.BOCHA_API_KEY / BOCHA_WEB_SEARCH_API_KEY
        settings = getattr(config, "settings", None)
        if settings:
            v = getattr(settings, "BOCHA_API_KEY", None) or getattr(settings, "BOCHA_WEB_SEARCH_API_KEY", None)
            if v:
                return str(v).strip()
        # fallback: module-level
        v = getattr(config, "BOCHA_API_KEY", None) or getattr(config, "BOCHA_WEB_SEARCH_API_KEY", None)
        if v:
            return str(v).strip()
    except Exception:
        pass

    return None

def _get_bocha_web_base_url() -> Optional[str]:
    # 1) env
    for k in (
        "BOCHA_WEB_SEARCH_BASE_URL",
        "BOCHA_WEB_SEARCH_URL",
        "BOCHA_WEB_SEARCH_ENDPOINT",
        "BOCHA_WEB_BASE_URL",
    ):
        v = os.getenv(k)
        if v:
            return v.strip()

    # 2) project config (if exists)
    try:
        import config  # type: ignore
        settings = getattr(config, "settings", None)
        if settings:
            v = (
                getattr(settings, "BOCHA_WEB_SEARCH_BASE_URL", None)
                or getattr(settings, "BOCHA_WEB_SEARCH_URL", None)
                or getattr(settings, "BOCHA_WEB_SEARCH_ENDPOINT", None)
                or getattr(settings, "BOCHA_WEB_BASE_URL", None)
            )
            if v:
                return str(v).strip()
        v = (
            getattr(config, "BOCHA_WEB_SEARCH_BASE_URL", None)
            or getattr(config, "BOCHA_WEB_SEARCH_URL", None)
            or getattr(config, "BOCHA_WEB_SEARCH_ENDPOINT", None)
            or getattr(config, "BOCHA_WEB_BASE_URL", None)
        )
        if v:
            return str(v).strip()
    except Exception:
        pass

    return None

class BochaRawClient:
    """
    直接调用 Bocha Web Search API（用于精确 freshness 日期区间检索）。
    API 示例与 freshness 参数规则来自博查官方文档/示例。  # (见你后续引用说明)
    """

    ENDPOINT = "https://api.bochaai.com/v1/web-search"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30, base_url: Optional[str] = None):
        self.api_key = api_key or _get_bocha_api_key()
        self.timeout = timeout
        self.base_url = (base_url or _get_bocha_web_base_url() or "").strip()

    def _iter_endpoints(self) -> List[str]:
        candidates: List[str] = []
        if self.base_url:
            candidates.append(self.base_url)
            if self.base_url.endswith("/ai-search"):
                candidates.append(self.base_url.replace("/ai-search", "/web-search"))
            elif self.base_url.endswith("/web-search"):
                candidates.append(self.base_url.replace("/web-search", "/ai-search"))
        if self.ENDPOINT not in candidates:
            candidates.append(self.ENDPOINT)
        # dedupe while preserving order
        return list(dict.fromkeys([c for c in candidates if c]))

    def available(self) -> bool:
        return bool(self.api_key)

    def web_search(self, query: str, freshness: str, count: int = 10, summary: bool = True) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("Bocha API key missing (BOCHA_API_KEY / BOCHA_WEB_SEARCH_API_KEY).")

        import requests  # local import to avoid hard dependency at import time

        payload = {
            "query": query,
            "freshness": freshness,
            "summary": bool(summary),
            "count": int(count),
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        last_err: Optional[Exception] = None
        for endpoint in self._iter_endpoints():
            try:
                resp = requests.post(endpoint, headers=headers, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                last_err = exc
                continue
        if last_err:
            raise last_err
        raise RuntimeError("Bocha request failed without response.")

# -------------------------
# MediaEngine agent bootstrap (LLM + search fallback)
# -------------------------
def _init_media_agent():
    try:
        from MediaEngine.agent import DeepSearchAgent  # type: ignore
        return DeepSearchAgent()
    except Exception as e:
        raise RuntimeError(f"Failed to init MediaEngine DeepSearchAgent: {e}")

# -------------------------
# News Collector
# -------------------------
class NewsCollector:
    """
    消息面材料采集器（网页新闻/公告线索）。

    - 主路径：BochaRawClient + freshness 日期区间实现“真实 days”
    - 兜底路径：MediaEngine.agent.execute_search_tool
    """

    def __init__(self):
        self.agent = _init_media_agent()
        self.bocha = BochaRawClient()

    def resolve_stock_name(self, code: str, name: Optional[str] = None) -> Optional[str]:
        """
        仅用于“补全简称”，不把结构化卡片写入材料（避免行情字段混入）。
        """
        if name:
            return name.strip()

        code = code.strip()
        # 用结构化查询尽量拿到简称
        try:
            resp = self.agent.execute_search_tool("search_for_structured_data", f"{code} 股票简称")
        except Exception:
            resp = None

        # 尽量从 response 文本里粗提“简称”
        txt = ""
        try:
            if hasattr(resp, "data") and getattr(resp, "data") is not None:
                txt = json.dumps(getattr(resp, "data"), ensure_ascii=False)
            elif isinstance(resp, dict):
                txt = json.dumps(resp, ensure_ascii=False)
            else:
                txt = str(resp or "")
        except Exception:
            txt = str(resp or "")

        # 常见字段：股票简称/名称/公司名称
        m = re.search(r"(股票简称|简称|名称|公司名称)[\"':：\s]+([A-Za-z0-9\u4e00-\u9fa5]{2,20})", txt)
        if m:
            cand = m.group(2).strip()
            # 简单排除“股票代码”这种误匹配
            if not re.fullmatch(r"\d{6}", cand):
                return cand

        # 兜底：用户没提供就返回 None
        return None

    @staticmethod
    def _bocha_json_to_items(resp_json: Dict[str, Any], source: str) -> List[NewsItem]:
        out: List[NewsItem] = []
        data = resp_json.get("data") if isinstance(resp_json, dict) else None
        if not data and isinstance(resp_json, dict):
            # 有些返回直接就是 SearchResponse
            data = resp_json

        web_pages = None
        if isinstance(data, dict):
            web_pages = data.get("webPages") or {}
        values = []
        if isinstance(web_pages, dict):
            values = web_pages.get("value") or []

        for v in values or []:
            if not isinstance(v, dict):
                continue
            title = v.get("name") or v.get("title") or ""
            url = v.get("url") or ""
            snippet = v.get("snippet") or v.get("summary") or ""
            date_published = v.get("datePublished") or v.get("date") or ""
            site_name = v.get("siteName") or v.get("site") or source
            out.append(
                NewsItem(
                    title=_safe_text(title, 240),
                    snippet=_safe_text(snippet, 600),
                    url=_safe_text(url, 500),
                    published_date=_safe_text(date_published, 40),
                    source=_safe_text(site_name, 80),
                )
            )
        return out

    def collect(self, code: str, name: Optional[str], days: int = 7, max_results_per_query: int = 10) -> List[NewsItem]:
        code = code.strip()
        resolved_name = (name or "").strip()
        base = resolved_name if resolved_name else code

        # 精确日期区间 freshness：YYYY-MM-DD..YYYY-MM-DD
        days = max(int(days), 1)
        end = datetime.now().date()
        start = end - timedelta(days=days - 1)
        freshness = f"{start.isoformat()}..{end.isoformat()}"

        # 消息面查询模板（刻意避免行情措辞）
        query_templates = [
            "{base} 最新 消息 新闻 事项 传闻 澄清",
            "{base} 公告 重大事项 停复牌 回购 增减持 股权激励",
            "{base} 业绩 预告 业绩快报 年报 季报 分红",
            "{base} 监管 问询函 立案 调查 处罚 合规 风险提示",
            "{base} 经营 合作 合同 中标 订单 渠道 经销商 库存",
            "{base} 产品 价格 批价 指导价 调价 新品",
            "{base} 产能 扩产 投资 项目 建设",
            "{base} 行业 政策 规范 指导意见 税收",
            "{base} 机构 调研 券商 研报 评级 观点",
            "{base} 诉讼 仲裁 事故 召回 质量 风险",
        ]

        items: List[NewsItem] = []
        for qt in query_templates:
            q = qt.format(base=base)
            # 主路径：Bocha raw
            if self.bocha.available():
                try:
                    j = self.bocha.web_search(query=q, freshness=freshness, count=max_results_per_query, summary=True)
                    items.extend(self._bocha_json_to_items(j, source="bocha_web"))
                    continue
                except Exception:
                    # fallthrough to agent fallback
                    pass

            # 兜底：MediaEngine 的工具（时间窗口可能较粗，但至少能跑通）
            tool = "search_last_24_hours" if days <= 1 else "search_last_week"
            try:
                resp = self.agent.execute_search_tool(tool, q, max_results=max_results_per_query)
            except TypeError:
                resp = self.agent.execute_search_tool(tool, q)
            # 兼容 BochaResponse / dict
            try:
                if hasattr(resp, "dict"):
                    rj = resp.dict()
                elif isinstance(resp, dict):
                    rj = resp
                else:
                    # 兜底：转成 dict 包一层
                    rj = {"data": getattr(resp, "data", None)}
                items.extend(self._bocha_json_to_items(rj, source="bocha_web"))
            except Exception:
                continue

        # 去重 + 过滤行情类
        items = _dedup_by_url(items)
        items = [it for it in items if not _is_market_quote_item(it.title, it.snippet)]
        return items

# -------------------------
# MindSpider integration
# -------------------------
class MindSpiderSocialCollector:
    """
    触发 MindSpider 的多平台关键词爬取，并尝试从数据库读取最新内容。

    - 触发爬取：PlatformCrawler.run_multi_platform_crawl_by_keywords()
    - 读取数据库：自动探测表结构并做“弱假设”抽取（你可按本地 schema 收紧）
    """

    def __init__(self):
        self._platform_crawler = None
        try:
            from MindSpider.DeepSentimentCrawling.MediaCrawler.platform_crawler import PlatformCrawler  # type: ignore
            self._platform_crawler = PlatformCrawler()
        except Exception:
            self._platform_crawler = None

    def crawl(
        self,
        keywords: List[str],
        platforms: List[str],
        max_notes_per_keyword: int = 50,
        login_type: str = "qrcode",
    ) -> Dict[str, Any]:
        if not self._platform_crawler:
            return {"enabled": False, "reason": "PlatformCrawler import failed or MediaCrawler missing."}

        platforms = [p for p in platforms if p in getattr(self._platform_crawler, "supported_platforms", platforms)]
        if not platforms:
            return {"enabled": False, "reason": "No valid platforms provided."}

        stats = self._platform_crawler.run_multi_platform_crawl_by_keywords(
            keywords=keywords,
            platforms=platforms,
            login_type=login_type,
            max_notes_per_keyword=max_notes_per_keyword,
        )
        return {"enabled": True, "stats": stats}

    def fetch_recent_posts_from_db(self, keywords: List[str], limit_total: int = 200) -> List[NewsItem]:
        """
        从 MindSpider/MediaCrawler 落库中抽取近期内容片段，作为“舆情材料”补充。
        """
        try:
            import config  # type: ignore
            from sqlalchemy import create_engine, inspect, text

            settings = getattr(config, "settings", config)
            dialect = (getattr(settings, "DB_DIALECT", "mysql") or "mysql").lower()
            user = getattr(settings, "DB_USER", "root")
            pwd = getattr(settings, "DB_PASSWORD", "")
            host = getattr(settings, "DB_HOST", "127.0.0.1")
            port = getattr(settings, "DB_PORT", "3306")
            db = getattr(settings, "DB_NAME", getattr(settings, "MYSQL_DB", "mindspider"))

            if dialect.startswith("mysql"):
                url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
            elif dialect.startswith("postgres"):
                url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
            else:
                return []

            engine = create_engine(url)
            insp = inspect(engine)
            tables = insp.get_table_names()

            # 经验规则：抓取“内容类表”
            cand_tables = [t for t in tables if any(k in t.lower() for k in ["weibo", "xhs", "douyin", "bili", "tieba", "zhihu", "comment", "note", "post"])]
            if not cand_tables:
                cand_tables = tables[:]

            # 优先找包含文本内容字段的表
            picked: List[Tuple[str, List[str]]] = []
            for t in cand_tables:
                cols = [c["name"] for c in insp.get_columns(t)]
                text_cols = [c for c in cols if any(k in c.lower() for k in ["content", "text", "desc", "title", "note", "detail", "comment"])]
                if text_cols:
                    picked.append((t, cols))

            if not picked:
                return []

            # 抽样读取：每表取少量，避免慢
            out: List[NewsItem] = []
            per_table = max(10, int(limit_total / max(1, min(len(picked), 6))))

            for t, cols in picked[:6]:
                title_col = next((c for c in cols if "title" in c.lower()), None)
                content_col = next((c for c in cols if any(k in c.lower() for k in ["content", "text", "desc", "detail", "comment"])), None)
                time_col = next((c for c in cols if any(k in c.lower() for k in ["time", "date", "publish", "create"])), None)

                if not content_col:
                    continue

                sql = f"SELECT {', '.join([c for c in [title_col, content_col, time_col] if c])} FROM {t} ORDER BY {time_col} DESC LIMIT {per_table}" if time_col else f"SELECT {', '.join([c for c in [title_col, content_col] if c])} FROM {t} LIMIT {per_table}"
                rows = engine.execute(text(sql)).fetchall()

                for r in rows:
                    row = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
                    title = _safe_text(row.get(title_col) if title_col else "", 120)
                    content = _safe_text(row.get(content_col) if content_col else "", 600)
                    if not content:
                        continue
                    # 关键词过滤
                    if keywords:
                        hay = f"{title} {content}"
                        if not any(k in hay for k in keywords if k):
                            continue
                    tstr = _safe_text(row.get(time_col) if time_col else "", 40)
                    out.append(NewsItem(title=title or "（社媒/论坛内容）", snippet=content, url="", published_date=tstr, source=t))

            # 过滤掉明显的“行情”类文本（有些帖子会复制行情播报）
            out = [it for it in out if not _is_market_quote_item(it.title, it.snippet)]
            return out[:limit_total]
        except Exception:
            return []

# -------------------------
# LLM analysis prompts
# -------------------------
SYSTEM_PROMPT_STOCK_NEWS = """你是A股上市公司消息面研究员。
你只能使用我提供的【材料】进行归纳、归类与推断；不得引入材料之外的事实。
注意：以下内容属于“技术面/行情面”，本任务必须忽略且不得输出：收盘价、涨跌幅、成交额、换手率、成交量、资金流、融资融券、龙虎榜、K线/技术指标等。
如果材料不足以支撑某个结论，必须写“材料不足，暂无法判断”，并给出需要补充的验证项。
输出必须包含：1) 事件要点与时间线 2) 利好/利空/中性分组与影响路径 3) 风险点与待确认事项 4) 接下来1-2周的跟踪清单。
语言要专业、克制、可复核。"""

def _call_llm(llm_client: Any, system_prompt: str, user_prompt: str) -> str:
    if hasattr(llm_client, "stream_invoke_to_string"):
        return llm_client.stream_invoke_to_string(system_prompt, user_prompt)
    if hasattr(llm_client, "invoke_to_string"):
        return llm_client.invoke_to_string(system_prompt, user_prompt)
    if hasattr(llm_client, "invoke"):
        return llm_client.invoke(system_prompt, user_prompt)
    raise RuntimeError("Unsupported LLM client interface in MediaEngine.llms.LLMClient")

def build_user_prompt(code: str, name: Optional[str], news_items: List[NewsItem], social_items: List[NewsItem]) -> str:
    header = f"股票：{code}" + (f"（{name}）" if name else "")
    lines = [header, "", "【材料-网页新闻/公告线索】"]
    for i, it in enumerate(news_items[:120], 1):
        lines.append(f"{i}. 标题：{_safe_text(it.title, 160)}")
        if it.published_date:
            lines.append(f"   时间：{_safe_text(it.published_date, 40)}")
        if it.source:
            lines.append(f"   来源：{_safe_text(it.source, 80)}")
        if it.url:
            lines.append(f"   链接：{it.url}")
        if it.snippet:
            lines.append(f"   摘要：{_safe_text(it.snippet, 360)}")

    if social_items:
        lines += ["", "【材料-社媒/论坛（数据库抽取）】"]
        for i, it in enumerate(social_items[:120], 1):
            lines.append(f"{i}. 表：{it.source}")
            if it.published_date:
                lines.append(f"   时间：{_safe_text(it.published_date, 40)}")
            lines.append(f"   标题：{_safe_text(it.title, 160)}")
            lines.append(f"   内容片段：{_safe_text(it.snippet, 360)}")
    else:
        lines += ["", "【材料-社媒/论坛】未抽取到可用内容（可能登录/配置/表结构问题）。"]

    lines += ["", "请基于以上材料输出消息面/新闻面分析。不要输出与材料无关的背景科普。不要写行情数据。"]
    return "\n".join(lines)

# -------------------------
# Pipeline
# -------------------------
def run_pipeline(
    code: str,
    name: Optional[str] = None,
    days: int = 7,
    max_results_per_query: int = 10,
    # MindSpider
    enable_crawl: bool = True,
    platforms: Optional[List[str]] = None,
    max_notes_per_keyword: int = 50,
    login_type: str = "qrcode",
    # output
    only_collect: bool = False,
    out_dir: Optional[Path] = None,
) -> Tuple[Path, Optional[Path], str]:
    code = code.strip()
    out_dir = out_dir or (PROJECT_ROOT / "output" / "stock_news_intel")
    out_dir.mkdir(parents=True, exist_ok=True)

    collector = NewsCollector()
    social = MindSpiderSocialCollector()

    resolved_name = collector.resolve_stock_name(code, name=name) or (name.strip() if name else None)

    # keywords for MindSpider
    keywords = [k for k in [resolved_name, code] if k]
    if resolved_name:
        keywords += [
            f"{resolved_name} 公告",
            f"{resolved_name} 业绩",
            f"{resolved_name} 监管",
            f"{resolved_name} 经销商",
        ]
    keywords = list(dict.fromkeys([k.strip() for k in keywords if k and k.strip()]))

    # 1) MindSpider crawl (default enabled)
    crawl_result: Dict[str, Any] = {"enabled": False, "reason": "not started"}
    platforms = platforms or ["wb", "xhs"]
    if enable_crawl:
        crawl_result = social.crawl(
            keywords=keywords,
            platforms=platforms,
            max_notes_per_keyword=max_notes_per_keyword,
            login_type=login_type,
        )
    else:
        crawl_result = {"enabled": False, "reason": "crawl disabled by flag."}

    # 2) Web news materials (precise days)
    news_items = collector.collect(code=code, name=resolved_name, days=days, max_results_per_query=max_results_per_query)

    # 3) Social materials from DB
    social_items = social.fetch_recent_posts_from_db(keywords=keywords, limit_total=200) if enable_crawl else []

    # 4) Save materials
    payload = {
        "meta": {
            "code": code,
            "name": resolved_name,
            "days": int(days),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        },
        "crawl_result": crawl_result,
        "web_news": [asdict(x) for x in news_items],
        "social_posts": [asdict(x) for x in social_items],
    }
    json_path = out_dir / f"{code}_{_now_ts()}_materials.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if only_collect:
        return json_path, None, ""

    # 5) LLM analysis
    user_prompt = build_user_prompt(code=code, name=resolved_name, news_items=news_items, social_items=social_items)
    analysis_text = _call_llm(collector.agent.llm_client, SYSTEM_PROMPT_STOCK_NEWS, user_prompt)

    md_path = out_dir / f"{code}_{_now_ts()}_analysis.md"
    md_path.write_text(analysis_text, encoding="utf-8")
    return json_path, md_path, analysis_text

# -------------------------
# CLI
# -------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BettaFish - Stock News Intel Pipeline")
    p.add_argument("--code", required=True, help="A股股票代码，如 000001 / 600519")
    p.add_argument("--name", default=None, help="股票简称（可选，不填则尝试自动识别）")
    p.add_argument("--days", type=int, default=7, help="检索窗口（天，默认7）。将使用 Bocha freshness 日期区间实现精确时间范围检索。")
    p.add_argument("--max-results", type=int, default=10, help="每个查询的最大搜索结果数（默认10）")
    p.add_argument("--only-collect", action="store_true", help="只采集材料，输出JSON，不做LLM分析")
    p.add_argument("--out-dir", default=None, help="输出目录（默认 ./output/stock_news_intel）")

    # MindSpider options (default enabled)
    p.add_argument("--crawl", dest="crawl", action="store_true", default=True, help="触发MindSpider爬取（默认开启）")
    p.add_argument("--no-crawl", dest="crawl", action="store_false", help="关闭MindSpider爬取")
    p.add_argument("--platforms", nargs="*", default=["wb", "xhs"], help="爬取平台列表（默认 wb xhs）")
    p.add_argument("--max-notes", type=int, default=50, help="每个平台每个关键词最大爬取数（默认50）")
    p.add_argument("--login-type", default="qrcode", help="MediaCrawler登录方式（默认 qrcode）")

    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    out_dir = Path(args.out_dir) if args.out_dir else None

    json_path, md_path, analysis = run_pipeline(
        code=args.code,
        name=args.name,
        days=args.days,
        max_results_per_query=args.max_results,
        enable_crawl=args.crawl,
        platforms=args.platforms,
        max_notes_per_keyword=args.max_notes,
        login_type=args.login_type,
        only_collect=args.only_collect,
        out_dir=out_dir,
    )

    print(f"[OK] Materials saved: {json_path}")
    if md_path:
        print(f"[OK] Analysis saved:  {md_path}")
        print("\n" + "=" * 80 + "\n")
        print(analysis)
