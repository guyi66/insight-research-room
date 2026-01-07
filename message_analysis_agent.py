from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pymysql

try:
    from config import settings
except Exception:
    settings = None

try:
    from QueryEngine.llms.base import LLMClient as QueryLLMClient
except Exception:
    QueryLLMClient = None

try:
    from stock_news_intel import BochaRawClient
except Exception:
    BochaRawClient = None


TEXT_COL_CANDIDATES = [
    "title", "content", "desc", "text", "full_text", "summary", "snippet",
    "note", "subject", "topic", "tag_list", "keywords", "keyword",
    "source_keyword", "search_keyword", "search_word", "query", "name",
]
URL_COL_CANDIDATES = [
    "url", "href", "link", "share_url", "note_url", "content_url",
    "video_url", "aweme_url", "jump_url", "detail_url",
]
AUTHOR_COL_CANDIDATES = [
    "author", "nickname", "user", "user_name", "user_nickname", "screen_name",
]
KEYWORD_COL_CANDIDATES = ["keyword", "source_keyword", "search_keyword", "search_word", "tag_list"]

PUBLISHED_TIME_CANDIDATES = [
    "published_at", "publish_time", "publish_date", "created_at", "create_time",
    "create_date_time", "created_time", "time", "date", "last_update_dt",
    "last_update_time", "post_time",
]
INGEST_TIME_CANDIDATES = [
    "crawled_at", "ingested_at", "updated_at", "update_ts", "last_modify_ts",
]

ENGAGEMENT_COL_CANDIDATES = [
    "like_count", "liked_count", "likes", "comment_count", "comments_count",
    "reply_count", "share_count", "shared_count", "collect_count", "favorite_count",
    "read_count", "view_count", "viewd_count", "video_play_count",
]

IMAGE_COL_KEYWORDS = ("image", "img", "pic", "cover", "poster", "thumb", "thumbnail")


@dataclass
class MessageItem:
    platform: str
    title: str
    content: str
    url: str
    author: str
    published_at: str
    keyword: str
    engagement: int
    match_count: int
    score: int
    source_table: str
    time_source: str
    media_urls: List[str]


class MessageAnalysisAgent:
    def __init__(
        self,
        llm_client: Any,
        keyword_llm: Optional[Any] = None,
        max_keywords: int = 10,
        max_items: int = 30,
        max_query_rows: int = 800,
        max_prompt_chars: int = 12000,
        db_name: Optional[str] = None,
    ) -> None:
        self.llm = llm_client
        self.keyword_llm = keyword_llm or self._init_keyword_llm()
        self.max_keywords = max(3, int(max_keywords))
        self.max_items = max(10, int(max_items))
        self.max_query_rows = max(200, int(max_query_rows))
        self.max_prompt_chars = max(4000, int(max_prompt_chars))
        self.db_name = db_name or self._get_db_name()

    def analyze(
        self,
        stock_code: str,
        stock_name: Optional[str] = None,
        days: int = 7,
        use_bocha: bool = False,
    ) -> Dict[str, Any]:
        stock_code = (stock_code or "").strip()
        stock_name = (stock_name or "").strip()
        days = max(1, int(days))

        keywords = self._generate_keywords(stock_code=stock_code, stock_name=stock_name, days=days)

        web_items: List[Dict[str, str]] = []
        if use_bocha and keywords:
            web_items = self._bocha_search_keywords(keywords, days)

        start_dt = datetime.now() - timedelta(days=days - 1)
        end_dt = datetime.now()

        rows, err = self._fetch_materials(
            stock_code=stock_code,
            keywords=keywords,
            limit=self.max_query_rows,
        )

        items = self._select_and_rank(rows, keywords, start_dt, end_dt)
        top_items = items[: self.max_items]

        analysis_md = self._build_message_report(
            stock_code=stock_code,
            stock_name=stock_name,
            days=days,
            keywords=keywords,
            items=top_items,
            web_items=web_items,
            db_error=err,
        )

        return {
            "analysis_md": analysis_md,
            "keywords": keywords,
            "items": top_items,
            "web_items": web_items,
            "db_error": err,
        }

    def _init_keyword_llm(self) -> Optional[Any]:
        if QueryLLMClient is None:
            return None
        if not settings:
            return None
        try:
            api_key = settings.QUERY_ENGINE_API_KEY
            model_name = settings.QUERY_ENGINE_MODEL_NAME
            base_url = settings.QUERY_ENGINE_BASE_URL
            if not api_key or not model_name:
                return None
            return QueryLLMClient(api_key=api_key, model_name=model_name, base_url=base_url)
        except Exception:
            return None

    def _generate_keywords(self, stock_code: str, stock_name: str, days: int) -> List[str]:
        base_keywords = []
        if stock_name:
            base_keywords.append(stock_name)
        if stock_code and stock_code not in base_keywords:
            base_keywords.append(stock_code)

        llm = self.keyword_llm or self.llm
        if not llm:
            return self._dedupe_keywords(base_keywords)

        system_prompt = (
            "You generate concise keywords for retrieving stock-related message/news/social data "
            "from local databases. Output JSON only in the form: "
            '{"keywords":["..."],"reasoning":"..."}. '\
            "Rules: 6-12 keywords, short phrases, include the stock name and stock code, "
            "prefer event/rumor/announcement topics if present. Avoid long sentences."
        )

        user_prompt = (
            f"Stock code: {stock_code}\n"
            f"Stock name: {stock_name or 'N/A'}\n"
            f"Time window: last {days} days\n"
        )

        raw = self._call_llm(llm, system_prompt, user_prompt)
        parsed = self._parse_keywords(raw)
        merged = base_keywords + parsed
        return self._dedupe_keywords(merged)[: self.max_keywords]

    def _build_message_report(
        self,
        stock_code: str,
        stock_name: str,
        days: int,
        keywords: Sequence[str],
        items: Sequence[MessageItem],
        web_items: Sequence[Dict[str, str]],
        db_error: Optional[str],
    ) -> str:
        if not items:
            if db_error:
                return f"## 消息面分析\n\n数据库查询失败：{db_error}"
            return f"## 消息面分析\n\n近 {days} 天未在数据库中找到 {stock_code} 的相关消息数据。"

        system_prompt = (
            "你是一名专业A股/港股/美股权益研究分析师，专注于对【消息面/新闻/公告/研报摘要/社媒舆情】进行结构化解读与情绪研判，并评估其对股价的潜在影响路径。\n"
            "\n"
            "【硬性约束】\n"
            "1) 只能使用用户提供的材料（文本、链接摘要、公告摘录、数据片段等）进行分析；不得引入外部信息或常识补全；不得编造公司事实、财务数据、政策细节、人物言论或时间线。\n"
            "2) 若材料不足以支持结论，必须明确写“材料不足/无法确认”，并说明缺失点；不得用模糊话替代证据。\n"
            "3) 必须区分“事实”“推断”“观点”，并对每条关键结论标注依据来自哪一段材料（用[证据#]引用编号）。\n"
            "4) 输出必须为【中文 Markdown】；结构清晰；结论前置；用词克制、专业，不写营销话术。\n"
            "\n"
            "【你的任务】\n"
            "对给定材料进行：事件归纳 → 影响机制拆解 → 情绪与风险评估 → 可跟踪验证点输出。\n"
            "\n"
            "【分析流程】\n"
            "A. 材料清点与证据编号：将材料按来源与时间排序，提炼关键句，并编号为[证据1]、[证据2]…\n"
            "B. 事件与主题聚类：识别材料涉及的事件类型（如：业绩/订单/产品/政策监管/股东减持增持/诉讼仲裁/安全事故/行业供需/同业对比/融资并购等），并给出一句话事件摘要。\n"
            "C. 情绪研判：\n"
            "   - 给出总体情绪：正面/中性/负面/混合，并给出情绪强度(1-5)。\n"
            "   - 分解情绪来源：市场预期差、确定性、可验证性、冲击范围、持续性。\n"
            "D. 影响机制拆解（从“消息→预期→估值/盈利→资金行为→股价波动”角度）：\n"
            "   - 明确可能影响的变量：收入、利润率、现金流、资本开支、资产减值、合规成本、竞争格局、供需价格、风险溢价等。\n"
            "   - 对每条机制给出：方向(利好/利空/不确定) + 传导路径 + 触发条件 + 证据引用。\n"
            "E. 影响时间尺度：将影响分为短期(1-5交易日)、中期(1-3个月)、长期(>3个月)；分别说明逻辑与不确定性。\n"
            "F. 关键不确定性与反证：列出最可能推翻当前判断的3-5个点，以及需要看到什么新信息才能确认/否定。\n"
            "G. 风险清单：监管/财务/经营/舆情/治理/交易结构等维度，给出风险等级(低/中/高)与原因。\n"
            "H. 可跟踪清单：输出“下一步要盯的公告/数据/时间点/口径变化”，并说明每项会如何改变结论。\n"
            "\n"
            "【输出格式（必须严格遵守）】\n"
            "## 1. 结论摘要（3-6条要点）\n"
            "## 2. 材料清点与证据编号\n"
            "## 3. 事件拆解与主题归类\n"
            "## 4. 情绪判断（方向+强度+来源）\n"
            "## 5. 影响机制与情景分析（逐条写：方向/路径/条件/依据[证据#]）\n"
            "## 6. 时间尺度：短期/中期/长期\n"
            "## 7. 不确定性、反证与需要补充的信息\n"
            "## 8. 风险清单与风险等级\n"
            "## 9. 可跟踪清单（监测项-触发信号-可能结论变化）\n"
            "\n"
            "【额外要求】\n"
            "- 不输出投资建议（如“买入/卖出/目标价”）；只做消息面影响研判与风险提示。\n"
            "- 若材料包含多条相互矛盾信息，必须显式标注冲突并解释可能原因。\n"
                )

        material_lines = self._format_items_for_prompt(items, max_chars=self.max_prompt_chars)
        web_lines = self._format_web_items_for_prompt(web_items, max_chars=2400)

        kw_str = ", ".join([k for k in keywords if k])
        user_prompt = (
            f"Stock code: {stock_code}\n"
            f"Stock name: {stock_name or 'N/A'}\n"
            f"Time window: last {days} days\n"
            f"Keywords: {kw_str}\n\n"
            "DB Materials:\n"
            f"{material_lines}\n\n"
        )
        if web_lines:
            user_prompt += "Web Materials (optional):\n" + web_lines + "\n\n"

        user_prompt += (
            "Task: Write a coherent message-side analysis (消息面分析). "
            "Include sections: 1) 关键信息摘要, 2) 可能的传导路径, "
            "3) 风险与不确定性, 4) 关注事项. "
            "Keep it concise and logical, 400-800 Chinese characters."
        )

        answer = self._call_llm(self.llm, system_prompt, user_prompt)
        if not answer:
            return self._fallback_summary(stock_code, days, items)
        return answer

    def _fallback_summary(self, stock_code: str, days: int, items: Sequence[MessageItem]) -> str:
        lines = [
            "## 消息面分析（自动回退）",
            f"- 标的：{stock_code}",
            f"- 窗口：近 {days} 天",
            "",
            "### 关键信息摘要",
        ]
        for i, it in enumerate(items[:10], 1):
            lines.append(f"{i}. [{it.platform}] {it.published_at} | {it.title}")
        lines.append("")
        lines.append("### 风险与不确定性")
        lines.append("样本数量有限，建议结合公告和财务数据进一步核验。")
        return "\n".join(lines)

    def _format_items_for_prompt(self, items: Sequence[MessageItem], max_chars: int) -> str:
        lines = []
        total = 0
        for i, it in enumerate(items, 1):
            snippet = (it.content or it.title or "").strip()
            snippet = re.sub(r"\s+", " ", snippet)
            if len(snippet) > 140:
                snippet = snippet[:139] + "..."
            media = ""
            if it.media_urls:
                media = " | media=" + ",".join(it.media_urls[:2])
            row = (
                f"{i}. [{it.platform}] {it.published_at} | {it.title} | "
                f"match={it.match_count} | eng={it.engagement} | src={it.source_table}{media}\n"
                f"   {snippet}\n"
                f"   url: {it.url}"
            )
            total += len(row)
            if total > max_chars:
                break
            lines.append(row)
        return "\n".join(lines)

    def _format_web_items_for_prompt(self, items: Sequence[Dict[str, str]], max_chars: int) -> str:
        if not items:
            return ""
        lines = []
        total = 0
        for i, it in enumerate(items, 1):
            title = (it.get("title") or "").strip()
            snippet = (it.get("snippet") or "").strip()
            url = (it.get("url") or "").strip()
            row = f"{i}. {title} | {snippet} | {url}"
            total += len(row)
            if total > max_chars:
                break
            lines.append(row)
        return "\n".join(lines)

    def _fetch_materials(
        self,
        stock_code: str,
        keywords: Sequence[str],
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        cfg = self._get_db_config()
        if not cfg:
            return [], "missing_db_config"

        try:
            conn = self._connect_mysql(cfg)
        except Exception as e:
            return [], repr(e)

        rows: List[Dict[str, Any]] = []
        try:
            tables = self._list_stock_tables(conn, cfg["database"])
            per_table_limit = min(max(120, self.max_items * 8), 400)
            rows = self._collect_rows(
                conn,
                tables,
                stock_code,
                keywords,
                per_table_limit,
            )
            if not rows and keywords:
                rows = self._collect_rows(
                    conn,
                    tables,
                    stock_code,
                    [],
                    per_table_limit,
                )
            return rows[:limit], None
        except Exception as e:
            return [], repr(e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _select_and_rank(
        self,
        rows: Sequence[Dict[str, Any]],
        keywords: Sequence[str],
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[MessageItem]:
        if not rows:
            return []

        kw_list = [k for k in keywords if k]
        parsed = []
        for r in rows:
            table = str(r.get("_source_table") or "")
            columns = list(r.keys())

            published_val = self._extract_first(r, PUBLISHED_TIME_CANDIDATES)
            ingested_val = self._extract_first(r, INGEST_TIME_CANDIDATES)
            pub_dt = self._parse_time(published_val)
            ing_dt = self._parse_time(ingested_val)

            time_source = "published" if pub_dt else ("ingested" if ing_dt else "unknown")
            published_at = self._format_time(pub_dt or ing_dt)

            platform = str(self._extract_first(r, ["platform"]) or table)
            title = str(self._extract_first(r, ["title", "name", "subject", "topic"]) or "").strip()
            content = str(self._extract_first(r, ["content", "desc", "text", "full_text", "summary"]) or "").strip()
            url = str(self._extract_first(r, URL_COL_CANDIDATES) or "").strip()
            author = str(self._extract_first(r, AUTHOR_COL_CANDIDATES) or "").strip()
            keyword = str(self._extract_first(r, KEYWORD_COL_CANDIDATES) or "").strip()

            media_urls = self._extract_media_urls(r)

            text = f"{title} {content} {keyword}".lower()
            match_count = 0
            for kw in kw_list:
                if kw:
                    match_count += text.count(kw.lower())

            engagement = 0
            for col in ENGAGEMENT_COL_CANDIDATES:
                if col in r:
                    try:
                        engagement += int(r.get(col) or 0)
                    except Exception:
                        pass

            score = match_count * 1000 + engagement

            parsed.append({
                "platform": platform,
                "title": title,
                "content": content,
                "url": url,
                "author": author,
                "published_at": published_at,
                "keyword": keyword,
                "engagement": engagement,
                "match_count": match_count,
                "score": score,
                "source_table": table,
                "time_source": time_source,
                "media_urls": media_urls,
                "pub_dt": pub_dt,
                "ing_dt": ing_dt,
            })

        has_match = any(p["match_count"] > 0 for p in parsed)
        if has_match:
            parsed = [p for p in parsed if p["match_count"] > 0]

        published_in_range = [
            p for p in parsed
            if p["pub_dt"] and start_dt <= p["pub_dt"] <= end_dt
        ]
        no_published = [p for p in parsed if p["pub_dt"] is None]
        ingested_in_range = [
            p for p in no_published
            if p["ing_dt"] and start_dt <= p["ing_dt"] <= end_dt
        ]

        published_in_range.sort(key=lambda x: (x["score"], x.get("pub_dt") or datetime.min), reverse=True)
        ingested_in_range.sort(key=lambda x: (x["score"], x.get("ing_dt") or datetime.min), reverse=True)

        selected = published_in_range[: self.max_items]
        remaining = self.max_items - len(selected)
        if remaining > 0:
            selected.extend(ingested_in_range[:remaining])

        remaining = self.max_items - len(selected)
        if remaining > 0:
            leftovers = [p for p in no_published if p not in ingested_in_range]
            leftovers.sort(key=lambda x: (x["score"], x.get("ing_dt") or datetime.min), reverse=True)
            selected.extend(leftovers[:remaining])

        out = []
        for p in selected:
            out.append(
                MessageItem(
                    platform=p["platform"],
                    title=p["title"],
                    content=p["content"],
                    url=p["url"],
                    author=p["author"],
                    published_at=p["published_at"],
                    keyword=p["keyword"],
                    engagement=p["engagement"],
                    match_count=p["match_count"],
                    score=p["score"],
                    source_table=p["source_table"],
                    time_source=p["time_source"],
                    media_urls=p["media_urls"],
                )
            )
        return out

    def _bocha_search_keywords(self, keywords: Sequence[str], days: int) -> List[Dict[str, str]]:
        if BochaRawClient is None:
            return []
        try:
            client = BochaRawClient()
            if not client.available():
                return []
            base = " ".join([k for k in keywords if k][:6])
            end = datetime.now().date()
            start = end - timedelta(days=max(1, int(days)) - 1)
            freshness = f"{start.isoformat()}..{end.isoformat()}"
            resp = client.web_search(base, freshness=freshness, count=10, summary=True)
            return self._parse_bocha_items(resp)
        except Exception:
            return []

    @staticmethod
    def _parse_bocha_items(resp: Dict[str, Any]) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        if not isinstance(resp, dict):
            return items
        data = resp.get("data") or resp
        web_pages = data.get("webPages") or {}
        values = web_pages.get("value") or []
        for v in values or []:
            if not isinstance(v, dict):
                continue
            items.append(
                {
                    "title": str(v.get("name") or v.get("title") or ""),
                    "snippet": str(v.get("snippet") or v.get("summary") or ""),
                    "url": str(v.get("url") or ""),
                }
            )
        return items

    def _call_llm(self, llm: Any, system_prompt: str, user_prompt: str) -> str:
        if not llm:
            return ""
        try:
            if hasattr(llm, "invoke"):
                return llm.invoke(system_prompt, user_prompt, temperature=0.2)
            if hasattr(llm, "stream_invoke_to_string"):
                return llm.stream_invoke_to_string(system_prompt, user_prompt, temperature=0.2)
        except Exception:
            return ""
        return ""

    @staticmethod
    def _parse_keywords(text: str) -> List[str]:
        if not text:
            return []
        text = text.strip()
        try:
            obj = json.loads(text)
            kws = obj.get("keywords") or []
            if isinstance(kws, list):
                return [str(k).strip() for k in kws if str(k).strip()]
        except Exception:
            pass
        parts = re.split(r"[\n,;，；、]+", text)
        return [p.strip() for p in parts if p.strip()]

    @staticmethod
    def _dedupe_keywords(keywords: Sequence[str]) -> List[str]:
        seen = set()
        out = []
        for k in keywords:
            s = (k or "").strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    def _collect_rows(
        self,
        conn: pymysql.connections.Connection,
        tables: Sequence[str],
        stock_code: str,
        keywords: Sequence[str],
        per_table_limit: int,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for table in tables:
            columns = self._get_table_columns(conn, table)
            if not columns:
                continue

            text_cols = self._pick_text_columns(columns)
            media_cols = self._pick_media_columns(columns)
            if not text_cols and not media_cols:
                continue

            select_cols = self._build_select_columns(columns, text_cols, media_cols)
            kw_clause, kw_params = self._build_keyword_filter(text_cols, keywords)
            stock_clause, stock_params = self._build_stock_filter(columns, stock_code)

            where_clause = " WHERE " + " AND ".join([c for c in [stock_clause, kw_clause] if c])

            order_col = self._pick_order_column(columns)
            order_sql = f" ORDER BY `{order_col}` DESC" if order_col else ""
            sql = (
                f"SELECT {select_cols} FROM `{table}`"
                f"{where_clause}{order_sql} LIMIT %s"
            )
            params = tuple(stock_params + kw_params + [int(per_table_limit)])

            with conn.cursor() as cur:
                cur.execute(sql, params)
                table_rows = cur.fetchall() or []
            for r in table_rows:
                r["_source_table"] = table
                rows.append(r)
        return rows

    def _get_db_name(self) -> str:
        if settings and getattr(settings, "DB_NAME", None):
            return str(settings.DB_NAME)
        return os.getenv("DB_NAME", "")

    def _get_db_config(self) -> Optional[Dict[str, Any]]:
        host = (settings.DB_HOST if settings else None) or os.getenv("DB_HOST") or "127.0.0.1"
        port = int((settings.DB_PORT if settings else None) or os.getenv("DB_PORT") or 3306)
        user = (settings.DB_USER if settings else None) or os.getenv("DB_USER") or "root"
        password = (settings.DB_PASSWORD if settings else None) or os.getenv("DB_PASSWORD") or ""
        database = self.db_name or ""
        if not database:
            database = (settings.DB_NAME if settings else None) or os.getenv("DB_NAME") or ""
        if not database:
            return None
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    @staticmethod
    def _connect_mysql(cfg: Dict[str, Any]) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=cfg["host"],
            port=int(cfg["port"]),
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    @staticmethod
    def _list_stock_tables(conn: pymysql.connections.Connection, db_name: str) -> List[str]:
        sql = (
            "SELECT table_name FROM information_schema.columns "
            "WHERE table_schema=%s AND column_name='stock_code'"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (db_name,))
            rows = cur.fetchall() or []
        tables = []
        for r in rows:
            name = r.get("table_name") or r.get("TABLE_NAME")
            if name:
                tables.append(name)
        return tables

    @staticmethod
    def _get_table_columns(conn: pymysql.connections.Connection, table: str) -> Dict[str, str]:
        with conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM `{table}`")
            rows = cur.fetchall() or []
        return {r["Field"]: str(r.get("Type") or "") for r in rows}

    @staticmethod
    def _pick_text_columns(columns: Dict[str, str]) -> List[str]:
        picked = []
        for c in TEXT_COL_CANDIDATES:
            if c in columns and "json" not in columns.get(c, "").lower():
                picked.append(c)
        if picked:
            return picked
        out = []
        for name, typ in columns.items():
            low = name.lower()
            if low in ("id", "stock_code", "platform", "author", "url", "source"):
                continue
            if any(x in low for x in ("time", "date", "count", "id")):
                continue
            if any(t in typ.lower() for t in ("text", "char")):
                out.append(name)
        return out

    @staticmethod
    def _pick_media_columns(columns: Dict[str, str]) -> List[str]:
        out = []
        for name in columns:
            low = name.lower()
            if any(k in low for k in IMAGE_COL_KEYWORDS):
                out.append(name)
        return out

    @staticmethod
    def _pick_order_column(columns: Dict[str, str]) -> Optional[str]:
        for cand in PUBLISHED_TIME_CANDIDATES + INGEST_TIME_CANDIDATES + ["id"]:
            if cand in columns:
                return cand
        return None

    @staticmethod
    def _build_select_columns(columns: Dict[str, str], text_cols: List[str], media_cols: List[str]) -> str:
        cols = {"stock_code"}
        for cand in ["platform"] + URL_COL_CANDIDATES + AUTHOR_COL_CANDIDATES + KEYWORD_COL_CANDIDATES:
            if cand in columns:
                cols.add(cand)
        for cand in PUBLISHED_TIME_CANDIDATES + INGEST_TIME_CANDIDATES:
            if cand in columns:
                cols.add(cand)
        for cand in ENGAGEMENT_COL_CANDIDATES:
            if cand in columns:
                cols.add(cand)
        for c in text_cols + media_cols:
            if c in columns:
                cols.add(c)
        return ",".join([f"`{c}`" for c in cols])

    @staticmethod
    def _build_keyword_filter(text_cols: List[str], keywords: Sequence[str]) -> Tuple[str, List[Any]]:
        kw_list = [k for k in keywords if k][:8]
        if not kw_list or not text_cols:
            return "", []
        clauses = []
        params: List[Any] = []
        for kw in kw_list:
            for col in text_cols:
                clauses.append(f"`{col}` LIKE %s")
                params.append(f"%{kw}%")
        return "(" + " OR ".join(clauses) + ")", params

    @staticmethod
    def _build_stock_filter(columns: Dict[str, str], stock_code: str) -> Tuple[str, List[Any]]:
        if "stock_codes" in columns:
            clause = "(`stock_code`=%s OR FIND_IN_SET(%s, `stock_codes`))"
            return clause, [stock_code, stock_code]
        return "`stock_code`=%s", [stock_code]

    @staticmethod
    def _extract_first(row: Dict[str, Any], candidates: Sequence[str]) -> Any:
        for c in candidates:
            if c in row and row.get(c) not in (None, ""):
                return row.get(c)
        return None

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1_000_000_000_000:
                ts = ts / 1000.0
            try:
                return datetime.fromtimestamp(ts)
            except Exception:
                return None
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            s = s.replace("/", "-")
            try:
                return datetime.fromisoformat(s.split("+")[0])
            except Exception:
                pass
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m-%d %H:%M",
                "%H:%M",
            ):
                try:
                    dt = datetime.strptime(s, fmt)
                    if fmt == "%m-%d %H:%M":
                        dt = dt.replace(year=datetime.now().year)
                    if fmt == "%H:%M":
                        dt = datetime.combine(datetime.now().date(), dt.time())
                    return dt
                except Exception:
                    continue
        return None

    @staticmethod
    def _format_time(dt_val: Optional[datetime]) -> str:
        if not dt_val:
            return ""
        return dt_val.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _extract_media_urls(row: Dict[str, Any]) -> List[str]:
        urls: List[str] = []
        for k, v in row.items():
            if not any(x in k.lower() for x in IMAGE_COL_KEYWORDS):
                continue
            urls.extend(MessageAnalysisAgent._urls_from_value(v))
        if "content" in row:
            urls.extend(MessageAnalysisAgent._urls_from_text(row.get("content")))
        if "desc" in row:
            urls.extend(MessageAnalysisAgent._urls_from_text(row.get("desc")))
        return MessageAnalysisAgent._dedupe_urls(urls)

    @staticmethod
    def _urls_from_value(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            out = []
            for it in value:
                out.extend(MessageAnalysisAgent._urls_from_value(it))
            return out
        if isinstance(value, dict):
            out = []
            for it in value.values():
                out.extend(MessageAnalysisAgent._urls_from_value(it))
            return out
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return []
            if s.startswith("[") or s.startswith("{"):
                try:
                    parsed = json.loads(s)
                    return MessageAnalysisAgent._urls_from_value(parsed)
                except Exception:
                    pass
            return MessageAnalysisAgent._urls_from_text(s)
        return []

    @staticmethod
    def _urls_from_text(text: Any) -> List[str]:
        if not isinstance(text, str):
            return []
        pattern = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)
        urls = pattern.findall(text)
        # filter image-like urls
        out = []
        for u in urls:
            low = u.lower()
            if any(low.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")):
                out.append(u)
        return out

    @staticmethod
    def _dedupe_urls(urls: Iterable[str]) -> List[str]:
        seen = set()
        out = []
        for u in urls:
            s = (u or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out
