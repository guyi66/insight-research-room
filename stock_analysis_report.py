"""
stock_analysis_report.py

生成 A 股个股分析报告（包含技术面与基本面）。

功能：
- 使用 InsightEngine.DeepSearchAgent 生成技术面 Markdown（agent.analyze_stock）
- 使用 agent.analyze_stock_fundamental_multimodal 生成基本面 Markdown + 可视化计划 + fundamentals payload
- 使用 finance_tools.fundamental_chart_renderer.render_charts 渲染基本面图（可选）
- 把技术面与基本面（图表、表格、Markdown）合并成 HTML 并写入 reports/<symbol>/report.html

这是一个尽量保守地整合现有仓库实现的可运行版本。
"""

import os
import re
import math
import html as _html
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from pathlib import Path
import json
try:
    import markdown
except Exception:
    markdown = None


# 新增：简单使用 matplotlib 绘制关键指标折线图，作为 render_charts 的回退
from typing import Dict, Any, List, Tuple
from InsightEngine.agent import DeepSearchAgent, create_agent
from finance_tools.aitrados_client import FinanceMCPClient
from message_analysis_agent import MessageAnalysisAgent

# 尝试导入基本面渲染器（若不可用则回退）
try:
    from finance_tools.fundamental_chart_renderer import (
        render_charts,
    )
    HAS_FUNDAMENTAL_RENDERER = True
except Exception:
    render_charts = None  # type: ignore
    HAS_FUNDAMENTAL_RENDERER = False
import pandas as pd

def index_summary(df: "pd.DataFrame", date_col="date", close_col="close") -> Dict[str, Any]:
    if df is None or len(df) < 2:
        return {"ok": False, "reason": "no_data_or_too_short"}
    if date_col not in df.columns or close_col not in df.columns:
        return {"ok": False, "reason": "missing_columns"}
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col, close_col]).sort_values(date_col)
    if len(d) < 2:
        return {"ok": False, "reason": "no_valid_rows"}
    start = d.iloc[0]
    end = d.iloc[-1]
    ret = (float(end[close_col]) / float(start[close_col]) - 1.0)
    return {
        "ok": True,
        "start_date": start[date_col].date().isoformat(),
        "end_date": end[date_col].date().isoformat(),
        "points": int(len(d)),
        "start_close": float(start[close_col]),
        "end_close": float(end[close_col]),
        "return_pct": ret * 100.0
    }

def plot_index_line(df: "pd.DataFrame", title: str, out_path: str, date_col="date", close_col="close") -> Optional[str]:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return None
    if df is None or len(df) < 2:
        return None
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col, close_col]).sort_values(date_col)
    if len(d) < 2:
        return None
    plt.figure(figsize=(10, 4))
    plt.plot(d[date_col], d[close_col])
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Close")
    plt.tight_layout()
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=140)
    plt.close()
    return out_path
def ohlc_ctx_to_df(ctx: Any) -> "pd.DataFrame":
    """
    兼容 FinanceMCPClient.get_ohlc 返回：
    - {"data":[{date,open,high,low,close,volume}, ...], ...}
    - 或直接是 list[dict]
    输出统一列名：date/open/high/low/close/volume（缺的列允许为空）
    """
    if ctx is None:
        return pd.DataFrame()
    data = None
    if isinstance(ctx, dict):
        data = ctx.get("data", [])
    elif isinstance(ctx, list):
        data = ctx
    else:
        data = []
    df = pd.DataFrame(data or [])
    if df.empty:
        return df

    # 规范列名（尽量兼容不同来源）
    rename_map = {}
    for cand in ["日期", "trade_date", "datetime", "time"]:
        if cand in df.columns and "date" not in df.columns:
            rename_map[cand] = "date"
            break
    for cand in ["开盘", "open_price"]:
        if cand in df.columns and "open" not in df.columns:
            rename_map[cand] = "open"
            break
    for cand in ["最高", "high_price"]:
        if cand in df.columns and "high" not in df.columns:
            rename_map[cand] = "high"
            break
    for cand in ["最低", "low_price"]:
        if cand in df.columns and "low" not in df.columns:
            rename_map[cand] = "low"
            break
    for cand in ["收盘", "close_price"]:
        if cand in df.columns and "close" not in df.columns:
            rename_map[cand] = "close"
            break
    for cand in ["成交量", "vol", "volume"]:
        if cand in df.columns and "volume" not in df.columns:
            rename_map[cand] = "volume"
            break

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _draw_candles_on_ax(ax, df: "pd.DataFrame", title: str,
                        date_col="date", o="open", h="high", l="low", c="close"):
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    for col in (o, h, l, c):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d.dropna(subset=[date_col, o, h, l, c]).sort_values(date_col)
    if len(d) < 2:
        ax.text(0.5, 0.5, "无有效OHLC数据", ha="center", va="center")
        ax.set_title(title)
        return

    # x 轴用序号，避免 matplotlib 把日期当 categorical 乱跳
    xs = list(range(len(d)))
    for i in range(len(d)):
        op = float(d.iloc[i][o])
        hi = float(d.iloc[i][h])
        lo = float(d.iloc[i][l])
        cl = float(d.iloc[i][c])

        ax.vlines(i, lo, hi, linewidth=1)

        body_low = min(op, cl)
        body_h = max(abs(cl - op), 1e-9)
        ax.add_patch(Rectangle((i - 0.3, body_low), 0.6, body_h, fill=False, linewidth=1))

    step = max(len(d) // 10, 1)
    xt = list(range(0, len(d), step))
    xl = [d.iloc[i][date_col].strftime("%Y-%m-%d") for i in xt]
    ax.set_xticks(xt)
    ax.set_xticklabels(xl, rotation=45, ha="right")

    ax.set_title(title)
    ax.set_ylabel("Price")


def plot_kline_compare(
    top_df: "pd.DataFrame",
    bottom_df: "pd.DataFrame",
    top_title: str,
    bottom_title: str,
    out_path: str,
    date_col="date",
) -> Optional[str]:
    """
    生成“上下对比K线图”：
    上：指数K线
    下：个股K线
    """
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return None

    if top_df is None or bottom_df is None:
        return None

    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    _draw_candles_on_ax(axes[0], top_df, top_title, date_col=date_col)
    _draw_candles_on_ax(axes[1], bottom_df, bottom_title, date_col=date_col)
    plt.tight_layout()

    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


# ===============================
# 数值解析工具（必须在绘图函数之前）
# ===============================

_NUM_UNIT = {
    "万": 1e4,
    "亿": 1e8,
    "千": 1e3,
    "百": 1e2,
}

def parse_number(v):
    """
    将常见财务字符串转换为 float:
    - "12.3%" -> 0.123
    - "3.4亿" -> 3.4e8
    - "1,234.5" -> 1234.5
    - "—"/"-"/""/None -> None
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if s == "" or s in {"—", "-", "None", "nan", "NaN"}:
        return None

    s = s.replace(",", "").replace(" ", "")

    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except:
            return None

    unit = s[-1]
    if unit in _NUM_UNIT:
        try:
            return float(s[:-1]) * _NUM_UNIT[unit]
        except:
            return None

    try:
        return float(s)
    except:
        return None

def unwrap_fundamentals_payload(raw: Any) -> Dict[str, Any]:
    """
    兼容两种形态：
    A) {"symbol": "...", "fundamentals": {profit:[...], ...}}
    B) {profit:[...], growth:[...], ... , "source":"baostock"}
    """
    if not isinstance(raw, dict):
        return {}

    if isinstance(raw.get("fundamentals"), dict):
        return raw["fundamentals"]

    # 形态 B：只保留 list 模块，避免把 source/metadata 混进来
    modules = {}
    for k, v in raw.items():
        if isinstance(v, list):
            modules[k] = v
    return modules

def _sanitize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_json(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "tolist"):
        return _sanitize_json(value.tolist())
    if hasattr(value, "item"):
        try:
            return _sanitize_json(value.item())
        except Exception:
            pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value

def dump_json(path: str, obj):
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    safe_obj = _sanitize_json(obj)
    payload = json.dumps(safe_obj, ensure_ascii=False, indent=2)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)
    os.replace(tmp_path, path)

def verify_chart_files(charts):
    """charts: list[dict] with keys like {'path': '...'}"""
    results = []
    for c in charts or []:
        p = c.get("path") or c.get("file") or c.get("filepath")
        exists = os.path.exists(p) if p else False
        results.append({"path": p, "exists": exists, "meta": {k:v for k,v in c.items() if k!="path"}})
    return results


def _format_inline_md(text: str) -> str:
    if text is None:
        return ""
    code_spans: List[str] = []

    def _code_repl(match: "re.Match[str]") -> str:
        code_spans.append(_html.escape(match.group(1)))
        return f"{{{{CODE{len(code_spans) - 1}}}}}"

    raw = re.sub(r"`([^`]+)`", _code_repl, str(text))
    escaped = _html.escape(raw)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\*(.+?)\*", r"<em>\1</em>", escaped)

    for idx, code in enumerate(code_spans):
        escaped = escaped.replace(f"{{{{CODE{idx}}}}}", f"<code>{code}</code>")
    return escaped


def _split_table_row(line: str) -> List[str]:
    parts = [p.strip() for p in line.strip().strip("|").split("|")]
    return parts


def _is_table_sep(line: str) -> bool:
    stripped = line.strip()
    if "|" not in stripped:
        return False
    cleaned = stripped.replace("|", "").replace(":", "").replace(" ", "")
    if not cleaned:
        return False
    return all(ch == "-" for ch in cleaned)


def _render_table(headers: List[str], rows: List[List[str]]) -> str:
    max_cols = max(len(headers), max((len(r) for r in rows), default=0))
    headers = headers + [""] * (max_cols - len(headers))

    out = ["<table>", "<thead><tr>"]
    for h in headers:
        out.append(f"<th>{_format_inline_md(h)}</th>")
    out.append("</tr></thead><tbody>")
    for row in rows:
        row = row + [""] * (max_cols - len(row))
        out.append("<tr>")
        for cell in row:
            out.append(f"<td>{_format_inline_md(cell)}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def _basic_markdown_to_html(md: str) -> str:
    if not md:
        return ""
    text = md.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    out: List[str] = []
    para_lines: List[str] = []
    list_stack: List[str] = []
    in_code_block = False
    code_lines: List[str] = []
    in_html_block = False
    html_block_end = ""

    def _flush_para() -> None:
        if para_lines:
            joined = " ".join(para_lines).strip()
            if joined:
                out.append(f"<p>{_format_inline_md(joined)}</p>")
            para_lines.clear()

    def _close_lists() -> None:
        while list_stack:
            out.append(f"</{list_stack.pop()}>")

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if in_html_block:
            out.append(line)
            if html_block_end and stripped.lower().startswith(html_block_end):
                in_html_block = False
                html_block_end = ""
            i += 1
            continue

        if in_code_block:
            if stripped.startswith("```"):
                out.append("<pre><code>" + _html.escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code_block = False
            else:
                code_lines.append(line)
            i += 1
            continue

        if stripped.startswith("```"):
            _flush_para()
            _close_lists()
            in_code_block = True
            code_lines = []
            i += 1
            continue

        if stripped.lower().startswith("<pre"):
            _flush_para()
            _close_lists()
            out.append(line)
            in_html_block = True
            html_block_end = "</pre>"
            i += 1
            continue

        if stripped.startswith("<"):
            _flush_para()
            _close_lists()
            out.append(line)
            i += 1
            continue

        if "|" in line and i + 1 < len(lines) and _is_table_sep(lines[i + 1]):
            _flush_para()
            _close_lists()
            headers = _split_table_row(line)
            i += 2
            rows: List[List[str]] = []
            while i < len(lines) and "|" in lines[i] and lines[i].strip():
                rows.append(_split_table_row(lines[i]))
                i += 1
            out.append(_render_table(headers, rows))
            continue

        heading_match = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading_match:
            _flush_para()
            _close_lists()
            level = len(heading_match.group(1))
            content = _format_inline_md(heading_match.group(2).strip())
            out.append(f"<h{level}>{content}</h{level}>")
            i += 1
            continue

        if re.match(r"^[-*_]{3,}$", stripped):
            _flush_para()
            _close_lists()
            out.append("<hr/>")
            i += 1
            continue

        list_match = re.match(r"^\s*([*+-])\s+(.*)$", line)
        ordered_match = re.match(r"^\s*(\d+)\.\s+(.*)$", line)
        if list_match or ordered_match:
            list_type = "ul" if list_match else "ol"
            if not list_stack or list_stack[-1] != list_type:
                _flush_para()
                if list_stack and list_stack[-1] != list_type:
                    out.append(f"</{list_stack.pop()}>")
                out.append(f"<{list_type}>")
                list_stack.append(list_type)
            content = (list_match.group(2) if list_match else ordered_match.group(2)).strip()
            out.append(f"<li>{_format_inline_md(content)}</li>")
            i += 1
            continue

        if stripped.startswith(">"):
            _flush_para()
            _close_lists()
            quote = stripped.lstrip(">").strip()
            out.append(f"<blockquote>{_format_inline_md(quote)}</blockquote>")
            i += 1
            continue

        if not stripped:
            _flush_para()
            _close_lists()
            i += 1
            continue

        para_lines.append(stripped)
        i += 1

    if in_code_block:
        out.append("<pre><code>" + _html.escape("\n".join(code_lines)) + "</code></pre>")

    _flush_para()
    _close_lists()
    return "\n".join(out)


def markdown_to_html(md: str) -> str:
    """将 Markdown 转为 HTML，若没有 markdown 库则做简单转义并包裹在 <pre> 中"""
    if not md:
        return ""
    if markdown:
        try:
            return markdown.markdown(md, extensions=["extra", "tables", "fenced_code"])  # type: ignore
        except Exception:
            pass
    # fallback
    return _basic_markdown_to_html(md)


def build_message_inputs_html(
    keywords: List[str],
    prompt_materials: Any,
    prompt_web_materials: Any,
) -> str:
    items = []
    web_items = []
    materials_text = ""
    web_text = ""

    if prompt_materials:
        if isinstance(prompt_materials, str):
            materials_text = prompt_materials
        elif isinstance(prompt_materials, (list, tuple)):
            items = list(prompt_materials)
        elif isinstance(prompt_materials, dict):
            items = [prompt_materials]
        else:
            items = list(prompt_materials) if hasattr(prompt_materials, "__iter__") else []

    if prompt_web_materials:
        if isinstance(prompt_web_materials, str):
            web_text = prompt_web_materials
        elif isinstance(prompt_web_materials, (list, tuple)):
            web_items = list(prompt_web_materials)
        elif isinstance(prompt_web_materials, dict):
            web_items = [prompt_web_materials]
        else:
            web_items = list(prompt_web_materials) if hasattr(prompt_web_materials, "__iter__") else []

    if not keywords and not items and not web_items and not materials_text and not web_text:
        return ""

    def _item_value(item: Any, key: str) -> Any:
        if isinstance(item, dict):
            return item.get(key, "")
        return getattr(item, key, "")

    parts = [
        "<div class='section'>",
        "<h2>消息面分析输入材料（LLM 输入）</h2>",
    ]
    kw_list = [k for k in (keywords or []) if k]
    if kw_list:
        kw_text = _html.escape(", ".join(kw_list))
        parts.append(f"<p><strong>关键词：</strong>{kw_text}</p>")

    if items:
        parts.append("<h3>DB Materials</h3>")
        parts.append("<table>")
        parts.append(
            "<thead><tr>"
            "<th>#</th><th>平台</th><th>发布时间</th><th>标题</th><th>摘要</th>"
            "<th>URL</th><th>匹配</th><th>互动</th><th>关键词</th>"
            "<th>来源表</th><th>时间来源</th><th>媒体</th>"
            "</tr></thead><tbody>"
        )
        for idx, item in enumerate(items, 1):
            platform = _html.escape(str(_item_value(item, "platform") or ""))
            published = _html.escape(str(_item_value(item, "published_at") or ""))
            title = _html.escape(str(_item_value(item, "title") or ""))
            snippet = _clip_text(str(_item_value(item, "content") or ""), 220)
            snippet = _html.escape(snippet)
            url_raw = str(_item_value(item, "url") or "")
            url_text = _html.escape(url_raw)
            url_html = f"<a href=\"{url_text}\" target=\"_blank\">{url_text}</a>" if url_raw else ""
            match_count = _html.escape(str(_item_value(item, "match_count") or ""))
            engagement = _html.escape(str(_item_value(item, "engagement") or ""))
            keyword = _html.escape(str(_item_value(item, "keyword") or ""))
            source_table = _html.escape(str(_item_value(item, "source_table") or ""))
            time_source = _html.escape(str(_item_value(item, "time_source") or ""))
            media_val = _item_value(item, "media_urls")
            if isinstance(media_val, (list, tuple)):
                media_text = ", ".join([str(m) for m in media_val if m])
            else:
                media_text = str(media_val or "")
            media_text = _html.escape(media_text)

            parts.append(
                "<tr>"
                f"<td>{idx}</td>"
                f"<td>{platform}</td>"
                f"<td>{published}</td>"
                f"<td>{title}</td>"
                f"<td>{snippet}</td>"
                f"<td>{url_html}</td>"
                f"<td>{match_count}</td>"
                f"<td>{engagement}</td>"
                f"<td>{keyword}</td>"
                f"<td>{source_table}</td>"
                f"<td>{time_source}</td>"
                f"<td>{media_text}</td>"
                "</tr>"
            )
        parts.append("</tbody></table>")
    elif materials_text:
        parts.append("<h3>DB Materials</h3>")
        parts.append("<pre style='white-space:pre-wrap;'>")
        parts.append(_html.escape(materials_text))
        parts.append("</pre>")

    if web_items:
        parts.append("<h3>Web Materials</h3>")
        parts.append("<table>")
        parts.append("<thead><tr><th>#</th><th>标题</th><th>摘要</th><th>URL</th></tr></thead><tbody>")
        for idx, item in enumerate(web_items, 1):
            title = _html.escape(str((item or {}).get("title") or ""))
            snippet = _html.escape(str((item or {}).get("snippet") or ""))
            url_raw = str((item or {}).get("url") or "")
            url_text = _html.escape(url_raw)
            url_html = f"<a href=\"{url_text}\" target=\"_blank\">{url_text}</a>" if url_raw else ""
            parts.append(f"<tr><td>{idx}</td><td>{title}</td><td>{snippet}</td><td>{url_html}</td></tr>")
        parts.append("</tbody></table>")
    elif web_text:
        parts.append("<h3>Web Materials</h3>")
        parts.append("<pre style='white-space:pre-wrap;'>")
        parts.append(_html.escape(web_text))
        parts.append("</pre>")

    parts.append("</div>")
    return "\n".join(parts)


def _clip_text(text: str, max_len: int) -> str:
    if not text:
        return ""
    s = str(text)
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "..."


def _extract_stock_name(profile: Dict[str, Any]) -> str:
    if not isinstance(profile, dict):
        return ""
    for key in ("code_name", "name", "shortName", "short_name", "company_name", "stock_name"):
        val = profile.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def build_combined_summary_md(
    symbol: str,
    tech_md: str,
    fundamental_md: str,
    news_md: str,
    llm_client: Any,
    max_len: int = 12000,
    evidence_text: str = "",
    critic_md: str = "",
) -> str:
    if not llm_client:
        return "综合报告未生成（LLM不可用），请参考各分项分析。"

    system_prompt = (
        "你是一名资深权益研究分析师。你将同时收到同一只股票的【技术面分析、】【基本面分析】与【消息/新闻面分析】输入。"
        "你的任务是将三者整合为一份逻辑自洽、可执行的综合研判报告。\n\n"

        "【核心要求：逻辑闭环与相互成立】\n"
        "1) 必须显式进行“交叉验证”：指出技术/基本面/消息面结论是否一致；若存在冲突，必须解释冲突来源（时间尺度不同/预期差/信息不完备/市场定价领先等），并给出你采用的仲裁原则（以哪类信号为主、为何）。\n"
        "2) 结论必须由证据支撑：每条关键判断要对应到输入材料中的要点（用括号注明来源：技术面/基本面/消息面），不得引入外部事实或补充数据；若材料不足，必须说明“缺口是什么”，并降低置信度。\n"
        "3) 输出必须给出明确的【投资建议】仅可选：买入 / 维持 / 卖出，并给出："
        "   - 建议的适用期限（短期：1-5个交易日 / 中期：1-3个月 / 长期：3个月以上，三选一或多选但要清晰）；\n"
        "   - 触发条件（什么情况下建议会改变：例如关键价位突破/跌破、业绩验证、风险事件落地等）；\n"
        "   - 置信度（高/中/低），并说明置信度来自“一致性”和“证据充分性”。\n"
        "4) 表达简洁：不重复粘贴长段落，不做空洞口号；以要点化句子呈现，每点尽量一句话讲清“观点+理由”。\n\n"

        "【输出格式：必须严格使用以下章节】\n"
        "## 综合结论\n"
        "- 投资建议：买入/维持/卖出（适用期限：…；置信度：高/中/低）\n"
        "- 核心逻辑链：用2-4条要点串起“消息/基本面 → 预期/盈利 → 资金/技术形态 → 价格行为”的闭环\n"
        "- 一致性检查：三类信号一致/部分一致/冲突（简述冲突点与仲裁原则）\n"
        "- 关键触发条件：列出1-3条，说明触发后建议如何调整\n\n"

        "## 技术面要点\n"
        "- 趋势/形态/关键价位/量能（只保留最影响交易决策的3-5点）\n"
        "- 技术面对建议的含义：支持买入/支持卖出/仅提示观望（给出原因）\n\n"

        "## 基本面要点\n"
        "- 业绩与经营质量（增长、盈利能力、现金流/负债、竞争地位等，保留3-5点）\n"
        "- 基本面对建议的含义：估值扩张/盈利改善/基本面承压/不确定（给出原因）\n\n"

        "## 消息面要点\n"
        "- 事件归纳（1-3条即可）与情绪方向（正/中/负/混合）\n"
        "- 影响路径与时间尺度：短期冲击/中期验证/长期结构性（明确哪一个为主）\n"
        "- 消息面对建议的含义：催化剂/风险源/噪声（给出原因）\n\n"

        "## 风险提示\n"
        "- 列出3-6条最重要风险（按优先级排序），并说明其可能如何改变结论\n"
        "- 若存在信息缺口，必须在此列明“还需要哪些信息/数据来验证”\n"
    )
    system_prompt += "5) When stating conclusions, cite evidence ids inline (e.g., Evidence: E0001, E0012). Only cite provided evidence.\n\n"

    user_prompt = (
        f"Stock: {symbol}\n\n"
        "Technical analysis (excerpt):\n"
        f"{_clip_text(tech_md, max_len)}\n\n"
        "Fundamental analysis (excerpt):\n"
        f"{_clip_text(fundamental_md, max_len)}\n\n"
        "Message/news analysis (excerpt):\n"
        f"{_clip_text(news_md, max_len)}\n\n"
        "Please generate the combined report now."
    )

    if evidence_text:
        user_prompt += "\n\nEvidence index:\n" + _clip_text(evidence_text, max_len)
    if critic_md:
        user_prompt += "\n\nCritic notes:\n" + _clip_text(critic_md, max_len)

    try:
        if hasattr(llm_client, "stream_invoke_to_string"):
            return llm_client.stream_invoke_to_string(system_prompt, user_prompt, temperature=0.2)
        if hasattr(llm_client, "invoke"):
            return llm_client.invoke(system_prompt, user_prompt, temperature=0.2)
    except Exception:
        return "综合报告生成失败，请参考各分项分析。"
    return "综合报告生成失败，请参考各分项分析。"


def render_tables_html(plan: Any, fundamentals: Dict[str, Any]) -> List[str]:
    """根据 FundamentalVisualPlan 中的 tables 生成 HTML 表格字符串列表。
    这个函数对缺失数据容错：若某个模块或字段不存在，会在表格中显示空值/说明。
    """
    tables_html: List[str] = []

    if not plan or not getattr(plan, "tables", None):
        return tables_html

    for tb in plan.tables:
        title = getattr(tb, "title", "未命名表格")
        desc = getattr(tb, "description", "")
        cols = getattr(tb, "columns", [])
        sort_by = getattr(tb, "sort_by", "statDate")
        limit = getattr(tb, "limit", 8)

        # 推断 module
        if not cols:
            continue
        first_field = cols[0].field
        module = first_field.split(".")[0]
        records = fundamentals.get(module, []) if fundamentals else []

        # 按 sort_by 排序并取最近 limit 条
        try:
            rows = []
            for item in records:
                row_date = item.get(sort_by, "")
                row_vals = [item.get(c.field.split(".")[-1], "") for c in cols]
                rows.append((row_date, row_vals))
            rows = sorted(rows, key=lambda x: str(x[0]) or "")[-limit:]
        except Exception:
            rows = []

        # 构建 HTML
        header_html = "".join(f"<th>{_html.escape(c.name)}</th>" for c in cols)
        body_lines = []
        for _, vals in rows:
            cells = "".join(f"<td>{_html.escape(str(v))}</td>" for v in vals)
            body_lines.append(f"<tr>{cells}</tr>")
        if not body_lines:
            body_html = "<tr><td colspan='{}'>无数据</td></tr>".format(len(cols))
        else:
            body_html = "\n".join(body_lines)

        table_html = f"""
        <div class='fund-table'>
          <h4>{_html.escape(title)}</h4>
          <p>{_html.escape(desc)}</p>
          <table>
            <thead><tr>{header_html}</tr></thead>
            <tbody>
            {body_html}
            </tbody>
          </table>
        </div>
        """
        tables_html.append(table_html)

    return tables_html


# 新增：如果 agent 没有返回可视化计划，或者计划里没有 tables，则从原始 fundamentals payload 中生成 HTML 表格
def render_tables_from_payload(fundamentals: Dict[str, Any], recent_n: int = 4) -> List[str]:
    """从 fundamentals payload（dict）创建若干常用模块的 HTML 表格作为回退。
    fundamentals 的结构通常为：{ module_name: [ {statDate: '2025-09-30', field1: val, ...}, ... ], ... }
    """
    if not fundamentals:
        return []

    def build_table(module: str, display_cols: List[tuple]) -> str:
        records = fundamentals.get(module, [])
        if not records:
            cols_html = "".join(f"<th>{_html.escape(name)}</th>" for name, _ in display_cols)
            return f"<div class='fund-table'><h4>{_html.escape(module)}</h4><p>无数据</p><table><thead><tr>{cols_html}</tr></thead><tbody><tr><td colspan='{len(display_cols)}'>无数据</td></tr></tbody></table></div>"
        # sort by statDate
        try:
            rows = sorted(records, key=lambda x: str(x.get('statDate','')))  # oldest -> newest
            rows = rows[-recent_n:]
        except Exception:
            rows = records[-recent_n:]

        header_html = "".join(f"<th>{_html.escape(name)}</th>" for name, _ in display_cols)
        body_lines = []
        for r in rows:
            cells = []
            for name, field in display_cols:
                v = r.get(field, "")
                cells.append(f"<td>{_html.escape(str(v))}</td>")
            body_lines.append("<tr>" + "".join(cells) + "</tr>")
        body_html = "\n".join(body_lines)
        return f"<div class='fund-table'><h4>{_html.escape(module)}</h4><p>最近{len(rows)}期</p><table><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table></div>"

    def build_peer_table(module: str, title: str, display_cols: List[tuple], desc: str = "同行比较") -> str:
        records = fundamentals.get(module, [])
        cols_html = "".join(f"<th>{_html.escape(name)}</th>" for name, _ in display_cols)
        if not records:
            return (
                f"<div class='fund-table'><h4>{_html.escape(title)}</h4>"
                f"<p>无数据</p><table><thead><tr>{cols_html}</tr></thead>"
                f"<tbody><tr><td colspan='{len(display_cols)}'>无数据</td></tr></tbody>"
                f"</table></div>"
            )

        header_html = "".join(f"<th>{_html.escape(name)}</th>" for name, _ in display_cols)
        body_lines = []
        for r in records:
            cells = []
            for name, field in display_cols:
                v = r.get(field, "")
                cells.append(f"<td>{_html.escape(str(v))}</td>")
            body_lines.append("<tr>" + "".join(cells) + "</tr>")
        body_html = "\n".join(body_lines) if body_lines else (
            f"<tr><td colspan='{len(display_cols)}'>无数据</td></tr>"
        )
        return (
            f"<div class='fund-table'><h4>{_html.escape(title)}</h4>"
            f"<p>{_html.escape(desc)}</p><table><thead><tr>{header_html}</tr></thead>"
            f"<tbody>{body_html}</tbody></table></div>"
        )

    tables = []
    # 盈利能力 & 成长
    tables.append(build_table('profit', [ ('统计期','statDate'), ('ROE(%)','roeAvg'), ('净利率(%)','npMargin'), ('净利润(元)','netProfit'), ('净利润同比(%)','YOYNI') ]))
    # 偿债能力
    tables.append(build_table('balance', [ ('统计期','statDate'), ('资产负债率(%)','liabilityToAsset_pct'), ('流动比率','currentRatio'), ('速动比率','quickRatio'), ('现金比率','cashRatio') ]))
    # 现金流
    tables.append(build_table('cashflow', [ ('统计期','statDate'), ('经营现金流(元)','CFO'), ('CFO/净利润','CFOToNP'), ('CFO/营业收','CFOToOR') ]))
    # 运营能力
    tables.append(build_table('operation', [ ('统计期','statDate'), ('存货周转天数','INVTurnDays'), ('应收账款周转天数','NRTurnDays'), ('总资产周转率','AssetTurnRatio') ]))
    # 杜邦
    tables.append(build_table('dupont', [ ('统计期','statDate'), ('dupontROE','dupontROE'), ('净利率','dupontNitogr'), ('资产周转率','dupontAssetTurn') ]))

    tables.append(build_peer_table(
        "peer_growth_comparison",
        "同行比较-成长性",
        [
            ("代码", "代码"),
            ("简称", "简称"),
            ("EPS-3年复合", "基本每股收益增长率-3年复合"),
            ("营收-3年复合", "营业收入增长率-3年复合"),
            ("净利-3年复合", "净利润增长率-3年复合"),
            ("EPS复合排名", "基本每股收益增长率-3年复合排名"),
        ],
    ))
    tables.append(build_peer_table(
        "peer_valuation_comparison",
        "同行比较-估值",
        [
            ("代码", "代码"),
            ("简称", "简称"),
            ("PEG", "PEG"),
            ("PE(TTM)", "市盈率-TTM"),
            ("PB(MRQ)", "市净率-MRQ"),
            ("PEG排名", "PEG排名"),
        ],
    ))
    tables.append(build_peer_table(
        "peer_dupont_comparison",
        "同行比较-杜邦",
        [
            ("代码", "代码"),
            ("简称", "简称"),
            ("ROE-3年平均", "ROE-3年平均"),
            ("净利率-3年平均", "净利率-3年平均"),
            ("总资产周转率-3年平均", "总资产周转率-3年平均"),
            ("权益乘数-3年平均", "权益乘数-3年平均"),
            ("ROE排名", "ROE-3年平均排名"),
        ],
    ))
    tables.append(build_peer_table(
        "balance_sheet_em",
        "资产负债表（AkShare）",
        [
            ("报告期", "报告期"),
            ("资产-总资产", "资产-总资产"),
            ("负债-总负债", "负债-总负债"),
            ("资产负债率", "资产负债率"),
            ("股东权益合计", "股东权益合计"),
            ("公告日期", "公告日期"),
        ],
        "多期数据，仅本股票",
    ))
    tables.append(build_peer_table(
        "income_statement_em",
        "利润表（AkShare）",
        [
            ("报告期", "报告期"),
            ("营业总收入", "营业总收入"),
            ("营业利润", "营业利润"),
            ("利润总额", "利润总额"),
            ("净利润", "净利润"),
            ("每股收益", "每股收益"),
            ("公告日期", "公告日期"),
        ],
        "多期数据，仅本股票",
    ))
    tables.append(build_peer_table(
        "cashflow_statement_em",
        "现金流量表（AkShare）",
        [
            ("报告期", "报告期"),
            ("经营活动产生的现金流量净额", "经营活动产生的现金流量净额"),
            ("投资活动产生的现金流量净额", "投资活动产生的现金流量净额"),
            ("筹资活动产生的现金流量净额", "筹资活动产生的现金流量净额"),
            ("现金及现金等价物净增加额", "现金及现金等价物净增加额"),
            ("公告日期", "公告日期"),
        ],
        "多期数据，仅本股票",
    ))

    return tables


# 新增：生成一个简单的基础文本分析（markdown）作为回退，覆盖在 agent 未生成文本时使用
def generate_basic_fundamental_md(fundamentals: Dict[str, Any], symbol: str) -> str:
    """从 fundamentals payload 中抽取关键趋势并生成简要的 Markdown 分析文本。"""
    if not fundamentals:
        return "**未能获取到基本面数据。**"

    def latest(module: str, field: str):
        recs = fundamentals.get(module, [])
        if not recs:
            return None
        try:
            recs_sorted = sorted(recs, key=lambda x: str(x.get('statDate','')))
            return recs_sorted[-1].get(field)
        except Exception:
            return recs[-1].get(field)

    parts = []
    parts.append(f"### 基本面数据概览（回退生成）\n以下为从可用原始财务数据提取的要点，可作为补充信息。")

    # 盈利
    roe = latest('profit', 'roeAvg')
    npmargin = latest('profit', 'npMargin')
    yoyni = latest('profit', 'YOYNI')
    if roe is not None or npmargin is not None or yoyni is not None:
        parts.append("#### 盈利能力 & 成长性")
        if roe is not None:
            parts.append(f"- 最新季度 ROE(平均)：{roe}")
        if npmargin is not None:
            parts.append(f"- 最新季度 净利率：{npmargin}")
        if yoyni is not None:
            parts.append(f"- 最新季度 净利润同比：{yoyni}")

    # 偿债
    debt = latest('balance', 'liabilityToAsset_pct')
    curr = latest('balance', 'currentRatio')
    if debt is not None or curr is not None:
        parts.append("#### 偿债能力与流动性")
        if debt is not None:
            parts.append(f"- 资产负债率：{debt}")
        if curr is not None:
            parts.append(f"- 流动比率：{curr}")

    # 现金流
    cf_np = latest('cashflow', 'CFOToNP')
    if cf_np is not None:
        parts.append("#### 现金流")
        parts.append(f"- 经营活动现金流 / 净利润：{cf_np}")

    # 杜邦
    dupont_roe = latest('dupont', 'dupontROE')
    if dupont_roe is not None:
        parts.append("#### 杜邦分析")
        parts.append(f"- 杜邦分解 ROE：{dupont_roe}")

    def _fmt_peer_value(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return None
        return s

    def _match_code(value: Any, code_6: str) -> bool:
        if not value or not code_6:
            return False
        s = str(value)
        if code_6 in s:
            return True
        digits = re.sub(r"\\D", "", s)
        return digits == code_6

    def _pick_row(records: Any, code_6: str) -> Optional[Dict[str, Any]]:
        if not isinstance(records, list) or not code_6:
            return None
        for r in records:
            if _match_code(r.get("代码", ""), code_6):
                return r
        return None

    def _pick_label(records: Any, label: str) -> Optional[Dict[str, Any]]:
        if not isinstance(records, list):
            return None
        for r in records:
            code = str(r.get("代码", ""))
            name = str(r.get("简称", ""))
            if code == label or name == label:
                return r
        return None

    def _build_peer_line(prefix: str, row: Optional[Dict[str, Any]], fields: List[tuple]) -> Optional[str]:
        if not row:
            return None
        items = []
        for label, key in fields:
            val = _fmt_peer_value(row.get(key))
            if val is not None:
                items.append(f"{label}{val}")
        if not items:
            return None
        return f"{prefix}：" + "，".join(items)

    def _latest_em(records: Any, date_fields: List[str]) -> Optional[Dict[str, Any]]:
        if not isinstance(records, list) or not records:
            return None
        def _key(row: Dict[str, Any]) -> str:
            for field in date_fields:
                val = row.get(field)
                if val is not None:
                    return str(val)
            return ""
        rows = sorted(records, key=_key)
        return rows[-1] if rows else None

    balance_em = fundamentals.get("balance_sheet_em", [])
    income_em = fundamentals.get("income_statement_em", [])
    cashflow_em = fundamentals.get("cashflow_statement_em", [])
    perf_report_em = fundamentals.get("performance_report_em", [])
    perf_express_em = fundamentals.get("performance_express_em", [])

    if (not fundamentals.get("balance")) and balance_em:
        row = _latest_em(balance_em, ["报告期", "公告日期"])
        if row:
            parts.append("#### AkShare 资产负债表补充")
            summary = []
            for label, key in (
                ("报告期", "报告期"),
                ("总资产", "资产-总资产"),
                ("总负债", "负债-总负债"),
                ("资产负债率", "资产负债率"),
                ("股东权益", "股东权益合计"),
            ):
                val = _fmt_peer_value(row.get(key))
                if val is not None:
                    summary.append(f"{label}：{val}")
            if summary:
                parts.append("- " + "，".join(summary))

    if (not fundamentals.get("profit")) and income_em:
        row = _latest_em(income_em, ["报告期", "公告日期"])
        if row:
            parts.append("#### AkShare 利润表补充")
            summary = []
            for label, key in (
                ("报告期", "报告期"),
                ("营业总收入", "营业总收入"),
                ("营收同比", "营业总收入同比"),
                ("净利润", "净利润"),
                ("净利润同比", "净利润同比"),
                ("营业利润", "营业利润"),
            ):
                val = _fmt_peer_value(row.get(key))
                if val is not None:
                    summary.append(f"{label}：{val}")
            if summary:
                parts.append("- " + "，".join(summary))

    if (not fundamentals.get("cashflow")) and cashflow_em:
        row = _latest_em(cashflow_em, ["报告期", "公告日期"])
        if row:
            parts.append("#### AkShare 现金流量表补充")
            summary = []
            for label, key in (
                ("报告期", "报告期"),
                ("净现金流", "净现金流-净现金流"),
                ("经营性现金流", "经营性现金流-现金流量净额"),
                ("投资性现金流", "投资性现金流-现金流量净额"),
                ("融资性现金流", "融资性现金流-现金流量净额"),
            ):
                val = _fmt_peer_value(row.get(key))
                if val is not None:
                    summary.append(f"{label}：{val}")
            if summary:
                parts.append("- " + "，".join(summary))

    if perf_report_em and not fundamentals.get("profit"):
        row = _latest_em(perf_report_em, ["报告期", "最新公告日期", "公告日期"])
        if row:
            parts.append("#### AkShare 业绩报表补充")
            summary = []
            for label, key in (
                ("报告期", "报告期"),
                ("每股收益", "每股收益"),
                ("净资产收益率", "净资产收益率"),
                ("销售毛利率", "销售毛利率"),
                ("所处行业", "所处行业"),
                ("最新公告日期", "最新公告日期"),
            ):
                val = _fmt_peer_value(row.get(key))
                if val is not None:
                    summary.append(f"{label}：{val}")
            if summary:
                parts.append("- " + "，".join(summary))

    if perf_express_em and not fundamentals.get("performance_express"):
        row = _latest_em(perf_express_em, ["报告期", "公告日期"])
        if row:
            parts.append("#### AkShare 业绩快报补充")
            summary = []
            for label, key in (
                ("报告期", "报告期"),
                ("营业收入同比", "营业收入-同比增长"),
                ("净利润同比", "净利润-同比增长"),
                ("每股收益", "每股收益"),
                ("净资产收益率", "净资产收益率"),
                ("公告日期", "公告日期"),
            ):
                val = _fmt_peer_value(row.get(key))
                if val is not None:
                    summary.append(f"{label}：{val}")
            if summary:
                parts.append("- " + "，".join(summary))

    code_match = re.search(r"(\\d{6})", symbol or "")
    code_6 = code_match.group(1) if code_match else ""
    peer_growth = fundamentals.get("peer_growth_comparison", [])
    peer_valuation = fundamentals.get("peer_valuation_comparison", [])
    peer_dupont = fundamentals.get("peer_dupont_comparison", [])

    if peer_growth or peer_valuation or peer_dupont:
        parts.append("#### 同行比较（东方财富）")

        growth_fields = [
            ("EPS-3年复合", "基本每股收益增长率-3年复合"),
            ("营收-3年复合", "营业收入增长率-3年复合"),
            ("净利-3年复合", "净利润增长率-3年复合"),
            ("EPS复合排名", "基本每股收益增长率-3年复合排名"),
        ]
        growth_lines = [
            _build_peer_line("公司", _pick_row(peer_growth, code_6), growth_fields),
            _build_peer_line("行业平均", _pick_label(peer_growth, "行业平均"), growth_fields),
            _build_peer_line("行业中值", _pick_label(peer_growth, "行业中值"), growth_fields),
        ]
        growth_lines = [line for line in growth_lines if line]
        if growth_lines:
            parts.append("- 成长性：" + "；".join(growth_lines))

        valuation_fields = [
            ("PEG", "PEG"),
            ("PE(TTM)", "市盈率-TTM"),
            ("PB(MRQ)", "市净率-MRQ"),
            ("PEG排名", "PEG排名"),
        ]
        valuation_lines = [
            _build_peer_line("公司", _pick_row(peer_valuation, code_6), valuation_fields),
            _build_peer_line("行业平均", _pick_label(peer_valuation, "行业平均"), valuation_fields),
            _build_peer_line("行业中值", _pick_label(peer_valuation, "行业中值"), valuation_fields),
        ]
        valuation_lines = [line for line in valuation_lines if line]
        if valuation_lines:
            parts.append("- 估值：" + "；".join(valuation_lines))

        dupont_fields = [
            ("ROE-3年平均", "ROE-3年平均"),
            ("净利率-3年平均", "净利率-3年平均"),
            ("总资产周转率-3年平均", "总资产周转率-3年平均"),
            ("权益乘数-3年平均", "权益乘数-3年平均"),
            ("ROE排名", "ROE-3年平均排名"),
        ]
        dupont_lines = [
            _build_peer_line("公司", _pick_row(peer_dupont, code_6), dupont_fields),
            _build_peer_line("行业平均", _pick_label(peer_dupont, "行业平均"), dupont_fields),
            _build_peer_line("行业中值", _pick_label(peer_dupont, "行业中值"), dupont_fields),
        ]
        dupont_lines = [line for line in dupont_lines if line]
        if dupont_lines:
            parts.append("- 杜邦：" + "；".join(dupont_lines))

    parts.append("\n> 说明：以上为自动摘要，基于原始字段直接提取，可能缺乏上下文和口径注释。若需要更深入的语义解读，请使用 agent 提供的多模态分析接口或修复数据获取问题。")

    return "\n".join(parts)


def render_simple_charts(symbol: str, fundamentals: Dict[str, Any], out_dir: str) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    debug = {"attempted_fields": [], "plotted": [], "skipped": []}
    charts: List[Dict[str, str]] = []

    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        debug["skipped"].append({"reason": "matplotlib_import_failed", "error": repr(e)})
        return charts, debug

    os.makedirs(out_dir, exist_ok=True)

    def is_nan(x: Any) -> bool:
        return isinstance(x, float) and (x != x)

    def plot_series(module: str, fields: Dict[str, str], title: str, fname: str):
        recs = fundamentals.get(module, [])
        if not recs:
            debug["skipped"].append({"module": module, "reason": "no_records"})
            return None

        try:
            rows = sorted(recs, key=lambda x: str(x.get("statDate", "")))
        except Exception:
            rows = recs

        dates = [r.get("statDate", "") for r in rows]

        plt.figure(figsize=(8, 4))
        plotted_any_field = False

        for key, label in fields.items():
            # 记录尝试的字段 & 样本
            sample_values = [r.get(key) for r in rows[:5]]
            debug["attempted_fields"].append({
                "module": module,
                "field": key,
                "label": label,
                "sample_values": sample_values
            })

            vals = []
            for r in rows:
                v = r.get(key)
                num = parse_number(v)  # 关键改动：用强健解析
                vals.append(num if num is not None else float("nan"))

            valid_count = sum(0 if is_nan(x) else 1 for x in vals)

            if valid_count < 2:
                debug["skipped"].append({
                    "module": module,
                    "field": key,
                    "label": label,
                    "reason": "not_enough_numeric_points",
                    "valid_count": valid_count
                })
                continue

            plt.plot(dates, vals, marker="o", label=label)
            plotted_any_field = True
            debug["plotted"].append({
                "module": module,
                "field": key,
                "label": label,
                "reason": "plotted"
            })

        if not plotted_any_field:
            plt.close()
            debug["skipped"].append({"module": module, "reason": "no_fields_plotted"})
            return None

        plt.xticks(rotation=30)
        plt.title(title)
        plt.legend()
        plt.tight_layout()

        path = os.path.join(out_dir, f"{symbol}_{fname}.png")
        plt.savefig(path)
        plt.close()

        debug["plotted"].append({"module": module, "figure": fname, "path": path})
        return path

    p = plot_series("profit", {"roeAvg": "ROE", "npMargin": "净利率", "netProfit": "净利润"}, "盈利能力指标", "profitability_trend")
    if p:
        charts.append({"id": "profit", "title": "盈利能力关键指标季度走势", "description": "ROE、净利率与净利润季度变化", "path": p})

    p = plot_series("growth", {"YOYNI": "净利润同比", "YOYEPSBasic": "每股收益同比"}, "成长能力指标", "growth_trend")
    if p:
        charts.append({"id": "growth", "title": "成长能力指标季度走势", "description": "净利润同比与每股收益同比季度变化", "path": p})

    p = plot_series("balance", {"currentRatio": "流动比率", "quickRatio": "速动比率", "cashRatio": "现金比率"}, "偿债能力指标", "liquidity_trend")
    if p:
        charts.append({"id": "liquidity", "title": "偿债能力指标季度走势", "description": "流动比率/速动比率/现金比率季度变化", "path": p})

    p = plot_series("dupont", {"dupontROE": "ROE(杜邦)", "dupontNitogr": "净利率", "dupontAssetTurn": "资产周转率"}, "杜邦分析关键驱动因素", "dupont_components")
    if p:
        charts.append({"id": "dupont", "title": "杜邦分析关键驱动因素", "description": "权益乘数/资产周转率/净利率驱动ROE", "path": p})

    # include available market / index or symbol monthly charts from the charts dir if present
    try:
        market_file = os.path.join(out_dir, "000001_month.png")
        if os.path.exists(market_file):
            charts.append({"id": "market_month", "title": "上证综指（月线）", "description": "大盘月线走势", "path": market_file})

        symbol_month_file = os.path.join(out_dir, f"{symbol}_month.png")
        if os.path.exists(symbol_month_file):
            charts.append({"id": f"{symbol}_month", "title": f"{symbol} 月线", "description": f"{symbol} 月线图表", "path": symbol_month_file})
    except Exception as e:
        debug["skipped"].append({"reason": "append_existing_charts_failed", "error": repr(e)})

    debug["fundamental_charts_count"] = len(charts)
    return charts, debug



def _remove_macro_section_from_html(html_str: str) -> str:
    """Remove the 2.4 宏观货币环境 section (if present) from generated HTML fragment.
    This is a simple heuristic: find the heading text and remove until the next <h2> or <h3>.
    """
    if not html_str:
        return html_str
    marker = '宏观货币环境'
    idx = html_str.find(marker)
    if idx == -1:
        return html_str
    # find start: backtrack to the opening tag
    start = html_str.rfind('<h', 0, idx)
    if start == -1:
        start = idx
    # find end: next occurrence of <h2 or <h3 or <h1> after idx
    next_h2 = html_str.find('<h2', idx)
    next_h3 = html_str.find('<h3', idx)
    next_h1 = html_str.find('<h1', idx)
    candidates = [p for p in (next_h1, next_h2, next_h3) if p != -1]
    if candidates:
        end = min(candidates)
    else:
        end = len(html_str)
    new_html = html_str[:start] + html_str[end:]
    return new_html


def build_html_report(
    symbol: str,
    tech_md: str,
    fundamental_md: str,
    fundamental_charts: Optional[List[Dict[str, str]]],
    fundamental_tables_html: Optional[List[str]],
    chart_paths: Dict[str, str],
    out_dir: str,
) -> str:
    """
    把技术面 Markdown、基本面 Markdown、基本面图表、基本面表格 与 技术面图合并为最终 HTML 报告并写入 reports/<symbol>/report.html

    返回写入的 HTML 文件路径。
    """
    analysis_html = markdown_to_html(tech_md or "")
    fund_html = markdown_to_html(fundamental_md or "")

    # remove unwanted macro section from fund_html
    fund_html = _remove_macro_section_from_html(fund_html)

    # build tech imgs html
    tech_imgs_html = ""
    for title, path in chart_paths.items():
        relpath = os.path.relpath(path, start=os.path.join(out_dir, symbol)).replace("\\", "/")
        tech_imgs_html += f"<h3>{_html.escape(title)}</h3>\n<img src=\"{relpath}\" class=\"chart\"/>\n"

    # build fundamental imgs html
    fundamental_imgs_html = ""
    if fundamental_charts:
        for ch in fundamental_charts:
            # ch has keys: id, title, description, path
            try:
                relp = os.path.relpath(ch.get("path"), start=os.path.join(out_dir, symbol)).replace("\\", "/")
            except Exception:
                relp = ch.get("path", "")
            fundamental_imgs_html += f"<h4>{_html.escape(ch.get('title',''))}</h4>\n<img src=\"{relp}\" class=\"chart\"/>\n<p>{_html.escape(ch.get('description',''))}</p>\n"

    tables_html = "\n".join(fundamental_tables_html or [])

    today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{symbol} 个股分析报告</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1100px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.chart {{ width:100%; max-width:1000px; display:block; margin:18px auto; border:1px solid #eee }}
.section {{ margin-top:28px }}
table {{ border-collapse: collapse; width:100%; margin-top:8px }}
th, td {{ border:1px solid #ddd; padding:8px }}
th {{ background:#f5f5f5; text-align:left }}
</style>
</head>
<body>
<h1>{_html.escape(symbol)} 个股分析报告</h1>
<p><strong>生成时间：</strong>{today}</p>

<div class='section'>
<h2>一、技术面分析</h2>
{tech_imgs_html}
{analysis_html}
</div>

<div class='section'>
<h2>二、基本面分析</h2>
{fund_html}
{fundamental_imgs_html}
{tables_html}
</div>

</body>
</html>
"""

    report_dir = os.path.join(out_dir, symbol)
    os.makedirs(report_dir, exist_ok=True)
    html_path = os.path.join(report_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return html_path


def build_three_reports(
    symbol: str,
    tech_md: str,
    fundamental_md: str,
    news_md: str,
    overview_md: str,
    fundamental_charts: Optional[List[Dict[str, str]]],
    fundamental_tables_html: Optional[List[str]],
    chart_paths: Dict[str, str],
    out_dir: str,
) -> Dict[str, str]:
    """生成四个 HTML：技术面（tech.html）、基本面（fund.html）、消息面（news.html）和总览（report.html，包含按钮切换视图）。

    返回包含四个路径的字典：{"tech":..., "fund":..., "news":..., "combined":...}
    """
    analysis_html = markdown_to_html(tech_md or "")
    fund_html = markdown_to_html(fundamental_md or "")
    news_html = markdown_to_html(news_md or "")
    overview_html = markdown_to_html(overview_md or "")

    # remove unwanted macro section from fund_html
    fund_html = _remove_macro_section_from_html(fund_html)

    report_dir = os.path.join(out_dir, symbol)
    os.makedirs(report_dir, exist_ok=True)

    # tech imgs
    tech_imgs_html = ""
    for title, path in chart_paths.items():
        try:
            relpath = os.path.relpath(path, start=report_dir).replace("\\", "/")
        except Exception:
            relpath = path
        tech_imgs_html += f"<h3>{_html.escape(title)}</h3>\n<img src=\"{relpath}\" class=\"chart\"/>\n"

    # fundamental imgs
    fundamental_imgs_html = ""
    if fundamental_charts:
        for ch in fundamental_charts:
            try:
                relp = os.path.relpath(ch.get("path"), start=report_dir).replace("\\", "/")
            except Exception:
                relp = ch.get("path", "")
            fundamental_imgs_html += f"<h4>{_html.escape(ch.get('title',''))}</h4>\n<img src=\"{relp}\" class=\"chart\"/>\n<p>{_html.escape(ch.get('description',''))}</p>\n"

    tables_html = "\n".join(fundamental_tables_html or [])

    now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    # 1) tech page
    tech_page = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{symbol} 技术面分析</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1100px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.toolbar {{ display:flex; justify-content:flex-end; margin-bottom:12px }}
.btn {{ padding:8px 12px; border-radius:6px; border:1px solid #ddd; cursor:pointer; background:#fff }}
.chart {{ width:100%; max-width:1000px; display:block; margin:18px auto; border:1px solid #eee }}
.section {{ margin-top:28px }}
table {{ border-collapse: collapse; width:100%; margin-top:8px }}
th, td {{ border:1px solid #ddd; padding:8px }}
th {{ background:#f5f5f5; text-align:left }}
@media print {{
  .no-print {{ display:none !important; }}
  body {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
<h1>{_html.escape(symbol)} 技术面分析</h1>
<div class='toolbar no-print'>
  <button class='btn' onclick="window.print()">导出 PDF</button>
</div>
<p><strong>生成时间：</strong>{now}</p>
<div class='section'>
{tech_imgs_html}
{analysis_html}
</div>
</body>
</html>
"""

    tech_path = os.path.join(report_dir, "tech.html")
    with open(tech_path, "w", encoding="utf-8") as f:
        f.write(tech_page)

    # 2) fundamental page
    fund_page = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{symbol} 基本面分析</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1100px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.toolbar {{ display:flex; justify-content:flex-end; margin-bottom:12px }}
.btn {{ padding:8px 12px; border-radius:6px; border:1px solid #ddd; cursor:pointer; background:#fff }}
.chart {{ width:100%; max-width:1000px; display:block; margin:18px auto; border:1px solid #eee }}
.section {{ margin-top:28px }}
table {{ border-collapse: collapse; width:100%; margin-top:8px }}
th, td {{ border:1px solid #ddd; padding:8px }}
th {{ background:#f5f5f5; text-align:left }}
@media print {{
  .no-print {{ display:none !important; }}
  body {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
<h1>{_html.escape(symbol)} 基本面分析</h1>
<div class='toolbar no-print'>
  <button class='btn' onclick="window.print()">导出 PDF</button>
</div>
<p><strong>生成时间：</strong>{now}</p>
<div class='section'>
{fund_html}
{fundamental_imgs_html}
{tables_html}
</div>
</body>
</html>
"""

    fund_path = os.path.join(report_dir, "fund.html")
    with open(fund_path, "w", encoding="utf-8") as f:
        f.write(fund_page)

    # 3) news page
    news_page = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{symbol} 消息面分析</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1100px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.toolbar {{ display:flex; justify-content:flex-end; margin-bottom:12px }}
.btn {{ padding:8px 12px; border-radius:6px; border:1px solid #ddd; cursor:pointer; background:#fff }}
.section {{ margin-top:28px }}
table {{ border-collapse: collapse; width:100%; margin-top:8px }}
th, td {{ border:1px solid #ddd; padding:8px }}
th {{ background:#f5f5f5; text-align:left }}
@media print {{
  .no-print {{ display:none !important; }}
  body {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
<h1>{_html.escape(symbol)} 消息面分析</h1>
<div class='toolbar no-print'>
  <button class='btn' onclick="window.print()">导出 PDF</button>
</div>
<p><strong>生成时间：</strong>{now}</p>
<div class='section'>
{news_html}
</div>
</body>
</html>
"""

    news_path = os.path.join(report_dir, "news.html")
    with open(news_path, "w", encoding="utf-8") as f:
        f.write(news_page)

    # 4) combined page with buttons
    combined_page = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{symbol} 综合分析报告</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1200px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.container {{ display:flex; flex-direction:column; gap:16px }}
.toolbar {{ display:flex; gap:8px; justify-content:flex-start }}
.page-actions {{ display:flex; justify-content:flex-end; margin-bottom:12px }}
.btn {{ padding:8px 14px; border-radius:6px; border:1px solid #ddd; cursor:pointer; background:#fff }}
.btn.active {{ background:#0078D4; color:#fff; border-color:#0078D4 }}
.report-panel {{ border:1px solid #eee; padding:16px; border-radius:6px; background:#fff }}
.section {{ margin-top:12px }}
.chart {{ width:100%; max-width:1000px; display:block; margin:18px auto; border:1px solid #eee }}
table {{ border-collapse: collapse; width:100%; margin-top:8px }}
th, td {{ border:1px solid #ddd; padding:8px }}
th {{ background:#f5f5f5; text-align:left }}
@media print {{
  .no-print {{ display:none !important; }}
  body {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
<div class='page-actions no-print'>
  <button class='btn' onclick="window.print()">导出 PDF</button>
</div>
<div class='container'>
  <div class='toolbar no-print'>
    <button id='btn-tech' class='btn active' onclick="showPanel('tech')">技术面</button>
    <button id='btn-fund' class='btn' onclick="showPanel('fund')">基本面</button>
    <button id='btn-news' class='btn' onclick="showPanel('news')">消息面</button>
    <button id='btn-over' class='btn' onclick="showPanel('overview')">总览</button>
  </div>
  <div id='panel-tech' class='report-panel'>
    <h2>技术面分析</h2>
    {tech_imgs_html}
    {analysis_html}
  </div>
  <div id='panel-fund' class='report-panel' style='display:none'>
    <h2>基本面分析</h2>
    {fund_html}
    {fundamental_imgs_html}
    {tables_html}
  </div>
  <div id='panel-news' class='report-panel' style='display:none'>
    <h2>消息面分析</h2>
    {news_html}
  </div>
  <div id='panel-overview' class='report-panel' style='display:none'>
    <h2>综合总览</h2>
    <div class='section'>
      <h3>综合结论</h3>
      <div>{overview_html}</div>
    </div>
    <div class='section'>
      <h3>技术面要点（摘要）</h3>
      <div>{analysis_html}</div>
    </div>
    <div class='section'>
      <h3>基本面要点（摘要）</h3>
      <div>{fund_html}</div>
    </div>
    <div class='section'>
      <h3>消息面要点（摘要）</h3>
      <div>{news_html}</div>
    </div>
  </div>
</div>
<script>
function showPanel(name) {{
  document.getElementById('panel-tech').style.display = (name === 'tech') ? '' : 'none';
  document.getElementById('panel-fund').style.display = (name === 'fund') ? '' : 'none';
  document.getElementById('panel-news').style.display = (name === 'news') ? '' : 'none';
  document.getElementById('panel-overview').style.display = (name === 'overview') ? '' : 'none';
  document.getElementById('btn-tech').classList.toggle('active', name === 'tech');
  document.getElementById('btn-fund').classList.toggle('active', name === 'fund');
  document.getElementById('btn-news').classList.toggle('active', name === 'news');
  document.getElementById('btn-over').classList.toggle('active', name === 'overview');
}}
</script>
</body>
</html>
"""

    combined_path = os.path.join(report_dir, "combined.html")
    with open(combined_path, "w", encoding="utf-8") as f:
        f.write(combined_page)

    return {"tech": tech_path, "fund": fund_path, "news": news_path, "combined": combined_path}


def gather_existing_charts(symbol: str, charts_dir: str) -> List[Dict[str, str]]:
    """Scan charts_dir for PNGs related to the symbol or common index (000001) and return a list of chart dicts.
    This helps when rendering pipeline failed but charts already exist on disk.
    """
    results: List[Dict[str, str]] = []
    if not os.path.isdir(charts_dir):
        return results
    for fn in sorted(os.listdir(charts_dir)):
        if not fn.lower().endswith('.png'):
            continue
        # candidate if file name contains symbol or is a common market index
        low = fn.lower()
        include = False
        if low.startswith(symbol.lower() + '_'):
            include = True
        if low.startswith('000001_') or low.startswith('sh000001_') or 'profitability' in low or 'growth' in low or 'liquidity' in low or 'dupont' in low or 'cash_flow' in low:
            include = True
        if not include:
            continue
        path = os.path.join(charts_dir, fn)
        title = fn.replace('.png', '').replace('_', ' ').title()
        # smarter title mapping
        if 'profitability' in low:
            title = '盈利能力关键指标季度走势'
        elif 'growth' in low:
            title = '成长能力指标季度走势'
        elif 'liquidity' in low or 'cash_flow' in low:
            title = '偿债能力/现金流季度走势'
        elif 'dupont' in low:
            title = '杜邦分析关键驱动因素'
        elif low.startswith('000001_') or 'market' in low:
            title = '上证综指（月线）'
        elif low.endswith('_month.png'):
            # e.g. 600519_month.png
            title = f"{symbol} 月线"
        description = ''
        results.append({'id': fn.replace('.','_'), 'title': title, 'description': description, 'path': path})
    return results


def main_interactive():
    """交互式主函数：提示 symbol 并生成包含基本面的 HTML 报告"""
    symbol = input("请输入 A 股代码（如 600519 或 000001）：").strip()
    if not symbol:
        print("未输入股票代码，退出")
        return

    mode = "both"  # 默认
    # message analysis options
    news_days = 7
    try:
        _days_in = input("Enter message window days (default 7, press Enter to skip): ").strip()
        if _days_in:
            news_days = max(1, int(_days_in))
    except Exception:
        news_days = 7

    use_bocha = os.getenv("BETTAFISH_USE_BOCHA", "0").strip().lower() in ("1", "true", "yes", "y")
    try:
        _bocha_in = input("Enable Bocha search for keywords? (y/N): ").strip().lower()
        if _bocha_in:
            use_bocha = _bocha_in in ("1", "true", "yes", "y")
    except Exception:
        pass


    print(f"开始生成报告：{symbol}（可能需要几秒到几分钟，取决于网络/LLM）")

    # 创建 agent
    try:
        agent = DeepSearchAgent()
    except Exception:
        # fallback to create_agent if available
        try:
            agent = create_agent()
        except Exception as e:
            print("无法创建 DeepSearchAgent：", e)
            return

    # 1) 技术面：调用 agent.analyze_stock（返回 Markdown）
    try:
        tech_md = agent.analyze_stock(symbol=symbol, mode="tech")
    except Exception as e:
        tech_md = f"**技术面分析失败：{e}**"

    # 2) 基本面多模态：Markdown + visuals + fundamentals payload
    fundamental_md = ""
    visuals = None
    fundamentals_payload = None
    fundamental_charts = []

    # Try preferred multimodal API, fall back to multiagent or synthesized outputs
    try:
        if hasattr(agent, "analyze_stock_fundamental_multimodal"):
            try:
                fundamental_md, visuals, fundamentals_payload = agent.analyze_stock_fundamental_multimodal(symbol)
            except Exception as e:
                print("调用 agent.analyze_stock_fundamental_multimodal 失败：", e)
                fundamental_md = ""
                visuals = None
                fundamentals_payload = None
        elif hasattr(agent, "analyze_stock_fundamental_multiagent"):
            try:
                # older API: returns markdown only
                fundamental_md = agent.analyze_stock_fundamental_multiagent(symbol)
                # try to get payload and visuals if available
                if hasattr(agent, "_build_fundamental_payload"):
                    fundamentals_payload = agent._build_fundamental_payload(symbol)
                else:
                    # try FinanceMCPClient fallback
                    try:
                        client = FinanceMCPClient()
                        raw = client.get_fundamentals(symbol)
                        fundamentals_payload = unwrap_fundamentals_payload(raw)
                    except Exception:
                        fundamentals_payload = {}
                if hasattr(agent, "plan_fundamental_visuals") and fundamentals_payload:
                    try:
                        visuals = agent.plan_fundamental_visuals(symbol, fundamentals_payload, fundamental_md)
                    except Exception:
                        visuals = None
            except Exception as e:
                print("调用 agent.analyze_stock_fundamental_multiagent 失败：", e)
                fundamental_md = ""
                visuals = None
                fundamentals_payload = None
        else:
            # Agent does not implement fundamental analysis API
            print("Warning: Agent 未实现基本面分析接口，使用本地回退逻辑生成基本面摘要。")
            fundamentals_payload = {}
            try:
                client = FinanceMCPClient()
                raw = client.get_fundamentals(symbol)
                fundamentals_payload = raw.get('fundamentals', {}) if isinstance(raw, dict) else {}
            except Exception:
                fundamentals_payload = {}
            # If agent provides a synthesizer, use it
            if hasattr(agent, "_synthesize_fundamental_analysis"):
                try:
                    # attempt to obtain profile if available
                    profile = {}
                    if hasattr(agent, 'finance_tool') and hasattr(agent.finance_tool, 'fetch_stock_profile'):
                        try:
                            profile = agent.finance_tool.fetch_stock_profile(symbol)
                        except Exception:
                            profile = {}
                    fundamental_md = agent._synthesize_fundamental_analysis(symbol, fundamentals_payload or {}, profile or {}, "", "")
                except Exception:
                    fundamental_md = generate_basic_fundamental_md(fundamentals_payload or {}, symbol)
            else:
                fundamental_md = generate_basic_fundamental_md(fundamentals_payload or {}, symbol)
    except Exception as e:
        # Catch-all to ensure we never expose AttributeError to the HTML
        print("获取基本面分析时遇到未知错误：", e)
        fundamental_md = generate_basic_fundamental_md({}, symbol)
        visuals = None
        fundamentals_payload = {}

    # 回退：如果 agent 未返回基本面文本，但我们拿到了 fundamentals_payload，则生成基础 Markdown
    if (not fundamental_md or '失败' in fundamental_md) and fundamentals_payload:
        try:
            generated_md = generate_basic_fundamental_md(fundamentals_payload, symbol)
            # 如果 agent 返回了错误信息，把回退摘要追加在后面；否则覆盖
            if fundamental_md and '失败' in fundamental_md:
                fundamental_md = generated_md
            else:
                fundamental_md = (fundamental_md + "\n\n" + generated_md).strip()
        except Exception:
            pass

    # 3) 渲染基本面图（如果 renderer 可用且 visuals 有 charts），否则使用简单绘图回退
    # 统一输出目录（后面所有图都写到 reports/<symbol>/charts）
    out_dir = getattr(agent.config, "OUTPUT_DIR", "reports") if hasattr(agent, "config") else "reports"
    report_dir = os.path.join(out_dir, symbol)
    charts_dir = os.path.join(report_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    # 3.5) message analysis (DB-driven)
    stock_name = ""
    try:
        if hasattr(agent, "finance_tool") and hasattr(agent.finance_tool, "fetch_stock_profile"):
            profile = agent.finance_tool.fetch_stock_profile(symbol)
            stock_name = _extract_stock_name(profile)
    except Exception:
        stock_name = ""

    news_md = ""
    try:
        message_agent = MessageAnalysisAgent(getattr(agent, "llm_client", None))
        message_result = message_agent.analyze(
            stock_code=symbol,
            stock_name=stock_name,
            days=news_days,
            use_bocha=use_bocha,
        )
        news_md = message_result.get("analysis_md") or ""
        prompt_materials = message_result.get("prompt_materials") or ""
        prompt_web_materials = message_result.get("prompt_web_materials") or ""
        input_keywords = message_result.get("keywords") or []
        items = message_result.get("items") or []
        web_items = message_result.get("web_items") or []
        inputs_html = build_message_inputs_html(
            input_keywords,
            items if items else prompt_materials,
            web_items if web_items else prompt_web_materials,
        )
        if inputs_html:
            news_md = (news_md + "\n\n" + inputs_html).strip()

        # save message materials for traceability
        try:
            materials_dir = os.path.join(report_dir, "materials")
            os.makedirs(materials_dir, exist_ok=True)
            dump_json(
                os.path.join(materials_dir, f"{symbol}_message_keywords.json"),
                {
                    "keywords": message_result.get("keywords") or [],
                    "db_error": message_result.get("db_error"),
                },
            )
            dump_json(
                os.path.join(materials_dir, f"{symbol}_message_items.json"),
                [vars(it) for it in (message_result.get("items") or [])],
            )
            dump_json(
                os.path.join(materials_dir, f"{symbol}_message_web_items.json"),
                message_result.get("web_items") or [],
            )
            sentiment_summary = message_result.get("sentiment_summary")
            if isinstance(sentiment_summary, dict):
                dump_json(
                    os.path.join(materials_dir, f"{symbol}_message_sentiment.json"),
                    sentiment_summary,
                )
        except Exception:
            pass
    except Exception as e:
        news_md = f"**消息面分析失败：{e}**"

    def _rel_to_report(p: str) -> str:
        """把绝对路径转成相对 reports/<symbol>/ 的相对路径，供 HTML img src 使用"""
        return os.path.relpath(p, start=report_dir).replace("\\", "/")

    simple_debug = None
    renderer_used = "none"
    renderer_error = None

    if HAS_FUNDAMENTAL_RENDERER and visuals is not None and getattr(visuals, "charts", None):
        try:
            # render_charts(symbol, plan, fundamentals, out_dir)
            fundamental_charts = render_charts(symbol, visuals, fundamentals_payload or {}, charts_dir)
            renderer_used = "visual_renderer"
        except Exception as e:
            print("渲染基本面图失败：", e)
            renderer_used = "visual_renderer_failed"
            renderer_error = repr(e)
            fundamental_charts = []
    else:
        # 回退：如果有原始数据，则绘制简单图表
        if fundamentals_payload:
            try:
                fundamental_charts, simple_debug = render_simple_charts(symbol, fundamentals_payload or {}, charts_dir)
                renderer_used = "simple_renderer"
            except Exception as e:
                print("回退绘制基本面图失败：", e)
                renderer_used = "simple_renderer_failed"
                renderer_error = repr(e)
                fundamental_charts = []
        else:
            renderer_used = "no_fundamentals_payload"
            fundamental_charts = []

    # 无论走哪条分支，都写 debug
    try:
        dump_json(os.path.join(report_dir, "debug_chart_files.json"), verify_chart_files(fundamental_charts))
        dump_json(os.path.join(report_dir, "debug_render.json"), {
            "symbol": symbol,
            "renderer_used": renderer_used,
            "renderer_error": renderer_error,
            "fundamental_charts_count": len(fundamental_charts),
            "simple_debug": simple_debug,
        })
        print(f"[DEBUG] renderer_used={renderer_used}, fundamental_charts count = {len(fundamental_charts)}")
    except Exception as e:
        print("写入 debug 文件失败：", e)

    # If no charts were produced, try to gather existing charts from charts_dir (helps when images were created earlier)
    if not fundamental_charts:
        try:
            existing = gather_existing_charts(symbol, charts_dir)
            # also search repo-level charts folder as a helpful fallback
            repo_charts_dir = os.path.join(os.getcwd(), "charts")
            existing_repo = []
            try:
                existing_repo = gather_existing_charts(symbol, repo_charts_dir)
            except Exception:
                existing_repo = []
            # combine unique paths
            paths = set((ch.get("path") for ch in (fundamental_charts or []) if ch.get("path")))
            for ch in (existing + existing_repo):
                p = ch.get("path")
                if p and p not in paths and os.path.exists(p):
                    fundamental_charts.append(ch)
                    paths.add(p)
        except Exception:
            pass

    # 4) 构造基本面表格 HTML：优先使用 visual plan，如果没有则从 payload 生成
    tables_html = []
    try:
        tables_html = render_tables_html(visuals, fundamentals_payload or {})
        if not tables_html:
            tables_html = render_tables_from_payload(fundamentals_payload or {})
    except Exception:
        try:
            tables_html = render_tables_from_payload(fundamentals_payload or {})
        except Exception:
            tables_html = []

    # 5) 生成技术面 K 线图（使用 FinanceMCPClient.plot_technical_charts）
    client = FinanceMCPClient()
    chart_paths = {}
    try:
        day_chart = client.plot_technical_charts(symbol=symbol, timeframe="DAY", limit=200, save_path=os.path.join(charts_dir, f"{symbol}_tech_day.png"), show=False)
        weekly_chart = client.plot_technical_charts(symbol=symbol, timeframe="WEEK", limit=260, save_path=os.path.join(charts_dir, f"{symbol}_week.png"), show=False)
        monthly_chart = client.plot_technical_charts(symbol=symbol, timeframe="MON", limit=360, save_path=os.path.join(charts_dir, f"{symbol}_month.png"), show=False)
        intraday_chart = client.plot_technical_charts(symbol=symbol, timeframe="60M", limit=80, save_path=os.path.join(charts_dir, f"{symbol}_60m.png"), show=False)
        chart_paths = {
            "日线技术图": day_chart,
            "周线技术图": weekly_chart,
            "月线技术图": monthly_chart,
            "60分线": intraday_chart,
        }
    except Exception as e:
        print("生成技术图失败：", e)
        chart_paths = {}
    macro_debug = {"ok": False, "reason": "not_built"}
    macro_evidence_html = ""


    try:
        client = FinanceMCPClient()

        # 为了对比：先拿个股日线 OHLC（用于下方K线）
        stock_ctx = client.get_ohlc(symbol=symbol, timeframe="DAY", limit=240)
        stock_df = ohlc_ctx_to_df(stock_ctx)

        # 1) 沪深300（日线 OHLC）+ 个股（日线）对比K线图
        hs300_ctx = client.get_ohlc(symbol="000300.SH", timeframe="DAY", limit=240)  # 日线更符合你需求
        hs300_data = (hs300_ctx.get("data", []) if isinstance(hs300_ctx, dict) else (hs300_ctx or []))
        hs300_df = pd.DataFrame(hs300_data)

        hs300_sum = index_summary(hs300_df, date_col="date", close_col="close")

        hs300_png = plot_kline_compare(
            top_df=hs300_df,
            bottom_df=stock_df,
            top_title="CSI300（沪深300）日线K",
            bottom_title=f"{symbol} 日线K（对比）",
            out_path=os.path.join(charts_dir, f"hs300_vs_{symbol}_day_k.png"),
            date_col="date",
        )

        # 2) 申万行业指数（日线优先）+ 个股（日线）对比K线图
        if hasattr(client, "get_industry_index"):
            sw_ctx = client.get_industry_index(symbol, stock_name=stock_name) or {}
        else:
            sw_ctx = client.get_sw_industry_index(symbol) or {}
        sw_meta = (sw_ctx.get("industry_index") or sw_ctx.get("sw_industry") or {})
        sw_code = (
            sw_meta.get("index_symbol")
            or sw_meta.get("index_code")
            or sw_meta.get("industry_code")
            or sw_meta.get("code")
        )
        sw_name = sw_meta.get("industry_name") or sw_meta.get("name") or "行业指数"
        sw_kind = sw_meta.get("index_kind") or ("sw" if re.search(r"^8\\d{5}$", str(sw_code or "")) else "index")
        sw_label = "SW 行业指数" if sw_kind == "sw" else "行业指数"
        sw_code_digits = None
        if sw_code:
            code_match = re.search(r"\d{6}", str(sw_code))
            sw_code_digits = code_match.group(0) if code_match else None
        sw_code_slug = re.sub(r"[^0-9A-Za-z]+", "", str(sw_code)) if sw_code else ""

        # 优先：如果未来你在 client 里补了 index_daily，这里直接用
        sw_day = ((sw_ctx.get("index_daily") or {}).get("data", [])) if isinstance(sw_ctx, dict) else []
        sw_df = pd.DataFrame(sw_day or [])

        # 兜底：如果 client 还没有日线，就用 AkShare 直接拉日线（只改这个 txt 也能跑起来）
        if sw_df.empty and sw_code_digits and sw_kind == "sw":
            try:
                import akshare as ak
                if hasattr(ak, "index_hist_sw"):
                    tmp = ak.index_hist_sw(symbol=str(sw_code_digits), period="day")
                    # 兼容列名映射到 date/open/high/low/close/volume
                    tmp = tmp.rename(columns={
                        "日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
                        "成交量": "volume"
                    })
                    sw_df = tmp[["date", "open", "high", "low", "close", "volume"]] if "volume" in tmp.columns else tmp[
                        ["date", "open", "high", "low", "close"]]
            except Exception:
                pass

        sw_sum = index_summary(sw_df, date_col="date", close_col="close")

        sw_png = plot_kline_compare(
            top_df=sw_df,
            bottom_df=stock_df,
            top_title=f"{sw_label}日线K：{sw_name} ({sw_code})",
            bottom_title=f"{symbol} 日线K（对比）",
            out_path=os.path.join(charts_dir,
                                  f"sw_{sw_code_slug}_vs_{symbol}_day_k.png" if sw_code_slug else f"sw_vs_{symbol}_day_k.png"),
            date_col="date",
        )
        # ===== NEW: 成功时构建宏观证据 HTML + debug_payload.json =====
        def _rel(p: Optional[str]) -> Optional[str]:
            if not p:
                return None
            return os.path.relpath(p, start=report_dir).replace("\\", "/")

        hs300_rel = _rel(hs300_png)
        sw_rel = _rel(sw_png)

        macro_debug = {
            "ok": True,
            "hs300": {
                "summary": hs300_sum,
                "png": hs300_rel
            },
            "sw_industry": {
                "code": sw_code,
                "name": sw_name,
                "summary": sw_sum,
                "png": sw_rel,
                "kind": sw_kind
            }
        }
        dump_json(os.path.join(report_dir, "debug_payload.json"), macro_debug)

        # summary 表行
        def _sum_row(name: str, s: Dict[str, Any]) -> str:
            if not s or not s.get("ok"):
                return f"<tr><td>{_html.escape(name)}</td><td colspan='5'>获取失败：{_html.escape(str((s or {}).get('reason')))}</td></tr>"
            return (
                f"<tr><td>{_html.escape(name)}</td>"
                f"<td>{_html.escape(str(s['start_date']))}</td>"
                f"<td>{_html.escape(str(s['end_date']))}</td>"
                f"<td>{int(s['points'])}</td>"
                f"<td>{float(s['start_close']):.2f} → {float(s['end_close']):.2f}</td>"
                f"<td>{float(s['return_pct']):.2f}%</td></tr>"
            )

        # 组装 HTML（注意：这里才是真正“插图”的地方）
        macro_evidence_html = "<div class='section'><h2>宏观与行业指数（硬证据）</h2>"
        macro_evidence_html += "<p>以下为程序实际取数与绘图结果；若无图/无 summary，则代表数据未成功获取。</p>"

        macro_evidence_html += """
        <table>
          <thead><tr><th>指数</th><th>起始</th><th>结束</th><th>样本点</th><th>收盘价</th><th>区间涨跌幅</th></tr></thead>
          <tbody>
        """
        macro_evidence_html += _sum_row("沪深300（日线）", hs300_sum)
        macro_evidence_html += _sum_row(f"{sw_label}（日线）：{sw_name}", sw_sum)
        macro_evidence_html += "</tbody></table>"

        if hs300_rel:
            macro_evidence_html += f"<h3>沪深300 vs {symbol}（日线K 对比）</h3><img class='chart' src='{hs300_rel}'/>"
        if sw_rel:
            macro_evidence_html += f"<h3>{sw_name} vs {symbol}（日线K 对比）</h3><img class='chart' src='{sw_rel}'/>"

        macro_evidence_html += "</div>"
        # ===== NEW END =====



    except Exception as e:
        macro_debug = {"ok": False, "reason": "exception", "error": repr(e)}
        dump_json(os.path.join(report_dir, "debug_payload.json"), macro_debug)
        macro_evidence_html = (
            "<div class='section'><h2>宏观与行业指数（硬证据）</h2>"
            f"<p>构建失败：{_html.escape(repr(e))}</p></div>"
        )

    fundamental_md = (macro_evidence_html + "\n\n" + (fundamental_md or "")).strip()
    overview_md = build_combined_summary_md(
        symbol=symbol,
        tech_md=tech_md,
        fundamental_md=fundamental_md,
        news_md=news_md,
        llm_client=getattr(agent, "llm_client", None),
    )
    # 6) 最终 HTML 写入 reports/<symbol>/report.html
    html_path = build_html_report(
        symbol=symbol,
        tech_md=tech_md,
        fundamental_md=fundamental_md,
        fundamental_charts=fundamental_charts,
        fundamental_tables_html=tables_html,
        chart_paths=chart_paths,
        out_dir=out_dir,
    )

    # 新增：同时生成三个独立的 HTML 报告
    three_report_paths = build_three_reports(
        symbol=symbol,
        tech_md=tech_md,
        fundamental_md=fundamental_md,
        news_md=news_md,
        overview_md=overview_md,
        fundamental_charts=fundamental_charts,
        fundamental_tables_html=tables_html,
        chart_paths=chart_paths,
        out_dir=out_dir,
    )

    print(f"报告已生成：{html_path}")
    print(f"独立技术面报告：{three_report_paths['tech']}")
    print(f"独立基本面报告：{three_report_paths['fund']}")
    print(f"独立消息面报告：{three_report_paths['news']}")
    print(f"综合分析报告：{three_report_paths['combined']}")


if __name__ == "__main__":
    main_interactive()

