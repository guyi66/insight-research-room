from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from finance_tools.aitrados_client import FinanceMCPClient
from message_analysis_agent import MessageAnalysisAgent
from stock_analysis_report import (
    HAS_FUNDAMENTAL_RENDERER,
    build_combined_summary_md,
    build_message_inputs_html,
    build_three_reports,
    dump_json,
    gather_existing_charts,
    generate_basic_fundamental_md,
    index_summary,
    ohlc_ctx_to_df,
    plot_kline_compare,
    render_charts,
    render_simple_charts,
    render_tables_from_payload,
    render_tables_html,
    unwrap_fundamentals_payload,
    verify_chart_files,
    _extract_stock_name,
)

try:
    from InsightEngine.agent import DeepSearchAgent, create_agent
except Exception:
    DeepSearchAgent = None
    create_agent = None


LogFn = Optional[Callable[[str], None]]
VALID_REPORT_MODES = ("tech", "fund", "news", "combined")
_CHART_LOCK = threading.Lock()
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
TMP_DIR = BASE_DIR / "tmp"


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


def _log(msg: str, logger: LogFn = None) -> None:
    if logger:
        logger(msg)
    else:
        print(msg)


def _emit_log(logger: LogFn, agent: str, status: str, message: str) -> None:
    if not logger:
        return
    payload = {"agent": agent, "status": status, "message": message}
    try:
        logger(payload)
    except TypeError:
        logger(f"[{agent}][{status}] {message}")


def _flatten_payload(obj: Any, prefix: str, rows: List[Dict[str, Any]], symbol: str, section: str) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_payload(value, next_prefix, rows, symbol, section)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            next_prefix = f"{prefix}[{idx}]"
            _flatten_payload(value, next_prefix, rows, symbol, section)
    else:
        rows.append({
            "section": section,
            "symbol": symbol,
            "path": prefix,
            "value": obj,
        })


def _normalize_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join([str(v) for v in value if v is not None])
    return value


def _rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    preferred = [
        "section",
        "symbol",
        "timeframe",
        "date",
        "dt",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "macd_diff",
        "macd_dea",
        "macd_hist",
        "rsi14",
        "boll_mid",
        "boll_upper",
        "boll_lower",
        "platform",
        "published_at",
        "title",
        "content",
        "snippet",
        "url",
        "author",
        "match_count",
        "engagement",
        "score",
        "keyword",
        "source_table",
        "time_source",
        "media_urls",
        "path",
        "value",
        "keywords",
        "db_error",
        "type",
        "summary",
        "source",
        "sort_ts",
        "id",
    ]
    fieldnames: List[str] = []
    seen = set()
    for key in preferred:
        if key not in seen:
            fieldnames.append(key)
            seen.add(key)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        normalized = {k: _normalize_csv_value(v) for k, v in row.items()}
        writer.writerow(normalized)
    return output.getvalue()


def _write_analysis_csv(
    symbol: str,
    report_dir: str,
    tech_payload: Optional[Dict[str, Any]],
    fundamentals_payload: Optional[Dict[str, Any]],
    news_bundle: Optional[Dict[str, Any]],
    evidence_items: Optional[List[Dict[str, Any]]],
) -> Optional[str]:
    materials_dir = os.path.join(report_dir, "materials")
    os.makedirs(materials_dir, exist_ok=True)

    rows: List[Dict[str, Any]] = []

    if isinstance(tech_payload, dict):
        for timeframe, records in tech_payload.items():
            if not isinstance(records, list):
                continue
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                row = {"section": "tech", "symbol": symbol, "timeframe": timeframe}
                row.update(rec)
                rows.append(row)

    if isinstance(fundamentals_payload, dict) and fundamentals_payload:
        _flatten_payload(fundamentals_payload, "", rows, symbol, "fund")

    if isinstance(news_bundle, dict):
        items = news_bundle.get("items_all") or news_bundle.get("items") or []
        for item in items:
            data = vars(item) if hasattr(item, "__dict__") else item
            if not isinstance(data, dict):
                continue
            rows.append({
                "section": "news_db",
                "symbol": symbol,
                "platform": data.get("platform", ""),
                "published_at": data.get("published_at", ""),
                "title": data.get("title", ""),
                "content": data.get("content", ""),
                "url": data.get("url", ""),
                "author": data.get("author", ""),
                "keyword": data.get("keyword", ""),
                "engagement": data.get("engagement", ""),
                "match_count": data.get("match_count", ""),
                "score": data.get("score", ""),
                "source_table": data.get("source_table", ""),
                "time_source": data.get("time_source", ""),
                "media_urls": data.get("media_urls", ""),
            })

        web_items = news_bundle.get("web_items") or []
        for item in web_items:
            if not isinstance(item, dict):
                continue
            rows.append({
                "section": "news_web",
                "symbol": symbol,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "url": item.get("url", ""),
            })

        keywords = news_bundle.get("keywords")
        if isinstance(keywords, list):
            rows.append({
                "section": "news_keywords",
                "symbol": symbol,
                "keywords": ", ".join([str(k) for k in keywords if k]),
                "db_error": news_bundle.get("db_error") or "",
            })

        ingest_meta = news_bundle.get("akshare_ingest")
        if isinstance(ingest_meta, dict):
            _flatten_payload(ingest_meta, "akshare_ingest", rows, symbol, "news_meta")

    if isinstance(evidence_items, list) and evidence_items:
        for item in evidence_items:
            if not isinstance(item, dict):
                continue
            row = {"section": "evidence", "symbol": symbol}
            row.update(item)
            rows.append(row)

    if not rows:
        return None
    csv_text = _rows_to_csv(rows)
    file_path = os.path.join(materials_dir, f"{symbol}_analysis_data.csv")
    with open(file_path, "wb") as f:
        f.write(csv_text.encode("utf-8-sig"))
    return file_path


def _normalize_report_modes(report_mode: Optional[str], report_modes: Optional[List[str]]) -> List[str]:
    raw: List[str] = []
    if report_modes:
        for item in report_modes:
            if item is None:
                continue
            raw.append(str(item))
    elif report_mode:
        raw.append(str(report_mode))

    modes: List[str] = []
    for item in raw:
        parts = [p.strip().lower() for p in item.split(",") if p.strip()]
        for part in parts:
            if part not in VALID_REPORT_MODES:
                raise ValueError(f"Unknown report_mode: {part}")
            if part not in modes:
                modes.append(part)
    return modes


def apply_env_overrides(overrides: Optional[Dict[str, str]]) -> None:
    if not overrides:
        return
    for key, value in overrides.items():
        os.environ[str(key)] = str(value)
    try:
        from config import reload_settings
        reload_settings()
    except Exception:
        pass


def resolve_db_config(overrides: Optional[Dict[str, str]] = None) -> Optional[DBConfig]:
    try:
        from config import settings
    except Exception:
        settings = None

    def _get(name: str, default: str) -> str:
        if overrides and overrides.get(name):
            return str(overrides.get(name))
        if settings and getattr(settings, name, None):
            return str(getattr(settings, name))
        return str(os.getenv(name, default))

    host = _get("DB_HOST", "127.0.0.1")
    port = int(_get("DB_PORT", "3306"))
    user = _get("DB_USER", "root")
    password = _get("DB_PASSWORD", "")
    database = _get("DB_NAME", "")
    charset = _get("DB_CHARSET", "utf8mb4")
    if not database:
        return None
    return DBConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    )


def write_temp_mysql_config(db_cfg: DBConfig, base_dir: Optional[Path] = None) -> Path:
    base_dir = base_dir or Path.cwd()
    base_dir.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".ini", dir=str(base_dir))
    tmp.write(
        "[mysql]\n"
        f"host={db_cfg.host}\n"
        f"port={db_cfg.port}\n"
        f"user={db_cfg.user}\n"
        f"password={db_cfg.password}\n"
        f"database={db_cfg.database}\n"
        f"charset={db_cfg.charset}\n"
    )
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def run_mindspider_crawler(
    symbol: str,
    stock_name: str,
    config_path: Path,
    platforms: List[str],
    days: int,
    max_notes: int,
    mindspider_dir: Optional[str] = None,
    keywords: Optional[str] = None,
    no_crawl: bool = False,
    log: LogFn = None,
) -> None:
    mindspider_dir = mindspider_dir or str(BASE_DIR)
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "run_mindspider_to_mysql_v8_3_1_fixed3.py"),
        "--mindspider-dir",
        mindspider_dir,
        "--config",
        str(config_path),
        "--code",
        symbol,
        "--name",
        stock_name or symbol,
        "--platforms",
        ",".join(platforms),
        "--days",
        str(int(days)),
        "--max-notes",
        str(int(max_notes)),
    ]
    if keywords:
        cmd += ["--keywords", keywords]
    if no_crawl:
        cmd.append("--no-crawl")
    _log(f"[crawl][mindspider] {cmd}", log)
    subprocess.run(cmd, check=True)


def run_guba_crawler(
    symbol: str,
    config_path: Path,
    guba_dir: Optional[str] = None,
    keywords: Optional[str] = None,
    pages: int = 10,
    init_schema: bool = False,
    fulltext: bool = False,
    fulltext_days: int = 7,
    fulltext_limit: int = 200,
    log: LogFn = None,
) -> None:
    guba_dir = guba_dir or str(BASE_DIR / "Guba-Crawler")
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "run_guba_crawler_mysql.py"),
        "--guba-dir",
        guba_dir,
        "--config",
        str(config_path),
        "--code",
        symbol,
    ]
    if keywords:
        cmd += ["--keywords", keywords]
    cmd += [
        "--pages",
        str(int(pages)),
    ]
    if init_schema:
        cmd.append("--init-schema")
    if fulltext:
        cmd.append("--fulltext")
        cmd += ["--fulltext-days", str(int(fulltext_days))]
        cmd += ["--fulltext-limit", str(int(fulltext_limit))]
    _log(f"[crawl][guba] {cmd}", log)
    subprocess.run(cmd, check=True)


def _init_agent(config: Optional[Any] = None) -> Any:
    if DeepSearchAgent:
        try:
            return DeepSearchAgent(config)
        except Exception:
            pass
    if create_agent:
        return create_agent(config)
    raise RuntimeError("DeepSearchAgent unavailable.")


def _clean_override(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _get_override_value(overrides: Optional[Dict[str, Any]], *keys: str) -> Optional[str]:
    if not overrides:
        return None
    for key in keys:
        if key in overrides:
            cleaned = _clean_override(overrides.get(key))
            if cleaned:
                return cleaned
    return None


def _normalize_agent_overrides(agent_overrides: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(agent_overrides, dict):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in agent_overrides.items():
        if isinstance(value, dict) and value:
            normalized[str(key).strip().lower()] = value
    return normalized


def _build_insight_settings(
    overrides: Optional[Dict[str, Any]],
    db_cfg: Optional[DBConfig],
    output_dir: Optional[str],
) -> Optional[Any]:
    if not overrides and not db_cfg and not output_dir:
        return None
    try:
        from InsightEngine.utils.config import Settings as InsightSettings
    except Exception:
        return None

    settings_kwargs: Dict[str, Any] = {}
    api_key = _get_override_value(
        overrides,
        "api_key",
        "apiKey",
        "INSIGHT_ENGINE_API_KEY",
        "API_KEY",
    )
    base_url = _get_override_value(
        overrides,
        "base_url",
        "baseUrl",
        "INSIGHT_ENGINE_BASE_URL",
        "BASE_URL",
    )
    model_name = _get_override_value(
        overrides,
        "model_name",
        "modelName",
        "INSIGHT_ENGINE_MODEL_NAME",
        "MODEL_NAME",
    )
    if api_key:
        settings_kwargs["INSIGHT_ENGINE_API_KEY"] = api_key
    if base_url:
        settings_kwargs["INSIGHT_ENGINE_BASE_URL"] = base_url
    if model_name:
        settings_kwargs["INSIGHT_ENGINE_MODEL_NAME"] = model_name

    if db_cfg:
        settings_kwargs.setdefault("DB_HOST", db_cfg.host)
        settings_kwargs.setdefault("DB_PORT", db_cfg.port)
        settings_kwargs.setdefault("DB_USER", db_cfg.user)
        settings_kwargs.setdefault("DB_PASSWORD", db_cfg.password)
        settings_kwargs.setdefault("DB_NAME", db_cfg.database)
        settings_kwargs.setdefault("DB_CHARSET", db_cfg.charset)
    if output_dir:
        settings_kwargs["OUTPUT_DIR"] = output_dir
    if not settings_kwargs:
        return None
    return InsightSettings(**settings_kwargs)


def _serialize_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    try:
        import numpy as np

        if isinstance(value, (np.integer, np.floating, np.bool_)):
            return value.item()
    except Exception:
        pass
    return value


def _df_to_records(df) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        row = {}
        for key, value in rec.items():
            row[key] = _serialize_json_value(value)
        records.append(row)
    return records


def _serialize_record(record: Any) -> Dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    return {key: _serialize_json_value(value) for key, value in record.items()}


def _serialize_records(records: Any) -> List[Dict[str, Any]]:
    if not isinstance(records, list):
        return []
    serialized: List[Dict[str, Any]] = []
    for rec in records:
        if isinstance(rec, dict):
            serialized.append(_serialize_record(rec))
    return serialized


def _clip_text(text: Any, max_len: int = 220) -> str:
    if text is None:
        return ""
    s = str(text).strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "..."


def _parse_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def _unique_terms(terms: Optional[List[str]]) -> List[str]:
    items = terms or []
    seen = set()
    result: List[str] = []
    for term in items:
        if term is None:
            continue
        raw = str(term).strip()
        if not raw:
            continue
        key = raw.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(raw)
    return result


def _parse_datetime_value(value: Any) -> Optional["datetime"]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y%m%d",
    ):
        try:
            return datetime.strptime(text[: len(fmt)], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _score_text(text: str, terms: List[str]) -> float:
    if not text or not terms:
        return 0.0
    low = text.lower()
    score = 0.0
    for term in terms:
        t = term.lower()
        if not t:
            continue
        if t in low:
            score += 3.0 + float(low.count(t))
    return score


def _normalize_ohlc_df(df: Any) -> Any:
    try:
        pd = __import__("pandas")
    except Exception:
        return df
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df

    def _pick(cands: List[str]) -> Optional[str]:
        for cand in cands:
            if cand in df.columns:
                return cand
        return None

    date_col = _pick(["date", "日期", "交易日期", "TRADE_DATE", "trade_date"])
    if date_col and date_col != "date":
        df["date"] = df[date_col]

    for key, cands in {
        "open": ["open", "开盘", "开盘价", "OPEN"],
        "high": ["high", "最高", "最高价", "HIGH"],
        "low": ["low", "最低", "最低价", "LOW"],
        "close": ["close", "收盘", "收盘价", "CLOSE"],
        "volume": ["volume", "成交量", "成交量(股)", "VOL"],
    }.items():
        col = _pick(cands)
        if col and col != key:
            df[key] = df[col]

    return df


def _infer_sw_industry_by_llm(
    llm_client: Any,
    symbol: str,
    stock_name: str,
    profile: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    if not llm_client:
        return {}
    prompt = (
        "You are an equity analyst. Infer the Shenwan (SW) industry index code for the stock. "
        "Return JSON only: {\"sw_code\":\"801xxx\",\"sw_name\":\"...\"}. "
        "Use 6-digit SW index code if possible."
    )
    user = {
        "symbol": symbol,
        "stock_name": stock_name,
        "profile": profile or {},
    }
    text = ""
    try:
        if hasattr(llm_client, "stream_invoke_to_string"):
            text = llm_client.stream_invoke_to_string(prompt, json.dumps(user, ensure_ascii=False), temperature=0.1)
        elif hasattr(llm_client, "invoke"):
            text = llm_client.invoke(prompt, json.dumps(user, ensure_ascii=False), temperature=0.1)
    except Exception:
        text = ""
    parsed = _parse_json_from_text(text) if text else None
    if not isinstance(parsed, dict):
        return {}
    code_raw = str(parsed.get("sw_code") or parsed.get("industry_code") or parsed.get("code") or "").strip()
    name_raw = str(parsed.get("sw_name") or parsed.get("industry_name") or parsed.get("name") or "").strip()
    code_match = re.search(r"\d{6}", code_raw)
    code = code_match.group(0) if code_match else ""
    if not code:
        return {}
    return {"code": code, "name": name_raw}


def _fetch_sw_index_df(sw_code: str) -> Optional[Any]:
    if not sw_code:
        return None
    code_match = re.search(r"\d{6}", str(sw_code))
    sw_code = code_match.group(0) if code_match else str(sw_code)
    try:
        import akshare as ak
    except Exception:
        return None
    df = None
    try:
        if hasattr(ak, "index_hist_sw"):
            df = ak.index_hist_sw(symbol=str(sw_code), period="day")
        elif hasattr(ak, "sw_index_daily"):
            df = ak.sw_index_daily(
                index_code=str(sw_code),
                start_date="2000-01-01",
                end_date=datetime.today().strftime("%Y-%m-%d"),
            )
    except Exception:
        df = None
    return df


def _build_orchestrator_plan(
    symbol: str,
    stock_name: str,
    modes: List[str],
    news_days: int,
    use_bocha: bool,
    llm_client: Any,
) -> Dict[str, Any]:
    base_plan = {
        "steps": [
            {"agent": "tech", "goal": "输出技术面关键趋势/形态/量价信号"},
            {"agent": "fund", "goal": "输出基本面增长/盈利/现金流/负债结构摘要"},
            {"agent": "news", "goal": "输出消息面事件归纳与情绪方向"},
            {"agent": "critic", "goal": "检查冲突与证据缺口，给出置信度"},
            {"agent": "combined", "goal": "综合输出可执行结论与风险"},
        ],
        "checks": [
            "是否存在结论与证据不匹配",
            "是否存在跨时间尺度的冲突",
            "是否存在关键数据缺口",
        ],
        "questions": [],
    }

    if not llm_client:
        return {"plan_json": base_plan, "plan_md": "基于默认流程生成任务计划。"}

    system_prompt = (
        "你是投资分析流程的编排 Agent。请根据输入给出清晰的分析任务计划，"
        "输出 JSON，字段包含 steps/checks/questions。仅输出 JSON。"
    )
    user_prompt = (
        f"Symbol: {symbol}\n"
        f"Stock name: {stock_name}\n"
        f"Report modes: {', '.join(modes)}\n"
        f"News window days: {news_days}\n"
        f"Use Bocha: {use_bocha}\n"
        "请生成分析编排计划。"
    )
    plan_text = ""
    try:
        if hasattr(llm_client, "stream_invoke_to_string"):
            plan_text = llm_client.stream_invoke_to_string(system_prompt, user_prompt, temperature=0.2)
        elif hasattr(llm_client, "invoke"):
            plan_text = llm_client.invoke(system_prompt, user_prompt, temperature=0.2)
    except Exception:
        plan_text = ""

    parsed = _parse_json_from_text(plan_text) if plan_text else None
    plan_json = parsed or base_plan
    plan_md = plan_text.strip() if plan_text else "基于默认流程生成任务计划。"
    return {"plan_json": plan_json, "plan_md": plan_md}


def _build_critic_report(
    tech_md: str,
    fundamental_md: str,
    news_md: str,
    evidence_text: str,
    llm_client: Any,
) -> Dict[str, Any]:
    missing = []
    if not tech_md:
        missing.append("技术面")
    if not fundamental_md:
        missing.append("基本面")
    if not news_md:
        missing.append("消息面")
    base_conf = "低" if missing else "中"
    base_json = {
        "conflicts": [],
        "gaps": missing,
        "confidence": {
            "tech": "低" if "技术面" in missing else "中",
            "fund": "低" if "基本面" in missing else "中",
            "news": "低" if "消息面" in missing else "中",
            "overall": base_conf,
        },
    }

    if not llm_client:
        md = f"## Critic Review\n\n- 缺口：{', '.join(missing) if missing else '无明显缺口'}\n- 置信度：{base_conf}"
        return {"critic_json": base_json, "critic_md": md}

    system_prompt = (
        "你是严格的投资研究质控 Agent。请只基于输入材料找出冲突、证据缺口与置信度，"
        "输出 JSON，字段包含 conflicts/gaps/confidence。仅输出 JSON。"
    )
    user_prompt = (
        "Technical analysis excerpt:\n"
        f"{_clip_text(tech_md, 1200)}\n\n"
        "Fundamental analysis excerpt:\n"
        f"{_clip_text(fundamental_md, 1200)}\n\n"
        "News analysis excerpt:\n"
        f"{_clip_text(news_md, 1200)}\n\n"
        "Evidence index:\n"
        f"{_clip_text(evidence_text, 1200)}\n"
    )
    critic_text = ""
    try:
        if hasattr(llm_client, "stream_invoke_to_string"):
            critic_text = llm_client.stream_invoke_to_string(system_prompt, user_prompt, temperature=0.2)
        elif hasattr(llm_client, "invoke"):
            critic_text = llm_client.invoke(system_prompt, user_prompt, temperature=0.2)
    except Exception:
        critic_text = ""

    parsed = _parse_json_from_text(critic_text) if critic_text else None
    critic_json = parsed or base_json
    md = critic_text.strip() if critic_text else f"## Critic Review\n\n- 缺口：{', '.join(missing) if missing else '无明显缺口'}\n- 置信度：{base_conf}"
    return {"critic_json": critic_json, "critic_md": md}


def _format_orchestrator_markdown(plan_json: Any, plan_text: str = "") -> str:
    data = plan_json
    if not isinstance(data, dict):
        parsed = _parse_json_from_text(plan_text) if plan_text else None
        if isinstance(parsed, dict):
            data = parsed

    lines: List[str] = ["## Orchestrator Plan"]
    if isinstance(data, dict):
        steps = data.get("steps") or []
        checks = data.get("checks") or []
        questions = data.get("questions") or []
        if not isinstance(steps, list):
            steps = [steps]
        if not isinstance(checks, list):
            checks = [checks]
        if not isinstance(questions, list):
            questions = [questions]

        if steps:
            lines.append("### 任务步骤")
            for idx, step in enumerate(steps, 1):
                if isinstance(step, dict):
                    agent = str(step.get("agent") or "").strip()
                    task = str(step.get("task") or step.get("goal") or step.get("title") or "").strip()
                    desc = str(step.get("description") or step.get("desc") or "").strip()
                    label = f"[{agent}] " if agent else ""
                    text = (label + task).strip() if task else label.strip()
                    if desc:
                        text = f"{text} — {desc}" if text else desc
                    lines.append(f"{idx}. {_clip_text(text or str(step), 260)}")
                else:
                    lines.append(f"{idx}. {_clip_text(step, 260)}")

        if checks:
            lines.append("### 关键检查")
            for chk in checks:
                if isinstance(chk, dict):
                    name = str(chk.get("check") or chk.get("name") or chk.get("title") or "").strip()
                    desc = str(chk.get("description") or chk.get("desc") or "").strip()
                    text = name or "检查项"
                    if desc:
                        text = f"{text}：{desc}"
                    lines.append(f"- {_clip_text(text, 240)}")
                else:
                    lines.append(f"- {_clip_text(chk, 240)}")

        if questions:
            lines.append("### 待确认问题")
            for q in questions:
                if isinstance(q, dict):
                    text = str(q.get("question") or q.get("text") or q.get("desc") or "").strip()
                    if text:
                        lines.append(f"- {_clip_text(text, 240)}")
                else:
                    text = str(q).strip()
                    if text:
                        lines.append(f"- {_clip_text(text, 240)}")

        if len(lines) > 1:
            return "\n".join(lines)

    if plan_text:
        lines.append("### 计划说明")
        for raw in plan_text.splitlines():
            text = raw.strip()
            if text:
                lines.append(f"- {_clip_text(text, 260)}")
        return "\n".join(lines)

    return ""


def _format_critic_markdown(critic_json: Any, critic_text: str = "") -> str:
    data = critic_json
    if not isinstance(data, dict):
        parsed = _parse_json_from_text(critic_text) if critic_text else None
        if isinstance(parsed, dict):
            data = parsed

    lines: List[str] = ["## Critic Review"]
    if isinstance(data, dict):
        conflicts = data.get("conflicts") or []
        gaps = data.get("gaps") or []
        confidence = data.get("confidence") or {}
        if not isinstance(conflicts, list):
            conflicts = [conflicts]
        if not isinstance(gaps, list):
            gaps = [gaps]
        if not isinstance(confidence, dict):
            confidence = {}

        if conflicts:
            lines.append("### 主要冲突")
            for idx, item in enumerate(conflicts, 1):
                if isinstance(item, dict):
                    desc = str(item.get("description") or item.get("summary") or "").strip()
                    sources = item.get("sources") or []
                    src_text = ""
                    if isinstance(sources, list) and sources:
                        clipped = [_clip_text(s, 80) for s in sources if s]
                        src_text = f"（来源：{', '.join(clipped)}）" if clipped else ""
                    text = desc or json.dumps(item, ensure_ascii=False)
                    lines.append(f"{idx}. {_clip_text(text, 260)}{src_text}")
                else:
                    lines.append(f"{idx}. {_clip_text(item, 260)}")

        if gaps:
            lines.append("### 证据缺口")
            for item in gaps:
                if isinstance(item, dict):
                    desc = str(item.get("description") or item.get("gap") or "").strip()
                    if desc:
                        lines.append(f"- {_clip_text(desc, 240)}")
                else:
                    text = str(item).strip()
                    if text:
                        lines.append(f"- {_clip_text(text, 240)}")

        if confidence:
            lines.append("### 置信度")
            label_map = {
                "tech": "技术面",
                "fund": "基本面",
                "news": "消息面",
                "overall": "综合",
            }
            for key in ("tech", "fund", "news", "overall"):
                if key in confidence:
                    lines.append(f"- {label_map.get(key, key)}：{confidence.get(key)}")
            for key, val in confidence.items():
                if key in label_map:
                    continue
                lines.append(f"- {key}：{val}")

        if len(lines) > 1:
            return "\n".join(lines)

    if critic_text:
        lines.append("### 复核说明")
        for raw in critic_text.splitlines():
            text = raw.strip()
            if text:
                lines.append(f"- {_clip_text(text, 260)}")
        return "\n".join(lines)

    return ""


def _build_evidence_bundle(
    symbol: str,
    tech_payload: Optional[Dict[str, Any]],
    fundamentals_payload: Optional[Dict[str, Any]],
    news_items: Optional[List[Any]],
    web_items: Optional[List[Dict[str, Any]]],
    relevance_terms: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {
        "news_db": [],
        "news_web": [],
        "tech": [],
        "fund": [],
    }
    max_total = 6000
    max_fund_per_module = 400
    terms = _unique_terms(relevance_terms)
    total_count = 0

    def _add_evidence(ev_type: str, title: str, summary: str, time_val: str, source: str, url: str) -> None:
        nonlocal total_count
        if total_count >= max_total:
            return
        text_blob = f"{title} {summary} {source}"
        score = _score_text(text_blob, terms)
        dt = _parse_datetime_value(time_val)
        sort_ts = dt.timestamp() if dt else 0.0
        buckets.setdefault(ev_type, []).append({
            "type": ev_type,
            "time": time_val or "",
            "title": title or "",
            "summary": summary or "",
            "source": source or "",
            "url": url or "",
            "symbol": symbol,
            "score": score,
            "sort_ts": sort_ts,
        })
        total_count += 1

    for item in news_items or []:
        data = vars(item) if hasattr(item, "__dict__") else item
        if not isinstance(data, dict):
            continue
        title = data.get("title") or data.get("content") or ""
        summary = _clip_text(data.get("content") or data.get("title") or "")
        time_val = str(data.get("published_at") or "")
        source = str(data.get("source_table") or data.get("platform") or "news_db")
        url = str(data.get("url") or "")
        _add_evidence("news_db", title, summary, time_val, source, url)

    for item in web_items or []:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or ""
        summary = _clip_text(item.get("snippet") or "")
        url = str(item.get("url") or "")
        _add_evidence("news_web", title, summary, "", "bocha", url)

    if isinstance(tech_payload, dict):
        for timeframe, records in tech_payload.items():
            if not isinstance(records, list):
                continue
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                time_val = str(rec.get("date") or rec.get("dt") or rec.get("time") or "")
                close = rec.get("close")
                volume = rec.get("volume")
                rsi = rec.get("rsi14")
                macd = rec.get("macd_hist")
                title = f"{timeframe} close={close} volume={volume}"
                summary = f"rsi14={rsi}, macd_hist={macd}"
                _add_evidence("tech", title, summary, time_val, f"tech_{timeframe}", "")

    if isinstance(fundamentals_payload, dict):
        for module, records in fundamentals_payload.items():
            if isinstance(records, list):
                for rec in records[:max_fund_per_module]:
                    if not isinstance(rec, dict):
                        continue
                    time_val = str(rec.get("statDate") or rec.get("date") or rec.get("year") or "")
                    summary = _clip_text(rec)
                    _add_evidence("fund", module, summary, time_val, f"fund_{module}", "")
            elif isinstance(records, dict):
                time_val = str(records.get("statDate") or records.get("date") or records.get("year") or "")
                summary = _clip_text(records)
                _add_evidence("fund", module, summary, time_val, f"fund_{module}", "")
    ordered: List[Dict[str, Any]] = []
    for key in ("news_db", "news_web", "tech", "fund"):
        bucket = buckets.get(key, [])
        bucket.sort(key=lambda item: (item.get("score", 0.0), item.get("sort_ts", 0.0)), reverse=True)
        ordered.extend(bucket)

    for idx, item in enumerate(ordered, start=1):
        item["id"] = f"E{idx:04d}"
        item.pop("sort_ts", None)

    return ordered


def _format_evidence_index(evidence: List[Dict[str, Any]], max_items: int = 300, max_chars: int = 15000) -> str:
    if not evidence:
        return ""
    lines: List[str] = []
    for item in evidence[:max_items]:
        line = (
            f"{item.get('id')} | {item.get('type')} | {item.get('time')} | "
            f"{_clip_text(item.get('title'), 60)} | {_clip_text(item.get('summary'), 120)}"
        )
        lines.append(line)
        if sum(len(l) for l in lines) > max_chars:
            break
    return "\n".join(lines)


def _summarize_intraday_minutes(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {}

    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    open_val = None
    close_val = None
    high_val = None
    low_val = None
    volume_sum = 0.0
    start_dt = None
    end_dt = None

    for idx, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue
        if open_val is None:
            open_val = _to_float(rec.get("open"))
        close_candidate = _to_float(rec.get("close"))
        if close_candidate is not None:
            close_val = close_candidate
        high_candidate = _to_float(rec.get("high"))
        low_candidate = _to_float(rec.get("low"))
        if high_candidate is not None:
            high_val = high_candidate if high_val is None else max(high_val, high_candidate)
        if low_candidate is not None:
            low_val = low_candidate if low_val is None else min(low_val, low_candidate)
        vol = _to_float(rec.get("volume"))
        if vol is not None:
            volume_sum += vol
        dt = _parse_datetime_value(rec.get("date") or rec.get("dt") or rec.get("day") or rec.get("time"))
        if dt:
            if start_dt is None or dt < start_dt:
                start_dt = dt
            if end_dt is None or dt > end_dt:
                end_dt = dt

    pct_change = None
    if open_val is not None and close_val is not None and open_val != 0:
        try:
            pct_change = (close_val - open_val) / open_val * 100.0
        except Exception:
            pct_change = None

    return {
        "count": len(records),
        "open": open_val,
        "close": close_val,
        "high": high_val,
        "low": low_val,
        "volume": volume_sum,
        "start": start_dt,
        "end": end_dt,
        "pct_change": pct_change,
    }


def _summarize_ticks(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {}

    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    side_counts: Dict[str, int] = {}
    total_volume = 0.0
    last_price = None
    last_time = ""

    for rec in records:
        if not isinstance(rec, dict):
            continue
        side = str(rec.get("side") or "").strip()
        if side:
            side_counts[side] = side_counts.get(side, 0) + 1
        vol = _to_float(rec.get("volume") or rec.get("size"))
        if vol is not None:
            total_volume += vol
        price = _to_float(rec.get("price") or rec.get("close"))
        if price is not None:
            last_price = price
        time_val = rec.get("time") or rec.get("date")
        if time_val:
            last_time = str(time_val)

    return {
        "count": len(records),
        "total_volume": total_volume,
        "side_counts": side_counts,
        "last_price": last_price,
        "last_time": last_time,
    }


def _format_realtime_snapshot(tech_payload: Dict[str, Any]) -> str:
    if not isinstance(tech_payload, dict):
        return ""

    def _fmt(value: Any, digits: int = 2) -> str:
        if value is None:
            return "-"
        try:
            return f"{float(value):.{digits}f}"
        except Exception:
            return str(value)

    def _fmt_range(start_dt: Optional[datetime], end_dt: Optional[datetime]) -> str:
        if not start_dt or not end_dt:
            return "-"
        if start_dt.date() == end_dt.date():
            return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
        return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%Y-%m-%d %H:%M')}"

    lines = ["## 实时与当日数据补充"]

    rt = None
    rt_records = tech_payload.get("realtime")
    if isinstance(rt_records, list) and rt_records:
        if isinstance(rt_records[0], dict):
            rt = rt_records[0]
    if rt:
        lines.append(
            "- 实时行情：最新价 {price}，今开 {open}，最高 {high}，最低 {low}，"
            "涨跌幅 {pct}% ，量比 {ratio}，5分钟涨跌 {chg5}%，更新时间 {time}".format(
                price=_fmt(rt.get("price")),
                open=_fmt(rt.get("open")),
                high=_fmt(rt.get("high")),
                low=_fmt(rt.get("low")),
                pct=_fmt(rt.get("change_pct")),
                ratio=_fmt(rt.get("volume_ratio")),
                chg5=_fmt(rt.get("change_5m")),
                time=rt.get("time") or "-",
            )
        )

    minute_summary = _summarize_intraday_minutes(tech_payload.get("1m") or [])
    if minute_summary:
        lines.append(
            "- 当日1分钟线：区间 {range}，开盘 {open}，收盘 {close}，最高 {high}，最低 {low}，"
            "成交量 {volume}，涨跌幅 {pct}%（{count} 条）".format(
                range=_fmt_range(minute_summary.get("start"), minute_summary.get("end")),
                open=_fmt(minute_summary.get("open")),
                close=_fmt(minute_summary.get("close")),
                high=_fmt(minute_summary.get("high")),
                low=_fmt(minute_summary.get("low")),
                volume=_fmt(minute_summary.get("volume"), digits=0),
                pct=_fmt(minute_summary.get("pct_change")),
                count=minute_summary.get("count", 0),
            )
        )

    tick_summary = _summarize_ticks(tech_payload.get("ticks") or [])
    if tick_summary:
        sides = tick_summary.get("side_counts") or {}
        lines.append(
            "- 高频成交样本：笔数 {count}，买盘 {buy}，卖盘 {sell}，中性 {neutral}，"
            "样本手数 {volume}，最新价 {price}，最新时间 {time}".format(
                count=tick_summary.get("count", 0),
                buy=sides.get("买盘", 0),
                sell=sides.get("卖盘", 0),
                neutral=sides.get("中性盘", 0),
                volume=_fmt(tick_summary.get("total_volume"), digits=0),
                price=_fmt(tick_summary.get("last_price")),
                time=tick_summary.get("last_time") or "-",
            )
        )

    if len(lines) <= 1:
        return ""
    return "\n".join(lines)


def _build_realtime_ai_insight(llm_client: Any, symbol: str, tech_payload: Dict[str, Any]) -> str:
    if not llm_client or not isinstance(tech_payload, dict):
        return ""

    def _safe_iso(value: Any) -> Any:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        return value

    def _tail(records: Any, limit: int, fields: List[str]) -> List[Dict[str, Any]]:
        if not isinstance(records, list) or limit <= 0:
            return []
        result: List[Dict[str, Any]] = []
        for rec in records[-limit:]:
            if not isinstance(rec, dict):
                continue
            slim = {key: rec.get(key) for key in fields if key in rec}
            if slim:
                result.append(slim)
        return result

    realtime = None
    rt_records = tech_payload.get("realtime") or []
    if isinstance(rt_records, list) and rt_records:
        if isinstance(rt_records[0], dict):
            realtime = rt_records[0]

    minute_records = tech_payload.get("1m") or []
    premarket_records = tech_payload.get("premarket") or []
    tick_records = tech_payload.get("ticks") or []

    minute_summary = _summarize_intraday_minutes(minute_records)
    premarket_summary = _summarize_intraday_minutes(premarket_records)
    tick_summary = _summarize_ticks(tick_records)

    minute_summary["start"] = _safe_iso(minute_summary.get("start"))
    minute_summary["end"] = _safe_iso(minute_summary.get("end"))
    premarket_summary["start"] = _safe_iso(premarket_summary.get("start"))
    premarket_summary["end"] = _safe_iso(premarket_summary.get("end"))

    payload = {
        "symbol": symbol,
        "realtime": realtime or {},
        "intraday_1m_summary": minute_summary,
        "intraday_1m_recent": _tail(minute_records, 12, ["date", "open", "high", "low", "close", "volume", "rsi14", "macd_hist"]),
        "premarket_summary": premarket_summary,
        "premarket_recent": _tail(premarket_records, 12, ["date", "open", "high", "low", "close", "volume", "amount", "price"]),
        "ticks_summary": tick_summary,
        "ticks_sample": _tail(tick_records, 20, ["time", "price", "volume", "side"]),
    }

    system_prompt = (
        "你是交易员，只基于当日实时/分时/分笔数据进行技术面解读。"
        "重点观察日内节奏、波动/振幅、量能变化、买卖盘结构与盘前差异，避免长周期结论。"
        "输出 3-6 条中文要点，使用项目符号。"
    )
    user_prompt = json.dumps(payload, ensure_ascii=False)

    try:
        if hasattr(llm_client, "stream_invoke_to_string"):
            text = llm_client.stream_invoke_to_string(system_prompt, user_prompt, temperature=0.2)
        elif hasattr(llm_client, "invoke"):
            text = llm_client.invoke(system_prompt, user_prompt, temperature=0.2)
        else:
            return ""
    except Exception:
        return ""

    text = str(text or "").strip()
    if not text:
        return ""
    if not text.startswith("##"):
        text = "## 日内分时解读\n" + text
    return text


def _collect_tech_materials(symbol: str, include_realtime: bool = True) -> Dict[str, Any]:
    timeframes = {
        "day": {"timeframe": "DAY", "limit": 200},
        "week": {"timeframe": "WEEK", "limit": 260},
        "month": {"timeframe": "MON", "limit": 360},
        "60m": {"timeframe": "60M", "limit": 80},
    }
    payload: Dict[str, Any] = {}
    client = FinanceMCPClient()
    for key, spec in timeframes.items():
        try:
            ohlc = client.get_ohlc(
                symbol=symbol,
                timeframe=spec["timeframe"],
                limit=spec["limit"],
            )
            df = client._ohlc_dict_to_df(ohlc)
            ind_df = client._compute_indicators_from_df(df)
            if spec["limit"] > 0 and len(ind_df) > spec["limit"]:
                ind_df = ind_df.tail(spec["limit"])
            payload[key] = _df_to_records(ind_df)
        except Exception:
            payload[key] = []

    if include_realtime:
        realtime = client.get_realtime_quote(symbol)
        if isinstance(realtime, dict) and realtime.get("has_data") and isinstance(realtime.get("data"), dict):
            payload["realtime"] = [_serialize_record(realtime.get("data") or {})]
        else:
            payload["realtime"] = []

        intraday_min = client.get_intraday_minute(symbol=symbol, period="1", adjust="", limit=480)
        minute_records: List[Dict[str, Any]] = []
        if isinstance(intraday_min, dict) and intraday_min.get("has_data"):
            try:
                pd = __import__("pandas")
                df = pd.DataFrame(intraday_min.get("data") or [])
                if not df.empty:
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    elif "day" in df.columns:
                        df["date"] = pd.to_datetime(df["day"], errors="coerce")
                    for col in ("open", "high", "low", "close", "volume"):
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna(subset=["date"]).sort_values("date")
                    required_cols = {"open", "high", "low", "close", "volume"}
                    if not required_cols.issubset(df.columns):
                        minute_records = _serialize_records(intraday_min.get("data") or [])
                    else:
                        ind_df = client._compute_indicators_from_df(df)
                        if len(ind_df) > 480:
                            ind_df = ind_df.tail(480)
                        minute_records = _df_to_records(ind_df)
            except Exception:
                minute_records = _serialize_records(intraday_min.get("data") or [])
        payload["1m"] = minute_records

        intraday_ticks = client.get_intraday_ticks(symbol=symbol, limit=240)
        tick_records = _serialize_records(intraday_ticks.get("data") or []) if isinstance(intraday_ticks, dict) else []
        for rec in tick_records:
            if "close" not in rec and "price" in rec:
                rec["close"] = rec.get("price")
        payload["ticks"] = tick_records

        premarket = client.get_premarket_minute_em(symbol=symbol, limit=480)
        if isinstance(premarket, dict) and premarket.get("has_data"):
            payload["premarket"] = _serialize_records(premarket.get("data") or [])
        else:
            payload["premarket"] = []
    else:
        payload["realtime"] = []
        payload["1m"] = []
        payload["ticks"] = []
        payload["premarket"] = []

    return payload


def _build_tech_charts(symbol: str, charts_dir: str, include_realtime: bool = True) -> Dict[str, str]:
    chart_paths = {}
    client = FinanceMCPClient()
    try:
        day_chart = client.plot_technical_charts(
            symbol=symbol,
            timeframe="DAY",
            limit=200,
            save_path=os.path.join(charts_dir, f"{symbol}_tech_day.png"),
            show=False,
        )
        chart_paths["日线技术图"] = day_chart
    except Exception:
        pass
    try:
        weekly_chart = client.plot_technical_charts(
            symbol=symbol,
            timeframe="WEEK",
            limit=260,
            save_path=os.path.join(charts_dir, f"{symbol}_week.png"),
            show=False,
        )
        chart_paths["周线技术图"] = weekly_chart
    except Exception:
        pass
    try:
        monthly_chart = client.plot_technical_charts(
            symbol=symbol,
            timeframe="MON",
            limit=360,
            save_path=os.path.join(charts_dir, f"{symbol}_month.png"),
            show=False,
        )
        chart_paths["月线技术图"] = monthly_chart
    except Exception:
        pass
    try:
        intraday_chart = client.plot_technical_charts(
            symbol=symbol,
            timeframe="60M",
            limit=80,
            save_path=os.path.join(charts_dir, f"{symbol}_60m.png"),
            show=False,
        )
        chart_paths["60分钟线"] = intraday_chart
    except Exception:
        pass
    if include_realtime:
        try:
            intraday_min_chart = client.plot_intraday_minute_chart(
                symbol=symbol,
                save_path=os.path.join(charts_dir, f"{symbol}_intraday_min.png"),
                show=False,
            )
            chart_paths["日内分时图"] = intraday_min_chart
        except Exception:
            pass
        try:
            intraday_tick_chart = client.plot_intraday_tick_chart(
                symbol=symbol,
                save_path=os.path.join(charts_dir, f"{symbol}_intraday_tick.png"),
                show=False,
            )
            chart_paths["分笔成交图"] = intraday_tick_chart
        except Exception:
            pass
    return chart_paths


def _analyze_fundamentals(agent: Any, symbol: str) -> Dict[str, Any]:
    fundamental_md = ""
    visuals = None
    fundamentals_payload = None
    if hasattr(agent, "analyze_stock_fundamental_multimodal"):
        try:
            fundamental_md, visuals, fundamentals_payload = agent.analyze_stock_fundamental_multimodal(symbol)
        except Exception:
            fundamental_md = ""
            visuals = None
            fundamentals_payload = None
    elif hasattr(agent, "analyze_stock_fundamental_multiagent"):
        try:
            fundamental_md = agent.analyze_stock_fundamental_multiagent(symbol)
            if hasattr(agent, "_build_fundamental_payload"):
                fundamentals_payload = agent._build_fundamental_payload(symbol)
            else:
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
        except Exception:
            fundamental_md = ""
            visuals = None
            fundamentals_payload = None
    else:
        fundamentals_payload = {}
        try:
            client = FinanceMCPClient()
            raw = client.get_fundamentals(symbol)
            fundamentals_payload = raw.get("fundamentals", {}) if isinstance(raw, dict) else {}
        except Exception:
            fundamentals_payload = {}
        if hasattr(agent, "_synthesize_fundamental_analysis"):
            try:
                profile = {}
                if hasattr(agent, "finance_tool") and hasattr(agent.finance_tool, "fetch_stock_profile"):
                    try:
                        profile = agent.finance_tool.fetch_stock_profile(symbol)
                    except Exception:
                        profile = {}
                fundamental_md = agent._synthesize_fundamental_analysis(symbol, fundamentals_payload or {}, profile or {}, "", "")
            except Exception:
                fundamental_md = generate_basic_fundamental_md(fundamentals_payload or {}, symbol)
        else:
            fundamental_md = generate_basic_fundamental_md(fundamentals_payload or {}, symbol)

    if (not fundamental_md or "失败" in fundamental_md) and fundamentals_payload:
        try:
            generated_md = generate_basic_fundamental_md(fundamentals_payload or {}, symbol)
            fundamental_md = generated_md if not fundamental_md else (fundamental_md + "\n\n" + generated_md).strip()
        except Exception:
            pass

    return {
        "fundamental_md": fundamental_md or "",
        "visuals": visuals,
        "fundamentals_payload": fundamentals_payload or {},
    }


def _render_fundamentals(
    agent: Any,
    symbol: str,
    stock_name: str,
    report_dir: str,
    charts_dir: str,
    fundamental_md: str,
    visuals: Any,
    fundamentals_payload: Dict[str, Any],
    log: LogFn = None,
) -> Dict[str, Any]:
    fundamental_charts: List[Dict[str, str]] = []
    simple_debug = None
    renderer_used = "none"
    renderer_error = None

    if HAS_FUNDAMENTAL_RENDERER and visuals is not None and getattr(visuals, "charts", None):
        try:
            fundamental_charts = render_charts(symbol, visuals, fundamentals_payload or {}, charts_dir)
            renderer_used = "visual_renderer"
        except Exception as e:
            renderer_used = "visual_renderer_failed"
            renderer_error = repr(e)
            fundamental_charts = []
    else:
        if fundamentals_payload:
            try:
                fundamental_charts, simple_debug = render_simple_charts(symbol, fundamentals_payload or {}, charts_dir)
                renderer_used = "simple_renderer"
            except Exception as e:
                renderer_used = "simple_renderer_failed"
                renderer_error = repr(e)
                fundamental_charts = []
        else:
            renderer_used = "no_fundamentals_payload"
            fundamental_charts = []

    try:
        dump_json(os.path.join(report_dir, "debug_chart_files.json"), verify_chart_files(fundamental_charts))
        dump_json(os.path.join(report_dir, "debug_render.json"), {
            "symbol": symbol,
            "renderer_used": renderer_used,
            "renderer_error": renderer_error,
            "fundamental_charts_count": len(fundamental_charts),
            "simple_debug": simple_debug,
        })
    except Exception:
        pass

    if not fundamental_charts:
        try:
            existing = gather_existing_charts(symbol, charts_dir)
            repo_charts_dir = os.path.join(os.getcwd(), "charts")
            existing_repo = []
            try:
                existing_repo = gather_existing_charts(symbol, repo_charts_dir)
            except Exception:
                existing_repo = []
            paths = set((ch.get("path") for ch in (fundamental_charts or []) if ch.get("path")))
            for ch in (existing + existing_repo):
                p = ch.get("path")
                if p and p not in paths and os.path.exists(p):
                    fundamental_charts.append(ch)
                    paths.add(p)
        except Exception:
            pass

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

    macro_debug = {"ok": False, "reason": "not_built"}
    macro_evidence_html = ""
    try:
        client = FinanceMCPClient()
        pd = __import__("pandas")

        def _has_cols(df: Any, cols: List[str]) -> bool:
            return isinstance(df, pd.DataFrame) and not df.empty and all(c in df.columns for c in cols)

        def _has_summary(df: Any) -> bool:
            return _has_cols(df, ["date", "close"])

        def _has_ohlc(df: Any) -> bool:
            return _has_cols(df, ["date", "open", "high", "low", "close"])

        stock_ctx = client.get_ohlc(symbol=symbol, timeframe="DAY", limit=240)
        stock_df = _normalize_ohlc_df(ohlc_ctx_to_df(stock_ctx))

        hs300_ctx = client.get_ohlc(symbol="000300.SH", timeframe="DAY", limit=240)
        hs300_data = (hs300_ctx.get("data", []) if isinstance(hs300_ctx, dict) else (hs300_ctx or []))
        hs300_df = _normalize_ohlc_df(pd.DataFrame(hs300_data))
        hs300_sum = index_summary(hs300_df, date_col="date", close_col="close")

        hs300_png = None
        if _has_ohlc(hs300_df) and _has_ohlc(stock_df):
            hs300_png = plot_kline_compare(
                top_df=hs300_df,
                bottom_df=stock_df,
                top_title="CSI300（沪深300）日线K",
                bottom_title=f"{symbol} 日线K（对比）",
                out_path=os.path.join(charts_dir, f"hs300_vs_{symbol}_day_k.png"),
                date_col="date",
            )

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
        sw_kind = sw_meta.get("index_kind") or ("sw" if re.search(r"^8\d{5}$", str(sw_code or "")) else "index")
        sw_label = "SW 行业指数" if sw_kind == "sw" else "行业指数"

        sw_day = ((sw_ctx.get("index_daily") or {}).get("data", [])) if isinstance(sw_ctx, dict) else []
        sw_df = _normalize_ohlc_df(pd.DataFrame(sw_day or []))
        sw_source = (
            sw_meta.get("fetch_source")
            or (sw_ctx.get("meta") or {}).get("matched_index", {}).get("fetch_source")
            or (sw_ctx.get("meta") or {}).get("source")
            or "mcp"
        )

        if not _has_summary(sw_df):
            profile = {}
            try:
                if agent and hasattr(agent, "finance_tool") and hasattr(agent.finance_tool, "fetch_stock_profile"):
                    profile = agent.finance_tool.fetch_stock_profile(symbol)
            except Exception:
                profile = {}

            resolved_name = stock_name or ""
            if not resolved_name and isinstance(profile, dict):
                for key in ("code_name", "name", "shortName", "short_name", "company_name", "stock_name"):
                    val = profile.get(key)
                    if isinstance(val, str) and val.strip():
                        resolved_name = val.strip()
                        break

            if not sw_code or sw_kind != "sw":
                inferred = _infer_sw_industry_by_llm(
                    getattr(agent, "llm_client", None),
                    symbol,
                    resolved_name,
                    profile,
                )
                if inferred:
                    sw_code = inferred.get("code")
                    sw_name = inferred.get("name") or sw_name
                    sw_source = "llm"
                    sw_kind = "sw"
                    sw_label = "SW 行业指数"

            if sw_code:
                sw_df = _normalize_ohlc_df(_fetch_sw_index_df(sw_code))
                if _has_summary(sw_df):
                    sw_source = "akshare"

        sw_sum = index_summary(sw_df, date_col="date", close_col="close")

        sw_code_slug = re.sub(r"[^0-9A-Za-z]+", "", str(sw_code)) if sw_code else ""
        sw_png = None
        if _has_ohlc(sw_df) and _has_ohlc(stock_df):
            sw_png = plot_kline_compare(
                top_df=sw_df,
                bottom_df=stock_df,
                top_title=f"{sw_label}日线K：{sw_name} ({sw_code})",
                bottom_title=f"{symbol} 日线K（对比）",
                out_path=os.path.join(charts_dir,
                                      f"sw_{sw_code_slug}_vs_{symbol}_day_k.png" if sw_code_slug else f"sw_vs_{symbol}_day_k.png"),
                date_col="date",
            )

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
                "source": sw_source,
                "kind": sw_kind,
            }
        }
        dump_json(os.path.join(report_dir, "debug_payload.json"), macro_debug)

        def _sum_row(name: str, s: Dict[str, Any]) -> str:
            if not s or not s.get("ok"):
                return f"<tr><td>{name}</td><td colspan='5'>获取失败：{(s or {}).get('reason')}</td></tr>"
            return (
                f"<tr><td>{name}</td>"
                f"<td>{s['start_date']}</td>"
                f"<td>{s['end_date']}</td>"
                f"<td>{int(s['points'])}</td>"
                f"<td>{float(s['start_close']):.2f} → {float(s['end_close']):.2f}</td>"
                f"<td>{float(s['return_pct']):.2f}%</td></tr>"
            )

        macro_evidence_html = "<div class='section'><h2>宏观与行业指数（硬证据）</h2>"
        macro_evidence_html += "<p>以下为程序实际取数与绘图结果；若无图/summary，则代表数据未成功获取。</p>"
        macro_evidence_html += """
        <table>
          <thead><tr><th>指数</th><th>起始</th><th>结束</th><th>样本数</th><th>收盘价</th><th>区间涨跌幅</th></tr></thead>
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
    except Exception as e:
        macro_debug = {"ok": False, "reason": "exception", "error": repr(e)}
        try:
            dump_json(os.path.join(report_dir, "debug_payload.json"), macro_debug)
        except Exception:
            pass
        macro_evidence_html = (
            "<div class='section'><h2>宏观与行业指数（硬证据）</h2>"
            f"<p>构建失败：{repr(e)}</p></div>"
        )

    if macro_evidence_html:
        fundamental_md = (macro_evidence_html + "\n\n" + (fundamental_md or "")).strip()

    return {
        "fundamental_md": fundamental_md or "",
        "fundamental_charts": fundamental_charts,
        "tables_html": tables_html,
    }


def _analyze_news_bundle(
    llm_client: Any,
    keyword_llm: Optional[Any],
    symbol: str,
    stock_name: str,
    news_days: int,
    use_bocha: bool,
    report_dir: str,
    db_name: Optional[str] = None,
) -> Dict[str, Any]:
    news_md = ""
    message_result: Dict[str, Any] = {}
    try:
        message_agent = MessageAnalysisAgent(
            llm_client,
            keyword_llm=keyword_llm,
            db_name=db_name,
        )
        message_result = message_agent.analyze(
            stock_code=symbol,
            stock_name=stock_name,
            days=news_days,
            use_bocha=use_bocha,
        )
        news_md = message_result.get("analysis_md") or ""

        input_keywords = message_result.get("keywords") or []
        items = message_result.get("items") or []
        web_items = message_result.get("web_items") or []
        inputs_html = build_message_inputs_html(
            input_keywords,
            items if items else (message_result.get("prompt_materials") or ""),
            web_items if web_items else (message_result.get("prompt_web_materials") or ""),
        )
        if inputs_html:
            news_md = (news_md + "\n\n" + inputs_html).strip()

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
        news_md = f"**News analysis failed: {e}**"
        message_result = {
            "analysis_md": news_md,
            "keywords": [],
            "items": [],
            "web_items": [],
            "prompt_materials": "",
            "prompt_web_materials": "",
            "db_error": str(e),
        }
    message_result["analysis_md"] = news_md
    return message_result


def _analyze_news(
    llm_client: Any,
    keyword_llm: Optional[Any],
    symbol: str,
    stock_name: str,
    news_days: int,
    use_bocha: bool,
    report_dir: str,
    db_name: Optional[str] = None,
) -> str:
    bundle = _analyze_news_bundle(
        llm_client=llm_client,
        keyword_llm=keyword_llm,
        symbol=symbol,
        stock_name=stock_name,
        news_days=news_days,
        use_bocha=use_bocha,
        report_dir=report_dir,
        db_name=db_name,
    )
    return bundle.get("analysis_md") or ""


def run_report(
    symbol: str,
    report_mode: str = "combined",
    report_modes: Optional[List[str]] = None,
    news_days: int = 7,
    use_bocha: bool = False,
    include_realtime: bool = True,
    output_dir: Optional[str] = None,
    agent: Optional[Any] = None,
    agent_overrides: Optional[Dict[str, Any]] = None,
    db_cfg: Optional[DBConfig] = None,
    log: LogFn = None,
) -> Dict[str, str]:
    symbol = (symbol or "").strip()
    if not symbol:
        raise ValueError("symbol is required")

    modes = _normalize_report_modes(report_mode, report_modes)
    if not modes:
        raise ValueError("report_modes is empty")
    run_combined = "combined" in modes
    run_tech = "tech" in modes or run_combined
    run_fund = "fund" in modes or run_combined
    run_news = "news" in modes or run_combined

    normalized_overrides = _normalize_agent_overrides(agent_overrides)
    combined_overrides = normalized_overrides.get("combined")
    fund_overrides = normalized_overrides.get("fund")
    tech_overrides = normalized_overrides.get("tech")
    news_overrides = normalized_overrides.get("news") or normalized_overrides.get("message")
    default_insight = combined_overrides or fund_overrides or tech_overrides
    if not tech_overrides:
        tech_overrides = default_insight
    if not fund_overrides:
        fund_overrides = default_insight
    if not combined_overrides:
        combined_overrides = default_insight

    main_overrides = combined_overrides or fund_overrides or tech_overrides
    main_config = _build_insight_settings(main_overrides, db_cfg, output_dir)
    agent_main = agent or _init_agent(main_config)
    out_dir = output_dir or (getattr(getattr(agent_main, "config", None), "OUTPUT_DIR", None) or "reports")
    report_dir = os.path.join(out_dir, symbol)
    charts_dir = os.path.join(report_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    stock_name = ""
    try:
        if hasattr(agent_main, "finance_tool") and hasattr(agent_main.finance_tool, "fetch_stock_profile"):
            profile = agent_main.finance_tool.fetch_stock_profile(symbol)
            stock_name = _extract_stock_name(profile)
    except Exception:
        stock_name = ""

    tech_md = ""
    chart_paths: Dict[str, str] = {}
    fundamental_md = ""
    fundamental_charts: List[Dict[str, str]] = []
    tables_html: List[str] = []
    news_md = ""
    tech_config = _build_insight_settings(tech_overrides, db_cfg, output_dir)
    fund_config = _build_insight_settings(fund_overrides, db_cfg, output_dir)
    combined_config = _build_insight_settings(combined_overrides, db_cfg, output_dir)
    query_llm = None
    orchestrator_info: Optional[Dict[str, Any]] = None
    critic_info: Optional[Dict[str, Any]] = None
    evidence_items: List[Dict[str, Any]] = []
    evidence_text = ""

    if run_tech or run_fund or run_news:
        _emit_log(log, "orchestrator", "running", "Orchestrator planning started")
        try:
            orchestrator_info = _build_orchestrator_plan(
                symbol=symbol,
                stock_name=stock_name,
                modes=modes,
                news_days=news_days,
                use_bocha=use_bocha,
                llm_client=getattr(agent_main, "llm_client", None),
            )
            _emit_log(log, "orchestrator", "done", "Orchestrator planning done")
            try:
                materials_dir = os.path.join(report_dir, "materials")
                os.makedirs(materials_dir, exist_ok=True)
                dump_json(
                    os.path.join(materials_dir, f"{symbol}_orchestrator.json"),
                    orchestrator_info or {},
                )
            except Exception:
                pass
        except Exception as exc:
            _emit_log(log, "orchestrator", "error", f"Orchestrator planning failed: {exc}")

    def _run_tech_task() -> Dict[str, Any]:
        local_agent = _init_agent(tech_config)
        local_md = ""
        tech_payload: Dict[str, Any] = {}
        try:
            local_md = local_agent.analyze_stock(symbol=symbol, mode="tech")
        except Exception as exc:
            local_md = f"**Technical analysis failed: {exc}**"
        with _CHART_LOCK:
            local_charts = _build_tech_charts(symbol, charts_dir, include_realtime=include_realtime)
        try:
            materials_dir = os.path.join(report_dir, "materials")
            os.makedirs(materials_dir, exist_ok=True)
            tech_payload = _collect_tech_materials(symbol, include_realtime=include_realtime)
            dump_json(os.path.join(materials_dir, f"{symbol}_tech_data.json"), tech_payload)
            if include_realtime:
                snapshot_md = _format_realtime_snapshot(tech_payload)
                if snapshot_md:
                    local_md = (local_md + "\n\n" + snapshot_md).strip()
                realtime_ai_md = _build_realtime_ai_insight(getattr(local_agent, "llm_client", None), symbol, tech_payload)
                if realtime_ai_md:
                    local_md = (local_md + "\n\n" + realtime_ai_md).strip()
        except Exception:
            pass
        return {"tech_md": local_md, "chart_paths": local_charts, "tech_payload": tech_payload}

    def _run_fund_task() -> Dict[str, Any]:
        local_agent = _init_agent(fund_config)
        fund_result = {
            "fundamental_md": "",
            "visuals": None,
            "fundamentals_payload": {},
        }
        try:
            fund_result = _analyze_fundamentals(local_agent, symbol)
        except Exception as exc:
            fund_result = {
                "fundamental_md": f"**Fundamental analysis failed: {exc}**",
                "visuals": None,
                "fundamentals_payload": {},
            }
        try:
            materials_dir = os.path.join(report_dir, "materials")
            os.makedirs(materials_dir, exist_ok=True)
            dump_json(
                os.path.join(materials_dir, f"{symbol}_fundamentals_payload.json"),
                fund_result.get("fundamentals_payload") or {},
            )
        except Exception:
            pass
        with _CHART_LOCK:
            rendered = _render_fundamentals(
                agent=local_agent,
                symbol=symbol,
                stock_name=stock_name,
                report_dir=report_dir,
                charts_dir=charts_dir,
                fundamental_md=fund_result["fundamental_md"],
                visuals=fund_result["visuals"],
                fundamentals_payload=fund_result["fundamentals_payload"],
                log=log,
            )
        rendered["fundamentals_payload"] = fund_result.get("fundamentals_payload") or {}
        return rendered

    def _run_news_task() -> Dict[str, Any]:
        if query_llm:
            bundle = _analyze_news_bundle(
                llm_client=query_llm,
                keyword_llm=query_llm,
                symbol=symbol,
                stock_name=stock_name,
                news_days=news_days,
                use_bocha=use_bocha,
                report_dir=report_dir,
                db_name=db_cfg.database if db_cfg else None,
            )
            return {"news_md": bundle.get("analysis_md") or "", "message_result": bundle}
        local_agent = _init_agent(main_config)
        bundle = _analyze_news_bundle(
            llm_client=getattr(local_agent, "llm_client", None),
            keyword_llm=None,
            symbol=symbol,
            stock_name=stock_name,
            news_days=news_days,
            use_bocha=use_bocha,
            report_dir=report_dir,
            db_name=db_cfg.database if db_cfg else None,
        )
        return {"news_md": bundle.get("analysis_md") or "", "message_result": bundle}

    tech_payload: Dict[str, Any] = {}
    fundamentals_payload: Dict[str, Any] = {}
    news_bundle: Dict[str, Any] = {}

    tasks: Dict[str, Any] = {}
    if run_tech:
        _emit_log(log, "tech", "running", "Technical analysis started")
    if run_fund:
        _emit_log(log, "fund", "running", "Fundamental analysis started")
    if run_news:
        _emit_log(log, "news", "running", "News analysis started")
    with ThreadPoolExecutor(max_workers=3) as executor:
        if run_tech:
            tasks["tech"] = executor.submit(_run_tech_task)
        if run_fund:
            tasks["fund"] = executor.submit(_run_fund_task)
        if run_news:
            tasks["news"] = executor.submit(_run_news_task)

        for name, fut in tasks.items():
            try:
                result = fut.result()
                if name == "tech":
                    tech_md = result.get("tech_md", "")
                    chart_paths = result.get("chart_paths", {})
                    tech_payload = result.get("tech_payload") or {}
                    _emit_log(log, "tech", "done", "Technical analysis done")
                elif name == "fund":
                    fundamental_md = result.get("fundamental_md", "")
                    fundamental_charts = result.get("fundamental_charts", []) or []
                    tables_html = result.get("tables_html", []) or []
                    fundamentals_payload = result.get("fundamentals_payload") or {}
                    _emit_log(log, "fund", "done", "Fundamental analysis done")
                elif name == "news":
                    news_md = result.get("news_md", "")
                    news_bundle = result.get("message_result") or {}
                    _emit_log(log, "news", "done", "News analysis done")
            except Exception as exc:
                _emit_log(log, name, "error", f"{name} failed: {exc}")

    try:
        materials_dir = os.path.join(report_dir, "materials")
        os.makedirs(materials_dir, exist_ok=True)
        if tech_md:
            dump_json(os.path.join(materials_dir, f"{symbol}_tech_md.json"), {"analysis_md": tech_md})
        if fundamental_md:
            dump_json(os.path.join(materials_dir, f"{symbol}_fund_md.json"), {"analysis_md": fundamental_md})
        if news_md:
            dump_json(os.path.join(materials_dir, f"{symbol}_news_md.json"), {"analysis_md": news_md})
    except Exception:
        pass

    try:
        relevance_terms: List[str] = [symbol]
        if stock_name:
            relevance_terms.append(stock_name)
        if isinstance(news_bundle, dict):
            relevance_terms.extend(news_bundle.get("keywords") or [])
        evidence_items = _build_evidence_bundle(
            symbol=symbol,
            tech_payload=tech_payload,
            fundamentals_payload=fundamentals_payload,
            news_items=(news_bundle.get("items_all") or news_bundle.get("items")) if isinstance(news_bundle, dict) else [],
            web_items=news_bundle.get("web_items") if isinstance(news_bundle, dict) else [],
            relevance_terms=relevance_terms,
        )
        evidence_text = _format_evidence_index(evidence_items)
        materials_dir = os.path.join(report_dir, "materials")
        os.makedirs(materials_dir, exist_ok=True)
        dump_json(
            os.path.join(materials_dir, f"{symbol}_evidence.json"),
            evidence_items,
        )
    except Exception:
        evidence_items = []
        evidence_text = ""

    try:
        _write_analysis_csv(
            symbol=symbol,
            report_dir=report_dir,
            tech_payload=tech_payload,
            fundamentals_payload=fundamentals_payload,
            news_bundle=news_bundle,
            evidence_items=evidence_items,
        )
    except Exception:
        pass

    run_critic = run_combined or sum([run_tech, run_fund, run_news]) >= 2
    if run_critic:
        _emit_log(log, "critic", "running", "Critic review started")
        try:
            critic_info = _build_critic_report(
                tech_md=tech_md,
                fundamental_md=fundamental_md,
                news_md=news_md,
                evidence_text=evidence_text,
                llm_client=getattr(agent_main, "llm_client", None),
            )
            _emit_log(log, "critic", "done", "Critic review done")
            try:
                materials_dir = os.path.join(report_dir, "materials")
                os.makedirs(materials_dir, exist_ok=True)
                dump_json(
                    os.path.join(materials_dir, f"{symbol}_critic.json"),
                    critic_info or {},
                )
            except Exception:
                pass
        except Exception as exc:
            _emit_log(log, "critic", "error", f"Critic review failed: {exc}")

    overview_md = ""
    if run_combined:
        _emit_log(log, "combined", "running", "Combined report started")
        try:
            combined_agent = agent_main
            if agent is None and combined_config is not None:
                combined_agent = _init_agent(combined_config)
            critic_md = ""
            if critic_info:
                critic_md = critic_info.get("critic_md") or ""
            overview_md = build_combined_summary_md(
                symbol=symbol,
                tech_md=tech_md,
                fundamental_md=fundamental_md,
                news_md=news_md,
                llm_client=getattr(combined_agent, "llm_client", None),
                evidence_text=evidence_text,
                critic_md=critic_md,
            )
            notes = []
            if orchestrator_info:
                plan_display_md = _format_orchestrator_markdown(
                    orchestrator_info.get("plan_json"),
                    orchestrator_info.get("plan_md") or "",
                )
                if plan_display_md:
                    notes.append(plan_display_md)
            if critic_info:
                critic_display_md = _format_critic_markdown(
                    critic_info.get("critic_json"),
                    critic_info.get("critic_md") or "",
                )
                if critic_display_md:
                    notes.append(critic_display_md)
            if notes:
                overview_md = (overview_md + "\n\n" + "\n\n".join(notes)).strip()
            _emit_log(log, "combined", "done", "Combined report done")
        except Exception as exc:
            _emit_log(log, "combined", "error", f"Combined report failed: {exc}")
    try:
        if overview_md:
            materials_dir = os.path.join(report_dir, "materials")
            os.makedirs(materials_dir, exist_ok=True)
            dump_json(
                os.path.join(materials_dir, f"{symbol}_overview.json"),
                {"overview_md": overview_md},
            )
    except Exception:
        pass
    report_paths = build_three_reports(
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
    return report_paths


def run_pipeline(
    symbol: str,
    report_mode: str = "combined",
    report_modes: Optional[List[str]] = None,
    stock_name: Optional[str] = None,
    news_days: int = 7,
    use_bocha: bool = False,
    include_realtime: bool = True,
    output_dir: Optional[str] = None,
    env_overrides: Optional[Dict[str, str]] = None,
    db_overrides: Optional[Dict[str, str]] = None,
    agent_overrides: Optional[Dict[str, Any]] = None,
    crawl_mindspider: bool = False,
    mindspider_platforms: Optional[List[str]] = None,
    mindspider_days: int = 1,
    mindspider_max_notes: int = 10,
    mindspider_dir: Optional[str] = None,
    mindspider_keywords: Optional[str] = None,
    mindspider_no_crawl: bool = False,
    crawl_guba: bool = False,
    guba_keywords: Optional[str] = None,
    guba_dir: Optional[str] = None,
    guba_pages: int = 10,
    guba_fulltext: bool = False,
    guba_fulltext_days: int = 7,
    guba_fulltext_limit: int = 200,
    guba_init_schema: bool = False,
    run_reports: bool = True,
    log: LogFn = None,
) -> Dict[str, Any]:
    apply_env_overrides(env_overrides)
    if db_overrides:
        apply_env_overrides(db_overrides)
    guba_keywords = guba_keywords or mindspider_keywords
    db_cfg = resolve_db_config(db_overrides)
    config_path = None
    if crawl_mindspider or crawl_guba:
        if not db_cfg:
            raise RuntimeError("DB config missing; cannot run crawlers.")
        config_path = write_temp_mysql_config(db_cfg, base_dir=TMP_DIR)

    try:
        crawl_status: Dict[str, str] = {}
        crawl_errors: Dict[str, str] = {}
        if crawl_mindspider:
            try:
                run_mindspider_crawler(
                    symbol=symbol,
                    stock_name=stock_name or symbol,
                    config_path=config_path,
                    platforms=mindspider_platforms or ["wb", "xhs", "zhihu", "tieba", "bilibili", "douyin", "kuaishou"],
                    days=mindspider_days,
                    max_notes=mindspider_max_notes,
                    mindspider_dir=mindspider_dir,
                    keywords=mindspider_keywords,
                    no_crawl=mindspider_no_crawl,
                    log=log,
                )
                crawl_status["mindspider"] = "ok"
            except Exception as exc:
                crawl_errors["mindspider"] = repr(exc)
                _log(f"[crawl][mindspider][error] {exc!r}", log)
        if crawl_guba:
            try:
                run_guba_crawler(
                    symbol=symbol,
                    config_path=config_path,
                    guba_dir=guba_dir,
                    keywords=guba_keywords,
                    pages=guba_pages,
                    init_schema=guba_init_schema,
                    fulltext=guba_fulltext,
                    fulltext_days=guba_fulltext_days,
                    fulltext_limit=guba_fulltext_limit,
                    log=log,
                )
                crawl_status["guba"] = "ok"
            except Exception as exc:
                crawl_errors["guba"] = repr(exc)
                _log(f"[crawl][guba][error] {exc!r}", log)
    finally:
        if config_path:
            try:
                config_path.unlink()
            except Exception:
                pass

    normalized_modes: List[str] = []
    report_paths: Dict[str, str] = {}
    report_urls: Dict[str, str] = {}
    selected = None
    selected_mode = None
    if run_reports:
        normalized_modes = _normalize_report_modes(report_mode, report_modes)
        if not normalized_modes:
            raise ValueError("report_modes is empty")
        report_paths_all = run_report(
            symbol=symbol,
            report_mode=report_mode,
            report_modes=normalized_modes,
            news_days=news_days,
            use_bocha=use_bocha,
            include_realtime=include_realtime,
            output_dir=output_dir,
            agent_overrides=agent_overrides,
            db_cfg=db_cfg,
            log=log,
        )
        report_paths = {k: report_paths_all[k] for k in normalized_modes if k in report_paths_all}

        root = Path.cwd()

        def _to_url(path: str) -> str:
            rel = os.path.relpath(path, start=root).replace("\\", "/")
            return "/" + rel.lstrip("/")

        report_urls = {k: _to_url(v) for k, v in report_paths.items()}
        if "combined" in normalized_modes:
            selected_mode = "combined"
        elif normalized_modes:
            selected_mode = normalized_modes[0]
        selected = report_urls.get(selected_mode)
    return {
        "action": "analysis" if run_reports else "crawler",
        "report_mode": selected_mode or report_mode,
        "report_modes": normalized_modes,
        "report_paths": report_paths,
        "report_urls": report_urls,
        "selected_url": selected,
        "report_dir": str(Path(output_dir or "reports") / symbol),
        "crawl_status": crawl_status if (crawl_mindspider or crawl_guba) else {},
        "crawl_errors": crawl_errors if (crawl_mindspider or crawl_guba) else {},
    }
