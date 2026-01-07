# finance_tools/aitrados_client.py
"""
FinanceMCPClient: 使用 Baostock 获取 A 股数据的客户端。

参考了 24mlight/a-share-mcp-is-just-i-need 项目中的实现思路：
- 使用 baostock.query_history_k_data_plus 获取 K 线
- 使用类似 normalize_stock_code 的规则统一股票代码格式

本版仅针对 A 股：
- 输入可以是：600519 / 600519.SH / sh600519 / sh.600519 等
- 内部统一转换为 Baostock 需要的格式：sh.600519 / sz.000001
"""

from __future__ import annotations
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

import logging
import sys
import os
import json
import hashlib
import sqlite3
import pickle
import time
import zlib
from contextlib import contextmanager
from datetime import datetime, timedelta
import threading
from typing import Any, Dict, List, Optional, Tuple
import baostock as bs
import pandas as pd
import re
from matplotlib import rcParams
import asyncio
import importlib.util

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from mcp import types as mcp_types
except Exception:  # mcp 可能没装
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None
    mcp_types = None

try:
    import pymysql
    HAS_PYMYSQL = True
except Exception:
    pymysql = None  # type: ignore
    HAS_PYMYSQL = False

try:
    from config import settings as _app_settings
except Exception:
    _app_settings = None



# 新增：AkShare 作为宏观 & 扩展财务指标的数据源
try:
    import akshare as ak
    HAS_AKSHARE = True
except Exception:
    ak = None  # type: ignore
    HAS_AKSHARE = False

# 在函数外设置一次即可
rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei"]  # 微软雅黑 / 黑体
rcParams["axes.unicode_minus"] = False  # 解决负号显示为方块的问题

logger = logging.getLogger(__name__)
_BAOSTOCK_LOCK = threading.RLock()

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_AKSHARE_CACHE_DB_PATH = os.getenv(
    "AKSHARE_CACHE_DB_PATH",
    os.path.join(_PROJECT_ROOT, "data", "cache", "akshare_cache.db"),
)
_AKSHARE_CACHE_ENABLED = os.getenv("AKSHARE_CACHE_DISABLE", "0").lower() not in ("1", "true", "yes")
_AKSHARE_CACHE_COMPRESS = os.getenv("AKSHARE_CACHE_COMPRESS", "1").lower() in ("1", "true", "yes")
_AKSHARE_CACHE_DEFAULT_TTL = int(os.getenv("AKSHARE_CACHE_TTL_DEFAULT", "3600"))
_AKSHARE_CACHE_LOCK = threading.RLock()
_AKSHARE_CACHE_INIT = False
_AKSHARE_CACHE_TTLS = {
    "stock_zcfz_em": 86400,
    "stock_zcfz_bj_em": 86400,
    "stock_lrb_em": 86400,
    "stock_xjll_em": 86400,
    "stock_yjbb_em": 86400,
    "stock_yjkb_em": 86400,
    "stock_balance_sheet_by_report_em": 86400,
    "stock_balance_sheet_by_yearly_em": 86400,
    "stock_profit_sheet_by_report_em": 86400,
    "stock_profit_sheet_by_yearly_em": 86400,
    "stock_cash_flow_sheet_by_report_em": 86400,
    "stock_cash_flow_sheet_by_yearly_em": 86400,
    "stock_financial_analysis_indicator": 21600,
    "stock_zh_a_spot_em": 120,
    "stock_zh_a_spot": 120,
    "stock_zh_a_hist_pre_min_em": 300,
    "stock_zh_a_minute": 300,
    "stock_intraday_sina": 120,
    "stock_intraday_em": 120,
    "stock_zh_a_tick_tx": 120,
    "stock_zh_a_tick_tx_js": 120,
    "stock_gsrl_gsdt_em": 86400,
    "stock_news_em": 3600,
    "stock_news_main_cx": 3600,
    "stock_zh_growth_comparison_em": 21600,
    "stock_zh_valuation_comparison_em": 21600,
    "stock_zh_dupont_comparison_em": 21600,
    "stock_zh_index_spot_sina": 300,
    "stock_zh_index_daily": 21600,
    "stock_zh_index_daily_em": 21600,
    "index_hist_sw": 21600,
    "sw_index_daily": 21600,
    "sw_index_second_info": 86400,
    "sw_index_first_info": 86400,
    "index_realtime_sw": 300,
    "sw_index_cons": 86400,
    "index_component_sw": 86400,
    "index_zh_a_hist": 21600,
    "macro_china_lpr": 21600,
    "macro_china_supply_of_money": 21600,
}
_AKSHARE_ORIGINAL_FUNCS: Dict[str, Any] = {}
_AKSHARE_CACHE_INSTALLED = False
_AKSHARE_MYSQL_TABLES: set[str] = set()


def _normalize_cache_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_normalize_cache_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _normalize_cache_value(v) for k, v in sorted(value.items(), key=lambda x: str(x[0]))}
    return repr(value)


def _akshare_cache_key(func_name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[str, str]:
    params_obj = {
        "args": [_normalize_cache_value(v) for v in args],
        "kwargs": {str(k): _normalize_cache_value(v) for k, v in sorted(kwargs.items(), key=lambda x: str(x[0]))},
    }
    params_json = json.dumps(params_obj, sort_keys=True, ensure_ascii=True)
    raw = f"{func_name}|{params_json}"
    key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return key, params_json


def _akshare_mysql_cache_enabled() -> bool:
    if not HAS_PYMYSQL:
        return False
    if os.getenv("AKSHARE_MYSQL_CACHE_DISABLE", "0").lower() in ("1", "true", "yes"):
        return False
    dialect = str(_get_db_config_value("DB_DIALECT", "mysql")).lower()
    if dialect and dialect != "mysql":
        return False
    return bool(_get_db_config_value("DB_NAME", ""))


def _get_db_config_value(key: str, default: str) -> str:
    if _app_settings is not None:
        value = getattr(_app_settings, key, None)
        if value not in (None, ""):
            return str(value)
    return str(os.getenv(key, default))


def _get_mysql_cache_config() -> Optional[Dict[str, Any]]:
    if not _akshare_mysql_cache_enabled():
        return None
    db_name = os.getenv("AKSHARE_CACHE_DB_NAME") or _get_db_config_value("DB_NAME", "")
    if not db_name:
        return None
    host = _get_db_config_value("DB_HOST", "127.0.0.1")
    raw_port = _get_db_config_value("DB_PORT", "3306")
    try:
        port = int(raw_port)
    except ValueError:
        port = 3306
    user = _get_db_config_value("DB_USER", "root")
    password = _get_db_config_value("DB_PASSWORD", "")
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": db_name,
    }


def _extract_cache_symbol(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Optional[str]:
    for key in ("symbol", "stock", "stock_code", "stock_symbol", "code", "secid", "ts_code"):
        if key in kwargs:
            value = kwargs.get(key)
            if value is None:
                continue
            match = re.search(r"(\d{6})", str(value))
            if match:
                return match.group(1)
    for item in args:
        if isinstance(item, (str, int)):
            match = re.search(r"(\d{6})", str(item))
            if match:
                return match.group(1)
    return None


def _akshare_cache_table_name(symbol: Optional[str]) -> str:
    base = symbol or "global"
    base = re.sub(r"[^0-9a-zA-Z_]", "_", base)
    if not base:
        base = "global"
    if not re.match(r"[A-Za-z_]", base[:1]):
        base = f"t_{base}"
    return f"akshare_cache_{base}"[:64]


def _ensure_mysql_cache_table(conn: "pymysql.connections.Connection", table: str) -> None:
    if table in _AKSHARE_MYSQL_TABLES:
        return
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        `cache_key` VARCHAR(64) NOT NULL,
        `func` VARCHAR(128) NOT NULL,
        `params_json` LONGTEXT,
        `created_at` BIGINT NOT NULL,
        `ttl_seconds` INT NOT NULL,
        `payload` LONGBLOB,
        `payload_type` VARCHAR(64),
        PRIMARY KEY (`cache_key`),
        KEY `idx_created_at` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """.strip()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    _AKSHARE_MYSQL_TABLES.add(table)


def _deserialize_payload(payload: bytes, payload_type: Optional[str]) -> Any:
    if payload is None:
        return None
    data = payload
    if isinstance(payload_type, str) and payload_type.startswith("zlib:"):
        try:
            data = zlib.decompress(data)
        except Exception:
            return None
    try:
        return pickle.loads(data)
    except Exception:
        return None


def _serialize_payload(payload: Any) -> Optional[Tuple[bytes, str]]:
    if payload is None:
        return None
    if isinstance(payload, pd.DataFrame) and payload.empty:
        return None
    try:
        raw = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        payload_type = type(payload).__name__
        if _AKSHARE_CACHE_COMPRESS:
            raw = zlib.compress(raw)
            payload_type = f"zlib:{payload_type}"
        return raw, payload_type
    except Exception:
        return None


def _akshare_mysql_cache_get(
    func_name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    ttl_seconds: int,
) -> Any:
    cfg = _get_mysql_cache_config()
    if not cfg or ttl_seconds <= 0:
        return None
    cache_key, _ = _akshare_cache_key(func_name, args, kwargs)
    symbol = _extract_cache_symbol(args, kwargs)
    table = _akshare_cache_table_name(symbol)
    try:
        with _AKSHARE_CACHE_LOCK:
            conn = pymysql.connect(
                host=cfg["host"],
                port=int(cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                _ensure_mysql_cache_table(conn, table)
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT created_at, ttl_seconds, payload, payload_type FROM `{table}` WHERE cache_key=%s",
                        (cache_key,),
                    )
                    row = cur.fetchone() or {}
                    if not row:
                        return None
                    created_at = int(row.get("created_at") or 0)
                    stored_ttl = row.get("ttl_seconds")
                    effective_ttl = ttl_seconds if ttl_seconds is not None else stored_ttl
                    if effective_ttl and int(time.time()) - created_at > int(effective_ttl):
                        cur.execute(f"DELETE FROM `{table}` WHERE cache_key=%s", (cache_key,))
                        conn.commit()
                        return None
                    payload = row.get("payload")
                    payload_type = row.get("payload_type")
                return _deserialize_payload(payload, payload_type)
            finally:
                conn.close()
    except Exception:
        return None


def _akshare_mysql_cache_set(
    func_name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    ttl_seconds: int,
    payload: Any,
) -> None:
    cfg = _get_mysql_cache_config()
    if not cfg or ttl_seconds <= 0:
        return
    serialized = _serialize_payload(payload)
    if not serialized:
        return
    raw, payload_type = serialized
    cache_key, params_json = _akshare_cache_key(func_name, args, kwargs)
    symbol = _extract_cache_symbol(args, kwargs)
    table = _akshare_cache_table_name(symbol)
    created_at = int(time.time())
    try:
        with _AKSHARE_CACHE_LOCK:
            conn = pymysql.connect(
                host=cfg["host"],
                port=int(cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                _ensure_mysql_cache_table(conn, table)
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO `{table}`
                        (cache_key, func, params_json, created_at, ttl_seconds, payload, payload_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                          func=VALUES(func),
                          params_json=VALUES(params_json),
                          created_at=VALUES(created_at),
                          ttl_seconds=VALUES(ttl_seconds),
                          payload=VALUES(payload),
                          payload_type=VALUES(payload_type)
                        """,
                        (
                            cache_key,
                            func_name,
                            params_json,
                            created_at,
                            int(ttl_seconds),
                            raw,
                            payload_type,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        return


def _init_akshare_cache_db() -> None:
    global _AKSHARE_CACHE_INIT
    global _AKSHARE_CACHE_ENABLED
    if not _AKSHARE_CACHE_ENABLED or _AKSHARE_CACHE_INIT:
        return
    try:
        os.makedirs(os.path.dirname(_AKSHARE_CACHE_DB_PATH), exist_ok=True)
        with sqlite3.connect(_AKSHARE_CACHE_DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS akshare_cache (
                    cache_key TEXT PRIMARY KEY,
                    func TEXT,
                    params_json TEXT,
                    created_at INTEGER,
                    ttl_seconds INTEGER,
                    payload BLOB,
                    payload_type TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_akshare_cache_created ON akshare_cache(created_at)"
            )
    except Exception as exc:
        logger.warning("Akshare cache init failed: %s", exc)
        _AKSHARE_CACHE_ENABLED = False
    finally:
        _AKSHARE_CACHE_INIT = True


def _akshare_cache_get(func_name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any], ttl_seconds: int) -> Any:
    if not _AKSHARE_CACHE_ENABLED or ttl_seconds <= 0:
        return None
    _init_akshare_cache_db()
    cache_key, _ = _akshare_cache_key(func_name, args, kwargs)
    try:
        with _AKSHARE_CACHE_LOCK:
            with sqlite3.connect(_AKSHARE_CACHE_DB_PATH) as conn:
                row = conn.execute(
                    "SELECT created_at, ttl_seconds, payload, payload_type FROM akshare_cache WHERE cache_key = ?",
                    (cache_key,),
                ).fetchone()
                if not row:
                    return None
                created_at, stored_ttl, payload, payload_type = row
                effective_ttl = ttl_seconds if ttl_seconds is not None else stored_ttl
                if effective_ttl and int(time.time()) - int(created_at) > int(effective_ttl):
                    conn.execute("DELETE FROM akshare_cache WHERE cache_key = ?", (cache_key,))
                    return None
    except Exception:
        return None
    if payload is None:
        return None
    try:
        data = payload
        if isinstance(payload_type, str) and payload_type.startswith("zlib:"):
            data = zlib.decompress(data)
        return pickle.loads(data)
    except Exception:
        try:
            with _AKSHARE_CACHE_LOCK:
                with sqlite3.connect(_AKSHARE_CACHE_DB_PATH) as conn:
                    conn.execute("DELETE FROM akshare_cache WHERE cache_key = ?", (cache_key,))
        except Exception:
            pass
        return None


def _akshare_cache_set(
    func_name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    ttl_seconds: int,
    payload: Any,
) -> None:
    if not _AKSHARE_CACHE_ENABLED or ttl_seconds <= 0:
        return
    if payload is None:
        return
    if isinstance(payload, pd.DataFrame) and payload.empty:
        return
    _init_akshare_cache_db()
    cache_key, params_json = _akshare_cache_key(func_name, args, kwargs)
    try:
        raw = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        payload_type = type(payload).__name__
        if _AKSHARE_CACHE_COMPRESS:
            raw = zlib.compress(raw)
            payload_type = f"zlib:{payload_type}"
    except Exception:
        return
    created_at = int(time.time())
    try:
        with _AKSHARE_CACHE_LOCK:
            with sqlite3.connect(_AKSHARE_CACHE_DB_PATH) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO akshare_cache
                    (cache_key, func, params_json, created_at, ttl_seconds, payload, payload_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cache_key,
                        func_name,
                        params_json,
                        created_at,
                        int(ttl_seconds),
                        sqlite3.Binary(raw),
                        payload_type,
                    ),
                )
    except Exception:
        return


def _get_akshare_ttl(func_name: str) -> int:
    env_key = f"AKSHARE_CACHE_TTL_{func_name.upper()}"
    env_val = os.getenv(env_key)
    if env_val:
        try:
            return max(0, int(env_val))
        except ValueError:
            pass
    return int(_AKSHARE_CACHE_TTLS.get(func_name, _AKSHARE_CACHE_DEFAULT_TTL))


def _akshare_cached_call(func_name: str, func: Any, *args: Any, **kwargs: Any) -> Any:
    ttl_seconds = _get_akshare_ttl(func_name)
    cached = _akshare_mysql_cache_get(func_name, args, kwargs, ttl_seconds)
    if cached is not None:
        return cached
    cached = _akshare_cache_get(func_name, args, kwargs, ttl_seconds)
    if cached is not None:
        _akshare_mysql_cache_set(func_name, args, kwargs, ttl_seconds, cached)
        return cached
    result = func(*args, **kwargs)
    _akshare_cache_set(func_name, args, kwargs, ttl_seconds, result)
    _akshare_mysql_cache_set(func_name, args, kwargs, ttl_seconds, result)
    return result


def _install_akshare_cache() -> None:
    global _AKSHARE_CACHE_INSTALLED
    if _AKSHARE_CACHE_INSTALLED or not HAS_AKSHARE:
        return
    for func_name in _AKSHARE_CACHE_TTLS.keys():
        func = getattr(ak, func_name, None)
        if not callable(func):
            continue
        _AKSHARE_ORIGINAL_FUNCS[func_name] = func
        def _make_wrapper(name: str, original: Any) -> Any:
            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                return _akshare_cached_call(name, original, *args, **kwargs)
            return _wrapped
        setattr(ak, func_name, _make_wrapper(func_name, func))
    _AKSHARE_CACHE_INSTALLED = True


def ensure_akshare_cache_installed() -> None:
    if HAS_AKSHARE:
        _install_akshare_cache()


if HAS_AKSHARE:
    _install_akshare_cache()


# ---------- Baostock 登录上下文 ----------

@contextmanager
def baostock_login():
    """
    简单的 Baostock 登录/登出上下文管理器。
    """
    with _BAOSTOCK_LOCK:
        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"Baostock 登录失败: {lg.error_msg} (code={lg.error_code})")
        try:
            yield
        finally:
            bs.logout()


# ---------- 帮助函数：代码归一化（参考 a-share-mcp helpers.py） ----------

def normalize_stock_code(code: str) -> str:
    """
    将各种 A 股代码格式统一成 Baostock 标准格式，例如：
    - '600000'        -> 'sh.600000'
    - '600000.SH'     -> 'sh.600000'
    - 'sh600000'      -> 'sh.600000'
    - 'sz000001'      -> 'sz.000001'
    - '000001.SZ'     -> 'sz.000001'
    """
    raw = (code or "").strip()
    if not raw:
        raise ValueError("股票代码不能为空")

    # 1) sh600000 / sz000001 / sh.600000 / sz.000001
    #    注意 (?i) 必须写在最前面
    m = re.fullmatch(r"(?i)(sh|sz)[\.]?(\d{6})", raw)
    if m:
        ex = m.group(1).lower()
        num = m.group(2)
        return f"{ex}.{num}"

    # 2) 600000.SH / 000001.SZ
    #    同理，把 (?i) 放到最开头
    m2 = re.fullmatch(r"(?i)(\d{6})[\.]?(sh|sz)", raw)
    if m2:
        num = m2.group(1)
        ex = m2.group(2).lower()
        return f"{ex}.{num}"

    # 3) 纯 6 位数字
    m3 = re.fullmatch(r"(\d{6})", raw)
    if m3:
        num = m3.group(1)
        ex = "sh" if num.startswith("6") else "sz"
        return f"{ex}.{num}"

    raise ValueError(
        f"无法识别的股票代码格式: {code}。示例: '600000', '000001.SZ', 'sh600000'"
    )


_INDEX_CATALOG_PATH = os.path.join(os.path.dirname(__file__), "data", "index_catalog.json")
_CODE_PATTERNS = (
    re.compile(r"^(?:sh|sz|bj|csi)\d{6}$", re.IGNORECASE),
    re.compile(r"^\d{6}(?:\.[A-Za-z]+)?$"),
)


def _init_index_catalog() -> Dict[str, Any]:
    return {"generated_at": "", "sources": [], "items": []}


def _load_index_catalog() -> Dict[str, Any]:
    data = _init_index_catalog()
    if not os.path.exists(_INDEX_CATALOG_PATH):
        return data
    try:
        with open(_INDEX_CATALOG_PATH, "r", encoding="utf-8") as f:
            loaded = json.load(f)
    except Exception as e:
        data["error"] = str(e)
        return data
    if isinstance(loaded, dict):
        data["generated_at"] = loaded.get("generated_at") or ""
        if isinstance(loaded.get("sources"), list):
            data["sources"] = loaded.get("sources") or []
        if isinstance(loaded.get("items"), list):
            data["items"] = loaded.get("items") or []
        if loaded.get("errors"):
            data["errors"] = loaded.get("errors")
    return data


def _save_index_catalog(catalog: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(_INDEX_CATALOG_PATH), exist_ok=True)
    with open(_INDEX_CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=True, indent=2)


def _dedupe_index_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for item in items:
        key = (item.get("fetcher"), item.get("symbol"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _guess_code_column(df: pd.DataFrame) -> Optional[str]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    best_col = None
    best_score = 0.0
    for col in df.columns:
        series = df[col].fillna("").astype(str).str.strip()
        if series.empty:
            continue
        matches = 0
        for pat in _CODE_PATTERNS:
            matches += series.str.match(pat).sum()
        score = matches / max(len(series), 1)
        if score > best_score:
            best_score = score
            best_col = col
    return best_col if best_score >= 0.2 else None


def _guess_name_column(df: pd.DataFrame, code_col: Optional[str]) -> Optional[str]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    best_col = None
    best_score = 0.0
    for col in df.columns:
        if col == code_col:
            continue
        series = df[col].fillna("").astype(str).str.strip()
        if series.empty:
            continue
        matches = series.str.contains(r"[A-Za-z\u4e00-\u9fff]")
        score = matches.mean()
        if score > best_score:
            best_score = score
            best_col = col
    return best_col if best_score >= 0.2 else None


def _normalize_index_symbol(code_raw: str, fetcher: str) -> Tuple[str, str, str]:
    text = str(code_raw or "").strip()
    if not text:
        return "", "", ""
    code_match = re.search(r"(\d{6})", text)
    code_digits = code_match.group(1) if code_match else ""
    if fetcher in ("index_hist_sw", "index_zh_a_hist"):
        return (code_digits, code_digits, "") if code_digits else ("", "", "")
    lower = text.lower()
    if re.match(r"^(sh|sz|bj|csi)\d{6}$", lower):
        return lower, code_digits or lower[-6:], lower[:2]
    return "", "", ""


def _extract_index_items_from_df(
    df: pd.DataFrame,
    source: str,
    fetcher: str,
    category: str,
) -> List[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    code_col = _guess_code_column(df)
    name_col = _guess_name_column(df, code_col)
    if not code_col or not name_col:
        return []
    items: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        code_raw = row.get(code_col, "")
        name = row.get(name_col, "")
        if not str(code_raw).strip() or not str(name).strip():
            continue
        symbol, code, market = _normalize_index_symbol(code_raw, fetcher)
        if not symbol or not code:
            continue
        items.append(
            {
                "symbol": symbol,
                "code": code,
                "name": str(name).strip(),
                "source": source,
                "provider": "akshare",
                "fetcher": fetcher,
                "category": category,
                "market": market,
            }
        )
    return items


def _build_index_catalog_from_akshare() -> Dict[str, Any]:
    catalog = _init_index_catalog()
    if not HAS_AKSHARE:
        catalog["errors"] = {"akshare": "not_installed"}
        return catalog
    items: List[Dict[str, Any]] = []
    sources: List[Dict[str, Any]] = []
    errors: Dict[str, Any] = {}

    def _add(df: pd.DataFrame, source: str, fetcher: str, category: str) -> None:
        extracted = _extract_index_items_from_df(df, source, fetcher, category)
        if extracted:
            items.extend(extracted)
            sources.append({"name": source, "count": len(extracted), "fetcher": fetcher})

    try:
        if hasattr(ak, "stock_zh_index_spot_sina"):
            _add(ak.stock_zh_index_spot_sina(), "stock_zh_index_spot_sina", "stock_zh_index_daily", "market")
    except Exception as e:
        errors["stock_zh_index_spot_sina"] = str(e)

    try:
        if hasattr(ak, "sw_index_second_info"):
            _add(ak.sw_index_second_info(), "sw_index_second_info", "index_hist_sw", "sw")
    except Exception as e:
        errors["sw_index_second_info"] = str(e)

    try:
        if hasattr(ak, "sw_index_first_info"):
            _add(ak.sw_index_first_info(), "sw_index_first_info", "index_hist_sw", "sw")
    except Exception as e:
        errors["sw_index_first_info"] = str(e)

    try:
        if hasattr(ak, "index_realtime_sw"):
            _add(ak.index_realtime_sw(symbol="一级行业"), "index_realtime_sw_first", "index_hist_sw", "sw")
            _add(ak.index_realtime_sw(symbol="二级行业"), "index_realtime_sw_second", "index_hist_sw", "sw")
    except Exception as e:
        errors["index_realtime_sw"] = str(e)

    catalog["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    catalog["sources"] = sources
    catalog["items"] = _dedupe_index_items(items)
    if errors:
        catalog["errors"] = errors
    return catalog


def _ensure_index_catalog() -> Dict[str, Any]:
    catalog = _load_index_catalog()
    if catalog.get("items"):
        return catalog
    built = _build_index_catalog_from_akshare()
    if built.get("items"):
        _save_index_catalog(built)
        return built
    return built if built.get("errors") else catalog


def _normalize_match_text(text: str) -> str:
    if not text:
        return ""
    raw = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(text).lower())
    for token in ("申万", "行业", "指数", "板块", "中证", "国证", "上证", "深证", "成份", "成指", "一级", "二级", "a股", "b股", "sw"):
        raw = raw.replace(token, "")
    return raw


def _expand_keywords(keywords: List[str]) -> List[str]:
    expanded: List[str] = []
    for kw in keywords:
        if not isinstance(kw, str):
            continue
        text = kw.strip()
        if not text:
            continue
        if text not in expanded:
            expanded.append(text)
        for part in re.split(r"[\s\-/_,;，、；]+", text):
            part = part.strip()
            if part and part not in expanded:
                expanded.append(part)
    return expanded


def _rank_index_candidates(items: List[Dict[str, Any]], keywords: List[str]) -> List[Dict[str, Any]]:
    if not items or not keywords:
        return []
    kw_pairs: List[tuple[str, str]] = []
    seen = set()
    for kw in keywords:
        kw_norm = _normalize_match_text(kw)
        if kw_norm and kw_norm not in seen:
            kw_pairs.append((kw, kw_norm))
            seen.add(kw_norm)
    if not kw_pairs:
        return []
    candidates: List[Dict[str, Any]] = []
    for item in items:
        name = item.get("name") or ""
        name_norm = _normalize_match_text(name)
        if not name_norm:
            continue
        best_score = 0.0
        best_kw = ""
        for kw_raw, kw_norm in kw_pairs:
            if kw_norm == name_norm:
                score = 1.0
            elif kw_norm in name_norm or name_norm in kw_norm:
                score = min(len(kw_norm), len(name_norm)) / max(len(kw_norm), len(name_norm))
            else:
                continue
            if score > best_score:
                best_score = score
                best_kw = kw_raw
        if best_score > 0:
            candidate = dict(item)
            candidate["match_score"] = round(best_score, 4)
            candidate["match_keyword"] = best_kw
            candidates.append(candidate)
    candidates.sort(
        key=lambda x: (x.get("match_score", 0), 1 if x.get("category") == "sw" else 0, len(str(x.get("name") or ""))),
        reverse=True,
    )
    return candidates


class AkshareMCPClient:
    """
    通过 MCP 协议调用 akshare_mcp 的一个轻量封装。

    目前只做两件事：
    - get_bank_ops：获取银行业运营指标（成本收入比、非息收入占比等）
    - 后续你可以很方便再加：行业指数、宏观数据等
    """

    def __init__(self, debug: bool = False):
        self.debug = debug
        # 检查 akshare_mcp 模块是否存在
        self.available = importlib.util.find_spec("akshare_mcp") is not None and ClientSession is not None
        if not self.available and self.debug:
            logger.warning("[AkshareMCPClient] akshare_mcp 或 mcp 未安装，MCP 功能不可用。")

    async def _call_tool_async(self, tool_name: str, arguments: dict) -> list[str]:
        """
        通过 stdio 启动 akshare_mcp server，调用指定 tool，返回 text 内容列表。
        """
        if not self.available:
            raise RuntimeError("akshare_mcp 或 mcp 未正确安装，无法通过 MCP 调用 AkShare。")

        # 用当前 python 解释器启动 akshare_mcp 实现 MCP server
        server_params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "akshare_mcp"],
        )

        texts: list[str] = []

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                # 初始化握手
                await session.initialize()
                await session.ready()

                if self.debug:
                    tools = await session.list_tools()
                    tool_names = [t.name for t in tools.tools]
                    logger.debug(f"[AkshareMCPClient] 可用工具列表: {tool_names}")

                # 真正调用工具
                result = await session.call_tool(tool_name, arguments=arguments)

                # 解析返回的 content
                for c in result.content:
                    # TextContent 场景：通常 akshare_mcp 会把 DataFrame 转成 markdown / 表格
                    text = getattr(c, "text", None)
                    if isinstance(text, str):
                        texts.append(text)

        return texts

    def call_tool(self, tool_name: str, arguments: dict) -> list[str]:
        """
        同步封装，方便在普通函数里直接用。
        """
        try:
            return asyncio.run(self._call_tool_async(tool_name, arguments))
        except Exception as e:
            if self.debug:
                logger.warning(f"[AkshareMCPClient] 调用 MCP 工具 {tool_name} 失败: {e}", exc_info=True)
            else:
                logger.warning(f"[AkshareMCPClient] 调用 MCP 工具 {tool_name} 失败: {e}")
            return []

    # =========== 具体业务封装：银行运营指标 ===========
    def get_bank_ops(self, symbol: str) -> dict[str, Any]:
        """
        使用 akshare_mcp 的 stock_financial_analysis_indicator 工具，
        获取银行运营相关指标（成本收入比、非息收入占比等）。

        这里不强行解析成结构化数值，而是把返回的表格/markdown 原样给 LLM。
        """

        if not self.available:
            return {
                "has_data": False,
                "error": "akshare_mcp 或 mcp 未安装",
                "raw_text": "",
            }

        # 转换成 akshare 习惯的 symbol：去掉交易所前缀，只留 6 位代码
        # 例如 sz.000001 -> 000001
        normalized = normalize_stock_code(symbol)
        numeric_code = normalized.split(".")[-1]

        # akshare 文档通常是这样用: stock_financial_analysis_indicator(symbol="000001")
        # MCP 的工具名一般就是 akshare 函数名
        tool_name = "stock_financial_analysis_indicator"
        arguments = {
            "symbol": numeric_code,
        }

        texts = self.call_tool(tool_name, arguments)
        raw_text = "\n\n".join(texts).strip()

        return {
            "has_data": bool(raw_text),
            "symbol": numeric_code,
            "tool_name": tool_name,
            "arguments": arguments,
            "raw_text": raw_text,
        }



# ---------- 主客户端类：供 FinanceMCPTool 调用 ----------

class FinanceMCPClient:
    """
    提供给 InsightEngine.tools.finance_mcp_tool 使用的客户端。

    目前实现：
    - get_ohlc: 通过 Baostock 获取历史 K 线数据（技术面）
    - get_indicators: 简单占位（以后可基于 OHLC 自己算 MA/RSI 等）
    """

    def __init__(self, debug: bool = False) -> None:
        self.debug = debug
        # 新增：AkShare MCP 客户端，用于补齐缺失数据（银行运营指标、行业指数等）
        self.akshare_mcp_client = AkshareMCPClient(debug=debug)
        if self.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    # 频率映射：把外部 timeframe => Baostock 的 frequency 参数
    def _map_timeframe_to_frequency(self, timeframe: str) -> str:
        """
        将统一的 timeframe 转为 Baostock 的 frequency 参数。
        """
        tf = timeframe.upper()

        # 日线
        if tf in ("DAY", "1D", "D"):
            return "d"

        # 周线
        if tf in ("WEEK", "1W", "W"):
            return "w"

        # 月线
        if tf in ("MON", "MONTH", "1MON"):
            return "m"

        # 分钟线（分时）
        if tf in ("60M", "60", "1H", "H1"):
            return "60"
        if tf in ("30M", "30"):
            return "30"
        if tf in ("15M", "15"):
            return "15"
        if tf in ("5M", "5"):
            return "5"

        # 默认退回日线
        return "d"

    def _calc_date_range(self, limit: int) -> (str, str):
        """
        简单根据条数估算一个起始日期区间（往前多取一些，最后再截 tail(limit)）
        """
        today = datetime.today().date()
        days = max(limit * 2, 250)  # 至少大约一年
        start = today - timedelta(days=days)
        return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    def get_ohlc(
        self,
        symbol: str,
        timeframe: str = "DAY",
        limit: int = 200,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust_flag: str = "2",  # 1=前复权, 2=后复权, 3=不复权
        basic_fields: bool = False,
    ) -> Dict[str, Any]:
        """
        获取 A 股 K 线数据。

        参数:
        - symbol: 股票代码，可以是 '600519' / '600519.SH' / 'sh600519' 等任意常见格式
        - timeframe: 时间维度：'DAY'/'WEEK'/'MON' 或 '5m','15m','30m','60m' 等
        - limit: 希望的返回条数（函数内部会取更长区间再截尾）
        - adjust_flag: 复权方式，默认为 '2' (后复权)

        返回:
        - dict 结构，包含 K 线列表，字段类似:
          {
            "source": "baostock",
            "symbol": "600519",
            "code": "sh.600519",
            "frequency": "d",
            "fields": [...],
            "data": [ { "date": "...", "open": "...", ... }, ... ]
          }
        """
        # 1) 归一化代码
        normalized_code = normalize_stock_code(symbol)
        freq = self._map_timeframe_to_frequency(timeframe)
        if start_date is None or end_date is None:
            default_start, default_end = self._calc_date_range(limit)
            start_date = start_date or default_start
            end_date = end_date or default_end
        start_date = str(start_date)
        end_date = str(end_date)

        # 1) 归一化代码
        normalized_code = normalize_stock_code(symbol)
        freq = self._map_timeframe_to_frequency(timeframe)
        if start_date is None or end_date is None:
            default_start, default_end = self._calc_date_range(limit)
            start_date = start_date or default_start
            end_date = end_date or default_end
        start_date = str(start_date)
        end_date = str(end_date)

        # 不同周期用不同字段：
        # - 日线：可以取一整套扩展字段
        # - 周线 / 月线：Baostock 对某些字段有限制，这里只取最安全的基础字段
        # - 分钟线：使用 date+time 的基础字段
        if freq == "d":
            if basic_fields:
                # 日线基础字段（更长历史）
                fields = [
                    "date",
                    "code",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                ]
            else:
                # 日线：全字段
                fields = [
                    "date",
                    "code",
                    "open",
                    "high",
                    "low",
                    "close",
                    "preclose",
                    "volume",
                    "amount",
                    "adjustflag",
                    "turn",
                    "tradestatus",
                    "pctChg",
                    "peTTM",
                    "pbMRQ",
                    "psTTM",
                    "pcfNcfTTM",
                    "isST",
                ]
        elif freq in ("w", "m"):
            # 周线 / 月线：用最基础、确保支持的一组字段
            # （date / code / OHLC / volume / amount 足够我们算均线、MACD、RSI 等）
            fields = [
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
            ]
        else:
            # 分钟线（5/15/30/60 等）用 date+time 的基础字段
            fields = [
                "date",
                "time",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
            ]

        fields_str = ",".join(fields)

        if self.debug:
            logger.debug(
                f"调用 Baostock K 线: code={normalized_code}, "
                f"start={start_date}, end={end_date}, freq={freq}, adjust={adjust_flag}"
            )

        with baostock_login():
            rs = bs.query_history_k_data_plus(
                normalized_code,
                fields_str,
                start_date=start_date,
                end_date=end_date,
                frequency=freq,
                adjustflag=adjust_flag,
            )

            if rs.error_code != "0":
                raise RuntimeError(
                    f"Baostock 查询失败: {rs.error_msg} (code={rs.error_code})"
                )

            data_list: List[List[str]] = []
            while rs.next():
                data_list.append(rs.get_row_data())

        if not data_list:
            raise RuntimeError(
                f"Baostock 未返回任何数据: code={normalized_code}, "
                f"range={start_date}~{end_date}, freq={freq}"
            )

        df = pd.DataFrame(data_list, columns=rs.fields)

        # 只保留最后 limit 条
        if limit > 0 and len(df) > limit:
            df = df.tail(limit)

        # 转为 list[dict] 形式，更适合给 LLM 看
        records = df.to_dict(orient="records")

        return {
            "source": "baostock",
            "symbol": symbol,
            "code": normalized_code,
            "frequency": freq,
            "fields": list(df.columns),
            "data": records,
        }

    def _split_symbol(self, symbol: str) -> Tuple[str, str, str]:
        normalized = symbol
        try:
            normalized = normalize_stock_code(symbol)
        except Exception:
            normalized = symbol
        m = re.search(r"(\d{6})", normalized)
        code_6 = m.group(1) if m else str(symbol)[-6:]
        lower = normalized.lower()
        ex = ""
        if lower.startswith("sh."):
            ex = "sh"
        elif lower.startswith("sz."):
            ex = "sz"
        elif lower.startswith("bj."):
            ex = "bj"
        if not ex and code_6:
            ex = "sh" if code_6.startswith("6") else "sz"
        return normalized, code_6, ex

    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "source": "",
            "data": {},
            "meta": {},
        }
        if not HAS_AKSHARE:
            result["meta"]["error"] = "akshare_not_installed"
            return result

        normalized, code_6, ex = self._split_symbol(symbol)

        variants = {
            code_6,
            f"{ex}{code_6}" if ex else "",
            f"{ex}.{code_6}" if ex else "",
            f"{code_6}.{ex}" if ex else "",
            f"{code_6}.{ex.upper()}" if ex else "",
            f"{ex.upper()}{code_6}" if ex else "",
        }
        variants = {v for v in variants if v}
        variants_norm = {v.replace(".", "").lower() for v in variants}

        def _pick(row: Dict[str, Any], keys: List[str]) -> Any:
            for key in keys:
                if key in row:
                    return row.get(key)
            return None

        def _to_float_safe(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _find_row(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
            if not isinstance(df, pd.DataFrame) or df.empty:
                return None
            code_col = None
            for cand in ("代码", "code", "symbol", "股票代码"):
                if cand in df.columns:
                    code_col = cand
                    break
            if not code_col:
                code_col = _guess_code_column(df)
            if not code_col:
                return None
            series = df[code_col].astype(str).str.strip()
            series_norm = series.str.replace(".", "", regex=False).str.lower()
            mask = series_norm.isin(variants_norm) | series.str.endswith(code_6)
            if not mask.any():
                return None
            row = df.loc[mask].iloc[0]
            return row.to_dict() if hasattr(row, "to_dict") else dict(row)

        df = None
        source = ""
        try:
            if hasattr(ak, "stock_zh_a_spot_em"):
                df = ak.stock_zh_a_spot_em()
                source = "akshare.stock_zh_a_spot_em"
        except Exception as e:
            result["meta"]["spot_em_error"] = str(e)
            df = None
        if df is None or df.empty:
            try:
                if hasattr(ak, "stock_zh_a_spot"):
                    df = ak.stock_zh_a_spot()
                    source = "akshare.stock_zh_a_spot"
            except Exception as e:
                result["meta"]["spot_sina_error"] = str(e)
                df = None

        if df is None or df.empty:
            result["meta"]["error"] = "spot_data_unavailable"
            return result

        row_dict = _find_row(df)
        if not row_dict:
            result["meta"]["error"] = "spot_symbol_not_found"
            return result

        data = {
            "code": _pick(row_dict, ["代码", "code", "symbol", "股票代码"]) or code_6,
            "name": _pick(row_dict, ["名称", "name", "股票名称", "简称"]),
            "price": _to_float_safe(_pick(row_dict, ["最新价", "现价", "当前价", "price", "close", "最新"])),
            "open": _to_float_safe(_pick(row_dict, ["今开", "开盘", "open"])),
            "high": _to_float_safe(_pick(row_dict, ["最高", "high"])),
            "low": _to_float_safe(_pick(row_dict, ["最低", "low"])),
            "prev_close": _to_float_safe(_pick(row_dict, ["昨收", "昨收盘", "前收盘", "pre_close", "preclose", "settlement"])),
            "change": _to_float_safe(_pick(row_dict, ["涨跌额", "涨跌", "change"])),
            "change_pct": _to_float_safe(_pick(row_dict, ["涨跌幅", "涨跌幅%", "涨跌幅(%)", "change_pct", "pct_chg"])),
            "volume": _to_float_safe(_pick(row_dict, ["成交量", "volume", "vol", "成交量(股)"])),
            "amount": _to_float_safe(_pick(row_dict, ["成交额", "amount", "turnover", "成交额(元)"])),
            "turnover": _to_float_safe(_pick(row_dict, ["换手率", "turnover"])),
            "volume_ratio": _to_float_safe(_pick(row_dict, ["量比", "volume_ratio"])),
            "speed": _to_float_safe(_pick(row_dict, ["涨速", "speed"])),
            "change_5m": _to_float_safe(_pick(row_dict, ["5分钟涨跌", "5分钟涨跌幅", "change_5m"])),
            "amplitude": _to_float_safe(_pick(row_dict, ["振幅", "amplitude"])),
            "time": _pick(row_dict, ["时间", "time", "更新时间", "日期", "date"]),
            "market": ex.upper() if ex else "",
        }

        result["has_data"] = True
        result["source"] = source
        result["data"] = data
        return result

    def _normalize_tick_side(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        mapping = {
            "D": "卖盘",
            "S": "卖盘",
            "卖盘": "卖盘",
            "U": "买盘",
            "B": "买盘",
            "买盘": "买盘",
            "E": "中性盘",
            "N": "中性盘",
            "中性盘": "中性盘",
        }
        return mapping.get(text, text)

    def get_premarket_minute_em(
        self,
        symbol: str,
        start_time: str = "09:00:00",
        end_time: str = "15:40:00",
        limit: int = 480,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "source": "",
            "data": [],
            "meta": {},
        }
        if not HAS_AKSHARE:
            result["meta"]["error"] = "akshare_not_installed"
            return result
        if not hasattr(ak, "stock_zh_a_hist_pre_min_em"):
            result["meta"]["error"] = "stock_zh_a_hist_pre_min_em_unavailable"
            return result

        _, code_6, _ = self._split_symbol(symbol)
        try:
            df = ak.stock_zh_a_hist_pre_min_em(
                symbol=code_6,
                start_time=str(start_time),
                end_time=str(end_time),
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                rename_map = {
                    "时间": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                    "最新价": "price",
                }
                df = df.rename(columns=rename_map)
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                for col in ("open", "close", "high", "low", "volume", "amount", "price"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["date"]).sort_values("date")
                if limit > 0 and len(df) > limit:
                    df = df.tail(limit)
                result["data"] = df.to_dict(orient="records")
                result["has_data"] = True
                result["source"] = "akshare.stock_zh_a_hist_pre_min_em"
        except Exception as e:
            result["meta"]["error"] = str(e)
        return result

    def get_intraday_minute(
        self,
        symbol: str,
        period: str = "1",
        adjust: str = "",
        limit: int = 480,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "period": str(period),
            "adjust": str(adjust),
            "source": "",
            "data": [],
            "meta": {},
        }
        if not HAS_AKSHARE:
            result["meta"]["error"] = "akshare_not_installed"
            return result
        if not hasattr(ak, "stock_zh_a_minute"):
            result["meta"]["error"] = "stock_zh_a_minute_unavailable"
            return result

        _, code_6, ex = self._split_symbol(symbol)
        ak_symbol = f"{ex}{code_6}" if ex else code_6
        try:
            df = ak.stock_zh_a_minute(symbol=ak_symbol, period=str(period), adjust=str(adjust or ""))
            if isinstance(df, pd.DataFrame) and not df.empty:
                if "day" in df.columns and "date" not in df.columns:
                    df["date"] = pd.to_datetime(df["day"], errors="coerce")
                elif "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                for col in ("open", "high", "low", "close", "volume"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["date"]).sort_values("date")
                if limit > 0 and len(df) > limit:
                    df = df.tail(limit)
                result["data"] = df.to_dict(orient="records")
                result["has_data"] = True
                result["source"] = "akshare.stock_zh_a_minute"
        except Exception as e:
            result["meta"]["error"] = str(e)
        return result

    def get_intraday_ticks(
        self,
        symbol: str,
        limit: int = 240,
        date: Optional[str] = None,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "source": "",
            "data": [],
            "meta": {},
        }
        if not HAS_AKSHARE:
            result["meta"]["error"] = "akshare_not_installed"
            return result

        _, code_6, ex = self._split_symbol(symbol)
        ak_symbol = f"{ex}{code_6}" if ex else code_6

        def _finalize(df: pd.DataFrame, source: str) -> bool:
            if not isinstance(df, pd.DataFrame) or df.empty:
                return False
            if "side" in df.columns:
                df["side"] = df["side"].apply(self._normalize_tick_side)
            if "price" in df.columns:
                df["price"] = pd.to_numeric(df["price"], errors="coerce")
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            if limit > 0 and len(df) > limit:
                df = df.tail(limit)
            result["data"] = df.to_dict(orient="records")
            result["has_data"] = True
            result["source"] = source
            return True

        if hasattr(ak, "stock_intraday_sina"):
            try:
                date_val = date or datetime.today().strftime("%Y%m%d")
                df = ak.stock_intraday_sina(symbol=ak_symbol, date=str(date_val))
                df = df.rename(columns={
                    "ticktime": "time",
                    "price": "price",
                    "volume": "volume",
                    "kind": "side",
                })
                if _finalize(df, "akshare.stock_intraday_sina"):
                    return result
            except Exception as e:
                result["meta"]["intraday_sina_error"] = str(e)

        if hasattr(ak, "stock_intraday_em"):
            try:
                df = ak.stock_intraday_em(symbol=code_6)
                df = df.rename(columns={
                    "时间": "time",
                    "成交价": "price",
                    "手数": "volume",
                    "买卖盘性质": "side",
                })
                if _finalize(df, "akshare.stock_intraday_em"):
                    return result
            except Exception as e:
                result["meta"]["intraday_em_error"] = str(e)

        tx_func = None
        if hasattr(ak, "stock_zh_a_tick_tx_js"):
            tx_func = ak.stock_zh_a_tick_tx_js
        elif hasattr(ak, "stock_zh_a_tick_tx"):
            tx_func = ak.stock_zh_a_tick_tx
        if tx_func:
            try:
                df = tx_func(symbol=ak_symbol)
                df = df.rename(columns={
                    "成交时间": "time",
                    "成交价格": "price",
                    "成交量": "volume",
                    "成交额": "amount",
                    "价格变动": "change",
                    "性质": "side",
                })
                if _finalize(df, "akshare.stock_zh_a_tick_tx"):
                    return result
            except Exception as e:
                result["meta"]["tick_tx_error"] = str(e)

        result["meta"]["error"] = "intraday_ticks_unavailable"
        return result

    def plot_intraday_minute_chart(
        self,
        symbol: str,
        start_time: str = "09:00:00",
        end_time: str = "15:40:00",
        limit: int = 480,
        save_path: Optional[str] = None,
        show: bool = True,
    ) -> str:
        data = self.get_premarket_minute_em(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        source_label = data.get("source") or ""
        if not data.get("has_data"):
            data = self.get_intraday_minute(symbol=symbol, period="1", adjust="", limit=limit)
            source_label = data.get("source") or ""
        if not data.get("has_data"):
            raise RuntimeError("intraday minute data unavailable")

        df = pd.DataFrame(data.get("data") or [])
        if df.empty:
            raise RuntimeError("intraday minute data empty")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        elif "time" in df.columns:
            df["date"] = pd.to_datetime(df["time"], errors="coerce")
        elif "day" in df.columns:
            df["date"] = pd.to_datetime(df["day"], errors="coerce")

        if "close" not in df.columns and "price" in df.columns:
            df["close"] = df["price"]
        if "open" not in df.columns and "close" in df.columns:
            df["open"] = df["close"]
        if "volume" not in df.columns:
            df["volume"] = 0.0

        df = df.dropna(subset=["date", "close"]).sort_values("date")
        if limit > 0 and len(df) > limit:
            df = df.tail(limit)

        x = range(len(df))
        fig = plt.figure(figsize=(12, 6))

        gs = GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)
        ax_price = fig.add_subplot(gs[0])
        ax_vol = fig.add_subplot(gs[1], sharex=ax_price)

        ax_price.plot(x, df["close"], color="#d32f2f", linewidth=1)
        ax_price.set_ylabel("Price")
        ax_price.grid(True, linestyle="--", alpha=0.3)

        ax_vol.bar(x, df["volume"], color="#90a4ae", width=0.6)
        ax_vol.set_ylabel("Volume")
        ax_vol.grid(True, linestyle="--", alpha=0.3)

        xticks_idx = list(range(0, len(x), max(len(x) // 8, 1)))
        xticks_labels = []
        for i in xticks_idx:
            dt = df["date"].iloc[i]
            label = dt.strftime("%H:%M") if hasattr(dt, "strftime") else str(dt)
            xticks_labels.append(label)
        plt.setp(ax_price.get_xticklabels(), visible=False)
        ax_vol.set_xticks(xticks_idx)
        ax_vol.set_xticklabels(xticks_labels, rotation=45, ha="right")

        label = source_label or "intraday"
        fig.suptitle(f"{symbol} 日内分时 ({label})", fontsize=12)

        if save_path is None:
            save_path = f"{symbol}_intraday_min.png"

        fig.subplots_adjust(left=0.08, right=0.98, top=0.9, bottom=0.1, hspace=0.12)
        fig.savefig(save_path, dpi=150)

        if show:
            plt.show()
        else:
            plt.close(fig)

        return save_path

    def plot_intraday_tick_chart(
        self,
        symbol: str,
        date: Optional[str] = None,
        limit: int = 400,
        save_path: Optional[str] = None,
        show: bool = True,
    ) -> str:
        ticks = self.get_intraday_ticks(symbol=symbol, limit=limit, date=date)
        if not ticks.get("has_data"):
            raise RuntimeError("intraday tick data unavailable")

        df = pd.DataFrame(ticks.get("data") or [])
        if df.empty:
            raise RuntimeError("intraday tick data empty")

        if "price" not in df.columns and "close" in df.columns:
            df["price"] = df["close"]

        time_series = df.get("time")
        if time_series is not None:
            time_series = time_series.astype(str)
            if date:
                date_str = str(date)
                if re.fullmatch(r"\d{8}", date_str):
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                dt = pd.to_datetime(date_str + " " + time_series, errors="coerce")
            else:
                dt = pd.to_datetime(time_series, errors="coerce")
            df["dt"] = dt
            if df["dt"].notna().any():
                df = df.sort_values("dt")

        side_series = df.get("side", "").astype(str)
        colors = []
        for val in side_series:
            if "买" in val:
                colors.append("#d32f2f")
            elif "卖" in val:
                colors.append("#388e3c")
            elif "中" in val:
                colors.append("#546e7a")
            else:
                colors.append("#546e7a")

        sizes = None
        if "volume" in df.columns:
            vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            max_vol = max(vol.max(), 1)
            sizes = (vol / max_vol * 40 + 10).tolist()

        x = range(len(df))
        fig = plt.figure(figsize=(12, 5))
        ax = fig.add_subplot(1, 1, 1)
        ax.scatter(x, df["price"], c=colors, s=sizes or 18, alpha=0.7)
        ax.set_ylabel("Price")
        ax.grid(True, linestyle="--", alpha=0.3)

        xticks_idx = list(range(0, len(x), max(len(x) // 8, 1)))
        xticks_labels = []
        if "dt" in df.columns:
            for i in xticks_idx:
                dt = df["dt"].iloc[i]
                label = dt.strftime("%H:%M") if hasattr(dt, "strftime") else str(dt)
                xticks_labels.append(label)
        else:
            xticks_labels = [str(i) for i in xticks_idx]
        ax.set_xticks(xticks_idx)
        ax.set_xticklabels(xticks_labels, rotation=45, ha="right")

        fig.suptitle(f"{symbol} 分笔成交", fontsize=12)

        if save_path is None:
            save_path = f"{symbol}_intraday_tick.png"

        fig.subplots_adjust(left=0.08, right=0.98, top=0.9, bottom=0.12, hspace=0.18)
        fig.savefig(save_path, dpi=150)

        if show:
            plt.show()
        else:
            plt.close(fig)

        return save_path

    def get_indicators(
            self,
            symbol: str,
            timeframe: str = "DAY",
            limit: int = 200,
            ohlc_src: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        真正计算技术指标。

        如果传入 ohlc_src，就直接用它；否则会内部再调一次 get_ohlc。
        返回结构适合 LLM 消化，同时也方便你自己画图或做进一步分析。
        """
        # 如果外面已经取过 K 线，就别重复请求了
        if ohlc_src is None:
            ohlc = self.get_ohlc(symbol=symbol, timeframe=timeframe, limit=limit)
        else:
            ohlc = ohlc_src

        df = self._ohlc_dict_to_df(ohlc)
        ind_df = self._compute_indicators_from_df(df)

        # 为了控制体积，只取最后 limit 条（再缩一遍也无妨）
        if limit > 0 and len(ind_df) > limit:
            ind_df = ind_df.tail(limit)

        records = ind_df.to_dict(orient="records")

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(records),
            "indicators": records,
        }

    def _compute_indicators_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        在 DataFrame 上计算常用技术指标：
        - MA5 / MA10 / MA20 / MA60
        - MACD (diff, dea, hist)
        - RSI14
        - BOLL(20) 上中下轨
        """
        ind = df.copy()

        close = ind["close"]

        # -------- 均线 --------
        ind["ma5"] = close.rolling(window=5).mean()
        ind["ma10"] = close.rolling(window=10).mean()
        ind["ma20"] = close.rolling(window=20).mean()
        ind["ma60"] = close.rolling(window=60).mean()

        # -------- MACD --------
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        diff = ema12 - ema26                      # DIF
        dea = diff.ewm(span=9, adjust=False).mean()  # DEA
        macd_hist = (diff - dea) * 2              # 通常软件里显示的是 2*(DIF-DEA)

        ind["macd_diff"] = diff
        ind["macd_dea"] = dea
        ind["macd_hist"] = macd_hist

        # -------- RSI14（简单版本）--------
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()

        rs = avg_gain / avg_loss
        rsi14 = 100 - (100 / (1 + rs))
        ind["rsi14"] = rsi14

        # -------- BOLL(20, 2σ) --------
        ma20 = ind["ma20"]
        std20 = close.rolling(window=20).std()
        ind["boll_mid"] = ma20
        ind["boll_upper"] = ma20 + 2 * std20
        ind["boll_lower"] = ma20 - 2 * std20

        return ind

    def plot_technical_charts(
        self,
        symbol: str,
        timeframe: str = "DAY",
        limit: int = 200,
        adjust_flag: str = "2",
        save_path: Optional[str] = None,
        show: bool = True,
    ) -> str:
        """
        画出类似炒股软件的四联图：
        1. K线 + MA5/10/20/60 + BOLL
        2. 成交量柱状图
        3. MACD (diff, dea, hist)
        4. RSI14

        返回最终保存的图片路径。
        """
        # 先拿数据
        ohlc = self.get_ohlc(symbol=symbol, timeframe=timeframe, limit=limit, adjust_flag=adjust_flag)
        df = self._ohlc_dict_to_df(ohlc)
        ind_df = self._compute_indicators_from_df(df)

        if limit > 0 and len(ind_df) > limit:
            ind_df = ind_df.tail(limit)

        # x 轴用序号，实际显示日期刻度
        dates = ind_df["date"]
        x = range(len(ind_df))

        close = ind_df["close"]
        open_ = ind_df["open"]
        high = ind_df["high"]
        low = ind_df["low"]
        volume = ind_df["volume"]

        # ------------- 准备画布 -------------
        fig = plt.figure(figsize=(12, 8))
        gs = GridSpec(4, 1, height_ratios=[3, 1, 1, 1], hspace=0.05)

        # --- 1. K线 + 均线 + BOLL ---
        ax_price = fig.add_subplot(gs[0])

        # 画蜡烛（简单版：竖线 + 实心柱）
        for i, (o, h, l, c) in enumerate(zip(open_, high, low, close)):
            color = "red" if c >= o else "green"
            # 上下影线
            ax_price.vlines(i, l, h, color=color, linewidth=0.8)
            # 实体
            ax_price.vlines(i, min(o, c), max(o, c), color=color, linewidth=4)

        # MA
        for ma_col, c in [("ma5", "orange"), ("ma10", "blue"), ("ma20", "purple"), ("ma60", "brown")]:
            if ma_col in ind_df.columns:
                ax_price.plot(x, ind_df[ma_col], label=ma_col.upper(), linewidth=1)

        # BOLL
        if {"boll_upper", "boll_mid", "boll_lower"} <= set(ind_df.columns):
            ax_price.plot(x, ind_df["boll_upper"], label="BOLL_UP", linestyle="--", linewidth=0.8)
            ax_price.plot(x, ind_df["boll_mid"], label="BOLL_MID", linestyle="--", linewidth=0.8)
            ax_price.plot(x, ind_df["boll_lower"], label="BOLL_LOW", linestyle="--", linewidth=0.8)

        ax_price.set_ylabel("Price")
        ax_price.legend(loc="upper left", fontsize=8)
        ax_price.grid(True, linestyle="--", alpha=0.3)

        # --- 2. 成交量 ---
        ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
        for i, (o, c, v) in enumerate(zip(open_, close, volume)):
            color = "red" if c >= o else "green"
            ax_vol.bar(i, v, color=color, width=0.6)
        ax_vol.set_ylabel("Volume")
        ax_vol.grid(True, linestyle="--", alpha=0.3)

        # --- 3. MACD ---
        ax_macd = fig.add_subplot(gs[2], sharex=ax_price)
        ax_macd.bar(x, ind_df["macd_hist"], label="MACD Hist", color="grey", width=0.6)
        ax_macd.plot(x, ind_df["macd_diff"], label="DIF", linewidth=1)
        ax_macd.plot(x, ind_df["macd_dea"], label="DEA", linewidth=1)
        ax_macd.axhline(0, color="black", linewidth=0.5)
        ax_macd.set_ylabel("MACD")
        ax_macd.legend(loc="upper left", fontsize=8)
        ax_macd.grid(True, linestyle="--", alpha=0.3)

        # --- 4. RSI ---
        ax_rsi = fig.add_subplot(gs[3], sharex=ax_price)
        ax_rsi.plot(x, ind_df["rsi14"], label="RSI14", linewidth=1)
        ax_rsi.axhline(70, color="red", linestyle="--", linewidth=0.5)
        ax_rsi.axhline(30, color="green", linestyle="--", linewidth=0.5)
        ax_rsi.set_ylabel("RSI")
        ax_rsi.set_xlabel("Date")
        ax_rsi.legend(loc="upper left", fontsize=8)
        ax_rsi.grid(True, linestyle="--", alpha=0.3)

        # --- x 轴日期刻度 ---
        xticks_idx = list(range(0, len(x), max(len(x) // 10, 1)))
        xticks_labels = [dates.iloc[i].strftime("%Y-%m-%d") for i in xticks_idx]
        plt.setp(ax_price.get_xticklabels(), visible=False)
        plt.setp(ax_vol.get_xticklabels(), visible=False)
        plt.setp(ax_macd.get_xticklabels(), visible=False)
        ax_rsi.set_xticks(xticks_idx)
        ax_rsi.set_xticklabels(xticks_labels, rotation=45, ha="right")

        fig.suptitle(f"{symbol} 技术面四联图", fontsize=12)

        # --- 保存/展示 ---
        if save_path is None:
            save_path = f"{symbol}_tech_chart.png"

        fig.subplots_adjust(left=0.08, right=0.98, top=0.9, bottom=0.12)
        fig.savefig(save_path, dpi=150)

        if show:
            plt.show()
        else:
            plt.close(fig)

        return save_path

    def _ohlc_dict_to_df(self, ohlc: Dict[str, Any]) -> pd.DataFrame:
        """
        将 get_ohlc 返回的 dict 转为 DataFrame，并把数字列转为 float。
        """
        data = ohlc.get("data", [])
        if not data:
            raise RuntimeError("OHLC 数据为空，无法计算技术指标")

        df = pd.DataFrame(data)

        # 转换日期
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 这些字段在 Baostock 里都是字符串，需要转成 float
        float_cols = [
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "turn",
            "pctChg",
            "peTTM",
            "pbMRQ",
            "psTTM",
            "pcfNcfTTM",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 按时间排序
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)

        return df

    # 基本面分析
    # ------- 财务数据基础工具 -------
    def _rs_to_df(self, rs) -> pd.DataFrame:
        """
        将 Baostock 的 query_* 返回结果转成 DataFrame。
        出错直接抛异常，让上层感知。
        """
        if rs.error_code != "0":
            raise RuntimeError(f"Baostock 查询失败: {rs.error_msg} (code={rs.error_code})")

        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame(columns=rs.fields)
        return pd.DataFrame(rows, columns=rs.fields)

    # ---------- 财务 & 公司基本面 ----------

    def _iter_quarters(self, start_year: int, start_quarter: int, max_periods: int):
        """
        从给定的起始年/季度往前枚举若干个季度。
        例如 start_year=2025, start_quarter=4, max_periods=6
        => (2025,4),(2025,3),(2025,2),(2025,1),(2024,4),(2024,3)
        """
        year, quarter = start_year, start_quarter
        for _ in range(max_periods):
            # Baostock 财务数据大致从 2007 年开始，再往前意义也不大
            if year < 2007:
                break
            yield year, quarter
            quarter -= 1
            if quarter == 0:
                year -= 1
                quarter = 4

    def _quarter_date_range(self, year: int, quarter: int) -> (str, str):
        """
        给定年份+季度，返回这个季度的起止日期字符串（YYYY-MM-DD）
        Q1: 01-01 ~ 03-31
        Q2: 04-01 ~ 06-30
        Q3: 07-01 ~ 09-30
        Q4: 10-01 ~ 12-31
        """
        if quarter == 1:
            return f"{year}-01-01", f"{year}-03-31"
        elif quarter == 2:
            return f"{year}-04-01", f"{year}-06-30"
        elif quarter == 3:
            return f"{year}-07-01", f"{year}-09-30"
        elif quarter == 4:
            return f"{year}-10-01", f"{year}-12-31"
        else:
            # 理论上不会走到这里
            raise ValueError(f"非法季度: {quarter}")

    def get_fundamentals(self, symbol: str, max_periods: int = 8) -> Dict[str, Any]:
        """
        获取单只股票的一组“扩展版”基础财务 & 业绩数据。

        一次性拉取最近 max_periods 个季度（从当前季度往前） 的：
        - 季频盈利能力                query_profit_data
        - 季频成长能力                query_growth_data
        - 季频偿债能力                query_balance_data
        - 季频现金流量                query_cash_flow_data
        - 季频营运能力                query_operation_data
        - 季频杜邦指数                query_dupont_data

        另外单独以一个较大的时间区间拉取（不按季度拆分）：
        - 公司业绩快报                query_performance_express_report
        - 公司业绩预告                query_forecast_report

        新增：
        - 如果安装了 AkShare，则额外补充一份“银行业运营指标”模块 bank_ops，
          来自 ak.stock_financial_analysis_indicator，用于成本收入比、非息收入占比等分析。
        """
        normalized_code = normalize_stock_code(symbol)

        today = datetime.today()
        start_year = today.year
        start_quarter = (today.month - 1) // 3 + 1  # 1~4

        # 生成最近 max_periods 个季度列表，例如：
        # 2025Q4, 2025Q3, 2025Q2, 2025Q1, 2024Q4, 2024Q3, 2024Q2, 2024Q1
        periods = list(self._iter_quarters(start_year, start_quarter, max_periods))
        if self.debug:
            logger.debug(f"[get_fundamentals] {symbol} -> 查询季度列表: {periods}")

        profit_frames: List[pd.DataFrame] = []
        growth_frames: List[pd.DataFrame] = []
        balance_frames: List[pd.DataFrame] = []
        cashflow_frames: List[pd.DataFrame] = []
        operation_frames: List[pd.DataFrame] = []
        dupont_frames: List[pd.DataFrame] = []

        express_all_df = pd.DataFrame()
        forecast_all_df = pd.DataFrame()

        def _safe_query_to_df(func, *args, **kwargs) -> pd.DataFrame:
            """安全封装 Baostock 查询，出错时返回空 DataFrame。"""
            try:
                rs = func(*args, **kwargs)
            except Exception as e:
                if self.debug:
                    logger.warning(f"[get_fundamentals] 调用 {func.__name__} 异常: {e}", exc_info=True)
                else:
                    logger.warning(f"[get_fundamentals] 调用 {func.__name__} 异常: {e}")
                return pd.DataFrame()

            try:
                df = self._rs_to_df(rs)
            except Exception as e:
                if self.debug:
                    logger.warning(f"[get_fundamentals] 解析 {func.__name__} 结果异常: {e}", exc_info=True)
                else:
                    logger.warning(f"[get_fundamentals] 解析 {func.__name__} 结果异常: {e}")
                return pd.DataFrame()

            return df

        with baostock_login():
            # 1）按季度拉取六大核心模块
            for year, quarter in periods:
                if self.debug:
                    logger.debug(f"[get_fundamentals] 获取 {normalized_code} {year}Q{quarter} 的财务数据")

                profit_df = _safe_query_to_df(
                    bs.query_profit_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )
                growth_df = _safe_query_to_df(
                    bs.query_growth_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )
                balance_df = _safe_query_to_df(
                    bs.query_balance_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )
                cashflow_df = _safe_query_to_df(
                    bs.query_cash_flow_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )
                operation_df = _safe_query_to_df(
                    bs.query_operation_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )
                dupont_df = _safe_query_to_df(
                    bs.query_dupont_data,
                    code=normalized_code,
                    year=year,
                    quarter=quarter,
                )

                # 打上 year/quarter 标签
                for df in (
                    profit_df,
                    growth_df,
                    balance_df,
                    cashflow_df,
                    operation_df,
                    dupont_df,
                ):
                    if not df.empty:
                        df["year"] = year
                        df["quarter"] = quarter

                if not profit_df.empty:
                    profit_frames.append(profit_df)
                if not growth_df.empty:
                    growth_frames.append(growth_df)
                if not balance_df.empty:
                    balance_frames.append(balance_df)
                if not cashflow_df.empty:
                    cashflow_frames.append(cashflow_df)
                if not operation_df.empty:
                    operation_frames.append(operation_df)
                if not dupont_df.empty:
                    dupont_frames.append(dupont_df)

            # 2）统一大时间窗查询业绩快报 & 业绩预告（不再按季度拆）
            #    这里直接照 Baostock 示例来：从 2010-01-01 拉到今天
            global_start_date = "2010-01-01"
            global_end_date = today.strftime("%Y-%m-%d")

            if self.debug:
                logger.debug(
                    f"[get_fundamentals] {symbol} 业绩快报/预告查询区间: "
                    f"{global_start_date} -> {global_end_date}"
                )

            # 注意这里按 Baostock 示例的调用方式：第一个参数是 code（位置参数）
            express_all_df = _safe_query_to_df(
                bs.query_performance_express_report,
                normalized_code,
                start_date=global_start_date,
                end_date=global_end_date,
            )
            forecast_all_df = _safe_query_to_df(
                bs.query_forecast_report,
                normalized_code,
                start_date=global_start_date,
                end_date=global_end_date,
            )

        # 3）给快报/预告也打上 year/quarter（基于“统计日期”字段），但不做季度过滤
        if not express_all_df.empty and "performanceExpStatDate" in express_all_df.columns:
            dt = pd.to_datetime(express_all_df["performanceExpStatDate"], errors="coerce")
            express_all_df["year"] = dt.dt.year
            express_all_df["quarter"] = ((dt.dt.month - 1) // 3 + 1).astype("Int64")

        if not forecast_all_df.empty and "profitForcastExpStatDate" in forecast_all_df.columns:
            dt = pd.to_datetime(forecast_all_df["profitForcastExpStatDate"], errors="coerce")
            forecast_all_df["year"] = dt.dt.year
            forecast_all_df["quarter"] = ((dt.dt.month - 1) // 3 + 1).astype("Int64")

        def _concat(frames: List[pd.DataFrame]) -> pd.DataFrame:
            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True)

        profit_all = _concat(profit_frames)
        growth_all = _concat(growth_frames)
        balance_all = _concat(balance_frames)
        cashflow_all = _concat(cashflow_frames)
        operation_all = _concat(operation_frames)
        dupont_all = _concat(dupont_frames)

        # 快报 / 预告已经是单独的 DataFrame 了
        express_all = express_all_df.reset_index(drop=True)
        forecast_all = forecast_all_df.reset_index(drop=True)

        # ---------- 对“容易误读单位”的比率字段做一次规范化 ----------
        # 目前主要处理：balance 模块中的资产负债率 liabilityToAsset
        if not balance_all.empty and "liabilityToAsset" in balance_all.columns:
            # Baostock 返回的是字符串，这里先转成 float
            balance_all["liabilityToAsset"] = pd.to_numeric(
                balance_all["liabilityToAsset"],
                errors="coerce",
            )
            max_val = balance_all["liabilityToAsset"].max()

            # 如果数值整体都在 0~1 之间，认为是小数形式的比例，换算成百分比
            if pd.notna(max_val) and max_val <= 1.0:
                balance_all["liabilityToAsset_pct"] = (
                        balance_all["liabilityToAsset"] * 100.0
                ).round(4)
            else:
                # 否则认为原始值已经是百分数（例如 85 表示 85%），直接复制一份到 *_pct 字段
                balance_all["liabilityToAsset_pct"] = balance_all["liabilityToAsset"]

        # ---------- 新增：用 AkShare 补充银行业运营指标（bank_ops 模块） ----------
        bank_ops: Dict[str, Any] = {
            "has_data": False,
            "provider": "akshare.stock_financial_analysis_indicator",
            "raw": [],
            "columns": [],
        }
        if HAS_AKSHARE:
            try:
                # 从 "sz.000001" / "sh.600000" 里提取 6 位代码给 AkShare 用
                m = re.search(r"(\d{6})", normalized_code)
                stock_code = m.group(1) if m else normalized_code[-6:]

                # 使用位置参数，避免不同版本 AkShare 的关键字参数名不一致
                bank_df = ak.stock_financial_analysis_indicator(stock_code)
                if isinstance(bank_df, pd.DataFrame) and not bank_df.empty:
                    # 只保留最近若干行，避免上下文过大（比如最近 10 期）
                    bank_df = bank_df.tail(10).reset_index(drop=True)

                    bank_ops = {
                        "has_data": True,
                        "provider": "akshare.stock_financial_analysis_indicator",
                        "raw": bank_df.to_dict(orient="records"),
                        "columns": list(bank_df.columns),
                    }
            except Exception as e:
                logger.warning(
                    "[get_fundamentals] 调用 AkShare 获取银行运营指标失败: %s",
                    e,
                )
                bank_ops["error"] = str(e)
        else:
            # 本地没装 AkShare，也写进 meta 里，方便上层 Prompt 如实告知“该模块目前无数据”
            bank_ops["error"] = "akshare_not_installed"

        # ---------- 4）只用“六大核心模块”的季度来确定 latest_year/latest_quarter 和 periods_meta ----------
        all_periods_with_data: set[tuple[int, int]] = set()
        for df in (
                profit_all,
                growth_all,
                balance_all,
                cashflow_all,
                operation_all,
                dupont_all,
        ):
            if "year" in df.columns and "quarter" in df.columns:
                for y, q in zip(df["year"], df["quarter"]):
                    try:
                        all_periods_with_data.add((int(y), int(q)))
                    except Exception:
                        continue

        if all_periods_with_data:
            latest_year, latest_quarter = max(
                all_periods_with_data, key=lambda x: (x[0], x[1])
            )
            periods_meta = [
                {"year": y, "quarter": q}
                for (y, q) in sorted(all_periods_with_data)
            ]
        else:
            # 极端情况：所有模块都没拉到数据，就退回到起始定义的年份/季度
            latest_year, latest_quarter = start_year, start_quarter
            periods_meta = []

        if self.debug:
            logger.debug(
                f"[get_fundamentals] {symbol} 汇总："
                f"profit={len(profit_all)}, growth={len(growth_all)}, "
                f"balance={len(balance_all)}, cashflow={len(cashflow_all)}, "
                f"operation={len(operation_all)}, dupont={len(dupont_all)}, "
                f"express={len(express_all)}, forecast={len(forecast_all)}；"
                f"有效季度={periods_meta}"
            )

        # ========== 通过 MCP 再补一层银行运营指标（文本版） ==========
        bank_ops_mcp: dict[str, Any] = {
            "has_data": False,
            "raw_text": "",
        }
        try:
            # 这里我们不判断是不是银行，统一都试一下；akshare_mcp 会自己报错，就当没有数据
            bank_ops_mcp = self.akshare_mcp_client.get_bank_ops(symbol)
        except Exception as e:
            if self.debug:
                logger.warning(
                    "[get_fundamentals] 调用 akshare_mcp 获取银行运营指标失败: %s",
                    e,
                    exc_info=True,
                )
            else:
                logger.warning(
                    "[get_fundamentals] 调用 akshare_mcp 获取银行运营指标失败: %s",
                    e,
                )

        _, code_6, ex = self._split_symbol(symbol)

        # ========== AkShare financial statements fallback (Eastmoney) ==========
        ak_balance_sheet: List[Dict[str, Any]] = []
        ak_income_statement: List[Dict[str, Any]] = []
        ak_cashflow_statement: List[Dict[str, Any]] = []
        ak_performance_report: List[Dict[str, Any]] = []
        ak_performance_express: List[Dict[str, Any]] = []
        akshare_fin_meta: Dict[str, Any] = {
            "enabled": HAS_AKSHARE,
            "used": False,
            "dates": [],
            "errors": {},
        }

        def _quarter_end_str(year: int, quarter: int) -> str:
            if quarter == 1:
                return f"{year}0331"
            if quarter == 2:
                return f"{year}0630"
            if quarter == 3:
                return f"{year}0930"
            return f"{year}1231"

        def _build_quarter_dates() -> List[str]:
            if periods_meta:
                return [_quarter_end_str(p["year"], p["quarter"]) for p in periods_meta if p.get("year") and p.get("quarter")]
            dates = []
            year = today.year
            quarter = (today.month - 1) // 3 + 1
            for _ in range(max_periods):
                dates.append(_quarter_end_str(year, quarter))
                quarter -= 1
                if quarter == 0:
                    quarter = 4
                    year -= 1
            return dates

        def _build_recent_quarters(limit: int) -> List[str]:
            dates = []
            year = today.year
            quarter = (today.month - 1) // 3 + 1
            for _ in range(limit):
                dates.append(_quarter_end_str(year, quarter))
                quarter -= 1
                if quarter == 0:
                    quarter = 4
                    year -= 1
            return dates

        def _filter_stock_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
            if not isinstance(df, pd.DataFrame) or df.empty or not code:
                return pd.DataFrame()
            code_col = None
            for cand in ("股票代码", "证券代码", "代码"):
                if cand in df.columns:
                    code_col = cand
                    break
            if not code_col:
                return pd.DataFrame()
            series = df[code_col].astype(str).str.extract(r"(\d{6})")[0]
            return df.loc[series == code]

        def _normalize_balance_df(df: pd.DataFrame) -> pd.DataFrame:
            if not isinstance(df, pd.DataFrame) or df.empty:
                return pd.DataFrame()
            d = df.copy()
            if "REPORT_DATE" in d.columns and "报告期" not in d.columns:
                d["报告期"] = d["REPORT_DATE"]
            if "NOTICE_DATE" in d.columns and "公告日期" not in d.columns:
                d["公告日期"] = d["NOTICE_DATE"]
            if "资产总计" in d.columns and "资产-总资产" not in d.columns:
                d["资产-总资产"] = d["资产总计"]
            if "TOTAL_ASSETS" in d.columns and "资产-总资产" not in d.columns:
                d["资产-总资产"] = d["TOTAL_ASSETS"]
            if "负债合计" in d.columns and "负债-总负债" not in d.columns:
                d["负债-总负债"] = d["负债合计"]
            if "TOTAL_LIABILITIES" in d.columns and "负债-总负债" not in d.columns:
                d["负债-总负债"] = d["TOTAL_LIABILITIES"]
            if "TOTAL_LIABILITY" in d.columns and "负债-总负债" not in d.columns:
                d["负债-总负债"] = d["TOTAL_LIABILITY"]
            if "所有者权益合计" in d.columns and "股东权益合计" not in d.columns:
                d["股东权益合计"] = d["所有者权益合计"]
            if "TOTAL_EQUITY" in d.columns and "股东权益合计" not in d.columns:
                d["股东权益合计"] = d["TOTAL_EQUITY"]
            if "DEBT_ASSET_RATIO" in d.columns and "资产负债率" not in d.columns:
                d["资产负债率"] = d["DEBT_ASSET_RATIO"]
            if "报告期" not in d.columns:
                for cand in ("报告期", "报告日期", "截止日期", "日期"):
                    if cand in d.columns:
                        d["报告期"] = d[cand]
                        break
            return d

        def _normalize_income_df(df: pd.DataFrame) -> pd.DataFrame:
            if not isinstance(df, pd.DataFrame) or df.empty:
                return pd.DataFrame()
            d = df.copy()
            if "REPORT_DATE" in d.columns and "报告期" not in d.columns:
                d["报告期"] = d["REPORT_DATE"]
            if "NOTICE_DATE" in d.columns and "公告日期" not in d.columns:
                d["公告日期"] = d["NOTICE_DATE"]
            if "TOTAL_OPERATE_INCOME" in d.columns and "营业总收入" not in d.columns:
                d["营业总收入"] = d["TOTAL_OPERATE_INCOME"]
            if "OPERATE_INCOME" in d.columns and "营业总收入" not in d.columns:
                d["营业总收入"] = d["OPERATE_INCOME"]
            if "TOTAL_PROFIT" in d.columns and "利润总额" not in d.columns:
                d["利润总额"] = d["TOTAL_PROFIT"]
            if "OPERATE_PROFIT" in d.columns and "营业利润" not in d.columns:
                d["营业利润"] = d["OPERATE_PROFIT"]
            if "NET_PROFIT" in d.columns and "净利润" not in d.columns:
                d["净利润"] = d["NET_PROFIT"]
            if "PARENT_NET_PROFIT" in d.columns and "归母净利润" not in d.columns:
                d["归母净利润"] = d["PARENT_NET_PROFIT"]
            if "BASIC_EPS" in d.columns and "每股收益" not in d.columns:
                d["每股收益"] = d["BASIC_EPS"]
            return d

        def _normalize_cashflow_df(df: pd.DataFrame) -> pd.DataFrame:
            if not isinstance(df, pd.DataFrame) or df.empty:
                return pd.DataFrame()
            d = df.copy()
            if "REPORT_DATE" in d.columns and "报告期" not in d.columns:
                d["报告期"] = d["REPORT_DATE"]
            if "NOTICE_DATE" in d.columns and "公告日期" not in d.columns:
                d["公告日期"] = d["NOTICE_DATE"]
            if "NETCASH_OPERATE" in d.columns and "经营活动产生的现金流量净额" not in d.columns:
                d["经营活动产生的现金流量净额"] = d["NETCASH_OPERATE"]
            if "NETCASH_INVEST" in d.columns and "投资活动产生的现金流量净额" not in d.columns:
                d["投资活动产生的现金流量净额"] = d["NETCASH_INVEST"]
            if "NETCASH_FINANCE" in d.columns and "筹资活动产生的现金流量净额" not in d.columns:
                d["筹资活动产生的现金流量净额"] = d["NETCASH_FINANCE"]
            if "NETCASH_INCREASE" in d.columns and "现金及现金等价物净增加额" not in d.columns:
                d["现金及现金等价物净增加额"] = d["NETCASH_INCREASE"]
            return d

        def _append_records(df: pd.DataFrame, date_str: str, bucket: List[Dict[str, Any]]) -> None:
            if df is None or df.empty:
                return
            report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            d = df.copy()
            d["报告期"] = report_date
            bucket.extend(d.to_dict(orient="records"))

        def _safe_ak_df(func, date_str: str, key: str) -> pd.DataFrame:
            try:
                df = func(date=date_str)
                if isinstance(df, pd.DataFrame):
                    return df
            except Exception as e:
                akshare_fin_meta["errors"].setdefault(key, []).append({"date": date_str, "error": str(e)})
            return pd.DataFrame()

        def _safe_ak_symbol_df(func, symbol_arg: str, key: str) -> pd.DataFrame:
            try:
                df = func(symbol=symbol_arg)
                if isinstance(df, pd.DataFrame):
                    return df
            except Exception as e:
                akshare_fin_meta["errors"].setdefault(key, []).append({"symbol": symbol_arg, "error": str(e)})
            return pd.DataFrame()

        missing_profit = len(profit_all) == 0
        missing_balance = len(balance_all) == 0
        missing_cashflow = len(cashflow_all) == 0
        missing_perf_express = express_all_df.empty
        force_balance = os.getenv("AKSHARE_FORCE_BALANCE", "1") == "1"
        force_financials = os.getenv("AKSHARE_FORCE_FINANCIALS", "0") == "1"
        force_profit = os.getenv("AKSHARE_FORCE_PROFIT", "1") == "1" or force_financials
        force_cashflow = os.getenv("AKSHARE_FORCE_CASHFLOW", "1") == "1" or force_financials
        force_perf_express = force_financials

        if HAS_AKSHARE and code_6 and (
            missing_profit
            or missing_balance
            or missing_cashflow
            or missing_perf_express
            or force_balance
            or force_profit
            or force_cashflow
            or force_financials
        ):
            target_akshare_periods = max(1, min(int(max_periods), 4))
            candidate_limit = max(target_akshare_periods, min(int(max_periods), 8))
            quarter_dates = [d for d in _build_quarter_dates() if d]
            if len(quarter_dates) < candidate_limit:
                for d in _build_recent_quarters(candidate_limit):
                    if d not in quarter_dates:
                        quarter_dates.append(d)
            quarter_dates = sorted(set(quarter_dates), reverse=True)[:candidate_limit]
            akshare_fin_meta["dates"] = quarter_dates
            for date_str in quarter_dates:
                if missing_balance or force_balance:
                    if ex == "bj" and hasattr(ak, "stock_zcfz_bj_em"):
                        df = _safe_ak_df(ak.stock_zcfz_bj_em, date_str, "balance_bj")
                    else:
                        df = _safe_ak_df(ak.stock_zcfz_em, date_str, "balance")
                    df = _filter_stock_df(df, code_6)
                    _append_records(df, date_str, ak_balance_sheet)
                if missing_profit or force_profit:
                    df = _safe_ak_df(ak.stock_lrb_em, date_str, "income")
                    df = _filter_stock_df(df, code_6)
                    _append_records(df, date_str, ak_income_statement)

                    df = _safe_ak_df(ak.stock_yjbb_em, date_str, "performance_report")
                    df = _filter_stock_df(df, code_6)
                    _append_records(df, date_str, ak_performance_report)
                if missing_cashflow or force_cashflow:
                    df = _safe_ak_df(ak.stock_xjll_em, date_str, "cashflow")
                    df = _filter_stock_df(df, code_6)
                    _append_records(df, date_str, ak_cashflow_statement)
                if missing_perf_express or force_perf_express:
                    df = _safe_ak_df(ak.stock_yjkb_em, date_str, "performance_express")
                    df = _filter_stock_df(df, code_6)
                    _append_records(df, date_str, ak_performance_express)

            def _limit_ak_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                if not records:
                    return records
                def _row_key(row: Dict[str, Any]) -> str:
                    return str(
                        row.get("报告期")
                        or row.get("公告日期")
                        or row.get("REPORT_DATE")
                        or row.get("REPORTDATE")
                        or row.get("REPORT_DATE_NAME")
                        or row.get("END_DATE")
                        or ""
                    )
                ordered = sorted(records, key=_row_key)
                return ordered[-target_akshare_periods:]

            ak_balance_sheet = _limit_ak_records(ak_balance_sheet)
            ak_income_statement = _limit_ak_records(ak_income_statement)
            ak_cashflow_statement = _limit_ak_records(ak_cashflow_statement)
            ak_performance_report = _limit_ak_records(ak_performance_report)
            ak_performance_express = _limit_ak_records(ak_performance_express)

            if len(ak_balance_sheet) < target_akshare_periods:
                ak_symbol = ""
                if ex in ("sh", "sz", "bj"):
                    ak_symbol = f"{ex.upper()}{code_6}"
                elif code_6:
                    ak_symbol = code_6
                if ak_symbol:
                    df = pd.DataFrame()
                    if hasattr(ak, "stock_balance_sheet_by_report_em"):
                        df = _safe_ak_symbol_df(ak.stock_balance_sheet_by_report_em, ak_symbol, "balance_by_report")
                    if df.empty and hasattr(ak, "stock_balance_sheet_by_yearly_em"):
                        df = _safe_ak_symbol_df(ak.stock_balance_sheet_by_yearly_em, ak_symbol, "balance_by_yearly")
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df = _normalize_balance_df(df)
                        ak_balance_sheet = df.to_dict(orient="records")
                        ak_balance_sheet = _limit_ak_records(ak_balance_sheet)

            if len(ak_income_statement) < target_akshare_periods:
                ak_symbol = ""
                if ex in ("sh", "sz", "bj"):
                    ak_symbol = f"{ex.upper()}{code_6}"
                elif code_6:
                    ak_symbol = code_6
                if ak_symbol:
                    df = pd.DataFrame()
                    if hasattr(ak, "stock_profit_sheet_by_report_em"):
                        df = _safe_ak_symbol_df(ak.stock_profit_sheet_by_report_em, ak_symbol, "income_by_report")
                    if df.empty and hasattr(ak, "stock_profit_sheet_by_yearly_em"):
                        df = _safe_ak_symbol_df(ak.stock_profit_sheet_by_yearly_em, ak_symbol, "income_by_yearly")
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df = _normalize_income_df(df)
                        ak_income_statement = df.to_dict(orient="records")
                        ak_income_statement = _limit_ak_records(ak_income_statement)

            if len(ak_cashflow_statement) < target_akshare_periods:
                ak_symbol = ""
                if ex in ("sh", "sz", "bj"):
                    ak_symbol = f"{ex.upper()}{code_6}"
                elif code_6:
                    ak_symbol = code_6
                if ak_symbol:
                    df = pd.DataFrame()
                    if hasattr(ak, "stock_cash_flow_sheet_by_report_em"):
                        df = _safe_ak_symbol_df(ak.stock_cash_flow_sheet_by_report_em, ak_symbol, "cashflow_by_report")
                    if df.empty and hasattr(ak, "stock_cash_flow_sheet_by_yearly_em"):
                        df = _safe_ak_symbol_df(ak.stock_cash_flow_sheet_by_yearly_em, ak_symbol, "cashflow_by_yearly")
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df = _normalize_cashflow_df(df)
                        ak_cashflow_statement = df.to_dict(orient="records")
                        ak_cashflow_statement = _limit_ak_records(ak_cashflow_statement)

            if any((ak_balance_sheet, ak_income_statement, ak_cashflow_statement, ak_performance_report, ak_performance_express)):
                akshare_fin_meta["used"] = True

        def _normalize_report_date(value: Any) -> str:
            if value is None:
                return ""
            text = str(value).strip()
            if not text:
                return ""
            if text.isdigit() and len(text) == 8:
                return f"{text[:4]}-{text[4:6]}-{text[6:]}"
            return text

        def _is_missing_value(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str) and not value.strip():
                return True
            try:
                return bool(pd.isna(value))
            except Exception:
                return False

        def _to_float(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                return float(value)
            except Exception:
                return None

        def _pick_first(record: Dict[str, Any], keys: List[str]) -> Any:
            for key in keys:
                if key in record:
                    val = record.get(key)
                    if not _is_missing_value(val):
                        return val
            return None

        # ---------- Backfill Baostock gaps using AkShare financial statements ----------
        if ak_balance_sheet:
            balance_map: Dict[str, Dict[str, Any]] = {}
            for rec in ak_balance_sheet:
                report_date = _normalize_report_date(
                    rec.get("报告期")
                    or rec.get("公告日期")
                    or rec.get("REPORT_DATE")
                    or rec.get("REPORTDATE")
                    or rec.get("REPORT_DATE_NAME")
                    or rec.get("END_DATE")
                )
                if not report_date:
                    continue
                total_assets = _pick_first(rec, ["资产-总资产", "总资产", "资产总计", "TOTAL_ASSETS"])
                total_liab = _pick_first(rec, ["负债-总负债", "总负债", "负债总计", "TOTAL_LIABILITIES", "TOTAL_LIABILITY"])
                ratio = _pick_first(rec, ["资产负债率", "DEBT_ASSET_RATIO"])
                ratio_val = _to_float(ratio)
                if ratio_val is None:
                    ta = _to_float(total_assets)
                    tl = _to_float(total_liab)
                    if ta:
                        ratio_val = (tl / ta * 100.0) if tl is not None else None
                balance_map[report_date] = {
                    "liabilityToAsset_pct": ratio_val,
                    "total_assets": total_assets,
                    "total_liab": total_liab,
                    "equity": _pick_first(rec, ["股东权益合计", "所有者权益合计", "TOTAL_EQUITY"]),
                }

            if balance_all.empty and balance_map:
                rows = []
                for report_date, info in balance_map.items():
                    rows.append({
                        "code": normalized_code,
                        "statDate": report_date,
                        "liabilityToAsset_pct": info.get("liabilityToAsset_pct"),
                    })
                balance_all = pd.DataFrame(rows)
            elif not balance_all.empty and balance_map:
                if "liabilityToAsset_pct" not in balance_all.columns:
                    balance_all["liabilityToAsset_pct"] = None
                for idx, row in balance_all.iterrows():
                    report_date = _normalize_report_date(row.get("statDate"))
                    if not report_date:
                        continue
                    info = balance_map.get(report_date)
                    if not info:
                        continue
                    if _is_missing_value(row.get("liabilityToAsset_pct")) and info.get("liabilityToAsset_pct") is not None:
                        balance_all.at[idx, "liabilityToAsset_pct"] = info.get("liabilityToAsset_pct")

        if ak_income_statement:
            income_map: Dict[str, Dict[str, Any]] = {}
            for rec in ak_income_statement:
                report_date = _normalize_report_date(
                    rec.get("报告期")
                    or rec.get("公告日期")
                    or rec.get("REPORT_DATE")
                    or rec.get("REPORTDATE")
                    or rec.get("REPORT_DATE_NAME")
                    or rec.get("END_DATE")
                )
                if not report_date:
                    continue
                income_map[report_date] = {
                    "netProfit": _pick_first(rec, ["净利润", "净利润-净利润", "NET_PROFIT", "PARENT_NET_PROFIT"]),
                    "MBRevenue": _pick_first(rec, ["营业总收入", "营业总收入-营业总收入", "营业收入-营业收入", "TOTAL_OPERATE_INCOME", "OPERATE_INCOME"]),
                }

            if profit_all.empty and income_map:
                rows = []
                for report_date, info in income_map.items():
                    rows.append({
                        "code": normalized_code,
                        "statDate": report_date,
                        "netProfit": info.get("netProfit"),
                        "MBRevenue": info.get("MBRevenue"),
                    })
                profit_all = pd.DataFrame(rows)
            elif not profit_all.empty and income_map:
                for idx, row in profit_all.iterrows():
                    report_date = _normalize_report_date(row.get("statDate"))
                    if not report_date:
                        continue
                    info = income_map.get(report_date)
                    if not info:
                        continue
                    if _is_missing_value(row.get("netProfit")) and info.get("netProfit") is not None:
                        profit_all.at[idx, "netProfit"] = info.get("netProfit")
                    if _is_missing_value(row.get("MBRevenue")) and info.get("MBRevenue") is not None:
                        profit_all.at[idx, "MBRevenue"] = info.get("MBRevenue")

        if ak_cashflow_statement:
            cashflow_map: Dict[str, Any] = {}
            for rec in ak_cashflow_statement:
                report_date = _normalize_report_date(
                    rec.get("报告期")
                    or rec.get("公告日期")
                    or rec.get("REPORT_DATE")
                    or rec.get("REPORTDATE")
                    or rec.get("REPORT_DATE_NAME")
                    or rec.get("END_DATE")
                )
                if not report_date:
                    continue
                cashflow_map[report_date] = _pick_first(
                    rec,
                    ["经营性现金流-现金流量净额", "经营性现金流-净额", "经营活动产生的现金流量净额", "NETCASH_OPERATE"],
                )

            if cashflow_all.empty and cashflow_map:
                rows = []
                for report_date, cfo in cashflow_map.items():
                    rows.append({
                        "code": normalized_code,
                        "statDate": report_date,
                        "CFO": cfo,
                    })
                cashflow_all = pd.DataFrame(rows)
            elif not cashflow_all.empty and cashflow_map:
                if "CFO" not in cashflow_all.columns:
                    cashflow_all["CFO"] = None
                for idx, row in cashflow_all.iterrows():
                    report_date = _normalize_report_date(row.get("statDate"))
                    if not report_date:
                        continue
                    cfo = cashflow_map.get(report_date)
                    if cfo is None:
                        continue
                    if _is_missing_value(row.get("CFO")):
                        cashflow_all.at[idx, "CFO"] = cfo

        # ========== AkShare peer comparisons (growth/valuation/dupont) ==========
        peer_growth_records: List[Dict[str, Any]] = []
        peer_valuation_records: List[Dict[str, Any]] = []
        peer_dupont_records: List[Dict[str, Any]] = []
        peer_symbol = ""
        try:
            if ex in ("sh", "sz"):
                peer_symbol = f"{ex.upper()}{code_6}"
            elif code_6:
                peer_symbol = code_6
        except Exception:
            peer_symbol = ""

        if HAS_AKSHARE and peer_symbol:
            try:
                if hasattr(ak, "stock_zh_growth_comparison_em"):
                    df = ak.stock_zh_growth_comparison_em(symbol=peer_symbol)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        peer_growth_records = df.to_dict(orient="records")
            except Exception as e:
                logger.warning("[get_fundamentals] growth comparison failed: %s", e)
            try:
                if hasattr(ak, "stock_zh_valuation_comparison_em"):
                    df = ak.stock_zh_valuation_comparison_em(symbol=peer_symbol)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        peer_valuation_records = df.to_dict(orient="records")
            except Exception as e:
                logger.warning("[get_fundamentals] valuation comparison failed: %s", e)
            try:
                if hasattr(ak, "stock_zh_dupont_comparison_em"):
                    df = ak.stock_zh_dupont_comparison_em(symbol=peer_symbol)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        peer_dupont_records = df.to_dict(orient="records")
            except Exception as e:
                logger.warning("[get_fundamentals] dupont comparison failed: %s", e)

        return {
            "source": "baostock",
            "symbol": symbol,
            "code": normalized_code,
            "latest_year": latest_year,
            "latest_quarter": latest_quarter,
            "periods": periods_meta,

            "profit": profit_all.to_dict(orient="records"),
            "growth": growth_all.to_dict(orient="records"),
            "balance": balance_all.to_dict(orient="records"),
            "cashflow": cashflow_all.to_dict(orient="records"),
            "operation": operation_all.to_dict(orient="records"),
            "dupont": dupont_all.to_dict(orient="records"),
            "performance_express": express_all.to_dict(orient="records"),
            "forecast": forecast_all.to_dict(orient="records"),

            # AkShare 数值版银行运营指标
            "bank_ops": bank_ops,
            # 通过 MCP 获取的银行运营指标文本
            "bank_ops_mcp": bank_ops_mcp,
            # AkShare 财报补充（缺失时启用）
            "balance_sheet_em": ak_balance_sheet,
            "income_statement_em": ak_income_statement,
            "cashflow_statement_em": ak_cashflow_statement,
            "performance_report_em": ak_performance_report,
            "performance_express_em": ak_performance_express,
            "akshare_financials_meta": akshare_fin_meta,
            "peer_growth_comparison": peer_growth_records,
            "peer_valuation_comparison": peer_valuation_records,
            "peer_dupont_comparison": peer_dupont_records,
        }


    # ---------- 宏观经济 & 货币环境 ----------

    def get_macro_indicators(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            start_year: Optional[int] = None,
            end_year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取一组“宏观货币环境 / 经济景气”指标。

        设计思路：
        - 优先用 AkShare 拿「最新」的宏观数据：
          * LPR：macro_china_lpr
          * 货币供应量（含 M2 等）：macro_china_supply_of_money
        - 保留原来的 Baostock 指标作为补充（主要覆盖 2015 年之前）：
          * 存款利率：query_deposit_rate_data()
          * 贷款利率：query_loan_rate_data()
          * 存款准备金率：query_required_reserve_ratio_data()
          * 货币供应量（月度、年末）：query_money_supply_data_month/year()

        参数：
        - start_date/end_date: "YYYY-MM-DD"，可选；如果不给，则按年份估一个窗口。
        - start_year/end_year: 也可以直接指定年份，优先级高于 date。

        返回结构（兼容旧版，同时暴露更细的分组）：
        {
            "source": {"akshare": bool, "baostock": bool},
            "time_window": {"start_year": int, "end_year": int},

            # AkShare 指标
            "akshare": {
                "lpr": [...],
                "money_supply": [...],
            },

            # Baostock 指标（兼容旧 key）
            "baostock": {
                "deposit_rate": [...],
                "loan_rate": [...],
                "required_reserve_ratio": [...],
                "money_supply_month": [...],
                "money_supply_year": [...],
            },

            # 为了不改 Agent 侧 prompt，保留一层扁平别名：
            "lpr": [...],
            "money_supply": [...],
            "deposit_rate": [...],
            "loan_rate": [...],
            "required_reserve_ratio": [...],
            "money_supply_month": [...],
            "money_supply_year": [...],
        }
        """
        today = datetime.today().date()

        # 1) 统一年份窗口
        if end_year is None:
            if end_date:
                try:
                    end_year = int(end_date[:4])
                except Exception:
                    end_year = today.year
            else:
                end_year = today.year

        if start_year is None:
            if start_date:
                try:
                    start_year = int(start_date[:4])
                except Exception:
                    start_year = end_year - 10
            else:
                # 默认看最近 10 年
                start_year = end_year - 10

        start_date_full = f"{start_year}-01-01"
        end_date_full = f"{end_year}-12-31"
        start_month = f"{start_year}-01"
        end_month = f"{end_year}-12"

        # 先初始化所有 DataFrame，保证即便出错也有变量存在
        deposit_df = pd.DataFrame()
        loan_df = pd.DataFrame()
        rrr_df = pd.DataFrame()
        money_month_df = pd.DataFrame()
        money_year_df = pd.DataFrame()
        lpr_df = pd.DataFrame()
        money_supply_df = pd.DataFrame()

        # ---------- 2) AkShare 部分：最新宏观数据 ----------
        if HAS_AKSHARE:
            # 2.1 LPR 利率
            try:
                lpr_raw = ak.macro_china_lpr()
                if isinstance(lpr_raw, pd.DataFrame) and not lpr_raw.empty:
                    if "TRADE_DATE" in lpr_raw.columns:
                        lpr_raw["date"] = pd.to_datetime(lpr_raw["TRADE_DATE"])
                        lpr_raw = lpr_raw[
                            (lpr_raw["date"].dt.year >= start_year)
                            & (lpr_raw["date"].dt.year <= end_year)
                            ]
                    lpr_df = lpr_raw
            except Exception as e:
                logger.warning(f"[get_macro_indicators] 获取 LPR 失败: {e}")

            # 2.2 货币供应量（含 M2）
            try:
                money_raw = ak.macro_china_supply_of_money()
                if isinstance(money_raw, pd.DataFrame) and not money_raw.empty:
                    # 日期列名字在不同版本里可能有差异，这里做个兜底
                    date_col = None
                    for candidate in ("月份", "统计时间", "month", "date"):
                        if candidate in money_raw.columns:
                            date_col = candidate
                            break
                    if date_col is not None:
                        money_raw["date"] = pd.to_datetime(
                            money_raw[date_col],
                            errors="coerce"  # 解析失败的设为 NaT
                        )
                        money_raw = money_raw.dropna(subset=["date"])
                        money_raw = money_raw[
                            (money_raw["date"].dt.year >= start_year)
                            & (money_raw["date"].dt.year <= end_year)
                            ]

                    money_supply_df = money_raw
            except Exception as e:
                logger.warning(f"[get_macro_indicators] 获取货币供应量(AkShare)失败: {e}")
        else:
            logger.info(
                "[get_macro_indicators] 未安装 akshare，仅使用 Baostock 宏观指标；"
                "建议 pip install akshare 以获取 2015 年之后的宏观数据。"
            )

        # ---------- 3) Baostock 部分：保持兼容 ----------
        try:
            with baostock_login():
                deposit_df = self._rs_to_df(
                    bs.query_deposit_rate_data(
                        start_date=start_date_full,
                        end_date=end_date_full,
                    )
                )
                loan_df = self._rs_to_df(
                    bs.query_loan_rate_data(
                        start_date=start_date_full,
                        end_date=end_date_full,
                    )
                )
                rrr_df = self._rs_to_df(
                    bs.query_required_reserve_ratio_data(
                        start_date=start_date_full,
                        end_date=end_date_full,
                    )
                )
                money_month_df = self._rs_to_df(
                    bs.query_money_supply_data_month(
                        start_date=start_month,
                        end_date=end_month,
                    )
                )
                money_year_df = self._rs_to_df(
                    bs.query_money_supply_data_year(
                        start_date=str(start_year),
                        end_date=str(end_year),
                    )
                )
        except Exception as e:
            logger.warning(f"[get_macro_indicators] Baostock 宏观接口失败: {e}")

        # ---------- 4) 组装返回 ----------
        result: Dict[str, Any] = {
            "source": {"akshare": HAS_AKSHARE, "baostock": True},
            "time_window": {"start_year": start_year, "end_year": end_year},
            "akshare": {
                "lpr": lpr_df.to_dict(orient="records"),
                "money_supply": money_supply_df.to_dict(orient="records"),
            },
            "baostock": {
                "deposit_rate": deposit_df.to_dict(orient="records"),
                "loan_rate": loan_df.to_dict(orient="records"),
                "required_reserve_ratio": rrr_df.to_dict(orient="records"),
                "money_supply_month": money_month_df.to_dict(orient="records"),
                "money_supply_year": money_year_df.to_dict(orient="records"),
            },
        }

        # 扁平 alias：兼容之前 Agent 里直接用的 key
        result["lpr"] = result["akshare"]["lpr"]
        result["money_supply"] = result["akshare"]["money_supply"]
        result["deposit_rate"] = result["baostock"]["deposit_rate"]
        result["loan_rate"] = result["baostock"]["loan_rate"]
        result["required_reserve_ratio"] = result["baostock"]["required_reserve_ratio"]
        result["money_supply_month"] = result["baostock"]["money_supply_month"]
        result["money_supply_year"] = result["baostock"]["money_supply_year"]

        return result

    def get_sw_industry_index(self, symbol: str) -> Dict[str, Any]:
        """
        使用 AkShare 的申万行业指数接口，基于个股代码识别其申万二级行业，
        并拉取该行业指数的月度历史行情，用作行业景气代理。

        设计思路：
        1）调用 ak.sw_index_second_info() 拿到所有申万二级行业的基本信息（行业代码、名称、上级行业、市盈率等）；
        2）遍历每个行业，调用 ak.sw_index_cons / ak.index_component_sw 拿成分股列表，匹配当前股票代码；
        3）一旦匹配成功，记录行业信息，并用 ak.index_hist_sw 或 ak.sw_index_daily 拉取该行业指数的行情数据（默认取最近 ~10 年月线）。
        """
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "sw_industry": {},     # 行业基本信息 + 估值
            "index_monthly": {},   # 行业指数月线行情，格式兼容 _summarize_ohlc_series
            "index_daily": {},  # 新增：行业指数日线行情
            "meta": {},            # 调试信息
        }

        # 1. AkShare 不可用就直接返回
        if not HAS_AKSHARE:
            result["meta"]["error"] = "akshare_not_installed"
            return result

        try:
            # ---------- 1) 标准化股票代码 ----------
            normalized = normalize_stock_code(symbol)
            m = re.search(r"(\d{6})", normalized)
            code_6 = m.group(1) if m else normalized[-6:]

            if normalized.startswith("sh."):
                exch = "SH"
            elif normalized.startswith("sz."):
                exch = "SZ"
            else:
                exch = ""
            stock_code_full = f"{code_6}.{exch}" if exch else code_6
            industry_guess = ""
            try:
                with baostock_login():
                    industry_df = self._rs_to_df(bs.query_stock_industry(code=normalized))
                if isinstance(industry_df, pd.DataFrame) and not industry_df.empty:
                    row0 = industry_df.iloc[0].to_dict()
                    for key in ("industry", "industryClassification", "industry_name", "industryClass", "industry_name_1"):
                        val = row0.get(key)
                        if isinstance(val, str) and val.strip():
                            industry_guess = val.strip()
                            break
            except Exception as e:
                result["meta"]["industry_lookup_error"] = str(e)

            # ---------- 2) 申万二级行业基本信息 ----------
            # sw_index_second_info: 返回申万二级行业列表及估值指标
            # 字段一般包含：行业代码、行业名称、上级行业、成份个数、静态市盈率、TTM市盈率、市净率、股息率等 :contentReference[oaicite:0]{index=0}
            def _norm_name(name: str) -> str:
                text = re.sub(r"[\\s\\-_/（）()]+", "", str(name or "")).lower()
                for token in ("申万", "行业", "指数"):
                    text = text.replace(token, "")
                for token in ("Ⅰ", "Ⅱ", "Ⅲ", "Ⅳ", "Ⅴ", "I", "II", "III", "IV", "V"):
                    text = text.replace(token, "")
                return text

            def _pick_first(row: Any, keys: List[str]) -> str:
                for key in keys:
                    if isinstance(row, dict):
                        val = row.get(key)
                    else:
                        val = row.get(key) if hasattr(row, "get") else None
                    if val not in (None, ""):
                        return str(val)
                return ""

            def _match_by_name(df: pd.DataFrame, target: str, name_cols: List[str]) -> Optional[Any]:
                if not isinstance(df, pd.DataFrame) or df.empty or not target:
                    return None
                target_norm = _norm_name(target)
                if not target_norm:
                    return None
                for name_col in name_cols:
                    if name_col not in df.columns:
                        continue
                    series = df[name_col].astype(str)
                    for idx, val in series.items():
                        val_norm = _norm_name(val)
                        if val_norm == target_norm:
                            return df.loc[idx]
                    for idx, val in series.items():
                        val_norm = _norm_name(val)
                        if val_norm and (val_norm in target_norm or target_norm in val_norm):
                            return df.loc[idx]
                return None

            info_df = pd.DataFrame()
            try:
                info_df = ak.sw_index_second_info()
            except Exception as e:
                logger.warning(f"调用 ak.sw_index_second_info 失败: {e}")
                result["meta"]["sw_index_second_info_error"] = str(e)

            first_df = pd.DataFrame()
            try:
                if hasattr(ak, "sw_index_first_info"):
                    first_df = ak.sw_index_first_info()
            except Exception as e:
                result["meta"]["sw_index_first_info_error"] = str(e)

            realtime_first_df = pd.DataFrame()
            realtime_second_df = pd.DataFrame()
            try:
                if hasattr(ak, "index_realtime_sw"):
                    realtime_first_df = ak.index_realtime_sw(symbol="一级行业")
                    realtime_second_df = ak.index_realtime_sw(symbol="二级行业")
            except Exception as e:
                result["meta"]["index_realtime_sw_error"] = str(e)

            matched_row = None
            matched_index_code = None  # 例如 801010
            matched_source = ""

            if industry_guess:
                result["meta"]["industry_guess"] = industry_guess
                for label, df, name_cols in (
                    ("sw_second_info", info_df, ["行业名称", "index_name", "名称", "name"]),
                    ("sw_first_info", first_df, ["行业名称", "index_name", "名称", "name"]),
                    ("sw_realtime_second", realtime_second_df, ["指数名称", "名称", "name"]),
                    ("sw_realtime_first", realtime_first_df, ["指数名称", "名称", "name"]),
                ):
                    row = _match_by_name(df, industry_guess, name_cols)
                    if row is not None:
                        row_dict = row.to_dict() if hasattr(row, "to_dict") else row
                        code_raw = _pick_first(row_dict, ["行业代码", "指数代码", "index_code", "code"])
                        code_match = re.search(r"\d{6}", code_raw)
                        matched_index_code = code_match.group(0) if code_match else code_raw.split(".")[0]
                        matched_row = row
                        matched_source = label
                        break

            # ---------- 3) 用成分股反查：股票 -> 申万二级行业 ----------
            if matched_row is None and isinstance(info_df, pd.DataFrame) and not info_df.empty:
                for _, row in info_df.iterrows():
                    industry_code_raw = str(row.get("行业代码") or row.get("index_code") or "").strip()
                    if not industry_code_raw:
                        continue

                    # sw_index_second_info 里通常是类似 '801010.SI'，index_hist_sw / sw_index_cons 用的是前面的数字部分
                    index_code = industry_code_raw.split(".")[0]

                    cons_df = None
                    # 3.1 优先尝试 sw_index_cons（申万一级/二级行业成份股）
                    try:
                        if hasattr(ak, "sw_index_cons"):
                            cons_df = ak.sw_index_cons(symbol=index_code)
                    except Exception as e:
                        logger.debug(f"sw_index_cons({index_code}) 失败，尝试 index_component_sw: {e}")

                    # 3.2 sw_index_cons 不可用时，退回 index_component_sw（申万指数成份股）
                    if cons_df is None:
                        try:
                            if hasattr(ak, "index_component_sw"):
                                cons_df = ak.index_component_sw(symbol=index_code)
                        except Exception as e:
                            logger.debug(f"index_component_sw({index_code}) 失败: {e}")
                            cons_df = None

                    if not isinstance(cons_df, pd.DataFrame) or cons_df.empty:
                        continue

                    # 找到证券代码列（不同版本字段名可能不同，做兼容）
                    code_col = None
                    for cand in ("stock_code", "证券代码", "代码", "code"):
                        if cand in cons_df.columns:
                            code_col = cand
                            break
                    if not code_col:
                        continue

                    codes = cons_df[code_col].astype(str)
                    if (codes == stock_code_full).any() or codes.str.endswith(code_6).any():
                        matched_row = row
                        matched_index_code = index_code
                        matched_source = "sw_component"
                        break

            if matched_row is None or not matched_index_code:
                result["meta"]["error"] = "stock_not_found_in_sw_second_industry"
                return result
            if matched_source:
                result["meta"]["match_source"] = matched_source

            # ---------- 4) 整理行业基本信息 + 估值 ----------
            def _to_float_safe(v):
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            row_dict = matched_row.to_dict() if hasattr(matched_row, "to_dict") else matched_row
            industry_code_raw = _pick_first(row_dict, ["行业代码", "指数代码", "index_code", "code"])
            industry_name_raw = _pick_first(row_dict, ["行业名称", "指数名称", "name"])
            parent_name_raw = _pick_first(row_dict, ["上级行业", "parent_industry", "parent"])
            constituent_raw = _pick_first(row_dict, ["成份个数", "成分个数", "constituent_count", "count"])

            code_match = re.search(r"\d{6}", industry_code_raw)
            index_code_norm = code_match.group(0) if code_match else matched_index_code
            index_code_fetch = index_code_norm or matched_index_code
            sw_industry: Dict[str, Any] = {
                "index_code": index_code_norm,                         # 801010
                "industry_code": industry_code_raw,   # 801010.SI 或其他
                "industry_name": industry_name_raw,
                "parent_industry": parent_name_raw,
                "constituent_count": int(_to_float_safe(constituent_raw) or 0),
            }

            # 不同版本的列名可能略有差异，这里做一下兼容
            for field_key, col_candidates in {
                "pe_static": ("静态市盈率", "市盈率", "PE"),
                "pe_ttm": ("TTM市盈率", "TTM(滚动)市盈率", "滚动市盈率", "市盈率TTM"),
                "pb": ("市净率", "PB"),
                "dividend_yield": ("静态股息率", "股息率"),
            }.items():
                value = None
                for col in col_candidates:
                    if col in info_df.columns:
                        value = _to_float_safe(matched_row.get(col))
                        if value is not None:
                            break
                if value is not None:
                    sw_industry[field_key] = value

            result["sw_industry"] = sw_industry

            # ---------- 5) 行业指数月度行情 ----------
            hist_df = None
            try:
                # 优先用 index_hist_sw：period 支持 day/week/month:contentReference[oaicite:4]{index=4}
                if hasattr(ak, "index_hist_sw"):
                    hist_df = ak.index_hist_sw(symbol=index_code_fetch, period="month")
                # 某些版本没有 index_hist_sw 时，用 sw_index_daily 兜底:contentReference[oaicite:5]{index=5}
                elif hasattr(ak, "sw_index_daily"):
                    hist_df = ak.sw_index_daily(
                        index_code=index_code_fetch,
                        start_date="2000-01-01",
                        end_date=datetime.today().strftime("%Y-%m-%d"),
                    )
            except Exception as e:
                logger.warning(f"获取申万行业指数行情失败: {e}")
                result["meta"]["index_hist_error"] = str(e)
                hist_df = None

            index_series = {"symbol": index_code_fetch, "period": "month", "data": []}
            if isinstance(hist_df, pd.DataFrame) and not hist_df.empty:
                # 尝试识别日期列、收盘价列
                date_col = None
                for cand in ("日期", "date", "交易日期", "TRADE_DATE"):
                    if cand in hist_df.columns:
                        date_col = cand
                        break

                close_col = None
                for cand in ("收盘", "收盘价", "close", "CLOSE"):
                    if cand in hist_df.columns:
                        close_col = cand
                        break

                if date_col and close_col:
                    df = hist_df.copy()
                    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
                    df = df.dropna(subset=["date"]).sort_values("date")
                    # 取最近 120 个月（约 10 年）
                    df = df.tail(120)

                    records: List[Dict[str, Any]] = []
                    for _, r in df.iterrows():
                        rec: Dict[str, Any] = {
                            "date": r["date"].strftime("%Y-%m-%d"),
                            "close": _to_float_safe(r.get(close_col)),
                        }
                        # 其他可选字段
                        extra_cols = {
                            "open": ("开盘", "开盘价", "open", "OPEN"),
                            "high": ("最高", "最高价", "high", "HIGH"),
                            "low": ("最低", "最低价", "low", "LOW"),
                            "volume": ("成交量", "成交量(股)", "volume", "VOL"),
                        }
                        for key, cand_cols in extra_cols.items():
                            for col in cand_cols:
                                if col in df.columns:
                                    rec[key] = _to_float_safe(r.get(col))
                                    break
                        records.append(rec)

                    index_series["data"] = records

            result["index_monthly"] = index_series
            # ---------- 6) 行业指数日度行情（新增） ----------
            hist_df_d = None
            try:
                if hasattr(ak, "index_hist_sw"):
                    hist_df_d = ak.index_hist_sw(symbol=index_code_fetch, period="day")
                elif hasattr(ak, "sw_index_daily"):
                    hist_df_d = ak.sw_index_daily(
                        index_code=index_code_fetch,
                        start_date="2000-01-01",
                        end_date=datetime.today().strftime("%Y-%m-%d"),
                    )
            except Exception as e:
                logger.warning(f"获取申万行业指数日线行情失败: {e}")
                result["meta"]["index_hist_day_error"] = str(e)
                hist_df_d = None

            index_series_d = {"symbol": index_code_fetch, "period": "day", "data": []}
            if isinstance(hist_df_d, pd.DataFrame) and not hist_df_d.empty:
                date_col = None
                for cand in ("日期", "date", "交易日期", "TRADE_DATE"):
                    if cand in hist_df_d.columns:
                        date_col = cand
                        break

                close_col = None
                for cand in ("收盘", "收盘价", "close", "CLOSE"):
                    if cand in hist_df_d.columns:
                        close_col = cand
                        break

                if date_col and close_col:
                    df = hist_df_d.copy()
                    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
                    df = df.dropna(subset=["date"]).sort_values("date")
                    # 取最近 480 个交易日（约两年），你也可以改成 240（一年）
                    df = df.tail(480)

                    records_d: List[Dict[str, Any]] = []
                    for _, r in df.iterrows():
                        rec: Dict[str, Any] = {
                            "date": r["date"].strftime("%Y-%m-%d"),
                            "close": _to_float_safe(r.get(close_col)),
                        }
                        extra_cols = {
                            "open": ("开盘", "开盘价", "open", "OPEN"),
                            "high": ("最高", "最高价", "high", "HIGH"),
                            "low": ("最低", "最低价", "low", "LOW"),
                            "volume": ("成交量", "成交量(股)", "volume", "VOL"),
                        }
                        for key, cand_cols in extra_cols.items():
                            for col in cand_cols:
                                if col in df.columns:
                                    rec[key] = _to_float_safe(r.get(col))
                                    break
                        records_d.append(rec)

                    index_series_d["data"] = records_d

            result["index_daily"] = index_series_d
            result["has_data"] = bool(sw_industry) and (
                    bool(result.get("index_daily", {}).get("data")) or bool(result.get("index_monthly", {}).get("data"))
            )
            return result

        except Exception as e:
            logger.warning(f"get_sw_industry_index({symbol}) 失败: {e}")
            result["meta"]["exception"] = str(e)
            return result



    def _normalize_index_df(self, df: Any, limit: int = 480) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame(), []

        def _pick(cands: List[str]) -> Optional[str]:
            for cand in cands:
                if cand in df.columns:
                    return cand
            return None

        def _to_float_safe(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        date_col = _pick(["date", "日期", "交易日期", "TRADE_DATE", "trade_date"])
        close_col = _pick(["close", "收盘", "收盘价", "CLOSE"])
        if not date_col or not close_col:
            return pd.DataFrame(), []

        open_col = _pick(["open", "开盘", "开盘价", "OPEN"])
        high_col = _pick(["high", "最高", "最高价", "HIGH"])
        low_col = _pick(["low", "最低", "最低价", "LOW"])
        volume_col = _pick(["volume", "成交量", "成交量(股)", "VOL", "vol"])

        work = df.copy()
        work["date"] = pd.to_datetime(work[date_col], errors="coerce")
        work = work.dropna(subset=["date"]).sort_values("date")
        if limit > 0 and len(work) > limit:
            work = work.tail(limit)

        out = pd.DataFrame()
        out["date"] = work["date"]
        out["close"] = work[close_col].apply(_to_float_safe)
        if open_col:
            out["open"] = work[open_col].apply(_to_float_safe)
        if high_col:
            out["high"] = work[high_col].apply(_to_float_safe)
        if low_col:
            out["low"] = work[low_col].apply(_to_float_safe)
        if volume_col:
            out["volume"] = work[volume_col].apply(_to_float_safe)

        records: List[Dict[str, Any]] = []
        for _, r in out.iterrows():
            rec: Dict[str, Any] = {
                "date": r["date"].strftime("%Y-%m-%d"),
                "close": r.get("close"),
            }
            if "open" in out.columns:
                rec["open"] = r.get("open")
            if "high" in out.columns:
                rec["high"] = r.get("high")
            if "low" in out.columns:
                rec["low"] = r.get("low")
            if "volume" in out.columns:
                rec["volume"] = r.get("volume")
            records.append(rec)

        return out, records

    def _resample_monthly_records(self, df: pd.DataFrame, limit: int = 120) -> List[Dict[str, Any]]:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty or "date" not in df.columns:
            return []
        work = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(work["date"]):
            work["date"] = pd.to_datetime(work["date"], errors="coerce")
        work = work.dropna(subset=["date"]).sort_values("date").set_index("date")

        agg: Dict[str, str] = {}
        if "open" in work.columns:
            agg["open"] = "first"
        if "high" in work.columns:
            agg["high"] = "max"
        if "low" in work.columns:
            agg["low"] = "min"
        if "close" in work.columns:
            agg["close"] = "last"
        if "volume" in work.columns:
            agg["volume"] = "sum"
        if not agg:
            return []

        monthly = work.resample("M").agg(agg)
        if "close" in monthly.columns:
            monthly = monthly.dropna(subset=["close"])
        if limit > 0 and len(monthly) > limit:
            monthly = monthly.tail(limit)

        records: List[Dict[str, Any]] = []
        for idx, row in monthly.iterrows():
            rec: Dict[str, Any] = {"date": idx.strftime("%Y-%m-%d")}
            for col in agg:
                try:
                    rec[col] = float(row[col])
                except (TypeError, ValueError):
                    rec[col] = None
            records.append(rec)
        return records

    def _fetch_index_daily_by_item(
        self, item: Dict[str, Any], limit: int = 480
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], str]:
        df = None
        fetch_source = ""
        fetcher = str(item.get("fetcher") or "")
        symbol = str(item.get("symbol") or item.get("code") or "").strip()
        code_digits = str(item.get("code") or "").strip()

        if HAS_AKSHARE and symbol:
            if fetcher == "index_hist_sw":
                try:
                    if hasattr(ak, "index_hist_sw"):
                        df = ak.index_hist_sw(symbol=str(symbol), period="day")
                        fetch_source = "akshare.index_hist_sw"
                except Exception:
                    df = None
                if (df is None or df.empty) and hasattr(ak, "sw_index_daily"):
                    try:
                        df = ak.sw_index_daily(
                            index_code=str(symbol),
                            start_date="2000-01-01",
                            end_date=datetime.today().strftime("%Y-%m-%d"),
                        )
                        fetch_source = "akshare.sw_index_daily"
                    except Exception:
                        df = None
            elif fetcher == "stock_zh_index_daily":
                try:
                    if hasattr(ak, "stock_zh_index_daily"):
                        df = ak.stock_zh_index_daily(symbol=str(symbol))
                        fetch_source = "akshare.stock_zh_index_daily"
                except Exception:
                    df = None
                if (df is None or df.empty) and hasattr(ak, "stock_zh_index_daily_em"):
                    try:
                        df = ak.stock_zh_index_daily_em(symbol=str(symbol))
                        fetch_source = "akshare.stock_zh_index_daily_em"
                    except Exception:
                        df = None
                if (df is None or df.empty) and hasattr(ak, "index_zh_a_hist"):
                    if not code_digits:
                        m = re.search(r"(\d{6})", symbol)
                        code_digits = m.group(1) if m else ""
                    if code_digits:
                        try:
                            df = ak.index_zh_a_hist(
                                symbol=code_digits,
                                period="daily",
                                start_date="20000101",
                                end_date=datetime.today().strftime("%Y%m%d"),
                            )
                            fetch_source = "akshare.index_zh_a_hist"
                        except Exception:
                            df = None
            elif fetcher == "index_zh_a_hist":
                if not code_digits:
                    m = re.search(r"(\d{6})", symbol)
                    code_digits = m.group(1) if m else ""
                if code_digits and hasattr(ak, "index_zh_a_hist"):
                    try:
                        df = ak.index_zh_a_hist(
                            symbol=code_digits,
                            period="daily",
                            start_date="20000101",
                            end_date=datetime.today().strftime("%Y%m%d"),
                        )
                        fetch_source = "akshare.index_zh_a_hist"
                    except Exception:
                        df = None

        if isinstance(df, pd.DataFrame) and not df.empty:
            norm_df, records = self._normalize_index_df(df, limit=limit)
            if records:
                return norm_df, records, fetch_source or "akshare"

        bs_df = None
        if symbol:
            try:
                ohlc = self.get_ohlc(symbol=symbol, timeframe="DAY", limit=limit)
                bs_df = self._ohlc_dict_to_df(ohlc)
            except Exception:
                bs_df = None

        if isinstance(bs_df, pd.DataFrame) and not bs_df.empty:
            norm_df, records = self._normalize_index_df(bs_df, limit=limit)
            if records:
                return norm_df, records, "baostock"

        return pd.DataFrame(), [], ""

    def get_industry_index(self, symbol: str, stock_name: str = "") -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "has_data": False,
            "symbol": symbol,
            "industry_index": {},
            "sw_industry": {},
            "index_monthly": {},
            "index_daily": {},
            "meta": {},
        }

        normalized = ""
        try:
            normalized = normalize_stock_code(symbol)
        except Exception:
            normalized = symbol

        industry_info: Dict[str, Any] = {}
        if normalized:
            try:
                with baostock_login():
                    industry_df = self._rs_to_df(bs.query_stock_industry(code=normalized))
                if isinstance(industry_df, pd.DataFrame) and not industry_df.empty:
                    industry_info = industry_df.iloc[0].to_dict()
            except Exception as e:
                result["meta"]["industry_lookup_error"] = str(e)

        keywords: List[str] = []
        for val in industry_info.values():
            if isinstance(val, str) and val.strip():
                keywords.append(val.strip())
        if stock_name and isinstance(stock_name, str) and stock_name.strip():
            keywords.append(stock_name.strip())

        keywords = _expand_keywords(keywords)
        deduped: List[str] = []
        seen = set()
        for kw in keywords:
            key = kw.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(kw.strip())
        keywords = deduped
        if not keywords:
            result["meta"]["error"] = "no_industry_keywords"
            return result
        chinese_keywords = [kw for kw in keywords if re.search(r"[\u4e00-\u9fff]", kw)]
        use_keywords = chinese_keywords or keywords
        result["meta"]["industry_keywords"] = use_keywords
        if chinese_keywords:
            result["meta"]["industry_keywords_cn"] = chinese_keywords
            result["meta"]["industry_keywords_full"] = keywords

        catalog = _ensure_index_catalog()
        result["meta"]["catalog_path"] = _INDEX_CATALOG_PATH
        result["meta"]["catalog_size"] = len(catalog.get("items") or [])
        if catalog.get("errors"):
            result["meta"]["catalog_errors"] = catalog.get("errors")

        candidates = _rank_index_candidates(catalog.get("items") or [], use_keywords)
        if chinese_keywords and not candidates:
            candidates = _rank_index_candidates(catalog.get("items") or [], keywords)
            result["meta"]["industry_keywords"] = keywords
            result["meta"]["industry_keywords_fallback"] = True
        result["meta"]["candidate_count"] = len(candidates)
        if candidates:
            result["meta"]["candidates"] = [
                {
                    "symbol": c.get("symbol"),
                    "code": c.get("code"),
                    "name": c.get("name"),
                    "score": c.get("match_score"),
                    "source": c.get("source"),
                    "fetcher": c.get("fetcher"),
                    "category": c.get("category"),
                }
                for c in candidates[:5]
            ]
        if not candidates:
            result["meta"]["error"] = "no_index_candidates"
            return result

        for cand in candidates[:8]:
            norm_df, records, fetch_source = self._fetch_index_daily_by_item(cand)
            if not records:
                continue

            monthly_records = self._resample_monthly_records(norm_df, limit=120)
            index_meta = {
                "index_code": cand.get("code") or cand.get("symbol"),
                "index_symbol": cand.get("symbol"),
                "industry_name": cand.get("name"),
                "index_name": cand.get("name"),
                "index_kind": cand.get("category") or "index",
                "index_provider": cand.get("provider") or "akshare",
                "index_source": cand.get("source"),
                "index_fetcher": cand.get("fetcher"),
                "match_score": cand.get("match_score"),
                "match_keyword": cand.get("match_keyword"),
                "fetch_source": fetch_source,
            }

            result["industry_index"] = index_meta
            result["sw_industry"] = index_meta
            result["index_daily"] = {"symbol": cand.get("symbol"), "period": "day", "data": records}
            if monthly_records:
                result["index_monthly"] = {"symbol": cand.get("symbol"), "period": "month", "data": monthly_records}
            result["meta"]["matched_index"] = {
                "symbol": cand.get("symbol"),
                "name": cand.get("name"),
                "score": cand.get("match_score"),
                "fetch_source": fetch_source,
            }
            result["has_data"] = True
            return result

        result["meta"]["error"] = "index_data_unavailable"
        return result

    def get_bank_ops_indicators(self, symbol: str) -> Dict[str, Any]:
        """
        使用 AkShare 获取“银行业常用运营指标”，补充 Baostock 没有的：
        - 利息收入 / 非息收入占比
        - 成本收入比等（取决于数据源提供的字段）

        这里不强行手工计算，而是把 AkShare 的财务分析指标表原样塞进 JSON，
        交给 LLM 去识别哪些字段有用。

        返回示例：
        {
            "has_data": True/False,
            "provider": "akshare.stock_financial_analysis_indicator",
            "raw": [...],      # 最近若干期的指标
            "columns": [...],  # 列名，方便调试
        }
        """
        if not HAS_AKSHARE:
            return {
                "has_data": False,
                "provider": "akshare.stock_financial_analysis_indicator",
                "error": "akshare_not_installed",
                "raw": [],
                "columns": [],
            }

        # 使用前面已经定义的 normalize_stock_code 工具函数
        normalized = normalize_stock_code(symbol)
        # 从 "sh.600000" / "sz.000001" 中提取 6 位代码给 AkShare 用
        m = re.search(r"(\d{6})", normalized)
        stock_code = m.group(1) if m else symbol[-6:]

        try:
            # 使用位置参数方式，兼容不同版本 AkShare 的函数签名
            df = ak.stock_financial_analysis_indicator(stock_code)
        except Exception as e:
            logger.warning(
                "[get_bank_ops_indicators] 调用 ak.stock_financial_analysis_indicator 失败: %s",
                e,
            )
            return {
                "has_data": False,
                "provider": "akshare.stock_financial_analysis_indicator",
                "error": str(e),
                "raw": [],
                "columns": [],
            }

        if df is None or df.empty:
            return {
                "has_data": False,
                "provider": "akshare.stock_financial_analysis_indicator",
                "raw": [],
                "columns": [],
            }

        # 只保留最近若干行，避免上下文过大
        df = df.tail(10).reset_index(drop=True)

        return {
            "has_data": True,
            "provider": "akshare.stock_financial_analysis_indicator",
            "raw": df.to_dict(orient="records"),
            "columns": list(df.columns),
        }

    # ---------- 公司静态信息：基本资料 + 行业 + 指数成分 ----------

    def get_stock_profile(self, symbol: str) -> Dict[str, Any]:
        """
        获取单只股票的“静态信息 + 行业 + 指数成分”：
        - 证券基本资料：query_stock_basic
        - 行业分类：    query_stock_industry
        - 是否属于上证50 / 沪深300 / 中证500 成分股：
          query_sz50_stocks / query_hs300_stocks / query_zz500_stocks
        """
        normalized_code = normalize_stock_code(symbol)

        with baostock_login():
            # 1）基本资料（上市日期、退市日期、类型、状态等）
            basic_df = self._rs_to_df(bs.query_stock_basic(code=normalized_code))

            # 2）行业分类（申万一级行业）
            industry_df = self._rs_to_df(bs.query_stock_industry(code=normalized_code))

            # 3）指数成分股列表（整表拉回来做包含判断）
            sz50_df = self._rs_to_df(bs.query_sz50_stocks())
            hs300_df = self._rs_to_df(bs.query_hs300_stocks())
            zz500_df = self._rs_to_df(bs.query_zz500_stocks())

        basic = basic_df.to_dict(orient="records")[0] if not basic_df.empty else {}
        industry = industry_df.to_dict(orient="records")[0] if not industry_df.empty else {}

        def _in_index(df: pd.DataFrame) -> bool:
            if df.empty:
                return False
            cols = {c.lower(): c for c in df.columns}
            code_col = cols.get("code") or cols.get("security_code") or cols.get("sec_code")
            if not code_col:
                return False
            return normalized_code in set(df[code_col])

        return {
            "source": "baostock",
            "symbol": symbol,
            "code": normalized_code,
            "basic": basic,        # ipoDate / outDate / type / status 等
            "industry": industry,  # industry / industryClassification 等
            "index_membership": {
                "is_sz50": _in_index(sz50_df),
                "is_hs300": _in_index(hs300_df),
                "is_zz500": _in_index(zz500_df),
            },
        }




