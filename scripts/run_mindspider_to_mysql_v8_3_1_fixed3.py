#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run MindSpider/MediaCrawler crawl (optional) and write/normalize results into MySQL.

Key guarantees for downstream "消息面/agent" usage:
1) For *all tables* in the target MySQL schema, we ensure columns:
   - stock_code (VARCHAR(16))
   - ingested_at (DATETIME)
2) We stamp stock_code + ingested_at onto newly ingested rows (and optionally backfill recent rows)
   so later you can query by (stock_code, time) without manual per-stock SQL edits.
3) We avoid "B 站卡住" by applying per-platform subprocess timeouts (bilibili uses a smaller default).
"""

from __future__ import annotations

import argparse
import configparser
import contextlib
import datetime as dt
import importlib.util
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pymysql


# ---------------------------
# Platform naming conventions
# ---------------------------
# Display names (used in your index/mindspider_post + for querying/feeding AI)
DISPLAY_PLATFORMS = ["wb", "xhs", "zhihu", "tieba", "bilibili", "douyin", "kuaishou"]

# MediaCrawler "platform" flags expected by MindSpider's platform_crawler.py
DISPLAY_TO_CRAWLER = {
    "wb": "wb",
    "xhs": "xhs",
    "zhihu": "zhihu",
    "tieba": "tieba",
    "bilibili": "bili",
    "douyin": "dy",
    "kuaishou": "ks",
}

# Raw tables created by MediaCrawler store (MySQL schema init script you ran)
RAW_TABLES = {
    "wb": {"content": "weibo_note", "comment": "weibo_note_comment"},
    "xhs": {"content": "xhs_note", "comment": "xhs_note_comment"},
    "zhihu": {"content": "zhihu_content", "comment": "zhihu_comment"},
    "tieba": {"content": "tieba_note", "comment": "tieba_comment"},
    "bilibili": {"content": "bilibili_video", "comment": "bilibili_video_comment"},
    "douyin": {"content": "douyin_aweme", "comment": "douyin_aweme_comment"},
    "kuaishou": {"content": "kuaishou_video", "comment": "kuaishou_video_comment"},
}

# Prefixes used to identify "platform tables" for tagging
PLATFORM_TABLE_PREFIXES = (
    "weibo_",
    "xhs_",
    "zhihu_",
    "tieba_",
    "bilibili_",
    "douyin_",
    "kuaishou_",
    "dy_",  # some tables use dy_ prefix
)

# Candidate columns for backfilling recent rows
KEYWORD_COL_CANDIDATES = ("source_keyword", "keyword", "query", "search_keyword", "search_word")
TIME_COL_CANDIDATES = (
    "crawled_at",
    "published_at",
    "created_at",
    "create_at",
    "create_time",
    "add_ts",
    "last_modify_ts",
    "update_ts",
    "updated_at",
)


@dataclass
class MySQLCfg:
    host: str
    port: int
    user: str
    password: str
    database: str


# ------------
# INI / MySQL
# ------------
def _read_mysql_from_ini(config_path: str) -> MySQLCfg:
    cp = configparser.ConfigParser(strict=False)
    # config.ini may be written with UTF-8 or ANSI; try UTF-8 first.
    try:
        cp.read(config_path, encoding="utf-8")
    except UnicodeDecodeError:
        cp.read(config_path, encoding="gbk")

    if not cp.has_section("mysql"):
        raise RuntimeError("config.ini 缺少 [mysql] 段（需要 host/port/user/password/database）。")

    def _get(key: str, default: Optional[str] = None) -> str:
        if cp.has_option("mysql", key):
            return cp.get("mysql", key)
        if default is None:
            raise RuntimeError(f"[mysql] 缺少配置项: {key}")
        return default

    return MySQLCfg(
        host=_get("host", "127.0.0.1"),
        port=int(_get("port", "3306")),
        user=_get("user"),
        password=_get("password"),
        database=_get("database"),
    )


def _connect_mysql(cfg: MySQLCfg) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=False,
    )


def _inject_mysql_env(cfg: MySQLCfg) -> None:
    """
    Make MediaCrawler use MySQL by injecting common env vars.
    This works with your existing MindSpider integration approach (no need to edit MediaCrawler).
    """
    os.environ["DB_DIALECT"] = "mysql"
    os.environ["DB_HOST"] = cfg.host
    os.environ["DB_PORT"] = str(cfg.port)
    os.environ["DB_USER"] = cfg.user
    os.environ["DB_PASSWORD"] = cfg.password
    os.environ["DB_NAME"] = cfg.database
    os.environ["MYSQL_HOST"] = cfg.host
    os.environ["MYSQL_PORT"] = str(cfg.port)
    os.environ["MYSQL_USER"] = cfg.user
    os.environ["MYSQL_PASSWORD"] = cfg.password
    os.environ["MYSQL_DATABASE"] = cfg.database


def _find_node_path() -> Optional[str]:
    for cmd in ("node", "nodejs"):
        path = shutil.which(cmd)
        if path:
            return path
    candidates = [
        r"C:\Program Files\nodejs\node.exe",
        r"C:\Program Files (x86)\nodejs\node.exe",
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "nodejs", "node.exe"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def _ensure_execjs_runtime() -> None:
    node_path = _find_node_path()
    if not node_path:
        print(
            "[WARN] Node.js not found; Zhihu sign may fail. Install Node and set EXECJS_RUNTIME=Node.",
            file=sys.stderr,
        )
        return
    node_dir = os.path.dirname(node_path)
    if node_dir and node_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = node_dir + os.pathsep + os.environ.get("PATH", "")
    os.environ["EXECJS_RUNTIME"] = "Node"


# -----------------
# DB Introspection
# -----------------
def _list_tables(conn: pymysql.connections.Connection, db: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=%s AND table_type='BASE TABLE'",
            (db,),
        )
        return [r[0] for r in cur.fetchall()]


def _list_columns(conn: pymysql.connections.Connection, db: str, table: str) -> Dict[str, str]:
    """
    Return {column_name: data_type} for a table.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s",
            (db, table),
        )
        return {r[0]: r[1].lower() for r in cur.fetchall()}


def _index_exists(conn: pymysql.connections.Connection, db: str, table: str, index_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema=%s AND table_name=%s AND index_name=%s",
            (db, table, index_name),
        )
        return int(cur.fetchone()[0]) > 0


def _ensure_column(
    conn: pymysql.connections.Connection,
    table: str,
    col: str,
    ddl: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN {ddl}")


def _ensure_stock_tagging_schema(
    conn: pymysql.connections.Connection,
    db: str,
    tag_all_tables: bool,
) -> None:
    """
    Ensure `stock_code` and `ingested_at` exist (and add index) for:
    - all tables in the schema (if tag_all_tables)
    - otherwise: platform-related tables only.
    """
    tables = _list_tables(conn, db)
    for t in tables:
        if not tag_all_tables:
            if not any(t.startswith(p) for p in PLATFORM_TABLE_PREFIXES):
                # keep DB style for non-platform tables when user disables "all"
                continue

        cols = _list_columns(conn, db, t)
        alter_needed = []
        if "stock_code" not in cols:
            alter_needed.append("`stock_code` VARCHAR(16) NULL")
        if "ingested_at" not in cols:
            alter_needed.append("`ingested_at` DATETIME NULL")
        if "stock_codes" not in cols:
            alter_needed.append("`stock_codes` TEXT NULL")
        if alter_needed:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE `{t}` " + ", ".join(f"ADD COLUMN {x}" for x in alter_needed))

        # indexes (non-blocking best effort)
        cols = _list_columns(conn, db, t)
        if "stock_code" in cols:
            idx1 = f"idx_{t}_stock_code"
            if len(idx1) > 64:
                idx1 = f"idx_{t[:45]}_stock_code"
            if not _index_exists(conn, db, t, idx1):
                with conn.cursor() as cur:
                    cur.execute(f"CREATE INDEX `{idx1}` ON `{t}` (`stock_code`)")

            # composite index if table has a usable time column
            time_col = None
            for c in TIME_COL_CANDIDATES:
                if c in cols:
                    time_col = c
                    break
            if time_col:
                idx2 = f"idx_{t}_stock_time"
                if len(idx2) > 64:
                    idx2 = f"idx_{t[:45]}_stock_time"
                if not _index_exists(conn, db, t, idx2):
                    with conn.cursor() as cur:
                        cur.execute(f"CREATE INDEX `{idx2}` ON `{t}` (`stock_code`, `{time_col}`)")


def _snapshot_max_ids(conn: pymysql.connections.Connection, db: str) -> Dict[str, int]:
    """
    Snapshot MAX(id) for all tables that have numeric `id` column.
    Used to stamp stock_code accurately for rows inserted during this run.
    """
    max_ids: Dict[str, int] = {}
    for t in _list_tables(conn, db):
        cols = _list_columns(conn, db, t)
        if "id" not in cols:
            continue
        if cols["id"] not in ("int", "bigint", "mediumint", "smallint", "tinyint"):
            continue
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(`id`), 0) FROM `{t}`")
            max_ids[t] = int(cur.fetchone()[0] or 0)
    return max_ids


def _stamp_new_rows_by_id(
    conn: pymysql.connections.Connection,
    db: str,
    max_ids_before: Dict[str, int],
    stock_code: str,
    ingested_at: dt.datetime,
) -> Dict[str, int]:
    """
    For each table with numeric `id`, stamp rows with id > max_id_before.
    """
    updated: Dict[str, int] = {}
    for t, max_id in max_ids_before.items():
        cols = _list_columns(conn, db, t)
        if "stock_code" not in cols:
            continue
        has_stock_codes = "stock_codes" in cols
        if has_stock_codes:
            set_sql = (
                "`stock_code`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `stock_code` END, "
                "`ingested_at`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `ingested_at` END, "
                "`stock_codes`=CASE "
                "WHEN `stock_codes` IS NULL OR `stock_codes`='' THEN %s "
                "WHEN FIND_IN_SET(%s, `stock_codes`) THEN `stock_codes` "
                "ELSE CONCAT(`stock_codes`, ',', %s) END"
            )
            params = [stock_code, ingested_at, stock_code, stock_code, stock_code, max_id]
            where_sql = "`id`>%s"
        else:
            set_sql = "`stock_code`=%s, `ingested_at`=%s"
            params = [stock_code, ingested_at, max_id]
            where_sql = "`id`>%s AND ( `stock_code` IS NULL OR `stock_code`='' )"
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{t}` SET {set_sql} WHERE {where_sql}",
                params,
            )
            updated[t] = cur.rowcount
    return updated


def _infer_time_filter(cols: Dict[str, str], since: dt.datetime) -> Optional[Tuple[str, object]]:
    """
    Return (time_col, threshold_value) where threshold_value is suitable for SQL comparison.
    Handles DATETIME and epoch-like ints (sec or ms).
    """
    for c in TIME_COL_CANDIDATES:
        if c not in cols:
            continue
        dtype = cols[c]
        if dtype in ("datetime", "timestamp", "date"):
            return (c, since)
        if dtype in ("bigint", "int", "mediumint", "smallint", "tinyint"):
            # heuristic: *_ts typically ms; create_time often seconds
            if c.endswith("_ts"):
                return (c, int(since.timestamp() * 1000))
            return (c, int(since.timestamp()))
    return None


def _backfill_recent_rows(
    conn: pymysql.connections.Connection,
    db: str,
    stock_code: str,
    ingested_at: dt.datetime,
    keywords: Sequence[str],
    days: int,
    only_platform_tables: bool = True,
) -> Dict[str, int]:
    """
    Backfill existing recent rows (within last `days`) that currently lack stock_code.
    This addresses your current situation: crawler has inserted many rows (e.g., bilibili comments)
    but they were not tagged.
    """
    since = ingested_at - dt.timedelta(days=days)
    updated: Dict[str, int] = {}

    tables = _list_tables(conn, db)
    for t in tables:
        if only_platform_tables and (not any(t.startswith(p) for p in PLATFORM_TABLE_PREFIXES)):
            continue

        cols = _list_columns(conn, db, t)
        if "stock_code" not in cols:
            continue
        has_stock_codes = "stock_codes" in cols

        # Prefer keyword-based filter if available
        keyword_col = None
        for kc in KEYWORD_COL_CANDIDATES:
            if kc in cols:
                keyword_col = kc
                break

        if keyword_col and keywords:
            placeholders = ", ".join(["%s"] * len(keywords))
            if has_stock_codes:
                set_sql = (
                    "`stock_code`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `stock_code` END, "
                    "`ingested_at`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `ingested_at` END, "
                    "`stock_codes`=CASE "
                    "WHEN `stock_codes` IS NULL OR `stock_codes`='' THEN %s "
                    "WHEN FIND_IN_SET(%s, `stock_codes`) THEN `stock_codes` "
                    "ELSE CONCAT(`stock_codes`, ',', %s) END"
                )
                where_sql = f"WHERE `{keyword_col}` IN ({placeholders})"
                params = [stock_code, ingested_at, stock_code, stock_code, stock_code, *keywords]
            else:
                set_sql = "`stock_code`=%s, `ingested_at`=%s"
                where_sql = f"WHERE ( `stock_code` IS NULL OR `stock_code`='' ) AND `{keyword_col}` IN ({placeholders})"
                params = [stock_code, ingested_at, *keywords]
            sql = f"UPDATE `{t}` SET {set_sql} {where_sql}"
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.rowcount:
                    updated[t] = updated.get(t, 0) + cur.rowcount
            continue

        # Else use time column filter
        tf = _infer_time_filter(cols, since)
        if tf:
            time_col, threshold = tf
            if has_stock_codes:
                set_sql = (
                    "`stock_code`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `stock_code` END, "
                    "`ingested_at`=CASE WHEN `stock_code` IS NULL OR `stock_code`='' THEN %s ELSE `ingested_at` END, "
                    "`stock_codes`=CASE "
                    "WHEN `stock_codes` IS NULL OR `stock_codes`='' THEN %s "
                    "WHEN FIND_IN_SET(%s, `stock_codes`) THEN `stock_codes` "
                    "ELSE CONCAT(`stock_codes`, ',', %s) END"
                )
                where_sql = f"WHERE `{time_col}` >= %s"
                params = [stock_code, ingested_at, stock_code, stock_code, stock_code, threshold]
            else:
                set_sql = "`stock_code`=%s, `ingested_at`=%s"
                where_sql = f"WHERE ( `stock_code` IS NULL OR `stock_code`='' ) AND `{time_col}` >= %s"
                params = [stock_code, ingested_at, threshold]
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE `{t}` SET {set_sql} {where_sql}",
                    params,
                )
                if cur.rowcount:
                    updated[t] = updated.get(t, 0) + cur.rowcount

    return updated


# -----------------------------
# MindSpider module importing
# -----------------------------
def _find_file(root: Path, filename: str) -> Optional[Path]:
    # Prefer expected MindSpider layout
    preferred = root / "MindSpider" / "DeepSentimentCrawling" / filename
    if preferred.exists():
        return preferred
    # Fallback walk
    for p in root.rglob(filename):
        return p
    return None


def _import_platform_crawler(mindspider_dir: Path):
    pc_path = _find_file(mindspider_dir, "platform_crawler.py")
    if not pc_path:
        raise RuntimeError(f"未找到 platform_crawler.py（mindspider_dir={mindspider_dir}）")

    # Make its folder importable
    sys.path.insert(0, str(pc_path.parent))
    # Some modules in MindSpider use relative imports expecting DeepSentimentCrawling root
    sys.path.insert(0, str(pc_path.parent.parent))

    spec = importlib.util.spec_from_file_location("platform_crawler", str(pc_path))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    print(f"[LOCATE] platform_crawler.py: {pc_path}")
    return mod


# ----------------
# Crawl execution
# ----------------
def _run_crawl_per_platform(
    platform_crawler_mod,
    display_platforms: Sequence[str],
    keywords: Sequence[str],
    max_notes: int,
    timeout_sec: int,
    bilibili_timeout_sec: int,
) -> Dict[str, str]:
    """
    Run MediaCrawler per platform (avoid long blocking on a single platform).
    Returns {platform: "ok"/"failed: ..."} for logging.
    """
    # Resolve crawler class name differences
    if hasattr(platform_crawler_mod, "PlatformCrawler"):
        CrawlerCls = getattr(platform_crawler_mod, "PlatformCrawler")
    elif hasattr(platform_crawler_mod, "PlatformCrawlerManager"):
        CrawlerCls = getattr(platform_crawler_mod, "PlatformCrawlerManager")
    else:
        raise RuntimeError("platform_crawler.py 中未找到 PlatformCrawler / PlatformCrawlerManager")

    crawler = CrawlerCls()

    # Patch subprocess.run inside platform_crawler module to enforce per-platform timeout
    original_run = platform_crawler_mod.subprocess.run

    def _patched_run(*args, **kwargs):
        # platform_crawler.py passes timeout=3600; we tighten it.
        # Use the current override from a closure var.
        t = getattr(_patched_run, "_override_timeout", None)
        if t is not None:
            kwargs["timeout"] = min(int(kwargs.get("timeout", t)), int(t))
        return original_run(*args, **kwargs)

    platform_crawler_mod.subprocess.run = _patched_run  # type: ignore

    results: Dict[str, str] = {}
    try:
        for dp in display_platforms:
            cp = DISPLAY_TO_CRAWLER.get(dp, dp)
            tsec = bilibili_timeout_sec if dp == "bilibili" else timeout_sec
            _patched_run._override_timeout = tsec  # type: ignore

            # run_crawler signature differs across MindSpider versions; inspect and call safely
            try:
                sig = str(getattr(platform_crawler_mod.PlatformCrawler, "run_crawler", ""))
            except Exception:
                sig = ""

            print(f"[CRAWL] platform={dp} (crawler={cp}) timeout={tsec}s max_notes={max_notes} kw={len(keywords)}")
            try:
                fn = getattr(crawler, "run_crawler")
            except AttributeError:
                # fallback: use run_multi_platform with single platform
                fn = getattr(crawler, "run_multi_platform_crawl_by_keywords")

            # Call with best-effort kwargs
            try:
                fn(platform=cp, keywords=list(keywords), login_type="qrcode", max_notes=max_notes)  # type: ignore
                results[dp] = "ok"
            except TypeError:
                # older versions: max_notes_per_keyword
                try:
                    fn(platform=cp, keywords=list(keywords), login_type="qrcode", max_notes_per_keyword=max_notes)  # type: ignore
                    results[dp] = "ok"
                except TypeError:
                    # positional minimal
                    fn(cp, list(keywords), "qrcode", max_notes)  # type: ignore
                    results[dp] = "ok"
            except subprocess.TimeoutExpired:
                results[dp] = f"failed: timeout({tsec}s)"
            except Exception as e:
                results[dp] = f"failed: {e!r}"
    finally:
        platform_crawler_mod.subprocess.run = original_run  # type: ignore

    return results


# -----------------------------
# Unified index + mindspider_post
# -----------------------------
DDL_STOCK_MEDIA_INDEX = """
CREATE TABLE IF NOT EXISTS `bettafish_stock_media_index` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `stock_code` VARCHAR(16) NOT NULL,
  `platform` VARCHAR(32) NOT NULL,
  `keyword` VARCHAR(128) NULL,
  `source_table` VARCHAR(64) NOT NULL,
  `source_id` VARCHAR(128) NOT NULL,
  `url` VARCHAR(1024) NULL,
  `title` VARCHAR(512) NULL,
  `author` VARCHAR(128) NULL,
  `published_at` DATETIME NULL,
  `content` LONGTEXT NULL,
  `like_count` INT NULL,
  `comment_count` INT NULL,
  `content_time` DATETIME NULL,
  `crawled_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stock_platform_source` (`stock_code`, `platform`, `source_table`, `source_id`),
  KEY `idx_stock_time` (`stock_code`, `content_time`),
  KEY `idx_stock_crawled` (`stock_code`, `crawled_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

DDL_MINDSPIDER_POST = """
CREATE TABLE IF NOT EXISTS `mindspider_post` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `stock_code` VARCHAR(16) NOT NULL,
  `platform` VARCHAR(32) NOT NULL,
  `keyword` VARCHAR(128) NULL,
  `platform_post_id` VARCHAR(128) NOT NULL,
  `url` VARCHAR(1024) NULL,
  `title` VARCHAR(512) NULL,
  `author` VARCHAR(128) NULL,
  `published_at` DATETIME NULL,
  `content` LONGTEXT NULL,
  `like_count` INT NULL,
  `crawled_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stock_platform_post` (`stock_code`, `platform`, `platform_post_id`),
  KEY `idx_stock_pub` (`stock_code`, `published_at`),
  KEY `idx_stock_crawled` (`stock_code`, `crawled_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

INDEX_TABLE_COLUMNS = {
    "bettafish_stock_media_index": {
        "stock_code": "VARCHAR(16) NULL",
        "platform": "VARCHAR(32) NULL",
        "keyword": "VARCHAR(128) NULL",
        "source_table": "VARCHAR(64) NULL",
        "source_id": "VARCHAR(128) NULL",
        "url": "VARCHAR(1024) NULL",
        "title": "VARCHAR(512) NULL",
        "author": "VARCHAR(128) NULL",
        "published_at": "DATETIME NULL",
        "content": "LONGTEXT NULL",
        "like_count": "INT NULL",
        "comment_count": "INT NULL",
        "content_time": "DATETIME NULL",
        "crawled_at": "DATETIME NULL",
    },
    "mindspider_post": {
        "stock_code": "VARCHAR(16) NULL",
        "platform": "VARCHAR(32) NULL",
        "keyword": "VARCHAR(128) NULL",
        "platform_post_id": "VARCHAR(128) NULL",
        "url": "VARCHAR(1024) NULL",
        "title": "VARCHAR(512) NULL",
        "author": "VARCHAR(128) NULL",
        "published_at": "DATETIME NULL",
        "content": "LONGTEXT NULL",
        "like_count": "INT NULL",
        "crawled_at": "DATETIME NULL",
    },
}

INDEX_TABLE_INDEXES = {
    "bettafish_stock_media_index": [
        ("uk_stock_platform_source", True, ["stock_code", "platform", "source_table", "source_id"]),
        ("idx_stock_time", False, ["stock_code", "content_time"]),
        ("idx_stock_crawled", False, ["stock_code", "crawled_at"]),
    ],
    "mindspider_post": [
        ("uk_stock_platform_post", True, ["stock_code", "platform", "platform_post_id"]),
        ("idx_stock_pub", False, ["stock_code", "published_at"]),
        ("idx_stock_crawled", False, ["stock_code", "crawled_at"]),
    ],
}


def _ensure_table_columns(
    conn: pymysql.connections.Connection,
    db: str,
    table: str,
    expected_cols: Dict[str, str],
) -> None:
    cols = _list_columns(conn, db, table)
    for col, ddl in expected_cols.items():
        if col in cols:
            continue
        _ensure_column(conn, table, col, f"`{col}` {ddl}")


def _ensure_table_indexes(
    conn: pymysql.connections.Connection,
    db: str,
    table: str,
    index_specs: Sequence[Tuple[str, bool, Sequence[str]]],
) -> None:
    cols = _list_columns(conn, db, table)
    for idx_name, unique, idx_cols in index_specs:
        if not all(c in cols for c in idx_cols):
            continue
        if _index_exists(conn, db, table, idx_name):
            continue
        col_sql = ", ".join(f"`{c}`" for c in idx_cols)
        with conn.cursor() as cur:
            if unique:
                cur.execute(f"CREATE UNIQUE INDEX `{idx_name}` ON `{table}` ({col_sql})")
            else:
                cur.execute(f"CREATE INDEX `{idx_name}` ON `{table}` ({col_sql})")


def _ensure_index_tables(conn: pymysql.connections.Connection, db: str) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_STOCK_MEDIA_INDEX)
        cur.execute(DDL_MINDSPIDER_POST)

    tables = set(_list_tables(conn, db))
    for table, col_defs in INDEX_TABLE_COLUMNS.items():
        if table in tables:
            _ensure_table_columns(conn, db, table, col_defs)
    for table, index_defs in INDEX_TABLE_INDEXES.items():
        if table in tables:
            _ensure_table_indexes(conn, db, table, index_defs)


def _safe_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _epoch_to_dt(x: Optional[int]) -> Optional[dt.datetime]:
    if x is None:
        return None
    # seconds vs ms heuristic
    if x > 10_000_000_000:  # ms
        x = x // 1000
    try:
        return dt.datetime.fromtimestamp(int(x))
    except Exception:
        return None


def _upsert_stock_media_index(
    conn: pymysql.connections.Connection,
    db: str,
    stock_code: str,
    display_platforms: Sequence[str],
    since_dt: dt.datetime,
) -> Dict[str, int]:
    """
    Upsert from raw MediaCrawler tables into bettafish_stock_media_index.
    Note: This is only the normalized index for "feeding AI". Raw tables are still kept.
    """
    upserted: Dict[str, int] = {}
    index_cols = _list_columns(conn, db, "bettafish_stock_media_index")
    has_content_id = "content_id" in index_cols
    insert_cols = [
        "stock_code",
        "platform",
        "keyword",
        "source_table",
        "source_id",
    ]
    insert_values = ["%s", "%s", "%s", "%s", "%s"]
    if has_content_id:
        insert_cols.append("content_id")
        insert_values.append("%s")
    insert_cols += [
        "url",
        "title",
        "author",
        "published_at",
        "content",
        "like_count",
        "comment_count",
        "content_time",
        "crawled_at",
    ]
    insert_values += [
        "%s",
        "%s",
        "%s",
        "%s",
        "%s",
        "%s",
        "%s",
        "%s",
        "NOW()",
    ]
    update_cols = [
        "`keyword`=VALUES(`keyword`)",
        "`url`=COALESCE(VALUES(`url`), `url`)",
        "`title`=COALESCE(VALUES(`title`), `title`)",
        "`author`=COALESCE(VALUES(`author`), `author`)",
        "`published_at`=COALESCE(VALUES(`published_at`), `published_at`)",
        "`content`=COALESCE(VALUES(`content`), `content`)",
        "`like_count`=COALESCE(VALUES(`like_count`), `like_count`)",
        "`comment_count`=COALESCE(VALUES(`comment_count`), `comment_count`)",
        "`content_time`=COALESCE(VALUES(`content_time`), `content_time`)",
        "`crawled_at`=NOW()",
    ]
    if has_content_id:
        update_cols.insert(1, "`content_id`=COALESCE(VALUES(`content_id`), `content_id`)")
    insert_sql = (
        "INSERT INTO `bettafish_stock_media_index` "
        f"({', '.join(f'`{c}`' for c in insert_cols)}) "
        f"VALUES ({', '.join(insert_values)}) "
        "ON DUPLICATE KEY UPDATE "
        f"{', '.join(update_cols)}"
    )

    with conn.cursor() as cur:
        for p in display_platforms:
            if p not in RAW_TABLES:
                continue
            mapping = RAW_TABLES[p]
            tbl = mapping["content"]
            cols = _list_columns(conn, db, tbl)

            # Platform-specific column mapping
            # We only need: source_id, url, title, author, content, published_at/create_time/add_ts, like_count
            if p in ("wb", "xhs"):
                source_id_col = "note_id"
                url_col = "note_url"
                title_col = "title" if "title" in cols else None
                author_col = "nickname" if "nickname" in cols else ("user_id" if "user_id" in cols else None)
                content_col = "content" if "content" in cols else ("desc" if "desc" in cols else None)
                kw_col = "source_keyword" if "source_keyword" in cols else None
                time_col = "create_time" if "create_time" in cols else ("publish_time" if "publish_time" in cols else None)
                like_col = "like_count" if "like_count" in cols else None
                comment_cnt_col = "comment_count" if "comment_count" in cols else None
            elif p == "zhihu":
                source_id_col = "content_id"
                url_col = "url" if "url" in cols else None
                title_col = "title" if "title" in cols else None
                author_col = "author_name" if "author_name" in cols else None
                content_col = "content" if "content" in cols else None
                kw_col = "source_keyword" if "source_keyword" in cols else None
                time_col = "created_at" if "created_at" in cols else ("create_time" if "create_time" in cols else None)
                like_col = "voteup_count" if "voteup_count" in cols else None
                comment_cnt_col = "comment_count" if "comment_count" in cols else None
            elif p == "tieba":
                source_id_col = "note_id" if "note_id" in cols else ("thread_id" if "thread_id" in cols else "id")
                url_col = "url" if "url" in cols else None
                title_col = "title" if "title" in cols else None
                author_col = "author_name" if "author_name" in cols else None
                content_col = "content" if "content" in cols else None
                kw_col = "source_keyword" if "source_keyword" in cols else None
                time_col = "create_time" if "create_time" in cols else None
                like_col = "like_count" if "like_count" in cols else None
                comment_cnt_col = "comment_count" if "comment_count" in cols else None
            elif p == "bilibili":
                source_id_col = "video_id"
                url_col = "video_url" if "video_url" in cols else ("url" if "url" in cols else None)
                title_col = "title" if "title" in cols else None
                author_col = "author" if "author" in cols else ("up_name" if "up_name" in cols else None)
                content_col = "desc" if "desc" in cols else ("content" if "content" in cols else None)
                kw_col = "source_keyword" if "source_keyword" in cols else None
                # bilibili often uses pubdate (epoch seconds)
                time_col = "pubdate" if "pubdate" in cols else ("create_time" if "create_time" in cols else None)
                like_col = "like_count" if "like_count" in cols else ("like" if "like" in cols else None)
                comment_cnt_col = "reply_count" if "reply_count" in cols else ("comment_count" if "comment_count" in cols else None)
            elif p == "douyin":
                source_id_col = "aweme_id"
                url_col = "share_url" if "share_url" in cols else ("url" if "url" in cols else None)
                title_col = "desc" if "desc" in cols else ("title" if "title" in cols else None)
                author_col = "author_nickname" if "author_nickname" in cols else ("author_id" if "author_id" in cols else None)
                content_col = "desc" if "desc" in cols else None
                kw_col = "source_keyword" if "source_keyword" in cols else None
                time_col = "create_time" if "create_time" in cols else None  # usually epoch seconds
                like_col = "digg_count" if "digg_count" in cols else None
                comment_cnt_col = "comment_count" if "comment_count" in cols else None
            elif p == "kuaishou":
                source_id_col = "video_id"
                url_col = "share_url" if "share_url" in cols else ("url" if "url" in cols else None)
                title_col = "caption" if "caption" in cols else ("title" if "title" in cols else None)
                author_col = "author_name" if "author_name" in cols else ("author_id" if "author_id" in cols else None)
                content_col = "caption" if "caption" in cols else ("content" if "content" in cols else None)
                kw_col = "source_keyword" if "source_keyword" in cols else None
                time_col = "create_time" if "create_time" in cols else None
                like_col = "like_count" if "like_count" in cols else None
                comment_cnt_col = "comment_count" if "comment_count" in cols else None
            else:
                continue

            # Build select
            select_cols = [source_id_col]
            if url_col: select_cols.append(url_col)
            if title_col: select_cols.append(title_col)
            if author_col: select_cols.append(author_col)
            if content_col: select_cols.append(content_col)
            if kw_col: select_cols.append(kw_col)
            if time_col: select_cols.append(time_col)
            if like_col: select_cols.append(like_col)
            if comment_cnt_col: select_cols.append(comment_cnt_col)

            # Filter by time if possible
            where_sql = ""
            params: List[object] = []
            if time_col:
                # time_col might be datetime or epoch
                dtype = cols.get(time_col, "")
                if dtype in ("datetime", "timestamp"):
                    where_sql = f"WHERE `{time_col}` >= %s"
                    params.append(since_dt)
                else:
                    # assume epoch seconds/ms
                    threshold = int(since_dt.timestamp())
                    where_sql = f"WHERE `{time_col}` >= %s"
                    params.append(threshold)

            sql = f"SELECT {', '.join('`'+c+'`' for c in select_cols)} FROM `{tbl}` {where_sql}"
            cur.execute(sql, params)
            rows = cur.fetchall()

            n = 0
            for r in rows:
                # unpack by position
                pos = 0
                source_id = str(r[pos]); pos += 1
                url = r[pos] if url_col else None
                if url_col: pos += 1
                title = r[pos] if title_col else None
                if title_col: pos += 1
                author = r[pos] if author_col else None
                if author_col: pos += 1
                content = r[pos] if content_col else None
                if content_col: pos += 1
                keyword = r[pos] if kw_col else None
                if kw_col: pos += 1
                tval = r[pos] if time_col else None
                if time_col: pos += 1
                like_cnt = _safe_int(r[pos]) if like_col else None
                if like_col: pos += 1
                comment_cnt = _safe_int(r[pos]) if comment_cnt_col else None
                if comment_cnt_col: pos += 1

                # Normalize content_time
                content_time: Optional[dt.datetime] = None
                if tval is not None:
                    if isinstance(tval, (dt.datetime, dt.date)):
                        content_time = dt.datetime.fromisoformat(str(tval)) if not isinstance(tval, dt.datetime) else tval
                    else:
                        content_time = _epoch_to_dt(_safe_int(tval))

                params = [
                    stock_code,
                    p,
                    keyword,
                    tbl,
                    source_id,
                ]
                if has_content_id:
                    params.append(source_id)
                params += [
                    url,
                    title,
                    author,
                    content_time,
                    content,
                    like_cnt,
                    comment_cnt,
                    content_time,
                ]
                cur.execute(insert_sql, params)
                n += 1

            upserted[p] = n

    return upserted


def _materialize_mindspider_post(conn: pymysql.connections.Connection, stock_code: str, since_dt: dt.datetime) -> int:
    """
    Copy latest records from bettafish_stock_media_index to mindspider_post for your downstream agent.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO `mindspider_post`
            (`stock_code`,`platform`,`keyword`,`platform_post_id`,`url`,`title`,`author`,
             `published_at`,`content`,`like_count`,`crawled_at`)
            SELECT
              `stock_code`,
              `platform`,
              `keyword`,
              CONCAT(`source_table`, ':', `source_id`) AS platform_post_id,
              `url`,`title`,`author`,
              `published_at`,
              `content`,
              `like_count`,
              NOW()
            FROM `bettafish_stock_media_index`
            WHERE `stock_code`=%s AND (`content_time` IS NULL OR `content_time` >= %s)
            ON DUPLICATE KEY UPDATE
              `keyword`=VALUES(`keyword`),
              `url`=COALESCE(VALUES(`url`), `url`),
              `title`=COALESCE(VALUES(`title`), `title`),
              `author`=COALESCE(VALUES(`author`), `author`),
              `published_at`=COALESCE(VALUES(`published_at`), `published_at`),
              `content`=COALESCE(VALUES(`content`), `content`),
              `like_count`=COALESCE(VALUES(`like_count`), `like_count`),
              `crawled_at`=NOW()
            """,
            (stock_code, since_dt),
        )
        return cur.rowcount


# ------------
# CLI Helpers
# ------------
def _parse_platforms(s: str) -> List[str]:
    if not s:
        return DISPLAY_PLATFORMS.copy()
    raw = [x.strip().lower() for x in re.split(r"[,\s]+", s) if x.strip()]
    out: List[str] = []
    for x in raw:
        if x in ("all", "full", "*"):
            out = DISPLAY_PLATFORMS.copy()
            break
        if x in ("weibo",):
            x = "wb"
        if x in ("bili", "bilibili"):
            x = "bilibili"
        if x in ("dy", "douyin"):
            x = "douyin"
        if x in ("ks", "kuaishou"):
            x = "kuaishou"
        if x not in DISPLAY_PLATFORMS:
            raise RuntimeError(f"不支持的平台: {x}（支持: {', '.join(DISPLAY_PLATFORMS)}）")
        if x not in out:
            out.append(x)
    return out


def _parse_keywords(user_kw: Optional[str], stock_code: str, stock_name: str) -> List[str]:
    if user_kw:
        kws = [k.strip() for k in re.split(r"[,\n]+", user_kw) if k.strip()]
        return kws[:50]
    # fallback keywords (lightweight, deterministic)
    base = [stock_name, stock_code]
    if stock_name and stock_name not in base:
        base.insert(0, stock_name)
    return [k for k in base if k]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mindspider-dir", required=True, help="BettaFish 项目根目录（包含 MindSpider/DeepSentimentCrawling）")
    ap.add_argument("--config", required=True, help="config.ini（包含 [mysql] 段）")
    ap.add_argument("--code", required=True, help="股票代码（作为 stock_code 写入）")
    ap.add_argument("--name", required=True, help="股票名称（用于默认关键词）")
    ap.add_argument("--platforms", default="wb,xhs,zhihu,tieba,bilibili,douyin,kuaishou")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--max-notes", type=int, default=10)
    ap.add_argument("--keywords", default=None, help="逗号分隔关键词；为空则使用 name/code")
    ap.add_argument("--no-crawl", action="store_true", help="只做 DB 标记/索引/物化，不触发爬虫")
    ap.add_argument("--timeout-sec", type=int, default=900, help="除 bilibili 外的平台超时（秒）")
    ap.add_argument("--bilibili-timeout-sec", type=int, default=600, help="bilibili 平台超时（秒）")
    ap.add_argument("--tag-all-tables", action="store_true", default=True, help="对 schema 内所有表增加 stock_code/ingested_at 并标记")
    ap.add_argument("--no-tag-all-tables", dest="tag_all_tables", action="store_false")
    ap.add_argument("--backfill", action="store_true", default=True, help="对最近 days 内历史记录进行补标（解决你现在的‘已爬但没标记’）")
    ap.add_argument("--no-backfill", dest="backfill", action="store_false")
    args = ap.parse_args()

    stock_code = str(args.code).strip()
    stock_name = str(args.name).strip().strip('"').strip("'")
    platforms = _parse_platforms(args.platforms)
    keywords = _parse_keywords(args.keywords, stock_code, stock_name)

    print(f"[TASK] code={stock_code} name={stock_name} platforms={platforms} days={args.days} max_notes={args.max_notes}")
    print(f"[TASK] keywords={keywords}")

    cfg = _read_mysql_from_ini(args.config)
    _inject_mysql_env(cfg)
    _ensure_execjs_runtime()

    conn = _connect_mysql(cfg)
    try:
        _ensure_index_tables(conn, cfg.database)

        # 1) Ensure schema columns/indexes for stock tagging
        _ensure_stock_tagging_schema(conn, cfg.database, tag_all_tables=args.tag_all_tables)
        conn.commit()

        # snapshot for precise stamping (rows inserted during this run)
        max_ids_before = _snapshot_max_ids(conn, cfg.database)

        # 2) Crawl (optional)
        crawl_results: Dict[str, str] = {}
        if not args.no_crawl:
            mindspider_dir = Path(args.mindspider_dir).resolve()
            pc_mod = _import_platform_crawler(mindspider_dir)
            crawl_results = _run_crawl_per_platform(
                pc_mod,
                platforms,
                keywords,
                args.max_notes,
                args.timeout_sec,
                args.bilibili_timeout_sec,
            )
            for k, v in crawl_results.items():
                if v != "ok":
                    print(f"[CRAWL] WARN {k}: {v}")

        # 3) Stamp stock_code into raw tables (new rows + optional backfill)
        now = dt.datetime.now()
        stamped = _stamp_new_rows_by_id(conn, cfg.database, max_ids_before, stock_code, now)
        if stamped:
            # keep log small
            top = {k: v for k, v in stamped.items() if v}
            print(f"[STAMP] updated_by_id(nonzero): {top}")

        if args.backfill:
            backfilled = _backfill_recent_rows(
                conn, cfg.database, stock_code, now, keywords, days=max(1, int(args.days)), only_platform_tables=True
            )
            top2 = {k: v for k, v in backfilled.items() if v}
            print(f"[BACKFILL] updated_recent(nonzero): {top2}")

        conn.commit()

        # 4) Build normalized index + materialize mindspider_post
        since_dt = dt.datetime.now() - dt.timedelta(days=max(1, int(args.days)))
        upserted = _upsert_stock_media_index(conn, cfg.database, stock_code, platforms, since_dt)
        print(f"[INDEX] upserted_by_platform: {upserted}")

        mat_n = _materialize_mindspider_post(conn, stock_code, since_dt)
        print(f"[MATERIALIZE] mindspider_post affected_rows={mat_n}")

        conn.commit()
        print("[DONE] pipeline finished OK.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e!r}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
