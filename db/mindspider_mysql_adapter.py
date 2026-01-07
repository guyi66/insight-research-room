#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MindSpider -> MySQL 适配器
- 读取 config.ini 的 [mysql] 段
- 初始化 schema（可选）
- 批量 upsert 写入 mindspider_post（包含 stock_code）
"""
from __future__ import annotations

import configparser
from datetime import datetime
import json
import os
from typing import Any, Dict, List, Optional

import pymysql


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


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        s = str(v).strip().replace(",", "")
        if not s:
            return None
        if s.endswith("万"):
            return int(float(s[:-1]) * 10000)
        if s.endswith("亿"):
            return int(float(s[:-1]) * 100000000)
        return int(float(s))
    except Exception:
        return None


def _parse_datetime(v: Any) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str):
        try:
            return v.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    s = v.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M",
                "%Y-%m-%d", "%m-%d %H:%M", "%H:%M"):
        try:
            import datetime as _dt
            now = _dt.datetime.now()
            if fmt == "%m-%d %H:%M":
                dt = _dt.datetime.strptime(s, fmt).replace(year=now.year)
            elif fmt == "%H:%M":
                t = _dt.datetime.strptime(s, fmt).time()
                dt = _dt.datetime.combine(now.date(), t)
            else:
                dt = _dt.datetime.strptime(s, fmt)
                if fmt == "%Y-%m-%d":
                    dt = dt.replace(hour=0, minute=0, second=0)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return None


def _coalesce(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return None


class MindSpiderMySQLAdapter:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.cfg = load_config(config_path)

    def init_schema_from_sql(self, schema_sql_path: str) -> None:
        if not os.path.exists(schema_sql_path):
            raise FileNotFoundError(f"schema sql 不存在: {schema_sql_path}")
        sql_text = open(schema_sql_path, "r", encoding="utf-8", errors="ignore").read()
        stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
        conn = _mysql_conn_from_cfg(self.cfg)
        try:
            with conn.cursor() as cur:
                for stmt in stmts:
                    cur.execute(stmt)
        finally:
            conn.close()

    def upsert_posts(self, stock_code: str, platform: str, keyword: Optional[str],
                     posts: List[Dict[str, Any]], crawled_at: Optional[str] = None) -> int:
        if not posts:
            return 0
        crawled_at = crawled_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for p in posts:
            pid = _coalesce(p, ["platform_post_id","note_id","post_id","mid","id","item_id","sid","thread_id"])
            url = _coalesce(p, ["url","share_url","link","jump_url"])
            title = _coalesce(p, ["title","desc","text"])
            content = _coalesce(p, ["content","text","desc","raw_text","正文"])
            author = _coalesce(p, ["author","nickname","screen_name","user","uname","用户名"])
            pub = _coalesce(p, ["published_at","publish_time","created_at","time","date","发布时间"])
            like = _coalesce(p, ["like_count","likes","digg_count","点赞"])
            cmt = _coalesce(p, ["comment_count","comments","reply_count","评论"])
            share = _coalesce(p, ["share_count","repost_count","转发"])
            collect = _coalesce(p, ["collect_count","favorite_count","收藏"])

            rows.append({
                "stock_code": stock_code,
                "platform": platform,
                "keyword": keyword,
                "platform_post_id": str(pid) if pid is not None else None,
                "url": str(url) if url is not None else None,
                "title": (str(title)[:512] if title is not None else None),
                "author": (str(author)[:128] if author is not None else None),
                "published_at": _parse_datetime(pub),
                "content": (str(content) if content is not None else None),
                "like_count": _safe_int(like),
                "comment_count": _safe_int(cmt),
                "share_count": _safe_int(share),
                "collect_count": _safe_int(collect),
                "raw_json": json.dumps(p, ensure_ascii=False),
                "crawled_at": crawled_at,
            })

        sql = """
        INSERT INTO mindspider_post
          (stock_code, platform, keyword, platform_post_id, url, title, author, published_at,
           content, like_count, comment_count, share_count, collect_count, raw_json, crawled_at)
        VALUES
          (%(stock_code)s, %(platform)s, %(keyword)s, %(platform_post_id)s, %(url)s, %(title)s, %(author)s, %(published_at)s,
           %(content)s, %(like_count)s, %(comment_count)s, %(share_count)s, %(collect_count)s,
           CAST(%(raw_json)s AS JSON), %(crawled_at)s)
        ON DUPLICATE KEY UPDATE
          stock_code=VALUES(stock_code),
          keyword=COALESCE(VALUES(keyword), keyword),
          url=COALESCE(VALUES(url), url),
          title=COALESCE(VALUES(title), title),
          author=COALESCE(VALUES(author), author),
          published_at=COALESCE(VALUES(published_at), published_at),
          content=COALESCE(VALUES(content), content),
          like_count=COALESCE(VALUES(like_count), like_count),
          comment_count=COALESCE(VALUES(comment_count), comment_count),
          share_count=COALESCE(VALUES(share_count), share_count),
          collect_count=COALESCE(VALUES(collect_count), collect_count),
          raw_json=COALESCE(VALUES(raw_json), raw_json),
          crawled_at=VALUES(crawled_at);
        """
        conn = _mysql_conn_from_cfg(self.cfg)
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        finally:
            conn.close()
        return len(rows)
