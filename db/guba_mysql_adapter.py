#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Guba -> MySQL 落库适配器

用途：
- 以“Mongo collection 风格接口”对齐 Guba-Crawler 的调用点
  - main_class.py: self.col.insert_many([data_json])
  - main_class.py: self.col.create_index(...) / self.col.index_information() （仅用于建索引检查）
  - full_text_CrawlerAsync.py: self.mongo_client.update_one({"href": url}, update_data)

依赖：
    pip install pymysql

配置（config.ini）示例：
[mysql]
host = 127.0.0.1
port = 3306
user = root
password = your_password
database = bettafish_guba
charset = utf8mb4

注意：
- 本适配器假设 MySQL 中存在表 bettafish_guba.guba_post（见 db/guba_mysql_schema.sql）
"""
from __future__ import annotations

import configparser
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import re

import pymysql


def _safe_int(v: Any) -> Optional[int]:
    """
    把 “阅读/评论” 等字段转换为 int
    支持：'123' / '1,234' / '1.2万' / '3.4亿' / '--' / None
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {"--", "-", "None", "null"}:
        return None
    s = s.replace(",", "")
    # 常见中文单位
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([万亿])$", s)
    if m:
        num = float(m.group(1))
        unit = m.group(2)
        mult = 10_000 if unit == "万" else 100_000_000
        return int(num * mult)
    # 纯数字或小数（向下取整）
    try:
        if "." in s:
            return int(float(s))
        return int(s)
    except Exception:
        return None


def _parse_last_update(raw: str) -> Optional[datetime]:
    """
    解析“最后更新/全文时间”等字段为 datetime。
    常见形式：
      - '12-23'
      - '09:15'
      - '2025-12-23'
      - '2025-12-23 09:15'
    解析失败返回 None。
    """
    if not raw:
        return None
    s = str(raw).strip()
    now = datetime.now()

    # yyyy-mm-dd HH:MM[:SS]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    # yyyy-mm-dd
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        pass

    # mm-dd HH:MM[:SS] -> use current year
    for fmt in ("%m-%d %H:%M:%S", "%m-%d %H:%M"):
        try:
            return datetime.strptime(f"{now.year}-{s}", f"%Y-{fmt}")
        except Exception:
            pass

    # mm-dd -> use current year
    try:
        dt = datetime.strptime(f"{now.year}-{s}", "%Y-%m-%d")
        return dt
    except Exception:
        pass

    # HH:MM -> use today
    try:
        t = datetime.strptime(s, "%H:%M").time()
        return datetime.combine(now.date(), t)
    except Exception:
        pass

    return None


@dataclass
class MySQLConn:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

    def connect(self, use_db: bool = True):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database if use_db else None,
            charset=self.charset,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )


class GubaMySQLAdapter:
    """
    适配 Mongo collection 常用方法：
      - insert_many
      - update_one
      - create_index / index_information（no-op，兼容 main_class.main()）
    """

    def __init__(self, config_path: str = "config.ini"):
        cfg = configparser.ConfigParser()
        cfg.read(config_path, encoding="utf-8")
        if not cfg.has_section("mysql"):
            raise RuntimeError(
                "config.ini 缺少 [mysql] 配置段。请添加 host/port/user/password/database/charset 后再运行。"
            )

        self.conn = MySQLConn(
            host=cfg.get("mysql", "host", fallback="127.0.0.1"),
            port=cfg.getint("mysql", "port", fallback=3306),
            user=cfg.get("mysql", "user", fallback="root"),
            password=cfg.get("mysql", "password", fallback=""),
            database=cfg.get("mysql", "database", fallback="bettafish_guba"),
            charset=cfg.get("mysql", "charset", fallback="utf8mb4"),
        )

    # -------- Mongo-compat no-op --------
    def index_information(self) -> Dict[str, Any]:
        # main_class.main() 只用它判断是否存在某个索引名
        return {}

    def create_index(self, *args, **kwargs) -> None:
        # MySQL 索引已在建表脚本中完成；这里做 no-op 即可
        return None

    # -------- core write APIs --------
    def insert_many(self, documents: List[Dict[str, Any]]) -> None:
        rows = [self._normalize_post(doc) for doc in documents if doc]
        rows = [r for r in rows if r]
        if not rows:
            return

        sql = """
        INSERT INTO guba_post
          (stock_code, href, url, title, author, read_count, comment_count,
           last_update_raw, last_update_dt, crawled_at, source)
        VALUES
          (%(stock_code)s, %(href)s, %(url)s, %(title)s, %(author)s, %(read_count)s, %(comment_count)s,
           %(last_update_raw)s, %(last_update_dt)s, %(crawled_at)s, %(source)s)
        ON DUPLICATE KEY UPDATE
          title = VALUES(title),
          author = VALUES(author),
          read_count = VALUES(read_count),
          comment_count = VALUES(comment_count),
          last_update_raw = VALUES(last_update_raw),
          last_update_dt = VALUES(last_update_dt),
          crawled_at = VALUES(crawled_at),
          url = COALESCE(VALUES(url), url);
        """

        conn = self.conn.connect(use_db=True)
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        finally:
            conn.close()

    def update_one(self, filter_doc: Dict[str, Any], update_doc: Dict[str, Any]) -> None:
        href = (filter_doc or {}).get("href")
        if not href:
            return

        full_text = update_doc.get("full_text")
        ft_time_raw = update_doc.get("full_text_time") or update_doc.get("full_text_time_raw")
        ft_dt = _parse_last_update(ft_time_raw) if ft_time_raw else None

        conn = self.conn.connect(use_db=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE guba_post
                       SET full_text = COALESCE(%s, full_text),
                           full_text_time_raw = COALESCE(%s, full_text_time_raw),
                           full_text_dt = COALESCE(%s, full_text_dt)
                     WHERE href = %s
                    """,
                    (full_text, ft_time_raw, ft_dt, href),
                )
            conn.commit()
        finally:
            conn.close()

    # -------- helpers --------
    def _normalize_post(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        href = str(doc.get("href", "")).strip()
        if not href:
            return None

        stock_code = str(doc.get("secCode") or doc.get("stock_code") or "").strip()
        if not stock_code:
            # 兜底：尝试从 href 里抽取
            m = re.search(r"(?:list,|stock=|code=)(\d{6})", href)
            if m:
                stock_code = m.group(1)
        if not stock_code:
            stock_code = "UNKNOWN"

        url = doc.get("url")
        if not url:
            url = href
            if url.startswith("/"):
                url = "https://guba.eastmoney.com" + url
            if url.startswith("http://"):
                url = "https://" + url[len("http://"):]
        last_update_raw = str(doc.get("最后更新", "")).strip() or None
        last_update_dt = _parse_last_update(last_update_raw) if last_update_raw else None

        return {
            "stock_code": stock_code,
            "href": href,
            "url": url,
            "title": str(doc.get("标题", "")).strip() or None,
            "author": str(doc.get("作者", "")).strip() or None,
            "read_count": _safe_int(doc.get("阅读")),
            "comment_count": _safe_int(doc.get("评论")),
            "last_update_raw": last_update_raw,
            "last_update_dt": last_update_dt,
            "crawled_at": datetime.now(),
            "source": "eastmoney_guba",
        }
