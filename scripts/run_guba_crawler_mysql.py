#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行脚本：把东方财富股吧（Guba-Crawler）帖子抓取并同步到 MySQL（备用 DB）

特点：
- 自动初始化 schema（可选）
- 复用你已拷贝的 Guba-Crawler “列表页抓取”逻辑（main_class.guba_comments）
- 不依赖 Redis：列表抓取阶段不会向 Redis 写队列（用 NullRedisQueue 兜底）
- 全文抓取不依赖原 full_text_CrawlerAsync.py（避免 Redis 队列依赖），直接从 MySQL 中找 full_text 为空的记录补齐

使用方式（示例）：
  # 1) 先在 config.ini 里补充 [mysql] 段（见下方说明）
  # 2) 初始化 schema + 抓取 600519 前 10 页列表 + 补齐全文（最多 200 条）
  python scripts/run_guba_crawler_mysql.py --code 600519 --pages 10 --init-schema --fulltext --fulltext-limit 200

依赖：
  pip install pymysql requests beautifulsoup4 lxml aiohttp tqdm tenacity
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import re
import sys
import time
from typing import List, Optional, Tuple, Dict, Any

import pymysql
import aiohttp
from bs4 import BeautifulSoup

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# --- local adapters ---
from db.guba_mysql_adapter import GubaMySQLAdapter


def _read_text_with_bom_fallback(file_path: str) -> str:
    """读取 ini 文本：优先 utf-8-sig（可去 BOM），失败则退化 utf-8。"""
    with open(file_path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8-sig", errors="strict")
    except Exception:
        return data.decode("utf-8", errors="replace")


def load_config(config_path: str) -> configparser.ConfigParser:
    """
    统一加载 config.ini：
    - strict=False：兼容重复 section/option
    - utf-8-sig：兼容带 BOM 的 ini
    - 若文件不存在/读取失败，抛出明确错误
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    cfg = configparser.ConfigParser(strict=False)
    text = _read_text_with_bom_fallback(config_path)
    cfg.read_string(text)
    return cfg


def auto_pick_config_path(user_given: str, guba_dir: str) -> str:
    """
    若用户未显式指定 --config，则优先使用 <guba_dir>/config.ini；
    否则保持 user_given。
    """
    # argparse default is 'config.ini' (relative). If user didn't pass, it stays that.
    if user_given and os.path.basename(user_given).lower() == "config.ini" and not os.path.isabs(user_given):
        candidates = [
            os.path.join(guba_dir, "config.ini"),
            os.path.join(os.getcwd(), "config.ini"),
            os.path.join(os.path.dirname(__file__), "config.ini"),
            os.path.join(os.path.dirname(__file__), "Guba-Crawler", "config.ini"),
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        # return original (will fail with clear error later)
        return os.path.abspath(user_given)
    return os.path.abspath(user_given)


def auto_pick_schema_sql_path(user_given: str) -> str:
    """若用户未显式指定 --schema-sql，则在常见位置自动寻找。"""
    if user_given and os.path.basename(user_given).lower().endswith(".sql") and not os.path.isabs(user_given):
        candidates = [
            os.path.join(os.getcwd(), user_given),
            os.path.join(ROOT_DIR, user_given),
            os.path.join(os.path.dirname(__file__), user_given),
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        return os.path.abspath(user_given)
    return os.path.abspath(user_given)


def _normalize_keywords(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    cleaned = str(raw).replace("，", ",").replace("；", ";")
    parts = re.split(r"[,\n;]+", cleaned)
    return [p.strip() for p in parts if p.strip()]


def _build_keyword_clause(keywords: List[str]) -> Tuple[str, List[str]]:
    if not keywords:
        return "", []
    clauses = []
    params: List[str] = []
    for kw in keywords:
        if not kw:
            continue
        clauses.append("title LIKE %s")
        params.append(f"%{kw}%")
    if not clauses:
        return "", []
    return " AND (" + " OR ".join(clauses) + ")", params



# --------------------- schema init ---------------------
def _split_sql_statements(sql_text: str) -> List[str]:
    # 简单分割；本项目 schema 无 DELIMITER/存储过程，足够用
    parts = [p.strip() for p in sql_text.split(";")]
    return [p for p in parts if p]


def init_schema_from_sql(config_path: str, sql_path: str) -> None:
    cfg = load_config(config_path)
    if not cfg.has_section("mysql"):
        raise RuntimeError(f"{config_path} 缺少 [mysql] 配置段，当前 sections={cfg.sections()}")

    host = cfg.get("mysql", "host", fallback="127.0.0.1")
    port = cfg.getint("mysql", "port", fallback=3306)
    user = cfg.get("mysql", "user", fallback="root")
    password = cfg.get("mysql", "password", fallback="")
    # 注意：创建数据库时不指定 database
    charset = cfg.get("mysql", "charset", fallback="utf8mb4")

    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"schema 文件不存在: {sql_path}")

    sql_text = open(sql_path, "r", encoding="utf-8", errors="ignore").read()
    stmts = _split_sql_statements(sql_text)

    conn = pymysql.connect(
        host=host, port=port, user=user, password=password, database=None,
        charset=charset, autocommit=False, cursorclass=pymysql.cursors.Cursor
    )
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


# --------------------- null redis queue ---------------------
class NullRedisQueue:
    """替代 DatabaseManager.get_redis_client()，避免必须安装/配置 Redis"""
    def add_url(self, url: str) -> None:
        return


def patch_database_manager_no_redis(guba_dir: str) -> None:
    """
    main_class.guba_comments 在 __init__ 会调用 DatabaseManager.get_redis_client()。
    这里通过 monkey patch 把该方法替换为返回 NullRedisQueue，从而让列表爬虫可在无 Redis 环境运行。
    """
    sys.path.insert(0, guba_dir)
    from database_client import DatabaseManager  # noqa

    def _no_redis(self):
        return NullRedisQueue()

    DatabaseManager.get_redis_client = _no_redis


# --------------------- list crawler ---------------------
def run_list_crawler(
    guba_dir: str,
    config_path: str,
    code: str,
    pages: int,
    use_hot: bool = False,
) -> None:
    """
    运行列表页抓取并写入 MySQL。
    - pages: 抓取页数（从 1 开始）
    """
    sys.path.insert(0, guba_dir)
    patch_database_manager_no_redis(guba_dir)

    # import after patch
    from main_class import guba_comments  # noqa

    try:
        cfg = load_config(config_path)
    except Exception:
        cfg = configparser.ConfigParser(strict=False)

    # 确保 mainClass 段存在
    if not cfg.has_section("mainClass"):
        cfg.add_section("mainClass")

    cfg.set("mainClass", "secCode", str(code))
    cfg.set("mainClass", "pages_start", "1")
    cfg.set("mainClass", "pages_end", str(int(pages) + 1))  # range end is exclusive
    # 是否热帖：原仓库文档提到 hot / all 两种 URL，这里仅留开关位（你的 main_class 当前用 f_{page}）
    cfg.set("mainClass", "mode", "hot" if use_hot else "all")

    # 关键：让 guba_comments.secCode 一定有值
    cfg.set("mainClass", "storage_backend", "mysql")
    crawler = guba_comments(
        config_path=config_path,
        config=cfg,
        secCode=str(code),
        pages_start=1,
        pages_end=int(pages) + 1,
        MongoDB=False,
        collectionName=str(code),
        full_text=False,
    )

    # 覆盖存储为 MySQL
    crawler.col = GubaMySQLAdapter(config_path=config_path)

    # 运行
    crawler.main()


# --------------------- fulltext crawler (from MySQL) ---------------------
@dataclass
class FTTask:
    href: str
    url: str


def _parse_post_content(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """
    返回 (full_text, time_raw)
    参照 full_text_CrawlerAsync.py 的选择器策略做简化版实现
    """
    # time
    time_raw = None
    for selector in ['div.time', 'span.time', 'div.publish-time', 'div.date', 'time', '[class*="time"]', '[class*="date"]']:
        elems = soup.select(selector)
        if elems:
            for e in elems:
                t = e.get_text(strip=True)
                if t and any(ch.isdigit() for ch in t):
                    time_raw = t
                    break
        if time_raw:
            break

    # content
    selectors = [
        'div.newstext', 'div#post_content', 'div.content',
        'div.article-content', 'div.main-content', 'article',
        'div.text-content', 'div[class*="content"]', 'div[id*="content"]',
        '.rich-text', '.article-body'
    ]
    candidates: List[str] = []
    for sel in selectors:
        for elem in soup.select(sel):
            txt = elem.get_text(strip=True)
            if txt and len(txt) >= 50:
                candidates.append(txt)

    full_text = max(candidates, key=len) if candidates else None
    if full_text:
        return full_text, time_raw

    # fallback: body text
    body = soup.body
    if body:
        for tag in body.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style']):
            tag.decompose()
        txt = body.get_text(strip=True)
        if txt and len(txt) >= 50:
            return txt, time_raw

    return None, time_raw


async def _ft_worker(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    task: FTTask,
    store: GubaMySQLAdapter,
    sleep_s: float,
) -> Tuple[bool, str]:
    async with sem:
        try:
            await asyncio.sleep(sleep_s)
            async with session.get(task.url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                html = await resp.text(errors="ignore")
            soup = BeautifulSoup(html, "lxml")
            full_text, time_raw = _parse_post_content(soup)
            if not full_text or len(full_text) < 30:
                return False, "no_content"
            store.update_one({"href": task.href}, {"full_text": full_text, "full_text_time": time_raw})
            return True, "ok"
        except Exception as e:
            return False, str(e)


def _mysql_fetch_missing_fulltext(
    config_path: str,
    code: str,
    days: int,
    limit: int,
    keywords: Optional[List[str]] = None,
) -> List[FTTask]:
    cfg = load_config(config_path)
    host = cfg.get("mysql", "host", fallback="127.0.0.1")
    port = cfg.getint("mysql", "port", fallback=3306)
    user = cfg.get("mysql", "user", fallback="root")
    password = cfg.get("mysql", "password", fallback="")
    database = cfg.get("mysql", "database", fallback="bettafish_guba")
    charset = cfg.get("mysql", "charset", fallback="utf8mb4")

    since = datetime.now() - timedelta(days=days)

    conn = pymysql.connect(
        host=host, port=port, user=user, password=password, database=database,
        charset=charset, autocommit=True, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cur:
            kw_clause, kw_params = _build_keyword_clause(keywords or [])
            sql = (
                """
                SELECT href, COALESCE(url, href) AS url
                  FROM guba_post
                 WHERE stock_code = %s
                   AND full_text IS NULL
                   AND crawled_at >= %s
                """
                + kw_clause
                + """
                 ORDER BY crawled_at DESC
                 LIMIT %s
                """
            )
            params = [code, since]
            params.extend(kw_params)
            params.append(int(limit))
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    tasks = []
    for r in rows:
        href = r["href"]
        url = r["url"]
        if url.startswith("/"):
            url = "https://guba.eastmoney.com" + url
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        tasks.append(FTTask(href=href, url=url))
    return tasks


async def run_fulltext_from_mysql_async(
    config_path: str,
    code: str,
    days: int,
    limit: int,
    concurrency: int,
    qps: int,
    keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    store = GubaMySQLAdapter(config_path=config_path)
    tasks = _mysql_fetch_missing_fulltext(config_path, code, days, limit, keywords=keywords)
    if not tasks:
        return {"total": 0, "success": 0, "fail": 0}

    sem = asyncio.Semaphore(concurrency)
    # 粗粒度限速：每个 worker 进入前 sleep 1/qps
    sleep_s = 1.0 / max(1, int(qps))

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "close",
    }

    success = 0
    fail = 0

    async with aiohttp.ClientSession(headers=headers) as session:
        coros = [_ft_worker(session, sem, t, store, sleep_s) for t in tasks]
        for fut in asyncio.as_completed(coros):
            ok, msg = await fut
            if ok:
                success += 1
            else:
                fail += 1

    return {"total": len(tasks), "success": success, "fail": fail}


# --------------------- CLI ---------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--guba-dir", default="Guba-Crawler", help="Guba-Crawler 代码所在目录（相对或绝对路径）")
    ap.add_argument("--config", default="config.ini", help="配置文件路径（需含 [mysql] 段）")
    ap.add_argument("--schema-sql", default="db/guba_mysql_schema.sql", help="建库建表 SQL 文件路径")
    ap.add_argument("--init-schema", action="store_true", help="启动前执行 schema 初始化")
    ap.add_argument("--code", required=True, help="股票代码，例如 600519")
    ap.add_argument("--pages", type=int, default=10, help="列表页抓取页数（从第1页开始）")
    ap.add_argument("--hot", action="store_true", help="抓热帖（仅记录开关；需你 main_class 支持 hot URL）")
    ap.add_argument("--fulltext", action="store_true", help="是否补齐全文（从 MySQL 中找 full_text 为空的记录补齐）")
    ap.add_argument("--fulltext-days", type=int, default=7, help="仅补齐最近 N 天入库的帖子全文")
    ap.add_argument("--fulltext-limit", type=int, default=200, help="本次最多补齐多少条全文")
    ap.add_argument("--keywords", default=None, help="关键词过滤，逗号分隔（用于筛选要补齐全文的帖子）")
    ap.add_argument("--concurrency", type=int, default=20, help="全文抓取并发")
    ap.add_argument("--qps", type=int, default=10, help="全文抓取 QPS 上限（粗粒度）")
    args = ap.parse_args()

    guba_dir = os.path.abspath(args.guba_dir)
    if not os.path.isdir(guba_dir):
        raise FileNotFoundError(f"找不到 Guba-Crawler 目录: {guba_dir}")

    keywords = _normalize_keywords(args.keywords)
    if keywords:
        print(f"[KW] keywords: {', '.join(keywords)}")

    # 自动选择 config.ini：优先使用 <guba_dir>/config.ini（你当前的实际配置就在这里）
    config_path = auto_pick_config_path(args.config, guba_dir)
    schema_sql = auto_pick_schema_sql_path(args.schema_sql)

    print(f"[CFG] using config: {config_path}")
    try:
        _cfg_preview = load_config(config_path)
        print(f"[CFG] sections: {_cfg_preview.sections()}")
        if not _cfg_preview.has_section('mysql'):
            print("[CFG][WARN] config 没有 [mysql] 段；若你要 --init-schema / 写 MySQL，请补齐。")
    except Exception as e:
        print(f"[CFG][WARN] failed to load config now: {e}")


    if args.init_schema:
        print(f"[INIT] init schema from: {schema_sql}")
        init_schema_from_sql(config_path, schema_sql)
        print("[INIT] schema ready")

    t0 = time.time()
    print(f"[LIST] crawling list pages: code={args.code}, pages={args.pages}")
    run_list_crawler(guba_dir, config_path, args.code, args.pages, use_hot=args.hot)
    print(f"[LIST] done, elapsed={time.time()-t0:.1f}s")

    if args.fulltext:
        print(f"[FT] fill full_text from mysql: days={args.fulltext_days}, limit={args.fulltext_limit}")
        res = asyncio.run(
            run_fulltext_from_mysql_async(
                config_path=config_path,
                code=args.code,
                days=args.fulltext_days,
                limit=args.fulltext_limit,
                concurrency=args.concurrency,
                qps=args.qps,
                keywords=keywords,
            )
        )
        print(f"[FT] done: {res}")

    print("[OK] All done.")


if __name__ == "__main__":
    main()
