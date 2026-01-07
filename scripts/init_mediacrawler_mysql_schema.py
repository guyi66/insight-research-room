#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
init_mediacrawler_mysql_schema.py

作用：
- 你现在已经把 MediaCrawler 的保存方式切到 MySQL（save_data_option=db），但 MySQL 里没有 MediaCrawler 需要的表，
  所以会出现：Table 'xxx.weibo_note' doesn't exist。
- 本脚本会“自动导入 MediaCrawler/store 下的模型”，收集 SQLAlchemy Declarative Base 的 metadata，
  并通过 AsyncEngine 在 MySQL 中执行 create_all，创建 weibo_note / xhs_note 等表（以你的 MediaCrawler 代码为准）。

用法（在 BettaFish 根目录执行）：
  C:\...\BettaFish\.venv\1\Scripts\python.exe scripts/init_mediacrawler_mysql_schema.py ^
    --mediacrawler-dir .\MindSpider\DeepSentimentCrawling\MediaCrawler ^
    --config .\Guba-Crawler\config.ini
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
import importlib
import sys
from pathlib import Path
from typing import List, Set, Tuple

from sqlalchemy.ext.asyncio import create_async_engine


def read_mysql_from_ini(config_path: str) -> Tuple[str, int, str, str, str]:
    cp = configparser.ConfigParser(strict=False)
    cp.read(config_path, encoding="utf-8-sig")
    if "mysql" not in cp:
        raise RuntimeError(f"config.ini 缺少 [mysql] 段: {config_path}")
    sec = cp["mysql"]
    host = sec.get("host", "127.0.0.1").strip()
    port = int(sec.get("port", "3306").strip())
    user = sec.get("user", "root").strip()
    password = sec.get("password", "").strip()
    db = sec.get("database", sec.get("db", "")).strip()
    if not db:
        raise RuntimeError("mysql.database/db 为空，请检查 config.ini 的 [mysql] 段。")
    return host, port, user, password, db


def iter_py_modules_under(mc_root: Path, pkg_dir: Path) -> List[str]:
    """
    给定 MediaCrawler 根目录 + store 目录，递归找 .py 文件并转为可 import 的模块名。
      MediaCrawler/store/weibo/_model.py -> store.weibo._model
    """
    out: List[str] = []
    for p in pkg_dir.rglob("*.py"):
        if p.name.startswith("__"):
            continue
        rel = p.relative_to(mc_root)  # from MediaCrawler root
        mod = ".".join(rel.with_suffix("").parts)
        out.append(mod)
    return sorted(set(out))


def collect_bases(mod) -> List[object]:
    bases: List[object] = []
    for name, obj in vars(mod).items():
        if hasattr(obj, "metadata"):
            # 过滤掉明显不是 Declarative Base 的对象
            if name.lower() in ("base", "declarative_base", "ormbase", "modelbase") or name.endswith("Base"):
                bases.append(obj)
    uniq = []
    seen = set()
    for b in bases:
        mid = id(b)
        if mid not in seen:
            uniq.append(b)
            seen.add(mid)
    return uniq


async def main_async():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mediacrawler-dir", required=True, help="MediaCrawler 根目录（包含 main.py / store / media_platform 等）")
    ap.add_argument("--config", required=True, help="含 [mysql] 段的 config.ini")
    ap.add_argument("--echo", action="store_true", help="SQLAlchemy echo")
    args = ap.parse_args()

    mc_dir = Path(args.mediacrawler_dir).resolve()
    store_dir = mc_dir / "store"
    if not store_dir.exists():
        raise RuntimeError(f"未找到 store 目录: {store_dir}")

    host, port, user, pwd, db = read_mysql_from_ini(str(Path(args.config).resolve()))
    url = f"mysql+asyncmy://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
    print(f"[MYSQL] {host}:{port} db={db} user={user}")

    sys.path.insert(0, str(mc_dir))

    modules = iter_py_modules_under(mc_dir, store_dir)
    print(f"[SCAN] store modules: {len(modules)}")

    bases: List[object] = []
    imported = 0
    failed: List[Tuple[str, str]] = []
    for m in modules:
        try:
            mod = importlib.import_module(m)
            imported += 1
            bases.extend(collect_bases(mod))
        except Exception as e:
            failed.append((m, repr(e)))

    uniq_bases = []
    seen: Set[int] = set()
    for b in bases:
        if id(b) not in seen:
            uniq_bases.append(b)
            seen.add(id(b))

    print(f"[IMPORT] ok={imported} failed={len(failed)} bases={len(uniq_bases)}")
    if failed:
        print("[WARN] failed imports (top 10):")
        for m, e in failed[:10]:
            print(f"  - {m}: {e}")

    if not uniq_bases:
        raise RuntimeError("未找到任何带 metadata 的 Base；请确认 store 模型定义在 store 下且使用 SQLAlchemy Declarative。")

    engine = create_async_engine(url, echo=args.echo, future=True)

    created_tables = set()
    async with engine.begin() as conn:
        for b in uniq_bases:
            md = getattr(b, "metadata", None)
            if md is None:
                continue
            for t in md.tables.values():
                created_tables.add(t.name)
            await conn.run_sync(md.create_all)

    await engine.dispose()
    print(f"[OK] create_all done. tables={len(created_tables)} (sample: {sorted(list(created_tables))[:20]})")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
