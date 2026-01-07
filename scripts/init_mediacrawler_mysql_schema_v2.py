#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
init_mediacrawler_mysql_schema_v2.py

你遇到的问题：
- 我上一版脚本只在 store 模块里寻找 Declarative Base（Base.metadata）。
- 但 MediaCrawler 很可能不是用 Declarative Base，而是：
  1) 直接用 SQLAlchemy Table + MetaData 定义表；或
  2) 用 mapper_registry = registry(); 通过 mapper_registry.metadata 管理表；或
  3) Base 名称不是 Base/xxxBase；或模型不在 store 目录内。
因此出现：imports ok=11 但 bases=0。

本版本改进：
1) 不再只找 “Base”，而是收集所有可用的 MetaData：
   - module-level `metadata = MetaData()`
   - `mapper_registry.metadata`
   - 任意对象 obj.metadata 是 MetaData
   - SQLModel.metadata（若使用 sqlmodel）
2) 扫描范围从 store 扩展到整个 MediaCrawler（通过文本特征筛选：含 Table( / __tablename__ / weibo_note / xhs_note 等）
3) 对收集到的每个 MetaData 执行 create_all 到 MySQL（异步引擎 run_sync）

用法（BettaFish 根目录）：
  C:\...\BettaFish\.venv\1\Scripts\python.exe scripts/init_mediacrawler_mysql_schema_v2.py ^
    --mediacrawler-dir .\MindSpider\DeepSentimentCrawling\MediaCrawler ^
    --config .\Guba-Crawler\config.ini

可选：
  --echo   打印 DDL
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
import importlib
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine


# ---------- config ----------
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


# ---------- scanning ----------
PATTERNS = [
    re.compile(r"\bTable\s*\(", re.I),
    re.compile(r"__tablename__\s*=", re.I),
    re.compile(r"\bMetaData\s*\(", re.I),
    re.compile(r"weibo_note|xhs_note|guba|comment", re.I),
]

SKIP_DIRS = {"__pycache__", ".git", ".idea", ".vscode", "dist", "build", "docs", "assets", "static"}


def py_to_module(mc_root: Path, py_file: Path) -> str:
    rel = py_file.relative_to(mc_root)
    return ".".join(rel.with_suffix("").parts)


def file_looks_like_model(py_file: Path) -> bool:
    try:
        txt = py_file.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return any(p.search(txt) for p in PATTERNS)


def iter_candidate_modules(mc_root: Path) -> List[str]:
    mods: List[str] = []
    for p in mc_root.rglob("*.py"):
        if p.name.startswith("__"):
            continue
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        # 限制：只导入“像模型”的文件，避免把爬虫主逻辑全导入导致副作用
        if not file_looks_like_model(p):
            continue
        mods.append(py_to_module(mc_root, p))
    # 去重并排序
    return sorted(set(mods))


# ---------- metadata discovery ----------
def collect_metadata_from_module(mod) -> List[MetaData]:
    mds: List[MetaData] = []
    for name, obj in vars(mod).items():
        # 1) module-level metadata
        if isinstance(obj, MetaData):
            mds.append(obj)

        # 2) registry.metadata
        if name in ("mapper_registry", "registry", "orm_registry") and hasattr(obj, "metadata"):
            md = getattr(obj, "metadata", None)
            if isinstance(md, MetaData):
                mds.append(md)

        # 3) any obj.metadata is MetaData (Declarative Base / SQLModel / etc.)
        if hasattr(obj, "metadata"):
            md = getattr(obj, "metadata", None)
            if isinstance(md, MetaData):
                mds.append(md)

        # 4) sqlmodel.SQLModel.metadata（若模块导出了 SQLModel）
        if name == "SQLModel" and hasattr(obj, "metadata"):
            md = getattr(obj, "metadata", None)
            if isinstance(md, MetaData):
                mds.append(md)

    # 去重（按 id）
    uniq: List[MetaData] = []
    seen: Set[int] = set()
    for md in mds:
        if id(md) not in seen:
            uniq.append(md)
            seen.add(id(md))
    return uniq


async def main_async():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mediacrawler-dir", required=True, help="MediaCrawler 根目录（包含 main.py / store / media_platform 等）")
    ap.add_argument("--config", required=True, help="含 [mysql] 段的 config.ini")
    ap.add_argument("--echo", action="store_true", help="SQLAlchemy echo DDL")
    ap.add_argument("--max-modules", type=int, default=120, help="最多导入的候选模块数（默认 120）")
    args = ap.parse_args()

    mc_dir = Path(args.mediacrawler_dir).resolve()
    if not mc_dir.exists():
        raise RuntimeError(f"mediacrawler-dir 不存在: {mc_dir}")

    host, port, user, pwd, db = read_mysql_from_ini(str(Path(args.config).resolve()))
    url = f"mysql+asyncmy://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
    print(f"[MYSQL] {host}:{port} db={db} user={user}")

    # allow import from MediaCrawler root
    sys.path.insert(0, str(mc_dir))

    mods = iter_candidate_modules(mc_dir)
    if len(mods) > args.max_modules:
        mods = mods[: args.max_modules]
    print(f"[SCAN] candidate model-like modules: {len(mods)}")

    all_mds: List[MetaData] = []
    ok = 0
    failed: List[Tuple[str, str]] = []
    for m in mods:
        try:
            mod = importlib.import_module(m)
            ok += 1
            all_mds.extend(collect_metadata_from_module(mod))
        except Exception as e:
            failed.append((m, repr(e)))

    # 去重 metadata
    uniq_mds: List[MetaData] = []
    seen: Set[int] = set()
    for md in all_mds:
        if id(md) not in seen:
            uniq_mds.append(md)
            seen.add(id(md))

    # 统计表
    table_names: Set[str] = set()
    for md in uniq_mds:
        for t in md.tables.values():
            table_names.add(t.name)

    print(f"[IMPORT] ok={ok} failed={len(failed)} metadatas={len(uniq_mds)} tables={len(table_names)}")
    if failed:
        print("[WARN] failed imports (top 10):")
        for m, e in failed[:10]:
            print(f"  - {m}: {e}")

    if not uniq_mds:
        raise RuntimeError(
            "仍未找到任何 MetaData。建议：在 MediaCrawler 目录里全文搜索 `weibo_note` 或 `__tablename__`，"
            "确认模型文件是否在当前目录结构内，或把模型文件路径贴出来我再做定向导入。"
        )

    engine = create_async_engine(url, echo=args.echo, future=True)

    async with engine.begin() as conn:
        for md in uniq_mds:
            await conn.run_sync(md.create_all)

    await engine.dispose()

    print(f"[OK] create_all done. sample tables: {sorted(list(table_names))[:25]}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
