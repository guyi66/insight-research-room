#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run BettaFish analysis pipeline with optional crawlers.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_pipeline import run_pipeline


def _parse_kv_pairs(values: Optional[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in values or []:
        if "=" not in item:
            raise ValueError(f"Invalid KEY=VALUE pair: {item}")
        key, val = item.split("=", 1)
        out[key.strip()] = val.strip()
    return out


def _parse_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="BettaFish analysis pipeline runner")
    ap.add_argument("--code", required=True, help="股票代码，例如 600519")
    ap.add_argument("--name", default=None, help="股票名称（可选，用于爬虫/消息面）")
    ap.add_argument("--report", default="combined", choices=["tech", "fund", "news", "combined"], help="single report mode")
    ap.add_argument("--reports", default=None, help="multi report modes, comma separated")
    ap.add_argument("--news-days", type=int, default=7, help="消息面时间窗口（天）")
    ap.add_argument("--bocha", action="store_true", help="启用 Bocha 搜索")
    ap.add_argument("--out-dir", default=None, help="报告输出目录（默认 reports）")

    # DB overrides
    ap.add_argument("--db-host", default=None)
    ap.add_argument("--db-port", default=None)
    ap.add_argument("--db-user", default=None)
    ap.add_argument("--db-password", default=None)
    ap.add_argument("--db-name", default=None)
    ap.add_argument("--db-charset", default=None)

    # env overrides (API keys, etc.)
    ap.add_argument("--set", action="append", default=[], help="环境变量覆盖，形如 KEY=VALUE")

    # MindSpider crawler
    ap.add_argument("--crawl-mindspider", action="store_true", help="启用 MindSpider 爬虫")
    ap.add_argument("--mindspider-dir", default=None, help="MindSpider 根目录（默认当前项目）")
    ap.add_argument("--mindspider-platforms", default=None, help="平台列表，逗号分隔")
    ap.add_argument("--mindspider-days", type=int, default=1)
    ap.add_argument("--mindspider-max-notes", type=int, default=10)
    ap.add_argument("--mindspider-keywords", default=None, help="MindSpider 关键词，逗号分隔")
    ap.add_argument("--mindspider-no-crawl", action="store_true", help="只打标不触发爬虫")

    # Guba crawler
    ap.add_argument("--crawl-guba", action="store_true", help="启用股吧爬虫")
    ap.add_argument("--guba-dir", default=None, help="Guba-Crawler 目录")
    ap.add_argument("--guba-pages", type=int, default=10)
    ap.add_argument("--guba-init-schema", action="store_true")
    ap.add_argument("--guba-fulltext", action="store_true")
    ap.add_argument("--guba-fulltext-days", type=int, default=7)
    ap.add_argument("--guba-fulltext-limit", type=int, default=200)

    args = ap.parse_args()

    env_overrides = _parse_kv_pairs(args.set)
    db_overrides = {
        k: v for k, v in {
            "DB_HOST": args.db_host,
            "DB_PORT": args.db_port,
            "DB_USER": args.db_user,
            "DB_PASSWORD": args.db_password,
            "DB_NAME": args.db_name,
            "DB_CHARSET": args.db_charset,
        }.items() if v
    }

    if args.bocha:
        os.environ["BETTAFISH_USE_BOCHA"] = "1"

    report_modes = _parse_list(args.reports)

    result = run_pipeline(
        symbol=args.code,
        report_mode=args.report,
        report_modes=report_modes,
        stock_name=args.name,
        news_days=args.news_days,
        use_bocha=args.bocha,
        output_dir=args.out_dir,
        env_overrides=env_overrides,
        db_overrides=db_overrides,
        crawl_mindspider=args.crawl_mindspider,
        mindspider_platforms=_parse_list(args.mindspider_platforms),
        mindspider_days=args.mindspider_days,
        mindspider_max_notes=args.mindspider_max_notes,
        mindspider_dir=args.mindspider_dir,
        mindspider_keywords=args.mindspider_keywords,
        mindspider_no_crawl=args.mindspider_no_crawl,
        crawl_guba=args.crawl_guba,
        guba_dir=args.guba_dir,
        guba_pages=args.guba_pages,
        guba_fulltext=args.guba_fulltext,
        guba_fulltext_days=args.guba_fulltext_days,
        guba_fulltext_limit=args.guba_fulltext_limit,
        guba_init_schema=args.guba_init_schema,
    )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
