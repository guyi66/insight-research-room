#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
from datetime import datetime, timedelta, date
import os
import re
import json
from typing import Any, Dict, List, Tuple

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


_POS = ["利好","看好","买入","增持","回购","分红","超预期","起飞","主升","反转","强势","稳"]
_NEG = ["利空","暴雷","调查","立案","处罚","问询","风险","出货","砸盘","崩","割肉","库存","价格倒挂"]
_EUP = ["梭哈","翻倍","闭眼买","躺赢","无脑买","一字板"]
_PAN = ["崩盘","完了","跑路","爆仓","踩踏","恐慌","顶不住"]


def score(text: str) -> Dict[str, int]:
    t = (text or "")
    return {
        "pos": sum(t.count(w) for w in _POS),
        "neg": sum(t.count(w) for w in _NEG),
        "eup": sum(t.count(w) for w in _EUP),
        "pan": sum(t.count(w) for w in _PAN),
    }


def fetch_today(cfg, code: str, limit: int = 300) -> List[Dict[str, Any]]:
    d = datetime.now().date()
    start = datetime(d.year, d.month, d.day, 0, 0, 0)
    end = start + timedelta(days=1)
    sql = """
    SELECT stock_code, platform, keyword, url, title, author, published_at, content,
           like_count, comment_count, share_count, collect_count, crawled_at
      FROM mindspider_post
     WHERE stock_code=%s
       AND (
         (published_at IS NOT NULL AND published_at >= %s AND published_at < %s)
         OR
         (published_at IS NULL AND crawled_at >= %s AND crawled_at < %s)
       )
     ORDER BY COALESCE(comment_count,0) DESC, COALESCE(like_count,0) DESC, COALESCE(published_at, crawled_at) DESC
     LIMIT %s
    """
    conn = _mysql_conn_from_cfg(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (code, start, end, start, end, int(limit)))
            return cur.fetchall() or []
    finally:
        conn.close()


def fetch_baseline(cfg, code: str, days: int = 14) -> List[Tuple[str, int, int]]:
    since = datetime.now() - timedelta(days=max(1, int(days)))
    sql = """
    SELECT DATE(COALESCE(published_at, crawled_at)) AS d,
           COUNT(*) AS cnt,
           SUM(COALESCE(comment_count,0)+COALESCE(like_count,0)+COALESCE(share_count,0)) AS eng
      FROM mindspider_post
     WHERE stock_code=%s
       AND COALESCE(published_at, crawled_at) >= %s
     GROUP BY DATE(COALESCE(published_at, crawled_at))
     ORDER BY d ASC
    """
    conn = _mysql_conn_from_cfg(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (code, since))
            rows = cur.fetchall() or []
    finally:
        conn.close()
    out = []
    for r in rows:
        out.append((str(r["d"]), int(r["cnt"] or 0), int(r["eng"] or 0)))
    return out


def classify(today_cnt: int, today_eng: int, base: List[Tuple[str,int,int]], posr: float, negr: float, eupr: float, panr: float) -> Tuple[str,str]:
    if today_cnt <= 0:
        return "跳过", "当天无 MindSpider 舆情数据（视为未进行舆情分析）"

    cnts = [c for _,c,_ in base] or [0]
    engs = [e for _,_,e in base] or [0]
    mean_cnt = sum(cnts)/max(1,len(cnts))
    mean_eng = sum(engs)/max(1,len(engs))
    heat = (today_cnt/mean_cnt) if mean_cnt else 0.0
    eng = (today_eng/mean_eng) if mean_eng else 0.0
    bias = posr - negr

    hot = (heat >= 1.5) or (eng >= 1.8)
    if hot and bias >= 0.18 and eupr >= 0.08:
        return "过热", "热度/互动显著偏高，且乐观与亢奋表达占优"
    if hot and bias <= -0.18 and panr >= 0.08:
        return "过冷", "热度/互动显著偏高，但悲观与恐慌表达占优（负面集中爆发）"
    if (heat <= 0.7) and (negr >= 0.25) and (panr >= 0.05):
        return "过冷", "热度偏低但负面浓度较高（冷清且悲观）"
    return "中性/分歧", "未触发过热/过冷阈值，整体更像分歧或中性"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--code", required=True)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--baseline-days", type=int, default=14)
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()

    cfg = load_config(args.config)
    today = datetime.now().date().isoformat()

    posts = fetch_today(cfg, args.code.strip(), limit=args.limit)
    if not posts:
        print(f"[SKIP] {args.code} {today}: 当天无 MindSpider 舆情数据，不做舆情分析。")
        return

    base = fetch_baseline(cfg, args.code.strip(), days=args.baseline_days)

    agg = {"pos":0,"neg":0,"eup":0,"pan":0}
    for p in posts:
        t = f"{p.get('title','')} {p.get('content','')}"
        s = score(t)
        for k in agg: agg[k] += s[k]

    denom = max(1, len(posts))
    posr, negr, eupr, panr = agg["pos"]/denom, agg["neg"]/denom, agg["eup"]/denom, agg["pan"]/denom
    today_eng = 0
    for p in posts:
        today_eng += int(p.get("comment_count") or 0) + int(p.get("like_count") or 0) + int(p.get("share_count") or 0)

    verdict, reason = classify(len(posts), today_eng, base, posr, negr, eupr, panr)

    posts_sorted = sorted(
        posts,
        key=lambda x: (int(x.get("comment_count") or 0), int(x.get("like_count") or 0), str(x.get("published_at") or x.get("crawled_at") or "")),
        reverse=True
    )

    out_dir = args.out_dir or os.path.join(".", "output", "mindspider_sentiment_intel")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    materials_path = os.path.join(out_dir, f"{args.code}_{today}_{ts}_mindspider_materials.json")
    report_path = os.path.join(out_dir, f"{args.code}_{today}_{ts}_mindspider_report.md")

    payload = {
        "meta": {"code": args.code, "date": today, "posts": len(posts), "engagement_proxy": today_eng},
        "baseline_daily": base,
        "lexicon": {"agg": agg, "pos_ratio": posr, "neg_ratio": negr, "eup_ratio": eupr, "pan_ratio": panr},
        "verdict": verdict,
        "reason": reason,
        "top_posts": posts_sorted[:max(10,args.top)]
    }
    with open(materials_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    def excerpt(s: str, n: int=120) -> str:
        s = re.sub(r"\s+"," ", (s or "").strip())
        return (s[:n-1]+"…") if len(s) > n else s

    lines = []
    lines.append(f"### MindSpider 舆情（日内）分析：{args.code}")
    lines.append(f"- 分析日期（当天）：{today}")
    lines.append(f"- 样本量：{len(posts)} 条（仅当日）")
    lines.append(f"- 结论：**{verdict}**")
    lines.append(f"- 理由：{reason}")
    lines.append("")
    lines.append("#### 重要舆情清单（当天，按互动强度排序）")
    for i, p in enumerate(posts_sorted[:max(10,args.top)], 1):
        dt = p.get("published_at") or p.get("crawled_at")
        lines.append(f"{i}. **[{p.get('platform','')}] {excerpt(p.get('title',''), 80)}**")
        lines.append(f"   - 时间：{dt}")
        lines.append(f"   - 作者：{p.get('author','')}")
        lines.append(f"   - 互动：赞/评/转={p.get('like_count',0)}/{p.get('comment_count',0)}/{p.get('share_count',0)}")
        lines.append(f"   - 链接：{p.get('url','')}")
        lines.append(f"   - 摘录：{excerpt(p.get('content','') or p.get('title',''), 120)}")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[OK] Materials saved: {materials_path}")
    print(f"[OK] Report saved:    {report_path}")
    print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
