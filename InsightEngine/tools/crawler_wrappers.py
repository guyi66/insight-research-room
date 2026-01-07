"""
简易爬虫适配器封装（本地 + 外部库 + FinGPT）
目标：为 `NewsAgent` 提供统一的外部数据适配器接口，尽量在无法访问数据库时回退到本地文件或可选第三方库。
每个函数签名：(symbol, start_date, end_date, limit) -> list[dict]
输出项字典应包含：publish_time, title, content, url, platform

实现原则：
- 以安全（不抛未捕获异常）和惰性导入第三方库为主。
- 当环境变量 SKIP_DB=1 时，`load_from_mediacrawler_db` 不会尝试连接数据库。
- 对于本地 HTML/JSON/CSV 文件，做简单解析并去除 HTML 标签以得到文本。
"""
from __future__ import annotations

import os
import re
import json
from typing import List, Dict, Any
from datetime import datetime

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def _strip_html_tags(text: str) -> str:
    if not text:
        return ""
    # very small HTML stripper
    text = re.sub(r'<script[\s\S]*?</script>', '', text, flags=re.I)
    text = re.sub(r'<style[\s\S]*?</style>', '', text, flags=re.I)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _normalize_item(d: Dict[str, Any], provider_name: str) -> Dict[str, Any]:
    title = d.get('title') or d.get('headline') or d.get('summary') or d.get('name') or ''
    content = d.get('content') or d.get('summary') or d.get('body') or ''
    publish_time = d.get('publish_time') or d.get('date') or d.get('datetime') or d.get('time') or ''
    url = d.get('url') or d.get('link') or ''
    platform = d.get('platform') or d.get('source') or provider_name
    # normalize publish_time to string
    if isinstance(publish_time, (datetime,)):
        publish_time = publish_time.strftime('%Y-%m-%d %H:%M:%S')
    return {
        'publish_time': str(publish_time) if publish_time is not None else '',
        'title': (title or '')[:300],
        'content': (content or ''),
        'url': url or '',
        'platform': platform,
    }


# ---------------------------
# FinGPT provider / scrapers
# ---------------------------
def fetch_from_fingpt_provider(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Try to call FinGPT bundled news provider if present.
    returns list of normalized dicts.
    """
    out = []
    try:
        fg_path = os.path.join(WORKSPACE_ROOT, 'FinGPT', 'fingpt', 'FinGPT_Others', 'news_provider.py')
        if os.path.exists(fg_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location('fg_news_provider', fg_path)
            fg_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fg_mod)
            try:
                fg_list = fg_mod.fetch_news(symbol, start_date, end_date, finnhub_client=None)
            except TypeError:
                # different signature
                fg_list = fg_mod.fetch_news(symbol, start_date, end_date)
            except Exception:
                fg_list = None
            if fg_list:
                for n in fg_list[:limit]:
                    out.append(_normalize_item({
                        'date': n.get('date') or n.get('datetime') or n.get('time'),
                        'headline': n.get('headline') or n.get('title'),
                        'summary': n.get('summary') or n.get('body') or n.get('description'),
                        'url': n.get('url')
                    }, 'FinGPT_news'))
    except Exception:
        # swallow exceptions; adapter must be safe
        return []
    return out


def fetch_from_fingpt_scraper(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Alternative FinGPT scrapers; try to find 'fingpt' scrapers or other scripts in FinGPT package.
    """
    # For safety, reuse provider implementation
    return fetch_from_fingpt_provider(symbol, start_date, end_date, limit=limit)


# ---------------------------
# Finance libs (akshare / baostock / tushare)
# ---------------------------

def fetch_from_finance_libs(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Try common finance libs (akshare, baostock, tushare) to fetch related news.
    This function attempts imports lazily and degrades gracefully when libs are not installed.
    """
    out = []
    # 1) akshare
    try:
        import akshare as ak
        # ak contains several news APIs; try a few known ones
        try:
            # ak.stock_news might exist in some versions
            if hasattr(ak, 'stock_news'):
                res = ak.stock_news(symbol)
                # res might be DataFrame
                if hasattr(res, 'to_dict'):
                    rows = res.to_dict('records')
                    for r in rows[:limit]:
                        out.append(_normalize_item({
                            'date': r.get('time') or r.get('date'),
                            'headline': r.get('title') or r.get('headline'),
                            'summary': r.get('summary') or r.get('content'),
                            'url': r.get('url')
                        }, 'akshare'))
        except Exception:
            pass
    except Exception:
        pass

    # 2) baostock (historical news less common) - try to fetch company news via baostock is limited; skip if not available
    try:
        import baostock as bs
        # baostock is primarily OHLC, skip heavy usage
    except Exception:
        pass

    # 3) tushare (if installed and token provided)
    try:
        import tushare as ts
        try:
            pro = ts.pro_api(os.environ.get('TUSHARE_API_TOKEN'))
            # tushare pro has news via 'news' endpoint in some versions
            if hasattr(pro, 'news'):
                res = pro.news(ts_code=symbol)
                if hasattr(res, 'to_dict'):
                    rows = res.to_dict('records')
                    for r in rows[:limit]:
                        out.append(_normalize_item({
                            'date': r.get('time') or r.get('datetime') or r.get('publish_time'),
                            'headline': r.get('title') or r.get('headline'),
                            'summary': r.get('text') or r.get('content'),
                            'url': r.get('url')
                        }, 'tushare'))
        except Exception:
            pass
    except Exception:
        pass

    # Deduplicate by title
    seen = set()
    uniq = []
    for it in out:
        k = (it.get('title') or '')[:120]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
        if len(uniq) >= limit:
            break
    return uniq


# ---------------------------
# Local files reader (reports, final_reports, charts, media folders)
# ---------------------------

def fetch_from_media_files(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Scan repository local report/artifacts and convert to news-like items.
    Looks under: reports/, final_reports/, charts/, media_engine_streamlit_reports/
    """
    candidates = []
    search_dirs = [
        os.path.join(WORKSPACE_ROOT, 'reports'),
        os.path.join(WORKSPACE_ROOT, 'final_reports'),
        os.path.join(WORKSPACE_ROOT, 'charts'),
        os.path.join(WORKSPACE_ROOT, 'media_engine_streamlit_reports'),
        os.path.join(WORKSPACE_ROOT, 'insight_engine_streamlit_reports') if os.path.exists(os.path.join(WORKSPACE_ROOT, 'insight_engine_streamlit_reports')) else '',
    ]
    for d in search_dirs:
        if not d or not os.path.exists(d):
            continue
        try:
            for root, _, files in os.walk(d):
                for fn in files:
                    if fn.lower().endswith(('.html', '.htm', '.json', '.csv', '.txt')):
                        path = os.path.join(root, fn)
                        try:
                            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                                data = f.read()
                        except Exception:
                            continue
                        title = fn
                        # attempt to extract a title from HTML
                        if fn.lower().endswith(('.html', '.htm')):
                            m = re.search(r'<title>(.*?)</title>', data, flags=re.I | re.S)
                            if m:
                                title = _strip_html_tags(m.group(1))[:200]
                        # extract excerpt
                        text = _strip_html_tags(data)[:2000]
                        # try to infer date from filename or file mtime
                        dt = ''
                        mdate = None
                        m = re.search(r'(20\d{2}[\-_]?\d{2}[\-_]?\d{2})', fn)
                        if m:
                            raw = m.group(1)
                            try:
                                mdate = datetime.strptime(raw.replace('_', '').replace('-', ''), '%Y%m%d')
                                dt = mdate.strftime('%Y-%m-%d')
                            except Exception:
                                mdate = None
                        if not dt:
                            try:
                                stat = os.stat(path)
                                dt = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                            except Exception:
                                dt = ''
                        candidates.append({
                            'publish_time': dt,
                            'title': title,
                            'content': text,
                            'url': path,
                            'platform': 'local_report'
                        })
        except Exception:
            continue
    # Filter by symbol presence
    filtered = [c for c in candidates if (symbol in (c.get('title') or '') or symbol in (c.get('content') or '') )]
    if not filtered:
        # if none match symbol, return recent candidates
        filtered = sorted(candidates, key=lambda x: x.get('publish_time') or '', reverse=True)
    # normalize
    out = []
    for it in filtered[:limit]:
        out.append(_normalize_item(it, 'local_report'))
    return out


# ---------------------------
# MediaCrawler DB loader (safe fallback)
# ---------------------------

def load_from_mediacrawler_db(symbol: str, start_date: str, end_date: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Adapter that would normally load from MediaCrawler's DB; here respect SKIP_DB and fallback to local files.
    If SKIP_DB env var is set to '1' or 'true', do not attempt DB connections.
    """
    skip = os.environ.get('SKIP_DB', '').strip() in ('1', 'true', 'True')
    if skip:
        # fallback to local files
        return fetch_from_media_files(symbol, start_date, end_date, limit=limit)

    # Otherwise, attempt to import the project's search backend and call it safely
    try:
        from InsightEngine.tools.search import MediaCrawlerDB as _MCDB
        db = _MCDB()
        # try a safe method search_topic_by_date
        if hasattr(db, 'search_topic_by_date'):
            resp = db.search_topic_by_date(topic=symbol, start_date=start_date, end_date=end_date, limit_per_table=limit)
            if resp and getattr(resp, 'results', None):
                out = []
                for r in resp.results:
                    out.append(_normalize_item({
                        'date': getattr(r, 'publish_time', None),
                        'headline': getattr(r, 'title_or_content', None),
                        'summary': getattr(r, 'title_or_content', None),
                        'url': getattr(r, 'url', None),
                    }, 'mediacrawler_db'))
                return out
    except Exception:
        # If any error (lack of DB driver, connectivity), fall back to local files
        return fetch_from_media_files(symbol, start_date, end_date, limit=limit)

    # default to empty
    return []


# ---------------------------
# Platform crawler shim
# ---------------------------

def fetch_from_platform_crawler(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Attempt to invoke platform crawler modules in repo (e.g., MindSpider/ForumEngine)
    and normalize results. Safe if modules are absent.
    """
    out = []
    try:
        # try MindSpider.platform_crawler like modules
        ms_path = os.path.join(WORKSPACE_ROOT, 'MindSpider')
        if os.path.exists(ms_path):
            try:
                import importlib
                mod = importlib.import_module('MindSpider')
                if hasattr(mod, 'platform_crawler'):
                    pc = getattr(mod, 'platform_crawler')
                    if hasattr(pc, 'run'):
                        fetched = pc.run(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
                        for it in fetched or []:
                            out.append(_normalize_item(it, 'platform_crawler'))
            except Exception:
                pass
    except Exception:
        pass
    return out


# ---------------------------
# Paid APIs (Finnhub / Marketaux / Tushare)
# ---------------------------

def fetch_from_paid_apis(symbol: str, start_date: str, end_date: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Query paid/free news APIs in priority order: Finnhub -> Marketaux -> Tushare.
    Returns normalized items. Respects missing libraries and missing API keys.
    """
    out: List[Dict[str, Any]] = []
    # helper to append and limit
    def _append_list(rows):
        for r in rows:
            if len(out) >= limit:
                break
            out.append(_normalize_item(r, 'paid_api'))
    # prepare symbol variants
    variants = [symbol]
    if symbol.isdigit():
        variants.extend([f"{symbol}.SS", f"{symbol}.SZ", f"SSE:{symbol}", f"SZSE:{symbol}", f"SH:{symbol}", f"SZ:{symbol}"])
    variants = list(dict.fromkeys(variants))
    # small debug print
    try:
        print(f"[crawler_wrappers] fetch_from_paid_apis trying variants: {variants}")
    except Exception:
        pass

    # 1) Finnhub
    try:
        api_key = os.environ.get('FINNHUB_API_KEY')
        if api_key:
            try:
                import requests
                url = 'https://finnhub.io/api/v1/company-news'
                for v in variants:
                    try:
                        params = {'symbol': v, 'from': start_date, 'to': end_date, 'token': api_key}
                        resp = requests.get(url, params=params, timeout=8)
                        print(f"[crawler_wrappers] Finnhub({v}) status={getattr(resp,'status_code',None)}")
                        if resp.status_code == 200:
                            data = resp.json()
                            if isinstance(data, list) and data:
                                rows = []
                                for n in data[:limit]:
                                    try:
                                        dt = n.get('datetime')
                                        if dt and isinstance(dt, (int, float, str)):
                                            try:
                                                dt_s = datetime.fromtimestamp(int(dt)).strftime('%Y-%m-%d %H:%M:%S')
                                            except Exception:
                                                dt_s = str(dt)
                                        else:
                                            dt_s = n.get('datetime') or n.get('time') or ''
                                        rows.append({
                                            'date': dt_s,
                                            'headline': n.get('headline') or n.get('title'),
                                            'summary': n.get('summary') or n.get('description') or n.get('body'),
                                            'url': n.get('url'),
                                            'source': n.get('source') or 'finnhub'
                                        })
                                    except Exception:
                                        continue
                                _append_list(rows)
                                break
                    except Exception as e:
                        print(f"[crawler_wrappers] Finnhub({v}) error: {e}")
                        continue
            except Exception as e:
                print(f"[crawler_wrappers] Finnhub driver error: {e}")
                pass
    except Exception:
        pass

    # early exit if enough
    if len(out) >= min(10, limit):
        return out[:limit]

    # 2) Marketaux
    try:
        api_key = os.environ.get('MARKETAUX_API_KEY')
        if api_key:
            try:
                import requests
                url = 'https://api.marketaux.com/v1/news/all'
                for v in variants:
                    try:
                        params = {'symbols': v, 'api_token': api_key, 'language': 'en', 'per_page': min(limit, 100)}
                        resp = requests.get(url, params=params, timeout=8)
                        print(f"[crawler_wrappers] Marketaux({v}) status={getattr(resp,'status_code',None)}")
                        if resp.status_code == 200:
                            data = resp.json()
                            results = data.get('data') or data.get('results') or []
                            if results:
                                rows = []
                                for n in results[:limit]:
                                    rows.append({
                                        'date': n.get('published_at') or n.get('published_at_datetime') or n.get('date'),
                                        'headline': n.get('title') or n.get('headline'),
                                        'summary': n.get('summary') or n.get('description') or n.get('body'),
                                        'url': n.get('url'),
                                        'source': n.get('source') or n.get('source_name') or 'marketaux'
                                    })
                                _append_list(rows)
                                break
                    except Exception as e:
                        print(f"[crawler_wrappers] Marketaux({v}) error: {e}")
                        continue
            except Exception as e:
                print(f"[crawler_wrappers] Marketaux driver error: {e}")
                pass
    except Exception:
        pass

    # 3) Tushare pro (if python tushare available and token set)
    try:
        token = os.environ.get('TUSHARE_API_TOKEN')
        if token:
            try:
                import tushare as ts
                pro = ts.pro_api(token)
                # try pro.news or pro.stk_basic news endpoints (versions vary)
                if hasattr(pro, 'news'):
                    try:
                        res = pro.news(ts_code=symbol)
                        # res may be DataFrame-like
                        if hasattr(res, 'to_dict'):
                            rows_raw = res.to_dict('records')
                        else:
                            rows_raw = list(res)
                        rows = []
                        for n in rows_raw[:limit]:
                            rows.append({
                                'date': n.get('time') or n.get('datetime') or n.get('publish_time') or n.get('date'),
                                'headline': n.get('title') or n.get('headline'),
                                'summary': n.get('text') or n.get('content') or '',
                                'url': n.get('url') or '',
                                'source': n.get('source') or 'tushare'
                            })
                        _append_list(rows)
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass

    # final debug
    try:
        print(f"[crawler_wrappers] fetch_from_paid_apis returning {len(out)} items")
    except Exception:
        pass

    # deduplicate by title
    seen = set()
    uniq = []
    for it in out:
        k = (it.get('title') or it.get('headline') or '')[:120]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
        if len(uniq) >= limit:
            break
    return uniq


# Expose names expected by InsightEngine.tools.__init__
__all__ = [
    'fetch_from_fingpt_provider',
    'fetch_from_fingpt_scraper',
    'fetch_from_finance_libs',
    'fetch_from_media_files',
    'load_from_mediacrawler_db',
    'fetch_from_platform_crawler',
    'fetch_from_paid_apis',
']
