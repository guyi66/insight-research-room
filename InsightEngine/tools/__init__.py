"""
工具调用模块
提供外部工具接口，如本地数据库查询等
"""

from .search import (
    MediaCrawlerDB,
    QueryResult,
    DBResponse,
    print_response_summary
)
from .keyword_optimizer import (
    KeywordOptimizer,
    KeywordOptimizationResponse,
    keyword_optimizer
)
from .sentiment_analyzer import (
    WeiboMultilingualSentimentAnalyzer,
    SentimentResult,
    BatchSentimentResult,
    multilingual_sentiment_analyzer,
    analyze_sentiment
)

# Try to import crawler_wrappers safely and export only available adapters.
try:
    from . import crawler_wrappers as _crawler_wrappers
    fetch_from_fingpt_provider = getattr(_crawler_wrappers, 'fetch_from_fingpt_provider', None)
    fetch_from_fingpt_scraper = getattr(_crawler_wrappers, 'fetch_from_fingpt_scraper', None)
    fetch_from_finance_libs = getattr(_crawler_wrappers, 'fetch_from_finance_libs', None)
    fetch_from_media_files = getattr(_crawler_wrappers, 'fetch_from_media_files', None)
    load_from_mediacrawler_db = getattr(_crawler_wrappers, 'load_from_mediacrawler_db', None)
    fetch_from_platform_crawler = getattr(_crawler_wrappers, 'fetch_from_platform_crawler', None)
except Exception:
    fetch_from_fingpt_provider = None
    fetch_from_fingpt_scraper = None
    fetch_from_finance_libs = None
    fetch_from_media_files = None
    load_from_mediacrawler_db = None
    fetch_from_platform_crawler = None

__all__ = [
    "MediaCrawlerDB",
    "QueryResult",
    "DBResponse",
    "print_response_summary",
    "KeywordOptimizer",
    "KeywordOptimizationResponse",
    "keyword_optimizer",
    "WeiboMultilingualSentimentAnalyzer",
    "SentimentResult",
    "BatchSentimentResult",
    "multilingual_sentiment_analyzer",
    "analyze_sentiment",
    'fetch_from_fingpt_provider',
    'fetch_from_fingpt_scraper',
    'load_from_mediacrawler_db',
    'fetch_from_platform_crawler',
    'fetch_from_finance_libs',
    'fetch_from_media_files',
]
