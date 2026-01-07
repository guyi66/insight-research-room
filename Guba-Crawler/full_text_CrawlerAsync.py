# FullTextCrawler_async_minimal.py
"""
东方财富股吧全文异步爬虫
内部 aiohttp，对外同步接口
限制QPS为60，纯异步实现
"""
import asyncio
import aiohttp
import logging
import os
import sys
from bs4 import BeautifulSoup
from database_client import DatabaseManager
import configparser
import time
import random
from datetime import datetime
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
from db.guba_mysql_adapter import GubaMySQLAdapter
from retrying import retry
from threading import Lock
from user_agent_manager import get_user_agent_manager
from functools import wraps
import asyncio.exceptions

# ----------------- 新增 -----------------
# 限速器 - 控制QPS为60
class RateLimiter:
    def __init__(self, rate_limit=10):
        self.rate_limit = rate_limit  # 每秒请求数限制
        self.tokens = rate_limit      # 令牌桶初始令牌数
        self.last_refill = time.time()
        self.lock = asyncio.Lock()    # 异步锁
    
    async def acquire(self):
        """获取一个令牌，如果没有令牌则等待"""
        async with self.lock:
            # 计算需要补充的令牌
            now = time.time()
            elapsed = now - self.last_refill
            # 补充令牌（不超过最大限制）
            self.tokens = min(self.rate_limit, self.tokens + elapsed * self.rate_limit)
            self.last_refill = now
            
            if self.tokens < 1:
                # 计算需要等待的时间
                wait_time = (1 - self.tokens) / self.rate_limit
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

# 异步重试装饰器，不阻塞队列
def async_retry(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    # 指数退避
                    wait_time = delay * (2 ** (retries - 1)) * (0.5 + random.random())
                    logging.warning(f"重试 {func.__name__}，第 {retries} 次，等待 {wait_time:.2f} 秒: {str(e)}")
                    await asyncio.sleep(wait_time)
        return wrapper
    return decorator

# 全局 aiohttp 的 TCPConnector，支持连接池、代理、超时
class AsyncHttpClient:
    def __init__(self, ua_manager, rate_limiter: RateLimiter):
        # 代理商提供的代理配置
        self.tunnel = "x291.kdltps.com:15818"
        self.username = "t15462021520395"
        self.password = "wkjzgkdb"
        self.proxy_auth = aiohttp.BasicAuth(self.username, self.password)
        
        self.ua_manager = ua_manager
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.rate_limiter = rate_limiter
        # 创建信号量，限制并发请求数不超过100
        self.semaphore = asyncio.Semaphore(100)  # 限制并发数为100个协程

    async def __aenter__(self):
        conn = aiohttp.TCPConnector(limit=100, limit_per_host=20, enable_cleanup_closed=True)
        
        # 优化后的请求头，模拟真实浏览器
        headers = {
            "User-Agent": self.ua_manager.get_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Accept-Encoding": "gzip",  # 支持gzip压缩
            "Cache-Control": "max-age=0",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
            'Connection': 'close'
        }
        
        self.session = aiohttp.ClientSession(
            connector=conn,
            timeout=self.timeout,
            headers=headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    @async_retry(max_retries=3, delay=1)
    async def get_soup(self, url: str) -> BeautifulSoup | None:
        # 获取令牌，限制请求速率
        await self.rate_limiter.acquire()
        
        # 随机分配通道（1-8）
        channel = random.randint(1, 8)
        
        try:
            # 使用信号量限制并发
            async with self.semaphore:
                proxy_url = f"http://{self.tunnel}"
                # 在请求头中指定通道
                headers = {"kdl-tps-channel": str(channel)}
                async with self.session.get(url, proxy=proxy_url, proxy_auth=self.proxy_auth, headers=headers) as resp:
                    if resp.status == 200:
                        # 检查内容是否为gzip压缩
                        content_encoding = resp.headers.get('Content-Encoding', '')
                        if 'gzip' in content_encoding:
                            html = await resp.text(encoding="utf-8", errors="ignore")
                        else:
                            html = await resp.text(encoding="utf-8", errors="ignore")
                        return BeautifulSoup(html, "lxml")
                    elif resp.status == 403:
                        logging.warning(f"[403] {url}")  # 精简日志
                        await asyncio.sleep(1)
                        raise ValueError(f"访问被拒绝 (403): {url}")
                    else:
                        logging.warning(f"[{resp.status}] {url}")  # 精简日志
                        await asyncio.sleep(1)
                        raise ValueError(f"HTTP错误 ({resp.status}): {url}")
        except Exception as e:
            logging.warning(f"[请求失败] {url}: {e}")  # 精简日志
            raise
# --------------------------------------


class FullTextCrawler():
    def __init__(self, config_path: str = 'config.ini',
                 db_name: str = 'guba',
                 collection_name: str = None,
                 concurrency: int = 30,
                 rate_limit: int = 60):
        self.success_count = 0
        self.fail_count = 0
        self.running = True
        self.count_lock = Lock()
        self.concurrency = concurrency     # 并发任务数
        self.rate_limit = rate_limit       # QPS限制

        self.db_manager = DatabaseManager(config_path)
        self.store = GubaMySQLAdapter(config_path)
        self.redis_client = self.db_manager.get_redis_client()

        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
        self.logger = logging.getLogger(self.config.get('logging', 'log_file'))
        self.ua_manager = get_user_agent_manager('random')

        # 代理
        tunnel = "x291.kdltps.com:15818"
        username = "t15462021520395"
        password = "wkjzgkdb"
        self.proxy_url = f"http://{username}:{password}@{tunnel}"
        
        # 创建限速器
        self.rate_limiter = RateLimiter(rate_limit=self.rate_limit)


    # ========= 以下方法签名不变，仅内部实现改为 async =========
    # 1. get_soup_form_url -> 删除，由 AsyncHttpClient 代替
    # 2. get_full_text / crawl 全部重写成 async
    # 3. worker_thread -> 改为 async worker
    # 4. run -> 包装 asyncio.run

    # ---------- 关键重写 ----------
    async def _change_proxy_ip(self):
        """调用快代理更换隧道IP接口"""
        try:
            import aiohttp
            change_url = "https://tps.kdlapi.com/api/changetpsip"
            params = {
                "secret_id": "oqifdb1h1ykoxm8comcv",
                "signature": "vhp8ervln42dkh85ht3ijw6adctj1wah"
            }
            
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(change_url, params=params) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("code") == 0:
                            logging.info(f"代理IP更换成功: {result.get('data', {}).get('new_ip')}")
                            return True
                        else:
                            logging.error(f"代理IP更换失败: {result.get('msg')}")
                    else:
                        logging.error(f"代理IP更换请求失败: {resp.status}")
        except Exception as e:
            logging.error(f"更换代理IP异常: {e}")
        return False

    @async_retry(max_retries=3, delay=2)
    async def _async_get_full_text(self, url: str, http: AsyncHttpClient):
        """异步版本 get_full_text，带重试机制"""
        full_text, post_time = None, ""

        if "caifuhao" in url:
            result = await self._async_handle_caifuhao_url(url, http)
            if result and result[0] and result[1]:
                return result
            else:
                # 调用更换IP
                await self._change_proxy_ip()
                raise ValueError("财富号页面解析失败")

        elif "/new" in url or "guba.eastmoney.com" in url:
            full_url = "http://guba.eastmoney.com" + url if url.startswith("/news") else url
            soup = await http.get_soup(full_url)
            if not soup:
                # 调用更换IP
                await self._change_proxy_ip()
                raise ValueError("未能获取页面内容")

            # 提取发布时间
            time_tag = soup.find("div", {"class": "time"})
            if time_tag:
                post_time = time_tag.text.strip()
            else:
                # 检查是否为天天基金网页面
                fund_link = soup.find("li").find("a", href="//fund.eastmoney.com") if soup.find("li") else None
                if fund_link and "天天基金" in fund_link.text:
                    return "天天基金", "-1"
                else:
                    # 尝试其他时间选择器
                    time_selectors = [
                        'div.time', 'span.time', 'div.publish-time', 
                        'div.date', 'time', '[class*="time"]', '[class*="date"]'
                    ]
                    for selector in time_selectors:
                        elements = soup.select(selector)
                        if elements:
                            for elem in elements:
                                text = elem.get_text(strip=True)
                                if text and any(char.isdigit() for char in text):
                                    post_time = text
                                    break
                            if post_time:
                                break
                    if not post_time:
                        # 调用更换IP
                        # 调用更换IP
                        await self._change_proxy_ip()
                        # 指数退避策略，更稳定
                        await asyncio.sleep(random.uniform(0.5, 1.5))

                        raise ValueError("未找到时间信息")

            # 提取正文内容
            content_tag = soup.find("div", {"id": "post_content"}) or \
                          soup.find("div", {"class": "newstext"})
            if content_tag:
                full_text = ' '.join(content_tag.get_text(strip=True).split())
                if not full_text:
                    raise ValueError("提取到空内容")
            else:
                # 尝试其他内容选择器
                content_selectors = [
                    'div.newstext', 'div#post_content', 'div.content',
                    'div.article-content', 'div.main-content', 'article',
                    'div.text-content', 'div[class*="content"]', 'div[id*="content"]',
                    '.rich-text', '.article-body'
                ]
                
                content = None
                for selector in content_selectors:
                    elements = soup.select(selector)
                    if elements:
                        # 取最长的内容
                        texts = [elem.get_text(strip=True) for elem in elements]
                        if texts:
                            content = max(texts, key=len)
                            if len(content) > 50:  # 内容长度合理
                                break
                
                if content:
                    full_text = ' '.join(content.split())
                else:
                    # 最后尝试从body中提取
                    body = soup.find('body')
                    if body:
                        # 移除导航、侧边栏等无关内容
                        for tag in body.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style']):
                            tag.decompose()
                        content = body.get_text(strip=True)
                        if content and len(content) >= 50:
                            full_text = ' '.join(content.split())
                        else:
                            raise ValueError("未找到内容区域")
                    else:
                        raise ValueError("未找到内容区域")
        
        return full_text, post_time

    @async_retry(max_retries=3, delay=2)
    async def _async_handle_caifuhao_url(self, url: str, http: AsyncHttpClient):
        """异步版 caifuhao 解析，带重试机制"""
        if url.startswith("//caifuhao"):
            full_url = "http:" + url
        else:
            full_url = url
        
        soup = await http.get_soup(full_url)
        if not soup:
            return None, None
            
        try:
            article = soup.select_one('div.article-body')
            full_text = None
            
            try:
                # 尝试三种不同的内容解析方式
                xeditor_content = article.select_one('div.xeditor_content.cfh_web')
                if xeditor_content:
                    # 获取所有p标签的文本并用换行符连接
                    p_texts = [p.get_text(strip=True) for p in xeditor_content.find_all('p') if p.get_text(strip=True)]
                    full_text = '\n'.join(p_texts)
                else:
                    xeditor_content = article.select_one('div.xeditor_content.app_h5_article')
                    if xeditor_content:
                        # 获取所有p标签的文本并用换行符连接
                        p_texts = [p.get_text(strip=True) for p in xeditor_content.find_all('p') if p.get_text(strip=True)]
                        full_text = '\n'.join(p_texts)
                    else:
                        full_text = await self._async_handle_caifuhao_type_full_text(article)
            except Exception as e:
                self.logger.error(f"3种类型解析失败 {url}: {e}")
                full_text = await self._async_handle_caifuhao_type_full_text(article)
                
            # 抓取日期
            element = soup.select_one('div.grid_wrapper > div.grid > div.g_content > div.article.page-article > div.article-head > div.article-meta > span:nth-child(2)')
            if element:
                post_time = element.get_text(strip=True)
            else:
                # 尝试其他时间选择器
                time_selectors = [
                    'div.time', 'span.time', 'div.publish-time', 
                    'div.date', 'time', '[class*="time"]', '[class*="date"]'
                ]
                post_time = ""
                for selector in time_selectors:
                    elements = soup.select(selector)
                    if elements:
                        for elem in elements:
                            text = elem.get_text(strip=True)
                            if text and any(char.isdigit() for char in text):
                                post_time = text
                                break
                        if post_time:
                            break
                
                if not post_time:
                    self.logger.error(f"{datetime.now().isoformat()} - {url} 获取时间失败")
                    post_time = -1
                    
            return full_text, post_time
        except Exception as e:
            self.logger.error(f"\n{datetime.now().isoformat()} - {url} 爬取失败、已丢弃")
            self.logger.error(e)
            # 调用更换IP
            await self._change_proxy_ip()
            return None, None

    async def _async_handle_caifuhao_type_full_text(self, article_body):
        """异步版处理财富号内容类型"""
        try:
            # 移除不需要的元素
            for unwanted in article_body.select('span.guba_stock, img, script, style'):
                unwanted.decompose()

            # 提取并过滤有效段落
            paragraphs = []
            for p in article_body.find_all("p"):
                text = p.get_text(strip=True)
                if text:
                    paragraphs.append(text)
            
            return '\n'.join(paragraphs)
        except Exception as e:
            self.logger.error(f"caifuhao类型3处理失败: {e}")
            return None

    async def _async_crawl(self, url: str, http: AsyncHttpClient):
        """异步 crawl"""
        try:
            full_text, post_time = await self._async_get_full_text(url, http)
            if full_text is None:
                raise ValueError("全文为空")

            update_data = {"full_text": full_text}
            if post_time:
                update_data["full_text_time"] = post_time

            self.store.update_one({"href": url}, update_data)
            with self.count_lock:
                self.success_count += 1
                
            self.logger.info(f"[成功] 已更新全文: {url}")
        except Exception as e:
            self.logger.error(f"[全文更新失败] {url}: {e}")
            with self.count_lock:
                self.fail_count += 1

    async def _async_worker(self, http):
        """单个 asyncio 协程 worker"""
        while self.running:
            url = self.redis_client.get_url()
            if url:
                # 等待当前请求完成后再获取下一个URL
                await self._async_crawl(url, http)
            else:
                # If the queue is empty, wait for 1 minute and check again
                self.logger.info("Redis queue is empty, waiting for 1 minute...")
                await asyncio.sleep(60)  # Wait for 1 minute
                if self.redis_client.llen(self.redis_key) == 0:
                    print("Redis queue is still empty after 1 minute.")
                    # 检查当前任务状态
                    task_status_response = self.get_current_task_status()
                    if task_status_response == "1":
                        print("No active task found, stopping crawler.")
                        self.running = False
                        break
                    else:
                        print("Active task found, continuing to wait for Redis queue.")
                else:
                    self.logger.info("Redis queue is no longer empty, resuming crawl.")

    # ---------- 对外同步接口 ----------
    def start(self):
        """保持原接口名，内部驱动异步"""
        self.logger.info("[FullTextCrawler] 启动异步爬虫...")
        try:
            asyncio.run(self._async_main())
        except KeyboardInterrupt:
            self.logger.info("收到 Ctrl-C，优雅退出...")
        finally:
            self.db_manager.close_all()
            self.logger.info("[FullTextCrawler] 已停止")

    async def _async_main(self):
        """真正的事件循环"""
        self.logger.info(f"启动异步爬虫，并发数: {self.concurrency}, QPS限制: {self.rate_limit}")
        # 在启动 worker 之前等待 30 秒
        self.logger.info("爬虫将在 30 秒后启动...")
        await asyncio.sleep(30)
        self.logger.info("爬虫启动中...")

        async with AsyncHttpClient(self.ua_manager, self.rate_limiter) as http:
            workers = [asyncio.create_task(self._async_worker(http)) for _ in range(self.concurrency)]
            await asyncio.gather(*workers)

    # 兼容旧 stop
    def stop(self):
        self.running = False

    def get_current_task_status(self):


        try:
            import requests

            status_url = 'http://127.0.0.1:5000/api/tasks/poststatus'
         
            response = requests.get(status_url)

            if response.status_code == 200:
                result = response.text
                import json
                result_json = json.loads(result)
                current_task = result_json['result_summary']
                return current_task


            else:
                print(f"获取任务状态失败: {response.text}")
                return None

        except Exception as e:
            print(f"请求发生异常: {str(e)}")
            return None
