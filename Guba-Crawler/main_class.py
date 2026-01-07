# -*- coding: utf-8 -*-
# @Time    : 2023/2/11 21:27
# @Author  : Euclid-Jie
# @File    : main_class.py
import os
import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
# from retrying import retry
from typing import Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
from db.guba_mysql_adapter import GubaMySQLAdapter
try:
     from Utils.MongoClient import MongoClient
except Exception:
     MongoClient = None
try:
    from Utils.EuclidDataTools import CsvClient
except Exception:
    CsvClient = None
try:
    from TreadCrawler import RedisClient
except Exception:
    RedisClient = None
import configparser
from database_client import DatabaseManager
from user_agent_manager import get_user_agent_manager
import time
import random
from tenacity import retry,retry_if_exception_type,stop_after_attempt,wait_exponential
from pymongo.errors import DuplicateKeyError, BulkWriteError
# 定义自定义异常类
class NetworkException(Exception): pass
class ServerException(Exception): pass
class ContentChangedException(Exception): pass


class guba_comments:
    """
    this class is designed for get hot comments for guba, have two method which can be set at def get_data()
    1、all: https://guba.eastmoney.com/list,600519_1.html, secCode: 600519, page: 1
    2、hot: https://guba.eastmoney.com/list,600519,99_1.html secCode: 600519, page: 1

    because to the ip control, this need to set proxies pools
    by using proxies https://www.kuaidaili.com/usercenter/overview/, can solve this problem

    Program characteristics:
        1、default write data to mongoDB, by init "MogoDB=False", can switch to write data to csv file
        2、Use retry mechanism, once rise error, the program will restart at the least page and num (each page has 80 num)

    """

    failed_proxies = {}
    proxy_fail_times_treshold = 3

    def __init__(
        self,
        config_path:str,
        config: configparser.ConfigParser,
        secCode,
        pages_start: int = 0,
        pages_end: int = 100,
        num_start: int = 0,
        MongoDB: bool = True,
        collectionName : str = 'default',
        full_text: bool = False,
    ):
        self.config_path = config_path
        self.config = config
        self.db_manager = DatabaseManager(self.config_path)
        # # param init
        # if isinstance(secCode, int):
        #     # 补齐6位数
        #     self.secCode = str(secCode).zfill(6)
        # elif isinstance(secCode, str):
            # self.secCode = secCode
        self.pages_start = pages_start
        self.pages_end = pages_end
        self.num_start = num_start
        self.full_text = full_text
        self._year = pd.Timestamp.now().year

        # redis client for full_text_Crawler
        self.redis_client = self.db_manager.get_redis_client()

        # rewrite the secCode setting
        if config.has_option("mainClass", "secCode"):
            self.secCode = config.get("mainClass", "secCode")
            print(
                f"secCode has been overridden by {self.secCode} in the configuration file."
            )
        if config.has_option("mainClass", "pages_start"):
            self.pages_start = int(config.get("mainClass", "pages_start"))
            print(
                f"pages_start has been overridden by {self.pages_start} in the configuration file."
            )
        if config.has_option("mainClass", "pages_end"):
            self.pages_end = int(config.get("mainClass", "pages_end"))
            print(
                f"pages_end has been overridden by {self.pages_end} in the configuration file."
            )
        if config.has_option("mainClass", "collectionname"):
            collectionName = config.get("mainClass", "collectionname")
            print(
                f"collectionName has been overridden by {collectionName} in the configuration file."
            )

        # choose one save method, default MongoDB
        # 1、csv
        # 2、MongoDB
        # 帖子数据存储到guba库
        # self.log_path = config.get("logging", "log_file")
        
        # print("这是读取到标题爬取的log"+self.log_path)
        collectionName = collectionName if collectionName else self.secCode
        if MongoDB and MongoClient is not None:
            self.col = MongoClient("guba", collectionName)
        else:
            self.col = None

        # log setting
        # log_format = "%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s"
        # logging.basicConfig(filename=self.log_path, format=log_format, level=logging.INFO,encoding = 'utf-8')
        
        self.logger = logging.getLogger(config.get('logging', 'log_file', fallback='main_controller.log'))
        
        # User-Agent管理器初始化
        self.ua_manager = get_user_agent_manager('random')  # 使用随机模式
        
        # header setting - 使用动态User-Agent
        self.header = {
            "User-Agent": self.ua_manager.get_user_agent(),
        }
        
        # proxies setting (default: disabled unless configured)
        self.proxies = None
        self.backup_proxies = None
        self.use_backup_proxy = False
        tunnel = ""
        backup_tunnel = ""
        username = ""
        password = ""
        enabled = False
        if config and config.has_section("proxies"):
            tunnel = config.get("proxies", "tunnel", fallback="").strip()
            backup_tunnel = config.get("proxies", "backup_tunnel", fallback="").strip()
            username = config.get("proxies", "username", fallback="").strip()
            password = config.get("proxies", "password", fallback="").strip()
            enabled_raw = config.get("proxies", "enabled", fallback="").strip().lower()
            if enabled_raw in ("1", "true", "yes", "y", "on"):
                enabled = True
        if not enabled:
            enabled = bool(tunnel)

        if enabled and tunnel:
            auth = f"{username}:{password}@" if username and password else ""
            proxy_url = f"http://{auth}{tunnel}/"
            self.proxies = {"http": proxy_url, "https": proxy_url}
            if backup_tunnel:
                backup_url = f"http://{auth}{backup_tunnel}/"
                self.backup_proxies = {"http": backup_url, "https": backup_url}

    def _change_proxy_ip(self):
        """调用快代理更换隧道IP接口"""
        if not self.proxies:
            return False
        try:
            import requests
            change_url = "https://tps.kdlapi.com/api/changetpsip"
            params = {
                "secret_id": "oqifdb1h1ykoxm8comcv",
                "signature": "vhp8ervln42dkh85ht3ijw6adctj1wah"
            }
            
            response = requests.get(change_url, params=params, timeout=3)
            if response.status_code == 200:
                self.logger.info("隧道IP更换成功")
                return True
            else:
                self.logger.error(f"更换隧道IP失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"更换隧道IP时发生异常: {e}")
            return False

    @staticmethod
    def clear_str(str_raw):
        for pat in ["\n", " ", " ", "\r", "\xa0", "\n\r\n"]:
            str_raw.strip(pat).replace(pat, "")
        return str_raw

    @staticmethod
    def run_thread_pool_sub(target, args, max_work_count):
        with ThreadPoolExecutor(max_workers=max_work_count) as t:
            res = [t.submit(target, i) for i in args]
            return res

    # @retry(stop_max_attempt_number=5, wait_fixed=2000)  # 最多尝试5次，每次间隔2秒
    def get_soup_form_url(self, url: str) -> BeautifulSoup:
        """
        get the html content used by requests.get
        :param url:
        :return: BeautifulSoup
        """
        current_headers = {
            "User-Agent": self.ua_manager.get_user_agent(),
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
        
        proxies = self.backup_proxies if (self.use_backup_proxy and self.backup_proxies) else self.proxies
        response = requests.get(
            url,
            headers=current_headers,
            timeout=10,
            proxies=proxies,
        )

        if response.status_code != 200:
            self.logger.error(f"请求失败，状态码: {response.status_code}，准备重试...")
            
            return None
        html = response.content.decode("utf-8", "ignore")
        #判断html是否为空或少于30行
        if len(html) < 79:
            # raise Exception("获取页面内容失败，准备重试")
            return None
        if not html:
            # raise Exception("获取页面内容失败，准备重试")
            return None
        # 检查是否包含预期的关键内容
        if "listitem" not in html:
            self.logger.warning(f"页面不包含预期内容，可能被重定向或阻止，URL: {url}")
            # 页面内容不符合预期时更换隧道IP
            self._change_proxy_ip()
            return None
        # 检查是否包含预期的关键内容
        soup = BeautifulSoup(html, features="lxml")
        return soup

    def get_data_json(self, item):
        """
        get the special keys from item, in this the project,
        the keys con be "阅读"、"评论"、……

        by use the get_full_text, the return json data will contain full_text
        :param item:
        :return: json data contains full_text
        """

        tds = item.find_all("td")
        # if len(tds) < 5:
        #     self.logger.warning(f"td 数量不足，跳过该条目: {tds[2].a.text}")
        if len(tds) < 5:
            self.logger.warning(f"td 数量不足: {len(tds)}，跳过该条目")
            return None
        try:
            data_json = {
                "阅读": tds[0].text,
                "评论": tds[1].text,
                "标题": tds[2].a.text,
                "href": tds[2].a["href"],
                "作者": tds[3].a.text,
                "最后更新": tds[4].text,
            }
            data_json["secCode"] = self.secCode
        except Exception as e:
            self.logger.error(f"解析数据失败: {e}")
            return None


        return data_json


    def _build_list_url(self, page: int) -> str:
        mode = ""
        if self.config and self.config.has_option("mainClass", "mode"):
            mode = self.config.get("mainClass", "mode", fallback="").strip().lower()
        template = ""
        if self.config and self.config.has_option("mainClass", "list_url_template"):
            template = self.config.get("mainClass", "list_url_template", fallback="").strip()
        if template:
            return template.format(code=self.secCode, page=page)
        if mode in ("hot", "99"):
            return f"https://guba.eastmoney.com/list,{self.secCode},99_{page}.html"
        if mode in ("f", "focus", "featured"):
            return f"https://guba.eastmoney.com/list,{self.secCode},f_{page}.html"
        return f"https://guba.eastmoney.com/list,{self.secCode}_{page}.html"


    @retry(
    retry=(
        retry_if_exception_type( NetworkException) |
        retry_if_exception_type(ServerException)
        # retry_if_exception(lambda e: isinstance(e, ContentChangedException))
    ),
    stop=stop_after_attempt(5) ,
    wait=wait_exponential(multiplier=1, min=2, max=4)
)
    def get_data(self, page):
        Url = self._build_list_url(page)
        data_list = None
        
        try:
            self.logger.info(f"开始处理页面 {Url}")
            
            soup = self.get_soup_form_url(Url)
            if soup is None:
                # 获取页面失败时更换隧道IP
                self._change_proxy_ip()
                raise NetworkException("获取页面失败")
            data_list = soup.find_all("tr", "listitem")
            if len(data_list) == 0 or len(data_list)<=20:
                self.logger.warning(f"页面 {page} 未找到任何数据项，可能页面为空或结构变化")
            # 检查页面是否包含预期内容
                page_text = soup.get_text()
                if "验证" in page_text or "captcha" in page_text.lower():
                    # 遇到验证页面时更换隧道IP
                    self._change_proxy_ip()
                    raise NetworkException("遇到验证页面，需要重试")
                elif len(page_text) < 100:
                    # 页面内容过少时更换隧道IP
                    self._change_proxy_ip()
                    raise NetworkException("页面内容过少，可能请求失败")
                else:
                    self.logger.info(f"页面 {page} 确实没有数据项，重试")
                    # 页面没有数据项时更换隧道IP
                    self._change_proxy_ip()
                    raise NetworkException("页面没有数据项，重试")
        except Exception as e:
            time.sleep(random.uniform(0.5, 3))
            self.logger.error(f"soup数据转data_list失败: {e}")
            raise NetworkException(e)
        success_count = 0
        is_retry = False
        for item in data_list:
            try:
                data_json = self.get_data_json(item)
                if data_json:
                    try:
                        exist_count = 0
        
                        	
                            
                        try:
                            backend = self.config.get("mainClass", "storage_backend", fallback="mysql").lower()
                            if backend == "mysql":
                                self.col = GubaMySQLAdapter(config_path=self.config_path)
                            elif backend == "mongo":
                                if MongoClient is None:
                                    raise RuntimeError("MongoClient 不可用，但 storage_backend=mongo")
                                self.col = MongoClient("guba", collectionName)
                            else:
                                raise ValueError(f"Unknown storage_backend: {backend}")
                            if hasattr(self.col, "insert_many"):
                                self.col.insert_many([data_json])
                            elif hasattr(self.col, "insert_one"):
                                self.col.insert_one(data_json)
                            else:
                                raise RuntimeError("Storage backend does not support insert")
                            self.logger.info(f"写入数据成功: {data_json['href']}")
                        except DuplicateKeyError as e:
                            if self.secCode not in  data_json['href']  and "zssh000001" in  data_json['href']:
                                self.logger.error(f"可能被重定向: {data_json['href']}")
                                raise ContentChangedException("内容重复")
                                is_retry = True
                            else:
                                self.logger.error(f"重复: {e}")
                                continue
                        except BulkWriteError as e:
                            # 检查是否是重复键错误
                            if self.secCode not in  data_json['href']  and "zssh000001" in  data_json['href']:
                                self.logger.error(f"可能被重定向: {data_json['href']}")
                                is_retry = True
                                raise ContentChangedException("内容重复")
                            if e.details.get('writeErrors') and any(err.get('code') == 11000 for err in e.details['writeErrors']):
                                self.logger.error(f"重复数据: {data_json['href']}")
                                exist_count += 1
                                continue
                            else:
                                self.logger.error(f"批量写入失败: {e}")
                                continue
                        except Exception as e:
                            self.logger.error(
                                f"写入数据失败: {e}"
                            )
                            continue
                        try:
                            self.redis_client.add_url(data_json["href"])
                        except Exception as e:
                            self.logger.error(
                                "写入redis数据失败:{}".format(data_json["标题"]),
                                extra={"data": data_json},
                            )
                        success_count += 1
                    except Exception as e:
                        self.logger.error(f"插入数据失败: {e}, href: {data_json.get('href', '无href')}")
                    self.t.set_postfix({"状态": "已写num:{}".format(self.num_start)})
                    self.num_start += 1
                else:
                    self.logger.warning("data_json 为空，跳过插入")
            except Exception as e:
                self.logger.error(f"处理单个 item 失败: {e}")
        if is_retry == True:
            # 页面没有新数据，准备重试
            self.logger.warning(f"页面 {page} 没有新数据，准备重试")
            #抛出 ContentChangedException 错误
            raise ContentChangedException("内容重复")
        
        self.logger.info(f"页面 {page} 处理完成，成功插入 {success_count}/{len(data_list)} 条数据")
        return exist_count

 
    #检查索引存在性
    def index_exists_by_name(self, collection, index_name):
        return index_name in collection.index_information()
                
    def main(self):
        
        #创建索引 
        try:
            if not self.index_exists_by_name(self.col, "href_index"):
                self.logger.info("创建索引")
                self.col.create_index("href", unique=True,name = "href_index")
        except Exception as e:
            self.logger.error(f"创建索引失败: {e}")
            pass
        with tqdm(range(self.pages_start, self.pages_end)) as self.t:
            for page in self.t:
                self.t.set_description("page:{}".format(page))
                
                # 手动实现页面级重试
                max_retries = 10
                retry_count = 0
                page_success = False
                
                while retry_count < max_retries and not page_success:
                    try:
                        time.sleep(random.uniform(0,1)) 
                        exist_count = self.get_data(page)  # get_data 保留自己的 @retry
                        page_success = True
                    except ContentChangedException as e:
                        self.logger.warning(f"页面 {page} 被重定向，重试")
                        retry_count += 1
                        # 调用kuaidaili API更换隧道IP
                        self._change_proxy_ip()
                        time.sleep(random.uniform(0, 3)) # 添加等待时间，模拟重新建立连接
                        self.logger.warning(f"检测到重定向，切换到备用代理并重新连接: {page}")
                        self.use_backup_proxy = not self.use_backup_proxy # 切换代理状态
                    except Exception as e:
                        retry_count += 1
                        self.logger.error(f"页面 {page} 处理失败 (第{retry_count}次重试): {e}")
                        if retry_count < max_retries:
                            time.sleep(random.uniform(0,1))  # 重试间隔
                        else:
                            self.logger.error(f"页面 {page} 重试 {max_retries} 次后仍然失败，跳过该页面")
                
                #切换回来
                self.use_backup_proxy = not self.use_backup_proxy # 切换代理状态
                        
                
                time.sleep(random.uniform(0, 1))
                self.num_start = 0
