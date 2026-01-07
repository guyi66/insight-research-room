#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主控制器
整合帖子爬取和评论爬取功能
"""

import configparser
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, List
import os
import sys

# 导入项目模块
from enhanced_comment_crawler import EnhancedCommentCrawler
from database_client import DatabaseManager
from comment_date_manager import CommentDateManager

# 导入现有的帖子爬取模块
try:
    from main_class import guba_comments
except ImportError:
    try:
        from simple_main import guba_comments
    except ImportError:
        print("警告: 无法导入帖子爬取模块，请检查 main_class.py 或 simple_main.py 是否存在")
        guba_comments = None


class MainController:
    """主控制器类"""
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
        
        # 数据库管理器
        self.db_manager = DatabaseManager(config_path)
        
        # 日期管理器
        self.date_manager = CommentDateManager(config_path)
        
        # 配置参数
        self.stock_symbol = self.config.get('mainClass', 'seccode', fallback='000001')
        self.start_page = self.config.getint('mainClass', 'pages_start', fallback=1)
        self.end_page = self.config.getint('mainClass', 'pages_end', fallback=10)
        self.thread_num = self.config.getint('mainClass', 'thread_num', fallback=4)
        self.save_to_mongo = self.config.getboolean('mainClass', 'save_to_mongo', fallback=True)
        self.save_to_csv = self.config.getboolean('mainClass', 'save_to_csv', fallback=False)
        self.add_to_redis = self.config.getboolean('mainClass', 'add_to_redis', fallback=True)
        self.collectionName = self.config.get('mainClass','collectionname',fallback=self.stock_symbol)
        
        # 评论爬取配置
        self.crawl_comments_enabled = self.config.getboolean('CommentCrawler', 'enabled', fallback=True)
        self.comment_start_date = self.config.get('CommentCrawler', 'start_date', fallback='')
        self.comment_end_date = self.config.get('CommentCrawler', 'end_date', fallback='')
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        # """设置日志"""
        # log_level = self.config.get('logging', 'log_level', fallback='INFO')
        # log_file = "/opt/module/guba-crawler/" + self.config.get('logging', 'log_file', fallback='main_controller.log')
        
        # print("这是读取到的log！"+log_file)
        
        # logging.basicConfig(
        #     level=getattr(logging, log_level.upper()),
        #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        #     handlers=[
        #         logging.FileHandler(log_file, encoding='utf-8'),
        #         logging.StreamHandler()
        #     ]
        # )
        # self.logger = logging.getLogger(__name__)

        log_level = self.config.get('logging', 'log_level', fallback='INFO')
        log_file = "/opt/module/guba-crawler/" + self.config.get('logging', 'log_file', fallback='main_controller.log')
        print("这是读取到的log！"+log_file)
        # 获取独立logger实例
        self.logger = logging.getLogger(self.config.get('logging', 'log_file', fallback='main_controller.log'))  # 唯一标识符
        self.logger.setLevel(log_level.upper())
        
        # 清除旧处理器
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # 创建新处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        console_handler = logging.StreamHandler()
        
        # 设置格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def crawl_posts(self) -> bool:
        """爬取帖子信息"""
        if guba_comments is None:
            self.logger.error("帖子爬取模块未找到，跳过帖子爬取")
            return False
        
        try:
            self.logger.info(f"开始爬取股票 {self.stock_symbol} 的帖子信息")
            self.logger.info(f"爬取范围: 第 {self.start_page} 页到第 {self.end_page} 页")
            
            # 读取全文爬取配置
            full_text_enabled = self.config.getboolean('PostCrawler', 'full_text', fallback=False)

            post_crawler = guba_comments(
                config_path=self.config_path ,
                config=self.config,
                secCode=self.stock_symbol,
                pages_start=self.start_page,
                pages_end=self.end_page,
                num_start = 0,
                MongoDB=True,
                collectionName=self.stock_symbol,
                full_text=full_text_enabled
            )
            
            # 执行爬取
            post_crawler.main()
            
            self.logger.info("帖子信息爬取完成")
            return True
            
        except Exception as e:
            self.logger.error(f"帖子爬取失败: {e}")
            return False
    
    def crawl_comments(self) -> bool:
        """爬取评论信息"""
        if not self.crawl_comments_enabled:
            self.logger.info("评论爬取功能已禁用")
            return True
        
        try:
            self.logger.info(f"开始爬取股票 {self.stock_symbol} 的评论信息")
            
            # 创建评论爬虫实例
            comment_crawler = EnhancedCommentCrawler(
                stock_symbol=self.stock_symbol,
                config_path=self.config_path
            )
            
            # 设置日期范围
            # if not self.comment_start_date or not self.comment_end_date:
                # 自动获取需要爬取评论的日期范围
                # auto_start_date, auto_end_date = self.date_manager.get_auto_date_range(self.stock_symbol)
                
            #     if auto_start_date and auto_end_date:
            #         start_date_str = auto_start_date
            #         end_date_str = auto_end_date
                    
            #         # 更新配置文件
            #         self.date_manager.update_config_dates(start_date_str, end_date_str, self.config_path)
            #         self.logger.info(f"自动设置评论爬取日期范围: {start_date_str} 到 {end_date_str}")
            #     else:
            #         # 如果没有找到需要爬取的帖子，使用最近7天
            #         end_date = datetime.now()
            #         start_date = end_date - timedelta(days=7)
            #         start_date_str = start_date.strftime('%Y-%m-%d')
            #         end_date_str = end_date.strftime('%Y-%m-%d')
            #         self.logger.info(f"未找到需要爬取评论的帖子，使用默认日期范围: {start_date_str} 到 {end_date_str}")
            # else:
            #     start_date_str = self.comment_start_date
            #     end_date_str = self.comment_end_date
            
            # self.logger.info(f"评论爬取日期范围: {start_date_str} 到 {end_date_str}")
            
            # 查找符合条件的帖子
            comment_crawler.find_all_post(
                db_name=self.collectionName
            )
            
            # 爬取评论
            comment_crawler.run(self.collectionName)
            
            self.logger.info("评论信息爬取完成")
            return True
            
        except Exception as e:
            self.logger.error(f"评论爬取失败: {e}")
            return False
    
    def crawl_full_text(self) -> bool:
        """爬取帖子全文内容"""
        try:
            # 检查是否启用了全文爬取
            full_text_enabled = self.config.getboolean('PostCrawler', 'full_text', fallback=False)
            if not full_text_enabled:
                self.logger.info("全文爬取功能已禁用")
                return True
            
            self.logger.info(f"开始爬取股票 {self.stock_symbol} 的帖子全文")
            
            # 导入全文爬虫
            from full_text_CrawlerAsync import FullTextCrawler
            
            # 创建全文爬虫实例，使用与帖子爬取相同的集合名称
            # 这样全文数据会更新到原有的帖子数据中，而不是创建新集合
            full_text_crawler = FullTextCrawler(
                config_path=self.config_path,
                db_name='guba',
                collection_name=self.collectionName  # 使用股票代码作为集合名称，与帖子爬取保持一致
            )
            
            # 执行全文爬取
            full_text_crawler.start()
           
            
            self.logger.info("帖子全文爬取完成")
            try:
                # self.logger.shutdown() #清除logging
                #清除logging
                logging.shutdown()
            except Exception as e:
                self.logger.error(f"清除logging失败: {e}")
            return True
            
        except Exception as e:
            self.logger.error(f"全文爬取失败: {e}")
            return False
    
    def run_sequential(self) -> None:
        """顺序执行爬取任务"""
        self.logger.info("开始执行爬取任务（顺序模式）")
        start_time = time.time()
        
        try:
            # 1. 爬取帖子信息
            post_success = self.crawl_posts()
            
            # 2. 等待一段时间，确保数据已保存
            if post_success:
                self.logger.info("等待帖子数据保存完成...")
                time.sleep(10)
            
            # 3. 爬取帖子全文
            full_text_success = self.crawl_full_text()
            
            # 4. 爬取评论信息
            comment_success = self.crawl_comments()
            
            # 统计结果
            end_time = time.time()
            total_time = end_time - start_time
            
            self.logger.info(f"爬取任务完成，总耗时: {total_time:.2f} 秒")
            self.logger.info(f"帖子爬取: {'成功' if post_success else '失败'}")
            self.logger.info(f"全文爬取: {'成功' if full_text_success else '失败'}")
            self.logger.info(f"评论爬取: {'成功' if comment_success else '失败'}")
            
        except Exception as e:
            self.logger.error(f"执行爬取任务时发生错误: {e}")
        finally:
            self.db_manager.close_all()
            self.date_manager.close()
    
    def run_parallel(self) -> None:
        """并行执行爬取任务"""
        self.logger.info("开始执行爬取任务（并行模式）")
        start_time = time.time()
        
        try:
            # 创建线程
            post_thread = threading.Thread(target=self.crawl_posts, name="PostCrawler")
            
            # 启动帖子爬取线程
            post_thread.start()
            
            # 等待帖子爬取完成
            post_thread.join()
            
            # 等待一段时间确保数据保存
            time.sleep(10)
            
            # 启动评论爬取
            comment_thread = threading.Thread(target=self.crawl_comments, name="CommentCrawler")
            comment_thread.start()
            comment_thread.join()
            
            # 统计结果
            end_time = time.time()
            total_time = end_time - start_time
            
            self.logger.info(f"并行爬取任务完成，总耗时: {total_time:.2f} 秒")
            
        except Exception as e:
            self.logger.error(f"并行执行爬取任务时发生错误: {e}")
        finally:
            self.db_manager.close_all()
            self.date_manager.close()
    
    def run(self, mode: str = 'sequential') -> None:
        """运行爬取任务
        
        Args:
            mode: 运行模式，'sequential' 或 'parallel'
        """
        self.logger.info(f"启动主控制器，股票代码: {self.stock_symbol}")
        
        if mode == 'parallel':
            self.run_parallel()
        else:
            self.run_sequential()
     
    def get_status(self):
        """获取执行状态"""
        # 这里应该返回实际的执行状态
        # 目前返回模拟数据
        return {
            'success_count': 100,
            'failed_count': 0,
            'total_count': 100
        }


# def main():
#     """主函数"""
#     import argparse
    
#     parser = argparse.ArgumentParser(description='东方财富股吧爬虫主控制器')
#     parser.add_argument('--config', default='config.ini', help='配置文件路径')
#     parser.add_argument('--mode', choices=['sequential', 'parallel'], default='sequential', help='运行模式')
#     parser.add_argument('--status', action='store_true', help='查看爬取状态')
    
#     args = parser.parse_args()
    
#     # 检查配置文件是否存在
#     if not os.path.exists(args.config):
#         print(f"错误: 配置文件 {args.config} 不存在")
#         sys.exit(1)
    
#     # 创建主控制器
#     controller = MainController(args.config)
    
#     if args.status:
#         # 查看状态
#         status = controller.get_status()
#         print("\n=== 爬取状态 ===")
#         for key, value in status.items():
#             print(f"{key}: {value}")
#     else:
#         # 运行爬取任务
#         controller.run(args.mode)


# if __name__ == '__main__':
#     main()