#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫任务调度系统 - 后端API服务
提供任务配置、队列管理、执行监控等RESTful接口
"""

import os
import json
import uuid
import tempfile
import subprocess
import threading
import time
import jwt
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from functools import wraps
import pymysql
import configparser
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

# 导入现有的爬虫模块
from main_controller import MainController
from full_text_CrawlerAsync import FullTextCrawler
from database_client import DatabaseManager as db_manager2

import logging

import multiprocessing
import smtplib
import email.utils
from email.mime.text import MIMEText


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, host='8.130.43.32', port=3337, user='root', password='aa20020320', database='crawler'):
        self.connection_config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'autocommit': True
        }
    
    def get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.connection_config)

    
    
    def execute_query(self, query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = True):
        """执行查询"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query, params)
                if fetch_one:
                    return cursor.fetchone()
                elif fetch_all:
                    return cursor.fetchall()
                else:
                    return cursor.rowcount
    
    def execute_insert(self, query: str, params: tuple = None):
        """执行插入操作"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.lastrowid


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.running = False
        self.current_task = None
        self.scheduler_thread = None
        
    def start(self):
        """启动调度器"""
        if not self.running:
            self.running = True
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()
            print("[TaskScheduler] 任务调度器已启动")
    
    def stop(self):
        """停止调度器"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        print("[TaskScheduler] 任务调度器已停止")

    

    def kill_existing_fulltext_processes(self):
        """杀死所有包含'full'的进程"""
        try:
            # 执行 shell 命令
            subprocess.run(
                "ps ux | grep full | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null || true",
                shell=True,
                check=True
            )
            print("[INFO] 已清除旧的全文爬虫进程")
        except Exception as e:
            print(f"[ERROR] 清理旧进程时发生错误: {e}")
    
    def _scheduler_loop(self):
        """调度器主循环"""
        while self.running:
            try:
                # 检查是否有待执行的任务
                if not self.current_task:
                    next_task = self._get_next_task()
                    if next_task:
                        self._execute_task(next_task)
                
                # 检查当前任务状态
                if self.current_task:
                    self._check_current_task()
                
                time.sleep(5)  # 每5秒检查一次
                
            except Exception as e:
                print(f"[TaskScheduler] 调度器错误: {e}")
                time.sleep(10)

    def send_email(self,config_path):
        try:
            #用configparser读取配置文件
            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')
            #  获取集合名称
            collection_name = config.get('mainClass', 'collectionname')
            secCode = config.get('mainClass', 'seccode')
            #查询当前集合内容数量
            db_mongo = db_manager2(config_path)
            mongo = db_mongo.get_mongo_client('guba',collection_name)
            count = mongo.count_documents({})
            duration = (datetime.now() - self.current_task['start_time']).total_seconds()
            #秒数转为小时数
            duration = int(duration // 3600)

            """获取下一个待执行任务"""
            query = """
            SELECT tc.task_name
            FROM task_queue tq
            JOIN task_configs tc ON tq.task_config_id = tc.id
            WHERE tq.status = 'pending' AND tc.is_active = 1
            ORDER BY tq.queue_position ASC, tq.id ASC
            LIMIT 1
            """
            # 执行查询获取结果
            result = self.db_manager.execute_query(query, fetch_one=True)
            # 结果处理逻辑
            if result:  # 存在待处理任务
                task_name = result['task_name']  # 提取任务名称

            else:  # 无待处理任务
                task_name  = "队列无任务"

        
            #根据传入的变量编写邮件内容
            content = f"任务完成！\n股票代码：{secCode}\n集合名称：{collection_name}\n开始时间：{self.current_task['start_time']}\n结束时间：{datetime.now()}\n执行时长：{duration}小时\n当前集合内数量：{count}\n下一队列：{task_name}"
        except Exception as e:
            content = f"邮件编辑失败：{e}"
        # 创建MIMEText对象，设置邮件内容
        message = MIMEText(content)
        
        # 设置收件人和发件人信息
        message['To'] = email.utils.formataddr(('shenghao', '1098966585@qq.com'))
        message['From'] = email.utils.formataddr(('爬虫运行通知', '1098966585@qq.com'))
        
        # 设置邮件主题
        message['Subject'] = '程序运行完成通知'

        # 连接到QQ邮箱的SMTP服务器
        server = smtplib.SMTP_SSL('smtp.qq.com', 465)
        
        # 使用邮箱和授权码登录
        server.login('1098966585@qq.com', 'jyxxabufjviejiia')
        
        try:
            # 发送邮件
            server.sendmail('1098966585@qq.com', ['1098966585@qq.com'], message.as_string())
            server.quit()
            print("邮件发送成功")
        except Exception as e:
            # 异常处理
            print("邮件发送失败:", e)
    
    def _get_next_task(self):
        """获取下一个待执行任务"""
        query = """
        SELECT tq.*, tc.task_name, tc.config_data, tc.crawler_type
        FROM task_queue tq
        JOIN task_configs tc ON tq.task_config_id = tc.id
        WHERE tq.status = 'pending' AND tc.is_active = 1
        ORDER BY tq.queue_position ASC, tq.id ASC
        LIMIT 1
        """
        return self.db_manager.execute_query(query, fetch_one=True)
    
    def _execute_task(self, task):
        """执行任务"""
        task_id = task['id']
        execution_id = str(uuid.uuid4())
        
        try:
            # 更新任务状态为运行中
            self._update_task_status(task_id, 'running', {
                'started_at': datetime.now(),
                'current_step': '正在初始化...',
                'progress_percent': 0
            })
            
            self.current_task = {
                'task_id': task_id,
                'execution_id': execution_id,
                'task_data': task,
                'start_time': datetime.now(),
                'process': None
            }
            
            # 根据爬虫类型执行不同的任务
            crawler_type = task['crawler_type']
            config_data = json.loads(task['config_data'])
            
            if crawler_type == 'post':
                self._execute_post_task(task, config_data, execution_id)
            elif crawler_type == 'comment':
                self._execute_comment_task(task, config_data, execution_id)
            elif crawler_type == 'sequential':
                self._execute_sequential_task(task, config_data, execution_id)
            elif crawler_type == 'parallel':
                self._execute_parallel_task(task, config_data, execution_id)
            elif crawler_type == 'full_text':
                self._execute_full_text_task(task, config_data, execution_id)
            else:
                raise ValueError(f"不支持的爬虫类型: {crawler_type}")
                
        except Exception as e:
            self._handle_task_error(task_id, execution_id, str(e))
    
    def _execute_post_task(self, task, config_data, execution_id):
        """执行标题爬取任务"""
        # 创建临时配置文件
        temp_config_path = f"temp_config_{execution_id}.ini"
        
        # 创建任务专用日志文件
        task_id = task['id']
        log_filename = f"task_{task_id}_{task['task_name']}_{task['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 确保logs目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 修改配置数据，添加日志文件路径
        config_data_with_log = config_data.copy()
        if 'logging' not in config_data_with_log:
            config_data_with_log['logging'] = {}
        config_data_with_log['logging']['log_file'] = log_file_path
        config_data_with_log['logging']['log_level'] = 'INFO'
        
        self._create_temp_config_file(config_data_with_log, temp_config_path, task['task_name'])
        
        try:
            self._execute_post_crawling(task, config_data_with_log, execution_id, log_file_path)
            
            # 获取最终结果
            result = {
                'success_count': 0,
                'failed_count': 0,
                'total_count': 0
            }
            
            # 更新任务完成状态
            self._complete_task(task['id'], execution_id, 'completed', result, log_file_path)
            
        except Exception as e:
            # 记录错误到日志文件
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 任务执行失败: {str(e)}\n")
            self._handle_task_error(task['id'], execution_id, str(e), log_file_path)
            raise
        finally:
            # 清理临时配置文件
            if os.path.exists(temp_config_path):
                os.remove(temp_config_path)
    
    def _execute_comment_task(self, task, config_data, execution_id):
        """执行评论爬取任务"""
        # 创建临时配置文件
        temp_config_path = f"temp_config_{execution_id}.ini"
        
        # 创建任务专用日志文件
        task_id = task['id']
        log_filename = f"task_{task_id}_{task['task_name']}_{task['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 确保logs目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 修改配置数据，添加日志文件路径
        config_data_with_log = config_data.copy()
        if 'logging' not in config_data_with_log:
            config_data_with_log['logging'] = {}
        config_data_with_log['logging']['log_file'] = log_file_path
        config_data_with_log['logging']['log_level'] = 'INFO'
        
        self._create_temp_config_file(config_data_with_log, temp_config_path, task['task_name'])
        
        try:
            self._execute_comment_crawling(task, temp_config_path, execution_id, log_file_path)
            
            # 获取最终结果
            result = {
                'success_count': 0,
                'failed_count': 0,
                'total_count': 0
            }
            
            # 更新任务完成状态
            self._complete_task(task['id'], execution_id, 'completed', result, log_file_path)
            
        except Exception as e:
            # 记录错误到日志文件
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 任务执行失败: {str(e)}\n")
            self._handle_task_error(task['id'], execution_id, str(e), log_file_path)
            raise
        finally:
            # 清理临时配置文件
            if os.path.exists(temp_config_path):
                os.remove(temp_config_path)
    
    def _execute_sequential_task(self, task, config_data, execution_id):
        """执行顺序爬虫任务（三阶段：标题->全文->评论）"""
        # 创建临时配置文件
        temp_config_path = f"temp_config_{execution_id}.ini"
        
        # 创建任务专用日志文件
        task_id = task['id']
        log_filename = f"task_{task_id}_{task['task_name']}_{task['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 确保logs目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 修改配置数据，添加日志文件路径
        config_data_with_log = config_data.copy()
        if 'logging' not in config_data_with_log:
            config_data_with_log['logging'] = {}
        config_data_with_log['logging']['log_file'] = log_file_path
        config_data_with_log['logging']['log_level'] = 'INFO'
        
        self._create_temp_config_file(config_data_with_log, temp_config_path, task['task_name'])

        #读取temp_config_path 打印
        with open(temp_config_path, 'r', encoding='utf-8') as f:
            print(f.read())
        
        try:
            # 阶段1：标题爬取
            self.kill_existing_fulltext_processes()
            self._update_task_status(task_id, 'running', {
                'current_step': '阶段1: 标题爬取中...',
                'progress_percent': 10
            })
            self._execute_post_crawling(task, temp_config_path, execution_id, log_file_path)
            
            # 阶段2：全文爬取
            self._update_task_status(task_id, 'running', {
                'current_step': '阶段2: 全文爬取中...',
                'progress_percent': 40
            })
            self._execute_full_text_crawling(task, temp_config_path, execution_id, log_file_path)
            # str_pid = str(pid)
            # with open(log_file_path, 'a') as f:
            #     f.write(f"全文爬虫进程，PID返回值: {str_pid}\n")
            
            # 阶段3：评论爬取
            self._update_task_status(task_id, 'running', {
                'current_step': '阶段3: 评论爬取中...',
                'progress_percent': 70
            })
            # self._execute_comment_crawling(task, temp_config_path, execution_id, log_file_path)

            #查看redies的数据
            db_redies = db_manager2(temp_config_path)
            redis_client = db_redies.get_redis_client()
            while True:
                
                queue_count = 0
                try:
                    r = redis.Redis(host='localhost', port=6379, password='123456', db=0)
                    queue_count = r.llen('urls')
                except:
                    pass
                
                if  queue_count == 0:
                    break
                


            #关闭全文爬取的pid进程
            # import subprocess
            # subprocess.run(['kill', str_pid], check=True)
            # #向log_file_path中写进程已被杀死 ，使用直接打开插入的方式
            # with open(log_file_path, 'a') as f:
            #     f.write(f"任务完成，已关闭全文爬虫进程，PID: {pid}\n")

            
            # 任务完成
            self._update_task_status(task_id, 'running', {
                'current_step': '任务完成',
                'progress_percent': 100
            })
            
            # 获取最终结果
            result = {
                'success_count': 0,
                'failed_count': 0,
                'total_count': 0
            }
            
            # 更新任务完成状态
            self._complete_task(task['id'], execution_id, 'completed', result, log_file_path)

            with open(log_file_path, 'a') as f:
                f.write(f"{temp_config_path} 临时配置文件已删除\n")

            
        except Exception as e:
            # 记录错误到日志文件
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 任务执行失败: {str(e)}\n")
            self._handle_task_error(task['id'], execution_id, str(e), log_file_path)
            raise
    
    def _execute_parallel_task(self, task, config_data, execution_id):
        """执行并行爬虫任务（三阶段：标题->全文->评论）"""
         # 创建临时配置文件
        temp_config_path = f"temp_config_{execution_id}.ini"
        
        # 创建任务专用日志文件
        task_id = task['id']
        log_filename = f"task_{task_id}_{task['task_name']}_{task['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 确保logs目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 修改配置数据，添加日志文件路径
        config_data_with_log = config_data.copy()
        if 'logging' not in config_data_with_log:
            config_data_with_log['logging'] = {}
        config_data_with_log['logging']['log_file'] = log_file_path
        config_data_with_log['logging']['log_level'] = 'INFO'
        
        self._create_temp_config_file(config_data_with_log, temp_config_path, task['task_name'])

        #读取temp_config_path 打印
        with open(temp_config_path, 'r', encoding='utf-8') as f:
            print(f.read())
        
            
        
        try:
            # 阶段1：标题爬取
            self.kill_existing_fulltext_processes()
            self._update_task_status(task_id, 'running', {
                'current_step': '并行爬取',
                'progress_percent': 50
            })

            post = multiprocessing.Process(target=self._execute_post_crawling, args=(task, temp_config_path, execution_id, log_file_path))
            
            # 阶段2：全文爬取
            full_text = multiprocessing.Process(target=self._execute_full_text_crawling, args=(task, temp_config_path, execution_id, log_file_path))

            post.start()
            full_text.start()

            post.join()
            full_text.join()
            



            #发送邮件
            self.send_email(temp_config_path)
            


            
            # 阶段3：评论爬取
            # self._update_task_status(task_id, 'running', {
            #     'current_step': '爬取完成',
            #     'progress_percent': 100
            # })
            # self._execute_comment_crawling(task, temp_config_path, execution_id, log_file_path)
            
            # 任务完成
            self._update_task_status(task_id, 'running', {
                'current_step': '任务完成',
                'progress_percent': 100
            })
            
            # 获取最终结果
            result = {
                'success_count': 0,
                'failed_count': 0,
                'total_count': 0
            }
            
            # 更新任务完成状态
            self._complete_task(task['id'], execution_id, 'completed', result, log_file_path)
            
        except Exception as e:
            # 记录错误到日志文件
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 任务执行失败: {str(e)}\n")
            self._handle_task_error(task['id'], execution_id, str(e), log_file_path)
            raise
        finally:
            # # 清理临时配置文件
            # if os.path.exists(temp_config_path):
            #     os.remove(temp_config_path)
                        #删除临时配置文件
            try:
                os.remove(temp_config_path)
                with open(log_file_path, 'a', encoding='utf-8') as f:
                    f.write(f"{temp_config_path} 临时配置文件已删除\n")
            except:
                with open(log_file_path, 'a', encoding='utf-8') as f:
                    f.write(f"{temp_config_path} 临时配置删除失败\n")
                pass
    
    def _execute_full_text_task(self, task, config_data, execution_id):
        """执行全文爬虫任务"""
        # 创建临时配置文件
        temp_config_path = f"temp_config_{execution_id}.ini"
        
        # 创建任务专用日志文件
        task_id = task['id']
        log_filename = f"task_{task_id}_{task['task_name']}_{task['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 确保logs目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 修改配置数据，添加日志文件路径
        config_data_with_log = config_data.copy()
        if 'logging' not in config_data_with_log:
            config_data_with_log['logging'] = {}
        config_data_with_log['logging']['log_file'] = log_file_path
        config_data_with_log['logging']['log_level'] = 'INFO'
        
        self._create_temp_config_file(config_data_with_log, temp_config_path, task['task_name'])
        
        try:
            # 获取集合名称和股票代码
            collection_name = config_data.get('mainClass', {}).get('collectionName', 'default')
            stock_symbol = config_data.get('mainClass', {}).get('secCode', '000001')
            
            # 使用股票代码作为集合名称（如果没有明确指定collectionName）
            if collection_name == 'default':
                collection_name = stock_symbol
            
            # 创建全文爬虫实例，使用与帖子爬取相同的集合名称
            # 这样全文数据会更新到原有的帖子数据中，而不是创建新集合
            crawler = FullTextCrawler(
                config_path=temp_config_path,
                db_name='guba',
                collection_name=collection_name  # 使用与帖子爬取相同的集合名称
            )
            
            # 在单独线程中启动爬虫
            import threading
            crawler_thread = threading.Thread(target=crawler.start)
            crawler_thread.daemon = True
            crawler_thread.start()
            
            # 等待爬虫完成（监控爬虫状态）
            max_wait_time = 3600  # 最大等待1小时
            wait_interval = 10    # 每10秒检查一次
            waited_time = 0
            
            while waited_time < max_wait_time:
                # 检查爬虫线程是否还活着
                if not crawler_thread.is_alive():
                    break
                    
                # 更新任务进度
                total_processed = crawler.success_count + crawler.fail_count
                if total_processed > 0:
                    self._update_task_status(task['id'], 'running', {
                        'current_step': f'已处理 {total_processed} 条数据',
                        'progress_percent': min(90, total_processed * 2)  # 简单的进度估算
                    })
                
                time.sleep(wait_interval)
                waited_time += wait_interval
            
            # 如果超时，强制停止爬虫
            if waited_time >= max_wait_time:
                crawler.stop()
                crawler_thread.join(timeout=30)  # 等待30秒让爬虫优雅停止
                raise Exception(f"全文爬虫任务超时（{max_wait_time}秒）")
            
            # 等待爬虫线程完全结束
            crawler_thread.join()
            
            # 更新任务完成状态
            result = {
                'success_count': crawler.success_count,
                'fail_count': crawler.fail_count,
                'total_processed': crawler.success_count + crawler.fail_count
            }
            
            self._complete_task(task['id'], execution_id, 'completed', result, log_file_path)
            
        except Exception as e:
            # 记录错误到日志文件
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 全文爬虫任务执行失败: {str(e)}\n")
            self._handle_task_error(task['id'], execution_id, str(e), log_file_path)
            raise
        finally:
            # 清理临时配置文件
            if os.path.exists(temp_config_path):
                os.remove(temp_config_path)
    
    def _execute_post_crawling(self, task, config_data, execution_id, log_file_path):
        """执行标题爬取"""
        try:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[INFO] {datetime.now().isoformat()} - 开始标题爬取\n")
            
            # 导入MainController
            from main_controller import MainController
            
            # 创建临时配置文件
            temp_config_path = config_data
            
            print('标题爬取config'+temp_config_path)
            
            # 创建控制器实例
            controller = MainController(config_path=temp_config_path)
            
            # 执行标题爬取
            res = controller.crawl_posts()
            if res == True:
                with open(log_file_path, 'a', encoding='utf-8') as f:
                    f.write(f"\n[INFO] {datetime.now().isoformat()} - 标题爬取完成\n")
                    
            #发送完成信号
            try:
                task_id = self.current_task['task_id']
                query = f"UPDATE task_queue SET result_summary=1 WHERE id = {task_id}"
                self.db_manager.execute_query(query, fetch_all=False)
                with open(log_file_path, 'a', encoding='utf-8') as f:
                    f.write(f"\n[ERROR] {datetime.now().isoformat()} - 更新任务状态")
            except Exception as e:
                with open(log_file_path, 'a', encoding='utf-8') as f:
                    f.write(f"\n[ERROR] {datetime.now().isoformat()} - 更新任务状态失败{e}")
            
            
            
            
            
            return res
                
                
        except Exception as e:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 标题爬取失败: {str(e)}\n")
            raise
    
    def _execute_full_text_crawling(self, task, config_data, execution_id, log_file_path):
        """执行全文爬取"""
        import subprocess
        import sys
        
        try:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[INFO] {datetime.now().isoformat()} - 开始全文爬取\n")

            # 导入MainController
            from main_controller import MainController
            
            
            # 创建控制器实例
            controller = MainController(config_path=config_data)
            controller.crawl_full_text()
#             #读取config_data 并加载配置
#             config = configparser.ConfigParser()
#             config.read(config_data, encoding='utf-8')

            
#             collection_name = config['mainClass']['collectionName']
#             stock_symbol = config['mainClass']['secCode']
            
#             # 使用股票代码作为集合名称（如果没有明确指定collectionName）
#             if collection_name == 'default':
#                 collection_name = stock_symbol
            
#             # 构建命令行参数
#             cmd = [
#                 sys.executable,
#                 '-c',
#                 f'''
# import sys
# sys.path.append('/opt/module/guba-crawler')
# from full_text_Crawler import FullTextCrawler

# crawler = FullTextCrawler(
#     config_path="{config_data}",
#     db_name="guba",
#     collection_name="{collection_name}"
# )
# crawler.run()
# '''
#             ]
            
#             # 启动异步进程
#             process = subprocess.Popen(
#                 cmd,
#                 cwd='/opt/module/guba-crawler',
#                 stdout=subprocess.PIPE,
#                 stderr=subprocess.STDOUT,
#                 universal_newlines=True,
#                 bufsize=1
#             )
            
#             # 保存进程对象到current_task
#             if self.current_task:
#                 self.current_task['process'] = process
#                 self.current_task['temp_config_path'] = config_data
            
#             with open(log_file_path, 'a', encoding='utf-8') as f:
#                 f.write(f"\n[INFO] {datetime.now().isoformat()} - 全文爬取进程已启动，PID: {process.pid}\n")
                
        except Exception as e:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 全文爬取启动失败: {str(e)}\n")
            raise

    
    def _execute_comment_crawling(self, task, temp_config_path, execution_id, log_file_path):
        """执行评论爬取"""
        try:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[INFO] {datetime.now().isoformat()} - 开始评论爬取\n")
            
            # 导入MainController
            from main_controller import MainController
            
            
            # 创建控制器实例
            controller = MainController(config_path=temp_config_path)
            
            # 执行评论爬取
            controller.crawl_comments()
            
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[INFO] {datetime.now().isoformat()} - 评论爬取完成\n")

        except Exception as e:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[ERROR] {datetime.now().isoformat()} - 评论爬取失败: {str(e)}\n")
            raise
    
    def _create_temp_config_file(self, config_data, file_path, task_name=None):
        """创建临时配置文件"""
        # 首先读取原始配置文件作为基础
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        
        # 用前端传来的数据覆盖相应配置段
        for section_name, section_data in config_data.items():
            if not config.has_section(section_name):
                config.add_section(section_name)
            for key, value in section_data.items():
                config.set(section_name, key, str(value))
        
        # 如果提供了任务名称，修改Redis键为任务名称
        if task_name and config.has_section('Redis'):
            # 使用任务名称作为Redis键，确保不同任务使用不同的队列
            redis_key = f"urls_{task_name.replace(' ', '_')}"
            config.set('Redis', 'redis_key', redis_key)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            config.write(f)
    
    def _update_task_status(self, task_id, status, extra_data=None):
        """更新任务状态"""
        update_fields = ['status = %s', 'updated_at = NOW()']
        params = [status]
        
        if extra_data:
            for key, value in extra_data.items():
                if key in ['started_at', 'completed_at']:
                    update_fields.append(f'{key} = %s')
                    params.append(value)
                elif key in ['progress_percent']:
                    update_fields.append(f'{key} = %s')
                    params.append(value)
                elif key in ['current_step', 'error_message']:
                    update_fields.append(f'{key} = %s')
                    params.append(value)
        
        params.append(task_id)
        query = f"UPDATE task_queue SET {', '.join(update_fields)} WHERE id = %s"
        self.db_manager.execute_query(query, tuple(params), fetch_all=False)

    
    
    def _complete_task(self, task_id, execution_id, status, result_data, log_file_path=None):
        """完成任务"""
        try:
            # 更新队列状态
            self._update_task_status(task_id, status, {
                'completed_at': datetime.now(),
                'progress_percent': 100,
                'current_step': '任务完成'
            })
            
            # 插入历史记录
            task_data = self.current_task['task_data']
            duration = (datetime.now() - self.current_task['start_time']).total_seconds()
            
            history_query = """
            INSERT INTO task_history 
            (task_config_id, task_queue_id, execution_id, status, started_at, completed_at, 
             duration_seconds, success_count, failed_count, total_count, result_data, log_file_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            history_params = (
                task_data['task_config_id'],
                task_id,
                execution_id,
                status,
                self.current_task['start_time'],
                datetime.now(),
                int(duration),
                result_data.get('success_count', 0),
                result_data.get('failed_count', 0),
                result_data.get('total_count', 0),
                json.dumps(result_data),
                log_file_path
            )
            
            self.db_manager.execute_insert(history_query, history_params)
            
            # 清理临时配置文件
            if self.current_task and 'temp_config_path' in self.current_task:
                temp_config_path = self.current_task['temp_config_path']
                if os.path.exists(temp_config_path):
                    os.remove(temp_config_path)
            
            # 清除当前任务
            self.current_task = None
            
        except Exception as e:
            print(f"完成任务时发生异常: {e}")
    
    def _handle_task_error(self, task_id, execution_id, error_message, log_file_path=None):
        """处理任务错误"""
        try:
            self._update_task_status(task_id, 'failed', {
                'completed_at': datetime.now(),
                'error_message': error_message,
                'current_step': '任务失败'
            })
            
            # 插入错误历史记录
            if self.current_task:
                task_data = self.current_task['task_data']
                duration = (datetime.now() - self.current_task['start_time']).total_seconds()
                
                history_query = """
                INSERT INTO task_history 
                (task_config_id, task_queue_id, execution_id, status, started_at, completed_at, 
                 duration_seconds, error_message, log_file_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                history_params = (
                    task_data['task_config_id'],
                    task_id,
                    execution_id,
                    'failed',
                    self.current_task['start_time'],
                    datetime.now(),
                    int(duration),
                    error_message,
                    log_file_path
                )
                
                self.db_manager.execute_insert(history_query, history_params)
                
                # 清理临时配置文件
                if 'temp_config_path' in self.current_task:
                    temp_config_path = self.current_task['temp_config_path']
                    if os.path.exists(temp_config_path):
                        os.remove(temp_config_path)
                
                # 清除当前任务
                self.current_task = None
                
        except Exception as e:
            print(f"处理任务错误时发生异常: {e}")
    
    def _check_current_task(self):
        """检查当前任务状态"""
        if not self.current_task:
            return
        
        task_id = self.current_task['task_id']
        
        # 更新任务运行时间和状态
        running_duration = (datetime.now() - self.current_task['start_time']).total_seconds()
        
        # 更新当前步骤信息
        current_step = f"任务运行中（已运行 {int(running_duration//60)} 分钟）"
        
        # 根据运行时间估算进度（这里可以根据实际情况调整）
        if running_duration < 300:  # 前5分钟
            progress = min(10, running_duration / 30)  # 0-10%
        elif running_duration < 1800:  # 5-30分钟
            progress = 10 + min(50, (running_duration - 300) / 30)  # 10-60%
        else:  # 30分钟以上
            progress = 60 + min(30, (running_duration - 1800) / 120)  # 60-90%
        
        # 更新任务状态
        self._update_task_status(task_id, 'running', {
            'current_step': current_step,
            'progress_percent': round(progress, 2)
        })
        
        # 检查任务是否超时
        timeout_minutes = 120  # 默认2小时超时
        if datetime.now() - self.current_task['start_time'] > timedelta(minutes=timeout_minutes):
            self._handle_task_error(
                self.current_task['task_id'],
                self.current_task['execution_id'],
                f"任务执行超时（超过{timeout_minutes}分钟）"
            )
        
        # 检查进程是否还在运行（如果有进程对象）
        if hasattr(self.current_task, 'process') and self.current_task['process']:
            if self.current_task['process'].poll() is not None:
                # 进程已结束，检查退出码
                exit_code = self.current_task['process'].returncode
                if exit_code == 0:
                    # 任务成功完成
                    self._complete_task(
                        task_id,
                        self.current_task['execution_id'],
                        'completed',
                        {'success_count': 0, 'failed_count': 0, 'total_count': 0}
                    )
                else:
                    # 任务失败
                    self._handle_task_error(
                        task_id,
                        self.current_task['execution_id'],
                        f"任务进程异常退出，退出码: {exit_code}"
                    )


# 创建Flask应用
app = Flask(__name__)
CORS(app)  # 允许跨域请求

# JWT配置
app.config['JWT_SECRET_KEY'] = 'crawler-scheduler-secret-key-2024'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=24)

# 认证配置
VALID_CREDENTIALS = {
    'shenghao': 'aa20020320'
}

# 认证相关函数
def generate_token(username: str) -> str:
    """生成JWT token"""
    payload = {
        'username': username,
        'exp': datetime.utcnow() + app.config['JWT_ACCESS_TOKEN_EXPIRES'],
        'iat': datetime.utcnow()
    }
    return jwt.encode(payload, app.config['JWT_SECRET_KEY'], algorithm='HS256')

def verify_token(token: str) -> Optional[str]:
    """验证JWT token"""
    try:
        payload = jwt.decode(token, app.config['JWT_SECRET_KEY'], algorithms=['HS256'])
        return payload.get('username')
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def require_auth(f):
    """认证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'success': False, 'message': '缺少认证信息'}), 401
        
        try:
            token = auth_header.split(' ')[1]  # Bearer <token>
        except IndexError:
            return jsonify({'success': False, 'message': '认证格式错误'}), 401
        
        username = verify_token(token)
        if not username:
            return jsonify({'success': False, 'message': '认证失败或已过期'}), 401
        
        request.current_user = username
        return f(*args, **kwargs)
    return decorated_function

def token_required(f):
    """Token认证装饰器（与require_auth功能相同）"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'success': False, 'message': '缺少认证信息'}), 401
        
        try:
            token = auth_header.split(' ')[1]  # Bearer <token>
        except IndexError:
            return jsonify({'success': False, 'message': '认证格式错误'}), 401
        
        username = verify_token(token)
        if not username:
            return jsonify({'success': False, 'message': '认证失败或已过期'}), 401
        
        request.current_user = username
        return f(*args, **kwargs)
    return decorated_function

# 初始化数据库管理器和任务调度器
db_manager = DatabaseManager()
task_scheduler = TaskScheduler(db_manager)




@app.route('/api/tasks/configs', methods=['GET'])
@require_auth
def get_task_configs():
    """获取任务配置列表"""
    try:
        query = """
        SELECT id, task_name, description, config_data, crawler_type, priority, is_active, 
               created_at, updated_at, created_by
        FROM task_configs 
        ORDER BY priority DESC, created_at DESC
        """
        configs = db_manager.execute_query(query)
        return jsonify({'success': True, 'data': configs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/configs', methods=['POST'])
@require_auth
def create_task_config():
    """创建任务配置"""
    try:
        data = request.get_json()
        
        # 验证必需字段
        required_fields = ['task_name', 'crawler_type', 'config_data']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'缺少必需字段: {field}'}), 400
        
        query = """
        INSERT INTO task_configs (task_name, description, config_data, crawler_type, priority, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        params = (
            data['task_name'],
            data.get('description', ''),
            json.dumps(data['config_data']),
            data['crawler_type'],
            data.get('priority', 0),
            data.get('created_by', 'user')
        )
        
        config_id = db_manager.execute_insert(query, params)
        return jsonify({'success': True, 'data': {'id': config_id}})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/configs/<int:config_id>', methods=['PUT'])
@require_auth
def update_task_config(config_id):
    """更新任务配置"""
    try:
        data = request.get_json()
        
        update_fields = []
        params = []
        
        for field in ['task_name', 'description', 'crawler_type', 'priority', 'is_active']:
            if field in data:
                update_fields.append(f'{field} = %s')
                params.append(data[field])
        
        if 'config_data' in data:
            update_fields.append('config_data = %s')
            params.append(json.dumps(data['config_data']))
        
        if not update_fields:
            return jsonify({'success': False, 'error': '没有提供更新字段'}), 400
        
        update_fields.append('updated_at = NOW()')
        params.append(config_id)
        
        query = f"UPDATE task_configs SET {', '.join(update_fields)} WHERE id = %s"
        db_manager.execute_query(query, tuple(params), fetch_all=False)
        
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/configs/<int:config_id>/data', methods=['GET'])
@require_auth
def get_task_config_data(config_id):
    """获取特定任务的配置数据"""
    try:
        query = "SELECT config_data FROM task_configs WHERE id = %s"
        result = db_manager.execute_query(query, (config_id,))
        
        if not result:
            return jsonify({'success': False, 'error': '任务配置不存在'}), 404
        
        config_data = result[0]['config_data']
        # 如果config_data是字符串，解析为JSON对象
        if isinstance(config_data, str):
            config_data = json.loads(config_data)
        
        return jsonify({'success': True, 'data': config_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/configs/<int:config_id>', methods=['DELETE'])
@require_auth
def delete_task_config(config_id):
    """删除任务配置"""
    try:
        query = "DELETE FROM task_configs WHERE id = %s"
        db_manager.execute_query(query, (config_id,), fetch_all=False)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/queue', methods=['GET'])
@require_auth
def get_task_queue():
    """获取任务队列"""
    try:
        # 获取所有队列中的任务（包括pending和running状态）
        queue_query = """
        SELECT tq.*, tc.task_name, tc.crawler_type,
               CASE 
                   WHEN tq.status = 'running' THEN '正在执行'
                   WHEN tq.status = 'pending' THEN '等待中'
                   ELSE tq.status
               END as status_display
        FROM task_queue tq
        JOIN task_configs tc ON tq.task_config_id = tc.id
        WHERE tq.status IN ('pending', 'running')
        ORDER BY 
            CASE WHEN tq.status = 'running' THEN 0 ELSE 1 END,
            tq.queue_position ASC, 
            tq.id ASC
        """
        queue_tasks = db_manager.execute_query(queue_query)
        
        # 获取当前正在运行的任务详细信息
        running_task_query = """
        SELECT tq.*, tc.task_name, tc.crawler_type,
               TIMESTAMPDIFF(SECOND, tq.started_at, NOW()) as running_duration
        FROM task_queue tq
        JOIN task_configs tc ON tq.task_config_id = tc.id
        WHERE tq.status = 'running'
        ORDER BY tq.started_at ASC
        LIMIT 1
        """
        current_running = db_manager.execute_query(running_task_query, fetch_one=True)
        
        # 获取等待队列统计
        pending_count_query = "SELECT COUNT(*) as count FROM task_queue WHERE status = 'pending'"
        pending_result = db_manager.execute_query(pending_count_query, fetch_one=True)
        pending_count = pending_result['count'] if pending_result else 0
        
        # 构建返回数据
        result_data = {
            'queue_tasks': queue_tasks,
            'current_running': current_running,
            'pending_count': pending_count,
            'total_in_queue': len(queue_tasks),
            'scheduler_status': 'running' if task_scheduler and task_scheduler.running else 'stopped'
        }
        
        return jsonify({'success': True, 'data': result_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/queue', methods=['POST'])
@require_auth
def add_task_to_queue():
    """添加任务到队列"""
    try:
        data = request.get_json()
        
        if 'task_config_id' not in data:
            return jsonify({'success': False, 'error': '缺少task_config_id'}), 400
        
        # 获取下一个队列位置
        position_query = "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_position FROM task_queue"
        result = db_manager.execute_query(position_query, fetch_one=True)
        next_position = result['next_position']
        
        query = """
        INSERT INTO task_queue (task_config_id, queue_position, scheduled_at)
        VALUES (%s, %s, %s)
        """
        
        params = (
            data['task_config_id'],
            next_position,
            data.get('scheduled_at')
        )
        
        queue_id = db_manager.execute_insert(query, params)
        return jsonify({'success': True, 'data': {'id': queue_id, 'position': next_position}})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/queue/<int:queue_id>', methods=['DELETE'])
@require_auth
def remove_task_from_queue(queue_id):
    """从队列中移除任务"""
    try:
        # 检查任务状态
        status_query = "SELECT status FROM task_queue WHERE id = %s"
        result = db_manager.execute_query(status_query, (queue_id,), fetch_one=True)
        
        if not result:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        if result['status'] == 'running':
            return jsonify({'success': False, 'error': '无法删除正在运行的任务'}), 400
        
        query = "DELETE FROM task_queue WHERE id = %s"
        db_manager.execute_query(query, (queue_id,), fetch_all=False)
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/history', methods=['GET'])
@require_auth
def get_task_history():
    """获取任务历史"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        offset = (page - 1) * limit
        
        query = """
        SELECT th.*, tc.task_name, tc.crawler_type
        FROM task_history th
        JOIN task_configs tc ON th.task_config_id = tc.id
        ORDER BY th.started_at DESC
        LIMIT %s OFFSET %s
        """
        
        history = db_manager.execute_query(query, (limit, offset))
        
        # 获取总数
        count_query = "SELECT COUNT(*) as total FROM task_history"
        total_result = db_manager.execute_query(count_query, fetch_one=True)
        total = total_result['total']
        
        return jsonify({
            'success': True,
            'data': {
                'items': history,
                'total': total,
                'page': page,
                'limit': limit
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tasks/status', methods=['GET'])
@require_auth
def get_system_status():
    """获取系统状态"""
    try:
        # 获取队列统计
        queue_stats_query = """
        SELECT status, COUNT(*) as count
        FROM task_queue
        GROUP BY status
        """
        queue_stats = db_manager.execute_query(queue_stats_query)
        
        # 获取今日执行统计
        today_stats_query = """
        SELECT status, COUNT(*) as count
        FROM task_history
        WHERE DATE(started_at) = CURDATE()
        GROUP BY status
        """
        today_stats = db_manager.execute_query(today_stats_query)
        
        # 获取当前运行任务
        current_task_info = None
        if task_scheduler.current_task:
            current_task_info = {
                'task_id': task_scheduler.current_task['task_id'],
                'execution_id': task_scheduler.current_task['execution_id'],
                'task_name': task_scheduler.current_task['task_data']['task_name'],
                'start_time': task_scheduler.current_task['start_time'].isoformat(),
                'duration_seconds': int((datetime.now() - task_scheduler.current_task['start_time']).total_seconds())
            }
        
        return jsonify({
            'success': True,
            'data': {
                'scheduler_running': task_scheduler.running,
                'current_task': current_task_info,
                'queue_stats': {item['status']: item['count'] for item in queue_stats},
                'today_stats': {item['status']: item['count'] for item in today_stats}
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/tasks/poststatus', methods=['GET'])
def post_task_status():
    try:
        query = """SELECT result_summary from task_queue where status = 'running' """
        status = db_manager.execute_query(query,fetch_one=True)
        return status if status else -1
    except Exception as e:
        print(e)
        return None



@app.route('/api/tasks/<int:task_id>/logs', methods=['GET'])
@require_auth
def get_task_log(task_id):
    """获取任务日志"""
    try:
        # 首先尝试从task_history表获取任务信息
        task_query = """
        SELECT th.*, tc.task_name, tc.crawler_type
        FROM task_history th
        JOIN task_configs tc ON th.task_config_id = tc.id
        WHERE th.id = %s
        """
        task_info = db_manager.execute_query(task_query, (task_id,), fetch_one=True)
        
        # 如果在历史表中没找到，尝试从队列表中查找
        if not task_info:
            queue_query = """
            SELECT tq.*, tc.task_name, tc.crawler_type
            FROM task_queue tq
            JOIN task_configs tc ON tq.task_config_id = tc.id
            WHERE tq.id = %s
            """
            task_info = db_manager.execute_query(queue_query, (task_id,), fetch_one=True)
        
        if not task_info:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        # 构建日志文件路径（task_queue表中没有log_file_path字段）
        log_filename = f"task_{task_id}_{task_info['task_name']}_{task_info['crawler_type']}.log"
        log_file_path = os.path.join('logs', log_filename)
        
        # 如果是历史任务，尝试使用数据库中存储的日志文件路径
        if 'log_file_path' in task_info and task_info['log_file_path']:
            log_file_path = task_info['log_file_path']
        
        # 检查日志文件是否存在
        if not os.path.exists(log_file_path):
            return jsonify({
            'success': True,
            'data': {
                'logs': '日志文件不存在或任务尚未开始执行',
                'file_exists': False
            }
        })
        
        # 读取日志文件内容
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
        except UnicodeDecodeError:
            # 如果UTF-8解码失败，尝试其他编码
            with open(log_file_path, 'r', encoding='gbk') as f:
                log_content = f.read()
        
        return jsonify({
            'success': True,
            'data': {
                'logs': log_content,
                'file_exists': True,
                'task_info': task_info
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scheduler/start', methods=['POST'])
@require_auth
def start_scheduler():
    """启动调度器"""
    try:
        task_scheduler.start()
        return jsonify({'success': True, 'message': '调度器已启动'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scheduler/stop', methods=['POST'])
@require_auth
def stop_scheduler():
    """停止调度器"""
    try:
        task_scheduler.stop()
        return jsonify({'success': True, 'message': '调度器已停止'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# 认证API路由
@app.route('/api/auth/login', methods=['POST'])
def login():
    """用户登录"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({
                'success': False,
                'message': '用户名和密码不能为空'
            }), 400
        
        # 验证用户名和密码
        if username in VALID_CREDENTIALS and VALID_CREDENTIALS[username] == password:
            token = generate_token(username)
            return jsonify({
                'success': True,
                'message': '登录成功',
                'data': {
                    'token': token,
                    'username': username,
                    'expires_in': int(app.config['JWT_ACCESS_TOKEN_EXPIRES'].total_seconds())
                }
            })
        else:
            return jsonify({
                'success': False,
                'message': '用户名或密码错误'
            }), 401
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'登录失败: {str(e)}'
        }), 500

@app.route('/api/auth/verify', methods=['GET'])
def verify_auth():
    """验证token有效性"""
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'success': False, 'message': '缺少认证信息'}), 401
    
    try:
        token = auth_header.split(' ')[1]
        username = verify_token(token)
        if username:
            return jsonify({
                'success': True,
                'data': {'username': username}
            })
        else:
            return jsonify({'success': False, 'message': '认证失败或已过期'}), 401
    except Exception as e:
        return jsonify({'success': False, 'message': '认证验证失败'}), 401

# 静态文件服务（用于前端）
@app.route('/')
def serve_frontend():
    """服务前端页面"""
    return send_from_directory('static', 'login.html')

@app.route('/static/index.html')
def main_page():
    """主页面（需要认证）"""
    return send_from_directory('static', 'index.html')


@app.route('/<path:path>')
def serve_static_files(path):
    """服务静态文件"""
    return send_from_directory('static', path)


# 全文爬取器相关API
@app.route('/api/fulltext/status', methods=['GET'])
@token_required
def get_fulltext_status():
    """获取全文爬取器状态"""
    try:
        import pymongo
        import redis
        import subprocess
        
        # 检查全文爬取器进程是否运行
        is_running = False


        # try:
        #     result = subprocess.run(['pgrep', '-f', 'full_text_Crawler.py'], 
        #                           capture_output=True, text=True)
        #     is_running = bool(result.stdout.strip())
        # except:
        #     pass
            
        #如果有任务是running状态  全文爬取器状态也为running
        
        
        # 获取Redis队列中URL数量
        queue_count = 0
        try:
            r = redis.Redis(host='localhost', port=6379, password='123456', db=0)
            for key in r.scan_iter('*'):  # 使用scan_iter避免阻塞
                queue_count += r.llen(key) or 0  # memory_usage需要Redis 4.0+
        except:
            pass
        if queue_count > 0:
            is_running = True
        
        # 获取MongoDB中全文爬取进度
        # 每个任务的collection 不一样 从配置文件中获取
        #获取当前运行的配置文件
        #根据当前运行的任务id查当前运行任务的collection_name
        collection_name='zssh00016'
        try:
            query = """
                SELECT tc.config_data
    FROM task_queue tq
    JOIN task_configs tc ON tq.task_config_id = tc.id
    WHERE tq.status = 'running' AND tc.is_active = 1
                """
                
            result = db_manager.execute_query(query, fetch_one=True)
            #得到了一个json 拿mainClass 中的collectionName
            collection_name = result['config_data']['mainClass']['collectionName']
        except:
            collection_name = 'zssh00016'

        
        progress = 0.0
        try:
            client = pymongo.MongoClient('mongodb://admin:aa20020320@localhost:27017/')
            db = client['guba']
            collection = db[collection_name]
            total = collection.count_documents({})
            with_fulltext = collection.count_documents({'full_text': {'$exists': True}})
            if total > 0:
                progress = round(with_fulltext / total * 100, 1)
        except:
            pass
        
        return jsonify({
            'success': True,
            'data': {
                'is_running': is_running,
                'queue_count': queue_count,
                'progress': str(progress)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

##获取帖子爬取器 mainclass的运行状态



@app.route('/api/fulltext/start', methods=['POST'])
@token_required
def start_fulltext_crawler():
    """启动全文爬取器"""
    try:
        import subprocess
        import os
        
        # 检查是否已经在运行
        result = subprocess.run(['pgrep', '-f', 'full_text_Crawler.py'], 
                              capture_output=True, text=True)
        if result.stdout.strip():
            return jsonify({'success': False, 'error': '全文爬取器已在运行中'}), 400
        
        # 启动全文爬取器
        cmd = ['python3', 'full_text_Crawler.py', '--config', 'config.ini', '--db', 'guba', '--collection', 'zssh000016']
        subprocess.Popen(cmd, cwd='/opt/module/guba-crawler')
        
        return jsonify({
            'success': True,
            'message': '全文爬取器启动成功'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/fulltext/stop', methods=['POST'])
@token_required
def stop_fulltext_crawler():
    """停止全文爬取器"""
    try:
        import subprocess
        
        # 查找并停止全文爬取器进程
        result = subprocess.run(['pgrep', '-f', 'full_text_Crawler.py'], 
                              capture_output=True, text=True)
        if not result.stdout.strip():
            return jsonify({'success': False, 'error': '全文爬取器未在运行'}), 400
        
        pids = result.stdout.strip().split('\n')
        for pid in pids:
            if pid:
                subprocess.run(['kill', pid])
        
        return jsonify({
            'success': True,
            'message': '全文爬取器停止成功'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    # # 启动调度器
    # werkzeug_logger = logging.getLogger('werkzeug')
    # werkzeug_handler = logging.FileHandler('/opt/module/guba-crawler/logs/api_access.log')
    # werkzeug_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    # werkzeug_logger.addHandler(werkzeug_handler)
    # werkzeug_logger.setLevel(logging.INFO)
    task_scheduler.start()
    
    try:
        # 启动Flask应用
        app.run(host='0.0.0.0', port=5000, debug=False)
    finally:
        # 停止调度器
        task_scheduler.stop()
