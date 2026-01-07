#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的数据库客户端
整合MongoDB和Redis配置管理，支持线程池连接
"""

import pymongo
import redis
import configparser
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime


class MongoDBClient:
    """MongoDB客户端类 - 支持连接池"""
    
    def __init__(self, config: configparser.ConfigParser, db_name: str, collection_name: str):
        self.host = config.get('MongoDB', 'host')
        self.port = config.getint('MongoDB', 'port')
        self.username = config.get('MongoDB', 'username') if config.get('MongoDB', 'username') else None
        self.password = config.get('MongoDB', 'password') if config.get('MongoDB', 'password') else None
        self.db_name = db_name
        self.collection_name = collection_name
        
        # 连接池配置 - 默认连接数为8
        self.max_pool_size = 60
        self.min_pool_size = 40
        self.max_idle_time_ms = 30000
        self.connect_timeout_ms = 20000
        self.server_selection_timeout_ms = 30000
        
        # 创建连接池
        if self.username and self.password:
            self.client = pymongo.MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                maxPoolSize=self.max_pool_size,
                minPoolSize=self.min_pool_size,
                maxIdleTimeMS=self.max_idle_time_ms,
                connectTimeoutMS=self.connect_timeout_ms,
                serverSelectionTimeoutMS=self.server_selection_timeout_ms,
                retryWrites=True,
                retryReads=True
            )
        else:
            self.client = pymongo.MongoClient(
                host=self.host, 
                port=self.port,
                maxPoolSize=self.max_pool_size,
                minPoolSize=self.min_pool_size,
                maxIdleTimeMS=self.max_idle_time_ms,
                connectTimeoutMS=self.connect_timeout_ms,
                serverSelectionTimeoutMS=self.server_selection_timeout_ms,
                retryWrites=True,
                retryReads=True
            )
        
        self.database = self.client[self.db_name]
        self.collection = self.database[self.collection_name]
        
        # 线程锁（虽然PyMongo是线程安全的，但为了确保操作的原子性）
        self._lock = threading.RLock()
    
    def insert_one(self, document: Dict[str, Any]) -> None:
        """插入单个文档"""
        with self._lock:
            self.collection.insert_one(document)
    
    def insert_many(self, documents: List[Dict[str, Any]]) -> None:
        """插入多个文档"""
        if documents:
            with self._lock:
                self.collection.insert_many(documents)
    
    def find_one(self, query: Dict[str, Any] = None, projection: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """查找单个文档"""
        return self.collection.find_one(query or {}, projection)
    
    def find(self, query: Dict[str, Any] = None, projection: Dict[str, Any] = None) -> pymongo.cursor.Cursor:
        """查找多个文档"""
        return self.collection.find(query or {}, projection)
    
    def find_first(self) -> Optional[Dict[str, Any]]:
        """查找第一个文档"""
        return self.collection.find_one(sort=[('_id', 1)])
    
    def find_last(self) -> Optional[Dict[str, Any]]:
        """查找最后一个文档"""
        return self.collection.find_one(sort=[('_id', -1)])
    
    def count_documents(self, query: Dict[str, Any] = None) -> int:
        """统计文档数量"""
        return self.collection.count_documents(query or {})
    

    
    
    def update_one(self, filter_query: Dict[str, Any], update_data: Dict[str, Any], upsert: bool = True) -> None:
        """更新单个文档"""
        with self._lock:
            self.collection.update_one(filter_query, {'$set': update_data}, upsert=upsert)
    
    def delete_many(self, query: Dict[str, Any]) -> None:
        """删除多个文档"""
        with self._lock:
            self.collection.delete_many(query)
    
    def drop(self) -> None:
        """删除集合"""
        with self._lock:
            self.collection.drop()
    
    def close(self) -> None:
        """关闭连接"""
        self.client.close()
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接池信息"""
        return {
            'max_pool_size': self.max_pool_size,
            'min_pool_size': self.min_pool_size,
            'max_idle_time_ms': self.max_idle_time_ms,
            'connect_timeout_ms': self.connect_timeout_ms,
            'server_selection_timeout_ms': self.server_selection_timeout_ms
        }


class RedisClient:
    """Redis客户端类 - 支持连接池"""
    
    def __init__(self, config: configparser.ConfigParser):
        self.host = config.get('Redis', 'redis_host')
        self.port = config.getint('Redis', 'redis_port')
        self.password = config.get('Redis', 'redis_password') if config.get('Redis', 'redis_password') else None
        self.db = config.getint('Redis', 'redis_db')
        self.redis_key = config.get('Redis', 'redis_key')
        
        # 连接池配置 - 默认连接数为8
        self.max_connections = 50
        self.retry_on_timeout = True
        self.socket_timeout = 5.0
        self.socket_connect_timeout = 5.0
        self.socket_keepalive = True
        self.socket_keepalive_options = {}
        
        # 创建连接池
        self.connection_pool = redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            max_connections=self.max_connections,
            retry_on_timeout=self.retry_on_timeout,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
            socket_keepalive=self.socket_keepalive,
            socket_keepalive_options=self.socket_keepalive_options,
            decode_responses=True
        )
        
        # 创建Redis客户端
        self.client = redis.StrictRedis(connection_pool=self.connection_pool)
        
        # 线程锁
        self._lock = threading.RLock()
    
    def add_url(self, url: str) -> None:
        """添加URL到队列"""
        with self._lock:
            self.client.lpush(self.redis_key, url)
    
    def get_url(self) -> Optional[str]:
        """从队列获取URL"""
        return self.client.rpop(self.redis_key)
    
    def get_queue_length(self) -> int:
        """获取队列长度"""
        return self.client.llen(self.redis_key)
    
    def clear_queue(self) -> None:
        """清空队列"""
        with self._lock:
            self.client.delete(self.redis_key)
    
    def close(self) -> None:
        """关闭连接池"""
        self.connection_pool.disconnect()
    
    def set(self, key: str, value: str, ex: int = None) -> bool:
        """设置键值对"""
        return self.client.set(key, value, ex=ex)
    
    def get(self, key: str) -> Optional[str]:
        """获取键对应的值"""
        return self.client.get(key)
    
    def llen(self, key: str) -> int:
        """获取列表长度"""
        return self.client.llen(key)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接池信息"""
        pool_info = {
            'max_connections': self.max_connections,
            'created_connections': self.connection_pool.created_connections,
            'available_connections': len(self.connection_pool._available_connections),
            'in_use_connections': len(self.connection_pool._in_use_connections)
        }
        return pool_info
    
    def ping(self) -> bool:
        """测试连接"""
        try:
            return self.client.ping()
        except Exception:
            return False


class DatabaseManager:
    """数据库管理器 - 支持线程池连接"""
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
        self.mongo_clients = {}
        self.redis_client = None
        self._lock = threading.RLock()
    
    def get_mongo_client(self, db_name: str, collection_name: str) -> MongoDBClient:
        """获取MongoDB客户端（线程安全）"""
        key = f"{db_name}.{collection_name}"
        with self._lock:
            if key not in self.mongo_clients:
                self.mongo_clients[key] = MongoDBClient(self.config, db_name, collection_name)
            return self.mongo_clients[key]
    
    def get_redis_client(self) -> RedisClient:
        """获取Redis客户端（线程安全）"""
        with self._lock:
            if self.redis_client is None:
                self.redis_client = RedisClient(self.config)
            return self.redis_client
    
    def close_all(self) -> None:
        """关闭所有连接"""
        with self._lock:
            for client in self.mongo_clients.values():
                client.close()
            if self.redis_client:
                self.redis_client.close()
    
    def get_all_connection_info(self) -> Dict[str, Any]:
        """获取所有连接池信息"""
        info = {
            'mongodb_clients': {},
            'redis_client': None
        }
        
        for key, client in self.mongo_clients.items():
            info['mongodb_clients'][key] = client.get_connection_info()
        
        if self.redis_client:
            info['redis_client'] = self.redis_client.get_connection_info()
        
        return info