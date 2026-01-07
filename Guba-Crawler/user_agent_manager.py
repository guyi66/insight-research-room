#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2024/01/20
# @Author  : Assistant
# @File    : user_agent_manager.py

import random
import threading
import time
from typing import List, Optional, Dict
import logging
from urllib.parse import urlparse
from user_agent_config import get_site_config, get_recommended_user_agents, USER_AGENT_CONFIG


class UserAgentManager:
    """
    User-Agent轮换管理器
    支持随机选择和顺序轮换两种模式
    """
    
    def __init__(self, mode: str = 'random', site_type: str = 'general'):
        """
        初始化User-Agent管理器
        
        Args:
            mode: 轮换模式，'random'(随机) 或 'sequential'(顺序)
            site_type: 网站类型，用于选择合适的User-Agent集合
        """
        self.mode = mode
        self.site_type = site_type
        self.current_index = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # 黑名单管理
        self.blacklisted_agents: Dict[str, float] = {}  # UA -> 黑名单到期时间
        self.last_used_agent = None
        self.last_request_time = 0
        
        # 从配置文件加载User-Agent列表
        self.user_agents = get_recommended_user_agents(site_type)
        
        # 如果配置文件中的列表为空，使用默认列表
        if not self.user_agents:
            self.user_agents = [
            # Chrome Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            
            # Chrome macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            
            # Firefox Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
            
            # Firefox macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:119.0) Gecko/20100101 Firefox/119.0',
            
            # Safari macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
            
            # Edge Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
            
            
            
            # Linux Chrome
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            ]
        
        self.logger.info(f"UserAgentManager初始化完成，模式: {mode}，网站类型: {site_type}，共加载 {len(self.user_agents)} 个User-Agent")
    
    def get_user_agent(self, url: Optional[str] = None) -> str:
        """
        获取一个User-Agent
        
        Args:
            url: 目标URL，用于获取网站特定配置
            
        Returns:
            User-Agent字符串
        """
        with self.lock:
            # 清理过期的黑名单
            self._clean_blacklist()
            
            # 获取可用的User-Agent列表
            available_agents = self._get_available_agents(url)
            
            if not available_agents:
                self.logger.warning("所有User-Agent都被黑名单，重置黑名单")
                self.blacklisted_agents.clear()
                available_agents = self.user_agents.copy()
            
            # 根据模式选择User-Agent
            if self.mode == 'random':
                # 避免连续使用相同的User-Agent
                if USER_AGENT_CONFIG['rotation_strategy']['avoid_consecutive'] and len(available_agents) > 1:
                    available_agents = [ua for ua in available_agents if ua != self.last_used_agent]
                
                user_agent = random.choice(available_agents)
            elif self.mode == 'sequential':
                user_agent = available_agents[self.current_index % len(available_agents)]
                self.current_index = (self.current_index + 1) % len(available_agents)
            else:
                self.logger.warning(f"未知模式: {self.mode}，使用随机模式")
                user_agent = random.choice(available_agents)
            
            self.last_used_agent = user_agent
            self.last_request_time = time.time()
            return user_agent
    
    def get_random_user_agent(self) -> str:
        """
        强制获取随机User-Agent（忽略当前模式）
        
        Returns:
            随机User-Agent字符串
        """
        return random.choice(self.user_agents)
    
    def get_mobile_user_agent(self) -> str:
        """
        获取移动端User-Agent
        
        Returns:
            移动端User-Agent字符串
        """
        mobile_agents = [
            ua for ua in self.user_agents 
            if 'Mobile' in ua or 'iPhone' in ua or 'Android' in ua
        ]
        return random.choice(mobile_agents) if mobile_agents else self.get_user_agent()
    
    def get_desktop_user_agent(self) -> str:
        """
        获取桌面端User-Agent
        
        Returns:
            桌面端User-Agent字符串
        """
        desktop_agents = [
            ua for ua in self.user_agents 
            if 'Mobile' not in ua and 'iPhone' not in ua and 'Android' not in ua
        ]
        return random.choice(desktop_agents) if desktop_agents else self.get_user_agent()
    
    def add_user_agent(self, user_agent: str) -> None:
        """
        添加自定义User-Agent
        
        Args:
            user_agent: 要添加的User-Agent字符串
        """
        with self.lock:
            if user_agent not in self.user_agents:
                self.user_agents.append(user_agent)
                self.logger.info(f"添加新的User-Agent: {user_agent}")
            else:
                self.logger.warning(f"User-Agent已存在: {user_agent}")
    
    def remove_user_agent(self, user_agent: str) -> bool:
        """
        移除指定的User-Agent
        
        Args:
            user_agent: 要移除的User-Agent字符串
            
        Returns:
            是否成功移除
        """
        with self.lock:
            if user_agent in self.user_agents:
                self.user_agents.remove(user_agent)
                self.logger.info(f"移除User-Agent: {user_agent}")
                return True
            else:
                self.logger.warning(f"User-Agent不存在: {user_agent}")
                return False
    
    def get_user_agent_count(self) -> int:
        """
        获取User-Agent总数
        
        Returns:
            User-Agent总数
        """
        return len(self.user_agents)
    
    def get_all_user_agents(self) -> List[str]:
        """
        获取所有User-Agent列表
        
        Returns:
            User-Agent列表的副本
        """
        return self.user_agents.copy()
    
    def set_mode(self, mode: str) -> None:
        """
        设置轮换模式
        
        Args:
            mode: 轮换模式，'random' 或 'sequential'
        """
        with self.lock:
            if mode in ['random', 'sequential']:
                self.mode = mode
                self.current_index = 0  # 重置索引
                self.logger.info(f"轮换模式已设置为: {mode}")
            else:
                self.logger.error(f"无效的轮换模式: {mode}")
    
    def reset_index(self) -> None:
        """
        重置顺序轮换的索引
        """
        with self.lock:
            self.current_index = 0
            self.logger.info("顺序轮换索引已重置")


    def _clean_blacklist(self) -> None:
        """
        清理过期的黑名单条目
        """
        current_time = time.time()
        expired_agents = [
            ua for ua, expire_time in self.blacklisted_agents.items()
            if current_time > expire_time
        ]
        for ua in expired_agents:
            del self.blacklisted_agents[ua]
    
    def _get_available_agents(self, url: Optional[str] = None) -> List[str]:
        """
        获取可用的User-Agent列表（排除黑名单）
        
        Args:
            url: 目标URL
            
        Returns:
            可用的User-Agent列表
        """
        # 如果提供了URL，尝试获取网站特定配置
        if url:
            try:
                domain = urlparse(url).netloc
                site_config = get_site_config(domain)
                if 'user_agents' in site_config:
                    base_agents = site_config['user_agents']
                else:
                    base_agents = self.user_agents
            except Exception:
                base_agents = self.user_agents
        else:
            base_agents = self.user_agents
        
        # 排除黑名单中的User-Agent
        return [ua for ua in base_agents if ua not in self.blacklisted_agents]
    
    def blacklist_user_agent(self, user_agent: str, duration: Optional[int] = None) -> None:
        """
        将User-Agent加入黑名单
        
        Args:
            user_agent: 要加入黑名单的User-Agent
            duration: 黑名单持续时间（秒），默认使用配置值
        """
        if duration is None:
            duration = USER_AGENT_CONFIG['rotation_strategy']['blacklist_duration']
        
        expire_time = time.time() + duration
        self.blacklisted_agents[user_agent] = expire_time
        self.logger.warning(f"User-Agent已加入黑名单: {user_agent[:50]}...，持续时间: {duration}秒")
    
    def get_request_delay(self, url: Optional[str] = None) -> float:
        """
        获取推荐的请求延迟时间
        
        Args:
            url: 目标URL
            
        Returns:
            推荐的延迟时间（秒）
        """
        if url:
            try:
                domain = urlparse(url).netloc
                site_config = get_site_config(domain)
                min_interval = site_config.get('min_interval', 1)
                max_interval = site_config.get('max_interval', 3)
                return random.uniform(min_interval, max_interval)
            except Exception:
                pass
        
        return random.uniform(1, 3)


# 全局User-Agent管理器实例
_user_agent_manager: Optional[UserAgentManager] = None


def get_user_agent_manager(mode: str = 'random', site_type: str = 'general') -> UserAgentManager:
    """
    获取全局User-Agent管理器实例
    
    Args:
        mode: 轮换模式，仅在首次创建时有效
        site_type: 网站类型，仅在首次创建时有效
        
    Returns:
        UserAgentManager实例
    """
    global _user_agent_manager
    if _user_agent_manager is None:
        _user_agent_manager = UserAgentManager(mode, site_type)
    return _user_agent_manager


if __name__ == "__main__":
    # 测试代码
    import time
    
    # 测试随机模式
    print("=== 测试随机模式 ===")
    ua_manager = UserAgentManager('random')
    for i in range(5):
        print(f"随机UA {i+1}: {ua_manager.get_user_agent()}")
    
    print("\n=== 测试顺序模式 ===")
    ua_manager.set_mode('sequential')
    for i in range(5):
        print(f"顺序UA {i+1}: {ua_manager.get_user_agent()}")
    
    print("\n=== 测试移动端UA ===")
    for i in range(3):
        print(f"移动端UA {i+1}: {ua_manager.get_mobile_user_agent()}")
    
    print("\n=== 测试桌面端UA ===")
    for i in range(3):
        print(f"桌面端UA {i+1}: {ua_manager.get_desktop_user_agent()}")
    
    print(f"\n总共有 {ua_manager.get_user_agent_count()} 个User-Agent")