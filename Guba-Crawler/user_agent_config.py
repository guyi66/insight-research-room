#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2024/01/20
# @Author  : Assistant
# @File    : user_agent_config.py

"""
User-Agent配置文件
包含不同类型的User-Agent集合和配置选项
"""

# 高质量桌面端User-Agent（经常更新，模拟真实用户）
DESKTOP_USER_AGENTS = [
    # Chrome Windows (最新版本)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    
    # Chrome macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    
    # Firefox Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0',
    
    # Firefox macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:119.0) Gecko/20100101 Firefox/119.0',
    
    # Safari macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
    
    # Edge Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    
    # Linux Chrome
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    
    # Linux Firefox
    'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
]

# 移动端User-Agent
MOBILE_USER_AGENTS = [
    # iPhone Safari
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1',
    
    # iPhone Chrome
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.0.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/119.0.0.0 Mobile/15E148 Safari/604.1',
    
    # iPad Safari
    'Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    
    # Android Chrome
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 12; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36',
    
    # Android Samsung Internet
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/115.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 12; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/22.0 Chrome/114.0.0.0 Mobile Safari/537.36',
    
    # Android Firefox
    'Mozilla/5.0 (Mobile; rv:120.0) Gecko/120.0 Firefox/120.0',
    'Mozilla/5.0 (Mobile; rv:119.0) Gecko/119.0 Firefox/119.0',
]

# 专门针对中国网站优化的User-Agent
CHINESE_OPTIMIZED_USER_AGENTS = [
    # 常见的中国用户使用的浏览器
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    
    # QQ浏览器
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 QQBrowser/11.9.5355.400',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 QQBrowser/11.8.5250.400',
    
    # 360浏览器
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 360SE',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 360SE',
    
    # 搜狗浏览器
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 SE 2.X MetaSr 1.0',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 SE 2.X MetaSr 1.0',
]

# 低调的User-Agent（不容易被检测）
STEALTH_USER_AGENTS = [
    # 较老但仍然常见的版本
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
]

# User-Agent轮换配置
USER_AGENT_CONFIG = {
    # 默认模式：random（随机）或 sequential（顺序）
    'default_mode': 'random',
    
    # 默认使用的User-Agent集合
    'default_user_agents': DESKTOP_USER_AGENTS + CHINESE_OPTIMIZED_USER_AGENTS,
    
    # 针对不同网站的特殊配置
    'site_specific': {
        'guba.eastmoney.com': {
            'user_agents': CHINESE_OPTIMIZED_USER_AGENTS + STEALTH_USER_AGENTS,
            'mode': 'random',
            'min_interval': 1,  # 最小请求间隔（秒）
            'max_interval': 3,  # 最大请求间隔（秒）
        },
        'caifuhao.eastmoney.com': {
            'user_agents': DESKTOP_USER_AGENTS + MOBILE_USER_AGENTS,
            'mode': 'random',
            'min_interval': 0.5,
            'max_interval': 2,
        },
        # 可以添加更多网站的特殊配置
    },
    
    # 轮换策略配置
    'rotation_strategy': {
        'change_frequency': 'per_request',  # per_request, per_page, per_session
        'avoid_consecutive': True,  # 避免连续使用相同的User-Agent
        'blacklist_on_error': True,  # 出错时将User-Agent加入黑名单
        'blacklist_duration': 300,  # 黑名单持续时间（秒）
    },
    
    # 检测和规避配置
    'anti_detection': {
        'randomize_case': False,  # 随机化大小写（不推荐，可能导致问题）
        'add_random_spaces': False,  # 添加随机空格（不推荐）
        'use_real_versions': True,  # 使用真实的浏览器版本号
        'update_frequency': 'weekly',  # User-Agent更新频率
    }
}

# 获取特定网站的User-Agent配置
def get_site_config(domain: str) -> dict:
    """
    获取特定网站的User-Agent配置
    
    Args:
        domain: 网站域名
        
    Returns:
        网站特定的配置字典
    """
    return USER_AGENT_CONFIG['site_specific'].get(domain, {
        'user_agents': USER_AGENT_CONFIG['default_user_agents'],
        'mode': USER_AGENT_CONFIG['default_mode'],
        'min_interval': 1,
        'max_interval': 3,
    })

# 获取推荐的User-Agent列表
def get_recommended_user_agents(site_type: str = 'general') -> list:
    """
    获取推荐的User-Agent列表
    
    Args:
        site_type: 网站类型 ('general', 'chinese', 'mobile', 'stealth')
        
    Returns:
        User-Agent列表
    """
    if site_type == 'chinese':
        return CHINESE_OPTIMIZED_USER_AGENTS
    elif site_type == 'mobile':
        return MOBILE_USER_AGENTS
    elif site_type == 'stealth':
        return STEALTH_USER_AGENTS
    elif site_type == 'desktop':
        return DESKTOP_USER_AGENTS
    else:
        return DESKTOP_USER_AGENTS + CHINESE_OPTIMIZED_USER_AGENTS