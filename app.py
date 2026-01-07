"""
Flask主应用 - 统一管理三个Streamlit应用
"""

import csv
import html as _html
import io
import json
import os
import re
import sys
import subprocess
import time
import threading
import uuid
from datetime import datetime
from queue import Queue
from typing import Any, Dict, List
from flask import Flask, render_template, request, jsonify, Response, send_from_directory, redirect, url_for
from flask_socketio import SocketIO, emit
import atexit
import requests
from loguru import logger
import importlib
from pathlib import Path
from MindSpider.main import MindSpider
from analysis_pipeline import run_pipeline
import pandas as pd

try:
    import akshare as ak
    HAS_AKSHARE = True
except Exception:
    ak = None  # type: ignore
    HAS_AKSHARE = False

# Cache for stock name lookup
_STOCK_NAME_CACHE: Dict[str, Any] = {"ts": 0.0, "name_to_codes": {}, "code_to_name": {}}
_STOCK_NAME_LOCK = threading.Lock()
_STOCK_NAME_TTL = 6 * 3600

app = Flask(__name__)
app.config['SECRET_KEY'] = 'Dedicated-to-creating-a-concise-and-versatile-public-opinion-analysis-platform'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# 设置UTF-8编码环境
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUTF8'] = '1'

# 创建日志目录
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

ANALYSIS_JOBS = {}
ANALYSIS_LOCK = threading.Lock()

def _load_stock_name_map() -> Dict[str, Dict[str, Any]]:
    if not HAS_AKSHARE:
        return {"name_to_codes": {}, "code_to_name": {}}
    now = time.time()
    cache_dir = Path("reports") / "_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "stock_name_map.json"

    with _STOCK_NAME_LOCK:
        ts = float(_STOCK_NAME_CACHE.get("ts") or 0.0)
        if _STOCK_NAME_CACHE.get("name_to_codes") and now - ts < _STOCK_NAME_TTL:
            return {
                "name_to_codes": _STOCK_NAME_CACHE.get("name_to_codes") or {},
                "code_to_name": _STOCK_NAME_CACHE.get("code_to_name") or {},
            }
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                cached_ts = float(data.get("ts") or 0.0)
                if data.get("name_to_codes") and now - cached_ts < _STOCK_NAME_TTL:
                    _STOCK_NAME_CACHE.update({
                        "ts": cached_ts,
                        "name_to_codes": data.get("name_to_codes") or {},
                        "code_to_name": data.get("code_to_name") or {},
                    })
                    return {
                        "name_to_codes": _STOCK_NAME_CACHE.get("name_to_codes") or {},
                        "code_to_name": _STOCK_NAME_CACHE.get("code_to_name") or {},
                    }
            except Exception:
                pass

        name_to_codes: Dict[str, List[str]] = {}
        code_to_name: Dict[str, str] = {}
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                for row in df.to_dict(orient="records"):
                    name = str(row.get("名称") or row.get("name") or row.get("股票简称") or "").strip()
                    code = str(row.get("代码") or row.get("code") or row.get("股票代码") or "").strip()
                    if not name or not code:
                        continue
                    if code.isdigit() and len(code) == 6:
                        code = code.zfill(6)
                    name_to_codes.setdefault(name, []).append(code)
                    code_to_name[code] = name
        except Exception:
            name_to_codes = {}
            code_to_name = {}

        _STOCK_NAME_CACHE.update({
            "ts": now,
            "name_to_codes": name_to_codes,
            "code_to_name": code_to_name,
        })
        try:
            cache_path.write_text(
                json.dumps({"ts": now, "name_to_codes": name_to_codes, "code_to_name": code_to_name}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass

        return {"name_to_codes": name_to_codes, "code_to_name": code_to_name}


def _resolve_symbols_from_query(query: str) -> List[Dict[str, str]]:
    query = (query or "").strip()
    if not query:
        return []

    ordered_codes: List[str] = []
    seen = set()

    def _add_code(code: str) -> None:
        if not code:
            return
        if code in seen:
            return
        seen.add(code)
        ordered_codes.append(code)

    for match in re.finditer(r"(?i)(sh|sz|bj)[\s\.]?(\d{6})", query):
        _add_code(match.group(2))
    for match in re.finditer(r"\b\d{6}\b", query):
        _add_code(match.group(0))

    has_chinese = re.search(r"[\u4e00-\u9fa5]", query) is not None
    name_to_codes: Dict[str, List[str]] = {}
    code_to_name: Dict[str, str] = {}
    if has_chinese:
        maps = _load_stock_name_map()
        name_to_codes = maps.get("name_to_codes") or {}
        code_to_name = maps.get("code_to_name") or {}

        stripped = re.sub(r"\s+", "", query)
        name_hits: List[tuple[int, str]] = []
        for name in name_to_codes.keys():
            if not name or len(name) < 2:
                continue
            idx = stripped.find(name)
            if idx >= 0:
                name_hits.append((idx, name))
        name_hits.sort(key=lambda x: x[0])
        for _, name in name_hits:
            codes = name_to_codes.get(name) or []
            for code in codes[:1]:
                _add_code(code)

        if not name_hits:
            tokens = re.split(r"[，,、/\s]+", query)
            for token in tokens:
                token = token.strip()
                if token in ("分析", "帮我分析", "对比", "比较", "以及", "还有"):
                    continue
                codes = name_to_codes.get(token) or []
                for code in codes[:1]:
                    _add_code(code)

    if not code_to_name and ordered_codes:
        maps = _load_stock_name_map()
        code_to_name = maps.get("code_to_name") or {}

    results: List[Dict[str, str]] = []
    for code in ordered_codes:
        results.append({
            "symbol": code,
            "name": code_to_name.get(code, ""),
        })
    return results


def _to_brief_paragraph(md_text: str, max_len: int = 420) -> str:
    text = re.sub(r"[`#>*_\[\]]", " ", md_text or "")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    if len(text) > max_len:
        text = text[:max_len].rstrip() + "..."
    return text


def _load_brief_md(report_dir: Path, symbol: str, report_urls: Dict[str, str]) -> str:
    if not report_dir or not symbol:
        return ""
    filename_map = {
        "tech": f"{symbol}_tech_md.json",
        "fund": f"{symbol}_fund_md.json",
        "news": f"{symbol}_news_md.json",
    }
    for mode in ("fund", "news", "tech"):
        if mode not in report_urls:
            continue
        path = report_dir / "materials" / filename_map.get(mode, "")
        if not path.name:
            continue
        data = _read_json_file(path)
        if isinstance(data, dict):
            text = str(data.get("analysis_md") or data.get("md") or data.get("overview_md") or "").strip()
            if text:
                return text
    return ""


def _build_multi_stock_report(
    results: List[Dict[str, Any]],
    output_dir: str,
    job_id: str,
    query: str,
) -> str:
    out_dir = Path(output_dir or "reports")
    report_dir = out_dir / "multi" / job_id
    report_dir.mkdir(parents=True, exist_ok=True)

    buttons_html = []
    panels_html = []
    for idx, item in enumerate(results):
        symbol = item.get("symbol") or ""
        name = item.get("name") or ""
        label = f"{name}({symbol})" if name else symbol
        btn_id = f"btn-{symbol}"
        panel_id = f"panel-{symbol}"
        active_cls = " active" if idx == 0 else ""
        display = "" if idx == 0 else "display:none"

        overview_md = item.get("overview_md") or ""
        brief = _to_brief_paragraph(overview_md)
        brief_html = f"<p class='summary'>{_html.escape(brief)}</p>" if brief else "<p class='summary'>暂无摘要</p>"

        report_urls = item.get("report_urls") or {}
        label_map = {
            "combined": "综合",
            "tech": "技术面",
            "fund": "基本面",
            "news": "消息面",
        }
        ordered_modes = [m for m in ("combined", "fund", "tech", "news") if m in report_urls]
        report_links = [
            f"<a class='link' href='{report_urls[m]}' target='_blank'>{label_map.get(m, m)}</a>"
            for m in ordered_modes
        ]
        report_link = " ".join(report_links)

        buttons_html.append(
            f"<button id='{btn_id}' class='btn{active_cls}' onclick=\"showPanel('{symbol}')\">{_html.escape(label)}</button>"
        )
        panels_html.append(
            f"""
            <div id='{panel_id}' class='panel' style='{display}'>
              <h2>{_html.escape(label)}</h2>
              {brief_html}
              <div class='links'>{report_link}</div>
            </div>
            """
        )

    title = "多股票分析"
    html = f"""
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>{_html.escape(title)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; max-width: 1100px; margin: auto; padding: 24px; line-height:1.7; color:#222 }}
.toolbar {{ display:flex; flex-wrap:wrap; gap:8px; margin:12px 0 20px }}
.btn {{ padding:8px 14px; border-radius:6px; border:1px solid #ddd; cursor:pointer; background:#fff }}
.btn.active {{ background:#0078D4; color:#fff; border-color:#0078D4 }}
.panel {{ border:1px solid #eee; padding:16px; border-radius:6px; background:#fff; margin-bottom:16px }}
.summary {{ font-size:16px; }}
.links {{ margin-top:12px }}
.link {{ color:#0078D4; text-decoration:none; }}
.link:hover {{ text-decoration:underline; }}
</style>
</head>
<body>
<h1>{_html.escape(title)}</h1>
<p>查询：{_html.escape(query)}</p>
<div class='toolbar'>
  {''.join(buttons_html)}
</div>
{''.join(panels_html)}
<script>
function showPanel(symbol) {{
  document.querySelectorAll('.panel').forEach((el) => {{
    el.style.display = (el.id === 'panel-' + symbol) ? '' : 'none';
  }});
  document.querySelectorAll('.btn').forEach((el) => {{
    el.classList.toggle('active', el.id === 'btn-' + symbol);
  }});
}}
</script>
</body>
</html>
"""
    out_path = report_dir / "multi_report.html"
    out_path.write_text(html, encoding="utf-8")
    return str(out_path)

CONFIG_MODULE_NAME = 'config'
CONFIG_FILE_PATH = Path(__file__).resolve().parent / 'config.py'
CONFIG_KEYS = [
    'HOST',
    'PORT',
    'DB_DIALECT',
    'DB_HOST',
    'DB_PORT',
    'DB_USER',
    'DB_PASSWORD',
    'DB_NAME',
    'DB_CHARSET',
    'INSIGHT_ENGINE_API_KEY',
    'INSIGHT_ENGINE_BASE_URL',
    'INSIGHT_ENGINE_MODEL_NAME',
    'MEDIA_ENGINE_API_KEY',
    'MEDIA_ENGINE_BASE_URL',
    'MEDIA_ENGINE_MODEL_NAME',
    'FORUM_HOST_API_KEY',
    'FORUM_HOST_BASE_URL',
    'FORUM_HOST_MODEL_NAME',
    'KEYWORD_OPTIMIZER_API_KEY',
    'KEYWORD_OPTIMIZER_BASE_URL',
    'KEYWORD_OPTIMIZER_MODEL_NAME',
    'TAVILY_API_KEY',
    'BOCHA_WEB_SEARCH_API_KEY'
]


def _load_config_module():
    """Load or reload the config module to ensure latest values are available."""
    importlib.invalidate_caches()
    module = sys.modules.get(CONFIG_MODULE_NAME)
    try:
        if module is None:
            module = importlib.import_module(CONFIG_MODULE_NAME)
        else:
            module = importlib.reload(module)
    except ModuleNotFoundError:
        return None
    return module


def read_config_values():
    """Return the current configuration values that are exposed to the frontend."""
    try:
        # 重新加载配置以获取最新的 Settings 实例
        from config import reload_settings, settings
        reload_settings()
        
        values = {}
        for key in CONFIG_KEYS:
            # 从 Pydantic Settings 实例读取值
            value = getattr(settings, key, None)
            # Convert to string for uniform handling on the frontend.
            if value is None:
                values[key] = ''
            else:
                values[key] = str(value)
        return values
    except Exception as exc:
        logger.exception(f"读取配置失败: {exc}")
        return {}


def _serialize_config_value(value):
    """Serialize Python values back to a config.py assignment-friendly string."""
    if isinstance(value, bool):
        return 'True' if value else 'False'
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return 'None'

    value_str = str(value)
    escaped = value_str.replace('\\', '\\\\').replace('"', '\\"')
    return f'"{escaped}"'


def write_config_values(updates):
    """Persist configuration updates to .env file (Pydantic Settings source)."""
    from pathlib import Path
    
    # 确定 .env 文件路径（与 config.py 中的逻辑一致）
    project_root = Path(__file__).resolve().parent
    cwd_env = Path.cwd() / ".env"
    env_file_path = cwd_env if cwd_env.exists() else (project_root / ".env")
    
    # 读取现有的 .env 文件内容
    env_lines = []
    env_key_indices = {}  # 记录每个键在文件中的索引位置
    if env_file_path.exists():
        env_lines = env_file_path.read_text(encoding='utf-8').splitlines()
        # 提取已存在的键及其索引
        for i, line in enumerate(env_lines):
            line_stripped = line.strip()
            if line_stripped and not line_stripped.startswith('#'):
                if '=' in line_stripped:
                    key = line_stripped.split('=')[0].strip()
                    env_key_indices[key] = i
    
    # 更新或添加配置项
    for key, raw_value in updates.items():
        # 格式化值用于 .env 文件（不需要引号，除非是字符串且包含空格）
        if raw_value is None or raw_value == '':
            env_value = ''
        elif isinstance(raw_value, (int, float)):
            env_value = str(raw_value)
        elif isinstance(raw_value, bool):
            env_value = 'True' if raw_value else 'False'
        else:
            value_str = str(raw_value)
            # 如果包含空格或特殊字符，需要引号
            if ' ' in value_str or '\n' in value_str or '#' in value_str:
                escaped = value_str.replace('\\', '\\\\').replace('"', '\\"')
                env_value = f'"{escaped}"'
            else:
                env_value = value_str
        
        # 更新或添加配置项
        if key in env_key_indices:
            # 更新现有行
            env_lines[env_key_indices[key]] = f'{key}={env_value}'
        else:
            # 添加新行到文件末尾
            env_lines.append(f'{key}={env_value}')
    
    # 写入 .env 文件
    env_file_path.parent.mkdir(parents=True, exist_ok=True)
    env_file_path.write_text('\n'.join(env_lines) + '\n', encoding='utf-8')
    
    # 重新加载配置模块（这会重新读取 .env 文件并创建新的 Settings 实例）
    _load_config_module()


system_state_lock = threading.Lock()
system_state = {
    'started': False,
    'starting': False
}


def _set_system_state(*, started=None, starting=None):
    """Safely update the cached system state flags."""
    with system_state_lock:
        if started is not None:
            system_state['started'] = started
        if starting is not None:
            system_state['starting'] = starting


def _get_system_state():
    """Return a shallow copy of the system state flags."""
    with system_state_lock:
        return system_state.copy()


def _prepare_system_start():
    """Mark the system as starting if it is not already running or starting."""
    with system_state_lock:
        if system_state['started']:
            return False, '系统已启动'
        if system_state['starting']:
            return False, '系统正在启动'
        system_state['starting'] = True
        return True, None


def initialize_system_components():
    """Start system components (Streamlit apps, ForumEngine)."""
    logs = []
    errors = []
    
    spider = MindSpider()
    if spider.initialize_database():
        logger.info("数据库初始化成功")
    else:
        logger.error("数据库初始化失败")

    try:
        stop_forum_engine()
        logs.append("已停止 ForumEngine 监控器以避免文件冲突")
    except Exception as exc:  # pragma: no cover - 安全捕获
        message = f"停止 ForumEngine 时发生异常: {exc}"
        logs.append(message)
        logger.exception(message)

    processes['forum']['status'] = 'stopped'

    for app_name, script_path in STREAMLIT_SCRIPTS.items():
        logs.append(f"检查文件: {script_path}")
        if os.path.exists(script_path):
            success, message = start_streamlit_app(app_name, script_path, processes[app_name]['port'])
            logs.append(f"{app_name}: {message}")
            if success:
                startup_success, startup_message = wait_for_app_startup(app_name, 30)
                logs.append(f"{app_name} 启动检查: {startup_message}")
                if not startup_success:
                    errors.append(f"{app_name} 启动失败: {startup_message}")
            else:
                errors.append(f"{app_name} 启动失败: {message}")
        else:
            msg = f"文件不存在: {script_path}"
            logs.append(f"错误: {msg}")
            errors.append(f"{app_name}: {msg}")

    forum_started = False
    try:
        start_forum_engine()
        processes['forum']['status'] = 'running'
        logs.append("ForumEngine 启动完成")
        forum_started = True
    except Exception as exc:  # pragma: no cover - 保底捕获
        error_msg = f"ForumEngine 启动失败: {exc}"
        logs.append(error_msg)
        errors.append(error_msg)

    if errors:
        cleanup_processes()
        processes['forum']['status'] = 'stopped'
        if forum_started:
            try:
                stop_forum_engine()
            except Exception:  # pragma: no cover
                logger.exception("停止ForumEngine失败")
        return False, logs, errors

    return True, logs, []

# 初始化ForumEngine的forum.log文件
def init_forum_log():
    """初始化forum.log文件"""
    try:
        forum_log_file = LOG_DIR / "forum.log"
        # 检查文件不存在则创建并且写一个开始，存在就清空写一个开始
        if not forum_log_file.exists():
            with open(forum_log_file, 'w', encoding='utf-8') as f:
                start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"=== ForumEngine 系统初始化 - {start_time} ===\n")
            logger.info(f"ForumEngine: forum.log 已初始化")
        else:
            with open(forum_log_file, 'w', encoding='utf-8') as f:
                start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"=== ForumEngine 系统初始化 - {start_time} ===\n")
            logger.info(f"ForumEngine: forum.log 已初始化")
    except Exception as e:
        logger.exception(f"ForumEngine: 初始化forum.log失败: {e}")

# 初始化forum.log
init_forum_log()

# 启动ForumEngine智能监控
def start_forum_engine():
    """启动ForumEngine论坛"""
    try:
        from ForumEngine.monitor import start_forum_monitoring
        logger.info("ForumEngine: 启动论坛...")
        success = start_forum_monitoring()
        if not success:
            logger.info("ForumEngine: 论坛启动失败")
    except Exception as e:
        logger.exception(f"ForumEngine: 启动论坛失败: {e}")

# 停止ForumEngine智能监控
def stop_forum_engine():
    """停止ForumEngine论坛"""
    try:
        from ForumEngine.monitor import stop_forum_monitoring
        logger.info("ForumEngine: 停止论坛...")
        stop_forum_monitoring()
        logger.info("ForumEngine: 论坛已停止")
    except Exception as e:
        logger.exception(f"ForumEngine: 停止论坛失败: {e}")

def parse_forum_log_line(line):
    """解析forum.log行内容，提取对话信息"""
    import re
    
    # 匹配格式: [时间] [来源] 内容
    pattern = r'\[(\d{2}:\d{2}:\d{2})\]\s*\[([A-Z]+)\]\s*(.*)'
    match = re.match(pattern, line)
    
    if match:
        timestamp, source, content = match.groups()
        
        # 过滤掉系统消息和空内容
        if source == 'SYSTEM' or not content.strip():
            return None
        
        # 只处理三个Engine的消息
        if source not in ['QUERY', 'INSIGHT', 'MEDIA']:
            return None
        
        # 根据来源确定消息类型和发送者
        message_type = 'agent'
        sender = f'{source} Engine'
        
        return {
            'type': message_type,
            'sender': sender,
            'content': content.strip(),
            'timestamp': timestamp,
            'source': source
        }
    
    return None

# Forum日志监听器
def monitor_forum_log():
    """监听forum.log文件变化并推送到前端"""
    import time
    from pathlib import Path
    
    forum_log_file = LOG_DIR / "forum.log"
    last_position = 0
    processed_lines = set()  # 用于跟踪已处理的行，避免重复
    
    # 如果文件存在，获取初始位置
    if forum_log_file.exists():
        with open(forum_log_file, 'r', encoding='utf-8', errors='ignore') as f:
            # 初始化时读取所有现有行，避免重复处理
            existing_lines = f.readlines()
            for line in existing_lines:
                line_hash = hash(line.strip())
                processed_lines.add(line_hash)
            last_position = f.tell()
    
    while True:
        try:
            if forum_log_file.exists():
                with open(forum_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    f.seek(last_position)
                    new_lines = f.readlines()
                    
                    if new_lines:
                        for line in new_lines:
                            line = line.rstrip('\n\r')
                            if line.strip():
                                line_hash = hash(line.strip())
                                
                                # 避免重复处理同一行
                                if line_hash in processed_lines:
                                    continue
                                
                                processed_lines.add(line_hash)
                                
                                # 解析日志行并发送forum消息
                                parsed_message = parse_forum_log_line(line)
                                if parsed_message:
                                    socketio.emit('forum_message', parsed_message)
                                
                                # 只有在控制台显示forum时才发送控制台消息
                                timestamp = datetime.now().strftime('%H:%M:%S')
                                formatted_line = f"[{timestamp}] {line}"
                                socketio.emit('console_output', {
                                    'app': 'forum',
                                    'line': formatted_line
                                })
                        
                        last_position = f.tell()
                        
                        # 清理processed_lines集合，避免内存泄漏（保留最近1000行的哈希）
                        if len(processed_lines) > 1000:
                            processed_lines.clear()
            
            time.sleep(1)  # 每秒检查一次
        except Exception as e:
            logger.error(f"Forum日志监听错误: {e}")
            time.sleep(5)

# 启动Forum日志监听线程
forum_monitor_thread = threading.Thread(target=monitor_forum_log, daemon=True)
forum_monitor_thread.start()

# 全局变量存储进程信息
processes = {
    'insight': {'process': None, 'port': 8501, 'status': 'stopped', 'output': [], 'log_file': None},
    'forum': {'process': None, 'port': None, 'status': 'stopped', 'output': [], 'log_file': None}  # 启动后标记为 running
}

STREAMLIT_SCRIPTS = {
    'insight': 'SingleEngineApp/insight_engine_streamlit_app.py',
}

# 输出队列
output_queues = {
    'insight': Queue(),
    'forum': Queue()
}

def write_log_to_file(app_name, line):
    """将日志写入文件"""
    try:
        log_file_path = LOG_DIR / f"{app_name}.log"
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
            f.flush()
    except Exception as e:
        logger.error(f"Error writing log for {app_name}: {e}")

def read_log_from_file(app_name, tail_lines=None):
    """从文件读取日志"""
    try:
        log_file_path = LOG_DIR / f"{app_name}.log"
        if not log_file_path.exists():
            return []
        
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            lines = [line.rstrip('\n\r') for line in lines if line.strip()]
            
            if tail_lines:
                return lines[-tail_lines:]
            return lines
    except Exception as e:
        logger.exception(f"Error reading log for {app_name}: {e}")
        return []

def read_process_output(process, app_name):
    """读取进程输出并写入文件"""
    import select
    import sys
    
    while True:
        try:
            if process.poll() is not None:
                # 进程结束，读取剩余输出
                remaining_output = process.stdout.read()
                if remaining_output:
                    lines = remaining_output.decode('utf-8', errors='replace').split('\n')
                    for line in lines:
                        line = line.strip()
                        if line:
                            timestamp = datetime.now().strftime('%H:%M:%S')
                            formatted_line = f"[{timestamp}] {line}"
                            write_log_to_file(app_name, formatted_line)
                            socketio.emit('console_output', {
                                'app': app_name,
                                'line': formatted_line
                            })
                break
            
            # 使用非阻塞读取
            if sys.platform == 'win32':
                # Windows下使用不同的方法
                output = process.stdout.readline()
                if output:
                    line = output.decode('utf-8', errors='replace').strip()
                    if line:
                        timestamp = datetime.now().strftime('%H:%M:%S')
                        formatted_line = f"[{timestamp}] {line}"
                        
                        # 写入日志文件
                        write_log_to_file(app_name, formatted_line)
                        
                        # 发送到前端
                        socketio.emit('console_output', {
                            'app': app_name,
                            'line': formatted_line
                        })
                else:
                    # 没有输出时短暂休眠
                    time.sleep(0.1)
            else:
                # Unix系统使用select
                ready, _, _ = select.select([process.stdout], [], [], 0.1)
                if ready:
                    output = process.stdout.readline()
                    if output:
                        line = output.decode('utf-8', errors='replace').strip()
                        if line:
                            timestamp = datetime.now().strftime('%H:%M:%S')
                            formatted_line = f"[{timestamp}] {line}"
                            
                            # 写入日志文件
                            write_log_to_file(app_name, formatted_line)
                            
                            # 发送到前端
                            socketio.emit('console_output', {
                                'app': app_name,
                                'line': formatted_line
                            })
                            
        except Exception as e:
            error_msg = f"Error reading output for {app_name}: {e}"
            logger.exception(error_msg)
            write_log_to_file(app_name, f"[{datetime.now().strftime('%H:%M:%S')}] {error_msg}")
            break

def start_streamlit_app(app_name, script_path, port):
    """启动Streamlit应用"""
    try:
        if processes[app_name]['process'] is not None:
            return False, "应用已经在运行"
        
        # 检查文件是否存在
        if not os.path.exists(script_path):
            return False, f"文件不存在: {script_path}"
        
        # 清空之前的日志文件
        log_file_path = LOG_DIR / f"{app_name}.log"
        if log_file_path.exists():
            log_file_path.unlink()
        
        # 创建启动日志
        start_msg = f"[{datetime.now().strftime('%H:%M:%S')}] 启动 {app_name} 应用..."
        write_log_to_file(app_name, start_msg)
        
        cmd = [
            sys.executable, '-m', 'streamlit', 'run',
            script_path,
            '--server.port', str(port),
            '--server.headless', 'true',
            '--browser.gatherUsageStats', 'false',
            # '--logger.level', 'debug',  # 增加日志详细程度
            '--logger.level', 'info',
            '--server.enableCORS', 'false'
        ]
        
        # 设置环境变量确保UTF-8编码和减少缓冲
        env = os.environ.copy()
        env.update({
            'PYTHONIOENCODING': 'utf-8',
            'PYTHONUTF8': '1',
            'LANG': 'en_US.UTF-8',
            'LC_ALL': 'en_US.UTF-8',
            'PYTHONUNBUFFERED': '1',  # 禁用Python缓冲
            'STREAMLIT_BROWSER_GATHER_USAGE_STATS': 'false'
        })
        
        # 使用当前工作目录而不是脚本目录
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,  # 无缓冲
            universal_newlines=False,
            cwd=os.getcwd(),
            env=env,
            encoding=None,  # 让我们手动处理编码
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        processes[app_name]['process'] = process
        processes[app_name]['status'] = 'starting'
        processes[app_name]['output'] = []
        
        # 启动输出读取线程
        output_thread = threading.Thread(
            target=read_process_output,
            args=(process, app_name),
            daemon=True
        )
        output_thread.start()
        
        return True, f"{app_name} 应用启动中..."
        
    except Exception as e:
        error_msg = f"启动失败: {str(e)}"
        write_log_to_file(app_name, f"[{datetime.now().strftime('%H:%M:%S')}] {error_msg}")
        return False, error_msg

def stop_streamlit_app(app_name):
    """停止Streamlit应用"""
    try:
        if processes[app_name]['process'] is None:
            return False, "应用未运行"
        
        process = processes[app_name]['process']
        process.terminate()
        
        # 等待进程结束
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        
        processes[app_name]['process'] = None
        processes[app_name]['status'] = 'stopped'
        
        return True, f"{app_name} 应用已停止"
        
    except Exception as e:
        return False, f"停止失败: {str(e)}"

HEALTHCHECK_PATH = "/_stcore/health"
HEALTHCHECK_PROXIES = {'http': None, 'https': None}


def _build_healthcheck_url(port):
    return f"http://127.0.0.1:{port}{HEALTHCHECK_PATH}"


def check_app_status():
    """检查应用状态"""
    for app_name, info in processes.items():
        if info['process'] is not None:
            if info['process'].poll() is None:
                # 进程仍在运行，检查端口是否可访问
                try:
                    response = requests.get(
                        _build_healthcheck_url(info['port']),
                        timeout=2,
                        proxies=HEALTHCHECK_PROXIES
                    )
                    if response.status_code == 200:
                        info['status'] = 'running'
                    else:
                        info['status'] = 'starting'
                except Exception as exc:
                    logger.warning(f"{app_name} 健康检查失败: {exc}")
                    info['status'] = 'starting'
            else:
                # 进程已结束
                info['process'] = None
                info['status'] = 'stopped'

def wait_for_app_startup(app_name, max_wait_time=90):
    """等待应用启动完成"""
    import time
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        info = processes[app_name]
        if info['process'] is None:
            return False, "进程已停止"
        
        if info['process'].poll() is not None:
            return False, "进程启动失败"
        
        try:
            response = requests.get(
                _build_healthcheck_url(info['port']),
                timeout=2,
                proxies=HEALTHCHECK_PROXIES
            )
            if response.status_code == 200:
                info['status'] = 'running'
                return True, "启动成功"
        except Exception as exc:
            logger.warning(f"{app_name} 健康检查失败: {exc}")

        time.sleep(1)

    return False, "启动超时"

def cleanup_processes():
    """清理所有进程"""
    for app_name in STREAMLIT_SCRIPTS:
        stop_streamlit_app(app_name)

    processes['forum']['status'] = 'stopped'
    try:
        stop_forum_engine()
    except Exception:  # pragma: no cover
        logger.exception("停止ForumEngine失败")
    _set_system_state(started=False, starting=False)

# 注册清理函数
atexit.register(cleanup_processes)

@app.route('/')
def index():
    """主页"""
    return redirect(url_for('insight_page'))


@app.route('/analysis')
def analysis_page():
    """analysis page"""
    return redirect(url_for('insight_page'))


@app.route('/insight')
def insight_page():
    """insight page"""
    return render_template('insight.html')


@app.route('/reports/<path:filename>')
def reports_file(filename):
    """serve generated reports"""
    reports_dir = Path('reports').resolve()
    return send_from_directory(reports_dir, filename)


def _append_analysis_log(job_id: str, message, agent: str | None = None, status: str | None = None) -> None:
    with ANALYSIS_LOCK:
        job = ANALYSIS_JOBS.get(job_id)
        if not job:
            return
        entry = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "message": "",
            "agent": agent,
            "status": status,
        }
        if isinstance(message, dict):
            entry.update(message)
        else:
            entry["message"] = str(message)
        job["log"].append(entry)
        if entry.get("agent") and entry.get("status"):
            agents = job.setdefault("agents", {})
            agents[str(entry["agent"])] = str(entry["status"])
        if len(job["log"]) > 300:
            job["log"] = job["log"][-300:]
        job["updated_at"] = datetime.now().isoformat(timespec="seconds")


def _normalize_chart_timeframes(raw) -> List[str]:
    default = ["day", "week", "month", "year", "60m", "30m", "15m", "5m"]
    if raw is None:
        return default
    items: List[str] = []
    if isinstance(raw, list):
        items = [str(item).strip().lower() for item in raw if str(item).strip()]
    else:
        items = [item.strip().lower() for item in str(raw).split(",") if item.strip()]
    if not items:
        return default

    alias_map = {
        "day": "day",
        "daily": "day",
        "d": "day",
        "1d": "day",
        "week": "week",
        "weekly": "week",
        "w": "week",
        "1w": "week",
        "month": "month",
        "monthly": "month",
        "mon": "month",
        "1mon": "month",
        "year": "year",
        "yearly": "year",
        "y": "year",
        "1y": "year",
        "60m": "60m",
        "60": "60m",
        "1h": "60m",
        "30m": "30m",
        "30": "30m",
        "15m": "15m",
        "15": "15m",
        "5m": "5m",
        "5": "5m",
    }
    normalized: List[str] = []
    for item in items:
        key = alias_map.get(item)
        if key and key not in normalized:
            normalized.append(key)
    return normalized or default


def _build_chart_output_dir(symbol: str, output_dir: str | None = None) -> Path:
    base_dir = Path(output_dir).resolve() if output_dir else Path("reports") / symbol / "interactive"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def _attach_datetime_index(df):
    import pandas as pd

    df = df.copy()
    if "date" not in df.columns:
        return df
    if "time" in df.columns:
        time_str = df["time"].astype(str).str.zfill(6)
        time_fmt = time_str.str.slice(0, 2) + ":" + time_str.str.slice(2, 4) + ":" + time_str.str.slice(4, 6)
        dt_str = df["date"].dt.strftime("%Y-%m-%d") + " " + time_fmt
        df["dt"] = pd.to_datetime(dt_str, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    else:
        df["dt"] = df["date"]
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df


def _add_kdj(df):
    import pandas as pd
    import numpy as np

    df = df.copy()
    low = pd.to_numeric(df["low"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    low_n = low.rolling(window=9).min()
    high_n = high.rolling(window=9).max()
    denom = (high_n - low_n).replace(0, np.nan)
    rsv = (close - low_n) / denom * 100
    rsv = pd.to_numeric(rsv, errors="coerce")
    k = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    d = k.ewm(alpha=1 / 3, adjust=False).mean()
    j = 3 * k - 2 * d
    df["kdj_k"] = k
    df["kdj_d"] = d
    df["kdj_j"] = j
    return df


def _resample_yearly(df):
    df = df.copy()
    if "date" not in df.columns:
        return df
    df = df.dropna(subset=["date"]).sort_values("date")
    df["year"] = df["date"].dt.year
    grouped = df.groupby("year", sort=True)
    yearly = grouped.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        date=("date", "max"),
    ).reset_index(drop=True)
    yearly["dt"] = yearly["date"]
    return yearly


def _build_plotly_figure(df, symbol: str, title_suffix: str):
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go

    x = df["dt"]
    up = df["close"] >= df["open"]
    vol_colors = ["#ef5350" if flag else "#26a69a" for flag in up]
    macd_colors = ["#ef5350" if val >= 0 else "#26a69a" for val in df["macd_hist"].fillna(0)]

    fig = make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.45, 0.15, 0.15, 0.12, 0.13],
        specs=[[{"type": "candlestick"}], [{"type": "bar"}], [{"type": "xy"}], [{"type": "xy"}], [{"type": "xy"}]],
    )

    fig.add_trace(
        go.Candlestick(
            x=x,
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Kline",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(go.Scatter(x=x, y=df["ma5"], name="MA5", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["ma10"], name="MA10", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["ma20"], name="MA20", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["ma60"], name="MA60", line=dict(width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["boll_upper"], name="BOLL U", line=dict(width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["boll_mid"], name="BOLL M", line=dict(width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["boll_lower"], name="BOLL L", line=dict(width=1, dash="dot")), row=1, col=1)

    fig.add_trace(go.Bar(x=x, y=df["volume"], name="Volume", marker_color=vol_colors), row=2, col=1)

    fig.add_trace(go.Bar(x=x, y=df["macd_hist"], name="MACD Hist", marker_color=macd_colors), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["macd_diff"], name="MACD Diff", line=dict(width=1)), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["macd_dea"], name="MACD DEA", line=dict(width=1)), row=3, col=1)

    fig.add_trace(go.Scatter(x=x, y=df["rsi14"], name="RSI14", line=dict(width=1)), row=4, col=1)

    fig.add_trace(go.Scatter(x=x, y=df["kdj_k"], name="K", line=dict(width=1)), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["kdj_d"], name="D", line=dict(width=1)), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["kdj_j"], name="J", line=dict(width=1)), row=5, col=1)

    fig.update_layout(
        title=f"{symbol} {title_suffix} Kline",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=900,
        margin=dict(l=40, r=20, t=60, b=40),
        xaxis_rangeslider_visible=False,
        dragmode="pan",
    )
    return fig


def _generate_interactive_charts(symbol: str, timeframes: List[str], output_dir: str | None = None) -> Dict[str, Any]:
    from finance_tools.aitrados_client import FinanceMCPClient

    timeframe_map = {
        "day": "DAY",
        "week": "WEEK",
        "month": "MON",
        "60m": "60m",
        "30m": "30m",
        "15m": "15m",
        "5m": "5m",
    }
    label_map = {
        "day": "Daily",
        "week": "Weekly",
        "month": "Monthly",
        "year": "Yearly",
        "60m": "60m",
        "30m": "30m",
        "15m": "15m",
        "5m": "5m",
    }

    client = FinanceMCPClient(debug=False)
    start_date = "1990-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    base_dir = _build_chart_output_dir(symbol, output_dir)
    urls: Dict[str, str] = {}
    errors: Dict[str, str] = {}

    for tf_key in timeframes:
        file_path = base_dir / f"{symbol}_{tf_key}.html"
        try:
            if tf_key == "year":
                ohlc = client.get_ohlc(
                    symbol=symbol,
                    timeframe="MON",
                    limit=0,
                    start_date=start_date,
                    end_date=end_date,
                )
                df = client._ohlc_dict_to_df(ohlc)
                df = _resample_yearly(df)
                df = _attach_datetime_index(df)
            else:
                basic_fields = tf_key == "day"
                ohlc = client.get_ohlc(
                    symbol=symbol,
                    timeframe=timeframe_map[tf_key],
                    limit=0,
                    start_date=start_date,
                    end_date=end_date,
                    basic_fields=basic_fields,
                )
                df = client._ohlc_dict_to_df(ohlc)
                df = _attach_datetime_index(df)

            if df.empty:
                raise RuntimeError("empty OHLC data")

            ind_df = client._compute_indicators_from_df(df)
            if "dt" in df.columns:
                ind_df["dt"] = df["dt"].values
            ind_df = _add_kdj(ind_df)

            fig = _build_plotly_figure(ind_df, symbol, label_map.get(tf_key, tf_key))
            html = fig.to_html(include_plotlyjs="cdn", full_html=True)

            file_path.write_text(html, encoding="utf-8")
            urls[tf_key] = f"/reports/{symbol}/interactive/{file_path.name}"
        except Exception as exc:
            errors[tf_key] = str(exc)
            if file_path.exists() and tf_key not in urls:
                urls[tf_key] = f"/reports/{symbol}/interactive/{file_path.name}"

    return {"urls": urls, "errors": errors}


def _read_json_file(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _flatten_payload(obj: Any, prefix: str, rows: List[Dict[str, Any]], symbol: str) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_payload(value, next_prefix, rows, symbol)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            next_prefix = f"{prefix}[{idx}]"
            _flatten_payload(value, next_prefix, rows, symbol)
    else:
        rows.append({
            "section": "fund",
            "symbol": symbol,
            "path": prefix,
            "value": obj,
        })


def _normalize_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join([str(v) for v in value if v is not None])
    return value


def _collect_csv_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
    preferred = [
        "section",
        "symbol",
        "timeframe",
        "date",
        "dt",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "macd_diff",
        "macd_dea",
        "macd_hist",
        "rsi14",
        "boll_mid",
        "boll_upper",
        "boll_lower",
        "platform",
        "published_at",
        "title",
        "content",
        "snippet",
        "url",
        "author",
        "match_count",
        "engagement",
        "score",
        "keyword",
        "source_table",
        "time_source",
        "media_urls",
        "path",
        "value",
        "keywords",
        "db_error",
    ]
    fieldnames = []
    seen = set()
    for key in preferred:
        if key not in seen:
            fieldnames.append(key)
            seen.add(key)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    return fieldnames


def _rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    fieldnames = _collect_csv_fieldnames(rows)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        normalized = {k: _normalize_csv_value(v) for k, v in row.items()}
        writer.writerow(normalized)
    return output.getvalue()


def _rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    fieldnames = _collect_csv_fieldnames(rows)
    normalized_rows = [{k: _normalize_csv_value(v) for k, v in row.items()} for row in rows]
    df = pd.DataFrame(normalized_rows)
    for col in fieldnames:
        if col not in df.columns:
            df[col] = ""
    return df[fieldnames]


def _run_analysis_job(job_id: str, payload: Dict[str, Any]) -> None:
    try:
        action = (payload.get("action") or "analysis").lower()
        report_modes = payload.get("report_modes")
        run_reports = action != "crawler"
        modes_list: List[str] = []
        if report_modes:
            if isinstance(report_modes, list):
                modes_list = [str(item).strip().lower() for item in report_modes if str(item).strip()]
            else:
                modes_list = [m.strip().lower() for m in str(report_modes).split(",") if m.strip()]
        else:
            modes_list = [str(payload.get("report_mode", "combined")).strip().lower()]
        if run_reports:
            run_combined = "combined" in modes_list
            run_tech = "tech" in modes_list or run_combined
            run_fund = "fund" in modes_list or run_combined
            run_news = "news" in modes_list or run_combined
            run_orchestrator = True
            run_critic = run_combined or sum([run_tech, run_fund, run_news]) >= 2
            agent_status = {
                "orchestrator": "pending" if run_orchestrator else "disabled",
                "tech": "pending" if run_tech else "disabled",
                "fund": "pending" if run_fund else "disabled",
                "news": "pending" if run_news else "disabled",
                "critic": "pending" if run_critic else "disabled",
                "combined": "pending" if run_combined else "disabled",
            }
        else:
            agent_status = {
                "orchestrator": "disabled",
                "tech": "disabled",
                "fund": "disabled",
                "news": "disabled",
                "critic": "disabled",
                "combined": "disabled",
            }
        with ANALYSIS_LOCK:
            job = ANALYSIS_JOBS.get(job_id, {})
            job["agents"] = agent_status
            ANALYSIS_JOBS[job_id] = job
        mindspider_keywords = payload.get("mindspider_keywords")
        guba_keywords = payload.get("guba_keywords") or mindspider_keywords
        symbols = payload.get("symbols")
        if isinstance(symbols, list) and len(symbols) > 1:
            results = []
            for item in symbols:
                sym = str(item.get("symbol") or "").strip()
                if not sym:
                    continue
                res = run_pipeline(
                    symbol=sym,
                    report_mode=payload.get("report_mode", "combined"),
                    report_modes=report_modes,
                    stock_name=item.get("name") or payload.get("stock_name"),
                    news_days=payload.get("news_days", 7),
                    use_bocha=payload.get("use_bocha", False),
                    include_realtime=payload.get("include_realtime", True),
                    output_dir=payload.get("output_dir"),
                    env_overrides=payload.get("env_overrides"),
                    db_overrides=payload.get("db_overrides"),
                    agent_overrides=payload.get("agent_overrides"),
                    crawl_mindspider=payload.get("crawl_mindspider", False),
                    mindspider_platforms=payload.get("mindspider_platforms"),
                    mindspider_days=payload.get("mindspider_days", 1),
                    mindspider_max_notes=payload.get("mindspider_max_notes", 10),
                    mindspider_dir=payload.get("mindspider_dir"),
                    mindspider_keywords=mindspider_keywords,
                    mindspider_no_crawl=payload.get("mindspider_no_crawl", False),
                    crawl_guba=payload.get("crawl_guba", False),
                    guba_keywords=guba_keywords,
                    guba_dir=payload.get("guba_dir"),
                    guba_pages=payload.get("guba_pages", 10),
                    guba_fulltext=payload.get("guba_fulltext", False),
                    guba_fulltext_days=payload.get("guba_fulltext_days", 7),
                    guba_fulltext_limit=payload.get("guba_fulltext_limit", 200),
                    guba_init_schema=payload.get("guba_init_schema", False),
                    run_reports=run_reports,
                    log=lambda msg: _append_analysis_log(job_id, msg),
                )
                report_urls = res.get("report_urls") or {}
                results.append({
                    "symbol": sym,
                    "name": item.get("name") or "",
                    "report_urls": report_urls,
                    "selected_url": res.get("selected_url") or "",
                    "report_dir": res.get("report_dir") or "",
                })

            if run_reports:
                output_dir = payload.get("output_dir") or "reports"
                enriched = []
                for item in results:
                    symbol = item.get("symbol") or ""
                    report_dir = Path(item.get("report_dir") or Path(output_dir) / symbol)
                    overview_path = report_dir / "materials" / f"{symbol}_overview.json"
                    overview_md = ""
                    if overview_path.exists():
                        try:
                            overview_md = json.loads(overview_path.read_text(encoding="utf-8")).get("overview_md") or ""
                        except Exception:
                            overview_md = ""
                    if not overview_md:
                        report_urls = item.get("report_urls") or {}
                        fallback_md = _load_brief_md(report_dir, symbol, report_urls)
                        if fallback_md:
                            overview_md = fallback_md
                    item["overview_md"] = overview_md
                    enriched.append(item)

                query = payload.get("symbol_query") or payload.get("symbol") or ""
                multi_path = _build_multi_stock_report(enriched, output_dir, job_id, query)
                root = Path.cwd()
                rel = os.path.relpath(multi_path, start=root).replace("\\", "/")
                url = "/" + rel.lstrip("/")
                result = {
                    "action": "analysis",
                    "report_mode": "multi",
                    "report_modes": ["multi"],
                    "report_paths": {"multi": multi_path},
                    "report_urls": {"multi": url},
                    "selected_url": url,
                    "report_dir": str(Path(output_dir) / "multi" / job_id),
                    "symbols": [r.get("symbol") for r in results],
                }
            else:
                output_dir = payload.get("output_dir") or "reports"
                result = {
                    "action": "crawler",
                    "report_mode": "crawler",
                    "report_modes": [],
                    "report_paths": {},
                    "report_urls": {},
                    "selected_url": "",
                    "report_dir": str(Path(output_dir)),
                    "symbols": [r.get("symbol") for r in results],
                    "results": results,
                }
        else:
            result = run_pipeline(
                symbol=payload["symbol"],
                report_mode=payload.get("report_mode", "combined"),
                report_modes=report_modes,
                stock_name=payload.get("stock_name"),
                news_days=payload.get("news_days", 7),
                use_bocha=payload.get("use_bocha", False),
                include_realtime=payload.get("include_realtime", True),
                output_dir=payload.get("output_dir"),
                env_overrides=payload.get("env_overrides"),
                db_overrides=payload.get("db_overrides"),
                agent_overrides=payload.get("agent_overrides"),
                crawl_mindspider=payload.get("crawl_mindspider", False),
                mindspider_platforms=payload.get("mindspider_platforms"),
                mindspider_days=payload.get("mindspider_days", 1),
                mindspider_max_notes=payload.get("mindspider_max_notes", 10),
                mindspider_dir=payload.get("mindspider_dir"),
                mindspider_keywords=mindspider_keywords,
                mindspider_no_crawl=payload.get("mindspider_no_crawl", False),
                crawl_guba=payload.get("crawl_guba", False),
                guba_keywords=guba_keywords,
                guba_dir=payload.get("guba_dir"),
                guba_pages=payload.get("guba_pages", 10),
                guba_fulltext=payload.get("guba_fulltext", False),
                guba_fulltext_days=payload.get("guba_fulltext_days", 7),
                guba_fulltext_limit=payload.get("guba_fulltext_limit", 200),
                guba_init_schema=payload.get("guba_init_schema", False),
                run_reports=run_reports,
                log=lambda msg: _append_analysis_log(job_id, msg),
            )
        with ANALYSIS_LOCK:
            job = ANALYSIS_JOBS.get(job_id, {})
            job["status"] = "done"
            job["result"] = result
            job["updated_at"] = datetime.now().isoformat(timespec="seconds")
            ANALYSIS_JOBS[job_id] = job
    except Exception as exc:
        with ANALYSIS_LOCK:
            job = ANALYSIS_JOBS.get(job_id, {})
            job["status"] = "error"
            job["error"] = str(exc)
            job["updated_at"] = datetime.now().isoformat(timespec="seconds")
            ANALYSIS_JOBS[job_id] = job


@app.route('/api/analysis/run', methods=['POST'])
def run_analysis_api():
    payload = request.get_json(silent=True) or {}
    symbol_query = str(payload.get("symbol", "")).strip()
    if not symbol_query:
        return jsonify({"success": False, "message": "symbol required"}), 400
    action = str(payload.get("action", "analysis")).lower()
    if action not in ("analysis", "crawler"):
        return jsonify({"success": False, "message": "invalid action"}), 400
    if action == "analysis":
        report_modes = payload.get("report_modes")
        if isinstance(report_modes, list):
            if not any(str(item).strip() for item in report_modes):
                return jsonify({"success": False, "message": "select at least one report mode"}), 400
        elif report_modes is not None and not str(report_modes).strip():
            return jsonify({"success": False, "message": "select at least one report mode"}), 400
    else:
        if not payload.get("crawl_mindspider") and not payload.get("crawl_guba"):
            return jsonify({"success": False, "message": "select at least one crawler"}), 400

    job_id = uuid.uuid4().hex
    with ANALYSIS_LOCK:
        ANALYSIS_JOBS[job_id] = {
            "status": "running",
            "log": [],
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    resolved = _resolve_symbols_from_query(symbol_query)
    if not resolved:
        return jsonify({"success": False, "message": "symbol not recognized"}), 400
    if len(resolved) == 1:
        payload["symbol"] = resolved[0].get("symbol") or symbol_query
        if not payload.get("stock_name") and resolved[0].get("name"):
            payload["stock_name"] = resolved[0]["name"]
    else:
        payload["symbols"] = resolved
        payload["symbol_query"] = symbol_query
        payload["symbol"] = resolved[0].get("symbol") or symbol_query
    payload["action"] = action
    thread = threading.Thread(target=_run_analysis_job, args=(job_id, payload), daemon=True)
    thread.start()

    return jsonify({"success": True, "job_id": job_id})


@app.route('/api/analysis/status/<job_id>')
def analysis_status(job_id):
    with ANALYSIS_LOCK:
        job = ANALYSIS_JOBS.get(job_id)
        if not job:
            return jsonify({"success": False, "message": "job not found"}), 404
        return jsonify({"success": True, "job": job})


@app.route('/api/symbols/resolve')
def resolve_symbols():
    query = str(request.args.get("query", "")).strip()
    if not query:
        return jsonify({"success": False, "message": "query required"}), 400
    resolved = _resolve_symbols_from_query(query)
    if not resolved:
        return jsonify({"success": False, "message": "symbol not recognized"}), 404
    return jsonify({"success": True, "symbols": resolved})


@app.route('/api/market/charts', methods=['POST'])
def market_charts():
    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get("symbol", "")).strip()
    if not symbol:
        return jsonify({"success": False, "message": "symbol required"}), 400

    timeframes = _normalize_chart_timeframes(payload.get("timeframes"))
    output_dir = payload.get("output_dir")

    result = _generate_interactive_charts(symbol=symbol, timeframes=timeframes, output_dir=output_dir)
    urls = result.get("urls", {})
    errors = result.get("errors", {})

    if not urls and errors:
        return jsonify({"success": False, "message": "no charts generated", "errors": errors}), 500

    return jsonify({
        "success": True,
        "symbol": symbol,
        "timeframes": timeframes,
        "urls": urls,
        "errors": errors,
    })


@app.route('/api/export/csv')
def export_csv():
    symbol_query = str(request.args.get("symbol", "")).strip()
    if not symbol_query:
        return jsonify({"success": False, "message": "symbol required"}), 400

    def _collect_rows_for_symbol(symbol: str) -> List[Dict[str, Any]]:
        materials_dir = Path("reports") / symbol / "materials"
        if not materials_dir.exists():
            return []
        rows: List[Dict[str, Any]] = []

        tech_payload = _read_json_file(materials_dir / f"{symbol}_tech_data.json")
        if isinstance(tech_payload, dict):
            for timeframe, records in tech_payload.items():
                if not isinstance(records, list):
                    continue
                for rec in records:
                    if not isinstance(rec, dict):
                        continue
                    row = {"section": "tech", "symbol": symbol, "timeframe": timeframe}
                    row.update(rec)
                    rows.append(row)

        fundamentals_payload = _read_json_file(materials_dir / f"{symbol}_fundamentals_payload.json")
        if isinstance(fundamentals_payload, dict):
            _flatten_payload(fundamentals_payload, "", rows, symbol)

        message_items = _read_json_file(materials_dir / f"{symbol}_message_items.json")
        if isinstance(message_items, list):
            for item in message_items:
                if not isinstance(item, dict):
                    continue
                rows.append({
                    "section": "news_db",
                    "symbol": symbol,
                    "platform": item.get("platform", ""),
                    "published_at": item.get("published_at", ""),
                    "title": item.get("title", ""),
                    "content": item.get("content", ""),
                    "url": item.get("url", ""),
                    "author": item.get("author", ""),
                    "keyword": item.get("keyword", ""),
                    "engagement": item.get("engagement", ""),
                    "match_count": item.get("match_count", ""),
                    "score": item.get("score", ""),
                    "source_table": item.get("source_table", ""),
                    "time_source": item.get("time_source", ""),
                    "media_urls": item.get("media_urls", ""),
                })

        web_items = _read_json_file(materials_dir / f"{symbol}_message_web_items.json")
        if isinstance(web_items, list):
            for item in web_items:
                if not isinstance(item, dict):
                    continue
                rows.append({
                    "section": "news_web",
                    "symbol": symbol,
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                    "url": item.get("url", ""),
                })

        keyword_info = _read_json_file(materials_dir / f"{symbol}_message_keywords.json")
        if isinstance(keyword_info, dict):
            rows.append({
                "section": "news_keywords",
                "symbol": symbol,
                "keywords": ", ".join(keyword_info.get("keywords") or []),
                "db_error": keyword_info.get("db_error") or "",
            })

        return rows

    resolved = _resolve_symbols_from_query(symbol_query)
    if not resolved:
        fallback_symbol = symbol_query
        if not (Path("reports") / fallback_symbol).exists():
            return jsonify({"success": False, "message": "symbol not recognized"}), 400
        resolved = [{"symbol": fallback_symbol, "name": ""}]

    symbols = [item.get("symbol") for item in resolved if item.get("symbol")]
    if not symbols:
        return jsonify({"success": False, "message": "symbol not recognized"}), 400

    if len(symbols) == 1:
        symbol = symbols[0]
        rows = _collect_rows_for_symbol(symbol)
        if not rows:
            return jsonify({"success": False, "message": "no materials found"}), 404
        csv_text = _rows_to_csv(rows)
        data = csv_text.encode("utf-8-sig")
        filename = f"{symbol}_all_data.csv"
        return Response(
            data,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    data_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in symbols:
        data_by_symbol[symbol] = _collect_rows_for_symbol(symbol)

    if not any(data_by_symbol.values()):
        return jsonify({"success": False, "message": "no materials found"}), 404

    engine = None
    try:
        import openpyxl  # noqa: F401
        engine = "openpyxl"
    except Exception:
        try:
            import xlsxwriter  # noqa: F401
            engine = "xlsxwriter"
        except Exception:
            return jsonify({"success": False, "message": "openpyxl or xlsxwriter required for multi export"}), 500

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine=engine) as writer:
        for symbol, rows in data_by_symbol.items():
            df = _rows_to_dataframe(rows)
            if df.empty:
                df = pd.DataFrame(columns=_collect_csv_fieldnames(rows) or ["section", "symbol"])
            df.to_excel(writer, sheet_name=str(symbol)[:31], index=False)
    output.seek(0)

    filename = "analysis_multi.xlsx"
    return Response(
        output.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route('/api/export/pdf')
def export_pdf():
    symbol_query = str(request.args.get("symbol", "")).strip()
    if not symbol_query:
        return jsonify({"success": False, "message": "symbol required"}), 400

    resolved = _resolve_symbols_from_query(symbol_query)
    if not resolved:
        fallback_symbol = symbol_query
        if not (Path("reports") / fallback_symbol).exists():
            return jsonify({"success": False, "message": "symbol not recognized"}), 400
        resolved = [{"symbol": fallback_symbol, "name": ""}]

    symbols = [item.get("symbol") for item in resolved if item.get("symbol")]
    if not symbols:
        return jsonify({"success": False, "message": "symbol not recognized"}), 400
    if len(symbols) != 1:
        return jsonify({"success": False, "message": "single symbol required"}), 400

    symbol = symbols[0]
    report_dir = Path("reports") / symbol
    if not report_dir.exists():
        return jsonify({"success": False, "message": "report not found"}), 404

    candidates = [
        report_dir / f"{symbol}_report.pdf",
        report_dir / "report.pdf",
        report_dir / f"{symbol}.pdf",
        report_dir / "combined.pdf",
    ]
    for path in candidates:
        if path.exists():
            return send_from_directory(
                report_dir,
                path.name,
                as_attachment=True,
                download_name=path.name,
                mimetype="application/pdf",
            )

    pdfs = sorted(report_dir.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
    if pdfs:
        path = pdfs[0]
        return send_from_directory(
            report_dir,
            path.name,
            as_attachment=True,
            download_name=path.name,
            mimetype="application/pdf",
        )

    return jsonify({"success": False, "message": "pdf not found"}), 404


@app.route('/api/evidence/<symbol>')
def get_evidence(symbol: str):
    symbol = str(symbol or "").strip()
    if not symbol:
        return jsonify({"success": False, "message": "symbol required"}), 400
    materials_dir = Path("reports") / symbol / "materials"
    evidence = _read_json_file(materials_dir / f"{symbol}_evidence.json")
    if not isinstance(evidence, list):
        return jsonify({"success": False, "message": "no evidence found"}), 404
    return jsonify({"success": True, "symbol": symbol, "items": evidence})


@app.route('/api/status')
def get_status():
    """获取所有应用状态"""
    check_app_status()
    return jsonify({
        app_name: {
            'status': info['status'],
            'port': info['port'],
            'output_lines': len(info['output'])
        }
        for app_name, info in processes.items()
    })

@app.route('/api/start/<app_name>')
def start_app(app_name):
    """启动指定应用"""
    if app_name not in processes:
        return jsonify({'success': False, 'message': '未知应用'})

    if app_name == 'forum':
        try:
            start_forum_engine()
            processes['forum']['status'] = 'running'
            return jsonify({'success': True, 'message': 'ForumEngine已启动'})
        except Exception as exc:  # pragma: no cover
            logger.exception("手动启动ForumEngine失败")
            return jsonify({'success': False, 'message': f'ForumEngine启动失败: {exc}'})

    script_path = STREAMLIT_SCRIPTS.get(app_name)
    if not script_path:
        return jsonify({'success': False, 'message': '该应用不支持启动操作'})

    success, message = start_streamlit_app(
        app_name,
        script_path,
        processes[app_name]['port']
    )

    if success:
        # 等待应用启动
        startup_success, startup_message = wait_for_app_startup(app_name, 15)
        if not startup_success:
            message += f" 但启动检查失败: {startup_message}"
    
    return jsonify({'success': success, 'message': message})

@app.route('/api/stop/<app_name>')
def stop_app(app_name):
    """停止指定应用"""
    if app_name not in processes:
        return jsonify({'success': False, 'message': '未知应用'})

    if app_name == 'forum':
        try:
            stop_forum_engine()
            processes['forum']['status'] = 'stopped'
            return jsonify({'success': True, 'message': 'ForumEngine已停止'})
        except Exception as exc:  # pragma: no cover
            logger.exception("手动停止ForumEngine失败")
            return jsonify({'success': False, 'message': f'ForumEngine停止失败: {exc}'})

    success, message = stop_streamlit_app(app_name)
    return jsonify({'success': success, 'message': message})

@app.route('/api/output/<app_name>')
def get_output(app_name):
    """获取应用输出"""
    if app_name not in processes:
        return jsonify({'success': False, 'message': '未知应用'})
    
    # 特殊处理Forum Engine
    if app_name == 'forum':
        try:
            forum_log_content = read_log_from_file('forum')
            return jsonify({
                'success': True,
                'output': forum_log_content,
                'total_lines': len(forum_log_content)
            })
        except Exception as e:
            return jsonify({'success': False, 'message': f'读取forum日志失败: {str(e)}'})
    
    # 从文件读取完整日志
    output_lines = read_log_from_file(app_name)
    
    return jsonify({
        'success': True,
        'output': output_lines
    })

@app.route('/api/test_log/<app_name>')
def test_log(app_name):
    """测试日志写入功能"""
    if app_name not in processes:
        return jsonify({'success': False, 'message': '未知应用'})
    
    # 写入测试消息
    test_msg = f"[{datetime.now().strftime('%H:%M:%S')}] 测试日志消息 - {datetime.now()}"
    write_log_to_file(app_name, test_msg)
    
    # 通过Socket.IO发送
    socketio.emit('console_output', {
        'app': app_name,
        'line': test_msg
    })
    
    return jsonify({
        'success': True,
        'message': f'测试消息已写入 {app_name} 日志'
    })

@app.route('/api/forum/start')
def start_forum_monitoring_api():
    """手动启动ForumEngine论坛"""
    try:
        from ForumEngine.monitor import start_forum_monitoring
        success = start_forum_monitoring()
        if success:
            return jsonify({'success': True, 'message': 'ForumEngine论坛已启动'})
        else:
            return jsonify({'success': False, 'message': 'ForumEngine论坛启动失败'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'启动论坛失败: {str(e)}'})

@app.route('/api/forum/stop')
def stop_forum_monitoring_api():
    """手动停止ForumEngine论坛"""
    try:
        from ForumEngine.monitor import stop_forum_monitoring
        stop_forum_monitoring()
        return jsonify({'success': True, 'message': 'ForumEngine论坛已停止'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'停止论坛失败: {str(e)}'})

@app.route('/api/forum/log')
def get_forum_log():
    """获取ForumEngine的forum.log内容"""
    try:
        forum_log_file = LOG_DIR / "forum.log"
        if not forum_log_file.exists():
            return jsonify({
                'success': True,
                'log_lines': [],
                'parsed_messages': [],
                'total_lines': 0
            })
        
        with open(forum_log_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            lines = [line.rstrip('\n\r') for line in lines if line.strip()]
        
        # 解析每一行日志并提取对话信息
        parsed_messages = []
        for line in lines:
            parsed_message = parse_forum_log_line(line)
            if parsed_message:
                parsed_messages.append(parsed_message)
        
        return jsonify({
            'success': True,
            'log_lines': lines,
            'parsed_messages': parsed_messages,
            'total_lines': len(lines)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'读取forum.log失败: {str(e)}'})

@app.route('/api/search', methods=['POST'])
def search():
    """统一搜索接口"""
    data = request.get_json()
    query = data.get('query', '').strip()
    
    if not query:
        return jsonify({'success': False, 'message': '搜索查询不能为空'})
    
    # ForumEngine论坛已经在后台运行，会自动检测搜索活动
    # logger.info("ForumEngine: 搜索请求已收到，论坛将自动检测日志变化")
    
    # 检查哪些应用正在运行
    check_app_status()
    running_apps = [name for name, info in processes.items() if info['status'] == 'running']
    
    if not running_apps:
        return jsonify({'success': False, 'message': '没有运行中的应用'})
    
    # 向运行中的应用发送搜索请求
    results = {}
    api_ports = {'insight': 8601}
    
    for app_name in running_apps:
        try:
            api_port = api_ports.get(app_name)
            if not api_port:
                continue
            # 调用Streamlit应用的API端点
            response = requests.post(
                f"http://localhost:{api_port}/api/search",
                json={'query': query},
                timeout=10
            )
            if response.status_code == 200:
                results[app_name] = response.json()
            else:
                results[app_name] = {'success': False, 'message': 'API调用失败'}
        except Exception as e:
            results[app_name] = {'success': False, 'message': str(e)}
    
    # 搜索完成后可以选择停止监控，或者让它继续运行以捕获后续的处理日志
    # 这里我们让监控继续运行，用户可以通过其他接口手动停止
    
    return jsonify({
        'success': True,
        'query': query,
        'results': results
    })


@app.route('/api/config', methods=['GET'])
def get_config():
    """Expose selected configuration values to the frontend."""
    try:
        config_values = read_config_values()
        return jsonify({'success': True, 'config': config_values})
    except Exception as exc:
        logger.exception("读取配置失败")
        return jsonify({'success': False, 'message': f'读取配置失败: {exc}'}), 500


@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration values and persist them to config.py."""
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict) or not payload:
        return jsonify({'success': False, 'message': '请求体不能为空'}), 400

    updates = {}
    for key, value in payload.items():
        if key in CONFIG_KEYS:
            updates[key] = value if value is not None else ''

    if not updates:
        return jsonify({'success': False, 'message': '没有可更新的配置项'}), 400

    try:
        write_config_values(updates)
        updated_config = read_config_values()
        return jsonify({'success': True, 'config': updated_config})
    except Exception as exc:
        logger.exception("更新配置失败")
        return jsonify({'success': False, 'message': f'更新配置失败: {exc}'}), 500


@app.route('/api/system/status')
def get_system_status():
    """返回系统启动状态。"""
    state = _get_system_state()
    return jsonify({
        'success': True,
        'started': state['started'],
        'starting': state['starting']
    })


@app.route('/api/system/start', methods=['POST'])
def start_system():
    """在接收到请求后启动完整系统。"""
    allowed, message = _prepare_system_start()
    if not allowed:
        return jsonify({'success': False, 'message': message}), 400

    try:
        success, logs, errors = initialize_system_components()
        if success:
            _set_system_state(started=True)
            return jsonify({'success': True, 'message': '系统启动成功', 'logs': logs})

        _set_system_state(started=False)
        return jsonify({
            'success': False,
            'message': '系统启动失败',
            'logs': logs,
            'errors': errors
        }), 500
    except Exception as exc:  # pragma: no cover - 保底捕获
        logger.exception("系统启动过程中出现异常")
        _set_system_state(started=False)
        return jsonify({'success': False, 'message': f'系统启动异常: {exc}'}), 500
    finally:
        _set_system_state(starting=False)

@socketio.on('connect')
def handle_connect():
    """客户端连接"""
    emit('status', 'Connected to Flask server')

@socketio.on('request_status')
def handle_status_request():
    """请求状态更新"""
    check_app_status()
    emit('status_update', {
        app_name: {
            'status': info['status'],
            'port': info['port']
        }
        for app_name, info in processes.items()
    })

if __name__ == '__main__':
    # 从配置文件读取 HOST 和 PORT
    from config import settings
    HOST = settings.HOST
    PORT = settings.PORT
    
    logger.info("等待配置确认，系统将在前端指令后启动组件...")
    logger.info(f"Flask服务器已启动，访问地址: http://{HOST}:{PORT}")
    
    try:
        socketio.run(app, host=HOST, port=PORT, debug=False)
    except KeyboardInterrupt:
        logger.info("\n正在关闭应用...")
        cleanup_processes()
        
    
