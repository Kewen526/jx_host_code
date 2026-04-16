#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
浏览器池管理模块

功能:
1. WebKit浏览器池管理（多Browser + 多Context）
2. 账号锁机制（保活和任务不冲突）
3. 错峰保活（24小时保持Cookie活跃）
4. Cookie异步上传队列
5. 状态持久化（退出保存/启动恢复）
6. 浏览器定时重启（防止内存泄漏）
"""

import os
import json
import time
import queue
import signal
import socket
import subprocess
import requests
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
from pathlib import Path

# Playwright导入
try:
    from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ 未安装playwright，浏览器池功能将不可用")

# 统一日志模块导入
from logger import (
    log_keepalive, log_system, log_review as _log_review_module,
    setup_stdout_redirect,
)

# 评价回复模块导入
try:
    from review_reply import process_review_replies
    REVIEW_REPLY_AVAILABLE = True
except ImportError:
    REVIEW_REPLY_AVAILABLE = False
    print("⚠️ 未安装review_reply模块，评价回复功能将不可用")


# ============================================================================
# 配置参数
# ============================================================================

# 浏览器池配置
MAX_BROWSERS = 10                    # 最大Browser数量
MAX_CONTEXTS_PER_BROWSER = 15        # 每个Browser最大Context数量
BROWSER_TYPE = "chromium"            # 浏览器类型: webkit / chromium / firefox

# 保活配置（优化后）
KEEPALIVE_INTERVAL = 60 * 60         # 保活间隔（秒）：60分钟（原30分钟）
KEEPALIVE_PAGE_URL = "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html"
KEEPALIVE_BATCH_SIZE = 2             # 每批保活账号数量（原5个）
KEEPALIVE_BATCH_INTERVAL = 60        # 每批之间的间隔（秒）
KEEPALIVE_TIMEOUT = 15000            # 保活页面超时（毫秒）：15秒（原30秒）
KEEPALIVE_FAIL_COOLDOWN = 10 * 60    # 失败冷却时间（秒）：10分钟

# 资源保护配置
RESOURCE_CHECK_INTERVAL = 30         # 资源检查间隔（秒）
CPU_WARNING_THRESHOLD = 50           # CPU警告阈值（%）
CPU_CRITICAL_THRESHOLD = 70          # CPU危险阈值（%）
MEMORY_WARNING_THRESHOLD = 60        # 内存警告阈值（%）
MEMORY_CRITICAL_THRESHOLD = 80       # 内存危险阈值（%）
MAX_ACTIVE_CONTEXTS = 10             # 最大活跃Context数量
CONTEXT_IDLE_TIMEOUT = 30 * 60       # Context空闲超时（秒）：30分钟

# 浏览器重启配置
BROWSER_RESTART_HOUR = 3             # 每天重启时间（凌晨3点）
BROWSER_MAX_RESTART_RETRIES = 3      # 重启失败最大重试次数

# Playwright driver 自愈配置
DRIVER_RESTART_SLEEP_SEC = 2         # driver 启动后等待时间（秒），确保 Node.js 进程就绪
DRIVER_ERROR_THRESHOLD = 1           # 触发自动重启的连续 driver 错误次数

# Cookie上传配置
COOKIE_UPLOAD_QUEUE_SIZE = 1000      # Cookie上传队列大小
COOKIE_UPLOAD_BATCH_SIZE = 10        # 批量上传数量
COOKIE_UPLOAD_INTERVAL = 5           # 上传间隔（秒）

# 状态持久化配置
STATE_DIR = "/home/meituan/data/state"
BROWSER_POOL_STATE_FILE = "browser_pool_state.json"

# API配置
API_BASE_URL = "http://8.146.210.145:3000"
COOKIE_CONFIG_API = f"{API_BASE_URL}/api/cookie_config"
PLATFORM_ACCOUNTS_API = f"{API_BASE_URL}/api/platform-accounts"
GET_TASK_API = f"{API_BASE_URL}/api/get_task"
ERROR_LOG_API = f"{API_BASE_URL}/api/log"
GET_SINGLE_ACCOUNT_API = f"{API_BASE_URL}/api/get_platform_account"  # 获取单个账号Cookie
GET_ACCOUNTS_BY_HOST_API = "https://kewenai.asia/api/servers/getAccountsByHost"  # 获取服务器分配的全量账号

# 获取公网IP的服务列表（按优先级）
PUBLIC_IP_SERVICES = [
    "https://ifconfig.me",
    "https://ip.sb",
    "https://api.ipify.org",
    "https://icanhazip.com",
]


# ============================================================================
# 全局变量
# ============================================================================

# 服务器公网IP（启动时获取并缓存）
_server_ip: Optional[str] = None

# 浏览器池运行状态
_pool_running = True


# ============================================================================
# 日志工具函数
# ============================================================================

def log_print(message: str, level: str = "INFO"):
    """带时间戳的日志打印（系统模块）

    Args:
        message: 日志消息
        level: 日志级别 (INFO, WARN, ERROR)
    """
    log_system(message, level)


def log_info(message: str):
    """INFO级别日志"""
    log_system(message, "INFO")


def log_warn(message: str):
    """WARN级别日志"""
    log_system(message, "WARN")


def log_error(message: str, error: Exception = None, upload: bool = True,
              context: str = None, account_id: str = None):
    """ERROR级别日志，并可选上传到服务器

    Args:
        message: 错误消息
        error: 异常对象
        upload: 是否上传到服务器
        context: 错误发生的上下文（如函数名）
        account_id: 相关账号ID
    """
    import traceback

    # 构建完整错误消息
    full_message = message
    if error:
        full_message += f" | Exception: {type(error).__name__}: {str(error)}"

    log_system(full_message, "ERROR")

    # 上传到服务器
    if upload:
        try:
            error_data = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "level": "ERROR",
                "message": message,
                "context": context or "browser_pool",
                "account_id": account_id,
                "error_type": type(error).__name__ if error else None,
                "error_detail": str(error) if error else None,
                "traceback": traceback.format_exc() if error else None,
                "server_ip": _server_ip
            }

            response = requests.post(
                ERROR_LOG_API,
                json=error_data,
                timeout=5  # 短超时，不阻塞主流程
            )

            if response.status_code != 200:
                log_system(f"日志上传失败: HTTP {response.status_code}", "WARN")

        except Exception as upload_error:
            # 上传失败不影响主流程，只打印警告
            log_system(f"日志上传异常: {upload_error}", "WARN")


def upload_error_log(error_type: str, error_message: str,
                     context: str = None, account_id: str = None,
                     extra_data: dict = None):
    """上传错误日志到服务器（独立函数）

    Args:
        error_type: 错误类型
        error_message: 错误消息
        context: 错误发生的上下文
        account_id: 相关账号ID
        extra_data: 额外数据
    """
    try:
        error_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level": "ERROR",
            "error_type": error_type,
            "message": error_message,
            "context": context or "browser_pool",
            "account_id": account_id,
            "server_ip": _server_ip,
            **(extra_data or {})
        }

        response = requests.post(
            ERROR_LOG_API,
            json=error_data,
            timeout=5
        )

        return response.status_code == 200

    except Exception as e:
        log_system(f"日志上传异常: {e}", "WARN")
        return False


# ============================================================================
# 资源监控模块
# ============================================================================

class ResourceMonitor:
    """资源监控器 - 防止服务器过载"""

    # 资源状态常量
    STATUS_NORMAL = "normal"      # 正常：可以执行所有操作
    STATUS_WARNING = "warning"    # 警告：暂停保活，只执行任务
    STATUS_CRITICAL = "critical"  # 危险：暂停所有操作，释放资源

    def __init__(self):
        self._last_check_time: Optional[datetime] = None
        self._cached_status: str = self.STATUS_NORMAL
        self._cached_cpu: float = 0.0
        self._cached_memory: float = 0.0

    def get_cpu_usage(self) -> float:
        """获取CPU使用率（%）"""
        try:
            # 读取 /proc/stat 计算CPU使用率
            with open('/proc/stat', 'r') as f:
                line = f.readline()
            values = line.split()[1:8]
            values = [int(v) for v in values]

            # user, nice, system, idle, iowait, irq, softirq
            idle = values[3] + values[4]
            total = sum(values)

            # 需要两次采样计算差值
            if not hasattr(self, '_last_cpu_idle'):
                self._last_cpu_idle = idle
                self._last_cpu_total = total
                time.sleep(0.1)
                return self.get_cpu_usage()

            idle_delta = idle - self._last_cpu_idle
            total_delta = total - self._last_cpu_total

            self._last_cpu_idle = idle
            self._last_cpu_total = total

            if total_delta == 0:
                return 0.0

            usage = 100.0 * (1.0 - idle_delta / total_delta)
            return round(usage, 1)

        except Exception as e:
            print(f"⚠️ 获取CPU使用率失败: {e}")
            return 0.0

    def get_memory_usage(self) -> float:
        """获取内存使用率（%）"""
        try:
            with open('/proc/meminfo', 'r') as f:
                lines = f.readlines()

            mem_info = {}
            for line in lines:
                parts = line.split()
                if len(parts) >= 2:
                    key = parts[0].rstrip(':')
                    value = int(parts[1])
                    mem_info[key] = value

            total = mem_info.get('MemTotal', 1)
            available = mem_info.get('MemAvailable', mem_info.get('MemFree', 0))

            usage = 100.0 * (1.0 - available / total)
            return round(usage, 1)

        except Exception as e:
            print(f"⚠️ 获取内存使用率失败: {e}")
            return 0.0

    def check_status(self, force: bool = False) -> str:
        """检查资源状态

        Args:
            force: 是否强制刷新（忽略缓存）

        Returns:
            str: STATUS_NORMAL / STATUS_WARNING / STATUS_CRITICAL
        """
        now = datetime.now()

        # 使用缓存（避免频繁检查）
        if not force and self._last_check_time:
            elapsed = (now - self._last_check_time).total_seconds()
            if elapsed < RESOURCE_CHECK_INTERVAL:
                return self._cached_status

        # 获取资源使用率
        cpu = self.get_cpu_usage()
        memory = self.get_memory_usage()

        self._cached_cpu = cpu
        self._cached_memory = memory
        self._last_check_time = now

        # 判断状态
        if cpu >= CPU_CRITICAL_THRESHOLD or memory >= MEMORY_CRITICAL_THRESHOLD:
            self._cached_status = self.STATUS_CRITICAL
        elif cpu >= CPU_WARNING_THRESHOLD or memory >= MEMORY_WARNING_THRESHOLD:
            self._cached_status = self.STATUS_WARNING
        else:
            self._cached_status = self.STATUS_NORMAL

        return self._cached_status

    def is_safe_for_keepalive(self) -> bool:
        """是否可以安全执行保活"""
        status = self.check_status()
        return status == self.STATUS_NORMAL

    def is_safe_for_task(self) -> bool:
        """是否可以安全执行任务"""
        status = self.check_status()
        return status != self.STATUS_CRITICAL

    def get_status_info(self) -> Dict[str, Any]:
        """获取资源状态详情"""
        status = self.check_status()
        return {
            'status': status,
            'cpu': self._cached_cpu,
            'memory': self._cached_memory,
            'safe_for_keepalive': status == self.STATUS_NORMAL,
            'safe_for_task': status != self.STATUS_CRITICAL
        }

    def print_status(self):
        """打印资源状态"""
        info = self.get_status_info()
        status_emoji = {
            self.STATUS_NORMAL: "✅",
            self.STATUS_WARNING: "⚠️",
            self.STATUS_CRITICAL: "🚨"
        }
        emoji = status_emoji.get(info['status'], "❓")
        print(f"   {emoji} 资源状态: CPU={info['cpu']}%, 内存={info['memory']}%, 状态={info['status']}")


# 全局资源监控器
resource_monitor = ResourceMonitor()


# ============================================================================
# 公网IP获取
# ============================================================================

def get_public_ip() -> Optional[str]:
    """获取服务器公网IP

    尝试多个服务，直到成功获取
    获取后缓存，避免重复请求

    Returns:
        str: 公网IP地址
        None: 获取失败
    """
    global _server_ip

    # 如果已缓存，直接返回
    if _server_ip:
        return _server_ip

    print("🌐 正在获取服务器公网IP...")

    for service_url in PUBLIC_IP_SERVICES:
        try:
            response = requests.get(service_url, timeout=10)
            if response.status_code == 200:
                ip = response.text.strip()
                # 简单验证IP格式
                parts = ip.split('.')
                if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                    _server_ip = ip
                    print(f"   ✅ 获取成功: {ip} (来源: {service_url})")
                    return ip
        except Exception as e:
            print(f"   ⚠️ {service_url} 获取失败: {e}")
            continue

    print("   ❌ 所有服务都获取失败")
    return None


def get_cached_ip() -> Optional[str]:
    """获取缓存的公网IP"""
    return _server_ip


# ============================================================================
# 账号锁管理器
# ============================================================================

class AccountLockManager:
    """账号锁管理器

    为每个账号维护一个锁，确保同一账号不会同时执行任务和保活
    """

    def __init__(self):
        self._locks: Dict[str, threading.Lock] = {}
        self._lock_mutex = threading.Lock()

    def get_lock(self, account_id: str) -> threading.Lock:
        """获取账号的锁（如果不存在则创建）"""
        with self._lock_mutex:
            if account_id not in self._locks:
                self._locks[account_id] = threading.Lock()
            return self._locks[account_id]

    def acquire(self, account_id: str, blocking: bool = True, timeout: float = -1) -> bool:
        """获取账号锁

        Args:
            account_id: 账号ID
            blocking: 是否阻塞等待
            timeout: 超时时间（秒），-1表示无限等待

        Returns:
            bool: 是否成功获取锁
        """
        lock = self.get_lock(account_id)
        return lock.acquire(blocking=blocking, timeout=timeout)

    def release(self, account_id: str):
        """释放账号锁"""
        lock = self.get_lock(account_id)
        try:
            lock.release()
        except RuntimeError:
            # 锁未被持有
            pass

    @contextmanager
    def lock_account(self, account_id: str, blocking: bool = True, timeout: float = -1):
        """账号锁上下文管理器"""
        acquired = self.acquire(account_id, blocking, timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(account_id)

    def try_lock(self, account_id: str) -> bool:
        """尝试获取锁（非阻塞）"""
        return self.acquire(account_id, blocking=False)

    def is_locked(self, account_id: str) -> bool:
        """检查账号是否被锁定"""
        lock = self.get_lock(account_id)
        acquired = lock.acquire(blocking=False)
        if acquired:
            lock.release()
            return False
        return True


# 全局账号锁管理器
account_lock_manager = AccountLockManager()


# ============================================================================
# Cookie上传队列
# ============================================================================

class CookieUploadQueue:
    """Cookie异步上传队列

    将Cookie上传任务放入队列，由后台线程异步处理
    支持批量上传和重试
    """

    def __init__(self, max_size: int = COOKIE_UPLOAD_QUEUE_SIZE):
        self._queue = queue.Queue(maxsize=max_size)
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """启动上传线程"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._upload_worker, daemon=True)
        self._thread.start()
        print("✅ Cookie上传队列已启动")

    def stop(self):
        """停止上传线程"""
        self._running = False
        if self._thread:
            # 放入一个None作为停止信号
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
            self._thread.join(timeout=5)
        print("✅ Cookie上传队列已停止")

    def put(self, account_id: str, cookies: dict):
        """添加Cookie到上传队列

        Args:
            account_id: 账号ID
            cookies: Cookie字典
        """
        try:
            self._queue.put_nowait({
                'account_id': account_id,
                'cookies': cookies,
                'timestamp': datetime.now()
            })
        except queue.Full:
            print(f"⚠️ Cookie上传队列已满，丢弃 {account_id} 的Cookie上传任务")

    def _upload_worker(self):
        """上传工作线程"""
        batch = []
        last_upload_time = time.time()

        while self._running:
            try:
                # 从队列获取任务，超时1秒
                try:
                    item = self._queue.get(timeout=1)
                except queue.Empty:
                    item = None

                if item is None:
                    # 停止信号或超时，检查是否需要上传当前批次
                    if batch and (time.time() - last_upload_time >= COOKIE_UPLOAD_INTERVAL):
                        self._upload_batch(batch)
                        batch = []
                        last_upload_time = time.time()
                    continue

                batch.append(item)

                # 达到批量大小或超过间隔时间，执行上传
                if len(batch) >= COOKIE_UPLOAD_BATCH_SIZE or \
                   (time.time() - last_upload_time >= COOKIE_UPLOAD_INTERVAL):
                    self._upload_batch(batch)
                    batch = []
                    last_upload_time = time.time()

            except Exception as e:
                print(f"❌ Cookie上传工作线程异常: {e}")
                time.sleep(1)

        # 退出前上传剩余的
        if batch:
            self._upload_batch(batch)

    def _upload_batch(self, batch: List[Dict]):
        """批量上传Cookie"""
        for item in batch:
            account_id = item['account_id']
            cookies = item['cookies']

            try:
                self._upload_single(account_id, cookies)
            except Exception as e:
                print(f"   ❌ {account_id} Cookie上传失败: {e}")

    def _upload_single(self, account_id: str, cookies: dict):
        """上传单个账号的Cookie到两个API"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1. 上传到 /api/cookie_config
        try:
            response = requests.post(
                COOKIE_CONFIG_API,
                json={
                    "name": account_id,
                    "cookies_json": cookies,
                    "cookie_refreshed_at": current_time
                },
                timeout=30
            )
            if response.status_code == 200:
                print(f"   ✅ {account_id} Cookie已上传到cookie_config")
            else:
                print(f"   ⚠️ {account_id} cookie_config上传失败: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {account_id} cookie_config上传异常: {e}")

        # 2. 上传到 /api/platform-accounts
        try:
            response = requests.post(
                PLATFORM_ACCOUNTS_API,
                json={
                    "account": account_id,
                    "cookie": cookies
                },
                timeout=30
            )
            if response.status_code == 200:
                print(f"   ✅ {account_id} Cookie已上传到platform-accounts")
            else:
                print(f"   ⚠️ {account_id} platform-accounts上传失败: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {account_id} platform-accounts上传异常: {e}")


# 全局Cookie上传队列
cookie_upload_queue = CookieUploadQueue()


# ============================================================================
# 账号信息获取函数（用于恢复时从API获取Cookie）
# ============================================================================

def fetch_account_cookie(account_id: str) -> Optional[Dict]:
    """从API获取单个账号的Cookie

    Args:
        account_id: 账号ID

    Returns:
        Cookie字典，失败返回None
    """
    try:
        response = requests.post(
            GET_SINGLE_ACCOUNT_API,
            headers={'Content-Type': 'application/json'},
            json={"account": account_id},
            timeout=30,
            proxies={'http': None, 'https': None}
        )

        if response.status_code != 200:
            return None

        result = response.json()
        if not result.get('success'):
            return None

        record = result.get('data', {})
        if not record:
            return None

        # 检查auth_status，如果已失效则不返回Cookie（避免为失效账号创建Context）
        auth_status = record.get('auth_status')
        if auth_status == 'invalid':
            log_info(f"{account_id} auth_status=invalid，跳过Cookie获取")
            return None

        # 解析cookie字段
        cookie_data = record.get('cookie')
        if isinstance(cookie_data, str):
            try:
                return json.loads(cookie_data)
            except json.JSONDecodeError:
                return None
        elif isinstance(cookie_data, dict):
            return cookie_data

        return None

    except Exception as e:
        log_warn(f"获取 {account_id} Cookie异常: {e}")
        return None


def fetch_accounts_by_host(server_ip: str) -> List[str]:
    """通过服务器IP获取分配的全量账号列表

    调用 /api/servers/getAccountsByHost 获取当前服务器的所有账号

    Args:
        server_ip: 服务器公网IP

    Returns:
        账号列表，失败返回空列表
    """
    try:
        response = requests.post(
            GET_ACCOUNTS_BY_HOST_API,
            headers={'Content-Type': 'application/json'},
            json={"server_host": server_ip},
            timeout=30,
            proxies={'http': None, 'https': None}
        )

        if response.status_code != 200:
            log_warn(f"获取服务器账号列表失败，HTTP状态码: {response.status_code}")
            return []

        result = response.json()
        if not result.get('success'):
            log_warn(f"获取服务器账号列表失败: {result.get('message', '未知错误')}")
            return []

        data = result.get('data', [])
        accounts = [item.get('account') for item in data if item.get('account')]
        log_info(f"获取服务器 {server_ip} 的账号列表: {len(accounts)} 个")
        return accounts

    except Exception as e:
        log_warn(f"获取服务器账号列表异常: {e}")
        return []


def upload_cookie_sync(account_id: str, cookies: Dict) -> bool:
    """同步上传Cookie到服务器（释放前调用）

    Args:
        account_id: 账号ID
        cookies: Cookie字典

    Returns:
        是否成功
    """
    if not cookies:
        return False

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success = False

    # 上传到 /api/platform-accounts
    try:
        response = requests.post(
            PLATFORM_ACCOUNTS_API,
            json={
                "account": account_id,
                "cookie": cookies
            },
            timeout=10,
            proxies={'http': None, 'https': None}
        )
        if response.status_code == 200:
            success = True
    except Exception:
        pass

    # 上传到 /api/cookie_config
    try:
        response = requests.post(
            COOKIE_CONFIG_API,
            json={
                "name": account_id,
                "cookies_json": cookies,
                "cookie_refreshed_at": current_time
            },
            timeout=10,
            proxies={'http': None, 'https': None}
        )
        if response.status_code == 200:
            success = True
    except Exception:
        pass

    return success


# ============================================================================
# Context包装器
# ============================================================================

class ContextWrapper:
    """Context包装器

    封装单个账号的浏览器Context，包含Cookie和状态信息
    """

    def __init__(self, account_id: str, context: BrowserContext, browser_index: int):
        self.account_id = account_id
        self.context = context
        self.browser_index = browser_index
        self.page: Optional[Page] = None

        # 状态信息
        self.created_at = datetime.now()
        self.last_used_at = datetime.now()
        self.last_keepalive_at: Optional[datetime] = None
        self.cookies: Dict = {}

        # 创建默认页面
        self._create_page()

    def _create_page(self):
        """创建页面"""
        try:
            self.page = self.context.new_page()
            self.page.set_default_timeout(60000)
        except Exception as e:
            print(f"   ⚠️ 创建页面失败: {e}")

    def update_last_used(self):
        """更新最后使用时间"""
        self.last_used_at = datetime.now()

    def update_last_keepalive(self):
        """更新最后保活时间"""
        self.last_keepalive_at = datetime.now()

    def get_cookies(self) -> Dict:
        """从Context获取当前Cookie"""
        try:
            cookies_list = self.context.cookies()
            cookies_dict = {}
            for cookie in cookies_list:
                cookies_dict[cookie['name']] = cookie['value']
            self.cookies = cookies_dict
            return cookies_dict
        except Exception as e:
            print(f"   ⚠️ 获取Cookie失败: {e}")
            return self.cookies

    def close(self):
        """关闭Context"""
        try:
            if self.page:
                self.page.close()
            self.context.close()
        except Exception as e:
            print(f"   ⚠️ 关闭Context失败: {e}")

    def to_dict(self) -> Dict:
        """转换为可序列化的字典（用于状态持久化）"""
        return {
            'account_id': self.account_id,
            'browser_index': self.browser_index,
            'cookies': self.cookies,
            'created_at': self.created_at.isoformat(),
            'last_used_at': self.last_used_at.isoformat(),
            'last_keepalive_at': self.last_keepalive_at.isoformat() if self.last_keepalive_at else None
        }

    def is_valid(self) -> bool:
        """检查 Context 和 Page 是否仍然有效

        Returns:
            bool: True 如果有效，False 如果已失效
        """
        try:
            # 检查 context 是否存在
            if not self.context:
                log_warn(f"[{self.account_id}] Context 为空")
                return False

            # 检查 page 是否存在
            if not self.page:
                log_warn(f"[{self.account_id}] Page 为空")
                return False

            # 尝试执行一个简单操作来验证 context 是否可用
            # 使用 context.cookies() 来验证，这是一个轻量级操作
            self.context.cookies()

            # 尝试检查 page 是否仍然可用
            # 通过获取 page.url 来验证
            _ = self.page.url

            return True

        except Exception as e:
            log_warn(f"[{self.account_id}] Context/Page 健康检查失败: {e}")
            return False

    def safe_goto(self, url: str, wait_until: str = 'load',
                  timeout: int = 60000, max_retries: int = 2) -> bool:
        """安全的页面跳转，带异常处理和重试

        Args:
            url: 目标URL
            wait_until: 等待条件 (load, domcontentloaded, networkidle)
            timeout: 超时时间（毫秒）
            max_retries: 最大重试次数

        Returns:
            bool: 是否成功跳转
        """
        for attempt in range(1, max_retries + 1):
            try:
                # 先检查有效性
                if not self.is_valid():
                    log_error(
                        f"[{self.account_id}] safe_goto 失败: Context/Page 已失效",
                        context="ContextWrapper.safe_goto",
                        account_id=self.account_id
                    )
                    return False

                # 执行跳转
                self.page.goto(url, wait_until=wait_until, timeout=timeout)
                self.update_last_used()
                return True

            except Exception as e:
                error_msg = str(e)

                # 检查是否是 TargetClosedError
                if 'Target' in error_msg and 'closed' in error_msg.lower():
                    log_error(
                        f"[{self.account_id}] safe_goto 失败: 浏览器已关闭",
                        error=e,
                        context="ContextWrapper.safe_goto",
                        account_id=self.account_id
                    )
                    # 标记为无效，不再重试
                    return False

                # 其他错误，尝试重试
                if attempt < max_retries:
                    log_warn(f"[{self.account_id}] safe_goto 第{attempt}次失败，重试中: {e}")
                    time.sleep(2)
                else:
                    log_error(
                        f"[{self.account_id}] safe_goto 最终失败",
                        error=e,
                        context="ContextWrapper.safe_goto",
                        account_id=self.account_id
                    )
                    return False

        return False

    def ensure_page(self) -> bool:
        """确保 page 可用，如果不可用则尝试重新创建

        Returns:
            bool: Page 是否可用
        """
        # 如果 page 已存在且有效，直接返回
        if self.page:
            try:
                _ = self.page.url
                return True
            except Exception:
                log_warn(f"[{self.account_id}] 现有 Page 已失效，尝试重新创建")
                self.page = None

        # 尝试创建新 page
        try:
            if not self.context:
                log_error(
                    f"[{self.account_id}] 无法创建 Page: Context 不存在",
                    context="ContextWrapper.ensure_page",
                    account_id=self.account_id
                )
                return False

            self.page = self.context.new_page()
            self.page.set_default_timeout(60000)
            log_info(f"[{self.account_id}] 成功重新创建 Page")
            return True

        except Exception as e:
            log_error(
                f"[{self.account_id}] 创建 Page 失败",
                error=e,
                context="ContextWrapper.ensure_page",
                account_id=self.account_id
            )
            return False


# ============================================================================
# 浏览器池管理器
# ============================================================================

class BrowserPoolManager:
    """浏览器池管理器

    管理多个WebKit浏览器实例和Context
    提供账号级别的浏览器会话管理
    """

    def __init__(self, max_browsers: int = MAX_BROWSERS,
                 max_contexts_per_browser: int = MAX_CONTEXTS_PER_BROWSER,
                 headless: bool = True):
        self.max_browsers = max_browsers
        self.max_contexts_per_browser = max_contexts_per_browser
        self.headless = headless

        # Playwright实例
        self._playwright: Optional[Playwright] = None

        # Browser列表
        self._browsers: List[Optional[Browser]] = [None] * max_browsers
        self._browser_context_counts: List[int] = [0] * max_browsers

        # 账号到Context的映射
        self._contexts: Dict[str, ContextWrapper] = {}

        # 【新增】已知账号集合（本服务器管理的所有账号，释放时不删除）
        self._known_accounts: set = set()

        # 线程锁（使用可重入锁，避免 restart_browsers 调用 get_context 时死锁）
        self._lock = threading.RLock()

        # 状态
        self._initialized = False
        self._last_restart_date: Optional[str] = None

        # driver 自愈状态
        self._driver_consecutive_errors: int = 0  # 连续 driver 错误计数
        self._driver_restarting: bool = False      # 防止并发重入的标志

    def initialize(self):
        """初始化浏览器池"""
        if self._initialized:
            return

        print("\n" + "=" * 60)
        print("🚀 初始化浏览器池")
        print("=" * 60)
        print(f"   浏览器类型: {BROWSER_TYPE}")
        print(f"   最大Browser数: {self.max_browsers}")
        print(f"   每Browser最大Context: {self.max_contexts_per_browser}")
        print(f"   Headless模式: {self.headless}")

        try:
            self._playwright = sync_playwright().start()
            self._initialized = True
            print("✅ 浏览器池初始化完成")

            # 尝试恢复状态
            self._restore_state()

        except Exception as e:
            print(f"❌ 浏览器池初始化失败: {e}")
            raise

    def shutdown(self):
        """关闭浏览器池"""
        print("\n🛑 正在关闭浏览器池...")

        # 保存状态
        self._save_state()

        # 关闭所有Context
        with self._lock:
            for account_id, wrapper in list(self._contexts.items()):
                try:
                    wrapper.close()
                    print(f"   ✅ 已关闭 {account_id} 的Context")
                except Exception as e:
                    print(f"   ⚠️ 关闭 {account_id} Context失败: {e}")
            self._contexts.clear()

            # 关闭所有Browser
            for i, browser in enumerate(self._browsers):
                if browser:
                    try:
                        browser.close()
                        print(f"   ✅ 已关闭 Browser {i}")
                    except Exception as e:
                        print(f"   ⚠️ 关闭 Browser {i} 失败: {e}")
                    self._browsers[i] = None

            # 关闭Playwright
            if self._playwright:
                self._playwright.stop()
                self._playwright = None

        self._initialized = False
        print("✅ 浏览器池已关闭")

    def _get_browser_launch_args(self) -> Dict:
        """获取浏览器启动参数"""
        return {
            'headless': self.headless,
            'args': [
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-setuid-sandbox',
                '--no-sandbox',
                '--disable-extensions',
                '--disable-plugins',
            ]
        }

    def _create_browser(self, index: int) -> Optional[Browser]:
        """创建Browser实例

        当检测到 "Connection closed while reading from the driver" 错误时，
        自动触发 driver 重启并重试一次（Fix 2+5：自动检测+跨任务自愈）。
        """
        if not self._playwright:
            return None

        launch_args = self._get_browser_launch_args()

        def _do_launch() -> Browser:
            """执行实际的 browser launch"""
            if BROWSER_TYPE == "webkit":
                return self._playwright.webkit.launch(**launch_args)
            elif BROWSER_TYPE == "firefox":
                return self._playwright.firefox.launch(**launch_args)
            else:
                return self._playwright.chromium.launch(**launch_args)

        try:
            browser = _do_launch()
            self._browsers[index] = browser
            self._browser_context_counts[index] = 0
            self._driver_consecutive_errors = 0  # 成功后重置计数
            print(f"   ✅ Browser {index} 创建成功 ({BROWSER_TYPE})")
            return browser

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Browser {index} 创建失败: {e}")

            # 检测 driver 连接断开的特征错误（Fix 2+5）
            is_driver_error = "Connection closed while reading from the driver" in error_msg

            if is_driver_error and not self._driver_restarting:
                self._driver_consecutive_errors += 1
                print(
                    f"   ⚠️ 检测到 Playwright driver 连接断开"
                    f"（连续第 {self._driver_consecutive_errors} 次），触发自动恢复..."
                )
                self._driver_restarting = True
                try:
                    # 自动恢复 driver（包含：清理孤儿进程、sleep、验证）
                    self._restart_playwright_driver_verified()

                    # driver 重启后旧的 Browser 对象全部失效，必须清理
                    # （当前 index 的 browser 就是本次要创建的，无需额外清理）
                    for i, old_browser in enumerate(self._browsers):
                        if old_browser is not None and i != index:
                            try:
                                old_browser.close()
                            except Exception:
                                pass
                            self._browsers[i] = None
                            self._browser_context_counts[i] = 0

                    # 恢复后立即重试一次 launch
                    if self._playwright:
                        try:
                            browser = _do_launch()
                            self._browsers[index] = browser
                            self._browser_context_counts[index] = 0
                            self._driver_consecutive_errors = 0
                            print(f"   ✅ Browser {index} 自动恢复成功 ({BROWSER_TYPE})")
                            return browser
                        except Exception as retry_e:
                            print(f"   ❌ Browser {index} 恢复后重试仍失败: {retry_e}")

                except Exception as recover_e:
                    print(f"   ❌ Playwright driver 自动恢复失败: {recover_e}")
                finally:
                    self._driver_restarting = False

            return None

    def _is_browser_healthy(self, browser: Browser) -> bool:
        """检查浏览器是否健康可用

        Args:
            browser: 浏览器实例

        Returns:
            bool: 是否健康
        """
        if browser is None:
            return False

        try:
            # 检查 is_connected 属性（如果存在）
            if hasattr(browser, 'is_connected'):
                if not browser.is_connected():
                    return False

            # 尝试获取 contexts 列表来验证浏览器是否可用
            # 这是一个轻量级操作
            _ = browser.contexts
            return True

        except Exception as e:
            log_warn(f"浏览器健康检查失败: {e}")
            return False

    def _rebuild_browser(self, index: int) -> Optional[Browser]:
        """重建指定索引的浏览器

        先清理失效的浏览器，再创建新的

        Args:
            index: 浏览器索引

        Returns:
            Browser: 新创建的浏览器，失败返回 None
        """
        log_info(f"正在重建 Browser {index}...")

        # 清理旧浏览器
        old_browser = self._browsers[index]
        if old_browser:
            try:
                old_browser.close()
            except Exception as e:
                log_warn(f"关闭旧 Browser {index} 失败: {e}")

            self._browsers[index] = None
            self._browser_context_counts[index] = 0

        # 清理关联的 Context
        contexts_to_remove = []
        for account_id, wrapper in self._contexts.items():
            if wrapper.browser_index == index:
                contexts_to_remove.append(account_id)

        for account_id in contexts_to_remove:
            try:
                wrapper = self._contexts.pop(account_id)

                # 【方案A】清理前尝试保存Cookie（可能失败，因为Browser已失效）
                try:
                    cookies = wrapper.get_cookies()
                    if cookies:
                        upload_cookie_sync(account_id, cookies)
                        log_info(f"清理前已保存Cookie: {account_id}")
                except Exception:
                    pass  # Browser失效时无法获取Cookie，忽略

                wrapper.close()
                log_info(f"清理失效 Context: {account_id}")
            except Exception as e:
                log_warn(f"清理 Context {account_id} 失败: {e}")

        # 创建新浏览器
        new_browser = self._create_browser(index)
        if new_browser:
            log_info(f"Browser {index} 重建成功")
        else:
            log_error(
                f"Browser {index} 重建失败",
                context="BrowserPoolManager._rebuild_browser"
            )

        return new_browser

    def _find_available_browser(self) -> Tuple[int, Browser]:
        """找到一个可用的Browser

        优先选择Context数量最少的健康Browser
        如果找到不健康的Browser，自动重建
        如果都满了，创建新的Browser

        Returns:
            (browser_index, browser)
        """
        # 找Context数量最少的健康Browser
        min_count = float('inf')
        min_index = -1
        unhealthy_browsers = []

        for i, browser in enumerate(self._browsers):
            if browser is not None:
                # 健康检查
                if not self._is_browser_healthy(browser):
                    log_warn(f"Browser {i} 健康检查失败，标记为需要重建")
                    unhealthy_browsers.append(i)
                    continue

                count = self._browser_context_counts[i]
                if count < self.max_contexts_per_browser and count < min_count:
                    min_count = count
                    min_index = i

        # 重建不健康的浏览器
        for i in unhealthy_browsers:
            self._rebuild_browser(i)

        # 如果找到了可用的健康Browser
        if min_index >= 0:
            return min_index, self._browsers[min_index]

        # 没有可用的，创建新的Browser
        for i in range(self.max_browsers):
            if self._browsers[i] is None:
                browser = self._create_browser(i)
                if browser:
                    return i, browser

        # 所有Browser都满了，尝试重建一个之前失败的
        for i in unhealthy_browsers:
            if self._browsers[i] is not None:
                return i, self._browsers[i]

        # 所有Browser都满了
        log_error(
            "浏览器池已满，无法创建新的Context",
            context="BrowserPoolManager._find_available_browser"
        )
        raise RuntimeError("浏览器池已满，无法创建新的Context")

    def get_context(self, account_id: str, cookies: Dict = None,
                    max_retries: int = 2) -> Optional[ContextWrapper]:
        """获取账号的Context

        如果已存在且有效，直接返回
        如果已存在但失效，自动清理并重新创建
        如果不存在，创建新的Context并加载Cookie
        包含自动重连和异常恢复机制

        Args:
            account_id: 账号ID
            cookies: Cookie字典（创建新Context时使用）
            max_retries: 最大重试次数

        Returns:
            ContextWrapper: Context包装器，失败返回 None
        """
        for attempt in range(1, max_retries + 1):
            try:
                with self._lock:
                    # 检查是否已存在
                    if account_id in self._contexts:
                        wrapper = self._contexts[account_id]

                        # 健康检查
                        if wrapper.is_valid():
                            wrapper.update_last_used()
                            return wrapper
                        else:
                            # Context 失效，清理并重新创建
                            log_warn(f"[{account_id}] 已有 Context 失效，正在清理并重新创建...")
                            upload_error_log(
                                error_type="ContextInvalid",
                                error_message=f"已有 Context 失效，正在重新创建",
                                context="BrowserPoolManager.get_context",
                                account_id=account_id
                            )

                            # 清理失效的 wrapper
                            browser_index = wrapper.browser_index
                            try:
                                wrapper.close()
                            except Exception:
                                pass
                            del self._contexts[account_id]
                            self._browser_context_counts[browser_index] = max(0, self._browser_context_counts[browser_index] - 1)

                            # 使用保存的 cookies 或传入的 cookies
                            if not cookies and wrapper.cookies:
                                cookies = wrapper.cookies

                    # 创建新的Context
                    browser_index, browser = self._find_available_browser()

                    # 再次验证浏览器健康状态
                    if not self._is_browser_healthy(browser):
                        log_warn(f"Browser {browser_index} 在创建 Context 前发现不健康，重建中...")
                        browser = self._rebuild_browser(browser_index)
                        if not browser:
                            raise RuntimeError(f"Browser {browser_index} 重建失败")

                    # 创建Context
                    try:
                        context = browser.new_context()
                    except Exception as e:
                        error_msg = str(e)
                        if 'Target' in error_msg and 'closed' in error_msg.lower():
                            # 浏览器已关闭，需要重建
                            log_warn(f"Browser {browser_index} 已关闭，尝试重建...")
                            browser = self._rebuild_browser(browser_index)
                            if browser:
                                context = browser.new_context()
                            else:
                                raise RuntimeError(f"Browser {browser_index} 重建失败")
                        else:
                            raise

                    # 加载Cookie
                    if cookies:
                        playwright_cookies = self._convert_cookies(cookies)
                        context.add_cookies(playwright_cookies)

                    # 创建包装器
                    wrapper = ContextWrapper(account_id, context, browser_index)
                    wrapper.cookies = cookies or {}

                    # 保存到映射
                    self._contexts[account_id] = wrapper
                    self._browser_context_counts[browser_index] += 1

                    # 【新增】将账号添加到已知账号集合（永久保留）
                    self._known_accounts.add(account_id)

                    log_info(f"为 {account_id} 创建新Context (Browser {browser_index})")

                    return wrapper

            except Exception as e:
                log_error(
                    f"[{account_id}] get_context 第 {attempt} 次尝试失败",
                    error=e,
                    context="BrowserPoolManager.get_context",
                    account_id=account_id
                )

                if attempt < max_retries:
                    log_info(f"[{account_id}] 等待 2 秒后重试...")
                    time.sleep(2)
                else:
                    log_error(
                        f"[{account_id}] get_context 最终失败，已重试 {max_retries} 次",
                        error=e,
                        context="BrowserPoolManager.get_context",
                        account_id=account_id
                    )
                    return None

        return None

    def _convert_cookies(self, cookies: Dict) -> List[Dict]:
        """将Cookie字典转换为Playwright格式"""
        playwright_cookies = []
        for name, value in cookies.items():
            cookie = {
                'name': name,
                'value': str(value),
                'domain': '.dianping.com',
                'path': '/'
            }
            playwright_cookies.append(cookie)
        return playwright_cookies

    def has_context(self, account_id: str) -> bool:
        """检查是否有账号的Context"""
        with self._lock:
            return account_id in self._contexts

    def remove_context(self, account_id: str, skip_cookie_upload: bool = False):
        """移除账号的Context（安全处理异常）

        Args:
            account_id: 账号ID
            skip_cookie_upload: 是否跳过Cookie上传（登录失效时应为True，避免覆盖auth_status）
        """
        with self._lock:
            if account_id in self._contexts:
                wrapper = self._contexts[account_id]
                browser_index = wrapper.browser_index

                # 【方案A】移除前先保存Cookie（登录失效时跳过，避免覆盖auth_status=invalid）
                if skip_cookie_upload:
                    log_info(f"跳过Cookie上传（登录已失效）: {account_id}")
                else:
                    try:
                        cookies = wrapper.get_cookies()
                        if cookies:
                            upload_cookie_sync(account_id, cookies)
                            log_info(f"移除前已保存Cookie: {account_id}")
                    except Exception:
                        pass  # 忽略获取Cookie失败

                # 安全关闭 wrapper（忽略错误）
                try:
                    wrapper.close()
                except Exception as e:
                    log_warn(f"关闭 {account_id} 的 Context 时出错: {e}")

                del self._contexts[account_id]
                self._browser_context_counts[browser_index] = max(0, self._browser_context_counts[browser_index] - 1)

                log_info(f"已移除 {account_id} 的Context")

    def get_all_account_ids(self) -> List[str]:
        """获取所有账号ID"""
        with self._lock:
            return list(self._contexts.keys())

    def get_context_count(self) -> int:
        """获取Context总数"""
        with self._lock:
            return len(self._contexts)

    def get_browser_count(self) -> int:
        """获取活跃Browser数量"""
        with self._lock:
            return sum(1 for b in self._browsers if b is not None)

    def _save_state(self):
        """保存状态到文件

        【新方案】保存两部分数据：
        1. known_accounts: 本服务器管理的所有账号ID（永久保留，释放时不删除）
        2. contexts: 当前活跃的Context及其Cookie
        """
        state = {
            'saved_at': datetime.now().isoformat(),
            'known_accounts': [],  # 【新增】所有已知账号列表
            'contexts': {}
        }

        with self._lock:
            # 【新增】保存所有已知账号
            state['known_accounts'] = list(self._known_accounts)

            for account_id, wrapper in self._contexts.items():
                # 获取最新Cookie
                cookies = wrapper.get_cookies()
                state['contexts'][account_id] = {
                    'cookies': cookies,
                    'last_used_at': wrapper.last_used_at.isoformat(),
                    'last_keepalive_at': wrapper.last_keepalive_at.isoformat() if wrapper.last_keepalive_at else None
                }

        # 保存到文件
        state_file = os.path.join(STATE_DIR, BROWSER_POOL_STATE_FILE)
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            print(f"   ✅ 状态已保存到 {state_file}（{len(state['known_accounts'])} 个账号）")
        except Exception as e:
            print(f"   ⚠️ 保存状态失败: {e}")

    def _restore_state(self):
        """从文件恢复状态

        【新方案】恢复逻辑：
        1. 从状态文件读取 known_accounts（本服务器管理的所有账号）
        2. 从状态文件的 contexts 恢复有本地Cookie的账号
        3. 对于没有本地Cookie的账号，从API获取Cookie并恢复

        注意：不再调用 fetch_all_account_ids()，因为该API不存在
        """
        state_file = os.path.join(STATE_DIR, BROWSER_POOL_STATE_FILE)
        restored_accounts = set()  # 记录已恢复的账号

        # ========== 步骤1：读取状态文件 ==========
        known_accounts = []
        contexts_data = {}
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                # 【新增】读取已知账号列表
                known_accounts = state.get('known_accounts', [])
                contexts_data = state.get('contexts', {})
                print(f"   📂 状态文件: {len(known_accounts)} 个已知账号，{len(contexts_data)} 个有Cookie")
            except Exception as e:
                print(f"   ⚠️ 读取状态文件失败: {e}")

        # 【新增】恢复已知账号集合
        with self._lock:
            self._known_accounts = set(known_accounts)

        if not known_accounts:
            print("   📝 无历史账号记录")
            return

        # ========== 步骤2：从本地Cookie恢复 ==========
        local_restored = 0
        for account_id in known_accounts:
            if account_id in contexts_data:
                try:
                    cookies = contexts_data[account_id].get('cookies', {})
                    if cookies:
                        self.get_context(account_id, cookies)
                        restored_accounts.add(account_id)
                        local_restored += 1
                except Exception as e:
                    print(f"   ⚠️ 从本地恢复 {account_id} 失败: {e}")

        print(f"   ✅ 从本地Cookie恢复 {local_restored} 个账号")

        # ========== 步骤3：从API补全没有本地Cookie的账号 ==========
        missing_accounts = [acc for acc in known_accounts if acc not in restored_accounts]

        if not missing_accounts:
            print("   ✅ 所有账号已恢复，无需从API补全")
            return

        print(f"   🔄 从API补全 {len(missing_accounts)} 个缺失账号的Cookie...")

        api_restored = 0
        api_failed = 0
        for account_id in missing_accounts:
            try:
                # 从API获取Cookie（POST /api/get_platform_account）
                cookies = fetch_account_cookie(account_id)
                if cookies:
                    self.get_context(account_id, cookies)
                    restored_accounts.add(account_id)
                    api_restored += 1
                    print(f"   ✅ 从API补全: {account_id}")
                else:
                    api_failed += 1
                    print(f"   ⚠️ {account_id} 无有效Cookie，跳过")
            except Exception as e:
                api_failed += 1
                print(f"   ⚠️ 从API恢复 {account_id} 失败: {e}")

        print(f"   ✅ 从API补全完成: 成功 {api_restored} 个，失败 {api_failed} 个")
        print(f"   📊 总计恢复: {len(restored_accounts)} 个账号")

    def _kill_orphan_playwright_processes(self):
        """强制清理残留的 playwright/node 子进程

        当 playwright.stop() 因异常无法正常终止 driver 进程时调用，
        防止新 driver 启动时与旧进程冲突（管道/端口占用）。
        使用 SIGKILL 确保进程被强制终止。
        """
        try:
            result = subprocess.run(
                ['pgrep', '-f', 'playwright/cli'],
                capture_output=True, text=True, timeout=5
            )
            pids = [p.strip() for p in result.stdout.strip().split('\n') if p.strip()]
            if pids:
                print(f"   🔪 发现 {len(pids)} 个残留 playwright 进程，强制清理...")
                for pid in pids:
                    try:
                        subprocess.run(['kill', '-9', pid], capture_output=True, timeout=3)
                        print(f"      已终止 PID={pid}")
                    except Exception:
                        pass
                time.sleep(0.5)  # 等待进程退出
        except Exception as e:
            print(f"   ⚠️ 清理残留进程异常（忽略）: {e}")

    def _restart_playwright_driver_verified(self):
        """停止旧 driver，启动并验证新 driver 可用

        核心修复逻辑，解决以下问题：
        1. sync_playwright().start() 成功但 driver 进程未完全就绪（Fix 4：加 sleep）
        2. playwright.stop() 失败导致旧进程残留冲突（Fix 3：清理孤儿进程）
        3. driver 启动后无法 launch browser（Fix 1：实际 launch 验证）
        4. 所有重试均内置指数退避

        注意：在 self._lock 持有期间调用是安全的（RLock 可重入）。

        Raises:
            RuntimeError: 所有重试均失败时抛出，由调用方决定如何处理
        """
        # 1. 停止旧 driver
        if self._playwright:
            try:
                self._playwright.stop()
                print("   ✅ Playwright driver 已停止")
            except Exception as e:
                print(f"   ⚠️ 停止 Playwright driver 失败: {e}，尝试强制清理残留进程")
                self._kill_orphan_playwright_processes()
            self._playwright = None

        # 2. 等待旧进程完全退出，避免 pipe/socket 冲突
        time.sleep(DRIVER_RESTART_SLEEP_SEC)

        # 3. 启动新 driver 并验证（最多 BROWSER_MAX_RESTART_RETRIES 次）
        last_error = None
        for attempt in range(1, BROWSER_MAX_RESTART_RETRIES + 1):
            try:
                # 3a. 启动 driver
                self._playwright = sync_playwright().start()

                # 3b. 等待 Node.js driver 进程完全就绪（关键！）
                time.sleep(DRIVER_RESTART_SLEEP_SEC)

                # 3c. 验证：实际 launch + 立即关闭一个测试浏览器
                #     仅用于验证，不放入浏览器池
                verify_args = {
                    'headless': True,
                    'args': ['--no-sandbox', '--disable-setuid-sandbox',
                             '--disable-dev-shm-usage']
                }
                test_browser = None
                try:
                    if BROWSER_TYPE == "webkit":
                        test_browser = self._playwright.webkit.launch(**verify_args)
                    elif BROWSER_TYPE == "firefox":
                        test_browser = self._playwright.firefox.launch(**verify_args)
                    else:
                        test_browser = self._playwright.chromium.launch(**verify_args)
                    test_browser.close()
                    test_browser = None
                except Exception as ve:
                    # 验证失败：driver 启动了但无法 launch browser
                    if test_browser:
                        try:
                            test_browser.close()
                        except Exception:
                            pass
                    raise RuntimeError(f"driver 验证失败: {ve}") from ve

                print(f"   ✅ Playwright driver 已重启并验证（第 {attempt} 次）")
                self._driver_consecutive_errors = 0
                return  # 成功

            except Exception as e:
                last_error = e
                print(f"   ❌ Playwright driver 重启失败（第 {attempt}/{BROWSER_MAX_RESTART_RETRIES} 次）: {e}")

                # 清理失败的 playwright 实例
                if self._playwright:
                    try:
                        self._playwright.stop()
                    except Exception:
                        pass
                    self._playwright = None

                # 强制清理可能残留的孤儿进程
                self._kill_orphan_playwright_processes()

                if attempt < BROWSER_MAX_RESTART_RETRIES:
                    wait_sec = 2 ** attempt  # 指数退避：2s, 4s
                    print(f"   ⏳ 等待 {wait_sec} 秒后重试...")
                    time.sleep(wait_sec)

        raise RuntimeError(
            f"Playwright driver 经过 {BROWSER_MAX_RESTART_RETRIES} 次重试仍无法启动: {last_error}"
        )

    def restart_browsers(self):
        """重启所有Browser（用于释放内存）

        【新方案】使用 _known_accounts 恢复，不依赖 fetch_all_account_ids()
        同时重启 Playwright driver，避免长时间运行后 driver 进入异常状态
        导致 "Connection closed while reading from the driver" 错误。
        """
        print("\n🔄 开始重启浏览器...")

        with self._lock:
            # 保存所有账号的Cookie
            saved_cookies = {}
            for account_id, wrapper in self._contexts.items():
                saved_cookies[account_id] = wrapper.get_cookies()

            # 关闭所有Context
            for wrapper in self._contexts.values():
                try:
                    wrapper.close()
                except:
                    pass
            self._contexts.clear()

            # 关闭所有Browser
            for i, browser in enumerate(self._browsers):
                if browser:
                    try:
                        browser.close()
                    except:
                        pass
                    self._browsers[i] = None
            self._browser_context_counts = [0] * self.max_browsers

            print("   ✅ 所有Browser已关闭")

            # 重启 Playwright driver（含等待和验证）
            # 使用 _restart_playwright_driver_verified() 统一处理：
            #   - 清理残留孤儿进程（Fix 3）
            #   - 启动后 sleep 等待就绪（Fix 4）
            #   - 实际 launch 测试浏览器验证（Fix 1）
            #   - 失败时指数退避重试
            self._restart_playwright_driver_verified()

            # 【关键修复】不再预建 Context，改为把 Cookie 上传到服务器
            # 原因：预建所有账号 Context 会触发 enforce_context_limit()，
            # 因为 MAX_ACTIVE_CONTEXTS=10 远小于账号总数（~150），
            # 导致重启后立即批量清洗，所有账号的 Context 全部丢失。
            # 修复方案：把 Cookie 上传到服务器保存，Context 由任务按需懒建。
            uploaded = 0
            for account_id, cookies in saved_cookies.items():
                if cookies:
                    try:
                        upload_cookie_sync(account_id, cookies)
                        uploaded += 1
                    except Exception as e:
                        print(f"   ⚠️ 上传Cookie失败 {account_id}: {e}")

            print(f"   ✅ 已将 {uploaded} 个账号的Cookie上传到服务器，Context 将按需懒建")

        self._last_restart_date = datetime.now().strftime("%Y-%m-%d")

    def check_and_restart(self):
        """检查是否需要重启（每天一次）"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_hour = datetime.now().hour

        # 检查是否到了重启时间
        if current_hour == BROWSER_RESTART_HOUR and self._last_restart_date != current_date:
            print(f"\n⏰ 到达每日重启时间 ({BROWSER_RESTART_HOUR}:00)")

            for attempt in range(BROWSER_MAX_RESTART_RETRIES):
                try:
                    self.restart_browsers()
                    return True
                except Exception as e:
                    print(f"   ⚠️ 重启失败 (尝试 {attempt + 1}/{BROWSER_MAX_RESTART_RETRIES}): {e}")
                    time.sleep(5)

            print("   ❌ 重启失败，将在下次尝试")
            return False

        return True

    def _cleanup_dead_browsers(self):
        """清理失效的浏览器

        扫描所有浏览器实例，清理已失效的引用
        注意：调用此方法前需要持有 _lock
        """
        for i, browser in enumerate(self._browsers):
            if browser is not None and not self._is_browser_healthy(browser):
                log_warn(f"发现失效浏览器 Browser {i}，正在清理...")

                # 清理该浏览器上的所有 context
                contexts_to_remove = [
                    account_id for account_id, wrapper in self._contexts.items()
                    if wrapper.browser_index == i
                ]

                for account_id in contexts_to_remove:
                    try:
                        wrapper = self._contexts.pop(account_id)
                        try:
                            wrapper.close()
                        except Exception:
                            pass
                        log_info(f"清理失效浏览器上的 Context: {account_id}")
                    except Exception as e:
                        log_warn(f"清理 Context {account_id} 失败: {e}")

                # 尝试关闭浏览器
                try:
                    browser.close()
                except Exception:
                    pass

                # 清理引用
                self._browsers[i] = None
                self._browser_context_counts[i] = 0

                log_info(f"Browser {i} 已清理")

    def release_idle_contexts(self) -> int:
        """释放空闲的Context（资源紧张时调用）

        释放超过 CONTEXT_IDLE_TIMEOUT 未使用的 Context
        同时检查并清理失效的浏览器

        Returns:
            int: 释放的Context数量
        """
        now = datetime.now()
        released = 0

        with self._lock:
            # 先清理失效的浏览器
            self._cleanup_dead_browsers()

            # 找出空闲的Context
            idle_accounts = []
            for account_id, wrapper in self._contexts.items():
                idle_time = (now - wrapper.last_used_at).total_seconds()
                if idle_time >= CONTEXT_IDLE_TIMEOUT:
                    idle_accounts.append((account_id, idle_time))

            # 按空闲时间排序（最久未使用的优先释放）
            idle_accounts.sort(key=lambda x: x[1], reverse=True)

            # 释放空闲Context
            for account_id, idle_time in idle_accounts:
                try:
                    wrapper = self._contexts.pop(account_id)
                    browser_index = wrapper.browser_index

                    # 【方案A】释放前先上传Cookie到服务器，确保不丢失
                    try:
                        cookies = wrapper.get_cookies()
                        if cookies:
                            upload_cookie_sync(account_id, cookies)
                            log_info(f"空闲释放前已保存Cookie: {account_id}")
                    except Exception as e:
                        log_warn(f"空闲释放前保存Cookie失败: {account_id} - {e}")

                    try:
                        wrapper.close()
                    except Exception:
                        pass

                    self._browser_context_counts[browser_index] = max(0, self._browser_context_counts[browser_index] - 1)

                    log_info(f"释放空闲Context: {account_id}（空闲 {int(idle_time/60)} 分钟）")
                    released += 1

                except Exception as e:
                    log_warn(f"释放 {account_id} Context失败: {e}")

        if released > 0:
            log_info(f"共释放 {released} 个空闲Context")

        return released

    def enforce_context_limit(self) -> int:
        """强制执行Context数量限制

        当 Context 数量超过 MAX_ACTIVE_CONTEXTS 时，
        释放最久未使用的 Context
        同时检查并清理失效的浏览器

        Returns:
            int: 释放的Context数量
        """
        with self._lock:
            # 先清理失效的浏览器
            self._cleanup_dead_browsers()

            current_count = len(self._contexts)

            if current_count <= MAX_ACTIVE_CONTEXTS:
                return 0

            # 需要释放的数量
            to_release = current_count - MAX_ACTIVE_CONTEXTS

            # 按最后使用时间排序
            sorted_accounts = sorted(
                self._contexts.items(),
                key=lambda x: x[1].last_used_at
            )

            released = 0
            for account_id, wrapper in sorted_accounts:
                if released >= to_release:
                    break

                # 跳过正在执行任务的账号，避免强杀导致任务误失败累积重试次数
                if account_lock_manager.is_locked(account_id):
                    log_warn(f"超限释放跳过: {account_id} 正在执行任务，不强杀")
                    continue

                try:
                    browser_index = wrapper.browser_index

                    # 【方案A】释放前先上传Cookie到服务器，确保不丢失
                    try:
                        cookies = wrapper.get_cookies()
                        if cookies:
                            upload_cookie_sync(account_id, cookies)
                            log_info(f"超限释放前已保存Cookie: {account_id}")
                    except Exception as e:
                        log_warn(f"超限释放前保存Cookie失败: {account_id} - {e}")

                    try:
                        wrapper.close()
                    except Exception:
                        pass

                    del self._contexts[account_id]
                    self._browser_context_counts[browser_index] = max(0, self._browser_context_counts[browser_index] - 1)

                    log_info(f"超限释放Context: {account_id}")
                    released += 1

                except Exception as e:
                    log_warn(f"释放 {account_id} Context失败: {e}")

            if released > 0:
                log_info(f"超限释放 {released} 个Context（当前 {len(self._contexts)} 个）")

            return released

    def emergency_release(self) -> int:
        """紧急释放资源（资源危险时调用）

        释放一半的**可释放**Context来降低资源占用。
        正在执行任务的Context（已加锁）会被跳过，避免杀死正在运行的任务。
        同时清理失效的浏览器。

        Returns:
            int: 释放的Context数量
        """
        log_warn("紧急资源释放...")

        # 上传紧急释放事件
        upload_error_log(
            error_type="EmergencyRelease",
            error_message="资源紧张，触发紧急释放",
            context="BrowserPoolManager.emergency_release"
        )

        with self._lock:
            # 先清理失效的浏览器
            self._cleanup_dead_browsers()

            current_count = len(self._contexts)
            if current_count == 0:
                return 0

            # 过滤掉正在执行任务的Context（已加锁的账号不可释放）
            releasable = [
                (aid, w) for aid, w in self._contexts.items()
                if not account_lock_manager.is_locked(aid)
            ]
            locked_count = current_count - len(releasable)

            if not releasable:
                log_warn(f"紧急释放: 所有 {current_count} 个Context都在执行任务，无法释放，等待任务完成")
                return 0

            if locked_count > 0:
                log_info(f"紧急释放: 跳过 {locked_count} 个正在执行任务的Context")

            # 释放一半的可释放Context
            to_release = max(1, len(releasable) // 2)

            # 按最后使用时间排序（最久未使用的优先）
            sorted_accounts = sorted(
                releasable,
                key=lambda x: x[1].last_used_at
            )

            released = 0
            for account_id, wrapper in sorted_accounts[:to_release]:
                try:
                    browser_index = wrapper.browser_index

                    # 释放前先上传Cookie到服务器，确保不丢失
                    try:
                        cookies = wrapper.get_cookies()
                        if cookies:
                            upload_cookie_sync(account_id, cookies)
                            log_info(f"紧急释放前已保存Cookie: {account_id}")
                    except Exception as e:
                        log_warn(f"紧急释放前保存Cookie失败: {account_id} - {e}")

                    try:
                        wrapper.close()
                    except Exception:
                        pass

                    del self._contexts[account_id]
                    self._browser_context_counts[browser_index] = max(0, self._browser_context_counts[browser_index] - 1)

                    log_info(f"紧急释放: {account_id}")
                    released += 1

                except Exception as e:
                    log_warn(f"释放 {account_id} 失败: {e}")

            log_info(f"紧急释放完成: {released}/{to_release}（跳过{locked_count}个执行中，剩余 {len(self._contexts)} 个）")
            return released


# 全局浏览器池管理器
browser_pool: Optional[BrowserPoolManager] = None


def get_browser_pool() -> BrowserPoolManager:
    """获取全局浏览器池管理器"""
    global browser_pool
    if browser_pool is None:
        browser_pool = BrowserPoolManager(headless=True)
    return browser_pool


# ============================================================================
# 保活服务（同步模式 - 解决 Playwright greenlet 线程限制）
# ============================================================================

class KeepaliveService:
    """保活服务（同步模式 + 资源保护）

    注意：Playwright sync API 使用 greenlet 实现，对象只能在创建它的线程中使用。
    因此保活操作必须在主线程中执行，不能使用后台线程。

    资源保护机制：
    - 执行前检查CPU/内存使用率
    - 资源紧张时自动跳过保活
    - 失败账号有冷却时间，避免重复重试

    使用方式：在主循环的空闲时间调用 perform_keepalive_batch()
    """

    # 账号同步检查间隔（秒）：30分钟
    ACCOUNT_SYNC_INTERVAL = 30 * 60

    def __init__(self, pool: BrowserPoolManager, server_ip: str = None):
        self.pool = pool
        self.server_ip = server_ip
        self._last_full_cycle_time: Optional[datetime] = None
        self._last_account_sync_time: Optional[datetime] = None  # 上次账号同步时间
        self._fail_cooldown: Dict[str, datetime] = {}  # 失败账号冷却记录
        self._pending_sync_accounts: list = []  # 待渐进式补全的缺失账号列表

    def start(self):
        """启动保活服务（同步模式下仅打印提示）"""
        print("✅ 保活服务已启动（同步模式 + 资源保护）")
        resource_monitor.print_status()

    def stop(self):
        """停止保活服务（同步模式下仅打印提示）"""
        print("✅ 保活服务已停止")

    def _is_in_cooldown(self, account_id: str) -> bool:
        """检查账号是否在冷却期"""
        if account_id not in self._fail_cooldown:
            return False

        cooldown_until = self._fail_cooldown[account_id]
        if datetime.now() >= cooldown_until:
            # 冷却期结束，移除记录
            del self._fail_cooldown[account_id]
            return False

        return True

    def _set_cooldown(self, account_id: str):
        """设置账号冷却"""
        self._fail_cooldown[account_id] = datetime.now() + timedelta(seconds=KEEPALIVE_FAIL_COOLDOWN)

    def get_accounts_needing_keepalive(self) -> List[str]:
        """获取需要保活的账号列表

        遍历 _known_accounts（而非仅 _contexts），确保所有服务器分配的账号
        都能被触达。对于有 Context 的账号，检查 last_keepalive_at；
        对于无 Context 的账号（被释放过），也加入保活队列（会按需创建Context）。
        排除在冷却期的账号，按最后保活时间排序（最久未保活的优先）。

        Returns:
            List[str]: 需要保活的账号ID列表
        """
        accounts_to_keepalive = []
        now = datetime.now()

        with self.pool._lock:
            # 遍历所有已知账号（包括有Context和无Context的）
            for account_id in self.pool._known_accounts:
                # 检查是否在冷却期
                if self._is_in_cooldown(account_id):
                    continue

                if account_id in self.pool._contexts:
                    # 有 Context：检查保活间隔
                    wrapper = self.pool._contexts[account_id]
                    if wrapper.last_keepalive_at is None:
                        accounts_to_keepalive.append((account_id, datetime.min))
                    else:
                        time_since_keepalive = (now - wrapper.last_keepalive_at).total_seconds()
                        if time_since_keepalive >= KEEPALIVE_INTERVAL:
                            accounts_to_keepalive.append((account_id, wrapper.last_keepalive_at))
                else:
                    # 无 Context（被释放过）：需要保活（会按需创建Context）
                    accounts_to_keepalive.append((account_id, datetime.min))

        # 按最后保活时间排序（最久未保活的优先）
        accounts_to_keepalive.sort(key=lambda x: x[1])

        return [account_id for account_id, _ in accounts_to_keepalive]

    def sync_accounts_from_server(self):
        """从服务器同步账号到浏览器池，渐进式补全缺失的账号

        每隔 ACCOUNT_SYNC_INTERVAL 重新拉取全量账号列表。
        每次调用最多创建 KEEPALIVE_BATCH_SIZE 个Context（渐进式），
        避免一次性创建大量Context导致内存尖峰。
        缺失列表在多次调用间保留，逐步补全。
        """
        if not self.server_ip:
            return

        now = datetime.now()

        # 定期刷新缺失账号列表（首次 + 每30分钟）
        need_refresh = (
            self._last_account_sync_time is None
            or (now - self._last_account_sync_time).total_seconds() >= self.ACCOUNT_SYNC_INTERVAL
        )

        if need_refresh:
            self._last_account_sync_time = now
            try:
                # 1. 从API获取服务器应有的全量账号
                all_accounts = fetch_accounts_by_host(self.server_ip)
                if not all_accounts:
                    log_warn("账号同步: 未获取到任何账号，跳过同步")
                    self._pending_sync_accounts = []
                    return

                # 2. 获取浏览器池中已有的账号
                pool_accounts = set(self.pool.get_all_account_ids())

                # 3. 找出缺失的账号，保存到待同步列表
                self._pending_sync_accounts = [acc for acc in all_accounts if acc not in pool_accounts]

                if not self._pending_sync_accounts:
                    log_keepalive("-", f"账号同步: 浏览器池账号完整（{len(pool_accounts)}/{len(all_accounts)}），无需补全")
                    return

                log_keepalive("-", f"账号同步: 发现 {len(self._pending_sync_accounts)} 个缺失账号，将渐进式补全 "
                         f"(池中 {len(pool_accounts)}/{len(all_accounts)})")

            except Exception as e:
                log_error(f"账号同步异常: {e}", error=e, context="KeepaliveService.sync_accounts_from_server")
                return

        # 没有待补全的账号
        if not getattr(self, '_pending_sync_accounts', None):
            return

        # 渐进式补全：每次最多创建 KEEPALIVE_BATCH_SIZE 个
        batch = self._pending_sync_accounts[:KEEPALIVE_BATCH_SIZE]
        success_count = 0

        for account_id in batch:
            # 资源检查，资源紧张时停止补全
            if not resource_monitor.is_safe_for_keepalive():
                log_keepalive("-", f"账号同步: 资源紧张，暂停本轮补全（剩余 {len(self._pending_sync_accounts)} 个待补全）", "WARN")
                return  # 不从列表中移除，下次继续

            try:
                # 检查是否已在池中（可能被任务执行时按需创建了）
                if self.pool.has_context(account_id):
                    self._pending_sync_accounts.remove(account_id)
                    continue

                # 从API获取该账号的Cookie
                cookies = fetch_account_cookie(account_id)
                if not cookies:
                    log_keepalive(account_id, "账号同步: 无法获取Cookie，跳过", "WARN")
                    self._pending_sync_accounts.remove(account_id)
                    continue

                # 创建Context加入浏览器池
                wrapper = self.pool.get_context(account_id, cookies)
                if wrapper:
                    success_count += 1
                    log_keepalive(account_id, "账号同步: 已补全到浏览器池")
                else:
                    log_keepalive(account_id, "账号同步: 创建Context失败", "WARN")

                self._pending_sync_accounts.remove(account_id)

            except Exception as e:
                log_keepalive(account_id, f"账号同步: 补全异常: {e}", "WARN")
                self._pending_sync_accounts.remove(account_id)

            # 短暂等待，避免过快创建
            time.sleep(1)

        remaining = len(self._pending_sync_accounts)
        if success_count > 0 or remaining > 0:
            log_keepalive("-", f"账号同步本轮: 补全 {success_count} 个，剩余 {remaining} 个待补全")

    def perform_keepalive_batch(self, allow_sync: bool = False) -> int:
        """执行一批保活操作（同步，必须在主线程调用）

        会先检查资源状态，资源紧张时自动跳过。
        每次调用处理最多 KEEPALIVE_BATCH_SIZE 个账号。

        Args:
            allow_sync: 是否允许执行账号同步补全。
                        True = 当前确认处于空闲状态（连续无任务），可以补全缺失账号。
                        False = 可能随时有任务，仅对已有Context做保活，不创建新Context。

        Returns:
            int: 成功保活的账号数，-1 表示因资源问题跳过
        """
        # ===== 账号同步检查（仅在空闲时补全缺失账号） =====
        if allow_sync:
            try:
                self.sync_accounts_from_server()
            except Exception as e:
                log_warn(f"账号同步检查异常: {e}")

        # ===== 资源检查 =====
        if not resource_monitor.is_safe_for_keepalive():
            resource_monitor.print_status()
            print("   ⏸️ 资源紧张，跳过本次保活")
            return -1

        # 检查浏览器是否需要重启
        self.pool.check_and_restart()

        # 获取需要保活的账号
        accounts = self.get_accounts_needing_keepalive()

        if not accounts:
            return 0

        # 取一批
        batch = accounts[:KEEPALIVE_BATCH_SIZE]
        cooldown_count = len(self._fail_cooldown)

        log_keepalive("-", f"执行保活批次，本批 {len(batch)} 个（待保活 {len(accounts)}，冷却中 {cooldown_count}）")
        resource_monitor.print_status()

        success_count = 0
        for account_id in batch:
            # 每个账号执行前再次检查资源
            if not resource_monitor.is_safe_for_keepalive():
                log_keepalive("-", "资源紧张，中断本批保活", "WARN")
                break

            if self._keepalive_single(account_id):
                success_count += 1

        log_keepalive("-", f"本批保活完成: {success_count}/{len(batch)} 成功")

        return success_count

    def perform_full_keepalive_cycle(self) -> Tuple[int, int]:
        """执行完整的保活周期（所有账号）

        Returns:
            Tuple[int, int]: (成功数, 总数)
        """
        # 资源检查
        if not resource_monitor.is_safe_for_keepalive():
            resource_monitor.print_status()
            print("   ⏸️ 资源紧张，跳过完整保活周期")
            return 0, 0

        account_ids = self.pool.get_all_account_ids()

        if not account_ids:
            return 0, 0

        print(f"\n🔄 开始完整保活周期，共 {len(account_ids)} 个账号")

        success_count = 0
        for i, account_id in enumerate(account_ids):
            # 每个账号执行前检查资源
            if not resource_monitor.is_safe_for_keepalive():
                print(f"   ⏸️ 资源紧张，中断保活周期（已处理 {i}/{len(account_ids)}）")
                break

            if self._keepalive_single(account_id):
                success_count += 1

            # 每批之间短暂等待（避免资源峰值）
            if (i + 1) % KEEPALIVE_BATCH_SIZE == 0 and i + 1 < len(account_ids):
                print(f"   ⏳ 已处理 {i + 1}/{len(account_ids)}，短暂等待...")
                time.sleep(5)

        self._last_full_cycle_time = datetime.now()
        print(f"✅ 完整保活周期完成: {success_count}/{len(account_ids)} 成功")

        return success_count, len(account_ids)

    def _keepalive_single(self, account_id: str) -> bool:
        """对单个账号执行保活

        支持 Context 不存在时按需创建（从API获取Cookie）。
        使用安全的页面跳转方法，自动处理浏览器失效。

        Returns:
            bool: 是否成功
        """
        # 尝试获取锁（非阻塞）
        if not account_lock_manager.try_lock(account_id):
            log_keepalive(account_id, "正在执行任务，跳过保活")
            return False

        try:
            # 如果没有 Context，按需创建（从API获取Cookie）
            if not self.pool.has_context(account_id):
                # 资源紧张时不创建新 Context
                if not resource_monitor.is_safe_for_keepalive():
                    log_keepalive(account_id, "资源紧张，跳过无Context账号的保活", "WARN")
                    return False

                log_keepalive(account_id, "无Context，尝试从API获取Cookie并按需创建")
                cookies = fetch_account_cookie(account_id)
                if not cookies:
                    log_keepalive(account_id, "无法获取Cookie，跳过保活", "WARN")
                    self._set_cooldown(account_id)
                    return False

                wrapper = self.pool.get_context(account_id, cookies)
                if not wrapper:
                    log_keepalive(account_id, "创建Context失败，跳过保活", "WARN")
                    self._set_cooldown(account_id)
                    return False

                log_keepalive(account_id, "按需创建Context成功")
            else:
                wrapper = self.pool._contexts.get(account_id)
                if not wrapper:
                    return False

            # 使用健康检查验证 wrapper 是否有效
            if not wrapper.is_valid():
                log_keepalive(account_id, "Context 已失效，跳过保活并移除", "WARN")
                upload_error_log(
                    error_type="ContextInvalid",
                    error_message="保活时发现 Context 已失效",
                    context="KeepaliveService._keepalive_single",
                    account_id=account_id
                )
                # 移除失效的 context
                self.pool.remove_context(account_id)
                self._set_cooldown(account_id)
                return False

            log_keepalive(account_id, "开始保活")

            # 使用安全的页面跳转
            if not wrapper.safe_goto(KEEPALIVE_PAGE_URL, wait_until='load',
                                      timeout=KEEPALIVE_TIMEOUT, max_retries=1):
                log_keepalive(account_id, "保活页面跳转失败", "WARN")
                upload_error_log(
                    error_type="KeepaliveGotoFailed",
                    error_message="保活页面跳转失败",
                    context="KeepaliveService._keepalive_single",
                    account_id=account_id
                )
                self._set_cooldown(account_id)
                # 移除失效的 context
                self.pool.remove_context(account_id)
                return False

            time.sleep(1)  # 等待页面加载

            # 检查页面是否仍然有效
            try:
                current_url = wrapper.page.url
            except Exception as e:
                log_keepalive(account_id, f"获取页面URL失败: {e}", "ERROR")
                upload_error_log(
                    error_type="KeepalivePageError",
                    error_message=f"获取页面URL失败: {e}",
                    context="KeepaliveService._keepalive_single",
                    account_id=account_id
                )
                self._set_cooldown(account_id)
                self.pool.remove_context(account_id)
                return False

            # 检查是否被重定向到登录页
            if 'login' in current_url.lower():
                log_keepalive(account_id, "Cookie已失效，上报失效状态", "WARN")
                self._report_cookie_invalid(account_id)
                # 移除失效的Context（跳过Cookie上传，避免覆盖auth_status=invalid）
                self.pool.remove_context(account_id, skip_cookie_upload=True)
                self._set_cooldown(account_id)
                return False

            # 获取并上传Cookie
            cookies = {}
            try:
                cookies = wrapper.get_cookies()
                cookie_upload_queue.put(account_id, cookies)
            except Exception as e:
                log_keepalive(account_id, f"获取Cookie失败: {e}", "WARN")

            wrapper.update_last_keepalive()
            log_keepalive(account_id, "保活成功")

            # 评价回复已拆分为独立守护进程 review_reply_runner.py，不再从保活触发

            return True

        except Exception as e:
            log_keepalive(account_id, f"保活异常: {type(e).__name__}: {e}", "ERROR")
            upload_error_log(
                error_type="KeepaliveException",
                error_message=str(e),
                context="KeepaliveService._keepalive_single",
                account_id=account_id
            )
            self._set_cooldown(account_id)
            return False

        finally:
            account_lock_manager.release(account_id)

    def _report_cookie_invalid(self, account_id: str):
        """上报Cookie失效"""
        try:
            response = requests.post(
                PLATFORM_ACCOUNTS_API,
                json={
                    "account": account_id,
                    "auth_status": "invalid"
                },
                timeout=30
            )
            if response.status_code == 200:
                log_keepalive(account_id, "已上报Cookie失效")
        except Exception as e:
            log_keepalive(account_id, f"上报失效状态异常: {e}", "ERROR")


# ============================================================================
# 任务获取（带服务器IP）
# ============================================================================

def fetch_task_with_server_ip() -> Optional[Dict[str, Any]]:
    """获取任务（传入服务器IP）

    Returns:
        dict: 任务信息
        None: 无任务或获取失败
    """
    server_ip = get_cached_ip() or get_public_ip()

    if not server_ip:
        print("❌ 无法获取服务器IP，跳过任务获取")
        return None

    headers = {'Content-Type': 'application/json'}

    print(f"\n{'=' * 80}")
    print("📋 获取待执行任务")
    print(f"{'=' * 80}")
    print(f"   URL: {GET_TASK_API}")
    print(f"   Server IP: {server_ip}")

    try:
        response = requests.post(
            GET_TASK_API,
            json={"server": server_ip},
            headers=headers,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            result = response.json()
            task_data = result.get('data') if result.get('success') else None

            if task_data:
                print(f"\n📌 获取到任务:")
                print(f"   任务ID: {task_data.get('id')}")
                print(f"   账号: {task_data.get('account_id')}")
                print(f"   类型: {task_data.get('task_type')}")
                print(f"   日期: {task_data.get('data_start_date')} ~ {task_data.get('data_end_date')}")
                return task_data
            else:
                print("   📝 暂无待执行任务")
                return None
        else:
            print(f"   ❌ API返回错误: {response.status_code}")
            return None

    except Exception as e:
        print(f"   ❌ 获取任务异常: {e}")
        return None


# ============================================================================
# 信号处理
# ============================================================================

def _shutdown_handler(signum, frame):
    """关闭信号处理"""
    global _pool_running
    _pool_running = False

    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    print(f"\n{'=' * 60}")
    print(f"⚠️ 收到退出信号 ({sig_name})，正在优雅关闭...")
    print(f"{'=' * 60}")


def setup_shutdown_handlers():
    """设置关闭信号处理器"""
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)


# ============================================================================
# 主入口
# ============================================================================

def initialize_browser_pool(headless: bool = True) -> BrowserPoolManager:
    """初始化浏览器池

    Args:
        headless: 是否使用无头模式

    Returns:
        BrowserPoolManager: 浏览器池管理器
    """
    global browser_pool

    # 获取公网IP
    get_public_ip()

    # 创建并初始化浏览器池
    browser_pool = BrowserPoolManager(headless=headless)
    browser_pool.initialize()

    # 启动Cookie上传队列
    cookie_upload_queue.start()

    return browser_pool



def start_keepalive_service(pool: BrowserPoolManager, server_ip: str = None) -> KeepaliveService:
    """启动保活服务

    Args:
        pool: 浏览器池管理器
        server_ip: 服务器公网IP，用于定期同步账号列表
    """
    service = KeepaliveService(pool, server_ip=server_ip)
    service.start()
    return service


def shutdown_all():
    """关闭所有服务"""
    global browser_pool

    # 停止Cookie上传队列
    cookie_upload_queue.stop()

    # 关闭浏览器池
    if browser_pool:
        browser_pool.shutdown()
        browser_pool = None


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    print("浏览器池模块测试")
    print("=" * 60)

    # 测试获取公网IP
    ip = get_public_ip()
    print(f"公网IP: {ip}")

    # 测试浏览器池
    try:
        pool = initialize_browser_pool(headless=True)
        print(f"Browser数量: {pool.get_browser_count()}")
        print(f"Context数量: {pool.get_context_count()}")

        # 测试创建Context
        test_cookies = {"test_cookie": "test_value"}
        wrapper = pool.get_context("test_account", test_cookies)
        print(f"创建Context成功: {wrapper.account_id}")

        # 清理
        shutdown_all()

    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
