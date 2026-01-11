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


# ============================================================================
# 配置参数
# ============================================================================

# 浏览器池配置
MAX_BROWSERS = 10                    # 最大Browser数量
MAX_CONTEXTS_PER_BROWSER = 15        # 每个Browser最大Context数量
BROWSER_TYPE = "webkit"              # 浏览器类型: webkit / chromium / firefox

# 保活配置
KEEPALIVE_INTERVAL = 30 * 60         # 保活间隔（秒）：30分钟
KEEPALIVE_PAGE_URL = "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html"
KEEPALIVE_BATCH_SIZE = 5             # 每批保活账号数量（错峰）
KEEPALIVE_BATCH_INTERVAL = 60        # 每批之间的间隔（秒）

# 浏览器重启配置
BROWSER_RESTART_HOUR = 14            # 每天重启时间（14点，任务少的时候）
BROWSER_MAX_RESTART_RETRIES = 3      # 重启失败最大重试次数

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

        # 线程锁
        self._lock = threading.Lock()

        # 状态
        self._initialized = False
        self._last_restart_date: Optional[str] = None

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
        """创建Browser实例"""
        if not self._playwright:
            return None

        try:
            launch_args = self._get_browser_launch_args()

            if BROWSER_TYPE == "webkit":
                browser = self._playwright.webkit.launch(**launch_args)
            elif BROWSER_TYPE == "firefox":
                browser = self._playwright.firefox.launch(**launch_args)
            else:
                browser = self._playwright.chromium.launch(**launch_args)

            self._browsers[index] = browser
            self._browser_context_counts[index] = 0
            print(f"   ✅ Browser {index} 创建成功 ({BROWSER_TYPE})")
            return browser

        except Exception as e:
            print(f"   ❌ Browser {index} 创建失败: {e}")
            return None

    def _find_available_browser(self) -> Tuple[int, Browser]:
        """找到一个可用的Browser

        优先选择Context数量最少的Browser
        如果都满了，创建新的Browser

        Returns:
            (browser_index, browser)
        """
        # 找Context数量最少的Browser
        min_count = float('inf')
        min_index = -1

        for i, browser in enumerate(self._browsers):
            if browser is not None:
                count = self._browser_context_counts[i]
                if count < self.max_contexts_per_browser and count < min_count:
                    min_count = count
                    min_index = i

        # 如果找到了可用的Browser
        if min_index >= 0:
            return min_index, self._browsers[min_index]

        # 没有可用的，创建新的Browser
        for i in range(self.max_browsers):
            if self._browsers[i] is None:
                browser = self._create_browser(i)
                if browser:
                    return i, browser

        # 所有Browser都满了
        raise RuntimeError("浏览器池已满，无法创建新的Context")

    def get_context(self, account_id: str, cookies: Dict = None) -> ContextWrapper:
        """获取账号的Context

        如果已存在，直接返回
        如果不存在，创建新的Context并加载Cookie

        Args:
            account_id: 账号ID
            cookies: Cookie字典（创建新Context时使用）

        Returns:
            ContextWrapper: Context包装器
        """
        with self._lock:
            # 检查是否已存在
            if account_id in self._contexts:
                wrapper = self._contexts[account_id]
                wrapper.update_last_used()
                return wrapper

            # 创建新的Context
            browser_index, browser = self._find_available_browser()

            # 创建Context
            context = browser.new_context()

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

            print(f"   ✅ 为 {account_id} 创建新Context (Browser {browser_index})")

            return wrapper

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

    def remove_context(self, account_id: str):
        """移除账号的Context"""
        with self._lock:
            if account_id in self._contexts:
                wrapper = self._contexts[account_id]
                browser_index = wrapper.browser_index

                wrapper.close()
                del self._contexts[account_id]
                self._browser_context_counts[browser_index] -= 1

                print(f"   ✅ 已移除 {account_id} 的Context")

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
        """保存状态到文件"""
        state = {
            'saved_at': datetime.now().isoformat(),
            'contexts': {}
        }

        with self._lock:
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
            print(f"   ✅ 状态已保存到 {state_file}")
        except Exception as e:
            print(f"   ⚠️ 保存状态失败: {e}")

    def _restore_state(self):
        """从文件恢复状态"""
        state_file = os.path.join(STATE_DIR, BROWSER_POOL_STATE_FILE)

        if not os.path.exists(state_file):
            print("   📝 无历史状态文件，跳过恢复")
            return

        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)

            contexts_data = state.get('contexts', {})
            if not contexts_data:
                print("   📝 历史状态为空，跳过恢复")
                return

            print(f"   📂 发现 {len(contexts_data)} 个账号的历史状态，正在恢复...")

            restored = 0
            for account_id, data in contexts_data.items():
                try:
                    cookies = data.get('cookies', {})
                    if cookies:
                        self.get_context(account_id, cookies)
                        restored += 1
                except Exception as e:
                    print(f"   ⚠️ 恢复 {account_id} 失败: {e}")

            print(f"   ✅ 成功恢复 {restored} 个账号的Context")

        except Exception as e:
            print(f"   ⚠️ 恢复状态失败: {e}")

    def restart_browsers(self):
        """重启所有Browser（用于释放内存）"""
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

            # 重新创建Context
            restored = 0
            for account_id, cookies in saved_cookies.items():
                try:
                    self.get_context(account_id, cookies)
                    restored += 1
                except Exception as e:
                    print(f"   ⚠️ 恢复 {account_id} 失败: {e}")

            print(f"   ✅ 浏览器重启完成，恢复了 {restored} 个账号")

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


# 全局浏览器池管理器
browser_pool: Optional[BrowserPoolManager] = None


def get_browser_pool() -> BrowserPoolManager:
    """获取全局浏览器池管理器"""
    global browser_pool
    if browser_pool is None:
        browser_pool = BrowserPoolManager(headless=True)
    return browser_pool


# ============================================================================
# 保活服务
# ============================================================================

class KeepaliveService:
    """保活服务

    后台线程定期对所有账号执行保活操作
    采用错峰策略，避免资源峰值
    """

    def __init__(self, pool: BrowserPoolManager):
        self.pool = pool
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """启动保活服务"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._keepalive_worker, daemon=True)
        self._thread.start()
        print("✅ 保活服务已启动")

    def stop(self):
        """停止保活服务"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        print("✅ 保活服务已停止")

    def _keepalive_worker(self):
        """保活工作线程"""
        while self._running:
            try:
                # 检查浏览器是否需要重启
                self.pool.check_and_restart()

                # 获取所有账号
                account_ids = self.pool.get_all_account_ids()

                if not account_ids:
                    time.sleep(60)  # 没有账号，等待1分钟
                    continue

                print(f"\n🔄 开始保活轮询，共 {len(account_ids)} 个账号")

                # 错峰保活
                for i in range(0, len(account_ids), KEEPALIVE_BATCH_SIZE):
                    if not self._running:
                        break

                    batch = account_ids[i:i + KEEPALIVE_BATCH_SIZE]

                    for account_id in batch:
                        if not self._running:
                            break

                        self._keepalive_single(account_id)

                    # 批次间等待
                    if i + KEEPALIVE_BATCH_SIZE < len(account_ids):
                        print(f"   ⏳ 等待 {KEEPALIVE_BATCH_INTERVAL} 秒后继续下一批...")
                        time.sleep(KEEPALIVE_BATCH_INTERVAL)

                print(f"✅ 保活轮询完成，等待 {KEEPALIVE_INTERVAL // 60} 分钟后开始下一轮")

                # 等待下一轮
                for _ in range(KEEPALIVE_INTERVAL):
                    if not self._running:
                        break
                    time.sleep(1)

            except Exception as e:
                print(f"❌ 保活工作线程异常: {e}")
                time.sleep(60)

    def _keepalive_single(self, account_id: str):
        """对单个账号执行保活"""
        # 尝试获取锁（非阻塞）
        if not account_lock_manager.try_lock(account_id):
            print(f"   ⏭️ {account_id} 正在执行任务，跳过保活")
            return

        try:
            if not self.pool.has_context(account_id):
                return

            wrapper = self.pool._contexts.get(account_id)
            if not wrapper or not wrapper.page:
                return

            print(f"   🔄 保活 {account_id}...")

            # 访问保活页面
            try:
                wrapper.page.goto(KEEPALIVE_PAGE_URL, timeout=30000)
                time.sleep(2)  # 等待页面加载

                # 检查是否被重定向到登录页
                current_url = wrapper.page.url
                if 'login' in current_url.lower():
                    print(f"   ⚠️ {account_id} Cookie已失效，上报失效状态")
                    self._report_cookie_invalid(account_id)
                    return

                # 获取并上传Cookie
                cookies = wrapper.get_cookies()
                cookie_upload_queue.put(account_id, cookies)

                wrapper.update_last_keepalive()
                print(f"   ✅ {account_id} 保活成功")

            except Exception as e:
                print(f"   ⚠️ {account_id} 保活失败: {e}")

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
                print(f"   ✅ 已上报 {account_id} Cookie失效")
        except Exception as e:
            print(f"   ❌ 上报失效状态异常: {e}")


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


def start_keepalive_service(pool: BrowserPoolManager) -> KeepaliveService:
    """启动保活服务"""
    service = KeepaliveService(pool)
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
