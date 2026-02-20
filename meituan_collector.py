#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美团点评数据采集统一入口 - 单文件版本

直接在PyCharm中运行，修改下方的配置参数即可
包含: 6个数据采集任务 + store_stats门店统计任务(使用Playwright浏览器)
"""

import json
import time
import random
import requests
import requests.exceptions
import pandas as pd
import math
import os
import sys
import subprocess
import signal
import logging
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from datetime import datetime, timedelta
from io import BytesIO
from contextlib import contextmanager

# Playwright导入 (用于store_stats任务)
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ 未安装playwright，store_stats任务将不可用")
    print("   安装方法: pip install playwright && playwright install chromium")

# 浏览器池模块导入
try:
    from browser_pool import (
        initialize_browser_pool,
        start_keepalive_service,
        shutdown_all as shutdown_browser_pool,
        get_browser_pool,
        account_lock_manager,
        cookie_upload_queue,
        fetch_task_with_server_ip,
        get_public_ip,
        get_cached_ip,
        BrowserPoolManager,
        KeepaliveService,
        resource_monitor,  # 资源监控器
    )
    BROWSER_POOL_AVAILABLE = True
except ImportError as e:
    BROWSER_POOL_AVAILABLE = False
    print(f"⚠️ 浏览器池模块未加载: {e}")
    print("   将使用传统模式运行")


# ============================================================================
# ★★★ 在这里修改配置参数 ★★★
# ============================================================================
ACCOUNT_NAME = "13718175572a"       # 账户名称 (必填)
START_DATE = "2025-12-01"           # 开始日期 (必填, 格式: YYYY-MM-DD)
END_DATE = "2025-12-15"             # 结束日期 (必填, 格式: YYYY-MM-DD)
TARGET_DATE = ""                    # store_stats目标日期 (留空=使用END_DATE)
TASK = "all"                        # 任务名称 (必填)
                                    # 可选值:
                                    #   "all" - 页面驱动模式，先跳转页面再执行任务
                                    #           顺序: 报表页面 → 客流分析页面 → 评价页面
                                    #   "store_stats" - 门店统计(强制下线、客流、排名等)
                                    #   "kewen_daily_report" - 客文日报
                                    #   "promotion_daily_report" - 推广日报
                                    #   "review_detail_dianping" - 点评评价明细
                                    #   "review_detail_meituan" - 美团评价明细
                                    #   "review_summary_dianping" - 点评评价汇总
                                    #   "review_summary_meituan" - 美团评价汇总

# store_stats 浏览器配置
HEADLESS = True                     # 浏览器模式: True=后台运行, False=显示窗口

# ============================================================================
# ★★★ 守护进程模式配置 ★★★
# ============================================================================
DEV_MODE = True                     # 开发模式: True=24小时运行, False=仅在工作时间运行
WORK_START_HOUR = 8                 # 工作开始时间 (仅DEV_MODE=False时生效)
WORK_END_HOUR = 23                  # 工作结束时间 (仅DEV_MODE=False时生效)
NO_TASK_WAIT_SECONDS = 300          # 无任务时等待秒数 (5分钟)

# ============================================================================
# ★★★ 浏览器池模式配置 ★★★
# ============================================================================
USE_BROWSER_POOL = True             # 是否使用浏览器池模式 (推荐开启)
                                    # True: 使用浏览器池 + 保活机制
                                    # False: 传统模式，每次任务新建浏览器

# ============================================================================
# ★★★ 路径配置 (服务器部署时使用绝对路径) ★★★
# ============================================================================
DATA_DIR = "/home/meituan/data"                     # 数据根目录
STATE_DIR = "/home/meituan/data/state"              # Cookie状态文件目录
DOWNLOAD_DIR = "/home/meituan/data/downloads"       # 下载文件目录

# ============================================================================
# API配置 (一般不需要修改)
# ============================================================================
PLATFORM_ACCOUNTS_API_URL = "http://8.146.210.145:3000/api/get_platform_accounts"
LOG_API_URL = "http://8.146.210.145:3000/api/log"
AUTH_STATUS_API_URL = "http://8.146.210.145:3000/api/post/platform_accounts"  # 登录状态上报API
TASK_STATUS_BATCH_API_URL = "http://8.146.210.145:3000/api/account_task/update_batch"  # 任务状态批量上报API
TASK_STATUS_SINGLE_API_URL = "http://8.146.210.145:3000/api/account_task/update_single"  # 任务状态单独上报API
TASK_SCHEDULE_API_URL = "http://8.146.210.145:3000/api/post_task_schedule"  # 任务调度生成API
GET_TASK_API_URL = "http://8.146.210.145:3000/api/get_task"  # 获取任务API
TASK_CALLBACK_API_URL = "http://8.146.210.145:3000/api/task/callback"  # 任务完成回调API
RESCHEDULE_FAILED_API_URL = "http://8.146.210.145:3000/api/task/reschedule-failed"  # 失败任务重新调度API
TASK_RESET_API_URL = "http://8.146.210.145:3000/api/task/schedule/reset"  # 任务重置API（资源不足时归还任务）
GET_PLATFORM_ACCOUNT_API_URL = "http://8.146.210.145:3000/api/get_platform_account"  # 获取平台账户信息API
POST_STORES_REGIONS_API_URL = "http://8.146.210.145:3000/api/post/platform_accounts"  # 门店/商圈数据回传API
SAVE_DIR = DOWNLOAD_DIR  # 使用绝对路径

# ============================================================================
# 报表模板相关API配置
# ============================================================================
TEMPLATE_LIST_API = "https://e.dianping.com/gateway/adviser/report/template/list"
TEMPLATE_SAVE_API = "https://e.dianping.com/gateway/adviser/report/template/save"
PLATFORM_ACCOUNTS_UPDATE_API = "http://8.146.210.145:3000/api/platform-accounts"
TEMPLATES_ID_UPDATE_API_URL = "https://kewenai.asia/api/up/templates_id"

# 报表中心页面URL（用于获取/创建模板时跳转）
REPORT_CENTER_URL = "https://e.dianping.com/app/merchant-platform/0fb1bec0bade47d?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL3JlcG9ydC1jZW50ZXIvaW5kZXguaHRtbA"

# 创建报表模板时使用的固定指标列表
TEMPLATE_METRIC_LIST = ",".join([
    "basic_shop_dp_score",
    "derived_shop_mt_score",
    "basic_shop_operation_score",
    "basic_shop_operation_grade",
    "basic_acc_shop_cpc_charge",
    "basic_shop_mem_publish_amount",
    "basic_shop_commission_amount",
    "basic_shop_commission_base_amount",
    "basic_shop_view_uv",
    "basic_shop_view_cnt",
    "basic_shop_visit_uv",
    "basic_shop_visit_cnt",
    "derived_shop_click_rate",
    "basic_shop_buy_uv",
    "basic_shop_booking_uv_new",
    "basic_shop_intent_uv_new",
    "derived_shop_intent_rate_new",
    "basic_shop_daily_favorite_user_cnt",
    "basic_shop_favorite_user_cnt",
    "basic_shop_flow_view_avg_page_stay_time",
    "basic_acc_shop_cpc_view_cnt",
    "basic_acc_shop_cpc_click_cnt",
    "basic_shop_trade_csm_amt",
    "basic_shop_trade_csm_amt_no_shop_share",
    "basic_shop_trade_csm_cnt",
    "basic_shop_trade_csm_order_cnt",
    "basic_shop_trade_csm_uv",
    "basic_shop_trade_csm_new_user_cnt",
    "basic_shop_product_sale_cnt",
    "basic_shop_product_sale_amt",
    "basic_shop_ask_user_cnt",
    "basic_shop_im_booking_uv",
    "derived_shop_ask_booking_rate",
    "derived_shop_ask_avg_reply_duration",
    "derived_shop_ask_30sec_reply_rate",
    "derived_shop_ask_5min_reply_rate",
    "basic_shop_refund_order_amt",
    "basic_shop_refund_order_cnt",
    "basic_shop_refund_user_cnt",
    "basic_shop_csmdisp_complain_cnt",
    "basic_shop_cpst_order_cnt",
    "basic_shop_review_chosen_new_cnt",
    "basic_shop_review_good_new_cnt",
    "basic_shop_review_good_mid_cnt",
    "basic_shop_review_chosen_bad_new_cnt",
    "derived_shop_review_chosen_bad_reply_rate",
    "basic_shop_review_all_cnt",
    "basic_shop_review_all_bad_cnt",
    "basic_scan_pay_order_cnt",
    "basic_scan_pay_amt",
    "basic_scan_consume_amt",
    "basic_scan_user_cnt",
    "basic_scan_favorite_user_cnt",
    "basic_scan_comment_user_cnt",
])

# ============================================================================
# ★★★ 统一超时参数配置 ★★★
# ============================================================================
CONNECT_TIMEOUT = 10        # 连接建立超时（秒）
API_TIMEOUT = 30            # 普通API请求超时（秒）
DOWNLOAD_TIMEOUT = 120      # 文件下载超时（秒）
BROWSER_PAGE_TIMEOUT = 60000  # 浏览器页面加载超时（毫秒）
LOGIN_CHECK_TIMEOUT = 30000   # 登录检测超时（毫秒）

# 指数退避重试配置
MAX_RETRY_ATTEMPTS = 3      # 最大重试次数
INITIAL_RETRY_DELAY = 2     # 初始重试延迟（秒）
MAX_RETRY_DELAY = 60        # 最大重试延迟（秒）
RETRY_BACKOFF_FACTOR = 2    # 退避因子

# ============================================================================
# ★★★ 日志配置 ★★★
# ============================================================================
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# 配置日志
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.StreamHandler()  # 输出到控制台
    ]
)
logger = logging.getLogger(__name__)

# 各任务的上传API
UPLOAD_APIS = {
    "store_stats": "http://8.146.210.145:3000/api/store_stats",
    "kewen_daily_report": "http://8.146.210.145:3000/api/kewen_daily_report",
    "promotion_daily_report": "http://8.146.210.145:3000/api/promotion_daily_report",
    "review_detail_dianping": "http://8.146.210.145:3000/api/review_detail_dianping",
    "review_detail_meituan": "http://8.146.210.145:3000/api/review_detail_meituan",
    "review_summary_dianping": "http://8.146.210.145:3000/api/review_summary_dianping",
    "review_summary_meituan": "http://8.146.210.145:3000/api/review_summary_meituan",
}

# ============================================================================
# 页面驱动任务配置 - 先跳转页面再执行对应任务
# ============================================================================
PAGE_URLS = {
    # 报表页面 - 执行 kewen_daily_report, promotion_daily_report
    "report": "https://e.dianping.com/app/merchant-platform/0fb1bec0bade47d?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL3JlcG9ydC1jZW50ZXIvaW5kZXguaHRtbA",
    # 客流分析页面 - 执行 store_stats
    "flow_analysis": "https://e.dianping.com/app/merchant-platform/468ccfd01240492?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL2FkdmljZS1mbG93LWFuYWx5c2lzL2luZGV4Lmh0bWw",
    # 评价页面 - 执行 review_detail_dianping, review_detail_meituan, review_summary_dianping, review_summary_meituan
    "review": "https://e.dianping.com/app/merchant-platform/7dfe97aa7164460?iUrl=Ly9lLmRpYW5waW5nLmNvbS92Zy1wbGF0Zm9ybS1yZXZpZXdtYW5hZ2Uvc2hvcC1jb21tZW50LWRwL2luZGV4Lmh0bWw",
}

# 页面与任务的映射关系
PAGE_TASKS = {
    "report": ["kewen_daily_report", "promotion_daily_report"],
    "flow_analysis": ["store_stats"],
    "review": ["review_detail_dianping", "review_detail_meituan", "review_summary_dianping", "review_summary_meituan"],
}

# 页面执行顺序（客流分析先执行以更新签名，评价页面放最后）
PAGE_ORDER = ["flow_analysis", "report", "review"]

# ============================================================================
# ★★★ 任务独立页面URL配置 ★★★
# ============================================================================
# 首页（Cookie登录后跳转）
HOME_PAGE_URL = "https://e.dianping.com/app/merchant-platform/"

# 每个任务对应的独立页面URL
TASK_PAGE_URLS = {
    "kewen_daily_report": "https://e.dianping.com/app/merchant-platform/0fb1bec0bade47d?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL3JlcG9ydC1jZW50ZXIvaW5kZXguaHRtbA",
    "promotion_daily_report": "https://e.dianping.com/app/merchant-platform/e77c7f630ee64ca?iUrl=Ly9oNS5kaWFucGluZy5jb20vYXBwL21lcmNoYW50LW1hbmFnZS1hZHZpY2UtcGMtc3RhdGljL2RpYWdub3Npcy1ob21lLmh0bWw#menuId=1",
    "store_stats": "https://e.dianping.com/app/merchant-platform/468ccfd01240492?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL2FkdmljZS1mbG93LWFuYWx5c2lzL2luZGV4Lmh0bWw",
    "review_detail_dianping": "https://e.dianping.com/app/merchant-platform/7dfe97aa7164460?iUrl=Ly9lLmRpYW5waW5nLmNvbS92Zy1wbGF0Zm9ybS1yZXZpZXdtYW5hZ2Uvc2hvcC1jb21tZW50LWRwL2luZGV4Lmh0bWw",
    "review_detail_meituan": "https://e.dianping.com/app/merchant-platform/27e9b6df520b47f?iUrl=Ly9lLmRpYW5waW5nLmNvbS92Zy1wbGF0Zm9ybS1yZXZpZXdtYW5hZ2Uvc2hvcC1jb21tZW50LW10L2luZGV4Lmh0bWw",
    # 以下两个任务已禁用，无需跳转页面
    "review_summary_dianping": None,
    "review_summary_meituan": None,
}

# store_stats 内部数据采集页面URL
STORE_STATS_PAGE_URLS = {
    "flow_analysis": "https://e.dianping.com/app/merchant-platform/468ccfd01240492?iUrl=Ly9oNS5kaWFucGluZy5jb20vdmctcGMtYWR2aWNlL2FkdmljZS1mbG93LWFuYWx5c2lzL2luZGV4Lmh0bWw",
    "rival_analysis": "https://e.dianping.com/app/merchant-platform/fe6031ae4f544c4?iUrl=Ly9lLmRpYW5waW5nLmNvbS9hcHAvbWVyY2hhbnQtd29ya2JlbmNoL2luZGV4Lmh0bWwjLw",
    "trade_analysis": "https://e.dianping.com/app/merchant-platform/8b352a79fb3e44e?iUrl=Ly9oNS5kaWFucGluZy5jb20vYXBwL21lcmNoYW50LW1hbmFnZS1hZHZpY2UtcGMtc3RhdGljL2FkdmljZS10cmFkZS1hbmFseXNpcy5odG1s",
    "notice_center": "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html",
}

# 任务页面名称映射（用于日志显示）
TASK_PAGE_NAMES = {
    "kewen_daily_report": "报表中心",
    "promotion_daily_report": "推广中心",
    "store_stats": "客流分析",
    "review_detail_dianping": "点评评价",
    "review_detail_meituan": "美团评价",
    "review_summary_dianping": "点评评价汇总(已禁用)",
    "review_summary_meituan": "美团评价汇总(已禁用)",
}

# store_stats 内部页面名称
STORE_STATS_PAGE_NAMES = {
    "flow_analysis": "客流分析",
    "rival_analysis": "同行分析",
    "trade_analysis": "交易分析",
    "notice_center": "消息中心",
}

# ============================================================================
# ★★★ 任务禁用开关 ★★★
# ============================================================================
TASK_DISABLED_FLAGS = {
    "kewen_daily_report": False,
    "promotion_daily_report": False,
    "store_stats": False,
    "review_detail_dianping": False,
    "review_detail_meituan": False,
    "review_summary_dianping": True,   # ⚠️ 已禁用
    "review_summary_meituan": True,    # ⚠️ 已禁用
}

# 任务执行顺序（all模式下的执行顺序）
TASK_EXECUTION_ORDER = [
    "store_stats",
    "kewen_daily_report",
    "promotion_daily_report",
    "review_detail_dianping",
    "review_detail_meituan",
    "review_summary_dianping",
    "review_summary_meituan",
]

# ============================================================================
# 共享签名存储 (store_stats执行后更新，供其他任务使用)
# ============================================================================
SHARED_SIGNATURE = {
    'mtgsig': None,          # 签名字符串
    'cookies': None,         # 更新后的cookies
    'updated_at': None,      # 更新时间
    'shop_list': None,       # 门店列表
    'compare_regions': None, # 门店商圈信息（用于同行排名）
    'brands_json': None,     # 团购ID映射（用于广告单）
}


# ============================================================================
# 守护进程模式: 信号处理和时间窗口控制
# ============================================================================
# 全局运行标志 (用于优雅退出)
_daemon_running = True


def _signal_handler(signum, frame):
    """信号处理函数，用于优雅退出"""
    global _daemon_running
    _daemon_running = False
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    print(f"\n{'=' * 60}")
    print(f"⚠️ 收到退出信号 ({sig_name})，等待当前任务完成后退出...")
    print(f"{'=' * 60}")


def _setup_signal_handlers():
    """设置信号处理器"""
    signal.signal(signal.SIGINT, _signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, _signal_handler)  # kill命令
    print("✅ 已设置信号处理器 (支持Ctrl+C优雅退出)")


def is_in_work_window() -> bool:
    """检查当前是否在工作时间窗口内

    DEV_MODE=True 时始终返回True (24小时运行)
    DEV_MODE=False 时检查是否在 WORK_START_HOUR 至 WORK_END_HOUR 之间
    """
    if DEV_MODE:
        return True
    current_hour = datetime.now().hour
    return WORK_START_HOUR <= current_hour < WORK_END_HOUR


def seconds_until_work_start() -> int:
    """计算距离下一个工作时间开始的秒数"""
    now = datetime.now()
    if now.hour >= WORK_END_HOUR:
        # 今天已过工作结束时间，等到明天开始时间
        next_start = now.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0) + timedelta(days=1)
    elif now.hour < WORK_START_HOUR:
        # 还没到今天的开始时间
        next_start = now.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    else:
        # 已在工作时间内
        return 0
    return int((next_start - now).total_seconds())


def ensure_directories():
    """确保所有必要的目录存在"""
    directories = [DATA_DIR, STATE_DIR, DOWNLOAD_DIR]
    for dir_path in directories:
        try:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print(f"✅ 目录已就绪: {dir_path}")
        except Exception as e:
            print(f"❌ 创建目录失败 {dir_path}: {e}")
            raise


def interruptible_sleep(seconds: int, check_interval: int = 10) -> bool:
    """可中断的睡眠函数

    Args:
        seconds: 总睡眠秒数
        check_interval: 检查间隔秒数

    Returns:
        bool: True=正常完成, False=被中断
    """
    global _daemon_running
    elapsed = 0
    while elapsed < seconds and _daemon_running:
        sleep_time = min(check_interval, seconds - elapsed)
        time.sleep(sleep_time)
        elapsed += sleep_time
    return _daemon_running


# ============================================================================
# 日志上报功能
# ============================================================================
def log_task_result(
    account_id: str,
    shop_id: int,
    table_name: str,
    data_date_start: str,
    data_date_end: str,
    upload_status: int,
    record_count: int = 0,
    error_message: str = "无"
) -> bool:
    """上报任务执行日志到API"""
    headers = {'Content-Type': 'application/json'}
    json_param = {
        "account_id": account_id,
        "shop_id": shop_id,
        "table_name": table_name,
        "data_date_start": data_date_start,
        "data_date_end": data_date_end,
        "upload_status": upload_status,
        "record_count": record_count,
        "error_message": error_message
    }
    proxies = {'http': None, 'https': None}

    print(f"\n{'─' * 50}")
    print(f"📝 日志上报请求:")
    print(f"   URL: {LOG_API_URL}")
    print(f"   请求参数: {json.dumps(json_param, ensure_ascii=False, indent=6)}")

    try:
        response = requests.post(LOG_API_URL, headers=headers, data=json.dumps(json_param), proxies=proxies, timeout=30)
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")
        if response.status_code == 200:
            print(f"   ✅ 日志上报成功")
            return True
        else:
            print(f"   ❌ 日志上报失败")
            return False
    except Exception as e:
        print(f"   ❌ 日志上报异常: {e}")
        return False


def log_success(account_id: str, shop_id: int, table_name: str, data_date_start: str, data_date_end: str, record_count: int) -> bool:
    return log_task_result(account_id, shop_id, table_name, data_date_start, data_date_end, 2, record_count, "无")


def log_failure(account_id: str, shop_id: int, table_name: str, data_date_start: str, data_date_end: str, error_message: str) -> bool:
    return log_task_result(account_id, shop_id, table_name, data_date_start, data_date_end, 1, 0, error_message)


# ============================================================================
# 通用工具函数
# ============================================================================
def disable_proxy():
    """禁用系统代理"""
    for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
        os.environ.pop(key, None)
    os.environ['NO_PROXY'] = '*'
    os.environ['no_proxy'] = '*'
    print("✅ 已禁用系统代理")


def get_session() -> requests.Session:
    """获取禁用代理的session"""
    session = requests.Session()
    session.trust_env = False
    session.proxies = {'http': None, 'https': None}
    return session


@contextmanager
def managed_session():
    """Session上下文管理器，确保Session正确关闭"""
    session = get_session()
    try:
        yield session
    finally:
        session.close()


def safe_json_parse(response: requests.Response, default: Any = None) -> Tuple[Any, Optional[str]]:
    """安全解析JSON响应

    Args:
        response: requests响应对象
        default: 解析失败时的默认返回值

    Returns:
        (解析结果, 错误信息) - 成功时错误信息为None
    """
    try:
        return response.json(), None
    except json.JSONDecodeError as e:
        error_msg = f"JSON解析失败: {e}. 响应内容: {response.text[:200] if response.text else '(空)'}"
        logger.error(error_msg)
        return default, error_msg
    except Exception as e:
        error_msg = f"解析响应时发生未知错误: {e}"
        logger.error(error_msg)
        return default, error_msg


def calculate_retry_delay(attempt: int, initial_delay: float = INITIAL_RETRY_DELAY,
                          max_delay: float = MAX_RETRY_DELAY,
                          backoff_factor: float = RETRY_BACKOFF_FACTOR) -> float:
    """计算指数退避延迟时间（带抖动）

    Args:
        attempt: 当前重试次数（从1开始）
        initial_delay: 初始延迟秒数
        max_delay: 最大延迟秒数
        backoff_factor: 退避因子

    Returns:
        延迟秒数（包含随机抖动）
    """
    delay = initial_delay * (backoff_factor ** (attempt - 1))
    delay = min(delay, max_delay)
    # 添加 ±25% 的随机抖动
    jitter = delay * 0.25 * (2 * random.random() - 1)
    return delay + jitter


def is_retryable_error(error: Exception) -> bool:
    """判断是否为可重试的错误

    Args:
        error: 异常对象

    Returns:
        True表示可以重试，False表示不应重试
    """
    # 连接超时 - 可重试
    if isinstance(error, requests.exceptions.ConnectTimeout):
        return True
    # 读取超时 - 可重试
    if isinstance(error, requests.exceptions.ReadTimeout):
        return True
    # 连接错误（不包含DNS失败）- 可重试
    if isinstance(error, requests.exceptions.ConnectionError):
        error_str = str(error).lower()
        # DNS解析失败不重试
        if 'name or service not known' in error_str or 'getaddrinfo failed' in error_str:
            return False
        return True
    # 服务器错误（5xx）- 通过HTTP状态码判断，这里不处理
    return False


def is_retryable_status_code(status_code: int) -> bool:
    """判断HTTP状态码是否可重试

    Args:
        status_code: HTTP状态码

    Returns:
        True表示可以重试
    """
    # 5xx 服务器错误可重试
    if 500 <= status_code < 600:
        return True
    # 429 Too Many Requests 可重试
    if status_code == 429:
        return True
    return False


def retry_request(request_func, max_attempts: int = MAX_RETRY_ATTEMPTS,
                  description: str = "请求") -> requests.Response:
    """带指数退避的请求重试包装器

    Args:
        request_func: 执行请求的函数，无参数
        max_attempts: 最大尝试次数
        description: 请求描述，用于日志

    Returns:
        响应对象

    Raises:
        最后一次失败的异常
    """
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = request_func()

            # 检查HTTP状态码是否需要重试
            if is_retryable_status_code(response.status_code):
                if attempt < max_attempts:
                    delay = calculate_retry_delay(attempt)
                    logger.warning(f"{description} 返回 {response.status_code}，第 {attempt}/{max_attempts} 次尝试，"
                                   f"{delay:.1f} 秒后重试...")
                    time.sleep(delay)
                    continue

            return response

        except Exception as e:
            last_error = e

            # 判断是否可重试
            if not is_retryable_error(e):
                logger.error(f"{description} 发生不可重试错误: {e}")
                raise

            if attempt < max_attempts:
                delay = calculate_retry_delay(attempt)
                logger.warning(f"{description} 失败: {e}，第 {attempt}/{max_attempts} 次尝试，"
                               f"{delay:.1f} 秒后重试...")
                time.sleep(delay)
            else:
                logger.error(f"{description} 重试 {max_attempts} 次均失败: {e}")
                raise

    raise last_error


def clean_download_directory(directory: str = DOWNLOAD_DIR, max_age_days: int = 7) -> int:
    """清理下载目录中的旧文件

    Args:
        directory: 下载目录路径
        max_age_days: 保留文件的最大天数

    Returns:
        删除的文件数量
    """
    deleted_count = 0
    try:
        dir_path = Path(directory)
        if not dir_path.exists():
            return 0

        cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)

        for file_path in dir_path.glob('*'):
            if file_path.is_file():
                try:
                    if file_path.stat().st_mtime < cutoff_time:
                        file_path.unlink()
                        deleted_count += 1
                        logger.info(f"已删除过期文件: {file_path.name}")
                except Exception as e:
                    logger.warning(f"删除文件失败 {file_path}: {e}")

        if deleted_count > 0:
            logger.info(f"清理完成，共删除 {deleted_count} 个过期文件")
    except Exception as e:
        logger.error(f"清理下载目录失败: {e}")

    return deleted_count


def delete_file_safely(file_path: str) -> bool:
    """安全删除文件

    Args:
        file_path: 文件路径

    Returns:
        是否删除成功
    """
    try:
        path = Path(file_path)
        if path.exists():
            path.unlink()
            logger.debug(f"已删除临时文件: {file_path}")
            return True
    except Exception as e:
        logger.warning(f"删除文件失败 {file_path}: {e}")
    return False


def validate_excel_file(file_path: str) -> Tuple[bool, Optional[str]]:
    """验证Excel文件完整性

    Args:
        file_path: Excel文件路径

    Returns:
        (是否有效, 错误信息)
    """
    try:
        path = Path(file_path)

        # 检查文件是否存在
        if not path.exists():
            return False, "文件不存在"

        # 检查文件大小
        file_size = path.stat().st_size
        if file_size == 0:
            return False, "文件大小为0"

        if file_size < 100:  # Excel文件至少应该有几百字节
            return False, f"文件大小异常: {file_size} bytes"

        # 尝试用pandas打开验证格式
        try:
            df = pd.read_excel(file_path, nrows=1)
            return True, None
        except Exception as e:
            return False, f"Excel格式无效: {e}"

    except Exception as e:
        return False, f"验证文件时发生错误: {e}"


def report_auth_invalid(account_name: str) -> bool:
    """上报账户登录失效状态到API

    当状态文件登录失败且API返回的cookie登录也失败后调用此函数
    """
    print(f"\n{'─' * 50}")
    print(f"🔔 上报账户登录失效状态...")

    headers = {'Content-Type': 'application/json'}
    json_param = {
        "account": account_name,
        "auth_status": "invalid"
    }
    proxies = {'http': None, 'https': None}

    try:
        response = requests.post(
            AUTH_STATUS_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   URL: {AUTH_STATUS_API_URL}")
        print(f"   请求参数: {json.dumps(json_param, ensure_ascii=False)}")
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print(f"   ✅ 账户失效状态上报成功")
            return True
        else:
            print(f"   ❌ 账户失效状态上报失败")
            return False
    except Exception as e:
        print(f"   ❌ 账户失效状态上报异常: {e}")
        return False


def is_auth_invalid_error(code: int = None, message: str = None) -> bool:
    """检测API响应是否为登录失效错误

    判断条件:
    - code == 401
    - code == 606
    - message 包含 "未登录"
    - message 包含 "登录状态失效"
    - message 包含 "请重新登录"

    Args:
        code: API返回的状态码
        message: API返回的消息内容

    Returns:
        bool: True表示登录已失效，需要上报
    """
    # 检查状态码
    if code in [401, 606]:
        return True

    # 检查消息内容
    if message:
        message_str = str(message)
        auth_invalid_keywords = ["未登录", "登录状态失效", "请重新登录"]
        for keyword in auth_invalid_keywords:
            if keyword in message_str:
                return True

    return False


def handle_auth_invalid(account_name: str, start_date: str, end_date: str,
                        task_name: str, error_message: str) -> None:
    """统一处理登录失效错误

    处理流程:
    1. 调用 report_auth_invalid() → /api/post/platform_accounts
    2. 调用 log_failure() → /api/log
    3. 调用 upload_task_status_batch() → /api/account_task/update_batch

    Args:
        account_name: 账户名称
        start_date: 数据开始日期
        end_date: 数据结束日期
        task_name: 当前任务名称
        error_message: 错误信息
    """
    print(f"\n{'=' * 60}")
    print(f"🚨 检测到登录失效，开始上报到三个接口...")
    print(f"   账户: {account_name}")
    print(f"   任务: {task_name}")
    print(f"   错误: {error_message}")
    print(f"{'=' * 60}")

    # 1. 上报账户失效状态到 /api/post/platform_accounts
    report_auth_invalid(account_name)

    # 2. 上报日志到 /api/log
    log_failure(account_name, 0, task_name, start_date, end_date, f"登录失效: {error_message}")

    # 3. 上报任务状态到 /api/account_task/update_batch
    # 将当前任务标记为失败，其他任务标记为未执行
    task_result = {
        'task_name': task_name,
        'success': False,
        'record_count': 0,
        'error_message': f"登录失效: {error_message}"
    }
    upload_task_status_batch(account_name, start_date, end_date, [task_result])

    print(f"\n✅ 登录失效上报完成")


class AuthInvalidError(Exception):
    """登录失效异常

    当检测到登录失效时抛出此异常，用于立即停止重试并终止任务
    """
    pass


def upload_task_status_batch(account_id: str, start_date: str, end_date: str, results: List[Dict[str, Any]]) -> bool:
    """批量上报所有任务状态到API

    Args:
        account_id: 账户ID
        start_date: 数据开始日期
        end_date: 数据结束日期
        results: 任务执行结果列表，每个元素包含 task_name, success, record_count, error_message

    Returns:
        bool: 上报是否成功
    """
    print(f"\n{'─' * 50}")
    print(f"📤 批量上报任务状态...")

    # 全部7个任务的名称列表
    ALL_TASK_NAMES = [
        "store_stats",
        "kewen_daily_report",
        "promotion_daily_report",
        "review_detail_dianping",
        "review_detail_meituan",
        "review_summary_dianping",
        "review_summary_meituan",
    ]

    # 构建API请求参数
    json_param = {
        "account_id": account_id,
        "data_start_date": start_date,
        "data_end_date": end_date,
    }

    # 先用默认值初始化所有7个任务的状态 (0=未执行)
    for task_name in ALL_TASK_NAMES:
        json_param[f"{task_name}_status"] = 0  # 未执行
        json_param[f"{task_name}_records"] = 0
        json_param[f"{task_name}_error"] = None

    # 用实际执行结果覆盖
    for result in results:
        task_name = result.get('task_name')
        if task_name not in ALL_TASK_NAMES:
            print(f"   ⚠️ 未知任务名称: {task_name}，跳过")
            continue

        success = result.get('success', False)
        record_count = result.get('record_count', 0)
        error_message = result.get('error_message', '无')

        # 状态码: success=True -> 2, success=False -> 3
        status = 2 if success else 3
        # 错误信息: success=True -> None, success=False -> 实际错误信息
        error = None if success else error_message

        json_param[f"{task_name}_status"] = status
        json_param[f"{task_name}_records"] = record_count
        json_param[f"{task_name}_error"] = error

    headers = {'Content-Type': 'application/json'}
    proxies = {'http': None, 'https': None}

    print(f"   URL: {TASK_STATUS_BATCH_API_URL}")
    print(f"   请求参数: {json.dumps(json_param, ensure_ascii=False, indent=6)}")

    try:
        response = requests.post(
            TASK_STATUS_BATCH_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print(f"   ✅ 批量任务状态上报成功")
            return True
        else:
            print(f"   ❌ 批量任务状态上报失败")
            return False
    except Exception as e:
        print(f"   ❌ 批量任务状态上报异常: {e}")
        return False


def upload_task_status_single(account_id: str, start_date: str, end_date: str, result: Dict[str, Any]) -> bool:
    """单独上报单个任务状态到API

    Args:
        account_id: 账户ID
        start_date: 数据开始日期
        end_date: 数据结束日期
        result: 单个任务执行结果，包含 task_name, success, record_count, error_message

    Returns:
        bool: 上报是否成功
    """
    print(f"\n{'─' * 50}")
    print(f"📤 单独上报任务状态...")

    task_name = result.get('task_name')
    success = result.get('success', False)
    record_count = result.get('record_count', 0)
    error_message = result.get('error_message', '无')

    # 状态码: success=True -> 2, success=False -> 3
    status = 2 if success else 3
    # 错误信息: success=True -> None, success=False -> 实际错误信息
    error = None if success else error_message

    json_param = {
        "account_id": account_id,
        "data_start_date": start_date,
        "data_end_date": end_date,
        "task_name": task_name,
        "status": status,
        "record_count": record_count,
        "error_message": error
    }

    headers = {'Content-Type': 'application/json'}
    proxies = {'http': None, 'https': None}

    print(f"   URL: {TASK_STATUS_SINGLE_API_URL}")
    print(f"   请求参数: {json.dumps(json_param, ensure_ascii=False, indent=6)}")

    try:
        response = requests.post(
            TASK_STATUS_SINGLE_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print(f"   ✅ 单个任务状态上报成功")
            return True
        else:
            print(f"   ❌ 单个任务状态上报失败")
            return False
    except Exception as e:
        print(f"   ❌ 单个任务状态上报异常: {e}")
        return False


def random_delay(min_seconds: float = 2, max_seconds: float = 5):
    """随机等待指定范围的时间（反爬虫措施）

    Args:
        min_seconds: 最小等待秒数，默认2秒
        max_seconds: 最大等待秒数，默认5秒
    """
    delay = random.uniform(min_seconds, max_seconds)
    print(f"⏳ 反爬虫等待 {delay:.1f} 秒...")
    time.sleep(delay)


# ============================================================================
# ★★★ 门店权限处理函数 ★★★
# ============================================================================
def handle_shop_permission_issue(page) -> bool:
    """处理门店权限问题，切换到全部门店

    当页面出现"该门店无此功能操作权限"时，自动切换到全部门店

    Args:
        page: Playwright page 对象

    Returns:
        是否检测到并处理了权限问题
    """
    try:
        # 检测是否出现"无权限"提示（等待2秒）
        permission_text = page.locator("text=该门店无此功能操作权限")
        if permission_text.is_visible(timeout=2000):
            logger.warning("检测到门店权限问题，尝试切换到全部门店...")

            try:
                # 点击门店选择器展开（通过ID定位）
                shop_selector = page.locator("#shopName")
                if shop_selector.is_visible(timeout=2000):
                    shop_selector.click()
                    time.sleep(1.5)
                    logger.info("已点击门店选择器")

                    # 点击"全部门店"选项
                    all_shops = page.locator("text=全部门店")
                    if all_shops.is_visible(timeout=2000):
                        all_shops.click()
                        time.sleep(2)
                        logger.info("已切换到全部门店")
                        return True
                    else:
                        logger.warning("未找到'全部门店'选项")
                else:
                    logger.warning("未找到门店选择器")
            except Exception as e:
                logger.warning(f"门店切换操作失败: {e}")

            # 即使切换失败也返回True，表示检测到了权限问题
            return True

    except Exception as e:
        # 没有检测到权限问题，这是正常情况
        pass

    return False


# ============================================================================
# ★★★ 通用页面跳转函数 ★★★
# ============================================================================
def navigate_to_url(page, url: str, page_name: str, max_retries: int = 3) -> bool:
    """跳转到指定URL

    Args:
        page: Playwright page 对象
        url: 目标URL
        page_name: 页面名称（用于日志）
        max_retries: 最大重试次数

    Returns:
        是否跳转成功
    """
    logger.info(f"正在跳转到 [{page_name}]...")
    logger.debug(f"URL: {url[:80]}...")

    for attempt in range(1, max_retries + 1):
        try:
            page.goto(url, wait_until='load', timeout=BROWSER_PAGE_TIMEOUT)

            # 额外等待页面稳定
            wait_time = random.uniform(3, 5)
            logger.debug(f"等待页面稳定 {wait_time:.1f} 秒...")
            time.sleep(wait_time)

            # 处理门店权限问题
            handle_shop_permission_issue(page)

            logger.info(f"已跳转到 [{page_name}]")
            return True

        except Exception as e:
            error_str = str(e).lower()
            is_timeout = 'timeout' in error_str

            if is_timeout and attempt < max_retries:
                delay = calculate_retry_delay(attempt)
                logger.warning(f"第 {attempt}/{max_retries} 次跳转超时，{delay:.1f} 秒后重试...")
                time.sleep(delay)
            else:
                logger.error(f"跳转失败: {e}")
                return False

    return False


def load_cookies_from_api(account_name: str) -> Dict[str, Any]:
    """从API加载cookies和相关信息

    使用 /api/get_platform_account 获取账户信息
    """
    print(f"🔍 正在从API获取账户 [{account_name}] 的cookie...")

    session = get_session()
    try:
        response = session.post(
            GET_PLATFORM_ACCOUNT_API_URL,
            headers={'Content-Type': 'application/json'},
            json={"account": account_name},
            timeout=API_TIMEOUT,
            proxies={'http': None, 'https': None}
        )
        response.raise_for_status()

        # 安全解析JSON
        result, json_error = safe_json_parse(response, {})
        if json_error:
            raise Exception(f"API响应解析失败: {json_error}")

        if not result.get('success'):
            raise Exception(f"API返回失败: {result.get('message', '未知错误')}")

        record = result.get('data', {})
        if not record:
            raise Exception(f"未找到账户 [{account_name}] 的cookie数据")

        # 解析cookies（字段名: cookie）
        cookie_data = record.get('cookie')
        if isinstance(cookie_data, str):
            cookies = json.loads(cookie_data)
        else:
            cookies = cookie_data or {}

        # 解析mtgsig
        mtgsig_data = record.get('mtgsig')
        if isinstance(mtgsig_data, str):
            mtgsig = mtgsig_data
        elif isinstance(mtgsig_data, dict):
            mtgsig = json.dumps(mtgsig_data)
        else:
            mtgsig = None

        # 解析shop_info（字段名: stores_json）
        # 使用 or [] 确保 null/None 也能正确转为空列表（dict.get默认值仅在key不存在时生效）
        shop_info = record.get('stores_json') or []

        # 获取templates_id
        templates_id = record.get('templates_id')

        # 获取门店商圈信息（用于同行排名数据）
        compare_regions = record.get('compareRegions_json', {})

        # 获取团购ID映射（用于广告单数据）
        brands_json = record.get('brands_json', [])

        print(f"✅ 成功加载 {len(cookies)} 个cookies")
        if compare_regions:
            print(f"✅ 成功加载 {len(compare_regions)} 个门店商圈信息")
        if brands_json:
            print(f"✅ 成功加载 {len(brands_json)} 个团购ID映射")

        return {
            'cookies': cookies,
            'mtgsig': mtgsig,
            'shop_info': shop_info,
            'templates_id': templates_id,
            'compare_regions': compare_regions,
            'brands_json': brands_json
        }
    finally:
        session.close()


def get_shop_ids(shop_info) -> List[int]:
    """从shop_info提取门店ID列表

    支持两种字段名格式：
    - shop_id: stores_json格式（来自get_platform_account）
    - shopId: 旧格式（兼容）
    """
    shop_ids = []
    if shop_info:
        if isinstance(shop_info, list):
            for shop in shop_info:
                if isinstance(shop, dict):
                    # 优先使用shop_id，兼容shopId
                    shop_id = shop.get('shop_id') or shop.get('shopId')
                    if shop_id:
                        shop_ids.append(int(shop_id))
        elif isinstance(shop_info, dict):
            shop_id = shop_info.get('shop_id') or shop_info.get('shopId')
            if shop_id:
                shop_ids.append(int(shop_id))
    return shop_ids if shop_ids else [0]


def get_platform_account(account: str) -> Dict[str, Any]:
    """获取平台账户信息

    调用 /api/get_platform_account 获取账户的完整信息，包括：
    - cookie: 登录凭证
    - mtgsig: 签名信息
    - templates_id: 报表模板ID
    - stores_json: 门店信息
    - auth_status: 登录状态
    等

    Args:
        account: 账户名称（手机号）

    Returns:
        dict: 包含账户完整信息的字典
            - success: 是否成功
            - data: 账户数据（成功时）
            - error_message: 错误信息（失败时）
    """
    print(f"\n{'─' * 50}")
    print(f"🔍 获取平台账户信息: {account}")

    headers = {'Content-Type': 'application/json'}
    json_param = {"account": account}
    proxies = {'http': None, 'https': None}

    print(f"   URL: {GET_PLATFORM_ACCOUNT_API_URL}")
    print(f"   请求参数: {json.dumps(json_param, ensure_ascii=False)}")

    try:
        response = requests.post(
            GET_PLATFORM_ACCOUNT_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                data = result.get('data', {})
                templates_id = data.get('templates_id')
                auth_status = data.get('auth_status')
                stores_json = data.get('stores_json', [])

                print(f"   ✅ 获取成功")
                print(f"   templates_id: {templates_id}")
                print(f"   auth_status: {auth_status}")
                print(f"   门店数量: {len(stores_json) if stores_json else 0}")

                return {
                    'success': True,
                    'data': data,
                    'templates_id': templates_id,
                    'auth_status': auth_status,
                    'cookie': data.get('cookie'),
                    'mtgsig': data.get('mtgsig'),
                    'stores_json': stores_json
                }
            else:
                error_msg = result.get('message', '获取账户信息失败')
                print(f"   ❌ API返回失败: {error_msg}")
                return {
                    'success': False,
                    'error_message': error_msg
                }
        else:
            error_msg = f"HTTP状态码: {response.status_code}"
            print(f"   ❌ 请求失败: {error_msg}")
            return {
                'success': False,
                'error_message': error_msg
            }
    except Exception as e:
        error_msg = f"请求异常: {str(e)}"
        print(f"   ❌ {error_msg}")
        return {
            'success': False,
            'error_message': error_msg
        }


def generate_mtgsig(cookies: dict, mtgsig_from_api: str = None) -> str:
    """生成mtgsig签名参数

    优先级: API签名 > 本地生成（每次生成新时间戳）
    注意: 不再使用共享签名，避免签名过期导致任务失败
    """
    # 1. 优先使用API返回的签名
    if mtgsig_from_api:
        return mtgsig_from_api

    # 2. 本地生成新签名（每次生成新时间戳，确保签名有效）
    timestamp = int(time.time() * 1000)
    webdfpid = cookies.get('WEBDFPID', '')
    a3 = webdfpid.split('-')[0] if webdfpid and '-' in webdfpid else '5y24v3837yu856y40w99918z268u6v77801vv1w288197958zzvzwy74'

    mtgsig = {
        "a1": "1.2",
        "a2": timestamp,
        "a3": a3,
        "a5": "jBpEMWibZqnOfn+vAsi8yo/kZpK57yUmniEBsbeugiBk2/5nSVi4jUHwsaXt01Ll43X26NE4uABqljWc7M9e8mkBxcu=",
        "a6": "hs1.6kqTyxwalpmvA3xfWt6C4GOVXV8jTW1AytrgLRPiQXPPO3n3UQFIKWTiDGaeXmDJtn4MQEi7f+BMdUtXeeSaMXW9hYSgOd2UuD/+Lac4sqD5ssj0nZesRyvVbOWEeBmBx",
        "a8": "e64733017f50d5892bacd63100c4099c",
        "a9": "4.1.1,7,205",
        "a10": "31",
        "x0": 4,
        "d1": "c9332725bc86a957c5b3185975b58e79"
    }
    return json.dumps(mtgsig)


# ============================================================================
# ★★★ 报表模板ID获取/创建功能 ★★★
# ============================================================================
def get_template_list(cookies: dict, mtgsig: str = None) -> Dict[str, Any]:
    """获取报表模板列表

    Args:
        cookies: cookie字典
        mtgsig: mtgsig签名（可选）

    Returns:
        包含模板列表的字典
    """
    print(f"\n📋 正在获取报表模板列表...")
    print(f"   API地址: {TEMPLATE_LIST_API}")

    session = get_session()

    params = {
        'source': '1',
        'device': 'pc',
        'offset': '0',
        'limit': '19',
        'keyword': '',
        'yodaReady': 'h5',
        'csecplatform': '4',
        'csecversion': '4.1.1',
        'mtgsig': generate_mtgsig(cookies, mtgsig)
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/json',
        'Referer': 'https://e.dianping.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = session.get(
            TEMPLATE_LIST_API,
            params=params,
            headers=headers,
            cookies=cookies,
            timeout=API_TIMEOUT,
            proxies={'http': None, 'https': None}
        )
        response.raise_for_status()
        result = response.json()

        print(f"   API响应状态: {response.status_code}")
        print(f"   API响应码: {result.get('code')}")

        return result

    except requests.exceptions.RequestException as e:
        print(f"   ❌ 获取模板列表失败: {e}")
        return {'code': -1, 'error': str(e)}
    finally:
        session.close()


def find_template_id(cookies: dict, mtgsig: str = None, template_names: List[str] = None) -> Dict[str, Any]:
    """获取报表模板ID

    优先查找顺序:
    1. Kewen_data
    2. hdp-all

    Args:
        cookies: cookie字典
        mtgsig: mtgsig签名（可选）
        template_names: 自定义模板名列表（可选）

    Returns:
        包含template_id和template_name的字典
    """
    # 默认查找的模板名顺序
    default_names = ['Kewen_data', 'hdp-all']

    if template_names:
        search_names = template_names + default_names
    else:
        search_names = default_names

    print(f"\n🔍 开始查找报表模板...")
    print(f"   查找顺序: {' -> '.join(search_names)}")

    # 获取模板列表
    result = get_template_list(cookies, mtgsig)

    if result.get('code') != 200:
        error_msg = f"API返回错误码: {result.get('code')}, 消息: {result.get('msg', '未知')}"
        print(f"   ❌ {error_msg}")
        return {
            'success': False,
            'template_id': None,
            'template_name': None,
            'all_templates': [],
            'error': error_msg
        }

    data = result.get('data', {})
    template_list = data.get('list', [])

    if not template_list:
        print("   ⚠️ 未获取到任何报表模板")
        return {
            'success': False,
            'template_id': None,
            'template_name': None,
            'all_templates': [],
            'error': "未获取到任何报表模板"
        }

    # 打印所有可用模板
    print(f"\n📊 可用的报表模板列表 (共 {len(template_list)} 个):")
    print("   " + "-" * 50)
    for idx, template in enumerate(template_list, 1):
        print(f"   {idx:2}. ID: {template.get('id'):15} | 名称: {template.get('name')}")
    print("   " + "-" * 50)

    # 按优先级查找模板
    for search_name in search_names:
        for template in template_list:
            if template.get('name') == search_name:
                template_id = template.get('id')
                template_name = template.get('name')
                print(f"\n✅ 找到目标模板!")
                print(f"   模板名称: {template_name}")
                print(f"   模板ID: {template_id}")
                return {
                    'success': True,
                    'template_id': template_id,
                    'template_name': template_name,
                    'all_templates': template_list
                }

    # 未找到目标模板
    print(f"\n⚠️ 未找到目标模板: {search_names}")
    return {
        'success': False,
        'template_id': None,
        'template_name': None,
        'all_templates': template_list,
        'error': f"未找到以下模板: {', '.join(search_names)}"
    }


def create_report_template(cookies: dict, mtgsig: str, template_name: str = "Kewen_data") -> Dict[str, Any]:
    """创建报表模板

    Args:
        cookies: cookie字典
        mtgsig: mtgsig签名
        template_name: 模板名称，默认为 "Kewen_data"

    Returns:
        包含创建结果的字典
    """
    print(f"\n📤 正在创建报表模板: {template_name}")
    print(f"   API地址: {TEMPLATE_SAVE_API}")

    if not mtgsig:
        print("   ❌ 创建模板需要mtgsig签名")
        return {
            'success': False,
            'template_id': None,
            'template_name': template_name,
            'error': "创建模板需要mtgsig签名"
        }

    session = get_session()

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://h5.dianping.com',
        'Referer': 'https://h5.dianping.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    }

    params = {
        'yodaReady': 'h5',
        'csecplatform': '4',
        'csecversion': '4.1.1',
        'mtgsig': mtgsig
    }

    post_data = {
        'source': '1',
        'device': 'pc',
        'name': template_name,
        'platform': '0',
        'relationObjectIds': '0',
        'compareTypes': '',
        'metricList': TEMPLATE_METRIC_LIST,
        'dateType': 'day',
        'dateSubType': '',
        'statsType': 'shop',
        'summaryType': 'shop'
    }

    try:
        response = session.post(
            TEMPLATE_SAVE_API,
            params=params,
            headers=headers,
            cookies=cookies,
            data=post_data,
            timeout=API_TIMEOUT,
            proxies={'http': None, 'https': None}
        )

        print(f"   HTTP状态码: {response.status_code}")
        resp_json = response.json()
        print(f"   响应内容: {json.dumps(resp_json, ensure_ascii=False)}")

        if resp_json.get('code') == 200:
            template_id = resp_json.get('data')
            print(f"\n✅ 报表模板创建成功!")
            print(f"   模板ID: {template_id}")
            return {
                'success': True,
                'template_id': template_id,
                'template_name': template_name
            }
        else:
            error_msg = resp_json.get('msg', '未知错误')
            print(f"\n❌ 创建失败: {error_msg}")
            return {
                'success': False,
                'template_id': None,
                'template_name': template_name,
                'error': error_msg
            }

    except requests.exceptions.RequestException as e:
        print(f"\n❌ 创建报表模板请求失败: {e}")
        return {
            'success': False,
            'template_id': None,
            'template_name': template_name,
            'error': str(e)
        }
    finally:
        session.close()


def update_template_id_to_backend(account_name: str, templates_id: int) -> bool:
    """将templates_id回写到后端数据库

    同时调用两个API：
    1. /api/platform-accounts - 原有API
    2. /api/cookie_config - 新增API

    Args:
        account_name: 账户名称
        templates_id: 模板ID

    Returns:
        是否更新成功（两个API都成功才返回True）
    """
    print(f"\n📤 正在回写 templates_id 到后端...")
    print(f"   账户: {account_name}")
    print(f"   templates_id: {templates_id}")

    session = get_session()
    success_count = 0

    try:
        # ========== 调用API 1: /api/platform-accounts ==========
        print(f"\n   [1/2] 调用 {PLATFORM_ACCOUNTS_UPDATE_API}")
        try:
            response1 = session.post(
                PLATFORM_ACCOUNTS_UPDATE_API,
                headers={'Content-Type': 'application/json'},
                json={"account": account_name, "templates_id": templates_id},
                timeout=API_TIMEOUT,
                proxies={'http': None, 'https': None}
            )

            print(f"      HTTP状态码: {response1.status_code}")
            result1 = response1.json()
            print(f"      响应内容: {json.dumps(result1, ensure_ascii=False)}")

            if result1.get('success'):
                affected_rows = result1.get('data', {}).get('affectedRows', 0)
                print(f"      ✅ 成功! 影响行数: {affected_rows}")
                success_count += 1
            else:
                print(f"      ❌ 失败: {result1.get('msg', '未知错误')}")
        except requests.exceptions.RequestException as e:
            print(f"      ❌ 请求失败: {e}")

        # ========== 调用API 2: /api/up/templates_id ==========
        print(f"\n   [2/2] 调用 {TEMPLATES_ID_UPDATE_API_URL}")
        try:
            response2 = session.post(
                TEMPLATES_ID_UPDATE_API_URL,
                headers={'Content-Type': 'application/json'},
                json={"name": account_name, "templates_id": templates_id},
                timeout=API_TIMEOUT,
                proxies={'http': None, 'https': None}
            )

            print(f"      HTTP状态码: {response2.status_code}")
            result2 = response2.json()
            print(f"      响应内容: {json.dumps(result2, ensure_ascii=False)}")

            if result2.get('success') or response2.status_code == 200:
                print(f"      ✅ 成功!")
                success_count += 1
            else:
                print(f"      ❌ 失败: {result2.get('msg', '未知错误')}")
        except requests.exceptions.RequestException as e:
            print(f"      ❌ 请求失败: {e}")

        # 汇总结果
        if success_count == 2:
            print(f"\n✅ 回写完成! 2个API均成功")
            return True
        elif success_count == 1:
            print(f"\n⚠️ 回写部分成功 (1/2)")
            return True  # 至少有一个成功也返回True
        else:
            print(f"\n❌ 回写失败! 2个API均失败")
            return False

    except Exception as e:
        print(f"❌ 回写过程异常: {e}")
        return False
    finally:
        session.close()


def ensure_template_id(account_name: str, cookies: dict, mtgsig: str) -> Optional[int]:
    """确保获取到 templates_id

    逻辑:
    1. 先尝试从模板列表中查找 "Kewen_data" 或 "hdp-all"
    2. 如果找不到，则创建名为 "Kewen_data" 的模板
    3. 获取到 templates_id 后回写到后端

    Args:
        account_name: 账户名称
        cookies: cookie字典
        mtgsig: mtgsig签名

    Returns:
        templates_id (int) 或 None
    """
    print("\n" + "=" * 60)
    print("🔧 开始获取/创建报表模板ID")
    print("=" * 60)

    # 步骤1: 尝试查找已有模板
    find_result = find_template_id(cookies, mtgsig)

    if find_result['success']:
        template_id = find_result['template_id']
        print(f"\n✅ 已找到现有模板，ID: {template_id}")

        # 回写到后端
        update_template_id_to_backend(account_name, template_id)

        return template_id

    # 步骤2: 未找到模板，需要创建
    print("\n⚠️ 未找到目标模板，开始创建新模板...")
    random_delay(1, 2)

    create_result = create_report_template(cookies, mtgsig, "Kewen_data")

    if create_result['success']:
        template_id = create_result['template_id']
        print(f"\n✅ 新模板创建成功，ID: {template_id}")

        # 回写到后端
        update_template_id_to_backend(account_name, template_id)

        return template_id

    # 创建也失败了
    print("\n❌ 无法获取或创建报表模板ID")
    return None


def ensure_template_id_with_browser(account_name: str, cookies: dict,
                                     mtgsig: str, headless: bool = True,
                                     browser_pool: 'BrowserPoolManager' = None) -> Optional[int]:
    """使用浏览器获取/创建报表模板ID

    支持两种模式：
    1. 浏览器池模式：传入 browser_pool，复用池中的 Context（避免 Playwright 实例冲突）
    2. 单任务模式：不传 browser_pool，创建独立的 Playwright 实例

    流程：
    1. 获取/创建浏览器页面
    2. 添加 cookies（单任务模式）
    3. 检查登录状态
    4. 登录有效 → 跳转到报表中心页面
    5. 调用 ensure_template_id() 获取/创建
    6. 关闭浏览器（单任务模式）
    7. 返回 templates_id

    Args:
        account_name: 账户名称
        cookies: cookie字典
        mtgsig: mtgsig签名
        headless: 是否使用无头模式
        browser_pool: 浏览器池管理器（可选）

    Returns:
        templates_id (int) 或 None
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("❌ Playwright未安装，无法使用浏览器模式获取模板ID")
        return None

    print("\n" + "=" * 60)
    print("🌐 启动浏览器获取/创建报表模板ID")
    print("=" * 60)

    # 浏览器池模式
    if browser_pool:
        print("   使用浏览器池模式")
        try:
            # 从浏览器池获取 Context（带自动重连）
            wrapper = browser_pool.get_context(account_name, cookies)
            if not wrapper:
                print("❌ 无法从浏览器池获取 Context")
                log_failure(account_name, 0, "ensure_template_id", "", "",
                           "无法从浏览器池获取 Context")
                return None

            # 验证 wrapper 有效性
            if not wrapper.is_valid():
                print("❌ 获取的 Context 无效")
                log_failure(account_name, 0, "ensure_template_id", "", "",
                           "获取的 Context 无效")
                # 移除无效的 context
                browser_pool.remove_context(account_name)
                return None

            page = wrapper.page

            # ========== 检查登录状态 ==========
            print(f"\n🔐 检查登录状态...")
            login_check_url = "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html"

            # 使用安全的页面跳转
            if not wrapper.safe_goto(login_check_url, wait_until='domcontentloaded',
                                      timeout=30000, max_retries=2):
                print(f"   ❌ 登录检测页面跳转失败")
                log_failure(account_name, 0, "ensure_template_id", "", "",
                           "登录检测页面跳转失败，浏览器可能已关闭")
                # 移除失效的 context
                browser_pool.remove_context(account_name)
                return None

            time.sleep(2)

            # 检查页面是否仍然有效
            try:
                current_url = page.url
            except Exception as e:
                print(f"   ❌ 获取页面URL失败: {e}")
                log_failure(account_name, 0, "ensure_template_id", "", "",
                           f"获取页面URL失败: {e}")
                browser_pool.remove_context(account_name)
                return None

            if 'login' in current_url.lower():
                print(f"   ❌ 检测到登录失效（重定向到登录页）")
                print(f"   当前URL: {current_url}")
                report_auth_invalid(account_name)
                return None

            try:
                has_content = page.evaluate("() => document.body.textContent.length > 100")
                if not has_content:
                    print(f"   ❌ 检测到登录失效（页面内容为空）")
                    report_auth_invalid(account_name)
                    return None
            except Exception as e:
                print(f"   ⚠️ 页面内容检测失败: {e}")

            print(f"   ✅ 登录状态有效")

            # ========== 跳转到报表中心页面 ==========
            print(f"\n📍 跳转到报表中心页面...")
            print(f"   URL: {REPORT_CENTER_URL[:80]}...")

            # 使用安全的页面跳转
            if not wrapper.safe_goto(REPORT_CENTER_URL, wait_until='networkidle',
                                      timeout=BROWSER_PAGE_TIMEOUT, max_retries=2):
                print(f"   ❌ 报表中心页面跳转失败")
                log_failure(account_name, 0, "ensure_template_id", "", "",
                           "报表中心页面跳转失败")
                return None

            random_delay(2, 3)
            print("   ✅ 页面加载完成")

            # 调用 ensure_template_id 获取或创建模板ID
            templates_id = ensure_template_id(account_name, cookies, mtgsig)
            return templates_id

        except Exception as e:
            error_msg = str(e)
            print(f"❌ 浏览器池模式获取模板ID失败: {e}")

            # 检测是否是浏览器关闭错误
            if 'Target' in error_msg and 'closed' in error_msg.lower():
                print("   检测到浏览器已关闭，移除失效 Context")
                try:
                    browser_pool.remove_context(account_name)
                except Exception:
                    pass

            # 上报错误
            log_failure(account_name, 0, "ensure_template_id", "", "",
                       f"浏览器池模式失败: {error_msg}")

            import traceback
            traceback.print_exc()
            return None

    # 单任务模式（创建独立 Playwright 实例）
    print("   使用单任务模式")
    playwright = None
    browser = None
    context = None

    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=headless, proxy=None)

        # 转换cookies为Playwright格式
        playwright_cookies = []
        for name, value in cookies.items():
            playwright_cookies.append({
                'name': name,
                'value': str(value),
                'domain': '.dianping.com',
                'path': '/'
            })

        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            proxy=None,
            bypass_csp=True,
            ignore_https_errors=True
        )
        context.add_cookies(playwright_cookies)
        page = context.new_page()

        # ========== 先检查登录状态 ==========
        print(f"\n🔐 检查登录状态...")
        login_check_url = "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html"
        try:
            page.goto(login_check_url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)

            current_url = page.url
            # 检查是否被重定向到登录页
            if 'login' in current_url.lower():
                print(f"   ❌ 检测到登录失效（重定向到登录页）")
                print(f"   当前URL: {current_url}")
                # 上报登录失效状态
                report_auth_invalid(account_name)
                return None

            # 检查页面是否有内容（防止空页面）
            has_content = page.evaluate("() => document.body.textContent.length > 100")
            if not has_content:
                print(f"   ❌ 检测到登录失效（页面内容为空）")
                # 上报登录失效状态
                report_auth_invalid(account_name)
                return None

            print(f"   ✅ 登录状态有效")

        except Exception as e:
            error_msg = str(e).lower()
            if 'timeout' in error_msg:
                print(f"   ⚠️ 登录检测超时，继续尝试...")
            else:
                print(f"   ❌ 登录检测失败: {e}")
                # 上报登录失效状态
                report_auth_invalid(account_name)
                return None

        # ========== 登录有效，跳转到报表中心页面 ==========
        print(f"\n📍 跳转到报表中心页面...")
        print(f"   URL: {REPORT_CENTER_URL[:80]}...")
        page.goto(REPORT_CENTER_URL, wait_until='networkidle', timeout=BROWSER_PAGE_TIMEOUT)
        random_delay(2, 3)
        print("   ✅ 页面加载完成")

        # 调用 ensure_template_id 获取或创建模板ID
        templates_id = ensure_template_id(account_name, cookies, mtgsig)

        return templates_id

    except Exception as e:
        print(f"❌ 浏览器获取模板ID失败: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        # 关闭浏览器
        try:
            if context:
                context.close()
            if browser:
                browser.close()
            if playwright:
                playwright.stop()
            print("✓ 浏览器已关闭")
        except Exception as e:
            print(f"⚠️ 关闭浏览器时出错: {e}")


# ============================================================================
# kewen_daily_report 任务
# ============================================================================
KEWEN_COLUMN_MAPPING = {
    0: ("report_date", "string"), 1: ("province", "string"), 2: ("city", "string"),
    3: ("shop_id", "number"), 4: ("shop_name", "string"), 5: ("dianping_star", "number"),
    6: ("meituan_star", "number"), 7: ("operation_score", "number"), 8: ("operation_level", "string"),
    9: ("promotion_cost", "number"), 10: ("merchant_cost", "number"), 11: ("platform_service_fee", "number"),
    12: ("commission_gtv", "number"), 13: ("exposure_users", "number"), 14: ("exposure_count", "number"),
    15: ("visit_users", "number"), 16: ("visit_count", "number"), 17: ("exposure_visit_rate", "string"),
    18: ("order_users", "number"), 19: ("lead_users", "number"), 20: ("intent_users", "number"),
    21: ("intent_rate", "string"), 22: ("new_collect_users", "number"), 23: ("total_collect_users", "number"),
    24: ("avg_stay_seconds", "number"), 25: ("promotion_exposure_count", "number"), 26: ("promotion_click_count", "number"),
    27: ("verify_sale_amount", "number"), 28: ("verify_after_discount", "number"), 29: ("verify_coupon_count", "number"),
    30: ("verify_order_count", "number"), 31: ("verify_person_count", "number"), 32: ("verify_new_customer", "number"),
    33: ("order_coupon_count", "number"), 34: ("order_sale_amount", "number"), 35: ("consult_users", "number"),
    36: ("consult_lead_count", "number"), 37: ("consult_lead_rate", "string"), 38: ("avg_response_seconds", "number"),
    39: ("reply_rate_30s", "string"), 40: ("reply_rate_5min", "string"), 41: ("refund_amount", "number"),
    42: ("refund_order_count", "number"), 43: ("refund_users", "number"), 44: ("complaint_count", "number"),
    45: ("compensation_order_count", "number"), 46: ("new_review_count", "number"), 47: ("new_good_review_count", "number"),
    48: ("new_medium_review_count", "number"), 49: ("new_bad_review_count", "number"), 50: ("bad_review_reply_rate", "string"),
    51: ("total_review_count", "number"), 52: ("total_bad_review_count", "number"),
    # ============ 新增字段: 门店优惠码 (BB-BH列) ============
    53: ("coupon_code_type", "string"),  # 码类型
    54: ("coupon_pay_order_count", "number"),  # 支付订单数(个)
    55: ("coupon_pay_amount", "number"),  # 支付金额(元)
    56: ("coupon_verify_amount", "number"),  # 核销金额(元)
    57: ("coupon_scan_users", "number"),  # 扫码人数(人)
    58: ("coupon_scan_collect_count", "number"),  # 扫码收藏数(个)
    59: ("coupon_scan_review_count", "number"),  # 扫码评价数(个)
}

KEWEN_STRING_DEFAULTS = {
    "operation_level": "暂无", "exposure_visit_rate": "0%", "intent_rate": "0%",
    "consult_lead_rate": "0%", "reply_rate_30s": "0%", "reply_rate_5min": "0%", "bad_review_reply_rate": "0%",
    "coupon_code_type": "",  # 门店优惠码类型默认值
}


def kewen_convert_value(value, data_type, field_name):
    """转换值为指定类型"""
    if value is None or (isinstance(value, float) and math.isnan(value)) or (isinstance(value, str) and value.strip() == ''):
        if data_type == "number":
            return 0
        elif data_type == "string":
            return KEWEN_STRING_DEFAULTS.get(field_name, "")
        return None

    if data_type == "string":
        if hasattr(value, 'strftime'):
            return value.strftime("%Y-%m-%d")
        return str(value).strip()
    elif data_type == "number":
        try:
            if isinstance(value, str):
                value = value.replace(',', '')
            return float(value)
        except:
            return 0
    return value


def kewen_parse_excel_row(row):
    """解析Excel行数据"""
    data = {}
    for col_idx, (field_name, data_type) in KEWEN_COLUMN_MAPPING.items():
        if col_idx < len(row):
            value = row.iloc[col_idx]
            converted = kewen_convert_value(value, data_type, field_name)
            if converted is not None:
                data[field_name] = converted
    return data


def kewen_is_empty_row(data):
    """检查是否为空行"""
    return not data.get('report_date') or data.get('shop_id', 0) == 0 or not data.get('shop_name')


def kewen_is_valid_coupon_type(data):
    """
    检查是否为有效的优惠码类型（只保留"全部码"）

    Args:
        data: 解析后的行数据

    Returns:
        True: 是"全部码"，应该保留
        False: 是其他类型（门店码/商品码/职人码/品牌码），应该跳过
    """
    coupon_code_type = data.get('coupon_code_type', '')
    return coupon_code_type == '全部码'


def run_kewen_daily_report(account_name: str, start_date: str, end_date: str, templates_id: Optional[int] = None,
                           cookies: Dict = None, mtgsig: str = None) -> Dict[str, Any]:
    """执行kewen_daily_report任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        templates_id: 报表模板ID（可选，如果传入则优先使用，不再从API重新获取）
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
    """
    table_name = "kewen_daily_report"
    print(f"\n{'=' * 60}")
    print(f"📊 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}
    session = None
    save_path = None  # 用于跟踪下载的临时文件

    try:
        disable_proxy()
        Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            shop_info = []  # kewen_daily_report 不需要 shop_info
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
            if not templates_id:
                templates_id = api_data['templates_id']

        # 如果外部传入了 templates_id，优先使用外部传入的值
        if templates_id:
            print(f"📌 使用 templates_id: {templates_id}")

        if not templates_id:
            raise Exception("未获取到报表模板ID")

        shop_ids = get_shop_ids(shop_info)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Referer': 'https://e.dianping.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        session = get_session()

        # 报表下载重试机制（使用指数退避）
        file_record = None
        last_error_message = ""

        for retry_attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            print(f"\n🔍 正在请求生成报表... (第 {retry_attempt}/{MAX_RETRY_ATTEMPTS} 次尝试)")
            url = "https://e.dianping.com/gateway/adviser/report/template/download"
            params = {
                'source': '1', 'device': 'pc', 'id': templates_id,
                'date': f"{start_date},{end_date}",
                'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1',
                'mtgsig': generate_mtgsig(cookies, mtgsig)
            }
            response = session.get(url, params=params, headers=headers, cookies=cookies, timeout=API_TIMEOUT)

            # 安全解析JSON响应
            resp_json, json_error = safe_json_parse(response, {})
            if json_error:
                raise Exception(f"API响应解析失败: {json_error}")
            print(f"📊 请求响应: {resp_json}")

            # 检查是否登录失效（code 606 或 message 包含登录失效关键词）
            resp_code = resp_json.get('code')
            resp_msg = resp_json.get('msg') or resp_json.get('message') or ''
            if is_auth_invalid_error(resp_code, resp_msg):
                error_msg = f"登录失效 (code={resp_code}, msg={resp_msg})"
                print(f"🚨 检测到登录失效: {error_msg}")
                handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                raise AuthInvalidError(error_msg)

            # 检查请求是否成功
            result_type = resp_json.get('data', {}).get('resultType')
            result_msg = resp_json.get('data', {}).get('resultMsg', '')
            if result_type == 3:
                # 服务异常，需要重试（使用指数退避）
                # 上传完整的美团返回的 resultMsg，而不是简化的错误信息
                if result_msg:
                    last_error_message = f"服务异常 (resultType={result_type}): {result_msg}"
                else:
                    last_error_message = f"服务异常 (resultType={result_type})"
                print(f"⚠️ 第 {retry_attempt} 次尝试失败: {last_error_message}")
                if retry_attempt < MAX_RETRY_ATTEMPTS:
                    delay = calculate_retry_delay(retry_attempt)
                    print(f"   等待 {delay:.1f} 秒后重试...")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(f"报表下载重试 {MAX_RETRY_ATTEMPTS} 次均失败: {last_error_message}")

            random_delay()  # 反爬虫等待

            # 等待报表生成
            print(f"\n⏳ 等待报表生成...")
            date_keyword = f"{start_date.replace('-', '')}-{end_date.replace('-', '')}"

            for _ in range(60):
                time.sleep(2)
                list_url = "https://e.dianping.com/gateway/merchant/downloadcenter/list"
                list_params = {'pageNo': 1, 'pageSize': 20, 'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': generate_mtgsig(cookies, mtgsig)}
                list_resp = session.get(list_url, params=list_params, headers=headers, cookies=cookies, timeout=API_TIMEOUT)

                # 安全解析JSON响应
                list_data, json_error = safe_json_parse(list_resp, {})
                if json_error:
                    logger.warning(f"下载列表解析失败: {json_error}")
                    continue

                # 检查是否登录失效
                list_code = list_data.get('code')
                list_msg = list_data.get('msg') or list_data.get('message') or ''
                if is_auth_invalid_error(list_code, list_msg):
                    error_msg = f"登录失效 (code={list_code}, msg={list_msg})"
                    print(f"🚨 检测到登录失效: {error_msg}")
                    handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                    raise AuthInvalidError(error_msg)

                if list_data.get('code') == 200:
                    for record in list_data.get('data', {}).get('records', []):
                        if record.get('recordStatus') == 300 and record.get('downloadable') == "1" and record.get('fileUrl'):
                            if date_keyword in record.get('fileName', ''):
                                file_record = record
                                print(f"   ✅ 文件已就绪: {record.get('fileName')}")
                                break
                if file_record:
                    break

            # 检查文件是否成功生成
            if file_record:
                print(f"✅ 第 {retry_attempt} 次尝试成功，文件已就绪")
                break  # 成功获取文件，跳出重试循环
            else:
                # 文件未生成，需要重试（使用指数退避）
                last_error_message = "报表生成超时，文件未就绪"
                print(f"⚠️ 第 {retry_attempt} 次尝试失败: {last_error_message}")
                if retry_attempt < MAX_RETRY_ATTEMPTS:
                    delay = calculate_retry_delay(retry_attempt)
                    print(f"   等待 {delay:.1f} 秒后重试...")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(f"报表下载重试 {MAX_RETRY_ATTEMPTS} 次均失败: {last_error_message}")

        random_delay()  # 反爬虫等待

        # 下载文件
        file_url = file_record['fileUrl']
        file_name = file_record.get('fileName', f'report_{templates_id}.xlsx')
        save_path = str(Path(SAVE_DIR) / file_name)

        print(f"📥 正在下载文件...")
        dl_resp = session.get(file_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        try:
            with open(save_path, 'wb') as f:
                for chunk in dl_resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        finally:
            dl_resp.close()  # 确保关闭流式响应
        print(f"✅ 文件已保存到: {save_path}")

        # 验证文件完整性
        is_valid, validation_error = validate_excel_file(save_path)
        if not is_valid:
            raise Exception(f"下载的文件无效: {validation_error}")

        # 解析Excel
        print(f"\n📄 开始解析Excel文件")
        df = pd.read_excel(save_path, header=None)
        print(f"✅ 读取成功，共 {len(df)} 行，{len(df.columns)} 列")
        data_list = []
        skip_count = 0
        coupon_type_skip_count = 0
        for idx in range(2, len(df)):
            row = df.iloc[idx]
            data = kewen_parse_excel_row(row)
            # 检查是否为空行
            if kewen_is_empty_row(data):
                skip_count += 1
                continue
            # 检查优惠码类型，只保留"全部码"
            if not kewen_is_valid_coupon_type(data):
                coupon_type_skip_count += 1
                continue
            data_list.append(data)
        print(f"✅ 解析完成:")
        print(f"   有效数据: {len(data_list)} 条 (全部码)")
        print(f"   跳过空行: {skip_count} 条")
        print(f"   跳过其他码类型: {coupon_type_skip_count} 条 (门店码/商品码/职人码/品牌码)")

        # 上传数据
        print(f"\n📤 开始上传数据到: {UPLOAD_APIS[table_name]}")
        success_count = 0
        fail_count = 0
        shop_record_counts = {}

        for idx, data in enumerate(data_list, 1):
            try:
                print(f"\n   [{idx}/{len(data_list)}] 上传数据:")
                print(f"      shop_id={data.get('shop_id')}, report_date={data.get('report_date')}, shop_name={data.get('shop_name')}")
                resp = session.post(UPLOAD_APIS[table_name], json=data, headers={'Content-Type': 'application/json'}, timeout=30)
                print(f"      HTTP状态码: {resp.status_code}")
                print(f"      响应: {resp.text[:200] if resp.text else '(空)'}")
                if resp.status_code in [200, 201]:
                    success_count += 1
                    shop_id = int(data.get('shop_id', 0))
                    shop_record_counts[shop_id] = shop_record_counts.get(shop_id, 0) + 1
                    print(f"      ✅ 成功")
                else:
                    fail_count += 1
                    print(f"      ❌ 失败")
            except Exception as e:
                fail_count += 1
                print(f"      ❌ 异常: {e}")

        print(f"\n✅ 上传完成: 成功 {success_count}, 失败 {fail_count}")

        if fail_count == 0:
            result["success"] = True
            result["record_count"] = success_count
            for shop_id, count in shop_record_counts.items():
                log_success(account_name, shop_id, table_name, start_date, end_date, count)
            # 任务成功后删除临时文件
            if save_path:
                delete_file_safely(save_path)
        else:
            result["error_message"] = f"部分上传失败: 成功{success_count}, 失败{fail_count}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    finally:
        # 确保关闭Session
        if session:
            session.close()

    return result


# ============================================================================
# promotion_daily_report 任务
# ============================================================================
def run_promotion_daily_report(account_name: str, start_date: str, end_date: str,
                               cookies: Dict = None, mtgsig: str = None) -> Dict[str, Any]:
    """执行promotion_daily_report任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
    """
    table_name = "promotion_daily_report"
    print(f"\n{'=' * 60}")
    print(f"📊 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}
    session = None
    save_path = None  # 用于跟踪下载的临时文件

    try:
        disable_proxy()
        Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            shop_info = []  # promotion_daily_report 不需要 shop_info
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
        shop_ids = get_shop_ids(shop_info)
        year = start_date.split('-')[0]

        headers = {
            'Accept': '*/*',
            'Referer': 'https://e.dianping.com/shopdiy-node/report',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        session = get_session()

        # 请求下载报表
        print(f"\n🔍 正在请求生成门店数据报表...")
        url = "https://e.dianping.com/shopdiy/report/datareport/pc/ajax/downloadReport"

        begin_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days_diff = (end_dt - begin_dt).days
        compare_end_dt = begin_dt - timedelta(days=1)
        compare_begin_dt = compare_end_dt - timedelta(days=days_diff)

        params = {
            'shopIds': '0', 'launchIds': '0', 'launchPremiumIds': '0', 'planIds': '0',
            'objectUnit': '', 'groupUnit': 'shopId', 'platform': '0',
            'beginDate': start_date, 'endDate': end_date, 'timeUnit': 'day', 'compareEnabled': '0',
            'compareBeginDate': compare_begin_dt.strftime('%Y-%m-%d'),
            'compareEndDate': compare_end_dt.strftime('%Y-%m-%d'),
            'tabIds': 'T30001,T30002,T30003,T30004,T30005,T30048,T30020,T30029,T30006,T30007,T30013,T30014,T30009,T30012,T30011',
            'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.0.4',
            'mtgsig': generate_mtgsig(cookies, mtgsig)
        }

        response = session.get(url, params=params, headers=headers, cookies=cookies, timeout=API_TIMEOUT)
        resp_json, json_error = safe_json_parse(response, {})
        if json_error:
            raise Exception(f"API响应解析失败: {json_error}")
        print(f"📊 请求响应: {resp_json}")

        # 检查是否登录失效（code 401 或 message 包含登录失效关键词）
        resp_code = resp_json.get('code')
        resp_msg = resp_json.get('msg') or resp_json.get('message') or ''
        if is_auth_invalid_error(resp_code, resp_msg):
            error_msg = f"登录失效 (code={resp_code}, msg={resp_msg})"
            print(f"🚨 检测到登录失效: {error_msg}")
            handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
            raise AuthInvalidError(error_msg)

        random_delay()  # 反爬虫等待

        # 检查是否直接返回URL
        file_url = None
        if resp_json.get('code') == 200:
            msg = resp_json.get('msg', {})
            if isinstance(msg, dict) and 'S3Url' in msg:
                s3_url = msg.get('S3Url')
                if isinstance(s3_url, list) and s3_url:
                    file_url = s3_url[0]
                elif isinstance(s3_url, str):
                    file_url = s3_url

        # 如果没有直接返回URL，等待下载历史
        if not file_url:
            print(f"\n⏳ 等待报表生成...")
            history_url = "https://e.dianping.com/shopdiy/report/datareport/subAccount/common/queryDownloadHistory"

            for _ in range(60):
                time.sleep(5)
                hist_params = {'types': '3,9,10', 'beginDate': '', 'endDate': '', 'pageNum': 1, 'pageSize': 20,
                               'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.0.4', 'mtgsig': generate_mtgsig(cookies, mtgsig)}
                hist_resp = session.get(history_url, params=hist_params, headers=headers, cookies=cookies, timeout=API_TIMEOUT)
                hist_data, json_error = safe_json_parse(hist_resp, {})
                if json_error:
                    logger.warning(f"下载历史解析失败: {json_error}")
                    continue

                # 检查是否登录失效
                hist_code = hist_data.get('code')
                hist_msg = hist_data.get('msg') or hist_data.get('message') or ''
                if is_auth_invalid_error(hist_code, hist_msg):
                    error_msg = f"登录失效 (code={hist_code}, msg={hist_msg})"
                    print(f"🚨 检测到登录失效: {error_msg}")
                    handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                    raise AuthInvalidError(error_msg)

                for record in hist_data.get('records', []):
                    if record.get('status') == 2:
                        file_path = record.get('filePath', '')
                        if isinstance(file_path, list) and file_path:
                            file_url = file_path[0]
                        elif isinstance(file_path, str):
                            file_url = file_path
                        if file_url:
                            print(f"   ✅ 报表已就绪")
                            break
                if file_url:
                    break

        if not file_url:
            raise Exception("报表生成超时")

        random_delay()  # 反爬虫等待

        # 下载文件
        file_name = f'门店报表_{start_date.replace("-", "")}_{end_date.replace("-", "")}.xlsx'
        save_path = str(Path(SAVE_DIR) / file_name)

        print(f"📥 正在下载文件...")
        dl_resp = session.get(file_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        try:
            with open(save_path, 'wb') as f:
                for chunk in dl_resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        finally:
            dl_resp.close()  # 确保关闭流式响应
        print(f"✅ 文件已保存到: {save_path}")

        # 验证文件完整性
        is_valid, validation_error = validate_excel_file(save_path)
        if not is_valid:
            raise Exception(f"下载的文件无效: {validation_error}")

        # 上传数据
        print(f"\n📤 开始上传报表数据到: {UPLOAD_APIS[table_name]}")
        df = pd.read_excel(save_path)
        print(f"   Excel行数: {len(df)}")
        success_count = 0
        fail_count = 0
        shop_ids_uploaded = set()

        def parse_value(val, default=0):
            if pd.isna(val) or val == '/' or val == '-' or val == '':
                return default
            try:
                return float(val) if '.' in str(val) else int(val)
            except:
                return default

        def format_date(date_str):
            if '-' in str(date_str):
                parts = str(date_str).split('-')
                if len(parts) == 2:
                    return f"{year}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            return str(date_str)

        for idx, row in df.iterrows():
            try:
                json_param = {
                    "report_date": format_date(row['日期']),
                    "shop_id": int(row['门店ID']),
                    "shop_name": str(row['推广门店']),
                    "city_name": str(row['门店所在城市']),
                    "cost": parse_value(row['花费（元）'], 0.0),
                    "exposure_count": parse_value(row['曝光（次）']),
                    "click_count": parse_value(row['点击（次）']),
                    "click_avg_price": parse_value(row['点击均价（元）'], 0.0),
                    "shop_view_count": parse_value(row['商户浏览量（次）']),
                    "coupon_order_count": parse_value(row['优惠预订订单量（个）']),
                    "groupbuy_order_count": parse_value(row['团购订单量（个）']),
                    "order_count": parse_value(row['订单量（个）']),
                    "view_pic_count": parse_value(row['查看图片（次）']),
                    "view_comment_count": parse_value(row['查看评论（次）']),
                    "view_address_count": parse_value(row['查看地址（次）']),
                    "view_phone_count": parse_value(row['查看电话（次）']),
                    "view_groupbuy_count": parse_value(row['查看团购（次）']),
                    "collect_count": parse_value(row['收藏（次）']),
                    "share_count": parse_value(row['分享（次）'])
                }
                print(f"\n   [{idx+1}/{len(df)}] 上传数据:")
                print(f"      shop_id={json_param['shop_id']}, report_date={json_param['report_date']}, shop_name={json_param['shop_name']}")
                resp = requests.post(UPLOAD_APIS[table_name], headers={'Content-Type': 'application/json'},
                                     data=json.dumps(json_param), proxies={'http': None, 'https': None})
                print(f"      HTTP状态码: {resp.status_code}")
                print(f"      响应: {resp.text[:200] if resp.text else '(空)'}")
                if resp.status_code == 200:
                    success_count += 1
                    shop_ids_uploaded.add(json_param['shop_id'])
                    print(f"      ✅ 成功")
                else:
                    fail_count += 1
                    print(f"      ❌ 失败")
            except Exception as e:
                fail_count += 1
                print(f"      ❌ 异常: {e}")

        print(f"\n✅ 上传完成: 成功 {success_count}, 失败 {fail_count}")

        if fail_count == 0:
            result["success"] = True
            result["record_count"] = success_count
            for shop_id in shop_ids_uploaded:
                log_success(account_name, shop_id, table_name, start_date, end_date, success_count // len(shop_ids_uploaded) if shop_ids_uploaded else success_count)
            # 任务成功后删除临时文件
            if save_path:
                delete_file_safely(save_path)
        else:
            result["error_message"] = f"部分上传失败: 成功{success_count}, 失败{fail_count}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    finally:
        # 确保关闭Session
        if session:
            session.close()

    return result


# ============================================================================
# review_detail_dianping 任务
# ============================================================================
def run_review_detail_dianping(account_name: str, start_date: str, end_date: str,
                               cookies: Dict = None, mtgsig: str = None, shop_info: List = None) -> Dict[str, Any]:
    """执行review_detail_dianping任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
        shop_info: 外部传入的门店信息（可选）
    """
    table_name = "review_detail_dianping"
    print(f"\n{'=' * 60}")
    print(f"💬 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}

    try:
        disable_proxy()

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            if not shop_info:
                shop_info = []
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
        shop_ids = get_shop_ids(shop_info)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://e.dianping.com/vg-platform-reviewmanage/shop-comment-dp/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        session = get_session()

        def timestamp_to_datetime(ts, default="1997-12-08 00:00:00"):
            if not ts or ts == 0:
                return default
            try:
                return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except:
                return default

        def safe_int(val, default=0):
            if val is None or val == '':
                return default
            try:
                return int(val)
            except:
                return default

        def safe_float(val, default=0.0):
            if val is None or val == '':
                return default
            try:
                return float(val)
            except:
                return default

        def safe_str(val, default=''):
            return str(val) if val is not None else default

        def get_review_shop_id(review):
            """
            获取正确的 shop_id
            优先级: shopIdStr > shopIdLong > shopId
            API的shopId字段在ID超过int32范围时会溢出为0
            """
            # 1. 优先使用 shopIdStr（字符串，最安全）
            shop_id_str = review.get('shopIdStr')
            if shop_id_str and str(shop_id_str) != '0':
                return str(shop_id_str)

            # 2. 其次使用 shopIdLong（长整型）
            shop_id_long = review.get('shopIdLong')
            if shop_id_long and shop_id_long != 0:
                return str(shop_id_long)

            # 3. 最后使用 shopId（可能溢出为0）
            shop_id = review.get('shopId')
            if shop_id and shop_id != 0:
                return str(shop_id)

            # 4. 都没有，返回 "0"
            return "0"

        all_reviews = []
        upload_stats = {"success": 0, "failed": 0}
        shop_ids_found = set()
        page_no = 1

        while True:
            print(f"\n📡 获取点评评价数据 第{page_no}页...")
            url = "https://e.dianping.com/review/app/index/ajax/pcreview/listV2"
            params = {
                'platform': 0, 'shopIdStr': '0', 'tagId': 0,
                'startDate': start_date, 'endDate': end_date,
                'pageNo': page_no, 'pageSize': 50, 'referType': 0, 'category': 0,
                'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1',
                'mtgsig': generate_mtgsig(cookies, mtgsig)
            }
            print(f"   请求参数: platform=0, startDate={start_date}, endDate={end_date}")

            resp = session.get(url, params=params, headers=headers, cookies=cookies, timeout=60, proxies={'http': None, 'https': None})
            resp_json = resp.json()

            print(f"   API响应码: {resp_json.get('code')}")

            # 检查是否登录失效
            resp_code = resp_json.get('code')
            resp_msg = resp_json.get('msg') or resp_json.get('message') or ''
            if is_auth_invalid_error(resp_code, resp_msg):
                error_msg = f"登录失效 (code={resp_code}, msg={resp_msg})"
                print(f"🚨 检测到登录失效: {error_msg}")
                handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                raise AuthInvalidError(error_msg)

            if resp_json.get('code') != 200:
                print(f"   ❌ API返回错误: {resp_json}")
                break

            msg_data = resp_json.get('msg', {})
            reviews = msg_data.get('reviewDetailDTOs', [])
            total = msg_data.get('totalReivewNum', 0)

            print(f"   获取到 {len(reviews)} 条, 总数 {total}")
            if not reviews:
                print(f"   ⚠️ 该日期范围内没有点评评价数据")
                break

            for review in reviews:
                # 获取正确的 shop_id（字符串格式，处理大整数溢出问题）
                # 优先级: shopIdStr > shopIdLong > shopId
                shop_id = get_review_shop_id(review)
                if shop_id and shop_id != "0":
                    try:
                        shop_ids_found.add(int(shop_id))
                    except ValueError:
                        pass

                # 映射数据
                star_raw = safe_int(review.get('star'), 0)
                add_time = timestamp_to_datetime(review.get('addTime'))
                update_time = timestamp_to_datetime(review.get('updateTime'), add_time)
                edit_time = timestamp_to_datetime(review.get('editTime'), "1997-12-08 00:00:00")
                score_map = review.get('scoreMap', {}) or {}
                pic_info = review.get('picInfo', []) or []
                video_info = review.get('videoInfo', []) or []
                reply_list = review.get('reviewFollowNoteDtoList', []) or []

                shop_reply = ""
                shop_reply_time = "1997-12-08 00:00:00"
                reply_list_formatted = []
                for reply in reply_list:
                    reply_content = safe_str(reply.get('noteBody', ''))
                    reply_time_str = timestamp_to_datetime(reply.get('addDate', 0), "1997-12-08 00:00:00")
                    reply_list_formatted.append({"reply_time": reply_time_str, "reply_content": reply_content})
                    if not shop_reply:
                        shop_reply = reply_content
                        shop_reply_time = reply_time_str

                upload_data = {
                    "review_id": safe_str(review.get('reviewId'), f"DP_{int(time.time())}"),
                    "shop_id": shop_id,
                    "shop_name": safe_str(review.get('shopName'), '未知门店'),
                    "city_name": safe_str(review.get('cityName'), '未知'),
                    "city_id": safe_int(review.get('cityId'), 0),
                    "user_id": safe_str(review.get('userId'), '0'),
                    "user_nickname": safe_str(review.get('userNickName'), '匿名用户'),
                    "user_face": safe_str(review.get('userFace'), ''),
                    "user_power": safe_str(review.get('userPower'), '') or '普通用户',
                    "vip_level": safe_int(review.get('vipLevel'), 0),
                    "add_time": add_time,
                    "update_time": update_time,
                    "edit_time": edit_time,
                    "star": star_raw,
                    "star_display": star_raw // 10 if star_raw else 0,
                    "accurate_star": safe_int(review.get('accurateStar'), star_raw),
                    "content": safe_str(review.get('content'), '') or '无',
                    "score_technician": safe_float(score_map.get('技师', 0)),
                    "score_service": safe_float(score_map.get('服务', 0)),
                    "score_environment": safe_float(score_map.get('环境', 0)),
                    "score_map": json.dumps(score_map, ensure_ascii=False),
                    "pic_count": len(pic_info),
                    "video_count": len(video_info),
                    "pic_info": json.dumps(pic_info, ensure_ascii=False),
                    "video_info": json.dumps(video_info, ensure_ascii=False),
                    "shop_reply": shop_reply or '暂无回复',
                    "shop_reply_time": shop_reply_time,
                    "is_reply_with_photo": safe_int(review.get('isReplyWithPhoto'), 0),
                    "reply_list": json.dumps(reply_list_formatted, ensure_ascii=False),
                    "order_id": safe_int(review.get('orderId'), 0),
                    "deal_group_id": safe_int(review.get('dealGroupId'), 0),
                    "refer_type": safe_int(review.get('referType'), 0),
                    "avg_price": safe_float(review.get('avgPrice'), 0),
                    "serial_numbers": safe_str(review.get('serialNumbers'), '') or '无',
                    "total_cost": safe_float(review.get('totalCost'), 0),
                    "consume_date": review.get('consumeDate') or "1997-12-08",
                    "status": safe_int(review.get('status'), 1),
                    "quality_score": safe_int(review.get('qualityScore'), 0),
                    "case_status": safe_int(review.get('caseStatus'), 0),
                    "case_status_desc": safe_str(review.get('caseStatusDesc'), ''),
                    "report_status": safe_int(review.get('reportStatus'), 0),
                    "report_status_desc": safe_str(review.get('reportStatusDesc'), '') or '无',
                    "case_id": safe_int(review.get('caseId'), 0),
                    "show_deal": 1 if review.get('showDeal', True) else 0,
                    "raw_data": json.dumps(review, ensure_ascii=False)
                }

                try:
                    print(f"\n      上传点评评价 review_id={upload_data.get('review_id')}, shop_id={upload_data.get('shop_id')}")
                    print(f"         user_nickname={upload_data.get('user_nickname')}, content={upload_data.get('content', '')[:50]}...")
                    upload_resp = session.post(UPLOAD_APIS[table_name], headers={'Content-Type': 'application/json'},
                                               json=upload_data, timeout=30, proxies={'http': None, 'https': None})
                    print(f"         HTTP状态码: {upload_resp.status_code}")
                    print(f"         响应: {upload_resp.text[:200] if upload_resp.text else '(空)'}")
                    if upload_resp.status_code == 200:
                        upload_stats["success"] += 1
                        print(f"         ✅ 成功")
                    else:
                        upload_stats["failed"] += 1
                        print(f"         ❌ 失败")
                        print(f"         原始数据: {json.dumps(review, ensure_ascii=False)[:500]}")
                except Exception as e:
                    upload_stats["failed"] += 1
                    print(f"         ❌ 异常: {e}")
                    print(f"         原始数据: {json.dumps(review, ensure_ascii=False)[:500]}")
                time.sleep(0.3)

            all_reviews.extend(reviews)
            if len(all_reviews) >= total:
                break
            page_no += 1
            random_delay()  # 反爬虫等待

        print(f"\n📊 点评评价完成: 获取 {len(all_reviews)} 条, 上传成功 {upload_stats['success']}, 失败 {upload_stats['failed']}")

        if upload_stats["failed"] == 0:
            result["success"] = True
            result["record_count"] = upload_stats["success"]
            for shop_id in (shop_ids_found or shop_ids):
                log_success(account_name, shop_id, table_name, start_date, end_date, upload_stats["success"])
        else:
            result["error_message"] = f"部分上传失败: 成功{upload_stats['success']}, 失败{upload_stats['failed']}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    return result


# ============================================================================
# review_detail_meituan 任务
# ============================================================================
def run_review_detail_meituan(account_name: str, start_date: str, end_date: str,
                              cookies: Dict = None, mtgsig: str = None, shop_info: List = None) -> Dict[str, Any]:
    """执行review_detail_meituan任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
        shop_info: 外部传入的门店信息（可选）
    """
    table_name = "review_detail_meituan"
    print(f"\n{'=' * 60}")
    print(f"🍔 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}

    try:
        disable_proxy()

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            if not shop_info:
                shop_info = []
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
        shop_ids = get_shop_ids(shop_info)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://e.dianping.com/vg-platform-reviewmanage/shop-comment-mt/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        session = get_session()

        def timestamp_to_datetime(ts, default="1997-12-08 00:00:00"):
            if not ts or ts == 0:
                return default
            try:
                return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except:
                return default

        def safe_int(val, default=0):
            if val is None or val == '':
                return default
            try:
                return int(val)
            except:
                return default

        def safe_float(val, default=0.0):
            if val is None or val == '':
                return default
            try:
                return float(val)
            except:
                return default

        def safe_str(val, default=''):
            return str(val) if val is not None else default

        def extract_order_info(order_info_list, field_id, default=''):
            if not order_info_list:
                return default
            for item in order_info_list:
                if item.get('id') == field_id:
                    return safe_str(item.get('content'), default)
            return default

        def get_review_shop_id(review):
            """
            获取正确的 shop_id
            优先级: shopIdStr > shopIdLong > shopId
            API的shopId字段在ID超过int32范围时会溢出为0
            """
            # 1. 优先使用 shopIdStr（字符串，最安全）
            shop_id_str = review.get('shopIdStr')
            if shop_id_str and str(shop_id_str) != '0':
                return str(shop_id_str)

            # 2. 其次使用 shopIdLong（长整型）
            shop_id_long = review.get('shopIdLong')
            if shop_id_long and shop_id_long != 0:
                return str(shop_id_long)

            # 3. 最后使用 shopId（可能溢出为0）
            shop_id = review.get('shopId')
            if shop_id and shop_id != 0:
                return str(shop_id)

            # 4. 都没有，返回 "0"
            return "0"

        all_reviews = []
        upload_stats = {"success": 0, "failed": 0}
        shop_ids_found = set()
        page_no = 1

        while True:
            print(f"\n📡 获取美团评价数据 第{page_no}页...")
            url = "https://e.dianping.com/review/app/index/ajax/pcreview/listV2"
            params = {
                'platform': 1, 'shopIdStr': '0', 'tagId': 0,
                'startDate': start_date, 'endDate': end_date,
                'pageNo': page_no, 'pageSize': 50, 'referType': 0, 'category': 0,
                'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1',
                'mtgsig': generate_mtgsig(cookies, mtgsig)
            }
            print(f"   请求参数: platform=1, startDate={start_date}, endDate={end_date}")

            resp = session.get(url, params=params, headers=headers, cookies=cookies, timeout=60, proxies={'http': None, 'https': None})
            resp_json = resp.json()

            print(f"   API响应码: {resp_json.get('code')}")

            # 检查是否登录失效
            resp_code = resp_json.get('code')
            resp_msg = resp_json.get('msg') or resp_json.get('message') or ''
            if is_auth_invalid_error(resp_code, resp_msg):
                error_msg = f"登录失效 (code={resp_code}, msg={resp_msg})"
                print(f"🚨 检测到登录失效: {error_msg}")
                handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                raise AuthInvalidError(error_msg)

            if resp_json.get('code') != 200:
                print(f"   ❌ API返回错误: {resp_json}")
                break

            msg_data = resp_json.get('msg', {})
            reviews = msg_data.get('reviewDetailDTOs', [])
            total = msg_data.get('totalReivewNum', 0)

            print(f"   获取到 {len(reviews)} 条, 总数 {total}")
            if not reviews:
                print(f"   ⚠️ 该日期范围内没有美团评价数据")
                break

            for review in reviews:
                # 获取正确的 shop_id（字符串格式，处理大整数溢出问题）
                # 优先级: shopIdStr > shopIdLong > shopId
                shop_id = get_review_shop_id(review)
                if shop_id and shop_id != "0":
                    try:
                        shop_ids_found.add(int(shop_id))
                    except ValueError:
                        pass

                star_raw = safe_int(review.get('star'), 0)
                add_time = timestamp_to_datetime(review.get('addTime'))
                update_time = timestamp_to_datetime(review.get('updateTime'), add_time)
                edit_time = timestamp_to_datetime(review.get('editTime'), "1997-12-08 00:00:00")
                pic_info = review.get('picInfo', []) or []
                video_info = review.get('videoInfo', []) or []
                shop_reply = safe_str(review.get('shopReply'), '') or '暂无回复'
                shop_reply_time = timestamp_to_datetime(review.get('shopReplyTime'), "1997-12-08 00:00:00")

                reply_list_formatted = []
                if review.get('shopReply'):
                    reply_list_formatted.append({"reply_time": shop_reply_time, "reply_content": review.get('shopReply')})

                order_info_list = review.get('orderInfoDTOList', [])
                business_type = extract_order_info(order_info_list, 9, '无')
                coupon_code = extract_order_info(order_info_list, 1, '无')
                product_name = extract_order_info(order_info_list, 2, '无')
                order_time = extract_order_info(order_info_list, 3, '1997-12-08').strip()
                consume_time = extract_order_info(order_info_list, 4, '1997-12-08').strip()
                quantity = safe_int(extract_order_info(order_info_list, 5, '0'), 0)
                price = safe_float(extract_order_info(order_info_list, 6, '0'), 0)

                upload_data = {
                    "review_id": safe_str(review.get('reviewId'), f"MT_{int(time.time())}"),
                    "feedback_id": safe_int(review.get('feedbackId'), 0),
                    "shop_id": shop_id,
                    "shop_name": safe_str(review.get('shopName'), '未知门店'),
                    "city_name": safe_str(review.get('cityName'), '未知'),
                    "city_id": safe_int(review.get('cityId'), 0),
                    "user_id": safe_str(review.get('userId'), '0'),
                    "user_nickname": safe_str(review.get('userNickName'), '匿名用户'),
                    "user_face": safe_str(review.get('userFace'), ''),
                    "user_power": safe_str(review.get('userPower'), '') or '普通用户',
                    "anonymous": 1 if review.get('anonymous', False) else 0,
                    "add_time": add_time,
                    "update_time": update_time,
                    "edit_time": edit_time,
                    "star": star_raw,
                    "star_display": star_raw // 10 if star_raw else 0,
                    "accurate_star": safe_int(review.get('accurateStar'), star_raw),
                    "content": safe_str(review.get('content'), '') or '无',
                    "pic_count": len(pic_info),
                    "video_count": len(video_info),
                    "pic_info": json.dumps(pic_info, ensure_ascii=False),
                    "video_info": json.dumps(video_info, ensure_ascii=False),
                    "shop_reply": shop_reply,
                    "shop_reply_time": shop_reply_time,
                    "reply_list": json.dumps(reply_list_formatted, ensure_ascii=False),
                    "order_id": safe_int(review.get('orderId'), 0),
                    "refer_type": safe_int(review.get('referType'), 0),
                    "business_type": business_type,
                    "coupon_code": coupon_code,
                    "product_name": product_name,
                    "order_time": order_time,
                    "consume_time": consume_time,
                    "quantity": quantity,
                    "price": price,
                    "case_status": safe_int(review.get('caseStatus'), 0),
                    "report_status": safe_int(review.get('reportStatus'), 0),
                    "case_id": safe_int(review.get('caseId'), 0),
                    "show_deal": 1 if review.get('showDeal', True) else 0,
                    "raw_data": json.dumps(review, ensure_ascii=False)
                }

                try:
                    print(f"\n      上传美团评价 review_id={upload_data.get('review_id')}, shop_id={upload_data.get('shop_id')}")
                    print(f"         user_nickname={upload_data.get('user_nickname')}, content={upload_data.get('content', '')[:50]}...")
                    upload_resp = session.post(UPLOAD_APIS[table_name], headers={'Content-Type': 'application/json'},
                                               json=upload_data, timeout=30, proxies={'http': None, 'https': None})
                    print(f"         HTTP状态码: {upload_resp.status_code}")
                    print(f"         响应: {upload_resp.text[:200] if upload_resp.text else '(空)'}")
                    if upload_resp.status_code == 200:
                        upload_stats["success"] += 1
                        print(f"         ✅ 成功")
                    else:
                        upload_stats["failed"] += 1
                        print(f"         ❌ 失败")
                        print(f"         原始数据: {json.dumps(review, ensure_ascii=False)[:500]}")
                except Exception as e:
                    upload_stats["failed"] += 1
                    print(f"         ❌ 异常: {e}")
                    print(f"         原始数据: {json.dumps(review, ensure_ascii=False)[:500]}")
                time.sleep(0.3)

            all_reviews.extend(reviews)
            if len(all_reviews) >= total:
                break
            page_no += 1
            random_delay()  # 反爬虫等待

        print(f"\n📊 美团评价完成: 获取 {len(all_reviews)} 条, 上传成功 {upload_stats['success']}, 失败 {upload_stats['failed']}")

        if upload_stats["failed"] == 0:
            result["success"] = True
            result["record_count"] = upload_stats["success"]
            for shop_id in (shop_ids_found or shop_ids):
                log_success(account_name, shop_id, table_name, start_date, end_date, upload_stats["success"])
        else:
            result["error_message"] = f"部分上传失败: 成功{upload_stats['success']}, 失败{upload_stats['failed']}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    return result


# ============================================================================
# review_summary_dianping 任务
# ============================================================================
def run_review_summary_dianping(account_name: str, start_date: str, end_date: str,
                                cookies: Dict = None, mtgsig: str = None, shop_info: List = None) -> Dict[str, Any]:
    """执行review_summary_dianping任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
        shop_info: 外部传入的门店信息（可选）
    """
    table_name = "review_summary_dianping"
    print(f"\n{'=' * 60}")
    print(f"💬 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}

    try:
        disable_proxy()
        Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            if not shop_info:
                shop_info = []
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
        shop_ids = get_shop_ids(shop_info)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://e.dianping.com',
            'Referer': 'https://e.dianping.com/app/merchant-workbench/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        session = get_session()

        # 触发下载
        print(f"\n📤 触发下载任务...")
        trigger_url = "https://e.dianping.com/gateway/merchant/review/pc/reviewdownload"
        trigger_params = {"yodaReady": "h5", "csecplatform": "4", "csecversion": "4.1.1", "mtgsig": generate_mtgsig(cookies, mtgsig)}
        trigger_payload = {"tagId": 0, "platform": 1, "shopIdStr": "0", "startDate": start_date, "endDate": end_date}

        trigger_resp = session.post(trigger_url, params=trigger_params, headers=headers, cookies=cookies, json=trigger_payload, timeout=60)
        trigger_json = trigger_resp.json()
        print(f"   响应: {trigger_json}")

        # 检查触发响应是否登录失效
        trigger_code = trigger_json.get('code')
        trigger_msg = trigger_json.get('msg') or trigger_json.get('message') or ''
        if is_auth_invalid_error(trigger_code, trigger_msg):
            error_msg = f"登录失效 (code={trigger_code}, msg={trigger_msg})"
            print(f"🚨 检测到登录失效: {error_msg}")
            handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
            raise AuthInvalidError(error_msg)

        random_delay()  # 反爬虫等待

        # 等待文件生成
        print(f"\n⏳ 等待文件生成...")
        trigger_time = time.time()
        file_record = None

        for _ in range(30):
            time.sleep(2)
            list_url = "https://e.dianping.com/gateway/merchant/downloadcenter/list"
            list_params = {'pageNo': 1, 'pageSize': 20, 'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': generate_mtgsig(cookies, mtgsig)}
            list_resp = session.get(list_url, params=list_params, headers=headers, cookies=cookies, timeout=30)
            list_data = list_resp.json()

            # 检查是否登录失效
            list_code = list_data.get('code')
            list_msg = list_data.get('msg') or list_data.get('message') or ''
            if is_auth_invalid_error(list_code, list_msg):
                error_msg = f"登录失效 (code={list_code}, msg={list_msg})"
                print(f"🚨 检测到登录失效: {error_msg}")
                handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                raise AuthInvalidError(error_msg)

            if list_data.get('code') == 200:
                for record in list_data.get('data', {}).get('records', []):
                    file_name = record.get('fileName', '')
                    if '门店评价' in file_name and record.get('recordStatus') == 300 and record.get('downloadable') == "1" and record.get('fileUrl'):
                        add_time = record.get('addTime', '')
                        try:
                            file_time = datetime.strptime(add_time, '%Y-%m-%d %H:%M:%S')
                            if file_time.timestamp() >= trigger_time - 10:
                                file_record = record
                                print(f"   ✅ 文件已就绪: {file_name}")
                                break
                        except:
                            file_record = record
                            break
            if file_record:
                break

        if not file_record:
            raise Exception("文件生成超时")

        random_delay()  # 反爬虫等待

        # 下载文件
        file_url = file_record['fileUrl']
        file_name = file_record.get('fileName', f'点评评价_{start_date}_{end_date}.xlsx')
        save_path = str(Path(SAVE_DIR) / file_name)

        print(f"📥 正在下载文件...")
        print(f"   URL: {file_url[:80]}...")
        dl_resp = session.get(file_url, timeout=120, stream=True)
        with open(save_path, 'wb') as f:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        file_size = Path(save_path).stat().st_size
        print(f"✅ 文件已保存到: {save_path}")
        print(f"   文件大小: {file_size / 1024:.2f} KB")

        # 检查文件是否为空或无效
        if file_size < 1000:  # 小于1KB可能是空文件
            print(f"⚠️ 文件可能为空或无效 (大小: {file_size} 字节)")

        # 上传数据
        print(f"\n📤 开始上传评价数据...")
        try:
            df = pd.read_excel(save_path)
        except ValueError as e:
            if "Worksheet index" in str(e) or "0 worksheets found" in str(e):
                print(f"⚠️ Excel文件为空(没有工作表)，该日期范围可能没有点评评价数据")
                result["success"] = True
                result["record_count"] = 0
                result["error_message"] = "无数据"
                return result
            raise
        success_count = 0
        fail_count = 0
        shop_ids_found = set()

        EMPTY_DATETIME = "1970-01-01 00:00:00"

        def format_datetime(dt_str):
            if pd.isna(dt_str) or str(dt_str).strip() == '':
                return EMPTY_DATETIME
            dt_str = str(dt_str).strip()
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"]:
                try:
                    return datetime.strptime(dt_str, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    continue
            return dt_str

        def safe_int(val, default=0):
            if pd.isna(val):
                return default
            try:
                return int(val)
            except:
                return default

        def safe_str(val, default=""):
            if pd.isna(val):
                return default
            return str(val).strip()

        for idx, row in df.iterrows():
            try:
                content = safe_str(row.get('评价内容')) or '无'
                is_replied = "是" if row.get('商家是否已经回复') == '已回复' else "否"
                is_after_consume = "是" if row.get('是否消费后评价') == '是' else "否"
                dp_shop_id = safe_int(row.get('点评门店ID'), None)
                if dp_shop_id:
                    shop_ids_found.add(dp_shop_id)

                params = {
                    "review_time": format_datetime(row.get('评价时间')),
                    "city": safe_str(row.get('城市')),
                    "shop_name": safe_str(row.get('评价门店')),
                    "dianping_shop_id": dp_shop_id,
                    "meituan_shop_id": safe_int(row.get('美团门店ID'), None),
                    "user_nickname": safe_str(row.get('用户昵称')),
                    "star": safe_str(row.get('星级')),
                    "score_detail": safe_str(row.get('评分')),
                    "content": content,
                    "content_length": safe_int(row.get('评价正文字数'), len(content)),
                    "pic_count": safe_int(row.get('图片数'), 0),
                    "video_count": safe_int(row.get('视频数'), 0),
                    "is_replied": is_replied,
                    "first_reply_time": format_datetime(row.get('商家首次回复时间')),
                    "is_after_consume": is_after_consume,
                    "consume_time": format_datetime(row.get('消费时间'))
                }

                print(f"\n   [{idx+1}/{len(df)}] 上传点评评价:")
                print(f"      shop_name={params.get('shop_name')}, dianping_shop_id={params.get('dianping_shop_id')}")
                print(f"      user_nickname={params.get('user_nickname')}, content={params.get('content', '')[:50]}...")
                resp = requests.post(UPLOAD_APIS[table_name], headers={'Content-Type': 'application/json'},
                                     data=json.dumps(params, ensure_ascii=False).encode('utf-8'),
                                     timeout=30, proxies={'http': None, 'https': None})
                print(f"      HTTP状态码: {resp.status_code}")
                print(f"      响应: {resp.text[:200] if resp.text else '(空)'}")
                if resp.status_code == 200:
                    success_count += 1
                    print(f"      ✅ 成功")
                else:
                    fail_count += 1
                    print(f"      ❌ 失败")
                    print(f"      完整参数: {json.dumps(params, ensure_ascii=False)}")
            except Exception as e:
                fail_count += 1
                print(f"      ❌ 异常: {e}")
                print(f"      完整参数: {json.dumps(params, ensure_ascii=False)}")

        print(f"\n✅ 上传完成: 成功 {success_count}, 失败 {fail_count}")

        if fail_count == 0:
            result["success"] = True
            result["record_count"] = success_count
            for shop_id in (shop_ids_found or shop_ids):
                log_success(account_name, shop_id, table_name, start_date, end_date, success_count)
        else:
            result["error_message"] = f"部分上传失败: 成功{success_count}, 失败{fail_count}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    return result


# ============================================================================
# review_summary_meituan 任务
# ============================================================================
def run_review_summary_meituan(account_name: str, start_date: str, end_date: str,
                               cookies: Dict = None, mtgsig: str = None, shop_info: List = None) -> Dict[str, Any]:
    """执行review_summary_meituan任务

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
        shop_info: 外部传入的门店信息（可选）
    """
    table_name = "review_summary_meituan"
    print(f"\n{'=' * 60}")
    print(f"🍔 {table_name}")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}

    try:
        disable_proxy()
        Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)

        # 优先使用外部传入的数据（页面驱动模式）
        if cookies and mtgsig:
            print(f"📌 使用共享 Cookie/签名（无需调用API）")
            if not shop_info:
                shop_info = []
        else:
            # 没有外部数据，从API获取
            api_data = load_cookies_from_api(account_name)
            cookies = api_data['cookies']
            mtgsig = api_data['mtgsig']
            shop_info = api_data['shop_info']
        shop_ids = get_shop_ids(shop_info)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://e.dianping.com',
            'Referer': 'https://e.dianping.com/vg-platform-reviewmanage/shop-comment-mt/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        session = get_session()

        # 触发下载
        print(f"\n📤 触发美团评价下载任务...")
        trigger_url = "https://e.dianping.com/gateway/merchant/review/pc/reviewdownload"
        trigger_params = {"yodaReady": "h5", "csecplatform": "4", "csecversion": "4.1.1", "mtgsig": generate_mtgsig(cookies, mtgsig)}
        trigger_payload = {"tagId": 0, "platform": 2, "shopIdStr": "0", "startDate": start_date, "endDate": end_date}

        trigger_resp = session.post(trigger_url, params=trigger_params, headers=headers, cookies=cookies, json=trigger_payload, timeout=60)
        trigger_json = trigger_resp.json()
        print(f"   响应: {trigger_json}")

        # 检查触发响应是否登录失效
        trigger_code = trigger_json.get('code')
        trigger_msg = trigger_json.get('msg') or trigger_json.get('message') or ''
        if is_auth_invalid_error(trigger_code, trigger_msg):
            error_msg = f"登录失效 (code={trigger_code}, msg={trigger_msg})"
            print(f"🚨 检测到登录失效: {error_msg}")
            handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
            raise AuthInvalidError(error_msg)

        random_delay()  # 反爬虫等待

        # 等待文件生成
        print(f"\n⏳ 等待文件生成...")
        trigger_time = time.time()
        file_record = None

        for _ in range(30):
            time.sleep(2)
            list_url = "https://e.dianping.com/gateway/merchant/downloadcenter/list"
            list_params = {'pageNo': 1, 'pageSize': 20, 'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': generate_mtgsig(cookies, mtgsig)}
            list_resp = session.get(list_url, params=list_params, headers=headers, cookies=cookies, timeout=30)
            list_data = list_resp.json()

            # 检查是否登录失效
            list_code = list_data.get('code')
            list_msg = list_data.get('msg') or list_data.get('message') or ''
            if is_auth_invalid_error(list_code, list_msg):
                error_msg = f"登录失效 (code={list_code}, msg={list_msg})"
                print(f"🚨 检测到登录失效: {error_msg}")
                handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)
                raise AuthInvalidError(error_msg)

            if list_data.get('code') == 200:
                for record in list_data.get('data', {}).get('records', []):
                    file_name = record.get('fileName', '')
                    if ('评价' in file_name or '门店评价' in file_name) and record.get('recordStatus') == 300 and record.get('downloadable') == "1" and record.get('fileUrl'):
                        add_time = record.get('addTime', '')
                        try:
                            file_time = datetime.strptime(add_time, '%Y-%m-%d %H:%M:%S')
                            if file_time.timestamp() >= trigger_time - 10:
                                file_record = record
                                print(f"   ✅ 文件已就绪: {file_name}")
                                break
                        except:
                            file_record = record
                            break
            if file_record:
                break

        if not file_record:
            raise Exception("文件生成超时")

        random_delay()  # 反爬虫等待

        # 下载文件
        file_url = file_record['fileUrl']
        file_name = file_record.get('fileName', f'美团评价_{start_date}_{end_date}.xlsx')
        save_path = str(Path(SAVE_DIR) / file_name)

        print(f"📥 正在下载文件...")
        print(f"   URL: {file_url[:80]}...")
        dl_resp = session.get(file_url, timeout=120, stream=True)
        with open(save_path, 'wb') as f:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        file_size = Path(save_path).stat().st_size
        print(f"✅ 文件已保存到: {save_path}")
        print(f"   文件大小: {file_size / 1024:.2f} KB")

        # 检查文件是否为空或无效
        if file_size < 1000:
            print(f"⚠️ 文件可能为空或无效 (大小: {file_size} 字节)")

        # 上传数据
        print(f"\n📤 开始上传美团评价数据...")
        try:
            df = pd.read_excel(save_path)
        except ValueError as e:
            if "Worksheet index" in str(e) or "0 worksheets found" in str(e):
                print(f"⚠️ Excel文件为空(没有工作表)，该日期范围可能没有美团评价数据")
                result["success"] = True
                result["record_count"] = 0
                result["error_message"] = "无数据"
                return result
            raise
        success_count = 0
        fail_count = 0
        shop_ids_found = set()

        EMPTY_DATETIME = "1970-01-01 00:00:00"

        def format_datetime(dt_str):
            if pd.isna(dt_str) or str(dt_str).strip() == '':
                return EMPTY_DATETIME
            dt_str = str(dt_str).strip()
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"]:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    if fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
                        return dt.strftime("%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    continue
            return dt_str

        def safe_int(val, default=0):
            if pd.isna(val):
                return default
            try:
                return int(val)
            except:
                return default

        def safe_str(val, default=""):
            if pd.isna(val):
                return default
            return str(val).strip()

        for idx, row in df.iterrows():
            try:
                content = safe_str(row.get('评价内容')) or '无'
                is_replied_raw = row.get('商家是否已经回复')
                is_replied = "是" if is_replied_raw == '已回复' or is_replied_raw == '是' else "否"
                is_after_consume = "是" if row.get('是否消费后评价') == '是' else "否"
                mt_shop_id = safe_int(row.get('美团门店ID'), None)
                if mt_shop_id:
                    shop_ids_found.add(mt_shop_id)

                params = {
                    "review_time": format_datetime(row.get('评价时间')),
                    "city": safe_str(row.get('城市')),
                    "shop_name": safe_str(row.get('评价门店')),
                    "dianping_shop_id": safe_int(row.get('点评门店ID'), None),
                    "meituan_shop_id": mt_shop_id,
                    "user_nickname": safe_str(row.get('用户昵称')),
                    "star": safe_str(row.get('星级')),
                    "content": content,
                    "content_length": safe_int(row.get('评价正文字数'), len(content)),
                    "pic_count": safe_int(row.get('图片数'), 0),
                    "video_count": safe_int(row.get('视频数'), 0),
                    "is_replied": is_replied,
                    "first_reply_time": format_datetime(row.get('商家首次回复时间')),
                    "is_after_consume": is_after_consume,
                    "consume_time": format_datetime(row.get('消费时间'))
                }

                print(f"\n   [{idx+1}/{len(df)}] 上传美团评价:")
                print(f"      shop_name={params.get('shop_name')}, meituan_shop_id={params.get('meituan_shop_id')}")
                print(f"      user_nickname={params.get('user_nickname')}, content={params.get('content', '')[:50]}...")
                resp = requests.post(UPLOAD_APIS[table_name], headers={'Content-Type': 'application/json'},
                                     data=json.dumps(params, ensure_ascii=False).encode('utf-8'),
                                     timeout=30, proxies={'http': None, 'https': None})
                print(f"      HTTP状态码: {resp.status_code}")
                print(f"      响应: {resp.text[:200] if resp.text else '(空)'}")
                if resp.status_code == 200:
                    success_count += 1
                    print(f"      ✅ 成功")
                else:
                    fail_count += 1
                    print(f"      ❌ 失败")
                    print(f"      完整参数: {json.dumps(params, ensure_ascii=False)}")
            except Exception as e:
                fail_count += 1
                print(f"      ❌ 异常: {e}")
                print(f"      完整参数: {json.dumps(params, ensure_ascii=False)}")

        print(f"\n✅ 上传完成: 成功 {success_count}, 失败 {fail_count}")

        if fail_count == 0:
            result["success"] = True
            result["record_count"] = success_count
            for shop_id in (shop_ids_found or shop_ids):
                log_success(account_name, shop_id, table_name, start_date, end_date, success_count)
        else:
            result["error_message"] = f"部分上传失败: 成功{success_count}, 失败{fail_count}"
            for shop_id in shop_ids:
                log_failure(account_name, shop_id, table_name, start_date, end_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 已在检测时调用 handle_auth_invalid，这里只记录结果
        result["error_message"] = str(e)
        print(f"❌ 登录失效，任务终止: {e}")

    except Exception as e:
        result["error_message"] = str(e)
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        log_failure(account_name, 0, table_name, start_date, end_date, str(e))

    return result


# ============================================================================
# DianpingStoreStats 类 (门店统计数据采集，使用Playwright浏览器)
# ============================================================================
class DianpingStoreStats:
    """大众点评门店统计数据采集类（带Playwright支持）"""

    def __init__(self, account_name: str, platform_api_url: str, headless: bool = True, disable_proxy: bool = True,
                 external_page=None, cookies: Dict = None, mtgsig: str = None, shop_info: List = None,
                 compare_regions: Dict = None, brands_json: List = None):
        """初始化

        Args:
            account_name: 账户名称
            platform_api_url: 平台API URL
            headless: 是否使用无头模式
            disable_proxy: 是否禁用代理
            external_page: 外部传入的 Playwright page 对象（用于页面驱动模式）
            cookies: 外部传入的Cookie（可选，避免重复调用API）
            mtgsig: 外部传入的签名（可选）
            shop_info: 外部传入的门店信息（可选）
            compare_regions: 外部传入的门店商圈信息（可选，用于同行排名）
            brands_json: 外部传入的团购ID映射（可选，用于广告单）
        """
        self.account_name = account_name
        self.platform_api_url = platform_api_url
        self.headless = headless
        self.disable_proxy = disable_proxy
        self.state_file = os.path.join(STATE_DIR, f'dianping_state_{account_name}.json')

        # Playwright相关
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 外部传入的 page 对象（页面驱动模式使用）
        self.external_page = external_page
        self.use_external_page = external_page is not None

        # 登录失效标志 - 一旦检测到失效，停止后续数据获取
        self.login_invalid = False
        self.login_invalid_error = ""

        # 从API获取的数据
        self.cookies = {}
        self.mtgsig_from_api = None
        self.shop_id = None
        self.shop_list = []
        self.product_mapping = []
        self.shop_region_info = {}
        self.cookie_data = None

        # 外部传入的数据（页面驱动模式，避免重复调用API）
        self._external_cookies = cookies
        self._external_mtgsig = mtgsig
        self._external_shop_info = shop_info
        self._external_compare_regions = compare_regions
        self._external_brands_json = brands_json

        if self.disable_proxy:
            self._disable_proxy()

        self._load_account_info_from_api()

    def _disable_proxy(self):
        """禁用系统代理"""
        proxy_vars = [
            'HTTP_PROXY', 'HTTPS_PROXY', 'FTP_PROXY', 'SOCKS_PROXY',
            'http_proxy', 'https_proxy', 'ftp_proxy', 'socks_proxy',
            'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy'
        ]
        for var in proxy_vars:
            os.environ.pop(var, None)
        os.environ['NO_PROXY'] = '*'
        os.environ['no_proxy'] = '*'
        print("✅ 已禁用系统代理")

    def _get_session(self) -> requests.Session:
        """获取禁用代理的session"""
        session = requests.Session()
        session.trust_env = False
        session.proxies = {'http': None, 'https': None, 'ftp': None, 'socks': None, 'no_proxy': '*'}
        session.mount('http://', requests.adapters.HTTPAdapter())
        session.mount('https://', requests.adapters.HTTPAdapter())
        return session

    def _load_account_info_from_api(self):
        """加载账户信息（优先使用外部传入数据，没有才调用API）"""
        try:
            # 检查是否有外部传入的数据（页面驱动模式）
            if self._external_cookies and self._external_shop_info:
                print(f"📌 使用外部传入的 Cookie/签名/门店信息（无需调用API）")

                # 使用外部传入的数据
                self.cookies = self._external_cookies
                print(f"✅ 成功加载 {len(self.cookies)} 个cookies（来自共享数据）")

                if self._external_mtgsig:
                    self.mtgsig_from_api = self._external_mtgsig
                    print(f"   已获取mtgsig: {self.mtgsig_from_api[:50]}...")

                # 处理门店信息（可能是列表或字典格式）
                if isinstance(self._external_shop_info, list):
                    self.shop_list = self._external_shop_info
                elif isinstance(self._external_shop_info, dict):
                    # 如果是字典，尝试提取门店列表
                    self.shop_list = [self._external_shop_info] if self._external_shop_info.get('shop_id') else []
                else:
                    self.shop_list = []

                if self.shop_list:
                    print(f"✅ 成功加载 {len(self.shop_list)} 个门店（来自共享数据）")
                    for shop in self.shop_list:
                        print(f"   - {shop.get('shop_name')} ({shop.get('shop_id')})")
                else:
                    # 门店信息为空，需要从API获取
                    print(f"⚠️ 共享数据中无门店信息，从API补充获取...")
                    self._fetch_additional_info_from_api()

                # 处理门店商圈信息（用于同行排名）
                if self._external_compare_regions:
                    self.shop_region_info = self._external_compare_regions
                    print(f"✅ 成功加载 {len(self.shop_region_info)} 个门店商圈信息（来自共享数据）")

                # 处理团购ID映射（用于广告单）
                if self._external_brands_json:
                    self.product_mapping = self._external_brands_json
                    print(f"✅ 成功加载 {len(self.product_mapping)} 个团购ID映射（来自共享数据）")

                # 获取店铺ID
                self.shop_id = self.cookies.get('mpmerchant_portal_shopid', '')
                if not self.shop_id and self.shop_list:
                    self.shop_id = self.shop_list[0].get('shop_id')

                # 检测并补全门店/商圈数据（任一为空则触发）
                self._check_and_complete_stores_regions()

                return

            # 没有外部数据，从API获取完整信息
            print(f"🔍 正在从API获取账户 [{self.account_name}] 的完整信息...")
            headers = {'Content-Type': 'application/json'}
            data = json.dumps({"account": self.account_name})

            session = self._get_session()
            response = session.post(self.platform_api_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            result = response.json()

            if not result or not result.get('success'):
                raise Exception(f"API返回失败")

            data = result.get('data', {})
            if not data:
                raise Exception(f"API返回的data为空")

            self.cookie_data = data

            # 获取cookies
            cookie_data = data.get('cookie', {})
            if cookie_data:
                self.cookies = cookie_data
                print(f"✅ 成功加载 {len(self.cookies)} 个cookies")
            else:
                raise Exception("未获取到cookie数据")

            # 获取mtgsig
            mtgsig_data = data.get('mtgsig')
            if mtgsig_data:
                if isinstance(mtgsig_data, str):
                    self.mtgsig_from_api = mtgsig_data
                else:
                    self.mtgsig_from_api = json.dumps(mtgsig_data)
                print(f"   已获取mtgsig: {self.mtgsig_from_api[:50]}...")

            # 获取门店列表（使用 or [] 确保 null/None 也能正确转为空列表）
            stores_json = data.get('stores_json') or []
            if stores_json:
                self.shop_list = stores_json
                print(f"✅ 成功加载 {len(self.shop_list)} 个门店")
                for shop in self.shop_list:
                    print(f"   - {shop.get('shop_name')} ({shop.get('shop_id')})")
            else:
                # API未返回门店列表，尝试从大众点评直接获取（兜底）
                print(f"⚠️ API未返回门店列表，尝试从大众点评直接获取...")
                fetched_shops = self._fetch_shop_list_from_dianping()
                if fetched_shops:
                    self.shop_list = [{'shop_id': s['shop_id'], 'shop_name': s['shop_name']} for s in fetched_shops]
                    print(f"✅ 兜底成功，加载 {len(self.shop_list)} 个门店")
                else:
                    raise Exception("未获取到门店列表")

            # 获取团购ID映射
            brands_json = data.get('brands_json', [])
            if brands_json:
                self.product_mapping = brands_json
                print(f"✅ 成功加载 {len(self.product_mapping)} 个团购ID映射")

            # 获取门店商圈信息
            compare_regions = data.get('compareRegions_json', {})
            if compare_regions:
                self.shop_region_info = compare_regions
                print(f"✅ 成功加载 {len(self.shop_region_info)} 个门店商圈信息")

            # 获取店铺ID
            self.shop_id = self.cookies.get('mpmerchant_portal_shopid', '')
            if not self.shop_id and stores_json:
                self.shop_id = stores_json[0].get('shop_id')

            # 检测并补全门店/商圈数据（任一为空则触发）
            self._check_and_complete_stores_regions()

        except Exception as e:
            print(f"❌ 加载账户信息失败: {e}")
            raise

    def _fetch_additional_info_from_api(self):
        """从API补充获取额外信息（门店列表、团购映射、商圈信息）"""
        try:
            headers = {'Content-Type': 'application/json'}
            data = json.dumps({"account": self.account_name})

            session = self._get_session()
            response = session.post(self.platform_api_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            result = response.json()

            if not result or not result.get('success'):
                return

            data = result.get('data', {})
            if not data:
                return

            self.cookie_data = data

            # 获取门店列表
            stores_json = data.get('stores_json', [])
            if stores_json:
                self.shop_list = stores_json
                print(f"✅ 成功加载 {len(self.shop_list)} 个门店")
                for shop in self.shop_list:
                    print(f"   - {shop.get('shop_name')} ({shop.get('shop_id')})")

            # 获取团购ID映射
            brands_json = data.get('brands_json', [])
            if brands_json:
                self.product_mapping = brands_json
                print(f"✅ 成功加载 {len(self.product_mapping)} 个团购ID映射")

            # 获取门店商圈信息
            compare_regions = data.get('compareRegions_json', {})
            if compare_regions:
                self.shop_region_info = compare_regions
                print(f"✅ 成功加载 {len(self.shop_region_info)} 个门店商圈信息")

        except Exception as e:
            print(f"⚠️ 补充获取信息失败: {e}")

    def _fetch_shop_list_from_dianping(self) -> list:
        """
        从大众点评API获取门店列表
        API: POST https://e.dianping.com/gateway/merchant/general/shopinfo
        返回: stores_json 格式的门店列表
        """
        try:
            print(f"\n📡 正在从大众点评获取门店列表...")
            url = "https://e.dianping.com/gateway/merchant/general/shopinfo"
            params = {
                'yodaReady': 'h5',
                'csecplatform': '4',
                'csecversion': '4.1.1'
            }
            payload = {
                "bizType": "pc-shouye",
                "device": "pc",
                "currentTab": "city",
                "shopIds": "0"
            }
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Referer': 'https://e.dianping.com/codejoy/2703/home/index.html',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            session = self._get_session()
            response = session.post(
                url,
                params=params,
                headers=headers,
                cookies=self.cookies,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            if data.get('code') == 200 and data.get('data') and data['data'].get('shopInfoList'):
                shop_list = []
                for shop in data['data']['shopInfoList']:
                    if shop.get('type') != 0:  # 过滤 type=0 的
                        shop_name = shop.get('shopName', '')
                        branch_name = shop.get('branchName') or ''
                        full_name = shop_name + branch_name
                        shop_list.append({
                            'shop_id': str(shop.get('shopId', '')),
                            'shop_name': full_name,
                            'shopName': shop_name,
                            'branchName': branch_name if branch_name else None,
                            'cityId': shop.get('cityId') or None
                        })
                print(f"✅ 成功获取 {len(shop_list)} 个门店")
                return shop_list
            else:
                print(f"⚠️ 门店列表返回数据格式错误: {data}")
                return []

        except Exception as e:
            print(f"⚠️ 获取门店列表失败: {e}")
            return []

    def _fetch_shop_regions_from_dianping(self, shop_list: list) -> dict:
        """
        从大众点评API获取所有门店的商圈数据
        API: GET https://e.dianping.com/gateway/adviser/complexfilter
        返回: compareRegions_json 格式的商圈数据
        """
        if not shop_list:
            return {}

        print(f"\n📡 开始获取 {len(shop_list)} 个门店的商圈数据...")
        compare_regions_data = {}

        for i, shop in enumerate(shop_list):
            shop_id = shop['shop_id']
            shop_name = shop.get('shopName', shop.get('shop_name', ''))

            print(f"   [{i + 1}/{len(shop_list)}] 正在获取门店 {shop_name} (ID: {shop_id}) 的商圈数据...")

            try:
                url = "https://e.dianping.com/gateway/adviser/complexfilter"
                params = {
                    'device': 'pc',
                    'source': '1',
                    'pageType': 'compareRegions',
                    'sign': '',
                    'shopIds': shop_id,
                    'yodaReady': 'h5',
                    'csecplatform': '4',
                    'csecversion': '4.1.1',
                    'mtgsig': self._generate_mtgsig() if hasattr(self, '_generate_mtgsig') else ''
                }
                headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Connection': 'keep-alive',
                    'Referer': 'https://e.dianping.com/codejoy/2703/home/index.html',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                session = self._get_session()
                response = session.get(
                    url,
                    params=params,
                    headers=headers,
                    cookies=self.cookies,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if data.get('success') and data.get('data') and data['data'].get('compareRegions'):
                    regions = data['data']['compareRegions'].get('content', [])

                    # 解析商圈数据
                    regions_dict = {}
                    for region in regions:
                        region_type = region.get('type', '')
                        if region_type == '城市':
                            regions_dict['city'] = {
                                'regionId': region.get('regionId'),
                                'regionName': region.get('regionName')
                            }
                        elif region_type == '行政区':
                            regions_dict['district'] = {
                                'regionId': region.get('regionId'),
                                'regionName': region.get('regionName')
                            }
                        elif region_type == '商圈':
                            regions_dict['business'] = {
                                'regionId': region.get('regionId'),
                                'regionName': region.get('regionName')
                            }

                    compare_regions_data[shop_id] = {
                        'shopName': shop.get('shopName', ''),
                        'branchName': shop.get('branchName'),
                        'cityId': shop.get('cityId'),
                        'regions': regions_dict
                    }
                    print(f"      ✅ 成功: {regions_dict}")
                else:
                    print(f"      ⚠️ 失败: {data.get('msg', '返回数据格式错误')}")

            except Exception as e:
                print(f"      ⚠️ 失败: {e}")

            # 间隔延迟，避免请求过快
            if i < len(shop_list) - 1:
                time.sleep(2)

        print(f"\n✅ 商圈数据获取完成，成功 {len(compare_regions_data)}/{len(shop_list)} 个")
        return compare_regions_data

    def _post_stores_regions_to_api(self, stores_json: list, compare_regions_json: dict) -> bool:
        """
        回传门店和商圈数据到后端API
        API: POST http://8.146.210.145:3000/api/post/platform_accounts
        """
        try:
            print(f"\n📤 正在回传门店和商圈数据...")

            payload = {
                "account": self.account_name,
                "stores_json": json.dumps(stores_json, ensure_ascii=False),
                "compareRegions_json": json.dumps(compare_regions_json, ensure_ascii=False)
            }

            session = self._get_session()
            response = session.post(
                POST_STORES_REGIONS_API_URL,
                headers={'Content-Type': 'application/json'},
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    print(f"✅ 门店和商圈数据回传成功")
                    return True
                else:
                    print(f"⚠️ 回传返回失败: {result}")
                    return False
            else:
                print(f"⚠️ 回传HTTP状态码异常: {response.status_code}")
                return False

        except Exception as e:
            print(f"⚠️ 回传门店和商圈数据失败: {e}")
            return False

    def _check_and_complete_stores_regions(self):
        """
        检测并补全门店/商圈数据
        触发条件: stores_json 为空 OR compareRegions_json 为空
        此任务失败不影响主流程，不记录失败日志
        """
        try:
            # 检查是否需要补全
            stores_empty = not self.shop_list or len(self.shop_list) == 0
            regions_empty = not self.shop_region_info or len(self.shop_region_info) == 0

            if not stores_empty and not regions_empty:
                # 数据完整，无需补全
                return

            print(f"\n🔄 检测到门店/商圈数据不完整，开始自动补全...")
            print(f"   门店数据: {'为空' if stores_empty else f'{len(self.shop_list)} 个'}")
            print(f"   商圈数据: {'为空' if regions_empty else f'{len(self.shop_region_info)} 个'}")

            # 如果门店数据为空，先获取门店列表
            if stores_empty:
                fetched_shops = self._fetch_shop_list_from_dianping()
                if fetched_shops:
                    # 转换为 stores_json 格式
                    self.shop_list = [{'shop_id': s['shop_id'], 'shop_name': s['shop_name']} for s in fetched_shops]
                    stores_empty = False
                else:
                    print(f"⚠️ 无法获取门店列表，跳过补全")
                    return
            else:
                # 门店数据不为空，用于获取商圈
                fetched_shops = []
                for shop in self.shop_list:
                    fetched_shops.append({
                        'shop_id': shop.get('shop_id', ''),
                        'shop_name': shop.get('shop_name', ''),
                        'shopName': shop.get('shop_name', '').split('店')[0] if '店' in shop.get('shop_name', '') else shop.get('shop_name', ''),
                        'branchName': None,
                        'cityId': None
                    })

            # 获取商圈数据
            if regions_empty and fetched_shops:
                fetched_regions = self._fetch_shop_regions_from_dianping(fetched_shops)
                if fetched_regions:
                    self.shop_region_info = fetched_regions

            # 准备回传数据
            stores_to_post = self.shop_list if self.shop_list else []
            regions_to_post = self.shop_region_info if self.shop_region_info else {}

            # 回传数据
            if stores_to_post or regions_to_post:
                self._post_stores_regions_to_api(stores_to_post, regions_to_post)

            print(f"✅ 门店/商圈数据补全完成")

        except Exception as e:
            # 补全失败不影响主流程，静默处理
            print(f"⚠️ 门店/商圈数据补全失败（不影响主任务）: {e}")

    def _install_browser(self):
        """自动安装Playwright浏览器"""
        print("\n⚠️ 检测到Chromium浏览器未安装，正在自动下载...")
        try:
            process = subprocess.Popen(
                [sys.executable, '-m', 'playwright', 'install', 'chromium'],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
            )
            for line in process.stdout:
                print(line.strip())
            process.wait()
            return process.returncode == 0
        except Exception as e:
            print(f"安装失败: {e}")
            return False

    def _convert_cookies_to_playwright_format(self, cookie_dict: dict) -> list:
        """将cookie字典转换为Playwright格式"""
        playwright_cookies = []
        for name, value in cookie_dict.items():
            cookie = {'name': name, 'value': str(value), 'domain': '.dianping.com', 'path': '/'}
            playwright_cookies.append(cookie)
        return playwright_cookies

    def _check_login_status(self, max_retries: int = 2) -> Tuple[bool, str]:
        """检查是否处于登录状态

        Args:
            max_retries: 最大重试次数

        Returns:
            (是否登录, 状态说明)
            - (True, "logged_in") - 已登录
            - (False, "not_logged_in") - 未登录（检测到登录页面）
            - (False, "timeout") - 超时（可能是网络问题）
            - (False, "error") - 其他错误
        """
        for attempt in range(1, max_retries + 1):
            try:
                # 使用 domcontentloaded 而不是 networkidle，避免因持续网络请求导致超时
                self.page.goto(
                    "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html",
                    wait_until='domcontentloaded',
                    timeout=LOGIN_CHECK_TIMEOUT
                )
                # 等待页面跳转完全稳定，避免 evaluate 时上下文被销毁
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception:
                    pass
                time.sleep(2)

                current_url = self.page.url
                if 'login' in current_url.lower():
                    logger.warning("检测到登录页面URL，账户登录状态已失效")
                    return False, "not_logged_in"

                has_content = self.page.evaluate("() => document.body.textContent.length > 100")
                if has_content:
                    return True, "logged_in"
                else:
                    logger.warning("页面内容为空，可能未正确加载")
                    return False, "not_logged_in"

            except Exception as e:
                error_str = str(e).lower()
                is_timeout = 'timeout' in error_str
                is_navigation = 'execution context was destroyed' in error_str or \
                                'most likely because of a navigation' in error_str

                if is_timeout or is_navigation:
                    if attempt < max_retries:
                        delay = calculate_retry_delay(attempt)
                        reason = "超时" if is_timeout else "页面跳转导致上下文销毁"
                        logger.warning(f"登录检测{reason}，第 {attempt}/{max_retries} 次尝试，"
                                       f"{delay:.1f} 秒后重试...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"登录检测失败，已重试 {max_retries} 次: {e}")
                        return False, "timeout"
                else:
                    logger.error(f"登录检测失败: {e}")
                    return False, "error"

        return False, "error"

    def start_browser(self):
        """启动浏览器并登录"""
        # 如果使用外部传入的 page，则不启动新浏览器
        if self.use_external_page:
            self.page = self.external_page
            print("✓ 使用外部传入的浏览器页面（页面驱动模式）")
            return

        if not PLAYWRIGHT_AVAILABLE:
            raise Exception("Playwright未安装，无法启动浏览器")

        print("\n🌐 启动浏览器")
        self.playwright = sync_playwright().start()

        max_retries = 2
        for attempt in range(max_retries):
            try:
                self.browser = self.playwright.chromium.launch(headless=self.headless, proxy=None)
                break
            except Exception as e:
                if "Executable doesn't exist" in str(e) and attempt == 0:
                    if self._install_browser():
                        continue
                    else:
                        raise Exception("浏览器安装失败")
                raise e

        use_saved_state = os.path.exists(self.state_file)

        if use_saved_state:
            print(f"✓ 检测到状态文件: {self.state_file}")
            try:
                self.context = self.browser.new_context(
                    storage_state=self.state_file,
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    proxy=None, bypass_csp=True, ignore_https_errors=True
                )
                self.page = self.context.new_page()
                is_logged_in, status = self._check_login_status()
                if is_logged_in:
                    print(f"✓ 浏览器已启动（使用保存的状态）")
                    return
                else:
                    self.context.close()
                    use_saved_state = False
            except Exception as e:
                print(f"⚠️ 状态文件加载失败: {e}")
                if self.context:
                    self.context.close()
                use_saved_state = False

        if not use_saved_state:
            print("正在使用Cookie登录...")
            playwright_cookies = self._convert_cookies_to_playwright_format(self.cookies)
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                proxy=None, bypass_csp=True, ignore_https_errors=True
            )
            self.context.add_cookies(playwright_cookies)
            self.page = self.context.new_page()

            is_logged_in, status = self._check_login_status()
            if not is_logged_in:
                if status == "not_logged_in":
                    # 确实是登录失效，上报账户失效状态
                    report_auth_invalid(self.account_name)
                    raise AuthInvalidError("Cookie登录失败，账户登录状态已失效")
                elif status == "timeout":
                    # 网络超时，不应该标记为登录失效
                    raise Exception("登录检测超时，请检查网络连接后重试")
                else:
                    # 其他错误
                    raise Exception(f"登录检测失败: {status}")

            self.context.storage_state(path=self.state_file)
            print(f"✓ 浏览器已启动（Cookie登录）")

    def stop_browser(self):
        """关闭浏览器"""
        # 如果使用外部传入的 page，则不关闭浏览器（由外部管理）
        if self.use_external_page:
            print("✓ 外部浏览器页面保持打开（由外部管理）")
            return

        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("✓ 浏览器已关闭")

    def _get_mtgsig(self) -> str:
        """获取mtgsig"""
        if self.mtgsig_from_api:
            return self.mtgsig_from_api
        timestamp = int(time.time() * 1000)
        webdfpid = self.cookies.get('WEBDFPID', '')
        a3 = webdfpid.split('-')[0] if webdfpid and '-' in webdfpid else ''
        mtgsig = {"a1": "1.2", "a2": timestamp, "a3": a3, "a5": "", "a6": "", "a8": "", "a9": "4.1.1,7,139", "a10": "9a", "x0": 4, "d1": ""}
        return json.dumps(mtgsig)

    def _get_headers(self) -> Dict[str, str]:
        """获取通用请求头"""
        return {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://h5.dianping.com',
            'Referer': 'https://h5.dianping.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def _calculate_flow_date_range(self) -> str:
        """计算客流数据的日期范围"""
        now = datetime.now()
        if now.hour < 7:
            end_date = now - timedelta(days=2)
        else:
            end_date = now - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        return f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}"

    def _get_yesterday_date(self) -> str:
        """获取昨天日期"""
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def _parse_rank_value(self, value) -> int:
        """解析排名值"""
        if pd.isna(value) or value == '' or value is None:
            return 0
        value_str = str(value).strip().replace('+', '')
        try:
            return int(float(value_str))
        except:
            return 0

    def _navigate_to_page(self, page_key: str) -> bool:
        """跳转到指定页面（store_stats内部使用）

        Args:
            page_key: 页面键名 (flow_analysis, rival_analysis, trade_analysis, notice_center)

        Returns:
            是否跳转成功
        """
        if not self.page:
            logger.debug("没有可用的浏览器页面，跳过页面跳转")
            return True  # 没有page时仍然尝试执行

        page_url = STORE_STATS_PAGE_URLS.get(page_key)
        page_name = STORE_STATS_PAGE_NAMES.get(page_key, page_key)

        if not page_url:
            logger.warning(f"未找到页面URL: {page_key}")
            return True

        return navigate_to_url(self.page, page_url, page_name)

    def get_force_offline_data(self, target_date: str) -> Dict[str, int]:
        """获取强制下线数据（使用浏览器环境）"""
        print("\n📋 获取强制下线数据（浏览器模式）")
        print(f"   目标日期: {target_date}")
        force_offline_count = {}

        # 跳转到消息中心页面
        self._navigate_to_page("notice_center")
        random_delay(2, 4)

        if not self.page:
            logger.warning("没有可用的浏览器页面，跳过强制下线数据获取")
            return force_offline_count

        try:
            # 检查页面是否被重定向到登录页
            current_url = self.page.url.lower()
            if 'login' in current_url or 'passport' in current_url:
                print(f"🚨 检测到页面被重定向到登录页，Cookie已失效")
                self.login_invalid = True
                self.login_invalid_error = "获取强制下线数据时检测到Cookie失效（重定向到登录页）"
                return force_offline_count

            api_url = "https://e.dianping.com/gateway/msg/MessageDzService/queryPcMessageList"
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()

            script = f"""
            async () => {{
                try {{
                    const response = await fetch('{api_url}?yodaReady=h5&csecplatform=4&csecversion=4.1.1', {{
                        method: 'POST', credentials: 'include',
                        headers: {{'Content-Type': 'application/json'}},
                        body: JSON.stringify({{"messageCategoryCode": 0, "status": null, "subCategoryIdList": null, "important": 1, "pageNo": 1, "pageSize": 100}})
                    }});
                    const status = response.status;
                    const data = await response.json();
                    return {{success: true, status: status, data: data}};
                }} catch(e) {{
                    return {{success: false, error: e.message}};
                }}
            }}
            """

            result = self.page.evaluate(script)
            if not result.get('success'):
                print(f"❌ API调用失败: {result.get('error')}")
                return force_offline_count

            # 检查HTTP状态码是否是登录失效
            http_status = result.get('status', 200)
            if http_status == 401:
                print(f"🚨 检测到HTTP 401，Cookie已失效")
                self.login_invalid = True
                self.login_invalid_error = "获取强制下线数据时检测到Cookie失效（HTTP 401）"
                return force_offline_count

            api_result = result.get('data', {})
            api_status = api_result.get('status')
            api_msg = api_result.get('msg', '')

            # 检查API返回是否是登录失效
            if is_auth_invalid_error(api_status, api_msg):
                print(f"🚨 检测到登录失效: status={api_status}, msg={api_msg}")
                self.login_invalid = True
                self.login_invalid_error = f"获取强制下线数据时检测到Cookie失效（API返回: status={api_status}, msg={api_msg}）"
                return force_offline_count

            if api_status != 0:
                print(f"❌ API返回错误: status={api_status}, msg={api_msg}")
                return force_offline_count

            message_list = api_result.get('messageList', [])
            print(f"   获取到 {len(message_list)} 条消息")

            for msg in message_list:
                title = msg.get('title', '')
                create_time = msg.get('createTime', 0)
                if '强制下线' not in title:
                    continue
                if create_time:
                    msg_date = datetime.fromtimestamp(create_time / 1000).date()
                    if msg_date != target_date_obj:
                        continue
                    shop_id = msg.get('mtShopId') or self.shop_id
                    if shop_id:
                        shop_id_str = str(shop_id)
                        force_offline_count[shop_id_str] = force_offline_count.get(shop_id_str, 0) + 1
                        print(f"   📌 发现强制下线: 门店{shop_id_str}")

            print(f"✅ 强制下线统计完成: {force_offline_count}")
            return force_offline_count
        except Exception as e:
            print(f"❌ 获取强制下线数据失败: {e}")
            return force_offline_count

    def get_flow_data(self) -> Dict[str, int]:
        """获取客流数据（打卡数）"""
        print("\n📋 获取客流数据（打卡数）")

        # 跳转到客流分析页面
        self._navigate_to_page("flow_analysis")
        random_delay(2, 4)

        url = "https://e.dianping.com/gateway/adviser/data"
        date_range = self._calculate_flow_date_range()
        print(f"   日期范围: {date_range}")

        params = {'componentId': 'flowDataSummaryDownloadPCAsync', 'pageType': 'flowAnalysis',
                  'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': self._get_mtgsig()}
        post_data = {'source': '1', 'device': 'pc', 'pageType': 'flowAnalysis', 'shopIds': '0', 'platform': '0', 'date': date_range}
        checkin_data = {}

        try:
            session = self._get_session()
            response = session.post(url, params=params, data=post_data, headers=self._get_headers(), cookies=self.cookies, timeout=60)
            response.raise_for_status()
            result = response.json()

            if result.get('code') != 200:
                print(f"❌ API返回错误")
                return checkin_data

            data_list = result.get('data', [])
            file_url = None
            for item in data_list:
                body = item.get('body', {})
                if body.get('fileUrl'):
                    file_url = body.get('fileUrl')
                    break

            if not file_url:
                print("❌ 未获取到文件URL")
                return checkin_data

            random_delay()  # 反爬虫等待
            print(f"   📥 下载文件...")
            file_response = session.get(file_url, timeout=60)
            df = pd.read_excel(BytesIO(file_response.content))
            print(f"   📊 读取到 {len(df)} 行数据")

            date_col = df.columns[0]
            df[date_col] = pd.to_datetime(df[date_col])
            latest_date = df[date_col].max()
            latest_df = df[df[date_col] == latest_date]

            shop_id_col = df.columns[3]
            checkin_col = df.columns[36]  # AM列 - 第37列 - 打卡数

            for _, row in latest_df.iterrows():
                shop_id = str(int(row[shop_id_col])) if pd.notna(row[shop_id_col]) else None
                checkin_count = int(row[checkin_col]) if pd.notna(row[checkin_col]) else 0
                if shop_id:
                    checkin_data[shop_id] = checkin_count

            print(f"✅ 客流数据获取完成: {len(checkin_data)} 个门店")
            return checkin_data
        except Exception as e:
            print(f"❌ 获取客流数据失败: {e}")
            return checkin_data

    def get_rival_rank_data(self) -> Dict[str, Dict[str, int]]:
        """获取同行排名数据"""
        print("\n📋 获取同行排名数据")

        # 跳转到同行分析页面
        self._navigate_to_page("rival_analysis")
        random_delay(2, 4)

        rank_data = {}

        if not self.shop_region_info:
            print("⚠️ 没有门店商圈信息，跳过排名数据获取")
            for shop in self.shop_list:
                rank_data[shop['shop_id']] = {'order_user_rank': 0, 'verify_amount_rank': 0}
            return rank_data

        for shop in self.shop_list:
            shop_id = shop['shop_id']
            shop_name = shop['shop_name']
            shop_info = self.shop_region_info.get(shop_id, {})
            regions = shop_info.get('regions', {})
            business = regions.get('business', {})
            region_id = business.get('regionId')

            if not region_id:
                rank_data[shop_id] = {'order_user_rank': 0, 'verify_amount_rank': 0}
                continue

            print(f"   🏪 获取门店 {shop_name}({shop_id}) 的排名数据...")
            shop_rank = self._get_rival_rank_by_shop(shop_id, region_id)
            rank_data[shop_id] = shop_rank
            print(f"      下单排名: {shop_rank['order_user_rank']}, 核销排名: {shop_rank['verify_amount_rank']}")
            random_delay()  # 反爬虫等待

        print(f"✅ 同行排名数据获取完成: {len(rank_data)} 个门店")
        return rank_data

    def _get_rival_rank_by_shop(self, shop_id: str, region_id: int) -> Dict[str, int]:
        """获取指定门店的同行排名数据"""
        url = "https://e.dianping.com/gateway/adviser/data"
        params = {
            'device': 'pc', 'source': '1', 'pageType': 'rivalAnalysisV2', 'sign': '', 'dateType': '1',
            'platform': '0', 'shopIds': shop_id, 'regionId': str(region_id), 'regionType': '商圈',
            'componentId': 'shopRankListDownload', 'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': self._get_mtgsig()
        }
        headers = {
            'Accept': 'application/json, text/plain, */*', 'Referer': 'https://e.dianping.com/codejoy/2703/home/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        default_result = {'order_user_rank': 0, 'verify_amount_rank': 0}

        try:
            session = self._get_session()
            response = session.get(url, params=params, headers=headers, cookies=self.cookies, timeout=60)
            response.raise_for_status()
            result = response.json()

            if result.get('code') != 200:
                return default_result

            data_list = result.get('data', [])
            file_url = None
            for item in data_list:
                body = item.get('body', {})
                if body.get('fileUrl'):
                    file_url = body.get('fileUrl')
                    break

            if not file_url:
                return default_result

            file_response = session.get(file_url, timeout=60)
            df = pd.read_excel(BytesIO(file_response.content))

            if len(df) == 0:
                return default_result

            shop_id_col = df.columns[4]
            order_rank_col = df.columns[10]
            verify_rank_col = df.columns[14]

            for _, row in df.iterrows():
                row_shop_id = str(int(row[shop_id_col])) if pd.notna(row[shop_id_col]) else None
                if row_shop_id == shop_id:
                    return {
                        'order_user_rank': self._parse_rank_value(row[order_rank_col]),
                        'verify_amount_rank': self._parse_rank_value(row[verify_rank_col])
                    }
            return default_result
        except Exception as e:
            print(f"      ❌ 获取排名数据失败: {e}")
            return default_result

    def get_trade_data(self) -> Dict[str, int]:
        """获取商品交易数据（广告单）"""
        print("\n📋 获取商品交易数据（广告单）")

        # 跳转到交易分析页面
        self._navigate_to_page("trade_analysis")
        random_delay(2, 4)

        ad_data = {}
        for shop in self.shop_list:
            ad_data[shop['shop_id']] = 0

        if not self.product_mapping:
            print("⚠️ 没有团购ID映射，跳过广告单数据获取")
            return ad_data

        shop_to_brands = {item['shop_id']: item['brands_id'] for item in self.product_mapping}
        url = "https://e.dianping.com/gateway/adviser/data"
        yesterday = self._get_yesterday_date()
        timestamp = int(time.time() * 1000)

        params = {'componentId': 'shopTradeProductRankDownload', 'pageType': 'v5Trade',
                  'yodaReady': 'h5', 'csecplatform': '4', 'csecversion': '4.1.1', 'mtgsig': self._get_mtgsig()}
        post_data = {
            'optionType': 'v5Trade', 'typeIds': '7', 'sortTypeId': '7',
            'prdIds': '1,2,3,4,5,6,11,12,13,14,15,16,17,18,19,20', 'source': '1', 'device': 'pc',
            'date': f'{yesterday},{yesterday}', 'platform': '0', 'pageType': 'v5Trade',
            'shopIds': '', 'excludeShopIds': '', 'cityId': '', 'spuId': '', 'pageNum': '', 'pageSize': '',
            'sign': '', 'fromPage': '', 'storeKey': self.shop_id, 'timeStamp': str(timestamp), 'downloadAllPrdIds': 'true'
        }

        try:
            session = self._get_session()
            response = session.post(url, params=params, data=post_data, headers=self._get_headers(), cookies=self.cookies, timeout=60)
            response.raise_for_status()
            result = response.json()

            if result.get('code') != 200:
                return ad_data

            data_list = result.get('data', [])
            file_url = None
            for item in data_list:
                body = item.get('body', {})
                if body.get('fileUrl'):
                    file_url = body.get('fileUrl')
                    break

            if not file_url:
                return ad_data

            random_delay()  # 反爬虫等待
            print(f"   📥 下载文件...")
            file_response = session.get(file_url, timeout=60)
            df = pd.read_excel(BytesIO(file_response.content))
            print(f"   📊 读取到 {len(df)} 行数据")

            product_id_col = df.columns[2]
            shop_id_col = df.columns[6]
            order_count_col = df.columns[8]

            for _, row in df.iterrows():
                product_id = str(int(row[product_id_col])) if pd.notna(row[product_id_col]) else None
                row_shop_id = str(int(row[shop_id_col])) if pd.notna(row[shop_id_col]) else None
                order_count = int(row[order_count_col]) if pd.notna(row[order_count_col]) else 0

                if row_shop_id and row_shop_id in shop_to_brands:
                    if product_id == shop_to_brands[row_shop_id]:
                        ad_data[row_shop_id] = order_count
                        print(f"   📌 找到: 门店ID={row_shop_id}, 下单人数={order_count}")

            print(f"✅ 商品交易数据获取完成")
            return ad_data
        except Exception as e:
            print(f"❌ 获取商品交易数据失败: {e}")
            return ad_data

    def get_finance_balance(self) -> float:
        """
        获取财务余额（综合推广余额）

        Returns:
            余额金额，失败时返回0
        """
        print("\n💰 获取财务余额数据")

        url = "https://e.dianping.com/adpaccount/finance/account/r/getHomeFinancialDetail"

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Referer': 'https://e.dianping.com/app/peon-promo-finance/html/flow-home.html',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

        try:
            session = self._get_session()
            response = session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=30
            )

            # 检查HTTP状态码是否是登录失效
            if response.status_code == 401:
                print(f"🚨 检测到HTTP 401，Cookie已失效")
                self.login_invalid = True
                self.login_invalid_error = "获取财务余额时检测到Cookie失效（HTTP 401）"
                return 0.0

            response.raise_for_status()
            result = response.json()

            # 检查API返回是否是登录失效
            api_code = result.get('code')
            api_msg = result.get('msg', '')
            if is_auth_invalid_error(api_code, api_msg):
                print(f"🚨 检测到登录失效: code={api_code}, msg={api_msg}")
                self.login_invalid = True
                self.login_invalid_error = f"获取财务余额时检测到Cookie失效（API返回: code={api_code}, msg={api_msg}）"
                return 0.0

            if api_code != 0:
                print(f"❌ API返回错误: {api_msg}")
                return 0.0

            data_list = result.get('data', [])
            if not data_list:
                print("❌ 未获取到财务数据")
                return 0.0

            # 查找"综合推广"的余额
            for item in data_list:
                product_name = item.get('productName', '')
                if product_name == '综合推广':
                    balance = item.get('totalBalance', 0)
                    # 确保balance是数字类型，去除可能的¥符号
                    if isinstance(balance, str):
                        balance = balance.replace('¥', '').replace('￥', '').replace(',', '').strip()
                    balance = float(balance) if balance else 0.0
                    print(f"✅ 财务余额获取成功")
                    print(f"   综合推广余额: {balance:.2f} 元")
                    return balance

            # 如果没找到"综合推广"，返回第一个产品的余额
            if data_list:
                first_item = data_list[0]
                balance = first_item.get('totalBalance', 0)
                # 确保balance是数字类型，去除可能的¥符号
                if isinstance(balance, str):
                    balance = balance.replace('¥', '').replace('￥', '').replace(',', '').strip()
                balance = float(balance) if balance else 0.0
                product_name = first_item.get('productName', '未知')
                print(f"⚠️ 未找到'综合推广'，使用'{product_name}'的余额")
                print(f"   余额: {balance:.2f} 元")
                return balance

            return 0.0

        except Exception as e:
            print(f"❌ 获取财务余额失败: {e}")
            import traceback
            traceback.print_exc()
            return 0.0

    def collect_and_upload(self, target_date: str, upload_api_url: str) -> bool:
        """收集所有数据并上传

        Raises:
            AuthInvalidError: 当检测到登录失效时抛出
        """
        print("\n🚀 开始收集和上传数据")
        print(f"   目标日期: {target_date}")
        print(f"   门店数量: {len(self.shop_list)}")

        try:
            self.start_browser()

            # 获取强制下线数据
            force_offline_data = self.get_force_offline_data(target_date)
            if self.login_invalid:
                raise AuthInvalidError(self.login_invalid_error)
            random_delay()  # 反爬虫等待

            # 获取财务余额数据
            finance_balance = self.get_finance_balance()
            if self.login_invalid:
                raise AuthInvalidError(self.login_invalid_error)
            random_delay()  # 反爬虫等待

            # 获取客流数据
            checkin_data = self.get_flow_data()
            if self.login_invalid:
                raise AuthInvalidError(self.login_invalid_error)
            random_delay()  # 反爬虫等待

            # 获取同行排名数据
            rank_data = self.get_rival_rank_data()
            if self.login_invalid:
                raise AuthInvalidError(self.login_invalid_error)
            random_delay()  # 反爬虫等待

            # 获取广告单数据
            ad_data = self.get_trade_data()
            if self.login_invalid:
                raise AuthInvalidError(self.login_invalid_error)
        finally:
            self.stop_browser()

        # 更新共享签名
        global SHARED_SIGNATURE
        SHARED_SIGNATURE['mtgsig'] = self.mtgsig_from_api
        SHARED_SIGNATURE['cookies'] = self.cookies
        SHARED_SIGNATURE['updated_at'] = datetime.now()
        SHARED_SIGNATURE['shop_list'] = self.shop_list
        print(f"✅ 已更新共享签名，供后续任务使用")

        # 整合数据
        print("\n📊 整合数据")
        upload_data_list = []

        for shop in self.shop_list:
            shop_id = shop['shop_id']
            shop_name = shop['shop_name']
            data = {
                "store_name": shop_name,
                "store_id": int(shop_id),
                "checkin_count": checkin_data.get(shop_id, 0),
                "order_user_rank": rank_data.get(shop_id, {}).get('order_user_rank', 0),
                "verify_amount_rank": rank_data.get(shop_id, {}).get('verify_amount_rank', 0),
                "ad_order_count": ad_data.get(shop_id, 0),
                "ad_balance": finance_balance,
                "is_force_offline": force_offline_data.get(shop_id, 0),
                "date": target_date
            }
            upload_data_list.append(data)
            print(f"   📌 门店: {shop_name} ({shop_id}) - 打卡:{data['checkin_count']}, 下单排名:{data['order_user_rank']}, 广告单:{data['ad_order_count']}, 广告余额:{finance_balance:.2f}元, 强制下线:{data['is_force_offline']}")

        # 上传数据
        print(f"\n📤 上传数据到API: {upload_api_url}")
        session = self._get_session()
        success_count = 0
        fail_count = 0

        for idx, data in enumerate(upload_data_list, 1):
            try:
                response = session.post(upload_api_url, json=data, headers={'Content-Type': 'application/json'}, timeout=API_TIMEOUT)
                if response.status_code in [200, 201]:
                    success_count += 1
                    print(f"   [{idx}/{len(upload_data_list)}] ✅ 成功 - {data['store_name']}")
                else:
                    fail_count += 1
                    print(f"   [{idx}/{len(upload_data_list)}] ❌ 失败 - {data['store_name']}")
                    print(f"      HTTP状态码: {response.status_code}")
                    print(f"      响应内容: {response.text[:200] if response.text else '(空)'}")
            except Exception as e:
                fail_count += 1
                print(f"   [{idx}/{len(upload_data_list)}] ❌ 失败 - {data['store_name']}: {e}")

        print(f"\n📊 上传完成: 成功 {success_count}, 失败 {fail_count}")
        return fail_count == 0


# ============================================================================
# run_store_stats 任务函数
# ============================================================================
def run_store_stats(account_name: str, start_date: str, end_date: str, external_page=None,
                    cookies: Dict = None, mtgsig: str = None, shop_info: List = None,
                    compare_regions: Dict = None, brands_json: List = None) -> Dict[str, Any]:
    """执行store_stats任务 - 门店统计数据采集

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        external_page: 外部传入的 Playwright page 对象（用于页面驱动模式）
        cookies: 外部传入的Cookie（可选，避免重复调用API）
        mtgsig: 外部传入的签名（可选）
        shop_info: 外部传入的门店信息（可选）
        compare_regions: 外部传入的门店商圈信息（可选，用于同行排名）
        brands_json: 外部传入的团购ID映射（可选，用于广告单）
    """
    table_name = "store_stats"
    print(f"\n{'=' * 60}")
    if external_page:
        print(f"🏪 {table_name} (门店统计 - 页面驱动模式)")
    else:
        print(f"🏪 {table_name} (门店统计 - Playwright浏览器模式)")
    print(f"{'=' * 60}")

    result = {"task_name": table_name, "success": False, "record_count": 0, "error_message": "无"}

    # 检查Playwright是否可用（仅在非页面驱动模式下检查）
    if not external_page and not PLAYWRIGHT_AVAILABLE:
        error_msg = "Playwright未安装，store_stats任务跳过"
        print(f"❌ {error_msg}")
        result["error_message"] = error_msg
        log_failure(account_name, 0, table_name, start_date, end_date, error_msg)
        return result

    # 计算目标日期（优先使用TARGET_DATE，否则使用END_DATE）
    if TARGET_DATE:
        target_date = TARGET_DATE
    else:
        target_date = END_DATE

    print(f"   目标日期: {target_date}")
    if external_page:
        print(f"   浏览器模式: 页面驱动模式（复用外部浏览器）")
    else:
        print(f"   浏览器模式: {'无头模式' if HEADLESS else '可视模式'}")

    try:
        disable_proxy()

        # 创建采集器（传递外部数据，避免重复调用API）
        collector = DianpingStoreStats(
            account_name,
            PLATFORM_ACCOUNTS_API_URL,
            headless=HEADLESS,
            disable_proxy=True,
            external_page=external_page,
            cookies=cookies,
            mtgsig=mtgsig,
            shop_info=shop_info,
            compare_regions=compare_regions,
            brands_json=brands_json
        )

        # 执行采集和上传
        success = collector.collect_and_upload(
            target_date=target_date,
            upload_api_url=UPLOAD_APIS[table_name]
        )

        if success:
            result["success"] = True
            result["record_count"] = len(collector.shop_list)
            for shop in collector.shop_list:
                log_success(account_name, int(shop['shop_id']), table_name, target_date, target_date, 1)
        else:
            result["error_message"] = "部分数据上传失败"
            log_failure(account_name, 0, table_name, target_date, target_date, result["error_message"])

    except AuthInvalidError as e:
        # 登录失效异常 - 调用三个接口上报
        error_msg = str(e)
        result["error_message"] = error_msg
        print(f"❌ 登录失效: {e}")
        # 使用统一的登录失效处理函数上报到三个接口
        handle_auth_invalid(account_name, start_date, end_date, table_name, error_msg)

    except Exception as e:
        error_msg = str(e)
        result["error_message"] = error_msg
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        # 上报到 /api/log
        log_failure(account_name, 0, table_name, start_date, end_date, error_msg)

    return result


# ============================================================================
# 页面驱动任务执行类 - 先跳转页面再执行对应任务
# ============================================================================
class PageDrivenTaskExecutor:
    """页面驱动的任务执行器

    工作流程:
    1. 启动 Playwright 浏览器
    2. 按顺序跳转到各个页面
    3. 在每个页面上执行对应的任务
    4. 关闭浏览器

    执行顺序:
    - 报表页面: kewen_daily_report, promotion_daily_report
    - 客流分析页面: store_stats
    - 评价页面(最后): review_detail_dianping, review_detail_meituan,
                      review_summary_dianping, review_summary_meituan
    """

    PAGE_NAME_MAP = {
        "report": "报表页面",
        "flow_analysis": "客流分析页面",
        "review": "评价页面",
    }

    def __init__(self, account_name: str, headless: bool = True, browser_pool: 'BrowserPoolManager' = None):
        """初始化

        Args:
            account_name: 账户名称
            headless: 是否使用无头模式
            browser_pool: 浏览器池管理器（可选，如果提供则使用浏览器池模式）
        """
        self.account_name = account_name
        self.headless = headless
        self.state_file = os.path.join(STATE_DIR, f'dianping_state_{account_name}.json')

        # 浏览器池模式
        self.browser_pool = browser_pool
        self.use_pool = browser_pool is not None
        self._context_wrapper = None  # 浏览器池模式下的Context包装器

        # Playwright相关
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 从API获取的数据
        self.cookies = {}
        self.mtgsig = None
        self.shop_info = {}
        self.templates_id = None
        self.compare_regions = {}  # 门店商圈信息（用于同行排名）
        self.brands_json = []      # 团购ID映射（用于广告单）

        # 执行结果
        self.results = []

        # 登录失效标志 - 一旦检测到失效，停止后续所有任务
        self.login_invalid = False
        self.login_invalid_error = ""

    def _disable_proxy(self):
        """禁用系统代理"""
        proxy_vars = [
            'HTTP_PROXY', 'HTTPS_PROXY', 'FTP_PROXY', 'SOCKS_PROXY',
            'http_proxy', 'https_proxy', 'ftp_proxy', 'socks_proxy',
            'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy'
        ]
        for var in proxy_vars:
            os.environ.pop(var, None)
        os.environ['NO_PROXY'] = '*'
        os.environ['no_proxy'] = '*'
        print("✅ 已禁用系统代理")

    def _load_account_info(self):
        """加载账户信息（优先从浏览器池获取Cookie，没有才调用API）

        优先级：
        1. 浏览器池中已有该账号的Context -> 直接使用其Cookie
        2. 浏览器池没有 -> 从API获取完整信息

        注意：mtgsig, shop_info, templates_id 始终从API获取（浏览器池不存储这些）
        """
        # 检查浏览器池是否已有该账号的Cookie
        pool_cookies = None
        if self.browser_pool and self.browser_pool.has_context(self.account_name):
            try:
                wrapper = self.browser_pool._contexts.get(self.account_name)
                if wrapper and wrapper.cookies:
                    pool_cookies = wrapper.cookies
                    print(f"\n✅ 从浏览器池获取 Cookie（{len(pool_cookies)} 个）")
            except Exception as e:
                print(f"\n⚠️ 从浏览器池获取Cookie失败: {e}，将从API获取")

        if pool_cookies:
            # 使用浏览器池的Cookie
            self.cookies = pool_cookies
            # 但仍需从API获取 mtgsig, shop_info, templates_id, compare_regions, brands_json
            print(f"🔍 正在从API获取账户 [{self.account_name}] 的其他信息...")
            api_data = load_cookies_from_api(self.account_name)
            self.mtgsig = api_data['mtgsig']
            self.shop_info = api_data['shop_info']
            self.templates_id = api_data['templates_id']
            self.compare_regions = api_data.get('compare_regions', {})
            self.brands_json = api_data.get('brands_json', [])
            print(f"✅ 账户信息加载完成（Cookie来自浏览器池）")
        else:
            # 浏览器池没有，从API获取完整信息
            print(f"\n🔍 正在从API获取账户 [{self.account_name}] 的完整信息...")
            api_data = load_cookies_from_api(self.account_name)
            self.cookies = api_data['cookies']
            self.mtgsig = api_data['mtgsig']
            self.shop_info = api_data['shop_info']
            self.templates_id = api_data['templates_id']
            self.compare_regions = api_data.get('compare_regions', {})
            self.brands_json = api_data.get('brands_json', [])
            print(f"✅ 账户信息加载完成（来自API）")

        # 初始化 SHARED_SIGNATURE（供后续任务共享使用）
        global SHARED_SIGNATURE
        SHARED_SIGNATURE['cookies'] = self.cookies
        SHARED_SIGNATURE['mtgsig'] = self.mtgsig
        SHARED_SIGNATURE['shop_list'] = self.shop_info
        SHARED_SIGNATURE['compare_regions'] = self.compare_regions
        SHARED_SIGNATURE['brands_json'] = self.brands_json
        SHARED_SIGNATURE['updated_at'] = datetime.now()

    def _convert_cookies_to_playwright_format(self) -> list:
        """将cookie字典转换为Playwright格式"""
        playwright_cookies = []
        for name, value in self.cookies.items():
            cookie = {
                'name': name,
                'value': str(value),
                'domain': '.dianping.com',
                'path': '/'
            }
            playwright_cookies.append(cookie)
        return playwright_cookies

    def _check_login_status(self, max_retries: int = 2) -> Tuple[bool, str]:
        """检查是否处于登录状态

        Args:
            max_retries: 最大重试次数

        Returns:
            (是否登录, 状态说明)
            - (True, "logged_in") - 已登录
            - (False, "not_logged_in") - 未登录（检测到登录页面）
            - (False, "timeout") - 超时（可能是网络问题）
            - (False, "error") - 其他错误
        """
        for attempt in range(1, max_retries + 1):
            try:
                # 使用 domcontentloaded 而不是 networkidle，避免因持续网络请求导致超时
                self.page.goto(
                    "https://e.dianping.com/app/vg-pc-platform-merchant-selfhelp/newNoticeCenter.html",
                    wait_until='domcontentloaded',
                    timeout=LOGIN_CHECK_TIMEOUT
                )
                # 等待页面跳转完全稳定，避免 evaluate 时上下文被销毁
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception:
                    pass
                time.sleep(2)

                current_url = self.page.url
                if 'login' in current_url.lower():
                    logger.warning("检测到登录页面URL，账户登录状态已失效")
                    return False, "not_logged_in"

                has_content = self.page.evaluate("() => document.body.textContent.length > 100")
                if has_content:
                    return True, "logged_in"
                else:
                    logger.warning("页面内容为空，可能未正确加载")
                    return False, "not_logged_in"

            except Exception as e:
                error_str = str(e).lower()
                is_timeout = 'timeout' in error_str
                is_navigation = 'execution context was destroyed' in error_str or \
                                'most likely because of a navigation' in error_str

                if is_timeout or is_navigation:
                    if attempt < max_retries:
                        delay = calculate_retry_delay(attempt)
                        reason = "超时" if is_timeout else "页面跳转导致上下文销毁"
                        logger.warning(f"登录检测{reason}，第 {attempt}/{max_retries} 次尝试，"
                                       f"{delay:.1f} 秒后重试...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"登录检测失败，已重试 {max_retries} 次: {e}")
                        return False, "timeout"
                else:
                    logger.error(f"登录检测失败: {e}")
                    return False, "error"

        return False, "error"

    def _try_relogin(self) -> bool:
        """尝试使用API Cookie重新登录

        当检测到Cookie失效时调用此方法尝试重新登录。

        流程:
        1. 关闭当前浏览器上下文
        2. 删除旧状态文件
        3. 重新从API获取Cookie
        4. 使用新Cookie创建浏览器上下文
        5. 检查登录状态
        6. 如果成功，保存新状态文件并返回True
        7. 如果失败，返回False

        Returns:
            bool: 重新登录是否成功
        """
        print("\n" + "=" * 60)
        print("🔄 检测到Cookie失效，尝试使用API Cookie重新登录...")
        print("=" * 60)

        try:
            # 3. 重新从API获取Cookie
            print(f"🔍 正在从API重新获取账户 [{self.account_name}] 的Cookie...")
            api_data = load_cookies_from_api(self.account_name)
            self.cookies = api_data['cookies']
            self.mtgsig = api_data['mtgsig']
            print(f"✅ 成功加载 {len(self.cookies)} 个新cookies")

            # ========== 浏览器池模式 ==========
            if self.use_pool and self.browser_pool:
                print("   使用浏览器池模式重新登录...")

                # 1. 从池中移除旧Context
                if self.browser_pool.has_context(self.account_name):
                    self.browser_pool.remove_context(self.account_name)
                    print(f"   ✓ 已移除旧Context")

                self.context = None
                self.page = None
                self._context_wrapper = None

                # 2. 从池中创建新Context
                print(f"   正在创建新Context...")
                self._context_wrapper = self.browser_pool.get_context(self.account_name, self.cookies)
                if not self._context_wrapper:
                    print(f"❌ 无法从浏览器池创建新Context")
                    return False

                self.context = self._context_wrapper.context
                self.page = self._context_wrapper.page

                # 3. 跳转到首页验证登录
                try:
                    self.page.goto("https://e.dianping.com/app/merchant-platform/", timeout=30000)
                    time.sleep(2)
                except Exception as e:
                    print(f"   ⚠️ 首页加载失败: {e}")

                # 4. 检查登录状态
                is_logged_in, status = self._check_login_status()

                if is_logged_in:
                    self._context_wrapper.update_last_used()
                    print(f"✅ 重新登录成功！（浏览器池模式）")
                    self.login_invalid = False
                    self.login_invalid_error = ""
                    return True
                else:
                    print(f"❌ 重新登录失败: {status}")
                    # 移除失败的Context
                    self.browser_pool.remove_context(self.account_name)
                    return False

            # ========== 传统模式 ==========
            # 1. 关闭当前浏览器上下文（保留browser实例）
            if self.context:
                try:
                    self.context.close()
                except:
                    pass
                self.context = None
                self.page = None

            # 2. 删除旧状态文件
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
                print(f"✓ 已删除旧状态文件: {self.state_file}")

            # 4. 使用新Cookie创建浏览器上下文
            print("正在使用新Cookie登录...")
            playwright_cookies = self._convert_cookies_to_playwright_format()
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                proxy=None,
                bypass_csp=True,
                ignore_https_errors=True
            )
            self.context.add_cookies(playwright_cookies)
            self.page = self.context.new_page()

            # 5. 检查登录状态
            is_logged_in, status = self._check_login_status()

            if is_logged_in:
                # 6. 登录成功，保存新状态文件
                self.context.storage_state(path=self.state_file)
                print(f"✅ 重新登录成功！已保存新状态文件")
                # 重置登录失效标志
                self.login_invalid = False
                self.login_invalid_error = ""

                # 7. 更新 SHARED_SIGNATURE（供后续任务使用新的Cookie/签名）
                global SHARED_SIGNATURE
                SHARED_SIGNATURE['cookies'] = self.cookies
                SHARED_SIGNATURE['mtgsig'] = self.mtgsig
                SHARED_SIGNATURE['updated_at'] = datetime.now()
                print(f"✅ 已更新共享签名（重新登录后）")

                return True
            else:
                # 7. 登录失败
                print(f"❌ 重新登录失败: {status}")
                return False

        except Exception as e:
            print(f"❌ 重新登录过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _install_browser(self):
        """自动安装Playwright浏览器"""
        print("\n⚠️ 检测到Chromium浏览器未安装，正在自动下载...")
        try:
            process = subprocess.Popen(
                [sys.executable, '-m', 'playwright', 'install', 'chromium'],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
            )
            for line in process.stdout:
                print(line.strip())
            process.wait()
            return process.returncode == 0
        except Exception as e:
            print(f"安装失败: {e}")
            return False

    def start_browser(self):
        """启动浏览器并登录

        浏览器池模式：从池中获取Context，不创建新浏览器
        传统模式：创建新的浏览器实例
        """
        if not PLAYWRIGHT_AVAILABLE:
            raise Exception("Playwright未安装，无法启动浏览器")

        # ========== 浏览器池模式 ==========
        if self.use_pool and self.browser_pool:
            print("\n🌐 使用浏览器池模式")

            # 检查池中是否已有该账号的Context
            if self.browser_pool.has_context(self.account_name):
                print(f"   ✓ 发现账号 {self.account_name} 的现有Context")
                self._context_wrapper = self.browser_pool._contexts[self.account_name]
                self.context = self._context_wrapper.context
                self.page = self._context_wrapper.page

                # 检查登录状态
                is_logged_in, status = self._check_login_status()
                if is_logged_in:
                    self._context_wrapper.update_last_used()
                    print(f"   ✓ 浏览器已就绪（使用池中Context）")
                    return
                else:
                    print(f"   ⚠️ 池中Context登录已失效，需要刷新Cookie")
                    # 移除失效的Context
                    self.browser_pool.remove_context(self.account_name)

            # 从池中创建新Context
            print(f"   正在为账号 {self.account_name} 创建新Context...")
            self._context_wrapper = self.browser_pool.get_context(self.account_name, self.cookies)
            self.context = self._context_wrapper.context
            self.page = self._context_wrapper.page

            # 跳转到首页验证登录
            try:
                self.page.goto("https://e.dianping.com/app/merchant-platform/", timeout=30000)
                time.sleep(2)
            except Exception as e:
                print(f"   ⚠️ 首页加载失败: {e}")

            # 检查登录状态
            is_logged_in, status = self._check_login_status()
            if not is_logged_in:
                if status == "not_logged_in":
                    report_auth_invalid(self.account_name)
                    self.browser_pool.remove_context(self.account_name)
                    raise AuthInvalidError("Cookie登录失败，账户登录状态已失效")
                elif status == "timeout":
                    raise Exception("登录检测超时，请检查网络连接后重试")
                else:
                    raise Exception(f"登录检测失败: {status}")

            self._context_wrapper.update_last_used()
            print(f"   ✓ 浏览器已就绪（浏览器池模式，已跳转首页）")
            return

        # ========== 传统模式 ==========
        print("\n🌐 启动浏览器")
        self.playwright = sync_playwright().start()

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # 使用WebKit（如果浏览器池模式启用）或Chromium
                if USE_BROWSER_POOL and BROWSER_POOL_AVAILABLE:
                    self.browser = self.playwright.webkit.launch(
                        headless=self.headless,
                        proxy=None
                    )
                    print("   使用 WebKit 引擎")
                else:
                    self.browser = self.playwright.chromium.launch(
                        headless=self.headless,
                        proxy=None
                    )
                break
            except Exception as e:
                if "Executable doesn't exist" in str(e) and attempt == 0:
                    if self._install_browser():
                        continue
                    else:
                        raise Exception("浏览器安装失败")
                raise e

        use_saved_state = os.path.exists(self.state_file)

        if use_saved_state:
            print(f"✓ 检测到状态文件: {self.state_file}")
            try:
                self.context = self.browser.new_context(
                    storage_state=self.state_file,
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    proxy=None,
                    bypass_csp=True,
                    ignore_https_errors=True
                )
                self.page = self.context.new_page()
                is_logged_in, status = self._check_login_status()
                if is_logged_in:
                    print(f"✓ 浏览器已启动（使用保存的状态）")
                    return
                else:
                    self.context.close()
                    use_saved_state = False
            except Exception as e:
                print(f"⚠️ 状态文件加载失败: {e}")
                if self.context:
                    self.context.close()
                use_saved_state = False

        if not use_saved_state:
            print("正在使用Cookie登录...")
            playwright_cookies = self._convert_cookies_to_playwright_format()
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                proxy=None,
                bypass_csp=True,
                ignore_https_errors=True
            )
            self.context.add_cookies(playwright_cookies)
            self.page = self.context.new_page()

            is_logged_in, status = self._check_login_status()
            if not is_logged_in:
                if status == "not_logged_in":
                    # 确实是登录失效，上报账户失效状态
                    report_auth_invalid(self.account_name)
                    raise AuthInvalidError("Cookie登录失败，账户登录状态已失效")
                elif status == "timeout":
                    # 网络超时，不应该标记为登录失效
                    raise Exception("登录检测超时，请检查网络连接后重试")
                else:
                    # 其他错误
                    raise Exception(f"登录检测失败: {status}")

            self.context.storage_state(path=self.state_file)
            print(f"✓ 浏览器已启动（Cookie登录）")

    def stop_browser(self):
        """关闭浏览器

        浏览器池模式：不关闭Context，保留在池中供保活使用
        传统模式：关闭浏览器
        """
        # 浏览器池模式：不关闭，保留Context供后续使用
        if self.use_pool and self._context_wrapper:
            print("✓ 任务完成（浏览器池模式，Context保留在池中）")

            # 上传最新Cookie到队列
            if BROWSER_POOL_AVAILABLE and not self.login_invalid:
                try:
                    cookies = self._context_wrapper.get_cookies()
                    if cookies:
                        cookie_upload_queue.put(self.account_name, cookies)
                        print("   ✓ Cookie已加入上传队列")
                except Exception as e:
                    print(f"   ⚠️ Cookie上传失败: {e}")
            return

        # 传统模式：关闭浏览器
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("✓ 浏览器已关闭")

    def navigate_to_page(self, page_key: str, max_retries: int = 2):
        """跳转到指定页面

        Args:
            page_key: 页面键名 (report, flow_analysis, review)
            max_retries: 最大重试次数
        """
        page_url = PAGE_URLS.get(page_key)
        page_name = self.PAGE_NAME_MAP.get(page_key, page_key)

        if not page_url:
            logger.warning(f"未找到页面URL: {page_key}")
            return False

        print(f"\n{'=' * 60}")
        print(f"🔗 正在跳转到 {page_name}...")
        print(f"   URL: {page_url[:80]}...")
        print(f"{'=' * 60}")

        for attempt in range(1, max_retries + 1):
            try:
                # 使用 load 而不是 networkidle，避免因持续网络请求导致超时
                self.page.goto(page_url, wait_until='load', timeout=BROWSER_PAGE_TIMEOUT)
                time.sleep(3)  # 等待页面稳定

                # 检查是否被重定向到登录页面
                current_url = self.page.url.lower()
                if 'login' in current_url or 'passport' in current_url:
                    print(f"🚨 检测到页面被重定向到登录页，Cookie已失效")
                    self.login_invalid = True
                    self.login_invalid_error = "页面跳转时检测到Cookie失效（重定向到登录页）"
                    return False

                # 检查页面内容是否包含登录失效提示
                try:
                    page_content = self.page.content()
                    if '请重新登录' in page_content or '登录状态失效' in page_content or '未登录' in page_content:
                        print(f"🚨 检测到页面包含登录失效提示，Cookie已失效")
                        self.login_invalid = True
                        self.login_invalid_error = "页面跳转时检测到Cookie失效（页面提示未登录）"
                        return False
                except:
                    pass  # 获取页面内容失败，继续执行

                # 处理门店权限问题（自动切换到全部门店）
                handle_shop_permission_issue(self.page)

                print(f"✅ 已跳转到 {page_name}")
                return True
            except Exception as e:
                error_str = str(e).lower()
                is_timeout = 'timeout' in error_str

                if is_timeout and attempt < max_retries:
                    delay = calculate_retry_delay(attempt)
                    logger.warning(f"页面加载超时，第 {attempt}/{max_retries} 次尝试，"
                                   f"{delay:.1f} 秒后重试...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"跳转失败: {e}")
                    return False

        return False

    def execute_page_tasks(self, page_key: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """执行指定页面的所有任务

        Args:
            page_key: 页面键名
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            任务执行结果列表
        """
        tasks = PAGE_TASKS.get(page_key, [])
        page_name = self.PAGE_NAME_MAP.get(page_key, page_key)
        results = []

        # 过滤掉已禁用的任务
        enabled_tasks = [t for t in tasks if not TASK_DISABLED_FLAGS.get(t, False)]
        disabled_tasks = [t for t in tasks if TASK_DISABLED_FLAGS.get(t, False)]

        print(f"\n📋 {page_name} 需要执行 {len(enabled_tasks)} 个任务: {', '.join(enabled_tasks)}")
        if disabled_tasks:
            print(f"   ⏭️ 已禁用任务: {', '.join(disabled_tasks)}")

        # 为禁用的任务添加成功状态
        for task_name in disabled_tasks:
            logger.info(f"任务 {task_name} 已禁用，跳过执行（返回成功）")
            results.append({
                "task_name": task_name,
                "success": True,
                "record_count": 0,
                "error_message": "任务已禁用(默认成功)"
            })

        for task_name in enabled_tasks:
            print(f"\n{'─' * 50}")
            print(f"▶ 开始执行任务: {task_name}")
            print(f"{'─' * 50}")

            # 获取最新的共享数据（可能被前一个任务更新，如 store_stats 更新签名）
            current_cookies = SHARED_SIGNATURE.get('cookies') or self.cookies
            current_mtgsig = SHARED_SIGNATURE.get('mtgsig') or self.mtgsig
            current_shop_info = SHARED_SIGNATURE.get('shop_list') or self.shop_info
            current_compare_regions = SHARED_SIGNATURE.get('compare_regions') or self.compare_regions
            current_brands_json = SHARED_SIGNATURE.get('brands_json') or self.brands_json

            task_func = TASK_MAP.get(task_name)
            if task_func:
                # 所有任务都传递共享的 cookies, mtgsig, shop_info（避免重复调用API）
                if task_name == 'store_stats':
                    result = task_func(
                        self.account_name, start_date, end_date,
                        external_page=self.page,
                        cookies=current_cookies,
                        mtgsig=current_mtgsig,
                        shop_info=current_shop_info,
                        compare_regions=current_compare_regions,
                        brands_json=current_brands_json
                    )
                elif task_name == 'kewen_daily_report':
                    result = task_func(
                        self.account_name, start_date, end_date,
                        templates_id=self.templates_id,
                        cookies=current_cookies,
                        mtgsig=current_mtgsig
                    )
                elif task_name == 'promotion_daily_report':
                    result = task_func(
                        self.account_name, start_date, end_date,
                        cookies=current_cookies,
                        mtgsig=current_mtgsig
                    )
                elif task_name in ('review_detail_dianping', 'review_detail_meituan',
                                   'review_summary_dianping', 'review_summary_meituan'):
                    result = task_func(
                        self.account_name, start_date, end_date,
                        cookies=current_cookies,
                        mtgsig=current_mtgsig,
                        shop_info=current_shop_info
                    )
                else:
                    # 其他任务（兼容）
                    result = task_func(self.account_name, start_date, end_date)
                results.append(result)

                if result.get('success'):
                    print(f"✅ 任务 {task_name} 执行成功")
                else:
                    print(f"❌ 任务 {task_name} 执行失败: {result.get('error_message')}")
            else:
                print(f"⚠️ 未找到任务函数: {task_name}")
                results.append({
                    "task_name": task_name,
                    "success": False,
                    "record_count": 0,
                    "error_message": f"未找到任务函数"
                })

            # 任务间随机延迟
            random_delay(2, 4)

        return results

    def run_all_tasks(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """按页面顺序执行所有任务

        执行顺序:
        1. 客流分析页面: store_stats (先执行，更新签名)
        2. 报表页面: kewen_daily_report, promotion_daily_report
        3. 评价页面(最后): 4个评价相关任务

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            所有任务的执行结果列表
        """
        print("\n" + "=" * 80)
        print("🚀 页面驱动任务执行模式")
        print("=" * 80)
        print(f"执行顺序:")
        for i, page_key in enumerate(PAGE_ORDER, 1):
            page_name = self.PAGE_NAME_MAP.get(page_key)
            tasks = PAGE_TASKS.get(page_key, [])
            print(f"   {i}. {page_name}: {', '.join(tasks)}")
        print("=" * 80)

        all_results = []

        try:
            self._disable_proxy()
            self._load_account_info()
            self.start_browser()

            # ========== 检查并自动获取/创建 templates_id ==========
            if not self.templates_id or self.templates_id == 0 or str(self.templates_id) == '0':
                print("\n" + "=" * 60)
                print("⚠️ templates_id 未获取到，开始自动获取/创建...")
                print("=" * 60)

                # 先跳转到报表中心页面
                print(f"\n📍 跳转到报表中心页面...")
                try:
                    self.page.goto(REPORT_CENTER_URL, wait_until='networkidle', timeout=BROWSER_PAGE_TIMEOUT)
                    random_delay(2, 3)
                    print("   ✅ 页面加载完成")

                    # 调用 ensure_template_id 获取或创建模板ID
                    new_templates_id = ensure_template_id(self.account_name, self.cookies, self.mtgsig)

                    if new_templates_id:
                        self.templates_id = new_templates_id
                        print(f"\n✅ 已成功获取 templates_id: {self.templates_id}")
                    else:
                        print("\n❌ 无法获取 templates_id，kewen_daily_report 任务可能失败")
                except Exception as e:
                    print(f"❌ 获取/创建模板ID失败: {e}")
                    import traceback
                    traceback.print_exc()

            for page_key in PAGE_ORDER:
                # 检查是否已经检测到登录失效
                if self.login_invalid:
                    print(f"\n🚨 已检测到Cookie失效，尝试重新登录...")
                    # 尝试重新登录
                    if self._try_relogin():
                        print(f"✅ 重新登录成功，继续执行任务")
                        # 重新登录成功，继续执行当前页面
                    else:
                        print(f"❌ 重新登录失败，停止后续所有任务")
                        # 重新登录失败，上报并退出
                        report_auth_invalid(self.account_name)
                        # 将剩余任务标记为失败
                        for remaining_page_key in PAGE_ORDER[PAGE_ORDER.index(page_key):]:
                            for task_name in PAGE_TASKS.get(remaining_page_key, []):
                                all_results.append({
                                    "task_name": task_name,
                                    "success": False,
                                    "record_count": 0,
                                    "error_message": f"Cookie失效且重新登录失败: {self.login_invalid_error}"
                                })
                        break

                page_name = self.PAGE_NAME_MAP.get(page_key)

                # 跳转到页面
                if not self.navigate_to_page(page_key):
                    # 检查是否是因为登录失效导致的跳转失败
                    if self.login_invalid:
                        print(f"🚨 页面跳转失败，检测到Cookie已失效，尝试重新登录...")
                        # 尝试重新登录
                        if self._try_relogin():
                            print(f"✅ 重新登录成功，重新跳转页面")
                            # 重新登录成功，重新尝试跳转
                            if not self.navigate_to_page(page_key):
                                # 重新跳转仍然失败
                                print(f"⚠️ 重新登录后页面跳转仍然失败，跳过 {page_name} 的任务")
                                for task_name in PAGE_TASKS.get(page_key, []):
                                    all_results.append({
                                        "task_name": task_name,
                                        "success": False,
                                        "record_count": 0,
                                        "error_message": f"重新登录后页面跳转失败"
                                    })
                                continue
                            # 跳转成功，继续执行任务（不需要break或continue）
                        else:
                            print(f"❌ 重新登录失败，停止后续所有任务")
                            # 重新登录失败，上报并退出
                            handle_auth_invalid(self.account_name, start_date, end_date,
                                              "page_navigate", self.login_invalid_error)
                            # 将所有任务标记为失败
                            for remaining_page_key in PAGE_ORDER[PAGE_ORDER.index(page_key):]:
                                for task_name in PAGE_TASKS.get(remaining_page_key, []):
                                    all_results.append({
                                        "task_name": task_name,
                                        "success": False,
                                        "record_count": 0,
                                        "error_message": f"Cookie失效且重新登录失败: {self.login_invalid_error}"
                                    })
                            break
                    else:
                        # 普通跳转失败，跳过该页面的任务
                        print(f"⚠️ 跳过 {page_name} 的任务")
                        for task_name in PAGE_TASKS.get(page_key, []):
                            all_results.append({
                                "task_name": task_name,
                                "success": False,
                                "record_count": 0,
                                "error_message": f"页面跳转失败"
                            })
                        continue

                # 执行该页面的任务
                results = self.execute_page_tasks(page_key, start_date, end_date)
                all_results.extend(results)

                # 检查任务执行结果中是否有登录失效
                for result in results:
                    error_msg = result.get('error_message', '')
                    if '登录失效' in error_msg or 'Cookie失效' in error_msg:
                        self.login_invalid = True
                        self.login_invalid_error = error_msg
                        break

                # 如果检测到登录失效，尝试重新登录
                if self.login_invalid:
                    print(f"\n🚨 任务执行中检测到Cookie失效，尝试重新登录...")
                    # 尝试重新登录
                    if self._try_relogin():
                        print(f"✅ 重新登录成功，继续执行后续任务")
                        # 重新登录成功，继续执行下一个页面的任务
                    else:
                        print(f"❌ 重新登录失败，停止后续任务")
                        # 重新登录失败，将在下一次循环的顶部检查中处理
                        continue

                # 页面间随机延迟
                random_delay(3, 5)

        except Exception as e:
            error_msg = str(e)
            print(f"❌ 执行过程中发生错误: {error_msg}")
            import traceback
            traceback.print_exc()

            # 如果是登录失败，同时上报日志到两个接口
            if "登录失败" in error_msg or "Cookie登录失败" in error_msg or "登录失效" in error_msg:
                print(f"\n📤 上报登录失败日志...")
                handle_auth_invalid(self.account_name, start_date, end_date, "task_execution", error_msg)
        finally:
            self.stop_browser()

        return all_results


def run_page_driven_tasks(account_name: str, start_date: str, end_date: str, headless: bool = True, browser_pool: 'BrowserPoolManager' = None) -> List[Dict[str, Any]]:
    """执行页面驱动的任务

    这是页面驱动模式的入口函数，会按照以下顺序执行:
    1. 跳转客流分析页面 → 执行 store_stats (先执行，更新签名)
    2. 跳转报表页面 → 执行 kewen_daily_report, promotion_daily_report
    3. 跳转评价页面 → 执行 4个评价任务 (最后执行)

    Args:
        account_name: 账户名称
        start_date: 开始日期
        end_date: 结束日期
        headless: 是否使用无头模式
        browser_pool: 浏览器池管理器（可选，如果提供则使用浏览器池模式）

    Returns:
        所有任务的执行结果列表
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("❌ Playwright未安装，无法使用页面驱动模式")
        return []

    executor = PageDrivenTaskExecutor(account_name, headless=headless, browser_pool=browser_pool)
    return executor.run_all_tasks(start_date, end_date)


# ============================================================================
# 任务映射和主函数
# ============================================================================
TASK_MAP = {
    'store_stats': run_store_stats,
    'kewen_daily_report': run_kewen_daily_report,
    'promotion_daily_report': run_promotion_daily_report,
    'review_detail_dianping': run_review_detail_dianping,
    'review_detail_meituan': run_review_detail_meituan,
    'review_summary_dianping': run_review_summary_dianping,
    'review_summary_meituan': run_review_summary_meituan,
}


# ============================================================================
# 任务调度API函数
# ============================================================================
def create_task_schedule() -> bool:
    """生成任务调度

    调用 post_task_schedule API，自动计算日期：
    - task_date: 当日日期
    - data_start_date: 前天日期
    - data_end_date: 昨天日期

    Returns:
        bool: 是否成功
    """
    today = datetime.now()
    task_date = today.strftime("%Y-%m-%d")
    data_start_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    data_end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    headers = {'Content-Type': 'application/json'}
    json_param = {
        "task_date": task_date,
        "data_start_date": data_start_date,
        "data_end_date": data_end_date
    }
    proxies = {'http': None, 'https': None}

    print(f"\n{'=' * 80}")
    print("📅 生成任务调度")
    print(f"{'=' * 80}")
    print(f"   URL: {TASK_SCHEDULE_API_URL}")
    print(f"   task_date (当日): {task_date}")
    print(f"   data_start_date (前天): {data_start_date}")
    print(f"   data_end_date (昨天): {data_end_date}")

    try:
        response = requests.post(
            TASK_SCHEDULE_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print("   ✅ 任务调度生成成功")
            return True
        else:
            print(f"   ❌ 任务调度生成失败: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ 任务调度生成异常: {e}")
        return False


def fetch_task(server_ip: str = None) -> Optional[Dict[str, Any]]:
    """获取一条待执行任务

    调用 get_task API 获取任务信息

    Args:
        server_ip: 服务器IP地址（用于任务分配）

    Returns:
        dict: 任务信息，包含 id, account_id, task_type, data_start_date, data_end_date 等
        None: 如果没有任务或获取失败
    """
    headers = {'Content-Type': 'application/json'}
    proxies = {'http': None, 'https': None}

    print(f"\n{'=' * 80}")
    print("📋 获取待执行任务")
    print(f"{'=' * 80}")
    print(f"   URL: {GET_TASK_API_URL}")

    # 构建请求参数
    json_param = {}
    if server_ip:
        json_param["server"] = server_ip
        print(f"   Server IP: {server_ip}")

    try:
        response = requests.post(
            GET_TASK_API_URL,
            json=json_param,
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            result = response.json()
            # API返回格式: {"success":true,"data":{...}}
            task_data = result.get('data') if result.get('success') else None
            if task_data:
                print(f"   ✅ 获取任务成功")
                print(f"   任务ID: {task_data.get('id')}")
                print(f"   账户: {task_data.get('account_id')}")
                print(f"   任务类型: {task_data.get('task_type')}")
                print(f"   数据日期: {task_data.get('data_start_date')} 至 {task_data.get('data_end_date')}")
                return task_data
            else:
                print("   ⚠️ 没有待执行的任务")
                return None
        else:
            print(f"   ❌ 获取任务失败: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"   ❌ 获取任务异常: {e}")
        return None


def report_task_callback(task_id: int, status: int, error_message: str, retry_add: int) -> bool:
    """上报任务完成状态

    Args:
        task_id: 任务ID (从fetch_task获取)
        status: 状态 (2=全部完成, 3=有任务失败)
        error_message: 错误信息 (status=3时需要填写)
        retry_add: 重试次数增加 (status=2时为0, status=3时为1)

    Returns:
        bool: 是否上报成功
    """
    headers = {'Content-Type': 'application/json'}
    json_param = {
        "id": task_id,
        "status": status,
        "error_message": error_message,
        "retry_add": retry_add
    }
    proxies = {'http': None, 'https': None}

    print(f"\n{'=' * 80}")
    print("📤 上报任务完成状态")
    print(f"{'=' * 80}")
    print(f"   URL: {TASK_CALLBACK_API_URL}")
    print(f"   任务ID: {task_id}")
    print(f"   状态: {status} ({'全部完成' if status == 2 else '有任务失败'})")
    if error_message:
        print(f"   错误信息: {error_message[:200]}...")
    print(f"   retry_add: {retry_add}")

    try:
        response = requests.post(
            TASK_CALLBACK_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print("   ✅ 任务状态上报成功")
            return True
        else:
            print(f"   ❌ 任务状态上报失败: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ 任务状态上报异常: {e}")
        return False


def reset_task_schedule(task_id: int) -> bool:
    """重置任务状态（资源不足时归还任务）

    当资源状态为critical无法执行任务时，调用此接口将任务状态从1重置为0，
    让任务重新进入待执行队列，避免任务丢失。

    Args:
        task_id: 任务ID

    Returns:
        bool: 是否重置成功
    """
    headers = {'Content-Type': 'application/json'}
    json_param = {"id": task_id}
    proxies = {'http': None, 'https': None}

    print(f"   🔄 重置任务 {task_id}（资源不足，归还任务）...")

    try:
        response = requests.post(
            TASK_RESET_API_URL,
            headers=headers,
            data=json.dumps(json_param),
            proxies=proxies,
            timeout=30
        )

        if response.status_code == 200:
            print(f"   ✅ 任务 {task_id} 已重置，将在资源恢复后重新执行")
            return True
        else:
            print(f"   ❌ 任务重置失败: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ 任务重置异常: {e}")
        return False


def reschedule_failed_tasks() -> bool:
    """重新调度失败的任务

    调用 reschedule-failed API，让失败的任务重新进入调度队列

    Returns:
        bool: 是否成功
    """
    headers = {'Content-Type': 'application/json'}
    proxies = {'http': None, 'https': None}

    print(f"\n{'=' * 80}")
    print("🔄 重新调度失败任务")
    print(f"{'=' * 80}")
    print(f"   URL: {RESCHEDULE_FAILED_API_URL}")

    try:
        response = requests.post(
            RESCHEDULE_FAILED_API_URL,
            json={},
            proxies=proxies,
            timeout=30
        )
        print(f"   HTTP状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500] if response.text else '(空)'}")

        if response.status_code == 200:
            print("   ✅ 失败任务重新调度成功")
            return True
        else:
            print(f"   ❌ 失败任务重新调度失败: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ 失败任务重新调度异常: {e}")
        return False


def validate_date(date_str: str) -> bool:
    """验证日期格式"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def print_summary(results: List[Dict[str, Any]]):
    """打印执行摘要"""
    print("\n" + "=" * 80)
    print("执行摘要")
    print("=" * 80)

    success_count = sum(1 for r in results if r.get('success'))
    print(f"总任务数: {len(results)}, 成功: {success_count}, 失败: {len(results) - success_count}")
    print("-" * 40)

    for result in results:
        status = "✅" if result.get('success') else "❌"
        print(f"{status} {result.get('task_name')}: 记录数={result.get('record_count', 0)}, 错误={result.get('error_message', '无')}")

    print("=" * 80)


def execute_single_task(task_info: Dict[str, Any], browser_pool: 'BrowserPoolManager' = None) -> bool:
    """执行单个任务

    Args:
        task_info: 从API获取的任务信息
        browser_pool: 浏览器池管理器（可选，如果提供则使用浏览器池模式）

    Returns:
        bool: 任务是否执行成功
    """
    global ACCOUNT_NAME, START_DATE, END_DATE, TASK, TARGET_DATE

    task_id = task_info.get("id")

    # 填充配置变量
    ACCOUNT_NAME = task_info.get("account_id", "")
    START_DATE = task_info.get("data_start_date", "")
    END_DATE = task_info.get("data_end_date", "")
    TASK = task_info.get("task_type", "all")
    TARGET_DATE = ""

    account_name = ACCOUNT_NAME
    start_date = START_DATE
    end_date = END_DATE
    task = TASK

    # 评价详细任务使用近7天日期（昨天往前推6天）
    if task in ['review_detail_dianping', 'review_detail_meituan']:
        today = datetime.now()
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")  # 昨天
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")  # 昨天往前6天
        START_DATE = start_date
        END_DATE = end_date

    print(f"\n{'=' * 80}")
    print("📌 任务配置")
    print(f"{'=' * 80}")
    print(f"   任务ID: {task_id}")
    print(f"   账户名称: {account_name}")
    print(f"   日期范围: {start_date} 至 {end_date}")
    print(f"   任务类型: {task}")

    # 验证参数
    if not account_name:
        error_msg = "账户名称为空"
        print(f"❌ {error_msg}")
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    if not validate_date(start_date) or not validate_date(end_date):
        error_msg = "日期格式错误，应为 YYYY-MM-DD"
        print(f"❌ {error_msg}")
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    if start > end:
        error_msg = "开始日期不能大于结束日期"
        print(f"❌ {error_msg}")
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    valid_tasks = ['all'] + list(TASK_MAP.keys())
    if task not in valid_tasks:
        error_msg = f"无效的任务名称: {task}，可选值: {', '.join(valid_tasks)}"
        print(f"❌ {error_msg}")
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    # ========== 获取平台账户信息并检查 templates_id ==========
    print(f"\n{'=' * 80}")
    print("🔍 检查平台账户配置")
    print(f"{'=' * 80}")

    platform_account = get_platform_account(account_name)

    if not platform_account.get('success'):
        error_msg = f"获取平台账户信息失败: {platform_account.get('error_message', '未知错误')}"
        print(f"❌ {error_msg}")
        # 同时上报到两个日志接口
        log_failure(account_name, 0, "platform_account_check", start_date, end_date, error_msg)
        upload_task_status_batch(account_name, start_date, end_date, [{
            'task_name': 'platform_account_check',
            'success': False,
            'record_count': 0,
            'error_message': error_msg
        }])
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    # 检查 auth_status 是否为无效状态
    auth_status = platform_account.get('auth_status')
    if auth_status == 'invalid':
        error_msg = f"账户Cookie已失效(auth_status=invalid)，请重新登录"
        print(f"❌ {error_msg}")
        # 同时上报到两个日志接口
        log_failure(account_name, 0, "auth_status_check", start_date, end_date, error_msg)
        upload_task_status_batch(account_name, start_date, end_date, [{
            'task_name': 'auth_status_check',
            'success': False,
            'record_count': 0,
            'error_message': error_msg
        }])
        # retry_add=0 避免无效重试（Cookie未更新时重试没有意义）
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=0)
        return False

    templates_id = platform_account.get('templates_id')
    if templates_id == 0 or templates_id is None:
        print("\n" + "=" * 60)
        print("⚠️ templates_id 未获取到，开始自动获取/创建...")
        print("=" * 60)

        # 获取 cookies 用于调用模板API
        cookies = platform_account.get('cookie', {})
        mtgsig = platform_account.get('mtgsig')

        # 使用浏览器跳转页面后获取/创建模板ID
        templates_id = ensure_template_id_with_browser(account_name, cookies, mtgsig, headless=HEADLESS, browser_pool=browser_pool)

        if templates_id:
            print(f"✅ 已成功获取 templates_id: {templates_id}")
        else:
            error_msg = "无法获取或创建报表模板ID"
            print(f"❌ {error_msg}")
            # 同时上报到两个日志接口
            log_failure(account_name, 0, "templates_id_check", start_date, end_date, error_msg)
            upload_task_status_batch(account_name, start_date, end_date, [{
                'task_name': 'templates_id_check',
                'success': False,
                'record_count': 0,
                'error_message': error_msg
            }])
            report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
            return False

    print(f"   ✅ templates_id 检查通过: {templates_id}")

    print("\n" + "=" * 80)
    print("🚀 开始执行任务")
    print("=" * 80)
    if task == 'all':
        print(f"执行模式: 页面驱动模式 - 先跳转页面再执行任务")
        print(f"执行顺序:")
        print(f"   1. 客流分析页面: store_stats (先执行，更新签名)")
        print(f"   2. 报表页面: kewen_daily_report, promotion_daily_report")
        print(f"   3. 评价页面: review_detail_dianping, review_detail_meituan,")
        print(f"                review_summary_dianping, review_summary_meituan")
    print("=" * 80)

    # 执行任务
    results = []
    try:
        if task == 'all':
            results = run_page_driven_tasks(
                account_name=account_name,
                start_date=start_date,
                end_date=end_date,
                headless=HEADLESS,
                browser_pool=browser_pool
            )
        else:
            result = TASK_MAP[task](account_name, start_date, end_date)
            results.append(result)
    except Exception as e:
        error_msg = f"任务执行异常: {str(e)}"
        print(f"❌ {error_msg}")
        report_task_callback(task_id, status=3, error_message=error_msg, retry_add=1)
        return False

    print_summary(results)

    # 上报任务状态
    if task == 'all':
        upload_task_status_batch(account_name, start_date, end_date, results)
    else:
        if results:
            upload_task_status_single(account_name, start_date, end_date, results[0])

    # 收集错误并上报任务回调
    task_errors = []
    for result in results:
        if not result.get('success'):
            task_name = result.get('task_name', '未知任务')
            error_msg = result.get('error_message', '未知错误')
            task_errors.append(f"[{task_name}] {error_msg}")

    if len(task_errors) == 0:
        report_task_callback(task_id, status=2, error_message="", retry_add=0)
        return True
    else:
        all_errors = "\n".join(task_errors)
        report_task_callback(task_id, status=3, error_message=all_errors, retry_add=1)
        return False


def main():
    """主函数 - 守护进程模式

    持续循环运行，自动获取并执行任务:
    1. 检查时间窗口 (DEV_MODE=True时24小时运行)
    2. 生成任务调度
    3. 获取任务并执行
    4. 无任务时等待5分钟后重试
    5. 支持 Ctrl+C 优雅退出

    浏览器池模式 (USE_BROWSER_POOL=True):
    - 启动时初始化浏览器池
    - 启动保活服务（24小时保持Cookie活跃）
    - 使用服务器IP获取任务
    - 退出时保存状态
    """
    global _daemon_running

    # ========== 初始化 ==========
    print("\n" + "=" * 80)
    print("美团点评数据采集系统 (守护进程模式)")
    print("=" * 80)
    print(f"   运行模式: {'开发模式 (24小时运行)' if DEV_MODE else f'生产模式 ({WORK_START_HOUR}:00-{WORK_END_HOUR}:00)'}")
    print(f"   浏览器池: {'启用' if USE_BROWSER_POOL and BROWSER_POOL_AVAILABLE else '禁用'}")
    print(f"   无任务等待: {NO_TASK_WAIT_SECONDS // 60} 分钟")
    print(f"   数据目录: {DATA_DIR}")
    print(f"   状态目录: {STATE_DIR}")
    print(f"   下载目录: {DOWNLOAD_DIR}")
    print("=" * 80)

    # 设置信号处理器
    _setup_signal_handlers()

    # 确保目录存在
    ensure_directories()

    # 浏览器池和保活服务
    browser_pool_instance = None
    keepalive_service = None
    server_ip = None

    # ========== 浏览器池模式初始化 ==========
    if USE_BROWSER_POOL and BROWSER_POOL_AVAILABLE:
        try:
            print("\n🚀 初始化浏览器池模式...")

            # 获取服务器公网IP
            server_ip = get_public_ip()
            if not server_ip:
                print("⚠️ 无法获取服务器IP，将使用传统模式")
            else:
                print(f"   服务器IP: {server_ip}")

                # 初始化浏览器池
                browser_pool_instance = initialize_browser_pool(headless=HEADLESS)

                # 启动保活服务
                keepalive_service = start_keepalive_service(browser_pool_instance)

                print("✅ 浏览器池模式初始化完成")

        except Exception as e:
            print(f"❌ 浏览器池初始化失败: {e}")
            print("   将使用传统模式运行")
            browser_pool_instance = None
            keepalive_service = None

    # 统计信息
    total_tasks = 0
    success_tasks = 0
    failed_tasks = 0

    print("\n🚀 开始守护进程循环...")
    print("   按 Ctrl+C 可优雅退出\n")

    # ========== 主循环 ==========
    try:
        while _daemon_running:
            try:
                # ========== Step 1: 时间窗口检查 ==========
                if not is_in_work_window():
                    wait_seconds = seconds_until_work_start()
                    hours = wait_seconds // 3600
                    minutes = (wait_seconds % 3600) // 60
                    print(f"\n{'=' * 60}")
                    print(f"💤 当前非工作时间 ({WORK_START_HOUR}:00-{WORK_END_HOUR}:00)")
                    print(f"   将在 {hours}小时{minutes}分钟 后开始工作...")
                    print(f"{'=' * 60}")

                    if not interruptible_sleep(wait_seconds):
                        break  # 收到退出信号
                    continue

                # ========== Step 2: 生成任务调度 ==========
                create_task_schedule()
                time.sleep(5)

                # ========== Step 3: 获取任务 ==========
                # 浏览器池模式下传入服务器IP
                if browser_pool_instance and server_ip:
                    task_info = fetch_task(server_ip=server_ip)
                else:
                    task_info = fetch_task()

                if not task_info:
                    print(f"\n⏳ 暂无待执行任务，{NO_TASK_WAIT_SECONDS // 60}分钟后重试...")
                    reschedule_failed_tasks()

                    # 在等待期间执行保活（同步模式 + 资源保护）
                    if keepalive_service and browser_pool_instance:
                        # 分段等待，每60秒检查一次
                        remaining_wait = NO_TASK_WAIT_SECONDS
                        keepalive_check_interval = 60  # 每60秒检查一次

                        while remaining_wait > 0 and _daemon_running:
                            # 等待一小段时间
                            sleep_chunk = min(keepalive_check_interval, remaining_wait)
                            if not interruptible_sleep(sleep_chunk):
                                break  # 收到退出信号
                            remaining_wait -= sleep_chunk

                            if not _daemon_running or remaining_wait <= 0:
                                break

                            # ===== 资源检查与自动调节 =====
                            try:
                                status = resource_monitor.check_status(force=True)

                                if status == resource_monitor.STATUS_CRITICAL:
                                    # 危险：紧急释放资源
                                    print("\n🚨 资源危险！执行紧急释放...")
                                    resource_monitor.print_status()
                                    browser_pool_instance.emergency_release()
                                    # 跳过保活
                                    continue

                                elif status == resource_monitor.STATUS_WARNING:
                                    # 警告：释放空闲Context，跳过保活
                                    resource_monitor.print_status()
                                    browser_pool_instance.release_idle_contexts()
                                    # 跳过保活
                                    continue

                                # 正常：执行保活
                                keepalive_service.perform_keepalive_batch()

                                # 执行Context数量限制
                                browser_pool_instance.enforce_context_limit()

                            except Exception as e:
                                print(f"   ⚠️ 空闲处理异常: {e}")

                        if not _daemon_running:
                            break  # 收到退出信号
                    else:
                        # 非浏览器池模式，直接等待
                        if not interruptible_sleep(NO_TASK_WAIT_SECONDS):
                            break  # 收到退出信号

                    continue

                # ========== Step 4: 执行任务 ==========

                # 资源检查（任务执行前）
                if browser_pool_instance and BROWSER_POOL_AVAILABLE:
                    status = resource_monitor.check_status(force=True)
                    if status == resource_monitor.STATUS_CRITICAL:
                        print("\n🚨 资源危险！暂停任务执行，等待资源恢复...")
                        resource_monitor.print_status()
                        # 重置任务状态，避免任务丢失
                        task_id = task_info.get('id')
                        if task_id:
                            reset_task_schedule(task_id)
                        browser_pool_instance.emergency_release()
                        # 等待30秒后重试
                        if not interruptible_sleep(30):
                            break
                        continue

                total_tasks += 1

                # 浏览器池模式下使用账号锁
                if browser_pool_instance:
                    account_id = task_info.get('account_id')
                    # 获取账号锁（阻塞等待，最多等60秒）
                    if account_lock_manager.acquire(account_id, blocking=True, timeout=60):
                        try:
                            success = execute_single_task(task_info, browser_pool=browser_pool_instance)
                        finally:
                            account_lock_manager.release(account_id)
                    else:
                        print(f"⚠️ 账号 {account_id} 锁获取超时，跳过此任务")
                        success = False
                else:
                    success = execute_single_task(task_info)

                if success:
                    success_tasks += 1
                else:
                    failed_tasks += 1

                # ========== Step 5: 重新调度失败任务 ==========
                reschedule_failed_tasks()

                # 打印当前统计
                print(f"\n📊 累计统计: 总任务={total_tasks}, 成功={success_tasks}, 失败={failed_tasks}")

                # 短暂等待后继续下一轮
                time.sleep(2)

            except KeyboardInterrupt:
                # 二次 Ctrl+C 强制退出
                print("\n⚠️ 再次收到中断信号，强制退出...")
                break
            except Exception as e:
                print(f"\n❌ 主循环发生异常: {e}")
                import traceback
                traceback.print_exc()
                # 等待一段时间后继续
                print(f"   将在60秒后继续运行...")
                if not interruptible_sleep(60):
                    break

    finally:
        # ========== 清理浏览器池 ==========
        if browser_pool_instance:
            print("\n🛑 正在关闭浏览器池...")
            try:
                if keepalive_service:
                    keepalive_service.stop()
                shutdown_browser_pool()
                print("✅ 浏览器池已关闭")
            except Exception as e:
                print(f"⚠️ 关闭浏览器池时出错: {e}")

    # ========== 退出 ==========
    print("\n" + "=" * 80)
    print("✅ 守护进程正常退出")
    print(f"   总任务: {total_tasks}, 成功: {success_tasks}, 失败: {failed_tasks}")
    print("=" * 80)


if __name__ == "__main__":
    main()

