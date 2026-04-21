#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一日志模块

功能:
1. 统一日志格式: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [MODULE] [ACCOUNT] message
2. 按天滚动日志文件，保留近7天
3. 支持按"日期 + 账号 + 动作类型"查询
4. 三种动作模块: 采集 / 保活 / 评价回复
5. stdout/stderr 自动重定向到日志文件（兜底捕获未改造的 print）

日志文件目录: /home/meituan/data/logs/
  run.log              -> 当前活跃日志
  run.log.2026-04-07   -> 历史日志（自动滚动）
"""

import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from contextvars import ContextVar

# ============================================================================
# 配置
# ============================================================================

LOG_DIR = "/home/meituan/data/logs"
LOG_FILE = os.path.join(LOG_DIR, "run.log")
LOG_BACKUP_COUNT = 7  # 保留7天历史日志

# 模块标签（固定三种 + 系统）
MODULE_COLLECT = "采集"
MODULE_KEEPALIVE = "保活"
MODULE_REVIEW = "评价回复"
MODULE_SYSTEM = "系统"

# 无账号时的占位符
NO_ACCOUNT = "-"

# 当前账号上下文（用于自动给 print() 输出打上账号标签）
_current_account: ContextVar[str] = ContextVar('current_account', default="")


def set_current_account(account: str):
    _current_account.set(account or "")


def clear_current_account():
    _current_account.set("")


def get_current_account() -> str:
    return _current_account.get()

# ============================================================================
# 初始化日志系统
# ============================================================================

# 保存原始 stdout/stderr（用于 _log 直接写控制台，避免重定向循环）
_original_stdout = sys.stdout
_original_stderr = sys.stderr

# 确保日志目录存在
os.makedirs(LOG_DIR, exist_ok=True)

# 创建文件 handler（按天滚动，保留7天）
_file_handler = TimedRotatingFileHandler(
    LOG_FILE,
    when='midnight',
    interval=1,
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8',
    delay=False,
)
# 后缀格式：run.log.2026-04-07
_file_handler.suffix = "%Y-%m-%d"

# 自定义 Formatter：直接输出 record.getMessage()，因为我们在 _log 中已格式化
class _RawFormatter(logging.Formatter):
    def format(self, record):
        return record.getMessage()

_file_handler.setFormatter(_RawFormatter())

# 创建 logger（仅文件输出，不加 console handler，避免与 stdout 重定向冲突）
_file_logger = logging.getLogger("meituan_app_file")
_file_logger.setLevel(logging.DEBUG)
_file_logger.addHandler(_file_handler)
_file_logger.propagate = False  # 不传播到 root logger


# ============================================================================
# 核心日志函数
# ============================================================================

def _log(module: str, account: str, message: str, level: str = "INFO"):
    """核心日志写入函数

    同时写入：
    1. 控制台（原始 stdout）
    2. 日志文件（通过 TimedRotatingFileHandler 自动滚动）

    Args:
        module: 模块标签（采集/保活/评价回复/系统）
        account: 账号标识（手机号/ID），无账号时传 "-"
        message: 日志消息
        level: 日志级别 (INFO/WARN/ERROR)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted = f"[{timestamp}] [{level}] [{module}] [{account}] {message}"

    # 写控制台（使用原始 stdout，绕过重定向）
    try:
        _original_stdout.write(formatted + "\n")
        _original_stdout.flush()
    except Exception:
        pass

    # 写文件（通过 logging handler，自动按天滚动）
    try:
        _file_logger.info(formatted)
    except Exception:
        pass


# ============================================================================
# 采集日志
# ============================================================================

def log_collect(account: str, message: str, level: str = "INFO"):
    """采集模块日志

    Args:
        account: 账号标识
        message: 日志消息
        level: 日志级别
    """
    _log(MODULE_COLLECT, account or NO_ACCOUNT, message, level)


# ============================================================================
# 保活日志
# ============================================================================

def log_keepalive(account: str, message: str, level: str = "INFO"):
    """保活模块日志

    Args:
        account: 账号标识
        message: 日志消息
        level: 日志级别
    """
    _log(MODULE_KEEPALIVE, account or NO_ACCOUNT, message, level)


# ============================================================================
# 评价回复日志
# ============================================================================

def log_review(account: str, message: str, level: str = "INFO"):
    """评价回复模块日志

    Args:
        account: 账号标识
        message: 日志消息
        level: 日志级别
    """
    _log(MODULE_REVIEW, account or NO_ACCOUNT, message, level)


# ============================================================================
# 系统日志（无账号上下文）
# ============================================================================

def log_system(message: str, level: str = "INFO"):
    """系统模块日志（无账号上下文）

    Args:
        message: 日志消息
        level: 日志级别
    """
    _log(MODULE_SYSTEM, NO_ACCOUNT, message, level)


# ============================================================================
# stdout/stderr 重定向（兜底捕获未改造的 print 输出）
# ============================================================================

class _StdoutRedirector:
    """将 stdout 写入同时输出到控制台和日志文件

    用于兜底捕获未改造为 log_* 调用的 print() 语句。
    这些日志以 [系统] 模块标签写入文件，可被查询工具检索。
    """

    def __init__(self, original_stream, is_stderr: bool = False):
        self._original = original_stream
        self._is_stderr = is_stderr
        self._level = "ERROR" if is_stderr else "INFO"

    def write(self, message: str):
        # 写到原始控制台
        try:
            self._original.write(message)
        except Exception:
            pass

        # 非空内容才写日志文件（跳过 print 自带的换行符等空输出）
        stripped = message.rstrip("\n\r") if message else ""
        if stripped:
            # 检查是否已经是我们格式化过的日志（避免 _log 输出被二次写入）
            # 格式化日志以 "[2026-" 开头
            if stripped.startswith("[20") and "] [" in stripped[:30]:
                # 已格式化的日志，只写文件不加额外格式
                try:
                    _file_logger.info(stripped)
                except Exception:
                    pass
            else:
                # 未格式化的 print 输出，包装为系统日志格式写入文件
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                account = get_current_account() or NO_ACCOUNT
                formatted = f"[{timestamp}] [{self._level}] [{MODULE_SYSTEM}] [{account}] {stripped}"
                try:
                    _file_logger.info(formatted)
                except Exception:
                    pass

    def flush(self):
        try:
            self._original.flush()
        except Exception:
            pass

    # 保持与原始 stdout 兼容的属性
    @property
    def encoding(self):
        return getattr(self._original, 'encoding', 'utf-8')

    def fileno(self):
        return self._original.fileno()

    def isatty(self):
        return self._original.isatty()


def setup_stdout_redirect():
    """启用 stdout/stderr 重定向

    调用后，所有 print() 输出将同时写入日志文件。
    已经使用 log_* 函数的日志不会被重复写入。

    应在程序启动时尽早调用（在 logger.py import 之后）。
    """
    sys.stdout = _StdoutRedirector(_original_stdout, is_stderr=False)
    sys.stderr = _StdoutRedirector(_original_stderr, is_stderr=True)
    log_system("日志系统已初始化，stdout/stderr 已重定向到日志文件")
    log_system(f"日志目录: {LOG_DIR}, 保留天数: {LOG_BACKUP_COUNT}")
