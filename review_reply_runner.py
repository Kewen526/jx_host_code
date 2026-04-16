#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评价回复独立守护进程
====================

与采集进程（meituan_collector.py）解耦的 24 小时评价回复 runner。

设计要点:
1. 纯 HTTP 任务，不依赖 Playwright / Browser / Context
2. 每 REPLY_POLL_INTERVAL 秒轮询一次：
   - 从 /api/servers/getAccountsByHost 拉取本机分配的账号列表
   - 从 /api/get_platform_account 拉取每个账号的最新 Cookie（本地周期性推送的那份）
   - 调 review_reply.process_review_replies 处理该账号的待回复评价
3. 账号级别并发：ThreadPoolExecutor(REPLY_CONCURRENCY)
4. 单实例保护（PID 文件）
5. SIGINT / SIGTERM 优雅退出
6. 完整独立于采集进程，即使采集进程 crash，评价回复依然继续运行

SLA:
    业务要求"2 小时内回复"。30 分钟轮询一次 + 每轮几十秒跑完 = 远低于 SLA。

部署:
    # systemd 示例（/etc/systemd/system/review-reply-runner.service）
    [Unit]
    Description=Meituan/Dianping Review Reply Runner
    After=network.target

    [Service]
    Type=simple
    User=meituan
    WorkingDirectory=/home/meituan/jx_host_code
    ExecStart=/usr/bin/python3 /home/meituan/jx_host_code/review_reply_runner.py
    Restart=always
    RestartSec=30

    [Install]
    WantedBy=multi-user.target
"""

import os
import sys
import time
import signal
import atexit
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional

# 依赖的业务模块
from review_reply import process_review_replies
from browser_pool import (
    fetch_accounts_by_host,
    fetch_account_cookie,
    get_public_ip,
)
from logger import log_review, log_system, setup_stdout_redirect


# ============================================================================
# 配置参数
# ============================================================================

# 轮询间隔（秒）：10 分钟跑一次
REPLY_POLL_INTERVAL = 10 * 60

# 账号级并发数：纯 HTTP 任务，10 并发对风控压力很小
REPLY_CONCURRENCY = 10

# 单个账号处理超时（秒）
ACCOUNT_TIMEOUT = 180

# 账号列表拉取失败的重试间隔（秒）
ACCOUNT_FETCH_RETRY_INTERVAL = 60

# PID 文件路径（单实例保护）
PID_FILE = "/home/meituan/data/review_reply_runner.pid"


# ============================================================================
# 全局状态
# ============================================================================

_running = True
_shutdown_event = threading.Event()


# ============================================================================
# 日志工具
# ============================================================================

def _log(message: str, level: str = "INFO"):
    """系统级日志"""
    log_system(f"[ReviewReplyRunner] {message}", level)


def _log_account(account: str, message: str, level: str = "INFO"):
    """账号级日志（走 log_review，与 review_reply 模块日志一致）"""
    log_review(account, message, level)


# ============================================================================
# 单实例保护
# ============================================================================

def acquire_single_instance_lock():
    """创建 PID 文件，防止多进程重复运行"""
    os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)

    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # 检查进程是否还在
            print(f"❌ 已有实例正在运行 (PID={old_pid})，本次启动退出")
            print(f"   若旧进程已卡死，请先执行: kill {old_pid} && rm {PID_FILE}")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            os.remove(PID_FILE)

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        except Exception:
            pass

    atexit.register(_cleanup)


# ============================================================================
# 信号处理
# ============================================================================

def _shutdown_handler(signum, frame):
    global _running
    _running = False
    _shutdown_event.set()
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    print(f"\n⚠️ 收到退出信号 ({sig_name})，正在优雅关闭...")


def setup_signal_handlers():
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)


def interruptible_wait(seconds: int) -> bool:
    """可被信号打断的等待。返回 True 表示正常走完等待，False 表示被中断"""
    return not _shutdown_event.wait(timeout=seconds)


# ============================================================================
# 单账号处理逻辑
# ============================================================================

def _process_one_account(account_id: str) -> Dict[str, int]:
    """处理单个账号的评价回复

    流程：
    1. 从云端拉 Cookie（本地定期推送的那份）
    2. 调 process_review_replies 处理 pending-reply/list 返回的所有评价

    Returns:
        {"total": 总数, "success": 成功数, "failed": 失败数} 或 {"skipped": 1}
    """
    try:
        cookies = fetch_account_cookie(account_id)
        if not cookies:
            _log_account(account_id, "无可用 Cookie（可能已失效或未推送），跳过", "WARN")
            return {"skipped": 1, "total": 0, "success": 0, "failed": 0}

        # process_review_replies 内部已经处理了 mtgsig 获取、pending list 拉取、逐条回复
        stats = process_review_replies(account_id, cookies)
        return {
            "skipped": 0,
            "total": stats.get("total", 0),
            "success": stats.get("success", 0),
            "failed": stats.get("failed", 0),
        }
    except Exception as e:
        _log_account(account_id, f"处理异常: {type(e).__name__}: {e}", "ERROR")
        return {"skipped": 0, "total": 0, "success": 0, "failed": 0, "error": str(e)}


# ============================================================================
# 单轮处理
# ============================================================================

def _run_one_cycle(server_ip: str):
    """执行一轮评价回复处理：拉账号列表 → 并发处理每个账号"""
    cycle_start = time.time()
    _log(f"========== 开始新一轮评价回复 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ==========")

    # 1. 拉取服务器分配的账号列表
    accounts: List[str] = []
    try:
        accounts = fetch_accounts_by_host(server_ip)
    except Exception as e:
        _log(f"拉取账号列表异常: {e}", "ERROR")
        return

    if not accounts:
        _log("本机未分配账号，跳过本轮", "WARN")
        return

    _log(f"本轮待处理账号数: {len(accounts)}，并发: {REPLY_CONCURRENCY}")

    # 2. 使用线程池并发处理所有账号
    totals = {"skipped": 0, "total": 0, "success": 0, "failed": 0, "accounts_with_tasks": 0}

    with ThreadPoolExecutor(max_workers=REPLY_CONCURRENCY,
                            thread_name_prefix="ReplyWorker") as executor:
        future_to_account = {
            executor.submit(_process_one_account, acc): acc
            for acc in accounts
        }

        for future in as_completed(future_to_account):
            if not _running:
                # 收到退出信号：取消未开始的 future，已运行的等它自己结束
                for f in future_to_account:
                    if not f.done():
                        f.cancel()
                break

            account_id = future_to_account[future]
            try:
                result = future.result(timeout=ACCOUNT_TIMEOUT)
                totals["skipped"] += result.get("skipped", 0)
                totals["total"] += result.get("total", 0)
                totals["success"] += result.get("success", 0)
                totals["failed"] += result.get("failed", 0)
                if result.get("total", 0) > 0:
                    totals["accounts_with_tasks"] += 1
            except Exception as e:
                _log_account(account_id, f"worker future 异常: {e}", "ERROR")

    elapsed = time.time() - cycle_start
    _log(
        f"========== 本轮结束 耗时={elapsed:.1f}s  "
        f"账号总数={len(accounts)} 有回复={totals['accounts_with_tasks']} "
        f"跳过={totals['skipped']} "
        f"评价总数={totals['total']} 成功={totals['success']} 失败={totals['failed']} =========="
    )


# ============================================================================
# 主循环
# ============================================================================

def main():
    """主入口：24 小时循环，每 REPLY_POLL_INTERVAL 跑一轮"""
    acquire_single_instance_lock()
    setup_signal_handlers()
    setup_stdout_redirect()

    print("\n" + "=" * 80)
    print("评价回复守护进程 (review_reply_runner.py)")
    print("=" * 80)
    print(f"   轮询间隔: {REPLY_POLL_INTERVAL // 60} 分钟")
    print(f"   账号并发: {REPLY_CONCURRENCY}")
    print(f"   单账号超时: {ACCOUNT_TIMEOUT} 秒")
    print(f"   PID: {os.getpid()}")
    print("=" * 80)

    # 获取服务器公网 IP（用于拉取账号列表）
    server_ip = None
    while _running and not server_ip:
        server_ip = get_public_ip()
        if not server_ip:
            _log("无法获取服务器公网 IP，60 秒后重试", "WARN")
            if not interruptible_wait(60):
                break

    if not server_ip:
        _log("退出：未获取到服务器 IP", "ERROR")
        return

    _log(f"服务器公网 IP: {server_ip}")
    _log("进入主循环，按 Ctrl+C 优雅退出")

    # ===== 主循环 =====
    while _running:
        try:
            _run_one_cycle(server_ip)
        except Exception as e:
            _log(f"本轮执行异常: {type(e).__name__}: {e}", "ERROR")
            import traceback
            traceback.print_exc()

        if not _running:
            break

        # 等待到下一轮
        _log(f"等待 {REPLY_POLL_INTERVAL // 60} 分钟后进入下一轮")
        if not interruptible_wait(REPLY_POLL_INTERVAL):
            break

    _log("主循环退出，守护进程结束")
    print("\n" + "=" * 80)
    print("✅ 评价回复守护进程正常退出")
    print("=" * 80)


if __name__ == "__main__":
    main()
