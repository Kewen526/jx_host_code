#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
meituan_collector 健康检查独立脚本

功能:
  - 检测进程是否存活（PID文件 + ps验证）
  - 检测线程数量（多线程是否正常启动）
  - 检测日志文件是否持续更新（判断卡死/僵尸进程）
  - 检测最近日志中的错误（影响正常运作的异常）
  - 检测系统资源（CPU / 内存）
  - 将健康状态上报到远程API

使用方式:
  # 单次检查并上报
  python3 health_check.py

  # 守护模式，每60秒检查一次
  python3 health_check.py --daemon --interval 60

  # 仅打印状态，不上报
  python3 health_check.py --dry-run

  # 配合 crontab 使用（每5分钟）
  # */5 * * * * /usr/bin/python3 /path/to/health_check.py >> /home/meituan/data/health_check.log 2>&1
"""

import os
import sys
import json
import time
import signal
import argparse
import subprocess
import re
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None
    print("⚠️ requests 未安装，API上报功能不可用")

# ============================================================================
# 配置 - 与 meituan_collector.py 保持一致
# ============================================================================
PID_FILE = "/home/meituan/data/meituan_collector.pid"
LOG_FILE = "/home/meituan/data/run.log"
PROCESS_NAME = "meituan_collector"

# 健康上报API（通过API管理平台）
API_BASE = "https://kewenai.asia"
HEALTH_REPORT_API = f"{API_BASE}/api/health-reports/upsert"

# 阈值
LOG_STALE_MINUTES = 10        # 日志超过10分钟没更新 → 可能卡死
CPU_HIGH_THRESHOLD = 90.0     # CPU > 90% → 异常（空转）
MEMORY_HIGH_THRESHOLD = 90.0  # 内存 > 90% → 危险
ERROR_LOOKBACK_LINES = 200    # 回看最近200行日志查错误

# 影响正常运作的错误关键词
ERROR_KEYWORDS = [
    "❌", "error", "exception", "traceback", "失败",
    "SIGTERM", "SIGINT", "退出",
    "context was destroyed", "target page, context or browser has been closed",
    "Cookie.*失效", "认证失败", "authentication_error",
    "OAuth token has expired",
    "连接超时", "ConnectionError", "TimeoutError",
    "资源.*紧张", "STATUS_CRITICAL",
    "Browser process was killed",
]

# 致命错误 - 直接判定服务异常
FATAL_KEYWORDS = [
    "守护进程正常退出", "收到退出信号",
    "OAuth token has expired", "authentication_error",
]


# ============================================================================
# 工具函数
# ============================================================================

def get_public_ip():
    """获取服务器公网IP"""
    try:
        resp = requests.get("https://ifconfig.me/ip", timeout=5)
        if resp.status_code == 200:
            return resp.text.strip()
    except Exception:
        pass
    try:
        resp = requests.get("https://api.ipify.org", timeout=5)
        if resp.status_code == 200:
            return resp.text.strip()
    except Exception:
        pass
    # 阿里云ECS元数据服务（内网可达，无需公网）
    try:
        resp = requests.get("http://100.100.100.200/latest/meta-data/eipv4", timeout=3)
        if resp.status_code == 200 and resp.text.strip():
            return resp.text.strip()
    except Exception:
        pass
    try:
        resp = requests.get("http://100.100.100.200/latest/meta-data/public-ipv4", timeout=3)
        if resp.status_code == 200 and resp.text.strip():
            return resp.text.strip()
    except Exception:
        pass
    # 最终兜底：取本机出口IP（不依赖外网）
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        pass
    return os.uname().nodename  # 兜底用主机名


# ============================================================================
# 健康检查项
# ============================================================================

def check_process_alive():
    """检查进程是否存活"""
    result = {
        "name": "process_alive",
        "healthy": False,
        "pid": None,
        "detail": "",
    }

    # 1. 检查PID文件
    pid_from_file = None
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid_from_file = int(f.read().strip())
        except (ValueError, IOError):
            pass

    # 2. 通过 ps 查找实际进程
    try:
        out = subprocess.check_output(
            ["ps", "aux"], text=True, timeout=10
        )
        matched = []
        for line in out.splitlines():
            if PROCESS_NAME in line and "grep" not in line and "health_check" not in line:
                parts = line.split()
                if len(parts) >= 2:
                    matched.append({
                        "pid": int(parts[1]),
                        "cpu": parts[2],
                        "mem": parts[3],
                        "started": parts[8] if len(parts) > 8 else "?",
                        "full_line": line,
                    })
    except Exception as e:
        result["detail"] = f"ps 执行失败: {e}"
        return result

    if not matched:
        result["detail"] = "进程未运行"
        if pid_from_file:
            result["detail"] += f"（PID文件残留: {pid_from_file}）"
        return result

    if len(matched) > 1:
        result["detail"] = f"发现 {len(matched)} 个进程实例（可能有僵尸进程）"
        result["pid"] = [p["pid"] for p in matched]
        result["processes"] = matched
        # 多个实例本身就是异常
        return result

    proc = matched[0]
    result["pid"] = proc["pid"]
    result["healthy"] = True
    result["cpu"] = proc["cpu"]
    result["mem"] = proc["mem"]
    result["detail"] = f"PID={proc['pid']}, CPU={proc['cpu']}%, MEM={proc['mem']}%"

    # PID文件与实际进程不匹配
    if pid_from_file and pid_from_file != proc["pid"]:
        result["detail"] += f"（⚠️ PID文件={pid_from_file}，实际={proc['pid']}）"

    return result


def check_thread_count():
    """检查进程的线程数"""
    result = {
        "name": "thread_count",
        "healthy": False,
        "thread_count": 0,
        "detail": "",
    }

    # 找到进程PID
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", PROCESS_NAME], text=True, timeout=10
        ).strip()
        pids = [int(p) for p in out.splitlines() if p.strip()]
    except (subprocess.CalledProcessError, ValueError):
        result["detail"] = "进程未运行，无法检查线程"
        return result

    if not pids:
        result["detail"] = "进程未运行"
        return result

    total_threads = 0
    thread_details = []
    for pid in pids:
        try:
            # /proc/<pid>/status 有 Threads 字段
            status_file = f"/proc/{pid}/status"
            if os.path.exists(status_file):
                with open(status_file, "r") as f:
                    for line in f:
                        if line.startswith("Threads:"):
                            count = int(line.split()[1])
                            total_threads += count
                            thread_details.append(f"PID {pid}: {count} threads")
                            break
        except (IOError, ValueError):
            thread_details.append(f"PID {pid}: 无法读取")

    result["thread_count"] = total_threads
    result["healthy"] = total_threads >= 1
    result["detail"] = "; ".join(thread_details) if thread_details else f"总线程数: {total_threads}"
    return result


def check_log_freshness():
    """检查日志文件是否持续更新（判断卡死）"""
    result = {
        "name": "log_freshness",
        "healthy": False,
        "last_modified": None,
        "stale_minutes": None,
        "detail": "",
    }

    if not os.path.exists(LOG_FILE):
        result["detail"] = f"日志文件不存在: {LOG_FILE}"
        return result

    try:
        mtime = os.path.getmtime(LOG_FILE)
        last_mod = datetime.fromtimestamp(mtime)
        now = datetime.now()
        stale_min = (now - last_mod).total_seconds() / 60

        result["last_modified"] = last_mod.strftime("%Y-%m-%d %H:%M:%S")
        result["stale_minutes"] = round(stale_min, 1)

        if stale_min <= LOG_STALE_MINUTES:
            result["healthy"] = True
            result["detail"] = f"日志正常更新（{stale_min:.0f}分钟前）"
        else:
            result["detail"] = f"日志已 {stale_min:.0f} 分钟未更新（阈值 {LOG_STALE_MINUTES} 分钟），可能卡死"
    except Exception as e:
        result["detail"] = f"读取日志文件失败: {e}"

    return result


def check_recent_errors(process_start_time=None):
    """检查最近日志中的错误，只检查当前进程启动后的日志行"""
    result = {
        "name": "recent_errors",
        "healthy": True,
        "error_count": 0,
        "fatal_count": 0,
        "errors": [],
        "detail": "",
    }

    if not os.path.exists(LOG_FILE):
        result["detail"] = "日志文件不存在"
        return result

    try:
        # 读取最后 N 行
        out = subprocess.check_output(
            ["tail", f"-{ERROR_LOOKBACK_LINES}", LOG_FILE],
            text=True, timeout=10
        )
    except Exception as e:
        result["detail"] = f"读取日志失败: {e}"
        return result

    error_pattern = re.compile(
        "|".join(ERROR_KEYWORDS), re.IGNORECASE
    )
    fatal_pattern = re.compile(
        "|".join(FATAL_KEYWORDS), re.IGNORECASE
    )
    # 日志行时间戳格式: [2026-03-19 04:01:52.401]
    log_ts_pattern = re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')

    errors_found = []
    fatal_found = []

    for line in out.splitlines():
        line_stripped = line.strip()
        if not line_stripped:
            continue
        # 跳过进程启动前的旧日志行（避免上次正常退出的消息误报）
        if process_start_time:
            ts_match = log_ts_pattern.match(line_stripped)
            if ts_match:
                try:
                    line_time = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
                    if line_time < process_start_time:
                        continue
                except ValueError:
                    pass
        if error_pattern.search(line_stripped):
            errors_found.append(line_stripped[:200])  # 截断过长行
        if fatal_pattern.search(line_stripped):
            fatal_found.append(line_stripped[:200])

    result["error_count"] = len(errors_found)
    result["fatal_count"] = len(fatal_found)
    # 只保留最近10条错误
    result["errors"] = errors_found[-10:]

    if fatal_found:
        result["healthy"] = False
        result["detail"] = f"发现 {len(fatal_found)} 条致命错误（最近: {fatal_found[-1][:100]}）"
    elif errors_found:
        # 有错误但不致命，仍然标记不健康让用户关注
        result["healthy"] = False
        result["detail"] = f"发现 {len(errors_found)} 条错误/异常"
    else:
        result["detail"] = "最近日志无异常"

    return result


def check_system_resources():
    """检查CPU和内存使用率"""
    result = {
        "name": "system_resources",
        "healthy": True,
        "cpu_percent": None,
        "mem_percent": None,
        "disk_percent": None,
        "detail": "",
    }

    # CPU - 通过进程的CPU占用
    try:
        out = subprocess.check_output(
            ["ps", "aux"], text=True, timeout=10
        )
        for line in out.splitlines():
            if PROCESS_NAME in line and "grep" not in line and "health_check" not in line:
                parts = line.split()
                if len(parts) >= 4:
                    cpu = float(parts[2])
                    mem = float(parts[3])
                    result["cpu_percent"] = cpu
                    result["mem_percent"] = mem

                    issues = []
                    if cpu > CPU_HIGH_THRESHOLD:
                        issues.append(f"CPU过高({cpu}%)")
                        result["healthy"] = False
                    if mem > MEMORY_HIGH_THRESHOLD:
                        issues.append(f"内存过高({mem}%)")
                        result["healthy"] = False

                    if issues:
                        result["detail"] = "，".join(issues)
                    else:
                        result["detail"] = f"CPU={cpu}%, MEM={mem}%（正常）"
                    break
        else:
            result["detail"] = "进程未运行，无资源数据"
    except Exception as e:
        result["detail"] = f"获取资源信息失败: {e}"

    # 磁盘使用率
    try:
        stat = os.statvfs("/home/meituan/data")
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bfree * stat.f_frsize
        if total > 0:
            used_pct = round((1 - free / total) * 100, 1)
            result["disk_percent"] = used_pct
            if used_pct > 90:
                result["healthy"] = False
                result["detail"] += f"，磁盘使用 {used_pct}%"
    except Exception:
        pass

    return result


def check_process_uptime():
    """检查进程运行时长（判断是否最近重启过）"""
    result = {
        "name": "process_uptime",
        "healthy": True,
        "uptime_seconds": None,
        "started_at": None,
        "detail": "",
    }

    try:
        out = subprocess.check_output(
            ["pgrep", "-f", PROCESS_NAME], text=True, timeout=10
        ).strip()
        pids = [int(p) for p in out.splitlines() if p.strip()]
    except (subprocess.CalledProcessError, ValueError):
        result["healthy"] = False
        result["detail"] = "进程未运行"
        return result

    if not pids:
        result["healthy"] = False
        result["detail"] = "进程未运行"
        return result

    pid = pids[0]
    try:
        # /proc/<pid>/stat 第22字段是 starttime (clock ticks since boot)
        # 更简单的方式: ps -o etimes
        out = subprocess.check_output(
            ["ps", "-o", "etimes=", "-p", str(pid)],
            text=True, timeout=10
        ).strip()
        uptime_sec = int(out.strip())
        result["uptime_seconds"] = uptime_sec

        started = datetime.now() - timedelta(seconds=uptime_sec)
        result["started_at"] = started.strftime("%Y-%m-%d %H:%M:%S")

        hours = uptime_sec // 3600
        minutes = (uptime_sec % 3600) // 60
        result["detail"] = f"已运行 {hours}小时{minutes}分钟（启动于 {result['started_at']}）"

        # 刚启动不到2分钟，可能是刚重启
        if uptime_sec < 120:
            result["detail"] += "（刚重启）"
    except Exception as e:
        result["detail"] = f"获取运行时长失败: {e}"

    return result


# ============================================================================
# 汇总 & 上报
# ============================================================================

def run_all_checks():
    """执行所有检查项，汇总结果（拍平为一级字段，适配API管理平台）"""
    proc = check_process_alive()
    threads = check_thread_count()
    log = check_log_freshness()
    resources = check_system_resources()
    uptime = check_process_uptime()

    # 解析进程启动时间，传给 check_recent_errors 过滤旧日志行
    process_start_dt = None
    if uptime.get("started_at"):
        try:
            process_start_dt = datetime.strptime(uptime["started_at"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    errors = check_recent_errors(process_start_time=process_start_dt)

    # 只用进程存活、日志新鲜度、系统资源判断服务是否正常运行
    # recent_errors 不参与健康判断（业务日志中的"失败"字样不代表服务挂了）
    checks = [proc, threads, log, resources, uptime]
    overall_healthy = all(c["healthy"] for c in checks)
    unhealthy_items = [c["name"] for c in checks if not c["healthy"]]
    all_checks = [proc, threads, log, errors, resources, uptime]

    # 拍平为一级字段，平台 #{} 可直接取值
    # unhealthy_items / recent_errors 序列化为 JSON 字符串，兼容数据库 TEXT 列
    report = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hostname": get_public_ip(),
        "service": PROCESS_NAME,
        "healthy": 1 if overall_healthy else 0,
        "status": "healthy" if overall_healthy else "unhealthy",
        "unhealthy_items": json.dumps(unhealthy_items, ensure_ascii=False),
        "summary": _build_summary(all_checks, overall_healthy),
        # 进程存活
        "process_alive": 1 if proc["healthy"] else 0,
        "pid": str(proc.get("pid", "")) if proc.get("pid") else "",
        "process_cpu": float(proc.get("cpu", 0) or 0),
        "process_mem": float(proc.get("mem", 0) or 0),
        # 线程数
        "thread_count": threads.get("thread_count", 0),
        # 日志新鲜度
        "log_fresh": 1 if log["healthy"] else 0,
        "log_stale_minutes": log.get("stale_minutes") or 0,
        "log_last_modified": log.get("last_modified", ""),
        # 错误
        "no_errors": 1 if errors["healthy"] else 0,
        "error_count": errors.get("error_count", 0),
        "fatal_count": errors.get("fatal_count", 0),
        "recent_errors": json.dumps(errors.get("errors", []), ensure_ascii=False),
        # 系统资源
        "resources_ok": 1 if resources["healthy"] else 0,
        "disk_percent": resources.get("disk_percent") or 0,
        # 运行时长
        "uptime_seconds": uptime.get("uptime_seconds") or 0,
        "started_at": uptime.get("started_at", ""),
    }

    # 保留原始 checks 供本地打印使用
    report["_checks"] = {c["name"]: c for c in all_checks}

    return report


def _build_summary(checks, overall_healthy):
    """构建人类可读的摘要"""
    if overall_healthy:
        return "服务运行正常"

    problems = []
    for c in checks:
        if not c["healthy"]:
            problems.append(f"[{c['name']}] {c['detail']}")
    return "；".join(problems)


def upload_report(report, api_url=None, dry_run=False):
    """上报健康状态到API（发送拍平的一级字段）"""
    url = api_url or HEALTH_REPORT_API

    # 排除内部字段，只发平台需要的一级字段
    payload = {k: v for k, v in report.items() if not k.startswith("_")}

    if dry_run:
        print(f"\n{'='*60}")
        print(f"[DRY-RUN] 以下数据将上报到: {url}")
        print(f"{'='*60}")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return True

    if requests is None:
        print("❌ requests 库未安装，无法上报")
        return False

    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=15,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            print(f"✅ 健康状态已上报 → {url}")
            return True
        else:
            print(f"⚠️ 上报返回 HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ 上报失败: {e}")
        return False


def print_report(report):
    """在终端打印健康报告"""
    is_healthy = report["healthy"] == 1 if isinstance(report["healthy"], int) else report["healthy"]
    status_icon = "✅" if is_healthy else "❌"
    print(f"\n{'='*60}")
    print(f" {status_icon} 美团采集服务健康报告")
    print(f" 时间: {report['timestamp']}")
    print(f" 主机: {report['hostname']}")
    print(f" 状态: {report['status'].upper()}")
    print(f"{'='*60}")

    # 使用内部保留的 _checks 打印详情
    checks = report.get("_checks", {})
    for name, check in checks.items():
        icon = "✅" if check["healthy"] else "❌"
        print(f"  {icon} {name}: {check['detail']}")

    if not is_healthy:
        print(f"\n  ⚠️ 摘要: {report['summary']}")

    print(f"{'='*60}\n")


# ============================================================================
# 入口
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="meituan_collector 健康检查")
    parser.add_argument("--daemon", action="store_true", help="守护模式，持续检查")
    parser.add_argument("--interval", type=int, default=60, help="检查间隔秒数（默认60秒）")
    parser.add_argument("--dry-run", action="store_true", help="仅打印，不上报API")
    parser.add_argument("--api-url", type=str, default=None, help="自定义上报API地址")
    parser.add_argument("--quiet", action="store_true", help="静默模式，仅输出异常")
    args = parser.parse_args()

    # 信号处理
    running = [True]

    def handle_signal(sig, frame):
        running[0] = False
        print("\n收到退出信号，停止健康检查...")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if args.daemon:
        print(f"🔍 健康检查守护模式启动，间隔 {args.interval} 秒")
        while running[0]:
            report = run_all_checks()
            if not args.quiet or not report["healthy"]:
                print_report(report)
            upload_report(report, api_url=args.api_url, dry_run=args.dry_run)
            # 可中断的等待
            for _ in range(args.interval):
                if not running[0]:
                    break
                time.sleep(1)
        print("健康检查已退出")
    else:
        report = run_all_checks()
        print_report(report)
        upload_report(report, api_url=args.api_url, dry_run=args.dry_run)
        # 退出码: 0=健康, 1=不健康
        sys.exit(0 if report["healthy"] else 1)


if __name__ == "__main__":
    main()
