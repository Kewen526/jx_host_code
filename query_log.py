#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志查询工具

支持按日期、账号、动作类型查询日志。

用法:
    # 查询某账号某天的保活日志
    python3 query_log.py --date 2026-04-07 --account 15123488333 --type 保活

    # 查询某账号某天的评价回复日志
    python3 query_log.py --date 2026-04-07 --account 15123488333 --type 评价回复

    # 查询某账号某天的所有日志（不限类型）
    python3 query_log.py --date 2026-04-07 --account 15123488333

    # 查询日期范围
    python3 query_log.py --date 2026-04-01:2026-04-07 --account 15123488333

    # 查询某天某类型的所有账号
    python3 query_log.py --date 2026-04-07 --type 采集

    # 查询今天的所有日志
    python3 query_log.py

    # 只显示ERROR级别
    python3 query_log.py --level ERROR

    # 限制输出行数
    python3 query_log.py --date 2026-04-07 --tail 50
"""

import os
import sys
import glob
import argparse
from datetime import datetime, timedelta


# 日志目录（与 logger.py 保持一致）
LOG_DIR = "/home/meituan/data/logs"
LOG_BASE = "run.log"

# 有效的模块类型
VALID_TYPES = ["采集", "保活", "评价回复", "系统"]


def get_log_files(date_str: str = None) -> list:
    """获取日志文件列表

    由于 TimedRotatingFileHandler 的轮转发生在午夜后的第一次写入，
    某天的日志可能分布在前一天的轮转文件和当天文件中。
    因此查询时会包含前一天的轮转文件，通过时间戳过滤确保准确性。

    Args:
        date_str: 日期字符串，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD:YYYY-MM-DD"（日期范围）
                  None 表示今天

    Returns:
        匹配的日志文件路径列表（按日期升序）
    """
    if not os.path.isdir(LOG_DIR):
        print(f"错误: 日志目录不存在 {LOG_DIR}", file=sys.stderr)
        return []

    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    # 解析日期范围
    if ":" in date_str:
        parts = date_str.split(":")
        if len(parts) != 2:
            print(f"错误: 日期范围格式无效，应为 YYYY-MM-DD:YYYY-MM-DD", file=sys.stderr)
            return []
        try:
            start_date = datetime.strptime(parts[0].strip(), "%Y-%m-%d")
            end_date = datetime.strptime(parts[1].strip(), "%Y-%m-%d")
        except ValueError:
            print(f"错误: 日期格式无效，应为 YYYY-MM-DD", file=sys.stderr)
            return []
    else:
        try:
            start_date = datetime.strptime(date_str.strip(), "%Y-%m-%d")
            end_date = start_date
        except ValueError:
            print(f"错误: 日期格式无效，应为 YYYY-MM-DD", file=sys.stderr)
            return []

    # 扩展范围：包含前一天的轮转文件（应对轮转边界问题）
    search_start = start_date - timedelta(days=1)

    # 收集匹配的文件
    files = []
    today = datetime.now().strftime("%Y-%m-%d")
    active_log = os.path.join(LOG_DIR, LOG_BASE)

    current = search_start
    while current <= end_date:
        date_suffix = current.strftime("%Y-%m-%d")

        if date_suffix == today:
            if os.path.exists(active_log) and active_log not in files:
                files.append(active_log)
        else:
            rotated_log = os.path.join(LOG_DIR, f"{LOG_BASE}.{date_suffix}")
            if os.path.exists(rotated_log) and rotated_log not in files:
                files.append(rotated_log)

        current += timedelta(days=1)

    # 今天的活跃日志总是包含在内（可能包含尚未轮转的条目）
    if os.path.exists(active_log) and active_log not in files:
        files.append(active_log)

    return sorted(files)


def query_logs(files: list, account: str = None, log_type: str = None,
               level: str = None, tail: int = None,
               date_filter: str = None) -> list:
    """查询日志内容

    Args:
        files: 日志文件列表
        account: 账号过滤（模糊匹配）
        log_type: 模块类型过滤（采集/保活/评价回复/系统）
        level: 日志级别过滤（INFO/WARN/ERROR）
        tail: 限制输出的最后N行
        date_filter: 日期过滤，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD:YYYY-MM-DD"
                     只返回时间戳在此范围内的日志行

    Returns:
        匹配的日志行列表
    """
    results = []

    # 解析日期过滤范围
    filter_start = None
    filter_end = None
    if date_filter:
        if ":" in date_filter:
            parts = date_filter.split(":")
            filter_start = parts[0].strip()
            filter_end = parts[1].strip()
        else:
            filter_start = date_filter.strip()
            filter_end = filter_start

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n\r")
                    if not line:
                        continue

                    # 时间戳日期过滤：日志格式 [YYYY-MM-DD HH:MM:SS.mmm] ...
                    if filter_start and line.startswith("["):
                        line_date = line[1:11]  # 提取 YYYY-MM-DD
                        if len(line_date) == 10 and line_date[4] == '-':
                            if line_date < filter_start or line_date > filter_end:
                                continue

                    if account and f"[{account}]" not in line:
                        continue

                    if log_type and f"[{log_type}]" not in line:
                        continue

                    if level and f"[{level}]" not in line:
                        continue

                    results.append(line)

        except Exception as e:
            print(f"警告: 读取文件 {filepath} 失败: {e}", file=sys.stderr)

    if tail and len(results) > tail:
        results = results[-tail:]

    return results


def main():
    parser = argparse.ArgumentParser(
        description="日志查询工具 - 按日期、账号、动作类型查询日志",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --date 2026-04-07 --account 15123488333 --type 保活
  %(prog)s --date 2026-04-07 --account 15123488333 --type 评价回复
  %(prog)s --date 2026-04-07 --account 15123488333
  %(prog)s --date 2026-04-01:2026-04-07 --account 15123488333
  %(prog)s --date 2026-04-07 --type 采集
  %(prog)s --level ERROR
  %(prog)s --tail 100
        """
    )

    parser.add_argument(
        "--date", "-d",
        help="日期 (YYYY-MM-DD) 或日期范围 (YYYY-MM-DD:YYYY-MM-DD)，默认今天"
    )
    parser.add_argument(
        "--account", "-a",
        help="账号（模糊匹配）"
    )
    parser.add_argument(
        "--type", "-t",
        choices=VALID_TYPES,
        help="动作类型: 采集 / 保活 / 评价回复 / 系统"
    )
    parser.add_argument(
        "--level", "-l",
        choices=["INFO", "WARN", "ERROR"],
        help="日志级别: INFO / WARN / ERROR"
    )
    parser.add_argument(
        "--tail", "-n",
        type=int,
        help="只显示最后N行"
    )
    parser.add_argument(
        "--count", "-c",
        action="store_true",
        help="只显示匹配行数，不输出内容"
    )
    parser.add_argument(
        "--files",
        action="store_true",
        help="列出可用的日志文件"
    )

    args = parser.parse_args()

    # 列出日志文件
    if args.files:
        pattern = os.path.join(LOG_DIR, f"{LOG_BASE}*")
        all_files = sorted(glob.glob(pattern))
        if not all_files:
            print(f"日志目录 {LOG_DIR} 中无日志文件")
        else:
            print(f"日志目录: {LOG_DIR}")
            print(f"{'文件名':<35} {'大小':>10} {'修改时间'}")
            print("-" * 70)
            for f in all_files:
                size = os.path.getsize(f)
                mtime = datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")
                name = os.path.basename(f)
                if size >= 1024 * 1024:
                    size_str = f"{size / 1024 / 1024:.1f} MB"
                elif size >= 1024:
                    size_str = f"{size / 1024:.1f} KB"
                else:
                    size_str = f"{size} B"
                print(f"{name:<35} {size_str:>10} {mtime}")
        return

    # 获取日志文件
    files = get_log_files(args.date)
    if not files:
        date_hint = args.date or "今天"
        print(f"未找到 {date_hint} 的日志文件")
        print(f"日志目录: {LOG_DIR}")
        return

    # 查询（传入日期过滤，确保跨文件搜索时只返回匹配日期的行）
    results = query_logs(
        files=files,
        account=args.account,
        log_type=args.type,
        level=args.level,
        tail=args.tail,
        date_filter=args.date or datetime.now().strftime("%Y-%m-%d"),
    )

    # 输出
    if args.count:
        print(f"匹配行数: {len(results)}")
    elif not results:
        filters = []
        if args.account:
            filters.append(f"账号={args.account}")
        if args.type:
            filters.append(f"类型={args.type}")
        if args.level:
            filters.append(f"级别={args.level}")
        filter_str = ", ".join(filters) if filters else "无过滤条件"
        print(f"未找到匹配的日志 ({filter_str})")
    else:
        for line in results:
            print(line)

        # 在末尾显示统计
        print(f"\n--- 共 {len(results)} 条匹配日志 ---")


if __name__ == "__main__":
    main()
