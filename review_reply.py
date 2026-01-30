#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评价回复模块

功能:
1. 获取待回复评价列表
2. 执行评价回复（大众点评/美团）
3. 回传回复结果

在保活成功后自动触发执行
"""

import json
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime


# ============================================================================
# 配置参数
# ============================================================================

API_BASE_URL = "http://8.146.210.145:3000"
PENDING_REPLY_LIST_API = f"{API_BASE_URL}/api/review/pending-reply/list"
TASK_REPLY_UPDATE_API = f"{API_BASE_URL}/api/review/task-reply/update"
GET_PLATFORM_ACCOUNT_API = f"{API_BASE_URL}/api/get_platform_account"

# 回复API配置
REPLY_API_URL = "https://e.dianping.com/review/app/reply/ajax/reviewreply"
API_TIMEOUT = 30


# ============================================================================
# 日志函数
# ============================================================================

def log_info(message: str):
    """输出信息日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [INFO] [评价回复] {message}")


def log_warn(message: str):
    """输出警告日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [WARN] [评价回复] {message}")


def log_error(message: str):
    """输出错误日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [ERROR] [评价回复] {message}")


# ============================================================================
# 工具函数
# ============================================================================

def _get_session() -> requests.Session:
    """获取禁用代理的Session"""
    session = requests.Session()
    session.trust_env = False
    session.proxies = {'http': None, 'https': None}
    return session


# ============================================================================
# API调用函数
# ============================================================================

def get_pending_reply_list(account: str) -> List[Dict]:
    """获取待回复评价列表

    Args:
        account: 账户名

    Returns:
        待回复评价列表
    """
    session = _get_session()
    try:
        response = session.post(
            PENDING_REPLY_LIST_API,
            headers={'Content-Type': 'application/json'},
            json={"account": account},
            timeout=API_TIMEOUT
        )

        result = response.json()
        if result.get('success'):
            data = result.get('data', [])
            return data if isinstance(data, list) else []
        else:
            log_warn(f"获取待回复列表失败: {result.get('message', '未知错误')}")
            return []
    except Exception as e:
        log_error(f"获取待回复列表异常: {e}")
        return []
    finally:
        session.close()


def get_mtgsig(account: str) -> Optional[str]:
    """从API获取mtgsig

    Args:
        account: 账户名

    Returns:
        mtgsig字符串，失败返回None
    """
    session = _get_session()
    try:
        response = session.post(
            GET_PLATFORM_ACCOUNT_API,
            headers={'Content-Type': 'application/json'},
            json={"account": account},
            timeout=API_TIMEOUT
        )

        result = response.json()
        if not result.get('success'):
            return None

        record = result.get('data', {})
        mtgsig_data = record.get('mtgsig')

        if isinstance(mtgsig_data, str):
            return mtgsig_data
        elif isinstance(mtgsig_data, dict):
            return json.dumps(mtgsig_data)
        return None
    except Exception as e:
        log_warn(f"获取mtgsig异常: {e}")
        return None
    finally:
        session.close()


def update_task_reply(data_name: str, review_id: str, task_reply: int, shop_reply: str) -> bool:
    """回传回复结果

    Args:
        data_name: 任务类型 (dianping/meituan)
        review_id: 评价ID
        task_reply: 回复状态 (2=成功, 3=失败)
        shop_reply: 回复内容或失败原因

    Returns:
        是否成功
    """
    session = _get_session()
    try:
        response = session.post(
            TASK_REPLY_UPDATE_API,
            headers={'Content-Type': 'application/json'},
            json={
                "data_name": data_name,
                "review_id": str(review_id),
                "task_reply": task_reply,
                "shop_reply": shop_reply
            },
            timeout=API_TIMEOUT
        )

        result = response.json()
        return result.get('success', False)
    except Exception as e:
        log_error(f"回传结果异常: {e}")
        return False
    finally:
        session.close()


# ============================================================================
# 回复执行函数
# ============================================================================

def reply_review(
    cookies: Dict,
    mtgsig: Optional[str],
    shop_id: str,
    review_id: str,
    user_id: str,
    content: str,
    platform: str = "dianping"
) -> Dict[str, Any]:
    """执行评价回复

    Args:
        cookies: Cookie字典
        mtgsig: mtgsig签名
        shop_id: 店铺ID
        review_id: 评价ID
        user_id: 用户ID
        content: 回复内容
        platform: 平台类型 (dianping/meituan)

    Returns:
        API返回结果
    """
    # 平台参数
    platform_code = 0 if platform == "dianping" else 1
    referer_page = "shop-comment-dp" if platform == "dianping" else "shop-comment-mt"

    # URL参数
    params = {
        'yodaReady': 'h5',
        'csecplatform': '4',
        'csecversion': '4.2.0',
    }

    # 添加mtgsig
    if mtgsig:
        params['mtgsig'] = mtgsig

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/json',
        'Origin': 'https://e.dianping.com',
        'Referer': f'https://e.dianping.com/vg-platform-reviewmanage/{referer_page}/index.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    # 请求体
    body = {
        "clientType": 1,
        "platform": platform_code,
        "content": content,
        "replyId": 0,
        "shopIdStr": str(shop_id),
        "reviewId": str(review_id),
        "userId": str(user_id)
    }

    session = _get_session()
    try:
        response = session.post(
            REPLY_API_URL,
            params=params,
            headers=headers,
            cookies=cookies,
            json=body,
            timeout=API_TIMEOUT
        )

        return response.json()
    except Exception as e:
        return {"code": -1, "msg": str(e)}
    finally:
        session.close()


# ============================================================================
# 主处理函数
# ============================================================================

def process_review_replies(account: str, cookies: Dict) -> Dict[str, int]:
    """处理账户的待回复评价

    在保活成功后调用，使用保活获取的cookie执行评价回复

    Args:
        account: 账户名
        cookies: 保活获取的cookie字典

    Returns:
        处理统计 {"total": 总数, "success": 成功数, "failed": 失败数}
    """
    stats = {"total": 0, "success": 0, "failed": 0}

    try:
        # 1. 获取待回复列表
        pending_list = get_pending_reply_list(account)
        if not pending_list:
            return stats

        stats["total"] = len(pending_list)
        log_info(f"{account} 有 {len(pending_list)} 条待回复评价")

        # 2. 获取mtgsig
        mtgsig = get_mtgsig(account)
        if not mtgsig:
            log_warn(f"{account} 未获取到mtgsig，回复可能失败")

        # 3. 遍历执行回复
        for task in pending_list:
            review_id = str(task.get('review_id', ''))
            shop_id = str(task.get('shop_id', ''))
            user_id = str(task.get('user_id', '0'))
            content = task.get('ai_gen', '')
            platform = task.get('platform', 'dianping')

            # 检查必要字段
            if not review_id or not shop_id or not content:
                log_warn(f"跳过无效任务: review_id={review_id}, shop_id={shop_id}")
                stats["failed"] += 1
                continue

            try:
                # 执行回复
                result = reply_review(
                    cookies=cookies,
                    mtgsig=mtgsig,
                    shop_id=shop_id,
                    review_id=review_id,
                    user_id=user_id,
                    content=content,
                    platform=platform
                )

                # 判断结果
                if result.get('code') == 200 or result.get('success'):
                    # 回复成功
                    log_info(f"评价 {review_id} 回复成功")
                    update_task_reply(platform, review_id, 2, content)
                    stats["success"] += 1
                else:
                    # 回复失败
                    error_msg = result.get('msg', str(result))
                    if isinstance(error_msg, dict):
                        error_msg = json.dumps(error_msg, ensure_ascii=False)
                    log_warn(f"评价 {review_id} 回复失败: {error_msg}")
                    update_task_reply(platform, review_id, 3, str(error_msg)[:500])
                    stats["failed"] += 1

            except Exception as e:
                log_error(f"评价 {review_id} 回复异常: {e}")
                update_task_reply(platform, review_id, 3, str(e)[:500])
                stats["failed"] += 1

        # 4. 输出统计
        log_info(f"{account} 评价回复完成: 总计 {stats['total']}, 成功 {stats['success']}, 失败 {stats['failed']}")

    except Exception as e:
        log_error(f"{account} 处理评价回复异常: {e}")

    return stats


# ============================================================================
# 测试入口
# ============================================================================

if __name__ == "__main__":
    # 测试用
    print("=" * 60)
    print("评价回复模块测试")
    print("=" * 60)

    # 测试账户
    test_account = "yunzu888"

    # 模拟cookie（实际使用时由保活提供）
    test_cookies = {}

    # 测试获取待回复列表
    print("\n1. 测试获取待回复列表...")
    pending = get_pending_reply_list(test_account)
    print(f"   获取到 {len(pending)} 条待回复评价")

    if pending:
        print(f"   第一条: {pending[0].get('review_id')} - {pending[0].get('platform')}")
