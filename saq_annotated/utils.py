# ============================================================================
# 模块: saq/utils.py
# 功能层: SAQ的通用工具函数，包括时间操作、UUID生成、指数退避、异步任务取消
# 设计层: 纯函数模块，无状态无副作用（除random），方便测试和复用
# 上下文层: 被job.py、worker.py、queue/redis.py等多个核心模块依赖
# ============================================================================

"""
Utils
"""

from __future__ import annotations
# 功能层: 启用延迟注解求值

import asyncio
# 功能层: 导入asyncio库，用于异步任务管理和事件循环操作
# 上下文层: cancel_tasks()函数需要asyncio.Task和asyncio.gather等API

import time
# 功能层: 导入time模块，提供时间戳获取功能
# 上下文层: now()和now_seconds()是SAQ中所有时间相关操作的基础

import typing as t
# 功能层: 导入类型注解工具

import uuid
# 功能层: 导入uuid模块，用于生成唯一标识符
# 上下文层: Job的key字段默认用UUID1生成

from random import random
# 功能层: 导入随机数生成函数，用于指数退避中的抖动(jitter)计算
# 设计层: 仅导入需要的函数而非整个模块，减少命名空间污染


if t.TYPE_CHECKING:
    # 功能层: 类型检查时的条件导入
    from collections.abc import Iterable


def now() -> int:
    # 功能层: 获取当前时间的毫秒级时间戳
    # 设计层: SAQ内部统一用毫秒，比秒精度高，也避免浮点运算
    # 上下文层: Job的queued、started、completed、touched等时间戳字段都调用此函数
    return int(time.time() * 1000)


def now_seconds() -> float:
    # 功能层: 获取当前时间的秒级浮点时间戳
    # 上下文层: Redis的schedule脚本用秒级时间戳做比较
    return time.time()


def uuid1() -> str:
    # 功能层: 生成UUID1字符串，基于时间戳和MAC地址保证全局唯一
    # 设计层: UUID1具有时间有序性，适合作为分布式系统中的标识符
    # 上下文层: Job的默认key和Worker的默认id都靠此函数生成
    return str(uuid.uuid1())


def millis(s: float) -> float:
    # 功能层: 将秒转换为毫秒
    # 上下文层: Worker.abort()中将秒级阈值转换为毫秒与Job.duration()比较
    return s * 1000


def seconds(ms: float) -> float:
    # 功能层: 将毫秒转换为秒
    # 上下文层: Job.stuck属性中将毫秒差值转换为秒与timeout比较
    return ms / 1000


def exponential_backoff(
    # 功能层: 计算指数退避延迟时间，用于任务重试时的等待间隔
    # 设计层: 分布式重试常用做法：指数增长加随机抖动，避免惊群效应
    # 上下文层: Job.next_retry_delay()调用此函数计算重试延迟

    attempts: int,
    # 功能层: 当前已尝试的次数
    base_delay: float,
    # 功能层: 基础延迟时间（秒）
    max_delay: float | None = None,
    # 功能层: 最大延迟上限（秒），None表示无上限
    jitter: bool = True,
    # 功能层: 是否添加随机抖动，True时在0到计算延迟之间取随机值
) -> float:
    if max_delay is None:
        max_delay = float("inf")
        # 功能层: 无上限时用无穷大替代，简化min()调用
    backoff = min(max_delay, base_delay * 2 ** max(attempts - 1, 0))
    # 功能层: 核心公式——delay = min(max_delay, base_delay * 2^(attempts-1))
    # 设计层: max(attempts-1, 0)保证attempts=0时首次重试不会产生0.5倍延迟
    if jitter:
        backoff = backoff * random()
        # 功能层: 添加随机抖动，防止多个任务同时重试导致的资源竞争
    return backoff


async def cancel_tasks(
    # 功能层: 取消一组异步任务并等待它们全部完成
    # 设计层: 先发送cancel信号，再用gather等待所有任务结束，支持超时控制
    # 上下文层: Worker在stop()和process()中调用此函数取消正在执行的任务

    tasks: Iterable[asyncio.Task],
    # 功能层: 要取消的任务可迭代对象
    timeout: float | None = 1.0,
    # 功能层: 等待任务完成的超时时间（秒），None表示无限等待
) -> bool:
    for task in tasks:
        task.cancel()
        # 功能层: 向每个任务发送CancelledError信号

    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout)
    # 功能层: 等待所有任务完成，return_exceptions=True让异常不会中断等待
    # 设计层: gather+wait_for组合做到在超时内等待所有任务完成
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
        # 功能层: 超时或自身被取消时静默处理
    return all(task.done() for task in tasks)
    # 功能层: 返回是否所有任务都已完成
