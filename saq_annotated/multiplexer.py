# ============================================================================
# 模块: saq/multiplexer.py
# 功能层: 通知多路复用器(Multiplexer)，在单一监听通道上广播多个频道的消息
# 设计层: 用抽象基类(ABC)定义接口，发布-订阅(Pub/Sub)模式分发消息
# 上下文层: RedisQueue中的PubSubMultiplexer继承此基类，将Redis的PubSub消息路由到多个内部消费者
# ============================================================================

"""
Notification Multiplexer used to listen to notifications on one channel and broadcast.
"""

from __future__ import annotations

import asyncio
# 功能层: 导入asyncio，用于异步队列和任务管理
import typing as t
# 功能层: 类型注解工具
from abc import ABC, abstractmethod
# 功能层: 导入抽象基类工具，用于定义Multiplexer的抽象接口
# 设计层: ABC保证Multiplexer不能直接实例化，子类必须实现_start()方法
from collections import defaultdict
# 功能层: 导入默认字典，用于自动初始化订阅关系中的空集合


if t.TYPE_CHECKING:
    Q = asyncio.Queue[dict]
    # 功能层: 类型别名，表示存储字典消息的异步队列


class Multiplexer(ABC):
    # 功能层: 通知多路复用器抽象基类，管理多个频道到多个消费者的消息路由
    # 设计层: 用ABC+abstractmethod定义接口契约，子类只需实现_start()和可选的_close()
    # 上下文层: PubSubMultiplexer(Redis)继承此类，实现基于Redis PubSub的具体多路复用

    def __init__(self) -> None:
        self._subscriptions: t.Dict[str, t.Set[Q]] = defaultdict(set)
        # 功能层: 频道到订阅者队列的映射，一个频道可以有多个订阅者
        self._queues: t.Dict[Q, t.Set[str]] = defaultdict(set)
        # 功能层: 队列到频道的反向映射，用于取消订阅时快速查找
        # 设计层: 双向映射让订阅和取消订阅都是O(1)
        self._daemon_task: t.Optional[asyncio.Task] = None
        # 功能层: 后台守护任务，持续监听消息并分发
        self._lock = asyncio.Lock()
        # 功能层: 异步锁，保护start/close操作的并发安全

    async def start(self) -> None:
        # 功能层: 启动多路复用器，创建后台监听任务
        # 设计层: 加锁保证只启动一次，幂等操作
        async with self._lock:
            if not self._daemon_task:
                self._daemon_task = asyncio.create_task(self._start())

    @abstractmethod
    async def _start(self) -> None: ...
    # 功能层: 抽象方法，子类实现具体的消息监听逻辑

    async def _close(self) -> None:
        # 功能层: 子类可覆写的关闭钩子
        pass

    async def close(self) -> None:
        # 功能层: 关闭多路复用器，取消守护任务并清理所有订阅
        async with self._lock:
            if self._daemon_task:
                self._daemon_task.cancel()
                self._daemon_task = None
            await self._close()
            self._subscriptions.clear()
            self._queues.clear()

    async def listen(self, *channels: str, timeout: float | None = None) -> t.AsyncGenerator:
        # 功能层: 异步生成器，监听指定频道的消息并逐条yield
        # 设计层: 用async generator模式，调用者能用async for语法消费消息流
        # 上下文层: RedisQueue.listen()通过此方法监听Job状态变化通知
        queue = await self.subscribe(*channels)
        try:
            while self._daemon_task and not self._daemon_task.done():
                try:
                    yield await asyncio.wait_for(queue.get(), timeout if timeout else 1.0)
                    queue.task_done()
                except asyncio.TimeoutError:
                    if timeout:
                        raise
        finally:
            await self.unsubscribe(queue)

    def publish(self, channel: str, message: t.Any) -> None:
        # 功能层: 向指定频道的所有订阅者发布消息
        # 设计层: 用put_nowait非阻塞入队，避免消息分发时卡住监听循环
        for queue in self._subscriptions[channel]:
            queue.put_nowait(message)

    async def subscribe(self, *channels: str) -> Q:
        # 功能层: 订阅指定频道，返回一个异步队列用于接收消息
        await self.start()
        queue: Q = asyncio.Queue()
        for channel in channels:
            self._queues[queue].add(channel)
            self._subscriptions[channel].add(queue)
        return queue

    async def unsubscribe(self, queue: Q) -> None:
        # 功能层: 取消订阅，从所有频道中移除指定队列
        # 设计层: 当没有剩余订阅者时自动关闭多路复用器，释放资源
        for channel in self._queues.pop(queue, []):
            self._subscriptions[channel].remove(queue)
        if not self._queues:
            await self.close()
