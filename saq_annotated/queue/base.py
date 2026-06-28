# ============================================================================
# 模块: saq/queue/base.py
# 功能层: 定义Queue抽象基类，提供任务队列的核心操作接口（入队、出队、重试、完成、中止等）
# 设计层: 用ABC(抽象基类)+模板方法模式，公共方法跑通用逻辑，抽象方法交给子类去实现具体存储
# 上下文层: RedisQueue、PostgresQueue、HttpQueue都继承这个基类，是SAQ队列系统的核心抽象
# ============================================================================

"""
Base Queue class
"""

from __future__ import annotations
# 功能层: 启用延迟注解求值，避免循环导入

import asyncio
# 功能层: 异步I/O库，用于事件循环和异步上下文管理器
import json
# 功能层: JSON序列化/反序列化库，作为默认的Job序列化方案
import logging
# 功能层: 日志模块
import typing as t
# 功能层: 类型注解工具
from abc import ABC, abstractmethod
# 功能层: 抽象基类工具，定义Queue的接口契约
# 设计层: ABC保证Queue不能直接实例化，得用具体子类（如RedisQueue）
from contextlib import asynccontextmanager
# 功能层: 异步上下文管理器装饰器，用于实现batch()方法
# 设计层: @asynccontextmanager将异步生成器转换为上下文管理器，支持async with语法
from urllib.parse import urlparse
# 功能层: URL解析工具，用于from_url()工厂方法

from saq.errors import InvalidUrlError
# 功能层: 导入无效URL异常
from saq.job import (
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
    Job,
    Status,
    get_default_job_key,
)
# 功能层: 导入Job相关的核心类和常量
from saq.utils import now
# 功能层: 导入时间戳工具函数

if t.TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence
    from saq.types import (
        BeforeEnqueueType, CountKind, DumpType, ListenCallback,
        LoadType, QueueInfo, WorkerInfo, WorkerStats,
    )


logger = logging.getLogger("saq")
# 功能层: 创建SAQ专用的日志记录器

DEFAULT_SWEPT_JOB_ERROR = "swept"
# 功能层: 清扫卡住任务时使用的默认错误消息


class JobError(Exception):
    # 功能层: 任务执行失败的异常类，包含失败Job的详细信息
    # 设计层: 继承Exception，封装Job对象以便调用者访问失败任务的详情
    # 上下文层: Queue.apply()和Queue.map()在任务失败时抛出此异常

    def __init__(self, job: Job) -> None:
        super().__init__(
            f"Job {job.id} {job.status}\n\nThe above job failed with the following error:\n\n{job.error}"
        )
        self.job = job
        # 功能层: 保存Job引用，允许调用者检查失败任务的完整信息


class Queue(ABC):
    # 功能层: 队列抽象基类，定义了任务队列的完整操作接口
    # 设计层: 采用模板方法模式——enqueue/retry/finish等公共方法跑通用业务逻辑，
    #          _enqueue/_retry/_finish等抽象方法交给子类去实现具体的存储操作
    # 上下文层: 所有队列后端（Redis/Postgres/HTTP）都实现这个接口

    def __init__(
        self,
        name: str,
        dump: DumpType | None,
        load: LoadType | None,
        swept_error_message: str | None = None,
    ) -> None:
        # 功能层: 初始化队列的基本属性
        self.name = name
        # 功能层: 队列名称，用于Redis键的命名空间隔离
        self.started: int = now()
        # 功能层: 队列启动时间戳，用于计算运行时间
        self.complete = 0
        self.failed = 0
        self.retried = 0
        self.aborted = 0
        # 功能层: 各类任务的计数器，用于统计和监控
        self._dump = dump or json.dumps
        self._load = load or json.loads
        # 功能层: 序列化/反序列化函数，默认使用JSON
        # 设计层: 用户可以自定义序列化器（如msgpack、pickle）
        self._swept_error_message = swept_error_message or DEFAULT_SWEPT_JOB_ERROR
        self._before_enqueues: dict[int, BeforeEnqueueType] = {}
        # 功能层: 入队前的回调函数注册表，以函数id为键
        # 上下文层: batch()上下文管理器使用此机制跟踪批量入队的任务
        self._loop: asyncio.AbstractEventLoop | None = None
        # 功能层: 缓存事件循环引用

    def job_id(self, job_key: str) -> str:
        # 功能层: 将Job的key转换为存储层的完整ID
        # 设计层: 基类默认直接返回key，子类（如RedisQueue）可添加前缀
        return job_key

    @property
    def swept_error_message(self) -> str:
        return self._swept_error_message

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        # 功能层: 获取当前事件循环，优先使用缓存的引用
        return self._loop or asyncio.get_running_loop()

    @abstractmethod
    async def disconnect(self) -> None:
        # 功能层: 抽象方法——断开与后端存储的连接
        pass

    @abstractmethod
    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        # 功能层: 抽象方法——获取队列信息（Worker状态、任务计数等）
        pass

    @abstractmethod
    async def count(self, kind: CountKind) -> int:
        # 功能层: 抽象方法——统计指定类型的任务数量
        pass

    async def schedule(self, _lock: int = 1) -> t.List[str]:
        # 功能层: 调度定时任务，将到期的任务从调度集合移入队列
        # 设计层: 基类提供空实现，RedisQueue通过Lua脚本实现原子调度
        return []

    @abstractmethod
    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        # 功能层: 抽象方法——清扫卡住的任务
        pass

    async def notify(self, job: Job) -> None:
        # 功能层: 发送任务状态变更通知，基类为空实现
        # 上下文层: RedisQueue通过PubSub实现实时通知
        pass

    async def update(self, job: Job, **kwargs: t.Any) -> None:
        # 功能层: 更新Job的属性并持久化到存储后端
        # 设计层: 模板方法——先更新内存中的属性，再调用子类的_update()持久化
        # 上下文层: Worker在process()中调用此方法将任务状态更新为ACTIVE
        job.touched = now()
        for k, v in kwargs.items():
            if hasattr(job, k):
                setattr(job, k, v)
        await self._update(self.copy(job), **kwargs)

    @abstractmethod
    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        # 功能层: 抽象方法——子类实现具体的持久化逻辑
        pass

    @abstractmethod
    async def job(self, job_key: str) -> Job | None:
        # 功能层: 抽象方法——根据key获取Job
        pass

    @abstractmethod
    async def jobs(self, job_keys: t.Iterable[str]) -> t.List[Job | None]:
        # 功能层: 抽象方法——批量获取多个Job
        pass

    @abstractmethod
    def iter_jobs(self, statuses: t.List[Status] = list(Status), batch_size: int = 100) -> t.AsyncIterator[Job]:
        # 功能层: 抽象方法——异步迭代器，逐批遍历所有Job
        pass

    @abstractmethod
    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        # 功能层: 抽象方法——中止任务
        pass

    @abstractmethod
    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        # 功能层: 抽象方法——从队列中取出一个任务
        pass

    async def finish_abort(self, job: Job) -> None:
        # 功能层: 完成中止操作，将任务标记为ABORTED终态
        await job.finish(Status.ABORTED, error=job.error)

    @abstractmethod
    async def write_worker_info(self, worker_id: str, info: WorkerInfo, ttl: int) -> None:
        # 功能层: 抽象方法——写入Worker的统计信息和元数据
        pass

    @abstractmethod
    async def _retry(self, job: Job, error: str | None) -> None:
        # 功能层: 抽象方法——子类实现具体的重试存储逻辑
        pass

    @abstractmethod
    async def _finish(self, job: Job, status: Status, *, result: t.Any = None, error: str | None = None) -> None:
        # 功能层: 抽象方法——子类实现具体的完成存储逻辑
        pass

    @abstractmethod
    async def _enqueue(self, job: Job) -> Job | None:
        # 功能层: 抽象方法——子类实现具体的入队存储逻辑
        pass

    @staticmethod
    def from_url(url: str, **kwargs: t.Any) -> Queue:
        # 功能层: 工厂方法，根据URL协议自动创建对应类型的Queue实例
        # 设计层: 静态工厂方法+延迟导入，支持redis://、postgres://、http://三种协议
        # 上下文层: 用户最常用的创建Queue的方式，如Queue.from_url("redis://localhost")
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        if scheme.startswith("redis"):
            from saq.queue.redis import RedisQueue
            return RedisQueue.from_url(url, **kwargs)
        elif scheme.startswith("postgres"):
            from saq.queue.postgres import PostgresQueue
            return PostgresQueue.from_url(url, **kwargs)
        elif scheme.startswith("http"):
            from saq.queue.http import HttpQueue
            return HttpQueue.from_url(url, **kwargs)
        else:
            raise InvalidUrlError(f"Invalid url: {url}")

    async def connect(self) -> None:
        # 功能层: 建立与后端存储的连接，缓存事件循环引用
        self._loop = asyncio.get_running_loop()

    def copy(self, job: Job) -> Job:
        # 功能层: 通过序列化-反序列化创建Job的深拷贝
        return self.deserialize(job.to_dict())

    def serialize(self, job: Job) -> bytes | str:
        # 功能层: 将Job序列化为字节或字符串
        return self._dump(job.to_dict())

    def deserialize(self, payload: dict | str | bytes | None, status: Status | str | None = None) -> Job | None:
        # 功能层: 将存储层的数据反序列化为Job对象
        # 设计层: 支持dict、str、bytes多种输入格式
        if not payload:
            return None
        job_dict = payload if isinstance(payload, dict) else self._load(payload)
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        if status:
            job_dict["status"] = status
        return Job(**job_dict, queue=self)

    async def worker_info(self, worker_id: str, queue_key: str, metadata: t.Optional[dict] = None, ttl: int = 60) -> WorkerInfo:
        # 功能层: 构建并写入Worker的运行时信息（统计数据+元数据）
        # 上下文层: Worker的upkeep任务定期调用此方法更新自身状态
        stats: WorkerStats = {
            "complete": self.complete, "failed": self.failed,
            "retried": self.retried, "aborted": self.aborted,
            "uptime": now() - self.started,
        }
        info: WorkerInfo = {"stats": stats, "queue_key": queue_key, "metadata": metadata}
        await self.write_worker_info(worker_id, info, ttl=ttl)
        return info

    def register_before_enqueue(self, callback: BeforeEnqueueType) -> None:
        # 功能层: 注册入队前的回调函数
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback: BeforeEnqueueType) -> None:
        # 功能层: 注销入队前的回调函数
        self._before_enqueues.pop(id(callback), None)

    async def retry(self, job: Job, error: str | None) -> None:
        # 功能层: 重试任务——重置状态为QUEUED，清除运行时数据，调用子类的_retry()持久化
        # 设计层: 模板方法——公共逻辑（重置状态）+ 抽象方法（存储操作）
        job.status = Status.QUEUED
        job.error = error
        job.completed = 0
        job.started = 0
        job.progress = 0
        job.touched = now()
        await self._retry(job=job, error=error)
        self.retried += 1
        logger.info("Retrying %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def finish(self, job: Job, status: Status, *, result: t.Any = None, error: str | None = None, **kwargs: t.Any) -> None:
        # 功能层: 完成任务——设置终态、结果、错误信息，更新计数器
        # 设计层: 模板方法——公共逻辑（设置状态+计数）+ 抽象方法（存储操作）
        job.status = status
        job.result = result
        job.error = error
        job.completed = now()
        if status == Status.COMPLETE:
            job.progress = 1.0
        await self._finish(job=job, status=status, result=result, error=error, **kwargs)
        logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        if status == Status.COMPLETE:
            self.complete += 1
        elif status == Status.FAILED:
            self.failed += 1
        elif status == Status.ABORTED:
            self.aborted += 1

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job | None:
        # 功能层: 入队任务——支持传入函数名字符串或Job实例，智能分离Job属性和函数参数
        # 设计层: 通过检查kwargs是否在Job.__dataclass_fields__中来区分Job属性和函数参数
        # 上下文层: 用户经常用这个API
        job_kwargs: dict[str, t.Any] = {}
        for k, v in kwargs.items():
            if k in Job.__dataclass_fields__:
                job_kwargs[k] = v
            else:
                job_kwargs.setdefault("kwargs", {})[k] = v

        if isinstance(job_or_func, str):
            job = Job(function=job_or_func, **job_kwargs)
        else:
            job = job_or_func
            for k, v in job_kwargs.items():
                setattr(job, k, v)

        if job.queue and job.queue.name != self.name:
            raise ValueError(f"Job {job} registered to a different queue")

        job.queue = self
        job.queued = now()
        job.status = Status.QUEUED
        await self._before_enqueue(job)
        return await self._enqueue(job)

    async def listen(self, job_keys: Iterable[str], callback: ListenCallback, timeout: float | None = 10, poll_interval: float = 0.5) -> None:
        # 功能层: 监听指定任务的状态变化，当回调返回True时停止监听
        # 设计层: 支持同步和异步回调，通过iscoroutinefunction判断
        async def listen() -> None:
            while True:
                for job in await self.jobs(job_keys):
                    if not job:
                        continue
                    if asyncio.iscoroutinefunction(callback):
                        stop = await callback(job.id, job.status)
                    else:
                        stop = callback(job.id, job.status)
                    if stop:
                        return
                await asyncio.sleep(poll_interval)

        if timeout:
            await asyncio.wait_for(listen(), timeout)
        else:
            await listen()

    async def apply(self, job_or_func: str, timeout: float | None = None, poll_interval: float = 0.5, **kwargs: t.Any) -> t.Any:
        # 功能层: 入队任务并同步等待其结果，成功返回结果，失败抛出JobError
        # 设计层: 委托给map()处理，简化单任务等待的逻辑
        results = await self.map(job_or_func, timeout=timeout, poll_interval=poll_interval, iter_kwargs=[kwargs])
        if results:
            return results[0]
        return None

    async def map(self, job_or_func: str | Job, iter_kwargs: Sequence[dict[str, t.Any]], timeout: float | None = None, return_exceptions: bool = False, poll_interval: float = 0.5, **kwargs: t.Any) -> list[t.Any]:
        # 功能层: 批量入队任务并收集所有结果，类似Python内置的map()
        # 设计层: 用asyncio.gather并发入队，轮询检查完成状态
        iter_kwargs = [
            {"timeout": timeout, "key": kwargs.get("key", "") or get_default_job_key(), **kwargs, **kw}
            for kw in iter_kwargs
        ]

        async def _map() -> list[t.Any]:
            await asyncio.gather(*(self.enqueue(job_or_func, **kw) for kw in iter_kwargs))
            incomplete = object()
            results = {key["key"]: incomplete for key in iter_kwargs}
            while remaining := [k for k, v in results.items() if v is incomplete]:
                for key, job in zip(remaining, await self.jobs(remaining)):
                    if not job:
                        results[key] = None
                    elif job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                        exc = JobError(job)
                        if not return_exceptions:
                            raise exc
                        results[key] = exc
                    elif job.status in TERMINAL_STATUSES:
                        results[key] = job.result
                await asyncio.sleep(poll_interval)
            return list(results.values())

        return await asyncio.wait_for(_map(), timeout)

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[None]:
        # 功能层: 异步上下文管理器，批量入队任务，异常时自动中止所有已入队的任务
        # 设计层: 用@asynccontextmanager把异步生成器转成上下文管理器
        # 上下文层: 用户用async with queue.batch()做事务性批量操作
        children = set()

        async def track_child(job: Job) -> None:
            children.add(job)

        self.register_before_enqueue(track_child)
        try:
            yield
        except Exception:
            await asyncio.gather(
                *[self.abort(child, "cancelled") for child in children],
                return_exceptions=True,
            )
            raise
        finally:
            self.unregister_before_enqueue(track_child)

    async def _before_enqueue(self, job: Job) -> None:
        # 功能层: 执行所有注册的入队前回调
        for cb in self._before_enqueues.values():
            await cb(job)
