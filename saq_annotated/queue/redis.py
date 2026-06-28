# ============================================================================
# 模块: saq/queue/redis.py
# 功能层: 基于Redis的Queue实现，用Redis数据结构（List、Sorted Set、PubSub）搭任务队列
# 设计层: 继承Queue抽象基类，靠Lua脚本保证原子性，BLMOVE做低延迟出队
# 上下文层: 这是SAQ最常用的队列后端，通过Queue.from_url("redis://...")创建
# ============================================================================

"""
Redis Queue
"""

from __future__ import annotations

import asyncio
# 功能层: 异步I/O库，用于Semaphore和事件循环
import logging
# 功能层: 日志模块
import json
# 功能层: JSON序列化，用于Worker信息的序列化
import time
# 功能层: 时间模块，用于计算重试延迟的计划时间
import typing as t
# 功能层: 类型注解工具

from saq.errors import MissingDependencyError
# 功能层: 依赖缺失异常
from saq.job import Job, Status
# 功能层: 核心数据类
from saq.multiplexer import Multiplexer
# 功能层: 多路复用器基类，PubSubMultiplexer继承此类
from saq.queue.base import Queue, logger
# 功能层: 导入Queue抽象基类
from saq.utils import millis, now, now_seconds
# 功能层: 工具函数

try:
    from redis import asyncio as aioredis
    # 功能层: 导入redis-py的异步客户端
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Redis. Install them with `pip install saq[redis]`."
    ) from e
    # 功能层: 如果未安装redis包，抛出友好的错误提示

if t.TYPE_CHECKING:
    from collections.abc import Iterable
    from redis.asyncio.client import Redis, PubSub
    from redis.commands.core import AsyncScript
    from saq.types import CountKind, DumpType, ListenCallback, LoadType, QueueInfo, VersionTuple, WorkerInfo

ID_PREFIX = "saq:job:"
# 功能层: Redis中Job键的全局前缀，用于命名空间隔离


class RedisQueue(Queue):
    # 功能层: 基于Redis的队列实现，使用Redis的多种数据结构管理任务生命周期
    # 设计层: 用Redis List管排队/活跃任务，Sorted Set管调度/不完整任务，PubSub做实时通知
    # 上下文层: Redis中的键命名规则为 saq:{queue_name}:{data_type}

    @classmethod
    def from_url(cls: type[RedisQueue], url: str, **kwargs: t.Any) -> RedisQueue:
        # 功能层: 类方法工厂，通过Redis URL创建队列实例
        # 设计层: 使用classmethod确保子类调用时返回正确类型
        return cls(aioredis.from_url(url), **kwargs)

    def __init__(self, redis: Redis[bytes], name: str = "default", dump: DumpType | None = None, load: LoadType | None = None, max_concurrent_ops: int = 20, swept_error_message: str | None = None) -> None:
        super().__init__(name=name, dump=dump, load=load, swept_error_message=swept_error_message)
        self.redis = redis
        # 功能层: Redis异步客户端实例
        self._version: VersionTuple | None = None
        # 功能层: 缓存Redis版本号，用于判断是否支持BLMOVE命令
        self._schedule_script: AsyncScript | None = None
        self._enqueue_script: AsyncScript | None = None
        self._cleanup_script: AsyncScript | None = None
        # 功能层: 缓存Lua脚本对象，避免重复注册
        # 设计层: 延迟初始化(Lazy Initialization)，首次使用时才注册脚本
        self._incomplete = self.namespace("incomplete")
        # 功能层: Sorted Set键，存储所有未完成的Job ID，score为计划执行时间
        self._queued = self.namespace("queued")
        # 功能层: List键，存储等待被消费的Job ID（FIFO队列）
        self._active = self.namespace("active")
        # 功能层: List键，存储正在被Worker处理的Job ID
        self._schedule = self.namespace("schedule")
        # 功能层: 调度锁键，防止多个Worker同时调度
        self._sweep = self.namespace("sweep")
        # 功能层: 清扫锁键，防止多个Worker同时清扫
        self._stats = self.namespace("stats")
        # 功能层: Sorted Set键，存储Worker信息的键和过期时间
        self._op_sem = asyncio.Semaphore(max_concurrent_ops)
        # 功能层: 信号量，限制并发Redis操作数，防止连接池耗尽
        # 设计层: 限流保护Redis连接资源
        self._pubsub = PubSubMultiplexer(redis.pubsub(), prefix=f"{ID_PREFIX}{self.name}")
        # 功能层: PubSub多路复用器，通过单一Redis连接监听所有Job的状态变化

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<redis={self.redis}, name='{self.name}'>"

    def job_id(self, job_key: str) -> str:
        # 功能层: 将Job key转换为Redis键，格式为 saq:job:{queue_name}:{job_key}
        return f"{ID_PREFIX}{self.name}:{job_key}"

    def job_key_from_id(self, job_id: str) -> str:
        # 功能层: 从Redis键中提取Job key
        return job_id[len(f"{ID_PREFIX}{self.name}:"):]

    def namespace(self, key: str) -> str:
        # 功能层: 生成命名空间化的Redis键，格式为 saq:{queue_name}:{key}
        return ":".join(["saq", self.name, key])

    async def disconnect(self) -> None:
        # 功能层: 断开Redis连接——关闭PubSub、关闭客户端、断开连接池
        await self._pubsub.close()
        if hasattr(self.redis, "aclose"):
            await self.redis.aclose()
        else:
            await self.redis.close()
        await self.redis.connection_pool.disconnect()

    async def version(self) -> VersionTuple:
        # 功能层: 获取Redis服务器版本号
        # 设计层: 延迟获取并缓存，用于判断是否支持BLMOVE（6.2+）
        if self._version is None:
            info = await self.redis.info()
            self._version = tuple(int(i) for i in str(info["redis_version"]).split("."))
        return self._version

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        # 功能层: 获取队列的完整信息——Worker列表、任务计数、任务详情
        worker_uuids = []
        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key_str = key.decode("utf-8")
            *_, worker_uuid = key_str.split(":")
            worker_uuids.append(worker_uuid)
        # 功能层: 从Sorted Set中获取未过期的Worker键

        worker_metadata = await self.redis.mget(
            self.namespace(f"worker_info:{worker_uuid}") for worker_uuid in worker_uuids
        )
        workers: dict[str, WorkerInfo] = {}
        worker_metadata_dict = dict(zip(worker_uuids, worker_metadata))
        for worker in worker_uuids:
            metadata = worker_metadata_dict.get(worker)
            if metadata:
                workers[worker] = json.loads(metadata)

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            deserialized_jobs = (
                self.deserialize(job_bytes)
                for job_bytes in await self.redis.mget(
                    (await self.redis.lrange(self._active, offset, limit - 1))
                    + (await self.redis.zrange(self._incomplete, offset, limit - 1, withscores=False))
                )
            )
            job_info = list({job["key"]: job for job in (job.to_dict() for job in deserialized_jobs if job is not None)}.values())
        else:
            job_info = []

        return {
            "workers": workers, "name": self.name,
            "queued": queued, "active": active,
            "scheduled": incomplete - queued - active,
            "jobs": job_info,
        }

    async def count(self, kind: CountKind) -> int:
        # 功能层: 统计指定类型的任务数量
        if kind == "queued":
            return await self.redis.llen(self._queued)
        if kind == "active":
            return await self.redis.llen(self._active)
        if kind == "incomplete":
            return await self.redis.zcard(self._incomplete)
        raise ValueError("Can't count unknown type {kind}")

    async def schedule(self, lock: int = 1) -> t.List[str]:
        # 功能层: 调度到期的任务——通过Lua脚本原子地将到期的任务从incomplete移到queued
        # 设计层: Lua脚本在Redis中原子执行，避免竞态条件
        if not self._schedule_script:
            self._schedule_script = self.redis.register_script("""
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])
                    for _, v in ipairs(jobs) do
                        redis.call('ZADD', KEYS[2], 0, v)
                        redis.call('RPUSH', KEYS[3], v)
                    end
                    return jobs
                end
            """)
        return [
            job_id.decode("utf-8")
            for job_id in await self._schedule_script(
                keys=[self._schedule, self._incomplete, self._queued],
                args=[lock, now_seconds()],
            ) or []
        ]

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        # 功能层: 清扫卡住的任务——检查活跃列表中的任务是否超时或丢失
        # 设计层: Lua脚本原子获取活跃列表，Python端判断是否需要清扫
        if not self._cleanup_script:
            self._cleanup_script = self.redis.register_script("""
                local id_jobs = {}
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    for i, v in ipairs(redis.call('LRANGE', KEYS[2], 0, -1)) do
                        id_jobs[i] = {v, redis.call('GET', v)}
                    end
                end
                return id_jobs
            """)

        id_jobs = await self._cleanup_script(keys=[self._sweep, self._active], args=[lock], client=self.redis)
        swept = []
        for job_id, job_bytes in id_jobs:
            job = self.deserialize(job_bytes)
            if job:
                if job.status != Status.ACTIVE or job.stuck:
                    swept.append(job_id)
                    await self.abort(job, error=self.swept_error_message)
                    try:
                        await job.refresh(abort)
                    except asyncio.TimeoutError:
                        logger.info("Could not abort job %s", job_id)
                    if job.retryable:
                        await self.retry(job, error=self.swept_error_message)
                    else:
                        await self.finish(job, Status.ABORTED, error=self.swept_error_message)
            else:
                swept.append(job_id)
                async with self.redis.pipeline(transaction=True) as pipe:
                    await (pipe.lrem(self._active, 0, job_id).zrem(self._incomplete, job_id).execute())
        return [job_id.decode("utf-8") for job_id in swept]

    async def notify(self, job: Job) -> None:
        # 功能层: 通过Redis PubSub发布任务状态变更通知
        await self.redis.publish(job.id, job.status)

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        # 功能层: 更新Redis中的Job数据并发送通知
        if not status:
            stored = await self.job(job.key)
            status = stored.status if stored else None
        job.status = status or job.status
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key: str) -> Job | None:
        return await self._get_job_by_id(self.job_id(job_key))

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        # 功能层: 批量获取Job，使用MGET实现高效批量读取
        return [self.deserialize(job_bytes) for job_bytes in await self.redis.mget(self.job_id(key) for key in job_keys)]

    async def iter_jobs(self, statuses: t.List[Status] = list(Status), batch_size: int = 100) -> t.AsyncIterator[Job]:
        # 功能层: 使用SCAN命令分批遍历所有Job键
        # 设计层: SCAN而非KEYS，避免阻塞Redis
        cursor = 0
        while True:
            cursor, job_ids = await self.redis.scan(cursor=cursor, match=self.job_id("*"), count=batch_size)
            statuses_set = set(statuses)
            for job in await self.jobs(self.job_key_from_id(job_id.decode("utf-8")) for job_id in job_ids):
                if job and job.status in statuses_set:
                    yield job
            if cursor <= 0:
                break

    async def _get_job_by_id(self, job_id: bytes | str) -> Job | None:
        # 功能层: 通过信号量限流后从Redis获取Job
        async with self._op_sem:
            return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        # 功能层: 中止任务——使用Pipeline事务原子地更新多个Redis键
        # 设计层: Pipeline保证多个Redis命令的原子性，也减少网络往返
        async with self._op_sem:
            async with self.redis.pipeline(transaction=True) as pipe:
                job.status = Status.ABORTING
                job.error = error
                dequeued, *_ = await (
                    pipe.lrem(self._queued, 0, job.id)
                    .zrem(self._incomplete, job.id)
                    .set(job.id, self.serialize(job))
                    .setex(job.abort_id, ttl, error)
                    .publish(job.id, job.status)
                    .execute()
                )
            if dequeued:
                await self.finish(job, Status.ABORTED, error=error)
                await self.redis.delete(job.abort_id)
            else:
                await self.redis.lrem(self._active, 0, job.id)

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        # 功能层: 从队列中取出一个任务——使用BLMOVE（Redis 6.2+）或BRPOPLPUSH实现阻塞式出队
        # 设计层: 阻塞式出队不用轮询，延迟低于5ms，比ARQ的0.5秒轮询快两个数量级
        if await self.version() < (6, 2, 0):
            job_id = await self.redis.brpoplpush(self._queued, self._active, timeout)
        else:
            job_id = await self.redis.blmove(self._queued, self._active, timeout, "LEFT", "RIGHT")
        if job_id is not None:
            return await self._get_job_by_id(job_id)
        logger.debug("Dequeue timed out")
        return None

    async def listen(self, job_keys: Iterable[str], callback: ListenCallback, timeout: float | None = 10, poll_interval: float = 0.5) -> None:
        # 功能层: 通过PubSub多路复用器监听任务状态变化
        job_ids = [self.job_id(job_key) for job_key in job_keys]
        if not job_ids:
            return
        async for message in self._pubsub.listen(*job_ids, timeout=timeout):
            job_id = message["channel"]
            job_key = self.job_key_from_id(job_id)
            status = Status[message["data"].decode("utf-8").upper()]
            if asyncio.iscoroutinefunction(callback):
                stop = await callback(job_key, status)
            else:
                stop = callback(job_key, status)
            if stop:
                break

    async def finish_abort(self, job: Job) -> None:
        # 功能层: 完成中止——删除abort键后调用父类方法
        await self.redis.delete(job.abort_id)
        await super().finish_abort(job)

    async def write_worker_info(self, worker_id: str, info: WorkerInfo, ttl: int) -> None:
        # 功能层: 写入Worker信息——使用Pipeline事务原子地更新多个键
        current = now()
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self.namespace(f"worker_info:{worker_id}")
            await (
                pipe.setex(key, ttl, json.dumps(info))
                .zremrangebyscore(self._stats, 0, current)
                .zadd(self._stats, {key: current + millis(ttl)})
                .expire(self._stats, ttl)
                .execute()
            )

    async def _retry(self, job: Job, error: str | None) -> None:
        # 功能层: 重试任务——从活跃列表移除，根据延迟决定立即入队还是延迟调度
        job_id = job.id
        next_retry_delay = job.next_retry_delay()
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe = pipe.lrem(self._active, 1, job_id)
            pipe = pipe.lrem(self._queued, 1, job_id)
            if next_retry_delay:
                scheduled = time.time() + next_retry_delay
                pipe = pipe.zadd(self._incomplete, {job_id: scheduled})
            else:
                pipe = pipe.zadd(self._incomplete, {job_id: job.scheduled})
                pipe = pipe.rpush(self._queued, job_id)
            await pipe.set(job_id, self.serialize(job)).execute()
            await self.notify(job)

    async def _finish(self, job: Job, status: Status, *, result: t.Any = None, error: str | None = None) -> None:
        # 功能层: 完成任务——从活跃/不完整集合中移除，根据TTL设置过期或永久存储
        job_id = job.id
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe = pipe.lrem(self._active, 1, job_id).zrem(self._incomplete, job_id)
            if job.ttl > 0:
                pipe = pipe.setex(job_id, job.ttl, self.serialize(job))
            elif job.ttl == 0:
                pipe = pipe.set(job_id, self.serialize(job))
            else:
                pipe.delete(job_id)
            await pipe.execute()
            await self.notify(job)

    async def _enqueue(self, job: Job) -> Job | None:
        # 功能层: 入队任务——通过Lua脚本原子地检查去重并写入Redis
        # 设计层: Lua脚本保证"检查是否存在+写入"的原子性，避免重复入队
        if not self._enqueue_script:
            self._enqueue_script = self.redis.register_script("""
                if not redis.call('ZSCORE', KEYS[1], KEYS[2]) and redis.call('EXISTS', KEYS[4]) == 0 then
                    redis.call('SET', KEYS[2], ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
                    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
                    return 1
                else
                    return nil
                end
            """)
        async with self._op_sem:
            if not await self._enqueue_script(
                keys=[self._incomplete, job.id, self._queued, job.abort_id],
                args=[self.serialize(job), job.scheduled],
                client=self.redis,
            ):
                return None
        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job


class PubSubMultiplexer(Multiplexer):
    # 功能层: 基于Redis PubSub的多路复用器，通过单一Redis连接监听所有Job状态变化
    # 设计层: 使用psubscribe模式匹配监听所有以队列前缀开头的频道，在进程内路由消息
    # 上下文层: 不用为每个Job开一个PubSub连接，省下大量Redis连接

    def __init__(self, pubsub: PubSub, prefix: str) -> None:
        super().__init__()
        self.prefix = prefix
        self.pubsub = pubsub

    async def _start(self) -> None:
        # 功能层: 启动PubSub监听——psubscribe模式匹配后持续消费消息
        await self.pubsub.psubscribe(f"{self.prefix}*")
        while True:
            try:
                message = await self.pubsub.get_message(timeout=None)
                if message and message["type"] == "pmessage":
                    message["channel"] = message["channel"].decode("utf-8")
                    self.publish(message["channel"], message)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Failed to consume message")

    async def _close(self) -> None:
        # 功能层: 取消PubSub订阅
        await self.pubsub.punsubscribe()
