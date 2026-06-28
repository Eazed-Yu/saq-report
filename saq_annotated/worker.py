# ============================================================================
# 模块: saq/worker.py
# 功能层: 定义Worker类——SAQ的任务执行引擎，负责从Queue中取出Job并执行，管理生命周期钩子和维护任务
# 设计层: 用泛型(Generic[CtxType])支持自定义上下文，信号处理做优雅关闭，ThreadPoolExecutor兼容同步函数
# 上下文层: Worker是SAQ的运行引擎，一边连Queue（数据源），一边连用户函数（业务逻辑）
# ============================================================================

"""
Workers
"""

from __future__ import annotations
# 功能层: 启用延迟注解求值

import asyncio
# 功能层: 异步I/O核心库，提供事件循环、Task、Event、Lock等并发原语
import contextvars
# 功能层: 上下文变量模块，用于在同步函数包装器中传播异步上下文
# 设计层: contextvars.copy_context()让线程池里的同步函数也能拿到异步上下文变量
import logging
# 功能层: 日志模块
import os
# 功能层: 操作系统接口，用于判断操作系统类型（信号处理兼容性）
import signal
# 功能层: 信号处理模块，用于捕获SIGINT/SIGTERM实现优雅关闭
import sys
# 功能层: 系统模块，用于版本检查和退出
import traceback
# 功能层: 堆栈跟踪模块，用于捕获异常信息
import threading
# 功能层: 线程模块，用于burst模式下的线程安全计数
import typing as t
# 功能层: 类型注解工具
import typing_extensions as te
# 功能层: typing_extensions提供ParamSpec等高级类型工具
from concurrent.futures import ThreadPoolExecutor
# 功能层: 线程池执行器，用于在异步环境中执行同步函数
# 设计层: 同步函数丢到线程池跑，不阻塞事件循环
from datetime import datetime, timezone, tzinfo
# 功能层: 日期时间模块，用于cron调度

from croniter import croniter
# 功能层: cron表达式解析库，用于解析和计算cron调度时间
# 上下文层: Worker.schedule()使用croniter计算下一次cron任务的执行时间

from saq.job import Status
# 功能层: 导入任务状态枚举
from saq.queue import Queue
# 功能层: 导入队列抽象基类
from saq.types import (
    CtxType, FunctionsType, JobTaskContext,
    LifecycleFunctionsType, SettingsDict,
)
# 功能层: 导入类型定义
from saq.utils import cancel_tasks, millis, now, uuid1
# 功能层: 导入工具函数

if t.TYPE_CHECKING:
    from asyncio import Task
    from collections.abc import Callable, Collection, Coroutine
    from aiohttp.web_app import Application
    from saq.job import CronJob, Job
    from saq.types import Function, PartialTimersDict, TimersDict, WorkerInfo


logger = logging.getLogger("saq")
# 功能层: 创建SAQ专用日志记录器

JsonDict = t.Dict[str, t.Any]
# 功能层: JSON字典类型别名


class Worker(t.Generic[CtxType]):
    # 功能层: Worker负责从Queue取出Job、执行注册的函数、管理生命周期
    # 设计层: 用Generic[CtxType]做泛型，用户可以自定义上下文类型；
    #          内部用asyncio.Task集合管理并发任务，Event控制优雅关闭
    # 上下文层: 用户通过Worker(**settings)创建实例并调用start()启动任务处理

    SIGNALS = [signal.SIGINT, signal.SIGTERM] if os.name != "nt" else []
    # 功能层: 定义需要捕获的信号列表——SIGINT(Ctrl+C)和SIGTERM(终止)
    # 设计层: Windows不支持信号处理，通过os.name判断条件设置空列表
    # 上下文层: start()方法中注册信号处理器，收到信号时触发event.set()停止Worker

    def __init__(
        self,
        queue: Queue,
        # 功能层: 关联的Queue实例，Worker从中取出Job
        functions: FunctionsType[CtxType],
        # 功能层: 注册的异步函数列表，Worker根据Job.function名称查找对应函数
        *,
        id: t.Optional[str] = None,
        # 功能层: Worker唯一标识，默认自动生成UUID
        concurrency: int = 10,
        # 功能层: 并发度——同时处理的最大任务数
        cron_jobs: Collection[CronJob[CtxType]] | None = None,
        # 功能层: 定时任务列表，使用cron表达式调度
        cron_tz: tzinfo = timezone.utc,
        # 功能层: cron调度的时区，默认UTC
        startup: LifecycleFunctionsType[CtxType] | None = None,
        # 功能层: 启动时执行的钩子函数列表
        shutdown: LifecycleFunctionsType[CtxType] | None = None,
        # 功能层: 关闭时执行的钩子函数列表
        before_process: LifecycleFunctionsType[CtxType] | None = None,
        # 功能层: 每个任务执行前的钩子函数列表
        after_process: LifecycleFunctionsType[CtxType] | None = None,
        # 功能层: 每个任务执行后的钩子函数列表
        timers: PartialTimersDict | None = None,
        # 功能层: 各种定时器的间隔配置（调度、Worker信息、清扫、中止检查）
        dequeue_timeout: float = 0.0,
        # 功能层: 出队操作的超时时间
        burst: bool = False,
        # 功能层: 突发模式——处理完所有任务后自动停止
        max_burst_jobs: int | None = None,
        # 功能层: 突发模式下最多处理的任务数
        shutdown_grace_period_s: int | None = None,
        # 功能层: 关闭时的优雅等待期（秒）
        cancellation_hard_deadline_s: float = 1.0,
        # 功能层: 取消任务的硬截止时间（秒）
        metadata: t.Optional[JsonDict] = None,
        # 功能层: Worker的自定义元数据
        poll_interval: float = 0.0,
        # 功能层: 轮询间隔（仅影响PostgreSQL后端）
    ) -> None:
        self.queue = queue
        self.concurrency = concurrency
        self.pool = ThreadPoolExecutor()
        # 功能层: 创建线程池，用于执行同步函数
        # 设计层: 同步函数在线程池中运行，避免阻塞asyncio事件循环
        self.startup = ensure_coroutine_function_many(startup, self.pool) if startup else None
        self.shutdown = shutdown
        self.before_process = (
            ensure_coroutine_function_many(before_process, self.pool) if before_process else None
        )
        self.after_process = (
            ensure_coroutine_function_many(after_process, self.pool) if after_process else None
        )
        # 功能层: 把生命周期钩子函数统一转成协程函数列表
        # 设计层: ensure_coroutine_function_many单个函数和列表都支持

        self.timers: TimersDict = {
            "schedule": 1,
            "worker_info": 10,
            "sweep": 60,
            "abort": 1,
        }
        # 功能层: 默认定时器间隔配置
        if timers is not None:
            self.timers.update(timers)
            # 功能层: 用用户配置覆盖默认值

        self.event = asyncio.Event()
        # 功能层: 异步事件标志，用于控制Worker的启停
        # 设计层: event.set()触发停止，event.wait()阻塞直到停止信号
        functions = set(functions)
        self.functions: dict[str, Function[CtxType]] = {}
        # 功能层: 函数注册表——函数名到函数对象的映射
        self.cron_jobs: Collection[CronJob] = cron_jobs or []
        self.cron_tz: tzinfo = cron_tz
        self.context: CtxType = t.cast(CtxType, {"worker": self})
        # 功能层: 共享上下文字典，所有任务共享，包含worker引用
        self.tasks: set[Task[t.Any]] = set()
        # 功能层: 当前活跃的asyncio任务集合
        self.job_task_contexts: dict[Job, JobTaskContext] = {}
        # 功能层: Job到其Task和状态的映射，用于中止操作
        self.dequeue_timeout = dequeue_timeout
        self.burst = burst
        self.max_burst_jobs = max_burst_jobs
        self.burst_jobs_processed = 0
        self.burst_jobs_processed_lock = threading.Lock()
        # 功能层: burst模式下的线程安全计数器
        self.burst_condition_met = False
        self._metadata = metadata
        self._poll_interval = poll_interval
        self._stop_lock = asyncio.Lock()
        # 功能层: 保护stop()方法的异步锁，防止重复关闭
        self._stopped = False
        self._shutdown_grace_period_s = shutdown_grace_period_s
        self._cancellation_hard_deadline_s = cancellation_hard_deadline_s
        self.id = uuid1() if id is None else id
        # 功能层: Worker唯一标识符

        if self.burst:
            if self.dequeue_timeout <= 0:
                raise ValueError(
                    "dequeue_timeout must be a positive value greater than 0 when the burst mode is enabled"
                )
            if self.max_burst_jobs is not None:
                self.concurrency = min(self.concurrency, self.max_burst_jobs)
            # 功能层: burst模式验证——必须有出队超时，并发度不超过最大任务数

        for job in self.cron_jobs:
            if not croniter.is_valid(job.cron):
                raise ValueError(f"Cron is invalid {job.cron}")
            functions.add(job.function)
            # 功能层: 验证cron表达式合法性，并将cron函数加入注册表

        for function in functions:
            if isinstance(function, tuple):
                name, function = function
                # 功能层: 支持(name, function)元组形式自定义函数名
            else:
                name = function.__qualname__
                # 功能层: 默认使用函数的限定名（包含类名/模块路径）
            self.functions[name] = function
            # 功能层: 注册函数到名称映射表

    async def _before_process(self, ctx: CtxType) -> None:
        # 功能层: 执行所有before_process钩子
        if self.before_process:
            for bp in self.before_process:
                await bp(ctx)

    async def _after_process(self, ctx: CtxType) -> None:
        # 功能层: 执行所有after_process钩子
        if self.after_process:
            for ap in self.after_process:
                await ap(ctx)

    async def start(self) -> None:
        # 功能层: 启动Worker——注册信号处理器、跑startup钩子、启动维护任务和任务处理循环
        # 设计层: 用try/finally保证无论如何都会执行stop()清理
        # 上下文层: Worker从这里开始跑
        logger.info("Worker starting: %s", repr(self.queue))
        logger.debug("Registered functions:\n%s", "\n".join(f"  {key}" for key in self.functions))
        try:
            self.event = asyncio.Event()
            async with self._stop_lock:
                self._stopped = False
            loop = asyncio.get_running_loop()
            for signum in self.SIGNALS:
                loop.add_signal_handler(signum, self.event.set)
                # 功能层: 注册信号处理器，收到SIGINT/SIGTERM时设置event触发停止
            if self.startup:
                for s in self.startup:
                    await s(self.context)
                    # 功能层: 执行所有startup钩子
            self.tasks.update(await self.upkeep())
            # 功能层: 启动维护任务（调度、清扫、中止检查、Worker信息更新）
            for _ in range(self.concurrency):
                self._process()
                # 功能层: 启动concurrency个并发的任务处理循环
            await self.event.wait()
            # 功能层: 阻塞等待停止信号
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Working shutting down")
            await self.stop()
            for signum in self.SIGNALS:
                loop.remove_signal_handler(signum)

    async def stop(self) -> None:
        # 功能层: 停止Worker——等待任务完成、取消剩余任务、关闭线程池、执行shutdown钩子
        # 设计层: 两阶段关闭——先优雅等待(grace_period)，再强制取消(hard_deadline)
        self.event.set()
        async with self._stop_lock:
            if self._stopped:
                return
            try:
                all_tasks = list(self.tasks)
                self.tasks.clear()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*all_tasks, return_exceptions=True),
                        timeout=self._shutdown_grace_period_s or 0,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Some tasks did not finish within the shutdown grace period, requesting cancellation")
                    cancelled = await cancel_tasks(all_tasks, timeout=self._cancellation_hard_deadline_s)
                    if not cancelled:
                        logger.warning("Some tasks did not finish cancellation in time, they may be stuck or blocked")

                if sys.version_info[0:2] < (3, 9):
                    self.pool.shutdown(True)
                else:
                    self.pool.shutdown(True, cancel_futures=True)
                # 功能层: 关闭线程池，Python 3.9+支持取消待处理的Future

                if not self.shutdown:
                    return
                with ThreadPoolExecutor() as shutdown_pool:
                    shutdown_callbacks = ensure_coroutine_function_many(self.shutdown, shutdown_pool)
                    for s in shutdown_callbacks:
                        await s(self.context)
                    # 功能层: 使用临时线程池执行shutdown钩子（因为主线程池已关闭）
            finally:
                self._stopped = True

    async def schedule(self, lock: int = 1) -> None:
        # 功能层: 调度cron任务——计算下次执行时间并入队，然后调度到期的延迟任务
        for cron_job in self.cron_jobs:
            kwargs = cron_job.__dict__.copy()
            function = kwargs.pop("function").__qualname__
            kwargs["key"] = f"cron:{function}" if kwargs.pop("unique") else None
            start_time = datetime.now(self.cron_tz)
            scheduled = croniter(kwargs.pop("cron"), start_time).get_next()
            # 功能层: 使用croniter计算下一次执行时间
            await self.queue.enqueue(
                function, scheduled=int(scheduled),
                **{k: v for k, v in kwargs.items() if v is not None},
            )
        job_ids = await self.queue.schedule(lock)
        # 功能层: 调度到期的延迟任务
        if job_ids:
            logger.info("Scheduled %s", job_ids)

    async def worker_info(self, ttl: int = 60) -> WorkerInfo:
        # 功能层: 更新并返回Worker的运行时信息
        return await self.queue.worker_info(
            self.id, queue_key=self.queue.name, metadata=self._metadata, ttl=ttl
        )

    async def upkeep(self) -> list[Task[None]]:
        # 功能层: 启动各种后台维护任务——中止检查、调度、清扫、Worker信息更新
        # 设计层: 每个维护任务都是独立的asyncio.Task，通过poll()函数循环执行

        async def poll(func: Callable[[int], Coroutine], sleep: int, arg: int | None = None) -> None:
            # 功能层: 通用的轮询包装器——循环执行func，间隔sleep秒
            while not self.event.is_set():
                try:
                    await func(arg or sleep)
                except (Exception, asyncio.CancelledError):
                    if self.event.is_set():
                        return
                    logger.exception("Upkeep task failed unexpectedly")
                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.abort, self.timers["abort"])),
            # 功能层: 定期检查是否有需要中止的任务
            asyncio.create_task(poll(self.schedule, self.timers["schedule"])),
            # 功能层: 定期调度cron任务和延迟任务
            asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"])),
            # 功能层: 定期清扫卡住的任务
            asyncio.create_task(
                poll(self.worker_info, self.timers["worker_info"], self.timers["worker_info"] + 1)
            ),
            # 功能层: 定期更新Worker信息
        ]

    async def abort(self, abort_threshold: float) -> None:
        # 功能层: 检查并中止被标记为ABORTING/ABORTED的任务
        def get_duration(job: Job) -> float:
            return job.duration("running") or 0

        jobs = [job for job in self.job_task_contexts if get_duration(job) >= millis(abort_threshold)]
        if not jobs:
            return

        for job in await self.queue.jobs(job.key for job in jobs):
            if not job or job.status not in (Status.ABORTING, Status.ABORTED):
                continue
            task_data = self.job_task_contexts.get(job, None)
            if not task_data:
                continue
            task = task_data["task"]
            if not task.done():
                task_data["aborted"] = "abort" if job.error is None else job.error
                _ = await cancel_tasks([task], None)
                # 功能层: 取消任务并等待完成（阻塞操作）
            await self.queue.finish_abort(job)

    async def process(self) -> bool:
        # 功能层: 处理单个任务——出队、执行、处理结果/异常/取消
        # 设计层: 用asyncio.shield挡住wait_for的超时取消，
        #          try/except/finally保证所有路径都有清理
        # 上下文层: 每个并发槽位循环调用这个方法
        context: CtxType | None = None
        job: Job | None = None
        task_ctx: JobTaskContext | None = None

        try:
            job = await self.queue.dequeue(timeout=self.dequeue_timeout, poll_interval=self._poll_interval)
            # 功能层: 从队列中阻塞等待取出一个任务
            if job is None:
                return False
                # 功能层: 出队超时，没有任务可处理

            job.started = now()
            job.attempts += 1
            job.worker_id = self.id
            await job.update(status=Status.ACTIVE)
            # 功能层: 更新任务元数据并标记为活跃状态

            context = t.cast(CtxType, {**self.context, "job": job})
            # 功能层: 创建任务级上下文，包含共享上下文和当前Job
            await self._before_process(context)
            # 功能层: 执行before_process钩子

            function = ensure_coroutine_function(self.functions[job.function], self.pool)
            # 功能层: 保证函数是协程函数（同步函数会被包成异步）
            task = asyncio.create_task(function(context, **(job.kwargs or {})))
            # 功能层: 创建asyncio Task执行任务函数
            task_ctx = JobTaskContext(task=task, aborted=None)
            self.job_task_contexts[job] = task_ctx
            # 功能层: 记录任务和上下文，供中止操作使用

            try:
                result = await asyncio.wait_for(
                    asyncio.shield(task), job.timeout if job.timeout else None
                )
                # 功能层: 等待任务完成，shield保护任务不被wait_for的取消传播
                # 设计层: shield+wait_for组合——超时只取消shield包装器，任务本身继续运行
            except asyncio.TimeoutError:
                task.cancel()
                raise
            if task_ctx["aborted"] is None:
                await job.finish(Status.COMPLETE, result=result)
                # 功能层: 任务成功完成

        except asyncio.CancelledError:
            # 功能层: 处理任务被取消的情况
            if not job or task_ctx is None:
                return False
            task = task_ctx["task"]
            aborted = task_ctx["aborted"]
            if aborted is not None:
                await job.finish(Status.ABORTED, error=aborted)
                return False
            if not task.done():
                cancelled = await cancel_tasks([task], self._cancellation_hard_deadline_s)
                if not cancelled:
                    logger.warning("Function: %s did not finish cancellation in time", job.function)
                await job.retry("cancelled")
                # 功能层: 非主动取消的任务重试

        except Exception as ex:
            # 功能层: 处理任务执行异常
            if context is not None:
                context["exception"] = ex
            if job:
                logger.exception("Error processing job %s", job)
                if task_ctx is not None:
                    task = task_ctx["task"]
                    if not task.done():
                        cancelled = await cancel_tasks([task], self._cancellation_hard_deadline_s)
                        if not cancelled:
                            logger.warning("Function '%s' did not finish cancellation in time", job.function)
                error = traceback.format_exc()
                if job.retryable:
                    await job.retry(error)
                    # 功能层: 可重试的任务进行重试
                else:
                    await job.finish(Status.FAILED, error=error)
                    # 功能层: 不可重试的任务标记为失败

        finally:
            if context:
                if (job is not None and task_ctx is not None
                        and self.job_task_contexts.get(job) is task_ctx):
                    del self.job_task_contexts[job]
                    # 功能层: 清理任务上下文（仅清理自己的，避免覆盖重试的新上下文）
                try:
                    await self._after_process(context)
                except (Exception, asyncio.CancelledError):
                    logger.exception("Failed to run after process hook")
        return True

    def _process(self, previous_task: Task | None = None) -> None:
        # 功能层: 任务处理循环——创建新的process任务并注册完成回调
        # 设计层: 用done_callback做自旋循环，每个任务完成后自动创建新任务
        if previous_task:
            self.tasks.discard(previous_task)
            if self.burst and self._check_burst(previous_task):
                if not any(t.get_name() == "process" for t in self.tasks):
                    self.event.set()
                return
        if not self.event.is_set():
            new_task = asyncio.create_task(self.process(), name="process")
            self.tasks.add(new_task)
            new_task.add_done_callback(self._process)
            # 功能层: 注册完成回调，任务完成后自动触发下一轮处理

    def _check_burst(self, previous_task: Task) -> bool:
        # 功能层: 检查burst模式的停止条件
        if self.burst_condition_met:
            return self.burst_condition_met
        job_dequeued = previous_task.result()
        if not job_dequeued:
            self.burst_condition_met = True
        elif self.max_burst_jobs is not None:
            with self.burst_jobs_processed_lock:
                self.burst_jobs_processed += 1
                if self.burst_jobs_processed >= self.max_burst_jobs:
                    self.burst_condition_met = True
        return self.burst_condition_met


P = te.ParamSpec("P")
R = te.TypeVar("R")
# 功能层: ParamSpec和TypeVar用于泛型函数签名保留

OneOrManyCallable = t.Union[t.Callable[P, R], t.Collection[t.Callable[P, R]]]
# 功能层: 类型别名——单个可调用对象或可调用对象集合


def ensure_coroutine_function_many(
    func: OneOrManyCallable[P, R] | OneOrManyCallable[P, Coroutine[t.Any, t.Any, R]],
    pool: ThreadPoolExecutor,
) -> t.List[Callable[P, Coroutine[t.Any, t.Any, R]]]:
    # 功能层: 将单个或列表形式的函数统一转换为协程函数列表
    # 设计层: 用户传单个函数或列表都行
    if callable(func):
        return [ensure_coroutine_function(func, pool)]
    return [ensure_coroutine_function(f, pool) for f in func]


def ensure_coroutine_function(
    func: Callable[P, R] | Callable[P, Coroutine[t.Any, t.Any, R]],
    pool: ThreadPoolExecutor,
) -> Callable[P, Coroutine[t.Any, t.Any, R]]:
    # 功能层: 确保函数是协程函数——如果已经是异步函数则直接返回，否则包装为异步
    # 设计层: 使用ThreadPoolExecutor执行同步函数，避免阻塞事件循环
    #          使用contextvars.copy_context()传播上下文变量到线程池
    if asyncio.iscoroutinefunction(func):
        return func

    async def wrapped(*args: t.Any, **kwargs: t.Any) -> t.Any:
        future = None
        try:
            ctx = contextvars.copy_context()
            future = pool.submit(lambda: ctx.run(func, *args, **kwargs))
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            try:
                if future is not None:
                    await asyncio.wrap_future(future)
            except Exception:
                pass
            raise

    return wrapped


def import_settings(settings: str) -> SettingsDict:
    # 功能层: 动态导入设置模块——将"module.path.settings"字符串解析为实际的配置字典
    # 设计层: 用importlib动态导入，支持字符串配置路径
    import importlib
    module_path, name = settings.strip().rsplit(".", 1)
    module = importlib.import_module(module_path)
    settings_obj = getattr(module, name)
    if callable(settings_obj):
        settings_obj = settings_obj()
        # 功能层: 支持callable返回设置字典（延迟初始化）
    if not isinstance(settings_obj, dict):
        raise TypeError(f"Settings {settings} must be a dictionary or callable returning dict")
    return t.cast(SettingsDict, settings_obj)


def start(settings: str, web: bool = False, extra_web_settings: list[str] | None = None, port: int = 8080) -> None:
    # 功能层: 启动Worker（可能带Web界面）——导入设置、创建Worker、运行事件循环
    # 上下文层: 被runner.run()调用，是Worker进程的实际入口
    settings_obj = import_settings(settings)
    if "queue" not in settings_obj:
        settings_obj["queue"] = Queue.from_url("redis://localhost")
    loop = asyncio.new_event_loop()
    worker = Worker(**settings_obj)

    async def worker_start() -> None:
        try:
            await worker.queue.connect()
            await worker.start()
        finally:
            await worker.queue.disconnect()

    if web:
        import aiohttp.web
        from saq.web.aiohttp import create_app
        extra_web_settings = extra_web_settings or []
        web_settings = [settings_obj] + [import_settings(s) for s in extra_web_settings]
        queues = [s["queue"] for s in web_settings if s.get("queue")]

        async def shutdown(_app: Application) -> None:
            await worker.stop()

        app = create_app(queues)
        app.on_shutdown.append(shutdown)
        loop.create_task(worker_start()).add_done_callback(
            lambda _: signal.raise_signal(signal.SIGTERM)
        )
        aiohttp.web.run_app(app, port=port, loop=loop)
    else:
        loop.run_until_complete(worker_start())


async def async_check_health(queue: Queue) -> int:
    # 功能层: 异步健康检查——验证队列名称和Worker状态
    await queue.connect()
    info = await queue.info()
    name = info.get("name")
    if name != queue.name:
        logger.warning("Health check failed. Unknown queue name %s", name)
        status = 1
    elif not info.get("workers"):
        logger.warning("No active workers found for queue %s", name)
        status = 1
    else:
        workers = len(info["workers"].values())
        logger.info("Found %d active workers for queue %s", workers, name)
        status = 0
    await queue.disconnect()
    return status


def check_health(settings: str) -> int:
    # 功能层: 同步健康检查入口——导入设置并在事件循环中执行异步检查
    settings_dict = import_settings(settings)
    loop = asyncio.new_event_loop()
    queue = settings_dict.get("queue") or Queue.from_url("redis://localhost")
    return loop.run_until_complete(async_check_health(queue))
