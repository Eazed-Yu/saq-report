# ============================================================================
# 模块: saq/types.py
# 功能层: 定义SAQ框架的所有类型注解——TypedDict结构体、类型别名、泛型变量
# 设计层: 使用TypedDict定义结构化字典类型，使用TypeVar/ParamSpec实现泛型，使用Literal约束字符串枚举
# 上下文层: 所有核心模块都引用本模块，是SAQ类型系统的底层支撑
# ============================================================================

"""
Types
"""

from __future__ import annotations
# 功能层: 启用延迟注解求值，避免循环导入

import typing as t
# 功能层: 标准类型注解工具
import typing_extensions as te
# 功能层: 扩展类型注解工具，补充ParamSpec、TypeAlias等待性
from collections.abc import Collection
# 功能层: 集合抽象基类，用于类型注解
from typing_extensions import Required, TypedDict, Generic
# 功能层: TypedDict支持带Required标记的字段，Generic支持泛型TypedDict

if t.TYPE_CHECKING:
    # 功能层: 仅在类型检查时导入，运行时不会触发循环依赖
    from asyncio import Task
    from saq.job import CronJob, Job, Status
    from saq.worker import Worker
    from saq.queue import Queue


class Context(TypedDict, total=False):
    # 功能层: 任务上下文字典类型——Worker传递给每个任务函数的上下文信息
    # 设计层: total=False表示所有字段默认可选，但worker字段用Required标记为必需
    # 上下文层: Worker.process()创建context字典并传递给任务函数

    worker: Required[Worker]
    # 功能层: 当前执行任务的Worker实例（必需字段）
    job: Job
    # 功能层: 当前正在执行的Job实例
    queue: Queue
    # 功能层: 任务运行的Queue实例
    exception: t.Optional[Exception]
    # 功能层: 任务执行过程中抛出的异常（如果有）


class JobTaskContext(TypedDict, total=True):
    # 功能层: 任务的运行时上下文——存储asyncio.Task和中止状态
    # 设计层: total=True表示所有字段都是必需的
    # 上下文层: Worker.job_task_contexts字典的值类型，用于中止操作

    task: Task[t.Any]
    # 功能层: 任务的asyncio.Task对象
    aborted: t.Optional[str]
    # 功能层: 中止原因，None表示未被中止


class WorkerInfo(TypedDict):
    # 功能层: Worker信息结构——包含队列键、统计数据和元数据
    queue_key: t.Optional[str]
    stats: t.Optional[WorkerStats]
    metadata: t.Optional[dict[str, t.Any]]


class QueueInfo(TypedDict):
    # 功能层: 队列信息结构——包含Worker列表、任务计数和任务详情
    # 上下文层: Web UI和health check使用此结构展示队列状态
    workers: dict[str, WorkerInfo]
    name: str
    queued: int
    active: int
    scheduled: int
    jobs: list[dict[str, t.Any]]


class WorkerStats(TypedDict):
    # 功能层: Worker统计数据——各类任务计数和运行时间
    complete: int
    failed: int
    retried: int
    aborted: int
    uptime: int


class TimersDict(TypedDict):
    # 功能层: 定时器配置字典——各种维护任务的执行间隔
    schedule: int
    worker_info: int
    sweep: int
    abort: int


class PartialTimersDict(TimersDict, total=False):
    # 功能层: 部分定时器配置——继承TimersDict但所有字段可选
    # 设计层: 允许用户只改几个定时器配置，其余沿用默认值
    pass


CtxType = t.TypeVar("CtxType", bound=Context)
# 功能层: 上下文类型变量——约束为Context的子类，用于Worker和CronJob的泛型参数化
# 设计层: 允许用户扩展Context（比如加个db连接字段），同时保持类型检查不丢


class SettingsDict(TypedDict, Generic[CtxType], total=False):
    # 功能层: Worker设置字典——创建Worker时传入的配置参数
    # 设计层: 使用Generic[CtxType]支持泛型，functions字段用Required标记为必需
    queue: Queue
    functions: Required[FunctionsType[CtxType]]
    concurrency: int
    cron_jobs: Collection[CronJob]
    startup: ReceivesContext[CtxType]
    shutdown: ReceivesContext[CtxType]
    before_process: ReceivesContext[CtxType]
    after_process: ReceivesContext[CtxType]
    timers: PartialTimersDict
    dequeue_timeout: float


P = te.ParamSpec("P")
# 功能层: ParamSpec——捕获函数的完整参数签名（位置参数+关键字参数）
# 设计层: 用于ensure_coroutine_function保留原始函数的类型签名

BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
# 功能层: 入队前回调的类型——接受Job返回Awaitable
CountKind = t.Literal["queued", "active", "incomplete"]
# 功能层: 计数类型的字面量约束
DumpType = t.Callable[[t.Mapping[t.Any, t.Any]], t.Union[bytes, str]]
# 功能层: 序列化函数类型
DurationKind = t.Literal["process", "start", "total", "running"]
# 功能层: 时长类型的字面量约束
Function = t.Callable[te.Concatenate[CtxType, ...], t.Any]
# 功能层: 任务函数类型——第一个参数是上下文，后续参数任意
# 设计层: Concatenate让类型检查器能确认第一个参数是CtxType
FunctionsType: te.TypeAlias = Collection[t.Union[Function[CtxType], tuple[str, Function[CtxType]]]]
# 功能层: 函数列表类型——支持函数对象或(name, function)元组
ReceivesContext = t.Callable[[CtxType], t.Any]
# 功能层: 接收上下文的回调函数类型
LifecycleFunctionsType = t.Union[ReceivesContext[CtxType], Collection[ReceivesContext[CtxType]]]
# 功能层: 生命周期函数类型——单个或列表
ListenCallback = t.Callable[[str, "Status"], t.Any]
# 功能层: 监听回调函数类型
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
# 功能层: 反序列化函数类型
VersionTuple = t.Tuple[int, ...]
# 功能层: 版本号元组类型
