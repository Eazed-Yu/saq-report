# ============================================================================
# 模块: saq/job.py
# 功能层: 定义SAQ框架中的核心数据模型——Job(任务)和CronJob(定时任务)类，以及任务状态枚举Status
# 设计层: 使用dataclass装饰器简化数据类定义，使用Enum枚举类表示有限状态集，使用泛型(Generic)支持类型参数化
# 上下文层: 本模块是SAQ框架的数据基础，Worker执行Job、Queue调度Job都要用到这里定义的Job结构
# ============================================================================

from __future__ import annotations
# 功能层: 启用PEP 563延迟注解求值，允许在类型注解中使用前向引用（如Queue在定义时尚未导入）
# 设计层: Python 3.7+的写法，可以避免循环导入，模块加载也更快
# 上下文层: 由于Job类引用了Queue类型（通过TYPE_CHECKING条件导入），必须使用此特性

import dataclasses
# 功能层: 导入dataclasses模块，用于通过@dataclass装饰器自动生成__init__、__repr__、__eq__等魔术方法
# 设计层: dataclass是Python 3.7引入的装饰器，省去手写__init__、__repr__等样板代码
# 上下文层: Job和CronJob都是数据承载类，字段多但逻辑少，非常适合使用dataclass

import enum
# 功能层: 导入enum模块，用于定义Status枚举类
# 设计层: 枚举类把状态值限定在固定集合内，不会出现拼错或非法状态
# 上下文层: Status在Worker、Queue等多个模块里都会用到，用来判断任务处于生命周期的哪个阶段

import typing as t
# 功能层: 导入typing模块并取别名t，提供类型注解工具（如Optional、Any、Union等）
# 设计层: 使用别名t是Python社区的常见惯例，减少代码视觉噪音
# 上下文层: 本模块用了很多类型注解，方便阅读和IDE补全

from saq.types import CtxType
# 功能层: 从types模块导入CtxType类型变量，用于泛型参数化
# 设计层: CtxType是一个TypeVar，约束为Context的子类，允许用户自定义上下文类型
# 上下文层: CronJob使用CtxType实现泛型，使得不同类型的Worker可以携带不同的上下文信息

from saq.utils import exponential_backoff, now, seconds, uuid1
# 功能层: 导入工具函数——指数退避算法、当前时间戳、秒毫秒转换、UUID生成
# 设计层: 将通用工具函数抽取到独立模块，遵循单一职责原则
# 上下文层: Job类在计算重试延迟、判断超时、生成唯一标识等场景中使用这些工具函数


if t.TYPE_CHECKING:
    # 功能层: TYPE_CHECKING在运行时为False，仅在类型检查器（如mypy）运行时为True
    # 设计层: 这是一种条件导入技巧，避免循环导入同时保留类型检查能力
    # 上下文层: Queue和DurationKind仅在类型注解中使用，不需要在运行时导入
    from saq.queue import Queue
    from saq.types import DurationKind, Function

ABORT_ID_PREFIX = "saq:abort:"
# 功能层: 定义中止标识的Redis键前缀，用于在Redis中标记被中止的任务
# 设计层: 使用常量前缀实现命名空间隔离，避免键冲突
# 上下文层: RedisQueue在abort操作中使用此前缀创建临时键，Worker通过检查此键判断任务是否被中止


def get_default_job_key() -> str:
    # 功能层: 生成默认的任务唯一标识符，基于UUID1（包含时间戳和MAC地址）
    # 设计层: 使用工厂函数作为dataclass字段的default_factory，确保每个Job实例获得唯一ID
    # 上下文层: Job的key字段用此函数生成默认值，保证每个任务ID不重复
    return uuid1()


class Status(str, enum.Enum):
    # 功能层: 定义任务状态的枚举类，包含7种状态：新建、已入队、活跃、中止中、已中止、失败、完成
    # 设计层: 同时继承str和Enum，使得枚举值既是枚举成员又是字符串，方便JSON序列化和Redis存储
    # 上下文层: Status是SAQ状态机的核心，Job从创建到结束的每个阶段都靠它判断，Worker和Queue都会查

    NEW = "new"
    # 功能层: 新建状态，任务刚被创建但尚未入队
    QUEUED = "queued"
    # 功能层: 已入队状态，任务已进入等待队列
    ACTIVE = "active"
    # 功能层: 活跃状态，任务正在被Worker执行
    ABORTING = "aborting"
    # 功能层: 中止中状态，任务正在被中止（中间状态）
    ABORTED = "aborted"
    # 功能层: 已中止状态，任务已被成功中止（终态）
    FAILED = "failed"
    # 功能层: 失败状态，任务执行过程中发生异常且无法重试（终态）
    COMPLETE = "complete"
    # 功能层: 完成状态，任务成功执行完毕（终态）


ACTIVE_STATUSES = {Status.NEW, Status.QUEUED, Status.ACTIVE}
# 功能层: 定义活跃状态集合，包含所有"任务仍在进行中"的状态
# 设计层: 使用frozenset语义的集合常量，提供O(1)的成员判断
# 上下文层: 用于判断任务是否仍在活跃处理中

TERMINAL_STATUSES = {Status.COMPLETE, Status.FAILED, Status.ABORTED}
# 功能层: 定义终态集合，包含所有"任务已结束"的状态
# 设计层: 用一个集合统一判断任务是否到终态，不用在各处分别写条件
# 上下文层: Queue.listen()和Job.refresh()中使用此集合判断是否停止等待

UNSUCCESSFUL_TERMINAL_STATUSES = TERMINAL_STATUSES - {Status.COMPLETE}
# 功能层: 定义非成功终态集合（失败+中止），即终态减去完成状态
# 设计层: 使用集合差运算，语义清晰且避免重复定义
# 上下文层: Queue.map()中使用此集合判断任务是否失败，决定是否抛出JobError


@dataclasses.dataclass
class CronJob(t.Generic[CtxType]):
    # 功能层: 定义定时任务类，支持使用cron表达式调度重复执行的任务
    # 设计层: 使用dataclass+泛型(Generic[CtxType])，既简化定义又支持类型参数化
    # 上下文层: Worker在初始化时读取cron_jobs列表，通过croniter库解析cron表达式并定期入队

    function: Function[CtxType]
    # 功能层: 要执行的异步函数，类型为接受上下文参数的可调用对象
    cron: str
    # 功能层: cron表达式字符串，如"* * * * * */5"表示每5秒执行一次
    unique: bool = True
    # 功能层: 是否保证唯一性，True时同一队列中只允许一个该cron任务实例
    timeout: int | None = None
    # 功能层: 任务超时时间（秒），None表示使用默认值
    heartbeat: int | None = None
    # 功能层: 心跳超时时间（秒），用于检测任务是否卡死
    retries: int | None = None
    # 功能层: 最大重试次数
    ttl: int | None = None
    # 功能层: 任务结果的生存时间（秒）
    kwargs: dict[str, t.Any] | None = None
    # 功能层: 传递给任务的额外关键字参数


@dataclasses.dataclass
class Job:
    # 功能层: SAQ的核心任务类，代表一次函数执行的完整生命周期
    # 设计层: 使用dataclass定义大量字段，通过__post_init__进行后处理，通过property实现计算属性
    # 上下文层: Job把Queue（调度）和Worker（执行）两头连起来，任务的元数据和运行时状态都放在这里

    function: str
    # 功能层: 要执行的异步函数的名称（字符串），Worker通过此名称在注册表中查找对应函数
    # 设计层: 使用字符串而非函数引用，使得Job可以被序列化存储到Redis
    # 上下文层: Queue.enqueue()时将函数名存入Job，Worker.process()时通过名称查找函数

    kwargs: dict[str, t.Any] | None = None
    # 功能层: 传递给目标函数的关键字参数，必须可JSON序列化
    # 设计层: 使用可选字典，None表示无参数

    queue: Queue | None = None
    # 功能层: 关联的Queue对象引用，用于执行入队、刷新等操作
    # 设计层: 使用可选类型，因为Job在序列化时不存储Queue引用，反序列化时才绑定

    key: str = dataclasses.field(default_factory=get_default_job_key)
    # 功能层: 任务的唯一标识符，默认使用UUID1自动生成
    # 设计层: 使用default_factory确保每个实例获得独立的UUID，而非共享同一个默认值
    # 上下文层: key是Job在Redis中的唯一标识，也是去重和查找的依据

    timeout: int = 10
    # 功能层: 任务最大执行时间（秒），超时后任务被取消，0表示不限制
    heartbeat: int = 0
    # 功能层: 心跳超时时间（秒），0表示禁用心跳检测
    retries: int = 1
    # 功能层: 最大重试次数，默认为1（即不重试）
    ttl: int = 600
    # 功能层: 任务完成后结果在Redis中的存活时间（秒），0表示永久保存，-1表示不保存
    retry_delay: float = 0.0
    # 功能层: 重试前的等待时间（秒）
    retry_backoff: bool | float = False
    # 功能层: 重试退避策略，False表示固定延迟，True表示指数退避，数值表示最大退避时间
    scheduled: int = 0
    # 功能层: 计划执行的epoch时间（毫秒），0表示立即执行
    progress: float = 0.0
    # 功能层: 任务进度，范围0.0到1.0，可由任务函数手动更新
    attempts: int = 0
    # 功能层: 已尝试执行的次数，每次process时递增
    completed: int = 0
    # 功能层: 任务完成时间戳（毫秒），0表示未完成
    queued: int = 0
    # 功能层: 任务入队时间戳（毫秒）
    started: int = 0
    # 功能层: 任务开始执行时间戳（毫秒）
    touched: int = 0
    # 功能层: 任务最后更新时间戳（毫秒），用于心跳检测
    result: t.Any = None
    # 功能层: 任务执行结果，必须是JSON可序列化的值
    error: str | None = None
    # 功能层: 错误信息，通常是异常的堆栈跟踪字符串
    status: Status = Status.NEW
    # 功能层: 当前任务状态，默认为NEW
    priority: int = 0
    # 功能层: 任务优先级，仅在PostgreSQL后端有效
    group_key: str | None = None
    # 功能层: 分组键，同一分组内同时只能有一个任务活跃，仅PostgreSQL后端支持
    meta: dict[t.Any, t.Any] = dataclasses.field(default_factory=dict)
    # 功能层: 任意元数据字典，用户可附加自定义信息
    worker_id: str | None = None
    # 功能层: 执行此任务的Worker的ID

    _EXCLUDE_NON_FULL = {
        # 功能层: 定义在简洁模式下需要排除的字段集合
        # 设计层: 使用类级别的集合常量，避免每次调用info()时重复创建
        # 上下文层: info()方法在日志输出时使用此集合过滤非关键字段
        "kwargs", "timeout", "heartbeat", "retries", "ttl",
        "retry_delay", "retry_backoff", "scheduled", "progress",
        "result", "error", "status", "priority", "group_key", "meta",
    }

    def info(self, full: bool = False) -> str:
        # 功能层: 生成Job的可读信息字符串，用于日志输出
        # 设计层: 通过full参数控制详细程度，使用排除列表而非包含列表以保持字段顺序
        # 上下文层: Worker在process()和finish()时调用此方法记录日志
        kwargs = {}
        for field in dataclasses.fields(self):
            # 功能层: 遍历dataclass的所有字段定义
            key = field.name
            value = getattr(self, key)
            if (full or key not in self._EXCLUDE_NON_FULL) and not _safe_eq(value, field.default):
                # 功能层: 如果是完整模式或字段不在排除列表中，且值不等于默认值，则包含此字段
                kwargs[key] = value

        if "queue" in kwargs:
            kwargs["queue"] = kwargs["queue"].name
            # 功能层: 将Queue对象替换为其名称字符串，避免显示冗长的对象表示
        if "status" in kwargs:
            kwargs["status"] = kwargs["status"].name.lower()
            # 功能层: 将Status枚举替换为其小写名称

        if not kwargs.get("meta"):
            kwargs.pop("meta", None)
            # 功能层: 如果meta为空则移除，减少噪音

        info = ", ".join(f"{k}: {v}" for k, v in kwargs.items())
        return f"Job<{info}>"
        # 功能层: 将所有键值对拼接为格式化字符串

    def __post_init__(self) -> None:
        # 功能层: dataclass的后初始化钩子，在__init__之后自动调用
        # 设计层: 利用__post_init__进行数据规范化，确保status字段始终为Status枚举类型
        # 上下文层: 从Redis反序列化Job时，status可能是字符串，此方法将其转换回枚举
        if isinstance(self.status, str):
            self.status = Status[self.status.upper()]

    def __repr__(self) -> str:
        # 功能层: 返回Job的详细字符串表示，调用info(True)获取完整信息
        return self.info(True)

    def __hash__(self) -> int:
        # 功能层: 基于key计算哈希值，使Job可以作为字典键和集合元素
        # 设计层: 和__eq__配合，按key判断同一性
        return hash(self.key)

    def __eq__(self, other: t.Any) -> bool:
        # 功能层: 基于key判断两个Job是否相等
        # 设计层: 先用isinstance检查类型，避免和别的类型比较时出问题
        return isinstance(other, Job) and self.key == other.key

    @property
    def id(self) -> str:
        # 功能层: 获取Job的完整ID（包含队列命名空间前缀）
        # 设计层: 用@property把方法包装成属性，调用时不用加括号
        # 上下文层: Redis中存储Job时使用此ID作为键
        return self.get_queue().job_id(self.key)

    @property
    def abort_id(self) -> str:
        # 功能层: 获取Job的中止标识ID，用于在Redis中标记中止状态
        return f"{ABORT_ID_PREFIX}{self.key}"

    def to_dict(self) -> dict[str, t.Any]:
        # 功能层: 将Job序列化为字典，用于JSON序列化和Redis存储
        # 设计层: 跳过默认值字段以减少存储空间，使用_safe_eq安全比较
        # 上下文层: Queue.serialize()调用此方法，然后使用json.dumps转为JSON字符串
        result = {}
        for field in dataclasses.fields(self):
            key = field.name
            value = getattr(self, key)
            if _safe_eq(value, field.default):
                continue
                # 功能层: 跳过与默认值相同的字段，减少序列化体积
            if key == "meta" and not value:
                continue
                # 功能层: 跳过空的meta字典
            if key == "queue" and value:
                value = value.name
                # 功能层: 将Queue对象替换为其名称，因为Queue对象不可序列化
            result[key] = value
        return result

    def duration(self, kind: DurationKind) -> int | None:
        # 功能层: 计算任务的各种时长指标
        # 设计层: 使用Literal类型约束kind参数，编译期即可检查非法值
        # 上下文层: Web UI和日志中使用此方法展示任务性能指标
        if kind == "process":
            return self._duration(self.completed, self.started)
            # 功能层: 处理时长 = 完成时间 - 开始时间
        if kind == "start":
            return self._duration(self.started, self.queued)
            # 功能层: 启动时长 = 开始时间 - 入队时间（即排队等待时间）
        if kind == "total":
            return self._duration(self.completed, self.queued)
            # 功能层: 总时长 = 完成时间 - 入队时间
        if kind == "running":
            return self._duration(now(), self.started)
            # 功能层: 运行时长 = 当前时间 - 开始时间（用于正在执行的任务）
        raise ValueError(f"Unknown duration type: {kind}")

    def _duration(self, a: int, b: int) -> int | None:
        # 功能层: 内部辅助方法，计算两个时间戳的差值，任一为0时返回None
        return a - b if a and b else None

    @property
    def stuck(self) -> bool:
        # 功能层: 检查任务是否"卡住"——即活跃状态但已超过超时或心跳限制
        # 设计层: 用@property把一段判断逻辑包成属性，读起来更像在访问一个字段
        # 上下文层: Queue.sweep()调用此属性判断是否需要清理卡住的任务
        current = now()
        return (self.status == Status.ACTIVE or self.status == Status.ABORTING) and bool(
            (self.timeout and seconds(current - self.started) > self.timeout)
            or (self.heartbeat and seconds(current - self.touched) > self.heartbeat)
        )

    @property
    def retryable(self) -> bool:
        # 功能层: 判断任务是否还可以重试（已尝试次数 < 最大重试次数）
        return self.retries > self.attempts

    def next_retry_delay(self) -> float:
        # 功能层: 计算下一次重试的延迟时间，支持固定延迟和指数退避两种策略
        # 设计层: 根据retry_backoff的类型分支处理，True使用无限指数退避，数值使用有上限的退避
        # 上下文层: Queue._retry()调用此方法确定任务重新入队的延迟时间
        if self.retry_backoff is not False:
            max_delay = None if self.retry_backoff is True else self.retry_backoff
            return exponential_backoff(
                attempts=self.attempts,
                base_delay=self.retry_delay,
                max_delay=max_delay,
                jitter=True,
            )
        return self.retry_delay

    async def enqueue(self, queue: Queue | None = None) -> None:
        # 功能层: 将任务入队到关联的Queue或指定的Queue
        # 设计层: 异步方法，因为入队涉及Redis I/O操作
        # 上下文层: 用户代码通过此方法将Job提交到队列
        queue = queue or self.get_queue()
        if not await queue.enqueue(self):
            await self.refresh()
            # 功能层: 如果入队失败（任务已存在），则刷新本地状态以匹配Redis中的数据

    async def abort(self, error: str, ttl: int = 5) -> None:
        # 功能层: 尝试中止任务
        await self.get_queue().abort(self, error, ttl=ttl)

    async def finish(
        self, status: Status, *, result: t.Any = None, error: str | None = None
    ) -> None:
        # 功能层: 完成任务，设置终态、结果和/或错误信息
        # 设计层: 使用keyword-only参数(*)防止result和error被位置传参
        await self.get_queue().finish(self, status, result=result, error=error)

    async def retry(self, error: str | None) -> None:
        # 功能层: 重试任务，将其从活跃列表移除并重新入队
        await self.get_queue().retry(self, error)

    async def update(self, **kwargs: t.Any) -> None:
        # 功能层: 更新Redis中存储的Job状态
        # 设计层: 使用**kwargs灵活接受任意属性更新
        # 上下文层: Worker在process()中调用此方法更新任务状态为ACTIVE
        await self.get_queue().update(self, **kwargs)

    async def refresh(self, until_complete: float | None = None) -> None:
        # 功能层: 从Redis刷新当前Job的最新数据
        # 设计层: 支持可选的等待模式——until_complete不为None时阻塞等待直到任务完成
        # 上下文层: 用户在enqueue后调用此方法等待任务执行结果
        job = await self.get_queue().job(self.key)

        if not job:
            raise RuntimeError(f"{self} doesn't exist")

        self.replace(job)
        # 功能层: 用Redis中的最新数据替换本地属性

        if until_complete is not None and not self.completed:
            # 功能层: 如果指定了等待且任务尚未完成，则监听状态变化

            def callback(_id: str, status: Status) -> bool:
                return status in TERMINAL_STATUSES
                # 功能层: 回调函数，当任务到达终态时返回True停止监听

            await self.get_queue().listen([self.key], callback, until_complete)
            await self.refresh()
            # 功能层: 监听完成后再次刷新，获取最终结果

    def replace(self, job: Job) -> None:
        # 功能层: 用另一个Job的所有属性替换当前Job的属性
        # 设计层: 使用dataclasses.fields()遍历字段，确保完整复制
        for field in dataclasses.fields(job):
            setattr(self, field.name, getattr(job, field.name))

    def get_queue(self) -> Queue:
        # 功能层: 获取关联的Queue对象，如果未关联则抛出TypeError
        # 设计层: 防御性编程，确保Job操作前已与Queue绑定
        if self.queue is None:
            raise TypeError(
                "`Job` must be associated with a `Queue` before this operation can proceed"
            )
        return self.queue


def _safe_eq(a: object, b: object) -> bool:
    # 功能层: 安全比较两个对象是否相等，捕获ValueError异常
    # 设计层: 某些对象（如numpy数组）的==操作可能抛出ValueError，此函数提供容错比较
    # 上下文层: Job.to_dict()和info()中使用此函数比较字段值与默认值
    try:
        return bool(a == b)
    except ValueError:
        return False
