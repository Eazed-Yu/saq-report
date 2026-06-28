# ============================================================================
# 模块: saq/queue/__init__.py
# 功能层: queue子包入口，导出Queue基类和JobError异常
# 设计层: 把base里的Queue和JobError提到包级别，导入时少写一层
# 上下文层: 外面用 from saq.queue import Queue 就行，不用管base模块
# ============================================================================

from saq.queue.base import JobError, Queue
# 从base模块拿Queue和JobError

__all__ = [
    "JobError",
    "Queue",
]
# 对外暴露的接口
