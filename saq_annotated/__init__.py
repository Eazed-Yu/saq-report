# ============================================================================
# 模块: saq/__init__.py
# 功能层: SAQ包的初始化模块，定义包的公共API接口
# 设计层: 通过__all__显式声明导出列表，控制from saq import *的行为
# 上下文层: 作为包的入口点，用户通过import saq即可访问所有核心类
# ============================================================================

"""
SAQ
"""
# 功能层: 模块级文档字符串，简要描述模块名称

from saq.job import CronJob, Job, Status
# 功能层: 从job子模块导入核心数据类——定时任务、任务、状态枚举
# 设计层: 将子模块的类提升到包级别，简化用户导入路径（from saq import Job而非from saq.job import Job）
# 上下文层: 这三个类是用户最常用的API

from saq.queue import Queue
# 功能层: 从queue子模块导入Queue抽象基类
# 上下文层: Queue是用户与SAQ交互的主要接口

from saq.worker import Worker
# 功能层: 从worker子模块导入Worker类
# 上下文层: Worker负责从Queue中取出Job并执行

__all__ = [
    # 功能层: 定义包的公共API列表
    # 设计层: 显式声明__all__能避免内部模块被意外导出
    "CronJob",
    "Job",
    "Queue",
    "Status",
    "Worker",
]

__version__ = "0.26.4"
# 功能层: 定义包的版本号，遵循语义化版本规范
# 上下文层: 可通过saq.__version__查询当前安装版本
