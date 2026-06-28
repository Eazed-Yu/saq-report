# ============================================================================
# 模块: saq/runner.py
# 功能层: 提供SAQ的启动运行器，负责配置日志、启动多进程Worker、健康检查
# 设计层: 用multiprocessing实现多进程Worker，通过positional-only参数(/)强制settings为位置参数
# 上下文层: 被__main__.py的CLI入口调用，是用户命令行启动Worker的最终执行者
# ============================================================================

from __future__ import annotations

import logging
# 功能层: 导入日志模块，用于配置日志级别
import multiprocessing
# 功能层: 导入多进程模块，用于启动多个Worker进程
# 设计层: 用多进程来利用多核CPU，每个Worker进程独立处理任务
import sys
# 功能层: 导入sys模块，用于sys.exit()退出程序

from saq.worker import check_health, start
# 功能层: 从worker模块导入健康检查和启动函数


def run(
    settings: str,
    /,
    # 功能层: positional-only参数（/），settings只能作为位置参数传入
    # 设计层: 防止settings与**kwargs中的关键字冲突
    *,
    workers: int = 1,
    # 功能层: Worker进程数量，默认为1
    verbose: int = 0,
    # 功能层: 日志详细程度，0=WARNING，1=INFO，2=DEBUG
    web: bool = False,
    # 功能层: 是否启动Web监控界面
    extra_web_settings: list[str] | None = None,
    # 功能层: 额外的Web监控配置，用于监控多个队列
    port: int = 8080,
    # 功能层: Web服务端口
    check: bool = False,
    # 功能层: 是否执行健康检查而非启动Worker
    quiet: bool = False,
    # 功能层: 是否禁用自动日志配置
) -> None:
    if not quiet:
        # 功能层: 根据verbose级别设置日志等级
        level = verbose
        if level == 0:
            level = logging.WARNING
        elif level == 1:
            level = logging.INFO
        else:
            level = logging.DEBUG
        logging.basicConfig(level=level)
        # 功能层: 配置根日志记录器的级别

    if check:
        sys.exit(check_health(settings))
        # 功能层: 健康检查模式，检查后以状态码退出
    else:
        if workers > 1:
            for _ in range(workers - 1):
                p = multiprocessing.Process(target=start, args=(settings,))
                p.start()
            # 功能层: 启动额外的Worker进程（主进程也运行一个Worker）
            # 设计层: 用multiprocessing.Process创建独立进程，每个进程有自己的事件循环

        try:
            start(
                settings,
                web=web,
                extra_web_settings=extra_web_settings,
                port=port,
            )
            # 功能层: 在主进程中启动Worker（可能带Web界面）
        except KeyboardInterrupt:
            pass
            # 功能层: 捕获Ctrl+C信号，优雅退出
