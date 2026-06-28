# ============================================================================
# 模块: saq/__main__.py
# 功能层: SAQ的命令行入口点，解析命令行参数并调用runner.run()启动Worker
# 设计层: 用argparse构建CLI接口，支持python -m saq方式运行
# 上下文层: 用户通过 saq module.settings 或 python -m saq module.settings 启动Worker
# ============================================================================

import argparse
# 功能层: 导入命令行参数解析库
import os
# 功能层: 导入操作系统接口，用于获取当前工作目录
import sys
# 功能层: 导入系统模块，用于修改sys.path

from saq.runner import run
# 功能层: 导入运行器函数


def main() -> None:
    # 功能层: CLI主入口函数
    parser = argparse.ArgumentParser(description="Start Simple Async Queue Worker")
    # 功能层: 创建参数解析器

    parser.add_argument(
        "settings",
        type=str,
        help="Namespaced variable containing worker settings eg: eg module_a.settings",
    )
    # 功能层: 位置参数settings，指定Worker配置的模块路径（如myapp.settings）

    parser.add_argument("--workers", type=int, help="Number of worker processes", default=1)
    # 功能层: Worker进程数量选项

    parser.add_argument(
        "--verbose", "-v",
        action="count",
        help="Logging level: 0: ERROR, 1: INFO, 2: DEBUG",
        default=0,
    )
    # 功能层: 详细程度选项，使用count动作支持-vv多次累加

    parser.add_argument(
        "--web",
        action="store_true",
        help="Start web app.",
    )
    # 功能层: 是否启动Web监控界面

    parser.add_argument(
        "--extra-web-settings", "-e",
        action="append",
        help="Additional worker settings to monitor in the web app",
    )
    # 功能层: 额外的Web监控配置，支持多次指定

    parser.add_argument("--port", type=int, default=8080, help="Web app port, defaults to 8080")
    # 功能层: Web服务端口选项

    parser.add_argument("--check", action="store_true", help="Perform a health check")
    # 功能层: 健康检查选项

    parser.add_argument("--quiet", "-q", action="store_true", help="Disable automatic logging configuration")
    # 功能层: 静默模式选项

    args = parser.parse_args()
    # 功能层: 解析命令行参数

    sys.path.append(os.getcwd())
    # 功能层: 将当前工作目录添加到模块搜索路径
    # 设计层: 让importlib能找到开发中的模块
    # 上下文层: 用户可能在项目根目录运行saq命令，需要能找到项目内的配置模块

    run(
        args.settings,
        workers=args.workers,
        verbose=args.verbose,
        web=args.web,
        extra_web_settings=args.extra_web_settings,
        port=args.port,
        check=args.check,
        quiet=args.quiet,
    )
    # 功能层: 将解析后的参数传递给run()函数启动Worker


if __name__ == "__main__":
    main()
    # 功能层: 当以python -m saq方式运行时调用main()
