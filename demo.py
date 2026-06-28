"""
SAQ 演示脚本
功能层: 演示SAQ的核心功能——任务入队、等待结果、批量执行、定时任务
设计层: 使用asyncio.run()作为异步入口，展示SAQ的典型使用模式
上下文层: 需要先启动Redis服务（docker run -d -p 6379:6379 redis）

注意: 下面的连接字符串使用占位符，请改为自己的 Redis 地址后再运行：
    redis://:<REDIS_PASSWORD>@<REDIS_HOST>:<REDIS_PORT>
例如:
    redis://:your_password@127.0.0.1:6379
"""

import asyncio
import time

from saq import CronJob, Queue
from saq.types import Context, SettingsDict


async def add(ctx: Context, *, a: int, b: int) -> dict:
    """加法任务：演示基本的任务执行和结果返回"""
    await asyncio.sleep(0.3)
    return {"result": a + b, "timestamp": time.time()}


async def greet(ctx: Context, *, name: str) -> str:
    """问候任务：演示字符串结果返回"""
    await asyncio.sleep(0.2)
    return f"Hello, {name}!"


async def slow_task(ctx: Context, *, duration: float) -> dict:
    """慢任务：演示超时和进度更新"""
    job = ctx.get("job")
    for i in range(int(duration * 10)):
        await asyncio.sleep(0.1)
        if job:
            await job.update(progress=(i + 1) / (duration * 10))
    return {"completed": True, "duration": duration}


async def failing_task(ctx: Context, *, fail: bool = True) -> str:
    """失败任务：演示错误处理和重试机制"""
    if fail:
        raise ValueError("This task is designed to fail!")
    return "Success!"


async def cron_job(ctx: Context) -> None:
    """定时任务：演示CronJob的使用"""
    print(f"[Cron] 定时任务执行于 {time.strftime('%H:%M:%S')}")


queue = Queue.from_url("redis://:<REDIS_PASSWORD>@<REDIS_HOST>:<REDIS_PORT>")

settings = SettingsDict[Context](
    queue=queue,
    functions=[add, greet, slow_task, failing_task],
    concurrency=10,
    cron_jobs=[CronJob(cron_job, cron="* * * * * */10")],
)


async def demo_basic():
    """演示1: 基本任务入队和结果获取"""
    print("\n" + "=" * 60)
    print("演示1: 基本任务入队 (enqueue + apply)")
    print("=" * 60)

    await queue.connect()

    job = await queue.enqueue("add", a=10, b=20)
    print(f"  任务已入队: key={job.key}, status={job.status}")

    await job.refresh(until_complete=5)
    print(f"  任务完成: status={job.status}, result={job.result}")

    result = await queue.apply("add", a=100, b=200)
    print(f"  apply结果: {result}")

    await queue.disconnect()


async def demo_batch():
    """演示2: 批量任务执行"""
    print("\n" + "=" * 60)
    print("演示2: 批量任务执行 (map)")
    print("=" * 60)

    await queue.connect()

    results = await queue.map(
        "greet",
        [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
        timeout=10,
    )
    print(f"  批量结果: {results}")

    await queue.disconnect()


async def demo_error_handling():
    """演示3: 错误处理和重试"""
    print("\n" + "=" * 60)
    print("演示3: 错误处理 (retry + error)")
    print("=" * 60)

    await queue.connect()

    job = await queue.enqueue("failing_task", fail=True, retries=2, retry_delay=0.5)
    print(f"  失败任务已入队: key={job.key}, retries={job.retries}")

    await job.refresh(until_complete=10)
    print(f"  最终状态: {job.status}")
    if job.error:
        print(f"  错误信息: {job.error[:80]}...")

    await queue.disconnect()


async def demo_progress():
    """演示4: 任务进度跟踪"""
    print("\n" + "=" * 60)
    print("演示4: 任务进度跟踪 (progress)")
    print("=" * 60)

    await queue.connect()

    job = await queue.enqueue("slow_task", duration=2.0)
    print(f"  慢任务已入队: key={job.key}")

    while not job.completed:
        try:
            await job.refresh(until_complete=0.3)
        except asyncio.TimeoutError:
            continue
        print(f"  进度: {job.progress:.1%}, 状态: {job.status}")

    print(f"  最终结果: {job.result}")

    await queue.disconnect()


async def main():
    """运行所有演示"""
    print("=" * 60)
    print("SAQ (Simple Async Queue) 功能演示")
    print(f"版本: {__import__('saq').__version__}")
    print("=" * 60)

    try:
        await demo_basic()
        await demo_batch()
        await demo_error_handling()
        await demo_progress()

        print("\n" + "=" * 60)
        print("所有演示完成!")
        print("=" * 60)
    except Exception as e:
        print(f"\n演示出错: {e}")
        print("请确保Redis服务已启动: docker run -d -p 6379:6379 redis")


if __name__ == "__main__":
    asyncio.run(main())
