# ============================================================================
# 模块: saq/web/aiohttp.py
# 功能层: aiohttp实现的Web监控服务，提供REST API和HTML页面来看队列和任务
# 设计层: 基于aiohttp.web搭RESTful API，中间件兜异常，可选Basic Auth
# 上下文层: saq --web启动，或者代码里调create_app()建app
# ============================================================================

"""
Built-in AIOHttp webserver, activated with --web param to worker.
"""

from __future__ import annotations

import logging
import os
import traceback
import typing as t

from aiohttp import web

from saq.queue import Queue
from saq.web.common import STATIC_PATH, job_dict, render
# 公共工具：静态文件路径、Job序列化、HTML渲染

if t.TYPE_CHECKING:
    from aiohttp.typedefs import Handler
    from aiohttp.web import StreamResponse
    from aiohttp.web_app import Application
    from aiohttp.web_request import Request
    from aiohttp.web_response import Response
    from saq.job import Job
    from saq.types import QueueInfo


QUEUES_KEY = web.AppKey("queues", t.Dict[str, Queue])
# 功能层: aiohttp应用键，在app里存队列映射用的
# 设计层: AppKey是aiohttp 3.9+的类型安全键


async def queues_(request: Request) -> Response:
    # 功能层: 拿队列信息，单个或全部都行
    queue_name = request.match_info.get("queue")
    response: dict[str, QueueInfo | list[QueueInfo]] = {}
    if queue_name:
        response["queue"] = await _get_queue(request, queue_name).info(jobs=True)
    else:
        response["queues"] = await _get_all_info(request)
    return web.json_response(response)


async def jobs(request: Request) -> Response:
    # 功能层: 取单个任务详情
    job = await _get_job(request)
    return web.json_response({"job": job_dict(job)})


async def retry(request: Request) -> Response:
    # 功能层: 重试某个任务
    job = await _get_job(request)
    await job.retry("retried from ui")
    return web.json_response({})


async def abort(request: Request) -> Response:
    # 功能层: 中止某个任务
    job = await _get_job(request)
    await job.abort("aborted from ui")
    return web.json_response({})


async def views(_request: Request) -> Response:
    # 功能层: 返回SPA前端页面
    return web.Response(text=render(root_path=""), content_type="text/html")


async def health(request: Request) -> Response:
    # 功能层: 健康检查
    if await _get_all_info(request):
        return web.Response(text="OK")
    raise web.HTTPInternalServerError


async def _get_all_info(request: Request) -> list[QueueInfo]:
    return [await q.info() for q in request.app[QUEUES_KEY].values()]


def _get_queue(request: Request, queue_name: str) -> Queue:
    return request.app[QUEUES_KEY][queue_name]


async def _get_job(request: Request) -> Job:
    # 功能层: 从路径里取队列名和任务键，找到对应Job
    queue_name = request.match_info.get("queue", "")
    job_key = request.match_info.get("job", "")
    job = await _get_queue(request, queue_name).job(job_key)
    if not job:
        raise ValueError(f"Job {job_key} not found")
    return job


@web.middleware
async def exceptions(request: Request, handler: Handler) -> StreamResponse:
    # 功能层: 异常中间件，/api路径下的异常统一返回JSON
    # 设计层: @web.middleware装饰器做的，AOP那套思路
    if request.path.startswith("/api"):
        try:
            resp = await handler(request)
            return resp
        except Exception:
            error = traceback.format_exc()
            logging.error(error)
            return web.json_response({"error": error})
    return await handler(request)


async def shutdown(app: Application) -> None:
    # 功能层: 关闭时把所有队列连接断开
    for queue in app.get(QUEUES_KEY, {}).values():
        await queue.disconnect()


def create_app(queues: list[Queue]) -> Application:
    # 功能层: 建aiohttp app，配中间件、路由、认证
    # 设计层: 工厂函数，返回配好的Application
    # 上下文层: Worker.start()开web模式时调这个
    middlewares = [exceptions]
    password = os.environ.get("AUTH_PASSWORD")
    if password:
        from aiohttp_basicauth import BasicAuthMiddleware
        user = os.environ.get("AUTH_USER", "admin")
        middlewares.append(BasicAuthMiddleware(username=user, password=password))
        # 设了AUTH_PASSWORD就开Basic Auth

    app = web.Application(middlewares=middlewares)
    app[QUEUES_KEY] = {q.name: q for q in queues}
    # 队列列表转成名字到对象的字典，塞到app里

    app.add_routes([
        web.static("/static", STATIC_PATH, append_version=True),
        # 静态文件，CSS和JS
        web.get("/api/queues/{queue}/jobs/{job}", jobs),
        web.post("/api/queues/{queue}/jobs/{job}/retry", retry),
        web.post("/api/queues/{queue}/jobs/{job}/abort", abort),
        web.get("/api/queues", queues_),
        web.get("/api/queues/{queue}", queues_),
        web.get("/", views),
        web.get("/queues/{queue}", views),
        web.get("/queues/{queue}/jobs/{job}", views),
        web.get("/health", health),
    ])
    # 路由表

    app.on_shutdown.append(shutdown)
    # 关闭时跑清理
    return app
