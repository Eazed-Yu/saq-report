# ============================================================================
# 模块: saq/errors.py
# 功能层: 定义SAQ框架的自定义异常类
# 设计层: 用继承Exception的自定义异常类，提供语义化的错误类型
# 上下文层: 被queue模块和redis模块引用，在依赖缺失或URL无效时抛出对应异常
# ============================================================================

"""
Errors
"""
# 功能层: 模块级文档字符串


class MissingDependencyError(Exception):
    # 功能层: 依赖缺失异常，当用户未安装可选依赖（如redis、aiohttp）时抛出
    # 设计层: 继承Exception而非BaseException，说明这是可预期的应用错误
    # 上下文层: RedisQueue和HttpQueue在导入可选依赖失败时抛出此异常，并附带安装指引
    pass


class InvalidUrlError(Exception):
    # 功能层: 无效URL异常，当Queue.from_url()接收到无法识别的URL协议时抛出
    # 设计层: 独立的异常类型使调用者可以精确捕获和处理此类错误
    # 上下文层: Queue.from_url()在解析URL scheme时，如果不支持redis/postgres/http则抛出
    pass
