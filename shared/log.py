"""统一日志：structlog 结构化 JSON + 敏感字段脱敏，所有模块（api/batch/airflow）共用。

双层日志模式（生产标准做法）
----------------------------
- structlog 负责**业务日志**：业务代码用 get_logger(__name__)，关键字参数即结构化字段
- 标准 logging 负责**第三方库日志**：uvicorn / spark / sqlalchemy 等通过
  ProcessorFormatter 桥接，渲染成同样的 JSON，全进程日志格式统一

设计原则
--------
- JSON 结构化写 stdout：容器友好，docker logs / kubectl logs 直接收集，便于接 ELK/Loki
- 敏感字段自动脱敏：password/secret/token/key 等递归打码，金融数据防泄露
- UTC ISO8601 时间：带时区，跨容器/跨时区对齐
- 入口调一次 setup_logging()，业务代码只 get_logger(__name__)

用法
----
    from shared.log import setup_logging, get_logger
    setup_logging()                                   # 进程入口调一次
    logger = get_logger(__name__)
    logger.info("pipeline start", dt="2026-09-10", rows=100)
    logger.warning("db connect", clickhouse_password="abc")  # 自动 ***
    logger.exception("unexpected error")                     # 自动带结构化堆栈

依赖：structlog（见 api/batch/airflow 的 requirements.txt）
"""
import logging  # Python 标准日志库，structlog 桥接模式的"底层引擎"
import sys      # 用于指定日志输出到 sys.stdout（容器标准输出）

import structlog  # 结构化日志库：业务日志用它，处理器链在它里面跑

# ============================================================
# 敏感字段关键词列表：键名（小写）包含其中任一子串就判定为敏感，值被替换为 ***
# 设计成元组（不可变）是因为这是常量，不需要修改
# 用子串匹配而非精确匹配：如 "clickhouse_password" 也能命中 "password"
# ============================================================
_SENSITIVE_MARKERS = (
    "password", "passwd", "pwd",   # 密码相关
    "secret",                       # 密钥
    "token",                        # 令牌
    "api_key", "apikey",            # API 密钥
    "access_key", "secret_key",     # 访问密钥/密钥对
    "authorization",                # 鉴权头
    "credential",                   # 凭证
    "signature",                    # 签名
)
_MASK = "***"  # 脱敏后的占位字符串，统一显示为 ***


def _is_sensitive(key) -> bool:
    """判断某个键名是否敏感。

    Args:
        key: 任意键名（可能不是字符串，所以先 str() 转换）

    Returns:
        True 如果键名（小写）包含任一敏感关键词
    """
    # str(key).lower(): 把键转成字符串再小写，兼容 int 键等情况
    # any(...): 只要有一个 marker 是子串就返回 True
    # marker in str(key).lower(): 子串匹配，"password" in "db_password" → True
    return any(marker in str(key).lower() for marker in _SENSITIVE_MARKERS)


def _mask_sensitive(obj):
    """递归脱敏函数：遍历数据结构，把敏感键的值替换为 ***。

    处理三种情况：
    - dict：对每个键判断是否敏感，敏感 → 值替换为 ***；不敏感 → 递归处理值
    - list/tuple：递归处理每个元素
    - 其他（str/int/bool 等）：原样返回（叶子节点）

    Args:
        obj: 待脱敏的数据（通常是 event_dict）

    Returns:
        脱敏后的新对象（不修改原对象）
    """
    if isinstance(obj, dict):
        # 字典推导式：构建新字典
        # k: 键名  v: 值
        # 如果键敏感 → 值替换为 _MASK
        # 否则 → 递归处理值（值可能又是 dict/list）
        return {
            k: (_MASK if _is_sensitive(k) else _mask_sensitive(v))
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        # 列表/元组：递归处理每个元素，结果统一成 list
        return [_mask_sensitive(v) for v in obj]
    # 叶子节点（字符串、数字、布尔等）：直接返回，不做处理
    return obj


def mask_sensitive_processor(logger, method_name, event_dict):
    """structlog 处理器：对整条 event_dict 递归脱敏。

    这是符合 structlog Processor 签名的函数：
        Processor = Callable[[WrappedLogger, str, EventDict], ProcessorReturnValue]

    Args:
        logger: 被包装的 logger 对象（底层是标准 logging.Logger）
        method_name: 调用的方法名，如 "info"、"error"
        event_dict: 事件字典，包含 event 消息和所有结构化字段

    Returns:
        脱敏后的 event_dict（传给下一个处理器）
    """
    return _mask_sensitive(event_dict)


def setup_logging(level: str = "INFO") -> None:
    """进程入口调一次：配置 structlog + 桥接标准 logging，全进程 JSON 统一。

    - structlog 业务日志走 ProcessorFormatter.wrap_for_formatter 打包后交给标准 logging
    - uvicorn/spark/sqlalchemy 等第三方日志走 foreign_pre_chain 加工成同样的 JSON
    - 日志写 stdout，不落文件（容器友好，docker logs 直接收集）
    """
    # getattr(logging, "INFO", logging.INFO) → logging.INFO = 20
    # 第三个参数 logging.INFO 是默认值，防止用户传了无效的 level 字符串
    log_level = getattr(logging, str(level).upper(), logging.INFO)

    # ============================================================
    # 共享处理器链：业务日志和第三方日志都跑这一串处理器
    # 保证两边的格式、时间、级别、脱敏完全一致
    # 顺序很重要：merge_contextvars 必须第一个（铺底层上下文，不覆盖后续字段）
    # ============================================================
    shared_processors = [
        # 把 contextvars 里的上下文字段（如 request_id）合并进 event_dict
        # 放第一个，避免覆盖后续处理器加的 level/logger/timestamp 字段
        structlog.contextvars.merge_contextvars,
        # 添加日志级别字段（如 "level": "info"）
        # stdlib 版能同时处理 structlog 和标准 logging 两种来源
        structlog.stdlib.add_log_level,
        # 添加 logger 名字段（如 "logger": "business.order"）
        structlog.stdlib.add_logger_name,
        # 添加时间戳字段，ISO8601 格式，UTC 时区（金融数据跨时区对齐）
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # 把异常信息格式化为结构化的 exc 字段（堆栈转字符串，不再是 exc_info 元组）
        structlog.processors.format_exc_info,
        # 自定义处理器：对 event_dict 递归脱敏，敏感字段值替换为 ***
        mask_sensitive_processor,
    ]

    # ============================================================
    # 配置 structlog：业务日志的处理管道
    # ============================================================
    structlog.configure(
        processors=[
            *shared_processors,  # 先跑共享处理器（加字段、脱敏等）
            # 最后一步：把 event_dict 打包成 ((event_dict,), {"extra": {...}})
            # 这样 bound logger 会调用底层标准 logger.info(event_dict, extra={...})
            # event_dict 进入 LogRecord.msg 槽位，extra 里贴 _logger/_name 暗号
            # 注意：此时还没有 LogRecord，是标准 logging 收到调用后才创建
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        # logger_factory：创建底层 logger 的工厂
        # LoggerFactory() 让 structlog 底层用标准 logging.Logger
        # 调用 get_logger("business.order") 时，底层返回 logging.getLogger("business.order")
        logger_factory=structlog.stdlib.LoggerFactory(),
        # wrapper_class：structlog 的外壳类，提供 info/error 等方法
        # make_filtering_bound_logger(log_level) 动态生成一个类，
        # 低于 log_level 的方法（如 debug 在 INFO 模式下）直接是空函数 return None
        # 好处：被过滤的日志几乎零开销，连处理器链都不进
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        # 首次使用 logger 后缓存 bound logger，避免重复创建，提升性能
        # 代价：运行时改 configure 不会影响已缓存的 logger（通常配置只在启动时设一次）
        cache_logger_on_first_use=True,
    )

    # ============================================================
    # 配置标准 logging：唯一的 Formatter，业务日志和第三方日志共用
    # ============================================================
    formatter = structlog.stdlib.ProcessorFormatter(
        # foreign_pre_chain：处理"外来"日志（第三方库直接调用标准 logging 的日志）
        # 这些日志天生不是 event_dict，先用 shared_processors 把它们加工成 dict
        foreign_pre_chain=shared_processors,
        # processors：渲染阶段的处理器（所有日志，不管来源，都跑这一串）
        processors=[
            # 清理处理器链内部用的元数据（如 _record、_logger 等），不输出到最终日志
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # 把 event_dict 渲染成 JSON 字符串
            # ensure_ascii=False：中文按原样输出（否则变 \uXXXX 转义，日志不可读）
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
    )
    # StreamHandler(sys.stdout)：日志输出到标准输出（容器约定：日志不落文件）
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)  # 给 handler 挂上上面的 formatter

    # ============================================================
    # 配置 root logger：所有 logger（业务 + 第三方）的日志最终都到这里
    # ============================================================
    root = logging.getLogger()  # 不传参数 → 获取 root logger
    root.handlers.clear()       # 清掉可能存在的默认 handler，防止重复输出
    root.addHandler(handler)    # 挂上我们的 handler（输出 JSON 到 stdout）
    root.setLevel(log_level)    # 设置 root 日志级别，低于此级别的不处理


def get_logger(name: str | None = None):
    """获取 structlog bound logger。业务代码统一用 get_logger(__name__)。

    Args:
        name: logger 名称，通常传 __name__（模块路径，如 "api.routes.order"）
              点号分隔形成 logger 层级树，用于传播和级别调整

    Returns:
        structlog 的 bound logger，调用 .info("msg", key=value) 记录结构化日志
        关键字参数会自动变成 JSON 里的字段
    """
    return structlog.get_logger(name)
