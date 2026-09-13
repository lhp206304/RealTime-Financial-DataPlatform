"""跨模块共享代码（api / batch / generator 通用）。

通过 docker-compose 挂载 + PYTHONPATH 注入，各模块 `from shared.xxx import ...` 引用。
"""
