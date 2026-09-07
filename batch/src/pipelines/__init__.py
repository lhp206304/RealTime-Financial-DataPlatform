"""表级加工任务：一个模块 = 一张目标表 = 一个可被调度的任务。

每个模块对外暴露：
    run(dt: str) -> None   # Airflow / 命令行 调用入口：建 Spark → 读 → 加工 → 校验 → 写
模块内的业务函数（clean/aggregate 等）是纯函数（DF→DF），可在 tests/ 里单测。
"""
