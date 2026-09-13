# 入口第一时间初始化统一日志（JSON + 脱敏），必须在任何 get_logger() 之前
from shared.log import setup_logging
setup_logging()

from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.db import init_engine, dispose_engine
from app.routers import transactions,customers

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_engine()      # 启动：建池
    yield
    dispose_engine()   # 关闭：释放池

app = FastAPI(lifespan=lifespan)

# 业务路由：交易相关接口都在 routers/transactions.py
app.include_router(transactions.router, tags=["交易"])
app.include_router(customers.router, tags=["客户"])




@app.get("/health")
def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    # log_config=None：不让 uvicorn 覆盖 setup_logging() 的配置
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_config=None)
