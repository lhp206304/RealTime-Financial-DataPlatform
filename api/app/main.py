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
