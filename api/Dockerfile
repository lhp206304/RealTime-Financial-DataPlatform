# 语法：docker build 把本目录（api/）作为构建上下文，WORKDIR /app
# 拷贝后容器内结构：/app/app/main.py，uvicorn app.main:app 从此启动
FROM python:3.11-slim

WORKDIR /app

# 先装依赖（单独一层，改代码不会重装包，利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 再拷代码
COPY . .

# 非 root 用户运行（生产安全要求）
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# 0.0.0.0：让容器外（宿主机/其他容器）能访问；不能只监听 127.0.0.1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
