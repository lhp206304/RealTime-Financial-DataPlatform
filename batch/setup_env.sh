#!/usr/bin/env bash
# 在 batch/ 目录下执行：source setup_env.sh
# 作用：一键设 PySpark 所需的三个环境变量 + venv 激活

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_ROOT="$SCRIPT_DIR"

# 1. 激活虚拟环境
if [ -d "$BATCH_ROOT/.venv" ]; then
    source "$BATCH_ROOT/.venv/bin/activate"
else
    echo "[WARN] 未找到 $BATCH_ROOT/.venv，请先创建 venv：python3 -m venv .venv"
fi

# 2. Java 17（brew 默认位置）
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
if [ ! -x "$JAVA_HOME/bin/java" ]; then
    echo "[WARN] 未找到 JDK 17 于 $JAVA_HOME，执行：brew install openjdk@17"
fi

# 3. Python 模块搜索路径（指向 batch/ 根，才能 import config/src/jobs）
export PYTHONPATH="$BATCH_ROOT"

# 4. Spark worker 子进程必须用 venv 的 python（不能用系统 python）
export PYSPARK_PYTHON="$BATCH_ROOT/.venv/bin/python"

# 5. 验证
echo "[setup_env]"
echo "  JAVA_HOME       = $JAVA_HOME"
echo "  PYTHONPATH      = $PYTHONPATH"
echo "  PYSPARK_PYTHON  = $PYSPARK_PYTHON"
echo "  python --version= $(python --version 2>&1)"
echo "  java -version  = $(java -version 2>&1 | head -1)"
