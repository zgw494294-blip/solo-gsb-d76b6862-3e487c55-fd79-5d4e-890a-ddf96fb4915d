FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FIRMWARE_DB_PATH=/data/firmware.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 持久卷挂载点；应用启动时自动建目录与表
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 单 worker：SQLite + BEGIN IMMEDIATE 已保证操作串行等价
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
