FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY wsgi.py .

# 必须单 worker：撤销后台线程与“同时只允许一项”的约束依赖单进程；
# 线程并发足以支撑演示，写事务在 SQLite WAL 下串行排队即可。
CMD ["gunicorn", "--workers=1", "--threads=4", "--bind=0.0.0.0:8000", "--timeout=120", "wsgi:app"]
