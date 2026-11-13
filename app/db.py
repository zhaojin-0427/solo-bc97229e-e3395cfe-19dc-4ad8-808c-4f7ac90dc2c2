"""SQLite 连接管理与建表脚本。

正式库中保存四类数据：
- batches         批次元数据（active / revoking / revoked）
- raw_records     上传的原始 CSV 记录（只增不删，撤销批次永久保留但被过滤）
- current_results 正式结果表（对外可读的当前值，随导入 upsert、随撤销重建）
- revoke_jobs     撤销任务状态（支持重启接续与进度查询）
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'revoking', 'revoked')),
    imported_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_records (
    batch_id   INTEGER NOT NULL REFERENCES batches(batch_id),
    station    TEXT NOT NULL,
    sample_day TEXT NOT NULL,
    analyte    TEXT NOT NULL,
    result     REAL NOT NULL,
    PRIMARY KEY (batch_id, station, sample_day, analyte)
);

CREATE TABLE IF NOT EXISTS current_results (
    station      TEXT NOT NULL,
    sample_day   TEXT NOT NULL,
    analyte      TEXT NOT NULL,
    result       REAL NOT NULL,
    source_batch INTEGER NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (station, sample_day, analyte)
);

CREATE TABLE IF NOT EXISTS revoke_jobs (
    job_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id       INTEGER NOT NULL REFERENCES batches(batch_id),
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'done', 'failed')),
    snapshot_batch INTEGER,
    keys_total     INTEGER NOT NULL DEFAULT 0,
    keys_done      INTEGER NOT NULL DEFAULT 0,
    shadow_path    TEXT,
    error          TEXT,
    created_at     TEXT NOT NULL,
    started_at     TEXT,
    finished_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_batch ON raw_records(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_key ON raw_records(station, sample_day, analyte);

-- 数据库级保证：同一时刻至多一个未结束的撤销任务
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_revoke
ON revoke_jobs(1) WHERE status IN ('pending', 'running');
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str) -> sqlite3.Connection:
    """打开一个 SQLite 连接。

    WAL 模式下读不阻塞写、写不阻塞读；busy_timeout 让切换的短写事务
    与并发导入自动排队而不是立刻报锁错误。
    """
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(current_app.config["DB_PATH"])
    return g.db


def close_db(exc=None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
