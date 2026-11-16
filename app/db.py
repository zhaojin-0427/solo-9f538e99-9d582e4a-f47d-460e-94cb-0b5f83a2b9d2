"""SQLite 访问层：连接管理、建表、轻量迁移、中断任务恢复。"""
from __future__ import annotations

import os
import sqlite3

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inputs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id),
    kind        TEXT NOT NULL CHECK (kind IN ('fasta', 'csv')),
    version     INTEGER NOT NULL,
    content     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (batch_id, kind, version)
);

CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        INTEGER NOT NULL REFERENCES batches(id),
    fasta_version   INTEGER NOT NULL,
    csv_version     INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'done', 'failed')),
    error           TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,  -- 连续写入失败次数（成功/到上限后不再变化）
    run_at          TEXT,          -- 退避重试：早于该时间不领取；NULL 表示立即可领取
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT
);

-- 每个计算任务至多一份报告；报告整份写入（单事务），失败不留半份
CREATE TABLE IF NOT EXISTS reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL UNIQUE REFERENCES jobs(id),
    batch_id        INTEGER NOT NULL REFERENCES batches(id),
    fasta_version   INTEGER NOT NULL,
    csv_version     INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
    error           TEXT,
    chrom           TEXT,
    ref_len         INTEGER,
    payload         TEXT,          -- ok 时为 JSON 结果；failed 时为 NULL
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, id);
CREATE INDEX IF NOT EXISTS idx_reports_batch ON reports(batch_id, id);

-- 同一批次同一版本组合至多一个未完成（pending/running）任务：
-- 并发更正/重复提交在数据库层去重，撞索引即视为已排队
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_one_active
    ON jobs(batch_id, fasta_version, csv_version)
    WHERE status IN ('pending', 'running');
"""

# 老库轻量迁移：CREATE TABLE IF NOT EXISTS 不会给已有表补列
_MIGRATIONS = (
    "ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE jobs ADD COLUMN run_at TEXT",
)


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(current_app.config["DATABASE"])
    return g.db


def close_db(_exc=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_app(app) -> None:
    path = app.config["DATABASE"]
    if path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc):
                    raise
        # 进程重启后，此前 running 的任务视为被中断，重新排队（立即可领取）。
        # attempts 不清零：反复崩溃/写失败的任务最终仍会到达上限并标记失败，
        # 不会在重启间无限循环。
        conn.execute(
            "UPDATE jobs SET status='pending', started_at=NULL, run_at=NULL"
            " WHERE status='running'"
        )
        conn.commit()
    finally:
        conn.close()
    app.teardown_appcontext(close_db)
