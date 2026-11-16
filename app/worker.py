"""后台计算 worker：认领任务 → 计算 → 单事务写入报告，写失败自动重试。

并发与故障语义：
- 报告与任务绑定固定的 (fasta_version, csv_version)，是否"当前"由版本派生，
  因此迟到完成的结果永远不会覆盖更新版本对应的结论；
- 报告 INSERT 与任务状态 UPDATE 在同一事务提交，失败整体回滚，不留半份报告；
- 报告写入的**瞬时失败**（如 database is locked / 磁盘暂不可写）自动重试：
  任务回到 pending 并按指数退避延迟领取；连续失败达到上限后任务标记 failed，
  网页据此显示失败并允许按当前版本重试；
- 非法输入产生的是**失败报告**（确定性结果），不进入写失败重试循环；
- 同一任务至多一份报告：reports.job_id 唯一，重复领取/重复执行撞唯一约束时
  幂等收场，不产生第二份报告；
- 进程重启时被中断的 running 任务由 db.init_app 重新排队；worker 崩溃等
  极端情况下，超时 running 任务由 claim_job 的过期回收兜底。
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
import traceback

from .db import connect
from .normalize import ValidationError, compute_report

DEFAULT_MAX_WRITE_ATTEMPTS = 3     # 连续写入失败上限（含首次）
DEFAULT_BACKOFF_SECONDS = 1.0      # 退避基数：第 n 次失败后延迟 base * 2^(n-1)
DEFAULT_STALE_SECONDS = 900.0      # running 超过该时长视为被中断，重新排队


class WriteFailureInjector:
    """演示/演练用故障注入：前 n 次报告写入必失败（模拟瞬时写故障）。"""

    def __init__(self, n: int):
        self.remaining = n

    def __call__(self) -> None:
        if self.remaining > 0:
            self.remaining -= 1
            raise sqlite3.OperationalError("injected transient write failure")


def claim_job(db_path: str, stale_seconds: float | None = None):
    """原子认领最早可领取的 pending 任务；没有则返回 None。

    stale_seconds 不为 None 时，先把超时未完成的 running 任务（worker 崩溃、
    写失败善后也未成功的兜底场景）重新排队。
    """
    conn = connect(db_path)
    try:
        if stale_seconds is not None:
            conn.execute(
                "UPDATE jobs SET status='pending', started_at=NULL"
                " WHERE status='running' AND started_at IS NOT NULL"
                " AND started_at <= datetime('now', ?)",
                (f"-{int(stale_seconds)} seconds",),
            )
            conn.commit()
        row = conn.execute(
            "SELECT id FROM jobs WHERE status='pending'"
            " AND (run_at IS NULL OR run_at <= datetime('now'))"
            " ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        cur = conn.execute(
            "UPDATE jobs SET status='running', started_at=datetime('now')"
            " WHERE id=? AND status='pending'",
            (row["id"],),
        )
        conn.commit()
        if cur.rowcount != 1:  # 被其他 worker 抢先
            return None
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
        return dict(job)
    finally:
        conn.close()


def _backoff_modifier(attempts: int, backoff_seconds: float) -> str:
    """第 attempts 次失败后的 SQLite 时间修饰串（指数退避，整数秒）。"""
    secs = max(0, int(math.ceil(backoff_seconds * (2 ** (attempts - 1)))))
    return f"+{secs} seconds"


def _record_write_failure(db_path: str, job_id: int, message: str,
                          max_attempts: int, backoff_seconds: float) -> None:
    """写失败善后：计数并重排队；连续失败达上限则标记任务失败。

    尽力而为：若善后写入本身也失败（数据库仍不可用），任务保持 running，
    留待进程重启恢复或 claim_job 的过期回收，不会丢失。
    """
    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT attempts FROM jobs WHERE id=? AND status='running'", (job_id,)
        ).fetchone()
        if row is None:
            return  # 已被其他路径处理（重复领取的另一方 / 重启恢复）
        attempts = row["attempts"] + 1
        if attempts >= max_attempts:
            conn.execute(
                "UPDATE jobs SET attempts=?, status='failed', error=?,"
                " finished_at=datetime('now') WHERE id=? AND status='running'",
                (attempts, message, job_id),
            )
        else:
            conn.execute(
                "UPDATE jobs SET attempts=?, status='pending', error=?,"
                " started_at=NULL, run_at=datetime('now', ?)"
                " WHERE id=? AND status='running'",
                (attempts, message, _backoff_modifier(attempts, backoff_seconds),
                 job_id),
            )
        conn.commit()
    except sqlite3.Error:
        traceback.print_exc()
    finally:
        conn.close()


def process_job(db_path: str, job: dict, delay: float = 0.0,
                max_attempts: int = DEFAULT_MAX_WRITE_ATTEMPTS,
                backoff_seconds: float = 0.0,
                before_write=None) -> None:
    """执行一个任务并把报告与任务状态在同一事务落库。

    before_write：写入前钩子（测试/演示注入瞬时写故障），抛出的异常
    按瞬时写失败处理。
    """
    conn = connect(db_path)
    try:
        try:
            fasta = conn.execute(
                "SELECT content FROM inputs WHERE batch_id=? AND kind='fasta' AND version=?",
                (job["batch_id"], job["fasta_version"]),
            ).fetchone()
            csv_row = conn.execute(
                "SELECT content FROM inputs WHERE batch_id=? AND kind='csv' AND version=?",
                (job["batch_id"], job["csv_version"]),
            ).fetchone()

            if delay > 0:
                time.sleep(delay)  # 模拟耗时计算；此期间更正照常提交

            try:
                if fasta is None or csv_row is None:
                    raise ValidationError("任务对应的输入版本不存在")
                result = compute_report(fasta["content"], csv_row["content"])
                status, error, payload = "ok", None, json.dumps(result, ensure_ascii=False)
                chrom, ref_len = result["chrom"], result["ref_len"]
            except ValidationError as exc:
                status, error, payload, chrom, ref_len = "failed", str(exc), None, None, None
            except Exception as exc:  # 非预期错误同样整批失败，不外泄堆栈
                status, error = "failed", f"内部错误: {exc}"
                payload, chrom, ref_len = None, None, None
        except sqlite3.Error as exc:
            # 读取输入阶段的瞬时数据库错误：与写失败同样重排队
            _record_write_failure(db_path, job["id"], f"数据库暂不可用: {exc}",
                                  max_attempts, backoff_seconds)
            return

        if before_write is not None:
            try:
                before_write()
            except Exception as exc:
                _record_write_failure(db_path, job["id"], f"报告写入失败: {exc}",
                                      max_attempts, backoff_seconds)
                return

        try:
            with conn:  # 单事务：报告 + 任务状态一起提交，失败整体回滚
                conn.execute(
                    "INSERT INTO reports(job_id, batch_id, fasta_version, csv_version,"
                    " status, error, chrom, ref_len, payload, created_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?, datetime('now'))",
                    (job["id"], job["batch_id"], job["fasta_version"], job["csv_version"],
                     status, error, chrom, ref_len, payload),
                )
                conn.execute(
                    "UPDATE jobs SET status=?, finished_at=datetime('now'), error=?"
                    " WHERE id=?",
                    ("done" if status == "ok" else "failed", error, job["id"]),
                )
        except sqlite3.IntegrityError:
            # reports.job_id 唯一：重复领取的另一方已写入本任务的报告，
            # 幂等收场，不产生第二份；若报告并不存在则按写失败善后。
            dup = conn.execute(
                "SELECT 1 FROM reports WHERE job_id=?", (job["id"],)
            ).fetchone()
            if dup is None:
                _record_write_failure(db_path, job["id"], "报告写入冲突",
                                      max_attempts, backoff_seconds)
        except sqlite3.Error as exc:
            # 瞬时写失败：事务已整体回滚（无半份报告），计数并退避重试
            _record_write_failure(db_path, job["id"], f"报告写入失败: {exc}",
                                  max_attempts, backoff_seconds)
    finally:
        conn.close()


def run_pending_jobs(db_path: str, delay: float = 0.0, limit=None,
                     max_attempts: int = DEFAULT_MAX_WRITE_ATTEMPTS,
                     backoff_seconds: float = 0.0,
                     stale_seconds: float | None = None,
                     before_write=None) -> int:
    """依次处理可领取的 pending 任务，返回处理数（测试与 worker 循环共用）。"""
    n = 0
    while limit is None or n < limit:
        job = claim_job(db_path, stale_seconds=stale_seconds)
        if job is None:
            break
        process_job(db_path, job, delay, max_attempts=max_attempts,
                    backoff_seconds=backoff_seconds, before_write=before_write)
        n += 1
    return n


def worker_loop(db_path: str, delay: float, poll: float,
                max_attempts: int = DEFAULT_MAX_WRITE_ATTEMPTS,
                backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
                stale_seconds: float | None = DEFAULT_STALE_SECONDS,
                before_write=None) -> None:
    while True:
        try:
            if run_pending_jobs(db_path, delay, limit=1,
                                max_attempts=max_attempts,
                                backoff_seconds=backoff_seconds,
                                stale_seconds=stale_seconds,
                                before_write=before_write) == 0:
                time.sleep(poll)
        except Exception:
            traceback.print_exc()
            time.sleep(poll)
