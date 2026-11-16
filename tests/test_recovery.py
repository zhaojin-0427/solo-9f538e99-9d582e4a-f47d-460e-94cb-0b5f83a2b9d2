"""故障恢复测试：瞬时写失败重试、上限失败与页面重试、重复领取幂等、重启/过期恢复。"""
import io
import json
import sqlite3
import time

import pytest

from app import create_app
from app.db import connect, init_app
from app.worker import (WriteFailureInjector, claim_job, process_job,
                        run_pending_jobs)

FASTA = ">chr1\n" + "A" * 20 + "\n"
CSV_OK = "ID,POS,REF,ALT\ni1,3,A,AA\ns1,5,A,C\n"
CSV_BAD = "ID,POS,REF,ALT\nbad,5,C,G\n"  # REF 与参考不符


@pytest.fixture()
def app(tmp_path):
    return create_app({
        "TESTING": True,
        "DATABASE": str(tmp_path / "t.db"),
        "WORKER_ENABLED": False,     # 测试手动驱动 worker
        "COMPUTE_DELAY_SECONDS": 0,
    })


@pytest.fixture()
def client(app):
    return app.test_client()


def _upload(client, fasta=FASTA, csv_text=CSV_OK, name="t"):
    return client.post("/batches", data={
        "name": name,
        "fasta": (io.BytesIO(fasta.encode()), "ref.fa"),
        "csv": (io.BytesIO(csv_text.encode()), "vars.csv"),
    })


def _db(app):
    return connect(app.config["DATABASE"])


def _reports(app, bid=1):
    conn = _db(app)
    rows = conn.execute(
        "SELECT * FROM reports WHERE batch_id=? ORDER BY id", (bid,)).fetchall()
    conn.close()
    return rows


def _job(app, jid=1):
    conn = _db(app)
    row = conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ---------------------------------------------------------------- 瞬时写失败

def test_transient_write_failure_retries_and_succeeds(app, client):
    """一次瞬时写失败：任务自动重排队，恢复后完成，恰好一份报告。"""
    _upload(client)
    db_path = app.config["DATABASE"]

    job = claim_job(db_path)
    assert job is not None
    process_job(db_path, job,
                before_write=WriteFailureInjector(1))  # 注入一次写失败

    j = _job(app)
    assert j["status"] == "pending"        # 已自动重排队，而非卡在 running
    assert j["attempts"] == 1
    assert "报告写入失败" in j["error"]
    assert _reports(app) == []             # 失败不留半份报告

    assert run_pending_jobs(db_path) == 1  # 写入恢复后再次领取并完成
    reports = _reports(app)
    assert len(reports) == 1 and reports[0]["status"] == "ok"
    assert _job(app)["status"] == "done"
    page = client.get("/batches/1").data.decode()
    assert "当前报告" in page and "计算中" not in page


def test_write_failure_limit_marks_failed_and_page_allows_retry(app, client):
    """连续写失败达上限：任务失败、无报告，页面明确显示失败并可重试当前版本。"""
    _upload(client)
    db_path = app.config["DATABASE"]

    for _ in range(3):                     # 默认上限 3 次，每次都写失败
        job = claim_job(db_path)
        assert job is not None
        process_job(db_path, job, before_write=WriteFailureInjector(1))

    j = _job(app)
    assert j["status"] == "failed" and j["attempts"] == 3
    assert _reports(app) == []             # 不留半份报告

    page = client.get("/batches/1").data.decode()
    assert "报告写入异常" in page and "计算中……" not in page
    assert "重试当前版本" in page

    resp = client.post("/batches/1/retry")  # 网页重试当前版本
    assert resp.status_code == 302
    assert run_pending_jobs(db_path) == 1   # 写入已恢复

    reports = _reports(app)
    assert len(reports) == 1 and reports[0]["status"] == "ok"
    page = client.get("/batches/1").data.decode()
    assert "当前报告" in page


def test_failed_report_write_is_also_retried(app, client):
    """非法输入的失败报告在写入时遇到瞬时故障，同样重试，最终一份完整失败报告。"""
    _upload(client, csv_text=CSV_BAD)
    db_path = app.config["DATABASE"]

    job = claim_job(db_path)
    process_job(db_path, job, before_write=WriteFailureInjector(1))
    assert _reports(app) == []
    assert _job(app)["status"] == "pending"

    run_pending_jobs(db_path)
    reports = _reports(app)
    assert len(reports) == 1
    assert reports[0]["status"] == "failed" and reports[0]["payload"] is None
    assert "与参考序列不符" in reports[0]["error"]


def test_invalid_input_fails_immediately_without_write_retry(app, client):
    """非法输入按原规则形成失败报告：确定性失败，不进入写失败重试循环。"""
    _upload(client, csv_text=CSV_BAD)
    run_pending_jobs(app.config["DATABASE"])
    j = _job(app)
    assert j["status"] == "failed" and j["attempts"] == 0
    reports = _reports(app)
    assert len(reports) == 1 and reports[0]["status"] == "failed"


def test_backoff_defers_retry_until_run_at(app, client):
    """退避：写失败后 run_at 未到时不可领取，到点后才重试。"""
    _upload(client)
    db_path = app.config["DATABASE"]

    job = claim_job(db_path)
    process_job(db_path, job, backoff_seconds=3600,
                before_write=WriteFailureInjector(1))
    assert _job(app)["status"] == "pending"
    assert claim_job(db_path) is None      # run_at 在一小时后，暂不可领取

    conn = _db(app)                        # 模拟退避到期
    conn.execute("UPDATE jobs SET run_at=datetime('now','-1 seconds') WHERE id=1")
    conn.commit()
    conn.close()
    assert run_pending_jobs(db_path) == 1
    assert _job(app)["status"] == "done"


# ---------------------------------------------------------------- 重复领取与重启

def test_duplicate_processing_same_job_yields_one_report(app, client):
    """重复领取/重复执行同一任务：撞 reports.job_id 唯一约束后幂等收场。"""
    _upload(client)
    db_path = app.config["DATABASE"]

    job = claim_job(db_path)
    process_job(db_path, job)              # 第一个执行者完成
    process_job(db_path, job)              # 模拟重复领取的第二个执行者

    reports = _reports(app)
    assert len(reports) == 1               # 同一任务至多一份报告
    assert _job(app)["status"] == "done"


def test_restart_requeues_interrupted_running_job(app, client, tmp_path):
    """进程重启：running 任务重新排队，且保留 attempts 计数。"""
    _upload(client)
    db_path = app.config["DATABASE"]
    job = claim_job(db_path)               # 任务进入 running 后"进程崩溃"
    assert _job(app)["status"] == "running"

    conn = connect(db_path)                # 模拟崩溃前已有一次写失败计数
    conn.execute("UPDATE jobs SET attempts=1 WHERE id=?", (job["id"],))
    conn.commit()
    conn.close()

    app2 = create_app({                    # 重启：init_app 恢复中断任务
        "TESTING": True, "DATABASE": db_path, "WORKER_ENABLED": False,
        "COMPUTE_DELAY_SECONDS": 0,
    })
    j = _job(app2)
    assert j["status"] == "pending" and j["run_at"] is None
    assert j["attempts"] == 1              # 计数保留，崩溃循环最终也会到上限

    assert run_pending_jobs(db_path) == 1
    assert len(_reports(app2)) == 1


def test_stale_running_job_reclaimed_by_worker(app, client):
    """worker 崩溃兜底：超时 running 任务被认领方回收重排。"""
    _upload(client)
    db_path = app.config["DATABASE"]
    job = claim_job(db_path)
    conn = _db(app)                        # 模拟 started_at 在 1 小时前
    conn.execute(
        "UPDATE jobs SET started_at=datetime('now','-3600 seconds') WHERE id=?",
        (job["id"],))
    conn.commit()
    conn.close()

    assert run_pending_jobs(db_path, stale_seconds=900) == 1
    assert _job(app)["status"] == "done"
    assert len(_reports(app)) == 1


def test_concurrent_enqueue_same_version_combo_deduped(app, client):
    """同版本组合的未完成任务在数据库层唯一：并发/重复排队不产生第二个任务。"""
    from app.views import _enqueue

    _upload(client)
    db = _db(app)
    _enqueue(db, 1)                        # 已有 pending 任务 → 不重复排
    db.execute("UPDATE jobs SET status='done' WHERE id=1")
    _enqueue(db, 1)                        # 已完成 → 可再排
    _enqueue(db, 1)                        # 新的 pending 存在 → 去重
    db.commit()
    n = db.execute(
        "SELECT COUNT(*) c FROM jobs WHERE batch_id=1 AND status IN ('pending','running')"
    ).fetchone()["c"]
    total = db.execute("SELECT COUNT(*) c FROM jobs WHERE batch_id=1").fetchone()["c"]
    db.close()
    assert n == 1 and total == 2


# ---------------------------------------------------------------- 更正交错

def test_correction_during_write_failure_keeps_versions_straight(app, client):
    """写失败重试期间提交更正：旧版本任务重试完成后仍不是当前结论。"""
    _upload(client)
    db_path = app.config["DATABASE"]

    job = claim_job(db_path)
    process_job(db_path, job, before_write=WriteFailureInjector(1))  # v1 写失败
    assert _job(app)["status"] == "pending"

    client.post("/batches/1/inputs/csv", data={   # 更正 → CSV v2 + 新任务
        "file": (io.BytesIO(b"ID,POS,REF,ALT\nd1,10,AA,A\n"), "new.csv"),
    })
    run_pending_jobs(db_path)              # 两个 pending 任务都完成

    reports = _reports(app)
    assert len(reports) == 2
    assert {(r["fasta_version"], r["csv_version"]) for r in reports} == {(1, 1), (1, 2)}
    page = client.get("/batches/1").data.decode()
    assert "d1" in page                    # 当前结论来自新版本
    old_page = client.get("/batches/1/reports/1").data.decode()
    assert "已过期" in old_page


# ---------------------------------------------------------------- 真实 worker 线程端到端

def test_worker_thread_recovers_from_injected_failures(tmp_path):
    """真实 worker 线程：注入连续写故障后自动恢复，页面最终显示当前报告。"""
    db_path = str(tmp_path / "live.db")
    app = create_app({
        "TESTING": True,
        "DATABASE": db_path,
        "WORKER_ENABLED": True,
        "COMPUTE_DELAY_SECONDS": 0,
        "WORKER_POLL_SECONDS": 0.05,
        "JOB_RETRY_BACKOFF_SECONDS": 0.2,
        "WORKER_INJECT_WRITE_FAILURES": 2,   # 前 2 次写入必失败
    })
    client = app.test_client()
    _upload(client)

    deadline = time.time() + 15
    job = None
    while time.time() < deadline:
        conn = connect(db_path)
        job = conn.execute("SELECT * FROM jobs WHERE id=1").fetchone()
        n = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]
        conn.close()
        if job["status"] == "done" and n == 1:
            break
        time.sleep(0.1)

    assert job["status"] == "done"         # 写故障恢复后自动完成，无需重启
    assert job["attempts"] == 2
    page = client.get("/batches/1").data.decode()
    assert "当前报告" in page
