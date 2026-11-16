"""Flask 应用工厂。"""
from __future__ import annotations

import os
import threading

from flask import Flask

from . import db, views, worker


def create_app(config=None):
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-not-secret"),
        DATABASE=os.environ.get("APP_DATABASE")
        or os.path.join(app.instance_path, "app.db"),
        # 模拟耗时计算的秒数；此期间输入更正照常提交（演示并发语义的窗口）
        COMPUTE_DELAY_SECONDS=float(os.environ.get("COMPUTE_DELAY_SECONDS", "2")),
        WORKER_ENABLED=True,
        WORKER_POLL_SECONDS=0.2,
        # 报告写入故障恢复：连续失败上限、退避基数、running 过期回收阈值
        JOB_MAX_WRITE_ATTEMPTS=int(os.environ.get("JOB_MAX_WRITE_ATTEMPTS", "3")),
        JOB_RETRY_BACKOFF_SECONDS=float(os.environ.get("JOB_RETRY_BACKOFF_SECONDS", "1")),
        JOB_STALE_SECONDS=float(os.environ.get("JOB_STALE_SECONDS", "900")),
        # 演练用：前 N 次报告写入必失败（模拟瞬时写故障），默认关闭
        WORKER_INJECT_WRITE_FAILURES=int(os.environ.get("WORKER_INJECT_WRITE_FAILURES", "0")),
        MAX_CONTENT_LENGTH=2 * 1024 * 1024,  # 上传总量上限 2MB
    )
    if config:
        app.config.update(config)

    db.init_app(app)
    app.register_blueprint(views.bp)

    if app.config["WORKER_ENABLED"]:
        inject = app.config["WORKER_INJECT_WRITE_FAILURES"]
        hook = worker.WriteFailureInjector(inject) if inject > 0 else None
        t = threading.Thread(
            target=worker.worker_loop,
            args=(app.config["DATABASE"],
                  app.config["COMPUTE_DELAY_SECONDS"],
                  app.config["WORKER_POLL_SECONDS"]),
            kwargs=dict(
                max_attempts=app.config["JOB_MAX_WRITE_ATTEMPTS"],
                backoff_seconds=app.config["JOB_RETRY_BACKOFF_SECONDS"],
                stale_seconds=app.config["JOB_STALE_SECONDS"],
                before_write=hook,
            ),
            daemon=True,
            name="job-worker",
        )
        t.start()
    return app
