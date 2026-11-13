"""Flask 应用工厂与 HTTP 路由。

页面/接口：
- GET  /                 单页 Web 入口（上传、撤销、进度、当前结果、下载）
- POST /api/upload       上传 CSV 批次
- POST /api/revoke       请求撤销某批次
- GET  /api/jobs         撤销任务与进度
- GET  /api/batches      批次列表
- GET  /api/results      当前正式结果 JSON
- GET  /api/results.csv  下载当前结果 CSV
"""
from __future__ import annotations

import csv
import io
import os

from flask import Flask, Response, jsonify, render_template, request

from .db import close_db, get_db, init_db
from .importer import ImportError_, import_batch, parse_csv
from .rebuild import RevokeManager


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    data_dir = os.environ.get("DATA_DIR", "/data")
    app.config["DATA_DIR"] = data_dir
    app.config["DB_PATH"] = db_path or os.path.join(data_dir, "warehouse.db")

    init_db(app.config["DB_PATH"])
    manager = RevokeManager(app)
    app.extensions["revoke_manager"] = manager
    manager.resume_on_startup()  # 重启接续未完成的撤销

    app.teardown_appcontext(close_db)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.post("/api/upload")
    def upload():
        f = request.files.get("file")
        if f is None or not f.filename:
            return jsonify({"ok": False, "error": "请选择要上传的 CSV 文件"}), 400
        try:
            records = parse_csv(f.read())
            conn = get_db()
            batch_id = import_batch(conn, f.filename, records)
        except ImportError_ as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:  # 唯一性约束等意外错误，整批回滚
            return jsonify({"ok": False, "error": f"导入失败，已整批回滚：{exc!r}"}), 400
        return jsonify(
            {
                "ok": True,
                "batch_id": batch_id,
                "row_count": len(records),
                "message": f"批次 {batch_id} 导入成功，共 {len(records)} 条",
            }
        )

    @app.post("/api/revoke")
    def revoke():
        body = request.get_json(silent=True) or {}
        try:
            batch_id = int(body["batch_id"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"ok": False, "error": "请求体需要整数 batch_id"}), 400
        try:
            job = manager.request_revoke(batch_id)
        except LookupError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 404
        except (ValueError, RuntimeError) as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409
        return jsonify({"ok": True, "job": job, "message": f"批次 {batch_id} 已进入后台撤销"})

    @app.get("/api/batches")
    def batches():
        conn = get_db()
        rows = conn.execute(
            """
            SELECT b.batch_id, b.filename, b.row_count, b.status, b.imported_at,
                   (SELECT COUNT(*) FROM revoke_jobs j
                     WHERE j.batch_id = b.batch_id AND j.status = 'done') AS revoked_once
            FROM batches b ORDER BY b.batch_id
            """
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/jobs")
    def jobs():
        return jsonify(manager.list_jobs())

    @app.get("/api/results")
    def results():
        conn = get_db()
        rows = conn.execute(
            "SELECT station, sample_day, analyte, result, source_batch"
            " FROM current_results ORDER BY station, sample_day, analyte"
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/results.csv")
    def results_csv():
        conn = get_db()
        rows = conn.execute(
            "SELECT station, sample_day, analyte, result, source_batch"
            " FROM current_results ORDER BY station, sample_day, analyte"
        ).fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["站点", "采样日", "检测项目", "结果", "来源批次"])
        for r in rows:
            writer.writerow([r["station"], r["sample_day"], r["analyte"], r["result"], r["source_batch"]])
        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=current_results.csv"},
        )

    return app
