"""批次撤销：后台影子重建 + 单事务原子切换。

流程
----
1. 请求撤销：批次 active -> revoking，记录任务与「快照批次号」
   （撤销开始时正式库中最大的批次号），之后仍可正常导入新批次。
2. 后台重建：在独立的影子 SQLite 文件中重算每个键的有效最新值
   （仅统计未撤销批次），不占用正式库的写事务；正式库只做秒级进度更新。
3. 原子切换：单个 BEGIN IMMEDIATE 事务内 ATTACH 影子库，
   清空并重写 current_results，合并快照之后导入的新批次，
   批次置 revoked、任务置 done。读者只能看到切换前或切换后的完整状态。
4. 失败：任务 failed、批次恢复 active，已撤销效果不生效，可再次请求。
5. 重启：启动时自动接续 pending / running 的任务（影子表可断点续建）。

同一时刻只允许一个撤销任务（revoke_jobs 中存在未结束任务即拒绝）。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

from .db import connect, utcnow_iso

# 每批处理的键数量与节流，便于演示进度；生产可调大/调小
PAGE_SIZE = int(os.environ.get("REBUILD_PAGE_SIZE", "10"))
REBUILD_SLEEP = float(os.environ.get("REBUILD_SLEEP", "0.15"))


class RevokeManager:
    def __init__(self, app):
        self.app = app
        self.db_path = app.config["DB_PATH"]
        self.data_dir = app.config["DATA_DIR"]
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ---------- 对外入口 ----------

    def request_revoke(self, batch_id: int) -> dict:
        """登记一个撤销任务并启动后台线程。返回任务信息字典。"""
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise LookupError(f"批次 {batch_id} 不存在")
            if row["status"] != "active":
                raise ValueError(
                    f"批次 {batch_id} 当前状态为 {row['status']}，"
                    "只有 active 批次可以撤销（已撤销批次不可恢复）"
                )
            busy = conn.execute(
                "SELECT COUNT(*) AS n FROM revoke_jobs WHERE status IN ('pending', 'running')"
            ).fetchone()["n"]
            if busy:
                raise RuntimeError("已有撤销任务在进行中，同一时刻只允许一项")

            snapshot = conn.execute(
                "SELECT COALESCE(MAX(batch_id), 0) AS m FROM batches"
            ).fetchone()["m"]
            now = utcnow_iso()
            try:
                cur = conn.execute(
                    "INSERT INTO revoke_jobs (batch_id, status, snapshot_batch, created_at)"
                    " VALUES (?, 'pending', ?, ?)",
                    (batch_id, snapshot, now),
                )
                job_id = cur.lastrowid
                conn.execute(
                    "UPDATE batches SET status = 'revoking' WHERE batch_id = ?", (batch_id,)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                raise RuntimeError("已有撤销任务在进行中，同一时刻只允许一项")
        finally:
            conn.close()

        self.spawn(job_id)
        return self.get_job(job_id)

    def spawn(self, job_id: int) -> None:
        """确保有一个后台工作线程处理任务（启动恢复 / 新任务共用）。"""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            t = threading.Thread(target=self._run_guarded, args=(job_id,), daemon=True)
            self._thread = t
        t.start()

    def resume_on_startup(self) -> int | None:
        """进程启动后接续未完成的撤销任务，返回恢复的任务 id（没有则 None）。"""
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT job_id FROM revoke_jobs WHERE status IN ('pending', 'running')"
                " ORDER BY job_id LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if row is not None:
            self.spawn(row["job_id"])
            return row["job_id"]
        return None

    def get_job(self, job_id: int) -> dict | None:
        conn = connect(self.db_path)
        try:
            row = conn.execute("SELECT * FROM revoke_jobs WHERE job_id = ?", (job_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_jobs(self, limit: int = 20) -> list[dict]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM revoke_jobs ORDER BY job_id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ---------- 后台工作线程 ----------

    def _run_guarded(self, job_id: int) -> None:
        with self.app.app_context():
            try:
                self._run(job_id)
            except Exception as exc:  # 任何失败都不能让批次“半撤销”
                self._fail(job_id, exc)

    def _run(self, job_id: int) -> None:
        conn = connect(self.db_path)
        try:
            job = conn.execute("SELECT * FROM revoke_jobs WHERE job_id = ?", (job_id,)).fetchone()
            if job is None:
                return
            if job["status"] == "done":
                return
            batch_id = job["batch_id"]
            shadow_path = job["shadow_path"] or os.path.join(
                self.data_dir, f"shadow_job{job_id}.db"
            )
            conn.execute(
                "UPDATE revoke_jobs SET status = 'running', started_at = COALESCE(started_at, ?),"
                " shadow_path = ? WHERE job_id = ?",
                (utcnow_iso(), shadow_path, job_id),
            )
            conn.commit()
        finally:
            conn.close()

        self._build_shadow(job_id, batch_id, shadow_path)
        self._swap(job_id, batch_id, shadow_path)

        conn = connect(self.db_path)
        try:
            conn.execute(
                "UPDATE revoke_jobs SET status = 'done', finished_at = ? WHERE job_id = ?",
                (utcnow_iso(), job_id),
            )
            conn.commit()
        finally:
            conn.close()
        try:
            os.remove(shadow_path)
        except OSError:
            pass

    def _fail(self, job_id: int, exc: Exception) -> None:
        self.app.logger.exception("revoke job %s failed", job_id)
        conn = connect(self.db_path)
        shadow_path = None
        try:
            row = conn.execute(
                "SELECT status, batch_id, shadow_path FROM revoke_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if row is None or row["status"] == "done":
                return
            shadow_path = row["shadow_path"]
            conn.execute(
                "UPDATE revoke_jobs SET status = 'failed', error = ?, finished_at = ?"
                " WHERE job_id = ?",
                (repr(exc), utcnow_iso(), job_id),
            )
            # 失败不撤销该批：批次恢复 active，撤销完全不生效
            conn.execute(
                "UPDATE batches SET status = 'active' WHERE batch_id = ? AND status = 'revoking'",
                (row["batch_id"],),
            )
            conn.commit()
        finally:
            conn.close()
        # 清理失败任务的影子文件（重试会生成新的）
        if shadow_path:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(shadow_path + suffix)
                except OSError:
                    pass

    # ---------- 步骤 1：影子库重建（不占正式库写事务） ----------

    def _build_shadow(self, job_id: int, revoked_batch: int, shadow_path: str) -> None:
        """在影子文件中按键重算有效最新批次；文件已存在时可断点续建。"""
        sh = sqlite3.connect(shadow_path)
        sh.execute("PRAGMA journal_mode=WAL")
        sh.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_results (
                station      TEXT NOT NULL,
                sample_day   TEXT NOT NULL,
                analyte      TEXT NOT NULL,
                result       REAL NOT NULL,
                source_batch INTEGER NOT NULL,
                PRIMARY KEY (station, sample_day, analyte)
            )
            """
        )
        sh.commit()

        main = connect(self.db_path)
        try:
            total = main.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT station, sample_day, analyte
                    FROM raw_records r
                    JOIN batches b ON b.batch_id = r.batch_id
                    WHERE b.status = 'active'
                )
                """
            ).fetchone()[0]
            main.execute(
                "UPDATE revoke_jobs SET keys_total = ? WHERE job_id = ?", (total, job_id)
            )
            main.commit()

            while True:
                last = sh.execute(
                    "SELECT station, sample_day, analyte FROM shadow_results"
                    " ORDER BY station ASC, sample_day ASC, analyte ASC"
                    " LIMIT 1 OFFSET (SELECT COUNT(*) FROM shadow_results) - 1"
                ).fetchone()
                # 键集分页（keyset pagination），每次只重算一页键
                if last is None:
                    page = main.execute(
                        """
                        WITH ranked AS (
                            SELECT r.station, r.sample_day, r.analyte, r.result, r.batch_id,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY r.station, r.sample_day, r.analyte
                                       ORDER BY r.batch_id DESC
                                   ) AS rn
                            FROM raw_records r
                            JOIN batches b ON b.batch_id = r.batch_id
                            WHERE b.status = 'active'
                        )
                        SELECT station, sample_day, analyte, result, batch_id
                        FROM ranked WHERE rn = 1
                        ORDER BY station, sample_day, analyte
                        LIMIT ?
                        """,
                        (PAGE_SIZE,),
                    ).fetchall()
                else:
                    page = main.execute(
                        """
                        WITH ranked AS (
                            SELECT r.station, r.sample_day, r.analyte, r.result, r.batch_id,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY r.station, r.sample_day, r.analyte
                                       ORDER BY r.batch_id DESC
                                   ) AS rn
                            FROM raw_records r
                            JOIN batches b ON b.batch_id = r.batch_id
                            WHERE b.status = 'active'
                        )
                        SELECT station, sample_day, analyte, result, batch_id
                        FROM ranked WHERE rn = 1
                          AND (station, sample_day, analyte) > (?, ?, ?)
                        ORDER BY station, sample_day, analyte
                        LIMIT ?
                        """,
                        (last[0], last[1], last[2], PAGE_SIZE),
                    ).fetchall()

                if not page:
                    break
                sh.executemany(
                    "INSERT OR REPLACE INTO shadow_results"
                    " (station, sample_day, analyte, result, source_batch)"
                    " VALUES (?, ?, ?, ?, ?)",
                    [tuple(r) for r in page],
                )
                sh.commit()
                done = sh.execute("SELECT COUNT(*) FROM shadow_results").fetchone()[0]
                # 正式库仅承担秒级进度写入（WAL 下极短，不阻塞重建逻辑）
                main.execute(
                    "UPDATE revoke_jobs SET keys_done = ? WHERE job_id = ?", (done, job_id)
                )
                main.commit()
                time.sleep(REBUILD_SLEEP)
        finally:
            main.close()
            sh.close()

    # ---------- 步骤 2：原子切换 ----------

    def _swap(self, job_id: int, revoked_batch: int, shadow_path: str) -> None:
        """单个写事务内完成正式表替换 + 期间导入合并 + 撤销状态生效。"""
        conn = connect(self.db_path)
        try:
            job = conn.execute(
                "SELECT snapshot_batch FROM revoke_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            snapshot = job["snapshot_batch"]

            conn.execute("ATTACH DATABASE ? AS shadow", (shadow_path,))
            # IMMEDIATE 立刻取得写锁：切换期间所有导入写事务排队，
            # 读者在 WAL 下继续读到完整的旧快照
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM current_results")
                # 影子结果 = 撤销生效后、快照时刻的完整正式状态
                conn.execute(
                    "INSERT INTO current_results"
                    " (station, sample_day, analyte, result, source_batch, updated_at)"
                    " SELECT station, sample_day, analyte, result, source_batch, ?"
                    " FROM shadow.shadow_results",
                    (utcnow_iso(),),
                )
                # 重建期间成功导入的数据（快照之后的批次）必须纳入：
                # 逐键只接受批次号更大的值，迟到计算不会覆盖新值
                conn.execute(
                    """
                    INSERT INTO current_results
                        (station, sample_day, analyte, result, source_batch, updated_at)
                    SELECT r.station, r.sample_day, r.analyte, r.result, r.batch_id, ?
                    FROM raw_records r
                    JOIN (
                        SELECT station, sample_day, analyte, MAX(batch_id) AS max_batch
                        FROM raw_records
                        WHERE batch_id > ?
                        GROUP BY station, sample_day, analyte
                    ) m ON m.station = r.station
                       AND m.sample_day = r.sample_day
                       AND m.analyte = r.analyte
                       AND m.max_batch = r.batch_id
                    ON CONFLICT (station, sample_day, analyte) DO UPDATE SET
                        result       = excluded.result,
                        source_batch = excluded.source_batch,
                        updated_at   = excluded.updated_at
                    WHERE excluded.source_batch > current_results.source_batch
                    """,
                    (utcnow_iso(), snapshot),
                )
                # 没有任何未撤销记录的键随 DELETE 已自然消失（即“无则删除”）
                conn.execute(
                    "UPDATE batches SET status = 'revoked' WHERE batch_id = ?",
                    (revoked_batch,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE shadow")
        finally:
            conn.close()
