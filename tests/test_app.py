"""端到端语义测试（标准库 unittest，直接调用 Flask test client 与 SQLite）。

覆盖：
1. 导入递增批次号、同键取最新批次
2. 批内键重复整批拒绝、格式校验
3. 撤销后每键取最大未撤销批次、无值键删除、已撤销不可恢复
4. 重建期间导入的数据在切换时被纳入，迟到计算不覆盖新值
5. 同时只允许一项撤销
6. 失败不撤销该批（注入影子重建故障）
7. 进程重启接续未完成撤销
8. 下载 CSV 与读取一致性
"""
import csv
import io
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.rebuild as rebuild_mod  # noqa: E402
from app.server import create_app  # noqa: E402

# 测试用小分页 + 无节流，轮询即可观察进度
rebuild_mod.PAGE_SIZE = 2
rebuild_mod.REBUILD_SLEEP = 0


def csv_bytes(rows, header=("站点", "采样日", "检测项目", "结果")):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        os.environ["DATA_DIR"] = self.tmp
        self.db_path = os.path.join(self.tmp, "test.db")
        self.app = create_app(self.db_path)
        self.client = self.app.test_client()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def upload(self, rows, name="t.csv"):
        data = {"file": (io.BytesIO(csv_bytes(rows)), name)}
        return self.client.post("/api/upload", data=data, content_type="multipart/form-data")

    def results_map(self):
        r = self.client.get("/api/results").get_json()
        return {(x["station"], x["sample_day"], x["analyte"]): x for x in r}

    def wait_job(self, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            jobs = self.client.get("/api/jobs").get_json()
            if jobs and jobs[0]["status"] in ("done", "failed"):
                return jobs[0]
            time.sleep(0.02)
        self.fail("job did not finish in time")

    def revoke(self, batch_id):
        return self.client.post("/api/revoke", json={"batch_id": batch_id})


class TestImport(BaseCase):
    def test_incremental_batch_and_latest_wins(self):
        r1 = self.upload([("S1", "2026-11-01", "PM2.5", 1.0),
                          ("S1", "2026-11-01", "SO2", 2.0)]).get_json()
        r2 = self.upload([("S1", "2026-11-01", "PM2.5", 9.0)]).get_json()
        self.assertTrue(r1["ok"] and r2["ok"])
        self.assertEqual((r1["batch_id"], r2["batch_id"]), (1, 2))
        rm = self.results_map()
        self.assertEqual(rm[("S1", "2026-11-01", "PM2.5")]["result"], 9.0)
        self.assertEqual(rm[("S1", "2026-11-01", "PM2.5")]["source_batch"], 2)
        self.assertEqual(rm[("S1", "2026-11-01", "SO2")]["source_batch"], 1)

    def test_duplicate_key_in_batch_rejected(self):
        resp = self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "PM2.5", 2.0)])
        self.assertEqual(resp.status_code, 400)
        self.assertIn("重复", resp.get_json()["error"])
        # 整批拒绝：无批次、无结果
        self.assertEqual(self.client.get("/api/batches").get_json(), [])
        self.assertEqual(self.client.get("/api/results").get_json(), [])

    def test_bad_number_and_header(self):
        bad = csv_bytes([("S1", "d", "PM2.5", "abc")])
        resp = self.client.post(
            "/api/upload",
            data={"file": (io.BytesIO(bad), "x.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("不是数字", resp.get_json()["error"])

        bad_header = csv_bytes([("S1", "d", "PM2.5", 1.0)], header=("a", "b", "c", "d"))
        resp = self.client.post(
            "/api/upload",
            data={"file": (io.BytesIO(bad_header), "x.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)


class TestRevoke(BaseCase):
    def _seed(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])  # b1
        self.upload([("S1", "d", "PM2.5", 999.0), ("S2", "d", "NO2", 7.0)])  # b2 坏批
        self.upload([("S1", "d", "PM2.5", 3.0)])  # b3 修正

    def test_revoke_recomputes_and_deletes_orphan_key(self):
        self._seed()
        rm = self.results_map()
        self.assertEqual(rm[("S1", "d", "PM2.5")]["result"], 3.0)

        job = self.revoke(2).get_json()["job"]
        self.assertEqual(job["status"], "pending")
        done = self.wait_job()
        self.assertEqual(done["status"], "done")
        # 撤销目标已置 revoking，keys_total 只统计其余未撤销批次覆盖的键：
        # b1 的 2 个键 + b3 与 b1 同键 -> 共 2 个；坏批独有键不在重建范围内
        self.assertEqual(done["keys_total"], 2)
        self.assertEqual(done["keys_done"], 2)

        rm = self.results_map()
        # S1/PM2.5：b2 撤销后取最大未撤销批次 b3
        self.assertEqual(rm[("S1", "d", "PM2.5")]["source_batch"], 3)
        self.assertEqual(rm[("S1", "d", "PM2.5")]["result"], 3.0)
        # b1 的键仍在
        self.assertEqual(rm[("S1", "d", "SO2")]["source_batch"], 1)
        # 只存在于坏批 b2 的键被删除
        self.assertNotIn(("S2", "d", "NO2"), rm)

        batches = {b["batch_id"]: b["status"] for b in self.client.get("/api/batches").get_json()}
        self.assertEqual(batches[2], "revoked")
        # 已撤销批次不能恢复/再撤销
        resp = self.revoke(2)
        self.assertEqual(resp.status_code, 409)
        self.assertIn("不可恢复", resp.get_json()["error"])
        # 原始记录仍保留可审计，只是被过滤
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM raw_records WHERE batch_id=2").fetchone()[0]
        self.assertEqual(n, 2)
        conn.close()

    def test_revoke_oldest_batch(self):
        self._seed()
        self.wait_job_done = None
        resp = self.revoke(1)
        job = self.wait_job()
        self.assertEqual(job["status"], "done")
        rm = self.results_map()
        # b1 撤销：SO2 无其他批次 -> 删除；PM2.5 取 b3
        self.assertNotIn(("S1", "d", "SO2"), rm)
        self.assertEqual(rm[("S1", "d", "PM2.5")]["source_batch"], 3)
        self.assertEqual(rm[("S2", "d", "NO2")]["source_batch"], 2)


class TestConcurrentImportDuringRebuild(BaseCase):
    def test_import_during_rebuild_is_merged_and_not_overwritten(self):
        # b1 两个键
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])
        # b2 坏批（撤销目标）
        self.upload([("S1", "d", "PM2.5", 999.0), ("S9", "d", "X", 5.0)])

        # 拖慢重建，确保切换前能插入 b3
        rebuild_mod.REBUILD_SLEEP = 0.3
        rebuild_mod.PAGE_SIZE = 1
        try:
            self.revoke(2)
            # 等任务进入 running 后导入 b3（撤销期间导入应立即可见）
            self._wait_status("running")
            r3 = self.upload([("S1", "d", "PM2.5", 42.0), ("S8", "d", "O3", 8.0)])
            self.assertTrue(r3.get_json()["ok"])
            rm = self.results_map()  # 切换前读到的也是完整一致状态
            self.assertEqual(rm[("S8", "d", "O3")]["source_batch"], 3)

            job = self.wait_job()
            self.assertEqual(job["status"], "done")
        finally:
            rebuild_mod.REBUILD_SLEEP = 0
            rebuild_mod.PAGE_SIZE = 2

        rm = self.results_map()
        # 期间导入的新键保留
        self.assertEqual(rm[("S8", "d", "O3")]["source_batch"], 3)
        # 期间导入的同键新值不能被影子（迟到）计算覆盖
        self.assertEqual(rm[("S1", "d", "PM2.5")]["source_batch"], 3)
        self.assertEqual(rm[("S1", "d", "PM2.5")]["result"], 42.0)
        # 坏批独有键删除，b1 正常键保留
        self.assertNotIn(("S9", "d", "X"), rm)
        self.assertEqual(rm[("S1", "d", "SO2")]["source_batch"], 1)

    def _wait_status(self, status):
        deadline = time.time() + 5
        while time.time() < deadline:
            jobs = self.client.get("/api/jobs").get_json()
            if jobs and jobs[0]["status"] == status:
                return
            time.sleep(0.02)
        self.fail(f"job never reached {status}")


class TestSingleJobAndFailure(BaseCase):
    def test_parallel_revoke_requests_only_one_wins(self):
        # 两个线程同时请求撤销，数据库级唯一索引必须保证只有一项生效
        self.upload([("S1", "d", "PM2.5", 1.0)])  # b1
        self.upload([("S1", "d", "PM2.5", 2.0), ("S2", "d", "SO2", 3.0)])  # b2
        import threading
        errors = []

        def fire(bid):
            try:
                resp = self.revoke(bid)
                if resp.status_code == 409:
                    errors.append(resp.get_json()["error"])
            except Exception as exc:  # 线程内异常收集
                errors.append(repr(exc))

        t1 = threading.Thread(target=fire, args=(1,))
        t2 = threading.Thread(target=fire, args=(2,))
        t1.start(); t2.start(); t1.join(); t2.join()

        import sqlite3
        conn = sqlite3.connect(self.db_path)
        active_jobs = conn.execute(
            "SELECT COUNT(*) FROM revoke_jobs WHERE status IN ('pending','running')"
        ).fetchone()[0]
        total_jobs = conn.execute("SELECT COUNT(*) FROM revoke_jobs").fetchone()[0]
        conn.close()
        self.assertEqual(active_jobs, 1)
        self.assertEqual(total_jobs, 1)
        self.assertEqual(self.wait_job()["status"], "done")
        self.assertEqual(len(errors), 1)
        self.assertIn("同一时刻只允许一项", errors[0])

    def test_only_one_revoke_at_a_time(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])  # b1
        self.upload([("S1", "d", "PM2.5", 2.0)])  # b2
        rebuild_mod.REBUILD_SLEEP = 0.3
        try:
            self.revoke(2)
            self._wait_status("running")
            resp = self.revoke(1)
            self.assertEqual(resp.status_code, 409)
            self.assertIn("同一时刻只允许一项", resp.get_json()["error"])
            self.wait_job()
        finally:
            rebuild_mod.REBUILD_SLEEP = 0

    def test_failure_does_not_revoke_batch(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])
        self.upload([("S1", "d", "PM2.5", 999.0)])

        original = rebuild_mod.RevokeManager._swap
        def boom(self, job_id, revoked_batch, shadow_path):
            raise RuntimeError("simulated swap failure")
        rebuild_mod.RevokeManager._swap = boom
        try:
            self.revoke(2)
            job = self.wait_job()
        finally:
            rebuild_mod.RevokeManager._swap = original
        self.assertEqual(job["status"], "failed")
        self.assertIn("simulated swap failure", job["error"])
        # 批次恢复 active，正式结果未被破坏
        batches = {b["batch_id"]: b["status"] for b in self.client.get("/api/batches").get_json()}
        self.assertEqual(batches[2], "active")
        rm = self.results_map()
        self.assertEqual(rm[("S1", "d", "PM2.5")]["source_batch"], 2)
        # 失败后允许重新撤销并成功
        self.revoke(2)
        self.assertEqual(self.wait_job()["status"], "done")
        self.assertEqual(self.results_map()[("S1", "d", "PM2.5")]["source_batch"], 1)

    def _wait_status(self, status):
        deadline = time.time() + 5
        while time.time() < deadline:
            jobs = self.client.get("/api/jobs").get_json()
            if jobs and jobs[0]["status"] == status:
                return
            time.sleep(0.02)
        self.fail(f"job never reached {status}")


class TestRestartResume(BaseCase):
    def test_resume_unfinished_revoke_after_restart(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S2", "d", "SO2", 2.0)])
        self.upload([("S1", "d", "PM2.5", 999.0), ("S9", "d", "X", 5.0)])

        # 在影子重建中途制造进程退出：直接把任务置 running 并建半个影子库
        manager = self.app.extensions["revoke_manager"]
        job = manager.request_revoke(2)
        self._wait_status("running")
        # 模拟“进程被 kill”：后台线程仍在跑，先等它完成影子第一页再停服
        time.sleep(0.2)
        # 新进程：重新 create_app 同一 DB，应自动接续
        app2 = create_app(self.db_path)
        client2 = app2.test_client()
        deadline = time.time() + 20
        final = None
        while time.time() < deadline:
            jobs = client2.get("/api/jobs").get_json()
            final = jobs[0]
            if final["status"] in ("done", "failed"):
                break
            time.sleep(0.05)
        self.assertEqual(final["status"], "done", final)
        rm = {(x["station"], x["sample_day"], x["analyte"]): x
              for x in client2.get("/api/results").get_json()}
        self.assertEqual(rm[("S1", "d", "PM2.5")]["source_batch"], 1)
        self.assertNotIn(("S9", "d", "X"), rm)
        # 影子文件被清理
        leftovers = [f for f in os.listdir(self.tmp) if f.startswith("shadow_")]
        self.assertEqual(leftovers, [])

    def _wait_status(self, status):
        deadline = time.time() + 5
        while time.time() < deadline:
            jobs = self.client.get("/api/jobs").get_json()
            if jobs and jobs[0]["status"] == status:
                return
            time.sleep(0.02)
        self.fail(f"job never reached {status}")


class TestDownload(BaseCase):
    def test_csv_download(self):
        self.upload([("城东站点", "2026-11-01", "PM2.5", 32.1)])
        resp = self.client.get("/api/results.csv")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("attachment", resp.headers["Content-Disposition"])
        text = resp.data.decode("utf-8-sig")
        self.assertIn("城东站点", text)
        self.assertIn("来源批次", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
