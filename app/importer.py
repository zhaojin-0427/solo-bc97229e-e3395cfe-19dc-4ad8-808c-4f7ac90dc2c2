"""CSV 批次导入。

规则：
- 列：站点 / 采样日 / 检测项目 / 结果（同时兼容英文表头）
- 批内前三列组合唯一，重复即整批拒绝（全有或全无）
- 成功导入分配递增批次号，正式结果按同键 upsert，仅接受更大批次号的值
"""
from __future__ import annotations

import csv
import io
import sqlite3

from .db import utcnow_iso

# 表头别名 -> 规范字段
ALIASES = {
    "站点": "station",
    "station": "station",
    "采样日": "sample_day",
    "sample_day": "sample_day",
    "sample_day_date": "sample_day",
    "检测项目": "analyte",
    "analyte": "analyte",
    "item": "analyte",
    "结果": "result",
    "result": "result",
}
CANONICAL = ("station", "sample_day", "analyte", "result")


class ImportError_(Exception):
    """面向页面的导入错误，信息直接展示给用户。"""


def parse_csv(data: bytes) -> list[dict]:
    """解析上传的 CSV 字节流，返回规范化记录列表。"""
    # 去掉可能存在的 UTF-8 BOM
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("gbk")  # 兼容 Excel 导出的中文 CSV

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ImportError_("CSV 文件为空")

    cols = []
    for name in header:
        key = ALIASES.get(name.strip().lower())
        if key is None:
            raise ImportError_(f"无法识别的表头列：{name!r}，应为 站点/采样日/检测项目/结果")
        cols.append(key)
    if len(set(cols)) != len(cols):
        raise ImportError_("表头存在重复列")
    if tuple(cols) != CANONICAL:
        missing = [c for c in CANONICAL if c not in cols]
        raise ImportError_(f"表头列不完整，缺少：{','.join(missing)}")

    records: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for line_no, row in enumerate(reader, start=2):
        if not row or all(not c.strip() for c in row):
            continue  # 跳过空行
        if len(row) != 4:
            raise ImportError_(f"第 {line_no} 行应有 4 列，实际 {len(row)} 列")
        station, sample_day, analyte, raw_result = (c.strip() for c in row)
        if not station or not sample_day or not analyte:
            raise ImportError_(f"第 {line_no} 行站点/采样日/检测项目不能为空")
        try:
            result = float(raw_result)
        except ValueError:
            raise ImportError_(f"第 {line_no} 行结果不是数字：{raw_result!r}")
        key = (station, sample_day, analyte)
        if key in seen:
            raise ImportError_(
                f"第 {line_no} 行与批内更早的行键值重复：{key[0]} / {key[1]} / {key[2]}"
            )
        seen.add(key)
        records.append(
            {"station": station, "sample_day": sample_day, "analyte": analyte, "result": result}
        )

    if not records:
        raise ImportError_("CSV 中没有任何数据行")
    return records


def import_batch(conn: sqlite3.Connection, filename: str, records: list[dict]) -> int:
    """在单个事务内完成整批导入，返回批次号。"""
    now = utcnow_iso()
    try:
        cur = conn.execute(
            "INSERT INTO batches (filename, row_count, status, imported_at) VALUES (?, ?, 'active', ?)",
            (filename, len(records), now),
        )
        batch_id = cur.lastrowid
        conn.executemany(
            "INSERT INTO raw_records (batch_id, station, sample_day, analyte, result)"
            " VALUES (?, ?, ?, ?, ?)",
            [(batch_id, r["station"], r["sample_day"], r["analyte"], r["result"]) for r in records],
        )
        # 同键取最新批次：仅当新批次号大于当前来源批次时才覆盖，
        # 防止任何迟到/乱序写入覆盖更新值。
        conn.executemany(
            """
            INSERT INTO current_results (station, sample_day, analyte, result, source_batch, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (station, sample_day, analyte) DO UPDATE SET
                result       = excluded.result,
                source_batch = excluded.source_batch,
                updated_at   = excluded.updated_at
            WHERE excluded.source_batch > current_results.source_batch
            """,
            [
                (r["station"], r["sample_day"], r["analyte"], r["result"], batch_id, now)
                for r in records
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return batch_id
