"""生成演示用 CSV 批次：
batch1 正常批 -> batch2 整批有误（含 batch1 同键错误值 + 新键）-> batch3 修正批。
"""
import csv
import os

DEMO_DIR = os.path.join(os.path.dirname(__file__), "..", "demo_data")

BATCH1 = [
    ("城东站点", "2026-11-01", "PM2.5", 32.1),
    ("城东站点", "2026-11-01", "SO2", 9.0),
    ("城西站点", "2026-11-01", "PM2.5", 41.5),
]
BATCH2 = [  # 仪器整批故障：同键值错误，且引入一个只存在于该批的键
    ("城东站点", "2026-11-01", "PM2.5", 999.9),
    ("城西站点", "2026-11-01", "PM2.5", 888.8),
    ("城南站点", "2026-11-02", "NO2", 777.7),
]
BATCH3 = [  # 后续正确数据
    ("城东站点", "2026-11-01", "PM2.5", 33.0),
    ("城北站点", "2026-11-02", "O3", 58.2),
]


def write(name, rows):
    os.makedirs(DEMO_DIR, exist_ok=True)
    path = os.path.join(DEMO_DIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["站点", "采样日", "检测项目", "结果"])
        w.writerows(rows)
    print("written", path)


if __name__ == "__main__":
    write("batch1_ok.csv", BATCH1)
    write("batch2_bad_instrument.csv", BATCH2)
    write("batch3_corrected.csv", BATCH3)
