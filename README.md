# 环境检测数仓 · 批次导入与撤销系统

Flask + SQLite 实现的单容器 Web 应用：保存仪器原始 CSV 批次，支持整批撤销（后台重建 +
原子切换），撤销期间不阻塞导入，读者永远只能看到切换前或切换后的完整一致状态。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

数据持久化在命名卷 `warehouse_data`（容器内 `/data`）。停止后重新 `up` 会自动接续
未完成的撤销任务。

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
DATA_DIR=./data python -m gunicorn --workers=1 --threads=4 --bind=0.0.0.0:8000 wsgi:app
```

> 必须单 worker 多线程：撤销的后台线程与"同时只允许一项"约束依赖单进程；
> SQLite WAL 下读不阻塞写、写事务串行排队。

演示数据与脚本：

```bash
python tests/make_demo_data.py     # 生成 demo_data/*.csv
bash demo.sh                       # 命令行全流程演示
```

## 数据模型（SQLite，`/data/warehouse.db`）

| 表 | 作用 |
|---|---|
| `batches` | 批次元数据，状态机 `active → revoking → revoked`（revoked 永久，无恢复入口） |
| `raw_records` | 原始 CSV 记录，只增不删；撤销后仍保留可审计，但查询时被过滤 |
| `current_results` | 正式结果表，对外只读消费；导入时 upsert，撤销时整体重建 |
| `revoke_jobs` | 撤销任务与进度，支持重启接续；部分唯一索引保证同时只有一项 |

## 关键语义与实现方式

### 1. 导入
- CSV 四列：站点、采样日、检测项目、结果（兼容英文表头、UTF-8 BOM、GBK）。
- 批内前三列组合唯一，重复或格式错误**整批拒绝、事务回滚**。
- 成功后分配自增批次号；`current_results` 按 `(站点,采样日,检测项目)` upsert，
  且 `WHERE excluded.source_batch > current_results.source_batch`，迟到/乱序写入
  永远不会覆盖更新批次的值。

### 2. 撤销：影子重建 + 单事务原子切换
1. 请求撤销：批次置 `revoking`，登记任务并记录**快照批次号**（当时最大批次号）。
2. 后台线程在**独立的影子 SQLite 文件**中按键重算"最大未撤销批次"的值，
   分页推进并把进度写回正式库（仅秒级短事务），**重建不占用正式库写事务**。
3. 切换：单个 `BEGIN IMMEDIATE` 事务内 ATTACH 影子库 → 清空并重写 `current_results`
   → 合并快照之后导入的批次 → 批次置 `revoked`。WAL 读者在切换全程读到的都是
   完整的旧状态，提交后才看到完整的新状态，不存在中间态。
4. 合并规则：快照后的新批次逐键取最大值，且仅在批次号更大时覆盖——
   **重建期间的导入不丢，迟到计算不覆盖新值**；无任何未撤销记录的键直接删除。
5. 失败：任务置 `failed`、批次恢复 `active`、影子文件清理，撤销完全不生效，可重新发起。
6. 重启：启动时自动发现 `pending/running` 任务，影子表支持 keyset 断点续建。
7. 同时只允许一项：应用层检查 + 数据库部分唯一索引
   `ON revoke_jobs(1) WHERE status IN ('pending','running')` 双重保证。

### 3. 页面与接口
- `GET /` 单页入口：上传、撤销、任务进度、当前结果、下载 CSV。
- `POST /api/upload`（multipart 文件）、`POST /api/revoke`（JSON `batch_id`）
- `GET /api/batches`、`GET /api/jobs`、`GET /api/results`、`GET /api/results.csv`

## 测试

```bash
python -m unittest tests.test_app -v
```

11 个端到端用例覆盖：递增批次号与同键最新值、批内重复整批拒绝、撤销重算与孤儿键删除、
重建期间并发导入的合并语义、并发撤销互斥、注入故障的失败回滚、kill 进程后的重启接续、
CSV 下载。

## 调参（环境变量）

- `REBUILD_PAGE_SIZE`：影子重建每批键数（默认 10，演示调小可观察进度条）
- `REBUILD_SLEEP`：每页间隔秒数（默认 0.15）
