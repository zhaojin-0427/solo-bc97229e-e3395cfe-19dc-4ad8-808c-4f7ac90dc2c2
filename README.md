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
6. 重启：启动时工作线程自动发现 `pending/running` 任务（遗留 `running` 先复位为
   `pending` 再认领），影子表支持 keyset 断点续建。
7. 同时只允许一项：受理写事务 + 数据库部分唯一索引
   `ON revoke_jobs(1) WHERE status IN ('pending','running')` 双重保证。

### 撤销受理与调度的并发正确性（两处竞态修复）

系统只有**一个常驻工作线程**，受理接口与工作线程的职责严格分离：

- **受理（`POST /api/revoke`）只负责落库为 `pending`**：整段受理在一个
  `BEGIN IMMEDIATE` 写事务内完成。并发的撤销请求在 SQLite 写锁上排队，
  后到者读到的一定是先到者提交后的**最新**批次状态；批次状态翻转使用带条件的
  `UPDATE ... WHERE status = 'active'`。因此迟到请求即使在更早时刻读到过
  `active`，真正受理时也会按最新状态判定——已 `revoked` 的批次不会被重新拉回
  `revoking`，不会二次进入撤销流程（接口返回 409）。
- **工作线程循环认领（claim）**：在独立写事务内把一笔 `pending` 翻转为 `running`
  后执行，排空所有 `pending` 任务才退出；线程退出清空自身引用与新任务登记共用
  同一把进程内锁。于是即使上一笔任务已显示 `done`、线程仍在做影子文件清理，
  随后受理的新任务也会被现存（或新拉起的）线程立即领取，**无需重启或再次点击**
  即可自动推进，且任一时刻仍只有一项在执行。
- 影子文件清理发生在任务置 `done` 之后，清理耗时不占用受理/查询路径；
  收尾期间所有读接口与导入照常响应。

### 3. 页面与接口
- `GET /` 单页入口：上传、撤销、任务进度、当前结果、下载 CSV。
- `POST /api/upload`（multipart 文件）、`POST /api/revoke`（JSON `batch_id`）
- `GET /api/batches`、`GET /api/jobs`、`GET /api/results`、`GET /api/results.csv`

## 测试

```bash
python -m unittest tests.test_app -v
```

13 个端到端用例覆盖：递增批次号与同键最新值、批内重复整批拒绝、撤销重算与孤儿键删除、
重建期间并发导入的合并语义、并发撤销互斥、注入故障的失败回滚、kill 进程后的重启接续、
CSV 下载，以及两处撤销竞态的确定性回归（用同步闸门精确控制交错）：

- `test_job_during_cleanup_progresses_without_restart_or_reclick`：前任务已 `done`
  而线程仍在影子清理时受理新任务，断言其被现存线程自动领取并推进到 `done`，
  且收尾窗口内 `/api/jobs`、`/api/batches`、`/api/results` 均保持响应、导入仍可提交。
- `test_late_request_judged_by_latest_status_revoked_not_readmitted`：迟到请求在
  批次仍 `active` 时停于受理写锁前，待先到者把批次撤销完成后放行，断言其按最新状态
  返回 409，全库只有一笔撤销任务、批次保持 `revoked` 不被重新受理。

## 调参（环境变量）

- `REBUILD_PAGE_SIZE`：影子重建每批键数（默认 10，演示调小可观察进度条）
- `REBUILD_SLEEP`：每页间隔秒数（默认 0.15）
