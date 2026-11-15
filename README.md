# 断面水质逐时氨氮采集平台

县站分页接口的模拟服务 + FastAPI 采集网页（SQLite 落库）+ 分析组本地接收端（只收完整日档），并附带"采集中途县站更正记录"和"发布后持续投送"的完整样例。

## 要解决的问题

断面水质数据员要按 **断面 + UTC 日期** 从县站分页接口拉取某日逐时氨氮（NH₃-N）观测，交给污染复核组。县站可能在采集途中更正记录。平台保证：

1. **逐页核对修订号**：首页取得的 `revision` 被钉住，后续任何一页修订号不一致即中止。
2. **游标随修订失效**：县站更正后修订号递增，旧游标再用返回 `409 stale_cursor`，杜绝把两个修订版本的页拼成"弗兰肯数据"。
3. **当日且时刻唯一**：每行必须是该 UTC 日期的整点时刻，跨页出现重复时刻即中止。
4. **整日发布（all-or-nothing）**：收齐 `total_rows` 且收尾再次确认源端修订号未变，才在一个 SQLite 事务里替换整天；失败时旧版（若有）原样保留，不完整数据永远查不到。
5. **超时重试当前页**：网络错误、超时、5xx 只重试当前页（游标不前进），最多 3 次；`409` 属永久性错误不重试。
6. **双 Web 进程并发安全**：同一断面+日期同时只允许一个活动采集任务（SQLite 部分唯一索引，跨进程生效）；两个进程共享同一 SQLite 卷。
7. **迟到采集不能盖掉较新修订**：发布事务里比对已发布修订号，旧修订号写入直接拒绝（`StaleRevisionError`）。
8. **发布即登记待送（事务性 outbox）**：整日发布的同一个 SQLite 事务里写入一条待送修订；进程在提交后任意时刻崩溃都不会丢掉"该送而未送"的日档。
9. **两个 Web 实例都可发送/重试，中断后可补送**：发送前用数据库租约（短租约，跨进程）认领，只有一个实例真正在发；网络中断、进程重启后由后台发送器按退避补送，也可在网页手动重试。
10. **发送失败不撤销本地日档，也不把旧修订显示为当前已同步**：投递只更新 `deliveries` 表，`published` 原样保留；同步状态只认"已发布修订号 == 投递修订号且 sent"，被取代的旧修订只留历史。
11. **接收端只开放完整日档，同版幂等、迟修订不覆盖**：只接受 24 个唯一整点、行数与摘要自洽的日档；同版同内容重送返回 `duplicate` 不重复写入；旧修订 409 `stale_revision`；同版不同内容 409 `revision_conflict`；坏档 400 `incomplete_day`/`digest_mismatch`。

## 架构

```
docker compose
├── station   县站模拟接口（可切换/递增修订号，分页 + 游标）   :8001
├── web1      采集 Web 进程 A（FastAPI + SQLite WAL）          :8011
├── web2      采集 Web 进程 B（同一份代码，同一 SQLite 卷）    :8012
└── receiver  分析组本地接收端（只收完整日档，SQLite）         :8021

卷 quality-data:/data   -> 两个 web 进程共享 quality.db
卷 receiver-data:/data  -> 接收端 receiver.db
web1/web2 的后台发送器 -> http://receiver:8000/api/deliveries
```

### 县站接口（`station/`，本地模拟，带管理端用于演示更正）

| 方法/路径 | 说明 |
|---|---|
| `GET /metadata?section=&date=` | 当前修订号、总行数、页大小 |
| `GET /observations?section=&date=&cursor=` | 分页返回 `revision`、`total_rows`、`page`、`next_cursor` 和行（含 `source_page`） |
| `POST /admin/correct` | 更正某时刻数值：修订号 +1，旧游标立即失效 |
| `POST /admin/reset` | 该日数据重置回修订号 1 |
| `POST /admin/fail-next` | 下一次分页读取返回 500（一次性，演示重试） |
| `POST /admin/latency` | 调整每页模拟延迟（演示并发用） |

游标是携带 `(section, date, revision, page)` 的不透明 token；服务端发现 token 里的修订号与当前不符即返回 409。

### Web 接口（`web/`）

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 采集网页：选断面、UTC 日期，触发采集，查看任务、已发布版本与同步状态 |
| `GET /api/sections` | 县站支持的断面 |
| `POST /api/collect` | 启动后台采集（同断面+日期已有活动任务时返回 409） |
| `GET /api/runs/{id}` | 单个任务状态（running/success/failed/interrupted、修订号、行数、页尝试次数、错误） |
| `GET /api/runs?section=&date=` | 该日全部采集任务（含两个 worker 的记录） |
| `GET /api/published?section=&date=` | 已发布整日版本：时刻、氨氮值、来源页、修订号；无完整版本时 404 |
| `GET /api/sync?section=&date=` | 当前已发布修订号的同步状态、投递历史、接收端对应修订号与可达性 |
| `POST /api/sync/retry` | 立即重试发送当前已发布修订（失败返回 502，接收端忙返回 409） |
| `GET /api/deliveries?limit=` | 全部投递队列（两个 worker 共用） |

### 本地接收端接口（`receiver/`）

| 方法/路径 | 说明 |
|---|---|
| `POST /api/deliveries` | 接收一个完整日档；`created`/`replaced`/`duplicate`，拒绝时 400/409 带 `error` |
| `GET /api/days/{section}/{date}` | 已归档的完整日档；不存在/不完整 404 |
| `GET /api/days?limit=` | 已归档日档列表 |
| `GET /api/deliveries?limit=` | 投递审计（含被拒绝的记录） |
| `GET /` | 接收端网页：已归档日档与最近投递审计 |

接收端校验：`complete=true`、`row_count` 与实际行数一致、恰好 24 个唯一整点、`digest` 与内容一致（收发两端用同一套 SHA-256 规范化摘要）。

## 快速启动（Docker Compose）

```bash
./run-demo.sh                 # 构建、起栈、等待健康、自动跑全部演示检查
# 或手动：
docker compose up -d --build
# 网页 http://localhost:8011 （或 8012），县站文档 http://localhost:8001/docs，
# 接收端 http://localhost:8021
```

手动的中途更正样例（curl 逐步可观察）：

```bash
./demo/mid-correction.sh
```

### 不用 Docker 的本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r web/requirements.txt -r station/requirements.txt -r receiver/requirements.txt
PAGE_SIZE=8 .venv/bin/python -m uvicorn station.app.main:app --port 8001 &
RECEIVER_DB_PATH=$PWD/data/receiver.db .venv/bin/python -m uvicorn receiver.app.main:app --port 8021 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 RECEIVER_URL=http://127.0.0.1:8021 WORKER_NAME=web1 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8011 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 RECEIVER_URL=http://127.0.0.1:8021 WORKER_NAME=web2 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8012 &
python3 -I demo/demo.py
python3 -I tests/test_delivery.py   # 36 项聚焦测试（不依赖 HTTP）
```

## 演示场景（`demo/demo.py`，53 项断言）

1. **中途更正**：采集开始后县站更正 08:00 数值（修订号 1→2）→ 采集因 409 失败、无半成品公布、旧游标确认为 409；重新采集后整日按修订号 2 原子发布，24 行时刻唯一、来源页齐全。
2. **读取失败重试**：注入一次性 500 → 仅当前页重试（`page_attempts` 大于页数），最终成功发布。
3. **双进程并发 + 陈旧修订防护**：两进程同时采集同日 → 一个 202、一个 409；再构造"源端退回旧修订号的迟到采集"，发布事务拒绝旧修订号，已发布的新版本原样保留；另有直接对 DB 事务的防护单测。
4. **发布后投送接收端**：发布即自动同步；接收端只开放 24 行完整日档；两个 Web 实例都可重试；同版重送为幂等 `duplicate`；县站更正后由另一实例采集并自动补送新修订，接收端整日替换；旧修订保留历史但不再显示为当前已同步。
5. **接收端约束**：接受完整日档、拒绝迟到旧修订、同版同内容幂等、同版不同内容冲突、缺整点/摘要不符拒绝，且被拒投递不影响既有日档。

另外人工验证过：接收端停机时发布 → 本地日档仍可查、同步状态为 `failed`；恢复并重启两个 Web 进程后，后台发送器按退避自动补送成功（`attempts` 递增、`sent_by` 记录实例名）；接收端忙时手动重试返回 409；发送失败时手动重试返回 502 且本地日档不变。worker 进程被 `kill -9` 后重启，遗留的 running 任务自动标记为 `interrupted`，可重新采集，且不会误伤另一实例仍在跑的任务。

## 关键设计

- **发布事务**（`web/app/db.py: publish_version`）：`BEGIN IMMEDIATE` 下检查已发布修订号 → 删除旧整天数据 → 插入新整天 → 任务置成功 → **同事务登记 outbox 待送行** → 单事务提交；任何步骤异常都回滚。
- **活动任务互斥**：`runs` 表上 `WHERE status='running'` 的部分唯一索引，跨进程互斥，不依赖单机内存锁。
- **采集状态机**（`web/app/collector.py`）：metadata 钉修订号 → 逐页（重试仅当前页、校验修订号/总行数/页序/时刻唯一）→ 收齐行数 → 再查一次 metadata 复核修订号 → 整日发布。
- **投送状态机**（`web/app/sync.py` + `web/app/db.py` 的 deliveries 表）：发布即登记 → 后台发送器轮询 → 租约认领（`status='sending'` + `claimed_at/claimed_by`，60s 租约）→ 成功记 `sent`、失败记 `failed`（指数退避）→ 只有最新本地修订可发送，旧修订 `superseded`。发送只改投递行，永不动 `published`。
- **收发一致性**：两边共用同一套规范化 SHA-256 摘要（`delivery_digest`/`content_digest`），接收端据此区分"同版幂等重送"与"同版内容冲突"。
- **崩溃恢复**：worker 启动时只把自己遗留的 `running` 任务标记为 `interrupted`（避免误伤另一实例），并释放自己的发送租约；租约过期后另一实例也能接手。
- **SQLite 并发**：WAL + `busy_timeout=30s`，两个 uvicorn 进程共享同一数据库文件；接收端使用独立卷。

## 目录

```
station/            县站模拟服务（FastAPI）
  app/main.py       分页/游标/修订号/管理端
  app/data.py       确定性 24 小时合成数据
web/                采集网页（FastAPI + SQLite）
  app/main.py       HTTP API 与网页（含同步状态与重试）
  app/collector.py  逐页核对、重试、整日发布状态机
  app/sync.py       事务性 outbox 的发送器、租约、退避、手动重试
  app/receiver_client.py  接收端 HTTP 客户端（瞬时/永久错误分类）
  app/station_client.py
  app/db.py         SQLite 模式、互斥、发布事务与投递队列
receiver/           分析组本地接收端（FastAPI + SQLite，只开放完整日档）
  app/store.py      校验、幂等、迟修订防护、整日原子替换
  app/main.py       HTTP API 与接收端网页
demo/demo.py        五场景自动化演示（python -I，仅标准库 + 本仓库模块）
demo/mid-correction.sh  curl 手动中途更正样例
tests/test_delivery.py  36 项聚焦测试（outbox、租约、并发、接收端约束）
docker-compose.yml  station + web1 + web2 + receiver + 各自卷
```
