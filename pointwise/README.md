# Pointwise — 分片采集与安全交接演示

一套从零构建、仅依赖 Python 标准库的分布式采集系统。来源服务、分片调度器、
接收库、Web 控制台作为**独立进程**运行，采集器以独立子进程组被监管。系统演示
在范围拆分 / 合并 / 旧进程恢复 / 来源暂时拒绝请求的同时，如何做到**不缺记录、
不把重复当新增、过期任务无法再提交进度**。

## 一、四个核心不变量如何保证

| 风险 | 机制 | 落点 |
|---|---|---|
| 分片编号永久有效 | 分片 ID 用随机 `sh_xxx`，拆分/合并一律**退役旧片、创建新片**，编号不复用 | `scheduler.py` |
| 旧任务提交过期进度 | 每次注册 bump `epoch` 并签发新令牌；接收库**每批**先向调度器验令牌，旧 epoch 一律 `409 fenced`，该批零写入 | `receiver.py` `ingest()` |
| 旧进程恢复后继续写 | 采集器心跳每 5s 验令牌，收到 409 立即自退；即使漏掉心跳，提交批次时仍被接收库 fence | `collector.py` |
| 重复被当新增 | 接收库以 `(shard_id, record_id)` 为主键 `INSERT OR IGNORE`，重复投递只计 `duplicates`，永不计 `inserted` | `receiver.py` |
| 缺记录 | 完整性只认**来源权威清单对账**：记录 ID 集合一致 + 32 位校验和一致 + 显式缺失/多余 ID 列表；cursor 仅是提示 | `receiver.py` `coverage()`/`global_evidence()` |

> 关键设计：**checkpoint / resume_cursor 只是续传提示，不是完整性依据。**
> 拆分合并后旧记录仍挂在退役分片名下，因此 per-shard 计数天然不完整；
> 全局 `DISTINCT record_id` 对来源清单的对账才是权威证据。

## 二、两种交接策略（请求成本 vs 空窗风险）

- **overlap（重叠交接）**：先起新任务，注册即 fence 旧 epoch；旧任务在下个
  心跳（≤5s）收到 409 自退。无空窗，代价是有界的重复抓取，重复被去重不计新增。
- **pause（暂停交接）**：先冻结旧任务再移交，无重叠抓取、请求成本低，
  代价是存在短暂空窗。

## 三、组件与端口

| 组件 | 文件 | 端口 | 职责 |
|---|---|---|---|
| 来源服务 | `source.py` | 8091 | 本地生成资料、真实 HTTP 分页、429 拒绝注入、权威 manifest |
| 分片调度器 | `scheduler.py` | 8092 | 拓扑（拆分/合并）、epoch fencing、租约、续点、令牌校验 |
| 接收库 | `receiver.py` | 8093 | 每批验令牌、按 ID 去重入库、清单对账证据 |
| Web 控制台 | `web.py` | 8080 | 网页触发全部操作 + 采集器进程监管 |
| 采集器 | `collector.py` | 控制口动态分配 | 真实分页抓取、心跳、断点续传、被 fence 自退 |

## 四、快速开始

```bash
# 一键端到端演示（自动清理遗留进程 → 从零建库 → 8 个场景 → 全程断言）
python3 -I demo.py

# 演示结束后保留服务，用浏览器打开控制台
python3 -I demo.py --keep
# 然后访问 http://127.0.0.1:8080

# 或手动逐个启动（每个都是独立进程）
python3 -I source.py     --seed 400
python3 -I scheduler.py
python3 -I receiver.py
python3 -I web.py
```

演示产出 `data/demo-report.json`，记录每步对账证据、fence 结果与请求成本。

## 五、演示的 8 个场景

1. 建两个初始分片，采集器真实分页抓完，清单对账一致
2. 迁移期间持续注入新资料，空闲轮询自动续抓
3. 来源进入 429 拒绝窗口，采集器按 Retry-After 退避，不丢进度
4. **拆分**一个范围：父片 epoch 退役，旧采集器被 fence 自退，两个新任务接管
5. **重叠交接**：续点回退制造确定性重复，观察到 `duplicates>0` 而新增不增长
6. **合并**两个相邻分片，合并片重新接管后全量对账
7. **旧进程恢复演练**：捕获一个有效令牌 → 交接作废旧 epoch → 重放该令牌，
   接收库与调度器双双 `409`，拒收且零写入
8. 最终对照来源权威清单与接收库（计数、校验和、缺失/多余 ID、真实请求成本）

## 六、网页控制台能做什么

- 注入新资料、开关来源 429 拒绝窗口
- 触发分片拆分 / 合并 / 启动采集器 / 两种模式交接
- 查看每个范围由谁负责（租约 owner、epoch、续点）
- 查看采集器实时状态、抓取/重复/拒绝/fence 计数、暂停/恢复/停止
- 查看完整性证据（清单对账）、来源真实请求流水、调度器审计事件流

## 七、目录与数据

- `data/*.db` — 各服务独立 SQLite 库；`data/run/` — 采集器续点与端口文件
- `logs/` — 各进程独立日志
- 仅用标准库（`http.server`、`sqlite3`、`urllib`、`subprocess`、`threading`）

## 八、常用 API

```
GET  /manifest?lo=&hi=          来源权威清单（ID 列表 + 校验和）
GET  /records?lo=&hi=&cursor=   真实分页（每页一次 HTTP 请求）
POST /shards/{id}/register      注册并签发 fencing 令牌（bump epoch）
POST /heartbeat /checkpoint     心跳 / 续点（旧令牌返回 409）
POST /validate-token            接收库每批调用的验令牌
POST /shards/{id}/split  /merge 拓扑变更（退役旧片）
POST /ingest                    批次入库（先验令牌，再按 ID 去重）
GET  /evidence  /coverage       全局 / 每片完整性证据
```
