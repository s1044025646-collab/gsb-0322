# Raft 跟随者日志前缀核对与冲突截断（教学后端）

> **重要说明**：本项目**只模拟 Raft 的跟随者 AppendEntries 日志复制路径**，用于观察前缀核对、首次冲突截断与提交位置推进的语义。**不包含**领导者选举、真实节点通信、心跳定时器、快照安装或任何完整分布式共识机制。**不能据此宣称实现了 Raft 共识。**

纯本地运行：TypeScript + Node.js（>= 22.5，推荐 25）+ SQLite（Node 内置 `node:sqlite`，无原生编译）。不依赖 Docker、WSL、付费服务或外部在线 API。命令字符串仅作为数据存储，**绝不执行**（无 `eval`、无 shell 调用）。

## 环境要求

- Windows 本地，Node.js >= 22.5（开发使用 Node 25 验证）
- `npm install`（仅安装 TypeScript 与类型定义两个开发依赖）

## 快速开始

```powershell
npm install
npm run build     # 编译 TypeScript 到 dist/
npm test          # 构建并运行全部测试（node:test）
npm start         # 启动 HTTP API（默认端口 0 = 自动选择空闲端口）
npm run demo      # 运行内置演示：逐请求打印前后日志表格
```

数据目录为项目内 `data/`（默认数据库文件 `data/raft.db`），可用 `--db 路径` 或环境变量 `RAFT_DB` 覆盖。端口可用 `--port N` 或环境变量 `PORT` 配置，默认 `0`（自动选择空闲端口）。

## 模型与规则

- 每个实验（跟随者）保存：当前任期 `currentTerm`、从索引 1 开始的日志条目、提交位置 `commitIndex`。
- 索引 0 是任期 0 的固定哨兵条目，自动创建。
- 条目 = `{ term, command }`，`command` 为普通字符串，不执行。

### AppendEntries 处理顺序

1. **参数校验**：所有任期 / 索引 / 提交位置必须是非负安全整数；条目任期单调不减、不小于 `prevLogTerm`、不超过请求任期。
2. **旧任期**：`term < currentTerm` → 拒绝（`STALE_TERM`），不修改任何状态。
3. **新任期**：`term > currentTerm` → 先持久化新任期，**即使随后前缀核对失败也保留**。
4. **前驱核对**：`prevLogIndex` 必须存在且本地任期等于 `prevLogTerm`，否则拒绝（`PREV_LOG_MISSING` / `PREV_LOG_TERM_MISMATCH`）。
5. **逐条比较**：相同索引相同任期 → 复用；若**同索引同任期但命令不同**，报告 `PROTOCOL_VIOLATION`（违反协议假设，不静默接受）。
6. **首次冲突截断**：从第一条任期冲突的索引删除本地后缀，再追加剩余新条目。冲突索引 `<= commitIndex` → 拒绝（`COMMITTED_CONFLICT`），日志与提交位置保持不变。
7. **无冲突**：只追加缺失部分；**不会**仅因请求较短就删除本地更长尾部（空条目心跳同样遵守）。
8. **提交推进**：`matchIndex = prevLogIndex + entries.length`；`commitIndex = max(commitIndex, min(leaderCommit, matchIndex))`，永不回退，不会把未被本次请求确认的本地旧尾部一并提交。

每条请求的状态变化与处理记录在**同一 SQLite 事务**中提交；拒绝的请求也会记录处理结果与合法的新任期更新，绝不产生部分后缀替换。批量请求按明确的逐请求边界顺序执行，遇到首个失败即停止并报告其序号。重启进程后任期、日志、提交位置与请求次序全部保持。

## HTTP API

`npm start` 后（控制台会打印实际端口）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/experiments` | 列出实验 |
| POST | `/experiments` | 创建实验：`{id?, currentTerm?, initialLog?: [{term,command}]}` |
| GET | `/experiments/:id` | 查询日志与已提交前缀 |
| POST | `/experiments/:id/append-entries` | 应用单条请求 |
| POST | `/experiments/:id/batch` | 有序应用一组请求：`{requests: [...]}` |
| GET | `/experiments/:id/records` | 读取处理记录 |
| GET | `/experiments/:id/export` | 导出实验（状态 + 记录） |

请求体示例：

```json
{
  "term": 2,
  "prevLogIndex": 3,
  "prevLogTerm": 2,
  "entries": [{ "term": 2, "command": "set w=4" }],
  "leaderCommit": 3
}
```

响应报告包含：`accepted`、`reason`（错误码）、`matchIndex`、`firstConflictIndex`、`deletedRange` / `appendedRange`、`oldTerm` / `newTerm`、`oldCommitIndex` / `newCommitIndex`、`newlyCommitted`（区分“接收条目”与“已提交条目”）。

### 错误码

- 参数校验（HTTP 400）：`INVALID_TERM` `INVALID_INDEX` `INVALID_COMMIT` `INVALID_ENTRIES` `INVALID_COMMAND` `ENTRY_TERM_ORDER` `ENTRY_TERM_EXCEEDS_REQUEST`
- 协议层拒绝（HTTP 200 且 `accepted=false`）：`STALE_TERM` `PREV_LOG_MISSING` `PREV_LOG_TERM_MISMATCH` `PROTOCOL_VIOLATION` `COMMITTED_CONFLICT`
- 其他：`NOT_FOUND`（404）`BAD_REQUEST`（400）`INTERNAL`（500）

## CLI

```powershell
node dist/src/cli.js serve --port 8080        # 启动 API
node dist/src/cli.js create exp1 --term 1 --log init.json
node dist/src/cli.js apply exp1 req.json      # 单条请求，打印前后表格
node dist/src/cli.js batch exp1 reqs.json     # 有序批量，报告首个失败
node dist/src/cli.js show exp1                # 日志与已提交前缀
node dist/src/cli.js records exp1             # 处理记录
node dist/src/cli.js export exp1              # 导出
node dist/src/cli.js list
node dist/src/cli.js demo                     # 内置演示场景
```

`apply` / `demo` 会逐请求打印处理前后的日志表格（含每条的 committed 标记）。演示覆盖：正常追加、重复发送（幂等）、前驱不匹配、冲突后缀替换、较短匹配保留尾部、旧任期、新任期但前缀失败、提交位置回退请求、已提交位置冲突。

## 测试

`npm test` 运行 16 个测试（`test/follower.test.ts`），包括：

- 全部核心场景：重复发送、前驱缺失/任期不匹配、冲突替换、较短请求保留尾部、空日志、空条目心跳、提交回退、已提交冲突、同索引同任期不同命令；
- **随机核对**：随机请求序列下与一个小型数组参考实现逐条比对状态，并断言已提交前缀从不改变、成功重发不重复追加；
- **故障注入**：在追加写入时注入异常，验证事务整体回滚（无部分后缀替换、任期与记录一并回滚）；
- **持久化**：关闭并重新打开数据库后状态与请求次序保持。

## 目录结构

```
src/types.ts     类型与错误码
src/store.ts     SQLite 持久化（实验 / 条目 / 请求记录，事务封装）
src/follower.ts  核心：参数校验 + AppendEntries 处理 + 报告生成
src/server.ts    HTTP API（node:http，无外部依赖）
src/cli.ts       命令行与演示
test/            node:test 测试（含数组参考实现核对）
data/            默认数据目录
```

## 限制

- 单进程、单文件 SQLite；不模拟网络分区、多节点或选举。
- 不执行命令字符串，不维护状态机，仅推进 `commitIndex` 并报告新提交条目。
- `node:sqlite` 在 Node 25 仍标记为实验特性（会打印一条警告，不影响功能）。
