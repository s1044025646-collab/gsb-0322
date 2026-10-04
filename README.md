# Raft 跟随者日志前缀核对与冲突截断（教学后端）

使用 **TypeScript + Node.js + SQLite** 从零实现的 Raft 跟随者 `AppendEntries`
处理教学项目，只提供 **HTTP API 与 CLI**，无前端、无 Docker、无外部服务。

## 它做什么 / 不做什么

本项目**只验证跟随者的日志复制语义中的一条路径**：

1. 任期检查：请求任期较旧直接拒绝（`TERM_STALE`）；请求任期较新时先持久化
   新任期，**即使随后前缀检查失败也保留新任期**。
2. 前缀核对：`prevLogIndex` 处条目必须存在（`PREV_LOG_MISSING`）且任期一致
   （`PREV_LOG_TERM_MISMATCH`）。
3. 逐条比较：相同索引、相同任期的条目复用（重发幂等，不重复追加）；从第一条
   **任期冲突**起删除本地后缀，再追加剩余新条目。
4. 较短请求不截断尾部：没有冲突时，不能仅因请求比本地日志短就删除本地尾部。
5. 协议假设保护：相同索引、相同任期但**命令不同**返回
   `SAME_TERM_COMMAND_CONFLICT`，明确报告输入违反 Raft 协议假设，绝不静默
   当作正常重复。
6. 已提交条目保护：首个冲突索引不大于 `commitIndex` 时返回
   `COMMITTED_ENTRY_CONFLICT`，保留日志与提交位置。
7. 提交推进：成功请求只确认到 `prevLogIndex + entries.length`；提交位置最多
   推进到 `min(leaderCommit, 确认前缀)`，且**只进不退**。空条目心跳同样受限，
   不会把本次未确认的本地旧尾部一并提交。

**明确不做**（因此不能据此宣称实现了 Raft 共识）：

- 不做领导人选举、投票、真实节点网络通信；
- 不做心跳定时器、快照安装、集群成员变更；
- 不执行命令字符串（没有 `eval`、没有真实 shell、不做状态机应用）；
- 不保证全局一致性、可用性或线性一致读——只是单跟随者视角的复制规则模拟器。

## 数据模型

每个 follower 实验保存：`currentTerm`、从索引 1 开始的日志条目（`term` +
字符串 `command`）、`commitIndex`。**索引 0 是任期 0 的固定哨兵**。
每条请求的状态变化与处理记录在**同一个 SQLite 事务**提交；批量请求以明确的
逐请求边界（每条各自一个事务）执行，并报告首个失败的零基位置。拒绝请求也会
留下处理记录（可能伴随合法的新任期更新），且不会产生部分后缀替换。

## 环境要求

- Windows 本地运行，仅需 **Node.js 22+（内置实验性 `node:sqlite`；本机使用
  Node v25 验证）**。无需 Docker、WSL、付费服务或在线 API。
- SQLite 数据库默认位于项目内 `./data/raft.db`（可用 `--data` 覆盖）。

## 构建、测试、启动、演示

```powershell
npm install        # 仅安装 typescript 与 @types/node
npm run build      # 编译到 dist/
npm test           # 编译并运行 19 个 node:test 用例
npm run demo       # 内置教学演示（10 个场景，逐请求前后表格）
npm start          # 启动 HTTP API（默认自动选择空闲端口）
```

指定端口与数据目录：

```powershell
node dist/src/cli.js serve --port 8080 --data ./data
node dist/src/cli.js demo --data ./data
```

## CLI

```powershell
node dist/src/cli.js create NAME [--entries entries.json]
node dist/src/cli.js list
node dist/src/cli.js show ID                  # 日志表格 + 提交前缀
node dist/src/cli.js records ID               # 逐条处理记录
node dist/src/cli.js export ID                # 导出整个实验 JSON
node dist/src/cli.js append ID --rpc rpc.json # 应用单个 AppendEntries
```

`entries.json` 示例（导入合法初始日志，索引隐含从 1 连续，任期非递减）：

```json
[{"term": 1, "command": "SET x=1"}, {"term": 2, "command": "SET y=2"}]
```

`rpc.json` 示例：

```json
{
  "term": 2,
  "prevLogIndex": 1,
  "prevLogTerm": 1,
  "entries": [{ "term": 2, "command": "SET y=20" }],
  "leaderCommit": 1
}
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/followers` | 创建实验，body：`{name, entries?}` |
| `GET` | `/followers` | 列出实验 |
| `GET` | `/followers/:id` | 查询任期、日志、提交位置 |
| `POST` | `/followers/:id/append` | 应用单个 AppendEntries RPC 对象 |
| `POST` | `/followers/:id/batch` | 有序应用一组 RPC（JSON 数组） |
| `GET` | `/followers/:id/records` | 读取处理记录 |
| `GET` | `/followers/:id/export` | 导出实验（状态 + 全部记录） |

快速试用：

```powershell
$body = '{"term":1,"prevLogIndex":0,"prevLogTerm":0,"entries":[{"term":1,"command":"SET a=1"}],"leaderCommit":1}'
Invoke-RestMethod -Method Post http://127.0.0.1:8080/followers/1/append `
  -ContentType 'application/json' -Body $body
```

处理结果字段包含：`outcome`（`accepted`/`rejected`/`invalid_input`/
`committed_conflict`）、`reason`、`matchedIndex`、`firstConflictIndex`、
`deletedRange`、`appendedRange`、`acknowledgedIndex`、`termBefore/After`、
`commitBefore/After`、`newlyCommittedRange`，可清楚区分**接收但未提交**与
**已提交**两种条目状态。

## 错误码

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | 400 | 索引/任期/提交位置不是非负安全整数、任期序列非法、JSON 非法等 |
| `TERM_STALE` | 200（业务拒绝） | 请求任期旧于当前任期 |
| `PREV_LOG_MISSING` | 200（业务拒绝） | 前驱索引不存在 |
| `PREV_LOG_TERM_MISMATCH` | 200（业务拒绝） | 前驱任期不一致 |
| `SAME_TERM_COMMAND_CONFLICT` | 200（业务错误） | 同索引同任期但命令不同，违反协议假设 |
| `COMMITTED_ENTRY_CONFLICT` | 200（业务错误） | 试图截断已提交条目 |
| `NAME_CONFLICT` | 409 | 实验名重复 |
| `NOT_FOUND` | 404 | 实验不存在 / 路由未知 |

## 测试与正确性核对

- `test/core.test.ts` 内含一个**小型数组参考实现**（以数组下标为日志索引），
  多步序列的状态变化与主实现逐一 `deepEqual` 比对。
- 覆盖：旧任期、新任期但前缀失败、空日志、空条目心跳、提交位置回退请求、
  已提交位置冲突、重复发送、冲突后缀替换、较短匹配保留尾部、命令不同的协议
  假设违反，并断言提交前缀从不非法改变、重发不重复追加、报告范围对应真实条目。
- `test/store.test.ts` 通过写入失败注入（提交前抛错）验证**事务整体回滚**，
  以及重启后任期、日志、提交位置与请求次序保持。

## 项目结构

```
src/core.ts    纯逻辑：校验 + 前缀核对 + 冲突截断 + 提交推进（无 IO）
src/store.ts   SQLite 持久化（事务、记录、导入/导出、失败注入钩子）
src/service.ts 单条与批量应用（逐请求事务边界、首个失败位置）
src/server.ts  纯 node:http 的 JSON API
src/cli.ts     命令行入口
src/demo.ts    内置 10 场景教学演示
src/display.ts 终端表格渲染
test/          node:test 用例
```
