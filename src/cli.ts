import * as fs from "node:fs";
import * as path from "node:path";
import { Store } from "./store.js";
import { FollowerService, validateInitialLog } from "./follower.js";
import { startServer } from "./server.js";
import { RaftError } from "./types.js";
import type { RequestReport } from "./types.js";

const DEFAULT_DB = path.join(process.cwd(), "data", "raft.db");

function parseArgs(argv: string[]): { cmd: string; positional: string[]; opts: Record<string, string> } {
  const [cmd = "help", ...rest] = argv;
  const positional: string[] = [];
  const opts: Record<string, string> = {};
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = rest[i + 1];
      if (next !== undefined && !next.startsWith("--")) {
        opts[key] = next;
        i++;
      } else {
        opts[key] = "true";
      }
    } else {
      positional.push(a);
    }
  }
  return { cmd, positional, opts };
}

function readJson(source: string): unknown {
  if (fs.existsSync(source)) return JSON.parse(fs.readFileSync(source, "utf8"));
  return JSON.parse(source);
}

function fmtLog(log: Array<{ index: number; term: number; command: string }>, commitIndex: number): string {
  const header = " idx | term | committed | command";
  const lines = log.map(
    (e) =>
      ` ${String(e.index).padStart(3)} | ${String(e.term).padStart(4)} | ${e.index <= commitIndex ? "    yes   " : "    no    "} | ${e.command}`
  );
  return [header, ...lines].join("\n");
}

function printReport(service: FollowerService, id: string, before: string, report: RequestReport): void {
  console.log(`\n--- 请求 #${report.seq} ---`);
  console.log("处理前:");
  console.log(before);
  const after = service.getState(id);
  console.log("处理后:");
  console.log(fmtLog(after.log, after.commitIndex));
  console.log(`结果: ${report.accepted ? "接受" : "拒绝/错误"} [${report.reason}] ${report.message}`);
  console.log(
    `任期: ${report.oldTerm} -> ${report.newTerm} | 提交位置: ${report.oldCommitIndex} -> ${report.newCommitIndex} | matchIndex: ${report.matchIndex}`
  );
  console.log(
    `首个冲突: ${report.firstConflictIndex ?? "无"} | 删除范围: ${report.deletedRange ? report.deletedRange.join("-") : "无"} | 追加范围: ${report.appendedRange ? report.appendedRange.join("-") : "无"}`
  );
  if (report.newlyCommitted.length > 0) {
    console.log(`新提交条目: ${report.newlyCommitted.map((e) => `#${e.index}(${e.command})`).join(", ")}`);
  }
}

function runDemo(service: FollowerService): void {
  const id = `demo-${Date.now()}`;
  service.createExperiment(id, 1, [
    { term: 1, command: "set x=1" },
    { term: 1, command: "set y=2" },
    { term: 2, command: "set z=3" },
  ]);
  console.log(`演示实验 id: ${id}`);
  const step = (label: string, req: unknown) => {
    console.log(`\n===== ${label} =====`);
    console.log("请求:", JSON.stringify(req));
    const before = fmtLog(service.getState(id).log, service.getState(id).commitIndex);
    const report = service.apply(id, req);
    printReport(service, id, before, report);
  };
  step("1. 正常追加并推进提交（leaderCommit=2）", {
    term: 2, prevLogIndex: 3, prevLogTerm: 2,
    entries: [{ term: 2, command: "set w=4" }], leaderCommit: 2,
  });
  step("2. 重复发送同一请求（幂等，不重复追加）", {
    term: 2, prevLogIndex: 3, prevLogTerm: 2,
    entries: [{ term: 2, command: "set w=4" }], leaderCommit: 2,
  });
  step("3. 前驱不匹配（prevLogTerm 错误）", {
    term: 2, prevLogIndex: 3, prevLogTerm: 9, entries: [], leaderCommit: 3,
  });
  step("4. 冲突后缀替换", {
    term: 3, prevLogIndex: 2, prevLogTerm: 1,
    entries: [{ term: 3, command: "set z=30" }, { term: 3, command: "set v=5" }],
    leaderCommit: 2,
  });
  step("5. 较短匹配请求保留本地更长尾部（空条目心跳）", {
    term: 3, prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 2,
  });
  step("6. 旧任期拒绝", {
    term: 1, prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 0,
  });
  step("7. 新任期但前缀失败（任期仍被持久化）", {
    term: 5, prevLogIndex: 99, prevLogTerm: 1, entries: [], leaderCommit: 0,
  });
  step("8. 提交位置回退请求（commitIndex 不回退）", {
    term: 5, prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 0,
  });
  step("9. 已提交位置冲突（拒绝截断）", {
    term: 5, prevLogIndex: 1, prevLogTerm: 1,
    entries: [{ term: 5, command: "evil" }], leaderCommit: 2,
  });
}

const HELP = [
  "Raft 跟随者日志复制教学后端 CLI",
  "",
  "用法: node dist/src/cli.js <命令> [参数] [--db 路径]",
  "",
  "命令:",
  "  serve [--port N]                        启动 HTTP API（默认端口 0 = 自动选空闲端口）",
  "  create <id> [--term N] [--log 文件或JSON]  创建跟随者实验（可导入初始日志）",
  "  apply <id> <请求JSON或文件>              应用单条 AppendEntries 请求",
  "  batch <id> <JSON数组或文件>              按序应用一组请求，首个失败处停止",
  "  show <id>                               查看日志与已提交前缀",
  "  records <id>                            查看处理记录",
  "  export <id>                             导出实验（状态 + 记录）",
  "  list                                    列出全部实验",
  "  demo                                    运行内置演示场景",
  "",
].join("\n");

async function main(): Promise<void> {
  const { cmd, positional, opts } = parseArgs(process.argv.slice(2));
  const dbPath = opts.db ?? process.env.RAFT_DB ?? DEFAULT_DB;
  const store = new Store(dbPath);
  const service = new FollowerService(store);
  try {
    switch (cmd) {
      case "serve": {
        const port = opts.port ? Number(opts.port) : Number(process.env.PORT ?? 0);
        const server = await startServer(service, port);
        const addr = server.address();
        const actual = typeof addr === "object" && addr ? addr.port : port;
        console.log(`API 已启动: http://127.0.0.1:${actual} (数据库: ${dbPath})`);
        return;
      }
      case "create": {
        const id = positional[0];
        if (!id) throw new RaftError("BAD_REQUEST", "缺少实验 id");
        const term = opts.term ? Number(opts.term) : 0;
        const log = opts.log ? validateInitialLog(readJson(opts.log)) : [];
        service.createExperiment(id, term, log);
        console.log(JSON.stringify(service.getState(id), null, 2));
        break;
      }
      case "apply": {
        const [id, reqSrc] = positional;
        if (!id || !reqSrc) throw new RaftError("BAD_REQUEST", "用法: apply <id> <请求JSON或文件>");
        const before = fmtLog(service.getState(id).log, service.getState(id).commitIndex);
        const report = service.apply(id, readJson(reqSrc));
        printReport(service, id, before, report);
        break;
      }
      case "batch": {
        const [id, src] = positional;
        if (!id || !src) throw new RaftError("BAD_REQUEST", "用法: batch <id> <JSON数组或文件>");
        const parsed = readJson(src) as { requests?: unknown[] } | unknown[];
        const list = Array.isArray(parsed) ? parsed : parsed.requests!;
        const out = service.applyBatch(id, list);
        for (const r of out.results) {
          console.log(`#${r.seq} ${r.accepted ? "接受" : "拒绝"} [${r.reason}] ${r.message}`);
        }
        console.log(`完成 ${out.completed}/${out.total}，首个失败序号: ${out.firstFailureSeq ?? "无"}`);
        break;
      }
      case "show": {
        const s = service.getState(positional[0]!);
        console.log(`实验 ${s.id} | 当前任期 ${s.currentTerm} | commitIndex ${s.commitIndex}`);
        console.log(fmtLog(s.log, s.commitIndex));
        break;
      }
      case "records":
        console.log(JSON.stringify(service.getRecords(positional[0]!), null, 2));
        break;
      case "export":
        console.log(JSON.stringify(service.exportExperiment(positional[0]!), null, 2));
        break;
      case "list":
        console.log(JSON.stringify(service.listExperiments(), null, 2));
        break;
      case "demo":
        runDemo(service);
        break;
      default:
        process.stdout.write(HELP);
    }
  } finally {
    if (cmd !== "serve") store.close();
  }
}

main().catch((err) => {
  if (err instanceof RaftError) {
    console.error(`错误 [${err.code}]: ${err.message}`);
  } else {
    console.error(err);
  }
  process.exitCode = 1;
});
