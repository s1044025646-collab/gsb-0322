#!/usr/bin/env node
/** CLI 入口：serve / demo / create / show / records / export / append。 */
import * as path from "node:path";
import * as fs from "node:fs";
import { Store } from "./store";
import { RaftService } from "./service";
import { createServer, listen } from "./server";
import { runDemo } from "./demo";
import { renderLog, renderSummary } from "./display";

function usage(): never {
  console.log(`Raft 跟随者日志前缀核对教学后端

用法:
  node dist/cli.js serve [--port PORT] [--data DIR]   启动 HTTP API（默认空闲端口）
  node dist/cli.js demo  [--data DIR]                 运行内置教学演示
  node dist/cli.js create NAME [--entries FILE] [--data DIR]
  node dist/cli.js list [--data DIR]
  node dist/cli.js show ID [--data DIR]               查看日志与提交前缀
  node dist/cli.js records ID [--data DIR]            读取处理记录
  node dist/cli.js export ID [--data DIR]             导出实验 JSON
  node dist/cli.js append ID --rpc FILE [--data DIR]  应用单个 AppendEntries 请求

--entries/--rpc 文件为 JSON；entries 形如 [{"term":1,"command":"SET a=1"}]。
数据默认存于项目内 ./data/raft.db。`);
  process.exit(1);
}

function parseArgs(argv: string[]): {
  positional: string[];
  flags: Record<string, string>;
} {
  const positional: string[] = [];
  const flags: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token.startsWith("--")) {
      flags[token.slice(2)] = argv[++i] ?? "";
    } else {
      positional.push(token);
    }
  }
  return { positional, flags };
}

function readJsonFile(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.resolve(file), "utf-8"));
}

async function main(): Promise<void> {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const command = positional[0];
  if (!command) usage();
  const dataDir = path.resolve(flags.data ?? path.join(process.cwd(), "data"));
  const store = new Store(path.join(dataDir, "raft.db"));
  const service = new RaftService(store);

  try {
    switch (command) {
      case "serve": {
        const port = Number(flags.port ?? 0);
        if (!Number.isInteger(port) || port < 0 || port > 65535) {
          throw new Error("--port 必须是 0..65535 的整数（0=自动选择空闲端口）");
        }
        const server = createServer(service);
        const actual = await listen(server, port);
        console.log(`HTTP API 已启动: http://127.0.0.1:${actual}`);
        console.log(`数据目录: ${dataDir}`);
        console.log("按 Ctrl+C 退出。");
        break;
      }
      case "demo": {
        runDemo(service);
        break;
      }
      case "create": {
        const name = positional[1];
        if (!name) throw new Error("必须提供 NAME");
        const entries = flags.entries
          ? (readJsonFile(flags.entries) as Array<{ term: number; command: string }>)
          : [];
        const id = service.create(name, entries);
        console.log(`已创建 follower：id=${id} name=${name}`);
        console.log(renderSummary(service.state(id)));
        break;
      }
      case "list": {
        const rows = service.list();
        if (rows.length === 0) console.log("（暂无实验）");
        for (const row of rows) {
          console.log(
            `#${row.id} ${row.name} 任期=${row.currentTerm} 提交=${row.commitIndex} 日志条数=${row.logLength}`,
          );
        }
        break;
      }
      case "show": {
        const id = Number(positional[1]);
        const state = service.state(id);
        console.log(renderSummary(state));
        console.log(renderLog(state));
        break;
      }
      case "records": {
        const id = Number(positional[1]);
        for (const record of service.records(id)) {
          console.log(
            `#${record.seq} [${record.createdAt}]\n  请求: ${record.request}\n  结果: ${record.result}`,
          );
        }
        break;
      }
      case "export": {
        const id = Number(positional[1]);
        console.log(JSON.stringify(service.export(id), null, 2));
        break;
      }
      case "append": {
        const id = Number(positional[1]);
        if (!flags.rpc) throw new Error("必须用 --rpc FILE 指定请求 JSON");
        const rpc = readJsonFile(flags.rpc);
        const result = service.append(id, rpc);
        console.log(renderSummary(result.state));
        console.log(renderLog(result.state));
        console.log(JSON.stringify({ ...result, state: undefined }, null, 2));
        break;
      }
      default:
        usage();
    }
  } finally {
    if (command !== "serve") store.close();
  }
}

main().catch((err: Error) => {
  console.error(`错误 [${(err as { code?: string }).code ?? "ERROR"}]: ${err.message}`);
  process.exit(1);
});
