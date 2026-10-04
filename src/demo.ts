/** 预置教学演示：覆盖重复发送、前驱不匹配、冲突替换、较短匹配保留尾部等。 */
import { FollowerState, ProcessResult } from "./core";
import { RaftService } from "./service";
import { renderRequest, renderSummary } from "./display";

interface DemoStep {
  title: string;
  request: any;
}

const steps: DemoStep[] = [
  {
    title: "1. 正常追加 3 条，并提交前 2 条",
    request: {
      term: 1,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: [
        { term: 1, command: "SET x=1" },
        { term: 1, command: "SET y=2" },
        { term: 1, command: "SET z=3" },
      ],
      leaderCommit: 2,
    },
  },
  {
    title: "2. 重复发送同一请求（幂等，不重复追加，提交不回退）",
    request: {
      term: 1,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: [
        { term: 1, command: "SET x=1" },
        { term: 1, command: "SET y=2" },
        { term: 1, command: "SET z=3" },
      ],
      leaderCommit: 0,
    },
  },
  {
    title: "3. 较短匹配请求（只到索引 2）：必须保留索引 3 的本地尾部",
    request: {
      term: 1,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: [
        { term: 1, command: "SET x=1" },
        { term: 1, command: "SET y=2" },
      ],
      leaderCommit: 0,
    },
  },
  {
    title: "4. 空条目心跳：确认前缀=2，提交位置保持不回退",
    request: { term: 1, prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 1 },
  },
  {
    title: "5. 前驱不存在（prevLogIndex=9）：拒绝",
    request: { term: 1, prevLogIndex: 9, prevLogTerm: 1, entries: [], leaderCommit: 2 },
  },
  {
    title: "6. 前驱任期不符：拒绝",
    request: { term: 1, prevLogIndex: 3, prevLogTerm: 5, entries: [], leaderCommit: 2 },
  },
  {
    title: "7. 旧任期请求（term=0 < 1）：直接拒绝",
    request: { term: 0, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0 },
  },
  {
    title: "8. 新任期但前缀失败：任期要持久化（term→2），请求仍被拒绝",
    request: { term: 2, prevLogIndex: 8, prevLogTerm: 2, entries: [], leaderCommit: 0 },
  },
  {
    title: "9. 冲突后缀替换：索引 3 任期变为 2，截断旧 [3] 并追加",
    request: {
      term: 2,
      prevLogIndex: 2,
      prevLogTerm: 1,
      entries: [{ term: 2, command: "SET z=30" }],
      leaderCommit: 2,
    },
  },
  {
    title: "10. 已提交位置冲突：索引 2 已提交，不能截断",
    request: {
      term: 3,
      prevLogIndex: 1,
      prevLogTerm: 1,
      entries: [{ term: 3, command: "EVIL" }],
      leaderCommit: 2,
    },
  },
];

export function runDemo(service: RaftService): number {
  const id = service.create("demo", []);
  let seq = 0;
  for (const step of steps) {
    seq += 1;
    const before: FollowerState = JSON.parse(JSON.stringify(service.state(id)));
    let result: ProcessResult;
    try {
      result = service.append(id, step.request);
    } catch (err) {
      console.log(`\n${step.title}\n校验错误：${(err as Error).message}`);
      continue;
    }
    console.log(`\n${step.title}`);
    console.log(renderRequest(seq, step.request, before, result));
  }
  console.log(`\n最终状态：${renderSummary(service.state(id))}`);
  console.log(`实验 ID=${id}，可用 CLI 查询/导出，重启后状态保持。`);
  return id;
}
