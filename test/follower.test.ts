import { test } from "node:test";
import assert from "node:assert/strict";
import { Store } from "../src/store.js";
import { FollowerService } from "../src/follower.js";
import type { AppendEntriesRequest, LogEntry } from "../src/types.js";

// ---------- 小型数组参考实现 ----------
interface RefState {
  currentTerm: number;
  commitIndex: number;
  log: LogEntry[]; // log[0] 为哨兵
}

function refApply(s: RefState, req: AppendEntriesRequest): { accepted: boolean } {
  if (req.term < s.currentTerm) return { accepted: false };
  if (req.term > s.currentTerm) s.currentTerm = req.term;
  const prev = s.log[req.prevLogIndex];
  if (!prev || prev.term !== req.prevLogTerm) return { accepted: false };
  for (let i = 0; i < req.entries.length; i++) {
    const idx = req.prevLogIndex + 1 + i;
    const existing = s.log[idx];
    if (!existing) break;
    if (existing.term !== req.entries[i]!.term) {
      if (idx <= s.commitIndex) return { accepted: false };
      s.log.length = idx;
      break;
    }
    if (existing.command !== req.entries[i]!.command) return { accepted: false };
  }
  const start = Math.min(req.prevLogIndex + 1 + req.entries.length, s.log.length);
  for (let i = start - (req.prevLogIndex + 1); i < req.entries.length; i++) {
    s.log.push(req.entries[i]!);
  }
  const matchIndex = req.prevLogIndex + req.entries.length;
  s.commitIndex = Math.max(s.commitIndex, Math.min(req.leaderCommit, matchIndex));
  return { accepted: true };
}

function makeService(): { service: FollowerService; store: Store } {
  const store = new Store(":memory:");
  return { service: new FollowerService(store), store };
}

function setup(id = "t"): FollowerService {
  const { service } = makeService();
  service.createExperiment(id, 1, [
    { term: 1, command: "a" },
    { term: 1, command: "b" },
    { term: 2, command: "c" },
  ]);
  return service;
}

// ---------- 场景测试 ----------

test("正常追加与提交推进", () => {
  const s = setup();
  const r = s.apply("t", {
    term: 2, prevLogIndex: 3, prevLogTerm: 2,
    entries: [{ term: 2, command: "d" }], leaderCommit: 4,
  });
  assert.equal(r.accepted, true);
  assert.equal(r.matchIndex, 4);
  assert.deepEqual(r.appendedRange, [4, 4]);
  assert.equal(r.newCommitIndex, 4);
  assert.equal(r.newlyCommitted.length, 4); // 索引 1..4
  const st = s.getState("t");
  assert.equal(st.log.length, 5);
  assert.equal(st.commitIndex, 4);
});

test("重复发送不重复追加", () => {
  const s = setup();
  const req = {
    term: 2, prevLogIndex: 3, prevLogTerm: 2,
    entries: [{ term: 2, command: "d" }], leaderCommit: 2,
  };
  s.apply("t", req);
  const r2 = s.apply("t", req);
  assert.equal(r2.accepted, true);
  assert.equal(r2.appendedRange, null);
  assert.equal(s.getState("t").log.length, 5);
});

test("前驱缺失与任期不匹配", () => {
  const s = setup();
  const r1 = s.apply("t", { term: 2, prevLogIndex: 9, prevLogTerm: 2, entries: [], leaderCommit: 0 });
  assert.equal(r1.reason, "PREV_LOG_MISSING");
  const r2 = s.apply("t", { term: 2, prevLogIndex: 3, prevLogTerm: 1, entries: [], leaderCommit: 0 });
  assert.equal(r2.reason, "PREV_LOG_TERM_MISMATCH");
  assert.equal(s.getState("t").log.length, 4);
});

test("旧任期拒绝且不修改状态", () => {
  const s = setup();
  const r = s.apply("t", { term: 0, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0 });
  assert.equal(r.accepted, false);
  assert.equal(r.reason, "STALE_TERM");
  assert.equal(s.getState("t").currentTerm, 1);
});

test("新任期即使前缀失败也被持久化", () => {
  const s = setup();
  const r = s.apply("t", { term: 7, prevLogIndex: 99, prevLogTerm: 0, entries: [], leaderCommit: 0 });
  assert.equal(r.accepted, false);
  assert.equal(r.oldTerm, 1);
  assert.equal(r.newTerm, 7);
  assert.equal(s.getState("t").currentTerm, 7);
});

test("冲突后缀替换：从首个任期冲突处截断并追加", () => {
  const s = setup();
  const r = s.apply("t", {
    term: 3, prevLogIndex: 1, prevLogTerm: 1,
    entries: [{ term: 3, command: "B" }, { term: 3, command: "C" }, { term: 3, command: "D" }],
    leaderCommit: 0,
  });
  assert.equal(r.accepted, true);
  assert.equal(r.firstConflictIndex, 2);
  assert.deepEqual(r.deletedRange, [2, 3]);
  assert.deepEqual(r.appendedRange, [2, 4]);
  const st = s.getState("t");
  assert.deepEqual(st.log.map((e) => e.command), ["", "a", "B", "C", "D"]);
});

test("较短匹配请求保留本地更长尾部（含空条目心跳）", () => {
  const s = setup();
  const r = s.apply("t", { term: 2, prevLogIndex: 1, prevLogTerm: 1, entries: [], leaderCommit: 1 });
  assert.equal(r.accepted, true);
  assert.equal(r.deletedRange, null);
  assert.equal(s.getState("t").log.length, 4);
  assert.equal(s.getState("t").commitIndex, 1);
});

test("提交位置不回退，且不提交未确认的本地旧尾部", () => {
  const s = setup();
  s.apply("t", { term: 2, prevLogIndex: 3, prevLogTerm: 2, entries: [], leaderCommit: 3 });
  const r = s.apply("t", { term: 2, prevLogIndex: 1, prevLogTerm: 1, entries: [], leaderCommit: 99 });
  assert.equal(r.newCommitIndex, 3); // 只确认到 matchIndex=1，但已有 3 不回退
  assert.equal(s.getState("t").commitIndex, 3);
});

test("已提交位置冲突被拒绝，日志与提交位置保留", () => {
  const s = setup();
  s.apply("t", { term: 2, prevLogIndex: 3, prevLogTerm: 2, entries: [], leaderCommit: 2 });
  const before = s.getState("t");
  const r = s.apply("t", {
    term: 3, prevLogIndex: 1, prevLogTerm: 1,
    entries: [{ term: 3, command: "x" }], leaderCommit: 2,
  });
  assert.equal(r.accepted, false);
  assert.equal(r.reason, "COMMITTED_CONFLICT");
  assert.equal(r.firstConflictIndex, 2);
  const after = s.getState("t");
  assert.deepEqual(after.log, before.log);
  assert.equal(after.commitIndex, 2);
});

test("同索引同任期不同命令报告协议违反", () => {
  const s = setup();
  const r = s.apply("t", {
    term: 2, prevLogIndex: 2, prevLogTerm: 1,
    entries: [{ term: 2, command: "DIFFERENT" }], leaderCommit: 0,
  });
  assert.equal(r.accepted, false);
  assert.equal(r.reason, "PROTOCOL_VIOLATION");
  assert.equal(s.getState("t").log.length, 4);
});

test("空日志实验与空条目心跳", () => {
  const { service } = makeService();
  service.createExperiment("empty", 0, []);
  const r = service.apply("empty", { term: 1, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0 });
  assert.equal(r.accepted, true);
  assert.equal(r.matchIndex, 0);
  assert.equal(service.getState("empty").log.length, 1); // 仅哨兵
});

test("参数校验：非法任期与条目任期顺序", () => {
  const s = setup();
  const codeOf = (fn: () => void) => {
    try {
      fn();
    } catch (e) {
      return (e as { code?: string }).code;
    }
    return undefined;
  };
  assert.equal(
    codeOf(() => s.apply("t", { term: -1, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0 })),
    "INVALID_TERM"
  );
  assert.equal(
    codeOf(() => s.apply("t", { term: 2, prevLogIndex: 0, prevLogTerm: 0, entries: [{ term: 3, command: "x" }], leaderCommit: 0 })),
    "ENTRY_TERM_EXCEEDS_REQUEST"
  );
  assert.equal(
    codeOf(() =>
      s.apply("t", {
        term: 3, prevLogIndex: 0, prevLogTerm: 0,
        entries: [{ term: 3, command: "x" }, { term: 2, command: "y" }], leaderCommit: 0,
      })
    ),
    "ENTRY_TERM_ORDER"
  );
});

test("批量：逐请求边界，报告首个失败位置", () => {
  const s = setup();
  const out = s.applyBatch("t", [
    { term: 2, prevLogIndex: 3, prevLogTerm: 2, entries: [{ term: 2, command: "d" }], leaderCommit: 0 },
    { term: 2, prevLogIndex: 99, prevLogTerm: 2, entries: [], leaderCommit: 0 },
    { term: 2, prevLogIndex: 4, prevLogTerm: 2, entries: [], leaderCommit: 0 },
  ]);
  assert.equal(out.completed, 2);
  assert.equal(out.total, 3);
  assert.equal(out.firstFailureSeq, out.results[1]!.seq);
  assert.equal(s.getState("t").log.length, 5);
});

test("重启后状态保持（重新打开同一数据库文件）", async () => {
  const fs = await import("node:fs");
  const os = await import("node:os");
  const path = await import("node:path");
  const db = path.join(os.tmpdir(), `raft-test-${Date.now()}.db`);
  try {
    const store1 = new Store(db);
    const s1 = new FollowerService(store1);
    s1.createExperiment("r", 1, [{ term: 1, command: "a" }]);
    s1.apply("r", { term: 2, prevLogIndex: 1, prevLogTerm: 1, entries: [{ term: 2, command: "b" }], leaderCommit: 2 });
    store1.close();
    const store2 = new Store(db);
    const s2 = new FollowerService(store2);
    const st = s2.getState("r");
    assert.equal(st.currentTerm, 2);
    assert.equal(st.commitIndex, 2);
    assert.equal(st.log.length, 3);
    assert.equal(s2.getRecords("r").length, 1);
    const r = s2.apply("r", { term: 2, prevLogIndex: 2, prevLogTerm: 2, entries: [], leaderCommit: 2 });
    assert.equal(r.seq, 2); // 请求次序延续
    store2.close();
  } finally {
    fs.rmSync(db, { force: true });
    fs.rmSync(db + "-wal", { force: true });
    fs.rmSync(db + "-shm", { force: true });
  }
});

test("注入写入失败：事务回滚，无部分后缀替换", () => {
  const { service, store } = makeService();
  service.createExperiment("t", 1, [
    { term: 1, command: "a" },
    { term: 1, command: "b" },
  ]);
  const orig = store.insertEntry.bind(store);
  store.insertEntry = () => {
    throw new Error("注入的磁盘写入失败");
  };
  assert.throws(() =>
    service.apply("t", {
      term: 2, prevLogIndex: 1, prevLogTerm: 1,
      entries: [{ term: 2, command: "B" }, { term: 2, command: "C" }], leaderCommit: 0,
    })
  );
  store.insertEntry = orig;
  const st = service.getState("t");
  assert.equal(st.log.length, 3); // 截断被回滚
  assert.deepEqual(st.log.map((e) => e.command), ["", "a", "b"]);
  assert.equal(st.currentTerm, 1); // 任期更新也回滚
  assert.equal(service.getRecords("t").length, 0); // 记录也回滚
});

// ---------- 与参考实现随机核对 ----------

test("随机请求序列与数组参考实现一致", () => {
  let seed = 42;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
  for (let trial = 0; trial < 30; trial++) {
    const { service } = makeService();
    const id = "p";
    service.createExperiment(id, 1, []);
    const ref: RefState = { currentTerm: 1, commitIndex: 0, log: [{ term: 0, command: "" }] };
    let leaderTerm = 1;
    const committedHistory: string[] = [];
    for (let step = 0; step < 60; step++) {
      if (rand() < 0.3) leaderTerm += 1;
      const prevLogIndex = Math.floor(rand() * ref.log.length);
      const prev = ref.log[prevLogIndex]!;
      const entries: LogEntry[] = [];
      const n = Math.floor(rand() * 3);
      let t = Math.max(prev.term, leaderTerm - (rand() < 0.5 ? 1 : 0));
      for (let i = 0; i < n; i++) {
        if (rand() < 0.5) t = Math.min(leaderTerm, t + 1);
        entries.push({ term: t, command: `c${step}-${i}` });
      }
      const req: AppendEntriesRequest = {
        term: leaderTerm,
        prevLogIndex,
        prevLogTerm: prev.term,
        entries,
        leaderCommit: Math.floor(rand() * (prevLogIndex + n + 2)),
      };
      const report = service.apply(id, req);
      const refRes = refApply(ref, req);
      assert.equal(report.accepted, refRes.accepted, `trial=${trial} step=${step} req=${JSON.stringify(req)}`);
      const st = service.getState(id);
      assert.equal(st.currentTerm, ref.currentTerm);
      assert.equal(st.commitIndex, ref.commitIndex);
      assert.deepEqual(
        st.log.map((e) => [e.term, e.command]),
        ref.log.map((e) => [e.term, e.command])
      );
      // 已提交前缀从不改变
      const committed = st.committedPrefix;
      for (const prev2 of committedHistory) {
        const p = JSON.parse(prev2) as unknown[];
        assert.deepEqual(committed.slice(0, p.length), p, "已提交前缀必须保持不变");
      }
      committedHistory.push(JSON.stringify(committed));
    }
  }
});
