import test from "node:test";
import assert from "node:assert/strict";
import {
  FollowerState,
  initialState,
  processAppendEntries,
  validateRequest,
} from "../src/core";

/** 教学用的小型数组参考实现：直接以数组下标作为日志索引（含哨兵）。 */
function referenceApply(base: FollowerState, req: ReturnType<typeof validateRequest>) {
  if (req.term < base.currentTerm) return { accepted: false, state: base };
  const term = Math.max(base.currentTerm, req.term);
  const log = base.log.map((e) => ({ ...e }));
  let commit = base.commitIndex;
  if (log.length <= req.prevLogIndex || log[req.prevLogIndex].term !== req.prevLogTerm) {
    return { accepted: false, state: { ...base, currentTerm: term } };
  }
  for (let i = 0; i < req.entries.length; i++) {
    const index = req.prevLogIndex + 1 + i;
    const incoming = req.entries[i];
    if (index < log.length) {
      if (log[index].term !== incoming.term) {
        if (index <= commit) return { accepted: false, state: { ...base, currentTerm: term } };
        log.length = index; // 截断冲突后缀
      } else {
        continue; // 相同任期视为同一日志槽位（参考实现不检查命令差异）
      }
    }
    log[index] = { index, term: incoming.term, command: incoming.command };
  }
  const ack = req.prevLogIndex + req.entries.length;
  commit = Math.max(commit, Math.min(req.leaderCommit, ack));
  return { accepted: true, state: { currentTerm: term, log, commitIndex: commit } };
}

function stateEqual(actual: FollowerState, expected: FollowerState): void {
  assert.deepEqual(actual, expected);
}

test("正常追加并按 leaderCommit 提交", () => {
  const result = processAppendEntries(initialState(), {
    term: 1,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [
      { term: 1, command: "a" },
      { term: 1, command: "b" },
      { term: 1, command: "c" },
    ],
    leaderCommit: 2,
  });
  assert.equal(result.outcome, "accepted");
  assert.deepEqual(result.appendedRange, { from: 1, to: 3 });
  assert.deepEqual(result.newlyCommittedRange, { from: 1, to: 2 });
  assert.equal(result.state.commitIndex, 2);
  const ref = referenceApply(initialState(), validateRequest({
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }, { term: 1, command: "c" }],
    leaderCommit: 2,
  }));
  stateEqual(result.state, ref.state);
});

test("成功重发不重复追加，提交位置不回退", () => {
  const rpc = {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }],
    leaderCommit: 2,
  };
  const first = processAppendEntries(initialState(), rpc);
  const second = processAppendEntries(first.state, { ...rpc, leaderCommit: 0 });
  assert.equal(second.outcome, "accepted");
  assert.equal(second.appendedRange, null);
  assert.equal(second.matchedIndex, 2);
  assert.equal(second.state.log.length, 3);
  assert.equal(second.state.commitIndex, 2, "提交位置不能回退");
  assert.equal(second.newlyCommittedRange, null);
});

test("较短匹配请求不删除本地更长尾部", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }, { term: 1, command: "c" }],
    leaderCommit: 0,
  });
  const short = processAppendEntries(first.state, {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }],
    leaderCommit: 0,
  });
  assert.equal(short.outcome, "accepted");
  assert.equal(short.deletedRange, null);
  assert.equal(short.appendedRange, null);
  assert.equal(short.state.log.length, 4, "索引 3 必须保留");
});

test("空日志上的空条目心跳", () => {
  const result = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 5,
  });
  assert.equal(result.outcome, "accepted");
  assert.equal(result.acknowledgedIndex, 0);
  assert.equal(result.state.commitIndex, 0, "空条目心跳不能提交任何条目");
});

test("非空日志空条目心跳遵守确认前缀", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }],
    leaderCommit: 0,
  });
  const hb = processAppendEntries(first.state, {
    term: 1, prevLogIndex: 1, prevLogTerm: 1, entries: [], leaderCommit: 2,
  });
  assert.equal(hb.state.commitIndex, 1, "只能确认到 prevLogIndex=1");
});

test("旧任期直接拒绝且不记录任何变化", () => {
  const base = { ...initialState(), currentTerm: 3 };
  const result = processAppendEntries(base, {
    term: 2, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0,
  });
  assert.equal(result.outcome, "rejected");
  assert.equal(result.reason, "TERM_STALE");
  assert.equal(result.termUpdated, false);
  stateEqual(result.state, base);
});

test("新任期但前缀失败仍持久化新任期", () => {
  const result = processAppendEntries(initialState(), {
    term: 5, prevLogIndex: 9, prevLogTerm: 5, entries: [], leaderCommit: 0,
  });
  assert.equal(result.outcome, "rejected");
  assert.equal(result.reason, "PREV_LOG_MISSING");
  assert.equal(result.termAfter, 5);
  assert.equal(result.termUpdated, true);
  assert.equal(result.state.currentTerm, 5);
  assert.equal(result.state.log.length, 1, "日志不变");
});

test("前驱任期不符被拒绝", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }], leaderCommit: 0,
  });
  const result = processAppendEntries(first.state, {
    term: 2, prevLogIndex: 1, prevLogTerm: 2, entries: [{ term: 2, command: "b" }],
    leaderCommit: 0,
  });
  assert.equal(result.reason, "PREV_LOG_TERM_MISMATCH");
  assert.equal(result.state.currentTerm, 2, "新任期仍要保留");
});

test("冲突后缀替换：删除旧后缀并追加新条目", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [
      { term: 1, command: "a" },
      { term: 1, command: "b" },
      { term: 1, command: "c" },
    ],
    leaderCommit: 1,
  });
  const result = processAppendEntries(first.state, {
    term: 2, prevLogIndex: 2, prevLogTerm: 1,
    entries: [{ term: 2, command: "c2" }, { term: 2, command: "d2" }],
    leaderCommit: 1,
  });
  assert.equal(result.outcome, "accepted");
  assert.equal(result.firstConflictIndex, 3);
  assert.deepEqual(result.deletedRange, { from: 3, to: 3 });
  assert.deepEqual(result.appendedRange, { from: 3, to: 4 });
  assert.equal(result.state.log.length, 5);
  assert.equal(result.state.log[3].command, "c2");
  assert.equal(result.state.log[3].term, 2);
  assert.equal(result.state.commitIndex, 1, "已提交位置不受影响");
  const ref = referenceApply(first.state, validateRequest({
    term: 2, prevLogIndex: 2, prevLogTerm: 1,
    entries: [{ term: 2, command: "c2" }, { term: 2, command: "d2" }],
    leaderCommit: 1,
  }));
  stateEqual(result.state, ref.state);
});

test("相同索引与任期但命令不同：报告协议假设违反", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }], leaderCommit: 0,
  });
  const result = processAppendEntries(first.state, {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "DIFFERENT" }], leaderCommit: 0,
  });
  assert.equal(result.outcome, "invalid_input");
  assert.equal(result.reason, "SAME_TERM_COMMAND_CONFLICT");
  assert.equal(result.state.log[1].command, "a", "日志保持原样");
});

test("不得截断已提交条目", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }],
    leaderCommit: 2,
  });
  const result = processAppendEntries(first.state, {
    term: 3, prevLogIndex: 1, prevLogTerm: 1,
    entries: [{ term: 3, command: "evil" }], leaderCommit: 2,
  });
  assert.equal(result.outcome, "committed_conflict");
  assert.equal(result.reason, "COMMITTED_ENTRY_CONFLICT");
  assert.equal(result.state.log.length, 3);
  assert.equal(result.state.commitIndex, 2);
  assert.equal(result.state.log[2].command, "b");
  assert.equal(result.state.currentTerm, 3, "新任期仍保留");
});

test("提交位置回退请求不会降低提交位置", () => {
  const first = processAppendEntries(initialState(), {
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }],
    leaderCommit: 2,
  });
  const result = processAppendEntries(first.state, {
    term: 1, prevLogIndex: 2, prevLogTerm: 1, entries: [], leaderCommit: 0,
  });
  assert.equal(result.state.commitIndex, 2);
});

test("参数校验：非负安全整数与任期顺序", () => {
  assert.throws(() => validateRequest({
    term: -1, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0,
  }), /非负安全整数/);
  assert.throws(() => validateRequest({
    term: 1, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 2, command: "x" }], leaderCommit: 0,
  }), /超过请求任期/);
  assert.throws(() => validateRequest({
    term: 3, prevLogIndex: 0, prevLogTerm: 2,
    entries: [{ term: 1, command: "x" }], leaderCommit: 0,
  }), /非递减/);
});

test("多步序列与数组参考实现全程一致", () => {
  const requests = [
    { term: 1, prevLogIndex: 0, prevLogTerm: 0,
      entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }, { term: 1, command: "c" }],
      leaderCommit: 2 },
    { term: 1, prevLogIndex: 0, prevLogTerm: 0,
      entries: [{ term: 1, command: "a" }, { term: 1, command: "b" }], leaderCommit: 5 },
    { term: 2, prevLogIndex: 2, prevLogTerm: 1,
      entries: [{ term: 2, command: "c2" }], leaderCommit: 2 },
    { term: 2, prevLogIndex: 3, prevLogTerm: 2, entries: [], leaderCommit: 3 },
  ];
  let actual = initialState();
  let refState = initialState();
  for (const request of requests) {
    const result = processAppendEntries(actual, request);
    assert.equal(result.outcome, "accepted");
    actual = result.state;
    refState = referenceApply(refState, validateRequest(request)).state;
    stateEqual(actual, refState);
  }
  assert.equal(actual.commitIndex, 3);
});
