import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Store } from "../src/store";
import { RaftService } from "../src/service";
import { initialState } from "../src/core";

function tempDb(): { dir: string; file: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "raft-test-"));
  return { dir, file: path.join(dir, "raft.db") };
}

test("状态与记录在同一事务提交，注入失败后整体回滚", () => {
  const { dir, file } = tempDb();
  const store = new Store(file);
  const service = new RaftService(store);
  const id = service.create("tx", []);

  service.injectCrash();
  assert.throws(
    () =>
      service.append(id, {
        term: 1, prevLogIndex: 0, prevLogTerm: 0,
        entries: [{ term: 1, command: "x" }], leaderCommit: 0,
      }),
    /INJECTED_WRITE_FAILURE/,
  );

  const after = service.state(id);
  assert.deepEqual(after, initialState(), "回滚后必须仍是初始状态");
  assert.equal(service.records(id).length, 0, "处理记录也必须回滚");
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test("拒绝请求仍记录处理结果与合法新任期更新", () => {
  const { dir, file } = tempDb();
  const store = new Store(file);
  const service = new RaftService(store);
  const id = service.create("rec", []);
  const result = service.append(id, {
    term: 4, prevLogIndex: 9, prevLogTerm: 4, entries: [], leaderCommit: 0,
  });
  assert.equal(result.success, false);
  assert.equal(service.state(id).currentTerm, 4, "新任期持久化");
  const records = service.records(id);
  assert.equal(records.length, 1);
  const stored = JSON.parse(records[0].result);
  assert.equal(stored.outcome, "rejected");
  assert.equal(stored.termAfter, 4);
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test("批量以逐请求边界执行并报告首个失败位置", () => {
  const { dir, file } = tempDb();
  const store = new Store(file);
  const service = new RaftService(store);
  const id = service.create("batch", []);
  const batch = [
    { term: 1, prevLogIndex: 0, prevLogTerm: 0,
      entries: [{ term: 1, command: "a" }], leaderCommit: 0 },
    { term: 1, prevLogIndex: 0, prevLogTerm: 0,
      entries: [{ term: 1, command: "a" }], leaderCommit: 0 },
    { term: 0, prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0 },
  ];
  const outcome = service.appendBatch(id, batch);
  assert.equal(outcome.firstFailureIndex, 2);
  assert.equal(outcome.applied, 2);
  assert.equal(service.records(id).length, 3, "拒绝请求同样留痕，批量在该位置停止");
  assert.equal(service.state(id).log.length, 2);
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test("重启后任期、日志、提交位置与请求次序保持", () => {
  const { dir, file } = tempDb();
  const store1 = new Store(file);
  const service1 = new RaftService(store1);
  const id = service1.create("persist", []);
  service1.append(id, {
    term: 2, prevLogIndex: 0, prevLogTerm: 0,
    entries: [{ term: 1, command: "a" }, { term: 2, command: "b" }],
    leaderCommit: 1,
  });
  service1.append(id, {
    term: 2, prevLogIndex: 2, prevLogTerm: 2,
    entries: [{ term: 2, command: "c" }], leaderCommit: 2,
  });
  store1.close();

  const store2 = new Store(file);
  const service2 = new RaftService(store2);
  const state = service2.state(id);
  assert.equal(state.currentTerm, 2);
  assert.equal(state.commitIndex, 2);
  assert.equal(state.log.length, 4);
  const records = service2.records(id);
  assert.deepEqual(records.map((r) => r.seq), [1, 2], "请求次序保持");
  store2.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test("报告范围对应真实条目", () => {
  const { dir, file } = tempDb();
  const store = new Store(file);
  const service = new RaftService(store);
  const id = service.create("range", [
    { term: 1, command: "a" },
    { term: 1, command: "b" },
    { term: 1, command: "c" },
  ]);
  const result = service.append(id, {
    term: 2, prevLogIndex: 1, prevLogTerm: 1,
    entries: [{ term: 2, command: "b2" }, { term: 2, command: "d2" }],
    leaderCommit: 1,
  });
  assert.deepEqual(result.deletedRange, { from: 2, to: 3 });
  assert.deepEqual(result.appendedRange, { from: 2, to: 3 });
  const commands = result.state.log.slice(2).map((e) => e.command);
  assert.deepEqual(commands, ["b2", "d2"]);
  store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

