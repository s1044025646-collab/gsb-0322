import { RaftError } from "./types.js";
import type { AppendEntriesRequest, LogEntry, RequestReport } from "./types.js";
import { Store } from "./store.js";
import type { StoredEntry } from "./store.js";

function isSafeNonNeg(n: unknown): n is number {
  return typeof n === "number" && Number.isSafeInteger(n) && n >= 0;
}

export function validateRequest(raw: unknown): AppendEntriesRequest {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new RaftError("BAD_REQUEST", "请求必须是 JSON 对象");
  }
  const r = raw as Record<string, unknown>;
  if (!isSafeNonNeg(r.term)) throw new RaftError("INVALID_TERM", "term 必须是非负安全整数");
  if (!isSafeNonNeg(r.prevLogIndex))
    throw new RaftError("INVALID_INDEX", "prevLogIndex 必须是非负安全整数");
  if (!isSafeNonNeg(r.prevLogTerm))
    throw new RaftError("INVALID_TERM", "prevLogTerm 必须是非负安全整数");
  if (!isSafeNonNeg(r.leaderCommit))
    throw new RaftError("INVALID_COMMIT", "leaderCommit 必须是非负安全整数");
  if (!Array.isArray(r.entries)) throw new RaftError("INVALID_ENTRIES", "entries 必须是数组");
  const entries: LogEntry[] = [];
  for (const e of r.entries as unknown[]) {
    if (typeof e !== "object" || e === null)
      throw new RaftError("INVALID_ENTRIES", "条目必须是对象 {term, command}");
    const eo = e as Record<string, unknown>;
    if (!isSafeNonNeg(eo.term)) throw new RaftError("INVALID_TERM", "条目 term 必须是非负安全整数");
    if (typeof eo.command !== "string")
      throw new RaftError("INVALID_COMMAND", "条目 command 必须是字符串");
    entries.push({ term: eo.term, command: eo.command });
  }
  for (let i = 0; i < entries.length; i++) {
    const t = entries[i]!.term;
    if (t > r.term)
      throw new RaftError("ENTRY_TERM_EXCEEDS_REQUEST", `条目任期 ${t} 超过请求任期 ${r.term}`);
    if (i > 0 && t < entries[i - 1]!.term)
      throw new RaftError("ENTRY_TERM_ORDER", "条目任期序列必须单调不减（符合日志顺序）");
  }
  if (entries.length > 0 && entries[0]!.term < r.prevLogTerm)
    throw new RaftError("ENTRY_TERM_ORDER", "首条新条目任期不得小于 prevLogTerm");
  return {
    term: r.term,
    prevLogIndex: r.prevLogIndex,
    prevLogTerm: r.prevLogTerm,
    entries,
    leaderCommit: r.leaderCommit,
  };
}

export function validateInitialLog(raw: unknown): LogEntry[] {
  if (!Array.isArray(raw)) throw new RaftError("INVALID_ENTRIES", "initialLog 必须是数组");
  const entries: LogEntry[] = [];
  for (const e of raw as unknown[]) {
    if (typeof e !== "object" || e === null)
      throw new RaftError("INVALID_ENTRIES", "条目必须是对象 {term, command}");
    const eo = e as Record<string, unknown>;
    if (!isSafeNonNeg(eo.term)) throw new RaftError("INVALID_TERM", "条目 term 必须是非负安全整数");
    if (typeof eo.command !== "string")
      throw new RaftError("INVALID_COMMAND", "条目 command 必须是字符串");
    entries.push({ term: eo.term, command: eo.command });
  }
  for (let i = 1; i < entries.length; i++) {
    if (entries[i]!.term < entries[i - 1]!.term)
      throw new RaftError("ENTRY_TERM_ORDER", "初始日志任期必须单调不减");
  }
  return entries;
}

export const SENTINEL: LogEntry = { term: 0, command: "" };

export class FollowerService {
  constructor(private store: Store) {}

  createExperiment(id: string, currentTerm: number, initialLog: LogEntry[]): void {
    if (!isSafeNonNeg(currentTerm)) throw new RaftError("INVALID_TERM", "currentTerm 非法");
    if (this.store.getExperiment(id)) throw new RaftError("BAD_REQUEST", `实验 ${id} 已存在`);
    const entries: StoredEntry[] = [
      { idx: 0, term: SENTINEL.term, command: SENTINEL.command },
      ...initialLog.map((e, i) => ({ idx: i + 1, term: e.term, command: e.command })),
    ];
    this.store.createExperiment(id, currentTerm, entries);
  }

  listExperiments() {
    return this.store.listExperiments();
  }

  getState(id: string) {
    const exp = this.store.getExperiment(id);
    if (!exp) throw new RaftError("NOT_FOUND", `实验 ${id} 不存在`);
    const entries = this.store.getEntries(id);
    return {
      id: exp.id,
      currentTerm: exp.currentTerm,
      commitIndex: exp.commitIndex,
      log: entries.map((e) => ({ index: e.idx, term: e.term, command: e.command })),
      committedPrefix: entries
        .filter((e) => e.idx <= exp.commitIndex)
        .map((e) => ({ index: e.idx, term: e.term, command: e.command })),
    };
  }

  apply(id: string, rawRequest: unknown): RequestReport {
    const req = validateRequest(rawRequest);
    const exp = this.store.getExperiment(id);
    if (!exp) throw new RaftError("NOT_FOUND", `实验 ${id} 不存在`);
    return this.store.transaction(() => this.applyInside(id, exp.currentTerm, exp.commitIndex, req));
  }

  applyBatch(id: string, rawRequests: unknown[]): {
    results: RequestReport[];
    firstFailureSeq: number | null;
    completed: number;
    total: number;
  } {
    if (!Array.isArray(rawRequests)) throw new RaftError("BAD_REQUEST", "requests 必须是数组");
    const results: RequestReport[] = [];
    let firstFailureSeq: number | null = null;
    for (const raw of rawRequests) {
      const report = this.apply(id, raw);
      results.push(report);
      if (!report.accepted && firstFailureSeq === null) {
        firstFailureSeq = report.seq;
        break;
      }
    }
    return { results, firstFailureSeq, completed: results.length, total: rawRequests.length };
  }

  private applyInside(
    id: string,
    currentTerm: number,
    commitIndex: number,
    req: AppendEntriesRequest
  ): RequestReport {
    const entries = this.store.getEntries(id);
    const seq = this.store.nextSeq(id);
    const base: RequestReport = {
      seq,
      accepted: false,
      reason: "OK",
      message: "",
      oldTerm: currentTerm,
      newTerm: currentTerm,
      matchIndex: null,
      firstConflictIndex: null,
      deletedRange: null,
      appendedRange: null,
      oldCommitIndex: commitIndex,
      newCommitIndex: commitIndex,
      newlyCommitted: [],
      logLengthBefore: entries.length,
      logLengthAfter: entries.length,
    };
    const finish = (report: RequestReport): RequestReport => {
      this.store.insertRequest(id, seq, JSON.stringify(req), JSON.stringify(report));
      return report;
    };

    if (req.term < currentTerm) {
      return finish({
        ...base,
        reason: "STALE_TERM",
        message: `请求任期 ${req.term} 小于当前任期 ${currentTerm}，拒绝`,
      });
    }

    if (req.term > currentTerm) {
      this.store.setTerm(id, req.term);
      base.newTerm = req.term;
    }

    const prev = entries[req.prevLogIndex];
    if (!prev) {
      return finish({
        ...base,
        reason: "PREV_LOG_MISSING",
        message: `本地日志长度为 ${entries.length}，不存在 prevLogIndex=${req.prevLogIndex}`,
      });
    }
    if (prev.term !== req.prevLogTerm) {
      return finish({
        ...base,
        reason: "PREV_LOG_TERM_MISMATCH",
        message: `prevLogIndex=${req.prevLogIndex} 本地任期 ${prev.term} 与 prevLogTerm=${req.prevLogTerm} 不一致`,
      });
    }

    let conflictIndex: number | null = null;
    for (let i = 0; i < req.entries.length; i++) {
      const idx = req.prevLogIndex + 1 + i;
      const existing = entries[idx];
      if (!existing) break;
      if (existing.term !== req.entries[i]!.term) {
        conflictIndex = idx;
        break;
      }
      if (existing.command !== req.entries[i]!.command) {
        return finish({
          ...base,
          reason: "PROTOCOL_VIOLATION",
          message: `索引 ${idx} 任期相同 (${existing.term}) 但命令不同：本地 "${existing.command}" vs 请求 "${req.entries[i]!.command}"，违反协议假设`,
        });
      }
    }

    if (conflictIndex !== null && conflictIndex <= commitIndex) {
      return finish({
        ...base,
        firstConflictIndex: conflictIndex,
        reason: "COMMITTED_CONFLICT",
        message: `首个冲突索引 ${conflictIndex} 已提交（commitIndex=${commitIndex}），拒绝截断，日志与提交位置保持不变`,
      });
    }

    if (conflictIndex !== null) {
      this.store.deleteEntriesFrom(id, conflictIndex);
      base.firstConflictIndex = conflictIndex;
      base.deletedRange = [conflictIndex, entries.length - 1];
    }

    const startAppend =
      conflictIndex !== null
        ? conflictIndex
        : Math.min(req.prevLogIndex + 1 + req.entries.length, entries.length);
    const appendFromEntry = startAppend - (req.prevLogIndex + 1);
    let appended: [number, number] | null = null;
    for (let i = appendFromEntry; i < req.entries.length; i++) {
      const idx = req.prevLogIndex + 1 + i;
      this.store.insertEntry(id, { idx, term: req.entries[i]!.term, command: req.entries[i]!.command });
      if (!appended) appended = [idx, idx];
      else appended[1] = idx;
    }
    base.appendedRange = appended;

    const matchIndex = req.prevLogIndex + req.entries.length;
    const target = Math.min(req.leaderCommit, matchIndex);
    const newCommit = Math.max(commitIndex, target);
    if (newCommit > commitIndex) {
      this.store.setCommitIndex(id, newCommit);
    }
    const after = this.store.getEntries(id);
    const newlyCommitted = after
      .filter((e) => e.idx > commitIndex && e.idx <= newCommit)
      .map((e) => ({ index: e.idx, term: e.term, command: e.command }));

    return finish({
      ...base,
      accepted: true,
      reason: "OK",
      message: "已接受",
      matchIndex,
      newCommitIndex: newCommit,
      newlyCommitted,
      logLengthAfter: after.length,
    });
  }

  getRecords(id: string) {
    if (!this.store.getExperiment(id)) throw new RaftError("NOT_FOUND", `实验 ${id} 不存在`);
    return this.store.getRequests(id);
  }

  exportExperiment(id: string) {
    return { state: this.getState(id), records: this.getRecords(id) };
  }
}
