/**
 * Raft 跟随者 AppendEntries 前缀核对与冲突截断 —— 纯逻辑实现。
 *
 * 仅模拟这一条日志复制路径：不做选举、节点通信、心跳定时器、快照安装，
 * 也不执行命令字符串。日志索引 0 是任期 0 的固定哨兵。
 */

export interface LogEntry {
  /** 日志索引，哨兵为 0，正常条目从 1 开始。 */
  index: number;
  /** 该条目所属任期。 */
  term: number;
  /** 简单字符串命令；模型不执行它。 */
  command: string;
}

export interface AppendEntriesRequest {
  /** 领导者的任期。 */
  term: number;
  /** 新条目紧邻的前驱索引。 */
  prevLogIndex: number;
  /** 前驱索引处条目的任期。 */
  prevLogTerm: number;
  /** 连续的新条目（隐含 prevLogIndex+1 起的连续索引）。 */
  entries: Array<{ term: number; command: string }>;
  /** 领导者已提交的位置。 */
  leaderCommit: number;
}

export interface FollowerState {
  currentTerm: number;
  /** 完整日志，含索引 0 的固定哨兵。 */
  log: LogEntry[];
  /** 已提交位置（最后已知已提交条目的索引；0 表示尚无提交）。 */
  commitIndex: number;
}

export type ProcessOutcome =
  | "accepted"
  | "rejected"
  | "invalid_input"
  | "committed_conflict";

export interface ProcessResult {
  outcome: ProcessOutcome;
  success: boolean;
  /** 拒绝/错误原因码。 */
  reason?: string;
  /** 人读说明。 */
  message: string;
  termBefore: number;
  termAfter: number;
  /** 任期是否被本请求更新（较新任期持久化，即使随后前缀失败）。 */
  termUpdated: boolean;
  /** 前驱检查通过后，与请求条目逐项比较得到的匹配末位。 */
  matchedIndex: number;
  /** 首个冲突索引；无冲突为 null。 */
  firstConflictIndex: number | null;
  /** 被删除的本地后缀范围（含端点）。 */
  deletedRange: { from: number; to: number } | null;
  /** 实际追加到本地的索引范围（含端点）。 */
  appendedRange: { from: number; to: number } | null;
  /** 成功时已确认到的前缀位置 = prevLogIndex + entries.length。 */
  acknowledgedIndex: number | null;
  commitBefore: number;
  commitAfter: number;
  /** 本次新变为已提交的条目索引范围（含端点）。 */
  newlyCommittedRange: { from: number; to: number } | null;
  /** 处理后的完整状态（拒绝时与处理前一致，仅可能更新任期）。 */
  state: FollowerState;
}

export class ValidationError extends Error {
  code = "INVALID_ARGUMENT";
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

export function isNonNegSafeInt(v: unknown): v is number {
  return typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
}

function assertNonNeg(v: unknown, field: string): number {
  if (!isNonNegSafeInt(v)) {
    throw new ValidationError(`字段 ${field} 必须是非负安全整数`);
  }
  return v;
}

/** 校验并规范化一次 AppendEntries 请求（结构层面的合法性）。 */
export function validateRequest(raw: unknown): AppendEntriesRequest {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new ValidationError("请求体必须是对象");
  }
  const r = raw as Record<string, unknown>;
  const term = assertNonNeg(r.term, "term");
  const prevLogIndex = assertNonNeg(r.prevLogIndex, "prevLogIndex");
  const prevLogTerm = assertNonNeg(r.prevLogTerm, "prevLogTerm");
  const leaderCommit = assertNonNeg(r.leaderCommit, "leaderCommit");
  if (!Array.isArray(r.entries)) {
    throw new ValidationError("字段 entries 必须是数组");
  }
  const entries = r.entries.map((e, i) => {
    if (typeof e !== "object" || e === null || Array.isArray(e)) {
      throw new ValidationError(`entries[${i}] 必须是对象`);
    }
    const entry = e as Record<string, unknown>;
    const entryTerm = assertNonNeg(entry.term, `entries[${i}].term`);
    if (typeof entry.command !== "string") {
      throw new ValidationError(`entries[${i}].command 必须是字符串`);
    }
    return { term: entryTerm, command: entry.command };
  });
  // 内容层面：任期序列符合日志顺序（非递减）且不超过请求任期。
  let prevTerm = prevLogTerm;
  for (let i = 0; i < entries.length; i++) {
    if (entries[i].term < prevTerm) {
      throw new ValidationError(
        `entries[${i}].term=${entries[i].term} 小于前驱任期 ${prevTerm}，任期序列必须非递减`,
      );
    }
    if (entries[i].term > term) {
      throw new ValidationError(
        `entries[${i}].term=${entries[i].term} 超过请求任期 term=${term}`,
      );
    }
    prevTerm = entries[i].term;
  }
  return { term, prevLogIndex, prevLogTerm, entries, leaderCommit };
}

export function initialState(): FollowerState {
  return {
    currentTerm: 0,
    log: [{ index: 0, term: 0, command: "" }],
    commitIndex: 0,
  };
}

/** 校验要导入的初始日志（不含哨兵）：索引连续、任期非递减。 */
export function validateImportedLog(
  entries: Array<{ term: number; command: string }>,
): LogEntry[] {
  if (!Array.isArray(entries)) {
    throw new ValidationError("导入条目必须是数组");
  }
  let lastTerm = 0;
  return entries.map((e, i) => {
    if (typeof e !== "object" || e === null) {
      throw new ValidationError(`entries[${i}] 必须是对象`);
    }
    const term = assertNonNeg(e.term, `entries[${i}].term`);
    if (typeof e.command !== "string") {
      throw new ValidationError(`entries[${i}].command 必须是字符串`);
    }
    if (term < lastTerm) {
      throw new ValidationError(
        `entries[${i}].term=${term} 小于前一条任期 ${lastTerm}，任期必须非递减`,
      );
    }
    lastTerm = term;
    return { index: i + 1, term, command: e.command };
  });
}

interface FailureInput {
  outcome: ProcessOutcome;
  reason: string;
  message: string;
  base: FollowerState;
  termAfter: number;
  termUpdated: boolean;
}

function failure(f: FailureInput): ProcessResult {
  return {
    outcome: f.outcome,
    success: false,
    reason: f.reason,
    message: f.message,
    termBefore: f.base.currentTerm,
    termAfter: f.termAfter,
    termUpdated: f.termUpdated,
    matchedIndex: 0,
    firstConflictIndex: null,
    deletedRange: null,
    appendedRange: null,
    acknowledgedIndex: null,
    commitBefore: f.base.commitIndex,
    commitAfter: f.base.commitIndex,
    newlyCommittedRange: null,
    state: {
      currentTerm: f.termAfter,
      // 失败绝不改变日志与提交位置（仅可能更新任期）。
      log: f.base.log,
      commitIndex: f.base.commitIndex,
    },
  };
}

/**
 * 处理一次 AppendEntries 请求。纯函数：不触碰数据库与 IO。
 * 调用方负责在同一事务里持久化返回的状态与记录。
 */
export function processAppendEntries(
  base: FollowerState,
  rawRequest: unknown,
): ProcessResult {
  const req = validateRequest(rawRequest);

  // 1. 请求任期较旧：直接拒绝，什么都不改。
  if (req.term < base.currentTerm) {
    return failure({
      outcome: "rejected",
      reason: "TERM_STALE",
      message: `请求任期 ${req.term} 小于当前任期 ${base.currentTerm}`,
      base,
      termAfter: base.currentTerm,
      termUpdated: false,
    });
  }

  // 2. 较新任期先持久化（即使随后前缀不匹配也保留新任期）。
  const termUpdated = req.term > base.currentTerm;
  const work: FollowerState = {
    currentTerm: req.term,
    log: base.log,
    commitIndex: base.commitIndex,
  };

  // 3. 前驱必须存在且任期一致。
  const prev = work.log.find((e) => e.index === req.prevLogIndex);
  if (!prev) {
    return failure({
      outcome: "rejected",
      reason: "PREV_LOG_MISSING",
      message: `前驱索引 ${req.prevLogIndex} 在本地日志中不存在（本地末位 ${
        work.log[work.log.length - 1].index
      }）`,
      base,
      termAfter: work.currentTerm,
      termUpdated,
    });
  }
  if (prev.term !== req.prevLogTerm) {
    return failure({
      outcome: "rejected",
      reason: "PREV_LOG_TERM_MISMATCH",
      message: `前驱索引 ${req.prevLogIndex} 本地任期 ${prev.term} 与请求任期 ${req.prevLogTerm} 不一致`,
      base,
      termAfter: work.currentTerm,
      termUpdated,
    });
  }

  // 4. 逐条比较已有日志。
  const logByIndex = new Map<number, LogEntry>();
  for (const e of work.log) logByIndex.set(e.index, e);

  let matchedIndex = req.prevLogIndex;
  let firstConflictIndex: number | null = null;

  for (let i = 0; i < req.entries.length; i++) {
    const incoming = req.entries[i];
    const index = req.prevLogIndex + 1 + i;
    const existing = logByIndex.get(index);
    if (!existing) {
      // 本地缺失：进入追加区间，前面的都算匹配。
      break;
    }
    if (existing.term === incoming.term) {
      if (existing.command !== incoming.command) {
        // 相同索引、相同任期但命令不同：违反协议假设，不当作正常重复。
        return failure({
          outcome: "invalid_input",
          reason: "SAME_TERM_COMMAND_CONFLICT",
          message: `索引 ${index} 本地与请求任期同为 ${incoming.term} 但命令不同（"${existing.command}" vs "${incoming.command}"），违反 Raft 协议假设`,
          base,
          termAfter: work.currentTerm,
          termUpdated,
        });
      }
      matchedIndex = index; // 幂等重复，复用本地条目。
    } else {
      firstConflictIndex = index;
      break;
    }
  }

  // 5. 冲突截断。
  let deletedRange: { from: number; to: number } | null = null;
  let finalLog = work.log;
  if (firstConflictIndex !== null) {
    // 绝不能截断已经提交的条目。
    if (firstConflictIndex <= work.commitIndex) {
      return failure({
        outcome: "committed_conflict",
        reason: "COMMITTED_ENTRY_CONFLICT",
        message: `首个冲突索引 ${firstConflictIndex} 不大于已提交位置 ${work.commitIndex}，不能截断已提交条目`,
        base,
        termAfter: work.currentTerm,
        termUpdated,
      });
    }
    deletedRange = {
      from: firstConflictIndex,
      to: work.log[work.log.length - 1].index,
    };
    finalLog = work.log.filter((e) => e.index < firstConflictIndex);
  }

  // 6. 追加剩余新条目（本地缺失的，以及触发截断的冲突条目本身起）。
  const lastLocalIndex = finalLog[finalLog.length - 1].index;
  const appendStartInclusive =
    firstConflictIndex !== null
      ? firstConflictIndex
      : Math.max(req.prevLogIndex + 1, lastLocalIndex + 1);
  let appendedRange: { from: number; to: number } | null = null;
  if (appendStartInclusive <= req.prevLogIndex + req.entries.length) {
    appendedRange = {
      from: appendStartInclusive,
      to: req.prevLogIndex + req.entries.length,
    };
    const additions: LogEntry[] = [];
    for (let i = 0; i < req.entries.length; i++) {
      const index = req.prevLogIndex + 1 + i;
      if (index >= appendStartInclusive) {
        additions.push({ index, term: req.entries[i].term, command: req.entries[i].command });
      }
    }
    finalLog = [...finalLog, ...additions];
  }

  // 7. 提交位置：最多推进到 min(leaderCommit, 本次确认前缀)，且不回退。
  const acknowledgedIndex = req.prevLogIndex + req.entries.length;
  const commitBefore = work.commitIndex;
  const commitAfter = Math.max(
    commitBefore,
    Math.min(req.leaderCommit, acknowledgedIndex),
  );
  const newlyCommittedRange =
    commitAfter > commitBefore
      ? { from: commitBefore + 1, to: commitAfter }
      : null;

  return {
    outcome: "accepted",
    success: true,
    message:
      appendedRange === null
        ? `请求与日志前缀一致，无新增条目；确认到 ${acknowledgedIndex}`
        : `已删除 ${
            deletedRange ? `[${deletedRange.from},${deletedRange.to}]` : "无"
          } 并追加 [${appendedRange.from},${appendedRange.to}]，确认到 ${acknowledgedIndex}`,
    termBefore: base.currentTerm,
    termAfter: work.currentTerm,
    termUpdated,
    matchedIndex,
    firstConflictIndex,
    deletedRange,
    appendedRange,
    acknowledgedIndex,
    commitBefore,
    commitAfter,
    newlyCommittedRange,
    state: {
      currentTerm: work.currentTerm,
      log: finalLog,
      commitIndex: commitAfter,
    },
  };
}
