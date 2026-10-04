export interface LogEntry {
  term: number;
  command: string;
}

export interface AppendEntriesRequest {
  term: number;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export type ErrorCode =
  | "INVALID_TERM"
  | "INVALID_INDEX"
  | "INVALID_COMMIT"
  | "INVALID_ENTRIES"
  | "INVALID_COMMAND"
  | "ENTRY_TERM_ORDER"
  | "ENTRY_TERM_EXCEEDS_REQUEST"
  | "STALE_TERM"
  | "PREV_LOG_MISSING"
  | "PREV_LOG_TERM_MISMATCH"
  | "PROTOCOL_VIOLATION"
  | "COMMITTED_CONFLICT"
  | "NOT_FOUND"
  | "BAD_REQUEST"
  | "INTERNAL";

export class RaftError extends Error {
  constructor(public code: ErrorCode, message: string) {
    super(message);
  }
}

export interface RequestReport {
  seq: number;
  accepted: boolean;
  reason: ErrorCode | "OK";
  message: string;
  oldTerm: number;
  newTerm: number;
  matchIndex: number | null;
  firstConflictIndex: number | null;
  deletedRange: [number, number] | null;
  appendedRange: [number, number] | null;
  oldCommitIndex: number;
  newCommitIndex: number;
  newlyCommitted: Array<{ index: number; term: number; command: string }>;
  logLengthBefore: number;
  logLengthAfter: number;
}

export interface BatchReport {
  results: RequestReport[];
  firstFailureSeq: number | null;
  completed: number;
  total: number;
}
