/**
 * SQLite 持久化层。每个 follower 实验一行状态 + 日志条目表 + 处理记录表。
 * 每次请求的状态变化与记录在同一事务提交。
 */
import { DatabaseSync, type StatementSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  FollowerState,
  LogEntry,
  ProcessResult,
  initialState,
  validateImportedLog,
} from "./core";

export interface FollowerSummary {
  id: number;
  name: string;
  currentTerm: number;
  commitIndex: number;
  logLength: number;
  createdAt: string;
}

export interface RequestRecord {
  seq: number;
  createdAt: string;
  request: string;
  result: string;
}

export class Store {
  private db: DatabaseSync;
  private crashBeforeCommit = false;

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.migrate();
  }

  close(): void {
    this.db.close();
  }

  /** 测试钩子：在提交前注入一次写入失败，验证事务回滚。 */
  injectCrashBeforeCommit(): void {
    this.crashBeforeCommit = true;
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS followers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        current_term INTEGER NOT NULL,
        commit_index INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS log_entries (
        follower_id INTEGER NOT NULL REFERENCES followers(id) ON DELETE CASCADE,
        idx INTEGER NOT NULL,
        term INTEGER NOT NULL,
        command TEXT NOT NULL,
        PRIMARY KEY (follower_id, idx)
      );
      CREATE TABLE IF NOT EXISTS records (
        follower_id INTEGER NOT NULL REFERENCES followers(id) ON DELETE CASCADE,
        seq INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        request_json TEXT NOT NULL,
        result_json TEXT NOT NULL,
        PRIMARY KEY (follower_id, seq)
      );
    `);
  }

  createFollower(
    name: string,
    imported: Array<{ term: number; command: string }> = [],
  ): number {
    if (typeof name !== "string" || name.trim() === "") {
      throw Object.assign(new Error("name 必须是非空字符串"), {
        code: "INVALID_ARGUMENT",
      });
    }
    const entries = validateImportedLog(imported);
    try {
      const info = this.db
        .prepare("INSERT INTO followers (name, current_term, commit_index) VALUES (?, 0, 0)")
        .run(name);
      const id = Number(info.lastInsertRowid);
      const ins = this.db.prepare(
        "INSERT INTO log_entries (follower_id, idx, term, command) VALUES (?, ?, ?, ?)",
      );
      // 哨兵 + 导入条目。
      ins.run(id, 0, 0, "");
      for (const e of entries) ins.run(id, e.index, e.term, e.command);
      return id;
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "SQLITE_CONSTRAINT_UNIQUE") {
        throw Object.assign(new Error(`名为 "${name}" 的实验已存在`), {
          code: "NAME_CONFLICT",
        });
      }
      throw err;
    }
  }

  listFollowers(): FollowerSummary[] {
    return this.db
      .prepare(
        `SELECT f.id, f.name, f.current_term AS currentTerm,
                f.commit_index AS commitIndex, f.created_at AS createdAt,
                (SELECT COUNT(*) FROM log_entries l WHERE l.follower_id = f.id) AS logLength
         FROM followers f ORDER BY f.id`,
      )
      .all() as unknown as FollowerSummary[];
  }

  private requireFollower(id: number): void {
    const row = this.db
      .prepare("SELECT id FROM followers WHERE id = ?")
      .get(id) as { id: number } | undefined;
    if (!row) {
      throw Object.assign(new Error(`follower ${id} 不存在`), {
        code: "NOT_FOUND",
      });
    }
  }

  getState(id: number): FollowerState {
    this.requireFollower(id);
    const meta = this.db
      .prepare("SELECT current_term AS t, commit_index AS c FROM followers WHERE id = ?")
      .get(id) as { t: number; c: number };
    const entries = this.db
      .prepare("SELECT idx, term, command FROM log_entries WHERE follower_id = ? ORDER BY idx")
      .all(id) as unknown as Array<{ idx: number; term: number; command: string }>;
    return {
      currentTerm: meta.t,
      commitIndex: meta.c,
      log: entries.map((e) => ({
        index: e.idx,
        term: e.term,
        command: e.command,
      })) as LogEntry[],
    };
  }

  /**
   * 在一个事务里完成：读取状态 → 应用结果 → 写日志/任期/提交 → 追加处理记录。
   * apply 接收当前状态，返回 { result }；拒绝同样在此记录。
   */
  applyWithRecord(
    id: number,
    requestJson: string,
    apply: (state: FollowerState) => ProcessResult,
  ): ProcessResult {
    this.requireFollower(id);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const state = this.getState(id);
      const result = apply(state);
      this.persistState(id, result.state);
      const seqRow = this.db
        .prepare("SELECT COALESCE(MAX(seq), 0) + 1 AS nextSeq FROM records WHERE follower_id = ?")
        .get(id) as { nextSeq: number };
      this.db
        .prepare(
          "INSERT INTO records (follower_id, seq, request_json, result_json) VALUES (?, ?, ?, ?)",
        )
        .run(id, seqRow.nextSeq, requestJson, JSON.stringify(toStorableResult(result)));
      if (this.crashBeforeCommit) {
        this.crashBeforeCommit = false;
        throw new Error("INJECTED_WRITE_FAILURE");
      }
      this.db.exec("COMMIT");
      return result;
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  private updateFollowerStmt?: StatementSync;
  private deleteSuffixStmt?: StatementSync;
  private insertEntryStmt?: StatementSync;

  private persistState(id: number, next: FollowerState): void {
    this.updateFollowerStmt ??= this.db.prepare(
      "UPDATE followers SET current_term = ?, commit_index = ? WHERE id = ?",
    );
    this.updateFollowerStmt.run(next.currentTerm, next.commitIndex, id);

    // 用最终日志集合对账：删除本地多出的后缀，插入/更新缺失项。
    const wanted = new Map<number, LogEntry>();
    for (const e of next.log) wanted.set(e.index, e);
    const existing = this.db
      .prepare("SELECT idx FROM log_entries WHERE follower_id = ?")
      .all(id) as unknown as Array<{ idx: number }>;
    this.deleteSuffixStmt ??= this.db.prepare(
      "DELETE FROM log_entries WHERE follower_id = ? AND idx = ?",
    );
    for (const row of existing) {
      if (!wanted.has(row.idx)) this.deleteSuffixStmt.run(id, row.idx);
    }
    this.insertEntryStmt ??= this.db.prepare(
      `INSERT INTO log_entries (follower_id, idx, term, command) VALUES (?, ?, ?, ?)
       ON CONFLICT(follower_id, idx) DO UPDATE SET term = excluded.term, command = excluded.command`,
    );
    for (const e of wanted.values()) {
      this.insertEntryStmt.run(id, e.index, e.term, e.command);
    }
  }

  getRecords(id: number): RequestRecord[] {
    this.requireFollower(id);
    return this.db
      .prepare(
        `SELECT seq, created_at AS createdAt, request_json AS request, result_json AS result
         FROM records WHERE follower_id = ? ORDER BY seq`,
      )
      .all(id) as unknown as RequestRecord[];
  }

  /** 导出整个实验：状态、日志、全部处理记录。 */
  export(id: number): {
    id: number;
    name: string;
    state: FollowerState;
    records: RequestRecord[];
  } {
    this.requireFollower(id);
    const meta = this.db
      .prepare("SELECT name FROM followers WHERE id = ?")
      .get(id) as { name: string };
    return {
      id,
      name: meta.name,
      state: this.getState(id),
      records: this.getRecords(id),
    };
  }
}

/** 去掉 result.state（与日志表重复），记录保持精简。 */
function toStorableResult(result: ProcessResult): Omit<ProcessResult, "state"> {
  const { state: _state, ...rest } = result;
  return rest;
}

export function freshState(): FollowerState {
  return initialState();
}
