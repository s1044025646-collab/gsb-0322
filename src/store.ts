import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";
import type { LogEntry } from "./types.js";

export interface StoredEntry extends LogEntry {
  idx: number;
}

export interface ExperimentRow {
  id: string;
  currentTerm: number;
  commitIndex: number;
  createdAt: string;
}

export class Store {
  private db: DatabaseSync;

  constructor(dbPath: string) {
    if (dbPath !== ":memory:") {
      fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    }
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS experiments (
        id TEXT PRIMARY KEY,
        current_term INTEGER NOT NULL,
        commit_index INTEGER NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS entries (
        exp_id TEXT NOT NULL,
        idx INTEGER NOT NULL,
        term INTEGER NOT NULL,
        command TEXT NOT NULL,
        PRIMARY KEY (exp_id, idx)
      );
      CREATE TABLE IF NOT EXISTS requests (
        exp_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        request_json TEXT NOT NULL,
        report_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (exp_id, seq)
      );
    `);
  }

  close(): void {
    this.db.close();
  }

  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const out = fn();
      this.db.exec("COMMIT");
      return out;
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  createExperiment(id: string, currentTerm: number, entries: StoredEntry[]): void {
    this.transaction(() => {
      this.db
        .prepare(
          "INSERT INTO experiments (id, current_term, commit_index, created_at) VALUES (?, ?, 0, ?)"
        )
        .run(id, currentTerm, new Date().toISOString());
      const stmt = this.db.prepare(
        "INSERT INTO entries (exp_id, idx, term, command) VALUES (?, ?, ?, ?)"
      );
      for (const e of entries) {
        stmt.run(id, e.idx, e.term, e.command);
      }
    });
  }

  getExperiment(id: string): ExperimentRow | null {
    const row = this.db
      .prepare("SELECT id, current_term, commit_index, created_at FROM experiments WHERE id = ?")
      .get(id) as
      | { id: string; current_term: number; commit_index: number; created_at: string }
      | undefined;
    if (!row) return null;
    return {
      id: row.id,
      currentTerm: row.current_term,
      commitIndex: row.commit_index,
      createdAt: row.created_at,
    };
  }

  listExperiments(): ExperimentRow[] {
    const rows = this.db
      .prepare("SELECT id, current_term, commit_index, created_at FROM experiments ORDER BY created_at")
      .all() as unknown as Array<{
      id: string;
      current_term: number;
      commit_index: number;
      created_at: string;
    }>;
    return rows.map((r) => ({
      id: r.id,
      currentTerm: r.current_term,
      commitIndex: r.commit_index,
      createdAt: r.created_at,
    }));
  }

  getEntries(id: string): StoredEntry[] {
    const rows = this.db
      .prepare("SELECT idx, term, command FROM entries WHERE exp_id = ? ORDER BY idx")
      .all(id) as unknown as Array<{ idx: number; term: number; command: string }>;
    return rows.map((r) => ({ idx: r.idx, term: r.term, command: r.command }));
  }

  setTerm(id: string, term: number): void {
    this.db.prepare("UPDATE experiments SET current_term = ? WHERE id = ?").run(term, id);
  }

  setCommitIndex(id: string, commitIndex: number): void {
    this.db.prepare("UPDATE experiments SET commit_index = ? WHERE id = ?").run(commitIndex, id);
  }

  deleteEntriesFrom(id: string, fromIdx: number): void {
    this.db.prepare("DELETE FROM entries WHERE exp_id = ? AND idx >= ?").run(id, fromIdx);
  }

  insertEntry(id: string, entry: StoredEntry): void {
    this.db
      .prepare("INSERT INTO entries (exp_id, idx, term, command) VALUES (?, ?, ?, ?)")
      .run(id, entry.idx, entry.term, entry.command);
  }

  nextSeq(id: string): number {
    const row = this.db
      .prepare("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM requests WHERE exp_id = ?")
      .get(id) as unknown as { seq: number };
    return row.seq;
  }

  insertRequest(id: string, seq: number, requestJson: string, reportJson: string): void {
    this.db
      .prepare(
        "INSERT INTO requests (exp_id, seq, request_json, report_json, created_at) VALUES (?, ?, ?, ?, ?)"
      )
      .run(id, seq, requestJson, reportJson, new Date().toISOString());
  }

  getRequests(id: string): Array<{ seq: number; request: unknown; report: unknown; createdAt: string }> {
    const rows = this.db
      .prepare("SELECT seq, request_json, report_json, created_at FROM requests WHERE exp_id = ? ORDER BY seq")
      .all(id) as unknown as Array<{
      seq: number;
      request_json: string;
      report_json: string;
      created_at: string;
    }>;
    return rows.map((r) => ({
      seq: r.seq,
      request: JSON.parse(r.request_json),
      report: JSON.parse(r.report_json),
      createdAt: r.created_at,
    }));
  }
}
