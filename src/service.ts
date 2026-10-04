/** 应用服务层：把纯逻辑接到存储事务上，并提供批量边界。 */
import {
  FollowerState,
  ProcessResult,
  ValidationError,
  processAppendEntries,
} from "./core";
import { Store } from "./store";

export interface BatchItemResult {
  index: number;
  result: ProcessResult;
}

export interface BatchOutcome {
  applied: number;
  /** 首个失败的零基位置；全部成功为 null。 */
  firstFailureIndex: number | null;
  items: BatchItemResult[];
}

export class RaftService {
  constructor(private store: Store) {}

  create(name: string, entries: Array<{ term: number; command: string }>): number {
    return this.store.createFollower(name, entries);
  }

  list() {
    return this.store.listFollowers();
  }

  state(id: number): FollowerState {
    return this.store.getState(id);
  }

  records(id: number) {
    return this.store.getRecords(id);
  }

  export(id: number) {
    return this.store.export(id);
  }

  /** 单条请求：拒绝也会落一条处理记录（可能伴随合法的新任期更新）。 */
  append(id: number, request: unknown): ProcessResult {
    const requestJson = JSON.stringify(request ?? null);
    return this.store.applyWithRecord(id, requestJson, (state) =>
      processAppendEntries(state, request),
    );
  }

  /**
   * 有序一组请求：每条各自一个事务（明确的逐请求边界）。
   * 遇到第一个失败（拒绝或校验错误）即停止，返回其零基位置。
   */
  appendBatch(id: number, requests: unknown[]): BatchOutcome {
    if (!Array.isArray(requests)) {
      throw new ValidationError("batch 请求体必须是数组");
    }
    const items: BatchItemResult[] = [];
    for (let i = 0; i < requests.length; i++) {
      const result = this.append(id, requests[i]);
      items.push({ index: i, result });
      if (!result.success) {
        return { applied: i, firstFailureIndex: i, items };
      }
    }
    return { applied: requests.length, firstFailureIndex: null, items };
  }

  /** 测试钩子：注入写入失败验证回滚。 */
  injectCrash(): void {
    this.store.injectCrashBeforeCommit();
  }
}
