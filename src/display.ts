/** 终端表格与状态渲染（不依赖第三方库）。 */
import { FollowerState, ProcessResult } from "./core";

function pad(value: string, width: number): string {
  // 中文按 2 个宽字符估算。
  let visual = 0;
  for (const ch of value) visual += ch.charCodeAt(0) > 255 ? 2 : 1;
  return value + " ".repeat(Math.max(0, width - visual));
}

function table(headers: string[], rows: string[][]): string {
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => visualWidth(r[i] ?? ""))),
  );
  const line = (cells: string[]) =>
    "| " + cells.map((c, i) => pad(c, widths[i])).join(" | ") + " |";
  const sep =
    "+" + widths.map((w) => "-".repeat(w + 2)).join("+") + "+";
  return [sep, line(headers), sep, ...rows.map(line), sep].join("\n");
}

function visualWidth(value: string): number {
  let width = 0;
  for (const ch of value) width += ch.charCodeAt(0) > 255 ? 2 : 1;
  return width;
}

function rangeText(r: { from: number; to: number } | null): string {
  return r === null ? "—" : r.from === r.to ? `[${r.from}]` : `[${r.from}..${r.to}]`;
}

export function renderLog(state: FollowerState): string {
  const rows = state.log.map((e) => [
    String(e.index),
    String(e.term),
    e.index === 0 ? "<哨兵>" : JSON.stringify(e.command),
    e.index <= state.commitIndex && e.index > 0 ? "已提交" : e.index === 0 ? "—" : "已接收",
  ]);
  return table(["索引", "任期", "命令", "状态"], rows);
}

export function renderSummary(state: FollowerState): string {
  return `当前任期=${state.currentTerm}  提交位置=${state.commitIndex}  日志末位=${
    state.log[state.log.length - 1].index
  }`;
}

export function renderRequest(
  seq: number,
  request: unknown,
  before: FollowerState,
  result: ProcessResult,
): string {
  const banner = `── 请求 #${seq} ──────────────────────────────────`;
  const reqText = `term=${(request as any).term} prevLogIndex=${
    (request as any).prevLogIndex
  } prevLogTerm=${(request as any).prevLogTerm} leaderCommit=${
    (request as any).leaderCommit
  } entries=${JSON.stringify((request as any).entries)}`;
  const outcomeText =
    result.outcome === "accepted"
      ? "接受"
      : result.outcome === "rejected"
        ? "拒绝"
        : result.outcome === "invalid_input"
          ? "输入违反协议假设"
          : "已提交位置冲突";
  const rows: string[][] = [
    ["结果", outcomeText],
    ["说明", result.message],
    ...(result.reason ? [["原因码", result.reason]] : []),
    ["任期", `${result.termBefore} → ${result.termAfter}${result.termUpdated ? "（已更新）" : ""}`],
    ["匹配位置", String(result.matchedIndex)],
    ["首个冲突", result.firstConflictIndex === null ? "—" : String(result.firstConflictIndex)],
    ["删除范围", rangeText(result.deletedRange)],
    ["追加范围", rangeText(result.appendedRange)],
    ["确认前缀", result.acknowledgedIndex === null ? "—" : String(result.acknowledgedIndex)],
    ["提交位置", `${result.commitBefore} → ${result.commitAfter}`],
    ["新增提交", rangeText(result.newlyCommittedRange)],
  ];
  return [
    banner,
    `请求：${reqText}`,
    `处理前：${renderSummary(before)}`,
    table(["项目", "值"], rows),
    `处理后：${renderSummary(result.state)}`,
    renderLog(result.state),
  ].join("\n");
}
