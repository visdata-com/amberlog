// 台账屏（REQ-AMB-UI-GOVERNANCE 台账屏一组）：查 `_ledger`，每个 hit 一行；列与取值的中文对照见 REQ-AMB-UI-GOVERNANCE，另有判定汇总、
// 按时间戳上报的来源与数值格式；每行可跳到对账屏与检索屏；不提供编辑台账内容的控件（LEDGER-NO-EDIT）。

import type { ComponentChildren } from "preact";
import { useRef } from "preact/hooks";
import { intOf, rawText, type Hit, type Json } from "../../lib/json.ts";
import { DAY_US, HOUR_US } from "../../lib/time.ts";
import { setDatasets, streamOf } from "../../shell/datasets.ts";
import { entries } from "../../shell/entries.ts";
import { useResultRequest } from "../../shell/requests.ts";
import { ResultArea } from "../../shell/ResultArea.tsx";
import { Time, TimeHeader } from "../../shell/Time.tsx";
import { TimeRangeInputs, useTimeRange, type QuickChoice } from "../../shell/TimeRange.tsx";
import { Bytes, Count } from "./Numbers.tsx";
import "./ledger.css";

const SIZE = 1000;
const QUICK: QuickChoice[] = [
  { label: "最近 1 小时", us: HOUR_US },
  { label: "最近 24 小时", us: 24n * HOUR_US },
  { label: "最近 7 天", us: 7n * DAY_US },
  { label: "最近 30 天", us: 30n * DAY_US },
];

const STATUS: Record<string, string> = {
  active: "未停用",
  disabled_format_version: "已停用（格式版本不兼容）",
  disabled_manifest_unreadable: "已停用（清单不可读）",
};
const TIER: Record<string, string> = { hot: "热层", hot_pending_lake: "热层（待入湖）", cold: "冷层", hot_cold: "热层与冷层" };
const RECONCILIATION: Record<string, string> = { three_way: "三方", two_way: "两方（发送计数缺失）", unknown: "计数不可读" };
const EXPIRES_NOTE = "（最早数据的到期时刻，到期数据随后删除）";

/** 判定汇总（LEDGER-VERDICTS）：列头与 `_ledger` 的列，按条款的先后。 */
const VERDICT_COLUMNS: readonly [string, string][] = [
  ["一致", "windows_consistent"],
  ["缺失", "windows_missing"],
  ["重复", "windows_duplicate"],
  ["上报中断", "windows_silent"],
  ["待定", "windows_pending"],
  ["不可判定", "windows_undeterminable"],
];
const SUMMARY_KEYS = [...VERDICT_COLUMNS.map(([, k]) => k), "silent_sources"];

const absent = (v: Json | undefined): boolean => v === null || v === undefined;

function mapped(table: Record<string, string>, v: Json | undefined): string {
  return typeof v === "string" && Object.hasOwn(table, v) ? (table[v] as string) : rawText(v);
}

function TimeCell({ v }: { v: Json | undefined }) {
  const us = intOf(v);
  return us !== null ? <Time us={us} /> : <>{rawText(v)}</>;
}

/** 列：表头文字与该格的内容。 */
interface Column {
  label: ComponentChildren;
  cell: (g: (k: string) => Json | undefined) => ComponentChildren;
}

const COLUMNS: readonly Column[] = [
  { label: "数据集", cell: (g) => rawText(g("dataset")) },
  { label: "状态", cell: (g) => mapped(STATUS, g("status")) },
  {
    label: "来源",
    cell: (g) => (absent(g("sources")) ? (g("reconciliation") === "unknown" ? "—" : "无上报") : rawText(g("sources"))),
  },
  { label: <TimeHeader label="首条数据时间" />, cell: (g) => <TimeCell v={g("time_min")} /> },
  { label: <TimeHeader label="末条数据时间" />, cell: (g) => <TimeCell v={g("time_max")} /> },
  { label: "条数", cell: (g) => <Count v={g("record_count")} /> },
  { label: "字节数", cell: (g) => <Bytes v={g("size_bytes")} /> },
  { label: "所在层", cell: (g) => mapped(TIER, g("tier")) },
  { label: "待入湖条数", cell: (g) => <Count v={g("pending_lake_record_count")} /> },
  { label: "保留期", cell: (g) => (absent(g("retention")) ? "未设定" : rawText(g("retention"))) },
  {
    label: <TimeHeader label="到期日" />,
    cell: (g) =>
      absent(g("expires_at")) ? "未设定" : (
        <>
          <TimeCell v={g("expires_at")} />
          {EXPIRES_NOTE}
        </>
      ),
  },
  { label: "对账状态", cell: (g) => mapped(RECONCILIATION, g("reconciliation")) },
];

/** 判定汇总的七列为 NULL 时整组显示「—」。 */
const summaryCell = (g: (k: string) => Json | undefined, key: string): string =>
  SUMMARY_KEYS.every((k) => absent(g(k))) ? "—" : rawText(g(key));

function LedgerTable({ hits, onJump }: { hits: Hit[]; onJump: (dataset: string, to: "recon" | "search") => void }) {
  return (
    <table class="ledger">
      <thead>
        <tr class="group">
          {COLUMNS.map(() => (
            <th />
          ))}
          <th colSpan={VERDICT_COLUMNS.length} class="verdicts">
            各判定的时间窗数
          </th>
          <th />
          <th />
          <th />
        </tr>
        <tr>
          {COLUMNS.map((c) => (
            <th>{c.label}</th>
          ))}
          {VERDICT_COLUMNS.map(([label]) => (
            <th class="verdict">{label}</th>
          ))}
          <th>上报中断的来源数</th>
          <th>按时间戳上报的来源</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {hits.map((h) => {
          const g = (k: string): Json | undefined => h.get(k);
          const dataset = g("dataset");
          const tsSources = g("ts_sources");
          return (
            <tr>
              {COLUMNS.map((c) => (
                <td>{c.cell(g)}</td>
              ))}
              {VERDICT_COLUMNS.map(([, k]) => (
                <td class="verdict">{summaryCell(g, k)}</td>
              ))}
              <td>{summaryCell(g, "silent_sources")}</td>
              <td>{absent(tsSources) ? (g("reconciliation") === "unknown" ? "—" : "无") : rawText(tsSources)}</td>
              <td class="jumps">
                {typeof dataset === "string" && (
                  <>
                    <button type="button" onClick={() => onJump(dataset, "recon")}>对账</button>
                    <button type="button" onClick={() => onJump(dataset, "search")}>检索</button>
                  </>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function LedgerScreen({ hidden }: { hidden: boolean }) {
  const req = useResultRequest();
  const range = useTimeRange(QUICK, 24n * HOUR_US);
  /** 上一次查询实际采用的时间范围（「对账」跳转带上它，LEDGER-JUMPS）。 */
  const last = useRef<[bigint, bigint] | null>(null);
  const query = async (): Promise<void> => {
    const r = range.resolve();
    if (r === null) {
      req.invalidTime();
      return;
    }
    last.current = r;
    const o = await req.result({ sql: "SELECT * FROM _ledger", start_time: r[0], end_time: r[1], size: SIZE, from: 0 });
    if (o?.kind === "ok" && o.hits.length < SIZE) setDatasets(o.hits);
  };
  const jump = (dataset: string, to: "recon" | "search"): void => {
    if (to === "search") entries.search?.setStream(streamOf(dataset));
    else if (last.current !== null) entries.recon?.jump(dataset, last.current[0], last.current[1]);
  };
  return (
    <section id="ledger" class="screen" hidden={hidden}>
      <div class="bar">
        <TimeRangeInputs range={range} prefix="l" />
        <button type="button" id="l-query" class="primary" disabled={req.queryDisabled} onClick={() => void query()}>
          {req.running ? "查询中" : "查询"}
        </button>
      </div>
      <ResultArea req={req} prefix="l" render={(hits) => <LedgerTable hits={hits} onJump={jump} />} />
    </section>
  );
}
