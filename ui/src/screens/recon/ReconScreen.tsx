// 对账屏（REQ-AMB-UI-GOVERNANCE 对账屏一组）：查某数据集的 `_reconciliation`，每个 hit 一行（不补没有计数的分钟）；
// 三个计数、两个差值、判定所比的两个数与判定列，「只看非一致」，常显说明文字。不设时间范围上限、不翻页。

import { useEffect, useRef, useState } from "preact/hooks";
import { intOf, rawText, type Hit, type Json } from "../../lib/json.ts";
import { useStore } from "../../lib/store.ts";
import { DAY_US, HOUR_US, MINUTE_US } from "../../lib/time.ts";
import { datasets } from "../../shell/datasets.ts";
import { entries, show } from "../../shell/entries.ts";
import { useResultRequest } from "../../shell/requests.ts";
import { ResultArea } from "../../shell/ResultArea.tsx";
import { Time, TimeHeader } from "../../shell/Time.tsx";
import { TimeRangeInputs, useTimeRange, type QuickChoice } from "../../shell/TimeRange.tsx";
import { Count } from "../ledger/Numbers.tsx";
import "./recon.css";

const SIZE = 10000;
const QUICK: QuickChoice[] = [
  { label: "最近 15 分钟", us: 15n * MINUTE_US },
  { label: "最近 1 小时", us: HOUR_US },
  { label: "最近 6 小时", us: 6n * HOUR_US },
  { label: "最近 24 小时", us: 24n * HOUR_US },
  { label: "最近 7 天", us: 7n * DAY_US },
];
const NOTE =
  "本地持久化确认数在记录写入本机预写日志并确认时计入，不反映此后的丢失（旧的本地目录挂回时被丢弃的记录、节点磁盘损坏时尚未提交的记录）。" +
  "判定按宽限期计算：时间窗结束后要等宽限期过去才下结论，在此之前、或来源可能仍在补发时显示「待定」，结论以后可能改变。";

/** 判定的文字（VERDICT-LABELS）；其他取值按原始 JSON 文本显示。 */
const VERDICT: Record<string, string> = {
  pending: "待定",
  consistent: "一致",
  missing: "缺失",
  duplicate: "重复",
  silent: "上报中断",
  undeterminable: "不可判定",
};

const sqlString = (s: string): string => "'" + s.replace(/'/g, "''") + "'";

/** 对账请求的 SQL（RECON-QUERY、RECON-ONLY-NONCONSISTENT）。 */
export function reconSql(dataset: string, onlyNonConsistent: boolean): string {
  const sql = "SELECT * FROM _reconciliation WHERE dataset = " + sqlString(dataset);
  return onlyNonConsistent ? sql + " AND verdict != 'consistent'" : sql;
}

const verdictText = (v: Json | undefined): string =>
  typeof v === "string" && Object.hasOwn(VERDICT, v) ? (VERDICT[v] as string) : rawText(v);

/** NULL 时显示 `absent`，否则按千分位。 */
function CountOr({ v, absent }: { v: Json | undefined; absent: string }) {
  return v === null || v === undefined ? <>{absent}</> : <Count v={v} />;
}

function ReconTable({ hits }: { hits: Hit[] }) {
  return (
    <table class="recon">
      <thead>
        <tr>
          <th><TimeHeader label="窗口起点" /></th>
          <th>发送数</th>
          <th>发送来源</th>
          <th>接收数</th>
          <th>本地持久化确认数</th>
          <th>「采集器 → 接收」差值</th>
          <th>「接收 → 本地持久化」差值</th>
          <th>按时间戳的发送数</th>
          <th>按时间戳的确认数</th>
          <th>判定</th>
        </tr>
      </thead>
      <tbody>
        {hits.map((h) => {
          const g = (k: string): Json | undefined => h.get(k);
          const ws = intOf(g("window_start"));
          const sources = g("sent_sources");
          return (
            <tr>
              <td>{ws !== null ? <Time us={ws} /> : rawText(g("window_start"))}</td>
              <td class="num"><CountOr v={g("sent")} absent="未上报" /></td>
              <td>{sources === null || sources === undefined ? "—" : rawText(sources)}</td>
              <td class="num"><Count v={g("received")} /></td>
              <td class="num"><Count v={g("durable")} /></td>
              <td class="num"><CountOr v={g("gap_sent_received")} absent="—" /></td>
              <td class="num"><Count v={g("gap_received_durable")} /></td>
              <td class="num"><CountOr v={g("ts_sent")} absent="—" /></td>
              <td class="num"><CountOr v={g("ts_durable")} absent="—" /></td>
              <td>{verdictText(g("verdict"))}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function ReconScreen({ hidden }: { hidden: boolean }) {
  const req = useResultRequest();
  const range = useTimeRange(QUICK, HOUR_US);
  const { list } = useStore(datasets);
  const [dataset, setDataset] = useState("");
  const [onlyNonConsistent, setOnlyNonConsistent] = useState(false);
  const query = (d: string): void => {
    const r = range.resolve();
    if (r === null) {
      req.invalidTime();
      return;
    }
    void req.result({ sql: reconSql(d, onlyNonConsistent), start_time: r[0], end_time: r[1], size: SIZE, from: 0 });
  };
  // 入口在挂载时注册一次，经它取此刻的 query（「只看非一致」的勾选随之）
  const latest = useRef(query);
  latest.current = query;
  useEffect(() => {
    entries.recon = {
      jump(d, start, end) {
        setDataset(d);
        range.setRange(start, end);
        show("recon");
        latest.current(d);
      },
    };
    return () => {
      entries.recon = null;
    };
  }, []);
  return (
    <section id="recon" class="screen" hidden={hidden}>
      <p class="notice" id="r-notice">{NOTE}</p>
      <div class="bar">
        <input
          id="r-dataset"
          list="r-datasets"
          autocomplete="off"
          aria-label="数据集"
          value={dataset}
          onInput={(e) => setDataset(e.currentTarget.value)}
        />
        <datalist id="r-datasets">
          {list.map((d) => (
            <option value={d} />
          ))}
        </datalist>
        <TimeRangeInputs range={range} prefix="r" />
        <label class="field">
          <input
            type="checkbox"
            id="r-only-nonconsistent"
            checked={onlyNonConsistent}
            onChange={(e) => setOnlyNonConsistent(e.currentTarget.checked)}
          />
          只看非一致
        </label>
        <button type="button" id="r-query" class="primary" disabled={req.queryDisabled} onClick={() => query(dataset)}>
          {req.running ? "查询中" : "查询"}
        </button>
      </div>
      <ResultArea req={req} prefix="r" render={(hits) => <ReconTable hits={hits} />} />
    </section>
  );
}
