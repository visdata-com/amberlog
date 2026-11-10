// 检索屏（REQ-AMB-UI-SEARCH-V2）：数据集与时间范围、条件框（条件与 SQL 两种模式）、发起检索（检索请求加分布请求或计数请求）、
// 分布图、总条数与结果信息、结果表与翻页、字段栏与取值提示。单条详情另在 detail/，查询历史另在 history/。

import { useEffect, useRef, useState } from "preact/hooks";
import { Num, intOf, rawText, toJsonText, type Hit, type Json } from "../../lib/json.ts";
import { useStore } from "../../lib/store.ts";
import { DAY_US, HOUR_US, MINUTE_US, nowUs } from "../../lib/time.ts";
import { type FieldsOutcome, postSearch, type Query } from "../../shell/api.ts";
import { datasets, setDatasets, streamOf } from "../../shell/datasets.ts";
import { entries, show } from "../../shell/entries.ts";
import { errorLines } from "../../shell/errors.ts";
import { shownView, useResultRequest, type ResultRequest } from "../../shell/requests.ts";
import { ResultArea } from "../../shell/ResultArea.tsx";
import { onTokenConfirmed, settings } from "../../shell/settings.ts";
import { Time } from "../../shell/Time.tsx";
import { TimeRangeInputs, useTimeRange, type QuickChoice } from "../../shell/TimeRange.tsx";
import { type Bar, bars, countsByStart, pageRows, total } from "./chart.ts";
import { type Chip, CondBox } from "./CondBox.tsx";
import { type Field, isStreamName } from "./fields.ts";
import { FieldsBar, RECORD_GROUP, type ValueRow, type Values } from "./FieldsBar.tsx";
import {
  FULLTEXT_LIMIT_TEXT, FULLTEXT_LIMIT_US, FULLTEXT_NARROW_US, SQL_FULLTEXT_NOTE, fulltextBlocked, narrowOnAdd,
} from "./fulltext.ts";
import { Histogram } from "./Histogram.tsx";
import { buckets, chooseInterval, zoomRange } from "./histogram.ts";
import {
  type Condition, conditionSql, countSql, detailCondition, histogramSql, insertAtCursor, mentionsFulltext, searchSql,
  selectSql, timeColumns, valuesSql, whereClause,
} from "./sql.ts";
import { Table, columnsOf } from "./Table.tsx";
import { Detail } from "./detail/Detail.tsx";
import { combine, contextQueries, whereOf, type ContextResult } from "./context/context.ts";
import { type HistoryItem, sameCondition } from "./history/history.ts";
import { HistoryPanel } from "./history/HistoryPanel.tsx";
import { addHistory } from "./history/store.ts";
import { zoneOf } from "./zone.ts";
import "./search.css";

const SIZE = 100;
const HIST_SIZE = 1000;
const QUICK: QuickChoice[] = [
  { label: "最近 15 分钟", us: 15n * MINUTE_US },
  { label: "最近 1 小时", us: HOUR_US },
  { label: "最近 6 小时", us: 6n * HOUR_US },
  { label: "最近 24 小时", us: 24n * HOUR_US },
  { label: "最近 7 天", us: 7n * DAY_US },
  { label: "最近 30 天", us: 30n * DAY_US },
];

/** 分布图收起与显示的状态（REQ-AMB-UI-SEARCH-HIST-COLLAPSE），存浏览器本地存储。 */
const HIST_KEY = "amberlog.search.histogram";
function loadCollapsed(): boolean {
  try {
    return localStorage.getItem(HIST_KEY) === "collapsed";
  } catch {
    return false;
  }
}
function saveCollapsed(collapsed: boolean): void {
  try {
    localStorage.setItem(HIST_KEY, collapsed ? "collapsed" : "shown");
  } catch {
    // 存储不可用时照常工作，只是不记
  }
}

/** 字段栏收起与显示的状态（REQ-AMB-UI-SEARCH-V2-FIELDS-COLLAPSE），存浏览器本地存储。 */
const FIELDS_KEY = "amberlog.search.fields";
function loadFieldsCollapsed(): boolean {
  try {
    return localStorage.getItem(FIELDS_KEY) === "collapsed";
  } catch {
    return false;
  }
}
function saveFieldsCollapsed(collapsed: boolean): void {
  try {
    localStorage.setItem(FIELDS_KEY, collapsed ? "collapsed" : "shown");
  } catch {
    // 存储不可用时照常工作，只是不记
  }
}

const FIELDS_FAIL = "无法读取字段列表";
const VALUES_TOO_MANY = "取值太多，无法统计";

/** 字段列表请求失败时字段栏显示的各行（FIELDS-FAIL）。 */
function fieldsErrorLines(o: Exclude<FieldsOutcome, { kind: "ok" } | { kind: "aborted" }>): string[] {
  if (o.kind === "connect") return [FIELDS_FAIL];
  return o.reason !== null ? [FIELDS_FAIL, String(o.status), o.reason] : [FIELDS_FAIL, String(o.status)];
}

/** 打开页面时查一次数据集候选；得到 401 时在口令被修改后再查一次（REQ-AMB-UI-SEARCH-DATASET-QUERY、DATASET-FAIL）。 */
function useDatasetCandidates(req: ResultRequest): void {
  useEffect(() => {
    let off: (() => void) | null = null;
    const run = async (): Promise<void> => {
      const now = nowUs();
      let confirmed = false;
      const watch = onTokenConfirmed(() => {
        confirmed = true;
      });
      const o = await req.ancillary({ sql: "SELECT dataset FROM _ledger", start_time: now - MINUTE_US, end_time: now, size: 10000 }, "datasets");
      watch();
      if (o === null) return;
      if (o.kind === "ok") setDatasets(o.hits);
      else if (o.kind === "http" && o.status === 401 && confirmed) {
        // 应答到达之前口令已被修改：马上再查一次
        void run();
      } else if (o.kind === "http" && o.status === 401 && off === null) {
        off = onTokenConfirmed(() => {
          off?.();
          off = null;
          void run();
        });
      }
    };
    void run();
    return () => off?.();
  }, [req]);
}

/** 分布请求的输入：stream、检索请求的 ` WHERE ` 部分与时间范围（改时区、「重画分布图」时沿用）。 */
interface HistInput {
  stream: string;
  where: string;
  start: bigint;
  end: bigint;
}

type Chart =
  | { kind: "none" }
  | { kind: "bars"; name: string; us: bigint; zone: string; bars: Bar[]; range: [bigint, bigint] }
  | { kind: "busy" }
  | { kind: "error"; lines: string[] };

/** 最近一次发起检索。 */
interface Launched {
  mode: "cond" | "sql";
  query: Query;
  /** 已生效的条件（条件模式）。 */
  chips: readonly Chip[];
  stream: string;
  /** 检索请求的 ` WHERE ` 部分（条件模式；不含 ` ORDER BY`）。 */
  where: string;
}

/** 正在显示的上下文结果（REQ-AMB-UI-SEARCH-V2 上下文浏览一组）：t 为打开详情的那一行的 `_timestamp`，`stream` 为所查的 stream。 */
interface Context extends ContextResult {
  t: bigint;
  stream: string;
}

const CONTEXT_EDGES = ["已是第一条", "已是最后一条"] as const;
const CENTER_MISSING = "与这条记录同一时刻的记录过多，无法标出这条记录";
const CONTEXT_FEW = "这条记录前后 1 小时内的记录不足 20 条";

/** 结果表正在显示的那次检索请求及其所属的检索（时刻列、全文标出、翻页与排序按它）。 */
interface Shown {
  launched: Launched;
  query: Query;
}

const numText = (v: Json | undefined): string | null => (v instanceof Num ? v.text : null);

export function SearchScreen({ hidden }: { hidden: boolean }) {
  const req = useResultRequest();
  const range = useTimeRange(QUICK, HOUR_US);
  useStore(range.store);
  const { list } = useStore(datasets);
  const [stream, setStream] = useState("");
  const [mode, setMode] = useState<"cond" | "sql">("cond");
  const [chips, setChips] = useState<Chip[]>([]);
  const nextId = useRef(1);
  const [sql, setSql] = useState("");
  /** 切到 SQL 模式时填入的文本（TO-COND 据此判断改过没有）。 */
  const [sqlBase, setSqlBase] = useState("");
  const cursor = useRef<number | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [ascending, setAscending] = useState(false);
  const [collapsed, setCollapsed] = useState(loadCollapsed);
  const [chart, setChart] = useState<Chart>({ kind: "none" });
  const [count, setCount] = useState<bigint | null>(null);
  const [launched, setLaunched] = useState<Launched | null>(null);
  const [shown, setShown] = useState<Shown | null>(null);
  /** 上一次在条件模式下发起检索时条件框中的条件（已生效）。 */
  const [appliedIds, setAppliedIds] = useState<ReadonlySet<number>>(new Set());
  const appliedChips = useRef<readonly Chip[]>([]);
  /** 分布请求与计数请求的编号：发起检索、改时区重发、「重画分布图」各取一个，晚到的旧应答丢弃。 */
  const totalSeq = useRef(0);
  const [histInput, setHistInput] = useState<HistInput | null>(null);
  const [sqlNote, setSqlNote] = useState(false);
  const [hiddenCols, setHiddenCols] = useState<ReadonlySet<string>>(new Set());
  const [viewed, setViewed] = useState(-1);
  /** 打开着详情的一行（REQ-AMB-UI-SEARCH-DETAIL-OPEN），没有时为 null。 */
  const [detail, setDetail] = useState<number | null>(null);
  /** 当前数据集（CURRENT-DATASET）：数据集输入框最近一次被确定的 stream 名。 */
  const [current, setCurrent] = useState<string | null>(null);
  /** 各 stream 的字段表（FIELDS-TABLE），只在本次页面里。 */
  const [tables, setTables] = useState<ReadonlyMap<string, readonly Field[]>>(new Map());
  const [fieldErrors, setFieldErrors] = useState<ReadonlyMap<string, string[]>>(new Map());
  /** 尚未得到响应的字段列表请求：stream → 其中最晚一个的编号（同一 stream 更晚的请求取代先发的）。 */
  const fieldsInFlight = useRef(new Map<string, number>());
  const fieldsSeq = useRef(0);
  const [fieldsCollapsed, setFieldsCollapsed] = useState(loadFieldsCollapsed);
  const [openGroups, setOpenGroups] = useState<ReadonlySet<string>>(new Set([RECORD_GROUP]));
  /** 展开了取值的字段（VALUES-SHOW）。 */
  const [values, setValues] = useState<ReadonlyMap<string, Values>>(new Map());
  /** 正在显示的上下文结果；显示检索请求的结果时为 null。 */
  const [ctx, setCtx] = useState<Context | null>(null);
  /** 最近一次发出的检索请求（「保留当前条件」取它的 WHERE 部分）。 */
  const lastSearch = useRef<Query | null>(null);
  useDatasetCandidates(req);

  const requestFields = async (s: string): Promise<void> => {
    const id = ++fieldsSeq.current;
    fieldsInFlight.current.set(s, id);
    const o = await req.fields(s, "fields:" + s);
    if (fieldsInFlight.current.get(s) === id) fieldsInFlight.current.delete(s);
    if (o === null || o.kind === "aborted") return;
    if (o.kind === "ok") {
      setTables((t) => new Map(t).set(s, o.fields));
      setFieldErrors((e) => {
        const m = new Map(e);
        m.delete(s);
        return m;
      });
    } else {
      setFieldErrors((e) => new Map(e).set(s, fieldsErrorLines(o)));
    }
  };

  /** 收起已展开的取值，中止尚未得到响应的取值请求（VALUES-RESET）。 */
  const resetValues = (): void => {
    req.dropSlot("values");
    setValues(new Map());
  };

  /** 确定数据集（CURRENT-DATASET）；`fetch` 为假时不引发字段列表请求（「检索」跳转）。 */
  const determine = (s: string, fetch: boolean): void => {
    if (s !== current) {
      setCurrent(s);
      resetValues();
      setOpenGroups(new Set([RECORD_GROUP]));
    }
    if (fetch && isStreamName(s) && !tables.has(s) && !fieldsInFlight.current.has(s)) void requestFields(s);
  };

  useEffect(() => {
    entries.search = {
      setStream(s) {
        req.dropContext();
        setStream(s);
        latest.current.determine(s, false);
        show("search");
      },
    };
    return () => {
      entries.search = null;
    };
  }, []);

  useEffect(
    () =>
      req.onCancel((e) => {
        // 取消上下文请求：结果表回到检索请求的结果，分布图与总条数不变（CONTEXT-FAIL）
        if (e.context) {
          setCtx(null);
          return;
        }
        setChart({ kind: "none" });
        setCount(null);
        setValues(new Map());
      }),
    [req],
  );

  const sendHist = async (h: HistInput): Promise<void> => {
    const seq = ++totalSeq.current;
    const tz = settings.get().timezone;
    const zone = zoneOf(tz);
    const iv = chooseInterval(h.start, h.end, zone);
    setChart({ kind: "none" });
    setCount(null);
    const o = await req.ancillary(
      { sql: histogramSql(h.stream, h.where, iv.name), start_time: h.start, end_time: h.end, size: HIST_SIZE, from: 0 },
      "total",
    );
    if (o === null || seq !== totalSeq.current) return;
    if (o.kind === "ok") {
      const rows = o.hits.map((x) => ({ t: numText(x.get("t")), n: numText(x.get("n")) }));
      const starts = buckets(h.start, h.end, iv.us, zone);
      setChart({ kind: "bars", name: iv.name, us: iv.us, zone: tz, bars: bars(starts, countsByStart(rows)), range: [h.start, h.end] });
      setCount(total(rows));
    } else {
      // HIST-FAIL 只说「得到 HTTP 429」，不论响应体
      setChart(o.kind === "http" && o.status === 429 ? { kind: "busy" } : { kind: "error", lines: errorLines(o) });
    }
  };

  const sendCount = async (h: HistInput): Promise<void> => {
    const seq = ++totalSeq.current;
    setChart({ kind: "none" });
    setCount(null);
    const o = await req.ancillary({ sql: countSql(h.stream, h.where), start_time: h.start, end_time: h.end, size: 1, from: 0 }, "total");
    if (o === null || seq !== totalSeq.current || o.kind !== "ok") return;
    const n = numText(o.hits[0]?.get("n"));
    setCount(n !== null && /^\d+$/.test(n) ? BigInt(n) : null);
  };

  // 改时区：最近一次检索在条件模式下发出且分布图显示时，以新时区重发分布请求（HIST-TZ）
  const latest = useRef({ launched, collapsed, histInput, sendHist, determine, current, tables, requestFields });
  latest.current = { launched, collapsed, histInput, sendHist, determine, current, tables, requestFields };
  useEffect(() => {
    let tz = settings.get().timezone;
    return settings.subscribe(() => {
      const now = settings.get().timezone;
      if (now === tz) return;
      tz = now;
      const l = latest.current;
      if (l.launched?.mode === "cond" && !l.collapsed && l.histInput !== null) void l.sendHist(l.histInput);
    });
  }, []);

  // 口令被修改之后，当前数据集没有字段表时补发字段列表请求（FIELDS-REQUEST）
  useEffect(
    () =>
      onTokenConfirmed(() => {
        const l = latest.current;
        if (l.current !== null && isStreamName(l.current) && !l.tables.has(l.current) && !fieldsInFlight.current.has(l.current)) {
          void l.requestFields(l.current);
        }
      }),
    [],
  );

  const table = current === null ? undefined : tables.get(current);
  const utf8Names = new Set((table ?? []).filter((f) => f.type === "Utf8").map((f) => f.name));
  /** 条件模式下按当前数据集的字段表写字面量（LITERAL）。 */
  const utf8 = (f: string): boolean => utf8Names.has(f);

  const conds = chips.map((c) => c.cond);
  const condPending = chips.length !== appliedIds.size || chips.some((c) => !appliedIds.has(c.id));
  // SQL 模式：上一次在 SQL 模式下检索过时比 SQL 文本；否则切过来时的未生效条件仍算，另加对 SQL 文本的修改
  const pending =
    mode === "cond"
      ? condPending
      : launched?.mode === "sql"
        ? sql !== launched.query.sql
        : condPending || sql !== sqlBase;
  const blocked = mode === "cond" && fulltextBlocked(conds, range.length());

  /** 发起检索（REQ-AMB-UI-SEARCH-QUERY）：点「查询」、点柱缩放、「关闭并查询」。 */
  const launch = (): void => {
    // 429 倒计时中、或全文范围超限时「查询」不可用；点柱缩放、「关闭并查询」同样不发
    if (req.queryDisabled || (mode === "cond" && fulltextBlocked(conds, range.length()))) return;
    const r = range.resolve();
    if (r === null) {
      // TIME-INPUT-INVALID 不发请求；分布图清空、总条数为「—」（HIST-CLEAR）
      req.invalidTime();
      setChart({ kind: "none" });
      setCount(null);
      return;
    }
    const [start, end] = r;
    // 发起检索结束上下文结果的显示（CONTEXT-BACK）；在途的上下文请求由 req.result 中止（CONTEXT-STALE）
    setCtx(null);
    const where = mode === "cond" ? whereClause(conds, utf8) : "";
    const text = mode === "cond" ? selectSql(stream, where, ascending) : sql;
    const query: Query = { sql: text, start_time: start, end_time: end, size: SIZE, from: 0 };
    const l: Launched = { mode, query, chips: mode === "cond" ? chips : [], stream, where };
    resetValues();
    setLaunched(l);
    if (mode === "cond") {
      setAppliedIds(new Set(chips.map((c) => c.id)));
      appliedChips.current = chips;
    }
    const m = range.model;
    const at = nowUs();
    const kept = m.quick !== null ? { quick: m.quick } : { start, end };
    addHistory(
      mode === "cond" ? { mode, stream, range: kept, conds: [...conds], at } : { mode, range: kept, sql: text, at },
    );
    setSqlNote(mode === "sql" && mentionsFulltext(text) && end - start > FULLTEXT_LIMIT_US);
    runResult(l, query);
    if (mode === "sql") {
      totalSeq.current++;
      setHistInput(null);
      setChart({ kind: "none" });
      setCount(null);
      return;
    }
    const h: HistInput = { stream, where, start, end };
    setHistInput(h);
    void (collapsed ? sendCount(h) : sendHist(h));
    // 条件模式下每次发起检索时请求当前数据集的字段列表（FIELDS-REQUEST）
    if (current !== null && isStreamName(current)) void requestFields(current);
  };

  /** 能展开字段的取值（VALUES-REQUEST 的前提）。 */
  const canExpand =
    mode === "cond" &&
    launched?.mode === "cond" &&
    launched.stream === current &&
    ![...values.values()].some((v) => v.kind === "loading");

  const toggleField = async (f: string): Promise<void> => {
    if (values.has(f)) {
      setValues((m) => {
        const n = new Map(m);
        n.delete(f);
        return n;
      });
      return;
    }
    if (!canExpand || launched === null || f === "_timestamp") return;
    setValues((m) => new Map(m).set(f, { kind: "loading" }));
    const q = launched.query;
    const vq: Query = { sql: valuesSql(launched.stream, launched.where, f), start_time: q.start_time, end_time: q.end_time, size: 10, from: 0 };
    const o = await req.inSlot("values", (signal) => postSearch(vq, signal));
    if (o === null) return;
    let v: Values;
    if (o.kind === "ok") {
      const rows: ValueRow[] = o.hits.map((h) => {
        const x = h.get(f);
        return { json: x === undefined ? "null" : toJsonText(x), isString: typeof x === "string", text: rawText(x), n: rawText(h.get("n__")) };
      });
      v = { kind: "ok", rows };
    } else if (o.kind === "http" && o.status === 400 && o.reason === "query_memory_exceeded") {
      v = { kind: "error", lines: [VALUES_TOO_MANY] };
    } else {
      v = { kind: "error", lines: errorLines(o) };
    }
    setValues((m) => (m.has(f) ? new Map(m).set(f, v) : m));
  };

  /** 发结果请求；成功时结果表改按这次请求显示。 */
  const runResult = (l: Launched, q: Query): void => {
    lastSearch.current = q;
    void req.result(q).then((o) => {
      if (o?.kind === "ok") {
        setShown({ launched: l, query: q });
        setViewed(-1);
        setDetail(null);
      }
    });
  };

  /** 翻页与排序：只重发检索请求（PAGING、TABLE-SORT）。 */
  const resend = (q: Query): void => {
    if (shown === null) return;
    runResult(shown.launched, q);
  };

  const addCondition = (c: Condition): void => {
    const length = range.length();
    if (narrowOnAdd(conds, c, length)) {
      const m = range.model;
      if (m.quick !== null) range.setQuick(FULLTEXT_NARROW_US);
      else if (m.end !== null) range.setRange(m.end - FULLTEXT_NARROW_US, m.end);
    }
    setChips([...chips, { id: nextId.current++, cond: c }]);
  };

  /**
   * 恢复查询历史中的一项（HISTORY-RESTORE）：只设定检索屏，不发请求、不收窄范围；与上一次在条件模式下检索时
   * 相同的条件沿用其编号，算已生效。
   */
  const restore = (it: HistoryItem): void => {
    if ("quick" in it.range) range.setQuick(it.range.quick);
    else range.setRange(it.range.start, it.range.end);
    setConfirming(false);
    cursor.current = null;
    if (it.mode === "sql") {
      setChips([]);
      setSql(it.sql);
      setSqlBase(it.sql);
      setMode("sql");
      return;
    }
    const unused = [...appliedChips.current];
    setChips(
      it.conds.map((cond) => {
        const k = unused.findIndex((a) => sameCondition(a.cond, cond));
        return k < 0 ? { id: nextId.current++, cond } : { id: unused.splice(k, 1)[0]!.id, cond };
      }),
    );
    if (it.stream !== stream) changeStream(it.stream);
    // 恢复数据集即确定一次当前数据集（CURRENT-DATASET；数据集没变时也按 FIELDS-REQUEST 判断要不要发）
    determine(it.stream, true);
    setMode("cond");
  };

  const switchMode = (m: "cond" | "sql"): void => {
    if (m === mode) return;
    if (m === "sql") {
      const text = searchSql(stream, conds, ascending, utf8);
      setSql(text);
      setSqlBase(text);
      cursor.current = null;
      setConfirming(false);
      setMode("sql");
    } else if (sql === sqlBase) {
      setMode("cond");
    } else {
      setConfirming(true);
    }
  };

  const view = shownView(req.view);
  const ok = view.kind === "ok" ? view : null;
  const hits: readonly Hit[] = ok?.hits ?? [];
  const columns = columnsOf(hits);
  const from = shown?.query.from ?? 0;
  const timeKeys = timeColumns(shown?.query.sql ?? "");
  const terms =
    shown?.launched.mode === "cond" ? shown.launched.chips.flatMap((c) => (c.cond.kind === "text" ? [c.cond.text] : [])) : [];

  /** 当前数据集改变：中止在途的上下文请求（CONTEXT-STALE）。 */
  const changeStream = (s: string): void => {
    req.dropContext();
    setStream(s);
  };

  /** 详情里的行（上下文结果或检索请求的结果）。 */
  const detailHits: readonly Hit[] = ctx !== null ? ctx.rows : (ok?.hits ?? []);

  /** 「看前后记录」：关闭详情，同时发出前段与后段两个上下文请求（CONTEXT-REQUEST）。 */
  const browse = async (row: Hit, from: string, keepConditions: boolean): Promise<void> => {
    const t = intOf(row.get("_timestamp"));
    if (t === null) return;
    setDetail(null);
    const where = keepConditions && lastSearch.current !== null ? whereOf(lastSearch.current.sql, from) : "";
    const outs = await req.context(contextQueries(from, t, where));
    if (outs === null) {
      // 失败或被取消：结果表保持最近一次检索请求的结果（CONTEXT-FAIL）；被取代时由取代者处理
      const k = req.view.kind;
      if (k === "error" || k === "busy" || k === "cancelled") setCtx(null);
      return;
    }
    const [before, after] = outs;
    if (before?.kind !== "ok" || after?.kind !== "ok") return;
    setCtx({ ...combine(before.hits, after.hits, row), t, stream: from });
    setViewed(-1);
  };

  /** 打开详情的那一行可「看前后记录」时，该行所属请求的 stream（CONTEXT-ENTRY）；不可时为 null。 */
  const contextStream = (row: Hit | undefined): string | null => {
    if (row === undefined || intOf(row.get("_timestamp")) === null) return null;
    if (ctx !== null) return ctx.stream;
    return shown?.launched.mode === "cond" ? shown.launched.stream : null;
  };

  const toggle = (): void => {
    if (chart.kind === "busy") {
      // 「重画分布图」只重发分布请求
      if (histInput !== null) void sendHist(histInput);
      return;
    }
    saveCollapsed(!collapsed);
    setCollapsed(!collapsed);
  };
  const toggleText =
    mode === "sql" ? "手写 SQL 不画分布图" : chart.kind === "busy" ? "重画分布图" : collapsed ? "显示分布图" : "收起分布图";

  const zoom = (a: number, b: number): void => {
    if (chart.kind !== "bars") return;
    const first = chart.bars[a], last = chart.bars[b];
    if (first === undefined || last === undefined) return;
    const [s, e] = zoomRange(first.start, last.start, chart.us, zoneOf(chart.zone), chart.range);
    range.setRange(s, e);
    launch();
  };

  const info = (body: Map<string, Json>): string => {
    const rows = pageRows(Number(rawText(body.get("from"))), hits.length);
    const parts = [
      "耗时 " + rawText(body.get("took")) + " ms",
      "扫描 " + rawText(body.get("scan_files")) + " 个文件",
      rawText(body.get("scan_bytes")) + " 字节",
      rawText(body.get("scan_records")) + " 条记录",
    ];
    return (rows === null ? parts : [rows, ...parts]).join(" · ");
  };

  return (
    <section id="search" class="screen" hidden={hidden}>
      <div class="bar">
        <input
          id="s-stream"
          list="s-streams"
          autocomplete="off"
          aria-label="数据集"
          value={stream}
          onInput={(e) => {
            const v = e.currentTarget.value;
            changeStream(v);
            // 从候选中选定（不是逐字输入）即确定数据集
            const kind = (e as InputEvent).inputType;
            if (kind === undefined || kind === "insertReplacementText") determine(v, true);
          }}
          onBlur={(e) => determine(e.currentTarget.value, true)}
          onKeyDown={(e) => {
            if (e.key === "Enter") determine(e.currentTarget.value, true);
          }}
        />
        <datalist id="s-streams">
          {list.map((d) => (
            <option value={streamOf(d)} />
          ))}
        </datalist>
        <TimeRangeInputs range={range} prefix="s" />
        {fieldsCollapsed && (
          <button
            type="button"
            id="s-fields-show"
            onClick={() => {
              saveFieldsCollapsed(false);
              setFieldsCollapsed(false);
            }}
          >
            » 字段
          </button>
        )}
      </div>
      <div class="bar">
        <CondBox
          mode={mode}
          chips={chips}
          applied={appliedIds}
          columns={table !== undefined ? table.map((f) => f.name) : columns}
          valuesOf={(f) => {
            const v = values.get(f);
            return v?.kind === "ok" ? v.rows : null;
          }}
          sql={sql}
          confirming={confirming}
          onAdd={addCondition}
          onRemove={(id) => setChips(chips.filter((c) => c.id !== id))}
          onMode={switchMode}
          onSql={(text, at) => {
            setSql(text);
            cursor.current = at;
          }}
          onCursor={(at) => {
            cursor.current = at;
          }}
          onConfirm={(discard) => {
            setConfirming(false);
            if (discard) {
              setSql(sqlBase);
              setMode("cond");
            }
          }}
        />
        <button type="button" id="s-query" class="primary" disabled={req.queryDisabled || blocked} onClick={launch}>
          {req.running ? "查询中" : "查询"}
        </button>
        {pending && <span class="staged-mark" id="s-staged" title="条件已改，尚未查询" />}
        <HistoryPanel
          sqlEdited={mode === "sql" && sql !== (launched?.mode === "sql" ? launched.query.sql : sqlBase)}
          onRestore={restore}
        />
      </div>
      {blocked && (
        <p class="hint" id="s-limit">
          {FULLTEXT_LIMIT_TEXT}
        </p>
      )}
      {sqlNote && (
        <p class="hint" id="s-sql-note">
          {SQL_FULLTEXT_NOTE}
        </p>
      )}
      <div class="results">
      {!fieldsCollapsed && (
        <FieldsBar
          fields={table ?? null}
          error={current === null ? null : (fieldErrors.get(current) ?? null)}
          open={openGroups}
          onToggleGroup={(k) => {
            const n = new Set(openGroups);
            if (n.has(k)) n.delete(k);
            else n.add(k);
            setOpenGroups(n);
          }}
          values={values}
          canExpand={canExpand}
          onToggleField={(f) => void toggleField(f)}
          onAdd={(f, negate, v) => addCondition(detailCondition(f, negate, v))}
          onCollapse={() => {
            saveFieldsCollapsed(true);
            setFieldsCollapsed(true);
          }}
        />
      )}
      <div class="card">
        <div class="info">
          <span id="s-total">{"共 " + (count === null || mode === "sql" ? "—" : String(count)) + " 条"}</span>
          {ok !== null && ctx === null && <span id="s-info">{info(ok.body)}</span>}
          <span class="spacer" />
          <button type="button" id="s-hist-toggle" disabled={mode === "sql"} onClick={toggle}>
            {toggleText}
          </button>
          {hiddenCols.size > 0 && (
            <span class="restore" id="s-hidden-cols">
              {[...hiddenCols].map((c) => (
                <button
                  type="button"
                  onClick={() => setHiddenCols(new Set([...hiddenCols].filter((x) => x !== c)))}
                >
                  {"+ " + c}
                </button>
              ))}
            </span>
          )}
          <button
            type="button"
            id="s-prev"
            disabled={shown === null || ok === null || ctx !== null || from === 0}
            onClick={() => shown !== null && resend({ ...shown.query, from: Math.max(0, from - SIZE) })}
          >
            上一页
          </button>
          <button
            type="button"
            id="s-next"
            disabled={shown === null || ok === null || ctx !== null || hits.length < SIZE}
            onClick={() => shown !== null && resend({ ...shown.query, from: from + SIZE })}
          >
            下一页
          </button>
        </div>
        {mode === "cond" && !collapsed && chart.kind !== "busy" && (
          <div class="hist-area" id="s-hist">
            {chart.kind === "bars" && <Histogram name={chart.name} bars={chart.bars} onZoom={zoom} />}
            {chart.kind === "error" && (
              <div class="message">
                {chart.lines.map((l) => (
                  <p>{l}</p>
                ))}
              </div>
            )}
          </div>
        )}
        {ctx !== null && (
          <div class="context-bar" id="s-context">
            <Time us={ctx.t} />
            <button
              type="button"
              id="s-context-back"
              onClick={() => {
                // 不发请求，结果表恢复为最近一次检索请求的结果
                setCtx(null);
                setViewed(-1);
              }}
            >
              返回检索结果
            </button>
            {ctx.center === null && (
              <span class="hint" id="s-context-center-missing">
                {CENTER_MISSING}
              </span>
            )}
            {ctx.few && (
              <span class="hint" id="s-context-few">
                {CONTEXT_FEW}
              </span>
            )}
          </div>
        )}
        <ResultArea
          req={req}
          prefix="s"
          plain={ctx !== null}
          render={(rows) => (
            <Table
              hits={ctx !== null ? ctx.rows : rows}
              timeKeys={timeKeys}
              terms={terms}
              hidden={hiddenCols}
              ascending={ctx === null && mode === "cond" && shown?.launched.mode === "cond" ? ascending : null}
              viewed={viewed}
              center={ctx?.center ?? -1}
              onSort={() => {
                if (shown === null || shown.launched.mode !== "cond") return;
                const l = shown.launched;
                const asc = !ascending;
                setAscending(asc);
                resend({ ...shown.query, sql: selectSql(l.stream, l.where, asc), from: 0 });
              }}
              onHide={(c) => setHiddenCols(new Set([...hiddenCols, c]))}
              onOpen={(i) => {
                setDetail(i);
                setViewed(i);
              }}
            />
          )}
        />
        {detail !== null && detailHits.length > 0 && (
          <Detail
            hits={detailHits}
            index={detail}
            onIndex={(i) => {
              setDetail(i);
              setViewed(i);
            }}
            timeKeys={timeKeys}
            textTerms={terms}
            onAdd={(c) => {
              // 条件模式加入未生效的条件，SQL 模式插到光标处（DETAIL-ADD）；都不发请求
              if (mode === "cond") {
                addCondition(c);
                return;
              }
              const r = insertAtCursor(sql, cursor.current, conditionSql(c));
              setSql(r.sql);
              cursor.current = r.cursor;
            }}
            pending={pending}
            onClose={() => setDetail(null)}
            onCloseAndQuery={() => {
              setDetail(null);
              launch();
            }}
            onContext={(() => {
              const row = detailHits[detail];
              const from = contextStream(row);
              return row === undefined || from === null ? undefined : (keep: boolean) => void browse(row, from, keep);
            })()}
            edges={ctx !== null ? CONTEXT_EDGES : undefined}
          />
        )}
      </div>
      </div>
    </section>
  );
}

