// 某屏的时间范围（REQ-AMB-UI-CONSOLE-TIME-INPUT、QUICK-RANGE、TIME-RANGE-KEPT）：以 UTC 微秒保存；起止框的文字只在
// 用户编辑该框之后按所选时区换算一次，快捷项、缩放与跳转写入的是时刻；改时区只重显框里的写法，不改范围。

import { useEffect, useMemo } from "preact/hooks";
import { createStore, useStore, type Store } from "../lib/store.ts";
import { formatLocal, nowUs, parseTimeInput } from "../lib/time.ts";
import { settings } from "./settings.ts";

export interface QuickChoice {
  label: string;
  us: bigint;
}

export interface RangeModel {
  /** 选了快捷项时为其时长（微秒），否则为 null。 */
  quick: bigint | null;
  /** 起止（UTC 微秒）；null 为空或写法不合。 */
  start: bigint | null;
  end: bigint | null;
  /** 用户编辑中的文字；null 为按 start / end 与所选时区显示。 */
  startText: string | null;
  endText: string | null;
}

export class TimeRange {
  readonly store: Store<RangeModel>;
  readonly choices: readonly QuickChoice[];

  constructor(choices: readonly QuickChoice[], initial: bigint) {
    this.choices = choices;
    const now = nowUs();
    this.store = createStore<RangeModel>({ quick: initial, start: now - initial, end: now, startText: null, endText: null });
  }

  get model(): RangeModel {
    return this.store.get();
  }

  /** 发结果请求前调：快捷项按此刻 T 取 `[T − N, T)` 并写回起止；有一框为空或写法不合时为 null。 */
  resolve(now: bigint = nowUs()): [bigint, bigint] | null {
    const m = this.model;
    if (m.quick !== null) {
      this.store.set({ start: now - m.quick, end: now, startText: null, endText: null });
      return [now - m.quick, now];
    }
    if (m.start === null || m.end === null) return null;
    this.store.set({ startText: null, endText: null });
    return [m.start, m.end];
  }

  /** 时间范围的长短（微秒，`end_time − start_time`；快捷项为其时长）；有一框为空或写法不合时为 null。 */
  length(): bigint | null {
    const m = this.model;
    if (m.quick !== null) return m.quick;
    return m.start === null || m.end === null ? null : m.end - m.start;
  }

  /** 写入起止时刻（缩放、跳转）。 */
  setRange(start: bigint, end: bigint): void {
    this.store.set({ quick: null, start, end, startText: null, endText: null });
  }

  /** 选快捷项：起止框先按此刻显示，发请求时再按发出时刻取。 */
  setQuick(us: bigint): void {
    const now = nowUs();
    this.store.set({ quick: us, start: now - us, end: now, startText: null, endText: null });
  }

  /** 用户编辑起点或止点框：按此刻的所选时区换算，改为自定义范围。 */
  edit(which: "start" | "end", text: string): void {
    const v = parseTimeInput(text, settings.get().timezone);
    this.store.set(which === "start" ? { quick: null, start: v, startText: text } : { quick: null, end: v, endText: text });
  }

  /** 所选时区改变：已换算的框改按新时区重显（写法不合的文字留着）。 */
  redisplay(): void {
    const m = this.model;
    this.store.set({
      startText: m.start === null ? m.startText : null,
      endText: m.end === null ? m.endText : null,
    });
  }
}

/** 本屏的时间范围；所选时区改变时重显起止框。 */
export function useTimeRange(choices: readonly QuickChoice[], initial: bigint): TimeRange {
  const r = useMemo(() => new TimeRange(choices, initial), []);
  useEffect(() => {
    let tz = settings.get().timezone;
    return settings.subscribe(() => {
      const now = settings.get().timezone;
      if (now !== tz) {
        tz = now;
        r.redisplay();
      }
    });
  }, [r]);
  return r;
}

const CUSTOM = "";

/** 起止框（`#<p>-start`、`#<p>-end`）与快捷项（`#<p>-quick`）。 */
export function TimeRangeInputs({ range, prefix }: { range: TimeRange; prefix: string }) {
  const m = useStore(range.store);
  const { timezone } = useStore(settings);
  const shown = (v: bigint | null, text: string | null): string =>
    text !== null ? text : v === null ? "" : formatLocal(v, timezone);
  return (
    <span class="range">
      <input
        id={prefix + "-start"}
        class="time-input"
        autocomplete="off"
        spellcheck={false}
        aria-label="起"
        value={shown(m.start, m.startText)}
        onInput={(e) => range.edit("start", e.currentTarget.value)}
      />
      <span class="arrow">→</span>
      <input
        id={prefix + "-end"}
        class="time-input"
        autocomplete="off"
        spellcheck={false}
        aria-label="止"
        value={shown(m.end, m.endText)}
        onInput={(e) => range.edit("end", e.currentTarget.value)}
      />
      <select
        id={prefix + "-quick"}
        value={m.quick === null ? CUSTOM : String(m.quick)}
        onChange={(e) => {
          const v = e.currentTarget.value;
          if (v !== CUSTOM) range.setQuick(BigInt(v));
        }}
      >
        <option value={CUSTOM}>自定义</option>
        {range.choices.map((c) => (
          <option value={String(c.us)}>{c.label}</option>
        ))}
      </select>
    </span>
  );
}
