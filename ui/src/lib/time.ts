// 时刻：UTC 微秒（BigInt）与所选时区的本地时刻互换（REQ-AMB-UI-CONSOLE 时区与时刻两组）。
//
// 时区规则取浏览器的 Intl（formatToParts 给出某一 UTC 秒在该时区的挂钟），微秒部分自己用整数算，不经 Date 的毫秒。
// 本地时刻的重复与跳过按块的规则自己判断：常见库的缺省取法与块不同（被跳过的 02:30 多取 03:30，块要 03:00）。
// 所选时区一律作参数传入，本文件不读界面状态，可由 node --test 直接测。

export const SECOND_US = 1_000_000n;
export const MINUTE_US = 60n * SECOND_US;
export const HOUR_US = 60n * MINUTE_US;
export const DAY_US = 24n * HOUR_US;

export function nowUs(): bigint {
  return BigInt(Date.now()) * 1000n;
}

/** 本地挂钟：年、月、日、时、分、秒与秒以下的微秒。 */
export interface Wall {
  y: number;
  mo: number;
  d: number;
  h: number;
  mi: number;
  s: number;
  us: number;
}

const floorDiv = (a: bigint, b: bigint): bigint => {
  const q = a / b;
  return q * b > a ? q - 1n : q;
};

// 公历日期与 1970-01-01 起的天数互换（Howard Hinnant 的算法）。
function daysFromCivil(y: number, m: number, d: number): number {
  y -= m <= 2 ? 1 : 0;
  const era = Math.floor(y / 400);
  const yoe = y - era * 400;
  const mp = m > 2 ? m - 3 : m + 9;
  const doy = Math.floor((153 * mp + 2) / 5) + d - 1;
  const doe = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy;
  return era * 146097 + doe - 719468;
}

function civilFromDays(z: number): [number, number, number] {
  z += 719468;
  const era = Math.floor(z / 146097);
  const doe = z - era * 146097;
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const d = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const m = mp < 10 ? mp + 3 : mp - 9;
  return [yoe + era * 400 + (m <= 2 ? 1 : 0), m, d];
}

/** 把挂钟当作 UTC 读成微秒（本文件称「本地微秒」）。 */
export function wallToUs(w: Wall): bigint {
  const secs = ((daysFromCivil(w.y, w.mo, w.d) * 24 + w.h) * 60 + w.mi) * 60 + w.s;
  return BigInt(secs) * SECOND_US + BigInt(w.us);
}

/** 本地微秒拆成挂钟。 */
export function usToWall(local: bigint): Wall {
  const day = floorDiv(local, DAY_US);
  const rem = local - day * DAY_US;
  const [y, mo, d] = civilFromDays(Number(day));
  const sec = Number(rem / SECOND_US);
  return { y, mo, d, h: Math.floor(sec / 3600), mi: Math.floor(sec / 60) % 60, s: sec % 60, us: Number(rem % SECOND_US) };
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function formatter(tz: string): Intl.DateTimeFormat {
  let f = formatters.get(tz);
  if (f === undefined) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hourCycle: "h23",
      era: "short",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      second: "numeric",
    });
    formatters.set(tz, f);
  }
  return f;
}

/** UTC 整秒在时区 `tz` 中的偏移（秒）。 */
function offsetAtSecond(sec: bigint, tz: string): number {
  const parts = formatter(tz).formatToParts(new Date(Number(sec) * 1000));
  const get = (t: string): number => Number(parts.find((p) => p.type === t)?.value);
  let y = get("year");
  if (parts.find((p) => p.type === "era")?.value === "BC") y = 1 - y;
  const local = ((daysFromCivil(y, get("month"), get("day")) * 24 + get("hour")) * 60 + get("minute")) * 60 + get("second");
  return local - Number(sec);
}

/** UTC 微秒 `us` 在时区 `tz` 中的偏移（秒）。 */
export function offsetOf(us: bigint, tz: string): number {
  return offsetAtSecond(floorDiv(us, SECOND_US), tz);
}

/** UTC 微秒 → 时区 `tz` 中的本地挂钟。 */
export function wallOf(us: bigint, tz: string): Wall {
  return usToWall(us + BigInt(offsetOf(us, tz)) * SECOND_US);
}

const pad = (n: number, w: number): string => String(n).padStart(w, "0");

function formatWall(w: Wall, sep: string): string {
  return (
    pad(w.y, 4) + "-" + pad(w.mo, 2) + "-" + pad(w.d, 2) + sep +
    pad(w.h, 2) + ":" + pad(w.mi, 2) + ":" + pad(w.s, 2) + "." + pad(w.us, 6)
  );
}

/** 所选时区的本地时刻，写作 `YYYY-MM-DD HH:MM:SS.ffffff`（REQ-AMB-UI-CONSOLE-TIME-FORMAT）。 */
export function formatLocal(us: bigint, tz: string): string {
  return formatWall(wallOf(us, tz), " ");
}

/** 该时刻在时区 `tz` 的 UTC 偏移，写作 `±HH:MM`（REQ-AMB-UI-CONSOLE-TIME-LABEL）。 */
export function formatOffset(us: bigint, tz: string): string {
  const o = offsetOf(us, tz);
  const a = Math.abs(o);
  // 偏移不足一分钟的部分（只在 1900 年前的地方平时出现）舍去
  return (o < 0 ? "-" : "+") + pad(Math.floor(a / 3600), 2) + ":" + pad(Math.floor(a / 60) % 60, 2);
}

/** 以 `Z` 结尾、精确到微秒的 RFC 3339 UTC 形式。 */
export function formatUtc(us: bigint): string {
  return formatWall(usToWall(us), "T") + "Z";
}

/** 本地微秒 `local` 前后各一天内出现过的偏移（秒）。 */
function nearbyOffsets(local: bigint, tz: string): number[] {
  const set = new Set<number>();
  for (const k of [-2n, -1n, 0n, 1n, 2n]) set.add(offsetOf(local + k * (DAY_US / 2n), tz));
  return [...set];
}

/** 本地微秒 `local` 在时区 `tz` 中对应的全部 UTC 微秒，从早到晚；该本地时刻被跳过时为空。 */
export function utcCandidates(local: bigint, tz: string): bigint[] {
  const out: bigint[] = [];
  for (const o of nearbyOffsets(local, tz)) {
    const t = local - BigInt(o) * SECOND_US;
    if (offsetOf(t, tz) === o && !out.includes(t)) out.push(t);
  }
  return out.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

/** 被跳过的本地微秒 `local`：跳变之后的第一个 UTC 微秒（时区规则在整秒处变化）。 */
export function gapEnd(local: bigint, tz: string): bigint {
  const offsets = nearbyOffsets(local, tz);
  // 跳变之前的偏移较小：按它换算的 UTC 时刻落在跳变之后；按较大的换算落在跳变之前
  let lo = floorDiv(local - BigInt(Math.max(...offsets)) * SECOND_US, SECOND_US);
  let hi = floorDiv(local - BigInt(Math.min(...offsets)) * SECOND_US, SECOND_US);
  const before = offsetAtSecond(lo, tz);
  if (offsetAtSecond(hi, tz) === before) return local - BigInt(before) * SECOND_US;
  // 不变式：lo 处仍是跳变前的偏移，hi 处已不是
  while (hi - lo > 1n) {
    const mid = (lo + hi) / 2n;
    if (offsetAtSecond(mid, tz) === before) lo = mid;
    else hi = mid;
  }
  return hi * SECOND_US;
}

/** 按所选时区解释本地时刻（REQ-AMB-UI-CONSOLE-TIME-INPUT-LOCAL）：重复取最早的 UTC 时刻，被跳过取跳变之后的第一个时刻。 */
export function localToUtcEarliest(local: bigint, tz: string): bigint {
  return utcCandidates(local, tz)[0] ?? gapEnd(local, tz);
}

const LOCAL_INPUT = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?$/;
const RFC3339_INPUT = /^(\d{4})-(\d{2})-(\d{2})[ Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?:([Zz])|([+-])(\d{2}):(\d{2}))$/;

function validWall(y: number, mo: number, d: number, h: number, mi: number, s: number): boolean {
  if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
  const leap = (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
  const dim = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1] as number;
  return d <= dim;
}

/**
 * 起止框的文字 → UTC 微秒（REQ-AMB-UI-CONSOLE-TIME-INPUT）：本地写法 `YYYY-MM-DD HH:MM[:SS[.f…]]`（空格或 `T`）按时区
 * `tz` 解释，带 `Z` 或 `±HH:MM` 的 RFC 3339 按所写偏移解释；空、写法不合或日历上不存在的日期为 null。
 */
export function parseTimeInput(text: string, tz: string): bigint | null {
  const t = text.trim();
  const r = RFC3339_INPUT.exec(t);
  const m = r ?? LOCAL_INPUT.exec(t);
  if (m === null) return null;
  const [y, mo, d, h, mi] = [1, 2, 3, 4, 5].map((i) => Number(m[i]));
  const s = Number(m[6] ?? "0");
  if (!validWall(y as number, mo as number, d as number, h as number, mi as number, s)) return null;
  const us = Number((m[7] ?? "").padEnd(6, "0"));
  const local = wallToUs({ y: y as number, mo: mo as number, d: d as number, h: h as number, mi: mi as number, s, us });
  if (r === null) return localToUtcEarliest(local, tz);
  if (r[8] !== undefined) return local;
  const oh = Number(r[10]);
  const om = Number(r[11]);
  if (oh > 23 || om > 59) return null;
  const off = BigInt((oh * 60 + om) * 60) * SECOND_US;
  return r[9] === "-" ? local + off : local - off;
}

/** 时区选择的候选（REQ-AMB-UI-CONSOLE-TZ-CHOICES）：`UTC` 与浏览器支持的全部 IANA 时区名。 */
export function timeZoneChoices(): string[] {
  const names = Intl.supportedValuesOf("timeZone").filter((z) => z !== "UTC");
  return ["UTC", ...names];
}

/** 浏览器的时区（REQ-AMB-UI-CONSOLE-TZ-INITIAL）；取不到或不在候选中为 `UTC`。 */
export function browserTimeZone(choices: readonly string[]): string {
  let tz: string | undefined;
  try {
    tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch {
    tz = undefined;
  }
  return tz !== undefined && choices.includes(tz) ? tz : "UTC";
}
