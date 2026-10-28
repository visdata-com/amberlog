// 台账屏与对账屏的数值格式（REQ-AMB-UI-GOVERNANCE-NUMBER-FORMAT）：条数与计数加千分位，字节数按 1024 进位、`B` 以外保留
// 一位小数。值不是 JSON 整数（含 NULL）时为 null，由调用方按原始 JSON 文本显示。

import { intOf, type Json } from "../../lib/json.ts";

/** JSON 整数的千分位写法（`1,234,567`）。 */
export function grouped(v: Json | undefined): string | null {
  const n = intOf(v);
  if (n === null) return null;
  const digits = (n < 0n ? -n : n).toString();
  const head = digits.length % 3 || 3;
  const parts = [digits.slice(0, head)];
  for (let i = head; i < digits.length; i += 3) parts.push(digits.slice(i, i + 3));
  return (n < 0n ? "-" : "") + parts.join(",");
}

const UNITS = ["B", "KiB", "MiB", "GiB", "TiB"] as const;

/** 非负 JSON 整数的字节数写法（`1.2 GiB`、`512 B`）：取使取值不小于 1 的最大单位，四舍五入到一位小数。 */
export function bytes(v: Json | undefined): string | null {
  const n = intOf(v);
  if (n === null || n < 0n) return null;
  let k = 0;
  while (k < UNITS.length - 1 && n >= 1024n ** BigInt(k + 1)) k++;
  if (k === 0) return n.toString() + " B";
  let tenths = (n * 10n * 2n + 1024n ** BigInt(k)) / (2n * 1024n ** BigInt(k));
  // 舍入后满 1024 时进到下一个单位（如 1048575 B 为 1.0 MiB 而不是 1024.0 KiB）
  if (tenths >= 10240n && k < UNITS.length - 1) {
    k++;
    tenths = (n * 10n * 2n + 1024n ** BigInt(k)) / (2n * 1024n ** BigInt(k));
  }
  return (tenths / 10n).toString() + "." + (tenths % 10n).toString() + " " + UNITS[k];
}
