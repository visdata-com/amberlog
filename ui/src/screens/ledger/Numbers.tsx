// 台账屏与对账屏的数值格（REQ-AMB-UI-GOVERNANCE-NUMBER-FORMAT）：格式化后的写法，悬停见原始 JSON 文本；
// 不是 JSON 整数时按 REQ-AMB-UI-CONSOLE-VALUE-RAW 显示（NULL 为「—」）。

import { rawText, type Json } from "../../lib/json.ts";
import { bytes, grouped } from "./format.ts";

function Formatted({ v, text }: { v: Json | undefined; text: string | null }) {
  return text === null ? <>{rawText(v)}</> : <span title={rawText(v)}>{text}</span>;
}

/** 条数、计数与差值：千分位。 */
export function Count({ v }: { v: Json | undefined }) {
  return <Formatted v={v} text={grouped(v)} />;
}

/** 字节数：按 1024 进位。 */
export function Bytes({ v }: { v: Json | undefined }) {
  return <Formatted v={v} text={bytes(v)} />;
}
