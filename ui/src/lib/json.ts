// 保留原文的 JSON 解析（REQ-AMB-UI-CONSOLE-VALUE-RAW）：数字保留十进制原文、不经浮点换算，对象保留键的先后。

/** JSON 数字：保留响应里的十进制原文。 */
export class Num {
  readonly text: string;
  constructor(text: string) {
    this.text = text;
  }
}

export type Json = null | boolean | string | Num | Json[] | JsonObject;
export type JsonObject = Map<string, Json>;
/** `_search` 响应 `hits` 的一个元素。 */
export type Hit = JsonObject;

const NUMBER = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
const STRING = /"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"/y;

/** 解析 JSON 文本；不合 JSON 时抛 `SyntaxError`。 */
export function parseJson(src: string): Json {
  let i = 0;
  const fail = (): never => {
    throw new SyntaxError("invalid JSON at " + i);
  };
  const ws = (): void => {
    while (i < src.length && " \t\r\n".includes(src[i] as string)) i++;
  };
  const string = (): string => {
    STRING.lastIndex = i;
    const m = STRING.exec(src);
    if (m === null) return fail();
    i += m[0].length;
    return JSON.parse(m[0]) as string;
  };
  const value = (): Json => {
    ws();
    const c = src[i];
    if (c === "{") {
      i++;
      const m: JsonObject = new Map();
      ws();
      if (src[i] === "}") {
        i++;
        return m;
      }
      for (;;) {
        ws();
        if (src[i] !== '"') fail();
        const k = string();
        ws();
        if (src[i++] !== ":") fail();
        m.set(k, value());
        ws();
        if (src[i] === ",") {
          i++;
          continue;
        }
        if (src[i++] === "}") return m;
        fail();
      }
    }
    if (c === "[") {
      i++;
      const a: Json[] = [];
      ws();
      if (src[i] === "]") {
        i++;
        return a;
      }
      for (;;) {
        a.push(value());
        ws();
        if (src[i] === ",") {
          i++;
          continue;
        }
        if (src[i++] === "]") return a;
        fail();
      }
    }
    if (c === '"') return string();
    if (src.startsWith("true", i)) {
      i += 4;
      return true;
    }
    if (src.startsWith("false", i)) {
      i += 5;
      return false;
    }
    if (src.startsWith("null", i)) {
      i += 4;
      return null;
    }
    NUMBER.lastIndex = i;
    const m = NUMBER.exec(src);
    if (m === null) return fail();
    i += m[0].length;
    return new Num(m[0]);
  };
  const v = value();
  ws();
  if (i !== src.length) fail();
  return v;
}

/** 值的原始 JSON 文本（对象与数组照原样写出，键的先后不变）。 */
export function toJsonText(v: Json): string {
  if (v instanceof Num) return v.text;
  if (v instanceof Map) {
    return "{" + [...v].map(([k, x]) => JSON.stringify(k) + ":" + toJsonText(x)).join(",") + "}";
  }
  if (Array.isArray(v)) return "[" + v.map(toJsonText).join(",") + "]";
  return JSON.stringify(v);
}

/** 显示用的文字：数字为原文，字符串去掉引号与转义，`true`/`false`，对象与数组为 JSON 原文；NULL（含缺少该键）为「—」。 */
export function rawText(v: Json | undefined): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  return toJsonText(v);
}

/** 值为 JSON 整数时返回它（BigInt），否则为 null。 */
export function intOf(v: Json | undefined): bigint | null {
  return v instanceof Num && /^-?\d+$/.test(v.text) ? BigInt(v.text) : null;
}
