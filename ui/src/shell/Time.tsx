// 时刻的显示（REQ-AMB-UI-CONSOLE-TIME-FORMAT、TIME-LABEL、TZ-REDRAW）：按所选时区，悬停见偏移与 UTC 形式，改时区即重显。

import { useStore } from "../lib/store.ts";
import { formatLocal, formatOffset, formatUtc } from "../lib/time.ts";
import { settings } from "./settings.ts";

/** 悬停文字：该时刻在所选时区的偏移与 UTC 的 RFC 3339 形式。 */
export function timeTitle(us: bigint, tz: string): string {
  return formatOffset(us, tz) + " " + formatUtc(us);
}

export function Time({ us }: { us: bigint }) {
  const { timezone } = useStore(settings);
  return (
    <span class="time" title={timeTitle(us, timezone)}>
      {formatLocal(us, timezone)}
    </span>
  );
}

/** 显示时刻的列的列头：标题加所选时区名。 */
export function TimeHeader({ label }: { label: string }) {
  const { timezone } = useStore(settings);
  return <>{label + "（" + timezone + "）"}</>;
}
