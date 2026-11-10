// 时区选择（REQ-AMB-UI-CONSOLE-TZ-CHOICES）：下拉列出全部候选，旁边的输入框按名字筛选（需求块未要求筛选，属实现自由）。

import { useState } from "preact/hooks";
import { setTimeZone, timeZones } from "./settings.ts";

/** 名字含 `filter`（不区分大小写）的候选；当前所选总在其中。 */
export function filterZones(zones: readonly string[], filter: string, selected: string): string[] {
  const f = filter.trim().toLowerCase();
  if (f === "") return [...zones];
  return zones.filter((z) => z === selected || z.toLowerCase().includes(f));
}

export function TimeZonePicker({ timezone }: { timezone: string }) {
  const [filter, setFilter] = useState("");
  return (
    <>
      <input
        id="tz-filter"
        class="tz-filter"
        autocomplete="off"
        spellcheck={false}
        aria-label="筛选时区"
        value={filter}
        onInput={(e) => setFilter(e.currentTarget.value)}
      />
      <select id="tz" value={timezone} onChange={(e) => setTimeZone(e.currentTarget.value)}>
        {filterZones(timeZones, filter, timezone).map((z) => (
          <option value={z}>{z}</option>
        ))}
      </select>
    </>
  );
}
