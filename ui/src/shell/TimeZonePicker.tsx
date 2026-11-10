// 时区选择（REQ-AMB-UI-CONSOLE-TZ-CHOICES）：在同一个下拉控件中搜索、选择全部候选。

import { setTimeZone, timeZones } from "./settings.ts";
import { Select } from "./Select.tsx";

export function TimeZonePicker({ timezone }: { timezone: string }) {
  return (
    <Select
      id="tz"
      class="tz-select"
      aria-label="时区"
      searchable
      placeholder="搜索或选择时区"
      value={timezone}
      options={timeZones.map((z) => ({ value: z, label: z }))}
      onChange={setTimeZone}
    />
  );
}
