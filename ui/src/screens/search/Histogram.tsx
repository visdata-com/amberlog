// 分布图（REQ-AMB-UI-SEARCH-HIST-DRAW、HIST-TOOLTIP、HIST-ZOOM）：每个分桶一根柱，同色；点一根柱或拖选几根缩放。

import { useRef, useState } from "preact/hooks";
import { useStore } from "../../lib/store.ts";
import { formatLocal } from "../../lib/time.ts";
import { settings } from "../../shell/settings.ts";
import { type Bar, heights } from "./chart.ts";

export interface HistogramProps {
  name: string;
  bars: readonly Bar[];
  /** 选中第 a 至第 b 根柱（含两端，a ≤ b）。 */
  onZoom: (a: number, b: number) => void;
}

export function Histogram({ name, bars, onZoom }: HistogramProps) {
  const { timezone } = useStore(settings);
  const [drag, setDrag] = useState<{ from: number; to: number } | null>(null);
  // 拖选了几根柱时，松开后紧跟的 click 不再按单根缩放
  const dragged = useRef(false);
  const hs = heights(bars);
  const lo = drag === null ? -1 : Math.min(drag.from, drag.to);
  const hi = drag === null ? -1 : Math.max(drag.from, drag.to);
  const finish = (): void => {
    if (drag === null) return;
    setDrag(null);
    if (lo < hi) {
      dragged.current = true;
      onZoom(lo, hi);
    }
  };
  return (
    <div class="hist">
      <div class="hbars" onPointerLeave={() => setDrag(null)}>
        {bars.map((b, i) => (
          <div
            class={i >= lo && i <= hi ? "hbar picked" : "hbar"}
            data-start={String(b.start)}
            title={formatLocal(b.start, timezone) + "  " + b.n + " 条"}
            onPointerDown={(e) => {
              e.preventDefault();
              dragged.current = false;
              setDrag({ from: i, to: i });
            }}
            onPointerEnter={() => drag !== null && setDrag({ from: drag.from, to: i })}
            onPointerUp={finish}
            onClick={() => {
              if (dragged.current) dragged.current = false;
              else onZoom(i, i);
            }}
          >
            <div class="fill" style={{ height: hs[i]! * 100 + "%" }} />
          </div>
        ))}
      </div>
      <p class="per-bar" id="s-per-bar">{"每柱 " + name}</p>
    </div>
  );
}
