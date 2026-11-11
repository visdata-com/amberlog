// 分布图（REQ-AMB-UI-SEARCH-HIST-DRAW、HIST-TOOLTIP、HIST-ZOOM）：每个分桶一根柱，同色；点一根柱或拖选几根缩放。

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
import { useStore } from "../../lib/store.ts";
import { settings } from "../../shell/settings.ts";
import { type Bar, heights } from "./chart.ts";
import { zoomRange } from "./histogram.ts";
import { histogramTick, histogramTime, histogramZone } from "./histogramLabels.ts";
import { zoneOf } from "./zone.ts";

export interface HistogramProps {
  name: string;
  bars: readonly Bar[];
  interval: bigint;
  range: readonly [bigint, bigint];
  /** 选中第 a 至第 b 根柱（含两端，a ≤ b）。 */
  onZoom: (a: number, b: number) => void;
}

export function Histogram({ name, bars, interval, range, onZoom }: HistogramProps) {
  const { timezone } = useStore(settings);
  const [drag, setDrag] = useState<{ from: number; to: number } | null>(null);
  const [hover, setHover] = useState<{ bar: Bar; index: number; x: number; y: number } | null>(null);
  const [focused, setFocused] = useState(0);
  const [position, setPosition] = useState({ left: 0, top: 0 });
  const tooltip = useRef<HTMLDivElement>(null);
  const columns = useRef<(HTMLDivElement | null)[]>([]);
  const labels = useMemo(() => bars.map((b) => {
    const [start, end] = zoomRange(b.start, b.start, interval, zoneOf(timezone), range);
    return { start, end, text: histogramTime(start, timezone) + " 至 " + histogramTime(end, timezone) + "，" + b.n + " 条日志" };
  }), [bars, interval, range, timezone]);
  const active = hover !== null && bars[hover.index] === hover.bar ? hover : null;
  const span = active === null ? null : labels[active.index]!;
  const tickIndices = [...new Set([0, Math.round(bars.length / 4), Math.round(bars.length / 2), Math.round(bars.length * 3 / 4), bars.length])];
  useLayoutEffect(() => {
    if (hover === null || tooltip.current === null) return;
    const box = tooltip.current.getBoundingClientRect();
    const left = Math.max(12, Math.min(hover.x + 14, window.innerWidth - box.width - 12));
    const above = hover.y - box.height - 14;
    const top = above >= 12 ? above : Math.max(12, Math.min(hover.y + 18, window.innerHeight - box.height - 12));
    setPosition({ left, top });
  }, [hover, timezone]);
  useEffect(() => {
    const close = (): void => setHover(null);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    window.addEventListener("blur", close);
    return () => {
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
      window.removeEventListener("blur", close);
    };
  }, []);
  // 拖选了几根柱时，松开后紧跟的 click 不再按单根缩放
  const dragged = useRef(false);
  const hs = useMemo(() => heights(bars), [bars]);
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
      <div class="hist-header">
        <span class="hist-title">日志时间分布</span>
        <span class="hist-range">{histogramTime(range[0], timezone) + " → " + histogramTime(range[1], timezone) + " · " + timezone}</span>
      </div>
      <div class="hbars" onPointerLeave={() => { setDrag(null); setHover(null); }} onPointerCancel={() => { setDrag(null); setHover(null); }}>
        {bars.map((b, i) => {
          return (
            <div
              key={String(b.start)}
              ref={(el) => { columns.current[i] = el; }}
              class={(i >= lo && i <= hi) || active?.index === i ? "hbar picked" : "hbar"}
              data-start={String(b.start)}
              role="button"
              tabIndex={i === Math.min(focused, bars.length - 1) ? 0 : -1}
              aria-label={labels[i]!.text + "，按回车缩小查询范围"}
              aria-describedby={active?.index === i ? "s-hist-tooltip" : undefined}
              onPointerDown={(e) => {
                e.preventDefault();
                dragged.current = false;
                setDrag({ from: i, to: i });
              }}
              onPointerEnter={(e) => {
                if (e.pointerType !== "touch") setHover({ bar: b, index: i, x: e.clientX, y: e.clientY });
                if (drag !== null) setDrag({ from: drag.from, to: i });
              }}
              onPointerMove={(e) => {
                if (e.pointerType !== "touch") setHover({ bar: b, index: i, x: e.clientX, y: e.clientY });
              }}
              onFocus={(e) => {
                const box = e.currentTarget.getBoundingClientRect();
                setFocused(i);
                setHover({ bar: b, index: i, x: box.x + box.width / 2, y: box.y });
              }}
              onBlur={() => setHover(null)}
              onKeyDown={(e) => {
                if (e.key === "Escape") setHover(null);
                else if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onZoom(i, i);
                } else if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(e.key)) {
                  e.preventDefault();
                  const next = e.key === "Home" ? 0 : e.key === "End" ? bars.length - 1 : i + (e.key === "ArrowLeft" ? -1 : 1);
                  columns.current[Math.max(0, Math.min(next, bars.length - 1))]?.focus();
                }
              }}
              onPointerUp={finish}
              onClick={() => {
                if (dragged.current) dragged.current = false;
                else onZoom(i, i);
              }}
            >
              <div class="fill" style={{ height: hs[i]! * 100 + "%" }} />
            </div>
          );
        })}
      </div>
      <div class="hist-axis" aria-label="时间轴">
        {tickIndices.map((index) => {
          const at = index === bars.length ? range[1] : index === 0 ? range[0] : bars[index]!.start;
          const edge = index === 0 ? "start" : index === bars.length ? "end" : "middle";
          const minor = index !== 0 && index !== bars.length && index !== Math.round(bars.length / 2);
          return <span
            key={index}
            class={"hist-axis__tick" + (minor ? " hist-axis__tick--minor" : "")}
            data-edge={edge}
            style={{ left: (bars.length === 0 ? 0 : index / bars.length * 100) + "%" }}
          >{histogramTick(at, range, timezone)}</span>;
        })}
      </div>
      <div class="hist-footer">
        <span>悬停查看详情 · 点击或拖选缩小时间范围</span>
        <span class="per-bar" id="s-per-bar">{"每柱 " + name}</span>
      </div>
      {active !== null && span !== null && (
        <div
          ref={tooltip}
          class="hist-tooltip"
          id="s-hist-tooltip"
          role="tooltip"
          style={{ left: position.left + "px", top: position.top + "px" }}
        >
          <div class="hist-tooltip__range">
            <span>开始</span><span>{histogramTime(span.start, timezone)}</span>
            <span>结束</span><span>{histogramTime(span.end, timezone)}</span>
          </div>
          <div class="hist-tooltip__count"><span>日志条数</span><strong>{/^\d+$/.test(active.bar.n) ? BigInt(active.bar.n).toLocaleString("en-US") : active.bar.n} 条</strong></div>
          <div class="hist-tooltip__zone">{histogramZone(span.start, span.end, timezone)}</div>
        </div>
      )}
    </div>
  );
}
