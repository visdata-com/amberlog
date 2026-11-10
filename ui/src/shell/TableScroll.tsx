// 三屏共用的表格滚动区域；新结果从顶部开始，普通界面更新保留滚动位置。
import type { ComponentChildren as ReactNode } from "preact";
import { useEffect, useRef } from "preact/hooks";
import type { Hit } from "../lib/json.ts";

export function TableScroll({ rows, label, children }: { rows: readonly Hit[]; label: string; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (ref.current !== null) ref.current.scrollTop = 0;
  }, [rows]);
  return (
    <div className="table-scroll" ref={ref} role="region" aria-label={label + "，可滚动"} tabIndex={0}>
      {children}
    </div>
  );
}
