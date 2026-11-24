import { useLayoutEffect, useRef, useState } from "preact/hooks";
import "./searchGuide.css";

const steps = [
  { target: "token", title: "输入访问口令", detail: "在这里输入访问口令。口令只保存在当前标签页，填写后即可加载数据集。" },
  { target: "s-stream", title: "选择数据集", detail: "选择要检索的日志数据集。没有候选项时，请先检查口令和日志是否已写入。" },
  { target: "s-time-trigger", title: "选择时间范围", detail: "默认查询最近一小时。点击这里可以切换常用时间范围或设置起止时间。" },
  { target: "s-conds", title: "添加筛选条件", detail: "按需输入关键词或字段条件并回车添加；也可以直接查询全部日志。" },
  { target: "s-query", title: "执行查询", detail: "点击查询查看日志。没有结果时，可扩大时间范围或减少筛选条件后重试。" },
] as const;

type Position = { rect: DOMRect; left: number; top: number };
const px = (value: number): string => `${value}px`;

export function SearchGuide({ onClose }: { onClose: () => void }) {
  const [index, setIndex] = useState(0);
  const [position, setPosition] = useState<Position | null>(null);
  const card = useRef<HTMLDivElement>(null);
  const next = useRef<HTMLButtonElement>(null);
  const previousFocus = useRef<HTMLElement | null>(null);

  useLayoutEffect(() => {
    previousFocus.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    return () => previousFocus.current?.focus({ preventScroll: true });
  }, []);

  useLayoutEffect(() => {
    const target = document.getElementById(index === 3 && !document.getElementById("s-conds") ? "s-sql" : steps[index]!.target);
    if (!target) return;
    const update = () => {
      const rect = target.getBoundingClientRect();
      const width = Math.min(340, window.innerWidth - 32);
      const height = card.current?.offsetHeight ?? 190;
      const below = window.innerHeight - rect.bottom;
      const above = rect.top;
      const top = below >= height + 24 || below >= above
        ? Math.min(rect.bottom + 14, window.innerHeight - height - 16)
        : Math.max(16, rect.top - height - 14);
      const left = Math.max(16, Math.min(rect.left, window.innerWidth - width - 16));
      setPosition({ rect, left, top: Math.max(16, top) });
    };
    const rect = target.getBoundingClientRect();
    const headerBottom = document.querySelector(".topbar")?.getBoundingClientRect().bottom ?? 0;
    if (rect.bottom < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth ||
        (!target.closest(".topbar") && (rect.top < headerBottom || rect.bottom > window.innerHeight))) {
      target.scrollIntoView({ block: "center", inline: "nearest" });
    }
    update();
    const observer = new ResizeObserver(update);
    observer.observe(target);
    if (card.current) observer.observe(card.current);
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [index]);

  useLayoutEffect(() => { next.current?.focus({ preventScroll: true }); }, [index]);

  const keydown = (event: KeyboardEvent) => {
    if (event.key === "Escape") { event.preventDefault(); onClose(); return; }
    if (event.key !== "Tab" || !card.current) return;
    const buttons = [...card.current.querySelectorAll<HTMLButtonElement>("button")];
    const first = buttons[0], last = buttons.at(-1);
    if (!card.current.contains(document.activeElement)) { event.preventDefault(); first?.focus(); }
    else if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };

  useLayoutEffect(() => {
    document.addEventListener("keydown", keydown);
    return () => document.removeEventListener("keydown", keydown);
  });

  const rect = position?.rect;
  const bounds = rect && { left: Math.max(0, rect.left - 5), top: Math.max(0, rect.top - 5), right: Math.min(window.innerWidth, rect.right + 5), bottom: Math.min(window.innerHeight, rect.bottom + 5) };
  const mask = (left: number, top: number, width: number, height: number) =>
    <div className="search-guide__mask" style={{ left: px(left), top: px(top), width: px(Math.max(0, width)), height: px(Math.max(0, height)) }} />;

  return (
    <div className="search-guide" aria-label="检索指引">
      {bounds && <>
        {mask(0, 0, window.innerWidth, bounds.top)}
        {mask(0, bounds.bottom, window.innerWidth, window.innerHeight - bounds.bottom)}
        {mask(0, bounds.top, bounds.left, bounds.bottom - bounds.top)}
        {mask(bounds.right, bounds.top, window.innerWidth - bounds.right, bounds.bottom - bounds.top)}
        <div className="search-guide__highlight" style={{ left: px(bounds.left), top: px(bounds.top), width: px(bounds.right - bounds.left), height: px(bounds.bottom - bounds.top) }} />
      </>}
      <div className="search-guide__card" ref={card} role="dialog" aria-modal="true" aria-labelledby="search-guide-title" style={{ left: px(position?.left ?? 16), top: px(position?.top ?? 80) }}>
        <div className="search-guide__heading"><span>{index + 1} / {steps.length}</span><button type="button" onClick={onClose}>跳过</button></div>
        <h2 id="search-guide-title">{index === 3 && !document.getElementById("s-conds") ? "输入 SQL" : steps[index]!.title}</h2>
        <p>{index === 3 && !document.getElementById("s-conds") ? "在这里输入查询 SQL；也可以切回条件模式，按字段添加筛选条件。" : steps[index]!.detail}</p>
        <div className="search-guide__actions">
          {index > 0 && <button type="button" onClick={() => setIndex(index - 1)}>上一步</button>}
          <button type="button" ref={next} className="search-guide__next" onClick={() => index === steps.length - 1 ? onClose() : setIndex(index + 1)}>
            {index === steps.length - 1 ? "完成" : "下一步"}
          </button>
        </div>
      </div>
    </div>
  );
}
