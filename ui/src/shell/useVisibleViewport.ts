import { useLayoutEffect, useState } from "preact/hooks";

/** Fixed mobile panels follow the visible viewport, including the software keyboard. */
export function useVisibleViewport(enabled: boolean): Record<string, string> {
  const [style, setStyle] = useState<Record<string, string>>({});
  useLayoutEffect(() => {
    if (!enabled) return;
    const viewport = window.visualViewport;
    const update = (): void => setStyle({
      "--visible-left": `${viewport?.offsetLeft ?? 0}px`,
      "--visible-top": `${viewport?.offsetTop ?? 0}px`,
      "--visible-width": `${viewport?.width ?? window.innerWidth}px`,
      "--visible-height": `${viewport?.height ?? window.innerHeight}px`,
    });
    update();
    window.addEventListener("resize", update);
    viewport?.addEventListener("resize", update);
    viewport?.addEventListener("scroll", update);
    return () => {
      window.removeEventListener("resize", update);
      viewport?.removeEventListener("resize", update);
      viewport?.removeEventListener("scroll", update);
    };
  }, [enabled]);
  return style;
}
