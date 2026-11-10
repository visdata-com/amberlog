// 小型状态容器：屏与屏之间共享的状态放在这里，组件经 useStore 读取并订阅。

import { useEffect, useState } from "preact/hooks";

export interface Store<T extends object> {
  get(): T;
  set(patch: Partial<T> | ((s: T) => Partial<T>)): void;
  subscribe(f: () => void): () => void;
}

export function createStore<T extends object>(init: T): Store<T> {
  let state = init;
  const subs = new Set<() => void>();
  return {
    get: () => state,
    set(patch) {
      const p = typeof patch === "function" ? patch(state) : patch;
      state = { ...state, ...p };
      for (const f of [...subs]) f();
    },
    subscribe(f) {
      subs.add(f);
      return () => subs.delete(f);
    },
  };
}

/** 在组件里读 `s` 的当前值；`s` 变化时重画。 */
export function useStore<T extends object>(s: Store<T>): T {
  const [, tick] = useState(0);
  useEffect(() => s.subscribe(() => tick((n) => n + 1)), [s]);
  return s.get();
}
