// 查询历史存在浏览器本地存储的一个键里（REQ-AMB-UI-SEARCH-V2-LOCAL-STORAGE）；存储不可用时照常工作，只是不记。

import { createStore } from "../../../lib/store.ts";
import { type HistoryItem, decode, encode, record } from "./history.ts";

const KEY = "amberlog.search.history";

function load(): HistoryItem[] {
  try {
    return decode(localStorage.getItem(KEY));
  } catch {
    return [];
  }
}

function save(items: readonly HistoryItem[]): void {
  try {
    if (items.length === 0) localStorage.removeItem(KEY);
    else localStorage.setItem(KEY, encode(items));
  } catch {
    // 存储不可用时照常工作，只是不记
  }
}

/** 从新到旧。 */
export const history = createStore({ items: load() });

/** 记入一次发起的检索（HISTORY-SAVE）。 */
export function addHistory(item: HistoryItem): void {
  const items = record(history.get().items, item);
  save(items);
  history.set({ items });
}

/** 「清空查询历史」（HISTORY-CLEAR）。 */
export function clearHistory(): void {
  save([]);
  history.set({ items: [] });
}
