// 数据集列表（REQ-AMB-UI-SEARCH-DATASET-QUERY、DATASET-FROM-LEDGER）：各 `dataset` 原样（带 `default/`）。
// 检索屏的候选查询与台账屏的查询写入，检索屏（去掉前缀的 stream 名）与对账屏（原样）读。

import type { Hit } from "../lib/json.ts";
import { createStore } from "../lib/store.ts";

export const datasets = createStore({ list: [] as string[] });

/** 以 `hits` 中各 `dataset` 为数据集列表。 */
export function setDatasets(hits: readonly Hit[]): void {
  const list = hits.map((h) => h.get("dataset")).filter((d): d is string => typeof d === "string");
  datasets.set({ list });
}

/** `dataset` 去掉 `default/` 前缀所得的 stream 名。 */
export function streamOf(dataset: string): string {
  return dataset.startsWith("default/") ? dataset.slice("default/".length) : dataset;
}
