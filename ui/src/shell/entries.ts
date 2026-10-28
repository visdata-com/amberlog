// 跨屏入口：台账屏的「检索」「对账」跳转经这里写入目标屏（REQ-AMB-UI-GOVERNANCE-LEDGER-JUMPS）。各屏挂载时注册自己的入口，
// 别的屏只经这里调，不直接碰别的屏的状态。

import { setScreen, type Screen } from "./settings.ts";

export interface SearchEntry {
  /** 填入 stream 名、切到检索屏；不改条件与时间范围，不查询。 */
  setStream(stream: string): void;
}

export interface ReconEntry {
  /** 填入数据集与起止时刻、切到对账屏，并自动查询一次。 */
  jump(dataset: string, start: bigint, end: bigint): void;
}

export const entries: { search: SearchEntry | null; recon: ReconEntry | null } = { search: null, recon: null };

/** 切到某屏（不发请求，REQ-AMB-UI-CONSOLE-SWITCH）。 */
export function show(screen: Screen): void {
  setScreen(screen);
}
