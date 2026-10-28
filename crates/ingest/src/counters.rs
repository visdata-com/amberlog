//! 治理计数（Counters）：`received`、`durable`、`rejected`、`received_503`、按来源的 `by_source`、按记录时间戳的
//! `durable_ts` 与 `durable_ts_by_source`、`_sent` 上报的 `sent` 与 `sent_ts` 的聚合与计数对象的写出
//! （REQ-AMB-OBS-INGEST-COUNTERS-V3、REQ-AMB-LIFE-DURABILITY 删段与停机两条、ADR-0023、ADR-0053 第 1 条）。
//!
//! - **计入**：被确认（200）批次的增量（含 `rejected` 与来源标识）与 `_sent` 元素随 WAL 承载（摄取管道把 [`WalEntry::Counts`]、
//!   [`WalEntry::Sent`] 与记录同一次追加），fsync 成功后在同一数据集写入锁之内交给这里，所以同一 (数据集, 小时) 的可重放增量
//!   按 WAL 位置顺序生效；全拒的 400 批次与 503 批次的贡献只在内存中累加（ADR-0023 第 1、3 条及其 2026-10-12 前向注记）。
//! - **按来源**：带合法来源标识的批次的贡献另记进该窗口的 `by_source`；一个窗口至多 [`MAX_SOURCES`] 个来源，
//!   已满时新来源的贡献只计入总计（归入未标识）。
//! - **按记录时间戳**：被确认批次的记录另按 `_timestamp` 所在分钟计入 `durable_ts`（有来源标识时另记进
//!   `durable_ts_by_source`，上限同上），写进该分钟所在小时的对象，所以一个批次可以改动几个小时的对象；
//!   这些增量随同一条计数条目承载，各小时按各自的水位筛选重放增量，恰计一次。
//! - **写出**：内存中按「(数据集, 小时) → 分钟窗口」聚合，每 [`TICK`] 把有变更的小时对象整体覆盖写（第 2 条）。
//!   每个对象独立地走「装载底稿 → 记计数意图 → 确认对象当前可覆盖 → 条件写」，一个对象失败只让它自己在下一个节拍重试，
//!   不拖住别的对象；每次对象存储请求都有超时（写出时限按计数对象逐个计时）。
//! - **本地计数水位**：覆盖写之前在 WAL 中追加计数意图 (数据集, 小时, x, 待写内容摘要)，x 为这次写入所含可重放增量的
//!   最大 WAL 位置；上一次尝试没有写成时，同一次追加里先重记最近一次写成的意图，使 WAL 为每个小时保留的最近两条意图
//!   总含「最近写成的」与「最近尝试的」。重启后装载底稿时按对象摘要找到对应意图，水位即其 x，重放只交回水位之后的增量
//!   （第 4 条）；对象与任何意图都不相符时水位为 0。
//! - **格式版本不兼容情形**：底稿 `format_version` 大于 1 或内容不合规时，该小时不覆盖写、计数只在进程内保留，
//!   并以 [`WalPosition::MAX`] 通知 WAL 不再为它保留段（第 2、5 条）；每次覆盖写之前都重读对象确认这一点。
//! - **停机**：[`Counters::flush_all`] 对每个有变更的对象尝试一次（第 6 条）。
//! - **读回**：[`Counters::read_persisted`] 只读对象存储中已写出的计数对象，供治理视图（REQ-AMB-GOV-RECONCILIATION、
//!   REQ-AMB-GOV-LEDGER）作答；不含进程内尚未写出的计数，解析与「内容无法解析」的判定与写出前装载底稿共用 [`parse`]。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_storage::chain::Digest;
use amberlog_storage::objects::{Swapped, Version};
use amberlog_storage::store::Store;
use amberlog_storage::wal::{Basis, Release, SentElement, Wal, WalEntry, WalPosition};
use bytes::Bytes;
use serde_json::{Map, Value, json};

/// 分钟窗口的长度，UTC 微秒。
pub const MINUTE_US: i64 = 60_000_000;
const HOUR_US: i64 = 3_600_000_000;

/// 一个分钟窗口的 `by_source` 至多含的来源标识个数（REQ-AMB-OBS-INGEST-COUNTERS-V2）。
pub const MAX_SOURCES: usize = 1000;

/// `_sent` 元素 `window_start` 的上界：9999-12-31T23:59:00Z（REQ-AMB-OBS-INGEST-COUNTERS-V2）。
pub const MAX_SENT_WINDOW_START: i64 = 253_402_300_740_000_000;

/// 写出节拍（ADR-0023 第 2 条；不对外、不设配置键）。
pub const TICK: Duration = Duration::from_secs(5);

/// 单次对象存储请求的超时：挂起的请求按失败处理，下一个节拍重试（ADR-0023 第 2 条末）。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// `us` 所在整分钟的起点。
pub fn window_of(us: i64) -> i64 {
    us.div_euclid(MINUTE_US) * MINUTE_US
}

fn hour_of(us: i64) -> i64 {
    us.div_euclid(HOUR_US) * HOUR_US
}

/// 一个批次（或一个来源、一个窗口）的四项摄取计数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// 从请求体中解析出的记录数。
    pub received: u64,
    /// 经 HTTP 200 确认的记录数。
    pub durable: u64,
    /// 逐条拒收的记录数（200 的 `failed` 与 400 `all_records_rejected` 的 `failed`）。
    pub rejected: u64,
    /// 计入 `received`、所在请求回 503 的记录数。
    pub received_503: u64,
}

impl Tally {
    fn is_zero(&self) -> bool {
        *self == Tally::default()
    }

    fn add(&mut self, other: &Tally) {
        self.received = self.received.saturating_add(other.received);
        self.durable = self.durable.saturating_add(other.durable);
        self.rejected = self.rejected.saturating_add(other.rejected);
        self.received_503 = self.received_503.saturating_add(other.received_503);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Window {
    counts: Tally,
    /// 带合法来源标识的贡献，按来源（至多 [`MAX_SOURCES`] 个）。
    by_source: BTreeMap<String, Tally>,
    /// 对该窗口有经 200 受理的按送达上报的各来源（取值为 0 的来源不省略）。
    sent: BTreeMap<String, u64>,
    /// 按记录时间戳落在该窗口、已被确认的记录数。
    durable_ts: u64,
    /// `durable_ts` 中带合法来源标识的部分，按来源（至多 [`MAX_SOURCES`] 个）。
    durable_ts_by_source: BTreeMap<String, u64>,
    /// 对该窗口有经 200 受理的按时间戳上报的各来源（取值为 0 的来源不省略）。
    sent_ts: BTreeMap<String, u64>,
}

impl Window {
    /// 计入一个批次的贡献：总计照加；有来源标识时另记进 `by_source`，已有 [`MAX_SOURCES`] 个其他来源时不记。
    fn add(&mut self, tally: &Tally, source: Option<&str>) {
        self.counts.add(tally);
        let Some(source) = source else {
            return;
        };
        if let Some(t) = self.by_source.get_mut(source) {
            t.add(tally);
        } else if self.by_source.len() < MAX_SOURCES {
            self.by_source.insert(source.to_owned(), *tally);
        }
    }

    /// 计入按记录时间戳落在本窗口的确认数，来源规则同 [`Window::add`]。
    fn add_ts(&mut self, n: u64, source: Option<&str>) {
        self.durable_ts = self.durable_ts.saturating_add(n);
        let Some(source) = source else {
            return;
        };
        if let Some(t) = self.durable_ts_by_source.get_mut(source) {
            *t = t.saturating_add(n);
        } else if self.durable_ts_by_source.len() < MAX_SOURCES {
            self.durable_ts_by_source.insert(source.to_owned(), n);
        }
    }
}

/// 一个小时内的分钟窗口，按 `window_start` 升序。
type Windows = BTreeMap<i64, Window>;

/// 一次计入。
#[derive(Debug, Clone)]
enum Increment {
    Counts {
        window: i64,
        tally: Tally,
        source: Option<String>,
    },
    /// 按记录时间戳落在 `window` 的确认数。
    Timestamped {
        window: i64,
        durable_ts: u64,
        source: Option<String>,
    },
    Sent {
        window: i64,
        source: String,
        sent: u64,
        basis: Basis,
    },
}

fn apply(windows: &mut Windows, inc: &Increment) {
    match inc {
        Increment::Counts {
            window,
            tally,
            source,
        } => {
            if tally.is_zero() {
                return;
            }
            windows
                .entry(*window)
                .or_default()
                .add(tally, source.as_deref());
        }
        Increment::Timestamped {
            window,
            durable_ts,
            source,
        } => {
            if *durable_ts == 0 {
                return;
            }
            windows
                .entry(*window)
                .or_default()
                .add_ts(*durable_ts, source.as_deref());
        }
        Increment::Sent {
            window,
            source,
            sent,
            basis,
        } => {
            let w = windows.entry(*window).or_default();
            let reports = match basis {
                Basis::Delivery => &mut w.sent,
                Basis::Timestamp => &mut w.sent_ts,
            };
            reports.insert(source.clone(), *sent);
        }
    }
}

/// 一次计入落在哪个小时的对象。
fn hour_of_increment(inc: &Increment) -> i64 {
    match inc {
        Increment::Counts { window, .. }
        | Increment::Timestamped { window, .. }
        | Increment::Sent { window, .. } => hour_of(*window),
    }
}

/// 一个被确认批次的全部计入：按接收时刻的四项计数，加按记录时间戳各分钟的确认数。
fn durable_increments(
    window: i64,
    tally: Tally,
    source: Option<&str>,
    durable_ts: &[(i64, u64)],
) -> Vec<Increment> {
    let mut out = vec![Increment::Counts {
        window,
        tally,
        source: source.map(str::to_owned),
    }];
    out.extend(durable_ts.iter().map(|(w, n)| Increment::Timestamped {
        window: *w,
        durable_ts: *n,
        source: source.map(str::to_owned),
    }));
    out
}

/// 被接受的记录按 `_timestamp` 所在整分钟分组的条数，窗口起点升序（WAL 计数条目的 `durable_ts`）。
pub fn durable_ts_groups(timestamps: impl IntoIterator<Item = i64>) -> Vec<(i64, u64)> {
    let mut groups: BTreeMap<i64, u64> = BTreeMap::new();
    for ts in timestamps {
        *groups.entry(window_of(ts)).or_default() += 1;
    }
    groups.into_iter().collect()
}

/// 把 `delta` 并到 `base` 上：四项计数（含按来源的部分，来源个数上限照旧）累加，`sent` 按来源以 `delta` 的取值覆盖。
fn merge(base: &mut Windows, delta: Windows) {
    for (start, d) in delta {
        let w = base.entry(start).or_default();
        let mut unsourced = d.counts;
        for (source, t) in &d.by_source {
            w.add(t, Some(source));
            unsourced.received = unsourced.received.saturating_sub(t.received);
            unsourced.durable = unsourced.durable.saturating_sub(t.durable);
            unsourced.rejected = unsourced.rejected.saturating_sub(t.rejected);
            unsourced.received_503 = unsourced.received_503.saturating_sub(t.received_503);
        }
        w.add(&unsourced, None);
        w.sent.extend(d.sent);
        let mut unsourced_ts = d.durable_ts;
        for (source, n) in &d.durable_ts_by_source {
            w.add_ts(*n, Some(source));
            unsourced_ts = unsourced_ts.saturating_sub(*n);
        }
        w.add_ts(unsourced_ts, None);
        w.sent_ts.extend(d.sent_ts);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    /// 本进程还没读过该小时的对象：`windows` 只是自启动以来的增量，`replayed` 与 `intents` 待装载时按水位筛选。
    Unloaded,
    /// `windows` 是该小时的完整内容（底稿加此后的全部计入）。
    Loaded,
    /// 格式版本不兼容情形：不覆盖写，计数只在进程内保留。
    Incompatible,
}

struct Hour {
    base: Base,
    windows: Windows,
    /// 重放交回的可重放计入（WAL 位置、计入），装载底稿时只取水位之后的。
    replayed: Vec<(u64, Increment)>,
    /// 重放交回的计数意图 (x, 摘要)，按 WAL 顺序。
    intents: Vec<(u64, Digest)>,
    /// 已计入 `windows` 的可重放计入的最大 WAL 位置。
    through: u64,
    /// 每次变更加一；与 `written` 不等即有待写出的变更。
    changes: u64,
    written: u64,
    /// 最近一次写成的意图 (x, 摘要)。
    confirmed: Option<(u64, Digest)>,
    /// WAL 中该小时的最近一条意图是否就是 `confirmed`。
    confirmed_is_last: bool,
}

impl Hour {
    fn new() -> Hour {
        Hour {
            base: Base::Unloaded,
            windows: Windows::new(),
            replayed: Vec::new(),
            intents: Vec::new(),
            through: 0,
            changes: 0,
            written: 0,
            confirmed: None,
            confirmed_is_last: false,
        }
    }

    fn dirty(&self) -> bool {
        self.changes != self.written
    }
}

type Key = (Dataset, i64);

/// 治理计数：每个数据集小时对象的唯一写者（REQ-AMB-ARCH-CONSTRAINTS 单进程前提）。
pub struct Counters {
    store: Store,
    wal: Arc<dyn Wal>,
    hours: Mutex<HashMap<Key, Hour>>,
}

impl Counters {
    pub fn new(store: Store, wal: Arc<dyn Wal>) -> Counters {
        Counters {
            store,
            wal,
            hours: Mutex::new(HashMap::new()),
        }
    }

    fn with_hour<R>(&self, key: Key, f: impl FnOnce(&mut Hour) -> R) -> R {
        let mut hours = self.hours.lock().expect("计数表未中毒");
        f(hours.entry(key).or_insert_with(Hour::new))
    }

    /// 一个被确认（200）批次的计入：`tally` 的 `received` 为解析出的记录数、`durable` 为 `successful` 数、
    /// `rejected` 为 `failed` 数；`source` 为请求的合法来源标识；`durable_ts` 为被接受的记录按 `_timestamp` 所在分钟
    /// 分组的条数（[`durable_ts_groups`]）；`position` 为承载它们的 [`WalEntry::Counts`] 的位置。
    pub fn record_durable(
        &self,
        dataset: &Dataset,
        window: i64,
        tally: Tally,
        source: Option<&str>,
        durable_ts: &[(i64, u64)],
        position: WalPosition,
    ) {
        let mut hours = self.hours.lock().expect("计数表未中毒");
        for inc in durable_increments(window, tally, source, durable_ts) {
            let h = hours
                .entry((dataset.clone(), hour_of_increment(&inc)))
                .or_insert_with(Hour::new);
            apply(&mut h.windows, &inc);
            h.through = h.through.max(position.0);
            h.changes += 1;
        }
    }

    /// 未获 200 的批次（全部被拒的 400、503）的贡献：只在内存中累加，进程终止时可丢。
    pub fn record_unconfirmed(
        &self,
        dataset: &Dataset,
        window: i64,
        tally: Tally,
        source: Option<&str>,
    ) {
        if tally.is_zero() {
            return;
        }
        let inc = Increment::Counts {
            window,
            tally,
            source: source.map(str::to_owned),
        };
        self.with_hour((dataset.clone(), hour_of(window)), |h| {
            apply(&mut h.windows, &inc);
            h.changes += 1;
        });
    }

    /// 一个被受理的 `_sent` 请求的全部元素（按数组顺序，同一来源与窗口以后出现者为准），`position` 为其 [`WalEntry::Sent`] 的位置。
    pub fn record_sent(&self, dataset: &Dataset, elements: &[SentElement], position: WalPosition) {
        let mut hours = self.hours.lock().expect("计数表未中毒");
        for e in elements {
            let h = hours
                .entry((dataset.clone(), hour_of(e.window_start_us)))
                .or_insert_with(Hour::new);
            apply(
                &mut h.windows,
                &Increment::Sent {
                    window: e.window_start_us,
                    source: e.source.clone(),
                    sent: e.sent,
                    basis: e.basis,
                },
            );
            h.through = h.through.max(position.0);
            h.changes += 1;
        }
    }

    /// 窗口恢复（ADR-0023 第 6 条）：重启时在开始受理之前交回 WAL 中的计数增量、`_sent` 元素与计数意图，
    /// 其余条目忽略。各小时在第一次写出时装载底稿、判定水位，只把水位之后的增量并入。
    ///
    /// 早期版本写下的计数条目不带按记录时间戳的分组（`durable_ts` 为 `None`）：按紧随其后的同一数据集的记录条目
    /// （摄取管道把它们同一次追加、计数条目紧挨在记录条目之前）重算，使升级前已确认的批次同样计入 `durable_ts`。
    pub fn restore(&self, entries: &[(WalPosition, WalEntry)]) {
        let mut hours = self.hours.lock().expect("计数表未中毒");
        for (i, (pos, entry)) in entries.iter().enumerate() {
            match entry {
                WalEntry::Counts {
                    dataset,
                    window_start_us,
                    received,
                    durable,
                    rejected,
                    source,
                    durable_ts,
                } => {
                    let recomputed;
                    let groups = match durable_ts {
                        Some(groups) => groups.as_slice(),
                        None => {
                            recomputed = match entries.get(i + 1) {
                                Some((
                                    next,
                                    WalEntry::Records {
                                        dataset: d,
                                        records,
                                    },
                                )) if next.0 == pos.0 + 1 && d == dataset => {
                                    durable_ts_groups(records.iter().map(|r| r.timestamp_us))
                                }
                                _ => Vec::new(),
                            };
                            recomputed.as_slice()
                        }
                    };
                    let tally = Tally {
                        received: *received,
                        durable: *durable,
                        rejected: *rejected,
                        received_503: 0,
                    };
                    for inc in
                        durable_increments(*window_start_us, tally, source.as_deref(), groups)
                    {
                        let h = hours
                            .entry((dataset.clone(), hour_of_increment(&inc)))
                            .or_insert_with(Hour::new);
                        h.replayed.push((pos.0, inc));
                        h.changes += 1;
                    }
                }
                WalEntry::Sent { dataset, elements } => {
                    for e in elements {
                        let h = hours
                            .entry((dataset.clone(), hour_of(e.window_start_us)))
                            .or_insert_with(Hour::new);
                        h.replayed.push((
                            pos.0,
                            Increment::Sent {
                                window: e.window_start_us,
                                source: e.source.clone(),
                                sent: e.sent,
                                basis: e.basis,
                            },
                        ));
                        h.changes += 1;
                    }
                }
                WalEntry::CountIntent {
                    dataset,
                    hour_start_us,
                    through,
                    digest,
                } => {
                    hours
                        .entry((dataset.clone(), hour_of(*hour_start_us)))
                        .or_insert_with(Hour::new)
                        .intents
                        .push((through.0, *digest));
                }
                _ => {}
            }
        }
        // 只有意图、没有滞留增量的小时无事可做
        hours.retain(|_, h| !h.replayed.is_empty() || h.dirty());
    }

    /// 按 [`TICK`] 写出有变更的计数对象，直到任务被中止。
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TICK).await;
                me.write_due().await;
            }
        })
    }

    /// 停机时把全部有变更的计数对象各写一次（ADR-0023 第 6 条）；写不成的留待下次以同一 `wal.dir` 重启后补齐。
    pub async fn flush_all(&self) {
        self.write_due().await;
    }

    /// 尚有计数贡献未写进计数对象的数据集（优雅退役判定是否已收尾，REQ-AMB-LIFE-RETIREMENT-TERM-SETTLED ②），按数据集
    /// 去重；`true` 即其中有计数对象处于格式版本不兼容情形、写不出的小时。
    pub fn pending(&self) -> Vec<(Dataset, bool)> {
        let hours = self.hours.lock().expect("计数表未中毒");
        let mut out: HashMap<Dataset, bool> = HashMap::new();
        for ((dataset, _), h) in hours.iter() {
            if h.dirty() {
                *out.entry(dataset.clone()).or_default() |= h.base == Base::Incompatible;
            }
        }
        let mut out: Vec<(Dataset, bool)> = out.into_iter().collect();
        out.sort();
        out
    }

    async fn write_due(&self) {
        let due: Vec<Key> = {
            let hours = self.hours.lock().expect("计数表未中毒");
            hours
                .iter()
                .filter(|(_, h)| h.base != Base::Incompatible && h.dirty())
                .map(|(k, _)| k.clone())
                .collect()
        };
        futures::future::join_all(due.into_iter().map(|k| self.write_one(k))).await;
        self.evict();
    }

    /// 写完且早于上一个小时的对象移出内存；迟到的 `_sent` 会让它以未装载状态重新出现、写出前重读底稿。
    fn evict(&self) {
        let now_hour = hour_of(now_us());
        let mut hours = self.hours.lock().expect("计数表未中毒");
        hours.retain(|(_, hour), h| {
            *hour >= now_hour - HOUR_US || h.dirty() || h.base == Base::Incompatible
        });
    }

    async fn write_one(&self, key: Key) {
        let path = self.store.object_path(&key.0, &counters_path(key.1));
        if self.with_hour(key.clone(), |h| h.base == Base::Unloaded) {
            let Some(Ok(fetched)) = timed(self.store.read(&path)).await else {
                return;
            };
            if !self.load(&key, fetched.map(|f| f.bytes)) {
                return;
            }
        }

        // 记计数意图（ADR-0023 第 4 条）
        let (bytes, through, digest, changes, entries) = {
            let mut hours = self.hours.lock().expect("计数表未中毒");
            let Some(h) = hours.get_mut(&key) else {
                return;
            };
            if h.base != Base::Loaded || !h.dirty() {
                return;
            }
            let bytes = encode(&key.0, &h.windows);
            let digest = Digest::of(&bytes);
            let mut entries = Vec::new();
            if let (Some((x, d)), false) = (h.confirmed, h.confirmed_is_last) {
                entries.push(intent(&key, x, d));
            }
            entries.push(intent(&key, h.through, digest));
            (bytes, h.through, digest, h.changes, entries)
        };
        if self.wal.append(entries).await.is_err() {
            return;
        }
        self.with_hour(key.clone(), |h| h.confirmed_is_last = false);

        // 覆盖写之前确认对象当前可以覆盖：不存在，或合规且 `format_version` 为 1
        let Some(Ok(current)) = timed(self.store.read(&path)).await else {
            return;
        };
        let expected: Option<Version> = match current {
            None => None,
            Some(f) if parse(&f.bytes).is_some() => Some(f.version),
            Some(_) => {
                self.incompatible(&key);
                return;
            }
        };
        let Some(Ok(Swapped::Done)) = timed(self.store.swap(&path, expected.as_ref(), bytes)).await
        else {
            return;
        };
        self.with_hour(key.clone(), |h| {
            h.written = h.written.max(changes);
            h.confirmed = Some((through, digest));
            h.confirmed_is_last = true;
        });
        if through > 0 {
            self.wal.release(Release::Counts {
                dataset: key.0.clone(),
                hour_start_us: key.1,
                through: WalPosition(through),
            });
        }
    }

    /// 以读到的对象内容（不存在为 `None`）作底稿：判定水位，把水位之后的重放增量与自启动以来的增量并上去。
    /// 底稿不合规时转入格式版本不兼容情形并返回 `false`。
    fn load(&self, key: &Key, object: Option<Bytes>) -> bool {
        let base = match &object {
            None => Some(Windows::new()),
            Some(bytes) => parse(bytes),
        };
        let Some(mut windows) = base else {
            self.incompatible(key);
            return false;
        };
        let digest = object.as_deref().map(Digest::of);
        let watermark = {
            let mut hours = self.hours.lock().expect("计数表未中毒");
            let Some(h) = hours.get_mut(key) else {
                return false;
            };
            if h.base != Base::Unloaded {
                return h.base == Base::Loaded;
            }
            let matched = digest.and_then(|d| h.intents.iter().rev().find(|(_, i)| *i == d));
            let watermark = matched.map_or(0, |(x, _)| *x);
            h.confirmed = matched.copied();
            h.confirmed_is_last = false;
            let mut applied = false;
            for (pos, inc) in &h.replayed {
                if *pos > watermark {
                    apply(&mut windows, inc);
                    h.through = h.through.max(*pos);
                    applied = true;
                }
            }
            let live = std::mem::take(&mut h.windows);
            applied |= !live.is_empty();
            merge(&mut windows, live);
            h.windows = windows;
            h.through = h.through.max(watermark);
            h.replayed.clear();
            h.intents.clear();
            h.base = Base::Loaded;
            if !applied {
                h.written = h.changes;
            }
            watermark
        };
        if watermark > 0 {
            self.wal.release(Release::Counts {
                dataset: key.0.clone(),
                hour_start_us: key.1,
                through: WalPosition(watermark),
            });
        }
        true
    }

    fn incompatible(&self, key: &Key) {
        self.with_hour(key.clone(), |h| {
            h.base = Base::Incompatible;
            h.replayed.clear();
            h.intents.clear();
        });
        self.wal.release(Release::Counts {
            dataset: key.0.clone(),
            hour_start_us: key.1,
            through: WalPosition::MAX,
        });
    }
}

/// 治理视图读回计数对象时，按小时逐个直读的上限：查询范围跨越更多小时时改为列出该数据集的计数对象再读
/// （范围可以很宽，逐小时直读的请求数随之增长，列举只读实际存在的对象）。
const DIRECT_READ_HOURS: i64 = 48;

/// 已持久化的计数对象中的一个分钟窗口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedWindow {
    pub window_start: i64,
    pub window_end: i64,
    pub received: u64,
    pub durable: u64,
    /// 各来源按送达的发送数（按来源名的 UTF-8 字节序）。
    pub sent: BTreeMap<String, u64>,
    /// 按记录时间戳落在该窗口的确认数。
    pub durable_ts: u64,
    /// `durable_ts` 中带来源标识的部分，按来源。
    pub durable_ts_by_source: BTreeMap<String, u64>,
    /// 各来源按时间戳的发送数。
    pub sent_ts: BTreeMap<String, u64>,
}

fn persisted(start: i64, w: Window) -> PersistedWindow {
    PersistedWindow {
        window_start: start,
        window_end: start + MINUTE_US,
        received: w.counts.received,
        durable: w.counts.durable,
        sent: w.sent,
        durable_ts: w.durable_ts,
        durable_ts_by_source: w.durable_ts_by_source,
        sent_ts: w.sent_ts,
    }
}

/// 一个小时的计数：小时起点，与该小时的窗口或读不出的原因（[`Counters::current_hours`]）。
pub type HourWindows = (i64, Result<Vec<PersistedWindow>, ReadError>);

/// 读回计数对象的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）。
    Unavailable(String),
    /// `format_version` 不为 1 或内容无法解析（REQ-AMB-OBS-INGEST-COUNTERS 的界定）。
    Unparseable(String),
}

impl Counters {
    /// 对象存储中 `dataset` 已写出的计数对象里 `window_start` 落在 `[start, end)` 内的全部窗口，按 `window_start` 升序。
    /// 每次调用都重读，不经内存中的计数；某小时的计数对象不存在时该小时没有窗口。
    pub async fn read_persisted(
        &self,
        dataset: &Dataset,
        start: i64,
        end: i64,
    ) -> Result<Vec<PersistedWindow>, ReadError> {
        if end <= start {
            return Ok(Vec::new());
        }
        let (first, last) = (hour_of(start), hour_of(end - 1));
        let hours: Vec<i64> = if (last - first) / HOUR_US < DIRECT_READ_HOURS {
            (0..=(last - first) / HOUR_US)
                .map(|i| first + i * HOUR_US)
                .collect()
        } else {
            self.listed_hours(dataset)
                .await?
                .into_iter()
                .filter(|h| (first..=last).contains(h))
                .collect()
        };
        let objects = futures::future::try_join_all(hours.into_iter().map(|hour| async move {
            let path = self.store.object_path(dataset, &counters_path(hour));
            let fetched = self
                .store
                .read(&path)
                .await
                .map_err(|e| ReadError::Unavailable(e.to_string()))?;
            match fetched {
                None => Ok(None),
                Some(f) => parse(&f.bytes).map(Some).ok_or_else(|| {
                    ReadError::Unparseable(format!(
                        "the counter object {path} has an unsupported format_version or cannot be parsed"
                    ))
                }),
            }
        }))
        .await?;
        let mut out: Vec<PersistedWindow> = objects
            .into_iter()
            .flatten()
            .flatten()
            .filter(|(w, _)| (start..end).contains(w))
            .map(|(w, c)| persisted(w, c))
            .collect();
        out.sort_by_key(|w| w.window_start);
        Ok(out)
    }

    /// 后台判定所用的计数（REQ-AMB-GOV-MISSED-COLLECTION-BG-COUNTS）：`[start, end)` 所跨的每个小时各一项，为该小时计数
    /// 对象中已写出的计数加上本进程已计入、尚未写出的计数，只含 `window_start` 落在 `[start, end)` 内的窗口。
    ///
    /// 每个小时都重读对象：读不出（不可访问、版本过高或内容无法解析）的小时给出错误，不沿用内存里的内容——
    /// 判定要按「所需的计数对象读不出即略去」作答。读得出时：本进程已装载底稿的小时，内存中的内容就是底稿加此后全部
    /// 计入（本进程是唯一写者）；尚未装载的，照写出前装载底稿的做法按对象摘要找水位、只并入水位之后的重放增量，
    /// 再并上自启动以来的计入。不改动内存中的计数。
    pub async fn current_hours(&self, dataset: &Dataset, start: i64, end: i64) -> Vec<HourWindows> {
        if end <= start {
            return Vec::new();
        }
        let (first, last) = (hour_of(start), hour_of(end - 1));
        let hours: Vec<i64> = (0..=(last - first) / HOUR_US)
            .map(|i| first + i * HOUR_US)
            .collect();
        let objects = futures::future::join_all(hours.iter().map(|&hour| async move {
            let path = self.store.object_path(dataset, &counters_path(hour));
            match self.store.read(&path).await {
                Err(e) => Err(ReadError::Unavailable(e.to_string())),
                Ok(None) => Ok((Windows::new(), None)),
                Ok(Some(f)) => match parse(&f.bytes) {
                    Some(w) => Ok((w, Some(Digest::of(&f.bytes)))),
                    None => Err(ReadError::Unparseable(format!(
                        "the counter object {path} has an unsupported format_version or cannot be parsed"
                    ))),
                },
            }
        }))
        .await;
        let mem = self.hours.lock().expect("计数表未中毒");
        hours
            .into_iter()
            .zip(objects)
            .map(|(hour, object)| {
                let windows = object.map(|(base, digest)| {
                    let windows = match mem.get(&(dataset.clone(), hour)) {
                        None => base,
                        Some(h) if h.base == Base::Loaded => h.windows.clone(),
                        Some(h) => {
                            let mut windows = base;
                            let watermark = digest
                                .and_then(|d| h.intents.iter().rev().find(|(_, i)| *i == d))
                                .map_or(0, |(x, _)| *x);
                            for (pos, inc) in &h.replayed {
                                if *pos > watermark {
                                    apply(&mut windows, inc);
                                }
                            }
                            merge(&mut windows, h.windows.clone());
                            windows
                        }
                    };
                    windows
                        .into_iter()
                        .filter(|(w, _)| (start..end).contains(w))
                        .map(|(w, c)| persisted(w, c))
                        .collect()
                });
                (hour, windows)
            })
            .collect()
    }

    /// 对象存储中 `dataset` 现有计数对象所属的各小时（只认 [`counters_path`] 的写法）。
    async fn listed_hours(&self, dataset: &Dataset) -> Result<Vec<i64>, ReadError> {
        let prefix = self.store.object_path(dataset, "counters");
        let listed = self
            .store
            .list(&prefix)
            .await
            .map_err(|e| ReadError::Unavailable(e.to_string()))?;
        let under = format!("{prefix}/");
        Ok(listed
            .iter()
            .filter_map(|meta| {
                let rel = meta.location.as_ref().strip_prefix(&under)?;
                let label: String = rel.strip_suffix(".json")?.split('/').collect();
                let hour = amberlog_storage::layout::HourBucket::parse(&label)?.start_us();
                (counters_path(hour) == format!("counters/{rel}")).then_some(hour)
            })
            .collect())
    }
}

fn intent(key: &Key, through: u64, digest: Digest) -> WalEntry {
    WalEntry::CountIntent {
        dataset: key.0.clone(),
        hour_start_us: key.1,
        through: WalPosition(through),
        digest,
    }
}

/// 超时的请求记为失败（`None`）。
async fn timed<T>(f: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::time::timeout(REQUEST_TIMEOUT, f).await.ok()
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// 计数对象相对数据集前缀的路径：`counters/<YYYY>/<MM>/<DD>/<HH>.json`。
pub fn counters_path(hour_start_us: i64) -> String {
    let label = amberlog_storage::layout::HourBucket::of(hour_start_us).to_string();
    format!(
        "counters/{}/{}/{}/{}.json",
        &label[0..4],
        &label[4..6],
        &label[6..8],
        &label[8..10]
    )
}

fn tally_json(t: &Tally) -> Value {
    json!({
        "received": t.received,
        "durable": t.durable,
        "rejected": t.rejected,
        "received_503": t.received_503,
    })
}

fn counts_json(counts: &BTreeMap<String, u64>) -> Value {
    Value::Object(
        counts
            .iter()
            .map(|(s, n)| (s.clone(), Value::from(*n)))
            .collect(),
    )
}

/// 计数对象的内容（`format_version` 1）：每个窗口都写 `rejected`、`received_503`、`durable_ts` 与 `sent_ts`，
/// 有来源时写 `by_source`，有按来源的按时间戳确认数时写 `durable_ts_by_source`。
fn encode(dataset: &Dataset, windows: &Windows) -> Bytes {
    let windows: Vec<Value> = windows
        .iter()
        .map(|(start, w)| {
            let mut entry = json!({
                "window_start": start,
                "window_end": start + MINUTE_US,
                "received": w.counts.received,
                "durable": w.counts.durable,
                "rejected": w.counts.rejected,
                "received_503": w.counts.received_503,
                "sent": counts_json(&w.sent),
                "durable_ts": w.durable_ts,
                "sent_ts": counts_json(&w.sent_ts),
            });
            if !w.durable_ts_by_source.is_empty() {
                entry["durable_ts_by_source"] = counts_json(&w.durable_ts_by_source);
            }
            if !w.by_source.is_empty() {
                let by_source: Map<String, Value> = w
                    .by_source
                    .iter()
                    .map(|(s, t)| (s.clone(), tally_json(t)))
                    .collect();
                entry["by_source"] = Value::Object(by_source);
            }
            entry
        })
        .collect();
    let body = json!({
        "format_version": 1,
        "dataset": dataset.to_string(),
        "windows": windows,
    });
    Bytes::from(serde_json::to_vec(&body).expect("JSON 值总能序列化"))
}

/// 解析计数对象；`format_version` 不为 1、不是合法 JSON 或字段与类型不合规时为 `None`（格式版本不兼容情形）。
/// 缺少的 `rejected`、`received_503`、`durable_ts` 读作 0，缺少 `sent_ts` 读作空对象，缺少 `by_source`、
/// `durable_ts_by_source` 读作没有来源；计数大于 2^64−1（`as_u64` 不成立）、`by_source` 或 `durable_ts_by_source`
/// 多于 [`MAX_SOURCES`] 个来源都属不合规。未定义的字段忽略（REQ-AMB-STO-HOT-LAYOUT 向前兼容）。
fn parse(bytes: &[u8]) -> Option<Windows> {
    let v: Value = serde_json::from_slice(bytes).ok()?;
    let obj = v.as_object()?;
    if obj.get("format_version")?.as_u64()? != 1 {
        return None;
    }
    obj.get("dataset")?.as_str()?;
    let mut windows = Windows::new();
    for w in obj.get("windows")?.as_array()? {
        let w = w.as_object()?;
        let start = w.get("window_start")?.as_i64()?;
        let end = w.get("window_end")?.as_i64()?;
        if start < 0 || start % MINUTE_US != 0 || end != start.checked_add(MINUTE_US)? {
            return None;
        }
        let sent = parse_counts(w.get("sent")?)?;
        let sent_ts = match w.get("sent_ts") {
            None => BTreeMap::new(),
            Some(v) => parse_counts(v)?,
        };
        let durable_ts = match w.get("durable_ts") {
            None => 0,
            Some(v) => v.as_u64()?,
        };
        let durable_ts_by_source = match w.get("durable_ts_by_source") {
            None => BTreeMap::new(),
            Some(v) => parse_counts(v).filter(|m| m.len() <= MAX_SOURCES)?,
        };
        let mut by_source = BTreeMap::new();
        if let Some(sources) = w.get("by_source") {
            let sources = sources.as_object()?;
            if sources.len() > MAX_SOURCES {
                return None;
            }
            for (source, t) in sources {
                by_source.insert(source.clone(), parse_tally(t.as_object()?)?);
            }
        }
        let window = Window {
            counts: parse_tally(w)?,
            by_source,
            sent,
            durable_ts,
            durable_ts_by_source,
            sent_ts,
        };
        if windows.insert(start, window).is_some() {
            return None;
        }
    }
    Some(windows)
}

/// 来源 → 计数的对象。
fn parse_counts(v: &Value) -> Option<BTreeMap<String, u64>> {
    v.as_object()?
        .iter()
        .map(|(source, n)| Some((source.clone(), n.as_u64()?)))
        .collect()
}

/// 四项计数：`received`、`durable` 必有，`rejected`、`received_503` 缺少时读作 0。
fn parse_tally(o: &Map<String, Value>) -> Option<Tally> {
    let optional = |name: &str| match o.get(name) {
        None => Some(0),
        Some(v) => v.as_u64(),
    };
    Some(Tally {
        received: o.get("received")?.as_u64()?,
        durable: o.get("durable")?.as_u64()?,
        rejected: optional("rejected")?,
        received_503: optional("received_503")?,
    })
}

/// `_sent` 请求体的拒绝原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SentError {
    /// 不是合法 JSON（400 `invalid_json`）。
    InvalidJson,
    /// 顶层不是数组，或某个元素不合规（400 `invalid_request`，整个请求不记录任何元素）。
    InvalidRequest(String),
}

/// 解析并校验 `_sent` 请求体（REQ-AMB-OBS-INGEST-COUNTERS「请求体解析与元素校验」）。
/// 元素按数组顺序返回；同一 `(source, window_start, basis)` 的「以最后出现者为准」由计入时的覆盖语义实现。
pub fn parse_sent(body: &[u8]) -> Result<Vec<SentElement>, SentError> {
    use amberlog_common::json::{parse_body, protocol_integer};
    let value = parse_body(body).map_err(|_| SentError::InvalidJson)?;
    let invalid = |why: String| SentError::InvalidRequest(why);
    let Value::Array(items) = value else {
        return Err(invalid("the request body must be a JSON array".into()));
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let Value::Object(e) = item else {
            return Err(invalid(format!("element {i} is not a JSON object")));
        };
        let int = |name: &str| -> Result<i64, SentError> {
            e.get(name).and_then(protocol_integer).ok_or_else(|| {
                invalid(format!(
                    "element {i}: `{name}` is missing or is not an integer"
                ))
            })
        };
        let source = match e.get("source") {
            Some(Value::String(s)) => s,
            _ => {
                return Err(invalid(format!(
                    "element {i}: `source` is missing or is not a string"
                )));
            }
        };
        let window_start = int("window_start")?;
        let window_end = int("window_end")?;
        // `sent` 可到 2^64−1，按 u64 取；负数与大于 2^64−1 的整数在下面的取值检查中拒绝（REQ-AMB-OBS-INGEST-COUNTERS-V2）
        let sent = match e.get("sent").and_then(Value::as_number) {
            Some(n) if integer_literal(n.as_str()) => n.as_u64(),
            _ => {
                return Err(invalid(format!(
                    "element {i}: `sent` is missing or is not an integer"
                )));
            }
        };
        if !valid_source(source) {
            return Err(invalid(format!(
                "element {i}: `source` must match ^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$"
            )));
        }
        let Some(sent) = sent.filter(|_| window_start >= 0) else {
            return Err(invalid(format!(
                "element {i}: `window_start` and `sent` must not be negative, and `sent` must not be greater than 18446744073709551615"
            )));
        };
        if window_start > MAX_SENT_WINDOW_START {
            return Err(invalid(format!(
                "element {i}: `window_start` must not be later than 9999-12-31T23:59:00Z"
            )));
        }
        if window_start % MINUTE_US != 0 || window_start.checked_add(MINUTE_US) != Some(window_end)
        {
            return Err(invalid(format!(
                "element {i}: the window must be one whole UTC minute (window_end = window_start + 60000000)"
            )));
        }
        // `basis` 缺省为按送达；其他取值、非字符串与 JSON null 都拒绝整个请求（REQ-AMB-OBS-INGEST-COUNTERS-V3）
        let basis = match e.get("basis") {
            None => Basis::Delivery,
            Some(Value::String(b)) if b == "delivery" => Basis::Delivery,
            Some(Value::String(b)) if b == "timestamp" => Basis::Timestamp,
            Some(_) => {
                return Err(invalid(format!(
                    "element {i}: `basis` must be \"delivery\" or \"timestamp\""
                )));
            }
        };
        out.push(SentElement {
            source: source.clone(),
            window_start_us: window_start,
            sent,
            basis,
        });
    }
    Ok(out)
}

/// 数字原文是不带小数点与指数的整数写法（arbitrary_precision 下 `Number::as_str` 是数字原文）。
fn integer_literal(literal: &str) -> bool {
    let digits = literal.strip_prefix('-').unwrap_or(literal);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// 来源标识的取值规则 `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`：`_sent` 元素的 `source` 与 `_json` 的
/// `X-Amberlog-Source` 请求头共用（REQ-AMB-OBS-INGEST-COUNTERS-V2、REQ-AMB-ING-JSON-ENDPOINT-V3）。
pub fn valid_source(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 128
        && b[0].is_ascii_alphanumeric()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

#[cfg(test)]
mod tests;
