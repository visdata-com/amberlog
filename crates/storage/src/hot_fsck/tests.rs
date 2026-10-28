//! 巡检缓存的紧凑表示与按头片还原的结果一致：随机生成一个桶的清单片历史（增量片、全量片、留痕链，含先删后又加回的
//! 文件与 `orphan` 留痕），对任取的一组可达头片，比较缓存所得与 [`chain::restore`] 逐个还原所得。

use std::collections::{BTreeSet, HashMap, HashSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::*;
use crate::chain::{Deletion, FileEntry, PieceVisit};
use crate::format::FORMAT_VERSION;

const B: &str = "2026082210";

fn piece_path(n: usize) -> String {
    format!("manifest/buckets/{B}-01k6{n:022}")
}

fn file(n: usize) -> FileEntry {
    FileEntry {
        path: format!("data/2026/08/22/10/01k7{n:022}.parquet"),
        min_timestamp: 1,
        max_timestamp: 2,
        record_count: 1,
        size_bytes: 10,
        content_digest: Digest::of(&n.to_le_bytes()),
        added_in: SnapshotId(1),
        column_stats: None,
    }
}

fn tomb(path: &str, reason: DeletionReason, at: i64) -> Deletion {
    Deletion {
        path: path.to_owned(),
        content_digest: Digest::of(path.as_bytes()),
        reason,
        added_in: None,
        max_timestamp: None,
        cutoff: None,
        deleted_at: at,
        removed_in: SnapshotId(1),
    }
}

/// 一个桶的清单片历史。
#[derive(Default)]
struct History {
    pieces: HashMap<String, Piece>,
    /// 依次写出的头片。
    heads: Vec<String>,
    /// 全量片及其留痕链的片数。
    fulls: Vec<(String, usize)>,
}

impl History {
    fn generate(rng: &mut StdRng, versions: usize) -> History {
        let mut h = History::default();
        let mut live: Vec<FileEntry> = Vec::new();
        let mut removed: Vec<String> = Vec::new();
        let mut next_file = 0usize;
        // 当前头片的增量链片数（含基准片）
        let mut delta_len = 0usize;
        for v in 0..versions {
            let at = v as i64;
            let mut deletions = Vec::new();
            // 删掉若干存活文件
            let mut i = 0;
            while i < live.len() {
                if rng.random_bool(0.15) {
                    let f = live.swap_remove(i);
                    let reason = match rng.random_range(0..3) {
                        0 => DeletionReason::Orphan,
                        1 => DeletionReason::Lake,
                        _ => DeletionReason::Compaction,
                    };
                    deletions.push(tomb(&f.path, reason, at));
                    removed.push(f.path);
                } else {
                    i += 1;
                }
            }
            // 给从未加入过的路径记 `orphan` 留痕
            if rng.random_bool(0.2) {
                next_file += 1;
                let f = file(next_file);
                deletions.push(tomb(&f.path, DeletionReason::Orphan, at));
                removed.push(f.path);
            }
            let mut added = Vec::new();
            for _ in 0..rng.random_range(0..4) {
                next_file += 1;
                added.push(file(next_file));
            }
            // 偶尔把删掉过的路径又加回来
            if !removed.is_empty() && rng.random_bool(0.1) {
                let p = removed[rng.random_range(0..removed.len())].clone();
                if !live.iter().chain(&added).any(|f| f.path == p) {
                    let mut f = file(0);
                    f.path = p;
                    added.push(f);
                }
            }
            live.retain(|f| !deletions.iter().any(|d| d.path == f.path));
            live.extend(added.iter().cloned());

            let path = piece_path(v);
            let full = h.heads.is_empty() || delta_len >= CHAIN_LIMIT || rng.random_bool(0.08);
            let piece = if full {
                let candidates: Vec<&(String, usize)> =
                    h.fulls.iter().filter(|(_, n)| *n < CHAIN_LIMIT).collect();
                let ledger = if !candidates.is_empty() && rng.random_bool(0.6) {
                    Some(candidates[rng.random_range(0..candidates.len())].clone())
                } else {
                    None
                };
                h.fulls
                    .push((path.clone(), ledger.as_ref().map_or(1, |(_, n)| n + 1)));
                delta_len = 1;
                Piece {
                    format_version: FORMAT_VERSION,
                    bucket: B.into(),
                    link: PieceLink::Full {
                        ledger_previous: ledger.map(|(p, _)| PieceRef {
                            path: p,
                            content_digest: Digest::of(b""),
                        }),
                    },
                    files: live.clone(),
                    deletions,
                    handovers: Vec::new(),
                }
            } else {
                delta_len += 1;
                Piece {
                    format_version: FORMAT_VERSION,
                    bucket: B.into(),
                    link: PieceLink::Delta {
                        previous: PieceRef {
                            path: h.heads.last().unwrap().clone(),
                            content_digest: Digest::of(b""),
                        },
                    },
                    files: added,
                    deletions,
                    handovers: Vec::new(),
                }
            };
            h.pieces.insert(path.clone(), piece);
            h.heads.push(path);
        }
        h
    }

    /// 自头片 `head` 起按增量链、留痕链读到的清单片（不核对摘要）。
    fn walk(&self, head: &str) -> PieceWalk {
        let mut walk = PieceWalk::default();
        let mut next = Some(head.to_owned());
        while let Some(path) = next.take() {
            let piece = self.pieces[&path].clone();
            next = match &piece.link {
                PieceLink::Delta { previous } => Some(previous.path.clone()),
                PieceLink::Full { ledger_previous } => {
                    ledger_previous.as_ref().map(|r| r.path.clone())
                }
            };
            walk.visits.push(PieceVisit {
                reference: PieceRef {
                    path,
                    content_digest: Digest::of(b""),
                },
                bytes: None,
                piece: Some(piece),
                problem: None,
            });
        }
        walk
    }

    fn restore(&self, head: &str) -> Restored {
        chain::restore(&self.walk(head), FORMAT_VERSION, B).unwrap()
    }
}

#[test]
fn compact_cache_matches_restore() {
    for seed in 0..40u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let h = History::generate(&mut rng, 300);
        let mut known = Known::default();
        for head in &h.heads {
            known.add_pieces(&h.walk(head));
        }

        // 还原：files（先后不作承诺）与 deletions（同序）
        for head in &h.heads {
            let r = h.restore(head);
            let (files, tombs) = known.restore_facts(head).unwrap();
            let got: BTreeSet<&str> = files.iter().map(|f| &**f).collect();
            let want: BTreeSet<&str> = r.manifest.files.iter().map(|f| f.path.as_str()).collect();
            assert_eq!(got, want, "seed {seed} {head}");
            assert_eq!(files.len(), r.manifest.files.len(), "seed {seed} {head}");
            let got: Vec<Tomb> = tombs.into_iter().cloned().collect();
            let want: Vec<Tomb> = r
                .manifest
                .deletions
                .iter()
                .map(|d| Tomb {
                    path: Arc::from(d.path.as_str()),
                    reason: d.reason,
                    deleted_at: d.deleted_at,
                })
                .collect();
            assert_eq!(got, want, "seed {seed} {head}");
        }

        // 任取一组可达头片：较新的一段，另加零散的较旧的
        let from = rng.random_range(0..h.heads.len());
        let reachable: Vec<&String> = h
            .heads
            .iter()
            .enumerate()
            .filter(|(i, _)| *i >= from || rng.random_bool(0.05))
            .map(|(_, p)| p)
            .collect();
        let mut reach = Reach::default();
        reach
            .manifests
            .extend(reachable.iter().map(|p| Arc::from(p.as_str())));
        assert!(known.reach_pieces(&mut reach));

        let mut pieces: HashSet<String> = HashSet::new();
        let mut files: HashSet<String> = HashSet::new();
        let mut deleted: HashSet<String> = HashSet::new();
        for head in &reachable {
            let r = h.restore(head);
            pieces.extend(r.pieces.iter().cloned());
            files.extend(r.manifest.files.iter().map(|f| f.path.clone()));
            deleted.extend(r.manifest.deletions.iter().map(|d| d.path.clone()));
        }
        let strings = |s: &HashSet<Arc<str>>| -> HashSet<String> {
            s.iter().map(|p| p.to_string()).collect()
        };
        assert_eq!(strings(&reach.pieces), pieces, "seed {seed}");
        assert_eq!(strings(&reach.deleted), deleted, "seed {seed}");
        let over = strings(&reach.files);
        assert!(over.is_superset(&files), "seed {seed}");
        assert!(
            over.difference(&files).all(|p| deleted.contains(p)),
            "seed {seed}"
        );
        // 精确判定与逐个还原所得的并集一致
        for p in over.iter().chain(&deleted) {
            assert_eq!(
                known.in_files(&reach.manifests, p),
                files.contains(p),
                "seed {seed} {p}"
            );
        }
        // 多出的确实出现过（否则这个测试测不到精确判定）
        if seed == 0 {
            assert!(over.len() > files.len(), "seed {seed}");
        }
    }
}

#[test]
fn diff_turns_child_into_parent() {
    let head = |b: &str, p: &str| {
        Arc::new(Head {
            bucket: Arc::from(b),
            path: Arc::from(p),
            digest: Digest::of(p.as_bytes()),
        })
    };
    let shared = head("2026082209", "a");
    let child: Heads = [
        (Arc::from("2026082209"), Arc::clone(&shared)),
        (Arc::from("2026082210"), head("2026082210", "b2")),
        (Arc::from("2026082212"), head("2026082212", "d")),
    ]
    .into_iter()
    .collect();
    let parent: Heads = [
        (Arc::from("2026082209"), Arc::clone(&shared)),
        (Arc::from("2026082210"), head("2026082210", "b1")),
        (Arc::from("2026082211"), head("2026082211", "c")),
    ]
    .into_iter()
    .collect();
    let mut heads = child.clone();
    for (b, old) in diff(&child, &parent) {
        match old {
            Some(h) => heads.insert(b, h),
            None => heads.remove(&b),
        };
    }
    assert_eq!(heads, parent);
    assert_eq!(diff(&child, &child), Vec::new());
}
