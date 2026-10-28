//! 线程分配区里释放掉的内存还给系统：[`memory::fix_trim_threshold`] 之后，即使先释放过一块 mmap 分配的大块，
//! 工作线程上释放的内存也随即还给系统，匿名常驻内存不再停在它的高水位上。
//! 只在 glibc Linux 上有意义；本测试二进制只有这一个测试，独占进程的分配器状态。

#![cfg(all(target_os = "linux", target_env = "gnu"))]

use amberlog_storage::memory;

/// `/proc/self/status` 的 `RssAnon`（字节）。
fn rss_anon() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("RssAnon:")).unwrap();
    line.split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024
}

#[test]
fn freed_thread_arena_memory_returns_to_system() {
    memory::fix_trim_threshold();
    // 释放一块 16 MiB 的 mmap 分配：glibc 缺省会据此把 trim 阈值抬到 32 MiB
    drop(std::hint::black_box(vec![1u8; 16 << 20]));
    let before = rss_anon();
    // 工作线程（非主分配区）分配 24 MiB 小块、写满后全部释放，再整理一次（同查询与后台任务之后的归还）
    std::thread::spawn(|| {
        let blocks: Vec<Vec<u8>> = (0..384).map(|_| vec![1u8; 64 << 10]).collect();
        drop(std::hint::black_box(blocks));
    })
    .join()
    .unwrap();
    // SAFETY: malloc_trim 只整理分配器自己的状态。
    unsafe { libc::malloc_trim(0) };
    let after = rss_anon();
    assert!(
        after < before + (4 << 20),
        "工作线程释放的 24 MiB 没有还给系统：RssAnon {before} → {after}"
    );
}
