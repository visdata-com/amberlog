//! 把分配器的空闲内存还给系统。
//!
//! glibc malloc 按线程分配区保留用过的内存、不还给系统。查询与后台的封口合并、入湖、冷层合并都在 tokio 工作线程上
//! 解码大量小块的批次，用完释放后留在各线程的分配区里；下一次多半落在另一个工作线程、另一个分配区上，匿名常驻内存
//! 便一级一级涨到各分配区高水位之和（REQ-AMB-QRY-RESOURCE-LIMITS-V2 RSS-NO-GROWTH 在查询一侧所见；封口合并一侧
//! 实测 38 次合并后涨到 36 GB、一次不回落）。只限制分配区个数不够，空闲内存卡在各分配区的堆中间，要 `malloc_trim(0)`
//! 才能还回去。
//!
//! 进程启动时 [`fix_trim_threshold`] 固定 trim 阈值，各线程分配区释放内存时随即收缩堆顶（`malloc_trim(0)` 收缩不了它们）。
//! 查询释放执行名额时、上述三种后台任务每做完一次时调用 [`release_if_much_free`]。空闲内存不多时不归还：归还之后
//! 下一次要重新向系统申请内存页，每次都归还会拖慢首尾相接的重查询。

/// 把 glibc 的 trim 阈值固定为 [`TRIM_THRESHOLD`]，进程启动时调用一次。
///
/// glibc 缺省按用过的 mmap 大块动态抬高 trim 阈值（至多 64 MiB）。抬高之后，各线程分配区堆顶的空闲内存不到阈值就不
/// 收缩，而 `malloc_trim(0)` 只收缩主分配区的堆顶，所以 [`release_if_much_free`] 也还不回去：每个用过的分配区
/// 都留着至多 64 MiB，匿名常驻内存随封口合并、巡检等后台任务一级一级抬高，直到分配区个数乘 64 MiB。固定阈值以后
/// 动态调整随之关闭（mmap 阈值也不再抬高，停在 128 KiB），各分配区释放时就收缩堆顶。代价是更常向系统申请和归还内存页
/// （实测摄取与合并的 CPU 时间在同机多轮的波动之内，小查询慢约 10～20 ms）。环境变量 `GLIBC_TUNABLES` 已设
/// `glibc.malloc.trim_threshold` 时不改，以它为准。其他平台与分配器上是空函数。
pub fn fix_trim_threshold() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let tuned = std::env::var_os("GLIBC_TUNABLES")
            .is_some_and(|v| v.to_string_lossy().contains("glibc.malloc.trim_threshold"));
        if !tuned {
            // SAFETY: mallopt 只改分配器参数；在启动之初、其他线程开始大量分配之前调用。
            unsafe { libc::mallopt(libc::M_TRIM_THRESHOLD, TRIM_THRESHOLD) };
        }
    }
}

/// [`fix_trim_threshold`] 所设的 trim 阈值：glibc 的缺省初值（128 KiB）。
pub const TRIM_THRESHOLD: i32 = 128 << 10;

/// 分配器各分配区的空闲内存合计超过它就归还（约为两个默认单查询内存上限）。
pub const FREE_BEFORE_RELEASE: usize = 256 << 20;

/// 分配器各分配区的空闲内存合计（glibc `mallinfo2` 的 `fordblks`）超过 [`FREE_BEFORE_RELEASE`] 就归还一次
/// （`malloc_trim(0)`）；glibc 没有 `mallinfo2` 时不看空闲多少，直接归还。查看与归还都放到阻塞线程池里做，不占调用方的
/// 线程；上一次还没做完时不再发起（全进程一个）。不在 tokio 运行时之内时不做。其他平台与分配器上是空函数。
pub fn release_if_much_free() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        use std::sync::atomic::{AtomicBool, Ordering};
        static RELEASING: AtomicBool = AtomicBool::new(false);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if RELEASING.swap(true, Ordering::SeqCst) {
            return;
        }
        runtime.spawn_blocking(|| {
            glibc::release(glibc::mallinfo2());
            RELEASING.store(false, Ordering::SeqCst);
        });
    }
}

/// glibc 的 `mallinfo2` 从 2.33 才有。发版二进制要在 glibc 2.28 上运行，所以不直接链接它，运行时按名字查找；
/// 旧的 `mallinfo` 字段是 `int`，空闲超过 2 GiB 就溢出，不用来判阈值。
#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod glibc {
    use std::sync::OnceLock;

    pub(super) type Mallinfo2 = unsafe extern "C" fn() -> libc::mallinfo2;

    /// 运行时所在 glibc 的 `mallinfo2`，没有时为 `None`（只查找一次）。
    pub(super) fn mallinfo2() -> Option<Mallinfo2> {
        static FOUND: OnceLock<Option<Mallinfo2>> = OnceLock::new();
        *FOUND.get_or_init(|| {
            // SAFETY: 名字是以 NUL 结尾的静态字符串；RTLD_DEFAULT 按全局符号查找顺序查找，不需要句柄。
            let sym = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"mallinfo2".as_ptr()) };
            // SAFETY: 查到的 `mallinfo2` 就是 glibc 按 `struct mallinfo2 mallinfo2(void)` 导出的函数。
            (!sym.is_null())
                .then(|| unsafe { std::mem::transmute::<*mut libc::c_void, Mallinfo2>(sym) })
        })
    }

    /// 有 `mallinfo2` 时空闲合计超过 [`super::FREE_BEFORE_RELEASE`] 才归还，没有时直接归还。返回是否归还了。
    pub(super) fn release(mallinfo2: Option<Mallinfo2>) -> bool {
        // SAFETY: mallinfo2 与 malloc_trim 只读取、整理分配器自己的状态，没有入参指针，可在任意线程调用。
        unsafe {
            if let Some(mallinfo2) = mallinfo2
                && mallinfo2().fordblks <= super::FREE_BEFORE_RELEASE
            {
                return false;
            }
            libc::malloc_trim(0);
        }
        true
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn info(fordblks: usize) -> libc::mallinfo2 {
            // SAFETY: mallinfo2 全是整数字段，全零是有效值。
            let mut m: libc::mallinfo2 = unsafe { std::mem::zeroed() };
            m.fordblks = fordblks;
            m
        }
        unsafe extern "C" fn little_free() -> libc::mallinfo2 {
            info(super::super::FREE_BEFORE_RELEASE)
        }
        unsafe extern "C" fn much_free() -> libc::mallinfo2 {
            info(super::super::FREE_BEFORE_RELEASE + 1)
        }

        #[test]
        fn release_checks_threshold_when_mallinfo2_found() {
            assert!(!release(Some(little_free)));
            assert!(release(Some(much_free)));
        }

        #[test]
        fn release_trims_without_check_when_mallinfo2_missing() {
            assert!(release(None));
        }

        /// 查找结果与运行时所在 glibc 的版本相符：2.33 及以上查得到，以下查不到。
        #[test]
        fn mallinfo2_found_iff_glibc_has_it() {
            // SAFETY: 返回 glibc 内部以 NUL 结尾的静态版本字符串。
            let version = unsafe { std::ffi::CStr::from_ptr(libc::gnu_get_libc_version()) };
            let mut parts = version
                .to_str()
                .unwrap()
                .split('.')
                .map(|p| p.parse::<u32>().unwrap());
            let (major, minor) = (parts.next().unwrap(), parts.next().unwrap());
            assert_eq!(
                mallinfo2().is_some(),
                (major, minor) >= (2, 33),
                "glibc {major}.{minor}"
            );
            if let Some(f) = mallinfo2() {
                // SAFETY: 同 release。
                let m = unsafe { f() };
                assert!(m.arena > 0, "主分配区已经用过");
            }
        }
    }
}
