//! Startup warning for thin `RLIMIT_NOFILE` headroom against a
//! [`crate::wal::manager::WalManager`]'s expected WAL descriptor demand.
//!
//! Advisory only: this module never calls `setrlimit` and never fails
//! startup. The warning exists so an operator with a tenant/dataset/signal
//! cardinality that will exhaust the descriptor limit finds out from a log
//! line at boot, not from `Wal::new` failing inside a write path months
//! later (#1305, #1342).

/// File descriptors held open by one live [`crate::wal::Wal`] instance: the
/// current segment's log file, its data file, and its index file.
pub const FDS_PER_WAL: u64 = 3;

/// Descriptors this process needs for everything that is not a WAL instance
/// — listeners, the object store client, the catalog connection pool, Flight
/// connections to peers. Deliberately generous rather than measured:
/// undercounting here would make the warning fire too late to be useful.
pub const RESERVED_FDS: u64 = 128;

/// Whether `expected_instances` WAL instances plus [`RESERVED_FDS`] would
/// leave no headroom under `soft_limit`.
///
/// Pure and unit-tested on its own, separate from the `getrlimit` syscall, so
/// the arithmetic is exercised without a platform-specific call.
pub fn fd_headroom_is_thin(soft_limit: u64, expected_instances: u64) -> bool {
    expected_instances.saturating_mul(FDS_PER_WAL) + RESERVED_FDS > soft_limit
}

/// The process's current `RLIMIT_NOFILE` soft limit, or `None` if it could
/// not be read (a failed syscall, or a non-Unix target).
#[cfg(unix)]
fn nofile_soft_limit() -> Option<u64> {
    // SAFETY: `rlim` is a plain-old-data struct the kernel fills in
    // entirely; zero-initializing before the call is the standard libc
    // pattern, matching `process_cpu_time_by_mode`'s use of `getrusage`.
    let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if rc != 0 {
        return None;
    }
    Some(rlim.rlim_cur as u64)
}

#[cfg(not(unix))]
fn nofile_soft_limit() -> Option<u64> {
    None
}

/// Log one warning if `RLIMIT_NOFILE` looks thin against `expected_instances`
/// WAL instances. `cap` is the manager's configured `[wal].max_instances`
/// (`0` = unbounded), used only to word the remedy: a bounded cap can be
/// lowered to fit, an unbounded one cannot, so growth past today's
/// `expected_instances` stays unprotected either way.
///
/// A missing/unreadable soft limit is not itself warned about — the check
/// degrades to silent on a platform or sandbox that will not report one.
pub fn warn_on_thin_fd_headroom(service: &str, expected_instances: u64, cap: usize) {
    let Some(soft_limit) = nofile_soft_limit() else {
        return;
    };
    if !fd_headroom_is_thin(soft_limit, expected_instances) {
        return;
    }

    if cap == 0 {
        tracing::warn!(
            service,
            soft_limit,
            expected_instances,
            fds_per_wal = FDS_PER_WAL,
            "RLIMIT_NOFILE has little headroom for this deployment's current WAL instance \
             count, and [wal].max_instances is 0 (unbounded): nothing will stop the descriptor \
             count from growing further. Raise RLIMIT_NOFILE (ulimit -n) or set \
             [wal].max_instances so eviction keeps the descriptor count bounded"
        );
    } else {
        tracing::warn!(
            service,
            soft_limit,
            expected_instances,
            cap,
            fds_per_wal = FDS_PER_WAL,
            "RLIMIT_NOFILE has little headroom for this deployment's expected WAL instance \
             count; raise it (ulimit -n) or lower [wal].max_instances so eviction keeps the \
             descriptor count under the limit"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thin_when_expected_demand_exceeds_the_limit() {
        // 300 * 3 + 128 = 1028 > 1000.
        assert!(fd_headroom_is_thin(1000, 300));
    }

    #[test]
    fn not_thin_with_headroom_to_spare() {
        // The documented default: 256 * 3 + 128 = 896 <= 1024.
        assert!(!fd_headroom_is_thin(1024, 256));
    }

    #[test]
    fn exactly_at_the_limit_is_not_thin() {
        assert!(!fd_headroom_is_thin(896, 256));
    }

    #[test]
    fn one_over_the_limit_is_thin() {
        assert!(fd_headroom_is_thin(895, 256));
    }
}
