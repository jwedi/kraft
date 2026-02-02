use std::time::Duration;
use lazy_static::lazy_static;
use minstant::{Anchor, Instant};

lazy_static! {
    /// Anchor for converting minstant monotonic time to Unix epoch.
    /// Created once at startup and reused for all time conversions.
    static ref ANCHOR: Anchor = Anchor::new();
}

/// Returns current Unix epoch time in milliseconds using minstant.
///
/// Uses TSC-based timing (~10ns) instead of syscall-based SystemTime (~27ns),
/// providing ~2.5x faster time lookups while maintaining wall clock accuracy.
#[inline]
pub fn now_millis() -> u128 {
    let instant = Instant::now();
    (instant.as_unix_nanos(&ANCHOR) / 1_000_000) as u128
}

/// Returns Unix epoch time in milliseconds for a future point in time.
#[inline]
pub fn now_plus_duration_millis(dur: Duration) -> u128 {
    now_millis() + dur.as_millis()
}