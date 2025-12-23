//! Safe LSN tracking for WAL truncation.

use std::collections::HashMap;
use std::sync::RwLock;

/// Partition identifier (start timestamp).
pub type PartitionId = i64;

#[derive(Debug, Default)]
struct SafeLsnState {
    flushed_lsns: HashMap<PartitionId, u64>,
    pending_drops: Vec<u64>,
}

/// Tracks safe LSN based on flushed partitions and pending DropPartition entries.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
///
/// let tracker = SafeLsnTracker::new();
/// ```
#[derive(Debug, Default)]
pub struct SafeLsnTracker {
    inner: RwLock<SafeLsnState>,
}

impl SafeLsnTracker {
    /// Creates a new tracker with empty state.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
    ///
    /// let tracker = SafeLsnTracker::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a flush completion for a partition.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
    ///
    /// let tracker = SafeLsnTracker::new();
    /// tracker.notify_flush(1, 42);
    /// ```
    pub fn notify_flush(&self, partition_id: PartitionId, flushed_lsn: u64) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.flushed_lsns.insert(partition_id, flushed_lsn);
    }

    /// Records a DropPartition WAL entry by LSN.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
    ///
    /// let tracker = SafeLsnTracker::new();
    /// tracker.notify_drop_recorded(100);
    /// ```
    pub fn notify_drop_recorded(&self, lsn: u64) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        if !state.pending_drops.contains(&lsn) {
            state.pending_drops.push(lsn);
        }
    }

    /// Records a completed DropPartition, removing the pending LSN.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
    ///
    /// let tracker = SafeLsnTracker::new();
    /// tracker.notify_drop_complete(100);
    /// ```
    pub fn notify_drop_complete(&self, lsn: u64) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.pending_drops.retain(|&pending| pending != lsn);
    }

    /// Calculates the safe LSN for WAL truncation.
    ///
    /// Formula: min(min(flushed_lsns), min(pending_drop_lsn) - 1).
    /// Returns None if no partitions have been flushed.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
    ///
    /// let tracker = SafeLsnTracker::new();
    /// tracker.notify_flush(1, 100);
    /// let _safe = tracker.calculate_safe_lsn();
    /// ```
    pub fn calculate_safe_lsn(&self) -> Option<u64> {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        let min_flushed = state.flushed_lsns.values().copied().min()?;

        let safe_lsn = if let Some(min_pending) = state.pending_drops.iter().copied().min() {
            min_flushed.min(min_pending.saturating_sub(1))
        } else {
            min_flushed
        };

        Some(safe_lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_safe_lsn_flushed_only() {
        let tracker = SafeLsnTracker::new();
        tracker.notify_flush(1, 100);
        tracker.notify_flush(2, 120);
        assert_eq!(tracker.calculate_safe_lsn(), Some(100));
    }

    #[test]
    fn test_calculate_safe_lsn_with_pending_drop() {
        let tracker = SafeLsnTracker::new();
        tracker.notify_flush(1, 150);
        tracker.notify_flush(2, 200);
        tracker.notify_drop_recorded(160);
        assert_eq!(tracker.calculate_safe_lsn(), Some(150));
    }

    #[test]
    fn test_calculate_safe_lsn_no_partitions() {
        let tracker = SafeLsnTracker::new();
        assert_eq!(tracker.calculate_safe_lsn(), None);
    }

    #[test]
    fn test_multiple_partitions_min_calculation() {
        let tracker = SafeLsnTracker::new();
        tracker.notify_flush(1, 300);
        tracker.notify_flush(2, 250);
        tracker.notify_flush(3, 275);
        assert_eq!(tracker.calculate_safe_lsn(), Some(250));
    }

    #[test]
    fn test_pending_drop_complete() {
        let tracker = SafeLsnTracker::new();
        tracker.notify_flush(1, 100);
        tracker.notify_drop_recorded(90);
        assert_eq!(tracker.calculate_safe_lsn(), Some(89));
        tracker.notify_drop_complete(90);
        assert_eq!(tracker.calculate_safe_lsn(), Some(100));
    }
}
