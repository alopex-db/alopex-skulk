//! Row-level ingest sequence allocation.

use crate::error::{Result, TsmError};
use crate::model::WideRow;
use serde::{Deserialize, Serialize};

/// Globally ordered identifier assigned once to an ingested row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IngestSeq(u64);

impl IngestSeq {
    /// Creates a sequence value recovered from or persisted by the storage layer.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying sequence number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A wide row coupled to its total-order ingest position.
#[derive(Debug, Clone, PartialEq)]
pub struct SequencedRow {
    ingest_seq: IngestSeq,
    row: WideRow,
}

impl SequencedRow {
    /// Creates a sequenced row from an already-issued sequence.
    pub const fn new(ingest_seq: IngestSeq, row: WideRow) -> Self {
        Self { ingest_seq, row }
    }

    /// Returns the row's total-order ingest sequence.
    pub const fn ingest_seq(&self) -> IngestSeq {
        self.ingest_seq
    }

    /// Returns the row without discarding its sequence.
    pub const fn row(&self) -> &WideRow {
        &self.row
    }

    /// Splits the value into its sequence and owned row.
    pub fn into_parts(self) -> (IngestSeq, WideRow) {
        (self.ingest_seq, self.row)
    }
}

/// Single-writer allocator for monotonically increasing row sequences.
#[derive(Debug, Clone)]
pub struct Sequencer {
    next: Option<u64>,
    highest_issued: Option<IngestSeq>,
}

impl Sequencer {
    /// Creates a fresh sequencer whose first issued value is one.
    pub const fn new() -> Self {
        Self {
            next: Some(1),
            highest_issued: None,
        }
    }

    /// Restores an allocator strictly after the durable high-water mark.
    ///
    /// Callers must supply the maximum value observed across every durable
    /// recovery source. Manifest integration supplies that combined boundary.
    pub fn resume_after(highest_issued: Option<IngestSeq>) -> Result<Self> {
        match highest_issued {
            None => Ok(Self::new()),
            Some(highest) => {
                let next = highest.get().checked_add(1).ok_or_else(|| {
                    TsmError::ResourceLimit("ingest sequence space is exhausted".into())
                })?;
                Ok(Self {
                    next: Some(next),
                    highest_issued: Some(highest),
                })
            }
        }
    }

    /// Assigns the next total-order value to exactly one row.
    pub fn issue(&mut self, row: WideRow) -> Result<SequencedRow> {
        let value = self
            .next
            .ok_or_else(|| TsmError::ResourceLimit("ingest sequence space is exhausted".into()))?;
        let ingest_seq = IngestSeq::new(value);
        self.next = value.checked_add(1);
        self.highest_issued = Some(ingest_seq);
        Ok(SequencedRow::new(ingest_seq, row))
    }

    /// Atomically reserves a contiguous sequence range for a validated batch.
    pub fn issue_batch(&mut self, rows: Vec<WideRow>) -> Result<Vec<SequencedRow>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let start = self
            .next
            .ok_or_else(|| TsmError::ResourceLimit("ingest sequence space is exhausted".into()))?;
        let additional = u64::try_from(rows.len() - 1)
            .map_err(|_| TsmError::ResourceLimit("ingest batch exceeds u64".into()))?;
        let last = start
            .checked_add(additional)
            .ok_or_else(|| TsmError::ResourceLimit("ingest sequence space is exhausted".into()))?;
        let mut sequenced = Vec::with_capacity(rows.len());
        for (offset, row) in rows.into_iter().enumerate() {
            let offset = u64::try_from(offset)
                .map_err(|_| TsmError::ResourceLimit("ingest batch exceeds u64".into()))?;
            let sequence = start.checked_add(offset).ok_or_else(|| {
                TsmError::ResourceLimit("ingest sequence space is exhausted".into())
            })?;
            sequenced.push(SequencedRow::new(IngestSeq::new(sequence), row));
        }
        self.next = last.checked_add(1);
        self.highest_issued = Some(IngestSeq::new(last));
        Ok(sequenced)
    }

    /// Returns the highest recovered or newly issued sequence.
    pub const fn highest_issued(&self) -> Option<IngestSeq> {
        self.highest_issued
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{IngestSeq, Sequencer};
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};

    fn row(timestamp: i64) -> WideRow {
        WideRow::new(
            SeriesKey::new("cpu", Tags::new()),
            timestamp,
            Fields::from([("usage".into(), FieldValue::Float(1.0))]),
        )
    }

    #[test]
    fn every_row_receives_a_strictly_increasing_sequence() {
        let mut sequencer = Sequencer::new();

        let first = sequencer.issue(row(10)).expect("first row");
        let second = sequencer.issue(row(10)).expect("second row");
        let third = sequencer.issue(row(11)).expect("third row");

        assert_eq!(first.ingest_seq(), IngestSeq::new(1));
        assert_eq!(second.ingest_seq(), IngestSeq::new(2));
        assert_eq!(third.ingest_seq(), IngestSeq::new(3));
        assert_eq!(second.row().timestamp(), 10);
    }

    #[test]
    fn recovery_resumes_strictly_after_the_highest_issued_sequence() {
        let durable_high_water = {
            let mut original = Sequencer::new();
            original.issue(row(10)).expect("first issued row");
            original.issue(row(11)).expect("second issued row");
            original.highest_issued()
        };

        let mut sequencer = Sequencer::resume_after(durable_high_water).expect("recover sequencer");

        let recovered_next = sequencer.issue(row(12)).expect("post-recovery row");

        assert_eq!(recovered_next.ingest_seq(), IngestSeq::new(3));
        assert_eq!(sequencer.highest_issued(), Some(IngestSeq::new(3)));
    }

    #[test]
    fn empty_recovery_uses_the_same_origin_as_a_fresh_sequencer() {
        let mut sequencer = Sequencer::resume_after(None).expect("empty recovery");

        assert_eq!(
            sequencer.issue(row(1)).expect("first row").ingest_seq(),
            IngestSeq::new(1)
        );
    }

    #[test]
    fn sequence_exhaustion_returns_an_error_instead_of_wrapping_or_reusing() {
        let mut sequencer =
            Sequencer::resume_after(Some(IngestSeq::new(u64::MAX - 1))).expect("last sequence");

        assert_eq!(
            sequencer.issue(row(1)).expect("issue u64 max").ingest_seq(),
            IngestSeq::new(u64::MAX)
        );
        assert!(sequencer.issue(row(2)).is_err());
        assert_eq!(sequencer.highest_issued(), Some(IngestSeq::new(u64::MAX)));
    }

    #[test]
    fn recovery_after_u64_max_fails_without_reusing_the_sequence_space() {
        assert!(Sequencer::resume_after(Some(IngestSeq::new(u64::MAX))).is_err());
    }

    #[test]
    fn oversized_batch_reservation_does_not_partially_advance() {
        let mut sequencer =
            Sequencer::resume_after(Some(IngestSeq::new(u64::MAX - 1))).expect("last slot");
        assert!(sequencer.issue_batch(vec![row(1), row(2)]).is_err());
        assert_eq!(
            sequencer
                .issue(row(3))
                .expect("still available")
                .ingest_seq(),
            IngestSeq::new(u64::MAX)
        );
    }
}
