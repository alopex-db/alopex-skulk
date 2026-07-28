//! Integrated startup, WAL replay, and durable flush lifecycle.

use crate::error::{Result, TsmError};
use crate::model::WideRow;
use crate::store::buffer::{FlushPolicy, MeasurementBuffer};
use crate::store::compaction::{CompactionConfig, CompactionResult, Compactor};
use crate::store::lock::DataRootLock;
use crate::store::manifest::{ActiveFile, ManifestState, ManifestStore, ManifestUpdate};
use crate::store::parquet_reader::{ParquetReader, ParquetReaderConfig};
use crate::store::parquet_writer::{
    ParquetWriter, ParquetWriterConfig, PublishHook, PublishedParquet,
};
use crate::store::seq::{IngestSeq, SequencedRow, Sequencer};
use crate::store::wal::{Wal, WalConfig, WalEntry};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Integrated recovery and persistence policies.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryConfig {
    wal: WalConfig,
    buffer: FlushPolicy,
    writer: ParquetWriterConfig,
    reader: ParquetReaderConfig,
}

impl RecoveryConfig {
    /// Creates a store policy from the component policies.
    pub const fn new(
        wal: WalConfig,
        buffer: FlushPolicy,
        writer: ParquetWriterConfig,
        reader: ParquetReaderConfig,
    ) -> Self {
        Self {
            wal,
            buffer,
            writer,
            reader,
        }
    }
}

/// Observable durability boundary used by integration fault injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryBoundary {
    /// A complete Parquet file exists but is not yet manifest-active.
    AfterParquetPublish,
    /// The manifest is durable but the WAL prefix is not checkpointed.
    AfterManifestPublish,
    /// The obsolete WAL prefix has been atomically removed.
    AfterWalCheckpoint,
}

/// Hook invoked at integrated durability boundaries.
pub trait RecoveryHook {
    /// Observes one boundary and its relevant file, if any.
    fn reached(&self, boundary: RecoveryBoundary, path: Option<&Path>) -> Result<()>;
}

struct ContinueRecovery;

impl RecoveryHook for ContinueRecovery {
    fn reached(&self, _boundary: RecoveryBoundary, _path: Option<&Path>) -> Result<()> {
        Ok(())
    }
}

struct ContinuePublish;

impl PublishHook for ContinuePublish {
    fn before_publish(&self, _temporary_path: &Path) -> Result<()> {
        Ok(())
    }
}

/// Single-writer storage lifecycle with manifest-fenced WAL replay.
pub struct RecoveryStore {
    _lock: DataRootLock,
    manifest: ManifestStore,
    wal: Wal,
    sequencer: Sequencer,
    buffers: BTreeMap<String, MeasurementBuffer>,
    pending: BTreeMap<String, Vec<SequencedRow>>,
    replayed_row_count: usize,
    config: RecoveryConfig,
}

impl RecoveryStore {
    /// Opens in lock→manifest/orphan cleanup→WAL replay→sequencer order.
    pub fn open(root: impl AsRef<Path>, config: RecoveryConfig) -> Result<Self> {
        let root = root.as_ref();
        let lock = DataRootLock::acquire(root)?;
        let manifest = ManifestStore::open(root)?;
        let wal = Wal::open(root, config.wal)?;
        let manifest_state = manifest.state()?;
        let wal_high_water = wal
            .recovered_entries()
            .iter()
            .map(WalEntry::sequence)
            .max()
            .map(IngestSeq::new);
        let sequencer = manifest.resume_sequencer(wal_high_water)?;
        let mut buffers = BTreeMap::new();
        let mut pending: BTreeMap<String, Vec<SequencedRow>> = BTreeMap::new();
        let mut observed_sequences = BTreeSet::new();
        let mut replayed_row_count = 0;

        for entry in wal.recovered_entries() {
            if !observed_sequences.insert(entry.sequence()) {
                return Err(TsmError::Corruption(format!(
                    "duplicate WAL ingest sequence {}",
                    entry.sequence()
                )));
            }
            let sequence = IngestSeq::new(entry.sequence());
            if !manifest_state.should_replay(sequence) {
                continue;
            }
            let sequenced = SequencedRow::new(sequence, entry.row().clone());
            let measurement = sequenced.row().series().measurement().to_owned();
            buffers
                .entry(measurement.clone())
                .or_insert_with(|| MeasurementBuffer::new(&measurement, config.buffer))
                .append(sequenced.clone())?;
            pending.entry(measurement).or_default().push(sequenced);
            replayed_row_count += 1;
        }

        Ok(Self {
            _lock: lock,
            manifest,
            wal,
            sequencer,
            buffers,
            pending,
            replayed_row_count,
            config,
        })
    }

    /// Durably logs one row before returning its acknowledgeable sequence.
    pub fn ingest(&mut self, row: WideRow) -> Result<IngestSeq> {
        let measurement = row.series().measurement().to_owned();
        if let Some(buffer) = self.buffers.get(&measurement) {
            buffer.validate_append(&row)?;
        } else {
            MeasurementBuffer::new(&measurement, self.config.buffer).validate_append(&row)?;
        }
        let sequenced = self.sequencer.issue(row)?;
        let sequence = sequenced.ingest_seq();
        self.wal
            .append_durable(&WalEntry::new(sequence.get(), sequenced.row().clone()))?;
        self.buffers
            .entry(measurement.clone())
            .or_insert_with(|| MeasurementBuffer::new(&measurement, self.config.buffer))
            .append(sequenced.clone())?;
        self.pending.entry(measurement).or_default().push(sequenced);
        Ok(sequence)
    }

    /// Persists every buffered measurement under one global recovery fence.
    pub fn flush_all(&mut self) -> Result<Vec<PublishedParquet>> {
        self.flush_all_with_hooks(&ContinueRecovery, &ContinuePublish)
    }

    /// Persists all buffers with observable post-Parquet/manifest/WAL boundaries.
    pub fn flush_all_with_hook(
        &mut self,
        hook: &dyn RecoveryHook,
    ) -> Result<Vec<PublishedParquet>> {
        self.flush_all_with_hooks(hook, &ContinuePublish)
    }

    /// Persists with both pre-publication and lifecycle fault injection.
    pub fn flush_all_with_hooks(
        &mut self,
        hook: &dyn RecoveryHook,
        publish_hook: &dyn PublishHook,
    ) -> Result<Vec<PublishedParquet>> {
        if self.pending.values().all(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let writer = ParquetWriter::new(self.config.writer);
        let mut published = Vec::new();
        let mut update = ManifestUpdate::new();
        for (measurement, buffer) in &self.buffers {
            if buffer.row_count() == 0 {
                continue;
            }
            let batch = buffer.to_sorted_record_batch()?;
            let name = format!(
                "segment-{:020}-{:016}.parquet",
                self.sequencer.highest_issued().map_or(0, IngestSeq::get),
                SEGMENT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            );
            let written = writer.write_atomic_with_hook(
                self.manifest.segments_dir().join(&name),
                &batch,
                publish_hook,
            )?;
            hook.reached(RecoveryBoundary::AfterParquetPublish, Some(written.path()))?;
            let active = ActiveFile::new(
                measurement,
                &name,
                written.row_count() as u64,
                written.file_bytes(),
            )?;
            update = update.add_file(active);
            published.push(written);
        }
        let fence = self
            .pending
            .values()
            .flatten()
            .map(SequencedRow::ingest_seq)
            .max()
            .ok_or_else(|| TsmError::Corruption("pending rows lack a recovery fence".into()))?;
        update = update
            .advance_persisted_through(fence)
            .observe_issued_through(
                self.sequencer
                    .highest_issued()
                    .ok_or_else(|| TsmError::Corruption("sequencer lacks high-water".into()))?,
            );
        self.manifest.publish(update)?;
        if let Err(error) = hook.reached(RecoveryBoundary::AfterManifestPublish, None) {
            self.clear_persisted_buffers()?;
            return Err(error);
        }
        self.clear_persisted_buffers()?;
        self.wal.checkpoint_through(fence.get())?;
        hook.reached(RecoveryBoundary::AfterWalCheckpoint, Some(self.wal.path()))?;
        Ok(published)
    }

    /// Reads active Parquet plus unflushed/replayed rows without duplicate sequences.
    pub fn read_measurement(&self, measurement: &str) -> Result<Vec<SequencedRow>> {
        let mut rows = ParquetReader::new(self.config.reader).read_measurement(
            &self.manifest.state()?,
            self.manifest.segments_dir(),
            measurement,
        )?;
        if let Some(pending) = self.pending.get(measurement) {
            rows.extend(pending.iter().cloned());
        }
        rows.sort_by_key(SequencedRow::ingest_seq);
        let mut sequences = BTreeSet::new();
        if let Some(duplicate) = rows
            .iter()
            .map(SequencedRow::ingest_seq)
            .find(|sequence| !sequences.insert(*sequence))
        {
            return Err(TsmError::Corruption(format!(
                "duplicate visible ingest sequence {}",
                duplicate.get()
            )));
        }
        Ok(rows)
    }

    /// Compacts all durable files for one measurement under this store's lock.
    pub fn compact_measurement(&mut self, measurement: &str) -> Result<Option<CompactionResult>> {
        Compactor::new(CompactionConfig::new(
            self.config.buffer,
            self.config.reader,
            self.config.writer,
        ))
        .compact_measurement(&self.manifest, measurement)
    }

    /// Returns how many WAL rows were replayed beyond the manifest fence.
    pub const fn replayed_row_count(&self) -> usize {
        self.replayed_row_count
    }

    /// Returns a snapshot of the durable manifest state.
    pub fn manifest_state(&self) -> Result<ManifestState> {
        self.manifest.state()
    }

    fn clear_persisted_buffers(&mut self) -> Result<()> {
        for buffer in self.buffers.values_mut() {
            if buffer.row_count() != 0 {
                let _ = buffer.drain_sorted()?;
            }
        }
        self.pending.clear();
        Ok(())
    }
}
