//! Integrated startup, WAL replay, and durable flush lifecycle.

use crate::error::{Result, TsmError};
use crate::model::{Timestamp, WideRow};
use crate::store::buffer::{
    BatchQualification, BatchValidator, FlushPolicy, MeasurementBuffer, MeasurementState,
};
use crate::store::compaction::{CompactionConfig, CompactionResult, Compactor};
use crate::store::format::reject_legacy_data_root;
use crate::store::lock::DataRootLock;
use crate::store::manifest::{ActiveFile, ManifestState, ManifestStore, ManifestUpdate};
use crate::store::parquet_reader::{ParquetReader, ParquetReaderConfig};
use crate::store::parquet_writer::{
    ParquetWriter, ParquetWriterConfig, PublishHook, PublishedParquet,
};
use crate::store::reader::{
    merge_pending_float_series, merge_pending_rows, FloatSeriesScanResult, ManifestStorageReader,
    ScanRequest, ScanResult, StorageReader, StorageReaderConfig,
};
use crate::store::retention::{
    current_timestamp_nanos, RetentionPolicy, RetentionResult, RetentionStore, TimePartition,
};
use crate::store::schema::{MeasurementSchema, SchemaResolver};
use crate::store::seq::{IngestSeq, SequencedRow, Sequencer};
use crate::store::wal::{Wal, WalConfig, WalEntry};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Integrated recovery and persistence policies.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryConfig {
    wal: WalConfig,
    buffer: FlushPolicy,
    writer: ParquetWriterConfig,
    reader: ParquetReaderConfig,
    storage_reader: StorageReaderConfig,
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
            storage_reader: StorageReaderConfig::DEFAULT,
        }
    }

    /// Replaces the query scan policy while retaining durability policies.
    pub const fn with_storage_reader(mut self, storage_reader: StorageReaderConfig) -> Self {
        self.storage_reader = storage_reader;
        self
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
    retention: RetentionStore,
    wal: Wal,
    sequencer: Sequencer,
    buffers: BTreeMap<String, MeasurementState>,
    pending: BTreeMap<String, Vec<SequencedRow>>,
    schema: SchemaResolver,
    float_scan_cache: Mutex<Option<FloatScanCacheEntry>>,
    replayed_row_count: usize,
    config: RecoveryConfig,
}

struct FloatScanCacheEntry {
    manifest_generation: u64,
    issued_through: Option<IngestSeq>,
    request: ScanRequest,
    field: String,
    result: Arc<FloatSeriesScanResult>,
}

impl RecoveryStore {
    /// Opens in lock→manifest/orphan cleanup→WAL replay→sequencer order.
    pub fn open(root: impl AsRef<Path>, config: RecoveryConfig) -> Result<Self> {
        let root = root.as_ref();
        let lock = DataRootLock::acquire(root)?;
        reject_legacy_data_root(root)?;
        let manifest = ManifestStore::open(root)?;
        let retention = RetentionStore::open(root)?;
        let wal = Wal::open(root, config.wal)?;
        let manifest_state = manifest.state()?;
        let wal_high_water = wal
            .recovered_entries()
            .iter()
            .map(WalEntry::sequence)
            .max()
            .map(IngestSeq::new);
        let sequencer = manifest.resume_sequencer(wal_high_water)?;
        let mut buffers: BTreeMap<String, MeasurementState> = BTreeMap::new();
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
                .or_insert_with(|| MeasurementState::new(&measurement))
                .record(sequenced.row())?;
            pending.entry(measurement).or_default().push(sequenced);
            replayed_row_count += 1;
        }

        Ok(Self {
            _lock: lock,
            manifest,
            retention,
            wal,
            sequencer,
            buffers,
            pending,
            schema: SchemaResolver::new(),
            float_scan_cache: Mutex::new(None),
            replayed_row_count,
            config,
        })
    }

    /// Durably logs one row before returning its acknowledgeable sequence.
    pub fn ingest(&mut self, row: WideRow) -> Result<IngestSeq> {
        self.ingest_at(row, current_timestamp_nanos()?)
    }

    /// Ingests with an explicit clock value for deterministic cutoff decisions.
    pub fn ingest_at(&mut self, row: WideRow, now: i64) -> Result<IngestSeq> {
        let measurement = row.series().measurement().to_owned();
        TimePartition::for_timestamp(row.timestamp())?;
        self.retention
            .validate_write(&measurement, row.timestamp(), now)?;
        if let Some(state) = self.buffers.get(&measurement) {
            state.validate_append(&row)?;
        } else {
            MeasurementState::new(&measurement).validate_append(&row)?;
        }
        self.wal.validate_row(&row)?;
        let sequenced = self.sequencer.issue(row)?;
        let sequence = sequenced.ingest_seq();
        self.wal
            .append_durable_row(sequence.get(), sequenced.row())?;
        self.buffers
            .entry(measurement.clone())
            .or_insert_with(|| MeasurementState::new(&measurement))
            .record(sequenced.row())?;
        self.pending.entry(measurement).or_default().push(sequenced);
        Ok(sequence)
    }

    /// Validates a batch, appends every WAL frame, and fsyncs once before Ack.
    pub fn ingest_batch_at(&mut self, rows: Vec<WideRow>, now: i64) -> Result<Vec<IngestSeq>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let affected = rows
            .iter()
            .map(|row| row.series().measurement())
            .collect::<BTreeSet<&str>>();
        let max_entry_bytes = self.config.wal.max_entry_bytes();
        let mut validation = BTreeMap::new();
        let mut cutoffs = BTreeMap::new();
        for measurement in &affected {
            validation.insert(
                *measurement,
                BatchValidator::new(self.buffers.get(*measurement), max_entry_bytes),
            );
            cutoffs.insert(
                *measurement,
                self.retention.reject_cutoff(measurement, now)?,
            );
        }
        for row in rows.iter() {
            let measurement = row.series().measurement();
            TimePartition::for_timestamp(row.timestamp())?;
            if let Some(Some(cutoff)) = cutoffs.get(measurement) {
                if row.timestamp() < *cutoff {
                    return Err(TsmError::InvalidInput(format!(
                        "timestamp {} is older than retention cutoff {cutoff} for '{measurement}'",
                        row.timestamp()
                    )));
                }
            }
            validation
                .get_mut(measurement)
                .ok_or_else(|| TsmError::Corruption("batch validation state is missing".into()))?
                .validate(row)?;
        }

        let qualification = BatchQualification(
            validation
                .into_iter()
                .map(|(measurement, validator)| (measurement.to_owned(), validator.finish()))
                .collect(),
        );
        drop(cutoffs);
        drop(affected);
        self.write_validated_batch(rows, qualification)
    }

    /// Durably writes rows whose admission walk already happened at the
    /// ingest layer (type-state trust; see `BatchQualification`). Only the
    /// cheap time-domain checks run here.
    pub fn ingest_qualified_batch_at(
        &mut self,
        rows: Vec<WideRow>,
        qualification: BatchQualification,
        now: i64,
    ) -> Result<Vec<IngestSeq>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut cutoffs = BTreeMap::new();
        for (measurement, _) in &qualification.0 {
            cutoffs.insert(
                measurement.as_str(),
                self.retention.reject_cutoff(measurement, now)?,
            );
        }
        for row in rows.iter() {
            TimePartition::for_timestamp(row.timestamp())?;
            if let Some(Some(cutoff)) = cutoffs.get(row.series().measurement()) {
                if row.timestamp() < *cutoff {
                    return Err(TsmError::InvalidInput(format!(
                        "timestamp {} is older than retention cutoff {cutoff} for '{}'",
                        row.timestamp(),
                        row.series().measurement()
                    )));
                }
            }
        }
        drop(cutoffs);
        self.write_validated_batch(rows, qualification)
    }

    /// Returns the durable hard-reject cutoff used by the shared ingest policy.
    ///
    /// This exposes only the effective cutoff; O3 remains an ingestion-layer
    /// concern and is not applied by direct storage APIs.
    pub fn retention_reject_cutoff(
        &self,
        measurement: &str,
        now: Timestamp,
    ) -> Result<Option<Timestamp>> {
        self.retention.reject_cutoff(measurement, now)
    }

    fn write_validated_batch(
        &mut self,
        rows: Vec<WideRow>,
        qualification: BatchQualification,
    ) -> Result<Vec<IngestSeq>> {
        let sequenced = self.sequencer.issue_batch(rows)?;
        self.wal.append_batch(
            sequenced
                .iter()
                .map(|row| (row.ingest_seq().get(), row.row())),
        )?;
        self.wal.sync()?;

        for (measurement, (new_columns, estimated_bytes, validated_rows)) in qualification.0 {
            if let Some(state) = self.buffers.get_mut(&measurement) {
                state.apply_batch(new_columns, estimated_bytes, validated_rows);
            } else {
                let mut state = MeasurementState::new(&measurement);
                state.apply_batch(new_columns, estimated_bytes, validated_rows);
                self.buffers.insert(measurement, state);
            }
        }
        let mut sequences = Vec::with_capacity(sequenced.len());
        for row in sequenced {
            sequences.push(row.ingest_seq());
            if let Some(rows) = self.pending.get_mut(row.row().series().measurement()) {
                rows.push(row);
            } else {
                let measurement = row.row().series().measurement().to_owned();
                self.pending.entry(measurement).or_default().push(row);
            }
        }
        Ok(sequences)
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
        for (measurement, rows) in &self.pending {
            let mut partitions: BTreeMap<TimePartition, Vec<&SequencedRow>> = BTreeMap::new();
            for row in rows {
                partitions
                    .entry(TimePartition::for_timestamp(row.row().timestamp())?)
                    .or_default()
                    .push(row);
            }
            for (partition, rows) in partitions {
                let mut buffer = MeasurementBuffer::new(measurement, self.config.buffer);
                let mut min_timestamp = i64::MAX;
                let mut max_timestamp = i64::MIN;
                for row in rows {
                    min_timestamp = min_timestamp.min(row.row().timestamp());
                    max_timestamp = max_timestamp.max(row.row().timestamp());
                    buffer.append(row)?;
                }
                let batch = buffer.to_sorted_record_batch()?;
                let name = format!(
                    "segment-{:020}-{:+020}-{:016}.parquet",
                    self.sequencer.highest_issued().map_or(0, IngestSeq::get),
                    partition.start(),
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
                    min_timestamp,
                    max_timestamp,
                )?;
                update = update.add_file(active);
                published.push(written);
            }
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

    /// Compacts one eligible durable time partition under this store's lock.
    pub fn compact_measurement(&mut self, measurement: &str) -> Result<Option<CompactionResult>> {
        Compactor::new(CompactionConfig::new(
            self.config.buffer,
            self.config.reader,
            self.config.writer,
        ))
        .compact_measurement(&self.manifest, measurement)
    }

    /// Atomically creates or changes a measurement retention policy.
    pub fn set_retention_policy(
        &mut self,
        measurement: &str,
        policy: RetentionPolicy,
    ) -> Result<()> {
        self.retention.set_policy(measurement, policy)
    }

    /// Returns the currently durable policy for a measurement.
    pub fn retention_policy(&self, measurement: &str) -> Result<Option<RetentionPolicy>> {
        self.retention.policy(measurement)
    }

    /// Lists manifest-active hourly partitions in timestamp order.
    pub fn list_partitions(&self, measurement: &str) -> Result<Vec<TimePartition>> {
        self.retention
            .list_partitions(&self.manifest.state()?, measurement)
    }

    /// Runs one idempotent TTL pass using an explicit nanosecond clock value.
    pub fn expire_retention(&mut self, measurement: &str, now: i64) -> Result<RetentionResult> {
        self.retention.expire(&self.manifest, measurement, now)
    }

    /// Returns how many WAL rows were replayed beyond the manifest fence.
    pub const fn replayed_row_count(&self) -> usize {
        self.replayed_row_count
    }

    /// Returns the current unflushed row count for admission control.
    pub fn pending_row_count(&self) -> usize {
        self.pending
            .values()
            .fold(0_usize, |total, rows| total.saturating_add(rows.len()))
    }

    /// Returns the recorded column state for one measurement, if any.
    pub fn measurement_state(&self, measurement: &str) -> Option<&MeasurementState> {
        self.buffers.get(measurement)
    }

    /// Resolves the current durable-plus-pending schema for one measurement.
    pub fn measurement_schema(&self, measurement: &str) -> Result<MeasurementSchema> {
        self.schema.resolve(
            &self.manifest.state()?,
            self.manifest.segments_dir(),
            measurement,
            self.buffers.get(measurement),
        )
    }

    /// Lists durable and pending measurement names in deterministic order.
    pub fn measurement_names(&self) -> Result<Vec<String>> {
        Ok(self.schema.measurement_names(
            &self.manifest.state()?,
            self.buffers
                .iter()
                .filter_map(|(name, state)| (state.row_count() > 0).then_some(name)),
        ))
    }

    /// Returns the WAL entry size limit for admission-time qualification.
    pub fn wal_max_entry_bytes(&self) -> usize {
        self.config.wal.max_entry_bytes()
    }

    /// Returns the current in-memory buffer estimate for admission control.
    pub fn pending_estimated_bytes(&self) -> Result<usize> {
        self.buffers.values().try_fold(0_usize, |total, state| {
            total
                .checked_add(state.estimated_bytes())
                .ok_or_else(|| TsmError::ResourceLimit("buffer pressure estimate overflow".into()))
        })
    }

    /// Returns the current durable-log size for admission control.
    pub fn wal_file_bytes(&self) -> Result<u64> {
        self.wal.file_bytes()
    }

    /// Returns a snapshot of the durable manifest state.
    pub fn manifest_state(&self) -> Result<ManifestState> {
        self.manifest.state()
    }

    fn clear_persisted_buffers(&mut self) -> Result<()> {
        for state in self.buffers.values_mut() {
            state.clear();
        }
        self.pending.clear();
        Ok(())
    }

    fn cached_float_scan(
        &self,
        manifest_generation: u64,
        issued_through: Option<IngestSeq>,
        request: &ScanRequest,
        field: &str,
    ) -> Result<Option<Arc<FloatSeriesScanResult>>> {
        let cache = self.float_scan_cache.lock().map_err(|_| {
            TsmError::Corruption("float-series scan cache lock is poisoned".to_string())
        })?;
        Ok(cache.as_ref().and_then(|entry| {
            (entry.manifest_generation == manifest_generation
                && entry.issued_through == issued_through
                && entry.request == *request
                && entry.field == field)
                .then(|| Arc::clone(&entry.result))
        }))
    }

    fn store_float_scan(
        &self,
        manifest_generation: u64,
        issued_through: Option<IngestSeq>,
        request: &ScanRequest,
        field: &str,
        result: Arc<FloatSeriesScanResult>,
    ) -> Result<Arc<FloatSeriesScanResult>> {
        let mut cache = self.float_scan_cache.lock().map_err(|_| {
            TsmError::Corruption("float-series scan cache lock is poisoned".to_string())
        })?;
        *cache = Some(FloatScanCacheEntry {
            manifest_generation,
            issued_through,
            request: request.clone(),
            field: field.to_string(),
            result: Arc::clone(&result),
        });
        Ok(result)
    }
}

impl StorageReader for RecoveryStore {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        let manifest = self.manifest.state()?;
        let durable = ManifestStorageReader::new(
            &manifest,
            self.manifest.segments_dir(),
            self.config.storage_reader,
        )
        .scan(request)?;
        let pending = self
            .pending
            .get(request.measurement())
            .map_or(&[][..], Vec::as_slice);
        merge_pending_rows(durable, pending, request)
    }

    fn try_scan_float_series(
        &self,
        request: &ScanRequest,
        field: &str,
    ) -> Result<Option<Arc<FloatSeriesScanResult>>> {
        let manifest = self.manifest.state()?;
        let issued_through = self.sequencer.highest_issued();
        if let Some(cached) =
            self.cached_float_scan(manifest.generation(), issued_through, request, field)?
        {
            return Ok(Some(cached));
        }
        let Some(durable) = ManifestStorageReader::new(
            &manifest,
            self.manifest.segments_dir(),
            self.config.storage_reader,
        )
        .try_scan_float_series(request, field)?
        else {
            return Ok(None);
        };
        let pending = self
            .pending
            .get(request.measurement())
            .map_or(&[][..], Vec::as_slice);
        let result = if pending.is_empty() {
            Some(durable)
        } else {
            let durable = match Arc::try_unwrap(durable) {
                Ok(durable) => durable,
                Err(shared) => (*shared).clone(),
            };
            merge_pending_float_series(durable, pending, request, field)?.map(Arc::new)
        };
        result
            .map(|result| {
                self.store_float_scan(
                    manifest.generation(),
                    issued_through,
                    request,
                    field,
                    result,
                )
            })
            .transpose()
    }

    fn measurement_names(&self) -> Result<Vec<String>> {
        RecoveryStore::measurement_names(self)
    }

    fn measurement_schema(&self, measurement: &str) -> Result<MeasurementSchema> {
        RecoveryStore::measurement_schema(self, measurement)
    }
}
