//! Immutable Parquet compaction and ingest-sequence deduplication.

use crate::error::{Result, TsmError};
use crate::model::{Tags, Timestamp};
use crate::store::buffer::{FlushPolicy, MeasurementBuffer};
use crate::store::manifest::{ActiveFile, ManifestStore, ManifestUpdate};
use crate::store::parquet_reader::{ParquetReader, ParquetReaderConfig};
use crate::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
use crate::store::seq::SequencedRow;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COMPACTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Resource and encoding policies used by one compaction pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionConfig {
    buffer: FlushPolicy,
    reader: ParquetReaderConfig,
    writer: ParquetWriterConfig,
}

impl CompactionConfig {
    /// Creates a compaction policy from existing storage component policies.
    pub const fn new(
        buffer: FlushPolicy,
        reader: ParquetReaderConfig,
        writer: ParquetWriterConfig,
    ) -> Self {
        Self {
            buffer,
            reader,
            writer,
        }
    }
}

/// Observable result of replacing multiple active files with one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    input_file_count: usize,
    input_row_count: usize,
    output_file: String,
    output_row_count: usize,
}

impl CompactionResult {
    /// Returns how many active input files were replaced.
    pub const fn input_file_count(&self) -> usize {
        self.input_file_count
    }

    /// Returns how many rows were read before deduplication.
    pub const fn input_row_count(&self) -> usize {
        self.input_row_count
    }

    /// Returns the manifest-relative compacted Parquet name.
    pub fn output_file(&self) -> &str {
        &self.output_file
    }

    /// Returns how many unique rows remain after deduplication.
    pub const fn output_row_count(&self) -> usize {
        self.output_row_count
    }
}

/// DataFusion-free compactor for manifest-active measurement files.
pub struct Compactor {
    config: CompactionConfig,
}

impl Compactor {
    /// Creates a compactor with explicit read, buffer, and write policies.
    pub const fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    /// Replaces all active files for one measurement when at least two exist.
    pub fn compact_measurement(
        &self,
        manifest: &ManifestStore,
        measurement: &str,
    ) -> Result<Option<CompactionResult>> {
        if measurement.is_empty() {
            return Err(TsmError::InvalidInput(
                "compaction measurement must be non-empty".into(),
            ));
        }
        let snapshot = manifest.state()?;
        let candidates = snapshot
            .active_files()
            .values()
            .filter(|file| file.measurement() == measurement)
            .cloned()
            .collect::<Vec<_>>();
        if candidates.len() < 2 {
            return Ok(None);
        }

        let rows = ParquetReader::new(self.config.reader).read_measurement(
            &snapshot,
            manifest.segments_dir(),
            measurement,
        )?;
        let input_row_count = rows.len();
        let winners = deduplicate(rows);
        let mut buffer = MeasurementBuffer::new(measurement, self.config.buffer);
        for row in winners.into_values() {
            buffer.append(row)?;
        }
        let batch = buffer.drain_sorted()?;
        let output_row_count = batch.num_rows();
        let output_file = format!(
            "compact-{:020}-{:016}.parquet",
            snapshot.generation(),
            COMPACTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let written = ParquetWriter::new(self.config.writer)
            .write_atomic(manifest.segments_dir().join(&output_file), &batch)?;
        let mut unpublished = UnpublishedFile::new(written.path().to_owned());
        let compacted = ActiveFile::new(
            measurement,
            &output_file,
            written.row_count() as u64,
            written.file_bytes(),
        )?;
        let mut update = ManifestUpdate::new().add_file(compacted);
        let input_files = candidates
            .iter()
            .map(|file| file.name().to_owned())
            .collect::<Vec<_>>();
        for file in &input_files {
            update = update.remove_file(file);
        }
        manifest.publish(update)?;
        unpublished.disarm();
        manifest.remove_inactive_files(&input_files)?;

        Ok(Some(CompactionResult {
            input_file_count: input_files.len(),
            input_row_count,
            output_file,
            output_row_count,
        }))
    }
}

fn deduplicate(rows: Vec<SequencedRow>) -> BTreeMap<(Tags, Timestamp), SequencedRow> {
    let mut winners = BTreeMap::new();
    for row in rows {
        let key = (row.row().series().tags().clone(), row.row().timestamp());
        match winners.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(row);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if row.ingest_seq() > entry.get().ingest_seq() =>
            {
                entry.insert(row);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    winners
}

struct UnpublishedFile {
    path: PathBuf,
    armed: bool,
}

impl UnpublishedFile {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnpublishedFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}
