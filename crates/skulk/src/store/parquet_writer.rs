//! Atomic Parquet persistence for sorted measurement batches.

use crate::error::{Result, TsmError};
use crate::store::buffer::{
    COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN, INGEST_SEQ_COLUMN_KIND,
    TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Brotli quality fixed by the approved v0.3 storage decision.
pub const BROTLI_QUALITY: u32 = 5;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Parquet row-group resource policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetWriterConfig {
    max_row_group_rows: usize,
}

impl ParquetWriterConfig {
    /// Creates a non-zero row-group limit.
    pub fn new(max_row_group_rows: usize) -> Result<Self> {
        if max_row_group_rows == 0 {
            return Err(TsmError::InvalidInput(
                "Parquet row-group size must be non-zero".into(),
            ));
        }
        Ok(Self { max_row_group_rows })
    }

    /// Returns the maximum rows written to one row group.
    pub const fn max_row_group_rows(&self) -> usize {
        self.max_row_group_rows
    }
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            max_row_group_rows: 65_536,
        }
    }
}

/// Fault-injection boundary after durable temp write and before publication.
pub trait PublishHook {
    /// Called with the fully written temporary file before atomic rename.
    fn before_publish(&self, temporary_path: &Path) -> Result<()>;
}

struct AllowPublish;

impl PublishHook for AllowPublish {
    fn before_publish(&self, _temporary_path: &Path) -> Result<()> {
        Ok(())
    }
}

/// Metadata for one successfully published Parquet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedParquet {
    path: PathBuf,
    row_count: usize,
    file_bytes: u64,
}

impl PublishedParquet {
    /// Returns the final published path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of rows written.
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the final file size.
    pub const fn file_bytes(&self) -> u64 {
        self.file_bytes
    }
}

/// Writes RecordBatches to complete, atomically published Parquet files.
pub struct ParquetWriter {
    config: ParquetWriterConfig,
}

impl ParquetWriter {
    /// Creates an atomic writer with fixed v0.3 encoding and Brotli q5 policy.
    pub const fn new(config: ParquetWriterConfig) -> Self {
        Self { config }
    }

    /// Writes, synchronizes, and atomically publishes a batch.
    pub fn write_atomic(
        &self,
        final_path: impl AsRef<Path>,
        batch: &RecordBatch,
    ) -> Result<PublishedParquet> {
        self.write_atomic_with_hook(final_path, batch, &AllowPublish)
    }

    /// Writes through an injectable pre-publication boundary.
    ///
    /// This is used by crash-recovery tests to stop after the durable temporary
    /// file exists but before it can become a final `.parquet` file.
    pub fn write_atomic_with_hook(
        &self,
        final_path: impl AsRef<Path>,
        batch: &RecordBatch,
        hook: &dyn PublishHook,
    ) -> Result<PublishedParquet> {
        if batch.num_rows() == 0 {
            return Err(TsmError::InvalidInput(
                "cannot persist an empty RecordBatch".into(),
            ));
        }
        let final_path = final_path.as_ref();
        if final_path.extension().and_then(|value| value.to_str()) != Some("parquet") {
            return Err(TsmError::InvalidInput(
                "published storage files must use the .parquet extension".into(),
            ));
        }
        if final_path.exists() {
            return Err(TsmError::InvalidInput(format!(
                "refusing to overwrite existing Parquet file '{}'",
                final_path.display()
            )));
        }
        let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary_path = temporary_path(final_path)?;
        let mut cleanup = TemporaryFile::new(temporary_path.clone());
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)?;
        let properties = writer_properties(batch, self.config)?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), Some(properties)).map_err(parquet_error)?;
        writer.write(batch).map_err(parquet_error)?;
        writer.close().map_err(parquet_error)?;

        File::open(&temporary_path)?.sync_all()?;
        hook.before_publish(&temporary_path)?;
        fs::rename(&temporary_path, final_path)?;
        cleanup.disarm();
        sync_directory(parent)?;

        Ok(PublishedParquet {
            path: final_path.to_owned(),
            row_count: batch.num_rows(),
            file_bytes: fs::metadata(final_path)?.len(),
        })
    }
}

fn writer_properties(batch: &RecordBatch, config: ParquetWriterConfig) -> Result<WriterProperties> {
    let brotli = BrotliLevel::try_new(BROTLI_QUALITY)
        .map_err(|error| TsmError::InvalidInput(format!("invalid Brotli quality: {error}")))?;
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::BROTLI(brotli))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(config.max_row_group_rows);

    for field in batch.schema().fields() {
        let kind = field
            .metadata()
            .get(COLUMN_KIND_METADATA_KEY)
            .map(String::as_str)
            .ok_or_else(|| {
                TsmError::InvalidInput(format!(
                    "Arrow column '{}' lacks Skulk column-kind metadata",
                    field.name()
                ))
            })?;
        let path = ColumnPath::from(field.name().as_str());
        match kind {
            TAG_COLUMN_KIND => {
                if field.data_type() != &DataType::Utf8 {
                    return Err(TsmError::InvalidInput(format!(
                        "tag column '{}' must be Utf8",
                        field.name()
                    )));
                }
                builder = builder.set_column_dictionary_enabled(path, true);
            }
            TIME_COLUMN_KIND if field.name() == TIME_COLUMN => {
                builder = builder.set_column_encoding(path, Encoding::DELTA_BINARY_PACKED);
            }
            INGEST_SEQ_COLUMN_KIND if field.name() == INGEST_SEQ_COLUMN => {}
            FIELD_COLUMN_KIND if field.data_type() == &DataType::Float64 => {
                builder = builder.set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT);
            }
            FIELD_COLUMN_KIND => {}
            _ => {
                return Err(TsmError::InvalidInput(format!(
                    "Arrow column '{}' has invalid Skulk column-kind metadata",
                    field.name()
                )));
            }
        }
    }
    Ok(builder.build())
}

fn temporary_path(final_path: &Path) -> Result<PathBuf> {
    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| TsmError::InvalidInput("Parquet file name must be valid UTF-8".into()))?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{file_name}.tmp-{}-{sequence}",
        std::process::id()
    )))
}

struct TemporaryFile {
    path: PathBuf,
    armed: bool,
}

impl TemporaryFile {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn parquet_error(error: parquet::errors::ParquetError) -> TsmError {
    TsmError::Serialization(format!("Parquet write failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{ParquetWriter, ParquetWriterConfig, PublishHook, BROTLI_QUALITY};
    use crate::error::{Result, TsmError};
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::{FlushPolicy, MeasurementBuffer, INGEST_SEQ_COLUMN, TIME_COLUMN};
    use crate::store::seq::{IngestSeq, SequencedRow};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{Compression, Encoding};
    use std::fs::File;
    use std::path::Path;

    fn row(sequence: u64, host: &str, timestamp: i64, value: f64) -> SequencedRow {
        SequencedRow::new(
            IngestSeq::new(sequence),
            WideRow::new(
                SeriesKey::new("cpu", Tags::from([("host".into(), host.into())])),
                timestamp,
                Fields::from([("value".into(), FieldValue::Float(value))]),
            ),
        )
    }

    fn batch(rows: impl IntoIterator<Item = SequencedRow>) -> arrow::record_batch::RecordBatch {
        let mut buffer = MeasurementBuffer::new(
            "cpu",
            FlushPolicy::new(100_000, 128 * 1024 * 1024).expect("policy"),
        );
        for row in rows {
            buffer.append(row).expect("buffer row");
        }
        buffer.drain_sorted().expect("batch")
    }

    #[test]
    fn writer_persists_individual_tag_stats_and_time_series_encodings() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("cpu.parquet");
        let batch = batch([
            row(1, "edge-b", 20, 2.0),
            row(2, "edge-a", 10, 1.0),
            row(3, "edge-c", 30, 3.0),
        ]);
        let writer = ParquetWriter::new(ParquetWriterConfig::default());

        let written = writer.write_atomic(&path, &batch).expect("write parquet");

        assert_eq!(written.path(), path);
        assert_eq!(written.row_count(), 3);
        assert!(written.file_bytes() > 0);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("open parquet"))
                .expect("metadata reader");
        let host_index = reader.schema().index_of("host").expect("host index");
        let time_index = reader.schema().index_of(TIME_COLUMN).expect("time index");
        let value_index = reader.schema().index_of("value").expect("value index");
        let row_group = reader.metadata().row_group(0);
        let host = row_group.column(host_index);
        let host_stats = host.statistics().expect("host min/max stats");

        assert_eq!(host_stats.min_bytes_opt(), Some("edge-a".as_bytes()));
        assert_eq!(host_stats.max_bytes_opt(), Some("edge-c".as_bytes()));
        assert!(matches!(host.compression(), Compression::BROTLI(_)));
        assert_eq!(BROTLI_QUALITY, 5);
        assert!(host.encodings().contains(&Encoding::RLE_DICTIONARY));
        assert!(row_group
            .column(time_index)
            .encodings()
            .contains(&Encoding::DELTA_BINARY_PACKED));
        assert!(row_group
            .column(value_index)
            .encodings()
            .contains(&Encoding::BYTE_STREAM_SPLIT));
        assert!(reader.schema().index_of(INGEST_SEQ_COLUMN).is_ok());
    }

    struct RejectPublish;

    impl PublishHook for RejectPublish {
        fn before_publish(&self, _temporary_path: &Path) -> Result<()> {
            Err(TsmError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected interruption",
            )))
        }
    }

    #[test]
    fn interruption_before_publish_never_exposes_a_partial_final_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("cpu.parquet");
        let writer = ParquetWriter::new(ParquetWriterConfig::default());

        assert!(writer
            .write_atomic_with_hook(&path, &batch([row(1, "a", 1, 1.0)]), &RejectPublish)
            .is_err());

        assert!(!path.exists());
        assert!(std::fs::read_dir(root.path())
            .expect("list output")
            .next()
            .is_none());
    }

    #[test]
    fn fixed_repeated_dataset_is_smaller_than_the_v02_gorilla_stream() {
        const ROWS: usize = 20_000;
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("repeated.parquet");
        let rows = (0..ROWS).map(|index| {
            row(
                index as u64 + 1,
                if index % 4 == 0 { "edge-a" } else { "edge-b" },
                1_000_000_000 + index as i64 * 1_000_000_000,
                if index % 2 == 0 { 0.0 } else { 1.0 },
            )
        });
        let batch = batch(rows);
        let writer = ParquetWriter::new(ParquetWriterConfig::default());

        let written = writer.write_atomic(&path, &batch).expect("write parquet");
        let gorilla_bytes = v02_gorilla_size_bytes((0..ROWS).map(|index| {
            (
                1_000_000_000 + index as i64 * 1_000_000_000,
                if index % 2 == 0 { 0.0 } else { 1.0 },
            )
        }));

        assert!(
            written.file_bytes() < gorilla_bytes as u64,
            "Parquet {} bytes must be smaller than v0.2 Gorilla {gorilla_bytes} bytes",
            written.file_bytes()
        );
    }

    fn v02_gorilla_size_bytes(points: impl IntoIterator<Item = (i64, f64)>) -> usize {
        let mut timestamp_bits = 0_usize;
        let mut value_bits = 0_usize;
        let mut prior_timestamp: Option<i64> = None;
        let mut prior_delta = 0_i64;
        let mut prior_value: Option<u64> = None;
        let mut prior_leading = 0_u32;
        let mut prior_trailing = 0_u32;

        for (timestamp, value) in points {
            if let Some(previous) = prior_timestamp {
                let delta = timestamp - previous;
                let delta_of_delta = delta - prior_delta;
                timestamp_bits += if delta_of_delta == 0 {
                    1
                } else if (-63..=64).contains(&delta_of_delta) {
                    9
                } else if (-255..=256).contains(&delta_of_delta) {
                    12
                } else if (-2047..=2048).contains(&delta_of_delta) {
                    16
                } else {
                    36
                };
                prior_delta = delta;
            } else {
                timestamp_bits += 64;
            }
            prior_timestamp = Some(timestamp);

            let bits = value.to_bits();
            if let Some(previous) = prior_value {
                let xor = bits ^ previous;
                if xor == 0 {
                    value_bits += 1;
                } else {
                    let leading = xor.leading_zeros();
                    let trailing = xor.trailing_zeros();
                    if leading >= prior_leading && trailing >= prior_trailing {
                        value_bits += 2 + (64 - prior_leading - prior_trailing) as usize;
                    } else {
                        value_bits += 13 + (64 - leading - trailing) as usize;
                        prior_leading = leading;
                        prior_trailing = trailing;
                    }
                }
            } else {
                value_bits += 64;
            }
            prior_value = Some(bits);
        }
        (timestamp_bits + value_bits).div_ceil(8)
    }
}
