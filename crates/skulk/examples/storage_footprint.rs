use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::buffer::{FlushPolicy, MeasurementBuffer};
use alopex_skulk::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> alopex_skulk::Result<()> {
    let root = unique_root()?;
    std::fs::create_dir(&root)?;
    let mut buffer = MeasurementBuffer::new("footprint", FlushPolicy::default());
    buffer.append(SequencedRow::new(
        IngestSeq::new(1),
        WideRow::new(
            SeriesKey::new("footprint", Tags::from([("host".into(), "edge".into())])),
            0,
            Fields::from([("value".into(), FieldValue::Float(1.0))]),
        ),
    ))?;
    let batch = buffer.drain_sorted()?;
    let written = ParquetWriter::new(ParquetWriterConfig::default())
        .write_atomic(root.join("footprint.parquet"), &batch)?;
    assert_eq!(written.row_count(), 1);
    std::fs::remove_file(written.path())?;
    std::fs::remove_dir(&root)?;
    Ok(())
}

fn unique_root() -> alopex_skulk::Result<PathBuf> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            alopex_skulk::TsmError::InvalidInput(format!("system clock precedes epoch: {error}"))
        })?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "alopex-skulk-footprint-{}-{nonce}",
        std::process::id()
    )))
}
