//! Durable write-ahead log for wide rows.

use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use crc32fast::Hasher as Crc32;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Name of the v0.3 WAL file below the data root.
pub const WAL_FILE_NAME: &str = "wal-v3.log";
const CHECKPOINT_PREFIX: &str = ".wal-v3.log.checkpoint-";

const WAL_MAGIC: [u8; 4] = *b"SKW3";
const WAL_VERSION: u16 = 1;
const HEADER_BYTES: u64 = 6;
const MIN_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SYNC_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_MAX_ENTRY_BYTES: usize = 1024 * 1024;
static CHECKPOINT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Injectable monotonic time source used by interval-based synchronization.
pub trait TimeSource: Send + Sync {
    /// Returns monotonic elapsed time from an arbitrary fixed origin.
    fn now(&self) -> Duration;
}

struct SystemTimeSource {
    started: Instant,
}

impl SystemTimeSource {
    fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl TimeSource for SystemTimeSource {
    fn now(&self) -> Duration {
        self.started.elapsed()
    }
}

/// Validated WAL resource and synchronization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalConfig {
    sync_interval: Duration,
    max_entry_bytes: usize,
}

impl WalConfig {
    /// Creates a policy with a sync interval from one second through one minute.
    pub fn new(sync_interval: Duration, max_entry_bytes: usize) -> Result<Self> {
        if !(MIN_SYNC_INTERVAL..=MAX_SYNC_INTERVAL).contains(&sync_interval) {
            return Err(TsmError::InvalidInput(format!(
                "WAL sync interval must be between {MIN_SYNC_INTERVAL:?} and {MAX_SYNC_INTERVAL:?}"
            )));
        }
        if max_entry_bytes == 0 || max_entry_bytes > u32::MAX as usize {
            return Err(TsmError::InvalidInput(
                "WAL max entry bytes must be in 1..=u32::MAX".into(),
            ));
        }
        Ok(Self {
            sync_interval,
            max_entry_bytes,
        })
    }

    /// Returns the configured synchronization interval.
    pub const fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Returns the maximum encoded payload size.
    pub const fn max_entry_bytes(&self) -> usize {
        self.max_entry_bytes
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            sync_interval: MIN_SYNC_INTERVAL,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        }
    }
}

/// One sequenced wide row stored in the WAL.
#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    sequence: u64,
    row: WideRow,
}

impl WalEntry {
    /// Creates a WAL entry with a caller-assigned row sequence.
    pub const fn new(sequence: u64, row: WideRow) -> Self {
        Self { sequence, row }
    }

    /// Returns the row sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the wide row.
    pub const fn row(&self) -> &WideRow {
        &self.row
    }

    /// Splits the entry into its owned sequence and row.
    pub fn into_parts(self) -> (u64, WideRow) {
        (self.sequence, self.row)
    }
}

/// Durability state returned by an interval-controlled append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendDurability {
    /// The entry is written but must not be acknowledged yet.
    Pending(u64),
    /// All entries through this sequence were synchronized and may be acknowledged.
    DurableThrough(u64),
}

/// Why recovery stopped at an incomplete or corrupt tail frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailStopReason {
    /// Fewer than four bytes remained for the frame length.
    PartialLength,
    /// The declared frame exceeded the configured allocation limit.
    OversizedFrame,
    /// The payload ended before its declared length.
    PartialPayload,
    /// The frame checksum was incomplete.
    PartialChecksum,
    /// The frame checksum did not match the payload.
    ChecksumMismatch,
    /// The payload could not be decoded without violating its schema.
    InvalidPayload,
}

/// Valid prefix and diagnostics produced by WAL recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryReport {
    entries: Vec<WalEntry>,
    valid_bytes: u64,
    stop_reason: Option<TailStopReason>,
}

impl RecoveryReport {
    /// Returns all entries decoded from the valid prefix.
    pub fn entries(&self) -> &[WalEntry] {
        &self.entries
    }

    /// Returns the byte length through the last valid frame.
    pub const fn valid_bytes(&self) -> u64 {
        self.valid_bytes
    }

    /// Returns the tail condition, if recovery stopped on a torn frame.
    pub const fn stop_reason(&self) -> Option<TailStopReason> {
        self.stop_reason
    }
}

/// Wide-row WAL whose durable append boundary is explicit.
pub struct Wal {
    path: PathBuf,
    file: File,
    config: WalConfig,
    time: Arc<dyn TimeSource>,
    last_sync: Duration,
    pending_through: Option<u64>,
    durable_through: Option<u64>,
    recovered: Vec<WalEntry>,
    poisoned: bool,
}

impl Wal {
    /// Opens a WAL using the system monotonic clock.
    pub fn open(root: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Self::open_with_time_source(root, config, Arc::new(SystemTimeSource::new()))
    }

    /// Opens a WAL with an injected time source.
    pub fn open_with_time_source(
        root: impl AsRef<Path>,
        config: WalConfig,
        time: Arc<dyn TimeSource>,
    ) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        cleanup_checkpoint_files(root)?;
        let path = root.join(WAL_FILE_NAME);

        if !path.exists() || fs::metadata(&path)?.len() == 0 {
            initialize_file(&path)?;
            sync_directory(root)?;
        }

        let report = recover(&path, config.max_entry_bytes)?;
        if report.stop_reason.is_some() {
            let repair = OpenOptions::new().write(true).open(&path)?;
            repair.set_len(report.valid_bytes)?;
            repair.sync_all()?;
        }

        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let durable_through = report.entries.iter().map(WalEntry::sequence).max();
        let last_sync = time.now();

        Ok(Self {
            path,
            file,
            config,
            time,
            last_sync,
            pending_through: None,
            durable_through,
            recovered: report.entries,
            poisoned: false,
        })
    }

    /// Appends an entry and synchronizes only when the configured interval is due.
    ///
    /// A `Pending` result is not an acknowledgement boundary.
    pub fn append_buffered(&mut self, entry: &WalEntry) -> Result<AppendDurability> {
        self.write_entry(entry)?;
        let now = self.time.now();
        let elapsed = now.checked_sub(self.last_sync).unwrap_or_default();
        if elapsed >= self.config.sync_interval {
            self.sync()?;
            Ok(AppendDurability::DurableThrough(entry.sequence))
        } else {
            Ok(AppendDurability::Pending(entry.sequence))
        }
    }

    /// Appends and synchronizes an entry before returning its acknowledgeable sequence.
    pub fn append_durable(&mut self, entry: &WalEntry) -> Result<u64> {
        self.write_entry(entry)?;
        self.sync()?;
        Ok(entry.sequence)
    }

    /// Synchronizes every pending frame and returns the durable sequence boundary.
    pub fn sync(&mut self) -> Result<Option<u64>> {
        self.ensure_writable()?;
        self.file.sync_data()?;
        if let Some(pending) = self.pending_through.take() {
            self.durable_through = Some(pending);
        }
        self.last_sync = self.time.now();
        Ok(self.durable_through)
    }

    /// Returns all retained entries, including appends made on this handle.
    pub fn recovered_entries(&self) -> &[WalEntry] {
        &self.recovered
    }

    /// Atomically removes entries at or below a manifest-persisted sequence.
    pub fn checkpoint_through(&mut self, persisted_through: u64) -> Result<()> {
        self.ensure_writable()?;
        self.sync()?;
        let retained = self
            .recovered
            .iter()
            .filter(|entry| entry.sequence > persisted_through)
            .cloned()
            .collect::<Vec<_>>();
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let checkpoint = parent.join(format!(
            "{CHECKPOINT_PREFIX}{}-{}",
            std::process::id(),
            CHECKPOINT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut cleanup = CheckpointFile::new(checkpoint.clone());
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&checkpoint)?;
        file.write_all(&WAL_MAGIC)?;
        file.write_all(&WAL_VERSION.to_le_bytes())?;
        for entry in &retained {
            write_frame(&mut file, entry, self.config.max_entry_bytes)?;
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&checkpoint, &self.path)?;
        cleanup.disarm();
        sync_directory(parent)?;
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.recovered = retained;
        self.pending_through = None;
        self.durable_through = self.recovered.iter().map(WalEntry::sequence).max();
        self.last_sync = self.time.now();
        Ok(())
    }

    /// Returns the latest sequence known to be synchronized.
    pub const fn durable_through(&self) -> Option<u64> {
        self.durable_through
    }

    /// Returns the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        self.ensure_writable()?;
        let payload = encode_entry(entry, self.config.max_entry_bytes)?;
        let mut crc = Crc32::new();
        crc.update(&payload);
        let checksum = crc.finalize();

        let write_result = (|| -> std::io::Result<()> {
            self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
            self.file.write_all(&payload)?;
            self.file.write_all(&checksum.to_le_bytes())
        })();
        if let Err(error) = write_result {
            self.poisoned = true;
            return Err(error.into());
        }
        self.pending_through = Some(
            self.pending_through
                .map_or(entry.sequence, |pending| pending.max(entry.sequence)),
        );
        self.recovered.push(entry.clone());
        Ok(())
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.poisoned {
            return Err(TsmError::Corruption(
                "WAL handle is poisoned after a partial write".into(),
            ));
        }
        Ok(())
    }
}

fn cleanup_checkpoint_files(root: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(CHECKPOINT_PREFIX)
        {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_file() || file_type.is_symlink() {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_directory(root)?;
    }
    Ok(())
}

fn write_frame(file: &mut File, entry: &WalEntry, max_entry_bytes: usize) -> Result<()> {
    let payload = encode_entry(entry, max_entry_bytes)?;
    let mut crc = Crc32::new();
    crc.update(&payload);
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&payload)?;
    file.write_all(&crc.finalize().to_le_bytes())?;
    Ok(())
}

struct CheckpointFile {
    path: PathBuf,
    armed: bool,
}

impl CheckpointFile {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CheckpointFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Recovers the valid prefix of one v0.3 WAL file.
pub fn recover(path: impl AsRef<Path>, max_entry_bytes: usize) -> Result<RecoveryReport> {
    if max_entry_bytes == 0 || max_entry_bytes > u32::MAX as usize {
        return Err(TsmError::InvalidInput(
            "WAL max entry bytes must be in 1..=u32::MAX".into(),
        ));
    }
    let mut file = File::open(path)?;
    read_and_validate_header(&mut file)?;
    let mut report = RecoveryReport {
        entries: Vec::new(),
        valid_bytes: HEADER_BYTES,
        stop_reason: None,
    };

    loop {
        let mut length_bytes = [0_u8; 4];
        match read_exact_or_tail(&mut file, &mut length_bytes)? {
            ReadState::Complete => {}
            ReadState::CleanEof => return Ok(report),
            ReadState::Partial => {
                report.stop_reason = Some(TailStopReason::PartialLength);
                return Ok(report);
            }
        }
        let payload_len = u32::from_le_bytes(length_bytes) as usize;
        if payload_len > max_entry_bytes {
            report.stop_reason = Some(TailStopReason::OversizedFrame);
            return Ok(report);
        }

        let mut payload = vec![0_u8; payload_len];
        if read_exact_or_tail(&mut file, &mut payload)? != ReadState::Complete {
            report.stop_reason = Some(TailStopReason::PartialPayload);
            return Ok(report);
        }

        let mut checksum_bytes = [0_u8; 4];
        if read_exact_or_tail(&mut file, &mut checksum_bytes)? != ReadState::Complete {
            report.stop_reason = Some(TailStopReason::PartialChecksum);
            return Ok(report);
        }
        let expected = u32::from_le_bytes(checksum_bytes);
        let mut crc = Crc32::new();
        crc.update(&payload);
        if crc.finalize() != expected {
            report.stop_reason = Some(TailStopReason::ChecksumMismatch);
            return Ok(report);
        }

        let entry = match decode_entry(&payload) {
            Ok(entry) => entry,
            Err(_) => {
                report.stop_reason = Some(TailStopReason::InvalidPayload);
                return Ok(report);
            }
        };
        report.entries.push(entry);
        report.valid_bytes += 4 + payload_len as u64 + 4;
    }
}

fn initialize_file(path: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(&WAL_MAGIC)?;
    file.write_all(&WAL_VERSION.to_le_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn read_and_validate_header(file: &mut File) -> Result<()> {
    let mut header = [0_u8; HEADER_BYTES as usize];
    file.read_exact(&mut header).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            TsmError::InvalidFormat("truncated WAL header".into())
        } else {
            error.into()
        }
    })?;
    if header[..4] != WAL_MAGIC {
        let detail = if header[..4] == *b"SWAL" {
            "v0.2 WAL is unsupported"
        } else {
            "invalid WAL magic"
        };
        return Err(TsmError::InvalidFormat(detail.into()));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != WAL_VERSION {
        return Err(TsmError::InvalidFormat(format!(
            "unsupported WAL version {version}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadState {
    Complete,
    CleanEof,
    Partial,
}

fn read_exact_or_tail(reader: &mut File, target: &mut [u8]) -> Result<ReadState> {
    let mut read = 0;
    while read < target.len() {
        match reader.read(&mut target[read..]) {
            Ok(0) if read == 0 => return Ok(ReadState::CleanEof),
            Ok(0) => return Ok(ReadState::Partial),
            Ok(count) => read += count,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(ReadState::Complete)
}

fn encode_entry(entry: &WalEntry, max_entry_bytes: usize) -> Result<Vec<u8>> {
    let size = encoded_size(entry)?;
    if size > max_entry_bytes {
        return Err(TsmError::ResourceLimit(format!(
            "encoded WAL entry is {size} bytes, limit is {max_entry_bytes}"
        )));
    }

    let mut output = Vec::with_capacity(size);
    output.extend_from_slice(&entry.sequence.to_le_bytes());
    output.extend_from_slice(&entry.row.timestamp().to_le_bytes());
    encode_string(&mut output, entry.row.series().measurement())?;
    output.extend_from_slice(&count_u32(entry.row.series().tags().len())?.to_le_bytes());
    for (name, value) in entry.row.series().tags() {
        encode_string(&mut output, name)?;
        encode_string(&mut output, value)?;
    }
    output.extend_from_slice(&count_u32(entry.row.fields().len())?.to_le_bytes());
    for (name, value) in entry.row.fields() {
        encode_string(&mut output, name)?;
        match value {
            FieldValue::Float(value) => {
                output.push(1);
                output.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            FieldValue::Integer(value) => {
                output.push(2);
                output.extend_from_slice(&value.to_le_bytes());
            }
            FieldValue::Unsigned(value) => {
                output.push(3);
                output.extend_from_slice(&value.to_le_bytes());
            }
            FieldValue::Boolean(value) => {
                output.push(4);
                output.push(u8::from(*value));
            }
            FieldValue::String(value) => {
                output.push(5);
                encode_string(&mut output, value)?;
            }
        }
    }
    debug_assert_eq!(output.len(), size);
    Ok(output)
}

fn encoded_size(entry: &WalEntry) -> Result<usize> {
    let measurement_size = string_size(entry.row.series().measurement())?;
    let mut size = 8_usize
        .checked_add(8)
        .and_then(|value| value.checked_add(measurement_size))
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| TsmError::ResourceLimit("WAL entry size overflow".into()))?;
    count_u32(entry.row.series().tags().len())?;
    for (name, value) in entry.row.series().tags() {
        size = checked_size_add(size, string_size(name)?)?;
        size = checked_size_add(size, string_size(value)?)?;
    }
    size = checked_size_add(size, 4)?;
    count_u32(entry.row.fields().len())?;
    for (name, value) in entry.row.fields() {
        size = checked_size_add(size, string_size(name)?)?;
        size = checked_size_add(size, 1)?;
        let value_size = match value {
            FieldValue::Float(_) | FieldValue::Integer(_) | FieldValue::Unsigned(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::String(value) => string_size(value)?,
        };
        size = checked_size_add(size, value_size)?;
    }
    Ok(size)
}

fn checked_size_add(current: usize, additional: usize) -> Result<usize> {
    current
        .checked_add(additional)
        .ok_or_else(|| TsmError::ResourceLimit("WAL entry size overflow".into()))
}

fn string_size(value: &str) -> Result<usize> {
    count_u32(value.len())?;
    4_usize
        .checked_add(value.len())
        .ok_or_else(|| TsmError::ResourceLimit("WAL string size overflow".into()))
}

fn count_u32(count: usize) -> Result<u32> {
    u32::try_from(count)
        .map_err(|_| TsmError::ResourceLimit("WAL collection count exceeds u32::MAX".into()))
}

fn encode_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = count_u32(value.len())?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_entry(payload: &[u8]) -> Result<WalEntry> {
    let mut input = PayloadCursor::new(payload);
    let sequence = input.u64()?;
    let timestamp = input.i64()?;
    let measurement = input.string()?;
    let tag_count = input.u32()? as usize;
    let mut tags = Tags::new();
    for _ in 0..tag_count {
        let name = input.string()?;
        let value = input.string()?;
        if tags.insert(name, value).is_some() {
            return Err(TsmError::Serialization(
                "duplicate tag in WAL payload".into(),
            ));
        }
    }
    let field_count = input.u32()? as usize;
    let mut fields = Fields::new();
    for _ in 0..field_count {
        let name = input.string()?;
        let value = match input.u8()? {
            1 => FieldValue::Float(f64::from_bits(input.u64()?)),
            2 => FieldValue::Integer(input.i64()?),
            3 => FieldValue::Unsigned(input.u64()?),
            4 => match input.u8()? {
                0 => FieldValue::Boolean(false),
                1 => FieldValue::Boolean(true),
                _ => {
                    return Err(TsmError::Serialization(
                        "invalid boolean in WAL payload".into(),
                    ));
                }
            },
            5 => FieldValue::String(input.string()?),
            kind => {
                return Err(TsmError::Serialization(format!(
                    "unknown field type {kind}"
                )));
            }
        };
        if fields.insert(name, value).is_some() {
            return Err(TsmError::Serialization(
                "duplicate field in WAL payload".into(),
            ));
        }
    }
    if !input.is_empty() {
        return Err(TsmError::Serialization(
            "trailing bytes in WAL payload".into(),
        ));
    }
    Ok(WalEntry::new(
        sequence,
        WideRow::new(SeriesKey::new(measurement, tags), timestamp, fields),
    ))
}

struct PayloadCursor<'a> {
    payload: &'a [u8],
    position: usize,
}

impl<'a> PayloadCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            position: 0,
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| TsmError::Serialization("WAL payload offset overflow".into()))?;
        let value = self
            .payload
            .get(self.position..end)
            .ok_or_else(|| TsmError::Serialization("truncated WAL payload".into()))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| TsmError::Serialization("invalid u32".into()))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| TsmError::Serialization("invalid u64".into()))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| TsmError::Serialization("invalid i64".into()))?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn string(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| TsmError::Serialization("invalid UTF-8 in WAL payload".into()))
    }

    fn is_empty(&self) -> bool {
        self.position == self.payload.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const CRASH_ROOT_ENV: &str = "SKULK_WAL_CRASH_TEST_ROOT";
    const ACK_MARKER_FILE: &str = "durable-append-returned";

    #[derive(Default)]
    struct ManualTime {
        millis: AtomicU64,
    }

    impl ManualTime {
        fn advance(&self, duration: Duration) {
            self.millis
                .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        }
    }

    impl TimeSource for ManualTime {
        fn now(&self) -> Duration {
            Duration::from_millis(self.millis.load(Ordering::SeqCst))
        }
    }

    fn row(value: i64) -> WideRow {
        WideRow::new(
            SeriesKey::new("cpu", Tags::from([("host".into(), "edge-1".into())])),
            value,
            Fields::from([("usage".into(), FieldValue::Integer(value))]),
        )
    }

    fn config() -> WalConfig {
        WalConfig::new(Duration::from_secs(1), 64 * 1024).expect("valid config")
    }

    #[test]
    fn durable_append_is_recovered_after_reopen() {
        let root = tempfile::tempdir().expect("tempdir");
        let entry = WalEntry::new(7, row(42));
        let mut wal = Wal::open(root.path(), config()).expect("open WAL");

        assert_eq!(wal.append_durable(&entry).expect("durable append"), 7);
        drop(wal);

        let reopened = Wal::open(root.path(), config()).expect("reopen WAL");
        assert_eq!(reopened.recovered_entries(), &[entry]);
        assert_eq!(reopened.durable_through(), Some(7));
    }

    #[test]
    fn acknowledged_entry_survives_forced_process_exit() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "store::wal::tests::forced_exit_writer_child",
                "--nocapture",
            ])
            .env(CRASH_ROOT_ENV, root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn crash writer");
        let marker = root.path().join(ACK_MARKER_FILE);

        for _ in 0..500 {
            if marker.exists() {
                break;
            }
            if child.try_wait().expect("query child").is_some() {
                panic!("crash writer exited before reporting durable append");
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            marker.exists(),
            "crash writer did not report durable append in time"
        );
        child.kill().expect("force-kill crash writer");
        child.wait().expect("reap crash writer");

        let reopened = Wal::open(root.path(), config()).expect("reopen after forced exit");
        assert_eq!(reopened.recovered_entries(), &[WalEntry::new(9, row(99))]);
    }

    #[test]
    fn forced_exit_writer_child() {
        let Some(root) = std::env::var_os(CRASH_ROOT_ENV) else {
            return;
        };
        let root = PathBuf::from(root);
        let mut wal = Wal::open(&root, config()).expect("open child WAL");
        wal.append_durable(&WalEntry::new(9, row(99)))
            .expect("durable child append");
        let marker = File::create(root.join(ACK_MARKER_FILE)).expect("create ack marker");
        marker.sync_all().expect("persist ack marker");
        loop {
            thread::park();
        }
    }

    #[test]
    fn recovery_stops_at_a_torn_tail_and_open_truncates_it() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(root.path(), config()).expect("open WAL");
        wal.append_durable(&WalEntry::new(1, row(1)))
            .expect("first append");
        wal.append_durable(&WalEntry::new(2, row(2)))
            .expect("second append");
        drop(wal);

        let path = root.path().join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("append torn bytes");
        file.write_all(&[32, 0, 0]).expect("partial frame length");
        file.sync_data().expect("persist torn tail");
        drop(file);

        let report = recover(&path, config().max_entry_bytes()).expect("recover prefix");
        assert_eq!(
            report.entries(),
            &[WalEntry::new(1, row(1)), WalEntry::new(2, row(2))]
        );
        assert_eq!(report.stop_reason(), Some(TailStopReason::PartialLength));

        let valid_bytes = report.valid_bytes();
        let reopened = Wal::open(root.path(), config()).expect("open repairs tail");
        assert_eq!(reopened.recovered_entries().len(), 2);
        assert_eq!(
            std::fs::metadata(path).expect("metadata").len(),
            valid_bytes
        );
    }

    #[test]
    fn recovery_limits_checksum_corruption_to_the_tail() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(root.path(), config()).expect("open WAL");
        wal.append_durable(&WalEntry::new(1, row(1)))
            .expect("first append");
        wal.append_durable(&WalEntry::new(2, row(2)))
            .expect("second append");
        drop(wal);

        let path = root.path().join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open for corruption");
        file.seek(SeekFrom::End(-1)).expect("seek checksum");
        file.write_all(&[0x7f]).expect("corrupt checksum");
        file.sync_data().expect("persist corruption");
        drop(file);

        let report = recover(&path, config().max_entry_bytes()).expect("recover valid prefix");
        assert_eq!(report.entries(), &[WalEntry::new(1, row(1))]);
        assert_eq!(report.stop_reason(), Some(TailStopReason::ChecksumMismatch));
    }

    #[test]
    fn recovery_rejects_an_oversized_declared_frame_before_allocation() {
        let root = tempfile::tempdir().expect("tempdir");
        let wal = Wal::open(root.path(), config()).expect("open WAL");
        let path = wal.path().to_owned();
        drop(wal);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("append frame declaration");
        file.write_all(&(u32::MAX).to_le_bytes())
            .expect("write oversized declaration");
        file.sync_data().expect("persist declaration");
        drop(file);

        let report = recover(&path, config().max_entry_bytes()).expect("bounded recovery");
        assert!(report.entries().is_empty());
        assert_eq!(report.stop_reason(), Some(TailStopReason::OversizedFrame));
        assert_eq!(report.valid_bytes(), HEADER_BYTES);
    }

    #[test]
    fn wal_round_trips_sparse_rows_and_every_field_type() {
        let root = tempfile::tempdir().expect("tempdir");
        let entry = WalEntry::new(
            11,
            WideRow::new(
                SeriesKey::new(
                    "weather",
                    Tags::from([
                        ("host".into(), "edge-1".into()),
                        ("region".into(), "北".into()),
                    ]),
                ),
                -42,
                Fields::from([
                    ("active".into(), FieldValue::Boolean(true)),
                    ("count".into(), FieldValue::Integer(-7)),
                    ("note".into(), FieldValue::String("sparse".into())),
                    ("ratio".into(), FieldValue::Float(1.25)),
                    ("total".into(), FieldValue::Unsigned(9)),
                ]),
            ),
        );
        let mut wal = Wal::open(root.path(), config()).expect("open WAL");
        wal.append_durable(&entry).expect("append typed row");
        drop(wal);

        let reopened = Wal::open(root.path(), config()).expect("reopen typed row");
        assert_eq!(reopened.recovered_entries(), &[entry]);
    }

    #[test]
    fn interval_sync_never_reports_a_pending_append_as_durable() {
        let root = tempfile::tempdir().expect("tempdir");
        let time = Arc::new(ManualTime::default());
        let mut wal =
            Wal::open_with_time_source(root.path(), config(), time.clone()).expect("open WAL");

        assert_eq!(
            wal.append_buffered(&WalEntry::new(1, row(1)))
                .expect("pending append"),
            AppendDurability::Pending(1)
        );
        assert_eq!(wal.durable_through(), None);

        time.advance(Duration::from_secs(1));
        assert_eq!(
            wal.append_buffered(&WalEntry::new(2, row(2)))
                .expect("due append"),
            AppendDurability::DurableThrough(2)
        );
        assert_eq!(wal.durable_through(), Some(2));
    }

    #[test]
    fn oversized_entry_is_rejected_before_the_log_grows() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = WalConfig::new(Duration::from_secs(1), 64).expect("valid config");
        let mut wal = Wal::open(root.path(), config).expect("open WAL");
        let before = std::fs::metadata(wal.path()).expect("metadata").len();
        let oversized = WideRow::new(
            SeriesKey::new("cpu", Tags::new()),
            1,
            Fields::from([("message".into(), FieldValue::String("x".repeat(512)))]),
        );

        assert!(matches!(
            wal.append_durable(&WalEntry::new(1, oversized)),
            Err(crate::TsmError::ResourceLimit(_))
        ));
        assert_eq!(
            std::fs::metadata(wal.path()).expect("metadata").len(),
            before
        );
    }

    #[test]
    fn sync_interval_is_bounded_by_the_reliability_contract() {
        assert!(WalConfig::new(Duration::from_millis(999), 1024).is_err());
        assert!(WalConfig::new(Duration::from_secs(61), 1024).is_err());
        assert!(WalConfig::new(Duration::from_secs(1), 1024).is_ok());
        assert!(WalConfig::new(Duration::from_secs(60), 1024).is_ok());
    }

    #[test]
    fn checkpoint_removes_only_the_persisted_sequence_prefix() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(root.path(), config()).expect("open WAL");
        for sequence in 1..=4 {
            wal.append_durable(&WalEntry::new(sequence, row(sequence as i64)))
                .expect("append");
        }

        wal.checkpoint_through(2).expect("checkpoint");
        drop(wal);

        let reopened = Wal::open(root.path(), config()).expect("reopen");
        assert_eq!(
            reopened
                .recovered_entries()
                .iter()
                .map(WalEntry::sequence)
                .collect::<Vec<_>>(),
            [3, 4]
        );
    }

    #[test]
    fn open_removes_only_scoped_checkpoint_artifacts() {
        let root = tempfile::tempdir().expect("tempdir");
        let checkpoint = root.path().join(".wal-v3.log.checkpoint-123-4");
        let unrelated = root.path().join("checkpoint-user-data");
        fs::write(&checkpoint, b"reproducible partial checkpoint").expect("checkpoint");
        fs::write(&unrelated, b"keep").expect("unrelated");

        let _wal = Wal::open(root.path(), config()).expect("open WAL");

        assert!(!checkpoint.exists());
        assert!(unrelated.exists());
    }
}
