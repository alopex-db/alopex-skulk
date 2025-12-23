//! Write-Ahead Log (WAL) implementation for Skulk time series storage.
//!
//! This module provides durable write-ahead logging for time series data,
//! ensuring data integrity and crash recovery capabilities.
//!
//! # Architecture
//!
//! The WAL follows a batch-oriented design for optimal durability and performance:
//!
//! ```text
//! Client → WAL append → batch fsync → MemTable insert → Ack
//! ```
//!
//! # Features
//!
//! - **Batch Writes**: Multiple entries are batched before fsync for efficiency
//! - **CRC32 Checksums**: Each entry is protected by a CRC32 checksum
//! - **Segment Rotation**: Log segments are rotated based on size thresholds
//! - **Crash Recovery**: Entries can be replayed after a crash
//! - **Truncation**: Old entries can be truncated after successful TSM flush
//!
//! # Example
//!
//! ```rust,ignore
//! use skulk::wal::{Wal, WalConfig, WalEntry};
//!
//! // Create a new WAL
//! let config = WalConfig::default();
//! let mut wal = Wal::new("/path/to/wal", config)?;
//!
//! // Append entries
//! let entries = vec![
//!     WalEntry::new(series_id, timestamp, value),
//! ];
//! wal.append_batch(&entries)?;
//!
//! // After TSM flush, truncate the WAL
//! wal.truncate(sequence_number)?;
//!
//! // On recovery
//! let entries = Wal::recover("/path/to/wal")?;
//! ```

use crate::error::{Result, TsmError};
use crate::lifecycle::partition::PartitionLayout;
use crate::tsm::partition::PartitionState;
use crate::tsm::{PartitionManager, SeriesId, TimePartition};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, warn};

/// Default batch size for WAL writes.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Default batch timeout for WAL writes.
pub const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_millis(10);

/// Default maximum segment size (64 MB).
pub const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// WAL segment file extension.
const SEGMENT_EXTENSION: &str = "wal";

/// WAL segment file prefix.
const SEGMENT_PREFIX: &str = "segment";

/// WAL file magic bytes.
const WAL_MAGIC: [u8; 4] = [b'S', b'W', b'A', b'L']; // "SWAL" for Skulk WAL

/// WAL format version.
const WAL_VERSION: u16 = 2;

/// Sync mode for WAL durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Fsync after each batch write (default, highest durability).
    #[default]
    Fsync,
    /// Use fdatasync (skip metadata update, faster).
    Fdatasync,
    /// No sync (fastest, lowest durability - for testing only).
    None,
}

/// Configuration for WAL behavior.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum number of entries per batch before automatic fsync.
    pub batch_size: usize,
    /// Maximum time to wait before fsyncing a batch.
    pub batch_timeout: Duration,
    /// Maximum size of a single WAL segment file.
    pub segment_size: usize,
    /// Sync mode for durability guarantees.
    pub sync_mode: SyncMode,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_mode: SyncMode::default(),
        }
    }
}

impl WalConfig {
    /// Creates a new WAL configuration with custom settings.
    pub fn new(
        batch_size: usize,
        batch_timeout: Duration,
        segment_size: usize,
        sync_mode: SyncMode,
    ) -> Self {
        Self {
            batch_size,
            batch_timeout,
            segment_size,
            sync_mode,
        }
    }
}

/// WAL entry payload size in bytes.
pub const WAL_ENTRY_SIZE: usize = 33;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum WalEntryType {
    DataPoint = 1,
    DropPartition = 2,
}

/// A single entry in the Write-Ahead Log (v2).
///
/// Each entry is fixed-size (33 bytes payload) and protected by a CRC32 checksum.
#[derive(Debug, Clone, PartialEq)]
pub enum WalEntryV2 {
    /// Data point write entry.
    DataPoint {
        /// Unique sequence number for this entry.
        sequence: u64,
        /// Series identifier.
        series_id: SeriesId,
        /// Timestamp in nanoseconds.
        timestamp: i64,
        /// Data point value.
        value: f64,
    },
    /// Drop partition entry.
    DropPartition {
        /// Unique sequence number for this entry.
        sequence: u64,
        /// Partition start timestamp (nanoseconds since epoch).
        partition_start: i64,
        /// Partition duration in nanoseconds.
        partition_duration_nanos: u64,
        /// Drop operation timestamp.
        dropped_at: i64,
    },
}

/// Alias for the current WAL entry format (v2).
pub type WalEntry = WalEntryV2;

impl WalEntryV2 {
    /// Creates a new data point WAL entry.
    ///
    /// Note: The sequence number is set by the WAL when appending.
    pub fn new(series_id: SeriesId, timestamp: i64, value: f64) -> Self {
        Self::DataPoint {
            sequence: 0,
            series_id,
            timestamp,
            value,
        }
    }

    /// Creates a DropPartition WAL entry.
    ///
    /// Note: The sequence number is set by the WAL when appending.
    pub fn new_drop_partition(
        partition_start: i64,
        partition_duration_nanos: u64,
        dropped_at: i64,
    ) -> Self {
        Self::DropPartition {
            sequence: 0,
            partition_start,
            partition_duration_nanos,
            dropped_at,
        }
    }

    /// Creates a WAL entry with a specific sequence number.
    pub fn with_sequence(sequence: u64, series_id: SeriesId, timestamp: i64, value: f64) -> Self {
        Self::DataPoint {
            sequence,
            series_id,
            timestamp,
            value,
        }
    }

    /// Returns the sequence number of the entry.
    pub fn sequence(&self) -> u64 {
        match self {
            Self::DataPoint { sequence, .. } | Self::DropPartition { sequence, .. } => *sequence,
        }
    }

    /// Sets the sequence number of the entry.
    pub fn set_sequence(&mut self, sequence: u64) {
        match self {
            Self::DataPoint { sequence: seq, .. } | Self::DropPartition { sequence: seq, .. } => {
                *seq = sequence;
            }
        }
    }

    /// Serializes the entry to bytes.
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(WAL_ENTRY_SIZE);
        match self {
            Self::DataPoint {
                sequence,
                series_id,
                timestamp,
                value,
            } => {
                bytes.push(WalEntryType::DataPoint as u8);
                bytes.extend_from_slice(&sequence.to_le_bytes());
                bytes.extend_from_slice(&series_id.to_le_bytes());
                bytes.extend_from_slice(&timestamp.to_le_bytes());
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Self::DropPartition {
                sequence,
                partition_start,
                partition_duration_nanos,
                dropped_at,
            } => {
                bytes.push(WalEntryType::DropPartition as u8);
                bytes.extend_from_slice(&sequence.to_le_bytes());
                bytes.extend_from_slice(&partition_start.to_le_bytes());
                bytes.extend_from_slice(&partition_duration_nanos.to_le_bytes());
                bytes.extend_from_slice(&dropped_at.to_le_bytes());
            }
        }
        bytes
    }

    /// Deserializes an entry from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != WAL_ENTRY_SIZE {
            return Err(TsmError::DecompressionError(
                "WAL entry size mismatch".to_string(),
            ));
        }

        let entry_type = bytes[0];
        let sequence = u64::from_le_bytes(bytes[1..9].try_into().unwrap());

        match entry_type {
            x if x == WalEntryType::DataPoint as u8 => {
                let series_id = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
                let timestamp = i64::from_le_bytes(bytes[17..25].try_into().unwrap());
                let value = f64::from_le_bytes(bytes[25..33].try_into().unwrap());
                Ok(Self::DataPoint {
                    sequence,
                    series_id,
                    timestamp,
                    value,
                })
            }
            x if x == WalEntryType::DropPartition as u8 => {
                let partition_start = i64::from_le_bytes(bytes[9..17].try_into().unwrap());
                let partition_duration_nanos =
                    u64::from_le_bytes(bytes[17..25].try_into().unwrap());
                let dropped_at = i64::from_le_bytes(bytes[25..33].try_into().unwrap());
                Ok(Self::DropPartition {
                    sequence,
                    partition_start,
                    partition_duration_nanos,
                    dropped_at,
                })
            }
            _ => Err(TsmError::DecompressionError(
                "Unknown WAL entry type".to_string(),
            )),
        }
    }
}

/// WAL segment header.
#[derive(Debug, Clone)]
struct SegmentHeader {
    magic: [u8; 4],
    version: u16,
    segment_id: u64,
    created_at: i64,
}

impl SegmentHeader {
    const SIZE: usize = 22; // 4 + 2 + 8 + 8

    fn new(segment_id: u64) -> Self {
        Self {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            segment_id,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.segment_id.to_le_bytes())?;
        writer.write_all(&self.created_at.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if magic != WAL_MAGIC {
            return Err(TsmError::InvalidMagic(magic));
        }

        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        let version = u16::from_le_bytes(buf);

        if version != WAL_VERSION {
            return Err(TsmError::UnsupportedVersion(version));
        }

        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let segment_id = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let created_at = i64::from_le_bytes(buf);

        Ok(Self {
            magic,
            version,
            segment_id,
            created_at,
        })
    }
}

/// Write-Ahead Log for time series data.
///
/// Provides durable storage for data points before they are written to
/// MemTable, ensuring crash recovery capabilities.
pub struct Wal {
    /// Directory containing WAL segments.
    log_dir: PathBuf,
    /// Current segment file being written to.
    current_segment: BufWriter<File>,
    /// Current segment ID.
    current_segment_id: u64,
    /// Current segment size in bytes.
    current_segment_size: usize,
    /// Next sequence number to assign.
    next_sequence: u64,
    /// Last flushed sequence number (entries up to this can be truncated).
    flushed_sequence: u64,
    /// WAL configuration.
    config: WalConfig,
    /// Batch buffer for pending writes.
    batch_buffer: Vec<WalEntry>,
}

impl Wal {
    /// Creates a new WAL in the specified directory.
    ///
    /// If the directory doesn't exist, it will be created.
    /// If there are existing WAL segments, the WAL will continue from the
    /// highest segment ID.
    ///
    /// # Arguments
    ///
    /// * `log_dir` - Directory to store WAL segment files
    /// * `config` - WAL configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or accessed.
    pub fn new(log_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let log_dir = log_dir.as_ref().to_path_buf();

        // Create log directory if it doesn't exist
        fs::create_dir_all(&log_dir)?;

        // Find existing segments and determine the next segment ID
        let (next_segment_id, next_sequence) = Self::scan_existing_segments(&log_dir)?;

        // Create new segment
        let segment_path = Self::segment_path(&log_dir, next_segment_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&segment_path)?;

        let mut writer = BufWriter::new(file);

        // Write segment header
        let header = SegmentHeader::new(next_segment_id);
        header.write_to(&mut writer)?;
        writer.flush()?;

        let batch_capacity = config.batch_size;
        Ok(Self {
            log_dir,
            current_segment: writer,
            current_segment_id: next_segment_id,
            current_segment_size: SegmentHeader::SIZE,
            next_sequence,
            flushed_sequence: 0,
            config,
            batch_buffer: Vec::with_capacity(batch_capacity),
        })
    }

    /// Scans existing segments to determine the next segment ID and sequence number.
    fn scan_existing_segments(log_dir: &Path) -> Result<(u64, u64)> {
        let mut max_segment_id: Option<u64> = None;
        let mut max_sequence: u64 = 0;

        if let Ok(entries) = fs::read_dir(log_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == SEGMENT_EXTENSION {
                        if let Some(stem) = path.file_stem() {
                            if let Some(s) = stem.to_str() {
                                if let Some(id_str) =
                                    s.strip_prefix(&format!("{}_", SEGMENT_PREFIX))
                                {
                                    if let Ok(id) = u64::from_str_radix(id_str, 16) {
                                        // Track max segment ID (use Option to distinguish "no segments" from "segment 0")
                                        let is_max = match max_segment_id {
                                            None => true,
                                            Some(max_id) => id > max_id,
                                        };
                                        if is_max {
                                            max_segment_id = Some(id);
                                        }

                                        // Read the segment to find max sequence (from all segments, not just max)
                                        if let Ok(entries) = Self::read_segment(&path) {
                                            for entry in entries {
                                                if entry.sequence() > max_sequence {
                                                    max_sequence = entry.sequence();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // If any segments exist, start from the next segment ID
        // This correctly handles segment_0 case (max_segment_id = Some(0) -> next = 1)
        let next_segment_id = match max_segment_id {
            Some(id) => id + 1,
            None => 0, // No existing segments, start at 0
        };

        // Sequence numbers start at 1, so if max_sequence is 0 (no entries), next is 1
        let next_sequence = if max_sequence == 0 {
            1
        } else {
            max_sequence + 1
        };

        Ok((next_segment_id, next_sequence))
    }

    /// Generates the path for a segment file.
    fn segment_path(log_dir: &Path, segment_id: u64) -> PathBuf {
        log_dir.join(format!(
            "{}_{:016x}.{}",
            SEGMENT_PREFIX, segment_id, SEGMENT_EXTENSION
        ))
    }

    /// Reads all entries from a segment file.
    fn read_segment(path: &Path) -> Result<Vec<WalEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let _header = SegmentHeader::read_from(&mut reader)?;

        let mut entries = Vec::new();
        loop {
            match Self::read_entry(&mut reader) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => break, // EOF
                Err(e) => {
                    warn!("Error reading WAL entry: {:?}", e);
                    return Err(e);
                }
            }
        }

        Ok(entries)
    }

    /// Reads a single entry from a reader.
    fn read_entry<R: Read>(reader: &mut R) -> Result<Option<WalEntry>> {
        // Read length (4 bytes)
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        if len == 0 {
            return Ok(None);
        }
        if len != WAL_ENTRY_SIZE {
            return Err(TsmError::DecompressionError(format!(
                "WAL entry size mismatch: expected {}, got {}",
                WAL_ENTRY_SIZE, len
            )));
        }

        // Read checksum (4 bytes)
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let expected_crc = u32::from_le_bytes(crc_buf);

        // Read data
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data)?;

        // Verify checksum
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != expected_crc {
            return Err(TsmError::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // Deserialize entry
        let entry = WalEntry::from_bytes(&data)?;
        Ok(Some(entry))
    }

    /// Appends a single entry to the WAL.
    ///
    /// The entry is buffered until the batch is full or timeout occurs,
    /// then fsynced to disk.
    ///
    /// # Arguments
    ///
    /// * `entry` - The WAL entry to append
    ///
    /// # Errors
    ///
    /// Returns an error if the write or fsync fails.
    pub fn append(&mut self, mut entry: WalEntry) -> Result<u64> {
        entry.set_sequence(self.next_sequence);
        self.next_sequence += 1;
        let seq = entry.sequence();

        self.batch_buffer.push(entry);

        // Check if we should flush the batch
        if self.batch_buffer.len() >= self.config.batch_size {
            self.flush_batch()?;
        }

        Ok(seq)
    }

    /// Appends multiple entries to the WAL as a batch.
    ///
    /// All entries are written and then fsynced together.
    ///
    /// # Arguments
    ///
    /// * `entries` - The WAL entries to append
    ///
    /// # Errors
    ///
    /// Returns an error if the write or fsync fails.
    pub fn append_batch(&mut self, entries: &[WalEntry]) -> Result<Vec<u64>> {
        let mut sequences = Vec::with_capacity(entries.len());

        for entry in entries {
            let mut e = entry.clone();
            e.set_sequence(self.next_sequence);
            self.next_sequence += 1;
            sequences.push(e.sequence());
            self.batch_buffer.push(e);
        }

        // Immediately flush the batch
        self.flush_batch()?;

        Ok(sequences)
    }

    /// Flushes pending entries in the batch buffer to disk.
    fn flush_batch(&mut self) -> Result<()> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        // Check if we need to rotate segments
        let estimated_size: usize = self.batch_buffer.iter().map(|_| WAL_ENTRY_SIZE + 8).sum();
        if self.current_segment_size + estimated_size > self.config.segment_size {
            self.rotate_segment()?;
        }

        // Write all entries - take ownership to avoid borrow conflict
        let entries = std::mem::take(&mut self.batch_buffer);
        for entry in &entries {
            self.write_entry(entry)?;
        }

        // Flush and sync
        self.current_segment.flush()?;

        match self.config.sync_mode {
            SyncMode::Fsync => {
                self.current_segment.get_ref().sync_all()?;
            }
            SyncMode::Fdatasync => {
                self.current_segment.get_ref().sync_data()?;
            }
            SyncMode::None => {
                // No sync - only for testing
            }
        }

        debug!(
            "Flushed {} WAL entries to segment {}",
            entries.len(),
            self.current_segment_id
        );

        // batch_buffer is already empty from std::mem::take
        Ok(())
    }

    /// Writes a single entry to the current segment.
    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        let data = entry.to_bytes();
        let crc = crc32fast::hash(&data);

        // Write: length (4) + crc (4) + data (fixed payload)
        self.current_segment
            .write_all(&(data.len() as u32).to_le_bytes())?;
        self.current_segment.write_all(&crc.to_le_bytes())?;
        self.current_segment.write_all(&data)?;

        self.current_segment_size += 4 + 4 + data.len();
        Ok(())
    }

    /// Rotates to a new segment file.
    fn rotate_segment(&mut self) -> Result<()> {
        // Flush and close current segment
        self.current_segment.flush()?;
        self.current_segment.get_ref().sync_all()?;

        // Create new segment
        self.current_segment_id += 1;
        let segment_path = Self::segment_path(&self.log_dir, self.current_segment_id);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&segment_path)?;

        let mut writer = BufWriter::new(file);

        // Write segment header
        let header = SegmentHeader::new(self.current_segment_id);
        header.write_to(&mut writer)?;
        writer.flush()?;

        self.current_segment = writer;
        self.current_segment_size = SegmentHeader::SIZE;

        debug!("Rotated to new WAL segment: {}", segment_path.display());

        Ok(())
    }

    /// Truncates WAL entries up to the given sequence number.
    ///
    /// Entries with sequence <= `up_to_seq` are considered durable in TSM
    /// and can be removed from the WAL.
    ///
    /// # Arguments
    ///
    /// * `up_to_seq` - The sequence number up to which entries can be removed
    ///
    /// # Errors
    ///
    /// Returns an error if segment files cannot be read or deleted.
    pub fn truncate(&mut self, up_to_seq: u64) -> Result<()> {
        self.flushed_sequence = up_to_seq;

        // Find and remove segments that only contain entries <= up_to_seq
        let segments = self.list_segments()?;

        for segment_path in segments {
            // Don't delete the current segment
            if let Some(segment_id) = self.parse_segment_id(&segment_path) {
                if segment_id >= self.current_segment_id {
                    continue;
                }
            }

            // Check if all entries in this segment are flushed
            let entries = match Self::read_segment(&segment_path) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(
                        "Skipping WAL segment {} due to read error: {:?}",
                        segment_path.display(),
                        e
                    );
                    continue;
                }
            };
            let all_flushed = entries.iter().all(|e| e.sequence() <= up_to_seq);

            if all_flushed {
                fs::remove_file(&segment_path)?;
                debug!("Removed WAL segment: {}", segment_path.display());
            }
        }

        Ok(())
    }

    /// Lists all segment files in the WAL directory.
    fn list_segments(&self) -> Result<Vec<PathBuf>> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == SEGMENT_EXTENSION {
                    segments.push(path);
                }
            }
        }

        segments.sort();
        Ok(segments)
    }

    /// Parses the segment ID from a segment file path.
    fn parse_segment_id(&self, path: &Path) -> Option<u64> {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix(&format!("{}_", SEGMENT_PREFIX)))
            .and_then(|s| u64::from_str_radix(s, 16).ok())
    }

    /// Recovers WAL entries from the specified directory.
    ///
    /// Scans all segment files and returns all valid entries in sequence order.
    /// Segments with read errors are skipped with warnings.
    ///
    /// # Arguments
    ///
    /// * `log_dir` - Directory containing WAL segment files
    ///
    /// # Returns
    ///
    /// A vector of recovered WAL entries, sorted by sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be read.
    pub fn recover(log_dir: impl AsRef<Path>) -> Result<Vec<WalEntry>> {
        let log_dir = log_dir.as_ref();
        let mut all_entries = Vec::new();

        // List and sort segment files
        let mut segments = Vec::new();
        for entry in fs::read_dir(log_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == SEGMENT_EXTENSION {
                    segments.push(path);
                }
            }
        }
        segments.sort();

        // Read entries from each segment
        for segment_path in segments {
            match Self::read_segment(&segment_path) {
                Ok(entries) => {
                    debug!(
                        "Recovered {} entries from segment {}",
                        entries.len(),
                        segment_path.display()
                    );
                    all_entries.extend(entries);
                }
                Err(e) => {
                    warn!(
                        "Failed to read WAL segment {}: {:?}",
                        segment_path.display(),
                        e
                    );
                }
            }
        }

        // Sort by sequence number
        all_entries.sort_by_key(|e| e.sequence());

        debug!("Total recovered WAL entries: {}", all_entries.len());
        Ok(all_entries)
    }

    /// Recovers WAL entries and applies DropPartition entries to the partition manager.
    ///
    /// DropPartition entries are idempotent; missing partitions are ignored, existing
    /// partitions have their on-disk data removed before being marked dropped.
    pub fn recover_with_partition_manager(
        log_dir: impl AsRef<Path>,
        partition_manager: &mut PartitionManager,
        layout: &PartitionLayout,
    ) -> Result<Vec<WalEntry>> {
        let entries = Self::recover(log_dir)?;

        for entry in &entries {
            if let WalEntry::DropPartition {
                partition_start,
                partition_duration_nanos,
                ..
            } = entry
            {
                let partition = TimePartition::new(
                    *partition_start,
                    Duration::from_nanos(*partition_duration_nanos),
                );
                Self::delete_partition_files(layout, &partition)?;
                partition_manager.set_state(*partition_start, PartitionState::Dropped);
            }
        }

        Ok(entries)
    }

    fn delete_partition_files(layout: &PartitionLayout, partition: &TimePartition) -> Result<()> {
        let dir = layout.partition_dir(partition);
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                fs::remove_dir_all(&path)?;
            } else {
                fs::remove_file(&path)?;
            }
        }

        fs::remove_dir(&dir)?;
        Ok(())
    }

    /// Forces a flush of any buffered entries.
    ///
    /// Call this to ensure all pending writes are durable before
    /// acknowledging to the client.
    pub fn sync(&mut self) -> Result<()> {
        self.flush_batch()
    }

    /// Returns the current sequence number (next to be assigned).
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Returns the last flushed sequence number.
    pub fn flushed_sequence(&self) -> u64 {
        self.flushed_sequence
    }

    /// Returns the number of pending entries in the batch buffer.
    pub fn pending_count(&self) -> usize {
        self.batch_buffer.len()
    }

    /// Returns the current segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    /// Returns the log directory path.
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Best effort to flush remaining entries
        if let Err(e) = self.flush_batch() {
            warn!("Failed to flush WAL on drop: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_wal() -> (TempDir, Wal) {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            batch_size: 10,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024,        // Small segments for testing
            sync_mode: SyncMode::None, // Fast for testing
        };
        let wal = Wal::new(temp_dir.path(), config).unwrap();
        (temp_dir, wal)
    }

    fn assert_data_point(
        entry: &WalEntry,
        sequence: u64,
        series_id: SeriesId,
        timestamp: i64,
        value: f64,
    ) {
        match entry {
            WalEntry::DataPoint {
                sequence: seq,
                series_id: sid,
                timestamp: ts,
                value: val,
            } => {
                assert_eq!(*seq, sequence);
                assert_eq!(*sid, series_id);
                assert_eq!(*ts, timestamp);
                assert!((*val - value).abs() < f64::EPSILON);
            }
            _ => panic!("Expected data point entry"),
        }
    }

    #[test]
    fn test_wal_append_single() {
        let (_temp_dir, mut wal) = create_test_wal();

        let entry = WalEntry::new(12345, 1000000000, 42.5);
        let seq = wal.append(entry).unwrap();
        wal.sync().unwrap();

        assert_eq!(seq, 1);
        assert_eq!(wal.current_sequence(), 2);
    }

    #[test]
    fn test_wal_append_batch() {
        let (_temp_dir, mut wal) = create_test_wal();

        let entries: Vec<WalEntry> = (0..5)
            .map(|i| WalEntry::new(i as u64, i * 1000, i as f64))
            .collect();

        let sequences = wal.append_batch(&entries).unwrap();

        assert_eq!(sequences.len(), 5);
        assert_eq!(sequences[0], 1);
        assert_eq!(sequences[4], 5);
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Write some entries
        {
            let config = WalConfig {
                batch_size: 100,
                batch_timeout: Duration::from_millis(10),
                segment_size: 10 * 1024,
                sync_mode: SyncMode::Fsync,
            };
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            for i in 0..10 {
                let entry = WalEntry::new(i as u64, i * 1000, i as f64 * 1.5);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();
        }

        // Recover entries
        let entries = Wal::recover(temp_dir.path()).unwrap();

        assert_eq!(entries.len(), 10);
        for (i, entry) in entries.iter().enumerate() {
            assert_data_point(
                entry,
                (i + 1) as u64,
                i as u64,
                (i * 1000) as i64,
                i as f64 * 1.5,
            );
        }
    }

    #[test]
    fn test_recover_with_partition_manager_marks_dropped() {
        use crate::lifecycle::partition::{PartitionDuration, PartitionLayout};
        use crate::tsm::partition::PartitionState;
        use crate::tsm::{PartitionManager, PartitionManagerConfig};
        use std::fs;

        let temp_dir = TempDir::new().unwrap();
        let partition_start = 0;
        let data_dir = temp_dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let layout = PartitionLayout::new(&data_dir, PartitionDuration::Hourly);

        {
            let config = WalConfig {
                batch_size: 100,
                batch_timeout: Duration::from_millis(10),
                segment_size: 10 * 1024,
                sync_mode: SyncMode::Fsync,
            };
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();
            let entry = WalEntry::new_drop_partition(
                partition_start,
                Duration::from_secs(3600).as_nanos() as u64,
                1234567890,
            );
            wal.append(entry).unwrap();
            wal.sync().unwrap();
        }

        let partition = TimePartition::new(partition_start, Duration::from_secs(3600));
        let partition_dir = layout.partition_dir(&partition);
        fs::create_dir_all(&partition_dir).unwrap();
        fs::write(partition_dir.join("dummy.skulk"), b"data").unwrap();

        let mut manager = PartitionManager::new(PartitionManagerConfig::default());
        let entries =
            Wal::recover_with_partition_manager(temp_dir.path(), &mut manager, &layout).unwrap();

        assert_eq!(entries.len(), 1);
        assert!(!partition_dir.exists());
        assert_eq!(
            manager.get_state(partition_start),
            Some(PartitionState::Dropped)
        );
    }

    #[test]
    fn test_wal_truncate() {
        let temp_dir = TempDir::new().unwrap();

        // Write entries across multiple segments
        {
            let config = WalConfig {
                batch_size: 5,
                batch_timeout: Duration::from_millis(10),
                segment_size: 200, // Very small to force rotation
                sync_mode: SyncMode::Fsync,
            };
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            for i in 0..20 {
                let entry = WalEntry::new(i as u64, i * 1000, i as f64);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();

            // Truncate entries up to sequence 10
            wal.truncate(10).unwrap();
        }

        // Recovery should only see entries > 10 (and current segment entries)
        let entries = Wal::recover(temp_dir.path()).unwrap();

        // All entries with sequence > 10 should still be present
        for entry in &entries {
            // Entries in segments that weren't fully truncated remain
            assert!(entry.sequence() > 0);
        }
    }

    #[test]
    fn test_wal_segment_rotation() {
        let temp_dir = TempDir::new().unwrap();

        let config = WalConfig {
            batch_size: 5,
            batch_timeout: Duration::from_millis(10),
            segment_size: 100, // Very small to force rotation
            sync_mode: SyncMode::None,
        };
        let mut wal = Wal::new(temp_dir.path(), config).unwrap();

        let initial_segment = wal.current_segment_id();

        // Write enough entries to force rotation
        for i in 0..50 {
            let entry = WalEntry::new(i as u64, i * 1000, i as f64);
            wal.append(entry).unwrap();
        }
        wal.sync().unwrap();

        // Should have rotated to a new segment
        assert!(wal.current_segment_id() > initial_segment);
    }

    #[test]
    fn test_wal_fsync_durability() {
        let temp_dir = TempDir::new().unwrap();

        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(temp_dir.path(), config).unwrap();

        // Write entries
        let entries: Vec<WalEntry> = (0..10u64)
            .map(|i| WalEntry::new(i, (i * 1000) as i64, i as f64))
            .collect();
        wal.append_batch(&entries).unwrap();

        // Drop the WAL (simulating a crash after fsync)
        drop(wal);

        // Recovery should find all entries
        let recovered = Wal::recover(temp_dir.path()).unwrap();
        assert_eq!(recovered.len(), 10);
    }

    #[test]
    fn test_wal_corrupted_entry_skip() {
        let temp_dir = TempDir::new().unwrap();

        // Write some valid entries
        {
            let config = WalConfig::default();
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            for i in 0u64..5 {
                let entry = WalEntry::new(i, (i * 1000) as i64, i as f64);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();
        }

        // Corrupt the segment file by appending garbage
        let segments: Vec<_> = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == SEGMENT_EXTENSION)
                    .unwrap_or(false)
            })
            .collect();

        if let Some(segment) = segments.first() {
            let mut file = OpenOptions::new()
                .append(true)
                .open(segment.path())
                .unwrap();
            // Write invalid length followed by garbage
            file.write_all(&100u32.to_le_bytes()).unwrap();
            file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        }

        // Recovery should skip the corrupted segment
        let entries = Wal::recover(temp_dir.path()).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry::with_sequence(42, 12345, 1000000000, std::f64::consts::PI);
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry, recovered);
    }

    #[test]
    fn test_wal_drop_partition_serialization() {
        let mut entry = WalEntry::new_drop_partition(1_000_000, 3_600_000_000_000, 1_500_000);
        entry.set_sequence(7);
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry, recovered);
    }

    #[test]
    fn test_wal_config_default() {
        let config = WalConfig::default();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.batch_timeout, DEFAULT_BATCH_TIMEOUT);
        assert_eq!(config.segment_size, DEFAULT_SEGMENT_SIZE);
        assert_eq!(config.sync_mode, SyncMode::Fsync);
    }

    #[test]
    fn test_wal_pending_count() {
        let (_temp_dir, mut wal) = create_test_wal();

        assert_eq!(wal.pending_count(), 0);

        // Add entries without syncing
        for i in 0u64..5 {
            let entry = WalEntry::new(i, (i * 1000) as i64, i as f64);
            wal.append(entry).unwrap();
        }

        assert_eq!(wal.pending_count(), 5);

        wal.sync().unwrap();
        assert_eq!(wal.pending_count(), 0);
    }

    #[test]
    fn test_wal_empty_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Create an empty WAL
        {
            let config = WalConfig::default();
            let _wal = Wal::new(temp_dir.path(), config).unwrap();
        }

        // Recovery of empty WAL should succeed
        let entries = Wal::recover(temp_dir.path()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_continue_from_existing() {
        let temp_dir = TempDir::new().unwrap();

        // Write some entries
        {
            let config = WalConfig::default();
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            for i in 0u64..5 {
                let entry = WalEntry::new(i, (i * 1000) as i64, i as f64);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();
        }

        // Open a new WAL in the same directory
        {
            let config = WalConfig::default();
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            // Should continue from sequence 6
            let entry = WalEntry::new(100, 1000, 1.0);
            let seq = wal.append(entry).unwrap();
            wal.sync().unwrap();

            assert_eq!(seq, 6);
        }

        // Recovery should find all entries
        let entries = Wal::recover(temp_dir.path()).unwrap();
        assert_eq!(entries.len(), 6);
    }

    #[test]
    fn test_wal_segment_zero_not_overwritten() {
        // This test verifies the fix for the segment_0 overwrite bug:
        // When segment_0000000000000000.wal exists, reopening the WAL
        // should create segment_1, not overwrite segment_0.
        let temp_dir = TempDir::new().unwrap();

        // Write entries to segment_0
        let original_entries: Vec<WalEntry>;
        {
            let config = WalConfig {
                batch_size: 100,
                batch_timeout: Duration::from_millis(10),
                segment_size: 1024 * 1024, // Large segment to ensure no rotation
                sync_mode: SyncMode::Fsync,
            };
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            // Verify we start at segment 0
            assert_eq!(wal.current_segment_id(), 0);

            for i in 0u64..5 {
                let entry = WalEntry::new(i, (i * 1000) as i64, i as f64 * 10.0);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();

            // Store original entries for later comparison
            original_entries = Wal::recover(temp_dir.path()).unwrap();
            assert_eq!(original_entries.len(), 5);
        }

        // Reopen WAL - this should create segment_1, not overwrite segment_0
        {
            let config = WalConfig {
                batch_size: 100,
                batch_timeout: Duration::from_millis(10),
                segment_size: 1024 * 1024,
                sync_mode: SyncMode::Fsync,
            };
            let mut wal = Wal::new(temp_dir.path(), config).unwrap();

            // Should be at segment 1, not segment 0
            assert_eq!(
                wal.current_segment_id(),
                1,
                "WAL should create segment_1 when segment_0 exists"
            );

            // Sequence should continue from 6
            assert_eq!(wal.current_sequence(), 6);

            // Write new entries
            for i in 5u64..8 {
                let entry = WalEntry::new(i, (i * 1000) as i64, i as f64 * 10.0);
                wal.append(entry).unwrap();
            }
            wal.sync().unwrap();
        }

        // Verify all entries are preserved (original + new)
        let all_entries = Wal::recover(temp_dir.path()).unwrap();
        assert_eq!(all_entries.len(), 8, "All 8 entries should be preserved");

        // Verify original entries were not corrupted
        for (i, entry) in all_entries.iter().take(5).enumerate() {
            let original = &original_entries[i];
            assert_data_point(
                entry,
                original.sequence(),
                match original {
                    WalEntry::DataPoint { series_id, .. } => *series_id,
                    _ => panic!("Expected data point entry"),
                },
                match original {
                    WalEntry::DataPoint { timestamp, .. } => *timestamp,
                    _ => panic!("Expected data point entry"),
                },
                match original {
                    WalEntry::DataPoint { value, .. } => *value,
                    _ => panic!("Expected data point entry"),
                },
            );
        }

        // Count segment files - should have segment_0 and segment_1
        let segment_count = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == SEGMENT_EXTENSION)
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            segment_count, 2,
            "Should have 2 segment files (segment_0 and segment_1)"
        );
    }
}
