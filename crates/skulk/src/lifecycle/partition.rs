//! Time-based partition layout for on-disk organization.

use crate::error::Result;
use crate::tsm::{SeriesId, TimePartition, Timestamp};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Supported partition durations for on-disk layout.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::partition::PartitionDuration;
///
/// let hourly = PartitionDuration::Hourly;
/// let nanos = hourly.as_nanos();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionDuration {
    /// One-hour partitions.
    Hourly,
    /// One-day partitions.
    Daily,
}

impl PartitionDuration {
    /// Returns the duration in nanoseconds.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::PartitionDuration;
    ///
    /// let nanos = PartitionDuration::Hourly.as_nanos();
    /// ```
    pub fn as_nanos(self) -> i64 {
        match self {
            Self::Hourly => Duration::from_secs(3600).as_nanos() as i64,
            Self::Daily => Duration::from_secs(86_400).as_nanos() as i64,
        }
    }

    /// Returns the duration as `Duration`.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::PartitionDuration;
    ///
    /// let duration = PartitionDuration::Daily.as_duration();
    /// ```
    pub fn as_duration(self) -> Duration {
        match self {
            Self::Hourly => Duration::from_secs(3600),
            Self::Daily => Duration::from_secs(86_400),
        }
    }
}

/// Information parsed from a TSM file name.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::partition::TsmFileInfo;
///
/// let name = TsmFileInfo::file_name(1, 0, 1);
/// let parsed = TsmFileInfo::parse_file_name(&name);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsmFileInfo {
    /// Series ID (u64).
    pub series_id: SeriesId,
    /// Compaction level.
    pub level: u8,
    /// Generation number.
    pub generation: u32,
    /// Full file path.
    pub path: PathBuf,
}

impl TsmFileInfo {
    /// Builds a file name using `{series_id_hex}_L{level}_G{generation}.skulk`.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::TsmFileInfo;
    ///
    /// let name = TsmFileInfo::file_name(1, 0, 1);
    /// ```
    pub fn file_name(series_id: SeriesId, level: u8, generation: u32) -> String {
        format!("{:016x}_L{}_G{:03}.skulk", series_id, level, generation)
    }

    /// Parses a file name into components.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::TsmFileInfo;
    ///
    /// let name = TsmFileInfo::file_name(1, 0, 1);
    /// let parsed = TsmFileInfo::parse_file_name(&name);
    /// ```
    pub fn parse_file_name(name: &str) -> Option<(SeriesId, u8, u32)> {
        let name = name.strip_suffix(".skulk")?;
        let mut parts = name.split('_');
        let series_hex = parts.next()?;
        let level_part = parts.next()?;
        let gen_part = parts.next()?;
        if parts.next().is_some() {
            return None;
        }

        if series_hex.len() != 16 {
            return None;
        }
        let series_id = u64::from_str_radix(series_hex, 16).ok()?;
        let level = level_part.strip_prefix('L')?.parse::<u8>().ok()?;
        let generation = gen_part.strip_prefix('G')?.parse::<u32>().ok()?;
        Some((series_id, level, generation))
    }
}

/// Provides filesystem paths for time partitions and TSM file names.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
///
/// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
/// let root = layout.data_dir();
/// ```
#[derive(Debug, Clone)]
pub struct PartitionLayout {
    /// Root data directory.
    data_dir: PathBuf,
    /// Partition duration.
    duration: PartitionDuration,
}

impl PartitionLayout {
    /// Creates a new partition layout for the given directory and duration.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Daily);
    /// ```
    pub fn new(data_dir: impl AsRef<Path>, duration: PartitionDuration) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            duration,
        }
    }

    /// Returns the root data directory.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let _root = layout.data_dir();
    /// ```
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the partition duration.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let _duration = layout.duration();
    /// ```
    pub fn duration(&self) -> PartitionDuration {
        self.duration
    }

    /// Returns the directory for the given partition.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    /// use alopex_skulk::tsm::TimePartition;
    /// use std::time::Duration;
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let partition = TimePartition::new(0, Duration::from_secs(3600));
    /// let _dir = layout.partition_dir(&partition);
    /// ```
    pub fn partition_dir(&self, partition: &TimePartition) -> PathBuf {
        self.partition_dir_for(partition.start_ts)
    }

    /// Returns the directory path for a timestamp.
    ///
    /// Note: v0.2 assumes timestamps are at or after Unix epoch. If negative
    /// timestamps are needed in the future, update the floor-division logic
    /// and directory naming to handle pre-epoch dates consistently.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let _dir = layout.partition_dir_for(0);
    /// ```
    pub fn partition_dir_for(&self, timestamp: Timestamp) -> PathBuf {
        let (year, month, day, hour) = timestamp_to_ymdh(timestamp);
        match self.duration {
            PartitionDuration::Daily => self
                .data_dir
                .join(format!("{:04}-{:02}-{:02}", year, month, day)),
            PartitionDuration::Hourly => self
                .data_dir
                .join(format!("{:04}-{:02}-{:02}", year, month, day))
                .join(format!("{:02}", hour)),
        }
    }

    /// Builds the TSM file path for the given series and partition.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    /// use alopex_skulk::tsm::TimePartition;
    /// use std::time::Duration;
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let partition = TimePartition::new(0, Duration::from_secs(3600));
    /// let _path = layout.tsm_file_path(&partition, 1, 0, 1);
    /// ```
    pub fn tsm_file_path(
        &self,
        partition: &TimePartition,
        series_id: SeriesId,
        level: u8,
        generation: u32,
    ) -> PathBuf {
        let file_name = TsmFileInfo::file_name(series_id, level, generation);
        self.partition_dir(partition).join(file_name)
    }

    /// Lists partitions that overlap with a time range.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let _partitions = layout.list_partitions_in_range(0, 3_600_000_000_000);
    /// ```
    pub fn list_partitions_in_range(
        &self,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Vec<TimePartition> {
        if end_ts <= start_ts {
            return Vec::new();
        }

        let duration_nanos = self.duration.as_nanos();
        let mut current = align_timestamp(start_ts, duration_nanos);
        let last = align_timestamp(end_ts - 1, duration_nanos);
        let mut partitions = Vec::new();

        while current <= last {
            partitions.push(TimePartition::new(current, self.duration.as_duration()));
            current += duration_nanos;
        }

        partitions
    }

    /// Lists TSM files in the given partition directory.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    /// use alopex_skulk::tsm::TimePartition;
    /// use std::time::Duration;
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let partition = TimePartition::new(0, Duration::from_secs(3600));
    /// let _files = layout.list_tsm_files(&partition);
    /// ```
    pub fn list_tsm_files(&self, partition: &TimePartition) -> Result<Vec<TsmFileInfo>> {
        let dir = self.partition_dir(partition);
        let mut files = Vec::new();

        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(files),
            Err(err) => return Err(err.into()),
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let file_name = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name,
                None => continue,
            };
            if let Some((series_id, level, generation)) = TsmFileInfo::parse_file_name(file_name) {
                files.push(TsmFileInfo {
                    series_id,
                    level,
                    generation,
                    path,
                });
            }
        }

        files.sort_by_key(|info| (info.series_id, info.level, info.generation));
        Ok(files)
    }
}

fn align_timestamp(timestamp: Timestamp, duration_nanos: i64) -> i64 {
    let (quotient, _) = div_floor(timestamp, duration_nanos);
    quotient * duration_nanos
}

fn div_floor(value: i64, divisor: i64) -> (i64, i64) {
    let mut quotient = value / divisor;
    let mut remainder = value % divisor;
    if remainder < 0 {
        quotient -= 1;
        remainder += divisor;
    }
    (quotient, remainder)
}

fn timestamp_to_ymdh(timestamp: Timestamp) -> (i32, u32, u32, u32) {
    let seconds = timestamp / 1_000_000_000;
    let (days, seconds_of_day) = div_floor(seconds, 86_400);
    let hour = (seconds_of_day / 3600) as u32;
    let (year, month, day) = civil_from_days(days);
    (year, month, day, hour)
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_partition_dir_daily() {
        let layout = PartitionLayout::new("/data", PartitionDuration::Daily);
        let partition = TimePartition::new(0, Duration::from_secs(86_400));
        assert_eq!(
            layout.partition_dir(&partition),
            PathBuf::from("/data/1970-01-01")
        );
    }

    #[test]
    fn test_partition_dir_hourly() {
        let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
        let ts = 27 * 3600_i64 * 1_000_000_000;
        let partition = TimePartition::new(ts, Duration::from_secs(3600));
        assert_eq!(
            layout.partition_dir(&partition),
            PathBuf::from("/data/1970-01-02/03")
        );
    }

    #[test]
    fn test_tsm_file_name_roundtrip() {
        let name = TsmFileInfo::file_name(0x3039, 2, 7);
        let parsed = TsmFileInfo::parse_file_name(&name).unwrap();
        assert_eq!(parsed, (0x3039, 2, 7));
    }

    #[test]
    fn test_list_partitions_in_range_hourly() {
        let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
        let hour = 3600_i64 * 1_000_000_000;
        let partitions = layout.list_partitions_in_range(0, hour * 3);
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].start_ts, 0);
        assert_eq!(partitions[1].start_ts, hour);
        assert_eq!(partitions[2].start_ts, hour * 2);
    }

    #[test]
    fn test_list_partitions_in_range_daily() {
        let layout = PartitionLayout::new("/data", PartitionDuration::Daily);
        let day = 86_400_i64 * 1_000_000_000;
        let partitions = layout.list_partitions_in_range(0, day * 2);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].start_ts, 0);
        assert_eq!(partitions[1].start_ts, day);
    }

    #[test]
    fn test_list_tsm_files() {
        let temp_dir = TempDir::new().unwrap();
        let layout = PartitionLayout::new(temp_dir.path(), PartitionDuration::Hourly);
        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let dir = layout.partition_dir(&partition);
        fs::create_dir_all(&dir).unwrap();

        let file_path = dir.join(TsmFileInfo::file_name(1, 0, 1));
        fs::write(&file_path, b"").unwrap();
        let invalid_path = dir.join("bad_name.txt");
        fs::write(&invalid_path, b"").unwrap();

        let files = layout.list_tsm_files(&partition).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].series_id, 1);
        assert_eq!(files[0].level, 0);
        assert_eq!(files[0].generation, 1);
    }

    #[test]
    fn test_partition_dir_for_uses_timestamp() {
        let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
        let ts = 2 * 3600_i64 * 1_000_000_000;
        assert_eq!(
            layout.partition_dir_for(ts),
            PathBuf::from("/data/1970-01-01/02")
        );
    }
}
