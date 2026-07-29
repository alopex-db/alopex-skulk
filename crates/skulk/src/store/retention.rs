//! Durable measurement retention policies and time partitions.

use crate::error::{Result, TsmError};
use crate::model::Timestamp;
use crate::store::manifest::{ManifestState, ManifestStore, ManifestUpdate};
use crc32fast::Hasher as Crc32;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Fixed v0.3 time-partition width.
pub const HOUR_NANOS: i64 = 3_600_000_000_000;

const POLICY_FILE: &str = "retention-v3.bin";
const POLICY_TEMP_PREFIX: &str = ".retention-v3.tmp-";
const POLICY_MAGIC: [u8; 4] = *b"SKR3";
const POLICY_VERSION: u16 = 1;
const HEADER_BYTES: usize = 10;
const MAX_POLICY_BYTES: usize = 4 * 1024 * 1024;
const MAX_POLICIES: usize = 10_000;
const MAX_MEASUREMENT_BYTES: usize = 1024;
static POLICY_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Explicit handling for writes older than a measurement's retention cutoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateWritePolicy {
    /// Accept the write; a later TTL pass may remove its partition.
    Accept,
    /// Reject the write before sequence allocation or WAL persistence.
    Reject,
}

/// Durable measurement-scoped TTL and late-write behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    retention_nanos: u64,
    late_writes: LateWritePolicy,
}

impl RetentionPolicy {
    /// Creates a bounded nanosecond TTL policy.
    pub fn new(retention_nanos: u64, late_writes: LateWritePolicy) -> Result<Self> {
        if retention_nanos > i64::MAX as u64 {
            return Err(TsmError::InvalidInput(
                "retention duration exceeds timestamp range".into(),
            ));
        }
        Ok(Self {
            retention_nanos,
            late_writes,
        })
    }

    /// Returns the configured TTL in nanoseconds.
    pub const fn retention_nanos(self) -> u64 {
        self.retention_nanos
    }

    /// Returns the explicit late-write decision.
    pub const fn late_write_policy(self) -> LateWritePolicy {
        self.late_writes
    }

    /// Computes the oldest accepted timestamp boundary at `now`.
    pub fn cutoff(self, now: Timestamp) -> Timestamp {
        now.saturating_sub(self.retention_nanos as i64)
    }
}

/// One half-open, floor-aligned hourly interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimePartition {
    start: Timestamp,
    end_exclusive: Timestamp,
}

impl TimePartition {
    /// Resolves the v0.3 hourly partition containing a timestamp.
    pub fn for_timestamp(timestamp: Timestamp) -> Result<Self> {
        let start = timestamp
            .div_euclid(HOUR_NANOS)
            .checked_mul(HOUR_NANOS)
            .ok_or_else(|| {
                TsmError::InvalidInput(
                    "timestamp cannot be represented in an hourly partition".into(),
                )
            })?;
        let end_exclusive = start.checked_add(HOUR_NANOS).ok_or_else(|| {
            TsmError::InvalidInput("timestamp cannot be represented in an hourly partition".into())
        })?;
        Ok(Self {
            start,
            end_exclusive,
        })
    }

    /// Returns the inclusive partition start.
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the exclusive partition end.
    pub const fn end_exclusive(self) -> Timestamp {
        self.end_exclusive
    }
}

/// Result of one idempotent TTL pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionResult {
    expired_partition_count: usize,
    removed_file_count: usize,
}

impl RetentionResult {
    /// Returns how many distinct partitions became inactive.
    pub const fn expired_partition_count(self) -> usize {
        self.expired_partition_count
    }

    /// Returns how many active Parquet files were removed.
    pub const fn removed_file_count(self) -> usize {
        self.removed_file_count
    }
}

/// Atomic policy registry and manifest-backed partition lifecycle.
pub struct RetentionStore {
    root: PathBuf,
    policies: Mutex<BTreeMap<String, RetentionPolicy>>,
}

impl RetentionStore {
    /// Opens durable policies and removes only scoped interrupted policy writes.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root)?;
        cleanup_policy_temps(&root)?;
        let path = root.join(POLICY_FILE);
        let policies = if path.exists() {
            load_policies(&path)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            root,
            policies: Mutex::new(policies),
        })
    }

    /// Atomically creates or replaces one measurement policy.
    pub fn set_policy(&self, measurement: &str, policy: RetentionPolicy) -> Result<()> {
        validate_measurement(measurement)?;
        let mut policies = self.lock_policies()?;
        let mut next = policies.clone();
        next.insert(measurement.to_owned(), policy);
        persist_policies(&self.root, &next)?;
        *policies = next;
        Ok(())
    }

    /// Returns one recovered policy.
    pub fn policy(&self, measurement: &str) -> Result<Option<RetentionPolicy>> {
        validate_measurement(measurement)?;
        Ok(self.lock_policies()?.get(measurement).copied())
    }

    /// Applies the explicit cutoff policy before sequence allocation and WAL append.
    /// Returns the reject cutoff for one measurement when its policy rejects
    /// late writes, so batch loops can hoist the policy lookup.
    pub fn reject_cutoff(&self, measurement: &str, now: Timestamp) -> Result<Option<Timestamp>> {
        Ok(self.policy(measurement)?.and_then(|policy| {
            (policy.late_write_policy() == LateWritePolicy::Reject).then(|| policy.cutoff(now))
        }))
    }

    /// Validates one write timestamp against the measurement's retention policy.
    pub fn validate_write(
        &self,
        measurement: &str,
        timestamp: Timestamp,
        now: Timestamp,
    ) -> Result<()> {
        let Some(policy) = self.policy(measurement)? else {
            return Ok(());
        };
        if timestamp < policy.cutoff(now) && policy.late_write_policy() == LateWritePolicy::Reject {
            return Err(TsmError::InvalidInput(format!(
                "timestamp {timestamp} is older than retention cutoff {} for '{measurement}'",
                policy.cutoff(now)
            )));
        }
        Ok(())
    }

    /// Lists distinct manifest-active hourly partitions for a measurement.
    pub fn list_partitions(
        &self,
        state: &ManifestState,
        measurement: &str,
    ) -> Result<Vec<TimePartition>> {
        validate_measurement(measurement)?;
        let mut partitions = BTreeSet::new();
        for file in state
            .active_files()
            .values()
            .filter(|file| file.measurement() == measurement)
        {
            let partition = TimePartition::for_timestamp(file.min_timestamp())?;
            if file.max_timestamp() >= partition.end_exclusive() {
                return Err(TsmError::Corruption(format!(
                    "active Parquet '{}' spans multiple v0.3 partitions",
                    file.name()
                )));
            }
            partitions.insert(partition);
        }
        Ok(partitions.into_iter().collect())
    }

    /// Atomically removes fully expired partitions, then unlinks exact inactive files.
    pub fn expire(
        &self,
        manifest: &ManifestStore,
        measurement: &str,
        now: Timestamp,
    ) -> Result<RetentionResult> {
        let Some(policy) = self.policy(measurement)? else {
            return Ok(RetentionResult::default());
        };
        let state = manifest.state()?;
        let cutoff = policy.cutoff(now);
        let expired = self
            .list_partitions(&state, measurement)?
            .into_iter()
            .filter(|partition| partition.end_exclusive() <= cutoff)
            .collect::<BTreeSet<_>>();
        if expired.is_empty() {
            return Ok(RetentionResult::default());
        }

        let mut files = Vec::new();
        for file in state
            .active_files()
            .values()
            .filter(|file| file.measurement() == measurement)
        {
            if expired.contains(&TimePartition::for_timestamp(file.min_timestamp())?) {
                files.push(file.name().to_owned());
            }
        }
        if files.is_empty() {
            return Ok(RetentionResult::default());
        }
        let mut update = ManifestUpdate::new();
        for name in &files {
            update = update.remove_file(name);
        }
        manifest.publish(update)?;
        manifest.remove_inactive_files(&files)?;
        Ok(RetentionResult {
            expired_partition_count: expired.len(),
            removed_file_count: files.len(),
        })
    }

    fn lock_policies(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, RetentionPolicy>>> {
        self.policies
            .lock()
            .map_err(|_| TsmError::Corruption("retention policy lock is poisoned".into()))
    }
}

pub(crate) fn current_timestamp_nanos() -> Result<Timestamp> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| TsmError::InvalidInput(format!("system clock precedes epoch: {error}")))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_| TsmError::ResourceLimit("system timestamp exceeds i64 nanoseconds".into()))
}

fn validate_measurement(measurement: &str) -> Result<()> {
    if measurement.is_empty() || measurement.len() > MAX_MEASUREMENT_BYTES {
        return Err(TsmError::InvalidInput(
            "retention measurement must be non-empty and bounded".into(),
        ));
    }
    Ok(())
}

fn persist_policies(root: &Path, policies: &BTreeMap<String, RetentionPolicy>) -> Result<()> {
    let bytes = encode_policies(policies)?;
    let temporary = root.join(format!(
        "{POLICY_TEMP_PREFIX}{}-{}",
        std::process::id(),
        POLICY_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut cleanup = PolicyTemp::new(temporary.clone());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, root.join(POLICY_FILE))?;
    cleanup.disarm();
    sync_directory(root)?;
    Ok(())
}

fn encode_policies(policies: &BTreeMap<String, RetentionPolicy>) -> Result<Vec<u8>> {
    if policies.len() > MAX_POLICIES {
        return Err(TsmError::ResourceLimit(
            "retention policy count exceeds limit".into(),
        ));
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(policies.len() as u32).to_le_bytes());
    for (measurement, policy) in policies {
        validate_measurement(measurement)?;
        let length = u16::try_from(measurement.len())
            .map_err(|_| TsmError::ResourceLimit("measurement name exceeds u16".into()))?;
        payload.extend_from_slice(&length.to_le_bytes());
        payload.extend_from_slice(measurement.as_bytes());
        payload.extend_from_slice(&policy.retention_nanos().to_le_bytes());
        payload.push(match policy.late_write_policy() {
            LateWritePolicy::Accept => 0,
            LateWritePolicy::Reject => 1,
        });
    }
    if payload.len() > MAX_POLICY_BYTES.saturating_sub(HEADER_BYTES + 4) {
        return Err(TsmError::ResourceLimit(
            "retention policy file exceeds limit".into(),
        ));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| TsmError::ResourceLimit("retention payload exceeds u32".into()))?;
    let mut crc = Crc32::new();
    crc.update(&payload);
    let mut output = Vec::with_capacity(HEADER_BYTES + payload.len() + 4);
    output.extend_from_slice(&POLICY_MAGIC);
    output.extend_from_slice(&POLICY_VERSION.to_le_bytes());
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&payload);
    output.extend_from_slice(&crc.finalize().to_le_bytes());
    Ok(output)
}

fn load_policies(path: &Path) -> Result<BTreeMap<String, RetentionPolicy>> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_POLICY_BYTES as u64 {
        return Err(TsmError::ResourceLimit(
            "retention policy file exceeds limit".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_policies(&bytes)
}

fn decode_policies(bytes: &[u8]) -> Result<BTreeMap<String, RetentionPolicy>> {
    if bytes.len() < HEADER_BYTES + 4 || bytes.len() > MAX_POLICY_BYTES {
        return Err(TsmError::InvalidFormat(
            "truncated or oversized retention policy file".into(),
        ));
    }
    if bytes[..4] != POLICY_MAGIC {
        return Err(TsmError::InvalidFormat(
            "invalid retention policy magic".into(),
        ));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != POLICY_VERSION {
        return Err(TsmError::InvalidFormat(format!(
            "unsupported retention policy version {version}"
        )));
    }
    let payload_len = u32::from_le_bytes(
        bytes[6..10]
            .try_into()
            .map_err(|_| TsmError::InvalidFormat("invalid retention payload length".into()))?,
    ) as usize;
    let expected = HEADER_BYTES
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| TsmError::InvalidFormat("retention length overflow".into()))?;
    if bytes.len() != expected {
        return Err(TsmError::InvalidFormat(
            "retention payload length mismatch".into(),
        ));
    }
    let payload = &bytes[HEADER_BYTES..HEADER_BYTES + payload_len];
    let expected_crc = u32::from_le_bytes(
        bytes[HEADER_BYTES + payload_len..]
            .try_into()
            .map_err(|_| TsmError::InvalidFormat("invalid retention checksum".into()))?,
    );
    let mut crc = Crc32::new();
    crc.update(payload);
    if crc.finalize() != expected_crc {
        return Err(TsmError::Corruption(
            "retention policy checksum mismatch".into(),
        ));
    }

    let mut cursor = PolicyCursor::new(payload);
    let count = cursor.u32()? as usize;
    if count > MAX_POLICIES {
        return Err(TsmError::ResourceLimit(
            "retention policy count exceeds limit".into(),
        ));
    }
    let mut policies = BTreeMap::new();
    for _ in 0..count {
        let measurement = cursor.string()?;
        validate_measurement(&measurement)?;
        let retention_nanos = cursor.u64()?;
        let late_writes = match cursor.u8()? {
            0 => LateWritePolicy::Accept,
            1 => LateWritePolicy::Reject,
            marker => {
                return Err(TsmError::InvalidFormat(format!(
                    "invalid late-write policy marker {marker}"
                )))
            }
        };
        let policy = RetentionPolicy::new(retention_nanos, late_writes)?;
        if policies.insert(measurement, policy).is_some() {
            return Err(TsmError::Corruption(
                "duplicate measurement retention policy".into(),
            ));
        }
    }
    if !cursor.is_empty() {
        return Err(TsmError::InvalidFormat(
            "trailing retention policy bytes".into(),
        ));
    }
    Ok(policies)
}

struct PolicyCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> PolicyCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| TsmError::InvalidFormat("retention cursor overflow".into()))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| TsmError::InvalidFormat("truncated retention payload".into()))?;
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid retention u32".into()),
        )?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid retention u64".into()),
        )?))
    }

    fn string(&mut self) -> Result<String> {
        let length = u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| TsmError::InvalidFormat("invalid retention string length".into()))?,
        ) as usize;
        if length > MAX_MEASUREMENT_BYTES {
            return Err(TsmError::ResourceLimit(
                "retention measurement exceeds limit".into(),
            ));
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| TsmError::InvalidFormat("retention measurement is not UTF-8".into()))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn cleanup_policy_temps(root: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(POLICY_TEMP_PREFIX)
        {
            let file_type = entry.file_type()?;
            if file_type.is_file() || file_type.is_symlink() {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
    }
    if removed {
        sync_directory(root)?;
    }
    Ok(())
}

struct PolicyTemp {
    path: PathBuf,
    armed: bool,
}

impl PolicyTemp {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PolicyTemp {
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

#[cfg(test)]
mod tests {
    use super::{
        decode_policies, encode_policies, LateWritePolicy, RetentionPolicy, TimePartition,
        HOUR_NANOS,
    };
    use std::collections::BTreeMap;

    #[test]
    fn negative_timestamps_use_floor_aligned_partitions() {
        let partition = TimePartition::for_timestamp(-1).expect("partition");
        assert_eq!(partition.start(), -HOUR_NANOS);
        assert_eq!(partition.end_exclusive(), 0);
    }

    #[test]
    fn timestamp_extremes_return_errors_without_arithmetic_panics() {
        assert!(TimePartition::for_timestamp(i64::MIN).is_err());
        assert!(TimePartition::for_timestamp(i64::MAX).is_err());
    }

    #[test]
    fn policy_encoding_round_trips_and_detects_corruption() {
        let policies = BTreeMap::from([(
            "cpu".to_owned(),
            RetentionPolicy::new(123, LateWritePolicy::Reject).expect("policy"),
        )]);
        let encoded = encode_policies(&policies).expect("encode");
        assert_eq!(decode_policies(&encoded).expect("decode"), policies);
        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        assert!(decode_policies(&corrupted).is_err());
    }
}
