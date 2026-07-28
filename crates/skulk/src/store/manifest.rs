//! Atomic active-file manifest and recovery high-water state.

use crate::error::{Result, TsmError};
use crate::store::seq::{IngestSeq, Sequencer};
use crc32fast::Hasher as Crc32;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Current atomically published manifest file.
pub const CURRENT_MANIFEST_FILE: &str = "manifest-v3.current";
/// Previous valid generation retained for fallback.
pub const PREVIOUS_MANIFEST_FILE: &str = "manifest-v3.previous";
/// Directory whose Parquet and temporary files are owned by the manifest.
pub const SEGMENTS_DIRECTORY: &str = "segments";

const MANIFEST_MAGIC: [u8; 4] = *b"SKM3";
const MANIFEST_VERSION: u16 = 2;
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACTIVE_FILES: usize = 100_000;
const MAX_FILE_NAME_BYTES: usize = 1024;
const HEADER_BYTES: usize = 10;
static MANIFEST_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// One immutable Parquet file referenced by a manifest generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveFile {
    measurement: String,
    name: String,
    row_count: u64,
    file_bytes: u64,
    min_timestamp: i64,
    max_timestamp: i64,
}

impl ActiveFile {
    /// Creates a validated file entry relative to the dedicated segments directory.
    pub fn new(
        measurement: impl Into<String>,
        name: impl Into<String>,
        row_count: u64,
        file_bytes: u64,
        min_timestamp: i64,
        max_timestamp: i64,
    ) -> Result<Self> {
        let measurement = measurement.into();
        let name = name.into();
        if measurement.is_empty() || measurement.len() > MAX_FILE_NAME_BYTES {
            return Err(TsmError::InvalidInput(
                "active Parquet measurement must be non-empty and bounded".into(),
            ));
        }
        validate_file_name(&name)?;
        if row_count == 0 || file_bytes == 0 {
            return Err(TsmError::InvalidInput(
                "active Parquet row count and file size must be non-zero".into(),
            ));
        }
        if min_timestamp > max_timestamp {
            return Err(TsmError::InvalidInput(
                "active Parquet timestamp range is inverted".into(),
            ));
        }
        Ok(Self {
            measurement,
            name,
            row_count,
            file_bytes,
            min_timestamp,
            max_timestamp,
        })
    }

    /// Returns the measurement/table stored in this file.
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the relative file name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the stored row count.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the expected file size.
    pub const fn file_bytes(&self) -> u64 {
        self.file_bytes
    }

    /// Returns the smallest timestamp stored in this file.
    pub const fn min_timestamp(&self) -> i64 {
        self.min_timestamp
    }

    /// Returns the largest timestamp stored in this file.
    pub const fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }
}

/// Durable active set and monotonic recovery boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    generation: u64,
    active_files: BTreeMap<String, ActiveFile>,
    persisted_through: Option<IngestSeq>,
    issued_through: Option<IngestSeq>,
}

impl ManifestState {
    fn fresh() -> Self {
        Self {
            generation: 0,
            active_files: BTreeMap::new(),
            persisted_through: None,
            issued_through: None,
        }
    }

    /// Returns the durable generation number.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the immutable active-file map keyed by relative file name.
    pub fn active_files(&self) -> &BTreeMap<String, ActiveFile> {
        &self.active_files
    }

    /// Returns one active-file entry.
    pub fn active_file(&self, name: &str) -> Option<&ActiveFile> {
        self.active_files.get(name)
    }

    /// Returns the largest sequence already represented by active Parquet.
    pub const fn persisted_through(&self) -> Option<IngestSeq> {
        self.persisted_through
    }

    /// Returns the largest sequence known to have been issued.
    pub const fn issued_through(&self) -> Option<IngestSeq> {
        self.issued_through
    }

    /// Returns whether a WAL row is strictly beyond the Parquet recovery fence.
    pub fn should_replay(&self, sequence: IngestSeq) -> bool {
        self.persisted_through
            .is_none_or(|persisted| sequence > persisted)
    }
}

/// Delta applied under the manifest's serialization lock.
#[derive(Debug, Clone, Default)]
pub struct ManifestUpdate {
    additions: BTreeMap<String, ActiveFile>,
    removals: BTreeSet<String>,
    persisted_through: Option<IngestSeq>,
    issued_through: Option<IngestSeq>,
}

impl ManifestUpdate {
    /// Creates an empty idempotent delta.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or idempotently confirms one active file.
    pub fn add_file(mut self, file: ActiveFile) -> Self {
        self.additions.insert(file.name.clone(), file);
        self
    }

    /// Removes one file name from the next active set.
    pub fn remove_file(mut self, name: impl Into<String>) -> Self {
        self.removals.insert(name.into());
        self
    }

    /// Requests monotonic advancement of the Parquet recovery fence.
    pub fn advance_persisted_through(mut self, sequence: IngestSeq) -> Self {
        self.persisted_through = max_sequence(self.persisted_through, Some(sequence));
        self
    }

    /// Records a durable observation of the sequence allocator high-water mark.
    pub fn observe_issued_through(mut self, sequence: IngestSeq) -> Self {
        self.issued_through = max_sequence(self.issued_through, Some(sequence));
        self
    }
}

/// Source selected when opening manifest state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveredFrom {
    /// No manifest or owned segment data existed.
    Fresh,
    /// The current generation passed all integrity checks.
    Current,
    /// The current generation was absent/torn and the previous one was valid.
    Previous,
}

struct ManifestInner {
    state: ManifestState,
    current_valid: bool,
}

/// Serialized manifest publisher and startup orphan reconciler.
pub struct ManifestStore {
    root: PathBuf,
    segments: PathBuf,
    recovered_from: RecoveredFrom,
    inner: Mutex<ManifestInner>,
}

impl ManifestStore {
    /// Opens the current manifest, falling back one generation before cleanup.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root)?;
        let segments = root.join(SEGMENTS_DIRECTORY);
        fs::create_dir_all(&segments)?;
        let current_path = root.join(CURRENT_MANIFEST_FILE);
        let previous_path = root.join(PREVIOUS_MANIFEST_FILE);
        let current = load_optional(&current_path)
            .map(|result| result.and_then(|state| validate_loaded_state(&segments, state)));
        let previous = load_optional(&previous_path)
            .map(|result| result.and_then(|state| validate_loaded_state(&segments, state)));

        let (state, recovered_from, current_valid) = match (current, previous) {
            (Some(Ok(state)), _) => (state, RecoveredFrom::Current, true),
            (Some(Err(_)), Some(Ok(state))) | (None, Some(Ok(state))) => {
                (state, RecoveredFrom::Previous, false)
            }
            (None, None) => (ManifestState::fresh(), RecoveredFrom::Fresh, false),
            (Some(Err(error)), None) | (Some(Err(error)), Some(Err(_))) => {
                return Err(error);
            }
            (None, Some(Err(error))) => return Err(error),
        };
        cleanup_orphans(&segments, &state.active_files)?;
        cleanup_manifest_temps(&root)?;

        Ok(Self {
            root,
            segments,
            recovered_from,
            inner: Mutex::new(ManifestInner {
                state,
                current_valid,
            }),
        })
    }

    /// Returns the dedicated directory for manifest-owned Parquet files.
    pub fn segments_dir(&self) -> &Path {
        &self.segments
    }

    /// Returns which durable generation was selected at open.
    pub const fn recovered_from(&self) -> RecoveredFrom {
        self.recovered_from
    }

    /// Returns a consistent snapshot of the in-memory durable state.
    pub fn state(&self) -> Result<ManifestState> {
        Ok(self.lock_inner()?.state.clone())
    }

    /// Applies a delta to the latest generation and persists it atomically.
    pub fn publish(&self, update: ManifestUpdate) -> Result<ManifestState> {
        let mut inner = self.lock_inner()?;
        validate_update_files(&self.segments, &update.additions)?;
        for (name, addition) in &update.additions {
            if inner
                .state
                .active_files
                .get(name)
                .is_some_and(|existing| existing != addition)
            {
                return Err(TsmError::InvalidInput(format!(
                    "active Parquet file '{name}' is immutable"
                )));
            }
        }
        let mut next = inner.state.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| TsmError::ResourceLimit("manifest generation exhausted".into()))?;
        for name in update.removals {
            validate_file_name(&name)?;
            next.active_files.remove(&name);
        }
        for (name, file) in update.additions {
            next.active_files.insert(name, file);
        }
        if next.active_files.len() > MAX_ACTIVE_FILES {
            return Err(TsmError::ResourceLimit(
                "manifest active-file limit exceeded".into(),
            ));
        }
        next.persisted_through = max_sequence(next.persisted_through, update.persisted_through);
        next.issued_through = max_sequence(
            max_sequence(next.issued_through, update.issued_through),
            next.persisted_through,
        );

        if let Err(error) = persist_state(&self.root, &next, inner.current_valid) {
            inner.current_valid = load_optional(&self.root.join(CURRENT_MANIFEST_FILE))
                .is_some_and(|result| result.is_ok());
            return Err(error);
        }
        inner.state = next.clone();
        inner.current_valid = true;
        Ok(next)
    }

    /// Resumes strictly after the maximum high-water mark in manifest and WAL.
    pub fn resume_sequencer(&self, wal_high_water: Option<IngestSeq>) -> Result<Sequencer> {
        let manifest_high_water = self.lock_inner()?.state.issued_through;
        Sequencer::resume_after(max_sequence(manifest_high_water, wal_high_water))
    }

    pub(crate) fn remove_inactive_files(&self, names: &[String]) -> Result<()> {
        let inner = self.lock_inner()?;
        for name in names {
            validate_file_name(name)?;
            if inner.state.active_files.contains_key(name) {
                return Err(TsmError::InvalidInput(format!(
                    "refusing to remove active Parquet file '{name}'"
                )));
            }
        }
        let mut removed = false;
        for name in names {
            match fs::remove_file(self.segments.join(name)) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if removed {
            sync_directory(&self.segments)?;
        }
        drop(inner);
        Ok(())
    }

    fn lock_inner(&self) -> Result<std::sync::MutexGuard<'_, ManifestInner>> {
        self.inner
            .lock()
            .map_err(|_| TsmError::Corruption("manifest state lock is poisoned".into()))
    }
}

fn validate_update_files(segments: &Path, additions: &BTreeMap<String, ActiveFile>) -> Result<()> {
    for file in additions.values() {
        let metadata = fs::metadata(segments.join(&file.name))?;
        if !metadata.is_file() || metadata.len() != file.file_bytes {
            return Err(TsmError::InvalidInput(format!(
                "active file '{}' is missing, non-regular, or changed size",
                file.name
            )));
        }
    }
    Ok(())
}

fn persist_state(root: &Path, state: &ManifestState, rotate_current: bool) -> Result<()> {
    let bytes = encode_state(state)?;
    let sequence = MANIFEST_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary_path = root.join(format!(
        ".manifest-v3.tmp-{}-{sequence}",
        std::process::id()
    ));
    let mut cleanup = TemporaryManifest::new(temporary_path.clone());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    let current = root.join(CURRENT_MANIFEST_FILE);
    let previous = root.join(PREVIOUS_MANIFEST_FILE);
    if rotate_current {
        if previous.exists() {
            fs::remove_file(&previous)?;
        }
        fs::rename(&current, &previous)?;
    } else if current.exists() {
        fs::remove_file(&current)?;
    }
    fs::rename(&temporary_path, &current)?;
    cleanup.disarm();
    sync_directory(root)?;
    Ok(())
}

fn load_optional(path: &Path) -> Option<Result<ManifestState>> {
    path.exists().then(|| load_state(path))
}

fn load_state(path: &Path) -> Result<ManifestState> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_MANIFEST_BYTES as u64 {
        return Err(TsmError::ResourceLimit(
            "manifest exceeds maximum byte size".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_state(&bytes)
}

fn encode_state(state: &ManifestState) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&state.generation.to_le_bytes());
    encode_sequence(&mut payload, state.persisted_through);
    encode_sequence(&mut payload, state.issued_through);
    let file_count = u32::try_from(state.active_files.len())
        .map_err(|_| TsmError::ResourceLimit("too many active files".into()))?;
    payload.extend_from_slice(&file_count.to_le_bytes());
    for file in state.active_files.values() {
        encode_string(&mut payload, &file.measurement)?;
        encode_string(&mut payload, &file.name)?;
        payload.extend_from_slice(&file.row_count.to_le_bytes());
        payload.extend_from_slice(&file.file_bytes.to_le_bytes());
        payload.extend_from_slice(&file.min_timestamp.to_le_bytes());
        payload.extend_from_slice(&file.max_timestamp.to_le_bytes());
    }
    if payload.len() > MAX_MANIFEST_BYTES.saturating_sub(HEADER_BYTES + 4) {
        return Err(TsmError::ResourceLimit(
            "encoded manifest exceeds maximum byte size".into(),
        ));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| TsmError::ResourceLimit("manifest payload exceeds u32".into()))?;
    let mut crc = Crc32::new();
    crc.update(&payload);
    let mut output = Vec::with_capacity(HEADER_BYTES + payload.len() + 4);
    output.extend_from_slice(&MANIFEST_MAGIC);
    output.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&payload);
    output.extend_from_slice(&crc.finalize().to_le_bytes());
    Ok(output)
}

fn decode_state(bytes: &[u8]) -> Result<ManifestState> {
    if bytes.len() < HEADER_BYTES + 4 || bytes.len() > MAX_MANIFEST_BYTES {
        return Err(TsmError::InvalidFormat(
            "truncated or oversized manifest".into(),
        ));
    }
    if bytes[..4] != MANIFEST_MAGIC {
        return Err(TsmError::InvalidFormat("invalid manifest magic".into()));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != MANIFEST_VERSION {
        return Err(TsmError::InvalidFormat(format!(
            "unsupported manifest version {version}"
        )));
    }
    let payload_len = u32::from_le_bytes(
        bytes[6..10]
            .try_into()
            .map_err(|_| TsmError::InvalidFormat("invalid manifest payload length".into()))?,
    ) as usize;
    let expected_len = HEADER_BYTES
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| TsmError::InvalidFormat("manifest length overflow".into()))?;
    if bytes.len() != expected_len {
        return Err(TsmError::InvalidFormat(
            "manifest length does not match payload".into(),
        ));
    }
    let payload = &bytes[HEADER_BYTES..HEADER_BYTES + payload_len];
    let expected_crc = u32::from_le_bytes(
        bytes[HEADER_BYTES + payload_len..]
            .try_into()
            .map_err(|_| TsmError::InvalidFormat("invalid manifest checksum".into()))?,
    );
    let mut crc = Crc32::new();
    crc.update(payload);
    if crc.finalize() != expected_crc {
        return Err(TsmError::Corruption("manifest checksum mismatch".into()));
    }

    let mut cursor = Cursor::new(payload);
    let generation = cursor.u64()?;
    let persisted_through = cursor.sequence()?;
    let issued_through = cursor.sequence()?;
    let file_count = cursor.u32()? as usize;
    if file_count > MAX_ACTIVE_FILES {
        return Err(TsmError::ResourceLimit(
            "manifest active-file limit exceeded".into(),
        ));
    }
    let mut active_files = BTreeMap::new();
    for _ in 0..file_count {
        let measurement = cursor.string()?;
        let name = cursor.string()?;
        let file = ActiveFile::new(
            measurement,
            name.clone(),
            cursor.u64()?,
            cursor.u64()?,
            cursor.i64()?,
            cursor.i64()?,
        )?;
        if active_files.insert(name, file).is_some() {
            return Err(TsmError::Corruption(
                "duplicate active file in manifest".into(),
            ));
        }
    }
    if !cursor.is_empty() {
        return Err(TsmError::InvalidFormat(
            "trailing bytes in manifest payload".into(),
        ));
    }
    if let (Some(persisted), Some(issued)) = (persisted_through, issued_through) {
        if persisted > issued {
            return Err(TsmError::Corruption(
                "manifest persisted fence exceeds issued high-water".into(),
            ));
        }
    } else if persisted_through.is_some() {
        return Err(TsmError::Corruption(
            "manifest persisted fence lacks an issued high-water".into(),
        ));
    }
    Ok(ManifestState {
        generation,
        active_files,
        persisted_through,
        issued_through,
    })
}

fn encode_sequence(output: &mut Vec<u8>, sequence: Option<IngestSeq>) {
    match sequence {
        Some(sequence) => {
            output.push(1);
            output.extend_from_slice(&sequence.get().to_le_bytes());
        }
        None => output.push(0),
    }
}

fn encode_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.len() > MAX_FILE_NAME_BYTES {
        return Err(TsmError::ResourceLimit(
            "manifest file name exceeds byte limit".into(),
        ));
    }
    let length = u16::try_from(value.len())
        .map_err(|_| TsmError::ResourceLimit("manifest file name exceeds u16".into()))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| TsmError::InvalidFormat("manifest cursor overflow".into()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| TsmError::InvalidFormat("truncated manifest payload".into()))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid manifest u16".into()),
        )?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid manifest u32".into()),
        )?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid manifest u64".into()),
        )?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| TsmError::InvalidFormat("invalid manifest i64".into()),
        )?))
    }

    fn sequence(&mut self) -> Result<Option<IngestSeq>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(IngestSeq::new(self.u64()?))),
            marker => Err(TsmError::InvalidFormat(format!(
                "invalid optional sequence marker {marker}"
            ))),
        }
    }

    fn string(&mut self) -> Result<String> {
        let length = self.u16()? as usize;
        if length > MAX_FILE_NAME_BYTES {
            return Err(TsmError::ResourceLimit(
                "manifest file name exceeds byte limit".into(),
            ));
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| TsmError::InvalidFormat("manifest file name is not UTF-8".into()))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.starts_with('.')
        || name.contains('/')
        || name.contains('\\')
        || Path::new(name).extension().and_then(|value| value.to_str()) != Some("parquet")
        || name.len() > MAX_FILE_NAME_BYTES
    {
        return Err(TsmError::InvalidInput(format!(
            "invalid active Parquet file name '{name}'"
        )));
    }
    Ok(())
}

fn max_sequence(left: Option<IngestSeq>, right: Option<IngestSeq>) -> Option<IngestSeq> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn validate_loaded_state(segments: &Path, state: ManifestState) -> Result<ManifestState> {
    validate_update_files(segments, &state.active_files)?;
    Ok(state)
}

fn cleanup_orphans(segments: &Path, active_files: &BTreeMap<String, ActiveFile>) -> Result<()> {
    for entry in fs::read_dir(segments)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_owned_candidate(&name) && !active_files.contains_key(&name) {
            fs::remove_file(entry.path())?;
        }
    }
    sync_directory(segments)?;
    Ok(())
}

fn cleanup_manifest_temps(root: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(".manifest-v3.tmp-")
        {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_directory(root)?;
    }
    Ok(())
}

fn is_owned_candidate(name: &str) -> bool {
    name.ends_with(".parquet") || name.contains(".parquet.tmp-")
}

struct TemporaryManifest {
    path: PathBuf,
    armed: bool,
}

impl TemporaryManifest {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryManifest {
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
    use super::{ActiveFile, ManifestStore, ManifestUpdate, RecoveredFrom, CURRENT_MANIFEST_FILE};
    use crate::store::seq::IngestSeq;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn create_segment(store: &ManifestStore, name: &str, contents: &[u8]) -> ActiveFile {
        let path = store.segments_dir().join(name);
        fs::write(&path, contents).expect("create segment");
        ActiveFile::new("test", name, 1, contents.len() as u64, 0, 0).expect("active file")
    }

    #[test]
    fn concurrent_delta_publishes_preserve_both_file_additions() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(ManifestStore::open(root.path()).expect("manifest"));
        let first = create_segment(&store, "first.parquet", b"first");
        let second = create_segment(&store, "second.parquet", b"second");
        let barrier = Arc::new(Barrier::new(3));

        let handles = [first, second].map(|file| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store
                    .publish(ManifestUpdate::new().add_file(file))
                    .expect("publish")
            })
        });
        barrier.wait();
        for handle in handles {
            handle.join().expect("publisher thread");
        }

        let state = store.state().expect("state");
        assert_eq!(state.generation(), 2);
        assert!(state.active_file("first.parquet").is_some());
        assert!(state.active_file("second.parquet").is_some());
        drop(store);
        let reopened = ManifestStore::open(root.path()).expect("reopen");
        assert_eq!(reopened.state().expect("state").active_files().len(), 2);
    }

    #[test]
    fn startup_removes_only_scoped_unreferenced_segments_without_growth() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let active = create_segment(&store, "active.parquet", b"active");
        store
            .publish(ManifestUpdate::new().add_file(active))
            .expect("publish active");
        fs::write(store.segments_dir().join("orphan.parquet"), b"orphan").expect("orphan");
        fs::write(
            store.segments_dir().join(".active.parquet.tmp-crashed"),
            b"partial",
        )
        .expect("temporary");
        fs::write(root.path().join("outside-user-file"), b"preserve").expect("outside");
        drop(store);

        for cycle in 0..3 {
            let reopened = ManifestStore::open(root.path()).expect("recover");
            assert!(reopened.segments_dir().join("active.parquet").exists());
            assert!(root.path().join("outside-user-file").exists());
            assert_eq!(
                fs::read_dir(reopened.segments_dir())
                    .expect("segments")
                    .count(),
                1
            );
            if cycle < 2 {
                fs::write(
                    reopened
                        .segments_dir()
                        .join(format!(".retry-{cycle}.parquet.tmp-crashed")),
                    b"partial",
                )
                .expect("next crash residue");
            }
        }
    }

    #[test]
    fn torn_current_manifest_falls_back_to_the_previous_generation() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let first = create_segment(&store, "first.parquet", b"first");
        store
            .publish(ManifestUpdate::new().add_file(first))
            .expect("generation one");
        let second = create_segment(&store, "second.parquet", b"second");
        store
            .publish(ManifestUpdate::new().add_file(second))
            .expect("generation two");
        drop(store);
        fs::write(root.path().join(CURRENT_MANIFEST_FILE), b"torn").expect("tear current");

        let recovered = ManifestStore::open(root.path()).expect("fallback");
        let state = recovered.state().expect("state");
        assert_eq!(recovered.recovered_from(), RecoveredFrom::Previous);
        assert_eq!(state.generation(), 1);
        assert!(state.active_file("first.parquet").is_some());
        assert!(state.active_file("second.parquet").is_none());
        assert!(!recovered.segments_dir().join("second.parquet").exists());

        let third = create_segment(&recovered, "third.parquet", b"third");
        recovered
            .publish(ManifestUpdate::new().add_file(third))
            .expect("publish after fallback");
        drop(recovered);
        let repaired = ManifestStore::open(root.path()).expect("reopen repaired current");
        assert_eq!(repaired.recovered_from(), RecoveredFrom::Current);
        assert!(repaired
            .state()
            .expect("state")
            .active_file("third.parquet")
            .is_some());
    }

    #[test]
    fn recovery_and_issued_high_water_marks_never_move_backwards() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        store
            .publish(
                ManifestUpdate::new()
                    .advance_persisted_through(IngestSeq::new(10))
                    .observe_issued_through(IngestSeq::new(12)),
            )
            .expect("advance");
        store
            .publish(
                ManifestUpdate::new()
                    .advance_persisted_through(IngestSeq::new(5))
                    .observe_issued_through(IngestSeq::new(7)),
            )
            .expect("stale update");

        let state = store.state().expect("state");
        assert_eq!(state.persisted_through(), Some(IngestSeq::new(10)));
        assert_eq!(state.issued_through(), Some(IngestSeq::new(12)));
        assert!(!state.should_replay(IngestSeq::new(10)));
        assert!(state.should_replay(IngestSeq::new(11)));
        drop(state);
        drop(store);

        let reopened = ManifestStore::open(root.path()).expect("reopen boundaries");
        assert_eq!(
            reopened.state().expect("state").persisted_through(),
            Some(IngestSeq::new(10))
        );
        let mut sequencer = reopened
            .resume_sequencer(Some(IngestSeq::new(15)))
            .expect("resume from manifest and WAL");
        let next = sequencer
            .issue(crate::model::WideRow::new(
                crate::model::SeriesKey::new("cpu", crate::model::Tags::new()),
                1,
                crate::model::Fields::new(),
            ))
            .expect("next row");
        assert_eq!(next.ingest_seq(), IngestSeq::new(16));
    }

    #[test]
    fn active_file_names_cannot_escape_the_segments_directory() {
        assert!(ActiveFile::new("cpu", "../escape.parquet", 1, 1, 0, 0).is_err());
        assert!(ActiveFile::new("cpu", "nested/file.parquet", 1, 1, 0, 0).is_err());
        assert!(ActiveFile::new("cpu", ".hidden.parquet", 1, 1, 0, 0).is_err());
        assert!(ActiveFile::new("cpu", "not-parquet.tmp", 1, 1, 0, 0).is_err());
    }

    #[test]
    fn active_file_timestamp_range_survives_manifest_reopen() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        fs::write(store.segments_dir().join("ranged.parquet"), b"x").expect("segment");
        let file = ActiveFile::new("cpu", "ranged.parquet", 1, 1, -5, 10).expect("active range");
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        drop(store);

        let reopened = ManifestStore::open(root.path()).expect("reopen");
        let state = reopened.state().expect("state");
        let file = state.active_file("ranged.parquet").expect("active");
        assert_eq!(file.min_timestamp(), -5);
        assert_eq!(file.max_timestamp(), 10);
    }
}
