//! On-disk format boundary checks.

use crate::error::{Result, TsmError};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

const V02_TSM_MAGIC: [u8; 4] = *b"ATSM";
const V02_WAL_MAGIC: [u8; 4] = *b"SWAL";
const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCAN_ENTRIES: usize = 200_000;

pub(crate) fn reject_legacy_data_root(root: &Path) -> Result<()> {
    let mut pending = vec![(root.to_owned(), 0_usize)];
    let mut scanned = 0_usize;
    while let Some((directory, depth)) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            scanned = scanned.checked_add(1).ok_or_else(|| {
                TsmError::ResourceLimit("legacy format scan count overflow".into())
            })?;
            if scanned > MAX_SCAN_ENTRIES {
                return Err(TsmError::ResourceLimit(format!(
                    "data root exceeds legacy format scan limit of {MAX_SCAN_ENTRIES} entries"
                )));
            }
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                if depth < MAX_SCAN_DEPTH {
                    pending.push((entry.path(), depth + 1));
                }
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let path = entry.path();
            match path.extension().and_then(|extension| extension.to_str()) {
                Some("skulk") => reject_candidate(&path, V02_TSM_MAGIC, "v0.2 TSM/Gorilla")?,
                Some("wal") => reject_candidate(&path, V02_WAL_MAGIC, "v0.2 WAL")?,
                _ => {}
            }
        }
    }
    Ok(())
}

fn reject_candidate(path: &Path, legacy_magic: [u8; 4], label: &str) -> Result<()> {
    let mut magic = [0_u8; 4];
    let mut file = File::open(path)?;
    let bytes_read = file.read(&mut magic)?;
    if bytes_read < magic.len() {
        return Err(TsmError::InvalidFormat(format!(
            "truncated legacy-format candidate '{}'",
            path.display()
        )));
    }
    if magic == legacy_magic {
        return Err(TsmError::InvalidFormat(format!(
            "{label} format is unsupported in v0.3: '{}'",
            path.display()
        )));
    }
    Err(TsmError::InvalidFormat(format!(
        "unsupported legacy file candidate '{}'",
        path.display()
    )))
}
