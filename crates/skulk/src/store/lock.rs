//! Single-process ownership for a Skulk data root.

use crate::error::{Result, TsmError};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Persistent inode used for OS-scoped data-root ownership.
pub const LOCK_FILE_NAME: &str = ".skulk-v3.lock";

/// Exclusive advisory lock released by the OS on close or process exit.
pub struct DataRootLock {
    file: File,
    path: PathBuf,
}

impl DataRootLock {
    /// Acquires exclusive ownership of one data root without stale PID semantics.
    pub fn acquire(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let path = root.join(LOCK_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        if let Err(error) = file.try_lock_exclusive() {
            return if error.kind() == ErrorKind::WouldBlock {
                Err(TsmError::InvalidInput(format!(
                    "Skulk data root '{}' is already open",
                    root.display()
                )))
            } else {
                Err(error.into())
            };
        }
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        writeln!(file, "{}", std::process::id())?;
        file.sync_data()?;
        Ok(Self { file, path })
    }

    /// Returns the persistent lock-file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DataRootLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::{DataRootLock, LOCK_FILE_NAME};
    use std::fs::File;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::Duration;

    const CHILD_ROOT_ENV: &str = "SKULK_LOCK_TEST_ROOT";
    const READY_FILE: &str = "lock-child-ready";
    const CHILD_READY_POLLS: usize = 3_000;

    #[test]
    fn second_open_fails_until_the_first_guard_is_dropped() {
        let root = tempfile::tempdir().expect("tempdir");
        let first = DataRootLock::acquire(root.path()).expect("first lock");

        assert!(DataRootLock::acquire(root.path()).is_err());
        assert_eq!(first.path(), root.path().join(LOCK_FILE_NAME));
        drop(first);

        DataRootLock::acquire(root.path()).expect("reacquire");
    }

    #[test]
    fn forced_process_exit_releases_ownership_despite_the_stale_lock_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "store::lock::tests::lock_holder_child",
                "--nocapture",
            ])
            .env(CHILD_ROOT_ENV, root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn lock holder");
        let ready = root.path().join(READY_FILE);
        for _ in 0..CHILD_READY_POLLS {
            if ready.exists() {
                break;
            }
            if child.try_wait().expect("query child").is_some() {
                panic!("lock holder exited before readiness");
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "lock holder did not become ready");
        assert!(DataRootLock::acquire(root.path()).is_err());

        child.kill().expect("force-kill holder");
        child.wait().expect("reap holder");

        assert!(root.path().join(LOCK_FILE_NAME).exists());
        DataRootLock::acquire(root.path()).expect("OS released stale ownership");
    }

    #[test]
    fn lock_holder_child() {
        let Some(root) = std::env::var_os(CHILD_ROOT_ENV) else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let _guard = DataRootLock::acquire(&root).expect("child lock");
        let ready = File::create(root.join(READY_FILE)).expect("ready marker");
        ready.sync_all().expect("sync ready marker");
        loop {
            thread::park();
        }
    }
}
