use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::QueryEngine;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> alopex_skulk::Result<()> {
    let root = TemporaryRoot::new()?;
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default())?;
    store.ingest(WideRow::new(
        SeriesKey::new(
            "footprint",
            Tags::from([("host".to_string(), "edge".to_string())]),
        ),
        0,
        Fields::from([("value".to_string(), FieldValue::Float(1.0))]),
    ))?;
    let engine = QueryEngine::new(&store);
    assert!(!engine.query_promql("footprint", 0)?.batches().is_empty());
    assert!(!engine
        .query_sql("SELECT AVG(value) AS average FROM footprint", 0)?
        .batches()
        .is_empty());
    Ok(())
}

struct TemporaryRoot {
    path: PathBuf,
}

impl TemporaryRoot {
    fn new() -> alopex_skulk::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                alopex_skulk::TsmError::InvalidInput(format!(
                    "system clock precedes epoch: {error}"
                ))
            })?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "alopex-skulk-query-footprint-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
