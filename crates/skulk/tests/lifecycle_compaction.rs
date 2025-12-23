//! Integration tests for compaction workflow.

use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
use alopex_skulk::tsm::{SeriesMeta, TimePartition, TsmReader, TsmWriter};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_compaction_end_to_end() {
    let temp_dir = TempDir::new().unwrap();
    let layout = PartitionLayout::new(temp_dir.path(), PartitionDuration::Hourly);
    let partition = TimePartition::new(0, Duration::from_secs(3600));

    let series_id = 1;
    let meta = SeriesMeta::new("metric", vec![]);

    for i in 0..5 {
        let path = layout.tsm_file_path(&partition, series_id, 0, i + 1);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut writer = TsmWriter::new(&path).unwrap();
        writer.set_level(0);

        let mut points = BTreeMap::new();
        points.insert(100, (100.0 + i as f64, (i + 1) as u64));
        points.insert(200 + i as i64, (200.0 + i as f64, (i + 1) as u64));

        writer.write_series(series_id, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    let strategy = CompactionStrategy::new(CompactionConfig::default(), layout.clone());
    let plan = strategy.needs_compaction(&partition).unwrap().unwrap();
    assert_eq!(plan.source_level, 0);
    assert_eq!(plan.target_level, 1);
    assert_eq!(plan.input_files.len(), 5);

    let result = block_on(strategy.run_compaction(&plan)).unwrap();
    assert_eq!(result.output_files.len(), 1);

    let output_path = &result.output_files[0];
    assert!(output_path.exists());
    let file_name = output_path.file_name().unwrap().to_string_lossy();
    assert!(file_name.contains("_L1_"));

    for input in &plan.input_files {
        assert!(!input.path.exists());
    }

    let reader = TsmReader::open(output_path).unwrap();
    let points = reader.read_series(series_id).unwrap().unwrap();
    let value = points.iter().find(|(ts, _)| *ts == 100).unwrap().1;
    assert!((value - 104.0).abs() < f64::EPSILON);
}

fn block_on<F: Future>(future: F) -> F::Output {
    fn raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            raw_waker()
        }
        let vtable = &RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), vtable)
    }

    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
