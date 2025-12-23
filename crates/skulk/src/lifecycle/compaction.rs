//! Compaction strategy and planning for TSM files.

use crate::error::Result;
use crate::lifecycle::partition::{PartitionLayout, TsmFileInfo};
use crate::tsm::file::{SeriesIndexEntry, TsmDataBlock, TsmReader, TsmWriter};
use crate::tsm::{SeriesId, SeriesMeta, TimePartition, Timestamp};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

/// Compaction level configuration.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::compaction::LevelConfig;
///
/// let level = LevelConfig {
///     level: 0,
///     max_files: 4,
///     target_file_size: 4 * 1024 * 1024,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct LevelConfig {
    /// Level number (L0, L1, ...).
    pub level: u8,
    /// Max number of files before compaction triggers.
    pub max_files: usize,
    /// Target file size in bytes.
    pub target_file_size: u64,
}

/// Compaction configuration for all levels.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::compaction::CompactionConfig;
///
/// let config = CompactionConfig::default();
/// ```
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Level configurations sorted by level.
    pub levels: Vec<LevelConfig>,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            levels: vec![
                LevelConfig {
                    level: 0,
                    max_files: 4,
                    target_file_size: 4 * 1024 * 1024,
                },
                LevelConfig {
                    level: 1,
                    max_files: 10,
                    target_file_size: 40 * 1024 * 1024,
                },
                LevelConfig {
                    level: 2,
                    max_files: 100,
                    target_file_size: 400 * 1024 * 1024,
                },
            ],
        }
    }
}

impl CompactionConfig {
    /// Returns level configuration for the given level.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::CompactionConfig;
    ///
    /// let config = CompactionConfig::default();
    /// let level = config.level_config(0);
    /// ```
    pub fn level_config(&self, level: u8) -> Option<&LevelConfig> {
        self.levels.iter().find(|config| config.level == level)
    }
}

/// Compaction plan for a partition.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::compaction::CompactionPlan;
/// use alopex_skulk::tsm::TimePartition;
/// use std::time::Duration;
///
/// let partition = TimePartition::new(0, Duration::from_secs(3600));
/// let plan = CompactionPlan {
///     partition,
///     source_level: 0,
///     target_level: 1,
///     input_files: Vec::new(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// Target partition.
    pub partition: TimePartition,
    /// Source level.
    pub source_level: u8,
    /// Target level.
    pub target_level: u8,
    /// Input files to compact.
    pub input_files: Vec<TsmFileInfo>,
}

/// Compaction result metadata.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::compaction::CompactionResult;
///
/// let result = CompactionResult::default();
/// ```
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Output file paths.
    pub output_files: Vec<PathBuf>,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Total points merged.
    pub points_merged: u64,
    /// Duplicate points resolved.
    pub duplicates_resolved: u64,
}

/// Compaction strategy for building plans and running compaction.
///
/// # Examples
/// ```rust,ignore
/// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
/// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
///
/// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
/// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
/// ```
#[derive(Debug, Clone)]
pub struct CompactionStrategy {
    config: CompactionConfig,
    layout: PartitionLayout,
}

impl CompactionStrategy {
    /// Creates a new compaction strategy.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
    /// ```
    pub fn new(config: CompactionConfig, layout: PartitionLayout) -> Self {
        Self { config, layout }
    }

    /// Returns the compaction config.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
    /// let _config = strategy.config();
    /// ```
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// Returns the partition layout.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
    /// let _layout = strategy.layout();
    /// ```
    pub fn layout(&self) -> &PartitionLayout {
        &self.layout
    }

    /// Returns a compaction plan when file count exceeds thresholds.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    /// use alopex_skulk::tsm::TimePartition;
    /// use std::time::Duration;
    ///
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
    /// let partition = TimePartition::new(0, Duration::from_secs(3600));
    /// let _plan = strategy.needs_compaction(&partition);
    /// ```
    pub fn needs_compaction(&self, partition: &TimePartition) -> Result<Option<CompactionPlan>> {
        let files = self.layout.list_tsm_files(partition)?;
        let mut files_by_level: HashMap<u8, Vec<TsmFileInfo>> = HashMap::new();
        for file in files {
            files_by_level.entry(file.level).or_default().push(file);
        }

        for level_config in &self.config.levels {
            let files_for_level = files_by_level
                .get(&level_config.level)
                .cloned()
                .unwrap_or_default();
            if files_for_level.len() > level_config.max_files {
                let target_level = level_config.level + 1;
                if self.config.level_config(target_level).is_none() {
                    return Ok(None);
                }
                return Ok(Some(CompactionPlan {
                    partition: partition.clone(),
                    source_level: level_config.level,
                    target_level,
                    input_files: files_for_level,
                }));
            }
        }

        Ok(None)
    }

    /// Runs compaction for the provided plan.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use alopex_skulk::lifecycle::compaction::{CompactionConfig, CompactionStrategy};
    /// use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
    /// use alopex_skulk::tsm::TimePartition;
    /// use std::time::Duration;
    ///
    /// # async fn run() -> alopex_skulk::error::Result<()> {
    /// let layout = PartitionLayout::new("/data", PartitionDuration::Hourly);
    /// let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
    /// let partition = TimePartition::new(0, Duration::from_secs(3600));
    /// if let Some(plan) = strategy.needs_compaction(&partition)? {
    ///     let _result = strategy.run_compaction(&plan).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_compaction(&self, plan: &CompactionPlan) -> Result<CompactionResult> {
        let mut blocks_by_series: HashMap<SeriesId, Vec<BlockPoints>> = HashMap::new();
        let mut series_meta: HashMap<SeriesId, SeriesMeta> = HashMap::new();
        let max_generation = next_generation_map(&self.layout, &plan.partition)?;

        for file in &plan.input_files {
            let reader = TsmReader::open(&file.path)?;
            for (series_id, entry) in reader.index().iter() {
                let meta = SeriesMeta::new(entry.metric_name.clone(), entry.labels.clone());
                if let Some(existing) = series_meta.get(series_id) {
                    if existing != &meta {
                        return Err(crate::error::TsmError::CompactionError(format!(
                            "Series meta mismatch for series {}",
                            series_id
                        )));
                    }
                } else {
                    series_meta.insert(*series_id, meta);
                }
                let block = read_block(&file.path, entry)?;
                let points = decompress_block(&block)?;
                blocks_by_series
                    .entry(*series_id)
                    .or_default()
                    .push(BlockPoints {
                        max_lsn: block.max_lsn,
                        generation: file.generation,
                        points,
                    });
            }
        }

        let mut result = CompactionResult::default();
        for (series_id, blocks) in blocks_by_series {
            if blocks.is_empty() {
                continue;
            }
            let (merged, duplicates) = merge_blocks(&blocks);
            let generation = max_generation
                .get(&series_id)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            let output_path = self.layout.tsm_file_path(
                &plan.partition,
                series_id,
                plan.target_level,
                generation,
            );
            let tmp_path = output_path.with_extension("skulk.tmp");

            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut writer = TsmWriter::new(&tmp_path)?;
            writer.set_level(plan.target_level as u16);
            let meta = series_meta.get(&series_id).ok_or_else(|| {
                crate::error::TsmError::CompactionError(format!(
                    "Missing series meta for series {}",
                    series_id
                ))
            })?;
            writer.write_series(series_id, meta, &merged)?;
            writer.finish()?;

            fs::rename(&tmp_path, &output_path)?;

            let bytes = fs::metadata(&output_path)?.len();
            result.bytes_written += bytes;
            result.points_merged += merged.len() as u64;
            result.duplicates_resolved += duplicates as u64;
            result.output_files.push(output_path);
        }

        for input in &plan.input_files {
            fs::remove_file(&input.path)?;
        }

        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct BlockPoints {
    max_lsn: u64,
    generation: u32,
    points: Vec<(Timestamp, f64)>,
}

fn merge_blocks(blocks: &[BlockPoints]) -> (BTreeMap<Timestamp, (f64, u64)>, usize) {
    let mut merged: BTreeMap<Timestamp, (f64, u64, u32)> = BTreeMap::new();
    let mut duplicates = 0;

    for block in blocks {
        for (ts, value) in &block.points {
            match merged.get(ts) {
                Some((_, existing_lsn, existing_gen)) => {
                    let should_replace = block.max_lsn > *existing_lsn
                        || (block.max_lsn == *existing_lsn && block.generation > *existing_gen);
                    if should_replace {
                        merged.insert(*ts, (*value, block.max_lsn, block.generation));
                    }
                    duplicates += 1;
                }
                None => {
                    merged.insert(*ts, (*value, block.max_lsn, block.generation));
                }
            }
        }
    }

    let final_map = merged
        .into_iter()
        .map(|(ts, (value, lsn, _))| (ts, (value, lsn)))
        .collect();

    (final_map, duplicates)
}

fn read_block(path: &Path, entry: &SeriesIndexEntry) -> Result<TsmDataBlock> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    use std::io::Seek;
    reader.seek(std::io::SeekFrom::Start(entry.block_offset))?;
    TsmDataBlock::read_from(&mut reader)
}

fn decompress_block(block: &TsmDataBlock) -> Result<Vec<(Timestamp, f64)>> {
    use crate::tsm::file::{TimestampEncoding, ValueEncoding};
    use crate::tsm::gorilla::CompressedBlock;
    use bitvec::prelude::*;

    if block.ts_encoding == TimestampEncoding::DeltaOfDelta
        && block.val_encoding == ValueEncoding::GorillaXor
    {
        let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
        let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
        let compressed = CompressedBlock {
            timestamps: ts_bitvec,
            values: val_bitvec,
            count: block.point_count,
        };
        return Ok(compressed.decompress());
    }

    let timestamps: Vec<i64> = if block.ts_encoding == TimestampEncoding::DeltaOfDelta {
        let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
        let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
        let compressed = CompressedBlock {
            timestamps: ts_bitvec,
            values: val_bitvec,
            count: block.point_count,
        };
        compressed
            .decompress()
            .into_iter()
            .map(|(ts, _)| ts)
            .collect()
    } else {
        let mut cursor = std::io::Cursor::new(&block.ts_data);
        let mut timestamps = Vec::with_capacity(block.point_count as usize);
        for _ in 0..block.point_count {
            let mut buf8 = [0u8; 8];
            use std::io::Read;
            cursor.read_exact(&mut buf8)?;
            timestamps.push(i64::from_le_bytes(buf8));
        }
        timestamps
    };

    let values: Vec<f64> = if block.val_encoding == ValueEncoding::GorillaXor {
        let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
        let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
        let compressed = CompressedBlock {
            timestamps: ts_bitvec,
            values: val_bitvec,
            count: block.point_count,
        };
        compressed
            .decompress()
            .into_iter()
            .map(|(_, val)| val)
            .collect()
    } else {
        let mut cursor = std::io::Cursor::new(&block.val_data);
        let mut values = Vec::with_capacity(block.point_count as usize);
        for _ in 0..block.point_count {
            let mut buf8 = [0u8; 8];
            use std::io::Read;
            cursor.read_exact(&mut buf8)?;
            values.push(f64::from_le_bytes(buf8));
        }
        values
    };

    let points = timestamps.into_iter().zip(values).collect();
    Ok(points)
}

fn next_generation_map(
    layout: &PartitionLayout,
    partition: &TimePartition,
) -> Result<HashMap<SeriesId, u32>> {
    let mut map = HashMap::new();
    let files = layout.list_tsm_files(partition)?;
    for file in files {
        map.entry(file.series_id)
            .and_modify(|gen| {
                if file.generation > *gen {
                    *gen = file.generation;
                }
            })
            .or_insert(file.generation);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::partition::PartitionDuration;
    use crate::tsm::SeriesMeta;
    use std::future::Future;
    use tempfile::TempDir;

    #[test]
    fn test_needs_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let layout = PartitionLayout::new(temp_dir.path(), PartitionDuration::Hourly);
        let partition = TimePartition::new(0, std::time::Duration::from_secs(3600));

        let mut files = Vec::new();
        for i in 0..5 {
            let path = layout.tsm_file_path(&partition, 1, 0, i + 1);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"").unwrap();
            files.push(path);
        }

        let strategy = CompactionStrategy::new(CompactionConfig::default(), layout);
        let plan = strategy.needs_compaction(&partition).unwrap();
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.input_files.len(), 5);
    }

    #[test]
    fn test_merge_blocks_duplicate_resolution() {
        let blocks = vec![
            BlockPoints {
                max_lsn: 10,
                generation: 1,
                points: vec![(100, 1.0), (200, 2.0)],
            },
            BlockPoints {
                max_lsn: 12,
                generation: 1,
                points: vec![(100, 3.0)],
            },
            BlockPoints {
                max_lsn: 12,
                generation: 2,
                points: vec![(200, 4.0)],
            },
        ];

        let (merged, duplicates) = merge_blocks(&blocks);
        assert_eq!(duplicates, 2);
        assert_eq!(merged.get(&100).unwrap().0, 3.0);
        assert_eq!(merged.get(&200).unwrap().0, 4.0);
    }

    #[test]
    fn test_run_compaction_merges_files() {
        let temp_dir = TempDir::new().unwrap();
        let layout = PartitionLayout::new(temp_dir.path(), PartitionDuration::Hourly);
        let partition = TimePartition::new(0, std::time::Duration::from_secs(3600));

        let series_id = 1;
        let meta = SeriesMeta::new("metric", vec![]);
        let file1 = layout.tsm_file_path(&partition, series_id, 0, 1);
        let file2 = layout.tsm_file_path(&partition, series_id, 0, 2);
        fs::create_dir_all(file1.parent().unwrap()).unwrap();

        let mut points1 = BTreeMap::new();
        points1.insert(100, (1.0, 10));
        points1.insert(200, (2.0, 10));

        let mut points2 = BTreeMap::new();
        points2.insert(100, (3.0, 12));

        {
            let mut writer = TsmWriter::new(&file1).unwrap();
            writer.write_series(series_id, &meta, &points1).unwrap();
            writer.finish().unwrap();
        }

        {
            let mut writer = TsmWriter::new(&file2).unwrap();
            writer.write_series(series_id, &meta, &points2).unwrap();
            writer.finish().unwrap();
        }

        let strategy = CompactionStrategy::new(CompactionConfig::default(), layout.clone());
        let plan = CompactionPlan {
            partition: partition.clone(),
            source_level: 0,
            target_level: 1,
            input_files: vec![
                TsmFileInfo {
                    series_id,
                    level: 0,
                    generation: 1,
                    path: file1.clone(),
                },
                TsmFileInfo {
                    series_id,
                    level: 0,
                    generation: 2,
                    path: file2.clone(),
                },
            ],
        };

        let result = block_on(strategy.run_compaction(&plan)).unwrap();
        assert_eq!(result.output_files.len(), 1);
        let output = &result.output_files[0];
        let reader = TsmReader::open(output).unwrap();
        let points = reader.read_series(series_id).unwrap().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].1, 3.0);
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::pin::Pin;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        fn raw_waker() -> RawWaker {
            fn no_op(_: *const ()) {}
            fn clone(_: *const ()) -> RawWaker {
                raw_waker()
            }
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }

        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match Pin::new(&mut future).poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => continue,
            }
        }
    }
}
