# Changelog

## [0.2.0] - 2025-12-23

### Added
- Retention/TTL manager with DropPartition WAL entries and safe LSN tracking.
- Time-based partition layout and lifecycle management.
- TSM compaction strategy with duplicate resolution.
- WAL truncation integration and DropPartition recovery handling.
- Lifecycle integration tests and API documentation updates.

### Changed
- TSM file format v3 with block-level max_lsn metadata.
- WAL v2 fixed-size entries for data and DropPartition operations.
