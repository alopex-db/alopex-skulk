//! Error and result types shared by Skulk components.

use std::io;
use thiserror::Error;

/// A convenience result type for Skulk operations.
pub type Result<T> = std::result::Result<T, TsmError>;

/// Errors returned by the v0.3 storage and ingestion boundaries.
#[derive(Debug, Error)]
pub enum TsmError {
    /// The input does not match a supported v0.3 format.
    #[error("unsupported or invalid format: {0}")]
    InvalidFormat(String),

    /// A caller supplied an invalid value.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// An input or in-memory resource limit was exceeded.
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Durable data failed an integrity check.
    #[error("data corruption: {0}")]
    Corruption(String),

    /// Durable and pending columns disagree on a query-visible schema.
    #[error(
        "schema conflict for measurement '{measurement}', column '{column}': \
         existing {existing}, incoming {incoming}"
    )]
    SchemaConflict {
        /// The affected measurement.
        measurement: String,
        /// The conflicting column name.
        column: String,
        /// The already observed role and type.
        existing: String,
        /// The newly observed role and type.
        incoming: String,
    },

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// An underlying filesystem operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}
