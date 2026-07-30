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

    /// A text query could not be parsed.
    #[error("{language} parse error at line {line}, column {column}, offset {offset}: {message}")]
    Parse {
        /// The query language whose parser rejected the input.
        language: String,
        /// Human-readable diagnostic from the parser or semantic decoder.
        message: String,
        /// One-based source line, or zero when the parser supplied no position.
        line: usize,
        /// One-based source column, or zero when the parser supplied no position.
        column: usize,
        /// Zero-based byte offset, or zero when the parser supplied no position.
        offset: usize,
    },

    /// A syntactically valid query requested semantics outside this release.
    #[error(
        "unsupported query feature at line {line}, column {column}, offset {offset}: {feature}"
    )]
    Unsupported {
        /// Description of the unsupported language feature.
        feature: String,
        /// One-based source line.
        line: usize,
        /// One-based source column.
        column: usize,
        /// Zero-based byte offset.
        offset: usize,
    },

    /// A query expression has an invalid value type or function signature.
    #[error("query type error at line {line}, column {column}, offset {offset}: {message}")]
    Type {
        /// Description of the expected and actual query value types.
        message: String,
        /// One-based source line.
        line: usize,
        /// One-based source column.
        column: usize,
        /// Zero-based byte offset.
        offset: usize,
    },

    /// A validated query could not be converted into a logical execution plan.
    #[error("query planning error at line {line}, column {column}, offset {offset}: {message}")]
    Plan {
        /// Description of the planning constraint that failed.
        message: String,
        /// One-based source line, or zero for a programmatic plan.
        line: usize,
        /// One-based source column, or zero for a programmatic plan.
        column: usize,
        /// Zero-based UTF-8 byte offset.
        offset: usize,
    },

    /// The vendored query parser violated or did not match its wire contract.
    #[error("Nim parser FFI contract error: {0}")]
    FfiContract(String),

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
