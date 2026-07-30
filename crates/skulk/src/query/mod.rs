//! Public query types and the query-engine namespace.

#[cfg(any(feature = "promql", feature = "sql-ts"))]
mod nimffi;

#[cfg(feature = "promql")]
pub mod promql;

pub mod engine;

pub mod exec;

pub mod plan;

#[cfg(feature = "sql-ts")]
pub mod sqlts;

mod types;

pub use engine::{MetadataRequest, QueryEngine};
pub use exec::limits::{
    CancellationToken, ExecutionLimits, ParsingLimits, QueryExecutionContext, QueryLimits,
    ScanLimits, DEFAULT_QUERY_TIMEOUT,
};
pub use types::{LabelMatcher, MatchOp, QueryResult, QueryResultKind, TSFunction};
