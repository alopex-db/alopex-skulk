//! Public query types and the query-engine namespace.

#[cfg(any(feature = "promql", feature = "sql-ts"))]
mod nimffi;

#[cfg(feature = "promql")]
pub mod promql;

pub mod plan;

#[cfg(feature = "sql-ts")]
pub mod sqlts;

mod types;

pub use types::{LabelMatcher, MatchOp, QueryResult, QueryResultKind, TSFunction};
