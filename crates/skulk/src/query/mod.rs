//! Public query types and the query-engine namespace.

#[cfg(any(feature = "promql", feature = "sql-ts"))]
mod nimffi;

#[cfg(feature = "promql")]
pub mod promql;

mod types;

pub use types::{LabelMatcher, MatchOp, QueryResult, QueryResultKind, TSFunction};
