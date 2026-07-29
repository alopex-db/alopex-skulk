use arrow_array::RecordBatch;
use std::slice;
use std::time::Duration;

/// The comparison operation applied by a [`LabelMatcher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    /// The label value must equal the matcher value.
    Equal,
    /// The label value must not equal the matcher value.
    NotEqual,
    /// The label value must match the regular expression.
    Regex,
    /// The label value must not match the regular expression.
    NotRegex,
}

/// A predicate over one time-series label.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LabelMatcher {
    /// The label name to inspect.
    pub name: String,
    /// The comparison operation.
    pub op: MatchOp,
    /// The literal or regular-expression comparison value.
    pub value: String,
}

impl LabelMatcher {
    /// Creates a label matcher from string-like values.
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
}

/// A time-series transformation understood by the shared query layer.
#[derive(Debug, Clone, PartialEq)]
pub enum TSFunction {
    /// Groups timestamps into fixed-width intervals.
    TimeBucket {
        /// The bucket width.
        interval: Duration,
        /// The timestamp column to bucket.
        column: String,
    },
    /// Calculates a per-second rate for a counter column.
    Rate {
        /// The input counter column.
        column: String,
    },
    /// Calculates the difference between successive values.
    Delta {
        /// The input value column.
        column: String,
    },
    /// Calculates the time derivative of successive values.
    Derivative {
        /// The input value column.
        column: String,
    },
    /// Selects the first value by timestamp.
    First {
        /// The value column to select.
        value_column: String,
        /// The timestamp column used for ordering.
        time_column: String,
    },
    /// Selects the last value by timestamp.
    Last {
        /// The value column to select.
        value_column: String,
        /// The timestamp column used for ordering.
        time_column: String,
    },
    /// Estimates a quantile from histogram buckets.
    HistogramQuantile {
        /// The target quantile in the inclusive range from zero to one.
        quantile: f64,
        /// The histogram bucket column.
        column: String,
    },
}

/// The logical shape of a [`QueryResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryResultKind {
    /// One scalar sample.
    Scalar,
    /// One current sample per series.
    Vector,
    /// A sequence of samples per series.
    Matrix,
    /// A schema-preserving tabular result.
    Table,
}

/// The logical result produced by a query frontend.
///
/// Physical values are carried as Arrow record batches. Time-series frontends
/// use the canonical long-form schema, while SQL queries preserve their table
/// schema.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// One scalar sample represented by a single-row record batch.
    Scalar(RecordBatch),
    /// One current sample per series.
    Vector(Vec<RecordBatch>),
    /// A sequence of samples per series.
    Matrix(Vec<RecordBatch>),
    /// A schema-preserving tabular result.
    Table(Vec<RecordBatch>),
}

impl QueryResult {
    /// Returns the logical result shape.
    pub const fn kind(&self) -> QueryResultKind {
        match self {
            Self::Scalar(_) => QueryResultKind::Scalar,
            Self::Vector(_) => QueryResultKind::Vector,
            Self::Matrix(_) => QueryResultKind::Matrix,
            Self::Table(_) => QueryResultKind::Table,
        }
    }

    /// Returns every physical Arrow record batch in the result.
    pub fn batches(&self) -> &[RecordBatch] {
        match self {
            Self::Scalar(batch) => slice::from_ref(batch),
            Self::Vector(batches) | Self::Matrix(batches) | Self::Table(batches) => {
                batches.as_slice()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use std::sync::Arc;
    use std::time::Duration;

    fn empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    #[test]
    fn label_matcher_shape_is_stable_for_query_common() {
        let matcher = LabelMatcher::new("host", MatchOp::Regex, "edge-[0-9]+");

        assert_eq!(matcher.name, "host");
        assert_eq!(matcher.op, MatchOp::Regex);
        assert_eq!(matcher.value, "edge-[0-9]+");
    }

    #[test]
    fn time_series_functions_cover_the_query_common_contract() {
        let functions = [
            TSFunction::TimeBucket {
                interval: Duration::from_secs(60),
                column: "time".into(),
            },
            TSFunction::Rate {
                column: "requests".into(),
            },
            TSFunction::Delta {
                column: "temperature".into(),
            },
            TSFunction::Derivative {
                column: "position".into(),
            },
            TSFunction::First {
                value_column: "value".into(),
                time_column: "time".into(),
            },
            TSFunction::Last {
                value_column: "value".into(),
                time_column: "time".into(),
            },
            TSFunction::HistogramQuantile {
                quantile: 0.95,
                column: "bucket".into(),
            },
        ];

        assert_eq!(functions.len(), 7);
    }

    #[test]
    fn query_result_exposes_the_four_d11_logical_forms() {
        let cases = [
            (QueryResult::Scalar(empty_batch()), QueryResultKind::Scalar),
            (
                QueryResult::Vector(vec![empty_batch()]),
                QueryResultKind::Vector,
            ),
            (
                QueryResult::Matrix(vec![empty_batch()]),
                QueryResultKind::Matrix,
            ),
            (
                QueryResult::Table(vec![empty_batch()]),
                QueryResultKind::Table,
            ),
        ];

        for (result, expected_kind) in cases {
            assert_eq!(result.kind(), expected_kind);
            assert_eq!(result.batches().len(), 1);
        }
    }
}
