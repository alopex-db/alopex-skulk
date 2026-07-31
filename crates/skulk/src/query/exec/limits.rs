//! Shared query resource limits and cooperative execution control.

use crate::store::reader::StorageReaderConfig;
use crate::{Result, TsmError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default wall-clock budget for one query.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Limits enforced before/during parser mapping and regular-expression compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsingLimits {
    max_input_bytes: usize,
    max_ast_depth: usize,
    max_ast_nodes: usize,
    max_regex_bytes: usize,
    max_regex_automaton_bytes: usize,
}

impl ParsingLimits {
    /// Embedded parser defaults.
    pub const DEFAULT: Self = Self {
        max_input_bytes: 1 << 20,
        max_ast_depth: 64,
        max_ast_nodes: 65_536,
        max_regex_bytes: 32 << 10,
        max_regex_automaton_bytes: 2 << 20,
    };

    /// Creates non-zero parser and regular-expression limits.
    pub fn new(
        max_input_bytes: usize,
        max_ast_depth: usize,
        max_ast_nodes: usize,
        max_regex_bytes: usize,
        max_regex_automaton_bytes: usize,
    ) -> Result<Self> {
        if max_input_bytes == 0
            || max_ast_depth == 0
            || max_ast_nodes == 0
            || max_regex_bytes == 0
            || max_regex_automaton_bytes == 0
        {
            return Err(TsmError::InvalidInput(
                "query parsing limits must all be non-zero".to_string(),
            ));
        }
        Ok(Self {
            max_input_bytes,
            max_ast_depth,
            max_ast_nodes,
            max_regex_bytes,
            max_regex_automaton_bytes,
        })
    }

    /// Returns the UTF-8 query byte limit checked before FFI.
    pub const fn max_input_bytes(self) -> usize {
        self.max_input_bytes
    }

    /// Returns the mapped AST nesting-depth limit.
    pub const fn max_ast_depth(self) -> usize {
        self.max_ast_depth
    }

    /// Returns the mapped AST node-count limit.
    pub const fn max_ast_nodes(self) -> usize {
        self.max_ast_nodes
    }

    /// Returns the source byte limit for one regular expression.
    pub const fn max_regex_bytes(self) -> usize {
        self.max_regex_bytes
    }

    /// Returns the compiled regex/DFA byte limit.
    pub const fn max_regex_automaton_bytes(self) -> usize {
        self.max_regex_automaton_bytes
    }
}

impl Default for ParsingLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Limits for series expansion and post-pruning storage decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanLimits {
    max_series_expansion: usize,
    max_decoded_rows: usize,
    max_decoded_bytes: usize,
}

impl ScanLimits {
    /// Embedded scan-governance defaults.
    pub const DEFAULT: Self = Self {
        max_series_expansion: 100_000,
        max_decoded_rows: StorageReaderConfig::DEFAULT.max_decoded_rows(),
        max_decoded_bytes: StorageReaderConfig::DEFAULT.max_decoded_bytes(),
    };

    /// Creates non-zero series and decode limits.
    pub fn new(
        max_series_expansion: usize,
        max_decoded_rows: usize,
        max_decoded_bytes: usize,
    ) -> Result<Self> {
        if max_series_expansion == 0 || max_decoded_rows == 0 || max_decoded_bytes == 0 {
            return Err(TsmError::InvalidInput(
                "query scan limits must all be non-zero".to_string(),
            ));
        }
        Ok(Self {
            max_series_expansion,
            max_decoded_rows,
            max_decoded_bytes,
        })
    }

    /// Returns the maximum measurements/series expanded by one query.
    pub const fn max_series_expansion(self) -> usize {
        self.max_series_expansion
    }

    /// Returns the maximum post-pruning rows passed to decoders.
    pub const fn max_decoded_rows(self) -> usize {
        self.max_decoded_rows
    }

    /// Returns the maximum post-pruning projected compressed bytes.
    pub const fn max_decoded_bytes(self) -> usize {
        self.max_decoded_bytes
    }
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Allocation limits applied at executor operator boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    max_intermediate_rows: usize,
    max_output_samples: usize,
    max_range_steps: usize,
}

impl ExecutionLimits {
    /// Embedded executor defaults.
    pub const DEFAULT: Self = Self {
        max_intermediate_rows: 1_000_000,
        max_output_samples: 1_000_000,
        max_range_steps: 100_000,
    };

    /// Creates non-zero operator and output limits.
    pub fn new(
        max_intermediate_rows: usize,
        max_output_samples: usize,
        max_range_steps: usize,
    ) -> Result<Self> {
        if max_intermediate_rows == 0 || max_output_samples == 0 || max_range_steps == 0 {
            return Err(TsmError::InvalidInput(
                "executor row, output, and range-step limits must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            max_intermediate_rows,
            max_output_samples,
            max_range_steps,
        })
    }

    /// Returns the per-operator input row/sample limit.
    pub const fn max_intermediate_rows(self) -> usize {
        self.max_intermediate_rows
    }

    /// Returns the complete query output sample limit.
    pub const fn max_output_samples(self) -> usize {
        self.max_output_samples
    }

    /// Returns the maximum number of inclusive range evaluations.
    pub const fn max_range_steps(self) -> usize {
        self.max_range_steps
    }
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One immutable set of parser, scan, and executor limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    parsing: ParsingLimits,
    scan: ScanLimits,
    execution: ExecutionLimits,
}

impl QueryLimits {
    /// Embedded defaults shared by every query stage.
    pub const DEFAULT: Self = Self::new(
        ParsingLimits::DEFAULT,
        ScanLimits::DEFAULT,
        ExecutionLimits::DEFAULT,
    );

    /// Composes already-validated stage limits.
    pub const fn new(parsing: ParsingLimits, scan: ScanLimits, execution: ExecutionLimits) -> Self {
        Self {
            parsing,
            scan,
            execution,
        }
    }

    /// Returns parser and regex limits.
    pub const fn parsing(self) -> ParsingLimits {
        self.parsing
    }

    /// Returns series and decode limits.
    pub const fn scan(self) -> ScanLimits {
        self.scan
    }

    /// Returns executor allocation limits.
    pub const fn execution(self) -> ExecutionLimits {
        self.execution
    }

    /// Replaces only the executor stage limits.
    pub const fn with_execution(mut self, execution: ExecutionLimits) -> Self {
        self.execution = execution;
        self
    }
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Cloneable cooperative-cancellation signal.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Creates a signal in the running state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation for every context sharing this token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Per-query immutable limits, absolute deadline, and cancellation signal.
#[derive(Debug, Clone)]
pub struct QueryExecutionContext {
    limits: QueryLimits,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl QueryExecutionContext {
    pub(crate) fn with_default_timeout(limits: QueryLimits) -> Self {
        let now = Instant::now();
        let deadline = now.checked_add(DEFAULT_QUERY_TIMEOUT).unwrap_or(now);
        Self::with_deadline(limits, deadline, CancellationToken::new())
    }

    /// Creates a context whose deadline is relative to the current instant.
    pub fn with_timeout(
        limits: QueryLimits,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        if timeout.is_zero() {
            return Err(TsmError::InvalidInput(
                "query timeout must be non-zero".to_string(),
            ));
        }
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            TsmError::ResourceLimit("query timeout exceeds monotonic clock range".to_string())
        })?;
        Ok(Self::with_deadline(limits, deadline, cancellation))
    }

    /// Creates a context with an explicit monotonic deadline.
    pub fn with_deadline(
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            limits,
            deadline,
            cancellation,
        }
    }

    /// Returns all resource limits for this query.
    pub const fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// Returns the absolute monotonic deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns a clone of the cooperative cancellation signal.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Fails at a cooperative boundary after cancellation or deadline expiry.
    pub fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(TsmError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(TsmError::Timeout);
        }
        Ok(())
    }
}
