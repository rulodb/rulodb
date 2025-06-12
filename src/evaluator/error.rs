use crate::ast::Datum;
use crate::storage::StorageError;

/// Evaluation errors that can occur during query execution
#[derive(Debug)]
pub enum EvalError {
    /// Storage backend error
    StorageError(StorageError),
    /// Invalid key type provided (expected string)
    InvalidKeyType,
    /// Required field is missing
    MissingField(String),
    /// Invalid target for insert operation
    InvalidInsertTarget,
    /// Invalid value used in match expression
    InvalidMatchValue(Datum),
    /// Invalid pattern in match expression
    InvalidMatchPattern(String),
    /// Failed to convert value to integer
    ConvertToInteger,
    /// Failed to convert value to float
    ConvertToFloat,
    /// Failed to convert value to string
    ConvertToString,
    /// Operation is not supported
    UnsupportedOperation,
    /// Invalid expression syntax or structure
    InvalidExpression,
    /// Invalid predicate in filter
    InvalidPredicate,
    /// Invalid order by clause
    InvalidOrderBy,
    /// Invalid subquery structure
    InvalidSubquery,
    /// Division by zero attempted
    DivisionByZero,
    /// Type mismatch in operation
    TypeMismatch,
    /// Invalid comparison operation
    InvalidComparison,
    /// Invalid limit value
    InvalidLimit,
    /// Invalid skip value
    InvalidSkip,
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageError(e) => write!(f, "Storage error: {e}"),
            Self::InvalidKeyType => write!(f, "Invalid key type: expected string"),
            Self::MissingField(field) => write!(f, "Missing required field: {field}"),
            Self::InvalidInsertTarget => write!(f, "Invalid document structure for insert"),
            Self::InvalidMatchValue(value) => write!(f, "Invalid match value: {value}"),
            Self::InvalidMatchPattern(pattern) => write!(f, "Invalid match pattern: {pattern}"),
            Self::ConvertToInteger => write!(f, "Cannot convert value to integer"),
            Self::ConvertToFloat => write!(f, "Cannot convert value to float"),
            Self::ConvertToString => write!(f, "Cannot convert value to string"),
            Self::UnsupportedOperation => write!(f, "Unsupported operation"),
            Self::InvalidExpression => write!(f, "Invalid expression"),
            Self::InvalidPredicate => write!(f, "Invalid predicate"),
            Self::InvalidOrderBy => write!(f, "Invalid order by clause"),
            Self::InvalidSubquery => write!(f, "Invalid subquery"),
            Self::DivisionByZero => write!(f, "Division by zero"),
            Self::TypeMismatch => write!(f, "Type mismatch in operation"),
            Self::InvalidComparison => write!(f, "Invalid comparison"),
            Self::InvalidLimit => write!(f, "Invalid limit value"),
            Self::InvalidSkip => write!(f, "Invalid skip value"),
        }
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StorageError> for EvalError {
    fn from(e: StorageError) -> Self {
        Self::StorageError(e)
    }
}

/// Statistics collected during query evaluation
#[derive(Debug, Clone, Default)]
pub struct EvalStats {
    pub rows_processed: usize,
    pub rows_returned: usize,
    pub duration_ms: u128,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl EvalStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge another stats instance into this one
    pub fn merge(&mut self, other: &EvalStats) {
        self.rows_processed += other.rows_processed;
        self.rows_returned += other.rows_returned;
        self.cache_hits += other.cache_hits;
        self.cache_misses += other.cache_misses;
    }

    /// Record rows processed
    pub fn record_rows_processed(&mut self, count: usize) {
        self.rows_processed += count;
    }

    /// Record rows returned
    pub fn record_rows_returned(&mut self, count: usize) {
        self.rows_returned += count;
    }

    /// Record cache hit
    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    /// Record cache miss
    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    /// Record execution duration
    pub fn record_duration(&mut self, duration: std::time::Duration) {
        self.duration_ms = duration.as_millis();
    }
}

/// Result of query evaluation including data and statistics
#[derive(Debug, Clone)]
pub struct EvalResult {
    pub result: crate::ast::query_result::Result,
    pub stats: EvalStats,
}

impl EvalResult {
    /// Create a new evaluation result
    pub fn new(result: crate::ast::query_result::Result, stats: EvalStats) -> Self {
        Self { result, stats }
    }
}
