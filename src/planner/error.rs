use std::fmt;

/// Errors that can occur during query planning
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    /// Operation is not supported
    UnsupportedOperation(String),
    /// Invalid expression in query
    InvalidExpression(String),
    /// Table reference is missing
    MissingTableReference,
    /// Invalid constant value
    InvalidConstant(String),
    /// Optimization failed
    OptimizationFailed(String),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::UnsupportedOperation(msg) => write!(f, "Unsupported operation: {msg}"),
            PlanError::InvalidExpression(msg) => write!(f, "Invalid expression: {msg}"),
            PlanError::MissingTableReference => write!(f, "Missing table reference"),
            PlanError::InvalidConstant(msg) => write!(f, "Invalid constant: {msg}"),
            PlanError::OptimizationFailed(msg) => write!(f, "Optimization failed: {msg}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Result type for planning operations
pub type PlanResult<T> = Result<T, PlanError>;
