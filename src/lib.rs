//! RuloDB - A high-performance NoSQL database with RethinkDB-like query language
//!
//! This library provides the core components for RuloDB including:
//! - AST definitions for the query language
//! - Parser for converting wire protocol messages to AST
//! - Planner for query optimization and execution planning
//! - Evaluator for executing planned queries
//! - Storage backend abstraction and implementations

pub mod ast;
pub mod evaluator;
pub mod parser;
pub mod planner;
pub mod storage;

// Re-export commonly used types
pub use ast::{Datum, Expr, Term, TermType};
pub use evaluator::{EvalError, EvalResult, EvalStats, Evaluator};
pub use parser::{ParseError, Parser};
pub use planner::{PlanError, PlanNode, Planner};
pub use storage::{DefaultStorage, StorageBackend, StorageError};
