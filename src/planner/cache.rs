use crate::ast::{Datum, Expression};
use std::collections::HashMap;

/// Cache for storing evaluated expressions and constants
#[derive(Debug, Default)]
pub struct PlanCache {
    /// Cache for expression evaluation results
    expr_cache: HashMap<String, Datum>,
    /// Cache for constant expressions
    constant_cache: HashMap<String, bool>,
}

impl PlanCache {
    /// Create a new cache instance
    pub fn new() -> Self {
        Self {
            expr_cache: HashMap::new(),
            constant_cache: HashMap::new(),
        }
    }

    /// Get a cached expression result
    pub fn get_expr(&self, expr: &Expression) -> Option<&Datum> {
        let key = format!("{:?}", expr);
        self.expr_cache.get(&key)
    }

    /// Cache an expression result
    pub fn cache_expr(&mut self, expr: Expression, result: Datum) {
        let key = format!("{:?}", expr);
        self.expr_cache.insert(key, result);
    }

    /// Check if an expression is cached as constant
    pub fn is_constant_cached(&self, expr: &Expression) -> Option<bool> {
        let key = format!("{:?}", expr);
        self.constant_cache.get(&key).copied()
    }

    /// Cache whether an expression is constant
    pub fn cache_constant_check(&mut self, expr: Expression, is_constant: bool) {
        let key = format!("{:?}", expr);
        self.constant_cache.insert(key, is_constant);
    }
}
