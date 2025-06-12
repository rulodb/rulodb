mod builder;
mod cache;
mod error;
mod explain;
mod node;
mod optimizer;

use crate::ast::{Cursor, Query};

// Re-export commonly used types
pub use error::{PlanError, PlanResult};
pub use explain::{ExplanationNode, PlanExplanation};
pub use node::{FILTER_COST, GET_COST, PlanNode, TABLE_SCAN_COST};

use builder::PlanBuilder;
use optimizer::PlanOptimizer;

/// The main query planner that converts AST queries into optimized execution plans
pub struct Planner {
    /// Cursor context for paginated queries
    cursor_context: Option<Cursor>,
    /// Builder for constructing plans
    builder: PlanBuilder,
    /// Optimizer for improving plans
    optimizer: PlanOptimizer,
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

impl Planner {
    /// Create a new planner instance
    pub fn new() -> Self {
        Self {
            cursor_context: None,
            builder: PlanBuilder::new(),
            optimizer: PlanOptimizer::new(),
        }
    }

    /// Plan a query and return an optimized execution plan
    pub fn plan(&mut self, query: &Query) -> PlanResult<PlanNode> {
        // Set cursor context in builder
        self.builder = PlanBuilder::with_cursor(self.cursor_context.clone());

        // Build the initial plan
        let initial_plan = self.builder.build(query)?;

        // Optimize the plan
        let old_builder = std::mem::replace(&mut self.builder, PlanBuilder::new());
        self.optimizer = PlanOptimizer::with_builder(old_builder);
        let optimized_plan = self.optimizer.optimize(initial_plan)?;

        // Restore builder from optimizer
        let old_optimizer = std::mem::replace(&mut self.optimizer, PlanOptimizer::new());
        self.builder = old_optimizer.into_builder();

        Ok(optimized_plan)
    }

    /// Explain a query plan
    pub fn explain(&self, plan: &PlanNode) -> PlanExplanation {
        PlanExplanation::new(plan)
    }

    /// Optimize an existing plan
    pub fn optimize(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        self.optimizer.optimize(plan)
    }
}

#[cfg(test)]
mod tests;
