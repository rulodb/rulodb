mod cursor;
mod database;
mod error;
mod expression;
mod query;
mod table;
mod utils;

#[cfg(test)]
mod tests;

use crate::ast::*;
use crate::planner::PlanNode;
use crate::storage::{DEFAULT_DATABASE, StorageBackend};
use std::sync::Arc;
use std::time::Instant;

// Re-export commonly used types for backward compatibility
pub use error::{EvalError, EvalResult, EvalStats};

/// Main evaluator that orchestrates query execution using specialized processors
pub struct Evaluator {
    database_ops: database::DatabaseOperations,
    table_ops: table::TableOperations,
    expression_eval: expression::ExpressionEvaluator,
    query_processor: query::QueryProcessor,
    stats: EvalStats,
    cursor_context: Option<Cursor>,
    skip_context: Option<u32>,
    limit_context: Option<u32>,
}

impl Evaluator {
    /// Create a new evaluator with the given storage backend
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            database_ops: database::DatabaseOperations::new(storage.clone()),
            table_ops: table::TableOperations::new(storage.clone()),
            expression_eval: expression::ExpressionEvaluator::new(),
            query_processor: query::QueryProcessor::new(storage),
            stats: EvalStats::new(),
            cursor_context: None,
            skip_context: None,
            limit_context: None,
        }
    }

    /// Evaluate a query plan and return the result with statistics
    pub async fn eval(&mut self, plan: &PlanNode) -> Result<EvalResult, EvalError> {
        let start = Instant::now();
        self.stats = EvalStats::new();
        self.skip_context = None;
        self.limit_context = None;

        let result = self.execute_plan(plan).await?;
        self.stats.record_duration(start.elapsed());

        Ok(EvalResult::new(result, self.stats.clone()))
    }

    /// Evaluate a query with cursor context and return the result with statistics
    pub async fn eval_with_cursor(
        &mut self,
        plan: &PlanNode,
        cursor: Option<Cursor>,
    ) -> Result<EvalResult, EvalError> {
        let start = Instant::now();
        self.stats = EvalStats::new();
        self.cursor_context = cursor;
        self.skip_context = None;
        self.limit_context = None;

        let result = self.execute_plan(plan).await?;
        self.stats.record_duration(start.elapsed());

        Ok(EvalResult::new(result, self.stats.clone()))
    }

    /// Execute a plan node recursively
    async fn execute_plan(&mut self, plan: &PlanNode) -> Result<query_result::Result, EvalError> {
        match plan {
            // Constant values
            PlanNode::Constant { value, .. } => Ok(query_result::Result::Literal(LiteralResult {
                value: if value.value.is_some() {
                    Some(value.clone())
                } else {
                    None
                },
            })),

            // Database operations
            PlanNode::CreateDatabase { name, .. } => {
                self.database_ops
                    .create_database(name, &mut self.stats)
                    .await
            }
            PlanNode::DropDatabase { name, .. } => {
                self.database_ops.drop_database(name, &mut self.stats).await
            }
            PlanNode::ListDatabases { cursor, .. } => {
                self.database_ops
                    .list_databases(cursor.clone(), &mut self.stats)
                    .await
            }

            // Table operations
            PlanNode::CreateTable { table_ref, .. } => {
                let database = self.extract_database_name(table_ref);
                self.table_ops
                    .create_table(&database, &table_ref.name, &mut self.stats)
                    .await
            }
            PlanNode::DropTable { table_ref, .. } => {
                let database = self.extract_database_name(table_ref);
                self.table_ops
                    .drop_table(&database, &table_ref.name, &mut self.stats)
                    .await
            }
            PlanNode::ListTables {
                database_ref,
                cursor,
                ..
            } => {
                self.table_ops
                    .list_tables(&database_ref.name, cursor.clone(), &mut self.stats)
                    .await
            }
            PlanNode::TableScan {
                table_ref,
                cursor,
                filter,
                ..
            } => {
                let database = self.extract_database_name(table_ref);

                // Create predicate if filter is provided
                let predicate = filter.as_ref().map(|filter_expr| {
                    let filter_clone = filter_expr.clone();
                    Box::new(move |doc: Document| -> bool {
                        let evaluator = expression::ExpressionEvaluator::new();
                        match evaluator.evaluate_expression(&filter_clone, &Datum::from(doc)) {
                            Ok(d) => matches!(d.value, Some(datum::Value::Bool(true))),
                            Err(err) => {
                                log::debug!("Error evaluating filter: {err}");
                                false
                            }
                        }
                    }) as Predicate
                });

                // Determine effective cursor with proper limit handling
                let effective_cursor = self.combine_cursor_with_context(cursor.clone());

                // Only apply skip on the initial query, not on cursor continuations
                let is_continuation = self
                    .cursor_context
                    .as_ref()
                    .and_then(|c| c.start_key.as_ref())
                    .is_some();

                let skip_count = if is_continuation {
                    None
                } else {
                    self.skip_context.map(|s| s as usize)
                };

                self.table_ops
                    .scan_table(
                        &database,
                        &table_ref.name,
                        effective_cursor,
                        predicate,
                        skip_count,
                        &mut self.stats,
                    )
                    .await
            }

            // Document operations
            PlanNode::Get { table_ref, key, .. } => {
                let database = self.extract_database_name(table_ref);
                self.table_ops
                    .get_document(&database, &table_ref.name, key, &mut self.stats)
                    .await
            }
            PlanNode::GetAll {
                table_ref,
                keys,
                cursor,
                ..
            } => {
                let database = self.extract_database_name(table_ref);
                // Determine effective cursor with proper limit handling
                let effective_cursor = self.combine_cursor_with_context(cursor.clone());

                // Only apply skip on the initial query, not on cursor continuations
                let is_continuation = self
                    .cursor_context
                    .as_ref()
                    .and_then(|c| c.start_key.as_ref())
                    .is_some();

                let skip_count = if is_continuation {
                    None
                } else {
                    self.skip_context.map(|s| s as usize)
                };

                self.table_ops
                    .get_documents(
                        &database,
                        &table_ref.name,
                        keys,
                        effective_cursor,
                        skip_count,
                        &mut self.stats,
                    )
                    .await
            }
            PlanNode::Insert {
                table_ref,
                documents,
                ..
            } => {
                let database = self.extract_database_name(table_ref);
                self.table_ops
                    .insert_documents(&database, &table_ref.name, documents, &mut self.stats)
                    .await
            }

            // Query processing operations
            PlanNode::Update { source, patch, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .update_documents(source_result, patch, source, &mut self.stats)
                    .await
            }
            PlanNode::Delete { source, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .delete_documents(source_result, &mut self.stats)
                    .await
            }
            PlanNode::Filter {
                source, predicate, ..
            } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .filter_documents(
                        source_result,
                        predicate,
                        self.cursor_context.clone(),
                        &mut self.stats,
                    )
                    .await
            }
            PlanNode::OrderBy { source, fields, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .order_documents(
                        source_result,
                        fields,
                        self.cursor_context.clone(),
                        &mut self.stats,
                    )
                    .await
            }
            PlanNode::Limit { source, count, .. } => {
                // Check if we can push the limit down to source
                if self.can_push_down_to_source(source) {
                    self.limit_context = Some(*count);
                    let source_result = Box::pin(self.execute_plan(source)).await?;

                    // Even when pushed down, we need to wrap the result in a LimitResult
                    // to maintain consistent API behavior
                    self.query_processor
                        .apply_limit(
                            source_result,
                            u32::MAX, // We already limited at the storage layer
                            self.cursor_context.clone(),
                            &mut self.stats,
                        )
                        .await
                } else {
                    // Execute the source first, then apply limit
                    let source_result = Box::pin(self.execute_plan(source)).await?;
                    self.query_processor
                        .apply_limit(
                            source_result,
                            *count,
                            self.cursor_context.clone(),
                            &mut self.stats,
                        )
                        .await
                }
            }
            PlanNode::Skip { source, count, .. } => {
                // If we have a cursor context with a start_key, this is a continuation query
                // Skip should only be applied on the initial query, not on continuations
                let is_continuation = self
                    .cursor_context
                    .as_ref()
                    .and_then(|c| c.start_key.as_ref())
                    .is_some();

                let skip_count = if is_continuation { 0 } else { *count };

                if self.can_push_down_to_source(source) && skip_count > 0 {
                    self.skip_context = Some(skip_count);
                    let source_result = Box::pin(self.execute_plan(source)).await?;

                    // Even when pushed down, we need to wrap the result in a SkipResult
                    // to maintain consistent API behavior
                    self.query_processor
                        .apply_skip(
                            source_result,
                            0, // We already skipped at the storage layer, so skip 0 here
                            self.cursor_context.clone(),
                            &mut self.stats,
                        )
                        .await
                } else {
                    // Execute the source first, then apply skip
                    let source_result = Box::pin(self.execute_plan(source)).await?;
                    self.query_processor
                        .apply_skip(
                            source_result,
                            skip_count,
                            self.cursor_context.clone(),
                            &mut self.stats,
                        )
                        .await
                }
            }
            PlanNode::Count { source, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .count_documents(source_result, &mut self.stats)
                    .await
            }
            PlanNode::Pluck { source, fields, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .pluck_documents_streaming(
                        source_result,
                        self.cursor_context.clone(),
                        fields,
                        &mut self.stats,
                    )
                    .await
            }

            PlanNode::Without { source, fields, .. } => {
                let source_result = Box::pin(self.execute_plan(source)).await?;
                self.query_processor
                    .without_documents_streaming(
                        source_result,
                        self.cursor_context.clone(),
                        fields,
                        &mut self.stats,
                    )
                    .await
            }

            // Subqueries
            PlanNode::Subquery { query, .. } => Box::pin(self.execute_plan(query)).await,
        }
    }

    /// Extract database name from table reference, using default if not specified
    fn extract_database_name(&self, table_ref: &TableRef) -> String {
        table_ref
            .database
            .as_ref()
            .map(|d| d.name.clone())
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string())
    }

    /// Get current evaluation statistics
    pub fn get_stats(&self) -> &EvalStats {
        &self.stats
    }

    /// Reset evaluation statistics
    pub fn reset_stats(&mut self) {
        self.stats = EvalStats::new();
    }

    /// Check if we can push skip/limit down to the source operation
    fn can_push_down_to_source(&self, source: &PlanNode) -> bool {
        matches!(source, PlanNode::TableScan { .. } | PlanNode::GetAll { .. })
    }

    /// Combine cursor context with skip/limit context
    fn combine_cursor_with_context(&self, plan_cursor: Option<Cursor>) -> Option<Cursor> {
        let base_cursor = self.cursor_context.clone().or(plan_cursor);

        match (base_cursor, self.limit_context) {
            (Some(mut cursor), Some(limit)) => {
                // Use the minimum of limit and batch_size
                let effective_limit = match cursor.batch_size {
                    Some(batch_size) => std::cmp::min(batch_size, limit),
                    None => limit,
                };
                cursor.batch_size = Some(effective_limit);
                Some(cursor)
            }
            (None, Some(limit)) => Some(Cursor::new(None, Some(limit))),
            (cursor, None) => cursor,
        }
    }

    /// Evaluate an expression in a given context
    pub fn evaluate_expression(
        &self,
        expr: &Expression,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        self.expression_eval.evaluate_expression(expr, context)
    }
}
