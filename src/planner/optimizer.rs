use crate::ast::*;
use crate::planner::builder::PlanBuilder;
use crate::planner::error::{PlanError, PlanResult};
use crate::planner::node::{FILTER_COST, PlanNode, TABLE_SCAN_COST};

/// Optimizer for query plans
pub struct PlanOptimizer {
    builder: PlanBuilder,
}

impl PlanOptimizer {
    /// Create a new optimizer
    pub fn new() -> Self {
        Self {
            builder: PlanBuilder::new(),
        }
    }

    /// Create a new optimizer with a plan builder
    pub fn with_builder(builder: PlanBuilder) -> Self {
        Self { builder }
    }

    /// Consume the optimizer and return the internal builder
    pub fn into_builder(self) -> PlanBuilder {
        self.builder
    }

    /// Optimize a query plan
    pub fn optimize(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        let mut optimized = plan;

        // Apply optimizations in order
        optimized = self.optimize_constants(optimized)?;
        optimized = self.optimize_predicates(optimized)?;
        optimized = self.merge_adjacent_operations(optimized)?;
        optimized = self.optimize_costs(optimized)?;

        Ok(optimized)
    }

    /// Optimize constant expressions in the plan
    pub fn optimize_constants(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        match plan {
            PlanNode::Filter {
                source,
                predicate,
                cost,
                selectivity,
            } => {
                // Fold constants in the predicate
                let folded_predicate = self.fold_constants(predicate)?;

                // Check if the predicate is now a constant
                if let Some(expression::Expr::Literal(lit)) = &folded_predicate.expr {
                    match &lit.value {
                        Some(datum::Value::Bool(true)) => {
                            // Filter is always true, remove it
                            self.optimize_constants(*source)
                        }
                        Some(datum::Value::Bool(false)) => {
                            // Filter is always false, return empty result
                            Ok(PlanNode::Constant {
                                value: Datum {
                                    value: Some(datum::Value::Array(DatumArray {
                                        items: vec![],
                                        element_type: String::new(),
                                    })),
                                },
                                cost: 0.0,
                            })
                        }
                        _ => {
                            // Keep the filter with folded predicate
                            let optimized_source = self.optimize_constants(*source)?;
                            Ok(PlanNode::Filter {
                                source: Box::new(optimized_source),
                                predicate: folded_predicate,
                                cost,
                                selectivity,
                            })
                        }
                    }
                } else {
                    // Recursively optimize the source
                    let optimized_source = self.optimize_constants(*source)?;
                    Ok(PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate: folded_predicate,
                        cost,
                        selectivity,
                    })
                }
            }

            // Recursively optimize child nodes
            PlanNode::Update {
                source,
                patch,
                cost,
            } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::Update {
                    source: Box::new(optimized_source),
                    patch,
                    cost,
                })
            }
            PlanNode::Delete { source, cost } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::Delete {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::OrderBy {
                source,
                fields,
                cost,
            } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::OrderBy {
                    source: Box::new(optimized_source),
                    fields,
                    cost,
                })
            }
            PlanNode::Limit {
                source,
                count,
                cost,
            } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::Limit {
                    source: Box::new(optimized_source),
                    count,
                    cost,
                })
            }
            PlanNode::Skip {
                source,
                count,
                cost,
            } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::Skip {
                    source: Box::new(optimized_source),
                    count,
                    cost,
                })
            }
            PlanNode::Count { source, cost } => {
                let optimized_source = self.optimize_constants(*source)?;
                Ok(PlanNode::Count {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::Subquery { query, cost } => {
                let optimized_query = self.optimize_constants(*query)?;
                Ok(PlanNode::Subquery {
                    query: Box::new(optimized_query),
                    cost,
                })
            }

            // Base cases - no optimization needed
            _ => Ok(plan),
        }
    }

    /// Optimize predicates by pushing them down and merging them
    pub fn optimize_predicates(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        match plan {
            // Push filter down through other operations
            PlanNode::Filter {
                source,
                predicate,
                cost,
                selectivity,
            } => match *source {
                // Push filter into table scan
                PlanNode::TableScan {
                    table_ref,
                    cursor,
                    filter: existing_filter,
                    cost: scan_cost,
                    estimated_rows,
                } => {
                    let combined_filter = if let Some(existing) = existing_filter {
                        self.combine_predicates(existing, predicate)
                    } else {
                        predicate
                    };

                    Ok(PlanNode::TableScan {
                        table_ref,
                        cursor,
                        filter: Some(combined_filter),
                        cost: scan_cost + FILTER_COST,
                        estimated_rows: estimated_rows * selectivity,
                    })
                }

                // Push filter through limit
                PlanNode::Limit {
                    source: limit_source,
                    count,
                    cost: limit_cost,
                } => {
                    let filtered_source = self.optimize_predicates(PlanNode::Filter {
                        source: limit_source,
                        predicate,
                        cost,
                        selectivity,
                    })?;
                    Ok(PlanNode::Limit {
                        source: Box::new(filtered_source),
                        count,
                        cost: limit_cost,
                    })
                }

                // Push filter through skip
                PlanNode::Skip {
                    source: skip_source,
                    count,
                    cost: skip_cost,
                } => {
                    let filtered_source = self.optimize_predicates(PlanNode::Filter {
                        source: skip_source,
                        predicate,
                        cost,
                        selectivity,
                    })?;
                    Ok(PlanNode::Skip {
                        source: Box::new(filtered_source),
                        count,
                        cost: skip_cost,
                    })
                }

                // Can't push further, optimize the source
                _ => {
                    let optimized_source = self.optimize_predicates(*source)?;
                    Ok(PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate,
                        cost,
                        selectivity,
                    })
                }
            },

            // Recursively optimize child nodes
            PlanNode::Update {
                source,
                patch,
                cost,
            } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::Update {
                    source: Box::new(optimized_source),
                    patch,
                    cost,
                })
            }
            PlanNode::Delete { source, cost } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::Delete {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::OrderBy {
                source,
                fields,
                cost,
            } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::OrderBy {
                    source: Box::new(optimized_source),
                    fields,
                    cost,
                })
            }
            PlanNode::Limit {
                source,
                count,
                cost,
            } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::Limit {
                    source: Box::new(optimized_source),
                    count,
                    cost,
                })
            }
            PlanNode::Skip {
                source,
                count,
                cost,
            } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::Skip {
                    source: Box::new(optimized_source),
                    count,
                    cost,
                })
            }
            PlanNode::Count { source, cost } => {
                let optimized_source = self.optimize_predicates(*source)?;
                Ok(PlanNode::Count {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::Subquery { query, cost } => {
                let optimized_query = self.optimize_predicates(*query)?;
                Ok(PlanNode::Subquery {
                    query: Box::new(optimized_query),
                    cost,
                })
            }

            // Base cases
            _ => Ok(plan),
        }
    }

    /// Merge adjacent operations that can be combined
    pub fn merge_adjacent_operations(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        match plan {
            // Merge adjacent filters
            PlanNode::Filter {
                source,
                predicate,
                cost,
                selectivity,
            } => match *source {
                PlanNode::Filter {
                    source: inner_source,
                    predicate: inner_predicate,
                    cost: inner_cost,
                    selectivity: inner_selectivity,
                } => {
                    let combined_predicate = self.combine_predicates(inner_predicate, predicate);
                    let combined_selectivity = selectivity * inner_selectivity;
                    let optimized_source = self.merge_adjacent_operations(*inner_source)?;

                    Ok(PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate: combined_predicate,
                        cost: inner_cost + FILTER_COST,
                        selectivity: combined_selectivity,
                    })
                }
                _ => {
                    let optimized_source = self.merge_adjacent_operations(*source)?;
                    Ok(PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate,
                        cost,
                        selectivity,
                    })
                }
            },

            // Merge adjacent limits (take the minimum)
            PlanNode::Limit {
                source,
                count,
                cost,
            } => match *source {
                PlanNode::Limit {
                    source: inner_source,
                    count: inner_count,
                    cost: inner_cost,
                } => {
                    let min_count = count.min(inner_count);
                    let optimized_source = self.merge_adjacent_operations(*inner_source)?;
                    Ok(PlanNode::Limit {
                        source: Box::new(optimized_source),
                        count: min_count,
                        cost: inner_cost,
                    })
                }
                _ => {
                    let optimized_source = self.merge_adjacent_operations(*source)?;
                    Ok(PlanNode::Limit {
                        source: Box::new(optimized_source),
                        count,
                        cost,
                    })
                }
            },

            // Merge adjacent skips (add them)
            PlanNode::Skip {
                source,
                count,
                cost,
            } => match *source {
                PlanNode::Skip {
                    source: inner_source,
                    count: inner_count,
                    cost: inner_cost,
                } => {
                    let total_count = count.saturating_add(inner_count);
                    let optimized_source = self.merge_adjacent_operations(*inner_source)?;
                    Ok(PlanNode::Skip {
                        source: Box::new(optimized_source),
                        count: total_count,
                        cost: inner_cost,
                    })
                }
                _ => {
                    let optimized_source = self.merge_adjacent_operations(*source)?;
                    Ok(PlanNode::Skip {
                        source: Box::new(optimized_source),
                        count,
                        cost,
                    })
                }
            },

            // Recursively optimize child nodes
            PlanNode::Update {
                source,
                patch,
                cost,
            } => {
                let optimized_source = self.merge_adjacent_operations(*source)?;
                Ok(PlanNode::Update {
                    source: Box::new(optimized_source),
                    patch,
                    cost,
                })
            }
            PlanNode::Delete { source, cost } => {
                let optimized_source = self.merge_adjacent_operations(*source)?;
                Ok(PlanNode::Delete {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::OrderBy {
                source,
                fields,
                cost,
            } => {
                let optimized_source = self.merge_adjacent_operations(*source)?;
                Ok(PlanNode::OrderBy {
                    source: Box::new(optimized_source),
                    fields,
                    cost,
                })
            }
            PlanNode::Count { source, cost } => {
                let optimized_source = self.merge_adjacent_operations(*source)?;
                Ok(PlanNode::Count {
                    source: Box::new(optimized_source),
                    cost,
                })
            }
            PlanNode::Subquery { query, cost } => {
                let optimized_query = self.merge_adjacent_operations(*query)?;
                Ok(PlanNode::Subquery {
                    query: Box::new(optimized_query),
                    cost,
                })
            }

            // Base cases
            _ => Ok(plan),
        }
    }

    /// Optimize costs throughout the plan
    #[allow(clippy::only_used_in_recursion)]
    pub fn optimize_costs(&mut self, plan: PlanNode) -> PlanResult<PlanNode> {
        match plan {
            PlanNode::TableScan {
                table_ref,
                cursor,
                filter,
                estimated_rows,
                ..
            } => {
                let base_cost = TABLE_SCAN_COST;
                let filter_cost = if filter.is_some() {
                    estimated_rows * FILTER_COST
                } else {
                    0.0
                };
                Ok(PlanNode::TableScan {
                    table_ref,
                    cursor,
                    filter,
                    cost: base_cost + filter_cost,
                    estimated_rows,
                })
            }
            PlanNode::Filter {
                source,
                predicate,
                selectivity,
                ..
            } => {
                let optimized_source = self.optimize_costs(*source)?;
                let source_cost = optimized_source.cost();
                let filter_cost = optimized_source.estimated_rows() * FILTER_COST;
                Ok(PlanNode::Filter {
                    source: Box::new(optimized_source),
                    predicate,
                    cost: source_cost + filter_cost,
                    selectivity,
                })
            }
            PlanNode::OrderBy { source, fields, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                let source_cost = optimized_source.cost();
                let n = optimized_source.estimated_rows();
                let sort_cost = n * n.log2().max(1.0) * 0.01;
                Ok(PlanNode::OrderBy {
                    source: Box::new(optimized_source),
                    fields,
                    cost: source_cost + sort_cost,
                })
            }
            PlanNode::Update { source, patch, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                let source_cost = optimized_source.cost();
                let update_cost = optimized_source.estimated_rows() * 0.5;
                Ok(PlanNode::Update {
                    source: Box::new(optimized_source),
                    patch,
                    cost: source_cost + update_cost,
                })
            }
            PlanNode::Delete { source, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                let source_cost = optimized_source.cost();
                let delete_cost = optimized_source.estimated_rows() * 0.3;
                Ok(PlanNode::Delete {
                    source: Box::new(optimized_source),
                    cost: source_cost + delete_cost,
                })
            }

            // Propagate for operations that don't add cost
            PlanNode::Limit { source, count, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                Ok(PlanNode::Limit {
                    cost: optimized_source.cost(),
                    source: Box::new(optimized_source),
                    count,
                })
            }
            PlanNode::Skip { source, count, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                Ok(PlanNode::Skip {
                    cost: optimized_source.cost(),
                    source: Box::new(optimized_source),
                    count,
                })
            }
            PlanNode::Count { source, .. } => {
                let optimized_source = self.optimize_costs(*source)?;
                Ok(PlanNode::Count {
                    cost: optimized_source.cost(),
                    source: Box::new(optimized_source),
                })
            }
            PlanNode::Subquery { query, .. } => {
                let optimized_query = self.optimize_costs(*query)?;
                Ok(PlanNode::Subquery {
                    cost: optimized_query.cost(),
                    query: Box::new(optimized_query),
                })
            }

            // Base cases
            _ => Ok(plan),
        }
    }

    /// Fold constant expressions
    fn fold_constants(&mut self, expr: Expression) -> PlanResult<Expression> {
        match expr.expr {
            Some(expression::Expr::Binary(mut bin)) => {
                // Recursively fold left and right
                if let Some(left) = bin.left.take() {
                    bin.left = Some(Box::new(self.fold_constants(*left)?));
                }
                if let Some(right) = bin.right.take() {
                    bin.right = Some(Box::new(self.fold_constants(*right)?));
                }

                // Check if both sides are constants
                if let (Some(left), Some(right)) = (&bin.left, &bin.right) {
                    if self.is_constant_expression(left) && self.is_constant_expression(right) {
                        let left_val = self.evaluate_constant_expression(left)?;
                        let right_val = self.evaluate_constant_expression(right)?;
                        let op = binary_op::Operator::try_from(bin.op).map_err(|_| {
                            PlanError::InvalidExpression("Invalid binary operator".to_string())
                        })?;
                        let result = self.evaluate_binary_constant(&op, &left_val, &right_val)?;
                        Ok(Expression {
                            expr: Some(expression::Expr::Literal(result)),
                        })
                    } else {
                        Ok(Expression {
                            expr: Some(expression::Expr::Binary(bin)),
                        })
                    }
                } else {
                    Ok(Expression {
                        expr: Some(expression::Expr::Binary(bin)),
                    })
                }
            }
            Some(expression::Expr::Unary(mut un)) => {
                // Recursively fold operand
                if let Some(operand) = un.expr.take() {
                    un.expr = Some(Box::new(self.fold_constants(*operand)?));
                }

                // Check if operand is constant
                if let Some(operand) = &un.expr {
                    if self.is_constant_expression(operand) {
                        let operand_val = self.evaluate_constant_expression(operand)?;
                        let op = unary_op::Operator::try_from(un.op).map_err(|_| {
                            PlanError::InvalidExpression("Invalid unary operator".to_string())
                        })?;
                        let result = self.evaluate_unary_constant(&op, &operand_val)?;
                        Ok(Expression {
                            expr: Some(expression::Expr::Literal(result)),
                        })
                    } else {
                        Ok(Expression {
                            expr: Some(expression::Expr::Unary(un)),
                        })
                    }
                } else {
                    Ok(Expression {
                        expr: Some(expression::Expr::Unary(un)),
                    })
                }
            }
            _ => Ok(expr),
        }
    }

    /// Combine two predicates with AND
    fn combine_predicates(&self, pred1: Expression, pred2: Expression) -> Expression {
        Expression {
            expr: Some(expression::Expr::Binary(Box::new(BinaryOp {
                op: binary_op::Operator::And.into(),
                left: Some(Box::new(pred1)),
                right: Some(Box::new(pred2)),
            }))),
        }
    }

    // Delegate to builder for expression evaluation
    fn is_constant_expression(&mut self, expr: &Expression) -> bool {
        self.builder
            .cache()
            .is_constant_cached(expr)
            .unwrap_or_else(|| {
                let is_const = match &expr.expr {
                    Some(expression::Expr::Literal(_)) => true,
                    Some(expression::Expr::Binary(bin)) => {
                        bin.left
                            .as_ref()
                            .map(|l| self.is_constant_expression(l))
                            .unwrap_or(false)
                            && bin
                                .right
                                .as_ref()
                                .map(|r| self.is_constant_expression(r))
                                .unwrap_or(false)
                    }
                    Some(expression::Expr::Unary(un)) => un
                        .expr
                        .as_ref()
                        .map(|e| self.is_constant_expression(e))
                        .unwrap_or(false),
                    _ => false,
                };
                self.builder
                    .cache_mut()
                    .cache_constant_check(expr.clone(), is_const);
                is_const
            })
    }

    fn evaluate_constant_expression(&mut self, expr: &Expression) -> PlanResult<Datum> {
        if let Some(cached) = self.builder.cache().get_expr(expr) {
            return Ok(cached.clone());
        }

        let result = match &expr.expr {
            Some(expression::Expr::Literal(lit)) => Ok(lit.clone()),
            Some(expression::Expr::Binary(bin)) => {
                let left = bin.left.as_ref().ok_or(PlanError::InvalidExpression(
                    "Binary missing left".to_string(),
                ))?;
                let right = bin.right.as_ref().ok_or(PlanError::InvalidExpression(
                    "Binary missing right".to_string(),
                ))?;
                let left_val = self.evaluate_constant_expression(left)?;
                let right_val = self.evaluate_constant_expression(right)?;
                let op = binary_op::Operator::try_from(bin.op).map_err(|_| {
                    PlanError::InvalidExpression("Invalid binary operator".to_string())
                })?;
                self.evaluate_binary_constant(&op, &left_val, &right_val)
            }
            Some(expression::Expr::Unary(un)) => {
                let operand = un.expr.as_ref().ok_or(PlanError::InvalidExpression(
                    "Unary missing operand".to_string(),
                ))?;
                let operand_val = self.evaluate_constant_expression(operand)?;
                let op = unary_op::Operator::try_from(un.op).map_err(|_| {
                    PlanError::InvalidExpression("Invalid unary operator".to_string())
                })?;
                self.evaluate_unary_constant(&op, &operand_val)
            }
            _ => Err(PlanError::InvalidConstant(
                "Expression is not constant".to_string(),
            )),
        }?;

        self.builder
            .cache_mut()
            .cache_expr(expr.clone(), result.clone());
        Ok(result)
    }

    fn evaluate_binary_constant(
        &self,
        op: &binary_op::Operator,
        left: &Datum,
        right: &Datum,
    ) -> PlanResult<Datum> {
        use datum::Value;

        let result = match (op, &left.value, &right.value) {
            // Comparison operations
            (binary_op::Operator::Eq, Some(a), Some(b)) => Some(Value::Bool(a == b)),
            (binary_op::Operator::Ne, Some(a), Some(b)) => Some(Value::Bool(a != b)),
            (binary_op::Operator::Lt, Some(Value::Int(a)), Some(Value::Int(b))) => {
                Some(Value::Bool(a < b))
            }
            (binary_op::Operator::Le, Some(Value::Int(a)), Some(Value::Int(b))) => {
                Some(Value::Bool(a <= b))
            }
            (binary_op::Operator::Gt, Some(Value::Int(a)), Some(Value::Int(b))) => {
                Some(Value::Bool(a > b))
            }
            (binary_op::Operator::Ge, Some(Value::Int(a)), Some(Value::Int(b))) => {
                Some(Value::Bool(a >= b))
            }

            // Logical operations
            (binary_op::Operator::And, Some(Value::Bool(a)), Some(Value::Bool(b))) => {
                Some(Value::Bool(*a && *b))
            }
            (binary_op::Operator::Or, Some(Value::Bool(a)), Some(Value::Bool(b))) => {
                Some(Value::Bool(*a || *b))
            }

            _ => None,
        };

        Ok(Datum { value: result })
    }

    fn evaluate_unary_constant(
        &self,
        op: &unary_op::Operator,
        operand: &Datum,
    ) -> PlanResult<Datum> {
        use datum::Value;

        let result = match (op, &operand.value) {
            (unary_op::Operator::Not, Some(Value::Bool(b))) => Some(Value::Bool(!b)),
            _ => None,
        };

        Ok(Datum { value: result })
    }
}
