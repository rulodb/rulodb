use crate::ast::*;
use crate::planner::cache::PlanCache;
use crate::planner::error::{PlanError, PlanResult};
use crate::planner::node::{GET_COST, PlanNode, TABLE_SCAN_COST};

/// Builder for constructing query plans from AST nodes
pub struct PlanBuilder {
    cursor_context: Option<Cursor>,
    cache: PlanCache,
}

impl PlanBuilder {
    /// Create a new plan builder
    pub fn new() -> Self {
        Self {
            cursor_context: None,
            cache: PlanCache::new(),
        }
    }

    /// Create a new plan builder with cursor context
    pub fn with_cursor(cursor: Option<Cursor>) -> Self {
        Self {
            cursor_context: cursor,
            cache: PlanCache::new(),
        }
    }

    /// Build a plan from a query
    pub fn build(&mut self, query: &Query) -> PlanResult<PlanNode> {
        self.build_query_internal(query)
    }

    /// Internal method to build a plan from a query
    fn build_query_internal(&mut self, query: &Query) -> PlanResult<PlanNode> {
        match &query.kind {
            // Data Manipulation
            Some(query::Kind::Insert(insert_query)) => self.build_insert_query(insert_query),
            Some(query::Kind::Update(update_query)) => self.build_update_query(update_query),
            Some(query::Kind::Delete(delete_query)) => self.build_delete_query(delete_query),

            // Querying & Data Retrieval
            Some(query::Kind::Table(table_query)) => self.build_table_query(table_query),
            Some(query::Kind::Get(get_query)) => self.build_get_query(get_query),
            Some(query::Kind::GetAll(get_all_query)) => self.build_get_all_query(get_all_query),
            Some(query::Kind::Filter(filter_query)) => self.build_filter_query(filter_query),

            // Transformations
            Some(query::Kind::OrderBy(order_by_query)) => self.build_order_by_query(order_by_query),
            Some(query::Kind::Limit(limit_query)) => self.build_limit_query(limit_query),
            Some(query::Kind::Skip(skip_query)) => self.build_skip_query(skip_query),

            // Aggregation & Grouping
            Some(query::Kind::Count(count_query)) => self.build_count_query(count_query),

            // Document Manipulation
            Some(query::Kind::Pluck(pluck_query)) => self.build_pluck_query(pluck_query),

            // Schema & Data Modeling
            Some(query::Kind::DatabaseCreate(create_db)) => Ok(PlanNode::CreateDatabase {
                name: create_db.name.clone(),
                cost: 1.0,
            }),
            Some(query::Kind::DatabaseDrop(drop_db)) => Ok(PlanNode::DropDatabase {
                name: drop_db.name.clone(),
                cost: 1.0,
            }),
            Some(query::Kind::DatabaseList(_list_db)) => Ok(PlanNode::ListDatabases {
                cursor: self.cursor_context.clone(),
                cost: 1.0,
            }),
            Some(query::Kind::TableCreate(create_table)) => Ok(PlanNode::CreateTable {
                table_ref: create_table
                    .table
                    .clone()
                    .ok_or(PlanError::MissingTableReference)?,
                cost: 1.0,
            }),
            Some(query::Kind::TableDrop(drop_table)) => Ok(PlanNode::DropTable {
                table_ref: drop_table
                    .table
                    .clone()
                    .ok_or(PlanError::MissingTableReference)?,
                cost: 1.0,
            }),
            Some(query::Kind::TableList(list_tables)) => Ok(PlanNode::ListTables {
                database_ref: list_tables.database.clone().unwrap_or(DatabaseRef {
                    name: "default".to_string(),
                }),
                cursor: self.cursor_context.clone(),
                cost: 1.0,
            }),

            // Control & Execution
            Some(query::Kind::Expression(expr)) => self.build_expression_plan(expr),
            Some(query::Kind::Subquery(subquery)) => {
                let subquery_plan = self.build_query_internal(subquery.query.as_ref().ok_or(
                    PlanError::InvalidExpression("Subquery missing query".to_string()),
                )?)?;
                Ok(PlanNode::Subquery {
                    cost: subquery_plan.cost(),
                    query: Box::new(subquery_plan),
                })
            }

            None => Err(PlanError::UnsupportedOperation("Empty query".to_string())),
        }
    }

    /// Build a plan for an expression
    fn build_expression_plan(&mut self, expr: &Expression) -> PlanResult<PlanNode> {
        // Check if this is a constant expression
        if self.is_constant_expression(expr) {
            let value = self.evaluate_constant_expression(expr)?;
            Ok(PlanNode::Constant { value, cost: 0.0 })
        } else {
            match &expr.expr {
                Some(expression::Expr::Subquery(subquery)) => {
                    let subquery_plan = self.build_query_internal(subquery)?;
                    Ok(PlanNode::Subquery {
                        cost: subquery_plan.cost(),
                        query: Box::new(subquery_plan),
                    })
                }
                _ => Err(PlanError::InvalidExpression(
                    "Non-constant expressions require a source".to_string(),
                )),
            }
        }
    }

    /// Build a plan for a table query
    fn build_table_query(&mut self, table_query: &Table) -> PlanResult<PlanNode> {
        let table_ref = table_query
            .table
            .clone()
            .ok_or(PlanError::MissingTableReference)?;
        let estimated_rows = 1000.0; // Default estimate
        Ok(PlanNode::TableScan {
            table_ref,
            cursor: self.cursor_context.clone(),
            filter: None,
            cost: TABLE_SCAN_COST,
            estimated_rows,
        })
    }

    /// Build a plan for a get query
    fn build_get_query(&mut self, get_query: &Get) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(get_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Get missing source".to_string()),
        )?)?;

        if let PlanNode::TableScan { table_ref, .. } = source_plan {
            let key = get_query
                .key
                .as_ref()
                .ok_or(PlanError::InvalidExpression("Get missing key".to_string()))?;

            // Convert the Datum key to string
            let key_str = match &key.value {
                Some(datum::Value::String(s)) => s.clone(),
                Some(datum::Value::Int(i)) => i.to_string(),
                _ => return Err(PlanError::InvalidExpression("Invalid key type".to_string())),
            };

            Ok(PlanNode::Get {
                table_ref,
                key: key_str,
                cost: GET_COST,
            })
        } else {
            Err(PlanError::InvalidExpression(
                "Get requires table source".to_string(),
            ))
        }
    }

    /// Build a plan for the get all query
    fn build_get_all_query(&mut self, get_all_query: &GetAll) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(get_all_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("GetAll missing source".to_string()),
        )?)?;

        if let PlanNode::TableScan { table_ref, .. } = source_plan {
            // Convert Datum keys to strings
            let keys: Result<Vec<String>, PlanError> = get_all_query
                .keys
                .iter()
                .map(|key| match &key.value {
                    Some(datum::Value::String(s)) => Ok(s.clone()),
                    Some(datum::Value::Int(i)) => Ok(i.to_string()),
                    _ => Err(PlanError::InvalidExpression("Invalid key type".to_string())),
                })
                .collect();

            let keys = keys?;
            let cost = GET_COST * keys.len() as f64;

            Ok(PlanNode::GetAll {
                table_ref,
                keys,
                cursor: self.cursor_context.clone(),
                cost,
            })
        } else {
            Err(PlanError::InvalidExpression(
                "GetAll requires table source".to_string(),
            ))
        }
    }

    /// Build a plan for an insert query
    fn build_insert_query(&mut self, insert_query: &Insert) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(insert_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Insert missing source".to_string()),
        )?)?;

        if let PlanNode::TableScan { table_ref, .. } = source_plan {
            let cost = 1.0 * insert_query.documents.len() as f64;
            Ok(PlanNode::Insert {
                table_ref,
                documents: insert_query.documents.clone(),
                cost,
            })
        } else {
            Err(PlanError::InvalidExpression(
                "Insert requires table source".to_string(),
            ))
        }
    }

    /// Build a plan for an update query
    fn build_update_query(&mut self, update_query: &Update) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(update_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Update missing source".to_string()),
        )?)?;
        let patch = update_query
            .patch
            .clone()
            .ok_or(PlanError::InvalidExpression(
                "Update missing patch".to_string(),
            ))?;
        let cost = source_plan.cost() + source_plan.estimated_rows() * 0.5;
        Ok(PlanNode::Update {
            source: Box::new(source_plan),
            patch,
            cost,
        })
    }

    /// Build a plan for a delete query
    fn build_delete_query(&mut self, delete_query: &Delete) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(delete_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Delete missing source".to_string()),
        )?)?;
        let cost = source_plan.cost() + source_plan.estimated_rows() * 0.3;
        Ok(PlanNode::Delete {
            source: Box::new(source_plan),
            cost,
        })
    }

    /// Build a plan for a filter query
    fn build_filter_query(&mut self, filter_query: &Filter) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(filter_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Filter missing source".to_string()),
        )?)?;
        let predicate = filter_query
            .predicate
            .clone()
            .ok_or(PlanError::InvalidExpression(
                "Filter missing predicate".to_string(),
            ))?;
        let selectivity = self.estimate_selectivity(&predicate);
        let cost = source_plan.cost() + source_plan.estimated_rows() * 0.1;
        Ok(PlanNode::Filter {
            source: Box::new(source_plan),
            predicate: *predicate,
            cost,
            selectivity,
        })
    }

    /// Build a plan for an order by query
    fn build_order_by_query(&mut self, order_by_query: &OrderBy) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(order_by_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("OrderBy missing source".to_string()),
        )?)?;
        let n = source_plan.estimated_rows();
        let cost = source_plan.cost() + n * n.log2().max(1.0) * 0.01;

        // Convert SortField to OrderByField
        let fields = order_by_query
            .fields
            .iter()
            .map(|f| OrderByField {
                field_name: f.field_name.clone(),
                ascending: f.direction == SortDirection::Asc as i32,
            })
            .collect();

        Ok(PlanNode::OrderBy {
            source: Box::new(source_plan),
            fields,
            cost,
        })
    }

    /// Build a plan for a limit query
    fn build_limit_query(&mut self, limit_query: &Limit) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(limit_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Limit missing source".to_string()),
        )?)?;
        let cost = source_plan.cost();
        Ok(PlanNode::Limit {
            source: Box::new(source_plan),
            count: limit_query.count,
            cost,
        })
    }

    /// Build a plan for a skip query
    fn build_skip_query(&mut self, skip_query: &Skip) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(skip_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Skip missing source".to_string()),
        )?)?;
        let cost = source_plan.cost();
        Ok(PlanNode::Skip {
            source: Box::new(source_plan),
            count: skip_query.count,
            cost,
        })
    }

    /// Build a plan for a count query
    fn build_count_query(&mut self, count_query: &Count) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(count_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Count missing source".to_string()),
        )?)?;
        let cost = source_plan.cost();
        Ok(PlanNode::Count {
            source: Box::new(source_plan),
            cost,
        })
    }

    /// Build a plan for a pluck query
    fn build_pluck_query(&mut self, pluck_query: &Pluck) -> PlanResult<PlanNode> {
        let source_plan = self.build_query_internal(pluck_query.source.as_ref().ok_or(
            PlanError::InvalidExpression("Pluck missing source".to_string()),
        )?)?;
        let cost = source_plan.cost();
        Ok(PlanNode::Pluck {
            source: Box::new(source_plan),
            fields: pluck_query.fields.clone(),
            cost,
        })
    }

    /// Check if an expression is constant
    fn is_constant_expression(&mut self, expr: &Expression) -> bool {
        // Check cache first
        if let Some(is_constant) = self.cache.is_constant_cached(expr) {
            return is_constant;
        }

        let is_constant = match &expr.expr {
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

        // Cache the result
        self.cache.cache_constant_check(expr.clone(), is_constant);
        is_constant
    }

    /// Evaluate a constant expression
    fn evaluate_constant_expression(&mut self, expr: &Expression) -> PlanResult<Datum> {
        // Check cache first
        if let Some(cached_result) = self.cache.get_expr(expr) {
            return Ok(cached_result.clone());
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

        // Cache the result
        self.cache.cache_expr(expr.clone(), result.clone());
        Ok(result)
    }

    /// Evaluate a binary operation on constants
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

    /// Evaluate the unary operation on constants
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

    /// Estimate the selectivity of a predicate (0.0 to 1.0)
    #[allow(clippy::only_used_in_recursion)]
    fn estimate_selectivity(&self, expr: &Expression) -> f64 {
        match &expr.expr {
            Some(expression::Expr::Literal(lit)) => match &lit.value {
                Some(datum::Value::Bool(true)) => 1.0,
                Some(datum::Value::Bool(false)) => 0.0,
                _ => 0.5,
            },
            Some(expression::Expr::Binary(bin)) => {
                let op = binary_op::Operator::try_from(bin.op).ok();
                match op {
                    Some(binary_op::Operator::Eq) => 0.1,
                    Some(binary_op::Operator::Ne) => 0.9,
                    Some(binary_op::Operator::Lt)
                    | Some(binary_op::Operator::Le)
                    | Some(binary_op::Operator::Gt)
                    | Some(binary_op::Operator::Ge) => 0.3,
                    Some(binary_op::Operator::And) => {
                        let left_sel = bin
                            .left
                            .as_ref()
                            .map(|l| self.estimate_selectivity(l))
                            .unwrap_or(0.5);
                        let right_sel = bin
                            .right
                            .as_ref()
                            .map(|r| self.estimate_selectivity(r))
                            .unwrap_or(0.5);
                        left_sel * right_sel
                    }
                    Some(binary_op::Operator::Or) => {
                        let left_sel = bin
                            .left
                            .as_ref()
                            .map(|l| self.estimate_selectivity(l))
                            .unwrap_or(0.5);
                        let right_sel = bin
                            .right
                            .as_ref()
                            .map(|r| self.estimate_selectivity(r))
                            .unwrap_or(0.5);
                        left_sel + right_sel - (left_sel * right_sel)
                    }
                    _ => 0.5,
                }
            }
            Some(expression::Expr::Unary(un)) => {
                let op = unary_op::Operator::try_from(un.op).ok();
                match op {
                    Some(unary_op::Operator::Not) => un
                        .expr
                        .as_ref()
                        .map(|e| 1.0 - self.estimate_selectivity(e))
                        .unwrap_or(0.5),
                    _ => 0.5,
                }
            }
            _ => 0.5,
        }
    }

    /// Get the internal cache
    pub fn cache(&self) -> &PlanCache {
        &self.cache
    }

    /// Get mutable access to the internal cache
    pub fn cache_mut(&mut self) -> &mut PlanCache {
        &mut self.cache
    }
}
