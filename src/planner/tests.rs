use super::*;
use crate::ast::*;
use crate::planner::node::PlanNode;

fn create_test_table_ref() -> TableRef {
    TableRef {
        database: Some(DatabaseRef {
            name: "test_db".to_string(),
        }),
        name: "test_table".to_string(),
    }
}

fn create_test_datum_int(value: i64) -> Datum {
    Datum {
        value: Some(datum::Value::Int(value)),
    }
}

fn create_test_datum_bool(value: bool) -> Datum {
    Datum {
        value: Some(datum::Value::Bool(value)),
    }
}

fn create_test_datum_string(value: &str) -> Datum {
    Datum {
        value: Some(datum::Value::String(value.to_string())),
    }
}

fn create_test_literal_expr(datum: Datum) -> Expression {
    Expression {
        expr: Some(expression::Expr::Literal(datum)),
    }
}

fn create_test_field_expr(field_name: &str) -> Expression {
    Expression {
        expr: Some(expression::Expr::Field(FieldRef {
            path: vec![field_name.to_string()],
            separator: ".".to_string(),
        })),
    }
}

fn create_test_binary_expr(
    left: Expression,
    op: binary_op::Operator,
    right: Expression,
) -> Expression {
    Expression {
        expr: Some(expression::Expr::Binary(Box::new(BinaryOp {
            left: Some(Box::new(left)),
            op: op.into(),
            right: Some(Box::new(right)),
        }))),
    }
}

fn create_test_table_query() -> Query {
    Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Table(Table {
            table: Some(create_test_table_ref()),
        })),
    }
}

#[test]
fn test_plan_node_cost() {
    let node = PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 5.0,
        estimated_rows: 100.0,
    };
    assert_eq!(node.cost(), 5.0);

    let node = PlanNode::Constant {
        value: create_test_datum_int(42),
        cost: 0.0,
    };
    assert_eq!(node.cost(), 0.0);
}

#[test]
fn test_plan_node_estimated_rows() {
    let node = PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };
    assert_eq!(node.estimated_rows(), 100.0);

    let node = PlanNode::Constant {
        value: create_test_datum_int(42),
        cost: 0.0,
    };
    assert_eq!(node.estimated_rows(), 1.0);

    let source = Box::new(PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    });

    let node = PlanNode::Filter {
        source,
        predicate: create_test_literal_expr(create_test_datum_bool(true)),
        cost: 1.1,
        selectivity: 0.5,
    };
    assert_eq!(node.estimated_rows(), 50.0);

    let source = Box::new(PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    });

    let node = PlanNode::Limit {
        source,
        count: 10,
        cost: 1.0,
    };
    assert_eq!(node.estimated_rows(), 10.0);

    let source = Box::new(PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    });

    let node = PlanNode::Skip {
        source,
        count: 50,
        cost: 1.0,
    };
    assert_eq!(node.estimated_rows(), 50.0);
}

#[test]
fn test_plan_error_display() {
    let err = PlanError::UnsupportedOperation("test operation".to_string());
    assert_eq!(err.to_string(), "Unsupported operation: test operation");

    let err = PlanError::InvalidExpression("bad expression".to_string());
    assert_eq!(err.to_string(), "Invalid expression: bad expression");

    let err = PlanError::MissingTableReference;
    assert_eq!(err.to_string(), "Missing table reference");

    let err = PlanError::InvalidConstant("bad constant".to_string());
    assert_eq!(err.to_string(), "Invalid constant: bad constant");

    let err = PlanError::OptimizationFailed("optimization error".to_string());
    assert_eq!(err.to_string(), "Optimization failed: optimization error");
}

#[test]
fn test_plan_error_debug() {
    let err = PlanError::UnsupportedOperation("test".to_string());
    let debug_str = format!("{err:?}");
    assert!(debug_str.contains("UnsupportedOperation"));
}

#[test]
fn test_plan_error_is_error() {
    use std::error::Error;
    let err = PlanError::UnsupportedOperation("test".to_string());
    assert!(err.source().is_none());
}

#[test]
fn test_planner_creation() {
    let planner = Planner::new();
    assert!(planner.cursor_context.is_none());
}

#[test]
fn test_planner_default() {
    let planner = Planner::default();
    assert!(planner.cursor_context.is_none());
}

#[test]
fn test_build_plan_table() {
    let mut planner = Planner::new();
    let query = create_test_table_query();
    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::TableScan {
            table_ref,
            cursor,
            filter,
            cost,
            estimated_rows,
        } => {
            assert_eq!(table_ref.name, "test_table");
            assert!(cursor.is_none());
            assert!(filter.is_none());
            assert_eq!(cost, TABLE_SCAN_COST);
            assert_eq!(estimated_rows, 1000.0);
        }
        _ => panic!("Expected TableScan node"),
    }
}

#[test]
fn test_build_plan_get() {
    let mut planner = Planner::new();
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Get(Box::new(Get {
            source: Some(Box::new(create_test_table_query())),
            key: Some(create_test_datum_string("key123")),
        }))),
    };
    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Get {
            table_ref,
            key,
            cost,
        } => {
            assert_eq!(table_ref.name, "test_table");
            assert_eq!(key, "key123");
            assert_eq!(cost, GET_COST);
        }
        _ => panic!("Expected Get node"),
    }
}

#[test]
fn test_build_plan_get_all() {
    let mut planner = Planner::new();
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::GetAll(Box::new(GetAll {
            source: Some(Box::new(create_test_table_query())),
            keys: vec![
                create_test_datum_string("key1"),
                create_test_datum_string("key2"),
            ],
        }))),
    };
    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::GetAll {
            table_ref,
            keys,
            cursor,
            cost,
        } => {
            assert_eq!(table_ref.name, "test_table");
            assert_eq!(keys.len(), 2);
            assert!(cursor.is_none());
            assert_eq!(cost, GET_COST * 2.0);
        }
        _ => panic!("Expected GetAll node"),
    }
}

#[test]
fn test_build_plan_insert() {
    let mut planner = Planner::new();
    let docs = vec![DatumObject::default()];
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Insert(Box::new(Insert {
            source: Some(Box::new(create_test_table_query())),
            documents: docs.clone(),
        }))),
    };
    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Insert {
            table_ref,
            documents,
            cost,
        } => {
            assert_eq!(table_ref.name, "test_table");
            assert_eq!(documents.len(), 1);
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected Insert node"),
    }
}

#[test]
fn test_build_plan_filter() {
    let mut planner = Planner::new();
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Filter(Box::new(Filter {
            source: Some(Box::new(create_test_table_query())),
            predicate: Some(Box::new(create_test_binary_expr(
                create_test_field_expr("status"),
                binary_op::Operator::Eq,
                create_test_literal_expr(create_test_datum_string("active")),
            ))),
        }))),
    };
    let plan = planner.plan(&query).unwrap();

    // After optimization, filter should be pushed into table scan
    match plan {
        PlanNode::TableScan { filter, .. } => {
            assert!(filter.is_some());
        }
        _ => panic!("Expected TableScan with filter after optimization"),
    }
}

#[test]
fn test_optimize_constants() {
    let mut planner = Planner::new();

    // Create a filter with constant true predicate
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Filter(Box::new(Filter {
            source: Some(Box::new(create_test_table_query())),
            predicate: Some(Box::new(create_test_literal_expr(create_test_datum_bool(
                true,
            )))),
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    // The constant true filter should be optimized away
    match plan {
        PlanNode::TableScan { filter, .. } => {
            // With constant true, the filter is optimized away
            assert!(filter.is_none());
        }
        _ => panic!("Expected TableScan"),
    }
}

#[test]
fn test_plan_explanation_display() {
    let plan = PlanNode::Filter {
        source: Box::new(PlanNode::TableScan {
            table_ref: create_test_table_ref(),
            cursor: None,
            filter: None,
            cost: 1.0,
            estimated_rows: 100.0,
        }),
        predicate: create_test_binary_expr(
            create_test_field_expr("age"),
            binary_op::Operator::Gt,
            create_test_literal_expr(create_test_datum_int(18)),
        ),
        cost: 1.1,
        selectivity: 0.5,
    };

    let explanation = PlanExplanation::new(&plan);
    let display = format!("{explanation}");

    assert!(display.contains("Query Plan:"));
    assert!(display.contains("Total Cost:"));
    assert!(display.contains("Estimated Rows:"));
    assert!(display.contains("Filter"));
    assert!(display.contains("TableScan"));
}

#[test]
fn test_plan_node_partial_eq() {
    let node1 = PlanNode::Constant {
        value: create_test_datum_int(42),
        cost: 0.0,
    };
    let node2 = PlanNode::Constant {
        value: create_test_datum_int(42),
        cost: 1.0, // Different cost
    };
    let node3 = PlanNode::Constant {
        value: create_test_datum_int(43),
        cost: 0.0,
    };

    assert_eq!(node1, node2); // Cost doesn't matter for equality
    assert_ne!(node1, node3); // Different value

    let table1 = PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };
    let table2 = PlanNode::TableScan {
        table_ref: create_test_table_ref(),
        cursor: None,
        filter: None,
        cost: 2.0,
        estimated_rows: 200.0,
    };

    assert_eq!(table1, table2); // Cost and estimated_rows don't matter
}

#[test]
fn test_cursor_context() {
    let mut planner = Planner::new();
    assert!(planner.cursor_context.is_none());

    let cursor = Cursor {
        start_key: Some("start".to_string()),
        batch_size: Some(10),
        sort: None,
    };

    planner.cursor_context = Some(cursor.clone());

    // Plan with cursor context
    let query = create_test_table_query();
    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::TableScan {
            cursor: plan_cursor,
            ..
        } => {
            assert_eq!(plan_cursor, Some(cursor));
        }
        _ => panic!("Expected TableScan"),
    }
}

#[test]
fn test_complex_query_optimization() {
    let mut planner = Planner::new();

    // Create a complex query: Filter(OrderBy(Limit(Skip(TableScan))))
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Filter(Box::new(Filter {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::OrderBy(Box::new(OrderBy {
                    source: Some(Box::new(Query {
                        options: None,
                        cursor: None,
                        kind: Some(query::Kind::Limit(Box::new(Limit {
                            source: Some(Box::new(Query {
                                options: None,
                                cursor: None,
                                kind: Some(query::Kind::Skip(Box::new(Skip {
                                    source: Some(Box::new(create_test_table_query())),
                                    count: 10,
                                }))),
                            })),
                            count: 20,
                        }))),
                    })),
                    fields: vec![SortField {
                        field_name: "id".to_string(),
                        direction: SortDirection::Asc as i32,
                    }],
                }))),
            })),
            predicate: Some(Box::new(create_test_binary_expr(
                create_test_field_expr("status"),
                binary_op::Operator::Eq,
                create_test_literal_expr(create_test_datum_string("active")),
            ))),
        }))),
    };

    let plan = planner.plan(&query).unwrap();
    let explanation = planner.explain(&plan);

    // Verify the plan was optimized
    assert!(!explanation.nodes.is_empty());
    assert!(explanation.total_cost > 0.0);
    assert!(explanation.estimated_rows > 0.0);
}

#[test]
fn test_constant_folding() {
    let mut planner = Planner::new();

    // Create an expression with constant operations: true && false
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(create_test_binary_expr(
            create_test_literal_expr(create_test_datum_bool(true)),
            binary_op::Operator::And,
            create_test_literal_expr(create_test_datum_bool(false)),
        )))),
    };

    let plan = planner.plan(&query).unwrap();

    // The expression should be folded to a constant
    match plan {
        PlanNode::Constant { value, cost } => {
            assert_eq!(cost, 0.0);
            match value.value {
                Some(datum::Value::Bool(v)) => assert!(!v), // true && false = false
                _ => panic!("Expected Bool value"),
            }
        }
        _ => panic!("Expected Constant node after constant folding"),
    }
}

#[test]
fn test_predicate_pushdown() {
    let mut planner = Planner::new();

    // Create nested filters that should be combined and pushed down
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Filter(Box::new(Filter {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Filter(Box::new(Filter {
                    source: Some(Box::new(create_test_table_query())),
                    predicate: Some(Box::new(create_test_binary_expr(
                        create_test_field_expr("age"),
                        binary_op::Operator::Gt,
                        create_test_literal_expr(create_test_datum_int(18)),
                    ))),
                }))),
            })),
            predicate: Some(Box::new(create_test_binary_expr(
                create_test_field_expr("status"),
                binary_op::Operator::Eq,
                create_test_literal_expr(create_test_datum_string("active")),
            ))),
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    // The optimizer pushes one filter down and keeps the other as a Filter node
    match plan {
        PlanNode::Filter {
            source, predicate, ..
        } => {
            // Check that the outer filter is for status
            match &predicate.expr {
                Some(expression::Expr::Binary(bin)) => {
                    assert_eq!(bin.op, binary_op::Operator::Eq as i32);
                }
                _ => panic!("Expected binary predicate"),
            }

            // Check that the inner source is a TableScan with the age filter pushed down
            match source.as_ref() {
                PlanNode::TableScan { filter, .. } => {
                    assert!(filter.is_some());
                    // The inner filter should be the age > 18 condition
                    match &filter.as_ref().unwrap().expr {
                        Some(expression::Expr::Binary(bin)) => {
                            assert_eq!(bin.op, binary_op::Operator::Gt as i32);
                        }
                        _ => panic!("Expected binary predicate in TableScan"),
                    }
                }
                _ => panic!("Expected TableScan as source of Filter"),
            }
        }
        _ => panic!("Expected Filter node with TableScan source"),
    }
}

#[test]
fn test_comparison_operations() {
    let mut planner = Planner::new();

    // Test Lt operation: 5 < 10 = true
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(create_test_binary_expr(
            create_test_literal_expr(create_test_datum_int(5)),
            binary_op::Operator::Lt,
            create_test_literal_expr(create_test_datum_int(10)),
        )))),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::Constant { value, .. } => {
            assert_eq!(value.value, Some(datum::Value::Bool(true)));
        }
        _ => panic!("Expected Constant node"),
    }

    // Test Gt operation: 10 > 5 = true
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(create_test_binary_expr(
            create_test_literal_expr(create_test_datum_int(10)),
            binary_op::Operator::Gt,
            create_test_literal_expr(create_test_datum_int(5)),
        )))),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::Constant { value, .. } => {
            assert_eq!(value.value, Some(datum::Value::Bool(true)));
        }
        _ => panic!("Expected Constant node"),
    }

    // Test Le operation: 5 <= 5 = true
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(create_test_binary_expr(
            create_test_literal_expr(create_test_datum_int(5)),
            binary_op::Operator::Le,
            create_test_literal_expr(create_test_datum_int(5)),
        )))),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::Constant { value, .. } => {
            assert_eq!(value.value, Some(datum::Value::Bool(true)));
        }
        _ => panic!("Expected Constant node"),
    }

    // Test Ge operation: 10 >= 15 = false
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(create_test_binary_expr(
            create_test_literal_expr(create_test_datum_int(10)),
            binary_op::Operator::Ge,
            create_test_literal_expr(create_test_datum_int(15)),
        )))),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::Constant { value, .. } => {
            assert_eq!(value.value, Some(datum::Value::Bool(false)));
        }
        _ => panic!("Expected Constant node"),
    }
}

#[test]
fn test_update_delete_optimization() {
    let mut planner = Planner::new();

    // Test Update with the filtered source
    let update_query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Update(Box::new(Update {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Filter(Box::new(Filter {
                    source: Some(Box::new(create_test_table_query())),
                    predicate: Some(Box::new(create_test_binary_expr(
                        create_test_field_expr("id"),
                        binary_op::Operator::Eq,
                        create_test_literal_expr(create_test_datum_int(42)),
                    ))),
                }))),
            })),
            patch: Some(DatumObject::default()),
        }))),
    };

    let plan = planner.plan(&update_query).unwrap();
    match plan {
        PlanNode::Update { source, .. } => {
            // Filter should be pushed down to TableScan
            match source.as_ref() {
                PlanNode::TableScan { filter, .. } => {
                    assert!(filter.is_some());
                }
                _ => panic!("Expected TableScan with filter"),
            }
        }
        _ => panic!("Expected Update node"),
    }

    // Test Delete with similar structure
    let delete_query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Delete(Box::new(Delete {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Filter(Box::new(Filter {
                    source: Some(Box::new(create_test_table_query())),
                    predicate: Some(Box::new(create_test_binary_expr(
                        create_test_field_expr("status"),
                        binary_op::Operator::Eq,
                        create_test_literal_expr(create_test_datum_string("deleted")),
                    ))),
                }))),
            })),
        }))),
    };

    let plan = planner.plan(&delete_query).unwrap();
    match plan {
        PlanNode::Delete { source, .. } => match source.as_ref() {
            PlanNode::TableScan { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected TableScan with filter"),
        },
        _ => panic!("Expected Delete node"),
    }
}

#[test]
fn test_subquery_planning() {
    let mut planner = Planner::new();

    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::Expression(Box::new(Expression {
            expr: Some(expression::Expr::Subquery(Box::new(
                create_test_table_query(),
            ))),
        }))),
    };

    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::Subquery { query, .. } => match query.as_ref() {
            PlanNode::TableScan { .. } => {}
            _ => panic!("Expected TableScan in subquery"),
        },
        _ => panic!("Expected Subquery node"),
    }
}

#[test]
fn test_database_operations() {
    let mut planner = Planner::new();

    // Test CreateDatabase
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::DatabaseCreate(DatabaseCreate {
            name: "test_db".to_string(),
        })),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::CreateDatabase { name, cost } => {
            assert_eq!(name, "test_db");
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected CreateDatabase node"),
    }

    // Test DropDatabase
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::DatabaseDrop(DatabaseDrop {
            name: "test_db".to_string(),
        })),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::DropDatabase { name, cost } => {
            assert_eq!(name, "test_db");
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected DropDatabase node"),
    }

    // Test ListDatabases
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::DatabaseList(DatabaseList {})),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::ListDatabases { cursor, cost } => {
            assert!(cursor.is_none());
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected ListDatabases node"),
    }
}

#[test]
fn test_table_operations() {
    let mut planner = Planner::new();

    // Test CreateTable
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::TableCreate(TableCreate {
            table: Some(create_test_table_ref()),
        })),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::CreateTable { table_ref, cost } => {
            assert_eq!(table_ref.name, "test_table");
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected CreateTable node"),
    }

    // Test DropTable
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::TableDrop(TableDrop {
            table: Some(create_test_table_ref()),
        })),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::DropTable { table_ref, cost } => {
            assert_eq!(table_ref.name, "test_table");
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected DropTable node"),
    }

    // Test ListTables
    let query = Query {
        options: None,
        cursor: None,
        kind: Some(query::Kind::TableList(TableList {
            database: Some(DatabaseRef {
                name: "test_db".to_string(),
            }),
        })),
    };
    let plan = planner.plan(&query).unwrap();
    match plan {
        PlanNode::ListTables {
            database_ref,
            cursor,
            cost,
        } => {
            assert_eq!(database_ref.name, "test_db");
            assert!(cursor.is_none());
            assert_eq!(cost, 1.0);
        }
        _ => panic!("Expected ListTables node"),
    }
}

#[test]
fn test_build_plan_pluck() {
    let mut planner = Planner::new();

    // Create a pluck query
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Pluck(Box::new(proto::Pluck {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "test_table".to_string(),
                    }),
                })),
            })),
            fields: vec![
                proto::FieldRef {
                    path: vec!["name".to_string()],
                    separator: ".".to_string(),
                },
                proto::FieldRef {
                    path: vec!["age".to_string()],
                    separator: ".".to_string(),
                },
            ],
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Pluck {
            source,
            fields,
            cost,
        } => {
            // Check that the source is a table scan
            match source.as_ref() {
                PlanNode::TableScan { table_ref, .. } => {
                    assert_eq!(table_ref.database.as_ref().unwrap().name, "test_db");
                    assert_eq!(table_ref.name, "test_table");
                }
                _ => panic!("Expected TableScan as source"),
            }

            // Check fields
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].path, vec!["name".to_string()]);
            assert_eq!(fields[1].path, vec!["age".to_string()]);

            // Check cost is inherited from source
            assert_eq!(cost, source.cost());
        }
        _ => panic!("Expected Pluck node"),
    }
}

#[test]
fn test_pluck_missing_source() {
    let mut planner = Planner::new();

    // Create a pluck query without source
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Pluck(Box::new(proto::Pluck {
            source: None,
            fields: vec![proto::FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            }],
        }))),
    };

    let result = planner.plan(&query);
    assert!(result.is_err());

    if let Err(PlanError::InvalidExpression(msg)) = result {
        assert_eq!(msg, "Pluck missing source");
    } else {
        panic!("Expected InvalidExpression error");
    }
}

#[test]
fn test_pluck_estimated_rows() {
    let table_ref = create_test_table_ref();
    let source = PlanNode::TableScan {
        table_ref,
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };

    let pluck_node = PlanNode::Pluck {
        source: Box::new(source),
        fields: vec![proto::FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        }],
        cost: 1.0,
    };

    // Pluck should inherit estimated rows from source
    assert_eq!(pluck_node.estimated_rows(), 100.0);
}

#[test]
fn test_pluck_node_partial_eq() {
    let table_ref = create_test_table_ref();
    let source = PlanNode::TableScan {
        table_ref: table_ref.clone(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };

    let fields = vec![proto::FieldRef {
        path: vec!["name".to_string()],
        separator: ".".to_string(),
    }];

    let pluck1 = PlanNode::Pluck {
        source: Box::new(source.clone()),
        fields: fields.clone(),
        cost: 1.0,
    };

    let pluck2 = PlanNode::Pluck {
        source: Box::new(source.clone()),
        fields: fields.clone(),
        cost: 2.0, // Different cost should still be equal
    };

    let pluck3 = PlanNode::Pluck {
        source: Box::new(source),
        fields: vec![proto::FieldRef {
            path: vec!["age".to_string()],
            separator: ".".to_string(),
        }],
        cost: 1.0,
    };

    assert_eq!(pluck1, pluck2); // Same source and fields
    assert_ne!(pluck1, pluck3); // Different fields
}

#[test]
fn test_build_plan_without() {
    let mut planner = Planner::new();

    // Create a without query
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Without(Box::new(proto::Without {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "test_table".to_string(),
                    }),
                })),
            })),
            fields: vec![
                proto::FieldRef {
                    path: vec!["password".to_string()],
                    separator: ".".to_string(),
                },
                proto::FieldRef {
                    path: vec!["secret".to_string()],
                    separator: ".".to_string(),
                },
            ],
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Without {
            source,
            fields,
            cost,
        } => {
            // Check that the source is a table scan
            match source.as_ref() {
                PlanNode::TableScan { table_ref, .. } => {
                    assert_eq!(table_ref.database.as_ref().unwrap().name, "test_db");
                    assert_eq!(table_ref.name, "test_table");
                }
                _ => panic!("Expected TableScan as source"),
            }

            // Check fields
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].path, vec!["password".to_string()]);
            assert_eq!(fields[1].path, vec!["secret".to_string()]);

            // Check cost is inherited from source
            assert_eq!(cost, source.cost());
        }
        _ => panic!("Expected Without node"),
    }
}

#[test]
fn test_without_missing_source() {
    let mut planner = Planner::new();

    // Create a without query without source
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Without(Box::new(proto::Without {
            source: None,
            fields: vec![proto::FieldRef {
                path: vec!["password".to_string()],
                separator: ".".to_string(),
            }],
        }))),
    };

    let result = planner.plan(&query);
    assert!(result.is_err());

    if let Err(PlanError::InvalidExpression(msg)) = result {
        assert_eq!(msg, "Without missing source");
    } else {
        panic!("Expected InvalidExpression error");
    }
}

#[test]
fn test_without_estimated_rows() {
    let table_ref = create_test_table_ref();
    let source = PlanNode::TableScan {
        table_ref,
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };

    let without_node = PlanNode::Without {
        source: Box::new(source),
        fields: vec![proto::FieldRef {
            path: vec!["password".to_string()],
            separator: ".".to_string(),
        }],
        cost: 1.0,
    };

    // Without should inherit estimated rows from source
    assert_eq!(without_node.estimated_rows(), 100.0);
}

#[test]
fn test_without_node_partial_eq() {
    let table_ref = create_test_table_ref();
    let source = PlanNode::TableScan {
        table_ref: table_ref.clone(),
        cursor: None,
        filter: None,
        cost: 1.0,
        estimated_rows: 100.0,
    };

    let fields = vec![proto::FieldRef {
        path: vec!["password".to_string()],
        separator: ".".to_string(),
    }];

    let without1 = PlanNode::Without {
        source: Box::new(source.clone()),
        fields: fields.clone(),
        cost: 1.0,
    };

    let without2 = PlanNode::Without {
        source: Box::new(source.clone()),
        fields: fields.clone(),
        cost: 2.0, // Different cost should still be equal
    };

    let without3 = PlanNode::Without {
        source: Box::new(source),
        fields: vec![proto::FieldRef {
            path: vec!["secret".to_string()],
            separator: ".".to_string(),
        }],
        cost: 1.0,
    };

    assert_eq!(without1, without2); // Same source and fields
    assert_ne!(without1, without3); // Different fields
}

#[test]
fn test_without_with_nested_fields() {
    let mut planner = Planner::new();

    // Create a without query with nested fields
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Without(Box::new(proto::Without {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "test_table".to_string(),
                    }),
                })),
            })),
            fields: vec![
                proto::FieldRef {
                    path: vec!["user".to_string(), "password".to_string()],
                    separator: ".".to_string(),
                },
                proto::FieldRef {
                    path: vec!["metadata".to_string(), "internal".to_string()],
                    separator: ".".to_string(),
                },
            ],
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Without { fields, .. } => {
            assert_eq!(fields.len(), 2);
            assert_eq!(
                fields[0].path,
                vec!["user".to_string(), "password".to_string()]
            );
            assert_eq!(
                fields[1].path,
                vec!["metadata".to_string(), "internal".to_string()]
            );
        }
        _ => panic!("Expected Without node"),
    }
}

#[test]
fn test_without_with_complex_source() {
    let mut planner = Planner::new();

    // Create a without query with a filter as source
    let query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Without(Box::new(proto::Without {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Filter(Box::new(proto::Filter {
                    source: Some(Box::new(proto::Query {
                        options: None,
                        cursor: None,
                        kind: Some(proto::query::Kind::Table(proto::Table {
                            table: Some(proto::TableRef {
                                database: Some(proto::DatabaseRef {
                                    name: "test_db".to_string(),
                                }),
                                name: "test_table".to_string(),
                            }),
                        })),
                    })),
                    predicate: Some(Box::new(create_test_binary_expr(
                        create_test_field_expr("status"),
                        binary_op::Operator::Eq,
                        create_test_literal_expr(create_test_datum_string("active")),
                    ))),
                }))),
            })),
            fields: vec![proto::FieldRef {
                path: vec!["sensitive_data".to_string()],
                separator: ".".to_string(),
            }],
        }))),
    };

    let plan = planner.plan(&query).unwrap();

    match plan {
        PlanNode::Without { source, fields, .. } => {
            // Check that the source is a filter
            match source.as_ref() {
                PlanNode::Filter { .. } => {
                    // Expected
                }
                _ => panic!("Expected Filter as source"),
            }

            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].path, vec!["sensitive_data".to_string()]);
        }
        _ => panic!("Expected Without node"),
    }
}
