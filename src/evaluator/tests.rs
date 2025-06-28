use crate::EvalError;
use crate::ast::{
    Cursor, Document, FieldRef, GetAllResult, GetResult, MatchExpr, OrderByField, Query,
    pluck_result, query_result, without_result,
};
use crate::evaluator::database::DatabaseOperations;
use crate::evaluator::expression::ExpressionEvaluator;
use crate::evaluator::query::QueryProcessor;
use crate::evaluator::table::TableOperations;
use crate::evaluator::utils::{bool_datum, datum_to_bool, extract_field_value, string_datum};
use crate::expression::Expr;
use crate::storage::StorageBackend;
use crate::storage::memory::MemoryStorage;
use crate::{
    BinaryOp, Datum, DatumObject, EvalStats, Expression, UnaryOp,
    binary_op::Operator as BinaryOperator, datum, unary_op::Operator as UnaryOperator,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Convert datum to integer
pub fn datum_to_int(datum: &Datum) -> Result<i64, EvalError> {
    match &datum.value {
        Some(datum::Value::Int(i)) => Ok(*i),
        Some(datum::Value::Float(f)) => Ok(*f as i64),
        Some(datum::Value::String(s)) => s.parse::<i64>().map_err(|_| EvalError::ConvertToInteger),
        Some(datum::Value::Bool(b)) => Ok(if *b { 1 } else { 0 }),
        _ => Err(EvalError::ConvertToInteger),
    }
}

/// Convert datum to string
pub fn datum_to_string(datum: &Datum) -> Result<String, EvalError> {
    match &datum.value {
        Some(datum::Value::String(s)) => Ok(s.clone()),
        Some(datum::Value::Int(i)) => Ok(i.to_string()),
        Some(datum::Value::Float(f)) => Ok(f.to_string()),
        Some(datum::Value::Bool(b)) => Ok(b.to_string()),
        _ => Err(EvalError::ConvertToString),
    }
}

/// Create a null datum
pub fn null_datum() -> Datum {
    Datum { value: None }
}

/// Create an integer datum
pub fn int_datum(i: i64) -> Datum {
    Datum {
        value: Some(datum::Value::Int(i)),
    }
}

#[tokio::test]
async fn test_create_database() {
    let storage = Arc::new(MemoryStorage::new());
    let db_ops = DatabaseOperations::new(storage);
    let mut stats = EvalStats::new();

    let result = db_ops.create_database("test_db", &mut stats).await;
    assert!(result.is_ok());
    assert_eq!(stats.rows_processed, 1);
}

#[tokio::test]
async fn test_drop_database() {
    let storage = Arc::new(MemoryStorage::new());
    let db_ops = DatabaseOperations::new(storage.clone());
    let mut stats = EvalStats::new();

    // First create a database
    let _ = db_ops.create_database("test_db", &mut stats).await;

    // Then drop it
    let result = db_ops.drop_database("test_db", &mut stats).await;
    assert!(result.is_ok());
    assert_eq!(stats.rows_processed, 2); // create + drop
}

#[tokio::test]
async fn test_list_databases() {
    let storage = Arc::new(MemoryStorage::new());
    let db_ops = DatabaseOperations::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create some databases
    let _ = db_ops.create_database("db1", &mut stats).await;
    let _ = db_ops.create_database("db2", &mut stats).await;

    // List them
    let result = db_ops.list_databases(None, &mut stats).await;
    assert!(result.is_ok());

    if let Ok(query_result::Result::DatabaseList(list_result)) = result {
        assert!(list_result.databases.len() >= 2);
    } else {
        panic!("Expected DatabaseList result");
    }
}

fn create_test_context() -> Datum {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), string_datum("Alice".to_string()));
    fields.insert("age".to_string(), int_datum(30));
    fields.insert("active".to_string(), bool_datum(true));

    Datum {
        value: Some(datum::Value::Object(DatumObject { fields })),
    }
}

#[test]
fn test_literal_evaluation() {
    let evaluator = ExpressionEvaluator::new();
    let context = null_datum();

    let expr = Expression {
        expr: Some(Expr::Literal(int_datum(42))),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert_eq!(datum_to_int(&result).unwrap(), 42);
}

#[test]
fn test_field_evaluation() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    let expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["name".to_string()],
            separator: String::new(),
        })),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert_eq!(datum_to_string(&result).unwrap(), "Alice");
}

#[test]
fn test_binary_comparison() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["age".to_string()],
            separator: String::new(),
        })),
    };

    let value_expr = Expression {
        expr: Some(Expr::Literal(int_datum(25))),
    };

    let expr = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Gt.into(),
            left: Some(Box::new(field_expr)),
            right: Some(Box::new(value_expr)),
        }))),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert!(datum_to_bool(&result));
}

#[test]
fn test_logical_and_short_circuit() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    let false_expr = Expression {
        expr: Some(Expr::Literal(bool_datum(false))),
    };

    let true_expr = Expression {
        expr: Some(Expr::Literal(bool_datum(true))),
    };

    let expr = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::And.into(),
            left: Some(Box::new(false_expr)),
            right: Some(Box::new(true_expr)),
        }))),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert!(!datum_to_bool(&result));
}

#[test]
fn test_logical_or_short_circuit() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    let true_expr = Expression {
        expr: Some(Expr::Literal(bool_datum(true))),
    };

    let false_expr = Expression {
        expr: Some(Expr::Literal(bool_datum(false))),
    };

    let expr = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Or.into(),
            left: Some(Box::new(true_expr)),
            right: Some(Box::new(false_expr)),
        }))),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert!(datum_to_bool(&result));
}

#[test]
fn test_unary_not() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["active".to_string()],
            separator: String::new(),
        })),
    };

    let expr = Expression {
        expr: Some(Expr::Unary(Box::new(UnaryOp {
            op: UnaryOperator::Not.into(),
            expr: Some(Box::new(field_expr)),
        }))),
    };

    let result = evaluator.evaluate_expression(&expr, &context).unwrap();
    assert!(!datum_to_bool(&result));
}

#[test]
fn test_is_boolean_expression() {
    let evaluator = ExpressionEvaluator::new();

    // Test boolean literal
    let bool_expr = Expression {
        expr: Some(Expr::Literal(bool_datum(true))),
    };
    assert!(evaluator.is_boolean_expression(&bool_expr));

    // Test comparison expression
    let comp_expr = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Eq.into(),
            left: Some(Box::new(Expression {
                expr: Some(Expr::Literal(int_datum(1))),
            })),
            right: Some(Box::new(Expression {
                expr: Some(Expr::Literal(int_datum(1))),
            })),
        }))),
    };
    assert!(evaluator.is_boolean_expression(&comp_expr));

    // Test non-boolean expression
    let int_expr = Expression {
        expr: Some(Expr::Literal(int_datum(42))),
    };
    assert!(!evaluator.is_boolean_expression(&int_expr));

    // Test subquery with boolean expression (match)
    let match_expr = Expression {
        expr: Some(Expr::Match(Box::new(MatchExpr {
            value: Some(Box::new(Expression {
                expr: Some(Expr::Field(FieldRef {
                    path: vec!["email".to_string()],
                    separator: ".".to_string(),
                })),
            })),
            pattern: "@example\\.com$".to_string(),
            flags: "".to_string(),
        }))),
    };
    let subquery_expr = Expression {
        expr: Some(Expr::Subquery(Box::new(Query {
            kind: Some(crate::ast::query::Kind::Expression(Box::new(match_expr))),
            cursor: None,
            options: None,
        }))),
    };
    assert!(evaluator.is_boolean_expression(&subquery_expr));

    // Test subquery with non-boolean expression
    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        })),
    };
    let non_bool_subquery_expr = Expression {
        expr: Some(Expr::Subquery(Box::new(Query {
            kind: Some(crate::ast::query::Kind::Expression(Box::new(field_expr))),
            cursor: None,
            options: None,
        }))),
    };
    assert!(!evaluator.is_boolean_expression(&non_bool_subquery_expr));
}

#[test]
fn test_simple_subquery_evaluation() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    // Create a subquery that wraps a field reference (similar to TypeScript SDK output)
    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        })),
    };

    let subquery = crate::ast::Query {
        options: None,
        cursor: None,
        kind: Some(crate::ast::query::Kind::Expression(Box::new(field_expr))),
    };

    let subquery_expr = Expression {
        expr: Some(Expr::Subquery(Box::new(subquery))),
    };

    // The subquery should evaluate to the same result as the wrapped field reference
    let result = evaluator
        .evaluate_expression(&subquery_expr, &context)
        .unwrap();
    assert_eq!(datum_to_string(&result).unwrap(), "Alice");
}

#[test]
fn test_complex_subquery_expression() {
    let evaluator = ExpressionEvaluator::new();
    let context = create_test_context();

    // Create a binary operation with subquery on the left side (like TypeScript SDK)
    let field_subquery = crate::ast::Query {
        options: None,
        cursor: None,
        kind: Some(crate::ast::query::Kind::Expression(Box::new(Expression {
            expr: Some(Expr::Field(FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            })),
        }))),
    };

    let left_expr = Expression {
        expr: Some(Expr::Subquery(Box::new(field_subquery))),
    };

    let right_expr = Expression {
        expr: Some(Expr::Literal(string_datum("Alice".to_string()))),
    };

    let comparison_expr = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Eq.into(),
            left: Some(Box::new(left_expr)),
            right: Some(Box::new(right_expr)),
        }))),
    };

    let result = evaluator
        .evaluate_expression(&comparison_expr, &context)
        .unwrap();
    assert!(datum_to_bool(&result));
}

fn create_test_datum(id: &str, name: &str, age: i64) -> Datum {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), string_datum(id.to_string()));
    fields.insert("name".to_string(), string_datum(name.to_string()));
    fields.insert("age".to_string(), int_datum(age));

    Datum {
        value: Some(datum::Value::Object(DatumObject { fields })),
    }
}

fn create_test_result(documents: Vec<Datum>) -> query_result::Result {
    query_result::Result::Table(crate::ast::TableScanResult {
        documents,
        cursor: None,
    })
}

#[tokio::test]
async fn test_filter_documents() {
    let storage = Arc::new(MemoryStorage::new());
    let processor = QueryProcessor::new(storage);
    let mut stats = EvalStats::new();

    let documents = vec![
        create_test_datum("1", "Alice", 30),
        create_test_datum("2", "Bob", 25),
        create_test_datum("3", "Charlie", 35),
    ];

    let source_result = create_test_result(documents);

    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["age".to_string()],
            separator: String::new(),
        })),
    };

    let value_expr = Expression {
        expr: Some(Expr::Literal(int_datum(28))),
    };

    let predicate = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Gt.into(),
            left: Some(Box::new(field_expr)),
            right: Some(Box::new(value_expr)),
        }))),
    };

    let result = processor
        .filter_documents(source_result, &predicate, None, &mut stats)
        .await;

    assert!(result.is_ok());
    if let Ok(query_result::Result::Filter(filter_result)) = result {
        assert_eq!(filter_result.documents.len(), 2); // Alice and Charlie
    }
}

#[tokio::test]
async fn test_order_documents() {
    let storage = Arc::new(MemoryStorage::new());
    let processor = QueryProcessor::new(storage);
    let mut stats = EvalStats::new();

    let documents = vec![
        create_test_datum("1", "Charlie", 25),
        create_test_datum("2", "Alice", 30),
        create_test_datum("3", "Bob", 20),
    ];

    let source_result = create_test_result(documents);
    let order_fields = vec![OrderByField {
        field_name: "age".to_string(),
        ascending: true,
    }];

    let result = processor
        .order_documents(source_result, &order_fields, None, &mut stats)
        .await;

    assert!(result.is_ok());
    if let Ok(query_result::Result::OrderBy(order_result)) = result {
        assert_eq!(order_result.documents.len(), 3);
        // Should be sorted by age: Bob(20), Charlie(25), Alice(30)
        let first_age = extract_field_value(&order_result.documents[0], "age");
        assert_eq!(datum_to_int(&first_age).unwrap(), 20);
    }
}

#[tokio::test]
async fn test_filter_documents_with_cursor() {
    let storage = Arc::new(MemoryStorage::new());
    let processor = QueryProcessor::new(storage);
    let mut stats = EvalStats::default();

    // Create 15 documents to exceed batch size of 10
    let mut documents = Vec::new();
    for i in 1..=15 {
        documents.push(create_test_datum(
            &i.to_string(),
            &format!("User{i}"),
            20 + i,
        ));
    }

    let source_result = create_test_result(documents);

    // Create a predicate: age > 19 (should match all)
    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["age".to_string()],
            separator: String::new(),
        })),
    };
    let value_expr = Expression {
        expr: Some(Expr::Literal(int_datum(19))),
    };
    let predicate = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Gt.into(),
            left: Some(Box::new(field_expr)),
            right: Some(Box::new(value_expr)),
        }))),
    };

    // Test with cursor having the batch size of 10
    let cursor = Some(Cursor {
        start_key: Some("start".to_string()),
        batch_size: Some(10),
        sort: None,
    });

    let result = processor
        .filter_documents(source_result, &predicate, cursor, &mut stats)
        .await;

    assert!(result.is_ok());
    if let Ok(query_result::Result::Filter(filter_result)) = result {
        assert_eq!(filter_result.documents.len(), 15); // All match age > 19
        assert!(filter_result.cursor.is_some()); // Should have cursor as 15 >= 10
    }
}

#[tokio::test]
async fn test_filter_documents_no_cursor_when_few_results() {
    let storage = Arc::new(MemoryStorage::new());
    let processor = QueryProcessor::new(storage);
    let mut stats = EvalStats::default();

    let documents = vec![
        create_test_datum("1", "Alice", 30),
        create_test_datum("2", "Bob", 20),
        create_test_datum("3", "Charlie", 25),
    ];

    let source_result = create_test_result(documents);

    // Create a predicate: age > 19
    let field_expr = Expression {
        expr: Some(Expr::Field(FieldRef {
            path: vec!["age".to_string()],
            separator: String::new(),
        })),
    };
    let value_expr = Expression {
        expr: Some(Expr::Literal(int_datum(19))),
    };
    let predicate = Expression {
        expr: Some(Expr::Binary(Box::new(BinaryOp {
            op: BinaryOperator::Gt.into(),
            left: Some(Box::new(field_expr)),
            right: Some(Box::new(value_expr)),
        }))),
    };

    // Test with cursor having the batch size of 10
    let cursor = Some(Cursor {
        start_key: Some("start".to_string()),
        batch_size: Some(10),
        sort: None,
    });

    let result = processor
        .filter_documents(source_result, &predicate, cursor, &mut stats)
        .await;

    assert!(result.is_ok());
    if let Ok(query_result::Result::Filter(filter_result)) = result {
        assert_eq!(filter_result.documents.len(), 3); // All match age > 19
        assert!(filter_result.cursor.is_none()); // No cursor since 3 < 10
    }
}

#[tokio::test]
async fn test_create_table() {
    let storage = Arc::new(MemoryStorage::new());
    let table_ops = TableOperations::new(storage.clone());
    let mut stats = EvalStats::new();

    storage.create_database("test_db").await.unwrap();

    let result = table_ops
        .create_table("test_db", "test_table", &mut stats)
        .await;

    assert!(result.is_ok());
    assert_eq!(stats.rows_processed, 1);
}

#[tokio::test]
async fn test_insert_documents() {
    let storage = Arc::new(MemoryStorage::new());
    let table_ops = TableOperations::new(storage.clone());
    let mut stats = EvalStats::new();

    storage.create_database("test_db").await.unwrap();

    table_ops
        .create_table("test_db", "test_table", &mut stats)
        .await
        .unwrap();

    // Insert a document
    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        Datum {
            value: Some(datum::Value::String("test".to_string())),
        },
    );
    let doc = DatumObject { fields };

    let result = table_ops
        .insert_documents("test_db", "test_table", &[doc], &mut stats)
        .await;
    assert!(result.is_ok());

    if let Ok(query_result::Result::Insert(insert_result)) = result {
        assert_eq!(insert_result.inserted, 1);
        assert_eq!(insert_result.generated_keys.len(), 1);
    } else {
        panic!("Expected Insert result");
    }
}

#[tokio::test]
async fn test_get_document() {
    let storage = Arc::new(MemoryStorage::new());
    let table_ops = TableOperations::new(storage.clone());
    let mut stats = EvalStats::new();

    storage.create_database("test_db").await.unwrap();

    table_ops
        .create_table("test_db", "test_table", &mut stats)
        .await
        .unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "id".to_string(),
        Datum {
            value: Some(datum::Value::String("test_id".to_string())),
        },
    );
    fields.insert(
        "name".to_string(),
        Datum {
            value: Some(datum::Value::String("test".to_string())),
        },
    );
    let doc = DatumObject { fields };

    table_ops
        .insert_documents("test_db", "test_table", &[doc], &mut stats)
        .await
        .unwrap();

    // Now get the document
    let result = table_ops
        .get_document("test_db", "test_table", "test_id", &mut stats)
        .await;
    assert!(result.is_ok());

    if let Ok(query_result::Result::Get(get_result)) = result {
        assert!(get_result.document.is_some());
    } else {
        panic!("Expected Get result");
    }
}

#[tokio::test]
async fn test_pluck_single_object_flat_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test document
    let mut doc = Document::new();
    doc.insert("id".to_string(), int_datum(1));
    doc.insert("name".to_string(), string_datum("John".to_string()));
    doc.insert("age".to_string(), int_datum(30));
    doc.insert(
        "email".to_string(),
        string_datum("john@example.com".to_string()),
    );

    let get_result = query_result::Result::Get(GetResult {
        document: Some(doc.into()),
    });

    // Pluck name and age fields
    let field_refs = vec![
        FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["age".to_string()],
            separator: ".".to_string(),
        },
    ];

    let result = query_processor
        .pluck_documents_streaming(get_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Pluck(pluck_result) = result {
        if let Some(pluck_result::Result::Document(doc)) = pluck_result.result {
            let doc_map = Document::from(&doc);
            assert_eq!(doc_map.len(), 2);
            assert_eq!(
                datum_to_string(doc_map.get("name").unwrap()).unwrap(),
                "John"
            );
            assert_eq!(datum_to_int(doc_map.get("age").unwrap()).unwrap(), 30);
            // Should not contain id or email
            assert!(!doc_map.contains_key("id"));
            assert!(!doc_map.contains_key("email"));
        } else {
            panic!("Expected single document result");
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_single_object_nested_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test document with nested structure
    let mut address = Document::new();
    address.insert(
        "street".to_string(),
        string_datum("123 Main St".to_string()),
    );
    address.insert("city".to_string(), string_datum("Boston".to_string()));
    address.insert("zip".to_string(), string_datum("02101".to_string()));

    let mut doc = Document::new();
    doc.insert("id".to_string(), int_datum(1));
    doc.insert("name".to_string(), string_datum("John".to_string()));
    doc.insert("address".to_string(), address.into());

    let get_result = query_result::Result::Get(GetResult {
        document: Some(doc.into()),
    });

    // Pluck nested fields
    let field_refs = vec![
        FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["address".to_string(), "city".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["address".to_string(), "zip".to_string()],
            separator: ".".to_string(),
        },
    ];

    let result = query_processor
        .pluck_documents_streaming(get_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Pluck(pluck_result) = result {
        if let Some(pluck_result::Result::Document(doc)) = pluck_result.result {
            let doc_map = Document::from(&doc);
            assert_eq!(doc_map.len(), 2); // name and address
            assert_eq!(
                datum_to_string(doc_map.get("name").unwrap()).unwrap(),
                "John"
            );

            // Check nested address structure
            if let Some(datum::Value::Object(addr_obj)) = &doc_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 2); // only city and zip
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Boston"
                );
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("zip").unwrap()).unwrap(),
                    "02101"
                );
                assert!(!addr_obj.fields.contains_key("street"));
            } else {
                panic!("Expected nested address object");
            }
        } else {
            panic!("Expected single document result");
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_multi_object_flat_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create multiple test documents
    let mut doc1 = Document::new();
    doc1.insert("id".to_string(), int_datum(1));
    doc1.insert("name".to_string(), string_datum("John".to_string()));
    doc1.insert("age".to_string(), int_datum(30));
    doc1.insert(
        "email".to_string(),
        string_datum("john@example.com".to_string()),
    );

    let mut doc2 = Document::new();
    doc2.insert("id".to_string(), int_datum(2));
    doc2.insert("name".to_string(), string_datum("Jane".to_string()));
    doc2.insert("age".to_string(), int_datum(25));
    doc2.insert(
        "email".to_string(),
        string_datum("jane@example.com".to_string()),
    );

    let docs = vec![doc1.into(), doc2.into()];
    let get_all_result = query_result::Result::GetAll(GetAllResult {
        documents: docs,
        cursor: None,
    });

    // Pluck name and age fields
    let field_refs = vec![
        FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["age".to_string()],
            separator: ".".to_string(),
        },
    ];

    let result = query_processor
        .pluck_documents_streaming(get_all_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Pluck(pluck_result) = result {
        if let Some(pluck_result::Result::Collection(collection)) = pluck_result.result {
            assert_eq!(collection.documents.len(), 2);

            // Check first document
            let doc1_map = Document::from(&collection.documents[0]);
            assert_eq!(doc1_map.len(), 2);
            assert_eq!(
                datum_to_string(doc1_map.get("name").unwrap()).unwrap(),
                "John"
            );
            assert_eq!(datum_to_int(doc1_map.get("age").unwrap()).unwrap(), 30);
            assert!(!doc1_map.contains_key("id"));
            assert!(!doc1_map.contains_key("email"));

            // Check second document
            let doc2_map = Document::from(&collection.documents[1]);
            assert_eq!(doc2_map.len(), 2);
            assert_eq!(
                datum_to_string(doc2_map.get("name").unwrap()).unwrap(),
                "Jane"
            );
            assert_eq!(datum_to_int(doc2_map.get("age").unwrap()).unwrap(), 25);
            assert!(!doc2_map.contains_key("id"));
            assert!(!doc2_map.contains_key("email"));
        } else {
            panic!("Expected collection result");
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_multi_object_nested_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test documents with nested structure
    let mut address1 = Document::new();
    address1.insert(
        "street".to_string(),
        string_datum("123 Main St".to_string()),
    );
    address1.insert("city".to_string(), string_datum("Boston".to_string()));
    address1.insert("zip".to_string(), string_datum("02101".to_string()));

    let mut doc1 = Document::new();
    doc1.insert("id".to_string(), int_datum(1));
    doc1.insert("name".to_string(), string_datum("John".to_string()));
    doc1.insert("address".to_string(), address1.into());

    let mut address2 = Document::new();
    address2.insert(
        "street".to_string(),
        string_datum("456 Oak Ave".to_string()),
    );
    address2.insert("city".to_string(), string_datum("Seattle".to_string()));
    address2.insert("zip".to_string(), string_datum("98101".to_string()));

    let mut doc2 = Document::new();
    doc2.insert("id".to_string(), int_datum(2));
    doc2.insert("name".to_string(), string_datum("Jane".to_string()));
    doc2.insert("address".to_string(), address2.into());

    let docs = vec![doc1.into(), doc2.into()];
    let get_all_result = query_result::Result::GetAll(GetAllResult {
        documents: docs,
        cursor: None,
    });

    // Pluck nested fields
    let field_refs = vec![
        FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["address".to_string(), "city".to_string()],
            separator: ".".to_string(),
        },
    ];

    let result = query_processor
        .pluck_documents_streaming(get_all_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Pluck(pluck_result) = result {
        if let Some(pluck_result::Result::Collection(collection)) = pluck_result.result {
            assert_eq!(collection.documents.len(), 2);

            // Check first document
            let doc1_map = Document::from(&collection.documents[0]);
            assert_eq!(doc1_map.len(), 2); // name and address
            assert_eq!(
                datum_to_string(doc1_map.get("name").unwrap()).unwrap(),
                "John"
            );

            if let Some(datum::Value::Object(addr_obj)) = &doc1_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 1); // only city
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Boston"
                );
            } else {
                panic!("Expected nested address object");
            }

            // Check second document
            let doc2_map = Document::from(&collection.documents[1]);
            assert_eq!(doc2_map.len(), 2); // name and address
            assert_eq!(
                datum_to_string(doc2_map.get("name").unwrap()).unwrap(),
                "Jane"
            );

            if let Some(datum::Value::Object(addr_obj)) = &doc2_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 1); // only city
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Seattle"
                );
            } else {
                panic!("Expected nested address object");
            }
        } else {
            panic!("Expected collection result");
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_without_single_object_flat_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test document
    let mut doc = Document::new();
    doc.insert("id".to_string(), int_datum(1));
    doc.insert("name".to_string(), string_datum("John".to_string()));
    doc.insert("age".to_string(), int_datum(30));
    doc.insert(
        "email".to_string(),
        string_datum("john@example.com".to_string()),
    );

    let get_result = query_result::Result::Get(GetResult {
        document: Some(doc.into()),
    });

    // Remove email field
    let field_refs = vec![FieldRef {
        path: vec!["email".to_string()],
        separator: ".".to_string(),
    }];

    let result = query_processor
        .without_documents_streaming(get_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Without(without_result) = result {
        if let Some(without_result::Result::Document(doc)) = without_result.result {
            let doc_map = Document::from(&doc);
            assert_eq!(doc_map.len(), 3); // id, name, age (email removed)
            assert_eq!(datum_to_int(doc_map.get("id").unwrap()).unwrap(), 1);
            assert_eq!(
                datum_to_string(doc_map.get("name").unwrap()).unwrap(),
                "John"
            );
            assert_eq!(datum_to_int(doc_map.get("age").unwrap()).unwrap(), 30);
            assert!(!doc_map.contains_key("email"));
        } else {
            panic!("Expected single document result");
        }
    } else {
        panic!("Expected Without result");
    }
}

#[tokio::test]
async fn test_without_single_object_nested_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test document with nested structure
    let mut address = Document::new();
    address.insert(
        "street".to_string(),
        string_datum("123 Main St".to_string()),
    );
    address.insert("city".to_string(), string_datum("Boston".to_string()));
    address.insert("zip".to_string(), string_datum("02101".to_string()));

    let mut doc = Document::new();
    doc.insert("id".to_string(), int_datum(1));
    doc.insert("name".to_string(), string_datum("John".to_string()));
    doc.insert("address".to_string(), address.into());

    let get_result = query_result::Result::Get(GetResult {
        document: Some(doc.into()),
    });

    // Remove nested street field
    let field_refs = vec![FieldRef {
        path: vec!["address".to_string(), "street".to_string()],
        separator: ".".to_string(),
    }];

    let result = query_processor
        .without_documents_streaming(get_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Without(without_result) = result {
        if let Some(without_result::Result::Document(doc)) = without_result.result {
            let doc_map = Document::from(&doc);
            assert_eq!(doc_map.len(), 3); // id, name, address
            assert_eq!(datum_to_int(doc_map.get("id").unwrap()).unwrap(), 1);
            assert_eq!(
                datum_to_string(doc_map.get("name").unwrap()).unwrap(),
                "John"
            );

            // Check nested address structure (street should be removed)
            if let Some(datum::Value::Object(addr_obj)) = &doc_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 2); // city and zip (street removed)
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Boston"
                );
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("zip").unwrap()).unwrap(),
                    "02101"
                );
                assert!(!addr_obj.fields.contains_key("street"));
            } else {
                panic!("Expected nested address object");
            }
        } else {
            panic!("Expected single document result");
        }
    } else {
        panic!("Expected Without result");
    }
}

#[tokio::test]
async fn test_without_multi_object_flat_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create multiple test documents
    let mut doc1 = Document::new();
    doc1.insert("id".to_string(), int_datum(1));
    doc1.insert("name".to_string(), string_datum("John".to_string()));
    doc1.insert("age".to_string(), int_datum(30));
    doc1.insert(
        "email".to_string(),
        string_datum("john@example.com".to_string()),
    );

    let mut doc2 = Document::new();
    doc2.insert("id".to_string(), int_datum(2));
    doc2.insert("name".to_string(), string_datum("Jane".to_string()));
    doc2.insert("age".to_string(), int_datum(25));
    doc2.insert(
        "email".to_string(),
        string_datum("jane@example.com".to_string()),
    );

    let docs = vec![doc1.into(), doc2.into()];
    let get_all_result = query_result::Result::GetAll(GetAllResult {
        documents: docs,
        cursor: None,
    });

    // Remove email and age fields
    let field_refs = vec![
        FieldRef {
            path: vec!["email".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["age".to_string()],
            separator: ".".to_string(),
        },
    ];

    let result = query_processor
        .without_documents_streaming(get_all_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Without(without_result) = result {
        if let Some(without_result::Result::Collection(collection)) = without_result.result {
            assert_eq!(collection.documents.len(), 2);

            // Check first document
            let doc1_map = Document::from(&collection.documents[0]);
            assert_eq!(doc1_map.len(), 2); // id, name (email and age removed)
            assert_eq!(datum_to_int(doc1_map.get("id").unwrap()).unwrap(), 1);
            assert_eq!(
                datum_to_string(doc1_map.get("name").unwrap()).unwrap(),
                "John"
            );
            assert!(!doc1_map.contains_key("email"));
            assert!(!doc1_map.contains_key("age"));

            // Check second document
            let doc2_map = Document::from(&collection.documents[1]);
            assert_eq!(doc2_map.len(), 2); // id, name (email and age removed)
            assert_eq!(datum_to_int(doc2_map.get("id").unwrap()).unwrap(), 2);
            assert_eq!(
                datum_to_string(doc2_map.get("name").unwrap()).unwrap(),
                "Jane"
            );
            assert!(!doc2_map.contains_key("email"));
            assert!(!doc2_map.contains_key("age"));
        } else {
            panic!("Expected collection result");
        }
    } else {
        panic!("Expected Without result");
    }
}

#[tokio::test]
async fn test_without_multi_object_nested_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let query_processor = QueryProcessor::new(storage.clone());
    let mut stats = EvalStats::new();

    // Create test documents with nested structure
    let mut address1 = Document::new();
    address1.insert(
        "street".to_string(),
        string_datum("123 Main St".to_string()),
    );
    address1.insert("city".to_string(), string_datum("Boston".to_string()));
    address1.insert("zip".to_string(), string_datum("02101".to_string()));

    let mut doc1 = Document::new();
    doc1.insert("id".to_string(), int_datum(1));
    doc1.insert("name".to_string(), string_datum("John".to_string()));
    doc1.insert("address".to_string(), address1.into());

    let mut address2 = Document::new();
    address2.insert(
        "street".to_string(),
        string_datum("456 Oak Ave".to_string()),
    );
    address2.insert("city".to_string(), string_datum("Seattle".to_string()));
    address2.insert("zip".to_string(), string_datum("98101".to_string()));

    let mut doc2 = Document::new();
    doc2.insert("id".to_string(), int_datum(2));
    doc2.insert("name".to_string(), string_datum("Jane".to_string()));
    doc2.insert("address".to_string(), address2.into());

    let docs = vec![doc1.into(), doc2.into()];
    let get_all_result = query_result::Result::GetAll(GetAllResult {
        documents: docs,
        cursor: None,
    });

    // Remove nested zip field
    let field_refs = vec![FieldRef {
        path: vec!["address".to_string(), "zip".to_string()],
        separator: ".".to_string(),
    }];

    let result = query_processor
        .without_documents_streaming(get_all_result, None, &field_refs, &mut stats)
        .await
        .unwrap();

    if let query_result::Result::Without(without_result) = result {
        if let Some(without_result::Result::Collection(collection)) = without_result.result {
            assert_eq!(collection.documents.len(), 2);

            // Check first document
            let doc1_map = Document::from(&collection.documents[0]);
            assert_eq!(doc1_map.len(), 3); // id, name, address
            assert_eq!(datum_to_int(doc1_map.get("id").unwrap()).unwrap(), 1);
            assert_eq!(
                datum_to_string(doc1_map.get("name").unwrap()).unwrap(),
                "John"
            );

            if let Some(datum::Value::Object(addr_obj)) = &doc1_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 2); // street and city (zip removed)
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("street").unwrap()).unwrap(),
                    "123 Main St"
                );
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Boston"
                );
                assert!(!addr_obj.fields.contains_key("zip"));
            } else {
                panic!("Expected nested address object");
            }

            // Check second document
            let doc2_map = Document::from(&collection.documents[1]);
            assert_eq!(doc2_map.len(), 3); // id, name, address
            assert_eq!(datum_to_int(doc2_map.get("id").unwrap()).unwrap(), 2);
            assert_eq!(
                datum_to_string(doc2_map.get("name").unwrap()).unwrap(),
                "Jane"
            );

            if let Some(datum::Value::Object(addr_obj)) = &doc2_map.get("address").unwrap().value {
                assert_eq!(addr_obj.fields.len(), 2); // street and city (zip removed)
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("street").unwrap()).unwrap(),
                    "456 Oak Ave"
                );
                assert_eq!(
                    datum_to_string(addr_obj.fields.get("city").unwrap()).unwrap(),
                    "Seattle"
                );
                assert!(!addr_obj.fields.contains_key("zip"));
            } else {
                panic!("Expected nested address object");
            }
        } else {
            panic!("Expected collection result");
        }
    } else {
        panic!("Expected Without result");
    }
}
