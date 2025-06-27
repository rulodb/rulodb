use crate::EvalError;
use crate::ast::{Cursor, FieldRef, OrderByField, query_result};
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
            &format!("User{}", i),
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

fn create_pluck_test_datum(id: &str, name: &str, age: i64, city: &str) -> Datum {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), string_datum(id.to_string()));
    fields.insert("name".to_string(), string_datum(name.to_string()));
    fields.insert("age".to_string(), int_datum(age));
    fields.insert("city".to_string(), string_datum(city.to_string()));

    Datum {
        value: Some(datum::Value::Object(DatumObject { fields })),
    }
}

fn create_nested_test_datum(id: &str, name: &str, street: &str, city: &str) -> Datum {
    let mut address_fields = HashMap::new();
    address_fields.insert("street".to_string(), string_datum(street.to_string()));
    address_fields.insert("city".to_string(), string_datum(city.to_string()));

    let nested_obj = Datum {
        value: Some(datum::Value::Object(DatumObject {
            fields: address_fields,
        })),
    };

    let mut fields = HashMap::new();
    fields.insert("id".to_string(), string_datum(id.to_string()));
    fields.insert("name".to_string(), string_datum(name.to_string()));
    fields.insert("address".to_string(), nested_obj);

    Datum {
        value: Some(datum::Value::Object(DatumObject { fields })),
    }
}

#[tokio::test]
async fn test_pluck_documents() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create test documents
    let docs = vec![
        create_pluck_test_datum("1", "Alice", 25, "New York"),
        create_pluck_test_datum("2", "Bob", 30, "San Francisco"),
        create_pluck_test_datum("3", "Charlie", 35, "Chicago"),
    ];

    // Create source result with documents
    let source_result = create_test_result(docs);

    // Define fields to pluck
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
        .pluck_documents_streaming(source_result, None, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 3);

        // Check that each document has only the plucked fields
        for doc in &pluck_result.documents {
            if let Some(datum::Value::Object(obj)) = &doc.value {
                assert_eq!(obj.fields.len(), 2);
                assert!(obj.fields.contains_key("name"));
                assert!(obj.fields.contains_key("age"));
                assert!(!obj.fields.contains_key("id"));
                assert!(!obj.fields.contains_key("city"));
            } else {
                panic!("Expected object datum");
            }
        }

        // Verify the first document's values
        if let Some(datum::Value::Object(obj)) = &pluck_result.documents[0].value {
            if let Some(datum::Value::String(name)) = &obj.fields.get("name").unwrap().value {
                assert_eq!(name, "Alice");
            }
            if let Some(datum::Value::Int(age)) = &obj.fields.get("age").unwrap().value {
                assert_eq!(*age, 25);
            }
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_documents_single_field() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create test documents
    let docs = vec![
        create_test_datum("1", "Alice", 25),
        create_test_datum("2", "Bob", 30),
    ];

    let source_result = create_test_result(docs);

    // Pluck only the name field
    let field_refs = vec![FieldRef {
        path: vec!["name".to_string()],
        separator: ".".to_string(),
    }];

    let result = query_processor
        .pluck_documents_streaming(source_result, None, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 2);

        // Check that each document has only the name field
        for doc in &pluck_result.documents {
            if let Some(datum::Value::Object(obj)) = &doc.value {
                assert_eq!(obj.fields.len(), 1);
                assert!(obj.fields.contains_key("name"));
                assert!(!obj.fields.contains_key("id"));
                assert!(!obj.fields.contains_key("age"));
            } else {
                panic!("Expected object datum");
            }
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_documents_nested_field() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create test documents with nested objects
    let docs = vec![
        create_nested_test_datum("1", "Alice", "123 Main St", "New York"),
        create_nested_test_datum("2", "Bob", "456 Oak Ave", "San Francisco"),
    ];

    let source_result = create_test_result(docs);

    // Pluck nested field
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
        .pluck_documents_streaming(source_result, None, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 2);

        // Check the structure of plucked documents
        for doc in &pluck_result.documents {
            if let Some(datum::Value::Object(obj)) = &doc.value {
                assert_eq!(obj.fields.len(), 2);
                assert!(obj.fields.contains_key("name"));
                assert!(obj.fields.contains_key("address.city"));
            } else {
                panic!("Expected object datum");
            }
        }
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_documents_empty_source() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create empty source result
    let source_result = create_test_result(vec![]);

    let field_refs = vec![FieldRef {
        path: vec!["name".to_string()],
        separator: ".".to_string(),
    }];

    let result = query_processor
        .pluck_documents_streaming(source_result, None, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 0);
        assert!(pluck_result.cursor.is_none());
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_documents_with_cursor() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create test documents
    let docs = vec![
        create_test_datum("1", "Alice", 25),
        create_test_datum("2", "Bob", 30),
    ];

    let source_result = create_test_result(docs);

    let field_refs = vec![FieldRef {
        path: vec!["name".to_string()],
        separator: ".".to_string(),
    }];

    // Create a cursor
    let cursor = Some(Cursor {
        start_key: Some("test_key".to_string()),
        batch_size: Some(10),
        sort: None,
    });

    let result = query_processor
        .pluck_documents_streaming(source_result, cursor, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 2);
        // Cursor should be None since we can't extract keys from plucked documents without ID field
        assert!(pluck_result.cursor.is_none());
    } else {
        panic!("Expected Pluck result");
    }
}

#[tokio::test]
async fn test_pluck_documents_with_id_field_cursor() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stats = EvalStats::new();
    let query_processor = QueryProcessor::new(storage.clone());

    // Create test documents
    let docs = vec![
        create_test_datum("1", "Alice", 25),
        create_test_datum("2", "Bob", 30),
    ];

    let source_result = create_test_result(docs);

    // Pluck both ID and name fields
    let field_refs = vec![
        FieldRef {
            path: vec!["id".to_string()],
            separator: ".".to_string(),
        },
        FieldRef {
            path: vec!["name".to_string()],
            separator: ".".to_string(),
        },
    ];

    // Create a cursor with small batch size to trigger pagination
    let cursor = Some(Cursor {
        start_key: Some("test_key".to_string()),
        batch_size: Some(1), // Small batch size
        sort: None,
    });

    let result = query_processor
        .pluck_documents_streaming(source_result, cursor, &field_refs, &mut stats)
        .await;

    assert!(result.is_ok());

    if let Ok(query_result::Result::Pluck(pluck_result)) = result {
        assert_eq!(pluck_result.documents.len(), 2);

        // Check that both ID and name fields are present
        for doc in &pluck_result.documents {
            if let Some(datum::Value::Object(obj)) = &doc.value {
                assert_eq!(obj.fields.len(), 2);
                assert!(obj.fields.contains_key("id"));
                assert!(obj.fields.contains_key("name"));
            } else {
                panic!("Expected object datum");
            }
        }

        // Since we have ID field and documents exceed batch size, cursor should be generated
        assert!(pluck_result.cursor.is_some());
    } else {
        panic!("Expected Pluck result");
    }
}
