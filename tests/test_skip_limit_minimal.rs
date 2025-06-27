use rulodb::ast::*;
use rulodb::evaluator::Evaluator;
use rulodb::planner::Planner;
use rulodb::storage::{Config, DefaultStorage};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_skip_limit_basic_functionality() {
    // Setup storage
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = Config {
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    let storage = Arc::new(DefaultStorage::open(&config).expect("Failed to create storage"));
    let mut evaluator = Evaluator::new(storage.clone());
    let mut planner = Planner::new();

    // Create database
    let create_db_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: None,
        kind: Some(query::Kind::DatabaseCreate(DatabaseCreate {
            name: "test_db".to_string(),
        })),
    };
    let plan = planner.plan(&create_db_query).unwrap();
    evaluator.eval(&plan).await.unwrap();

    // Create table
    let create_table_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: None,
        kind: Some(query::Kind::TableCreate(TableCreate {
            table: Some(TableRef {
                database: Some(DatabaseRef {
                    name: "test_db".to_string(),
                }),
                name: "users".to_string(),
            }),
        })),
    };
    let plan = planner.plan(&create_table_query).unwrap();
    evaluator.eval(&plan).await.unwrap();

    // Insert test data - 20 users
    let mut documents = Vec::new();
    for i in 0..20 {
        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String(format!("user_{i:02}"))),
            },
        );
        fields.insert(
            "name".to_string(),
            Datum {
                value: Some(datum::Value::String(format!("User {i}"))),
            },
        );
        fields.insert(
            "age".to_string(),
            Datum {
                value: Some(datum::Value::Int((20 + i) as i64)),
            },
        );
        documents.push(DatumObject { fields });
    }

    let insert_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: None,
        kind: Some(query::Kind::Insert(Box::new(Insert {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Table(Table {
                    table: Some(TableRef {
                        database: Some(DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "users".to_string(),
                    }),
                })),
            })),
            documents,
        }))),
    };
    let plan = planner.plan(&insert_query).unwrap();
    evaluator.eval(&plan).await.unwrap();

    // Test Case 1: Simple table scan to verify data
    println!("Test Case 1: Simple table scan");
    let simple_scan = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: Some(Cursor {
            start_key: None,
            batch_size: Some(5),
            sort: None,
        }),
        kind: Some(query::Kind::Table(Table {
            table: Some(TableRef {
                database: Some(DatabaseRef {
                    name: "test_db".to_string(),
                }),
                name: "users".to_string(),
            }),
        })),
    };

    let plan = planner.plan(&simple_scan).unwrap();
    let result = evaluator
        .eval_with_cursor(&plan, simple_scan.cursor.clone())
        .await
        .unwrap();

    match &result.result {
        query_result::Result::Table(table_result) => {
            println!("  Got {} documents", table_result.documents.len());
            assert_eq!(
                table_result.documents.len(),
                5,
                "Should return 5 documents (batch_size)"
            );
        }
        _ => panic!("Expected Table result"),
    }

    // Test Case 2: Table scan with Skip pushed down
    println!("\nTest Case 2: Table scan with Skip");
    let skip_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: Some(Cursor {
            start_key: None,
            batch_size: Some(3),
            sort: None,
        }),
        kind: Some(query::Kind::Skip(Box::new(Skip {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Table(Table {
                    table: Some(TableRef {
                        database: Some(DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "users".to_string(),
                    }),
                })),
            })),
            count: 5,
        }))),
    };

    let plan = planner.plan(&skip_query).unwrap();
    let result = evaluator
        .eval_with_cursor(&plan, skip_query.cursor.clone())
        .await
        .unwrap();

    match &result.result {
        query_result::Result::Skip(skip_result) => {
            println!(
                "  Got {} documents after skipping 5",
                skip_result.documents.len()
            );
            assert_eq!(
                skip_result.documents.len(),
                3,
                "Should return 3 documents (batch_size)"
            );

            // Verify first document is user_05 (after skipping 0-4)
            if let Some(datum::Value::Object(obj)) = &skip_result.documents[0].value {
                if let Some(Datum {
                    value: Some(datum::Value::String(id)),
                }) = obj.fields.get("id")
                {
                    println!("  First document ID: {id}");
                    assert_eq!(
                        id, "user_05",
                        "First document should be user_05 after skipping 5"
                    );
                }
            }
        }
        _ => panic!("Expected Skip result"),
    }

    // Test Case 3: Limit with batch size interaction
    println!("\nTest Case 3: Limit with batch size");
    let limit_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: Some(Cursor {
            start_key: None,
            batch_size: Some(10),
            sort: None,
        }),
        kind: Some(query::Kind::Limit(Box::new(Limit {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Table(Table {
                    table: Some(TableRef {
                        database: Some(DatabaseRef {
                            name: "test_db".to_string(),
                        }),
                        name: "users".to_string(),
                    }),
                })),
            })),
            count: 5,
        }))),
    };

    let plan = planner.plan(&limit_query).unwrap();
    let result = evaluator
        .eval_with_cursor(&plan, limit_query.cursor.clone())
        .await
        .unwrap();

    match &result.result {
        query_result::Result::Limit(limit_result) => {
            println!(
                "  Got {} documents with limit 5 and batch_size 10",
                limit_result.documents.len()
            );
            assert_eq!(
                limit_result.documents.len(),
                5,
                "Should return 5 documents (min of limit and batch_size)"
            );
        }
        _ => panic!("Expected Limit result"),
    }

    // Test Case 4: Skip + Limit with small batch size
    println!("\nTest Case 4: Skip + Limit with small batch size");
    let complex_query = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: Some(Cursor {
            start_key: None,
            batch_size: Some(2),
            sort: None,
        }),
        kind: Some(query::Kind::Skip(Box::new(Skip {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Limit(Box::new(Limit {
                    source: Some(Box::new(Query {
                        options: None,
                        cursor: None,
                        kind: Some(query::Kind::Table(Table {
                            table: Some(TableRef {
                                database: Some(DatabaseRef {
                                    name: "test_db".to_string(),
                                }),
                                name: "users".to_string(),
                            }),
                        })),
                    })),
                    count: 10,
                }))),
            })),
            count: 3,
        }))),
    };

    let plan = planner.plan(&complex_query).unwrap();
    // Print the plan for debugging
    println!("  Query plan: {}", planner.explain(&plan));

    let result = evaluator
        .eval_with_cursor(&plan, complex_query.cursor.clone())
        .await
        .unwrap();

    match &result.result {
        query_result::Result::Skip(skip_result) => {
            println!(
                "  Got {} documents with skip 3, limit 10, batch_size 2",
                skip_result.documents.len()
            );
            // With Skip(Limit(TableScan)), the execution is:
            // 1. TableScan with limit=10 and batch_size=2 returns min(10,2)=2 documents
            // 2. Skip=3 is applied to those 2 documents, skipping all of them
            // Result: 0 documents
            assert_eq!(
                skip_result.documents.len(),
                0,
                "Should return 0 documents (skip count exceeds available documents)"
            );
        }
        _ => panic!("Expected Skip result"),
    }

    // Test Case 5: Skip + Limit where skip is less than returned documents
    println!("\nTest Case 5: Skip + Limit where skip < batch_size");
    let complex_query2 = Query {
        options: Some(QueryOptions {
            timeout_ms: 1000,
            explain: false,
        }),
        cursor: Some(Cursor {
            start_key: None,
            batch_size: Some(5),
            sort: None,
        }),
        kind: Some(query::Kind::Skip(Box::new(Skip {
            source: Some(Box::new(Query {
                options: None,
                cursor: None,
                kind: Some(query::Kind::Limit(Box::new(Limit {
                    source: Some(Box::new(Query {
                        options: None,
                        cursor: None,
                        kind: Some(query::Kind::Table(Table {
                            table: Some(TableRef {
                                database: Some(DatabaseRef {
                                    name: "test_db".to_string(),
                                }),
                                name: "users".to_string(),
                            }),
                        })),
                    })),
                    count: 10,
                }))),
            })),
            count: 2,
        }))),
    };

    let plan2 = planner.plan(&complex_query2).unwrap();
    let result2 = evaluator
        .eval_with_cursor(&plan2, complex_query2.cursor.clone())
        .await
        .unwrap();

    match &result2.result {
        query_result::Result::Skip(skip_result) => {
            println!(
                "  Got {} documents with skip 2, limit 10, batch_size 5",
                skip_result.documents.len()
            );
            // With Skip(Limit(TableScan)), the execution is:
            // 1. TableScan with limit=10 and batch_size=5 returns min(10,5)=5 documents
            // 2. Skip=2 is applied to those 5 documents, skipping first 2
            // Result: 3 documents
            assert_eq!(
                skip_result.documents.len(),
                3,
                "Should return 3 documents (5 from limit/batch_size minus 2 skipped)"
            );
        }
        _ => panic!("Expected Skip result"),
    }

    println!("\nAll tests passed!");
}
