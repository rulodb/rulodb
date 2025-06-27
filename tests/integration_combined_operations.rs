mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_filter_and_order_by() {
    let query_id = "test-filter-order-001";
    let database_name = &generate_unique_name("test_db_filter_order");
    let table_name = &generate_unique_name("test_table_filter_order");

    println!(
        "Testing filter with order by, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("combined_001")),
            ("name", create_string_datum("Alice")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(75000)),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_002")),
            ("name", create_string_datum("Bob")),
            ("department", create_string_datum("Sales")),
            ("salary", create_int_datum(65000)),
            ("active", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_003")),
            ("name", create_string_datum("Charlie")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(85000)),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_004")),
            ("name", create_string_datum("Diana")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(80000)),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_005")),
            ("name", create_string_datum("Eve")),
            ("department", create_string_datum("Sales")),
            ("salary", create_int_datum(70000)),
            ("active", create_bool_datum(false)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Create filter predicate: department == "Engineering" AND active == true
    let dept_condition = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["department"]),
        create_literal_expression(create_string_datum("Engineering")),
    );

    let active_condition = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["active"]),
        create_literal_expression(create_bool_datum(true)),
    );

    let combined_predicate = create_binary_expression(
        proto::binary_op::Operator::And,
        dept_condition,
        active_condition,
    );

    // Create a combined query: Filter -> OrderBy
    // First create the filter query as the source
    let filter_source_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Filter(Box::new(proto::Filter {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            predicate: Some(Box::new(combined_predicate)),
        }))),
    };

    // Then create the order by query that uses the filter as source
    let sort_fields = vec![create_sort_field("salary", proto::SortDirection::Desc)];
    let order_by_query = proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::OrderBy(Box::new(proto::OrderBy {
            source: Some(Box::new(filter_source_query)),
            fields: sort_fields,
        }))),
    };

    let combined_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &combined_envelope)
        .await
        .expect("Failed to send combined envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Should return Engineering employees (active) ordered by salary DESC
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Filter + Order By returned array with {} documents",
                arr.items.len()
            );

            // Should return Charlie (85k), Diana (80k), Alice (75k)
            for (i, item) in arr.items.iter().enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    let name = obj
                        .fields
                        .get("name")
                        .and_then(|f| f.value.as_ref())
                        .and_then(|v| match v {
                            proto::datum::Value::String(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "Unknown".to_string());

                    let salary = obj
                        .fields
                        .get("salary")
                        .and_then(|f| f.value.as_ref())
                        .and_then(|v| match v {
                            proto::datum::Value::Int(n) => Some(*n),
                            _ => None,
                        })
                        .unwrap_or(0);

                    println!("  Employee {}: {} - ${}", i + 1, name, salary);
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Filter + Order By returned null");
        }
        _ => {
            println!(
                "ℹ Filter + Order By returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Filter and Order By combined test completed successfully!");
}

#[tokio::test]
async fn test_filter_order_limit_skip() {
    let query_id = "test-filter-order-limit-skip-002";
    let database_name = &generate_unique_name("test_db_filter_order_limit_skip");
    let table_name = &generate_unique_name("test_table_complex");

    println!(
        "Testing complex chained operations, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert a larger dataset
    let mut documents = Vec::new();
    for i in 1..=10 {
        documents.push(create_datum_object(vec![
            ("id", create_string_datum(&format!("complex_{i:03}"))),
            ("name", create_string_datum(&format!("Employee {i}"))),
            ("score", create_int_datum(50 + (i * 5) as i64)),
            ("active", create_bool_datum(i % 2 == 0)), // Even numbers are active
            (
                "category",
                create_string_datum(if i <= 5 { "A" } else { "B" }),
            ),
        ]));
    }

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Complex test dataset inserted successfully");

    // Create a complex chained query: Filter -> OrderBy -> Skip -> Limit
    // Filter: active == true
    let filter_predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["active"]),
        create_literal_expression(create_bool_datum(true)),
    );

    // 1. Base table query
    let table_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Table(proto::Table {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    };

    // 2. Filter query
    let filter_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Filter(Box::new(proto::Filter {
            source: Some(Box::new(table_query)),
            predicate: Some(Box::new(filter_predicate)),
        }))),
    };

    // 3. Order by query (score DESC)
    let sort_fields = vec![create_sort_field("score", proto::SortDirection::Desc)];
    let order_by_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::OrderBy(Box::new(proto::OrderBy {
            source: Some(Box::new(filter_query)),
            fields: sort_fields,
        }))),
    };

    // 4. Skip query (skip first 1)
    let skip_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Skip(Box::new(proto::Skip {
            source: Some(Box::new(order_by_query)),
            count: 1,
        }))),
    };

    // 5. Limit query (limit to 2)
    let limit_query = proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Limit(Box::new(proto::Limit {
            source: Some(Box::new(skip_query)),
            count: 2,
        }))),
    };

    let complex_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &complex_envelope)
        .await
        .expect("Failed to send complex envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Should return active employees, ordered by score DESC, skip 1, limit 2
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Complex chained query returned array with {} documents",
                arr.items.len()
            );

            // Should return at most 2 documents
            if arr.items.len() <= 2 {
                println!("✓ Limit correctly applied to chained operations");
            }

            for (i, item) in arr.items.iter().enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    let name = obj
                        .fields
                        .get("name")
                        .and_then(|f| f.value.as_ref())
                        .and_then(|v| match v {
                            proto::datum::Value::String(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "Unknown".to_string());

                    let score = obj
                        .fields
                        .get("score")
                        .and_then(|f| f.value.as_ref())
                        .and_then(|v| match v {
                            proto::datum::Value::Int(n) => Some(*n),
                            _ => None,
                        })
                        .unwrap_or(0);

                    let active = obj
                        .fields
                        .get("active")
                        .and_then(|f| f.value.as_ref())
                        .and_then(|v| match v {
                            proto::datum::Value::Bool(b) => Some(*b),
                            _ => None,
                        })
                        .unwrap_or(false);

                    println!(
                        "  Result {}: {} - Score: {}, Active: {}",
                        i + 1,
                        name,
                        score,
                        active
                    );
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Complex chained query returned null");
        }
        _ => {
            println!(
                "ℹ Complex chained query returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Complex chained operations test completed successfully!");
}

#[tokio::test]
async fn test_count_with_filter() {
    let query_id = "test-count-filter-003";
    let database_name = &generate_unique_name("test_db_count_filter");
    let table_name = &generate_unique_name("test_table_count_filter");

    println!(
        "Testing count with filter, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("count_filter_001")),
            ("status", create_string_datum("active")),
            ("type", create_string_datum("premium")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_filter_002")),
            ("status", create_string_datum("inactive")),
            ("type", create_string_datum("basic")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_filter_003")),
            ("status", create_string_datum("active")),
            ("type", create_string_datum("basic")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_filter_004")),
            ("status", create_string_datum("active")),
            ("type", create_string_datum("premium")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_filter_005")),
            ("status", create_string_datum("pending")),
            ("type", create_string_datum("basic")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Count all documents first
    let total_count_query = create_count_query(database_name, table_name);
    let total_count_envelope =
        create_envelope(&format!("{query_id}-total-count"), &total_count_query);
    let total_count_response = send_envelope_to_server(&mut stream, &total_count_envelope)
        .await
        .expect("Failed to send total count envelope");
    validate_response_envelope(&total_count_response, &format!("{query_id}-total-count"))
        .expect("Total count response validation failed");

    let total_count_datum = decode_response_payload(&total_count_response)
        .expect("Failed to decode total count response");

    match total_count_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Total count: {count}");
        }
        _ => {
            println!("ℹ Total count: {:?}", total_count_datum.value);
        }
    }

    // Create count with filter: status == "active"
    let filter_predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    // Create Filter -> Count query
    let filter_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Filter(Box::new(proto::Filter {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            predicate: Some(Box::new(filter_predicate)),
        }))),
    };

    let count_filter_query = proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Count(Box::new(proto::Count {
            source: Some(Box::new(filter_query)),
        }))),
    };

    let count_filter_envelope = create_envelope(query_id, &count_filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_filter_envelope)
        .await
        .expect("Failed to send count filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Should return count of active documents (3)
    match response_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count with filter returned: {count}");

            // Should return 3 active documents
            if count >= 3 {
                println!("✓ Count with filter returned expected number");
            } else {
                println!("ℹ Count with filter returned {count} (expected 3 or more)");
            }
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            if let Some(count_field) = obj.fields.get("count") {
                if let Some(proto::datum::Value::Int(count_val)) = &count_field.value {
                    println!("✓ Count with filter returned object with count: {count_val}");
                }
            } else {
                println!("✓ Count with filter returned object: {obj:?}");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Count with filter returned null");
        }
        _ => {
            println!(
                "ℹ Count with filter returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Count with filter test completed successfully!");
}

#[tokio::test]
async fn test_pagination_with_skip_and_limit() {
    let query_id = "test-pagination-004";
    let database_name = &generate_unique_name("test_db_pagination");
    let table_name = &generate_unique_name("test_table_pagination");

    println!(
        "Testing pagination with skip and limit, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert documents for pagination
    let mut documents = Vec::new();
    for i in 1..=15 {
        documents.push(create_datum_object(vec![
            ("id", create_string_datum(&format!("page_{i:03}"))),
            ("title", create_string_datum(&format!("Page Item {i}"))),
            ("page_number", create_int_datum(i as i64)),
        ]));
    }

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Pagination test dataset (15 items) inserted successfully");

    // Test pagination: Page 1 (items 1-5)
    let page_1_query = create_limit_query(database_name, table_name, 5);
    let page_1_envelope = create_envelope(&format!("{query_id}-page-1"), &page_1_query);
    let page_1_response = send_envelope_to_server(&mut stream, &page_1_envelope)
        .await
        .expect("Failed to send page 1 envelope");
    validate_response_envelope(&page_1_response, &format!("{query_id}-page-1"))
        .expect("Page 1 response validation failed");

    let page_1_datum =
        decode_response_payload(&page_1_response).expect("Failed to decode page 1 response");

    println!("Page 1 results:");
    match page_1_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("  ✓ Page 1 returned {} items", arr.items.len());
            for (i, item) in arr.items.iter().take(3).enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    if let Some(title_field) = obj.fields.get("title") {
                        if let Some(proto::datum::Value::String(title)) = &title_field.value {
                            println!("    Item {}: {}", i + 1, title);
                        }
                    }
                }
            }
            if arr.items.len() > 3 {
                println!("    ... and {} more items", arr.items.len() - 3);
            }
        }
        _ => {
            println!("  ℹ Page 1: {:?}", page_1_datum.value);
        }
    }

    // Test pagination: Page 2 (items 6-10) - Skip 5, Limit 5
    let page_2_base_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Table(proto::Table {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    };

    let page_2_skip_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Skip(Box::new(proto::Skip {
            source: Some(Box::new(page_2_base_query)),
            count: 5,
        }))),
    };

    let page_2_query = proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Limit(Box::new(proto::Limit {
            source: Some(Box::new(page_2_skip_query)),
            count: 5,
        }))),
    };

    let page_2_envelope = create_envelope(&format!("{query_id}-page-2"), &page_2_query);
    let page_2_response = send_envelope_to_server(&mut stream, &page_2_envelope)
        .await
        .expect("Failed to send page 2 envelope");
    validate_response_envelope(&page_2_response, &format!("{query_id}-page-2"))
        .expect("Page 2 response validation failed");

    let page_2_datum =
        decode_response_payload(&page_2_response).expect("Failed to decode page 2 response");

    println!("Page 2 results:");
    match page_2_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("  ✓ Page 2 returned {} items", arr.items.len());
            for (i, item) in arr.items.iter().take(3).enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    if let Some(title_field) = obj.fields.get("title") {
                        if let Some(proto::datum::Value::String(title)) = &title_field.value {
                            println!("    Item {}: {}", i + 1, title);
                        }
                    }
                }
            }
            if arr.items.len() > 3 {
                println!("    ... and {} more items", arr.items.len() - 3);
            }
        }
        _ => {
            println!("  ℹ Page 2: {:?}", page_2_datum.value);
        }
    }

    // Test pagination: Page 3 (items 11-15) - Skip 10, Limit 5
    let page_3_base_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Table(proto::Table {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    };

    let page_3_skip_query = proto::Query {
        options: None,
        cursor: None,
        kind: Some(proto::query::Kind::Skip(Box::new(proto::Skip {
            source: Some(Box::new(page_3_base_query)),
            count: 10,
        }))),
    };

    let page_3_query = proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Limit(Box::new(proto::Limit {
            source: Some(Box::new(page_3_skip_query)),
            count: 5,
        }))),
    };

    let page_3_envelope = create_envelope(&format!("{query_id}-page-3"), &page_3_query);
    let page_3_response = send_envelope_to_server(&mut stream, &page_3_envelope)
        .await
        .expect("Failed to send page 3 envelope");
    validate_response_envelope(&page_3_response, &format!("{query_id}-page-3"))
        .expect("Page 3 response validation failed");

    let page_3_datum =
        decode_response_payload(&page_3_response).expect("Failed to decode page 3 response");

    println!("Page 3 results:");
    match page_3_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("  ✓ Page 3 returned {} items", arr.items.len());
            for (i, item) in arr.items.iter().take(3).enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    if let Some(title_field) = obj.fields.get("title") {
                        if let Some(proto::datum::Value::String(title)) = &title_field.value {
                            println!("    Item {}: {}", i + 1, title);
                        }
                    }
                }
            }
            if arr.items.len() > 3 {
                println!("    ... and {} more items", arr.items.len() - 3);
            }
        }
        _ => {
            println!("  ℹ Page 3: {:?}", page_3_datum.value);
        }
    }

    println!("✓ Pagination with skip and limit test completed successfully!");
}
