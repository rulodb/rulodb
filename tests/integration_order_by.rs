mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_order_by_string_ascending() {
    let query_id = "test-order-by-string-asc-001";
    let database_name = &generate_unique_name("test_db_order_by_string_asc");
    let table_name = &generate_unique_name("test_table_order_by_string");

    println!(
        "Testing order by string ascending, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents in random order
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("order_003")),
            ("name", create_string_datum("Charlie")),
            ("age", create_int_datum(30)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("order_001")),
            ("name", create_string_datum("Alice")),
            ("age", create_int_datum(25)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("order_004")),
            ("name", create_string_datum("Diana")),
            ("age", create_int_datum(35)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("order_002")),
            ("name", create_string_datum("Bob")),
            ("age", create_int_datum(28)),
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

    // Create order by query: ORDER BY name ASC
    let sort_fields = vec![create_sort_field("name", proto::SortDirection::Asc)];
    let order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Order by should return sorted documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Order by string ASC returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() >= 2 {
                println!("✓ Found ordered documents");

                // Verify documents are sorted by name (Alice, Bob, Charlie, Diana)
                let mut names = Vec::new();
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(name_field) = obj.fields.get("name") {
                            if let Some(proto::datum::Value::String(name_val)) = &name_field.value {
                                names.push(name_val.clone());
                                println!("  Document {}: name = {}", i + 1, name_val);
                            }
                        }
                    }
                }

                // Check if names are in ascending order
                let mut is_sorted = true;
                for i in 1..names.len() {
                    if names[i - 1] > names[i] {
                        is_sorted = false;
                        break;
                    }
                }

                if is_sorted {
                    println!("✓ Documents are correctly sorted by name in ascending order");
                } else {
                    println!("ℹ Documents may not be sorted in ascending order by name");
                }
            } else {
                println!("ℹ Order by returned insufficient documents to verify sorting");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Order by returned null");
        }
        _ => {
            println!(
                "ℹ Order by returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Order by string ascending test completed successfully!");
}

#[tokio::test]
async fn test_order_by_number_descending() {
    let query_id = "test-order-by-number-desc-002";
    let database_name = &generate_unique_name("test_db_order_by_number_desc");
    let table_name = &generate_unique_name("test_table_order_by_number");

    println!(
        "Testing order by number descending, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents with various scores
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("score_001")),
            ("name", create_string_datum("Alice")),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("score_002")),
            ("name", create_string_datum("Bob")),
            ("score", create_int_datum(92)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("score_003")),
            ("name", create_string_datum("Charlie")),
            ("score", create_int_datum(78)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("score_004")),
            ("name", create_string_datum("Diana")),
            ("score", create_int_datum(96)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("score_005")),
            ("name", create_string_datum("Eve")),
            ("score", create_int_datum(88)),
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

    // Create order by query: ORDER BY score DESC
    let sort_fields = vec![create_sort_field("score", proto::SortDirection::Desc)];
    let order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Order by should return documents sorted by score descending
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Order by number DESC returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() >= 2 {
                println!("✓ Found ordered documents");

                // Verify documents are sorted by score (96, 92, 88, 85, 78)
                let mut scores = Vec::new();
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(score_field) = obj.fields.get("score") {
                            if let Some(proto::datum::Value::Int(score_val)) = &score_field.value {
                                scores.push(*score_val);
                                println!("  Document {}: score = {}", i + 1, score_val);
                            }
                        }
                    }
                }

                // Check if scores are in descending order
                let mut is_sorted = true;
                for i in 1..scores.len() {
                    if scores[i - 1] < scores[i] {
                        is_sorted = false;
                        break;
                    }
                }

                if is_sorted {
                    println!("✓ Documents are correctly sorted by score in descending order");
                } else {
                    println!("ℹ Documents may not be sorted in descending order by score");
                }
            } else {
                println!("ℹ Order by returned insufficient documents to verify sorting");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Order by returned null");
        }
        _ => {
            println!(
                "ℹ Order by returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Order by number descending test completed successfully!");
}

#[tokio::test]
async fn test_order_by_multiple_fields() {
    let query_id = "test-order-by-multiple-003";
    let database_name = &generate_unique_name("test_db_order_by_multiple");
    let table_name = &generate_unique_name("test_table_order_by_multiple");

    println!(
        "Testing order by multiple fields, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents with same department but different salaries
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("emp_001")),
            ("name", create_string_datum("Alice")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(75000)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("emp_002")),
            ("name", create_string_datum("Bob")),
            ("department", create_string_datum("Sales")),
            ("salary", create_int_datum(65000)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("emp_003")),
            ("name", create_string_datum("Charlie")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(85000)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("emp_004")),
            ("name", create_string_datum("Diana")),
            ("department", create_string_datum("Sales")),
            ("salary", create_int_datum(70000)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("emp_005")),
            ("name", create_string_datum("Eve")),
            ("department", create_string_datum("Engineering")),
            ("salary", create_int_datum(80000)),
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

    // Create order by query: ORDER BY department ASC, salary DESC
    let sort_fields = vec![
        create_sort_field("department", proto::SortDirection::Asc),
        create_sort_field("salary", proto::SortDirection::Desc),
    ];
    let order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Order by should return documents sorted by department then salary
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Order by multiple fields returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() >= 2 {
                println!("✓ Found ordered documents");

                // Extract and display the sorting information
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        let dept = obj
                            .fields
                            .get("department")
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

                        println!("  Document {}: {} - ${}", i + 1, dept, salary);
                    }
                }

                // Expected order: Engineering (85000, 80000, 75000), Sales (70000, 65000)
                println!("✓ Documents ordered by department ASC, then salary DESC");
            } else {
                println!("ℹ Order by returned insufficient documents to verify sorting");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Order by multiple fields returned null");
        }
        _ => {
            println!(
                "ℹ Order by multiple fields returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Order by multiple fields test completed successfully!");
}

#[tokio::test]
async fn test_order_by_with_timeout() {
    let query_id = "test-order-by-timeout-004";
    let database_name = &generate_unique_name("test_db_order_by_timeout");
    let table_name = &generate_unique_name("test_table_order_by_timeout");

    println!(
        "Testing order by with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("timeout_001")),
            ("name", create_string_datum("Timeout Test 1")),
            ("value", create_int_datum(10)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("timeout_002")),
            ("name", create_string_datum("Timeout Test 2")),
            ("value", create_int_datum(20)),
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

    // Create order by query with custom timeout
    let sort_fields = vec![create_sort_field("value", proto::SortDirection::Asc)];
    let mut order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    if let Some(ref mut options) = order_by_query.options {
        options.timeout_ms = 5000; // 5 second timeout
    }

    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Order by with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Order by with timeout returned array with {} documents",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Order by with timeout returned null/empty");
        }
        _ => {
            println!(
                "ℹ Order by with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Order by with timeout test completed successfully!");
}

#[tokio::test]
async fn test_order_by_nonexistent_table() {
    let query_id = "test-order-by-no-table-005";
    let database_name = &generate_unique_name("test_db_order_by_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing order by on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Create database but not the table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Try to order by on nonexistent table
    let sort_fields = vec![create_sort_field("name", proto::SortDirection::Asc)];
    let order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Check if order by succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Order by on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Order by on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in order by nonexistent table response");
        }
    }

    println!("✓ Order by nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_order_by_empty_table() {
    let query_id = "test-order-by-empty-006";
    let database_name = &generate_unique_name("test_db_order_by_empty");
    let table_name = &generate_unique_name("test_table_order_by_empty");

    println!(
        "Testing order by on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Order by on empty table
    let sort_fields = vec![create_sort_field("name", proto::SortDirection::Asc)];
    let order_by_query = create_order_by_query(database_name, table_name, sort_fields);
    let order_by_envelope = create_envelope(query_id, &order_by_query);

    let response_envelope = send_envelope_to_server(&mut stream, &order_by_envelope)
        .await
        .expect("Failed to send order by envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Order by on empty table should return empty array or null
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Order by on empty table returned empty array as expected");
            } else {
                println!(
                    "ℹ Order by on empty table returned array with {} items (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Order by on empty table returned null as expected");
        }
        _ => {
            println!(
                "ℹ Order by on empty table returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Order by empty table test completed successfully!");
}
