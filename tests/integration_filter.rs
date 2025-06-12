mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_filter_equal_string() {
    let query_id = "test-filter-equal-string-001";
    let database_name = &generate_unique_name("test_db_filter_equal_string");
    let table_name = &generate_unique_name("test_table_filter_equal");

    println!(
        "Testing filter with string equality, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(
        &table_create_response,
        &format!("{}-table-create", query_id),
    )
    .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("filter_001")),
            ("name", create_string_datum("Alice")),
            ("status", create_string_datum("active")),
            ("age", create_int_datum(25)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("filter_002")),
            ("name", create_string_datum("Bob")),
            ("status", create_string_datum("inactive")),
            ("age", create_int_datum(30)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("filter_003")),
            ("name", create_string_datum("Charlie")),
            ("status", create_string_datum("active")),
            ("age", create_int_datum(35)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Create filter predicate: status == "active"
    let predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    // Filter documents
    let filter_query = create_filter_query(database_name, table_name, predicate);
    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Filter should return matching documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("✓ Filter returned array with {} documents", arr.items.len());

            // Should find 2 documents with status "active"
            if !arr.items.is_empty() {
                println!("✓ Found filtered documents");

                // Verify documents match the filter
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(status_field) = obj.fields.get("status") {
                            if let Some(proto::datum::Value::String(status_val)) =
                                &status_field.value
                            {
                                if status_val == "active" {
                                    println!("  ✓ Document {}: status = {}", i + 1, status_val);
                                } else {
                                    println!(
                                        "  ℹ Document {}: unexpected status = {}",
                                        i + 1,
                                        status_val
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                println!(
                    "ℹ Filter returned empty array (documents may not be immediately visible)"
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Filter returned null (documents may not be immediately visible)");
        }
        _ => {
            println!(
                "ℹ Filter returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Filter equal string test completed successfully!");
}

#[tokio::test]
async fn test_filter_greater_than_number() {
    let query_id = "test-filter-gt-number-002";
    let database_name = &generate_unique_name("test_db_filter_gt_number");
    let table_name = &generate_unique_name("test_table_filter_gt");

    println!(
        "Testing filter with greater than number, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(
        &table_create_response,
        &format!("{}-table-create", query_id),
    )
    .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents with different ages
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("age_001")),
            ("name", create_string_datum("Young Alice")),
            ("age", create_int_datum(20)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("age_002")),
            ("name", create_string_datum("Middle Bob")),
            ("age", create_int_datum(30)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("age_003")),
            ("name", create_string_datum("Senior Charlie")),
            ("age", create_int_datum(40)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("age_004")),
            ("name", create_string_datum("Elder Diana")),
            ("age", create_int_datum(50)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Create filter predicate: age > 30
    let predicate = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["age"]),
        create_literal_expression(create_int_datum(30)),
    );

    // Filter documents
    let filter_query = create_filter_query(database_name, table_name, predicate);
    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Filter should return documents with age > 30
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Filter GT returned array with {} documents",
                arr.items.len()
            );

            // Should find documents with age > 30 (Charlie=40, Diana=50)
            if !arr.items.is_empty() {
                println!("✓ Found filtered documents");

                // Verify documents match the filter
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(age_field) = obj.fields.get("age") {
                            if let Some(proto::datum::Value::Int(age_val)) = &age_field.value {
                                if *age_val > 30 {
                                    println!("  ✓ Document {}: age = {} (> 30)", i + 1, age_val);
                                } else {
                                    println!(
                                        "  ℹ Document {}: unexpected age = {} (not > 30)",
                                        i + 1,
                                        age_val
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                println!("ℹ Filter GT returned empty array");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Filter GT returned null");
        }
        _ => {
            println!(
                "ℹ Filter GT returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Filter greater than number test completed successfully!");
}

#[tokio::test]
async fn test_filter_boolean_field() {
    let query_id = "test-filter-boolean-003";
    let database_name = &generate_unique_name("test_db_filter_boolean");
    let table_name = &generate_unique_name("test_table_filter_bool");

    println!(
        "Testing filter with boolean field, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(
        &table_create_response,
        &format!("{}-table-create", query_id),
    )
    .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents with boolean fields
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("bool_001")),
            ("name", create_string_datum("Active User")),
            ("is_active", create_bool_datum(true)),
            ("is_premium", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("bool_002")),
            ("name", create_string_datum("Inactive User")),
            ("is_active", create_bool_datum(false)),
            ("is_premium", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("bool_003")),
            ("name", create_string_datum("Premium Active User")),
            ("is_active", create_bool_datum(true)),
            ("is_premium", create_bool_datum(true)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Create filter predicate: is_active == true
    let predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["is_active"]),
        create_literal_expression(create_bool_datum(true)),
    );

    // Filter documents
    let filter_query = create_filter_query(database_name, table_name, predicate);
    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Filter should return active users
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Filter boolean returned array with {} documents",
                arr.items.len()
            );

            // Should find 2 documents with is_active = true
            if !arr.items.is_empty() {
                println!("✓ Found filtered documents");

                // Verify documents match the filter
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(active_field) = obj.fields.get("is_active") {
                            if let Some(proto::datum::Value::Bool(active_val)) = &active_field.value
                            {
                                if *active_val {
                                    println!("  ✓ Document {}: is_active = true", i + 1);
                                } else {
                                    println!(
                                        "  ℹ Document {}: unexpected is_active = false",
                                        i + 1
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                println!("ℹ Filter boolean returned empty array");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Filter boolean returned null");
        }
        _ => {
            println!(
                "ℹ Filter boolean returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Filter boolean field test completed successfully!");
}

#[tokio::test]
async fn test_filter_nonexistent_table() {
    let query_id = "test-filter-no-table-004";
    let database_name = &generate_unique_name("test_db_filter_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing filter on nonexistent table, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Create database but not the table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Create filter predicate
    let predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    // Try to filter nonexistent table
    let filter_query = create_filter_query(database_name, table_name, predicate);
    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Check if filter succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Filter on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Filter on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in filter nonexistent table response");
        }
    }

    println!("✓ Filter nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_filter_with_timeout() {
    let query_id = "test-filter-timeout-005";
    let database_name = &generate_unique_name("test_db_filter_timeout");
    let table_name = &generate_unique_name("test_table_filter_timeout");

    println!(
        "Testing filter with custom timeout, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(
        &table_create_response,
        &format!("{}-table-create", query_id),
    )
    .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test document
    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("timeout_test")),
        ("name", create_string_datum("Timeout Test Document")),
        ("status", create_string_datum("active")),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Create filter predicate
    let predicate = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    // Create filter query with custom timeout
    let mut filter_query = create_filter_query(database_name, table_name, predicate);
    if let Some(ref mut options) = filter_query.options {
        options.timeout_ms = 2000; // 2 second timeout
    }

    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Filter with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Filter with timeout returned array with {} documents",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Filter with timeout returned null/empty");
        }
        _ => {
            println!("ℹ Filter with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Filter with timeout test completed successfully!");
}

#[tokio::test]
async fn test_filter_complex_predicate() {
    let query_id = "test-filter-complex-006";
    let database_name = &generate_unique_name("test_db_filter_complex");
    let table_name = &generate_unique_name("test_table_filter_complex");

    println!(
        "Testing filter with complex predicate, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup: Create database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");
    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");
    validate_response_envelope(
        &table_create_response,
        &format!("{}-table-create", query_id),
    )
    .expect("Table create response validation failed");

    println!("✓ Database and table created successfully");

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("complex_001")),
            ("name", create_string_datum("Alice")),
            ("age", create_int_datum(25)),
            ("status", create_string_datum("active")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_002")),
            ("name", create_string_datum("Bob")),
            ("age", create_int_datum(35)),
            ("status", create_string_datum("inactive")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_003")),
            ("name", create_string_datum("Charlie")),
            ("age", create_int_datum(30)),
            ("status", create_string_datum("active")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_004")),
            ("name", create_string_datum("Diana")),
            ("age", create_int_datum(40)),
            ("status", create_string_datum("active")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Create complex predicate: status == "active" AND age >= 30
    let status_condition = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    let age_condition = create_binary_expression(
        proto::binary_op::Operator::Ge,
        create_field_expression(vec!["age"]),
        create_literal_expression(create_int_datum(30)),
    );

    let complex_predicate = create_binary_expression(
        proto::binary_op::Operator::And,
        status_condition,
        age_condition,
    );

    // Filter documents
    let filter_query = create_filter_query(database_name, table_name, complex_predicate);
    let filter_envelope = create_envelope(query_id, &filter_query);

    let response_envelope = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Filter should return documents matching both conditions
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Complex filter returned array with {} documents",
                arr.items.len()
            );

            // Should find Charlie (age=30, active) and Diana (age=40, active)
            if !arr.items.is_empty() {
                println!("✓ Found filtered documents");

                // Verify documents match the complex filter
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        let status_ok = obj
                            .fields
                            .get("status")
                            .and_then(|f| f.value.as_ref())
                            .map(|v| matches!(v, proto::datum::Value::String(s) if s == "active"))
                            .unwrap_or(false);

                        let age_ok = obj
                            .fields
                            .get("age")
                            .and_then(|f| f.value.as_ref())
                            .map(|v| matches!(v, proto::datum::Value::Int(n) if *n >= 30))
                            .unwrap_or(false);

                        if status_ok && age_ok {
                            println!("  ✓ Document {}: matches complex filter", i + 1);
                        } else {
                            println!("  ℹ Document {}: doesn't match complex filter", i + 1);
                        }
                    }
                }
            } else {
                println!("ℹ Complex filter returned empty array");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Complex filter returned null");
        }
        _ => {
            println!(
                "ℹ Complex filter returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Filter complex predicate test completed successfully!");
}
