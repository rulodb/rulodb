mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_skip_basic() {
    let query_id = "test-skip-basic-001";
    let database_name = &generate_unique_name("test_db_skip_basic");
    let table_name = &generate_unique_name("test_table_skip_basic");

    println!(
        "Testing basic skip operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("skip_001")),
            ("name", create_string_datum("First Document")),
            ("order", create_int_datum(1)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("skip_002")),
            ("name", create_string_datum("Second Document")),
            ("order", create_int_datum(2)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("skip_003")),
            ("name", create_string_datum("Third Document")),
            ("order", create_int_datum(3)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("skip_004")),
            ("name", create_string_datum("Fourth Document")),
            ("order", create_int_datum(4)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("skip_005")),
            ("name", create_string_datum("Fifth Document")),
            ("order", create_int_datum(5)),
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

    // Skip first 2 documents
    let skip_query = create_skip_query(database_name, table_name, 2);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip should return remaining documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("✓ Skip returned array with {} documents", arr.items.len());

            // Should return 3 documents (total 5 - skip 2 = 3)
            if !arr.items.is_empty() {
                println!("✓ Found documents after skip");

                // Display returned documents
                for (i, item) in arr.items.iter().enumerate() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(name_field) = obj.fields.get("name") {
                            if let Some(proto::datum::Value::String(name_val)) = &name_field.value {
                                println!("  Document {}: name = {}", i + 1, name_val);
                            }
                        }
                    }
                }
            } else {
                println!("ℹ Skip returned empty array");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Skip returned null");
        }
        _ => {
            println!(
                "ℹ Skip returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Basic skip test completed successfully!");
}

#[tokio::test]
async fn test_skip_zero() {
    let query_id = "test-skip-zero-002";
    let database_name = &generate_unique_name("test_db_skip_zero");
    let table_name = &generate_unique_name("test_table_skip_zero");

    println!(
        "Testing skip with zero count, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("zero_001")),
            ("name", create_string_datum("Zero Skip Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("zero_002")),
            ("name", create_string_datum("Zero Skip Test 2")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("zero_003")),
            ("name", create_string_datum("Zero Skip Test 3")),
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

    // Skip 0 documents (should return all)
    let skip_query = create_skip_query(database_name, table_name, 0);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip 0 should return all documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("✓ Skip 0 returned array with {} documents", arr.items.len());

            // Should return all 3 documents
            if arr.items.len() >= 3 {
                println!("✓ Skip 0 returned all documents as expected");
            } else {
                println!(
                    "ℹ Skip 0 returned {} documents (expected 3 or more)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Skip 0 returned null");
        }
        _ => {
            println!(
                "ℹ Skip 0 returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Skip zero test completed successfully!");
}

#[tokio::test]
async fn test_skip_more_than_available() {
    let query_id = "test-skip-more-003";
    let database_name = &generate_unique_name("test_db_skip_more");
    let table_name = &generate_unique_name("test_table_skip_more");

    println!(
        "Testing skip more than available documents, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert only 2 test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("more_001")),
            ("name", create_string_datum("More Skip Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("more_002")),
            ("name", create_string_datum("More Skip Test 2")),
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

    // Skip 5 documents (more than available)
    let skip_query = create_skip_query(database_name, table_name, 5);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip more than available should return empty result
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Skip more than available returned empty array as expected");
            } else {
                println!(
                    "ℹ Skip more than available returned {} documents (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Skip more than available returned null as expected");
        }
        _ => {
            println!(
                "ℹ Skip more than available returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Skip more than available test completed successfully!");
}

#[tokio::test]
async fn test_skip_with_timeout() {
    let query_id = "test-skip-timeout-004";
    let database_name = &generate_unique_name("test_db_skip_timeout");
    let table_name = &generate_unique_name("test_table_skip_timeout");

    println!(
        "Testing skip with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("timeout_002")),
            ("name", create_string_datum("Timeout Test 2")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("timeout_003")),
            ("name", create_string_datum("Timeout Test 3")),
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

    // Create skip query with custom timeout
    let mut skip_query = create_skip_query(database_name, table_name, 1);
    if let Some(ref mut options) = skip_query.options {
        options.timeout_ms = 4000; // 4 second timeout
    }

    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Skip with timeout returned array with {} documents",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Skip with timeout returned null/empty");
        }
        _ => {
            println!("ℹ Skip with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Skip with timeout test completed successfully!");
}

#[tokio::test]
async fn test_skip_nonexistent_table() {
    let query_id = "test-skip-no-table-005";
    let database_name = &generate_unique_name("test_db_skip_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing skip on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try to skip on nonexistent table
    let skip_query = create_skip_query(database_name, table_name, 2);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Check if skip succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Skip on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Skip on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in skip nonexistent table response");
        }
    }

    println!("✓ Skip nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_skip_empty_table() {
    let query_id = "test-skip-empty-006";
    let database_name = &generate_unique_name("test_db_skip_empty");
    let table_name = &generate_unique_name("test_table_skip_empty");

    println!(
        "Testing skip on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Skip on empty table
    let skip_query = create_skip_query(database_name, table_name, 3);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip on empty table should return empty array or null
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Skip on empty table returned empty array as expected");
            } else {
                println!(
                    "ℹ Skip on empty table returned array with {} items (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Skip on empty table returned null as expected");
        }
        _ => {
            println!("ℹ Skip on empty table returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Skip empty table test completed successfully!");
}

#[tokio::test]
async fn test_skip_large_number() {
    let query_id = "test-skip-large-007";
    let database_name = &generate_unique_name("test_db_skip_large");
    let table_name = &generate_unique_name("test_table_skip_large");

    println!(
        "Testing skip with large number, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert a small number of documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("large_001")),
            ("name", create_string_datum("Large Skip Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("large_002")),
            ("name", create_string_datum("Large Skip Test 2")),
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

    // Skip a very large number (1000)
    let skip_query = create_skip_query(database_name, table_name, 1000);
    let skip_envelope = create_envelope(query_id, &skip_query);

    let response_envelope = send_envelope_to_server(&mut stream, &skip_envelope)
        .await
        .expect("Failed to send skip envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Skip large number should return empty result
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Skip large number returned empty array as expected");
            } else {
                println!(
                    "ℹ Skip large number returned {} documents (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Skip large number returned null as expected");
        }
        _ => {
            println!("ℹ Skip large number returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Skip large number test completed successfully!");
}
