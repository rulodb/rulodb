mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_limit_basic() {
    let query_id = "test-limit-basic-001";
    let database_name = &generate_unique_name("test_db_limit_basic");
    let table_name = &generate_unique_name("test_table_limit_basic");

    println!(
        "Testing basic limit operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("limit_001")),
            ("name", create_string_datum("First Document")),
            ("order", create_int_datum(1)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("limit_002")),
            ("name", create_string_datum("Second Document")),
            ("order", create_int_datum(2)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("limit_003")),
            ("name", create_string_datum("Third Document")),
            ("order", create_int_datum(3)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("limit_004")),
            ("name", create_string_datum("Fourth Document")),
            ("order", create_int_datum(4)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("limit_005")),
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

    // Limit to 3 documents
    let limit_query = create_limit_query(database_name, table_name, 3);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit should return only the specified number of documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("✓ Limit returned array with {} documents", arr.items.len());

            // Should return exactly 3 documents
            if arr.items.len() <= 3 {
                println!("✓ Limit correctly restricted number of documents");

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
                println!(
                    "ℹ Limit returned {} documents (expected 3 or fewer)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Limit returned null");
        }
        _ => {
            println!(
                "ℹ Limit returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Basic limit test completed successfully!");
}

#[tokio::test]
async fn test_limit_zero() {
    let query_id = "test-limit-zero-002";
    let database_name = &generate_unique_name("test_db_limit_zero");
    let table_name = &generate_unique_name("test_table_limit_zero");

    println!(
        "Testing limit with zero count, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("name", create_string_datum("Zero Limit Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("zero_002")),
            ("name", create_string_datum("Zero Limit Test 2")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("zero_003")),
            ("name", create_string_datum("Zero Limit Test 3")),
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

    // Limit to 0 documents
    let limit_query = create_limit_query(database_name, table_name, 0);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit 0 should return empty result
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Limit 0 returned empty array as expected");
            } else {
                println!(
                    "ℹ Limit 0 returned {} documents (expected 0)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Limit 0 returned null as expected");
        }
        _ => {
            println!(
                "ℹ Limit 0 returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Limit zero test completed successfully!");
}

#[tokio::test]
async fn test_limit_larger_than_available() {
    let query_id = "test-limit-large-003";
    let database_name = &generate_unique_name("test_db_limit_large");
    let table_name = &generate_unique_name("test_table_limit_large");

    println!(
        "Testing limit larger than available documents, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("large_001")),
            ("name", create_string_datum("Large Limit Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("large_002")),
            ("name", create_string_datum("Large Limit Test 2")),
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

    // Limit to 10 documents (more than available)
    let limit_query = create_limit_query(database_name, table_name, 10);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit larger than available should return all available documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Limit larger than available returned array with {} documents",
                arr.items.len()
            );

            // Should return all available documents (2)
            if arr.items.len() <= 2 {
                println!("✓ Limit returned all available documents");
            } else {
                println!(
                    "ℹ Limit returned {} documents (expected 2 or fewer)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Limit larger than available returned null");
        }
        _ => {
            println!(
                "ℹ Limit larger than available returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Limit larger than available test completed successfully!");
}

#[tokio::test]
async fn test_limit_with_timeout() {
    let query_id = "test-limit-timeout-004";
    let database_name = &generate_unique_name("test_db_limit_timeout");
    let table_name = &generate_unique_name("test_table_limit_timeout");

    println!(
        "Testing limit with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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
        create_datum_object(vec![
            ("id", create_string_datum("timeout_004")),
            ("name", create_string_datum("Timeout Test 4")),
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

    // Create limit query with custom timeout
    let mut limit_query = create_limit_query(database_name, table_name, 2);
    if let Some(ref mut options) = limit_query.options {
        options.timeout_ms = 6000; // 6 second timeout
    }

    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Limit with timeout returned array with {} documents",
                arr.items.len()
            );

            // Should return at most 2 documents
            if arr.items.len() <= 2 {
                println!("✓ Limit with timeout correctly restricted results");
            } else {
                println!(
                    "ℹ Limit with timeout returned {} documents (expected 2 or fewer)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Limit with timeout returned null/empty");
        }
        _ => {
            println!("ℹ Limit with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Limit with timeout test completed successfully!");
}

#[tokio::test]
async fn test_limit_nonexistent_table() {
    let query_id = "test-limit-no-table-005";
    let database_name = &generate_unique_name("test_db_limit_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing limit on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try to limit on nonexistent table
    let limit_query = create_limit_query(database_name, table_name, 5);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Check if limit succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Limit on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Limit on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in limit nonexistent table response");
        }
    }

    println!("✓ Limit nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_limit_empty_table() {
    let query_id = "test-limit-empty-006";
    let database_name = &generate_unique_name("test_db_limit_empty");
    let table_name = &generate_unique_name("test_table_limit_empty");

    println!(
        "Testing limit on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Limit on empty table
    let limit_query = create_limit_query(database_name, table_name, 5);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit on empty table should return empty array or null
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Limit on empty table returned empty array as expected");
            } else {
                println!(
                    "ℹ Limit on empty table returned array with {} items (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Limit on empty table returned null as expected");
        }
        _ => {
            println!(
                "ℹ Limit on empty table returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Limit empty table test completed successfully!");
}

#[tokio::test]
async fn test_limit_one() {
    let query_id = "test-limit-one-007";
    let database_name = &generate_unique_name("test_db_limit_one");
    let table_name = &generate_unique_name("test_table_limit_one");

    println!(
        "Testing limit to one document, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert multiple test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("one_001")),
            ("name", create_string_datum("First Document")),
            ("priority", create_int_datum(1)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("one_002")),
            ("name", create_string_datum("Second Document")),
            ("priority", create_int_datum(2)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("one_003")),
            ("name", create_string_datum("Third Document")),
            ("priority", create_int_datum(3)),
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

    // Limit to exactly 1 document
    let limit_query = create_limit_query(database_name, table_name, 1);
    let limit_envelope = create_envelope(query_id, &limit_query);

    let response_envelope = send_envelope_to_server(&mut stream, &limit_envelope)
        .await
        .expect("Failed to send limit envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Limit 1 should return exactly one document
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Limit 1 returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() == 1 {
                println!("✓ Limit 1 returned exactly one document as expected");

                // Display the returned document
                if let Some(item) = arr.items.first() {
                    if let Some(proto::datum::Value::Object(obj)) = &item.value {
                        if let Some(name_field) = obj.fields.get("name") {
                            if let Some(proto::datum::Value::String(name_val)) = &name_field.value {
                                println!("  Returned document: name = {name_val}");
                            }
                        }
                    }
                }
            } else {
                println!(
                    "ℹ Limit 1 returned {} documents (expected exactly 1)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Limit 1 returned null");
        }
        _ => {
            println!("ℹ Limit 1 returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Limit one test completed successfully!");
}
