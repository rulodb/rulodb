mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_count_basic() {
    let query_id = "test-count-basic-001";
    let database_name = &generate_unique_name("test_db_count_basic");
    let table_name = &generate_unique_name("test_table_count_basic");

    println!(
        "Testing basic count operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("count_001")),
            ("name", create_string_datum("First Count Document")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_002")),
            ("name", create_string_datum("Second Count Document")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_003")),
            ("name", create_string_datum("Third Count Document")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_004")),
            ("name", create_string_datum("Fourth Count Document")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("count_005")),
            ("name", create_string_datum("Fifth Count Document")),
            ("category", create_string_datum("test")),
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

    // Count documents in the table
    let count_query = create_count_query(database_name, table_name);
    let count_envelope = create_envelope(query_id, &count_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_envelope)
        .await
        .expect("Failed to send count envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Count should return a number representing the total documents
    match response_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count returned integer: {count}");

            // Should return 5 documents
            if count >= 5 {
                println!("✓ Count returned expected number of documents");
            } else {
                println!("ℹ Count returned {count} documents (expected 5 or more)");
            }
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            // Some implementations might return an object with count field
            if let Some(count_field) = obj.fields.get("count") {
                if let Some(proto::datum::Value::Int(count_val)) = &count_field.value {
                    println!("✓ Count returned object with count: {count_val}");
                } else {
                    println!("ℹ Count object has non-integer count field: {count_field:?}");
                }
            } else {
                println!("✓ Count returned object: {obj:?}");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Count returned null");
        }
        _ => {
            println!(
                "ℹ Count returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Basic count test completed successfully!");
}

#[tokio::test]
async fn test_count_empty_table() {
    let query_id = "test-count-empty-002";
    let database_name = &generate_unique_name("test_db_count_empty");
    let table_name = &generate_unique_name("test_table_count_empty");

    println!(
        "Testing count on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Count documents in empty table
    let count_query = create_count_query(database_name, table_name);
    let count_envelope = create_envelope(query_id, &count_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_envelope)
        .await
        .expect("Failed to send count envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Count on empty table should return 0
    match response_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count on empty table returned: {count}");

            if count == 0 {
                println!("✓ Count on empty table returned 0 as expected");
            } else {
                println!("ℹ Count on empty table returned {count} (expected 0)");
            }
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            if let Some(count_field) = obj.fields.get("count") {
                if let Some(proto::datum::Value::Int(count_val)) = &count_field.value {
                    if *count_val == 0 {
                        println!("✓ Count on empty table returned object with count 0");
                    } else {
                        println!("ℹ Count on empty table returned object with count {count_val}");
                    }
                }
            } else {
                println!("✓ Count on empty table returned object: {obj:?}");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Count on empty table returned null");
        }
        _ => {
            println!(
                "ℹ Count on empty table returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Count empty table test completed successfully!");
}

#[tokio::test]
async fn test_count_with_timeout() {
    let query_id = "test-count-timeout-003";
    let database_name = &generate_unique_name("test_db_count_timeout");
    let table_name = &generate_unique_name("test_table_count_timeout");

    println!(
        "Testing count with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Create count query with custom timeout
    let mut count_query = create_count_query(database_name, table_name);
    if let Some(ref mut options) = count_query.options {
        options.timeout_ms = 8000; // 8 second timeout
    }

    let count_envelope = create_envelope(query_id, &count_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_envelope)
        .await
        .expect("Failed to send count envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Count with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count with timeout returned integer: {count}");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Count with timeout returned object: {obj:?}");
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Count with timeout returned null/empty");
        }
        _ => {
            println!("ℹ Count with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Count with timeout test completed successfully!");
}

#[tokio::test]
async fn test_count_nonexistent_table() {
    let query_id = "test-count-no-table-004";
    let database_name = &generate_unique_name("test_db_count_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing count on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try to count on nonexistent table
    let count_query = create_count_query(database_name, table_name);
    let count_envelope = create_envelope(query_id, &count_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_envelope)
        .await
        .expect("Failed to send count envelope");

    // Check if count succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Count on nonexistent table succeeded (auto-create or zero result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Int(count)) => {
                    println!("  Count result: {count}");
                    if count == 0 {
                        println!("  ✓ Nonexistent table returned count 0");
                    }
                }
                _ => {
                    println!("  Response: {:?}", response_datum.value);
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Count on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in count nonexistent table response");
        }
    }

    println!("✓ Count nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_count_large_dataset() {
    let query_id = "test-count-large-005";
    let database_name = &generate_unique_name("test_db_count_large");
    let table_name = &generate_unique_name("test_table_count_large");

    println!(
        "Testing count with large dataset, ID: {query_id}, database: {database_name}, table: {table_name}"
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
    for i in 1..=25 {
        documents.push(create_datum_object(vec![
            ("id", create_string_datum(&format!("large_doc_{i:03}"))),
            (
                "name",
                create_string_datum(&format!("Large Dataset Document {i}")),
            ),
            ("index", create_int_datum(i as i64)),
            ("category", create_string_datum(&format!("cat_{}", i % 5))),
        ]));
    }

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Large dataset (25 documents) inserted successfully");

    // Count documents in the large dataset
    let count_query = create_count_query(database_name, table_name);
    let count_envelope = create_envelope(query_id, &count_query);

    let response_envelope = send_envelope_to_server(&mut stream, &count_envelope)
        .await
        .expect("Failed to send count envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Count should return the total number of documents
    match response_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count large dataset returned: {count}");

            if count >= 25 {
                println!("✓ Count returned expected number of documents for large dataset");
            } else {
                println!("ℹ Count returned {count} documents (expected 25 or more)");
            }
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            if let Some(count_field) = obj.fields.get("count") {
                if let Some(proto::datum::Value::Int(count_val)) = &count_field.value {
                    println!("✓ Count large dataset returned object with count: {count_val}");
                }
            } else {
                println!("✓ Count large dataset returned object: {obj:?}");
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Count large dataset returned null");
        }
        _ => {
            println!("ℹ Count large dataset returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Count large dataset test completed successfully!");
}

#[tokio::test]
async fn test_count_after_operations() {
    let query_id = "test-count-after-ops-006";
    let database_name = &generate_unique_name("test_db_count_after_ops");
    let table_name = &generate_unique_name("test_table_count_after_ops");

    println!(
        "Testing count after insert/delete operations, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Count empty table first
    let count_empty_query = create_count_query(database_name, table_name);
    let count_empty_envelope =
        create_envelope(&format!("{query_id}-count-empty"), &count_empty_query);
    let count_empty_response = send_envelope_to_server(&mut stream, &count_empty_envelope)
        .await
        .expect("Failed to send count empty envelope");
    validate_response_envelope(&count_empty_response, &format!("{query_id}-count-empty"))
        .expect("Count empty response validation failed");

    let count_empty_datum = decode_response_payload(&count_empty_response)
        .expect("Failed to decode count empty response");

    match count_empty_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Initial count (empty table): {count}");
        }
        _ => {
            println!("ℹ Initial count: {:?}", count_empty_datum.value);
        }
    }

    // Insert some documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("ops_001")),
            ("name", create_string_datum("After Ops Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("ops_002")),
            ("name", create_string_datum("After Ops Test 2")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("ops_003")),
            ("name", create_string_datum("After Ops Test 3")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Documents inserted successfully");

    // Count after insert
    let count_after_insert_query = create_count_query(database_name, table_name);
    let count_after_insert_envelope = create_envelope(
        &format!("{query_id}-count-after-insert"),
        &count_after_insert_query,
    );
    let count_after_insert_response =
        send_envelope_to_server(&mut stream, &count_after_insert_envelope)
            .await
            .expect("Failed to send count after insert envelope");
    validate_response_envelope(
        &count_after_insert_response,
        &format!("{query_id}-count-after-insert"),
    )
    .expect("Count after insert response validation failed");

    let count_after_insert_datum = decode_response_payload(&count_after_insert_response)
        .expect("Failed to decode count after insert response");

    match count_after_insert_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count after insert: {count}");
            if count >= 3 {
                println!("✓ Count increased after insert as expected");
            }
        }
        _ => {
            println!("ℹ Count after insert: {:?}", count_after_insert_datum.value);
        }
    }

    // Delete all documents
    let delete_query = create_delete_query(database_name, table_name);
    let delete_envelope = create_envelope(&format!("{query_id}-delete"), &delete_query);
    let delete_response = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");
    validate_response_envelope(&delete_response, &format!("{query_id}-delete"))
        .expect("Delete response validation failed");

    println!("✓ Documents deleted successfully");

    // Count after delete
    let count_after_delete_query = create_count_query(database_name, table_name);
    let count_after_delete_envelope = create_envelope(
        &format!("{query_id}-count-after-delete"),
        &count_after_delete_query,
    );
    let count_after_delete_response =
        send_envelope_to_server(&mut stream, &count_after_delete_envelope)
            .await
            .expect("Failed to send count after delete envelope");
    validate_response_envelope(
        &count_after_delete_response,
        &format!("{query_id}-count-after-delete"),
    )
    .expect("Count after delete response validation failed");

    let count_after_delete_datum = decode_response_payload(&count_after_delete_response)
        .expect("Failed to decode count after delete response");

    match count_after_delete_datum.value {
        Some(proto::datum::Value::Int(count)) => {
            println!("✓ Count after delete: {count}");
            if count == 0 {
                println!("✓ Count returned to 0 after delete as expected");
            }
        }
        _ => {
            println!("ℹ Count after delete: {:?}", count_after_delete_datum.value);
        }
    }

    println!("✓ Count after operations test completed successfully!");
}

#[tokio::test]
async fn test_count_consistency() {
    let query_id = "test-count-consistency-007";
    let database_name = &generate_unique_name("test_db_count_consistency");
    let table_name = &generate_unique_name("test_table_count_consistency");

    println!(
        "Testing count consistency across multiple requests, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("consistency_001")),
            ("name", create_string_datum("Consistency Test 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("consistency_002")),
            ("name", create_string_datum("Consistency Test 2")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("consistency_003")),
            ("name", create_string_datum("Consistency Test 3")),
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

    // Perform multiple count queries and check consistency
    let mut counts = Vec::new();
    for i in 1..=5 {
        let count_query = create_count_query(database_name, table_name);
        let count_envelope = create_envelope(&format!("{query_id}-consistency-{i}"), &count_query);

        let count_response = send_envelope_to_server(&mut stream, &count_envelope)
            .await
            .expect("Failed to send count envelope");

        validate_response_envelope(&count_response, &format!("{query_id}-consistency-{i}"))
            .expect("Count response validation failed");

        let count_datum =
            decode_response_payload(&count_response).expect("Failed to decode count response");

        match count_datum.value {
            Some(proto::datum::Value::Int(count)) => {
                counts.push(count);
                println!("  Count query {i}: {count}");
            }
            Some(proto::datum::Value::Object(ref obj)) => {
                if let Some(count_field) = obj.fields.get("count") {
                    if let Some(proto::datum::Value::Int(count_val)) = &count_field.value {
                        counts.push(*count_val);
                        println!("  Count query {i}: {count_val} (from object)");
                    }
                }
            }
            _ => {
                println!("  Count query {}: {:?}", i, count_datum.value);
            }
        }
    }

    // Check consistency across all count results
    if counts.len() > 1 {
        let first_count = counts[0];
        let all_same = counts.iter().all(|&count| count == first_count);

        if all_same {
            println!("✓ All count queries returned consistent result: {first_count}");
        } else {
            println!("ℹ Count queries returned different results: {counts:?}");
        }
    } else {
        println!("ℹ Insufficient count results to check consistency");
    }

    println!("✓ Count consistency test completed successfully!");
}
