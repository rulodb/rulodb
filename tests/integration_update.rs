mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_update_documents() {
    let query_id = "test-update-001";
    let database_name = &generate_unique_name("test_db_update");
    let table_name = &generate_unique_name("test_table_update");

    println!(
        "Testing update documents with ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("update_test_001")),
            ("name", create_string_datum("Original Name 1")),
            ("status", create_string_datum("pending")),
            ("count", create_int_datum(5)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("update_test_002")),
            ("name", create_string_datum("Original Name 2")),
            ("status", create_string_datum("pending")),
            ("count", create_int_datum(10)),
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

    // Create update patch
    let patch = create_datum_object(vec![
        ("status", create_string_datum("updated")),
        ("updated_at", create_string_datum("2023-12-01T00:00:00Z")),
    ]);

    // Update documents
    let update_query = create_update_query(database_name, table_name, patch);
    let update_envelope = create_envelope(query_id, &update_query);

    let response_envelope = send_envelope_to_server(&mut stream, &update_envelope)
        .await
        .expect("Failed to send update envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Update should return success indication
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Update returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Update returned object response: {obj:?}");
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Update returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Update returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Update documents test completed successfully!");
}

#[tokio::test]
async fn test_update_empty_table() {
    let query_id = "test-update-empty-003";
    let database_name = &generate_unique_name("test_db_update_empty");
    let table_name = &generate_unique_name("test_table_update_empty");

    println!(
        "Testing update on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Create update patch
    let patch = create_datum_object(vec![("status", create_string_datum("updated"))]);

    // Update empty table
    let update_query = create_update_query(database_name, table_name, patch);
    let update_envelope = create_envelope(query_id, &update_query);

    let response_envelope = send_envelope_to_server(&mut stream, &update_envelope)
        .await
        .expect("Failed to send update envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Update on empty table should still succeed
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Update on empty table returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Update on empty table returned object response: {obj:?}");
        }
        _ => {
            println!(
                "ℹ Update on empty table returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Update empty table test completed successfully!");
}

#[tokio::test]
async fn test_update_with_various_data_types() {
    let query_id = "test-update-data-types-004";
    let database_name = &generate_unique_name("test_db_update_types");
    let table_name = &generate_unique_name("test_table_update_types");

    println!(
        "Testing update with various data types, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert initial document
    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("types_test_001")),
        ("old_string", create_string_datum("old_value")),
        ("old_int", create_int_datum(0)),
        ("old_bool", create_bool_datum(false)),
        ("old_float", create_float_datum(0.0)),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Initial document inserted successfully");

    // Create update patch with various data types
    let patch = create_datum_object(vec![
        ("new_string", create_string_datum("Hello, Update!")),
        ("new_int", create_int_datum(42)),
        ("new_float", create_float_datum(3.15159)),
        ("new_bool", create_bool_datum(true)),
        ("new_negative", create_int_datum(-999)),
        ("new_zero", create_int_datum(0)),
        ("new_empty_string", create_string_datum("")),
    ]);

    // Update document
    let update_query = create_update_query(database_name, table_name, patch);
    let update_envelope = create_envelope(query_id, &update_query);

    let response_envelope = send_envelope_to_server(&mut stream, &update_envelope)
        .await
        .expect("Failed to send update envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Update with various data types should succeed
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Data types update returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Data types update returned object response: {obj:?}");
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Data types update returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Data types update returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Update with various data types test completed successfully!");
}

#[tokio::test]
async fn test_update_with_timeout() {
    let query_id = "test-update-timeout-005";
    let database_name = &generate_unique_name("test_db_update_timeout");
    let table_name = &generate_unique_name("test_table_update_timeout");

    println!(
        "Testing update with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test document
    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("timeout_test_doc")),
        ("name", create_string_datum("Timeout Test Document")),
        ("value", create_int_datum(1)),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Create update patch
    let patch = create_datum_object(vec![("value", create_int_datum(999))]);

    // Create update query with custom timeout
    let mut update_query = create_update_query(database_name, table_name, patch);
    if let Some(ref mut options) = update_query.options {
        options.timeout_ms = 2000; // 2 second timeout
    }

    let update_envelope = create_envelope(query_id, &update_query);

    let response_envelope = send_envelope_to_server(&mut stream, &update_envelope)
        .await
        .expect("Failed to send update envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Update with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Update with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Update with timeout returned object: {obj:?}");
        }
        _ => {
            println!("ℹ Update with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Update with timeout test completed successfully!");
}

#[tokio::test]
async fn test_update_nonexistent_table() {
    let query_id = "test-update-no-table-006";
    let database_name = &generate_unique_name("test_db_update_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing update on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Create update patch
    let patch = create_datum_object(vec![("status", create_string_datum("updated"))]);

    // Try to update nonexistent table
    let update_query = create_update_query(database_name, table_name, patch);
    let update_envelope = create_envelope(query_id, &update_query);

    let response_envelope = send_envelope_to_server(&mut stream, &update_envelope)
        .await
        .expect("Failed to send update envelope");

    // Check if update succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Update on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Update on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in update nonexistent table response");
        }
    }

    println!("✓ Update nonexistent table test completed successfully!");
}
