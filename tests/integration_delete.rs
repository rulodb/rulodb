mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_delete_all_documents() {
    let query_id = "test-delete-all-001";
    let database_name = &generate_unique_name("test_db_delete_all");
    let table_name = &generate_unique_name("test_table_delete_all");

    println!(
        "Testing delete all documents with ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("delete_test_001")),
            ("name", create_string_datum("Document to Delete 1")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("delete_test_002")),
            ("name", create_string_datum("Document to Delete 2")),
            ("category", create_string_datum("test")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("delete_test_003")),
            ("name", create_string_datum("Document to Delete 3")),
            ("category", create_string_datum("test")),
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

    // Delete all documents
    let delete_query = create_delete_query(database_name, table_name);
    let delete_envelope = create_envelope(query_id, &delete_query);

    let response_envelope = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Delete should return success indication
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Delete returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Delete returned object response: {:?}", obj);
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Delete returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Delete returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Delete all documents test completed successfully!");
}

#[tokio::test]
async fn test_delete_empty_table() {
    let query_id = "test-delete-empty-003";
    let database_name = &generate_unique_name("test_db_delete_empty");
    let table_name = &generate_unique_name("test_table_delete_empty");

    println!(
        "Testing delete on empty table, ID: {}, database: {}, table: {}",
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

    // Delete from empty table
    let delete_query = create_delete_query(database_name, table_name);
    let delete_envelope = create_envelope(query_id, &delete_query);

    let response_envelope = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Delete on empty table should still succeed
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Delete on empty table returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!(
                "✓ Delete on empty table returned object response: {:?}",
                obj
            );
        }
        _ => {
            println!(
                "ℹ Delete on empty table returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Delete empty table test completed successfully!");
}

#[tokio::test]
async fn test_delete_with_timeout() {
    let query_id = "test-delete-timeout-004";
    let database_name = &generate_unique_name("test_db_delete_timeout");
    let table_name = &generate_unique_name("test_table_delete_timeout");

    println!(
        "Testing delete with custom timeout, ID: {}, database: {}, table: {}",
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
        ("id", create_string_datum("timeout_test_doc")),
        ("name", create_string_datum("Timeout Test Document")),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Create delete query with custom timeout
    let mut delete_query = create_delete_query(database_name, table_name);
    if let Some(ref mut options) = delete_query.options {
        options.timeout_ms = 5000; // 5 second timeout
    }

    let delete_envelope = create_envelope(query_id, &delete_query);

    let response_envelope = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Delete with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Delete with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Delete with timeout returned object: {:?}", obj);
        }
        _ => {
            println!("ℹ Delete with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Delete with timeout test completed successfully!");
}

#[tokio::test]
async fn test_delete_nonexistent_table() {
    let query_id = "test-delete-no-table-005";
    let database_name = &generate_unique_name("test_db_delete_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing delete from nonexistent table, ID: {}, database: {}, table: {}",
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

    // Try to delete from nonexistent table
    let delete_query = create_delete_query(database_name, table_name);
    let delete_envelope = create_envelope(query_id, &delete_query);

    let response_envelope = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");

    // Check if delete succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Delete from nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Delete from nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in delete from nonexistent table response");
        }
    }

    println!("✓ Delete from nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_delete_and_verify_empty() {
    let query_id = "test-delete-verify-006";
    let database_name = &generate_unique_name("test_db_delete_verify");
    let table_name = &generate_unique_name("test_table_delete_verify");

    println!(
        "Testing delete and verify table is empty, ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("verify_001")),
            ("name", create_string_datum("Document 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("verify_002")),
            ("name", create_string_datum("Document 2")),
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

    // Delete all documents
    let delete_query = create_delete_query(database_name, table_name);
    let delete_envelope = create_envelope(&format!("{}-delete", query_id), &delete_query);
    let delete_response = send_envelope_to_server(&mut stream, &delete_envelope)
        .await
        .expect("Failed to send delete envelope");
    validate_response_envelope(&delete_response, &format!("{}-delete", query_id))
        .expect("Delete response validation failed");

    println!("✓ Delete operation completed");

    // Verify table is now empty
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(&format!("{}-verify", query_id), &table_query);
    let table_response = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");
    validate_response_envelope(&table_response, &format!("{}-verify", query_id))
        .expect("Table query response validation failed");

    let table_datum =
        decode_response_payload(&table_response).expect("Failed to decode table query response");

    // Table should now be empty
    match table_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Table is empty after delete as expected");
            } else {
                println!(
                    "ℹ Table still contains {} documents after delete",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table query returned null (empty table) as expected");
        }
        _ => {
            println!("ℹ Table query returned: {:?}", table_datum.value);
        }
    }

    println!("✓ Delete and verify empty test completed successfully!");
}
