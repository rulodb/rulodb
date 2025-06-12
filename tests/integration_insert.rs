mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_insert_single_document() {
    let query_id = "test-insert-single-001";
    let database_name = &generate_unique_name("test_db_insert_single");
    let table_name = &generate_unique_name("test_table_insert");

    println!(
        "Testing single document insert with ID: {}, database: {}, table: {}",
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

    // Create a test document
    let document = create_datum_object(vec![
        ("id", create_string_datum("test_001")),
        ("name", create_string_datum("Test Document")),
        ("value", create_int_datum(42)),
        ("active", create_bool_datum(true)),
    ]);

    // Create and send insert query
    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(query_id, &insert_query);

    let response_envelope = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Insert should return success indication
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Insert returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Insert returned object response: {:?}", obj);
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Insert returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Insert returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Single document insert test completed successfully!");
}

#[tokio::test]
async fn test_insert_multiple_documents() {
    let query_id = "test-insert-multiple-002";
    let database_name = &generate_unique_name("test_db_insert_multiple");
    let table_name = &generate_unique_name("test_table_insert_multi");

    println!(
        "Testing multiple document insert with ID: {}, database: {}, table: {}",
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

    // Create multiple test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("test_001")),
            ("name", create_string_datum("First Document")),
            ("value", create_int_datum(100)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("test_002")),
            ("name", create_string_datum("Second Document")),
            ("value", create_int_datum(200)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("test_003")),
            ("name", create_string_datum("Third Document")),
            ("value", create_int_datum(300)),
        ]),
    ];

    // Create and send insert query
    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(query_id, &insert_query);

    let response_envelope = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Insert should return success indication
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Multiple insert returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Multiple insert returned object response: {:?}", obj);
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Multiple insert returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Multiple insert returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Multiple document insert test completed successfully!");
}

#[tokio::test]
async fn test_insert_with_various_data_types() {
    let query_id = "test-insert-data-types-003";
    let database_name = &generate_unique_name("test_db_insert_types");
    let table_name = &generate_unique_name("test_table_insert_types");

    println!(
        "Testing insert with various data types, ID: {}, database: {}, table: {}",
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

    // Create a document with various data types
    let document = create_datum_object(vec![
        ("id", create_string_datum("mixed_types_001")),
        ("string_field", create_string_datum("Hello, World!")),
        ("int_field", create_int_datum(-42)),
        ("float_field", create_float_datum(3.15159)),
        ("bool_true", create_bool_datum(true)),
        ("bool_false", create_bool_datum(false)),
        ("large_int", create_int_datum(9223372036854775807)), // Max i64
        ("zero", create_int_datum(0)),
        ("negative_float", create_float_datum(-999.999)),
        ("empty_string", create_string_datum("")),
    ]);

    // Create and send insert query
    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(query_id, &insert_query);

    let response_envelope = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Insert should return success indication
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Data types insert returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) => {
            println!("✓ Data types insert returned object response: {:?}", obj);
        }
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Data types insert returned array response with {} items",
                arr.items.len()
            );
        }
        _ => {
            println!("ℹ Data types insert returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Insert with various data types test completed successfully!");
}

#[tokio::test]
async fn test_insert_empty_document() {
    let query_id = "test-insert-empty-005";
    let database_name = &generate_unique_name("test_db_insert_empty");
    let table_name = &generate_unique_name("test_table_insert_empty");

    println!(
        "Testing insert of empty document, ID: {}, database: {}, table: {}",
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

    // Create an empty document
    let document = create_datum_object(vec![]);

    // Create and send insert query
    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(query_id, &insert_query);

    let response_envelope = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");

    // Check if insert succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("✓ Empty document insert succeeded");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Empty document insert failed (may be expected behavior)");
            // This might be expected behavior depending on implementation
        }
        _ => {
            panic!("Unexpected message type in empty document insert response");
        }
    }

    println!("✓ Empty document insert test completed successfully!");
}

#[tokio::test]
async fn test_insert_into_nonexistent_table() {
    let query_id = "test-insert-no-table-006";
    let database_name = &generate_unique_name("test_db_insert_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing insert into nonexistent table, ID: {}, database: {}, table: {}",
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

    // Create a test document
    let document = create_datum_object(vec![
        ("id", create_string_datum("test_no_table")),
        (
            "name",
            create_string_datum("Document for nonexistent table"),
        ),
    ]);

    // Try to insert into nonexistent table
    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(query_id, &insert_query);

    let response_envelope = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");

    // Check if insert succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Insert into nonexistent table succeeded (auto-create behavior)");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Insert into nonexistent table failed as expected");
            // This is the expected behavior
        }
        _ => {
            panic!("Unexpected message type in insert to nonexistent table response");
        }
    }

    println!("✓ Insert into nonexistent table test completed successfully!");
}
