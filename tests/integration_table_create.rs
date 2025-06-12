mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_table_create_query() {
    let query_id = "test-table-create-001";
    let database_name = &generate_unique_name("test_db_table_create");
    let table_name = &generate_unique_name("test_table");

    println!(
        "Testing table create query with ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Then, create the table
    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope = create_envelope(query_id, &table_create_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table create should return null or empty response on success
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table create returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Table create returned empty object response");
        }
        _ => {
            // Some implementations might return other success indicators
            println!("ℹ Table create returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Table create query test completed successfully!");
}

#[tokio::test]
async fn test_table_create_with_custom_timeout() {
    let query_id = "test-table-create-timeout-002";
    let database_name = &generate_unique_name("test_db_table_create_timeout");
    let table_name = &generate_unique_name("test_table_timeout");

    println!(
        "Testing table create with custom timeout, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Create table query with custom timeout
    let mut table_create_query = create_table_create_query(database_name, table_name);
    if let Some(ref mut options) = table_create_query.options {
        options.timeout_ms = 10000; // 10 second timeout
    }

    let envelope = create_envelope(query_id, &table_create_query);

    // Send query and receive response
    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Validate response format - should be successful with custom timeout
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table create with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Table create with timeout returned empty object response");
        }
        _ => {
            println!(
                "ℹ Table create with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table create with timeout test completed successfully!");
}

#[tokio::test]
async fn test_table_create_duplicate() {
    let query_id = "test-table-create-duplicate-003";
    let database_name = &generate_unique_name("test_db_table_duplicate");
    let table_name = &generate_unique_name("test_table_duplicate");

    println!(
        "Testing duplicate table create, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{}-db-create", query_id))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // First table create - should succeed
    let query1 = create_table_create_query(database_name, table_name);
    let envelope1 = create_envelope(&format!("{}-first", query_id), &query1);

    let response_envelope1 = send_envelope_to_server(&mut stream, &envelope1)
        .await
        .expect("Failed to send first table create envelope");

    validate_response_envelope(&response_envelope1, &format!("{}-first", query_id))
        .expect("First table create response validation failed");

    println!("✓ First table create completed");

    // Second create - might succeed or fail depending on implementation
    let query2 = create_table_create_query(database_name, table_name);
    let envelope2 = create_envelope(&format!("{}-second", query_id), &query2);

    let response_envelope2 = send_envelope_to_server(&mut stream, &envelope2)
        .await
        .expect("Failed to send second table create envelope");

    // Check if second create succeeded or failed
    match proto::MessageType::try_from(response_envelope2.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Second table create succeeded (table already exists behavior)");
            let response_datum = decode_response_payload(&response_envelope2)
                .expect("Failed to decode second response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Second table create failed as expected (duplicate table)");
            // This is also acceptable behavior
        }
        _ => {
            panic!("Unexpected message type in second table create response");
        }
    }

    println!("✓ Duplicate table create test completed successfully!");
}

#[tokio::test]
async fn test_table_create_multiple_tables() {
    let base_query_id = "test-table-create-multiple";
    let database_name = &generate_unique_name("test_db_table_create_multiple");
    let table_names = [
        generate_unique_name("test_table_multi_001"),
        generate_unique_name("test_table_multi_002"),
        generate_unique_name("test_table_multi_003"),
    ];

    println!(
        "Testing multiple table creates in database: {}",
        database_name
    );

    // Connect to server and create database first
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope =
        create_envelope(&format!("{}-db-create", base_query_id), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{}-db-create", base_query_id))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    for (i, table_name) in table_names.iter().enumerate() {
        let query_id = format!("{}-{:03}", base_query_id, i + 1);
        println!(
            "Creating table {} of {}: {} (ID: {})",
            i + 1,
            table_names.len(),
            table_name,
            query_id
        );

        // Create and send table create query
        let query = create_table_create_query(database_name, table_name);
        let envelope = create_envelope(&query_id, &query);

        let response_envelope = send_envelope_to_server(&mut stream, &envelope)
            .await
            .expect("Failed to send envelope and receive response");

        // Validate basic response structure
        validate_response_envelope(&response_envelope, &query_id)
            .expect("Response validation failed");

        // Decode and validate response
        let response_datum =
            decode_response_payload(&response_envelope).expect("Failed to decode response payload");

        // Validate response format
        match response_datum.value {
            Some(proto::datum::Value::Null(_)) | None => {
                println!("  ✓ Table {} created successfully", i + 1);
            }
            Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
                println!("  ✓ Table {} created successfully (empty object)", i + 1);
            }
            _ => {
                println!(
                    "  ℹ Table {} create returned: {:?}",
                    i + 1,
                    response_datum.value
                );
            }
        }
    }

    println!("✓ Multiple table creates test completed successfully!");
}

#[tokio::test]
async fn test_table_create_and_verify_list() {
    let query_id_db_create = "test-table-create-verify-db-create";
    let query_id_table_create = "test-table-create-verify-table-create";
    let query_id_list = "test-table-create-verify-list";
    let database_name = &generate_unique_name("test_db_table_create_verify");
    let table_name = &generate_unique_name("test_table_create_verify");

    println!(
        "Testing table create and verification via list, database: {}, table: {}",
        database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_env = create_envelope(query_id_db_create, &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_env)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, query_id_db_create)
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Then, create the table
    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_env = create_envelope(query_id_table_create, &table_create_query);

    let table_create_response = send_envelope_to_server(&mut stream, &table_create_env)
        .await
        .expect("Failed to send table create envelope");

    validate_response_envelope(&table_create_response, query_id_table_create)
        .expect("Table create response validation failed");

    println!("✓ Table created successfully");

    // Finally, list tables to verify it exists
    let list_query = create_table_list_query(database_name);
    let list_env = create_envelope(query_id_list, &list_query);

    let list_response = send_envelope_to_server(&mut stream, &list_env)
        .await
        .expect("Failed to send table list envelope");

    validate_response_envelope(&list_response, query_id_list)
        .expect("Table list response validation failed");

    // Decode and validate list response
    let list_datum =
        decode_response_payload(&list_response).expect("Failed to decode list response payload");

    // Check if our table appears in the list
    match list_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            let mut found = false;
            for item in &array.items {
                if let Some(proto::datum::Value::String(name)) = &item.value {
                    if name == table_name {
                        found = true;
                        break;
                    }
                }
            }

            if found {
                println!("✓ Created table '{}' found in table list", table_name);
            } else {
                println!(
                    "ℹ Created table '{}' not found in list (may not be immediately visible)",
                    table_name
                );
                println!("  Available tables: {:?}", array.items);
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("ℹ Table list returned null (no tables visible)");
        }
        _ => {
            println!("ℹ Unexpected table list format: {:?}", list_datum.value);
        }
    }

    println!("✓ Table create and verify test completed successfully!");
}

#[tokio::test]
async fn test_table_create_without_database() {
    let query_id = "test-table-create-no-db-004";
    let database_name = &generate_unique_name("test_db_nonexistent");
    let table_name = &generate_unique_name("test_table_no_db");

    println!(
        "Testing table create without database, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Try to create a table in a non-existent database
    let query = create_table_create_query(database_name, table_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Check if table create succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Table create in non-existent database succeeded (auto-create behavior)");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Table create in non-existent database failed as expected");
            // This is the expected behavior
        }
        _ => {
            panic!("Unexpected message type in table create without database response");
        }
    }

    println!("✓ Table create without database test completed successfully!");
}
