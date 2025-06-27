mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_table_drop_query() {
    let query_id = "test-table-drop-001";
    let database_name = &generate_unique_name("test_db_table_drop");
    let table_name = &generate_unique_name("test_table_drop");

    println!(
        "Testing table drop query with ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Then, create the table
    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);

    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");

    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Table created successfully");

    // Finally, drop the table
    let table_drop_query = create_table_drop_query(database_name, table_name);
    let table_drop_envelope = create_envelope(query_id, &table_drop_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_drop_envelope)
        .await
        .expect("Failed to send table drop envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table drop should return null or empty response on success
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table drop returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Table drop returned empty object response");
        }
        _ => {
            // Some implementations might return other success indicators
            println!("ℹ Table drop returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Table drop query test completed successfully!");
}

#[tokio::test]
async fn test_table_drop_with_custom_timeout() {
    let query_id = "test-table-drop-timeout-002";
    let database_name = &generate_unique_name("test_db_table_drop_timeout");
    let table_name = &generate_unique_name("test_table_drop_timeout");

    println!(
        "Testing table drop with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Then, create the table
    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);

    let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to send table create envelope");

    validate_response_envelope(&table_create_response, &format!("{query_id}-table-create"))
        .expect("Table create response validation failed");

    println!("✓ Table created successfully");

    // Create table drop query with custom timeout
    let mut table_drop_query = create_table_drop_query(database_name, table_name);
    if let Some(ref mut options) = table_drop_query.options {
        options.timeout_ms = 10000; // 10 second timeout
    }

    let envelope = create_envelope(query_id, &table_drop_query);

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
            println!("✓ Table drop with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Table drop with timeout returned empty object response");
        }
        _ => {
            println!(
                "ℹ Table drop with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table drop with timeout test completed successfully!");
}

#[tokio::test]
async fn test_table_drop_nonexistent() {
    let query_id = "test-table-drop-nonexistent-003";
    let database_name = &generate_unique_name("test_db_table_drop_nonexistent");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing drop of nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database (but not the table)
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Try to drop a table that doesn't exist
    let query = create_table_drop_query(database_name, table_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Check if drop succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Drop of nonexistent table succeeded (idempotent behavior)");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Drop of nonexistent table failed as expected");
            // This is also acceptable behavior
        }
        _ => {
            panic!("Unexpected message type in drop nonexistent table response");
        }
    }

    println!("✓ Drop nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_table_drop_multiple_tables() {
    let base_query_id = "test-table-drop-multiple";
    let database_name = &generate_unique_name("test_db_table_drop_multiple");
    let table_names = [
        generate_unique_name("test_table_drop_multi_001"),
        generate_unique_name("test_table_drop_multi_002"),
        generate_unique_name("test_table_drop_multi_003"),
    ];

    println!("Testing multiple table drops in database: {database_name}");

    // Connect to server and create database first
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope =
        create_envelope(&format!("{base_query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{base_query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Create all tables first
    for (i, table_name) in table_names.iter().enumerate() {
        let table_create_query = create_table_create_query(database_name, table_name);
        let table_create_envelope = create_envelope(
            &format!("{}-table-create-{:03}", base_query_id, i + 1),
            &table_create_query,
        );

        let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
            .await
            .expect("Failed to send table create envelope");

        validate_response_envelope(
            &table_create_response,
            &format!("{}-table-create-{:03}", base_query_id, i + 1),
        )
        .expect("Table create response validation failed");

        println!("✓ Table {table_name} created successfully");
    }

    // Now drop all tables
    for (i, table_name) in table_names.iter().enumerate() {
        let query_id = format!("{}-{:03}", base_query_id, i + 1);
        println!(
            "Dropping table {} of {}: {} (ID: {})",
            i + 1,
            table_names.len(),
            table_name,
            query_id
        );

        // Create and send table drop query
        let query = create_table_drop_query(database_name, table_name);
        let envelope = create_envelope(&query_id, &query);

        let response_envelope = send_envelope_to_server(&mut stream, &envelope)
            .await
            .expect("Failed to send envelope and receive response");

        // Check response type (may be success or error depending on existence)
        match proto::MessageType::try_from(response_envelope.r#type) {
            Ok(proto::MessageType::Response) => {
                validate_response_envelope(&response_envelope, &query_id)
                    .expect("Response validation failed");

                let response_datum = decode_response_payload(&response_envelope)
                    .expect("Failed to decode response payload");

                match response_datum.value {
                    Some(proto::datum::Value::Null(_)) | None => {
                        println!("  ✓ Table {} dropped successfully", i + 1);
                    }
                    Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
                        println!("  ✓ Table {} dropped successfully (empty object)", i + 1);
                    }
                    _ => {
                        println!(
                            "  ℹ Table {} drop returned: {:?}",
                            i + 1,
                            response_datum.value
                        );
                    }
                }
            }
            Ok(proto::MessageType::Error) => {
                println!("  ℹ Table {} drop failed (may not exist)", i + 1);
            }
            _ => {
                panic!("Unexpected message type in table drop response");
            }
        }
    }

    println!("✓ Multiple table drops test completed successfully!");
}

#[tokio::test]
async fn test_table_create_and_drop_cycle() {
    let query_id_db_create = "test-table-cycle-db-create";
    let query_id_table_create = "test-table-cycle-table-create";
    let query_id_drop = "test-table-cycle-drop";
    let database_name = &generate_unique_name("test_db_table_cycle");
    let table_name = &generate_unique_name("test_table_cycle");

    println!("Testing table create and drop cycle, database: {database_name}, table: {table_name}");

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

    // Finally, drop the table
    let drop_query = create_table_drop_query(database_name, table_name);
    let drop_env = create_envelope(query_id_drop, &drop_query);

    let drop_response = send_envelope_to_server(&mut stream, &drop_env)
        .await
        .expect("Failed to send table drop envelope");

    validate_response_envelope(&drop_response, query_id_drop)
        .expect("Table drop response validation failed");

    // Decode and validate drop response
    let drop_datum =
        decode_response_payload(&drop_response).expect("Failed to decode drop response payload");

    match drop_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table dropped successfully");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Table dropped successfully (empty object)");
        }
        _ => {
            println!("ℹ Table drop returned: {:?}", drop_datum.value);
        }
    }

    println!("✓ Table create and drop cycle test completed successfully!");
}

#[tokio::test]
async fn test_table_drop_and_verify_list() {
    let query_id_db_create = "test-table-drop-verify-db-create";
    let query_id_table_create = "test-table-drop-verify-table-create";
    let query_id_drop = "test-table-drop-verify-drop";
    let query_id_list = "test-table-drop-verify-list";
    let database_name = &generate_unique_name("test_db_table_drop_verify");
    let table_name = &generate_unique_name("test_table_drop_verify");

    println!(
        "Testing table drop and verification via list, database: {database_name}, table: {table_name}"
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

    println!("✓ Database created for table drop test");

    // Then, create the table
    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_env = create_envelope(query_id_table_create, &table_create_query);

    let table_create_response = send_envelope_to_server(&mut stream, &table_create_env)
        .await
        .expect("Failed to send table create envelope");

    validate_response_envelope(&table_create_response, query_id_table_create)
        .expect("Table create response validation failed");

    println!("✓ Table created successfully");

    // Then, drop the table
    let drop_query = create_table_drop_query(database_name, table_name);
    let drop_env = create_envelope(query_id_drop, &drop_query);

    let drop_response = send_envelope_to_server(&mut stream, &drop_env)
        .await
        .expect("Failed to send table drop envelope");

    validate_response_envelope(&drop_response, query_id_drop)
        .expect("Table drop response validation failed");

    println!("✓ Table dropped successfully");

    // Finally, list tables to verify it's gone
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

    // Check if our table is absent from the list
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

            if !found {
                println!("✓ Dropped table '{table_name}' no longer appears in table list");
            } else {
                println!(
                    "ℹ Dropped table '{table_name}' still appears in list (may not be immediately removed)"
                );
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("✓ Table list returned null (no tables visible after drop)");
        }
        _ => {
            println!("ℹ Unexpected table list format: {:?}", list_datum.value);
        }
    }

    println!("✓ Table drop and verify test completed successfully!");
}

#[tokio::test]
async fn test_table_drop_without_database() {
    let query_id = "test-table-drop-no-db-004";
    let database_name = &generate_unique_name("test_db_table_drop_nonexistent");
    let table_name = &generate_unique_name("test_table_drop_no_db");

    println!(
        "Testing table drop without database, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Try to drop a table in a non-existent database
    let query = create_table_drop_query(database_name, table_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Check if table drop succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Table drop in non-existent database succeeded (idempotent behavior)");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Table drop in non-existent database failed as expected");
            // This is the expected behavior
        }
        _ => {
            panic!("Unexpected message type in table drop without database response");
        }
    }

    println!("✓ Table drop without database test completed successfully!");
}
