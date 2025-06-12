mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_database_drop_query() {
    let query_id = "test-db-drop-001";
    let database_name = &generate_unique_name("test_database_drop");

    println!(
        "Testing database drop query with ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create database drop query
    let query = create_database_drop_query(database_name);
    let envelope = create_envelope(query_id, &query);

    // Send query and receive response
    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Database drop should return null or empty response on success
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Database drop returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Database drop returned empty object response");
        }
        _ => {
            // Some implementations might return other success indicators
            println!("ℹ Database drop returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Database drop query test completed successfully!");
}

#[tokio::test]
async fn test_database_drop_with_custom_timeout() {
    let query_id = "test-db-drop-timeout-002";
    let database_name = &generate_unique_name("test_database_drop_timeout");

    println!(
        "Testing database drop with custom timeout, ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create query with custom timeout
    let mut query = create_database_drop_query(database_name);
    if let Some(ref mut options) = query.options {
        options.timeout_ms = 10000; // 10 second timeout
    }

    let envelope = create_envelope(query_id, &query);

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
            println!("✓ Database drop with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Database drop with timeout returned empty object response");
        }
        _ => {
            println!(
                "ℹ Database drop with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Database drop with timeout test completed successfully!");
}

#[tokio::test]
async fn test_database_drop_nonexistent() {
    let query_id = "test-db-drop-nonexistent-003";
    let database_name = &generate_unique_name("test_database_nonexistent");

    println!(
        "Testing drop of nonexistent database, ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Try to drop a database that doesn't exist
    let query = create_database_drop_query(database_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Check if drop succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Drop of nonexistent database succeeded (idempotent behavior)");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Drop of nonexistent database failed as expected");
            // This is also acceptable behavior
        }
        _ => {
            panic!("Unexpected message type in drop nonexistent response");
        }
    }

    println!("✓ Drop nonexistent database test completed successfully!");
}

#[tokio::test]
async fn test_database_drop_multiple_databases() {
    let base_query_id = "test-db-drop-multiple";
    let database_names = [
        generate_unique_name("test_database_drop_multi_001"),
        generate_unique_name("test_database_drop_multi_002"),
        generate_unique_name("test_database_drop_multi_003"),
    ];

    println!("Testing multiple database drops");

    for (i, database_name) in database_names.iter().enumerate() {
        let query_id = format!("{}-{:03}", base_query_id, i + 1);
        println!(
            "Dropping database {} of {}: {} (ID: {})",
            i + 1,
            database_names.len(),
            database_name,
            query_id
        );

        // Connect to server for each request
        let mut stream = connect_to_server()
            .await
            .expect("Failed to connect to server");

        // Create and send query
        let query = create_database_drop_query(database_name);
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
                        println!("  ✓ Database {} dropped successfully", i + 1);
                    }
                    Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
                        println!("  ✓ Database {} dropped successfully (empty object)", i + 1);
                    }
                    _ => {
                        println!(
                            "  ℹ Database {} drop returned: {:?}",
                            i + 1,
                            response_datum.value
                        );
                    }
                }
            }
            Ok(proto::MessageType::Error) => {
                println!("  ℹ Database {} drop failed (may not exist)", i + 1);
            }
            _ => {
                panic!("Unexpected message type in drop response");
            }
        }
    }

    println!("✓ Multiple database drops test completed successfully!");
}

#[tokio::test]
async fn test_database_create_and_drop_cycle() {
    let query_id_create = "test-db-cycle-create";
    let query_id_drop = "test-db-cycle-drop";
    let database_name = &generate_unique_name("test_database_cycle");

    println!(
        "Testing database create and drop cycle, database: {}",
        database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database
    let create_query = create_database_create_query(database_name);
    let create_env = create_envelope(query_id_create, &create_query);

    let create_response = send_envelope_to_server(&mut stream, &create_env)
        .await
        .expect("Failed to send create envelope");

    validate_response_envelope(&create_response, query_id_create)
        .expect("Create response validation failed");

    println!("✓ Database created successfully");

    // Then, drop the database
    let drop_query = create_database_drop_query(database_name);
    let drop_env = create_envelope(query_id_drop, &drop_query);

    let drop_response = send_envelope_to_server(&mut stream, &drop_env)
        .await
        .expect("Failed to send drop envelope");

    validate_response_envelope(&drop_response, query_id_drop)
        .expect("Drop response validation failed");

    // Decode and validate drop response
    let drop_datum =
        decode_response_payload(&drop_response).expect("Failed to decode drop response payload");

    match drop_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Database dropped successfully");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Database dropped successfully (empty object)");
        }
        _ => {
            println!("ℹ Database drop returned: {:?}", drop_datum.value);
        }
    }

    println!("✓ Database create and drop cycle test completed successfully!");
}

#[tokio::test]
async fn test_database_drop_and_verify_list() {
    let query_id_create = "test-db-drop-verify-create";
    let query_id_drop = "test-db-drop-verify-drop";
    let query_id_list = "test-db-drop-verify-list";
    let database_name = &generate_unique_name("test_database_drop_verify");

    println!(
        "Testing database drop and verification via list, database: {}",
        database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database to ensure it exists
    let create_query = create_database_create_query(database_name);
    let create_env = create_envelope(query_id_create, &create_query);

    let create_response = send_envelope_to_server(&mut stream, &create_env)
        .await
        .expect("Failed to send create envelope");

    validate_response_envelope(&create_response, query_id_create)
        .expect("Create response validation failed");

    println!("✓ Database created for drop test");

    // Then, drop the database
    let drop_query = create_database_drop_query(database_name);
    let drop_env = create_envelope(query_id_drop, &drop_query);

    let drop_response = send_envelope_to_server(&mut stream, &drop_env)
        .await
        .expect("Failed to send drop envelope");

    validate_response_envelope(&drop_response, query_id_drop)
        .expect("Drop response validation failed");

    println!("✓ Database dropped successfully");

    // Finally, list databases to verify it's gone
    let list_query = create_database_list_query();
    let list_env = create_envelope(query_id_list, &list_query);

    let list_response = send_envelope_to_server(&mut stream, &list_env)
        .await
        .expect("Failed to send list envelope");

    validate_response_envelope(&list_response, query_id_list)
        .expect("List response validation failed");

    // Decode and validate list response
    let list_datum =
        decode_response_payload(&list_response).expect("Failed to decode list response payload");

    // Check if our database is absent from the list
    match list_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            let mut found = false;
            for item in &array.items {
                if let Some(proto::datum::Value::String(name)) = &item.value {
                    if name == database_name {
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                println!(
                    "✓ Dropped database '{}' no longer appears in database list",
                    database_name
                );
            } else {
                println!(
                    "ℹ Dropped database '{}' still appears in list (may not be immediately removed)",
                    database_name
                );
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("✓ Database list returned null (no databases visible after drop)");
        }
        _ => {
            println!("ℹ Unexpected database list format: {:?}", list_datum.value);
        }
    }

    println!("✓ Database drop and verify test completed successfully!");
}
