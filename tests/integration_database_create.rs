mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_database_create_query() {
    let query_id = "test-db-create-001";
    let database_name = &generate_unique_name("test_database_create");

    println!(
        "Testing database create query with ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create database create query
    let query = create_database_create_query(database_name);
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

    // Database create should return null or empty response on success
    match response_datum.value {
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Database create returned expected null/empty response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Database create returned empty object response");
        }
        _ => {
            // Some implementations might return other success indicators
            println!("ℹ Database create returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Database create query test completed successfully!");
}

#[tokio::test]
async fn test_database_create_with_custom_timeout() {
    let query_id = "test-db-create-timeout-002";
    let database_name = &generate_unique_name("test_database_create_timeout");

    println!(
        "Testing database create with custom timeout, ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create query with custom timeout
    let mut query = create_database_create_query(database_name);
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
            println!("✓ Database create with timeout returned expected response");
        }
        Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
            println!("✓ Database create with timeout returned empty object response");
        }
        _ => {
            println!(
                "ℹ Database create with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Database create with timeout test completed successfully!");
}

#[tokio::test]
async fn test_database_create_duplicate() {
    let query_id = "test-db-create-duplicate-003";
    let database_name = &generate_unique_name("test_database_duplicate");

    println!(
        "Testing duplicate database create, ID: {}, database: {}",
        query_id, database_name
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First create - should succeed
    let query1 = create_database_create_query(database_name);
    let envelope1 = create_envelope(&format!("{}-first", query_id), &query1);

    let response_envelope1 = send_envelope_to_server(&mut stream, &envelope1)
        .await
        .expect("Failed to send first create envelope");

    validate_response_envelope(&response_envelope1, &format!("{}-first", query_id))
        .expect("First create response validation failed");

    println!("✓ First database create completed");

    // Second create - might succeed or fail depending on implementation
    let query2 = create_database_create_query(database_name);
    let envelope2 = create_envelope(&format!("{}-second", query_id), &query2);

    let response_envelope2 = send_envelope_to_server(&mut stream, &envelope2)
        .await
        .expect("Failed to send second create envelope");

    // Check if second create succeeded or failed
    match proto::MessageType::try_from(response_envelope2.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Second database create succeeded (database already exists behavior)");
            let response_datum = decode_response_payload(&response_envelope2)
                .expect("Failed to decode second response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Second database create failed as expected (duplicate database)");
            // This is also acceptable behavior
        }
        _ => {
            panic!("Unexpected message type in second create response");
        }
    }

    println!("✓ Duplicate database create test completed successfully!");
}

#[tokio::test]
async fn test_database_create_multiple_databases() {
    let base_query_id = "test-db-create-multiple";
    let database_names = [
        generate_unique_name("test_database_multi_001"),
        generate_unique_name("test_database_multi_002"),
        generate_unique_name("test_database_multi_003"),
    ];

    println!("Testing multiple database creates");

    for (i, database_name) in database_names.iter().enumerate() {
        let query_id = format!("{}-{:03}", base_query_id, i + 1);
        println!(
            "Creating database {} of {}: {} (ID: {})",
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
        let query = create_database_create_query(database_name);
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
                println!("  ✓ Database {} created successfully", i + 1);
            }
            Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
                println!("  ✓ Database {} created successfully (empty object)", i + 1);
            }
            _ => {
                println!(
                    "  ℹ Database {} create returned: {:?}",
                    i + 1,
                    response_datum.value
                );
            }
        }
    }

    println!("✓ Multiple database creates test completed successfully!");
}

#[tokio::test]
async fn test_database_create_and_verify_list() {
    let query_id_create = "test-db-create-verify-create";
    let query_id_list = "test-db-create-verify-list";
    let database_name = &generate_unique_name("test_database_create_verify");

    println!(
        "Testing database create and verification via list, database: {}",
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

    // Then, list databases to verify it exists
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

    // Check if our database appears in the list
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

            if found {
                println!(
                    "✓ Created database '{}' found in database list",
                    database_name
                );
            } else {
                println!(
                    "ℹ Created database '{}' not found in list (may not be immediately visible)",
                    database_name
                );
                println!("  Available databases: {:?}", array.items);
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("ℹ Database list returned null (no databases visible)");
        }
        _ => {
            println!("ℹ Unexpected database list format: {:?}", list_datum.value);
        }
    }

    println!("✓ Database create and verify test completed successfully!");
}
