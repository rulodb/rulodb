mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_database_list_query() {
    let query_id = "test-db-list-001";

    println!("Testing database list query with ID: {query_id}");

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create database list query
    let query = create_database_list_query();
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

    // The response should be an array of database names
    match response_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            println!("Successfully retrieved {} databases", array.items.len());

            // Validate that each database name is a string
            for (i, item) in array.items.iter().enumerate() {
                match &item.value {
                    Some(proto::datum::Value::String(name)) => {
                        println!("  Database {}: {}", i + 1, name);
                    }
                    _ => panic!("Database entry {i} should be a string, got: {item:?}"),
                }
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("No databases found (null result)");
        }
        _ => panic!(
            "Database list response should be an array or null, got: {:?}",
            response_datum.value
        ),
    }

    println!("✓ Database list query test completed successfully!");
}

#[tokio::test]
async fn test_database_list_query_with_timeout() {
    let query_id = "test-db-list-timeout-002";

    println!("Testing database list query with custom timeout, ID: {query_id}");

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Create query with custom timeout
    let mut query = create_database_list_query();
    if let Some(ref mut options) = query.options {
        options.timeout_ms = 5000; // 5 second timeout
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

    // Validate response format
    match response_datum.value {
        Some(proto::datum::Value::Array(_)) | Some(proto::datum::Value::Null(_)) => {
            println!("✓ Received valid response format");
        }
        _ => panic!(
            "Database list response should be array or null, got: {:?}",
            response_datum.value
        ),
    }

    println!("✓ Database list query with timeout test completed successfully!");
}

#[tokio::test]
async fn test_database_list_multiple_requests() {
    let query_ids = [
        "test-db-list-multi-001",
        "test-db-list-multi-002",
        "test-db-list-multi-003",
    ];

    println!("Testing multiple database list requests");

    for (i, query_id) in query_ids.iter().enumerate() {
        println!(
            "Sending request {} of {}: {}",
            i + 1,
            query_ids.len(),
            query_id
        );

        // Connect to server for each request
        let mut stream = connect_to_server()
            .await
            .expect("Failed to connect to server");

        // Create and send query
        let query = create_database_list_query();
        let envelope = create_envelope(query_id, &query);

        let response_envelope = send_envelope_to_server(&mut stream, &envelope)
            .await
            .expect("Failed to send envelope and receive response");

        // Validate basic response structure
        validate_response_envelope(&response_envelope, query_id)
            .expect("Response validation failed");

        // Decode and validate response
        let response_datum =
            decode_response_payload(&response_envelope).expect("Failed to decode response payload");

        // Validate response format
        match response_datum.value {
            Some(proto::datum::Value::Array(_)) | Some(proto::datum::Value::Null(_)) => {
                println!("  ✓ Request {} completed successfully", i + 1);
            }
            _ => panic!("Response should be array or null"),
        }
    }

    println!("✓ Multiple database list requests test completed successfully!");
}
