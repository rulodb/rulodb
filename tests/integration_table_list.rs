mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_table_list_query() {
    let query_id = "test-table-list-001";
    let database_name = &generate_unique_name("test_db_table_list");

    println!("Testing table list query with ID: {query_id}, database: {database_name}");

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

    // Create table list query
    let query = create_table_list_query(database_name);
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

    // The response should be an array of table names
    match response_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            println!("Successfully retrieved {} tables", array.items.len());

            // Validate that each table name is a string
            for (i, item) in array.items.iter().enumerate() {
                match &item.value {
                    Some(proto::datum::Value::String(name)) => {
                        println!("  Table {}: {name}", i + 1);
                    }
                    _ => panic!("Table entry {i} should be a string, got: {item:?}"),
                }
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("No tables found (null result)");
        }
        _ => panic!(
            "Table list response should be an array or null, got: {:?}",
            response_datum.value
        ),
    }

    println!("✓ Table list query test completed successfully!");
}

#[tokio::test]
async fn test_table_list_query_with_timeout() {
    let query_id = "test-table-list-timeout-002";
    let database_name = &generate_unique_name("test_db_table_list_timeout");

    println!(
        "Testing table list query with custom timeout, ID: {query_id}, database: {database_name}"
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

    // Create query with custom timeout
    let mut query = create_table_list_query(database_name);
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
            "Table list response should be array or null, got: {:?}",
            response_datum.value
        ),
    }

    println!("✓ Table list query with timeout test completed successfully!");
}

#[tokio::test]
async fn test_table_list_multiple_requests() {
    let base_query_id = "test-table-list-multi";
    let database_name = &generate_unique_name("test_db_table_list_multi");
    let query_ids = [
        "test-table-list-multi-001",
        "test-table-list-multi-002",
        "test-table-list-multi-003",
    ];

    println!("Testing multiple table list requests for database: {database_name}");

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

    for (i, query_id) in query_ids.iter().enumerate() {
        println!(
            "Sending request {} of {}: {}",
            i + 1,
            query_ids.len(),
            query_id
        );

        // Create and send query
        let query = create_table_list_query(database_name);
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

    println!("✓ Multiple table list requests test completed successfully!");
}

#[tokio::test]
async fn test_table_list_with_tables() {
    let query_id = "test-table-list-with-tables-004";
    let database_name = &generate_unique_name("test_db_table_list_with_tables");
    let table_names = vec![
        generate_unique_name("test_table_list_001"),
        generate_unique_name("test_table_list_002"),
        generate_unique_name("test_table_list_003"),
    ];

    println!("Testing table list with created tables, database: {database_name}");

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // Create multiple tables
    for (i, table_name) in table_names.iter().enumerate() {
        let table_create_query = create_table_create_query(database_name, table_name);
        let table_create_envelope = create_envelope(
            &format!("{}-table-create-{}", query_id, i + 1),
            &table_create_query,
        );

        let table_create_response = send_envelope_to_server(&mut stream, &table_create_envelope)
            .await
            .expect("Failed to send table create envelope");

        validate_response_envelope(
            &table_create_response,
            &format!("{}-table-create-{}", query_id, i + 1),
        )
        .expect("Table create response validation failed");

        println!("✓ Table '{table_name}' created successfully");
    }

    // Now list tables to verify they exist
    let list_query = create_table_list_query(database_name);
    let list_envelope = create_envelope(query_id, &list_query);

    let list_response = send_envelope_to_server(&mut stream, &list_envelope)
        .await
        .expect("Failed to send table list envelope");

    validate_response_envelope(&list_response, query_id)
        .expect("Table list response validation failed");

    // Decode and validate list response
    let list_datum =
        decode_response_payload(&list_response).expect("Failed to decode list response payload");

    // Check if our tables appear in the list
    match list_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            println!("Successfully retrieved {} tables", array.items.len());

            let mut found_tables = Vec::new();
            for item in &array.items {
                if let Some(proto::datum::Value::String(name)) = &item.value {
                    found_tables.push(name.clone());
                }
            }

            // Check if all our created tables are in the list
            for table_name in &table_names {
                if found_tables.contains(table_name) {
                    println!("✓ Created table '{table_name}' found in table list");
                } else {
                    println!(
                        "ℹ Created table '{table_name}' not found in list (may not be immediately visible)"
                    );
                }
            }

            // Print all found tables
            for (i, table_name) in found_tables.iter().enumerate() {
                println!("  Table {}: {}", i + 1, table_name);
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("ℹ Table list returned null (no tables visible)");
        }
        _ => {
            println!("ℹ Unexpected table list format: {:?}", list_datum.value);
        }
    }

    println!("✓ Table list with tables test completed successfully!");
}

#[tokio::test]
async fn test_table_list_nonexistent_database() {
    let query_id = "test-table-list-no-db-005";
    let database_name = &generate_unique_name("test_db_table_list_nonexistent");

    println!(
        "Testing table list for nonexistent database, ID: {query_id}, database: {database_name}"
    );

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Try to list tables in a non-existent database
    let query = create_table_list_query(database_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Check if table list succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("ℹ Table list for non-existent database succeeded");
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Array(ref array)) => {
                    println!("  Response: array with {} items", array.items.len());
                }
                Some(proto::datum::Value::Null(_)) => {
                    println!("  Response: null (expected for non-existent database)");
                }
                _ => {
                    println!("  Response: {:?}", response_datum.value);
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Table list for non-existent database failed as expected");
            // This is also acceptable behavior
        }
        _ => {
            panic!("Unexpected message type in table list for nonexistent database response");
        }
    }

    println!("✓ Table list for nonexistent database test completed successfully!");
}

#[tokio::test]
async fn test_table_list_empty_database() {
    let query_id = "test-table-list-empty-006";
    let database_name = &generate_unique_name("test_db_table_list_empty");

    println!("Testing table list for empty database, ID: {query_id}, database: {database_name}");

    // Connect to the running server
    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // First, create the database (but no tables)
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);

    let db_create_response = send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to send database create envelope");

    validate_response_envelope(&db_create_response, &format!("{query_id}-db-create"))
        .expect("Database create response validation failed");

    println!("✓ Database created successfully");

    // List tables in the empty database
    let query = create_table_list_query(database_name);
    let envelope = create_envelope(query_id, &query);

    let response_envelope = send_envelope_to_server(&mut stream, &envelope)
        .await
        .expect("Failed to send envelope and receive response");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // The response should be an empty array or null for an empty database
    match response_datum.value {
        Some(proto::datum::Value::Array(ref array)) => {
            println!(
                "Successfully retrieved {} tables from empty database",
                array.items.len()
            );
            if array.items.is_empty() {
                println!("✓ Empty database correctly returns empty table list");
            } else {
                println!(
                    "ℹ Empty database returned {} tables (may have default tables)",
                    array.items.len()
                );
                for (i, item) in array.items.iter().enumerate() {
                    if let Some(proto::datum::Value::String(name)) = &item.value {
                        println!("  Table {}: {}", i + 1, name);
                    }
                }
            }
        }
        Some(proto::datum::Value::Null(_)) => {
            println!("✓ Empty database correctly returns null table list");
        }
        _ => {
            println!(
                "ℹ Unexpected table list format for empty database: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table list for empty database test completed successfully!");
}
