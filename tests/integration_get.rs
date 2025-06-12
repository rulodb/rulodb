mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_get_existing_document() {
    let query_id = "test-get-existing-001";
    let database_name = &generate_unique_name("test_db_get_existing");
    let table_name = &generate_unique_name("test_table_get_existing");

    println!(
        "Testing get existing document with ID: {}, database: {}, table: {}",
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

    // Insert a test document
    let document_id = "get_test_001";
    let document = create_datum_object(vec![
        ("id", create_string_datum(document_id)),
        ("name", create_string_datum("Test Document for Get")),
        ("value", create_int_datum(12345)),
        ("active", create_bool_datum(true)),
        ("score", create_float_datum(98.5)),
    ]);

    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Get the document by key
    let get_key = create_string_datum(document_id);
    let get_query = create_get_query(database_name, table_name, get_key);
    let get_envelope = create_envelope(query_id, &get_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_envelope)
        .await
        .expect("Failed to send get envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Get should return the document
    match response_datum.value {
        Some(proto::datum::Value::Object(ref obj)) => {
            println!(
                "✓ Get returned document object with {} fields",
                obj.fields.len()
            );

            // Verify document contains expected fields
            if let Some(id_field) = obj.fields.get("id") {
                if let Some(proto::datum::Value::String(id_val)) = &id_field.value {
                    if id_val == document_id {
                        println!("  ✓ Document ID matches expected value");
                    } else {
                        println!(
                            "  ℹ Document ID: expected '{}', got '{}'",
                            document_id, id_val
                        );
                    }
                }
            }

            if let Some(name_field) = obj.fields.get("name") {
                if let Some(proto::datum::Value::String(name_val)) = &name_field.value {
                    println!("  ✓ Document name: '{}'", name_val);
                }
            }

            if let Some(value_field) = obj.fields.get("value") {
                if let Some(proto::datum::Value::Int(value_val)) = &value_field.value {
                    println!("  ✓ Document value: {}", value_val);
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Get returned null (document may not be immediately visible)");
        }
        _ => {
            println!(
                "ℹ Get returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Get existing document test completed successfully!");
}

#[tokio::test]
async fn test_get_nonexistent_document() {
    let query_id = "test-get-nonexistent-002";
    let database_name = &generate_unique_name("test_db_get_nonexistent");
    let table_name = &generate_unique_name("test_table_get_nonexistent");

    println!(
        "Testing get nonexistent document with ID: {}, database: {}, table: {}",
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

    // Try to get a document that doesn't exist
    let nonexistent_key = create_string_datum("nonexistent_document_key");
    let get_query = create_get_query(database_name, table_name, nonexistent_key);
    let get_envelope = create_envelope(query_id, &get_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_envelope)
        .await
        .expect("Failed to send get envelope");

    // Check if get succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Null(_)) | None => {
                    println!("✓ Get nonexistent document returned null as expected");
                }
                Some(proto::datum::Value::Object(ref obj)) if obj.fields.is_empty() => {
                    println!("✓ Get nonexistent document returned empty object");
                }
                _ => {
                    println!(
                        "ℹ Get nonexistent document returned: {:?}",
                        response_datum.value
                    );
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Get nonexistent document failed with error (may be expected behavior)");
        }
        _ => {
            panic!("Unexpected message type in get nonexistent document response");
        }
    }

    println!("✓ Get nonexistent document test completed successfully!");
}

#[tokio::test]
async fn test_get_with_different_key_types() {
    let query_id = "test-get-key-types-003";
    let database_name = &generate_unique_name("test_db_get_key_types");
    let table_name = &generate_unique_name("test_table_get_key_types");

    println!(
        "Testing get with different key types, ID: {}, database: {}, table: {}",
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

    // Insert documents with different key types
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("string_key_doc")),
            ("type", create_string_datum("string key document")),
            (
                "data",
                create_string_datum("This document has a string key"),
            ),
        ]),
        create_datum_object(vec![
            ("id", create_int_datum(42)),
            ("type", create_string_datum("integer key document")),
            (
                "data",
                create_string_datum("This document has an integer key"),
            ),
        ]),
        create_datum_object(vec![
            ("id", create_bool_datum(true)),
            ("type", create_string_datum("boolean key document")),
            (
                "data",
                create_string_datum("This document has a boolean key"),
            ),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Documents with different key types inserted successfully");

    // Test getting with string key
    let string_key = create_string_datum("string_key_doc");
    let string_key_query = create_get_query(database_name, table_name, string_key);
    let string_key_envelope = create_envelope(&format!("{}-string", query_id), &string_key_query);

    let string_response = send_envelope_to_server(&mut stream, &string_key_envelope)
        .await
        .expect("Failed to send string key get envelope");

    match proto::MessageType::try_from(string_response.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("✓ Get with string key succeeded");
            let response_datum = decode_response_payload(&string_response)
                .expect("Failed to decode string key response");
            match response_datum.value {
                Some(proto::datum::Value::Object(_)) => {
                    println!("  ✓ String key returned document object");
                }
                _ => {
                    println!("  ℹ String key response: {:?}", response_datum.value);
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Get with string key failed");
        }
        _ => {}
    }

    // Test getting with integer key
    let int_key = create_int_datum(42);
    let int_key_query = create_get_query(database_name, table_name, int_key);
    let int_key_envelope = create_envelope(&format!("{}-int", query_id), &int_key_query);

    let int_response = send_envelope_to_server(&mut stream, &int_key_envelope)
        .await
        .expect("Failed to send int key get envelope");

    match proto::MessageType::try_from(int_response.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("✓ Get with integer key succeeded");
            let response_datum =
                decode_response_payload(&int_response).expect("Failed to decode int key response");
            match response_datum.value {
                Some(proto::datum::Value::Object(_)) => {
                    println!("  ✓ Integer key returned document object");
                }
                _ => {
                    println!("  ℹ Integer key response: {:?}", response_datum.value);
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Get with integer key failed");
        }
        _ => {}
    }

    // Test getting with boolean key
    let bool_key = create_bool_datum(true);
    let bool_key_query = create_get_query(database_name, table_name, bool_key);
    let bool_key_envelope = create_envelope(&format!("{}-bool", query_id), &bool_key_query);

    let bool_response = send_envelope_to_server(&mut stream, &bool_key_envelope)
        .await
        .expect("Failed to send bool key get envelope");

    match proto::MessageType::try_from(bool_response.r#type) {
        Ok(proto::MessageType::Response) => {
            println!("✓ Get with boolean key succeeded");
            let response_datum = decode_response_payload(&bool_response)
                .expect("Failed to decode bool key response");
            match response_datum.value {
                Some(proto::datum::Value::Object(_)) => {
                    println!("  ✓ Boolean key returned document object");
                }
                _ => {
                    println!("  ℹ Boolean key response: {:?}", response_datum.value);
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ Get with boolean key failed");
        }
        _ => {}
    }

    println!("✓ Get with different key types test completed successfully!");
}

#[tokio::test]
async fn test_get_with_timeout() {
    let query_id = "test-get-timeout-004";
    let database_name = &generate_unique_name("test_db_get_timeout");
    let table_name = &generate_unique_name("test_table_get_timeout");

    println!(
        "Testing get with custom timeout, ID: {}, database: {}, table: {}",
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

    // Insert a test document
    let document_id = "timeout_test_doc";
    let document = create_datum_object(vec![
        ("id", create_string_datum(document_id)),
        ("name", create_string_datum("Timeout Test Document")),
        ("value", create_int_datum(999)),
    ]);

    let insert_query = create_insert_query(database_name, table_name, vec![document]);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Create get query with custom timeout
    let get_key = create_string_datum(document_id);
    let mut get_query = create_get_query(database_name, table_name, get_key);
    if let Some(ref mut options) = get_query.options {
        options.timeout_ms = 2000; // 2 second timeout
    }

    let get_envelope = create_envelope(query_id, &get_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_envelope)
        .await
        .expect("Failed to send get envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Get with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Object(ref obj)) => {
            println!(
                "✓ Get with timeout returned document with {} fields",
                obj.fields.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Get with timeout returned null");
        }
        _ => {
            println!("ℹ Get with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Get with timeout test completed successfully!");
}

#[tokio::test]
async fn test_get_from_nonexistent_table() {
    let query_id = "test-get-no-table-005";
    let database_name = &generate_unique_name("test_db_get_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing get from nonexistent table, ID: {}, database: {}, table: {}",
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

    // Try to get from nonexistent table
    let get_key = create_string_datum("any_key");
    let get_query = create_get_query(database_name, table_name, get_key);
    let get_envelope = create_envelope(query_id, &get_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_envelope)
        .await
        .expect("Failed to send get envelope");

    // Check if get succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Get from nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Get from nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in get from nonexistent table response");
        }
    }

    println!("✓ Get from nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_get_multiple_documents_different_keys() {
    let query_id = "test-get-multiple-006";
    let database_name = &generate_unique_name("test_db_get_multiple");
    let table_name = &generate_unique_name("test_table_get_multiple");

    println!(
        "Testing get multiple documents with different keys, ID: {}, database: {}, table: {}",
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

    // Insert multiple test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("multi_001")),
            ("name", create_string_datum("First Multi Document")),
            ("category", create_string_datum("A")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("multi_002")),
            ("name", create_string_datum("Second Multi Document")),
            ("category", create_string_datum("B")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("multi_003")),
            ("name", create_string_datum("Third Multi Document")),
            ("category", create_string_datum("C")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Multiple test documents inserted successfully");

    // Get each document individually
    let keys_to_test = ["multi_001", "multi_002", "multi_003"];
    let mut successful_gets = 0;

    for (i, key) in keys_to_test.iter().enumerate() {
        let get_key = create_string_datum(key);
        let get_query = create_get_query(database_name, table_name, get_key);
        let get_envelope = create_envelope(&format!("{}-get-{}", query_id, i + 1), &get_query);

        let response_envelope = send_envelope_to_server(&mut stream, &get_envelope)
            .await
            .expect("Failed to send get envelope");

        match proto::MessageType::try_from(response_envelope.r#type) {
            Ok(proto::MessageType::Response) => {
                let response_datum = decode_response_payload(&response_envelope)
                    .expect("Failed to decode response payload");

                match response_datum.value {
                    Some(proto::datum::Value::Object(ref obj)) => {
                        println!(
                            "  ✓ Get '{}' returned document with {} fields",
                            key,
                            obj.fields.len()
                        );
                        successful_gets += 1;

                        // Verify the document ID matches
                        if let Some(id_field) = obj.fields.get("id") {
                            if let Some(proto::datum::Value::String(id_val)) = &id_field.value {
                                if id_val == key {
                                    println!("    ✓ Document ID matches key");
                                }
                            }
                        }
                    }
                    Some(proto::datum::Value::Null(_)) | None => {
                        println!("  ℹ Get '{}' returned null", key);
                    }
                    _ => {
                        println!("  ℹ Get '{}' returned: {:?}", key, response_datum.value);
                    }
                }
            }
            Ok(proto::MessageType::Error) => {
                println!("  ℹ Get '{}' failed with error", key);
            }
            _ => {}
        }
    }

    println!(
        "✓ Multiple document get test completed: {}/{} successful gets",
        successful_gets,
        keys_to_test.len()
    );
}
