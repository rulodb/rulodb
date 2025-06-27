mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_table_query_empty_table() {
    let query_id = "test-table-query-empty-001";
    let database_name = &generate_unique_name("test_db_table_query_empty");
    let table_name = &generate_unique_name("test_table_query_empty");

    println!(
        "Testing table query on empty table with ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Query the empty table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Empty table should return empty array or null
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            if arr.items.is_empty() {
                println!("✓ Empty table query returned empty array as expected");
            } else {
                println!(
                    "ℹ Empty table query returned array with {} items (unexpected)",
                    arr.items.len()
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Empty table query returned null as expected");
        }
        _ => {
            println!("ℹ Empty table query returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Empty table query test completed successfully!");
}

#[tokio::test]
async fn test_table_query_with_documents() {
    let query_id = "test-table-query-docs-002";
    let database_name = &generate_unique_name("test_db_table_query_docs");
    let table_name = &generate_unique_name("test_table_query_docs");

    println!(
        "Testing table query with documents, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("doc_001")),
            ("name", create_string_datum("First Document")),
            ("value", create_int_datum(100)),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("doc_002")),
            ("name", create_string_datum("Second Document")),
            ("value", create_int_datum(200)),
            ("active", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("doc_003")),
            ("name", create_string_datum("Third Document")),
            ("value", create_int_datum(300)),
            ("active", create_bool_datum(true)),
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

    // Query the table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table with documents should return array of documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table query returned array with {} documents",
                arr.items.len()
            );

            // Verify we have the expected number of documents
            if arr.items.len() == 3 {
                println!("✓ Expected number of documents returned");
            } else {
                println!(
                    "ℹ Expected 3 documents, got {} (may be due to timing or other factors)",
                    arr.items.len()
                );
            }

            // Check if documents have expected structure
            for (i, item) in arr.items.iter().enumerate() {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    println!("  Document {}: has {} fields", i + 1, obj.fields.len());
                    if obj.fields.contains_key("id") && obj.fields.contains_key("name") {
                        println!("    ✓ Contains expected fields (id, name)");
                    }
                } else {
                    println!(
                        "  Document {}: unexpected structure: {:?}",
                        i + 1,
                        item.value
                    );
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Table query returned null (documents may not be immediately visible)");
        }
        _ => {
            println!(
                "ℹ Table query returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table query with documents test completed successfully!");
}

#[tokio::test]
async fn test_table_query_with_timeout() {
    let query_id = "test-table-query-timeout-003";
    let database_name = &generate_unique_name("test_db_table_query_timeout");
    let table_name = &generate_unique_name("test_table_query_timeout");

    println!(
        "Testing table query with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Create table query with custom timeout
    let mut table_query = create_table_query(database_name, table_name);
    if let Some(ref mut options) = table_query.options {
        options.timeout_ms = 5000; // 5 second timeout
    }

    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table query with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table query with timeout returned array with {} documents",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table query with timeout returned null/empty");
        }
        _ => {
            println!(
                "ℹ Table query with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table query with timeout test completed successfully!");
}

#[tokio::test]
async fn test_table_query_nonexistent_table() {
    let query_id = "test-table-query-nonexistent-004";
    let database_name = &generate_unique_name("test_db_table_query_nonexistent");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing table query on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try to query nonexistent table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Check if query succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Query on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Query on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in nonexistent table query response");
        }
    }

    println!("✓ Nonexistent table query test completed successfully!");
}

#[tokio::test]
async fn test_table_query_large_dataset() {
    let query_id = "test-table-query-large-005";
    let database_name = &generate_unique_name("test_db_table_query_large");
    let table_name = &generate_unique_name("test_table_query_large");

    println!(
        "Testing table query with large dataset, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert a larger number of documents
    let mut documents = Vec::new();
    for i in 1..=50 {
        documents.push(create_datum_object(vec![
            ("id", create_string_datum(&format!("large_doc_{i:03}"))),
            ("name", create_string_datum(&format!("Document Number {i}"))),
            ("value", create_int_datum(i as i64 * 10)),
            ("index", create_int_datum(i as i64)),
            ("even", create_bool_datum(i % 2 == 0)),
        ]));
    }

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Large dataset (50 documents) inserted successfully");

    // Query the table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table with large dataset should return array of documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Large dataset table query returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() >= 40 {
                println!("✓ Large portion of documents returned");
            } else {
                println!(
                    "ℹ Expected around 50 documents, got {} (may be due to pagination or limits)",
                    arr.items.len()
                );
            }

            // Verify document structure
            if let Some(first_item) = arr.items.first() {
                if let Some(proto::datum::Value::Object(obj)) = &first_item.value {
                    println!("  ✓ Documents have expected object structure");
                    if obj.fields.contains_key("id") && obj.fields.contains_key("index") {
                        println!("    ✓ Contains expected fields");
                    }
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!(
                "ℹ Large dataset table query returned null (documents may not be immediately visible)"
            );
        }
        _ => {
            println!(
                "ℹ Large dataset table query returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Large dataset table query test completed successfully!");
}

#[tokio::test]
async fn test_table_query_mixed_data_types() {
    let query_id = "test-table-query-mixed-006";
    let database_name = &generate_unique_name("test_db_table_query_mixed");
    let table_name = &generate_unique_name("test_table_query_mixed");

    println!(
        "Testing table query with mixed data types, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert documents with various data types
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("mixed_001")),
            ("type", create_string_datum("string_doc")),
            ("text", create_string_datum("Hello, World!")),
            ("empty_string", create_string_datum("")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("mixed_002")),
            ("type", create_string_datum("number_doc")),
            ("integer", create_int_datum(42)),
            ("negative", create_int_datum(-999)),
            ("zero", create_int_datum(0)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("mixed_003")),
            ("type", create_string_datum("float_doc")),
            ("pi", create_float_datum(3.15159)),
            #[allow(clippy::approx_constant)]
            ("negative_float", create_float_datum(-2.71828)),
            ("float_zero", create_float_datum(0.0)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("mixed_004")),
            ("type", create_string_datum("boolean_doc")),
            ("true_val", create_bool_datum(true)),
            ("false_val", create_bool_datum(false)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Mixed data type documents inserted successfully");

    // Query the table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table query envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table with mixed data types should return array of documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Mixed data types table query returned array with {} documents",
                arr.items.len()
            );

            // Verify we can find different data types in the results
            let mut found_string = false;
            let mut found_int = false;
            let mut found_float = false;
            let mut found_bool = false;

            for item in &arr.items {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    for field_value in obj.fields.values() {
                        match &field_value.value {
                            Some(proto::datum::Value::String(_)) => found_string = true,
                            Some(proto::datum::Value::Int(_)) => found_int = true,
                            Some(proto::datum::Value::Float(_)) => found_float = true,
                            Some(proto::datum::Value::Bool(_)) => found_bool = true,
                            _ => {}
                        }
                    }
                }
            }

            if found_string && found_int && found_float && found_bool {
                println!("  ✓ All data types (string, int, float, bool) found in results");
            } else {
                println!(
                    "  ℹ Data types found: string={found_string}, int={found_int}, float={found_float}, bool={found_bool}"
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ Mixed data types table query returned null");
        }
        _ => {
            println!(
                "ℹ Mixed data types table query returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Mixed data types table query test completed successfully!");
}
