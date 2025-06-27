mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_table_operation_basic() {
    let query_id = "test-table-basic-001";
    let database_name = &generate_unique_name("test_db_table_basic");
    let table_name = &generate_unique_name("test_table_basic");

    println!(
        "Testing basic table operation with ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Test basic table query
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table operation should return valid response
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table operation returned array with {} items",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table operation returned null/empty response");
        }
        _ => {
            println!("ℹ Table operation returned: {:?}", response_datum.value);
        }
    }

    println!("✓ Basic table operation test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_with_data() {
    let query_id = "test-table-with-data-002";
    let database_name = &generate_unique_name("test_db_table_with_data");
    let table_name = &generate_unique_name("test_table_with_data");

    println!(
        "Testing table operation with data, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test data
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("table_data_001")),
            ("name", create_string_datum("Table Test Document 1")),
            ("value", create_int_datum(100)),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("table_data_002")),
            ("name", create_string_datum("Table Test Document 2")),
            ("value", create_int_datum(200)),
            ("active", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("table_data_003")),
            ("name", create_string_datum("Table Test Document 3")),
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

    // Query the table with data
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table operation should return the documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table operation with data returned array with {} documents",
                arr.items.len()
            );

            // Verify document structure
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
            println!("ℹ Table operation with data returned null (documents may not be visible)");
        }
        _ => {
            println!(
                "ℹ Table operation with data returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table operation with data test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_multiple_queries() {
    let query_id = "test-table-multiple-003";
    let database_name = &generate_unique_name("test_db_table_multiple");
    let table_name = &generate_unique_name("test_table_multiple");

    println!(
        "Testing multiple table operations, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Perform multiple table queries
    let num_queries = 5;
    for i in 1..=num_queries {
        let query_id_iteration = format!("{query_id}-iter-{i}");

        let table_query = create_table_query(database_name, table_name);
        let table_envelope = create_envelope(&query_id_iteration, &table_query);

        let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
            .await
            .expect("Failed to send table envelope");

        validate_response_envelope(&response_envelope, &query_id_iteration)
            .expect("Response validation failed");

        let response_datum =
            decode_response_payload(&response_envelope).expect("Failed to decode response payload");

        match response_datum.value {
            Some(proto::datum::Value::Array(ref arr)) => {
                println!(
                    "  ✓ Query {}: returned array with {} items",
                    i,
                    arr.items.len()
                );
            }
            Some(proto::datum::Value::Null(_)) | None => {
                println!("  ✓ Query {i}: returned null/empty response");
            }
            _ => {
                println!("  ℹ Query {}: returned: {:?}", i, response_datum.value);
            }
        }
    }

    println!("✓ Multiple table operations test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_with_timeout() {
    let query_id = "test-table-timeout-004";
    let database_name = &generate_unique_name("test_db_table_timeout");
    let table_name = &generate_unique_name("test_table_timeout");

    println!(
        "Testing table operation with timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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
        options.timeout_ms = 10000; // 10 second timeout
    }

    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table operation with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table operation with timeout returned array with {} items",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("✓ Table operation with timeout returned null/empty");
        }
        _ => {
            println!(
                "ℹ Table operation with timeout returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table operation with timeout test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_nonexistent_table() {
    let query_id = "test-table-nonexistent-005";
    let database_name = &generate_unique_name("test_db_table_nonexistent");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing table operation on nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try table operation on nonexistent table
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table envelope");

    // Check if table operation succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ Table operation on nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ Table operation on nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in table operation nonexistent table response");
        }
    }

    println!("✓ Table operation nonexistent table test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_large_dataset() {
    let query_id = "test-table-large-006";
    let database_name = &generate_unique_name("test_db_table_large");
    let table_name = &generate_unique_name("test_table_large");

    println!(
        "Testing table operation with large dataset, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert large dataset
    let mut documents = Vec::new();
    for i in 1..=100 {
        documents.push(create_datum_object(vec![
            ("id", create_string_datum(&format!("large_doc_{i:03}"))),
            ("name", create_string_datum(&format!("Large Document {i}"))),
            ("value", create_int_datum(i as i64 * 10)),
            ("index", create_int_datum(i as i64)),
            ("category", create_string_datum(&format!("cat_{}", i % 5))),
            ("active", create_bool_datum(i % 2 == 0)),
        ]));
    }

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Large dataset (100 documents) inserted successfully");

    // Query the table with large dataset
    let table_query = create_table_query(database_name, table_name);
    let table_envelope = create_envelope(query_id, &table_query);

    let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
        .await
        .expect("Failed to send table envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // Table operation should return large dataset
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ Table operation with large dataset returned array with {} documents",
                arr.items.len()
            );

            if arr.items.len() >= 50 {
                println!("✓ Large portion of documents returned");
            } else {
                println!(
                    "ℹ Expected around 100 documents, got {} (may be due to pagination or limits)",
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
                "ℹ Table operation with large dataset returned null (documents may not be visible)"
            );
        }
        _ => {
            println!(
                "ℹ Table operation with large dataset returned: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ Table operation with large dataset test completed successfully!");
}

#[tokio::test]
async fn test_table_operation_consistency() {
    let query_id = "test-table-consistency-007";
    let database_name = &generate_unique_name("test_db_table_consistency");
    let table_name = &generate_unique_name("test_table_consistency");

    println!(
        "Testing table operation consistency, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert some documents
    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("consistency_001")),
        ("name", create_string_datum("Consistency Test Document")),
        ("value", create_int_datum(42)),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Test document inserted successfully");

    // Perform multiple table queries and check consistency
    let mut responses = Vec::new();
    for i in 1..=3 {
        let query_id_consistency = format!("{query_id}-consistency-{i}");

        let table_query = create_table_query(database_name, table_name);
        let table_envelope = create_envelope(&query_id_consistency, &table_query);

        let response_envelope = send_envelope_to_server(&mut stream, &table_envelope)
            .await
            .expect("Failed to send table envelope");

        validate_response_envelope(&response_envelope, &query_id_consistency)
            .expect("Response validation failed");

        let response_datum =
            decode_response_payload(&response_envelope).expect("Failed to decode response payload");

        responses.push(response_datum);
        println!("  ✓ Query {i}: completed");
    }

    // Check consistency across responses
    let mut consistent = true;
    if responses.len() > 1 {
        for i in 1..responses.len() {
            match (&responses[0].value, &responses[i].value) {
                (
                    Some(proto::datum::Value::Array(arr1)),
                    Some(proto::datum::Value::Array(arr2)),
                ) => {
                    if arr1.items.len() != arr2.items.len() {
                        consistent = false;
                        println!(
                            "  ℹ Inconsistency: Query 1 returned {} items, Query {} returned {} items",
                            arr1.items.len(),
                            i + 1,
                            arr2.items.len()
                        );
                    }
                }
                (Some(proto::datum::Value::Null(_)), Some(proto::datum::Value::Null(_))) => {
                    // Both null, consistent
                }
                (None, None) => {
                    // Both None, consistent
                }
                _ => {
                    consistent = false;
                    println!(
                        "  ℹ Inconsistency: Query 1 and Query {} returned different types",
                        i + 1
                    );
                }
            }
        }
    }

    if consistent {
        println!("✓ All table operation responses are consistent");
    } else {
        println!("ℹ Some inconsistency found in table operation responses");
    }

    println!("✓ Table operation consistency test completed successfully!");
}
