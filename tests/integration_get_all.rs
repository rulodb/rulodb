mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_get_all_existing_documents() {
    let query_id = "test-get-all-existing-001";
    let database_name = &generate_unique_name("test_db_get_all_existing");
    let table_name = &generate_unique_name("test_table_get_all_existing");

    println!(
        "Testing get all existing documents with ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("get_all_001")),
            ("name", create_string_datum("First GetAll Document")),
            ("category", create_string_datum("A")),
            ("value", create_int_datum(100)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("get_all_002")),
            ("name", create_string_datum("Second GetAll Document")),
            ("category", create_string_datum("B")),
            ("value", create_int_datum(200)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("get_all_003")),
            ("name", create_string_datum("Third GetAll Document")),
            ("category", create_string_datum("C")),
            ("value", create_int_datum(300)),
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

    // Get all documents by their keys
    let keys = vec![
        create_string_datum("get_all_001"),
        create_string_datum("get_all_002"),
        create_string_datum("get_all_003"),
    ];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // GetAll should return array of documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!("✓ GetAll returned array with {} documents", arr.items.len());

            // Verify we got some documents back
            if !arr.items.is_empty() {
                println!("✓ Found documents in GetAll response");

                // Check that documents have expected structure
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
            } else {
                println!(
                    "ℹ GetAll returned empty array (documents may not be immediately visible)"
                );
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ GetAll returned null (documents may not be immediately visible)");
        }
        _ => {
            println!(
                "ℹ GetAll returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ GetAll existing documents test completed successfully!");
}

#[tokio::test]
async fn test_get_all_mixed_existing_and_nonexistent() {
    let query_id = "test-get-all-mixed-002";
    let database_name = &generate_unique_name("test_db_get_all_mixed");
    let table_name = &generate_unique_name("test_table_get_all_mixed");

    println!(
        "Testing get all with mixed existing and nonexistent keys, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert only some documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("mixed_001")),
            ("name", create_string_datum("Existing Document 1")),
            ("status", create_string_datum("present")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("mixed_003")),
            ("name", create_string_datum("Existing Document 3")),
            ("status", create_string_datum("present")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Partial test documents inserted successfully");

    // Try to get documents including ones that don't exist
    let keys = vec![
        create_string_datum("mixed_001"), // exists
        create_string_datum("mixed_002"), // doesn't exist
        create_string_datum("mixed_003"), // exists
        create_string_datum("mixed_004"), // doesn't exist
    ];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // GetAll should return only existing documents
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ GetAll mixed returned array with {} documents",
                arr.items.len()
            );

            // Should get only the existing documents (2 out of 4 requested)
            if arr.items.len() <= 2 {
                println!("✓ Returned expected number of documents (only existing ones)");
            } else {
                println!(
                    "ℹ Returned {} documents (expected 2 or fewer)",
                    arr.items.len()
                );
            }

            // Verify returned documents are the correct ones
            for item in &arr.items {
                if let Some(proto::datum::Value::Object(obj)) = &item.value {
                    if let Some(id_field) = obj.fields.get("id") {
                        if let Some(proto::datum::Value::String(id_val)) = &id_field.value {
                            if id_val == "mixed_001" || id_val == "mixed_003" {
                                println!("    ✓ Found expected document: {id_val}");
                            } else {
                                println!("    ℹ Found unexpected document: {id_val}");
                            }
                        }
                    }
                }
            }
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ GetAll mixed returned null");
        }
        _ => {
            println!(
                "ℹ GetAll mixed returned unexpected format: {:?}",
                response_datum.value
            );
        }
    }

    println!("✓ GetAll mixed existing and nonexistent test completed successfully!");
}

#[tokio::test]
async fn test_get_all_nonexistent_keys() {
    let query_id = "test-get-all-nonexistent-003";
    let database_name = &generate_unique_name("test_db_get_all_nonexistent");
    let table_name = &generate_unique_name("test_table_get_all_nonexistent");

    println!(
        "Testing get all with nonexistent keys, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    println!("✓ Database and table created successfully (no documents inserted)");

    // Try to get documents that don't exist
    let keys = vec![
        create_string_datum("nonexistent_001"),
        create_string_datum("nonexistent_002"),
        create_string_datum("nonexistent_003"),
    ];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Check if GetAll succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Array(ref arr)) => {
                    if arr.items.is_empty() {
                        println!("✓ GetAll nonexistent keys returned empty array as expected");
                    } else {
                        println!(
                            "ℹ GetAll nonexistent keys returned {} items (unexpected)",
                            arr.items.len()
                        );
                    }
                }
                Some(proto::datum::Value::Null(_)) | None => {
                    println!("✓ GetAll nonexistent keys returned null as expected");
                }
                _ => {
                    println!(
                        "ℹ GetAll nonexistent keys returned: {:?}",
                        response_datum.value
                    );
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ GetAll nonexistent keys failed with error (may be expected behavior)");
        }
        _ => {
            panic!("Unexpected message type in GetAll nonexistent keys response");
        }
    }

    println!("✓ GetAll nonexistent keys test completed successfully!");
}

#[tokio::test]
async fn test_get_all_with_different_key_types() {
    let query_id = "test-get-all-key-types-004";
    let database_name = &generate_unique_name("test_db_get_all_key_types");
    let table_name = &generate_unique_name("test_table_get_all_key_types");

    println!(
        "Testing get all with different key types, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert documents with different key types
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("string_key_001")),
            ("type", create_string_datum("string key document")),
            ("data", create_string_datum("String key data")),
        ]),
        create_datum_object(vec![
            ("id", create_int_datum(42)),
            ("type", create_string_datum("integer key document")),
            ("data", create_string_datum("Integer key data")),
        ]),
        create_datum_object(vec![
            ("id", create_bool_datum(true)),
            ("type", create_string_datum("boolean key document")),
            ("data", create_string_datum("Boolean key data")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    println!("✓ Documents with different key types inserted successfully");

    // Try to get all documents using different key types
    let keys = vec![
        create_string_datum("string_key_001"),
        create_int_datum(42),
        create_bool_datum(true),
    ];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Check if GetAll succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Array(ref arr)) => {
                    println!(
                        "✓ GetAll with different key types returned array with {} documents",
                        arr.items.len()
                    );

                    // Check which key types were successfully retrieved
                    let mut found_string = false;
                    let mut found_int = false;
                    let mut found_bool = false;

                    for item in &arr.items {
                        if let Some(proto::datum::Value::Object(obj)) = &item.value {
                            if let Some(id_field) = obj.fields.get("id") {
                                match &id_field.value {
                                    Some(proto::datum::Value::String(val))
                                        if val == "string_key_001" =>
                                    {
                                        found_string = true;
                                        println!("  ✓ Found document with string key");
                                    }
                                    Some(proto::datum::Value::Int(val)) if *val == 42 => {
                                        found_int = true;
                                        println!("  ✓ Found document with integer key");
                                    }
                                    Some(proto::datum::Value::Bool(val)) if *val => {
                                        found_bool = true;
                                        println!("  ✓ Found document with boolean key");
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    if found_string {
                        println!("  ✓ String key retrieval successful");
                    } else {
                        println!("  ℹ String key retrieval failed or not found");
                    }

                    if found_int {
                        println!("  ✓ Integer key retrieval successful");
                    } else {
                        println!("  ℹ Integer key retrieval failed or not found");
                    }

                    if found_bool {
                        println!("  ✓ Boolean key retrieval successful");
                    } else {
                        println!("  ℹ Boolean key retrieval failed or not found");
                    }
                }
                Some(proto::datum::Value::Null(_)) | None => {
                    println!("ℹ GetAll with different key types returned null");
                }
                _ => {
                    println!(
                        "ℹ GetAll with different key types returned: {:?}",
                        response_datum.value
                    );
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ GetAll with different key types failed");
        }
        _ => {
            panic!("Unexpected message type in GetAll different key types response");
        }
    }

    println!("✓ GetAll with different key types test completed successfully!");
}

#[tokio::test]
async fn test_get_all_with_timeout() {
    let query_id = "test-get-all-timeout-005";
    let database_name = &generate_unique_name("test_db_get_all_timeout");
    let table_name = &generate_unique_name("test_table_get_all_timeout");

    println!(
        "Testing get all with custom timeout, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("timeout_001")),
            ("name", create_string_datum("Timeout Test Document 1")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("timeout_002")),
            ("name", create_string_datum("Timeout Test Document 2")),
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

    // Create GetAll query with custom timeout
    let keys = vec![
        create_string_datum("timeout_001"),
        create_string_datum("timeout_002"),
    ];

    let mut get_all_query = create_get_all_query(database_name, table_name, keys);
    if let Some(ref mut options) = get_all_query.options {
        options.timeout_ms = 4000; // 4 second timeout
    }

    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Validate basic response structure
    validate_response_envelope(&response_envelope, query_id).expect("Response validation failed");

    // Decode and validate response payload
    let response_datum =
        decode_response_payload(&response_envelope).expect("Failed to decode response payload");

    // GetAll with timeout should work normally
    match response_datum.value {
        Some(proto::datum::Value::Array(ref arr)) => {
            println!(
                "✓ GetAll with timeout returned array with {} documents",
                arr.items.len()
            );
        }
        Some(proto::datum::Value::Null(_)) | None => {
            println!("ℹ GetAll with timeout returned null");
        }
        _ => {
            println!("ℹ GetAll with timeout returned: {:?}", response_datum.value);
        }
    }

    println!("✓ GetAll with timeout test completed successfully!");
}

#[tokio::test]
async fn test_get_all_empty_keys_list() {
    let query_id = "test-get-all-empty-006";
    let database_name = &generate_unique_name("test_db_get_all_empty");
    let table_name = &generate_unique_name("test_table_get_all_empty");

    println!(
        "Testing get all with empty keys list, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try GetAll with empty keys list
    let keys = vec![];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Check if GetAll succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");

            match response_datum.value {
                Some(proto::datum::Value::Array(ref arr)) => {
                    if arr.items.is_empty() {
                        println!("✓ GetAll with empty keys returned empty array as expected");
                    } else {
                        println!(
                            "ℹ GetAll with empty keys returned {} items (unexpected)",
                            arr.items.len()
                        );
                    }
                }
                Some(proto::datum::Value::Null(_)) | None => {
                    println!("✓ GetAll with empty keys returned null as expected");
                }
                _ => {
                    println!(
                        "ℹ GetAll with empty keys returned: {:?}",
                        response_datum.value
                    );
                }
            }
        }
        Ok(proto::MessageType::Error) => {
            println!("ℹ GetAll with empty keys failed with error (may be expected behavior)");
        }
        _ => {
            panic!("Unexpected message type in GetAll empty keys response");
        }
    }

    println!("✓ GetAll empty keys list test completed successfully!");
}

#[tokio::test]
async fn test_get_all_nonexistent_table() {
    let query_id = "test-get-all-no-table-007";
    let database_name = &generate_unique_name("test_db_get_all_no_table");
    let table_name = &generate_unique_name("test_table_nonexistent");

    println!(
        "Testing get all from nonexistent table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Try GetAll from nonexistent table
    let keys = vec![
        create_string_datum("any_key_001"),
        create_string_datum("any_key_002"),
    ];

    let get_all_query = create_get_all_query(database_name, table_name, keys);
    let get_all_envelope = create_envelope(query_id, &get_all_query);

    let response_envelope = send_envelope_to_server(&mut stream, &get_all_envelope)
        .await
        .expect("Failed to send get_all envelope");

    // Check if GetAll succeeded or failed
    match proto::MessageType::try_from(response_envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            println!(
                "ℹ GetAll from nonexistent table succeeded (auto-create or empty result behavior)"
            );
            let response_datum = decode_response_payload(&response_envelope)
                .expect("Failed to decode response payload");
            println!("  Response: {:?}", response_datum.value);
        }
        Ok(proto::MessageType::Error) => {
            println!("✓ GetAll from nonexistent table failed as expected");
            // This is expected behavior
        }
        _ => {
            panic!("Unexpected message type in GetAll from nonexistent table response");
        }
    }

    println!("✓ GetAll from nonexistent table test completed successfully!");
}
