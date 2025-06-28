mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_pluck_basic() {
    let query_id = "test-pluck-basic-001";
    let database_name = &generate_unique_name("test_db_pluck_basic");
    let table_name = &generate_unique_name("test_table_pluck_basic");

    println!(
        "Testing basic pluck operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("pluck_001")),
            ("name", create_string_datum("Alice")),
            ("age", create_int_datum(30)),
            ("email", create_string_datum("alice@example.com")),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("pluck_002")),
            ("name", create_string_datum("Bob")),
            ("age", create_int_datum(25)),
            ("email", create_string_datum("bob@example.com")),
            ("active", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("pluck_003")),
            ("name", create_string_datum("Charlie")),
            ("age", create_int_datum(35)),
            ("email", create_string_datum("charlie@example.com")),
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

    // Test pluck with single field
    let pluck_query = create_pluck_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["id".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    let pluck_envelope = create_envelope(&format!("{query_id}-pluck"), &pluck_query);
    let pluck_response = send_envelope_to_server(&mut stream, &pluck_envelope)
        .await
        .expect("Failed to send pluck envelope");
    validate_response_envelope(&pluck_response, &format!("{query_id}-pluck"))
        .expect("Pluck response validation failed");

    let pluck_result =
        decode_response_payload(&pluck_response).expect("Failed to decode pluck response payload");

    if let Some(proto::datum::Value::Array(array)) = pluck_result.value {
        assert_eq!(array.items.len(), 3, "Expected 3 documents in pluck result");
        println!("✓ Pluck returned {} documents", array.items.len());

        // Verify each document has only the name field
        for (i, doc) in array.items.iter().enumerate() {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("id"),
                    "Document {i} should contain 'id' field"
                );
                assert!(
                    obj.fields.contains_key("name"),
                    "Document {i} should contain 'name' field"
                );
                assert_eq!(
                    obj.fields.len(),
                    2,
                    "Document {i} should contain only 'id' and 'name' fields"
                );
            } else {
                panic!("Document {i} is not an object");
            }
        }
        println!("✓ All documents contain only the plucked field");
    } else {
        panic!("Pluck result is not an array");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_pluck_multiple_fields() {
    let query_id = "test-pluck-multiple-fields-001";
    let database_name = &generate_unique_name("test_db_pluck_multiple");
    let table_name = &generate_unique_name("test_table_pluck_multiple");

    println!(
        "Testing pluck with multiple fields, ID: {query_id}, database: {database_name}, table: {table_name}"
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
            ("id", create_string_datum("pluck_001")),
            ("name", create_string_datum("Alice")),
            ("age", create_int_datum(30)),
            ("email", create_string_datum("alice@example.com")),
            ("active", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("pluck_002")),
            ("name", create_string_datum("Bob")),
            ("age", create_int_datum(25)),
            ("email", create_string_datum("bob@example.com")),
            ("active", create_bool_datum(false)),
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

    // Test pluck with multiple fields
    let pluck_query = create_pluck_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["id".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["email".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    let pluck_envelope = create_envelope(&format!("{query_id}-pluck"), &pluck_query);
    let pluck_response = send_envelope_to_server(&mut stream, &pluck_envelope)
        .await
        .expect("Failed to send pluck envelope");
    validate_response_envelope(&pluck_response, &format!("{query_id}-pluck"))
        .expect("Pluck response validation failed");

    let pluck_result =
        decode_response_payload(&pluck_response).expect("Failed to decode pluck response payload");

    if let Some(proto::datum::Value::Array(array)) = pluck_result.value {
        assert_eq!(array.items.len(), 2, "Expected 2 documents in pluck result");
        println!("✓ Pluck returned {} documents", array.items.len());

        // Verify each document has only the id, name and email fields
        for (i, doc) in array.items.iter().enumerate() {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("id"),
                    "Document {i} should contain 'id' field"
                );
                assert!(
                    obj.fields.contains_key("name"),
                    "Document {i} should contain 'name' field"
                );
                assert!(
                    obj.fields.contains_key("email"),
                    "Document {i} should contain 'email' field"
                );
                assert_eq!(
                    obj.fields.len(),
                    3,
                    "Document {i} should contain only 'id', 'name' and 'email' fields"
                );
            } else {
                panic!("Document {i} is not an object");
            }
        }
        println!("✓ All documents contain only the plucked fields");
    } else {
        panic!("Pluck result is not an array");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_pluck_nested_fields() {
    let query_id = "test-pluck-nested-fields-001";
    let database_name = &generate_unique_name("test_db_pluck_nested");
    let table_name = &generate_unique_name("test_table_pluck_nested");

    println!(
        "Testing pluck with nested fields, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents with nested structure
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("pluck_001")),
            ("name", create_string_datum("Alice")),
            (
                "profile",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(create_datum_object(vec![
                        ("bio", create_string_datum("Software Engineer")),
                        (
                            "settings",
                            proto::Datum {
                                value: Some(proto::datum::Value::Object(create_datum_object(
                                    vec![
                                        ("theme", create_string_datum("dark")),
                                        ("notifications", create_bool_datum(true)),
                                    ],
                                ))),
                            },
                        ),
                    ]))),
                },
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("pluck_002")),
            ("name", create_string_datum("Bob")),
            (
                "profile",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(create_datum_object(vec![
                        ("bio", create_string_datum("Designer")),
                        (
                            "settings",
                            proto::Datum {
                                value: Some(proto::datum::Value::Object(create_datum_object(
                                    vec![
                                        ("theme", create_string_datum("light")),
                                        ("notifications", create_bool_datum(false)),
                                    ],
                                ))),
                            },
                        ),
                    ]))),
                },
            ),
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

    // Test pluck with nested fields
    let pluck_query = create_pluck_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["id".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["profile".to_string(), "bio".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    let pluck_envelope = create_envelope(&format!("{query_id}-pluck"), &pluck_query);
    let pluck_response = send_envelope_to_server(&mut stream, &pluck_envelope)
        .await
        .expect("Failed to send pluck envelope");
    validate_response_envelope(&pluck_response, &format!("{query_id}-pluck"))
        .expect("Pluck response validation failed");

    let pluck_result =
        decode_response_payload(&pluck_response).expect("Failed to decode pluck response payload");

    if let Some(proto::datum::Value::Array(array)) = pluck_result.value {
        assert_eq!(array.items.len(), 2, "Expected 2 documents in pluck result");
        println!("✓ Pluck returned {} documents", array.items.len());

        // Verify each document has the expected structure
        for (i, doc) in array.items.iter().enumerate() {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("id"),
                    "Document {i} should contain 'id' field"
                );
                assert!(
                    obj.fields.contains_key("name"),
                    "Document {i} should contain 'name' field"
                );
                assert!(
                    obj.fields.contains_key("profile"),
                    "Document {i} should contain 'profile' field"
                );

                // Check nested profile structure
                if let Some(proto::datum::Value::Object(profile_obj)) =
                    &obj.fields.get("profile").unwrap().value
                {
                    assert!(
                        profile_obj.fields.contains_key("bio"),
                        "Document {i} profile should contain 'bio' field"
                    );
                } else {
                    panic!("Document {i} profile should be an object");
                }
                assert_eq!(
                    obj.fields.len(),
                    3,
                    "Document {i} should contain only 'id', 'name' and 'profile' fields"
                );
            } else {
                panic!("Document {i} is not an object");
            }
        }
        println!("✓ All documents contain the expected nested structure");
    } else {
        panic!("Pluck result is not an array");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_pluck_custom_separator() {
    let query_id = "test-pluck-custom-separator-001";
    let database_name = &generate_unique_name("test_db_pluck_separator");
    let table_name = &generate_unique_name("test_table_pluck_separator");

    println!(
        "Testing pluck with custom separator, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents with nested structure that matches the pluck path
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("pluck_001")),
            ("name", create_string_datum("Alice")),
            (
                "user",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(create_datum_object(vec![(
                        "profile",
                        proto::Datum {
                            value: Some(proto::datum::Value::Object(create_datum_object(vec![
                                ("bio", create_string_datum("Software Engineer")),
                                (
                                    "settings",
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Object(
                                            create_datum_object(vec![(
                                                "theme",
                                                create_string_datum("dark"),
                                            )]),
                                        )),
                                    },
                                ),
                            ]))),
                        },
                    )]))),
                },
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("pluck_002")),
            ("name", create_string_datum("Bob")),
            (
                "user",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(create_datum_object(vec![(
                        "profile",
                        proto::Datum {
                            value: Some(proto::datum::Value::Object(create_datum_object(vec![
                                ("bio", create_string_datum("Designer")),
                                (
                                    "settings",
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Object(
                                            create_datum_object(vec![(
                                                "theme",
                                                create_string_datum("light"),
                                            )]),
                                        )),
                                    },
                                ),
                            ]))),
                        },
                    )]))),
                },
            ),
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

    // Test pluck with custom separator
    let pluck_query = create_pluck_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["id".to_string()],
                separator: "::".to_string(),
            },
            proto::FieldRef {
                path: vec!["name".to_string()],
                separator: "::".to_string(),
            },
            proto::FieldRef {
                path: vec!["user".to_string(), "profile".to_string(), "bio".to_string()],
                separator: "::".to_string(),
            },
        ],
    );

    let pluck_envelope = create_envelope(&format!("{query_id}-pluck"), &pluck_query);
    let pluck_response = send_envelope_to_server(&mut stream, &pluck_envelope)
        .await
        .expect("Failed to send pluck envelope");
    validate_response_envelope(&pluck_response, &format!("{query_id}-pluck"))
        .expect("Pluck response validation failed");

    let pluck_result =
        decode_response_payload(&pluck_response).expect("Failed to decode pluck response payload");

    if let Some(proto::datum::Value::Array(array)) = pluck_result.value {
        assert_eq!(array.items.len(), 2, "Expected 2 documents in pluck result");
        println!("✓ Pluck returned {} documents", array.items.len());

        // Verify each document has the expected structure
        for (i, doc) in array.items.iter().enumerate() {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("id"),
                    "Document {i} should contain 'id' field"
                );
                assert!(
                    obj.fields.contains_key("name"),
                    "Document {i} should contain 'name' field"
                );
                assert!(
                    obj.fields.contains_key("user"),
                    "Document {i} should contain 'user' field"
                );

                // Check nested user structure
                if let Some(proto::datum::Value::Object(user_obj)) =
                    &obj.fields.get("user").unwrap().value
                {
                    assert!(
                        user_obj.fields.contains_key("profile"),
                        "Document {i} user should contain 'profile' field"
                    );

                    if let Some(proto::datum::Value::Object(profile_obj)) =
                        &user_obj.fields.get("profile").unwrap().value
                    {
                        assert!(
                            profile_obj.fields.contains_key("bio"),
                            "Document {i} user.profile should contain 'bio' field"
                        );
                    } else {
                        panic!("Document {i} user.profile should be an object");
                    }
                } else {
                    panic!("Document {i} user should be an object");
                }
                assert_eq!(
                    obj.fields.len(),
                    3,
                    "Document {i} should contain only 'id', 'name' and 'user' fields"
                );
            } else {
                panic!("Document {i} is not an object");
            }
        }
        println!("✓ All documents contain the expected nested structure with custom separator");
    } else {
        panic!("Pluck result is not an array");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_pluck_empty_table() {
    let query_id = "test-pluck-empty-table-001";
    let database_name = &generate_unique_name("test_db_pluck_empty");
    let table_name = &generate_unique_name("test_table_pluck_empty");

    println!(
        "Testing pluck on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Test pluck on empty table
    let pluck_query = create_pluck_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["id".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["name".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    let pluck_envelope = create_envelope(&format!("{query_id}-pluck"), &pluck_query);
    let pluck_response = send_envelope_to_server(&mut stream, &pluck_envelope)
        .await
        .expect("Failed to send pluck envelope");
    validate_response_envelope(&pluck_response, &format!("{query_id}-pluck"))
        .expect("Pluck response validation failed");

    let pluck_result =
        decode_response_payload(&pluck_response).expect("Failed to decode pluck response payload");

    if let Some(proto::datum::Value::Array(array)) = pluck_result.value {
        assert_eq!(
            array.items.len(),
            0,
            "Expected 0 documents in pluck result from empty table"
        );
        println!(
            "✓ Pluck returned {} documents from empty table",
            array.items.len()
        );
    } else {
        panic!("Pluck result is not an array");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}
