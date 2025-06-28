mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_without_basic() {
    let query_id = "test-without-basic-001";
    let database_name = &generate_unique_name("test_db_without_basic");
    let table_name = &generate_unique_name("test_table_without_basic");

    println!(
        "Testing basic without operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test data with sensitive fields
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("user1")),
            ("name", create_string_datum("Alice")),
            ("email", create_string_datum("alice@example.com")),
            ("password", create_string_datum("secret123")),
            ("ssn", create_string_datum("123-45-6789")),
            ("phone", create_string_datum("555-1234")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("user2")),
            ("name", create_string_datum("Bob")),
            ("email", create_string_datum("bob@example.com")),
            ("password", create_string_datum("secret456")),
            ("ssn", create_string_datum("987-65-4321")),
            ("phone", create_string_datum("555-5678")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    // Test without single field (remove password)
    let without_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec!["password".to_string()],
            separator: ".".to_string(),
        }],
    );

    let without_envelope = create_envelope(&format!("{query_id}-without-single"), &without_query);
    let without_response = send_envelope_to_server(&mut stream, &without_envelope)
        .await
        .expect("Failed to send without envelope");

    let without_result =
        decode_response_payload(&without_response).expect("Failed to decode without response");

    // Verify the result structure
    if let Some(proto::datum::Value::Array(array)) = &without_result.value {
        assert_eq!(array.items.len(), 2, "Expected 2 documents in result");

        for doc in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                // Password field should be removed
                assert!(
                    !obj.fields.contains_key("password"),
                    "Password field should be removed"
                );
                // Other fields should remain
                assert!(obj.fields.contains_key("name"), "Name field should remain");
                assert!(
                    obj.fields.contains_key("email"),
                    "Email field should remain"
                );
                assert!(obj.fields.contains_key("ssn"), "SSN field should remain");
                assert!(
                    obj.fields.contains_key("phone"),
                    "Phone field should remain"
                );
            } else {
                panic!("Expected object in result array");
            }
        }
    } else {
        panic!("Expected array result from without query");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope).await;

    println!("✅ Basic without test completed successfully");
}

#[tokio::test]
async fn test_without_multiple_fields() {
    let query_id = "test-without-multiple-001";
    let database_name = &generate_unique_name("test_db_without_multiple");
    let table_name = &generate_unique_name("test_table_without_multiple");

    println!(
        "Testing multiple fields without operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test data with many fields
    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("item1")),
        ("title", create_string_datum("Product A")),
        ("price", create_float_datum(29.99)),
        ("cost", create_float_datum(15.50)),
        ("internal_notes", create_string_datum("Check with supplier")),
        ("category", create_string_datum("electronics")),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    // Test without multiple fields (remove internal fields)
    let without_query = create_without_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["cost".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["internal_notes".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    let without_envelope = create_envelope(&format!("{query_id}-without-multiple"), &without_query);
    let without_response = send_envelope_to_server(&mut stream, &without_envelope)
        .await
        .expect("Failed to send without envelope");

    let without_result =
        decode_response_payload(&without_response).expect("Failed to decode without response");

    // Verify the result structure
    if let Some(proto::datum::Value::Array(array)) = &without_result.value {
        assert_eq!(array.items.len(), 1, "Expected 1 document in result");

        for doc in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                // Internal fields should be removed
                assert!(
                    !obj.fields.contains_key("cost"),
                    "Cost field should be removed"
                );
                assert!(
                    !obj.fields.contains_key("internal_notes"),
                    "Internal notes field should be removed"
                );
                // Public fields should remain
                assert!(obj.fields.contains_key("id"), "ID field should remain");
                assert!(
                    obj.fields.contains_key("title"),
                    "Title field should remain"
                );
                assert!(
                    obj.fields.contains_key("price"),
                    "Price field should remain"
                );
                assert!(
                    obj.fields.contains_key("category"),
                    "Category field should remain"
                );
            } else {
                panic!("Expected object in result array");
            }
        }
    } else {
        panic!("Expected array result from without query");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope).await;

    println!("✅ Multiple fields without test completed successfully");
}

#[tokio::test]
async fn test_without_nested_fields() {
    let query_id = "test-without-nested-001";
    let database_name = &generate_unique_name("test_db_without_nested");
    let table_name = &generate_unique_name("test_table_without_nested");

    println!(
        "Testing nested fields without operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test data with nested structures
    let profile_private = create_datum_object(vec![
        ("ssn", create_string_datum("123-45-6789")),
        ("phone", create_string_datum("555-1234")),
    ]);

    let profile = create_datum_object(vec![
        ("bio", create_string_datum("Software developer")),
        (
            "private",
            proto::Datum {
                value: Some(proto::datum::Value::Object(profile_private)),
            },
        ),
    ]);

    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("user1")),
        ("name", create_string_datum("Alice")),
        (
            "profile",
            proto::Datum {
                value: Some(proto::datum::Value::Object(profile)),
            },
        ),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    // Test without nested field (remove profile.private.ssn)
    let without_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec![
                "profile".to_string(),
                "private".to_string(),
                "ssn".to_string(),
            ],
            separator: ".".to_string(),
        }],
    );

    let without_envelope = create_envelope(&format!("{query_id}-without-nested"), &without_query);
    let without_response = send_envelope_to_server(&mut stream, &without_envelope)
        .await
        .expect("Failed to send without envelope");

    let without_result =
        decode_response_payload(&without_response).expect("Failed to decode without response");

    // Verify the result structure
    if let Some(proto::datum::Value::Array(array)) = &without_result.value {
        assert_eq!(array.items.len(), 1, "Expected 1 document in result");

        for doc in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("profile"),
                    "Profile field should remain"
                );

                if let Some(proto::datum::Value::Object(profile_obj)) =
                    &obj.fields.get("profile").unwrap().value
                {
                    assert!(
                        profile_obj.fields.contains_key("private"),
                        "Private field should remain"
                    );

                    if let Some(proto::datum::Value::Object(private_obj)) =
                        &profile_obj.fields.get("private").unwrap().value
                    {
                        // SSN should be removed
                        assert!(
                            !private_obj.fields.contains_key("ssn"),
                            "SSN field should be removed"
                        );
                        // Phone should remain
                        assert!(
                            private_obj.fields.contains_key("phone"),
                            "Phone field should remain"
                        );
                    } else {
                        panic!("Expected private object");
                    }
                } else {
                    panic!("Expected profile object");
                }
            } else {
                panic!("Expected object in result array");
            }
        }
    } else {
        panic!("Expected array result from without query");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope).await;

    println!("✅ Nested fields without test completed successfully");
}

#[tokio::test]
async fn test_without_custom_separator() {
    let query_id = "test-without-separator-001";
    let database_name = &generate_unique_name("test_db_without_separator");
    let table_name = &generate_unique_name("test_table_without_separator");

    println!(
        "Testing custom separator without operation, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test data with nested structures for custom separator testing
    let credentials = create_datum_object(vec![
        ("username", create_string_datum("admin")),
        ("password", create_string_datum("secret123")),
    ]);

    let database_config = create_datum_object(vec![
        ("host", create_string_datum("localhost")),
        (
            "credentials",
            proto::Datum {
                value: Some(proto::datum::Value::Object(credentials)),
            },
        ),
    ]);

    let documents = vec![create_datum_object(vec![
        ("id", create_string_datum("config1")),
        (
            "database",
            proto::Datum {
                value: Some(proto::datum::Value::Object(database_config)),
            },
        ),
    ])];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{query_id}-insert"))
        .expect("Insert response validation failed");

    // Test with :: separator
    let without_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec![
                "database".to_string(),
                "credentials".to_string(),
                "password".to_string(),
            ],
            separator: "::".to_string(),
        }],
    );

    let without_envelope =
        create_envelope(&format!("{query_id}-without-separator"), &without_query);
    let without_response = send_envelope_to_server(&mut stream, &without_envelope)
        .await
        .expect("Failed to send without envelope");

    let without_result =
        decode_response_payload(&without_response).expect("Failed to decode without response");

    // Verify the result structure
    if let Some(proto::datum::Value::Array(array)) = &without_result.value {
        assert_eq!(array.items.len(), 1, "Expected 1 document in result");

        for doc in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &doc.value {
                assert!(
                    obj.fields.contains_key("database"),
                    "Database field should remain"
                );

                if let Some(proto::datum::Value::Object(db_obj)) =
                    &obj.fields.get("database").unwrap().value
                {
                    assert!(
                        db_obj.fields.contains_key("credentials"),
                        "Credentials field should remain"
                    );

                    if let Some(proto::datum::Value::Object(creds_obj)) =
                        &db_obj.fields.get("credentials").unwrap().value
                    {
                        // Password should be removed
                        assert!(
                            !creds_obj.fields.contains_key("password"),
                            "Password field should be removed"
                        );
                        // Username should remain
                        assert!(
                            creds_obj.fields.contains_key("username"),
                            "Username field should remain"
                        );
                    } else {
                        panic!("Expected credentials object");
                    }
                } else {
                    panic!("Expected database object");
                }
            } else {
                panic!("Expected object in result array");
            }
        }
    } else {
        panic!("Expected array result from without query");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope).await;

    println!("✅ Custom separator without test completed successfully");
}

#[tokio::test]
async fn test_without_empty_table() {
    let query_id = "test-without-empty-001";
    let database_name = &generate_unique_name("test_db_without_empty");
    let table_name = &generate_unique_name("test_table_without_empty");

    println!(
        "Testing without on empty table, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Test without on empty table
    let without_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec!["password".to_string()],
            separator: ".".to_string(),
        }],
    );

    let without_envelope = create_envelope(&format!("{query_id}-without-empty"), &without_query);
    let without_response = send_envelope_to_server(&mut stream, &without_envelope)
        .await
        .expect("Failed to send without envelope");

    let without_result =
        decode_response_payload(&without_response).expect("Failed to decode without response");

    // Verify the result structure
    if let Some(proto::datum::Value::Array(array)) = &without_result.value {
        assert_eq!(
            array.items.len(),
            0,
            "Expected 0 documents in empty table result"
        );
    } else {
        panic!("Expected array result from without query on empty table");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope).await;

    println!("✅ Empty table without test completed successfully");
}

#[test]
fn test_without_query_creation() {
    // Test that without query creation works correctly
    let database_name = "test_db";
    let table_name = "test_table";

    // Test single field without
    let single_field_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec!["password".to_string()],
            separator: ".".to_string(),
        }],
    );

    // Verify query structure
    if let Some(proto::query::Kind::Without(without_op)) = &single_field_query.kind {
        assert_eq!(without_op.fields.len(), 1);
        assert_eq!(without_op.fields[0].path, vec!["password".to_string()]);
        assert_eq!(without_op.fields[0].separator, ".");

        // Verify source is a table query
        if let Some(source) = &without_op.source {
            if let Some(proto::query::Kind::Table(table)) = &source.kind {
                assert_eq!(table.table.as_ref().unwrap().name, table_name);
                assert_eq!(
                    table
                        .table
                        .as_ref()
                        .unwrap()
                        .database
                        .as_ref()
                        .unwrap()
                        .name,
                    database_name
                );
            } else {
                panic!("Expected table source");
            }
        } else {
            panic!("Expected source");
        }
    } else {
        panic!("Expected without query");
    }

    // Test multiple fields without
    let multi_field_query = create_without_query(
        database_name,
        table_name,
        vec![
            proto::FieldRef {
                path: vec!["password".to_string()],
                separator: ".".to_string(),
            },
            proto::FieldRef {
                path: vec!["ssn".to_string()],
                separator: ".".to_string(),
            },
        ],
    );

    if let Some(proto::query::Kind::Without(without_op)) = &multi_field_query.kind {
        assert_eq!(without_op.fields.len(), 2);
        assert_eq!(without_op.fields[0].path, vec!["password".to_string()]);
        assert_eq!(without_op.fields[1].path, vec!["ssn".to_string()]);
    } else {
        panic!("Expected without query");
    }

    // Test nested field without
    let nested_field_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec![
                "profile".to_string(),
                "private".to_string(),
                "ssn".to_string(),
            ],
            separator: ".".to_string(),
        }],
    );

    if let Some(proto::query::Kind::Without(without_op)) = &nested_field_query.kind {
        assert_eq!(without_op.fields.len(), 1);
        assert_eq!(
            without_op.fields[0].path,
            vec![
                "profile".to_string(),
                "private".to_string(),
                "ssn".to_string()
            ]
        );
    } else {
        panic!("Expected without query");
    }

    // Test custom separator
    let custom_separator_query = create_without_query(
        database_name,
        table_name,
        vec![proto::FieldRef {
            path: vec![
                "database".to_string(),
                "credentials".to_string(),
                "password".to_string(),
            ],
            separator: "::".to_string(),
        }],
    );

    if let Some(proto::query::Kind::Without(without_op)) = &custom_separator_query.kind {
        assert_eq!(without_op.fields.len(), 1);
        assert_eq!(without_op.fields[0].separator, "::");
        assert_eq!(
            without_op.fields[0].path,
            vec![
                "database".to_string(),
                "credentials".to_string(),
                "password".to_string()
            ]
        );
    } else {
        panic!("Expected without query");
    }

    println!("✅ Without query creation test completed successfully");
}
