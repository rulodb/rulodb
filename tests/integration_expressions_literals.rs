mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_literal_expression_string() {
    let query_id = "test-literal-expr-string-001";
    let database_name = &generate_unique_name("test_db_literal_string");
    let table_name = &generate_unique_name("test_table_literal_string");

    println!(
        "Testing literal string expression, ID: {}, database: {}, table: {}",
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

    // Insert test documents
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("literal_001")),
            ("category", create_string_datum("electronics")),
            ("name", create_string_datum("Laptop")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("literal_002")),
            ("category", create_string_datum("books")),
            ("name", create_string_datum("Novel")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("literal_003")),
            ("category", create_string_datum("electronics")),
            ("name", create_string_datum("Phone")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    let insert_response = send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to send insert envelope");
    validate_response_envelope(&insert_response, &format!("{}-insert", query_id))
        .expect("Insert response validation failed");

    println!("✓ Test documents inserted successfully");

    // Test literal string expression in binary comparison
    let field_expr = create_field_expression(vec!["category"]);
    let literal_expr = create_literal_expression(create_string_datum("electronics"));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Eq, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");
    validate_response_envelope(&filter_response, &format!("{}-filter", query_id))
        .expect("Filter response validation failed");

    // Decode and validate the response
    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 electronics items");

        // Verify the returned documents
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let category = obj
                    .fields
                    .get("category")
                    .expect("Category field should exist");
                if let Some(proto::datum::Value::String(cat_val)) = &category.value {
                    assert_eq!(cat_val, "electronics", "Category should be electronics");
                } else {
                    panic!("Category field should be a string");
                }
            } else {
                panic!("Result item should be an object");
            }
        }

        println!("✓ Literal string expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_literal_expression_integer() {
    let query_id = "test-literal-expr-int-001";
    let database_name = &generate_unique_name("test_db_literal_int");
    let table_name = &generate_unique_name("test_table_literal_int");

    println!(
        "Testing literal integer expression, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with integers
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("int_001")),
            ("score", create_int_datum(85)),
            ("level", create_int_datum(3)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("int_002")),
            ("score", create_int_datum(92)),
            ("level", create_int_datum(5)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("int_003")),
            ("score", create_int_datum(78)),
            ("level", create_int_datum(2)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test literal integer in comparison: score > 80
    let field_expr = create_field_expression(vec!["score"]);
    let literal_expr = create_literal_expression(create_int_datum(80));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Gt, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with score > 80");

        // Verify scores are > 80
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let score = obj.fields.get("score").expect("Score field should exist");
                if let Some(proto::datum::Value::Int(score_val)) = &score.value {
                    assert!(*score_val > 80, "Score should be > 80: {}", score_val);
                }
            }
        }

        println!("✓ Literal integer expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_literal_expression_float() {
    let query_id = "test-literal-expr-float-001";
    let database_name = &generate_unique_name("test_db_literal_float");
    let table_name = &generate_unique_name("test_table_literal_float");

    println!(
        "Testing literal float expression, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with floats
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("float_001")),
            ("price", create_float_datum(19.99)),
            ("rating", create_float_datum(4.5)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("float_002")),
            ("price", create_float_datum(29.99)),
            ("rating", create_float_datum(4.8)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("float_003")),
            ("price", create_float_datum(15.99)),
            ("rating", create_float_datum(3.2)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test literal float in comparison: rating >= 4.0
    let field_expr = create_field_expression(vec!["rating"]);
    let literal_expr = create_literal_expression(create_float_datum(4.0));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Ge, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items with rating >= 4.0"
        );

        // Verify ratings are >= 4.0
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let rating = obj.fields.get("rating").expect("Rating field should exist");
                if let Some(proto::datum::Value::Float(rating_val)) = &rating.value {
                    assert!(
                        *rating_val >= 4.0,
                        "Rating should be >= 4.0: {}",
                        rating_val
                    );
                }
            }
        }

        println!("✓ Literal float expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_literal_expression_boolean() {
    let query_id = "test-literal-expr-bool-001";
    let database_name = &generate_unique_name("test_db_literal_bool");
    let table_name = &generate_unique_name("test_table_literal_bool");

    println!(
        "Testing literal boolean expression, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with booleans
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("bool_001")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("bool_002")),
            ("active", create_bool_datum(false)),
            ("verified", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("bool_003")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(true)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test literal boolean in comparison: active == true
    let field_expr = create_field_expression(vec!["active"]);
    let literal_expr = create_literal_expression(create_bool_datum(true));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Eq, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items with active = true"
        );

        // Verify active fields are true
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let active = obj.fields.get("active").expect("Active field should exist");
                if let Some(proto::datum::Value::Bool(active_val)) = &active.value {
                    assert!(*active_val, "Active should be true");
                }
            }
        }

        println!("✓ Literal boolean expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_field_reference_simple() {
    let query_id = "test-field-ref-simple-001";
    let database_name = &generate_unique_name("test_db_field_simple");
    let table_name = &generate_unique_name("test_table_field_simple");

    println!(
        "Testing simple field reference, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("field_001")),
            ("username", create_string_datum("alice")),
            ("email", create_string_datum("alice@example.com")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("field_002")),
            ("username", create_string_datum("bob")),
            ("email", create_string_datum("bob@example.com")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test field reference: username == "alice"
    let field_expr = create_field_expression(vec!["username"]);
    let literal_expr = create_literal_expression(create_string_datum("alice"));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Eq, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item with username = alice"
        );

        // Verify the username
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let username = obj
                    .fields
                    .get("username")
                    .expect("Username field should exist");
                if let Some(proto::datum::Value::String(username_val)) = &username.value {
                    assert_eq!(username_val, "alice", "Username should be alice");
                }
            }
        }

        println!("✓ Simple field reference returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_field_reference_nested() {
    let query_id = "test-field-ref-nested-001";
    let database_name = &generate_unique_name("test_db_field_nested");
    let table_name = &generate_unique_name("test_table_field_nested");

    println!(
        "Testing nested field reference, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with nested objects
    let address1 = create_datum_object(vec![
        ("street", create_string_datum("123 Main St")),
        ("city", create_string_datum("New York")),
        ("country", create_string_datum("USA")),
    ]);

    let address2 = create_datum_object(vec![
        ("street", create_string_datum("456 Oak Ave")),
        ("city", create_string_datum("Los Angeles")),
        ("country", create_string_datum("USA")),
    ]);

    let address3 = create_datum_object(vec![
        ("street", create_string_datum("789 Pine Rd")),
        ("city", create_string_datum("London")),
        ("country", create_string_datum("UK")),
    ]);

    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("nested_001")),
            ("name", create_string_datum("Alice")),
            (
                "address",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(address1)),
                },
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("nested_002")),
            ("name", create_string_datum("Bob")),
            (
                "address",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(address2)),
                },
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("nested_003")),
            ("name", create_string_datum("Charlie")),
            (
                "address",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(address3)),
                },
            ),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test nested field reference: address.country == "USA"
    let field_expr = create_field_expression(vec!["address", "country"]);
    let literal_expr = create_literal_expression(create_string_datum("USA"));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Eq, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with USA address");

        // Verify the nested field values
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let address = obj
                    .fields
                    .get("address")
                    .expect("Address field should exist");
                if let Some(proto::datum::Value::Object(addr_obj)) = &address.value {
                    let country = addr_obj
                        .fields
                        .get("country")
                        .expect("Country field should exist");
                    if let Some(proto::datum::Value::String(country_val)) = &country.value {
                        assert_eq!(country_val, "USA", "Country should be USA");
                    }
                }
            }
        }

        println!("✓ Nested field reference returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_field_reference_multiple_levels() {
    let query_id = "test-field-ref-deep-001";
    let database_name = &generate_unique_name("test_db_field_deep");
    let table_name = &generate_unique_name("test_table_field_deep");

    println!(
        "Testing deeply nested field reference, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{}-db-create", query_id), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{}-table-create", query_id), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Create deeply nested test data
    let geo1 = create_datum_object(vec![
        ("lat", create_float_datum(40.7128)),
        ("lng", create_float_datum(-74.0060)),
    ]);

    let geo2 = create_datum_object(vec![
        ("lat", create_float_datum(34.0522)),
        ("lng", create_float_datum(-118.2437)),
    ]);

    let location1 = create_datum_object(vec![
        ("address", create_string_datum("123 Main St")),
        ("city", create_string_datum("New York")),
        (
            "coordinates",
            proto::Datum {
                value: Some(proto::datum::Value::Object(geo1)),
            },
        ),
    ]);

    let location2 = create_datum_object(vec![
        ("address", create_string_datum("456 Oak Ave")),
        ("city", create_string_datum("Los Angeles")),
        (
            "coordinates",
            proto::Datum {
                value: Some(proto::datum::Value::Object(geo2)),
            },
        ),
    ]);

    let profile1 = create_datum_object(vec![
        ("name", create_string_datum("Alice")),
        (
            "location",
            proto::Datum {
                value: Some(proto::datum::Value::Object(location1)),
            },
        ),
    ]);

    let profile2 = create_datum_object(vec![
        ("name", create_string_datum("Bob")),
        (
            "location",
            proto::Datum {
                value: Some(proto::datum::Value::Object(location2)),
            },
        ),
    ]);

    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("deep_001")),
            (
                "user",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(profile1)),
                },
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("deep_002")),
            (
                "user",
                proto::Datum {
                    value: Some(proto::datum::Value::Object(profile2)),
                },
            ),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test deep nested field reference: user.location.city == "New York"
    let field_expr = create_field_expression(vec!["user", "location", "city"]);
    let literal_expr = create_literal_expression(create_string_datum("New York"));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Eq, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item with New York city"
        );

        // Verify the deeply nested field value
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let user = obj.fields.get("user").expect("User field should exist");
                if let Some(proto::datum::Value::Object(user_obj)) = &user.value {
                    let location = user_obj
                        .fields
                        .get("location")
                        .expect("Location field should exist");
                    if let Some(proto::datum::Value::Object(loc_obj)) = &location.value {
                        let city = loc_obj.fields.get("city").expect("City field should exist");
                        if let Some(proto::datum::Value::String(city_val)) = &city.value {
                            assert_eq!(city_val, "New York", "City should be New York");
                        }
                    }
                }
            }
        }

        println!("✓ Deep nested field reference returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test another deep nested field: user.location.coordinates.lat > 35.0
    let field_expr = create_field_expression(vec!["user", "location", "coordinates", "lat"]);
    let literal_expr = create_literal_expression(create_float_datum(35.0));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Gt, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-lat", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with lat > 35.0");

        println!("✓ Deep nested numeric field reference returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Deep nested field reference test completed successfully");
}
