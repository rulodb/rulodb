mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_binary_expression_equal_string() {
    let query_id = "test-binary-expr-eq-string-001";
    let database_name = &generate_unique_name("test_db_binary_eq_str");
    let table_name = &generate_unique_name("test_table_binary_eq");

    println!(
        "Testing binary expression EQ with strings, ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("binary_001")),
            ("name", create_string_datum("Alice")),
            ("category", create_string_datum("premium")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("binary_002")),
            ("name", create_string_datum("Bob")),
            ("category", create_string_datum("standard")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("binary_003")),
            ("name", create_string_datum("Charlie")),
            ("category", create_string_datum("premium")),
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

    // Test binary EQ expression: category == "premium"
    let field_expr = create_field_expression(vec!["category"]);
    let literal_expr = create_literal_expression(create_string_datum("premium"));
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
        assert_eq!(array.items.len(), 2, "Should find 2 premium category items");

        // Verify the returned documents
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let category = obj
                    .fields
                    .get("category")
                    .expect("Category field should exist");
                if let Some(proto::datum::Value::String(cat_val)) = &category.value {
                    assert_eq!(cat_val, "premium", "Category should be premium");
                } else {
                    panic!("Category field should be a string");
                }
            } else {
                panic!("Result item should be an object");
            }
        }

        println!("✓ Binary EQ expression returned correct results");
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
async fn test_binary_expression_not_equal() {
    let query_id = "test-binary-expr-ne-001";
    let database_name = &generate_unique_name("test_db_binary_ne");
    let table_name = &generate_unique_name("test_table_binary_ne");

    println!(
        "Testing binary expression NE, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup database and table
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
            ("id", create_string_datum("ne_001")),
            ("status", create_string_datum("active")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("ne_002")),
            ("status", create_string_datum("inactive")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("ne_003")),
            ("status", create_string_datum("pending")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test NE expression: status != "active"
    let field_expr = create_field_expression(vec!["status"]);
    let literal_expr = create_literal_expression(create_string_datum("active"));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Ne, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 non-active items");
        println!("✓ Binary NE expression returned correct results");
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
async fn test_binary_expression_numeric_comparisons() {
    let query_id = "test-binary-expr-numeric-001";
    let database_name = &generate_unique_name("test_db_binary_numeric");
    let table_name = &generate_unique_name("test_table_binary_numeric");

    println!(
        "Testing binary numeric comparisons, ID: {}, database: {}, table: {}",
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

    // Insert numeric test data
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("num_001")),
            ("score", create_int_datum(85)),
            ("price", create_float_datum(19.99)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("num_002")),
            ("score", create_int_datum(92)),
            ("price", create_float_datum(29.99)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("num_003")),
            ("score", create_int_datum(78)),
            ("price", create_float_datum(15.99)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test GT expression: score > 80
    let field_expr = create_field_expression(vec!["score"]);
    let literal_expr = create_literal_expression(create_int_datum(80));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Gt, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-gt", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send GT filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode GT response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with score > 80");
        println!("✓ Binary GT expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test LT expression: score < 90
    let field_expr = create_field_expression(vec!["score"]);
    let literal_expr = create_literal_expression(create_int_datum(90));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Lt, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-lt", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send LT filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode LT response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with score < 90");
        println!("✓ Binary LT expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test LE expression: score <= 85
    let field_expr = create_field_expression(vec!["score"]);
    let literal_expr = create_literal_expression(create_int_datum(85));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Le, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-le", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send LE filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode LE response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with score <= 85");
        println!("✓ Binary LE expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test GE expression: score >= 85
    let field_expr = create_field_expression(vec!["score"]);
    let literal_expr = create_literal_expression(create_int_datum(85));
    let binary_expr =
        create_binary_expression(proto::binary_op::Operator::Ge, field_expr, literal_expr);
    let filter_query = create_filter_query(database_name, table_name, binary_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-ge", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send GE filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode GE response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 items with score >= 85");
        println!("✓ Binary GE expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Numeric comparisons test completed successfully");
}

#[tokio::test]
async fn test_binary_expression_logical_operations() {
    let query_id = "test-binary-expr-logical-001";
    let database_name = &generate_unique_name("test_db_binary_logical");
    let table_name = &generate_unique_name("test_table_binary_logical");

    println!(
        "Testing binary logical operations, ID: {}, database: {}, table: {}",
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

    // Insert test data for logical operations
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("logical_001")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(true)),
            ("score", create_int_datum(95)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_002")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(false)),
            ("score", create_int_datum(80)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_003")),
            ("active", create_bool_datum(false)),
            ("verified", create_bool_datum(true)),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_004")),
            ("active", create_bool_datum(false)),
            ("verified", create_bool_datum(false)),
            ("score", create_int_datum(70)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test AND expression: active == true AND verified == true
    let active_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["active"]),
        create_literal_expression(create_bool_datum(true)),
    );
    let verified_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["verified"]),
        create_literal_expression(create_bool_datum(true)),
    );
    let and_expr =
        create_binary_expression(proto::binary_op::Operator::And, active_expr, verified_expr);
    let filter_query = create_filter_query(database_name, table_name, and_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-and", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send AND filter envelope");

    let response =
        decode_response_payload(&filter_response).expect("Failed to decode AND response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item with active=true AND verified=true"
        );
        println!("✓ Binary AND expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test OR expression: active == false OR score > 90
    let active_false_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["active"]),
        create_literal_expression(create_bool_datum(false)),
    );
    let score_gt_expr = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(90)),
    );
    let or_expr = create_binary_expression(
        proto::binary_op::Operator::Or,
        active_false_expr,
        score_gt_expr,
    );
    let filter_query = create_filter_query(database_name, table_name, or_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-or", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send OR filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode OR response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            3,
            "Should find 3 items with active=false OR score > 90"
        );
        println!("✓ Binary OR expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Logical operations test completed successfully");
}

#[tokio::test]
async fn test_binary_expression_nested_operations() {
    let query_id = "test-binary-expr-nested-001";
    let database_name = &generate_unique_name("test_db_binary_nested");
    let table_name = &generate_unique_name("test_table_binary_nested");

    println!(
        "Testing nested binary operations, ID: {}, database: {}, table: {}",
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

    // Insert complex test data
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("nested_001")),
            ("category", create_string_datum("electronics")),
            ("price", create_float_datum(299.99)),
            ("in_stock", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("nested_002")),
            ("category", create_string_datum("books")),
            ("price", create_float_datum(19.99)),
            ("in_stock", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("nested_003")),
            ("category", create_string_datum("electronics")),
            ("price", create_float_datum(199.99)),
            ("in_stock", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("nested_004")),
            ("category", create_string_datum("clothing")),
            ("price", create_float_datum(49.99)),
            ("in_stock", create_bool_datum(true)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test nested expression: (category == "electronics" AND price > 200) OR (in_stock == true AND price < 50)
    // Left side: category == "electronics" AND price > 200
    let category_electronics = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["category"]),
        create_literal_expression(create_string_datum("electronics")),
    );
    let price_gt_200 = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["price"]),
        create_literal_expression(create_float_datum(200.0)),
    );
    let left_and = create_binary_expression(
        proto::binary_op::Operator::And,
        category_electronics,
        price_gt_200,
    );

    // Right side: in_stock == true AND price < 50
    let in_stock_true = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["in_stock"]),
        create_literal_expression(create_bool_datum(true)),
    );
    let price_lt_50 = create_binary_expression(
        proto::binary_op::Operator::Lt,
        create_field_expression(vec!["price"]),
        create_literal_expression(create_float_datum(50.0)),
    );
    let right_and =
        create_binary_expression(proto::binary_op::Operator::And, in_stock_true, price_lt_50);

    // Combine with OR
    let nested_expr = create_binary_expression(proto::binary_op::Operator::Or, left_and, right_and);

    let filter_query = create_filter_query(database_name, table_name, nested_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-nested", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send nested filter envelope");

    let response =
        decode_response_payload(&filter_response).expect("Failed to decode nested response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        // Should find 3 items:
        // 1. nested_001: electronics with price 299.99 (matches left side)
        // 2. nested_002: books with price 19.99 and in_stock=true (matches right side)
        // 3. nested_004: clothing with price 49.99 and in_stock=true (matches right side)
        assert_eq!(
            array.items.len(),
            3,
            "Should find 3 items matching nested expression"
        );
        println!("✓ Nested binary expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Nested operations test completed successfully");
}
