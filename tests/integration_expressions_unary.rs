mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_unary_expression_not_boolean() {
    let query_id = "test-unary-expr-not-bool-001";
    let database_name = &generate_unique_name("test_db_unary_not_bool");
    let table_name = &generate_unique_name("test_table_unary_not");

    println!(
        "Testing unary NOT expression with boolean, ID: {}, database: {}, table: {}",
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

    // Insert test documents with boolean fields
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("unary_001")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("unary_002")),
            ("active", create_bool_datum(false)),
            ("verified", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("unary_003")),
            ("active", create_bool_datum(true)),
            ("verified", create_bool_datum(true)),
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

    // Test unary NOT expression: NOT active (equivalent to active != true)
    let field_expr = create_field_expression(vec!["active"]);
    let unary_not_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(field_expr)),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, unary_not_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");
    validate_response_envelope(&filter_response, &format!("{}-filter", query_id))
        .expect("Filter response validation failed");

    // Decode and validate the response
    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item where active is false"
        );

        // Verify the returned document
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let active = obj.fields.get("active").expect("Active field should exist");
                if let Some(proto::datum::Value::Bool(active_val)) = &active.value {
                    assert!(!(*active_val), "Active should be false");
                } else {
                    panic!("Active field should be a boolean");
                }
            } else {
                panic!("Result item should be an object");
            }
        }

        println!("✓ Unary NOT expression returned correct results");
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
async fn test_unary_expression_not_with_binary_expression() {
    let query_id = "test-unary-not-binary-001";
    let database_name = &generate_unique_name("test_db_unary_not_binary");
    let table_name = &generate_unique_name("test_table_unary_not_binary");

    println!(
        "Testing unary NOT with binary expression, ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("not_binary_001")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("not_binary_002")),
            ("status", create_string_datum("inactive")),
            ("score", create_int_datum(65)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("not_binary_003")),
            ("status", create_string_datum("pending")),
            ("score", create_int_datum(95)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test NOT with binary expression: NOT (score > 80)
    let binary_expr = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(80)),
    );
    let unary_not_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(binary_expr)),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, unary_not_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with score <= 80");

        // Verify the score is indeed <= 80
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let score = obj.fields.get("score").expect("Score field should exist");
                if let Some(proto::datum::Value::Int(score_val)) = &score.value {
                    assert!(*score_val <= 80, "Score should be <= 80");
                }
            }
        }

        println!("✓ Unary NOT with binary expression returned correct results");
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
async fn test_unary_expression_not_with_logical_operations() {
    let query_id = "test-unary-not-logical-001";
    let database_name = &generate_unique_name("test_db_unary_not_logical");
    let table_name = &generate_unique_name("test_table_unary_not_logical");

    println!(
        "Testing unary NOT with logical operations, ID: {}, database: {}, table: {}",
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

    // Insert test data for logical operations with NOT
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("logical_not_001")),
            ("premium", create_bool_datum(true)),
            ("verified", create_bool_datum(true)),
            ("score", create_int_datum(95)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_not_002")),
            ("premium", create_bool_datum(true)),
            ("verified", create_bool_datum(false)),
            ("score", create_int_datum(80)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_not_003")),
            ("premium", create_bool_datum(false)),
            ("verified", create_bool_datum(true)),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("logical_not_004")),
            ("premium", create_bool_datum(false)),
            ("verified", create_bool_datum(false)),
            ("score", create_int_datum(70)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test NOT with AND expression: NOT (premium == true AND verified == true)
    // This should find all items except the first one
    let premium_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["premium"]),
        create_literal_expression(create_bool_datum(true)),
    );
    let verified_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["verified"]),
        create_literal_expression(create_bool_datum(true)),
    );
    let and_expr =
        create_binary_expression(proto::binary_op::Operator::And, premium_expr, verified_expr);
    let not_and_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(and_expr)),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, not_and_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-not-and", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send NOT AND filter envelope");

    let response =
        decode_response_payload(&filter_response).expect("Failed to decode NOT AND response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            3,
            "Should find 3 items where NOT (premium=true AND verified=true)"
        );
        println!("✓ Unary NOT with AND expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test NOT with OR expression: NOT (premium == false OR score < 80)
    // This should find items that are premium=true AND score >= 80
    let premium_false_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["premium"]),
        create_literal_expression(create_bool_datum(false)),
    );
    let score_lt_expr = create_binary_expression(
        proto::binary_op::Operator::Lt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(80)),
    );
    let or_expr = create_binary_expression(
        proto::binary_op::Operator::Or,
        premium_false_expr,
        score_lt_expr,
    );
    let not_or_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(or_expr)),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, not_or_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-not-or", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send NOT OR filter envelope");

    let response =
        decode_response_payload(&filter_response).expect("Failed to decode NOT OR response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items where NOT (premium=false OR score < 80)"
        );

        // Verify that all returned items have premium=true AND score >= 80
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let premium = obj
                    .fields
                    .get("premium")
                    .expect("Premium field should exist");
                let score = obj.fields.get("score").expect("Score field should exist");

                if let (
                    Some(proto::datum::Value::Bool(premium_val)),
                    Some(proto::datum::Value::Int(score_val)),
                ) = (&premium.value, &score.value)
                {
                    assert!(*premium_val, "Premium should be true");
                    assert!(*score_val >= 80, "Score should be >= 80");
                }
            }
        }

        println!("✓ Unary NOT with OR expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Logical operations with NOT test completed successfully");
}

#[tokio::test]
async fn test_unary_expression_double_negation() {
    let query_id = "test-unary-double-not-001";
    let database_name = &generate_unique_name("test_db_unary_double_not");
    let table_name = &generate_unique_name("test_table_unary_double_not");

    println!(
        "Testing double negation (NOT NOT), ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("double_not_001")),
            ("enabled", create_bool_datum(true)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("double_not_002")),
            ("enabled", create_bool_datum(false)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("double_not_003")),
            ("enabled", create_bool_datum(true)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test double negation: NOT (NOT enabled)
    // This should be equivalent to just "enabled"
    let field_expr = create_field_expression(vec!["enabled"]);
    let first_not = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(field_expr)),
        }))),
    };
    let double_not = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(first_not)),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, double_not);

    let filter_envelope =
        create_envelope(&format!("{}-filter-double-not", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send double NOT filter envelope");

    let response =
        decode_response_payload(&filter_response).expect("Failed to decode double NOT response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items where enabled=true (double negation)"
        );

        // Verify that all returned items have enabled=true
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let enabled = obj
                    .fields
                    .get("enabled")
                    .expect("Enabled field should exist");
                if let Some(proto::datum::Value::Bool(enabled_val)) = &enabled.value {
                    assert!(*enabled_val, "Enabled should be true after double negation");
                }
            }
        }

        println!("✓ Double negation returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Double negation test completed successfully");
}
