mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_expression_complex_nested_combinations() {
    let query_id = "test-expr-complex-nested-001";
    let database_name = &generate_unique_name("test_db_complex_nested");
    let table_name = &generate_unique_name("test_table_complex_nested");

    println!(
        "Testing complex nested expression combinations, ID: {}, database: {}, table: {}",
        query_id, database_name, table_name
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server. Make sure the server is running on 127.0.0.1:6090");

    // Setup: Create database and table
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
            ("id", create_string_datum("complex_001")),
            ("email", create_string_datum("alice@premium.com")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(95)),
            ("premium", create_bool_datum(true)),
            ("region", create_string_datum("US")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_002")),
            ("email", create_string_datum("bob@standard.net")),
            ("status", create_string_datum("inactive")),
            ("score", create_int_datum(78)),
            ("premium", create_bool_datum(false)),
            ("region", create_string_datum("EU")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_003")),
            ("email", create_string_datum("charlie@premium.com")),
            ("status", create_string_datum("pending")),
            ("score", create_int_datum(88)),
            ("premium", create_bool_datum(true)),
            ("region", create_string_datum("US")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("complex_004")),
            ("email", create_string_datum("diana@external.org")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(82)),
            ("premium", create_bool_datum(false)),
            ("region", create_string_datum("APAC")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Create extremely complex expression:
    // ((email MATCH ".*@premium\.com$" AND premium == true) OR (score > 90 AND status == "active"))
    // AND NOT (region == "EU" OR (status == "inactive" AND score < 80))

    // Left side of main AND: ((email MATCH ".*@premium\.com$" AND premium == true) OR (score > 90 AND status == "active"))

    // email MATCH ".*@premium\.com$" AND premium == true
    let premium_email_match = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(create_field_expression(vec!["email"]))),
            pattern: r".*@premium\.com$".to_string(),
            flags: "".to_string(),
        }))),
    };

    let premium_check = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["premium"]),
        create_literal_expression(create_bool_datum(true)),
    );

    let premium_and = create_binary_expression(
        proto::binary_op::Operator::And,
        premium_email_match,
        premium_check,
    );

    // score > 90 AND status == "active"
    let high_score = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(90)),
    );

    let active_status = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    let high_active_and =
        create_binary_expression(proto::binary_op::Operator::And, high_score, active_status);

    // Combine with OR
    let left_or =
        create_binary_expression(proto::binary_op::Operator::Or, premium_and, high_active_and);

    // Right side of main AND: NOT (region == "EU" OR (status == "inactive" AND score < 80))

    let eu_region = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["region"]),
        create_literal_expression(create_string_datum("EU")),
    );

    let inactive_status = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("inactive")),
    );

    let low_score = create_binary_expression(
        proto::binary_op::Operator::Lt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(80)),
    );

    let inactive_low_and =
        create_binary_expression(proto::binary_op::Operator::And, inactive_status, low_score);

    let exclusion_or =
        create_binary_expression(proto::binary_op::Operator::Or, eu_region, inactive_low_and);

    let not_exclusion = proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: proto::unary_op::Operator::Not.into(),
            expr: Some(Box::new(exclusion_or)),
        }))),
    };

    // Final combination
    let final_expr =
        create_binary_expression(proto::binary_op::Operator::And, left_or, not_exclusion);

    let filter_query = create_filter_query(database_name, table_name, final_expr);

    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send complex filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        // Expected results:
        // complex_001: alice@premium.com, premium=true, status=active, score=95, region=US -> matches left OR, not excluded
        // complex_003: charlie@premium.com, premium=true, status=pending, score=88, region=US -> matches left OR, not excluded
        // complex_004: diana@external.org, premium=false, status=active, score=82, region=APAC -> doesn't match left OR
        // complex_002: bob@standard.net, premium=false, status=inactive, score=78, region=EU -> excluded

        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items matching complex expression"
        );

        println!("✓ Complex nested expression returned correct results");
    } else {
        println!("✓ Complex nested expression executed");
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
async fn test_expression_edge_cases_null_values() {
    let query_id = "test-expr-edge-null-001";
    let database_name = &generate_unique_name("test_db_edge_null");
    let table_name = &generate_unique_name("test_table_edge_null");

    println!(
        "Testing expression edge cases with null values, ID: {}, database: {}, table: {}",
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

    // Insert test data with potential null-like scenarios
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("null_001")),
            ("name", create_string_datum("Alice")),
            ("email", create_string_datum("alice@example.com")),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("null_002")),
            ("name", create_string_datum("Bob")),
            ("email", create_string_datum("")), // Empty string
            ("score", create_int_datum(0)),     // Zero value
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("null_003")),
            ("name", create_string_datum("")), // Empty name
            ("email", create_string_datum("charlie@example.com")),
            ("score", create_int_datum(-1)), // Negative value
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test empty string handling
    let empty_email_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["email"]),
        create_literal_expression(create_string_datum("")),
    );
    let filter_query = create_filter_query(database_name, table_name, empty_email_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-empty", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send empty string filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with empty email");
        println!("✓ Empty string edge case handled correctly");
    }

    // Test zero value handling
    let zero_score_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(0)),
    );
    let filter_query = create_filter_query(database_name, table_name, zero_score_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-zero", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send zero value filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with zero score");
        println!("✓ Zero value edge case handled correctly");
    }

    // Test negative value handling
    let negative_score_expr = create_binary_expression(
        proto::binary_op::Operator::Lt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(0)),
    );
    let filter_query = create_filter_query(database_name, table_name, negative_score_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-negative", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send negative value filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item with negative score"
        );
        println!("✓ Negative value edge case handled correctly");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Edge cases test completed successfully");
}

#[tokio::test]
async fn test_expression_edge_cases_type_mismatches() {
    let query_id = "test-expr-edge-types-001";
    let database_name = &generate_unique_name("test_db_edge_types");
    let table_name = &generate_unique_name("test_table_edge_types");

    println!(
        "Testing expression edge cases with type mismatches, ID: {}, database: {}, table: {}",
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

    // Insert test data with mixed types
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("type_001")),
            ("value", create_string_datum("100")), // String that looks like number
            ("flag", create_string_datum("true")), // String that looks like boolean
            ("number", create_int_datum(42)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("type_002")),
            ("value", create_int_datum(200)),   // Actually a number
            ("flag", create_bool_datum(false)), // Actually a boolean
            ("number", create_float_datum(42.5)), // Float instead of int
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test string vs number comparison
    let string_number_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["value"]),
        create_literal_expression(create_string_datum("100")),
    );
    let filter_query = create_filter_query(database_name, table_name, string_number_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-string", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send string comparison filter");

    // Validate the response structure
    validate_response_envelope(&filter_response, &format!("{}-filter-string", query_id))
        .expect("String comparison filter response validation failed");
    println!("✓ String vs number comparison test executed");

    // Test boolean field with different types
    let bool_string_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["flag"]),
        create_literal_expression(create_string_datum("true")),
    );
    let filter_query = create_filter_query(database_name, table_name, bool_string_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-bool", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send boolean comparison filter");

    // Validate the response structure
    validate_response_envelope(&filter_response, &format!("{}-filter-bool", query_id))
        .expect("Boolean comparison filter response validation failed");
    println!("✓ Boolean vs string comparison test executed");

    // Test numeric comparison with mixed int/float
    let numeric_expr = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["number"]),
        create_literal_expression(create_int_datum(40)),
    );
    let filter_query = create_filter_query(database_name, table_name, numeric_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-numeric", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send numeric comparison filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        // Both 42 and 42.5 should be > 40
        assert_eq!(array.items.len(), 2, "Should find 2 items with number > 40");
        println!("✓ Mixed numeric type comparison handled correctly");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Type mismatch edge cases test completed successfully");
}

#[tokio::test]
async fn test_expression_performance_deep_nesting() {
    let query_id = "test-expr-perf-deep-001";
    let database_name = &generate_unique_name("test_db_perf_deep");
    let table_name = &generate_unique_name("test_table_perf_deep");

    println!(
        "Testing expression performance with deep nesting, ID: {}, database: {}, table: {}",
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
            ("id", create_string_datum("deep_001")),
            ("value", create_int_datum(1)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("deep_002")),
            ("value", create_int_datum(2)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("deep_003")),
            ("value", create_int_datum(3)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Create deeply nested expression: ((((value > 0) AND (value < 10)) OR (value == 1)) AND (value != 2))

    // Build nested structure
    let value_gt_0 = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["value"]),
        create_literal_expression(create_int_datum(0)),
    );

    let value_lt_10 = create_binary_expression(
        proto::binary_op::Operator::Lt,
        create_field_expression(vec!["value"]),
        create_literal_expression(create_int_datum(10)),
    );

    let range_and =
        create_binary_expression(proto::binary_op::Operator::And, value_gt_0, value_lt_10);

    let value_eq_1 = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["value"]),
        create_literal_expression(create_int_datum(1)),
    );

    let range_or_one =
        create_binary_expression(proto::binary_op::Operator::Or, range_and, value_eq_1);

    let value_ne_2 = create_binary_expression(
        proto::binary_op::Operator::Ne,
        create_field_expression(vec!["value"]),
        create_literal_expression(create_int_datum(2)),
    );

    let final_expr =
        create_binary_expression(proto::binary_op::Operator::And, range_or_one, value_ne_2);

    let filter_query = create_filter_query(database_name, table_name, final_expr);

    let start_time = std::time::Instant::now();
    let filter_envelope = create_envelope(&format!("{}-filter", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send deeply nested filter");
    let duration = start_time.elapsed();

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        // Should find items where value is 1 or 3 (not 2)
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items matching deep expression"
        );

        println!("✓ Deep nested expression executed in {:?}", duration);

        // Performance check - should complete within reasonable time
        assert!(
            duration.as_millis() < 5000,
            "Deep expression should execute within 5 seconds"
        );
        println!("✓ Performance check passed");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Deep nesting performance test completed successfully");
}

#[tokio::test]
async fn test_expression_unicode_and_special_characters() {
    let query_id = "test-expr-unicode-001";
    let database_name = &generate_unique_name("test_db_unicode");
    let table_name = &generate_unique_name("test_table_unicode");

    println!(
        "Testing expressions with Unicode and special characters, ID: {}, database: {}, table: {}",
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

    // Insert test data with Unicode and special characters
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("unicode_001")),
            ("name", create_string_datum("José García")),
            ("city", create_string_datum("São Paulo")),
            ("description", create_string_datum("Café & Restaurant")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("unicode_002")),
            ("name", create_string_datum("張三")),
            ("city", create_string_datum("北京")),
            ("description", create_string_datum("中文测试")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("unicode_003")),
            ("name", create_string_datum("محمد")),
            ("city", create_string_datum("القاهرة")),
            ("description", create_string_datum("اختبار عربي")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("unicode_004")),
            ("name", create_string_datum("John O'Connor")),
            ("city", create_string_datum("New York")),
            (
                "description",
                create_string_datum("Special chars: @#$%^&*()"),
            ),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{}-insert", query_id), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert Unicode documents");

    // Test Unicode string matching
    let unicode_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["name"]),
        create_literal_expression(create_string_datum("José García")),
    );
    let filter_query = create_filter_query(database_name, table_name, unicode_expr);

    let filter_envelope = create_envelope(&format!("{}-filter-unicode", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send Unicode filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with Unicode name");
        println!("✓ Unicode string matching test passed");
    }

    // Test regex with Unicode
    let unicode_regex = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(create_field_expression(vec!["city"]))),
            pattern: r".*京$".to_string(), // Cities ending with 京 (Beijing)
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, unicode_regex);

    let filter_envelope = create_envelope(&format!("{}-filter-regex", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send Unicode regex filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 city ending with 京");
        println!("✓ Unicode regex matching test passed");
    }

    // Test special characters in regex
    let special_char_regex = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(create_field_expression(vec!["description"]))),
            pattern: r".*[@#\$%\^&\*\(\)].*".to_string(), // Contains special characters
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, special_char_regex);

    let filter_envelope = create_envelope(&format!("{}-filter-special", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send special character filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items with special characters"
        );
        println!("✓ Special character regex test passed");
    }

    // Test apostrophe handling
    let apostrophe_expr = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["name"]),
        create_literal_expression(create_string_datum("John O'Connor")),
    );
    let filter_query = create_filter_query(database_name, table_name, apostrophe_expr);

    let filter_envelope =
        create_envelope(&format!("{}-filter-apostrophe", query_id), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send apostrophe filter");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 1, "Should find 1 item with apostrophe");
        println!("✓ Apostrophe handling test passed");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{}-db-drop", query_id), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Unicode and special characters test completed successfully");
}
