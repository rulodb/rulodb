mod common;

use common::*;
use rulodb::ast::proto;

#[tokio::test]
async fn test_match_expression_basic_regex() {
    let query_id = "test-match-expr-basic-001";
    let database_name = &generate_unique_name("test_db_match_basic");
    let table_name = &generate_unique_name("test_table_match_basic");

    println!(
        "Testing basic match expression with regex, ID: {query_id}, database: {database_name}, table: {table_name}"
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

    // Insert test documents with various email patterns
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("match_001")),
            ("email", create_string_datum("alice@example.com")),
            ("phone", create_string_datum("+1-555-1234")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("match_002")),
            ("email", create_string_datum("bob@gmail.com")),
            ("phone", create_string_datum("555.9876")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("match_003")),
            ("email", create_string_datum("charlie.doe@company.org")),
            ("phone", create_string_datum("(555) 555-0123")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("match_004")),
            ("email", create_string_datum("invalid-email")),
            ("phone", create_string_datum("123")),
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

    // Test match expression for email pattern: email matches ".*@.*\.com$"
    let field_expr = create_field_expression(vec!["email"]);
    let match_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(field_expr)),
            pattern: r".*@.*\.com$".to_string(),
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, match_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");
    validate_response_envelope(&filter_response, &format!("{query_id}-filter"))
        .expect("Filter response validation failed");

    // Decode and validate the response
    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 .com email addresses");

        // Verify the returned documents have .com emails
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let email = obj.fields.get("email").expect("Email field should exist");
                if let Some(proto::datum::Value::String(email_val)) = &email.value {
                    assert!(
                        email_val.ends_with(".com"),
                        "Email should end with .com: {email_val}"
                    );
                } else {
                    panic!("Email field should be a string");
                }
            } else {
                panic!("Result item should be an object");
            }
        }

        println!("✓ Match expression for .com emails returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    let _db_drop_response = send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to send database drop envelope");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_match_expression_phone_number_patterns() {
    let query_id = "test-match-expr-phone-001";
    let database_name = &generate_unique_name("test_db_match_phone");
    let table_name = &generate_unique_name("test_table_match_phone");

    println!(
        "Testing match expression for phone number patterns, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup database and table
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with various phone number formats
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("phone_001")),
            ("contact", create_string_datum("+1-555-123-4567")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("phone_002")),
            ("contact", create_string_datum("(555) 987-6543")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("phone_003")),
            ("contact", create_string_datum("555.555.0123")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("phone_004")),
            ("contact", create_string_datum("not-a-phone-number")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("phone_005")),
            ("contact", create_string_datum("123")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test match expression for US phone number pattern
    // Pattern matches: +1-XXX-XXX-XXXX, (XXX) XXX-XXXX, XXX.XXX.XXXX
    let field_expr = create_field_expression(vec!["contact"]);
    let match_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(field_expr)),
            pattern: r"^(\+1-\d{3}-\d{3}-\d{4}|\(\d{3}\) \d{3}-\d{4}|\d{3}\.\d{3}\.\d{4})$"
                .to_string(),
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, match_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 3, "Should find 3 valid phone numbers");

        // Verify the returned contacts are valid phone numbers
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let contact = obj
                    .fields
                    .get("contact")
                    .expect("Contact field should exist");
                if let Some(proto::datum::Value::String(contact_val)) = &contact.value {
                    assert!(
                        contact_val.contains('-')
                            || contact_val.contains('.')
                            || contact_val.contains('('),
                        "Contact should be a formatted phone number: {contact_val}"
                    );
                }
            }
        }

        println!("✓ Match expression for phone numbers returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_match_expression_case_insensitive() {
    let query_id = "test-match-expr-case-001";
    let database_name = &generate_unique_name("test_db_match_case");
    let table_name = &generate_unique_name("test_table_match_case");

    println!(
        "Testing case-insensitive match expression, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with mixed case strings
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("case_001")),
            ("name", create_string_datum("ALICE SMITH")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("case_002")),
            ("name", create_string_datum("bob johnson")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("case_003")),
            ("name", create_string_datum("Charlie Brown")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("case_004")),
            ("name", create_string_datum("David Wilson")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test case-insensitive match for names containing "smith" or "brown"
    let field_expr = create_field_expression(vec!["name"]);
    let match_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(field_expr)),
            pattern: r".*(smith|brown).*".to_string(),
            flags: "(?i)".to_string(), // Case-insensitive flag
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, match_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 names containing Smith or Brown (case-insensitive)"
        );

        // Verify the returned names contain "smith" or "brown" (case-insensitive)
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let name = obj.fields.get("name").expect("Name field should exist");
                if let Some(proto::datum::Value::String(name_val)) = &name.value {
                    let name_lower = name_val.to_lowercase();
                    assert!(
                        name_lower.contains("smith") || name_lower.contains("brown"),
                        "Name should contain Smith or Brown: {name_val}"
                    );
                }
            }
        }

        println!("✓ Case-insensitive match expression returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Test completed successfully");
}

#[tokio::test]
async fn test_match_expression_complex_patterns() {
    let query_id = "test-match-expr-complex-001";
    let database_name = &generate_unique_name("test_db_match_complex");
    let table_name = &generate_unique_name("test_table_match_complex");

    println!(
        "Testing complex match expression patterns, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with various URL patterns
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("url_001")),
            ("url", create_string_datum("https://www.example.com/page")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("url_002")),
            (
                "url",
                create_string_datum("http://api.service.org/v1/endpoint"),
            ),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("url_002")),
            (
                "url",
                create_string_datum("https://secure.bank.com:8443/login"),
            ),
            ("description", create_string_datum("Secure banking site")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("url_003")),
            (
                "url",
                create_string_datum("http://api.service.org/v1/endpoint"),
            ),
            ("description", create_string_datum("REST API endpoint")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("url_004")),
            ("url", create_string_datum("ftp://files.example.net/data")),
            ("description", create_string_datum("File transfer location")),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("url_005")),
            ("url", create_string_datum("mailto:admin@company.com")),
            ("description", create_string_datum("Contact email")),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test complex URL pattern matching: https URLs with optional port
    let field_expr = create_field_expression(vec!["url"]);
    let match_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(field_expr)),
            pattern: r"^https://[a-zA-Z0-9.-]+(?::[0-9]+)?/.*$".to_string(),
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, match_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(array.items.len(), 2, "Should find 2 HTTPS URLs");

        // Verify the returned URLs are HTTPS
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let url = obj.fields.get("url").expect("URL field should exist");
                if let Some(proto::datum::Value::String(url_val)) = &url.value {
                    assert!(
                        url_val.starts_with("https://"),
                        "URL should start with https://: {url_val}"
                    );
                }
            }
        }

        println!("✓ Complex HTTPS URL pattern returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test another complex pattern: match domain names ending with .com or .org
    let field_expr = create_field_expression(vec!["url"]);
    let match_expr = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(field_expr)),
            pattern: r"://[^/]*\.(com|org)".to_string(),
            flags: "".to_string(),
        }))),
    };
    let filter_query = create_filter_query(database_name, table_name, match_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter-domain"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            3,
            "Should find 3 URLs with .com or .org domains"
        );

        // Verify the returned URLs have .com or .org domains
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let url = obj.fields.get("url").expect("URL field should exist");
                if let Some(proto::datum::Value::String(url_val)) = &url.value {
                    assert!(
                        url_val.contains(".com") || url_val.contains(".org"),
                        "URL should contain .com or .org: {url_val}"
                    );
                }
            }
        }

        println!("✓ Complex domain pattern returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Complex patterns test completed successfully");
}

#[tokio::test]
async fn test_match_expression_with_binary_operations() {
    let query_id = "test-match-expr-binary-001";
    let database_name = &generate_unique_name("test_db_match_binary");
    let table_name = &generate_unique_name("test_table_match_binary");

    println!(
        "Testing match expression combined with binary operations, ID: {query_id}, database: {database_name}, table: {table_name}"
    );

    let mut stream = connect_to_server()
        .await
        .expect("Failed to connect to server");

    // Setup
    let db_create_query = create_database_create_query(database_name);
    let db_create_envelope = create_envelope(&format!("{query_id}-db-create"), &db_create_query);
    send_envelope_to_server(&mut stream, &db_create_envelope)
        .await
        .expect("Failed to create database");

    let table_create_query = create_table_create_query(database_name, table_name);
    let table_create_envelope =
        create_envelope(&format!("{query_id}-table-create"), &table_create_query);
    send_envelope_to_server(&mut stream, &table_create_envelope)
        .await
        .expect("Failed to create table");

    // Insert test data with emails and status
    let documents = vec![
        create_datum_object(vec![
            ("id", create_string_datum("combined_001")),
            ("email", create_string_datum("alice@company.com")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(85)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_002")),
            ("email", create_string_datum("bob@personal.org")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(92)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_003")),
            ("email", create_string_datum("charlie@company.com")),
            ("status", create_string_datum("inactive")),
            ("score", create_int_datum(78)),
        ]),
        create_datum_object(vec![
            ("id", create_string_datum("combined_004")),
            ("email", create_string_datum("diana@service.net")),
            ("status", create_string_datum("active")),
            ("score", create_int_datum(95)),
        ]),
    ];

    let insert_query = create_insert_query(database_name, table_name, documents);
    let insert_envelope = create_envelope(&format!("{query_id}-insert"), &insert_query);
    send_envelope_to_server(&mut stream, &insert_envelope)
        .await
        .expect("Failed to insert documents");

    // Test match expression AND binary expression:
    // email matches company domain AND status is active
    let email_match = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(create_field_expression(vec!["email"]))),
            pattern: r".*@company\.com$".to_string(),
            flags: "".to_string(),
        }))),
    };

    let status_eq = create_binary_expression(
        proto::binary_op::Operator::Eq,
        create_field_expression(vec!["status"]),
        create_literal_expression(create_string_datum("active")),
    );

    let combined_expr =
        create_binary_expression(proto::binary_op::Operator::And, email_match, status_eq);

    let filter_query = create_filter_query(database_name, table_name, combined_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter-combined"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            1,
            "Should find 1 item with company email AND active status"
        );

        // Verify the result matches both conditions
        for item in &array.items {
            if let Some(proto::datum::Value::Object(obj)) = &item.value {
                let email = obj.fields.get("email").expect("Email field should exist");
                let status = obj.fields.get("status").expect("Status field should exist");

                if let (
                    Some(proto::datum::Value::String(email_val)),
                    Some(proto::datum::Value::String(status_val)),
                ) = (&email.value, &status.value)
                {
                    assert!(
                        email_val.ends_with("@company.com"),
                        "Email should be from company domain"
                    );
                    assert_eq!(status_val, "active", "Status should be active");
                }
            }
        }

        println!("✓ Match expression combined with binary operation returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Test match expression OR high score:
    // email matches .org domain OR score > 90
    let org_match = proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(create_field_expression(vec!["email"]))),
            pattern: r".*\.org$".to_string(),
            flags: "".to_string(),
        }))),
    };

    let score_gt = create_binary_expression(
        proto::binary_op::Operator::Gt,
        create_field_expression(vec!["score"]),
        create_literal_expression(create_int_datum(90)),
    );

    let or_expr = create_binary_expression(proto::binary_op::Operator::Or, org_match, score_gt);

    let filter_query = create_filter_query(database_name, table_name, or_expr);

    let filter_envelope = create_envelope(&format!("{query_id}-filter-or"), &filter_query);
    let filter_response = send_envelope_to_server(&mut stream, &filter_envelope)
        .await
        .expect("Failed to send filter envelope");

    let response = decode_response_payload(&filter_response).expect("Failed to decode response");

    if let Some(proto::datum::Value::Array(array)) = &response.value {
        assert_eq!(
            array.items.len(),
            2,
            "Should find 2 items with .org email OR score > 90"
        );

        println!("✓ Match expression with OR operation returned correct results");
    } else {
        panic!("Expected array result");
    }

    // Cleanup
    let db_drop_query = create_database_drop_query(database_name);
    let db_drop_envelope = create_envelope(&format!("{query_id}-db-drop"), &db_drop_query);
    send_envelope_to_server(&mut stream, &db_drop_envelope)
        .await
        .expect("Failed to drop database");

    println!("✓ Match expression with binary operations test completed successfully");
}
