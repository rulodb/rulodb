use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rmpv::Value;
use rulodb::ast::TermType;
use rulodb::parser::Parser;
use std::hint::black_box;

fn create_database_query() -> Value {
    Value::Array(vec![
        Value::from(TermType::DatabaseCreate as u64),
        Value::Array(vec![Value::String("test_db".into())]),
        Value::Map(vec![]),
    ])
}

fn create_table_query() -> Value {
    let db_term = Value::Array(vec![
        Value::from(TermType::Database as u64),
        Value::Array(vec![Value::String("test_db".into())]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::TableCreate as u64),
        Value::Array(vec![db_term, Value::String("users".into())]),
        Value::Map(vec![]),
    ])
}

fn create_simple_filter_query() -> Value {
    let table_term = Value::Array(vec![
        Value::from(TermType::Table as u64),
        Value::Array(vec![Value::String("users".into())]),
        Value::Map(vec![]),
    ]);

    let field_expr = Value::Array(vec![
        Value::from(TermType::GetField as u64),
        Value::Array(vec![Value::String("age".into())]),
        Value::Map(vec![]),
    ]);

    let constant_expr = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::Integer(25.into())]),
        Value::Map(vec![]),
    ]);

    let predicate = Value::Array(vec![
        Value::from(TermType::Gt as u64),
        Value::Array(vec![field_expr, constant_expr]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::Filter as u64),
        Value::Array(vec![table_term, predicate]),
        Value::Map(vec![]),
    ])
}

fn create_complex_filter_query() -> Value {
    let table_term = Value::Array(vec![
        Value::from(TermType::Table as u64),
        Value::Array(vec![Value::String("users".into())]),
        Value::Map(vec![]),
    ]);

    // age > 25
    let age_field = Value::Array(vec![
        Value::from(TermType::GetField as u64),
        Value::Array(vec![Value::String("age".into())]),
        Value::Map(vec![]),
    ]);
    let age_value = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::Integer(25.into())]),
        Value::Map(vec![]),
    ]);
    let age_condition = Value::Array(vec![
        Value::from(TermType::Gt as u64),
        Value::Array(vec![age_field, age_value]),
        Value::Map(vec![]),
    ]);

    // status == "active"
    let status_field = Value::Array(vec![
        Value::from(TermType::GetField as u64),
        Value::Array(vec![Value::String("status".into())]),
        Value::Map(vec![]),
    ]);
    let status_value = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::String("active".into())]),
        Value::Map(vec![]),
    ]);
    let status_condition = Value::Array(vec![
        Value::from(TermType::Eq as u64),
        Value::Array(vec![status_field, status_value]),
        Value::Map(vec![]),
    ]);

    // Combined with AND
    let combined_predicate = Value::Array(vec![
        Value::from(TermType::And as u64),
        Value::Array(vec![age_condition, status_condition]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::Filter as u64),
        Value::Array(vec![table_term, combined_predicate]),
        Value::Map(vec![]),
    ])
}

fn create_insert_query(num_docs: usize) -> Value {
    let table_term = Value::Array(vec![
        Value::from(TermType::Table as u64),
        Value::Array(vec![Value::String("users".into())]),
        Value::Map(vec![]),
    ]);

    let documents: Vec<Value> = (0..num_docs)
        .map(|i| {
            Value::Map(vec![
                (
                    Value::String("id".into()),
                    Value::String(format!("user_{i}").into()),
                ),
                (
                    Value::String("name".into()),
                    Value::String(format!("User {i}").into()),
                ),
                (
                    Value::String("age".into()),
                    Value::Integer((20 + i % 50).into()),
                ),
                (
                    Value::String("email".into()),
                    Value::String(format!("user{}@example.com", i).into()),
                ),
                (Value::String("active".into()), Value::Boolean(i % 2 == 0)),
            ])
        })
        .collect();

    Value::Array(vec![
        Value::from(TermType::Insert as u64),
        Value::Array(vec![table_term, Value::Array(documents)]),
        Value::Map(vec![]),
    ])
}

fn create_nested_object_query() -> Value {
    let nested_obj = Value::Map(vec![
        (
            Value::String("street".into()),
            Value::String("123 Main St".into()),
        ),
        (
            Value::String("city".into()),
            Value::String("Anytown".into()),
        ),
        (
            Value::String("zipcode".into()),
            Value::String("12345".into()),
        ),
        (
            Value::String("coordinates".into()),
            Value::Map(vec![
                (Value::String("lat".into()), Value::F64(40.7128)),
                (Value::String("lng".into()), Value::F64(-74.0060)),
            ]),
        ),
    ]);

    let document = Value::Map(vec![
        (Value::String("id".into()), Value::String("user_1".into())),
        (
            Value::String("name".into()),
            Value::String("John Doe".into()),
        ),
        (Value::String("address".into()), nested_obj),
        (
            Value::String("tags".into()),
            Value::Array(vec![
                Value::String("developer".into()),
                Value::String("rust".into()),
                Value::String("database".into()),
            ]),
        ),
    ]);

    let table_term = Value::Array(vec![
        Value::from(TermType::Table as u64),
        Value::Array(vec![Value::String("users".into())]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::Insert as u64),
        Value::Array(vec![table_term, Value::Array(vec![document])]),
        Value::Map(vec![]),
    ])
}

fn create_get_query() -> Value {
    let table_term = Value::Array(vec![
        Value::from(TermType::Table as u64),
        Value::Array(vec![Value::String("users".into())]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::Get as u64),
        Value::Array(vec![table_term, Value::String("user_123".into())]),
        Value::Map(vec![]),
    ])
}

fn create_complex_expression_query() -> Value {
    // Create a deeply nested expression: ((age > 18) AND (age < 65)) OR (status == "premium")
    let age_field = Value::Array(vec![
        Value::from(TermType::GetField as u64),
        Value::Array(vec![Value::String("age".into())]),
        Value::Map(vec![]),
    ]);

    let eighteen = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::Integer(18.into())]),
        Value::Map(vec![]),
    ]);

    let sixty_five = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::Integer(65.into())]),
        Value::Map(vec![]),
    ]);

    let age_gt_18 = Value::Array(vec![
        Value::from(TermType::Gt as u64),
        Value::Array(vec![age_field.clone(), eighteen]),
        Value::Map(vec![]),
    ]);

    let age_lt_65 = Value::Array(vec![
        Value::from(TermType::Lt as u64),
        Value::Array(vec![age_field, sixty_five]),
        Value::Map(vec![]),
    ]);

    let age_range = Value::Array(vec![
        Value::from(TermType::And as u64),
        Value::Array(vec![age_gt_18, age_lt_65]),
        Value::Map(vec![]),
    ]);

    let status_field = Value::Array(vec![
        Value::from(TermType::GetField as u64),
        Value::Array(vec![Value::String("status".into())]),
        Value::Map(vec![]),
    ]);

    let premium_value = Value::Array(vec![
        Value::from(TermType::Datum as u64),
        Value::Array(vec![Value::String("premium".into())]),
        Value::Map(vec![]),
    ]);

    let status_premium = Value::Array(vec![
        Value::from(TermType::Eq as u64),
        Value::Array(vec![status_field, premium_value]),
        Value::Map(vec![]),
    ]);

    let final_expr = Value::Array(vec![
        Value::from(TermType::Or as u64),
        Value::Array(vec![age_range, status_premium]),
        Value::Map(vec![]),
    ]);

    Value::Array(vec![
        Value::from(TermType::Expr as u64),
        Value::Array(vec![final_expr]),
        Value::Map(vec![]),
    ])
}

fn bench_parser_simple_operations(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_database_create", |b| {
        let query = create_database_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });

    c.bench_function("parse_table_create", |b| {
        let query = create_table_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });

    c.bench_function("parse_get_operation", |b| {
        let query = create_get_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });
}

fn bench_parser_filter_operations(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_simple_filter", |b| {
        let query = create_simple_filter_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });

    c.bench_function("parse_complex_filter", |b| {
        let query = create_complex_filter_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });

    c.bench_function("parse_complex_expression", |b| {
        let query = create_complex_expression_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });
}

fn bench_parser_insert_operations(c: &mut Criterion) {
    let parser = Parser::new();
    let mut group = c.benchmark_group("parse_insert_operations");

    for size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("documents", size), size, |b, &size| {
            let query = create_insert_query(size);
            b.iter(|| {
                let result = parser.parse(black_box(&query));
                black_box(result).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_parser_nested_objects(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_nested_object", |b| {
        let query = create_nested_object_query();
        b.iter(|| {
            let result = parser.parse(black_box(&query));
            black_box(result).unwrap();
        });
    });
}

fn bench_parser_data_types(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_various_data_types", |b| {
        // Create a datum term with complex data types
        let complex_data = Value::Array(vec![
            Value::from(TermType::Datum as u64),
            Value::Array(vec![Value::Map(vec![
                (
                    Value::String("string".into()),
                    Value::String("test string".into()),
                ),
                (Value::String("integer".into()), Value::Integer(42.into())),
                (Value::String("float".into()), Value::F64(2.123)),
                (Value::String("boolean".into()), Value::Boolean(true)),
                (Value::String("null".into()), Value::Nil),
                (
                    Value::String("array".into()),
                    Value::Array(vec![
                        Value::Integer(1.into()),
                        Value::Integer(2.into()),
                        Value::Integer(3.into()),
                    ]),
                ),
                (
                    Value::String("nested_object".into()),
                    Value::Map(vec![(
                        Value::String("inner_key".into()),
                        Value::String("inner_value".into()),
                    )]),
                ),
            ])]),
            Value::Map(vec![]),
        ]);

        b.iter(|| {
            let result = parser.parse(black_box(&complex_data));
            black_box(result).unwrap();
        });
    });
}

fn bench_parser_opt_args(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_with_opt_args", |b| {
        let table_term = Value::Array(vec![
            Value::from(TermType::Table as u64),
            Value::Array(vec![Value::String("users".into())]),
            Value::Map(vec![
                (
                    Value::String("start_key".into()),
                    Value::String("user_100".into()),
                ),
                (
                    Value::String("batch_size".into()),
                    Value::Integer(50.into()),
                ),
                (Value::String("timeout".into()), Value::Integer(5000.into())),
                (
                    Value::String("include_metadata".into()),
                    Value::Boolean(true),
                ),
            ]),
        ]);

        b.iter(|| {
            let result = parser.parse(black_box(&table_term));
            black_box(result).unwrap();
        });
    });
}

fn bench_parser_error_conditions(c: &mut Criterion) {
    let parser = Parser::new();

    c.bench_function("parse_invalid_structure", |b| {
        let invalid_query = Value::String("not an array".into());
        b.iter(|| {
            let result = parser.parse(black_box(&invalid_query));
            black_box(result).unwrap_err();
        });
    });

    c.bench_function("parse_wrong_term_type", |b| {
        let invalid_query = Value::Array(vec![
            Value::from(999u64), // Invalid term type
            Value::Array(vec![]),
            Value::Map(vec![]),
        ]);
        b.iter(|| {
            let result = parser.parse(black_box(&invalid_query));
            black_box(result).unwrap_err();
        });
    });
}

criterion_group!(
    benches,
    bench_parser_simple_operations,
    bench_parser_filter_operations,
    bench_parser_insert_operations,
    bench_parser_nested_objects,
    bench_parser_data_types,
    bench_parser_opt_args,
    bench_parser_error_conditions
);
criterion_main!(benches);
