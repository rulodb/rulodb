use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rulodb::ast::{BinOp, Datum, Expr, Term, UnOp};
use rulodb::planner::{PlanNode, Planner};
use std::collections::BTreeMap;
use std::hint::black_box;

fn create_simple_table_term() -> Term {
    Term::Table {
        db: Some("test_db".to_string()),
        name: "users".to_string(),
        opt_args: BTreeMap::new(),
    }
}

fn create_database_term() -> Term {
    Term::Database {
        name: "test_db".to_string(),
    }
}

fn create_simple_filter_term() -> Term {
    let table = create_simple_table_term();
    let predicate = Term::Expr(Expr::BinaryOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field {
            name: "status".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::String("active".to_string()))),
    });

    Term::Filter {
        source: Box::new(table),
        predicate: Box::new(predicate),
        opt_args: BTreeMap::new(),
    }
}

fn create_complex_filter_term() -> Term {
    let table = create_simple_table_term();

    // Create (age > 25 AND age < 65) OR status == "premium"
    let age_gt_25 = Expr::BinaryOp {
        op: BinOp::Gt,
        left: Box::new(Expr::Field {
            name: "age".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::Integer(25))),
    };

    let age_lt_65 = Expr::BinaryOp {
        op: BinOp::Lt,
        left: Box::new(Expr::Field {
            name: "age".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::Integer(65))),
    };

    let age_range = Expr::BinaryOp {
        op: BinOp::And,
        left: Box::new(age_gt_25),
        right: Box::new(age_lt_65),
    };

    let status_premium = Expr::BinaryOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field {
            name: "status".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::String("premium".to_string()))),
    };

    let complex_predicate = Expr::BinaryOp {
        op: BinOp::Or,
        left: Box::new(age_range),
        right: Box::new(status_premium),
    };

    Term::Filter {
        source: Box::new(table),
        predicate: Box::new(Term::Expr(complex_predicate)),
        opt_args: BTreeMap::new(),
    }
}

fn create_nested_filter_term() -> Term {
    let inner_table = create_simple_table_term();
    let inner_predicate = Term::Expr(Expr::BinaryOp {
        op: BinOp::Gt,
        left: Box::new(Expr::Field {
            name: "age".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::Integer(18))),
    });

    let inner_filter = Term::Filter {
        source: Box::new(inner_table),
        predicate: Box::new(inner_predicate),
        opt_args: BTreeMap::new(),
    };

    let outer_predicate = Term::Expr(Expr::BinaryOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field {
            name: "status".to_string(),
            separator: None,
        }),
        right: Box::new(Expr::Constant(Datum::String("active".to_string()))),
    });

    Term::Filter {
        source: Box::new(inner_filter),
        predicate: Box::new(outer_predicate),
        opt_args: BTreeMap::new(),
    }
}

fn create_insert_term(num_docs: usize) -> Term {
    let table = create_simple_table_term();
    let documents: Vec<Datum> = (0..num_docs)
        .map(|i| {
            let mut obj = BTreeMap::new();
            obj.insert("id".to_string(), Datum::String(format!("user_{i}")));
            obj.insert("name".to_string(), Datum::String(format!("User {i}")));
            obj.insert("age".to_string(), Datum::Integer(20 + i as i64 % 50));
            obj.insert(
                "email".to_string(),
                Datum::String(format!("user{}@example.com", i)),
            );
            obj.insert("active".to_string(), Datum::Bool(i % 2 == 0));
            Datum::Object(obj)
        })
        .collect();

    Term::Insert {
        table: Box::new(table),
        documents,
        opt_args: BTreeMap::new(),
    }
}

fn create_get_term() -> Term {
    let table = create_simple_table_term();
    Term::Get {
        table: Box::new(table),
        key: Datum::String("user_123".to_string()),
        opt_args: BTreeMap::new(),
    }
}

fn create_delete_term() -> Term {
    let filter = create_simple_filter_term();
    Term::Delete {
        source: Box::new(filter),
        opt_args: BTreeMap::new(),
    }
}

fn create_expression_term(depth: usize) -> Term {
    fn create_nested_expr(current_depth: usize, max_depth: usize) -> Expr {
        if current_depth >= max_depth {
            return Expr::Field {
                name: "value".to_string(),
                separator: None,
            };
        }

        Expr::BinaryOp {
            op: if current_depth % 2 == 0 {
                BinOp::And
            } else {
                BinOp::Or
            },
            left: Box::new(create_nested_expr(current_depth + 1, max_depth)),
            right: Box::new(Expr::BinaryOp {
                op: BinOp::Gt,
                left: Box::new(Expr::Field {
                    name: format!("field_{current_depth}"),
                    separator: None,
                }),
                right: Box::new(Expr::Constant(Datum::Integer(current_depth as i64))),
            }),
        }
    }

    Term::Expr(create_nested_expr(0, depth))
}

fn bench_planner_simple_operations(c: &mut Criterion) {
    let mut planner = Planner::new();

    c.bench_function("plan_database_operation", |b| {
        let term = create_database_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });

    c.bench_function("plan_table_scan", |b| {
        let term = create_simple_table_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });

    c.bench_function("plan_get_operation", |b| {
        let term = create_get_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });

    c.bench_function("plan_delete_operation", |b| {
        let term = create_delete_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });
}

fn bench_planner_filter_operations(c: &mut Criterion) {
    let mut planner = Planner::new();

    c.bench_function("plan_simple_filter", |b| {
        let term = create_simple_filter_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });

    c.bench_function("plan_complex_filter", |b| {
        let term = create_complex_filter_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });

    c.bench_function("plan_nested_filter", |b| {
        let term = create_nested_filter_term();
        b.iter(|| {
            let result = planner.plan(black_box(&term));
            black_box(result).unwrap();
        });
    });
}

fn bench_planner_insert_operations(c: &mut Criterion) {
    let mut planner = Planner::new();
    let mut group = c.benchmark_group("plan_insert_operations");

    for size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("documents", size), size, |b, &size| {
            let term = create_insert_term(size);
            b.iter(|| {
                let result = planner.plan(black_box(&term));
                black_box(result).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_expression_simplification(c: &mut Criterion) {
    c.bench_function("simplify_constant_folding", |b| {
        let expr = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Constant(Datum::Bool(true))),
            right: Box::new(Expr::Constant(Datum::Bool(false))),
        };

        b.iter(|| {
            let result = Planner::simplify_expr(black_box(expr.clone()));
            black_box(result);
        });
    });

    c.bench_function("simplify_identity_operations", |b| {
        let expr = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Field {
                name: "test".to_string(),
                separator: None,
            }),
            right: Box::new(Expr::Constant(Datum::Bool(true))),
        };

        b.iter(|| {
            let result = Planner::simplify_expr(black_box(expr.clone()));
            black_box(result);
        });
    });

    c.bench_function("simplify_double_negation", |b| {
        let expr = Expr::UnaryOp {
            op: UnOp::Not,
            expr: Box::new(Expr::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(Expr::Field {
                    name: "active".to_string(),
                    separator: None,
                }),
            }),
        };

        b.iter(|| {
            let result = Planner::simplify_expr(black_box(expr.clone()));
            black_box(result);
        });
    });

    c.bench_function("simplify_complex_expression", |b| {
        let complex_expr = Expr::BinaryOp {
            op: BinOp::Or,
            left: Box::new(Expr::BinaryOp {
                op: BinOp::And,
                left: Box::new(Expr::Constant(Datum::Bool(true))),
                right: Box::new(Expr::Field {
                    name: "age".to_string(),
                    separator: None,
                }),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field {
                    name: "status".to_string(),
                    separator: None,
                }),
                right: Box::new(Expr::Constant(Datum::String("active".to_string()))),
            }),
        };

        b.iter(|| {
            let result = Planner::simplify_expr(black_box(complex_expr.clone()));
            black_box(result);
        });
    });
}

fn bench_expression_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("expression_complexity");

    for depth in [1, 3, 5, 10].iter() {
        group.bench_with_input(BenchmarkId::new("depth", depth), depth, |b, &depth| {
            let term = create_expression_term(depth);
            let mut planner = Planner::new();
            b.iter(|| {
                let result = planner.plan(black_box(&term));
                black_box(result).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_optimization_passes(c: &mut Criterion) {
    let mut planner = Planner::new();

    c.bench_function("optimize_simple_plan", |b| {
        let term = create_simple_filter_term();
        let plan = planner.plan(&term).unwrap();

        b.iter(|| {
            let result = planner.optimize(black_box(plan.clone()));
            black_box(result);
        });
    });

    c.bench_function("optimize_complex_plan", |b| {
        let term = create_nested_filter_term();
        let plan = planner.plan(&term).unwrap();

        b.iter(|| {
            let result = planner.optimize(black_box(plan.clone()));
            black_box(result);
        });
    });

    c.bench_function("optimize_filter_with_constant_true", |b| {
        let _table = create_simple_table_term();
        let plan = PlanNode::Filter {
            source: Box::new(PlanNode::ScanTable {
                db: Some("test_db".to_string()),
                name: "users".to_string(),
                opt_args: BTreeMap::new(),
            }),
            predicate: Expr::Constant(Datum::Bool(true)),
            opt_args: BTreeMap::new(),
        };

        b.iter(|| {
            let result = planner.optimize(black_box(plan.clone()));
            black_box(result);
        });
    });

    c.bench_function("optimize_filter_with_constant_false", |b| {
        let plan = PlanNode::Filter {
            source: Box::new(PlanNode::ScanTable {
                db: Some("test_db".to_string()),
                name: "users".to_string(),
                opt_args: BTreeMap::new(),
            }),
            predicate: Expr::Constant(Datum::Bool(false)),
            opt_args: BTreeMap::new(),
        };

        b.iter(|| {
            let result = planner.optimize(black_box(plan.clone()));
            black_box(result);
        });
    });

    c.bench_function("optimize_empty_insert", |b| {
        let plan = PlanNode::Insert {
            table: Box::new(PlanNode::ScanTable {
                db: Some("test_db".to_string()),
                name: "users".to_string(),
                opt_args: BTreeMap::new(),
            }),
            documents: vec![],
            opt_args: BTreeMap::new(),
        };

        b.iter(|| {
            let result = planner.optimize(black_box(plan.clone()));
            black_box(result);
        });
    });
}

fn bench_explain_plans(c: &mut Criterion) {
    let planner = Planner::new();

    c.bench_function("explain_simple_plan", |b| {
        let plan = PlanNode::ScanTable {
            db: Some("test_db".to_string()),
            name: "users".to_string(),
            opt_args: BTreeMap::new(),
        };

        b.iter(|| {
            let result = planner.explain(black_box(&plan), 0);
            black_box(result);
        });
    });

    c.bench_function("explain_complex_plan", |b| {
        let plan = PlanNode::Filter {
            source: Box::new(PlanNode::Filter {
                source: Box::new(PlanNode::ScanTable {
                    db: Some("test_db".to_string()),
                    name: "users".to_string(),
                    opt_args: BTreeMap::new(),
                }),
                predicate: Expr::BinaryOp {
                    op: BinOp::Gt,
                    left: Box::new(Expr::Field {
                        name: "age".to_string(),
                        separator: None,
                    }),
                    right: Box::new(Expr::Constant(Datum::Integer(18))),
                },
                opt_args: BTreeMap::new(),
            }),
            predicate: Expr::BinaryOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field {
                    name: "status".to_string(),
                    separator: None,
                }),
                right: Box::new(Expr::Constant(Datum::String("active".to_string()))),
            },
            opt_args: BTreeMap::new(),
        };

        b.iter(|| {
            let result = planner.explain(black_box(&plan), 0);
            black_box(result);
        });
    });
}

fn bench_plan_end_to_end(c: &mut Criterion) {
    c.bench_function("plan_and_optimize_end_to_end", |b| {
        let term = create_complex_filter_term();

        b.iter(|| {
            let mut planner = Planner::new();
            let plan = planner.plan(black_box(&term)).unwrap();
            let optimized = planner.optimize(plan);
            black_box(optimized);
        });
    });

    c.bench_function("plan_optimize_explain_end_to_end", |b| {
        let term = create_nested_filter_term();

        b.iter(|| {
            let mut planner = Planner::new();
            let plan = planner.plan(black_box(&term)).unwrap();
            let optimized = planner.optimize(plan);
            let explanation = planner.explain(&optimized, 0);
            black_box(explanation);
        });
    });
}

criterion_group!(
    benches,
    bench_planner_simple_operations,
    bench_planner_filter_operations,
    bench_planner_insert_operations,
    bench_expression_simplification,
    bench_expression_complexity,
    bench_optimization_passes,
    bench_explain_plans,
    bench_plan_end_to_end
);
criterion_main!(benches);
