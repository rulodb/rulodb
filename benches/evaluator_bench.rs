use criterion::{Criterion, criterion_group, criterion_main};
use rulodb::ast::{BinOp, Datum, Expr};
use rulodb::evaluator::Evaluator;
use rulodb::planner::PlanNode;
use rulodb::storage::{Config, DefaultStorage};
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

// Helper to create a test storage backend
fn create_test_storage() -> (
    Arc<dyn rulodb::storage::StorageBackend + Send + Sync>,
    TempDir,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config {
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Config::default()
    };
    let storage = DefaultStorage::open(&config).unwrap();
    (Arc::new(storage), temp_dir)
}

fn bench_evaluator_constants(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("eval_constant_string", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Constant(Datum::String("test".to_string()));
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_constant_integer", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Constant(Datum::Integer(42));
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_constant_boolean", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Constant(Datum::Bool(true));
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });
}

fn bench_evaluator_expressions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("eval_simple_comparison", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Eval {
                    expr: Expr::BinaryOp {
                        op: BinOp::Eq,
                        left: Box::new(Expr::Constant(Datum::Integer(10))),
                        right: Box::new(Expr::Constant(Datum::Integer(10))),
                    },
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_boolean_logic", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Eval {
                    expr: Expr::BinaryOp {
                        op: BinOp::And,
                        left: Box::new(Expr::Constant(Datum::Bool(true))),
                        right: Box::new(Expr::Constant(Datum::Bool(false))),
                    },
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_comparison", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Eval {
                    expr: Expr::BinaryOp {
                        op: BinOp::Lt,
                        left: Box::new(Expr::Constant(Datum::Integer(5))),
                        right: Box::new(Expr::Constant(Datum::Integer(10))),
                    },
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_nested_expression", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::Eval {
                    expr: Expr::BinaryOp {
                        op: BinOp::Or,
                        left: Box::new(Expr::BinaryOp {
                            op: BinOp::Eq,
                            left: Box::new(Expr::Constant(Datum::Integer(1))),
                            right: Box::new(Expr::Constant(Datum::Integer(1))),
                        }),
                        right: Box::new(Expr::BinaryOp {
                            op: BinOp::Gt,
                            left: Box::new(Expr::Constant(Datum::Integer(5))),
                            right: Box::new(Expr::Constant(Datum::Integer(3))),
                        }),
                    },
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });
}

fn bench_evaluator_database_ops(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("eval_create_database", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::CreateDatabase {
                    name: format!("testdb_{}", fastrand::u32(..)),
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_list_databases", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::ListDatabases;
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });

    c.bench_function("eval_create_table", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage();
                storage.create_database("testdb").await.unwrap();
                let mut evaluator = Evaluator::new(storage);
                let plan = PlanNode::CreateTable {
                    db: Some("testdb".to_string()),
                    name: format!("table_{}", fastrand::u32(..)),
                };
                let result = evaluator.eval(black_box(&plan)).await;
                black_box(result).unwrap();
            });
        });
    });
}

// Note: More complex operations like table scans, filters, and inserts
// are disabled due to segmentation faults in the RocksDB storage layer.
// See benches/README.md for details on this known issue.

criterion_group!(
    benches,
    bench_evaluator_constants,
    bench_evaluator_expressions,
    bench_evaluator_database_ops
);
criterion_main!(benches);
