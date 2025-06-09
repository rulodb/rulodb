use crate::ast::{BinOp, Datum, Expr, OptArgs, Term, UnOp};
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Debug)]
pub enum PlanError {
    UnsupportedTerm(Term),
    InvalidPredicate(Term),
    InvalidGetTerm(Term),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedTerm(term) => {
                write!(f, "Unsupported term encountered during planning: {term:?}")
            }
            Self::InvalidPredicate(term) => {
                write!(f, "Filter predicate is not a boolean expression: {term:?}")
            }
            Self::InvalidGetTerm(term) => write!(f, "Get term missing table or key: {term:?}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Planner‐side representation of a compiled/optimized node.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    SelectDatabase {
        name: String,
    },
    CreateDatabase {
        name: String,
    },
    DropDatabase {
        name: String,
    },
    ListDatabases,
    ScanTable {
        db: Option<String>,
        name: String,
        opt_args: OptArgs,
    },
    CreateTable {
        db: Option<String>,
        name: String,
    },
    DropTable {
        db: Option<String>,
        name: String,
    },
    ListTables {
        db: Option<String>,
    },
    GetByKey {
        db: Option<String>,
        table: String,
        key: Datum,
        opt_args: OptArgs,
    },
    Filter {
        source: Box<PlanNode>,
        predicate: Expr,
        opt_args: OptArgs,
    },
    Insert {
        table: Box<PlanNode>,
        documents: Vec<Datum>,
        opt_args: OptArgs,
    },
    Delete {
        source: Box<PlanNode>,
        opt_args: OptArgs,
    },
    Eval {
        expr: Expr,
    },
    Constant(Datum),
}

pub struct Planner {
    expr_cache: HashMap<String, Expr>,
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

impl Planner {
    pub fn new() -> Self {
        Self {
            expr_cache: HashMap::new(),
        }
    }

    pub fn plan(&mut self, term: &Term) -> Result<PlanNode, PlanError> {
        match term {
            Term::Expr(e) => Ok(PlanNode::Eval { expr: e.clone() }),

            Term::Database { name } => Ok(PlanNode::SelectDatabase { name: name.clone() }),

            Term::DatabaseCreate { name } => Ok(PlanNode::CreateDatabase { name: name.clone() }),

            Term::DatabaseDrop { name } => Ok(PlanNode::DropDatabase { name: name.clone() }),

            Term::DatabaseList => Ok(PlanNode::ListDatabases),

            Term::Table { db, name, opt_args } => Ok(PlanNode::ScanTable {
                db: db.clone(),
                name: name.clone(),
                opt_args: opt_args.clone(),
            }),

            Term::TableCreate { db, name } => Ok(PlanNode::CreateTable {
                db: db.clone(),
                name: name.clone(),
            }),

            Term::TableDrop { db, name } => Ok(PlanNode::DropTable {
                db: db.clone(),
                name: name.clone(),
            }),

            Term::TableList { db } => Ok(PlanNode::ListTables { db: db.clone() }),

            Term::Get {
                table,
                key,
                opt_args,
            } => {
                let (db, table) = match &**table {
                    Term::Table { db, name, .. } => (db.clone(), name.clone()),
                    _ => (None, format!("{:?}", self.plan(table)?)),
                };

                let key = match key {
                    Datum::String(_) | Datum::Integer(_) => key.clone(),
                    other => {
                        return Err(PlanError::InvalidGetTerm(Term::Datum(other.clone())));
                    }
                };

                Ok(PlanNode::GetByKey {
                    db,
                    table,
                    key,
                    opt_args: opt_args.clone(),
                })
            }

            Term::Filter {
                source,
                predicate,
                opt_args,
            } => {
                if let Term::Expr(e) = predicate.as_ref() {
                    let optimized_source = self.plan(source)?;
                    let simplified_predicate = self.simplify_expr_cached(e.clone());

                    if let PlanNode::ScanTable {
                        db,
                        name,
                        opt_args: table_opts,
                    } = &optimized_source
                    {
                        if self.can_push_down_filter(&simplified_predicate) {
                            let mut merged_opts = table_opts.clone();
                            merged_opts.extend(opt_args.clone());
                            return Ok(PlanNode::ScanTable {
                                db: db.clone(),
                                name: name.clone(),
                                opt_args: merged_opts,
                            });
                        }
                    }

                    Ok(PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate: simplified_predicate,
                        opt_args: opt_args.clone(),
                    })
                } else {
                    Err(PlanError::InvalidPredicate(predicate.as_ref().clone()))
                }
            }

            Term::Insert {
                table,
                documents,
                opt_args,
            } => Ok(PlanNode::Insert {
                table: Box::new(self.plan(table)?),
                documents: documents.clone(),
                opt_args: opt_args.clone(),
            }),

            Term::Delete { source, opt_args } => Ok(PlanNode::Delete {
                source: Box::new(self.plan(source)?),
                opt_args: opt_args.clone(),
            }),

            Term::Datum(_) => Err(PlanError::UnsupportedTerm(term.clone())),
        }
    }

    #[allow(clippy::unused_self)]
    fn can_push_down_filter(&self, predicate: &Expr) -> bool {
        match predicate {
            Expr::Field { .. } => true,
            Expr::BinaryOp { op, left, right } => {
                matches!(
                    op,
                    BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
                ) && matches!(left.as_ref(), Expr::Field { .. })
                    && matches!(right.as_ref(), Expr::Constant(_))
            }
            _ => false,
        }
    }

    fn simplify_expr_cached(&mut self, expr: Expr) -> Expr {
        let expr_key = format!("{expr:?}");
        if let Some(cached) = self.expr_cache.get(&expr_key) {
            return cached.clone();
        }

        let simplified = Self::simplify_expr(expr);
        self.expr_cache.insert(expr_key, simplified.clone());
        simplified
    }

    pub fn simplify_expr(expr: Expr) -> Expr {
        match expr {
            Expr::Constant(_) | Expr::Field { .. } => expr,

            Expr::BinaryOp { op, left, right } => {
                if let (Expr::Constant(l), Expr::Constant(r)) = (left.as_ref(), right.as_ref()) {
                    if let Some(result) = Self::fold_constants(&op, l, r) {
                        return Expr::Constant(result);
                    }
                }

                let left_simplified = Self::simplify_expr(*left);
                let right_simplified = Self::simplify_expr(*right);

                match (&op, &left_simplified, &right_simplified) {
                    (BinOp::And, Expr::Constant(Datum::Bool(false)), _)
                    | (BinOp::And, _, Expr::Constant(Datum::Bool(false))) => {
                        Expr::Constant(Datum::Bool(false))
                    }

                    (BinOp::Or, Expr::Constant(Datum::Bool(true)), _)
                    | (BinOp::Or, _, Expr::Constant(Datum::Bool(true))) => {
                        Expr::Constant(Datum::Bool(true))
                    }

                    (BinOp::And, Expr::Constant(Datum::Bool(true)), r)
                    | (BinOp::Or, Expr::Constant(Datum::Bool(false)), r) => r.clone(),

                    (BinOp::And, l, Expr::Constant(Datum::Bool(true)))
                    | (BinOp::Or, l, Expr::Constant(Datum::Bool(false))) => l.clone(),

                    (BinOp::Eq, l, r) if l == r => Expr::Constant(Datum::Bool(true)),
                    (BinOp::Ne, l, r) if l == r => Expr::Constant(Datum::Bool(false)),

                    _ => Expr::BinaryOp {
                        op,
                        left: Box::new(left_simplified),
                        right: Box::new(right_simplified),
                    },
                }
            }

            Expr::UnaryOp { op, expr } => {
                let simplified_expr = Self::simplify_expr(*expr);
                match (&op, &simplified_expr) {
                    (UnOp::Not, Expr::Constant(Datum::Bool(b))) => Expr::Constant(Datum::Bool(!*b)),
                    (
                        UnOp::Not,
                        Expr::UnaryOp {
                            op: UnOp::Not,
                            expr,
                        },
                    ) => *expr.clone(),
                    _ => Expr::UnaryOp {
                        op,
                        expr: Box::new(simplified_expr),
                    },
                }
            }
        }
    }

    fn fold_constants(op: &BinOp, left: &Datum, right: &Datum) -> Option<Datum> {
        match (op, left, right) {
            (BinOp::Eq, l, r) => Some(Datum::Bool(l == r)),
            (BinOp::Ne, l, r) => Some(Datum::Bool(l != r)),
            (BinOp::And, Datum::Bool(l), Datum::Bool(r)) => Some(Datum::Bool(*l && *r)),
            (BinOp::Or, Datum::Bool(l), Datum::Bool(r)) => Some(Datum::Bool(*l || *r)),

            (BinOp::Lt, Datum::Integer(l), Datum::Integer(r)) => Some(Datum::Bool(l < r)),
            (BinOp::Le, Datum::Integer(l), Datum::Integer(r)) => Some(Datum::Bool(l <= r)),
            (BinOp::Gt, Datum::Integer(l), Datum::Integer(r)) => Some(Datum::Bool(l > r)),
            (BinOp::Ge, Datum::Integer(l), Datum::Integer(r)) => Some(Datum::Bool(l >= r)),

            _ => None,
        }
    }

    pub fn optimize(&mut self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Filter {
                source,
                predicate,
                opt_args,
            } => {
                let optimized_source = self.optimize(*source);
                let simplified_pred = self.simplify_expr_cached(predicate);

                match &simplified_pred {
                    Expr::Constant(Datum::Bool(true)) => optimized_source,
                    Expr::Constant(Datum::Bool(false)) => PlanNode::Constant(Datum::Array(vec![])),
                    _ => {
                        if let PlanNode::Filter {
                            source: inner_source,
                            predicate: inner_pred,
                            opt_args: inner_opts,
                        } = optimized_source
                        {
                            let combined_predicate = Expr::BinaryOp {
                                op: BinOp::And,
                                left: Box::new(inner_pred),
                                right: Box::new(simplified_pred),
                            };
                            let final_predicate = self.simplify_expr_cached(combined_predicate);

                            let mut merged_opts = inner_opts;
                            merged_opts.extend(opt_args);

                            PlanNode::Filter {
                                source: inner_source,
                                predicate: final_predicate,
                                opt_args: merged_opts,
                            }
                        } else {
                            PlanNode::Filter {
                                source: Box::new(optimized_source),
                                predicate: simplified_pred,
                                opt_args,
                            }
                        }
                    }
                }
            }

            PlanNode::Insert {
                table,
                documents,
                opt_args,
            } => {
                if documents.is_empty() {
                    return PlanNode::Constant(Datum::Array(vec![]));
                }

                PlanNode::Insert {
                    table: Box::new(self.optimize(*table)),
                    documents,
                    opt_args,
                }
            }

            PlanNode::Delete { source, opt_args } => PlanNode::Delete {
                source: Box::new(self.optimize(*source)),
                opt_args,
            },

            PlanNode::GetByKey {
                db,
                table,
                key,
                opt_args,
            } => PlanNode::GetByKey {
                db,
                table,
                key,
                opt_args,
            },

            PlanNode::Eval { expr } => {
                let simplified = self.simplify_expr_cached(expr);
                if let Expr::Constant(datum) = simplified {
                    PlanNode::Constant(datum)
                } else {
                    PlanNode::Eval { expr: simplified }
                }
            }

            plan_node => plan_node,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn explain(&self, plan: &PlanNode, indent: usize) -> String {
        use std::fmt::Write;
        let mut result = String::with_capacity(256); // Pre-allocate reasonable capacity
        let pad = "  ".repeat(indent);

        match plan {
            PlanNode::SelectDatabase { name } => {
                write!(result, "{pad}SelectDatabase: {name}").unwrap();
            }

            PlanNode::CreateDatabase { name } => {
                write!(result, "{pad}CreateDatabase: {name}").unwrap();
            }

            PlanNode::DropDatabase { name } => {
                write!(result, "{pad}DropDatabase: {name}").unwrap();
            }

            PlanNode::ListDatabases => {
                write!(result, "{pad}ListDatabases").unwrap();
            }

            PlanNode::ScanTable { db, name, opt_args } => {
                let database = self.explain(
                    &PlanNode::SelectDatabase {
                        name: db.as_deref().unwrap_or("").to_string(),
                    },
                    indent + 1,
                );

                let paginator = Self::format_pagination_info(opt_args);
                write!(result, "{pad}ScanTable: {name} ({paginator})\n{database}").unwrap();
            }

            PlanNode::CreateTable { name, .. } => {
                write!(result, "{pad}CreateTable: {name}").unwrap();
            }

            PlanNode::DropTable { name, .. } => {
                write!(result, "{pad}DropTable: {name}").unwrap();
            }

            PlanNode::ListTables { .. } => {
                write!(result, "{pad}ListTables").unwrap();
            }

            PlanNode::GetByKey { table, key, .. } => {
                write!(result, "{pad}GetByKey: table={table}, key={key:?}").unwrap();
            }

            PlanNode::Filter {
                source, predicate, ..
            } => {
                write!(
                    result,
                    "{pad}Filter: {predicate}\n{}",
                    self.explain(source, indent + 1)
                )
                .unwrap();
            }

            PlanNode::Insert {
                table, documents, ..
            } => {
                write!(
                    result,
                    "{}Insert {} docs\n{}",
                    pad,
                    documents.len(),
                    self.explain(table, indent + 1)
                )
                .unwrap();
            }

            PlanNode::Delete { source, .. } => {
                write!(
                    result,
                    "{}Delete\n{}",
                    pad,
                    self.explain(source, indent + 1)
                )
                .unwrap();
            }

            PlanNode::Eval { expr } => {
                write!(result, "{pad}Eval: {expr}").unwrap();
            }

            PlanNode::Constant(d) => {
                write!(result, "{pad}Constant: {d:?}").unwrap();
            }
        }

        result
    }

    fn format_pagination_info(opt_args: &OptArgs) -> Cow<'static, str> {
        match (opt_args.get("start_key"), opt_args.get("batch_size")) {
            (None, Some(Term::Datum(Datum::Integer(batch_size)))) => {
                Cow::Owned(format!("start_key=none, batch_size={batch_size}"))
            }
            (Some(Term::Datum(Datum::String(start_key))), None) => {
                Cow::Owned(format!("start_key={start_key}, batch_size=none"))
            }
            (
                Some(Term::Datum(Datum::String(start_key))),
                Some(Term::Datum(Datum::Integer(batch_size))),
            ) => Cow::Owned(format!("start_key={start_key}, batch_size={batch_size}")),
            _ => Cow::Borrowed("start_key=none, batch_size=none"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_test_table() -> Term {
        Term::Table {
            db: Some("test_db".to_string()),
            name: "users".to_string(),
            opt_args: BTreeMap::new(),
        }
    }

    #[test]
    fn test_expression_simplification() {
        let expr = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Constant(Datum::Bool(true))),
            right: Box::new(Expr::Constant(Datum::Bool(false))),
        };
        let simplified = Planner::simplify_expr(expr);
        assert_eq!(simplified, Expr::Constant(Datum::Bool(false)));

        let expr = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Field {
                name: "age".to_string(),
                separator: None,
            }),
            right: Box::new(Expr::Constant(Datum::Bool(true))),
        };
        let simplified = Planner::simplify_expr(expr);
        assert_eq!(
            simplified,
            Expr::Field {
                name: "age".to_string(),
                separator: None
            }
        );

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
        let simplified = Planner::simplify_expr(expr);
        assert_eq!(
            simplified,
            Expr::Field {
                name: "active".to_string(),
                separator: None
            }
        );
    }

    #[test]
    fn test_constant_folding() {
        assert_eq!(
            Planner::fold_constants(&BinOp::Lt, &Datum::Integer(5), &Datum::Integer(10)),
            Some(Datum::Bool(true))
        );
        assert_eq!(
            Planner::fold_constants(&BinOp::Gt, &Datum::Integer(5), &Datum::Integer(10)),
            Some(Datum::Bool(false))
        );

        assert_eq!(
            Planner::fold_constants(
                &BinOp::Eq,
                &Datum::String("test".to_string()),
                &Datum::String("test".to_string())
            ),
            Some(Datum::Bool(true))
        );
        assert_eq!(
            Planner::fold_constants(
                &BinOp::Ne,
                &Datum::String("test".to_string()),
                &Datum::String("other".to_string())
            ),
            Some(Datum::Bool(true))
        );
    }

    #[test]
    fn test_filter_predicate_pushdown() {
        let planner = Planner::new();

        let predicate = Expr::BinaryOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field {
                name: "status".to_string(),
                separator: None,
            }),
            right: Box::new(Expr::Constant(Datum::String("active".to_string()))),
        };

        assert!(planner.can_push_down_filter(&predicate));

        let complex_predicate = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Field {
                name: "age".to_string(),
                separator: None,
            }),
            right: Box::new(Expr::Field {
                name: "score".to_string(),
                separator: None,
            }),
        };

        assert!(!planner.can_push_down_filter(&complex_predicate));
    }

    #[test]
    fn test_filter_optimization() {
        let mut planner = Planner::new();

        let table = create_test_table();
        let source = planner.plan(&table).unwrap();

        let true_filter = PlanNode::Filter {
            source: Box::new(source.clone()),
            predicate: Expr::Constant(Datum::Bool(true)),
            opt_args: BTreeMap::new(),
        };

        let optimized = planner.optimize(true_filter);
        assert!(matches!(optimized, PlanNode::ScanTable { .. }));

        let false_filter = PlanNode::Filter {
            source: Box::new(source),
            predicate: Expr::Constant(Datum::Bool(false)),
            opt_args: BTreeMap::new(),
        };

        let optimized = planner.optimize(false_filter);
        assert!(matches!(optimized, PlanNode::Constant(Datum::Array(_))));
    }

    #[test]
    fn test_consecutive_filter_merging() {
        let mut planner = Planner::new();

        let table = create_test_table();
        let source = planner.plan(&table).unwrap();

        let inner_filter = PlanNode::Filter {
            source: Box::new(source),
            predicate: Expr::Field {
                name: "age".to_string(),
                separator: None,
            },
            opt_args: BTreeMap::new(),
        };

        let outer_filter = PlanNode::Filter {
            source: Box::new(inner_filter),
            predicate: Expr::Field {
                name: "status".to_string(),
                separator: None,
            },
            opt_args: BTreeMap::new(),
        };

        let optimized = planner.optimize(outer_filter);

        if let PlanNode::Filter { predicate, .. } = optimized {
            assert!(matches!(predicate, Expr::BinaryOp { op: BinOp::And, .. }));
        } else {
            panic!("Expected merged filter node");
        }
    }

    #[test]
    fn test_empty_insert_optimization() {
        let mut planner = Planner::new();

        let table = create_test_table();
        let table_plan = planner.plan(&table).unwrap();

        let empty_insert = PlanNode::Insert {
            table: Box::new(table_plan),
            documents: vec![],
            opt_args: BTreeMap::new(),
        };

        let optimized = planner.optimize(empty_insert);
        assert!(matches!(optimized, PlanNode::Constant(Datum::Array(_))));
    }

    #[test]
    fn test_expression_caching() {
        let mut planner = Planner::new();

        let expr = Expr::BinaryOp {
            op: BinOp::And,
            left: Box::new(Expr::Constant(Datum::Bool(true))),
            right: Box::new(Expr::Field {
                name: "test".to_string(),
                separator: None,
            }),
        };

        let result1 = planner.simplify_expr_cached(expr.clone());

        let result2 = planner.simplify_expr_cached(expr);

        assert_eq!(result1, result2);
        assert_eq!(
            result1,
            Expr::Field {
                name: "test".to_string(),
                separator: None
            }
        );

        assert!(!planner.expr_cache.is_empty());
    }

    #[test]
    fn test_eval_node_optimization() {
        let mut planner = Planner::new();

        let const_expr = Expr::BinaryOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Constant(Datum::Integer(42))),
            right: Box::new(Expr::Constant(Datum::Integer(42))),
        };

        let eval_node = PlanNode::Eval { expr: const_expr };
        let optimized = planner.optimize(eval_node);

        assert!(matches!(optimized, PlanNode::Constant(Datum::Bool(true))));
    }

    #[test]
    fn test_pagination_info_formatting() {
        let mut opt_args = BTreeMap::new();

        opt_args.insert("batch_size".to_string(), Term::Datum(Datum::Integer(100)));
        let result = Planner::format_pagination_info(&opt_args);
        assert_eq!(result, "start_key=none, batch_size=100");

        opt_args.clear();
        opt_args.insert(
            "start_key".to_string(),
            Term::Datum(Datum::String("key123".to_string())),
        );
        let result = Planner::format_pagination_info(&opt_args);
        assert_eq!(result, "start_key=key123, batch_size=none");

        opt_args.insert("batch_size".to_string(), Term::Datum(Datum::Integer(50)));
        let result = Planner::format_pagination_info(&opt_args);
        assert_eq!(result, "start_key=key123, batch_size=50");

        opt_args.clear();
        let result = Planner::format_pagination_info(&opt_args);
        assert_eq!(result, "start_key=none, batch_size=none");
    }

    #[test]
    fn test_get_key_validation() {
        let mut planner = Planner::new();

        let table = create_test_table();

        let get_term = Term::Get {
            table: Box::new(table.clone()),
            key: Datum::String("user123".to_string()),
            opt_args: BTreeMap::new(),
        };
        assert!(planner.plan(&get_term).is_ok());

        let get_term = Term::Get {
            table: Box::new(table.clone()),
            key: Datum::Integer(123),
            opt_args: BTreeMap::new(),
        };
        assert!(planner.plan(&get_term).is_ok());

        let get_term = Term::Get {
            table: Box::new(table),
            key: Datum::Null,
            opt_args: BTreeMap::new(),
        };
        assert!(matches!(
            planner.plan(&get_term).unwrap_err(),
            PlanError::InvalidGetTerm(_)
        ));
    }
}
