use crate::ast::{BinOp, Datum, Expr, OptArgs, Term, UnOp};

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
    ScanTable {
        name: String,
    },
    CreateTable {
        name: String,
    },
    DropTable {
        name: String,
    },
    ListTables,
    GetByKey {
        table: String,
        key: Datum,
        optargs: OptArgs,
    },
    Filter {
        source: Box<PlanNode>,
        predicate: Expr, // raw Expr, to be simplified in optimize()
        optargs: OptArgs,
    },
    Insert {
        table: Box<PlanNode>,
        documents: Vec<Datum>,
        optargs: OptArgs,
    },
    Delete {
        source: Box<PlanNode>,
        optargs: OptArgs,
    },
    // New: A top‐level expression (e.g. Term::Expr) becomes Eval.
    Eval {
        expr: Expr,
    },
    Constant(Datum),
}

pub struct Planner;

impl Planner {
    pub const fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn plan(&self, term: &Term) -> Result<PlanNode, PlanError> {
        match term {
            Term::Expr(e) => Ok(PlanNode::Eval { expr: e.clone() }),

            Term::Table { name } => Ok(PlanNode::ScanTable { name: name.clone() }),

            Term::TableCreate { name } => Ok(PlanNode::CreateTable { name: name.clone() }),

            Term::TableDrop { name } => Ok(PlanNode::DropTable { name: name.clone() }),

            Term::TableList => Ok(PlanNode::ListTables),

            Term::Get {
                table,
                key,
                optargs,
            } => {
                let table = match &**table {
                    Term::Table { name } => name.clone(),
                    _ => format!("{:?}", self.plan(table)?),
                };

                let key = match key {
                    Datum::String(s) => Datum::String(s.clone()),
                    other => {
                        return Err(PlanError::InvalidGetTerm(Term::Datum(other.clone())));
                    }
                };

                Ok(PlanNode::GetByKey {
                    table,
                    key,
                    optargs: optargs.clone(),
                })
            }

            Term::Filter {
                source,
                predicate,
                optargs,
            } => {
                if let Term::Expr(e) = predicate.as_ref() {
                    Ok(PlanNode::Filter {
                        source: Box::new(self.plan(source)?),
                        predicate: e.clone(),
                        optargs: optargs.clone(),
                    })
                } else {
                    Err(PlanError::InvalidPredicate(predicate.as_ref().clone()))
                }
            }

            Term::Insert {
                table,
                documents,
                optargs,
            } => Ok(PlanNode::Insert {
                table: Box::new(self.plan(table)?),
                documents: documents.clone(),
                optargs: optargs.clone(),
            }),

            Term::Delete { source, optargs } => Ok(PlanNode::Delete {
                source: Box::new(self.plan(source)?),
                optargs: optargs.clone(),
            }),

            Term::Datum(_) => Err(PlanError::UnsupportedTerm(term.clone())),
        }
    }

    pub fn simplify_expr(expr: Expr) -> Expr {
        match expr {
            Expr::Constant(_) | Expr::Column(_) => expr,
            Expr::BinaryOp { op, left, right } => {
                let left_simplified = Self::simplify_expr(*left);
                let right_simplified = Self::simplify_expr(*right);

                match (&op, &left_simplified, &right_simplified) {
                    (BinOp::And, Expr::Constant(Datum::Bool(true)), r)
                    | (BinOp::Or, Expr::Constant(Datum::Bool(false)), r) => r.clone(),

                    (BinOp::And, l, Expr::Constant(Datum::Bool(true)))
                    | (BinOp::Or, l, Expr::Constant(Datum::Bool(false))) => l.clone(),

                    (BinOp::And, _, Expr::Constant(Datum::Bool(false)))
                    | (BinOp::And, Expr::Constant(Datum::Bool(false)), _) => {
                        Expr::Constant(Datum::Bool(false))
                    }

                    (BinOp::Or, Expr::Constant(Datum::Bool(true)), _)
                    | (BinOp::Or, _, Expr::Constant(Datum::Bool(true))) => {
                        Expr::Constant(Datum::Bool(true))
                    }

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
                    _ => Expr::UnaryOp {
                        op,
                        expr: Box::new(simplified_expr),
                    },
                }
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn optimize(&self, plan: PlanNode) -> PlanNode {
        match plan {
            PlanNode::Filter {
                source,
                predicate,
                optargs,
            } => {
                let optimized_source = self.optimize(*source);
                let simplified_pred = Self::simplify_expr(predicate);

                match &simplified_pred {
                    Expr::Constant(Datum::Bool(true)) => optimized_source,
                    Expr::Constant(Datum::Bool(false)) => PlanNode::Constant(Datum::Array(vec![])),
                    _ => PlanNode::Filter {
                        source: Box::new(optimized_source),
                        predicate: simplified_pred,
                        optargs,
                    },
                }
            }

            PlanNode::Insert {
                table,
                documents,
                optargs,
            } => PlanNode::Insert {
                table: Box::new(self.optimize(*table)),
                documents,
                optargs,
            },

            PlanNode::Delete { source, optargs } => PlanNode::Delete {
                source: Box::new(self.optimize(*source)),
                optargs,
            },

            PlanNode::GetByKey {
                table,
                key,
                optargs,
            } => PlanNode::GetByKey {
                table,
                key,
                optargs,
            },

            PlanNode::ScanTable { name } => PlanNode::ScanTable { name },

            PlanNode::CreateTable { name } => PlanNode::CreateTable { name },

            PlanNode::DropTable { name } => PlanNode::DropTable { name },

            PlanNode::ListTables => PlanNode::ListTables,

            PlanNode::Eval { expr } => {
                // If the PlanNode is a top‐level Eval(expression), we can simplify that expression here.
                let simplified = Self::simplify_expr(expr);
                PlanNode::Eval { expr: simplified }
            }

            PlanNode::Constant(d) => PlanNode::Constant(d),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn explain(&self, plan: &PlanNode, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        match plan {
            PlanNode::ScanTable { name } => format!("{pad}ScanTable: {name}"),

            PlanNode::CreateTable { name } => format!("{pad}CreateTable: {name}"),

            PlanNode::DropTable { name } => format!("{pad}DropTable: {name}"),

            PlanNode::ListTables => format!("{pad}ListTables"),

            PlanNode::GetByKey { table, key, .. } => {
                format!("{pad}GetByKey: table={table}, key={key:?}")
            }

            PlanNode::Filter {
                source, predicate, ..
            } => format!(
                "{pad}Filter: {predicate}\n{}",
                self.explain(source, indent + 1),
            ),

            PlanNode::Insert {
                table, documents, ..
            } => format!(
                "{}Insert {} docs\n{}",
                pad,
                documents.len(),
                self.explain(table, indent + 1)
            ),

            PlanNode::Delete { source, .. } => {
                format!("{}Delete\n{}", pad, self.explain(source, indent + 1))
            }

            PlanNode::Eval { expr } => format!("{pad}Eval: {expr}"),

            PlanNode::Constant(d) => format!("{pad}Constant: {d:?}"),
        }
    }
}
