use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type OptArgs = BTreeMap<String, Term>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Datum {
    String(String),
    Integer(i64),
    #[serde(with = "rust_decimal::serde::float")]
    Decimal(Decimal),
    Bool(bool),
    Null,
    Array(Vec<Datum>),
    Object(BTreeMap<String, Datum>),
    Parameter(String),
}

impl std::fmt::Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(s) => write!(f, "{s}"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Decimal(d) => write!(f, "{d}"),
            Self::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Self::Null => write!(f, "null"),
            Self::Array(arr) => {
                write!(f, "[")?;
                let mut first = true;
                for item in arr {
                    if !first {
                        write!(f, ",")?;
                    }
                    write!(f, "{item}")?;
                    first = false;
                }
                write!(f, "]")
            }
            Self::Object(obj) => {
                write!(f, "{{")?;
                let mut first = true;
                for (k, v) in obj {
                    if !first {
                        write!(f, ",")?;
                    }
                    write!(f, "\"{k}\":\"{v}\"")?;
                    first = false;
                }
                write!(f, "}}")
            }
            Self::Parameter(p) => write!(f, "\"{p}\""),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnOp {
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Constant(Datum),
    Column(String),
    BinaryOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnOp,
        expr: Box<Expr>,
    },
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(d) => write!(f, "{d}"),
            Self::Column(col) => write!(f, ".{col}"),
            Self::BinaryOp { op, left, right } => {
                let op_str = match op {
                    BinOp::Eq => "==",
                    BinOp::Ne => "!=",
                    BinOp::Gt => ">",
                    BinOp::Lt => "<",
                    BinOp::Ge => ">=",
                    BinOp::Le => "<=",
                    BinOp::And => "AND",
                    BinOp::Or => "OR",
                };
                write!(f, "({left} {op_str} {right})")
            }
            Self::UnaryOp { op, expr } => {
                let op_str = match op {
                    UnOp::Not => "NOT",
                };
                write!(f, "({op_str} {expr})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[repr(u64)]
pub enum TermType {
    Invalid = 0,
    Datum = 1,
    Expr = 2,
    Eq = 17,
    Ne = 18,
    Lt = 19,
    Le = 20,
    Gt = 21,
    Ge = 22,
    Not = 23,
    And = 66,
    Or = 67,
    Database = 14,
    Table = 15,
    Get = 16,
    GetField = 31,
    Filter = 39,
    Delete = 54,
    Insert = 56,
    DatabaseCreate = 57,
    DatabaseDrop = 58,
    DatabaseList = 59,
    TableCreate = 60,
    TableDrop = 61,
    TableList = 62,
}

impl std::fmt::Display for TermType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Datum => "Datum",
                Self::Expr => "Expr",
                Self::Eq => "Eq",
                Self::Ne => "Ne",
                Self::Lt => "Lt",
                Self::Le => "Le",
                Self::Gt => "Gt",
                Self::Ge => "Ge",
                Self::Not => "Not",
                Self::And => "And",
                Self::Or => "Or",
                Self::Database => "Database",
                Self::DatabaseCreate => "DatabaseCreate",
                Self::DatabaseDrop => "DatabaseDrop",
                Self::DatabaseList => "DatabaseList",
                Self::Table => "Table",
                Self::TableCreate => "TableCreate",
                Self::TableDrop => "TableDrop",
                Self::TableList => "TableList",
                Self::Get => "Get",
                Self::GetField => "GetField",
                Self::Filter => "Filter",
                Self::Delete => "Delete",
                Self::Insert => "Insert",
                Self::Invalid => "Invalid",
            }
        )
    }
}

impl From<u64> for TermType {
    fn from(value: u64) -> Self {
        match value {
            1 => Self::Datum,
            2 => Self::Expr,
            17 => Self::Eq,
            18 => Self::Ne,
            19 => Self::Lt,
            20 => Self::Le,
            21 => Self::Gt,
            22 => Self::Ge,
            23 => Self::Not,
            66 => Self::And,
            67 => Self::Or,
            14 => Self::Database,
            15 => Self::Table,
            16 => Self::Get,
            31 => Self::GetField,
            39 => Self::Filter,
            54 => Self::Delete,
            56 => Self::Insert,
            57 => Self::DatabaseCreate,
            58 => Self::DatabaseDrop,
            59 => Self::DatabaseList,
            60 => Self::TableCreate,
            61 => Self::TableDrop,
            62 => Self::TableList,
            _ => Self::Invalid,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Term {
    Datum(Datum),
    Expr(Expr),
    Database {
        name: String,
    },
    DatabaseCreate {
        name: String,
    },
    DatabaseDrop {
        name: String,
    },
    DatabaseList,
    Table {
        db: Option<String>,
        name: String,
    },
    TableList {
        db: Option<String>,
    },
    TableCreate {
        db: Option<String>,
        name: String,
    },
    TableDrop {
        db: Option<String>,
        name: String,
    },
    Get {
        table: Box<Term>,
        key: Datum,
        #[serde(default)]
        optargs: OptArgs,
    },
    Filter {
        source: Box<Term>,
        predicate: Box<Term>,
        #[serde(default)]
        optargs: OptArgs,
    },
    Delete {
        source: Box<Term>,
        #[serde(default)]
        optargs: OptArgs,
    },
    Insert {
        table: Box<Term>,
        documents: Vec<Datum>,
        #[serde(default)]
        optargs: OptArgs,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_datum_display() {
        let mut map = BTreeMap::new();
        map.insert("key".to_string(), Datum::String("value".to_string()));

        let cases = vec![
            (Datum::String("hello".to_string()), "hello"),
            (Datum::Decimal(Decimal::new(420, 1)), "42.0"),
            (Datum::Bool(true), "true"),
            (Datum::Null, "null"),
            (
                Datum::Array(vec![Datum::Decimal(1.into()), Datum::Decimal(2.into())]),
                "[1,2]",
            ),
            (
                Datum::Array(vec![
                    Datum::Decimal(Decimal::new(60, 1)),
                    Datum::Decimal(Decimal::new(90, 1)),
                ]),
                "[6.0,9.0]",
            ),
            (Datum::Object(map), "{\"key\":\"value\"}"),
            (Datum::Parameter("p".to_string()), "\"p\""),
        ];

        for (datum, expected) in cases {
            assert_eq!(datum.to_string(), expected);
        }
    }

    #[test]
    fn test_expr_display() {
        let make_binop_expr = |op: BinOp, left: Datum, right: Datum| Expr::BinaryOp {
            op,
            left: Box::new(Expr::Constant(left)),
            right: Box::new(Expr::Constant(right)),
        };

        let make_unop_expr = |op: UnOp, expr: Expr| Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        };

        let cases = vec![
            (Expr::Constant(Datum::String("hello".to_string())), "hello"),
            (Expr::Column("id".to_string()), ".id"),
            (
                make_binop_expr(BinOp::Eq, Datum::Integer(1), Datum::Integer(2)),
                "(1 == 2)",
            ),
            (
                make_binop_expr(BinOp::Ne, Datum::Integer(1), Datum::Integer(2)),
                "(1 != 2)",
            ),
            (
                make_binop_expr(BinOp::Lt, Datum::Integer(1), Datum::Integer(2)),
                "(1 < 2)",
            ),
            (
                make_binop_expr(BinOp::Le, Datum::Integer(1), Datum::Integer(2)),
                "(1 <= 2)",
            ),
            (
                make_binop_expr(BinOp::Gt, Datum::Integer(2), Datum::Integer(1)),
                "(2 > 1)",
            ),
            (
                make_binop_expr(BinOp::Ge, Datum::Integer(2), Datum::Integer(1)),
                "(2 >= 1)",
            ),
            (
                make_binop_expr(BinOp::And, Datum::Bool(true), Datum::Bool(false)),
                "(true AND false)",
            ),
            (
                make_binop_expr(BinOp::Or, Datum::Bool(true), Datum::Bool(false)),
                "(true OR false)",
            ),
            (
                make_unop_expr(UnOp::Not, Expr::Constant(Datum::Bool(true))),
                "(NOT true)",
            ),
        ];

        for (expr, expected) in cases {
            assert_eq!(expr.to_string(), expected);
        }
    }

    #[test]
    fn test_term_type_display() {
        let cases = vec![
            (TermType::Datum, "Datum"),
            (TermType::Expr, "Expr"),
            (TermType::Eq, "Eq"),
            (TermType::Ne, "Ne"),
            (TermType::Lt, "Lt"),
            (TermType::Le, "Le"),
            (TermType::Gt, "Gt"),
            (TermType::Ge, "Ge"),
            (TermType::Not, "Not"),
            (TermType::And, "And"),
            (TermType::Or, "Or"),
            (TermType::Database, "Database"),
            (TermType::DatabaseCreate, "DatabaseCreate"),
            (TermType::DatabaseDrop, "DatabaseDrop"),
            (TermType::DatabaseList, "DatabaseList"),
            (TermType::Table, "Table"),
            (TermType::TableCreate, "TableCreate"),
            (TermType::TableDrop, "TableDrop"),
            (TermType::TableList, "TableList"),
            (TermType::Get, "Get"),
            (TermType::GetField, "GetField"),
            (TermType::Filter, "Filter"),
            (TermType::Delete, "Delete"),
            (TermType::Insert, "Insert"),
            (TermType::Invalid, "Invalid"),
        ];

        for (term_type, expected) in cases {
            assert_eq!(term_type.to_string(), expected);
        }
    }

    #[test]
    fn test_term_type_from() {
        let cases: Vec<(u64, TermType)> = vec![
            (0, TermType::Invalid),
            (1, TermType::Datum),
            (2, TermType::Expr),
            (14, TermType::Database),
            (15, TermType::Table),
            (16, TermType::Get),
            (17, TermType::Eq),
            (18, TermType::Ne),
            (19, TermType::Lt),
            (20, TermType::Le),
            (21, TermType::Gt),
            (22, TermType::Ge),
            (23, TermType::Not),
            (31, TermType::GetField),
            (39, TermType::Filter),
            (54, TermType::Delete),
            (56, TermType::Insert),
            (57, TermType::DatabaseCreate),
            (58, TermType::DatabaseDrop),
            (59, TermType::DatabaseList),
            (60, TermType::TableCreate),
            (61, TermType::TableDrop),
            (62, TermType::TableList),
            (66, TermType::And),
            (67, TermType::Or),
            (999, TermType::Invalid),
        ];

        for (num, expected) in cases {
            let tt: TermType = num.into();
            assert_eq!(tt, expected);
        }
    }
}
