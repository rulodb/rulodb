use crate::ast::{BinOp, Datum, Expr, OptArgs, Term, TermType, UnOp};
use rmpv::Value;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum ParseError {
    ExpectedArray,
    ExpectedString,
    ExpectedInteger,
    ExpectedDecimal(f64),
    WrongNumberOfArgs(usize),
    WrongVariant,
    WrongDatum,
    InvalidASTStructure(String),
    UnexpectedTermType,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedArray => write!(f, "expected array"),
            Self::ExpectedString => write!(f, "expected string"),
            Self::ExpectedInteger => write!(f, "expected integer"),
            Self::ExpectedDecimal(v) => write!(f, "expected decimal, got {v}"),
            Self::WrongNumberOfArgs(len) => write!(f, "wrong number of arguments, received {len}"),
            Self::WrongVariant => write!(f, "wrong variant"),
            Self::WrongDatum => write!(f, "wrong datum"),
            Self::InvalidASTStructure(v) => write!(f, "invalid AST structure: {v}"),
            Self::UnexpectedTermType => write!(f, "unexpected term type"),
        }
    }
}

impl std::error::Error for ParseError {}

pub struct Parser;

impl Parser {
    pub const fn new() -> Self {
        Self
    }

    pub fn parse(&self, value: &Value) -> Result<Term, ParseError> {
        match value {
            Value::Array(arr) if arr.len() == 3 || arr.len() == 2 => self.parse_ast(arr),
            v => Err(ParseError::InvalidASTStructure(v.to_string())),
        }
    }

    fn parse_ast(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let term_type = parse_term_type(arr)?;
        let args = self.parse_args(arr)?;
        let opt_args = self.parse_opt_args(arr)?;

        match term_type {
            TermType::Datum => {
                check_arg_count(args, 1)?;
                let datum = self.parse_datum(&args[0])?;
                Ok(Term::Datum(datum))
            }
            TermType::Expr => self.parse_expr_term(args),
            TermType::Eq
            | TermType::Ne
            | TermType::Lt
            | TermType::Le
            | TermType::Gt
            | TermType::Ge
            | TermType::And
            | TermType::Or => self.parse_binary_expr_term(&term_type, args),
            TermType::Not => self.parse_unary_expr_term(&term_type, args),
            TermType::Database => self.parse_database_term(args),
            TermType::DatabaseCreate => self.parse_database_create_term(args),
            TermType::DatabaseDrop => self.parse_database_drop_term(args),
            TermType::DatabaseList => self.parse_database_list_term(args),
            TermType::Table => self.parse_table_term(args),
            TermType::TableCreate => self.parse_table_create_term(args),
            TermType::TableDrop => self.parse_table_drop_term(args),
            TermType::TableList => self.parse_table_list_term(args),
            TermType::Get => self.parse_get_term(args, opt_args),
            TermType::GetField => self.parse_get_field_term(args, &opt_args),
            TermType::Filter => self.parse_filter_term(args, opt_args),
            TermType::Delete => self.parse_delete_term(args, opt_args),
            TermType::Insert => self.parse_insert_term(args, opt_args),
            TermType::Invalid => Err(ParseError::UnexpectedTermType),
        }
    }

    fn parse_expr_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let expr = self.parse_expr(&args[0])?;
        Ok(Term::Expr(expr))
    }

    fn parse_database_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::Database { name })
    }

    fn parse_database_create_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::DatabaseCreate { name })
    }

    fn parse_database_drop_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::DatabaseDrop { name })
    }

    #[allow(clippy::unused_self)]
    fn parse_database_list_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count(args, 0)?;
        Ok(Term::DatabaseList)
    }

    fn parse_table_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count_between(args, 1, 2)?;
        match args.len() {
            1 => {
                let name = match_string(self.parse_datum(&args[0])?)?;
                Ok(Term::Table { db: None, name })
            }
            2 => {
                // First arg is a DB term, not just a string
                let db_term = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
                let Term::Database { name: db } = db_term else {
                    return Err(ParseError::WrongVariant);
                };
                let name = match_string(self.parse_datum(&args[1])?)?;
                Ok(Term::Table { db: Some(db), name })
            }
            n => Err(ParseError::WrongNumberOfArgs(n)),
        }
    }

    fn parse_table_create_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count_between(args, 1, 2)?;
        match args.len() {
            1 => {
                let name = match_string(self.parse_datum(&args[0])?)?;
                Ok(Term::TableCreate { db: None, name })
            }
            2 => {
                let db_term = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
                let Term::Database { name: db } = db_term else {
                    return Err(ParseError::WrongVariant);
                };
                let name = match_string(self.parse_datum(&args[1])?)?;
                Ok(Term::TableCreate { db: Some(db), name })
            }
            n => Err(ParseError::WrongNumberOfArgs(n)),
        }
    }

    fn parse_table_drop_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count_between(args, 1, 2)?;
        match args.len() {
            1 => {
                let name = match_string(self.parse_datum(&args[0])?)?;
                Ok(Term::TableDrop { db: None, name })
            }
            2 => {
                let db_term = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
                let Term::Database { name: db } = db_term else {
                    return Err(ParseError::WrongVariant);
                };
                let name = match_string(self.parse_datum(&args[1])?)?;
                Ok(Term::TableDrop { db: Some(db), name })
            }
            n => Err(ParseError::WrongNumberOfArgs(n)),
        }
    }

    fn parse_table_list_term(&self, args: &[Value]) -> Result<Term, ParseError> {
        check_arg_count_between(args, 0, 2)?;
        match args.len() {
            0 => Ok(Term::TableList { db: None }),
            1 => {
                let db_term = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
                let Term::Database { name: db } = db_term else {
                    return Err(ParseError::WrongVariant);
                };
                Ok(Term::TableList { db: Some(db) })
            }
            n => Err(ParseError::WrongNumberOfArgs(n)),
        }
    }

    fn parse_get_term(&self, args: &[Value], opt_args: OptArgs) -> Result<Term, ParseError> {
        check_arg_count(args, 2)?;
        let table = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let table = match_table(table)?;
        let key = match_key(self.parse_datum(&args[1])?)?;
        Ok(Term::Get {
            table: Box::new(table),
            key,
            opt_args,
        })
    }

    fn parse_filter_term(&self, args: &[Value], opt_args: OptArgs) -> Result<Term, ParseError> {
        check_arg_count(args, 2)?;
        let source = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let predicate = parse_term_from_array(&args[1], |arr| self.parse_ast(arr))?;
        Ok(Term::Filter {
            source: Box::new(source),
            predicate: Box::new(predicate),
            opt_args,
        })
    }

    fn parse_delete_term(&self, args: &[Value], opt_args: OptArgs) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let source = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        Ok(Term::Delete {
            source: Box::new(source),
            opt_args,
        })
    }

    fn parse_insert_term(&self, args: &[Value], opt_args: OptArgs) -> Result<Term, ParseError> {
        check_arg_count(args, 2)?;
        let table = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let table = match_table(table)?;
        let documents = match &args[1] {
            Value::Array(docs) => docs
                .iter()
                .map(|v| self.parse_datum(v))
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(ParseError::ExpectedArray),
        };
        Ok(Term::Insert {
            table: Box::new(table),
            documents,
            opt_args,
        })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn parse_datum(&self, value: &Value) -> Result<Datum, ParseError> {
        match value {
            Value::String(s) => Ok(Datum::String(
                s.as_str().ok_or(ParseError::ExpectedString)?.to_string(),
            )),
            Value::Integer(i) => Ok(Datum::Integer(
                i.as_i64().ok_or(ParseError::ExpectedInteger)?,
            )),
            Value::F64(f) => Ok(Datum::Decimal(
                Decimal::from_f64(*f).ok_or(ParseError::ExpectedDecimal(*f))?,
            )),
            Value::Boolean(b) => Ok(Datum::Bool(*b)),
            Value::Nil => Ok(Datum::Null),
            Value::Array(arr) => {
                let vals = arr
                    .iter()
                    .map(|v| self.parse_datum(v))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Datum::Array(vals))
            }
            Value::Map(map) => {
                let mut obj = BTreeMap::new();
                for (k, v) in map {
                    let key = match k {
                        Value::String(s) => {
                            s.as_str().ok_or(ParseError::ExpectedString)?.to_string()
                        }
                        _ => return Err(ParseError::ExpectedString),
                    };
                    obj.insert(key, self.parse_datum(v)?);
                }
                Ok(Datum::Object(obj))
            }
            _ => Err(ParseError::WrongDatum),
        }
    }

    fn parse_get_field_term(&self, args: &[Value], opt_args: &OptArgs) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let field = match_string(self.parse_datum(&args[0])?)?;
        let separator = opt_args.get("separator").and_then(|term| {
            if let Term::Datum(Datum::String(s)) = term {
                Some(s.clone())
            } else {
                None
            }
        });

        Ok(Term::Expr(Expr::Field {
            name: field,
            separator,
        }))
    }

    fn parse_binary_expr_term(
        &self,
        term_type: &TermType,
        args: &[Value],
    ) -> Result<Term, ParseError> {
        check_arg_count(args, 2)?;
        let left = self.parse_expr(&args[0])?;
        let right = self.parse_expr(&args[1])?;
        let op = match term_type {
            TermType::Eq => BinOp::Eq,
            TermType::Ne => BinOp::Ne,
            TermType::Lt => BinOp::Lt,
            TermType::Le => BinOp::Le,
            TermType::Gt => BinOp::Gt,
            TermType::Ge => BinOp::Ge,
            TermType::And => BinOp::And,
            TermType::Or => BinOp::Or,
            _ => return Err(ParseError::UnexpectedTermType),
        };

        Ok(Term::Expr(Expr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }

    fn parse_unary_expr_term(
        &self,
        term_type: &TermType,
        args: &[Value],
    ) -> Result<Term, ParseError> {
        check_arg_count(args, 1)?;
        let expr = self.parse_expr(&args[0])?;
        let op = match term_type {
            TermType::Not => UnOp::Not,
            _ => return Err(ParseError::UnexpectedTermType),
        };

        Ok(Term::Expr(Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }))
    }

    fn parse_expr(&self, value: &Value) -> Result<Expr, ParseError> {
        match value {
            Value::Array(arr) if !arr.is_empty() => {
                let term_type = parse_term_type(arr)?;
                let args = self.parse_args(arr)?;
                let opt_args = self.parse_opt_args(arr)?;

                match term_type {
                    TermType::GetField => {
                        let field = match_string(self.parse_datum(&args[0])?)?;
                        let separator = opt_args.get("separator").and_then(|term| {
                            if let Term::Datum(Datum::String(s)) = term {
                                Some(s.clone())
                            } else {
                                None
                            }
                        });

                        Ok(Expr::Field {
                            name: field,
                            separator,
                        })
                    }
                    TermType::Eq
                    | TermType::Ne
                    | TermType::Lt
                    | TermType::Le
                    | TermType::Gt
                    | TermType::Ge
                    | TermType::And
                    | TermType::Or => {
                        let term = self.parse_binary_expr_term(&term_type, args)?;
                        if let Term::Expr(expr) = term {
                            Ok(expr)
                        } else {
                            Err(ParseError::WrongVariant)
                        }
                    }
                    TermType::Not => {
                        let term = self.parse_unary_expr_term(&term_type, args)?;
                        if let Term::Expr(expr) = term {
                            Ok(expr)
                        } else {
                            Err(ParseError::WrongVariant)
                        }
                    }
                    TermType::Datum => {
                        check_arg_count(args, 1)?;
                        let datum = self.parse_datum(&args[0])?;
                        Ok(Expr::Constant(datum))
                    }
                    _ => {
                        let datum = self.parse_datum(value)?;
                        Ok(Expr::Constant(datum))
                    }
                }
            }
            _ => {
                let datum = self.parse_datum(value)?;
                Ok(Expr::Constant(datum))
            }
        }
    }

    #[allow(clippy::unused_self)]
    fn parse_args<'a>(&self, arr: &'a [Value]) -> Result<&'a [Value], ParseError> {
        match arr.get(1) {
            Some(Value::Array(args)) => Ok(args.as_slice()),
            _ => Err(ParseError::ExpectedArray),
        }
    }

    fn parse_opt_args(&self, args: &[Value]) -> Result<OptArgs, ParseError> {
        let mut opt_args = OptArgs::new();
        if let Some(Value::Map(map)) = args.get(2) {
            for (k, v) in map {
                let key = match k {
                    Value::String(s) => s.as_str().ok_or(ParseError::ExpectedString)?.to_string(),
                    _ => return Err(ParseError::ExpectedString),
                };
                let value = self.parse_datum(v)?;
                opt_args.insert(key, Term::Datum(value));
            }
        }
        Ok(opt_args)
    }
}

fn parse_term_from_array<T>(
    value: &Value,
    f: impl Fn(&[Value]) -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    match value {
        Value::Array(arr) => f(arr),
        _ => Err(ParseError::ExpectedArray),
    }
}

fn parse_term_type(arr: &[Value]) -> Result<TermType, ParseError> {
    let term_type = arr
        .first()
        .and_then(Value::as_u64)
        .ok_or(ParseError::ExpectedInteger)?;
    Ok(TermType::from(term_type))
}

const fn check_arg_count(arr: &[Value], length: usize) -> Result<(), ParseError> {
    if arr.len() == length {
        Ok(())
    } else {
        Err(ParseError::WrongNumberOfArgs(arr.len()))
    }
}

const fn check_arg_count_between(
    arr: &[Value],
    min_len: usize,
    max_len: usize,
) -> Result<(), ParseError> {
    match arr.len() {
        len if len < min_len || len > max_len => Err(ParseError::WrongNumberOfArgs(len)),
        _ => Ok(()),
    }
}

fn match_table(table: Term) -> Result<Term, ParseError> {
    match table {
        Term::Table { .. } => Ok(table),
        _ => Err(ParseError::WrongVariant),
    }
}

fn match_string(datum: Datum) -> Result<String, ParseError> {
    match datum {
        Datum::String(s) => Ok(s),
        _ => Err(ParseError::ExpectedString),
    }
}

fn match_key(datum: Datum) -> Result<Datum, ParseError> {
    match datum {
        Datum::String(_) | Datum::Integer(_) => Ok(datum),
        _ => Err(ParseError::WrongDatum),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Datum, Expr, OptArgs, Term};
    use rmpv::Value;
    use rust_decimal::Decimal;
    use rust_decimal::prelude::FromPrimitive;
    use std::collections::BTreeMap;

    fn make_input(term_type: u64, args: Vec<Value>) -> Vec<Value> {
        vec![
            Value::from(term_type),
            Value::Array(args),
            Value::Map(Vec::<(Value, Value)>::new()),
        ]
    }

    fn make_input_with_opt_args(
        term_type: u64,
        args: Vec<Value>,
        opt_args: Vec<(Value, Value)>,
    ) -> Vec<Value> {
        vec![
            Value::from(term_type),
            Value::Array(args),
            Value::Map(opt_args),
        ]
    }

    fn parse(input: Vec<Value>) -> Result<Term, ParseError> {
        Parser::new().parse(&Value::Array(input))
    }

    #[test]
    fn test_parse_args_wrong_type() {
        let arr = vec![
            Value::from(TermType::Table as u64),
            Value::Nil,
            Value::Map(vec![]),
        ];
        let err = Parser::new().parse_args(&arr).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedArray));
    }

    #[test]
    fn test_parse_invalid_structure() {
        let v = Value::Nil;
        let err = Parser::new().parse(&v).unwrap_err();
        assert!(matches!(err, ParseError::InvalidASTStructure(_)));
    }

    #[test]
    fn test_parse_ast_unexpected_term_type() {
        let input = make_input(999, vec![]);
        let err = parse(input).unwrap_err();
        assert!(matches!(err, ParseError::UnexpectedTermType));
    }

    #[test]
    fn test_parse_database_term() {
        let input = make_input(
            TermType::Database as u64,
            vec![Value::String("database".into())],
        );
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::Database {
                name: "database".to_string()
            }
        );
    }

    #[test]
    fn test_parse_database_create_term() {
        let input = make_input(
            TermType::DatabaseCreate as u64,
            vec![Value::String("database".into())],
        );
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::DatabaseCreate {
                name: "database".to_string()
            }
        );
    }

    #[test]
    fn test_parse_database_drop_term() {
        let input = make_input(
            TermType::DatabaseDrop as u64,
            vec![Value::String("database".into())],
        );
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::DatabaseDrop {
                name: "database".to_string()
            }
        );
    }

    #[test]
    fn test_parse_database_list_term() {
        let input = make_input(TermType::DatabaseList as u64, vec![]);
        let term = parse(input).unwrap();
        assert_eq!(term, Term::DatabaseList);
    }

    #[test]
    fn test_parse_table_term() {
        let input = make_input(TermType::Table as u64, vec![Value::String("table".into())]);
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::Table {
                db: None,
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_create_term() {
        let input = make_input(
            TermType::TableCreate as u64,
            vec![Value::String("table".into())],
        );
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::TableCreate {
                db: None,
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_drop_term() {
        let input = make_input(
            TermType::TableDrop as u64,
            vec![Value::String("table".into())],
        );
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::TableDrop {
                db: None,
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_list_term() {
        let input = make_input(TermType::TableList as u64, vec![]);
        let term = parse(input).unwrap();
        assert_eq!(term, Term::TableList { db: None });
    }

    #[test]
    fn test_parse_get_term() {
        let table = make_input(TermType::Table as u64, vec![Value::String("table".into())]);
        let input = make_input(
            TermType::Get as u64,
            vec![Value::Array(table), Value::String("key".into())],
        );

        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::Get {
                table: Box::new(Term::Table {
                    db: None,
                    name: "table".to_string()
                }),
                key: Datum::String("key".to_string()),
                opt_args: OptArgs::new(),
            }
        );
    }

    #[test]
    fn test_parse_get_field_term() {
        let input = make_input(TermType::GetField as u64, vec![Value::String("foo".into())]);
        let term = Parser::new().parse(&Value::Array(input)).unwrap();
        assert_eq!(
            term,
            Term::Expr(Expr::Field {
                name: "foo".to_string(),
                separator: None
            })
        );
    }

    #[test]
    fn test_parse_get_field_term_with_separator() {
        let input = make_input_with_opt_args(
            TermType::GetField as u64,
            vec![Value::String("foo".into())],
            vec![(Value::String("separator".into()), Value::String(",".into()))],
        );
        let term = Parser::new().parse(&Value::Array(input)).unwrap();
        assert_eq!(
            term,
            Term::Expr(Expr::Field {
                name: "foo".to_string(),
                separator: Some(",".to_string())
            })
        );
    }

    #[test]
    fn test_parse_filter_term() {
        let table = make_input(TermType::Table as u64, vec![Value::String("table".into())]);
        let expected_field =
            make_input(TermType::GetField as u64, vec![Value::String("foo".into())]);
        let expected_value = make_input(TermType::Datum as u64, vec![Value::Integer(21.into())]);
        let predicate = make_input(
            17,
            vec![Value::Array(expected_field), Value::Array(expected_value)],
        );
        let input = make_input(
            TermType::Filter as u64,
            vec![Value::Array(table), Value::Array(predicate)],
        );

        let term = parse(input).unwrap();
        if let Term::Filter {
            source,
            predicate,
            opt_args,
        } = term
        {
            assert_eq!(
                *source,
                Term::Table {
                    db: None,
                    name: "table".to_string()
                }
            );
            assert!(matches!(*predicate, Term::Expr(_)));
            assert_eq!(opt_args.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_delete_term() {
        let table = make_input(TermType::Table as u64, vec![Value::String("table".into())]);
        let input = make_input(TermType::Delete as u64, vec![Value::Array(table)]);

        let term = parse(input).unwrap();
        if let Term::Delete { source, opt_args } = term {
            assert_eq!(
                *source,
                Term::Table {
                    db: None,
                    name: "table".to_string()
                }
            );
            assert_eq!(opt_args.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_insert_term() {
        let table = make_input(TermType::Table as u64, vec![Value::String("table".into())]);
        let docs = vec![
            Value::Map(vec![(
                Value::String("doc1key1".into()),
                Value::String("doc1val1".into()),
            )]),
            Value::Map(vec![(
                Value::String("doc2key1".into()),
                Value::String("doc2val1".into()),
            )]),
        ];
        let input = make_input(
            TermType::Insert as u64,
            vec![Value::Array(table), Value::Array(docs)],
        );

        let term = parse(input).unwrap();
        if let Term::Insert {
            table,
            documents,
            opt_args,
        } = term
        {
            assert_eq!(
                *table,
                Term::Table {
                    db: None,
                    name: "table".to_string()
                }
            );
            assert_eq!(
                documents,
                vec![
                    Datum::Object(BTreeMap::from([(
                        "doc1key1".to_string(),
                        Datum::String("doc1val1".to_string())
                    ),])),
                    Datum::Object(BTreeMap::from([(
                        "doc2key1".to_string(),
                        Datum::String("doc2val1".to_string())
                    ),])),
                ]
            );
            assert_eq!(opt_args.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_datum_term() {
        let input = make_input(TermType::Datum as u64, vec![Value::String("foo".into())]);

        let term = parse(input).unwrap();
        assert_eq!(term, Term::Datum(Datum::String("foo".to_string())));
    }

    #[test]
    fn test_parse_datum_all_types() {
        let parser = Parser::new();
        assert_eq!(
            parser.parse_datum(&Value::String("foo".into())).unwrap(),
            Datum::String("foo".into())
        );
        assert_eq!(
            parser.parse_datum(&Value::Integer(42.into())).unwrap(),
            Datum::Integer(42)
        );
        assert_eq!(
            parser.parse_datum(&Value::F64(1.5)).unwrap(),
            Datum::Decimal(Decimal::from_f64(1.5).unwrap())
        );
        assert_eq!(
            parser.parse_datum(&Value::Boolean(true)).unwrap(),
            Datum::Bool(true)
        );
        assert_eq!(parser.parse_datum(&Value::Nil).unwrap(), Datum::Null);
        assert_eq!(
            parser
                .parse_datum(&Value::Array(vec![Value::String("foo".into())]))
                .unwrap(),
            Datum::Array(vec![Datum::String("foo".into())])
        );

        let val = vec![(Value::String("k".into()), Value::String("v".into()))];
        assert_eq!(parser.parse_datum(&Value::Map(val)).unwrap(), {
            let mut obj = BTreeMap::new();
            obj.insert("k".into(), Datum::String("v".into()));
            Datum::Object(obj)
        });
    }

    #[test]
    fn test_parse_datum_errors() {
        let parser = Parser::new();
        assert!(matches!(
            parser.parse_datum(&Value::F64(f64::NAN)).unwrap_err(),
            ParseError::ExpectedDecimal(_)
        ));
        assert!(matches!(
            parser.parse_datum(&Value::Ext(1, vec![])).unwrap_err(),
            ParseError::WrongDatum
        ));
    }

    #[test]
    fn test_parse_expr_term() {
        let field_expr = make_input(TermType::GetField as u64, vec![Value::String("foo".into())]);
        let input = make_input(TermType::Expr as u64, vec![Value::Array(field_expr)]);

        let term = parse(input).unwrap();
        if let Term::Expr(Expr::Field {
            name: ref s,
            separator: None,
        }) = term
        {
            assert_eq!(s, "foo");
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_term_from_array() {
        let arr = vec![
            Value::from(TermType::Table as u64),
            Value::Array(vec![Value::String("table".into())]),
            Value::Map(Vec::<(Value, Value)>::new()),
        ];
        let res = parse_term_from_array(&Value::Array(arr), |a| Ok(a.len()));
        assert_eq!(res.unwrap(), 3);
        assert!(matches!(
            parse_term_from_array(&Value::Nil, |_| Ok(0)).unwrap_err(),
            ParseError::ExpectedArray
        ));
    }

    #[test]
    fn test_parse_term_type() {
        let arr = vec![Value::from(TermType::Table as u64)];
        assert!(matches!(parse_term_type(&arr).unwrap(), TermType::Table));
        let arr = vec![Value::Nil];
        assert!(matches!(
            parse_term_type(&arr).unwrap_err(),
            ParseError::ExpectedInteger
        ));
    }

    #[test]
    fn test_match_table() {
        assert!(
            match_table(Term::Table {
                db: None,
                name: "foo".into()
            })
            .is_ok()
        );
        assert!(match_table(Term::Datum(Datum::Null)).is_err());
    }

    #[test]
    fn test_match_string() {
        assert_eq!(match_string(Datum::String("foo".into())).unwrap(), "foo");
        assert!(match_string(Datum::Null).is_err());
    }

    #[test]
    fn test_match_key() {
        assert!(match_key(Datum::String("foo".into())).is_ok());
        assert!(match_key(Datum::Integer(1)).is_ok());
        assert!(match_key(Datum::Null).is_err());
    }
}
