use crate::ast::{BinOp, Datum, Expr, OptArgs, Term, TermType, UnOp};
use rmpv::Value;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum ParseError {
    ExpectedArray(usize),
    ExpectedString,
    ExpectedInteger,
    ExpectedDecimal(f64),
    WrongVariant,
    WrongDatum,
    InvalidASTStructure(String),
    UnexpectedTermType,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedArray(len) => write!(f, "expected array of length {len}"),
            Self::ExpectedString => write!(f, "expected string"),
            Self::ExpectedInteger => write!(f, "expected integer"),
            Self::ExpectedDecimal(v) => write!(f, "expected decimal, got {v}"),
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
            Value::Array(arr) if arr.len() == 3 => self.parse_ast(arr),
            v => Err(ParseError::InvalidASTStructure(v.to_string())),
        }
    }

    fn parse_ast(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let term_type = parse_term_type(arr)?;
        let optargs = self.parse_opt_args(arr)?;

        match term_type {
            TermType::Expr => self.parse_expr_term(arr),
            TermType::Table => self.parse_table_term(arr),
            TermType::TableCreate => self.parse_table_create_term(arr),
            TermType::TableDrop => self.parse_table_drop_term(arr),
            TermType::TableList => self.parse_table_list_term(arr),
            TermType::Get => self.parse_get_term(arr, optargs),
            TermType::GetField => self.parse_get_field_term(arr),
            TermType::Filter => self.parse_filter_term(arr, optargs),
            TermType::Delete => self.parse_delete_term(arr, optargs),
            TermType::Insert => self.parse_insert_term(arr, optargs),
            TermType::Datum => {
                let args = self.parse_args(arr, 1)?;
                let datum = self.parse_datum(&args[0])?;
                Ok(Term::Datum(datum))
            }
            TermType::Eq
            | TermType::Ne
            | TermType::Lt
            | TermType::Le
            | TermType::Gt
            | TermType::Ge
            | TermType::And
            | TermType::Or => self.parse_binary_expr_term(&term_type, arr),
            TermType::Not => self.parse_unary_expr_term(&term_type, arr),
            TermType::Invalid => Err(ParseError::UnexpectedTermType),
        }
    }

    fn parse_expr_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let expr = self.parse_expr(&args[0])?;
        Ok(Term::Expr(expr))
    }

    fn parse_table_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::Table { name })
    }

    fn parse_table_create_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::TableCreate { name })
    }

    fn parse_table_drop_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let name = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::TableDrop { name })
    }

    fn parse_table_list_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        self.parse_args(arr, 0)?;
        Ok(Term::TableList)
    }

    fn parse_get_term(&self, arr: &[Value], optargs: OptArgs) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 2)?;
        let table = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let table = match_table(table)?;
        let key = match_key(self.parse_datum(&args[1])?)?;
        Ok(Term::Get {
            table: Box::new(table),
            key,
            optargs,
        })
    }

    fn parse_filter_term(&self, arr: &[Value], optargs: OptArgs) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 2)?;
        let source = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let predicate = parse_term_from_array(&args[1], |arr| self.parse_ast(arr))?;
        Ok(Term::Filter {
            source: Box::new(source),
            predicate: Box::new(predicate),
            optargs,
        })
    }

    fn parse_delete_term(&self, arr: &[Value], optargs: OptArgs) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let source = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        Ok(Term::Delete {
            source: Box::new(source),
            optargs,
        })
    }

    fn parse_insert_term(&self, arr: &[Value], optargs: OptArgs) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 2)?;
        let table = parse_term_from_array(&args[0], |arr| self.parse_ast(arr))?;
        let table = match_table(table)?;
        let documents = match &args[1] {
            Value::Array(docs) => docs
                .iter()
                .map(|v| self.parse_datum(v))
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(ParseError::ExpectedArray(2)),
        };
        Ok(Term::Insert {
            table: Box::new(table),
            documents,
            optargs,
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

    fn parse_get_field_term(&self, arr: &[Value]) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
        let field = match_string(self.parse_datum(&args[0])?)?;
        Ok(Term::Expr(Expr::Column(field)))
    }

    fn parse_binary_expr_term(
        &self,
        term_type: &TermType,
        arr: &[Value],
    ) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 2)?;
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
        arr: &[Value],
    ) -> Result<Term, ParseError> {
        let args = self.parse_args(arr, 1)?;
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

                match term_type {
                    TermType::Datum => {
                        let args = self.parse_args(arr, 1)?;
                        let datum = self.parse_datum(&args[0])?;
                        Ok(Expr::Constant(datum))
                    }
                    TermType::GetField => {
                        let args = self.parse_args(arr, 1)?;
                        let field = match_string(self.parse_datum(&args[0])?)?;
                        Ok(Expr::Column(field))
                    }
                    _ => {
                        let term = self.parse_ast(arr)?;
                        match term {
                            Term::Expr(expr) => Ok(expr),
                            Term::Datum(d) => Ok(Expr::Constant(d)),
                            _ => Err(ParseError::WrongVariant),
                        }
                    }
                }
            }
            _ => Err(ParseError::WrongVariant),
        }
    }

    #[allow(clippy::unused_self)]
    fn parse_args<'a>(&self, arr: &'a [Value], expected: usize) -> Result<&'a [Value], ParseError> {
        let args = match arr.get(1) {
            Some(Value::Array(args)) => args.as_slice(),
            _ => return Err(ParseError::ExpectedArray(expected)),
        };

        if args.len() != expected {
            return Err(ParseError::ExpectedArray(expected));
        }

        Ok(args)
    }

    fn parse_opt_args(&self, arr: &[Value]) -> Result<OptArgs, ParseError> {
        let mut optargs = OptArgs::new();
        if let Some(Value::Map(map)) = arr.get(2) {
            for (k, v) in map {
                let key = match k {
                    Value::String(s) => s.as_str().ok_or(ParseError::ExpectedString)?.to_string(),
                    _ => return Err(ParseError::ExpectedString),
                };
                let term = parse_term_from_array(v, |arr| self.parse_ast(arr))?;
                optargs.insert(key, term);
            }
        }
        Ok(optargs)
    }
}

fn parse_term_from_array<T>(
    value: &Value,
    f: impl Fn(&[Value]) -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    match value {
        Value::Array(arr) => f(arr),
        _ => Err(ParseError::ExpectedArray(3)),
    }
}

fn parse_term_type(arr: &[Value]) -> Result<TermType, ParseError> {
    let term_type = arr
        .first()
        .and_then(Value::as_u64)
        .ok_or(ParseError::ExpectedInteger)?;
    Ok(TermType::from(term_type))
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

    fn parse(input: Vec<Value>) -> Result<Term, ParseError> {
        Parser::new().parse(&Value::Array(input))
    }

    #[test]
    fn test_parse_args_wrong_type() {
        let arr = vec![Value::from(15), Value::Nil, Value::Map(vec![])];
        let err = Parser::new().parse_args(&arr, 1).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedArray(1)));
    }

    #[test]
    fn test_parse_args_wrong_len() {
        let arr = vec![Value::from(15), Value::Array(vec![]), Value::Map(vec![])];
        let err = Parser::new().parse_args(&arr, 1).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedArray(1)));
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
    fn test_parse_table_term() {
        let input = make_input(15, vec![Value::String("table".into())]);
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::Table {
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_create_term() {
        let input = make_input(60, vec![Value::String("table".into())]);
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::TableCreate {
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_drop_term() {
        let input = make_input(61, vec![Value::String("table".into())]);
        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::TableDrop {
                name: "table".to_string()
            }
        );
    }

    #[test]
    fn test_parse_table_list_term() {
        let input = make_input(62, vec![]);
        let term = parse(input).unwrap();
        assert_eq!(term, Term::TableList);
    }

    #[test]
    fn test_parse_get_term() {
        let table = make_input(15, vec![Value::String("table".into())]);
        let input = make_input(16, vec![Value::Array(table), Value::String("key".into())]);

        let term = parse(input).unwrap();
        assert_eq!(
            term,
            Term::Get {
                table: Box::new(Term::Table {
                    name: "table".to_string()
                }),
                key: Datum::String("key".to_string()),
                optargs: OptArgs::new(),
            }
        );
    }

    #[test]
    fn test_parse_get_field_term() {
        let input = make_input(31, vec![Value::String("foo".into())]);
        let term = Parser::new().parse(&Value::Array(input)).unwrap();
        assert_eq!(term, Term::Expr(Expr::Column("foo".to_string())));
    }

    #[test]
    fn test_parse_filter_term() {
        let table = make_input(15, vec![Value::String("table".into())]);
        let expected_field = make_input(31, vec![Value::String("foo".into())]);
        let expected_value = make_input(1, vec![Value::Integer(21.into())]);
        let predicate = make_input(
            17,
            vec![Value::Array(expected_field), Value::Array(expected_value)],
        );
        let input = make_input(39, vec![Value::Array(table), Value::Array(predicate)]);

        let term = parse(input).unwrap();
        if let Term::Filter {
            source,
            predicate,
            optargs,
        } = term
        {
            assert_eq!(
                *source,
                Term::Table {
                    name: "table".to_string()
                }
            );
            assert!(matches!(*predicate, Term::Expr(_)));
            assert_eq!(optargs.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_delete_term() {
        let table = make_input(15, vec![Value::String("table".into())]);
        let input = make_input(54, vec![Value::Array(table)]);

        let term = parse(input).unwrap();
        if let Term::Delete { source, optargs } = term {
            assert_eq!(
                *source,
                Term::Table {
                    name: "table".to_string()
                }
            );
            assert_eq!(optargs.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_insert_term() {
        let table = make_input(15, vec![Value::String("table".into())]);
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
        let input = make_input(56, vec![Value::Array(table), Value::Array(docs)]);

        let term = parse(input).unwrap();
        if let Term::Insert {
            table,
            documents,
            optargs,
        } = term
        {
            assert_eq!(
                *table,
                Term::Table {
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
            assert_eq!(optargs.len(), 0);
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_datum_term() {
        let input = make_input(1, vec![Value::String("foo".into())]);

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
        let field_expr = make_input(31, vec![Value::String("foo".into())]);
        let input = make_input(2, vec![Value::Array(field_expr)]);

        let term = parse(input).unwrap();
        if let Term::Expr(Expr::Column(ref s)) = term {
            assert_eq!(s, "foo");
        } else {
            panic!()
        }
    }

    #[test]
    fn test_parse_expr_errors() {
        let parser = Parser::new();

        assert!(matches!(
            parser.parse_expr(&Value::Nil).unwrap_err(),
            ParseError::WrongVariant
        ));
        assert!(matches!(
            parser.parse_expr(&Value::Integer(1.into())).unwrap_err(),
            ParseError::WrongVariant
        ));
    }

    #[test]
    fn test_parse_term_from_array() {
        let arr = vec![
            Value::from(15),
            Value::Array(vec![Value::String("table".into())]),
            Value::Map(Vec::<(Value, Value)>::new()),
        ];
        let res = parse_term_from_array(&Value::Array(arr), |a| Ok(a.len()));
        assert_eq!(res.unwrap(), 3);
        assert!(matches!(
            parse_term_from_array(&Value::Nil, |_| Ok(0)).unwrap_err(),
            ParseError::ExpectedArray(3)
        ));
    }

    #[test]
    fn test_parse_term_type() {
        let arr = vec![Value::from(15)];
        assert!(matches!(parse_term_type(&arr).unwrap(), TermType::Table));
        let arr = vec![Value::Nil];
        assert!(matches!(
            parse_term_type(&arr).unwrap_err(),
            ParseError::ExpectedInteger
        ));
    }

    #[test]
    fn test_match_table() {
        assert!(match_table(Term::Table { name: "foo".into() }).is_ok());
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
