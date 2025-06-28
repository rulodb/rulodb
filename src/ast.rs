pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

pub use proto::*;
use std::collections::HashMap;

/// The Document is a map of string keys to datum values.
pub type Document = HashMap<String, Datum>;

/// Predicate is a function that takes a document and returns a boolean value.
pub type Predicate = Box<dyn Fn(Document) -> bool + Send + Sync>;

/// Represents a field in an ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByField {
    pub field_name: String,
    pub ascending: bool,
}

impl std::fmt::Display for datum::Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            datum::Value::Bool(v) => write!(f, "{v}"),
            datum::Value::Int(v) => write!(f, "{v}"),
            datum::Value::Float(v) => write!(f, "{v}"),
            datum::Value::String(v) => write!(f, "{v}"),
            datum::Value::Binary(v) => write!(f, "{v:?}"),
            datum::Value::Object(v) => write!(f, "{v}"),
            datum::Value::Array(v) => write!(f, "{v}"),
            datum::Value::Null(_) => write!(f, "NULL"),
        }
    }
}

impl From<Document> for Datum {
    fn from(fields: Document) -> Self {
        Datum {
            value: Some(crate::ast::datum::Value::Object(DatumObject { fields })),
        }
    }
}

impl std::fmt::Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(value) => write!(f, "{value}"),
            None => write!(f, "NULL"),
        }
    }
}

impl std::fmt::Display for DatumObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.fields
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::fmt::Display for DatumArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]",
            self.items
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl From<&DatumObject> for Document {
    fn from(obj: &DatumObject) -> Self {
        obj.fields.clone()
    }
}

impl From<&Datum> for Document {
    fn from(datum: &Datum) -> Self {
        match &datum.value {
            Some(datum::Value::Object(obj)) => obj.fields.clone(),
            _ => HashMap::new(),
        }
    }
}

impl From<&Document> for Datum {
    fn from(doc: &Document) -> Self {
        Datum {
            value: Some(datum::Value::Object(DatumObject {
                fields: doc.clone(),
            })),
        }
    }
}

impl std::fmt::Display for FieldRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.join(&self.separator))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_datum_value_display_bool() {
        let value = datum::Value::Bool(true);
        assert_eq!(format!("{value}"), "true");

        let value = datum::Value::Bool(false);
        assert_eq!(format!("{value}"), "false");
    }

    #[test]
    fn test_datum_value_display_int() {
        let value = datum::Value::Int(42);
        assert_eq!(format!("{value}"), "42");

        let value = datum::Value::Int(-123);
        assert_eq!(format!("{value}"), "-123");

        let value = datum::Value::Int(0);
        assert_eq!(format!("{value}"), "0");
    }

    #[test]
    fn test_datum_value_display_float() {
        let value = datum::Value::Float(3.15);
        assert_eq!(format!("{value}"), "3.15");

        let value = datum::Value::Float(-2.5);
        assert_eq!(format!("{value}"), "-2.5");

        let value = datum::Value::Float(0.0);
        assert_eq!(format!("{value}"), "0");
    }

    #[test]
    fn test_datum_value_display_string() {
        let value = datum::Value::String("hello".to_string());
        assert_eq!(format!("{value}"), "hello");

        let value = datum::Value::String("".to_string());
        assert_eq!(format!("{value}"), "");

        let value = datum::Value::String("multi word string".to_string());
        assert_eq!(format!("{value}"), "multi word string");
    }

    #[test]
    fn test_datum_value_display_binary() {
        let value = datum::Value::Binary(vec![1, 2, 3]);
        assert_eq!(format!("{value}"), "[1, 2, 3]");

        let value = datum::Value::Binary(vec![]);
        assert_eq!(format!("{value}"), "[]");

        let value = datum::Value::Binary(vec![255, 0, 128]);
        assert_eq!(format!("{value}"), "[255, 0, 128]");
    }

    #[test]
    fn test_datum_value_display_object() {
        let mut fields = HashMap::new();
        fields.insert(
            "name".to_string(),
            Datum {
                value: Some(datum::Value::String("John".to_string())),
            },
        );
        fields.insert(
            "age".to_string(),
            Datum {
                value: Some(datum::Value::Int(30)),
            },
        );

        let object = DatumObject { fields };

        let value = datum::Value::Object(object);
        let result = format!("{value}");

        // Since HashMap iteration order is not guaranteed, check that both possible orders are valid
        assert!(result == "{name: John, age: 30}" || result == "{age: 30, name: John}");
    }

    #[test]
    fn test_datum_value_display_array() {
        let items = vec![
            Datum {
                value: Some(datum::Value::String("first".to_string())),
            },
            Datum {
                value: Some(datum::Value::Int(42)),
            },
            Datum {
                value: Some(datum::Value::Bool(true)),
            },
        ];

        let array = DatumArray {
            items,
            element_type: "mixed".to_string(),
        };

        let value = datum::Value::Array(array);
        assert_eq!(format!("{value}"), "[first, 42, true]");
    }

    #[test]
    fn test_datum_value_display_null() {
        let value = datum::Value::Null(NullValue::NullValue.into());
        assert_eq!(format!("{value}"), "NULL");
    }

    #[test]
    fn test_datum_display_with_value() {
        let datum = Datum {
            value: Some(datum::Value::String("test".to_string())),
        };
        assert_eq!(format!("{datum}"), "test");

        let datum = Datum {
            value: Some(datum::Value::Int(123)),
        };
        assert_eq!(format!("{datum}"), "123");

        let datum = Datum {
            value: Some(datum::Value::Bool(false)),
        };
        assert_eq!(format!("{datum}"), "false");
    }

    #[test]
    fn test_datum_display_none() {
        let datum = Datum { value: None };
        assert_eq!(format!("{datum}"), "NULL");
    }

    #[test]
    fn test_datum_object_display_empty() {
        let object = DatumObject {
            fields: HashMap::new(),
        };
        assert_eq!(format!("{object}"), "{}");
    }

    #[test]
    fn test_datum_object_display_single_field() {
        let mut fields = HashMap::new();
        fields.insert(
            "key".to_string(),
            Datum {
                value: Some(datum::Value::String("value".to_string())),
            },
        );

        let object = DatumObject { fields };
        assert_eq!(format!("{object}"), "{key: value}");
    }

    #[test]
    fn test_datum_object_display_multiple_fields() {
        let mut fields = HashMap::new();
        fields.insert(
            "str_field".to_string(),
            Datum {
                value: Some(datum::Value::String("text".to_string())),
            },
        );
        fields.insert(
            "num_field".to_string(),
            Datum {
                value: Some(datum::Value::Int(42)),
            },
        );
        fields.insert(
            "bool_field".to_string(),
            Datum {
                value: Some(datum::Value::Bool(true)),
            },
        );

        let object = DatumObject { fields };

        let result = format!("{object}");

        // Check that the result contains all expected key-value pairs
        assert!(result.starts_with('{'));
        assert!(result.ends_with('}'));
        assert!(result.contains("str_field: text"));
        assert!(result.contains("num_field: 42"));
        assert!(result.contains("bool_field: true"));
    }

    #[test]
    fn test_datum_object_display_nested() {
        let mut inner_fields = HashMap::new();
        inner_fields.insert(
            "inner".to_string(),
            Datum {
                value: Some(datum::Value::String("nested".to_string())),
            },
        );

        let inner_object = DatumObject {
            fields: inner_fields,
        };

        let mut outer_fields = HashMap::new();
        outer_fields.insert(
            "object".to_string(),
            Datum {
                value: Some(datum::Value::Object(inner_object)),
            },
        );

        let outer_object = DatumObject {
            fields: outer_fields,
        };

        assert_eq!(format!("{outer_object}"), "{object: {inner: nested}}");
    }

    #[test]
    fn test_datum_array_display_empty() {
        let array = DatumArray {
            items: vec![],
            element_type: "string".to_string(),
        };
        assert_eq!(format!("{array}"), "[]");
    }

    #[test]
    fn test_datum_array_display_single_item() {
        let items = vec![Datum {
            value: Some(datum::Value::String("only".to_string())),
        }];

        let array = DatumArray {
            items,
            element_type: "string".to_string(),
        };
        assert_eq!(format!("{array}"), "[only]");
    }

    #[test]
    fn test_datum_array_display_multiple_items() {
        let items = vec![
            Datum {
                value: Some(datum::Value::String("first".to_string())),
            },
            Datum {
                value: Some(datum::Value::String("second".to_string())),
            },
            Datum {
                value: Some(datum::Value::String("third".to_string())),
            },
        ];

        let array = DatumArray {
            items,
            element_type: "string".to_string(),
        };
        assert_eq!(format!("{array}"), "[first, second, third]");
    }

    #[test]
    fn test_datum_array_display_mixed_types() {
        let items = vec![
            Datum {
                value: Some(datum::Value::String("text".to_string())),
            },
            Datum {
                value: Some(datum::Value::Int(123)),
            },
            Datum {
                value: Some(datum::Value::Bool(true)),
            },
            Datum {
                value: Some(datum::Value::Float(3.15)),
            },
            Datum { value: None },
        ];

        let array = DatumArray {
            items,
            element_type: "mixed".to_string(),
        };
        assert_eq!(format!("{array}"), "[text, 123, true, 3.15, NULL]");
    }

    #[test]
    fn test_datum_array_display_nested() {
        let inner_items = vec![
            Datum {
                value: Some(datum::Value::Int(1)),
            },
            Datum {
                value: Some(datum::Value::Int(2)),
            },
        ];

        let inner_array = DatumArray {
            items: inner_items,
            element_type: "int".to_string(),
        };

        let outer_items = vec![
            Datum {
                value: Some(datum::Value::String("before".to_string())),
            },
            Datum {
                value: Some(datum::Value::Array(inner_array)),
            },
            Datum {
                value: Some(datum::Value::String("after".to_string())),
            },
        ];

        let outer_array = DatumArray {
            items: outer_items,
            element_type: "mixed".to_string(),
        };

        assert_eq!(format!("{outer_array}"), "[before, [1, 2], after]");
    }

    #[test]
    fn test_complex_nested_structure() {
        // Create a complex nested structure to test all display implementations
        let mut user_fields = HashMap::new();
        user_fields.insert(
            "name".to_string(),
            Datum {
                value: Some(datum::Value::String("Alice".to_string())),
            },
        );
        user_fields.insert(
            "age".to_string(),
            Datum {
                value: Some(datum::Value::Int(25)),
            },
        );

        let user_object = DatumObject {
            fields: user_fields,
        };

        let tags_array = DatumArray {
            items: vec![
                Datum {
                    value: Some(datum::Value::String("admin".to_string())),
                },
                Datum {
                    value: Some(datum::Value::String("user".to_string())),
                },
            ],
            element_type: "string".to_string(),
        };

        let mut root_fields = HashMap::new();
        root_fields.insert(
            "user".to_string(),
            Datum {
                value: Some(datum::Value::Object(user_object)),
            },
        );
        root_fields.insert(
            "tags".to_string(),
            Datum {
                value: Some(datum::Value::Array(tags_array)),
            },
        );
        root_fields.insert(
            "active".to_string(),
            Datum {
                value: Some(datum::Value::Bool(true)),
            },
        );

        let root_object = DatumObject {
            fields: root_fields,
        };

        let result = format!("{root_object}");

        // Check that all components are present in the output
        assert!(result.contains("Alice"));
        assert!(result.contains("25"));
        assert!(result.contains("admin"));
        assert!(result.contains("user"));
        assert!(result.contains("true"));
    }

    #[test]
    fn test_datum_with_null_values() {
        let mut fields = HashMap::new();
        fields.insert(
            "null_field".to_string(),
            Datum {
                value: Some(datum::Value::Null(NullValue::NullValue.into())),
            },
        );
        fields.insert("none_field".to_string(), Datum { value: None });

        let object = DatumObject { fields };

        let result = format!("{object}");
        // Both should display as NULL
        assert!(result.contains("null_field: NULL"));
        assert!(result.contains("none_field: NULL"));
    }

    #[test]
    fn test_special_characters_in_strings() {
        let value = datum::Value::String("Hello\nWorld\t\"Test\"".to_string());
        assert_eq!(format!("{value}"), "Hello\nWorld\t\"Test\"");

        let value = datum::Value::String("UTF-8: 中文 🚀".to_string());
        assert_eq!(format!("{value}"), "UTF-8: 中文 🚀");
    }

    #[test]
    fn test_large_numbers() {
        let value = datum::Value::Int(i64::MAX);
        assert_eq!(format!("{value}"), "9223372036854775807");

        let value = datum::Value::Int(i64::MIN);
        assert_eq!(format!("{value}"), "-9223372036854775808");

        let value = datum::Value::Float(f64::MAX);
        assert_eq!(
            format!("{value}"),
            "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        );

        let value = datum::Value::Float(f64::MIN);
        assert_eq!(
            format!("{value}"),
            "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_field_ref_to_string() {
        assert_eq!(
            FieldRef {
                path: vec!["id".to_string()],
                separator: ".".to_string()
            }
            .to_string(),
            "id"
        );
        assert_eq!(
            FieldRef {
                path: vec!["table".to_string(), "id".to_string()],
                separator: ".".to_string()
            }
            .to_string(),
            "table.id"
        );
        assert_eq!(
            FieldRef {
                path: vec!["table".to_string(), "column".to_string()],
                separator: "::".to_string()
            }
            .to_string(),
            "table::column"
        );
    }
}
