use crate::ast::{Datum, DatumObject, Document, FieldRef, datum, query_result};
use crate::evaluator::error::EvalError;

/// Extract a field value from a datum using field name
pub fn extract_field_value(datum: &Datum, field: &str) -> Datum {
    match &datum.value {
        Some(datum::Value::Object(obj)) => obj
            .fields
            .get(field)
            .cloned()
            .unwrap_or(Datum { value: None }),
        _ => Datum { value: None },
    }
}

/// Extract a field value from a datum using the field reference path
pub fn extract_field_from_ref(datum: &Datum, field_ref: &FieldRef) -> Datum {
    if field_ref.path.is_empty() {
        return Datum { value: None };
    }

    let mut result = datum.clone();
    for path_segment in &field_ref.path {
        result = extract_field_value(&result, path_segment);
        if result.value.is_none() {
            break;
        }
    }

    result
}

/// Exclude fields from an DatumObject based on field references
pub fn exclude_field_refs(
    datum: &Datum,
    field_refs: &[FieldRef],
    output: &mut Document,
    path: Vec<String>,
) {
    let obj = match &datum.value {
        Some(datum::Value::Object(obj)) => obj,
        _ => return,
    };

    for (key, value) in &obj.fields {
        let mut path = path.clone();
        path.push(key.clone());

        // Check if the current path matches any field ref
        let should_exclude = field_refs.iter().any(|fr| fr.path == path);

        if should_exclude {
            continue;
        }

        if matches!(value.value, Some(datum::Value::Object(_))) {
            let mut nested = Document::new();
            exclude_field_refs(value, field_refs, &mut nested, path);
            output.insert(key.clone(), nested.into());
        } else {
            output.insert(key.clone(), value.clone());
        }
    }
}

/// Insert a field into a document by reference, preserving nested structure.
pub fn insert_field_by_ref(doc: &mut Document, field_ref: &FieldRef, value: Datum) {
    let mut current = doc;

    for (i, key) in field_ref.path.iter().enumerate() {
        if i == field_ref.path.len() - 1 {
            current.insert(key.clone(), value);
            return;
        }

        // Ensure there's an object at this level
        let is_object = matches!(
            current.get_mut(key),
            Some(Datum {
                value: Some(datum::Value::Object(_)),
                ..
            })
        );

        if !is_object {
            current.insert(
                key.clone(),
                Datum {
                    value: Some(datum::Value::Object(DatumObject {
                        fields: Document::new(),
                    })),
                },
            );
        }

        // Safe to unwrap as we just inserted if it didn't exist
        current = match current.get_mut(key) {
            Some(Datum {
                value: Some(datum::Value::Object(obj)),
                ..
            }) => &mut obj.fields,
            _ => unreachable!("Should always be an object here"),
        };
    }
}

/// Compare two datum values with proper type handling
pub fn compare_values(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    match (&a.value, &b.value) {
        (Some(datum::Value::String(a)), Some(datum::Value::String(b))) => a.cmp(b),
        (Some(datum::Value::Int(a)), Some(datum::Value::Int(b))) => a.cmp(b),
        (Some(datum::Value::Float(a)), Some(datum::Value::Float(b))) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Some(datum::Value::Int(a)), Some(datum::Value::Float(b))) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(datum::Value::Float(a)), Some(datum::Value::Int(b))) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(datum::Value::Bool(a)), Some(datum::Value::Bool(b))) => a.cmp(b),
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

/// Convert a datum to boolean value
pub fn datum_to_bool(datum: &Datum) -> bool {
    match &datum.value {
        Some(datum::Value::Bool(b)) => *b,
        Some(datum::Value::String(s)) => !s.is_empty(),
        Some(datum::Value::Int(i)) => *i != 0,
        Some(datum::Value::Float(f)) => *f != 0.0,
        Some(datum::Value::Object(_)) => true,
        Some(datum::Value::Array(arr)) => !arr.items.is_empty(),
        Some(datum::Value::Binary(b)) => !b.is_empty(),
        Some(datum::Value::Null(_)) => false,
        None => false,
    }
}

/// Extract the document key from a datum (expects a string type)
pub fn extract_document_key(datum: &Datum) -> Result<String, EvalError> {
    match &datum.value {
        Some(datum::Value::String(s)) => Ok(s.clone()),
        _ => Err(EvalError::InvalidKeyType),
    }
}

/// Check if two datums are equal
pub fn datums_equal(a: &Datum, b: &Datum) -> bool {
    match (&a.value, &b.value) {
        (Some(datum::Value::String(a)), Some(datum::Value::String(b))) => a == b,
        (Some(datum::Value::Int(a)), Some(datum::Value::Int(b))) => a == b,
        (Some(datum::Value::Float(a)), Some(datum::Value::Float(b))) => {
            (a - b).abs() < f64::EPSILON
        }
        (Some(datum::Value::Int(a)), Some(datum::Value::Float(b))) => {
            (*a as f64 - b).abs() < f64::EPSILON
        }
        (Some(datum::Value::Float(a)), Some(datum::Value::Int(b))) => {
            (a - *b as f64).abs() < f64::EPSILON
        }
        (Some(datum::Value::Bool(a)), Some(datum::Value::Bool(b))) => a == b,
        (None, None) => true,
        _ => false,
    }
}

/// Create a string datum
pub fn string_datum(s: String) -> Datum {
    Datum {
        value: Some(datum::Value::String(s)),
    }
}

/// Create a boolean datum
pub fn bool_datum(b: bool) -> Datum {
    Datum {
        value: Some(datum::Value::Bool(b)),
    }
}

pub fn is_single_doc_source(source_result: &query_result::Result) -> bool {
    matches!(source_result, query_result::Result::Get(_))
}
