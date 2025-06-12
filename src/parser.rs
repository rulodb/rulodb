use crate::ast::{Envelope, Query};
use prost::Message;

#[derive(Debug)]
pub enum ParseError {
    InvalidProtobuf(prost::DecodeError),
    MissingField(String),
    InvalidStructure(String),
    UnexpectedValue(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProtobuf(e) => write!(f, "Invalid protobuf: {e}"),
            Self::MissingField(field) => write!(f, "Missing required field: {field}"),
            Self::InvalidStructure(msg) => write!(f, "Invalid structure: {msg}"),
            Self::UnexpectedValue(v) => write!(f, "Unexpected value: {v}"),
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidProtobuf(e) => Some(e),
            _ => None,
        }
    }
}

impl From<prost::DecodeError> for ParseError {
    fn from(error: prost::DecodeError) -> Self {
        Self::InvalidProtobuf(error)
    }
}

pub fn parse_envelope(bytes: &[u8]) -> Result<Envelope, ParseError> {
    Ok(Envelope::decode(bytes)?)
}

pub fn parse_query(bytes: &[u8]) -> Result<Query, ParseError> {
    Ok(Query::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use prost::Message;
    use std::error::Error;

    fn create_test_envelope() -> Envelope {
        Envelope {
            version: ProtocolVersion::Version1.into(),
            query_id: "test-query-123".to_string(),
            r#type: MessageType::Query.into(),
            payload: vec![1, 2, 3, 4],
        }
    }

    fn create_test_query() -> Query {
        Query {
            options: Some(QueryOptions {
                timeout_ms: 5000,
                explain: false,
            }),
            cursor: None,
            kind: Some(query::Kind::DatabaseList(DatabaseList {})),
        }
    }

    #[test]
    fn test_parse_envelope_success() {
        let envelope = create_test_envelope();
        let encoded = envelope.encode_to_vec();

        let result = parse_envelope(&encoded);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.version, ProtocolVersion::Version1 as i32);
        assert_eq!(parsed.query_id, "test-query-123");
        assert_eq!(parsed.r#type, MessageType::Query as i32);
        assert_eq!(parsed.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_parse_envelope_invalid_protobuf() {
        let invalid_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF];

        let result = parse_envelope(&invalid_bytes);
        assert!(result.is_err());

        match result.unwrap_err() {
            ParseError::InvalidProtobuf(_) => (),
            _ => panic!("Expected InvalidProtobuf error"),
        }
    }

    #[test]
    fn test_parse_envelope_empty_bytes() {
        let empty_bytes = vec![];

        let result = parse_envelope(&empty_bytes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_query_success() {
        let query = create_test_query();
        let encoded = query.encode_to_vec();

        let result = parse_query(&encoded);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(parsed.options.is_some());
        assert_eq!(parsed.options.unwrap().timeout_ms, 5000);
        assert!(matches!(parsed.kind, Some(query::Kind::DatabaseList(_))));
    }

    #[test]
    fn test_parse_query_invalid_protobuf() {
        let invalid_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF];

        let result = parse_query(&invalid_bytes);
        assert!(result.is_err());

        match result.unwrap_err() {
            ParseError::InvalidProtobuf(_) => (),
            _ => panic!("Expected InvalidProtobuf error"),
        }
    }

    #[test]
    fn test_parse_query_empty_bytes() {
        let empty_bytes = vec![];

        let result = parse_query(&empty_bytes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_error_display() {
        let decode_error = prost::DecodeError::new("test decode error");
        let parse_error = ParseError::InvalidProtobuf(decode_error);
        let display_string = format!("{}", parse_error);
        assert!(display_string.contains("Invalid protobuf"));
        assert!(display_string.contains("test decode error"));

        let missing_field_error = ParseError::MissingField("test_field".to_string());
        let display_string = format!("{}", missing_field_error);
        assert_eq!(display_string, "Missing required field: test_field");

        let invalid_structure_error = ParseError::InvalidStructure("test structure".to_string());
        let display_string = format!("{}", invalid_structure_error);
        assert_eq!(display_string, "Invalid structure: test structure");

        let unexpected_value_error = ParseError::UnexpectedValue("test value".to_string());
        let display_string = format!("{}", unexpected_value_error);
        assert_eq!(display_string, "Unexpected value: test value");
    }

    #[test]
    fn test_parse_error_source() {
        let decode_error = prost::DecodeError::new("test decode error");
        let parse_error = ParseError::InvalidProtobuf(decode_error);
        assert!(parse_error.source().is_some());

        let missing_field_error = ParseError::MissingField("test_field".to_string());
        assert!(missing_field_error.source().is_none());

        let invalid_structure_error = ParseError::InvalidStructure("test structure".to_string());
        assert!(invalid_structure_error.source().is_none());

        let unexpected_value_error = ParseError::UnexpectedValue("test value".to_string());
        assert!(unexpected_value_error.source().is_none());
    }

    #[test]
    fn test_parse_error_from_decode_error() {
        let decode_error = prost::DecodeError::new("test decode error");
        let parse_error: ParseError = decode_error.into();

        match parse_error {
            ParseError::InvalidProtobuf(_) => (),
            _ => panic!("Expected InvalidProtobuf error"),
        }
    }

    #[test]
    fn test_parse_error_debug() {
        let decode_error = prost::DecodeError::new("test decode error");
        let parse_error = ParseError::InvalidProtobuf(decode_error);
        let debug_string = format!("{:?}", parse_error);
        assert!(debug_string.contains("InvalidProtobuf"));

        let missing_field_error = ParseError::MissingField("test_field".to_string());
        let debug_string = format!("{:?}", missing_field_error);
        assert!(debug_string.contains("MissingField"));
        assert!(debug_string.contains("test_field"));

        let invalid_structure_error = ParseError::InvalidStructure("test structure".to_string());
        let debug_string = format!("{:?}", invalid_structure_error);
        assert!(debug_string.contains("InvalidStructure"));
        assert!(debug_string.contains("test structure"));

        let unexpected_value_error = ParseError::UnexpectedValue("test value".to_string());
        let debug_string = format!("{:?}", unexpected_value_error);
        assert!(debug_string.contains("UnexpectedValue"));
        assert!(debug_string.contains("test value"));
    }

    #[test]
    fn test_parse_complex_query() {
        let table_ref = TableRef {
            database: Some(DatabaseRef {
                name: "test_db".to_string(),
            }),
            name: "test_table".to_string(),
        };

        let table_query = Query {
            options: Some(QueryOptions {
                timeout_ms: 10000,
                explain: true,
            }),
            cursor: Some(Cursor {
                start_key: Some("start".to_string()),
                batch_size: Some(100),
                sort: Some(SortOptions {
                    fields: vec![SortField {
                        field_name: "id".to_string(),
                        direction: SortDirection::Asc.into(),
                    }],
                }),
            }),
            kind: Some(query::Kind::Table(Table {
                table: Some(table_ref),
            })),
        };

        let encoded = table_query.encode_to_vec();
        let result = parse_query(&encoded);

        assert!(result.is_ok());
        let parsed = result.unwrap();

        assert!(parsed.options.is_some());
        assert_eq!(parsed.options.unwrap().timeout_ms, 10000);
        assert!(parsed.cursor.is_some());

        let cursor = parsed.cursor.unwrap();
        assert_eq!(cursor.start_key, Some("start".to_string()));
        assert_eq!(cursor.batch_size, Some(100));

        match parsed.kind {
            Some(query::Kind::Table(table)) => {
                assert!(table.table.is_some());
                let table_ref = table.table.unwrap();
                assert_eq!(table_ref.name, "test_table");
                assert!(table_ref.database.is_some());
                assert_eq!(table_ref.database.unwrap().name, "test_db");
            }
            _ => panic!("Expected Table query kind"),
        }
    }

    #[test]
    fn test_parse_envelope_with_complex_payload() {
        let query = create_test_query();
        let query_payload = query.encode_to_vec();

        let envelope = Envelope {
            version: ProtocolVersion::Version1.into(),
            query_id: "complex-query-456".to_string(),
            r#type: MessageType::Query.into(),
            payload: query_payload,
        };

        let encoded = envelope.encode_to_vec();
        let result = parse_envelope(&encoded);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query_id, "complex-query-456");

        let query_result = parse_query(&parsed.payload);
        assert!(query_result.is_ok());
    }

    #[test]
    fn test_parse_error_variants_creation() {
        // Test creating each ParseError variant directly
        let missing_field = ParseError::MissingField("test".to_string());
        assert!(matches!(missing_field, ParseError::MissingField(_)));

        let invalid_structure = ParseError::InvalidStructure("test".to_string());
        assert!(matches!(invalid_structure, ParseError::InvalidStructure(_)));

        let unexpected_value = ParseError::UnexpectedValue("test".to_string());
        assert!(matches!(unexpected_value, ParseError::UnexpectedValue(_)));

        let decode_error = prost::DecodeError::new("test");
        let invalid_protobuf = ParseError::InvalidProtobuf(decode_error);
        assert!(matches!(invalid_protobuf, ParseError::InvalidProtobuf(_)));
    }

    #[test]
    fn test_parse_envelope_minimal() {
        // Test parsing a minimal envelope with default values
        let envelope = Envelope {
            version: 0,
            query_id: String::new(),
            r#type: 0,
            payload: vec![],
        };

        let encoded = envelope.encode_to_vec();
        let result = parse_envelope(&encoded);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.version, 0);
        assert_eq!(parsed.query_id, "");
        assert_eq!(parsed.r#type, 0);
        assert_eq!(parsed.payload, vec![]);
    }

    #[test]
    fn test_parse_query_minimal() {
        // Test parsing a minimal query with default values
        let query = Query {
            options: None,
            cursor: None,
            kind: None,
        };

        let encoded = query.encode_to_vec();
        let result = parse_query(&encoded);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.options.is_none());
        assert!(parsed.cursor.is_none());
        assert!(parsed.kind.is_none());
    }

    #[test]
    fn test_parse_error_chain() {
        let original_error = prost::DecodeError::new("original error message");
        let parse_error = ParseError::from(original_error);

        match parse_error {
            ParseError::InvalidProtobuf(ref e) => {
                assert!(e.to_string().contains("original error message"));
            }
            _ => panic!("Expected InvalidProtobuf variant"),
        }

        let source = parse_error.source();
        assert!(source.is_some());
        assert!(
            source
                .unwrap()
                .to_string()
                .contains("original error message")
        );
    }
}
