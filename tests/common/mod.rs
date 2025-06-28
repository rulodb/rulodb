use byteorder::{BigEndian, WriteBytesExt};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rulodb::ast::proto;

/// Helper function to generate unique names with timestamp
#[allow(dead_code)]
pub fn generate_unique_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let random_suffix = fastrand::u32(1000..9999);
    format!("{prefix}_{timestamp}{random_suffix}")
}

/// Helper function to create a database list query
#[allow(dead_code)]
pub fn create_database_list_query() -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::DatabaseList(proto::DatabaseList {})),
    }
}

/// Helper function to create a database create query
#[allow(dead_code)]
pub fn create_database_create_query(database_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::DatabaseCreate(proto::DatabaseCreate {
            name: database_name.to_string(),
        })),
    }
}

/// Helper function to create a database drop query
#[allow(dead_code)]
pub fn create_database_drop_query(database_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::DatabaseDrop(proto::DatabaseDrop {
            name: database_name.to_string(),
        })),
    }
}

/// Helper function to create a table list query
#[allow(dead_code)]
pub fn create_table_list_query(database_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::TableList(proto::TableList {
            database: Some(proto::DatabaseRef {
                name: database_name.to_string(),
            }),
        })),
    }
}

/// Helper function to create a table create query
#[allow(dead_code)]
pub fn create_table_create_query(database_name: &str, table_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::TableCreate(proto::TableCreate {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    }
}

/// Helper function to create a table drop query
#[allow(dead_code)]
pub fn create_table_drop_query(database_name: &str, table_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::TableDrop(proto::TableDrop {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    }
}

/// Helper function to create an envelope from a query
pub fn create_envelope(query_id: &str, query: &proto::Query) -> proto::Envelope {
    let mut query_payload = Vec::new();
    query
        .encode(&mut query_payload)
        .expect("Failed to encode query");

    proto::Envelope {
        version: proto::ProtocolVersion::Version1.into(),
        query_id: query_id.to_string(),
        r#type: proto::MessageType::Query.into(),
        payload: query_payload,
    }
}

/// Helper function to send an envelope to the server and receive a response
pub async fn send_envelope_to_server(
    stream: &mut TcpStream,
    envelope: &proto::Envelope,
) -> Result<proto::Envelope, Box<dyn std::error::Error + Send + Sync>> {
    // Encode the envelope
    let mut envelope_bytes = Vec::new();
    envelope.encode(&mut envelope_bytes)?;

    // Send length-prefixed message
    let mut message = Vec::new();
    WriteBytesExt::write_u32::<BigEndian>(&mut message, envelope_bytes.len() as u32)?;
    message.extend(envelope_bytes);

    stream.write_all(&message).await?;

    // Read response length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let response_len = u32::from_be_bytes(len_buf) as usize;

    // Read response payload
    let mut response_buf = vec![0u8; response_len];
    stream.read_exact(&mut response_buf).await?;

    // Decode response envelope
    let response_envelope = proto::Envelope::decode(&response_buf[..])?;
    Ok(response_envelope)
}

/// Helper function to decode response envelope payload
pub fn decode_response_payload(
    envelope: &proto::Envelope,
) -> Result<proto::Datum, Box<dyn std::error::Error + Send + Sync>> {
    match proto::MessageType::try_from(envelope.r#type) {
        Ok(proto::MessageType::Response) => {
            // Decode as Response wrapper
            let response = proto::Response::decode(&envelope.payload[..])?;

            match response.result {
                Some(proto::response::Result::Query(query_result)) => match query_result.result {
                    Some(proto::query_result::Result::Get(get_result)) => {
                        match get_result.document {
                            Some(document) => Ok(document),
                            None => Ok(proto::Datum {
                                value: Some(proto::datum::Value::Null(
                                    proto::NullValue::NullValue.into(),
                                )),
                            }),
                        }
                    }
                    Some(proto::query_result::Result::GetAll(get_all_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Array(proto::DatumArray {
                            items: get_all_result.documents,
                            element_type: String::new(),
                        })),
                    }),
                    Some(proto::query_result::Result::Table(table_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Array(proto::DatumArray {
                            items: table_result.documents,
                            element_type: String::new(),
                        })),
                    }),
                    Some(proto::query_result::Result::Filter(filter_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Array(proto::DatumArray {
                            items: filter_result.documents,
                            element_type: String::new(),
                        })),
                    }),
                    Some(proto::query_result::Result::OrderBy(order_by_result)) => {
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Array(proto::DatumArray {
                                items: order_by_result.documents,
                                element_type: String::new(),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::Limit(limit_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Array(proto::DatumArray {
                            items: limit_result.documents,
                            element_type: String::new(),
                        })),
                    }),
                    Some(proto::query_result::Result::Skip(skip_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Array(proto::DatumArray {
                            items: skip_result.documents,
                            element_type: String::new(),
                        })),
                    }),
                    Some(proto::query_result::Result::Pluck(pluck_result)) => {
                        match &pluck_result.result {
                            Some(proto::pluck_result::Result::Document(doc)) => Ok(doc.clone()),
                            Some(proto::pluck_result::Result::Collection(collection)) => {
                                Ok(proto::Datum {
                                    value: Some(proto::datum::Value::Array(proto::DatumArray {
                                        items: collection.documents.clone(),
                                        element_type: String::new(),
                                    })),
                                })
                            }
                            None => Ok(proto::Datum {
                                value: Some(proto::datum::Value::Array(proto::DatumArray {
                                    items: vec![],
                                    element_type: String::new(),
                                })),
                            }),
                        }
                    }
                    Some(proto::query_result::Result::Count(count_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Int(count_result.count as i64)),
                    }),
                    Some(proto::query_result::Result::Insert(insert_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Object(proto::DatumObject {
                            fields: std::collections::HashMap::from([
                                (
                                    "inserted".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            insert_result.inserted as i64,
                                        )),
                                    },
                                ),
                                (
                                    "generated_keys".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Array(
                                            proto::DatumArray {
                                                items: insert_result.generated_keys,
                                                element_type: String::new(),
                                            },
                                        )),
                                    },
                                ),
                            ]),
                        })),
                    }),
                    Some(proto::query_result::Result::Delete(delete_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Object(proto::DatumObject {
                            fields: std::collections::HashMap::from([(
                                "deleted".to_string(),
                                proto::Datum {
                                    value: Some(proto::datum::Value::Int(
                                        delete_result.deleted as i64,
                                    )),
                                },
                            )]),
                        })),
                    }),
                    Some(proto::query_result::Result::Update(update_result)) => Ok(proto::Datum {
                        value: Some(proto::datum::Value::Object(proto::DatumObject {
                            fields: std::collections::HashMap::from([(
                                "updated".to_string(),
                                proto::Datum {
                                    value: Some(proto::datum::Value::Int(
                                        update_result.updated as i64,
                                    )),
                                },
                            )]),
                        })),
                    }),
                    Some(proto::query_result::Result::DatabaseCreate(db_create_result)) => {
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Object(proto::DatumObject {
                                fields: std::collections::HashMap::from([(
                                    "created".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            db_create_result.created as i64,
                                        )),
                                    },
                                )]),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::DatabaseDrop(db_drop_result)) => {
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Object(proto::DatumObject {
                                fields: std::collections::HashMap::from([(
                                    "dropped".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            db_drop_result.dropped as i64,
                                        )),
                                    },
                                )]),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::DatabaseList(db_list_result)) => {
                        let items: Vec<proto::Datum> = db_list_result
                            .databases
                            .iter()
                            .map(|db_name| proto::Datum {
                                value: Some(proto::datum::Value::String(db_name.clone())),
                            })
                            .collect();
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Array(proto::DatumArray {
                                items,
                                element_type: "string".to_string(),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::TableCreate(table_create_result)) => {
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Object(proto::DatumObject {
                                fields: std::collections::HashMap::from([(
                                    "created".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            table_create_result.created as i64,
                                        )),
                                    },
                                )]),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::TableDrop(table_drop_result)) => {
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Object(proto::DatumObject {
                                fields: std::collections::HashMap::from([(
                                    "dropped".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            table_drop_result.dropped as i64,
                                        )),
                                    },
                                )]),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::TableList(table_list_result)) => {
                        let items: Vec<proto::Datum> = table_list_result
                            .tables
                            .iter()
                            .map(|table_name| proto::Datum {
                                value: Some(proto::datum::Value::String(table_name.clone())),
                            })
                            .collect();
                        Ok(proto::Datum {
                            value: Some(proto::datum::Value::Array(proto::DatumArray {
                                items,
                                element_type: "string".to_string(),
                            })),
                        })
                    }
                    Some(proto::query_result::Result::Literal(literal_result)) => literal_result
                        .value
                        .ok_or("Missing value in literal result".into()),
                    None => Err("No query result found".into()),
                },
                Some(proto::response::Result::Error(error_info)) => Err(format!(
                    "Server error: {} - {}",
                    error_info.code, error_info.message
                )
                .into()),
                Some(proto::response::Result::Pong(pong)) => {
                    // Convert pong to datum
                    Ok(proto::Datum {
                        value: Some(proto::datum::Value::Object(proto::DatumObject {
                            fields: std::collections::HashMap::from([
                                (
                                    "timestamp".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            pong.timestamp as i64,
                                        )),
                                    },
                                ),
                                (
                                    "latency_ms".to_string(),
                                    proto::Datum {
                                        value: Some(proto::datum::Value::Int(
                                            pong.latency_ms as i64,
                                        )),
                                    },
                                ),
                            ]),
                        })),
                    })
                }
                Some(proto::response::Result::Plan(_)) => {
                    Err("Query plan responses not supported in this helper".into())
                }
                Some(proto::response::Result::AuthResult(_)) => {
                    Err("Auth result responses not supported in this helper".into())
                }
                None => Err("No result in response".into()),
            }
        }
        Ok(proto::MessageType::Error) => {
            // Legacy error handling - this shouldn't happen with new server
            let error_info = proto::ErrorInfo::decode(&envelope.payload[..])?;
            Err(format!("Server error: {} - {}", error_info.code, error_info.message).into())
        }
        _ => Err("Unexpected message type in response".into()),
    }
}

/// Helper function to connect to the test server
pub async fn connect_to_server() -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = "127.0.0.1:6090";
    let stream = TcpStream::connect(server_addr).await?;
    Ok(stream)
}

/// Helper function to create an insert query
#[allow(dead_code)]
pub fn create_insert_query(
    database_name: &str,
    table_name: &str,
    documents: Vec<proto::DatumObject>,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Insert(Box::new(proto::Insert {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            documents,
        }))),
    }
}

/// Helper function to create a table query (for querying all documents in a table)
#[allow(dead_code)]
pub fn create_table_query(database_name: &str, table_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Table(proto::Table {
            table: Some(proto::TableRef {
                database: Some(proto::DatabaseRef {
                    name: database_name.to_string(),
                }),
                name: table_name.to_string(),
            }),
        })),
    }
}

/// Helper function to create a get query
#[allow(dead_code)]
pub fn create_get_query(database_name: &str, table_name: &str, key: proto::Datum) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Get(Box::new(proto::Get {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            key: Some(key),
        }))),
    }
}

/// Helper function to create a DatumObject from key-value pairs
#[allow(dead_code)]
pub fn create_datum_object(fields: Vec<(&str, proto::Datum)>) -> proto::DatumObject {
    let mut field_map = std::collections::HashMap::new();
    for (key, value) in fields {
        field_map.insert(key.to_string(), value);
    }
    proto::DatumObject { fields: field_map }
}

/// Helper function to create a string Datum
#[allow(dead_code)]
pub fn create_string_datum(value: &str) -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::String(value.to_string())),
    }
}

/// Helper function to create an integer Datum
#[allow(dead_code)]
pub fn create_int_datum(value: i64) -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::Int(value)),
    }
}

/// Helper function to create a boolean Datum
#[allow(dead_code)]
pub fn create_bool_datum(value: bool) -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::Bool(value)),
    }
}

/// Helper function to create a float Datum
#[allow(dead_code)]
pub fn create_float_datum(value: f64) -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::Float(value)),
    }
}

/// Helper function to create a delete query
#[allow(dead_code)]
pub fn create_delete_query(database_name: &str, table_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Delete(Box::new(proto::Delete {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
        }))),
    }
}

/// Helper function to create an update query
#[allow(dead_code)]
pub fn create_update_query(
    database_name: &str,
    table_name: &str,
    patch: proto::DatumObject,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Update(Box::new(proto::Update {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            patch: Some(patch),
        }))),
    }
}

/// Helper function to create a get_all query
#[allow(dead_code)]
pub fn create_get_all_query(
    database_name: &str,
    table_name: &str,
    keys: Vec<proto::Datum>,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::GetAll(Box::new(proto::GetAll {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            keys,
        }))),
    }
}

/// Helper function to create a filter query
#[allow(dead_code)]
pub fn create_filter_query(
    database_name: &str,
    table_name: &str,
    predicate: proto::Expression,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Filter(Box::new(proto::Filter {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            predicate: Some(Box::new(predicate)),
        }))),
    }
}

/// Helper function to create an order by query
#[allow(dead_code)]
pub fn create_order_by_query(
    database_name: &str,
    table_name: &str,
    fields: Vec<proto::SortField>,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::OrderBy(Box::new(proto::OrderBy {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            fields,
        }))),
    }
}

/// Helper function to create a limit query
#[allow(dead_code)]
pub fn create_limit_query(database_name: &str, table_name: &str, count: u32) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Limit(Box::new(proto::Limit {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            count,
        }))),
    }
}

/// Helper function to create a skip query
#[allow(dead_code)]
pub fn create_skip_query(database_name: &str, table_name: &str, count: u32) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Skip(Box::new(proto::Skip {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            count,
        }))),
    }
}

/// Helper function to create a count query
#[allow(dead_code)]
pub fn create_count_query(database_name: &str, table_name: &str) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Count(Box::new(proto::Count {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
        }))),
    }
}

/// Helper function to create a binary expression
#[allow(dead_code)]
pub fn create_binary_expression(
    operator: proto::binary_op::Operator,
    left: proto::Expression,
    right: proto::Expression,
) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Binary(Box::new(proto::BinaryOp {
            op: operator.into(),
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        }))),
    }
}

/// Helper function to create a pluck query
#[allow(dead_code)]
pub fn create_pluck_query(
    database_name: &str,
    table_name: &str,
    fields: Vec<proto::FieldRef>,
) -> proto::Query {
    proto::Query {
        options: Some(proto::QueryOptions {
            timeout_ms: 30000,
            explain: false,
        }),
        cursor: None,
        kind: Some(proto::query::Kind::Pluck(Box::new(proto::Pluck {
            source: Some(Box::new(proto::Query {
                options: None,
                cursor: None,
                kind: Some(proto::query::Kind::Table(proto::Table {
                    table: Some(proto::TableRef {
                        database: Some(proto::DatabaseRef {
                            name: database_name.to_string(),
                        }),
                        name: table_name.to_string(),
                    }),
                })),
            })),
            fields,
        }))),
    }
}

/// Helper function to create a field reference expression
#[allow(dead_code)]
pub fn create_field_expression(field_path: Vec<&str>) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Field(proto::FieldRef {
            path: field_path.iter().map(|s| s.to_string()).collect(),
            separator: ".".to_string(),
        })),
    }
}

/// Helper function to create a literal expression from a Datum
#[allow(dead_code)]
pub fn create_literal_expression(datum: proto::Datum) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Literal(datum)),
    }
}

/// Helper function to create a sort field
#[allow(dead_code)]
pub fn create_sort_field(field_name: &str, direction: proto::SortDirection) -> proto::SortField {
    proto::SortField {
        field_name: field_name.to_string(),
        direction: direction.into(),
    }
}

/// Helper function to create a unary expression
#[allow(dead_code)]
pub fn create_unary_expression(
    operator: proto::unary_op::Operator,
    expr: proto::Expression,
) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Unary(Box::new(proto::UnaryOp {
            op: operator.into(),
            expr: Some(Box::new(expr)),
        }))),
    }
}

/// Helper function to create a match expression
#[allow(dead_code)]
pub fn create_match_expression(
    value: proto::Expression,
    pattern: &str,
    flags: &str,
) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Match(Box::new(proto::MatchExpr {
            value: Some(Box::new(value)),
            pattern: pattern.to_string(),
            flags: flags.to_string(),
        }))),
    }
}

/// Helper function to create a variable expression
#[allow(dead_code)]
pub fn create_variable_expression(name: &str) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Variable(proto::Variable {
            name: name.to_string(),
        })),
    }
}

/// Helper function to create a subquery expression
#[allow(dead_code)]
pub fn create_subquery_expression(query: proto::Query) -> proto::Expression {
    proto::Expression {
        expr: Some(proto::expression::Expr::Subquery(Box::new(query))),
    }
}

/// Helper function to create a null datum
#[allow(dead_code)]
pub fn create_null_datum() -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::Null(
            proto::NullValue::NullValue.into(),
        )),
    }
}

/// Helper function to create a binary datum
#[allow(dead_code)]
pub fn create_binary_datum(data: Vec<u8>) -> proto::Datum {
    proto::Datum {
        value: Some(proto::datum::Value::Binary(data)),
    }
}

/// Helper function to validate basic response structure
pub fn validate_response_envelope(
    response_envelope: &proto::Envelope,
    expected_query_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Validate response correlation
    if response_envelope.query_id != expected_query_id {
        return Err(format!(
            "Response query_id '{}' should match request query_id '{}'",
            response_envelope.query_id, expected_query_id
        )
        .into());
    }

    // Validate response type
    if response_envelope.r#type != proto::MessageType::Response as i32 {
        // Print error details for debugging
        print_error_details(response_envelope);
        return Err("Response should be of type Response, not Error".into());
    }

    Ok(())
}

pub fn print_error_details(response_envelope: &proto::Envelope) {
    if response_envelope.r#type == proto::MessageType::Error as i32 {
        println!("\n=== SERVER ERROR DETAILS ===");
        println!("Query ID: {}", response_envelope.query_id);
        println!("Message Type: {} (Error)", response_envelope.r#type);
        println!("Payload length: {} bytes", response_envelope.payload.len());

        // Try to decode as ErrorInfo directly
        if let Ok(error) = proto::ErrorInfo::decode(&response_envelope.payload[..]) {
            println!("✓ Decoded as ErrorInfo:");
            println!("  Error Code: {}", error.code);
            println!("  Error Message: {}", error.message);
            println!("  Error Type: {}", error.r#type);
            println!("  Line: {}", error.line);
            println!("  Column: {}", error.column);
        } else if let Ok(response) = proto::Response::decode(&response_envelope.payload[..]) {
            println!("✓ Decoded as Response wrapper:");
            match response.result {
                Some(proto::response::Result::Error(error)) => {
                    println!("  Error Code: {}", error.code);
                    println!("  Error Message: {}", error.message);
                    println!("  Error Type: {}", error.r#type);
                    println!("  Line: {}", error.line);
                    println!("  Column: {}", error.column);
                }
                Some(proto::response::Result::Query(query_result)) => {
                    println!("  Unexpected Query result in error response: {query_result:?}");
                }
                Some(proto::response::Result::AuthResult(auth_result)) => {
                    println!("  Unexpected Auth result in error response: {auth_result:?}");
                }
                Some(proto::response::Result::Pong(pong_result)) => {
                    println!("  Unexpected Pong result in error response: {pong_result:?}");
                }
                Some(proto::response::Result::Plan(plan_result)) => {
                    println!("  Plan-based error response:");
                    for (i, node) in plan_result.nodes.iter().enumerate() {
                        println!("    Node {}: operation={}", i, node.operation);
                        if !node.properties.is_empty() {
                            for (key, value) in &node.properties {
                                if !key.is_empty() && !value.is_empty() {
                                    println!("      {key}: {value}");
                                }
                            }
                        }
                    }
                    if let Some(stats) = &plan_result.statistics {
                        println!(
                            "    Statistics: planning_time={}ms, execution_time={}ms",
                            stats.planning_time_ms, stats.execution_time_ms
                        );
                    }
                }
                None => {
                    println!("  Response has no result field");
                }
            }
            if let Some(metadata) = response.metadata {
                println!(
                    "  Metadata: query_id={}, timestamp={}, server_version={}",
                    metadata.query_id, metadata.timestamp, metadata.server_version
                );
            }
        } else {
            println!("✗ Failed to decode error response payload");
            println!(
                "  Raw payload (first 100 bytes): {:?}",
                &response_envelope.payload[..std::cmp::min(100, response_envelope.payload.len())]
            );
        }
        println!("=============================\n");
    }
}

/// Helper function to create a database using evaluator and planner
#[allow(dead_code)]
pub async fn create_database(
    evaluator: &mut rulodb::evaluator::Evaluator,
    planner: &mut rulodb::planner::Planner,
    db_name: &str,
) {
    let query = create_database_create_query(db_name);
    let plan = planner.plan(&query).expect("Failed to plan query");
    let result = evaluator
        .eval(&plan)
        .await
        .expect("Failed to execute query");

    match result.result {
        proto::query_result::Result::DatabaseCreate(create_result) => {
            assert_eq!(create_result.created, 1);
        }
        _ => panic!("Expected DatabaseCreate result"),
    }
}

/// Helper function to create a table using evaluator and planner
#[allow(dead_code)]
pub async fn create_table(
    evaluator: &mut rulodb::evaluator::Evaluator,
    planner: &mut rulodb::planner::Planner,
    db_name: &str,
    table_name: &str,
) {
    let query = create_table_create_query(db_name, table_name);
    let plan = planner.plan(&query).expect("Failed to plan query");
    let result = evaluator
        .eval(&plan)
        .await
        .expect("Failed to execute query");

    match result.result {
        proto::query_result::Result::TableCreate(create_result) => {
            assert_eq!(create_result.created, 1);
        }
        _ => panic!("Expected TableCreate result"),
    }
}
