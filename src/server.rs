use byteorder::{BigEndian, WriteBytesExt};
use prost::Message;
use rulodb::ast::proto;
use rulodb::{Evaluator, Planner, StorageBackend, parse_query};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn start_server(
    db: Arc<dyn StorageBackend + Send + Sync>,
    address: &str,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(address).await?;
    log::info!("server listening on {address}");

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(db, stream).await {
                log::error!("client error: {e}");
            }
        });
    }
}

async fn handle_client(
    db: Arc<dyn StorageBackend + Send + Sync>,
    stream: TcpStream,
) -> anyhow::Result<()> {
    let peer = stream.peer_addr()?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        // Read length prefix (big-endian 4-byte length)
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Read message payload
        let mut buffer = vec![0u8; msg_len];
        if reader.read_exact(&mut buffer).await.is_err() {
            break;
        }

        // Process the envelope message and get response envelope
        let response_envelope = process_envelope_message(db.clone(), &buffer)
            .await
            .unwrap_or_else(|err| {
                log::error!("failed to process envelope from {peer}: {err}");
                // Create error response envelope with default query ID
                create_error_envelope("unknown".to_string(), &err.to_string())
            });

        // Serialize the response envelope
        let mut envelope_payload = Vec::new();
        if let Err(e) = response_envelope.encode(&mut envelope_payload) {
            log::error!("failed to encode response envelope: {e}");
            continue;
        }

        // Frame the response with length prefix
        let mut out: Vec<u8> = Vec::new();
        if let Err(e) =
            WriteBytesExt::write_u32::<BigEndian>(&mut out, u32::try_from(envelope_payload.len())?)
        {
            log::error!("failed to write length prefix: {e}");
            continue;
        }
        out.extend(envelope_payload);

        if let Err(e) = write_half.write_all(&out).await {
            log::error!("failed to write response to {peer}: {e}");
            break;
        }
    }

    Ok(())
}

async fn process_envelope_message(
    db: Arc<dyn StorageBackend + Send + Sync>,
    message: &[u8],
) -> anyhow::Result<proto::Envelope> {
    let envelope = proto::Envelope::decode(message)?;

    match proto::MessageType::try_from(envelope.r#type) {
        Ok(proto::MessageType::Query) => {
            // Process the query from the payload
            match process_query(db, &envelope.payload).await {
                Ok(query_result) => {
                    // Create proper Response wrapper
                    let response = create_response_wrapper(&envelope.query_id, query_result);
                    let mut response_payload = Vec::new();
                    match response.encode(&mut response_payload) {
                        Ok(()) => Ok(proto::Envelope {
                            version: proto::ProtocolVersion::Version1.into(),
                            query_id: envelope.query_id.clone(),
                            r#type: proto::MessageType::Response.into(),
                            payload: response_payload,
                        }),
                        Err(err) => {
                            log::error!("Query processing failed: {err}");
                            Err(err.into())
                        }
                    }
                }
                Err(err) => {
                    log::error!("Query processing failed: {err}");
                    log::error!("Error details: {err}");
                    Ok(create_error_envelope(envelope.query_id, &err.to_string()))
                }
            }
        }
        Ok(
            proto::MessageType::AuthInit
            | proto::MessageType::AuthResponse
            | proto::MessageType::AuthChallenge
            | proto::MessageType::AuthOk,
        ) => Ok(create_error_envelope(
            envelope.query_id,
            "Authentication not implemented",
        )),
        Ok(msg_type) => {
            let error_msg = format!("Unexpected message type from client: {msg_type:?}");
            Ok(create_error_envelope(envelope.query_id, &error_msg))
        }
        Err(_) => {
            let error_msg = format!("Invalid message type: {}", envelope.r#type);
            Ok(create_error_envelope(envelope.query_id, &error_msg))
        }
    }
}

async fn process_query(
    db: Arc<dyn StorageBackend + Send + Sync>,
    payload: &[u8],
) -> Result<proto::query_result::Result, Box<dyn std::error::Error + Send + Sync>> {
    let query = parse_query(payload)?;

    let mut planner = Planner::new();
    let plan = planner.plan(&query)?;
    let plan = planner.optimize(plan)?;

    let explanation = planner.explain(&plan);
    log::debug!("Plan explanation:\n{explanation}");

    let mut evaluator = Evaluator::new(db);
    let result = if let Some(cursor) = query.cursor.clone() {
        evaluator.eval_with_cursor(&plan, Some(cursor)).await?
    } else {
        evaluator.eval(&plan).await?
    };

    Ok(result.result)
}

fn create_response_wrapper(
    query_id: &str,
    query_result: proto::query_result::Result,
) -> proto::Response {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Create metadata
    let metadata = proto::ResponseMetadata {
        query_id: query_id.to_string(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        server_version: SERVER_VERSION.to_string(),
    };

    let proto_query_result = proto::QueryResult {
        result: Some(query_result),
    };

    proto::Response {
        metadata: Some(metadata),
        result: Some(proto::response::Result::Query(proto_query_result)),
    }
}

fn create_error_envelope(query_id: String, error_message: &str) -> proto::Envelope {
    let error_info = proto::ErrorInfo {
        code: 1, // Generic error code
        message: error_message.to_string(),
        r#type: "query_error".to_string(),
        line: 0,
        column: 0,
    };

    let mut error_payload = Vec::new();
    if let Err(e) = error_info.encode(&mut error_payload) {
        log::error!("failed to encode error info: {e}");
        error_payload = error_message.as_bytes().to_vec();
    }

    proto::Envelope {
        version: proto::ProtocolVersion::Version1.into(),
        query_id,
        r#type: proto::MessageType::Error.into(),
        payload: error_payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rulodb::storage::DefaultStorage;
    use rulodb::{Datum, datum};
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_datum_protobuf_types() {
        let string_datum = Datum {
            value: Some(datum::Value::String("test".to_string())),
        };
        match string_datum.value {
            Some(datum::Value::String(s)) => assert_eq!(s, "test"),
            _ => panic!("Expected string value"),
        }

        let int_datum = Datum {
            value: Some(datum::Value::Int(42)),
        };
        match int_datum.value {
            Some(datum::Value::Int(i)) => assert_eq!(i, 42),
            _ => panic!("Expected int value"),
        }

        // Test object datum
        let mut fields = HashMap::new();
        fields.insert(
            "name".to_string(),
            Datum {
                value: Some(datum::Value::String("Alice".to_string())),
            },
        );
        fields.insert(
            "age".to_string(),
            Datum {
                value: Some(datum::Value::Int(30)),
            },
        );

        let object_datum = Datum {
            value: Some(datum::Value::Object(rulodb::ast::DatumObject { fields })),
        };
        if let Some(datum::Value::Object(obj)) = object_datum.value {
            assert_eq!(obj.fields.len(), 2);
            assert!(obj.fields.contains_key("name"));
            assert!(obj.fields.contains_key("age"));
        } else {
            panic!("Expected object value");
        }
    }

    #[tokio::test]
    async fn test_server_startup() {
        let temp_dir = TempDir::new().unwrap();
        let config = rulodb::storage::Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = Arc::new(DefaultStorage::open(&config).unwrap());
        let handle = tokio::spawn(start_server(storage, "127.0.0.1:0"));

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        handle.abort();
    }

    #[test]
    fn test_envelope_creation() {
        let query_id = "test-123".to_string();
        let error_message = "Test error";

        let envelope = create_error_envelope(query_id.clone(), error_message);

        assert_eq!(envelope.query_id, query_id);
        assert_eq!(envelope.r#type, proto::MessageType::Error.into());
        assert_eq!(envelope.version, proto::ProtocolVersion::Version1.into());
        assert!(!envelope.payload.is_empty());
    }

    #[test]
    fn test_envelope_message_types() {
        assert_eq!(proto::MessageType::Query as i32, 0);
        assert_eq!(proto::MessageType::Response as i32, 1);
        assert_eq!(proto::MessageType::Error as i32, 2);
        assert_eq!(proto::MessageType::AuthInit as i32, 3);

        assert_eq!(
            proto::MessageType::try_from(0).unwrap(),
            proto::MessageType::Query
        );
        assert_eq!(
            proto::MessageType::try_from(1).unwrap(),
            proto::MessageType::Response
        );
        assert_eq!(
            proto::MessageType::try_from(2).unwrap(),
            proto::MessageType::Error
        );
    }
}
