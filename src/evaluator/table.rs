use crate::ast::{
    Cursor, Datum, DatumObject, Document, GetAllResult, GetResult, InsertResult, Predicate,
    TableCreateResult, TableDropResult, TableListResult, TableScanResult, query_result,
};
use crate::evaluator::error::{EvalError, EvalStats};
use crate::evaluator::utils::string_datum;
use crate::storage::StorageBackend;
use futures_util::StreamExt;
use std::sync::Arc;
use ulid::Ulid;

/// Handler for table-level operations
pub struct TableOperations {
    storage: Arc<dyn StorageBackend>,
}

impl TableOperations {
    /// Create a new table operations handler
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }

    /// Create a new table in the specified database
    pub async fn create_table(
        &self,
        database: &str,
        table: &str,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        self.storage.create_table(database, table).await?;
        stats.record_rows_processed(1);

        Ok(query_result::Result::TableCreate(TableCreateResult {
            created: 1,
        }))
    }

    /// Drop an existing table from the specified database
    pub async fn drop_table(
        &self,
        database: &str,
        table: &str,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        self.storage.drop_table(database, table).await?;
        stats.record_rows_processed(1);

        Ok(query_result::Result::TableDrop(TableDropResult {
            dropped: 1,
        }))
    }

    /// List all tables in the specified database
    pub async fn list_tables(
        &self,
        database: &str,
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let (start_key, limit) = Cursor::convert_to_page_params(cursor.as_ref());

        let mut stream = self
            .storage
            .stream_tables(database, start_key, limit, None)
            .await?;

        let mut tables = Vec::new();
        let mut last_key = None;

        while let Some(table_name) = stream.next().await.transpose()? {
            last_key = Some(table_name.clone());
            tables.push(table_name);
        }

        let next_cursor = Cursor::from_previous(cursor, last_key, &tables);

        stats.record_rows_processed(tables.len());
        stats.record_rows_returned(tables.len());

        Ok(query_result::Result::TableList(TableListResult {
            tables,
            cursor: next_cursor,
        }))
    }

    /// Scan all documents in a table with optional filtering
    pub async fn scan_table(
        &self,
        database: &str,
        table: &str,
        cursor: Option<Cursor>,
        predicate: Option<Predicate>,
        skip: Option<usize>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let (start_key, limit) = Cursor::convert_to_page_params(cursor.as_ref());

        let mut stream = self
            .storage
            .scan_table(database, table, start_key, limit, skip, predicate)
            .await?;

        let mut documents: Vec<Datum> = Vec::new();
        let mut last_key = None;

        while let Some(doc) = stream.next().await.transpose()? {
            last_key = Some(extract_document_primary_key(&doc)?);
            documents.push(doc.into());
        }

        let next_cursor = Cursor::from_previous(cursor, last_key, &documents);

        stats.record_rows_processed(documents.len());
        stats.record_rows_returned(documents.len());

        Ok(query_result::Result::Table(TableScanResult {
            documents,
            cursor: next_cursor,
        }))
    }

    /// Get a single document by key
    pub async fn get_document(
        &self,
        database: &str,
        table: &str,
        key: &str,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let doc = self.storage.get(database, table, key).await?;

        stats.record_rows_processed(1);
        stats.record_rows_returned(1);

        Ok(query_result::Result::Get(GetResult {
            document: doc.map(|d| d.into()),
        }))
    }

    /// Get multiple documents by their keys
    pub async fn get_documents(
        &self,
        database: &str,
        table: &str,
        keys: &[String],
        cursor: Option<Cursor>,
        skip: Option<usize>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let (start_key, limit) = Cursor::convert_to_page_params(cursor.as_ref());

        let mut documents = Vec::new();
        let mut last_key = None;

        let mut stream = self
            .storage
            .stream_get_all(database, table, keys, start_key, limit, skip)
            .await?;

        while let Some(doc) = stream.next().await.transpose()? {
            last_key = Some(extract_document_primary_key(&doc)?);
            documents.push(doc.into());
        }

        let next_cursor = Cursor::from_previous(cursor, last_key, &documents);

        stats.record_rows_processed(keys.len());
        stats.record_rows_returned(documents.len());

        Ok(query_result::Result::GetAll(GetAllResult {
            documents,
            cursor: next_cursor,
        }))
    }

    /// Insert multiple documents into a table
    pub async fn insert_documents(
        &self,
        database: &str,
        table: &str,
        documents: &[DatumObject],
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let mut generated_keys = Vec::new();
        let docs: Vec<(String, Document)> = documents
            .iter()
            .map(|d| {
                let mut doc_fields = d.fields.clone();

                // Handle the document key (id field)
                let key = ensure_document_key(&mut doc_fields, &mut generated_keys);
                let doc = Document::from(&DatumObject { fields: doc_fields });
                (key, doc)
            })
            .collect();

        self.storage.put_batch(database, table, &docs).await?;
        stats.record_rows_processed(documents.len());

        Ok(query_result::Result::Insert(InsertResult {
            inserted: documents.len() as u64,
            generated_keys,
        }))
    }
}

/// Extract the document key from a document.
fn extract_document_primary_key(document: &Document) -> Result<String, EvalError> {
    match document.get("id") {
        Some(id) => Ok(id.clone().to_string()),
        None => Err(EvalError::MissingField("id".to_string())),
    }
}

/// Ensure a document has a valid key, generating one if necessary.
fn ensure_document_key(
    doc_fields: &mut std::collections::HashMap<String, Datum>,
    generated_keys: &mut Vec<Datum>,
) -> String {
    if let Some(id_datum) = doc_fields.get("id") {
        match &id_datum.value {
            Some(crate::ast::datum::Value::String(s)) => s.clone(),
            _ => {
                let gen_id = Ulid::new().to_string();
                doc_fields.insert("id".to_string(), string_datum(gen_id.clone()));
                generated_keys.push(string_datum(gen_id.clone()));
                gen_id
            }
        }
    } else {
        let gen_id = Ulid::new().to_string();
        doc_fields.insert("id".to_string(), string_datum(gen_id.clone()));
        generated_keys.push(string_datum(gen_id.clone()));
        gen_id
    }
}
