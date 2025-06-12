use crate::ast::{
    Cursor, DatabaseCreateResult, DatabaseDropResult, DatabaseListResult, query_result,
};
use crate::evaluator::error::{EvalError, EvalStats};
use crate::storage::StorageBackend;
use futures_util::StreamExt;
use std::sync::Arc;

/// Handler for database-level operations
pub struct DatabaseOperations {
    storage: Arc<dyn StorageBackend>,
}

impl DatabaseOperations {
    /// Create a new database operations handler
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }

    /// Create a new database
    pub async fn create_database(
        &self,
        name: &str,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        self.storage.create_database(name).await?;
        stats.record_rows_processed(1);

        Ok(query_result::Result::DatabaseCreate(DatabaseCreateResult {
            created: 1,
        }))
    }

    /// Drop an existing database
    pub async fn drop_database(
        &self,
        name: &str,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        self.storage.drop_database(name).await?;
        stats.record_rows_processed(1);

        Ok(query_result::Result::DatabaseDrop(DatabaseDropResult {
            dropped: 1,
        }))
    }

    /// List all databases with optional cursor for pagination
    pub async fn list_databases(
        &self,
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let (start_key, limit) = Cursor::convert_to_page_params(cursor.as_ref());

        let mut stream = self
            .storage
            .stream_databases(start_key, limit, None)
            .await?;
        let mut databases = Vec::new();
        let mut last_key = None;

        while let Some(db_name) = stream.next().await.transpose()? {
            last_key = Some(db_name.clone());
            databases.push(db_name);
        }

        let next_cursor = Cursor::from_previous(cursor, last_key, &databases);

        stats.record_rows_processed(databases.len());
        stats.record_rows_returned(databases.len());

        Ok(query_result::Result::DatabaseList(DatabaseListResult {
            databases,
            cursor: next_cursor,
        }))
    }
}
