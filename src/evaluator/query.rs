use crate::ast::{
    CountResult, Cursor, Datum, DeleteResult, Expression, FieldRef, FilterResult, LimitResult,
    OrderByField, OrderByResult, SkipResult, UpdateResult, query_result,
};
use crate::evaluator::error::{EvalError, EvalStats};
use crate::evaluator::expression::ExpressionEvaluator;
use crate::evaluator::utils::{
    compare_values, datum_to_bool, extract_document_key, extract_field_from_ref,
    extract_field_value,
};
use crate::planner::PlanNode;
use crate::storage::StorageBackend;
use futures_util::{Stream, StreamExt, stream};
use std::sync::Arc;

pub type StreamingResult =
    std::pin::Pin<Box<dyn Stream<Item = Result<Datum, crate::storage::StorageError>> + Send>>;

/// Handler for query processing operations like filtering, sorting, and streaming
pub struct QueryProcessor {
    storage: Arc<dyn StorageBackend>,
    expression_evaluator: ExpressionEvaluator,
}

impl QueryProcessor {
    /// Create a new query processor
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            expression_evaluator: ExpressionEvaluator::new(),
        }
    }

    /// Filter documents based on a predicate expression
    pub async fn filter_documents(
        &self,
        source_result: query_result::Result,
        predicate: &Expression,
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;
        let mut filtered_docs = Vec::new();

        if !self.expression_evaluator.is_boolean_expression(predicate) {
            return Err(EvalError::InvalidPredicate);
        }

        for doc in documents {
            let result = self
                .expression_evaluator
                .evaluate_expression(predicate, &doc)?;

            if datum_to_bool(&result) {
                filtered_docs.push(doc);
            }
        }

        stats.record_rows_processed(filtered_docs.len());
        stats.record_rows_returned(filtered_docs.len());

        let last_key = filtered_docs
            .last()
            .map(|doc| self.extract_document_key(doc))
            .transpose()?;

        let next_cursor = Cursor::from_previous(cursor, last_key, &filtered_docs);

        Ok(query_result::Result::Filter(FilterResult {
            documents: filtered_docs,
            cursor: next_cursor,
        }))
    }

    /// Sort documents based on order by fields
    pub async fn order_documents(
        &self,
        source_result: query_result::Result,
        order_fields: &[OrderByField],
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;
        let mut sorted_docs = documents.clone();

        // Sort documents based on order by fields
        sorted_docs.sort_by(|a, b| {
            for field in order_fields {
                let field_ref = FieldRef {
                    path: vec![field.field_name.clone()],
                    separator: String::new(),
                };
                let a_val = extract_field_from_ref(a, &field_ref);
                let b_val = extract_field_from_ref(b, &field_ref);

                let cmp = compare_values(&a_val, &b_val);
                let final_cmp = if field.ascending { cmp } else { cmp.reverse() };

                if final_cmp != std::cmp::Ordering::Equal {
                    return final_cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        stats.record_rows_processed(documents.len());
        stats.record_rows_returned(sorted_docs.len());

        let last_key = sorted_docs
            .last()
            .map(|doc| self.extract_document_key(doc))
            .transpose()?;

        let next_cursor = Cursor::from_previous(cursor, last_key, &sorted_docs);

        Ok(query_result::Result::OrderBy(OrderByResult {
            documents: sorted_docs,
            cursor: next_cursor,
        }))
    }

    /// Apply limit to documents from the source result
    pub async fn apply_limit(
        &self,
        source_result: query_result::Result,
        count: u32,
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;

        // Take only the first 'count' documents
        let limited_docs: Vec<Datum> = documents.into_iter().take(count as usize).collect();

        stats.record_rows_processed(limited_docs.len());
        stats.record_rows_returned(limited_docs.len());

        let last_key = limited_docs
            .last()
            .map(|doc| self.extract_document_key(doc))
            .transpose()?;

        let next_cursor = Cursor::from_previous(cursor, last_key, &limited_docs);

        Ok(query_result::Result::Limit(LimitResult {
            documents: limited_docs,
            cursor: next_cursor,
        }))
    }

    /// Apply skip to documents from the source result
    pub async fn apply_skip(
        &self,
        source_result: query_result::Result,
        count: u32,
        cursor: Option<Cursor>,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;

        // Skip the first 'count' documents
        let skipped_docs: Vec<Datum> = documents.into_iter().skip(count as usize).collect();

        stats.record_rows_processed(skipped_docs.len());
        stats.record_rows_returned(skipped_docs.len());

        let last_key = skipped_docs
            .last()
            .map(|doc| self.extract_document_key(doc))
            .transpose()?;

        let next_cursor = Cursor::from_previous(cursor, last_key, &skipped_docs);

        Ok(query_result::Result::Skip(SkipResult {
            documents: skipped_docs,
            cursor: next_cursor,
        }))
    }

    /// Count documents (streaming version)
    pub async fn count_documents_streaming(
        &self,
        source: &PlanNode,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let mut count = 0;
        let mut processed = 0;

        // Create a stream from the source plan
        let mut stream = self.create_document_stream(source, None).await?;

        while let Some(doc_result) = stream.next().await {
            processed += 1;
            match doc_result {
                Ok(_) => count += 1,
                Err(e) => return Err(EvalError::StorageError(e)),
            }
        }

        stats.record_rows_processed(processed);
        stats.record_rows_returned(1); // Count result is always 1 row

        Ok(query_result::Result::Count(CountResult { count }))
    }

    /// Update documents based on a patch
    pub async fn update_documents(
        &self,
        source_result: query_result::Result,
        patch: &crate::ast::DatumObject,
        source_plan: &PlanNode,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;
        let mut updated_count = 0;

        let (database, table) = self.extract_table_context(source_plan)?;

        for doc in documents {
            let updated_doc = self.apply_patch_to_document(&doc, patch)?;
            let key = self.extract_document_key(&updated_doc)?;

            // Convert to storage document format
            if let Some(crate::ast::datum::Value::Object(obj)) = &updated_doc.value {
                let storage_doc = crate::ast::Document::from(obj);
                self.storage
                    .put(&database, &table, &key, &storage_doc)
                    .await?;
                updated_count += 1;
            }
        }

        stats.record_rows_processed(updated_count);

        Ok(query_result::Result::Update(UpdateResult {
            updated: updated_count as u64,
        }))
    }

    /// Delete documents
    pub async fn delete_documents(
        &self,
        source_result: query_result::Result,
        stats: &mut EvalStats,
    ) -> Result<query_result::Result, EvalError> {
        let documents = self.extract_documents_from_result(source_result)?;
        let deleted_count = documents.len();

        // Note: Actual deletion would require table context and storage operations
        // For now, we'll just return the count
        stats.record_rows_processed(deleted_count);

        Ok(query_result::Result::Delete(DeleteResult {
            deleted: deleted_count as u64,
        }))
    }

    /// Extract documents from various result types
    fn extract_documents_from_result(
        &self,
        result: query_result::Result,
    ) -> Result<Vec<Datum>, EvalError> {
        match result {
            query_result::Result::Table(scan_result) => Ok(scan_result.documents),
            query_result::Result::Get(get_result) => Ok(get_result.document.into_iter().collect()),
            query_result::Result::GetAll(get_all_result) => Ok(get_all_result.documents),
            query_result::Result::Filter(filter_result) => Ok(filter_result.documents),
            query_result::Result::OrderBy(order_result) => Ok(order_result.documents),
            query_result::Result::Skip(skip_result) => Ok(skip_result.documents),
            query_result::Result::Limit(limit_result) => Ok(limit_result.documents),
            _ => Err(EvalError::InvalidExpression),
        }
    }

    /// Apply a patch to a document using various update operations
    fn apply_patch_to_document(
        &self,
        doc: &Datum,
        patch: &crate::ast::DatumObject,
    ) -> Result<Datum, EvalError> {
        if let Some(crate::ast::datum::Value::Object(obj)) = &doc.value {
            let mut updated_fields = obj.fields.clone();

            // Apply patch fields to the target object
            for (key, value) in &patch.fields {
                if value.value.is_none() {
                    // Remove field if value is null
                    updated_fields.remove(key);
                } else {
                    // Set or update field
                    updated_fields.insert(key.clone(), value.clone());
                }
            }

            Ok(Datum {
                value: Some(crate::ast::datum::Value::Object(crate::ast::DatumObject {
                    fields: updated_fields,
                })),
            })
        } else {
            Err(EvalError::InvalidInsertTarget)
        }
    }

    /// Extract the document key from a datum
    fn extract_document_key(&self, doc: &Datum) -> Result<String, EvalError> {
        let id_field = extract_field_value(doc, "id");
        extract_document_key(&id_field)
    }

    /// Extract table context from a plan node
    #[allow(clippy::only_used_in_recursion)]
    fn extract_table_context(&self, plan: &PlanNode) -> Result<(String, String), EvalError> {
        match plan {
            PlanNode::CreateTable { table_ref, .. }
            | PlanNode::DropTable { table_ref, .. }
            | PlanNode::TableScan { table_ref, .. }
            | PlanNode::Insert { table_ref, .. }
            | PlanNode::Get { table_ref, .. }
            | PlanNode::GetAll { table_ref, .. } => {
                let database = table_ref
                    .database
                    .as_ref()
                    .map(|d| d.name.clone())
                    .unwrap_or_else(|| crate::storage::DEFAULT_DATABASE.to_string());
                Ok((database, table_ref.name.clone()))
            }
            PlanNode::Update { source, .. }
            | PlanNode::Delete { source, .. }
            | PlanNode::Filter { source, .. }
            | PlanNode::OrderBy { source, .. }
            | PlanNode::Limit { source, .. }
            | PlanNode::Skip { source, .. }
            | PlanNode::Count { source, .. } => self.extract_table_context(source),
            _ => Err(EvalError::InvalidExpression),
        }
    }

    /// Create a document stream from a plan node
    async fn create_document_stream(
        &self,
        plan: &PlanNode,
        cursor_override: Option<&Cursor>,
    ) -> Result<StreamingResult, EvalError> {
        match plan {
            PlanNode::TableScan {
                table_ref,
                cursor: plan_cursor,
                ..
            } => {
                let database = table_ref
                    .database
                    .as_ref()
                    .map(|d| d.name.clone())
                    .unwrap_or_else(|| crate::storage::DEFAULT_DATABASE.to_string());

                let storage = self.storage.clone();
                let table_name = table_ref.name.clone();

                // Decide effective cursor: override takes precedence over plan cursor
                let effective_cursor = cursor_override.or(plan_cursor.as_ref());

                // Extract cursor parameters for storage
                let (start_key, limit) = Cursor::convert_to_page_params(effective_cursor);

                let documents_stream = async_stream::stream! {
                    match storage.scan_table(&database, &table_name, start_key.clone(), limit, None, None).await {
                        Ok(mut stream) => {
                            while let Some(result) = stream.next().await {
                                match result {
                                    Ok(doc) => {
                                        yield Ok(Datum::from(doc));
                                    }
                                    Err(e) => yield Err(e),
                                }
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                };

                Ok(Box::pin(documents_stream))
            }

            PlanNode::Get { table_ref, key, .. } => {
                let database = table_ref
                    .database
                    .as_ref()
                    .map(|d| d.name.clone())
                    .unwrap_or_else(|| crate::storage::DEFAULT_DATABASE.to_string());

                let storage = self.storage.clone();
                let table_name = table_ref.name.clone();
                let key = key.clone();

                let documents_stream = async_stream::stream! {
                    match storage.get(&database, &table_name, &key).await {
                        Ok(Some(doc)) => {
                            yield Ok(Datum::from(doc));
                        }
                        Ok(None) => {
                            // No document found - don't yield anything
                        }
                        Err(e) => yield Err(e),
                    }
                };

                Ok(Box::pin(documents_stream))
            }

            PlanNode::GetAll {
                table_ref,
                keys,
                cursor: plan_cursor,
                ..
            } => {
                let database = table_ref
                    .database
                    .as_ref()
                    .map(|d| d.name.clone())
                    .unwrap_or_else(|| crate::storage::DEFAULT_DATABASE.to_string());

                // Create a stream from the storage get_all operation
                let storage = self.storage.clone();
                let table_name = table_ref.name.clone();
                let keys = keys.clone();

                // Decide effective cursor: override takes precedence over plan cursor
                let effective_cursor = cursor_override.or(plan_cursor.as_ref());

                // Extract cursor parameters for storage
                let (start_key, limit) = Cursor::convert_to_page_params(effective_cursor);

                let documents_stream = async_stream::stream! {
                    match storage.stream_get_all(&database, &table_name, &keys, start_key.clone(), limit, None).await {
                        Ok(mut stream) => {
                            while let Some(result) = stream.next().await {
                                match result {
                                    Ok(doc) => {
                                        yield Ok(Datum::from(doc));
                                    }
                                    Err(e) => yield Err(e),
                                }
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                };

                Ok(Box::pin(documents_stream))
            }

            _ => {
                // For unsupported plan types, return empty stream
                let empty_stream = stream::empty();
                Ok(Box::pin(empty_stream))
            }
        }
    }
}
