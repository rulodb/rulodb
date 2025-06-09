use crate::ast::{BinOp, Datum, Expr, OptArgs, Term, UnOp};
use crate::planner::PlanNode;
use crate::storage::{
    DEFAULT_DATABASE, Document, Result as StorageResult, StorageBackend, StorageError,
};
use futures_util::StreamExt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

pub type Row = BTreeMap<String, Datum>;

#[derive(Debug)]
pub enum EvalError {
    StorageError(StorageError),
    InvalidKeyType,
    MissingField(String),
    InvalidInsertTarget,
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageError(e) => write!(f, "Storage error: {e}"),
            Self::InvalidKeyType => write!(f, "Invalid key type: expected string"),
            Self::MissingField(field) => write!(f, "Missing required field: {field}"),
            Self::InvalidInsertTarget => write!(f, "Invalid document structure for insert"),
        }
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StorageError> for EvalError {
    fn from(e: StorageError) -> Self {
        Self::StorageError(e)
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct EvalStats {
    pub read_count: usize,
    pub inserted_count: usize,
    pub deleted_count: usize,
    pub error_count: usize,
    pub duration_ms: u128,
    pub cache_hits: usize,
    pub batch_operations: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalResult {
    pub result: Datum,
    pub stats: EvalStats,
}

pub struct Evaluator {
    pub storage: Arc<dyn StorageBackend + Send + Sync>,
    pub stats: EvalStats,
}

impl Evaluator {
    pub fn new(storage: Arc<dyn StorageBackend + Send + Sync>) -> Self {
        Self {
            storage,
            stats: EvalStats::default(),
        }
    }

    pub async fn eval(&mut self, plan: &PlanNode) -> Result<EvalResult, EvalError> {
        let start = Instant::now();
        let result = self.evaluate(plan).await?;
        self.stats.duration_ms = start.elapsed().as_millis();

        Ok(EvalResult {
            result,
            stats: self.stats.clone(),
        })
    }

    async fn evaluate_lazy(
        &self,
        plan: &PlanNode,
        predicate: Option<Box<dyn Fn(Document) -> bool + Send + Sync>>,
    ) -> Option<Result<ReceiverStream<StorageResult<Document>>, EvalError>> {
        match (plan, predicate) {
            (PlanNode::ScanTable { db, name, opt_args }, predicate) => {
                let db = use_database(db.as_ref());
                let start_key = opt_start_key(opt_args);
                let batch_size = opt_batch_size(opt_args);

                let stream = self
                    .storage
                    .scan_table(&db, name, start_key, batch_size, predicate)
                    .await
                    .map_err(EvalError::from);

                Some(stream)
            }
            _ => None,
        }
    }

    fn evaluate<'b>(
        &'b mut self,
        plan: &'b PlanNode,
    ) -> Pin<Box<dyn Future<Output = Result<Datum, EvalError>> + Send + 'b>> {
        Box::pin(async move {
            match plan {
                PlanNode::Constant(d) => Ok(d.clone()),
                PlanNode::Eval { expr } => eval_expr(expr, &BTreeMap::new()),
                PlanNode::SelectDatabase { name } => Ok(Datum::String(name.clone())),
                PlanNode::CreateDatabase { name } => self.eval_create_database(name).await,
                PlanNode::DropDatabase { name } => self.eval_drop_database(name).await,
                PlanNode::ListDatabases => self.eval_list_databases().await,
                PlanNode::ScanTable { db, name, opt_args } => {
                    let db = use_database(db.as_ref());
                    let start_key = opt_start_key(opt_args);
                    let batch_size = opt_batch_size(opt_args);
                    self.eval_scan_table(&db, name, start_key, batch_size).await
                }
                PlanNode::CreateTable { db, name, .. } => {
                    self.eval_create_table(&use_database(db.as_ref()), name)
                        .await
                }
                PlanNode::DropTable { db, name } => {
                    self.eval_drop_table(&use_database(db.as_ref()), name).await
                }
                PlanNode::ListTables { db } => {
                    self.eval_list_tables(&use_database(db.as_ref())).await
                }
                PlanNode::GetByKey { db, table, key, .. } => {
                    self.eval_get(&use_database(db.as_ref()), table, key).await
                }
                PlanNode::Filter {
                    source, predicate, ..
                } => self.eval_filter(source, predicate).await,
                PlanNode::Insert {
                    table, documents, ..
                } => self.eval_insert(table, documents).await,
                PlanNode::Delete { source, .. } => self.eval_delete(source).await,
            }
        })
    }

    async fn eval_create_database(&mut self, name: &str) -> Result<Datum, EvalError> {
        self.storage
            .create_database(name)
            .await
            .map_err(EvalError::from)?;
        self.stats.inserted_count += 1;
        Ok(Datum::Null)
    }

    async fn eval_drop_database(&mut self, name: &str) -> Result<Datum, EvalError> {
        self.storage
            .drop_database(name)
            .await
            .map_err(EvalError::from)?;
        self.stats.deleted_count += 1;
        Ok(Datum::Null)
    }

    async fn eval_list_databases(&mut self) -> Result<Datum, EvalError> {
        let databases = self
            .storage
            .list_databases()
            .await
            .map_err(EvalError::from)?;

        self.stats.read_count += databases.len();
        let result: Vec<Datum> = databases.into_iter().map(Datum::String).collect();

        Ok(Datum::Array(result))
    }

    async fn eval_scan_table(
        &mut self,
        db: &str,
        name: &str,
        start_key: Option<String>,
        batch_size: Option<usize>,
    ) -> Result<Datum, EvalError> {
        let mut stream = self
            .storage
            .scan_table(db, name, start_key, batch_size, None)
            .await
            .map_err(EvalError::from)?;

        let mut result = Vec::new();
        let mut batch_count = 0;

        while let Some(doc_result) = stream.next().await {
            match doc_result {
                Ok(doc) => {
                    self.stats.read_count += 1;
                    result.push(Datum::Object(doc));
                    batch_count += 1;
                }
                Err(e) => {
                    self.stats.error_count += 1;
                    return Err(EvalError::StorageError(e));
                }
            }
        }

        if batch_count > 0 {
            self.stats.batch_operations += 1;
        }

        Ok(Datum::Array(result))
    }

    async fn eval_create_table(&mut self, db: &str, name: &str) -> Result<Datum, EvalError> {
        self.storage.create_table(db, name).await?;
        self.stats.inserted_count += 1;
        Ok(Datum::Null)
    }

    async fn eval_drop_table(&mut self, db: &str, name: &str) -> Result<Datum, EvalError> {
        self.storage.drop_table(db, name).await?;
        self.stats.deleted_count += 1;
        Ok(Datum::Null)
    }

    async fn eval_list_tables(&mut self, db: &str) -> Result<Datum, EvalError> {
        let tables = self.storage.list_tables(db).await?;

        self.stats.read_count += tables.len();
        let result: Vec<Datum> = tables.into_iter().map(Datum::String).collect();

        Ok(Datum::Array(result))
    }

    async fn eval_get(&mut self, db: &str, table: &str, key: &Datum) -> Result<Datum, EvalError> {
        let Datum::String(key) = key else {
            return Err(EvalError::InvalidKeyType);
        };

        if let Some(doc) = self.storage.get(db, table, key).await? {
            self.stats.read_count += 1;
            self.stats.cache_hits += 1; // Assume cache hit for single key lookups
            Ok(Datum::Object(doc))
        } else {
            Ok(Datum::Null)
        }
    }

    async fn eval_filter(
        &mut self,
        source: &PlanNode,
        predicate: &Expr,
    ) -> Result<Datum, EvalError> {
        let mut result = Vec::new();

        let predicate_clone = Arc::new(predicate.clone());
        let lazy_predicate = {
            let predicate = Arc::clone(&predicate_clone);
            Box::new(move |doc: Document| {
                eval_expr(&predicate, &doc).is_ok_and(|datum| matches!(datum, Datum::Bool(true)))
            })
        };

        if let Some(Ok(mut stream)) = self.evaluate_lazy(source, Some(lazy_predicate)).await {
            while let Some(doc_result) = stream.next().await {
                match doc_result {
                    Ok(doc) => {
                        self.stats.read_count += 1;
                        result.push(Datum::Object(doc));
                    }
                    Err(e) => {
                        self.stats.error_count += 1;
                        return Err(EvalError::StorageError(e));
                    }
                }
            }
        } else if let Datum::Array(rows) = self.evaluate(source).await? {
            for row in rows {
                if let Datum::Object(doc) = &row {
                    match eval_expr(&predicate_clone, doc) {
                        Ok(Datum::Bool(true)) => {
                            self.stats.read_count += 1;
                            result.push(row);
                        }
                        Err(e) => {
                            self.stats.error_count += 1;
                            return Err(e);
                        }
                        _ => {} // Filter out non-matching rows
                    }
                }
            }
        }

        Ok(Datum::Array(result))
    }

    async fn eval_insert(
        &mut self,
        table: &PlanNode,
        documents: &[Datum],
    ) -> Result<Datum, EvalError> {
        let PlanNode::ScanTable { db, name, .. } = table else {
            self.stats.error_count += 1;
            return Err(EvalError::InvalidInsertTarget);
        };

        let db = &use_database(db.as_ref());
        let mut inserted = Vec::new();
        let mut batch_docs = Vec::new();

        for d in documents {
            match d.clone() {
                Datum::Object(mut obj) => {
                    obj.insert("table".to_string(), Datum::String(name.clone()));

                    let key = if let Some(Datum::String(id)) = obj.get("id") {
                        id.clone()
                    } else {
                        let id = Uuid::new_v4().to_string();
                        obj.insert("id".to_string(), Datum::String(id.clone()));
                        id
                    };

                    inserted.push(Datum::Object(obj.clone()));
                    batch_docs.push((key, obj));
                }
                _ => {
                    self.stats.error_count += 1;
                }
            }
        }

        if batch_docs.len() > 1 {
            match self.storage.put_batch(db, name, &batch_docs).await {
                Ok(()) => {
                    self.stats.inserted_count += batch_docs.len();
                    self.stats.batch_operations += 1;
                }
                Err(_) => {
                    self.stats.error_count += batch_docs.len();
                }
            }
        } else if let Some((key, obj)) = batch_docs.into_iter().next() {
            match self.storage.put(db, name, &key, &obj).await {
                Ok(()) => {
                    self.stats.inserted_count += 1;
                }
                Err(_) => {
                    self.stats.error_count += 1;
                }
            }
        }

        Ok(Datum::Array(inserted))
    }

    async fn eval_delete(&mut self, source: &PlanNode) -> Result<Datum, EvalError> {
        let mut deleted = 0;
        let mut errors = 0;

        let db = use_database(extract_db_from_plan(source));

        if let Some(Ok(mut stream)) = self.evaluate_lazy(source, None).await {
            while let Some(doc_result) = stream.next().await {
                match doc_result {
                    Ok(doc) => {
                        match (
                            extract_document_key_value(&doc, "table"),
                            extract_document_key_value(&doc, "id"),
                        ) {
                            (Ok(table), Ok(key)) => {
                                if self.storage.delete(&db, &table, &key).await.is_ok() {
                                    deleted += 1;
                                } else {
                                    errors += 1;
                                }
                            }
                            _ => errors += 1,
                        }
                    }
                    Err(_) => errors += 1,
                }
            }
        } else {
            let result = self.evaluate(source).await?;
            let documents = match result {
                Datum::Array(arr) => arr,
                Datum::Object(_) => vec![result],
                _ => vec![],
            };

            for row in documents {
                if let Datum::Object(doc) = row {
                    match (
                        extract_document_key_value(&doc, "table"),
                        extract_document_key_value(&doc, "id"),
                    ) {
                        (Ok(table), Ok(key)) => {
                            if self.storage.delete(&db, &table, &key).await.is_ok() {
                                deleted += 1;
                            } else {
                                errors += 1;
                            }
                        }
                        _ => errors += 1,
                    }
                } else {
                    errors += 1;
                }
            }
        }

        self.stats.deleted_count += deleted;
        self.stats.error_count += errors;

        if deleted > 0 {
            self.stats.batch_operations += 1;
        }

        Ok(Datum::Null)
    }
}

fn eval_expr(expr: &Expr, row: &Row) -> Result<Datum, EvalError> {
    match expr {
        Expr::Constant(d) => Ok(d.clone()),
        Expr::Field { name, separator } => {
            Ok(extract_field(row, name, separator.clone()).unwrap_or(Datum::Null))
        }
        Expr::BinaryOp { op, left, right } => {
            let left_val = eval_expr(left, row)?;
            let right_val = eval_expr(right, row)?;

            match (op, left_val, right_val) {
                (BinOp::Eq, a, b) => Ok(Datum::Bool(a == b)),
                (BinOp::Ne, a, b) => Ok(Datum::Bool(a != b)),
                (BinOp::Lt, Datum::Decimal(a), Datum::Decimal(b)) => Ok(Datum::Bool(a < b)),
                (BinOp::Le, Datum::Decimal(a), Datum::Decimal(b)) => Ok(Datum::Bool(a <= b)),
                (BinOp::Gt, Datum::Decimal(a), Datum::Decimal(b)) => Ok(Datum::Bool(a > b)),
                (BinOp::Ge, Datum::Decimal(a), Datum::Decimal(b)) => Ok(Datum::Bool(a >= b)),
                (BinOp::And, Datum::Bool(a), Datum::Bool(b)) => Ok(Datum::Bool(a && b)),
                (BinOp::Or, Datum::Bool(a), Datum::Bool(b)) => Ok(Datum::Bool(a || b)),

                (BinOp::Lt, Datum::String(a), Datum::String(b)) => Ok(Datum::Bool(a < b)),
                (BinOp::Le, Datum::String(a), Datum::String(b)) => Ok(Datum::Bool(a <= b)),
                (BinOp::Gt, Datum::String(a), Datum::String(b)) => Ok(Datum::Bool(a > b)),
                (BinOp::Ge, Datum::String(a), Datum::String(b)) => Ok(Datum::Bool(a >= b)),

                (BinOp::Lt, Datum::Integer(a), Datum::Integer(b)) => Ok(Datum::Bool(a < b)),
                (BinOp::Le, Datum::Integer(a), Datum::Integer(b)) => Ok(Datum::Bool(a <= b)),
                (BinOp::Gt, Datum::Integer(a), Datum::Integer(b)) => Ok(Datum::Bool(a > b)),
                (BinOp::Ge, Datum::Integer(a), Datum::Integer(b)) => Ok(Datum::Bool(a >= b)),
                _ => Ok(Datum::Null),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, row)?;
            match (op, val) {
                (UnOp::Not, Datum::Bool(b)) => Ok(Datum::Bool(!b)),
                _ => Ok(Datum::Null),
            }
        }
    }
}

#[inline]
fn use_database(db: Option<&String>) -> String {
    db.cloned().unwrap_or_else(|| DEFAULT_DATABASE.to_string())
}

fn extract_db_from_plan(plan: &PlanNode) -> Option<&String> {
    match plan {
        PlanNode::ScanTable { db, .. } => db.as_ref(),
        PlanNode::Filter { source, .. } => extract_db_from_plan(source),
        _ => None,
    }
}

fn extract_document_key_value(doc: &Document, key: &str) -> Result<String, EvalError> {
    match doc.get(key) {
        Some(Datum::String(s)) => Ok(s.clone()),
        Some(_) => Err(EvalError::InvalidKeyType),
        None => Err(EvalError::MissingField(key.to_string())),
    }
}

fn extract_field(doc: &Document, path: &str, separator: Option<String>) -> Option<Datum> {
    let separator = separator.unwrap_or_else(|| ".".to_string());
    let parts: Vec<&str> = path.split(&separator).collect();

    let mut current_value = None;
    let mut current_doc = doc;

    for (i, part) in parts.iter().enumerate() {
        match current_doc.get(*part) {
            Some(Datum::Object(obj)) if i < parts.len() - 1 => {
                current_doc = obj;
            }
            Some(datum) if i == parts.len() - 1 => {
                current_value = Some(datum.clone());
                break;
            }
            _ => return None,
        }
    }

    current_value
}

#[inline]
fn opt_start_key(opt_args: &OptArgs) -> Option<String> {
    opt_args.get("start_key").and_then(|term| {
        if let Term::Datum(Datum::String(s)) = term {
            Some(s.clone())
        } else {
            None
        }
    })
}

#[inline]
fn opt_batch_size(opt_args: &OptArgs) -> Option<usize> {
    opt_args.get("batch_size").and_then(|term| {
        if let Term::Datum(Datum::Integer(n)) = term {
            (*n).try_into().ok()
        } else {
            None
        }
    })
}
