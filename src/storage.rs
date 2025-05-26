use crate::ast::Datum;
use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBWithThreadMode, IteratorMode, MultiThreaded, Options, WriteBatch,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;

const SYSTEM_TABLES: &[&str] = &[
    "__schemas__",
    "__tables__",
    "__indexes__",
    "__feeds__",
    "__meta__",
];

pub type Document = BTreeMap<String, Datum>;

pub enum DocumentKey {
    Id,
    Table,
}

impl DocumentKey {
    pub const fn as_str(&self) -> &str {
        match self {
            Self::Id => "id",
            Self::Table => "$table",
        }
    }
}

impl std::fmt::Display for DocumentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug)]
pub enum StorageError {
    BackendError(rocksdb::Error),
    InvalidUtf8(std::string::FromUtf8Error),
    InvalidDocument(rmp_serde::decode::Error),
    EncodeError(rmp_serde::encode::Error),
    MissingColumnFamily(String),
    InvalidTableName(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendError(e) => write!(f, "Storage backend error: {e}"),
            Self::InvalidUtf8(e) => write!(f, "Invalid UTF-8 sequence: {e}"),
            Self::InvalidDocument(e) => write!(f, "Invalid document: {e}"),
            Self::EncodeError(e) => write!(f, "Document encoding error: {e}"),
            Self::MissingColumnFamily(name) => write!(f, "Missing table: {name}"),
            Self::InvalidTableName(name) => write!(f, "Invalid table name: {name}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BackendError(e) => Some(e),
            Self::InvalidUtf8(e) => Some(e),
            Self::InvalidDocument(e) => Some(e),
            Self::EncodeError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(e: rocksdb::Error) -> Self {
        Self::BackendError(e)
    }
}

impl From<std::string::FromUtf8Error> for StorageError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::InvalidUtf8(e)
    }
}

impl From<rmp_serde::decode::Error> for StorageError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::InvalidDocument(e)
    }
}

impl From<rmp_serde::encode::Error> for StorageError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::EncodeError(e)
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn list_tables(&self) -> Result<Vec<String>>;
    async fn create_table(&self, table: &str) -> Result<()>;
    async fn drop_table(&self, table: &str) -> Result<()>;
    async fn put(&self, table: &str, key: &str, doc: &Document) -> Result<()>;
    async fn put_batch(&self, table: &str, docs: &[(String, Document)]) -> Result<()>;
    async fn get(&self, table: &str, key: &str) -> Result<Option<Document>>;
    async fn scan_table(&self, table: &str) -> Result<ReceiverStream<Result<Document>>>;
    async fn delete(&self, table: &str, key: &str) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: String,
    pub write_buffer_size_mb: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
    pub max_background_jobs: i32,
    pub parallelism: i32,
    pub level_zero_file_num_compaction_trigger: i32,
    pub level_zero_slowdown_writes_trigger: i32,
    pub level_zero_stop_writes_trigger: i32,
}

pub struct DefaultStorage {
    inner: Arc<DBWithThreadMode<MultiThreaded>>,
    schema_lock: Arc<Mutex<()>>,
    path: String,
    opts: Options,
}

impl DefaultStorage {
    pub fn open(cfg: &Config) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Write buffer settings
        opts.set_write_buffer_size(cfg.write_buffer_size_mb * 1024 * 1024);
        opts.set_max_write_buffer_number(cfg.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(cfg.min_write_buffer_number_to_merge);

        // Compaction settings
        opts.set_max_background_jobs(cfg.max_background_jobs);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_level_zero_file_num_compaction_trigger(cfg.level_zero_file_num_compaction_trigger);
        opts.set_level_zero_slowdown_writes_trigger(cfg.level_zero_slowdown_writes_trigger);
        opts.set_level_zero_stop_writes_trigger(cfg.level_zero_stop_writes_trigger);

        // Parallelism
        opts.increase_parallelism(cfg.parallelism);
        opts.set_allow_concurrent_memtable_write(true);

        // Compression
        opts.set_compression_type(DBCompressionType::Lz4);

        // Block-based table options
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_size(16 * 1024); // 16KB
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);

        let cfs_on_disk = DB::list_cf(&opts, &cfg.data_dir).unwrap_or_else(|_| {
            SYSTEM_TABLES
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
        });

        let descriptors = cfs_on_disk
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect::<Vec<_>>();

        let db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_cf_descriptors(&opts, &cfg.data_dir, descriptors)?;

        Ok(Self {
            inner: Arc::new(db),
            schema_lock: Arc::new(Mutex::new(())),
            path: cfg.data_dir.clone(),
            opts,
        })
    }
}

#[async_trait]
impl StorageBackend for DefaultStorage {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let opts = self.opts.clone();
        let path = self.path.clone();

        spawn_blocking(move || {
            let tables = DB::list_cf(&opts, &path)
                .unwrap_or_default()
                .into_iter()
                .filter(|cf| !SYSTEM_TABLES.contains(&cf.as_str()))
                .collect();
            Ok(tables)
        })
        .await
        .unwrap()
    }

    async fn create_table(&self, table: &str) -> Result<()> {
        if SYSTEM_TABLES.contains(&table) {
            return Err(StorageError::InvalidTableName(format!(
                "{table} is a reserved name"
            )));
        }

        let db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let table = table.to_string();

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();
            // SAFETY: schema_lock guarantees exclusive access
            let db_mut = unsafe { &mut *(Arc::as_ptr(&db).cast_mut()) };
            db_mut.create_cf(&table, &Options::default())?;
            db_mut
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_table(&self, table: &str) -> Result<()> {
        if SYSTEM_TABLES.contains(&table) {
            return Err(StorageError::InvalidTableName(format!(
                "{table} is a reserved name"
            )));
        }

        let db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let table = table.to_string();

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();
            // SAFETY: schema_lock guarantees exclusive access
            let db_mut = unsafe { &mut *(Arc::as_ptr(&db).cast_mut()) };
            db_mut.drop_cf(&table)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put(&self, table: &str, key: &str, doc: &Document) -> Result<()> {
        let db = self.inner.clone();
        let table = table.to_string();
        let key = key.to_string();
        let doc = rmp_serde::to_vec(doc)?; // Serialize to MsgPack

        spawn_blocking(move || {
            let cf = db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            db.put_cf(&cf, key, doc)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put_batch(&self, table: &str, docs: &[(String, Document)]) -> Result<()> {
        let db = self.inner.clone();
        let table = table.to_string();
        let docs: Vec<(String, Vec<u8>)> = docs
            .iter()
            .map(|(k, d)| Ok((k.clone(), rmp_serde::to_vec(d)?)))
            .collect::<Result<_>>()?;

        spawn_blocking(move || {
            let cf = db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            let mut batch = WriteBatch::default();

            for (key, doc) in docs {
                batch.put_cf(&cf, key, doc);
            }

            db.write(batch)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn get(&self, table: &str, key: &str) -> Result<Option<Document>> {
        let db = self.inner.clone();
        let table = table.to_string();
        let key = key.to_string();

        spawn_blocking(move || {
            let cf = db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            match db.get_cf(&cf, key)? {
                Some(val) => Ok(Some(parse_doc(val.as_slice())?)),
                None => Ok(None),
            }
        })
        .await
        .unwrap()
    }

    async fn scan_table(&self, table: &str) -> Result<ReceiverStream<Result<Document>>> {
        let db = self.inner.clone();
        let table = table.to_string();
        let (tx, rx) = mpsc::channel(16);

        spawn_blocking(move || {
            let cf = db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            for res in db.iterator_cf(&cf, IteratorMode::Start) {
                let doc = res
                    .map_err(StorageError::BackendError)
                    .and_then(|(_, v)| parse_doc(v.as_ref()));

                if tx.blocking_send(doc).is_err() {
                    break;
                }
            }

            Ok::<(), StorageError>(())
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn delete(&self, table: &str, key: &str) -> Result<()> {
        let db = self.inner.clone();
        let table = table.to_string();
        let key = key.to_string();

        spawn_blocking(move || {
            let cf = db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            db.delete_cf(&cf, key)?;
            Ok(())
        })
        .await
        .unwrap()
    }
}

fn parse_doc(data: &[u8]) -> Result<Document> {
    let doc = rmp_serde::from_slice(data).map_err(StorageError::InvalidDocument)?;
    Ok(doc)
}
