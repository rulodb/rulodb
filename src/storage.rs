use crate::ast::Datum;
use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBWithThreadMode, IteratorMode, MultiThreaded, Options, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;

/// The system database name, used for internal metadata storage.
///
/// This database contains the users and roles for authentication, clustering metadata, and other
/// system-level information.
///
/// TODO: Add support for authentication, clustering, and system configuration.
const SYSTEM_DATABASE: &str = "__system__";

/// The default database name, used when no specific database is selected.
///
/// This database is created automatically and cannot be dropped. It is used for user data storage
/// and is the default target for operations when no database is specified. Later this may be up to
/// changes, so the user can rename the default database, but it will always exist as a fallback.
///
/// TODO: Allow renaming the default database in the future.
pub const DEFAULT_DATABASE: &str = "default";

/// List of system tables that are reserved and cannot be created or dropped by users.
///
/// These tables are used for internal metadata management and should not be modified by users. The
/// tables are created automatically when the database is initialized and are essential for the
/// operation of the storage backend.
enum SystemTable {
    Databases,
    Schemas,
    Indexes,
    Feeds,
    Meta,
}

impl SystemTable {
    pub fn variants() -> Vec<Self> {
        vec![
            Self::Databases,
            Self::Schemas,
            Self::Indexes,
            Self::Feeds,
            Self::Meta,
        ]
    }
}

impl std::fmt::Display for SystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Databases => write!(f, "{}", format_table_name(SYSTEM_DATABASE, "__databases__")),
            Self::Schemas => write!(f, "{}", format_table_name(SYSTEM_DATABASE, "__schemas__")),
            Self::Indexes => write!(f, "{}", format_table_name(SYSTEM_DATABASE, "__indexes__")),
            Self::Feeds => write!(f, "{}", format_table_name(SYSTEM_DATABASE, "__feeds__")),
            Self::Meta => write!(f, "{}", format_table_name(SYSTEM_DATABASE, "__meta__")),
        }
    }
}

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
    InvalidDatabaseName(String),
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
            Self::InvalidDatabaseName(name) => write!(f, "Invalid database name: {name}"),
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DatabaseConfig {}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<String>>;
    async fn create_database(&self, db: &str) -> Result<()>;
    async fn drop_database(&self, db: &str) -> Result<()>;
    async fn list_tables(&self, db: &str) -> Result<Vec<String>>;
    async fn create_table(&self, db: &str, table: &str) -> Result<()>;
    async fn drop_table(&self, db: &str, table: &str) -> Result<()>;
    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()>;
    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()>;
    async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>>;
    async fn scan_table(&self, db: &str, table: &str) -> Result<ReceiverStream<Result<Document>>>;
    async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()>;
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

        let cfs_on_disk: Vec<String> = DB::list_cf(&opts, &cfg.data_dir)
            .unwrap_or_else(|_| vec![format_table_name(DEFAULT_DATABASE, "default")]);

        let merged_cfs: Vec<String> = cfs_on_disk
            .into_iter()
            .chain(SystemTable::variants().iter().map(ToString::to_string))
            .collect::<std::collections::HashSet<String>>()
            .into_iter()
            .collect();

        let descriptors = merged_cfs
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect::<Vec<_>>();

        let db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_cf_descriptors(&opts, &cfg.data_dir, descriptors)?;

        let storage = Self {
            inner: Arc::new(db),
            schema_lock: Arc::new(Mutex::new(())),
            path: cfg.data_dir.clone(),
            opts,
        };

        storage.ensure_databases(&merged_cfs)?;

        Ok(storage)
    }

    fn ensure_databases(&self, tables: &[String]) -> Result<()> {
        for table in tables {
            let db = self.inner.clone();
            let system_table = SystemTable::Databases.to_string();

            let db_name = match extract_database_from_table(table) {
                Some(name) => name.to_string(),
                None => return Err(StorageError::InvalidTableName(table.to_string())),
            };

            let default_config = rmp_serde::to_vec(&DatabaseConfig::default())?;

            let cf = db
                .cf_handle(&system_table)
                .ok_or_else(|| StorageError::MissingColumnFamily(system_table.clone()))?;

            let db_config = db.get_cf(&cf, &db_name)?;

            if db_config.is_none() {
                // TODO: Per-database configuration will be stored with the database metadata
                db.put_cf(&cf, &db_name, default_config)?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for DefaultStorage {
    async fn list_databases(&self) -> Result<Vec<String>> {
        let db = self.inner.clone();
        let system_table = SystemTable::Databases.to_string();

        spawn_blocking(move || {
            let mut databases: Vec<String> = Vec::new();

            let cf = db
                .cf_handle(&system_table)
                .ok_or_else(|| StorageError::MissingColumnFamily(system_table.clone()))?;

            db.iterator_cf(&cf, IteratorMode::Start)
                .flatten()
                .for_each(|(key, _)| {
                    if let Ok(db_name) = String::from_utf8(key.to_vec()) {
                        databases.push(db_name);
                    }
                });

            Ok(databases)
        })
        .await
        .unwrap()
    }

    async fn create_database(&self, db: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let system_table = SystemTable::Databases.to_string();

        let db = db.to_string();
        let default_config = rmp_serde::to_vec(&DatabaseConfig::default())?;

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();

            let cf = inner_db
                .cf_handle(&system_table)
                .ok_or_else(|| StorageError::MissingColumnFamily(system_table.clone()))?;

            if inner_db.get_cf(&cf, &db)?.is_some() {
                return Ok(());
            }

            inner_db.put_cf(&cf, &db, default_config)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_database(&self, db: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let system_table = SystemTable::Databases.to_string();
        let db = db.to_string();

        for table in self.list_tables(&db).await? {
            inner_db.drop_cf(&table)?;
        }

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();

            let cf = inner_db
                .cf_handle(&system_table)
                .ok_or_else(|| StorageError::MissingColumnFamily(system_table.clone()))?;

            if inner_db.get_cf(&cf, &db)?.is_some() {
                inner_db.delete_cf(&cf, &db)?;
            }

            Ok::<(), StorageError>(())
        })
        .await
        .unwrap()?;

        Ok(())
    }

    async fn list_tables(&self, db: &str) -> Result<Vec<String>> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let opts = self.opts.clone();
        let path = self.path.clone();
        let db = db.to_string();

        spawn_blocking(move || {
            let tables = DB::list_cf(&opts, &path)
                .unwrap_or_default()
                .into_iter()
                .filter(|cf| cf.as_str() != "default") // Exclude the default column family
                .filter(|cf| cf.as_str().starts_with(&db)) // Filter by database prefix
                .map(|cf| {
                    extract_table_from_database(&cf)
                        .unwrap_or_else(|| &cf)
                        .to_string()
                })
                .collect();
            Ok(tables)
        })
        .await
        .unwrap()
    }

    async fn create_table(&self, db: &str, table: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let table = format_table_name(db, table);

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();
            // SAFETY: schema_lock guarantees exclusive access
            let db_mut = unsafe { &mut *(Arc::as_ptr(&inner_db).cast_mut()) };
            db_mut.create_cf(&table, &Options::default())?;
            db_mut
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_table(&self, db: &str, table: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let schema_lock = self.schema_lock.clone();
        let table = format_table_name(db, table);

        spawn_blocking(move || {
            let _guard = schema_lock.lock().unwrap();
            // SAFETY: schema_lock guarantees exclusive access
            let db_mut = unsafe { &mut *(Arc::as_ptr(&inner_db).cast_mut()) };
            db_mut.drop_cf(&table)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let table = format_table_name(db, table);
        let key = key.to_string();
        let doc = rmp_serde::to_vec(doc)?;

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            inner_db.put_cf(&cf, key, doc)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let table = format_table_name(db, table);
        let docs: Vec<(String, Vec<u8>)> = docs
            .iter()
            .map(|(k, d)| Ok((k.clone(), rmp_serde::to_vec(d)?)))
            .collect::<Result<_>>()?;

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            let mut batch = WriteBatch::default();

            for (key, doc) in docs {
                batch.put_cf(&cf, key, doc);
            }

            inner_db.write(batch)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let table = format_table_name(db, table);
        let key = key.to_string();

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            match inner_db.get_cf(&cf, key)? {
                Some(val) => Ok(Some(parse_doc(val.as_slice())?)),
                None => Ok(None),
            }
        })
        .await
        .unwrap()
    }

    async fn scan_table(&self, db: &str, table: &str) -> Result<ReceiverStream<Result<Document>>> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let (tx, rx) = mpsc::channel(16);
        let inner_db = self.inner.clone();
        let table = format_table_name(db, table);

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            for res in inner_db.iterator_cf(&cf, IteratorMode::Start) {
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

    async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            Err(StorageError::InvalidDatabaseName(db.to_string()))?;
        }

        let inner_db = self.inner.clone();
        let table = format_table_name(db, table);
        let key = key.to_string();

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&table)
                .ok_or_else(|| StorageError::MissingColumnFamily(table.clone()))?;

            inner_db.delete_cf(&cf, key)?;
            Ok(())
        })
        .await
        .unwrap()
    }
}

fn is_valid_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn format_table_name(db: &str, table: &str) -> String {
    format!("{db}:{table}")
}

fn extract_database_from_table(table_name: &str) -> Option<&str> {
    table_name.split(':').next()
}

fn extract_table_from_database(table_name: &str) -> Option<&str> {
    table_name.split(':').nth(1)
}

fn parse_doc(data: &[u8]) -> Result<Document> {
    let doc = rmp_serde::from_slice(data).map_err(StorageError::InvalidDocument)?;
    Ok(doc)
}
