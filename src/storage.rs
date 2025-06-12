use crate::ast::{Document, Predicate};
use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, ReadOptions, SliceTransform,
    WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;

/// The system database name, used for internal metadata storage.
const SYSTEM_DATABASE: &str = "__system__";

/// The default database name, used when no specific database is selected.
pub const DEFAULT_DATABASE: &str = "default";

/// The default limit for streaming operations.
pub const DEFAULT_STREAMING_LIMIT: usize = 1000;

/// Maximum concurrent operations to prevent resource exhaustion
const MAX_CONCURRENT_OPERATIONS: usize = 1000;

/// Cache size for frequently accessed column family handles
const CF_CACHE_SIZE: usize = 1024;

/// List of system tables that are reserved and cannot be created or dropped by users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            Self::Databases => write!(f, "__databases__"),
            Self::Schemas => write!(f, "__schemas__"),
            Self::Indexes => write!(f, "__indexes__"),
            Self::Feeds => write!(f, "__feeds__"),
            Self::Meta => write!(f, "__meta__"),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    BackendError(rocksdb::Error),
    InvalidUtf8(std::string::FromUtf8Error),
    InvalidDocument(bincode::error::DecodeError),
    EncodeError(bincode::error::EncodeError),
    DecodeError(bincode::error::DecodeError),
    MissingColumnFamily(String),
    InvalidDatabaseName(String),
    InvalidTableName(String),
    ResourceExhausted,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendError(e) => write!(f, "Backend error: {e}"),
            Self::InvalidUtf8(e) => write!(f, "Invalid UTF-8: {e}"),
            Self::InvalidDocument(e) => write!(f, "Invalid document: {e}"),
            Self::EncodeError(e) => write!(f, "Encode error: {e}"),
            Self::DecodeError(e) => write!(f, "Decode error: {e}"),
            Self::MissingColumnFamily(cf) => write!(f, "Missing column family: {cf}"),
            Self::InvalidDatabaseName(db) => write!(f, "Invalid database name: {db}"),
            Self::InvalidTableName(table) => write!(f, "Invalid table name: {table}"),
            Self::ResourceExhausted => {
                write!(f, "Resource exhausted - too many concurrent operations")
            }
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
            Self::DecodeError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(value: rocksdb::Error) -> Self {
        Self::BackendError(value)
    }
}

impl From<std::string::FromUtf8Error> for StorageError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::InvalidUtf8(value)
    }
}

impl From<bincode::error::EncodeError> for StorageError {
    fn from(value: bincode::error::EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<bincode::error::DecodeError> for StorageError {
    fn from(value: bincode::error::DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

// Cached column family handle to avoid repeated lookups
#[derive(Clone)]
struct CachedCF {
    last_access: Arc<AtomicUsize>,
}

// Thread-safe LRU cache for column family handles
struct CFCache {
    cache: Arc<RwLock<HashMap<String, CachedCF>>>,
    access_counter: AtomicUsize,
}

impl CFCache {
    fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            access_counter: AtomicUsize::new(0),
        }
    }

    fn get<'a>(
        &self,
        key: &str,
        db: &'a Arc<DBWithThreadMode<MultiThreaded>>,
    ) -> Option<Arc<rocksdb::BoundColumnFamily<'a>>> {
        let access_time = self.access_counter.fetch_add(1, Ordering::Relaxed);

        // Try read lock first for fast path
        if let Ok(cache) = self.cache.read() {
            if let Some(cached) = cache.get(key) {
                cached.last_access.store(access_time, Ordering::Relaxed);
                // Get fresh handle from database
                return db.cf_handle(key);
            }
        }

        // Fallback to database lookup and cache insertion
        if let Some(cf) = db.cf_handle(key) {
            let cached_cf = CachedCF {
                last_access: Arc::new(AtomicUsize::new(access_time)),
            };

            if let Ok(mut cache) = self.cache.write() {
                // Evict oldest entries if cache is full
                if cache.len() >= CF_CACHE_SIZE {
                    let mut oldest_key = None;
                    let mut oldest_access = usize::MAX;

                    for (k, v) in cache.iter() {
                        let access = v.last_access.load(Ordering::Relaxed);
                        if access < oldest_access {
                            oldest_access = access;
                            oldest_key = Some(k.clone());
                        }
                    }

                    if let Some(key_to_remove) = oldest_key {
                        cache.remove(&key_to_remove);
                    }
                }

                cache.insert(key.to_string(), cached_cf);
            }

            return Some(cf);
        }

        None
    }
}

static CF_CACHE: std::sync::OnceLock<CFCache> = std::sync::OnceLock::new();

fn get_cf_cache() -> &'static CFCache {
    CF_CACHE.get_or_init(CFCache::new)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DatabaseConfig {
    // Future database-specific configuration
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn create_database(&self, name: &str) -> Result<()>;
    async fn drop_database(&self, name: &str) -> Result<()>;
    async fn database_exists(&self, name: &str) -> Result<bool>;
    async fn create_table(&self, db: &str, table: &str) -> Result<()>;
    async fn drop_table(&self, db: &str, table: &str) -> Result<()>;
    async fn table_exists(&self, db: &str, table: &str) -> Result<bool>;
    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()>;
    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()>;
    async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>>;
    async fn scan_table(
        &self,
        db: &str,
        table: &str,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
        predicate: Option<Predicate>,
    ) -> Result<ReceiverStream<Result<Document>>>;
    async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()>;

    // Streaming versions for cursor pagination
    async fn stream_databases(
        &self,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<String>>>;
    async fn stream_tables(
        &self,
        db: &str,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<String>>>;
    async fn stream_get_all(
        &self,
        db: &str,
        table: &str,
        keys: &[String],
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<Document>>>;
}

#[derive(Clone)]
pub struct Config {
    pub data_dir: String,
    pub max_background_jobs: i32,
    pub parallelism: i32,
    pub level_zero_file_num_compaction_trigger: i32,
    pub level_zero_slowdown_writes_trigger: i32,
    pub level_zero_stop_writes_trigger: i32,
    pub block_cache_size: usize,
    pub max_open_files: i32,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub use_direct_reads: bool,
    pub bytes_per_sync: u64,
    pub wal_bytes_per_sync: u64,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            max_background_jobs: std::thread::available_parallelism()
                .map(|p| i32::try_from(p.get()).unwrap_or(4))
                .unwrap_or(4),
            parallelism: std::thread::available_parallelism()
                .map(|p| i32::try_from(p.get()).unwrap_or(4))
                .unwrap_or(4),
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 20,
            level_zero_stop_writes_trigger: 36,
            block_cache_size: 268_435_456, // 256MB
            max_open_files: 100,
            use_direct_io_for_flush_and_compaction: true,
            use_direct_reads: false,
            bytes_per_sync: 1_048_576,             // 1MB
            wal_bytes_per_sync: 1_048_576,         // 1MB
            target_file_size_base: 67_108_864,     // 64MB
            max_bytes_for_level_base: 268_435_456, // 256MB
            write_buffer_size: 67_108_864,         // 64MB
            max_write_buffer_number: 3,
            min_write_buffer_number_to_merge: 1,
        }
    }
}

pub struct DefaultStorage {
    inner: Arc<DBWithThreadMode<MultiThreaded>>,
    schema_lock: Arc<RwLock<()>>,
    path: String,
    opts: Options,
    operation_semaphore: Arc<Semaphore>,
}

impl DefaultStorage {
    pub fn open(cfg: &Config) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Write buffer settings
        opts.set_write_buffer_size(cfg.write_buffer_size);
        opts.set_max_write_buffer_number(cfg.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(cfg.min_write_buffer_number_to_merge);

        // Compaction settings
        opts.set_max_background_jobs(cfg.max_background_jobs);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_level_zero_file_num_compaction_trigger(cfg.level_zero_file_num_compaction_trigger);
        opts.set_level_zero_slowdown_writes_trigger(cfg.level_zero_slowdown_writes_trigger);
        opts.set_level_zero_stop_writes_trigger(cfg.level_zero_stop_writes_trigger);

        // Level compaction settings for better write amplification
        opts.set_target_file_size_base(cfg.target_file_size_base);
        opts.set_max_bytes_for_level_base(cfg.max_bytes_for_level_base);
        opts.set_level_compaction_dynamic_level_bytes(true);

        // Parallelism
        opts.increase_parallelism(cfg.parallelism);
        opts.set_allow_concurrent_memtable_write(true);
        opts.set_enable_write_thread_adaptive_yield(true);

        // Compression - use zstd for better compression ratio
        opts.set_compression_type(DBCompressionType::Zstd);

        // I/O optimization
        opts.set_max_open_files(cfg.max_open_files);
        opts.set_use_direct_io_for_flush_and_compaction(cfg.use_direct_io_for_flush_and_compaction);
        opts.set_use_direct_reads(cfg.use_direct_reads);
        opts.set_bytes_per_sync(cfg.bytes_per_sync);
        opts.set_wal_bytes_per_sync(cfg.wal_bytes_per_sync);

        // WAL optimization
        opts.set_wal_ttl_seconds(0);
        opts.set_wal_size_limit_mb(0);

        // Create shared block cache
        let block_cache = Cache::new_lru_cache(cfg.block_cache_size);

        // Block-based table options with optimizations
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_size(32 * 1024); // 32KB for better cache efficiency
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_block_cache(&block_cache);

        // Use prefix extractor for better seek performance
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(4));
        opts.set_block_based_table_factory(&block_opts);

        // Enable statistics for monitoring
        opts.enable_statistics();

        let cfs_on_disk: Vec<String> = DB::list_cf(&opts, &cfg.data_dir)
            .unwrap_or_else(|_| vec![format_table_name(DEFAULT_DATABASE, "default")]);

        let merged_cfs: Vec<String> = cfs_on_disk
            .into_iter()
            .chain(SystemTable::variants().iter().map(ToString::to_string))
            .collect::<HashSet<String>>()
            .into_iter()
            .collect();

        let descriptors = merged_cfs
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compression_type(DBCompressionType::Zstd);
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect::<Vec<_>>();

        let db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_cf_descriptors(&opts, &cfg.data_dir, descriptors)?;

        let storage = Self {
            inner: Arc::new(db),
            schema_lock: Arc::new(RwLock::new(())),
            path: cfg.data_dir.clone(),
            opts,
            operation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS)),
        };

        storage.ensure_databases(&merged_cfs)?;

        Ok(storage)
    }

    fn ensure_databases(&self, cfs: &[String]) -> Result<()> {
        let _lock = self.schema_lock.write().unwrap();

        for table in SystemTable::variants() {
            let cf_name = table.to_string();
            if !cfs.contains(&cf_name) {
                self.inner.create_cf(&cf_name, &Options::default())?;
            }
        }

        Ok(())
    }

    fn serialize_batch(docs: &[(String, Document)]) -> Result<Vec<(String, Vec<u8>)>> {
        docs.iter()
            .map(|(k, d)| {
                let serialized = bincode::serde::encode_to_vec(d, bincode::config::standard())?;
                Ok((k.clone(), serialized))
            })
            .collect()
    }

    fn create_read_opts() -> ReadOptions {
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false); // Disable for better read performance
        read_opts.fill_cache(true);
        read_opts
    }

    fn create_write_opts() -> WriteOptions {
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Use periodic sync for better performance
        write_opts.disable_wal(false);
        write_opts
    }
}

#[async_trait]
impl StorageBackend for DefaultStorage {
    async fn create_database(&self, name: &str) -> Result<()> {
        if !is_valid_key(name) || is_system_db(name) {
            return Err(StorageError::InvalidDatabaseName(name.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let write_opts = Self::create_write_opts();
        let name = name.to_string();

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&SystemTable::Databases.to_string())
                .ok_or_else(|| {
                    StorageError::MissingColumnFamily(SystemTable::Databases.to_string())
                })?;

            let db_config = DatabaseConfig::default();
            let serialized =
                bincode::serde::encode_to_vec(&db_config, bincode::config::standard())?;

            inner_db.put_cf_opt(&cf, &name, serialized, &write_opts)?;

            let table_cf_name = format_table_name(&name, "default");
            inner_db.create_cf(&table_cf_name, &Options::default())?;

            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_database(&self, name: &str) -> Result<()> {
        if !is_valid_key(name) || is_system_db(name) || name == DEFAULT_DATABASE {
            return Err(StorageError::InvalidDatabaseName(name.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let name = name.to_string();

        spawn_blocking(move || {
            let table_names: Vec<String> = DB::list_cf(&Options::default(), ".")
                .unwrap_or_default()
                .into_iter()
                .filter(|cf_name| cf_name.starts_with(&format!("{name}:")))
                .collect();

            for table_name in table_names {
                inner_db.drop_cf(&table_name)?;
            }

            let cf = inner_db
                .cf_handle(&SystemTable::Databases.to_string())
                .ok_or_else(|| {
                    StorageError::MissingColumnFamily(SystemTable::Databases.to_string())
                })?;

            inner_db.delete_cf(&cf, &name)?;

            Ok(())
        })
        .await
        .unwrap()
    }

    async fn database_exists(&self, name: &str) -> Result<bool> {
        let inner_db = self.inner.clone();
        let name = name.to_string();

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&SystemTable::Databases.to_string())
                .ok_or_else(|| {
                    StorageError::MissingColumnFamily(SystemTable::Databases.to_string())
                })?;

            Ok(inner_db.get_cf(&cf, &name)?.is_some())
        })
        .await
        .unwrap()
    }

    async fn create_table(&self, db: &str, table: &str) -> Result<()> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }
        if !is_valid_key(table) {
            return Err(StorageError::InvalidTableName(table.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);

        spawn_blocking(move || {
            inner_db.create_cf(&table_name, &Options::default())?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_table(&self, db: &str, table: &str) -> Result<()> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);

        spawn_blocking(move || {
            inner_db.drop_cf(&table_name)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn table_exists(&self, db: &str, table: &str) -> Result<bool> {
        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);

        spawn_blocking(move || Ok(inner_db.cf_handle(&table_name).is_some()))
            .await
            .unwrap()
    }

    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let key = key.to_string();
        let serialized_doc = bincode::serde::encode_to_vec(doc, bincode::config::standard())?;
        let write_opts = Self::create_write_opts();

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            inner_db.put_cf_opt(&cf, key, serialized_doc, &write_opts)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let docs = Self::serialize_batch(docs)?;
        let write_opts = Self::create_write_opts();

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            let mut batch = WriteBatch::default();
            for (key, doc) in docs {
                batch.put_cf(&cf, key, doc);
            }

            inner_db.write_opt(batch, &write_opts)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let key = key.to_string();
        let read_opts = Self::create_read_opts();

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            match inner_db.get_cf_opt(&cf, key, &read_opts)? {
                Some(val) => Ok(Some(parse_doc(val.as_slice())?)),
                None => Ok(None),
            }
        })
        .await
        .unwrap()
    }

    async fn scan_table(
        &self,
        db: &str,
        table: &str,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
        predicate: Option<Predicate>,
    ) -> Result<ReceiverStream<Result<Document>>> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let limit = limit.unwrap_or(DEFAULT_STREAMING_LIMIT);
        let skip = skip.unwrap_or(0);
        let read_opts = Self::create_read_opts();
        // Ensure channel capacity is at least 1, even if limit is 0
        let channel_capacity = limit.clamp(1, 1000);
        let (tx, rx) = mpsc::channel(channel_capacity);

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            let mode = start_key.as_ref().map_or(IteratorMode::Start, |key| {
                IteratorMode::From(key.as_bytes(), Direction::Forward)
            });

            let iterator = inner_db.iterator_cf_opt(&cf, read_opts, mode);
            let iterator = match mode {
                IteratorMode::Start => iterator.skip(skip),
                _ => iterator.skip(1 + skip),
            };

            for res in iterator.take(limit) {
                match res {
                    Ok((_, v)) => match parse_doc(v.as_ref()) {
                        Ok(doc) => {
                            if let Some(predicate) = &predicate {
                                if !predicate(doc.clone()) {
                                    continue;
                                }
                            }

                            if tx.blocking_send(Ok(doc)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            break;
                        }
                    },
                    Err(e) => {
                        let _ = tx.blocking_send(Err(StorageError::BackendError(e)));
                        break;
                    }
                }
            }

            Ok::<(), StorageError>(())
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let key = key.to_string();
        let write_opts = Self::create_write_opts();

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            inner_db.delete_cf_opt(&cf, key, &write_opts)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn stream_databases(
        &self,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<String>>> {
        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let read_opts = Self::create_read_opts();
        let limit = limit.unwrap_or(DEFAULT_STREAMING_LIMIT);
        let skip = skip.unwrap_or(0);
        // Ensure channel capacity is at least 1, even if limit is 0
        let channel_capacity = limit.clamp(1, 1000);
        let (tx, rx) = mpsc::channel(channel_capacity);

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&SystemTable::Databases.to_string())
                .ok_or_else(|| {
                    StorageError::MissingColumnFamily(SystemTable::Databases.to_string())
                })?;

            let mode = start_key.as_ref().map_or(IteratorMode::Start, |key| {
                IteratorMode::From(key.as_bytes(), Direction::Forward)
            });

            let iterator = inner_db
                .iterator_cf_opt(&cf, read_opts, mode)
                .skip(if start_key.is_some() { 1 + skip } else { skip });

            for res in iterator.take(limit) {
                match res {
                    Ok((key_bytes, _)) => match String::from_utf8(key_bytes.to_vec()) {
                        Ok(db_name) => {
                            if !is_system_db(&db_name) && tx.blocking_send(Ok(db_name)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(StorageError::InvalidUtf8(e)));
                            break;
                        }
                    },
                    Err(e) => {
                        let _ = tx.blocking_send(Err(StorageError::BackendError(e)));
                        break;
                    }
                }
            }

            Ok::<(), StorageError>(())
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn stream_tables(
        &self,
        db: &str,
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<String>>> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let opts = self.opts.clone();
        let path = self.path.clone();
        let db = db.to_string();
        let limit = limit.unwrap_or(DEFAULT_STREAMING_LIMIT);
        let skip = skip.unwrap_or(0);
        let channel_capacity = limit.clamp(1, 1000);
        let (tx, rx) = mpsc::channel(channel_capacity);

        spawn_blocking(move || {
            let prefix = format!("{db}:");
            let mut tables: Vec<String> = DB::list_cf(&opts, &path)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|cf_name| {
                    if cf_name.starts_with(&prefix) {
                        extract_table_from_database(&cf_name).map(std::string::ToString::to_string)
                    } else {
                        None
                    }
                })
                .collect();

            // Sort for consistent pagination
            tables.sort();

            // Apply start_key filter
            if let Some(start_key) = start_key {
                tables.retain(|table| table >= &start_key);
            }

            // Apply skip and limit
            let tables: Vec<String> = tables.into_iter().skip(skip).take(limit).collect();

            for table in tables {
                if tx.blocking_send(Ok(table)).is_err() {
                    break;
                }
            }

            Ok::<(), StorageError>(())
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn stream_get_all(
        &self,
        db: &str,
        table: &str,
        keys: &[String],
        start_key: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<ReceiverStream<Result<Document>>> {
        if !is_valid_key(db) || is_system_db(db) {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let keys = keys.to_vec();
        let limit = limit.unwrap_or(DEFAULT_STREAMING_LIMIT);
        let skip = skip.unwrap_or(0);
        // Ensure channel capacity is at least 1, even if limit is 0
        let channel_capacity = limit.clamp(1, 1000);
        let (tx, rx) = mpsc::channel(channel_capacity);

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            let mut filtered_keys = keys;

            // Apply start_key filter if provided
            if let Some(start_key) = start_key {
                let start_index = filtered_keys
                    .iter()
                    .position(|k| k >= &start_key)
                    .unwrap_or(filtered_keys.len());
                filtered_keys = filtered_keys.into_iter().skip(start_index).collect();
            }

            // Apply skip and limit
            let filtered_keys: Vec<String> =
                filtered_keys.into_iter().skip(skip).take(limit).collect();

            for key in filtered_keys {
                match inner_db.get_cf_opt(&cf, &key, &Self::create_read_opts()) {
                    Ok(Some(val)) => match parse_doc(val.as_slice()) {
                        Ok(doc) => {
                            if tx.blocking_send(Ok(doc)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            break;
                        }
                    },
                    Ok(None) => {
                        // Key not found, skip
                        continue;
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(StorageError::BackendError(e)));
                        break;
                    }
                }
            }

            Ok::<(), StorageError>(())
        });

        Ok(ReceiverStream::new(rx))
    }
}

#[inline]
fn is_system_db(db_name: &str) -> bool {
    db_name == SYSTEM_DATABASE
}

#[inline]
fn is_valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 255 && // Add reasonable length limit
        key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[inline]
fn format_table_name(db: &str, table: &str) -> String {
    let mut result = String::with_capacity(db.len() + table.len() + 1);
    result.push_str(db);
    result.push(':');
    result.push_str(table);
    result
}

#[inline]
fn extract_table_from_database(table_name: &str) -> Option<&str> {
    table_name.split_once(':').map(|(_, table)| table)
}

#[inline]
fn parse_doc(data: &[u8]) -> Result<Document> {
    log::trace!("Attempting to deserialize document, {} bytes", data.len());
    bincode::serde::decode_from_slice(data, bincode::config::standard())
        .map(|(doc, _)| doc)
        .map_err(|e| {
            log::error!(
                "Failed to deserialize document: {} (data: {} bytes)",
                e,
                data.len()
            );

            StorageError::DecodeError(e)
        })
}

#[cfg(test)]
pub mod memory {
    use super::*;
    use std::sync::Mutex;

    // Mock in-memory storage backend for benchmarking
    #[derive(Debug, Clone)]
    pub struct MemoryStorage {
        #[allow(clippy::type_complexity)]
        data: Arc<Mutex<HashMap<String, HashMap<String, HashMap<String, Document>>>>>,
        databases: Arc<Mutex<Vec<String>>>,
        operation_count: Arc<Mutex<u64>>,
    }

    impl Default for MemoryStorage {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MemoryStorage {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
                databases: Arc::new(Mutex::new(vec!["default".to_string()])),
                operation_count: Arc::new(Mutex::new(0)),
            }
        }

        fn increment_operation_count(&self) {
            let mut count = self.operation_count.lock().unwrap();
            *count += 1;
        }

        pub fn get_operation_count(&self) -> u64 {
            *self.operation_count.lock().unwrap()
        }

        pub fn reset_operation_count(&self) {
            let mut count = self.operation_count.lock().unwrap();
            *count = 0;
        }
    }

    #[async_trait]
    impl StorageBackend for MemoryStorage {
        async fn create_database(&self, name: &str) -> Result<()> {
            self.increment_operation_count();
            let mut databases = self.databases.lock().unwrap();
            if !databases.contains(&name.to_string()) {
                databases.push(name.to_string());
            }

            let mut data = self.data.lock().unwrap();
            data.insert(name.to_string(), HashMap::new());
            Ok(())
        }

        async fn drop_database(&self, name: &str) -> Result<()> {
            self.increment_operation_count();
            let mut databases = self.databases.lock().unwrap();
            databases.retain(|db| db != name);

            let mut data = self.data.lock().unwrap();
            data.remove(name);
            Ok(())
        }

        async fn database_exists(&self, name: &str) -> Result<bool> {
            self.increment_operation_count();
            let databases = self.databases.lock().unwrap();
            Ok(databases.contains(&name.to_string()))
        }

        async fn create_table(&self, db: &str, table: &str) -> Result<()> {
            self.increment_operation_count();
            let mut data = self.data.lock().unwrap();
            if let Some(db_data) = data.get_mut(db) {
                db_data.insert(table.to_string(), HashMap::new());
                Ok(())
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn drop_table(&self, db: &str, table: &str) -> Result<()> {
            self.increment_operation_count();
            let mut data = self.data.lock().unwrap();
            if let Some(db_data) = data.get_mut(db) {
                db_data.remove(table);
                Ok(())
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn table_exists(&self, db: &str, table: &str) -> Result<bool> {
            self.increment_operation_count();
            let data = self.data.lock().unwrap();
            if let Some(db_data) = data.get(db) {
                Ok(db_data.contains_key(table))
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()> {
            self.increment_operation_count();
            let mut data = self.data.lock().unwrap();
            if let Some(db_data) = data.get_mut(db) {
                if let Some(table_data) = db_data.get_mut(table) {
                    table_data.insert(key.to_string(), doc.clone());
                    Ok(())
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn put_batch(
            &self,
            db: &str,
            table: &str,
            docs: &[(String, Document)],
        ) -> Result<()> {
            self.increment_operation_count();
            let mut data = self.data.lock().unwrap();
            if let Some(db_data) = data.get_mut(db) {
                if let Some(table_data) = db_data.get_mut(table) {
                    for (key, doc) in docs {
                        table_data.insert(key.clone(), doc.clone());
                    }
                    Ok(())
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>> {
            self.increment_operation_count();
            let data = self.data.lock().unwrap();
            if let Some(db_data) = data.get(db) {
                if let Some(table_data) = db_data.get(table) {
                    Ok(table_data.get(key).cloned())
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn scan_table(
            &self,
            db: &str,
            table: &str,
            _start_key: Option<String>,
            limit: Option<usize>,
            skip: Option<usize>,
            predicate: Option<Box<dyn Fn(Document) -> bool + Send + Sync>>,
        ) -> Result<ReceiverStream<Result<Document>>> {
            self.increment_operation_count();
            let (tx, rx) = mpsc::channel(100);

            let data = self.data.lock().unwrap();
            if let Some(db_data) = data.get(db) {
                if let Some(table_data) = db_data.get(table) {
                    let mut docs: Vec<Document> = table_data.values().cloned().collect();

                    if let Some(pred) = predicate {
                        docs.retain(|doc| pred(doc.clone()));
                    }

                    let skip = skip.unwrap_or(0);
                    let limit = limit.unwrap_or(docs.len());
                    let docs: Vec<Document> = docs.into_iter().skip(skip).take(limit).collect();

                    tokio::spawn(async move {
                        for doc in docs {
                            if tx.send(Ok(doc)).await.is_err() {
                                break;
                            }
                        }
                    });

                    Ok(ReceiverStream::new(rx))
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()> {
            self.increment_operation_count();
            let mut data = self.data.lock().unwrap();
            if let Some(db_data) = data.get_mut(db) {
                if let Some(table_data) = db_data.get_mut(table) {
                    table_data.remove(key);
                    Ok(())
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn stream_databases(
            &self,
            _start_key: Option<String>,
            limit: Option<usize>,
            skip: Option<usize>,
        ) -> Result<ReceiverStream<Result<String>>> {
            self.increment_operation_count();
            let (tx, rx) = mpsc::channel(100);

            let databases = self.databases.lock().unwrap();
            let dbs = databases.clone();

            let skip_count = skip.unwrap_or(0);
            let limit_count = limit.unwrap_or(dbs.len());
            let dbs: Vec<String> = dbs.into_iter().skip(skip_count).take(limit_count).collect();

            tokio::spawn(async move {
                for db in dbs {
                    if tx.send(Ok(db)).await.is_err() {
                        break;
                    }
                }
            });

            Ok(ReceiverStream::new(rx))
        }

        async fn stream_tables(
            &self,
            db: &str,
            _start_key: Option<String>,
            limit: Option<usize>,
            skip: Option<usize>,
        ) -> Result<ReceiverStream<Result<String>>> {
            self.increment_operation_count();
            let (tx, rx) = mpsc::channel(100);

            let data = self.data.lock().unwrap();
            if let Some(db_data) = data.get(db) {
                let tables: Vec<String> = db_data.keys().cloned().collect();

                let skip_count = skip.unwrap_or(0);
                let limit_count = limit.unwrap_or(tables.len());
                let tables: Vec<String> = tables
                    .into_iter()
                    .skip(skip_count)
                    .take(limit_count)
                    .collect();

                tokio::spawn(async move {
                    for table in tables {
                        if tx.send(Ok(table)).await.is_err() {
                            break;
                        }
                    }
                });

                Ok(ReceiverStream::new(rx))
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }

        async fn stream_get_all(
            &self,
            db: &str,
            table: &str,
            keys: &[String],
            _start_key: Option<String>,
            limit: Option<usize>,
            skip: Option<usize>,
        ) -> Result<ReceiverStream<Result<Document>>> {
            self.increment_operation_count();
            let (tx, rx) = mpsc::channel(100);

            let data = self.data.lock().unwrap();
            if let Some(db_data) = data.get(db) {
                if let Some(table_data) = db_data.get(table) {
                    let mut docs = Vec::new();
                    for key in keys {
                        if let Some(doc) = table_data.get(key) {
                            docs.push(doc.clone());
                        }
                    }

                    let skip = skip.unwrap_or(0);
                    let limit = limit.unwrap_or(docs.len());
                    let docs: Vec<Document> = docs.into_iter().skip(skip).take(limit).collect();

                    tokio::spawn(async move {
                        for doc in docs {
                            if tx.send(Ok(doc)).await.is_err() {
                                break;
                            }
                        }
                    });

                    Ok(ReceiverStream::new(rx))
                } else {
                    Err(StorageError::InvalidTableName(table.to_string()))
                }
            } else {
                Err(StorageError::InvalidDatabaseName(db.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_document_serialization_roundtrip() {
        // Create a document exactly like the one in our failing test
        let mut doc = Document::new();
        doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String("test1".to_string())),
            },
        );
        doc.insert(
            "value".to_string(),
            Datum {
                value: Some(datum::Value::String("hello".to_string())),
            },
        );

        println!("Original document: {:?}", doc);

        // Test the exact same serialization used in storage
        let serialized = bincode::serde::encode_to_vec(&doc, bincode::config::standard())
            .expect("Failed to serialize document");
        println!("Serialized: {} bytes", serialized.len());

        // Test the exact same deserialization used in storage
        let result = parse_doc(&serialized);
        match result {
            Ok(deserialized) => {
                println!("Successfully deserialized: {:?}", deserialized);
                assert_eq!(doc, deserialized);
            }
            Err(e) => {
                panic!("Failed to deserialize: {}", e);
            }
        }
    }

    #[test]
    fn test_individual_datum_serialization() {
        let datum = Datum {
            value: Some(datum::Value::String("test".to_string())),
        };

        let serialized = bincode::serde::encode_to_vec(&datum, bincode::config::standard())
            .expect("Failed to serialize datum");
        let (deserialized, _): (Datum, _) =
            bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
                .expect("Failed to deserialize datum");

        assert_eq!(datum, deserialized);
    }

    #[test]
    fn test_document_to_datum_conversion() {
        // Create a document exactly like what's stored in storage
        let mut doc = Document::new();
        doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String("test1".to_string())),
            },
        );
        doc.insert(
            "value".to_string(),
            Datum {
                value: Some(datum::Value::String("hello".to_string())),
            },
        );

        println!("Original document (BTreeMap): {:?}", doc);

        // Convert to Datum using the same conversion as in evaluator
        let datum: Datum = doc.into();
        println!("Converted to Datum: {:?}", datum);

        // Test serialization of the converted Datum (this might be where it fails)
        match bincode::serde::encode_to_vec(&datum, bincode::config::standard()) {
            Ok(serialized) => {
                println!(
                    "✓ Datum serialization successful: {} bytes",
                    serialized.len()
                );

                // Test deserialization
                let decode_result: std::result::Result<(Datum, usize), _> =
                    bincode::serde::decode_from_slice(&serialized, bincode::config::standard());
                match decode_result {
                    Ok((deserialized, _)) => {
                        println!("✓ Datum deserialization successful");
                        assert_eq!(datum, deserialized);
                    }
                    Err(e) => {
                        panic!("❌ Datum deserialization failed: {}", e);
                    }
                }
            }
            Err(e) => {
                panic!("❌ Datum serialization failed: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_storage_layer_roundtrip() {
        use tempfile::TempDir;

        println!("=== Testing Direct Storage Layer Operations ===");

        // Create temporary storage
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test data
        let db_name = "test_db";
        let table_name = "test_table";
        let key = "test_key";

        // Create a document exactly like in our failing case
        let mut original_doc = Document::new();
        original_doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String("test1".to_string())),
            },
        );
        original_doc.insert(
            "value".to_string(),
            Datum {
                value: Some(datum::Value::String("hello".to_string())),
            },
        );

        println!("Original document: {:?}", original_doc);

        // Step 1: Create database
        println!("Step 1: Creating database...");
        storage
            .create_database(db_name)
            .await
            .expect("Failed to create database");
        println!("✓ Database created");

        // Step 2: Create table
        println!("Step 2: Creating table...");
        storage
            .create_table(db_name, table_name)
            .await
            .expect("Failed to create table");
        println!("✓ Table created");

        // Step 3: Put document
        println!("Step 3: Storing document...");
        storage
            .put(db_name, table_name, key, &original_doc)
            .await
            .expect("Failed to put document");
        println!("✓ Document stored");

        // Step 4: Get document (this is where the error should occur if it's a storage issue)
        println!("Step 4: Retrieving document...");
        match storage.get(db_name, table_name, key).await {
            Ok(Some(retrieved_doc)) => {
                println!("✓ Document retrieved successfully");
                println!("Retrieved document: {:?}", retrieved_doc);

                // Verify they match
                if original_doc == retrieved_doc {
                    println!("✓ Documents match perfectly");
                } else {
                    panic!(
                        "❌ Documents don't match!\nOriginal: {:?}\nRetrieved: {:?}",
                        original_doc, retrieved_doc
                    );
                }
            }
            Ok(None) => {
                panic!("❌ Document not found");
            }
            Err(e) => {
                panic!("❌ Failed to retrieve document: {}", e);
            }
        }

        println!("=== Storage layer test completed successfully ===");
    }

    #[tokio::test]
    async fn test_raw_storage_data_verification() {
        use tempfile::TempDir;

        println!("=== Testing Raw Storage Data ===");

        // Create temporary storage
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test data
        let db_name = "test_db";
        let table_name = "test_table";
        let key = "test_key";

        // Create a simple document
        let mut original_doc = Document::new();
        original_doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String("test1".to_string())),
            },
        );
        original_doc.insert(
            "value".to_string(),
            Datum {
                value: Some(datum::Value::String("hello".to_string())),
            },
        );

        println!("Original document: {:?}", original_doc);

        // Setup storage
        storage
            .create_database(db_name)
            .await
            .expect("Failed to create database");
        storage
            .create_table(db_name, table_name)
            .await
            .expect("Failed to create table");

        // Store the document
        println!("Storing document...");
        storage
            .put(db_name, table_name, key, &original_doc)
            .await
            .expect("Failed to put document");
        println!("✓ Document stored");

        // Now let's examine what was actually stored by accessing RocksDB directly
        println!("Examining raw stored data...");
        let table_full_name = format_table_name(db_name, table_name);

        // Access the raw database
        let inner_db = &storage.inner;
        let cf = get_cf_cache()
            .get(&table_full_name, inner_db)
            .expect("Column family not found");

        match inner_db.get_cf_opt(&cf, key, &DefaultStorage::create_read_opts()) {
            Ok(Some(raw_bytes)) => {
                println!("✓ Raw data found: {} bytes", raw_bytes.len());
                println!(
                    "Raw bytes (first 100): {:?}",
                    &raw_bytes[..raw_bytes.len().min(100)]
                );

                // Try to deserialize the raw bytes manually
                println!("Attempting manual deserialization...");
                let decode_result: std::result::Result<(Document, usize), _> =
                    bincode::serde::decode_from_slice(&raw_bytes, bincode::config::standard());
                match decode_result {
                    Ok((deserialized, _)) => {
                        println!("✓ Manual deserialization successful");
                        println!("Manually deserialized: {:?}", deserialized);

                        if original_doc == deserialized {
                            println!("✓ Manual deserialization matches original");
                        } else {
                            println!("❌ Manual deserialization differs from original");
                        }
                    }
                    Err(e) => {
                        println!("❌ Manual deserialization failed: {}", e);
                        println!("Error details: {:?}", e);

                        // Try to understand the error better
                        println!("Attempting to diagnose the raw data...");

                        // Try deserializing as different types to understand the structure
                        let btreemap_result: std::result::Result<
                            (std::collections::BTreeMap<String, String>, usize),
                            _,
                        > = bincode::serde::decode_from_slice(
                            &raw_bytes,
                            bincode::config::standard(),
                        );
                        if let Ok((as_btreemap, _)) = btreemap_result {
                            println!(
                                "Could deserialize as BTreeMap<String, String>: {:?}",
                                as_btreemap
                            );
                        }

                        let hashmap_result: std::result::Result<
                            (HashMap<String, String>, usize),
                            _,
                        > = bincode::serde::decode_from_slice(
                            &raw_bytes,
                            bincode::config::standard(),
                        );
                        if let Ok((as_hashmap, _)) = hashmap_result {
                            println!(
                                "Could deserialize as HashMap<String, String>: {:?}",
                                as_hashmap
                            );
                        }
                    }
                }

                // Also try to use the storage's parse_doc function
                println!("Testing storage's parse_doc function...");
                match parse_doc(&raw_bytes) {
                    Ok(parsed) => {
                        println!("✓ parse_doc successful: {:?}", parsed);
                    }
                    Err(e) => {
                        println!("❌ parse_doc failed: {}", e);
                        println!("This confirms the issue is in deserialization");
                    }
                }
            }
            Ok(None) => {
                panic!("❌ No data found for key '{}'", key);
            }
            Err(e) => {
                panic!("❌ Failed to read raw data: {}", e);
            }
        }

        // Compare with what the normal storage API returns
        println!("Testing normal storage API...");
        match storage.get(db_name, table_name, key).await {
            Ok(Some(retrieved)) => {
                println!("✓ Normal storage API successful: {:?}", retrieved);
            }
            Ok(None) => {
                println!("❌ Normal storage API returned None");
            }
            Err(e) => {
                println!("❌ Normal storage API failed: {}", e);
                println!("This is where our actual error occurs!");
            }
        }

        println!("=== Raw storage data test completed ===");
    }

    #[tokio::test]
    async fn test_server_async_execution_pattern() {
        use std::sync::Arc;
        use tempfile::TempDir;
        use tokio::task;

        println!("=== Testing Server-like Async Execution Pattern ===");

        // Create temporary storage exactly like the server does
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = Arc::new(DefaultStorage::open(&config).expect("Failed to create storage"));

        // Test data
        let db_name = "test_db";
        let table_name = "test_table";
        let key = "test_key";

        // Create document
        let mut original_doc = Document::new();
        original_doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::String("test1".to_string())),
            },
        );
        original_doc.insert(
            "value".to_string(),
            Datum {
                value: Some(datum::Value::String("hello".to_string())),
            },
        );

        println!("Original document: {:?}", original_doc);

        // Step 1: Setup storage (like server initialization)
        println!("Step 1: Setting up storage...");
        let storage_clone = storage.clone();
        task::spawn(async move {
            storage_clone
                .create_database(db_name)
                .await
                .expect("Failed to create database");
        })
        .await
        .expect("Database creation task failed");

        let storage_clone = storage.clone();
        task::spawn(async move {
            storage_clone
                .create_table(db_name, table_name)
                .await
                .expect("Failed to create table");
        })
        .await
        .expect("Table creation task failed");

        println!("✓ Storage setup completed");

        // Step 2: Insert operation (like server insert handling)
        println!("Step 2: Async insert operation...");
        let storage_clone = storage.clone();
        let doc_clone = original_doc.clone();
        let insert_result = task::spawn(async move {
            storage_clone
                .put(db_name, table_name, key, &doc_clone)
                .await
        })
        .await
        .expect("Insert task panicked");

        match insert_result {
            Ok(()) => println!("✓ Async insert successful"),
            Err(e) => panic!("❌ Async insert failed: {}", e),
        }

        // Step 3: Get operation (like server get handling) - this should reproduce the error
        println!("Step 3: Async get operation...");
        let storage_clone = storage.clone();
        let get_result =
            task::spawn(async move { storage_clone.get(db_name, table_name, key).await })
                .await
                .expect("Get task panicked");

        match get_result {
            Ok(Some(retrieved)) => {
                println!("✓ Async get successful: {:?}", retrieved);

                // Test the same conversion path as the evaluator
                println!("Step 4: Testing evaluator-style conversion...");
                let datum_result: Datum = retrieved.into();
                println!("✓ Document to Datum conversion: {:?}", datum_result);
            }
            Ok(None) => {
                panic!("❌ Async get returned None");
            }
            Err(e) => {
                println!("❌ Async get failed: {}", e);
                println!("Error type: {:?}", e);
                panic!("This should reproduce the server error!");
            }
        }

        // Step 5: Test concurrent operations (like real server load)
        println!("Step 5: Testing concurrent operations...");
        let mut tasks = vec![];

        for i in 0..5 {
            let storage_clone = storage.clone();
            let test_key = format!("concurrent_key_{}", i);
            let test_doc = original_doc.clone();

            // Spawn concurrent insert
            let insert_task = task::spawn(async move {
                storage_clone
                    .put(db_name, table_name, &test_key, &test_doc)
                    .await
            });
            tasks.push(insert_task);
        }

        // Wait for all inserts
        for (i, task) in tasks.into_iter().enumerate() {
            match task.await.expect("Concurrent insert task panicked") {
                Ok(()) => println!("✓ Concurrent insert {} successful", i),
                Err(e) => panic!("❌ Concurrent insert {} failed: {}", i, e),
            }
        }

        // Now test concurrent gets
        let mut get_tasks = vec![];
        for i in 0..5 {
            let storage_clone = storage.clone();
            let test_key = format!("concurrent_key_{}", i);

            let get_task =
                task::spawn(async move { storage_clone.get(db_name, table_name, &test_key).await });
            get_tasks.push((i, get_task));
        }

        // Check concurrent get results
        for (i, task) in get_tasks.into_iter() {
            match task.await.expect("Concurrent get task panicked") {
                Ok(Some(_)) => println!("✓ Concurrent get {} successful", i),
                Ok(None) => panic!("❌ Concurrent get {} returned None", i),
                Err(e) => {
                    println!("❌ Concurrent get {} failed: {}", i, e);
                    panic!("Concurrent operation error: {}", e);
                }
            }
        }

        println!("=== Server-like async execution test completed successfully ===");
    }

    #[test]
    fn test_system_table_variants() {
        let variants = SystemTable::variants();
        assert_eq!(variants.len(), 5);
        assert!(variants.contains(&SystemTable::Databases));
        assert!(variants.contains(&SystemTable::Schemas));
        assert!(variants.contains(&SystemTable::Indexes));
        assert!(variants.contains(&SystemTable::Feeds));
        assert!(variants.contains(&SystemTable::Meta));
    }

    #[test]
    fn test_system_table_display() {
        assert_eq!(SystemTable::Databases.to_string(), "__databases__");
        assert_eq!(SystemTable::Schemas.to_string(), "__schemas__");
        assert_eq!(SystemTable::Indexes.to_string(), "__indexes__");
        assert_eq!(SystemTable::Feeds.to_string(), "__feeds__");
        assert_eq!(SystemTable::Meta.to_string(), "__meta__");
    }

    #[test]
    fn test_storage_error_display() {
        let storage_error = StorageError::MissingColumnFamily("test_cf".to_string());
        assert_eq!(storage_error.to_string(), "Missing column family: test_cf");

        let storage_error = StorageError::InvalidDatabaseName("test_db".to_string());
        assert_eq!(storage_error.to_string(), "Invalid database name: test_db");

        let storage_error = StorageError::InvalidTableName("test_table".to_string());
        assert_eq!(storage_error.to_string(), "Invalid table name: test_table");

        let storage_error = StorageError::ResourceExhausted;
        assert_eq!(
            storage_error.to_string(),
            "Resource exhausted - too many concurrent operations"
        );
    }

    #[test]
    fn test_storage_error_source() {
        use std::error::Error;

        let storage_error = StorageError::MissingColumnFamily("test".to_string());
        assert!(storage_error.source().is_none());

        let storage_error = StorageError::ResourceExhausted;
        assert!(storage_error.source().is_none());
    }

    #[test]
    fn test_storage_error_from_utf8() {
        // Test FromUtf8Error conversion
        let invalid_bytes = vec![0, 159, 146, 150];
        let utf8_error = String::from_utf8(invalid_bytes).unwrap_err();
        let storage_error: StorageError = utf8_error.into();
        matches!(storage_error, StorageError::InvalidUtf8(_));
    }

    #[test]
    fn test_storage_error_from_bincode() {
        // Test encode error case
        let test_data: Vec<u8> = vec![1, 2, 3, 4];
        let mut small_buffer = [0u8; 1]; // Too small buffer to cause encode error

        if let Err(encode_error) = bincode::serde::encode_into_slice(
            &test_data,
            &mut small_buffer,
            bincode::config::standard(),
        ) {
            let storage_error: StorageError = encode_error.into();
            matches!(storage_error, StorageError::EncodeError(_));
        }

        // Test decode error case - invalid data
        let invalid_data = [255u8; 10];
        let decode_result: std::result::Result<(Vec<String>, usize), _> =
            bincode::serde::decode_from_slice(&invalid_data, bincode::config::standard());
        if let Err(decode_error) = decode_result {
            let storage_error: StorageError = decode_error.into();
            matches!(storage_error, StorageError::DecodeError(_));
        }
    }

    #[test]
    fn test_is_valid_key() {
        assert!(is_valid_key("valid_key"));
        assert!(is_valid_key("valid-key"));
        assert!(is_valid_key("validkey123"));
        assert!(is_valid_key("a"));
        assert!(is_valid_key("test_table_123"));

        assert!(!is_valid_key(""));
        assert!(!is_valid_key("invalid key")); // space
        assert!(!is_valid_key("invalid@key")); // special char
        assert!(!is_valid_key("invalid.key")); // dot
        assert!(!is_valid_key("invalid/key")); // slash
        assert!(!is_valid_key(&"a".repeat(256))); // too long
    }

    #[test]
    fn test_format_table_name() {
        assert_eq!(format_table_name("db", "table"), "db:table");
        assert_eq!(
            format_table_name("test_db", "test_table"),
            "test_db:test_table"
        );
        assert_eq!(format_table_name("", "table"), ":table");
        assert_eq!(format_table_name("db", ""), "db:");
    }

    #[test]
    fn test_extract_table_from_database() {
        assert_eq!(extract_table_from_database("db:table"), Some("table"));
        assert_eq!(
            extract_table_from_database("test_db:test_table"),
            Some("test_table")
        );
        assert_eq!(
            extract_table_from_database("db:table:with:colons"),
            Some("table:with:colons")
        );
        assert_eq!(extract_table_from_database("no_colon"), None);
        assert_eq!(extract_table_from_database(""), None);
        assert_eq!(extract_table_from_database(":"), Some(""));
        assert_eq!(extract_table_from_database("db:"), Some(""));
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.data_dir, "./data");
        assert!(config.max_background_jobs > 0);
        assert!(config.parallelism > 0);
        assert_eq!(config.level_zero_file_num_compaction_trigger, 4);
        assert_eq!(config.level_zero_slowdown_writes_trigger, 20);
        assert_eq!(config.level_zero_stop_writes_trigger, 36);
        assert_eq!(config.block_cache_size, 268_435_456);
        assert_eq!(config.max_open_files, 100);
        assert!(config.use_direct_io_for_flush_and_compaction);
        assert!(!config.use_direct_reads);
        assert_eq!(config.bytes_per_sync, 1_048_576);
        assert_eq!(config.wal_bytes_per_sync, 1_048_576);
        assert_eq!(config.target_file_size_base, 67_108_864);
        assert_eq!(config.max_bytes_for_level_base, 268_435_456);
        assert_eq!(config.write_buffer_size, 67_108_864);
        assert_eq!(config.max_write_buffer_number, 3);
        assert_eq!(config.min_write_buffer_number_to_merge, 1);
    }

    #[test]
    fn test_cf_cache() {
        let cache = CFCache::new();

        // Test that cache starts empty
        // We can't directly test get() without a real database, but we can test the structure
        assert_eq!(cache.access_counter.load(Ordering::Relaxed), 0);

        // Test that the cache map is accessible
        assert!(cache.cache.read().unwrap().is_empty());
    }

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        // DatabaseConfig is empty for now, but this tests the Default implementation
        let _ = format!("{:?}", config); // Just ensure it's debuggable
    }

    #[tokio::test]
    async fn test_create_database_invalid_names() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.create_database("").await.is_err());
        assert!(storage.create_database("invalid name").await.is_err());
        assert!(storage.create_database("invalid@name").await.is_err());
        assert!(storage.create_database("__system__").await.is_err());
        assert!(storage.create_database(&"a".repeat(256)).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_database_invalid_names() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.drop_database("").await.is_err());
        assert!(storage.drop_database("invalid name").await.is_err());
        assert!(storage.drop_database("__system__").await.is_err());
        assert!(storage.drop_database("default").await.is_err());
        assert!(storage.drop_database("nonexistent_db").await.is_ok()); // Should succeed even if doesn't exist
    }

    #[tokio::test]
    async fn test_create_table_invalid_names() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Create a valid database first
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");

        // Test invalid database names
        assert!(storage.create_table("", "table").await.is_err());
        assert!(storage.create_table("__system__", "table").await.is_err());
        assert!(storage.create_table("invalid name", "table").await.is_err());

        // Test invalid table names
        assert!(storage.create_table("test_db", "").await.is_err());
        assert!(
            storage
                .create_table("test_db", "invalid name")
                .await
                .is_err()
        );
        assert!(
            storage
                .create_table("test_db", "invalid@table")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_drop_table_invalid_names() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.drop_table("", "table").await.is_err());
        assert!(storage.drop_table("__system__", "table").await.is_err());
        assert!(storage.drop_table("invalid name", "table").await.is_err());
    }

    #[tokio::test]
    async fn test_put_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        let doc = Document::new();

        // Test invalid database names
        assert!(storage.put("", "table", "key", &doc).await.is_err());
        assert!(
            storage
                .put("__system__", "table", "key", &doc)
                .await
                .is_err()
        );
        assert!(
            storage
                .put("invalid name", "table", "key", &doc)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_put_batch_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        let docs = vec![("key1".to_string(), Document::new())];

        // Test invalid database names
        assert!(storage.put_batch("", "table", &docs).await.is_err());
        assert!(
            storage
                .put_batch("__system__", "table", &docs)
                .await
                .is_err()
        );
        assert!(
            storage
                .put_batch("invalid name", "table", &docs)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_get_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.get("", "table", "key").await.is_err());
        assert!(storage.get("__system__", "table", "key").await.is_err());
        assert!(storage.get("invalid name", "table", "key").await.is_err());
    }

    #[tokio::test]
    async fn test_scan_table_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(
            storage
                .scan_table("", "table", None, None, None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .scan_table("__system__", "table", None, None, None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .scan_table("invalid name", "table", None, None, None, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_delete_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.delete("", "table", "key").await.is_err());
        assert!(storage.delete("__system__", "table", "key").await.is_err());
        assert!(
            storage
                .delete("invalid name", "table", "key")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_stream_tables_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Test invalid database names
        assert!(storage.stream_tables("", None, None, None).await.is_err());
        assert!(
            storage
                .stream_tables("__system__", None, None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .stream_tables("invalid name", None, None, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_stream_get_all_invalid_database() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        let keys = vec!["key1".to_string()];

        // Test invalid database names
        assert!(
            storage
                .stream_get_all("", "table", &[], None, None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .stream_get_all("__system__", "table", &[], None, None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .stream_get_all("invalid name", "table", &keys, None, None, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_put_batch_empty() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Test empty batch
        let empty_docs = vec![];
        assert!(
            storage
                .put_batch("test_db", "test_table", &empty_docs)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Test getting nonexistent key
        let result = storage
            .get("test_db", "test_table", "nonexistent")
            .await
            .expect("Get should succeed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Test deleting nonexistent key (should succeed)
        assert!(
            storage
                .delete("test_db", "test_table", "nonexistent")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_scan_table_with_predicate() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Insert test data
        for i in 0..5 {
            let mut doc = Document::new();
            doc.insert(
                "id".to_string(),
                Datum {
                    value: Some(datum::Value::Float(i as f64)),
                },
            );
            doc.insert(
                "value".to_string(),
                Datum {
                    value: Some(datum::Value::String(format!("value_{}", i))),
                },
            );

            storage
                .put("test_db", "test_table", &format!("key_{}", i), &doc)
                .await
                .expect("Failed to put document");
        }

        // Test scan with predicate that filters even numbers
        let predicate = Box::new(|doc: Document| -> bool {
            if let Some(Datum {
                value: Some(datum::Value::Float(n)),
            }) = doc.get("id")
            {
                *n as i32 % 2 == 0
            } else {
                false
            }
        });

        let mut stream = storage
            .scan_table("test_db", "test_table", None, None, None, Some(predicate))
            .await
            .expect("Failed to scan table");

        let mut count = 0;
        while let Some(result) = stream.next().await {
            let doc = result.expect("Failed to get document from stream");
            if let Some(Datum {
                value: Some(datum::Value::Float(n)),
            }) = doc.get("id")
            {
                assert_eq!(*n as i32 % 2, 0); // Should only get even numbers
                count += 1;
            }
        }

        assert_eq!(count, 3); // Should get 0, 2, 4
    }

    #[tokio::test]
    async fn test_scan_table_with_start_key_and_limit() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Insert test data
        for i in 0..10 {
            let mut doc = Document::new();
            doc.insert(
                "id".to_string(),
                Datum {
                    value: Some(datum::Value::Float(i as f64)),
                },
            );

            storage
                .put("test_db", "test_table", &format!("key_{:02}", i), &doc)
                .await
                .expect("Failed to put document");
        }

        // Test scan with start key and batch size
        let mut stream = storage
            .scan_table(
                "test_db",
                "test_table",
                Some("key_05".to_string()),
                Some(3),
                None,
                None,
            )
            .await
            .expect("Failed to scan table");

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.expect("Failed to get document from stream");
            count += 1;
        }

        assert_eq!(count, 3); // Should get exactly 3 documents starting from key_05
    }

    #[tokio::test]
    async fn test_stream_databases() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Create some test databases
        for i in 0..5 {
            storage
                .create_database(&format!("test_db_{}", i))
                .await
                .expect("Failed to create database");
        }

        // Test streaming all databases
        let mut stream = storage
            .stream_databases(None, None, None)
            .await
            .expect("Failed to stream databases");

        let mut databases = Vec::new();
        while let Some(result) = stream.next().await {
            let db_name = result.expect("Failed to get database from stream");
            databases.push(db_name);
        }

        assert!(databases.len() >= 5); // Should have at least our test databases
        for i in 0..5 {
            assert!(databases.contains(&format!("test_db_{}", i)));
        }
    }

    #[tokio::test]
    async fn test_stream_databases_with_pagination() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Create some test databases
        for i in 0..5 {
            storage
                .create_database(&format!("db_{:02}", i))
                .await
                .expect("Failed to create database");
        }

        // Test streaming with start key and batch size
        let mut stream = storage
            .stream_databases(Some("db1".to_string()), Some(1), None)
            .await
            .expect("Failed to stream databases");

        let mut databases = Vec::new();
        while let Some(result) = stream.next().await {
            let db_name = result.expect("Failed to get database from stream");
            databases.push(db_name);
        }

        assert_eq!(databases.len(), 1); // Should get exactly 1 database with limit=1
    }

    #[tokio::test]
    async fn test_stream_tables() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");

        // Create some test tables
        for i in 0..3 {
            storage
                .create_table("test_db", &format!("table_{}", i))
                .await
                .expect("Failed to create table");
        }

        // Test streaming tables
        let mut stream = storage
            .stream_tables("test_db", None, None, None)
            .await
            .expect("Failed to stream tables");

        let mut tables = Vec::new();
        while let Some(result) = stream.next().await {
            let table_name = result.expect("Failed to get table from stream");
            tables.push(table_name);
        }

        // Should have default table plus our created tables
        assert!(tables.len() >= 3);
        for i in 0..3 {
            assert!(tables.contains(&format!("table_{}", i)));
        }
    }

    #[tokio::test]
    async fn test_stream_get_all() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Insert test data
        let mut keys = Vec::new();
        for i in 0..5 {
            let key = format!("key_{}", i);
            keys.push(key.clone());

            let mut doc = Document::new();
            doc.insert(
                "id".to_string(),
                Datum {
                    value: Some(datum::Value::Float(i as f64)),
                },
            );

            storage
                .put("test_db", "test_table", &key, &doc)
                .await
                .expect("Failed to put document");
        }

        // Test streaming get all
        let mut stream = storage
            .stream_get_all("test_db", "test_table", &keys, None, None, None)
            .await
            .expect("Failed to stream get all");

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.expect("Failed to get document from stream");
            count += 1;
        }

        assert_eq!(count, 5); // Should get all 5 documents
    }

    #[tokio::test]
    async fn test_stream_get_all_with_missing_keys() {
        use tempfile::TempDir;
        use tokio_stream::StreamExt;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Insert only some of the test data
        let mut doc = Document::new();
        doc.insert(
            "id".to_string(),
            Datum {
                value: Some(datum::Value::Float(1.0)),
            },
        );
        storage
            .put("test_db", "test_table", "key_1", &doc)
            .await
            .expect("Failed to put document");

        // Try to get both existing and non-existing keys
        let keys = vec![
            "key_0".to_string(),
            "key_1".to_string(),
            "key_2".to_string(),
        ];
        let mut stream = storage
            .stream_get_all("test_db", "test_table", &keys, None, None, None)
            .await
            .expect("Failed to stream get all");

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.expect("Failed to get document from stream");
            count += 1;
        }

        assert_eq!(count, 1); // Should get only the existing document
    }

    #[test]
    fn test_serialize_batch() {
        let mut doc1 = Document::new();
        doc1.insert(
            "key1".to_string(),
            Datum {
                value: Some(datum::Value::String("value1".to_string())),
            },
        );

        let mut doc2 = Document::new();
        doc2.insert(
            "key2".to_string(),
            Datum {
                value: Some(datum::Value::Float(42.0)),
            },
        );

        let docs = vec![
            ("batch_key1".to_string(), doc1),
            ("batch_key2".to_string(), doc2),
        ];

        let result = DefaultStorage::serialize_batch(&docs).expect("Failed to serialize batch");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "batch_key1");
        assert_eq!(result[1].0, "batch_key2");
        assert!(!result[0].1.is_empty());
        assert!(!result[1].1.is_empty());
    }

    #[test]
    fn test_create_read_opts() {
        let _opts = DefaultStorage::create_read_opts();
        // Can't directly test private fields, but ensure it doesn't panic
    }

    #[test]
    fn test_create_write_opts() {
        let _opts = DefaultStorage::create_write_opts();
        // Can't directly test private fields, but ensure it doesn't panic
    }

    #[tokio::test]
    async fn test_storage_with_custom_config() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            max_background_jobs: 2,
            parallelism: 1,
            block_cache_size: 1024 * 1024, // 1MB
            max_open_files: 50,
            write_buffer_size: 1024 * 1024, // 1MB
            ..Default::default()
        };

        let storage =
            DefaultStorage::open(&config).expect("Failed to create storage with custom config");

        // Basic operations should still work
        storage
            .create_database("custom_test")
            .await
            .expect("Failed to create database");
        storage
            .create_table("custom_test", "table")
            .await
            .expect("Failed to create table");

        let mut doc = Document::new();
        doc.insert(
            "test".to_string(),
            Datum {
                value: Some(datum::Value::String("custom_config_test".to_string())),
            },
        );

        storage
            .put("custom_test", "table", "key", &doc)
            .await
            .expect("Failed to put with custom config");
        let result = storage
            .get("custom_test", "table", "key")
            .await
            .expect("Failed to get with custom config");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_doc_error_handling() {
        // Test with invalid data
        let invalid_data = b"not valid bincode data";
        let result = parse_doc(invalid_data);
        assert!(result.is_err());
        matches!(result.unwrap_err(), StorageError::DecodeError(_));

        // Test with empty data
        let empty_data = b"";
        let result = parse_doc(empty_data);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_missing_column_family_errors() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Try to access a table that doesn't exist
        let doc = Document::new();

        // These should fail because the column family doesn't exist
        let result = storage
            .put("nonexistent_db", "nonexistent_table", "key", &doc)
            .await;
        assert!(result.is_err());

        let result = storage
            .get("nonexistent_db", "nonexistent_table", "key")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_database_lifecycle() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        let db_name = "lifecycle_test_db";
        let table_name = "test_table";

        // Create database
        storage
            .create_database(db_name)
            .await
            .expect("Failed to create database");

        // Create table
        storage
            .create_table(db_name, table_name)
            .await
            .expect("Failed to create table");

        // Add some data
        let mut doc = Document::new();
        doc.insert(
            "test".to_string(),
            Datum {
                value: Some(datum::Value::String("lifecycle_test".to_string())),
            },
        );
        storage
            .put(db_name, table_name, "test_key", &doc)
            .await
            .expect("Failed to put document");

        // Verify data exists
        let result = storage
            .get(db_name, table_name, "test_key")
            .await
            .expect("Failed to get document");
        assert!(result.is_some());

        // Drop table
        storage
            .drop_table(db_name, table_name)
            .await
            .expect("Failed to drop table");

        // Data should no longer be accessible
        let result = storage.get(db_name, table_name, "test_key").await;
        assert!(result.is_err()); // Should fail because table doesn't exist

        // Drop database
        storage
            .drop_database(db_name)
            .await
            .expect("Failed to drop database");
    }

    #[tokio::test]
    async fn test_large_document_handling() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        // Create a large document
        let mut large_doc = Document::new();
        for i in 0..1000 {
            large_doc.insert(
                format!("field_{}", i),
                Datum {
                    value: Some(datum::Value::String(format!(
                        "value_{}_with_some_longer_content_to_make_it_bigger",
                        i
                    ))),
                },
            );
        }

        // Should be able to store and retrieve large documents
        storage
            .put("test_db", "test_table", "large_key", &large_doc)
            .await
            .expect("Failed to put large document");
        let result = storage
            .get("test_db", "test_table", "large_key")
            .await
            .expect("Failed to get large document");
        assert!(result.is_some());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.len(), 1000);
    }

    #[tokio::test]
    async fn test_edge_case_key_names() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = Config {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = DefaultStorage::open(&config).expect("Failed to create storage");

        // Setup
        storage
            .create_database("test_db")
            .await
            .expect("Failed to create database");
        storage
            .create_table("test_db", "test_table")
            .await
            .expect("Failed to create table");

        let doc = Document::new();

        // Test edge case valid keys
        let max_length_key = "a".repeat(255);
        let valid_keys = vec![
            "a",
            "single_char",
            "with-dashes",
            "with_underscores",
            "alphanumeric123",
            "A",
            "MixedCase123",
            &max_length_key, // Maximum length
        ];

        for key in valid_keys {
            storage
                .put("test_db", "test_table", key, &doc)
                .await
                .unwrap_or_else(|_| panic!("Failed to put document with key: {}", key));

            let result = storage
                .get("test_db", "test_table", key)
                .await
                .unwrap_or_else(|_| panic!("Failed to get document with key: {}", key));

            assert!(result.is_some());
        }
    }
}
