use crate::ast::Datum;
use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, ReadOptions, SliceTransform,
    WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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

pub type Document = BTreeMap<String, Datum>;

#[derive(Debug)]
pub enum StorageError {
    BackendError(rocksdb::Error),
    InvalidUtf8(std::string::FromUtf8Error),
    InvalidDocument(rmp_serde::decode::Error),
    EncodeError(rmp_serde::encode::Error),
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

impl From<rmp_serde::decode::Error> for StorageError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::InvalidDocument(value)
    }
}

impl From<rmp_serde::encode::Error> for StorageError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::EncodeError(value)
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

// Cached column family handle to avoid repeated lookups
#[derive(Clone)]
struct CachedCF {
    handle: Arc<rocksdb::BoundColumnFamily<'static>>,
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

    fn get(
        &self,
        key: &str,
        db: &Arc<DBWithThreadMode<MultiThreaded>>,
    ) -> Option<Arc<rocksdb::BoundColumnFamily<'static>>> {
        let access_time = self.access_counter.fetch_add(1, Ordering::Relaxed);

        // Try read lock first for fast path
        if let Ok(cache) = self.cache.read() {
            if let Some(cached) = cache.get(key) {
                cached.last_access.store(access_time, Ordering::Relaxed);
                return Some(cached.handle.clone());
            }
        }

        // Fallback to database lookup and cache insertion
        if let Some(cf) = db.cf_handle(key) {
            // Convert to static lifetime - this is safe because the DB outlives the cache
            let static_cf: Arc<rocksdb::BoundColumnFamily<'static>> =
                unsafe { std::mem::transmute(cf) };

            let cached_cf = CachedCF {
                handle: static_cf.clone(),
                last_access: Arc::new(AtomicUsize::new(access_time)),
            };

            // Try to insert into cache
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
                return Some(static_cf);
            }
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
    // Future: database-specific configuration
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<String>>;
    async fn create_database(&self, name: &str) -> Result<()>;
    async fn drop_database(&self, name: &str) -> Result<()>;
    async fn list_tables(&self, db: &str) -> Result<Vec<String>>;
    async fn create_table(&self, db: &str, table: &str) -> Result<()>;
    async fn drop_table(&self, db: &str, table: &str) -> Result<()>;
    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()>;
    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()>;
    async fn get(&self, db: &str, table: &str, key: &str) -> Result<Option<Document>>;
    async fn scan_table(
        &self,
        db: &str,
        table: &str,
        start_key: Option<String>,
        batch_size: Option<usize>,
        predicate: Option<Box<dyn Fn(Document) -> bool + Send + Sync>>,
    ) -> Result<ReceiverStream<Result<Document>>>;
    async fn delete(&self, db: &str, table: &str, key: &str) -> Result<()>;
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
            block_cache_size: 268_435_456,
            max_open_files: 1000,
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
                let serialized = rmp_serde::to_vec(d)?;
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
    async fn list_databases(&self) -> Result<Vec<String>> {
        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let read_opts = Self::create_read_opts();

        spawn_blocking(move || {
            let cf = inner_db
                .cf_handle(&SystemTable::Databases.to_string())
                .ok_or_else(|| {
                    StorageError::MissingColumnFamily(SystemTable::Databases.to_string())
                })?;

            let mut databases = Vec::new();
            let iterator = inner_db.iterator_cf_opt(&cf, read_opts, IteratorMode::Start);

            for res in iterator {
                let (key, _) = res?;
                let db_name = String::from_utf8(key.to_vec())?;
                if db_name != SYSTEM_DATABASE {
                    databases.push(db_name);
                }
            }

            Ok(databases)
        })
        .await
        .unwrap()
    }

    async fn create_database(&self, name: &str) -> Result<()> {
        if !is_valid_key(name) || name == SYSTEM_DATABASE {
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
            let serialized = rmp_serde::to_vec(&db_config)?;

            inner_db.put_cf_opt(&cf, &name, serialized, &write_opts)?;

            let table_cf_name = format_table_name(&name, "default");
            inner_db.create_cf(&table_cf_name, &Options::default())?;

            Ok(())
        })
        .await
        .unwrap()
    }

    async fn drop_database(&self, name: &str) -> Result<()> {
        if !is_valid_key(name) || name == SYSTEM_DATABASE || name == DEFAULT_DATABASE {
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

    async fn list_tables(&self, db: &str) -> Result<Vec<String>> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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

        spawn_blocking(move || {
            let prefix = format!("{db}:");
            let tables: Vec<String> = DB::list_cf(&opts, &path)
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

            Ok(tables)
        })
        .await
        .unwrap()
    }

    async fn create_table(&self, db: &str, table: &str) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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

    async fn put(&self, db: &str, table: &str, key: &str, doc: &Document) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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
        let doc = rmp_serde::to_vec(doc)?;
        let write_opts = Self::create_write_opts();

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            inner_db.put_cf_opt(&cf, key, doc, &write_opts)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn put_batch(&self, db: &str, table: &str, docs: &[(String, Document)]) -> Result<()> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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
        batch_size: Option<usize>,
        predicate: Option<Box<dyn Fn(Document) -> bool + Send + Sync>>,
    ) -> Result<ReceiverStream<Result<Document>>> {
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
            return Err(StorageError::InvalidDatabaseName(db.to_string()));
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::ResourceExhausted)?;

        let inner_db = self.inner.clone();
        let table_name = format_table_name(db, table);
        let batch_size = batch_size.unwrap_or(DEFAULT_STREAMING_LIMIT);
        let read_opts = Self::create_read_opts();
        let (tx, rx) = mpsc::channel(batch_size.min(1000));

        spawn_blocking(move || {
            let cf = get_cf_cache()
                .get(&table_name, &inner_db)
                .ok_or_else(|| StorageError::MissingColumnFamily(table_name.clone()))?;

            let mode = start_key.as_ref().map_or(IteratorMode::Start, |key| {
                IteratorMode::From(key.as_bytes(), Direction::Forward)
            });

            let iterator = inner_db
                .iterator_cf_opt(&cf, read_opts, mode)
                .skip(usize::from(start_key.is_some()));

            let mut count = 0;
            for res in iterator {
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

                            count += 1;
                            if count >= batch_size {
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
        if !is_valid_key(db) || db == SYSTEM_DATABASE {
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
    rmp_serde::from_slice(data).map_err(StorageError::InvalidDocument)
}
