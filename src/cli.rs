use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    version,
    name = "RuloDB",
    about = r#"
A simple document database built for developer ergonomics.

RuloDB is a simple document database designed for developer ergonomics. It
provides a straightforward API for storing and retrieving documents, with a
focus on ease of use and performance."#
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the server
    Start(StartCommand),
}

/// Configuration for the server, including database path and address.
#[derive(Debug, Clone, Args)]
pub struct ServerConfig {
    /// Address for the database server.
    #[arg(long, short, env = "RULODB_ADDRESS", default_value = "127.0.0.1:6969")]
    pub address: String,
}

#[derive(Debug, Clone, Args)]
pub struct EngineConfig {
    /// Path to the database directory.
    #[arg(long, short, env = "RULODB_DATA_DIR", default_value = "./rulodb_data")]
    pub data_dir: String,
    /// Size of the write buffer.
    #[arg(long, env = "RULODB_WRITE_BUFFER_SIZE", default_value_t = 134217728)]
    pub write_buffer_size: usize,
    /// Maximum number of write buffers.
    #[arg(long, env = "RULODB_MAX_WRITE_BUFFERS", default_value_t = 4)]
    pub max_write_buffers: i32,
    /// Minimum number of write buffers to merge.
    #[arg(long, env = "RULODB_MIN_WRITE_BUFFERS_TO_MERGE", default_value_t = 2)]
    pub min_write_buffers_to_merge: i32,
    /// Maximum background jobs.
    #[arg(long, env = "RULODB_MAX_BACKGROUND_JOBS", default_value_t = num_cpus::get() as i32 * 2)]
    pub max_background_jobs: i32,
    /// The number of files to trigger level-0 compaction. A value < 0 means that level-0 compaction
    /// will not be triggered by the number of files at all.
    #[arg(long, env = "RULODB_LEVEL0_FILE_NUM_COMPACTION", default_value_t = 4)]
    pub level0_file_num_compaction: i32,
    /// The soft limit on the number of level-0 files. A value < 0 means that no writing slowdown
    /// will be triggered by the number of files in level-0.
    #[arg(long, env = "RULODB_LEVEL0_SLOWDOWN_WRITES", default_value_t = 20)]
    pub level0_slowdown_writes: i32,
    /// Sets the maximum number of level-0 files. We stop writes at this point.
    #[arg(long, env = "RULODB_LEVEL0_STOP_WRITES", default_value_t = 24)]
    pub level0_stop_writes: i32,
    /// Number of background threads for flush and compaction.
    #[arg(long, env = "RULODB_PARALLELISM", default_value_t = num_cpus::get() as i32)]
    pub parallelism: i32,
    /// Size of the block cache.
    #[arg(long, env = "RULODB_BLOCK_CACHE_SIZE", default_value_t = 268435456)]
    pub block_cache_size: usize,
    /// Maximum number of open files.
    #[arg(long, env = "RULODB_MAX_OPEN_FILES", default_value_t = 1000)]
    pub max_open_files: i32,
    /// Use direct I/O for flush and compaction operations.
    #[arg(long, env = "RULODB_USE_DIRECT_IO", default_value_t = true)]
    pub use_direct_io_for_flush_and_compaction: bool,
    /// Use direct reads for better I/O performance.
    #[arg(long, env = "RULODB_USE_DIRECT_READS", default_value_t = false)]
    pub use_direct_reads: bool,
    /// Number of bytes to sync at a time.
    #[arg(long, env = "RULODB_BYTES_PER_SYNC", default_value_t = 1048576)]
    pub bytes_per_sync: u64,
    /// Number of WAL bytes to sync at a time.
    #[arg(long, env = "RULODB_WAL_BYTES_PER_SYNC", default_value_t = 1048576)]
    pub wal_bytes_per_sync: u64,
    /// Target file size base in bytes.
    #[arg(long, env = "RULODB_TARGET_FILE_SIZE_BASE", default_value_t = 67108864)]
    pub target_file_size_base: u64,
    /// Maximum bytes for level base.
    #[arg(
        long,
        env = "RULODB_MAX_BYTES_FOR_LEVEL_BASE",
        default_value_t = 268435456
    )]
    pub max_bytes_for_level_base: u64,
}

#[derive(Debug, Clone, Args)]
pub struct StartCommand {
    #[command(flatten)]
    pub server_config: ServerConfig,

    #[command(flatten)]
    pub engine_config: EngineConfig,
}
