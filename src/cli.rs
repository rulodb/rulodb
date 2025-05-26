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
    /// Size of the write buffer in megabytes.
    #[arg(long, env = "RULODB_WRITE_BUFFER_SIZE", default_value_t = 128)]
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
    #[arg(long, env = "RULODB_MAX_BACKGROUND_JOBS", default_value_t = num_cpus::get() as i32)]
    pub parallelism: i32,
}

#[derive(Debug, Clone, Args)]
pub struct StartCommand {
    #[command(flatten)]
    pub server_config: ServerConfig,

    #[command(flatten)]
    pub engine_config: EngineConfig,
}
