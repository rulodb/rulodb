#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![warn(clippy::nursery)]
#![allow(clippy::multiple_crate_versions)]
mod cli;
mod server;

use crate::cli::{Cli, Commands};
use clap::Parser;
use rulodb::{DefaultStorage, StorageBackend};
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(porcelain())
}

async fn porcelain() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(cmd) => {
            let engine = &cmd.engine_config;
            let engine_config = rulodb::storage::Config {
                data_dir: engine.data_dir.clone(),
                max_background_jobs: engine.max_background_jobs,
                parallelism: engine.parallelism,
                level_zero_file_num_compaction_trigger: engine.level0_file_num_compaction,
                level_zero_slowdown_writes_trigger: engine.level0_slowdown_writes,
                level_zero_stop_writes_trigger: engine.level0_stop_writes,
                block_cache_size: engine.block_cache_size,
                max_open_files: engine.max_open_files,
                use_direct_io_for_flush_and_compaction: engine
                    .use_direct_io_for_flush_and_compaction,
                use_direct_reads: engine.use_direct_reads,
                bytes_per_sync: engine.bytes_per_sync,
                wal_bytes_per_sync: engine.wal_bytes_per_sync,
                target_file_size_base: engine.target_file_size_base,
                max_bytes_for_level_base: engine.max_bytes_for_level_base,
                write_buffer_size: engine.write_buffer_size,
                max_write_buffer_number: engine.max_write_buffers,
                min_write_buffer_number_to_merge: engine.min_write_buffers_to_merge,
            };

            let db = DefaultStorage::open(&engine_config)?;
            let storage: Arc<dyn StorageBackend + Send + Sync> = Arc::new(db);

            server::start_server(storage, &cmd.server_config.address).await?;
        }
    }

    Ok(())
}
