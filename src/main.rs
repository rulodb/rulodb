#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![warn(clippy::nursery)]
#![allow(clippy::multiple_crate_versions)]
mod ast;
mod cli;
mod evaluator;
mod parser;
mod planner;
mod server;
mod storage;

use crate::cli::{Cli, Commands};
use crate::storage::{Config, DefaultStorage};
use clap::Parser;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(cmd) => {
            let engine = &cmd.engine_config;
            let engine_config = Config {
                data_dir: engine.data_dir.clone(),
                write_buffer_size_mb: engine.write_buffer_size,
                max_write_buffer_number: engine.max_write_buffers,
                min_write_buffer_number_to_merge: engine.min_write_buffers_to_merge,
                max_background_jobs: engine.max_background_jobs,
                parallelism: engine.parallelism,
                level_zero_file_num_compaction_trigger: engine.level0_file_num_compaction,
                level_zero_slowdown_writes_trigger: engine.level0_slowdown_writes,
                level_zero_stop_writes_trigger: engine.level0_stop_writes,
            };

            let db = DefaultStorage::open(&engine_config)?;

            server::start_server(
                Arc::new(Mutex::new(Box::new(db))),
                &cmd.server_config.address,
            )
            .await?;
        }
    }

    Ok(())
}
