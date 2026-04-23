use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config::{DEFAULT_INTERVAL_SECS, MIN_INTERVAL_SECS, RunConfig};
use crate::fetcher::{DEFAULT_DATA_URL, DEFAULT_STATUS_URL};

#[derive(Debug, Parser)]
#[command(name = "vatsim-recorder", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Poll the VATSIM feed and persist snapshots until interrupted.
    Run(RunArgs),
    /// Print a summary of a recorded data directory.
    Stats(StatsArgs),
}

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(long)]
    pub data_dir: PathBuf,
    #[arg(long, default_value_t = DEFAULT_INTERVAL_SECS)]
    pub interval_secs: u64,
    #[arg(long, default_value = DEFAULT_STATUS_URL)]
    pub status_url: String,
    #[arg(long, default_value = DEFAULT_DATA_URL)]
    pub data_url: String,
}

#[derive(Debug, clap::Args)]
pub struct StatsArgs {
    #[arg(long)]
    pub data_dir: PathBuf,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => {
            let cfg = build_run_config(args)?;
            crate::scheduler::run(cfg).await
        }
        Command::Stats(args) => crate::stats::run(args.data_dir).await,
    }
}

pub fn build_run_config(args: RunArgs) -> Result<RunConfig> {
    if args.interval_secs < MIN_INTERVAL_SECS {
        bail!(
            "--interval-secs must be at least {} (VATSIM feed cache TTL); got {}",
            MIN_INTERVAL_SECS,
            args.interval_secs
        );
    }
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("create data dir {}", args.data_dir.display()))?;
    Ok(RunConfig {
        data_dir: args.data_dir,
        interval_secs: args.interval_secs,
        status_url: args.status_url,
        data_url: args.data_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rejects_interval_below_minimum() {
        let dir = tempdir().unwrap();
        let err = build_run_config(RunArgs {
            data_dir: dir.path().to_path_buf(),
            interval_secs: 10,
            status_url: DEFAULT_STATUS_URL.into(),
            data_url: DEFAULT_DATA_URL.into(),
        })
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("at least 15"), "unexpected message: {msg}");
    }

    #[test]
    fn accepts_interval_at_minimum() {
        let dir = tempdir().unwrap();
        let cfg = build_run_config(RunArgs {
            data_dir: dir.path().to_path_buf(),
            interval_secs: 15,
            status_url: DEFAULT_STATUS_URL.into(),
            data_url: DEFAULT_DATA_URL.into(),
        })
        .unwrap();
        assert_eq!(cfg.interval_secs, 15);
    }

    #[test]
    fn accepts_default_interval() {
        let dir = tempdir().unwrap();
        let cfg = build_run_config(RunArgs {
            data_dir: dir.path().to_path_buf(),
            interval_secs: DEFAULT_INTERVAL_SECS,
            status_url: DEFAULT_STATUS_URL.into(),
            data_url: DEFAULT_DATA_URL.into(),
        })
        .unwrap();
        assert_eq!(cfg.interval_secs, 60);
    }
}
