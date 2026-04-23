use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub data_dir: PathBuf,
    pub interval_secs: u64,
    pub status_url: String,
    pub data_url: String,
}

pub const MIN_INTERVAL_SECS: u64 = 60;
