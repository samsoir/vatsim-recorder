use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub data_dir: PathBuf,
    pub interval_secs: u64,
    pub status_url: String,
    pub data_url: String,
}

/// Default poll cadence if the user does not specify `--interval-secs`.
pub const DEFAULT_INTERVAL_SECS: u64 = 60;

/// Hard floor on the poll cadence. Matches the VATSIM v3 feed's advertised
/// cache TTL (~15 s); polling faster would only return duplicates (which the
/// writer already dedupes on `api_update`) and begin to look like abuse.
pub const MIN_INTERVAL_SECS: u64 = 15;
