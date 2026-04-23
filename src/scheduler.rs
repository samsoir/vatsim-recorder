use anyhow::{Context, Result};
use chrono::{DateTime, Timelike, Utc};
use std::time::Duration;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::config::RunConfig;
use crate::db;
use crate::fetcher::Fetcher;
use crate::raw_store::{self, Suffix};
use crate::writer::{self, WriteOutcome};

pub async fn run(cfg: RunConfig) -> Result<()> {
    let db_path = cfg.data_dir.join("recorder.db");
    let mut conn = db::open(&db_path).with_context(|| format!("open db {}", db_path.display()))?;
    let mut fetcher = Fetcher::new(&cfg.status_url, &cfg.data_url).await?;

    // Align to the next wall-clock boundary of the configured interval.
    let now = Utc::now();
    let sleep_secs = secs_until_next_boundary(now, cfg.interval_secs);
    if sleep_secs > 0 {
        info!(sleep_secs, "aligning scheduler to wall-clock boundary");
        time::sleep(Duration::from_secs(sleep_secs)).await;
    }

    let mut interval = time::interval(Duration::from_secs(cfg.interval_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

    info!(interval_secs = cfg.interval_secs, "scheduler started");
    loop {
        select! {
            _ = interval.tick() => {
                if let Err(e) = one_tick(&mut conn, &cfg.data_dir, &mut fetcher).await {
                    error!(error = %e, "tick failed");
                }
            }
            _ = sigterm.recv() => { info!("SIGTERM received; shutting down"); break; }
            _ = sigint.recv()  => { info!("SIGINT received; shutting down");  break; }
        }
    }
    Ok(())
}

async fn one_tick(
    conn: &mut rusqlite::Connection,
    data_dir: &std::path::Path,
    fetcher: &mut Fetcher,
) -> Result<()> {
    let fetch = match fetcher.fetch_once().await {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "fetch failed; skipping tick");
            return Ok(());
        }
    };

    let outcome = match writer::write(
        conn,
        data_dir,
        fetch.fetch_ts,
        fetch.duration_ms,
        &fetch.raw,
        &fetch.feed,
    ) {
        Ok(o) => o,
        Err(e) => {
            // If the raw body parsed but the DB write failed, preserve the raw
            // with a parse-error suffix so it can still be reprocessed.
            let _ = raw_store::write_snapshot(data_dir, fetch.fetch_ts, &fetch.raw, Suffix::ParseError);
            error!(error = %e, "writer failed");
            return Ok(());
        }
    };

    match outcome {
        WriteOutcome::Written { pilots, controllers } => info!(
            pilots,
            controllers,
            fetch_ms = fetch.duration_ms,
            api_update = %fetch.feed.general.update_timestamp,
            "fetch_ok",
        ),
        WriteOutcome::DuplicateApiUpdate => {
            info!(api_update = %fetch.feed.general.update_timestamp, "fetch_dedupe");
        }
    }
    Ok(())
}

/// Seconds from `now` until the next UTC boundary evenly divisible by `interval_secs`.
/// Returns 0 when `interval_secs` is 0 (guard; scheduler never passes 0).
pub fn secs_until_next_boundary(now: DateTime<Utc>, interval_secs: u64) -> u64 {
    if interval_secs == 0 { return 0; }
    let seconds_since_epoch = now.timestamp() as u64;
    let nanos_remainder = now.nanosecond() as u64;
    let mod_now = seconds_since_epoch % interval_secs;
    if mod_now == 0 && nanos_remainder == 0 {
        0
    } else {
        interval_secs - mod_now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn alignment_at_boundary_is_zero() {
        let t = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        assert_eq!(secs_until_next_boundary(t, 60), 0);
    }

    #[test]
    fn alignment_mid_minute() {
        let t = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 17).unwrap();
        assert_eq!(secs_until_next_boundary(t, 60), 43);
    }

    #[test]
    fn alignment_for_longer_interval() {
        let t = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 30).unwrap();
        // 5-minute boundary: next is 12:05:00, 4 min 30 sec away.
        assert_eq!(secs_until_next_boundary(t, 300), 270);
    }
}
