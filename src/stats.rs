use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

pub async fn run(data_dir: PathBuf) -> Result<()> {
    let db_path = data_dir.join("recorder.db");
    let conn = Connection::open(&db_path)
        .with_context(|| format!("open db {}", db_path.display()))?;
    let report = collect(&conn, &data_dir)?;
    print!("{}", render(&report));
    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct Report {
    pub fetches: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
    pub peak_pilots: i64,
    pub peak_controllers: i64,
    pub disk_bytes: u64,
}

pub fn collect(conn: &Connection, data_dir: &Path) -> Result<Report> {
    let fetches: i64 = conn.query_row("SELECT COUNT(*) FROM fetches", [], |r| r.get(0))?;
    let first_ts: Option<String> = conn
        .query_row("SELECT MIN(fetch_ts) FROM fetches", [], |r| r.get(0))
        .ok()
        .flatten();
    let last_ts: Option<String> = conn
        .query_row("SELECT MAX(fetch_ts) FROM fetches", [], |r| r.get(0))
        .ok()
        .flatten();
    let peak_pilots: i64 = conn
        .query_row("SELECT COALESCE(MAX(pilot_count), 0) FROM fetches", [], |r| r.get(0))?;
    let peak_controllers: i64 = conn
        .query_row("SELECT COALESCE(MAX(controller_count), 0) FROM fetches", [], |r| r.get(0))?;
    let disk_bytes = dir_size(data_dir)?;
    Ok(Report { fetches, first_ts, last_ts, peak_pilots, peak_controllers, disk_bytes })
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total: u64 = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let ft = match entry.file_type() { Ok(t) => t, Err(_) => continue };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() && let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    Ok(total)
}

pub fn render(r: &Report) -> String {
    let human = humanize_bytes(r.disk_bytes);
    format!(
        "fetches:         {}\nfirst:           {}\nlast:            {}\npeak pilots:     {}\npeak controllers: {}\ndisk:            {} ({} bytes)\n",
        r.fetches,
        r.first_ts.as_deref().unwrap_or("-"),
        r.last_ts.as_deref().unwrap_or("-"),
        r.peak_pilots,
        r.peak_controllers,
        human,
        r.disk_bytes,
    )
}

fn humanize_bytes(n: u64) -> String {
    const UNITS: &[(&str, u64)] = &[("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)];
    for (label, threshold) in UNITS {
        if n >= *threshold {
            return format!("{:.2} {}", n as f64 / *threshold as f64, label);
        }
    }
    format!("{} B", n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    const FIXTURE: &str = include_str!("../tests/fixtures/vatsim-data.json");

    #[test]
    fn empty_db_report() {
        let dir = tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("recorder.db")).unwrap();
        let r = collect(&conn, dir.path()).unwrap();
        assert_eq!(r.fetches, 0);
        assert_eq!(r.peak_pilots, 0);
        assert!(r.first_ts.is_none());
    }

    #[test]
    fn reports_after_one_write() {
        let dir = tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("recorder.db")).unwrap();
        let feed: crate::vatsim::DataFeed = serde_json::from_str(FIXTURE).unwrap();
        let ts = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        crate::writer::write(&mut conn, dir.path(), ts, 15, FIXTURE.as_bytes(), &feed).unwrap();

        let r = collect(&conn, dir.path()).unwrap();
        assert_eq!(r.fetches, 1);
        assert_eq!(r.peak_pilots, 2);
        assert_eq!(r.peak_controllers, 1);
        assert_eq!(r.first_ts.as_deref(), Some("2026-04-21T12:00:00Z"));
        assert_eq!(r.last_ts.as_deref(), Some("2026-04-21T12:00:00Z"));

        let out = render(&r);
        assert!(out.contains("fetches:         1"));
        assert!(out.contains("peak pilots:     2"));
    }
}
