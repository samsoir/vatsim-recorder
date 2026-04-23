use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS fetches (
    fetch_ts          TEXT PRIMARY KEY,
    api_update        TEXT NOT NULL,
    snapshot_path     TEXT NOT NULL,
    pilot_count       INTEGER NOT NULL,
    controller_count  INTEGER NOT NULL,
    fetch_duration_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    fetch_ts         TEXT NOT NULL,
    cid              INTEGER NOT NULL,
    callsign         TEXT NOT NULL,
    lat              REAL NOT NULL,
    lon              REAL NOT NULL,
    altitude         INTEGER NOT NULL,
    groundspeed      INTEGER NOT NULL,
    heading          INTEGER NOT NULL,
    transponder      TEXT,
    qnh_i_hg         REAL,
    qnh_mb           INTEGER,
    logon_time       TEXT NOT NULL,
    last_updated     TEXT NOT NULL,
    flight_plan_hash TEXT,
    PRIMARY KEY (fetch_ts, cid)
);
CREATE INDEX IF NOT EXISTS idx_positions_cid_ts ON positions(cid, fetch_ts);

CREATE TABLE IF NOT EXISTS flight_plans (
    hash           TEXT PRIMARY KEY,
    flight_rules   TEXT,
    aircraft       TEXT,
    aircraft_faa   TEXT,
    aircraft_short TEXT,
    departure      TEXT,
    arrival        TEXT,
    alternate      TEXT,
    cruise_tas     TEXT,
    altitude       TEXT,
    deptime        TEXT,
    enroute_time   TEXT,
    fuel_time      TEXT,
    remarks        TEXT,
    route          TEXT
);

CREATE TABLE IF NOT EXISTS controllers (
    fetch_ts     TEXT NOT NULL,
    cid          INTEGER NOT NULL,
    callsign     TEXT NOT NULL,
    facility     INTEGER NOT NULL,
    rating       INTEGER NOT NULL,
    frequency    TEXT,
    visual_range INTEGER,
    text_atis    TEXT,
    logon_time   TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    PRIMARY KEY (fetch_ts, cid)
);
"#;

/// Open (or create) the recorder database at `path`, apply pragmas, and
/// ensure the schema exists. Returns a connection ready for writes.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn creates_all_tables() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("recorder.db");
        let conn = open(&path).unwrap();

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            tables,
            vec!["controllers", "fetches", "flight_plans", "positions"]
        );

        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("recorder.db");
        let _ = open(&path).unwrap();
        let _ = open(&path).unwrap();
    }
}
