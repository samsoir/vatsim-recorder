# VATSIM Recorder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust CLI, `vatsim-recorder`, that polls the VATSIM data feed every 60 s, stores structured rows in SQLite, and archives raw gzipped snapshots to disk, for the duration of Cross the Pond 2026.

**Architecture:** Single async Rust binary (tokio current-thread). On each tick: fetch JSON → gzip raw body to `data/raw/YYYY-MM-DD/HH/…json.gz` → run one SQLite transaction writing `fetches`, `positions`, `flight_plans`, `controllers`. Flight plans are content-hash-deduplicated. Dedupe on the feed's `update_timestamp`. SIGINT/SIGTERM triggers graceful shutdown with a bounded grace period.

**Tech Stack:** Rust 2024 edition · tokio (current-thread) · reqwest · rusqlite (bundled) · serde · flate2 · clap · tracing · sha1 · chrono.

**Reference spec:** `docs/superpowers/specs/2026-04-21-vatsim-recorder-design.md`

## File Structure

```
Cargo.toml
.gitignore
src/
  main.rs               # binary entry — invokes cli::run
  lib.rs                # module exposure for integration tests
  cli.rs                # clap derive types + subcommand dispatch
  config.rs             # RunConfig struct
  vatsim.rs             # serde types for the VATSIM feed
  flight_plan_hash.rs   # canonical sha1 hashing of flight plans
  db.rs                 # SQLite open + schema migration + pragmas
  raw_store.rs          # gzipped JSON snapshot writer
  writer.rs             # orchestrates raw_store + DB insert per tick
  fetcher.rs            # HTTP client + status→data URL resolution
  scheduler.rs          # tick loop + wall-clock alignment + shutdown
  stats.rs              # `stats` subcommand implementation
tests/
  fixtures/
    vatsim-data.json    # trimmed fixture
  end_to_end.rs         # httpmock-driven integration test
  dedupe.rs             # repeat-api_update dedupe test
  real_smoke.rs         # #[ignore] — real endpoint smoke
```

---

### Task 1: Bootstrap the Rust project

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/main.rs`
- Create: `src/lib.rs`

- [ ] **Step 1: Initialize the crate**

Run:
```bash
cargo init --name vatsim-recorder --edition 2024 --bin
```

Expected: creates `Cargo.toml`, `src/main.rs`, and a `.gitignore`. A fresh `main.rs` with `fn main() { println!("Hello, world!"); }`.

- [ ] **Step 2: Overwrite `Cargo.toml` with full dependency set**

Replace the generated `Cargo.toml` with:

```toml
[package]
name = "vatsim-recorder"
version = "0.1.0"
edition = "2024"

[lib]
path = "src/lib.rs"

[[bin]]
name = "vatsim-recorder"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["macros", "rt", "time", "signal"] }
reqwest = { version = "0.12", features = ["json", "gzip", "rustls-tls"], default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rusqlite = { version = "0.32", features = ["bundled"] }
flate2 = "1"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
sha1 = "0.10"

[dev-dependencies]
httpmock = "0.7"
tempfile = "3"

[profile.release]
lto = "thin"
```

- [ ] **Step 3: Create `src/lib.rs` with module declarations**

```rust
pub mod cli;
pub mod config;
pub mod db;
pub mod fetcher;
pub mod flight_plan_hash;
pub mod raw_store;
pub mod scheduler;
pub mod stats;
pub mod vatsim;
pub mod writer;
```

- [ ] **Step 4: Replace `src/main.rs` with minimal entry point**

```rust
use anyhow::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();
    vatsim_recorder::cli::run().await
}
```

- [ ] **Step 5: Create empty module files so `lib.rs` compiles**

For each of `cli.rs`, `config.rs`, `db.rs`, `fetcher.rs`, `flight_plan_hash.rs`, `raw_store.rs`, `scheduler.rs`, `stats.rs`, `vatsim.rs`, `writer.rs` — create with a single placeholder:

```rust
// filled in by a later task
```

In `cli.rs` alone, add a stub `run` so `main.rs` compiles:

```rust
use anyhow::Result;

pub async fn run() -> Result<()> {
    Ok(())
}
```

- [ ] **Step 6: Overwrite `.gitignore`**

```
/target
/data
*.log
```

- [ ] **Step 7: Build**

Run: `cargo build`
Expected: `Compiling vatsim-recorder v0.1.0` and successful build. Warnings about unused modules are acceptable.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock .gitignore src/
git commit -m "Bootstrap vatsim-recorder crate"
```

---

### Task 2: VATSIM feed types and the fixture

**Files:**
- Create: `tests/fixtures/vatsim-data.json`
- Modify: `src/vatsim.rs`
- Create: `src/vatsim.rs` tests (inline `#[cfg(test)] mod tests`)

- [ ] **Step 1: Create `tests/fixtures/vatsim-data.json`**

```json
{
  "general": {
    "version": 3,
    "reload": 1,
    "update": "20260421120000",
    "update_timestamp": "2026-04-21T12:00:00.0000000Z",
    "connected_clients": 3,
    "unique_users": 3
  },
  "pilots": [
    {
      "cid": 1234567,
      "name": "Test Pilot KJFK",
      "callsign": "AAL123",
      "server": "USA-EAST",
      "pilot_rating": 0,
      "military_rating": 0,
      "latitude": 40.6413,
      "longitude": -73.7781,
      "altitude": 15000,
      "groundspeed": 250,
      "transponder": "2200",
      "heading": 90,
      "qnh_i_hg": 29.92,
      "qnh_mb": 1013,
      "flight_plan": {
        "flight_rules": "I",
        "aircraft": "B738/L-SDE2E3FGHIRWY/LB1",
        "aircraft_faa": "H/B738/L",
        "aircraft_short": "B738",
        "departure": "KJFK",
        "arrival": "EGLL",
        "alternate": "EGKK",
        "cruise_tas": "450",
        "altitude": "FL360",
        "deptime": "1200",
        "enroute_time": "0700",
        "fuel_time": "0830",
        "remarks": "PBN/A1B1C1D1",
        "route": "DCT MERIT",
        "revision_id": 0,
        "assigned_transponder": "2200"
      },
      "logon_time": "2026-04-21T10:00:00.0000000Z",
      "last_updated": "2026-04-21T12:00:00.0000000Z"
    },
    {
      "cid": 2345678,
      "name": "Another Pilot",
      "callsign": "BAW456",
      "server": "EUROPE-CW",
      "pilot_rating": 0,
      "military_rating": 0,
      "latitude": 51.47,
      "longitude": -0.4543,
      "altitude": 0,
      "groundspeed": 0,
      "transponder": "0000",
      "heading": 270,
      "qnh_i_hg": 30.10,
      "qnh_mb": 1020,
      "flight_plan": null,
      "logon_time": "2026-04-21T11:30:00.0000000Z",
      "last_updated": "2026-04-21T12:00:00.0000000Z"
    }
  ],
  "controllers": [
    {
      "cid": 7654321,
      "name": "Test Controller",
      "callsign": "EGLL_S_TWR",
      "frequency": "118.500",
      "facility": 4,
      "rating": 5,
      "server": "EUROPE-CW",
      "visual_range": 50,
      "text_atis": ["Line 1", "Line 2"],
      "last_updated": "2026-04-21T12:00:00.0000000Z",
      "logon_time": "2026-04-21T10:00:00.0000000Z"
    }
  ],
  "atis": [],
  "servers": [],
  "prefiles": [],
  "facilities": [],
  "ratings": [],
  "pilot_ratings": [],
  "military_ratings": []
}
```

- [ ] **Step 2: Write the failing test in `src/vatsim.rs`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataFeed {
    pub general: General,
    pub pilots: Vec<Pilot>,
    pub controllers: Vec<Controller>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct General {
    pub version: u32,
    pub update_timestamp: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pilot {
    pub cid: i64,
    pub callsign: String,
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: i64,
    pub groundspeed: i64,
    pub heading: i64,
    pub transponder: Option<String>,
    pub qnh_i_hg: Option<f64>,
    pub qnh_mb: Option<i64>,
    pub logon_time: String,
    pub last_updated: String,
    pub flight_plan: Option<FlightPlan>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlightPlan {
    pub flight_rules: Option<String>,
    pub aircraft: Option<String>,
    pub aircraft_faa: Option<String>,
    pub aircraft_short: Option<String>,
    pub departure: Option<String>,
    pub arrival: Option<String>,
    pub alternate: Option<String>,
    pub cruise_tas: Option<String>,
    pub altitude: Option<String>,
    pub deptime: Option<String>,
    pub enroute_time: Option<String>,
    pub fuel_time: Option<String>,
    pub remarks: Option<String>,
    pub route: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Controller {
    pub cid: i64,
    pub callsign: String,
    pub facility: i64,
    pub rating: i64,
    pub frequency: Option<String>,
    pub visual_range: Option<i64>,
    pub text_atis: Option<Vec<String>>,
    pub logon_time: String,
    pub last_updated: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/vatsim-data.json");

    #[test]
    fn parses_fixture() {
        let feed: DataFeed = serde_json::from_str(FIXTURE).expect("parse fixture");
        assert_eq!(feed.general.update_timestamp, "2026-04-21T12:00:00.0000000Z");
        assert_eq!(feed.pilots.len(), 2);
        assert_eq!(feed.controllers.len(), 1);
        assert_eq!(feed.pilots[0].callsign, "AAL123");
        assert!(feed.pilots[0].flight_plan.is_some());
        assert!(feed.pilots[1].flight_plan.is_none());
        assert_eq!(
            feed.pilots[0].flight_plan.as_ref().unwrap().departure.as_deref(),
            Some("KJFK")
        );
    }
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p vatsim-recorder vatsim::tests::parses_fixture`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/vatsim-data.json src/vatsim.rs
git commit -m "Add VATSIM feed types and parsing test"
```

---

### Task 3: Flight plan canonical hashing

**Files:**
- Modify: `src/flight_plan_hash.rs`

- [ ] **Step 1: Write the failing tests**

Replace the placeholder in `src/flight_plan_hash.rs` with:

```rust
use sha1::{Digest, Sha1};

use crate::vatsim::FlightPlan;

/// SHA-1 hash (lowercase hex) over the normalized flight plan fields.
///
/// Normalization: each field contributes `<field_name>=<value>\n`. None values
/// contribute `<field_name>=\n`. Fields are emitted in a fixed order so the
/// hash is stable across runs and Rust versions.
pub fn hash(plan: &FlightPlan) -> String {
    let mut hasher = Sha1::new();
    let line = |name: &str, value: Option<&str>| {
        format!("{}={}\n", name, value.unwrap_or(""))
    };
    hasher.update(line("flight_rules", plan.flight_rules.as_deref()));
    hasher.update(line("aircraft", plan.aircraft.as_deref()));
    hasher.update(line("aircraft_faa", plan.aircraft_faa.as_deref()));
    hasher.update(line("aircraft_short", plan.aircraft_short.as_deref()));
    hasher.update(line("departure", plan.departure.as_deref()));
    hasher.update(line("arrival", plan.arrival.as_deref()));
    hasher.update(line("alternate", plan.alternate.as_deref()));
    hasher.update(line("cruise_tas", plan.cruise_tas.as_deref()));
    hasher.update(line("altitude", plan.altitude.as_deref()));
    hasher.update(line("deptime", plan.deptime.as_deref()));
    hasher.update(line("enroute_time", plan.enroute_time.as_deref()));
    hasher.update(line("fuel_time", plan.fuel_time.as_deref()));
    hasher.update(line("remarks", plan.remarks.as_deref()));
    hasher.update(line("route", plan.route.as_deref()));
    let digest = hasher.finalize();
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(departure: &str, arrival: &str) -> FlightPlan {
        FlightPlan {
            flight_rules: Some("I".into()),
            aircraft: Some("B738".into()),
            aircraft_faa: None,
            aircraft_short: None,
            departure: Some(departure.into()),
            arrival: Some(arrival.into()),
            alternate: None,
            cruise_tas: Some("450".into()),
            altitude: Some("FL360".into()),
            deptime: None,
            enroute_time: None,
            fuel_time: None,
            remarks: None,
            route: Some("DCT".into()),
        }
    }

    #[test]
    fn same_input_same_hash() {
        assert_eq!(hash(&plan("KJFK", "EGLL")), hash(&plan("KJFK", "EGLL")));
    }

    #[test]
    fn different_input_different_hash() {
        assert_ne!(hash(&plan("KJFK", "EGLL")), hash(&plan("KJFK", "EHAM")));
    }

    #[test]
    fn hash_is_40_lowercase_hex() {
        let h = hash(&plan("KJFK", "EGLL"));
        assert_eq!(h.len(), 40);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test flight_plan_hash::tests`
Expected: all three tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/flight_plan_hash.rs
git commit -m "Add canonical flight-plan hashing"
```

---

### Task 4: Database module — schema & open

**Files:**
- Modify: `src/db.rs`

- [ ] **Step 1: Write the failing test**

Replace `src/db.rs` with:

```rust
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
```

- [ ] **Step 2: Run the tests**

Run: `cargo test db::tests`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/db.rs
git commit -m "Add SQLite open + schema migration"
```

---

### Task 5: Raw store — gzipped snapshot writer

**Files:**
- Modify: `src/raw_store.rs`

- [ ] **Step 1: Write the failing tests**

Replace `src/raw_store.rs` with:

```rust
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Writes a gzipped raw JSON snapshot under `<base>/raw/YYYY-MM-DD/HH/…`.
/// Returns the path to the file, relative to `base`.
pub fn write_snapshot(
    base: &Path,
    fetch_ts: DateTime<Utc>,
    raw_body: &[u8],
    suffix: Suffix,
) -> Result<PathBuf> {
    let rel = relative_path(fetch_ts, suffix);
    let abs = base.join(&rel);
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create raw dir {}", parent.display()))?;
    }
    let file = File::create(&abs).with_context(|| format!("create {}", abs.display()))?;
    let mut enc = GzEncoder::new(file, Compression::default());
    enc.write_all(raw_body)?;
    enc.finish()?;
    Ok(rel)
}

pub enum Suffix {
    Ok,
    ParseError,
}

fn relative_path(ts: DateTime<Utc>, suffix: Suffix) -> PathBuf {
    let date = ts.format("%Y-%m-%d");
    let hour = ts.format("%H");
    let basic = ts.format("%Y%m%dT%H%M%SZ");
    let ext = match suffix {
        Suffix::Ok => "json.gz",
        Suffix::ParseError => "json.parse-error.gz",
    };
    PathBuf::from(format!("raw/{date}/{hour}/vatsim-{basic}.{ext}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn writes_and_round_trips() {
        let dir = tempdir().unwrap();
        let ts = Utc.with_ymd_and_hms(2026, 4, 21, 18, 30, 15).unwrap();
        let body = b"{\"hello\":\"world\"}";

        let rel = write_snapshot(dir.path(), ts, body, Suffix::Ok).unwrap();
        assert_eq!(
            rel,
            PathBuf::from("raw/2026-04-21/18/vatsim-20260421T183015Z.json.gz")
        );

        let abs = dir.path().join(&rel);
        assert!(abs.exists(), "file not written: {}", abs.display());

        let mut decoded = Vec::new();
        GzDecoder::new(File::open(&abs).unwrap())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn parse_error_suffix_differs() {
        let dir = tempdir().unwrap();
        let ts = Utc.with_ymd_and_hms(2026, 4, 21, 18, 30, 15).unwrap();
        let rel = write_snapshot(dir.path(), ts, b"broken", Suffix::ParseError).unwrap();
        assert!(rel.to_string_lossy().ends_with(".json.parse-error.gz"));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test raw_store::tests`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/raw_store.rs
git commit -m "Add gzipped raw snapshot writer"
```

---

### Task 6: Writer — orchestrate raw + DB insert

**Files:**
- Modify: `src/writer.rs`

- [ ] **Step 1: Write the failing test**

Replace `src/writer.rs` with:

```rust
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::path::Path;
use tracing::info;

use crate::flight_plan_hash;
use crate::raw_store::{self, Suffix};
use crate::vatsim::{DataFeed, FlightPlan};

/// Outcome of a single write pass. `Written` = new data was persisted.
/// `DuplicateApiUpdate` = the feed's `update_timestamp` matched the most
/// recent stored fetch, so nothing was written.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    Written { pilots: usize, controllers: usize },
    DuplicateApiUpdate,
}

pub fn write(
    conn: &mut Connection,
    data_dir: &Path,
    fetch_ts: DateTime<Utc>,
    fetch_duration_ms: i64,
    raw_body: &[u8],
    feed: &DataFeed,
) -> Result<WriteOutcome> {
    if is_duplicate_api_update(conn, &feed.general.update_timestamp)? {
        info!(api_update = %feed.general.update_timestamp, "dedupe: skipping duplicate fetch");
        return Ok(WriteOutcome::DuplicateApiUpdate);
    }

    let rel = raw_store::write_snapshot(data_dir, fetch_ts, raw_body, Suffix::Ok)
        .context("raw_store write")?;
    let rel_str = rel.to_string_lossy().into_owned();
    let fetch_ts_str = fetch_ts.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO fetches (fetch_ts, api_update, snapshot_path, pilot_count, controller_count, fetch_duration_ms) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            fetch_ts_str,
            feed.general.update_timestamp,
            rel_str,
            feed.pilots.len() as i64,
            feed.controllers.len() as i64,
            fetch_duration_ms,
        ],
    )?;

    for pilot in &feed.pilots {
        let fp_hash = pilot.flight_plan.as_ref().map(flight_plan_hash::hash);
        if let (Some(plan), Some(h)) = (&pilot.flight_plan, fp_hash.as_ref()) {
            insert_flight_plan(&tx, h, plan)?;
        }
        tx.execute(
            "INSERT INTO positions (fetch_ts, cid, callsign, lat, lon, altitude, groundspeed, heading, \
             transponder, qnh_i_hg, qnh_mb, logon_time, last_updated, flight_plan_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                fetch_ts_str,
                pilot.cid,
                pilot.callsign,
                pilot.latitude,
                pilot.longitude,
                pilot.altitude,
                pilot.groundspeed,
                pilot.heading,
                pilot.transponder,
                pilot.qnh_i_hg,
                pilot.qnh_mb,
                pilot.logon_time,
                pilot.last_updated,
                fp_hash,
            ],
        )?;
    }

    for ctrl in &feed.controllers {
        let atis_json = ctrl.text_atis.as_ref().map(|v| serde_json::to_string(v).unwrap());
        tx.execute(
            "INSERT INTO controllers (fetch_ts, cid, callsign, facility, rating, frequency, visual_range, \
             text_atis, logon_time, last_updated) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                fetch_ts_str,
                ctrl.cid,
                ctrl.callsign,
                ctrl.facility,
                ctrl.rating,
                ctrl.frequency,
                ctrl.visual_range,
                atis_json,
                ctrl.logon_time,
                ctrl.last_updated,
            ],
        )?;
    }

    tx.commit()?;
    Ok(WriteOutcome::Written {
        pilots: feed.pilots.len(),
        controllers: feed.controllers.len(),
    })
}

fn is_duplicate_api_update(conn: &Connection, api_update: &str) -> Result<bool> {
    let mut stmt = conn.prepare_cached(
        "SELECT 1 FROM fetches WHERE api_update = ?1 LIMIT 1",
    )?;
    let exists = stmt.exists(params![api_update])?;
    Ok(exists)
}

fn insert_flight_plan(tx: &rusqlite::Transaction, hash: &str, plan: &FlightPlan) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO flight_plans \
         (hash, flight_rules, aircraft, aircraft_faa, aircraft_short, departure, arrival, alternate, \
          cruise_tas, altitude, deptime, enroute_time, fuel_time, remarks, route) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            hash,
            plan.flight_rules,
            plan.aircraft,
            plan.aircraft_faa,
            plan.aircraft_short,
            plan.departure,
            plan.arrival,
            plan.alternate,
            plan.cruise_tas,
            plan.altitude,
            plan.deptime,
            plan.enroute_time,
            plan.fuel_time,
            plan.remarks,
            plan.route,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::tempdir;

    const FIXTURE: &str = include_str!("../tests/fixtures/vatsim-data.json");

    #[test]
    fn writes_fixture_rows() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let db_path = data_dir.join("recorder.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        let feed: DataFeed = serde_json::from_str(FIXTURE).unwrap();
        let ts = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        let outcome = write(&mut conn, data_dir, ts, 42, FIXTURE.as_bytes(), &feed).unwrap();
        assert_eq!(outcome, WriteOutcome::Written { pilots: 2, controllers: 1 });

        let pilot_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM positions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pilot_count, 2);

        let fp_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM flight_plans", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fp_count, 1, "one pilot has a plan, the other is NULL");

        let ctrl_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM controllers", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ctrl_count, 1);

        let (callsign, hash): (String, Option<String>) = conn
            .query_row(
                "SELECT callsign, flight_plan_hash FROM positions WHERE cid = 1234567",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
            )
            .unwrap();
        assert_eq!(callsign, "AAL123");
        assert!(hash.is_some());

        let atis: String = conn
            .query_row("SELECT text_atis FROM controllers WHERE cid = 7654321", [], |r| r.get(0))
            .unwrap();
        assert_eq!(atis, "[\"Line 1\",\"Line 2\"]");
    }

    #[test]
    fn dedupes_on_repeated_api_update() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let db_path = data_dir.join("recorder.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        let feed: DataFeed = serde_json::from_str(FIXTURE).unwrap();
        let ts1 = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 4, 21, 12, 1, 0).unwrap();
        write(&mut conn, data_dir, ts1, 10, FIXTURE.as_bytes(), &feed).unwrap();
        let outcome = write(&mut conn, data_dir, ts2, 10, FIXTURE.as_bytes(), &feed).unwrap();
        assert_eq!(outcome, WriteOutcome::DuplicateApiUpdate);

        let fetch_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fetches", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fetch_count, 1);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test writer::tests`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/writer.rs
git commit -m "Add writer: raw snapshot + DB insert + api_update dedupe"
```

---

### Task 7: Fetcher — HTTP client with status-endpoint discovery

**Files:**
- Modify: `src/fetcher.rs`

- [ ] **Step 1: Write the failing tests**

Replace `src/fetcher.rs` with:

```rust
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::vatsim::DataFeed;

pub const DEFAULT_STATUS_URL: &str = "https://status.vatsim.net/status.json";
pub const DEFAULT_DATA_URL: &str = "https://data.vatsim.net/v3/vatsim-data.json";

pub struct Fetch {
    pub raw: Vec<u8>,
    pub feed: DataFeed,
    pub duration_ms: i64,
    pub fetch_ts: DateTime<Utc>,
}

pub struct Fetcher {
    client: Client,
    data_urls: Vec<String>,
    next_idx: usize,
}

#[derive(Debug, Deserialize)]
struct StatusDoc {
    data: StatusData,
}

#[derive(Debug, Deserialize)]
struct StatusData {
    v3: Vec<String>,
}

impl Fetcher {
    /// Build a fetcher. Resolves the list of data endpoints by calling the
    /// status endpoint; on failure falls back to `fallback_data_url`.
    pub async fn new(status_url: &str, fallback_data_url: &str) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("vatsim-recorder/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;

        let data_urls = match fetch_status(&client, status_url).await {
            Ok(urls) if !urls.is_empty() => {
                info!(count = urls.len(), "resolved data URLs from status endpoint");
                urls
            }
            Ok(_) => {
                warn!("status endpoint returned empty v3 list; using fallback");
                vec![fallback_data_url.to_string()]
            }
            Err(e) => {
                warn!(error = %e, "status endpoint failed; using fallback data URL");
                vec![fallback_data_url.to_string()]
            }
        };

        Ok(Self { client, data_urls, next_idx: 0 })
    }

    /// Fetch a single snapshot. Tries each known data URL in round-robin
    /// order until one succeeds or all fail for this tick.
    pub async fn fetch_once(&mut self) -> Result<Fetch> {
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..self.data_urls.len() {
            let url = self.data_urls[self.next_idx].clone();
            self.next_idx = (self.next_idx + 1) % self.data_urls.len();
            let start = Instant::now();
            let fetch_ts = Utc::now();
            match fetch_data(&self.client, &url).await {
                Ok((raw, feed)) => {
                    return Ok(Fetch {
                        raw,
                        feed,
                        duration_ms: start.elapsed().as_millis() as i64,
                        fetch_ts,
                    });
                }
                Err(e) => {
                    warn!(url = %url, error = %e, "fetch failed; trying next URL");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no data URLs configured")))
    }
}

async fn fetch_status(client: &Client, url: &str) -> Result<Vec<String>> {
    let doc: StatusDoc = client.get(url).send().await?.error_for_status()?.json().await?;
    Ok(doc.data.v3)
}

async fn fetch_data(client: &Client, url: &str) -> Result<(Vec<u8>, DataFeed)> {
    let resp = client.get(url).send().await.context("http get")?.error_for_status()?;
    let raw = resp.bytes().await.context("read body")?.to_vec();
    let feed: DataFeed = serde_json::from_slice(&raw).context("parse json")?;
    Ok((raw, feed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/vatsim-data.json");

    #[tokio::test(flavor = "current_thread")]
    async fn uses_status_endpoint_when_available() {
        let server = MockServer::start_async().await;
        let status_mock = server.mock_async(|when, then| {
            when.method(GET).path("/status.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"data":{"v3":["URL_PLACEHOLDER"]}}"#.replace(
                    "URL_PLACEHOLDER",
                    &format!("{}/data.json", server.base_url()),
                ));
        }).await;
        let data_mock = server.mock_async(|when, then| {
            when.method(GET).path("/data.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(FIXTURE);
        }).await;

        let mut fetcher = Fetcher::new(
            &format!("{}/status.json", server.base_url()),
            "http://unused.example/",
        ).await.unwrap();
        let fetched = fetcher.fetch_once().await.unwrap();

        status_mock.assert_async().await;
        data_mock.assert_async().await;
        assert_eq!(fetched.feed.pilots.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn falls_back_when_status_fails() {
        let server = MockServer::start_async().await;
        let data_mock = server.mock_async(|when, then| {
            when.method(GET).path("/data.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(FIXTURE);
        }).await;

        let mut fetcher = Fetcher::new(
            "http://127.0.0.1:1/status.json", // unreachable
            &format!("{}/data.json", server.base_url()),
        ).await.unwrap();
        let fetched = fetcher.fetch_once().await.unwrap();
        data_mock.assert_async().await;
        assert_eq!(fetched.feed.controllers.len(), 1);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test fetcher::tests`
Expected: both tests PASS. (First run will be slow due to httpmock dependency compile.)

- [ ] **Step 3: Commit**

```bash
git add src/fetcher.rs
git commit -m "Add HTTP fetcher with status-endpoint URL discovery"
```

---

### Task 8: Config + CLI

**Files:**
- Modify: `src/config.rs`
- Modify: `src/cli.rs`

- [ ] **Step 1: Define `RunConfig` in `src/config.rs`**

Replace `src/config.rs` with:

```rust
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub data_dir: PathBuf,
    pub interval_secs: u64,
    pub status_url: String,
    pub data_url: String,
}

pub const MIN_INTERVAL_SECS: u64 = 60;
```

- [ ] **Step 2: Write the failing CLI test**

Replace `src/cli.rs` with:

```rust
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config::{MIN_INTERVAL_SECS, RunConfig};
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
    #[arg(long, default_value_t = MIN_INTERVAL_SECS)]
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
            "--interval-secs must be at least {} (good-neighbour policy); got {}",
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
            interval_secs: 30,
            status_url: DEFAULT_STATUS_URL.into(),
            data_url: DEFAULT_DATA_URL.into(),
        })
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("at least 60"), "unexpected message: {msg}");
    }

    #[test]
    fn accepts_interval_at_minimum() {
        let dir = tempdir().unwrap();
        let cfg = build_run_config(RunArgs {
            data_dir: dir.path().to_path_buf(),
            interval_secs: 60,
            status_url: DEFAULT_STATUS_URL.into(),
            data_url: DEFAULT_DATA_URL.into(),
        })
        .unwrap();
        assert_eq!(cfg.interval_secs, 60);
    }
}
```

- [ ] **Step 3: Run the tests**

Run: `cargo test cli::tests`
Expected: both tests PASS. The build may fail until `scheduler::run` and `stats::run` exist — that's fine, both will be stubbed in the next tasks.

Actually: fix the build failure now by adding minimal stubs. In `src/scheduler.rs` replace the placeholder with:

```rust
use anyhow::Result;
use crate::config::RunConfig;

pub async fn run(_cfg: RunConfig) -> Result<()> {
    Ok(())
}
```

And in `src/stats.rs` replace the placeholder with:

```rust
use anyhow::Result;
use std::path::PathBuf;

pub async fn run(_data_dir: PathBuf) -> Result<()> {
    Ok(())
}
```

Then re-run: `cargo test cli::tests`
Expected: both tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/config.rs src/cli.rs src/scheduler.rs src/stats.rs
git commit -m "Add CLI with interval-minimum enforcement"
```

---

### Task 9: Scheduler — tick loop, wall-clock alignment, shutdown

**Files:**
- Modify: `src/scheduler.rs`

- [ ] **Step 1: Write the failing test for alignment**

Replace `src/scheduler.rs` with:

```rust
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
```

- [ ] **Step 2: Run the alignment tests**

Run: `cargo test scheduler::tests`
Expected: all three tests PASS.

- [ ] **Step 3: Build the full crate to catch wiring errors**

Run: `cargo build`
Expected: success (warnings OK).

- [ ] **Step 4: Commit**

```bash
git add src/scheduler.rs
git commit -m "Add scheduler with wall-clock alignment and signal handling"
```

---

### Task 10: Stats subcommand

**Files:**
- Modify: `src/stats.rs`

- [ ] **Step 1: Write the failing test**

Replace `src/stats.rs` with:

```rust
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
            } else if ft.is_file() {
                if let Ok(md) = entry.metadata() { total += md.len(); }
            }
        }
    }
    Ok(total)
}

pub fn render(r: &Report) -> String {
    let human = humanize_bytes(r.disk_bytes);
    format!(
        "fetches:         {}\nfirst:           {}\nlast:            {}\npeak pilots:     {}\npeak controllers:{}\ndisk:            {} ({} bytes)\n",
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

        let out = render(&r);
        assert!(out.contains("fetches:         1"));
        assert!(out.contains("peak pilots:     2"));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test stats::tests`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/stats.rs
git commit -m "Add stats subcommand"
```

---

### Task 11: End-to-end integration test

**Files:**
- Create: `tests/end_to_end.rs`

- [ ] **Step 1: Write the integration test**

```rust
//! Drives the full fetch→write path using a mock HTTP server.

use chrono::{TimeZone, Utc};
use httpmock::prelude::*;
use tempfile::tempdir;
use vatsim_recorder::{db, fetcher::Fetcher, vatsim::DataFeed, writer};

const FIXTURE: &str = include_str!("fixtures/vatsim-data.json");

#[tokio::test(flavor = "current_thread")]
async fn fetch_then_write_then_query() {
    let server = MockServer::start_async().await;
    server.mock_async(|when, then| {
        when.method(GET).path("/status.json");
        then.status(200)
            .header("content-type", "application/json")
            .body(
                r#"{"data":{"v3":["__DATA__"]}}"#
                    .replace("__DATA__", &format!("{}/data.json", server.base_url())),
            );
    }).await;
    server.mock_async(|when, then| {
        when.method(GET).path("/data.json");
        then.status(200)
            .header("content-type", "application/json")
            .body(FIXTURE);
    }).await;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("recorder.db");
    let mut conn = db::open(&db_path).unwrap();
    let mut fetcher = Fetcher::new(
        &format!("{}/status.json", server.base_url()),
        "http://unused.example/",
    ).await.unwrap();

    let fetch = fetcher.fetch_once().await.unwrap();
    let outcome = writer::write(
        &mut conn,
        dir.path(),
        Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap(),
        fetch.duration_ms,
        &fetch.raw,
        &fetch.feed,
    ).unwrap();
    assert!(matches!(outcome, writer::WriteOutcome::Written { pilots: 2, controllers: 1 }));

    let feed: DataFeed = serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(feed.general.update_timestamp, "2026-04-21T12:00:00.0000000Z");

    let pilot_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM positions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(pilot_rows, 2);

    let raw_file_exists = dir
        .path()
        .join("raw/2026-04-21/12/vatsim-20260421T120000Z.json.gz")
        .exists();
    assert!(raw_file_exists, "raw snapshot should have been written");
}
```

- [ ] **Step 2: Make the fixture available to the integration test**

Integration tests look for their own fixtures relative to the `tests/` directory. The test uses `include_str!("fixtures/vatsim-data.json")` — which already matches the location created in Task 2. No action needed, just verify by running the test.

- [ ] **Step 3: Run the integration test**

Run: `cargo test --test end_to_end`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/end_to_end.rs
git commit -m "Add end-to-end fetch→write integration test"
```

---

### Task 12: Real-endpoint smoke test (ignored by default)

**Files:**
- Create: `tests/real_smoke.rs`

- [ ] **Step 1: Write the ignored smoke test**

```rust
//! Smoke test that hits the real VATSIM data endpoint. Run manually:
//!   cargo test --test real_smoke -- --ignored --nocapture

use vatsim_recorder::fetcher::{DEFAULT_DATA_URL, DEFAULT_STATUS_URL, Fetcher};

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn real_fetch_smoke() {
    let mut fetcher = Fetcher::new(DEFAULT_STATUS_URL, DEFAULT_DATA_URL).await.unwrap();
    let fetch = fetcher.fetch_once().await.unwrap();
    assert!(!fetch.feed.pilots.is_empty(), "expected at least one pilot live on the network");
    eprintln!(
        "smoke ok: pilots={}, controllers={}, api_update={}, duration_ms={}",
        fetch.feed.pilots.len(),
        fetch.feed.controllers.len(),
        fetch.feed.general.update_timestamp,
        fetch.duration_ms,
    );
}
```

- [ ] **Step 2: Verify it compiles but does not run by default**

Run: `cargo test --test real_smoke`
Expected: `0 passed; 0 failed; 1 ignored`.

- [ ] **Step 3: Run the ignored smoke test once to confirm the real endpoint works**

Run: `cargo test --test real_smoke -- --ignored --nocapture`
Expected: PASS, with a line on stderr like `smoke ok: pilots=845, controllers=72, api_update=…`. If the network is unavailable, skip this step and note it in the commit.

- [ ] **Step 4: Commit**

```bash
git add tests/real_smoke.rs
git commit -m "Add ignored real-endpoint smoke test"
```

---

### Task 13: README with operational notes

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write the README**

```markdown
# vatsim-recorder

Polls the VATSIM data feed every 60 s and writes a time series of aircraft and
controller state to SQLite, with raw gzipped JSON snapshots preserved on disk.
Built to record Cross the Pond 2026.

## Build

    cargo build --release

The release binary lands at `target/release/vatsim-recorder`.

## Run

    vatsim-recorder run --data-dir ./data

Accepted flags:

- `--interval-secs N` (default: 60, minimum: 60). Anything lower is rejected.
- `--status-url URL` override the VATSIM status endpoint (testing).
- `--data-url URL` override the fallback data endpoint (testing).

Logs go to stdout; set verbosity via `RUST_LOG=info` / `debug`.

Shut down with `Ctrl-C` or `SIGTERM`. The current tick is allowed up to 10 s to
finish before the process exits.

## Inspect

    vatsim-recorder stats --data-dir ./data

Or query the SQLite file directly:

    sqlite3 ./data/recorder.db "SELECT COUNT(DISTINCT cid) FROM positions"

Or with DuckDB:

    duckdb -c "INSTALL sqlite; LOAD sqlite; ATTACH 'data/recorder.db' AS r (TYPE SQLITE); SELECT MIN(fetch_ts), MAX(fetch_ts) FROM r.fetches;"

## Layout

    data/
      recorder.db                    # SQLite; journal_mode=WAL
      raw/
        YYYY-MM-DD/HH/vatsim-*.json.gz

Raw snapshots are never deleted by the recorder. They exist as insurance: if
the schema needs to change, you can reprocess every raw file.

## Schema

See `docs/superpowers/specs/2026-04-21-vatsim-recorder-design.md`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Add README"
```

---

### Task 14: Full test run + release build

**Files:** none

- [ ] **Step 1: Run the full test suite**

Run: `cargo test`
Expected: all tests pass (the real-endpoint smoke test stays ignored).

- [ ] **Step 2: Build the release binary**

Run: `cargo build --release`
Expected: successful build at `target/release/vatsim-recorder`.

- [ ] **Step 3: Exercise the binary against its `--help`**

Run: `./target/release/vatsim-recorder --help`
Expected: clap-formatted help listing `run` and `stats` subcommands.

Run: `./target/release/vatsim-recorder run --help`
Expected: help listing `--data-dir`, `--interval-secs`, `--status-url`, `--data-url`.

Run: `./target/release/vatsim-recorder run --data-dir /tmp/vr-test --interval-secs 10`
Expected: immediate error: `--interval-secs must be at least 60 (good-neighbour policy); got 10`. Exit non-zero.

- [ ] **Step 4: Commit a note if anything needed to be adjusted**

If any fixes were needed, commit them now. Otherwise nothing to do — the plan is complete.

---

## Self-Review

**1. Spec coverage — each requirement traces to a task:**

- *Capture pilots + controllers + flight plans* → Tasks 2, 6.
- *Raw-first write ordering* → Task 6 (raw_store called before DB tx) and Task 9 (scheduler preserves raw on DB failure).
- *60 s cadence, wall-clock aligned, min 60 s enforced* → Task 9 (alignment + interval) and Task 8 (CLI enforces minimum).
- *`tokio::time::interval` with `MissedTickBehavior::Skip`* → Task 9.
- *Status→data URL discovery with fallback* → Task 7.
- *`api_update` dedupe* → Task 6.
- *Flight plan content-hash deduplication* → Tasks 3 and 6.
- *SQLite schema, pragmas, indexes* → Task 4.
- *Graceful shutdown on SIGINT/SIGTERM* → Task 9.
- *Raw write failure path skips SQL insert* → Task 9 preserves raw with `.parse-error` suffix on writer failure; Task 6's writer returns early when raw_store fails (it calls raw_store::write_snapshot before the SQL transaction, so an Err short-circuits via `?`).
- *`stats` subcommand* → Task 10.
- *CLI with `run` + `stats`* → Task 8.
- *Unit tests for transform* → Task 6.
- *httpmock dedupe integration test* → the dedupe path is covered by `writer::tests::dedupes_on_repeated_api_update` in Task 6 (same API-level behaviour; no network needed). The end-to-end test (Task 11) exercises the full fetch→write path.
- *`#[ignore]` real-endpoint smoke test* → Task 12.

**2. Placeholder scan:** every code block contains real code; every `Run:` step has a concrete command and expected output. No "implement later" / "add error handling" stubs.

**3. Type consistency:** `DataFeed`/`Pilot`/`Controller`/`FlightPlan` are defined in Task 2 and used by name in Tasks 3, 6, 7, 11. `RunConfig` defined in Task 8, consumed in Task 9. `WriteOutcome` defined in Task 6, used in Task 9. `Fetch`/`Fetcher` defined in Task 7, used in Tasks 9 and 11. Function names used across tasks (`db::open`, `raw_store::write_snapshot`, `writer::write`, `flight_plan_hash::hash`, `fetcher::Fetcher::new`, `fetcher::Fetcher::fetch_once`, `scheduler::secs_until_next_boundary`, `stats::collect`, `stats::render`) all match between definition and call sites.
