# VATSIM Recorder — Design

**Date:** 2026-04-21
**Status:** Approved for planning
**Author:** Sam de Freyssinet (with Claude)

## Purpose

Capture a complete time series of VATSIM network activity — aircraft positions, telemetry, flight plans, and controllers — during the Cross the Pond 2026 event, in a form that supports both ad-hoc analytical queries and later replay into a 3D visualization.

## Goals

- Capture everything the feed publishes for pilots and controllers, with enough fidelity to drive a smooth playback.
- Store data in a format that is directly queryable (SQL) and compact enough to archive.
- Preserve raw API responses alongside the structured store so the data can be reprocessed if the schema turns out wrong.
- Run unattended for the duration of the event on a home server, surviving network blips and API hiccups.
- Be a good neighbour on the VATSIM data endpoint — poll no faster than once per minute.

## Non-goals

- No built-in visualization or playback UI. Query access is via `sqlite3` / DuckDB / pandas; the 3D viewer is a future project.
- No long-term retention or rotation strategy. This is a single-event tool; a few GB of raw data is acceptable.
- No multi-host or distributed operation.
- No automatic recovery beyond "the next tick will try again". The recorder doesn't attempt to backfill missed samples.

## Architecture

A single Rust binary, `vatsim-recorder`, built on tokio. Three internal components:

- **Fetcher** — async HTTP client (`reqwest`). On startup, fetches `https://status.vatsim.net/status.json` to discover the list of data-feed mirrors, picks one, and falls back to the others on failure. A hardcoded default (`https://data.vatsim.net/v3/vatsim-data.json`) is used if the status endpoint itself is unreachable. No in-tick retries — a failed fetch is logged and the next tick tries again.
- **Writer** — given a successful fetch (raw bytes + parsed snapshot), performs two side effects in order: (1) writes the gzipped raw body to disk at a timestamped path; (2) runs one SQLite transaction inserting the structured rows. Raw-first so that if the SQL write fails, the data is still captured and can be reprocessed. If the raw-write itself fails (e.g. disk full, permission error), the SQL insert is skipped — we never store a DB row pointing at a `snapshot_path` that does not exist.
- **Scheduler** — `tokio::time::interval` at the configured cadence. On startup, the scheduler sleeps until the next wall-clock minute boundary (`UTC seconds-within-minute == 0`), then begins ticking. This is purely for neat timestamps; it is not load-bearing. `MissedTickBehavior::Skip` — if a fetch+write takes longer than the interval, the next tick is skipped rather than queued.

Graceful shutdown on `SIGINT` / `SIGTERM`: if a tick is in flight, wait for it to finish (or fail) up to a bounded grace period (10 s); otherwise exit immediately. Then close the DB and exit 0. An in-flight HTTP fetch is *not* cancelled — reqwest will complete or time out normally.

## Data flow

```
tick → Fetcher.fetch() → (raw_bytes, parsed_snapshot)
                         │
                         ├── write raw_bytes.gz to data/raw/YYYY-MM-DD/HH/…
                         └── transform + SQL transaction against recorder.db
```

Each tick is independent. No in-memory buffering between ticks; if the process is killed mid-tick, at worst the current sample is lost (not already-persisted samples).

## CLI

```
vatsim-recorder run   --data-dir <DIR> [--interval-secs N] [--status-url URL] [--data-url URL]
vatsim-recorder stats --data-dir <DIR>
```

- `--interval-secs` defaults to **60**, with a hard minimum of **60** (good-neighbour policy). The CLI rejects lower values with a clear error.
- `--status-url` / `--data-url` override the VATSIM endpoints, for testing.
- `stats` prints fetch count, time range, peak concurrent pilots, and disk usage. It is a convenience; all real queries happen directly against the SQLite file.

No `tail` / live-watch mode in v1. Logs during `run` cover that need.

## On-disk layout

```
data/
  recorder.db         # SQLite (WAL mode: also recorder.db-wal, recorder.db-shm)
  raw/
    2026-04-25/
      18/
        vatsim-20260425T183015Z.json.gz
        vatsim-20260425T184015Z.json.gz
        ...
```

Raw filenames use `vatsim-<UTC-basic-ISO-8601>Z.json.gz`. The directory layout (`YYYY-MM-DD/HH/`) keeps any single directory small enough for shell globbing to stay responsive.

## Schema

Timestamps are ISO-8601 UTC strings (e.g. `2026-04-25T18:30:15Z`). `cid` is the VATSIM numeric user ID.

```sql
CREATE TABLE fetches (
  fetch_ts          TEXT PRIMARY KEY,
  api_update        TEXT NOT NULL,
  snapshot_path     TEXT NOT NULL,
  pilot_count       INTEGER NOT NULL,
  controller_count  INTEGER NOT NULL,
  fetch_duration_ms INTEGER NOT NULL
);

CREATE TABLE positions (
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
CREATE INDEX idx_positions_cid_ts ON positions(cid, fetch_ts);

CREATE TABLE flight_plans (
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

CREATE TABLE controllers (
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
```

### Design notes

**Flight plans are deduplicated by content hash.** A pilot's flight plan rarely changes during a flight, so storing the full plan on every position row would waste substantial space. Instead we compute a SHA-1 over the normalized plan fields, `INSERT OR IGNORE` into `flight_plans`, and reference the hash from each position row. Result: one row per *version* of each pilot's plan, not per sample. Positions without a plan filed have `flight_plan_hash = NULL`.

**Primary keys chosen for scan patterns.** `(fetch_ts, cid)` puts time first so range scans (playback, "all aircraft between T1 and T2") hit the primary key directly. The secondary index `(cid, fetch_ts)` makes per-aircraft track queries fast.

**Dedupe on `api_update`.** If two consecutive polls return the same `update_timestamp` from the feed (the feed hasn't republished yet), the second is dropped — no SQL insert, no raw file. At 60 s cadence this will rarely trigger, but it is a cheap guard against accidental duplication.

**SQLite pragmas:** `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=OFF`. WAL enables concurrent read access while the recorder writes, so DuckDB or `sqlite3` can query the live DB during the event.

## Operational behaviour

| Failure mode | Response |
|---|---|
| Status endpoint unreachable at startup | Fall back to default data URL, log a warning. |
| Data endpoint 5xx / timeout | Log error, skip this tick, retry next tick. |
| JSON parse error | Save raw body with a `.parse-error` suffix for post-hoc diagnosis; skip DB insert. |
| Raw-write error (disk full, permission) | Log error; skip the SQL insert so the DB never references a missing snapshot. Next tick retries. |
| SQLite write error | Log loudly. Raw file is already on disk, so the sample is not lost — it can be reprocessed. |
| Disk full | Log error. Writes keep failing; no self-repair. |
| Network dropout for minutes | Each tick logs and skips. Recorder resumes automatically. |
| Process killed mid-tick | At worst the in-flight sample is lost. Previously-persisted data is unaffected (WAL + transaction). |
| Duplicate `api_update` | Skip write. |

Logging uses `tracing` with a line-oriented formatter. One log line per tick summarises `fetch_ok { pilots, controllers, fetch_ms, api_update }` or the failure. Log level controlled via `RUST_LOG`.

## Testing

- **Unit tests** on the transform step. Commit a trimmed real VATSIM response as `tests/fixtures/vatsim-data.json`. Drive it through the writer into an in-memory SQLite database; assert row counts and a handful of representative field values.
- **One integration test** using `httpmock`: a mock server serves two successive JSON bodies with the *same* `update_timestamp`; assert the second is deduped.
- **One `#[ignore]`-gated smoke test** that hits the real VATSIM endpoint. Run manually before the event to confirm nothing is broken.

## Dependencies

```toml
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time", "signal"] }
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
```

`rusqlite` with `bundled` removes the system SQLite dependency — the build is self-contained.

## Out of scope / future work

- 3D playback viewer (CesiumJS / deck.gl / custom) that reads from `recorder.db`.
- Derived analytical views (e.g. NAT track assignment, density heatmaps) — these live in notebooks or a follow-up tool, not in the recorder itself.
- Long-term storage format (Parquet export, compression tiers).
- Supervision: systemd unit, health-check endpoint. The user runs this in tmux or under a hand-rolled unit for v1.
