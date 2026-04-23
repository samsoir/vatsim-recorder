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
