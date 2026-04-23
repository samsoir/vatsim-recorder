# vatsim-recorder

Polls the VATSIM data feed every 60 s (configurable down to the feed's own 15 s
cache TTL) and writes a time series of aircraft and controller state to SQLite,
with raw gzipped JSON snapshots preserved on disk. Built to record Cross the
Pond 2026.

## Build

    cargo build --release

The release binary lands at `target/release/vatsim-recorder`.

## Run

    vatsim-recorder run --data-dir ./data

Accepted flags:

- `--interval-secs N` (default: 60, minimum: 15). The floor matches the
  VATSIM v3 feed's advertised cache TTL; polling faster returns duplicates.
- `--status-url URL` override the VATSIM status endpoint (testing).
- `--data-url URL` override the fallback data endpoint (testing).

Logs go to stdout; set verbosity via `RUST_LOG=info` / `debug`.

Shut down with `Ctrl-C` or `SIGTERM`. If a tick is in flight when a signal is
received, the recorder waits for it to complete (up to the HTTP fetch timeout
of ~30 s) before exiting.

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

See `docs/specs/2026-04-21-vatsim-recorder-design.md`.

## License

MIT — see [LICENSE](LICENSE).
