use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::path::Path;
use tracing::{info, warn};

use crate::flight_plan_hash;
use crate::raw_store::{self, Suffix};
use crate::vatsim::{Controller, DataFeed, FlightPlan, Pilot};

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

    // VATSIM occasionally returns the same CID twice in one snapshot (multi-connection
    // or ghost entries). The `positions`/`controllers` tables have PK (fetch_ts, cid),
    // so we keep one row per cid — the entry with the latest `last_updated`.
    let pilots = dedupe_pilots(&feed.pilots);
    let controllers = dedupe_controllers(&feed.controllers);

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
            pilots.len() as i64,
            controllers.len() as i64,
            fetch_duration_ms,
        ],
    )?;

    for pilot in &pilots {
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

    for ctrl in &controllers {
        let atis_json = ctrl
            .text_atis
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap());
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
        pilots: pilots.len(),
        controllers: controllers.len(),
    })
}

fn dedupe_pilots(pilots: &[Pilot]) -> Vec<&Pilot> {
    let mut winner: HashMap<i64, usize> = HashMap::with_capacity(pilots.len());
    for (i, p) in pilots.iter().enumerate() {
        match winner.get(&p.cid).copied() {
            Some(j) if pilots[j].last_updated >= p.last_updated => {
                warn!(
                    cid = p.cid,
                    kept_callsign = %pilots[j].callsign,
                    dropped_callsign = %p.callsign,
                    "duplicate pilot cid in feed; dropping older entry",
                );
            }
            Some(j) => {
                warn!(
                    cid = p.cid,
                    kept_callsign = %p.callsign,
                    dropped_callsign = %pilots[j].callsign,
                    "duplicate pilot cid in feed; dropping older entry",
                );
                winner.insert(p.cid, i);
            }
            None => {
                winner.insert(p.cid, i);
            }
        }
    }
    pilots
        .iter()
        .enumerate()
        .filter(|(i, p)| winner.get(&p.cid).copied() == Some(*i))
        .map(|(_, p)| p)
        .collect()
}

fn dedupe_controllers(controllers: &[Controller]) -> Vec<&Controller> {
    let mut winner: HashMap<i64, usize> = HashMap::with_capacity(controllers.len());
    for (i, c) in controllers.iter().enumerate() {
        match winner.get(&c.cid).copied() {
            Some(j) if controllers[j].last_updated >= c.last_updated => {
                warn!(
                    cid = c.cid,
                    kept_callsign = %controllers[j].callsign,
                    dropped_callsign = %c.callsign,
                    "duplicate controller cid in feed; dropping older entry",
                );
            }
            Some(j) => {
                warn!(
                    cid = c.cid,
                    kept_callsign = %c.callsign,
                    dropped_callsign = %controllers[j].callsign,
                    "duplicate controller cid in feed; dropping older entry",
                );
                winner.insert(c.cid, i);
            }
            None => {
                winner.insert(c.cid, i);
            }
        }
    }
    controllers
        .iter()
        .enumerate()
        .filter(|(i, c)| winner.get(&c.cid).copied() == Some(*i))
        .map(|(_, c)| c)
        .collect()
}

fn is_duplicate_api_update(conn: &Connection, api_update: &str) -> Result<bool> {
    let mut stmt = conn.prepare_cached("SELECT 1 FROM fetches WHERE api_update = ?1 LIMIT 1")?;
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
        assert_eq!(
            outcome,
            WriteOutcome::Written {
                pilots: 2,
                controllers: 1
            }
        );

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
            .query_row(
                "SELECT text_atis FROM controllers WHERE cid = 7654321",
                [],
                |r| r.get(0),
            )
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

        // Dedupe must short-circuit BEFORE raw_store is called — verify only one raw file exists.
        let raw_files: Vec<_> = walkdir_json_gz(data_dir);
        assert_eq!(
            raw_files.len(),
            1,
            "duplicate tick must not write a second raw file"
        );
    }

    #[test]
    fn dedupes_duplicate_cid_within_single_snapshot() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        let db_path = data_dir.join("recorder.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        let mut feed: DataFeed = serde_json::from_str(FIXTURE).unwrap();
        // Clone the first pilot (cid 1234567, "AAL123") and keep the same cid,
        // but give it a newer last_updated and a different callsign so we can
        // tell which row survived.
        let mut twin = feed.pilots[0].clone();
        twin.callsign = "DUPE".to_string();
        twin.last_updated = "2099-01-01T00:00:00.0000000Z".to_string();
        feed.pilots.push(twin);

        let ts = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        let outcome =
            write(&mut conn, data_dir, ts, 42, FIXTURE.as_bytes(), &feed).unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::Written {
                pilots: 2,
                controllers: 1,
            },
            "duplicate cid must collapse to one row, not abort the tick",
        );

        let rows_for_cid: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM positions WHERE cid = 1234567",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows_for_cid, 1);

        let surviving_callsign: String = conn
            .query_row(
                "SELECT callsign FROM positions WHERE cid = 1234567",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            surviving_callsign, "DUPE",
            "later last_updated should win",
        );

        let (pilot_count, controller_count): (i64, i64) = conn
            .query_row(
                "SELECT pilot_count, controller_count FROM fetches",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(pilot_count, 2);
        assert_eq!(controller_count, 1);
    }

    fn walkdir_json_gz(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        fn recurse(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    recurse(&path, out);
                } else if path.extension().and_then(|s| s.to_str()) == Some("gz") {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        recurse(root, &mut out);
        out
    }
}
