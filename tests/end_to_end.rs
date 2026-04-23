//! Drives the full fetch→write path using a mock HTTP server.

use chrono::{TimeZone, Utc};
use httpmock::prelude::*;
use tempfile::tempdir;
use vatsim_recorder::{db, fetcher::Fetcher, writer};

const FIXTURE: &str = include_str!("fixtures/vatsim-data.json");

#[tokio::test(flavor = "current_thread")]
async fn fetch_then_write_then_query() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/status.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"{"data":{"v3":["__DATA__"]}}"#
                        .replace("__DATA__", &format!("{}/data.json", server.base_url())),
                );
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/data.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(FIXTURE);
        })
        .await;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("recorder.db");
    let mut conn = db::open(&db_path).unwrap();
    let mut fetcher = Fetcher::new(
        &format!("{}/status.json", server.base_url()),
        "http://unused.example/",
    )
    .await
    .unwrap();

    let fetch = fetcher.fetch_once().await.unwrap();
    let outcome = writer::write(
        &mut conn,
        dir.path(),
        Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap(),
        fetch.duration_ms,
        &fetch.raw,
        &fetch.feed,
    )
    .unwrap();
    assert!(matches!(
        outcome,
        writer::WriteOutcome::Written {
            pilots: 2,
            controllers: 1
        }
    ));

    assert_eq!(
        fetch.feed.general.update_timestamp,
        "2026-04-21T12:00:00.0000000Z"
    );

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
