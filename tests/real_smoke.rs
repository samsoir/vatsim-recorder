//! Smoke test that hits the real VATSIM data endpoint. Run manually:
//!   cargo test --test real_smoke -- --ignored --nocapture

use vatsim_recorder::fetcher::{DEFAULT_DATA_URL, DEFAULT_STATUS_URL, Fetcher};

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn real_fetch_smoke() {
    let mut fetcher = Fetcher::new(DEFAULT_STATUS_URL, DEFAULT_DATA_URL)
        .await
        .unwrap();
    let fetch = fetcher.fetch_once().await.unwrap();
    assert!(
        !fetch.feed.pilots.is_empty(),
        "expected at least one pilot live on the network"
    );
    eprintln!(
        "smoke ok: pilots={}, controllers={}, api_update={}, duration_ms={}",
        fetch.feed.pilots.len(),
        fetch.feed.controllers.len(),
        fetch.feed.general.update_timestamp,
        fetch.duration_ms,
    );
}
