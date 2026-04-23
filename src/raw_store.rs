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
    enc.write_all(raw_body)
        .with_context(|| format!("write gzip body to {}", abs.display()))?;
    enc.finish()
        .with_context(|| format!("finish gzip stream for {}", abs.display()))?;
    Ok(rel)
}

#[derive(Debug)]
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
