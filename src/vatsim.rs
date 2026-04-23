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
