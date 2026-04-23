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
