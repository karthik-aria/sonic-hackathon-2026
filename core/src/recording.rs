use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A Redis recording (the JSON written by `--record`). Only the fields the analyzer needs;
/// `fields` duplicates `args`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Recording {
    /// `BTreeMap` so `ObjectMap` messages go out in a stable order.
    pub port_map: BTreeMap<String, String>,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Event {
    pub offset_s: f64,
    pub db: String,
    pub cmd: String,
    pub key: String,
    pub args: Vec<String>,
    pub client: String,
}

/// None for a negative or non-finite offset.
// std has no checked f64 -> u64 conversion, and `as` is denied. Rounding first makes the value a
// whole number, so reading it back as whole seconds of a Duration is exact.
#[must_use]
#[inline]
pub fn ts_ns(offset_s: f64) -> Option<u64> {
    Duration::try_from_secs_f64((offset_s * 1e9).round())
        .ok()
        .map(|d| d.as_secs())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::zero(0.0_f64, Some(0_u64))]
    #[case::micros(8.593_362_f64, Some(8_593_362_000_u64))]
    #[case::rounds_up(0.000_000_000_6_f64, Some(1_u64))]
    #[case::rounds_down(0.000_000_000_4_f64, Some(0_u64))]
    #[case::negative(-1.0_f64, None)]
    #[case::nan(f64::NAN, None)]
    #[case::infinite(f64::INFINITY, None)]
    fn test_ts_ns(#[case] offset_s: f64, #[case] expected: Option<u64>) {
        assert_eq!(ts_ns(offset_s), expected);
    }

    #[test]
    fn test_recording_deserialize() {
        let text = r#"{
            "case": "test",
            "port_map": {"oid:0x2": "Ethernet9", "oid:0x1": "Ethernet1"},
            "events": [{
                "ts": 1.5, "offset_s": 0.5, "db": "APPL_DB", "cmd": "DEL",
                "key": "_PORT_TABLE:Ethernet1", "name": "Ethernet1", "client": "lua",
                "fields": {}, "args": []
            }]
        }"#;
        assert_eq!(
            serde_json::from_str::<Recording>(text).unwrap(),
            Recording {
                port_map: BTreeMap::from([
                    ("oid:0x1".to_owned(), "Ethernet1".to_owned()),
                    ("oid:0x2".to_owned(), "Ethernet9".to_owned()),
                ]),
                events: vec![Event {
                    offset_s: 0.5_f64,
                    db: "APPL_DB".to_owned(),
                    cmd: "DEL".to_owned(),
                    key: "_PORT_TABLE:Ethernet1".to_owned(),
                    args: vec![],
                    client: "lua".to_owned(),
                }],
            }
        );
    }
}
