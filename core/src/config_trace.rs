use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use crate::recording::{Event, Recording};

const ASIC_QUEUE_KEY: &str = "ASIC_STATE_KEY_VALUE_OP_QUEUE";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub lines: u64,
    pub skipped: u64,
    pub unpaired: u64,
    pub unmapped: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Line {
    ts_ns: u64,
    process: String,
    event: String,
    table: String,
    key: String,
    detail: String,
    order: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SaiOperation {
    Set,
    Create,
    Remove,
}

#[derive(Clone, Debug)]
struct Request {
    table: String,
    oid: String,
    operation: SaiOperation,
    attr: Option<String>,
    process: String,
    ts_ns: u64,
    order: u64,
}

#[derive(Debug)]
struct Candidate {
    ts_ns: u64,
    order: u64,
    suborder: u8,
    oid: Option<String>,
    event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecvDetail {
    db: String,
    op: String,
    fields: Vec<String>,
}

/// Parses one or more per-process config-trace streams into Redis-recorder-shaped data.
#[must_use]
#[inline]
pub fn parse(inputs: &[&str]) -> (Recording, Stats) {
    let mut stats = Stats::default();
    let mut lines = Vec::new();
    let mut order = 0_u64;
    for input in inputs {
        for raw in input.lines() {
            stats.lines = stats.lines.saturating_add(1);
            if is_recording_header(raw) {
                continue;
            }
            match parse_line(raw, order) {
                Some(line) => lines.push(line),
                None => stats.skipped = stats.skipped.saturating_add(1),
            }
            order = order.saturating_add(1);
        }
    }
    lines.sort_by_key(|line| (line.ts_ns, line.order));
    let first_ts = lines.first().map_or(0, |line| line.ts_ns);

    let mut port_map = BTreeMap::new();
    let mut candidates = Vec::new();
    let mut requests: HashMap<String, VecDeque<Request>> = HashMap::new();
    for line in lines {
        match line.event.as_str() {
            "map" => {
                if line.table.is_empty() || line.key.is_empty() || line.detail.is_empty() {
                    stats.skipped = stats.skipped.saturating_add(1);
                } else {
                    drop(port_map.insert(line.key, line.detail));
                }
            }
            "recv" => match recv_detail(&line.process, &line.detail) {
                Some(detail) => {
                    if let Some(event) = recv_event(&line, detail) {
                        candidates.push(event);
                    } else {
                        stats.skipped = stats.skipped.saturating_add(1);
                    }
                }
                None => stats.skipped = stats.skipped.saturating_add(1),
            },
            "appl" => match parse_fields(&line.detail) {
                Some(fields) => candidates.push(Candidate {
                    ts_ns: line.ts_ns,
                    order: line.order,
                    suborder: 0,
                    oid: None,
                    event: Event {
                        offset_s: 0.0,
                        db: "APPL_DB".to_owned(),
                        cmd: "HSET".to_owned(),
                        key: format!("_{}:{}", line.table, line.key),
                        args: redis_args(fields),
                        client: line.process,
                    },
                }),
                None => stats.skipped = stats.skipped.saturating_add(1),
            },
            "sai_req" => {
                if let Some(request) = parse_request(&line) {
                    if request.operation != SaiOperation::Create {
                        candidates.push(request_event(&request, &request.oid));
                    }
                    requests.entry(line.process).or_default().push_back(request);
                } else {
                    stats.skipped = stats.skipped.saturating_add(1);
                }
            }
            "sai_resp" => {
                if !pair_response(&line, &mut requests, &mut candidates, &mut stats) {
                    stats.unpaired = stats.unpaired.saturating_add(1);
                }
            }
            "unmap" | "done" | "pending" | "error" | "sai_fail" => {}
            _ => stats.skipped = stats.skipped.saturating_add(1),
        }
    }
    stats.unpaired = requests.values().fold(stats.unpaired, |count, queue| {
        count.saturating_add(u64::try_from(queue.len()).unwrap_or(u64::MAX))
    });

    candidates.sort_by_key(|candidate| (candidate.ts_ns, candidate.order, candidate.suborder));
    let mut events = Vec::with_capacity(candidates.len());
    for mut candidate in candidates {
        if candidate
            .oid
            .as_ref()
            .is_some_and(|oid| !port_map.contains_key(oid))
        {
            stats.unmapped = stats.unmapped.saturating_add(1);
            continue;
        }
        let offset_ns = candidate.ts_ns.saturating_sub(first_ts);
        candidate.event.offset_s = Duration::from_nanos(offset_ns).as_secs_f64();
        events.push(candidate.event);
    }
    (Recording { port_map, events }, stats)
}

fn is_recording_header(line: &str) -> bool {
    line.split_once('|')
        .is_some_and(|(_, detail)| detail == "recording started")
}

fn parse_line(raw: &str, order: u64) -> Option<Line> {
    let mut fields = raw.split('|');
    let ts = fields.next()?;
    let process = fields.next()?;
    let event = fields.next()?;
    let table = fields.next()?;
    let key = fields.next()?;
    let _epoch = fields.next()?;
    let detail = fields.next()?;
    if fields.next().is_some() || process.is_empty() || event.is_empty() {
        return None;
    }
    Some(Line {
        ts_ns: parse_ts(ts)?,
        process: process.to_owned(),
        event: event.to_owned(),
        table: table.to_owned(),
        key: key.to_owned(),
        detail: detail.to_owned(),
        order,
    })
}

fn parse_ts(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 26
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'.')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || bytes.get(19) != Some(&b'.')
    {
        return None;
    }
    for (start, end) in [
        (0, 4),
        (5, 7),
        (8, 10),
        (11, 13),
        (14, 16),
        (17, 19),
        (20, 26),
    ] {
        if !bytes.get(start..end)?.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }
    let year = number(value, 0, 4)?;
    let month = number(value, 5, 7)?;
    let day = number(value, 8, 10)?;
    let hour = number(value, 11, 13)?;
    let minute = number(value, 14, 16)?;
    let second = number(value, 17, 19)?;
    let micros = number(value, 20, 26)?;
    if year == 0 || !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days_in_month = match month {
        2 if leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > days_in_month {
        return None;
    }

    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let adjusted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600)?
        .checked_add(i64::from(minute) * 60)?
        .checked_add(i64::from(second))?;
    let seconds = u64::try_from(seconds).ok()?;
    seconds
        .checked_mul(1_000_000_000)?
        .checked_add(u64::from(micros) * 1_000)
}

fn number(value: &str, start: usize, end: usize) -> Option<u32> {
    value.get(start..end)?.parse().ok()
}

const fn leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn recv_detail(process: &str, detail: &str) -> Option<RecvDetail> {
    let (db, remainder) = match detail.split_once(':') {
        Some((db, remainder)) if db.ends_with("_DB") => (db.to_owned(), remainder),
        _ if process.ends_with("mgrd") => ("CONFIG_DB".to_owned(), detail),
        _ if process == "orchagent" => ("APPL_DB".to_owned(), detail),
        _ => ("UNKNOWN_DB".to_owned(), detail),
    };
    let (op, names) = remainder.split_once(':')?;
    if op.is_empty() {
        return None;
    }
    let fields = parse_fields(names)?;
    Some(RecvDetail {
        db,
        op: op.to_owned(),
        fields,
    })
}

fn parse_fields(value: &str) -> Option<Vec<String>> {
    if value.is_empty() {
        return Some(Vec::new());
    }
    let fields: Vec<String> = value.split(',').map(str::to_owned).collect();
    fields
        .iter()
        .all(|field| !field.is_empty())
        .then_some(fields)
}

fn redis_args(fields: Vec<String>) -> Vec<String> {
    fields
        .into_iter()
        .flat_map(|field| [field, String::new()])
        .collect()
}

fn recv_event(line: &Line, detail: RecvDetail) -> Option<Candidate> {
    let (cmd, key, args) = match detail.db.as_str() {
        "CONFIG_DB" => {
            let key = format!("{}|{}", line.table, line.key);
            match detail.op.as_str() {
                "SET" => ("HSET", key, redis_args(detail.fields)),
                "DEL" => ("DEL", key, Vec::new()),
                _ => return None,
            }
        }
        "APPL_DB" => ("DEL", format!("_{}:{}", line.table, line.key), Vec::new()),
        _ => (
            detail.op.as_str(),
            format!("{}|{}", line.table, line.key),
            Vec::new(),
        ),
    };
    Some(Candidate {
        ts_ns: line.ts_ns,
        order: line.order,
        suborder: 0,
        oid: None,
        event: Event {
            offset_s: 0.0,
            db: detail.db,
            cmd: cmd.to_owned(),
            key,
            args,
            client: line.process.clone(),
        },
    })
}

fn parse_request(line: &Line) -> Option<Request> {
    if line.table.is_empty() {
        return None;
    }
    let (operation, attr) = match line.detail.as_str() {
        "create" => (SaiOperation::Create, None),
        "remove" => (SaiOperation::Remove, None),
        detail => {
            let attr = detail.strip_prefix("set:")?;
            if attr.is_empty() {
                return None;
            }
            (SaiOperation::Set, Some(attr.to_owned()))
        }
    };
    if (operation == SaiOperation::Create) != (line.key == "-") || line.key.is_empty() {
        return None;
    }
    Some(Request {
        table: line.table.clone(),
        oid: line.key.clone(),
        operation,
        attr,
        process: line.process.clone(),
        ts_ns: line.ts_ns,
        order: line.order,
    })
}

fn request_event(request: &Request, oid: &str) -> Candidate {
    let op = match request.operation {
        SaiOperation::Set => "Sset",
        SaiOperation::Create => "Screate",
        SaiOperation::Remove => "Sremove",
    };
    let attrs = request
        .attr
        .as_ref()
        .map_or_else(|| "[]".to_owned(), |attr| format!("[\"{attr}\",\"\"]"));
    Candidate {
        ts_ns: request.ts_ns,
        order: request.order,
        suborder: 0,
        oid: Some(oid.to_owned()),
        event: Event {
            offset_s: 0.0,
            db: "ASIC_DB".to_owned(),
            cmd: "LPUSH".to_owned(),
            key: ASIC_QUEUE_KEY.to_owned(),
            args: vec![format!("{}:{oid}", request.table), attrs, op.to_owned()],
            client: request.process.clone(),
        },
    }
}

fn pair_response(
    line: &Line,
    requests: &mut HashMap<String, VecDeque<Request>>,
    candidates: &mut Vec<Candidate>,
    stats: &mut Stats,
) -> bool {
    let Some((op, sai_status)) = line.detail.split_once(':') else {
        stats.skipped = stats.skipped.saturating_add(1);
        return true;
    };
    let Some(queue) = requests.get_mut(&line.process) else {
        return false;
    };
    let Some(request) = queue.pop_front() else {
        return false;
    };
    let expected_op = match request.operation {
        SaiOperation::Set => "set",
        SaiOperation::Create => "create",
        SaiOperation::Remove => "remove",
    };
    let oid = if request.operation == SaiOperation::Create {
        line.key.as_str()
    } else {
        request.oid.as_str()
    };
    if line.table != request.table
        || op != expected_op
        || oid != line.key
        || oid.is_empty()
        || oid == "-"
    {
        stats.unpaired = stats.unpaired.saturating_add(1);
        return true;
    }
    if request.operation == SaiOperation::Create {
        candidates.push(request_event(&request, oid));
    }
    if sai_status == "SAI_STATUS_SUCCESS" {
        candidates.push(Candidate {
            ts_ns: line.ts_ns,
            order: line.order,
            suborder: 1,
            oid: Some(oid.to_owned()),
            event: Event {
                offset_s: 0.0,
                db: "ASIC_DB".to_owned(),
                cmd: if request.operation == SaiOperation::Remove {
                    "DEL".to_owned()
                } else {
                    "HSET".to_owned()
                },
                key: format!("ASIC_STATE:{}:{oid}", line.table),
                args: match request.operation {
                    SaiOperation::Set => redis_args(vec![request.attr.unwrap_or_default()]),
                    SaiOperation::Create => vec!["NULL".to_owned(), "NULL".to_owned()],
                    SaiOperation::Remove => Vec::new(),
                },
                client: line.process.clone(),
            },
        });
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn event(db: &str, cmd: &str, key: &str, args: &[&str], client: &str) -> Event {
        event_at(0.0, db, cmd, key, args, client)
    }

    fn event_at(
        offset_s: f64,
        db: &str,
        cmd: &str,
        key: &str,
        args: &[&str],
        client: &str,
    ) -> Event {
        Event {
            offset_s,
            db: db.to_owned(),
            cmd: cmd.to_owned(),
            key: key.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            client: client.to_owned(),
        }
    }

    #[rstest]
    #[case("1970-01-01.00:00:00.000000", Some(0))]
    #[case("2024-02-29.12:34:56.123456", Some(1_709_210_096_123_456_000))]
    #[case("2023-02-29.12:34:56.123456", None)]
    #[case("2024-13-01.12:34:56.123456", None)]
    #[case("2024-04-31.12:34:56.123456", None)]
    #[case("2024-01-01.24:00:00.123456", None)]
    #[case("2024-01-01.00:00:00.12345", None)]
    #[case("1969-12-31.23:59:59.999999", None)]
    fn test_parse_ts(#[case] input: &str, #[case] expected: Option<u64>) {
        assert_eq!(parse_ts(input), expected);
    }

    #[rstest]
    #[case::valid(
        "2026-10-01.03:38:55.123456|orchagent|recv|PORT_TABLE|Ethernet1|-|APPL_DB:SET:mtu",
        true
    )]
    #[case::too_many(
        "2026-10-01.03:38:55.123456|orchagent|recv|PORT_TABLE|Ethernet1|-|APPL_DB:SET:mtu|extra",
        false
    )]
    #[case::too_few(
        "2026-10-01.03:38:55.123456|orchagent|recv|PORT_TABLE|Ethernet1|-",
        false
    )]
    fn test_parse_line(#[case] input: &str, #[case] expected: bool) {
        assert_eq!(parse_line(input, 0).is_some(), expected);
    }

    #[rstest]
    #[case("orchagent", "APPL_DB:SET:mtu,admin_status", Some(("APPL_DB", "SET", vec!["mtu", "admin_status"] )))]
    #[case("portmgrd", "CONFIG_DB:SET:mtu", Some(("CONFIG_DB", "SET", vec!["mtu"] )))]
    #[case("orchagent", "SET:mtu", Some(("APPL_DB", "SET", vec!["mtu"] )))]
    #[case("portmgrd", "SET:mtu", Some(("CONFIG_DB", "SET", vec!["mtu"] )))]
    #[case("other", "STATE_DB:SET:oper_status", Some(("STATE_DB", "SET", vec!["oper_status"] )))]
    #[case("other", "SET:mtu", Some(("UNKNOWN_DB", "SET", vec!["mtu"] )))]
    #[case("orchagent", "APPL_DB:SET:mtu,,speed", None)]
    fn test_recv_detail(
        #[case] process: &str,
        #[case] detail: &str,
        #[case] expected: Option<(&str, &str, Vec<&str>)>,
    ) {
        let expected = expected.map(|(db, op, fields)| RecvDetail {
            db: db.to_owned(),
            op: op.to_owned(),
            fields: fields.into_iter().map(str::to_owned).collect(),
        });
        assert_eq!(recv_detail(process, detail), expected);
    }

    #[test]
    fn test_parse_normalizes_config_appl_and_map() {
        let text = concat!(
            "2026-10-01.03:38:55.000000|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:mtu,admin_status\n",
            "2026-10-01.03:38:55.000010|portmgrd|appl|PORT_TABLE|Ethernet1|-|mtu,admin_status\n",
            "2026-10-01.03:38:55.000020|orchagent|recv|PORT_TABLE|Ethernet1|-|APPL_DB:SET:mtu,admin_status\n",
            "2026-10-01.03:38:55.000030|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x1|-|Ethernet1\n",
        );
        let (recording, stats) = parse(&[text]);
        assert_eq!(
            (recording, stats),
            (
                Recording {
                    port_map: BTreeMap::from([("oid:0x1".to_owned(), "Ethernet1".to_owned())]),
                    events: vec![
                        event(
                            "CONFIG_DB",
                            "HSET",
                            "PORT|Ethernet1",
                            &["mtu", "", "admin_status", ""],
                            "portmgrd"
                        ),
                        event_at(
                            0.000_01,
                            "APPL_DB",
                            "HSET",
                            "_PORT_TABLE:Ethernet1",
                            &["mtu", "", "admin_status", ""],
                            "portmgrd"
                        ),
                        event_at(
                            0.000_02,
                            "APPL_DB",
                            "DEL",
                            "_PORT_TABLE:Ethernet1",
                            &[],
                            "orchagent"
                        ),
                    ],
                },
                Stats {
                    lines: 4,
                    skipped: 0,
                    unpaired: 0,
                    unmapped: 0
                },
            )
        );
    }

    #[test]
    fn test_parse_pairs_sai_and_drops_unmapped_oid() {
        let text = concat!(
            "2026-10-01.03:38:55.000000|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_PORT_ATTR_MTU\n",
            "2026-10-01.03:38:55.000001|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000002|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x2|-|set:SAI_PORT_ATTR_MTU\n",
            "2026-10-01.03:38:55.000003|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x2|-|set:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000004|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x1|-|Ethernet1\n",
        );
        let (recording, stats) = parse(&[text]);
        assert_eq!(
            (recording, stats),
            (
                Recording {
                    port_map: BTreeMap::from([("oid:0x1".to_owned(), "Ethernet1".to_owned())]),
                    events: vec![
                        event(
                            "ASIC_DB",
                            "LPUSH",
                            ASIC_QUEUE_KEY,
                            &[
                                "SAI_OBJECT_TYPE_PORT:oid:0x1",
                                "[\"SAI_PORT_ATTR_MTU\",\"\"]",
                                "Sset"
                            ],
                            "orchagent"
                        ),
                        event_at(
                            0.000_001,
                            "ASIC_DB",
                            "HSET",
                            "ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x1",
                            &["SAI_PORT_ATTR_MTU", ""],
                            "orchagent"
                        ),
                    ],
                },
                Stats {
                    lines: 5,
                    skipped: 0,
                    unpaired: 0,
                    unmapped: 2
                },
            )
        );
    }

    #[test]
    fn test_bulk_sai_requests_pair_responses_fifo_per_process() {
        let text = concat!(
            "2026-10-01.03:38:55.000000|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_PORT_ATTR_MTU\n",
            "2026-10-01.03:38:55.000001|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x2|-|set:SAI_PORT_ATTR_ADMIN_STATE\n",
            "2026-10-01.03:38:55.000002|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000003|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x2|-|set:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000004|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x1|-|Ethernet1\n",
            "2026-10-01.03:38:55.000005|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x2|-|Ethernet2\n",
        );
        let (recording, stats) = parse(&[text]);
        assert_eq!(
            recording,
            Recording {
                port_map: BTreeMap::from([
                    ("oid:0x1".to_owned(), "Ethernet1".to_owned()),
                    ("oid:0x2".to_owned(), "Ethernet2".to_owned()),
                ]),
                events: vec![
                    event(
                        "ASIC_DB",
                        "LPUSH",
                        ASIC_QUEUE_KEY,
                        &[
                            "SAI_OBJECT_TYPE_PORT:oid:0x1",
                            "[\"SAI_PORT_ATTR_MTU\",\"\"]",
                            "Sset"
                        ],
                        "orchagent"
                    ),
                    event_at(
                        0.000_001,
                        "ASIC_DB",
                        "LPUSH",
                        ASIC_QUEUE_KEY,
                        &[
                            "SAI_OBJECT_TYPE_PORT:oid:0x2",
                            "[\"SAI_PORT_ATTR_ADMIN_STATE\",\"\"]",
                            "Sset"
                        ],
                        "orchagent"
                    ),
                    event_at(
                        0.000_002,
                        "ASIC_DB",
                        "HSET",
                        "ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x1",
                        &["SAI_PORT_ATTR_MTU", ""],
                        "orchagent"
                    ),
                    event_at(
                        0.000_003,
                        "ASIC_DB",
                        "HSET",
                        "ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x2",
                        &["SAI_PORT_ATTR_ADMIN_STATE", ""],
                        "orchagent"
                    ),
                ],
            }
        );
        assert_eq!(
            stats,
            Stats {
                lines: 6,
                skipped: 0,
                unpaired: 0,
                unmapped: 0
            }
        );
    }

    #[test]
    fn test_create_request_waits_for_response_oid_and_failure_stays_open() {
        let text = concat!(
            "2026-10-01.03:38:55.000000|orchagent|sai_req|SAI_OBJECT_TYPE_ROUTER_INTERFACE|-|-|create\n",
            "2026-10-01.03:38:55.000001|orchagent|sai_resp|SAI_OBJECT_TYPE_ROUTER_INTERFACE|oid:0x2|-|create:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000002|orchagent|map|SAI_OBJECT_TYPE_ROUTER_INTERFACE|oid:0x2|-|Ethernet1\n",
            "2026-10-01.03:38:55.000003|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x3|-|set:SAI_PORT_ATTR_MTU\n",
            "2026-10-01.03:38:55.000004|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x3|-|set:SAI_STATUS_FAILURE\n",
            "2026-10-01.03:38:55.000005|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x3|-|Ethernet2\n",
        );
        let (recording, stats) = parse(&[text]);
        assert_eq!(
            (recording, stats),
            (
                Recording {
                    port_map: BTreeMap::from([
                        ("oid:0x2".to_owned(), "Ethernet1".to_owned()),
                        ("oid:0x3".to_owned(), "Ethernet2".to_owned()),
                    ]),
                    events: vec![
                        event(
                            "ASIC_DB",
                            "LPUSH",
                            ASIC_QUEUE_KEY,
                            &["SAI_OBJECT_TYPE_ROUTER_INTERFACE:oid:0x2", "[]", "Screate"],
                            "orchagent"
                        ),
                        event_at(
                            0.000_001,
                            "ASIC_DB",
                            "HSET",
                            "ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE:oid:0x2",
                            &["NULL", "NULL"],
                            "orchagent"
                        ),
                        event_at(
                            0.000_003,
                            "ASIC_DB",
                            "LPUSH",
                            ASIC_QUEUE_KEY,
                            &[
                                "SAI_OBJECT_TYPE_PORT:oid:0x3",
                                "[\"SAI_PORT_ATTR_MTU\",\"\"]",
                                "Sset"
                            ],
                            "orchagent"
                        ),
                    ],
                },
                Stats {
                    lines: 6,
                    skipped: 0,
                    unpaired: 0,
                    unmapped: 0
                },
            )
        );
    }

    #[test]
    fn test_inputs_merge_by_timestamp_and_keep_input_order_for_ties() {
        let first = concat!(
            "2026-10-01.03:38:55.000002|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:mtu\n",
            "2026-10-01.03:38:55.000001|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:speed\n",
        );
        let second = "2026-10-01.03:38:55.000002|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:admin_status\n";
        let (recording, stats) = parse(&[first, second]);
        assert_eq!(
            recording.events,
            vec![
                event(
                    "CONFIG_DB",
                    "HSET",
                    "PORT|Ethernet1",
                    &["speed", ""],
                    "portmgrd"
                ),
                event_at(
                    0.000_001,
                    "CONFIG_DB",
                    "HSET",
                    "PORT|Ethernet1",
                    &["mtu", ""],
                    "portmgrd"
                ),
                event_at(
                    0.000_001,
                    "CONFIG_DB",
                    "HSET",
                    "PORT|Ethernet1",
                    &["admin_status", ""],
                    "portmgrd"
                ),
            ]
        );
        assert_eq!(
            stats,
            Stats {
                lines: 3,
                skipped: 0,
                unpaired: 0,
                unmapped: 0
            }
        );
    }

    #[test]
    fn test_stats_skip_malformed_and_count_orphan_sai_response() {
        let text = concat!(
            "not a record\n",
            "2026-10-01.03:38:55.000000|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_STATUS_SUCCESS\n",
            "2026-10-01.03:38:55.000001|orchagent|unknown|x|y|-|z\n",
        );
        let (recording, stats) = parse(&[text]);
        assert_eq!(recording.events, vec![]);
        assert_eq!(
            stats,
            Stats {
                lines: 3,
                skipped: 2,
                unpaired: 1,
                unmapped: 0
            }
        );
    }
}
