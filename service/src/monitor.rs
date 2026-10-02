use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use config_analyzer_pipeline_manager::pipeline::Pipeline;

use crate::db_layout::MonitorSource;
use crate::redis::Connection;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MonitorEvent {
    pub(crate) timestamp_ns: u64,
    pub(crate) database: String,
    pub(crate) client: String,
    pub(crate) args: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum MonitorMessage {
    Event(MonitorEvent),
    SourceState { name: String, connected: bool },
}

pub(crate) fn run(
    source: &MonitorSource,
    sender: &Sender<MonitorMessage>,
    active: &AtomicBool,
    pipelines: &[&'static Pipeline],
) {
    let label = source_label(&source.databases);
    loop {
        if connect_and_read(source, sender, &label, active, pipelines).is_ok() {
            break;
        }
        if sender
            .send(MonitorMessage::SourceState {
                name: label.clone(),
                connected: false,
            })
            .is_err()
        {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn connect_and_read(
    source: &MonitorSource,
    sender: &Sender<MonitorMessage>,
    label: &str,
    active: &AtomicBool,
    pipelines: &[&'static Pipeline],
) -> Result<(), ()> {
    let mut connection = Connection::connect(&source.endpoint).map_err(|_error| ())?;
    connection
        .authenticate(&source.endpoint)
        .map_err(|_error| ())?;
    let database_id = source.databases.keys().next().copied().ok_or(())?;
    connection.select(database_id).map_err(|_error| ())?;
    connection.start_monitor().map_err(|_error| ())?;
    if sender
        .send(MonitorMessage::SourceState {
            name: label.to_owned(),
            connected: true,
        })
        .is_err()
    {
        return Ok(());
    }

    let mut line = String::new();
    loop {
        match connection.read_monitor_line(&mut line) {
            Ok(true) => {
                if let Some(parsed) = parse_monitor_line(&line)
                    && let Some(database) = source.databases.get(&parsed.database_id)
                {
                    let event = MonitorEvent {
                        timestamp_ns: parsed.timestamp_ns,
                        database: database.clone(),
                        client: parsed.client,
                        args: parsed.args,
                    };
                    let trigger = super::window::is_trigger(&event, pipelines);
                    let was_active = active.load(Ordering::Acquire);
                    if trigger {
                        active.store(true, Ordering::Release);
                    }
                    if super::window::should_capture(&event, was_active || trigger, pipelines)
                        && sender.send(MonitorMessage::Event(event)).is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Ok(false) | Err(_) => return Err(()),
        }
    }
}

fn source_label(databases: &BTreeMap<u32, String>) -> String {
    databases.values().cloned().collect::<Vec<_>>().join(",")
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedMonitorLine {
    timestamp_ns: u64,
    database_id: u32,
    client: String,
    args: Vec<String>,
}

fn parse_monitor_line(line: &str) -> Option<ParsedMonitorLine> {
    let line = line.trim();
    let line = line.strip_prefix('+').unwrap_or(line);
    let (timestamp, rest) = line.split_once(" [")?;
    let (metadata, command_text) = rest.split_once("] ")?;
    let (database_id, client) = metadata.split_once(' ')?;
    let args = parse_arguments(command_text)?;
    if args.is_empty() {
        return None;
    }
    Some(ParsedMonitorLine {
        timestamp_ns: parse_timestamp_ns(timestamp)?,
        database_id: database_id.parse().ok()?,
        client: client.to_owned(),
        args,
    })
}

fn parse_timestamp_ns(timestamp: &str) -> Option<u64> {
    let (seconds, fraction) = timestamp.split_once('.')?;
    if fraction.is_empty()
        || fraction.len() > 9
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let seconds = seconds.parse::<u64>().ok()?;
    let fractional = fraction.parse::<u64>().ok()?;
    let scale = 10_u64.checked_pow(u32::try_from(9_usize.checked_sub(fraction.len())?).ok()?)?;
    seconds
        .checked_mul(1_000_000_000)?
        .checked_add(fractional.checked_mul(scale)?)
}

fn parse_arguments(text: &str) -> Option<Vec<String>> {
    let bytes = text.as_bytes();
    let mut args = Vec::new();
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if cursor == bytes.len() {
            break;
        }
        let quoted = bytes.get(cursor) == Some(&b'"');
        if quoted {
            cursor += 1;
        }
        let mut token = Vec::new();
        let mut closed = !quoted;
        while let Some(byte) = bytes.get(cursor).copied() {
            if quoted && byte == b'"' {
                cursor += 1;
                closed = true;
                break;
            }
            if !quoted && byte.is_ascii_whitespace() {
                break;
            }
            if byte == b'\\' {
                cursor += 1;
                let escape = bytes.get(cursor).copied()?;
                if escape == b'x' {
                    let high = hex_value(*bytes.get(cursor + 1)?)?;
                    let low = hex_value(*bytes.get(cursor + 2)?)?;
                    token.push(high * 16 + low);
                    cursor += 3;
                    continue;
                }
                token.push(match escape {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'a' => 7,
                    b'b' => 8,
                    other => other,
                });
                cursor += 1;
                continue;
            }
            token.push(byte);
            cursor += 1;
        }
        if !closed {
            return None;
        }
        args.push(String::from_utf8_lossy(&token).into_owned());
    }
    Some(args)
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::quoted(
        "+1710000000.123456 [4 127.0.0.1:1234] \"HSET\" \"PORT|Ethernet1\" \"mtu\" \"9100\"",
        Some(ParsedMonitorLine {
            timestamp_ns: 1_710_000_000_123_456_000,
            database_id: 4,
            client: "127.0.0.1:1234".to_owned(),
            args: vec!["HSET".to_owned(), "PORT|Ethernet1".to_owned(), "mtu".to_owned(), "9100".to_owned()]
        })
    )]
    #[case::lua(
        "+1710000000.000001 [0 lua] HSET \"_PORT_TABLE:Ethernet1\" \"mtu\" \"9100\"",
        Some(ParsedMonitorLine {
            timestamp_ns: 1_710_000_000_000_001_000,
            database_id: 0,
            client: "lua".to_owned(),
            args: vec!["HSET".to_owned(), "_PORT_TABLE:Ethernet1".to_owned(), "mtu".to_owned(), "9100".to_owned()]
        })
    )]
    #[case::escaped_json(
        "+1710000000.1 [1 unix] \"HSET\" \"ASIC_STATE:x:{\\\"a\\\":\\\"b\\\"}\"",
        Some(ParsedMonitorLine {
            timestamp_ns: 1_710_000_000_100_000_000,
            database_id: 1,
            client: "unix".to_owned(),
            args: vec!["HSET".to_owned(), "ASIC_STATE:x:{\"a\":\"b\"}".to_owned()]
        })
    )]
    #[case::bad_line("nonsense", None)]
    fn test_parse_monitor_line(#[case] line: &str, #[case] expected: Option<ParsedMonitorLine>) {
        assert_eq!(parse_monitor_line(line), expected);
    }

    #[test]
    fn test_parse_hex_escaped_argument_bytes() {
        assert_eq!(parse_arguments("\"a\\x20b\""), Some(vec!["a b".to_owned()]));
    }
}
