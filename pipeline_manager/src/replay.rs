use std::sync::Arc;

use config_analyzer_core::recording::{self, Recording, ts_ns};
use config_analyzer_core::{At, SpanSink};
use tokio::sync::{mpsc, oneshot};

use crate::instance::Pending;
use crate::pipeline::Pipeline;
use crate::{Db, Message, RedisOp, spawn};

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ReplayError {
    #[error("event {seq}: invalid offset_s {offset_s}")]
    InvalidOffset { seq: u64, offset_s: f64 },
    #[error("analyzer input channel closed")]
    Closed,
    #[error("analyzer dropped the End reply")]
    NoReply,
}

// Must be called inside a tokio runtime. Each event's seq is its index in `events`; End carries
// the last event's time. Returns the End report.
#[inline]
pub async fn replay<K>(
    pipelines: &[&'static Pipeline],
    recording: Recording,
    sink: K,
) -> Result<Vec<Pending>, ReplayError>
where
    K: SpanSink + Clone + 'static,
{
    let tx = spawn(pipelines, sink);
    for (oid, name) in recording.port_map {
        if oid.is_empty() {
            continue;
        }
        send(
            &tx,
            Message::ObjectMap {
                oid: Arc::from(oid),
                name: Arc::from(name),
            },
        )
        .await?;
    }
    let mut end = At::default();
    for (seq, ev) in (0_u64..).zip(recording.events) {
        let op = into_op(ev, seq)?;
        end = op.at;
        send(&tx, Message::Op(op)).await?;
    }
    let (reply, rx) = oneshot::channel();
    send(&tx, Message::End { at: end, reply }).await?;
    rx.await.map_err(|_dropped| ReplayError::NoReply)
}

async fn send(tx: &mpsc::Sender<Message>, msg: Message) -> Result<(), ReplayError> {
    tx.send(msg).await.map_err(|_closed| ReplayError::Closed)
}

fn into_op(ev: recording::Event, seq: u64) -> Result<RedisOp, ReplayError> {
    let ts_ns = ts_ns(ev.offset_s).ok_or(ReplayError::InvalidOffset {
        seq,
        offset_s: ev.offset_s,
    })?;
    Ok(RedisOp {
        at: At { seq, ts_ns },
        db: db(&ev.db),
        cmd: Arc::from(ev.cmd),
        key: (!ev.key.is_empty()).then(|| Arc::from(ev.key)),
        args: ev.args.into_iter().map(Arc::from).collect(),
        client: Arc::from(ev.client),
    })
}

fn db(name: &str) -> Db {
    match name {
        "CONFIG_DB" => Db::Config,
        "APPL_DB" => Db::Appl,
        "ASIC_DB" => Db::Asic,
        _ => Db::Other,
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::instance::PendingReason;
    use crate::test_support::TEST_PORT;
    use config_analyzer_core::config_trace;
    use config_analyzer_core::{SpanEvent, VecSink};
    use rstest::rstest;
    use std::collections::BTreeMap;

    #[rstest]
    #[case::config("CONFIG_DB", Db::Config)]
    #[case::appl("APPL_DB", Db::Appl)]
    #[case::asic("ASIC_DB", Db::Asic)]
    #[case::counters("COUNTERS_DB", Db::Other)]
    #[case::appl_state("APPL_STATE_DB", Db::Other)]
    #[case::empty("", Db::Other)]
    fn test_db(#[case] name: &str, #[case] expected: Db) {
        assert_eq!(db(name), expected);
    }

    fn event(offset_s: f64, db: &str, cmd: &str, key: &str, args: &[&str]) -> recording::Event {
        recording::Event {
            offset_s,
            db: db.to_owned(),
            cmd: cmd.to_owned(),
            key: key.to_owned(),
            args: args.iter().map(|a| (*a).to_owned()).collect(),
            client: "lua".to_owned(),
        }
    }

    fn trace_event(
        offset_s: f64,
        db: &str,
        cmd: &str,
        key: &str,
        args: &[&str],
        client: &str,
    ) -> recording::Event {
        recording::Event {
            offset_s,
            db: db.to_owned(),
            cmd: cmd.to_owned(),
            key: key.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            client: client.to_owned(),
        }
    }

    #[rstest]
    #[case::with_key(
        "ASIC_STATE_KEY_VALUE_OP_QUEUE",
        Some(Arc::from("ASIC_STATE_KEY_VALUE_OP_QUEUE"))
    )]
    #[case::empty_key("", None)]
    fn test_into_op(#[case] key: &str, #[case] expected_key: Option<Arc<str>>) {
        let ev = event(
            8.605_097,
            "ASIC_DB",
            "LPUSH",
            key,
            &["SAI_OBJECT_TYPE_PORT:oid:0x1", "Sset"],
        );
        assert_eq!(
            into_op(ev, 7).unwrap(),
            RedisOp {
                at: At {
                    seq: 7,
                    ts_ns: 8_605_097_000,
                },
                db: Db::Asic,
                cmd: Arc::from("LPUSH"),
                key: expected_key,
                args: vec![Arc::from("SAI_OBJECT_TYPE_PORT:oid:0x1"), Arc::from("Sset")],
                client: Arc::from("lua"),
            }
        );
    }

    #[test]
    fn test_into_op_invalid_offset() {
        let ev = event(-1.0, "APPL_DB", "DEL", "k", &[]);
        let err = into_op(ev, 3).unwrap_err();
        assert_eq!(err.to_string(), "event 3: invalid offset_s -1");
    }

    fn recording(events: Vec<recording::Event>) -> Recording {
        Recording {
            port_map: BTreeMap::from([
                (String::new(), String::new()),
                ("oid:0x1".to_owned(), "Ethernet1".to_owned()),
            ]),
            events,
        }
    }

    fn config_write() -> recording::Event {
        event(1.0, "CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"])
    }

    fn at(seq: u64, ts_ns: u64) -> At {
        At { seq, ts_ns }
    }

    fn span(track: &'static str, name: &'static str, start: At, end: At, open: bool) -> SpanEvent {
        SpanEvent {
            pipeline: "port",
            key: Arc::from("Ethernet1"),
            track,
            name,
            start,
            end,
            oid: None,
            attr: None,
            open,
        }
    }

    #[tokio::test]
    async fn test_replay_completed() {
        let sink = VecSink::default();
        let events = vec![
            config_write(),
            event(
                1.000_01,
                "APPL_DB",
                "HSET",
                "_PORT_TABLE:Ethernet1",
                &["mtu", "9100"],
            ),
            event(
                1.000_02,
                "COUNTERS_DB",
                "HSET",
                "COUNTERS:oid:0x1",
                &["x", "1"],
            ),
            event(1.000_03, "APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[]),
        ];
        let pending = replay(&[&TEST_PORT], recording(events), sink.clone())
            .await
            .unwrap();
        assert_eq!(
            (pending, sink.take()),
            (
                vec![],
                vec![
                    span(
                        "config",
                        "Written->Forwarded",
                        at(0, 1_000_000_000),
                        at(1, 1_000_010_000),
                        false
                    ),
                    span(
                        "appl",
                        "Queued->Consumed",
                        at(1, 1_000_010_000),
                        at(3, 1_000_030_000),
                        false
                    ),
                ]
            )
        );
    }

    fn config_trace_recording(field: &str, sai_attr: &str) -> Recording {
        let portmgrd = format!(
            "2026-10-01.03:38:55.000000|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:{field}\n\
             2026-10-01.03:38:55.000010|portmgrd|appl|PORT_TABLE|Ethernet1|-|{field}\n"
        );
        let orchagent = format!(
            "2026-10-01.03:38:55.000020|orchagent|recv|PORT_TABLE|Ethernet1|-|APPL_DB:SET:{field}\n\
             2026-10-01.03:38:55.000030|orchagent|sai_req|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:{sai_attr}\n\
             2026-10-01.03:38:55.000040|orchagent|sai_resp|SAI_OBJECT_TYPE_PORT|oid:0x1|-|set:SAI_STATUS_SUCCESS\n\
             2026-10-01.03:38:55.000050|orchagent|map|SAI_OBJECT_TYPE_PORT|oid:0x1|-|Ethernet1\n"
        );
        let (recording, stats) = config_trace::parse(&[&portmgrd, &orchagent]);
        assert_eq!(
            stats,
            config_trace::Stats {
                lines: 6,
                skipped: 0,
                unpaired: 0,
                unmapped: 0,
            }
        );
        let expected = Recording {
            port_map: BTreeMap::from([("oid:0x1".to_owned(), "Ethernet1".to_owned())]),
            events: vec![
                trace_event(
                    0.0,
                    "CONFIG_DB",
                    "HSET",
                    "PORT|Ethernet1",
                    &[field, ""],
                    "portmgrd",
                ),
                trace_event(
                    0.000_01,
                    "APPL_DB",
                    "HSET",
                    "_PORT_TABLE:Ethernet1",
                    &[field, ""],
                    "portmgrd",
                ),
                trace_event(
                    0.000_02,
                    "APPL_DB",
                    "DEL",
                    "_PORT_TABLE:Ethernet1",
                    &[],
                    "orchagent",
                ),
                trace_event(
                    0.000_03,
                    "ASIC_DB",
                    "LPUSH",
                    "ASIC_STATE_KEY_VALUE_OP_QUEUE",
                    &[
                        "SAI_OBJECT_TYPE_PORT:oid:0x1",
                        &format!("[\"{sai_attr}\",\"\"]"),
                        "Sset",
                    ],
                    "orchagent",
                ),
                trace_event(
                    0.000_04,
                    "ASIC_DB",
                    "HSET",
                    "ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x1",
                    &[sai_attr, ""],
                    "orchagent",
                ),
            ],
        };
        assert_eq!(recording, expected);
        recording
    }

    async fn assert_config_trace_replay(recording: Recording, sai_attr: &str) {
        let sink = VecSink::default();
        let pending = replay(&[&TEST_PORT], recording, sink.clone())
            .await
            .unwrap();
        assert_eq!(
            (pending, sink.take()),
            (
                vec![],
                vec![
                    span(
                        "config",
                        "Written->Forwarded",
                        at(0, 0),
                        at(1, 10_000),
                        false
                    ),
                    span(
                        "appl",
                        "Queued->Consumed",
                        at(1, 10_000),
                        at(2, 20_000),
                        false
                    ),
                    SpanEvent {
                        pipeline: "port",
                        key: Arc::from("Ethernet1"),
                        track: "SAI_OBJECT_TYPE_PORT",
                        name: "Sset",
                        start: at(3, 30_000),
                        end: at(4, 40_000),
                        oid: Some(Arc::from("oid:0x1")),
                        attr: Some(Arc::from(sai_attr)),
                        open: false,
                    },
                ]
            )
        );
    }

    async fn test_config_trace_round_trip(field: &str, sai_attr: &str) {
        let recording = config_trace_recording(field, sai_attr);
        assert_config_trace_replay(recording, sai_attr).await;
    }

    #[tokio::test]
    async fn test_config_trace_mtu_round_trip() {
        test_config_trace_round_trip("mtu", "SAI_PORT_ATTR_MTU").await;
    }

    #[tokio::test]
    async fn test_config_trace_admin_status_down_to_up_round_trip() {
        test_config_trace_round_trip("admin_status", "SAI_PORT_ATTR_ADMIN_STATE").await;
    }

    #[tokio::test]
    async fn test_replay_pending() {
        let sink = VecSink::default();
        let pending = replay(&[&TEST_PORT], recording(vec![config_write()]), sink.clone())
            .await
            .unwrap();
        let pending_item = |reason| Pending {
            pipeline: Some("port"),
            key: Arc::from("Ethernet1"),
            reason,
        };
        let start = at(0, 1_000_000_000);
        assert_eq!(
            (pending, sink.take()),
            (
                vec![
                    pending_item(PendingReason::ConfigNotForwarded),
                    pending_item(PendingReason::Uncovered(1_u64 << 12)),
                ],
                vec![span("config", "Written->Forwarded", start, start, true)]
            )
        );
    }

    #[tokio::test]
    async fn test_replay_invalid_offset() {
        let events = vec![
            config_write(),
            event(f64::INFINITY, "APPL_DB", "DEL", "k", &[]),
        ];
        assert_eq!(
            replay(&[&TEST_PORT], recording(events), VecSink::default()).await,
            Err(ReplayError::InvalidOffset {
                seq: 1,
                offset_s: f64::INFINITY,
            })
        );
    }
}
