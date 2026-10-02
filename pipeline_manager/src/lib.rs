use config_analyzer_core::{At, SpanSink};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::instance::{Pending, PipelineState};
use crate::pipeline::Pipeline;
use crate::router::Router;
use crate::worker::WorkerMsg;

pub mod classify;
pub mod instance;
pub mod pipeline;
pub mod replay;
mod router;
#[cfg(test)]
pub(crate) mod test_support;
mod worker;

// Input channel and each worker channel.
pub const CHANNEL_CAPACITY: usize = 4096;

#[derive(Debug)]
pub enum Message {
    // Event stream: one decoded Redis command.
    Op(RedisOp),
    // Mapping data: "oid:0x1000000000002" -> "Ethernet1" (port_map).
    ObjectMap {
        oid: Arc<str>,
        name: Arc<str>,
    },
    // Consistent pending report.
    Checkpoint {
        reply: oneshot::Sender<Vec<Pending>>,
    },
    // Final report; closes open spans and flushes sinks. The router exits afterwards.
    End {
        at: At,
        reply: oneshot::Sender<Vec<Pending>>,
    },
}

// Must be called inside a tokio runtime. Spawns one worker task per entry of `pipelines`, in
// that order, then the router task. `PipelineIdx` is the index into `pipelines`.
#[must_use]
#[inline]
pub fn spawn<K>(pipelines: &[&'static Pipeline], sink: K) -> mpsc::Sender<Message>
where
    K: SpanSink + Clone + 'static,
{
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let mut workers = Vec::with_capacity(pipelines.len());
    // `sink` goes to the first worker and every other worker gets a clone of it.
    let template = K::clone(&sink);
    if let Some((first, rest)) = pipelines.split_first() {
        workers.push(spawn_worker(first, sink));
        for pipeline in rest {
            workers.push(spawn_worker(pipeline, K::clone(&template)));
        }
    }
    drop(tokio::spawn(
        Router::new(pipelines.to_vec(), workers).run(rx),
    ));
    tx
}

#[inline]
fn spawn_worker<K>(pipeline: &'static Pipeline, sink: K) -> mpsc::Sender<WorkerMsg>
where
    K: SpanSink + 'static,
{
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    drop(tokio::spawn(worker::run(
        PipelineState::new(pipeline, sink),
        rx,
    )));
    tx
}

// CONFIG_DB, APPL_DB, ASIC_DB, anything else
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Db {
    Config,
    Appl,
    Asic,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisOp {
    // seq: assigned by the producer, increasing; ts_ns: Redis op time, ns
    pub at: At,
    pub db: Db,
    // As recorded: "HSET", "HMSET", "DEL", "LPUSH", "EVALSHA", ...
    pub cmd: Arc<str>,
    pub key: Option<Arc<str>>,
    // Arguments after the key
    pub args: Vec<Arc<str>>,
    // Not used by the classifier
    pub client: Arc<str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::PendingReason;
    use crate::pipeline::{NameRule, SPAN_APPL, SPAN_CONFIG, SPAN_SET, TRACK_APPL, TRACK_CONFIG};
    use crate::test_support::{
        TEST_PORT, TEST_PORT_BASE as BASE, TEST_PORT_CONFIG_FIELDS as CONFIG14,
    };
    use config_analyzer_core::{SpanEvent, VecSink};

    const E1: &str = "Ethernet1";
    const P: &str = "SAI_OBJECT_TYPE_PORT";
    const R: &str = "SAI_OBJECT_TYPE_ROUTER_INTERFACE";
    const OP: &str = "oid:0x1000000000002";
    const OR: &str = "oid:0x60000000001fb";
    const O99: &str = "oid:0x1000000000099";
    const FL: &str = "SAI_PORT_ATTR_FAST_LINKUP_ENABLED";
    const PM: &str = "SAI_PORT_ATTR_MTU";
    const RM: &str = "SAI_ROUTER_INTERFACE_ATTR_MTU";
    const MTU: &str = "mtu";
    const ADMIN: &str = "admin_status";
    enum Step {
        Send(Message),
        Checkpoint,
        End(At),
    }

    async fn drive(steps: Vec<Step>) -> (Vec<Vec<Pending>>, Vec<SpanEvent>) {
        drive_pipelines(&[&TEST_PORT], steps).await
    }

    async fn drive_pipelines(
        pipelines: &[&'static Pipeline],
        steps: Vec<Step>,
    ) -> (Vec<Vec<Pending>>, Vec<SpanEvent>) {
        let sink = VecSink::default();
        let tx = spawn(pipelines, sink.clone());
        let mut replies = Vec::new();
        for step in steps {
            let rx = match step {
                Step::Send(msg) => {
                    drop(tx.send(msg).await);
                    continue;
                }
                Step::Checkpoint => {
                    let (reply, rx) = oneshot::channel();
                    drop(tx.send(Message::Checkpoint { reply }).await);
                    rx
                }
                Step::End(at) => {
                    let (reply, rx) = oneshot::channel();
                    drop(tx.send(Message::End { at, reply }).await);
                    rx
                }
            };
            let reply = rx.await;
            assert!(reply.is_ok());
            replies.push(reply.unwrap_or_default());
        }
        (replies, sink.take())
    }

    const fn at(n: u64) -> At {
        At {
            seq: n,
            ts_ns: n * 1_000,
        }
    }

    const fn e1(seq: u64) -> At {
        let ts_ns = match seq {
            1 => 8_593_362_000,
            2..=13 => 8_601_837_000 + (seq - 2) * 40_000,
            14 => 8_603_793_000,
            15 => 8_605_097_000,
            16 => 8_607_229_000,
            17 => 8_609_536_000,
            18 => 8_611_495_000,
            19 => 8_612_121_000,
            20 => 8_614_187_000,
            21 => 8_614_362_000,
            22 => 8_615_124_000,
            23 => 8_615_654_000,
            24 => 8_618_806_000,
            25 => 8_620_000_000,
            _ => 0,
        };
        At { seq, ts_ns }
    }

    fn arcs(items: &[&str]) -> Vec<Arc<str>> {
        items.iter().map(|item| Arc::from(*item)).collect()
    }

    fn op(at: At, db: Db, cmd: &str, key: &str, args: &[&str]) -> Step {
        Step::Send(Message::Op(RedisOp {
            at,
            db,
            cmd: Arc::from(cmd),
            key: Some(Arc::from(key)),
            args: arcs(args),
            client: Arc::from("test"),
        }))
    }

    fn with_values(fields: &[&str]) -> Vec<String> {
        fields
            .iter()
            .flat_map(|field| [(*field).to_owned(), "v".to_owned()])
            .collect()
    }

    fn map(oid: &str, name: &str) -> Step {
        Step::Send(Message::ObjectMap {
            oid: Arc::from(oid),
            name: Arc::from(name),
        })
    }

    fn cfg(at: At, fields: &[&str]) -> Step {
        let args = with_values(fields);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        op(at, Db::Config, "HSET", "PORT|Ethernet1", &args)
    }

    fn appl(at: At, fields: &[&str]) -> Step {
        let args = with_values(fields);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        op(at, Db::Appl, "HSET", "_PORT_TABLE:Ethernet1", &args)
    }

    fn del(at: At) -> Step {
        op(at, Db::Appl, "DEL", "_PORT_TABLE:Ethernet1", &[])
    }

    fn req(at: At, obj_type: &str, oid: &str, attr: &str) -> Step {
        let target = format!("{obj_type}:{oid}");
        let attrs = format!("[\"{attr}\",\"v\"]");
        op(
            at,
            Db::Asic,
            "LPUSH",
            "ASIC_STATE_KEY_VALUE_OP_QUEUE",
            &[&target, &attrs, "Sset"],
        )
    }

    fn wr(at: At, obj_type: &str, oid: &str) -> Step {
        let key = format!("ASIC_STATE:{obj_type}:{oid}");
        op(at, Db::Asic, "HSET", &key, &["a", "v"])
    }

    fn span(
        track: &'static str,
        name: &'static str,
        start: At,
        end: At,
        oid: Option<&str>,
        attr: Option<&str>,
    ) -> SpanEvent {
        SpanEvent {
            oid: oid.map(Arc::from),
            attr: attr.map(Arc::from),
            ..span_of("port", E1, track, name, start, end)
        }
    }

    fn span_of(
        pipeline: &'static str,
        key: &str,
        track: &'static str,
        name: &'static str,
        start: At,
        end: At,
    ) -> SpanEvent {
        SpanEvent {
            pipeline,
            key: Arc::from(key),
            track,
            name,
            start,
            end,
            oid: None,
            attr: None,
            open: false,
        }
    }

    fn asic_span_of(
        pipeline: &'static str,
        key: &str,
        track: &'static str,
        start: At,
        end: At,
        oid: &str,
        attr: &str,
    ) -> SpanEvent {
        SpanEvent {
            oid: Some(Arc::from(oid)),
            attr: Some(Arc::from(attr)),
            ..span_of(pipeline, key, track, SPAN_SET, start, end)
        }
    }

    // Every worker pushes into the same sink, so the order between pipelines is not fixed.
    fn sorted(mut spans: Vec<SpanEvent>) -> Vec<SpanEvent> {
        spans.sort_by_key(|ev| {
            (
                ev.start.seq,
                ev.end.seq,
                ev.pipeline,
                ev.track,
                ev.name,
                Arc::clone(&ev.key),
            )
        });
        spans
    }

    fn config_span(start: At, end: At) -> SpanEvent {
        span(TRACK_CONFIG, SPAN_CONFIG, start, end, None, None)
    }

    fn appl_span(start: At, end: At) -> SpanEvent {
        span(TRACK_APPL, SPAN_APPL, start, end, None, None)
    }

    fn set_span(track: &'static str, start: At, end: At, oid: &str, attr: &str) -> SpanEvent {
        span(track, SPAN_SET, start, end, Some(oid), Some(attr))
    }

    fn open(ev: SpanEvent) -> SpanEvent {
        SpanEvent { open: true, ..ev }
    }

    fn pending(reason: PendingReason) -> Pending {
        Pending {
            pipeline: Some("port"),
            key: Arc::from(E1),
            reason,
        }
    }

    fn pending_of(pipeline: &'static str, key: &str, reason: PendingReason) -> Pending {
        Pending {
            pipeline: Some(pipeline),
            key: Arc::from(key),
            reason,
        }
    }

    fn orphan(oid: &str, ops: usize) -> Pending {
        Pending {
            pipeline: None,
            key: Arc::from(oid),
            reason: PendingReason::OrphanOid { ops },
        }
    }

    fn in_flight(slot: usize, sets: usize) -> PendingReason {
        PendingReason::AsicInFlight {
            slot,
            sets,
            create: false,
            remove: false,
        }
    }

    // (seq, step); the two `map`s carry seq 0.
    fn ethernet1_steps() -> Vec<(u64, Step)> {
        let mut steps = vec![
            (0, map(OP, E1)),
            (0, map(OR, E1)),
            (1, cfg(e1(1), &CONFIG14)),
        ];
        steps.extend(
            BASE.iter()
                .zip(2_u64..)
                .map(|(field, seq)| (seq, appl(e1(seq), &[*field]))),
        );
        steps.extend([
            (14, del(e1(14))),
            (15, req(e1(15), P, OP, FL)),
            (16, wr(e1(16), P, OP)),
            (17, appl(e1(17), &[MTU])),
            (18, del(e1(18))),
            (19, req(e1(19), P, OP, PM)),
            (20, wr(e1(20), P, OP)),
            (21, req(e1(21), R, OR, RM)),
            (22, appl(e1(22), &[ADMIN])),
            (23, wr(e1(23), R, OR)),
            (24, del(e1(24))),
        ]);
        steps
    }

    fn ethernet1_spans() -> Vec<SpanEvent> {
        vec![
            config_span(e1(1), e1(2)),
            appl_span(e1(2), e1(14)),
            set_span(P, e1(15), e1(16), OP, FL),
            appl_span(e1(17), e1(18)),
            set_span(P, e1(19), e1(20), OP, PM),
            set_span(R, e1(21), e1(23), OR, RM),
            appl_span(e1(22), e1(24)),
        ]
    }

    fn noise() -> Vec<Step> {
        let zero = At::default();
        vec![
            op(zero, Db::Appl, "EVALSHA", "c7faa1a7", &["1"]),
            op(zero, Db::Appl, "SADD", "PORT_TABLE_KEY_SET", &["Ethernet1"]),
            op(zero, Db::Appl, "SPOP", "PORT_TABLE_KEY_SET", &["1024"]),
            op(
                zero,
                Db::Appl,
                "HSET",
                "PORT_TABLE:Ethernet1",
                &["mtu", "9112"],
            ),
            op(zero, Db::Appl, "PUBLISH", "PORT_TABLE_CHANNEL@0", &["G"]),
            op(
                zero,
                Db::Asic,
                "LPUSH",
                "GETRESPONSE_KEY_VALUE_OP_QUEUE",
                &["SAI_STATUS_SUCCESS", "[]", "Sgetresponse"],
            ),
            op(zero, Db::Config, "SET", "CONFIG_DB_UPDATED_PORT", &["1"]),
            op(
                zero,
                Db::Other,
                "HSET",
                "_PORT_TABLE:Ethernet1",
                &["mtu", "1"],
            ),
        ]
    }

    #[tokio::test]
    async fn test_ethernet1_scenario() {
        let mut steps: Vec<Step> = ethernet1_steps()
            .into_iter()
            .map(|(_, step)| step)
            .collect();
        steps.push(Step::End(e1(25)));
        assert_eq!(drive(steps).await, (vec![vec![]], ethernet1_spans()));
    }

    #[tokio::test]
    async fn test_ethernet1_checkpoint_after_seq_14() {
        let mut steps = Vec::new();
        for (seq, step) in ethernet1_steps() {
            steps.push(step);
            if seq == 14 {
                steps.push(Step::Checkpoint);
            }
        }
        steps.push(Step::End(e1(25)));
        let expected = vec![
            vec![pending(PendingReason::Uncovered(
                (1_u64 << 12) | (1_u64 << 13),
            ))],
            vec![],
        ];
        assert_eq!(drive(steps).await, (expected, ethernet1_spans()));
    }

    #[tokio::test]
    async fn test_ethernet1_checkpoint_after_seq_22() {
        let mut steps = Vec::new();
        for (seq, step) in ethernet1_steps() {
            steps.push(step);
            if seq == 22 {
                steps.push(Step::Checkpoint);
            }
        }
        steps.push(Step::End(e1(25)));
        let expected = vec![
            vec![pending(PendingReason::ApplQueued), pending(in_flight(1, 1))],
            vec![],
        ];
        assert_eq!(drive(steps).await, (expected, ethernet1_spans()));
    }

    #[tokio::test]
    async fn test_ethernet1_truncated() {
        let end = At {
            seq: 23,
            ts_ns: 8_615_200_000,
        };
        let mut steps: Vec<Step> = ethernet1_steps()
            .into_iter()
            .filter(|(seq, _)| *seq <= 22)
            .map(|(_, step)| step)
            .collect();
        steps.push(Step::End(end));
        let mut spans: Vec<SpanEvent> = ethernet1_spans().into_iter().take(5).collect();
        spans.extend([
            open(appl_span(e1(22), end)),
            open(set_span(R, e1(21), end, OR, RM)),
        ]);
        let expected = vec![vec![
            pending(PendingReason::ApplQueued),
            pending(in_flight(1, 1)),
        ]];
        assert_eq!(drive(steps).await, (expected, spans));
    }

    #[tokio::test]
    async fn test_ethernet1_noise() {
        let mut steps = Vec::new();
        for (seq, step) in ethernet1_steps() {
            steps.push(step);
            if seq == 1 || seq == 14 {
                steps.extend(noise());
            }
        }
        steps.push(Step::End(e1(25)));
        assert_eq!(drive(steps).await, (vec![vec![]], ethernet1_spans()));
    }

    fn same_mtu_prefix() -> Vec<Step> {
        vec![
            map(OP, E1),
            map(OR, E1),
            cfg(at(1), &[MTU]),
            appl(at(2), &BASE),
            del(at(3)),
            req(at(4), P, OP, FL),
            wr(at(5), P, OP),
            appl(at(6), &[MTU]),
            del(at(7)),
        ]
    }

    #[tokio::test]
    async fn test_same_mtu() {
        let mut steps = same_mtu_prefix();
        steps.extend([appl(at(8), &[ADMIN]), del(at(9)), Step::End(at(10))]);
        let spans = vec![
            config_span(at(1), at(2)),
            appl_span(at(2), at(3)),
            set_span(P, at(4), at(5), OP, FL),
            appl_span(at(6), at(7)),
            appl_span(at(8), at(9)),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], spans));
    }

    #[tokio::test]
    async fn test_mtu_change() {
        let mut steps = same_mtu_prefix();
        steps.extend([
            req(at(8), P, OP, PM),
            wr(at(9), P, OP),
            req(at(10), R, OR, RM),
            wr(at(11), R, OR),
            appl(at(12), &[ADMIN]),
            del(at(13)),
            Step::End(at(14)),
        ]);
        let spans = vec![
            config_span(at(1), at(2)),
            appl_span(at(2), at(3)),
            set_span(P, at(4), at(5), OP, FL),
            appl_span(at(6), at(7)),
            set_span(P, at(8), at(9), OP, PM),
            set_span(R, at(10), at(11), OR, RM),
            appl_span(at(12), at(13)),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], spans));
    }

    #[tokio::test]
    async fn test_asic_write_lags_next_request() {
        let steps = vec![
            map(OP, E1),
            map(OR, E1),
            req(at(1), P, OP, PM),
            req(at(2), R, OR, RM),
            wr(at(3), P, OP),
            wr(at(4), R, OR),
            Step::End(at(5)),
        ];
        let spans = vec![
            set_span(P, at(1), at(3), OP, PM),
            set_span(R, at(2), at(4), OR, RM),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], spans));
    }

    #[tokio::test]
    async fn test_config_rewrite_while_asic_in_flight() {
        let steps = vec![
            map(OP, E1),
            cfg(at(1), &[MTU]),
            appl(at(2), &[MTU]),
            del(at(3)),
            req(at(4), P, OP, PM),
            cfg(at(5), &[MTU]),
            Step::Checkpoint,
            wr(at(6), P, OP),
            appl(at(7), &[MTU]),
            del(at(8)),
            Step::End(at(9)),
        ];
        let replies = vec![
            vec![
                pending(PendingReason::ConfigNotForwarded),
                pending(PendingReason::Uncovered(1_u64 << 12)),
                pending(in_flight(0, 1)),
            ],
            vec![],
        ];
        let spans = vec![
            config_span(at(1), at(2)),
            appl_span(at(2), at(3)),
            set_span(P, at(4), at(6), OP, PM),
            config_span(at(5), at(7)),
            appl_span(at(7), at(8)),
        ];
        assert_eq!(drive(steps).await, (replies, spans));
    }

    #[tokio::test]
    async fn test_two_config_writes_before_forwarding() {
        let steps = vec![
            cfg(at(1), &[MTU]),
            cfg(at(2), &[ADMIN]),
            Step::Checkpoint,
            appl(at(3), &[MTU, ADMIN]),
            del(at(4)),
            Step::End(at(5)),
        ];
        let replies = vec![
            vec![
                pending(PendingReason::ConfigNotForwarded),
                pending(PendingReason::Uncovered((1_u64 << 12) | (1_u64 << 13))),
            ],
            vec![],
        ];
        let spans = vec![config_span(at(1), at(3)), appl_span(at(3), at(4))];
        assert_eq!(drive(steps).await, (replies, spans));
    }

    #[tokio::test]
    async fn test_orphan_oid_mapped_late() {
        let steps = vec![
            req(at(1), P, OP, PM),
            wr(at(2), P, OP),
            Step::Checkpoint,
            map(OP, E1),
            Step::End(at(3)),
        ];
        assert_eq!(
            drive(steps).await,
            (
                vec![vec![orphan(OP, 2)], vec![]],
                vec![set_span(P, at(1), at(2), OP, PM)]
            )
        );
    }

    #[tokio::test]
    async fn test_orphan_oid_never_mapped() {
        let steps = vec![req(at(1), P, OP, PM), wr(at(2), P, OP), Step::End(at(3))];
        assert_eq!(drive(steps).await, (vec![vec![orphan(OP, 2)]], vec![]));
    }

    #[tokio::test]
    async fn test_object_type_in_no_asic_slots() {
        let steps = vec![
            map("oid:0x15000000000028", "Ethernet9:7"),
            op(
                at(1),
                Db::Asic,
                "LPUSH",
                "ASIC_STATE_KEY_VALUE_OP_QUEUE",
                &[
                    "SAI_OBJECT_TYPE_QUEUE:oid:0x15000000000028",
                    "[\"SAI_QUEUE_ATTR_TYPE\",\"v\"]",
                    "Sset",
                ],
            ),
            Step::End(at(2)),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], vec![]));
    }

    #[tokio::test]
    async fn test_tracked_type_name_owned_by_no_pipeline() {
        let steps = vec![
            map("oid:0x60000000009999", "Vlan100"),
            req(at(1), R, "oid:0x60000000009999", RM),
            Step::End(at(2)),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], vec![]));
    }

    #[tokio::test]
    async fn test_asic_hset_with_nothing_open() {
        let steps = vec![map(OP, E1), wr(at(1), P, OP), Step::End(at(2))];
        assert_eq!(drive(steps).await, (vec![vec![]], vec![]));
    }

    #[tokio::test]
    async fn test_consumed_without_queued() {
        let steps = vec![del(at(1)), Step::End(at(2))];
        assert_eq!(drive(steps).await, (vec![vec![]], vec![]));
    }

    #[tokio::test]
    async fn test_unknown_table() {
        let steps = vec![
            op(
                at(1),
                Db::Config,
                "HSET",
                "VLAN|Vlan100",
                &["admin_status", "up"],
            ),
            Step::End(at(2)),
        ];
        assert_eq!(drive(steps).await, (vec![vec![]], vec![]));
    }

    #[tokio::test]
    async fn test_oid_remapped_to_another_name() {
        let steps = vec![
            map(OP, E1),
            req(at(1), P, OP, PM),
            map(OP, "Ethernet5"),
            wr(at(2), P, OP),
            Step::End(at(3)),
        ];
        assert_eq!(
            drive(steps).await,
            (vec![vec![]], vec![set_span(P, at(1), at(2), OP, PM)])
        );
    }

    #[tokio::test]
    async fn test_slot_remapped_to_new_oid() {
        let steps = vec![
            map(OP, E1),
            map(O99, E1),
            req(at(1), P, OP, PM),
            req(at(2), P, O99, PM),
            wr(at(3), P, O99),
            Step::End(at(4)),
        ];
        assert_eq!(
            drive(steps).await,
            (
                vec![vec![pending(in_flight(0, 1))]],
                vec![
                    set_span(P, at(1), at(3), O99, PM),
                    open(set_span(P, at(2), at(4), O99, PM)),
                ]
            )
        );
    }

    // One ASIC slot, and a name rule that does not match ports.
    static LAG: Pipeline = Pipeline {
        name: "lag",
        config_table: Some("PORTCHANNEL"),
        appl_table: "LAG_TABLE",
        fields: &["mtu"],
        field_alias: &[],
        asic_slots: &["SAI_OBJECT_TYPE_LAG"],
        object_names: NameRule {
            prefix: "PortChannel",
            digits: true,
        },
    };

    // No ASIC slots; the name rule is free-form.
    static VLAN_MEMBER: Pipeline = Pipeline {
        name: "vlan_member",
        config_table: Some("VLAN_MEMBER"),
        appl_table: "VLAN_MEMBER_TABLE",
        fields: &["tagging_mode"],
        field_alias: &[],
        asic_slots: &[],
        object_names: NameRule {
            prefix: "Vlan",
            digits: false,
        },
    };

    const PC1: &str = "PortChannel1";
    const LAG_OID: &str = "oid:0x3000000000001";
    const LAG_ATTR: &str = "SAI_LAG_ATTR_PORT_VLAN_ID";
    const VM_KEY: &str = "Vlan100:Ethernet0";

    fn multiple_pipelines_steps() -> Vec<Step> {
        vec![
            map(LAG_OID, PC1),
            op(
                at(1),
                Db::Config,
                "HSET",
                "PORTCHANNEL|PortChannel1",
                &["mtu", "9100"],
            ),
            op(
                at(2),
                Db::Appl,
                "HSET",
                "_LAG_TABLE:PortChannel1",
                &["mtu", "9100"],
            ),
            req(at(3), "SAI_OBJECT_TYPE_LAG", LAG_OID, LAG_ATTR),
            wr(at(4), "SAI_OBJECT_TYPE_LAG", LAG_OID),
            op(at(5), Db::Appl, "DEL", "_LAG_TABLE:PortChannel1", &[]),
            op(
                at(6),
                Db::Config,
                "HSET",
                "VLAN_MEMBER|Vlan100|Ethernet0",
                &["tagging_mode", "untagged"],
            ),
            // The CONFIG write alone: attributed to `vlan_member` under the normalized key, with
            // its only field not yet seen in APPL.
            Step::Checkpoint,
            op(
                at(7),
                Db::Appl,
                "HSET",
                "_VLAN_MEMBER_TABLE:Vlan100:Ethernet0",
                &["tagging_mode", "untagged"],
            ),
            op(
                at(8),
                Db::Appl,
                "DEL",
                "_VLAN_MEMBER_TABLE:Vlan100:Ethernet0",
                &[],
            ),
            map(OP, E1),
            map(OR, E1),
            req(at(9), R, OR, RM),
            wr(at(10), R, OR),
            Step::End(at(11)),
        ]
    }

    // Routing across three workers, CONFIG `|` -> `:` normalization for a two-part key, and one
    // slot for `lag` against two for `port`.
    #[tokio::test]
    async fn test_multiple_pipelines() {
        let (replies, spans) = drive_pipelines(
            &[&TEST_PORT, &LAG, &VLAN_MEMBER],
            multiple_pipelines_steps(),
        )
        .await;
        assert_eq!(
            replies,
            vec![
                vec![
                    pending_of("vlan_member", VM_KEY, PendingReason::ConfigNotForwarded),
                    pending_of("vlan_member", VM_KEY, PendingReason::Uncovered(1)),
                ],
                vec![],
            ]
        );
        assert_eq!(
            sorted(spans),
            sorted(vec![
                span_of("lag", PC1, TRACK_CONFIG, SPAN_CONFIG, at(1), at(2)),
                span_of("lag", PC1, TRACK_APPL, SPAN_APPL, at(2), at(5)),
                asic_span_of(
                    "lag",
                    PC1,
                    "SAI_OBJECT_TYPE_LAG",
                    at(3),
                    at(4),
                    LAG_OID,
                    LAG_ATTR
                ),
                span_of(
                    "vlan_member",
                    VM_KEY,
                    TRACK_CONFIG,
                    SPAN_CONFIG,
                    at(6),
                    at(7)
                ),
                span_of("vlan_member", VM_KEY, TRACK_APPL, SPAN_APPL, at(7), at(8)),
                set_span(R, at(9), at(10), OR, RM),
            ])
        );
    }
}
