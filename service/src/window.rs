use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use config_analyzer_core::At;
use config_analyzer_core::recording::{Event as RecordingEvent, Recording};
use config_analyzer_perfetto::spawn_writer;
use config_analyzer_pipeline_manager::instance::Pending;
use config_analyzer_pipeline_manager::pipeline::Pipeline;
use config_analyzer_pipeline_manager::{Db, Message, RedisOp, spawn};
use tokio::sync::oneshot;

use crate::db_layout::Layout;
use crate::monitor::{MonitorEvent, MonitorMessage};
use crate::redis;
use crate::store::{TraceEntry, TraceStatus, TraceStore};

const STAGING_LIMIT: usize = 128;
const MAX_WINDOW_EVENTS: usize = 250_000;
const WAIT_WHEN_IDLE: Duration = Duration::from_millis(250);
const ASIC_QUEUE_KEY: &str = "ASIC_STATE_KEY_VALUE_OP_QUEUE";

#[derive(Clone, Copy)]
pub(crate) struct WindowConfig {
    pub(crate) settle: Duration,
    pub(crate) timeout: Duration,
    pub(crate) cap: Duration,
}

#[derive(Clone)]
struct CapturedEvent {
    arrival: u64,
    event: MonitorEvent,
}

struct Capture {
    id: u64,
    opened: Instant,
    opened_at_ms: u64,
    last_trigger: Instant,
    last_activity: Instant,
    dirty: bool,
    next_arrival: u64,
    events: Vec<CapturedEvent>,
    keys: BTreeSet<String>,
    oid_map: std::collections::BTreeMap<String, String>,
    pending: Vec<String>,
}

impl Capture {
    fn new(
        id: u64,
        trigger: MonitorEvent,
        oid_map: std::collections::BTreeMap<String, String>,
        pipelines: &[&Pipeline],
    ) -> Self {
        let now = Instant::now();
        let mut capture = Self {
            id,
            opened: now,
            opened_at_ms: unix_ms(),
            last_trigger: now,
            last_activity: now,
            dirty: false,
            next_arrival: 0,
            events: Vec::new(),
            keys: BTreeSet::new(),
            oid_map,
            pending: Vec::new(),
        };
        capture.add(trigger, pipelines);
        capture
    }

    fn add(&mut self, event: MonitorEvent, pipelines: &[&Pipeline]) {
        let now = Instant::now();
        self.last_activity = now;
        if event.database == "CONFIG_DB" {
            self.last_trigger = now;
        }
        if let Some(key) = tracked_key(&event, pipelines) {
            let _ = self.keys.insert(key.to_owned());
        }
        let arrival = self.next_arrival;
        self.next_arrival = self.next_arrival.saturating_add(1);
        self.events.push(CapturedEvent { arrival, event });
        self.dirty = true;
    }

    fn summary(&self, status: TraceStatus) -> TraceEntry {
        TraceEntry {
            id: self.id,
            opened_at_ms: self.opened_at_ms,
            closed_at_ms: None,
            keys: self.keys.iter().cloned().collect(),
            status,
            pending: self.pending.clone(),
            trace: None,
            recording: None,
        }
    }

    fn sorted_events(&self) -> Vec<CapturedEvent> {
        let mut events = self.events.clone();
        events.sort_by_key(|event| (event.event.timestamp_ns, event.arrival));
        events
    }

    fn normalized_recording(&self) -> Recording {
        let events = self.sorted_events();
        let base_ns = events
            .first()
            .map_or(0, |captured| captured.event.timestamp_ns);
        let events = events
            .into_iter()
            .filter_map(|captured| {
                let command = captured.event.args.first()?;
                let key = captured.event.args.get(1)?;
                Some(RecordingEvent {
                    offset_s: Duration::from_nanos(
                        captured.event.timestamp_ns.saturating_sub(base_ns),
                    )
                    .as_secs_f64(),
                    db: captured.event.database,
                    cmd: command.to_ascii_uppercase(),
                    key: key.clone(),
                    args: captured.event.args.into_iter().skip(2).collect(),
                    client: captured.event.client,
                })
            })
            .collect();
        Recording {
            port_map: self.oid_map.clone(),
            events,
        }
    }
}

pub(crate) fn is_trigger(event: &MonitorEvent, pipelines: &[&Pipeline]) -> bool {
    if event.database != "CONFIG_DB" || !is_hash_write(event) {
        return false;
    }
    tracked_key(event, pipelines).is_some()
}

pub(crate) fn should_capture(event: &MonitorEvent, active: bool, pipelines: &[&Pipeline]) -> bool {
    if is_trigger(event, pipelines) {
        return true;
    }
    if !active {
        return false;
    }
    match event.database.as_str() {
        "CONFIG_DB" => is_trigger(event, pipelines),
        "APPL_DB" => {
            tracked_key(event, pipelines).is_some() && (is_hash_write(event) || is_delete(event))
        }
        "ASIC_DB" => {
            if event
                .args
                .first()
                .is_some_and(|command| command.eq_ignore_ascii_case("LPUSH"))
            {
                event.args.get(1).is_some_and(|key| key == ASIC_QUEUE_KEY)
                    && event
                        .args
                        .get(2)
                        .is_some_and(|target| is_tracked_sai_target(target, pipelines))
            } else {
                let key = event.args.get(1).map(String::as_str).unwrap_or_default();
                is_tracked_asic_state_key(key, pipelines)
                    && (is_hash_write(event) || is_delete(event))
            }
        }
        _ => false,
    }
}

fn is_hash_write(event: &MonitorEvent) -> bool {
    event.args.first().is_some_and(|command| {
        command.eq_ignore_ascii_case("HSET") || command.eq_ignore_ascii_case("HMSET")
    })
}

fn is_delete(event: &MonitorEvent) -> bool {
    event
        .args
        .first()
        .is_some_and(|command| command.eq_ignore_ascii_case("DEL"))
}

fn is_tracked_sai_target(target: &str, pipelines: &[&Pipeline]) -> bool {
    pipelines.iter().any(|pipeline| {
        pipeline.asic_slots.iter().any(|slot| {
            target
                .strip_prefix(slot)
                .is_some_and(|rest| rest.starts_with(':'))
        })
    })
}

fn is_tracked_asic_state_key(key: &str, pipelines: &[&Pipeline]) -> bool {
    key.strip_prefix("ASIC_STATE:")
        .is_some_and(|target| is_tracked_sai_target(target, pipelines))
}

fn tracked_key<'a>(event: &'a MonitorEvent, pipelines: &[&Pipeline]) -> Option<&'a str> {
    let key = event.args.get(1)?.as_str();
    pipelines.iter().find_map(|pipeline| {
        let object = match event.database.as_str() {
            "CONFIG_DB" => {
                let table = pipeline.config_table?;
                key.strip_prefix(table)?.strip_prefix('|')
            }
            "APPL_DB" => key
                .strip_prefix('_')?
                .strip_prefix(pipeline.appl_table)?
                .strip_prefix(':'),
            _ => None,
        }?;
        (!object.is_empty() && pipeline.object_names.matches(object)).then_some(object)
    })
}

pub(crate) fn run(
    layout: &Layout,
    store: &Mutex<TraceStore>,
    initial_oid_map: &std::collections::BTreeMap<String, String>,
    pipelines: &[&'static Pipeline],
    receiver: &Receiver<MonitorMessage>,
    active: &AtomicBool,
    config: WindowConfig,
) {
    let mut capture: Option<Capture> = None;
    let mut staged = Vec::new();
    let mut next_staged_arrival = 0_u64;
    let mut next_id = 1_u64;
    loop {
        let wait = capture.as_ref().map_or(WAIT_WHEN_IDLE, |current| {
            next_wait(current, config.settle, config.timeout, config.cap)
        });
        match receiver.recv_timeout(wait) {
            Ok(MonitorMessage::Event(event)) => {
                let is_tracked_trigger = is_trigger(&event, pipelines);
                let trigger_time = event.timestamp_ns;
                if capture.is_some() {
                    let overflow = if let Some(current) = capture.as_mut() {
                        if should_capture(&event, true, pipelines) {
                            current.add(event, pipelines);
                            update_active(store, current);
                            current.events.len() >= MAX_WINDOW_EVENTS
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if overflow {
                        finish_capture(
                            capture.take(),
                            TraceStatus::Capped,
                            store,
                            active,
                            pipelines,
                        );
                    }
                } else if is_tracked_trigger {
                    let oid_map = match redis::read_oid_map(layout) {
                        Ok(map) => map,
                        Err(error) => {
                            store_lock(store)
                                .add_warning(format!("OID map unavailable: {error:#}"));
                            initial_oid_map.clone()
                        }
                    };
                    let mut current = Capture::new(next_id, event, oid_map, pipelines);
                    next_id = next_id.saturating_add(1);
                    staged.sort_by_key(|item: &CapturedEvent| {
                        (item.event.timestamp_ns, item.arrival)
                    });
                    for item in std::mem::take(&mut staged) {
                        if item.event.timestamp_ns >= trigger_time
                            && should_capture(&item.event, true, pipelines)
                        {
                            current.add(item.event, pipelines);
                        }
                    }
                    active.store(true, Ordering::Release);
                    update_active(store, &current);
                    capture = Some(current);
                } else if active.load(Ordering::Acquire) {
                    stage(&mut staged, event, &mut next_staged_arrival);
                }
            }
            Ok(MonitorMessage::SourceState { name, connected }) => {
                store_lock(store).set_source(name, connected);
            }
            Err(RecvTimeoutError::Timeout) => {
                staged.retain(|item: &CapturedEvent| {
                    item.event.timestamp_ns.saturating_add(2_000_000_000) >= unix_ns()
                });
                handle_timer(
                    &mut capture,
                    config.settle,
                    config.timeout,
                    config.cap,
                    store,
                    active,
                    pipelines,
                );
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn stage(staged: &mut Vec<CapturedEvent>, event: MonitorEvent, next_arrival: &mut u64) {
    if staged.len() == STAGING_LIMIT {
        drop(staged.remove(0));
    }
    let arrival = *next_arrival;
    *next_arrival = next_arrival.saturating_add(1);
    staged.push(CapturedEvent { arrival, event });
}

fn next_wait(capture: &Capture, settle: Duration, timeout: Duration, cap: Duration) -> Duration {
    let now = Instant::now();
    let quiet_deadline = if capture.dirty {
        capture.last_activity + settle
    } else {
        capture.opened + cap
    };
    let deadline = [
        capture.opened + cap,
        capture.last_trigger + timeout,
        quiet_deadline,
    ]
    .into_iter()
    .min()
    .unwrap_or(now);
    deadline.saturating_duration_since(now)
}

fn handle_timer(
    capture_slot: &mut Option<Capture>,
    settle: Duration,
    timeout: Duration,
    cap: Duration,
    store: &Mutex<TraceStore>,
    active: &AtomicBool,
    pipelines: &[&'static Pipeline],
) {
    let now = Instant::now();
    let mut completed = None;
    let status = {
        let Some(current) = capture_slot.as_mut() else {
            return;
        };
        if now.duration_since(current.opened) >= cap {
            Some(TraceStatus::Capped)
        } else if now.duration_since(current.last_trigger) >= timeout {
            Some(TraceStatus::Stuck)
        } else if current.dirty && now.duration_since(current.last_activity) >= settle {
            match analyze(current, false, pipelines) {
                Ok((pending, Some(trace))) if pending.is_empty() => {
                    current.pending.clear();
                    current.dirty = false;
                    let mut entry = current.summary(TraceStatus::Complete);
                    entry.closed_at_ms = Some(unix_ms());
                    entry.trace = Some(trace);
                    entry.recording = Some(current.normalized_recording());
                    completed = Some(entry);
                    None
                }
                Ok((pending, _trace)) => {
                    current.pending = pending_strings(&pending);
                    current.dirty = false;
                    update_active(store, current);
                    None
                }
                Err(error) => {
                    current.pending = vec![format!("analyzer error: {error:#}")];
                    current.dirty = false;
                    update_active(store, current);
                    None
                }
            }
        } else {
            None
        }
    };
    if let Some(entry) = completed {
        store_lock(store).finish(entry);
        *capture_slot = None;
        active.store(false, Ordering::Release);
        return;
    }
    if let Some(status) = status {
        finish_capture(capture_slot.take(), status, store, active, pipelines);
    }
}

fn finish_capture(
    capture: Option<Capture>,
    status: TraceStatus,
    store: &Mutex<TraceStore>,
    active: &AtomicBool,
    pipelines: &[&'static Pipeline],
) {
    let Some(mut capture) = capture else {
        active.store(false, Ordering::Release);
        return;
    };
    let recording = capture.normalized_recording();
    match analyze(&capture, true, pipelines) {
        Ok((pending, Some(trace))) => {
            capture.pending = pending_strings(&pending);
            let mut entry = capture.summary(status);
            entry.closed_at_ms = Some(unix_ms());
            entry.trace = Some(trace);
            entry.recording = Some(recording);
            store_lock(store).finish(entry);
        }
        Ok((pending, None)) => {
            capture.pending = pending_strings(&pending);
            let mut entry = capture.summary(status);
            entry.closed_at_ms = Some(unix_ms());
            entry.recording = Some(recording);
            store_lock(store).finish(entry);
        }
        Err(error) => {
            capture.pending = vec![format!("analyzer error: {error:#}")];
            let mut entry = capture.summary(status);
            entry.closed_at_ms = Some(unix_ms());
            entry.recording = Some(recording);
            store_lock(store).finish(entry);
        }
    }
    active.store(false, Ordering::Release);
}

fn analyze(
    capture: &Capture,
    finalizing: bool,
    pipelines: &[&'static Pipeline],
) -> Result<(Vec<Pending>, Option<Vec<u8>>)> {
    let (sink, writer) = spawn_writer(Vec::new()).context("starting Perfetto writer")?;
    let runtime = tokio::runtime::Runtime::new().context("starting analyzer runtime")?;
    let result = runtime.block_on(async {
        let tx = spawn(pipelines, sink.clone());
        for (oid, name) in &capture.oid_map {
            tx.send(Message::ObjectMap {
                oid: Arc::from(oid.as_str()),
                name: Arc::from(name.as_str()),
            })
            .await
            .context("sending OID mapping")?;
        }
        let events = capture.sorted_events();
        let base_ns = events.first().map_or(0, |event| event.event.timestamp_ns);
        let mut end = At::default();
        for (index, captured) in events.iter().enumerate() {
            let sequence = u64::try_from(index).context("too many events in capture window")?;
            let at = At {
                seq: sequence,
                ts_ns: captured.event.timestamp_ns.saturating_sub(base_ns),
            };
            let Some(command) = captured.event.args.first() else {
                continue;
            };
            let key = captured.event.args.get(1);
            let args = captured
                .event
                .args
                .iter()
                .skip(2)
                .map(|argument| Arc::from(argument.as_str()))
                .collect();
            let op = RedisOp {
                at,
                db: match captured.event.database.as_str() {
                    "CONFIG_DB" => Db::Config,
                    "APPL_DB" => Db::Appl,
                    "ASIC_DB" => Db::Asic,
                    _ => Db::Other,
                },
                cmd: Arc::from(command.to_ascii_uppercase()),
                key: key.map(|key| Arc::from(key.as_str())),
                args,
                client: Arc::from(captured.event.client.as_str()),
            };
            end = op.at;
            tx.send(Message::Op(op))
                .await
                .context("sending analyzer event")?;
        }
        if finalizing {
            end_analyzer(&tx, end).await
        } else {
            let pending = checkpoint_analyzer(&tx).await?;
            if pending.is_empty() {
                end_analyzer(&tx, end).await
            } else {
                Ok((pending, false))
            }
        }
    });
    drop(runtime);
    drop(sink);
    let output = writer
        .join()
        .map_err(|_panic| anyhow!("Perfetto writer thread panicked"))??;
    let (pending, complete) = result?;
    Ok((pending, complete.then_some(output)))
}

async fn checkpoint_analyzer(tx: &tokio::sync::mpsc::Sender<Message>) -> Result<Vec<Pending>> {
    let (reply, receive) = oneshot::channel();
    tx.send(Message::Checkpoint { reply })
        .await
        .context("sending analyzer checkpoint")?;
    receive.await.context("analyzer dropped checkpoint reply")
}

async fn end_analyzer(
    tx: &tokio::sync::mpsc::Sender<Message>,
    at: At,
) -> Result<(Vec<Pending>, bool)> {
    let (reply, receive) = oneshot::channel();
    tx.send(Message::End { at, reply })
        .await
        .context("sending analyzer end")?;
    Ok((receive.await.context("analyzer dropped end reply")?, true))
}

fn pending_strings(pending: &[Pending]) -> Vec<String> {
    pending.iter().map(|item| format!("{item:?}")).collect()
}

fn update_active(store: &Mutex<TraceStore>, capture: &Capture) {
    store_lock(store).set_active(capture.summary(TraceStatus::InProgress));
}

fn store_lock(store: &Mutex<TraceStore>) -> std::sync::MutexGuard<'_, TraceStore> {
    store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn unix_ns() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos()),
    )
    .unwrap_or(u64::MAX)
}

fn unix_ms() -> u64 {
    unix_ns() / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_analyzer_pipeline_manager::pipeline::NameRule;

    static TEST_PORT: Pipeline = Pipeline {
        name: "port",
        config_table: Some("PORT"),
        appl_table: "PORT_TABLE",
        fields: &["mtu", "admin_status"],
        field_alias: &[],
        asic_slots: &["SAI_OBJECT_TYPE_PORT", "SAI_OBJECT_TYPE_ROUTER_INTERFACE"],
        object_names: NameRule {
            prefix: "Ethernet",
            digits: true,
        },
    };

    fn event(database: &str, command: &str, key: &str, args: &[&str]) -> MonitorEvent {
        MonitorEvent {
            timestamp_ns: 1,
            database: database.to_owned(),
            client: "lua".to_owned(),
            args: std::iter::once(command.to_owned())
                .chain(std::iter::once(key.to_owned()))
                .chain(args.iter().map(|arg| (*arg).to_owned()))
                .collect(),
        }
    }

    static VLAN: Pipeline = Pipeline {
        name: "vlan",
        config_table: Some("VLAN"),
        appl_table: "VLAN_TABLE",
        fields: &["admin_status"],
        field_alias: &[],
        asic_slots: &["SAI_OBJECT_TYPE_VLAN"],
        object_names: NameRule {
            prefix: "Vlan",
            digits: true,
        },
    };

    #[rstest::rstest]
    #[case::config(event("CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"]), false, true)]
    #[case::untracked_config(event("CONFIG_DB", "HSET", "DEVICE_METADATA|localhost", &["x", "y"]), false, false)]
    #[case::appl_outside_window(event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]), false, false)]
    #[case::appl_inside_window(event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]), true, true)]
    #[case::appl_delete(event("APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[]), true, true)]
    #[case::appl_untracked(event("APPL_DB", "HSET", "PORT_TABLE:Ethernet1", &["mtu", "9100"]), true, false)]
    #[case::asic_queue_other_object(event("ASIC_DB", "LPUSH", ASIC_QUEUE_KEY, &["SAI_OBJECT_TYPE_VLAN:oid:0x1", "[]", "Screate"]), true, false)]
    #[case::asic_queue_port(event("ASIC_DB", "LPUSH", ASIC_QUEUE_KEY, &["SAI_OBJECT_TYPE_PORT:oid:0x1", "[]", "Sset"]), true, true)]
    #[case::asic_port_state(event("ASIC_DB", "HSET", "ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x1", &["mtu", "9100"]), true, true)]
    #[case::asic_rif_delete(event("ASIC_DB", "DEL", "ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE:oid:0x2", &[]), true, true)]
    #[case::asic_vlan_state(event("ASIC_DB", "HSET", "ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x3", &["mtu", "9100"]), true, false)]
    #[case::counters_dropped(event("COUNTERS_DB", "HSET", "COUNTERS:oid:0x1", &["x", "1"]), true, false)]
    #[case::config_delete_not_a_trigger(event("CONFIG_DB", "DEL", "PORT|Ethernet1", &[]), false, false)]
    fn test_filter(#[case] event: MonitorEvent, #[case] active: bool, #[case] expected: bool) {
        assert_eq!(should_capture(&event, active, &[&TEST_PORT]), expected);
    }

    #[test]
    fn test_filter_uses_loaded_pipeline_metadata() {
        let pipelines = [&VLAN];
        let trigger = event("CONFIG_DB", "HSET", "VLAN|Vlan10", &["admin_status", "up"]);
        let actual = vec![
            should_capture(&trigger, false, &pipelines),
            should_capture(
                &event("CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"]),
                false,
                &pipelines,
            ),
            should_capture(
                &event("CONFIG_DB", "HSET", "VLAN|VlanX", &["admin_status", "up"]),
                false,
                &pipelines,
            ),
            should_capture(
                &event(
                    "APPL_DB",
                    "HSET",
                    "_VLAN_TABLE:Vlan10",
                    &["admin_status", "up"],
                ),
                true,
                &pipelines,
            ),
            should_capture(
                &event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]),
                true,
                &pipelines,
            ),
            should_capture(
                &event(
                    "ASIC_DB",
                    "LPUSH",
                    ASIC_QUEUE_KEY,
                    &["SAI_OBJECT_TYPE_VLAN:oid:0x1", "[]", "Screate"],
                ),
                true,
                &pipelines,
            ),
            should_capture(
                &event(
                    "ASIC_DB",
                    "LPUSH",
                    ASIC_QUEUE_KEY,
                    &["SAI_OBJECT_TYPE_PORT:oid:0x1", "[]", "Sset"],
                ),
                true,
                &pipelines,
            ),
            should_capture(
                &event(
                    "ASIC_DB",
                    "HSET",
                    "ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x1",
                    &["admin_status", "up"],
                ),
                true,
                &pipelines,
            ),
        ];
        assert_eq!(
            actual,
            vec![true, false, false, true, false, true, false, true]
        );

        let mut capture = Capture::new(1, trigger, std::collections::BTreeMap::new(), &pipelines);
        capture.add(
            event(
                "APPL_DB",
                "HSET",
                "_VLAN_TABLE:Vlan10",
                &["admin_status", "up"],
            ),
            &pipelines,
        );
        assert_eq!(
            capture.summary(TraceStatus::InProgress).keys,
            vec!["Vlan10".to_owned()]
        );
    }

    #[test]
    fn test_normalized_recording_matches_sorted_analyzer_input() {
        let mut config = event("CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9000"]);
        config.timestamp_ns = 1_000_000_000;
        let mut capture = Capture::new(
            1,
            config,
            std::collections::BTreeMap::from([("oid:0x1".to_owned(), "Ethernet1".to_owned())]),
            &[&TEST_PORT],
        );
        let mut appl = event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9000"]);
        appl.timestamp_ns = 1_000_002_000;
        capture.add(appl, &[&TEST_PORT]);

        assert_eq!(
            capture.normalized_recording(),
            Recording {
                port_map: std::collections::BTreeMap::from([(
                    "oid:0x1".to_owned(),
                    "Ethernet1".to_owned(),
                )]),
                events: vec![
                    RecordingEvent {
                        offset_s: 0.0,
                        db: "CONFIG_DB".to_owned(),
                        cmd: "HSET".to_owned(),
                        key: "PORT|Ethernet1".to_owned(),
                        args: vec!["mtu".to_owned(), "9000".to_owned()],
                        client: "lua".to_owned(),
                    },
                    RecordingEvent {
                        offset_s: 0.000_002,
                        db: "APPL_DB".to_owned(),
                        cmd: "HSET".to_owned(),
                        key: "_PORT_TABLE:Ethernet1".to_owned(),
                        args: vec!["mtu".to_owned(), "9000".to_owned()],
                        client: "lua".to_owned(),
                    },
                ],
            }
        );
    }

    #[test]
    fn test_window_sorts_events_by_redis_time_stably() {
        let first = event("CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"]);
        let mut capture = Capture::new(1, first, std::collections::BTreeMap::new(), &[&TEST_PORT]);
        let mut second = event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]);
        second.timestamp_ns = 10;
        capture.add(second, &[&TEST_PORT]);
        let mut earlier = event(
            "ASIC_DB",
            "LPUSH",
            ASIC_QUEUE_KEY,
            &["SAI_OBJECT_TYPE_PORT:oid:0x1", "[]", "Sset"],
        );
        earlier.timestamp_ns = 5;
        capture.add(earlier, &[&TEST_PORT]);
        assert_eq!(
            capture
                .sorted_events()
                .iter()
                .map(|item| item.event.timestamp_ns)
                .collect::<Vec<_>>(),
            vec![1, 5, 10]
        );
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test code")]
    fn test_real_analyzer_completes_port_mtu_trace() {
        let mut config = event("CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"]);
        config.timestamp_ns = 1_000_000_000;
        let mut capture = Capture::new(1, config, std::collections::BTreeMap::new(), &[&TEST_PORT]);

        let mut queued = event("APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]);
        queued.timestamp_ns = 1_000_010_000;
        capture.add(queued, &[&TEST_PORT]);

        let mut consumed = event("APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[]);
        consumed.timestamp_ns = 1_000_030_000;
        capture.add(consumed, &[&TEST_PORT]);

        assert_eq!(
            analyze(&capture, true, &[&TEST_PORT]).unwrap(),
            (
                Vec::<Pending>::new(),
                Some(
                    concat!(
                        "[\n",
                        "{\"ph\":\"M\",\"pid\":1,\"name\":\"process_name\",\"args\":{\"name\":\"Port configuration\"}},\n",
                        "{\"ph\":\"M\",\"pid\":1,\"tid\":1,\"name\":\"thread_name\",\"args\":{\"name\":\"Ethernet1 - Step 1: Config saved (CONFIG_DB)\"}},\n",
                        "{\"ph\":\"X\",\"pid\":1,\"tid\":1,\"name\":\"Waiting for config to be forwarded\",\"ts\":0,\"dur\":10,\"args\":{\"seq\":[0,1]}},\n",
                        "{\"ph\":\"M\",\"pid\":1,\"tid\":2,\"name\":\"thread_name\",\"args\":{\"name\":\"Ethernet1 - Step 2: Sent to orchagent (APPL_DB)\"}},\n",
                        "{\"ph\":\"X\",\"pid\":1,\"tid\":2,\"name\":\"APPL_DB update pending consumption\",\"ts\":10,\"dur\":20,\"args\":{\"seq\":[1,2]}}\n",
                        "]\n"
                    )
                    .as_bytes()
                    .to_vec(),
                ),
            )
        );
    }
}
