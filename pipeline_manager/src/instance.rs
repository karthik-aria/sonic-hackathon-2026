use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use config_analyzer_core::{At, SpanEvent, SpanSink};

use crate::classify::SaiOp;
use crate::pipeline::{
    InstanceId, Pipeline, SPAN_APPL, SPAN_CONFIG, SPAN_CREATE, SPAN_REMOVE, SPAN_SET, SlotIdx,
    TRACK_APPL, TRACK_CONFIG,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigStage {
    Written,
    Forwarded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplStage {
    Queued,
    Consumed,
}

// No `Default` derive: it would require `S: Default`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageTrack<S> {
    // Stage and when it was entered; None = never seen.
    pub current: Option<(S, At)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenSet {
    pub at: At,
    // First SAI attr name of the request.
    pub attr: Option<Arc<str>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AsicSlot {
    pub oid: Option<Arc<str>>,
    // Pending Sset requests, paired FIFO with writes.
    pub open: VecDeque<OpenSet>,
    pub pending_create: Option<At>,
    pub pending_remove: Option<At>,
}

impl AsicSlot {
    fn is_idle(&self) -> bool {
        self.open.is_empty() && self.pending_create.is_none() && self.pending_remove.is_none()
    }
}

// Mutable per-key open state. No history; closed spans go to the sink. One ASIC slot per entry
// in `Pipeline::asic_slots`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub key: Arc<str>,
    pub config: StageTrack<ConfigStage>,
    pub appl: StageTrack<ApplStage>,
    // CONFIG field bits not yet seen in `_<APPL_TABLE>:<key>`.
    pub uncovered: u64,
    pub asic: Box<[AsicSlot]>,
}

impl Instance {
    #[must_use]
    #[inline]
    pub fn new(key: Arc<str>, slots: usize) -> Self {
        Self {
            key,
            config: StageTrack { current: None },
            appl: StageTrack { current: None },
            uncovered: 0,
            asic: vec![AsicSlot::default(); slots].into_boxed_slice(),
        }
    }

    #[must_use]
    #[inline]
    pub fn is_terminal(&self) -> bool {
        !matches!(self.config.current, Some((ConfigStage::Written, _)))
            && !matches!(self.appl.current, Some((ApplStage::Queued, _)))
            && self.uncovered == 0
            && self.asic.iter().all(AsicSlot::is_idle)
    }
}

// Owned exclusively by one worker; no locks.
#[derive(Debug)]
pub struct PipelineState<K: SpanSink> {
    pub pipeline: &'static Pipeline,
    // Arena indexed by InstanceId, in creation order.
    pub instances: Vec<Instance>,
    pub by_key: HashMap<Arc<str>, InstanceId>,
    // Local half of the OID map.
    pub by_oid: HashMap<Arc<str>, (InstanceId, SlotIdx)>,
    pub sink: K,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Update {
    Config {
        at: At,
        key: Arc<str>,
        mask: u64,
    },
    Queued {
        at: At,
        key: Arc<str>,
        mask: u64,
    },
    Consumed {
        at: At,
        key: Arc<str>,
    },
    Map {
        oid: Arc<str>,
        key: Arc<str>,
        slot: SlotIdx,
    },
    Asic {
        at: At,
        op: AsicOp,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsicOp {
    pub oid: Arc<str>,
    pub kind: AsicOpKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AsicOpKind {
    Request { op: SaiOp, attr: Option<Arc<str>> },
    Written,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub pipeline: Option<&'static str>,
    pub key: Arc<str>,
    pub reason: PendingReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingReason {
    ConfigNotForwarded,
    ApplQueued,
    Uncovered(u64),
    AsicInFlight {
        slot: SlotIdx,
        sets: usize,
        create: bool,
        remove: bool,
    },
    // `Pending::pipeline` is None and `Pending::key` is the oid.
    OrphanOid {
        ops: usize,
    },
}

const fn event(
    pipeline: &'static str,
    key: Arc<str>,
    track: &'static str,
    name: &'static str,
    start: At,
    end: At,
) -> SpanEvent {
    SpanEvent {
        pipeline,
        key,
        track,
        name,
        start,
        end,
        oid: None,
        attr: None,
        open: false,
    }
}

impl<K: SpanSink> PipelineState<K> {
    #[must_use]
    #[inline]
    pub fn new(pipeline: &'static Pipeline, sink: K) -> Self {
        Self {
            pipeline,
            instances: Vec::new(),
            by_key: HashMap::new(),
            by_oid: HashMap::new(),
            sink,
        }
    }

    #[inline]
    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Config { at, key, mask } => self.config(at, key, mask),
            Update::Queued { at, key, mask } => self.queued(at, key, mask),
            Update::Consumed { at, key } => self.consumed(at, key),
            Update::Map { oid, key, slot } => self.map(oid, key, slot),
            Update::Asic { at, op } => self.asic(at, op),
        }
    }

    #[must_use]
    #[inline]
    pub fn pending(&self) -> Vec<Pending> {
        let pipeline = self.pipeline.name;
        self.instances
            .iter()
            .filter(|instance| !instance.is_terminal())
            .flat_map(|instance| {
                let config = matches!(instance.config.current, Some((ConfigStage::Written, _)))
                    .then_some(PendingReason::ConfigNotForwarded);
                let appl = matches!(instance.appl.current, Some((ApplStage::Queued, _)))
                    .then_some(PendingReason::ApplQueued);
                let uncovered = (instance.uncovered != 0)
                    .then_some(PendingReason::Uncovered(instance.uncovered));
                let asic = instance
                    .asic
                    .iter()
                    .enumerate()
                    .filter(|(_, a)| !a.is_idle())
                    .map(|(slot, a)| PendingReason::AsicInFlight {
                        slot,
                        sets: a.open.len(),
                        create: a.pending_create.is_some(),
                        remove: a.pending_remove.is_some(),
                    });
                config
                    .into_iter()
                    .chain(appl)
                    .chain(uncovered)
                    .chain(asic)
                    .map(|reason| Pending {
                        pipeline: Some(pipeline),
                        key: Arc::clone(&instance.key),
                        reason,
                    })
            })
            .collect()
    }

    // Emits every open entry with `open: true` and `end = at`, then flushes. State is unchanged.
    #[inline]
    pub fn end(&mut self, at: At) {
        let pipeline = self.pipeline;
        for instance in &self.instances {
            let stage_event = |track, name, start| SpanEvent {
                open: true,
                ..event(
                    pipeline.name,
                    Arc::clone(&instance.key),
                    track,
                    name,
                    start,
                    at,
                )
            };
            if let Some((ConfigStage::Written, start)) = instance.config.current {
                self.sink
                    .span(stage_event(TRACK_CONFIG, SPAN_CONFIG, start));
            }
            if let Some((ApplStage::Queued, start)) = instance.appl.current {
                self.sink.span(stage_event(TRACK_APPL, SPAN_APPL, start));
            }
            for (track, slot) in pipeline.asic_slots.iter().zip(instance.asic.iter()) {
                let asic_event = |name, start, attr| SpanEvent {
                    oid: slot.oid.as_ref().map(Arc::clone),
                    attr,
                    ..stage_event(track, name, start)
                };
                if let Some(start) = slot.pending_create {
                    self.sink.span(asic_event(SPAN_CREATE, start, None));
                }
                for set in &slot.open {
                    self.sink.span(asic_event(
                        SPAN_SET,
                        set.at,
                        set.attr.as_ref().map(Arc::clone),
                    ));
                }
                if let Some(start) = slot.pending_remove {
                    self.sink.span(asic_event(SPAN_REMOVE, start, None));
                }
            }
        }
        self.sink.flush();
    }

    fn instance_id(&mut self, key: Arc<str>) -> InstanceId {
        if let Some(id) = self.by_key.get(&key) {
            return *id;
        }
        let id = self.instances.len();
        let slots = self.pipeline.asic_slots.len();
        self.instances.push(Instance::new(Arc::clone(&key), slots));
        let _ = self.by_key.insert(key, id);
        id
    }

    fn config(&mut self, at: At, key: Arc<str>, mask: u64) {
        let id = self.instance_id(key);
        let Some(instance) = self.instances.get_mut(id) else {
            return;
        };
        instance.uncovered |= mask;
        if !matches!(instance.config.current, Some((ConfigStage::Written, _))) {
            instance.config.current = Some((ConfigStage::Written, at));
        }
    }

    fn queued(&mut self, at: At, key: Arc<str>, mask: u64) {
        let id = self.instance_id(key);
        let Some(instance) = self.instances.get_mut(id) else {
            return;
        };
        instance.uncovered &= !mask;
        if let Some((ConfigStage::Written, start)) = instance.config.current {
            instance.config.current = Some((ConfigStage::Forwarded, at));
            let key = Arc::clone(&instance.key);
            self.sink.span(event(
                self.pipeline.name,
                key,
                TRACK_CONFIG,
                SPAN_CONFIG,
                start,
                at,
            ));
        }
        if !matches!(instance.appl.current, Some((ApplStage::Queued, _))) {
            instance.appl.current = Some((ApplStage::Queued, at));
        }
    }

    fn consumed(&mut self, at: At, key: Arc<str>) {
        let id = self.instance_id(key);
        let Some(instance) = self.instances.get_mut(id) else {
            return;
        };
        if let Some((ApplStage::Queued, start)) = instance.appl.current {
            let key = Arc::clone(&instance.key);
            self.sink.span(event(
                self.pipeline.name,
                key,
                TRACK_APPL,
                SPAN_APPL,
                start,
                at,
            ));
        }
        instance.appl.current = Some((ApplStage::Consumed, at));
    }

    fn map(&mut self, oid: Arc<str>, key: Arc<str>, slot: SlotIdx) {
        let id = self.instance_id(key);
        let Some(asic) = self
            .instances
            .get_mut(id)
            .and_then(|i| i.asic.get_mut(slot))
        else {
            return;
        };
        if let Some(old) = asic.oid.as_ref().filter(|old| **old != oid) {
            let _ = self.by_oid.remove(old);
        }
        let _ = self.by_oid.insert(Arc::clone(&oid), (id, slot));
        asic.oid = Some(oid);
    }

    fn asic(&mut self, at: At, op: AsicOp) {
        let Some(&(id, slot)) = self.by_oid.get(&op.oid) else {
            return;
        };
        let pipeline = self.pipeline;
        let Some(track) = pipeline.asic_slots.get(slot).copied() else {
            return;
        };
        let Some(Instance { key, asic, .. }) = self.instances.get_mut(id) else {
            return;
        };
        let Some(state) = asic.get_mut(slot) else {
            return;
        };
        let closed = match op.kind {
            AsicOpKind::Request {
                op: SaiOp::Set,
                attr,
            } => {
                state.open.push_back(OpenSet { at, attr });
                None
            }
            AsicOpKind::Request {
                op: SaiOp::Create, ..
            } => {
                if state.pending_create.is_none() {
                    state.pending_create = Some(at);
                }
                None
            }
            AsicOpKind::Request {
                op: SaiOp::Remove, ..
            } => {
                if state.pending_remove.is_none() {
                    state.pending_remove = Some(at);
                }
                None
            }
            AsicOpKind::Written => state
                .pending_create
                .take()
                .map(|start| (SPAN_CREATE, start, None))
                .or_else(|| {
                    state
                        .open
                        .pop_front()
                        .map(|set| (SPAN_SET, set.at, set.attr))
                }),
            AsicOpKind::Deleted => state
                .pending_remove
                .take()
                .map(|start| (SPAN_REMOVE, start, None)),
        };
        if let Some((name, start, attr)) = closed {
            self.sink.span(SpanEvent {
                oid: state.oid.as_ref().map(Arc::clone),
                attr,
                ..event(pipeline.name, Arc::clone(key), track, name, start, at)
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TEST_PORT;
    use config_analyzer_core::VecSink;

    const E1: &str = "Ethernet1";
    const P: &str = "SAI_OBJECT_TYPE_PORT";
    const OP: &str = "oid:0x1000000000002";
    const PM: &str = "SAI_PORT_ATTR_MTU";

    type State = PipelineState<VecSink>;

    fn new_state() -> (State, VecSink) {
        let sink = VecSink::default();
        (PipelineState::new(&TEST_PORT, sink.clone()), sink)
    }

    const fn at(n: u64) -> At {
        At {
            seq: n,
            ts_ns: n * 1_000,
        }
    }

    fn arc(s: &str) -> Arc<str> {
        Arc::from(s)
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
            pipeline: "port",
            key: arc(E1),
            track,
            name,
            start,
            end,
            oid: oid.map(arc),
            attr: attr.map(arc),
            open: false,
        }
    }

    fn open(ev: SpanEvent) -> SpanEvent {
        SpanEvent { open: true, ..ev }
    }

    fn pending(key: &str, reason: PendingReason) -> Pending {
        Pending {
            pipeline: Some("port"),
            key: arc(key),
            reason,
        }
    }

    fn in_flight(slot: SlotIdx, sets: usize, create: bool, remove: bool) -> PendingReason {
        PendingReason::AsicInFlight {
            slot,
            sets,
            create,
            remove,
        }
    }

    fn map(oid: &str, slot: SlotIdx) -> Update {
        Update::Map {
            oid: arc(oid),
            key: arc(E1),
            slot,
        }
    }

    fn config(at: At, key: &str, mask: u64) -> Update {
        Update::Config {
            at,
            key: arc(key),
            mask,
        }
    }

    fn queued(at: At, mask: u64) -> Update {
        Update::Queued {
            at,
            key: arc(E1),
            mask,
        }
    }

    fn asic(at: At, oid: &str, kind: AsicOpKind) -> Update {
        Update::Asic {
            at,
            op: AsicOp {
                oid: arc(oid),
                kind,
            },
        }
    }

    fn set(attr: &str) -> AsicOpKind {
        AsicOpKind::Request {
            op: SaiOp::Set,
            attr: Some(arc(attr)),
        }
    }

    const CREATE: AsicOpKind = AsicOpKind::Request {
        op: SaiOp::Create,
        attr: None,
    };
    const REMOVE: AsicOpKind = AsicOpKind::Request {
        op: SaiOp::Remove,
        attr: None,
    };

    fn apply_all<I>(state: &mut State, updates: I)
    where
        I: IntoIterator<Item = Update>,
    {
        for update in updates {
            state.apply(update);
        }
    }

    #[test]
    fn test_create_pairing_repeated_create() {
        let (mut state, sink) = new_state();
        apply_all(
            &mut state,
            [map(OP, 0), asic(at(1), OP, CREATE), asic(at(2), OP, CREATE)],
        );
        let checkpoint = state.pending();
        state.apply(asic(at(3), OP, AsicOpKind::Written));
        assert_eq!(
            (checkpoint, state.pending()),
            (vec![pending(E1, in_flight(0, 0, true, false))], vec![])
        );
        assert_eq!(
            sink.take(),
            vec![span(P, SPAN_CREATE, at(1), at(3), Some(OP), None)]
        );
    }

    #[test]
    fn test_remove_pairing_repeated_remove() {
        let (mut state, sink) = new_state();
        apply_all(
            &mut state,
            [
                map(OP, 0),
                asic(at(1), OP, AsicOpKind::Deleted),
                asic(at(2), OP, REMOVE),
                asic(at(3), OP, REMOVE),
                asic(at(4), OP, AsicOpKind::Deleted),
            ],
        );
        assert_eq!(state.pending(), vec![]);
        assert_eq!(
            sink.take(),
            vec![span(P, SPAN_REMOVE, at(2), at(4), Some(OP), None)]
        );
    }

    #[test]
    fn test_written_closes_create_before_set() {
        let (mut state, sink) = new_state();
        apply_all(
            &mut state,
            [
                map(OP, 0),
                asic(at(1), OP, set(PM)),
                asic(at(2), OP, CREATE),
                asic(at(3), OP, AsicOpKind::Written),
                asic(at(4), OP, AsicOpKind::Written),
            ],
        );
        assert_eq!(state.pending(), vec![]);
        assert_eq!(
            sink.take(),
            vec![
                span(P, SPAN_CREATE, at(2), at(3), Some(OP), None),
                span(P, SPAN_SET, at(1), at(4), Some(OP), Some(PM)),
            ]
        );
    }

    #[test]
    fn test_end_order() {
        let (mut state, sink) = new_state();
        apply_all(
            &mut state,
            [
                queued(at(1), 0),
                config(at(2), E1, 0),
                map(OP, 0),
                asic(at(3), OP, CREATE),
                asic(at(4), OP, set("A")),
                asic(at(5), OP, set("B")),
                asic(at(6), OP, REMOVE),
            ],
        );
        state.end(at(7));
        let expected = vec![
            pending(E1, PendingReason::ConfigNotForwarded),
            pending(E1, PendingReason::ApplQueued),
            pending(E1, in_flight(0, 2, true, true)),
        ];
        assert_eq!(state.pending(), expected);
        assert_eq!(
            sink.take(),
            vec![
                open(span(TRACK_CONFIG, SPAN_CONFIG, at(2), at(7), None, None)),
                open(span(TRACK_APPL, SPAN_APPL, at(1), at(7), None, None)),
                open(span(P, SPAN_CREATE, at(3), at(7), Some(OP), None)),
                open(span(P, SPAN_SET, at(4), at(7), Some(OP), Some("A"))),
                open(span(P, SPAN_SET, at(5), at(7), Some(OP), Some("B"))),
                open(span(P, SPAN_REMOVE, at(6), at(7), Some(OP), None)),
            ]
        );
    }

    #[test]
    fn test_asic_unmapped_oid_ignored() {
        let (mut state, sink) = new_state();
        state.apply(asic(at(1), OP, AsicOpKind::Written));
        assert_eq!((state.pending(), state.instances.clone()), (vec![], vec![]));
        assert_eq!(sink.take(), vec![]);
    }

    #[test]
    fn test_pending_arena_order() {
        let (mut state, sink) = new_state();
        apply_all(
            &mut state,
            [config(at(1), "Ethernet5", 0), config(at(2), E1, 0)],
        );
        assert_eq!(
            state.pending(),
            vec![
                pending("Ethernet5", PendingReason::ConfigNotForwarded),
                pending(E1, PendingReason::ConfigNotForwarded),
            ]
        );
        assert_eq!(sink.take(), vec![]);
    }
}
