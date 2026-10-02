use std::collections::HashMap;
use std::sync::Arc;

use config_analyzer_core::At;
use tokio::sync::{mpsc, oneshot};

use crate::classify::{Event, classify};
use crate::instance::{AsicOp, AsicOpKind, Pending, PendingReason, Update};
use crate::pipeline::{Pipeline, PipelineIdx};
use crate::worker::WorkerMsg;
use crate::{Message, RedisOp};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Buffered {
    pub(crate) at: At,
    pub(crate) obj_type: Arc<str>,
    pub(crate) op: AsicOp,
}

#[derive(Debug)]
pub(crate) struct Router {
    // index = PipelineIdx
    pipelines: Vec<&'static Pipeline>,
    by_config: HashMap<&'static str, PipelineIdx>,
    by_appl: HashMap<&'static str, PipelineIdx>,
    // oid -> name, from ObjectMap
    names: HashMap<Arc<str>, Arc<str>>,
    by_oid: HashMap<Arc<str>, PipelineIdx>,
    // oid -> buffered ops, in arrival order
    unmapped: HashMap<Arc<str>, Vec<Buffered>>,
    // index = PipelineIdx
    workers: Vec<mpsc::Sender<WorkerMsg>>,
}

// CONFIG keys are `|`-separated and APPL keys `:`-separated, so composite keys must be normalized
// before both stages can land on the same instance.
fn normalize_key(key: &Arc<str>) -> Arc<str> {
    if key.contains('|') {
        Arc::from(key.replace('|', ":"))
    } else {
        Arc::clone(key)
    }
}

impl Router {
    pub(crate) fn new(
        pipelines: Vec<&'static Pipeline>,
        workers: Vec<mpsc::Sender<WorkerMsg>>,
    ) -> Self {
        let by_config = pipelines
            .iter()
            .enumerate()
            .filter_map(|(p, pipeline)| pipeline.config_table.map(|table| (table, p)))
            .collect();
        let by_appl = pipelines
            .iter()
            .enumerate()
            .map(|(p, pipeline)| (pipeline.appl_table, p))
            .collect();
        Self {
            pipelines,
            by_config,
            by_appl,
            names: HashMap::new(),
            by_oid: HashMap::new(),
            unmapped: HashMap::new(),
            workers,
        }
    }

    fn field_mask(&self, p: PipelineIdx, fields: &[Arc<str>]) -> u64 {
        self.pipelines
            .get(p)
            .map_or(0, |pipeline| pipeline.field_mask(fields))
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<Message>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Op(op) => self.on_op(&op).await,
                Message::ObjectMap { oid, name } => self.on_object_map(oid, name).await,
                Message::Checkpoint { reply } => drop(reply.send(self.barrier(None).await)),
                Message::End { at, reply } => {
                    drop(reply.send(self.barrier(Some(at)).await));
                    break;
                }
            }
        }
    }

    async fn on_op(&mut self, op: &RedisOp) {
        let Some(event) = classify(op) else {
            return;
        };
        let at = op.at;
        let (obj_type, asic) = match event {
            Event::ConfigWritten { table, key, fields } => {
                if let Some(&p) = self.by_config.get(table.as_ref()) {
                    let mask = self.field_mask(p, &fields);
                    let key = normalize_key(&key);
                    self.send(p, WorkerMsg::Update(Update::Config { at, key, mask }))
                        .await;
                }
                return;
            }
            Event::ApplQueued { table, key, fields } => {
                if let Some(&p) = self.by_appl.get(table.as_ref()) {
                    let mask = self.field_mask(p, &fields);
                    self.send(p, WorkerMsg::Update(Update::Queued { at, key, mask }))
                        .await;
                }
                return;
            }
            Event::ApplConsumed { table, key } => {
                if let Some(&p) = self.by_appl.get(table.as_ref()) {
                    self.send(p, WorkerMsg::Update(Update::Consumed { at, key }))
                        .await;
                }
                return;
            }
            Event::SaiRequested {
                obj_type,
                oid,
                op: sai_op,
                attr,
            } => (
                obj_type,
                AsicOp {
                    oid,
                    kind: AsicOpKind::Request { op: sai_op, attr },
                },
            ),
            Event::AsicWritten { obj_type, oid } => (
                obj_type,
                AsicOp {
                    oid,
                    kind: AsicOpKind::Written,
                },
            ),
            Event::AsicDeleted { obj_type, oid } => (
                obj_type,
                AsicOp {
                    oid,
                    kind: AsicOpKind::Deleted,
                },
            ),
        };
        if !self.pipelines.iter().any(|pipeline| {
            pipeline
                .asic_slots
                .iter()
                .any(|slot| *slot == obj_type.as_ref())
        }) {
            return;
        }
        if let Some(&p) = self.by_oid.get(&asic.oid) {
            self.send(p, WorkerMsg::Update(Update::Asic { at, op: asic }))
                .await;
            return;
        }
        let oid = Arc::clone(&asic.oid);
        let buffered = Buffered {
            at,
            obj_type,
            op: asic,
        };
        if let Some(name) = self.names.get(&oid).map(Arc::clone) {
            self.resolve(oid, name, vec![buffered]).await;
        } else {
            self.unmapped.entry(oid).or_default().push(buffered);
        }
    }

    async fn on_object_map(&mut self, oid: Arc<str>, name: Arc<str>) {
        drop(self.names.insert(Arc::clone(&oid), Arc::clone(&name)));
        if let Some(ops) = self.unmapped.remove(&oid) {
            self.resolve(oid, name, ops).await;
        }
    }

    // Object type alone is ambiguous (a router interface can be port, LAG or VLAN), hence the
    // name check. On no match the ops are dropped and nothing is recorded.
    async fn resolve(&mut self, oid: Arc<str>, name: Arc<str>, ops: Vec<Buffered>) {
        let Some(first) = ops.first() else {
            return;
        };
        let found = self.pipelines.iter().enumerate().find_map(|(p, pipeline)| {
            pipeline
                .asic_slots
                .iter()
                .position(|slot| *slot == first.obj_type.as_ref())
                .filter(|_| pipeline.object_names.matches(&name))
                .map(|slot| (p, slot))
        });
        let Some((p, slot)) = found else {
            return;
        };
        let _ = self.by_oid.insert(Arc::clone(&oid), p);
        self.send(
            p,
            WorkerMsg::Update(Update::Map {
                oid,
                key: name,
                slot,
            }),
        )
        .await;
        for Buffered { at, op, .. } in ops {
            self.send(p, WorkerMsg::Update(Update::Asic { at, op }))
                .await;
        }
    }

    // All barriers are sent before any reply is awaited, so the replies form one consistent cut.
    async fn barrier(&self, end: Option<At>) -> Vec<Pending> {
        let mut receivers = Vec::with_capacity(self.workers.len());
        for p in 0..self.workers.len() {
            let (reply, rx) = oneshot::channel();
            self.send(p, WorkerMsg::Barrier { end, reply }).await;
            receivers.push(rx);
        }
        let mut replies = Vec::with_capacity(receivers.len());
        for rx in receivers {
            replies.push(rx.await.unwrap_or_default());
        }
        merge(replies, &self.unmapped)
    }

    async fn send(&self, p: PipelineIdx, msg: WorkerMsg) {
        if let Some(tx) = self.workers.get(p) {
            drop(tx.send(msg).await);
        }
    }
}

pub(crate) fn merge(
    workers: Vec<Vec<Pending>>,
    unmapped: &HashMap<Arc<str>, Vec<Buffered>>,
) -> Vec<Pending> {
    let mut orphans: Vec<Pending> = unmapped
        .iter()
        .map(|(oid, ops)| Pending {
            pipeline: None,
            key: Arc::clone(oid),
            reason: PendingReason::OrphanOid { ops: ops.len() },
        })
        .collect();
    orphans.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    workers.into_iter().flatten().chain(orphans).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffered(seq: u64) -> Buffered {
        Buffered {
            at: At {
                seq,
                ts_ns: seq * 1_000,
            },
            obj_type: Arc::from("SAI_OBJECT_TYPE_PORT"),
            op: AsicOp {
                oid: Arc::from("oid:0x1"),
                kind: AsicOpKind::Written,
            },
        }
    }

    fn pending(key: &str, reason: PendingReason) -> Pending {
        Pending {
            pipeline: Some("port"),
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

    #[test]
    fn test_merge() {
        let a = pending("Ethernet1", PendingReason::ApplQueued);
        let b = pending("Ethernet5", PendingReason::ConfigNotForwarded);
        let unmapped = HashMap::from([
            (Arc::from("oid:0x2"), vec![buffered(1)]),
            (Arc::from("oid:0x1"), vec![buffered(2), buffered(3)]),
        ]);
        assert_eq!(
            merge(vec![vec![a.clone()], vec![b.clone()]], &unmapped),
            vec![a, b, orphan("oid:0x1", 2), orphan("oid:0x2", 1)]
        );
    }
}
