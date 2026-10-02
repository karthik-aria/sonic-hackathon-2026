use std::collections::{BTreeMap, VecDeque};

use config_analyzer_core::recording::Recording;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TraceStatus {
    InProgress,
    Complete,
    Stuck,
    Capped,
}

impl TraceStatus {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Complete => "complete",
            Self::Stuck => "stuck",
            Self::Capped => "capped",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TraceEntry {
    pub(crate) id: u64,
    pub(crate) opened_at_ms: u64,
    pub(crate) closed_at_ms: Option<u64>,
    pub(crate) keys: Vec<String>,
    pub(crate) status: TraceStatus,
    pub(crate) pending: Vec<String>,
    pub(crate) trace: Option<Vec<u8>>,
    pub(crate) recording: Option<Recording>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TraceSummary {
    pub(crate) id: u64,
    pub(crate) opened_at_ms: u64,
    pub(crate) closed_at_ms: Option<u64>,
    pub(crate) keys: Vec<String>,
    pub(crate) status: TraceStatus,
    pub(crate) pending: Vec<String>,
}

impl From<&TraceEntry> for TraceSummary {
    fn from(entry: &TraceEntry) -> Self {
        Self {
            id: entry.id,
            opened_at_ms: entry.opened_at_ms,
            closed_at_ms: entry.closed_at_ms,
            keys: entry.keys.clone(),
            status: entry.status,
            pending: entry.pending.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TraceStore {
    keep: usize,
    completed: VecDeque<TraceEntry>,
    active: Option<TraceEntry>,
    sources: BTreeMap<String, bool>,
    warnings: Vec<String>,
}

impl TraceStore {
    pub(crate) fn new(keep: usize) -> Self {
        Self {
            keep: keep.max(1),
            completed: VecDeque::new(),
            active: None,
            sources: BTreeMap::new(),
            warnings: Vec::new(),
        }
    }

    pub(crate) fn set_active(&mut self, entry: TraceEntry) {
        self.active = Some(entry);
    }

    pub(crate) fn finish(&mut self, entry: TraceEntry) {
        self.active = None;
        self.completed.push_back(entry);
        while self.completed.len() > self.keep {
            drop(self.completed.pop_front());
        }
    }

    pub(crate) fn set_source(&mut self, name: String, connected: bool) {
        let _ = self.sources.insert(name, connected);
    }

    pub(crate) fn add_warning(&mut self, warning: String) {
        if self.warnings.len() == 8 {
            drop(self.warnings.remove(0));
        }
        self.warnings.push(warning);
    }

    pub(crate) fn summaries(&self) -> Vec<TraceSummary> {
        self.completed
            .iter()
            .chain(self.active.iter())
            .map(TraceSummary::from)
            .collect()
    }

    pub(crate) const fn active(&self) -> Option<&TraceEntry> {
        self.active.as_ref()
    }

    pub(crate) fn trace(&self, id: u64) -> Option<Vec<u8>> {
        self.completed
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.trace.clone())
    }

    pub(crate) fn recording(&self, id: u64) -> Option<&Recording> {
        self.completed
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.recording.as_ref())
    }

    pub(crate) fn has_trace(&self, id: u64) -> bool {
        self.completed
            .iter()
            .any(|entry| entry.id == id && entry.trace.is_some())
    }

    pub(crate) const fn source_states(&self) -> &BTreeMap<String, bool> {
        &self.sources
    }

    pub(crate) fn monitors_connected(&self) -> bool {
        !self.sources.is_empty() && self.sources.values().all(|connected| *connected)
    }

    pub(crate) fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub(crate) fn completed_count(&self) -> usize {
        self.completed.len()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    const DEFAULT_KEEP: usize = 5;

    fn entry(id: u64) -> TraceEntry {
        TraceEntry {
            id,
            opened_at_ms: id,
            closed_at_ms: Some(id + 1),
            keys: vec![format!("Ethernet{id}")],
            status: TraceStatus::Complete,
            pending: Vec::new(),
            trace: Some(vec![u8::try_from(id).unwrap()]),
            recording: Some(Recording {
                port_map: BTreeMap::from([(format!("oid:0x{id:x}"), format!("Ethernet{id}"))]),
                events: Vec::new(),
            }),
        }
    }

    #[test]
    fn test_store_retains_last_five_traces_and_normalized_recordings() {
        let mut store = TraceStore::new(DEFAULT_KEEP);
        for id in 1..=6 {
            store.finish(entry(id));
        }
        assert_eq!(
            store
                .summaries()
                .iter()
                .map(|summary| summary.id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 5, 6]
        );
        assert_eq!(store.trace(1), None);
        assert_eq!(store.trace(6), Some(vec![6]));
        assert_eq!(store.recording(1), None);
        let expected_recording = entry(6).recording;
        assert_eq!(store.recording(6), expected_recording.as_ref());
    }

    #[test]
    fn test_active_trace_is_listed_but_not_downloadable() {
        let mut store = TraceStore::new(DEFAULT_KEEP);
        let mut active = entry(1);
        active.closed_at_ms = None;
        active.status = TraceStatus::InProgress;
        active.trace = None;
        active.recording = None;
        store.set_active(active);
        assert_eq!(store.summaries().len(), 1);
        assert_eq!(store.trace(1), None);
    }
}
