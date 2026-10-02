use std::sync::{Arc, Mutex, PoisonError};

pub mod config_trace;
pub mod recording;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct At {
    pub seq: u64,
    pub ts_ns: u64,
}

pub const TRACK_CONFIG: &str = "config";
pub const TRACK_APPL: &str = "appl";
pub const SPAN_CONFIG: &str = "Written->Forwarded";
pub const SPAN_APPL: &str = "Queued->Consumed";
pub const SPAN_SET: &str = "Sset";
pub const SPAN_CREATE: &str = "Screate";
pub const SPAN_REMOVE: &str = "Sremove";

pub trait SpanSink: Send {
    fn span(&mut self, ev: SpanEvent);
    #[inline]
    fn flush(&mut self) {}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanEvent {
    pub pipeline: &'static str,
    pub key: Arc<str>,
    pub track: &'static str,
    pub name: &'static str,
    pub start: At,
    pub end: At,
    /// ASIC spans only.
    pub oid: Option<Arc<str>>,
    /// `Sset` spans only: the SAI attribute name.
    pub attr: Option<Arc<str>>,
    /// Still pending at `End`; `end` is the `End` time.
    pub open: bool,
}

/// Clones share one list, so a test can keep one clone and read what every holder emitted.
#[derive(Clone, Debug, Default)]
pub struct VecSink {
    spans: Arc<Mutex<Vec<SpanEvent>>>,
}

impl VecSink {
    /// Drains everything collected so far, in emission order.
    #[must_use]
    #[inline]
    pub fn take(&self) -> Vec<SpanEvent> {
        std::mem::take(&mut *self.spans.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl SpanSink for VecSink {
    #[inline]
    fn span(&mut self, ev: SpanEvent) {
        self.spans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(n: u64) -> At {
        At {
            seq: n,
            ts_ns: n * 1_000,
        }
    }

    fn span_a() -> SpanEvent {
        SpanEvent {
            pipeline: "port",
            key: Arc::from("Ethernet1"),
            track: "config",
            name: "Written->Forwarded",
            start: at(1),
            end: at(4),
            oid: None,
            attr: None,
            open: false,
        }
    }

    fn span_b() -> SpanEvent {
        SpanEvent {
            pipeline: "port",
            key: Arc::from("Ethernet1"),
            track: "appl",
            name: "Queued->Consumed",
            start: at(4),
            end: at(10),
            oid: None,
            attr: None,
            open: false,
        }
    }

    #[test]
    fn test_vec_sink_shares_ordered_spans_and_drains() {
        let sink = VecSink::default();
        let mut worker_sink = sink.clone();
        worker_sink.span(span_a());
        worker_sink.span(span_b());

        assert_eq!(sink.take(), vec![span_a(), span_b()]);
        assert!(sink.take().is_empty());
    }
}
