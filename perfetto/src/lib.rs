use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use config_analyzer_core::{
    SPAN_APPL, SPAN_CONFIG, SPAN_CREATE, SPAN_REMOVE, SPAN_SET, SpanEvent, SpanSink, TRACK_APPL,
    TRACK_CONFIG,
};
use serde::Serialize;

// One clone per worker.
#[derive(Clone, Debug)]
pub struct PerfettoSink {
    tx: mpsc::Sender<SpanEvent>,
}

impl SpanSink for PerfettoSink {
    // Unbounded channel: never blocks. A closed writer (thread ended on an I/O error) is ignored.
    #[inline]
    fn span(&mut self, ev: SpanEvent) {
        drop(self.tx.send(ev));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TrackKey {
    pipeline: &'static str,
    key: Arc<str>,
    track: &'static str,
}

// Line structs borrow `&str` because the workspace `serde` has no `rc` feature, so `Arc<str>` is
// not `Serialize`. Field order is the JSON key order.
#[derive(Debug, Serialize)]
struct NameArgs<'a> {
    name: &'a str,
}

#[derive(Debug, Serialize)]
struct MetaLine<'a> {
    ph: &'static str,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tid: Option<u32>,
    name: &'static str,
    args: NameArgs<'a>,
}

#[derive(Debug, Serialize)]
struct SpanArgs<'a> {
    seq: [u64; 2],
    #[serde(skip_serializing_if = "Option::is_none")]
    object_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oid: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attr: Option<&'a str>,
    // Not `bool` with a skip fn: that fn would take `&bool` (`trivially_copy_pass_by_ref`).
    #[serde(skip_serializing_if = "Option::is_none")]
    open: Option<bool>,
}

#[derive(Debug, Serialize)]
struct SpanLine<'a> {
    ph: &'static str,
    pid: u32,
    tid: u32,
    name: &'a str,
    ts: u64,
    dur: u64,
    args: SpanArgs<'a>,
}

// Chrome JSON trace, array form. The closing `]` is optional in that format, so a file read
// mid-run or after a crash still opens in ui.perfetto.dev.
#[derive(Debug)]
pub struct TraceWriter<W> {
    out: W,
    pids: HashMap<&'static str, u32>,
    // Global across pipelines.
    tids: HashMap<TrackKey, u32>,
    next_pid: u32,
    next_tid: u32,
    started: bool,
}

impl<W> TraceWriter<W>
where
    W: Write,
{
    #[inline]
    pub fn new(mut out: W) -> io::Result<Self> {
        out.write_all(b"[")?;
        Ok(Self {
            out,
            pids: HashMap::new(),
            tids: HashMap::new(),
            next_pid: 1,
            next_tid: 1,
            started: false,
        })
    }

    #[inline]
    pub fn write(&mut self, ev: &SpanEvent) -> io::Result<()> {
        let pid = self.pid(ev.pipeline)?;
        let tid = self.tid(pid, ev)?;
        self.line(&SpanLine {
            ph: "X",
            pid,
            tid,
            name: &span_label(ev),
            ts: ev.start.ts_ns / 1_000,
            dur: ev.end.ts_ns.saturating_sub(ev.start.ts_ns) / 1_000,
            args: SpanArgs {
                seq: [ev.start.seq, ev.end.seq],
                object_type: is_asic_track(ev.track).then_some(ev.track),
                oid: ev.oid.as_deref(),
                attr: ev.attr.as_deref(),
                open: ev.open.then_some(true),
            },
        })
    }

    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    #[inline]
    pub fn into_inner(mut self) -> io::Result<W> {
        self.out.write_all(b"\n]\n")?;
        self.out.flush()?;
        Ok(self.out)
    }

    fn pid(&mut self, pipeline: &'static str) -> io::Result<u32> {
        if let Some(id) = self.pids.get(pipeline) {
            return Ok(*id);
        }
        let pid = self.next_pid;
        self.next_pid += 1;
        let _ = self.pids.insert(pipeline, pid);
        self.line(&MetaLine {
            ph: "M",
            pid,
            tid: None,
            name: "process_name",
            args: NameArgs {
                name: &format!("{} configuration", capitalize(&pipeline.replace('_', " "))),
            },
        })?;
        Ok(pid)
    }

    fn tid(&mut self, pid: u32, ev: &SpanEvent) -> io::Result<u32> {
        let track = TrackKey {
            pipeline: ev.pipeline,
            key: Arc::clone(&ev.key),
            track: ev.track,
        };
        if let Some(id) = self.tids.get(&track) {
            return Ok(*id);
        }
        let tid = self.next_tid;
        self.next_tid += 1;
        let _ = self.tids.insert(track, tid);
        self.line(&MetaLine {
            ph: "M",
            pid,
            tid: Some(tid),
            name: "thread_name",
            args: NameArgs {
                name: &format!("{} - {}", ev.key, track_label(ev.track)),
            },
        })?;
        Ok(tid)
    }

    fn line<T>(&mut self, value: &T) -> io::Result<()>
    where
        T: Serialize,
    {
        let sep: &[u8] = if self.started { b",\n" } else { b"\n" };
        self.out.write_all(sep)?;
        self.started = true;
        serde_json::to_writer(&mut self.out, value)?;
        Ok(())
    }
}

fn is_asic_track(track: &str) -> bool {
    track != TRACK_CONFIG && track != TRACK_APPL
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}

// Labels shown in the Perfetto UI. The raw SAI object type and attribute stay in the span args.
fn track_label(track: &str) -> String {
    match track {
        TRACK_CONFIG => "Step 1: Config saved (CONFIG_DB)".to_owned(),
        TRACK_APPL => "Step 2: Sent to orchagent (APPL_DB)".to_owned(),
        object_type => {
            let object = object_type
                .strip_prefix("SAI_OBJECT_TYPE_")
                .unwrap_or(object_type);
            format!(
                "Step 3: Hardware {} (ASIC_DB)",
                object.replace('_', " ").to_lowercase()
            )
        }
    }
}

fn span_label(ev: &SpanEvent) -> String {
    match ev.name {
        SPAN_CONFIG => "Waiting for config to be forwarded".to_owned(),
        SPAN_APPL => "APPL_DB update pending consumption".to_owned(),
        SPAN_SET => ev.attr.as_deref().map_or_else(
            || "Program hardware attribute".to_owned(),
            |attr| {
                let name = attr.split_once("_ATTR_").map_or(attr, |(_, name)| name);
                format!("Program {}", name.replace('_', " "))
            },
        ),
        SPAN_CREATE => "Create hardware object".to_owned(),
        SPAN_REMOVE => "Remove hardware object".to_owned(),
        other => other.to_owned(),
    }
}

// Spawns the writer thread, which flushes whenever the channel is momentarily empty and ends when
// every `PerfettoSink` clone is dropped, returning the completed output.
#[inline]
pub fn spawn_writer<W>(out: W) -> io::Result<(PerfettoSink, JoinHandle<io::Result<W>>)>
where
    W: Write + Send + 'static,
{
    let mut writer = TraceWriter::new(out)?;
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        while let Ok(ev) = rx.recv() {
            writer.write(&ev)?;
            while let Ok(ev) = rx.try_recv() {
                writer.write(&ev)?;
            }
            writer.flush()?;
        }
        writer.into_inner()
    });
    Ok((PerfettoSink { tx }, handle))
}

// Creates (truncates) the file and spawns its writer thread.
#[inline]
pub fn spawn_file(
    path: &Path,
) -> io::Result<(PerfettoSink, JoinHandle<io::Result<BufWriter<File>>>)> {
    spawn_writer(BufWriter::new(File::create(path)?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use config_analyzer_core::At;
    use rstest::rstest;

    const E1: &str = "Ethernet1";
    const P: &str = "SAI_OBJECT_TYPE_PORT";
    const R: &str = "SAI_OBJECT_TYPE_ROUTER_INTERFACE";
    const OP: &str = "oid:0x1000000000002";
    const OR: &str = "oid:0x60000000001fb";
    const FL: &str = "SAI_PORT_ATTR_FAST_LINKUP_ENABLED";
    const PM: &str = "SAI_PORT_ATTR_MTU";
    const RM: &str = "SAI_ROUTER_INTERFACE_ATTR_MTU";
    const CONFIG: &str = TRACK_CONFIG;
    const APPL: &str = TRACK_APPL;
    const WF: &str = SPAN_CONFIG;
    const QC: &str = SPAN_APPL;
    const META_PORT: &str =
        r#"{"ph":"M","pid":1,"name":"process_name","args":{"name":"Port configuration"}}"#;
    const META_E1_CONFIG: &str = r#"{"ph":"M","pid":1,"tid":1,"name":"thread_name","args":{"name":"Ethernet1 - Step 1: Config saved (CONFIG_DB)"}}"#;
    const TWO_PIPELINES: [&str; 6] = [
        META_PORT,
        META_E1_CONFIG,
        r#"{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":1,"dur":1,"args":{"seq":[1,2]}}"#,
        r#"{"ph":"M","pid":2,"name":"process_name","args":{"name":"Vlan member configuration"}}"#,
        r#"{"ph":"M","pid":2,"tid":2,"name":"thread_name","args":{"name":"Vlan100|Ethernet1 - Step 2: Sent to orchagent (APPL_DB)"}}"#,
        r#"{"ph":"X","pid":2,"tid":2,"name":"APPL_DB update pending consumption","ts":3,"dur":1,"args":{"seq":[3,4]}}"#,
    ];
    const ETHERNET1: [&str; 12] = [
        META_PORT,
        META_E1_CONFIG,
        r#"{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":8593362,"dur":8475,"args":{"seq":[1,2]}}"#,
        r#"{"ph":"M","pid":1,"tid":2,"name":"thread_name","args":{"name":"Ethernet1 - Step 2: Sent to orchagent (APPL_DB)"}}"#,
        r#"{"ph":"X","pid":1,"tid":2,"name":"APPL_DB update pending consumption","ts":8601837,"dur":1956,"args":{"seq":[2,14]}}"#,
        r#"{"ph":"M","pid":1,"tid":3,"name":"thread_name","args":{"name":"Ethernet1 - Step 3: Hardware port (ASIC_DB)"}}"#,
        r#"{"ph":"X","pid":1,"tid":3,"name":"Program FAST LINKUP ENABLED","ts":8605097,"dur":2132,"args":{"seq":[15,16],"object_type":"SAI_OBJECT_TYPE_PORT","oid":"oid:0x1000000000002","attr":"SAI_PORT_ATTR_FAST_LINKUP_ENABLED"}}"#,
        r#"{"ph":"X","pid":1,"tid":2,"name":"APPL_DB update pending consumption","ts":8609536,"dur":1959,"args":{"seq":[17,18]}}"#,
        r#"{"ph":"X","pid":1,"tid":3,"name":"Program MTU","ts":8612121,"dur":2066,"args":{"seq":[19,20],"object_type":"SAI_OBJECT_TYPE_PORT","oid":"oid:0x1000000000002","attr":"SAI_PORT_ATTR_MTU"}}"#,
        r#"{"ph":"M","pid":1,"tid":4,"name":"thread_name","args":{"name":"Ethernet1 - Step 3: Hardware router interface (ASIC_DB)"}}"#,
        r#"{"ph":"X","pid":1,"tid":4,"name":"Program MTU","ts":8614362,"dur":1292,"args":{"seq":[21,23],"object_type":"SAI_OBJECT_TYPE_ROUTER_INTERFACE","oid":"oid:0x60000000001fb","attr":"SAI_ROUTER_INTERFACE_ATTR_MTU"}}"#,
        r#"{"ph":"X","pid":1,"tid":2,"name":"APPL_DB update pending consumption","ts":8615124,"dur":3682,"args":{"seq":[22,24]}}"#,
    ];

    const fn at(n: u64) -> At {
        At {
            seq: n,
            ts_ns: n * 1_000,
        }
    }

    const fn us(seq: u64, ts_us: u64) -> At {
        At {
            seq,
            ts_ns: ts_us * 1_000,
        }
    }

    fn span(track: &'static str, name: &'static str, start: At, end: At) -> SpanEvent {
        SpanEvent {
            pipeline: "port",
            key: Arc::from(E1),
            track,
            name,
            start,
            end,
            oid: None,
            attr: None,
            open: false,
        }
    }

    fn sset(track: &'static str, start: At, end: At, oid: &str, attr: &str) -> SpanEvent {
        SpanEvent {
            oid: Some(Arc::from(oid)),
            attr: Some(Arc::from(attr)),
            ..span(track, SPAN_SET, start, end)
        }
    }

    fn asic(name: &'static str, track: &'static str, attr: Option<&str>) -> SpanEvent {
        SpanEvent {
            oid: Some(Arc::from(OP)),
            attr: attr.map(Arc::from),
            ..span(track, name, at(1), at(2))
        }
    }

    fn first() -> SpanEvent {
        span(CONFIG, WF, at(1), at(2))
    }

    fn two_pipelines() -> Vec<SpanEvent> {
        vec![
            first(),
            SpanEvent {
                pipeline: "vlan_member",
                key: Arc::from("Vlan100|Ethernet1"),
                ..span(APPL, QC, at(3), at(4))
            },
        ]
    }

    fn ethernet1() -> Vec<SpanEvent> {
        vec![
            span(CONFIG, WF, us(1, 8_593_362), us(2, 8_601_837)),
            span(APPL, QC, us(2, 8_601_837), us(14, 8_603_793)),
            sset(P, us(15, 8_605_097), us(16, 8_607_229), OP, FL),
            span(APPL, QC, us(17, 8_609_536), us(18, 8_611_495)),
            sset(P, us(19, 8_612_121), us(20, 8_614_187), OP, PM),
            sset(R, us(21, 8_614_362), us(23, 8_615_654), OR, RM),
            span(APPL, QC, us(22, 8_615_124), us(24, 8_618_806)),
        ]
    }

    fn doc(lines: &[&str]) -> String {
        format!("[\n{}\n]\n", lines.join(",\n"))
    }

    fn single(x_line: &str) -> String {
        doc(&[META_PORT, META_E1_CONFIG, x_line])
    }

    #[rstest]
    #[case::empty(vec![], "[\n]\n".to_owned())]
    #[case::ethernet1(ethernet1(), doc(&ETHERNET1))]
    #[case::two_pipelines(two_pipelines(), doc(&TWO_PIPELINES))]
    #[case::open_span(
        vec![SpanEvent { open: true, ..first() }],
        single(r#"{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":1,"dur":1,"args":{"seq":[1,2],"open":true}}"#),
    )]
    #[case::end_before_start(
        vec![SpanEvent { start: at(5), end: at(3), ..first() }],
        single(r#"{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":5,"dur":0,"args":{"seq":[5,3]}}"#),
    )]
    #[case::sub_us_truncation(
        vec![SpanEvent {
            start: At { seq: 1, ts_ns: 1_500 },
            end: At { seq: 2, ts_ns: 3_499 },
            ..first()
        }],
        single(r#"{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":1,"dur":1,"args":{"seq":[1,2]}}"#),
    )]
    fn test_trace_writer(#[case] spans: Vec<SpanEvent>, #[case] expected: String) {
        let mut writer = TraceWriter::new(Vec::new()).unwrap();
        for ev in &spans {
            writer.write(ev).unwrap();
        }
        assert_eq!(
            String::from_utf8(writer.into_inner().unwrap()).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case::set_attr(
        asic(SPAN_SET, R, Some("SAI_ROUTER_INTERFACE_ATTR_ADMIN_V4_STATE")),
        "Ethernet1 - Step 3: Hardware router interface (ASIC_DB)",
        "Program ADMIN V4 STATE"
    )]
    #[case::set_attr_without_attr_marker(
        asic(SPAN_SET, P, Some("CUSTOM_FIELD")),
        "Ethernet1 - Step 3: Hardware port (ASIC_DB)",
        "Program CUSTOM FIELD"
    )]
    #[case::set_without_attr(
        asic(SPAN_SET, P, None),
        "Ethernet1 - Step 3: Hardware port (ASIC_DB)",
        "Program hardware attribute"
    )]
    #[case::create(
        asic(SPAN_CREATE, P, None),
        "Ethernet1 - Step 3: Hardware port (ASIC_DB)",
        "Create hardware object"
    )]
    #[case::remove(
        asic(SPAN_REMOVE, "SAI_OBJECT_TYPE_BRIDGE_PORT", None),
        "Ethernet1 - Step 3: Hardware bridge port (ASIC_DB)",
        "Remove hardware object"
    )]
    #[case::object_type_without_prefix(
        asic(SPAN_CREATE, "CUSTOM_OBJECT", None),
        "Ethernet1 - Step 3: Hardware custom object (ASIC_DB)",
        "Create hardware object"
    )]
    #[case::unknown_span_name(
        span(CONFIG, "Other", at(1), at(2)),
        "Ethernet1 - Step 1: Config saved (CONFIG_DB)",
        "Other"
    )]
    fn test_labels(#[case] ev: SpanEvent, #[case] track: &str, #[case] name: &str) {
        assert_eq!(
            (
                format!("{} - {}", ev.key, track_label(ev.track)),
                span_label(&ev)
            ),
            (track.to_owned(), name.to_owned())
        );
    }

    #[test]
    fn test_spawn_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.json");
        let (mut sink, handle) = spawn_file(&path).unwrap();
        for ev in two_pipelines() {
            sink.span(ev);
        }
        drop(sink);
        drop(handle.join().unwrap().unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), doc(&TWO_PIPELINES));
    }

    #[test]
    fn test_spawn_writer_returns_complete_trace() {
        let (mut sink, handle) = spawn_writer(Vec::new()).unwrap();
        for ev in two_pipelines() {
            sink.span(ev);
        }
        drop(sink);
        let output = handle.join().unwrap().unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), doc(&TWO_PIPELINES));
    }
}
