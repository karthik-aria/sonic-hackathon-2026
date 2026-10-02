use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;

use crate::store::{TraceStatus, TraceStore, TraceSummary};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

struct Response {
    status: &'static str,
    content_type: &'static str,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

#[derive(Serialize)]
struct Health<'a> {
    monitor_connected: bool,
    sources: &'a std::collections::BTreeMap<String, bool>,
    active_window: Option<TraceSummary>,
    entries_held: usize,
    warnings: &'a [String],
}

pub(crate) fn serve(listener: &TcpListener, store: &Arc<Mutex<TraceStore>>) -> io::Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(store);
        drop(
            thread::Builder::new()
                .name("debug-http-client".to_owned())
                .spawn(move || {
                    drop(handle_client(stream, &store));
                })?,
        );
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, store: &Mutex<TraceStore>) -> io::Result<()> {
    stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    let Some((method, path)) = read_request(&mut stream)? else {
        return Ok(());
    };
    let response = route(&method, &path, &lock_store(store));
    write_response(&mut stream, response)
}

fn read_request(stream: &mut TcpStream) -> io::Result<Option<(String, String)>> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete HTTP request",
                ))
            };
        }
        if bytes.len().saturating_add(count) > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request headers are too large",
            ));
        }
        let Some(read_bytes) = chunk.get(..count) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request read exceeded buffer length",
            ));
        };
        bytes.extend_from_slice(read_bytes);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let first_line = bytes
        .split(|byte| *byte == b'\n')
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP request line"))?;
    let first_line = std::str::from_utf8(first_line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .trim_end_matches('\r');
    let mut parts = first_line.split_ascii_whitespace();
    let method = parts.next();
    let path = parts.next();
    let version = parts.next();
    if parts.next().is_some() || !matches!(version, Some("HTTP/1.0" | "HTTP/1.1")) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed HTTP request line",
        ));
    }
    match (method, path) {
        (Some(method), Some(path)) => Ok(Some((method.to_owned(), path.to_owned()))),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed HTTP request line",
        )),
    }
}

fn write_response(stream: &mut TcpStream, response: Response) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\n",
        response.status,
        response.content_type,
        response.body.len()
    )?;
    for (name, value) in response.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&response.body)
}

fn route(method: &str, path: &str, store: &TraceStore) -> Response {
    if method != "GET" {
        return text_response("405 Method Not Allowed", "method not allowed");
    }
    let path = path.split_once('?').map_or(path, |(path, _query)| path);
    match path {
        "/" => match render_index(store) {
            Ok(html) => html_response(html),
            Err(_error) => text_response("500 Internal Server Error", "could not render index"),
        },
        "/traces" => json_response(&store.summaries()),
        "/healthz" => {
            let sources = store.source_states();
            let health = Health {
                monitor_connected: store.monitors_connected(),
                sources,
                active_window: store.active().map(TraceSummary::from),
                entries_held: store.completed_count(),
                warnings: store.warnings(),
            };
            json_response(&health)
        }
        _ => {
            if let Some(id) = path
                .strip_prefix("/traces/")
                .and_then(|name| name.strip_suffix("/events.json"))
                .and_then(|name| name.parse::<u64>().ok())
            {
                let Some(recording) = store.recording(id) else {
                    return text_response("404 Not Found", "normalized events not found");
                };
                return json_response(recording);
            }
            let Some(id) = path
                .strip_prefix("/traces/")
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<u64>().ok())
            else {
                return text_response("404 Not Found", "not found");
            };
            let Some(trace) = store.trace(id) else {
                return text_response("404 Not Found", "trace not found");
            };
            Response {
                status: "200 OK",
                content_type: "application/json; charset=utf-8",
                headers: Vec::new(),
                body: trace,
            }
        }
    }
}

const DASHBOARD_START: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Config change progress</title>
  <style>
    :root { color-scheme: light; font-family: system-ui, sans-serif; color: #172033; background: #f3f6fb; }
    * { box-sizing: border-box; }
    body { margin: 0; }
    main { max-width: 1040px; margin: 0 auto; padding: 32px 20px 48px; }
    header, .section-head { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
    header { margin-bottom: 28px; }
    h1, h2, p { margin: 0; }
    h1 { font-size: clamp(1.6rem, 4vw, 2.2rem); letter-spacing: -0.03em; }
    h2 { font-size: 1.1rem; }
    .eyebrow, .muted, .meta, .empty { color: #617089; }
    .eyebrow { margin-bottom: 6px; font-size: .78rem; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; }
    .muted { margin-top: 8px; font-size: .92rem; }
    .panel { border: 1px solid #dce3ee; border-radius: 14px; background: white; box-shadow: 0 3px 12px #24324a0a; }
    .panel { padding: 20px; }
    .section-head { margin-bottom: 16px; }
    .refresh, .monitor, .status { display: inline-flex; align-items: center; gap: 8px; border-radius: 999px; padding: 6px 10px; font-size: .82rem; font-weight: 650; }
    .refresh { background: #eef2f8; color: #526078; }
    .monitor.online, .status--complete { background: #e8f7ee; color: #17683a; }
    .monitor.offline, .status--stuck { background: #fff0ed; color: #9a3020; }
    .status--in-progress { background: #eaf2ff; color: #2457a6; }
    .status--capped { background: #fff5dc; color: #805b00; }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; min-width: 680px; border-collapse: collapse; text-align: left; }
    th, td { padding: 12px 14px; border-bottom: 1px solid #e7ebf2; vertical-align: top; }
    th { color: #617089; font-size: .78rem; letter-spacing: .04em; text-transform: uppercase; }
    tbody tr:last-child td { border-bottom: 0; }
    .capture-meta { display: block; margin-top: 6px; color: #617089; font-size: .82rem; }
    .status { margin-top: 8px; }
    .entities { margin: 0; padding-left: 18px; }
    .pending { margin-top: 8px; color: #617089; font-size: .85rem; }
    .pending ul { margin: 4px 0 0; padding-left: 18px; }
    .empty-cell { padding: 28px 16px; }
    .empty { padding: 20px 16px; border: 1px dashed #cbd5e3; border-radius: 10px; color: #617089; text-align: center; }
    .action-link { display: inline-block; padding: 0; color: #2457a6; background: none; font: inherit; text-decoration: underline; }
    button.action-link { border: 0; cursor: pointer; }
    .unavailable { color: #8793a6; font-size: .88rem; }
    a, button { color: #2457a6; }
    button { border: 0; padding: 0; background: none; font: inherit; cursor: pointer; text-decoration: underline; }
    .trace-viewer { position: fixed; inset: 0; z-index: 10; display: flex; flex-direction: column; background: white; }
    .trace-viewer[hidden] { display: none; }
    .viewer-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 12px 18px; border-bottom: 1px solid #dce3ee; }
    .viewer-title { font-weight: 700; }
    .viewer-status { margin-left: 12px; color: #617089; font-size: .9rem; }
    .viewer-header button { border: 1px solid #cbd5e3; border-radius: 8px; padding: 8px 12px; background: white; text-decoration: none; }
    #perfetto-frame { width: 100%; flex: 1; border: 0; }
    footer { display: flex; gap: 16px; margin-top: 20px; font-size: .88rem; }
    @media (max-width: 620px) { header { align-items: flex-start; flex-direction: column; } .section-head { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <main>
    <header>
      <div><p class="eyebrow">SONiC configuration pipelines</p><h1>Config change progress</h1><p class="muted">Review impacted entities, normalized Redis events, and pipeline timing.</p></div>
      <span class="monitor "#;

const DASHBOARD_AFTER_HEALTH: &str = r#"
    </header>
    <section class="panel" aria-labelledby="recent-heading">
      <div class="section-head"><h2 id="recent-heading">Recent config changes</h2><span class="refresh">Refreshes every 2 seconds; paused while viewing</span></div>
      <div class="table-wrap">
        <table>
          <thead><tr><th scope="col">Timestamp</th><th scope="col">Impacted Config Entities</th><th scope="col">Normalized Events</th><th scope="col">Perfetto</th></tr></thead>
          <tbody>
"#;

const DASHBOARD_END: &str = r#"
          </tbody>
        </table>
      </div>
    </section>
    <footer><a href="/traces">JSON index</a><a href="/healthz">Health details</a><span class="muted">Recent captures are held in memory only.</span></footer>
  </main>
  <section id="trace-viewer" class="trace-viewer" role="dialog" aria-modal="true" aria-labelledby="trace-viewer-title" hidden>
    <div class="viewer-header">
      <div><span id="trace-viewer-title" class="viewer-title">Perfetto trace viewer</span><span id="trace-viewer-status" class="viewer-status" role="status"></span></div>
      <button id="trace-viewer-close" type="button" onclick="closeTraceViewer()">Back to captures</button>
    </div>
    <iframe id="perfetto-frame" src="https://ui.perfetto.dev/#!/?mode=embedded" title="Perfetto trace viewer"></iframe>
  </section>
  <script>
    const PERFETTO_ORIGIN = 'https://ui.perfetto.dev';
    const viewer = document.getElementById('trace-viewer');
    const viewerStatus = document.getElementById('trace-viewer-status');
    const perfettoFrame = document.getElementById('perfetto-frame');
    let viewerOpen = false;
    let perfettoReady;

    document.querySelectorAll("time[data-timestamp]").forEach((element) => {
      element.textContent = new Date(Number(element.dataset.timestamp)).toLocaleString();
    });

    function waitForPerfetto() {
      if (perfettoReady) return perfettoReady;
      perfettoReady = new Promise((resolve, reject) => {
        const timeout = window.setTimeout(() => {
          window.clearInterval(ping);
          window.removeEventListener('message', onMessage);
          perfettoReady = undefined;
          reject(new Error('Perfetto did not become ready'));
        }, 30000);
        const onMessage = (event) => {
          if (event.source !== perfettoFrame.contentWindow || event.origin !== PERFETTO_ORIGIN || event.data !== 'PONG') return;
          window.clearTimeout(timeout);
          window.clearInterval(ping);
          window.removeEventListener('message', onMessage);
          resolve();
        };
        window.addEventListener('message', onMessage);
        const ping = window.setInterval(() => {
          perfettoFrame.contentWindow.postMessage('PING', PERFETTO_ORIGIN);
        }, 100);
        perfettoFrame.contentWindow.postMessage('PING', PERFETTO_ORIGIN);
      });
      return perfettoReady;
    }

    async function openTrace(id) {
      viewerOpen = true;
      viewer.hidden = false;
      viewerStatus.textContent = 'Loading trace...';
      document.getElementById('trace-viewer-close').focus();
      try {
        const response = await fetch('/traces/' + id + '.json', {cache: 'no-store'});
        if (!response.ok) throw new Error('Could not fetch trace #' + id + ' (' + response.status + ')');
        const buffer = await response.arrayBuffer();
        await waitForPerfetto();
        perfettoFrame.contentWindow.postMessage({perfetto: {buffer, title: 'Capture #' + id, keepApiOpen: true}}, PERFETTO_ORIGIN);
        viewerStatus.textContent = 'Trace sent to Perfetto';
      } catch (error) {
        viewerStatus.textContent = error instanceof Error ? error.message : String(error);
      }
    }

    function closeTraceViewer() {
      viewerOpen = false;
      viewer.hidden = true;
      window.location.reload();
    }

    window.setInterval(() => {
      if (!viewerOpen) window.location.reload();
    }, 2000);
  </script>
</body>
</html>
"#;

fn render_index(store: &TraceStore) -> Result<String, std::fmt::Error> {
    use std::fmt::Write as FmtWrite;

    let summaries = store.summaries();
    let monitors_connected = store.monitors_connected();
    let (monitor_class, monitor_label) = if monitors_connected {
        ("online", "Connected")
    } else {
        ("offline", "Disconnected")
    };
    let mut html = String::from(DASHBOARD_START);
    write!(
        html,
        "{monitor_class}\">Redis monitors: {monitor_label}</span>"
    )?;
    html.push_str(DASHBOARD_AFTER_HEALTH);
    if summaries.is_empty() {
        html.push_str(
            "<tr><td class=\"empty-cell\" colspan=\"4\"><p class=\"empty\">Waiting for a tracked config change. New captures will appear here automatically.</p></td></tr>",
        );
    }
    for entry in summaries.iter().rev() {
        render_capture(
            &mut html,
            entry,
            store.recording(entry.id).is_some(),
            store.has_trace(entry.id),
        )?;
    }
    html.push_str(DASHBOARD_END);
    Ok(html)
}

fn render_capture(
    html: &mut String,
    entry: &TraceSummary,
    has_recording: bool,
    has_trace: bool,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as FmtWrite;

    let status_class = entry.status.label().replace('_', "-");
    let status_label = match entry.status {
        TraceStatus::InProgress => "In progress",
        TraceStatus::Complete => "Complete",
        TraceStatus::Stuck => "Stuck",
        TraceStatus::Capped => "Time limit reached",
    };
    write!(
        html,
        "<tr><td><time data-timestamp=\"{}\">{}</time><span class=\"capture-meta\">Capture #{}</span><span class=\"status status--{}\">{}</span></td><td>",
        entry.opened_at_ms, entry.opened_at_ms, entry.id, status_class, status_label
    )?;
    if entry.keys.is_empty() {
        html.push_str("<span class=\"unavailable\">No tracked entities identified</span>");
    } else {
        html.push_str("<ul class=\"entities\">");
        for key in &entry.keys {
            write!(html, "<li>{}</li>", escape_html(key))?;
        }
        html.push_str("</ul>");
    }
    if !entry.pending.is_empty() {
        html.push_str("<details class=\"pending\"><summary>Pending work</summary><ul>");
        for item in &entry.pending {
            write!(html, "<li>{}</li>", escape_html(item))?;
        }
        html.push_str("</ul></details>");
    }
    html.push_str("</td><td>");
    if has_recording {
        write!(
            html,
            "<a class=\"action-link\" href=\"/traces/{}/events.json\" target=\"_blank\" rel=\"noopener\">Normalized events</a>",
            entry.id,
        )?;
    } else {
        html.push_str("<span class=\"unavailable\">Available when capture completes</span>");
    }
    html.push_str("</td><td>");
    if has_trace {
        write!(
            html,
            "<button class=\"action-link\" type=\"button\" onclick=\"openTrace({})\">Open in Perfetto</button>",
            entry.id
        )?;
    } else {
        html.push_str("<span class=\"unavailable\">Not available</span>");
    }
    html.push_str("</td></tr>");
    Ok(())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn json_response<T>(value: &T) -> Response
where
    T: Serialize,
{
    match serde_json::to_vec(value) {
        Ok(body) => Response {
            status: "200 OK",
            content_type: "application/json; charset=utf-8",
            headers: Vec::new(),
            body,
        },
        Err(_error) => text_response("500 Internal Server Error", "could not serialize response"),
    }
}

const fn html_response(body: String) -> Response {
    Response {
        status: "200 OK",
        content_type: "text/html; charset=utf-8",
        headers: Vec::new(),
        body: body.into_bytes(),
    }
}

fn text_response(status: &'static str, body: &str) -> Response {
    Response {
        status,
        content_type: "text/plain; charset=utf-8",
        headers: Vec::new(),
        body: body.as_bytes().to_vec(),
    }
}

fn lock_store(store: &Mutex<TraceStore>) -> std::sync::MutexGuard<'_, TraceStore> {
    store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use config_analyzer_core::recording::{Event, Recording};

    fn store_with_trace() -> TraceStore {
        let mut store = TraceStore::new(5);
        store.finish(crate::store::TraceEntry {
            id: 7,
            opened_at_ms: 100,
            closed_at_ms: Some(200),
            keys: vec!["Ethernet1<script>".to_owned()],
            status: TraceStatus::Complete,
            pending: Vec::new(),
            trace: Some(b"[]\n".to_vec()),
            recording: Some(Recording {
                port_map: std::collections::BTreeMap::from([(
                    "oid:0x1".to_owned(),
                    "Ethernet1".to_owned(),
                )]),
                events: vec![Event {
                    offset_s: 0.0,
                    db: "CONFIG_DB".to_owned(),
                    cmd: "HSET".to_owned(),
                    key: "PORT|Ethernet1".to_owned(),
                    args: vec!["mtu".to_owned(), "9100".to_owned()],
                    client: "127.0.0.1:6379".to_owned(),
                }],
            }),
        });
        store
    }

    #[test]
    fn test_routes_return_complete_trace_json_without_forcing_download() {
        let store = store_with_trace();
        let index = route("GET", "/traces", &store);
        assert_eq!(index.status, "200 OK");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&index.body).unwrap(),
            serde_json::json!([{
                "id": 7_u64,
                "opened_at_ms": 100_u64,
                "closed_at_ms": 200_u64,
                "keys": ["Ethernet1<script>"],
                "status": "complete",
                "pending": []
            }])
        );

        let trace = route("GET", "/traces/7.json", &store);
        assert_eq!(trace.status, "200 OK");
        assert_eq!(trace.body, b"[]\n");
        assert!(trace.headers.is_empty());

        let events = route("GET", "/traces/7/events.json", &store);
        assert_eq!(events.status, "200 OK");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&events.body).unwrap(),
            serde_json::json!({
                "port_map": {"oid:0x1": "Ethernet1"},
                "events": [{
                    "offset_s": 0.0_f64,
                    "db": "CONFIG_DB",
                    "cmd": "HSET",
                    "key": "PORT|Ethernet1",
                    "args": ["mtu", "9100"],
                    "client": "127.0.0.1:6379"
                }]
            })
        );
    }

    #[test]
    fn test_health_reports_connection_states_and_capture_count() {
        let mut store = store_with_trace();
        store.set_source("CONFIG_DB".to_owned(), true);
        let health = route("GET", "/healthz", &store);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&health.body).unwrap(),
            serde_json::json!({
                "monitor_connected": true,
                "sources": {"CONFIG_DB": true},
                "active_window": null,
                "entries_held": 1_usize,
                "warnings": []
            })
        );
    }

    #[test]
    fn test_index_escapes_untrusted_values_and_renders_trace_table() {
        let page = route("GET", "/", &store_with_trace());
        let html = String::from_utf8(page.body).unwrap();
        assert!(html.contains("Ethernet1&lt;script&gt;"));
        assert!(!html.contains("Ethernet1<script>"));
        assert!(html.contains("<th scope=\"col\">Timestamp</th>"));
        assert!(html.contains("<th scope=\"col\">Impacted Config Entities</th>"));
        assert!(html.contains("href=\"/traces/7/events.json\""));
        assert!(html.contains(">Normalized events</a>"));
        assert!(html.contains("onclick=\"openTrace(7)\">Open in Perfetto</button>"));
        assert!(html.contains("<iframe id=\"perfetto-frame\""));
        assert!(html.contains("https://ui.perfetto.dev/#!/?mode=embedded"));
        assert!(html.contains("keepApiOpen: true"));
        assert!(!html.contains("window.open("));
        assert!(!html.contains("Download trace"));
    }

    #[test]
    fn test_index_shows_active_capture_progress() {
        let mut store = TraceStore::new(5);
        store.set_active(crate::store::TraceEntry {
            id: 8,
            opened_at_ms: 300,
            closed_at_ms: None,
            keys: vec!["Ethernet2".to_owned()],
            status: TraceStatus::InProgress,
            pending: vec!["APPL_DB: Ethernet2".to_owned()],
            trace: None,
            recording: None,
        });

        let page = route("GET", "/", &store);
        let html = String::from_utf8(page.body).unwrap();

        assert!(html.contains("In progress"));
        assert!(html.contains("Ethernet2"));
        assert!(html.contains("APPL_DB: Ethernet2"));
        assert!(html.contains("Refreshes every 2 seconds; paused while viewing"));
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test code")]
    fn test_http_request_returns_a_complete_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _address) = listener.accept().unwrap();
        let store = Mutex::new(TraceStore::new(5));
        let server_thread = thread::spawn(move || handle_client(server, &store).unwrap());

        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = Vec::new();
        let bytes_read = client.read_to_end(&mut response).unwrap();
        assert_eq!(bytes_read, response.len());
        server_thread.join().unwrap();

        let body = r#"{"monitor_connected":false,"sources":{},"active_window":null,"entries_held":0,"warnings":[]}"#;
        assert_eq!(
            String::from_utf8(response).unwrap(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\n\r\n{body}",
                body.len()
            )
        );
    }

    #[rstest::rstest]
    #[case::unknown_path("GET", "/missing", "404 Not Found")]
    #[case::unknown_trace("GET", "/traces/8.json", "404 Not Found")]
    #[case::unknown_normalized_events("GET", "/traces/8/events.json", "404 Not Found")]
    #[case::wrong_method("POST", "/healthz", "405 Method Not Allowed")]
    fn test_error_routes(#[case] method: &str, #[case] path: &str, #[case] expected: &str) {
        assert_eq!(route(method, path, &store_with_trace()).status, expected);
    }
}
