//! Replays a Redis recording or config-trace streams through the config analyzer and writes a
//! Chrome JSON trace for <https://ui.perfetto.dev/>. Prints the final pending list.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use config_analyzer_core::config_trace::{self, Stats};
use config_analyzer_core::recording::Recording;
use config_analyzer_loader::load;
use config_analyzer_perfetto::spawn_file;
use config_analyzer_pipeline_manager::instance::Pending;
use config_analyzer_pipeline_manager::pipeline::Pipeline;
use config_analyzer_pipeline_manager::replay::replay;

#[derive(Parser)]
#[command(about = "Replays a Redis recording or config-trace streams and writes a Perfetto trace")]
struct Args {
    /// Input file(s): one JSON recording, or one or more config-trace files.
    #[arg(required = true, num_args = 1..)]
    inputs: Vec<PathBuf>,
    /// Chrome JSON trace to write (truncated if it exists).
    output: PathBuf,
    /// Input format. Config-trace mode accepts multiple input files.
    #[arg(long, value_enum, default_value = "recording")]
    format: InputFormat,
    /// Directory of YANG models to read pipelines from. Required with --pipelines.
    #[arg(long, requires = "pipelines")]
    yang: Option<PathBuf>,
    /// Pipeline definitions. Required with --yang.
    #[arg(long, requires = "yang")]
    pipelines: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum InputFormat {
    #[default]
    Recording,
    ConfigTrace,
}

#[expect(clippy::use_debug, reason = "Pending has no Display impl")]
fn main() -> Result<()> {
    let args = Args::parse();
    let pipelines = load_pipelines(&args)?;
    let pending = replay_files(&pipelines, &args.inputs, args.format, &args.output)?;
    let mut out = std::io::stdout().lock();
    for p in &pending {
        writeln!(out, "{p:?}")?;
    }
    Ok(())
}

fn load_pipelines(args: &Args) -> Result<Vec<&'static Pipeline>> {
    match (&args.yang, &args.pipelines) {
        (Some(yang), Some(pipelines)) => Ok(load(yang, pipelines)?),
        _ => bail!("both --yang and --pipelines are required"),
    }
}

fn replay_files(
    pipelines: &[&'static Pipeline],
    inputs: &[PathBuf],
    format: InputFormat,
    output: &Path,
) -> Result<Vec<Pending>> {
    let recording = read_recording(inputs, format)?;
    let (sink, writer) =
        spawn_file(output).with_context(|| format!("failed to create {}", output.display()))?;
    let runtime = tokio::runtime::Runtime::new().context("failed to start tokio runtime")?;
    let pending = runtime.block_on(replay(pipelines, recording, sink));
    // Worker tasks hold the PerfettoSink clones; dropping the runtime drops them, which lets the
    // writer thread finish the file.
    drop(runtime);
    drop(
        writer
            .join()
            .map_err(|_panic| anyhow!("trace writer thread panicked"))?
            .with_context(|| format!("failed to write {}", output.display()))?,
    );
    Ok(pending?)
}

fn read_recording(inputs: &[PathBuf], format: InputFormat) -> Result<Recording> {
    match format {
        InputFormat::Recording => {
            let [input] = inputs else {
                bail!("recording format requires exactly one input file")
            };
            let text = fs::read_to_string(input)
                .with_context(|| format!("failed to read {}", input.display()))?;
            serde_json::from_str(&text)
                .with_context(|| format!("failed to parse {}", input.display()))
        }
        InputFormat::ConfigTrace => {
            let texts = inputs
                .iter()
                .map(|input| {
                    fs::read_to_string(input)
                        .with_context(|| format!("failed to read {}", input.display()))
                })
                .collect::<Result<Vec<_>>>()?;
            let text_refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            let (recording, stats) = config_trace::parse(&text_refs);
            report_stats(stats)?;
            Ok(recording)
        }
    }
}

fn report_stats(stats: Stats) -> Result<()> {
    if stats.skipped > 0 || stats.unpaired > 0 || stats.unmapped > 0 {
        writeln!(
            std::io::stderr().lock(),
            "config-trace: {} line(s), {} skipped, {} unpaired SAI event(s), {} unmapped SAI event(s)",
            stats.lines,
            stats.skipped,
            stats.unpaired,
            stats.unmapped
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use config_analyzer_pipeline_manager::pipeline::NameRule;
    use serde_json::json;

    static TEST_PORT: Pipeline = Pipeline {
        name: "port",
        config_table: Some("PORT"),
        appl_table: "PORT_TABLE",
        fields: &["mtu"],
        field_alias: &[],
        asic_slots: &["SAI_OBJECT_TYPE_PORT", "SAI_OBJECT_TYPE_ROUTER_INTERFACE"],
        object_names: NameRule {
            prefix: "Ethernet",
            digits: true,
        },
    };

    #[test]
    fn test_pipeline_metadata_is_required() {
        let args = Args {
            inputs: Vec::new(),
            output: PathBuf::new(),
            format: InputFormat::Recording,
            yang: None,
            pipelines: None,
        };
        assert_eq!(
            load_pipelines(&args).unwrap_err().to_string(),
            "both --yang and --pipelines are required"
        );
    }

    fn event(offset_s: f64, db: &str, cmd: &str, key: &str, args: &[&str]) -> serde_json::Value {
        json!({
            "offset_s": offset_s,
            "db": db,
            "cmd": cmd,
            "key": key,
            "args": args,
            "fields": {},
            "client": "test",
        })
    }

    #[test]
    fn test_replay() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("recording.json");
        let output = dir.path().join("trace.json");
        let recording = json!({
            "case": "test",
            "port_map": {"": "", "oid:0x1": "Ethernet1"},
            "events": [
                event(1.0, "CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"]),
                event(1.000_01, "APPL_DB", "HSET", "_PORT_TABLE:Ethernet1", &["mtu", "9100"]),
                event(1.000_02, "COUNTERS_DB", "HSET", "COUNTERS:oid:0x1", &["x", "1"]),
                event(1.000_03, "APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[]),
            ],
        });
        fs::write(&input, recording.to_string()).unwrap();

        let pending =
            replay_files(&[&TEST_PORT], &[input], InputFormat::Recording, &output).unwrap();

        assert_eq!(pending, vec![]);
        assert_eq!(
            fs::read_to_string(&output).unwrap(),
            r#"[
{"ph":"M","pid":1,"name":"process_name","args":{"name":"Port configuration"}},
{"ph":"M","pid":1,"tid":1,"name":"thread_name","args":{"name":"Ethernet1 - Step 1: Config saved (CONFIG_DB)"}},
{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":1000000,"dur":10,"args":{"seq":[0,1]}},
{"ph":"M","pid":1,"tid":2,"name":"thread_name","args":{"name":"Ethernet1 - Step 2: Sent to orchagent (APPL_DB)"}},
{"ph":"X","pid":1,"tid":2,"name":"APPL_DB update pending consumption","ts":1000010,"dur":20,"args":{"seq":[1,3]}}
]
"#
        );
    }

    #[test]
    fn test_replay_config_trace_from_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let input_a = dir.path().join("config-trace-portmgrd.rec");
        let input_b = dir.path().join("config-trace.rec");
        let output = dir.path().join("trace.json");
        fs::write(
            &input_a,
            concat!(
                "2026-10-01.03:38:55.000000|portmgrd|recv|PORT|Ethernet1|-|CONFIG_DB:SET:mtu\n",
                "2026-10-01.03:38:55.000010|portmgrd|appl|PORT_TABLE|Ethernet1|-|mtu\n",
            ),
        )
        .unwrap();
        fs::write(
            &input_b,
            "2026-10-01.03:38:55.000020|orchagent|recv|PORT_TABLE|Ethernet1|-|APPL_DB:SET:mtu\n",
        )
        .unwrap();

        let pending = replay_files(
            &[&TEST_PORT],
            &[input_a, input_b],
            InputFormat::ConfigTrace,
            &output,
        )
        .unwrap();

        assert_eq!(pending, vec![]);
        assert_eq!(
            fs::read_to_string(&output).unwrap(),
            r#"[
{"ph":"M","pid":1,"name":"process_name","args":{"name":"Port configuration"}},
{"ph":"M","pid":1,"tid":1,"name":"thread_name","args":{"name":"Ethernet1 - Step 1: Config saved (CONFIG_DB)"}},
{"ph":"X","pid":1,"tid":1,"name":"Waiting for config to be forwarded","ts":0,"dur":10,"args":{"seq":[0,1]}},
{"ph":"M","pid":1,"tid":2,"name":"thread_name","args":{"name":"Ethernet1 - Step 2: Sent to orchagent (APPL_DB)"}},
{"ph":"X","pid":1,"tid":2,"name":"APPL_DB update pending consumption","ts":10,"dur":10,"args":{"seq":[1,2]}}
]
"#
        );
    }

    #[test]
    fn test_recording_format_requires_one_input() {
        assert_eq!(
            read_recording(&[], InputFormat::Recording)
                .unwrap_err()
                .to_string(),
            "recording format requires exactly one input file"
        );
    }

    #[test]
    fn test_cli_accepts_multiple_config_trace_files_before_output() {
        let args = Args::try_parse_from([
            "replay",
            "--format",
            "config-trace",
            "orchagent.rec",
            "portmgrd.rec",
            "trace.json",
        ])
        .unwrap();

        assert_eq!(
            (args.inputs, args.output, args.format),
            (
                vec![
                    PathBuf::from("orchagent.rec"),
                    PathBuf::from("portmgrd.rec")
                ],
                PathBuf::from("trace.json"),
                InputFormat::ConfigTrace,
            )
        );
    }
}
