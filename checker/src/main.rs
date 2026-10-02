//! Replays a Redis recording or config-trace streams through the config analyzer and checks that
//! every tracked instance reached a terminal state, i.e. the final pending list is empty. Prints
//! any pending items and exits non-zero if there are some.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use config_analyzer_core::config_trace::{self, Stats};
use config_analyzer_core::recording::Recording;
use config_analyzer_core::{SpanEvent, SpanSink};
use config_analyzer_loader::load;
use config_analyzer_pipeline_manager::instance::Pending;
use config_analyzer_pipeline_manager::pipeline::Pipeline;
use config_analyzer_pipeline_manager::replay::replay;

#[derive(Parser)]
#[command(about = "Checks that every config change in a recording completed")]
struct Args {
    /// Input file(s): one JSON recording, or one or more config-trace files.
    #[arg(required = true, num_args = 1..)]
    inputs: Vec<PathBuf>,
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

#[derive(Clone)]
struct NullSink;

impl SpanSink for NullSink {
    fn span(&mut self, _ev: SpanEvent) {}
}

#[expect(clippy::use_debug, reason = "Pending has no Display impl")]
fn main() -> Result<()> {
    let args = Args::parse();
    let pipelines = load_pipelines(&args)?;
    let pending = check_inputs(&pipelines, &args.inputs, args.format)?;
    let mut out = std::io::stdout().lock();
    for p in &pending {
        writeln!(out, "{p:?}")?;
    }
    if !pending.is_empty() {
        bail!(
            "{} pending item(s) in {}",
            pending.len(),
            args.inputs
                .iter()
                .map(|input| input.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    writeln!(out, "all transitions completed")?;
    Ok(())
}

fn load_pipelines(args: &Args) -> Result<Vec<&'static Pipeline>> {
    match (&args.yang, &args.pipelines) {
        (Some(yang), Some(pipelines)) => Ok(load(yang, pipelines)?),
        _ => bail!("both --yang and --pipelines are required"),
    }
}

fn check_inputs(
    pipelines: &[&'static Pipeline],
    inputs: &[PathBuf],
    format: InputFormat,
) -> Result<Vec<Pending>> {
    let recording = read_recording(inputs, format)?;
    let runtime = tokio::runtime::Runtime::new().context("failed to start tokio runtime")?;
    Ok(runtime.block_on(replay(pipelines, recording, NullSink))?)
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
    use config_analyzer_pipeline_manager::instance::PendingReason;
    use config_analyzer_pipeline_manager::pipeline::NameRule;
    use serde_json::json;
    use std::sync::Arc;

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

    fn check_events(events: &[serde_json::Value]) -> Result<Vec<Pending>> {
        let dir = tempfile::tempdir()?;
        let input = dir.path().join("recording.json");
        let recording = json!({"port_map": {"oid:0x1": "Ethernet1"}, "events": events});
        fs::write(&input, recording.to_string())?;
        check_inputs(&[&TEST_PORT], &[input], InputFormat::Recording)
    }

    fn config_write() -> serde_json::Value {
        event(1.0, "CONFIG_DB", "HSET", "PORT|Ethernet1", &["mtu", "9100"])
    }

    #[test]
    fn test_check_completed() {
        let events = [
            config_write(),
            event(
                1.000_01,
                "APPL_DB",
                "HSET",
                "_PORT_TABLE:Ethernet1",
                &["mtu", "9100"],
            ),
            event(1.000_02, "APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[]),
        ];
        assert_eq!(check_events(&events).unwrap(), vec![]);
    }

    #[test]
    fn test_check_pending() {
        let events = [
            config_write(),
            event(
                1.000_01,
                "APPL_DB",
                "HSET",
                "_PORT_TABLE:Ethernet1",
                &["mtu", "9100"],
            ),
        ];
        assert_eq!(
            check_events(&events).unwrap(),
            vec![Pending {
                pipeline: Some("port"),
                key: Arc::from("Ethernet1"),
                reason: PendingReason::ApplQueued,
            }]
        );
    }

    #[test]
    fn test_check_invalid_offset() {
        let events = [event(-1.0, "APPL_DB", "DEL", "_PORT_TABLE:Ethernet1", &[])];
        assert_eq!(
            check_events(&events).unwrap_err().to_string(),
            "event 0: invalid offset_s -1"
        );
    }

    #[test]
    fn test_check_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("missing.json");
        assert_eq!(
            check_inputs(&[&TEST_PORT], &[input.clone()], InputFormat::Recording)
                .unwrap_err()
                .to_string(),
            format!("failed to read {}", input.display())
        );
    }

    #[test]
    fn test_check_config_trace_from_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let input_a = dir.path().join("config-trace-portmgrd.rec");
        let input_b = dir.path().join("config-trace.rec");
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

        assert_eq!(
            check_inputs(&[&TEST_PORT], &[input_a, input_b], InputFormat::ConfigTrace).unwrap(),
            vec![]
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
    fn test_cli_accepts_multiple_config_trace_files() {
        let args = Args::try_parse_from([
            "checker",
            "--format",
            "config-trace",
            "orchagent.rec",
            "portmgrd.rec",
        ])
        .unwrap();

        assert_eq!(
            (args.inputs, args.format),
            (
                vec![
                    PathBuf::from("orchagent.rec"),
                    PathBuf::from("portmgrd.rec")
                ],
                InputFormat::ConfigTrace,
            )
        );
    }
}
