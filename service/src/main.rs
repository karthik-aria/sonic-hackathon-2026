mod db_layout;
mod http;
mod monitor;
mod redis;
mod store;
mod window;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use config_analyzer_loader::load;

use crate::db_layout::Layout;
use crate::redis::read_oid_map;
use crate::store::TraceStore;

#[derive(Debug, Parser)]
#[command(about = "Capture and serve Perfetto traces for SONiC configuration changes")]
struct Args {
    #[arg(long, default_value = "/var/run/redis/sonic-db/database_config.json")]
    db_config: PathBuf,
    /// Directory containing the `SONiC` YANG modules referenced by the pipeline file.
    #[arg(long, default_value = "/usr/models/yang")]
    yang: PathBuf,
    /// Pipeline metadata file to load together with the YANG models.
    #[arg(long, default_value = "/etc/config-analyzer/pipelines.yaml")]
    pipelines: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8099")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 500)]
    settle_ms: u64,
    #[arg(long, default_value_t = 60)]
    timeout_s: u64,
    #[arg(long, default_value_t = 300)]
    cap_s: u64,
    #[arg(long, default_value_t = 5)]
    keep: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let pipelines = Arc::new(load(&args.yang, &args.pipelines).with_context(|| {
        format!(
            "loading pipelines from {} using YANG models in {}",
            args.pipelines.display(),
            args.yang.display()
        )
    })?);
    let layout = Arc::new(Layout::load(&args.db_config)?);
    let store = Arc::new(Mutex::new(TraceStore::new(args.keep)));
    let initial_oid_map = match read_oid_map(&layout) {
        Ok(oid_map) => oid_map,
        Err(error) => {
            store
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .add_warning(format!("initial OID map unavailable: {error:#}"));
            std::collections::BTreeMap::new()
        }
    };
    let active = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel();

    for source in layout.monitor_sources()? {
        let tx = tx.clone();
        let label = source
            .databases
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        drop(
            thread::Builder::new()
                .name(format!("redis-monitor-{label}"))
                .spawn({
                    let active = Arc::clone(&active);
                    let pipelines = Arc::clone(&pipelines);
                    move || monitor::run(&source, &tx, &active, pipelines.as_slice())
                })
                .with_context(|| format!("starting MONITOR reader for {label}"))?,
        );
    }
    drop(tx);

    let manager_store = Arc::clone(&store);
    let manager_layout = Arc::clone(&layout);
    let pipelines = Arc::clone(&pipelines);
    drop(
        thread::Builder::new()
            .name("capture-window-manager".to_owned())
            .spawn(move || {
                window::run(
                    &manager_layout,
                    &manager_store,
                    &initial_oid_map,
                    pipelines.as_slice(),
                    &rx,
                    &active,
                    window::WindowConfig {
                        settle: Duration::from_millis(args.settle_ms),
                        timeout: Duration::from_secs(args.timeout_s),
                        cap: Duration::from_secs(args.cap_s),
                    },
                );
            })
            .context("starting capture window manager")?,
    );

    let listener = std::net::TcpListener::bind(args.listen)
        .with_context(|| format!("binding debug HTTP listener at {}", args.listen))?;
    http::serve(&listener, &store)?;
    Ok(())
}
