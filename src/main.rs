//! `ddi` — exactly-once, append-only, stateless Delta→Delta streaming.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::{Parser, Subcommand};
use delta_delta_ingest::config::{Config, ResolvedPipeline};
use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use tokio::signal;
use tokio_util_shim::CancellationToken;
use tracing::{error, info, warn};

/// Minimal cancellation primitive so the binary does not pull in tokio-util for one type.
mod tokio_util_shim {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Default)]
    pub struct CancellationToken(Arc<AtomicBool>);

    impl CancellationToken {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn cancel(&self) {
            self.0.store(true, Ordering::SeqCst);
        }
        pub fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }
}

#[derive(Parser)]
#[command(name = "ddi", version, about, long_about = None)]
struct Cli {
    /// Path to the pipeline config (TOML).
    #[arg(
        short,
        long,
        global = true,
        env = "DDI_CONFIG",
        default_value = "pipelines.toml"
    )]
    config: PathBuf,

    /// Prometheus metrics listen address. Omit to disable.
    #[arg(long, global = true, env = "DDI_METRICS_ADDR")]
    metrics_addr: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Validate the config and exit. Nothing is read or written.
    Validate,
    /// Run every pipeline until each is caught up, then exit.
    Once,
    /// Run continuously (the default).
    Run,
    /// Print each pipeline's resume point and how far behind it is.
    Status,
}

/// Default log filter: our own events at info, and the storage stack quiet.
///
/// `buoyant_kernel` is delta-rs's Delta-kernel implementation, and it logs a full snapshot
/// dump per commit at info. Leaving it out of this filter buries every line the daemon
/// actually emits.
const DEFAULT_LOG: &str =
    "info,deltalake=warn,deltalake_core=warn,buoyant_kernel=warn,datafusion=warn";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_LOG.into()),
        )
        .init();

    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> delta_delta_ingest::Result<()> {
    let cfg = Config::from_path(&cli.config)?;
    let pipelines = cfg.resolve()?;
    info!(
        config = %cli.config.display(),
        pipelines = pipelines.len(),
        "config valid"
    );

    match cli.command.unwrap_or(Command::Run) {
        Command::Validate => {
            for p in &pipelines {
                println!(
                    "{:<24} {} -> {}  (app_id={}, change_policy={:?})",
                    p.name, p.source_uri, p.target_uri, p.app_id, p.change_policy
                );
            }
            println!("\n{} pipeline(s) valid.", pipelines.len());
            Ok(())
        }
        Command::Status => status(pipelines).await,
        Command::Once => run_all(pipelines, cli.metrics_addr, true).await,
        Command::Run => run_all(pipelines, cli.metrics_addr, false).await,
    }
}

async fn status(pipelines: Vec<ResolvedPipeline>) -> delta_delta_ingest::Result<()> {
    for cfg in pipelines {
        let name = cfg.name.clone();
        match Pipeline::open(cfg).await {
            Ok(p) => println!("{name:<24} resume_from={}", p.cursor()),
            Err(e) => println!("{name:<24} ERROR: {e}"),
        }
    }
    Ok(())
}

async fn run_all(
    pipelines: Vec<ResolvedPipeline>,
    metrics_addr: Option<String>,
    once: bool,
) -> delta_delta_ingest::Result<()> {
    let metrics = Metrics::new();
    let token = CancellationToken::new();

    if let Some(addr) = metrics_addr {
        spawn_metrics_server(addr, metrics.clone(), token.clone());
    }

    let mut handles = Vec::new();
    for cfg in pipelines {
        let m = metrics.clone();
        let t = token.clone();
        handles.push(tokio::spawn(async move { drive(cfg, m, t, once).await }));
    }

    // Graceful shutdown: let the in-flight step finish. A step is atomic, so stopping
    // between steps can never leave a half-applied batch.
    let shutdown = {
        let t = token.clone();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            warn!("shutdown requested; finishing the current batch then stopping");
            t.cancel();
        })
    };

    let mut failed = false;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("pipeline failed: {e}");
                failed = true;
                token.cancel();
            }
            Err(e) => {
                error!("pipeline task panicked: {e}");
                failed = true;
                token.cancel();
            }
        }
    }
    shutdown.abort();

    if failed {
        return Err(delta_delta_ingest::Error::Other(
            "one or more pipelines failed".into(),
        ));
    }
    Ok(())
}

/// One tokio task per pipeline. No coordination between them: masterless, like KDI.
async fn drive(
    cfg: ResolvedPipeline,
    metrics: Metrics,
    token: CancellationToken,
    once: bool,
) -> delta_delta_ingest::Result<()> {
    let name = cfg.name.clone();
    let idle = Duration::from_secs(cfg.allowed_latency_secs.max(1));
    let m = metrics.pipeline(&name);

    let mut pipeline = Pipeline::open(cfg).await?;

    loop {
        if token.is_cancelled() {
            info!(pipeline = %name, "stopped");
            return Ok(());
        }

        let outcome = pipeline.step().await;

        // Record head and cursor on every step, including CaughtUp — otherwise the lag
        // gauge freezes at whatever it was when the last batch committed instead of
        // falling to zero once the pipeline has drained the source.
        if let Some(head) = pipeline.source_head_version() {
            m.source_head_version.store(head as i64, Ordering::Relaxed);
        }
        m.cursor_version
            .store(pipeline.cursor().version as i64, Ordering::Relaxed);

        match outcome {
            Ok(StepOutcome::CaughtUp) => {
                if once {
                    info!(pipeline = %name, "caught up");
                    return Ok(());
                }
                tokio::time::sleep(idle).await;
            }
            Ok(StepOutcome::Progressed {
                through_version,
                files,
                rows,
                ..
            }) => {
                m.batches_committed.fetch_add(1, Ordering::Relaxed);
                m.rows_written.fetch_add(rows as u64, Ordering::Relaxed);
                m.files_read.fetch_add(files as u64, Ordering::Relaxed);
                m.last_source_version
                    .store(through_version as i64, Ordering::Relaxed);
            }
            Ok(StepOutcome::Skipped { through_version }) => {
                m.commits_skipped.fetch_add(1, Ordering::Relaxed);
                m.last_source_version
                    .store(through_version as i64, Ordering::Relaxed);
            }
            Err(e) => {
                // A cast failure or a change commit under ChangePolicy::Fail should stop
                // the world, not spin. There is no dead-letter queue by design.
                m.errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }
    }
}

fn spawn_metrics_server(addr: String, metrics: Metrics, token: CancellationToken) {
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("metrics: cannot bind {addr}: {e}");
                return;
            }
        };
        info!(%addr, "metrics listening on /metrics");
        loop {
            if token.is_cancelled() {
                return;
            }
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            let body = metrics.render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            use tokio::io::AsyncWriteExt;
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
}
