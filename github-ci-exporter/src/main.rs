//! Entry point: CLI parsing, tracing setup, and the poll loop.

use std::path::PathBuf;

use clap::Parser;
use github_ci_exporter::{
    collector::{self, CycleOutcome, WorkflowCache},
    config::Config,
    github::Client,
    metrics::{Metrics, Publisher},
    server,
};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Parser)]
#[command(name = "github-ci-exporter", version, about)]
struct Cli {
    /// Path to a TOML configuration file.
    #[arg(short, long, env = "GHCI_CONFIG")]
    config: Option<PathBuf>,

    /// Emit logs as JSON.
    #[arg(long, env = "GHCI_LOG_JSON")]
    log_json: bool,

    /// Validate configuration and credentials, then exit.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = EnvFilter::try_from_env("GHCI_LOG")
        .unwrap_or_else(|_| EnvFilter::new("github_ci_exporter=info,warn"));
    let registry = tracing_subscriber::registry().with(filter);
    if cli.log_json {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer()).init();
    }

    let config = Config::load(cli.config.as_deref())?;
    let token = config.resolve_token()?;

    let cache_path = config.state_dir.join("etags.json");
    let client = Client::new(&token, &config.github_api_url, &config.github_graphql_url)?
        .with_cache_file(&cache_path);

    if cli.check {
        info!(
            orgs = ?config.orgs,
            interval = ?config.interval,
            listen = %config.listen,
            "configuration valid"
        );
        return Ok(());
    }

    // Seeds the served set so `/metrics` answers before the first cycle
    // completes. Each cycle publishes a wholly new set in its place.
    let (metrics, prom_registry) = Metrics::new();
    let publisher = Publisher::new(metrics, prom_registry);

    let shutdown = async {
        let ctrl_c = async {
            signal::ctrl_c().await.ok();
        };
        #[cfg(unix)]
        let terminate = async {
            if let Ok(mut sig) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
                sig.recv().await;
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {},
            () = terminate => {},
        }
        info!("shutdown signal received");
    };

    // The poll loop and the HTTP server are peers: the server owns the
    // shutdown signal, and the loop exits when the server does. This keeps
    // metrics being served while a slow collection cycle is in flight.
    let mut server = tokio::spawn(server::serve(config.listen, publisher.clone(), shutdown));

    let mut cache = WorkflowCache::default();
    let mut ticker = tokio::time::interval(config.interval);
    // A cycle that overruns the interval must not queue up a backlog of
    // immediately-firing ticks.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match collector::collect(&client, &config, &publisher, &mut cache).await {
                    Ok(CycleOutcome::Complete) => info!("collection cycle complete"),
                    Ok(CycleOutcome::BypassedLowBudget) => {
                        warn!("collection cycle bypassed to protect the API budget");
                    }
                    Err(error) => {
                        error!(%error, "collection cycle failed");
                        collector::record_failure(&publisher);
                    }
                }
            }
            result = &mut server => {
                match result {
                    Ok(Ok(())) => info!("server stopped"),
                    Ok(Err(error)) => error!(%error, "server failed"),
                    Err(error) => error!(%error, "server task panicked"),
                }
                break;
            }
        }
    }

    if let Err(error) = client.persist_cache() {
        warn!(%error, "failed to persist etag cache on shutdown");
    }

    Ok(())
}
