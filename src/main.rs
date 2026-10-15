use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use pgshadow::config::Config;
use pgshadow::{admin, app};

/// A never-stale caching proxy for Postgres.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Configuration file (TOML). Values can also come from PGSHADOW_<SECTION>_<KEY>.
    #[arg(short, long, env = "PGSHADOW_CONFIG")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the proxy.
    Serve,
    /// Create or upgrade the pgshadow schema in Postgres (idempotent).
    Install,
    /// Remove the pgshadow schema and all capture triggers. Stop the proxies first.
    Uninstall,
    /// Opt tables in to caching.
    Enable { tables: Vec<String> },
    /// Opt tables out of caching.
    Disable { tables: Vec<String> },
    /// Show which tables are cached.
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Config::load(cli.config.as_deref())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        match cli.cmd {
            Cmd::Serve => serve(cfg).await,
            Cmd::Install => {
                admin::install(&cfg).await?;
                println!("pgshadow installed");
                Ok(())
            }
            Cmd::Uninstall => {
                admin::uninstall(&cfg).await?;
                println!("pgshadow uninstalled");
                Ok(())
            }
            Cmd::Enable { tables } => admin::enable(&cfg, &tables).await,
            Cmd::Disable { tables } => admin::disable(&cfg, &tables).await,
            Cmd::Status => admin::status(&cfg).await,
        }
    })
}

async fn serve(cfg: Config) -> Result<()> {
    pgshadow::observe::init_logging(&cfg.log);
    let listener = tokio::net::TcpListener::bind(&cfg.listen.addr)
        .await
        .with_context(|| format!("binding {}", cfg.listen.addr))?;
    let app = app::start(cfg).await?;
    tracing::info!(addr = %app.addr(&listener), "pgshadow listening");
    let server = app.server.clone();
    tokio::select! {
        r = server.serve(listener) => r?,
        _ = shutdown_signal() => tracing::info!("shutting down"),
    }
    app.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[allow(dead_code)]
fn _assert_send(_: Arc<()>) {}
