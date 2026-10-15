//! Wiring: everything a running proxy needs. Also used by the integration tests to run
//! a proxy in-process.

use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::admin;
use crate::cache::Store;
use crate::config::Config;
use crate::net;
use crate::observe::{self, Stats};
use crate::server::Server;
use crate::session::Shared;
use crate::validator::Validator;

pub struct App {
    pub server: Arc<Server>,
    pub shared: Arc<Shared>,
}

pub async fn start(cfg: Config) -> Result<App> {
    let tls = net::client_tls_config(cfg.upstream.tls, cfg.upstream.ca_file.as_deref())?;
    let store = Store::open(&cfg.cache).await?;
    let validator = Validator::start(&cfg, tls.clone());
    let stats = Arc::new(Stats::default());
    let shared = Shared::new(cfg.clone(), store.clone(), validator, stats.clone(), tls);
    let server = Arc::new(Server::new(shared.clone())?);
    observe::spawn_summary(stats, cfg.log.summary_interval, move || {
        store.memory_usage()
    });
    admin::spawn_trimmer(cfg.clone());
    let sh = shared.clone();
    let admin_addr = cfg.admin.addr.clone();
    tokio::spawn(async move {
        if let Err(e) = admin::serve_http(sh, admin_addr).await {
            tracing::error!(error = %format!("{e:#}"), "admin endpoint failed");
        }
    });
    Ok(App { server, shared })
}

impl App {
    pub fn addr(&self, l: &TcpListener) -> String {
        l.local_addr().map(|a| a.to_string()).unwrap_or_default()
    }

    /// Flush the cache to disk (if any) so a restart starts warm.
    pub async fn shutdown(&self) {
        if let Err(e) = self.shared.store.close().await {
            tracing::warn!(error = %format!("{e:#}"), "closing cache");
        }
    }

    /// Run the proxy on an already-bound listener (tests).
    pub fn spawn(&self, listener: TcpListener) -> tokio::task::JoinHandle<()> {
        let server = self.server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.serve(listener).await {
                tracing::error!(error = %format!("{e:#}"), "server stopped");
            }
        })
    }
}
