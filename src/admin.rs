//! Administration: installing the Postgres objects, opting tables in and out, the
//! changelog trimmer, and the small HTTP endpoint for health checks and the kill switch.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;

use crate::config::Config;
use crate::net;
use crate::session::Shared;

pub const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

async fn client(cfg: &Config) -> Result<tokio_postgres::Client> {
    let tls = net::client_tls_config(cfg.upstream.tls, cfg.upstream.ca_file.as_deref())?;
    net::connect_plain(&cfg.check.dsn, cfg.upstream.tls, &tls).await
}

pub async fn install(cfg: &Config) -> Result<()> {
    let c = client(cfg).await?;
    c.batch_execute(SCHEMA_SQL).await.context("installing pgshadow schema")?;
    Ok(())
}

pub async fn uninstall(cfg: &Config) -> Result<()> {
    let c = client(cfg).await?;
    c.batch_execute(
        "DO $$ DECLARE r record; BEGIN
           FOR r IN SELECT tgrelid::regclass AS t FROM pg_trigger WHERE tgname = 'pgshadow_capture' LOOP
             EXECUTE format('DROP TRIGGER pgshadow_capture ON %s', r.t);
           END LOOP;
         END $$;
         DROP SCHEMA IF EXISTS pgshadow CASCADE;",
    )
    .await
    .context("uninstalling")?;
    Ok(())
}

pub async fn enable(cfg: &Config, tables: &[String]) -> Result<()> {
    let c = client(cfg).await?;
    for t in tables {
        c.execute("SELECT pgshadow.enable($1::pg_catalog.text::pg_catalog.regclass)", &[t])
            .await
            .with_context(|| format!("enabling {t}"))?;
        println!("{t}: cached");
    }
    Ok(())
}

pub async fn disable(cfg: &Config, tables: &[String]) -> Result<()> {
    let c = client(cfg).await?;
    for t in tables {
        c.execute("SELECT pgshadow.disable($1::pg_catalog.text::pg_catalog.regclass)", &[t])
            .await
            .with_context(|| format!("disabling {t}"))?;
        println!("{t}: no longer cached");
    }
    Ok(())
}

pub async fn status(cfg: &Config) -> Result<()> {
    let c = client(cfg).await?;
    let rows = c
        .query("SELECT table_name::text, cached, coalesce(reason, '') FROM pgshadow.tables ORDER BY 1", &[])
        .await
        .context("reading pgshadow.tables (is pgshadow installed?)")?;
    if rows.is_empty() {
        println!("no tables enabled; use `pgshadow enable <table>`");
    }
    for r in rows {
        let (t, ok, reason): (String, bool, String) = (r.get(0), r.get(1), r.get(2));
        println!("{t:<40} {}", if ok { "cached".to_string() } else { format!("NOT cached: {reason}") });
    }
    let n: i64 = c.query_one("SELECT count(*) FROM pgshadow.changelog", &[]).await?.get(0);
    println!("changelog rows: {n}");
    Ok(())
}

/// Trims the changelog. Every proxy runs this; an advisory lock lets only one act.
pub fn spawn_trimmer(cfg: Config) {
    let interval = cfg.changelog.trim_interval;
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        loop {
            if let Err(e) = trim_loop(&cfg).await {
                tracing::warn!(error = %format!("{e:#}"), "changelog trimmer stopped; restarting");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

async fn trim_loop(cfg: &Config) -> Result<()> {
    let c = client(cfg).await?;
    let mut tick = tokio::time::interval(cfg.changelog.trim_interval);
    let keep = format!("{} seconds", cfg.changelog.retention.as_secs());
    loop {
        tick.tick().await;
        let leader: bool = c
            .query_one("SELECT pg_try_advisory_lock(hashtext('pgshadow.trim'))", &[])
            .await?
            .get(0);
        if !leader {
            continue;
        }
        let n: i64 = c.query_one("SELECT pgshadow.trim($1::pg_catalog.text::pg_catalog.interval)", &[&keep]).await?.get(0);
        if n > 0 {
            tracing::debug!(removed = n, "changelog trimmed");
        }
    }
}

pub async fn serve_http(shared: Arc<Shared>, addr: String) -> Result<()> {
    if addr.is_empty() {
        return Ok(());
    }
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(ready))
        .route("/cache/enable", post(enable_cache))
        .route("/cache/disable", post(disable_cache))
        .route("/cache/purge", post(purge))
        .with_state(shared);
    let listener = tokio::net::TcpListener::bind(&addr).await.with_context(|| format!("binding admin {addr}"))?;
    tracing::info!(%addr, "admin endpoint listening");
    axum::serve(listener, app).await.context("admin endpoint")
}

/// Ready = can serve clients. Postgres reachability is per client connection; this
/// reports whether caching is operational (checks connected, schema installed).
async fn ready(State(sh): State<Arc<Shared>>) -> (StatusCode, String) {
    let caching = sh.enabled() && sh.validator.ready();
    let body = format!(
        "{{\"caching\":{},\"cache_enabled\":{},\"checks_ready\":{},\"connections\":{}}}",
        caching,
        sh.enabled(),
        sh.validator.ready(),
        sh.stats.connections.load(Ordering::Relaxed)
    );
    // Not being able to cache never stops the proxy from serving (it forwards), so
    // readiness only fails if checks are down while caching is supposed to be on.
    let code = if !sh.enabled() || sh.validator.ready() { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, body)
}

async fn enable_cache(State(sh): State<Arc<Shared>>) -> &'static str {
    sh.enabled.store(true, Ordering::Relaxed);
    tracing::warn!("cache enabled via admin endpoint");
    "cache enabled\n"
}

async fn disable_cache(State(sh): State<Arc<Shared>>) -> &'static str {
    sh.enabled.store(false, Ordering::Relaxed);
    tracing::warn!("cache disabled via admin endpoint: all queries go to Postgres");
    "cache disabled\n"
}

async fn purge(State(sh): State<Arc<Shared>>) -> (StatusCode, &'static str) {
    match sh.store.clear().await {
        Ok(()) => {
            tracing::warn!("cache purged via admin endpoint");
            (StatusCode::OK, "cache purged\n")
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "purge failed\n"),
    }
}
