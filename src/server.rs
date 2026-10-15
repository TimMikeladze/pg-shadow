//! Accepting clients: TLS, cancel requests, startup, and relaying authentication
//! between the client and its upstream Postgres session.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

use crate::net::{self, Stream};
use crate::protocol::{self, Frame, FrameReader};
use crate::session::{self, Shared};

static CONN_IDS: AtomicU64 = AtomicU64::new(1);

pub struct Server {
    pub shared: Arc<Shared>,
    tls: Option<TlsAcceptor>,
    limit: Arc<Semaphore>,
}

impl Server {
    pub fn new(shared: Arc<Shared>) -> Result<Self> {
        let l = &shared.cfg.listen;
        let tls = match (&l.tls_cert, &l.tls_key) {
            (Some(c), Some(k)) => Some(net::server_tls_acceptor(c, k)?),
            _ => None,
        };
        let limit = Arc::new(Semaphore::new(l.max_connections));
        Ok(Self { shared, tls, limit })
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            let (tcp, peer) = listener.accept().await.context("accept")?;
            let _ = tcp.set_nodelay(true);
            let me = self.clone();
            tokio::spawn(async move {
                let permit = match me.limit.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        tracing::warn!(%peer, "connection limit reached; refusing");
                        let _ = refuse(tcp, "53300", "pgshadow: too many connections").await;
                        return;
                    }
                };
                let conn = CONN_IDS.fetch_add(1, Ordering::Relaxed);
                if let Err(e) = me.handle(tcp, conn).await {
                    tracing::debug!(conn, %peer, error = %format!("{e:#}"), "connection ended with error");
                }
                drop(permit);
            });
        }
    }

    async fn handle(&self, tcp: TcpStream, conn: u64) -> Result<()> {
        let mut stream: Stream = Box::new(tcp);
        let mut rd = FrameReader::new(stream);
        let mut tls_done = false;
        // Startup phase: SSL/GSS negotiation, cancel requests, then the StartupMessage.
        let startup = loop {
            let Some(pkt) = rd.read_startup().await? else { return Ok(()) };
            match protocol::startup_code(&pkt) {
                protocol::SSL_REQUEST if !tls_done => {
                    let (mut s, buf) = rd.into_parts();
                    if !buf.is_empty() {
                        bail!("data received before TLS handshake");
                    }
                    match &self.tls {
                        Some(acceptor) => {
                            s.write_all(b"S").await?;
                            let tls = acceptor.accept(s).await.context("client TLS handshake")?;
                            stream = Box::new(tls);
                            tls_done = true;
                        }
                        None => {
                            s.write_all(b"N").await?;
                            stream = s;
                        }
                    }
                    rd = FrameReader::new(stream);
                }
                protocol::GSSENC_REQUEST => {
                    let (mut s, buf) = rd.into_parts();
                    s.write_all(b"N").await?;
                    rd = FrameReader::from_parts(s, buf);
                }
                protocol::CANCEL_REQUEST => {
                    // The client got the upstream backend's key, so the cancel is simply
                    // passed on to Postgres.
                    let sh = &self.shared;
                    net::send_cancel(&sh.cfg.upstream, &sh.upstream_tls, &pkt).await?;
                    return Ok(());
                }
                code if code >> 16 == 3 => break pkt,
                code => bail!("unsupported startup code {code}"),
            }
        };
        let params = protocol::startup_params(&startup);
        let param = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()).unwrap_or_default();
        if param("replication") != "" && param("replication") != "false" {
            let (mut s, _) = rd.into_parts();
            let _ = send_error(&mut s, "0A000", "pgshadow: replication connections are not supported").await;
            return Ok(());
        }
        let (user, database, app) = (param("user"), param("database"), param("application_name"));

        let sh = &self.shared;
        let upstream = match net::connect_upstream(&sh.cfg.upstream, &sh.upstream_tls).await {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(conn, error = %format!("{e:#}"), "cannot reach Postgres");
                let (mut s, _) = rd.into_parts();
                let _ = send_error(&mut s, "08006", &format!("pgshadow: cannot reach Postgres: {e:#}")).await;
                return Ok(());
            }
        };
        let up_rd = FrameReader::new(upstream);
        let (client_stream, client_buf) = rd.into_parts();
        let (up_stream, up_buf) = up_rd.into_parts();
        let (up_r, mut up_w) = tokio::io::split(up_stream);
        let (cl_r, mut cl_w) = tokio::io::split(client_stream);
        up_w.write_all(&startup).await?;
        up_w.flush().await?;
        let mut up_rd = FrameReader::from_parts(up_r, up_buf);
        let mut cl_rd = FrameReader::from_parts(cl_r, client_buf);

        // Relay authentication until the first ReadyForQuery.
        let status = loop {
            let Some(f) = up_rd.read_frame().await? else { return Ok(()) };
            match f.tag {
                b'R' => {
                    let code = auth_code(&f);
                    let f = if code == 10 { strip_channel_binding(&f) } else { f };
                    cl_w.write_all(&f.to_bytes()).await?;
                    cl_w.flush().await?;
                    // These ask the client for a password message.
                    if matches!(code, 2 | 3 | 5 | 7 | 8 | 9 | 10 | 11) {
                        let Some(reply) = cl_rd.read_frame().await? else { return Ok(()) };
                        up_w.write_all(&reply.to_bytes()).await?;
                        up_w.flush().await?;
                    }
                }
                b'Z' => {
                    cl_w.write_all(&f.to_bytes()).await?;
                    cl_w.flush().await?;
                    break f.body.first().copied().unwrap_or(b'I');
                }
                b'E' => {
                    let (code, msg) = protocol::error_fields(&f.body);
                    let f = if code == "28000" && msg.contains("channel binding") {
                        // See strip_channel_binding: explain how to connect instead.
                        protocol::error_frame(
                            "FATAL",
                            "28000",
                            &format!("{msg} (pgshadow: SCRAM channel binding cannot pass through a TLS-terminating proxy. \
                                      Connect to pgshadow without TLS on a private network, or add channel_binding=disable \
                                      to the client connection string.)"),
                        )
                    } else {
                        f
                    };
                    cl_w.write_all(&f.to_bytes()).await?;
                    cl_w.flush().await?;
                    return Ok(());
                }
                _ => {
                    // ParameterStatus, BackendKeyData, NegotiateProtocolVersion, notices.
                    cl_w.write_all(&f.to_bytes()).await?;
                }
            }
        };
        let stats = sh.stats.clone();
        stats.connections.fetch_add(1, Ordering::Relaxed);
        stats.connections_total.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(conn, %user, %database, application = %app, "session started");
        session::run(sh.clone(), conn, cl_rd, cl_w, up_rd, up_w, status).await;
        stats.connections.fetch_sub(1, Ordering::Relaxed);
        tracing::debug!(conn, "session ended");
        Ok(())
    }
}

fn auth_code(f: &Frame) -> i32 {
    if f.body.len() < 4 {
        return -1;
    }
    i32::from_be_bytes([f.body[0], f.body[1], f.body[2], f.body[3]])
}

/// Channel binding ties SCRAM to the TLS connection it runs over. The proxy terminates
/// the client's TLS, so binding to the client's connection cannot be verified by
/// Postgres; offer only the non-binding mechanisms (clients with channel_binding=prefer,
/// the libpq default, use those).
fn strip_channel_binding(f: &Frame) -> Frame {
    let mut out = BytesMut::new();
    out.put_i32(10);
    let mut kept = 0;
    for mech in f.body[4..].split(|b| *b == 0).filter(|m| !m.is_empty()) {
        if mech.ends_with(b"-PLUS") {
            continue;
        }
        out.extend_from_slice(mech);
        out.put_u8(0);
        kept += 1;
    }
    if kept == 0 {
        return f.clone();
    }
    out.put_u8(0);
    Frame::new(b'R', out.freeze())
}

async fn send_error(s: &mut Stream, code: &str, msg: &str) -> std::io::Result<()> {
    let f = protocol::error_frame("FATAL", code, msg);
    s.write_all(&f.to_bytes()).await?;
    s.flush().await
}

async fn refuse(tcp: TcpStream, code: &str, msg: &str) -> std::io::Result<()> {
    // Read the startup packet first so the client sees the error.
    let mut rd = FrameReader::new(Box::new(tcp) as Stream);
    loop {
        let Some(pkt) = rd.read_startup().await? else { return Ok(()) };
        let (mut s, buf) = rd.into_parts();
        if protocol::startup_code(&pkt) == protocol::SSL_REQUEST {
            s.write_all(b"N").await?;
            rd = FrameReader::from_parts(s, buf);
            continue;
        }
        return send_error(&mut s, code, msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_binding_is_stripped() {
        let mut b = BytesMut::new();
        b.put_i32(10);
        for m in ["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"] {
            b.extend_from_slice(m.as_bytes());
            b.put_u8(0);
        }
        b.put_u8(0);
        let f = strip_channel_binding(&Frame::new(b'R', b.freeze()));
        assert_eq!(&f.body[4..], b"SCRAM-SHA-256\0\0");
    }
}
