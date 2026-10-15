//! Sockets and TLS: client-side TLS termination, upstream connections with Postgres's
//! SSLRequest negotiation, cancel forwarding, and TLS for the proxy's own connections.

use std::io;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{ring, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::{TlsMode, Upstream};
use crate::protocol::SSL_REQUEST;

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}
pub type Stream = Box<dyn AsyncStream>;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

/// Accepts any certificate (sslmode=require semantics: encrypted, not authenticated).
#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

pub fn client_tls_config(mode: TlsMode, ca_file: Option<&str>) -> Result<Arc<rustls::ClientConfig>> {
    let p = provider();
    let builder = rustls::ClientConfig::builder_with_provider(p.clone())
        .with_safe_default_protocol_versions()
        .context("TLS setup")?;
    let cfg = if mode == TlsMode::VerifyFull {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = ca_file {
            let pem = std::fs::read(path).with_context(|| format!("reading {path}"))?;
            for cert in rustls_pemfile::certs(&mut &pem[..]) {
                roots.add(cert.context("parsing CA file")?).context("adding CA certificate")?;
            }
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(p)))
            .with_no_client_auth()
    };
    Ok(Arc::new(cfg))
}

pub fn server_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &std::fs::read(cert_path)?[..])
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing {cert_path}"))?;
    let key = rustls_pemfile::private_key(&mut &std::fs::read(key_path)?[..])
        .with_context(|| format!("parsing {key_path}"))?
        .ok_or_else(|| anyhow!("no private key in {key_path}"))?;
    let cfg = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("TLS certificate")?;
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

/// Connect to Postgres, negotiating TLS per `cfg.tls`.
pub async fn connect_upstream(cfg: &Upstream, tls: &Arc<rustls::ClientConfig>) -> Result<Stream> {
    let addr = (cfg.host.as_str(), cfg.port);
    let tcp = tokio::time::timeout(cfg.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow!("timed out connecting to {}:{}", cfg.host, cfg.port))?
        .with_context(|| format!("connecting to {}:{}", cfg.host, cfg.port))?;
    tcp.set_nodelay(true)?;
    if cfg.tls == TlsMode::Disable {
        return Ok(Box::new(tcp));
    }
    let mut tcp = tcp;
    let mut req = [0u8; 8];
    req[..4].copy_from_slice(&8i32.to_be_bytes());
    req[4..].copy_from_slice(&SSL_REQUEST.to_be_bytes());
    tcp.write_all(&req).await?;
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await?;
    match answer[0] {
        b'S' => {
            let name = ServerName::try_from(cfg.host.clone()).context("upstream host name")?;
            let tls = TlsConnector::from(tls.clone())
                .connect(name, tcp)
                .await
                .context("TLS handshake with Postgres")?;
            Ok(Box::new(tls))
        }
        b'N' if cfg.tls == TlsMode::Prefer => Ok(Box::new(tcp)),
        b'N' => bail!("Postgres at {}:{} does not support TLS", cfg.host, cfg.port),
        other => bail!("unexpected answer {other:#x} to SSLRequest"),
    }
}

/// Forward a client's CancelRequest packet to Postgres.
pub async fn send_cancel(cfg: &Upstream, tls: &Arc<rustls::ClientConfig>, packet: &[u8]) -> io::Result<()> {
    let mut s = connect_upstream(cfg, tls).await.map_err(io::Error::other)?;
    s.write_all(packet).await?;
    s.flush().await?;
    let mut sink = [0u8; 1];
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut sink)).await;
    Ok(())
}

/// tokio-postgres connection for the proxy's own queries.
pub async fn connect_check(
    dsn: &str,
    mode: TlsMode,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<tokio_postgres::Client> {
    // No session settings are overridden: pgshadow.settings_hash() on these connections
    // must see the server's configured values, to notice configuration reloads. (All of
    // pgshadow's functions pin their own search_path, and the proxy's queries qualify
    // every name.)
    connect_plain(dsn, mode, tls).await
}

/// tokio-postgres connection with the configured TLS, default session settings.
pub async fn connect_plain(
    dsn: &str,
    mode: TlsMode,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<tokio_postgres::Client> {
    let mut pg: tokio_postgres::Config = dsn.parse().context("check.dsn")?;
    pg.application_name("pgshadow");
    let client = if mode == TlsMode::Disable {
        let (client, conn) = pg.connect(tokio_postgres::NoTls).await.context("check connection")?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!(error = %e, "check connection closed");
            }
        });
        client
    } else {
        if mode != TlsMode::Prefer {
            pg.ssl_mode(tokio_postgres::config::SslMode::Require);
        }
        let connector = tokio_postgres_rustls::MakeRustlsConnect::new((**tls).clone());
        let (client, conn) = pg.connect(connector).await.context("check connection")?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!(error = %e, "check connection closed");
            }
        });
        client
    };
    Ok(client)
}
