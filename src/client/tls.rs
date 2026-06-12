//! Opt-in mTLS for the worker's control-plane HTTP client
//! (Secrets Wallet Phase 4b, noetl/ai-meta#61).
//!
//! Plain HTTP by default (unchanged).  When `NOETL_TLS_CLIENT_CERT` +
//! `NOETL_TLS_CLIENT_KEY` are set the worker presents a client certificate to
//! the server's TLS listener (the **mTLS** client half of Phase 4a, server#103);
//! `NOETL_TLS_CA` adds a private CA the worker trusts when verifying the
//! server's certificate (needed when the server cert is signed by an internal
//! CA rather than a public root).  This authenticates + encrypts the
//! worker→server credential fetch (`GET /api/credentials/<alias>`) so the
//! resolved secret no longer travels plaintext on the wire.

use std::time::Duration;

use anyhow::{Context, Result};

/// Client-side TLS material resolved from the environment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkerClientTls {
    /// PEM client-certificate-chain path (the mTLS identity cert).
    pub client_cert_path: Option<String>,
    /// PEM private-key path for the identity cert.
    pub client_key_path: Option<String>,
    /// PEM CA-bundle path added as a trust root for verifying the server.
    pub ca_path: Option<String>,
}

impl WorkerClientTls {
    /// Whether a client identity (mTLS) is configured.
    pub fn has_identity(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }
}

/// Resolve the worker TLS config from the environment.
///
/// `None` ⇒ no TLS config at all (plain HTTP client, the default).
pub fn from_env() -> Result<Option<WorkerClientTls>> {
    let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    resolve(
        env("NOETL_TLS_CLIENT_CERT"),
        env("NOETL_TLS_CLIENT_KEY"),
        env("NOETL_TLS_CA"),
    )
}

/// Pure resolver (testable without touching the process environment).
///
/// The identity cert + key are both-or-neither; setting exactly one is a
/// fail-fast misconfiguration.  The CA is independent (a server with a
/// publicly-trusted cert needs no CA; a client may verify without presenting
/// an identity).
pub fn resolve(
    cert: Option<String>,
    key: Option<String>,
    ca: Option<String>,
) -> Result<Option<WorkerClientTls>> {
    match (&cert, &key) {
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "worker TLS misconfigured: set both NOETL_TLS_CLIENT_CERT and \
                 NOETL_TLS_CLIENT_KEY (or neither)"
            );
        }
        _ => {}
    }
    if cert.is_none() && key.is_none() && ca.is_none() {
        return Ok(None);
    }
    Ok(Some(WorkerClientTls {
        client_cert_path: cert,
        client_key_path: key,
        ca_path: ca,
    }))
}

/// Build the worker's control-plane HTTP client, applying mTLS when configured.
///
/// Reads the environment once.  With no TLS env set this is a plain client
/// (identical to the prior behaviour); otherwise it presents a client identity
/// and/or trusts a private CA.  Errors are fatal for a worker that must reach
/// an mTLS server — the caller fails fast rather than silently downgrading.
pub fn build_http_client(timeout: Duration) -> Result<reqwest::Client> {
    // Service-account bearer token for the out-of-cluster (Cloud Run) runtime
    // authenticating to the control plane (RFC #90 Phase 5, the
    // NOETL_INTERNAL_API_TOKEN shape the system pool uses).  Unset in-cluster
    // (the worker reaches the server over the trusted pod network); set on
    // Cloud Run so every /api/* call carries `Authorization: Bearer <token>`.
    let bearer = std::env::var("NOETL_INTERNAL_API_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    build_http_client_with(timeout, from_env()?, bearer)
}

/// Pure builder over already-resolved params (testable without env).
pub fn build_http_client_with(
    timeout: Duration,
    tls: Option<WorkerClientTls>,
    bearer: Option<String>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(timeout);

    if let Some(token) = bearer {
        let mut headers = reqwest::header::HeaderMap::new();
        let mut val = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .context("worker: invalid NOETL_INTERNAL_API_TOKEN (not a valid header value)")?;
        val.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, val);
        builder = builder.default_headers(headers);
        tracing::info!("control-plane HTTP client: bearer auth enabled (NOETL_INTERNAL_API_TOKEN)");
    }

    if let Some(tls) = tls {
        if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
            // reqwest::Identity::from_pem wants the key + cert chain in one
            // PEM buffer; concatenate (the parser scans for both).
            let cert_pem =
                std::fs::read(cert).with_context(|| format!("worker TLS: read cert '{cert}'"))?;
            let key_pem =
                std::fs::read(key).with_context(|| format!("worker TLS: read key '{key}'"))?;
            let mut pem = Vec::with_capacity(cert_pem.len() + key_pem.len() + 1);
            pem.extend_from_slice(&key_pem);
            pem.push(b'\n');
            pem.extend_from_slice(&cert_pem);
            let identity = reqwest::Identity::from_pem(&pem)
                .context("worker TLS: invalid client identity (cert/key PEM)")?;
            builder = builder.identity(identity);
        }
        if let Some(ca) = &tls.ca_path {
            let ca_pem =
                std::fs::read(ca).with_context(|| format!("worker TLS: read CA '{ca}'"))?;
            let certs = reqwest::Certificate::from_pem_bundle(&ca_pem)
                .with_context(|| format!("worker TLS: invalid CA bundle '{ca}'"))?;
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        tracing::info!(
            mtls = tls.has_identity(),
            ca = tls.ca_path.is_some(),
            "control-plane HTTP client: TLS enabled"
        );
    }

    builder
        .build()
        .context("worker TLS: building control-plane HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_none_when_unset() {
        assert!(resolve(None, None, None).unwrap().is_none());
    }

    #[test]
    fn resolve_identity_when_cert_key_set() {
        let p = resolve(Some("c.pem".into()), Some("k.pem".into()), None)
            .unwrap()
            .unwrap();
        assert!(p.has_identity());
        assert_eq!(p.client_cert_path.as_deref(), Some("c.pem"));
        assert!(p.ca_path.is_none());
    }

    #[test]
    fn resolve_ca_only_no_identity() {
        let p = resolve(None, None, Some("ca.pem".into())).unwrap().unwrap();
        assert!(!p.has_identity());
        assert_eq!(p.ca_path.as_deref(), Some("ca.pem"));
    }

    #[test]
    fn resolve_rejects_partial_identity() {
        assert!(resolve(Some("c.pem".into()), None, None).is_err());
        assert!(resolve(None, Some("k.pem".into()), None).is_err());
    }

    #[test]
    fn build_plain_when_no_tls() {
        // None params ⇒ a plain client builds fine (default path unchanged).
        assert!(build_http_client_with(Duration::from_secs(1), None, None).is_ok());
    }

    #[test]
    fn build_errors_on_missing_cert_file() {
        // A bogus identity path must surface as an error (fail fast), not a
        // silent plain client.
        let p = WorkerClientTls {
            client_cert_path: Some("/nonexistent/c.pem".into()),
            client_key_path: Some("/nonexistent/k.pem".into()),
            ca_path: None,
        };
        let err = build_http_client_with(Duration::from_secs(1), Some(p), None).unwrap_err();
        assert!(
            format!("{err:#}").contains("read cert"),
            "expected a cert-read error, got: {err:#}"
        );
    }
}
