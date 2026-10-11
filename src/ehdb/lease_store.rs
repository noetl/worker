//! **The Kubernetes `LeaseStore` adapter** — C2's election backing store
//! ([noetl/ai-meta#460](https://github.com/noetl/ai-meta/issues/460),
//! `ehdb/docs/spec/lease-election-k8s-binding.md`).
//!
//! ## Why it lives in the worker and not in ehdb
//!
//! The spec left the placement open and named the objection:
//!
//! > This workspace has **no HTTP or `kube` client**. Pulling one in drags
//! > `k8s-openapi`, `tower`, `hyper` and a TLS stack into a crate tree that
//! > currently has none of it.
//!
//! Measured, that is exactly right: no ehdb crate declares `reqwest`, `hyper`,
//! `tower`, `rustls`, `kube` or `k8s-openapi`. This crate already has
//! `reqwest 0.12` with `rustls-tls`. So the adapter is implemented here and
//! injected through the `LeaseStore` seam, and ehdb gains **zero**
//! dependencies — which is what the seam was left open for.
//!
//! ## ⚠⚠ Nothing here is authoritative
//!
//! Default **off** (`NOETL_EHDB_ELECTION`). With it on, the election runs
//! alongside the existing arrangement and issues tokens; single-writer still
//! rests on `StatefulSet replicas: 1`. Promotion — the writer refusing to
//! append without a token, and `FencingMode::Enforce` — is owner-gated and
//! deliberately not reachable from here.
//!
//! ## The three calls, and where the guarantee lives
//!
//! | trait method | HTTP | mutual exclusion |
//! | :-- | :-- | :-- |
//! | `read` | `GET .../leases/{name}` | — (404 ⇒ `Ok(None)`) |
//! | `create` | `POST .../leases` | 409 ⇒ `Ok(false)` |
//! | `compare_and_swap` | `PUT .../leases/{name}` with `metadata.resourceVersion` | **409 ⇒ `Ok(false)`** |
//!
//! ⭐ **That 409 is the mutual exclusion.** The API server's optimistic
//! concurrency does the work. The spec is explicit that an adapter which
//! "retries a 409 by re-reading and writing again has thrown the guarantee
//! away", so neither write path retries — a 409 is returned to the caller as a
//! lost race, which is what it is.

use std::sync::Arc;

use ehdb_reference::election::{LeaseRecord, LeaseStore};
use ehdb_reference::{EhdbError, Result};

const API: &str = "/apis/coordination.k8s.io/v1/namespaces";
const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Where the adapter talks to, and as whom.
#[derive(Debug, Clone)]
pub struct KubeLeaseConfig {
    /// e.g. `https://10.0.0.1:443`.
    pub api_base: String,
    pub namespace: String,
    pub bearer_token: String,
    /// PEM bundle for the API server. `None` is only valid when the host's
    /// trust store already covers it.
    pub ca_pem: Option<Vec<u8>>,
}

impl KubeLeaseConfig {
    /// In-cluster defaults, each overridable so the same adapter can be pointed
    /// at a kind cluster from outside for the spec's conformance run.
    pub fn from_env() -> Result<Self> {
        let api_base = std::env::var("NOETL_K8S_API").ok().unwrap_or_else(|| {
            let h = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default();
            let p = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
            format!("https://{h}:{p}")
        });
        if api_base.contains("https://:") || api_base == "https://:443" {
            return Err(EhdbError::InvalidState(
                "no Kubernetes API endpoint: set NOETL_K8S_API or run in-cluster".into(),
            ));
        }
        let token_file =
            std::env::var("NOETL_K8S_TOKEN_FILE").unwrap_or_else(|_| format!("{SA_DIR}/token"));
        let bearer_token = std::fs::read_to_string(&token_file)
            .map_err(|e| EhdbError::InvalidState(format!("read {token_file}: {e}")))?
            .trim()
            .to_string();
        let namespace = match std::env::var("NOETL_K8S_NAMESPACE") {
            Ok(n) => n,
            Err(_) => std::fs::read_to_string(format!("{SA_DIR}/namespace"))
                .map_err(|e| EhdbError::InvalidState(format!("read namespace: {e}")))?
                .trim()
                .to_string(),
        };
        let ca_file =
            std::env::var("NOETL_K8S_CA_FILE").unwrap_or_else(|_| format!("{SA_DIR}/ca.crt"));
        let ca_pem = std::fs::read(&ca_file).ok();
        Ok(Self {
            api_base,
            namespace,
            bearer_token,
            ca_pem,
        })
    }
}

/// Whether to run the shard election at all. Default **off**.
pub fn election_enabled() -> bool {
    std::env::var("NOETL_EHDB_ELECTION")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

/// A [`LeaseStore`] backed by `coordination.k8s.io/v1` Leases.
pub struct KubeLeaseStore {
    cfg: KubeLeaseConfig,
    client: reqwest::Client,
    /// ⚠ Its **own** runtime. `LeaseStore` is a sync trait and this crate is
    /// async, so the calls are bridged with `block_on`. A separate runtime is
    /// what makes that safe: `block_on` on the *current* runtime from inside an
    /// async task deadlocks. Callers on an async path should still reach this
    /// through `spawn_blocking` rather than parking a worker thread — the
    /// election ticks on a timer, so that is cheap.
    rt: tokio::runtime::Runtime,
}

impl KubeLeaseStore {
    pub fn new(cfg: KubeLeaseConfig) -> Result<Self> {
        let mut b = reqwest::Client::builder();
        if let Some(pem) = &cfg.ca_pem {
            let cert = reqwest::Certificate::from_pem(pem)
                .map_err(|e| EhdbError::InvalidState(format!("API server CA is not PEM: {e}")))?;
            b = b.add_root_certificate(cert);
        }
        let client = b
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| EhdbError::Storage(format!("build http client: {e}")))?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EhdbError::Storage(format!("build runtime: {e}")))?;
        Ok(Self { cfg, client, rt })
    }

    /// Build from the environment, or `None` when the election flag is off.
    pub fn from_env_if_enabled() -> Result<Option<Arc<Self>>> {
        if !election_enabled() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self::new(KubeLeaseConfig::from_env()?)?)))
    }

    fn url(&self, name: Option<&str>) -> String {
        match name {
            Some(n) => format!(
                "{}{API}/{}/leases/{n}",
                self.cfg.api_base, self.cfg.namespace
            ),
            None => format!("{}{API}/{}/leases", self.cfg.api_base, self.cfg.namespace),
        }
    }

    /// `renewed_at_millis` -> RFC3339 with microseconds, which is what
    /// `spec.renewTime` (`MicroTime`) carries.
    ///
    /// ⚠ Formatted to microseconds rather than seconds on purpose: truncating
    /// here would make a lease look renewed up to 999 ms earlier than it was and
    /// silently shorten every liveness window. ehdb's conformance suite has a
    /// clause for exactly this (`renewed_at_round_trip`) with a non-round
    /// fixture, so a truncating adapter fails rather than passing by luck.
    fn millis_to_micro_time(ms: u64) -> Result<String> {
        let secs = (ms / 1000) as i64;
        let nanos = ((ms % 1000) * 1_000_000) as u32;
        chrono::DateTime::from_timestamp(secs, nanos)
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
            .ok_or_else(|| EhdbError::InvalidState(format!("renewed_at_millis {ms} out of range")))
    }

    fn micro_time_to_millis(s: &str) -> Result<u64> {
        let dt = chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| EhdbError::Storage(format!("renewTime {s:?} is not RFC3339: {e}")))?;
        let ms = dt.timestamp_millis();
        u64::try_from(ms).map_err(|_| EhdbError::Storage(format!("renewTime {s:?} is negative")))
    }

    /// ⚠⚠ `metadata.resourceVersion` is documented by Kubernetes as an **opaque
    /// string** that clients must not interpret. `LeaseRecord::version` is a
    /// `u64`, and the spec's mapping table says to use one as the other — so
    /// this parses it.
    ///
    /// That is an assumption about etcd-backed clusters, where the value is a
    /// monotonically increasing integer, and it is the assumption the
    /// conformance clause `cas_advances_version` actually tests. A non-numeric
    /// resourceVersion fails **loudly** here rather than being coerced to 0,
    /// because a silent 0 would make every CAS compare against the wrong token.
    fn parse_resource_version(s: &str) -> Result<u64> {
        s.parse::<u64>().map_err(|_| {
            EhdbError::Storage(format!(
                "resourceVersion {s:?} is not an integer; this adapter maps it to \
                 LeaseRecord::version per the spec, which assumes an etcd-backed API server"
            ))
        })
    }

    fn body(&self, name: &str, r: &LeaseRecord, resource_version: Option<u64>) -> Result<String> {
        let mut meta = serde_json::json!({ "name": name, "namespace": self.cfg.namespace });
        if let Some(v) = resource_version {
            meta["resourceVersion"] = serde_json::Value::String(v.to_string());
        }
        let transitions = i64::try_from(r.transitions).map_err(|_| {
            EhdbError::InvalidState(format!("transitions {} exceeds i64", r.transitions))
        })?;
        Ok(serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": meta,
            "spec": {
                "holderIdentity": r.holder,
                "leaseDurationSeconds": r.duration_secs,
                "leaseTransitions": transitions,
                "renewTime": Self::millis_to_micro_time(r.renewed_at_millis)?,
            }
        })
        .to_string())
    }

    fn parse(v: &serde_json::Value) -> Result<LeaseRecord> {
        let spec = v.get("spec").ok_or_else(|| {
            EhdbError::Storage("Lease has no spec; the API returned something else".into())
        })?;
        let rv = v
            .pointer("/metadata/resourceVersion")
            .and_then(|x| x.as_str())
            .ok_or_else(|| EhdbError::Storage("Lease has no metadata.resourceVersion".into()))?;
        Ok(LeaseRecord {
            holder: spec
                .get("holderIdentity")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            // ⚠ Absent means 0, not an error: a Lease created by something else
            // legitimately has no leaseTransitions yet.
            transitions: spec
                .get("leaseTransitions")
                .and_then(|x| x.as_i64())
                .unwrap_or(0)
                .max(0) as u64,
            renewed_at_millis: match spec.get("renewTime").and_then(|x| x.as_str()) {
                Some(s) => Self::micro_time_to_millis(s)?,
                None => 0,
            },
            duration_secs: spec
                .get("leaseDurationSeconds")
                .and_then(|x| x.as_u64())
                .unwrap_or(0),
            version: Self::parse_resource_version(rv)?,
        })
    }

    fn send(&self, req: reqwest::RequestBuilder) -> Result<(reqwest::StatusCode, String)> {
        self.rt.block_on(async move {
            let resp = req
                .send()
                .await
                .map_err(|e| EhdbError::Storage(format!("k8s request failed: {e}")))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| EhdbError::Storage(format!("read k8s response: {e}")))?;
            Ok((status, text))
        })
    }

    fn authed(&self, b: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        b.bearer_auth(&self.cfg.bearer_token)
            .header("Accept", "application/json")
    }
}

impl LeaseStore for KubeLeaseStore {
    fn read(&self, name: &str) -> Result<Option<LeaseRecord>> {
        let (status, body) = self.send(self.authed(self.client.get(self.url(Some(name)))))?;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(EhdbError::Storage(format!(
                "GET lease {name}: {status} {body}"
            )));
        }
        let v: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| EhdbError::Storage(format!("decode lease {name}: {e}")))?;
        Ok(Some(Self::parse(&v)?))
    }

    fn create(&self, name: &str, record: &LeaseRecord) -> Result<bool> {
        let payload = self.body(name, record, None)?;
        let (status, body) = self.send(
            self.authed(self.client.post(self.url(None)))
                .header("Content-Type", "application/json")
                .body(payload),
        )?;
        // ⭐ 409 is the race, not an error.
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(EhdbError::Storage(format!(
                "POST lease {name}: {status} {body}"
            )));
        }
        Ok(true)
    }

    fn compare_and_swap(
        &self,
        name: &str,
        expected_version: u64,
        record: &LeaseRecord,
    ) -> Result<bool> {
        // ⚠⚠ Existence is checked BEFORE the PUT, and this is not defensive
        // padding — it is required by a behaviour the spec's mapping table does
        // not mention.
        //
        // `PUT` on a Lease that does not exist returns **201 Created and creates
        // it**, even with `metadata.resourceVersion` set: the API server ignores
        // the precondition when there is no object to apply it to. Measured
        // against a real API server (kind, v1.35.0) — HTTP 201, with the
        // resourceVersion in the request simply discarded.
        //
        // Left alone, `compare_and_swap` on an absent lease would therefore
        // *invent* the lease and report `Ok(true)`. For an election that is
        // worse than a wrong answer: the caller's `transitions` came from a
        // stale read, so it would mint an epoch that may already have been
        // issued, and a reused epoch makes the store accept a superseded writer
        // as current. ehdb's conformance clause `cas_absent_errors` caught this
        // on the first run against kind.
        //
        // ⚠ And it cannot be cleaned up after the fact: the election Role
        // deliberately has no `delete` verb (deleting a Lease discards
        // `leaseTransitions`). So the create has to be prevented, not undone.
        match self.read(name)? {
            Some(_) => {}
            None => return Err(EhdbError::NotFound(format!("lease {name}"))),
        }
        let payload = self.body(name, record, Some(expected_version))?;
        let (status, body) = self.send(
            self.authed(self.client.put(self.url(Some(name))))
                .header("Content-Type", "application/json")
                .body(payload),
        )?;
        // ⚠ A missing lease is an error, not a lost race — `Ok(false)` would
        // tell the caller "someone else wrote first", which is a different fact
        // and would make an election loop spin instead of creating.
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(EhdbError::NotFound(format!("lease {name}")));
        }
        // The lease was read a moment ago and is gone now — it was deleted
        // between the two calls, so the PUT created it rather than updating it.
        // Report the absence the caller needs to know about. ⚠ This leaves the
        // created object behind, which is unavoidable without a `delete` verb;
        // it is a genuinely rare race and the next `try_acquire` reads it and
        // proceeds normally.
        if status == reqwest::StatusCode::CREATED {
            return Err(EhdbError::NotFound(format!(
                "lease {name} vanished between the existence check and the write; the PUT \
                 created it instead of updating it"
            )));
        }
        // ⭐ THE mutual exclusion. Deliberately NOT retried: the spec notes that
        // "an adapter that retries a 409 by re-reading and writing again has
        // thrown the guarantee away".
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(EhdbError::Storage(format!(
                "PUT lease {name}: {status} {body}"
            )));
        }
        Ok(true)
    }
}
