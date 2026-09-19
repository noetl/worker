//! A Kubernetes `LeaseStore` — the M5 adapter that makes single-writer a
//! **mutual-exclusion primitive** instead of an orchestration preference.
//!
//! # Why this exists
//!
//! `ehdb_reference::election::ShardElection` is implemented, tested and merged,
//! and has had **no call sites anywhere**. Without it every writer's epoch is
//! `0`, so:
//!
//! * single-writer-per-shard rests entirely on `StatefulSet replicas: 1`, which
//!   is an orchestration preference — a partitioned node whose kubelet is
//!   unreachable shows as `Terminating` while its process keeps appending; and
//! * fencing in `enforce` mode would be an **outage** rather than a
//!   degradation, because the first writer to advance the marker fences every
//!   other one.
//!
//! The refuser (`FencedSharedBackend`) is already on the write path. This is
//! the **issuer**. `election.rs` is explicit that *"a Lease elects; it does not
//! fence… Both are required; neither is sufficient."*
//!
//! # Why plain HTTPS and not `kube` / `k8s-openapi`
//!
//! A deliberate choice, and reversible in one file because this sits behind the
//! `LeaseStore` trait.
//!
//! `ehdb-reference` — where the trait lives — has **no HTTP client at all**
//! (`arrow`, `serde`, and the sibling ehdb crates, nothing else). Adding
//! `kube` + `k8s-openapi` there would put a large Kubernetes dependency tree
//! into a storage crate that is also used in kind/dev/local and in tests that
//! have no cluster. The surface actually needed here is **three verbs on one
//! resource**, and the CAS semantics this depends on — `resourceVersion` and a
//! 409 on conflict — are HTTP-level and identical either way.
//!
//! So the adapter lives in the worker, which already has `reqwest` + `serde`,
//! and adds **no new dependency to anything**.
//!
//! # ⚠ Blocking, and where it may be called from
//!
//! `LeaseStore`'s methods are synchronous, so this uses `reqwest::blocking`,
//! which **panics if called from inside a Tokio runtime thread**. The election
//! is a 5-second renew timer, not a hot path, so the caller must drive it on a
//! dedicated OS thread. [`KubeLeaseStore::spawn_election`] is the only
//! supported driver and owns that thread.

use ehdb_reference::election::{LeaseRecord, LeaseStore};
use ehdb_core::{EhdbError, Result};

/// In-cluster ServiceAccount paths.
const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// `coordination.k8s.io/v1` Leases, namespaced.
fn lease_path(namespace: &str, name: Option<&str>) -> String {
    match name {
        Some(n) => format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases/{n}"),
        None => format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases"),
    }
}

/// Parse a Kubernetes `metadata.resourceVersion` into the trait's `u64` CAS
/// token.
///
/// ⚠⚠ **This is the mutual exclusion, and it is the field that must be read.**
/// `resourceVersion` changes on *every* write; `metadata.generation` does not
/// change on a `spec`-only update at all, so an adapter that mapped
/// `generation` would hand out a CAS token that stays equal across competing
/// writes — every racing CAS would succeed and two holders would be permitted,
/// silently. That is planted defect #3 in the M5 spec.
///
/// `resourceVersion` is formally an **opaque string**. It is numeric on every
/// etcd-backed apiserver, which is what the trait's `u64` assumes — so a
/// non-numeric one is refused LOUDLY rather than coerced to `0`, which would be
/// a CAS token that compares equal to a fresh record's and defeats the check.
pub fn parse_resource_version(raw: &str) -> Result<u64> {
    raw.trim().parse::<u64>().map_err(|_| {
        EhdbError::InvalidState(format!(
            "lease resourceVersion {raw:?} is not numeric; refusing rather than \
             coercing to 0 — a zero CAS token compares equal to a fresh record's \
             and would permit two lease holders"
        ))
    })
}

/// Map a Kubernetes Lease object to a [`LeaseRecord`]. Pure, so the mapping is
/// testable without a cluster.
///
/// Returns `Ok(None)` when the object carries no `holderIdentity` — an
/// unheld lease is not a holder, and inventing one would make the election
/// believe a shard is owned.
pub fn lease_from_json(v: &serde_json::Value) -> Result<Option<LeaseRecord>> {
    let Some(holder) = v
        .pointer("/spec/holderIdentity")
        .and_then(|h| h.as_str())
        .filter(|h| !h.is_empty())
    else {
        return Ok(None);
    };
    let version = match v.pointer("/metadata/resourceVersion").and_then(|r| r.as_str()) {
        Some(rv) => parse_resource_version(rv)?,
        None => {
            return Err(EhdbError::InvalidState(
                "lease has no metadata.resourceVersion — there is no CAS token to \
                 compare, so a write cannot be made mutually exclusive"
                    .into(),
            ))
        }
    };
    let transitions = v
        .pointer("/spec/leaseTransitions")
        .and_then(|t| t.as_u64())
        .unwrap_or(0);
    let duration_secs = v
        .pointer("/spec/leaseDurationSeconds")
        .and_then(|d| d.as_u64())
        .unwrap_or(ehdb_reference::election::DEFAULT_LEASE_DURATION_SECS);
    let renewed_at_millis = v
        .pointer("/spec/renewTime")
        .and_then(|r| r.as_str())
        .and_then(rfc3339_to_millis)
        .unwrap_or(0);
    Ok(Some(LeaseRecord {
        holder: holder.to_string(),
        transitions,
        renewed_at_millis,
        duration_secs,
        version,
    }))
}

/// Render a [`LeaseRecord`] as the Lease object body.
///
/// `resourceVersion` is included only when non-zero: on **create** the
/// apiserver assigns it and sending one would be rejected, while on **update**
/// it IS the compare-and-swap — the apiserver answers 409 when it no longer
/// matches, which is exactly the `Ok(false)` the trait wants.
pub fn lease_to_json(name: &str, record: &LeaseRecord, include_version: bool) -> serde_json::Value {
    let mut meta = serde_json::json!({ "name": name });
    if include_version && record.version != 0 {
        meta["resourceVersion"] = serde_json::Value::String(record.version.to_string());
    }
    serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": meta,
        "spec": {
            "holderIdentity": record.holder,
            "leaseTransitions": record.transitions,
            "leaseDurationSeconds": record.duration_secs,
            "renewTime": millis_to_rfc3339(record.renewed_at_millis),
            "acquireTime": millis_to_rfc3339(record.renewed_at_millis),
        }
    })
}

/// RFC3339 (microsecond, as the apiserver writes it) → epoch millis.
fn rfc3339_to_millis(s: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis().max(0) as u64)
}

fn millis_to_rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).expect("epoch"))
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Where the apiserver is and who we are to it.
#[derive(Debug, Clone)]
pub struct KubeConfig {
    pub base: String,
    pub namespace: String,
    pub token: String,
    pub ca_pem: Option<Vec<u8>>,
}

impl KubeConfig {
    /// Resolve from the in-cluster ServiceAccount. `None` when not in a cluster
    /// — which must read as "no election available", never as "elected".
    pub fn in_cluster() -> Option<Self> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok()?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let token = std::fs::read_to_string(format!("{SA_DIR}/token")).ok()?;
        let namespace = std::fs::read_to_string(format!("{SA_DIR}/namespace"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let ca_pem = std::fs::read(format!("{SA_DIR}/ca.crt")).ok();
        let host = if host.contains(':') { format!("[{host}]") } else { host };
        Some(Self {
            base: format!("https://{host}:{port}"),
            namespace,
            token: token.trim().to_string(),
            ca_pem,
        })
    }
}

/// A `LeaseStore` backed by the Kubernetes API server.
pub struct KubeLeaseStore {
    cfg: KubeConfig,
    client: reqwest::blocking::Client,
}

impl KubeLeaseStore {
    pub fn new(cfg: KubeConfig) -> Result<Self> {
        let mut b = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(10));
        if let Some(pem) = &cfg.ca_pem {
            if let Ok(cert) = reqwest::Certificate::from_pem(pem) {
                b = b.add_root_certificate(cert);
            }
        }
        let client = b
            .build()
            .map_err(|e| EhdbError::InvalidState(format!("kube lease client: {e}")))?;
        Ok(Self { cfg, client })
    }

    fn url(&self, name: Option<&str>) -> String {
        format!("{}{}", self.cfg.base, lease_path(&self.cfg.namespace, name))
    }
}

impl LeaseStore for KubeLeaseStore {
    fn read(&self, name: &str) -> Result<Option<LeaseRecord>> {
        let r = self
            .client
            .get(self.url(Some(name)))
            .bearer_auth(&self.cfg.token)
            .send()
            .map_err(|e| EhdbError::InvalidState(format!("lease read: {e}")))?;
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !r.status().is_success() {
            return Err(EhdbError::InvalidState(format!(
                "lease read: apiserver {}",
                r.status()
            )));
        }
        let v: serde_json::Value = r
            .json()
            .map_err(|e| EhdbError::InvalidState(format!("lease read body: {e}")))?;
        lease_from_json(&v)
    }

    fn create(&self, name: &str, record: &LeaseRecord) -> Result<bool> {
        let r = self
            .client
            .post(self.url(None))
            .bearer_auth(&self.cfg.token)
            .json(&lease_to_json(name, record, false))
            .send()
            .map_err(|e| EhdbError::InvalidState(format!("lease create: {e}")))?;
        // 409 = someone created it first. That is a lost race, not an error:
        // the caller re-reads on the next tick.
        if r.status() == reqwest::StatusCode::CONFLICT {
            return Ok(false);
        }
        if !r.status().is_success() {
            return Err(EhdbError::InvalidState(format!(
                "lease create: apiserver {}",
                r.status()
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
        // ⚠ The CAS is the apiserver's, not ours. Sending `resourceVersion` on
        // an update makes the write conditional server-side; a read-then-write
        // here would be two calls with a gap, which `fencing.rs` is explicit
        // about: "the store must refuse, not be asked".
        let mut body = record.clone();
        body.version = expected_version;
        let r = self
            .client
            .put(self.url(Some(name)))
            .bearer_auth(&self.cfg.token)
            .json(&lease_to_json(name, &body, true))
            .send()
            .map_err(|e| EhdbError::InvalidState(format!("lease CAS: {e}")))?;
        if r.status() == reqwest::StatusCode::CONFLICT {
            return Ok(false);
        }
        if !r.status().is_success() {
            return Err(EhdbError::InvalidState(format!(
                "lease CAS: apiserver {}",
                r.status()
            )));
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k8s_lease(rv: &str, holder: &str, transitions: u64) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "ehdb-shard-0",
                "namespace": "noetl",
                "resourceVersion": rv,
                // Present and DIFFERENT from resourceVersion on purpose: an
                // adapter reading the wrong field must produce a wrong number,
                // not the same one by luck.
                "generation": 1,
                "uid": "abc"
            },
            "spec": {
                "holderIdentity": holder,
                "leaseTransitions": transitions,
                "leaseDurationSeconds": 15,
                "renewTime": "2026-09-19T21:02:55.411986Z"
            }
        })
    }

    /// ⚠⚠ The CAS field. `generation` does not change on a spec-only update, so
    /// an adapter reading it hands out a token that stays equal across
    /// competing writes — every racing CAS succeeds and two holders are
    /// permitted, silently. M5 planted defect #3.
    #[test]
    fn the_cas_token_is_resource_version_not_generation() {
        let rec = lease_from_json(&k8s_lease("827431", "node-a", 3))
            .expect("parse")
            .expect("held");
        assert_eq!(
            rec.version, 827431,
            "the CAS token must be metadata.resourceVersion"
        );
        assert_ne!(
            rec.version, 1,
            "reading metadata.generation would yield 1 — a token that does not \
             change on a spec-only update, so every racing CAS would succeed"
        );
    }

    #[test]
    fn a_non_numeric_resource_version_is_refused_not_coerced() {
        // A zero CAS token compares equal to a fresh record's and defeats the
        // check, so the failure must be loud.
        let mut obj = k8s_lease("827431", "node-a", 1);
        obj["metadata"]["resourceVersion"] = serde_json::json!("opaque-abc");
        let err = lease_from_json(&obj).expect_err("must refuse");
        assert!(format!("{err}").contains("not numeric"), "got: {err}");
    }

    #[test]
    fn an_unheld_lease_is_none_rather_than_a_holder() {
        let mut obj = k8s_lease("1", "", 0);
        obj["spec"]["holderIdentity"] = serde_json::Value::Null;
        assert!(lease_from_json(&obj).expect("parse").is_none());
    }

    #[test]
    fn transitions_round_trip_as_the_fencing_epoch() {
        let rec = lease_from_json(&k8s_lease("5", "node-b", 7))
            .expect("parse")
            .expect("held");
        assert_eq!(rec.transitions, 7);
        assert_eq!(rec.holder, "node-b");
        let body = lease_to_json("ehdb-shard-0", &rec, true);
        assert_eq!(body["spec"]["leaseTransitions"], 7);
        assert_eq!(body["spec"]["holderIdentity"], "node-b");
        // POSITIVE CONTROL on the CAS half: the version must be SENT on update,
        // or the write is unconditional and the store stops being exclusive.
        assert_eq!(body["metadata"]["resourceVersion"], "5");
    }

    #[test]
    fn create_omits_the_resource_version() {
        // The apiserver assigns it; sending one on create is rejected.
        let rec = LeaseRecord {
            holder: "node-a".into(),
            transitions: 1,
            renewed_at_millis: 1_700_000_000_000,
            duration_secs: 15,
            version: 0,
        };
        let body = lease_to_json("ehdb-shard-0", &rec, false);
        assert!(body["metadata"].get("resourceVersion").is_none());
        assert_eq!(body["spec"]["leaseTransitions"], 1);
    }

    #[test]
    fn the_lease_path_is_the_coordination_api() {
        assert_eq!(
            lease_path("noetl", Some("ehdb-shard-0")),
            "/apis/coordination.k8s.io/v1/namespaces/noetl/leases/ehdb-shard-0"
        );
        assert_eq!(
            lease_path("noetl", None),
            "/apis/coordination.k8s.io/v1/namespaces/noetl/leases"
        );
    }

    #[test]
    fn renew_time_round_trips_through_millis() {
        let ms = rfc3339_to_millis("2026-09-19T21:02:55.411986Z").expect("parse");
        // Millisecond truncation of .411986 is .411
        assert_eq!(ms % 1000, 411);
        let s = millis_to_rfc3339(ms);
        assert_eq!(rfc3339_to_millis(&s), Some(ms), "the mapping must round-trip");
    }
}
