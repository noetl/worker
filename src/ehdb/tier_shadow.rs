//! Route the KV and object **shadow** mirrors to the durable tier store.
//!
//! **[noetl/ai-meta#348](https://github.com/noetl/ai-meta/issues/348).**
//!
//! # The defect this closes
//!
//! Both tiers wrote their shadow records under
//! `NOETL_EHDB_LOCAL_REFERENCE_LOG`, which on production is `/tmp/ehdb/ref.jsonl`
//! — the container's writable layer, with no volumeMount. Measured across three
//! prod pods, every store had been created within ~3 minutes of its own pod's
//! start and a pod rolled minutes earlier had none at all. The shadow tiers were
//! **destroyed on every pod roll**.
//!
//! A shadow tier exists to accumulate the evidence that justifies a cutover. One
//! that resets on every roll accumulates nothing, which is why both the
//! tier-service read path and a parity comparator were unbuildable before this.
//!
//! # What is stored, and what deliberately is not
//!
//! **Identity + digest + size. Not the payload.**
//!
//! * The object tier's payloads are blobs — megabytes. Putting them into a JSONL
//!   append log that shares a writer process with the event log would put the
//!   tier serving `primary` in production behind blob I/O.
//! * A shadow tier holding a second full copy of authoritative data raises its
//!   own questions (retention, PII, cost) that a parity record does not.
//!
//! ⚠ **This shape supports a COMPARATOR, not a serve path.** A cutover that
//! served reads from these tiers would need the values, and that is a different
//! store and a different decision — the one the owner reserved
//! ([noetl/ehdb#321](https://github.com/noetl/ehdb/issues/321)). Nothing here
//! moves toward it: `kv` and `object` stay out of `SERVE_WIRED_TIERS`, and three
//! tests fail if that changes.
//!
//! # Best-effort, always
//!
//! Every function here swallows its failures and meters them. KV's incumbent is
//! NATS-KV and object's is the external object store; both remain authoritative,
//! and a shadow append that cannot reach the writer must never affect the write
//! that already succeeded. The call sites reflect that: the tier append happens
//! **after** the authoritative write is durable, and its result is discarded.

use super::store_tier::StoreTier;
use super::tier_client::TierClient;

/// The partition key used when a record carries no execution scope.
///
/// A literal rather than an empty string: `tier_store::read_execution` refuses an
/// empty `execution_id` as `Invalid`, so an unscoped record would be silently
/// dropped at the boundary instead of stored under a readable key.
pub const UNSCOPED_PARTITION: &str = "_unscoped";

/// Extract the `execution=<id>` segment from a §7 object key.
///
/// Result-tier and state-shard keys are partitioned by execution, and storing the
/// shadow record under the same partition is what lets a later comparator read
/// one execution's records back with a single `read_execution` rather than
/// scanning the whole log.
///
/// Keys with no execution segment (the object tier is not execution-only) fall
/// back to [`UNSCOPED_PARTITION`].
pub fn partition_of_object_key(key: &str) -> String {
    key.split('/')
        .find_map(|seg| seg.strip_prefix("execution="))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| UNSCOPED_PARTITION.to_string())
}

/// SHA-256 of `bytes`, hex — the same digest the object store addresses by, so a
/// comparator can compare the shadow record against the authoritative object
/// without fetching either payload.
pub fn digest_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// The shadow record for one object write.
pub fn object_record(key: &str, bytes: &[u8]) -> String {
    serde_json::json!({
        "tier": "object",
        "key": key,
        "digest": digest_hex(bytes),
        "byte_len": bytes.len(),
    })
    .to_string()
}

/// The shadow record for one KV write.
///
/// The value is digested rather than carried, for the same reason the object
/// tier's is: this store is parity evidence, not a second copy.
pub fn kv_record(bucket: &str, key: &str, value: &str) -> String {
    serde_json::json!({
        "tier": "kv",
        "bucket": bucket,
        "key": key,
        "digest": digest_hex(value.as_bytes()),
        "byte_len": value.len(),
    })
    .to_string()
}

/// Append one shadow record to the durable tier store, best-effort.
///
/// Returns the outcome label that was metered, so a caller (and a test) can see
/// which path ran without reading the metric registry. `None` when the tier
/// client is not configured — a strict no-op, byte-identical to the behaviour
/// before this module existed.
pub async fn append(tier: StoreTier, partition: &str, record: &str) -> Option<&'static str> {
    let client = TierClient::from_env()?;
    let started = std::time::Instant::now();
    let reply = client.append_tier(tier, partition, record).await;
    let elapsed = started.elapsed().as_secs_f64();
    // ⚠ Inspect the reply. The catalog arm originally reported `appended` for a
    // reply it never read, so appends a writer REFUSED were scored as stored and
    // the records were lost silently during a rolling upgrade. Same trap, same
    // shape, so the same discipline.
    let label = super::store_tier::append_label(reply.as_deref().map_err(String::as_str));
    let ok = reply.is_ok();
    super::metrics::record_tier_client(
        &format!("shadow_append.{}", tier.as_str()),
        label,
        ok,
        !ok,
        elapsed,
    );
    if let Err(e) = &reply {
        tracing::debug!(
            tier = tier.as_str(), partition, error = %e,
            "EHDB shadow tier append failed (non-fatal; the authoritative write already succeeded)"
        );
    }
    Some(label)
}

/// Mirror one object write into the durable object shadow tier.
pub async fn mirror_object(key: &str, bytes: &[u8]) -> Option<&'static str> {
    append(
        StoreTier::Object,
        &partition_of_object_key(key),
        &object_record(key, bytes),
    )
    .await
}

/// Mirror one KV write into the durable KV shadow tier.
///
/// Partitioned by **bucket**: KV writes carry no execution scope, and a bucket is
/// the closest thing to one — it keeps a comparator's read bounded instead of
/// making every KV parity check a full-log scan.
pub async fn mirror_kv(bucket: &str, key: &str, value: &str) -> Option<&'static str> {
    append(StoreTier::Kv, bucket, &kv_record(bucket, key, value)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_execution_scoped_key_partitions_by_execution() {
        // A real §7 result-tier key, from the prod GCS bucket.
        let k = "noetl/env=prod/region=usc1/cell=usc1-a/shard=s0004/tenant=default/\
                 project=default/date=2026-09-15/execution=358337687603650560/results/\
                 hotelbeds_dispatch/0/0/1.json";
        assert_eq!(partition_of_object_key(k), "358337687603650560");
    }

    #[test]
    fn a_key_without_an_execution_segment_is_unscoped_not_empty() {
        // `tier_store::read_execution` refuses an empty execution_id as Invalid,
        // so an empty partition would drop the record at the boundary rather than
        // store it somewhere readable.
        for k in ["noetl/some/other/key.bin", "", "execution=/x"] {
            assert_eq!(
                partition_of_object_key(k),
                UNSCOPED_PARTITION,
                "{k:?} must fall back to a readable partition"
            );
        }
        assert!(!UNSCOPED_PARTITION.trim().is_empty());
    }

    #[test]
    fn the_record_carries_identity_and_digest_but_never_the_payload() {
        let payload = b"the quick brown fox";
        let rec = object_record("noetl/execution=7/x.bin", payload);
        let v: serde_json::Value = serde_json::from_str(&rec).unwrap();
        assert_eq!(v["key"], "noetl/execution=7/x.bin");
        assert_eq!(v["byte_len"], payload.len());
        assert_eq!(v["digest"], digest_hex(payload));
        // THE POINT: a megabyte blob must not end up in a JSONL append log that
        // shares a writer process with the tier serving primary in production.
        assert!(
            !rec.contains("quick brown fox"),
            "the shadow record carries the payload: {rec}"
        );
    }

    #[test]
    fn a_kv_record_digests_the_value_rather_than_carrying_it() {
        let rec = kv_record("circuit", "k1", "super-secret-value");
        let v: serde_json::Value = serde_json::from_str(&rec).unwrap();
        assert_eq!(v["bucket"], "circuit");
        assert_eq!(v["key"], "k1");
        assert_eq!(v["digest"], digest_hex(b"super-secret-value"));
        assert!(
            !rec.contains("super-secret-value"),
            "a shadow tier holding a second copy of authoritative values raises \
             retention and PII questions a parity record does not: {rec}"
        );
    }

    #[test]
    fn digests_distinguish_different_bytes_and_agree_on_equal_ones() {
        assert_eq!(digest_hex(b"a"), digest_hex(b"a"));
        assert_ne!(digest_hex(b"a"), digest_hex(b"b"));
        // Pinned against a known SHA-256 so a swapped hash function is caught
        // rather than silently changing what every comparison means.
        assert_eq!(
            digest_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// Not configured ⇒ strict no-op, so this module is byte-identical to the
    /// behaviour before it existed until an operator points a worker at a tier
    /// service.
    #[tokio::test]
    async fn an_unconfigured_tier_client_is_a_no_op() {
        // `TierClient::from_env` reads the process env; with the address unset
        // there is no client and nothing is attempted.
        let prev = std::env::var(super::super::tier_store::TIER_SERVICE_DIR_ENV).ok();
        let _ = prev; // the read below is what matters, not the store dir
        if std::env::var("NOETL_EHDB_TIER_SERVICE_ADDR").is_ok() {
            // A developer machine with the var set would make this vacuous.
            return;
        }
        assert!(append(StoreTier::Object, "e1", "{}").await.is_none());
        assert!(mirror_kv("b", "k", "v").await.is_none());
    }
}
