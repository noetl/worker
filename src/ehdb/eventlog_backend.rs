//! Event-log **storage-backend selection** (EHDB durable event-log backend,
//! slice 4 — worker wiring).
//!
//! Slices 1-3 built the production-durable substrate under the event-log tier
//! in `ehdb-reference`:
//!
//! * **slice 1** — [`DurableEventLogDriver`]: segmented, CRC-framed, fsync'd
//!   append files with an offset index and crash-recovery replay.
//! * **slice 2** — [`AffinityRoutedEventLog`]: execution-affinity single-writer
//!   routing over per-shard durable stores (owner appends; non-owner refused /
//!   cold-loads read-only). The ownership hash is byte-identical to the worker's
//!   own [`crate::sharding::shard_for`].
//! * **slice 3** — [`SharedTierEventLog`]: the owner publishes its per-shard
//!   segments to a shared durable medium ([`FilesystemSharedBackend`] — a PVC on
//!   kind, an object tier later) and a non-owner (or a new owner inheriting a
//!   shard with an empty local disk) cold-loads / hydrates them from the shared
//!   store, so a shard survives the loss of the writer's pod-local disk.
//!
//! This module is the worker's **selection seam** over that stack. The event-log
//! tier's *mode* axis (`off`/`shadow`/`primary`, [`super::eventlog::EventLogMode`])
//! decides *whether* EHDB serves; this *backend* axis
//! ([`EventLogStorageBackend`]) decides *which durable engine* does the append —
//! orthogonal, exactly as the `ehdb-reference` docs frame it.
//!
//! ## Disabled-by-default, reversible, zero behavior change when unset
//!
//! [`EventLogStorageBackend::from_raw`] is fail-safe: only the exact token
//! `durable_segment` selects the durable stack; unset / empty / unrecognised is
//! [`EventLogStorageBackend::LocalReference`] — the pod-local JSONL driver the
//! worker has always used. So a deployment that sets no
//! `NOETL_EHDB_EVENTLOG_BACKEND` appends byte-identically to before, and flipping
//! the env back to `local_reference` (or unsetting it) restores the incumbent
//! store with no redeploy. The durable stack is only ever constructed under the
//! same already-resolved data-plane contract (`worker`/`playbook`/`system` role,
//! `local_reference` integration runtime, a live log) that gates the JSONL path.
//!
//! ## What the durable stack persists — still the *derived* EHDB fabric
//!
//! Selecting `durable_segment` changes *where the mirrored/served event bytes
//! land* (segmented durable files + shared medium instead of a JSONL file); it
//! does **not** change event authorship. The event was already authored by the
//! gateway/server path; this only persists the already-authored event into the
//! EHDB event-log engine. The event-log-authoritative boundary the rest of
//! `src/ehdb/eventlog.rs` asserts is preserved.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ehdb_reference::{
    DurableSegmentStore, EventLogAppendOutcome, EventLogAppendRequest, EventLogDriver,
    EventLogScanRequest, EventLogStorageBackend, FilesystemSharedBackend,
    LocalReferenceEventLogDriver, Routed, ShardOwnership, SharedSegmentBackend, SharedTierEventLog,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::EhdbContract;
use super::eventlog::EventLogOptions;
use super::EnvMap;

/// Base directory for the durable per-shard segment stores + derived
/// shared/cold-load roots. Optional — when unset the base is derived from the
/// configured `local_reference` log's parent (so `durable_segment` always has a
/// usable root without a second required env), overridable to point at a
/// dedicated durable volume.
pub const DURABLE_DIR_ENV: &str = "NOETL_EHDB_EVENTLOG_DURABLE_DIR";

/// The shared-tier medium root (slice 3). Optional — defaults to a `shared/`
/// subdir under the durable base so the full slice-3 stack is always
/// constructed; override to point at the PVC / shared mount the pool agrees on.
pub const SHARED_DIR_ENV: &str = "NOETL_EHDB_EVENTLOG_SHARED_DIR";

/// The worker's shard-index env (this replica's `0..shard_count-1` bucket).
/// Matches [`crate::sharding::AffinityConfig::from_env`] so the durable
/// event-log shard ownership is byte-identical to the drive pool's execution
/// affinity — the same replica that owns the drive owns its event-log shard.
pub const WORKER_SHARD_INDEX_ENV: &str = "NOETL_SHARD_INDEX";
/// The worker's pool shard-count env. Matches
/// [`crate::sharding::AffinityConfig::from_env`].
pub const WORKER_SHARD_COUNT_ENV: &str = "NOETL_SHARD_COUNT";

/// Which storage engine the event-log tier appends through, resolved fail-safe
/// from `NOETL_EHDB_EVENTLOG_BACKEND` (default [`EventLogStorageBackend::LocalReference`]).
pub fn selected_backend(env: &EnvMap) -> EventLogStorageBackend {
    EventLogStorageBackend::from_raw(env.get(EventLogStorageBackend::ENV_VAR).map(|s| s.as_str()))
}

fn env_u32(env: &EnvMap, key: &str, default: u32) -> u32 {
    env.get(key)
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

/// Resolve the shard ownership for the durable event-log stack from the worker's
/// own affinity env — identical selection to
/// [`crate::sharding::AffinityConfig::from_env`]. An out-of-range index degrades
/// to the single-owner default (owns every execution) rather than erroring —
/// correctness never depends on the partition; a single writer is always safe.
pub fn ownership_from_env(env: &EnvMap) -> ShardOwnership {
    let shard_index = env_u32(env, WORKER_SHARD_INDEX_ENV, 0);
    let shard_count = env_u32(env, WORKER_SHARD_COUNT_ENV, 1).max(1);
    ShardOwnership::new(shard_index, shard_count).unwrap_or_else(|_| ShardOwnership::single_owner())
}

/// The resolved on-disk layout for the durable stack, all derived from one base
/// so the full slice-3 (segment + affinity + shared) composition is always
/// constructible from a single required knob.
#[derive(Debug, Clone)]
pub struct DurablePaths {
    /// Local per-shard store root (owned-shard fast path + hydrate target).
    pub local_root: PathBuf,
    /// The shared durable medium root (owner publish target / non-owner source).
    pub shared_root: PathBuf,
    /// Scratch root under which non-owner cold-loads materialize shared segments.
    pub coldload_root: PathBuf,
}

impl DurablePaths {
    /// Resolve the layout from the env + the resolved contract's log path.
    ///
    /// The base is `NOETL_EHDB_EVENTLOG_DURABLE_DIR` when set, else
    /// `<log-parent>/ehdb-durable` derived from the `local_reference` log so
    /// `durable_segment` never requires a second env to be usable. `shared_root`
    /// is `NOETL_EHDB_EVENTLOG_SHARED_DIR` when set, else `<base>/shared`.
    pub fn resolve(env: &EnvMap, contract: &EhdbContract) -> Self {
        let base = env
            .get(DURABLE_DIR_ENV)
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| durable_base_from_log(contract.local_reference_log.as_deref()));
        let shared_root = env
            .get(SHARED_DIR_ENV)
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| base.join("shared"));
        DurablePaths {
            local_root: base.join("local"),
            shared_root,
            coldload_root: base.join("coldload"),
        }
    }
}

/// Derive the durable base dir from the configured JSONL log path: its parent
/// directory + `ehdb-durable`. Falls back to a relative `ehdb-durable` when the
/// log has no parent (defensive — the contract always carries an absolute log in
/// practice).
fn durable_base_from_log(log: Option<&Path>) -> PathBuf {
    match log.and_then(|p| p.parent()) {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join("ehdb-durable"),
        _ => PathBuf::from("ehdb-durable"),
    }
}

/// Construct the full durable stack: [`SharedTierEventLog`] = shared-tier
/// (slice 3) over affinity single-writer routing (slice 2) over per-shard
/// [`DurableEventLogDriver`] segment stores (slice 1), pinned to this replica's
/// [`ownership_from_env`] and pointed at [`DurablePaths`].
pub fn build_durable_stack(
    env: &EnvMap,
    contract: &EhdbContract,
) -> Result<SharedTierEventLog, String> {
    let paths = DurablePaths::resolve(env, contract);
    let ownership = ownership_from_env(env);
    let shared: Arc<dyn SharedSegmentBackend> =
        Arc::new(FilesystemSharedBackend::open(&paths.shared_root).map_err(|e| e.to_string())?);
    SharedTierEventLog::open(&paths.local_root, ownership, shared, &paths.coldload_root)
        .map_err(|e| e.to_string())
}

/// The append dispatch outcome, normalized so the caller's parity path is
/// backend-agnostic.
pub enum AppendDispatch {
    /// The append was served (by whichever backend). Carries the same
    /// [`EventLogAppendOutcome`] shape both backends produce.
    Served(EventLogAppendOutcome),
    /// The durable stack refused the append because this replica does not own
    /// the execution's shard (single-writer routing). Never happens on the
    /// local-reference backend or under the single-owner default.
    RoutedAway { owner_shard: u32 },
}

/// Append one already-authored event through the *selected* backend.
///
/// * [`EventLogStorageBackend::LocalReference`] (default) — byte-identical to
///   the incumbent: open a [`LocalReferenceEventLogDriver`] over the JSONL log
///   and append.
/// * [`EventLogStorageBackend::DurableSegment`] — build the durable stack
///   ([`build_durable_stack`]) and route the append through affinity
///   single-writer + shared-tier publish; an owned shard is [`AppendDispatch::Served`],
///   a non-owner is [`AppendDispatch::RoutedAway`].
///
/// The stack is constructed per-op and dropped (stateless boundary, matching the
/// incumbent JSONL path): the durable store replays its existing segments on
/// open (crash-recovery) so the sequence continues correctly across ops. Errors
/// are returned as `String` so the caller's `classify_helper_error` (which keys
/// on the `invalid identifier` Display prefix) works uniformly across backends.
pub fn append_selected(
    env: &EnvMap,
    contract: &EhdbContract,
    request: &EventLogAppendRequest,
    opts: &EventLogOptions,
    backend: EventLogStorageBackend,
) -> Result<AppendDispatch, String> {
    match backend {
        EventLogStorageBackend::LocalReference => {
            let driver = LocalReferenceEventLogDriver::new(
                contract
                    .local_reference_log
                    .clone()
                    .expect("contract carries a local_reference log"),
                opts.tenant
                    .clone()
                    .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string()),
                opts.namespace
                    .clone()
                    .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string()),
            );
            driver
                .append(request)
                .map(AppendDispatch::Served)
                .map_err(|e| e.to_string())
        }
        EventLogStorageBackend::DurableSegment => {
            let stack = build_durable_stack(env, contract)?;
            match stack.append(request).map_err(|e| e.to_string())? {
                Routed::Served(outcome) => Ok(AppendDispatch::Served(outcome)),
                Routed::NotOwner { owner_shard } => Ok(AppendDispatch::RoutedAway { owner_shard }),
            }
        }
    }
}

/// Read-back proof primitive for the durable backend: how many records the
/// durable segment store holds for `execution_id`'s owning shard, opened
/// **read-only from disk** (a fresh reader replays the segments = crash-recovery
/// proof). Resolves the same [`DurablePaths`] + [`ownership_from_env`] the append
/// path uses, so it reads exactly what `durable_segment` wrote. Errors (a
/// yet-uncreated shard store, an I/O failure) surface as `String`.
///
/// Used by `ehdb-selfcheck durable-eventlog` to prove appended events land in
/// durable segments (not the JSONL log), independently reopened.
pub fn durable_shard_record_count(
    env: &EnvMap,
    contract: &EhdbContract,
    execution_id: &str,
) -> Result<usize, String> {
    let paths = DurablePaths::resolve(env, contract);
    let shard = ownership_from_env(env).shard_of(execution_id);
    let shard_dir = paths.local_root.join(format!("shard-{shard:04}"));
    let store = DurableSegmentStore::open_read_only(&shard_dir).map_err(|e| e.to_string())?;
    let scan = store
        .scan_global(&EventLogScanRequest {
            after: None,
            limit: 4096,
        })
        .map_err(|e| e.to_string())?;
    Ok(scan.record_count)
}

#[cfg(test)]
mod tests {
    // `DurableSegmentStore` / `EventLogScanRequest` come in via `super::*`
    // (imported at module top for `durable_shard_record_count`).
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-elb-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn contract_for(log: &Path) -> EhdbContract {
        use super::super::contract::{EhdbClientRole, EhdbIntegrationMode};
        EhdbContract {
            enabled: true,
            mode: EhdbIntegrationMode::LocalReference,
            role: EhdbClientRole::Worker,
            capabilities: Default::default(),
            local_reference_log: Some(log.to_path_buf()),
        }
    }

    fn req(execution_id: &str, payload: &str) -> EventLogAppendRequest {
        EventLogAppendRequest {
            execution_id: execution_id.to_string(),
            transaction_id: format!("txn-{execution_id}-{}", payload.len()),
            payload: payload.to_string(),
        }
    }

    #[test]
    fn default_backend_is_local_reference() {
        assert_eq!(
            selected_backend(&env(&[])),
            EventLogStorageBackend::LocalReference
        );
        assert_eq!(
            selected_backend(&env(&[("NOETL_EHDB_EVENTLOG_BACKEND", "local_reference")])),
            EventLogStorageBackend::LocalReference
        );
    }

    #[test]
    fn durable_segment_selected_only_on_exact_token() {
        assert_eq!(
            selected_backend(&env(&[("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment")])),
            EventLogStorageBackend::DurableSegment
        );
        // Fail-safe: an unknown value is local_reference, never silently durable.
        assert_eq!(
            selected_backend(&env(&[("NOETL_EHDB_EVENTLOG_BACKEND", "bogus")])),
            EventLogStorageBackend::LocalReference
        );
    }

    #[test]
    fn ownership_matches_worker_affinity_env() {
        // Single-owner default when unset.
        let o = ownership_from_env(&env(&[]));
        assert_eq!(o.shard_count(), 1);
        assert!(o.owns_execution("478775660589088776"));
        // A real 2-shard partition reads the worker's own env names.
        let o = ownership_from_env(&env(&[
            ("NOETL_SHARD_INDEX", "1"),
            ("NOETL_SHARD_COUNT", "2"),
        ]));
        assert_eq!(o.shard_index(), 1);
        assert_eq!(o.shard_count(), 2);
    }

    #[test]
    fn out_of_range_index_degrades_to_single_owner() {
        let o = ownership_from_env(&env(&[
            ("NOETL_SHARD_INDEX", "5"),
            ("NOETL_SHARD_COUNT", "2"),
        ]));
        assert_eq!(o.shard_count(), 1);
    }

    #[test]
    fn durable_paths_derive_from_log_when_unset() {
        let dir = tmp_dir("paths");
        let log = dir.join("log.jsonl");
        let paths = DurablePaths::resolve(&env(&[]), &contract_for(&log));
        assert_eq!(paths.local_root, dir.join("ehdb-durable").join("local"));
        assert_eq!(paths.shared_root, dir.join("ehdb-durable").join("shared"));
        assert_eq!(
            paths.coldload_root,
            dir.join("ehdb-durable").join("coldload")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_paths_honor_explicit_env() {
        let dir = tmp_dir("paths-env");
        let log = dir.join("log.jsonl");
        let e = env(&[
            (
                "NOETL_EHDB_EVENTLOG_DURABLE_DIR",
                dir.join("d").to_str().unwrap(),
            ),
            (
                "NOETL_EHDB_EVENTLOG_SHARED_DIR",
                dir.join("s").to_str().unwrap(),
            ),
        ]);
        let paths = DurablePaths::resolve(&e, &contract_for(&log));
        assert_eq!(paths.local_root, dir.join("d").join("local"));
        assert_eq!(paths.shared_root, dir.join("s"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_reference_append_lands_in_jsonl_not_segments() {
        let dir = tmp_dir("local-land");
        let log = dir.join("log.jsonl");
        let contract = contract_for(&log);
        let d = append_selected(
            &env(&[]),
            &contract,
            &req("100", "{\"seq\":1}"),
            &EventLogOptions::default(),
            EventLogStorageBackend::LocalReference,
        )
        .unwrap();
        match d {
            AppendDispatch::Served(o) => assert_eq!(o.global_sequence, 1),
            _ => panic!("local reference always serves"),
        }
        // The JSONL log exists; no durable segment tree was created.
        assert!(log.exists(), "local reference writes the JSONL log");
        assert!(
            !dir.join("ehdb-durable").exists(),
            "no durable segments on local backend"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_append_lands_in_segments_not_jsonl() {
        let dir = tmp_dir("durable-land");
        let log = dir.join("log.jsonl");
        let contract = contract_for(&log);
        let e = env(&[("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment")]);
        // Append three events for one execution through the durable stack.
        for seq in 1..=3u64 {
            let d = append_selected(
                &e,
                &contract,
                &req("100", &format!("{{\"seq\":{seq}}}")),
                &EventLogOptions::default(),
                EventLogStorageBackend::DurableSegment,
            )
            .unwrap();
            match d {
                AppendDispatch::Served(o) => {
                    assert_eq!(o.global_sequence, seq, "gapless per shard")
                }
                AppendDispatch::RoutedAway { .. } => {
                    panic!("single-owner default owns every shard")
                }
            }
        }
        // Durable segments exist under the derived local root; the JSONL log does not.
        let paths = DurablePaths::resolve(&e, &contract);
        assert!(paths.local_root.exists(), "durable local root created");
        assert!(!log.exists(), "durable backend never writes the JSONL log");
        // Segments published to the shared medium too (slice 3).
        assert!(paths.shared_root.exists(), "shared tier root created");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_append_survives_crash_recovery_replay() {
        // Append through the stack, then reopen a fresh read-only durable store
        // over the same shard-0 dir (simulated pod restart) and prove zero-loss
        // replay from the segments alone.
        let dir = tmp_dir("durable-recover");
        let log = dir.join("log.jsonl");
        let contract = contract_for(&log);
        let e = env(&[("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment")]);
        for seq in 1..=4u64 {
            append_selected(
                &e,
                &contract,
                &req("100", &format!("{{\"seq\":{seq}}}")),
                &EventLogOptions::default(),
                EventLogStorageBackend::DurableSegment,
            )
            .unwrap();
        }
        let paths = DurablePaths::resolve(&e, &contract);
        // Single-owner default → shard 0.
        let shard0 = paths.local_root.join("shard-0000");
        let store = DurableSegmentStore::open_read_only(&shard0).unwrap();
        let scan = store
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 16,
            })
            .unwrap();
        assert_eq!(scan.record_count, 4, "reopened store replays all 4 events");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_append_respects_single_writer_routing() {
        // Two replicas over one shared store, shard_count=2. Each replica only
        // owns half the executions; an append for a non-owned execution routes
        // away with no side effect.
        let dir = tmp_dir("durable-affinity");
        let log = dir.join("log.jsonl");
        let contract = contract_for(&log);
        let shared = dir.join("shared");
        // Find an execution owned by shard 1 (so shard 0 routes it away).
        let owner1 = ownership_from_env(&env(&[
            ("NOETL_SHARD_COUNT", "2"),
            ("NOETL_SHARD_INDEX", "1"),
        ]));
        let exec = (1000i64..)
            .map(|n| n.to_string())
            .find(|id| owner1.owns_execution(id))
            .unwrap();
        // Replica 0 does not own it → RoutedAway.
        let e0 = env(&[
            ("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment"),
            (
                "NOETL_EHDB_EVENTLOG_DURABLE_DIR",
                dir.join("r0").to_str().unwrap(),
            ),
            ("NOETL_EHDB_EVENTLOG_SHARED_DIR", shared.to_str().unwrap()),
            ("NOETL_SHARD_COUNT", "2"),
            ("NOETL_SHARD_INDEX", "0"),
        ]);
        let d0 = append_selected(
            &e0,
            &contract,
            &req(&exec, "{\"x\":1}"),
            &EventLogOptions::default(),
            EventLogStorageBackend::DurableSegment,
        )
        .unwrap();
        assert!(matches!(d0, AppendDispatch::RoutedAway { owner_shard: 1 }));
        // Replica 1 owns it → Served.
        let e1 = env(&[
            ("NOETL_EHDB_EVENTLOG_BACKEND", "durable_segment"),
            (
                "NOETL_EHDB_EVENTLOG_DURABLE_DIR",
                dir.join("r1").to_str().unwrap(),
            ),
            ("NOETL_EHDB_EVENTLOG_SHARED_DIR", shared.to_str().unwrap()),
            ("NOETL_SHARD_COUNT", "2"),
            ("NOETL_SHARD_INDEX", "1"),
        ]);
        let d1 = append_selected(
            &e1,
            &contract,
            &req(&exec, "{\"x\":1}"),
            &EventLogOptions::default(),
            EventLogStorageBackend::DurableSegment,
        )
        .unwrap();
        assert!(matches!(d1, AppendDispatch::Served(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
