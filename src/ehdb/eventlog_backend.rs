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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use ehdb_reference::fencing as ehdb_fencing;
use ehdb_reference::fencing::FencingMetrics;
use ehdb_reference::{
    DurableSegmentStore, EventLogAppendOutcome, EventLogAppendRequest, EventLogDriver,
    EventLogScanRequest, EventLogStorageBackend, FilesystemSharedBackend,
    LocalReferenceEventLogDriver, Routed, SegmentGcPolicy, ShardOwnership, SharedSegmentBackend,
    SharedShardGcOutcome, SharedTierEventLog, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};

use super::contract::EhdbContract;
use super::eventlog::EventLogOptions;
use super::EnvMap;

/// Process-global per-shard advisory lock registry. The durable backend's
/// single-writer invariant is enforced *across replicas* by execution-affinity,
/// but **within** a replica the durable append path and the periodic segment-GC
/// path are two writers to the same shard's segment files (GC write-forwards
/// consumer state + unlinks sealed segments; an append writes the active
/// segment). Both acquire this per-shard lock so they never interleave — GC's
/// reclamation is serialized against appends on the *same* shard, while appends
/// (and GC) on *other* shards run unblocked. A side benefit: it also closes a
/// latent intra-replica append↔append race on one shard's active segment.
///
/// Only the `durable_segment` backend touches this; the default `local_reference`
/// path is unchanged.
fn shard_lock(shard: u32) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<u32, Arc<Mutex<()>>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .entry(shard)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

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

/// The per-shard segment rollover threshold. Optional — defaults to the engine's
/// 8 MiB ([`ehdb_reference::DEFAULT_SEGMENT_MAX_BYTES`]). A smaller value rotates
/// segments more often (useful to exercise / observe segment GC without driving
/// 8 MiB of events per rotation); changing it is safe on an existing store (it
/// only affects new rotations — replay is size-agnostic).
pub const SEGMENT_MAX_BYTES_ENV: &str = "NOETL_EHDB_EVENTLOG_SEGMENT_MAX_BYTES";

/// Stale-writer fencing over the shared segment store (noetl/ehdb#330).
///
/// `off` (the default, and any unrecognised value) does not wrap the backend at
/// all — byte-for-byte today's behaviour. `shadow` wraps it and **counts** a
/// write from an epoch below the shard's highest accepted epoch while letting it
/// through. `enforce` refuses it.
///
/// ⚠⚠ `enforce` is **owner-gated**: it changes what the store does, not just
/// what it reports.
///
/// ⚠ Until an election issues real tokens (noetl/ehdb#331) every writer's epoch
/// is `0`, so `shadow` can only ever observe `0 < 0` — false — and will record
/// zero stale writes. That zero is *meaningful only because* `writes_checked`
/// climbs beside it; a zero with a flat `writes_checked` says the decorator is
/// unreached, not that the system is healthy.
pub const FENCING_ENV: &str = "NOETL_EHDB_FENCING";

/// Process-wide fencing counters.
///
/// ⚠ [`build_durable_stack`] constructs the whole stack **per operation** and
/// drops it, so per-instance counters would be discarded on every call. These
/// have to outlive the stack to mean anything.
pub static FENCING_METRICS: std::sync::LazyLock<Arc<FencingMetrics>> =
    std::sync::LazyLock::new(FencingMetrics::new);

/// Whether a shard-lease election is running and issuing fencing tokens.
///
/// ⚠⚠ **Always 0 today, and that is the point.** `ShardElection`
/// (noetl/ehdb#331) is implemented, tested and merged — and has **no call sites
/// anywhere**. Without it every writer's epoch is `0`, which has two
/// consequences an operator cannot otherwise see:
///
/// * single-writer-per-shard still rests entirely on `StatefulSet replicas: 1`,
///   an orchestration preference rather than a mutual-exclusion primitive; and
/// * fencing in `enforce` mode would be an **outage**, not a degradation,
///   because the first writer to advance the marker fences every other one.
///
/// A dead feature that reports nothing is indistinguishable from a live one that
/// has nothing to report. This gauge makes the difference legible on the scrape
/// instead of requiring someone to grep the source.
///
/// ⚠ It is deliberately **not** wired to a fake election. The Kubernetes
/// `LeaseStore` adapter needs an HTTP/`kube` dependency decision; driving the
/// in-memory store here would publish a token nothing else honours and a `1`
/// that means nothing.
pub fn render_election() -> String {
    let mut out = String::new();
    out.push_str("# HELP ehdb_election_active Whether a shard-lease election is running and issuing fencing tokens. 0 means single-writer rests on StatefulSet replicas:1 alone, and fencing enforce would be an outage.\n");
    out.push_str("# TYPE ehdb_election_active gauge\n");
    out.push_str("ehdb_election_active 0\n");
    out.push_str("# HELP ehdb_election_epoch The fencing token this writer holds. 0 means no token has been issued.\n");
    out.push_str("# TYPE ehdb_election_epoch gauge\n");
    out.push_str("ehdb_election_epoch 0\n");
    out
}

/// The age-based seal trigger (noetl/ehdb#329), read from the process env.
///
/// `None` — the default, and any unparsable value — is today's behaviour: seal on
/// size or record count only, which leaves the durability window **unbounded in
/// time** on a shard that goes quiet.
///
/// ⚠⚠ Setting this is **not sufficient on its own**. `should_seal()` is only
/// consulted on append, so the flag bounds the window on a shard that keeps
/// taking traffic and does **nothing** for an idle one — which is precisely the
/// shard it exists to protect. Bounding an idle shard needs a timer driving
/// `L0Engine::seal_aged_parts()`, and that timer touches the live writer loop, so
/// it stays owner-gated.
///
/// Plumbing the knob is inert: unset means `None` means unchanged. Before this it
/// was not read at all, so setting it on prod would have silently done nothing.
pub const SEAL_MAX_AGE_ENV: &str = "NOETL_EHDB_SEAL_MAX_AGE_MS";

/// Spawn the age-seal sweep for a writer, **only when the trigger is set**.
///
/// WHY a sweep is needed at all: `should_seal()` is consulted only on append, so
/// a shard that stops taking traffic never re-evaluates, and its records sit
/// unreplicated indefinitely -- which is precisely the shard the age trigger
/// exists to protect. The flag without this is inert on exactly the case it was
/// added for.
///
/// Returns `None` when `NOETL_EHDB_SEAL_MAX_AGE_MS` is unset, so nothing is
/// spawned and the engine lock is never taken on a schedule. "Switched off" has
/// to cost nothing, or it is not a real rollback.
pub fn spawn_seal_age_sweep<D>(
    engine: std::sync::Arc<std::sync::Mutex<ehdb_l0::L0Engine<D>>>,
    label: &'static str,
    shard: u32,
) -> Option<tokio::task::JoinHandle<()>>
where
    D: ehdb_l0::Dataset + Send + 'static,
{
    let age = seal_max_age_from_env()?;
    let every = seal_sweep_interval();
    tracing::info!(
        %label,
        shard,
        seal_max_age_ms = age.as_millis() as u64,
        sweep_every_ms = every.as_millis() as u64,
        "EHDB age-seal sweep armed (noetl/ehdb#329)"
    );
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            // Hold the engine lock only for the sweep itself. `seal_aged_parts`
            // short-circuits when nothing has aged out, so the common case is a
            // cheap scan of the writer map.
            let sealed = match engine.lock() {
                Ok(mut e) => e.seal_aged_parts(),
                // A poisoned lock means a writer thread panicked; the sweep is
                // not the place to decide what that means.
                Err(_) => continue,
            };
            match sealed {
                Ok(n) if n > 0 => {
                    tracing::debug!(%label, shard, sealed = n, "age-seal sweep sealed parts")
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(%label, shard, error = %e, "age-seal sweep failed")
                }
            }
        }
    }))
}

/// How often the writer sweeps for aged-out parts.
///
/// Only meaningful when [`SEAL_MAX_AGE_ENV`] is set. Default 1s — comfortably
/// under the 5s trigger, so a bounded window is bounded promptly rather than
/// within one tick of the limit.
pub const SEAL_SWEEP_INTERVAL_MS_ENV: &str = "NOETL_EHDB_SEAL_SWEEP_INTERVAL_MS";

/// The sweep interval, defaulting to 1s.
pub fn seal_sweep_interval() -> std::time::Duration {
    std::env::var(SEAL_SWEEP_INTERVAL_MS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(std::time::Duration::from_millis)
        .unwrap_or_else(|| std::time::Duration::from_millis(1_000))
}

/// Parse [`SEAL_MAX_AGE_ENV`] from the process env.
pub fn seal_max_age_from_env() -> Option<std::time::Duration> {
    std::env::var(SEAL_MAX_AGE_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(std::time::Duration::from_millis)
}

/// The replica-domain observation, computed where the real paths are known.
///
/// ⚠ **Observation only — this never refuses anything.** It uses
/// `check_replica_domains` (which returns findings) rather than
/// `validate_replica_domains` (which errors), because refusing to open the
/// durable stack on a domain violation would be a startup outage, and prod's
/// current layout *does* violate it: the shared root sits inside the writer's
/// own data dir on one PVC.
///
/// It exists to make noetl/ehdb#332's G4 verify-before **evaluable from the
/// running system** instead of by reading manifests. Before this, nothing
/// computed the domains against the live paths at all.
pub static REPLICA_DOMAINS: std::sync::OnceLock<DomainObservation> = std::sync::OnceLock::new();

/// What the live paths look like as failure domains.
#[derive(Debug, Clone)]
pub struct DomainObservation {
    pub shared: ehdb_l0::FailureDomain,
    pub local: ehdb_l0::FailureDomain,
    pub violations: Vec<ehdb_l0::DomainViolation>,
    pub survives_node_loss: bool,
}

impl DomainObservation {
    fn of(local_root: &Path, shared_root: &Path) -> Self {
        let replicas = vec![
            ehdb_l0::ReplicaDomain {
                replica: "writer-local".to_string(),
                domain: ehdb_l0::FailureDomain::for_path(local_root),
                root: Some(local_root.to_path_buf()),
            },
            ehdb_l0::ReplicaDomain {
                replica: "shared-substrate".to_string(),
                domain: ehdb_l0::FailureDomain::for_path(shared_root),
                root: Some(shared_root.to_path_buf()),
            },
        ];
        Self {
            local: replicas[0].domain.clone(),
            shared: replicas[1].domain.clone(),
            violations: ehdb_l0::check_replica_domains(&replicas),
            survives_node_loss: ehdb_l0::survives_node_loss(&replicas),
        }
    }
}

/// Render the replica-domain observation.
///
/// ⚠ Pinned: emitted with a count of 0 when clean, so "no violations" is
/// distinguishable from "never computed". `ehdb_replica_domains_observed` says
/// which of those it is.
pub fn render_replica_domains() -> String {
    let mut out = String::new();
    out.push_str("# HELP ehdb_replica_domains_observed Whether the durable stack has been opened and its replica failure domains computed.\n");
    out.push_str("# TYPE ehdb_replica_domains_observed gauge\n");
    let obs = REPLICA_DOMAINS.get();
    out.push_str(&format!(
        "ehdb_replica_domains_observed {}\n",
        u8::from(obs.is_some())
    ));

    out.push_str("# HELP ehdb_replica_domain_violations Replica-set failure-domain violations by kind. A non-zero shared_domain or nested_path means replication buys no independent failure domain.\n");
    out.push_str("# TYPE ehdb_replica_domain_violations gauge\n");
    let (mut shared_domain, mut nested, mut undeclared) = (0u32, 0u32, 0u32);
    if let Some(o) = obs {
        for v in &o.violations {
            match v {
                ehdb_l0::DomainViolation::SharedDomain { .. } => shared_domain += 1,
                ehdb_l0::DomainViolation::NestedPath { .. } => nested += 1,
                ehdb_l0::DomainViolation::Undeclared { .. } => undeclared += 1,
            }
        }
    }
    // ⚠ All three label values pinned, so a clean reading is 0 rather than an
    // absent series — the label set is closed, so this is exactly the case
    // pinning is for.
    for (kind, n) in [
        ("shared_domain", shared_domain),
        ("nested_path", nested),
        ("undeclared", undeclared),
    ] {
        out.push_str(&format!(
            "ehdb_replica_domain_violations{{kind=\"{kind}\"}} {n}\n"
        ));
    }

    out.push_str("# HELP ehdb_replica_survives_node_loss Whether any replica lives in a failure domain independent of this node. 0 means losing the node loses every copy.\n");
    out.push_str("# TYPE ehdb_replica_survives_node_loss gauge\n");
    out.push_str(&format!(
        "ehdb_replica_survives_node_loss {}\n",
        u8::from(obs.map(|o| o.survives_node_loss).unwrap_or(false))
    ));
    out
}

/// Whether fencing wraps the shared store, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FencingSetting {
    /// Not wrapped at all — today's behaviour.
    #[default]
    Off,
    /// Wrapped, counting stale epochs, refusing nothing.
    Shadow,
    /// ⚠⚠ Wrapped and refusing. Owner-gated.
    Enforce,
}

impl FencingSetting {
    /// Parse from the env map. ⚠ Anything unrecognised is [`Self::Off`] — the
    /// fail-safe direction here is "change nothing", not "start refusing".
    pub fn from_env(env: &EnvMap) -> Self {
        match env
            .get(FENCING_ENV)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("shadow") => Self::Shadow,
            Some("enforce") => Self::Enforce,
            _ => Self::Off,
        }
    }

    fn wraps(self) -> bool {
        !matches!(self, Self::Off)
    }
}

/// Render the fencing counters.
///
/// ⚠ Emitted **whatever the setting**, including `off`. A family that appears
/// only once fencing is enabled would make "not wrapped" indistinguishable from
/// "wrapped and quiet" — and the whole point of the shadow period is being able
/// to tell those apart. `ehdb_fencing_active` says which state this is.
pub fn render_fencing(setting: FencingSetting) -> String {
    let mode = match setting {
        FencingSetting::Enforce => ehdb_fencing::FencingMode::Enforce,
        _ => ehdb_fencing::FencingMode::Shadow,
    };
    let mut out = FENCING_METRICS.render_prometheus(mode);
    out.push_str(
        "# HELP ehdb_fencing_active Whether the shared store is wrapped by the fencing decorator at all (0 = not wrapped, today's behaviour).\n",
    );
    out.push_str("# TYPE ehdb_fencing_active gauge\n");
    out.push_str(&format!(
        "ehdb_fencing_active {}\n",
        u8::from(setting.wraps())
    ));
    out
}

/// Resolve the segment rollover threshold from [`SEGMENT_MAX_BYTES_ENV`], falling
/// back to the engine default. A non-numeric / zero value uses the default.
fn segment_max_bytes(env: &EnvMap) -> u64 {
    env.get(SEGMENT_MAX_BYTES_ENV)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(ehdb_reference::DEFAULT_SEGMENT_MAX_BYTES)
}

/// Construct the full durable stack: [`SharedTierEventLog`] = shared-tier
/// (slice 3) over affinity single-writer routing (slice 2) over per-shard
/// [`DurableEventLogDriver`] segment stores (slice 1), pinned to this replica's
/// [`ownership_from_env`] and pointed at [`DurablePaths`], with the segment
/// rollover threshold from [`SEGMENT_MAX_BYTES_ENV`].
pub fn build_durable_stack(
    env: &EnvMap,
    contract: &EhdbContract,
) -> Result<SharedTierEventLog, String> {
    let paths = DurablePaths::resolve(env, contract);
    let ownership = ownership_from_env(env);
    // Observe (never enforce) the replica failure domains, once per process.
    let _ = REPLICA_DOMAINS
        .get_or_init(|| DomainObservation::of(&paths.local_root, &paths.shared_root));

    let plain = FilesystemSharedBackend::open(&paths.shared_root).map_err(|e| e.to_string())?;
    let setting = FencingSetting::from_env(env);
    let shared: Arc<dyn SharedSegmentBackend> = match setting {
        FencingSetting::Off => Arc::new(plain),
        FencingSetting::Shadow | FencingSetting::Enforce => {
            // The ledger lives beside the shared objects, not inside them: it is
            // metadata about who may write, not a segment.
            let ledger = ehdb_fencing::FencingLedger::new(paths.shared_root.join(".fencing"))
                .map_err(|e| e.to_string())?;
            let mode = if setting == FencingSetting::Enforce {
                ehdb_fencing::FencingMode::Enforce
            } else {
                ehdb_fencing::FencingMode::Shadow
            };
            let fenced = ehdb_fencing::FencedSharedBackend::new(plain, ledger)
                .with_mode(mode)
                .with_metrics(Arc::clone(&FENCING_METRICS));
            // ⚠ No election yet (noetl/ehdb#331), so this writer holds no token
            // and its epoch stays 0. Shadow therefore observes nothing —
            // deliberately: the point of wiring it now is that `writes_checked`
            // starts climbing, which is what makes a later zero on
            // `stale_observed` mean anything at all.
            Arc::new(fenced)
        }
    };
    SharedTierEventLog::open_with_segment_size(
        &paths.local_root,
        ownership,
        shared,
        &paths.coldload_root,
        segment_max_bytes(env),
    )
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
            // Serialize against the periodic segment-GC path (and any concurrent
            // append) on this shard — see `shard_lock`.
            let shard = ownership_from_env(env).shard_of(&request.execution_id);
            let lock = shard_lock(shard);
            let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
            let stack = build_durable_stack(env, contract)?;
            match stack.append(request).map_err(|e| e.to_string())? {
                Routed::Served(outcome) => Ok(AppendDispatch::Served(outcome)),
                Routed::NotOwner { owner_shard } => Ok(AppendDispatch::RoutedAway { owner_shard }),
            }
        }
    }
}

/// The shards this replica owns, ascending, from its
/// [`ownership_from_env`] (`0..shard_count` filtered by ownership).
pub fn owned_shards(env: &EnvMap) -> Vec<u32> {
    let ownership = ownership_from_env(env);
    (0..ownership.shard_count())
        .filter(|s| ownership.owns_shard(*s))
        .collect()
}

/// Run one segment-GC pass over every shard this replica owns — the periodic
/// reclaim the worker's GC task invokes (and the `ehdb-selfcheck` GC verb drives
/// once). For each owned shard it acquires the per-shard [`shard_lock`] (so it
/// never interleaves with a durable append on that shard), builds the durable
/// stack (per-op, stateless — matching the append path), and calls
/// [`SharedTierEventLog::reclaim_shard`], which reclaims local **and** shared
/// segments watermark-first. Returns one outcome per owned shard actually served
/// (a shard the replica doesn't own is skipped; a `RoutedAway` never happens
/// since we only iterate owned shards).
///
/// A per-shard error is collected as `Err` and does not abort the other shards —
/// GC is best-effort maintenance, never fatal.
pub fn reclaim_owned_shards(
    env: &EnvMap,
    contract: &EhdbContract,
    policy: &SegmentGcPolicy,
) -> Vec<Result<SharedShardGcOutcome, String>> {
    let mut out = Vec::new();
    for shard in owned_shards(env) {
        let lock = shard_lock(shard);
        let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        let result = build_durable_stack(env, contract).and_then(|stack| {
            match stack.reclaim_shard(shard, policy) {
                Ok(Routed::Served(outcome)) => Ok(outcome),
                // Never happens (we only iterate owned shards), but map it
                // defensively rather than panic.
                Ok(Routed::NotOwner { owner_shard }) => Err(format!(
                    "reclaim_shard refused: shard {shard} owned by {owner_shard}"
                )),
                Err(e) => Err(e.to_string()),
            }
        });
        out.push(result);
    }
    out
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
    // `mut`: the ehdb read methods take `&mut self` since ehdb#267 (a
    // checkpoint-trust open defers the offset-index rebuild to the first read;
    // a read-only cold-load loads it eagerly at open, so this read is O(1)).
    let mut store = DurableSegmentStore::open_read_only(&shard_dir).map_err(|e| e.to_string())?;
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

    /// The claim in this module's doc comments — "identical selection to
    /// `AffinityConfig::from_env`" — asserted **against that rule**, over a
    /// matrix, rather than against hardcoded numbers (noetl/ai-meta#266).
    ///
    /// The test above is named `ownership_matches_worker_affinity_env` but never
    /// referenced the affinity code at all: it checked two hand-written pairs. So
    /// the correspondence three doc comments depend on was prose with nothing
    /// forcing it true, and either side could have moved while both kept claiming
    /// to match. This is the assertion that makes the claim load-bearing.
    #[test]
    fn ownership_selection_is_the_same_rule_the_affinity_config_uses() {
        for (index, count) in [
            (0u32, 0u32),  // count 0 is normalised to 1 by both
            (0, 1),
            (0, 2),
            (1, 2),
            (2, 2),        // out of range -> single owner
            (3, 2),        // further out of range
            (0, 4),
            (3, 4),
            (4, 4),        // boundary: index == count is out of range
            (7, 8),
        ] {
            let (want_index, want_count) =
                crate::sharding::effective_shard_selection(index, count);
            let got = ownership_from_env(&env(&[
                ("NOETL_SHARD_INDEX", &index.to_string()),
                ("NOETL_SHARD_COUNT", &count.to_string()),
            ]));
            assert_eq!(
                (got.shard_index(), got.shard_count()),
                (want_index, want_count),
                "durable ownership diverged from the affinity selection rule at \
                 index={index} count={count}"
            );
        }

        // Positive control: the matrix must contain a case where the rule
        // actually rewrites the input, otherwise the loop above could pass with a
        // selection function that returned its arguments unchanged.
        assert_eq!(
            crate::sharding::effective_shard_selection(2, 2),
            (0, 1),
            "an out-of-range index must degrade — without this the matrix proves nothing"
        );
        assert_ne!(
            crate::sharding::effective_shard_selection(3, 4),
            (0, 1),
            "and an in-range index must NOT degrade"
        );
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
        let mut store = DurableSegmentStore::open_read_only(&shard0).unwrap();
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

#[cfg(test)]
mod fencing_tests {
    use super::*;
    use ehdb_reference::durable_eventlog_shared::SharedSegmentBackend;

    fn env_with(v: Option<&str>) -> EnvMap {
        let mut m = EnvMap::new();
        if let Some(v) = v {
            m.insert(FENCING_ENV.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn fencing_is_off_unless_asked_for() {
        // ⚠ The safety claim: an untouched deployment must not start wrapping
        // the shared store. Unset AND unrecognised both mean Off — the
        // fail-safe direction here is "change nothing", never "start refusing".
        assert_eq!(FencingSetting::from_env(&env_with(None)), FencingSetting::Off);
        assert_eq!(FencingSetting::default(), FencingSetting::Off);
        for junk in ["", "  ", "on", "true", "enfroce", "shadw", "1"] {
            assert_eq!(
                FencingSetting::from_env(&env_with(Some(junk))),
                FencingSetting::Off,
                "unrecognised value {junk:?} must not enable fencing"
            );
        }
    }

    #[test]
    fn shadow_and_enforce_are_recognised_and_case_insensitive() {
        for v in ["shadow", "SHADOW", " Shadow "] {
            assert_eq!(
                FencingSetting::from_env(&env_with(Some(v))),
                FencingSetting::Shadow
            );
        }
        assert_eq!(
            FencingSetting::from_env(&env_with(Some("enforce"))),
            FencingSetting::Enforce
        );
    }

    #[test]
    fn only_off_leaves_the_store_unwrapped() {
        assert!(!FencingSetting::Off.wraps());
        assert!(FencingSetting::Shadow.wraps());
        assert!(FencingSetting::Enforce.wraps());
    }

    #[test]
    fn the_counters_render_at_zero_and_say_whether_fencing_is_active() {
        // ⚠ A family that appeared only once fencing was enabled would make
        // "not wrapped" indistinguishable from "wrapped and quiet" — and telling
        // those apart is the entire purpose of the shadow period.
        let off = render_fencing(FencingSetting::Off);
        assert!(off.contains("ehdb_fencing_active 0\n"), "{off}");
        assert!(off.contains("# TYPE ehdb_fencing_stale_observed_total counter"), "{off}");
        assert!(off.contains("ehdb_fencing_enforcing 0\n"), "{off}");

        let shadow = render_fencing(FencingSetting::Shadow);
        assert!(shadow.contains("ehdb_fencing_active 1\n"), "{shadow}");
        assert!(
            shadow.contains("ehdb_fencing_enforcing 0\n"),
            "shadow must never report itself as enforcing: {shadow}"
        );

        let enforce = render_fencing(FencingSetting::Enforce);
        assert!(enforce.contains("ehdb_fencing_active 1\n"), "{enforce}");
        assert!(enforce.contains("ehdb_fencing_enforcing 1\n"), "{enforce}");
    }

    #[test]
    fn the_writes_checked_counter_is_what_makes_a_zero_meaningful() {
        // ⚠⚠ The gate reads two numbers. `stale_observed == 0` is only evidence
        // when `writes_checked` is climbing beside it; a zero with a flat
        // checked-counter means the decorator is unreached, which is the exact
        // failure this programme keeps finding. So prove the counter moves when
        // a fenced store is actually written through.
        let dir = std::env::temp_dir().join(format!(
            "noetl-fencing-reach-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let objects = dir.join("objects");
        let ledger_dir = dir.join(".fencing");
        std::fs::create_dir_all(&objects).unwrap();

        let plain =
            ehdb_reference::durable_eventlog_shared::FilesystemSharedBackend::open(&objects)
                .unwrap();
        let ledger = ehdb_fencing::FencingLedger::new(&ledger_dir).unwrap();
        let metrics = FencingMetrics::new();
        let fenced = ehdb_fencing::FencedSharedBackend::new(plain, ledger)
            .with_mode(ehdb_fencing::FencingMode::Shadow)
            .with_metrics(std::sync::Arc::clone(&metrics));

        let before = metrics.writes_checked.load(std::sync::atomic::Ordering::Relaxed);
        fenced.put_segment(0, 1, b"payload").unwrap();
        let after = metrics.writes_checked.load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(after, before + 1, "a write through the fenced store must be counted");
        assert_eq!(
            metrics.stale_observed.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "with no election issuing tokens every epoch is 0, so nothing is stale"
        );
        // And the bytes really landed — shadow refuses nothing.
        assert_eq!(
            fenced.get_segment(0, 1).unwrap().as_deref(),
            Some(&b"payload"[..])
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod domain_observation_tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "noetl-dom-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ))
    }

    #[test]
    fn the_prod_layout_is_reported_as_violating() {
        // ⚠⚠ This is prod's actual shape: NOETL_EHDB_TIER_SERVICE_DIR sits
        // INSIDE NOETL_EVENT_BUS_WRITER_DIR on one PVC. The observation must say
        // so — that is the whole point of computing it against the live paths
        // rather than reading manifests, and it is G4's verify-before.
        let base = tmp("nested");
        let local = base.join("local");
        let shared = base.join("local").join("ehdb-tier");
        std::fs::create_dir_all(&shared).unwrap();

        let obs = DomainObservation::of(&local, &shared);
        assert!(
            !obs.violations.is_empty(),
            "a shared root nested inside the local root must be reported: {obs:?}"
        );
        assert!(
            !obs.survives_node_loss,
            "two paths on one node never survive losing it"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn it_observes_and_never_refuses() {
        // The safety property. `DomainObservation::of` returns findings; it has
        // no error path at all, so it cannot turn a misconfigured layout into a
        // startup failure. Prod violates the check today, so an enforcing
        // version here would be an outage by construction.
        let base = tmp("nofail");
        let local = base.join("a");
        let shared = base.join("a").join("inside");
        std::fs::create_dir_all(&shared).unwrap();
        let obs = DomainObservation::of(&local, &shared); // returns, does not Err
        assert!(!obs.violations.is_empty());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn the_gauges_are_pinned_so_clean_is_not_absent() {
        // Before the stack is opened nothing has been computed, and the render
        // must say that rather than reporting a reassuring zero.
        let text = render_replica_domains();
        assert!(text.contains("# TYPE ehdb_replica_domain_violations gauge"), "{text}");
        for kind in ["shared_domain", "nested_path", "undeclared"] {
            assert!(
                text.contains(&format!("ehdb_replica_domain_violations{{kind=\"{kind}\"}}")),
                "label {kind} must be pinned, not absent: {text}"
            );
        }
        assert!(text.contains("ehdb_replica_domains_observed"), "{text}");
        assert!(text.contains("ehdb_replica_survives_node_loss"), "{text}");
    }

    #[test]
    fn separate_roots_on_one_node_are_clean_but_still_die_with_it() {
        // ⚠ The positive control AND the distinction that matters: passing the
        // domain check is not the same as surviving node loss. A validator that
        // reported violations for everything would fail this.
        let base = tmp("separate");
        let a = base.join("a");
        let b = base.join("b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let obs = DomainObservation::of(&a, &b);
        // Same device in a test env, so a shared-domain finding is expected and
        // correct; what must NOT appear is a nesting finding.
        assert!(
            !obs.violations.iter().any(|v| matches!(
                v,
                ehdb_l0::DomainViolation::NestedPath { .. }
            )),
            "sibling dirs are not nested: {obs:?}"
        );
        assert!(!obs.survives_node_loss);
        let _ = std::fs::remove_dir_all(&base);
    }
}

#[cfg(test)]
mod seal_age_env_tests {
    use super::*;

    /// ⚠ These mutate the process env, so they are serialised behind one lock.
    /// `cargo test` does NOT serialise tests — a SAFETY note claiming it does
    /// was wrong once before on this platform.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(SEAL_MAX_AGE_ENV).ok();
        match value {
            Some(v) => std::env::set_var(SEAL_MAX_AGE_ENV, v),
            None => std::env::remove_var(SEAL_MAX_AGE_ENV),
        }
        let out = f();
        match prev {
            Some(v) => std::env::set_var(SEAL_MAX_AGE_ENV, v),
            None => std::env::remove_var(SEAL_MAX_AGE_ENV),
        }
        out
    }

    #[test]
    fn unset_is_none_which_is_todays_behaviour() {
        assert_eq!(with_env(None, seal_max_age_from_env), None);
    }

    #[test]
    fn a_value_is_read_so_setting_it_is_no_longer_a_silent_no_op() {
        // ⚠⚠ The regression this closes: the knob existed on L0Config and was
        // never read from the env, so setting NOETL_EHDB_SEAL_MAX_AGE_MS on prod
        // would have done NOTHING — the gate would have looked taken and changed
        // nothing at all.
        assert_eq!(
            with_env(Some("5000"), seal_max_age_from_env),
            Some(std::time::Duration::from_millis(5000))
        );
    }

    #[test]
    fn junk_and_zero_fail_safe_to_off() {
        // Fail-safe direction is "today's behaviour", never an accidental seal
        // storm from a typo.
        for junk in ["", "  ", "abc", "-1", "5s", "0"] {
            assert_eq!(
                with_env(Some(junk), seal_max_age_from_env),
                None,
                "{junk:?} must not enable the trigger"
            );
        }
    }
}

#[cfg(test)]
mod election_visibility_tests {
    use super::*;

    #[test]
    fn the_election_reports_itself_as_not_running() {
        // ⚠⚠ This test is a placeholder that must FAIL when the election is
        // actually wired — that is deliberate. If someone implements the K8s
        // LeaseStore adapter and forgets to publish real state here, the scrape
        // would keep asserting `0` while tokens were being issued, which is
        // worse than no gauge: it would say fencing enforce is unsafe when it
        // had become safe.
        let text = render_election();
        assert!(text.contains("ehdb_election_active 0\n"), "{text}");
        assert!(text.contains("ehdb_election_epoch 0\n"), "{text}");
    }

    #[test]
    fn the_election_is_still_unwired_in_this_build() {
        // The guard that pairs with the gauge: if `ShardElection` ever gains a
        // call site in this crate, `render_election` must stop hard-coding 0.
        // Counts CODE, not prose.
        let sources = [
            include_str!("eventlog_backend.rs"),
            include_str!("../command_bus.rs"),
            include_str!("../event_bus.rs"),
        ];
        // ⚠⚠ Scan only the NON-TEST portion of each file. `include_str!` yields
        // the whole file including this module, so the first two attempts both
        // matched their own source: once on the bare name, and again on the very
        // needle literals written to avoid that. A guard that counts itself is
        // the same family as a counter that counts its own doc comment — and it
        // took two turns to stop being self-referential.
        let needle = concat!("Shard", "Election");
        let wired = sources.iter().any(|src| {
            src.split("#[cfg(test)]")
                .next()
                .unwrap_or("")
                .lines()
                .map(str::trim_start)
                .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
                .any(|l| l.contains(needle))
        });
        assert!(
            !wired,
            "ShardElection now has a call site — render_election() must publish \
             real state instead of a hard-coded 0, and noetl/ehdb#331's gate \
             needs revisiting"
        );
    }
}


#[cfg(test)]
mod seal_sweep_tests {
    use super::*;

    /// Mutates the process env, so serialised. `cargo test` does NOT serialise.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_age<T>(ms: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(SEAL_MAX_AGE_ENV).ok();
        match ms {
            Some(v) => std::env::set_var(SEAL_MAX_AGE_ENV, v),
            None => std::env::remove_var(SEAL_MAX_AGE_ENV),
        }
        let out = f();
        match prev {
            Some(v) => std::env::set_var(SEAL_MAX_AGE_ENV, v),
            None => std::env::remove_var(SEAL_MAX_AGE_ENV),
        }
        out
    }

    fn engine(dir: &std::path::Path) -> std::sync::Arc<std::sync::Mutex<ehdb_l0::L0Engine<ehdb_l0::D1EventLog>>> {
        let store: std::sync::Arc<dyn ehdb_l0::substrate::DurableSubstrate> =
            std::sync::Arc::new(ehdb_l0::LocalFsSubstrate::new(dir).unwrap());
        let cfg = ehdb_l0::L0Config::d1(dir)
            .with_shard_count(1)
            .with_seal_max_age(seal_max_age_from_env());
        std::sync::Arc::new(std::sync::Mutex::new(
            ehdb_l0::L0Engine::<ehdb_l0::D1EventLog>::open(cfg, store).unwrap(),
        ))
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "noetl-sweep-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ))
    }

    #[tokio::test]
    async fn nothing_is_spawned_when_the_trigger_is_unset() {
        // The safety property: "switched off" must cost nothing, not even a
        // periodic lock on the engine the writer is appending through.
        let d = tmp("off");
        let e = with_age(None, || engine(&d));
        let h = with_age(None, || spawn_seal_age_sweep(e, "test", 0));
        assert!(h.is_none(), "no sweep task may exist while the flag is unset");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn a_sweep_is_spawned_when_the_trigger_is_set() {
        let d = tmp("on");
        let e = with_age(Some("5000"), || engine(&d));
        let h = with_age(Some("5000"), || spawn_seal_age_sweep(e, "test", 0));
        assert!(h.is_some(), "the sweep must exist once the flag is set");
        h.unwrap().abort();
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn the_sweep_seals_an_idle_shard_which_is_the_whole_point() {
        // The end-to-end property. `should_seal()` runs only on append, so
        // without this task an idle shard never seals and its records never
        // reach the substrate. One append, then silence, then the sweep must
        // seal it with no further traffic.
        let d = tmp("idle");
        let e = with_age(Some("60"), || engine(&d));
        {
            let mut g = e.lock().unwrap();
            g.append_record(ehdb_l0::EventRecord::new(1, "exec-idle", "t1", "p1"))
                .unwrap();
            assert_eq!(g.metrics().snapshot().seals, 0, "nothing seals on one append");
        }
        let h = with_age(Some("60"), || {
            std::env::set_var(SEAL_SWEEP_INTERVAL_MS_ENV, "20");
            let h = spawn_seal_age_sweep(e.clone(), "test", 0);
            std::env::remove_var(SEAL_SWEEP_INTERVAL_MS_ENV);
            h
        })
        .expect("sweep spawned");

        // No further appends. Only the sweep can seal this.
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if e.lock().unwrap().metrics().snapshot().seals > 0 {
                break;
            }
        }
        h.abort();
        assert!(
            e.lock().unwrap().metrics().snapshot().seals > 0,
            "an IDLE shard must be sealed by the sweep -- this is the behaviour \
             the flag alone cannot produce"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn an_idle_shard_is_not_sealed_without_the_sweep() {
        // The negative control for the test above. Same fixture, no sweep task:
        // nothing seals, which is exactly today's production behaviour.
        let d = tmp("idle-control");
        let e = with_age(Some("60"), || engine(&d));
        {
            let mut g = e.lock().unwrap();
            g.append_record(ehdb_l0::EventRecord::new(1, "exec-idle", "t1", "p1"))
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            e.lock().unwrap().metrics().snapshot().seals,
            0,
            "without the sweep an idle shard never seals -- if this fails, the \
             sweep is not what makes the difference"
        );
        let _ = std::fs::remove_dir_all(&d);
    }
}
