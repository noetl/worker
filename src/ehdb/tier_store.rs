//! Durable store behind the tier service — the first slice where tier data
//! actually moves.
//!
//! **PR 3 of [ai-meta#257](https://github.com/noetl/ai-meta/issues/257).**
//! PR 1 gave the writer a listener; PR 2 gave callers a client. Both were inert
//! by construction because nothing was stored. This module backs the `append`
//! and `read` operations with a real `ehdb_reference` driver over a directory
//! the writer owns, so a remote caller's append is durable and a remote read
//! returns it.
//!
//! # Scope
//!
//! `append` / `read_execution` / `scan`, over the tiers in
//! [`StoreTier`][super::store_tier::StoreTier] — **event-log and projection**
//! ([ai-meta#265](https://github.com/noetl/ai-meta/issues/265) A1). KV, object
//! and vector still have no store here; they gain one in the same change set
//! that gives them a `StoreTier` variant, not before.
//!
//! Each tier is a **separate store file** under the same directory, with its
//! own lock. That is the whole of the genericisation and it is deliberately
//! that small: one engine, one serialised-append critical section, N files. A
//! shared file with a tier discriminator inside each record would put the
//! projection tier's write volume — roughly one append per orchestrator trigger
//! — behind the event log's lock, on the process that is already serving the
//! event-log tier primary in production.
//!
//! The RFC puts `ack` here too; it is held back because `ack` drives segment GC
//! — deleting data on a remote caller's say-so deserves its own PR and its own
//! gate, and bundling it here would put "returns the right bytes" and "removes
//! bytes" in one review.
//!
//! # Store location
//!
//! `NOETL_EHDB_TIER_SERVICE_DIR` names the directory. Unset ⇒ the store is not
//! constructed and every data op answers `unavailable` rather than falling back
//! to a guessed path. A service that silently writes tier data somewhere nobody
//! configured is worse than one that refuses.
//!
//! In production this points **inside the writer's PVC**, which is the whole
//! reason the writer hosts this face: it is the process that owns durable
//! volumes, and prod's volumes are `ReadWriteOnce` so no other pod can mount
//! them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use ehdb_reference::{
    EventLogAppendRequest, EventLogDriver, EventLogReadExecutionRequest, EventLogScanRequest,
    LocalReferenceEventLogDriver, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};
use tokio::sync::RwLock;

use super::store_tier::StoreTier;

/// Directory the tier service stores into. Unset ⇒ no store.
pub const TIER_SERVICE_DIR_ENV: &str = "NOETL_EHDB_TIER_SERVICE_DIR";

/// Largest `limit` a caller may request from a scan.
///
/// A remote caller controls this number, and an unbounded scan on a
/// single-replica writer is a denial-of-service with extra steps. Requests above
/// the cap are **clamped, not rejected** — a caller asking for more than we will
/// give should still get the most we will give, and the response carries the
/// count so the truncation is visible rather than silent.
pub const MAX_SCAN_LIMIT: usize = 1_000;

/// The record limit for a per-execution read: none.
///
/// ⚠ noetl/ai-meta#492. This used to be [`MAX_SCAN_LIMIT`], and the driver
/// applies it with `take`, so an execution with more than 1000 records in one
/// segment came back cut at 1000 — and the merged (sealed) path then reported
/// `record_count` as the RETURNED count, so the cut was invisible. Every reader
/// of this op wants the whole execution: the comparator would call the tail
/// `missing_event`, recovery would fold a prefix. The scan cap exists because a
/// global scan is unbounded by construction; one execution is not. A reply
/// that outgrows the frame cap is refused loudly by the service
/// (`reply_too_large`), which is the right failure for an oversized answer.
pub const READ_EXECUTION_LIMIT: usize = usize::MAX;

/// Resolved store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierStoreConfig {
    pub dir: PathBuf,
}

impl TierStoreConfig {
    /// Resolve from the environment. `None` ⇒ no store is configured.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(TIER_SERVICE_DIR_ENV).ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        Some(Self {
            dir: PathBuf::from(raw),
        })
    }

    /// Path of the JSONL this store appends `tier`'s records to.
    ///
    /// The filename comes from [`StoreTier::file_name`], never from the caller,
    /// so a wire value cannot traverse out of the writer's directory.
    pub fn path_for(&self, tier: StoreTier) -> PathBuf {
        self.dir.join(tier.file_name())
    }

    /// Back-compat alias for the event-log store path.
    pub fn eventlog_path(&self) -> PathBuf {
        self.path_for(StoreTier::EventLog)
    }
}

/// Outcome of a data operation, kept distinct so a caller can tell "there is no
/// store" from "the store has nothing for you". Collapsing those into one empty
/// answer is exactly the absent-vs-broken ambiguity this platform keeps paying
/// for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierStoreOutcome {
    /// The operation succeeded; payload is the JSON body.
    Ok(String),
    /// No store is configured on this writer.
    Unavailable,
    /// The request was malformed.
    Invalid(String),
    /// The store errored.
    Error(String),
}

/// Recently-appended `event_id`s, per tier — the seal-straddling dedup window
/// (noetl/ai-meta#472).
///
/// ⚠⚠ **Why a ring and not the driver's dedup.** The driver already rejects a
/// duplicate `event_id`, but it does so by replaying the log it has open, and
/// that is the **active** segment only: sealing is `std::fs::rename(active,
/// sealed)`, so every seal moves the previous records out of the driver's view
/// and the dedup window closes. Reads enumerate sealed segments; the append path
/// does not. A redelivery of an `event_id` that landed before a seal therefore
/// found nothing and appended a second copy — the mechanism behind the
/// `duplicate_event` divergence (+2 over 30 h, with the id SETS staying equal,
/// which is exactly what a second copy of a present id looks like).
///
/// ⚠ Deliberately NOT the alternatives, each rejected on a measured cost:
///
/// * **Search sealed segments on append.** Prod holds segments of 1.08 GB and
///   3.3 GB; a streaming search is seconds, on the write path of a
///   `primary`-serving tier. The file's own note at `event_id_from_payload` says
///   that path was kept free of per-record work on purpose.
/// * **An `event_id` index per sealed segment.** The existing `.idx` is a set of
///   `execution_id`s — a read-pruning filter — so this would be a new structure
///   with one entry per record, ~1M entries resident on a writer with documented
///   OOMs (noetl/ai-meta#332).
///
/// ⚠⚠ **What this does and does not guarantee.** It covers the window a retry
/// actually spans: `MAX_RETRIES=7` with 500 ms doubling plus per-attempt
/// timeouts is ~80 s, and the ring holds the last `NOETL_EHDB_TIER_DEDUP_WINDOW`
/// ids (default 10,000). A redelivery older than that window still gets through,
/// so this is **not** a total guarantee — the read-side dedup in
/// `ehdb_projection_fold::events_from_tier` remains the backstop. It is a
/// bounded fix for the observed mechanism, not a proof of uniqueness.
static RECENT_EVENT_IDS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<StoreTier, DedupRing>>,
> = std::sync::OnceLock::new();

/// Bounded FIFO set: O(1) contains, O(1) insert, fixed memory.
#[derive(Default)]
pub(crate) struct DedupRing {
    seen: std::collections::HashSet<String>,
    order: std::collections::VecDeque<String>,
    cap: usize,
}

impl DedupRing {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            order: std::collections::VecDeque::new(),
            cap,
        }
    }

    /// ⚠ `#[cfg(test)]`: the production path never asks "is this present" on its
    /// own — `insert` answers both questions in one lock. This exists so the
    /// ring's eviction order is assertable, which is what the mutation battery
    /// needs. Gated rather than `allow(dead_code)` so it cannot quietly become a
    /// second, drifting way to read the ring.
    #[cfg(test)]
    pub(crate) fn contains(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    /// Insert `id`, evicting the oldest when at capacity. Returns false when the
    /// id was already present (i.e. this is a duplicate).
    pub(crate) fn insert(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        // ⚠⚠ A capacity of 0 disables the ring entirely — and this guard is
        // LOAD-BEARING, not tidiness.
        //
        // Without it the eviction loop below is `while self.order.len() >= 0`,
        // which is **always true for a usize**, while `pop_front()` on an empty
        // deque returns `None` and never decrements. That is an INFINITE LOOP, on
        // the append path, the moment someone sets the rollback value.
        //
        // Found by mutation: deleting this block did not fail the suite, it HUNG
        // it — which on a Rust repo reads as a slow compile rather than a defect.
        if self.cap == 0 {
            return true;
        }
        while self.order.len() >= self.cap {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        self.order.push_back(id.to_string());
        self.seen.insert(id.to_string());
        true
    }

    /// ⚠ `#[cfg(test)]`, same reason as [`contains`]: the bound is asserted by
    /// tests, not consulted at runtime.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.order.len()
    }
}

/// Size of the seal-straddling dedup window, in `event_id`s per tier.
///
/// `0` disables it, restoring the pre-fix behaviour exactly — the no-redeploy
/// rollback.
pub const TIER_DEDUP_WINDOW_ENV: &str = "NOETL_EHDB_TIER_DEDUP_WINDOW";

fn dedup_window() -> usize {
    std::env::var(TIER_DEDUP_WINDOW_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(10_000)
}

/// Claim `event_id` for `tier`. `false` means it was appended recently, so the
/// caller must NOT append again.
fn claim_event_id(tier: StoreTier, event_id: &str) -> bool {
    let map = RECENT_EVENT_IDS.get_or_init(|| std::sync::Mutex::new(Default::default()));
    let mut guard = match map.lock() {
        Ok(g) => g,
        // A poisoned lock must not turn into a dropped append: fail OPEN, which
        // degrades to the previous behaviour rather than losing a record.
        Err(e) => e.into_inner(),
    };
    guard
        .entry(tier)
        .or_insert_with(|| DedupRing::with_capacity(dedup_window()))
        .insert(event_id)
}

/// Per-store lock serialising access to one event-log file.
///
/// **This is the fix for the P0 the serve-ready soak found.** Until the mirror
/// source moved to the server, every worker pod appended to *its own* store
/// through *one* caller, so two appends never overlapped and the store's
/// single-writer assumption held by accident. Pointing every execution's mirror
/// at one writer-fronted store through one relay made concurrent appends the
/// normal case, and the store is not built for them:
///
/// * `LocalJsonlTransactionLog::append_record_to_disk` calls
///   `serde_json::to_writer` straight at the `File` — **unbuffered**, so one
///   record becomes hundreds of small `write(2)` calls. `O_APPEND` makes each
///   of those atomic *individually*, which is precisely the problem: two
///   appenders interleave at write-call granularity and the second record lands
///   **inside** the first one's `payload` byte array. Read-back then hits `{`
///   where a `u8` belongs — `invalid transaction log record at line N: invalid
///   type: map, expected u8`.
/// * the sequence is a read-modify-write. `LocalReferenceEventLogDriver::append`
///   replays the whole log to compute `next = count + 1`, then writes. Two
///   appenders that replay the same state both claim the same
///   `global_sequence`.
///
/// One line of corruption is not one lost record. The replay in
/// `LocalJsonlTransactionLog::open` runs on **every** operation and fails on the
/// first bad line, so a single torn write makes every subsequent append *and*
/// every read fail — which is how the soak got `append` ok 46 / error 302 and
/// `ehdb_unavailable` on all reads.
///
/// The lock closes both holes at once because the replay, the sequence
/// decision and the write all sit inside one critical section.
///
/// **Reads take it too**, shared. A read concurrent with an append would
/// otherwise replay a file whose last line is half-written and report the store
/// broken — the same error, from a store that is fine.
///
/// It is `tokio::sync::RwLock`, not `std::sync`, on purpose: this process also
/// hosts both buses, and N blocked appenders must yield their runtime threads
/// rather than park them. Only the lock holder occupies a thread.
///
/// Keyed by store path so two distinct stores never block each other — which is
/// also what keeps the tests below independent.
///
/// Since #265 the key is the **per-tier** path, so the projection tier's appends
/// and the event log's do not serialise against each other. They are different
/// files; sharing a lock would be pure contention with no invariant behind it.
fn store_lock(cfg: &TierStoreConfig, tier: StoreTier) -> Arc<RwLock<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    // Poison-tolerant: the registry guards a map, not an invariant, and a
    // panic while holding it must not take down the writer.
    let mut guard = registry.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .entry(cfg.path_for(tier))
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

/// One engine for every tier.
///
/// `LocalReferenceEventLogDriver` is an append-only sequenced record store; the
/// "event log" in its name is where it was first used, not a constraint on what
/// it can hold. Reusing it for the projection tier means the concurrency fix
/// that #257's soak paid for — the serialised replay → sequence → write window
/// — protects the new tier from its first append rather than being reproduced
/// for it.
/// The producer's `event_id`, lifted out of a mirrored payload.
///
/// The payload is the server's `mirror_payload` JSON, whose first field is
/// `event_id` — a snowflake, minted once and immutable, and the only value in
/// the body that identifies the event across redeliveries (noetl/ai-meta#313).
///
/// ⚠ Extracted HERE rather than inside the driver. The driver's append is the
/// write path of a `primary`-serving tier; a parse per record there would be on
/// that path, and it would silently degrade to "never matches" — a dedupe that
/// stops working while still reporting success — the day the payload shape moved.
/// Doing it at the boundary means a shape change shows up as `event_id: None`,
/// i.e. dedupe simply off, which is the safe direction.
///
/// Accepts both the numeric and string spellings: the server emits `event_id` as
/// a JSON number, but a re-serialised payload can carry it quoted.
fn event_id_from_payload(payload: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;
    match v.get("event_id")? {
        serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Seal the tier's active log once it exceeds this many bytes.
///
/// ⚠⚠ WHY THIS EXISTS — measured, not assumed.
///
/// The tier is **not** an L0 engine, so it has none of the part-sealing the
/// command bus and events feed get from `seal_aged_parts`. It is one JSONL file
/// that `LocalReferenceRuntime::open` replays **in full** into an in-memory
/// `ReferenceDatabase`, which the reference-runtime cache then holds for the
/// life of the process. Two consequences, both linear in the record count:
///
/// * **Memory** is proportional to the store. In production the store reached
///   **4.0 GB** and the writer's baseline sat at ~3 GiB.
/// * **Every append costs a copy of that state** — `LocalReferenceRuntime::append`
///   does `self.state.clone()` before applying. Measured on a synthetic store:
///
///   ```text
///   records   ms/append   on_disk
///       400       8.12      0.7MB
///      2000      20.87      3.6MB
///      4000      36.56      7.2MB
///   ```
///
///   A 4.5x rise in per-append cost over a 10x store growth — O(n) per append.
///   At production size that is the observed `"timed out after 4s"`.
///
/// On 2026-09-20 this OOM-killed `noetl-cmdbus-writer-0`, which hosts BOTH
/// buses, repeatedly — at a 4 GiB limit and again at 8 GiB. Raising the limit
/// is a delay; bounding the store is the fix.
///
/// **Unset ⇒ no sealing ⇒ byte-identical to today.** A store that has never
/// sealed has exactly one segment, which is the file that exists now.
pub const TIER_SEAL_MAX_BYTES_ENV: &str = "NOETL_EHDB_TIER_SEAL_MAX_BYTES";

/// Resolve the seal threshold. `None` (unset/unparsable/zero) ⇒ never seal.
///
/// Fail-safe direction is **off**: a typo must not start rotating a
/// primary-serving tier's store behind the operator's back.
pub fn tier_seal_max_bytes() -> Option<u64> {
    parse_seal_max_bytes(std::env::var(TIER_SEAL_MAX_BYTES_ENV).ok().as_deref())
}

/// Pure parse, so the fail-safe direction is testable without the process env.
pub fn parse_seal_max_bytes(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
}

/// `NOETL_EHDB_TIER_SEAL_MAX_AGE_HOURS` — seal the ACTIVE segment once its
/// oldest record is older than this, even below [`TIER_SEAL_MAX_BYTES_ENV`]
/// (noetl/ai-meta#491 follow-up).
///
/// ⚠⚠ Why this exists. The retention planner keeps a sealed segment while any
/// of its executions also appears in the ACTIVE segment (rule 4), and always
/// keeps the newest sealed segment (rule 7). A tier that writes slowly never
/// reaches the size seal, so its active segment grows old and, with it, pins
/// every older segment forever. Production, 2026-10-27: the projection active
/// segment had been open for 34 days (146 MB of a 256 MiB cap), so
/// `projection.jsonl.1` (3.3 GB) stayed "pinned" and `.2` (268 MB) stayed
/// "newest sealed" — 3.57 GB the enabled pass could never reclaim.
///
/// Values:
/// * **unset** ⇒ [`TIER_SEAL_MAX_AGE_HOURS_DEFAULT`] (48 h), but ONLY when the
///   size seal is armed. A store that never opted into sealing stays
///   byte-identical: no env ⇒ no rotation, as before.
/// * `0` / `off` / `false` / `none` ⇒ no age seal.
/// * a positive number of hours (fractional allowed, e.g. `0.05` = 3 min for a
///   test cluster) ⇒ that age, floored at [`TIER_SEAL_MAX_AGE_FLOOR`]. An
///   explicit value arms the age seal even without the size seal.
/// * anything else ⇒ **off** (the size seal's fail-safe direction: a typo must
///   not start rotating a store).
///
/// Only [`PRUNABLE_TIERS`] age-seal. The age seal exists to make data
/// prunable; a tier that is never pruned (catalog, kv, object, ...) would only
/// be fragmented into more segments for every read to merge.
pub const TIER_SEAL_MAX_AGE_HOURS_ENV: &str = "NOETL_EHDB_TIER_SEAL_MAX_AGE_HOURS";

/// 48 h: just over the observed event-log size-seal cadence in production
/// (every 1-2 days; the longest gap on 2026-10-24..25 was 41 h), so the event
/// log keeps sealing by size as before and the age rule is only its backstop
/// on a quiet stretch. For projection it bounds a finished execution's time to
/// prunable at age + retention window (48 + 49 h) instead of "never".
pub const TIER_SEAL_MAX_AGE_HOURS_DEFAULT: f64 = 48.0;

/// The shortest age seal accepted. A seal renames the active file and starts a
/// background index build, so an absurdly small value must not turn every
/// append into a seal. One minute keeps a test cluster's "few minutes" usable.
pub const TIER_SEAL_MAX_AGE_FLOOR: std::time::Duration = std::time::Duration::from_secs(60);

/// When the active segment is sealed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SealPolicy {
    /// Size cap ([`TIER_SEAL_MAX_BYTES_ENV`]). `None` ⇒ no size seal.
    pub max_bytes: Option<u64>,
    /// Age cap ([`TIER_SEAL_MAX_AGE_HOURS_ENV`]). `None` ⇒ no age seal.
    pub max_age: Option<std::time::Duration>,
}

/// A bare size threshold is a size-only policy — what every caller that
/// predates the age seal (and every size-seal test) means.
impl From<Option<u64>> for SealPolicy {
    fn from(max_bytes: Option<u64>) -> Self {
        SealPolicy {
            max_bytes,
            max_age: None,
        }
    }
}

impl SealPolicy {
    /// The policy the process env asks for.
    pub fn from_env() -> Self {
        let max_bytes = tier_seal_max_bytes();
        SealPolicy {
            max_bytes,
            max_age: parse_seal_max_age(
                std::env::var(TIER_SEAL_MAX_AGE_HOURS_ENV).ok().as_deref(),
                max_bytes.is_some(),
            ),
        }
    }
}

/// Pure parse of [`TIER_SEAL_MAX_AGE_HOURS_ENV`]; see it for the rules.
/// `size_seal_armed` decides what UNSET means.
pub fn parse_seal_max_age(raw: Option<&str>, size_seal_armed: bool) -> Option<std::time::Duration> {
    let hours = match raw.map(str::trim) {
        None | Some("") => {
            return size_seal_armed.then(|| {
                std::time::Duration::from_secs_f64(TIER_SEAL_MAX_AGE_HOURS_DEFAULT * 3600.0)
            })
        }
        Some(v) => v,
    };
    if matches!(
        hours.to_ascii_lowercase().as_str(),
        "0" | "off" | "false" | "none"
    ) {
        return None;
    }
    let h = hours
        .parse::<f64>()
        .ok()
        .filter(|h| h.is_finite() && *h > 0.0)?;
    // ⚠ `try_from_secs_f64`, not `from_secs_f64`: the latter PANICS on a value
    // too large for a Duration, and this runs on every append.
    let age = std::time::Duration::try_from_secs_f64(h * 3600.0).ok()?;
    Some(age.max(TIER_SEAL_MAX_AGE_FLOOR))
}

/// When the ACTIVE segment of `tier` began, from durable state only.
///
/// ⚠ Not the active file's mtime: an active file is written continuously, so
/// its mtime is "now" and says nothing about how old its contents are. Not an
/// in-memory "opened at" either: it would reset on every writer restart, and a
/// writer that restarts more often than the age would never seal.
///
/// 1. **The first record's transaction id.** Every tier-store append mints
///    `ehdbel-<snowflake>` (`eventlog::new_transaction_id`), and a snowflake
///    carries its wall-clock ms. The first line of the file is the oldest
///    record in it, and it is on disk, so this survives restarts. Verified on
///    production 2026-10-27: `projection.jsonl` line 1 decodes to
///    2026-09-23 23:19:46 UTC, one second after `projection.jsonl.2`'s mtime.
/// 2. Otherwise **the newest sealed segment's mtime** — its last write, i.e. the
///    seal that created this active file (a seal is a rename, which keeps the
///    mtime, and the planner's rule 7 never unlinks the newest segment). It errs
///    old if the store sat idle before that seal, which only seals sooner.
/// 3. Otherwise unknown ⇒ `None` ⇒ no age seal.
///
/// Reads at most 4 KiB: the id sits in the first ~60 bytes of a line, but a
/// line can be megabytes (payloads are numeric byte arrays).
fn active_segment_started(cfg: &TierStoreConfig, tier: StoreTier) -> Option<std::time::SystemTime> {
    use std::io::Read;
    let mut head = Vec::with_capacity(4096);
    if let Ok(f) = std::fs::File::open(cfg.path_for(tier)) {
        let _ = f.take(4096).read_to_end(&mut head);
    }
    if let Some(ms) = first_record_ms(&head) {
        return Some(std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms));
    }
    sealed_segments(cfg, tier)
        .last()
        .and_then(|p| std::fs::metadata(p).ok()?.modified().ok())
}

/// The wall-clock ms in the FIRST line's `ehdbel-<snowflake>` transaction id.
fn first_record_ms(head: &[u8]) -> Option<u64> {
    const KEY: &[u8] = b"\"transaction_id\":\"ehdbel-";
    let first_line = head.split(|b| *b == b'\n').next()?;
    let at = first_line.windows(KEY.len()).position(|w| w == KEY)? + KEY.len();
    let digits: Vec<u8> = first_line[at..]
        .iter()
        .copied()
        .take_while(u8::is_ascii_digit)
        .collect();
    let id = std::str::from_utf8(&digits).ok()?.parse::<i64>().ok()?;
    let ms = crate::snowflake::timestamp_ms(id);
    (ms > 0).then_some(ms as u64)
}

/// Why a seal happened. A metrics label and a log field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    Size,
    Age,
}

impl SealReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SealReason::Size => "size",
            SealReason::Age => "age",
        }
    }
}

/// Should the active segment of `tier` (`len` bytes) be sealed now?
///
/// ⚠ Never for an empty or missing active segment: sealing it would produce an
/// empty segment and, worse, a seal per append. After any seal the active file
/// is absent until the append that follows writes a record minted NOW, so its
/// age restarts at ~0 and an age seal cannot repeat until `max_age` later.
fn seal_reason(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    len: u64,
    policy: SealPolicy,
    now: std::time::SystemTime,
) -> Option<SealReason> {
    if len == 0 {
        return None;
    }
    if policy.max_bytes.is_some_and(|max| len >= max) {
        return Some(SealReason::Size);
    }
    let max_age = policy.max_age?;
    if !PRUNABLE_TIERS.contains(&tier) {
        return None;
    }
    // A start in the future (clock step, foreign epoch) is "not old": the fail
    // direction is to not seal.
    let age = now
        .duration_since(active_segment_started(cfg, tier)?)
        .ok()?;
    (age >= max_age).then_some(SealReason::Age)
}

/// Sealed segments of `tier`, oldest first.
///
/// Named `<active>.<seq>` beside the active file — same directory, **same JSONL
/// format**, so nothing about the on-disk encoding changes. Sealing renames;
/// it never rewrites and never deletes.
pub fn sealed_segments(cfg: &TierStoreConfig, tier: StoreTier) -> Vec<PathBuf> {
    let active = cfg.path_for(tier);
    let Some(dir) = active.parent() else {
        return Vec::new();
    };
    let Some(stem) = active.file_name().and_then(|s| s.to_str()) else {
        return Vec::new();
    };
    let prefix = format!("{stem}.");
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    for e in rd.flatten() {
        let p = e.path();
        let Some(name) = p.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = name.strip_prefix(&prefix) {
            // ⚠ `parse::<u64>` IS the filter: it rejects "", "bak", "1a" and
            // "-1" on its own. An earlier version also checked every byte was a
            // digit; the mutation battery showed that guard was redundant
            // (removing it changed no behaviour), so it is gone rather than left
            // as a second rule that could drift from the first. A stray
            // `eventlog.jsonl.bak` is still never replayed as tier data.
            if let Ok(n) = rest.parse::<u64>() {
                found.push((n, p));
            }
        }
    }
    found.sort_by_key(|(n, _)| *n);
    found.into_iter().map(|(_, p)| p).collect()
}

/// Seal the active log if it is over the size threshold or older than the age
/// threshold ([`SealPolicy`]). Returns the sealed path.
///
/// ⚠ Called with the store's WRITE lock held, before an append, so no reader or
/// writer can be mid-operation on the file being renamed. Both reasons take the
/// SAME path below: same rename, same runtime drop, same `.idx`/`.offidx`
/// build — an age-sealed segment is indistinguishable from a size-sealed one.
///
/// ⚠ The rename is the whole operation. There is no copy and no truncate, so
/// there is no window in which data exists in neither file and no way for this
/// to lose a record: either the rename happened or it did not.
fn maybe_seal(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    policy: impl Into<SealPolicy>,
) -> Option<PathBuf> {
    maybe_seal_at(cfg, tier, policy.into(), std::time::SystemTime::now())
}

/// [`maybe_seal`] with the clock injected, so a test can age a segment without
/// sleeping or forging records.
fn maybe_seal_at(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    policy: SealPolicy,
    now: std::time::SystemTime,
) -> Option<PathBuf> {
    if policy.max_bytes.is_none() && policy.max_age.is_none() {
        return None;
    }
    let active = cfg.path_for(tier);
    let len = std::fs::metadata(&active).ok()?.len();
    let reason = seal_reason(cfg, tier, len, policy, now)?;
    let next = sealed_segments(cfg, tier)
        .last()
        .and_then(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.rsplit('.').next())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap_or(0)
        + 1;
    let sealed = active.with_file_name(format!(
        "{}.{next}",
        active
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("tier")
    ));
    match std::fs::rename(&active, &sealed) {
        Ok(()) => {
            // ⚠ The cached runtime is keyed by path and holds the replayed state
            // of the file that just moved. Dropping it is what actually releases
            // the memory — without this the seal bounds the FILE and not the
            // process, which is the entire point.
            ehdb_reference::forget_runtime(&active);
            super::metrics::record_tier_seal(tier, reason.as_str());
            tracing::info!(
                tier = tier.as_str(),
                bytes = len,
                reason = reason.as_str(),
                max_bytes = ?policy.max_bytes,
                max_age_secs = ?policy.max_age.map(|d| d.as_secs()),
                sealed = %sealed.display(),
                "EHDB tier store sealed (noetl/ai-meta#332 size cap, #491 age cap)"
            );
            // ⚠ Indexed in the BACKGROUND, not here: building it replays the
            // whole segment, and this runs under the store's write lock on the
            // append path. Doing it inline would make the append that happens to
            // trigger a seal pay a multi-GB scan.
            spawn_index_build(sealed.clone(), tier);
            Some(sealed)
        }
        Err(e) => {
            tracing::warn!(tier = tier.as_str(), error = %e, "EHDB tier seal failed; continuing unsealed");
            None
        }
    }
}

fn driver(cfg: &TierStoreConfig, tier: StoreTier) -> LocalReferenceEventLogDriver {
    LocalReferenceEventLogDriver::new(
        cfg.path_for(tier),
        DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
        DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
    )
}

/// Ensure the store directory exists. Called before an append; a missing parent
/// directory is a configuration state, not an error to surface per-request.
fn ensure_dir(cfg: &TierStoreConfig) -> Result<(), String> {
    std::fs::create_dir_all(&cfg.dir).map_err(|e| format!("create {}: {e}", cfg.dir.display()))
}

/// Append one record. `execution_id` and `payload` are required; an empty
/// payload is refused rather than stored, because an empty record is
/// indistinguishable from a read miss later.
/// Run a store operation on the BLOCKING pool instead of a runtime thread.
///
/// ⚠⚠ This exists because of a production outage on 2026-09-22. Every
/// `*_locked` helper below is synchronous and CPU-bound — it replays JSONL
/// segments into memory, and production's are 1.08 GB and 3.3 GB. They were
/// called directly from the `async fn` wrappers, so the replay ran **on a tokio
/// runtime thread**.
///
/// Tokio sizes its runtime to available parallelism and the writer's cpu limit
/// is **2**, so there are **2 runtime threads**. A handful of concurrent tier
/// requests therefore occupied every runtime thread, and nothing else on the
/// runtime could run: the tier accept loop, its shed path, the metrics server,
/// and the worker's own control-plane registration all stopped. Measured at the
/// time: `tier-concurrency` failing with `timed out after 30s`, the process
/// pegged at ~0.88 cores across two `tokio-rt-worker` threads, and
/// `spawn_blocking` appearing **zero** times in the tier service.
///
/// The bound on concurrency (`NOETL_EHDB_TIER_MAX_INFLIGHT`) does not help here
/// and made it worse when raised: more permits means more CPU-bound tasks on
/// the same two threads. Concurrency limits share a runtime; they do not create
/// one.
///
/// ⚠ The caller holds the store lock across this await deliberately. The guard
/// lives in the async frame while the work runs on the blocking pool, so the
/// critical section is unchanged — only the thread it burns is.
async fn off_runtime<T, F>(f: F) -> Result<T, String>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("tier store task failed to join: {e}"))
}

pub async fn append(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    // Validation is outside the critical section: refusing a malformed request
    // must not wait behind a queue of good ones.
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    append_with_seal(
        Some(cfg),
        tier,
        execution_id,
        payload,
        SealPolicy::from_env(),
    )
    .await
}

/// [`append`] with the seal threshold injected.
///
/// ⚠ Exists so tests can arm the seal WITHOUT `std::env::set_var`. They did
/// once: `cargo test` does not serialise tests, the variable is process-global,
/// and it armed sealing inside an unrelated sibling
/// (`concurrent_appends_to_one_tier_stay_readable` went from 24 records to 8).
/// A test that can only be written by mutating global state is a test that will
/// eventually break a different one.
pub async fn append_with_seal(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
    seal: impl Into<SealPolicy>,
) -> TierStoreOutcome {
    let seal: SealPolicy = seal.into();
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    let lock = store_lock(cfg, tier);
    let _exclusive = lock.write().await;
    // ⚠ Under the WRITE lock, before the append: no reader or writer can be
    // mid-operation on the file being renamed. No-op unless a threshold is
    // configured AND the active segment is over it (size) or older than it
    // (age). Inside `off_runtime`: the age check reads the file.
    let cfg = cfg.clone();
    let execution_id = execution_id.to_string();
    let payload = payload.to_string();
    match off_runtime(move || {
        let _ = maybe_seal(&cfg, tier, seal);
        append_locked(&cfg, tier, &execution_id, &payload)
    })
    .await
    {
        Ok(v) => v,
        Err(e) => TierStoreOutcome::Error(e),
    }
}

/// Append N records under ONE store-write lock and ONE `fsync`
/// (noetl/ai-meta#155).
///
/// The single-record path pays an `fsync` per record — measured at ~118 ms per
/// record at production payload size, on an empty store, so it is a fixed cost
/// and it dominates mirroring. This is the same work with the per-record cost
/// paid once.
///
/// Durability and ordering are unchanged: the engine writes the records in the
/// order given and returns only after the `fsync` that covers them, so every
/// returned sequence is on disk exactly as it was before.
///
/// Returns one outcome per record, in request order, so a caller can report
/// per-record results exactly as it did when it looped.
pub async fn append_batch(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
    payloads: &[String],
) -> Vec<TierStoreOutcome> {
    let Some(cfg) = cfg else {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Unavailable)
            .collect();
    };
    if payloads.is_empty() {
        return Vec::new();
    }
    // Validation outside the critical section, same as the single path. An
    // invalid batch is refused whole: a partial append would leave the caller
    // unable to say which records landed.
    if execution_id.trim().is_empty() {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Invalid("execution_id is empty".to_string()))
            .collect();
    }
    if let Some(index) = payloads.iter().position(|p| p.is_empty()) {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Invalid(format!("payload {index} of the batch is empty")))
            .collect();
    }

    let lock = store_lock(cfg, tier);
    let _exclusive = lock.write().await;
    let seal = SealPolicy::from_env();
    let cfg = cfg.clone();
    let execution_id = execution_id.to_string();
    let payloads: Vec<String> = payloads.to_vec();
    // ⚠ The seal runs INSIDE `off_runtime`, under the same write lock, exactly
    // as on the single path. It used to run on the runtime thread here; a stat
    // and a rename were cheap enough to get away with, but the age check reads
    // the active file, and blocking file I/O does not belong on one of the
    // writer's two runtime threads (the 2026-09-22 outage).
    match off_runtime(move || {
        let _ = maybe_seal(&cfg, tier, seal);
        append_batch_locked(&cfg, tier, &execution_id, &payloads)
    })
    .await
    {
        Ok(v) => v,
        Err(e) => vec![TierStoreOutcome::Error(e)],
    }
}

fn append_batch_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    payloads: &[String],
) -> Vec<TierStoreOutcome> {
    if let Err(e) = ensure_dir(cfg) {
        return payloads
            .iter()
            .map(|_| TierStoreOutcome::Error(e.clone()))
            .collect();
    }
    let requests: Vec<EventLogAppendRequest> = payloads
        .iter()
        .map(|payload| EventLogAppendRequest {
            execution_id: execution_id.to_string(),
            transaction_id: super::eventlog::new_transaction_id(),
            event_id: event_id_from_payload(payload),
            payload: payload.clone(),
        })
        .collect();

    // Seal-straddling dedup (noetl/ai-meta#472), same reasoning as the single
    // path. A request whose id was claimed recently is dropped from the batch
    // rather than sent, so the driver never sees it.
    let mut deduped = 0usize;
    let requests: Vec<EventLogAppendRequest> = requests
        .into_iter()
        .filter(|r| match r.event_id.as_deref() {
            Some(id) if !claim_event_id(tier, id) => {
                deduped += 1;
                super::metrics::record_tier_dedup_hit(tier);
                false
            }
            _ => true,
        })
        .collect();
    if requests.is_empty() {
        return payloads
            .iter()
            .map(|_| {
                TierStoreOutcome::Ok(
                    serde_json::json!({
                        "action": "eventlog-append-batch",
                        "deduplicated": true,
                        "deduped": deduped,
                        "window": "recent-event-ids",
                    })
                    .to_string(),
                )
            })
            .collect();
    }
    match driver(cfg, tier).append_batch(&requests) {
        Ok(outs) => {
            // Record store state once for the batch — the same signal the
            // single path records per append (ai-meta#260), and the last
            // record's sequence is the store's sequence after the batch.
            if let Some(last) = outs.last() {
                super::metrics::record_tier_service_append(
                    tier,
                    last.global_sequence,
                    store_bytes(cfg, tier),
                );
            }
            outs.into_iter()
                .map(|out| {
                    TierStoreOutcome::Ok(
                        serde_json::to_string(&serde_json::json!({
                            "appended": true,
                            "global_sequence": out.global_sequence,
                            "log_record_count": out.log_record_count,
                            // ⚠ The caller checks the reply for strictly
                            // increasing sequences and for
                            // log_record_count == global_sequence.  A dedupe
                            // satisfies NEITHER — it returns the existing
                            // position and does not advance the count — so
                            // without this flag a working dedupe reports as a
                            // parity divergence (noetl/ai-meta#313).
                            "deduplicated": out.deduplicated,
                        }))
                        .unwrap_or_else(|_| "{}".to_string()),
                    )
                })
                .collect()
        }
        // The batch is refused whole, so every record reports the same error
        // rather than the caller guessing a split point.
        Err(e) => payloads
            .iter()
            .map(|_| TierStoreOutcome::Error(e.to_string()))
            .collect(),
    }
}

/// The append itself. Callers reach it only through [`append`], which holds the
/// store's write lock for the whole replay → sequence → write → flush window.
fn append_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    payload: &str,
) -> TierStoreOutcome {
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    if payload.is_empty() {
        return TierStoreOutcome::Invalid("payload is empty".to_string());
    }
    if let Err(e) = ensure_dir(cfg) {
        return TierStoreOutcome::Error(e);
    }
    let request = EventLogAppendRequest {
        execution_id: execution_id.to_string(),
        transaction_id: super::eventlog::new_transaction_id(),
        payload: payload.to_string(),
        // Deploy A: inert.  Deploy B populates this from the payload's event_id.
        event_id: event_id_from_payload(payload),
    };
    // Seal-straddling dedup (noetl/ai-meta#472). The driver's own dedup sees the
    // ACTIVE segment only, so a redelivery whose original landed before a seal
    // would append a second copy. Checked HERE, before the driver, because this
    // is the only place that sees appends across seals.
    if let Some(id) = request.event_id.as_deref() {
        if !claim_event_id(tier, id) {
            super::metrics::record_tier_dedup_hit(tier);
            return TierStoreOutcome::Ok(
                serde_json::json!({
                    "action": "eventlog-append",
                    "execution_id": request.execution_id,
                    "deduplicated": true,
                    "window": "recent-event-ids",
                })
                .to_string(),
            );
        }
    }
    match driver(cfg, tier).append(&request) {
        Ok(out) => {
            // The store's own state, recorded on the write path (ai-meta#260).
            // Observing it here rather than on a read is what makes the tier
            // checkable without generating traffic against it — the question
            // before a `primary` flip is "does this store hold anything", and
            // answering it by reading would change what is being measured.
            super::metrics::record_tier_service_append(
                tier,
                out.global_sequence,
                store_bytes(cfg, tier),
            );
            // `log_record_count` is what makes the append VERIFIABLE by the
            // caller.  The remote appender cannot open this store, so without the
            // count in the reply the only parity check available to it is
            // ordering — and the serve decision on the service-resolved path
            // (`eventlog::serve_service_append`) needs the same gapless
            // invariant `mirror_event` checks locally: `log_record_count ==
            // global_sequence`.  Additive: a caller that does not read it is
            // unaffected, and one that does degrades to the ordering check alone
            // when talking to a writer that predates the field.
            TierStoreOutcome::Ok(
                serde_json::to_string(&serde_json::json!({
                    "appended": true,
                    "global_sequence": out.global_sequence,
                    "log_record_count": out.log_record_count,
                    // See the batch path: a dedupe fails both parity checks
                    // unless the caller is told it was one.
                    "deduplicated": out.deduplicated,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
            )
        }
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    }
}

/// Size of the backing event-log file, or 0 when it does not exist yet.
///
/// 0-on-error is safe here in a way it usually is not: this is only ever
/// reported alongside `store_appends_total` and `store_sequence`, so a stat that
/// fails shows as "0 bytes holding N records", which is visibly wrong rather
/// than quietly plausible.
pub(crate) fn store_bytes(cfg: &TierStoreConfig, tier: StoreTier) -> u64 {
    std::fs::metadata(cfg.path_for(tier))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Highest global sequence the store already holds, read once at startup.
///
/// Without this, a writer that restarts in front of a populated store reports
/// `sequence 0` until the next append — which reads exactly like an empty store,
/// on the component being promoted to authoritative.
///
/// ⚠ Pages to the END. This used to read one `MAX_SCAN_LIMIT` page and take its
/// max, so a store holding more than 1000 records reported the 1000th record's
/// sequence after every restart — a confident wrong number, not a zero, on the
/// gauge meant to show the store is populated (noted on noetl/worker#368).
pub(crate) fn startup_sequence(cfg: &TierStoreConfig, tier: StoreTier) -> u64 {
    let d = driver(cfg, tier);
    let mut highest = 0u64;
    let mut after: Option<u64> = None;
    loop {
        let page = match d.scan_global(&EventLogScanRequest {
            after,
            limit: MAX_SCAN_LIMIT,
        }) {
            Ok(out) => out.records,
            Err(_) => return highest,
        };
        let Some(last) = page.iter().map(|r| r.global_sequence).max() else {
            return highest;
        };
        // A page that does not move past what we already have means the cursor
        // is not advancing; stop rather than spin at startup forever.
        if last <= highest {
            return highest;
        }
        highest = last;
        if page.len() < MAX_SCAN_LIMIT {
            return highest;
        }
        after = Some(last);
    }
}

/// Read every record for one execution.
pub async fn read_execution(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    execution_id: &str,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    if execution_id.trim().is_empty() {
        return TierStoreOutcome::Invalid("execution_id is empty".to_string());
    }
    let lock = store_lock(cfg, tier);
    let shared = lock.read().await;
    let locked_phase = {
        let cfg = cfg.clone();
        let execution_id = execution_id.to_string();
        off_runtime(move || read_execution_locked(&cfg, tier, &execution_id)).await
    };
    // ⚠⚠ Released BEFORE the sealed segments are read. A sealed segment is
    // immutable — it is created by a rename under the WRITE lock and never
    // written again — so reading it needs no lock. Holding the shared guard
    // across its replay (256 MiB on prod) queued every append behind the read,
    // because tokio's RwLock is write-preferring: on 2026-10-20 a live chat
    // turn's mirror append was shed behind comparator reads of a freshly sealed
    // segment (noetl/ai-meta#490). The sealed LIST and the active segment were
    // both taken under the guard, so the answer is the store as of that instant;
    // a seal after release renames a file this read already holds the contents
    // of, and adds a segment it correctly does not consult.
    drop(shared);
    match locked_phase {
        Err(e) => TierStoreOutcome::Error(e),
        Ok(LockedRead::Complete(outcome)) => outcome,
        Ok(LockedRead::NeedsSealed {
            sealed,
            active,
            active_torn,
            lease,
        }) => {
            let cfg = cfg.clone();
            let execution_id = execution_id.to_string();
            match off_runtime(move || {
                let out = read_execution_across_segments(
                    &cfg,
                    tier,
                    &execution_id,
                    &sealed,
                    active,
                    active_torn,
                );
                drop(lease);
                out
            })
            .await
            {
                Ok(v) => v,
                Err(e) => TierStoreOutcome::Error(e),
            }
        }
    }
}

/// Sealed segments that end mid-line: a torn tail (noetl/ehdb#262).
///
/// ⚠ A sealed segment is immutable, so its last byte answers this for good,
/// and it answers it for EVERY sealed segment, not only the ones a read
/// replays. The torn line is unparseable, so its execution is unknowable:
/// neither the index nor the offset index can say which reads it belongs to,
/// and a read that skipped the segment via either one could be missing it.
/// Every JSONL line the store writes ends in `\n`, so any other last byte is a
/// write cut short.
fn sealed_torn_tails(cfg: &TierStoreConfig, tier: StoreTier) -> usize {
    use std::io::{Read, Seek, SeekFrom};
    sealed_segments(cfg, tier)
        .iter()
        .filter(|path| {
            let Ok(mut f) = std::fs::File::open(path) else {
                return false;
            };
            let mut last = [0u8; 1];
            f.seek(SeekFrom::End(-1)).is_ok() && f.read_exact(&mut last).is_ok() && last[0] != b'\n'
        })
        .count()
}

/// What the locked phase of a read produced.
enum LockedRead {
    /// No sealed segment can hold the execution; the active segment answered.
    Complete(TierStoreOutcome),
    /// These sealed segments must be merged in front of the active segment's
    /// records, which were already read under the lock.
    NeedsSealed {
        sealed: Vec<PathBuf>,
        active: Result<Vec<serde_json::Value>, String>,
        /// The active segment's own torn-tail count, from the driver.
        active_torn: usize,
        /// Held until the unlocked sealed read finishes, so a prune cannot
        /// unlink a segment this read has already named (noetl/ai-meta#491).
        lease: SegmentLease,
    },
}

fn read_execution_locked(cfg: &TierStoreConfig, tier: StoreTier, execution_id: &str) -> LockedRead {
    let request = EventLogReadExecutionRequest {
        execution_id: execution_id.to_string(),
        after: None,
        limit: READ_EXECUTION_LIMIT,
    };
    // ⚠⚠ Sealed segments FIRST, then the active one. A read that consulted only
    // the active segment would silently lose every record written before the
    // last seal — turning a memory fix into a data-loss bug, which is strictly
    // worse than the OOM it was meant to cure. `sealed_segments` is empty on a
    // store that has never sealed, so this is exactly today's behaviour there.
    // ⚠ The INDEX decides which sealed segments must be opened. Without it every
    // read replays every sealed segment, which is what broke the tier read path
    // after sealing (2259 "timed out after 2s" in production). A segment with no
    // index is still opened — absence is "unknown", never "absent".
    let (sealed, total_sealed) = segments_possibly_holding(cfg, tier, execution_id);
    if !sealed.is_empty() {
        // Only the ACTIVE segment is read here; the sealed ones are read by the
        // caller after it releases the lock.
        let active =
            read_segment_then_release(&cfg.path_for(tier), false, |d| d.read_execution(&request))
                .map_err(|e| e.to_string());
        let active_torn = active.as_ref().map_or(0, |out| out.torn_tail_skipped);
        let active = active.map(|out| records_of(&out));
        // ⚠ Leased HERE, under the shared lock, in the same instant the sealed
        // list was snapshot. The prune takes the WRITE lock and skips a leased
        // segment, so the two cannot interleave: a segment is either leased
        // before the prune looks, or gone before any reader can name it.
        let lease = SegmentLease::acquire(&sealed);
        return LockedRead::NeedsSealed {
            sealed,
            active,
            active_torn,
            lease,
        };
    }
    if total_sealed > 0 {
        // Every sealed segment was ruled out by its index; the active segment
        // alone answers. This is the fast path the index exists to create.
        tracing::debug!(
            tier = tier.as_str(),
            execution_id,
            skipped = total_sealed,
            "tier read skipped sealed segments via index"
        );
    }
    LockedRead::Complete(match driver(cfg, tier).read_execution(&request) {
        Ok(out) => {
            let mut v = serde_json::to_value(&out).unwrap_or(serde_json::Value::Null);
            // ⚠ The driver reports `exists: true` for an execution it holds NO
            // records for, so a caller using `exists` to decide hit-vs-miss gets
            // the wrong answer (found by the PR-3 gate, which asserts on record
            // payloads for exactly this reason).  Normalise it at the boundary we
            // own rather than leaving a field that means the opposite of its name:
            // `exists` now tracks whether any record came back.
            if let Some(obj) = v.as_object_mut() {
                let n = obj
                    .get("record_count")
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0);
                obj.insert("exists".to_string(), serde_json::Value::Bool(n > 0));
                // The driver saw the active segment only; sealed segments the
                // index ruled out can still hold a torn tail.
                let torn = out.torn_tail_skipped + sealed_torn_tails(cfg, tier);
                if torn > 0 {
                    obj.insert(
                        "torn_tail_skipped".to_string(),
                        serde_json::Value::from(torn),
                    );
                }
            }
            TierStoreOutcome::Ok(serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string()))
        }
        Err(e) => TierStoreOutcome::Error(e.to_string()),
    })
}

/// Build a segment's index off the request path.
///
/// Best-effort by design: a failed index build leaves the segment unindexed,
/// which means "might contain" — correct, just slow. It must never fail an
/// append or a read.
/// Index builds are serialised behind this lock.
///
/// The backfill spawns one build per unindexed segment, and production has two
/// (1.08 GB + 3.3 GB). Serialising bounds the work to one segment scan at a
/// time rather than letting startup contend for CPU across all of them.
static INDEX_BUILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn spawn_index_build(segment: PathBuf, tier: StoreTier) {
    tokio::spawn(async move {
        let seg = segment.clone();
        let res = tokio::task::spawn_blocking(move || {
            let _serialised = INDEX_BUILD_LOCK.lock();
            build_segment_index(&seg)
        })
        .await;
        match res {
            Ok(Ok(n)) => tracing::info!(
                tier = tier.as_str(), segment = %segment.display(), executions = n,
                "EHDB tier segment indexed"
            ),
            Ok(Err(e)) => tracing::warn!(
                tier = tier.as_str(), segment = %segment.display(), error = %e,
                "EHDB tier segment index build failed; reads will open this segment"
            ),
            Err(e) => tracing::warn!(
                tier = tier.as_str(), segment = %segment.display(), error = %e,
                "EHDB tier segment index task failed; reads will open this segment"
            ),
        }
    });
}

/// Index any sealed segment that has none yet.
///
/// ⚠ Needed because segments sealed by an earlier build have no index, and
/// production already has two of them (1.08 GB and 3.3 GB). Without a backfill
/// the index would only ever help segments sealed in the future, leaving the
/// exact segments that caused the regression unindexed forever.
pub fn backfill_segment_indexes(cfg: &TierStoreConfig) {
    for &tier in StoreTier::ALL.iter() {
        for seg in sealed_segments(cfg, tier) {
            if load_segment_index(&seg).is_none() || !segment_offset_index_path(&seg).exists() {
                tracing::info!(
                    tier = tier.as_str(), segment = %seg.display(),
                    "EHDB tier segment has no index; building it"
                );
                spawn_index_build(seg, tier);
            }
        }
    }
}

/// The execution-id index beside a sealed segment: `<segment>.idx`.
///
/// ⚠⚠ WHY THIS EXISTS — a regression I introduced and measured.
///
/// Sealing bounded memory (2.9 GB -> 186 MB in production) but broke the tier's
/// READ path. A merged read must consult every sealed segment, and
/// `read_segment_then_release` forgets each one immediately, so **every** read
/// re-replays the whole sealed file. Production's first sealed segment is
/// 1.08 GB, which cannot be replayed inside the tier client's 2-second read
/// timeout. Measured on prod logs, split at the seal:
///
/// ```text
///   before the seal (22:00-22:30Z):     0 read failures
///   after  the seal (22:35Z onward): 2259 read failures, all "timed out after 2s"
/// ```
///
/// (I had previously called this failure pre-existing. That was wrong: I ran a
/// 100-minute window and asserted it spanned the seal without checking the
/// timestamps. It did not.)
///
/// The index makes a read skip segments that cannot contain the execution.
/// That works because the tier mirrors an execution's events over the execution's
/// lifetime, so an execution's records land in the segment active at the time —
/// almost always exactly one.
///
/// Format: one execution id per line, sorted, newline-terminated. A sidecar, so
/// **the segment's own bytes are untouched** and an older binary that ignores
/// `.idx` files still reads the segment correctly.
///
/// ⚠ A MISSING index means "might contain" — the segment is opened. Absence must
/// never be read as "does not contain", or a read would silently skip records
/// that exist, which is the data-loss-shaped failure this whole change has been
/// avoiding.
fn segment_index_path(segment: &std::path::Path) -> PathBuf {
    let mut s = segment.as_os_str().to_os_string();
    s.push(".idx");
    PathBuf::from(s)
}

/// Load a segment's index. `None` = no index = "might contain, must open".
fn load_segment_index(segment: &std::path::Path) -> Option<std::collections::HashSet<String>> {
    let raw = std::fs::read_to_string(segment_index_path(segment)).ok()?;
    // An index that exists but is EMPTY is a real answer (a segment with no
    // executions), so it is distinguished from a missing file by `Some`.
    Some(
        raw.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|l| l.to_string())
            .collect(),
    )
}

/// Build the index for a sealed segment by paging its records once.
///
/// One replay total: the runtime cache serves the pages after the first open,
/// and the runtime is released at the end so the scan does not leave a
/// multi-GB state resident — the point of sealing in the first place.
///
/// Written to a temp file and renamed, so a reader never observes a partial
/// index and mistake it for "this segment contains only these executions".
/// Extract the execution id from one stored segment line.
///
/// The driver derives `EventLogRecordView::execution_id` as
/// `execution_from_subject(record.subject)` (`ehdb-reference/src/eventlog.rs`),
/// i.e. the trailing token after [`ehdb_reference::EVENT_LOG_SUBJECT_PREFIX`].
/// This reads the same field off the stored line, so the index is keyed exactly
/// as a lookup keys it. `streaming_index_equals_replay_index` is the guard on
/// that claim — it builds both ways and asserts set equality.
///
/// Why a targeted scan rather than a full JSON parse: a stored payload is a
/// **numeric byte array**, so it contains no quoted strings and cannot produce a
/// false `"subject":"` match; parsing it as `Value` would allocate one `Value`
/// per payload byte for no gain. Measured on the production segment, 82,906 of
/// 82,906 lines carry the subject.
fn execution_id_from_line(line: &str) -> Option<String> {
    const KEY: &str = "\"subject\":\"";
    let start = line.find(KEY)? + KEY.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    let subject = &rest[..end];
    Some(
        subject
            .strip_prefix(&format!("{}.", ehdb_reference::EVENT_LOG_SUBJECT_PREFIX))
            .unwrap_or(subject)
            .to_string(),
    )
}

/// Build `<segment>.idx` by STREAMING the segment, never replaying it.
///
/// ⚠ This must not go back to a `LocalReferenceEventLogDriver` replay. Replay
/// holds the whole segment in the runtime cache, and RSS was measured at ~2.8x
/// record bytes for this workload — production's two sealed segments are 1.08 GB
/// and 3.3 GB, so a replay-based backfill peaks around 12 GB and OOM-kills the
/// writer on startup, on every restart. Streaming is O(1) in the segment size:
/// one line at a time, and only the distinct ids are retained (969 for the
/// 1.08 GB production segment).
///
/// ⚠ Fail-safe direction: a line whose subject cannot be read ABORTS the build
/// and writes no index. A missing index means "might contain" and the segment is
/// opened — correct but slow. A *partial* index read as complete would silently
/// skip a segment that holds the execution, which is a wrong answer.
pub fn build_segment_index(segment: &std::path::Path) -> Result<usize, String> {
    use std::io::BufRead;
    let f = std::fs::File::open(segment).map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::with_capacity(1 << 20, f);
    // execution_id -> (offset, length) of each of its lines, in file order.
    let mut ids: std::collections::BTreeMap<String, Vec<(u64, u64)>> = Default::default();
    let mut line = String::new();
    let mut offset = 0u64;
    let mut n = 0usize;
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("line {}: {e}", n + 1))?;
        if read == 0 {
            break;
        }
        n += 1;
        let start = offset;
        offset += read as u64;
        let body = line.strip_suffix('\n').unwrap_or(&line);
        if body.trim().is_empty() {
            continue;
        }
        match execution_id_from_line(body) {
            Some(id) => ids.entry(id).or_default().push((start, body.len() as u64)),
            None => {
                return Err(format!(
                    "line {n} carries no readable subject; refusing to write a partial index"
                ))
            }
        }
    }

    let body: String = ids.keys().map(|i| format!("{i}\n")).collect();
    let final_path = segment_index_path(segment);
    let tmp = final_path.with_extension("idx.tmp");
    std::fs::write(&tmp, body.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &final_path).map_err(|e| e.to_string())?;

    // Best-effort: without it a read replays the segment, which is slow and
    // correct. It must never fail the `.idx` that rules segments out.
    if let Err(e) = write_offset_index(segment, offset, &ids) {
        tracing::warn!(
            segment = %segment.display(), error = %e,
            "EHDB tier segment offset index not written; reads will replay this segment"
        );
    }
    Ok(ids.len())
}

/// The per-execution line offsets beside a sealed segment: `<segment>.offidx`.
///
/// ⚠⚠ WHY — noetl/ai-meta#490. `.idx` decides WHICH sealed segments to open,
/// but opening one replays the whole file: 256 MiB per read on prod, far past
/// the tier client's 2 s timeout, and the abandoned replay keeps its in-flight
/// permit until it finishes. On 2026-10-20 that shed a live chat turn's mirror
/// append. With this sidecar a sealed read costs this execution's lines.
///
/// ⚠ A SEPARATE file, not a new `.idx` format. An older binary reads every
/// `.idx` line as an execution id; handing it offsets after a rollback would
/// make it rule out segments that hold the execution — a wrong answer. It
/// ignores `.offidx` entirely.
///
/// Format: a header `ehdb-offidx v1 <segment byte length>`, then one line per
/// execution: `<execution_id>\t<offset>:<length> <offset>:<length> …`. The
/// recorded length is checked on every read; a mismatch means the sidecar does
/// not describe this file and the read replays instead.
fn segment_offset_index_path(segment: &std::path::Path) -> PathBuf {
    let mut s = segment.as_os_str().to_os_string();
    s.push(".offidx");
    PathBuf::from(s)
}

const OFFSET_INDEX_HEADER: &str = "ehdb-offidx v1";

fn write_offset_index(
    segment: &std::path::Path,
    segment_len: u64,
    ids: &std::collections::BTreeMap<String, Vec<(u64, u64)>>,
) -> Result<(), String> {
    use std::fmt::Write as _;
    let mut body = format!("{OFFSET_INDEX_HEADER} {segment_len}\n");
    for (id, spans) in ids {
        body.push_str(id);
        body.push('\t');
        for (i, (off, len)) in spans.iter().enumerate() {
            if i > 0 {
                body.push(' ');
            }
            let _ = write!(body, "{off}:{len}");
        }
        body.push('\n');
    }
    let final_path = segment_offset_index_path(segment);
    let tmp = final_path.with_extension("offidx.tmp");
    std::fs::write(&tmp, body.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &final_path).map_err(|e| e.to_string())
}

/// This execution's line spans in `segment`, if the offset index can vouch for
/// them. `None` = no usable index (missing, unparsable, or written for a file
/// of a different length) — the caller must replay.
fn load_offset_spans(segment: &std::path::Path, execution_id: &str) -> Option<Vec<(u64, u64)>> {
    let raw = std::fs::read_to_string(segment_offset_index_path(segment)).ok()?;
    let mut lines = raw.lines();
    let recorded: u64 = lines
        .next()?
        .strip_prefix(OFFSET_INDEX_HEADER)?
        .trim()
        .parse()
        .ok()?;
    if std::fs::metadata(segment).ok()?.len() != recorded {
        return None;
    }
    let prefix = format!("{execution_id}\t");
    for l in lines {
        if let Some(rest) = l.strip_prefix(&prefix) {
            return rest
                .split(' ')
                .map(|span| {
                    let (o, n) = span.split_once(':')?;
                    Some((o.parse().ok()?, n.parse().ok()?))
                })
                .collect();
        }
    }
    // The header is valid and the execution is absent: this segment holds none
    // of its records. That is an answer, not "unknown".
    Some(Vec::new())
}

/// Read one execution's records from a SEALED segment by seeking to its lines.
///
/// Builds the same `EventLogRecordView` the driver projects — `global_sequence`
/// from the `Publish` mutation's sequence, the execution from the subject, the
/// payload bytes as lossy UTF-8 — so the result is indistinguishable from a
/// replay (`an_offset_read_equals_a_replay_read` holds it to that).
///
/// ⚠ Fail-safe: ANY surprise returns `None` and the caller replays — a line
/// that does not parse, a span past the end, a line whose `Publish` is not this
/// execution's, a foreign tenant/namespace/stream. The offset path may only
/// ever be faster than the replay, never different from it.
fn read_sealed_by_offsets(
    segment: &std::path::Path,
    execution_id: &str,
) -> Option<Vec<serde_json::Value>> {
    use std::io::{Read, Seek, SeekFrom};
    let spans = load_offset_spans(segment, execution_id)?;
    let subject = format!(
        "{}.{execution_id}",
        ehdb_reference::EVENT_LOG_SUBJECT_PREFIX
    );
    let mut f = std::fs::File::open(segment).ok()?;
    let mut views: Vec<ehdb_reference::EventLogRecordView> = Vec::new();
    let mut buf = Vec::new();
    for (off, len) in spans {
        buf.resize(usize::try_from(len).ok()?, 0);
        f.seek(SeekFrom::Start(off)).ok()?;
        f.read_exact(&mut buf).ok()?;
        let line: serde_json::Value = serde_json::from_slice(&buf).ok()?;
        if line.get("tenant")?.as_str()? != DEFAULT_LOCAL_REFERENCE_TENANT
            || line.get("namespace")?.as_str()? != DEFAULT_LOCAL_REFERENCE_NAMESPACE
        {
            return None;
        }
        let transaction_id = line.get("transaction_id")?.as_str()?;
        let before = views.len();
        for m in line.get("mutations")?.as_array()? {
            let Some(p) = m.get("Stream").and_then(|s| s.get("Publish")) else {
                continue;
            };
            if p.get("stream")?.as_str()? != ehdb_reference::EVENT_LOG_STREAM
                || p.get("subject")?.as_str()? != subject
            {
                return None;
            }
            let payload: Vec<u8> = p
                .get("payload")?
                .as_array()?
                .iter()
                .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                .collect::<Option<_>>()?;
            views.push(ehdb_reference::EventLogRecordView {
                global_sequence: p.get("sequence")?.as_u64()?,
                execution_id: execution_id.to_string(),
                transaction_id: transaction_id.to_string(),
                byte_len: payload.len(),
                payload: String::from_utf8_lossy(&payload).into_owned(),
            });
        }
        // The index said this line is the execution's; a line with no Publish
        // of it means the index and the file disagree.
        if views.len() == before {
            return None;
        }
    }
    views.sort_by_key(|v| v.global_sequence);
    views.iter().map(|v| serde_json::to_value(v).ok()).collect()
}

/// Segments that could hold `execution_id`, oldest first.
///
/// A segment with no index is INCLUDED — absence of an index is "unknown", never
/// "absent".
fn segments_possibly_holding(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
) -> (Vec<PathBuf>, usize) {
    let all = sealed_segments(cfg, tier);
    let total = all.len();
    let kept = all
        .into_iter()
        .filter(|s| match load_segment_index(s) {
            Some(ids) => ids.contains(execution_id),
            None => true,
        })
        .collect();
    (kept, total)
}

/// Read a segment and RELEASE it, so a merge is bounded by one segment at a
/// time rather than by the sum of them.
///
/// ⚠⚠ Without this the merge is a second route to the OOM it was built to cure.
/// `LocalReferenceEventLogDriver` opens through the reference-runtime cache,
/// which replays the file and then holds that state for the life of the
/// process. Production's existing tier store is a single **4.1 GB** file, so
/// the first seal makes it sealed segment #1 — and the first cross-segment read
/// would replay 4.1 GB back into memory and keep it, on top of the active
/// segment's state. Two segments later that is worse than where this started.
///
/// So each SEALED segment is forgotten immediately after it is read. The active
/// segment is deliberately NOT forgotten: it is the one the append path uses on
/// every write, and evicting it would reintroduce a full replay per append —
/// the exact cost the runtime cache exists to avoid.
fn read_segment_then_release<T>(
    path: &Path,
    is_sealed: bool,
    f: impl FnOnce(&LocalReferenceEventLogDriver) -> T,
) -> T {
    let d = LocalReferenceEventLogDriver::new(
        path.to_path_buf(),
        DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
        DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
    );
    let out = f(&d);
    if is_sealed {
        ehdb_reference::forget_runtime(path);
    }
    out
}

/// A merged scan in progress across sealed segments + the active one, oldest
/// first.
///
/// ⚠ `after` is applied to the **merged, renumbered** sequence, not to each
/// segment's own numbering. Passing it down per segment would skip a different
/// set of records in every file, because each driver numbers from 1 within its
/// own log.
///
/// ⚠⚠ Each segment is counted IN FULL before it is paged. This used to read one
/// `MAX_SCAN_LIMIT` page from every segment and renumber what came back, so a
/// sealed segment (~80k records at the 256 MiB seal) contributed its first 1000
/// and nothing else: `record_count` under-reported the log, and every cursor
/// past the first page of a segment landed in the NEXT segment, silently
/// skipping the rest. Same failure class as noetl/ai-meta#490's 1000-record
/// read cap, on the scan side.
struct MergedScan {
    after: u64,
    limit: usize,
    total: u64,
    torn: usize,
    page: Vec<serde_json::Value>,
    /// Sealed segments already folded in, oldest first.
    sealed: Vec<PathBuf>,
}

impl MergedScan {
    fn new(after: Option<u64>, limit: usize) -> Self {
        Self {
            after: after.unwrap_or(0),
            limit,
            total: 0,
            torn: 0,
            page: Vec::new(),
            sealed: Vec::new(),
        }
    }

    /// Fold one segment in: count it, then take whatever part of the page it
    /// owns.
    fn add_segment(&mut self, path: &Path, is_sealed: bool) -> Result<(), String> {
        let base = self.total;
        let want = self.limit - self.page.len();
        let after = self.after;
        // Both reads inside one closure, so a sealed segment is loaded once.
        let read = read_segment_then_release(path, is_sealed, |d| {
            let count = d.scan_global(&EventLogScanRequest {
                after: None,
                limit: 0,
            })?;
            let n = count.record_count as u64;
            let local_after = after.saturating_sub(base).min(n);
            if want == 0 || local_after >= n {
                return Ok((count, None));
            }
            let rows = d.scan_global(&EventLogScanRequest {
                after: (local_after > 0).then_some(local_after),
                limit: want,
            })?;
            Ok::<_, ehdb_reference::EhdbError>((count, Some((local_after, rows))))
        })
        .map_err(|e| format!("tier segment {} unreadable: {e}", path.display()))?;
        let (count, rows) = read;
        self.total += count.record_count as u64;
        self.torn += count.torn_tail_skipped;
        if let Some((local_after, rows)) = rows {
            for (k, mut r) in records_of(&rows).into_iter().enumerate() {
                if let Some(o) = r.as_object_mut() {
                    o.insert(
                        "global_sequence".to_string(),
                        serde_json::Value::from(base + local_after + k as u64 + 1),
                    );
                }
                self.page.push(r);
            }
        }
        Ok(())
    }

    fn finish(self) -> TierStoreOutcome {
        let mut body = serde_json::json!({
            "action": "eventlog-scan",
            "exists": self.total > 0,
            "record_count": self.total,
            "returned": self.page.len(),
            "records": self.page,
            "segments": self.sealed.len() + 1,
        });
        // noetl/ehdb#262: a torn tail skipped in ANY segment stays visible on
        // the wire, exactly as the single-segment driver reply carries it.
        // Absent when zero, matching the driver.
        if self.torn > 0 {
            body["torn_tail_skipped"] = serde_json::Value::from(self.torn);
        }
        TierStoreOutcome::Ok(serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()))
    }
}

/// The unlocked phase of a scan: fold in every sealed segment not yet seen.
///
/// ⚠ A sealed segment is immutable and new seals only ever take the next
/// higher number, so the list can only GROW at its end. Anything else — a
/// segment gone or renumbered — means the store changed shape under the scan,
/// and the merged numbering is no longer one total order; refuse rather than
/// return it.
fn scan_sealed_phase(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    mut acc: MergedScan,
    sealed: Vec<PathBuf>,
) -> Result<MergedScan, String> {
    // The store is named only for the test probes; the list arrives leased.
    let _ = (cfg, tier);
    if !sealed.starts_with(&acc.sealed) {
        return Err("tier segments changed during the scan; retry".to_string());
    }
    let fresh = sealed[acc.sealed.len()..].to_vec();
    if !fresh.is_empty() {
        #[cfg(test)]
        tests::probe_sealed_phase(cfg, tier);
        #[cfg(test)]
        tests::probe_scan_leases(cfg, &fresh);
    }
    for path in &fresh {
        acc.add_segment(path, true)?;
        acc.sealed.push(path.clone());
    }
    Ok(acc)
}

/// The `records` array of a driver read, as JSON values.
fn records_of<T: serde::Serialize>(out: &T) -> Vec<serde_json::Value> {
    match serde_json::to_value(out) {
        Ok(serde_json::Value::Object(mut o)) => match o.remove("records") {
            Some(serde_json::Value::Array(rs)) => rs,
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Read one execution across sealed segments + the active one, oldest first.
///
/// ⚠ `global_sequence` is **renumbered** over the merged result. Each segment's
/// driver numbers records from 1 within its own file, so the raw values collide
/// across segments. Renumbering keeps the field meaning what a reader expects —
/// a total order over what was returned — instead of handing back several
/// records that all claim to be number 1.
fn read_execution_across_segments(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    execution_id: &str,
    sealed: &[PathBuf],
    active: Result<Vec<serde_json::Value>, String>,
    active_torn: usize,
) -> TierStoreOutcome {
    #[cfg(test)]
    tests::probe_sealed_phase(cfg, tier);
    let request = EventLogReadExecutionRequest {
        execution_id: execution_id.to_string(),
        after: None,
        // noetl/ai-meta#492: the sealed replay builds its own request, so it
        // needs the same uncapped limit as the locked phase, or a sealed segment
        // holding >1000 of one execution's records is cut here instead.
        limit: READ_EXECUTION_LIMIT,
    };
    let mut records: Vec<serde_json::Value> = Vec::new();
    for path in sealed {
        // A sealed segment with a usable offset index answers from just this
        // execution's lines; anything else replays the segment as before.
        if let Some(rs) = read_sealed_by_offsets(path, execution_id) {
            records.extend(rs);
            continue;
        }
        match read_segment_then_release(path, true, |d| d.read_execution(&request)) {
            Ok(out) => records.extend(records_of(&out)),
            // A segment that cannot be read is NOT silently skipped: a partial
            // answer presented as complete is the failure this whole change is
            // trying not to cause.
            Err(e) => {
                return TierStoreOutcome::Error(format!(
                    "tier segment {} unreadable: {e}",
                    path.display()
                ))
            }
        }
    }
    // The active segment's records were read under the lock and go LAST.
    match active {
        Ok(rs) => records.extend(rs),
        Err(e) => {
            return TierStoreOutcome::Error(format!(
                "tier segment {} unreadable: {e}",
                cfg.path_for(tier).display()
            ))
        }
    }
    for (i, r) in records.iter_mut().enumerate() {
        if let Some(o) = r.as_object_mut() {
            o.insert(
                "global_sequence".to_string(),
                serde_json::Value::from((i + 1) as u64),
            );
        }
    }
    let n = records.len();
    let mut body = serde_json::json!({
        "action": "eventlog-read-execution",
        "execution_id": execution_id,
        "exists": n > 0,
        "record_count": n,
        "returned": n,
        "records": records,
        "segments": sealed.len() + 1,
    });
    // noetl/ehdb#262: the merge used to drop every segment's torn-tail count,
    // so a torn write read exactly like a complete log. Absent when zero,
    // matching the driver.
    let torn = active_torn + sealed_torn_tails(cfg, tier);
    if torn > 0 {
        body["torn_tail_skipped"] = serde_json::Value::from(torn);
    }
    TierStoreOutcome::Ok(serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()))
}

/// Bounded global scan. `limit` is clamped to [`MAX_SCAN_LIMIT`].
pub async fn scan(
    cfg: Option<&TierStoreConfig>,
    tier: StoreTier,
    after: Option<u64>,
    limit: usize,
) -> TierStoreOutcome {
    let Some(cfg) = cfg else {
        return TierStoreOutcome::Unavailable;
    };
    let limit = limit.clamp(1, MAX_SCAN_LIMIT);
    let lock = store_lock(cfg, tier);
    let mut acc = MergedScan::new(after, limit);
    // ⚠⚠ Sealed segments are read with the store lock RELEASED, as in
    // `read_execution` (noetl/ai-meta#490): holding the shared guard across a
    // 256 MiB replay queues every append behind the scan, because tokio's RwLock
    // is write-preferring. Only the active segment is read under the guard, and
    // only once the sealed list re-read under that same guard matches what was
    // already folded in. A seal in between moved records from the active file
    // into a new sealed one, so the loop releases, folds the new segment in, and
    // checks again. Seals are hours apart; this converges on the second pass.
    // ⚠ The sealed list is taken, and LEASED, under a brief shared guard — the
    // same invariant `read_execution` keeps (noetl/ai-meta#491): the prune takes
    // the write lock and skips a leased segment, so a segment is either leased
    // before the prune looks or gone before this scan can name it. The leases
    // are held until the scan returns.
    let mut leases: Vec<SegmentLease> = Vec::new();
    loop {
        let sealed = {
            let _shared = lock.read().await;
            let cfg = cfg.clone();
            match off_runtime(move || {
                let s = sealed_segments(&cfg, tier);
                let l = SegmentLease::acquire(&s);
                (s, l)
            })
            .await
            {
                Ok((s, l)) => {
                    leases.push(l);
                    s
                }
                Err(e) => return TierStoreOutcome::Error(e),
            }
        };
        acc = {
            let cfg = cfg.clone();
            match off_runtime(move || scan_sealed_phase(&cfg, tier, acc, sealed)).await {
                Ok(Ok(a)) => a,
                Ok(Err(e)) | Err(e) => return TierStoreOutcome::Error(e),
            }
        };
        let _shared = lock.read().await;
        let cfg = cfg.clone();
        match off_runtime(move || scan_locked(&cfg, tier, acc)).await {
            Ok(LockedScan::Done(outcome)) => return outcome,
            Ok(LockedScan::SealedSince(a)) => acc = a,
            Err(e) => return TierStoreOutcome::Error(e),
        }
    }
}

/// What the locked phase of a scan produced.
enum LockedScan {
    Done(TierStoreOutcome),
    /// A segment was sealed after the unlocked phase listed them; fold it in
    /// without the lock and come back.
    SealedSince(MergedScan),
}

fn scan_locked(cfg: &TierStoreConfig, tier: StoreTier, mut acc: MergedScan) -> LockedScan {
    // ⚠⚠ Sealed segments first, same as `read_execution`. This was MISSED in the
    // first cut of the seal — `read_execution` merged and `scan` did not — and a
    // leaked env var in a sibling test is what exposed it: a scan that had been
    // returning 24 records returned 8, the active segment only. Silently
    // returning a suffix of the log is the data-loss failure this seal must not
    // cause, so both readers go through the same merge.
    let sealed = sealed_segments(cfg, tier);
    if sealed != acc.sealed {
        return LockedScan::SealedSince(acc);
    }
    if sealed.is_empty() {
        // Unsealed store: the driver's own reply, unchanged.
        let request = EventLogScanRequest {
            after: (acc.after > 0).then_some(acc.after),
            limit: acc.limit,
        };
        return LockedScan::Done(match driver(cfg, tier).scan_global(&request) {
            Ok(out) => TierStoreOutcome::Ok(
                serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()),
            ),
            Err(e) => TierStoreOutcome::Error(e.to_string()),
        });
    }
    LockedScan::Done(match acc.add_segment(&cfg.path_for(tier), false) {
        Ok(()) => acc.finish(),
        Err(e) => TierStoreOutcome::Error(e),
    })
}

// ===========================================================================
// Segment leases + the retention planner (noetl/ai-meta#491).
// ===========================================================================

/// Per-segment read refcounts: a sealed segment a read has named and not yet
/// finished with. The prune skips any segment with a non-zero count.
fn lease_counts() -> &'static StdMutex<HashMap<PathBuf, usize>> {
    static L: OnceLock<StdMutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    L.get_or_init(|| StdMutex::new(HashMap::new()))
}

/// A read's hold on the sealed segments it is about to read.
///
/// ⚠ Why a lease rather than holding the files open: the replay fallback hands
/// `LocalReferenceEventLogDriver` a PATH and the driver opens it lazily, so a
/// file handle held here would protect only the offset path. A refcount checked
/// by the prune under the write lock protects both.
///
/// Released on drop, so a panic or an early return cannot strand a lease.
pub(crate) struct SegmentLease(Vec<PathBuf>);

impl SegmentLease {
    /// Take a lease on `segments`. Call under the store's shared lock.
    pub(crate) fn acquire(segments: &[PathBuf]) -> Self {
        let mut g = lease_counts().lock().unwrap_or_else(|e| e.into_inner());
        for s in segments {
            *g.entry(s.clone()).or_insert(0) += 1;
        }
        Self(segments.to_vec())
    }
}

impl Drop for SegmentLease {
    fn drop(&mut self) {
        let mut g = lease_counts().lock().unwrap_or_else(|e| e.into_inner());
        for s in &self.0 {
            if let Some(n) = g.get_mut(s) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    g.remove(s);
                }
            }
        }
    }
}

/// How many reads currently hold `segment`.
pub(crate) fn lease_count(segment: &Path) -> usize {
    lease_counts()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(segment)
        .copied()
        .unwrap_or(0)
}

/// `NOETL_EHDB_TIER_PRUNE_MODE`. Default `off`.
pub const TIER_PRUNE_MODE_ENV: &str = "NOETL_EHDB_TIER_PRUNE_MODE";
/// `NOETL_EHDB_TIER_RETENTION_HOURS`. Default 49.
pub const TIER_RETENTION_HOURS_ENV: &str = "NOETL_EHDB_TIER_RETENTION_HOURS";
/// Default retention: 49 hours (owner decision, noetl/ai-meta#491). Still ~16x
/// the longest live reader window (repair sweep, 180 min) and above the 24 h
/// floor below.
pub const TIER_RETENTION_HOURS_DEFAULT: u64 = 49;
/// Below this the live readers (comparator 1 h, repair sweep 3 h) could lose
/// records they still read; refused, not clamped.
pub const TIER_RETENTION_HOURS_MIN: u64 = 24;

/// What the retention pass is allowed to do.
///
/// Three gates, `Off` by default (noetl/ai-meta#491):
/// - `Off` — no pass at all.
/// - `DryRun` — plan and log; deletes nothing.
/// - `Enabled` — unlink the planner's `WouldPrune` prefix (archiving first when
///   [`TIER_PRUNE_ARCHIVE_ENV`] is set). Armed ONLY by the exact value
///   `enabled`: the casual spellings `on` / `true` / `1` stay refused out loud,
///   so a deleting pass can never be switched on by a typo or a boolean habit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneMode {
    Off,
    DryRun,
    Enabled,
}

/// Parse the mode. `dry_run` -> `DryRun`; exactly `enabled` -> `Enabled`;
/// `on`/`true`/`1`/`yes` are refused loudly; anything else is `Off`.
///
/// ⚠ `Enabled` is matched on the RAW value, byte for byte: no trimming, no case
/// folding. `ENABLED`, `Enabled` or `enabled ` are refused out loud like `on`,
/// because the owner decision is that real deletion arms on the exact word only
/// (noetl/ai-meta#491). The non-deleting spellings stay forgiving.
pub fn parse_prune_mode(raw: Option<&str>) -> PruneMode {
    if raw == Some("enabled") {
        return PruneMode::Enabled;
    }
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("dry_run") | Some("dry-run") | Some("dryrun") => PruneMode::DryRun,
        Some("enabled") | Some("on") | Some("true") | Some("1") | Some("yes") => {
            tracing::error!(
                env = TIER_PRUNE_MODE_ENV,
                "tier prune mode must be spelled `enabled` to delete; `on`/`true` \
                 are refused so a deleting pass is never armed by accident \
                 (noetl/ai-meta#491). Treated as `off`."
            );
            PruneMode::Off
        }
        _ => PruneMode::Off,
    }
}

/// `NOETL_EHDB_TIER_PRUNE_ARCHIVE`. Unset (the default) = an enabled pass
/// deletes outright, with NO archive. Set to `gs://bucket[/prefix]` = every
/// segment (and its `.idx` / `.offidx`) is uploaded to GCS and the upload's size
/// verified BEFORE it is unlinked; a segment whose archive fails is not
/// unlinked, and the pass stops. Setting this one variable is all it takes.
pub const TIER_PRUNE_ARCHIVE_ENV: &str = "NOETL_EHDB_TIER_PRUNE_ARCHIVE";
/// `NOETL_EHDB_TIER_PRUNE_ARCHIVE_ENDPOINT`. GCS JSON-API base URL; defaults to
/// `https://storage.googleapis.com`. Overridable for tests and emulators — a
/// non-default endpoint is called WITHOUT a metadata-server token.
pub const TIER_PRUNE_ARCHIVE_ENDPOINT_ENV: &str = "NOETL_EHDB_TIER_PRUNE_ARCHIVE_ENDPOINT";
const GCS_DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

/// Where an enabled pass archives to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveTarget {
    pub bucket: String,
    /// Normalised: no leading/trailing `/`; may be empty.
    pub prefix: String,
}

/// Parse [`TIER_PRUNE_ARCHIVE_ENV`]. `Ok(None)` = unset/blank (no archive).
/// `Err` = set but not a `gs://bucket[/prefix]` URL: the caller must then NOT
/// delete, because the operator asked for an archive and would silently lose it.
pub fn parse_archive_target(raw: Option<&str>) -> Result<Option<ArchiveTarget>, String> {
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let rest = v
        .strip_prefix("gs://")
        .ok_or_else(|| format!("{TIER_PRUNE_ARCHIVE_ENV}={v:?} is not a gs:// URL"))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(format!("{TIER_PRUNE_ARCHIVE_ENV}={v:?} names no bucket"));
    }
    Ok(Some(ArchiveTarget {
        bucket: bucket.to_string(),
        prefix: prefix.trim_matches('/').to_string(),
    }))
}

/// Uploads one file of a segment being pruned. Returns the size the remote
/// store reports for the uploaded object, which the pass compares with the
/// local length before it will unlink anything.
#[async_trait::async_trait]
pub trait SegmentArchiver: Send + Sync {
    async fn archive(&self, tier: StoreTier, file: &Path) -> Result<u64, String>;
}

/// GCS archiver over the JSON API (`uploadType=media`), authenticated with the
/// GCE metadata server's token (Workload Identity on GKE).
pub struct GcsArchiver {
    target: ArchiveTarget,
    endpoint: String,
    client: reqwest::Client,
}

impl GcsArchiver {
    pub fn new(target: ArchiveTarget, endpoint: Option<String>) -> Self {
        Self {
            target,
            endpoint: endpoint
                .map(|e| e.trim_end_matches('/').to_string())
                .filter(|e| !e.is_empty())
                .unwrap_or_else(|| GCS_DEFAULT_ENDPOINT.to_string()),
            client: reqwest::Client::new(),
        }
    }

    /// `<prefix>/<tier>/<file-name>`.
    pub fn object_name(&self, tier: StoreTier, file: &Path) -> String {
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("segment");
        if self.target.prefix.is_empty() {
            format!("{}/{name}", tier.as_str())
        } else {
            format!("{}/{}/{name}", self.target.prefix, tier.as_str())
        }
    }

    async fn token(&self) -> Result<Option<String>, String> {
        if self.endpoint != GCS_DEFAULT_ENDPOINT {
            return Ok(None);
        }
        let r = self
            .client
            .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
            .header("Metadata-Flavor", "Google")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("metadata token: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("metadata token: HTTP {}", r.status()));
        }
        let v: serde_json::Value = r.json().await.map_err(|e| format!("metadata token: {e}"))?;
        v.get("access_token")
            .and_then(|t| t.as_str())
            .map(|t| Some(t.to_string()))
            .ok_or_else(|| "metadata token: no access_token".to_string())
    }
}

#[async_trait::async_trait]
impl SegmentArchiver for GcsArchiver {
    async fn archive(&self, tier: StoreTier, file: &Path) -> Result<u64, String> {
        // ⚠ Whole-file read: a sealed segment is ~256 MiB (the oldest prod
        // segment is ~1 GiB). One at a time, once per pass; the writer's limit
        // is 12 GiB. Streaming would need reqwest's `stream` feature.
        let bytes = tokio::fs::read(file)
            .await
            .map_err(|e| format!("read {}: {e}", file.display()))?;
        let object = self.object_name(tier, file);
        let url = format!(
            "{}/upload/storage/v1/b/{}/o",
            self.endpoint, self.target.bucket
        );
        let mut req = self
            .client
            .post(&url)
            .query(&[("uploadType", "media"), ("name", object.as_str())])
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .timeout(std::time::Duration::from_secs(600))
            .body(bytes);
        if let Some(t) = self.token().await? {
            req = req.bearer_auth(t);
        }
        let r = req
            .send()
            .await
            .map_err(|e| format!("upload {object}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("upload {object}: HTTP {}", r.status()));
        }
        let v: serde_json::Value = r
            .json()
            .await
            .map_err(|e| format!("upload {object}: {e}"))?;
        // GCS reports `size` as a decimal string (uint64); accept a number too.
        match v.get("size") {
            Some(serde_json::Value::String(s)) => s.parse::<u64>().ok(),
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            _ => None,
        }
        .ok_or_else(|| format!("upload {object}: reply has no parseable size"))
    }
}

/// Parse the retention window. `None` = refused (below the minimum, or
/// unparsable); the caller must then not run the pass at all.
pub fn parse_retention_hours(raw: Option<&str>) -> Option<u64> {
    let h = match raw {
        None => TIER_RETENTION_HOURS_DEFAULT,
        Some(v) => v.trim().parse::<u64>().ok()?,
    };
    // ⚠ The caller turns this into `h * 3600` seconds. Without the overflow
    // check, a 16-digit value wraps (release builds do not trap) to a window of
    // minutes and slips under the 24 h floor.
    (h >= TIER_RETENTION_HOURS_MIN && h.checked_mul(3600).is_some()).then_some(h)
}

/// The tiers retention may ever touch. Catalog needs full history (its fold
/// reads from sequence 1); kv/object parity reads the oldest records. Code, not
/// configuration, so no env value can widen it.
pub const PRUNABLE_TIERS: &[StoreTier] = &[StoreTier::EventLog, StoreTier::Projection];

/// One sealed segment's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruneVerdict {
    /// Every rule held; a deleting build would remove it.
    WouldPrune,
    /// Kept, and why.
    Keep(&'static str),
    /// Not considered: an older segment of the same tier was kept, and pruning
    /// is strictly oldest-first.
    BehindAKeptSegment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneDecision {
    pub segment: PathBuf,
    pub bytes: u64,
    pub verdict: PruneVerdict,
}

/// Execution ids appearing anywhere in the ACTIVE segment, by streaming it.
/// `None` = a line whose subject cannot be read: treat as unknown, prune nothing.
fn active_execution_ids(path: &Path) -> Option<std::collections::HashSet<String>> {
    execution_ids_since(path, 0)
}

/// Execution ids in `path` from byte `offset` on. `offset` must be a line
/// boundary — the enabled pass takes it under the store lock, where no append
/// is mid-write. `None` = unreadable: treat as unknown, prune nothing.
fn execution_ids_since(path: &Path, offset: u64) -> Option<std::collections::HashSet<String>> {
    use std::io::{BufRead, Seek, SeekFrom};
    let mut out = std::collections::HashSet::new();
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(out),
        Err(_) => return None,
    };
    f.seek(SeekFrom::Start(offset)).ok()?;
    for line in std::io::BufReader::with_capacity(1 << 20, f).lines() {
        let line = line.ok()?;
        if line.trim().is_empty() {
            continue;
        }
        out.insert(execution_id_from_line(&line)?);
    }
    Some(out)
}

/// Plan a retention pass for one tier. Reads only; changes nothing.
///
/// A sealed segment is `WouldPrune` only if ALL hold:
/// 1. the tier is in [`PRUNABLE_TIERS`];
/// 2. it is older than `retention` (its mtime is its seal time: a sealed file
///    is never written again, and `rename` preserves mtime);
/// 3. it has an `.idx` (unknown contents are kept);
/// 4. none of its executions appears in a segment sealed WITHIN `retention`,
///    nor in the active segment. Whole executions are kept together, so a run
///    whose records straddle a seal pins the older segment until the younger one
///    ages out too — and a long-lived ACTIVE segment (a tier that rarely reaches
///    the size seal) pins every older segment holding any of its executions;
/// 5. no read currently holds a lease on it;
/// 6. every OLDER segment of the tier is also `WouldPrune` (oldest-first);
/// 7. it is not the NEWEST sealed segment. `maybe_seal` names the next segment
///    `<highest surviving number> + 1`, so unlinking the newest would make the
///    next seal REUSE its name: a leftover `.idx`/`.offidx` (a crash between the
///    segment's unlink and its sidecars') would then describe the wrong file, so
///    reads would skip records that index does not list, and an archive object
///    of that name would be overwritten.
pub fn plan_tier_prune(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    retention: std::time::Duration,
    now: std::time::SystemTime,
) -> Vec<PruneDecision> {
    let segs = sealed_segments(cfg, tier);
    let bytes = |p: &Path| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    if !PRUNABLE_TIERS.contains(&tier) {
        return segs
            .into_iter()
            .map(|s| PruneDecision {
                bytes: bytes(&s),
                segment: s,
                verdict: PruneVerdict::Keep("tier is never pruned"),
            })
            .collect();
    }
    let age = |p: &Path| -> Option<std::time::Duration> {
        let m = std::fs::metadata(p).ok()?.modified().ok()?;
        now.duration_since(m).ok()
    };
    let is_old = |p: &Path| age(p).is_some_and(|a| a >= retention);

    // Execution ids still "hot": in any recently sealed segment or the active one.
    let mut hot: Option<std::collections::HashSet<String>> =
        active_execution_ids(&cfg.path_for(tier));
    for s in segs.iter().filter(|s| !is_old(s)) {
        match (hot.as_mut(), load_segment_index(s)) {
            (Some(h), Some(ids)) => h.extend(ids),
            // A recent segment with no index: its ids are unknown, so nothing
            // older can be proven unpinned.
            _ => hot = None,
        }
    }

    let mut out = Vec::with_capacity(segs.len());
    let mut stopped = false;
    let newest = segs.last().cloned();
    for s in segs {
        let verdict = if stopped {
            PruneVerdict::BehindAKeptSegment
        } else if !is_old(&s) {
            PruneVerdict::Keep("younger than the retention window")
        } else if Some(&s) == newest.as_ref() {
            PruneVerdict::Keep("newest sealed segment: kept so a later seal never reuses its name")
        } else {
            match (load_segment_index(&s), hot.as_ref()) {
                (None, _) => PruneVerdict::Keep("no .idx: contents unknown"),
                (_, None) => PruneVerdict::Keep(
                    "hot set unknown (unreadable active line or unindexed recent segment)",
                ),
                (Some(ids), Some(h)) if ids.iter().any(|id| h.contains(id)) => {
                    PruneVerdict::Keep("pinned: an execution in it also appears in the active segment or a segment younger than the window")
                }
                _ if lease_count(&s) > 0 => PruneVerdict::Keep("leased by a read in flight"),
                _ => PruneVerdict::WouldPrune,
            }
        };
        if !matches!(verdict, PruneVerdict::WouldPrune) {
            stopped = true;
        }
        out.push(PruneDecision {
            bytes: bytes(&s),
            segment: s,
            verdict,
        });
    }
    out
}

/// `NOETL_EHDB_TIER_PRUNE_INTERVAL_SECS`. Default 3600; floor 60.
pub const TIER_PRUNE_INTERVAL_SECS_ENV: &str = "NOETL_EHDB_TIER_PRUNE_INTERVAL_SECS";

/// Start the retention pass if configured. A no-op unless
/// `NOETL_EHDB_TIER_PRUNE_MODE=dry_run` and the retention window is valid, so
/// shipping this changes nothing on a writer that does not opt in.
pub fn spawn_retention_pass(cfg: &TierStoreConfig) {
    let mode = parse_prune_mode(std::env::var(TIER_PRUNE_MODE_ENV).ok().as_deref());
    if mode == PruneMode::Off {
        return;
    }
    let raw = std::env::var(TIER_RETENTION_HOURS_ENV).ok();
    let Some(hours) = parse_retention_hours(raw.as_deref()) else {
        tracing::error!(
            env = TIER_RETENTION_HOURS_ENV,
            value = raw.as_deref().unwrap_or(""),
            min = TIER_RETENTION_HOURS_MIN,
            "EHDB tier retention window refused (unparsable or below the minimum); \
             the retention pass will NOT run"
        );
        return;
    };
    let interval = std::env::var(TIER_PRUNE_INTERVAL_SECS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(3600)
        .max(60);
    let retention = std::time::Duration::from_secs(hours * 3600);
    let cfg = cfg.clone();
    if mode == PruneMode::Enabled {
        let raw_archive = std::env::var(TIER_PRUNE_ARCHIVE_ENV).ok();
        match parse_archive_target(raw_archive.as_deref()) {
            Ok(target) => {
                let archiver = target.clone().map(|t| {
                    GcsArchiver::new(t, std::env::var(TIER_PRUNE_ARCHIVE_ENDPOINT_ENV).ok())
                });
                tracing::warn!(
                    retention_hours = hours,
                    interval_secs = interval,
                    archive = ?target,
                    "EHDB tier retention: ENABLED — unlinking the WouldPrune prefix \
                     (noetl/ai-meta#491)"
                );
                tokio::spawn(async move {
                    loop {
                        let a = archiver.as_ref().map(|a| a as &dyn SegmentArchiver);
                        let _ = run_prune_enabled(&cfg, retention, a).await;
                        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                    }
                });
                return;
            }
            Err(e) => {
                // Asked for an archive and could not have one: deleting now would
                // lose data the operator meant to keep. Fall back to the dry run.
                tracing::error!(
                    error = %e,
                    "EHDB tier retention: archive target invalid; refusing to delete, \
                     running the DRY RUN instead"
                );
            }
        }
    }
    tracing::info!(
        retention_hours = hours,
        interval_secs = interval,
        "EHDB tier retention: DRY RUN enabled — logs what would be pruned, deletes nothing"
    );
    tokio::spawn(async move {
        loop {
            let c = cfg.clone();
            // Streaming the active segment and reading indexes is file I/O: off
            // the runtime, like every other store entry point.
            let _ = tokio::task::spawn_blocking(move || run_prune_dry_run(&c, retention)).await;
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }
    });
}

/// Log a dry-run pass over every tier. Deletes nothing.
pub fn run_prune_dry_run(cfg: &TierStoreConfig, retention: std::time::Duration) {
    let now = std::time::SystemTime::now();
    for &tier in StoreTier::ALL.iter() {
        let plan = plan_tier_prune(cfg, tier, retention, now);
        if plan.is_empty() {
            continue;
        }
        let (mut n, mut b) = (0usize, 0u64);
        for d in &plan {
            let verdict = match &d.verdict {
                PruneVerdict::WouldPrune => {
                    n += 1;
                    b += d.bytes;
                    "would_prune".to_string()
                }
                PruneVerdict::Keep(why) => format!("keep: {why}"),
                PruneVerdict::BehindAKeptSegment => "keep: behind a kept segment".to_string(),
            };
            tracing::info!(
                tier = tier.as_str(),
                segment = %d.segment.display(),
                bytes = d.bytes,
                verdict,
                "EHDB tier retention DRY RUN (nothing deleted)"
            );
        }
        tracing::info!(
            tier = tier.as_str(),
            would_prune_segments = n,
            would_prune_bytes = b,
            retention_hours = retention.as_secs() / 3600,
            "EHDB tier retention DRY RUN summary (nothing deleted; noetl/ai-meta#491)"
        );
    }
}

/// What an enabled pass did to one tier.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PruneReport {
    pub pruned: Vec<PathBuf>,
    pub pruned_bytes: u64,
    /// Why the pass stopped before the end of its plan, if it did.
    pub stopped: Option<String>,
}

/// One ENABLED retention pass over every prunable tier.
pub async fn run_prune_enabled(
    cfg: &TierStoreConfig,
    retention: std::time::Duration,
    archiver: Option<&dyn SegmentArchiver>,
) -> Vec<(StoreTier, PruneReport)> {
    let mut out = Vec::new();
    for &tier in PRUNABLE_TIERS {
        let r = prune_tier_enabled(cfg, tier, retention, archiver).await;
        tracing::info!(
            tier = tier.as_str(),
            pruned_segments = r.pruned.len(),
            pruned_bytes = r.pruned_bytes,
            stopped = r.stopped.as_deref().unwrap_or(""),
            retention_hours = retention.as_secs() / 3600,
            archived = archiver.is_some(),
            "EHDB tier retention ENABLED summary (noetl/ai-meta#491)"
        );
        out.push((tier, r));
    }
    out
}

/// Prune one tier: plan, archive, then unlink under the write lock.
///
/// ⚠⚠ Three phases, and the lock is held for the LAST one only.
///
/// 1. **Snapshot + plan.** The active segment's length and the sealed list are
///    read under a brief SHARED guard (so the length is a line boundary), then
///    [`plan_tier_prune`] runs with no lock, exactly as the dry run does.
/// 2. **Archive** (when configured), with no lock: sealed segments are
///    immutable, and holding the write lock across a 256 MiB upload would queue
///    every append behind it — the noetl/ai-meta#490 starvation shape. A failed
///    or size-mismatched upload stops the pass; that segment and everything
///    newer stay on disk.
/// 3. **Re-validate + unlink under the WRITE lock.** Only what could have
///    changed since phase 1 is re-checked, so the hold is short: each segment
///    still exists, is not leased, and none of its executions appears in the
///    records written since the snapshot (the active tail, plus any segment
///    sealed in between). Then the segment is unlinked, then its sidecars.
async fn prune_tier_enabled(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    retention: std::time::Duration,
    archiver: Option<&dyn SegmentArchiver>,
) -> PruneReport {
    use crate::ehdb::metrics::record_tier_prune;
    let mut report = PruneReport::default();
    let lock = store_lock(cfg, tier);
    let active = cfg.path_for(tier);

    // ---- phase 1: snapshot (shared guard) + plan (no lock)
    let (active_len0, sealed0) = {
        let _r = lock.read().await;
        let len = std::fs::metadata(&active).map(|m| m.len()).unwrap_or(0);
        (len, sealed_segments(cfg, tier))
    };
    let plan = {
        let c = cfg.clone();
        match off_runtime(move || {
            plan_tier_prune(&c, tier, retention, std::time::SystemTime::now())
        })
        .await
        {
            Ok(p) => p,
            Err(e) => {
                report.stopped = Some(format!("plan failed: {e}"));
                return report;
            }
        }
    };
    let candidates: Vec<PathBuf> = plan
        .into_iter()
        .take_while(|d| d.verdict == PruneVerdict::WouldPrune)
        .map(|d| d.segment)
        .collect();
    if candidates.is_empty() {
        return report;
    }

    // ---- phase 2: archive, no lock
    let mut ready: Vec<PathBuf> = Vec::new();
    'segments: for seg in candidates {
        if let Some(a) = archiver {
            for file in [
                seg.clone(),
                segment_index_path(&seg),
                segment_offset_index_path(&seg),
            ] {
                let Ok(local) = std::fs::metadata(&file).map(|m| m.len()) else {
                    if file == seg {
                        report.stopped = Some(format!("{} vanished before archive", seg.display()));
                        break 'segments;
                    }
                    continue; // a missing sidecar is not data
                };
                let why = match a.archive(tier, &file).await {
                    Ok(remote) if remote == local => None,
                    Ok(remote) => Some(format!(
                        "archive size mismatch for {}: local {local} remote {remote}",
                        file.display()
                    )),
                    Err(e) => Some(format!("archive failed for {}: {e}", file.display())),
                };
                if let Some(why) = why {
                    tracing::error!(tier = tier.as_str(), segment = %seg.display(), error = %why,
                        "EHDB tier retention: archive failed; NOT unlinking, pass stopped");
                    record_tier_prune(tier, "archive_failed", 0);
                    report.stopped = Some(why);
                    break 'segments;
                }
            }
        }
        ready.push(seg);
    }

    #[cfg(test)]
    tests::probe_before_unlink(cfg);

    // ---- phase 3: re-validate + unlink under the WRITE lock
    // ⚠ The guard lives in this async frame; the tail scan and the unlinks run
    // on the blocking pool (`off_runtime`), like every other store entry point.
    // A seal between the snapshot and here makes the scan cover the WHOLE
    // active file plus the new segment, which must not occupy one of the
    // writer's two runtime threads (the 2026-09-22 outage).
    let _w = lock.write().await;
    let c = cfg.clone();
    let archived = archiver.is_some();
    match off_runtime(move || {
        unlink_revalidated_locked(&c, tier, active_len0, &sealed0, ready, archived, report)
    })
    .await
    {
        Ok(r) => r,
        Err(e) => PruneReport {
            stopped: Some(e),
            ..Default::default()
        },
    }
}

/// Phase 3 of [`prune_tier_enabled`]. Caller holds the store's WRITE lock.
fn unlink_revalidated_locked(
    cfg: &TierStoreConfig,
    tier: StoreTier,
    active_len0: u64,
    sealed0: &[PathBuf],
    ready: Vec<PathBuf>,
    archived: bool,
    mut report: PruneReport,
) -> PruneReport {
    use crate::ehdb::metrics::record_tier_prune;
    let active = cfg.path_for(tier);
    let sealed_now = sealed_segments(cfg, tier);
    let sealed_since: Vec<PathBuf> = sealed_now
        .iter()
        .filter(|p| !sealed0.contains(p))
        .cloned()
        .collect();
    // Records written since the snapshot: a segment sealed in between holds the
    // whole former active file; otherwise only the active tail is new.
    let mut fresh: Option<std::collections::HashSet<String>> = Some(Default::default());
    for p in &sealed_since {
        match (
            fresh.as_mut(),
            load_segment_index(p).or_else(|| execution_ids_since(p, 0)),
        ) {
            (Some(f), Some(ids)) => f.extend(ids),
            _ => fresh = None,
        }
    }
    let from = if sealed_since.is_empty() {
        active_len0
    } else {
        0
    };
    match (fresh.as_mut(), execution_ids_since(&active, from)) {
        (Some(f), Some(ids)) => f.extend(ids),
        _ => fresh = None,
    }
    let Some(fresh) = fresh else {
        record_tier_prune(tier, "skipped_revalidation", 0);
        report.stopped = Some("records written since the plan are unreadable".into());
        return report;
    };
    for seg in ready {
        let skip = if !seg.exists() {
            Some("segment vanished")
        } else if lease_count(&seg) > 0 {
            Some("leased by a read in flight")
        } else {
            match load_segment_index(&seg) {
                None => Some("no .idx at unlink time"),
                Some(ids) if ids.iter().any(|id| fresh.contains(id)) => {
                    Some("an execution in it wrote again since the plan")
                }
                Some(_) => None,
            }
        };
        if let Some(why) = skip {
            record_tier_prune(tier, "skipped_revalidation", 0);
            report.stopped = Some(format!("{}: {why}", seg.display()));
            break;
        }
        let bytes = std::fs::metadata(&seg).map(|m| m.len()).unwrap_or(0);
        if let Err(e) = std::fs::remove_file(&seg) {
            tracing::error!(tier = tier.as_str(), segment = %seg.display(), error = %e,
                "EHDB tier retention: unlink failed; pass stopped");
            record_tier_prune(tier, "failed", 0);
            report.stopped = Some(format!("unlink {}: {e}", seg.display()));
            break;
        }
        ehdb_reference::forget_runtime(&seg);
        // The segment goes first: a sidecar left behind is harmless (nothing
        // reads an index without its segment, and the planner never prunes the
        // newest segment, so no later seal can reuse this name and inherit it);
        // a segment without its index is merely kept by the next plan.
        for side in [segment_index_path(&seg), segment_offset_index_path(&seg)] {
            if let Err(e) = std::fs::remove_file(&side) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(file = %side.display(), error = %e,
                        "EHDB tier retention: sidecar not removed");
                }
            }
        }
        tracing::info!(tier = tier.as_str(), segment = %seg.display(), bytes,
            archived, "EHDB tier retention: segment pruned");
        record_tier_prune(tier, "pruned", bytes);
        report.pruned_bytes += bytes;
        report.pruned.push(seg);
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    // ---- tier seal (noetl/ai-meta#332 writer OOM) --------------------------

    fn seal_cfg(tag: &str) -> TierStoreConfig {
        let mut d = std::env::temp_dir();
        d.push(format!(
            "ehdb-seal-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        TierStoreConfig { dir: d }
    }

    fn read_count(body: &str) -> (usize, usize) {
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        (
            v["records"].as_array().unwrap().len(),
            v["record_count"].as_u64().unwrap() as usize,
        )
    }

    /// ⭐ noetl/ai-meta#492: a long execution comes back WHOLE, sealed or not.
    #[tokio::test]
    async fn a_long_execution_is_not_truncated_at_the_scan_cap() {
        const N: usize = MAX_SCAN_LIMIT + 200;
        let payloads: Vec<String> = (0..N).map(|i| format!("{{\"i\":{i}}}")).collect();

        // One segment (the driver's own path).
        let cfg = seal_cfg("long-active");
        for chunk in payloads.chunks(100) {
            let _ = append_batch(Some(&cfg), StoreTier::EventLog, "exec-long", chunk).await;
        }
        let TierStoreOutcome::Ok(b) =
            read_execution(Some(&cfg), StoreTier::EventLog, "exec-long").await
        else {
            panic!("read failed")
        };
        assert_eq!(read_count(&b), (N, N), "unsealed read truncated");
        let _ = std::fs::remove_dir_all(&cfg.dir);

        // Sealed: every record in ONE sealed segment, then a fresh active one —
        // the merged path, which also reported the cut as the total.
        let cfg = seal_cfg("long-sealed");
        for chunk in payloads.chunks(100) {
            let _ = append_batch(Some(&cfg), StoreTier::EventLog, "exec-long", chunk).await;
        }
        let active = cfg.path_for(StoreTier::EventLog);
        std::fs::rename(
            &active,
            active.with_file_name(format!(
                "{}.1",
                active.file_name().unwrap().to_str().unwrap()
            )),
        )
        .unwrap();
        ehdb_reference::forget_runtime(&active);
        let _ = append(Some(&cfg), StoreTier::EventLog, "exec-other", "{}").await;
        assert_eq!(sealed_segments(&cfg, StoreTier::EventLog).len(), 1);
        let TierStoreOutcome::Ok(b) =
            read_execution(Some(&cfg), StoreTier::EventLog, "exec-long").await
        else {
            panic!("merged read failed")
        };
        assert_eq!(read_count(&b), (N, N), "merged read truncated");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Sealed-phase probes, keyed by store dir: `0` not reached, `1` the store
    /// lock was free when the sealed segments were read, `2` it was held.
    /// Keyed so a parallel test's reads cannot answer for this one.
    fn sealed_probes() -> &'static StdMutex<HashMap<PathBuf, Arc<std::sync::atomic::AtomicU8>>> {
        static P: OnceLock<StdMutex<HashMap<PathBuf, Arc<std::sync::atomic::AtomicU8>>>> =
            OnceLock::new();
        P.get_or_init(|| StdMutex::new(HashMap::new()))
    }

    /// Lowest lease count seen on any segment a scan's sealed phase read, keyed
    /// by store dir. `usize::MAX` until the phase runs.
    fn scan_lease_probes() -> &'static StdMutex<HashMap<PathBuf, usize>> {
        static P: OnceLock<StdMutex<HashMap<PathBuf, usize>>> = OnceLock::new();
        P.get_or_init(|| StdMutex::new(HashMap::new()))
    }

    pub(super) fn probe_scan_leases(cfg: &TierStoreConfig, fresh: &[PathBuf]) {
        let mut g = scan_lease_probes().lock().unwrap();
        if let Some(min) = g.get_mut(&cfg.dir) {
            for p in fresh {
                *min = (*min).min(lease_count(p));
            }
        }
    }

    /// noetl/ai-meta#491: every sealed segment a scan reads unlocked is LEASED
    /// while it reads it, so a prune cannot unlink it underneath the scan.
    #[tokio::test]
    async fn a_scan_holds_a_lease_on_every_sealed_segment_it_reads() {
        let cfg = multi_page_segment_fixture("scan-lease", 20, 3).await;
        scan_lease_probes()
            .lock()
            .unwrap()
            .insert(cfg.dir.clone(), usize::MAX);
        let (total, _) = scan_page(&scan(Some(&cfg), EL, None, MAX_SCAN_LIMIT).await);
        let min = scan_lease_probes()
            .lock()
            .unwrap()
            .remove(&cfg.dir)
            .unwrap();
        assert_eq!(total, 23);
        assert_ne!(min, usize::MAX, "the sealed phase never ran — vacuous");
        assert!(
            min >= 1,
            "a sealed segment was read with NO lease held (count {min})"
        );
        let sealed = sealed_segments(&cfg, EL);
        assert!(
            sealed.iter().all(|p| lease_count(p) == 0),
            "the scan leaked its leases past its return"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    pub(super) fn probe_sealed_phase(cfg: &TierStoreConfig, tier: StoreTier) {
        let probe = sealed_probes().lock().unwrap().get(&cfg.dir).cloned();
        if let Some(p) = probe {
            let free = store_lock(cfg, tier).try_write().is_ok();
            p.store(
                if free { 1 } else { 2 },
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        let lease_probe = lease_probes().lock().unwrap().get(&cfg.dir).cloned();
        if let Some(p) = lease_probe {
            let leased = sealed_segments(cfg, tier)
                .iter()
                .map(|s| lease_count(s))
                .min()
                .unwrap_or(0);
            p.store(leased, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Lease probes, keyed by store dir: the MINIMUM lease count over the
    /// store's sealed segments, observed at the start of the sealed phase.
    fn lease_probes() -> &'static StdMutex<HashMap<PathBuf, Arc<std::sync::atomic::AtomicUsize>>> {
        static P: OnceLock<StdMutex<HashMap<PathBuf, Arc<std::sync::atomic::AtomicUsize>>>> =
            OnceLock::new();
        P.get_or_init(|| StdMutex::new(HashMap::new()))
    }

    // ---- noetl/ai-meta#491 follow-up: the ENABLED pass + archive ----

    /// What `probe_before_unlink` does for a store dir: lease these segments
    /// and/or append for these executions, between the plan and the unlink.
    #[derive(Default)]
    struct RaceAction {
        lease: Vec<PathBuf>,
        append_exec: Vec<String>,
    }
    fn race_actions() -> &'static StdMutex<HashMap<PathBuf, RaceAction>> {
        static P: OnceLock<StdMutex<HashMap<PathBuf, RaceAction>>> = OnceLock::new();
        P.get_or_init(|| StdMutex::new(HashMap::new()))
    }
    fn held_leases() -> &'static StdMutex<Vec<SegmentLease>> {
        static L: OnceLock<StdMutex<Vec<SegmentLease>>> = OnceLock::new();
        L.get_or_init(|| StdMutex::new(Vec::new()))
    }
    pub(super) fn probe_before_unlink(cfg: &TierStoreConfig) {
        let Some(a) = race_actions().lock().unwrap().remove(&cfg.dir) else {
            return;
        };
        if !a.lease.is_empty() {
            held_leases()
                .lock()
                .unwrap()
                .push(SegmentLease::acquire(&a.lease));
        }
        for exec in a.append_exec {
            // A raw line in the driver's shape, appended directly: the probe
            // runs on the async path and must not take the store lock.
            let line = format!(
                "{{\"sequence\":999999,\"transaction_id\":\"race\",\"tenant\":\"{t}\",\"namespace\":\"{n}\",\"mutations\":[{{\"Stream\":{{\"Publish\":{{\"stream\":\"{s}\",\"subject\":\"{p}.{exec}\",\"payload\":[123,125]}}}}}}]}}\n",
                t = DEFAULT_LOCAL_REFERENCE_TENANT,
                n = DEFAULT_LOCAL_REFERENCE_NAMESPACE,
                s = ehdb_reference::EVENT_LOG_STREAM,
                p = ehdb_reference::EVENT_LOG_SUBJECT_PREFIX,
            );
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(cfg.path_for(StoreTier::EventLog))
                .unwrap();
            std::io::Write::write_all(&mut f, line.as_bytes()).unwrap();
        }
    }

    const R49: std::time::Duration = std::time::Duration::from_secs(49 * 3600);

    /// s1 old+finished, s2 old but PINNED (exec-b also in recent s3), s3 recent,
    /// exec-live in the ACTIVE segment. The prunable prefix is exactly [s1].
    async fn enabled_fixture(tag: &str) -> (TierStoreConfig, PathBuf, PathBuf, PathBuf) {
        let cfg = seal_cfg(tag);
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 4).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 4).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 2).await;
        put(&cfg, t, "exec-c", 2).await;
        let s3 = seal_now(&cfg, t);
        put(&cfg, t, "exec-live", 3).await;
        for p in [&s1, &s2] {
            age(p, 60);
        }
        (cfg, s1, s2, s3)
    }

    fn sidecars(seg: &Path) -> [PathBuf; 2] {
        [segment_index_path(seg), segment_offset_index_path(seg)]
    }

    /// A stub GCS JSON-API upload endpoint. `mode`: "ok" | "fail" | "short".
    async fn stub_gcs(mode: &'static str) -> (String, Arc<StdMutex<Vec<(String, usize)>>>) {
        use axum::{extract::Query, routing::post, Router};
        let seen: Arc<StdMutex<Vec<(String, usize)>>> = Arc::default();
        let s2 = seen.clone();
        let app = Router::new().route(
            "/upload/storage/v1/b/{bucket}/o",
            post(
                move |Query(q): Query<HashMap<String, String>>, body: axum::body::Bytes| {
                    let seen = s2.clone();
                    async move {
                        let name = q.get("name").cloned().unwrap_or_default();
                        seen.lock().unwrap().push((name, body.len()));
                        match mode {
                            "fail" => (
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                "boom".to_string(),
                            ),
                            "short" => (
                                axum::http::StatusCode::OK,
                                format!("{{\"size\":\"{}\"}}", body.len() + 1),
                            ),
                            _ => (
                                axum::http::StatusCode::OK,
                                format!("{{\"size\":\"{}\"}}", body.len()),
                            ),
                        }
                    }
                },
            ),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn stub_archiver(endpoint: &str) -> GcsArchiver {
        GcsArchiver::new(
            parse_archive_target(Some("gs://noetl-archive/tier"))
                .unwrap()
                .unwrap(),
            Some(endpoint.to_string()),
        )
    }

    /// ⭐ Enabled unlinks EXACTLY the WouldPrune prefix — the segment and its
    /// sidecars — and nothing else: the pinned, the recent and the active
    /// segment survive.
    #[tokio::test]
    async fn enabled_unlinks_exactly_the_would_prune_prefix() {
        let (cfg, s1, s2, s3) = enabled_fixture("en-prefix").await;
        let r = run_prune_enabled(&cfg, R49, None).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert_eq!(el.pruned, vec![s1.clone()], "pruned the wrong set: {el:?}");
        assert!(!s1.exists(), "the prunable segment is still there");
        for side in sidecars(&s1) {
            assert!(
                !side.exists(),
                "sidecar {} survived its segment",
                side.display()
            );
        }
        for keep in [&s2, &s3, &cfg.path_for(StoreTier::EventLog)] {
            assert!(
                keep.exists(),
                "{} was unlinked but is not in the prefix",
                keep.display()
            );
        }
        assert!(
            sidecars(&s2).iter().all(|p| p.exists()),
            "a kept segment lost its index"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// The dry run, run over the same store, deletes nothing.
    #[tokio::test]
    async fn the_dry_run_deletes_nothing_on_an_enabled_shaped_store() {
        let (cfg, s1, s2, s3) = enabled_fixture("en-dry").await;
        run_prune_dry_run(&cfg, R49);
        for p in [&s1, &s2, &s3] {
            assert!(p.exists(), "dry run removed {}", p.display());
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[test]
    fn only_the_exact_word_enabled_arms_deletion() {
        assert_eq!(parse_prune_mode(Some("enabled")), PruneMode::Enabled);
        for casual in [
            "on",
            "true",
            "1",
            "yes",
            "enable",
            "ENABLED-ish",
            // Exact word only: no case folding, no trimming.
            "ENABLED",
            "Enabled",
            " enabled",
            "enabled ",
            "enabled\n",
        ] {
            assert_eq!(
                parse_prune_mode(Some(casual)),
                PruneMode::Off,
                "{casual:?} armed deletion"
            );
        }
        assert_eq!(
            parse_retention_hours(None),
            Some(49),
            "default window is 49 h"
        );
    }

    #[test]
    fn the_archive_target_is_off_unless_a_gs_url_is_given() {
        assert_eq!(parse_archive_target(None), Ok(None));
        assert_eq!(parse_archive_target(Some("  ")), Ok(None));
        assert_eq!(
            parse_archive_target(Some("gs://b/x/y/")),
            Ok(Some(ArchiveTarget {
                bucket: "b".into(),
                prefix: "x/y".into()
            }))
        );
        assert_eq!(
            parse_archive_target(Some("gs://b")),
            Ok(Some(ArchiveTarget {
                bucket: "b".into(),
                prefix: String::new()
            }))
        );
        assert!(parse_archive_target(Some("s3://b/x")).is_err());
        assert!(parse_archive_target(Some("gs:///x")).is_err());
    }

    /// ⭐ Archive on: every file of the segment is uploaded (named
    /// `<prefix>/<tier>/<file>`) BEFORE it is unlinked.
    #[tokio::test]
    async fn with_an_archive_the_segment_is_uploaded_then_unlinked() {
        let (cfg, s1, s2, _s3) = enabled_fixture("en-arch-ok").await;
        let (ep, seen) = stub_gcs("ok").await;
        let a = stub_archiver(&ep);
        let r = run_prune_enabled(&cfg, R49, Some(&a)).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert_eq!(el.pruned, vec![s1.clone()]);
        let names: Vec<String> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        let f1 = s1.file_name().unwrap().to_str().unwrap();
        for want in [
            format!("tier/eventlog/{f1}"),
            format!("tier/eventlog/{f1}.idx"),
            format!("tier/eventlog/{f1}.offidx"),
        ] {
            assert!(names.contains(&want), "missing upload {want}: {names:?}");
        }
        assert!(
            !names
                .iter()
                .any(|n| n.contains(s2.file_name().unwrap().to_str().unwrap())),
            "a kept segment was archived: {names:?}"
        );
        assert!(!s1.exists() && s2.exists());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠⚠ A failed upload: NOTHING is unlinked.
    #[tokio::test]
    async fn an_archive_failure_keeps_the_segment() {
        let (cfg, s1, _s2, _s3) = enabled_fixture("en-arch-fail").await;
        let (ep, seen) = stub_gcs("fail").await;
        let r = run_prune_enabled(&cfg, R49, Some(&stub_archiver(&ep))).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert!(
            el.pruned.is_empty(),
            "unlinked despite a failed archive: {el:?}"
        );
        assert!(s1.exists() && sidecars(&s1).iter().all(|p| p.exists()));
        assert!(
            !seen.lock().unwrap().is_empty(),
            "the stub was never called — vacuous"
        );
        assert!(el
            .stopped
            .as_deref()
            .unwrap_or("")
            .contains("archive failed"));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠⚠ The store reports a different size than we sent: NOT unlinked.
    #[tokio::test]
    async fn an_archive_size_mismatch_keeps_the_segment() {
        let (cfg, s1, _s2, _s3) = enabled_fixture("en-arch-short").await;
        let (ep, _seen) = stub_gcs("short").await;
        let r = run_prune_enabled(&cfg, R49, Some(&stub_archiver(&ep))).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert!(
            el.pruned.is_empty(),
            "unlinked despite a size mismatch: {el:?}"
        );
        assert!(s1.exists());
        assert!(el
            .stopped
            .as_deref()
            .unwrap_or("")
            .contains("size mismatch"));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// A lease taken AFTER the plan and before the unlink keeps the segment.
    #[tokio::test]
    async fn a_lease_taken_after_the_plan_keeps_the_segment() {
        let (cfg, s1, _s2, _s3) = enabled_fixture("en-lease").await;
        race_actions().lock().unwrap().insert(
            cfg.dir.clone(),
            RaceAction {
                lease: vec![s1.clone()],
                ..Default::default()
            },
        );
        let r = run_prune_enabled(&cfg, R49, None).await;
        held_leases().lock().unwrap().clear();
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert!(el.pruned.is_empty(), "unlinked a leased segment: {el:?}");
        assert!(s1.exists());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// An execution that writes again AFTER the plan pins its old segment.
    #[tokio::test]
    async fn a_write_after_the_plan_keeps_that_executions_segment() {
        let (cfg, s1, _s2, _s3) = enabled_fixture("en-race-write").await;
        race_actions().lock().unwrap().insert(
            cfg.dir.clone(),
            RaceAction {
                append_exec: vec!["exec-a".into()],
                ..Default::default()
            },
        );
        let r = run_prune_enabled(&cfg, R49, None).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert!(
            el.pruned.is_empty(),
            "unlinked a segment whose execution wrote again: {el:?}"
        );
        assert!(s1.exists());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠⚠ The NEWEST sealed segment is never unlinked, however old. `maybe_seal`
    /// numbers the next segment from the highest surviving one, so pruning the
    /// newest made the next seal reuse its name: a sidecar left by a crash
    /// mid-unlink then indexed the WRONG file (reads skip what it does not
    /// list), and its archive object was overwritten.
    #[tokio::test]
    async fn the_newest_sealed_segment_is_kept_so_a_seal_never_reuses_its_name() {
        let cfg = seal_cfg("en-newest");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 3).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-live", 2).await;
        age(&s1, 400);
        age(&s2, 300);
        let r = run_prune_enabled(&cfg, R49, None).await;
        let el = &r.iter().find(|(t, _)| *t == StoreTier::EventLog).unwrap().1;
        assert_eq!(el.pruned, vec![s1.clone()], "pruned the wrong set: {el:?}");
        assert!(
            s2.exists() && sidecars(&s2).iter().all(|p| p.exists()),
            "the newest sealed segment was unlinked"
        );
        let next = maybe_seal(&cfg, t, Some(1)).expect("seal");
        assert!(
            next != s1 && next != s2,
            "the next seal reused a name: {}",
            next.display()
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    // ---- noetl/ai-meta#491 follow-up: the age seal ----

    const H: u64 = 3600;

    fn hours(h: u64) -> std::time::Duration {
        std::time::Duration::from_secs(h * H)
    }

    /// Age-only policy: no size cap, so any seal these tests see is an age seal.
    fn age_only(h: u64) -> SealPolicy {
        SealPolicy {
            max_bytes: None,
            max_age: Some(hours(h)),
        }
    }

    fn later(h: u64) -> std::time::SystemTime {
        std::time::SystemTime::now() + hours(h)
    }

    /// The sidecars are built in the background by the seal (as for a size
    /// seal); wait for both.
    async fn wait_for_sidecars(seg: &Path) {
        for _ in 0..200 {
            if sidecars(seg).iter().all(|p| p.exists()) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("no .idx/.offidx for {} after 5 s", seg.display());
    }

    #[test]
    fn the_age_seal_defaults_on_only_with_the_size_seal_and_typos_turn_it_off() {
        let d48 = Some(hours(48));
        assert_eq!(
            parse_seal_max_age(None, true),
            d48,
            "unset + size seal armed"
        );
        assert_eq!(parse_seal_max_age(Some(" "), true), d48);
        assert_eq!(
            parse_seal_max_age(None, false),
            None,
            "unset without a size seal must stay byte-identical (no rotation)"
        );
        for off in ["0", "off", "OFF", "false", "none"] {
            assert_eq!(parse_seal_max_age(Some(off), true), None, "{off}");
        }
        for typo in ["4 8", "48h", "-1", "nan", "inf"] {
            assert_eq!(parse_seal_max_age(Some(typo), true), None, "{typo}");
        }
        assert_eq!(
            parse_seal_max_age(Some("24"), false),
            Some(hours(24)),
            "explicit arms alone"
        );
        assert_eq!(
            parse_seal_max_age(Some("0.05"), true),
            Some(std::time::Duration::from_secs(180))
        );
        assert_eq!(
            parse_seal_max_age(Some("0.00001"), true),
            Some(TIER_SEAL_MAX_AGE_FLOOR),
            "a tiny value is floored, not a seal per append"
        );
        // Too large for a Duration: refused, not a panic on the append path.
        assert_eq!(parse_seal_max_age(Some("1e300"), true), None);
    }

    /// The age is the FIRST record's minted time — durable, so a writer restart
    /// does not reset it — not the active file's mtime (always "now").
    #[tokio::test]
    async fn the_active_segment_age_comes_from_its_first_record() {
        let cfg = seal_cfg("age-first");
        let t = StoreTier::Projection;
        assert_eq!(
            active_segment_started(&cfg, t),
            None,
            "no file, no sealed: unknown"
        );
        let before = std::time::SystemTime::now() - std::time::Duration::from_millis(5);
        put(&cfg, t, "exec-a", 3).await;
        let after = std::time::SystemTime::now() + std::time::Duration::from_millis(5);
        let started = active_segment_started(&cfg, t).expect("decoded");
        assert!(started >= before && started <= after, "{started:?}");
        // The file's mtime moves with every write; the start must not.
        age(&cfg.path_for(t), 1);
        put(&cfg, t, "exec-a", 1).await;
        ehdb_reference::forget_runtime(&cfg.path_for(t)); // a "restart"
        assert_eq!(active_segment_started(&cfg, t), Some(started));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// No snowflake in line 1 (a foreign writer): fall back to the newest sealed
    /// segment's mtime, i.e. when this active file was opened. Neither: no seal.
    #[tokio::test]
    async fn without_a_readable_first_record_the_newest_seal_time_is_the_start() {
        let cfg = seal_cfg("age-fallback");
        let t = StoreTier::Projection;
        std::fs::write(
            cfg.path_for(t),
            b"{\"sequence\":1,\"transaction_id\":\"legacy-1\"}\n",
        )
        .unwrap();
        assert_eq!(
            maybe_seal_at(&cfg, t, age_only(1), later(1000)),
            None,
            "unknown age"
        );
        let sealed = cfg.path_for(t).with_file_name("projection.jsonl.4");
        std::fs::write(&sealed, b"{}\n").unwrap();
        age(&sealed, 3);
        assert_eq!(
            maybe_seal_at(&cfg, t, age_only(4), later(0)),
            None,
            "3 h < 4 h"
        );
        let got = maybe_seal_at(&cfg, t, age_only(2), later(0)).expect("3 h >= 2 h");
        assert_eq!(got.file_name().unwrap(), "projection.jsonl.5");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ Triggers once the first record is older than the cap, even far below
    /// the size cap; writes the same sidecars a size seal does; numbers the
    /// segment after the highest existing one; and does not seal again until
    /// the NEW segment is itself that old.
    #[tokio::test]
    async fn an_old_active_segment_is_age_sealed_below_the_size_cap() {
        let cfg = seal_cfg("age-seal");
        let t = StoreTier::Projection;
        put(&cfg, t, "exec-a", 2).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-a", 2).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 4).await;
        let len = std::fs::metadata(cfg.path_for(t)).unwrap().len();
        let policy = SealPolicy {
            max_bytes: Some(len * 100),
            max_age: Some(hours(48)),
        };
        assert_eq!(
            maybe_seal_at(&cfg, t, policy, later(47)),
            None,
            "young: 47 h < 48 h"
        );
        assert_eq!(
            maybe_seal_at(&cfg, t, policy.max_bytes.into(), later(1000)),
            None,
            "size-only policy: no age seal"
        );
        let s3 = maybe_seal_at(&cfg, t, policy, later(48)).expect("aged: 48 h >= 48 h");
        assert_eq!(s3.file_name().unwrap(), "projection.jsonl.3");
        assert!(!cfg.path_for(t).exists(), "the rename IS the seal");
        assert_eq!(sealed_segments(&cfg, t), vec![s1, s2, s3.clone()]);
        wait_for_sidecars(&s3).await;
        assert_eq!(load_segment_index(&s3).unwrap().len(), 1, "exec-b only");
        assert_eq!(
            read_execution_across_segments_count(&cfg, t, "exec-b").await,
            4,
            "an age-sealed segment reads like any other"
        );
        // No hot loop: nothing to seal while the active segment is absent, and
        // the next record starts a fresh clock.
        assert_eq!(
            maybe_seal_at(&cfg, t, policy, later(1000)),
            None,
            "absent active"
        );
        put(&cfg, t, "exec-c", 1).await;
        assert_eq!(
            maybe_seal_at(&cfg, t, policy, later(47)),
            None,
            "fresh segment"
        );
        let s4 = maybe_seal_at(&cfg, t, policy, later(49)).expect("and it ages too");
        assert_eq!(s4.file_name().unwrap(), "projection.jsonl.4");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    async fn read_execution_across_segments_count(
        cfg: &TierStoreConfig,
        t: StoreTier,
        exec: &str,
    ) -> usize {
        match read_execution(Some(cfg), t, exec).await {
            TierStoreOutcome::Ok(body) => read_count(&body).0,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_empty_active_segment_is_never_age_sealed() {
        let cfg = seal_cfg("age-empty");
        let t = StoreTier::Projection;
        put(&cfg, t, "exec-a", 1).await;
        let s1 = seal_now(&cfg, t);
        age(&s1, 1000);
        std::fs::write(cfg.path_for(t), b"").unwrap();
        assert_eq!(maybe_seal_at(&cfg, t, age_only(1), later(1000)), None);
        assert_eq!(sealed_segments(&cfg, t), vec![s1]);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// The event log gets the same rule (deliberately: it is prunable, and the
    /// same pin applies on a quiet stretch); its size seal is unchanged. A tier
    /// that is never pruned is never age-sealed.
    #[tokio::test]
    async fn the_event_log_age_seals_too_and_unprunable_tiers_never_do() {
        let cfg = seal_cfg("age-tiers");
        for t in [StoreTier::EventLog, StoreTier::Projection] {
            put(&cfg, t, "exec-a", 2).await;
            let s = maybe_seal_at(&cfg, t, age_only(48), later(48)).expect("prunable tier");
            assert!(s.to_str().unwrap().ends_with(".jsonl.1"), "{}", s.display());
        }
        for &t in StoreTier::ALL
            .iter()
            .filter(|t| !PRUNABLE_TIERS.contains(t))
        {
            put(&cfg, t, "exec-a", 2).await;
            assert_eq!(
                maybe_seal_at(&cfg, t, age_only(1), later(10_000)),
                None,
                "{t:?}"
            );
            // ...and its size seal still works exactly as before.
            let both = SealPolicy {
                max_bytes: Some(1),
                max_age: Some(hours(1)),
            };
            assert!(
                maybe_seal_at(&cfg, t, both, later(0)).is_some(),
                "{t:?} size seal"
            );
        }
        // Size wins over age and both use the same numbering.
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 2).await;
        let s = maybe_seal_at(&cfg, t, Some(1).into(), later(0)).expect("size");
        assert!(s.to_str().unwrap().ends_with("eventlog.jsonl.2"));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ End to end, the production shape: `.1` is old but pinned because an
    /// execution in it still writes to a long-open active segment, and `.2` is
    /// the newest sealed segment, so retention can reclaim neither. An age seal
    /// moves that execution's live records into `.3`; once `.3` is older than
    /// the window, `.1` and `.2` would prune and an enabled pass unlinks them.
    #[tokio::test]
    async fn an_age_seal_unpins_old_projection_segments_once_it_ages() {
        let cfg = seal_cfg("age-e2e");
        let t = StoreTier::Projection;
        put(&cfg, t, "exec-long", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-x", 3).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-long", 2).await; // still writing into the active segment
        age(&s1, 900);
        age(&s2, 850);
        let plan = |now| -> Vec<PruneVerdict> {
            plan_tier_prune(&cfg, t, R49, now)
                .into_iter()
                .map(|d| d.verdict)
                .collect()
        };
        let pinned = PruneVerdict::Keep("pinned: an execution in it also appears in the active segment or a segment younger than the window");
        let newest =
            PruneVerdict::Keep("newest sealed segment: kept so a later seal never reuses its name");
        // Without an age seal: stuck however far into the future we look.
        assert_eq!(
            plan(later(10_000)),
            vec![pinned.clone(), PruneVerdict::BehindAKeptSegment]
        );

        let s3 = maybe_seal_at(&cfg, t, age_only(48), later(48)).expect("age seal");
        wait_for_sidecars(&s3).await;
        // Traffic after the seal lands in a fresh active segment.
        put(&cfg, t, "exec-new", 1).await;
        // .3 is younger than the window and still holds exec-long, so .1 stays
        // pinned (rule 4) and everything newer waits behind it (rule 6).
        assert_eq!(
            plan(later(1)),
            vec![
                pinned,
                PruneVerdict::BehindAKeptSegment,
                PruneVerdict::BehindAKeptSegment
            ]
        );
        // .3 older than the window: .1 and .2 would prune; .3 is the newest (rule 7).
        assert_eq!(
            plan(later(50)),
            vec![PruneVerdict::WouldPrune, PruneVerdict::WouldPrune, newest]
        );
        // And the enabled pass (real clock) agrees once .3 really is that old.
        age(&s3, 50);
        let bytes = std::fs::metadata(&s1).unwrap().len() + std::fs::metadata(&s2).unwrap().len();
        let r = run_prune_enabled(&cfg, R49, None).await;
        let pr = &r.iter().find(|(x, _)| *x == t).unwrap().1;
        assert_eq!(pr.pruned, vec![s1.clone(), s2.clone()], "{pr:?}");
        assert_eq!(pr.pruned_bytes, bytes);
        assert!(s3.exists() && !s1.exists() && !s2.exists());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    // ---- noetl/ai-meta#491: leases + the retention planner ----

    /// Seal the active segment by hand, exactly as `maybe_seal` does (a rename),
    /// so a test controls which executions land in which segment.
    fn seal_now(cfg: &TierStoreConfig, tier: StoreTier) -> PathBuf {
        let active = cfg.path_for(tier);
        let n = sealed_segments(cfg, tier).len() + 1;
        let sealed = active.with_file_name(format!(
            "{}.{n}",
            active.file_name().unwrap().to_str().unwrap()
        ));
        std::fs::rename(&active, &sealed).unwrap();
        ehdb_reference::forget_runtime(&active);
        build_segment_index(&sealed).unwrap();
        sealed
    }

    async fn put(cfg: &TierStoreConfig, tier: StoreTier, exec: &str, n: usize) {
        for i in 0..n {
            let _ = append(Some(cfg), tier, exec, &format!("{{\"i\":{i}}}")).await;
        }
    }

    fn age(path: &Path, hours: u64) {
        let t = std::time::SystemTime::now() - std::time::Duration::from_secs(hours * 3600);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(t)
            .unwrap();
    }

    const R: std::time::Duration = std::time::Duration::from_secs(168 * 3600);

    fn verdicts(cfg: &TierStoreConfig, tier: StoreTier) -> Vec<PruneVerdict> {
        plan_tier_prune(cfg, tier, R, std::time::SystemTime::now())
            .into_iter()
            .map(|d| d.verdict)
            .collect()
    }

    /// ⭐ The rules, one case each. Segment 1 old+finished, 2 old+finished,
    /// 3 recent; exec-live writes in the ACTIVE segment.
    #[tokio::test]
    async fn an_old_finished_segment_would_prune_and_a_recent_one_is_kept() {
        let cfg = seal_cfg("prune-basic");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 5).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 5).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-c", 5).await;
        let _s3 = seal_now(&cfg, t);
        put(&cfg, t, "exec-live", 2).await;
        age(&s1, 400);
        age(&s2, 300);
        assert_eq!(
            verdicts(&cfg, t),
            vec![
                PruneVerdict::WouldPrune,
                PruneVerdict::WouldPrune,
                PruneVerdict::Keep("younger than the retention window"),
            ]
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// An execution still writing — in the active segment or a recent one —
    /// pins every old segment it touches, and oldest-first stops behind it.
    #[tokio::test]
    async fn a_long_runner_pins_its_old_segment_and_everything_behind_it() {
        let cfg = seal_cfg("prune-pin");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-long", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-b", 3).await;
        let s2 = seal_now(&cfg, t);
        put(&cfg, t, "exec-long", 1).await; // still writing, in the ACTIVE segment
        age(&s1, 400);
        age(&s2, 300);
        assert_eq!(
            verdicts(&cfg, t),
            vec![
                PruneVerdict::Keep("pinned: an execution in it also appears in the active segment or a segment younger than the window"),
                PruneVerdict::BehindAKeptSegment,
            ]
        );

        // Pinned by a RECENT sealed segment too, not only the active one.
        let cfg2 = seal_cfg("prune-pin2");
        put(&cfg2, t, "exec-long", 3).await;
        let a = seal_now(&cfg2, t);
        put(&cfg2, t, "exec-long", 1).await;
        let _recent = seal_now(&cfg2, t);
        age(&a, 400);
        assert_eq!(
            verdicts(&cfg2, t)[0],
            PruneVerdict::Keep("pinned: an execution in it also appears in the active segment or a segment younger than the window")
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
        let _ = std::fs::remove_dir_all(&cfg2.dir);
    }

    #[tokio::test]
    async fn an_unindexed_segment_is_kept() {
        let cfg = seal_cfg("prune-noidx");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-z", 1).await;
        let _recent = seal_now(&cfg, t); // s1 must not be the newest (rule 7)
        std::fs::remove_file(segment_index_path(&s1)).unwrap();
        age(&s1, 400);
        assert_eq!(
            verdicts(&cfg, t)[0],
            PruneVerdict::Keep("no .idx: contents unknown")
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn the_catalog_tier_is_never_pruned_however_old() {
        let cfg = seal_cfg("prune-catalog");
        let t = StoreTier::Catalog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        age(&s1, 10_000);
        assert_eq!(
            verdicts(&cfg, t),
            vec![PruneVerdict::Keep("tier is never pruned")]
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn a_leased_segment_is_kept() {
        let cfg = seal_cfg("prune-leased");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-z", 1).await;
        let _recent = seal_now(&cfg, t); // s1 must not be the newest (rule 7)
        age(&s1, 400);
        let lease = SegmentLease::acquire(std::slice::from_ref(&s1));
        assert_eq!(
            verdicts(&cfg, t)[0],
            PruneVerdict::Keep("leased by a read in flight")
        );
        drop(lease);
        assert_eq!(verdicts(&cfg, t)[0], PruneVerdict::WouldPrune);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// The read path actually takes the lease for the sealed phase, and releases it.
    #[tokio::test]
    async fn a_sealed_read_holds_a_lease_and_releases_it() {
        let cfg = seal_cfg("lease-read");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-a", 2).await;
        let probe = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
        lease_probes()
            .lock()
            .unwrap()
            .insert(cfg.dir.clone(), probe.clone());
        let got = read_execution(Some(&cfg), t, "exec-a").await;
        lease_probes().lock().unwrap().remove(&cfg.dir);
        assert!(matches!(got, TierStoreOutcome::Ok(_)), "{got:?}");
        let seen = probe.load(std::sync::atomic::Ordering::SeqCst);
        assert_ne!(
            seen,
            usize::MAX,
            "the sealed phase never ran — measured nothing"
        );
        assert!(
            seen >= 1,
            "the sealed segment was NOT leased during the read"
        );
        assert_eq!(lease_count(&s1), 0, "the lease outlived the read");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Dry run is a dry run: every file survives a pass that finds work.
    #[tokio::test]
    async fn a_dry_run_deletes_nothing() {
        let cfg = seal_cfg("prune-dry");
        let t = StoreTier::EventLog;
        put(&cfg, t, "exec-a", 3).await;
        let s1 = seal_now(&cfg, t);
        put(&cfg, t, "exec-z", 1).await;
        let _recent = seal_now(&cfg, t); // s1 must not be the newest (rule 7)
        age(&s1, 400);
        assert_eq!(
            verdicts(&cfg, t)[0],
            PruneVerdict::WouldPrune,
            "precondition"
        );
        let before: Vec<_> = std::fs::read_dir(&cfg.dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .collect();
        run_prune_dry_run(&cfg, R);
        for p in &before {
            assert!(p.exists(), "dry run removed {}", p.display());
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[test]
    fn mode_on_is_refused_and_retention_below_the_floor_is_refused() {
        assert_eq!(parse_prune_mode(None), PruneMode::Off);
        assert_eq!(parse_prune_mode(Some("dry_run")), PruneMode::DryRun);
        assert_eq!(
            parse_prune_mode(Some("on")),
            PruneMode::Off,
            "`on` must never arm deletion; only the exact word `enabled` does"
        );
        assert_eq!(parse_prune_mode(Some("bogus")), PruneMode::Off);
        assert_eq!(parse_retention_hours(None), Some(49));
        assert_eq!(parse_retention_hours(Some("24")), Some(24));
        assert_eq!(parse_retention_hours(Some("23")), None);
        assert_eq!(parse_retention_hours(Some("nope")), None);
        // u64::MAX / 3600 + 1 hours: `* 3600` wraps to 3584 s (~1 h) in a
        // release build, under the floor. Refused, not wrapped.
        assert_eq!(parse_retention_hours(Some("5124095576030432")), None);
        assert_eq!(
            parse_retention_hours(Some("5124095576030431")),
            Some(5124095576030431),
            "the largest window that fits is still accepted"
        );
    }

    /// ⭐⭐ The sealed segments are read with the store lock RELEASED.
    ///
    /// noetl/ai-meta#490: a read held the shared guard across a 256 MiB sealed
    /// replay, so appends (write guard) queued behind it and a live mirror
    /// append was shed. The probe runs at the start of the sealed phase and
    /// tries the WRITE lock — what an append needs.
    #[tokio::test]
    async fn sealed_segments_are_read_without_the_store_lock() {
        let cfg = seal_cfg("unlocked");
        const N: usize = 30;
        for i in 0..N {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-unl",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "u".repeat(120)),
                Some(2048),
            )
            .await;
        }
        assert!(
            !sealed_segments(&cfg, StoreTier::EventLog).is_empty(),
            "fixture never sealed; the sealed phase would not run"
        );
        let probe = Arc::new(std::sync::atomic::AtomicU8::new(0));
        sealed_probes()
            .lock()
            .unwrap()
            .insert(cfg.dir.clone(), probe.clone());

        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-unl").await;
        sealed_probes().lock().unwrap().remove(&cfg.dir);

        let TierStoreOutcome::Ok(body) = got else {
            panic!("read failed: {got:?}");
        };
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["record_count"], N,
            "the split read lost or duplicated records"
        );
        // Oldest first across the split: sealed records, then the active ones.
        let order: Vec<u64> = v["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let p = r["payload"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| r["payload"].to_string());
                serde_json::from_str::<serde_json::Value>(&p).unwrap()["i"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        assert_eq!(order, (0..N as u64).collect::<Vec<_>>());
        match probe.load(std::sync::atomic::Ordering::SeqCst) {
            1 => {}
            0 => panic!("the sealed phase never ran — this test measured nothing"),
            _ => panic!("the store lock was HELD while sealed segments were read"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Fixture: three executions interleaved across several sealed segments,
    /// every segment indexed. Returns the store and its sealed segments.
    async fn offidx_fixture(tag: &str) -> (TierStoreConfig, Vec<PathBuf>) {
        let cfg = seal_cfg(tag);
        for i in 0..60 {
            let exec = ["exec-a", "exec-b", "exec-c"][i % 3];
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                exec,
                &format!(
                    "{{\"i\":{i},\"q\":\"\\\"x\\\"\",\"pad\":\"{}\"}}",
                    "o".repeat(100)
                ),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 3, "need several segments, got {}", segs.len());
        for sg in &segs {
            build_segment_index(sg).expect("index build");
            assert!(
                segment_offset_index_path(sg).exists(),
                "no offset index written"
            );
        }
        (cfg, segs)
    }

    /// One segment's records for one execution, by full replay.
    fn replayed(sg: &Path, exec: &str) -> Vec<serde_json::Value> {
        let req = EventLogReadExecutionRequest {
            execution_id: exec.to_string(),
            after: None,
            limit: MAX_SCAN_LIMIT,
        };
        let out = read_segment_then_release(sg, true, |d| d.read_execution(&req)).expect("replay");
        let v = serde_json::to_value(&out).unwrap();
        v["records"].as_array().unwrap().clone()
    }

    /// ⭐⭐ The offset path is indistinguishable from the replay it replaces.
    #[tokio::test]
    async fn an_offset_read_equals_a_replay_read() {
        let (cfg, segs) = offidx_fixture("off-eq").await;
        let mut nonempty = 0;
        for sg in &segs {
            for exec in ["exec-a", "exec-b", "exec-c", "exec-absent"] {
                let fast = read_sealed_by_offsets(sg, exec).expect("index should be usable");
                assert_eq!(fast, replayed(sg, exec), "{exec} in {}", sg.display());
                nonempty += usize::from(!fast.is_empty());
            }
        }
        assert!(nonempty >= 3, "compared only empty answers ({nonempty})");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ A sealed read touches only the execution's own lines.
    ///
    /// Proven by damage: another execution's line in the middle of the segment
    /// is overwritten in place (same length, so offsets hold). A replay must
    /// now FAIL — a torn middle fails the whole open — so a read that still
    /// succeeds did not replay. Deleting the offset index then makes the same
    /// read fail, which shows the fallback really is the replay.
    #[tokio::test]
    async fn a_sealed_read_does_not_replay_the_segment() {
        let (cfg, segs) = offidx_fixture("off-noreplay").await;
        let before = read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await;
        let TierStoreOutcome::Ok(before) = before else {
            panic!("baseline read failed: {before:?}")
        };

        let sg = &segs[0];
        let spans = load_offset_spans(sg, "exec-b").expect("spans");
        let (off, len) = spans[spans.len() / 2];
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(sg).unwrap();
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&vec![b'#'; len as usize]).unwrap();
        }
        ehdb_reference::forget_runtime(sg);

        let after = read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await;
        assert!(
            matches!(&after, TierStoreOutcome::Ok(b) if *b == before),
            "the offset read changed or failed after damage it should never touch: {after:?}"
        );

        std::fs::remove_file(segment_offset_index_path(sg)).unwrap();
        ehdb_reference::forget_runtime(sg);
        let replay = read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await;
        assert!(
            matches!(replay, TierStoreOutcome::Error(_)),
            "without the offset index the damaged segment must be REPLAYED (and fail): {replay:?}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// One sealed segment holding `exec-a`, an active one holding `exec-b`,
    /// and the sealed segment torn (a write cut short) when `tear` is set.
    ///
    /// ⚠ The index is built BEFORE the tear: `build_segment_index` refuses a
    /// torn segment outright, so a torn segment in practice is unindexed and
    /// always replayed. Indexes that predate the damage are the case where a
    /// read can step around the segment, which is what this exercises.
    async fn torn_sealed_fixture(tag: &str, tear: bool, index: bool) -> (TierStoreConfig, PathBuf) {
        let cfg = seal_cfg(tag);
        for i in 0..6 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-a",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        let active = cfg.path_for(StoreTier::EventLog);
        let sealed = active.with_file_name("eventlog.jsonl.1");
        std::fs::rename(&active, &sealed).unwrap();
        ehdb_reference::forget_runtime(&active);
        for i in 0..3 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-b",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        if index {
            build_segment_index(&sealed).expect("index build");
            assert!(
                load_offset_spans(&sealed, "exec-a").is_some(),
                "offset index unusable"
            );
        }
        if tear {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&sealed)
                .unwrap();
            std::io::Write::write_all(&mut f, b"{\"torn").unwrap();
        }
        ehdb_reference::forget_runtime(&sealed);
        (cfg, sealed)
    }

    fn torn_of(out: &TierStoreOutcome) -> (u64, Option<u64>) {
        let TierStoreOutcome::Ok(b) = out else {
            panic!("read failed: {out:?}")
        };
        let v: serde_json::Value = serde_json::from_str(b).unwrap();
        (
            v["record_count"].as_u64().unwrap(),
            v["torn_tail_skipped"].as_u64(),
        )
    }

    /// noetl/ehdb#262: a torn tail in a SEALED segment stays visible on a
    /// per-execution read, whichever path the read takes through that segment —
    /// replayed, answered from the offset index, or ruled out by the index.
    #[tokio::test]
    async fn a_torn_sealed_segment_is_reported_by_every_read_path() {
        // Clean control: no field at all, on any path.
        let (cfg, _) = torn_sealed_fixture("torn-clean", false, true).await;
        for exec in ["exec-a", "exec-b"] {
            let (n, torn) = torn_of(&read_execution(Some(&cfg), StoreTier::EventLog, exec).await);
            assert!(n > 0, "{exec}: control read returned nothing");
            assert_eq!(torn, None, "{exec}: a clean store reported a torn tail");
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);

        // Replayed: no index, so the sealed segment is opened by the driver.
        let (cfg, _) = torn_sealed_fixture("torn-replay", true, false).await;
        assert_eq!(
            torn_of(&read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await),
            (6, Some(1)),
            "replay path dropped the sealed segment's torn tail"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);

        // Offset index: the segment is never replayed, so the driver never
        // sees the tear.
        let (cfg, _) = torn_sealed_fixture("torn-offset", true, true).await;
        assert_eq!(
            torn_of(&read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await),
            (6, Some(1)),
            "offset-index path dropped the sealed segment's torn tail"
        );
        // Ruled out by the index: only the active segment is read, but the torn
        // line's execution is unknowable, so this read may be missing it too.
        assert_eq!(
            torn_of(&read_execution(Some(&cfg), StoreTier::EventLog, "exec-b").await),
            (3, Some(1)),
            "index-ruled-out path dropped the sealed segment's torn tail"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);

        // The ACTIVE segment torn, on a read that also merges a sealed one: the
        // driver's own count must survive the merge.
        let (cfg, _) = torn_sealed_fixture("torn-active", false, false).await;
        let active = cfg.path_for(StoreTier::EventLog);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap();
        std::io::Write::write_all(&mut f, b"{\"torn").unwrap();
        drop(f);
        ehdb_reference::forget_runtime(&active);
        assert_eq!(
            torn_of(&read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await),
            (6, Some(1)),
            "the merged read dropped the active segment's torn tail"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// An offset index that does not describe the file is not used.
    #[tokio::test]
    async fn a_stale_or_wrong_offset_index_falls_back_to_replay() {
        let (cfg, segs) = offidx_fixture("off-stale").await;
        let sg = &segs[0];
        let want = replayed(sg, "exec-a");

        // Length mismatch: the header no longer vouches for the file.
        let len = std::fs::metadata(sg).unwrap().len();
        let idx = segment_offset_index_path(sg);
        let raw = std::fs::read_to_string(&idx).unwrap();
        std::fs::write(
            &idx,
            raw.replacen(&len.to_string(), &(len + 1).to_string(), 1),
        )
        .unwrap();
        assert!(
            read_sealed_by_offsets(sg, "exec-a").is_none(),
            "stale length was trusted"
        );

        // Spans that point at ANOTHER execution's lines.
        let b = load_offset_spans_unchecked(&raw, "exec-b");
        let swapped: String = raw
            .lines()
            .map(|l| match l.strip_prefix("exec-a\t") {
                Some(_) => format!("exec-a\t{b}\n"),
                None => format!("{l}\n"),
            })
            .collect();
        std::fs::write(&idx, swapped).unwrap();
        assert!(
            read_sealed_by_offsets(sg, "exec-a").is_none(),
            "foreign lines were served"
        );

        // Either way the public read still returns the right records.
        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-a").await;
        assert!(matches!(got, TierStoreOutcome::Ok(_)), "{got:?}");
        assert!(!want.is_empty());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    fn load_offset_spans_unchecked(raw: &str, exec: &str) -> String {
        raw.lines()
            .find_map(|l| l.strip_prefix(&format!("{exec}\t")))
            .expect("exec in index")
            .to_string()
    }

    /// Measurement, not a gate: one sealed segment of `NOETL_OFFIDX_BENCH_MB`
    /// (default 64) MiB, ~1 KiB events over 500 executions; time a replay read
    /// against an offset read of the same execution. Run with
    /// `cargo test --release --lib offset_read_cost -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn offset_read_cost_vs_replay() {
        let mb: u64 = std::env::var("NOETL_OFFIDX_BENCH_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);
        let cfg = seal_cfg("off-bench");
        let active = cfg.path_for(StoreTier::EventLog);
        let pad = "b".repeat(1000);
        // Batched, as prod writes them (NOETL_EHDB_TIER_APPEND_BATCH=true): one
        // line per batch of one execution's events, 500 executions interleaved.
        let mut i = 0u64;
        while std::fs::metadata(&active).map(|m| m.len()).unwrap_or(0) < mb << 20 {
            let exec = format!("exec-{}", (i / 10) % 500);
            let batch: Vec<String> = (0..10)
                .map(|k| format!("{{\"i\":{},\"pad\":\"{pad}\"}}", i + k))
                .collect();
            let _ = append_batch(Some(&cfg), StoreTier::EventLog, &exec, &batch).await;
            i += 10;
        }
        // Seal exactly as `maybe_seal` does: a rename, nothing else.
        let sg = active.with_file_name(format!(
            "{}.1",
            active.file_name().unwrap().to_str().unwrap()
        ));
        std::fs::rename(&active, &sg).unwrap();
        ehdb_reference::forget_runtime(&active);
        build_segment_index(&sg).unwrap();
        let bytes = std::fs::metadata(&sg).unwrap().len();

        ehdb_reference::forget_runtime(&sg);
        let t = std::time::Instant::now();
        let slow = replayed(&sg, "exec-7");
        let replay = t.elapsed();
        let t = std::time::Instant::now();
        let fast = read_sealed_by_offsets(&sg, "exec-7").unwrap();
        let offset = t.elapsed();
        assert_eq!(fast, slow);
        eprintln!(
            "segment {:.1} MiB, {i} events: replay {replay:?}, offset {offset:?} ({} records)",
            bytes as f64 / 1048576.0,
            fast.len()
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Segments indexed by an older build have `.idx` and no `.offidx`; the
    /// backfill must give them one, and `.idx` must keep its old format.
    #[tokio::test]
    async fn the_backfill_adds_an_offset_index_and_leaves_idx_alone() {
        let (cfg, segs) = offidx_fixture("off-backfill").await;
        let idx_before = std::fs::read_to_string(segment_index_path(&segs[0])).unwrap();
        assert!(
            idx_before
                .lines()
                .all(|l| !l.contains('\t') && !l.contains(':')),
            ".idx format changed — an older binary would misread it: {idx_before:?}"
        );
        for sg in &segs {
            std::fs::remove_file(segment_offset_index_path(sg)).unwrap();
        }
        backfill_segment_indexes(&cfg);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !segs.iter().all(|sg| segment_offset_index_path(sg).exists()) {
            assert!(
                std::time::Instant::now() < deadline,
                "backfill never wrote .offidx"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            std::fs::read_to_string(segment_index_path(&segs[0])).unwrap(),
            idx_before
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// The restart gauge reads the store's TRUE highest sequence, not the
    /// highest within the first scan page.
    #[tokio::test]
    async fn startup_sequence_reads_past_the_first_page() {
        let cfg = seal_cfg("startseq");
        const N: usize = MAX_SCAN_LIMIT + 250;
        let payloads: Vec<String> = (0..N).map(|i| format!("{{\"i\":{i}}}")).collect();
        for chunk in payloads.chunks(100) {
            let _ = append_batch(Some(&cfg), StoreTier::EventLog, "exec-many", chunk).await;
        }
        ehdb_reference::forget_runtime(&cfg.path_for(StoreTier::EventLog));
        assert_eq!(startup_sequence(&cfg, StoreTier::EventLog), N as u64);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ A merge must be bounded by ONE segment at a time, not by their sum.
    ///
    /// Production's tier store is a single 4.1 GB file, so the first seal makes
    /// it sealed segment #1. If the merge left each segment cached, the first
    /// cross-segment read would replay 4.1 GB back into memory and KEEP it —
    /// recreating the OOM by a second route.
    ///
    /// ⚠ This test SETS `NOETL_EHDB_REFERENCE_RUNTIME_CACHE`, and that is a
    /// deliberate exception to "no `set_var` in tests". The property under test
    /// only exists when the cache is on — with it off nothing is retained and
    /// the assertion is vacuous, which is exactly how an earlier version of
    /// this test passed against a planted defect. The distinction from the seal
    /// threshold (which is injected, never set globally) is that this flag
    /// changes only CACHING; it cannot change what a sibling test stores or
    /// reads back. The precondition is asserted, so if that ever stops being
    /// true the test fails rather than quietly proving nothing.
    #[tokio::test]
    async fn a_merged_read_does_not_leave_sealed_segments_cached() {
        std::env::set_var("NOETL_EHDB_REFERENCE_RUNTIME_CACHE", "true");
        assert!(
            ehdb_reference::runtime_cache_enabled(),
            "the runtime cache must be ON, or nothing is retained and this test \
             proves nothing (it passed against a planted defect that way once)"
        );

        let cfg = seal_cfg("release");
        const N: usize = 30;
        for i in 0..N {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-rel",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "q".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(
            segs.len() >= 2,
            "need >=2 sealed segments to exercise the merge, got {}",
            segs.len()
        );

        for sp in &segs {
            ehdb_reference::forget_runtime(sp);
        }
        ehdb_reference::forget_runtime(&cfg.path_for(StoreTier::EventLog));

        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-rel").await;
        assert!(
            matches!(got, TierStoreOutcome::Ok(_)),
            "read failed: {got:?}"
        );

        // The ACTIVE segment stays cached on purpose — the append path uses it
        // on every write, and evicting it would reintroduce a full replay per
        // append. Every SEALED one must be gone.
        for sp in &segs {
            assert!(
                !ehdb_reference::forget_runtime(sp),
                "sealed segment {} was STILL cached after the merge — each one \
                 holds its whole file replayed, so leaving them is the OOM again",
                sp.display()
            );
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ THE ONE THAT MATTERS. An index must never make a read MISS records
    /// that exist. Skipping a segment that does contain the execution is silent
    /// data loss — strictly worse than the slow reads the index exists to cure.
    #[tokio::test]
    async fn an_indexed_read_returns_exactly_what_an_unindexed_read_returns() {
        let cfg = seal_cfg("idx-equiv");
        const N: usize = 36;
        for i in 0..N {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                // Two executions interleaved, so segments genuinely differ in
                // which ids they hold — a fixture where every segment held every
                // execution could not detect a wrong skip.
                if i % 2 == 0 { "exec-even" } else { "exec-odd" },
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(
            segs.len() >= 2,
            "need >=2 sealed segments, got {}",
            segs.len()
        );

        // UNINDEXED answer first — the reference.
        for id in ["exec-even", "exec-odd"] {
            let before = read_execution(Some(&cfg), StoreTier::EventLog, id).await;
            let TierStoreOutcome::Ok(b) = before else {
                panic!("read failed: {before:?}")
            };
            let bv: serde_json::Value = serde_json::from_str(&b).unwrap();

            // Now index every sealed segment and read again.
            for sg in &segs {
                build_segment_index(sg).expect("index build");
            }
            let after = read_execution(Some(&cfg), StoreTier::EventLog, id).await;
            let TierStoreOutcome::Ok(a) = after else {
                panic!("indexed read failed: {after:?}")
            };
            let av: serde_json::Value = serde_json::from_str(&a).unwrap();

            assert_eq!(
                av["record_count"], bv["record_count"],
                "indexing CHANGED the answer for {id}: {} -> {} — a skipped segment \
                 that holds the execution is silent data loss",
                bv["record_count"], av["record_count"]
            );
            assert!(
                bv["record_count"].as_u64().unwrap() > 0,
                "the fixture returned no records for {id}, so equality is vacuous"
            );
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ The index must actually SKIP something, or it is decorative and the
    /// equality test above would pass against an index nobody consults.
    /// Remove every sealed-segment index under a fixture store.
    ///
    /// ⚠ Needed because the seal builds indexes in the BACKGROUND. Two tests
    /// here asserted "no index exists" and passed only while that build was
    /// slow — a race, not a precondition. Moving the store work to the blocking
    /// pool made the build finish promptly and both tests failed, correctly.
    /// State the precondition instead of depending on losing a race.
    fn drop_all_indexes(cfg: &TierStoreConfig) {
        if let Ok(rd) = std::fs::read_dir(&cfg.dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|x| x.to_str()) == Some("idx") {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
    }

    /// ⚠⚠ Every store entry point must hand its CPU-bound work to the blocking
    /// pool. Calling a `*_locked` helper directly from an `async fn` puts a
    /// multi-hundred-MB JSONL replay on a tokio runtime thread, and the writer
    /// has only TWO of those (cpu limit 2). That is what made the tier service,
    /// its metrics server and the worker's own registration all stop responding
    /// in production on 2026-09-22 while the process looked healthy.
    ///
    /// A concurrency cap does not help — it shares a runtime, it does not create
    /// one — and raising it made the failure worse.
    #[test]
    fn every_store_entry_point_runs_its_work_off_the_runtime() {
        let src = include_str!("tier_store.rs");
        let mut checked = 0;
        for (entry, locked) in [
            ("pub async fn read_execution(", "read_execution_locked"),
            ("pub async fn scan(", "scan_locked"),
            ("pub async fn append_with_seal(", "append_locked"),
            ("pub async fn append_batch(", "append_batch_locked"),
            // The seal (size or age) stats, reads and renames the active file.
            ("pub async fn append_with_seal(", "maybe_seal("),
            ("pub async fn append_batch(", "maybe_seal("),
            ("async fn prune_tier_enabled(", "unlink_revalidated_locked"),
        ] {
            let body = src
                .split(entry)
                .nth(1)
                .unwrap_or_else(|| panic!("{entry} not found — guard anchored to a renamed fn"));
            let body = &body[..body.find("\n}\n").unwrap_or(body.len())];
            assert!(
                body.len() > 120,
                "extracted body for {entry} is implausibly small ({} bytes)",
                body.len()
            );
            assert!(
                body.contains(locked),
                "{entry} no longer calls {locked}; this guard is measuring nothing"
            );
            // The call must sit INSIDE an `off_runtime` closure, not merely
            // share a body with some other `off_runtime` call.
            let at = body.find(locked).unwrap_or(0);
            let lead = String::from_utf8_lossy(&body.as_bytes()[at.saturating_sub(160)..at]);
            assert!(
                lead.contains("off_runtime("),
                "{entry} calls {locked} directly on the async runtime. A tier \
                 replay there occupies one of only two runtime threads and \
                 starves the accept loop, the shed path, the metrics server and \
                 worker registration — the 2026-09-22 outage."
            );
            checked += 1;
        }
        assert_eq!(
            checked, 7,
            "expected to check 7 entry points, checked {checked}"
        );
        // The helper must actually use the blocking pool, or the call above is
        // decorative.
        let h = src
            .split("async fn off_runtime")
            .nth(1)
            .expect("off_runtime not found");
        assert!(
            h[..h.find("\n}\n").unwrap_or(h.len())].contains("spawn_blocking"),
            "off_runtime does not use spawn_blocking; it is not moving work off the runtime"
        );
    }

    #[tokio::test]
    async fn streaming_index_equals_replay_index() {
        // The differential that keeps the streaming derivation honest: build the
        // index by streaming, and independently derive the same set from the
        // DRIVER's own `execution_id` (which is what a lookup compares against).
        // If the two ever key differently, a read silently skips a segment that
        // holds the execution — a wrong answer, not a slow one.
        let cfg = seal_cfg("idx-diff");
        for i in 0..40 {
            let exec = if i % 3 == 0 {
                "exec-alpha"
            } else {
                "exec-beta"
            };
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                exec,
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "z".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 2, "need several segments, got {}", segs.len());

        let mut checked = 0usize;
        for sg in &segs {
            let n = build_segment_index(sg).expect("index build");
            let streamed = load_segment_index(sg).expect("index present");

            // Independent derivation, via the driver.
            let d = LocalReferenceEventLogDriver::new(
                sg.clone(),
                DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
                DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            );
            let out = d
                .scan_global(&EventLogScanRequest {
                    after: None,
                    limit: MAX_SCAN_LIMIT,
                })
                .expect("scan");
            ehdb_reference::forget_runtime(sg);
            let replayed: std::collections::BTreeSet<String> =
                out.records.iter().map(|r| r.execution_id.clone()).collect();

            assert!(!replayed.is_empty(), "fixture segment held no records");
            let streamed_set: std::collections::BTreeSet<String> =
                streamed.iter().cloned().collect();
            assert_eq!(
                streamed_set,
                replayed,
                "streaming index disagrees with the driver's own execution_id for {}",
                sg.display()
            );
            assert_eq!(n, replayed.len());
            checked += 1;
        }
        assert!(checked >= 2, "compared only {checked} segments");
    }

    #[tokio::test]
    async fn a_line_with_no_subject_refuses_to_write_an_index() {
        // Fail-safe: a partial index read as complete would skip a segment that
        // holds the execution. No index at all means "might contain" -> opened.
        let cfg = seal_cfg("idx-torn");
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-torn",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "t".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        let sg = segs.first().expect("a sealed segment").clone();
        // ⚠ The seal already built an index in the background. Drop it, or this
        // asserts against that valid index rather than the partial one under
        // test.
        drop_all_indexes(&cfg);
        assert!(
            load_segment_index(&sg).is_none(),
            "precondition: no index yet"
        );
        // Append a line that carries no subject at all.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&sg).unwrap();
            writeln!(f, "{{\"sequence\":999,\"mutations\":[]}}").unwrap();
        }
        let err = build_segment_index(&sg).expect_err("must refuse a partial index");
        assert!(
            err.contains("no readable subject"),
            "unexpected error: {err}"
        );
        assert!(
            load_segment_index(&sg).is_none(),
            "a partial index was written anyway"
        );
        // And the segment is therefore still considered for the read.
        let (kept, _) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-torn");
        assert!(
            kept.contains(&sg),
            "an unindexed segment must still be opened"
        );
    }

    #[tokio::test]
    async fn the_index_actually_rules_segments_out() {
        let cfg = seal_cfg("idx-skip");
        // Fill the early segments with one execution only...
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-early",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)),
                Some(2048),
            )
            .await;
        }
        // ...then a DIFFERENT one, which cannot appear in those early segments.
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-late",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(segs.len() >= 3, "need several segments, got {}", segs.len());
        for sg in &segs {
            build_segment_index(sg).expect("index build");
        }
        let (kept, total) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-late");
        assert_eq!(total, segs.len());
        assert!(
            kept.len() < total,
            "the index ruled out NOTHING ({}/{} kept) — it is not being consulted",
            kept.len(),
            total
        );
        // And the early execution must still find its own segments.
        let (kept_early, _) = segments_possibly_holding(&cfg, StoreTier::EventLog, "exec-early");
        assert!(
            !kept_early.is_empty(),
            "the index ruled out the segments that DO hold exec-early"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠ A MISSING index means "might contain", never "absent".
    #[tokio::test]
    async fn a_segment_without_an_index_is_always_opened() {
        let cfg = seal_cfg("idx-missing");
        for i in 0..24 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-x",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "k".repeat(120)),
                Some(2048),
            )
            .await;
        }
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(!segs.is_empty());
        // No indexes built at all — stated, not raced (see drop_all_indexes).
        drop_all_indexes(&cfg);
        assert!(
            segs.iter().all(|g| load_segment_index(g).is_none()),
            "fixture precondition failed: an index survived"
        );
        let (kept, total) = segments_possibly_holding(&cfg, StoreTier::EventLog, "anything-at-all");
        assert_eq!(
            kept.len(),
            total,
            "an unindexed segment was ruled out; absence of an index must mean \
             UNKNOWN, or a read silently skips records that exist"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐ The index build must PAGE. `scan_global` is capped at
    /// `MAX_SCAN_LIMIT` (1000), so a build that reads one page indexes only the
    /// first 1000 records — and every execution appearing later is then
    /// silently "not in this segment", which makes reads skip records that
    /// exist.
    ///
    /// ⚠ The other index tests cannot catch this: their segments hold far fewer
    /// than 1000 records, so paging never engages and the fixture cannot exhibit
    /// the failure. This one deliberately builds a segment LARGER than a page.
    #[tokio::test]
    async fn the_index_build_pages_past_the_scan_limit() {
        let cfg = seal_cfg("idx-paging");
        // Seal OFF, so everything lands in ONE segment larger than a page.
        const EARLY: usize = MAX_SCAN_LIMIT + 200;
        for i in 0..EARLY {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-early",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        // This one appears ONLY after the first page boundary.
        for i in 0..5 {
            let _ = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-past-the-page",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        // Make it a sealed segment by name, so the index path applies to it.
        let active = cfg.path_for(StoreTier::EventLog);
        let sealed = active.with_file_name("eventlog.jsonl.1");
        std::fs::rename(&active, &sealed).unwrap();
        ehdb_reference::forget_runtime(&active);

        let n = build_segment_index(&sealed).expect("index build");
        let ids = load_segment_index(&sealed).expect("index must exist");
        assert!(
            ids.contains("exec-early"),
            "the index lost the execution on the FIRST page ({n} ids)"
        );
        assert!(
            ids.contains("exec-past-the-page"),
            "the index stopped at the first page: {} records were written but only \
             {n} execution ids indexed, so everything past record {} is invisible \
             and reads would skip it",
            EARLY + 5,
            MAX_SCAN_LIMIT
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Fail-safe: the seal is OFF unless explicitly and usably configured.
    #[test]
    fn the_seal_is_off_unless_explicitly_configured() {
        for raw in [None, Some(""), Some("0"), Some("abc"), Some("-1")] {
            assert_eq!(
                parse_seal_max_bytes(raw),
                None,
                "{raw:?} must leave the seal OFF — a typo must not start rotating a \
                 primary-serving tier's store behind the operator's back"
            );
        }
        assert_eq!(parse_seal_max_bytes(Some("1024")), Some(1024));
        assert_eq!(parse_seal_max_bytes(Some(" 1024 ")), Some(1024));
    }

    /// A store that has never sealed looks exactly like today's.
    #[test]
    fn an_unsealed_store_has_no_segments() {
        let cfg = seal_cfg("none");
        assert!(
            sealed_segments(&cfg, StoreTier::EventLog).is_empty(),
            "a fresh store must report no sealed segments"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⚠ Only all-digit suffixes are segments. A stray `eventlog.jsonl.bak`
    /// must never be replayed as if it were tier data.
    #[test]
    fn only_numbered_suffixes_count_as_segments() {
        let cfg = seal_cfg("suffix");
        let active = cfg.path_for(StoreTier::EventLog);
        std::fs::write(&active, b"").unwrap();
        for bad in ["bak", "tmp", "1a", ""] {
            std::fs::write(active.with_file_name(format!("eventlog.jsonl.{bad}")), b"").unwrap();
        }
        std::fs::write(active.with_file_name("eventlog.jsonl.1"), b"").unwrap();
        std::fs::write(active.with_file_name("eventlog.jsonl.2"), b"").unwrap();
        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert_eq!(
            segs.len(),
            2,
            "expected exactly the two numbered segments, got {segs:?}"
        );
        // Oldest first — the read path depends on this order.
        assert!(segs[0].to_string_lossy().ends_with(".1"));
        assert!(segs[1].to_string_lossy().ends_with(".2"));
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ THE ONE THAT MATTERS. Sealing must bound the ACTIVE segment while
    /// losing **nothing** — every record written before a seal must still read
    /// back, through BOTH readers.
    ///
    /// A seal that bounded memory by dropping records would be strictly worse
    /// than the OOM it cures, so this asserts counts AND payloads, and asserts
    /// the read actually spanned segments (otherwise it proves nothing about
    /// the merge).
    ///
    /// ⚠ The threshold is INJECTED, never `set_var`. An earlier version set the
    /// process env and armed sealing inside an unrelated sibling test, which is
    /// how the missing `scan` merge was found — but it is not a technique to
    /// keep.
    #[tokio::test]
    async fn sealing_bounds_the_active_segment_and_loses_no_records() {
        let cfg = seal_cfg("roundtrip");
        const N: usize = 40;
        const MAX: u64 = 2048;
        for i in 0..N {
            let out = append_with_seal(
                Some(&cfg),
                StoreTier::EventLog,
                "exec-seal",
                &format!("{{\"i\":{i},\"pad\":\"{}\"}}", "y".repeat(120)),
                Some(MAX),
            )
            .await;
            assert!(
                matches!(out, TierStoreOutcome::Ok(_)),
                "append {i} failed: {out:?}"
            );
        }

        let segs = sealed_segments(&cfg, StoreTier::EventLog);
        assert!(
            !segs.is_empty(),
            "the fixture never crossed the threshold, so it cannot show sealing works"
        );

        // BOUND: the active segment stayed small.
        let active_len = std::fs::metadata(cfg.path_for(StoreTier::EventLog))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            active_len < MAX * 2,
            "active segment is {active_len} bytes against a {MAX}-byte threshold — \
             sealing did not bound it"
        );

        // NO LOSS, reader 1: read_execution.
        let got = read_execution(Some(&cfg), StoreTier::EventLog, "exec-seal").await;
        let TierStoreOutcome::Ok(body) = got else {
            panic!("read failed: {got:?}")
        };
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["record_count"].as_u64().unwrap() as usize,
            N,
            "read_execution LOST records: {} of {N} (segments={})",
            v["record_count"],
            v["segments"]
        );
        assert!(
            v["segments"].as_u64().unwrap() > 1,
            "the read did not span segments"
        );

        // ⚠ Sequences must be renumbered over the MERGE. Each segment's driver
        // numbers from 1 within its own file, so without renumbering the merged
        // reply contains several records all claiming to be number 1 — a total
        // order that is not one. (The battery caught that this was unasserted.)
        let seqs: Vec<u64> = v["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["global_sequence"].as_u64().unwrap_or(0))
            .collect();
        let expected: Vec<u64> = (1..=N as u64).collect();
        assert_eq!(
            seqs, expected,
            "merged global_sequence must be 1..N with no duplicates; got {seqs:?}"
        );

        // Payloads, not just the count — a merge returning N copies of one
        // record satisfies a count assertion.
        let recs = v["records"].as_array().unwrap();
        for i in 0..N {
            let needle = format!("\\\"i\\\":{i},");
            assert!(
                recs.iter().any(|r| r.to_string().contains(&needle)),
                "record {i} missing from the merged read_execution"
            );
        }

        // NO LOSS, reader 2: scan. This is the one the first cut forgot.
        let sc = scan(Some(&cfg), StoreTier::EventLog, None, MAX_SCAN_LIMIT).await;
        let TierStoreOutcome::Ok(sbody) = sc else {
            panic!("scan failed: {sc:?}")
        };
        let sv: serde_json::Value = serde_json::from_str(&sbody).unwrap();
        assert_eq!(
            sv["record_count"].as_u64().unwrap() as usize,
            N,
            "scan LOST records: {} of {N} — a scan that returns only the active \
             segment is silent data loss",
            sv["record_count"]
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// A sealed segment holding MORE than one scan page, then a small active one.
    async fn multi_page_segment_fixture(
        tag: &str,
        sealed_n: usize,
        active_n: usize,
    ) -> TierStoreConfig {
        let cfg = seal_cfg(tag);
        for i in 0..sealed_n {
            let _ = append_with_seal(
                Some(&cfg),
                EL,
                "exec-sealed",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        let active = cfg.path_for(EL);
        std::fs::rename(&active, active.with_file_name("eventlog.jsonl.1")).unwrap();
        ehdb_reference::forget_runtime(&active);
        for i in sealed_n..sealed_n + active_n {
            let _ = append_with_seal(
                Some(&cfg),
                EL,
                "exec-active",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }
        assert_eq!(
            sealed_segments(&cfg, EL).len(),
            1,
            "fixture must have one sealed segment"
        );
        cfg
    }

    fn scan_page(body: &TierStoreOutcome) -> (u64, Vec<(u64, u64)>) {
        let TierStoreOutcome::Ok(b) = body else {
            panic!("scan failed: {body:?}")
        };
        let v: serde_json::Value = serde_json::from_str(b).unwrap();
        let rows = v["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let i = serde_json::from_str::<serde_json::Value>(r["payload"].as_str().unwrap())
                    .unwrap()["i"]
                    .as_u64()
                    .unwrap();
                (r["global_sequence"].as_u64().unwrap(), i)
            })
            .collect();
        (v["record_count"].as_u64().unwrap(), rows)
    }

    /// The scan pages THROUGH a sealed segment larger than one page, instead of
    /// taking its first `MAX_SCAN_LIMIT` records and jumping to the next file.
    #[tokio::test]
    async fn a_scan_pages_through_a_sealed_segment_larger_than_one_page() {
        const SEALED: usize = MAX_SCAN_LIMIT + 250;
        const ACTIVE: usize = 30;
        const N: usize = SEALED + ACTIVE;
        let cfg = multi_page_segment_fixture("scan-pages", SEALED, ACTIVE).await;

        let (total, _) = scan_page(&scan(Some(&cfg), EL, None, 1).await);
        assert_eq!(
            total as usize,
            N,
            "record_count must be the whole log; a per-segment page cap reports \
             {} here",
            MAX_SCAN_LIMIT + ACTIVE
        );

        // Walk the log with the cursor, the way a remote reader does.
        let mut seen: Vec<(u64, u64)> = Vec::new();
        let mut after = None;
        loop {
            let (_, rows) = scan_page(&scan(Some(&cfg), EL, after, MAX_SCAN_LIMIT).await);
            let Some(&(last, _)) = rows.last() else { break };
            seen.extend(rows);
            after = Some(last);
            assert!(seen.len() <= N, "the cursor walk never ended");
        }
        let expected: Vec<(u64, u64)> = (0..N as u64).map(|i| (i + 1, i)).collect();
        assert_eq!(
            seen.len(),
            N,
            "the cursor walk returned {} of {N} records",
            seen.len()
        );
        assert_eq!(
            seen, expected,
            "sequences and payloads must be 1..N over 0..N-1, in order"
        );

        // A cursor INSIDE the sealed segment's second page lands there, not in
        // the active segment.
        let (_, rows) =
            scan_page(&scan(Some(&cfg), EL, Some(MAX_SCAN_LIMIT as u64 + 100), 5).await);
        let want: Vec<(u64, u64)> = (0..5u64)
            .map(|k| {
                (
                    MAX_SCAN_LIMIT as u64 + 101 + k,
                    MAX_SCAN_LIMIT as u64 + 100 + k,
                )
            })
            .collect();
        assert_eq!(
            rows, want,
            "a cursor past the first page skipped into the next segment"
        );

        // A cursor at the end returns nothing and still states the total.
        let (total, rows) = scan_page(&scan(Some(&cfg), EL, Some(N as u64), 10).await);
        assert!(rows.is_empty());
        assert_eq!(total as usize, N);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// ⭐⭐ The scan reads sealed segments with the store lock RELEASED, as
    /// `read_execution` does (noetl/ai-meta#490). The probe tries the WRITE
    /// lock — what an append needs — at the start of the sealed phase.
    #[tokio::test]
    async fn a_scan_reads_sealed_segments_without_the_store_lock() {
        let cfg = multi_page_segment_fixture("scan-unlocked", 40, 5).await;
        let probe = Arc::new(std::sync::atomic::AtomicU8::new(0));
        sealed_probes()
            .lock()
            .unwrap()
            .insert(cfg.dir.clone(), probe.clone());
        let got = scan(Some(&cfg), EL, None, MAX_SCAN_LIMIT).await;
        sealed_probes().lock().unwrap().remove(&cfg.dir);
        let (total, rows) = scan_page(&got);
        assert_eq!(total, 45);
        assert_eq!(rows, (0..45u64).map(|i| (i + 1, i)).collect::<Vec<_>>());
        match probe.load(std::sync::atomic::Ordering::SeqCst) {
            1 => {}
            0 => panic!("the sealed phase never ran — this test measured nothing"),
            _ => panic!("the store lock was HELD while the scan read sealed segments"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// A seal landing BETWEEN the unlocked sealed phase and the locked active
    /// read moves records out of the active file into a new sealed one. The
    /// locked phase must notice and send the scan back for that segment, or
    /// those records vanish from the reply.
    #[tokio::test]
    async fn a_seal_between_the_scan_phases_loses_no_records() {
        let cfg = multi_page_segment_fixture("scan-race", 20, 10).await;
        let acc = scan_sealed_phase(
            &cfg,
            EL,
            MergedScan::new(None, MAX_SCAN_LIMIT),
            sealed_segments(&cfg, EL),
        )
        .unwrap();
        assert_eq!(acc.sealed.len(), 1);

        // The race: the active segment is sealed as .2, and appends continue.
        let active = cfg.path_for(EL);
        std::fs::rename(&active, active.with_file_name("eventlog.jsonl.2")).unwrap();
        ehdb_reference::forget_runtime(&active);
        for i in 30..35 {
            let _ = append_with_seal(
                Some(&cfg),
                EL,
                "exec-after",
                &format!("{{\"i\":{i}}}"),
                None,
            )
            .await;
        }

        let LockedScan::SealedSince(acc) = scan_locked(&cfg, EL, acc) else {
            panic!("the locked phase answered from a sealed list that is out of date");
        };
        let acc = scan_sealed_phase(&cfg, EL, acc, sealed_segments(&cfg, EL)).unwrap();
        let LockedScan::Done(out) = scan_locked(&cfg, EL, acc) else {
            panic!("the scan did not converge once the new segment was folded in");
        };
        let (total, rows) = scan_page(&out);
        assert_eq!(total, 35, "records lost across the seal");
        assert_eq!(rows, (0..35u64).map(|i| (i + 1, i)).collect::<Vec<_>>());

        // And the public entry point agrees.
        let (total, rows) = scan_page(&scan(Some(&cfg), EL, None, MAX_SCAN_LIMIT).await);
        assert_eq!(total, 35);
        assert_eq!(rows.len(), 35);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// A sealed segment that disappears or is renumbered mid-scan breaks the
    /// merged numbering; the scan refuses instead of answering.
    #[tokio::test]
    async fn a_segment_vanishing_mid_scan_is_an_error_not_an_answer() {
        let cfg = multi_page_segment_fixture("scan-vanish", 5, 2).await;
        let acc = scan_sealed_phase(
            &cfg,
            EL,
            MergedScan::new(None, MAX_SCAN_LIMIT),
            sealed_segments(&cfg, EL),
        )
        .unwrap();
        let sealed = cfg.path_for(EL).with_file_name("eventlog.jsonl.1");
        std::fs::rename(&sealed, sealed.with_file_name("eventlog.jsonl.7")).unwrap();
        assert!(scan_sealed_phase(&cfg, EL, acc, sealed_segments(&cfg, EL)).is_err());
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// noetl/ehdb#262: a torn tail in a SEALED segment stays visible on the
    /// merged scan, as it is on a single-segment driver reply.
    #[tokio::test]
    async fn a_torn_tail_in_a_sealed_segment_is_reported_by_the_merged_scan() {
        let cfg = multi_page_segment_fixture("scan-torn", 10, 3).await;
        let clean = scan(Some(&cfg), EL, None, MAX_SCAN_LIMIT).await;
        let TierStoreOutcome::Ok(b) = &clean else {
            panic!("{clean:?}")
        };
        assert!(
            !b.contains("torn_tail_skipped"),
            "a clean store must not report a torn tail: {b}"
        );

        let sealed = cfg.path_for(EL).with_file_name("eventlog.jsonl.1");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&sealed)
            .unwrap();
        std::io::Write::write_all(&mut f, b"{\"torn").unwrap();
        drop(f);
        ehdb_reference::forget_runtime(&sealed);

        let torn = scan(Some(&cfg), EL, None, MAX_SCAN_LIMIT).await;
        let TierStoreOutcome::Ok(b) = &torn else {
            panic!("{torn:?}")
        };
        let v: serde_json::Value = serde_json::from_str(b).unwrap();
        assert_eq!(
            v["record_count"].as_u64(),
            Some(13),
            "intact records still read: {b}"
        );
        assert_eq!(
            v["torn_tail_skipped"].as_u64(),
            Some(1),
            "the sealed segment's torn tail vanished from the merged reply: {b}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    /// Every pre-#265 test addresses the event log, which is also the tier the
    /// wire default resolves to — so these keep asserting exactly what they
    /// asserted before the tier argument existed.
    const EL: StoreTier = StoreTier::EventLog;
    const PROJ: StoreTier = StoreTier::Projection;

    fn tmp_cfg(name: &str) -> TierStoreConfig {
        let mut d = std::env::temp_dir();
        d.push(format!("ehdb-tier-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        TierStoreConfig { dir: d }
    }

    #[tokio::test]
    async fn no_store_configured_is_unavailable_not_empty() {
        // The distinction that matters: a caller must be able to tell "no store
        // here" from "the store is empty".
        assert_eq!(
            append(None, EL, "e1", "{}").await,
            TierStoreOutcome::Unavailable
        );
        assert_eq!(
            read_execution(None, EL, "e1").await,
            TierStoreOutcome::Unavailable
        );
        assert_eq!(
            scan(None, EL, None, 10).await,
            TierStoreOutcome::Unavailable
        );
    }

    #[tokio::test]
    async fn append_then_read_returns_the_same_payload() {
        let cfg = tmp_cfg("roundtrip");
        let payload = r#"{"event_type":"probe","n":1}"#;
        match append(Some(&cfg), EL, "exec-1", payload).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("append failed: {other:?}"),
        }
        let body = match read_execution(Some(&cfg), EL, "exec-1").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("read failed: {other:?}"),
        };
        assert!(
            body.contains("probe"),
            "read must return the appended payload; got {body}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn reading_an_absent_execution_is_distinguishable_from_a_hit() {
        // The NEGATIVE control.  Without it, "read returned something" proves
        // nothing — a store that returned the same blob for every key would pass
        // the round-trip test above.
        let cfg = tmp_cfg("absent");
        append(Some(&cfg), EL, "present-1", r#"{"marker":"HIT"}"#).await;

        let hit = match read_execution(Some(&cfg), EL, "present-1").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("expected a hit: {other:?}"),
        };
        let miss = match read_execution(Some(&cfg), EL, "definitely-not-there").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("a miss must still be Ok with an empty result: {other:?}"),
        };
        assert!(hit.contains("HIT"), "hit must carry the marker: {hit}");
        assert!(
            !miss.contains("HIT"),
            "a miss must NOT return another execution's data: {miss}"
        );
        assert_ne!(hit, miss, "hit and miss must be distinguishable");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn exists_reflects_whether_records_came_back() {
        // Regression guard for the field that meant the opposite of its name:
        // the driver reports exists:true for an execution holding no records.
        let cfg = tmp_cfg("exists");
        append(Some(&cfg), EL, "has-records", r#"{"n":1}"#).await;

        let hit: serde_json::Value = match read_execution(Some(&cfg), EL, "has-records").await {
            TierStoreOutcome::Ok(b) => serde_json::from_str(&b).unwrap(),
            other => panic!("{other:?}"),
        };
        let miss: serde_json::Value =
            match read_execution(Some(&cfg), EL, "no-such-execution").await {
                TierStoreOutcome::Ok(b) => serde_json::from_str(&b).unwrap(),
                other => panic!("{other:?}"),
            };
        assert_eq!(hit["exists"], serde_json::Value::Bool(true));
        assert_eq!(hit["record_count"], 1);
        assert_eq!(
            miss["exists"],
            serde_json::Value::Bool(false),
            "a miss must report exists:false — the driver says true, which is why we normalise"
        );
        assert_eq!(miss["record_count"], 0);
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn empty_payload_and_empty_id_are_refused() {
        let cfg = tmp_cfg("invalid");
        match append(Some(&cfg), EL, "e", "").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty payload must be Invalid, got {other:?}"),
        }
        match append(Some(&cfg), EL, "  ", "{}").await {
            TierStoreOutcome::Invalid(_) => {}
            other => panic!("empty execution_id must be Invalid, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn scan_limit_is_clamped_not_trusted() {
        let cfg = tmp_cfg("clamp");
        for i in 0..5 {
            append(Some(&cfg), EL, &format!("e{i}"), &format!(r#"{{"i":{i}}}"#)).await;
        }
        // A caller asking for far more than the cap is clamped, and still served.
        // Parsed, not substring-matched: the record payload is JSON-ESCAPED
        // inside the response ("payload":"{\\"i\\":0}"), so a naive
        // `contains("\"i\":0")` fails against correct data — which is exactly
        // how a good store gets blamed for a bad assertion.
        match scan(Some(&cfg), EL, None, usize::MAX).await {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).expect("scan returns JSON");
                assert_eq!(
                    v["record_count"], 5,
                    "clamped scan still serves every record: {b}"
                );
                assert!(v["records"].as_array().is_some_and(|r| r.len() == 5));
            }
            other => panic!("clamped scan must serve: {other:?}"),
        }
        // A zero/absurd low limit is raised to 1 rather than returning nothing.
        match scan(Some(&cfg), EL, None, 0).await {
            TierStoreOutcome::Ok(_) => {}
            other => panic!("limit 0 must clamp to 1, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    // ---------------------------------------------------------------------
    // #265 A1 — per-tier isolation.
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn tiers_do_not_read_each_others_records() {
        // The property the whole genericisation rests on. If these shared a
        // store, a projection append would land in the log that is serving
        // primary in prod, and the event-log comparator would report it as a
        // record with no authoritative counterpart — divergence caused by the
        // mirror rather than found by it.
        let cfg = tmp_cfg("isolation");
        append(Some(&cfg), EL, "exec-9", r#"{"marker":"EVENTLOG-ONLY"}"#).await;
        append(
            Some(&cfg),
            PROJ,
            "exec-9",
            r#"{"marker":"PROJECTION-ONLY"}"#,
        )
        .await;

        let el = match read_execution(Some(&cfg), EL, "exec-9").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        let proj = match read_execution(Some(&cfg), PROJ, "exec-9").await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        // POSITIVE control first: each tier must actually hold its own record,
        // or "did not see the other one" is satisfied by an empty store.
        assert!(
            el.contains("EVENTLOG-ONLY"),
            "event-log tier lost its record: {el}"
        );
        assert!(
            proj.contains("PROJECTION-ONLY"),
            "projection tier lost its record: {proj}"
        );
        assert!(
            !el.contains("PROJECTION-ONLY"),
            "the event-log tier can see projection records: {el}"
        );
        assert!(
            !proj.contains("EVENTLOG-ONLY"),
            "the projection tier can see event-log records: {proj}"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn each_tier_sequences_independently_from_one() {
        // A shared sequence would make the projection tier's global_sequence
        // jump with event-log traffic, and the append-side parity check is
        // exactly `log_record_count == global_sequence`. Sharing would fail it
        // on a healthy store.
        let cfg = tmp_cfg("sequence");
        for i in 0..3 {
            append(Some(&cfg), EL, &format!("e{i}"), r#"{"t":"el"}"#).await;
        }
        let body = match append(Some(&cfg), PROJ, "p0", r#"{"t":"proj"}"#).await {
            TierStoreOutcome::Ok(b) => b,
            other => panic!("{other:?}"),
        };
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["global_sequence"], 1,
            "the projection tier's first append must be sequence 1, not 4: {body}"
        );
        assert_eq!(v["log_record_count"], 1, "{body}");
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn concurrent_appends_to_one_tier_stay_readable() {
        // Reproduces the shape of the #257 P0 (torn appends under concurrent
        // writers) on the NEW tier, so the projection store inherits the fix
        // rather than being assumed to. Before the serialising lock this test
        // fails on read-back, not on append.
        let cfg = tmp_cfg("concurrent-proj");
        let n = 24;
        let mut set = tokio::task::JoinSet::new();
        for i in 0..n {
            let c = cfg.clone();
            set.spawn(async move {
                append(
                    Some(&c),
                    PROJ,
                    &format!("exec-{i}"),
                    &format!(r#"{{"i":{i}}}"#),
                )
                .await
            });
        }
        let mut ok = 0;
        while let Some(r) = set.join_next().await {
            if matches!(r.expect("task panicked"), TierStoreOutcome::Ok(_)) {
                ok += 1;
            }
        }
        assert_eq!(ok, n, "every concurrent append must succeed");
        // The real assertion is the READ: one torn line makes the replay fail
        // on every subsequent operation, so a store that tore is unreadable.
        match scan(Some(&cfg), PROJ, None, MAX_SCAN_LIMIT).await {
            TierStoreOutcome::Ok(b) => {
                let v: serde_json::Value = serde_json::from_str(&b).unwrap();
                assert_eq!(v["record_count"], n, "records lost or torn: {b}");
            }
            other => panic!("store unreadable after concurrent appends: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }

    #[tokio::test]
    async fn store_bytes_and_startup_sequence_are_per_tier() {
        // These two feed the gauges an operator reads before a flip ("does this
        // store hold anything"). Reporting the event log's size under the
        // projection tier would answer that question about the wrong store.
        let cfg = tmp_cfg("gauges");
        for i in 0..4 {
            append(Some(&cfg), EL, &format!("e{i}"), r#"{"t":"el"}"#).await;
        }
        assert_eq!(
            startup_sequence(&cfg, PROJ),
            0,
            "an untouched projection store must report 0, not the event log's tip"
        );
        assert_eq!(store_bytes(&cfg, PROJ), 0);
        assert_eq!(startup_sequence(&cfg, EL), 4);
        assert!(store_bytes(&cfg, EL) > 0);

        append(Some(&cfg), PROJ, "p0", r#"{"t":"proj"}"#).await;
        assert_eq!(startup_sequence(&cfg, PROJ), 1);
        assert_eq!(
            startup_sequence(&cfg, EL),
            4,
            "a projection append must not move the event log's tip"
        );
        let _ = std::fs::remove_dir_all(&cfg.dir);
    }
}

#[cfg(test)]
mod dedup_window_tests {
    use super::*;

    /// The ring's contract: bounded memory, O(1) membership, FIFO eviction.
    #[test]
    fn the_ring_is_bounded_and_evicts_oldest_first() {
        let mut r = DedupRing::with_capacity(3);
        for id in ["a", "b", "c"] {
            assert!(r.insert(id), "{id} is new");
        }
        assert_eq!(r.len(), 3);
        assert!(r.contains("a"));

        // Fourth insert evicts "a", the oldest.
        assert!(r.insert("d"));
        assert_eq!(r.len(), 3, "capacity must hold");
        assert!(!r.contains("a"), "oldest must be evicted");
        assert!(r.contains("b") && r.contains("c") && r.contains("d"));

        // ⚠ The whole point: "a" is now accepted AGAIN. The window is finite, so
        // a redelivery older than it still gets through — stated as a test, not
        // only in prose, so nobody reads this as a uniqueness guarantee.
        assert!(
            r.insert("a"),
            "an id older than the window is NOT deduplicated"
        );
    }

    #[test]
    fn a_repeat_inside_the_window_is_refused() {
        let mut r = DedupRing::with_capacity(10);
        assert!(r.insert("e1"));
        assert!(!r.insert("e1"), "the second insert must be refused");
        assert!(!r.insert("e1"), "and stay refused");
        assert_eq!(r.len(), 1, "a refused insert must not grow the ring");
    }

    /// ⚠ Capacity 0 is the no-redeploy rollback and must mean OFF, not "a window
    /// of one". A naive eviction loop would evict the id it just inserted and
    /// then accept everything — which looks like off but costs the work.
    #[test]
    fn capacity_zero_disables_the_window_entirely() {
        let mut r = DedupRing::with_capacity(0);
        assert!(r.insert("e1"));
        assert!(
            r.insert("e1"),
            "with the window off, nothing is deduplicated"
        );
        assert_eq!(r.len(), 0, "and nothing is retained");
    }

    #[test]
    fn the_window_size_comes_from_the_env_and_defaults_to_10k() {
        // Default when unset or unparseable.
        assert_eq!(dedup_window(), 10_000);
    }

    /// Distinct ids must not collide — the ring keys on the full id.
    #[test]
    fn distinct_ids_are_independent() {
        let mut r = DedupRing::with_capacity(100);
        for i in 0..50u64 {
            assert!(r.insert(&format!("event-{i}")), "event-{i} is new");
        }
        for i in 0..50u64 {
            assert!(!r.insert(&format!("event-{i}")), "event-{i} repeats");
        }
        assert_eq!(r.len(), 50);
    }
}
