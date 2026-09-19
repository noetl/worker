//! An **L0-backed `EventLogDriver`** for the tier store — M0.5.
//!
//! # The gap this closes
//!
//! `tier_store::driver()` returned a concrete `LocalReferenceEventLogDriver`
//! with no dispatch, and `ehdb-reference` — where that driver lives — does not
//! depend on `ehdb-l0` at all. Its storage is
//! `OpenOptions::new().append(true)` plus `BufReader::lines()`; occurrence
//! counts in `ehdb-stream`'s `lib.rs` are `replica` 0, `seal` 0, `manifest` 0,
//! `fsync` 0.
//!
//! So **every L0 primitive the multi-region phases extend is not on the tier's
//! path**: M1 locality, M4 region-survival and M7 replication are inert on the
//! tier for the same reason `NOETL_EHDB_EVENTLOG_BACKEND` is. This driver is
//! the seam that ends that.
//!
//! # ⚠ The two backends are NOT interchangeable on an existing store
//!
//! `ehdb-stream`'s `StreamSequence` starts at 1 and refuses 0; L0's
//! `global_sequence` is recovered as `manifest.max_sequence()`. A backend
//! switch on a NON-EMPTY store is therefore not a no-op, and this ships the
//! dispatch, **not a data migration**. The refusal that makes that safe is the
//! point of `l0_refuses_a_local_reference_store` — a silent misparse on a tier
//! that is `primary` is a wrong answer, not an outage.
//!
//! # Single writer, enforced by a lock
//!
//! `EventLogDriver` takes `&self`; `L0Engine::append_record` takes `&mut self`.
//! The `Mutex` that bridges them is not an implementation detail being papered
//! over — the engine IS single-writer-per-partition by contract, so serialising
//! appends behind one lock is the contract, made explicit.

use std::sync::{Arc, Mutex};

use ehdb_core::{EhdbError, Result};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine};
use ehdb_reference::{
    EventLogAckOutcome, EventLogAckRequest, EventLogAppendOutcome, EventLogAppendRequest,
    EventLogDriver, EventLogReadExecutionOutcome, EventLogReadExecutionRequest, EventLogRecordView,
    EventLogScanOutcome, EventLogScanRequest, EventLogTailOutcome, EventLogTailRequest,
};

/// One shard. The tier is single-writer by construction (one writer pod owns
/// the store), and a second shard would only split the log without adding a
/// writer.
const TIER_SHARD: u32 = 0;

pub struct L0TierDriver {
    engine: Mutex<L0EventLogEngine>,
    substrate: Arc<dyn DurableSubstrate>,
}

impl std::fmt::Debug for L0TierDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0TierDriver").finish_non_exhaustive()
    }
}

impl L0TierDriver {
    /// Open (or create) an L0 store rooted at `root`.
    ///
    /// ⚠ `L0Engine::open` runs `format_version::verify_or_initialise`, which
    /// **refuses a directory written by a different layout** rather than
    /// parsing it. That refusal is what makes the backend flag safe to flip:
    /// pointing `l0` at a `local_reference` store errors instead of returning
    /// garbled records.
    pub fn open(root: &std::path::Path) -> Result<Self> {
        let substrate: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(root)?);
        let engine = L0EventLogEngine::open(
            L0Config::d1(root).with_shard_count(1),
            Arc::clone(&substrate),
        )?;
        Ok(Self {
            engine: Mutex::new(engine),
            substrate,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, L0EventLogEngine>> {
        // Poison is not recoverable here: a panic mid-append may have left the
        // writer half-advanced, and continuing would append behind the tail.
        self.engine
            .lock()
            .map_err(|_| EhdbError::InvalidState("l0 tier engine mutex poisoned".into()))
    }

    fn view(r: &EventRecord) -> EventLogRecordView {
        EventLogRecordView {
            global_sequence: r.global_sequence,
            execution_id: r.execution_id.clone(),
            transaction_id: r.transaction_id.clone(),
            byte_len: r.payload.len(),
            payload: r.payload.clone(),
        }
    }

    fn all(&self) -> Result<Vec<EventRecord>> {
        self.lock()?.read_partition_after(TIER_SHARD, 0)
    }
}


/// The tier's durable consumer cursor.
///
/// ⚠ Hand-rolled rather than `ehdb_l0::cursor` because that module is **not
/// public at the ehdb rev this crate pins** (`49fdefcc`). Repinning would pull
/// unrelated engine changes into this review, so the cursor — one `u64` behind
/// one key — lives here instead. It uses the same substrate the engine does, so
/// it is as durable as the parts beside it.
mod tier_cursor {
    use super::*;

    fn key(shard: u32) -> String {
        format!("cursor/tier-{shard}.ack")
    }

    pub fn load(s: &dyn DurableSubstrate, shard: u32) -> Result<u64> {
        let k = key(shard);
        if !s.exists(&k)? {
            // Absent is 0 — a consumer that has acked nothing. Distinct from an
            // unreadable cursor, which propagates as an error rather than
            // silently restarting the consumer from the beginning.
            return Ok(0);
        }
        let bytes = s.get_all(&k)?;
        std::str::from_utf8(&bytes)
            .ok()
            .and_then(|t| t.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                EhdbError::Storage(format!(
                    "tier cursor {k} is unreadable; refusing rather than treating it as 0, \
                     which would redeliver the whole log"
                ))
            })
    }

    /// Monotonic: the cursor never moves backwards, so a late or out-of-order
    /// ack cannot resurrect records a consumer already processed.
    pub fn advance(s: &dyn DurableSubstrate, shard: u32, through: u64) -> Result<u64> {
        let cur = load(s, shard)?;
        let next = cur.max(through);
        if next != cur {
            s.put_overwrite(&key(shard), next.to_string().as_bytes())?;
        }
        Ok(next)
    }
}

impl EventLogDriver for L0TierDriver {
    fn driver_name(&self) -> &'static str {
        "l0_segment"
    }

    fn append(&self, request: &EventLogAppendRequest) -> Result<EventLogAppendOutcome> {
        let mut g = self.lock()?;
        // `append_writer_assigned`, not `append_record`: the writer assigns the
        // next monotonic sequence, which is what keeps the shard log ascending
        // and every record claimable. The caller does not own a sort key here.
        let seq = g.append_writer_assigned(EventRecord::new(
            0,
            request.execution_id.clone(),
            request.transaction_id.clone(),
            request.payload.clone(),
        ))?;
        let count = g.read_partition_after(TIER_SHARD, 0)?.len();
        Ok(EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id: request.execution_id.clone(),
            global_sequence: seq,
            byte_len: request.payload.len(),
            created_stream: seq == 1,
            log_record_count: count,
            // This driver does not dedupe: the tier's idempotency key lives in
            // the payload, not in a column the pinned engine indexes. Reporting
            // `true` would claim a suppression that did not happen.
            deduplicated: false,
        })
    }

    fn scan_global(&self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome> {
        let recs = self.all()?;
        let total = recs.len();
        let after = request.after.unwrap_or(0);
        let out: Vec<EventLogRecordView> = recs
            .iter()
            .filter(|r| r.global_sequence > after)
            .take(request.limit)
            .map(Self::view)
            .collect();
        Ok(EventLogScanOutcome {
            action: "eventlog-scan".to_string(),
            exists: total > 0,
            record_count: total,
            returned: out.len(),
            records: out,
        })
    }

    fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome> {
        let recs = self
            .lock()?
            .read_execution_after(&request.execution_id, request.after.unwrap_or(0))?;
        let total = recs.len();
        let out: Vec<EventLogRecordView> =
            recs.iter().take(request.limit).map(Self::view).collect();
        Ok(EventLogReadExecutionOutcome {
            action: "eventlog-read-exec".to_string(),
            execution_id: request.execution_id.clone(),
            // ⚠ `exists` tracks whether RECORDS came back, not whether the
            // execution is known. The local-reference driver reports `true` for
            // an execution it holds no records for, and `tier_store` has to
            // normalise that at the boundary; this driver simply does not
            // introduce the discrepancy.
            exists: total > 0,
            record_count: total,
            returned: out.len(),
            records: out,
        })
    }

    fn tail(&self, request: &EventLogTailRequest) -> Result<EventLogTailOutcome> {
        // The durable consumer cursor is L0's own, keyed per shard.
        let acked = tier_cursor::load(self.substrate.as_ref(), TIER_SHARD)?;
        let recs = self.all()?;
        let pending: Vec<&EventRecord> =
            recs.iter().filter(|r| r.global_sequence > acked).collect();
        let out: Vec<EventLogRecordView> = pending
            .iter()
            .take(request.limit)
            .map(|r| Self::view(r))
            .collect();
        Ok(EventLogTailOutcome {
            action: "eventlog-tail".to_string(),
            consumer: request.consumer.clone(),
            exists: !recs.is_empty(),
            // L0's cursor is per shard, not per named consumer: there is no
            // creation event to report, and claiming one would be a fact about
            // a consumer registry this backend does not have.
            created_consumer: false,
            acked_sequence: (acked > 0).then_some(acked),
            pending_count: pending.len(),
            returned: out.len(),
            records: out,
        })
    }

    fn ack(&self, request: &EventLogAckRequest) -> Result<EventLogAckOutcome> {
        let acked = tier_cursor::advance(self.substrate.as_ref(), TIER_SHARD, request.sequence)?;
        Ok(EventLogAckOutcome {
            action: "eventlog-ack".to_string(),
            consumer: request.consumer.clone(),
            acked_sequence: acked,
        })
    }
}
