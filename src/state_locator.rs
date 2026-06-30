//! State-shard locator ([noetl/ai-meta#166](https://github.com/noetl/ai-meta/issues/166)
//! Phase 2) — the §7 physical key for a per-execution **state shard**.
//!
//! This is the write-side sibling of [`crate::result_locator`]: the result
//! tier addresses one *result* under
//! `noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`;
//! the state tier addresses one *execution's slim event-chain* under
//! `noetl://<tenant>/<project>/state/<eid>/<open|sealed>`. Both reuse the same
//! #104 primitives — `noetl_tools::locator::shard_key` (FNV-1a folder shard) and
//! [`CellPlacement`] — so a state shard **co-locates with the same execution's
//! result bytes** under one `shard=sNNNN/.../execution=<eid>/` prefix (one prefix
//! listing returns both).
//!
//! ## Why a local URN (not promoted into `noetl-tools` yet)
//!
//! Phase 2 is a **shadow** writer: nothing reads these shards (that is Phase 3).
//! Keeping [`StateCoordinates`] local to the worker — exactly as
//! [`crate::result_locator::coords_from_uri`] keeps the result-URN inversion
//! local — makes Phase 2 a single-repo (worker-only), flag-gated, reversible
//! change with no `noetl-tools` release + dependency-bump cycle. When Phase 3
//! adds the read path, the URN can be promoted into `noetl-locator` so writer and
//! reader stay in lockstep (the same lifecycle the result locator followed).

use noetl_tools::locator::{shard_key, CellPlacement, DEFAULT_TENANT, DEFAULT_PROJECT};

/// The `kind` segment for execution state-shard assets (parallel to the result
/// locator's `results`).
pub const KIND_STATE: &str = "state";

/// Whether a state shard is still receiving events (open) or has observed the
/// execution's terminal event (sealed). The seal segment is part of the §7 key
/// so an open shard and its final sealed shard are distinct objects — a reader
/// (Phase 3) prefers `sealed` and falls back to `open` + the WAL tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardSeal {
    /// The execution is live; the shard is appended as events arrive.
    Open,
    /// The terminal event landed; the shard is the final compacted chain.
    Sealed,
}

impl ShardSeal {
    /// The `<open|sealed>` segment used in both the logical URI and the §7 key.
    pub fn segment(self) -> &'static str {
        match self {
            ShardSeal::Open => "open",
            ShardSeal::Sealed => "sealed",
        }
    }
}

/// Execution-scoped coordinates that address one state shard. Unlike
/// [`noetl_tools::locator::ResultCoordinates`] there is no `(step, frame, row,
/// attempt)` fan-out — a state shard is **one object per execution** (per seal
/// state), carrying that execution's whole slim chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCoordinates {
    pub tenant: String,
    pub project: String,
    pub execution_id: i64,
}

impl StateCoordinates {
    /// Construct coordinates, defaulting tenant/project for single-tenant
    /// deployments (and for executions whose events carry no result reference to
    /// derive a tenant from — see [`crate::state_materializer`]).
    pub fn new(tenant: Option<&str>, project: Option<&str>, execution_id: i64) -> Self {
        Self {
            tenant: tenant.unwrap_or(DEFAULT_TENANT).to_string(),
            project: project.unwrap_or(DEFAULT_PROJECT).to_string(),
            execution_id,
        }
    }

    /// The stable logical URI for this execution's state shard:
    /// `noetl://<tenant>/<project>/state/<execution_id>/<open|sealed>`.
    pub fn logical_uri(&self, seal: ShardSeal) -> String {
        format!(
            "noetl://{}/{}/{}/{}/{}",
            self.tenant,
            self.project,
            KIND_STATE,
            self.execution_id,
            seal.segment()
        )
    }

    /// Stable folder shard, co-celling state with the same execution's results by
    /// feeding `execution_id` as the affinity — **identical** to
    /// [`noetl_tools::locator::ResultCoordinates::shard_key`], so the `s{fnv:04}`
    /// folder a state shard lands in is the same folder that execution's result
    /// bytes land in.
    pub fn shard_key(&self, shard_count: u32) -> u32 {
        shard_key(
            &self.tenant,
            &self.project,
            Some(&self.execution_id.to_string()),
            shard_count,
        )
    }

    /// The §7 physical object-store key for this execution's state shard under a
    /// resolved cell placement. Mirrors the result key's prefix exactly up to the
    /// `execution=<eid>/` segment, then diverges into `state/<open|sealed>.<ext>`:
    ///
    /// ```text
    /// noetl/env=<env>/region=<region>/cell=<cell>/shard=s<NNNN>/
    ///   tenant=<tenant>/project=<project>/date=<date>/execution=<eid>/
    ///   state/<open|sealed>.<ext>
    /// ```
    ///
    /// `date` is the execution-id-derived UTC partition (so the read path can
    /// reconstruct the key from the URI's `execution_id` alone, no carried date —
    /// the same derivable-not-carried contract the result tier uses); `ext` is
    /// the payload extension (`feather`).
    pub fn physical_key(&self, placement: &CellPlacement, date: &str, seal: ShardSeal, ext: &str) -> String {
        format!(
            "noetl/env={env}/region={region}/cell={cell}/shard={shard}/\
             tenant={tenant}/project={project}/date={date}/execution={eid}/\
             state/{seal}.{ext}",
            env = placement.env,
            region = placement.region,
            cell = placement.cell,
            shard = placement.shard,
            tenant = self.tenant,
            project = self.project,
            date = date,
            eid = self.execution_id,
            seal = seal.segment(),
            ext = ext,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noetl_tools::locator::ResultCoordinates;

    #[test]
    fn logical_uri_open_and_sealed() {
        let c = StateCoordinates::new(Some("muno"), Some("travel"), 325);
        assert_eq!(c.logical_uri(ShardSeal::Open), "noetl://muno/travel/state/325/open");
        assert_eq!(c.logical_uri(ShardSeal::Sealed), "noetl://muno/travel/state/325/sealed");
    }

    #[test]
    fn default_tenant_project() {
        let c = StateCoordinates::new(None, None, 7);
        assert_eq!(c.tenant, "default");
        assert_eq!(c.project, "default");
        assert_eq!(c.logical_uri(ShardSeal::Open), "noetl://default/default/state/7/open");
    }

    #[test]
    fn physical_key_layout_and_seal() {
        let c = StateCoordinates::new(Some("muno"), Some("travel"), 325);
        let placement = CellPlacement::new("prod", "usc1", "usc1-a", 42);
        let open = c.physical_key(&placement, "2026-06-30", ShardSeal::Open, "feather");
        assert_eq!(
            open,
            "noetl/env=prod/region=usc1/cell=usc1-a/shard=s0042/\
             tenant=muno/project=travel/date=2026-06-30/execution=325/\
             state/open.feather"
        );
        let sealed = c.physical_key(&placement, "2026-06-30", ShardSeal::Sealed, "feather");
        assert!(sealed.ends_with("/state/sealed.feather"));
        // Open and sealed are DISTINCT objects under the same execution prefix.
        assert_ne!(open, sealed);
    }

    #[test]
    fn state_shard_colocates_with_result_bytes() {
        // The load-bearing co-location property (RFC §3.1): a state shard's folder
        // shard == the same execution's result-byte folder shard, because both
        // feed `execution_id` as the affinity into the SAME stable FNV-1a hash.
        let eid = 325i64;
        let state = StateCoordinates::new(Some("muno"), Some("travel"), eid);
        let result = ResultCoordinates::new(Some("muno"), Some("travel"), eid, "load", 0, 0, 1);
        assert_eq!(
            state.shard_key(256),
            result.shard_key(256),
            "state shard must land in the same s{{fnv}} folder as the execution's results"
        );
    }

    #[test]
    fn shard_key_is_stable_and_in_range() {
        // Same inputs → same key, every call; and always within the configured
        // shard count (the storage-layout stability contract from #104).
        let c = StateCoordinates::new(Some("muno"), Some("travel"), 99);
        assert_eq!(c.shard_key(256), c.shard_key(256));
        for i in 0..200 {
            let k = StateCoordinates::new(None, None, i).shard_key(16);
            assert!(k < 16, "shard {k} out of range for count 16");
        }
    }
}
