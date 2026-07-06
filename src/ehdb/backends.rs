//! Phase 10 — worker-side resolution of the tunable per-tier backend-selection
//! surface.
//!
//! This is the operator-facing consolidation layer over the scattered
//! `NOETL_EHDB_*` env: it resolves the process env into the shared
//! [`ehdb_reference::backends`] schema — one coherent matrix of
//! `umbrella-enable → per-tier mode → derived backend` — and validates it.
//!
//! ## Backward-compatible by construction
//!
//! The resolver does **not** re-parse the env with a parallel scheme.  Each
//! tier's mode is read through the *same* `<Tier>Mode::from_env` parser the
//! runtime dispatch uses ([`eventlog::EventLogMode`],
//! [`projection::ProjectionMode`], [`kv::KvMode`], [`object::ObjectMode`],
//! [`vector::VectorMode`]), and the umbrella facts come from
//! [`contract::contract_from_env`] / [`contract::enabled_from_env`].  So the
//! consolidated view can never drift from what the tiers actually do — a
//! deployment that sets no `NOETL_EHDB_*` env resolves every tier to its current
//! default (external incumbent, `off`), and an enabled `primary` tier resolves
//! to EHDB exactly as the dispatch serves it.  Phase 10 adds a *unifying view +
//! validation*, not a behavior change.
//!
//! ## Validation
//!
//! [`resolve`] surfaces every coherence violation
//! ([`ehdb_reference::backends::BackendMatrix::validate`]) plus any umbrella
//! contract error, so the `ehdb-selfcheck config` verb can reject an incoherent
//! deployment (`shadow`/`primary` without `NOETL_EHDB_ENABLED`; a data-plane
//! tier on a control-plane role) with a clear message.  The tier runtime stays
//! disabled-by-default no-op regardless — this layer classifies, it does not
//! change the strict-no-op guarantee.

use ehdb_reference::backends::{
    Backend, BackendConfigError, BackendMatrix, PlatformTier, TierMode, TierSelection,
};

use super::contract::{contract_from_env, enabled_from_env, safe_client_role, EHDB_MODE_ENV};
use super::{eventlog, kv, object, projection, vector, EnvMap};

/// Map a worker event-log mode to the shared crate vocabulary.
fn eventlog_mode(env: &EnvMap) -> TierMode {
    match eventlog::EventLogMode::from_env(env) {
        eventlog::EventLogMode::Off => TierMode::Off,
        eventlog::EventLogMode::Shadow => TierMode::Shadow,
        eventlog::EventLogMode::Primary => TierMode::Primary,
    }
}

fn projection_mode(env: &EnvMap) -> TierMode {
    match projection::ProjectionMode::from_env(env) {
        projection::ProjectionMode::Off => TierMode::Off,
        projection::ProjectionMode::Shadow => TierMode::Shadow,
        projection::ProjectionMode::Primary => TierMode::Primary,
    }
}

fn kv_mode(env: &EnvMap) -> TierMode {
    match kv::KvMode::from_env(env) {
        kv::KvMode::Off => TierMode::Off,
        kv::KvMode::Shadow => TierMode::Shadow,
        kv::KvMode::Primary => TierMode::Primary,
    }
}

fn object_mode(env: &EnvMap) -> TierMode {
    match object::ObjectMode::from_env(env) {
        object::ObjectMode::Off => TierMode::Off,
        object::ObjectMode::Shadow => TierMode::Shadow,
        object::ObjectMode::Primary => TierMode::Primary,
    }
}

fn vector_mode(env: &EnvMap) -> TierMode {
    match vector::VectorMode::from_env(env) {
        vector::VectorMode::Off => TierMode::Off,
        vector::VectorMode::Shadow => TierMode::Shadow,
        vector::VectorMode::Primary => TierMode::Primary,
    }
}

/// Resolve a single tier's mode through its own runtime parser.
fn mode_for_tier(env: &EnvMap, tier: PlatformTier) -> TierMode {
    match tier {
        PlatformTier::EventLog => eventlog_mode(env),
        PlatformTier::Projection => projection_mode(env),
        PlatformTier::Kv => kv_mode(env),
        PlatformTier::Object => object_mode(env),
        PlatformTier::Vector => vector_mode(env),
    }
}

/// The resolved backend configuration + its validation verdict.
#[derive(Debug, Clone)]
pub struct ResolvedBackends {
    /// The resolved per-tier backend matrix.
    pub matrix: BackendMatrix,
    /// Every coherence violation (empty ⇒ coherent).  Includes matrix-level
    /// incoherence and any umbrella contract error.
    pub errors: Vec<BackendConfigError>,
}

impl ResolvedBackends {
    /// The resolved mode of a tier.
    pub fn mode_for(&self, tier: PlatformTier) -> TierMode {
        self.matrix
            .selection(tier)
            .map(|s| s.mode)
            .unwrap_or(TierMode::Off)
    }

    /// The backend serving a tier.
    pub fn backend_for(&self, tier: PlatformTier) -> Backend {
        self.matrix.backend_for(tier)
    }

    /// Whether the resolved config is coherent (no violations).
    pub fn is_coherent(&self) -> bool {
        self.errors.is_empty()
    }

    /// Secret-free JSON render of the resolved 5-tier matrix.
    pub fn to_json(&self) -> serde_json::Value {
        self.matrix.to_json()
    }
}

/// Resolve the consolidated per-tier backend-selection matrix from the env.
///
/// Precedence: umbrella enable (`NOETL_EHDB_ENABLED`) → per-tier mode
/// (`NOETL_EHDB_<TIER>`, read through each tier's own parser) → derived backend
/// (`primary` ⇒ EHDB, else the external incumbent).
pub fn resolve(env: &EnvMap) -> ResolvedBackends {
    let contract_result = contract_from_env(env);
    let enabled = enabled_from_env(env);
    let role = safe_client_role(env);
    let (role_str, control_plane) = match role {
        Some(r) => (r.as_str().to_string(), r.is_control_plane()),
        None => ("unknown".to_string(), false),
    };
    // The umbrella integration mode string: prefer the validated contract; on a
    // malformed contract fall back to the raw env (or the enable-derived
    // default) so the matrix still renders while the error is surfaced below.
    let integration_mode = match &contract_result {
        Ok(c) => c.mode.as_str().to_string(),
        Err(_) => env
            .get(EHDB_MODE_ENV)
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if enabled {
                    "local_reference"
                } else {
                    "disabled"
                }
                .to_string()
            }),
    };

    let tiers: Vec<TierSelection> = PlatformTier::ALL
        .iter()
        .map(|&t| TierSelection::new(t, mode_for_tier(env, t)))
        .collect();

    let matrix = BackendMatrix {
        enabled,
        role: role_str,
        role_is_control_plane: control_plane,
        integration_mode,
        tiers,
    };

    let mut errors = matrix.validate();
    if let Err(e) = contract_result {
        errors.push(BackendConfigError {
            tier: None,
            message: e.0,
        });
    }

    ResolvedBackends { matrix, errors }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Backward-compat: the consolidated view is read through the SAME per-tier
    /// parsers the runtime dispatch uses, so `resolve().mode_for(tier)` must
    /// equal each tier's own `from_env` for any env.
    fn assert_view_matches_tier_parsers(env: &EnvMap) {
        let r = resolve(env);
        assert_eq!(r.mode_for(PlatformTier::EventLog), eventlog_mode(env));
        assert_eq!(r.mode_for(PlatformTier::Projection), projection_mode(env));
        assert_eq!(r.mode_for(PlatformTier::Kv), kv_mode(env));
        assert_eq!(r.mode_for(PlatformTier::Object), object_mode(env));
        assert_eq!(r.mode_for(PlatformTier::Vector), vector_mode(env));
    }

    #[test]
    fn empty_env_all_tiers_default_to_external_off() {
        // The current default: no NOETL_EHDB_* env → every tier off → external
        // incumbent serves, and it is coherent.
        let e = env(&[]);
        let r = resolve(&e);
        assert!(!r.matrix.enabled);
        assert!(r.is_coherent());
        for t in PlatformTier::ALL {
            assert_eq!(r.mode_for(t), TierMode::Off);
            assert_eq!(r.backend_for(t), Backend::External);
        }
        assert_view_matches_tier_parsers(&e);
    }

    #[test]
    fn all_ehdb_enabled_worker_resolves_to_ehdb() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
            ("NOETL_EHDB_PROJECTION", "primary"),
            ("NOETL_EHDB_KV", "primary"),
            ("NOETL_EHDB_OBJECT", "primary"),
            ("NOETL_EHDB_VECTOR", "primary"),
        ]);
        let r = resolve(&e);
        assert!(r.is_coherent());
        for t in PlatformTier::ALL {
            assert_eq!(r.mode_for(t), TierMode::Primary);
            assert_eq!(r.backend_for(t), Backend::Ehdb);
        }
        assert_view_matches_tier_parsers(&e);
    }

    #[test]
    fn mixed_selection_per_tier() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
            ("NOETL_EHDB_VECTOR", "primary"),
            // projection/kv/object left unset → off → external incumbent.
        ]);
        let r = resolve(&e);
        assert!(r.is_coherent());
        assert_eq!(r.backend_for(PlatformTier::EventLog), Backend::Ehdb);
        assert_eq!(r.backend_for(PlatformTier::Vector), Backend::Ehdb);
        assert_eq!(r.backend_for(PlatformTier::Projection), Backend::External);
        assert_eq!(r.backend_for(PlatformTier::Kv), Backend::External);
        assert_eq!(r.backend_for(PlatformTier::Object), Backend::External);
        assert_view_matches_tier_parsers(&e);
    }

    #[test]
    fn shadow_tiers_keep_external_backend() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "system"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "shadow"),
            ("NOETL_EHDB_KV", "shadow"),
        ]);
        let r = resolve(&e);
        assert!(r.is_coherent());
        // shadow dual-writes but the incumbent still serves.
        assert_eq!(r.mode_for(PlatformTier::EventLog), TierMode::Shadow);
        assert_eq!(r.backend_for(PlatformTier::EventLog), Backend::External);
        assert_eq!(r.backend_for(PlatformTier::Kv), Backend::External);
    }

    #[test]
    fn primary_without_enable_is_rejected() {
        // Incoherent: a tier asks to serve but the umbrella is disabled.  The
        // runtime still no-ops (disabled-by-default), but the config verb flags
        // it so an operator catches the misconfig.
        let e = env(&[("NOETL_EHDB_EVENTLOG", "primary")]);
        let r = resolve(&e);
        assert!(!r.is_coherent());
        assert!(r
            .errors
            .iter()
            .any(|x| x.tier == Some(PlatformTier::EventLog)
                && x.message.contains("NOETL_EHDB_ENABLED")));
        // The tier still resolves through its own parser (backward-compat view).
        assert_view_matches_tier_parsers(&e);
    }

    #[test]
    fn shadow_without_enable_is_rejected() {
        let e = env(&[("NOETL_EHDB_PROJECTION", "shadow")]);
        let r = resolve(&e);
        assert!(!r.is_coherent());
        assert!(r
            .errors
            .iter()
            .any(|x| x.tier == Some(PlatformTier::Projection)));
    }

    #[test]
    fn control_plane_role_serving_a_tier_is_rejected() {
        // A gateway/api/server role trying to run a data-plane tier is
        // incoherent: two errors surface — the matrix-level control-plane
        // violation AND the umbrella contract's role/log rejection.
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "gateway"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
        ]);
        let r = resolve(&e);
        assert!(!r.is_coherent());
        assert!(r.errors.iter().any(
            |x| x.message.contains("control-plane") || x.message.contains("gateway/api/server")
        ));
    }

    #[test]
    fn json_render_reflects_resolution() {
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
        ]);
        let r = resolve(&e);
        let json = r.to_json();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["role"], "worker");
        assert_eq!(json["control_plane"], false);
        assert_eq!(json["coherent"], true);
        let tiers = json["tiers"].as_array().unwrap();
        assert_eq!(tiers.len(), 5);
        assert_eq!(tiers[0]["tier"], "eventlog");
        assert_eq!(tiers[0]["backend"], "ehdb");
    }

    #[test]
    fn secret_keyed_env_values_never_appear_in_render() {
        // Sensitive-keyed env alongside the EHDB flags must not leak into the
        // matrix — it only reflects tier keys/modes/backends, not env values.
        let e = env(&[
            ("NOETL_EHDB_ENABLED", "true"),
            ("NOETL_EHDB_MODE", "local_reference"),
            ("NOETL_EHDB_CLIENT_ROLE", "worker"),
            ("NOETL_EHDB_LOCAL_REFERENCE_LOG", "/tmp/x.jsonl"),
            ("NOETL_EHDB_EVENTLOG", "primary"),
            ("AWS_SECRET_ACCESS_KEY", "super-secret-value-xyz"),
            ("DB_PASSWORD", "hunter2-do-not-leak"),
        ]);
        let r = resolve(&e);
        let rendered = serde_json::to_string(&r.to_json()).unwrap();
        assert!(!rendered.contains("super-secret-value-xyz"));
        assert!(!rendered.contains("hunter2-do-not-leak"));
    }
}
