//! Shared result-locator helpers ([noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)).
//!
//! The write side (the shadow result materializer, [`crate::result_materializer`])
//! and the read side (the resolve-by-URN path, [`crate::result_resolver`]) must
//! derive the **same** §7 physical key from the same logical URI, or the read
//! never finds what the write stored. This module is the single place both call,
//! so the two stay in lockstep:
//!
//! - [`coords_from_uri`] — parse the canonical logical URI
//!   (`noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`)
//!   into [`ResultCoordinates`].
//! - the `date=` partition is derived from the execution_id snowflake
//!   ([`crate::snowflake::date_partition`]), not the event timestamp — so the
//!   read path reconstructs the key from the URI's `execution_id` alone, with no
//!   carried date (RFC §6.4 derivable-not-carried).

use noetl_tools::locator::ResultCoordinates;

/// Parse the canonical
/// `noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>` URI
/// into coordinates. Returns `None` (never panics) for any non-result / too-short
/// / non-numeric-tail shape.
///
/// Transitional local inversion of `ResultCoordinates::logical_uri`, kept in
/// lockstep with the producer's stamp and `noetl_locator::ResultCoordinates::from_locator`.
pub fn coords_from_uri(uri: &str) -> Option<ResultCoordinates> {
    let rest = uri.strip_prefix("noetl://")?;
    let segs: Vec<&str> = rest.split('/').collect();
    // tenant / project / "results" / eid / step… / frame / row / attempt
    if segs.len() < 8 || segs[2] != "results" {
        return None;
    }
    let tenant = segs[0];
    let project = segs[1];
    if tenant.is_empty() || project.is_empty() {
        return None;
    }
    let n = segs.len();
    let execution_id = segs[3].parse::<i64>().ok()?;
    let frame = segs[n - 3].parse::<u64>().ok()?;
    let row = segs[n - 2].parse::<u64>().ok()?;
    let attempt = segs[n - 1].parse::<u32>().ok()?;
    let step = segs[4..n - 3].join("/");
    if step.is_empty() {
        return None;
    }
    Some(ResultCoordinates::new(
        Some(tenant),
        Some(project),
        execution_id,
        step,
        frame,
        row,
        attempt,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coords_from_uri_round_trips_and_rejects() {
        let c = coords_from_uri("noetl://t_acme/p_gen/results/325/load_next/2/4/1").unwrap();
        assert_eq!(c.tenant, "t_acme");
        assert_eq!(c.project, "p_gen");
        assert_eq!(c.execution_id, 325);
        assert_eq!(c.step, "load_next");
        assert_eq!(c.frame, 2);
        assert_eq!(c.row, 4);
        assert_eq!(c.attempt, 1);
        assert_eq!(
            c.logical_uri(),
            "noetl://t_acme/p_gen/results/325/load_next/2/4/1"
        );
        // Wrong kind / too short / non-numeric tail → None (never panics).
        assert!(coords_from_uri("noetl://t/p/datasets/1/s/0/0/1").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0").is_none());
        assert!(coords_from_uri("noetl://t/p/results/1/s/0/0/x").is_none());
        assert!(coords_from_uri("https://nope").is_none());
    }

    #[test]
    fn step_with_slash_survives() {
        let c = coords_from_uri("noetl://default/default/results/9/a/b/c/3/7/2").unwrap();
        assert_eq!(c.step, "a/b/c");
        assert_eq!(c.frame, 3);
        assert_eq!(c.row, 7);
        assert_eq!(c.attempt, 2);
    }
}
