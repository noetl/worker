//! Control-plane guard — the in-code enforcement of the EHDB data-access
//! boundary, defense-in-depth on top of the [`super::contract`] validation.
//!
//! Only `worker` / `playbook` / `system` roles may touch EHDB data.  A
//! control-plane role (`gateway` / `api` / `server`) that reaches any
//! data-plane / event-stream op is refused here BEFORE the `ehdb_reference`
//! runtime is opened, so no write can occur.

use super::contract::EhdbClientRole;

/// A refusal of a data-plane / event-stream op on role grounds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EhdbGuardError {
    /// A control-plane role (gateway/api/server) attempted a data-plane op.
    #[error(
        "EHDB data-plane {operation} refused for control-plane role '{role}'; \
         gateway/api/server remain gatekeepers and never touch EHDB data"
    )]
    ControlPlane {
        role: &'static str,
        operation: &'static str,
    },
    /// A role that is neither control-plane nor a recognised data-plane role.
    #[error("EHDB data-plane {operation} requires a worker/playbook/system role, got '{role}'")]
    WrongRole {
        role: &'static str,
        operation: &'static str,
    },
}

/// Guard: only worker/playbook/system roles may run the named data-plane op.
///
/// Returns `Err` for any control-plane role (the boundary) and for any role
/// that is not a recognised data-plane role.
pub fn assert_data_plane_access_allowed(
    role: EhdbClientRole,
    operation: &'static str,
) -> Result<(), EhdbGuardError> {
    if role.is_control_plane() {
        return Err(EhdbGuardError::ControlPlane {
            role: role.as_str(),
            operation,
        });
    }
    if !role.is_data_plane() {
        return Err(EhdbGuardError::WrongRole {
            role: role.as_str(),
            operation,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_plane_roles_refused() {
        for role in [
            EhdbClientRole::Gateway,
            EhdbClientRole::Api,
            EhdbClientRole::Server,
        ] {
            let err = assert_data_plane_access_allowed(role, "append").unwrap_err();
            assert!(matches!(err, EhdbGuardError::ControlPlane { .. }));
        }
    }

    #[test]
    fn data_plane_roles_allowed() {
        for role in [
            EhdbClientRole::Worker,
            EhdbClientRole::Playbook,
            EhdbClientRole::System,
        ] {
            assert!(assert_data_plane_access_allowed(role, "append").is_ok());
        }
    }
}
