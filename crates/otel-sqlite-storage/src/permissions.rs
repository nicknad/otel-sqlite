use std::path::Path;

use crate::backup::BackupError;

/// Restricts `path` to owner-only access.
///
/// * Unix: `chmod 0o600`.
/// * Windows: strips inherited ACLs and grants Full Control to the current
///   user plus `SYSTEM`/`Administrators` via `icacls` (best-effort: warns
///   instead of failing when `icacls` is unavailable so backups never break
///   on exotic runners).
/// * Other platforms: no-op.
pub(crate) fn restrict_permissions(path: &Path) -> Result<(), BackupError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    #[cfg(windows)]
    {
        restrict_windows(path)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)] // mirrors the unix arm's signature for uniform call sites
fn restrict_windows(path: &Path) -> Result<(), BackupError> {
    // Well-known SIDs (locale-independent): *S-1-5-32-544 = Administrators,
    // *S-1-5-18 = SYSTEM. Display names like "Administrators" fail on
    // localized Windows editions.
    let user = std::env::var("USERNAME").unwrap_or_default();
    let mut cmd = std::process::Command::new("icacls");
    cmd.arg(path)
        .arg("/inheritance:r")
        .arg("/grant:r")
        .arg(format!("{user}:F"))
        .arg("*S-1-5-18:F")
        .arg("*S-1-5-32-544:F");
    // `icacls` prints its own diagnostics; suppress stdout on success.
    match cmd.output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr);
            tracing::warn!(
                path = %path.display(),
                detail = detail.trim(),
                "icacls failed; backup artifact may inherit permissive ACLs"
            );
            Ok(())
        }
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "icacls unavailable; backup artifact may inherit permissive ACLs"
            );
            Ok(())
        }
    }
}

/// Warns when `path` is readable beyond its owner (unix `mode & 0o044`).
/// No-op on non-unix: ACL inspection needs platform APIs.
pub(crate) fn warn_if_world_readable(path: &Path, label: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        match std::fs::metadata(path).map(|metadata| metadata.permissions().mode()) {
            Ok(mode) if mode & 0o044 != 0 => tracing::warn!(
                path = %path.display(),
                mode = format!("{mode:o}"),
                "{label} is readable beyond its owner; chmod 600 it",
            ),
            _ => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, label);
    }
}
