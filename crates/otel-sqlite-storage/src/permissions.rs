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

/// SQLite sidecar suffixes that carry the same telemetry bytes as the main
/// database file: WAL frames, the shared-memory index, and rollback journals.
const SIDECAR_SUFFIXES: &[&str] = &["-wal", "-shm", "-journal"];

/// Restricts the database file *and* its SQLite sidecars to owner-only
/// access.
///
/// SQLite creates `-wal`/`-shm` lazily on first write, so hardening only the
/// `.db` file at creation leaves the secret bytes world-readable next to it
/// under a permissive umask. Missing sidecars are skipped: call this after
/// open and again once writes have flowed (the writer repeats it after its
/// bootstrap checkpoint). Best-effort per file — failures warn, never fail
/// startup, mirroring [`restrict_permissions`].
pub(crate) fn harden_database_files(db_path: &Path) {
    if let Err(error) = restrict_permissions(db_path) {
        tracing::warn!(
            path = %db_path.display(),
            %error,
            "could not restrict database file permissions"
        );
    }
    for suffix in SIDECAR_SUFFIXES {
        let mut sidecar = db_path.as_os_str().to_owned();
        sidecar.push(suffix);
        match restrict_permissions(Path::new(&sidecar)) {
            Ok(()) => {}
            // Sidecars appear lazily; absence is the normal case, not trouble.
            Err(crate::backup::BackupError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    path = %Path::new(&sidecar).display(),
                    %error,
                    "could not restrict database sidecar permissions"
                );
            }
        }
    }
}

/// Restricts a database directory to owner-only access when this process
/// created it; warns when a pre-existing directory stays reachable beyond
/// its owner.
///
/// Tightening a directory someone else created (e.g. a root-owned `/data`
/// the service user cannot chmod) would fail anyway, so pre-existing paths
/// only warn — exactly like [`warn_if_world_readable`].
pub(crate) fn harden_database_dir(dir: &Path, created: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if created {
            if let Err(error) =
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            {
                tracing::warn!(
                    path = %dir.display(),
                    %error,
                    "could not restrict database directory permissions"
                );
            }
            return;
        }
        match std::fs::metadata(dir).map(|metadata| metadata.permissions().mode()) {
            Ok(mode) if mode & 0o077 != 0 => tracing::warn!(
                path = %dir.display(),
                mode = format!("{mode:o}"),
                "database directory is accessible beyond its owner; chmod 700 it",
            ),
            _ => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (dir, created);
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    }

    fn make_world_readable(path: &Path) {
        std::fs::write(path, "telemetry").expect("write test file");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    }

    #[test]
    fn database_files_end_owner_only_while_missing_sidecars_are_skipped() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let db = dir.path().join("otel-logs.db");
        make_world_readable(&db);
        // Only some sidecars exist yet — the rest appear on later writes.
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = db.as_os_str().to_owned();
            sidecar.push(suffix);
            make_world_readable(Path::new(&sidecar));
        }

        harden_database_files(&db);

        assert_eq!(mode(&db), 0o600);
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = db.as_os_str().to_owned();
            sidecar.push(suffix);
            let sidecar = Path::new(&sidecar);
            if sidecar.exists() {
                assert_eq!(mode(sidecar), 0o600, "{}", sidecar.display());
            }
        }
    }

    #[test]
    fn created_directories_are_locked_down_pre_existing_only_warned() {
        let dir = tempfile::tempdir().expect("tmpdir");

        let created = dir.path().join("new-data");
        std::fs::create_dir(&created).expect("mkdir");
        std::fs::set_permissions(&created, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        harden_database_dir(&created, true);
        assert_eq!(mode(&created), 0o700);

        let existing = dir.path().join("old-data");
        std::fs::create_dir(&existing).expect("mkdir");
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        harden_database_dir(&existing, false);
        assert_eq!(
            mode(&existing),
            0o755,
            "pre-existing directories are warned about, not mutated"
        );
    }
}
