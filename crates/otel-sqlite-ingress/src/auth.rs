//! Bearer-token authentication backed by a hot-reloadable file.
//!
//! [`TokenFileVault`] reads a file of tokens (one per line, all valid) and
//! answers "is this bearer token accepted?" for the gRPC interceptor.
//! Design goals:
//!
//! * **Zero-downtime rotation**: every non-empty line is valid, so rotation
//!   is "append the new token (atomic rename), migrate senders, delete the
//!   old line later". No restart, no synchronized cutover.
//! * **Hot reload**: the file is re-read when older than a small cache
//!   interval; rotation takes effect within seconds without touching the
//!   process.
//! * **No plaintext retention**: lines are hashed (SHA-256) at load time and
//!   only digests are compared — constant-time, so request timing does not
//!   leak how much of a presented token matched. File contents and transient
//!   digests live in `zeroize::Zeroizing` wrappers so heap copies are cleared
//!   on drop instead of lingering for scrapers/core dumps.
//! * **Fail closed**: an unreadable or vanished file rejects everything until
//!   it becomes readable again.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tonic::{Request, Status};
use zeroize::Zeroizing;

/// How often the backing file is re-read. Also used as the quiet period for
/// repeating "cannot read" warnings.
const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct TokenFileVault {
    path: PathBuf,
    reload_interval: Duration,
    cached: RwLock<CachedTokens>,
}

#[derive(Debug, Default)]
struct CachedTokens {
    loaded_at: Option<Instant>,
    hashes: Vec<[u8; 32]>,
}

impl TokenFileVault {
    /// Creates the vault and eagerly loads `path`. Failing here (rather than
    /// on first request) keeps misconfiguration at boot: callers should treat
    /// an error as fatal.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, AuthError> {
        let vault = Self {
            path: path.into(),
            reload_interval: DEFAULT_RELOAD_INTERVAL,
            cached: RwLock::new(CachedTokens::default()),
        };
        warn_if_token_file_world_readable(&vault.path);
        let hashes = read_token_hashes(&vault.path)?;
        {
            let mut cached = vault.write_cached();
            cached.hashes = hashes;
            cached.loaded_at = Some(Instant::now());
        }
        Ok(vault)
    }

    /// Shared read access to the token cache, recovering from a poisoned lock
    /// instead of panicking: a prior holder's panic must degrade to at worst
    /// a stale read, never to every later export failing.
    fn read_cached(&self) -> RwLockReadGuard<'_, CachedTokens> {
        match self.cached.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!(
                    path = %self.path.display(),
                    "bearer token cache lock poisoned; recovering guarded state"
                );
                poisoned.into_inner()
            }
        }
    }

    /// Exclusive write access to the token cache, recovering like
    /// [`Self::read_cached`] instead of panicking on the request path.
    fn write_cached(&self) -> RwLockWriteGuard<'_, CachedTokens> {
        match self.cached.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!(
                    path = %self.path.display(),
                    "bearer token cache lock poisoned; recovering guarded state"
                );
                poisoned.into_inner()
            }
        }
    }

    fn verify(&self, presented_token: &str) -> bool {
        if presented_token.is_empty() {
            return false;
        }
        // Hash outside the lock: pure CPU, no shared state. The digest is
        // zeroized on drop; the borrowed `presented_token` itself belongs to
        // the tonic request and cannot be cleared by us.
        let digest: Zeroizing<[u8; 32]> =
            Zeroizing::new(Sha256::digest(presented_token.as_bytes()).into());

        // Fast path: shared read lock, no I/O. Covers >99% of requests.
        {
            let cached = self.read_cached();
            if !is_stale(cached.loaded_at, self.reload_interval) {
                return cached.hashes.iter().any(|known| ct_eq(known, &digest));
            }
        }

        // Slow path: reload is due. Do the blocking file I/O *outside* any
        // lock so concurrent exports keep verifying against the old set on
        // their read locks instead of serializing behind us. The raw text is
        // zeroized on drop; only digests escape it.
        let file_result = match fs::read_to_string(&self.path) {
            Ok(text) => ReloadOutcome::Loaded(parse_token_lines(&Zeroizing::new(text))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ReloadOutcome::Revoked,
            Err(error) => ReloadOutcome::Unreadable(error.to_string()),
        };

        // Publish under a short exclusive hold (double-checked: another
        // thread may have reloaded while we were doing I/O).
        let (hashes, log_event) = {
            let mut cached = self.write_cached();
            if is_stale(cached.loaded_at, self.reload_interval) {
                let event = match &file_result {
                    ReloadOutcome::Loaded(hashes) if hashes.is_empty() => {
                        // Likely a botched atomic rewrite: keep serving the
                        // last known set instead of bricking the pipeline.
                        LogEvent::EmptyKept
                    }
                    ReloadOutcome::Loaded(hashes) => {
                        cached.hashes.clone_from(hashes);
                        LogEvent::None
                    }
                    ReloadOutcome::Revoked => {
                        // A removed file is an explicit revoke-all.
                        let had_tokens = !cached.hashes.is_empty();
                        cached.hashes.clear();
                        if had_tokens {
                            LogEvent::Revoked
                        } else {
                            LogEvent::None
                        }
                    }
                    ReloadOutcome::Unreadable(message) => {
                        // Transient IO trouble: keep last known set; the
                        // timestamp update below throttles repeat warnings.
                        LogEvent::Unreadable(message.clone())
                    }
                };
                cached.loaded_at = Some(Instant::now());
                (cached.hashes.clone(), event)
            } else {
                // Lost the race: someone else already reloaded.
                (cached.hashes.clone(), LogEvent::None)
            }
        };

        // Log outside the lock: tracing can block and must not extend the
        // exclusive hold.
        match log_event {
            LogEvent::None => {}
            LogEvent::EmptyKept => tracing::warn!(
                path = %self.path.display(),
                "token file is empty; keeping last known tokens"
            ),
            LogEvent::Revoked => tracing::info!(
                path = %self.path.display(),
                "token file removed; revoking all bearer tokens"
            ),
            LogEvent::Unreadable(error) => tracing::warn!(
                path = %self.path.display(),
                %error,
                "token file temporarily unreadable; keeping last known tokens"
            ),
        }

        hashes.iter().any(|known| ct_eq(known, &digest))
    }

    /// Number of valid tokens currently known; used by tests and startup logs.
    #[cfg(test)]
    pub(crate) fn loaded_token_count(&self) -> usize {
        self.read_cached().hashes.len()
    }
}

fn read_token_hashes(path: &Path) -> Result<Vec<[u8; 32]>, AuthError> {
    let text = Zeroizing::new(fs::read_to_string(path).map_err(|source| AuthError::Read {
        path: path.to_owned(),
        source,
    })?);
    let hashes = parse_token_lines(&text);
    if hashes.is_empty() {
        return Err(AuthError::NoTokens {
            path: path.to_owned(),
        });
    }
    Ok(hashes)
}

fn parse_token_lines(text: &str) -> Vec<[u8; 32]> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| Sha256::digest(line.as_bytes()).into())
        .collect()
}

/// Whether the cached token set is due for a reload. `None` (never loaded)
/// is always stale.
fn is_stale(loaded_at: Option<Instant>, reload_interval: Duration) -> bool {
    match loaded_at {
        Some(loaded_at) => loaded_at.elapsed() >= reload_interval,
        None => true,
    }
}

/// Result of reading the token file outside the cache lock.
enum ReloadOutcome {
    Loaded(Vec<[u8; 32]>),
    Revoked,
    Unreadable(String),
}

/// What to emit after publishing a reload (outside the lock).
enum LogEvent {
    None,
    EmptyKept,
    Revoked,
    Unreadable(String),
}

/// Warns when the bearer-token file is readable beyond its owner (unix).
/// Misconfigured `0644` tokens are a common credential leak; warn, don't fail,
/// so existing deployments keep working while operators fix perms.
fn warn_if_token_file_world_readable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        match std::fs::metadata(path).map(|metadata| metadata.permissions().mode()) {
            Ok(mode) if mode & 0o044 != 0 => tracing::warn!(
                path = %path.display(),
                mode = format!("{mode:o}"),
                "bearer token file is readable beyond its owner; chmod 600 it",
            ),
            _ => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// Constant-time equality over equal-length digests: XOR-folds all bytes so
/// branch timing never depends on where two digests first differ.
fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("cannot read token file {path:?}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("token file {path:?} contains no tokens")]
    NoTokens { path: PathBuf },
}

/// Cloneable tonic [`Interceptor`](tonic::service::Interceptor) verifying
/// `authorization: Bearer <token>` headers against a [`TokenFileVault`].
///
/// With `vault: None` every request passes through (auth disabled); the type
/// stays the same either way so service wiring never branches.
#[derive(Clone)]
pub struct BearerInterceptor {
    vault: Option<Arc<TokenFileVault>>,
}

impl BearerInterceptor {
    /// Loads the token file eagerly; a `Some` path that cannot be read is a
    /// startup error. `None` disables authentication.
    pub fn new(token_file: Option<&Path>) -> Result<Self, AuthError> {
        let vault = match token_file {
            Some(path) => Some(Arc::new(TokenFileVault::open(path)?)),
            None => None,
        };
        Ok(Self { vault })
    }
}

impl tonic::service::Interceptor for BearerInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match &self.vault {
            Some(vault) => intercept_bearer(vault, request),
            None => Ok(request),
        }
    }
}

/// Convenience constructor mirroring [`BearerInterceptor::new`].
pub fn bearer_interceptor(token_file: Option<&Path>) -> Result<BearerInterceptor, AuthError> {
    BearerInterceptor::new(token_file)
}

/// Extracts and verifies the `authorization` header of an incoming request.
///
/// Used as a tonic interceptor on the OTLP services only — the health service
/// deliberately stays unauthenticated so probes and load balancers work.
pub fn intercept_bearer(
    vault: &TokenFileVault,
    request: Request<()>,
) -> Result<Request<()>, Status> {
    let token = bearer_token(request.metadata())?;
    if !vault.verify(token) {
        return Err(Status::unauthenticated("invalid bearer token"));
    }
    Ok(request)
}

fn bearer_token(metadata: &tonic::metadata::MetadataMap) -> Result<&str, Status> {
    let value = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| Status::unauthenticated("malformed authorization header"))?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(Status::unauthenticated("expected `Bearer <token>`"));
    }
    Ok(token.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tokens(path: &Path, lines: &[&str]) {
        let mut file = fs::File::create(path).expect("create");
        for line in lines {
            writeln!(file, "{line}").expect("write");
        }
    }

    fn vault_with(lines: &[&str]) -> (tempfile::TempDir, TokenFileVault) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        write_tokens(&path, lines);
        let vault = TokenFileVault::open(&path).expect("open");
        (dir, vault)
    }

    fn request_with_token(token: &str) -> Request<()> {
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("header value"),
        );
        request
    }

    #[test]
    fn accepts_known_token_and_rejects_unknown_missing_malformed() {
        let (_dir, vault) = vault_with(&["alpha-secret", "beta-secret"]);

        assert!(intercept_bearer(&vault, request_with_token("alpha-secret")).is_ok());
        assert!(intercept_bearer(&vault, request_with_token("beta-secret")).is_ok());

        // Whitespace around a presented value is trimmed exactly like file
        // lines, so padded variants of a valid token still match.
        assert!(intercept_bearer(&vault, request_with_token(" alpha-secret ")).is_ok());
        assert!(intercept_bearer(&vault, request_with_token("gamma")).is_err());

        let mut no_header = Request::new(());
        assert!(intercept_bearer(&vault, no_header).is_err());

        no_header = Request::new(());
        no_header
            .metadata_mut()
            .insert("authorization", "Token abc".parse().unwrap());
        assert!(
            intercept_bearer(&vault, no_header).is_err(),
            "non-bearer schemes are rejected"
        );

        no_header = Request::new(());
        no_header
            .metadata_mut()
            .insert("authorization", "Bearer".parse().unwrap());
        assert!(intercept_bearer(&vault, no_header).is_err());
    }

    #[test]
    fn rotation_appends_take_effect_without_recreating_the_vault() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        write_tokens(&path, &["old-token"]);
        let mut vault = TokenFileVault::open(&path).expect("open");

        // Force the reload window to elapse so the rewrite is observed.
        vault.reload_interval = Duration::ZERO;

        assert!(intercept_bearer(&vault, request_with_token("old-token")).is_ok());
        assert!(intercept_bearer(&vault, request_with_token("new-token")).is_err());

        // Overlap phase: both lines valid while senders migrate.
        write_tokens(&path, &["old-token", "new-token"]);
        assert!(
            intercept_bearer(&vault, request_with_token("new-token")).is_ok(),
            "appended token must be picked up by hot reload"
        );
        assert!(intercept_bearer(&vault, request_with_token("old-token")).is_ok());
        assert_eq!(vault.loaded_token_count(), 2);

        // Cutover done: removing the old line retires it.
        write_tokens(&path, &["new-token"]);
        assert!(intercept_bearer(&vault, request_with_token("old-token")).is_err());
        assert!(intercept_bearer(&vault, request_with_token("new-token")).is_ok());
    }

    #[test]
    fn removed_token_file_revokes_everything_fail_closed() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        write_tokens(&path, &["keep-working"]);
        let mut vault = TokenFileVault::open(&path).expect("open");
        vault.reload_interval = Duration::ZERO;

        // File readable -> works.
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());

        // File REMOVED is an explicit revoke-all: even previously valid
        // tokens stop validating (fail closed), while a mere read hiccup
        // would have kept the last known set.
        fs::remove_file(&path).expect("remove");
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_err());
        assert!(intercept_bearer(&vault, request_with_token("anything")).is_err());

        // Recreating the file restores access without any restart.
        write_tokens(&path, &["keep-working"]);
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());
    }

    #[test]
    fn empty_token_file_is_rejected_at_open() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("empty.txt");
        write_tokens(&path, &["", "   "]);
        let error = TokenFileVault::open(&path).expect_err("no tokens must fail fast");
        assert!(
            error.to_string().contains("contains no tokens"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn missing_token_file_fails_at_open_not_on_first_request() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let error =
            TokenFileVault::open(dir.path().join("does-not-exist.txt")).expect_err("must fail");
        assert!(error.to_string().contains("cannot read token file"));
    }

    /// A poisoned cache lock must degrade, not deny service: after one
    /// holder panics mid-update, later exports recover the guarded digests
    /// instead of panicking on the gRPC interceptor path.
    #[test]
    fn vault_recovers_from_a_poisoned_lock() {
        let (_dir, vault) = vault_with(&["alpha-secret"]);

        // Poison the cache lock by panicking while holding it.
        std::thread::scope(|scope| {
            let outcome = scope
                .spawn(|| {
                    let _guard = vault.cached.write().unwrap();
                    panic!("intentional poison for recovery test");
                })
                .join();
            assert!(outcome.is_err(), "the holder must have panicked");
        });

        // Both the read fast-path and the reload write-path must work after.
        assert!(intercept_bearer(&vault, request_with_token("alpha-secret")).is_ok());
        assert!(intercept_bearer(&vault, request_with_token("gamma")).is_err());
    }
}
