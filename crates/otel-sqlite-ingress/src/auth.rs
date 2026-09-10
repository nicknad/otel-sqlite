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
//! * **Bounded parsing**: the file is read through a [`MAX_TOKEN_FILE_BYTES`]
//!   cap, lines past [`MAX_TOKEN_LINE_BYTES`] are skipped, and files holding
//!   more than [`MAX_TOKENS`] tokens are rejected, so a bloated token file
//!   cannot OOM the ingress on its reload loop.
//! * **Fail closed**: a removed file revokes everything immediately. A file
//!   that stays unreadable, over-limit, or persistently empty fails closed
//!   once [`MAX_STALE_TOKEN_AGE`] of grace has passed since the last good
//!   reload — a single bad read (slow disk, mid-rewrite observation) keeps
//!   serving the last known set, but an attacker pinning the file in a bad
//!   state cannot defer revocation forever.

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

/// Maximum accepted token-file size in bytes (M3). The file is read through
/// `take(MAX + 1)` and pre-checked via metadata, so a file grown past this —
/// accidentally or maliciously — is rejected without ever buffering it fully.
const MAX_TOKEN_FILE_BYTES: u64 = 1024 * 1024;
/// Maximum tokens accepted from one file (M3). Past this the reload is
/// rejected (startup fails instead), bounding per-reload hashing and the
/// `hashes` clone held across the verify path.
const MAX_TOKENS: usize = 10_000;
/// Maximum bytes of one token line (M3). Longer lines are skipped: bearer
/// tokens are short random strings, so an over-long line is garbage, not a
/// credential anyone could type into an `authorization` header.
const MAX_TOKEN_LINE_BYTES: usize = 4_096;
/// Grace period for a stale token set (M3). While reloads keep failing
/// (unreadable, over-limit) the last known set keeps serving; once no good
/// reload has happened for this long, the vault fails closed (rejects
/// everything) until the file reads cleanly again. Bounds how long an
/// attacker holding the file in a bad state can defer a revocation.
const MAX_STALE_TOKEN_AGE: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub struct TokenFileVault {
    path: PathBuf,
    reload_interval: Duration,
    max_stale_age: Duration,
    cached: RwLock<CachedTokens>,
}

#[derive(Debug, Default)]
struct CachedTokens {
    loaded_at: Option<Instant>,
    hashes: Vec<[u8; 32]>,
    /// When consecutive failing reloads started (`None` = last reload was
    /// good). Compared against `max_stale_age` for fail-closed expiry.
    unhealthy_since: Option<Instant>,
    /// Consecutive reloads that observed an empty file. The first is treated
    /// as a mid-rewrite observation (keep serving); a persistent empty file
    /// revokes, matching `open` refusing an empty file at startup.
    consecutive_empty: u32,
}

impl TokenFileVault {
    /// Creates the vault and eagerly loads `path`. Failing here (rather than
    /// on first request) keeps misconfiguration at boot: callers should treat
    /// an error as fatal.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, AuthError> {
        let vault = Self {
            path: path.into(),
            reload_interval: DEFAULT_RELOAD_INTERVAL,
            max_stale_age: MAX_STALE_TOKEN_AGE,
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
        let file_result = match read_token_file(&self.path) {
            Ok(parsed) => ReloadOutcome::Loaded(parsed),
            Err(LoadError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                ReloadOutcome::Revoked
            }
            Err(LoadError::Io(error)) => ReloadOutcome::Unreadable(error.to_string()),
            Err(LoadError::TooLarge(bytes)) => ReloadOutcome::Invalid(format!(
                "token file is {bytes} bytes, limit is {MAX_TOKEN_FILE_BYTES}"
            )),
            Err(LoadError::TooManyTokens(count)) => ReloadOutcome::Invalid(format!(
                "token file holds {count} tokens, limit is {MAX_TOKENS}"
            )),
        };

        // Publish under a short exclusive hold (double-checked: another
        // thread may have reloaded while we were doing I/O).
        let (hashes, log_event) = {
            let mut cached = self.write_cached();
            if is_stale(cached.loaded_at, self.reload_interval) {
                let now = Instant::now();
                let event = match &file_result {
                    ReloadOutcome::Loaded(parsed) if parsed.hashes.is_empty() => {
                        cached.consecutive_empty += 1;
                        if cached.consecutive_empty >= 2 {
                            // Still empty on the next reload: not a rewrite
                            // race but a real state — revoke, matching the
                            // startup refusal of empty files.
                            cached.hashes.clear();
                            cached.unhealthy_since = None;
                            LogEvent::EmptyRevoked
                        } else {
                            // Likely a botched atomic rewrite observed
                            // mid-flight: keep serving the last known set
                            // this once instead of bricking the pipeline.
                            LogEvent::EmptyKept
                        }
                    }
                    ReloadOutcome::Loaded(parsed) => {
                        cached.hashes.clone_from(&parsed.hashes);
                        cached.unhealthy_since = None;
                        cached.consecutive_empty = 0;
                        if parsed.skipped_long_lines > 0 {
                            LogEvent::SkippedLong(parsed.skipped_long_lines)
                        } else {
                            LogEvent::None
                        }
                    }
                    ReloadOutcome::Revoked => {
                        // A removed file is an explicit revoke-all.
                        let had_tokens = !cached.hashes.is_empty();
                        cached.hashes.clear();
                        cached.unhealthy_since = None;
                        cached.consecutive_empty = 0;
                        if had_tokens {
                            LogEvent::Revoked
                        } else {
                            LogEvent::None
                        }
                    }
                    ReloadOutcome::Unreadable(message) | ReloadOutcome::Invalid(message) => {
                        // Transient trouble (or an over-limit file): keep the
                        // last known set, but only within the grace period —
                        // past it, fail closed so a pinned-bad file cannot
                        // defer revocation forever.
                        let since = *cached.unhealthy_since.get_or_insert(now);
                        if now.saturating_duration_since(since) >= self.max_stale_age {
                            cached.hashes.clear();
                            LogEvent::StaleExpired(message.clone())
                        } else if matches!(file_result, ReloadOutcome::Invalid(_)) {
                            LogEvent::Invalid(message.clone())
                        } else {
                            // The timestamp update below throttles repeat
                            // warnings.
                            LogEvent::Unreadable(message.clone())
                        }
                    }
                };
                cached.loaded_at = Some(now);
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
                "token file is empty; keeping last known tokens this once"
            ),
            LogEvent::EmptyRevoked => tracing::warn!(
                path = %self.path.display(),
                "token file is persistently empty; revoking all bearer tokens"
            ),
            LogEvent::Revoked => tracing::info!(
                path = %self.path.display(),
                "token file removed; revoking all bearer tokens"
            ),
            LogEvent::SkippedLong(count) => tracing::warn!(
                path = %self.path.display(),
                count,
                "token file holds over-long lines; skipped them"
            ),
            LogEvent::Unreadable(error) => tracing::warn!(
                path = %self.path.display(),
                %error,
                "token file temporarily unreadable; keeping last known tokens"
            ),
            LogEvent::Invalid(error) => tracing::warn!(
                path = %self.path.display(),
                %error,
                "token file over limits; keeping last known tokens until the grace period expires"
            ),
            LogEvent::StaleExpired(error) => tracing::warn!(
                path = %self.path.display(),
                %error,
                "token file unhealthy past the grace period; revoking all bearer tokens"
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
    match read_token_file(path) {
        Ok(parsed) => {
            if parsed.skipped_long_lines > 0 {
                tracing::warn!(
                    path = %path.display(),
                    count = parsed.skipped_long_lines,
                    "token file holds over-long lines; skipped them",
                );
            }
            if parsed.hashes.is_empty() {
                return Err(AuthError::NoTokens {
                    path: path.to_owned(),
                });
            }
            Ok(parsed.hashes)
        }
        Err(LoadError::Io(source)) => Err(AuthError::Read {
            path: path.to_owned(),
            source,
        }),
        Err(LoadError::TooLarge(bytes)) => Err(AuthError::TooLarge {
            path: path.to_owned(),
            bytes,
        }),
        Err(LoadError::TooManyTokens(count)) => Err(AuthError::TooManyTokens {
            path: path.to_owned(),
            count,
        }),
    }
}

/// What one bounded read of the token file produced.
#[derive(Debug)]
struct ParsedTokens {
    hashes: Vec<[u8; 32]>,
    skipped_long_lines: usize,
}

/// Why a token-file read failed. `Io` covers ordinary read failures (its
/// `ErrorKind` tells a removed file apart from transient trouble); the other
/// variants are the M3 size/count bounds.
#[derive(Debug)]
enum LoadError {
    Io(std::io::Error),
    TooLarge(u64),
    TooManyTokens(usize),
}

/// Reads and parses the token file with hard resource bounds: the size is
/// pre-checked via metadata *and* the read itself goes through
/// `take(MAX + 1)`, so a file grown between the two (TOCTOU) still cannot
/// buffer past the cap.
fn read_token_file(path: &Path) -> Result<ParsedTokens, LoadError> {
    use std::io::Read as _;

    let file = fs::File::open(path).map_err(LoadError::Io)?;
    let size = file.metadata().map_or(0, |m| m.len());
    if size > MAX_TOKEN_FILE_BYTES {
        return Err(LoadError::TooLarge(size));
    }
    let mut text = Zeroizing::new(String::new());
    (&file)
        .take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(LoadError::Io)?;
    if text.len() as u64 > MAX_TOKEN_FILE_BYTES {
        return Err(LoadError::TooLarge(text.len() as u64));
    }
    parse_token_lines(&text)
}

fn parse_token_lines(text: &str) -> Result<ParsedTokens, LoadError> {
    let mut hashes = Vec::new();
    let mut skipped_long_lines = 0usize;
    let mut total = 0usize;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if line.len() > MAX_TOKEN_LINE_BYTES {
            skipped_long_lines += 1;
            continue;
        }
        total += 1;
        // Bound the hashing work itself: past the cap we only keep counting
        // so the error reports the real size.
        if hashes.len() < MAX_TOKENS {
            hashes.push(Sha256::digest(line.as_bytes()).into());
        }
    }
    if total > MAX_TOKENS {
        return Err(LoadError::TooManyTokens(total));
    }
    Ok(ParsedTokens {
        hashes,
        skipped_long_lines,
    })
}

/// Whether the cached token set is due for a reload. `None` (never loaded)
/// is always stale.
fn is_stale(loaded_at: Option<Instant>, reload_interval: Duration) -> bool {
    loaded_at.is_none_or(|loaded_at| loaded_at.elapsed() >= reload_interval)
}

/// Result of reading the token file outside the cache lock.
enum ReloadOutcome {
    Loaded(ParsedTokens),
    Revoked,
    Unreadable(String),
    Invalid(String),
}

/// What to emit after publishing a reload (outside the lock).
enum LogEvent {
    None,
    EmptyKept,
    EmptyRevoked,
    Revoked,
    SkippedLong(usize),
    Unreadable(String),
    Invalid(String),
    StaleExpired(String),
}

/// Warns when the bearer-token file is readable beyond its owner (unix).
/// Misconfigured `0644` tokens are a common credential leak; warn, don't fail,
/// so existing deployments keep working while operators fix perms.
fn warn_if_token_file_world_readable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(mode) = std::fs::metadata(path).map(|metadata| metadata.permissions().mode())
            && mode & 0o044 != 0
        {
            tracing::warn!(
                path = %path.display(),
                mode = format!("{mode:o}"),
                "bearer token file is readable beyond its owner; chmod 600 it",
            );
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
    #[error("token file {path:?} is too large ({bytes} bytes; limit is 1 MiB)")]
    TooLarge { path: PathBuf, bytes: u64 },
    #[error("token file {path:?} holds {count} tokens (limit is 10000)")]
    TooManyTokens { path: PathBuf, count: usize },
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
        let vault = token_file
            .map(TokenFileVault::open)
            .transpose()?
            .map(Arc::new);
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
    let token = token.trim();
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(Status::unauthenticated("expected `Bearer <token>`"));
    }
    Ok(token)
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

    #[test]
    fn oversized_token_file_is_rejected_at_open() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        fs::write(&path, vec![b'a'; 1024 * 1024 + 1]).expect("write");
        let error = TokenFileVault::open(&path).expect_err("over-limit file must fail fast");
        assert!(
            error.to_string().contains("too large"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn too_many_tokens_are_rejected_at_open() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        let mut file = fs::File::create(&path).expect("create");
        for i in 0..10_001 {
            writeln!(file, "token-{i}").expect("write");
        }
        drop(file);
        let error = TokenFileVault::open(&path).expect_err("over-limit count must fail fast");
        assert!(
            error.to_string().contains("10000"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn over_long_lines_are_skipped_not_hashed() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        let long = "x".repeat(5_000);
        write_tokens(&path, &["good-token", &long]);
        let vault = TokenFileVault::open(&path).expect("one good line suffices");
        assert_eq!(vault.loaded_token_count(), 1);
        assert!(intercept_bearer(&vault, request_with_token("good-token")).is_ok());
        assert!(intercept_bearer(&vault, request_with_token(&long)).is_err());
    }

    #[test]
    fn persistently_empty_file_revokes_after_one_grace_reload() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        write_tokens(&path, &["keep-working"]);
        let mut vault = TokenFileVault::open(&path).expect("open");
        vault.reload_interval = Duration::ZERO;

        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());

        // Truncated mid-rewrite: the first observation keeps serving.
        fs::write(&path, "").expect("truncate");
        assert!(
            intercept_bearer(&vault, request_with_token("keep-working")).is_ok(),
            "first empty observation is a possible rewrite race"
        );

        // Still empty on the next reload: not a race — revoke like startup.
        assert!(
            intercept_bearer(&vault, request_with_token("keep-working")).is_err(),
            "persistently empty file must revoke"
        );

        // A rewritten file restores access without any restart.
        write_tokens(&path, &["keep-working"]);
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());
    }

    #[test]
    fn unreadable_file_fails_closed_once_grace_expires() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("tokens.txt");
        write_tokens(&path, &["keep-working"]);
        let mut vault = TokenFileVault::open(&path).expect("open");
        vault.reload_interval = Duration::ZERO;

        // Swap the file for a directory: reads fail with a non-NotFound
        // error on every platform, i.e. the transient-unreadable path.
        fs::remove_file(&path).expect("remove");
        fs::create_dir(&path).expect("mkdir");

        // Within the grace period the last known set keeps serving.
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());

        // Past the grace period the vault fails closed instead of honoring
        // stale tokens forever.
        vault.max_stale_age = Duration::ZERO;
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_err());
        assert!(intercept_bearer(&vault, request_with_token("anything")).is_err());

        // A readable file again restores access without any restart.
        fs::remove_dir(&path).expect("rmdir");
        write_tokens(&path, &["keep-working"]);
        assert!(intercept_bearer(&vault, request_with_token("keep-working")).is_ok());
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
