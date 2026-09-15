//! `otel-sqlite gen-certs`: bootstrap a private CA plus server and client
//! certificates entirely in-house (no external authority involved).
//!
//! Output layout under `--out` (default `./certs`):
//!
//! ```text
//! ca.pem / ca.key          private root CA (keep ca.key safe and offline!)
//! server.pem / server.key  TLS identity for the otel-sqlite listener;
//!                          carries both serverAuth and clientAuth so the
//!                          container HEALTHCHECK can authenticate as this
//!                          node when mTLS is required
//! <client>.pem/.key        one pair per `--client NAME`
//! ```
//!
//! Server SANs cover every `--host` value plus `localhost`, `127.0.0.1` and
//! `::1` so local probes verify out of the box.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// One parsed command line for the subcommand.
#[derive(Debug, Default)]
pub struct GenCertsArgs {
    pub hosts: Vec<String>,
    pub clients: Vec<String>,
    pub out_dir: PathBuf,
    /// Rotate the CA even when `ca.pem`/`ca.key` already exist. Without this,
    /// an existing CA is reused and only the missing/requested certificates
    /// are (re)issued.
    pub force: bool,
}

/// Parses `["--host","a","--client","b",..]`; errors on unknown/missing values.
pub fn parse_args(args: &[String]) -> Result<GenCertsArgs> {
    let mut parsed = GenCertsArgs {
        out_dir: PathBuf::from("certs"),
        ..Default::default()
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == "--force" {
            parsed.force = true;
            continue;
        }
        let value = iter
            .next()
            .with_context(|| format!("flag {flag} is missing its value"))?;
        match flag.as_str() {
            "--host" => parsed.hosts.push(value.clone()),
            "--client" => parsed.clients.push(value.clone()),
            "--out" => parsed.out_dir = PathBuf::from(value),
            other => bail!("unknown flag `{other}` (expected --host, --client, --out, --force)"),
        }
    }
    if parsed.hosts.is_empty() {
        parsed.hosts.push("localhost".to_owned());
    }
    if parsed.clients.is_empty() {
        parsed.clients.push("otel-client".to_owned());
    }
    Ok(parsed)
}

pub fn run(args: &GenCertsArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("cannot create {}", args.out_dir.display()))?;

    // Private root CA. Reuse the existing CA unless `--force` rotates it:
    // silently replacing a CA invalidates every client certificate issued
    // from it, so the documented rotation flow (`gen-certs --client NAME`
    // against the same output directory) must keep it stable.
    let ca_key_path = args.out_dir.join("ca.key");
    let ca_pem_path = args.out_dir.join("ca.pem");
    let reuse_ca = !args.force && ca_key_path.is_file() && ca_pem_path.is_file();
    let ca_key = if reuse_ca {
        let pem = std::fs::read_to_string(&ca_key_path)
            .with_context(|| format!("read existing CA key {}", ca_key_path.display()))?;
        rcgen::KeyPair::from_pem(&pem)
            .with_context(|| format!("parse existing CA key {}", ca_key_path.display()))?
    } else {
        rcgen::KeyPair::generate().context("generate CA key")?
    };
    let ca_cert = make_ca_params()?
        .self_signed(&ca_key)
        .context("self-sign CA certificate")?;
    if reuse_ca {
        println!("reusing existing CA {}", ca_pem_path.display());
    } else {
        write_pem(&ca_pem_path, &ca_cert.pem())?;
        write_pem(&ca_key_path, &ca_key.serialize_pem())?;
    }

    // Server certificate: every host SAN + loopback, dual EKU so it can also
    // act as mTLS client identity for the built-in healthcheck probe. Keep an
    // existing identity unless the CA was (re)generated or `--force` asks for
    // a rotation.
    let server_pem_path = args.out_dir.join("server.pem");
    let server_key_path = args.out_dir.join("server.key");
    if !args.force && reuse_ca && server_pem_path.is_file() && server_key_path.is_file() {
        println!(
            "keeping existing server certificate {} (use --force to rotate)",
            server_pem_path.display()
        );
    } else {
        let server_key = rcgen::KeyPair::generate().context("generate server key")?;
        let mut server_params = rcgen::CertificateParams::new(vec![]).context("server params")?;
        server_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "otel-sqlite server");
        server_params.subject_alt_names = server_sans(&args.hosts)?;
        server_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        server_params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .context("sign server certificate")?;
        write_pem(&server_pem_path, &server_cert.pem())?;
        write_pem(&server_key_path, &server_key.serialize_pem())?;
    }

    for client in &args.clients {
        sanitize_client_name(client)?;
        let key = rcgen::KeyPair::generate().context("generate client key")?;
        let mut params = rcgen::CertificateParams::new(vec![]).context("client params")?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, client.clone());
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        let cert = params
            .signed_by(&key, &ca_cert, &ca_key)
            .context("sign client certificate")?;
        write_pem(&args.out_dir.join(format!("{client}.pem")), &cert.pem())?;
        write_pem(
            &args.out_dir.join(format!("{client}.key")),
            &key.serialize_pem(),
        )?;
        println!("wrote client certificate {client}.pem / {client}.key");
    }

    println!(
        "wrote CA + server certificates to {}",
        args.out_dir.display()
    );
    println!("NOTE: ca.key signs your whole PKI - move it somewhere safe/offline.");
    println!(
        "server config:\n[tls]\ncert = \"{}\"\nkey = \"{}\"\nclient_ca = \"{}\"",
        args.out_dir.join("server.pem").display(),
        args.out_dir.join("server.key").display(),
        args.out_dir.join("ca.pem").display(),
    );
    if reuse_ca {
        println!(
            "NOTE: reused the existing CA; `--force` rotates the CA and server identity \
             (this invalidates every certificate previously issued from it)."
        );
    }
    Ok(())
}

/// Parameters of the in-house CA. Also used to reconstruct the issuer when an
/// existing `ca.key` is reused, so the subject/constraints must stay stable.
fn make_ca_params() -> Result<rcgen::CertificateParams> {
    let mut ca_params = rcgen::CertificateParams::new(vec![]).context("CA params")?;
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "otel-sqlite in-house CA");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    Ok(ca_params)
}

fn server_sans(hosts: &[String]) -> Result<Vec<rcgen::SanType>> {
    let mut sans = Vec::new();
    for host in hosts {
        if let Ok(ip) = host.parse::<IpAddr>() {
            sans.push(rcgen::SanType::IpAddress(ip));
        } else {
            sans.push(rcgen::SanType::DnsName(
                host.clone()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid DNS name `{host}`"))?,
            ));
        }
    }
    if !hosts.contains(&"localhost".to_owned()) {
        sans.push(rcgen::SanType::DnsName(
            "localhost"
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid DNS name `localhost`"))?,
        ));
    }
    for ip in ["127.0.0.1", "::1"] {
        let ip: IpAddr = ip.parse().expect("literal IP");
        if !hosts.contains(&ip.to_string()) {
            sans.push(rcgen::SanType::IpAddress(ip));
        }
    }
    Ok(sans)
}

fn sanitize_client_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !name.starts_with('.');
    if !ok {
        bail!("invalid client name `{name}` (use ASCII letters, digits, '-', '_', '.')")
    }
    // Windows resolves these stems to devices, so `NUL.pem` silently writes
    // nowhere on Windows. Reject them everywhere for portability.
    let stem = name.split('.').next().unwrap_or(name).to_ascii_lowercase();
    let reserved = matches!(
        stem.as_str(),
        "con"
            | "prn"
            | "aux"
            | "nul"
            | "com1"
            | "com2"
            | "com3"
            | "com4"
            | "com5"
            | "com6"
            | "com7"
            | "com8"
            | "com9"
            | "lpt1"
            | "lpt2"
            | "lpt3"
            | "lpt4"
            | "lpt5"
            | "lpt6"
            | "lpt7"
            | "lpt8"
            | "lpt9"
    );
    if reserved {
        bail!("invalid client name `{name}` (reserved Windows device name)");
    }
    Ok(())
}

fn write_pem(path: &Path, contents: &str) -> Result<()> {
    std::fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    if path.extension().is_some_and(|ext| ext == "key") {
        restrict_key_file(path)?;
    }
    println!("wrote {}", path.display());
    Ok(())
}

/// Owner-only restriction for private keys: `0o600` on unix, `icacls`
/// hardening on Windows (best-effort warn, never fails generation).
#[allow(clippy::unnecessary_wraps)] // Windows/bare-metal arms always succeed by design
fn restrict_key_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restrict permissions on {}", path.display()))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        let user = std::env::var("USERNAME").unwrap_or_default();
        if user.is_empty() {
            eprintln!(
                "warning: cannot determine USERNAME; key {} may inherit permissive ACLs; restrict it manually",
                path.display()
            );
            return Ok(());
        }
        let status = std::process::Command::new("icacls")
            .arg(path)
            .arg("/inheritance:r")
            .arg("/grant:r")
            .arg(format!("{user}:F"))
            .arg("*S-1-5-18:F")
            .arg("*S-1-5-32-544:F")
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!(
                    "warning: icacls exited {status} for {}; restrict key ACLs manually",
                    path.display()
                );
            }
            Err(error) => {
                eprintln!(
                    "warning: icacls unavailable ({error}); key {} may inherit permissive ACLs",
                    path.display()
                );
            }
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg_vec(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }

    #[test]
    fn parse_defaults_apply_when_no_flags_given() {
        let parsed = parse_args(&[]).expect("parses");
        assert_eq!(parsed.hosts, vec!["localhost".to_owned()]);
        assert_eq!(parsed.clients, vec!["otel-client".to_owned()]);
        assert_eq!(parsed.out_dir, PathBuf::from("certs"));
    }

    #[test]
    fn parse_rejects_unknown_flags_and_dangling_values() {
        assert!(parse_args(&arg_vec(&["--bogus", "x"])).is_err());
        assert!(parse_args(&arg_vec(&["--out"])).is_err(), "dangling --out");
    }

    #[test]
    fn generate_writes_parseable_pems_with_expected_files() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let args = GenCertsArgs {
            hosts: vec!["localhost".to_owned()],
            clients: vec!["collector-a".to_owned(), "collector.b".to_owned()],
            out_dir: dir.path().to_path_buf(),
            force: false,
        };
        run(&args).expect("generation succeeds");

        for name in [
            "ca.pem",
            "ca.key",
            "server.pem",
            "server.key",
            "collector-a.pem",
            "collector-a.key",
            "collector.b.pem",
            "collector.b.key",
        ] {
            let path = dir.path().join(name);
            let text =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name} must exist: {e}"));
            assert!(text.contains("-----BEGIN "), "{name} must be PEM");
        }

        // Invalid client names are rejected before any signing happens.
        let bad = GenCertsArgs {
            clients: vec!["../evil".to_owned()],
            ..GenCertsArgs::default()
        };
        assert!(run(&bad).is_err(), "path traversal names must be rejected");
    }

    #[test]
    fn rerun_reuses_existing_ca_and_keeps_server_identity() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let first = GenCertsArgs {
            hosts: vec!["localhost".to_owned()],
            clients: vec!["first".to_owned()],
            out_dir: dir.path().to_path_buf(),
            force: false,
        };
        run(&first).expect("first generation");
        let ca_before = std::fs::read_to_string(dir.path().join("ca.pem")).expect("ca.pem");
        let server_before =
            std::fs::read_to_string(dir.path().join("server.pem")).expect("server.pem");

        // Rotation = reissue a client certificate without touching the CA.
        let second = GenCertsArgs {
            hosts: vec!["localhost".to_owned()],
            clients: vec!["second".to_owned()],
            out_dir: dir.path().to_path_buf(),
            force: false,
        };
        run(&second).expect("second generation reuses the CA");
        assert_eq!(
            ca_before,
            std::fs::read_to_string(dir.path().join("ca.pem")).expect("ca.pem"),
            "re-running must not replace the CA"
        );
        assert_eq!(
            server_before,
            std::fs::read_to_string(dir.path().join("server.pem")).expect("server.pem"),
            "re-running must not replace the server identity"
        );
        assert!(dir.path().join("first.pem").is_file());
        assert!(dir.path().join("second.pem").is_file());

        // `--force` is the explicit destructive rotation.
        let forced = GenCertsArgs {
            hosts: vec!["localhost".to_owned()],
            clients: vec!["third".to_owned()],
            out_dir: dir.path().to_path_buf(),
            force: true,
        };
        run(&forced).expect("forced rotation");
        assert_ne!(
            ca_before,
            std::fs::read_to_string(dir.path().join("ca.pem")).expect("ca.pem"),
            "--force must rotate the CA"
        );
    }

    #[test]
    fn rejects_reserved_windows_device_names() {
        assert!(sanitize_client_name("con").is_err());
        assert!(sanitize_client_name("NUL.pem").is_err());
        assert!(sanitize_client_name("com1").is_err());
        assert!(sanitize_client_name("collector-a").is_ok());
    }
}
