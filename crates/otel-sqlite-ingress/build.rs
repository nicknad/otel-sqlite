use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    if env::var_os("PROTOC").is_none() {
        // SAFETY: single-threaded build script; `set_var` is only unsafe as of
        // edition 2024 because other threads may read the environment
        // concurrently, which cannot happen here. Required so prost-build's
        // tonic codegen picks up the vendored protoc binary.
        #[allow(unsafe_code)]
        // nosemgrep: rust.unsafe-block
        unsafe {
            env::set_var("PROTOC", &protoc);
        };
    }

    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");

    println!("cargo:rerun-if-changed={}", proto_root.display());

    tonic_prost_build::configure().compile_protos(
        &[
            proto_root.join("opentelemetry/proto/collector/logs/v1/logs_service.proto"),
            proto_root.join("opentelemetry/proto/collector/metrics/v1/metrics_service.proto"),
        ],
        &[proto_root],
    )?;

    Ok(())
}
