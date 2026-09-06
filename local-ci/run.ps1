# Local CI with act — runs `.forgejo/workflows/ci.yml` on this machine using
# the Docker daemon, exactly the pipeline the Forgejo runner would run.
#
# The wrapper first changes into this directory so `.actrc` / `.act.env` are
# read, then invokes `act -C <repo-root> -W <repo>/.forgejo/workflows`.
#
# Usage (PowerShell):
#   .\local-ci\run.ps1            # release-gate jobs (containerized, no dind)
#   .\local-ci\run.ps1 -All       # + docker jobs (needs host Docker daemon)
#   .\local-ci\run.ps1 -List      # list the workflows act would run
#   .\local-ci\run.ps1 -Act path\to\act.exe   # custom act binary
#
# Prerequisites: Docker Desktop running and the `act` binary on PATH
# (install: `winget install nektos.act` or scoop/choco).

[CmdletBinding()]
param(
    # Also run the Docker jobs (fuzz-smoke, crash-e2e-container,
    # container-smoke). These mount the host Docker daemon so `docker compose`
    # inside the job talks to the real daemon; requires Docker Desktop.
    [switch]$All,
    # Only list the workflows act can run for the push event, then exit.
    [switch]$List,
    # Path to the act binary (defaults to `act` on PATH).
    [string]$Act = "act"
)
$ErrorActionPreference = "Stop"

$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

# Change into local-ci so `.actrc` and `.act.env` are picked up by act.
Set-Location $PSScriptRoot

# The release-gate jobs all run in their own `container:` images (rust /
# semgrep) and need no Docker daemon inside the job container.
$Jobs = @("lint-and-test", "crash-recovery-release", "e2e-benchmark", "cargo-deny", "cargo-audit", "semgrep")
$SocketArgs = @()

if ($All) {
    $Jobs += @("fuzz-smoke", "crash-e2e-container", "container-smoke")
    # Docker Desktop's daemon is reachable through the named pipe; act mounts
    # it into the job container so `docker compose` works without dind.
    $SocketArgs = @("--container-daemon-socket", "//./pipe/docker_engine")
}

$Workflows = Join-Path $Repo ".forgejo\workflows"

if ($List) {
    & $Act -C $Repo -W $Workflows -l
    exit $LASTEXITCODE
}

# act takes a single job id per `-j` flag; build the repeated flags for the
# selected job set.
$JobArgs = @()
foreach ($job in $Jobs) {
    $JobArgs += @("-j", $job)
}

& $Act -C $Repo -W $Workflows @SocketArgs @JobArgs
exit $LASTEXITCODE