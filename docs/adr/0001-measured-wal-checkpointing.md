# A checkpoint is a measurement, not a timer

`PRAGMA wal_checkpoint` answers with `(busy, log, done)`; the periodic tick
threw that row away and counted every attempt as a completed run. The scheduled
checkpoint is now gated by the writer's own state: it runs unless the commit
count is unchanged since the last attempt *and* that attempt returned
`busy == 0`. A blocked attempt is not skippable — a reader holding a snapshot
(a live `verify`) lets every frame fold but cannot reset the WAL, so SQLite
reports `busy = 1` with `done == log` and raises no error. Measured on this
build before the change: 37 frames folded, `busy = 1`, the 152 KB sidecar still
in place, and the old helper returning success. The skipped path is free because the writer is the only mutator, so a
zero commit delta proves `log` can only be zero. Each attempt records
`log - done` as `storage_wal_backlog_frames` with `log` alongside it, and
`busy == 0 && done == log` is the only thing that counts as folded. The
five-minute interval stays as a ceiling, not a promise: its job is the tail
after traffic stops, which SQLite's own autocheckpoint (1000 frames) does not
cover.

## Considered options

- **Bytes instead of frames** (the `-wal` sidecar size): rejected as the gate.
  It costs an extra `stat()` and it is the same number as `log`, which the
  pragma hands back free and authoritatively.
- **An adaptive cadence that fires early when the WAL grows**: deferred. It
  needs a control loop through a scheduler that is deliberately database-free,
  for a problem SQLite's autocheckpoint already handles during bursts. Ship the
  gauge first.
- **A second connection for PASSIVE checkpoints** (separate `ck_db`, admitted
  between write transactions): deferred, not rejected. It breaks the
  single-writer invariant and there is no evidence yet that checkpoints stall.

## Consequences

- A checkpoint is no longer evidence that the WAL is folded; callers that need
  the fold (size-quota eviction, shutdown, bootstrap) must read the report.
  `enforce_size_quota` stops its eviction loop when `busy != 0` instead of
  evicting another batch behind a WAL that never shrank.
- `maintenance_runs` counts attempts that did work, so a quiet database no
  longer reports a checkpoint every five minutes.
- A stuck backlog (non-zero while the writer is idle) becomes the signal that a
  long-lived reader is pinning the WAL, instead of silent WAL growth.
- When the fold is blocked, size-quota enforcement stops rather than deleting
  behind an unfolded WAL, and counts the deferral instead of hiding it.

## Revisit when

A `storage_checkpoint_duration` histogram (added with these gauges) shows p99
checkpoint duration above roughly 250 ms — 10x the 27 ms baseline recorded in
`docs/performance.md` — or the command queue crosses the heavy-maintenance
admission limit (1000) while a checkpoint is in flight. That is the point at
which page-copying stops being noise on the writer thread and a second
checkpoint connection is worth the invariant it costs. A non-zero backlog with
an idle writer is not that trigger: it means a reader is pinning the WAL, and a
second connection contends on the same file.
