# A commit linger window is opt-in and anchored

Group commit coalesces *consecutive* insert commands — commands that are
already queued when the writer collects. Under concurrent low-volume traffic
the writer keeps up, the queue is empty at collection time, and every
one-record command pays a full transaction (~45 µs fixed cost, one WAL append
and — under `synchronous = full` — one fsync) alone. The missing dimension is
temporal: request A and request B arriving 1 ms apart commit separately today.

`group_commit_linger` adds that dimension, defaulting to `0` so nothing
changes for existing deployments. When armed, an insert group that drained the
queue under budget waits inside a fixed window for straggler commands instead
of committing immediately: the deadline is set once, when the group opens, and
is never reset by later arrivals — the same anchoring rule the insert batcher
applies to its record-age deadline, and for the same reason: continuous
traffic must not be able to extend a wait indefinitely. The window only ever
delays a *commit*, so the added latency lands exclusively on
`durability = "commit"` acknowledgements (enqueue acks never wait for the
transaction); the record budget, the first non-insert command (flush,
maintenance, shutdown) and a queue disconnect all close a group immediately, so
barriers keep their ordering and the shutdown drain never waits out an open
window.

The window lives where the rest of the group policy already lives:
`collect_group` drains first (a backlog still never waits) and only then
awaits, through a `PullMode` that keeps the channel mapping in the writer.
`MAX_GROUP_COMMIT_LINGER = 1 s` is the storage crate's own ceiling: 200x the
largest benchmarked value and orders of magnitude below every watchdog
threshold, so a mid-group wait can never be mistaken for a stalled pipeline
(`in_flight` already counts as pending work, which keeps the leak detector
off).

## Considered options

- **The wait stage in the writer, next to the channel** (collect
  non-blocking, then a second `linger_for_more` loop pushing into the group):
  rejected as temporal decomposition. "When a group closes" would live in two
  modules — budget and boundaries in `transaction_group`, the deadline in
  `writer` — and an edit to one rule would not see the other.
- **Linger in the insert batcher** (hold emitted batches briefly): rejected as
  the wrong owner. It would delay record batching, not transaction grouping;
  tickets and segments attach at batch emission, and the batcher neither knows
  the writer's record budget nor can see barriers that arrive behind it. It
  would also be a second temporal knob next to `max_batch_age` at the same
  layer.
- **A per-durability gate** (linger only groups carrying commit-awaited
  tickets): rejected. The ledger cannot answer that cheaply at group-open
  time, and enqueue-mode traffic still benefits from fewer commits; the
  knob's latency story already restricts itself to commit acks.

## Consequences

- Under low-volume concurrent traffic, commits (and fsyncs under
  `synchronous = full`) are amortised across everything arriving within the
  window; the price is commit-mode ack latency bounded by the window.
- Backlog behaviour is unchanged: a group that fills by budget never waits, so
  saturation throughput is untouched.
- `RawDuration` gains microsecond units (`"250us"`), so the TOML/env path can
  express sub-millisecond windows; fractional-millisecond durations that
  previously rounded up are now exact.
- The e2e harness gains `OTEL_SQLITE_E2E_GROUP_COMMIT_LINGER` and records the
  effective window in the JSON `configuration` block.

## Revisit when

A commit-ack p99 regression at the chosen window shows up in
`docs/performance.md`'s linger sweep, or a deployment wants windows beyond
1 s for very slow storage — that is the point to revisit both the ceiling and
whether the batcher's `max_batch_age` should shrink instead.
