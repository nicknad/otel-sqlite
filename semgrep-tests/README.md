# semgrep self-tests

Fixture-based tests for the rules in [`.semgrep.yml`](../.semgrep.yml), run with
`semgrep test`. A fixture marks every line the rule is expected to match with
`// ruleid: <id>` and every near-miss it must *not* match with `// ok: <id>`;
`semgrep test` fails if an annotation is wrong in either direction.

```bash
# from the repository root
docker run --rm -v "$PWD:/src" -w /src/semgrep-tests semgrep/semgrep:1.176.0 \
  semgrep test --config ../.semgrep.yml .

# or, with semgrep installed locally
cd semgrep-tests && semgrep test --config ../.semgrep.yml .
```

The tests deliberately run with the working directory set to `semgrep-tests/`:
`rust.std-net-in-pipeline` is scoped by `paths.include`, and its fixtures live
under a mirrored `crates/otel-sqlite-ingress/src/` path so the include still
resolves. That mirror is also why the rule cannot simply be pointed at a
neutral fixture directory.

## What is covered

| fixture | rules |
|---|---|
| `rust-inline.rs` | `rust.dbg-macro`, `rust.todo-unimplemented`, `rust.unsafe-block`, `security.sql-format-string`, `generic.hardcoded-secret` |
| `crates/otel-sqlite-ingress/src/net.rs` | `rust.std-net-in-pipeline` (inside the include path) |
| `crates/otel-sqlite-storage/src/net.rs` | `rust.std-net-in-pipeline` negative control (outside the include path) |

## Known blind spot

`security.sql-format-string` is a syntactic rule: it sees `format!` written
*inside* the call. `let sql = format!(...); conn.prepare(&sql)` is not matched
and is not asserted here either way. If that shape is to be caught, the rule
needs taint mode or the codebase needs a rule that SQL text is only ever
constructed through the helpers in `otel-sqlite-storage`.
