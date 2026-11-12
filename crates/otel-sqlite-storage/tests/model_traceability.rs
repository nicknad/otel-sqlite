// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Drift check for `docs/models/traceability.tsv` and the generated
//! `docs/models/fidelity-matrix.md`.
//!
//! This test is deliberately cross-layer: it is the only place that can see the
//! vendored protos, the domain model and the migrated schema at once, so it
//! verifies the five things a hand-written matrix cannot:
//!
//! 1. every field of every consumed OTLP message is classified,
//! 2. every row names a field the vendored protos actually declare,
//! 3. every stored column of the migrated schema is produced by some row,
//! 4. every read-side view column is accounted for,
//! 5. the `loss_counter` values match the counters the ingress really emits.
//!
//! Regenerate the matrix after editing the TSV:
//!
//! ```text
//! UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;

use otel_sqlite_storage::migrate_up_to;
use rusqlite::Connection;

/// Newest migration this binary knows; the matrix describes the schema after
/// all of them have been applied. Read from the crate instead of pinned, so a
/// new migration fails this check rather than silently freezing the matrix.
fn current_version() -> &'static str {
    otel_sqlite_storage::latest_schema_version()
}

/// Messages the sink consumes, outermost first. Order here is the order of the
/// generated matrix, so it reads like an OTLP request nesting downwards.
const COVERED_MESSAGES: &[&str] = &[
    "opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest",
    "opentelemetry.proto.logs.v1.ResourceLogs",
    "opentelemetry.proto.logs.v1.ScopeLogs",
    "opentelemetry.proto.logs.v1.LogRecord",
    "opentelemetry.proto.resource.v1.Resource",
    "opentelemetry.proto.common.v1.InstrumentationScope",
    "opentelemetry.proto.common.v1.KeyValue",
    "opentelemetry.proto.common.v1.AnyValue",
    "opentelemetry.proto.common.v1.ArrayValue",
    "opentelemetry.proto.common.v1.KeyValueList",
];

/// Messages present in the vendored protos that carry no log data this sink
/// stores. Each needs a reason, so a new message cannot slip in unexplained.
const EXCLUDED_MESSAGES: &[(&str, &str)] = &[
    (
        "opentelemetry.proto.logs.v1.LogsData",
        "canonical container for file embeds and non-OTLP transports; the gRPC path delivers ExportLogsServiceRequest, so it is compiled but never instantiated",
    ),
    (
        "opentelemetry.proto.common.v1.EntityRef",
        "Development-status entity references; not consumed",
    ),
    (
        "opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse",
        "response carries no log data; the sink answers with an empty success or an error status",
    ),
    (
        "opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess",
        "partial-success bookkeeping; this sink rejects or accepts whole requests, so it never populates it",
    ),
];

const CLASSIFICATIONS: &[&str] = &[
    "mapped",
    "re-encoded",
    "coerced",
    "dropped",
    "structural",
    "derived",
];

const TSV: &str = "docs/models/traceability.tsv";
const MATRIX: &str = "docs/models/fidelity-matrix.md";

/// Tables that hold the log model. Anything else in the schema must be listed
/// in `NON_MODEL_TABLES` with a reason.
const MODEL_TABLES: &[&str] = &["log_resource", "log_event"];

const NON_MODEL_TABLES: &[(&str, &str)] = &[(
    "schema_migrations",
    "migration ledger maintained by the migrator, not log data",
)];

#[derive(Debug)]
struct Row {
    message: String,
    field: String,
    domain_type: String,
    domain_field: String,
    column: String,
    classification: String,
    loss_counter: String,
    note: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn tsv_path() -> PathBuf {
    repo_root().join(TSV)
}

fn proto_root() -> PathBuf {
    repo_root().join("crates/otel-sqlite-ingress/proto/opentelemetry/proto")
}

// ---------------------------------------------------------------- the TSV

fn rows() -> Vec<Row> {
    let text = std::fs::read_to_string(tsv_path()).expect("read traceability.tsv");
    let mut rows = Vec::new();
    for (number, line) in text.lines().enumerate() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if line.starts_with("proto_message\t") {
            continue;
        }
        let cells: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            cells.len(),
            8,
            "{}:{}: expected 8 tab-separated cells, found {}: {line}",
            TSV,
            number + 1,
            cells.len()
        );
        rows.push(Row {
            message: cells[0].trim().to_owned(),
            field: cells[1].trim().to_owned(),
            domain_type: cells[2].trim().to_owned(),
            domain_field: cells[3].trim().to_owned(),
            column: cells[4].trim().to_owned(),
            classification: cells[5].trim().to_owned(),
            loss_counter: cells[6].trim().to_owned(),
            note: cells[7].trim().to_owned(),
        });
    }
    assert!(!rows.is_empty(), "{TSV} has no rows");
    rows
}

// -------------------------------------------------------------- the protos

/// Fields per fully qualified message name, parsed straight out of the vendored
/// `.proto` sources. `oneof` blocks are transparent (their members are fields
/// of the enclosing message) and `enum` bodies are skipped.
fn proto_fields() -> BTreeMap<String, Vec<String>> {
    let mut messages = BTreeMap::new();
    for file in [
        "collector/logs/v1/logs_service.proto",
        "logs/v1/logs.proto",
        "common/v1/common.proto",
        "resource/v1/resource.proto",
    ] {
        let path = proto_root().join(file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        parse_proto(&source, &mut messages);
    }
    assert!(
        messages.len() >= 12,
        "expected at least 12 messages from the vendored protos, parsed {}",
        messages.len()
    );
    messages
}

fn parse_proto(source: &str, messages: &mut BTreeMap<String, Vec<String>>) {
    let mut package = String::new();
    let mut stack: Vec<(String, i32)> = Vec::new();
    let mut depth: i32 = 0;
    let mut skipping_enum: Option<i32> = None;

    for raw in source.lines() {
        let line = match raw.find("//") {
            Some(comment) => &raw[..comment],
            None => raw,
        }
        .trim();
        if line.is_empty() {
            continue;
        }

        if let Some(enum_depth) = skipping_enum {
            let remaining = enum_depth + braces(line);
            skipping_enum = (remaining > 0).then_some(remaining);
            depth += braces(line);
            continue;
        }

        if let Some(rest) = line.strip_prefix("package ") {
            package.clear();
            package.push_str(rest.trim_end_matches(';').trim());
            continue;
        }
        if line.starts_with("enum ") || line.starts_with("service ") {
            let opened = braces(line);
            depth += opened;
            skipping_enum = (opened > 0).then_some(opened);
            continue;
        }
        if let Some(rest) = line.strip_prefix("message ") {
            let name = rest
                .chars()
                .take_while(|character| character.is_alphanumeric() || *character == '_')
                .collect::<String>();
            depth += braces(line);
            let qualified = if package.is_empty() {
                name.clone()
            } else {
                format!("{package}.{name}")
            };
            messages.entry(qualified.clone()).or_default();
            stack.push((qualified, depth));
            continue;
        }

        depth += braces(line);
        while let Some((_, close_at)) = stack.last() {
            if depth < *close_at {
                stack.pop();
            } else {
                break;
            }
        }

        if let Some((message, _)) = stack.last()
            && let Some(field) = field_name(line)
        {
            messages.entry(message.clone()).or_default().push(field);
        }
    }
}

fn braces(line: &str) -> i32 {
    let mut delta = 0;
    for character in line.chars() {
        match character {
            '{' => delta += 1,
            '}' => delta -= 1,
            _ => {}
        }
    }
    delta
}

fn field_name(line: &str) -> Option<String> {
    let declaration = line.trim_end_matches(';').trim();
    if !declaration.contains('=') || declaration.starts_with("option ") {
        return None;
    }
    let left = declaration.split('=').next()?.trim();
    let name = left.split_whitespace().next_back()?;
    let starts_lowercase = name
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase());
    let all_field_chars = name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_');
    (starts_lowercase && all_field_chars).then(|| name.to_owned())
}

// -------------------------------------------------------------- the schema

/// `table.column` for every stored model table of a freshly migrated database.
fn stored_columns() -> BTreeSet<String> {
    let directory = tempfile::tempdir().expect("create scratch directory");
    let database = directory.path().join("traceability.db");
    let mut conn = Connection::open(&database).expect("open scratch database");
    migrate_up_to(&mut conn, current_version()).expect("apply migrations");

    let mut present: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .expect("query sqlite_master")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("read sqlite_master")
        .map(|row| row.expect("table row"))
        .filter(|name| !name.contains("_fts_"))
        .collect();
    present.sort();

    for (excluded, reason) in NON_MODEL_TABLES {
        assert!(
            present.iter().any(|name| name == excluded),
            "exclusion list names `{excluded}` ({reason}) but the schema has no such table"
        );
    }
    for table in MODEL_TABLES {
        assert!(
            present.iter().any(|name| name == table),
            "model table `{table}` is missing from the migrated schema"
        );
    }

    let mut columns = BTreeSet::new();
    for table in MODEL_TABLES {
        for column in columns_of(&conn, table) {
            columns.insert(format!("{table}.{column}"));
        }
    }
    columns
}

fn columns_of(conn: &Connection, table: &str) -> Vec<String> {
    // The table-valued pragma takes the name as a bound parameter, so no SQL
    // text is built from it.
    conn.prepare("SELECT name FROM pragma_table_info(?1)")
        .expect("prepare table_info")
        .query_map([table], |row| row.get::<_, String>(0))
        .expect("read table_info")
        .map(|row| row.expect("column row"))
        .collect()
}

/// Columns of the read-side view, which must all be projections of stored
/// columns rather than a second model.
fn view_columns() -> Vec<String> {
    let directory = tempfile::tempdir().expect("create scratch directory");
    let database = directory.path().join("view.db");
    let mut conn = Connection::open(&database).expect("open scratch database");
    migrate_up_to(&mut conn, current_version()).expect("apply migrations");
    columns_of(&conn, "logs")
}

// ------------------------------------------------------------- the counters

fn ingress_counter_reasons() -> BTreeSet<String> {
    let directory = repo_root().join("crates/otel-sqlite-ingress/src/mapping");
    let mut reasons = BTreeSet::new();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&directory)
        .expect("read ingress mapping directory")
        .map(|entry| entry.expect("mapping entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    files.sort();
    for path in files {
        let source = std::fs::read_to_string(&path).expect("read ingress mapping source");
        // Two shapes: the counter macro carries the label inline, and the id
        // parser receives it as an argument at the call site.
        for marker in ["\"reason\" => \"", "into_id(\""] {
            for line in source.lines() {
                let Some(position) = line.find(marker) else {
                    continue;
                };
                let rest = &line[position + marker.len()..];
                let reason: String = rest
                    .chars()
                    .take_while(|character| *character != '"')
                    .collect();
                if !reason.is_empty() && !reason.contains(' ') {
                    reasons.insert(reason);
                }
            }
        }
    }
    assert!(
        !reasons.is_empty(),
        "found no otlp_mapping_loss_total call sites in the ingress sources"
    );
    reasons
}

// --------------------------------------------------------------- the checks

fn assert_every_proto_field_is_classified(fields: &BTreeMap<String, Vec<String>>, rows: &[Row]) {
    let mut missing = Vec::new();
    for message in COVERED_MESSAGES {
        let declared = fields
            .get(*message)
            .unwrap_or_else(|| panic!("covered message `{message}` is not in the vendored protos"));
        for field in declared {
            let classified = rows
                .iter()
                .any(|row| row.message == *message && row.field == *field);
            if !classified {
                missing.push(format!("{message}.{field}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} proto fields have no row in {TSV}:\n  {}",
        missing.len(),
        missing.join("\n  ")
    );
}

fn assert_rows_reference_real_fields(fields: &BTreeMap<String, Vec<String>>, rows: &[Row]) {
    for row in rows {
        if row.message.is_empty() {
            assert!(
                !row.field.is_empty() || row.classification == "derived",
                "row with no proto message must be `derived`: {row:?}"
            );
            continue;
        }
        let declared = fields
            .get(&row.message)
            .unwrap_or_else(|| panic!("row names unknown proto message `{}`", row.message));
        assert!(
            declared.contains(&row.field),
            "row names `{}`, which `{}` does not declare",
            row.field,
            row.message
        );
        assert!(
            CLASSIFICATIONS.contains(&row.classification.as_str()),
            "unknown classification `{}` in row {}.{}",
            row.classification,
            row.message,
            row.field
        );
    }
}

fn assert_every_message_is_accounted_for(fields: &BTreeMap<String, Vec<String>>) {
    let excluded: BTreeSet<&str> = EXCLUDED_MESSAGES.iter().map(|(name, _)| *name).collect();
    for message in fields.keys() {
        assert!(
            COVERED_MESSAGES.contains(&message.as_str()) || excluded.contains(message.as_str()),
            "`{message}` is neither covered nor excluded; add it to COVERED_MESSAGES \
             (with TSV rows) or to EXCLUDED_MESSAGES (with a reason)"
        );
    }
    for (message, reason) in EXCLUDED_MESSAGES {
        assert!(
            fields.contains_key(*message),
            "exclusion list names `{message}`, which the vendored protos do not declare"
        );
        assert!(
            !reason.trim().is_empty(),
            "`{message}` is excluded without a reason"
        );
    }
}

fn assert_every_column_is_produced(columns: &BTreeSet<String>, rows: &[Row]) {
    let produced: BTreeSet<&str> = rows.iter().map(|row| row.column.as_str()).collect();
    let unproduced: Vec<&String> = columns
        .iter()
        .filter(|column| !produced.contains(column.as_str()))
        .collect();
    assert!(
        unproduced.is_empty(),
        "stored columns with no traceability row:\n  {}",
        unproduced
            .iter()
            .map(|column| column.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    for row in rows {
        if row.column.is_empty() {
            assert!(
                row.classification == "dropped" || row.classification == "structural",
                "row {}.{} has no column but is `{}`",
                row.message,
                row.field,
                row.classification
            );
            continue;
        }
        assert!(
            columns.contains(&row.column),
            "row {}.{} names column `{}`, which the migrated schema does not have",
            row.message,
            row.field,
            row.column
        );
    }
}

/// The view renames one column on the way out; every other view column keeps
/// the name of the stored column it projects.
const VIEW_ALIASES: &[(&str, &str)] = &[("resource_schema_url", "schema_url")];

fn assert_view_columns_are_projections(view: &[String], columns: &BTreeSet<String>, rows: &[Row]) {
    let names: BTreeSet<&str> = columns
        .iter()
        .filter_map(|column| column.split_once('.').map(|(_, name)| name))
        .collect();
    let mut produced: BTreeSet<&str> = names.clone();
    for row in rows {
        if let Some((_, name)) = row.column.split_once('.') {
            produced.insert(name);
        }
    }
    for (alias, source) in VIEW_ALIASES {
        assert!(
            produced.contains(source),
            "view alias `{alias}` points at `{source}`, which no row produces"
        );
        produced.insert(alias);
    }
    let missing: Vec<&String> = view
        .iter()
        .filter(|column| !produced.contains(column.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "the `logs` view exposes columns with no stored source: {missing:?}"
    );
}

fn assert_loss_counters_match_the_ingress(rows: &[Row]) {
    let documented: BTreeSet<String> = rows
        .iter()
        .filter(|row| !row.loss_counter.is_empty())
        .map(|row| row.loss_counter.clone())
        .collect();
    let emitted = ingress_counter_reasons();
    assert_eq!(
        documented, emitted,
        "the documented otlp_mapping_loss_total reasons and the ones the ingress emits disagree"
    );
}

fn assert_domain_fields_exist(rows: &[Row]) {
    let mut sources = String::new();
    let directory = repo_root().join("crates/otel-sqlite-core/src/model");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&directory)
        .expect("read model directory")
        .map(|entry| entry.expect("model entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    files.sort();
    for path in files {
        sources.push_str(&std::fs::read_to_string(&path).expect("read model source"));
        sources.push('\n');
    }

    for row in rows {
        if row.domain_field.is_empty() {
            continue;
        }
        assert!(
            contains_word(&sources, &row.domain_field),
            "row {}.{} names domain field `{}`, which no model source declares",
            row.message,
            row.field,
            row.domain_field
        );
    }
}

fn contains_word(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let before_ok =
            start == 0 || !bytes[start - 1].is_ascii_alphanumeric() && bytes[start - 1] != b'_';
        let after_ok =
            end == bytes.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_';
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

// ------------------------------------------------------------- the matrix

fn generate_matrix(rows: &[Row]) -> String {
    let mut out = String::new();
    out.push_str("<!-- GENERATED FILE - do not edit by hand. -->\n");
    out.push_str("# OTLP logs: mapping fidelity matrix\n\n");
    out.push_str("What happens to every field of the vendored OTLP protobufs on its way into\n");
    out.push_str("the domain model and the SQLite schema. Generated from\n");
    out.push_str(
        "[`traceability.tsv`](traceability.tsv), which the test suite checks against the\n",
    );
    out.push_str("protos and a migrated database.\n\n");
    out.push_str("```text\n");
    out.push_str(
        "UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability\n",
    );
    out.push_str("```\n\n");
    out.push_str(
        "`re-encoded` keeps the information but changes the representation (JSON, BLOB,\n",
    );
    out.push_str("text). `coerced` means the contract normalizes and the original form is gone.\n");
    out.push_str("`dropped` means it is not stored at all; `loss_counter` names the\n");
    out.push_str("`otlp_mapping_loss_total` reason that records the loss.\n\n");

    let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        *totals.entry(row.classification.as_str()).or_default() += 1;
    }
    out.push_str("## Summary\n\n");
    out.push_str("| classification | rows |\n|---|---:|\n");
    for classification in CLASSIFICATIONS {
        let count = totals.get(classification).copied().unwrap_or(0);
        let _ = writeln!(out, "| `{classification}` | {count} |");
    }
    let _ = writeln!(out, "| **total rows** | **{}** |", rows.len());
    out.push_str("\nRows, not fields: one proto field fans out when persistence denormalizes\n");
    out.push_str(
        "it (a scope's name, version and attributes all land on every `log_event` row).\n\n",
    );

    out.push_str("## Message coverage\n\n");
    out.push_str("Every message in the vendored protos is either classified or excluded with a\n");
    out.push_str("reason — the test fails on anything unaccounted for.\n\n");
    out.push_str("| message | fields classified |\n|---|---:|\n");
    for message in COVERED_MESSAGES {
        let count = rows
            .iter()
            .filter(|row| row.message == *message)
            .map(|row| row.field.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let _ = writeln!(out, "| `{}` | {count} |", short_name(message));
    }
    for (message, reason) in EXCLUDED_MESSAGES {
        let _ = writeln!(out, "| `{}` | excluded: {reason} |", short_name(message));
    }
    out.push('\n');

    for message in COVERED_MESSAGES {
        let _ = writeln!(out, "## `{}`\n", short_name(message));
        let _ = writeln!(out, "Full name: `{message}`\n");
        out.push_str("| proto field | domain | column | classification | loss counter | note |\n");
        out.push_str("|---|---|---|---|---|---|\n");
        for row in rows.iter().filter(|row| row.message == *message) {
            let domain = if row.domain_type.is_empty() {
                "—".to_owned()
            } else if row.domain_field.is_empty() {
                format!("`{}`", row.domain_type)
            } else {
                format!("`{}::{}`", row.domain_type, row.domain_field)
            };
            let column = if row.column.is_empty() {
                "—".to_owned()
            } else {
                format!("`{}`", row.column)
            };
            let counter = if row.loss_counter.is_empty() {
                "—".to_owned()
            } else {
                format!("`{}`", row.loss_counter)
            };
            let _ = writeln!(
                out,
                "| `{}` | {domain} | {column} | `{}` | {counter} | {} |",
                row.field, row.classification, row.note
            );
        }
        out.push('\n');
    }

    out.push_str("## Columns with no wire field\n\n");
    out.push_str(
        "Computed by the sink; a reader of the database needs these to interpret rows.\n\n",
    );
    out.push_str("| column | domain | note |\n|---|---|---|\n");
    for row in rows
        .iter()
        .filter(|row| row.classification == "derived" && row.message.is_empty())
    {
        let domain = if row.domain_field.is_empty() {
            "—".to_owned()
        } else {
            format!("`{}::{}`", row.domain_type, row.domain_field)
        };
        let _ = writeln!(out, "| `{}` | {domain} | {} |", row.column, row.note);
    }

    out
}

fn short_name(qualified: &str) -> &str {
    qualified.rsplit('.').next().unwrap_or(qualified)
}

/// GitHub renders ```mermaid fences but not `.mmd` files, and most editors
/// preview neither. This page embeds the four diagrams as fenced blocks so the
/// diagrams are visible where people actually read the repository.
const DIAGRAM_SOURCES: &[(&str, &str)] = &[
    ("Wire model: how an OTLP logs export nests", "wire.mmd"),
    (
        "Domain model: the in-memory types and the module owning each",
        "domain.mmd",
    ),
    (
        "Persisted schema: the current schema only, generated from a migrated database",
        "schema.mmd",
    ),
    (
        "Conversion path: which module turns a message into a row",
        "pipeline.mmd",
    ),
];

fn generate_diagrams() -> String {
    let mut out = String::new();
    out.push_str("<!-- GENERATED FILE - do not edit by hand. -->\n");
    out.push_str("# Data models, rendered\n\n");
    out.push_str("Embedded copies of the `.mmd` sources next to this file, because GitHub\n");
    out.push_str("renders `mermaid` fences but not `.mmd` files. The sources are authoritative:\n");
    out.push_str("edit and regenerate those, never the fences below.\n\n");
    out.push_str("```text\n");
    out.push_str(
        "UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability\n",
    );
    out.push_str("```\n");

    for (title, file) in DIAGRAM_SOURCES {
        let path = repo_root().join("docs/models").join(file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let _ = writeln!(out, "\n## {title}\n");
        let _ = writeln!(out, "Source: [`{file}`]({file})\n");
        out.push_str("```mermaid\n");
        out.push_str(source.trim_end());
        out.push_str("\n```\n");
    }
    out
}

fn assert_doc_matches(relative: &str, generated: &str) {
    let path = repo_root().join(relative);
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{relative} is missing ({error}); regenerate with \
             UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability"
        )
    });
    assert_eq!(
        committed, generated,
        "\n{relative} is stale: regenerate with \
         UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test model_traceability\n"
    );
}

fn write_if_changed(relative: &str, generated: &str) {
    let path = repo_root().join(relative);
    if std::fs::read_to_string(&path).ok().as_deref() != Some(generated) {
        std::fs::write(&path, generated).expect("write generated doc");
    }
}

#[test]
fn traceability_matrix_matches_protos_and_schema() {
    let rows = rows();
    let fields = proto_fields();
    let columns = stored_columns();

    assert_every_proto_field_is_classified(&fields, &rows);
    assert_rows_reference_real_fields(&fields, &rows);
    assert_every_message_is_accounted_for(&fields);
    assert_every_column_is_produced(&columns, &rows);
    assert_view_columns_are_projections(&view_columns(), &columns, &rows);
    assert_loss_counters_match_the_ingress(&rows);
    assert_domain_fields_exist(&rows);

    let matrix = generate_matrix(&rows);
    let diagrams = generate_diagrams();

    if std::env::var_os("UPDATE_MODEL_DOCS").is_some() {
        std::fs::create_dir_all(repo_root().join("docs/models")).expect("create docs/models");
        write_if_changed(MATRIX, &matrix);
        write_if_changed("docs/models/diagrams.md", &diagrams);
        return;
    }

    assert_doc_matches(MATRIX, &matrix);
    assert_doc_matches("docs/models/diagrams.md", &diagrams);
}
