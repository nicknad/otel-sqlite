// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Drift check for `docs/models/schema.mmd`.
//!
//! The diagram is derived from the schema a freshly migrated database actually
//! has — never from the migration SQL. Migrations are a replay script, not a
//! current shape: 001 rebuilds the search index and adds triggers, 003 adds
//! scope columns and recreates the read-side view, 004 rebuilds a table, and
//! 005 drops every metric table plus the `scope` dimension. Reading
//! `000_baseline.sql` would describe a schema this binary cannot open.
//!
//! Regenerate after changing a migration:
//!
//! ```text
//! UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram
//! ```

use std::fmt::Write as _;
use std::path::PathBuf;

use otel_sqlite_storage::migrate_up_to;
use rusqlite::Connection;

/// Newest migration this binary knows; the diagram describes the schema after
/// all of them have been applied. Read from the crate instead of pinned, so a
/// new migration fails this check rather than silently freezing the diagram.
fn current_version() -> &'static str {
    otel_sqlite_storage::latest_schema_version()
}

/// FTS5 stores its internal state in shadow tables derived from the virtual
/// table name. They are engine detail, not model.
const FTS_SHADOW_PREFIX: &str = "logs_fts_";

struct Column {
    name: String,
    declared_type: String,
    not_null: bool,
    default: Option<String>,
    primary_key: bool,
}

struct Entity {
    name: String,
    kind: String,
    columns: Vec<Column>,
}

struct Relationship {
    parent: String,
    child: String,
    label: String,
    kind: &'static str,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn doc_path() -> PathBuf {
    repo_root().join("docs/models/schema.mmd")
}

/// Opens a scratch database, runs every migration, and renders the diagram.
fn generate() -> String {
    let directory = tempfile::tempdir().expect("create scratch directory");
    let database = directory.path().join("schema.db");
    let mut conn = Connection::open(&database).expect("open scratch database");
    migrate_up_to(&mut conn, current_version()).expect("apply migrations");

    let entities = entities(&conn);
    let relationships = relationships(&conn, &entities);
    let indexes = indexes(&conn);
    let triggers = triggers(&conn);

    let mut out = String::new();
    out.push_str("%% GENERATED FILE - do not edit by hand. Pure Mermaid: render with any\n");
    out.push_str("%% Mermaid viewer or a `mermaid` fenced block.\n");
    out.push_str("%% Source: the schema of a database migrated through ");
    let _ = writeln!(
        out,
        "{} (see migrations/*.sql replay order).",
        current_version()
    );
    out.push_str("%% Regenerate: UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram\n");
    out.push_str("%% Signal scope: logs only; migration 005 dropped every metric table.\n");
    out.push_str(
        "%% `logs` is a view, `logs_fts` a contentless FTS5 index kept in sync by triggers;\n",
    );
    out.push_str("%% both are dashed because neither owns storage of its own.\n");
    out.push_str("%% `schema_migrations` is the migration ledger, not log data.\n");
    out.push_str("erDiagram\n");

    for relationship in &relationships {
        let _ = writeln!(
            out,
            "    {} {} {} : \"{}\"",
            relationship.parent, relationship.kind, relationship.child, relationship.label
        );
    }

    for entity in &entities {
        let _ = writeln!(out, "    {} {{", entity.name);
        for column in &entity.columns {
            let _ = write!(
                out,
                "        {} {}",
                type_token(&column.declared_type),
                column.name
            );
            if column.primary_key {
                out.push_str(" PK");
            }
            out.push_str(&attribute_comment(column));
            out.push('\n');
        }
        let _ = writeln!(out, "    }}");
        if entity.kind != "table" {
            let _ = writeln!(out, "    %% {} is a {}", entity.name, entity.kind);
        }
    }

    if !indexes.is_empty() {
        let _ = writeln!(out, "    %% indexes: {}", indexes.join("; "));
    }
    if !triggers.is_empty() {
        let _ = writeln!(out, "    %% triggers: {}", triggers.join("; "));
    }

    out
}

fn entities(conn: &Connection) -> Vec<Entity> {
    let mut statement = conn
        .prepare(
            "SELECT name, type, sql FROM sqlite_master
             WHERE type IN ('table', 'view')
               AND name NOT LIKE 'sqlite_%'
             ORDER BY type DESC, name",
        )
        .expect("query sqlite_master");

    let mut entities: Vec<Entity> = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .expect("read sqlite_master")
        .map(|row| row.expect("sqlite_master row"))
        .filter(|(name, _, _)| !name.starts_with(FTS_SHADOW_PREFIX))
        .map(|(name, object_type, sql)| Entity {
            columns: columns(conn, &name),
            kind: object_kind(&object_type, sql.as_deref()),
            name,
        })
        .collect();

    entities.sort_by(|left, right| left.name.cmp(&right.name));
    entities
}

/// `sqlite_master.type` is `table` for virtual tables too, so the FTS5 index
/// would otherwise read as an ordinary stored table.
fn object_kind(object_type: &str, sql: Option<&str>) -> String {
    if object_type == "view" {
        return "view".to_owned();
    }
    match sql {
        Some(sql) if sql.trim_start().starts_with("CREATE VIRTUAL TABLE") => {
            "virtual table".to_owned()
        }
        _ => "table".to_owned(),
    }
}

fn columns(conn: &Connection, entity: &str) -> Vec<Column> {
    // Identifiers come from sqlite_master, but a virtual table name still has
    // to be quoted into the pragma call.
    let sql = format!("PRAGMA table_info(\"{}\")", entity.replace('"', "\"\""));
    let mut statement = conn.prepare(&sql).expect("prepare table_info");
    let mut columns: Vec<Column> = statement
        .query_map([], |row| {
            Ok(Column {
                name: row.get(1)?,
                declared_type: row.get(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                default: row.get(4)?,
                primary_key: row.get::<_, i64>(5)? != 0,
            })
        })
        .expect("read table_info")
        .map(|row| row.expect("table_info row"))
        .collect();

    // INTEGER PRIMARY KEY is the rowid alias and cannot hold NULL even though
    // SQLite reports `notnull = 0` for it.
    for column in &mut columns {
        if column.primary_key && column.declared_type.eq_ignore_ascii_case("INTEGER") {
            column.not_null = true;
        }
    }
    columns
}

fn relationships(conn: &Connection, entities: &[Entity]) -> Vec<Relationship> {
    let mut relationships = Vec::new();
    for entity in entities {
        if entity.kind != "table" {
            continue;
        }
        let sql = format!(
            "PRAGMA foreign_key_list(\"{}\")",
            entity.name.replace('"', "\"\"")
        );
        let mut statement = conn.prepare(&sql).expect("prepare foreign_key_list");
        let mut rows: Vec<Relationship> = statement
            .query_map([], |row| {
                let parent: String = row.get(2)?;
                let column: String = row.get(3)?;
                let on_delete: Option<String> = row.get(6)?;
                Ok(Relationship {
                    parent,
                    child: entity.name.clone(),
                    label: match on_delete.as_deref() {
                        Some(action) if action != "NO ACTION" => {
                            format!("{column} (on delete {action})")
                        }
                        _ => column,
                    },
                    kind: "||--o{",
                })
            })
            .expect("read foreign_key_list")
            .map(|row| row.expect("foreign_key_list row"))
            .collect();
        rows.sort_by(|left, right| left.parent.cmp(&right.parent));
        relationships.append(&mut rows);
    }

    // Logical (non-identifying) edges: the view projects rows it does not own,
    // and the search index mirrors them through triggers.
    relationships.push(Relationship {
        parent: "log_event".to_owned(),
        child: "logs".to_owned(),
        label: "read-side projection of log_event + log_resource".to_owned(),
        kind: "||..o{",
    });
    relationships.push(Relationship {
        parent: "log_event".to_owned(),
        child: "logs_fts".to_owned(),
        label: "rowid = log_event.id, maintained by AFTER INSERT/DELETE triggers".to_owned(),
        kind: "||..o{",
    });

    relationships.sort_by(|left, right| {
        (left.parent.as_str(), left.child.as_str())
            .cmp(&(right.parent.as_str(), right.child.as_str()))
    });
    relationships
}

fn indexes(conn: &Connection) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT name, tbl_name FROM sqlite_master
             WHERE type = 'index' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .expect("query indexes");
    let mut indexes: Vec<String> = statement
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let table: String = row.get(1)?;
            let columns = indexed_columns(conn, &name);
            Ok(format!("{name}({table}: {})", columns.join(", ")))
        })
        .expect("read indexes")
        .map(|row| row.expect("index row"))
        .collect();
    indexes.sort();
    indexes
}

fn indexed_columns(conn: &Connection, index: &str) -> Vec<String> {
    let sql = format!("PRAGMA index_info(\"{}\")", index.replace('"', "\"\""));
    let mut statement = conn.prepare(&sql).expect("prepare index_info");
    let mut columns: Vec<String> = statement
        .query_map([], |row| row.get::<_, Option<String>>(2))
        .expect("read index_info")
        .map(|row| {
            row.expect("index_info row")
                .unwrap_or_else(|| "<expr>".to_owned())
        })
        .collect();
    columns.sort();
    columns
}

fn triggers(conn: &Connection) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .expect("query triggers");
    let mut triggers: Vec<String> = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("read triggers")
        .map(|row| row.expect("trigger row"))
        .collect();
    triggers.sort();
    triggers
}

/// Mermaid attribute types are single tokens; FTS5 and view expressions declare
/// none, so they render as `ANY` rather than pretending to a storage type.
fn type_token(declared_type: &str) -> String {
    let token: String = declared_type
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    if token.is_empty() {
        "ANY".to_owned()
    } else {
        token
    }
}

/// Mermaid allows one free-form comment per attribute; nullability and defaults
/// are the constraints a reader cannot guess from the type alone.
fn attribute_comment(column: &Column) -> String {
    let mut parts = Vec::new();
    if column.not_null && !column.primary_key {
        parts.push("not null".to_owned());
    }
    if let Some(default) = &column.default {
        parts.push(format!("default {}", default.replace('"', "'")));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" \"{}\"", parts.join("; "))
    }
}

#[test]
fn schema_diagram_matches_a_migrated_database() {
    let generated = generate();
    let path = doc_path();

    if std::env::var_os("UPDATE_MODEL_DOCS").is_some() {
        std::fs::create_dir_all(path.parent().expect("doc directory")).expect("create docs/models");
        std::fs::write(&path, &generated).expect("write schema diagram");
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{} is missing ({error}); regenerate with \
             UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram",
            path.display()
        )
    });

    assert_eq!(
        committed,
        generated,
        "\n{} is stale: a migration changed the schema. Regenerate with \
         UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-storage --test schema_diagram\n",
        path.display()
    );
}
