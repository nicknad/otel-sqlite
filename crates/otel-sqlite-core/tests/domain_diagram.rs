// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Drift check for `docs/models/domain.mmd`.
//!
//! The diagram is extracted from the type definitions in
//! `crates/otel-sqlite-core/src/model/*.rs` — struct fields, enum variants and
//! the arcs between them — so adding or renaming a field makes this test fail
//! instead of leaving a stale picture behind.
//!
//! Regenerate after changing a model type:
//!
//! ```text
//! UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core --test domain_diagram
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const MODEL_DIR: &str = "src/model";

/// Types the extractor must find. If the parser regresses (for example on a
/// syntax it does not understand) it would silently emit an empty diagram;
/// this list turns that into a failure.
const REQUIRED_TYPES: &[&str] = &[
    "Attribute",
    "AttributeValue",
    "LogRecord",
    "LogScope",
    "Resource",
    "Severity",
];

struct Member {
    name: String,
    type_text: String,
}

struct ModelType {
    /// `struct` or `enum`.
    kind: String,
    members: Vec<Member>,
    /// Path of the defining file, relative to the repository root.
    module: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn doc_path() -> PathBuf {
    repo_root().join("docs/models/domain.mmd")
}

fn model_files() -> Vec<PathBuf> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(MODEL_DIR);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&directory)
        .expect("read model directory")
        .map(|entry| entry.expect("model directory entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no model sources found in {MODEL_DIR}");
    files
}

fn extract() -> BTreeMap<String, ModelType> {
    let mut types = BTreeMap::new();
    for path in model_files() {
        let source = std::fs::read_to_string(&path).expect("read model source");
        let module = format!("crates/otel-sqlite-core/{}", relative_to_crate(&path));
        parse_source(&source, &module, &mut types);
    }

    for required in REQUIRED_TYPES {
        assert!(
            types.contains_key(*required),
            "extractor missed `{required}`; the parser no longer understands the model sources"
        );
    }
    types
}

fn relative_to_crate(path: &Path) -> String {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(&crate_root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .expect("model source lives inside the crate")
}

/// Walks `pub struct` / `pub enum` declarations and collects their members.
/// Nested `impl` blocks are skipped by tracking brace depth from the opening
/// brace of the declaration itself.
fn parse_source(source: &str, module: &str, types: &mut BTreeMap<String, ModelType>) {
    let mut current: Option<(String, ModelType)> = None;
    let mut depth: i32 = 0;

    for line in source.lines() {
        if current.is_some() {
            let (name, mut model) = current.take().expect("declaration in progress");
            depth += brace_delta(line);
            if depth <= 0 {
                types.insert(name, model);
                continue;
            }
            if let Some(member) = parse_member(line) {
                model.members.push(member);
            }
            current = Some((name, model));
            continue;
        }

        if let Some((kind, name)) = declaration(line) {
            let mut model = ModelType {
                kind,
                members: Vec::new(),
                module: module.to_owned(),
            };
            depth = brace_delta(line);
            if depth <= 0 {
                model.members.clear();
                types.insert(name, model);
            } else {
                current = Some((name, model));
            }
        }
    }
}

fn declaration(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim_start();
    for (prefix, kind) in [("pub struct ", "struct"), ("pub enum ", "enum")] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let name: String = rest
                .chars()
                .take_while(|character| character.is_alphanumeric() || *character == '_')
                .collect();
            if !name.is_empty() {
                return Some((kind.to_owned(), name));
            }
        }
    }
    None
}

fn brace_delta(line: &str) -> i32 {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") {
        // Doc comments in the model sources quote `{}` inline; counting braces
        // inside them would desynchronise the depth tracking.
        return 0;
    }
    let code = line.split("//").next().unwrap_or_default();
    let mut delta = 0;
    for character in code.chars() {
        match character {
            '{' => delta += 1,
            '}' => delta -= 1,
            _ => {}
        }
    }
    delta
}

fn parse_member(line: &str) -> Option<Member> {
    let trimmed = line.trim();
    if trimmed.is_empty()
        || trimmed.starts_with("///")
        || trimmed.starts_with("//")
        || trimmed.starts_with("#[")
        || trimmed.starts_with("#!")
        || trimmed == "}"
    {
        return None;
    }

    let declaration = trimmed.trim_end_matches(',').trim();
    if let Some((name, type_text)) = declaration.split_once(':') {
        let name = name.trim().trim_start_matches("pub ").trim();
        if name.is_empty() || name.contains('(') {
            return None;
        }
        return Some(Member {
            name: name.to_owned(),
            type_text: type_text.trim().to_owned(),
        });
    }

    // Enum variants: bare identifier, optionally carrying a payload.
    let name: String = declaration
        .chars()
        .take_while(|character| character.is_alphanumeric() || *character == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    let rest = declaration[name.len()..].trim();
    let payload = if rest.starts_with('(') && rest.ends_with(')') {
        rest.trim_start_matches('(')
            .trim_end_matches(')')
            .to_owned()
    } else {
        // `Variant = 7` discriminants and unit variants carry no type.
        String::new()
    };
    Some(Member {
        name,
        type_text: payload,
    })
}

/// Mermaid types are single tokens: strip generics that Mermaid cannot render
/// (`Arc<T>` keeps `T`, `Vec<T>` becomes `Vec_T`) and flatten the rest.
fn type_token(type_text: &str) -> String {
    let without_arc = type_text
        .replace("Arc<", "")
        .replace("Mutex<", "")
        .trim_end_matches('>')
        .to_owned();
    let flat: String = without_arc
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let collapsed = flat
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if collapsed.is_empty() {
        "any".to_owned()
    } else {
        collapsed.join("_")
    }
}

fn generate() -> String {
    let types = extract();

    let mut out = String::new();
    out.push_str("%% GENERATED FILE - do not edit by hand. Pure Mermaid: render with any\n");
    out.push_str("%% Mermaid viewer or a `mermaid` fenced block.\n");
    out.push_str("%% Source: pub struct / pub enum declarations in");
    out.push_str(" crates/otel-sqlite-core/src/model/*.rs.\n");
    out.push_str(
        "%% Module owning every type: `otel_sqlite_core::model` (re-export); each class\n",
    );
    out.push_str("%% names its defining file in the comment above it.\n");
    out.push_str(
        "%% Regenerate: UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core --test domain_diagram\n",
    );
    out.push_str("%% Arrows are derived from field/variant types: `Vec<T>` means many.\n");
    out.push_str("%% Enum variants render as `+payload Variant`; `Vec<u8>` is `Vec_u8` and\n");
    out.push_str("%% `Arc<LogScope>` collapses to `LogScope` (shared, not copied per record).\n");
    out.push_str(
        "%% Self-references (`AttributeValue::Array(Vec<AttributeValue>)`) have no arrow.\n",
    );
    out.push_str("classDiagram\n");

    for (name, model) in &types {
        let _ = writeln!(out, "    %% {name}: {} in {}", model.kind, model.module);
        let _ = writeln!(out, "    class {name} {{");
        for member in &model.members {
            if member.type_text.is_empty() {
                let _ = writeln!(out, "        +{}", member.name);
            } else {
                let _ = writeln!(
                    out,
                    "        +{} {}",
                    type_token(&member.type_text),
                    member.name
                );
            }
        }
        let _ = writeln!(out, "    }}");
    }

    let mut edges: Vec<(String, String, String)> = Vec::new();
    for (name, model) in &types {
        for member in &model.members {
            for target in types.keys() {
                if target == name || !mentions_type(&member.type_text, target) {
                    continue;
                }
                let cardinality = if member.type_text.contains("Vec<") {
                    "many"
                } else {
                    "one"
                };
                edges.push((
                    name.clone(),
                    target.clone(),
                    format!("{} ({cardinality})", member.name),
                ));
            }
        }
    }
    edges.sort();
    for (owner, target, label) in edges {
        let _ = writeln!(out, "    {owner} --> {target} : {label}");
    }

    out
}

fn mentions_type(type_text: &str, target: &str) -> bool {
    type_text
        .split(|character: char| !(character.is_alphanumeric() || character == '_'))
        .any(|token| token == target)
}

#[test]
fn domain_diagram_matches_the_model_types() {
    let generated = generate();
    let path = doc_path();

    if std::env::var_os("UPDATE_MODEL_DOCS").is_some() {
        std::fs::create_dir_all(path.parent().expect("doc directory")).expect("create docs/models");
        std::fs::write(&path, &generated).expect("write domain diagram");
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{} is missing ({error}); regenerate with \
             UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core --test domain_diagram",
            path.display()
        )
    });

    assert_eq!(
        committed,
        generated,
        "\n{} is stale: a model type changed. Regenerate with \
         UPDATE_MODEL_DOCS=1 cargo test -p otel-sqlite-core --test domain_diagram\n",
        path.display()
    );
}
