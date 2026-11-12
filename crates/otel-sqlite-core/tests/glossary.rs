// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Structural check for [`GLOSSARY.md`](../../GLOSSARY.md).
//!
//! The glossary is a human artifact: this test checks its *shape*, not its
//! meaning. It fails when a concept the rest of the documentation depends on
//! disappears, when a term is defined twice, or when a term lists itself under
//! `_Avoid_`.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Concepts the codebase and its docs name explicitly. Removing one from the
/// glossary is a vocabulary decision, not an edit to make silently.
const REQUIRED_TERMS: &[&str] = &[
    "Admission",
    "Attribute",
    "Backup retention",
    "Body",
    "Dropped attributes count",
    "Durable acknowledgement",
    "Event",
    "Event name",
    "Index mode",
    "Instrumentation scope",
    "Log record",
    "Mapping fidelity",
    "Mapping loss",
    "Observed timestamp",
    "Prune",
    "Purge",
    "Quarantine",
    "Rebuild",
    "Reconcile",
    "Resource",
    "Retention",
    "Schema URL",
    "Search index",
    "Severity number",
    "Severity text",
    "Signal",
    "Sink",
    "Timestamp",
    "Trace context",
];

struct Entry {
    term: String,
    definition: String,
    avoid: Vec<String>,
    line_number: usize,
}

fn glossary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../GLOSSARY.md")
}

fn entries() -> Vec<Entry> {
    let path = glossary_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));

    let mut entries: Vec<Entry> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(term) = term_heading(trimmed) {
            entries.push(Entry {
                term,
                definition: String::new(),
                avoid: Vec::new(),
                line_number: index + 1,
            });
            continue;
        }
        let Some(entry) = entries.last_mut() else {
            continue;
        };
        if trimmed.starts_with("## ") {
            // A section heading closes the previous entry.
            continue;
        }
        if let Some(avoided) = trimmed.strip_prefix("_Avoid_:") {
            entry.avoid = avoided
                .split(',')
                .map(|word| word.trim().to_lowercase())
                .filter(|word| !word.is_empty())
                .collect();
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        if !entry.definition.is_empty() {
            entry.definition.push(' ');
        }
        entry.definition.push_str(trimmed);
    }
    entries
}

fn term_heading(line: &str) -> Option<String> {
    let inner = line.strip_prefix("**")?;
    let (term, rest) = inner.split_once("**")?;
    if !rest.starts_with(':') || term.is_empty() {
        return None;
    }
    Some(term.to_owned())
}

#[test]
fn glossary_keeps_its_shape() {
    let entries = entries();
    assert!(
        !entries.is_empty(),
        "{} has no `**Term**:` entries",
        glossary_path().display()
    );

    let terms: BTreeSet<&str> = entries.iter().map(|entry| entry.term.as_str()).collect();
    for required in REQUIRED_TERMS {
        assert!(
            terms.contains(required),
            "GLOSSARY.md no longer defines `{required}`; if that concept was retired, \
             update REQUIRED_TERMS in this test as part of the same change"
        );
    }

    assert_eq!(
        terms.len(),
        entries.len(),
        "GLOSSARY.md defines a term more than once"
    );

    for entry in &entries {
        assert!(
            !entry.definition.trim().is_empty(),
            "GLOSSARY.md:{}: `{}` has no definition",
            entry.line_number,
            entry.term
        );
        let term = entry.term.to_lowercase();
        assert!(
            !entry.avoid.contains(&term),
            "GLOSSARY.md:{}: `{}` lists itself under _Avoid_",
            entry.line_number,
            entry.term
        );
    }
}
