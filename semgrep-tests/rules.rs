// Fixtures for the syntax-scoped rules. Not a cargo target: no crate declares
// this directory, so nothing here is compiled.
#![allow(unused)]

fn debug_leftovers() {
    // ruleid: rust.dbg-macro
    dbg!(1);

    // ruleid: rust.todo-unimplemented
    todo!();

    // ruleid: rust.todo-unimplemented
    unimplemented!();
}

fn unsafe_without_justification() {
    // ruleid: rust.unsafe-block
    unsafe {
        std::ptr::null::<u8>();
    }
}

#[allow(unsafe_code)]
fn unsafe_with_justification() {
    // ok: rust.unsafe-block
    unsafe {
        std::ptr::null::<u8>();
    }
}

fn secrets() {
    // ruleid: generic.hardcoded-secret
    let api_key = "hunter2";

    // ok: generic.hardcoded-secret
    let api_key_path = from_environment();
}

fn from_environment() -> String {
    String::new()
}

fn sql_shapes(conn: &rusqlite::Connection, table: &str, mode: &str) {
    // ruleid: security.sql-format-string
    let _ = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"));

    // ruleid: security.sql-format-string
    let _ = conn.execute_batch(&format!("PRAGMA wal_checkpoint({mode});"));

    // ruleid: security.sql-format-string
    let _ = conn.execute(&format!("DELETE FROM {table}"), []);

    // ruleid: security.sql-format-string
    let _ = conn.query(&format!("SELECT * FROM {table}"), []);

    // ruleid: security.sql-format-string
    let _ = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    });

    // ruleid: security.sql-format-string
    let _ = conn.prepare_cached(&format!("SELECT * FROM {table}"));

    // ok: security.sql-format-string
    let _ = conn.prepare("SELECT * FROM log_event WHERE id = ?1");

    // ok: security.sql-format-string
    let _ = conn.execute("DELETE FROM log_event WHERE id = ?1", [table]);
}
