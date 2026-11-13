// Canary file for the semgrep rule-coverage audit. Not a cargo target: no crate
// declares this directory, so nothing here is compiled. Deleted with the branch.
#![allow(unused)]

fn shape_1_inline_prepare(conn: &rusqlite::Connection, table: &str) {
    let _ = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"));
}

fn shape_2_variable_then_prepare(conn: &rusqlite::Connection, table: &str) {
    let sql = format!("PRAGMA index_info(\"{table}\")");
    let _ = conn.prepare(&sql);
}

fn shape_3_execute_batch(conn: &rusqlite::Connection, mode: &str) {
    let _ = conn.execute_batch(&format!("PRAGMA wal_checkpoint({mode});"));
}

fn shape_4_query_row(conn: &rusqlite::Connection, table: &str) {
    let _ = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    });
}

fn shape_5_query_without_ref(conn: &rusqlite::Connection, table: &str) {
    let _ = conn.query(format!("SELECT * FROM {table}"), []);
}

fn shape_6_prepare_cached(conn: &rusqlite::Connection, table: &str) {
    let _ = conn.prepare_cached(&format!("SELECT * FROM {table}"));
}

fn shape_7_uri_open(path: &str) {
    let _ = rusqlite::Connection::open(format!("file:{path}?immutable=1"));
}

fn shape_8_control_execute(conn: &rusqlite::Connection, table: &str) {
    let _ = conn.execute(&format!("DELETE FROM {table}"), []);
}

fn shape_9_variable_then_execute_batch(conn: &rusqlite::Connection, mode: &str) {
    let sql = format!("PRAGMA wal_checkpoint({mode});");
    let _ = conn.execute_batch(&sql);
}

fn shape_10_static_format(conn: &rusqlite::Connection) {
    let _ = conn.execute(&format!("SELECT 1"), []);
}
