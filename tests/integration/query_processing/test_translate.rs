use turso_core::sync::Arc;
use turso_core::translate::*;
use turso_core::vdbe::builder::QueryMode;
use turso_core::SymbolTable;
use turso_parser::ast;
use turso_sqlite::SqliteDialect;

use turso_core::alloc::TryClone;

use turso_core::io::MemoryIO;

use turso_core::namespace::NamespaceId;

use turso_core::schema::{BTreeTable, Table, SQLITE_SEQUENCE_TABLE_NAME};

use turso_core::security::roles::SessionRole;

use turso_core::ConnectOpts;

use turso_core::Database;

/// Verify that REGEXP produces the correct error when no regexp function is registered.
#[test]
fn test_regexp_no_function_registered() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let schema = db.schema.lock().clone();
    let pager = conn.pager.load().clone();

    // Use an empty SymbolTable so regexp() is not available.
    let empty_syms = SymbolTable::new();
    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "SELECT 'x' REGEXP 'y'")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let result = translate(
        &schema,
        stmt,
        pager,
        conn,
        &empty_syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("no such function: regexp"),
        "expected 'no such function: regexp', got: {err}"
    );
}

#[test]
fn test_insert_autoincrement_with_malformed_sqlite_sequence_is_corrupt() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    let seq_root_page = schema
        .get_btree_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
        .expect("sqlite_sequence should exist after creating AUTOINCREMENT table")
        .root_page;
    let malformed_seq = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE sqlite_sequence(name)",
        seq_root_page,
        false,
    )
    .expect("malformed sqlite_sequence SQL should parse");
    schema.insert_table(
        NamespaceId::ROOT,
        SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        Arc::new(Table::BTree(Arc::new(malformed_seq))),
    );

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "INSERT INTO t(v) VALUES('x')")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let err = translate(
        &schema,
        stmt,
        pager,
        conn,
        &syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    )
    .expect_err("translation should fail with malformed sqlite_sequence");
    match err {
        turso_core::LimboError::Corrupt(msg) => {
            assert!(
                msg.contains("sqlite_sequence"),
                "expected sqlite_sequence corruption error, got: {msg}"
            );
        }
        other => panic!("expected LimboError::Corrupt, got: {other}"),
    }
}

#[test]
fn test_insert_autoincrement_with_missing_sqlite_sequence_is_corrupt() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    schema.take_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME);

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "INSERT INTO t(v) VALUES('x')")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let err = translate(
        &schema,
        stmt,
        pager,
        conn,
        &syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    )
    .expect_err("translation should fail with missing sqlite_sequence");
    match err {
        turso_core::LimboError::Corrupt(msg) => {
            assert!(
                msg.contains("missing sqlite_sequence"),
                "expected missing sqlite_sequence error, got: {msg}"
            );
        }
        other => panic!("expected LimboError::Corrupt, got: {other}"),
    }
}
