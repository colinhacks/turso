use turso_core::connection::*;
use turso_core::io::IO;
use turso_core::sync::Arc;
#[allow(unused_imports)]
use turso_core::util::IOExt;
use turso_core::Database;
use turso_core::DatabaseOpts;
use turso_core::OpenFlags;
use turso_core::Pager;
use turso_core::Result;
use turso_core::Schema;
use turso_sqlite::SqliteDialect;

use turso_core::attach::AttachDatabaseState;

use turso_core::security::roles::SessionRole;

use turso_core::ConnectOpts;

use turso_core::IOResult;

use tempfile::TempDir;

fn open_connection_with_opts(path: &std::path::Path, opts: DatabaseOpts) -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap()
}

fn open_connection(path: &std::path::Path) -> Arc<Connection> {
    open_connection_with_opts(path, DatabaseOpts::new())
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) -> Result<()> {
    let mut state = AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state)? {
            IOResult::Done(()) => return Ok(()),
            IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
        }
    }
}

fn drive_attach_with_config(
    conn: &Arc<Connection>,
    path: &str,
    alias: &str,
    reserved_space: Option<u8>,
) -> Result<()> {
    let mut state = AttachDatabaseState::default();
    loop {
        match conn.attach_database_with_config(path, alias, reserved_space, &mut state)? {
            IOResult::Done(()) => return Ok(()),
            IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
        }
    }
}

fn attached_entry(conn: &Connection, alias: &str) -> (Arc<Database>, Arc<Pager>) {
    let index = conn.qualifiers.database_id(alias).unwrap();
    let catalog = conn.attached_databases.read();
    let entry = catalog.index_to_data.get(&index).unwrap();
    (entry.db.clone(), entry.pager.clone())
}

#[test]
fn test_attach_database_with_config_overrides_reserved_space_before_initialization() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(48)).unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    assert!(!attached_db.initialized());
    assert!(!pager.db_initialized());
    assert_eq!(pager.get_reserved_space(), Some(48));
}

#[cfg(feature = "checksum")]
#[test]
fn test_attach_database_with_config_rejects_reserved_space_below_minimum() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    let err = drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(0))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "cannot attach database 'aux': reserved space 0 is smaller than attached database minimum 8"
    );
}

#[test]
fn test_fresh_mvcc_attach_installs_wal_before_bootstrap() {
    // this is a test to check that mvcc db on attach with a fresh db, makes the
    // attached db also mvcc
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    assert!(attached_db.get_mv_store().as_ref().is_some());
    assert!(pager.has_wal());

    conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();
    conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn test_fresh_mvcc_attach_reuses_database_shared_wal() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    let pager_shared_ptr = pager
        .wal_shared_ptr()
        .expect("fresh MVCC attach must expose WAL shared state in tests");
    let db_shared_ptr = Arc::as_ptr(&attached_db.shared_wal) as usize;

    assert_eq!(pager_shared_ptr, db_shared_ptr);
}

#[test]
fn test_prepared_statement_reprepares_after_automatic_index_pragma() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TABLE t1(a)").unwrap();
    let mut stmt = conn.prepare("SELECT a FROM t1").unwrap();

    conn.execute("PRAGMA automatic_index = OFF").unwrap();
    stmt.run_collect_rows().unwrap();

    let reprepares = stmt.stmt_status(turso_core::statement::StatementStatusCounter::Reprepare);
    assert_eq!(reprepares, 1);
}

/// The reparse of the schema installs a fresh schema before the
/// dialect's schema state is read, and a statement compiled meanwhile
/// must not see it empty.
#[test]
fn schema_installed_while_being_read_keeps_the_loaded_dialect_schema() {
    let db = Database::open_file(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.with_schema_mut(|schema| {
        schema.dialect_schema = Some(Arc::new(()));
    })
    .unwrap();
    let fresh = Schema::with_options(false, &SqliteDialect).unwrap();
    assert!(fresh.dialect_schema.is_none());

    conn.install_schema_keeping_dialect_schema(&fresh).unwrap();

    assert!(conn.schema.read().dialect_schema.is_some());
}
