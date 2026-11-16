use turso_core::pragma::PragmaVirtualTable;
use turso_core::sync::Arc;
use turso_core::Value;
use turso_sqlite::SqliteDialect;

use turso_core::security::roles::SessionRole;

use turso_core::{ConnectOpts, Database, MemoryIO};

#[test]
fn filter_starts_each_pragma_scan_at_rowid_one() {
    let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
    let db =
        Database::open_file(io, turso_core::util::MEMORY_PATH, Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE scan_target(first, second)")
        .unwrap();

    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };
    let mut cursor = pragma_vtab.open(conn).unwrap();

    assert!(cursor
        .filter(turso_core::alloc::vec![Value::from_text("scan_target")])
        .unwrap());
    assert_eq!(cursor.rowid(), 1);
    assert!(cursor.next().unwrap());
    assert_eq!(cursor.rowid(), 2);
    assert!(!cursor.next().unwrap());

    assert!(cursor
        .filter(turso_core::alloc::vec![Value::from_text("scan_target")])
        .unwrap());
    assert_eq!(cursor.rowid(), 1);
}
