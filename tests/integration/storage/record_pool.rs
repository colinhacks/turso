use turso_core::security::roles::SessionRole;
#[cfg(feature = "autovacuum")]
#[allow(unused_imports)]
use turso_core::storage::pager::ptrmap::*;
#[allow(unused_imports)]
use turso_core::util::IOExt as _;
use turso_sqlite::SqliteDialect;

use turso_core::sync::Arc;

use turso_core::io::MemoryIO;

#[test]
fn record_pool_frees_buffers_of_rows_larger_than_a_page() {
    let io = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST(zeroblob(6 * 1024 * 1024) AS TEXT), 'big')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST(zeroblob(5000) AS TEXT), 'small')")
        .unwrap();
    conn.execute("SELECT body FROM t WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 2")
        .unwrap();

    let pager = conn.get_pager();
    let pool = pager.record_pool.lock();
    assert!(!pool.is_empty());
    for buf in pool.iter() {
        assert!(buf.capacity() <= turso_core::storage::sqlite3_ondisk::PageSize::MAX as usize);
    }
}
