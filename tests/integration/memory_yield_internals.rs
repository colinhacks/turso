use std::sync::Arc;
use turso_core::io::clock::MonotonicInstant;
use turso_core::io::clock::WallClockInstant;
use turso_core::io::memory_yield::*;
use turso_core::io::Clock;
use turso_core::io::File;
use turso_core::io::IO;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

use turso_core::security::roles::SessionRole;

use turso_core::vdbe::StepResult;

use turso_core::{ConnectOpts, Database, OpenFlags};

use std::sync::atomic::{AtomicBool, Ordering};

struct StepGuardedIO {
    inner: MemoryYieldIO,
    step_allowed: AtomicBool,
}

impl StepGuardedIO {
    fn new() -> Self {
        Self {
            inner: MemoryYieldIO::new(),
            step_allowed: AtomicBool::new(true),
        }
    }

    fn set_step_allowed(&self, allowed: bool) {
        self.step_allowed.store(allowed, Ordering::SeqCst);
    }
}

impl Clock for StepGuardedIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for StepGuardedIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }

    fn step(&self) -> Result<()> {
        assert!(
            self.step_allowed.load(Ordering::SeqCst),
            "IO::step must only be called by the test driver"
        );
        self.inner.step()
    }
}

#[test]
fn fresh_mvcc_attach_yields_without_internal_step() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "attach_main_yield.db",
        OpenFlags::Create,
        turso_core::DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let run = |sql: &str| {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut io_yields = 0usize;
        loop {
            io.set_step_allowed(false);
            let step = stmt.step();
            io.set_step_allowed(true);

            match step.unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    io.step().unwrap();
                }
                StepResult::Row => {}
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        io_yields
    };

    run("PRAGMA journal_mode = 'mvcc'");
    let attach_yields = run("ATTACH 'attach_aux_yield.db' AS aux");

    assert!(
        attach_yields > 0,
        "MemoryYieldIO should force ATTACH through cooperative yields"
    );
    assert!(conn.get_database_id_by_name("aux").is_ok());
    assert!(conn
        .mv_store_for_db(conn.get_database_id_by_name("aux").unwrap())
        .is_some());
}
