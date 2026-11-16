use turso_core::alloc::api::AllocError;
use turso_core::alloc::api::ApiAllocator;
use turso_core::alloc::api::Global;
use turso_core::alloc::api::Layout;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoAllocExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoBinaryHeapExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoBoxExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoHashMapExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoHashSetExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoIteratorExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoNewExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoSliceExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoTryNewExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoTryWithCapacityExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoVecDequeExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoVecExt;
#[allow(unused_imports)]
use turso_core::alloc::collections::TursoVecInExt;

use std::{
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    },
};

#[derive(Clone)]
#[allow(dead_code)]
struct CountingAlloc {
    allocations: StdArc<AtomicUsize>,
    deallocations: StdArc<AtomicUsize>,
}

unsafe impl ApiAllocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        <Global as ApiAllocator>::allocate(&Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        unsafe {
            <Global as ApiAllocator>::deallocate(&Global, ptr, layout);
        }
    }
}

#[cfg(all(nightly, feature = "fts"))]
#[test]
fn database_fts_build_and_merge_use_only_the_fts_allocator() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let mv_allocations = StdArc::new(AtomicUsize::new(0));
    let db = turso_core::Database::open(
        StdArc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(StdArc::new(SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::default().with_index_method(true))
            .allocators(DatabaseAllocators {
                mv_store: DynAllocator::new(CountingAlloc {
                    allocations: mv_allocations.clone(),
                    deallocations: StdArc::new(AtomicUsize::new(0)),
                }),
                fts: DynAllocator::new(CountingAlloc {
                    allocations: allocations.clone(),
                    deallocations: deallocations.clone(),
                }),
            }),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for sql in [
        "INSERT INTO docs VALUES (7, 'hello turso')",
        "INSERT INTO docs VALUES (19, 'hello world')",
        "OPTIMIZE INDEX docs_fts",
    ] {
        allocations.store(0, Ordering::Relaxed);
        deallocations.store(0, Ordering::Relaxed);
        conn.execute(sql).unwrap();
        assert!(allocations.load(Ordering::Relaxed) > 0, "{sql}");
        assert_eq!(
            allocations.load(Ordering::Relaxed),
            deallocations.load(Ordering::Relaxed),
            "{sql}"
        );
        assert_eq!(mv_allocations.load(Ordering::Relaxed), 0, "{sql}");
    }
    let rows = conn
        .prepare("SELECT id FROM docs WHERE fts_match(body, 'hello') ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        std::vec![
            std::vec![turso_core::Value::from_i64(7)],
            std::vec![turso_core::Value::from_i64(19)]
        ]
    );
}
