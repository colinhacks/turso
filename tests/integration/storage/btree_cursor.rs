use std::fmt::Debug;
#[allow(unused_imports)]
use turso_core::alloc::TursoSliceExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoVecExt;
use turso_core::schema::Index;
use turso_core::security::roles::SessionRole;
use turso_core::storage::sqlite3_ondisk::TableInteriorCell;
use turso_core::types::IOResult;
use turso_core::types::IOResultOr;
use turso_core::types::ImmutableRecord;
use turso_core::types::SeekKey;
use turso_core::types::SeekOp;
use turso_core::types::SeekResult;
use turso_core::types::ValueRef;
#[allow(unused_imports)]
use turso_core::util::IOExt;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

use sorted_vec::SortedVec;

use test_log::test;

use turso_core::storage::btree::*;

use turso_core::{
    io::{MemoryIO, OpenFlags, IO},
    mvcc::{
        yield_hooks::YieldPointMarker,
        yield_points::{YieldInjector, YieldPoint},
    },
    schema::IndexColumn,
    storage::{
        database::DatabaseFile,
        page_cache::PageCache,
        pager::{default_page1, SharedPagerState},
        sqlite3_ondisk::PageSize,
    },
    vdbe::Register,
    BufferPool, ConnectOpts, Connection, StepResult, Wal, WalAutoActions, WalFile, WalFileShared,
};

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use turso_core::{
    storage::sqlite3_ondisk::{BTreeCell, PageType, TableLeafCell},
    types::Value,
    Database, Pager,
};

use turso_core::storage::btree::btree_init_page;

#[derive(Debug)]
struct TargetedYieldInjector {
    point: YieldPoint,
    selection_key: u64,
    fired: AtomicBool,
}

impl TargetedYieldInjector {
    fn new(point: YieldPoint, selection_key: u64) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl YieldInjector for TargetedYieldInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        point == self.point
            && selection_key == self.selection_key
            && !self.fired.swap(true, Ordering::AcqRel)
    }
}

fn btree_write_selection_key(root_page: i64) -> u64 {
    BTREE_WRITE_YIELD_FAMILY ^ root_page as u64
}

fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}

fn query_single_text(conn: &Arc<Connection>, sql: &str) -> String {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<String>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}

/// Deterministic coverage for the allocation-failure window created by routing
/// overflow-cell payloads through `TursoAllocator`: in
/// `OverwriteCellState::ClearOverflowPagesAndOverwrite`, `drop_cell` mutates the
/// page before `insert_into_cell`, whose overflow-cell branch can fail on
/// allocation. A fault there must not lose the row or corrupt the tree.
///
/// Nightly-only because on stable `turso_core::alloc::Vec` degrades to the global
/// allocator, so the injected fault can never fire.
#[cfg(all(nightly, feature = "allocation_metric"))]
mod allocation_fault_repro {
    use super::*;
    use std::cell::Cell as StdCell;
    use std::ptr::NonNull;
    use test_log::test;
    use turso_core::alloc::{
        current_allocation_site, AllocError, AllocationSite, ApiAllocator, BTreeAllocationSite,
        Global, Layout, TursoAllocBackend,
    };
    use turso_core::security::roles::SessionRole;

    thread_local! {
        static FAIL_SITE: StdCell<Option<AllocationSite>> = const { StdCell::new(None) };
    }

    struct SiteFaultBackend;

    unsafe impl TursoAllocBackend for SiteFaultBackend {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            let fail = FAIL_SITE.with(|slot| {
                let target = slot.get();
                target.is_some() && target == current_allocation_site()
            });
            if fail {
                return Err(AllocError);
            }
            <Global as ApiAllocator>::allocate(&Global, layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { <Global as ApiAllocator>::deallocate(&Global, ptr, layout) }
        }
    }

    static BACKEND: SiteFaultBackend = SiteFaultBackend;

    fn arm_fault(site: BTreeAllocationSite) {
        // The backend passes through to Global unless a fault is armed on this
        // thread, so sharing it process-wide with other tests is safe.
        let _ = unsafe { turso_core::alloc::set_allocator(&BACKEND) };
        FAIL_SITE.with(|slot| slot.set(Some(AllocationSite::BTree(site))));
    }

    fn disarm_fault() {
        FAIL_SITE.with(|slot| slot.set(None));
    }

    #[test]
    fn overwrite_survives_overflow_cell_allocation_failure() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db =
            Database::open_file(io, "overflow-cell-fault.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        // Row 1 is the overwrite target; the filler rows pack the leaf page so
        // that growing row 1 cannot fit locally and must spill to an overflow
        // cell inside insert_into_cell.
        conn.execute(format!("INSERT INTO t VALUES (1, '{}')", "a".repeat(600)))
            .unwrap();
        for id in 2..=6 {
            conn.execute(format!(
                "INSERT INTO t VALUES ({id}, '{}')",
                "b".repeat(600)
            ))
            .unwrap();
        }
        let rows_before = query_single_i64(&conn, "SELECT count(*) FROM t");
        assert_eq!(rows_before, 6);

        arm_fault(BTreeAllocationSite::OverflowCell);
        let update = conn.execute(format!(
            "UPDATE t SET v = '{}' WHERE id = 1",
            "c".repeat(1400)
        ));
        disarm_fault();
        assert!(
            update.is_err(),
            "update should fail once the overflow-cell allocation is denied"
        );

        // The failed statement must not lose the row or corrupt the tree.
        assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM t"), 6);
        assert_eq!(
            query_single_i64(&conn, "SELECT length(v) FROM t WHERE id = 1"),
            600
        );
        assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
    }

    /// Regression test: the overflow-read epilogue empties the payload held
    /// inside `read_overflow_state` before the fallible record allocations
    /// run. On failure it must clear the state — same invariant the adjacent
    /// corrupt-chain branch upholds — otherwise a later call resumes against
    /// the emptied buffer and silently completes with an empty record.
    #[test]
    fn overflow_read_epilogue_clears_state_on_allocation_failure() {
        let pager = setup_test_env(5);
        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);
        let usable = cursor.usable_space();
        let data_per_page = usable - 4;

        // Re-purpose pages 4 and 5 as a valid 2-page overflow chain: 4 -> 5 -> end.
        let load_and_fill = |id: i64, next: u32, fill: u8| {
            let (page, c) = cursor.read_page_blocking(id).unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            while page.is_locked() {
                pager.io.step().unwrap();
            }
            let buf = page.get_contents().as_ptr();
            buf[0..4].copy_from_slice(&next.to_be_bytes());
            buf[4..usable].fill(fill);
        };
        load_and_fill(4, 5, b'A');
        load_and_fill(5, 0, b'B');

        let payload_size = (2 * data_per_page) as u64;
        let cursor_pager = cursor.pager.clone();

        // Fail the epilogue's record allocation, which runs after the chain
        // has been fully read and the accumulated payload was already moved
        // out of `read_overflow_state`.
        arm_fault(BTreeAllocationSite::RecordPayload);
        run_until_done(
            || cursor.process_overflow_read(b"", 4, payload_size),
            &cursor_pager,
        )
        .expect_err("record allocation failure should surface");
        disarm_fault();

        assert!(
            cursor.read_overflow_state.is_none(),
            "failed overflow read left resumable state behind"
        );

        // A retry must rebuild the record from scratch and see the full chain.
        run_until_done(
            || cursor.process_overflow_read(b"", 4, payload_size),
            &cursor_pager,
        )
        .unwrap();
        let rec_slot = cursor.get_immutable_record_or_create().unwrap();
        let bytes = rec_slot.as_ref().unwrap().get_payload();
        assert_eq!(bytes.len(), 2 * data_per_page);
        assert!(bytes[..data_per_page].iter().all(|&b| b == b'A'));
        assert!(bytes[data_per_page..].iter().all(|&b| b == b'B'));
    }
}

#[test]
fn wal_reuses_freelist_leaf_after_abandoned_overflowing_insert() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db =
        Database::open_file(io, "freelist-stale-overflow.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(
        "CREATE TABLE red_rain_308 (
            slow_desk_238 INTEGER NOT NULL,
            quiet_tree_398 TEXT NOT NULL,
            smart_dog_422 REAL UNIQUE,
            new_hill_140 TEXT,
            light_door_957 REAL,
            wild_dog_803 TEXT NOT NULL,
            new_flower_28 BLOB,
            old_fish_931 TEXT PRIMARY KEY
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE full_fish_194 (
            brave_star_722 TEXT PRIMARY KEY,
            hot_star_957 NUMERIC,
            empty_chair_834 REAL
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO red_rain_308 (
            slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
            light_door_957, wild_dog_803, new_flower_28, old_fish_931
        ) VALUES (
            1, 'seed', 0.24, 'seed', 1.0, 'seed', zeroblob(3600), 'existing_unique'
        )",
    )
    .unwrap();
    for id in 2..260 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
            )"
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM red_rain_308 WHERE old_fish_931 = 'seed_2'")
        .unwrap();
    conn.execute("SAVEPOINT sp_91").unwrap();
    conn.execute(
        "INSERT INTO full_fish_194 (brave_star_722, hot_star_957, empty_chair_834)
         VALUES ('sweet_fish_597', 328, 7.71)",
    )
    .unwrap();

    let red_rain_root_page = query_single_i64(
        &conn,
        "SELECT rootpage FROM sqlite_schema
         WHERE type = 'table' AND name = 'red_rain_308'",
    );
    let injector = TargetedYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        btree_write_selection_key(red_rain_root_page),
    );
    conn.set_yield_injector(Some(injector.clone()));
    let mut abandoned = conn
        .prepare(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                926, 'dry_hill_411', 9.24, 'blue_hill_65',
                7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
            )",
        )
        .unwrap();
    assert!(matches!(abandoned.step().unwrap(), StepResult::Yield));
    assert!(injector.fired());
    abandoned.reset().unwrap();
    drop(abandoned);
    conn.set_yield_injector(None);

    conn.execute("DELETE FROM red_rain_308 WHERE slow_desk_238 BETWEEN 2 AND 240")
        .unwrap();
    conn.execute("RELEASE sp_91").unwrap();

    assert!(conn
        .execute(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                927, 'dry_hill_412', 0.24, 'blue_hill_66',
                7.50, 'happy_moon_475', x'736f66745f73746f6e655f313732', 'smart_rock_114'
            )"
        )
        .is_err());

    for id in 300..420 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                1.5, 'wild_dog_{id}', zeroblob(3600), 'reuse_{id}'
            )"
        ))
        .unwrap();
    }

    conn.execute("ROLLBACK").unwrap();
    assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
}

#[test]
fn wal_overflowing_insert_resumes_after_yield_before_balance() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, "resume-overflow-yield.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(
        "CREATE TABLE red_rain_308 (
            slow_desk_238 INTEGER NOT NULL,
            quiet_tree_398 TEXT NOT NULL,
            smart_dog_422 REAL UNIQUE,
            new_hill_140 TEXT,
            light_door_957 REAL,
            wild_dog_803 TEXT NOT NULL,
            new_flower_28 BLOB,
            old_fish_931 TEXT PRIMARY KEY
        )",
    )
    .unwrap();

    for id in 1..260 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
            )"
        ))
        .unwrap();
    }

    let red_rain_root_page = query_single_i64(
        &conn,
        "SELECT rootpage FROM sqlite_schema
         WHERE type = 'table' AND name = 'red_rain_308'",
    );
    let injector = TargetedYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        btree_write_selection_key(red_rain_root_page),
    );
    conn.set_yield_injector(Some(injector.clone()));
    let mut stmt = conn
        .prepare(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                926, 'dry_hill_411', 9.24, 'blue_hill_65',
                7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
            )",
        )
        .unwrap();

    assert!(matches!(stmt.step().unwrap(), StepResult::Yield));
    assert!(injector.fired());
    assert!(matches!(stmt.step().unwrap(), StepResult::Done));
    drop(stmt);
    conn.set_yield_injector(None);

    assert_eq!(
        query_single_i64(
            &conn,
            "SELECT COUNT(*) FROM red_rain_308 WHERE old_fish_931 = 'smart_rock_113'",
        ),
        1
    );
    assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
}

#[allow(dead_code)]
struct Cell {
    pos: usize,
    payload: turso_core::alloc::Vec<u8>,
}

fn validate_btree(pager: Arc<Pager>, page_idx: i64) -> (usize, bool) {
    let num_columns = 5;
    let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
    let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
    while page.is_locked() {
        pager.io.step().unwrap();
    }

    // Pin page in order to not drop it in between
    page.set_dirty();
    let contents = page.get_contents();
    let mut previous_key = None;
    let mut valid = true;
    let mut depth = None;
    #[cfg(debug_assertions)]
    turso_core::storage::btree::debug_validate_cells_core(contents, pager.usable_space());
    let mut child_pages = Vec::new();
    for cell_idx in 0..contents.cell_count() {
        let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
        let current_depth = match cell {
            BTreeCell::TableLeafCell(..) => 1,
            BTreeCell::TableInteriorCell(TableInteriorCell {
                left_child_page, ..
            }) => {
                let (child_page, _c) = cursor.read_page_blocking(left_child_page as i64).unwrap();
                while child_page.is_locked() {
                    pager.io.step().unwrap();
                }
                child_pages.push(child_page);
                if left_child_page == page.get().id() as u32 {
                    valid = false;
                    tracing::error!("left child page is the same as parent {}", left_child_page);
                    continue;
                }
                let (child_depth, child_valid) =
                    validate_btree(pager.clone(), left_child_page as i64);
                valid &= child_valid;
                child_depth
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        };
        if current_depth >= 100 {
            tracing::error!("depth is too big");
            page.clear_dirty();
            return (100, false);
        }
        depth = Some(depth.unwrap_or(current_depth + 1));
        if depth != Some(current_depth + 1) {
            tracing::error!("depth is different for child of page {}", page_idx);
            valid = false;
        }
        match cell {
            BTreeCell::TableInteriorCell(TableInteriorCell { rowid, .. })
            | BTreeCell::TableLeafCell(TableLeafCell { rowid, .. }) => {
                if previous_key.is_some() && previous_key.unwrap() >= rowid {
                    tracing::error!(
                        "keys are in bad order: prev={:?}, current={}",
                        previous_key,
                        rowid
                    );
                    valid = false;
                }
                previous_key = Some(rowid);
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        }
    }
    if let Some(right) = contents.rightmost_pointer().ok().flatten() {
        let (right_depth, right_valid) = validate_btree(pager.clone(), right as i64);
        valid &= right_valid;
        depth = Some(depth.unwrap_or(right_depth + 1));
        if depth != Some(right_depth + 1) {
            tracing::error!("depth is different for child of page {}", page_idx);
            valid = false;
        }
    }
    let first_page_type = child_pages.first_mut().map(|p| {
        if !p.is_loaded() {
            let (new_page, _c) = pager
                .io
                .block(|| pager.read_page(p.get().id() as i64))
                .unwrap();
            *p = new_page;
        }
        while p.is_locked() {
            pager.io.step().unwrap();
        }
        p.get_contents().page_type().ok()
    });
    if let Some(child_type) = first_page_type {
        for page in child_pages.iter_mut().skip(1) {
            if !page.is_loaded() {
                let (new_page, _c) = pager
                    .io
                    .block(|| pager.read_page(page.get().id() as i64))
                    .unwrap();
                *page = new_page;
            }
            while page.is_locked() {
                pager.io.step().unwrap();
            }
            if page.get_contents().page_type().ok() != child_type {
                tracing::error!("child pages have different types");
                valid = false;
            }
        }
    }
    if contents.rightmost_pointer().ok().flatten().is_none() && contents.cell_count() == 0 {
        valid = false;
    }
    page.clear_dirty();
    (depth.unwrap(), valid)
}

fn format_btree(pager: Arc<Pager>, page_idx: i64, depth: usize) -> String {
    let num_columns = 5;

    let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
    let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
    while page.is_locked() {
        pager.io.step().unwrap();
    }

    // Pin page in order to not drop it in between loading of different pages. If not contents will be a dangling reference.
    page.set_dirty();
    let contents = page.get_contents();
    let mut current = Vec::new();
    let mut child = Vec::new();
    for cell_idx in 0..contents.cell_count() {
        let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
        match cell {
            BTreeCell::TableInteriorCell(cell) => {
                current.push(format!(
                    "node[rowid:{}, ptr(<=):{}]",
                    cell.rowid, cell.left_child_page
                ));
                child.push(format_btree(
                    pager.clone(),
                    cell.left_child_page as i64,
                    depth + 2,
                ));
            }
            BTreeCell::TableLeafCell(cell) => {
                current.push(format!(
                    "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                    cell.rowid,
                    cell.payload.len(),
                    cell.first_overflow_page.is_some()
                ));
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        }
    }
    if let Some(rightmost) = contents.rightmost_pointer().ok().flatten() {
        child.push(format_btree(pager, rightmost as i64, depth + 2));
    }
    let current = format!(
        "{}-page:{}, ptr(right):{:?}\n{}+cells:{}",
        " ".repeat(depth),
        page_idx,
        contents.rightmost_pointer().ok().flatten(),
        " ".repeat(depth),
        current.join(", ")
    );
    page.clear_dirty();
    if child.is_empty() {
        current
    } else {
        current + "\n" + &child.join("\n")
    }
}

/// A pager over an empty in-memory database file, opened without a `Database`.
#[allow(clippy::arc_with_non_send_sync)]
#[allow(dead_code)]
fn empty_pager(page_size: u32, page_cache: PageCache) -> Arc<Pager> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);

    let db_file = Arc::new(DatabaseFile::new(
        io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
    ));

    let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
    let wal_shared = WalFileShared::new_shared(wal_file).unwrap();
    let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
    let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
        io.clone(),
        wal_shared,
        last_checksum_and_max_frame,
        buffer_pool.clone(),
    ));

    // `allocate_page1` needs the initial page 1 to be present.
    let shared = Arc::new(SharedPagerState::new(Some(default_page1(None))));
    let pager =
        Arc::new(Pager::new(db_file, Some(wal), io, page_cache, buffer_pool, shared).unwrap());

    pager.io.step().unwrap();
    pager
        .set_initial_page_size(PageSize::new(page_size).unwrap())
        .unwrap();
    pager
}

fn empty_btree_in_database() -> (Arc<Pager>, i64, Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let pager = conn.pager.load().clone();

    // FIXME: handle page cache is full

    // force allocate page1 with a transaction
    pager.begin_read_tx().unwrap();
    run_until_done(
        || pager.begin_write_tx(WalAutoActions::all_enabled()),
        &pager,
    )
    .unwrap();
    run_until_done(
        || pager.commit_tx(&conn, conn.get_sync_mode(), true),
        &pager,
    )
    .unwrap();

    let page2 = run_until_done(|| pager.allocate_page(), &pager).unwrap();
    btree_init_page(&page2, PageType::TableLeaf, 0, pager.usable_space());
    (pager, page2.get().id() as i64, db, conn)
}

fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

fn btree_insert_fuzz_run(attempts: usize, inserts: usize, size: impl Fn(&mut ChaCha8Rng) -> usize) {
    const VALIDATE_INTERVAL: usize = 1000;
    let do_validate_btree =
        std::env::var("VALIDATE_BTREE").is_ok_and(|v| v.parse().expect("validate should be bool"));
    let (mut rng, seed) = rng_from_time_or_env();
    let mut seen = turso_core::HashSet::default();
    tracing::info!("super seed: {}", seed);
    let num_columns = 5;

    for _ in 0..attempts {
        let (pager, root_page, _db, conn) = empty_btree_in_database();
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let mut keys = SortedVec::new();
        tracing::info!("seed: {seed}");
        for insert_id in 0..inserts {
            let do_validate = do_validate_btree || (insert_id % VALIDATE_INTERVAL == 0);
            pager.begin_read_tx().unwrap();
            run_until_done(
                || pager.begin_write_tx(WalAutoActions::all_enabled()),
                &pager,
            )
            .unwrap();
            let size = size(&mut rng);
            let key = {
                let result;
                loop {
                    let key = (rng.next_u64() % (1 << 30)) as i64;
                    if seen.contains(&key) {
                        continue;
                    } else {
                        seen.insert(key);
                    }
                    result = key;
                    break;
                }
                result
            };
            keys.push(key);
            tracing::info!(
                "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                key,
                size,
                insert_id
            );
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(key);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            let regs = &[Register::Value(Value::Blob(
                turso_core::alloc::vec![0; size],
            ))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let btree_before = if do_validate {
                format_btree(pager.clone(), root_page, 0)
            } else {
                "".to_string()
            };
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(key, Some(&value))),
                pager.deref(),
            )
            .unwrap();
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
            pager.begin_read_tx().unwrap();
            // FIXME: add sorted vector instead, should be okay for small amounts of keys for now :P, too lazy to fix right now
            let _c = cursor.move_to_root().unwrap();
            let mut valid = true;
            if do_validate {
                let _c = cursor.move_to_root().unwrap();
                for key in keys.iter() {
                    tracing::trace!("seeking key: {}", key);
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                        .unwrap()
                        .unwrap();
                    if *key != cursor_rowid {
                        valid = false;
                        println!("key {key} is not found, got {cursor_rowid}");
                        break;
                    }
                }
            }
            // let's validate btree too so that we undertsand where the btree failed
            if do_validate
                && (!valid || matches!(validate_btree(pager.clone(), root_page), (_, false)))
            {
                let btree_after = format_btree(pager, root_page, 0);
                println!("btree before:\n{btree_before}");
                println!("btree after:\n{btree_after}");
                panic!("invalid btree");
            }
            pager.end_read_tx();
        }
        pager.begin_read_tx().unwrap();
        tracing::info!(
            "=========== btree ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
            panic!("invalid btree");
        }
        let _c = cursor.move_to_root().unwrap();
        for key in keys.iter() {
            tracing::trace!("seeking key: {}", key);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                .unwrap()
                .unwrap();
            assert_eq!(
                *key, cursor_rowid,
                "key {key} is not found, got {cursor_rowid}"
            );
        }
        pager.end_read_tx();
    }
}

fn btree_index_insert_fuzz_run(attempts: usize, inserts: usize) {
    use turso_core::storage::pager::CreateBTreeFlags;

    let (mut rng, seed) = if std::env::var("SEED").is_ok() {
        let seed = std::env::var("SEED").unwrap();
        let seed = seed.parse::<u64>().unwrap();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    } else {
        rng_from_time_or_env()
    };
    let mut seen = turso_core::HashSet::default();
    tracing::info!("super seed: {}", seed);
    for _ in 0..attempts {
        let (pager, _, _db, conn) = empty_btree_in_database();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many((0..10).map(|i| format!("test{i}"))),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
            constraint_name: None,
        };
        let num_columns = index_def.columns.len();
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, num_columns)
                .unwrap();
        let mut keys = SortedVec::new();
        tracing::info!("seed: {seed}");
        for i in 0..inserts {
            pager.begin_read_tx().unwrap();
            pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                .unwrap();
            let key = {
                let result;
                loop {
                    let cols = (0..num_columns)
                        .map(|_| (rng.next_u64() % (1 << 30)) as i64)
                        .collect::<Vec<_>>();
                    if seen.contains(&cols) {
                        continue;
                    } else {
                        seen.insert(cols.clone());
                    }
                    result = cols;
                    break;
                }
                result
            };
            tracing::info!("insert {}/{}: {:?}", i + 1, inserts, key);
            keys.push(key.clone());
            let regs = key
                .iter()
                .map(|col| Register::Value(Value::from_i64(*col)))
                .collect::<Vec<_>>();
            let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
            run_until_done(
                || {
                    let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                    let key = SeekKey::IndexKey(record.as_record_ref());
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
            let c = cursor.move_to_root().unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
        }

        // Check that all keys can be found by seeking
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in keys.iter().enumerate() {
            tracing::info!("seeking key {}/{}: {:?}", i + 1, keys.len(), key);
            let exists = run_until_done(
                || {
                    let regs = key
                        .iter()
                        .map(|col| Register::Value(Value::from_i64(*col)))
                        .collect::<Vec<_>>();
                    cursor.seek(
                        SeekKey::IndexKey(
                            ImmutableRecord::from_registers(&regs, regs.len())
                                .unwrap()
                                .as_record_ref(),
                        ),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            let mut found = matches!(exists, SeekResult::Found);
            if matches!(exists, SeekResult::TryAdvance) {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                found = cursor.has_record();
            }
            assert!(found, "key {key:?} is not found");
        }
        // Check that key count is right
        let _c = cursor.move_to_root().unwrap();
        let mut count = 0;
        while {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            cursor.has_record
        } {
            count += 1;
        }
        assert_eq!(
            count,
            keys.len(),
            "key count is not right, got {}, expected {}",
            count,
            keys.len()
        );
        // Check that all keys can be found in-order, by iterating the btree
        let _c = cursor.move_to_root().unwrap();
        let mut prev = None;
        for (i, key) in keys.iter().enumerate() {
            tracing::info!("iterating key {}/{}: {:?}", i + 1, keys.len(), key);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            let record = loop {
                match cursor.record().unwrap() {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            };
            let record = record.as_ref().unwrap();
            let cur = record
                .get_values()
                .unwrap()
                .iter()
                .map(|value| value.to_owned().expect(turso_core::alloc::ALLOC_ERR_MSG))
                .collect::<Vec<_>>();
            if let Some(prev) = prev {
                if prev >= cur {
                    println!("Seed: {seed}");
                }
                assert!(
                    prev < cur,
                    "keys are not in ascending order: {prev:?} < {cur:?}",
                );
            }
            prev = Some(cur);
        }
        pager.end_read_tx();
    }
}

fn btree_index_insert_delete_fuzz_run(
    attempts: usize,
    operations: usize,
    size: impl Fn(&mut ChaCha8Rng) -> usize,
    insert_chance: f64,
) {
    use turso_core::storage::pager::CreateBTreeFlags;

    let (mut rng, seed) = if std::env::var("SEED").is_ok() {
        let seed = std::env::var("SEED").unwrap();
        let seed = seed.parse::<u64>().unwrap();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    } else {
        rng_from_time_or_env()
    };
    let mut seen = turso_core::HashSet::default();
    tracing::info!("super seed: {}", seed);

    for _ in 0..attempts {
        let (pager, _, _db, conn) = empty_btree_in_database();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["testcol"]),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
            constraint_name: None,
        };
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

        // Track expected keys that should be present in the tree
        let mut expected_keys = Vec::new();

        tracing::info!("seed: {seed}");
        for i in 0..operations {
            let print_progress = i % 100 == 0;
            pager.begin_read_tx().unwrap();

            pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                .unwrap();

            // Decide whether to insert or delete (80% chance of insert)
            let is_insert = rng.next_u64() % 100 < (insert_chance * 100.0) as u64;

            if is_insert {
                // Generate a unique key for insertion
                let key = {
                    let result;
                    loop {
                        let sizeof_blob = size(&mut rng);
                        let blob = (0..sizeof_blob)
                            .map(|_| (rng.next_u64() % 256) as u8)
                            .collect::<Vec<_>>();
                        if seen.contains(&blob) {
                            continue;
                        } else {
                            seen.insert(blob.clone());
                        }
                        result = blob;
                        break;
                    }
                    result
                };

                if print_progress {
                    tracing::info!("insert {}/{}, seed: {seed}", i + 1, operations);
                }
                expected_keys.push(key.clone());

                let regs = vec![Register::Value(
                    Value::from_slice(&key).expect(turso_core::alloc::ALLOC_ERR_MSG),
                )];
                let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                let seek_result = run_until_done(
                    || {
                        let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                        let key = SeekKey::IndexKey(record.as_record_ref());
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                if let SeekResult::TryAdvance = seek_result {
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                }
                run_until_done(
                    || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                    pager.deref(),
                )
                .unwrap();
            } else {
                // Delete a random existing key
                if !expected_keys.is_empty() {
                    let delete_idx = rng.next_u64() as usize % expected_keys.len();
                    let key_to_delete = expected_keys[delete_idx].clone();

                    if print_progress {
                        tracing::info!("delete {}/{}, seed: {seed}", i + 1, operations);
                    }

                    let regs = vec![Register::Value(
                        Value::from_slice(&key_to_delete).expect(turso_core::alloc::ALLOC_ERR_MSG),
                    )];
                    let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                    // Seek to the key to delete
                    let seek_result = run_until_done(
                        || {
                            cursor.seek(
                                SeekKey::IndexKey(record.as_record_ref()),
                                SeekOp::GE { eq_only: true },
                            )
                        },
                        pager.deref(),
                    )
                    .unwrap();
                    let mut found = matches!(seek_result, SeekResult::Found);
                    if matches!(seek_result, SeekResult::TryAdvance) {
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                        found = cursor.has_record()
                    }
                    assert!(found, "expected key {key_to_delete:?} is not found");

                    // Delete the key
                    run_until_done(|| cursor.delete(), pager.deref()).unwrap();

                    // Remove from expected keys
                    expected_keys.remove(delete_idx);
                }
            }

            let c = cursor.move_to_root().unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
        }

        // Final validation
        let mut sorted_keys = expected_keys.clone();
        sorted_keys.sort();
        validate_expected_keys(&pager, &mut cursor, &sorted_keys, seed);

        pager.end_read_tx();
    }
}

fn validate_expected_keys(
    pager: &Arc<Pager>,
    cursor: &mut BTreeCursor,
    expected_keys: &[Vec<u8>],
    seed: u64,
) {
    // Check that all expected keys can be found by seeking
    pager.begin_read_tx().unwrap();
    let _c = cursor.move_to_root().unwrap();
    for (i, key) in expected_keys.iter().enumerate() {
        tracing::info!(
            "validating key {}/{}, seed: {seed}",
            i + 1,
            expected_keys.len()
        );
        let exists = run_until_done(
            || {
                let regs = vec![Register::Value(
                    Value::from_slice(key).expect(turso_core::alloc::ALLOC_ERR_MSG),
                )];
                cursor.seek(
                    SeekKey::IndexKey(
                        ImmutableRecord::from_registers(&regs, regs.len())
                            .unwrap()
                            .as_record_ref(),
                    ),
                    SeekOp::GE { eq_only: true },
                )
            },
            pager.deref(),
        )
        .unwrap();
        let mut found = matches!(exists, SeekResult::Found);
        if matches!(exists, SeekResult::TryAdvance) {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            found = cursor.has_record();
        }
        assert!(found, "expected key {key:?} is not found");
    }

    // Check key count
    let _c = cursor.move_to_root().unwrap();
    run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
    if !cursor.has_record() {
        panic!("no keys in tree");
    }
    let mut count = 1;
    loop {
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
        if !cursor.has_record() {
            break;
        }
        count += 1;
    }
    assert_eq!(
        count,
        expected_keys.len(),
        "key count is not right, got {}, expected {}, seed: {seed}",
        count,
        expected_keys.len()
    );

    // Check that all keys can be found in-order, by iterating the btree
    let _c = cursor.move_to_root().unwrap();
    for (i, key) in expected_keys.iter().enumerate() {
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
        tracing::info!(
            "iterating key {}/{}, cursor stack cur idx: {:?}, cursor stack depth: {:?}, seed: {seed}",
            i + 1,
            expected_keys.len(),
            cursor.stack.current_cell_index(),
            cursor.stack.current()
        );
        let record = loop {
            match cursor.record().unwrap() {
                IOResult::Done(r) => break r,
                IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
            }
        };
        let record = record.as_ref().unwrap();
        let cur = record.get_value(0).expect("expected at least one column");
        let ValueRef::Blob(ref cur) = cur else {
            panic!("expected blob, got {cur:?}");
        };
        assert_eq!(cur, key, "key {key:?} is not found, seed: {seed}");
    }
    pager.end_read_tx();
}

#[test]
pub fn btree_insert_fuzz_run_equal_size() {
    for size in 1..8 {
        tracing::info!("======= size:{} =======", size);
        btree_insert_fuzz_run(2, 1024, |_| size);
    }
}

#[test]
pub fn btree_index_insert_fuzz_run_equal_size() {
    btree_index_insert_fuzz_run(2, 1024);
}

#[test]
pub fn btree_index_insert_delete_fuzz_run_test() {
    btree_index_insert_delete_fuzz_run(
        2,
        2000,
        |rng| {
            let min: u32 = 4;
            let size = min + rng.next_u32() % (1024 - min);
            size as usize
        },
        0.65,
    );
}

#[test]
pub fn btree_insert_fuzz_run_random() {
    btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_small() {
    btree_insert_fuzz_run(1, 100, |rng| (rng.next_u32() % 128) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_big() {
    btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_overflow() {
    btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_equal_size() {
    for size in 1..8 {
        tracing::info!("======= size:{} =======", size);
        btree_insert_fuzz_run(2, 10_000, |_| size);
    }
}

#[test]
#[ignore]
pub fn fuzz_long_btree_index_insert_fuzz_run_equal_size() {
    btree_index_insert_fuzz_run(2, 10_000);
}

/// Re-insert an already-present index key the way FTS's RowInserter
/// does — seek `GE { eq_only: true }`, advance once on `TryAdvance`,
/// insert at the cursor — after delete churn that leaves stale interior
/// dividers. `insert` only checks the cursor's current cell for an
/// exact-key overwrite, so a caller that skips the advance lands one
/// leaf early and writes a second physical copy of the key (the whopper
/// duplicate-registry-row corruption). The full scan must see every key
/// exactly once.
#[test]
pub fn btree_index_reinsert_after_tryadvance_never_duplicates() {
    use turso_core::storage::pager::CreateBTreeFlags;
    let (mut rng, seed) = rng_from_time_or_env();
    tracing::info!("seed: {seed}");
    for attempt in 0..8 {
        let (pager, _, _db, conn) = empty_btree_in_database();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["testcol"]),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
            constraint_name: None,
        };
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

        let key_bytes = |i: u64| -> Vec<u8> {
            let mut k = vec![0u8; 128];
            k[..8].copy_from_slice(&i.to_be_bytes());
            k
        };
        let record_for = |k: &[u8]| {
            let regs = vec![Register::Value(Value::from_slice(k).unwrap())];
            ImmutableRecord::from_registers(&regs, regs.len()).unwrap()
        };

        let mut live: Vec<u64> = Vec::new();
        pager.begin_read_tx().unwrap();
        pager
            .io
            .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
            .unwrap();
        for i in 0..600u64 {
            let record = record_for(&key_bytes(i));
            run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
            live.push(i);
        }
        // Delete a random half to churn interior dividers.
        for _ in 0..300 {
            let idx = rng.next_u64() as usize % live.len();
            let victim = live.swap_remove(idx);
            let record = record_for(&key_bytes(victim));
            let seek_result = run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            if matches!(seek_result, SeekResult::TryAdvance) {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            run_until_done(|| cursor.delete(), pager.deref()).unwrap();
        }
        // Re-insert EVERY surviving key with RowInserter's protocol:
        // seek GE eq_only, advance once on TryAdvance, insert.
        let mut tryadvance_reinserts = 0usize;
        for &survivor in &live {
            let record = record_for(&key_bytes(survivor));
            let seek_result = run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            if matches!(seek_result, SeekResult::TryAdvance) {
                tryadvance_reinserts += 1;
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
        }
        let c = cursor.move_to_root().unwrap();
        if let Some(c) = c {
            pager.io.wait_for_completion(c).unwrap();
        }
        pager
            .io
            .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
            .unwrap();

        // Full scan: every key must appear exactly once.
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while cursor.has_record() {
            let bytes = loop {
                match cursor.record().unwrap() {
                    IOResult::Done(record) => {
                        let record = record.unwrap();
                        break record
                            .get_value_opt(0)
                            .and_then(|v| match v {
                                turso_core::types::ValueRef::Blob(b) => Some(b.to_vec()),
                                _ => None,
                            })
                            .unwrap();
                    }
                    IOResult::IO(io) => {
                        while !io.finished() {
                            pager.io.step().unwrap();
                        }
                    }
                }
            };
            seen.push(bytes);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
        }
        pager.end_read_tx();
        let total = seen.len();
        seen.dedup();
        assert_eq!(
            total,
            seen.len(),
            "attempt {attempt}: duplicate index keys after RowInserter-style \
             re-inserts (seed {seed}, {tryadvance_reinserts} TryAdvance re-inserts)"
        );
        assert_eq!(total, live.len(), "attempt {attempt}: scan count mismatch");
        tracing::info!(
            "attempt {attempt}: {} keys, {} TryAdvance re-inserts, no duplicates",
            total,
            tryadvance_reinserts
        );
    }
}

#[test]
#[ignore]
pub fn fuzz_long_btree_index_insert_delete_fuzz_run() {
    btree_index_insert_delete_fuzz_run(
        2,
        10000,
        |rng| {
            let min: u32 = 4;
            let size = min + rng.next_u32() % (1024 - min);
            size as usize
        },
        0.65,
    );
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_random() {
    btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 4096) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_small() {
    btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 128) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_big() {
    btree_insert_fuzz_run(2, 10_000, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_overflow() {
    btree_insert_fuzz_run(2, 5_000, |rng| (rng.next_u32() % 32 * 1024) as usize);
}

#[allow(dead_code)]
fn setup_test_env(database_size: u32) -> Arc<Pager> {
    let pager = empty_pager(512, PageCache::new(10));
    let _ = run_until_done(|| pager.allocate_page1(), &pager);
    for _ in 0..(database_size - 1) {
        let _res = pager.allocate_page().unwrap();
    }

    pager
}

fn run_until_done<T>(action: impl FnMut() -> IOResultOr<T>, pager: &Pager) -> Result<T> {
    pager.io.block(action)
}
