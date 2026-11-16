#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
use turso_core::storage::wal::{
    classify_authority_snapshot_against_wal, AuthoritySnapshotRebuildReason,
    AuthoritySnapshotValidation,
};
#[allow(unused_imports)]
use turso_core::util::IOExt as _;
use turso_sqlite::SqliteDialect;

use turso_core::security::roles::SessionRole;

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
use turso_core::storage::shared_wal_coordination::SharedWalCoordinationHeader;

use turso_core::sync::{atomic::Ordering, Arc};

use turso_core::sync::{Mutex, RwLock};

use turso_core::{
    storage::{
        buffer_pool::BufferPool,
        sqlite3_ondisk::{self, WAL_HEADER_SIZE},
        wal::READMARK_NOT_USED,
    },
    types::IOResult,
    util::IOExt,
    CheckpointMode, CheckpointResult, Completion, ConnectOpts, Connection, Database, LimboError,
    PlatformIO, SyncMode, WalFileShared, IO,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

/// Returns an IO backend that supports shared WAL coordination on the host.
/// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
/// and mapping primitives, so the experimental IOCP backend is used when
/// the `experimental_win_iocp` feature is enabled.
fn shared_wal_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "win_iocp"))]
    {
        Arc::new(turso_core::WindowsIOCP::new().unwrap())
    }
    #[cfg(not(all(target_os = "windows", feature = "win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) fn get_database() -> (Arc<Database>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("test.db");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();
    }
    let io = shared_wal_test_io();
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_multiprocess_wal(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    // db + tmp directory
    (db, temp_dir)
}

#[test]
fn test_truncate_file() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    let _ = conn.execute("insert into test (value) values ('test1'), ('test2'), ('test3')");
    let wal = db.shared_wal.write();
    let wal_file = wal.runtime.file.as_ref().unwrap().clone();
    let done = Arc::new(Mutex::new(false));
    let _done = done.clone();
    let _ = wal_file.truncate(
        WAL_HEADER_SIZE as u64,
        Completion::new_trunc(move |_| {
            *_done.lock() = true;
        }),
    );
    assert!(wal_file.size().unwrap() == WAL_HEADER_SIZE as u64);
    assert!(*done.lock());
}

fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
    for _ in 0..n_txns {
        conn.execute("begin transaction").unwrap();
        for i in 0..rows_per_txn {
            conn.execute(format!("insert into test(value) values ('v{i}')"))
                .unwrap();
        }
        conn.execute("commit").unwrap();
    }
}

fn count_test_table(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn run_checkpoint_until_done(pager: &turso_core::Pager, mode: CheckpointMode) -> CheckpointResult {
    // Use pager.checkpoint() instead of wal.checkpoint() directly because
    // WAL truncation (for TRUNCATE mode) now happens in pager's TruncateWalFile phase.
    pager
        .io
        .block(|| pager.checkpoint(mode, turso_core::SyncMode::Full, true))
        .unwrap()
}

#[test]
fn in_process_checkpoint_with_no_frames_to_backfill_compares_no_frames() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("late-reader.db");
    let db = Database::open_file(
        shared_wal_test_io(),
        path.to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&writer, 20, 10);
    let reader = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    reader.execute("begin").unwrap();
    let reader_rows = count_test_table(&reader);
    let pager = writer.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    let held_frame = wal.get_max_frame_in_wal();
    bulk_inserts(&writer, 20, 10);
    assert!(wal.get_max_frame_in_wal() > held_frame);
    let passive = CheckpointMode::Passive {
        upper_bound_inclusive: None,
    };
    let frames_compared = || {
        db.shared_wal
            .read()
            .runtime
            .take_frames_compared_for_tests()
    };
    frames_compared();
    let checkpoint = run_checkpoint_until_done(&pager, passive);
    assert_eq!(checkpoint.wal_total_backfilled, held_frame);
    assert!(frames_compared() > 0);

    let checkpoint = run_checkpoint_until_done(&pager, passive);
    assert_eq!(checkpoint.wal_total_backfilled, held_frame);
    assert_eq!(checkpoint.wal_checkpoint_backfilled, 0);
    assert_eq!(frames_compared(), 0);
    assert_eq!(count_test_table(&reader), reader_rows);
    reader.execute("commit").unwrap();
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn write_test_wal_with_single_commit_frame(
    io: &Arc<dyn IO>,
    wal_path: &std::path::Path,
) -> SharedWalCoordinationHeader {
    let file = io
        .open_file(
            wal_path.to_str().unwrap(),
            turso_core::OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut wal_header = sqlite3_ondisk::WalHeader {
        page_size: 4096,
        checkpoint_seq: 5,
        salt_1: 17,
        salt_2: 23,
        ..sqlite3_ondisk::WalHeader::new()
    };
    let use_native_endian = cfg!(target_endian = "big") == ((wal_header.magic & 1) != 0);
    let mut header_prefix = [0u8; WAL_HEADER_SIZE - 8];
    header_prefix[0..4].copy_from_slice(&wal_header.magic.to_be_bytes());
    header_prefix[4..8].copy_from_slice(&wal_header.file_format.to_be_bytes());
    header_prefix[8..12].copy_from_slice(&wal_header.page_size.to_be_bytes());
    header_prefix[12..16].copy_from_slice(&wal_header.checkpoint_seq.to_be_bytes());
    header_prefix[16..20].copy_from_slice(&wal_header.salt_1.to_be_bytes());
    header_prefix[20..24].copy_from_slice(&wal_header.salt_2.to_be_bytes());
    let header_checksum =
        sqlite3_ondisk::checksum_wal(&header_prefix, &wal_header, (0, 0), use_native_endian);
    wal_header.checksum_1 = header_checksum.0;
    wal_header.checksum_2 = header_checksum.1;

    io.wait_for_completion(
        sqlite3_ondisk::begin_write_wal_header(file.as_ref(), &wal_header, None).unwrap(),
    )
    .unwrap();

    let buffer_pool = BufferPool::begin_init(io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(wal_header.page_size as usize)
        .unwrap();
    #[allow(unused_mut)]
    let mut page = vec![0x5a; wal_header.page_size as usize];
    #[cfg(feature = "checksum")]
    turso_core::storage::checksum::ChecksumContext::new()
        .add_checksum_to_page(&mut page, 7)
        .unwrap();
    let (frame_checksum, frame_buf) = sqlite3_ondisk::prepare_wal_frame(
        &buffer_pool,
        &wal_header,
        header_checksum,
        wal_header.page_size,
        7,
        1,
        &page,
    );
    let c = file
        .pwrite(
            WAL_HEADER_SIZE as u64,
            frame_buf,
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let c = file
        .sync(
            Completion::new_sync(|_| {}),
            turso_core::io::FileSyncType::Fsync,
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    SharedWalCoordinationHeader {
        max_frame: 1,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: wal_header.checkpoint_seq,
        checkpoint_epoch: 7,
        page_size: wal_header.page_size,
        salt_1: wal_header.salt_1,
        salt_2: wal_header.salt_2,
        checksum_1: frame_checksum.0,
        checksum_2: frame_checksum.1,
        reader_slot_count: 64,
    }
}

#[cfg(test)]
fn read_slots_with_readers(shared: &WalFileShared) -> Vec<usize> {
    shared
        .runtime
        .read_locks
        .iter()
        .enumerate()
        .filter_map(|(slot, lock)| {
            let state = lock.0.load(Ordering::Acquire);
            let has_readers =
                (state & turso_core::storage::wal::TursoRwLock::READER_COUNT_MASK) != 0;
            has_readers.then_some(slot)
        })
        .collect()
}

fn wal_header_snapshot(shared: &Arc<RwLock<WalFileShared>>) -> (u32, u32, u32, u32) {
    // (checkpoint_seq, salt1, salt2, page_size)
    let shared_guard = shared.read();
    let hdr = shared_guard.metadata.wal_header.lock();
    (hdr.checkpoint_seq, hdr.salt_1, hdr.salt_2, hdr.page_size)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn test_restart_checkpoint_clears_backfill_proof_and_later_replaces_it() {
    let (db, path) = get_database();
    let wal_path = path.path().join("test.db-wal");
    let wal_path_str = wal_path.to_str().unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let partial = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
        "setup must create a partial checkpoint with a positive durable backfill proof"
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before_restart = authority.snapshot();
    let (db_size_before, db_crc_before) =
        turso_core::storage::wal::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_before_restart, db_size_before, db_crc_before),
        "setup must install a valid proof before RESTART"
    );

    let restart = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
    assert!(
        restart.everything_backfilled(),
        "RESTART should fully backfill before resetting the WAL generation"
    );

    let snapshot_after_restart = authority.snapshot();
    assert_eq!(snapshot_after_restart.max_frame, 0);
    assert_eq!(snapshot_after_restart.nbackfills, 0);
    assert!(
        !authority.validate_backfill_proof(snapshot_before_restart, db_size_before, db_crc_before),
        "RESTART must clear the proof for the old WAL generation"
    );

    bulk_inserts(&conn, 6, 2);
    let replacement = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
        "replacement setup must create a new partial checkpoint after RESTART"
    );

    let snapshot_after_replacement = authority.snapshot();
    let (db_size_after, db_crc_after) =
        turso_core::storage::wal::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_after_replacement, db_size_after, db_crc_after),
        "partial checkpoint after RESTART must install a replacement proof for the new generation"
    );
    assert_ne!(
        snapshot_after_replacement.checkpoint_seq, snapshot_before_restart.checkpoint_seq,
        "replacement proof must belong to the restarted WAL generation"
    );
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn test_truncate_checkpoint_clears_backfill_proof_and_later_replaces_it() {
    let (db, path) = get_database();
    let wal_path = path.path().join("test.db-wal");
    let wal_path_str = wal_path.to_str().unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let partial = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
        "setup must create a partial checkpoint with a positive durable backfill proof"
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before_truncate = authority.snapshot();
    let (db_size_before, db_crc_before) =
        turso_core::storage::wal::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_before_truncate, db_size_before, db_crc_before),
        "setup must install a valid proof before TRUNCATE"
    );

    let truncate = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    assert!(
        truncate.everything_backfilled(),
        "TRUNCATE should fully backfill before truncating the WAL"
    );

    let snapshot_after_truncate = authority.snapshot();
    assert_eq!(snapshot_after_truncate.max_frame, 0);
    assert_eq!(snapshot_after_truncate.nbackfills, 0);
    assert!(
        !authority.validate_backfill_proof(snapshot_before_truncate, db_size_before, db_crc_before),
        "TRUNCATE must clear the proof for the truncated WAL generation"
    );
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        0,
        "TRUNCATE must leave the WAL file empty before the new generation begins"
    );

    bulk_inserts(&conn, 6, 2);
    let replacement = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
        "replacement setup must create a new partial checkpoint after TRUNCATE"
    );

    let snapshot_after_replacement = authority.snapshot();
    let (db_size_after, db_crc_after) =
        turso_core::storage::wal::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_after_replacement, db_size_after, db_crc_after),
        "partial checkpoint after TRUNCATE must install a replacement proof for the new generation"
    );
    assert_ne!(
        snapshot_after_replacement.checkpoint_seq, snapshot_before_truncate.checkpoint_seq,
        "replacement proof must belong to the truncated WAL generation"
    );
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn test_classify_authority_snapshot_marks_truncated_wal_for_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-truncated.db-wal");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);

    let wal_len = std::fs::metadata(&wal_path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap()
        .set_len(wal_len - 1)
        .unwrap();

    assert_eq!(
        classify_authority_snapshot_against_wal(
            &io,
            &io.open_file(
                wal_path.to_str().unwrap(),
                turso_core::OpenFlags::Create,
                false
            )
            .unwrap(),
            snapshot,
        )
        .unwrap(),
        AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalLengthMismatch
        )
    );
}

#[test]
fn restart_checkpoint_reset_wal_state_handling() {
    let (db, path) = get_database();

    let walpath = path.path().join("test.db-wal");

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 20, 3);
    let IOResult::Done(completions) = conn.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Snapshot header & counters before the RESTART checkpoint.
    let wal_shared = db.shared_wal.clone();
    let (seq_before, salt1_before, salt2_before, _ps_before) = wal_header_snapshot(&wal_shared);
    let (mx_before, backfill_before) = {
        let s = wal_shared.read();
        (
            s.metadata.max_frame.load(Ordering::SeqCst),
            s.metadata.nbackfills.load(Ordering::SeqCst),
        )
    };
    assert!(mx_before > 0);
    assert_eq!(backfill_before, 0);

    let meta_before = std::fs::metadata(&walpath).unwrap();
    #[cfg(unix)]
    let size_before = meta_before.blocks();
    #[cfg(not(unix))]
    let size_before = meta_before.len();
    // Run a RESTART checkpoint, should backfill everything and reset WAL counters,
    // but NOT truncate the file.
    {
        let pager = conn.pager.load();
        let res = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert_eq!(res.wal_max_frame, mx_before);
        assert_eq!(res.wal_total_backfilled, mx_before);
        assert_eq!(res.wal_checkpoint_backfilled, mx_before);
    }

    // Validate post‑RESTART header & counters.
    let (seq_after, salt1_after, salt2_after, _ps_after) = wal_header_snapshot(&wal_shared);
    assert_eq!(
        seq_after,
        seq_before.wrapping_add(1),
        "checkpoint_seq must increment on RESTART"
    );
    assert_eq!(
        salt1_after,
        salt1_before.wrapping_add(1),
        "salt_1 is incremented"
    );
    assert_ne!(salt2_after, salt2_before, "salt_2 is randomized");

    let (mx_after, backfill_after) = {
        let s = wal_shared.read();
        (
            s.metadata.max_frame.load(Ordering::SeqCst),
            s.metadata.nbackfills.load(Ordering::SeqCst),
        )
    };
    assert_eq!(mx_after, 0, "mxFrame reset to 0 after RESTART");
    assert_eq!(backfill_after, 0, "nBackfill reset to 0 after RESTART");

    // File size should be unchanged for RESTART (no truncate).
    let meta_after = std::fs::metadata(&walpath).unwrap();
    #[cfg(unix)]
    let size_after = meta_after.blocks();
    #[cfg(not(unix))]
    let size_after = meta_after.len();
    assert_eq!(
        size_before, size_after,
        "RESTART must not change WAL file size"
    );

    // Next write should start a new sequence at frame 1.
    conn.execute("insert into test(value) values ('post_restart')")
        .unwrap();
    conn.pager
        .load()
        .wal
        .as_ref()
        .unwrap()
        .finish_append_frames_commit()
        .unwrap();
    let new_max = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);
    assert_eq!(new_max, 1, "first append after RESTART starts at frame 1");
}

#[test]
fn test_wal_read_marks_after_restart() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 5);
    // Checkpoint with restart
    {
        let pager = conn.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(result.everything_backfilled());
    }

    // Verify read marks after restart
    let read_marks_after: Vec<_> = {
        let s = wal_shared.read();
        (0..5)
            .map(|i| s.runtime.read_locks[i].get_value())
            .collect()
    };

    assert_eq!(read_marks_after[0], 0, "Slot 0 should remain 0");
    assert_eq!(
        read_marks_after[1], 0,
        "Slot 1 (default reader) should be reset to 0"
    );
    for (i, item) in read_marks_after.iter().take(5).skip(2).enumerate() {
        assert_eq!(
            *item, READMARK_NOT_USED,
            "Slot {i} should be READMARK_NOT_USED after restart",
        );
    }
}

#[test]
fn test_wal_checkpoint_updates_read_marks() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 5);

    // get max frame before checkpoint
    let max_frame_before = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);

    {
        let pager = conn.pager.load();
        let _result = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }

    // check that read mark 1 (default reader) was updated to max_frame
    let read_mark_1 = wal_shared.read().runtime.read_locks[1].get_value();

    assert_eq!(
        read_mark_1 as u64, max_frame_before,
        "Read mark 1 should be updated to max frame during checkpoint"
    );
}

fn check_read_lock_slot(conn: &Arc<Connection>, _expected_slot: usize) -> bool {
    let pager = conn.pager.load();
    let _wal = pager.wal.as_ref().unwrap();
    #[cfg(debug_assertions)]
    {
        let wal_any = _wal.as_any();
        if let Some(wal_file) = wal_any.downcast_ref::<turso_core::WalFile>() {
            return wal_file.max_frame_read_lock_index.load(Ordering::Acquire) == _expected_slot;
        }
    }

    false
}

#[test]
fn test_wal_readlock0_optimization_behavior() {
    let (db, _path) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 5, 5);
    // Do a full checkpoint to move all data to DB file
    {
        let pager = conn1.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }

    // Start a read transaction on conn2
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
    }
    // should use slot 0, as everything is backfilled
    assert!(check_read_lock_slot(&conn2, 0));
    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let frame = wal.find_frame(5, None);
        // since we hold readlock0, we should ignore the db file and find_frame should return none
        assert!(frame.is_ok_and(|f| f.is_none()));
    }
    // Try checkpoint, should fail because reader has slot 0
    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let result = wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full);

        assert!(
            matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
            "RESTART checkpoint should fail when a reader is using slot 0"
        );
    }
    // End the read transaction
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.end_read_tx();
    }
    {
        let pager = conn1.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "RESTART checkpoint should succeed after reader releases slot 0"
        );
    }
}

#[test]
fn test_rollback_releases_shared_read_lock_slot() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    let locked_slots_before = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert_eq!(
        locked_slots_before.len(),
        1,
        "expected exactly one shared read-lock slot while transaction is active"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release the shared read-lock slot"
    );
}

#[test]
fn test_rollback_releases_slot_zero_read_lock() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 3, 3);
    {
        let pager = conn.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "restart checkpoint setup must fully backfill WAL"
        );
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test(value) VALUES('slot0')")
        .unwrap();

    let locked_slots_before = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert_eq!(
        locked_slots_before,
        vec![0],
        "writer should use slot 0 when WAL is fully checkpointed"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release slot 0 shared read-lock as well"
    );
}

#[test]
fn test_savepoint_then_tx_rollback_allows_restart_checkpoint_from_other_connection() {
    let (db, _path) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 2, 2);
    let count_before = count_test_table(&conn1);

    conn1.execute("BEGIN").unwrap();
    conn1
        .execute("INSERT INTO test(id, value) VALUES(1000, 'first')")
        .unwrap();
    let duplicate = conn1.execute("INSERT INTO test(id, value) VALUES(1000, 'dup')");
    assert!(duplicate.is_err(), "duplicate PK insert must fail");

    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must still be held after savepoint rollback"
        );
        assert!(
            wal.holds_write_lock(),
            "write lock must still be held after savepoint rollback"
        );
    }

    conn1.execute("ROLLBACK").unwrap();

    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            !wal.holds_read_lock(),
            "read lock must be released after transaction rollback"
        );
        assert!(
            !wal.holds_write_lock(),
            "write lock must be released after transaction rollback"
        );
    }

    let locked_slots_after_rollback = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after_rollback.is_empty(),
        "transaction rollback after savepoint failure must not leak shared read locks"
    );
    assert_eq!(
        count_test_table(&conn1),
        count_before,
        "transaction rollback should remove writes made before savepoint failure"
    );

    let result = {
        let pager = conn2.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Restart)
    };
    assert!(
        result.everything_backfilled(),
        "restart checkpoint from another connection must succeed after full rollback"
    );
}
