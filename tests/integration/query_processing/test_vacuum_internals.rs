use std::sync::Arc;
use turso_core::namespace::NamespaceId;
use turso_core::schema::SequencesByNamespace;
use turso_core::storage::sqlite3_ondisk::CacheSize;
use turso_core::storage::sqlite3_ondisk::DatabaseHeader;
use turso_core::storage::sqlite3_ondisk::RawVersion;
use turso_core::storage::sqlite3_ondisk::TextEncoding;
use turso_core::vdbe::vacuum::capture_custom_types;
use turso_core::vdbe::vacuum::capture_target_metadata;
use turso_core::vdbe::vacuum::finalize_vacuum_into_output;
use turso_core::vdbe::vacuum::finalize_vacuum_target_header;
use turso_core::vdbe::vacuum::install_committed_vacuum_image;
use turso_core::vdbe::vacuum::mirror_symbols;
use turso_core::vdbe::vacuum::open_vacuum_temp_db;
use turso_core::vdbe::vacuum::replace_shared_schema_after_vacuum;
use turso_core::vdbe::vacuum::vacuum_in_place_cleanup;
use turso_core::vdbe::vacuum::vacuum_target_build_step;
use turso_core::vdbe::vacuum::vacuum_target_opts_from_source;
use turso_core::vdbe::vacuum::VacuumCommittedImageMeta;
use turso_core::vdbe::vacuum::VacuumDbHeaderMeta;
use turso_core::vdbe::vacuum::VacuumInPlaceCleanupState;
use turso_core::vdbe::vacuum::VacuumInPlacePhase;
use turso_core::vdbe::vacuum::VacuumTargetBuildConfig;
use turso_core::vdbe::vacuum::VacuumTargetBuildContext;
use turso_core::vdbe::vacuum::VacuumTargetBuildPhase;
use turso_core::Database;
use turso_core::EncryptionOpts;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

use turso_core::io::{FileId, FileSyncType};

use turso_core::mvcc::yield_hooks::YieldPointMarker;

use turso_core::mvcc::yield_points::{YieldInjector, YieldPoint};

use turso_core::schema::Schema;

use turso_core::security::roles::SessionRole;

use turso_core::storage::encryption::{CipherMode, EncryptionKey};

use turso_core::util::IOExt;

use turso_core::vdbe::execute::TransactionYieldPoint;

use turso_core::{
    Buffer, Clock, Completion, ConnectOpts, DatabaseOpts, File, MemoryIO, MonotonicInstant,
    OpenFlags, WallClockInstant, IO,
};

use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SyncCounts {
    db: usize,
    wal: usize,
}

struct SyncCountingIo {
    inner: Arc<MemoryIO>,
    tracked_db_path: String,
    tracked_wal_path: String,
    db_syncs: Arc<AtomicUsize>,
    wal_syncs: Arc<AtomicUsize>,
}

impl SyncCountingIo {
    fn new(tracked_db_path: &str) -> Self {
        Self {
            inner: Arc::new(MemoryIO::new()),
            tracked_db_path: tracked_db_path.to_string(),
            tracked_wal_path: format!("{tracked_db_path}-wal"),
            db_syncs: Arc::new(AtomicUsize::new(0)),
            wal_syncs: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn counts(&self) -> SyncCounts {
        SyncCounts {
            db: self.db_syncs.load(StdOrdering::SeqCst),
            wal: self.wal_syncs.load(StdOrdering::SeqCst),
        }
    }
}

impl Clock for SyncCountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for SyncCountingIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(SyncCountingFile {
            inner,
            count_as_db_sync: path == self.tracked_db_path,
            count_as_wal_sync: path == self.tracked_wal_path,
            db_syncs: self.db_syncs.clone(),
            wal_syncs: self.wal_syncs.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> Result<()> {
        self.inner.step()
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.inner.drain_completions(completions)
    }

    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        self.inner.cancel(completions)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

struct SyncCountingFile {
    inner: Arc<dyn File>,
    count_as_db_sync: bool,
    count_as_wal_sync: bool,
    db_syncs: Arc<AtomicUsize>,
    wal_syncs: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct OneShotTransactionYieldInjector {
    yielded: AtomicUsize,
}

impl OneShotTransactionYieldInjector {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            yielded: AtomicUsize::new(0),
        })
    }

    fn yielded(&self) -> bool {
        self.yielded.load(StdOrdering::SeqCst) != 0
    }
}

impl YieldInjector for OneShotTransactionYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        point == TransactionYieldPoint::BeforeStart.point()
            && self
                .yielded
                .compare_exchange(0, 1, StdOrdering::SeqCst, StdOrdering::SeqCst)
                .is_ok()
    }
}

impl File for SyncCountingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        if self.count_as_db_sync {
            self.db_syncs.fetch_add(1, StdOrdering::SeqCst);
        }
        if self.count_as_wal_sync {
            self.wal_syncs.fetch_add(1, StdOrdering::SeqCst);
        }
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}

#[test]
fn replace_shared_schema_after_vacuum_replaces_wrapped_schema_cookie() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;

    db.with_schema_mut(|schema| {
        schema.schema_version = u32::MAX;
        Ok(())
    })?;

    let mut replacement = Schema::with_options(true, &SqliteDialect).unwrap();
    replacement.schema_version = 0;
    let replacement = Arc::new(replacement);
    replace_shared_schema_after_vacuum(db.as_ref(), replacement.clone());

    let schema = db.clone_schema();
    assert_eq!(schema.schema_version, 0);
    assert!(Arc::ptr_eq(&schema, &replacement));

    Ok(())
}

#[test]
fn install_committed_vacuum_image_updates_connection_and_shared_schema() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;

    conn.with_schema_mut(|schema| {
        schema.schema_version = 1;
    })?;
    db.with_schema_mut(|schema| {
        schema.schema_version = 1;
        Ok(())
    })?;

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 42.into();
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    schema.schema_version = 42;
    let schema = Arc::new(schema);
    let committed = VacuumCommittedImageMeta {
        header,
        schema: schema.clone(),
    };

    install_committed_vacuum_image(&conn, turso_core::MAIN_DB_ID, &committed);

    assert_eq!(conn.schema.read().schema_version, 42);
    assert!(Arc::ptr_eq(&*conn.schema.read(), &schema));
    let shared = db.clone_schema();
    assert_eq!(shared.schema_version, 42);
    assert!(Arc::ptr_eq(&shared, &schema));

    Ok(())
}

#[test]
fn cleanup_after_published_vacuum_installs_committed_image() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;

    conn.with_schema_mut(|schema| {
        schema.schema_version = 1;
    })?;
    db.with_schema_mut(|schema| {
        schema.schema_version = 1;
        Ok(())
    })?;

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 43.into();
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    schema.schema_version = 43;
    let schema = Arc::new(schema);
    let committed = VacuumCommittedImageMeta {
        header,
        schema: schema.clone(),
    };

    vacuum_in_place_cleanup(
        &conn,
        turso_core::MAIN_DB_ID,
        VacuumInPlacePhase::Checkpoint,
        None,
        Some(committed),
        VacuumInPlaceCleanupState::default(),
    )?;

    assert_eq!(conn.schema.read().schema_version, 43);
    assert!(Arc::ptr_eq(&*conn.schema.read(), &schema));
    let shared = db.clone_schema();
    assert_eq!(shared.schema_version, 43);
    assert!(Arc::ptr_eq(&shared, &schema));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn capture_target_metadata_uses_final_header_cookie_and_preserves_tvfs() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    // Source is uninitialized so its header reserved_space isn't usable.
    // Derive from the IOContext to keep reserved_space and the auto-
    // installed checksum context consistent under `feature = "checksum"`.
    let reserved_space = source_conn
        .get_pager()
        .io_ctx
        .read()
        .get_reserved_space_bytes();
    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

    temp.conn.execute("BEGIN IMMEDIATE")?;
    temp.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    temp.conn.execute("CREATE INDEX idx_t_v ON t(v)")?;
    temp.conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    let mvcc_version = RawVersion::from(turso_core::storage::sqlite3_ondisk::Version::Mvcc);
    let header_meta = VacuumDbHeaderMeta {
        read_version: mvcc_version,
        write_version: mvcc_version,
        schema_cookie: 42,
        default_page_cache_size: CacheSize::new(321),
        text_encoding: TextEncoding::Utf8,
        user_version: 17,
        application_id: 29,
    };
    match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
        turso_core::IOResult::Done(()) => {}
        turso_core::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
    }
    temp.conn.execute("COMMIT")?;

    let committed = capture_target_metadata(&temp, SequencesByNamespace::new())?;

    assert_eq!(committed.header.schema_cookie.get(), 42);
    assert_eq!(committed.header.read_version, mvcc_version);
    assert_eq!(committed.header.write_version, mvcc_version);
    assert_eq!(committed.schema.schema_version, 42);
    assert!(
        committed
            .schema
            .has_table(NamespaceId::ROOT, "generate_series"),
        "captured schema must retain built-in table-valued functions"
    );
    let table_root = match committed
        .schema
        .get_table(NamespaceId::ROOT, "t")
        .expect("table t")
        .as_ref()
    {
        turso_core::schema::Table::BTree(btree) => btree.root_page,
        _ => panic!("expected btree table"),
    };
    assert!(table_root > 0);
    let index_root = committed
        .schema
        .indices_including_backing(NamespaceId::ROOT, "t")
        .next()
        .map(|index| index.root_page)
        .expect("index idx_t_v");
    assert!(index_root > 0);

    Ok(())
}

#[test]
fn finalize_vacuum_into_output_syncs_destination_db_file() -> Result<()> {
    let io = Arc::new(SyncCountingIo::new("vacuum-into-output.db"));
    let io_dyn: Arc<dyn IO> = io.clone();
    let db = Database::open_file_with_flags(
        io_dyn,
        "vacuum-into-output.db",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    conn.set_sync_mode(turso_core::SyncMode::Off);
    conn.wal_auto_actions_disable();

    conn.execute("BEGIN IMMEDIATE")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    conn.execute("COMMIT")?;

    let before = io.counts();
    assert_eq!(
        before.db, 1,
        "creating the database does exactly one db fsync (page 1 must be durable \
         before the first WAL commit, even with synchronous=OFF); the target \
         build must not add more before finalization"
    );

    finalize_vacuum_into_output(&VacuumTargetBuildContext::new(conn))?;

    let after = io.counts();
    assert_eq!(
        after.db - before.db,
        1,
        "VACUUM INTO finalization must do exactly one destination DB fsync"
    );
    assert_eq!(
        after.wal - before.wal,
        2,
        "VACUUM INTO finalization must do one WAL fsync before backfill (the \
         checkpoint crash-atomicity barrier; finalization forces synchronous=FULL) \
         and one after truncation"
    );

    Ok(())
}

#[test]
fn vacuum_target_build_preserves_nested_statement_explicit_yield() -> Result<()> {
    let io = Arc::new(MemoryIO::new());
    let source_io: Arc<dyn IO> = io.clone();
    let source_db = Database::open_file_with_flags(
        source_io,
        "vacuum-yield-source.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    source_conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;
    source_conn.execute("CREATE INDEX idx_t_payload ON t(payload)")?;
    for id in 1..=8 {
        source_conn.execute(format!("INSERT INTO t VALUES({id}, 'payload-{id}')"))?;
    }
    source_conn.execute("BEGIN")?;

    let source_pager = source_conn.get_pager();
    let header_meta = source_pager
        .io
        .block(|| source_pager.with_header(VacuumDbHeaderMeta::from_source_header))?;

    let target_io: Arc<dyn IO> = io.clone();
    let target_db = Database::open_file_with_flags(
        target_io,
        "vacuum-yield-target.db",
        OpenFlags::Create,
        vacuum_target_opts_from_source(&source_db),
        None,
        Arc::new(SqliteDialect),
    )?;
    let target_conn = target_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    mirror_symbols(&source_conn, &target_conn);

    let config = VacuumTargetBuildConfig {
        source_conn: source_conn.clone(),
        escaped_schema_name: "main".to_string(),
        source_db_id: turso_core::MAIN_DB_ID,
        header_meta,
        source_custom_types: capture_custom_types(&source_conn, turso_core::MAIN_DB_ID),
        target_mvcc_enabled: source_db.mvcc_enabled(),
        target_auto_vacuum_mode: source_pager.get_auto_vacuum_mode(),
        copy_mvcc_metadata_table: false,
    };

    let injector = OneShotTransactionYieldInjector::new();
    source_conn.set_yield_injector(Some(injector.clone()));

    let mut context = VacuumTargetBuildContext::new(target_conn.clone());
    let mut saw_explicit_yield = false;
    loop {
        match vacuum_target_build_step(&config, &mut context)? {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(completions) => {
                saw_explicit_yield |= completions.is_explicit_yield();
                if !completions.is_explicit_yield() {
                    completions.wait(io.as_ref())?;
                }
            }
        }
    }

    source_conn.set_yield_injector(None);
    source_conn.execute("COMMIT")?;

    assert!(injector.yielded(), "test must trigger the injected yield");
    assert!(
        saw_explicit_yield,
        "vacuum target build must return the nested explicit yield instead of panicking"
    );

    let mut copied_rows = None;
    let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM t")?;
    stmt.run_with_row_callback(|row| {
        copied_rows = Some(row.get::<i64>(0)?);
        Ok(())
    })?;
    assert_eq!(copied_rows, Some(8));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_db_header_meta_updates_target_header() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

    let mut source_header = DatabaseHeader::default();
    source_header.schema_cookie = 41.into();
    source_header.default_page_cache_size = CacheSize::new(321);
    source_header.text_encoding = TextEncoding::Utf8;
    source_header.user_version = 17.into();
    source_header.application_id = 29.into();
    let header_meta = VacuumDbHeaderMeta::from_source_header(&source_header);

    temp.conn.execute("BEGIN")?;
    match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
        turso_core::IOResult::Done(()) => {}
        turso_core::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
    }
    temp.conn.execute("COMMIT")?;

    let pager = temp.conn.pager.load();
    let header = pager.io.block(|| {
        pager.with_header(|header| {
            (
                header.schema_cookie.get(),
                header.default_page_cache_size,
                header.text_encoding,
                header.user_version.get(),
                header.application_id.get(),
            )
        })
    })?;

    assert_eq!(
        header,
        (42, CacheSize::new(321), TextEncoding::Utf8, 17, 29)
    );

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_db_header_meta_schema_cookie_survives_mvcc_target_checkpoint() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let target_dir = tempfile::tempdir().unwrap();
    let target_path = target_dir.path().join("target.db");
    let target_path = target_path.to_str().unwrap();
    let target_db = Database::open_file_with_flags(
        io,
        target_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let target_conn = target_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    target_conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    target_conn.wal_auto_actions_disable();

    target_conn.execute("BEGIN IMMEDIATE")?;
    target_conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    target_conn.execute("INSERT INTO t VALUES (1, 'x')")?;

    let mut source_header = DatabaseHeader::default();
    source_header.schema_cookie = 41.into();
    source_header.user_version = 17.into();
    source_header.application_id = 29.into();
    let header_meta = VacuumDbHeaderMeta::from_source_header(&source_header);

    match finalize_vacuum_target_header(&target_conn, &header_meta)? {
        turso_core::IOResult::Done(()) => {}
        turso_core::IOResult::IO(_) => panic!("MVCC header update should not need async I/O"),
    }
    target_conn.execute("COMMIT")?;
    target_conn.checkpoint(turso_core::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })?;

    let pager = target_conn.pager.load();
    let on_disk = pager.io.block(|| {
        pager.with_header(|header| {
            (
                header.schema_cookie.get(),
                header.user_version.get(),
                header.application_id.get(),
            )
        })
    })?;
    assert_eq!(on_disk, (42, 17, 29));

    let schema_version = target_conn.pragma_query("schema_version")?;
    assert_eq!(schema_version, vec![vec![turso_core::Value::from_i64(42)]]);

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn internal_vacuum_temp_db_uses_source_runtime_and_disables_auto_checkpoint() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect(ConnectOpts::new(SessionRole::Superuser))?;

    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

    assert!(Arc::ptr_eq(&temp._db.io, &source_db.io));
    assert_ne!(temp.path, source_db.path);
    assert_eq!(
        temp.conn.wal_auto_actions(),
        turso_core::storage::wal::WalAutoActions::empty()
    );
    assert_eq!(temp.conn.get_page_size().get(), 4096);
    assert_eq!(temp.conn.get_reserved_bytes(), Some(0));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn internal_vacuum_temp_db_preserves_source_encryption() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("encrypted-source.db");
    let source_path = source_path.to_str().unwrap();
    let key_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let key = EncryptionKey::from_hex_string(key_hex)?;
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new().with_encryption(true),
        Some(EncryptionOpts {
            cipher: CipherMode::Aes256Gcm.to_string(),
            hexkey: key_hex.to_string(),
        }),
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;
    let reserved_space = source_conn
        .get_reserved_bytes()
        .expect("encrypted source should have reserved bytes");

    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

    assert!(temp._db.experimental_encryption_enabled());
    assert_eq!(
        temp.conn.get_encryption_cipher_mode(),
        source_conn.get_encryption_cipher_mode()
    );
    assert!(temp.conn.encryption_key.read().is_some());
    assert_eq!(temp.conn.get_reserved_bytes(), Some(reserved_space));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_target_build_cleanup_clears_target_connection_state() -> Result<()> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::io::PlatformIO::new()?);
    let target_dir = tempfile::tempdir().unwrap();
    let target_path = target_dir.path().join("target.db");
    let target_path = target_path.to_str().unwrap();
    let target_db = Database::open_file_with_flags(
        io,
        target_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let target_conn = target_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    target_conn.execute("CREATE TABLE seed(x)")?;

    let mut context = VacuumTargetBuildContext::new(target_conn.clone());
    let helper_stmt = target_conn.prepare_internal("SELECT 1")?;
    context.phase = VacuumTargetBuildPhase::CollectSchemaRows {
        schema_stmt: Box::new(helper_stmt),
    };

    target_conn.execute("BEGIN IMMEDIATE")?;
    target_conn.execute("INSERT INTO seed VALUES (1)")?;
    let pager = target_conn.pager.load();

    assert!(!target_conn.get_auto_commit());
    assert!(target_conn.is_nested_stmt());

    context.cleanup_after_error()?;

    assert!(target_conn.get_auto_commit());
    assert_eq!(
        target_conn.get_tx_state(),
        turso_core::connection::TransactionState::None
    );
    assert!(!target_conn.is_nested_stmt());
    assert!(!pager.holds_read_lock());
    assert!(!pager.holds_write_lock());

    let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM seed")?;
    let mut saw_row = false;
    stmt.run_with_row_callback(|_| {
        saw_row = true;
        Ok(())
    })?;
    assert!(
        saw_row,
        "target cleanup should leave the connection queryable"
    );

    Ok(())
}
