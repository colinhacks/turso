use turso_core::security::roles::SessionRole;
#[allow(unused_imports)]
use turso_core::util::IOExt;
use turso_sqlite::SqliteDialect;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use turso_core::database::{Database, DatabaseKey, InitState, DATABASE_MANAGER};

use turso_core::storage::encryption::EncryptionKey;

use turso_core::storage::page_transform::{
    PageCodec, PageCodecContext, PageCodecHeaderInfo, PageCodecId, PageLocation,
};

use turso_core::storage::sqlite3_ondisk::DatabaseHeader;

use turso_core::{
    storage::database::DatabaseFile, CompletionError, ConnectOpts, Connection, DatabaseOpts,
    DatabaseStorage, IOResult, LimboError, OpenDbAsyncState, OpenFlags, OpenOptions, PlatformIO,
    IO,
};

#[derive(Debug)]
struct IdentityPageCodec;

impl PageCodec for IdentityPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"identity-codec--")
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        output.copy_from_slice(input);
        Ok(())
    }
}

#[derive(Debug)]
struct XorPageCodec {
    mask: u8,
    reserved_bytes: u8,
}

impl XorPageCodec {
    fn transform(&self, page: &[u8], output: &mut [u8]) {
        for (input, output) in page.iter().zip(output) {
            *output = input ^ self.mask;
        }
    }
}

impl PageCodec for XorPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"xor-page-codec--";
        id[15] = self.mask;
        id[14] = self.reserved_bytes;
        PageCodecId::new(id)
    }

    fn bootstrap_page_info(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<PageCodecHeaderInfo> {
        if raw_page1_prefix.len() < 21 {
            return Err(LimboError::NotADB);
        }

        let decoded_magic = raw_page1_prefix[..16]
            .iter()
            .map(|byte| byte ^ self.mask)
            .collect::<Vec<_>>();
        if decoded_magic.as_slice() != b"SQLite format 3\0" {
            return Err(LimboError::NotADB);
        }

        let ps_raw = u16::from_be_bytes([
            raw_page1_prefix[16] ^ self.mask,
            raw_page1_prefix[17] ^ self.mask,
        ]);
        let page_size = if ps_raw == 1 { 65536 } else { ps_raw as usize };
        Ok(PageCodecHeaderInfo {
            page_size,
            reserved_space: raw_page1_prefix[20] ^ self.mask,
        })
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.reserved_bytes
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.transform(input, output);
        Ok(())
    }

    fn decode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.transform(input, output);
        Ok(())
    }
}

#[derive(Debug)]
struct CountingPageCodec {
    inner: XorPageCodec,
    database_page1_decodes: Arc<AtomicUsize>,
}

impl PageCodec for CountingPageCodec {
    fn codec_id(&self) -> PageCodecId {
        self.inner.codec_id()
    }

    fn bootstrap_page_info(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<PageCodecHeaderInfo> {
        self.inner.bootstrap_page_info(raw_page1_prefix)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.inner.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.inner.encode_page(context, input, output)
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        if context.page_no == DatabaseHeader::PAGE_ID as u32
            && context.location == PageLocation::Database
        {
            self.database_page1_decodes.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.decode_page(context, input, output)
    }
}

#[derive(Debug)]
struct FailOncePageCodec {
    inner: XorPageCodec,
    fail_decode: Arc<AtomicBool>,
    fail_context: PageCodecContext,
}

impl PageCodec for FailOncePageCodec {
    fn codec_id(&self) -> PageCodecId {
        self.inner.codec_id()
    }

    fn bootstrap_page_info(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<PageCodecHeaderInfo> {
        self.inner.bootstrap_page_info(raw_page1_prefix)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.inner.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        self.inner.encode_page(context, input, output)
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        if context == self.fail_context && self.fail_decode.swap(false, Ordering::Relaxed) {
            return Err(LimboError::InternalError(
                "injected page decode failure".into(),
            ));
        }
        self.inner.decode_page(context, input, output)
    }
}

#[derive(Debug)]
struct LocationPageCodec;

impl LocationPageCodec {
    const MASK: u8 = 0x6d;

    fn byte_key(page_no: u32, location: PageLocation, offset: usize) -> u8 {
        // Persisted transforms must not depend on the host pointer width.
        let page_id_byte = (page_no as u64).to_le_bytes()[offset % std::mem::size_of::<u64>()];
        let page_id_rotation = ((offset / std::mem::size_of::<u64>()) % (u8::BITS as usize)) as u32;
        Self::MASK
            ^ page_id_byte.rotate_left(page_id_rotation)
            ^ match location {
                PageLocation::Database => 0,
                PageLocation::Wal => 0xa5,
            }
    }

    fn transform(input: &[u8], output: &mut [u8], context: PageCodecContext) {
        for (offset, (input, output)) in input.iter().zip(output).enumerate() {
            *output = input ^ Self::byte_key(context.page_no, context.location, offset);
        }
    }
}

impl PageCodec for LocationPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"location-page-co")
    }

    fn bootstrap_page_info(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<PageCodecHeaderInfo> {
        if raw_page1_prefix.len() < 21 {
            return Err(LimboError::NotADB);
        }
        if !raw_page1_prefix[..16]
            .iter()
            .enumerate()
            .zip(b"SQLite format 3\0")
            .all(|((offset, encoded), expected)| {
                *encoded
                    ^ Self::byte_key(
                        DatabaseHeader::PAGE_ID as u32,
                        PageLocation::Database,
                        offset,
                    )
                    == *expected
            })
        {
            return Err(LimboError::NotADB);
        }

        let page_size = u16::from_be_bytes([
            raw_page1_prefix[16]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 16),
            raw_page1_prefix[17]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 17),
        ]);
        Ok(PageCodecHeaderInfo {
            page_size: if page_size == 1 {
                65_536
            } else {
                page_size as usize
            },
            reserved_space: raw_page1_prefix[20]
                ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 20),
        })
    }

    fn required_reserved_bytes(&self) -> u8 {
        1
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        Self::transform(input, output, context);
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> turso_core::Result<()> {
        Self::transform(input, output, context);
        Ok(())
    }
}

#[cfg(feature = "fs")]
fn open_with_page_codec_result(
    io: Arc<dyn IO>,
    path: &str,
    codec: Arc<dyn PageCodec>,
) -> turso_core::Result<Arc<Database>> {
    open_with_page_codec_with_opts_result(io, path, codec, DatabaseOpts::new())
}

#[cfg(feature = "fs")]
fn open_with_page_codec_with_opts_result(
    io: Arc<dyn IO>,
    path: &str,
    codec: Arc<dyn PageCodec>,
    opts: DatabaseOpts,
) -> turso_core::Result<Arc<Database>> {
    let file = io.open_file(path, OpenFlags::Create, true).unwrap();
    let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(file));
    let mut state = OpenDbAsyncState::new();
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(db_file)
        .flags(OpenFlags::Create)
        .db_opts(opts)
        .page_codec(codec);

    loop {
        match Database::open_async(&mut state, io.clone(), path, &options)? {
            IOResult::Done(db) => return Ok(db),
            IOResult::IO(completion) => completion.wait(&*io)?,
        }
    }
}

#[cfg(feature = "fs")]
fn open_with_page_codec(io: Arc<dyn IO>, path: &str, codec: Arc<dyn PageCodec>) -> Arc<Database> {
    open_with_page_codec_result(io, path, codec).unwrap()
}

#[cfg(feature = "fs")]
fn count_test_rows(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

#[cfg(feature = "fs")]
fn passive_checkpoint_busy(conn: &Arc<Connection>) -> turso_core::Result<i64> {
    let mut stmt = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
    let mut busy = None;
    stmt.run_with_row_callback(|row| {
        busy = Some(row.get(0)?);
        Ok(())
    })?;
    busy.ok_or_else(|| {
        LimboError::InternalError("wal_checkpoint did not return a result row".to_owned())
    })
}

#[cfg(feature = "fs")]
#[test]
fn registry_entry_is_removed_when_last_database_reference_drops() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("registry-drop.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let file_key = DatabaseKey::File(turso_core::io::get_file_id(path).unwrap());
    let reopened = Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    assert!(Arc::ptr_eq(&db, &reopened));
    drop(reopened);
    assert!(DATABASE_MANAGER.lock().contains_key(&file_key));
    drop(db);
    assert!(!DATABASE_MANAGER.lock().contains_key(&file_key));

    let name = "registry-entry-is-removed-when-last-database-reference-drops";
    let memory_key = DatabaseKey::SharedMemory(name.to_string());
    let db = Database::open_shared_memory(name, Arc::new(SqliteDialect)).unwrap();
    assert!(DATABASE_MANAGER.lock().contains_key(&memory_key));
    drop(db);
    assert!(!DATABASE_MANAGER.lock().contains_key(&memory_key));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_checkpoint_decodes_database_page1_for_identity() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-checkpoint-identity.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let database_page1_decodes = Arc::new(AtomicUsize::new(0));
    let codec: Arc<dyn PageCodec> = Arc::new(CountingPageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        database_page1_decodes: database_page1_decodes.clone(),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
         insert into test(value) values ('alpha');",
    )
    .unwrap();
    conn.set_sync_mode(turso_core::SyncMode::Full);

    database_page1_decodes.store(0, Ordering::Relaxed);
    let checkpoint = conn
        .checkpoint(turso_core::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();

    assert!(checkpoint.wal_checkpoint_backfilled > 0);
    assert_eq!(
        database_page1_decodes.load(Ordering::Relaxed),
        1,
        "checkpoint identity must decode database page 1 exactly once"
    );
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_checkpoint_recovers_after_page1_decode_failure() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-checkpoint-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let fail_database_page1_decode = Arc::new(AtomicBool::new(false));
    let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        fail_decode: fail_database_page1_decode.clone(),
        fail_context: PageCodecContext::new(DatabaseHeader::PAGE_ID as u32, PageLocation::Database),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute(
        "create table test(id integer primary key, value text);
         insert into test(value) values ('alpha');",
    )
    .unwrap();
    conn.set_sync_mode(turso_core::SyncMode::Full);

    fail_database_page1_decode.store(true, Ordering::Relaxed);
    let err = conn
        .checkpoint(turso_core::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 1 })
    ));

    let checkpoint = conn
        .checkpoint(turso_core::storage::wal::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(checkpoint.wal_checkpoint_backfilled > 0);
    assert_eq!(count_test_rows(&conn), 1);
}

#[cfg(feature = "fs")]
#[test]
fn pragma_checkpoint_codec_error_releases_checkpoint_lock() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-pragma-checkpoint-retry.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let fail_wal_page2_decode = Arc::new(AtomicBool::new(false));
    let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
        inner: XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        },
        fail_decode: fail_wal_page2_decode.clone(),
        fail_context: PageCodecContext::new(2, PageLocation::Wal),
    });
    let db = open_with_page_codec(io, path, codec.clone());
    let writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
        .unwrap();
    writer.wal_auto_actions_disable();
    writer.execute("PRAGMA journal_mode = 'wal'").unwrap();
    writer
        .execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha');",
        )
        .unwrap();

    let pager = writer.get_pager();
    assert!(
        pager.wal_pos().1 > 0,
        "test setup must leave committed WAL frames"
    );
    assert_eq!(
        pager.wal_backfill_frame(),
        Some(0),
        "test setup must not checkpoint the WAL before injecting the codec error"
    );
    assert!(
        pager.wal_changed_pages_after(0).unwrap().contains(&2),
        "test setup must leave page 2 in the WAL"
    );

    let first = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
        .unwrap();
    assert!(first.get_pager().has_wal());
    fail_wal_page2_decode.store(true, Ordering::Relaxed);
    let first_error = passive_checkpoint_busy(&first).unwrap_err();
    assert!(matches!(first_error, LimboError::CheckpointFailed(_)));
    assert!(!fail_wal_page2_decode.load(Ordering::Relaxed));

    let second = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    assert!(second.get_pager().has_wal());
    let other_connection_retry = passive_checkpoint_busy(&second);
    let same_connection_retry = passive_checkpoint_busy(&first);
    assert!(
        matches!(same_connection_retry, Ok(0)) && matches!(other_connection_retry, Ok(0)),
        "checkpoint failure must not leave stale state or retain its guard: same connection \
         returned {same_connection_retry:?}, other connection returned {other_connection_retry:?}"
    );
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_round_trips_wal_and_checkpointed_database_with_bootstrap_header() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-roundtrip.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0x5a,
        reserved_bytes: 1,
    });

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha'), ('bravo');",
        )
        .unwrap();
        assert_eq!(count_test_rows(&conn), 2);
        conn.set_sync_mode(turso_core::SyncMode::Full);
        let passive = conn
            .checkpoint(turso_core::storage::wal::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();
        assert!(
            passive.wal_checkpoint_backfilled > 0,
            "PASSIVE checkpoint must backfill codec-transformed pages"
        );
        conn.execute("insert into test(value) values ('charlie')")
            .unwrap();
        let full = conn
            .checkpoint(turso_core::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(
            full.wal_checkpoint_backfilled > 0,
            "FULL checkpoint must backfill codec-transformed pages"
        );
        assert_eq!(count_test_rows(&conn), 3);
    }

    let raw_database = std::fs::read(path).unwrap();
    assert_ne!(&raw_database[..16], b"SQLite format 3\0");

    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    assert_eq!(count_test_rows(&conn), 3);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_reopens_live_wal_then_checkpointed_database() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-live-wal-recovery.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha'), ('bravo'), ('charlie');",
        )
        .unwrap();
        assert_eq!(count_test_rows(&conn), 3);
    }
    let wal_path = format!("{path}-wal");
    assert!(
        std::fs::metadata(&wal_path).unwrap().len() > 0,
        "the first reopen must recover committed codec-transformed WAL frames"
    );

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
            .unwrap();
        assert_eq!(count_test_rows(&conn), 3);
        let checkpoint = conn
            .checkpoint(turso_core::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(
            checkpoint.wal_checkpoint_backfilled > 0,
            "the recovery checkpoint must backfill codec-transformed WAL frames"
        );
    }
    let db = open_with_page_codec(io, path, codec.clone());
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    assert_eq!(count_test_rows(&conn), 3);
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_and_encryption_cannot_share_pager() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-encryption-conflict.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
    let db = open_with_page_codec(io, path, codec.clone());
    let encryption_key = EncryptionKey::from_hex_string(&"00".repeat(32)).unwrap();
    let mut state = InitState::default();

    let err = match db._init_nonblock(&mut state, Some(&encryption_key), Some(&codec)) {
        Ok(_) => panic!("encryption and an external page codec must not share a pager"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("built-in encryption cannot be combined with an external page codec"));
}

#[cfg(feature = "fs")]
#[test]
fn page_codec_rejects_database_already_in_mvcc_mode() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("codec-existing-mvcc.db");
    let path_str = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path_str,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }

    let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
        mask: 0,
        reserved_bytes: if cfg!(feature = "checksum") {
            turso_core::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            0
        },
    });
    let err = open_with_page_codec_result(io.clone(), path_str, codec).unwrap_err();
    assert!(
        err.to_string()
            .contains("external page codecs are not supported with MVCC databases"),
        "unexpected error: {err}"
    );

    let db = Database::open_file_with_flags(
        io,
        path_str,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
}

#[cfg(feature = "fs")]
#[test]
fn location_page_codec_round_trips_wal_checkpoint_and_overflow_pages() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("location-codec.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

    {
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec.clone()))
            .unwrap();
        conn.execute(
            "create table test(id integer primary key, value blob);
             insert into test(value) values (zeroblob(12000));
             delete from test where id = 1;
             insert into test(value)
             values (cast(replace(printf('%0*d', 16000, 0), '0', 'x') as blob));",
        )
        .unwrap();
        let checkpoint = conn
            .checkpoint(turso_core::storage::wal::CheckpointMode::Full)
            .unwrap();
        assert!(checkpoint.wal_checkpoint_backfilled > 0);
    }

    let raw_database = std::fs::read(path).unwrap();
    assert_ne!(&raw_database[..16], b"SQLite format 3\0");
    let reopened = open_with_page_codec(io, path, codec.clone());
    let conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser).with_page_codec(codec))
        .unwrap();
    let mut values: Vec<(i64, String)> = Vec::new();
    conn.prepare("select length(value), hex(substr(value, 1, 8)) from test order by id")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push((row.get(0).unwrap(), row.get(1).unwrap()));
            Ok(())
        })
        .unwrap();
    assert_eq!(values, vec![(16_000, "7878787878787878".to_string())]);
}
