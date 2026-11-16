use std::sync::Arc;
#[allow(unused_imports)]
use turso_core::alloc::TursoAllocExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoIteratorExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoSliceExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoTryWithCapacityExt;
#[allow(unused_imports)]
use turso_core::alloc::TursoVecExt;
use turso_core::error::LimboError;
use turso_core::numeric::Numeric;
#[allow(unused_imports)]
use turso_core::storage::btree::CursorTrait;
use turso_core::types::Cursor;
use turso_core::types::IOResult;
use turso_core::types::Value;
#[allow(unused_imports)]
use turso_core::vdbe::builder::CursorTypeExt;
use turso_core::vdbe::execute::cleanup_op_vacuum_into;
use turso_core::vdbe::execute::*;
use turso_core::vdbe::hash_table::HashTable;
use turso_core::vdbe::hash_table::HashTableConfig;
use turso_core::vdbe::insn::Insn;
use turso_core::vdbe::Program;
use turso_core::vdbe::ProgramState;
use turso_core::vdbe::Register;
use turso_core::vdbe::TxnCleanup;
#[allow(unused_imports)]
use turso_core::vdbe::ValueIteratorExt;
#[allow(unused_imports)]
use turso_core::IOExt;
use turso_core::OpenFlags;
use turso_core::Pager;
use turso_core::QueryMode;
use turso_sqlite::SqliteDialect;

use turso_core::alloc::vec;

use turso_core::security::roles::SessionRole;

use turso_core::translate::collate::CollationSeq;

use turso_core::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};

use turso_core::vdbe::BranchOffset;

use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, IO};

#[test]
fn set_subtype_non_integer_returns_error() {
    assert_set_subtype_error(Value::build_text("74"), "stored subtype must be an integer");
}

#[test]
fn set_subtype_unknown_integer_returns_error() {
    assert_set_subtype_error(Value::from_i64(75), "unknown stored subtype: 75");
}

fn assert_set_subtype_error(subtype: Value, expected_message: &str) {
    let (program, pager) = empty_program();
    let mut state = ProgramState::new(2, 0);
    let original = Value::build_text("unchanged");
    state.set_register(0, Register::Value(subtype));
    state.set_register(1, Register::Value(original.clone()));
    let insn = Insn::SetSubtype {
        subtype_reg: 0,
        dest_reg: 1,
    };
    let err = match op_set_subtype(&program, &mut state, &insn, &pager) {
        Ok(_) => panic!("invalid subtype must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message == expected_message),
        "unexpected error: {err:?}"
    );
    assert_eq!(state.pc, 0);
    assert_eq!(state.get_register(1).get_value(), &original);
}

fn empty_program() -> (Program, Arc<Pager>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let pager = conn.get_pager();
    let builder = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 0, 0));
    let program = builder.build(conn, false, "").unwrap();
    (program, pager)
}

fn make_spilled_hash_table() -> (HashTable, turso_core::alloc::Vec<Value>, usize) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 4,
        mem_budget: 1024,
        num_keys: 1,
        collations: vec![CollationSeq::BINARY],
        temp_store: turso_core::TempStore::Default,
        track_matched: false,
        ..Default::default()
    };
    let mut ht = HashTable::new(config, io).unwrap();

    for i in 0..1024 {
        match ht
            .insert(vec![Value::from_i64(i)], i, vec![], None)
            .unwrap()
        {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO should complete synchronously"),
        }
    }

    match ht.finalize_build(None).unwrap() {
        IOResult::Done(()) => {}
        IOResult::IO(_) => panic!("memory IO should complete synchronously"),
    }
    assert!(ht.has_spilled(), "test requires spilled hash table");

    let probe_key = (0..1024)
        .map(|i| vec![Value::from_i64(i)])
        .find(|key| {
            let partition_idx = ht.partition_for_keys(key).unwrap();
            !ht.is_partition_loaded(partition_idx)
        })
        .expect("expected an unloaded spilled partition");
    let partition_idx = ht.partition_for_keys(&probe_key).unwrap();

    (ht, probe_key, partition_idx)
}

/// same like `test_vacuum_into_busy_after_source_begin_rolls_back_source_txn`
/// but for attached dbs
#[test]
fn test_cleanup_vacuum_into_rolls_back_attached_only_source_txn() {
    let dir = tempfile::TempDir::new().unwrap();
    let main_path = dir.path().join("vacuum-into-cleanup-main.db");
    let attached_path = dir.path().join("vacuum-into-cleanup-attached.db");

    let io: Arc<dyn IO> = Arc::new(turso_core::io::PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        main_path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute(format!(
        "ATTACH DATABASE '{}' AS att",
        attached_path.display()
    ))
    .unwrap();
    conn.execute("CREATE TABLE att.t(x)").unwrap();
    conn.execute("INSERT INTO att.t VALUES (1)").unwrap();

    conn.execute("BEGIN").unwrap();
    let attached_db_id = conn.get_database_id_by_name("att").unwrap();
    let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
    attached_pager.begin_read_tx().unwrap();

    assert!(
        !conn.pager.load().holds_read_lock(),
        "attached-only cleanup regression requires the main pager to stay lock-free"
    );
    assert!(
        attached_pager.holds_read_lock(),
        "attached source pager should hold the pinned read snapshot"
    );

    let mut state = ProgramState::new(0, 0);
    state.auto_txn_cleanup = TxnCleanup::RollbackTxn;

    cleanup_op_vacuum_into(&conn, &mut state, Box::default()).unwrap();

    assert!(
        conn.get_auto_commit(),
        "cleanup should restore auto-commit without going through SQL ROLLBACK"
    );
    assert_eq!(state.auto_txn_cleanup, TxnCleanup::None);
    assert!(
        !attached_pager.holds_read_lock(),
        "cleanup should release the attached source read snapshot"
    );

    conn.execute("INSERT INTO att.t VALUES (2)").unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM att.t").unwrap();
    let mut count = 0_i64;
    stmt.run_with_row_callback(|row| {
        count = row.get(0)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_savepoint_loads_evicted_attached_header_before_mirroring() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "savepoint-main.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("ATTACH 'savepoint-aux.db' AS aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x)").unwrap();

    let attached_db_id = conn.get_database_id_by_name("aux").unwrap();
    let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
    attached_pager.begin_read_tx().unwrap();
    attached_pager.clear_page_cache(false);

    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("RELEASE s").unwrap();
}

#[test]
fn test_hash_probe_rejects_unloaded_spilled_partition_without_probe_rowid() {
    let (program, pager) = empty_program();
    let (ht, probe_key, _) = make_spilled_hash_table();

    let mut state = ProgramState::new(2, 0);
    state.hash_tables.insert(7, ht);
    state.set_register(0, Register::Value(probe_key[0].clone()));

    let insn = Insn::HashProbe {
        hash_table_id: 7,
        key_start_reg: 0,
        num_keys: 1,
        dest_reg: 1,
        target_pc: BranchOffset::Offset(99),
        deferred_target_pc: BranchOffset::Offset(98),
        payload_dest_reg: None,
        num_payload: 0,
        probe_rowid_reg: None,
    };

    let err = match op_hash_probe(&program, &mut state, &insn, &pager) {
        Ok(_) => {
            panic!("HashProbe should reject grace-only probing without a loaded partition")
        }
        Err(err) => err,
    };

    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("probe_rowid_reg=None is grace-only")),
        "unexpected error: {err:?}"
    );
    assert_eq!(state.pc, 0, "pc should not advance on invariant violation");
    assert!(
        state.active_op_state.hash_probe().is_none(),
        "HashProbe should not stash resumable state for the removed fallback path"
    );
}

#[test]
fn test_hash_probe_allows_grace_style_probe_after_partition_preload() {
    let (program, pager) = empty_program();
    let (mut ht, probe_key, partition_idx) = make_spilled_hash_table();

    loop {
        match ht.load_spilled_partition(partition_idx, None).unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(_) => continue,
        }
    }
    assert!(
        ht.is_partition_loaded(partition_idx),
        "grace-style probe requires the build partition to be resident"
    );

    let expected_rowid = match &probe_key[0] {
        Value::Numeric(Numeric::Integer(i)) => *i,
        ref other => panic!("expected integer probe key, got {other:?}"),
    };

    let mut state = ProgramState::new(2, 0);
    state.hash_tables.insert(7, ht);
    state.set_register(0, Register::Value(probe_key[0].clone()));

    let insn = Insn::HashProbe {
        hash_table_id: 7,
        key_start_reg: 0,
        num_keys: 1,
        dest_reg: 1,
        target_pc: BranchOffset::Offset(99),
        deferred_target_pc: BranchOffset::Offset(98),
        payload_dest_reg: None,
        num_payload: 0,
        probe_rowid_reg: None,
    };

    let step = op_hash_probe(&program, &mut state, &insn, &pager)
        .expect("preloaded grace probe should succeed");
    assert!(matches!(step, InsnFunctionStepResult::Step));
    assert_eq!(state.pc, 1, "matching probe should fall through");
    assert_eq!(
        state.get_register(1).get_value(),
        &Value::from_i64(expected_rowid),
        "HashProbe should return the matching build rowid"
    );
}

#[test]
fn test_decr_jump_zero_non_integer_register_returns_error() {
    let (program, pager) = empty_program();

    let mut state = ProgramState::new(1, 0);
    state.set_register(0, Register::Value(Value::Text("not-an-int".into())));

    let insn = Insn::DecrJumpZero {
        reg: 0,
        target_pc: turso_core::vdbe::BranchOffset::Offset(1),
    };

    let err = match op_decr_jump_zero(&program, &mut state, &insn, &pager) {
        Ok(_) => panic!("non-integer register must fail"),
        Err(err) => err,
    };
    assert!(matches!(*err, LimboError::Constraint(message) if message == "datatype mismatch"));
    assert_eq!(state.pc, 0);
}

#[test]
fn test_make_record_overlapping_dest_returns_error() {
    let (program, pager) = empty_program();

    let mut state = ProgramState::new(3, 0);
    for i in 0..3 {
        state.set_register(i, Register::Value(Value::from_i64(i as i64)));
    }
    let insn = Insn::MakeRecord {
        start_reg: 0,
        count: 3,
        dest_reg: 1,
        index_name: None,
        affinity_str: None,
    };

    let err = match op_make_record(&program, &mut state, &insn, &pager) {
        Ok(_) => panic!("overlapping destination register must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("overlaps its source range")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_sorter_data_unpaired_content_register_returns_error() {
    let (program, pager) = empty_program();

    // Pseudo cursor 1 exposes register 2, but SorterData targets register 3.
    let mut state = ProgramState::new(4, 2);
    state.cursors[1] = Some(Cursor::new_pseudo(turso_core::pseudo::PseudoCursor::new(2)));
    let insn = Insn::SorterData {
        cursor_id: 0,
        dest_reg: 3,
        pseudo_cursor: 1,
    };

    let err = match op_sorter_data(&program, &mut state, &insn, &pager) {
        Ok(_) => panic!("unpaired content register must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(*err, LimboError::InternalError(ref message) if message.contains("content register")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn if_null_row_does_nothing_for_unopened_cursor() {
    let (program, pager) = empty_program();
    let mut state = ProgramState::new(1, 1);
    state.set_register(0, Register::Value(Value::from_i64(7)));
    let insn = Insn::IfNullRow {
        cursor_id: 0,
        target_pc: BranchOffset::Offset(9),
        dest: 0,
    };

    let step = op_if_null_row(&program, &mut state, &insn, &pager)
        .expect("IfNullRow must accept an unopened cursor");

    assert!(matches!(step, InsnFunctionStepResult::Step));
    assert_eq!(state.pc, 1);
    assert_eq!(state.get_register(0).get_value(), &Value::from_i64(7));
}

#[test]
fn if_null_row_clears_destination_and_jumps() {
    let (program, pager) = empty_program();
    let mut state = ProgramState::new(1, 1);
    state.cursors[0] = Some(Cursor::NullRow);
    state.set_register(0, Register::Value(Value::from_i64(7)));
    let insn = Insn::IfNullRow {
        cursor_id: 0,
        target_pc: BranchOffset::Offset(9),
        dest: 0,
    };

    let step = op_if_null_row(&program, &mut state, &insn, &pager)
        .expect("IfNullRow must accept a null-row cursor");

    assert!(matches!(step, InsnFunctionStepResult::Step));
    assert_eq!(state.pc, 9);
    assert_eq!(state.get_register(0).get_value(), &Value::Null);
}
