use std::sync::Arc;
use std::task::Waker;
#[allow(unused_imports)]
use turso_core::alloc::TursoIteratorExt;
use turso_core::statement::StepResult;
use turso_core::vdbe;
use turso_core::LimboError;
use turso_core::Value;
use turso_sqlite::SqliteDialect;

use turso_core::security::roles::SessionRole;

use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, IO};

#[test]
fn cloned_halt_retains_constraint_context() {
    assert_cloned_constraint_context(
        "INSERT INTO parent VALUES (1, 'b', 1)",
        turso_core::ConstraintKind::PrimaryKey,
        "parent",
        "id",
    );
}

#[test]
fn cloned_halt_if_null_retains_constraint_context() {
    assert_cloned_constraint_context(
        "INSERT INTO parent VALUES (2, NULL, 1)",
        turso_core::ConstraintKind::NotNull,
        "parent",
        "code",
    );
}

#[test]
fn cloned_fk_counter_retains_constraint_context() {
    assert_cloned_constraint_context(
        "UPDATE child SET pid = 99",
        turso_core::ConstraintKind::ForeignKey,
        "child",
        "pid",
    );
}

fn assert_cloned_constraint_context(
    sql: &str,
    kind: turso_core::ConstraintKind,
    table: &str,
    column: &str,
) {
    use turso_core::vdbe::{execute, insn::Insn};

    let conn = constraint_test_connection();
    let mut stmt = conn.prepare(sql).unwrap();
    let insn = stmt
        .program
        .insns
        .iter()
        .find_map(|(insn, _)| {
            let matches = match kind {
                turso_core::ConstraintKind::PrimaryKey => matches!(
                    insn,
                    Insn::Halt {
                        err_code: turso_core::error::SQLITE_CONSTRAINT_PRIMARYKEY,
                        ..
                    }
                ),
                turso_core::ConstraintKind::NotNull => matches!(insn, Insn::HaltIfNull { .. }),
                turso_core::ConstraintKind::ForeignKey => matches!(
                    insn,
                    Insn::FkCounter {
                        increment_value: 1,
                        deferred: false,
                        ..
                    }
                ),
                _ => unreachable!(),
            };
            matches.then(|| insn.clone())
        })
        .expect("constraint instruction");
    for index in 0..stmt.state.column_count() {
        stmt.state
            .set_register(index, vdbe::Register::Value(Value::from_i64(99)));
    }
    let error = match &insn {
        Insn::Halt { .. } => execute::op_halt(&stmt.program, &mut stmt.state, &insn, &stmt.pager)
            .err()
            .expect("constraint error"),
        Insn::HaltIfNull { target_reg, .. } => {
            stmt.state
                .set_register(*target_reg, vdbe::Register::Value(Value::Null));
            execute::op_halt_if_null(&stmt.program, &mut stmt.state, &insn, &stmt.pager)
                .err()
                .expect("constraint error")
        }
        Insn::FkCounter { .. } => {
            execute::op_fk_counter(&stmt.program, &mut stmt.state, &insn, &stmt.pager).unwrap();
            Box::new(stmt.state.immediate_foreign_key_error())
        }
        _ => unreachable!(),
    };
    let LimboError::ConstraintViolation(violation) = *error else {
        panic!("expected structured constraint: {error:?}");
    };
    assert_eq!(violation.kind, kind);
    assert_eq!(violation.table_name.as_deref(), Some(table));
    assert_eq!(violation.column_names, vec![column]);
    assert!(!violation.values.is_empty());
}

#[test]
fn internal_constraint_errors_are_structured() {
    for (sql, kind, message, foreign_key, extended_code) in constraint_error_cases() {
        let conn = constraint_test_connection();
        let mut stmt = conn.prepare(sql).unwrap();
        let error = loop {
            match stmt.step_with_structured_errors() {
                Err(error) => break error,
                Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                    stmt.pager.io.step().unwrap();
                }
                Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
            }
        };
        let LimboError::ConstraintViolation(violation) = &error else {
            panic!("{sql}: {error:?}");
        };
        assert_eq!(violation.kind, kind, "{sql}");
        assert_eq!(error.to_string(), message, "{sql}");
        assert_eq!(error.is_foreign_key_constraint(), foreign_key, "{sql}");
        assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        assert_eq!(error.sqlite_extended_result_code(), extended_code, "{sql}");
    }
}

#[test]
fn public_constraint_errors_preserve_sqlite_variants() {
    for (sql, _, message, foreign_key, _) in constraint_error_cases() {
        for with_waker in [false, true] {
            let conn = constraint_test_connection();
            let mut stmt = conn.prepare(sql).unwrap();
            let error = loop {
                let result = if with_waker {
                    stmt.step_with_waker(Waker::noop())
                } else {
                    stmt.step()
                };
                match result {
                    Err(error) => break error,
                    Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                        stmt.pager.io.step().unwrap();
                    }
                    Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
                }
            };
            if foreign_key {
                assert!(
                    matches!(error, LimboError::ForeignKeyConstraint(_)),
                    "{sql}: {error:?}"
                );
            } else {
                assert!(
                    matches!(error, LimboError::Constraint(_)),
                    "{sql}: {error:?}"
                );
            }
            assert_eq!(error.to_string(), message, "{sql}");
            assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        }
    }
}

#[test]
fn public_reset_preserves_sqlite_foreign_key_error() {
    let conn = constraint_test_connection();
    let mut stmt = conn
        .prepare("INSERT INTO deferred_child VALUES (99) RETURNING pid")
        .unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                stmt.pager.io.step().unwrap();
            }
            result => panic!("expected RETURNING row, got {result:?}"),
        }
    }
    let error = stmt.reset().unwrap_err();
    assert!(
        matches!(error, LimboError::ForeignKeyConstraint(_)),
        "{error:?}"
    );
    assert_eq!(error.to_string(), "FOREIGN KEY constraint failed");
    assert_eq!(error.sqlite_result_code(), 19);
}

fn constraint_error_cases() -> [(
    &'static str,
    turso_core::ConstraintKind,
    &'static str,
    bool,
    i32,
); 13] {
    [
        (
            "INSERT INTO parent VALUES (1, 'b', 1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: parent.id",
            false,
            1555,
        ),
        (
            "INSERT INTO parent VALUES (2, 'a', 1)",
            turso_core::ConstraintKind::Unique,
            "UNIQUE constraint failed: parent.code",
            false,
            2067,
        ),
        (
            "INSERT INTO parent VALUES (2, NULL, 1)",
            turso_core::ConstraintKind::NotNull,
            "NOT NULL constraint failed: parent.code",
            false,
            1299,
        ),
        (
            "INSERT INTO parent VALUES (2, 'b', -1)",
            turso_core::ConstraintKind::Check,
            "CHECK constraint failed: positive",
            false,
            275,
        ),
        (
            "INSERT INTO child VALUES (99)",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "DELETE FROM parent",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "INSERT INTO deferred_child VALUES (99)",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "INSERT INTO trigger_source VALUES (1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: parent.id",
            false,
            1555,
        ),
        (
            "INSERT INTO text_key VALUES ('k')",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: text_key.k",
            false,
            1555,
        ),
        (
            "INSERT INTO pair_key VALUES (1, 1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "UPDATE pair_key SET b = 1 WHERE b = 2",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "INSERT INTO pair_key VALUES (1, 2) ON CONFLICT DO UPDATE SET b = 1",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "INSERT INTO strict_int VALUES ('x')",
            turso_core::ConstraintKind::Datatype,
            "cannot store TEXT value in INTEGER column strict_int.a",
            false,
            3091,
        ),
    ]
}

/// RAISE and a constraint's own ON CONFLICT clause halt with their own
/// resolve type, and the error still reports the halt's result code.
#[test]
fn errors_with_their_own_resolve_type_keep_the_extended_result_code() {
    for (sql, message, extended_code) in [
        ("INSERT INTO raise_abort VALUES (1)", "nope", 1811),
        (
            "INSERT INTO unique_rollback VALUES (1)",
            "UNIQUE constraint failed: unique_rollback.a",
            2067,
        ),
    ] {
        let conn = constraint_test_connection();
        let mut stmt = conn.prepare(sql).unwrap();
        let error = loop {
            match stmt.step_with_structured_errors() {
                Err(error) => break error,
                Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                    stmt.pager.io.step().unwrap();
                }
                Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
            }
        };
        assert!(matches!(error, LimboError::Raise(..)), "{sql}: {error:?}");
        assert_eq!(error.to_string(), message, "{sql}");
        assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        assert_eq!(error.sqlite_extended_result_code(), extended_code, "{sql}");
    }
}

fn constraint_test_connection() -> Arc<turso_core::Connection> {
    let conn = open_test_connection().unwrap();
    conn.set_foreign_keys_enabled(true);
    for sql in [
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, score INTEGER CONSTRAINT positive CHECK (score > 0))",
        "CREATE TABLE child (pid INTEGER REFERENCES parent(id))",
        "CREATE TABLE deferred_child (pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE trigger_source (id INTEGER)",
        "CREATE TRIGGER duplicate_parent AFTER INSERT ON trigger_source BEGIN INSERT INTO parent VALUES (NEW.id, 'b', 1); END",
        "INSERT INTO parent VALUES (1, 'a', 1)",
        "INSERT INTO child VALUES (1)",
        "CREATE TABLE text_key (k TEXT PRIMARY KEY)",
        "INSERT INTO text_key VALUES ('k')",
        "CREATE TABLE pair_key (a, b, PRIMARY KEY (a, b))",
        "INSERT INTO pair_key VALUES (1, 1), (1, 2)",
        "CREATE TABLE strict_int (a INTEGER) STRICT",
        "CREATE TABLE raise_abort (a)",
        "CREATE TRIGGER raise_abort_trigger BEFORE INSERT ON raise_abort BEGIN SELECT RAISE(ABORT, 'nope'); END",
        "CREATE TABLE unique_rollback (a UNIQUE ON CONFLICT ROLLBACK)",
        "INSERT INTO unique_rollback VALUES (1)",
    ] {
        conn.execute(sql).unwrap();
    }
    conn
}

fn open_test_connection() -> turso_core::Result<Arc<turso_core::Connection>> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    db.connect(ConnectOpts::new(SessionRole::Superuser))
}

/// A hash join inside a correlated subquery is built once and probed once per
/// outer row. The budget is forced down so the build spills in every build
/// profile; the second and third passes then go through the grace phase and
/// must count the same rows as the first (#9774).
#[test]
fn test_spilled_hash_join_counts_the_same_on_every_probe_pass() {
    let conn = open_test_connection().unwrap();
    for sql in [
        "CREATE TABLE a(x INTEGER PRIMARY KEY, y INT)",
        "CREATE TABLE c(k INT, v INT)",
        "CREATE TABLE d(k INT, w INT)",
        "INSERT INTO a VALUES (1, 0), (2, 1000000), (3, 1000000)",
        "INSERT INTO c SELECT value % 50, value FROM generate_series(1, 1000)",
        "INSERT INTO d SELECT value % 70, value FROM generate_series(1, 1000)",
    ] {
        conn.execute(sql).unwrap();
    }
    let mut stmt = conn
        .prepare("SELECT a.x, (SELECT count(*) FROM c JOIN d ON c.k = d.k AND d.w < a.y) FROM a")
        .unwrap();
    let mut hash_builds = 0;
    for (insn, _) in &mut Arc::make_mut(&mut stmt.program.prepared).insns {
        if let vdbe::insn::Insn::HashBuild { data } = insn {
            data.mem_budget = 32 * 1024;
            hash_builds += 1;
        }
    }
    assert_eq!(hash_builds, 1, "the join must be built once and reused");

    let rows = stmt.run_collect_rows().unwrap();
    let expected: Vec<Vec<Value>> = [[1, 0], [2, 14400], [3, 14400]]
        .iter()
        .map(|row| row.iter().copied().map(Value::from_i64).collect())
        .collect();
    assert_eq!(rows, expected);

    let metrics = stmt.metrics().hash_join;
    assert!(metrics.spill_chunks > 0, "build did not spill: {metrics:?}");
    assert!(
        metrics.grace_partitions_processed > 0,
        "no pass went through grace: {metrics:?}"
    );
}
