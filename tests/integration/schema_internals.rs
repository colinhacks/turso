use rustc_hash::FxHashMap as HashMap;
#[allow(unused_imports)]
use turso_core::alloc::*;
use turso_core::incremental::view::dbsp_version_marker_table_name;
use turso_core::incremental::view::DBSP_CIRCUIT_VERSION;
use turso_core::namespace::NamespaceId;
use turso_core::schema::SequenceMetadata;
use turso_core::schema::*;
#[allow(unused_imports)]
use turso_core::storage::btree::CursorTrait;
use turso_core::sync::Arc;
use turso_core::translate::plan::ColumnMask;
use turso_core::LimboError;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

use turso_core::security::roles::SessionRole;

#[test]
fn builtin_types_read_as_the_kind_their_decode_gives() {
    use turso_core::{
        translate::expr::expr_type::value_type, ConnectOpts, Database, DatabaseOpts, MemoryIO,
        OpenFlags, IO, MAIN_DB_ID,
    };
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new().with_custom_types(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let syms = conn.syms.read();
    conn.with_schema(MAIN_DB_ID, |schema| {
        assert!(schema.type_registry.values().all(|t| t.is_builtin));
        for (name, type_def) in &schema.type_registry {
            assert_eq!(
                Some(type_def.value_type()),
                value_type(type_def, schema, &syms),
                "built-in type {name}"
            );
        }
    });
}

#[test]
#[should_panic(expected = "kinds of type price were read before they were worked out")]
fn reading_the_kind_of_a_type_not_yet_worked_out_panics() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    schema
        .insert_type_from_sql(
            &SqliteDialect,
            "CREATE TYPE price(value any) BASE text ENCODE value DECODE CAST(value AS real)",
        )
        .unwrap();
    schema.get_type_def_unchecked("price").unwrap().value_type();
}

#[test]
fn persisted_materialized_views_are_loaded_in_dependency_order() {
    let mut views = HashMap::default();
    views.insert(
        "third".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW third AS SELECT * FROM second".to_string(),
            5,
        ),
    );
    views.insert(
        "first".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW first AS SELECT * FROM base".to_string(),
            3,
        ),
    );
    views.insert(
        "second".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW second AS SELECT * FROM first".to_string(),
            4,
        ),
    );

    let ordered = Schema::with_options(true, &SqliteDialect)
        .unwrap()
        .order_persisted_materialized_views(&SqliteDialect, views)
        .unwrap();
    assert_eq!(
        ordered
            .into_iter()
            .map(|(name, _)| name)
            .collect::<std::vec::Vec<_>>(),
        vec!["first", "second", "third"]
    );
}

#[test]
fn persisted_materialized_view_cycle_is_corrupt() {
    let mut views = HashMap::default();
    views.insert(
        "left_view".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW left_view AS SELECT * FROM right_view".to_string(),
            3,
        ),
    );
    views.insert(
        "right_view".to_string(),
        (
            NamespaceId::ROOT,
            "CREATE MATERIALIZED VIEW right_view AS SELECT * FROM left_view".to_string(),
            4,
        ),
    );

    let error = Schema::with_options(true, &SqliteDialect)
        .unwrap()
        .order_persisted_materialized_views(&SqliteDialect, views)
        .unwrap_err();
    assert!(matches!(error, LimboError::Corrupt(_)));
}

#[test]
fn incompatible_materialized_view_can_be_removed() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let root = schema.namespace_mut(NamespaceId::ROOT);
    root.incompatible_views.insert("old_view".to_string());
    root.incompatible_materialized_view_roots
        .insert("old_view".to_string(), 42);
    root.table_to_materialized_views
        .insert("base".to_string(), vec!["old_view".to_string()]);

    assert!(schema.materialized_view_exists(NamespaceId::ROOT, "old_view"));
    assert_eq!(
        schema.incompatible_materialized_view_root(NamespaceId::ROOT, "old_view"),
        Some(42)
    );
    schema.remove_view(NamespaceId::ROOT, "old_view").unwrap();
    assert!(!schema.materialized_view_exists(NamespaceId::ROOT, "old_view"));
    assert!(!schema.is_incompatible_view(NamespaceId::ROOT, "old_view"));
    assert!(schema
        .get_dependent_materialized_views(NamespaceId::ROOT, "base")
        .is_empty());
}

#[test]
fn stateless_materialized_view_requires_current_version_marker() -> Result<()> {
    fn add_marker(schema: &mut Schema, name: &str, root_page: i64) -> Result<()> {
        let sql = format!(
            "CREATE TABLE {} (version INTEGER)",
            turso_core::util::quote_identifier(name)
        );
        schema.add_btree_table(
            NamespaceId::ROOT,
            Arc::new(BTreeTable::from_stored_sql(
                &SqliteDialect,
                &sql,
                root_page,
                false,
            )?),
        )
    }

    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    assert!(!schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));

    let old_version = DBSP_CIRCUIT_VERSION - 1;
    let other_view_marker = format!("{DBSP_METADATA_TABLE_PREFIX}{old_version}_stateless__n1");
    add_marker(&mut schema, &other_view_marker, 2)?;
    assert!(
        !schema.dbsp_storage_version_mismatch(NamespaceId::ROOT, "stateless"),
        "a similarly prefixed view must not own this view's version marker"
    );

    let current_marker = dbsp_version_marker_table_name("stateless");
    add_marker(&mut schema, &current_marker, 3)?;
    assert!(schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));

    let old_marker = format!("{DBSP_METADATA_TABLE_PREFIX}{old_version}_stateless");
    add_marker(&mut schema, &old_marker, 4)?;
    assert!(!schema.has_compatible_materialized_view_storage(NamespaceId::ROOT, "stateless"));
    Ok(())
}

use turso_core::alloc::vec;

fn indices(mask: &ColumnMask) -> Vec<usize> {
    let mut v: Vec<usize> = mask.iter().try_collect().unwrap();
    v.sort_unstable();
    v
}

fn stored(bits: &ColumnMask) -> Vec<usize> {
    let mut v: Vec<usize> = bits.iter().try_collect().unwrap();
    v.sort_unstable();
    v
}

#[test]
fn gencol_graph_no_virtual_columns() -> Result<()> {
    let t = BTreeTable::from_stored_sql(&SqliteDialect, "CREATE TABLE t(a, b)", 0, false)?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0]);
    assert_eq!(indices(&t.columns_affected_by_update([0, 1])?), vec![0, 1]);
    assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([])?), Vec::<usize>::new());
    Ok(())
}

#[test]
fn gencol_graph_linear_chain() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL)",
        0,
        false,
    )?;
    // affected-by({a}) = {a, b, c}
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1, 2]);
    // affected-by({b}) = {b, c} (b is virtual, but updating it still propagates through dependents)
    assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
    // deps-of({c}) = {a} (transitive stored deps of virtual c)
    assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0]);
    // deps-of({b}) = {a}
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    // deps-of({a}) = {a} (stored target included)
    assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_diamond() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
        0,
        false,
    )?;
    assert_eq!(
        indices(&t.columns_affected_by_update([0])?),
        vec![0, 1, 2, 3]
    );
    assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_multiple_stored_roots() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b, c AS (a + b) VIRTUAL)",
        0,
        false,
    )?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 2]);
    assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
    assert_eq!(
        indices(&t.columns_affected_by_update([0, 1])?),
        vec![0, 1, 2]
    );
    assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0, 1]);
    Ok(())
}

#[test]
fn gencol_graph_empty_input() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL)",
        0,
        false,
    )?;
    assert!(t.columns_affected_by_update(std::iter::empty())?.is_empty());
    assert!(t.dependencies_of_columns(std::iter::empty())?.is_empty());
    Ok(())
}

#[test]
fn gencol_graph_disjoint_components() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c, d AS (c) VIRTUAL)",
        0,
        false,
    )?;
    assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1]);
    assert_eq!(indices(&t.columns_affected_by_update([2])?), vec![2, 3]);
    assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
    assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![2]);
    Ok(())
}

#[test]
fn gencol_graph_deep_chain() -> Result<()> {
    // Build 50-long chain: c0 (stored), c1 := c0, c2 := c1, ... c49 := c48.
    let mut sql = String::from("CREATE TABLE t(c0");
    for i in 1..50 {
        sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
    }
    sql.push(')');
    let t = BTreeTable::from_stored_sql(&SqliteDialect, &sql, 0, false)?;
    // affected-by({c0}) = {c0..c49}
    let affected = t.columns_affected_by_update([0])?;
    assert_eq!(affected.count(), 50);
    // deps-of({c49}) = {c0}
    assert_eq!(stored(&t.dependencies_of_columns([49])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_very_deep_chain_no_stack_overflow() -> Result<()> {
    // Validates that the iterative Kahn's + DP don't blow the stack on
    // realistic worst-case generated-column depth.
    let mut sql = String::from("CREATE TABLE t(c0");
    for i in 1..500 {
        sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
    }
    sql.push(')');
    let t = BTreeTable::from_stored_sql(&SqliteDialect, &sql, 0, false)?;
    assert_eq!(t.columns_affected_by_update([0])?.count(), 500);
    assert_eq!(stored(&t.dependencies_of_columns([499])?), vec![0]);
    Ok(())
}

#[test]
fn gencol_graph_rowid_sentinel_passthrough() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL)",
        0,
        false,
    )?;
    let affected = t.columns_affected_by_update([ROWID_SENTINEL])?;
    // ROWID_SENTINEL is preserved in the mask flag but does not propagate through the graph
    // (no generated column can depend on ROWID_SENTINEL directly).
    assert!(affected.get(ROWID_SENTINEL));
    assert_eq!(affected.count(), 1);
    Ok(())
}

#[test]
fn gencol_graph_transpose_duality() -> Result<()> {
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL, d AS (a + c) VIRTUAL)",
        0,
        false,
    )?;
    let graph = t.column_graph()?;
    // j ∈ dependencies[i] iff i ∈ dependents[j]
    for i in 0..graph.dependencies.len() {
        for j in graph.dependencies[i].iter() {
            assert!(
                graph.dependents[j].get(i),
                "transpose violated: {j} is in dependencies[{i}] but {i} is not in dependents[{j}]"
            );
        }
        for j in graph.dependents[i].iter() {
            assert!(
                graph.dependencies[j].get(i),
                "transpose violated: {j} is in dependents[{i}] but {i} is not in dependencies[{j}]"
            );
        }
    }
    Ok(())
}

#[test]
fn gencol_graph_idempotence() -> Result<()> {
    // affected_by(affected_by(xs)) == affected_by(xs).
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
        0,
        false,
    )?;
    let once = t.columns_affected_by_update([0, 1])?;
    let twice = t.columns_affected_by_update(once.iter())?;
    assert_eq!(indices(&twice), indices(&once));
    Ok(())
}

#[test]
fn gencol_graph_union_monotonicity() -> Result<()> {
    // affected_by(A ∪ B) == affected_by(A) ∪ affected_by(B).
    let t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b) VIRTUAL, e AS (c + d) VIRTUAL)",
        0,
        false,
    )?;
    let mut expected = t.columns_affected_by_update([0])?;
    let b_mask = t.columns_affected_by_update([1])?;
    expected.union_with(&b_mask).unwrap();
    let union_mask = t.columns_affected_by_update([0, 1])?;
    assert_eq!(indices(&union_mask), indices(&expected));
    Ok(())
}

#[test]
#[allow(clippy::redundant_clone)]
fn gencol_graph_clone_invalidates_cache() -> Result<()> {
    // After cloning a BTreeTable, the cache is fresh. Mutating columns on
    // the clone via `columns_mut()` keeps it fresh; `prepare_generated_columns`
    // rebuilds correctly.
    let original = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL)",
        0,
        false,
    )?;
    // Force the cache to be populated on the original.
    let _ = original.columns_affected_by_update([0])?;
    assert!(original.peek_column_dependencies().is_some());

    // Clone: ResetOnClone makes the cloned cache empty. We keep a real clone
    // (not a move) because the point of the test is that Clone produces a
    // fresh cache independently from the original.
    let cloned = original.clone();
    assert!(cloned.peek_column_dependencies().is_none());
    // Original's cache is still populated — clone didn't touch it.
    assert!(original.peek_column_dependencies().is_some());

    // The clone still returns correct results — cache rebuilds lazily.
    assert_eq!(
        indices(&cloned.columns_affected_by_update([0])?),
        vec![0, 1]
    );
    assert!(cloned.peek_column_dependencies().is_some());
    Ok(())
}

#[test]
fn gencol_graph_columns_mut_invalidates_cache() -> Result<()> {
    let mut t = BTreeTable::from_stored_sql(
        &SqliteDialect,
        "CREATE TABLE t(a, b AS (a) VIRTUAL)",
        0,
        false,
    )?;
    // Force the cache to be populated.
    let _ = t.columns_affected_by_update([0])?;
    assert!(t.peek_column_dependencies().is_some());

    // Any access through columns_mut() wipes the cache, even if we don't mutate.
    let _ = t.columns_mut();
    assert!(t.peek_column_dependencies().is_none());
    Ok(())
}

/// `install_sequence_descriptor` must surface an error when the
/// persisted metadata is invalid (e.g. min > max) rather than
/// silently dropping the sequence. The internal backing table is
/// the only persistent record of the sequence; a silent drop would
/// manifest later as a misleading "sequence does not exist" on the
/// next nextval that masks real on-disk corruption.
#[test]
fn install_sequence_descriptor_rejects_invalid_metadata_with_corruption_error() {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let bogus = SequenceMetadata {
        // increment of zero is universally invalid; Sequence::new
        // rejects it with a clear error.
        start: 0,
        increment: 0,
        min: 0,
        max: 100,
        cycle: false,
    };
    let result = schema.install_sequence_descriptor("broken_seq", bogus);
    let err = result.expect_err(
        "invalid persisted descriptor must surface as an error, not be silently dropped",
    );
    assert!(
        matches!(err, LimboError::Corrupt(_)),
        "expected Corrupt error for unreadable internal backing table, got: {err:?}",
    );
    assert!(
        schema
            .get_sequence(NamespaceId::ROOT, "broken_seq")
            .is_none(),
        "rejected descriptor must not land in the sequences map",
    );
}
