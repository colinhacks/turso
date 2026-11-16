//! The catalog tables and types that the schema of every database has,
//! whatever its dialect.

use super::{RangeOffsets, SchemaBuilder, Table, TypeDef, TypeOrigin};
use crate::pragma::PragmaVirtualTable;
use crate::sync::Arc;
use crate::vtab::{VirtualTable, VirtualTableType};
use turso_ext::VTabKind;

/// Starts the schema of a database: the built-in catalog tables and, if
/// `enable_custom_types`, the built-in types. A dialect adds its own tables
/// and types to the builder.
pub fn schema_builder(enable_custom_types: bool) -> crate::Result<SchemaBuilder> {
    let mut builder = SchemaBuilder::new()?.custom_types(enable_custom_types);
    if enable_custom_types {
        builder = builtin_types(builder)?;
    }
    builtin_catalog(builder, enable_custom_types)
}

fn builtin_types(mut builder: SchemaBuilder) -> crate::Result<SchemaBuilder> {
    use turso_parser::ast::{Cmd, Stmt};

    // Whether values of the type take a COLLATE. In PostgreSQL only text
    // types do, so of the types below only varchar does.
    const COLLATE: bool = true;
    const NO_COLLATE: bool = false;
    let types: &[(bool, RangeOffsets, &str)] = &[
        #[cfg(feature = "uuid")]
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE uuid(value any) BASE blob ENCODE CASE WHEN value IS NULL THEN NULL WHEN typeof(value) = 'blob' AND length(value) = 16 THEN value ELSE coalesce(uuid_blob(value), RAISE(ABORT, 'invalid UUID value')) END DECODE uuid_str(value) OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE boolean(value any) BASE integer ENCODE boolean_to_int(value) DECODE CASE WHEN value THEN 1 ELSE 0 END OPERATOR '<'"),
        #[cfg(feature = "json")]
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE json(value text) BASE text ENCODE json(value) DECODE value"),
        #[cfg(feature = "json")]
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE jsonb(value text) BASE blob ENCODE jsonb(value) DECODE json(value)"),
        (COLLATE, RangeOffsets::None, "CREATE TYPE varchar(value text, maxlen integer) BASE text COERCE CASE WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for varchar') END DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE date(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN date(value) IS NULL THEN RAISE(ABORT, 'invalid date value') ELSE date(value) END DECODE value OPERATOR '<'"),
        // ENCODE preserves sub-second precision through strftime + a rtrim pair
        // that strips trailing zeros and the dangling dot, matching PostgreSQL's
        // text format: whole seconds render as `HH:MM:SS` (no .000), trailing
        // zeros are dropped (`.500` -> `.5`), and the dot is removed when no
        // fractional digits remain. `time(...)` / `datetime(...)` would truncate
        // the fraction outright, silently dropping precision on insert.
        // Caveat: Turso's `%f` directive is millisecond resolution, so PG's
        // microsecond inputs are clamped to 3 digits (`.123456` -> `.123`).
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE time(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN time(value) IS NULL THEN RAISE(ABORT, 'invalid time value') ELSE rtrim(rtrim(strftime('%H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE timestamp(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN datetime(value) IS NULL THEN RAISE(ABORT, 'invalid timestamp value') ELSE rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE timestamptz(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN datetime(value) IS NULL THEN RAISE(ABORT, 'invalid timestamp value') ELSE rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Integer, "CREATE TYPE smallint(value integer) BASE integer COERCE CASE WHEN value BETWEEN -32768 AND 32767 THEN value ELSE RAISE(ABORT, 'integer out of range for smallint') END OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Integer, "CREATE TYPE bigint(value integer) BASE integer"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE inet(value text) BASE text ENCODE validate_ipaddr(value) DECODE value"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE cidr(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE macaddr(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE macaddr8(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, "CREATE TYPE bytea(value blob) BASE blob OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Numeric, "CREATE TYPE numeric(value any, precision integer, scale integer) BASE blob ENCODE numeric_encode(value) COERCE numeric_fit(value, precision, scale) DECODE numeric_decode(value)"),
    ];

    let aliases: &[(&str, &str)] = &[
        ("bool", "boolean"),
        ("int2", "smallint"),
        ("int8", "bigint"),
    ];
    for &(accepts_collate, range_offsets, sql) in types {
        let Ok(Some(Cmd::Stmt(Stmt::CreateType {
            type_name, body, ..
        }))) = crate::dialect::engine::parse_statement(sql)
        else {
            return Err(crate::LimboError::InternalError(format!(
                "failed to parse built-in type SQL: {sql}"
            )));
        };

        let type_def = TypeDef::from_create_type(
            &type_name,
            &body,
            TypeOrigin::Builtin {
                accepts_collate,
                range_offsets,
            },
            sql.to_string(),
        )?;
        let type_def = Arc::new(type_def);
        for (alias, target) in aliases {
            if type_name.eq_ignore_ascii_case(target) {
                builder = builder.type_def(alias, Arc::clone(&type_def));
            }
        }
        builder = builder.type_def(&type_name, type_def);
    }
    Ok(builder)
}

/// Adds the standard SQLite-style catalog tables.
///
/// `pragma_*` virtual tables use the dedicated `VirtualTableType::Pragma`
/// variant (they aren't `InternalVirtualTable`), so they are added as
/// tables.
fn builtin_catalog(
    mut builder: SchemaBuilder,
    enable_custom_types: bool,
) -> crate::Result<SchemaBuilder> {
    for vtab in pragma_vtabs() {
        builder = builder.table(
            vtab.name.to_owned(),
            Arc::new(Table::Virtual(Arc::new((*vtab).clone()))),
        );
    }

    #[cfg(feature = "json")]
    {
        builder = builder
            .internal_vtab(crate::json::vtab::JsonVirtualTable::json_each())?
            .internal_vtab(crate::json::vtab::JsonVirtualTable::json_tree())?;
    }
    #[cfg(feature = "cli_only")]
    {
        builder = builder
            .internal_vtab(crate::dbpage::DbPageTable::new())?
            .internal_vtab(crate::btree_dump::BtreeDumpTable::new())?;
    }
    if enable_custom_types {
        builder = builder.internal_vtab(crate::turso_types_vtab::TursoTypesTable::new())?;
    }
    Ok(builder)
}

/// Build a `VirtualTable` for each PRAGMA table-valued function.
fn pragma_vtabs() -> Vec<Arc<VirtualTable>> {
    PragmaVirtualTable::functions()
        .into_iter()
        .map(|(tab, schema_sql)| {
            Arc::new(VirtualTable {
                name: format!("pragma_{}", tab.pragma_name),
                columns: VirtualTable::resolve_columns(schema_sql)
                    .expect("pragma table-valued function schema resolution should not fail"),
                kind: VTabKind::TableValuedFunction,
                vtab_type: VirtualTableType::Pragma(tab),
                vtab_id: 0,
                is_droppable: false,
                innocuous: true,
            })
        })
        .collect()
}
