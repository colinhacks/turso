use std::sync::Arc;
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::sync::RwLock;
use turso_core::translate::emitter::DoubleQuotedDml;
use turso_core::translate::emitter::Resolver;
use turso_core::DatabaseCatalog;
use turso_core::SymbolTable;
use turso_sqlite::SqliteDialect;

/// A resolver over an empty main schema with the SQLite dialect. Its state is leaked so
/// that a test can create one in a single call.
pub fn empty_resolver() -> Resolver<'static> {
    Resolver::new(
        Box::leak(Box::new(
            Schema::with_options(true, &SqliteDialect).unwrap(),
        )),
        Box::leak(Box::new(RwLock::new(Default::default()))),
        Box::leak(Box::new(RwLock::new(None))),
        Box::leak(Box::new(RwLock::new(DatabaseCatalog::new()))),
        Box::leak(Box::new(turso_core::Qualifiers::default())),
        Box::leak(Box::new(SymbolTable::new())),
        true,
        DoubleQuotedDml::Enabled,
        Arc::new(SqliteDialect),
        None,
        RoleId::SUPERUSER,
    )
}
