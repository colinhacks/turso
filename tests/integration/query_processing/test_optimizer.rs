#[allow(unused_imports)]
use turso_core::alloc::TursoIteratorExt;
use turso_core::translate::optimizer::Optimizable;
use turso_sqlite::SqliteDialect;

use turso_core::translate::emitter::{DoubleQuotedDml, Resolver};

use turso_core::sync::RwLock;
use turso_core::{schema::Schema, DatabaseCatalog, SymbolTable};

use rustc_hash::FxHashMap as HashMap;

use turso_parser::ast::{self, Expr, FunctionTail, Name, TableInternalId};

fn empty_resolver<'a>(
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, turso_core::sync::Arc<Schema>>>,
    temp_database: &'a RwLock<Option<turso_core::connection::TempDatabase>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    namespaces: &'a turso_core::Qualifiers,
    syms: &'a SymbolTable,
) -> Resolver<'a> {
    Resolver::new(
        schema,
        database_schemas,
        temp_database,
        attached_databases,
        namespaces,
        syms,
        true,
        DoubleQuotedDml::Enabled,
        turso_core::sync::Arc::new(SqliteDialect),
        None,
        turso_core::security::roles::RoleId::SUPERUSER,
    )
}

fn no_tail() -> FunctionTail {
    FunctionTail {
        filter_clause: None,
        over_clause: None,
    }
}

fn fn_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        name: Name::exact(name.to_string()),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: vec![],
        within_group: vec![],
        filter_over: no_tail(),
    }
}

#[test]
fn constant_classifier_for_coalesce_with_in_list() {
    let schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let syms = SymbolTable::new();
    let database_schemas = RwLock::new(HashMap::default());
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let namespaces = turso_core::Qualifiers::default();
    let temp_database = RwLock::new(None);
    let resolver = empty_resolver(
        &schema,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &namespaces,
        &syms,
    );

    let expr = fn_call(
        "coalesce",
        vec![
            fn_call(
                "length",
                vec![Expr::Literal(ast::Literal::String("a".into()))],
            ),
            Expr::InList {
                lhs: Box::new(fn_call(
                    "hex",
                    vec![Expr::Literal(ast::Literal::Blob("X'01'".into()))],
                )),
                not: false,
                rhs: vec![Box::new(Expr::Literal(ast::Literal::Blob("X'02'".into())))],
            },
        ],
    );

    assert!(expr.is_constant(&resolver));
}

#[test]
fn constant_classifier_for_quote_of_column() {
    let schema = Schema::with_options(true, &SqliteDialect).unwrap();
    let syms = SymbolTable::new();
    let database_schemas = RwLock::new(HashMap::default());
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let namespaces = turso_core::Qualifiers::default();
    let temp_database = RwLock::new(None);
    let resolver = empty_resolver(
        &schema,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &namespaces,
        &syms,
    );

    let expr = fn_call(
        "quote",
        vec![Expr::Column {
            database: None,
            table: TableInternalId::default(),
            column: 0,
            is_rowid_alias: false,
        }],
    );

    assert!(!expr.is_constant(&resolver));
}
