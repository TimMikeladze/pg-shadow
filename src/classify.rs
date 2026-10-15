//! Statement classification with the real Postgres parser (libpg_query).
//!
//! For every statement the proxy forwards it needs to know two things:
//!  1. Can the result be cached? Only a single plain `SELECT` whose result depends on
//!     nothing but table contents qualifies: no time functions, no volatile or
//!     user-defined functions, no row locks, no sampling, no writes hidden in CTEs.
//!  2. What does it do to the session? Writes inside a transaction stop caching for the
//!     rest of it; anything that might change session settings (SET, DO, unknown
//!     functions, ...) makes the proxy re-read the session's settings before caching again.
//!
//! Everything defaults to "not cacheable, may change anything". Unknown is unsafe.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;
use pg_query::protobuf::{self, a_const, DiscardMode, SqlValueFunctionOp, TransactionStmtKind};
use pg_query::NodeEnum;

/// A relation referenced by a query, as written (identifiers already case-folded by the parser).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelName {
    pub schema: Option<String>,
    pub name: String,
    /// The name also matches a CTE in the query, so it may not refer to a table at all.
    pub maybe_cte: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Isolation {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read uncommitted" => Some(Self::ReadUncommitted),
            "read committed" => Some(Self::ReadCommitted),
            "repeatable read" => Some(Self::RepeatableRead),
            "serializable" => Some(Self::Serializable),
            _ => None,
        }
    }

    /// READ UNCOMMITTED behaves as READ COMMITTED in Postgres.
    pub fn is_read_committed(self) -> bool {
        matches!(self, Self::ReadCommitted | Self::ReadUncommitted)
    }
}

/// What a statement (or a multi-statement string) does to the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Read-only, touches nothing but data: plain SELECT with allowlisted functions, SHOW.
    Read,
    /// BEGIN / START TRANSACTION, with an explicit isolation level if one was given.
    Begin(Option<Isolation>),
    /// SHOW: reads a setting, runs no user code.
    Show,
    /// COMMIT or ROLLBACK: ends the transaction.
    TxnEnd,
    /// SAVEPOINT, RELEASE, ROLLBACK TO: no effect on settings or transaction boundaries.
    TxnControl,
    /// INSERT/UPDATE/DELETE/MERGE using only allowlisted functions: writes data,
    /// leaves settings alone.
    Write,
    /// DEALLOCATE name / DEALLOCATE ALL (None).
    Deallocate(Option<String>),
    /// DISCARD ALL, or anything else that resets session state.
    DiscardAll,
    /// Anything else: may write, may change settings or session state.
    Other,
}

#[derive(Debug, Clone)]
pub struct Classified {
    /// Present when the statement's result may be cached.
    pub cacheable: Option<Cacheable>,
    /// Why it is not cacheable (for logs).
    pub bypass_reason: &'static str,
    /// Effects of every statement in the string, in order.
    pub effects: Vec<Effect>,
    /// Normalized shape of the statement (constants removed), for logs. Never contains values.
    pub shape: u64,
}

#[derive(Debug, Clone)]
pub struct Cacheable {
    pub relations: Vec<RelName>,
    /// Names the query relies on being the built-in ones. The proxy checks, per database,
    /// that no user-defined object could take any of them over (see pgshadow.shadowed).
    pub functions: Vec<String>,
    pub operators: Vec<String>,
    /// Type and collation names.
    pub types: Vec<String>,
    /// Columns cast to a date/time type: the proxy confirms they are date/time columns.
    pub clock_columns: Vec<String>,
}

impl Classified {
    fn not_cacheable(reason: &'static str, effects: Vec<Effect>, shape: u64) -> Self {
        Self {
            cacheable: None,
            bypass_reason: reason,
            effects,
            shape,
        }
    }

    /// True if no statement could have written data.
    pub fn is_read_only(&self) -> bool {
        self.effects.iter().all(|e| {
            matches!(
                e,
                Effect::Read
                    | Effect::Show
                    | Effect::TxnControl
                    | Effect::TxnEnd
                    | Effect::Begin(_)
            )
        })
    }

    /// True if a statement might have changed session settings or state.
    pub fn may_change_settings(&self) -> bool {
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::Other | Effect::DiscardAll))
    }
}

/// Functions whose result depends only on their arguments and session settings that are
/// part of the cache key. Aggregates and window functions included. Anything else
/// (now(), random(), nextval(), current_setting(), user-defined functions, ...) makes a
/// query uncacheable.
const SAFE_FUNCTIONS: &[&str] = &[
    // aggregates
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "bool_and",
    "bool_or",
    "every",
    "array_agg",
    "string_agg",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "percentile_cont",
    "percentile_disc",
    "mode",
    "bit_and",
    "bit_or",
    "bit_xor",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "regr_count",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
    "any_value",
    // window
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
    // strings
    "lower",
    "upper",
    "initcap",
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "bit_length",
    "substr",
    "substring",
    "left",
    "right",
    "lpad",
    "rpad",
    "btrim",
    "ltrim",
    "rtrim",
    "trim",
    "concat",
    "concat_ws",
    "replace",
    "split_part",
    "strpos",
    "position",
    "starts_with",
    "reverse",
    "repeat",
    "ascii",
    "chr",
    "md5",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "encode",
    "decode",
    "format",
    "quote_ident",
    "quote_literal",
    "quote_nullable",
    "regexp_replace",
    "regexp_match",
    "regexp_matches",
    "regexp_like",
    "regexp_count",
    "regexp_instr",
    "regexp_substr",
    "regexp_split_to_array",
    "regexp_split_to_table",
    "translate",
    "overlay",
    "to_char",
    "to_hex",
    "to_ascii",
    "unistr",
    "normalize",
    "string_to_array",
    "string_to_table",
    "array_to_string",
    "convert_from",
    "convert_to",
    // numbers
    "abs",
    "round",
    "ceil",
    "ceiling",
    "floor",
    "trunc",
    "mod",
    "power",
    "pow",
    "sqrt",
    "cbrt",
    "exp",
    "ln",
    "log",
    "log10",
    "sign",
    "div",
    "degrees",
    "radians",
    "pi",
    "gcd",
    "lcm",
    "width_bucket",
    "scale",
    "min_scale",
    "trim_scale",
    "factorial",
    "to_number",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    // date/time functions that do not read the clock
    "date_trunc",
    "date_part",
    "extract",
    "date_bin",
    "make_date",
    "make_time",
    "make_timestamp",
    "make_timestamptz",
    "make_interval",
    "to_timestamp",
    "to_date",
    "justify_days",
    "justify_hours",
    "justify_interval",
    "isfinite",
    "timezone",
    // json
    "to_json",
    "to_jsonb",
    "row_to_json",
    "array_to_json",
    "json_build_object",
    "jsonb_build_object",
    "json_build_array",
    "jsonb_build_array",
    "json_object",
    "jsonb_object",
    "json_array_length",
    "jsonb_array_length",
    "json_typeof",
    "jsonb_typeof",
    "json_extract_path",
    "jsonb_extract_path",
    "json_extract_path_text",
    "jsonb_extract_path_text",
    "json_each",
    "jsonb_each",
    "json_each_text",
    "jsonb_each_text",
    "json_array_elements",
    "jsonb_array_elements",
    "json_array_elements_text",
    "jsonb_array_elements_text",
    "json_object_keys",
    "jsonb_object_keys",
    "jsonb_set",
    "jsonb_insert",
    "jsonb_strip_nulls",
    "json_strip_nulls",
    "jsonb_pretty",
    "jsonb_path_query",
    "jsonb_path_query_array",
    "jsonb_path_query_first",
    "jsonb_path_exists",
    "jsonb_path_match",
    "json_populate_record",
    "jsonb_populate_record",
    "json_to_record",
    "jsonb_to_record",
    "json_to_recordset",
    "jsonb_to_recordset",
    // arrays and sets
    "array_length",
    "array_lower",
    "array_upper",
    "array_ndims",
    "array_dims",
    "cardinality",
    "unnest",
    "array_position",
    "array_positions",
    "array_remove",
    "array_replace",
    "array_append",
    "array_prepend",
    "array_cat",
    "array_fill",
    "array_reverse",
    "array_sort",
    "trim_array",
    "generate_series",
    "generate_subscripts",
    // misc
    "coalesce",
    "nullif",
    "greatest",
    "least",
    "num_nulls",
    "num_nonnulls",
    "host",
    "masklen",
    "network",
    "family",
    "lower_inf",
    "upper_inf",
    "isempty",
    "range_merge",
];

/// Functions with no side effects whose results are NOT stable (time, randomness,
/// session info). A SELECT using them is read-only but never cached.
const READ_ONLY_UNCACHEABLE: &[&str] = &[
    "now",
    "clock_timestamp",
    "statement_timestamp",
    "transaction_timestamp",
    "timeofday",
    "random",
    "random_normal",
    "gen_random_uuid",
    "uuidv4",
    "uuidv7",
    "current_setting",
    "version",
    "pg_backend_pid",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "pg_postmaster_start_time",
    "pg_conf_load_time",
    "current_database",
    "current_schema",
    "current_schemas",
    "age",
    "pg_is_in_recovery",
    "txid_current_snapshot",
    "pg_current_snapshot",
    "pg_typeof",
    "has_table_privilege",
    "has_schema_privilege",
    "has_database_privilege",
    "pg_get_userbyid",
    "pg_table_is_visible",
    "obj_description",
    "col_description",
    "format_type",
    "pg_get_expr",
    "pg_get_indexdef",
    "pg_get_constraintdef",
    "pg_relation_size",
    "pg_total_relation_size",
    "pg_size_pretty",
    "to_regclass",
    "to_regtype",
];

/// Literal strings that Postgres turns into the current time when cast to a date/time type.
const TIME_WORDS: &[&str] = &["now", "today", "tomorrow", "yesterday"];

/// Whether a string could be (or contain) a date/time keyword Postgres evaluates against
/// the clock: 'now', 'today 10:00', ' Yesterday', ... Matched as a substring, ignoring case.
pub fn is_time_word(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    TIME_WORDS.iter().any(|w| lower.contains(w))
}

/// Date/time target types: casting text to these can read the clock.
const DATETIME_TYPES: &[&str] = &["timestamp", "timestamptz", "date", "time", "timetz"];

fn safe_functions() -> &'static HashSet<&'static str> {
    use std::sync::OnceLock;
    static SET: OnceLock<HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| SAFE_FUNCTIONS.iter().copied().collect())
}

fn read_only_functions() -> &'static HashSet<&'static str> {
    use std::sync::OnceLock;
    static SET: OnceLock<HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| {
        SAFE_FUNCTIONS
            .iter()
            .chain(READ_ONLY_UNCACHEABLE)
            .copied()
            .collect()
    })
}

/// Everything found anywhere in a statement's parse tree.
///
/// The walk runs over the parse tree serialized to JSON, so it visits every field.
/// (pg_query's own node iterator skips several clauses -- DISTINCT ON, WINDOW, VALUES,
/// GROUPING SETS, ARRAY[...], FOR UPDATE, XML/JSON constructors -- which would let a
/// `now()` hidden there go unnoticed.)
#[derive(Default, Debug)]
struct Scan {
    funcs: Vec<Vec<String>>,
    operators: Vec<String>,
    cast_types: Vec<Vec<String>>,
    /// A cast to a date/time type of something that could be text at run time.
    clock_cast: bool,
    /// Column names cast to a date/time type: fine only if they are date/time columns.
    clock_columns: Vec<String>,
    collations: Vec<String>,
    /// A cast to a catalog-name type (regclass, regtype, ...).
    reg_cast: bool,
    time_sql_value_function: bool,
    time_literal: bool,
    locking: bool,
    sample: bool,
    into: bool,
    dml: bool,
    cross_db: bool,
    rels: Vec<RelName>,
    ctes: HashSet<String>,
}

fn scan(v: &serde_json::Value, out: &mut Scan) {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            // RangeVar, whether it appears as a tagged node or as a typed field.
            if let (Some(Value::String(rel)), Some(_)) =
                (map.get("relname"), map.get("relpersistence"))
            {
                let schema = map.get("schemaname").and_then(Value::as_str).unwrap_or("");
                let catalog = map.get("catalogname").and_then(Value::as_str).unwrap_or("");
                if !catalog.is_empty() {
                    out.cross_db = true;
                }
                out.rels.push(RelName {
                    schema: (!schema.is_empty()).then(|| schema.to_string()),
                    name: rel.clone(),
                    maybe_cte: false,
                });
            }
            for (k, val) in map {
                match k.as_str() {
                    "FuncCall" => {
                        let names = val
                            .get("funcname")
                            .and_then(Value::as_array)
                            .map(|a| {
                                a.iter()
                                    .filter_map(|n| {
                                        n.pointer("/node/String/sval").and_then(Value::as_str)
                                    })
                                    .map(str::to_string)
                                    .collect()
                            })
                            .unwrap_or_default();
                        out.funcs.push(names);
                    }
                    "SqlvalueFunction" => {
                        let op = val.get("op").and_then(Value::as_i64).unwrap_or(0) as i32;
                        if !matches!(
                            SqlValueFunctionOp::try_from(op),
                            Ok(SqlValueFunctionOp::SvfopCurrentRole
                                | SqlValueFunctionOp::SvfopCurrentUser
                                | SqlValueFunctionOp::SvfopUser
                                | SqlValueFunctionOp::SvfopSessionUser
                                | SqlValueFunctionOp::SvfopCurrentCatalog
                                | SqlValueFunctionOp::SvfopCurrentSchema)
                        ) {
                            out.time_sql_value_function = true;
                        }
                    }
                    "AConst" => {
                        // the parser's decoded value, so escapes cannot hide a time word
                        if let Some(s) = val.pointer("/val/Sval/sval").and_then(Value::as_str) {
                            if is_time_word(s) {
                                out.time_literal = true;
                            }
                        }
                    }
                    "AExpr" => {
                        if let Some(names) = val.get("name").and_then(Value::as_array) {
                            for n in names {
                                if let Some(op) =
                                    n.pointer("/node/String/sval").and_then(Value::as_str)
                                {
                                    out.operators.push(op.to_string());
                                }
                            }
                        }
                    }
                    "TypeCast" => {
                        let names: Vec<String> = val
                            .pointer("/type_name/names")
                            .and_then(Value::as_array)
                            .map(|a| {
                                a.iter()
                                    .filter_map(|n| {
                                        n.pointer("/node/String/sval").and_then(Value::as_str)
                                    })
                                    .map(str::to_string)
                                    .collect()
                            })
                            .unwrap_or_default();
                        if let Some(last) = names.last() {
                            let base = last.trim_start_matches('_');
                            if base.starts_with("reg") {
                                out.reg_cast = true;
                            }
                            if DATETIME_TYPES.contains(&base) {
                                match val.get("arg") {
                                    Some(arg) if datetime_safe(arg, &mut out.clock_columns) => {}
                                    _ => out.clock_cast = true,
                                }
                            }
                        }
                        out.cast_types.push(names);
                    }
                    "CollateClause" => {
                        if let Some(n) = val
                            .get("collname")
                            .and_then(Value::as_array)
                            .and_then(|a| a.last())
                            .and_then(|n| n.pointer("/node/String/sval"))
                            .and_then(Value::as_str)
                        {
                            out.collations.push(n.to_string());
                        }
                    }
                    "LockingClause" => out.locking = true,
                    "RangeTableSample" => out.sample = true,
                    "InsertStmt" | "UpdateStmt" | "DeleteStmt" | "MergeStmt" => out.dml = true,
                    "into_clause" if !val.is_null() => out.into = true,
                    "CommonTableExpr" => {
                        if let Some(n) = val.get("ctename").and_then(Value::as_str) {
                            out.ctes.insert(n.to_string());
                        }
                    }
                    _ => {}
                }
                scan(val, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|i| scan(i, out)),
        _ => {}
    }
}

/// Functions over date/time values whose result, cast to a date/time type, cannot depend
/// on the clock (given date/time or constant arguments).
const DATETIME_FUNCS: &[&str] = &[
    "date_trunc",
    "date_bin",
    "timezone",
    "make_date",
    "make_time",
    "make_timestamp",
    "make_timestamptz",
    "to_timestamp",
    "date_add",
    "date_subtract",
    "make_interval",
];

/// Whether casting this expression to a date/time type is certain not to read the clock.
/// Only parameters (checked at run time), constants (checked for time words), columns
/// (recorded; the proxy confirms they are date/time columns), casts to date/time types,
/// date/time functions and + / - of those qualify. Anything that could be text at run
/// time -- a text column, `j->>'ts'`, string functions, CASE, ... -- does not.
fn datetime_safe(v: &serde_json::Value, cols: &mut Vec<String>) -> bool {
    use serde_json::Value;
    let Some(map) = v.as_object() else {
        return false;
    };
    if let Some(inner) = map.get("node") {
        return datetime_safe(inner, cols);
    }
    let Some((kind, val)) = map.iter().next().filter(|_| map.len() == 1) else {
        return false;
    };
    let names = |key: &str| -> Vec<String> {
        val.get(key)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|n| n.pointer("/node/String/sval").and_then(Value::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    match kind.as_str() {
        "ParamRef" | "AConst" => true,
        "ColumnRef" => {
            let fields = val.get("fields").and_then(Value::as_array);
            match fields
                .and_then(|f| f.last())
                .and_then(|n| n.pointer("/node/String/sval"))
                .and_then(Value::as_str)
            {
                Some(c) => {
                    cols.push(c.to_string());
                    true
                }
                None => false, // t.*
            }
        }
        "TypeCast" => {
            let target = names("names");
            let target = val
                .pointer("/type_name/names")
                .and_then(Value::as_array)
                .and_then(|a| a.last())
                .and_then(|n| n.pointer("/node/String/sval"))
                .and_then(Value::as_str)
                .or(target.last().map(String::as_str))
                .unwrap_or("");
            DATETIME_TYPES.contains(&target)
                && val.get("arg").is_some_and(|a| datetime_safe(a, cols))
        }
        "FuncCall" => {
            let f = names("funcname");
            let ok_name = match f.as_slice() {
                [n] => DATETIME_FUNCS.contains(&n.as_str()),
                [s, n] => s == "pg_catalog" && DATETIME_FUNCS.contains(&n.as_str()),
                _ => false,
            };
            ok_name
                && val
                    .get("args")
                    .and_then(Value::as_array)
                    .is_none_or(|args| args.iter().all(|a| datetime_safe(a, cols)))
        }
        "AExpr" => {
            let op = names("name");
            matches!(op.as_slice(), [o] if o == "+" || o == "-")
                && ["lexpr", "rexpr"]
                    .iter()
                    .all(|k| val.get(*k).is_none_or(|x| datetime_safe(x, cols)))
        }
        _ => false,
    }
}

fn func_name_in(parts: &[String], set: &HashSet<&'static str>) -> bool {
    match parts {
        [name] => set.contains(name.as_str()),
        [schema, name] if schema == "pg_catalog" => set.contains(name.as_str()),
        _ => false,
    }
}

/// Classify a SQL string. Never fails: anything the parser rejects is `Other`.
pub fn classify(sql: &str) -> Classified {
    let shape = shape_of(sql);
    let parsed = match pg_query::parse(sql) {
        Ok(p) => p,
        Err(_) => return Classified::not_cacheable("parse error", vec![Effect::Other], shape),
    };
    let stmts = &parsed.protobuf.stmts;
    if stmts.is_empty() {
        return Classified::not_cacheable("empty query", vec![], shape);
    }

    let mut effects = Vec::with_capacity(stmts.len());
    let mut scans = Vec::with_capacity(stmts.len());
    for raw in stmts {
        let node = raw.stmt.as_ref().and_then(|s| s.node.as_ref());
        let mut sc = Scan::default();
        match serde_json::to_value(raw) {
            Ok(v) => scan(&v, &mut sc),
            Err(_) => {
                effects.push(Effect::Other);
                scans.push(None);
                continue;
            }
        }
        effects.push(effect_of(node, &sc));
        scans.push(Some(sc));
    }

    if stmts.len() != 1 {
        return Classified::not_cacheable("multiple statements", effects, shape);
    }
    let Some(NodeEnum::SelectStmt(_)) = stmts[0].stmt.as_ref().and_then(|s| s.node.as_ref()) else {
        return Classified::not_cacheable("not a SELECT", effects, shape);
    };
    let Some(sc) = scans.pop().flatten() else {
        return Classified::not_cacheable("could not analyze", effects, shape);
    };
    if effects[0] != Effect::Read {
        return Classified::not_cacheable(
            "SELECT calling functions that may have side effects",
            effects,
            shape,
        );
    }
    let reason = if sc.into {
        Some("SELECT INTO")
    } else if sc.locking {
        Some("row locking (FOR UPDATE/SHARE)")
    } else if sc.sample {
        Some("TABLESAMPLE")
    } else if sc.time_sql_value_function {
        Some("time function")
    } else if sc.time_literal {
        Some("time literal ('now', 'today', ...)")
    } else if sc.clock_cast {
        Some("run-time text cast to a date/time type")
    } else if sc.reg_cast {
        Some("cast to a catalog-name type (regclass, ...)")
    } else if sc.cross_db {
        Some("cross-database reference")
    } else if sc.funcs.iter().any(|f| !func_name_in(f, safe_functions())) {
        Some("non-deterministic or unknown function")
    } else if sc.rels.is_empty() {
        Some("reads no tables")
    } else {
        None
    };
    if let Some(r) = reason {
        return Classified::not_cacheable(r, effects, shape);
    }

    // A name matching a CTE may still be a real table (e.g. `WITH f AS (SELECT * FROM f)`).
    // Keep every reference; the proxy requires each one that resolves to a relation to be
    // cacheable, and only skips names that resolve to nothing.
    let mut rels = sc.rels;
    for r in &mut rels {
        if r.schema.is_none() && sc.ctes.contains(&r.name) {
            r.maybe_cte = true;
        }
    }
    let mut seen = HashSet::new();
    rels.retain(|r| seen.insert(r.clone()));

    let mut functions: Vec<String> = sc.funcs.iter().filter_map(|f| f.last().cloned()).collect();
    functions.sort();
    functions.dedup();
    // Comparisons are resolved by type even when written implicitly (IN, BETWEEN, CASE,
    // joins with USING), so the common comparison operators are always included.
    let mut operators: Vec<String> = sc.operators.clone();
    operators.extend(["=", "<>", "<", ">", "<=", ">="].map(String::from));
    operators.sort();
    operators.dedup();
    let mut types: Vec<String> = sc
        .cast_types
        .iter()
        .filter_map(|t| t.last().cloned())
        .collect();
    types.extend(sc.collations.iter().cloned());
    types.sort();
    types.dedup();
    let mut clock_columns = sc.clock_columns.clone();
    clock_columns.sort();
    clock_columns.dedup();

    Classified {
        cacheable: Some(Cacheable {
            relations: rels,
            functions,
            operators,
            types,
            clock_columns,
        }),
        bypass_reason: "",
        effects,
        shape,
    }
}

fn effect_of(node: Option<&NodeEnum>, sc: &Scan) -> Effect {
    let Some(node) = node else {
        return Effect::Other;
    };
    let funcs_read_only = sc
        .funcs
        .iter()
        .all(|f| func_name_in(f, read_only_functions()));
    match node {
        NodeEnum::SelectStmt(_) => {
            if funcs_read_only && !sc.dml && !sc.locking && !sc.into {
                Effect::Read
            } else {
                Effect::Other
            }
        }
        NodeEnum::VariableShowStmt(_) => Effect::Show,
        NodeEnum::TransactionStmt(t) => {
            match TransactionStmtKind::try_from(t.kind) {
                Ok(TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart) => {
                    Effect::Begin(isolation_option(&t.options))
                }
                Ok(
                    TransactionStmtKind::TransStmtCommit | TransactionStmtKind::TransStmtRollback,
                ) if !t.chain => Effect::TxnEnd,
                Ok(
                    TransactionStmtKind::TransStmtSavepoint
                    | TransactionStmtKind::TransStmtRelease
                    | TransactionStmtKind::TransStmtRollbackTo,
                ) => Effect::TxnControl,
                _ => Effect::Other,
            }
        }
        NodeEnum::InsertStmt(_)
        | NodeEnum::UpdateStmt(_)
        | NodeEnum::DeleteStmt(_)
        | NodeEnum::MergeStmt(_) => {
            if funcs_read_only {
                Effect::Write
            } else {
                Effect::Other
            }
        }
        NodeEnum::DeallocateStmt(d) => {
            if d.isall || d.name.is_empty() {
                Effect::Deallocate(None)
            } else {
                Effect::Deallocate(Some(d.name.clone()))
            }
        }
        NodeEnum::DiscardStmt(d) => match DiscardMode::try_from(d.target) {
            Ok(
                DiscardMode::DiscardPlans
                | DiscardMode::DiscardSequences
                | DiscardMode::DiscardTemp,
            ) => Effect::Other,
            _ => Effect::DiscardAll,
        },
        _ => Effect::Other,
    }
}

fn isolation_option(options: &[protobuf::Node]) -> Option<Isolation> {
    for o in options {
        if let Some(NodeEnum::DefElem(d)) = &o.node {
            if d.defname == "transaction_isolation" {
                if let Some(arg) = &d.arg {
                    if let Some(NodeEnum::AConst(c)) = &arg.node {
                        if let Some(a_const::Val::Sval(s)) = &c.val {
                            return Isolation::parse(&s.sval);
                        }
                    }
                }
            }
        }
    }
    None
}

/// A hash of the statement with constants removed, for grouping in logs.
fn shape_of(sql: &str) -> u64 {
    match pg_query::fingerprint(sql) {
        Ok(f) => f.value,
        Err(_) => xxhash_rust::xxh3::xxh3_64(sql.as_bytes()),
    }
}

/// Bounded memo of classifications.
///
/// Two levels: the exact SQL text (prepared and ORM queries repeat their text), and the
/// statement's shape, for texts with inlined constants that never repeat exactly.
/// A shape (pg_query's fingerprint) ignores constant values but keeps statement types,
/// table, schema and function names, locking clauses, etc.; the only constant-dependent
/// part of classifying a SELECT is a time literal ('now', ...), so the shape level is
/// only used for SELECTs whose text contains none of those words anywhere.
pub struct Classifier {
    exact: Mutex<std::collections::HashMap<u64, (Arc<str>, Arc<Classified>)>>,
    shapes: Mutex<std::collections::HashMap<u64, Arc<Classified>>>,
    capacity: usize,
}

/// Whether the shape level may be used for this text: no time word anywhere (including
/// identifiers and comments -- a false positive only means the slower path), and no
/// escapes that could spell one (E'\x6eow', U&'\006eow').
fn shape_level_ok(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    !TIME_WORDS.iter().any(|w| lower.contains(w)) && !lower.contains('\\') && !lower.contains("u&")
}

impl Classifier {
    pub fn new(capacity: usize) -> Self {
        Self {
            exact: Mutex::new(Default::default()),
            shapes: Mutex::new(Default::default()),
            capacity,
        }
    }

    pub fn get(&self, sql: &str) -> Arc<Classified> {
        let h = xxhash_rust::xxh3::xxh3_64(sql.as_bytes());
        if let Some((text, c)) = self.exact.lock().get(&h) {
            if &**text == sql {
                return c.clone();
            }
        }
        let shape_ok = shape_level_ok(sql);
        if shape_ok {
            if let Ok(fp) = pg_query::fingerprint(sql) {
                if let Some(c) = self.shapes.lock().get(&fp.value) {
                    return c.clone();
                }
            }
        }
        let c = Arc::new(classify(sql)); // parse outside the locks
        if shape_ok && c.effects == [Effect::Read] && sql.trim_start().len() > 6 && is_select(&c) {
            let mut m = self.shapes.lock();
            if m.len() >= self.capacity {
                m.clear();
            }
            m.insert(c.shape, c.clone());
        }
        let mut m = self.exact.lock();
        if m.len() >= self.capacity {
            m.clear();
        }
        m.insert(h, (Arc::from(sql), c.clone()));
        c
    }
}

/// A single SELECT (cacheable or not, as long as it was classified as one).
fn is_select(c: &Classified) -> bool {
    c.cacheable.is_some()
        || c.bypass_reason != "not a SELECT"
            && c.bypass_reason != "multiple statements"
            && c.bypass_reason != "parse error"
            && c.bypass_reason != "empty query"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cacheable(sql: &str) -> bool {
        classify(sql).cacheable.is_some()
    }

    fn rels(sql: &str) -> Vec<(Option<String>, String, bool)> {
        let mut v: Vec<_> = classify(sql)
            .cacheable
            .unwrap()
            .relations
            .into_iter()
            .map(|r| (r.schema, r.name, r.maybe_cte))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn plain_selects_are_cacheable() {
        assert!(cacheable("SELECT * FROM orders WHERE id = $1"));
        assert!(cacheable(
            "select status, count(*), sum(total) from public.orders group by 1 order by 1"
        ));
        assert!(cacheable(
            "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id"
        ));
        assert!(cacheable(
            "SELECT date_trunc('day', created_at), count(*) FROM orders GROUP BY 1"
        ));
        assert!(cacheable(
            "SELECT row_number() OVER (ORDER BY id), lower(name) FROM t"
        ));
        assert!(cacheable("SELECT current_user, id FROM t"));
    }

    #[test]
    fn nondeterminism_is_not_cacheable() {
        for sql in [
            "SELECT now(), id FROM t",
            "SELECT id FROM t WHERE created_at > now() - interval '1 day'",
            "SELECT CURRENT_TIMESTAMP, id FROM t",
            "SELECT id FROM t WHERE d = CURRENT_DATE",
            "SELECT id FROM t WHERE ts > 'now'::timestamptz",
            "SELECT id FROM t WHERE d = 'today'",
            "SELECT random(), id FROM t",
            "SELECT nextval('s'), id FROM t",
            "SELECT current_setting('app.tenant'), id FROM t",
            "SELECT my_function(id) FROM t",
            "SELECT public.lower(name) FROM t",
            "SELECT id FROM t TABLESAMPLE SYSTEM (10)",
            "SELECT id FROM t FOR UPDATE",
            "SELECT id FROM t FOR SHARE SKIP LOCKED",
            "SELECT * INTO newt FROM t",
            "SELECT 1",
            "SELECT * FROM generate_series(1, 10)",
            // positions pg_query's own node iterator does not visit
            "SELECT ARRAY[now()], a FROM t",
            "SELECT a FROM t WHERE a = ANY(ARRAY[now()])",
            "SELECT DISTINCT ON (now()) a FROM t",
            "SELECT a FROM t WINDOW w AS (ORDER BY now())",
            "SELECT a FROM t GROUP BY GROUPING SETS ((now()))",
            "SELECT xmlelement(name x, now()) FROM t",
            "SELECT JSON_OBJECT('a': now()) FROM t",
            "SELECT a FROM t WHERE a = JSON_VALUE(b, '$.x' DEFAULT now() ON EMPTY)",
            "SELECT a FROM t, (VALUES (now())) v(x)",
            "SELECT a FROM t FOR NO KEY UPDATE OF t",
            "SELECT a FROM t JOIN u ON u.x = now()",
            "SELECT sum(a) FILTER (WHERE b > now()) FROM t",
            "SELECT a FROM t, LATERAL (SELECT now()) l",
            "SELECT a FROM t WHERE x = (WITH q AS (SELECT now()) SELECT * FROM q)",
            "SELECT a FROM db.s.t",
            // clock-dependent literals found by review
            "SELECT * FROM t WHERE ts > 'today 10:00'::timestamp",
            "SELECT * FROM t WHERE ts > 'Yesterday 23:59'",
            "SELECT * FROM t WHERE ts > E'\\x6eow'::timestamptz",
            "SELECT * FROM t WHERE ts > U&'\\006Eow'::timestamptz",
            "SELECT * FROM t WHERE ts > $$now$$::timestamptz",
            "SELECT * FROM t WHERE ts > ('n' || 'ow')::timestamptz",
            "SELECT * FROM t WHERE ts > concat(a, b)::date",
            "SELECT (j->>'ts')::timestamptz FROM t",
            "SELECT CASE WHEN a THEN b ELSE c END::date FROM t",
            "SELECT t.*::timestamp FROM t",
            "SELECT * FROM t WHERE ts > CAST(lower(a) AS timestamp)",
            // catalog-dependent output
            "SELECT 't'::regclass, a FROM t",
            "SELECT a::regtype FROM t",
            "SELECT pg_typeof(a) FROM t",
        ] {
            assert!(!cacheable(sql), "{sql} should not be cacheable");
        }
    }

    #[test]
    fn date_casts_of_columns_and_constants_stay_cacheable() {
        assert!(cacheable(
            "SELECT created_at::date, count(*) FROM t GROUP BY 1"
        ));
        assert!(cacheable(
            "SELECT * FROM t WHERE created_at > '2026-01-01'::timestamptz"
        ));
        assert!(cacheable(
            "SELECT date_trunc('day', created_at)::date FROM t"
        ));
        assert!(cacheable(
            "SELECT * FROM t WHERE created_at > $1::timestamptz"
        ));
    }

    #[test]
    fn date_cast_columns_are_reported() {
        let c =
            classify("SELECT created_at::date, CAST(d AS timestamptz) + interval '1 day' FROM t")
                .cacheable
                .unwrap();
        assert_eq!(c.clock_columns, vec!["created_at", "d"]);
        let c = classify("SELECT date_trunc('day', t.ts)::date FROM t ORDER BY v COLLATE \"C\"")
            .cacheable
            .unwrap();
        assert_eq!(c.clock_columns, vec!["ts"]);
        assert!(c.types.contains(&"C".to_string()));
    }

    #[test]
    fn names_for_the_shadowing_check() {
        let c = classify("SELECT lower(a), a::int4 FROM t WHERE b ~~ 'x%' AND c @> $1")
            .cacheable
            .unwrap();
        assert_eq!(c.functions, vec!["lower"]);
        assert!(c.operators.contains(&"~~".to_string()) && c.operators.contains(&"@>".to_string()));
        assert!(c.operators.contains(&"=".to_string()));
        assert_eq!(c.types, vec!["int4"]);
    }

    #[test]
    fn writes_and_multi_statements_are_not_cacheable() {
        assert!(!cacheable(
            "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"
        ));
        assert!(!cacheable("SELECT * FROM t; SELECT * FROM u"));
        assert!(!cacheable("UPDATE t SET a = 1"));
        assert!(!cacheable("not sql at all"));
        assert!(!cacheable(""));
    }

    #[test]
    fn relations_include_cte_shadowed_tables() {
        assert_eq!(
            rels("WITH f AS (SELECT * FROM f) SELECT * FROM f JOIN s.g ON true"),
            vec![
                (None, "f".into(), true),
                (Some("s".into()), "g".into(), false)
            ]
        );
        assert_eq!(
            rels("SELECT * FROM \"Mixed\" , lower_t"),
            vec![
                (None, "Mixed".into(), false),
                (None, "lower_t".into(), false)
            ]
        );
        assert_eq!(
            rels("SELECT (SELECT max(x) FROM inner_t) FROM outer_t"),
            vec![
                (None, "inner_t".into(), false),
                (None, "outer_t".into(), false)
            ]
        );
    }

    #[test]
    fn shape_memo_agrees_with_full_classification() {
        let c = Classifier::new(1000);
        // Same shape, different constants -> shape memo.
        let a = c.get("SELECT * FROM orders WHERE id = 5");
        let b = c.get("SELECT * FROM orders WHERE id = 99");
        assert!(a.cacheable.is_some() && b.cacheable.is_some());
        // A time literal in an otherwise identical shape must not reuse it.
        let n = c.get("SELECT * FROM orders WHERE id = 5 AND d < 'now'");
        assert!(n.cacheable.is_none());
        let n = c.get("SELECT * FROM orders WHERE d < 'Today'::date");
        assert!(n.cacheable.is_none());
        // Escapes that decode to a time word must not reuse the shape either.
        c.get("SELECT * FROM orders WHERE d < '2024-01-01'::timestamptz");
        assert!(c
            .get("SELECT * FROM orders WHERE d < E'\\x6eow'::timestamptz")
            .cacheable
            .is_none());
        // Different functions are different shapes.
        c.get("SELECT lower(name) FROM orders WHERE id = 1");
        assert!(c
            .get("SELECT random() FROM orders WHERE id = 1")
            .cacheable
            .is_none());
        assert!(c
            .get("SELECT * FROM orders WHERE id = 1 FOR UPDATE")
            .cacheable
            .is_none());
        for sql in [
            "UPDATE orders SET a = 1 WHERE id = 2",
            "UPDATE orders SET a = 1 WHERE id = 3",
        ] {
            assert_eq!(c.get(sql).effects, vec![Effect::Write]);
        }
        assert_eq!(
            c.get("BEGIN ISOLATION LEVEL SERIALIZABLE").effects,
            vec![Effect::Begin(Some(Isolation::Serializable))]
        );
        assert_eq!(
            c.get("BEGIN ISOLATION LEVEL READ COMMITTED").effects,
            vec![Effect::Begin(Some(Isolation::ReadCommitted))]
        );
    }

    #[test]
    fn effects() {
        assert_eq!(classify("BEGIN").effects, vec![Effect::Begin(None)]);
        assert_eq!(
            classify("BEGIN ISOLATION LEVEL REPEATABLE READ").effects,
            vec![Effect::Begin(Some(Isolation::RepeatableRead))]
        );
        assert_eq!(
            classify("START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY").effects,
            vec![Effect::Begin(Some(Isolation::Serializable))]
        );
        assert_eq!(classify("COMMIT").effects, vec![Effect::TxnEnd]);
        assert_eq!(classify("ROLLBACK").effects, vec![Effect::TxnEnd]);
        assert_eq!(classify("COMMIT AND CHAIN").effects, vec![Effect::Other]);
        assert_eq!(classify("SAVEPOINT a").effects, vec![Effect::TxnControl]);
        assert_eq!(classify("UPDATE t SET a = 1").effects, vec![Effect::Write]);
        assert_eq!(
            classify("UPDATE t SET a = set_config('x','y',false)").effects,
            vec![Effect::Other]
        );
        assert_eq!(classify("SET search_path = x").effects, vec![Effect::Other]);
        assert_eq!(classify("SHOW search_path").effects, vec![Effect::Show]);
        assert_eq!(classify("DISCARD TEMP").effects, vec![Effect::Other]);
        assert_eq!(
            classify("SELECT set_config('a','b',false)").effects,
            vec![Effect::Other]
        );
        assert_eq!(classify("SELECT now()").effects, vec![Effect::Read]);
        assert_eq!(classify("SELECT nextval('s')").effects, vec![Effect::Other]);
        assert_eq!(
            classify("DEALLOCATE foo").effects,
            vec![Effect::Deallocate(Some("foo".into()))]
        );
        assert_eq!(
            classify("DEALLOCATE ALL").effects,
            vec![Effect::Deallocate(None)]
        );
        assert_eq!(classify("DISCARD ALL").effects, vec![Effect::DiscardAll]);
        assert_eq!(classify("DO $$ BEGIN END $$").effects, vec![Effect::Other]);
        assert!(classify("BEGIN; SELECT 1").is_read_only());
        assert!(!classify("SELECT 1; SET x = 1").is_read_only());
    }
}
