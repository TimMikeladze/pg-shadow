-- pgshadow: Postgres-side objects. Idempotent; `pgshadow install` runs this.
--
-- How it works: every opted-in table gets one statement-level trigger that records
-- (table, transaction id) in pgshadow.changelog, once per transaction. A cached result
-- remembers the snapshot it was read under; before serving it, the proxy asks Postgres
-- (pgshadow.validate) whether anything the result depends on could differ now:
--   * a transaction the snapshot could not see has since committed a change to a table;
--   * a table's definition, column types, enum labels, capture trigger, eligibility,
--     privileges, or Timescale chunk set changed;
--   * a table name would now resolve to a different table for that session;
--   * user-defined functions, operators, types, casts or collations changed (catalog
--     epoch), or a built-in function the query uses had its privileges changed;
--   * result-affecting settings changed through a configuration reload;
--   * the server is not the one the result was read from (crash recovery, failover,
--     restore: see pgshadow.boot).
-- If any holds, the result is not used.

CREATE SCHEMA IF NOT EXISTS pgshadow;

CREATE TABLE IF NOT EXISTS pgshadow.changelog (
    rel  oid         NOT NULL,
    xid  xid8        NOT NULL,
    at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (rel, xid)
);
CREATE INDEX IF NOT EXISTS changelog_at ON pgshadow.changelog (at);

-- Recorded snapshot xmins over time (one row per trim run). Snapshot xmin never goes
-- backwards, so any snapshot taken after `at` has an xmin of at least `snap_xmin`.
CREATE TABLE IF NOT EXISTS pgshadow.horizon (
    at         timestamptz PRIMARY KEY,
    snap_xmin  xid8        NOT NULL
);

-- One row. trimmed_through: every changelog entry removed by trimming has a transaction
-- id below this value. A cached result is only valid if its snapshot's xmin is at least
-- this value -- then every removed transaction was already visible to it -- so trimming
-- can never make a stale result look valid.
CREATE TABLE IF NOT EXISTS pgshadow.state (
    id               boolean PRIMARY KEY DEFAULT true CHECK (id),
    trimmed_through  xid8    NOT NULL DEFAULT '0'::xid8,
    schema_version   int     NOT NULL DEFAULT 1
);
INSERT INTO pgshadow.state DEFAULT VALUES ON CONFLICT DO NOTHING;
-- Bumped whenever the proxy depends on a change in these objects.
UPDATE pgshadow.state SET schema_version = 5;

-- One row, in an UNLOGGED table: emptied by crash recovery, and empty on a promoted
-- standby or restored backup. Transaction ids can be reused across those events (the
-- lost tail of a crashed or failed-over primary), so results read before one must never
-- be validated after it: each "boot" gets a new token, part of the settings fingerprint.
CREATE UNLOGGED TABLE IF NOT EXISTS pgshadow.boot (
    id     boolean PRIMARY KEY DEFAULT true CHECK (id),
    token  text    NOT NULL
);

-- Objects of earlier versions.
DROP FUNCTION IF EXISTS pgshadow.prep(text[], text[]);
DROP FUNCTION IF EXISTS pgshadow.snapshot();
DROP FUNCTION IF EXISTS pgshadow.table_state(text[], text[], text[]);
DROP FUNCTION IF EXISTS pgshadow.validate(int[], oid[], text[], text[], text[], text[], text[]);
DROP FUNCTION IF EXISTS pgshadow.validate(int[], oid[], text[], text[], text[], text[], text[], text[], text[], text[]);
DROP FUNCTION IF EXISTS pgshadow.validate(int[], oid[], text[], text[], text[], text[], text[], text[], text[], text[], text[]);
DROP FUNCTION IF EXISTS pgshadow.type_fp(oid);
DROP FUNCTION IF EXISTS pgshadow.fill_context();
REVOKE ALL ON SCHEMA pgshadow FROM PUBLIC;

-- The trigger function. SECURITY DEFINER so application roles need no rights on
-- the pgshadow schema. ON CONFLICT makes it one row per (table, transaction).
CREATE OR REPLACE FUNCTION pgshadow.capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    INSERT INTO pgshadow.changelog (rel, xid) VALUES (TG_RELID, pg_current_xact_id())
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END $$;

-- Timescale hypertables: dropping chunks (retention policies, drop_chunks) deletes rows
-- without firing triggers, so the chunk set is part of a table's fingerprint.
DO $do$
BEGIN
    IF to_regclass('_timescaledb_catalog.chunk') IS NOT NULL THEN
        EXECUTE $f$
        CREATE OR REPLACE FUNCTION pgshadow.hypertable_fp(p_rel oid) RETURNS text
        LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $b$
            SELECT CASE WHEN h.id IS NULL THEN '' ELSE
                   'ht' || h.id || ':' || (SELECT count(*) || ':' || coalesce(max(ch.id), 0)
                                           FROM _timescaledb_catalog.chunk ch
                                           WHERE ch.hypertable_id = h.id AND NOT ch.dropped) END
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN _timescaledb_catalog.hypertable h
                   ON h.schema_name = n.nspname AND h.table_name = c.relname
            WHERE c.oid = p_rel
        $b$;
        $f$;
    ELSE
        CREATE OR REPLACE FUNCTION pgshadow.hypertable_fp(p_rel oid) RETURNS text
        LANGUAGE sql IMMUTABLE AS $b$ SELECT ''::text $b$;
    END IF;
END $do$;

-- Internal helpers below (type_fp, rel_state, effective_schemas, resolve_rel,
-- catalog_epoch, settings_hash) have no SET search_path of their own: they run inside
-- validate(), table_state(), fill_context() and enable(), which pin it, and changing it on
-- every nested call made each check ~20% slower.

-- A column type's definition as it affects output, followed through composite
-- attributes, array elements, domain bases and range subtypes: the type rows, attribute
-- names, and exact enum labels and their order. Built-in scalar types stop the walk.
-- Nesting deeper than 8 levels yields NULL, which makes the table ineligible.
CREATE OR REPLACE FUNCTION pgshadow.type_fp(p_type oid, p_depth int DEFAULT 0) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    t    record;
    out  text;
BEGIN
    IF p_depth > 8 THEN
        RETURN NULL;
    END IF;
    SELECT ty.xmin::text AS x, ty.typtype, ty.typelem, ty.typbasetype, ty.typrelid INTO t
      FROM pg_type ty WHERE ty.oid = p_type;
    IF NOT FOUND THEN
        RETURN '?';
    END IF;
    out := p_type::text || '@' || t.x;
    IF p_type < 16384 AND t.typtype IN ('b', 'p') AND t.typelem = 0 THEN
        RETURN out;
    END IF;
    IF t.typtype = 'e' THEN
        out := out || '[' || coalesce((SELECT string_agg(quote_literal(e.enumlabel), ',' ORDER BY e.enumsortorder)
                                       FROM pg_enum e WHERE e.enumtypid = p_type), '') || ']';
    END IF;
    IF t.typelem <> 0 THEN
        out := out || '<' || pgshadow.type_fp(t.typelem, p_depth + 1) || '>';
    END IF;
    IF t.typbasetype <> 0 THEN
        out := out || '{' || pgshadow.type_fp(t.typbasetype, p_depth + 1) || '}';
    END IF;
    IF t.typrelid <> 0 THEN
        out := out || '(' || coalesce((
            SELECT string_agg(quote_ident(a.attname) || ':' || a.xmin::text || ':' || pgshadow.type_fp(a.atttypid, p_depth + 1),
                              ',' ORDER BY a.attnum)
              FROM pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped), '') || ')';
    END IF;
    IF t.typtype = 'r' THEN
        out := out || '~' || (SELECT pgshadow.type_fp(r.rngsubtype, p_depth + 1) FROM pg_range r WHERE r.rngtypid = p_type);
    ELSIF t.typtype = 'm' THEN
        out := out || '~' || (SELECT pgshadow.type_fp(r.rngtypid, p_depth + 1) FROM pg_range r WHERE r.rngmultitypid = p_type);
    END IF;
    RETURN out;  -- NULL if anything below was too deep
END $$;

-- Everything about a table that can change query results without a data write:
-- columns (add/drop/rename/type), column types and enum labels, relkind, schema,
-- row-level security, inheritance, the capture trigger and whether it is enabled, the
-- table's ACL, and (hypertables) its chunks. DDL rewrites the relevant catalog rows,
-- which changes their xmin.
CREATE OR REPLACE FUNCTION pgshadow.rel_state(p_rel oid, OUT eligible boolean, OUT fp text, OUT reason text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    c        record;
    trig     text;
    trig_on  boolean;
    ht       text;
    cols     text;
BEGIN
    SELECT cl.oid, cl.xmin::text AS x, cl.relkind, cl.relrowsecurity, cl.relhassubclass,
           cl.relispartition, cl.relpersistence
      INTO c FROM pg_class cl WHERE cl.oid = p_rel;
    IF NOT FOUND THEN
        eligible := false; fp := NULL; reason := 'relation not found'; RETURN;
    END IF;
    SELECT string_agg(t.oid::text || ':' || t.tgenabled::text || ':' || t.xmin::text, ',' ORDER BY t.oid),
           bool_and(t.tgenabled = 'A')
      INTO trig, trig_on FROM pg_trigger t WHERE t.tgrelid = p_rel AND t.tgname = 'pgshadow_capture';
    ht := pgshadow.hypertable_fp(p_rel);
    cols := (SELECT string_agg(a.attnum || ':' || a.xmin::text || ':' || coalesce(pgshadow.type_fp(a.atttypid), '!'), ','
                               ORDER BY a.attnum)
               FROM pg_attribute a WHERE a.attrelid = p_rel AND a.attnum > 0);
    fp := concat_ws('|', c.x, c.relkind::text, c.relrowsecurity, c.relhassubclass, c.relispartition, trig, ht, cols,
          (SELECT count(*) FROM pg_inherits i WHERE i.inhrelid = p_rel));
    eligible := false;
    IF c.relkind <> 'r' THEN
        reason := 'not a plain table (relkind ' || c.relkind::text || ')';
    ELSIF c.relpersistence = 't' THEN
        reason := 'temporary table';
    ELSIF c.relpersistence = 'u' THEN
        reason := 'unlogged table (crash recovery empties it without firing triggers)';
    ELSIF position(':!' IN coalesce(cols, '')) > 0 THEN
        reason := 'column types nested too deeply';
    ELSIF c.relrowsecurity THEN
        reason := 'row-level security enabled';
    ELSIF c.relispartition OR EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p_rel) THEN
        -- Statement triggers fire only on the table a statement names: writes through
        -- the parent would never be recorded for this table.
        reason := 'table is a partition or inheritance child (writes through its parent are not captured)';
    ELSIF c.relhassubclass AND ht = '' THEN
        reason := 'table has inheritance children';
    ELSIF EXISTS (SELECT 1 FROM pg_subscription_rel s WHERE s.srrelid = p_rel) THEN
        reason := 'logical replication target (applied changes do not fire statement triggers)';
    ELSIF EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                  WHERE a.attrelid = p_rel AND a.attnum > 0 AND NOT a.attisdropped
                    AND (t.typname LIKE 'reg%' OR t.typname LIKE '\_reg%')
                    AND t.typnamespace = 'pg_catalog'::regnamespace) THEN
        reason := 'has reg* columns (their output depends on catalog names)';
    ELSIF trig IS NULL THEN
        reason := 'not enabled for caching (run pgshadow enable)';
    ELSIF NOT trig_on THEN
        reason := 'capture trigger is disabled';
    ELSE
        eligible := true; reason := NULL;
    END IF;
END $$;

-- Did any transaction that p_snap could not see commit a change to p_rel?
-- Two index lookups: transactions that started after the snapshot, and those that
-- were in progress when it was taken. Also invalid if trimming may have removed
-- an entry the snapshot could not see.
-- (No SET clause and fully qualified names, so the planner can inline it.)
CREATE OR REPLACE FUNCTION pgshadow.changed_since(p_rel oid, p_snap pg_snapshot) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM pgshadow.changelog c
                   WHERE c.rel OPERATOR(pg_catalog.=) p_rel
                     AND c.xid OPERATOR(pg_catalog.>=) pg_catalog.pg_snapshot_xmax(p_snap))
        OR EXISTS (SELECT 1 FROM pgshadow.changelog c
                   WHERE c.rel OPERATOR(pg_catalog.=) p_rel
                     AND c.xid OPERATOR(pg_catalog.=) ANY (ARRAY(SELECT pg_catalog.pg_snapshot_xip(p_snap))))
        OR (SELECT s.trimmed_through FROM pgshadow.state s) OPERATOR(pg_catalog.>) pg_catalog.pg_snapshot_xmin(p_snap)
$$;

-- The schemas a session with this search_path setting and role searches for unqualified
-- relation names, in order: "$user" expanded, schemas that do not exist or that the role
-- has no USAGE on skipped, pg_catalog first unless listed. (Sessions with temporary
-- relations are never cached, so pg_temp is ignored.)
CREATE OR REPLACE FUNCTION pgshadow.effective_schemas(p_path text, p_role text) RETURNS text[]
LANGUAGE plpgsql STABLE AS $$
DECLARE
    item     text;
    out      text[] := '{}';
    catalog  boolean := false;
    r        oid;
BEGIN
    SELECT oid INTO r FROM pg_roles WHERE rolname = p_role;
    IF r IS NULL THEN
        RETURN NULL;
    END IF;
    -- split on commas outside double quotes
    FOR item IN
        SELECT btrim(m[1]) FROM regexp_matches(coalesce(p_path, ''), '((?:"(?:[^"]|"")*"|[^,"])+)', 'g') AS m
    LOOP
        IF item = '' THEN CONTINUE; END IF;
        IF left(item, 1) = '"' THEN
            item := replace(substr(item, 2, length(item) - 2), '""', '"');
        ELSE
            -- Postgres downcases only ASCII letters in unquoted identifiers
            item := translate(item, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz');
        END IF;
        -- ... and truncates identifiers to 63 bytes (on a character boundary)
        WHILE octet_length(item) > 63 LOOP
            item := left(item, -1);
        END LOOP;
        IF item = '$user' THEN item := p_role; END IF;
        IF item = 'pg_temp' OR item LIKE 'pg\_temp\_%' THEN CONTINUE; END IF;
        IF item = 'pg_catalog' THEN catalog := true; END IF;
        IF EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname = item)
           AND has_schema_privilege(r, item, 'USAGE') AND NOT item = ANY (out) THEN
            out := array_append(out, item);
        END IF;
    END LOOP;
    IF NOT catalog THEN
        out := array_prepend('pg_catalog'::text, out);
    END IF;
    RETURN out;
END $$;

-- Resolve an unqualified relation name through a schema list.
CREATE OR REPLACE FUNCTION pgshadow.resolve(p_name text, p_schemas text[]) RETURNS oid
LANGUAGE sql STABLE AS $$
    SELECT c.oid
    FROM pg_catalog.unnest(p_schemas) WITH ORDINALITY AS s(nsp, ord)
    JOIN pg_catalog.pg_namespace n ON n.nspname OPERATOR(pg_catalog.=) s.nsp
    JOIN pg_catalog.pg_class c ON c.relnamespace OPERATOR(pg_catalog.=) n.oid
                               AND c.relname OPERATOR(pg_catalog.=) p_name
    ORDER BY s.ord
    LIMIT 1
$$;

-- Resolve a relation reference the way a session with this search_path and role would.
CREATE OR REPLACE FUNCTION pgshadow.resolve_rel(p_nsp text, p_name text, p_path text, p_role text) RETURNS oid
LANGUAGE plpgsql STABLE AS $$
DECLARE
    r oid;
BEGIN
    IF p_nsp IS NULL THEN
        RETURN pgshadow.resolve(p_name, pgshadow.effective_schemas(p_path, p_role));
    END IF;
    SELECT c.oid INTO r FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = p_nsp AND c.relname = p_name;
    RETURN r;
END $$;

-- Changes to user-defined functions, operators, types, casts and collations anywhere in
-- the database: a hash of every such catalog row's identity and version (xmin). A cached
-- result is only used while this is unchanged, so an object created, replaced, renamed
-- or dropped after it was cached can never change what the same SQL would now return
-- without the result being discarded. (Table row types and array types are left out:
-- table changes are tracked per table, and creating tables must not invalidate.)
CREATE OR REPLACE FUNCTION pgshadow.catalog_epoch() RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    f text; o text; t text; c text; l text;
BEGIN
    SELECT md5(coalesce(string_agg(p.oid::text || ':' || p.xmin::text, ',' ORDER BY p.oid), '')) INTO f
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE p.oid >= 16384 AND n.nspname NOT IN ('pgshadow') AND n.nspname !~ '^pg_(toast_)?temp_';
    SELECT md5(coalesce(string_agg(op.oid::text || ':' || op.xmin::text, ',' ORDER BY op.oid), '')) INTO o
      FROM pg_operator op WHERE op.oid >= 16384;
    SELECT md5(coalesce(string_agg(ty.oid::text || ':' || ty.xmin::text, ',' ORDER BY ty.oid), '')) INTO t
      FROM pg_type ty JOIN pg_namespace n ON n.oid = ty.typnamespace
     WHERE ty.oid >= 16384 AND ty.typrelid = 0 AND ty.typcategory <> 'A'
       AND n.nspname !~ '^pg_(toast_)?temp_';
    SELECT md5(coalesce(string_agg(ca.oid::text || ':' || ca.xmin::text, ',' ORDER BY ca.oid), '')) INTO c
      FROM pg_cast ca WHERE ca.oid >= 16384;
    SELECT md5(coalesce(string_agg(co.oid::text || ':' || co.xmin::text, ',' ORDER BY co.oid), '')) INTO l
      FROM pg_collation co WHERE co.oid >= 16384;
    RETURN md5(concat_ws('|', f, o, t, c, l));
END $$;

-- Settings that change what a query returns, as configured for the server (the proxy's
-- connections do not override any of them), plus this server's boot token. A
-- configuration reload changes them for every session without telling the client; the
-- proxy then re-reads each session's settings before using the cache again.
-- Must be called at the top level of the proxy's queries (inside a function that pins
-- search_path it would see that instead). NULL during recovery (hot standby) and while
-- there is no boot token: then nothing is cached or served.
CREATE OR REPLACE FUNCTION pgshadow.settings_hash() RETURNS text
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF pg_is_in_recovery() OR NOT EXISTS (SELECT 1 FROM pgshadow.boot) THEN
        RETURN NULL;
    END IF;
    RETURN md5(concat_ws(',', (SELECT b.token FROM pgshadow.boot b),
        current_setting('bytea_output'), current_setting('extra_float_digits'),
        current_setting('lc_monetary'), current_setting('lc_numeric'), current_setting('lc_time'),
        current_setting('xmloption'), current_setting('xmlbinary'), current_setting('IntervalStyle'),
        current_setting('DateStyle'), current_setting('TimeZone'), current_setting('timezone_abbreviations'),
        current_setting('standard_conforming_strings'), current_setting('backslash_quote'),
        current_setting('escape_string_warning'), current_setting('default_text_search_config'),
        current_setting('array_nulls'), current_setting('search_path'), current_setting('quote_all_identifiers'),
        current_setting('transform_null_equals'), current_setting('gin_fuzzy_search_limit'),
        current_setting('default_transaction_isolation'), current_setting('client_encoding')));
END $$;

-- This server's boot token, created on first use after install, crash recovery,
-- failover or restore.
CREATE OR REPLACE FUNCTION pgshadow.boot_token() RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    tok text;
BEGIN
    SELECT b.token INTO tok FROM pgshadow.boot b;
    IF tok IS NULL THEN
        INSERT INTO pgshadow.boot (token) VALUES (gen_random_uuid()::text) ON CONFLICT DO NOTHING;
        SELECT b.token INTO tok FROM pgshadow.boot b;
    END IF;
    RETURN tok;
END $$;

-- Whether a date/time cast of any of these column names, over these tables, could read
-- the clock: a column of that name that is not of a date/time type (a text 'now' cast to
-- timestamptz is the current time), or no such column at all (an alias or output name).
CREATE OR REPLACE FUNCTION pgshadow.clock_columns(p_rels oid[], p_cols text[]) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT EXISTS (
        SELECT 1 FROM unnest(p_cols) AS w(col)
        WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                          WHERE a.attrelid = ANY (p_rels) AND a.attname = w.col AND a.attnum > 0 AND NOT a.attisdropped)
           OR EXISTS (SELECT 1 FROM pg_attribute a
                      JOIN pg_type t ON t.oid = a.atttypid
                      LEFT JOIN pg_type b ON b.oid = t.typbasetype
                      WHERE a.attrelid = ANY (p_rels) AND a.attname = w.col AND a.attnum > 0 AND NOT a.attisdropped
                        AND coalesce(b.typcategory, t.typcategory) <> 'D'))
$$;

-- Which of these function, operator, type and collation names might not mean the
-- built-in object for some session, or not be usable like it: a same-named object exists
-- outside pg_catalog that is not an immutable extension member (Postgres prefers an exact
-- argument-type match over pg_catalog's version, so a user function or operator can take
-- over a built-in's name), or a built-in function of that name has non-default
-- privileges. A user-defined cast that is not immutable is reported as '(cast)'.
CREATE OR REPLACE FUNCTION pgshadow.shadowed(p_funcs text[], p_ops text[], p_types text[]) RETURNS text[]
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT coalesce(array_agg(DISTINCT x), '{}') FROM (
        SELECT p.proname::text AS x FROM pg_proc p
        WHERE p.proname = ANY (p_funcs) AND p.pronamespace <> 'pg_catalog'::regnamespace
          AND NOT (p.provolatile = 'i' AND EXISTS (SELECT 1 FROM pg_depend d
                   WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'))
        UNION ALL
        SELECT p.proname::text FROM pg_proc p
        WHERE p.proname = ANY (p_funcs) AND p.pronamespace = 'pg_catalog'::regnamespace AND p.proacl IS NOT NULL
        UNION ALL
        SELECT o.oprname::text FROM pg_operator o JOIN pg_proc p ON p.oid = o.oprcode
        WHERE o.oprname = ANY (p_ops) AND o.oprnamespace <> 'pg_catalog'::regnamespace
          AND NOT (p.provolatile = 'i' AND EXISTS (SELECT 1 FROM pg_depend d
                   WHERE d.classid = 'pg_operator'::regclass AND d.objid = o.oid AND d.deptype = 'e'))
        UNION ALL
        SELECT t.typname::text FROM pg_type t
        WHERE t.typname = ANY (p_types) AND t.typnamespace <> 'pg_catalog'::regnamespace
          AND NOT EXISTS (SELECT 1 FROM pg_depend d
                          WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e')
        UNION ALL
        SELECT co.collname::text FROM pg_collation co
        WHERE co.collname = ANY (p_types) AND co.collnamespace <> 'pg_catalog'::regnamespace
        UNION ALL
        SELECT '(cast)' FROM pg_cast ca JOIN pg_proc p ON p.oid = ca.castfunc
        WHERE ca.oid >= 16384 AND p.provolatile <> 'i'
    ) s
$$;

-- Everything a fill needs besides table state, taken on the proxy's own connection
-- before the client's query is sent. Anything later only makes validation stricter.
-- NULLs during recovery. (The proxy adds settings_hash() at the top level of its query.)
CREATE OR REPLACE FUNCTION pgshadow.fill_context(OUT snap text, OUT epoch text)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF pg_is_in_recovery() THEN
        RETURN;
    END IF;
    snap := pg_current_snapshot()::text;
    epoch := pgshadow.catalog_epoch();
END $$;

-- Table state for the proxy's own connection: how each reference resolves for a session
-- with this search_path and role, and that table's eligibility, fingerprint and name.
CREATE OR REPLACE FUNCTION pgshadow.table_state(p_path text, p_role text, p_nsp text[], p_names text[])
RETURNS TABLE (ord int, rel oid, eligible boolean, fp text, qualified text)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT r.ord::int, c.oid, st.eligible, st.fp,
           quote_ident(n.nspname) || '.' || quote_ident(cl.relname)
    FROM ROWS FROM (unnest(p_nsp), unnest(p_names)) WITH ORDINALITY AS r(nsp, name, ord)
    CROSS JOIN LATERAL (SELECT pgshadow.resolve_rel(r.nsp, r.name, p_path, p_role) AS oid) c
    LEFT JOIN pg_class cl ON cl.oid = c.oid
    LEFT JOIN pg_namespace n ON n.oid = cl.relnamespace
    LEFT JOIN LATERAL pgshadow.rel_state(c.oid) st ON c.oid IS NOT NULL
    ORDER BY r.ord
$$;

-- The batched check. One row per (item, table); returns one row per item.
-- An item is valid only if every one of its tables is unchanged, still eligible, still
-- readable by the role (table SELECT and schema USAGE), every reference still resolves to
-- the same table, the catalog epoch and settings are unchanged, and no built-in function
-- the query calls (p_funcs, comma-separated) has non-default privileges.
-- meta_ok is false when something other than table data changed. settings is the current
-- settings hash (the same on every row).
CREATE OR REPLACE FUNCTION pgshadow.validate(
    p_item int[], p_rel oid[], p_fp text[], p_snap text[], p_role text[],
    p_nsp text[], p_name text[], p_path text[], p_epoch text[], p_settings text[], p_funcs text[],
    p_cur_settings text)
RETURNS TABLE (item int, valid boolean, meta_ok boolean, settings text)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp SET plan_cache_mode = force_generic_plan AS $$
DECLARE
    cur_epoch     text := pgshadow.catalog_epoch();
    cur_settings  text := p_cur_settings;
BEGIN
    -- PL/pgSQL so the plan is cached per session (the proxy's check connections are long-lived).
    RETURN QUERY
    WITH t AS MATERIALIZED (
        SELECT * FROM unnest(p_item, p_rel, p_fp, p_snap, p_role, p_nsp, p_name, p_path, p_epoch, p_settings, p_funcs)
                 AS t(item, rel, fp, snap, role, nsp, name, path, epoch, settings, funcs)
    ),
    fa AS MATERIALIZED (
        SELECT d.funcs, NOT EXISTS (SELECT 1 FROM pg_proc p
                                     WHERE p.proname = ANY (string_to_array(d.funcs, ','))
                                       AND p.pronamespace = 'pg_catalog'::regnamespace
                                       AND p.proacl IS NOT NULL) AS ok
        FROM (SELECT DISTINCT funcs FROM t) d
    ),
    -- Per distinct table / snapshot / reference / role: computed once per batch.
    rs AS MATERIALIZED (
        SELECT d.rel, st.eligible, st.fp
        FROM (SELECT DISTINCT rel FROM t) d CROSS JOIN LATERAL pgshadow.rel_state(d.rel) st
    ),
    ch AS MATERIALIZED (
        SELECT d.rel, d.snap, pgshadow.changed_since(d.rel, d.snap::pg_snapshot) AS changed
        FROM (SELECT DISTINCT rel, snap FROM t) d
    ),
    rv AS MATERIALIZED (
        SELECT d.nsp, d.name, d.path, d.role, pgshadow.resolve_rel(d.nsp, d.name, d.path, d.role) AS rel
        FROM (SELECT DISTINCT nsp, name, path, role FROM t) d
    ),
    pr AS MATERIALIZED (
        SELECT d.role, d.rel,
               (r.oid IS NOT NULL AND has_table_privilege(r.oid, d.rel, 'SELECT')
                AND has_schema_privilege(r.oid, c.relnamespace, 'USAGE')) AS ok
        FROM (SELECT DISTINCT role, rel FROM t) d
        JOIN pg_class c ON c.oid = d.rel
        LEFT JOIN pg_roles r ON r.rolname = d.role
    )
    SELECT t.item,
           bool_and(rs.eligible AND rs.fp = t.fp AND pr.ok AND NOT ch.changed AND rv.rel = t.rel AND fa.ok
                    AND t.epoch = cur_epoch AND t.settings = cur_settings) IS TRUE,
           bool_and(rs.eligible AND rs.fp = t.fp AND pr.ok AND rv.rel = t.rel AND fa.ok
                    AND t.epoch = cur_epoch AND t.settings = cur_settings) IS TRUE,
           cur_settings
    FROM t
    JOIN fa ON fa.funcs = t.funcs
    JOIN rs ON rs.rel = t.rel
    JOIN ch ON ch.rel = t.rel AND ch.snap = t.snap
    LEFT JOIN pr ON pr.role = t.role AND pr.rel = t.rel
    LEFT JOIN rv ON rv.nsp IS NOT DISTINCT FROM t.nsp AND rv.name = t.name
                AND rv.path IS NOT DISTINCT FROM t.path AND rv.role = t.role
    GROUP BY t.item;
END
$$;

-- Opt a table in. Validates eligibility up front so mistakes surface immediately.
CREATE OR REPLACE FUNCTION pgshadow.enable(p_table regclass) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    st record;
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS pgshadow_capture ON %s', p_table);
    EXECUTE format('CREATE TRIGGER pgshadow_capture AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s '
                   'FOR EACH STATEMENT EXECUTE FUNCTION pgshadow.capture()', p_table);
    -- Fire even with session_replication_role = replica (pg_restore, replication tools).
    EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER pgshadow_capture', p_table);
    SELECT * INTO st FROM pgshadow.rel_state(p_table);
    IF NOT st.eligible THEN
        RAISE EXCEPTION 'pgshadow: % cannot be cached: %', p_table, st.reason;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION pgshadow.disable(p_table regclass) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS pgshadow_capture ON %s', p_table);
END $$;

-- Remove changelog entries no cached result can still need. Run periodically by one proxy.
--
-- Results older than p_keep are never used (the proxy's cache TTL is shorter), so the
-- changelog only has to answer for snapshots taken within p_keep. Those snapshots all have
-- an xmin at least as large as the xmin recorded p_keep ago (xmin only moves forward), and
-- every transaction below that xmin is visible to them. So entries below it can go.
-- A long-running transaction holds xmin back, which just delays trimming (like vacuum);
-- it never invalidates cached results.
-- SECURITY DEFINER so a proxy role granted EXECUTE needs no write access of its own.
CREATE OR REPLACE FUNCTION pgshadow.trim(p_keep interval) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    n  bigint := 0;
    h  xid8;
BEGIN
    INSERT INTO pgshadow.horizon (at, snap_xmin)
    VALUES (clock_timestamp(), pg_snapshot_xmin(pg_current_snapshot()))
    ON CONFLICT DO NOTHING;
    SELECT hz.snap_xmin INTO h FROM pgshadow.horizon hz
     WHERE hz.at <= clock_timestamp() - p_keep ORDER BY hz.at DESC LIMIT 1;
    IF h IS NOT NULL THEN
        DELETE FROM pgshadow.changelog WHERE xid < h;
        GET DIAGNOSTICS n = ROW_COUNT;
        UPDATE pgshadow.state SET trimmed_through = greatest(trimmed_through, h);
        DELETE FROM pgshadow.horizon WHERE at < clock_timestamp() - p_keep - interval '1 hour';
    END IF;
    RETURN n;
END $$;

CREATE OR REPLACE VIEW pgshadow.tables AS
SELECT c.oid::regclass AS table_name, st.eligible AS cached, st.reason
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
CROSS JOIN LATERAL pgshadow.rel_state(c.oid) st
WHERE t.tgname = 'pgshadow_capture';

-- Rights for the role pgshadow connects as (check.dsn), if it is not the owner.
CREATE OR REPLACE FUNCTION pgshadow.grant_proxy(p_role name) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    EXECUTE format('GRANT USAGE ON SCHEMA pgshadow TO %I', p_role);
    EXECUTE format('GRANT SELECT ON pgshadow.changelog, pgshadow.state TO %I', p_role);
    EXECUTE format('GRANT SELECT ON pgshadow.boot TO %I', p_role);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgshadow.validate(int[], oid[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text),
                    pgshadow.settings_hash(), pgshadow.table_state(text, text, text[], text[]), pgshadow.fill_context(),
                    pgshadow.shadowed(text[], text[], text[]), pgshadow.clock_columns(oid[], text[]),
                    pgshadow.catalog_epoch(), pgshadow.boot_token() TO %I', p_role);
END $$;

-- Managing what is cached is an admin action.
REVOKE ALL ON FUNCTION pgshadow.enable(regclass), pgshadow.disable(regclass),
                       pgshadow.trim(interval), pgshadow.grant_proxy(name) FROM PUBLIC;
