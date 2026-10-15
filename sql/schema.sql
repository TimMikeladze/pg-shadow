-- pgshadow: Postgres-side objects. Idempotent; `pgshadow install` runs this.
--
-- How it works: every opted-in table gets one statement-level trigger that records
-- (table, transaction id) in pgshadow.changelog, once per transaction. A cached result
-- remembers the snapshot it was read under; before serving it, the proxy asks Postgres
-- whether any transaction that this snapshot could not see has since committed a change
-- to one of the result's tables (pgshadow.validate). If so, or if anything about the
-- table changed (schema, triggers, privileges, chunks), the result is not used.

CREATE SCHEMA IF NOT EXISTS pgshadow;

CREATE TABLE IF NOT EXISTS pgshadow.changelog (
    rel  oid         NOT NULL,
    xid  xid8        NOT NULL,
    at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (rel, xid)
);
CREATE INDEX IF NOT EXISTS changelog_at ON pgshadow.changelog (at);

-- Recorded snapshot xmins over time (one row per trim run). Snapshot xmin never goes
-- backwards, so any snapshot taken after `at` has an xmin of at least `xmin`.
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
UPDATE pgshadow.state SET schema_version = 1;

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

-- Everything about a table that can change query results without a data write:
-- columns (add/drop/rename/type), relkind, row-level security, inheritance, the capture
-- trigger and whether it is enabled, the table's own ACL, and (hypertables) its chunks.
-- Any DDL rewrites the relevant catalog rows, which changes their xmin.
CREATE OR REPLACE FUNCTION pgshadow.rel_state(p_rel oid, OUT eligible boolean, OUT fp text, OUT reason text)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    c        record;
    trig     text;
    trig_on  boolean;
    ht       text;
BEGIN
    SELECT cl.oid, cl.xmin::text AS x, cl.relkind, cl.relrowsecurity, cl.relhassubclass,
           cl.relpersistence
      INTO c FROM pg_class cl WHERE cl.oid = p_rel;
    IF NOT FOUND THEN
        eligible := false; fp := NULL; reason := 'relation not found'; RETURN;
    END IF;
    SELECT string_agg(t.oid::text || ':' || t.tgenabled::text || ':' || t.xmin::text, ',' ORDER BY t.oid),
           bool_and(t.tgenabled = 'A')
      INTO trig, trig_on FROM pg_trigger t WHERE t.tgrelid = p_rel AND t.tgname = 'pgshadow_capture';
    ht := pgshadow.hypertable_fp(p_rel);
    fp := concat_ws('|', c.x, c.relkind, c.relrowsecurity, c.relhassubclass, trig, ht,
          (SELECT string_agg(a.attnum || ':' || a.xmin::text, ',' ORDER BY a.attnum)
             FROM pg_attribute a WHERE a.attrelid = p_rel AND a.attnum > 0));
    eligible := false;
    IF c.relkind <> 'r' THEN
        reason := 'not a plain table (relkind ' || c.relkind || ')';
    ELSIF c.relpersistence = 't' THEN
        reason := 'temporary table';
    ELSIF c.relrowsecurity THEN
        reason := 'row-level security enabled';
    ELSIF c.relhassubclass AND ht = '' THEN
        reason := 'table has inheritance children';
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

-- Resolve an unqualified relation name the way a session with this schema search
-- list would (current_schemas(true) order).
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

-- The batched check. One row per (item, table); returns one row per item.
-- An item is valid only if every one of its tables is unchanged, still eligible,
-- still readable by the role, and (for unqualified names) still resolves the same way.
CREATE OR REPLACE FUNCTION pgshadow.validate(
    p_item int[], p_rel oid[], p_fp text[], p_snap text[], p_role text[],
    p_name text[], p_schemas text[])
RETURNS TABLE (item int, valid boolean)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- PL/pgSQL so the plan is cached per session (the proxy's check connections are long-lived).
    RETURN QUERY
    WITH t AS MATERIALIZED (
        SELECT * FROM unnest(p_item, p_rel, p_fp, p_snap, p_role, p_name, p_schemas)
                 AS t(item, rel, fp, snap, role, name, schemas)
    ),
    -- Per distinct table, per distinct snapshot, per distinct name: computed once per batch.
    rs AS MATERIALIZED (
        SELECT d.rel, st.eligible, st.fp
        FROM (SELECT DISTINCT rel FROM t) d CROSS JOIN LATERAL pgshadow.rel_state(d.rel) st
    ),
    ch AS MATERIALIZED (
        SELECT d.rel, d.snap, pgshadow.changed_since(d.rel, d.snap::pg_snapshot) AS changed
        FROM (SELECT DISTINCT rel, snap FROM t) d
    ),
    rv AS MATERIALIZED (
        SELECT d.name, d.schemas, pgshadow.resolve(d.name, string_to_array(d.schemas, ',')) AS rel
        FROM (SELECT DISTINCT name, schemas FROM t WHERE name IS NOT NULL) d
    ),
    pr AS MATERIALIZED (
        SELECT d.role, d.rel, (r.oid IS NOT NULL AND has_table_privilege(r.oid, d.rel, 'SELECT')) AS ok
        FROM (SELECT DISTINCT role, rel FROM t) d
        LEFT JOIN pg_roles r ON r.rolname = d.role
        WHERE EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = d.rel)
    )
    SELECT t.item,
           bool_and(rs.eligible AND rs.fp = t.fp AND pr.ok AND NOT ch.changed
                    AND (t.name IS NULL OR rv.rel = t.rel)) IS TRUE
    FROM t
    JOIN rs ON rs.rel = t.rel
    JOIN ch ON ch.rel = t.rel AND ch.snap = t.snap
    LEFT JOIN pr ON pr.role = t.role AND pr.rel = t.rel
    LEFT JOIN rv ON rv.name = t.name AND rv.schemas = t.schemas
    GROUP BY t.item;
END
$$;

-- Called by the proxy on the client's own connection, just before running a query whose
-- result it wants to cache: the snapshot the result will be validated against, and the
-- current state of every relation the query names, resolved exactly as this session
-- resolves names. Runs before the query, so anything it records is never newer than the
-- data the query reads (the conservative direction).
--
-- It must never fail: inside a client transaction an error would abort the client's
-- transaction. Every error, including statement timeouts and cancels, is caught (the
-- exception block uses a subtransaction) and reported as a row with ord = 0.
--
-- No SET search_path: names must resolve with the caller's search path. Everything else
-- is schema-qualified so the caller's search path cannot redirect it.
CREATE OR REPLACE FUNCTION pgshadow.prep(p_schemas text[], p_names text[])
RETURNS TABLE (snap text, ord int, rel oid, eligible boolean, fp text, reason text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    s text;
BEGIN
    BEGIN
        s := pg_catalog.pg_current_snapshot()::text;
        RETURN QUERY
        SELECT s, r.ord::int, c.oid, st.eligible, st.fp, st.reason
        FROM ROWS FROM (pg_catalog.unnest(p_schemas), pg_catalog.unnest(p_names)) WITH ORDINALITY AS r(sch, name, ord)
        CROSS JOIN LATERAL (
            SELECT pg_catalog.to_regclass(
                       CASE WHEN r.sch IS NULL THEN pg_catalog.quote_ident(r.name)
                            ELSE pg_catalog.quote_ident(r.sch) OPERATOR(pg_catalog.||) '.'
                                 OPERATOR(pg_catalog.||) pg_catalog.quote_ident(r.name) END)::oid AS oid
        ) c
        LEFT JOIN LATERAL pgshadow.rel_state(c.oid) st ON c.oid IS NOT NULL
        ORDER BY r.ord;
    EXCEPTION
        WHEN query_canceled THEN
            RETURN QUERY SELECT s, 0, NULL::oid, false, NULL::text, 'canceled'::text;
        WHEN OTHERS THEN
            RETURN QUERY SELECT s, 0, NULL::oid, false, NULL::text, SQLERRM;
    END;
END $$;

-- The prep function and the per-table state run as the application's role.
GRANT USAGE ON SCHEMA pgshadow TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgshadow.prep(text[], text[]), pgshadow.rel_state(oid),
                          pgshadow.hypertable_fp(oid) TO PUBLIC;
-- The changelog itself is only for the proxy's check role (and the trigger).
REVOKE ALL ON pgshadow.changelog, pgshadow.state, pgshadow.horizon FROM PUBLIC;

-- Opt a table in. Validates eligibility up front so mistakes surface immediately.
CREATE OR REPLACE FUNCTION pgshadow.enable(p_table regclass) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    st record;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_subscription_rel WHERE srrelid = p_table) THEN
        RAISE EXCEPTION 'pgshadow: % is a logical replication target; its applied changes do not fire statement triggers', p_table;
    END IF;
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
CREATE OR REPLACE FUNCTION pgshadow.trim(p_keep interval) RETURNS bigint
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
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

-- Managing what is cached is an admin action.
REVOKE ALL ON FUNCTION pgshadow.enable(regclass), pgshadow.disable(regclass),
                       pgshadow.trim(interval) FROM PUBLIC;
