"""Real-client tests: psycopg 3 and SQLAlchemy 2 through the pgshadow binary.

    PGSHADOW_BIN=target/debug/pgshadow PGSHADOW_TEST_PG="host=127.0.0.1 port=5433 user=postgres dbname=shadow" pytest py_tests

Every result through the proxy is compared with the same query run directly on Postgres.
"""

import datetime as dt
import decimal
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import uuid

import psycopg
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

PG = os.environ.get("PGSHADOW_TEST_PG", "host=127.0.0.1 port=5433 user=postgres dbname=shadow")
BIN = os.environ.get("PGSHADOW_BIN", "target/debug/pgshadow")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Proxy:
    def __init__(self, extra=""):
        self.port = _free_port()
        self.dir = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self.dir.name, "proxy.log")
        host = dict(kv.split("=") for kv in PG.split())
        cfg = f"""
[listen]
addr = "127.0.0.1:{self.port}"
[upstream]
host = "{host.get('host', '127.0.0.1')}"
port = {host.get('port', '5432')}
tls = "disable"
[check]
dsn = "{PG}"
[cache]
memory = "64MB"
min_query_time = "0ms"
{extra}
[admin]
addr = ""
[log]
format = "json"
level = "warn,pgshadow::query=debug"
summary_interval = "0s"
"""
        path = os.path.join(self.dir.name, "pgshadow.toml")
        with open(path, "w") as f:
            f.write(cfg)
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen([BIN, "-c", path, "serve"], stdout=self.log, stderr=self.log)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with psycopg.connect(self.dsn(), connect_timeout=1) as c:
                    c.execute("SELECT 1")
                break
            except psycopg.OperationalError:
                time.sleep(0.1)
        time.sleep(0.3)  # let check connections come up

    def dsn(self, user="postgres"):
        return f"host=127.0.0.1 port={self.port} user={user} dbname=shadow"

    def url(self):
        return f"postgresql+psycopg://postgres@127.0.0.1:{self.port}/shadow"

    def outcomes(self):
        out = {}
        with open(self.log_path) as f:
            for line in f:
                try:
                    o = json.loads(line).get("outcome")
                except json.JSONDecodeError:
                    continue
                if o:
                    out[o] = out.get(o, 0) + 1
        return out

    def hits(self):
        return self.outcomes().get("hit", 0)

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)
        self.log.close()


@pytest.fixture(scope="module")
def proxy():
    with psycopg.connect(PG, autocommit=True) as c:
        here = os.path.dirname(os.path.abspath(__file__))
        c.execute(open(os.path.join(here, "..", "sql", "schema.sql")).read())
        c.execute("""
            DROP TABLE IF EXISTS py_items, py_users CASCADE;
            CREATE TABLE py_users (id serial PRIMARY KEY, name text NOT NULL, email text UNIQUE);
            CREATE TABLE py_items (
                id bigserial PRIMARY KEY, user_id int REFERENCES py_users(id), title text, price numeric(10,2),
                qty int, created timestamptz DEFAULT '2026-01-01 00:00+00', day date, ext uuid, meta jsonb, tags text[],
                active boolean, ratio double precision);
            INSERT INTO py_users (name, email) SELECT 'user' || g, 'u' || g || '@x.io' FROM generate_series(1, 50) g;
            INSERT INTO py_items (user_id, title, price, qty, day, ext, meta, tags, active, ratio)
            SELECT 1 + g % 50, 'item ' || g, g * 0.5, g % 7, date '2026-01-01' + g % 30,
                   md5(g::text)::uuid, jsonb_build_object('g', g, 'odd', g % 2 = 1), ARRAY['t' || g % 3], g % 2 = 0, g / 3.0
            FROM generate_series(1, 500) g;
            SELECT pgshadow.enable('py_users'); SELECT pgshadow.enable('py_items');
        """)
    p = Proxy()
    yield p
    p.stop()


def direct_rows(sql, params=None):
    with psycopg.connect(PG) as c:
        return c.execute(sql, params).fetchall()


QUERIES = [
    ("SELECT * FROM py_items ORDER BY id", None),
    ("SELECT user_id, count(*), sum(price), avg(ratio), max(created), array_agg(id ORDER BY id) FROM py_items GROUP BY 1 ORDER BY 1", None),
    ("SELECT i.title, u.name FROM py_items i JOIN py_users u ON u.id = i.user_id WHERE i.price > %s ORDER BY i.id", (100,)),
    ("SELECT meta, tags, ext, day FROM py_items WHERE active AND qty = %s ORDER BY id", (3,)),
    ("SELECT count(*) FROM py_items WHERE day BETWEEN %s AND %s", (dt.date(2026, 1, 5), dt.date(2026, 1, 9))),
    ("SELECT * FROM py_items WHERE ext = %s", (uuid.UUID("c4ca4238-a0b9-2382-0dcc-509a6f75849b"),)),
    ("SELECT sum(price) FROM py_items WHERE price > %s", (decimal.Decimal("12.50"),)),
]


@pytest.mark.parametrize("prepare_threshold", [None, 0, 5])
def test_psycopg_results_identical_and_cached(proxy, prepare_threshold):
    before = proxy.hits()
    with psycopg.connect(proxy.dsn(), prepare_threshold=prepare_threshold) as c:
        for _ in range(3):
            for sql, params in QUERIES:
                assert c.execute(sql, params).fetchall() == direct_rows(sql, params), sql
            c.commit()
            time.sleep(0.05)
    assert proxy.hits() > before + len(QUERIES), proxy.outcomes()


def test_psycopg_binary_results(proxy):
    with psycopg.connect(proxy.dsn()) as c, psycopg.connect(PG) as d:
        for sql, params in QUERIES:
            want = d.cursor(binary=True).execute(sql, params).fetchall()
            for _ in range(2):
                assert c.cursor(binary=True).execute(sql, params).fetchall() == want, sql
                c.commit()
                time.sleep(0.05)


def test_writes_are_seen_immediately(proxy):
    sql = "SELECT sum(qty) FROM py_items WHERE user_id = 7"
    with psycopg.connect(proxy.dsn(), autocommit=True) as c, psycopg.connect(PG, autocommit=True) as d:
        for i in range(20):
            got = c.execute(sql).fetchone()
            assert got == d.execute(sql).fetchone()
            if i % 3 == 0:
                d.execute("UPDATE py_items SET qty = qty + 1 WHERE user_id = 7")
            if i % 5 == 0:
                c.execute("UPDATE py_items SET qty = qty + 1 WHERE user_id = 7")


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "py_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    email: Mapped[str | None]


class Item(Base):
    __tablename__ = "py_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.ForeignKey("py_users.id"))
    title: Mapped[str | None]
    price: Mapped[decimal.Decimal | None]
    qty: Mapped[int | None]


def test_sqlalchemy_orm(proxy):
    engine = sa.create_engine(proxy.url(), pool_size=4)
    direct = sa.create_engine("postgresql+psycopg:///?" + "&".join(f"{k}={v}" for k, v in (kv.split("=") for kv in PG.split())))
    before = proxy.hits()
    q = sa.select(Item.user_id, sa.func.sum(Item.price)).where(Item.qty > 2).group_by(Item.user_id).order_by(Item.user_id)
    with Session(direct) as s:
        want = s.execute(q).all()
    for _ in range(5):
        with Session(engine) as s:
            assert s.execute(q).all() == want
            assert s.get(User, 3).name == "user3"
            assert [u.name for u in s.scalars(sa.select(User).where(User.id < 4).order_by(User.id))] == ["user1", "user2", "user3"]
        time.sleep(0.05)
    assert proxy.hits() >= before + 4, proxy.outcomes()

    # Read your own write inside the ORM transaction, then commit and read again.
    with Session(engine) as s:
        u = s.get(User, 5)
        u.name = "renamed"
        s.flush()
        assert s.scalar(sa.select(User.name).where(User.id == 5)) == "renamed"
        s.rollback()
        assert s.scalar(sa.select(User.name).where(User.id == 5)) == "user5"
        s.get(User, 5).name = "renamed"
        s.commit()
    with Session(engine) as s:
        assert s.scalar(sa.select(User.name).where(User.id == 5)) == "renamed"
        s.get(User, 5).name = "user5"
        s.commit()

    # Errors pass through and the pooled connection keeps working.
    with Session(engine) as s:
        s.add(User(name="dup", email="u1@x.io"))
        with pytest.raises(sa.exc.IntegrityError):
            s.commit()
    with Session(engine) as s:
        assert s.scalar(sa.select(sa.func.count()).select_from(User)) == 50


def test_sqlalchemy_isolation_levels(proxy):
    engine = sa.create_engine(proxy.url(), isolation_level="REPEATABLE READ")
    sql = sa.text("SELECT count(*) FROM py_items WHERE qty = 1")
    before = proxy.hits()
    with engine.connect() as c:
        first = c.execute(sql).scalar()
        with psycopg.connect(PG, autocommit=True) as d:
            d.execute("UPDATE py_items SET qty = 1 WHERE id = (SELECT min(id) FROM py_items WHERE qty <> 1)")
        assert c.execute(sql).scalar() == first, "repeatable read keeps its snapshot"
        c.commit()
        assert c.execute(sql).scalar() == first + 1
    assert proxy.hits() == before, "nothing served from cache under REPEATABLE READ"


def test_pipeline_executemany_and_copy(proxy):
    with psycopg.connect(proxy.dsn()) as c:
        c.execute("CREATE TEMP TABLE py_tmp (a int, b text)")
        with c.cursor() as cur:
            cur.executemany("INSERT INTO py_tmp VALUES (%s, %s)", [(i, f"v{i}") for i in range(200)])
        with c.cursor().copy("COPY py_tmp FROM STDIN") as cp:
            for i in range(200, 300):
                cp.write_row((i, f"v{i}"))
        assert c.execute("SELECT count(*), sum(a) FROM py_tmp").fetchone() == (300, sum(range(300)))
        with c.pipeline():
            curs = [c.execute("SELECT %s::int * 2", (i,)) for i in range(50)]
        assert [cur.fetchone()[0] for cur in curs] == [i * 2 for i in range(50)]
        with c.cursor().copy("COPY (SELECT a FROM py_tmp WHERE a < 3 ORDER BY a) TO STDOUT") as cp:
            out = b"".join(bytes(chunk) for chunk in cp)
        assert out == b"0\n1\n2\n"


def test_server_side_cursor_and_listen_notify(proxy):
    with psycopg.connect(proxy.dsn()) as c:
        with c.cursor(name="py_cur") as cur:
            cur.itersize = 37
            cur.execute("SELECT id FROM py_items ORDER BY id")
            assert sum(1 for _ in cur) == 500
    with psycopg.connect(proxy.dsn(), autocommit=True) as listener, psycopg.connect(PG, autocommit=True) as d:
        listener.execute("LISTEN py_channel")
        threading.Timer(0.2, lambda: d.execute("NOTIFY py_channel, 'hello'")).start()
        got = next(listener.notifies(timeout=5), None)
        assert got is not None and got.payload == "hello"


def test_async_psycopg(proxy):
    import asyncio

    async def run():
        async with await psycopg.AsyncConnection.connect(proxy.dsn()) as c:
            rows = []
            for _ in range(3):
                cur = await c.execute("SELECT count(*), max(price) FROM py_items WHERE qty = %s", (4,))
                rows.append(await cur.fetchone())
                await c.commit()
                await asyncio.sleep(0.05)
            return rows

    rows = asyncio.run(run())
    assert rows[0] == rows[1] == rows[2] == tuple(direct_rows("SELECT count(*), max(price) FROM py_items WHERE qty = %s", (4,))[0])


def test_scram_login(proxy):
    with psycopg.connect(f"host=127.0.0.1 port={proxy.port} user=pgs_scram password=scram-pass dbname=shadow") as c:
        assert c.execute("SELECT current_user").fetchone()[0] == "pgs_scram"
