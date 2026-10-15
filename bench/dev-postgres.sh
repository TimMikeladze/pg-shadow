#!/usr/bin/env bash
# Local test Postgres for pgshadow's tests (sandbox helper).
# Port 5433, trust auth for postgres, SCRAM and MD5 users over TCP, TLS enabled.
set -e
if [ ! -f /var/tmp/pgtest/data/PG_VERSION ]; then
  # No prepared data dir (macOS, CI): run postgres:16 in Docker as container pgshadow-test-pg.
  if ! docker ps --format '{{.Names}}' | grep -qx pgshadow-test-pg; then
    docker rm -f pgshadow-test-pg >/dev/null 2>&1 || true
    docker run -d --name pgshadow-test-pg -p 127.0.0.1:5433:5432 -e POSTGRES_HOST_AUTH_METHOD=trust \
      postgres:16 -c max_connections=300 >/dev/null
    for i in $(seq 1 100); do docker exec pgshadow-test-pg pg_isready -U postgres -q 2>/dev/null && break; sleep 0.2; done
    docker exec pgshadow-test-pg bash -c '
      cd /var/lib/postgresql/data
      { echo "host all pgs_scram all scram-sha-256"; echo "host all pgs_md5 all md5"; cat pg_hba.conf; } > h && mv h pg_hba.conf
      openssl req -new -x509 -days 3650 -nodes -subj "/CN=localhost" -keyout server.key -out server.crt >/dev/null 2>&1
      chown postgres server.key server.crt pg_hba.conf; chmod 600 server.key'
    docker exec -u postgres pgshadow-test-pg psql -qc "ALTER SYSTEM SET ssl = on" -c "SELECT pg_reload_conf()" >/dev/null
    sleep 1
  fi
  P="docker exec -u postgres pgshadow-test-pg psql -qAt"
else
PGBIN=/usr/lib/postgresql/16/bin
DATA=/var/tmp/pgtest/data
if ! grep -q "pgshadow-test" $DATA/pg_hba.conf 2>/dev/null; then
  cp $DATA/pg_hba.conf $DATA/pg_hba.conf.orig 2>/dev/null || true
  { echo "# pgshadow-test"
    echo "host all pgs_scram 127.0.0.1/32 scram-sha-256"
    echo "host all pgs_md5   127.0.0.1/32 md5"
    cat $DATA/pg_hba.conf.orig; } > $DATA/pg_hba.conf
  chown postgres $DATA/pg_hba.conf
fi
if [ ! -f $DATA/server.crt ]; then
  openssl req -new -x509 -days 3650 -nodes -subj "/CN=localhost" -keyout $DATA/server.key -out $DATA/server.crt >/dev/null 2>&1
  chown postgres $DATA/server.key $DATA/server.crt; chmod 600 $DATA/server.key
fi
if ! $PGBIN/pg_isready -h /tmp -p 5433 -q; then
  su postgres -c "$PGBIN/pg_ctl -D $DATA -o '-p 5433 -k /tmp -c listen_addresses=127.0.0.1 -c ssl=on -c max_connections=300' -l /var/tmp/pgtest/log start" >/dev/null
  for i in $(seq 1 50); do $PGBIN/pg_isready -h /tmp -p 5433 -q && break; sleep 0.2; done
else
  su postgres -c "$PGBIN/pg_ctl -D $DATA reload" >/dev/null
fi
P="psql -h /tmp -p 5433 -U postgres -qAt"
fi
$P -c "SELECT 1 FROM pg_database WHERE datname='shadow'" | grep -q 1 || $P -c "CREATE DATABASE shadow"
$P -d shadow -c "DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgs_scram') THEN CREATE ROLE pgs_scram LOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgs_md5') THEN CREATE ROLE pgs_md5 LOGIN; END IF;
END \$\$;
SET password_encryption = 'scram-sha-256'; ALTER ROLE pgs_scram PASSWORD 'scram-pass';
SET password_encryption = 'md5'; ALTER ROLE pgs_md5 PASSWORD 'md5-pass';" 2>/dev/null
