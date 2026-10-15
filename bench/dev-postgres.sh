#!/usr/bin/env bash
# Local test Postgres for pgshadow's tests (sandbox helper).
# Port 5433, trust auth for postgres, SCRAM and MD5 users over TCP, TLS enabled.
set -e
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
$P -c "SELECT 1 FROM pg_database WHERE datname='shadow'" | grep -q 1 || $P -c "CREATE DATABASE shadow"
$P -d shadow -c "DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgs_scram') THEN CREATE ROLE pgs_scram LOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgs_md5') THEN CREATE ROLE pgs_md5 LOGIN; END IF;
END \$\$;
SET password_encryption = 'scram-sha-256'; ALTER ROLE pgs_scram PASSWORD 'scram-pass';
SET password_encryption = 'md5'; ALTER ROLE pgs_md5 PASSWORD 'md5-pass';" 2>/dev/null
