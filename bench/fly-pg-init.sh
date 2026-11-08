#!/bin/bash
# First-start setup for the Postgres Machine bench/fly.sh creates (run by the postgres image
# from /docker-entrypoint-initdb.d). The same test setup as bench/dev-postgres.sh: SCRAM and MD5
# test roles, TLS on, databases shadow (integration tests) and bench (bench/blog.sh).
set -e
cd "$PGDATA"
{
  echo "host all pgs_scram all scram-sha-256"
  echo "host all pgs_md5 all md5"
  echo "host all pgs_path all trust" # a role tests/it/gaps.rs creates without a password
  cat pg_hba.conf
} > h && mv h pg_hba.conf
openssl req -new -x509 -days 30 -nodes -subj "/CN=localhost" -keyout server.key -out server.crt >/dev/null 2>&1
chmod 600 server.key
psql -v ON_ERROR_STOP=1 -U postgres -qAt <<'SQL'
ALTER SYSTEM SET ssl = on;
CREATE DATABASE shadow;
CREATE DATABASE bench;
CREATE ROLE pgs_scram LOGIN;
CREATE ROLE pgs_md5 LOGIN;
SET password_encryption = 'scram-sha-256';
ALTER ROLE pgs_scram PASSWORD 'scram-pass';
SET password_encryption = 'md5';
ALTER ROLE pgs_md5 PASSWORD 'md5-pass';
SQL
