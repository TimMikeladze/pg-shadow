#!/usr/bin/env bash
# Benchmarks on Fly and Neon: bench/blog.sh and the staleness oracle, run from Fly Machines.
#
#   bench/fly.sh up                    app, Postgres Machine, and a client and a proxy Machine per target
#   bench/fly.sh run fly               Fly Postgres over the private network
#   NEON_URL=postgresql://user:pass@ep-....aws.neon.tech/db bench/fly.sh run neon
#   ONLY=mix STALE_SECS=0 bench/fly.sh run fly   one blog.sh scenario, no oracle or suite
#   bench/fly.sh status                last lines of each run's log
#   bench/fly.sh fetch <dir>           copy each bench Machine's /out to <dir>
#   bench/fly.sh down                  destroy the app with its Machines and volume
#
# Per target, pgbench runs on a client Machine (performance-4x) and pgshadow on its own proxy
# Machine (performance-2x, 2 vCPU), so every request crosses the network as it would in a
# deployment: client -> pgshadow -> Postgres, or client -> Postgres direct. The Postgres Machine
# is performance-8x with 32 GB, the size of the Neon compute's autoscaling cap (8 CU).
# Everything runs in FLY_REGION (sjc). The app name and Postgres password are kept in bench/.fly-bench (gitignored);
# the Neon password is passed to the run over SSH and not stored anywhere.
set -euo pipefail
cd "$(dirname "$0")/.."
STATE=bench/.fly-bench
REGION=${FLY_REGION:-sjc}
[ -f "$STATE" ] && . "$STATE"

mid() { fly machine list -a "$APP" --json | python3 -c "import json,sys; print(next(m['id'] for m in json.load(sys.stdin) if m['name'] == '$1'))"; }
image() { fly machine list -a "$APP" --json | python3 -c "import json,sys; print(next(m['config']['image'] for m in json.load(sys.stdin) if m['name'] == '$1'))"; }
on() { local m=$1; shift; fly ssh console -q -a "$APP" --machine "$(mid "$m")" -C "$*"; }

case ${1:-} in
  up)
    APP=${FLY_APP:-pgshadow-bench-$(python3 -c 'import secrets; print(secrets.token_hex(3))')}
    PW=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
    printf 'APP=%s\nPW=%s\n' "$APP" "$PW" > "$STATE"
    fly apps create "$APP" --org "${FLY_ORG:-personal}"
    fly volumes create pgdata -a "$APP" -r "$REGION" -s 10 -y
    fly machine run postgres:18 -a "$APP" -r "$REGION" -n pg --vm-size performance-8x --vm-memory 32768 \
      -v pgdata:/var/lib/postgresql -e POSTGRES_PASSWORD="$PW" --restart no \
      --file-local /docker-entrypoint-initdb.d/pgshadow.sh=bench/fly-pg-init.sh \
      -- postgres -c shared_buffers=256MB -c max_connections=200 -c shared_preload_libraries=pg_stat_statements
    fly machine run . -a "$APP" -r "$REGION" -n bench-fly --dockerfile bench/Dockerfile.bench \
      --vm-size performance-4x --restart no
    img=$(image bench-fly)
    fly machine run "$img" -a "$APP" -r "$REGION" -n bench-neon --vm-size performance-4x --restart no
    for t in fly neon; do
      fly machine run "$img" -a "$APP" -r "$REGION" -n "proxy-$t" --vm-size performance-2x --restart no \
        -- python3 /pgshadow/bench/proxy-ctl.py
    done
    ;;
  rebuild) # after changing bench files: rebuild the image and move every bench Machine to it
    fly machine update "$(mid bench-fly)" -a "$APP" --dockerfile bench/Dockerfile.bench -y
    img=$(image bench-fly)
    for m in bench-neon proxy-fly proxy-neon; do fly machine update "$(mid $m)" -a "$APP" --image "$img" -y; done
    ;;
  run)
    v=$(git describe --always --dirty 2>/dev/null || echo unknown)
    case ${2:-} in
      fly)
        env="FLY_PG_HOST=$(mid pg).vm.$APP.internal FLY_PG_PASSWORD=$PW" ;;
      neon)
        read -r host user pass db < <(python3 -c 'import os, urllib.parse as u
p = u.urlparse(os.environ["NEON_URL"])
print(p.hostname, u.unquote(p.username), u.unquote(p.password), p.path.lstrip("/") or "neondb")')
        # The pooler endpoint (PgBouncer in transaction mode) can't hold pgshadow's per-session state.
        [[ $host == *-pooler.* ]] && { echo "use the direct endpoint, without -pooler" >&2; exit 2; }
        env="NEON_HOST=$host NEON_USER=$user NEON_PASSWORD=$pass NEON_DB=$db" ;;
      *) echo "run fly|neon" >&2; exit 2 ;;
    esac
    env="$env PROXY_HOST=$(mid "proxy-$2").vm.$APP.internal"
    on "bench-$2" "sh -c 'mkdir -p /out && cd /pgshadow && $env PGSHADOW_VERSION=$v ${STALE_SECS:+STALE_SECS=$STALE_SECS} ${ONLY:+ONLY=$ONLY} setsid nohup bash bench/cloud-run.sh $2 > /out/$2.log 2>&1 < /dev/null &'"
    ;;
  status)
    for t in fly neon; do echo "== $t"; on "bench-$t" "tail -n 4 /out/$t.log" 2>/dev/null || true; done ;;
  fetch)
    dir=${2:?fetch <dir>}; mkdir -p "$dir"
    for t in fly neon; do
      on "bench-$t" "sh -c 'tar czf - -C /out . | base64 -w0'" | tr -d '\r' | base64 -d | tar xzf - -C "$dir"
    done ;;
  down)
    fly apps destroy "$APP" -y && rm -f "$STATE" ;;
  *) sed -n 2,15p "$0"; exit 2 ;;
esac
