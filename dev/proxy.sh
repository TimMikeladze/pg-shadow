#!/usr/bin/env bash
# dev/proxy.sh start|stop|restart [config] -- run the proxy in the background (sandbox helper)
LOG=${PGSHADOW_LOG:-/tmp/pgshadow-dev.log}; PIDF=/tmp/pgshadow-dev.pid; CFG=${2:-dev/local.toml}
stop() { [ -f $PIDF ] && kill $(cat $PIDF) 2>/dev/null; rm -f $PIDF; sleep 0.3; }
start() { ${PGSHADOW_BIN:-./target/debug/pgshadow} -c "$CFG" serve > "$LOG" 2>&1 & echo $! > $PIDF; for i in $(seq 1 50); do grep -q "check connection ready" "$LOG" 2>/dev/null && break; sleep 0.1; done; }
case "$1" in start) start;; stop) stop;; restart) stop; start;; esac
