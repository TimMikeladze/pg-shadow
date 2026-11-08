#!/usr/bin/env python3
"""Starts and stops pgshadow on a proxy Machine for bench/blog.sh (PROXY_CTL=http://host:7000).
Standard library only. Listens on [::]:PROXY_CTL_PORT (7000); on Fly only the private network reaches it.

  POST /start   body: {"toml": "...", "env": {...}}  stop any running proxy, start a fresh one,
                wait for /readyz on the admin address in the config
  POST /stop    stop it
  GET  /cpu     CPU seconds the running proxy has used
  GET  /log     its log so far
  GET  /rss     its resident memory in KB
"""
import json
import os
import re
import subprocess
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket

BIN = os.environ.get("PGSHADOW_BIN", "/pgshadow/target/release/pgshadow")
DIR = "/tmp/proxy-ctl"
os.makedirs(DIR, exist_ok=True)
proc = None


def stop():
    global proc
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    proc = None


def start(toml, env):
    global proc
    stop()
    with open(f"{DIR}/proxy.toml", "w") as f:
        f.write(toml)
    log = open(f"{DIR}/proxy.log", "w")
    proc = subprocess.Popen([BIN, "-c", f"{DIR}/proxy.toml", "serve"], stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ, **env})
    admin = re.search(r'^\[admin\]\s*\naddr = "([^"]+)"', toml, re.M).group(1)
    for _ in range(200):
        if proc.poll() is not None:
            break
        try:
            urllib.request.urlopen(f"http://{admin}/readyz", timeout=1)
            return True
        except OSError:
            time.sleep(0.1)
    return False


def stat(field):
    if not proc or proc.poll() is not None:
        return 0
    if not os.path.exists(f"/proc/{proc.pid}"):  # macOS, for trying it locally
        out = subprocess.run(["ps", "-o", "time=" if field == "cpu" else "rss=", "-p", str(proc.pid)],
                             capture_output=True, text=True).stdout.strip()
        if field == "rss":
            return int(out or 0)
        secs = 0.0
        for p in out.split(":"):
            secs = secs * 60 + float(p)
        return round(secs, 2)
    if field == "cpu":
        parts = open(f"/proc/{proc.pid}/stat").read().rsplit(")", 1)[1].split()
        return round((int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK"), 2)
    for line in open(f"/proc/{proc.pid}/status"):
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


class H(BaseHTTPRequestHandler):
    def reply(self, code, body):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("content-length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/start":
            ok = start(req["toml"], req.get("env", {}))
            self.reply(200 if ok else 500, "ready\n" if ok else open(f"{DIR}/proxy.log").read()[-4000:])
        elif self.path == "/stop":
            stop()
            self.reply(200, "stopped\n")
        else:
            self.reply(404, "")

    def do_GET(self):
        if self.path == "/log":
            self.reply(200, open(f"{DIR}/proxy.log", "rb").read() if os.path.exists(f"{DIR}/proxy.log") else b"")
        elif self.path in ("/cpu", "/rss"):
            self.reply(200, f"{stat(self.path[1:])}\n")
        else:
            self.reply(404, "")

    def log_message(self, *_):
        pass


class Server(ThreadingHTTPServer):
    address_family = socket.AF_INET6


if __name__ == "__main__":
    Server(("::", int(os.environ.get("PROXY_CTL_PORT", "7000"))), H).serve_forever()
