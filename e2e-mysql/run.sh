#!/usr/bin/env bash
#
# Runs the e2e tests against a throwaway MySQL server.
#
# It locates the MySQL server binaries (PATH, Homebrew, or the Debian/Ubuntu
# /usr/sbin/mysqld layout), initializes a data directory in a temp directory,
# starts mysqld on a private unix socket (no TCP port, so concurrent runs
# cannot collide) with the general query log configured the way mycheck
# requires, exports TXNPROOF_E2E_MYSQL_DSN / TXNPROOF_E2E_MYSQL_LOG, runs
# `go test ./...` in this directory, and tears everything down. No Docker
# required.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

find_mysqld() {
    if command -v mysqld >/dev/null 2>&1; then
        command -v mysqld
        return 0
    fi
    # Fall back to well-known install locations; unmatched globs stay
    # literal and fail the -x test harmlessly. The last match wins, which
    # is the highest version under each layout's lexical ordering.
    candidates=""
    if command -v brew >/dev/null 2>&1; then
        candidates="$candidates $(brew --prefix)/opt/mysql*/bin/mysqld"
    fi
    candidates="$candidates /opt/homebrew/opt/mysql*/bin/mysqld /usr/local/opt/mysql*/bin/mysqld /usr/sbin/mysqld"
    best=""
    for f in $candidates; do
        if [ -x "$f" ]; then
            best="$f"
        fi
    done
    if [ -n "$best" ]; then
        printf '%s\n' "$best"
        return 0
    fi
    return 1
}

# find_tool locates a client tool (mysql, mysqladmin): next to mysqld first
# (Homebrew), then on PATH (Debian/Ubuntu keep mysqld in /usr/sbin but the
# clients in /usr/bin).
find_tool() {
    tool="$1"
    if [ -x "$(dirname "$mysqld")/$tool" ]; then
        printf '%s\n' "$(dirname "$mysqld")/$tool"
        return 0
    fi
    command -v "$tool"
}

if ! mysqld="$(find_mysqld)"; then
    echo "error: MySQL server binary (mysqld) not found on PATH," >&2
    echo "under \$(brew --prefix)/opt/mysql*/bin, or /usr/sbin." >&2
    echo "Install MySQL to run the e2e tests." >&2
    exit 1
fi
mysql="$(find_tool mysql)"
mysqladmin="$(find_tool mysqladmin)"
echo "using MySQL server binary $mysqld ($("$mysqld" --version))"

# The workdir must be short: it holds the unix socket, and socket paths are
# limited to ~100 bytes.
workdir="$(mktemp -d /tmp/txpmy.XXXXXX)"
data="$workdir/data"
sock="$workdir/mysql.sock"
generallog="$workdir/general.log"
errorlog="$workdir/error.log"
pidfile="$workdir/mysqld.pid"

cleanup() {
    "$mysqladmin" --no-defaults --socket="$sock" -u root shutdown >/dev/null 2>&1 || true
    if [ -f "$pidfile" ]; then
        kill "$(cat "$pidfile")" >/dev/null 2>&1 || true
    fi
    rm -rf "$workdir"
}
trap cleanup EXIT

"$mysqld" --no-defaults --initialize-insecure --datadir="$data" \
    --log-error="$errorlog" >/dev/null 2>&1

# General query logging exactly as mycheck documents: every statement of
# every connection is logged to a file with UTC timestamps. Socket-only, no
# TCP.
"$mysqld" --no-defaults \
    --datadir="$data" \
    --socket="$sock" \
    --skip-networking \
    --general-log=ON \
    --general-log-file="$generallog" \
    --log-timestamps=UTC \
    --log-error="$errorlog" \
    --pid-file="$pidfile" &

for i in $(seq 1 120); do
    if "$mysqladmin" --no-defaults --socket="$sock" -u root ping >/dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 120 ]; then
        echo "error: mysqld did not become ready; error log follows:" >&2
        cat "$errorlog" >&2 || true
        exit 1
    fi
    sleep 0.5
done

"$mysql" --no-defaults --socket="$sock" -u root -e "CREATE DATABASE IF NOT EXISTS txnproof_e2e"

export TXNPROOF_E2E_MYSQL_DSN="root@unix($sock)/txnproof_e2e"
export TXNPROOF_E2E_MYSQL_LOG="$generallog"

echo "server ready in $workdir; running e2e tests"
cd "$here"
go test -count=1 "$@" ./...
