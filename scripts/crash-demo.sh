#!/usr/bin/env bash
# ep03 demo: write keys, kill -9 the server, restart it, and the data is back.
# Then corrupt the WAL's tail by hand and watch replay detect and cut it.
#
#   scripts/crash-demo.sh [fsync-policy]     # always | every-sec (default) | never
#
# Uses bash's /dev/tcp, so no nc needed (works in Git Bash on Windows).
set -euo pipefail

cd "$(dirname "$0")/.."
command -v cargo >/dev/null || export PATH="$HOME/.cargo/bin:$PATH"

fsync="${1:-every-sec}"
addr_host=127.0.0.1
addr_port=6390
wal="$(mktemp -d)/rkv.wal"
cargo build --release -q

start() {
  target/release/rkv --addr "$addr_host:$addr_port" --store sharded \
    --wal "$wal" --fsync "$fsync" 2>&1 | sed -u 's/^/    [rkv] /' &
  until (exec 3<>"/dev/tcp/$addr_host/$addr_port") 2>/dev/null; do sleep 0.1; done
}

# Kill the rkv process itself (not the pipeline), the way kill -9 does.
kill9() {
  if command -v taskkill >/dev/null; then
    taskkill //F //IM rkv.exe >/dev/null
  else
    pkill -9 -x rkv
  fi
  sleep 0.3
}

# Send commands on one connection, print each reply.
rkv() {
  exec 3<>"/dev/tcp/$addr_host/$addr_port"
  for cmd in "$@"; do
    printf '%s\n' "$cmd" >&3
    IFS= read -r reply <&3
    printf '    > %-28s %s\n' "$cmd" "$reply"
  done
  exec 3>&-
}

echo "== 1. start rkv (fsync=$fsync), write some keys"
start
rkv "SET user:1 nader" "SET user:2 sara" "SET counter 41" "SET counter 42" "DEL user:2"
cmds=(); for i in $(seq 1 1000); do cmds+=("SET bulk:$i value-$i"); done
rkv "${cmds[@]}" | tail -1
echo "    WAL size: $(wc -c <"$wal") bytes"

echo
echo "== 2. kill -9 (no shutdown, no flush)"
kill9

echo
echo "== 3. restart: replay the WAL"
start
rkv "GET user:1" "GET user:2" "GET counter" "GET bulk:1" "GET bulk:1000"
kill9

echo
echo "== 4. simulate a crash mid-write: append half a record to the WAL"
printf '\x32\x00\x00\x00\xef\xbe\xad\xde\x01\x01\x01' >>"$wal"
echo "    WAL size: $(wc -c <"$wal") bytes (11 garbage bytes at the end)"
start
rkv "GET counter" "GET bulk:1000"
echo "    WAL size: $(wc -c <"$wal") bytes (torn tail truncated)"
kill9
rm -rf "$(dirname "$wal")"
