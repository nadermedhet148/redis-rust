#!/usr/bin/env bash
# ep04 demo: a leader and two replicas, a backup, a partition, a failover.
#
#   1. start a leader and two replicas; writes on the leader show up on both
#   2. replicas are read-only
#   3. replication is asynchronous: a read right after a write can be stale
#   4. back up the leader with rkv-backup and verify the file
#   5. "partition" replica 1 away, keep writing, then kill -9 the leader
#   6. manual failover: promoting the wrong replica would lose acknowledged
#      writes; promote the one with the highest offset, re-point the other
#   7. restore the backup into a fresh server
#
#   scripts/repl-demo.sh
#
# Uses bash's /dev/tcp, so no nc needed (works in Git Bash on Windows).
set -euo pipefail

cd "$(dirname "$0")/.."
command -v cargo >/dev/null || export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release -q

host=127.0.0.1
leader=6391 r1=6392 r2=6393 restored=6394
dir="$(mktemp -d)"
pids=()
declare -A pid
cleanup() {
  for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
  rm -rf "$dir"
}
trap cleanup EXIT

# start <port> [rkv args...]: run rkv in the background, log to $dir/<port>.log
start() {
  local port=$1; shift
  target/release/rkv --addr "$host:$port" --store sharded "$@" >"$dir/$port.log" 2>&1 &
  pids+=($!)
  pid[$port]=$!
  disown # no "Killed" job message when we kill -9 it
  until (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do sleep 0.1; done
}

# rkv <port> <cmd>...: send commands on one connection, print each reply.
rkv() {
  local port=$1; shift
  exec 3<>"/dev/tcp/$host/$port"
  for cmd in "$@"; do
    printf '%s\n' "$cmd" >&3
    IFS= read -r reply <&3
    printf '    [%s] > %-26s %s\n' "$port" "$cmd" "$reply"
  done
  exec 3>&-
}

# ask <port> <cmd>: just the reply.
ask() {
  exec 3<>"/dev/tcp/$host/$1"
  printf '%s\n' "$2" >&3
  IFS= read -r reply <&3
  exec 3>&-
  printf '%s' "$reply"
}

wait_digest() { # wait_digest <port> <digest>
  for _ in $(seq 1 100); do
    [[ "$(ask "$1" DIGEST)" == "$2" ]] && return 0
    sleep 0.05
  done
  echo "    !! $1 never reached digest $2"
  return 1
}

echo "== 1. a leader on :$leader and two replicas"
start $leader
start $r1 --replica-of "$host:$leader"
start $r2 --replica-of "$host:$leader"
rkv $leader "SET user:1 nader" "SET user:2 sara" "SET counter 42" "DEL user:2"
sleep 0.2
rkv $r1 "GET user:1" "GET user:2" "GET counter"
rkv $r2 "GET counter" "DIGEST"
rkv $leader "DIGEST" "ROLE"

echo
echo "== 2. replicas don't take writes"
rkv $r1 "SET user:3 x"

echo
echo "== 3. asynchronous: the leader says OK before replicas have the write"
echo "    2000 times: SET n <i> on the leader, wait for OK, then GET n on replica 1"
exec 3<>"/dev/tcp/$host/$leader" 4<>"/dev/tcp/$host/$r1"
stale=0
for i in $(seq 1 2000); do
  printf 'SET n %d
' "$i" >&3; IFS= read -r _ <&3
  printf 'GET n
' >&4; IFS= read -r v <&4
  [[ "$v" == "$i" ]] || stale=$((stale + 1))
done
exec 3>&- 4>&-
echo "    stale reads on the replica: $stale / 2000"

echo
echo "== 4. back up the leader (rkv-backup connects like a new replica)"
cmds=(); for i in $(seq 1 1000); do cmds+=("SET bulk:$i value-$i"); done
rkv $leader "${cmds[@]}" | tail -1
target/release/rkv-backup snapshot --from "$host:$leader" --out "$dir/backup.rkv" | sed 's/^/    /'
target/release/rkv-backup verify "$dir/backup.rkv" | sed 's/^/    /'
backup_digest="$(ask $leader DIGEST)"

echo
echo "== 5. partition: replica 1 loses the leader (simulated: point it at a dead address)"
sleep 0.2
r1_offset="$(ask $r1 ROLE | grep -o ' offset=[0-9]*' | cut -d= -f2)"
rkv $r1 "REPLICAOF $host 1"
cmds=(); for i in $(seq 1 1000); do cmds+=("SET late:$i v"); done
rkv $leader "${cmds[@]}" | tail -1
acked="$(ask $leader DBSIZE)"
wait_digest $r2 "$(ask $leader DIGEST)"
echo "    the leader acknowledged writes up to $acked keys; now kill -9 it"
kill -9 "${pid[$leader]}"
sleep 0.2
rkv $r1 "DBSIZE"
rkv $r2 "DBSIZE" "ROLE"
r2_offset="$(ask $r2 ROLE | grep -o ' offset=[0-9]*' | cut -d= -f2)"

echo
echo "== 6. failover: which replica to promote?"
echo "    in the old leader's stream, replica 1 stopped at offset $r1_offset, replica 2 reached $r2_offset"
echo "    promoting replica 1 would lose $((acked - $(ask $r1 DBSIZE))) acknowledged writes"
echo "    so promote the highest offset (replica 2) and point replica 1 at it"
rkv $r2 "REPLICAOF NO ONE" "SET after failover"
rkv $r1 "REPLICAOF $host $r2"
wait_digest $r1 "$(ask $r2 DIGEST)"
rkv $r1 "GET after" "GET late:1000" "DBSIZE"
rkv $r2 "ROLE"

echo
echo "== 7. restore the backup from step 4 into a fresh server"
cp "$dir/backup.rkv" "$dir/restored.wal"
start $restored --wal "$dir/restored.wal"
rkv $restored "DBSIZE" "GET bulk:1000" "DIGEST"
echo "    digest when the backup was taken: $backup_digest"
