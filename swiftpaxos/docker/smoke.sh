#!/usr/bin/env bash
# Deploy SwiftPaxos on the cluster ../tuplesky/docker/up.sh stood up, the
# way jepsen.swiftpaxos.db does, over SSH, and put a write and a read
# through every replica with swiftpaxos-jepsen.
#
#   docker/smoke.sh --bin-dir DIR [--dir CLUSTER_DIR]
#
# It is the Jepsen test's setup without Jepsen: the master on this host,
# one replica on each node, all reading one configuration file. A write or
# read that is not ok is a deployment problem, and the smoke fails.
set -euo pipefail

DIR=docker-cluster
BIN=
MASTER_PORT=7087
while [ $# -gt 0 ]; do
  case $1 in
    --bin-dir) BIN=$2; shift 2 ;;
    --dir) DIR=$2; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[ -n "$BIN" ] || { echo "--bin-dir is required" >&2; exit 2; }
BIN=$(cd "$BIN" && pwd)
DIR=$(cd "$DIR" && pwd)
KEY=$DIR/id_ed25519
SSH=(ssh -q -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
SCP=(scp -q -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
mapfile -t NODES < "$DIR/nodes"
N=${#NODES[@]}
RUN=$DIR/swiftpaxos-smoke
rm -rf "$RUN" && mkdir -p "$RUN"

# Where the replicas reach this host: the address it reaches them from.
# A UDP socket connected to the node has it as its local address.
MASTER=$(python3 -c 'import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect((sys.argv[1], 7070))
print(s.getsockname()[0])' "${NODES[0]}")

{
  echo "-- Replicas --"
  for node in "${NODES[@]}"; do echo "$node $node"; done
  echo
  echo "-- Master --"
  echo "master $MASTER"
  echo
  echo "masterPort: $MASTER_PORT"
  echo "protocol: swiftpaxos"
  echo "noop: false"
  echo "thrifty: false"
  echo "optread: false"
  echo "leaderless: false"
  echo "fast: true"
  echo
  echo "-- Proxy --"
  for node in "${NODES[@]}"; do echo "server_alias $node"; done
  echo "---"
} > "$RUN/swiftpaxos.conf"

"$BIN/swiftpaxos" -run master -config "$RUN/swiftpaxos.conf" > "$RUN/master.log" 2>&1 &
master=$!
cleanup() {
  for node in "${NODES[@]}"; do
    "${SSH[@]}" "root@$node" "pkill -9 -x swiftpaxos || true; rm -rf /opt/swiftpaxos" || true
  done
  kill "$master" 2>/dev/null || true
}
trap cleanup EXIT
echo "master at $MASTER:$MASTER_PORT"

for node in "${NODES[@]}"; do
  "${SSH[@]}" "root@$node" "pkill -9 -x swiftpaxos || true; rm -rf /opt/swiftpaxos && mkdir -p /opt/swiftpaxos"
  "${SCP[@]}" "$BIN/swiftpaxos" "$RUN/swiftpaxos.conf" "root@$node:/opt/swiftpaxos/"
  "${SSH[@]}" "root@$node" "start-stop-daemon --start --background --no-close --make-pidfile \
      --pidfile /opt/swiftpaxos/replica.pid --chdir /opt/swiftpaxos \
      --exec /opt/swiftpaxos/swiftpaxos -- -run server -config /opt/swiftpaxos/swiftpaxos.conf -alias $node \
      >> /opt/swiftpaxos/replica.log 2>&1"
  echo "started a replica on $node"
done

for node in "${NODES[@]}"; do
  for _ in $(seq 1 120); do
    if "${SSH[@]}" "root@$node" "grep -q 'done connecting to peers' /opt/swiftpaxos/replica.log"; then
      echo "$node: peers connected"
      continue 2
    fi
    sleep 1
  done
  echo "$node never connected to its peers; its log:" >&2
  "${SSH[@]}" "root@$node" "tail -n 30 /opt/swiftpaxos/replica.log" >&2
  exit 1
done

shim() {
  local node=$1 timeout=$2
  timeout 120 "$BIN/swiftpaxos-jepsen" -server "$node:7070" -master "$MASTER" \
    -master-port "$MASTER_PORT" -replicas "$N" -timeout-ms "$timeout" \
    -log "$RUN/shim-$node.log"
}

# The cluster's first command can take seconds.
out=$(echo '{"f":"write","key":-1,"value":0}' | shim "${NODES[0]}" 30000)
echo "warm-up: $(echo "$out" | tr '\n' ' ')"

for i in $(seq 1 "$N"); do
  node=${NODES[$((i-1))]}
  out=$(printf '%s\n' "{\"f\":\"write\",\"key\":$i,\"value\":$i}" "{\"f\":\"read\",\"key\":$i}" \
    | shim "$node" 10000)
  echo "$node: $(echo "$out" | tr '\n' ' ')"
  [ "$(echo "$out" | sed -n 2p)" = "{\"type\":\"ok\",\"value\":$i}" ] || { echo "$node did not take a write" >&2; exit 1; }
  [ "$(echo "$out" | sed -n 3p)" = "{\"type\":\"ok\",\"value\":$i}" ] || { echo "$node did not read it back" >&2; exit 1; }
done
echo "smoke passed: every replica took a write and read it back"
