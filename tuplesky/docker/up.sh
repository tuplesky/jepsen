#!/usr/bin/env bash
# Stand up a Jepsen cluster of Debian containers on this host, with this
# host as the control node.
#
#   docker/up.sh [--nodes N] [--dir DIR]
#
# Writes into DIR (default ./docker-cluster):
#   id_ed25519, id_ed25519.pub   the control node's key, authorized on every node
#   nodes                        one node name per line, for --nodes-file
#
# Nodes are containers n1..nN on the Docker network `tuplesky-jepsen`, where
# they reach each other by name. This host reaches them by name through an
# /etc/hosts block this script maintains (sudo when not root), because
# TupleSky's certificates name each voter by the host it was provisioned
# at, and Jepsen and the client shim run here.
#
# The containers share this host's kernel, and so its clock: a clock fault
# would move every node's clock and this host's together. Do not run the
# `clock` nemesis against this cluster. They get NET_ADMIN, which the
# partition and packet faults need, and nothing more.
set -euo pipefail

NODES=5
DIR=docker-cluster
IMAGE=tuplesky-jepsen-node
NETWORK=tuplesky-jepsen
HERE=$(cd "$(dirname "$0")" && pwd)
BUILD_ARGS=()

while [ $# -gt 0 ]; do
  case $1 in
    --nodes) NODES=$2; shift 2 ;;
    --dir) DIR=$2; shift 2 ;;
    # A base image with its own apt configuration, for hosts whose
    # containers reach the Debian mirrors only through a proxy.
    --base) BUILD_ARGS+=(--build-arg "BASE=$2"); shift 2 ;;
    --build-network) BUILD_ARGS+=(--network "$2"); shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done

sudo_() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

mkdir -p "$DIR"
DIR=$(cd "$DIR" && pwd)

echo "building $IMAGE"
# The expansion guard keeps an empty array working under `set -u` on
# bash before 4.4 (macOS).
docker build -q ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} -t "$IMAGE" "$HERE/node" >/dev/null

[ -f "$DIR/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -C jepsen-control -f "$DIR/id_ed25519"

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null

: > "$DIR/nodes"
hosts=""
for i in $(seq 1 "$NODES"); do
  node=n$i
  docker rm -f "$node" >/dev/null 2>&1 || true
  docker run -d --name "$node" --hostname "$node" --network "$NETWORK" \
    --cap-add NET_ADMIN --init "$IMAGE" >/dev/null
  docker cp "$DIR/id_ed25519.pub" "$node:/root/.ssh/authorized_keys"
  docker exec "$node" sh -c 'chown root:root /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys'
  ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$node")
  echo "$node" >> "$DIR/nodes"
  hosts+="$ip $node"$'\n'
done

# Replace this script's block in /etc/hosts.
tmp=$(mktemp)
sed '/# tuplesky-jepsen begin/,/# tuplesky-jepsen end/d' /etc/hosts > "$tmp"
{ echo "# tuplesky-jepsen begin"; printf '%s' "$hosts"; echo "# tuplesky-jepsen end"; } >> "$tmp"
sudo_ cp "$tmp" /etc/hosts
rm -f "$tmp"

for node in $(cat "$DIR/nodes"); do
  for _ in $(seq 1 30); do
    if ssh -q -i "$DIR/id_ed25519" -o BatchMode=yes -o StrictHostKeyChecking=no \
         -o UserKnownHostsFile=/dev/null "root@$node" true; then
      continue 2
    fi
    sleep 1
  done
  echo "no ssh to $node" >&2
  exit 1
done
echo "cluster up: $(paste -sd' ' "$DIR/nodes"); key $DIR/id_ed25519; nodes file $DIR/nodes"
