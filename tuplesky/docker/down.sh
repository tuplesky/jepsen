#!/usr/bin/env bash
# Remove the cluster docker/up.sh started, and its /etc/hosts block.
#
#   docker/down.sh [--dir DIR]
set -euo pipefail

DIR=docker-cluster
[ "${1:-}" = --dir ] && DIR=$2

sudo_() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

if [ -f "$DIR/nodes" ]; then
  for node in $(cat "$DIR/nodes"); do docker rm -f "$node" >/dev/null 2>&1 || true; done
fi
docker network rm tuplesky-jepsen >/dev/null 2>&1 || true
tmp=$(mktemp)
sed '/# tuplesky-jepsen begin/,/# tuplesky-jepsen end/d' /etc/hosts > "$tmp"
sudo_ cp "$tmp" /etc/hosts
rm -f "$tmp"
echo "cluster down"
