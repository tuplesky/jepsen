#!/usr/bin/env bash
# Deploy a TupleSky domain on the cluster the way jepsen.tuplesky.db does,
# over SSH, and put one request through every voter with coord-jepsen.
#
#   docker/smoke.sh --bin-dir DIR [--dir CLUSTER_DIR]
#
# It is the Jepsen test's setup without Jepsen: provision on this host with
# `coord-harness provision --hosts`, upload coordd and each voter's bundle,
# `coordd init`, start it with start-stop-daemon from its bundle, wait for
# the mesh, then write through each voter's frontend and read it back.
#
# A write that is not ok is a deployment problem, and a Jepsen run would
# only have reported it as a test that could not start: the smoke fails.
# A read that is not ok is reported and does not fail the smoke. It is the
# domain's behaviour, not the deployment's, and it is the Jepsen test's to
# record: in this cluster one voter's frontend has been seen to hold every
# read `Pending` after the domain's first start (see
# docs/operations/jepsen.md in the TupleSky repository).
set -euo pipefail

DIR=docker-cluster
BIN=
API=7001
PEER=7002
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
RUN=$DIR/smoke-run

hosts=""
for i in $(seq 1 "$N"); do
  hosts+="${hosts:+,}n$i=${NODES[$((i-1))]}:$API:$PEER"
done
rm -rf "$RUN"
"$BIN/coord-harness" provision --dir "$RUN" --hosts "$hosts" >/dev/null
echo "provisioned $hosts"

for i in $(seq 1 "$N"); do
  node=${NODES[$((i-1))]}
  tar czf "$RUN/n$i.tgz" -C "$RUN" "n$i"
  "${SSH[@]}" "root@$node" "pkill -9 coordd || true; rm -rf /opt/tuplesky && mkdir -p /opt/tuplesky"
  "${SCP[@]}" "$BIN/coordd" "$RUN/n$i.tgz" "root@$node:/opt/tuplesky/"
  "${SSH[@]}" "root@$node" "set -e
    cd /opt/tuplesky && tar xzf n$i.tgz && rm n$i.tgz
    cd n$i && /opt/tuplesky/coordd --config coordd.toml init >/dev/null
    start-stop-daemon --start --background --no-close --make-pidfile \
      --pidfile /opt/tuplesky/n$i/coordd.pid --chdir /opt/tuplesky/n$i \
      --exec /opt/tuplesky/coordd -- --config coordd.toml >> /opt/tuplesky/n$i/coordd.log 2>&1"
  echo "started voter $i on $node"
done

last() { "${SSH[@]}" "root@$1" "grep -E '^$2' /opt/tuplesky/n$3/coordd.log | tail -n 1" || true; }
for i in $(seq 1 "$N"); do
  node=${NODES[$((i-1))]}
  for _ in $(seq 1 120); do
    p=$(last "$node" 'peers connected=' "$i")
    v=$(last "$node" 'voters submittable=' "$i")
    if [[ $p == "peers connected=$((N-1)) of"* && $v == "voters submittable=$((N-1)) of"* ]]; then
      echo "voter $i meshed: $p; $v"
      continue 2
    fi
    sleep 1
  done
  echo "voter $i never meshed; its log:" >&2
  "${SSH[@]}" "root@$node" "grep -v '^metrics' /opt/tuplesky/n$i/coordd.log | tail -n 30" >&2
  exit 1
done

for i in $(seq 1 "$N"); do
  out=$(printf '%s\n' \
    "{\"f\":\"write\",\"key\":\"smoke-$i\",\"value\":$i}" \
    "{\"f\":\"read\",\"key\":\"smoke-$i\"}" \
    | timeout 60 "$BIN/coord-jepsen" --dir "$RUN" --voter "$i" --prefix smoke/)
  echo "voter $i: $(echo "$out" | tr '\n' ' ')"
  [ "$(echo "$out" | sed -n 2p)" = "{\"type\":\"ok\",\"value\":$i}" ] || { echo "voter $i did not take a write" >&2; exit 1; }
  [ "$(echo "$out" | sed -n 3p)" = "{\"type\":\"ok\",\"value\":$i}" ] || echo "warning: voter $i did not serve the read back" >&2
done

for i in $(seq 1 "$N"); do
  "${SSH[@]}" "root@${NODES[$((i-1))]}" "pkill -9 coordd || true; rm -rf /opt/tuplesky"
done
echo "smoke passed: every voter took a write"
