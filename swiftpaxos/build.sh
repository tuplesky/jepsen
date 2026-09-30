#!/usr/bin/env bash
# Build SwiftPaxos and its Jepsen client, swiftpaxos-jepsen, into DIR from
# the agentsky/swiftpaxos fork at a pinned commit: upstream
# (imdea-software/swiftpaxos) plus cmd/swiftpaxos-jepsen.
#
#   build.sh [DIR]     (default: bin, next to this script)
#
# SWIFTPAXOS_REPO and SWIFTPAXOS_REF override the repository and commit.
# Static binaries (no cgo), so the ones built on the control node run on
# the Debian nodes whatever their C library.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=${SWIFTPAXOS_REPO:-https://github.com/agentsky/swiftpaxos}
# agentsky/swiftpaxos#1: upstream 35c6936 plus cmd/swiftpaxos-jepsen.
REF=${SWIFTPAXOS_REF:-24a7be183017a4e23f08aee98c8cfa24de2c6e2f}
OUT=${1:-$HERE/bin}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
SRC=$HERE/.build/swiftpaxos

rm -rf "$SRC" && mkdir -p "$SRC"
git -C "$SRC" init -q
git -C "$SRC" fetch -q --depth 1 "$REPO" "$REF"
git -C "$SRC" checkout -q FETCH_HEAD
cd "$SRC"
export CGO_ENABLED=0
go build -o "$OUT/swiftpaxos" .
go build -o "$OUT/swiftpaxos-jepsen" ./cmd/swiftpaxos-jepsen
echo "built $OUT/swiftpaxos and $OUT/swiftpaxos-jepsen from $REPO@$(git rev-parse --short HEAD)"
