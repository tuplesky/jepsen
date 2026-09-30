#!/usr/bin/env bash
# Build SwiftPaxos (the version go.mod pins) and the Jepsen shim into DIR.
#
#   shim/build.sh [DIR]     (default: shim/bin)
#
# Static binaries (no cgo), so the ones built on the control node run on
# the Debian nodes whatever their C library.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${1:-$HERE/bin}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
cd "$HERE"
export CGO_ENABLED=0
go build -o "$OUT/swiftpaxos" github.com/imdea-software/swiftpaxos
go build -o "$OUT/swiftpaxos-jepsen" .
echo "built $OUT/swiftpaxos and $OUT/swiftpaxos-jepsen"
