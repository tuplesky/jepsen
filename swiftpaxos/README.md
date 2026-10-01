# jepsen.swiftpaxos

Jepsen tests for [SwiftPaxos](https://github.com/imdea-software/swiftpaxos),
the reference implementation of the protocol (NSDI '24) that TupleSky's
consensus follows. It is a baseline for the TupleSky test in `../tuplesky`:
the same Docker cluster (`../tuplesky/docker/up.sh`), the same fault schedule
(`jepsen.tuplesky.nemesis`, loaded from `../tuplesky/src`), rate and
concurrency.

## How it fits together

| Piece | Where it runs | What it does |
| --- | --- | --- |
| `swiftpaxos -run master` | control node, once per test | assigns replica ids, tells clients the replicas and the leader, replaces a leader it cannot ping |
| `swiftpaxos -run server` | every node | one replica, in memory, from `/opt/swiftpaxos` |
| `swiftpaxos-jepsen` | control node, one per Jepsen process | a session of the upstream SwiftPaxos client, answering JSON lines |

The shim is `cmd/swiftpaxos-jepsen` in
[tuplesky/swiftpaxos](https://github.com/tuplesky/swiftpaxos), a fork of
SwiftPaxos that adds only it. `build.sh [DIR]` builds both binaries from the
fork at a pinned commit (`SWIFTPAXOS_REPO` and `SWIFTPAXOS_REF` override
it), statically, into `bin` by default.

The master runs on the control node, outside the nodes, so no fault reaches
it; the replicas reach it at the address the control node reaches the first
node from (`--master-host` overrides). The partition faults cut replicas from
each other, not from the master or the clients.

The shim speaks the same JSON lines as TupleSky's `coord-jepsen`, and reports
each operation itself: `ok`; `info` for a write that timed out (it may still
take effect); `fail` for a read that timed out. After a timeout it ends its
session, since the upstream client keeps one reply value per session and a
late reply could be taken for the next command's; the Jepsen process ends
with it and the next operation starts a fresh shim.

## Workload

SwiftPaxos's state machine is a map of registers, with reads and writes of
one key per command: no transactions, no compare-and-set. So the one
workload, `register`, is Knossos linearizability over independent registers,
half the workers per key reading and half writing. Run it with
`--concurrency 2n`.

Every test also runs a crash check: a Go panic, or one of the upstream
replica's fatal-exit messages, in a replica's `replica.log` fails the test
even if the history is clean.

## Throughput and a simulated WAN

As in the TupleSky test, `--rate 0` takes the throttle off, so with
`--nemesis none` a run measures the most the cluster sustains at
`--concurrency`; and `--wan regions` or `--wan MILLISECONDS` delays the
traffic between replicas, with `packet` faults on top
(`jepsen.tuplesky.wan`; see the TupleSky test's README). The master, on the
control node, is not delayed.

## Faults

`--nemesis` takes a comma-separated list of `kill`, `pause`, `partition`
and `packet`, or `none`. Use `pause,partition`: SwiftPaxos is an in-memory prototype that
does not recover a replica that stops. A killed replica restarts empty and
cannot rejoin; its new peer connections reach the other replicas' client
listener, which exits on the first peer message ("received unknown client
message"). One kill and restart of one replica has been seen to end every
replica in a three-node cluster this way.

The master pings replicas without a timeout, in a sequential loop, so a
paused **or partitioned** leader is not replaced: the cluster waits for it to
resume or for the partition to heal.

A partition also stalls a replica's sends toward the cut peer within tens of
seconds. Each replica flushes its peers' sockets from one sender under its
global lock, with no write deadline, so once the send buffer toward a cut
peer fills, every send from that replica, acks and client replies alike,
waits on TCP's backed-off retransmit timer. So `ok` counts under a partition
measure TCP's timers, not the protocol. Each node's `ss -tin` snapshots,
every 10 s in `sockets.log` beside `replica.log`, show it: Send-Q and the
retransmit timer on the connections toward the cut peers. The TupleSky test
keeps the same snapshots of its voters' UDP sockets.

Each shim session's client log is kept only with `--shim-logs`; the shims'
stderr goes to `control/shims.err`.

## Running it

```sh
(cd ../jepsen && lein install)
./build.sh
../tuplesky/docker/up.sh --nodes 5 --dir /tmp/cluster
docker/smoke.sh --bin-dir bin --dir /tmp/cluster
lein run test --nodes-file /tmp/cluster/nodes \
  --ssh-private-key /tmp/cluster/id_ed25519 --username root \
  --nemesis pause,partition --time-limit 300 --concurrency 2n
../tuplesky/docker/down.sh --dir /tmp/cluster
```

`docker/smoke.sh` is the test's setup without Jepsen: the master here, a
replica on every node, then a write and a read through each replica.
