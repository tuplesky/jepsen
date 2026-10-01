# jepsen.tuplesky

Jepsen tests for [TupleSky](https://github.com/tuplesky/tuplesky): every node
runs one voter of a TupleSky domain, and every client drives the domain
through its native API.

Jepsen's etcd test cannot be pointed at TupleSky. TupleSky's etcd face is
Kine, which serves only the four transaction shapes Kubernetes sends (create,
revision-guarded update, revision-guarded delete, compaction): the etcd
test's `register`, `set`, `append`, `wr` and `lock` workloads are all refused
there, and a plain etcd `Put` on an existing key is itself broken at the Kine
commit TupleSky pins. So the clients here run `coord-jepsen`, a small client
process in the TupleSky repository built on its Rust SDK.

## How it fits together

| Piece | Where it runs | What it does |
| --- | --- | --- |
| `coord-harness provision --hosts ...` | control node, once per test | writes a signed genesis, endpoint catalog and one bundle per voter into a run directory |
| `coordd` | every node | one voter, run from its bundle in `/opt/tuplesky/nN` |
| `coord-jepsen` | control node, one per Jepsen process | binds a session on its node's voter and answers JSON lines |

The shim reports each operation as `ok`, `fail` or `info` itself:

* A write is `fail` only when the domain established that it did not happen
  (a compare that did not hold, a planner refusal) or when it was refused
  before it could be submitted. Anything else that is not `ok` is `info`.
* A read that did not complete is `fail`, since it had no effect.
* An answer that did not arrive is first resolved: the shim asks the domain
  about the request's identity, rebinding the same session on a new
  connection if the old one is gone, until it learns the outcome or the
  operation's budget (`--budget-ms`) is spent.

A transaction that writes is optimistic, like the etcd test's: a snapshot
read of every key it touches, then one transaction guarded on each key's
modification revision. If the guard fails, the result is `fail`.

## Workloads

| `--workload` | Checker | Operations |
| --- | --- | --- |
| `append` (default) | Elle list-append, strict serializability | transactions of appends and reads over 3 keys |
| `wr` | Elle rw-register, strict serializability | transactions of writes and reads over 3 keys |
| `register` | Knossos, linearizability per key | read, write, compare-and-set on independent keys |

`register` uses 2 workers per node per key: run it with `--concurrency 2n`
or a multiple of it. Each register gets about `--per-key-limit` operations
(100) before the workers move on to a new one, which keeps Knossos's
search short.

Every test also runs a crash check: any `panicked at` in a voter's
`coordd.log` fails the test even if the history is clean.

Each node also keeps `ss -uanm` snapshots of its voter's API and peer
sockets, every 10 s, in `sockets.log` (`jepsen.tuplesky.sockets`), which
Jepsen collects with `coordd.log`: a send queue that stops draining under a
partition shows there. The SwiftPaxos baseline keeps the same for its TCP
connections.

## Throughput

`--rate 0` takes the throttle off: each worker issues its next operation as
soon as the last completes, so the `ok` rate is what the domain sustains at
`--concurrency`. With `--nemesis none` that is a maximum-throughput test,
still checked like any other run. The register workload compares across
systems best, since its keys do not contend:

```sh
lein run test --nodes-file ~/nodes --workload register --nemesis none \
  --rate 0 --concurrency 10n --time-limit 120
```

## A simulated WAN

`--wan` puts a wide-area network between the nodes, with tc netem on each
node's egress (`jepsen.tuplesky.wan`):

- `--wan regions`: three regions, nodes placed round-robin in `--nodes`
  order (`n1` us-east, `n2` us-west, `n3` eu-west, `n4` us-east, ...). One
  way, us-east to us-west is 33 ms, us-east to eu-west 37 ms, and us-west
  to eu-west 65 ms: about half the round trips between AWS us-east-1,
  us-west-2 and eu-west-1. Nodes in one region are 1 ms apart.
- `--wan 50`: 50 ms one way between every two nodes.

The delays hold for the whole test, final reads included; they are the
network, not a fault. Traffic between a node and the control node, where
the clients run, is not delayed: each client sits next to its node. At
setup the nemesis logs each node's region and the round trip it measures
from the first node to each other, and warns when one is short of the
profile.

The `packet` fault disrupts packets on top of that network, to and from
one node, a minority or every node, for a while: 1% or 5% loss, 50 ms more
delay with 25 ms of jitter (which reorders), 5% reordering, 2%
duplication, 1% corruption, or a 10 Mbit/s cap. Stopping it, and the final
heal, go back to the profile, not to an unshaped network.

Jepsen's own packet nemesis cannot be used under a profile: it gives each
node one netem queue and clears the rest, so a fault would erase the WAN.
This one gives each node a prio qdisc with a netem band per distinct
delay among its peers. The nodes need `tc` (iproute2) and a kernel with
`sch_prio`, `sch_netem` and `cls_u32`.

```sh
lein run test --nodes-file ~/nodes --wan regions --nemesis packet,partition \
  --time-limit 300
```

## Faults

`--nemesis` takes a comma-separated list of `kill`, `pause`, `partition`,
`clock` and `packet` (see above), or `none`. Jepsen's combined nemesis
package drives them, except `packet`.

Kills and pauses run on schedules of their own (`jepsen.tuplesky.nemesis`):
each is a flip-flop, kill then start or pause then resume, staggered by
`--nemesis-interval`, with delays uniform up to twice the interval (newer
Jepsen's `gen/stagger` is exponential, capped at 100 seconds). So a start
follows every kill within twice the interval. The combined package draws both from one mix instead, where a
start waits until the mix draws kill/start again; that left every node down
for two to four minutes in some runs. The namespace depends on Jepsen
alone, so TupleSky's etcd baseline loads it too.

Pauses and resumes signal `coordd` by process name (`pkill`), not with
Jepsen's `grepkill!`. `grepkill!` runs `pgrep -f coordd | xargs kill` in a
`bash -c` that names the pattern, and `pgrep` can list the pipeline's own
`xargs` before it execs, which then stops itself: the pause never returns
and holds the nemesis. The etcd baseline hung this way three times.

## Running it

You need a Jepsen cluster: a control node and 3 or 5 Debian nodes reachable
by SSH as root, whose names resolve on every node and on the control node.
The certificates name each node by the name in `--nodes`, and each voter
dials the others at those names.

1. Build TupleSky for the nodes' platform. If the control node has the same
   platform, one build serves both:

   ```sh
   cd tuplesky
   cargo build --locked --release -p coordd -p coord-harness -p coord-jepsen
   ```

2. Install this repository's Jepsen, which the project depends on:

   ```sh
   cd jepsen/jepsen && lein install
   ```

3. Run a test from this directory:

   ```sh
   lein run test --nodes-file ~/nodes --bin-dir ../../tuplesky/target/release \
     --workload append --nemesis kill,partition --time-limit 300
   lein run test --nodes-file ~/nodes --workload register --concurrency 2n
   lein run test-all --nodes-file ~/nodes --time-limit 120
   ```

### On one machine, with Docker

`docker/up.sh` stands up a cluster of Debian containers `n1`..`nN` on a
Docker network, with this host as the control node. It writes an SSH key
and a nodes file, and an `/etc/hosts` block so this host reaches the nodes
by name (it uses `sudo` when not root). `docker/smoke.sh` then deploys a
domain on the cluster the way the test's DB does, over SSH, and checks that
every voter takes a write. It is a quick check of the deployment before a
Jepsen run.

```sh
docker/up.sh --nodes 5 --dir docker-cluster
docker/smoke.sh --bin-dir ../../tuplesky/target/release --dir docker-cluster
lein run test --nodes-file docker-cluster/nodes \
  --ssh-private-key docker-cluster/id_ed25519 --username root \
  --bin-dir ../../tuplesky/target/release \
  --workload append --nemesis kill,pause,partition --time-limit 300
docker/down.sh --dir docker-cluster
```

The containers share the host's kernel and clock. Leave `clock` out of
`--nemesis` here, since a clock fault would move every node's clock and
the control node's together. The containers get `NET_ADMIN` for
partitions and `--wan` shaping, and nothing more; the host's kernel must
have netem (`sudo modprobe sch_netem sch_prio cls_u32`).

The TupleSky repository's `jepsen` workflow does exactly this on a GitHub
runner.

Useful options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--bin-dir` | `../../tuplesky/target/release` | where `coordd`, `coord-harness` and `coord-jepsen` are |
| `--api-port`, `--peer-port` | 7001, 7002 | UDP ports of each voter's two planes |
| `--attempt-ms` | 2000 | how long one attempt waits for an answer |
| `--budget-ms` | 10000 | how long an operation may take before it is `info` |
| `--nemesis-interval` | 30 | seconds between fault operations |
| `--recovery-time` | 60 | seconds to wait after healing, before the final reads |
| `--rate` | 20 | operations per second; 0 for unthrottled |
| `--per-key-limit` | 100 | operations per register, in the register workload |
| `--wan` | none | `regions`, or one-way milliseconds between every two nodes |

The UDP ports must be open between nodes, and from the control node to every
node's API port.

## Things to expect

* **Slow failure detection.** A voter notices a dead or partitioned peer only
  at the transport's 30-second idle timeout, and a voter restarted within
  that window can rejoin up to 30 seconds late. Expect `info` operations
  and unavailability of that order after each kill or partition. The
  defaults for `--nemesis-interval` and `--recovery-time` allow for it.
* **Clock faults produce refusals, not only anomalies.** A frontend refuses
  a service token issued more than 5 seconds in its future, and the shim
  mints tokens with the control node's clock. A skewed node refuses new
  sessions (`:no-client` failures) until its clock is reset.
* **No membership nemesis.** TupleSky cannot add or remove voters at runtime
  yet.
* **A voter that stops itself stays down** until the kill nemesis's final
  generator restarts every node. A fenced voter stops on purpose; a panic
  is caught by the crash check.

## What it found while it was being written

The shim was first run against a local 3-voter domain with a small stress
driver, not yet with this Jepsen project. No safety anomaly was found in
those histories. Two liveness failures were found, both after repeatedly
killing and restarting voter 1 while the other two stayed up:

* A restarted follower wedged: its command table stayed full, and it refused
  every submission with `Backpressure` for minutes. Requests through its
  frontend never got an answer.
* A restarted voter panicked in `Follower::advance_sync`
  (`crates/coord-consensus/src/follower.rs:943`, `expect("installed")`) on
  two successive restarts. After that, the two surviving voters never
  completed an election: one campaigned for ballot after ballot while the
  other followed it. The domain served nothing.

The TupleSky repository's `docs/operations/jepsen.md` has the details.
