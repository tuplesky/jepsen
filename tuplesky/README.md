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

`register` uses 2 workers per node per key: run it with `--concurrency 2n`.

Every test also runs a crash check: any `panicked at` in a voter's
`coordd.log` fails the test even if the history is clean.

## Faults

`--nemesis` takes a comma-separated list of `kill`, `pause`, `partition` and
`clock`, or `none`. Jepsen's combined nemesis package drives them.

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

Useful options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--bin-dir` | `../../tuplesky/target/release` | where `coordd`, `coord-harness` and `coord-jepsen` are |
| `--api-port`, `--peer-port` | 7001, 7002 | UDP ports of each voter's two planes |
| `--attempt-ms` | 2000 | how long one attempt waits for an answer |
| `--budget-ms` | 10000 | how long an operation may take before it is `info` |
| `--nemesis-interval` | 30 | seconds between fault operations |
| `--recovery-time` | 60 | seconds to wait after healing, before the final reads |
| `--rate` | 20 | operations per second |

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
