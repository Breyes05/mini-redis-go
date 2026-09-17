# Design Notes

This document goes one level deeper than the README on the decisions made
so far and the ones still ahead. It's written the way I'd want to answer
"walk me through a design decision in this project" in an interview.

## Why RESP instead of a custom JSON-over-HTTP API

Speaking real RESP (Redis's actual wire protocol) instead of inventing a
simpler HTTP+JSON API means any existing Redis client — `redis-cli`,
`go-redis`, `redis-py`, even raw `nc` — can talk to this server with zero
changes. That constraint is also what makes the parser interesting: RESP is
a length-prefixed binary-safe protocol (a bulk string carries its own byte
length, so it can contain `\r\n` or arbitrary binary data), not
newline-delimited text, so the parser has to read exactly the number of
bytes a frame declares rather than scanning for a delimiter.

## Concurrency: sharded locks over a single global mutex

**The simple version** is one `map[string]entry` behind one `sync.Mutex`.
It's correct, but every command — even two `GET`s for unrelated keys — waits
on the same lock, so throughput caps out at whatever one lock can push
through regardless of CPU core count.

**What's implemented:** the keyspace is split into 16 shards by
`fnv32a(key) % 16`, each with its own `sync.RWMutex`. Two commands only
contend if their keys hash to the same shard, so throughput scales with
core count instead of flatlining. `RWMutex` specifically lets concurrent
`GET`s on the same shard proceed without blocking each other, only `SET`/
`DEL` need exclusive access.

**Tradeoff:** sharding makes any future "atomic operation across two keys"
(e.g. `MSET` with all-or-nothing semantics, or a `RENAME`) awkward, since it
would need to lock multiple shards in a consistent order to avoid deadlock.
Real Redis sidesteps this by being single-threaded for command execution —
worth mentioning as the alternative design if asked.

## Expiration: lazy + active, not just one

- **Lazy:** every `Get`/`TTL`/`Exists` checks the key's `expiresAt` against
  `time.Now()` before returning it, so a stale read never leaks out.
- **Active:** a background goroutine sweeps every shard every 100ms and
  deletes anything expired.

Lazy alone would never reclaim memory for a key that's set with a TTL and
never read again — it would just sit in the map forever. Active-only would
mean a key could still be logically expired but returned by `Get` in the gap
between sweeps. Running both is what real Redis does, and combining them
is the point: correctness from lazy checks, bounded memory from active
sweeps.

## Persistence: append-only file

**The mechanism.** Every mutating command (`SET`, `DEL`, `EXPIRE` — never
reads) is appended to a log file encoded exactly as it would be sent over
the wire (a RESP array of bulk strings). On startup, before the server
accepts any connections, that file is replayed by feeding each logged
command back through the *same* `dispatch` function a live connection uses,
just pointed at a discard writer instead of a socket. There is deliberately
only one command-parsing/validation code path in the whole project — replay
reuses it rather than reimplementing "apply a SET" a second time, which
would be an easy place for the two to drift out of sync.

**Ordering: mutate → log → respond.** Within each handler, the store is
mutated first, then the command is appended to the AOF, and only then is
the client's reply written. This means that by the time a client sees
`+OK`, the command is already durably appended (subject to the fsync policy
below) — the alternative ordering (respond, then log) would let a client
believe a write succeeded when a crash in that gap could lose it entirely.
The mutate-first-log-second choice does mean a crash between those two
steps loses the write silently either way; a stricter design would log
*before* mutating (a true write-ahead log) so the log is always authoritative
even about writes that never reached memory. That's the natural next
hardening step, deferred here because the store is the single source of
truth for live reads regardless, and getting log-then-respond right already
removes the main correctness gap.

**Fsync policy — a configurable throughput/durability tradeoff.** Appending
to the file's buffer and actually forcing it to disk (`fsync`) are
different costs: an `fsync` is a real disk operation, potentially
milliseconds, versus a buffered write that's essentially free. Two
policies are implemented, both real Redis options:

- `always` — fsync after every single command. Zero data loss on crash, but
  every write now costs a disk sync.
- `everysec` (default) — a background goroutine fsyncs on a 1-second timer;
  writes in between are buffered. Bounds data loss to roughly the last
  second, at negligible per-command cost. This is Redis's own default for
  exactly this reason — most workloads would rather risk ~1s of writes than
  pay a disk sync per command.

**Why replay reuses `dispatch` instead of applying to the store directly.**
It would be a little faster to write a second, storage-only "apply" path
that skips RESP writer setup entirely. That's a real option, but it
introduces a second place where "what does a SET actually do" is defined —
if the two ever disagreed (e.g. someone adds `EX` validation to live `SET`
and forgets the replay path), replayed state could silently diverge from
what clients experienced before the crash. Reusing `dispatch` costs a
throwaway `resp.Writer` around `io.Discard` per replayed command, which is
irrelevant next to disk I/O.

## Replication: leader-follower

**The mechanism.** A follower dials the leader and sends `SYNC`. The leader
registers it as a replica, takes a snapshot of the store, and sends the
snapshot back as a count followed by that many `SET` commands — then keeps
the connection open and streams every subsequent mutating command to it,
indefinitely, as `propagate` (the same function that logs to the AOF) fans
each one out. The follower applies everything it receives — snapshot
entries and streamed commands alike — through `Server.Apply`, the same
function AOF replay uses, so there is still only one definition of "what a
command does" anywhere in the project.

**The ordering bug this design has to avoid.** A snapshot is a
point-in-time copy; live writes keep happening while it's being taken and
sent. If a replica registered for the live stream *after* the snapshot was
captured, any write that landed in that gap would be in neither the
snapshot nor the stream — permanently lost, silently, with no error
anywhere. The fix: **register for the live stream before taking the
snapshot.** Concretely, `Hub.RegisterWithOffset` adds the replica's channel
under the same lock it reads the current offset from, so no `Broadcast` can
happen in between; only after that does `serveReplica` call
`store.Snapshot()`. A write that lands in the (now harmless) gap between
registering and snapshotting shows up in both the snapshot and the live
stream — a duplicate `SET`/`DEL` of the same key/value, which is a no-op the
second time. Duplicates are fine; missing writes are not, so the design
optimizes for that asymmetry.

**Making the replication offset actually mean something.** The leader's
`Hub` counts total bytes broadcast since the *leader* started. A follower
that connects later — after the leader has already broadcast some writes —
would, if its own counter started at zero, never converge with the
leader's number even once fully caught up. Real Redis solves this with its
`PSYNC`/`FULLRESYNC` handshake: the leader tells the connecting replica what
offset its snapshot corresponds to, and the replica's counter starts there
instead of at zero. This project does the same thing in miniature — the
snapshot header is `<baseOffset>\r\n<count>\r\n`, and the follower seeds its
own counter with `baseOffset` before adding anything it streams
afterward. The result: `REPLOFFSET` on a caught-up follower reports exactly
what the leader reports, and the gap between them when it isn't caught up
is a real, comparable measure of replication lag rather than an
apples-to-oranges number.

**Read-only followers, and how a "replicated write" avoids being rejected
by that same rule.** A follower rejects `SET`/`DEL`/`EXPIRE` from ordinary
clients (`READONLY You can't write against a read only replica.`), which
means the one code path that mutates the store (`dispatch`) has to somehow
allow writes that arrive *from the leader* while still rejecting writes
from a normal connection. `dispatch` takes an `internal bool`: client
connections always pass `false`; `Apply` (used by both AOF replay and the
replication stream) always passes `true`, skipping the read-only check.
The result is worth noticing: an ordinary write and a replicated write are
almost the same operation and go through almost the same code, differing
only in that one bit, rather than being two separate, divergent
implementations of "how to run a command."

**A side effect of reusing `propagate` for both AOF and replication: chained
replication basically falls out for free.** Since `Apply` calls `dispatch`,
and `dispatch`'s write handlers always call `propagate` regardless of the
`internal` flag, a follower that itself has sub-replicas connected will
re-broadcast (and, if configured, re-log to its own AOF) everything it
applies from its own leader — without any code written specifically to
support chaining. Untested (no test in this repo exercises three levels
deep), but the mechanism is the same one the two-level case already relies
on and exercises.

**What's deliberately simplified.** No partial resync (`PSYNC` in real
Redis can hand a reconnecting replica just what it missed, using a
replication backlog buffer, instead of a full snapshot every time) — every
reconnect here re-syncs from scratch. No replica acknowledgment tracking on
the leader (real Redis's `WAIT` command blocks until N replicas confirm
they've applied up to some offset); this project's replication is
fire-and-forget from the leader's perspective — a slow or dead replica is
simply dropped (see `Hub.Broadcast`) rather than causing backpressure on
writes.

## What's deliberately not built yet

- **LRU eviction:** once a max-memory config exists, evict on write when
  over budget. Real Redis uses *approximated* LRU — sample a handful of
  random keys and evict the oldest of the sample, rather than maintaining an
  exact recency-ordered list — because an exact LRU structure would need a
  global lock on every read (to update recency), which defeats the sharding
  above. Worth implementing the approximation, not the exact version, and
  explaining why.

See the [README roadmap](../README.md#roadmap) for the milestone order.

## Benchmarking plan

With persistence and replication both in place, the plan is to measure with
`redis-benchmark` (ships with real Redis) against this server:

- Throughput (ops/sec) for `SET`/`GET` at increasing concurrency
- p50/p99 latency
- Throughput impact of synchronous vs. async replication once a follower is
  attached

Numbers go in the README once measured — no fabricated benchmarks in the
meantime.
