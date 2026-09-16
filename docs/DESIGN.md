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

## What's deliberately not built yet

- **Replication:** single leader, N followers, full sync on connect then a
  streamed command log. The follower's replication offset gives a concrete
  "how far behind is this replica" number to report.
- **LRU eviction:** once a max-memory config exists, evict on write when
  over budget. Real Redis uses *approximated* LRU — sample a handful of
  random keys and evict the oldest of the sample, rather than maintaining an
  exact recency-ordered list — because an exact LRU structure would need a
  global lock on every read (to update recency), which defeats the sharding
  above. Worth implementing the approximation, not the exact version, and
  explaining why.

See the [README roadmap](../README.md#roadmap) for the milestone order.

## Benchmarking plan

Once persistence and replication land, the plan is to measure with
`redis-benchmark` (ships with real Redis) against this server:

- Throughput (ops/sec) for `SET`/`GET` at increasing concurrency
- p50/p99 latency
- Throughput impact of synchronous vs. async replication once a follower is
  attached

Numbers go in the README once measured — no fabricated benchmarks in the
meantime.
