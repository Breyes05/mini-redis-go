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

## LRU eviction under a memory budget

**The constraint that shapes this whole feature.** An *exact* LRU — a
recency-ordered structure (like a doubly-linked list) that gets touched on
every single read — needs a lock on every `Get`, because reading a key and
updating "this was just used" are the same operation. But `Get` currently
only takes a shard's `RWMutex.RLock` (see [concurrency](#concurrency-sharded-locks-over-a-single-global-mutex)),
which is exactly what lets concurrent reads on the same shard proceed
without blocking each other. Making every read also acquire a write lock to
maintain exact recency ordering would silently undo that entire design.
Real Redis's own `allkeys-lru` policy doesn't try to maintain exact
ordering either — it samples a handful of random keys and evicts whichever
one of the sample was accessed longest ago (`maxmemory-samples`, default
5). This project makes the same tradeoff, for the same reason.

**How recency is tracked without a write lock.** Each entry carries a
`lastAccess atomic.Int64` (unix nanoseconds). `Get` reads the entry pointer
under `RLock`, releases the lock, and *then* calls `lastAccess.Store(...)`
— no lock held at all for that update, because it's an atomic write to a
field on an object nobody else needs exclusive access to. This only works
because entries are stored as `*entry` (pointers) rather than plain structs
in the map: a pointer stays valid and safely mutable via atomics even after
the map itself changes around it, whereas updating a field on a struct
*copy* would go nowhere. This was a real refactor this milestone required
— every prior method that read `sh.data[key]` as a value had to change to
work with pointers instead.

**Sampling, concretely.** `evictOneSampled` picks `evictionSamples` (5)
random shards, takes whichever key each shard's (Go-randomized) map
iteration visits first, and deletes the one with the oldest `lastAccess`
among that sample. With a large keyspace spread across 16 shards, 5 random
draws almost always land on 5 different, meaningfully random keys — cheap
and good enough. With a *small* keyspace, though, 5 random shard picks can
easily miss the few shards that actually hold data (e.g., 2 keys spread
across 16 shards — a 5-draw sample has better than even odds of hitting
neither). Left unhandled, that would make eviction incorrectly give up
("nothing to evict") while a key genuinely exists. The fix:
`sampleEveryShard` is a fallback that visits all 16 shards instead of just
5, guaranteeing a hit if the store isn't completely empty — used only when
the fast random path comes back empty, so it doesn't cost anything in the
common case.

**Byte accounting is an estimate, not a measurement.** Each entry's `size`
is `len(key) + len(value) + a constant overhead guess (48 bytes)`. Go
doesn't expose exact per-entry heap accounting (map bucket overhead,
pointer sizes, allocator padding), so this is deliberately approximate —
the point is to make a memory *budget* mean something and behave
consistently, not to match `RSS` byte-for-byte. Worth saying plainly if
asked, rather than implying more precision than exists.

**Why eviction is checked from `Set`, not from a background loop.** Similar
to the active expiry sweep, a background eviction loop was considered —
but eviction only needs to happen in response to something adding bytes, so
checking synchronously right after each `Set` (which is also where
`usedBytes` gets updated) means the budget is enforced immediately rather
than for however long it takes a ticker to notice. The cost: a `Set` that
pushes the store over budget pays for its own eviction inline rather than
returning immediately — a legitimate alternative worth naming if asked
"how would you make this not block the writer."

## What's deliberately not built yet

Nothing from the original roadmap — persistence, replication, benchmarks,
and LRU eviction are all in. If extended further, the natural next
additions would be: partial resync for replication (see above), a real
`OBJECT IDLETIME`-style command to inspect a key's recency directly instead
of only observing eviction behavior indirectly, and eviction *policies*
beyond `allkeys-lru` (e.g. `volatile-lru`, evicting only keys with a TTL
set, or `allkeys-random`).

## Benchmarking methodology

**Why a custom tool ([cmd/bench](../cmd/bench)) instead of real Redis's
`redis-benchmark`.** The obvious choice would be installing real Redis and
pointing its own benchmark tool at this server. That wasn't available in
the environment this was built in, and depending on it would mean the
numbers in the README aren't reproducible by someone who clones this repo
without also installing Redis separately. Writing a small load generator
instead means `go build ./cmd/bench && ./bench ...` reproduces every number
in the README with nothing but this repo — and it's one more consumer of
the shared `resp` package (request encoding and reply decoding are the same
`resp.EncodeCommand`/`resp.Reader.ReadValue` the server, AOF, and
replication code already use), rather than new protocol code written a
fifth time.

**What it measures and how.** Each of `-c` concurrent goroutines opens its
own connection and issues requests **sequentially** — write a command, wait
for the reply, then the next — matching how a single real client actually
behaves (no pipelining). Per-request latency is the wall-clock time between
writing the request and finishing the read of its reply. Percentiles are
computed by sorting all latencies from every worker after the run and
indexing by rank — nearest-rank, not a streaming quantile sketch, which is
fine for reporting to two significant figures and not worth the complexity
of something like t-digest at this scale.

**What the numbers do and don't tell you.** The benchmark client and the
server under test ran on the same machine, competing for the same CPU
cores over a loopback connection. That's a legitimate way to compare
configurations *against each other* (fsync policy A vs. B, replica
attached vs. not — both measured under identical conditions, so the
relative difference is real), but it is **not** a clean measurement of the
server's absolute ceiling the way running the client on a separate
physical machine would be. Take the relative comparisons (the ~175x fsync
gap, the ~10% replication cost) as the trustworthy findings; take the raw
ops/sec numbers as "in this ballpark on this machine," not a portable
performance claim.

**The fsync result is the one worth understanding, not just quoting.**
`fsync=always` measured at 732 ops/sec against `everysec`'s 128,284 — a
~175x gap — because `AOF.AppendEncoded` holds a single mutex for the
duration of the `fsync` syscall itself (see [persistence](#persistence-append-only-file)),
so with `always`, every one of 50 concurrent workers' writes serializes
through one lock *and* a real disk sync, one at a time. That's not a bug —
it's the direct, measurable cost of the durability guarantee `always` is
supposed to provide, and it's exactly why `everysec` is the default both
here and in real Redis.
