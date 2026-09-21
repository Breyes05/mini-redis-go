# Engineering Notes

A collection of the decisions, tradeoffs, and bugs from building this
project — kept in one place as a quick refresher before discussing it
(an interview, a code review, or just picking the project back up later).
For deeper prose on any of this, see [DESIGN.md](DESIGN.md); this doc is
organized for quick recall instead.

## 30-second summary

A Redis-compatible in-memory key-value store built from scratch in Go: a
real RESP protocol parser, a sharded concurrent store with TTL expiry,
crash recovery via an append-only log, and leader-follower replication with
full sync + live streaming. No existing Redis library used anywhere — the
point was to build the pieces, not wire up someone else's.

## Talking points by topic

### Concurrency & the sharded store
*(code: [internal/store/store.go](../internal/store/store.go))*

- **The naive version** is one `map` behind one mutex — correct, but every
  command serializes through that one lock regardless of how many CPU cores
  are available.
- **What's built:** the keyspace is split into 16 shards by `fnv32a(key) %
  16`, each with its own `sync.RWMutex`. Two commands only contend if their
  keys land on the same shard. `RWMutex` also lets concurrent `GET`s on the
  same shard proceed without blocking each other.
- **The tradeoff this creates:** a future atomic multi-key operation (e.g.
  `MSET` with all-or-nothing semantics) gets harder — it'd need to lock
  multiple shards in a consistent order to avoid deadlock. Real Redis
  sidesteps this entirely by being single-threaded for command execution.
  Good answer if asked "what's the downside of sharding here."

### Expiration: lazy + active
*(code: [internal/store/store.go](../internal/store/store.go))*

- **Lazy:** every read checks the key's expiry against `time.Now()` before
  returning it — guarantees no stale read ever leaks out.
- **Active:** a background goroutine sweeps every shard every 100ms and
  deletes anything expired.
- **Why both:** lazy alone never reclaims memory for a key that's set with
  a TTL and never read again. Active alone would still be correct (lazy
  backs it up) but memory would only shrink on the sweep's schedule.
  Together: immediate correctness, bounded memory. This is the same
  two-pronged approach real Redis uses.

### Persistence (AOF)
*(code: [internal/persistence/aof.go](../internal/persistence/aof.go))*

- **What it is:** every mutating command (`SET`/`DEL`/`EXPIRE`) is appended
  to a log file, RESP-encoded — the exact same bytes a client would send
  over the wire. On startup, the file is replayed through the same
  `dispatch` function live connections use, to rebuild state.
- **Ordering decision — mutate, then log, then respond.** By the time a
  client sees `+OK`, the write is already durably appended. The
  alternative order (respond, then log) would let a client believe a write
  succeeded when a crash in that gap could lose it. Good follow-up if
  pushed further: a *true* write-ahead log would log **before** mutating
  memory at all, so the log stays authoritative even about writes that
  never reached memory — that's the natural next hardening step, not yet
  built.
- **fsync tradeoff:** `always` (fsync every write — zero loss, one disk
  sync per command) vs `everysec` (fsync on a 1s timer — bounds loss to
  ~1s, default, matches real Redis's own default for the same reason).
- **Why replay reuses `dispatch` instead of a separate "apply" path:** a
  second implementation of "what does SET do" is a place the two can
  silently drift apart. One code path, one definition, replay just points
  its output at `io.Discard` instead of a socket.

### Replication
*(code: [internal/replication/](../internal/replication/), [internal/server/server.go](../internal/server/server.go))*

- **The model:** a follower sends `SYNC`; the leader takes a snapshot,
  sends it as a batch of `SET` commands, then keeps the connection open and
  streams every subsequent mutating command live.
- **The ordering hazard, and how it's avoided:** a snapshot is a
  point-in-time copy, but writes keep happening while it's being taken. If
  the leader registered the follower for live streaming *after* taking the
  snapshot, a write landing in that gap would be in neither the snapshot
  nor the stream — silently lost. Fix: **register for the stream first,
  snapshot second.** A write that lands in the now-harmless overlap gets
  applied twice (duplicate `SET`/`DEL`), which is a no-op. The design
  explicitly optimizes for "maybe duplicate" over "maybe miss."
- **Read-only followers:** a follower rejects writes from ordinary clients,
  but has to accept them from its own leader. `dispatch(args, w, internal
  bool)` is the single knob: client connections pass `false`; `Apply`
  (used by both AOF replay and the replication stream) passes `true`,
  skipping the read-only check. One code path, one boolean, instead of two
  divergent implementations of "run a command."
- **Emergent property worth mentioning:** because a follower's write
  handlers call `propagate` regardless of the `internal` flag, a follower
  with its own sub-replicas automatically re-broadcasts (and re-logs)
  everything it applies from its leader — chained replication falls out of
  the existing design without code written specifically for it. (Not
  covered by a test — worth saying so if asked, rather than overclaiming.)
- **What's deliberately not built:** partial resync (real Redis's `PSYNC`
  can hand a reconnecting replica just what it missed via a backlog
  buffer; this always full-resyncs), and no `WAIT`-style acknowledgment
  tracking — a slow or dead replica is just dropped (`Hub.Broadcast`)
  rather than applying backpressure to writes.

### LRU eviction under a memory budget
*(code: [internal/store/store.go](../internal/store/store.go))*

- **The constraint that drives the whole design:** an exact LRU needs to
  update a recency structure on every `Get`, which means every `Get` needs
  a write lock — directly undoing the sharded `RWMutex` design built
  earlier specifically so concurrent reads don't block each other. Real
  Redis's `allkeys-lru` sidesteps this the same way: sample a few random
  keys, evict whichever was used longest ago, rather than track exact
  order.
- **The refactor this forced:** entries had to move from being stored as
  plain struct values in the map to `*entry` pointers, so `Get` can update
  a `lastAccess` field via `atomic.Int64.Store` *after releasing the read
  lock* — no lock needed for that update at all, since it's a pointer to a
  stable object rather than a copy sitting in the map.
- **A correctness bug worth being able to explain:** with a large keyspace,
  5 random shard picks (out of 16) reliably land on different real keys.
  With a *small* keyspace, they can just as reliably miss the few
  occupied shards entirely — and naively treating "sampling found nothing"
  as "nothing to evict" would leave the store stuck over budget forever in
  that case. Fixed with a fallback that scans every shard (not just 5)
  when random sampling comes back empty, so eviction only gives up when the
  store is verifiably empty, not when it just got unlucky.
- **Byte accounting is admittedly an estimate** (`len(key)+len(value)` plus
  a flat per-entry overhead guess), not real memory measurement — worth
  saying plainly rather than implying Go exposes exact heap accounting per
  map entry, because it doesn't.
- **Testing something probabilistic:** the "hot keys survive eviction more
  than cold keys" test failed about 1 run in 30 during development — not
  because the mechanism was wrong, but because the test's own eviction
  pressure was so aggressive it sometimes wiped both groups down to zero
  survivors, a tie that a strict `<=` comparison misread as failure. Fixed
  by dialing back the pressure and changing the failure condition to "cold
  clearly beat hot" instead of "hot didn't strictly win" — a good example
  of a flaky test being a test-design bug, not a hint to just add a retry.

### Protocol design (RESP)
*(code: [internal/resp/resp.go](../internal/resp/resp.go))*

- **Why real RESP instead of a custom JSON/HTTP API:** any existing Redis
  client — `redis-cli`, `go-redis`, `redis-py`, raw `nc` — talks to this
  server with zero changes. It also makes the parser more interesting:
  RESP is length-prefixed and binary-safe (a bulk string carries its own
  byte length, so it can contain `\r\n` or arbitrary bytes), not
  newline-delimited — the parser reads exactly the declared number of
  bytes rather than scanning for a delimiter.
- **One encoder, four consumers:** `resp.EncodeCommand` builds the bytes
  for a live reply, an AOF log entry, a replication broadcast, and the
  benchmark tool's requests — the same framing everywhere, encoded once
  per command rather than four separate ad hoc implementations.

### Testing strategy
- **The pyramid:** unit tests per package (protocol parsing, store logic in
  isolation) → integration tests that open a real TCP socket and exchange
  raw RESP bytes → a full leader+follower test with two real server
  instances talking over TCP.
- **`-race` caught two distinct things** (see below) — a genuinely good
  story about why the race detector isn't optional for concurrent code.
- **Testing something asynchronous (replication):** there's no single
  synchronous call that means "the follower is caught up," so tests use a
  small `eventually(t, timeout, cond)` poll helper instead of a fixed
  `time.Sleep` — more reliable and faster than guessing a sleep duration,
  and it's a pattern worth knowing generally for testing async systems.

### Benchmarking
*(code: [cmd/bench](../cmd/bench), methodology: [DESIGN.md](DESIGN.md#benchmarking-methodology))*

- **Built a load generator instead of depending on real Redis's
  `redis-benchmark`** — keeps every number in the README reproducible by
  anyone who clones the repo, with no external install, and it's one more
  real consumer of the shared `resp` package rather than throwaway script
  code.
- **The one number worth being able to explain, not just recite:**
  `fsync=always` measured ~175x slower than `everysec` (732 vs. 128,284
  ops/sec). Why: `AOF.AppendEncoded` holds one mutex across the entire
  `fsync` syscall, so with `always`, every concurrent writer serializes
  through one lock *and* a real disk sync, one at a time. That's the
  direct, now-measured cost of the durability guarantee `always` promises
  — good evidence that the earlier design tradeoff writeup wasn't just
  hand-waving.
- **Be upfront about the methodology's limits if asked:** client and
  server shared one machine's CPU cores over loopback, so the raw ops/sec
  numbers are "this ballpark on this machine," not a portable performance
  claim — but the *relative* comparisons (fsync gap, ~10% replication
  cost) are measured under identical conditions each time, so those
  differences are trustworthy. Saying this proactively reads as rigor, not
  as undermining your own numbers.

## Bugs found & fixed (concrete stories for "tell me about a bug you found")

**1. Replication offsets never converged for a follower joining after the
leader had already processed some writes.**
- *Symptom:* an assertion that leader and follower `REPLOFFSET` would match
  once caught up failed, but only in one particular test scenario (existing
  data before the follower connected).
- *Root cause:* the leader's offset counter was "total bytes broadcast
  since the leader started," but a new follower's counter started at zero
  — two numbers counting from different starting lines, so they'd never
  agree even when genuinely caught up.
- *Fix:* mirrored real Redis's `PSYNC`/`FULLRESYNC` handshake — the leader
  tells the connecting follower what its own offset is *at the moment of
  the snapshot*, and the follower seeds its counter there instead of at
  zero. Implemented as `Hub.RegisterWithOffset`, returning the offset
  atomically with registration (under the same lock), so there's no gap
  where a broadcast could land uncounted on either side.

**2. A data race in the *test* itself, not the production code.**
- *Symptom:* `go test -race` flagged a race between a goroutine calling
  `atomic.AddInt64` on an offset variable and the test's assertion reading
  that same variable directly (`offset == leader.hub.Offset()`).
- *Root cause:* the production code correctly used
  `atomic.LoadInt64`/`atomic.AddInt64` everywhere; the test I wrote to
  *check* that value read the plain variable instead of using
  `atomic.LoadInt64`.
- *Why it's worth mentioning distinctly from bug #1:* it demonstrates being
  able to tell "the design has a bug" apart from "my test harness has a
  bug" — both showed up as the same red X, but they needed different fixes
  and only one of them was actually about replication correctness.

**3. A flaky eviction test — again a test-design bug, not a production one.**
- *Symptom:* `TestMaxMemory_PrefersEvictingLeastRecentlyUsed` failed
  roughly 1 run in 30, always with both "hot" and "cold" survivor counts
  at zero.
- *Root cause:* the test's own eviction pressure was tuned so aggressively
  (forcing out ~2/3 of the keyspace) that it sometimes wiped both groups
  down to zero survivors — a tie — which the assertion (`survivingHot <=
  survivingCold` treated as failure) misread as evidence the LRU mechanism
  wasn't preferring hot keys, when actually both groups had simply been
  annihilated by an overly harsh test setup.
- *Fix:* two changes — reduced the forced eviction to about half the
  keyspace so a real split is visible in the normal case, and changed the
  failure condition to "cold keys clearly did *better* than hot" (the one
  outcome that actually contradicts the mechanism) instead of "hot didn't
  strictly beat cold." Verified with 100 repeated runs post-fix, 0
  failures, versus 1/30 before.
- *Why it's worth mentioning:* the instinct when a test is flaky is often
  "add a retry" or "loosen a sleep." Here the right fix was recognizing
  the *test's* pressure parameters and comparison operator were both
  wrong, not the code under test.

## Anticipated questions & prepared answers

**Q: Walk me through what happens when a client sends `SET foo bar EX 60`.**
A: RESP reader parses the wire bytes into `["SET","foo","bar","EX","60"]`
→ `dispatch` routes to `handleSet` → it hashes `"foo"` to pick 1 of 16
shards, locks just that shard, writes the value with an expiry timestamp →
`propagate` appends the command to the AOF and broadcasts it to any
connected replicas → the client gets `+OK`.

**Q: How do you handle two clients writing to the same key concurrently?**
A: They contend on that key's shard lock — one wins, one waits, no
corruption. Unrelated keys on different shards proceed in parallel.

**Q: What happens if the server crashes mid-write?**
A: Depends on exactly when. If it crashes after the AOF append but before
responding, the client never got confirmation but the write did survive
(safe). If it crashes between mutating memory and appending to the AOF,
that write is lost — a true write-ahead log (append first, mutate after)
would close that gap; that's a known, stated simplification, not an
oversight I'd hide from.

**Q: How would you know if a replica is falling behind?**
A: `REPLOFFSET` on both sides — the difference between the leader's and a
follower's reported offset is a concrete number of bytes of lag, not a
guess.

**Q: How does eviction decide what to remove, and why not exact LRU?**
A: It samples 5 random keys and evicts whichever was read longest ago,
rather than maintaining an exact recency-ordered structure — because
updating exact order on every read would mean every `Get` needs a write
lock, undoing the sharded-`RWMutex` design that lets concurrent reads not
block each other. Same tradeoff real Redis's `allkeys-lru` makes, for the
same reason.

**Q: What's missing that a production version would need?**
A: Partial resync instead of always full-syncing on reconnect; write
acknowledgment/quorum tracking instead of fire-and-forget replication; LRU
eviction under a memory budget; actual throughput benchmarks (planned, not
done yet).

**Q: Why didn't you just use an existing Redis client library or a
database like SQLite under the hood?**
A: The point of the project was to build the mechanisms — protocol
parsing, concurrency control, durability, replication — not to wire up
something that already does them. Using a library would have made the
project look similar but demonstrate nothing about how any of it works.

## Where to point someone who wants to see the code

- Protocol: [internal/resp/resp.go](../internal/resp/resp.go)
- Concurrency + expiry: [internal/store/store.go](../internal/store/store.go)
- Command handling + read-only gating: [internal/server/server.go](../internal/server/server.go)
- Persistence: [internal/persistence/aof.go](../internal/persistence/aof.go)
- Replication: [internal/replication/hub.go](../internal/replication/hub.go), [internal/replication/follower.go](../internal/replication/follower.go)
- The offset-convergence fix specifically: `Hub.RegisterWithOffset` in
  [internal/replication/hub.go](../internal/replication/hub.go) and the
  base-offset read in [internal/replication/follower.go](../internal/replication/follower.go)
