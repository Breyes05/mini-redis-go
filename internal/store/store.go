// Package store implements the in-memory keyspace: a sharded map with
// per-key expiration, combining lazy expiry (checked on read) with an
// active background sweep (so expired keys that are never read again don't
// sit in memory forever) — the same two-pronged approach real Redis uses.
// It also enforces an optional memory budget via approximate LRU eviction
// (see evictIfOverBudget).
package store

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// shardCount trades a bit of memory overhead for reduced lock contention:
// keys hash across this many independent shards, so operations on two
// different keys almost never block each other. A single global mutex would
// be simpler but would serialize every command through one lock.
const shardCount = 16

// evictionSamples is how many random keys evictOneSampled looks at before
// evicting the least-recently-used one of the sample. 5 matches real
// Redis's own default (maxmemory-samples).
const evictionSamples = 5

// entryOverhead is a rough per-entry byte estimate for map/struct
// bookkeeping beyond the raw key+value bytes. Go doesn't expose exact
// per-entry heap accounting, so this is an approximation — enough to make
// a memory budget mean something more than "count only the strings," not a
// precise RSS measurement.
const entryOverhead = 48

// entry is always stored as *entry (never copied by value) for two
// reasons: lastAccess is a sync/atomic type, which go vet correctly flags
// if copied; and storing a pointer lets Get update lastAccess after
// releasing the shard's read lock, without needing a write lock just to
// record that a read happened (see Get).
type entry struct {
	value      string
	expiresAt  time.Time    // zero value means "no expiry"
	size       int64        // approximate bytes this entry counts against maxMemory
	lastAccess atomic.Int64 // unix nanoseconds; updated on Get/Set, read by eviction
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

func entrySize(key, value string) int64 {
	return int64(len(key)+len(value)) + entryOverhead
}

type shard struct {
	mu   sync.RWMutex
	data map[string]*entry
}

// Store is a sharded, in-memory, thread-safe key-value store.
type Store struct {
	shards    [shardCount]*shard
	done      chan struct{}
	once      sync.Once
	maxMemory atomic.Int64 // bytes; <= 0 means unlimited (the default)
	usedBytes atomic.Int64 // approximate total bytes currently stored
	evictions atomic.Int64 // count of keys evicted so far, for observability/testing
}

// New creates a Store and starts its background expiry sweep. Call Close
// when done to stop the sweep goroutine.
func New() *Store {
	s := &Store{done: make(chan struct{})}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]*entry)}
	}
	go s.activeExpiryLoop()
	return s
}

// Close stops the background expiry sweep. Safe to call multiple times.
func (s *Store) Close() {
	s.once.Do(func() { close(s.done) })
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()%shardCount]
}

// SetMaxMemory sets an approximate memory budget in bytes. Once exceeded,
// Set evicts approximately-least-recently-used keys (see
// evictIfOverBudget) to make room. A value <= 0 disables the limit, which
// is the default.
func (s *Store) SetMaxMemory(bytes int64) {
	s.maxMemory.Store(bytes)
}

// UsedBytes returns the approximate total bytes currently stored.
func (s *Store) UsedBytes() int64 { return s.usedBytes.Load() }

// Evictions returns how many keys have been evicted for memory pressure
// since the store was created.
func (s *Store) Evictions() int64 { return s.evictions.Load() }

// KeyCount returns the total number of keys currently stored, including any
// not yet reclaimed by the active expiry sweep (matching how real Redis's
// DBSIZE doesn't lazily filter expired keys either).
func (s *Store) KeyCount() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// Set stores key=value. A ttl of 0 means the key never expires.
func (s *Store) Set(key, value string, ttl time.Duration) {
	size := entrySize(key, value)
	e := &entry{value: value, size: size}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	e.lastAccess.Store(time.Now().UnixNano())

	sh := s.shardFor(key)
	sh.mu.Lock()
	old, existed := sh.data[key]
	sh.data[key] = e
	sh.mu.Unlock()

	if existed {
		s.usedBytes.Add(size - old.size)
	} else {
		s.usedBytes.Add(size)
	}

	s.evictIfOverBudget()
}

// Get returns the value for key and whether it was found (and not expired).
// A successful read marks the key as just-used for eviction purposes.
func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()
	if !ok || e.expired(time.Now()) {
		return "", false
	}
	// Safe without holding any lock: lastAccess is only ever touched via
	// atomic ops, and e is a stable pointer even if this key is deleted or
	// overwritten in the map concurrently — we'd just be updating a field
	// on an entry nobody looks at anymore.
	e.lastAccess.Store(time.Now().UnixNano())
	return e.value, true
}

// Del removes key, returning whether it existed.
func (s *Store) Del(key string) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	e, ok := sh.data[key]
	if ok {
		delete(sh.data, key)
	}
	sh.mu.Unlock()
	if ok {
		s.usedBytes.Add(-e.size)
	}
	return ok
}

// Exists reports whether key is present and not expired.
func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// TTL returns the remaining time-to-live for key. ok is false if the key
// doesn't exist (or has already expired); remaining is -1 if the key exists
// but carries no expiry. Unlike Get, this doesn't count as a "use" for
// eviction purposes — it's pure introspection, not a read of the value.
func (s *Store) TTL(key string) (remaining time.Duration, ok bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, exists := sh.data[key]
	sh.mu.RUnlock()
	if !exists || e.expired(time.Now()) {
		return 0, false
	}
	if e.expiresAt.IsZero() {
		return -1, true
	}
	return time.Until(e.expiresAt), true
}

// SnapshotEntry is one key's state as captured by Snapshot.
type SnapshotEntry struct {
	Key   string
	Value string
	TTL   time.Duration // remaining TTL; <= 0 means no expiry
}

// Snapshot returns every live (non-expired) key — used for replication's
// full sync when a follower first connects. It locks one shard at a time
// rather than the whole store for the duration of the copy, so a snapshot
// in progress doesn't stall unrelated reads/writes on other shards the way
// a single global lock would. (Real Redis instead forks a child process to
// write an RDB snapshot from a copy-on-write view of memory — cheaper still,
// but out of scope for this project.)
func (s *Store) Snapshot() []SnapshotEntry {
	now := time.Now()
	var out []SnapshotEntry
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			var ttl time.Duration
			if !e.expiresAt.IsZero() {
				ttl = e.expiresAt.Sub(now)
			}
			out = append(out, SnapshotEntry{Key: k, Value: e.value, TTL: ttl})
		}
		sh.mu.RUnlock()
	}
	return out
}

// activeExpiryLoop periodically sweeps every shard for expired keys.
// Without this, a key that's set with a TTL and never read again would
// linger in memory indefinitely — lazy expiry alone only reclaims a key at
// the moment something tries to read it.
func (s *Store) activeExpiryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			for _, sh := range s.shards {
				sh.mu.Lock()
				for k, e := range sh.data {
					if e.expired(now) {
						delete(sh.data, k)
						s.usedBytes.Add(-e.size)
					}
				}
				sh.mu.Unlock()
			}
		}
	}
}

// sampledKey is one candidate gathered for eviction consideration.
type sampledKey struct {
	shard *shard
	key   string
	entry *entry
}

// evictIfOverBudget evicts approximately-least-recently-used keys until the
// store is back under its configured memory budget. A no-op if no budget is
// set. The loop is capped so a bug that failed to reduce usedBytes couldn't
// spin forever.
func (s *Store) evictIfOverBudget() {
	budget := s.maxMemory.Load()
	if budget <= 0 {
		return
	}
	for i := 0; i < 10000 && s.usedBytes.Load() > budget; i++ {
		if !s.evictOneSampled() {
			return // nothing left to evict
		}
	}
}

// evictOneSampled implements approximate LRU: rather than maintaining an
// exact recency-ordered structure — which would need a lock on every read
// just to update it, undoing the whole point of sharding the store (see
// Get) — it samples a handful of random keys and evicts whichever one of
// the sample was accessed longest ago. This is exactly the tradeoff real
// Redis's own maxmemory-policy allkeys-lru makes, and for the same reason.
//
// Go's per-call-randomized map iteration order stands in for the random
// sampling primitive Redis's own dict implementation provides.
func (s *Store) evictOneSampled() bool {
	samples := s.sampleRandom(evictionSamples)
	if len(samples) == 0 {
		// Random sampling got unlucky and hit only empty shards — most
		// likely with a very small total key count, where a handful of
		// random draws across shardCount shards can easily miss the few
		// that are occupied. Fall back to a full scan so eviction never
		// gives up while a key genuinely exists to evict.
		samples = s.sampleEveryShard()
	}
	if len(samples) == 0 {
		return false
	}

	oldest := samples[0]
	for _, c := range samples[1:] {
		if c.entry.lastAccess.Load() < oldest.entry.lastAccess.Load() {
			oldest = c
		}
	}

	oldest.shard.mu.Lock()
	// Re-check under the write lock: this exact entry may already have
	// been deleted or replaced since it was sampled without holding a
	// write lock. If so, someone else already made progress — fine either
	// way, the caller's loop re-checks the budget regardless.
	if cur, ok := oldest.shard.data[oldest.key]; ok && cur == oldest.entry {
		delete(oldest.shard.data, oldest.key)
		s.usedBytes.Add(-cur.size)
		s.evictions.Add(1)
	}
	oldest.shard.mu.Unlock()
	return true
}

// sampleRandom draws up to n candidates by picking a random shard and
// taking whatever key its (randomized) map iteration visits first, n
// times. With replacement, and with no guarantee of hitting every shard —
// this is the fast, approximate path; see sampleEveryShard for the
// guaranteed-coverage fallback.
func (s *Store) sampleRandom(n int) []sampledKey {
	var out []sampledKey
	for i := 0; i < n; i++ {
		sh := s.shards[rand.Intn(shardCount)]
		sh.mu.RLock()
		for k, e := range sh.data {
			out = append(out, sampledKey{shard: sh, key: k, entry: e})
			break
		}
		sh.mu.RUnlock()
	}
	return out
}

// sampleEveryShard takes one key from every shard that currently has any.
// Guarantees finding a candidate if the store isn't completely empty,
// unlike sampleRandom, at the cost of visiting all shardCount shards
// instead of just n of them.
func (s *Store) sampleEveryShard() []sampledKey {
	var out []sampledKey
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			out = append(out, sampledKey{shard: sh, key: k, entry: e})
			break
		}
		sh.mu.RUnlock()
	}
	return out
}
