// Package store implements the in-memory keyspace: a sharded map with
// per-key expiration, combining lazy expiry (checked on read) with an
// active background sweep (so expired keys that are never read again don't
// sit in memory forever) — the same two-pronged approach real Redis uses.
package store

import (
	"hash/fnv"
	"sync"
	"time"
)

// shardCount trades a bit of memory overhead for reduced lock contention:
// keys hash across this many independent shards, so operations on two
// different keys almost never block each other. A single global mutex would
// be simpler but would serialize every command through one lock.
const shardCount = 16

type entry struct {
	value     string
	expiresAt time.Time // zero value means "no expiry"
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a sharded, in-memory, thread-safe key-value store.
type Store struct {
	shards [shardCount]*shard
	done   chan struct{}
	once   sync.Once
}

// New creates a Store and starts its background expiry sweep. Call Close
// when done to stop the sweep goroutine.
func New() *Store {
	s := &Store{done: make(chan struct{})}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]entry)}
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

// Set stores key=value. A ttl of 0 means the key never expires.
func (s *Store) Set(key, value string, ttl time.Duration) {
	sh := s.shardFor(key)
	e := entry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	sh.mu.Lock()
	sh.data[key] = e
	sh.mu.Unlock()
}

// Get returns the value for key and whether it was found (and not expired).
func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()
	if !ok || e.expired(time.Now()) {
		return "", false
	}
	return e.value, true
}

// Del removes key, returning whether it existed.
func (s *Store) Del(key string) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	_, ok := sh.data[key]
	delete(sh.data, key)
	sh.mu.Unlock()
	return ok
}

// Exists reports whether key is present and not expired.
func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// TTL returns the remaining time-to-live for key. ok is false if the key
// doesn't exist (or has already expired); remaining is -1 if the key exists
// but carries no expiry.
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
					}
				}
				sh.mu.Unlock()
			}
		}
	}
}
