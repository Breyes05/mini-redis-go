package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("foo", "bar", 0)
	got, ok := s.Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("Get(foo) = (%q, %v), want (bar, true)", got, ok)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get(missing) returned ok=true, want false")
	}
}

func TestDel(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("foo", "bar", 0)
	if !s.Del("foo") {
		t.Fatalf("Del(foo) = false, want true")
	}
	if s.Del("foo") {
		t.Fatalf("Del(foo) second call = true, want false")
	}
	if s.Exists("foo") {
		t.Fatalf("Exists(foo) = true after Del, want false")
	}
}

func TestExpiry_Lazy(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("foo", "bar", 10*time.Millisecond)
	if !s.Exists("foo") {
		t.Fatalf("Exists(foo) = false immediately after Set, want true")
	}

	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("foo"); ok {
		t.Fatalf("Get(foo) succeeded after TTL expired, want miss")
	}
}

func TestTTL(t *testing.T) {
	s := New()
	defer s.Close()

	// No expiry set.
	s.Set("no-ttl", "v", 0)
	remaining, ok := s.TTL("no-ttl")
	if !ok || remaining != -1 {
		t.Fatalf("TTL(no-ttl) = (%v, %v), want (-1, true)", remaining, ok)
	}

	// Expiry set.
	s.Set("with-ttl", "v", time.Minute)
	remaining, ok = s.TTL("with-ttl")
	if !ok || remaining <= 0 || remaining > time.Minute {
		t.Fatalf("TTL(with-ttl) = (%v, %v), want (0,1m], true", remaining, ok)
	}

	// Missing key.
	if _, ok := s.TTL("missing"); ok {
		t.Fatalf("TTL(missing) ok = true, want false")
	}
}

func TestActiveExpirySweep(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("foo", "bar", 10*time.Millisecond)
	// Wait long enough for the active expiry sweep (ticks every 100ms) to
	// run at least once and reclaim the key, independent of any Get/lazy
	// expiry check.
	time.Sleep(150 * time.Millisecond)

	sh := s.shardFor("foo")
	sh.mu.RLock()
	_, stillPresent := sh.data["foo"]
	sh.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expired key still present in shard after active sweep window")
	}
}

// TestConcurrentAccess exercises the store under concurrent readers and
// writers across many keys, primarily so `go test -race` can catch any
// sharding/locking mistake.
func TestConcurrentAccess(t *testing.T) {
	s := New()
	defer s.Close()

	const goroutines = 50
	const keysPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				s.Set(key, "v", 0)
				s.Get(key)
				s.Exists(key)
				s.Del(key)
			}
		}(g)
	}
	wg.Wait()
}
