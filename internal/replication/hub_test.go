package replication

import (
	"testing"
	"time"
)

func TestHub_BroadcastReachesRegisteredReplicas(t *testing.T) {
	h := NewHub()
	ch1 := h.Register()
	ch2 := h.Register()

	if got := h.ReplicaCount(); got != 2 {
		t.Fatalf("ReplicaCount() = %d, want 2", got)
	}

	h.Broadcast([]byte("hello"))

	for i, ch := range []chan []byte{ch1, ch2} {
		select {
		case got := <-ch:
			if string(got) != "hello" {
				t.Fatalf("replica %d got %q, want %q", i, got, "hello")
			}
		default:
			t.Fatalf("replica %d received nothing", i)
		}
	}

	if got := h.Offset(); got != int64(len("hello")) {
		t.Fatalf("Offset() = %d, want %d", got, len("hello"))
	}
}

func TestHub_UnregisterStopsDelivery(t *testing.T) {
	h := NewHub()
	ch := h.Register()
	h.Unregister(ch)

	if got := h.ReplicaCount(); got != 0 {
		t.Fatalf("ReplicaCount() = %d, want 0", got)
	}

	// Unregister closes the channel; a closed channel with nothing left to
	// receive should read back a zero value immediately, not block.
	if _, ok := <-ch; ok {
		t.Fatalf("expected channel to be closed after Unregister")
	}

	// Broadcasting after Unregister must not panic (e.g. sending on a
	// closed channel) even though nothing is registered to receive it.
	h.Broadcast([]byte("x"))
}

func TestHub_UnregisterIsIdempotent(t *testing.T) {
	h := NewHub()
	ch := h.Register()
	h.Unregister(ch)
	h.Unregister(ch) // must not panic (e.g. double-close)
}

func TestHub_SlowReplicaIsDroppedNotBlocked(t *testing.T) {
	h := NewHub()
	ch := h.Register()

	// Fill the replica's buffer without ever draining it, then send one
	// more — Broadcast must drop the replica instead of blocking.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			h.Broadcast([]byte("x"))
		}
		close(done)
	}()

	select {
	case <-done: // Broadcast returned promptly for all 2000 sends despite ch never being read
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast appears to have blocked on a full replica channel instead of dropping it")
	}

	if got := h.ReplicaCount(); got != 0 {
		t.Fatalf("ReplicaCount() = %d, want 0 (slow replica should have been dropped)", got)
	}
	if _, ok := <-ch; ok {
		// Channel may still have buffered items; drain until closed.
		for ok {
			_, ok = <-ch
		}
	}
}
