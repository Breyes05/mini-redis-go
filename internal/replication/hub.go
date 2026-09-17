// Package replication implements leader-follower replication: Hub is the
// leader-side registry that fans mutating commands out to connected
// followers, and RunFollower is the follower-side client that connects to a
// leader, full-syncs, and streams live writes.
//
// Neither half imports the server package — Hub only deals in raw encoded
// bytes, and RunFollower is handed an apply callback — so a Server can wire
// both sides in without any import cycle.
package replication

import "sync"

// Hub tracks connected replicas and fans mutating commands out to them.
type Hub struct {
	mu       sync.Mutex
	offset   int64
	replicas map[chan []byte]struct{}
}

func NewHub() *Hub {
	return &Hub{replicas: make(map[chan []byte]struct{})}
}

// Register adds a new replica, returning the channel its connection handler
// should read encoded commands from. The channel is buffered so one slow
// replica can't block command processing for every other client — see
// Broadcast.
func (h *Hub) Register() chan []byte {
	ch, _ := h.RegisterWithOffset()
	return ch
}

// RegisterWithOffset is like Register, but also returns the replication
// offset at the exact moment of registration, atomically with respect to
// Broadcast (both happen under h.mu, so no broadcast can land in the gap
// between reading the offset and starting to listen for future ones).
//
// This is the baseline a full sync hands to a new follower — mirroring
// real Redis's `+FULLRESYNC <offset>` handshake — so the follower can start
// its own counter there instead of at zero. Without it, a follower joining
// after the leader has already broadcast some writes would never converge
// with the leader's offset, since its own counter would only ever reflect
// bytes received after it connected.
func (h *Hub) RegisterWithOffset() (chan []byte, int64) {
	ch := make(chan []byte, 1024)
	h.mu.Lock()
	h.replicas[ch] = struct{}{}
	offset := h.offset
	h.mu.Unlock()
	return ch, offset
}

// Unregister removes and closes a replica's channel. Safe to call more than
// once (or on a channel Broadcast already dropped) — it's a no-op if the
// channel isn't currently registered.
func (h *Hub) Unregister(ch chan []byte) {
	h.mu.Lock()
	if _, ok := h.replicas[ch]; ok {
		delete(h.replicas, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// Broadcast fans an already wire-encoded command out to every connected
// replica and advances the replication offset by its length. A replica
// whose channel is full is dropped instead of blocking every other client's
// write path on one stuck connection; its connection handler notices the
// closed channel and disconnects on its own.
func (h *Hub) Broadcast(encoded []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.offset += int64(len(encoded))
	for ch := range h.replicas {
		select {
		case ch <- encoded:
		default:
			delete(h.replicas, ch)
			close(ch)
		}
	}
}

// Offset returns the total bytes of replicated writes broadcast so far.
// Directly comparable to a follower's own offset (see RunFollower), since
// both count the same encoded command bytes.
func (h *Hub) Offset() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.offset
}

// ReplicaCount returns how many replicas are currently connected.
func (h *Hub) ReplicaCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.replicas)
}
