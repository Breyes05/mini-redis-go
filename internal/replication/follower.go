package replication

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/resp"
)

// RunFollower connects to a leader at addr, performs a full sync, then
// streams subsequent writes indefinitely, applying each one via apply. If
// the connection drops — leader restart, network blip — it reconnects with
// exponential backoff (capped at 30s) rather than giving up, so a follower
// recovers on its own once the leader is reachable again. It never returns;
// run it in its own goroutine.
//
// offset, if non-nil, is updated atomically with the bytes of replicated
// writes applied so far (not counting the initial snapshot) — directly
// comparable to the leader's own Hub.Offset(), since both count the same
// encoded command bytes at the same point in the stream.
func RunFollower(addr string, apply func(args []string), offset *int64) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		connected, err := syncOnce(addr, apply, offset)
		if err != nil {
			log.Printf("replication: %v; retrying in %s", err, backoff)
		}
		if connected {
			// We did talk to the leader, however briefly — trust the
			// network again on the next disconnect rather than staying at
			// whatever backoff we'd climbed to.
			backoff = time.Second
		} else if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		time.Sleep(backoff)
	}
}

func syncOnce(addr string, apply func(args []string), offset *int64) (connected bool, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false, fmt.Errorf("dial leader at %s: %w", addr, err)
	}
	defer conn.Close()
	connected = true
	log.Printf("replication: connected to leader %s, requesting full sync", addr)

	if _, err := conn.Write([]byte("*1\r\n$4\r\nSYNC\r\n")); err != nil {
		return connected, fmt.Errorf("send SYNC: %w", err)
	}

	r := resp.NewReader(bufio.NewReader(conn))

	// The leader hands us the replication offset its snapshot corresponds
	// to (mirroring real Redis's `+FULLRESYNC <offset>`), so our own offset
	// counter starts at the same baseline the leader's already at — not at
	// zero — and the two stay directly comparable from here on.
	baseOffset, err := r.ReadValue()
	if err != nil {
		return connected, fmt.Errorf("read base offset: %w", err)
	}
	if baseOffset.Type != resp.Integer {
		return connected, fmt.Errorf("expected integer base offset, got %q", baseOffset.Type)
	}
	if offset != nil {
		atomic.StoreInt64(offset, baseOffset.Num)
	}

	header, err := r.ReadValue()
	if err != nil {
		return connected, fmt.Errorf("read snapshot header: %w", err)
	}
	if header.Type != resp.Integer {
		return connected, fmt.Errorf("expected integer snapshot header, got %q", header.Type)
	}

	for i := int64(0); i < header.Num; i++ {
		args, err := r.ReadCommand()
		if err != nil {
			return connected, fmt.Errorf("read snapshot entry %d/%d: %w", i+1, header.Num, err)
		}
		apply(args)
	}
	log.Printf("replication: full sync complete (%d keys), streaming live writes", header.Num)

	for {
		args, err := r.ReadCommand()
		if err != nil {
			return connected, err
		}
		if len(args) == 0 {
			continue
		}
		apply(args)
		if offset != nil {
			atomic.AddInt64(offset, int64(len(resp.EncodeCommand(args))))
		}
	}
}
