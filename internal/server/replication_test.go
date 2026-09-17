package server

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/replication"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

// startListening boots srv on an OS-assigned TCP port and returns its
// address, cleaning up the listener on test completion.
func startListening(t *testing.T, srv *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// eventually polls cond every 10ms until it's true or the timeout elapses,
// failing the test if it never becomes true. Needed because replication is
// asynchronous — there's no single call that means "the follower is caught
// up".
func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func TestReplication_FollowerReceivesExistingAndLiveWrites(t *testing.T) {
	// Leader starts with one key already set, before any follower connects
	// — this is what the full-sync snapshot has to carry across.
	leaderStore := store.New()
	defer leaderStore.Close()
	leader := New("", leaderStore)
	leader.Apply([]string{"SET", "existing", "before-sync"})
	leaderAddr := startListening(t, leader)

	followerStore := store.New()
	defer followerStore.Close()
	follower := New("", followerStore)
	follower.SetReadOnly(true)
	var offset int64
	follower.SetReplicaOffset(&offset)
	go replication.RunFollower(leaderAddr, follower.Apply, &offset)

	// The pre-existing key must arrive via the full-sync snapshot.
	eventually(t, 2*time.Second, func() bool {
		v, ok := followerStore.Get("existing")
		return ok && v == "before-sync"
	})

	// A write on the leader after the follower connected must be streamed
	// live and show up on the follower.
	leader.Apply([]string{"SET", "live", "streamed"})
	eventually(t, 2*time.Second, func() bool {
		v, ok := followerStore.Get("live")
		return ok && v == "streamed"
	})

	// A DEL on the leader must also propagate.
	leader.Apply([]string{"DEL", "existing"})
	eventually(t, 2*time.Second, func() bool {
		return !followerStore.Exists("existing")
	})

	// Once caught up, the follower's reported offset should match the
	// leader's — both count the same encoded bytes of the same commands.
	eventually(t, 2*time.Second, func() bool {
		return atomic.LoadInt64(&offset) == leader.hub.Offset()
	})
}

func TestReplication_FollowerRejectsWritesFromOrdinaryClients(t *testing.T) {
	leaderStore := store.New()
	defer leaderStore.Close()
	leader := New("", leaderStore)
	leaderAddr := startListening(t, leader)

	followerStore := store.New()
	defer followerStore.Close()
	follower := New("", followerStore)
	follower.SetReadOnly(true)
	go replication.RunFollower(leaderAddr, follower.Apply, nil)

	followerAddr := startListening(t, follower)
	time.Sleep(100 * time.Millisecond) // give RunFollower a moment to dial before we connect as a client

	conn, err := net.Dial("tcp", followerAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer conn.Close()

	sendAndExpect(t, conn, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"-READONLY You can't write against a read only replica.\r\n")
}
