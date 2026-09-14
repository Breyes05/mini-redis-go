package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/store"
)

// startTestServer boots a real server on an OS-assigned port and returns a
// connected client conn, cleaning both up on test completion.
func startTestServer(t *testing.T) net.Conn {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	st := store.New()
	srv := New("", st)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	t.Cleanup(func() {
		ln.Close()
		st.Close()
	})

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	return conn
}

// sendAndExpect writes a raw RESP request and asserts the exact raw RESP
// reply, exercising the real wire protocol end to end (the same bytes a
// redis-cli or redis client library would send and parse).
func sendAndExpect(t *testing.T, conn net.Conn, request, want string) {
	t.Helper()
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("write: %v", err)
	}
	r := bufio.NewReader(conn)
	buf := make([]byte, len(want))
	if _, err := readFull(r, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != want {
		t.Fatalf("got %q, want %q", string(buf), want)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestServer_PingSetGetDel(t *testing.T) {
	conn := startTestServer(t)

	sendAndExpect(t, conn, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	sendAndExpect(t, conn, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", "+OK\r\n")
	sendAndExpect(t, conn, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$3\r\nbar\r\n")
	sendAndExpect(t, conn, "*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n", ":1\r\n")
	sendAndExpect(t, conn, "*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n", ":1\r\n")
	sendAndExpect(t, conn, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$-1\r\n")
}

func TestServer_ExpireAndTTL(t *testing.T) {
	conn := startTestServer(t)

	sendAndExpect(t, conn, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", "+OK\r\n")
	sendAndExpect(t, conn, "*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$2\r\n60\r\n", ":1\r\n")
	sendAndExpect(t, conn, "*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n", ":-2\r\n")
}

func TestServer_UnknownCommand(t *testing.T) {
	conn := startTestServer(t)
	sendAndExpect(t, conn, "*1\r\n$4\r\nNOPE\r\n", "-ERR unknown command 'NOPE'\r\n")
}
