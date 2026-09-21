// Package server wires the RESP protocol to the store: it accepts TCP
// connections, parses commands off the wire, and dispatches them to
// handlers.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/persistence"
	"github.com/Breyes05/mini-redis-go/internal/replication"
	"github.com/Breyes05/mini-redis-go/internal/resp"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

// Server accepts connections and serves commands against a Store.
type Server struct {
	addr          string
	store         *store.Store
	aof           *persistence.AOF // nil means persistence is disabled
	hub           *replication.Hub // always present: any server can accept SYNC
	readOnly      bool             // true once this node is a follower
	replicaOffset *int64           // non-nil once this node is a follower
}

func New(addr string, st *store.Store) *Server {
	return &Server{addr: addr, store: st, hub: replication.NewHub()}
}

// SetAOF attaches an AOF log: every mutating command dispatched from this
// point on is appended to it. Call this only after any startup replay is
// done — logging while replaying the very file being replayed would corrupt
// it.
func (s *Server) SetAOF(aof *persistence.AOF) {
	s.aof = aof
}

// SetReadOnly marks this server as a follower: ordinary clients can still
// read, but writes are rejected (mirroring real Redis's replica behavior).
// Commands applied via Apply — i.e. replicated from this node's own leader —
// bypass this check, since those are exactly the writes a follower exists
// to apply.
func (s *Server) SetReadOnly(readOnly bool) {
	s.readOnly = readOnly
}

// SetReplicaOffset attaches the counter RunFollower updates as it applies
// replicated writes, so REPLOFFSET can report it.
func (s *Server) SetReplicaOffset(offset *int64) {
	s.replicaOffset = offset
}

// Apply runs a single previously-logged or replicated command directly
// against the store, discarding its response. AOF replay and a follower's
// replication stream both funnel through this: it reuses the exact same
// parsing, validation, and propagation path a live connection would go
// through (via dispatch), just with nowhere to send the reply, and with the
// read-only check bypassed since this is the one path meant to write
// regardless of that flag.
func (s *Server) Apply(args []string) {
	w := resp.NewWriter(bufio.NewWriter(io.Discard))
	_ = s.dispatch(args, w, true)
}

// propagate is called after a command has mutated the store: it appends the
// command to the AOF (if persistence is enabled) and fans it out to any
// connected replicas (if any are connected — Broadcast is a no-op
// otherwise). Encoding once and sharing it between the two avoids paying to
// serialize the same command twice.
func (s *Server) propagate(args []string) {
	encoded := resp.EncodeCommand(args)
	if s.aof != nil {
		if err := s.aof.AppendEncoded(encoded); err != nil {
			log.Printf("aof: failed to append %v: %v", args, err)
		}
	}
	s.hub.Broadcast(encoded)
}

// isWriteCommand reports whether cmd mutates the store — used to reject
// writes from ordinary clients on a read-only follower.
func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "EXPIRE":
		return true
	default:
		return false
	}
}

// ListenAndServe binds addr and serves connections until it hits an error
// (e.g. the listener is closed).
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("mini-redis-go listening on %s", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn serves one client connection until it disconnects or sends a
// malformed request. Each connection gets its own goroutine, so slow or
// idle clients never block others.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(bufio.NewReader(conn))
	writer := resp.NewWriter(bufio.NewWriter(conn))

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		if strings.ToUpper(args[0]) == "SYNC" {
			s.serveReplica(conn)
			return
		}
		if err := s.dispatch(args, writer, false); err != nil {
			log.Printf("write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

// serveReplica handles a connection that just sent SYNC: it registers as a
// replica *before* taking a snapshot (so nothing mutated after this point
// can be missed — see RegisterWithOffset), sends that snapshot (as a
// baseline offset and count, followed by that many SET commands), then
// streams every subsequent mutating command until the connection breaks.
// The connection's read side is abandoned at this point — a replica has
// nothing further to say once it's asked to sync.
func (s *Server) serveReplica(conn net.Conn) {
	addr := conn.RemoteAddr()
	log.Printf("replica %s connected, starting full sync", addr)

	ch, baseOffset := s.hub.RegisterWithOffset()
	defer s.hub.Unregister(ch)

	snapshot := s.store.Snapshot()
	if _, err := fmt.Fprintf(conn, ":%d\r\n:%d\r\n", baseOffset, len(snapshot)); err != nil {
		log.Printf("replica %s: failed writing snapshot header: %v", addr, err)
		return
	}
	for _, e := range snapshot {
		cmd := []string{"SET", e.Key, e.Value}
		if e.TTL > 0 {
			// Round up so a key with, say, 400ms left doesn't truncate to
			// "0 seconds" and get misread as "no expiry" by SET.
			secs := int64(e.TTL / time.Second)
			if e.TTL%time.Second != 0 {
				secs++
			}
			cmd = append(cmd, "EX", strconv.FormatInt(secs, 10))
		}
		if _, err := conn.Write(resp.EncodeCommand(cmd)); err != nil {
			log.Printf("replica %s: failed writing snapshot entry: %v", addr, err)
			return
		}
	}
	log.Printf("replica %s: full sync complete (%d keys), now streaming", addr, len(snapshot))

	for encoded := range ch {
		if _, err := conn.Write(encoded); err != nil {
			log.Printf("replica %s disconnected: %v", addr, err)
			return
		}
	}
}

func (s *Server) dispatch(args []string, w *resp.Writer, internal bool) error {
	cmd := strings.ToUpper(args[0])
	if !internal && s.readOnly && isWriteCommand(cmd) {
		return w.WriteError("READONLY You can't write against a read only replica.")
	}
	switch cmd {
	case "PING":
		return w.WriteSimpleString("PONG")
	case "ECHO":
		if len(args) != 2 {
			return w.WriteError("ERR wrong number of arguments for 'echo' command")
		}
		return w.WriteBulkString(args[1])
	case "SET":
		return s.handleSet(args, w)
	case "GET":
		if len(args) != 2 {
			return w.WriteError("ERR wrong number of arguments for 'get' command")
		}
		if v, ok := s.store.Get(args[1]); ok {
			return w.WriteBulkString(v)
		}
		return w.WriteNilBulkString()
	case "DEL":
		if len(args) < 2 {
			return w.WriteError("ERR wrong number of arguments for 'del' command")
		}
		var n int64
		for _, key := range args[1:] {
			if s.store.Del(key) {
				n++
			}
		}
		s.propagate(args)
		return w.WriteInteger(n)
	case "EXISTS":
		if len(args) != 2 {
			return w.WriteError("ERR wrong number of arguments for 'exists' command")
		}
		if s.store.Exists(args[1]) {
			return w.WriteInteger(1)
		}
		return w.WriteInteger(0)
	case "EXPIRE":
		return s.handleExpire(args, w)
	case "DBSIZE":
		if len(args) != 1 {
			return w.WriteError("ERR wrong number of arguments for 'dbsize' command")
		}
		return w.WriteInteger(int64(s.store.KeyCount()))
	case "TTL":
		if len(args) != 2 {
			return w.WriteError("ERR wrong number of arguments for 'ttl' command")
		}
		d, ok := s.store.TTL(args[1])
		switch {
		case !ok:
			return w.WriteInteger(-2) // key does not exist
		case d == -1:
			return w.WriteInteger(-1) // key exists, no expiry set
		default:
			return w.WriteInteger(int64(d.Seconds()))
		}
	case "REPLOFFSET":
		// Not a real Redis command: a small addition so replication lag is
		// a number you can actually observe rather than something you have
		// to take on faith. See docs/DESIGN.md for what it measures.
		return w.WriteInteger(s.replicationOffset())
	default:
		return w.WriteError("ERR unknown command '" + args[0] + "'")
	}
}

// replicationOffset reports this node's view of replication progress: a
// follower reports how many bytes of its leader's stream it has applied;
// anything else (including a leader with no followers) reports how many
// bytes it has broadcast to its own replicas, if any.
func (s *Server) replicationOffset() int64 {
	if s.replicaOffset != nil {
		return atomic.LoadInt64(s.replicaOffset)
	}
	return s.hub.Offset()
}

// handleSet supports `SET key value` and `SET key value EX seconds`.
func (s *Server) handleSet(args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("ERR wrong number of arguments for 'set' command")
	}
	key, value := args[1], args[2]
	var ttl time.Duration
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(args[i]) != "EX" {
			return w.WriteError("ERR syntax error")
		}
		if i+1 >= len(args) {
			return w.WriteError("ERR syntax error")
		}
		secs, err := strconv.Atoi(args[i+1])
		if err != nil {
			return w.WriteError("ERR value is not an integer or out of range")
		}
		ttl = time.Duration(secs) * time.Second
		i++
	}
	s.store.Set(key, value, ttl)
	s.propagate(args)
	return w.WriteSimpleString("OK")
}

func (s *Server) handleExpire(args []string, w *resp.Writer) error {
	if len(args) != 3 {
		return w.WriteError("ERR wrong number of arguments for 'expire' command")
	}
	secs, err := strconv.Atoi(args[2])
	if err != nil {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	v, ok := s.store.Get(args[1])
	if !ok {
		return w.WriteInteger(0)
	}
	s.store.Set(args[1], v, time.Duration(secs)*time.Second)
	s.propagate(args)
	return w.WriteInteger(1)
}
