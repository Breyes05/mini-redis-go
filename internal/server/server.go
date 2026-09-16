// Package server wires the RESP protocol to the store: it accepts TCP
// connections, parses commands off the wire, and dispatches them to
// handlers.
package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/persistence"
	"github.com/Breyes05/mini-redis-go/internal/resp"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

// Server accepts connections and serves commands against a Store.
type Server struct {
	addr  string
	store *store.Store
	aof   *persistence.AOF // nil means persistence is disabled
}

func New(addr string, st *store.Store) *Server {
	return &Server{addr: addr, store: st}
}

// SetAOF attaches an AOF log: every mutating command dispatched from this
// point on is appended to it. Call this only after any startup replay is
// done — logging while replaying the very file being replayed would corrupt
// it.
func (s *Server) SetAOF(aof *persistence.AOF) {
	s.aof = aof
}

// Apply runs a single previously-logged command directly against the store,
// discarding its response. This is how AOF replay rebuilds state on
// startup: it reuses the exact same parsing and validation path a live
// connection would go through (via dispatch), just with nowhere to send the
// reply.
func (s *Server) Apply(args []string) {
	w := resp.NewWriter(bufio.NewWriter(io.Discard))
	_ = s.dispatch(args, w)
}

// logToAOF appends a command that just mutated the store. It's a no-op
// when persistence is disabled (s.aof == nil, e.g. during replay itself).
func (s *Server) logToAOF(args []string) {
	if s.aof == nil {
		return
	}
	if err := s.aof.Append(args); err != nil {
		log.Printf("aof: failed to append %v: %v", args, err)
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
		if err := s.dispatch(args, writer); err != nil {
			log.Printf("write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (s *Server) dispatch(args []string, w *resp.Writer) error {
	switch strings.ToUpper(args[0]) {
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
		s.logToAOF(args)
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
	default:
		return w.WriteError("ERR unknown command '" + args[0] + "'")
	}
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
	s.logToAOF(args)
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
	s.logToAOF(args)
	return w.WriteInteger(1)
}
