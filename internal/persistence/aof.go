// Package persistence implements an append-only file (AOF): a durable log
// of every mutating command, written in the same RESP wire format clients
// use, so it can be replayed through the exact same parser that serves live
// connections to rebuild in-memory state after a restart or crash.
package persistence

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/resp"
)

// FsyncPolicy controls how aggressively the AOF is flushed to durable
// storage, trading throughput against how much data could be lost in a
// crash.
type FsyncPolicy int

const (
	// FsyncEverySec fsyncs on a 1-second timer: bounded data loss (at most
	// ~1s of writes) with negligible per-command overhead. This is Redis's
	// own default (appendfsync everysec) for exactly that reason.
	FsyncEverySec FsyncPolicy = iota
	// FsyncAlways fsyncs after every single write: no data loss on crash,
	// at the cost of one disk sync per mutating command.
	FsyncAlways
)

// AOF is an append-only command log.
type AOF struct {
	mu     sync.Mutex
	file   *os.File
	w      *bufio.Writer
	policy FsyncPolicy
	done   chan struct{}
}

// Open opens (creating if necessary) the AOF file at path for appending. If
// policy is FsyncEverySec, a background goroutine syncs the file once a
// second until Close is called.
func Open(path string, policy FsyncPolicy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}
	a := &AOF{
		file:   f,
		w:      bufio.NewWriter(f),
		policy: policy,
		done:   make(chan struct{}),
	}
	if policy == FsyncEverySec {
		go a.fsyncLoop()
	}
	return a, nil
}

// Append writes one command to the log, encoded exactly as a client would
// send it over the wire (a RESP array of bulk strings). Reusing the wire
// format means Replay can reuse the same resp.Reader that parses live
// connections — there's only one command parser in the whole codebase.
func (a *AOF) Append(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := fmt.Fprintf(a.w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(a.w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.file.Sync()
	}
	return nil
}

func (a *AOF) fsyncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.mu.Lock()
			_ = a.file.Sync()
			a.mu.Unlock()
		}
	}
}

// Close flushes and syncs any buffered writes, stops the background fsync
// goroutine (if running), and closes the underlying file.
func (a *AOF) Close() error {
	if a.policy == FsyncEverySec {
		close(a.done)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.Flush(); err != nil {
		return err
	}
	if err := a.file.Sync(); err != nil {
		return err
	}
	return a.file.Close()
}

// Replay reads every command previously logged to path and calls apply for
// each one, in order. A missing file is treated as an empty log (nothing to
// replay) rather than an error, since that's the normal state on first run.
func Replay(path string, apply func(args []string)) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("aof: open %s for replay: %w", path, err)
	}
	defer f.Close()

	r := resp.NewReader(bufio.NewReader(f))
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("aof: replay %s: %w", path, err)
		}
		if len(args) == 0 {
			continue
		}
		apply(args)
	}
}
