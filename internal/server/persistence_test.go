package server

import (
	"path/filepath"
	"testing"

	"github.com/Breyes05/mini-redis-go/internal/persistence"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

// TestPersistence_SurvivesRestart simulates a full crash-and-restart cycle:
// commands are applied against one store and logged to an AOF, that store
// is discarded entirely (as if the process had crashed), and a brand new
// store/server pair replays the same file. If persistence is wired
// correctly, the new store ends up in exactly the state the old one was in
// right before it "crashed".
func TestPersistence_SurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	// --- before "crash" ---
	aof, err := persistence.Open(path, persistence.FsyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	st1 := store.New()
	srv1 := New("", st1)
	srv1.SetAOF(aof)

	srv1.Apply([]string{"SET", "foo", "bar"})
	srv1.Apply([]string{"SET", "temp", "value"})
	srv1.Apply([]string{"DEL", "temp"})
	srv1.Apply([]string{"GET", "foo"}) // reads must not be logged

	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st1.Close()

	// --- "restart": fresh store, nothing carried over except the file ---
	st2 := store.New()
	defer st2.Close()
	srv2 := New("", st2)

	if err := persistence.Replay(path, srv2.Apply); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if v, ok := st2.Get("foo"); !ok || v != "bar" {
		t.Fatalf("Get(foo) after replay = (%q, %v), want (bar, true)", v, ok)
	}
	if st2.Exists("temp") {
		t.Fatalf("Exists(temp) after replay = true, want false (it was deleted before the crash)")
	}
}

// TestPersistence_OnlyMutatingCommandsAreLogged guards against log bloat
// (and wasted replay time) from logging read commands that can't possibly
// change what a replay needs to reproduce.
func TestPersistence_OnlyMutatingCommandsAreLogged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	aof, err := persistence.Open(path, persistence.FsyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	st := store.New()
	defer st.Close()
	srv := New("", st)
	srv.SetAOF(aof)

	srv.Apply([]string{"SET", "foo", "bar"})
	srv.Apply([]string{"GET", "foo"})
	srv.Apply([]string{"EXISTS", "foo"})
	srv.Apply([]string{"TTL", "foo"})
	srv.Apply([]string{"PING"})
	srv.Apply([]string{"EXPIRE", "missing-key", "60"}) // no-op: key doesn't exist

	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var replayed int
	if err := persistence.Replay(path, func(args []string) { replayed++ }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("replayed %d commands, want 1 (only the SET)", replayed)
	}
}
