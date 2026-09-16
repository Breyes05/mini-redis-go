package persistence

import (
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	aof, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	commands := [][]string{
		{"SET", "foo", "bar"},
		{"SET", "baz", "qux", "EX", "60"},
		{"DEL", "foo"},
	}
	for _, cmd := range commands {
		if err := aof.Append(cmd); err != nil {
			t.Fatalf("Append(%v): %v", cmd, err)
		}
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var replayed [][]string
	if err := Replay(path, func(args []string) {
		replayed = append(replayed, args)
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != len(commands) {
		t.Fatalf("replayed %d commands, want %d: %v", len(replayed), len(commands), replayed)
	}
	for i, want := range commands {
		got := replayed[i]
		if len(got) != len(want) {
			t.Fatalf("command %d: got %v, want %v", i, got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("command %d arg %d: got %q, want %q", i, j, got[j], want[j])
			}
		}
	}
}

func TestReplay_MissingFileIsNotAnError(t *testing.T) {
	called := false
	path := filepath.Join(t.TempDir(), "does-not-exist.aof")

	if err := Replay(path, func(args []string) { called = true }); err != nil {
		t.Fatalf("Replay on missing file returned error: %v", err)
	}
	if called {
		t.Fatalf("apply was called while replaying a nonexistent file")
	}
}

func TestOpen_AppendsToExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	aof, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatalf("Open (first): %v", err)
	}
	if err := aof.Append([]string{"SET", "a", "1"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	// Re-opening the same path must append after existing content, not
	// truncate it — otherwise every restart would wipe prior history before
	// replay even had a chance to read it.
	aof2, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatalf("Open (second): %v", err)
	}
	if err := aof2.Append([]string{"SET", "b", "2"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := aof2.Close(); err != nil {
		t.Fatalf("Close (second): %v", err)
	}

	var replayed [][]string
	if err := Replay(path, func(args []string) { replayed = append(replayed, args) }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %d commands, want 2: %v", len(replayed), replayed)
	}
}

func TestFsyncEverySec_CloseStopsCleanly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	aof, err := Open(path, FsyncEverySec)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Close must stop the background fsync goroutine without hanging.
	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
