package resp

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReadCommand_Array(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(raw)))

	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand() error = %v", err)
	}
	want := []string{"SET", "foo", "bar"}
	if len(args) != len(want) {
		t.Fatalf("got %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg[%d] = %q, want %q", i, args[i], want[i])
		}
	}
}

func TestReadCommand_Inline(t *testing.T) {
	r := NewReader(bufio.NewReader(bytes.NewBufferString("PING\r\n")))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand() error = %v", err)
	}
	if len(args) != 1 || args[0] != "PING" {
		t.Fatalf("got %v, want [PING]", args)
	}
}

func TestReadCommand_MultipleInSequence(t *testing.T) {
	raw := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(raw)))
	for i := 0; i < 2; i++ {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("ReadCommand() #%d error = %v", i, err)
		}
		if len(args) != 1 || args[0] != "PING" {
			t.Fatalf("#%d: got %v, want [PING]", i, args)
		}
	}
}

func TestWriter(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*Writer) error
		want string
	}{
		{"simple string", func(w *Writer) error { return w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) error { return w.WriteError("ERR boom") }, "-ERR boom\r\n"},
		{"integer", func(w *Writer) error { return w.WriteInteger(42) }, ":42\r\n"},
		{"bulk string", func(w *Writer) error { return w.WriteBulkString("hi") }, "$2\r\nhi\r\n"},
		{"nil bulk string", func(w *Writer) error { return w.WriteNilBulkString() }, "$-1\r\n"},
		{"array", func(w *Writer) error { return w.WriteArray([]string{"a", "bb"}) }, "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(bufio.NewWriter(&buf))
			if err := tt.fn(w); err != nil {
				t.Fatalf("write error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
