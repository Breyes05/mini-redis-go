// Package resp implements a reader and writer for RESP (REdis Serialization
// Protocol), the wire protocol Redis clients and servers use to talk to each
// other. Speaking real RESP means any existing Redis client — redis-cli,
// go-redis, redis-py, even `nc` — can talk to this server unmodified.
//
// Protocol reference: https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Type is the one-byte RESP type prefix.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

// Value is a single parsed RESP value.
type Value struct {
	Type  Type
	Str   string  // SimpleString, Error, BulkString payload
	Num   int64   // Integer payload
	Array []Value // Array payload
	IsNil bool    // true for a null bulk string ($-1) or null array (*-1)
}

// Reader parses RESP-encoded data from a byte stream.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r *bufio.Reader) *Reader {
	return &Reader{r: r}
}

// ReadCommand reads one client request. Real clients always send commands as
// a RESP array of bulk strings, e.g. SET foo bar encodes as:
//
//	*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
//
// For convenience when poking at the server with `nc` or `telnet`, a plain
// inline line (no RESP framing at all, just "PING\r\n") is also accepted.
func (r *Reader) ReadCommand() ([]string, error) {
	line, err := r.peekLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || Type(line[0]) != Array {
		return r.readInline()
	}

	v, err := r.readValue()
	if err != nil {
		return nil, err
	}
	if v.IsNil {
		return nil, nil
	}
	args := make([]string, len(v.Array))
	for i, item := range v.Array {
		if item.Type != BulkString {
			return nil, fmt.Errorf("resp: expected bulk string in command array, got %q", item.Type)
		}
		args[i] = item.Str
	}
	return args, nil
}

// readInline consumes one line and splits it on whitespace, supporting
// plain-text commands typed by a human over a raw socket.
func (r *Reader) readInline() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, nil
	}
	var args []string
	start := -1
	for i := 0; i <= len(line); i++ {
		if i < len(line) && line[i] != ' ' {
			if start == -1 {
				start = i
			}
			continue
		}
		if start != -1 {
			args = append(args, line[start:i])
			start = -1
		}
	}
	return args, nil
}

func (r *Reader) readValue() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, errors.New("resp: empty line where a type prefix was expected")
	}
	t := Type(line[0])
	body := line[1:]
	switch t {
	case SimpleString:
		return Value{Type: SimpleString, Str: body}, nil
	case Error:
		return Value{Type: Error, Str: body}, nil
	case Integer:
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid integer %q: %w", body, err)
		}
		return Value{Type: Integer, Num: n}, nil
	case BulkString:
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid bulk string length %q: %w", body, err)
		}
		if n < 0 {
			return Value{Type: BulkString, IsNil: true}, nil
		}
		buf := make([]byte, n+2) // payload + trailing \r\n
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: string(buf[:n])}, nil
	case Array:
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid array length %q: %w", body, err)
		}
		if n < 0 {
			return Value{Type: Array, IsNil: true}, nil
		}
		items := make([]Value, n)
		for i := 0; i < n; i++ {
			v, err := r.readValue()
			if err != nil {
				return Value{}, err
			}
			items[i] = v
		}
		return Value{Type: Array, Array: items}, nil
	default:
		return Value{}, fmt.Errorf("resp: unknown type prefix %q", t)
	}
}

// readLine reads up to (and strips) a trailing \r\n.
func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}

// peekLine looks at the first byte of the next line without consuming input,
// so ReadCommand can decide between RESP framing and inline commands.
func (r *Reader) peekLine() ([]byte, error) {
	b, err := r.r.Peek(1)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Writer serializes values into the RESP wire format.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w *bufio.Writer) *Writer { return &Writer{w: w} }

func (w *Writer) WriteSimpleString(s string) error {
	if _, err := fmt.Fprintf(w.w, "+%s\r\n", s); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) WriteError(msg string) error {
	if _, err := fmt.Fprintf(w.w, "-%s\r\n", msg); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) WriteInteger(n int64) error {
	if _, err := fmt.Fprintf(w.w, ":%d\r\n", n); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) WriteBulkString(s string) error {
	if _, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) WriteNilBulkString() error {
	if _, err := fmt.Fprint(w.w, "$-1\r\n"); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) WriteArray(values []string) error {
	if _, err := fmt.Fprintf(w.w, "*%d\r\n", len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if _, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(v), v); err != nil {
			return err
		}
	}
	return w.w.Flush()
}
