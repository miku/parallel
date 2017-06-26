package parallel

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

var errFake1 = errors.New("fake error #1")

func StringSliceContains(sl []string, s string) bool {
	for _, v := range sl {
		if s == v {
			return true
		}
	}
	return false
}

// LinesEqualSeparator returns true, if every line in a, when separated by
// separator, can be found in b.
func LinesEqualSeparator(a, b, sep string) bool {
	al := strings.Split(a, sep)
	bl := strings.Split(b, sep)
	if len(al) != len(bl) {
		return false
	}
	for _, line := range al {
		if !StringSliceContains(bl, line) {
			return false
		}
	}
	return true
}

// LinesEqual returns true, if every line in a, when separated by a newline, can be found in b.
func LinesEqual(a, b string) bool {
	return LinesEqualSeparator(a, b, "\n")
}

func TestSimple(t *testing.T) {
	var cases = []struct {
		about    string
		r        io.Reader
		expected string
		f        TransformerFunc
		err      error
	}{
		{
			about:    `No input produces no output.`,
			r:        strings.NewReader(""),
			expected: "",
			f:        func(b []byte) ([]byte, error) { return []byte{}, nil },
			err:      nil,
		},
		{
			about:    `Order is not guaranteed.`,
			r:        strings.NewReader("a\nb\n"),
			expected: "B\nA\n",
			f:        func(b []byte) ([]byte, error) { return bytes.ToUpper(b), nil },
			err:      nil,
		},
		{
			about:    `Like grep, we can filter out items by returning nothing.`,
			r:        strings.NewReader("a\nb\n"),
			expected: "B\n",
			f: func(b []byte) ([]byte, error) {
				if strings.TrimSpace(string(b)) == "a" {
					return []byte{}, nil
				}
				return bytes.ToUpper(b), nil
			},
			err: nil,
		},
		{
			about:    `Empty lines are passed on.`,
			r:        strings.NewReader("a\na\na\na\n\n\nb\n"),
			expected: "\n\nB\n",
			f: func(b []byte) ([]byte, error) {
				if strings.TrimSpace(string(b)) == "a" {
					return []byte{}, nil
				}
				return bytes.ToUpper(b), nil
			},
			err: nil,
		},
		{
			about:    `On empty input, the transformer func is never called.`,
			r:        strings.NewReader(""),
			expected: "",
			f: func(b []byte) ([]byte, error) {
				return nil, errFake1
			},
			err: nil,
		},
		{
			about:    `Error is passed on.`,
			r:        strings.NewReader("\n"),
			expected: "",
			f: func(b []byte) ([]byte, error) {
				return nil, errFake1
			},
			err: errFake1,
		},
	}

	for _, c := range cases {
		var buf bytes.Buffer
		p := NewProcessor(c.r, &buf, c.f)
		err := p.Run()
		if err != c.err {
			t.Errorf("p.Run: got %v, want %v", err, c.err)
		}
		if !LinesEqual(buf.String(), c.expected) {
			t.Errorf("p.Run: got %v, want %v", buf.String(), c.expected)
		}
	}
}
