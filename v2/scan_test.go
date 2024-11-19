package parallel

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestProc(t *testing.T) {
	var cases = []struct {
		R        io.Reader
		Expected string
		F        Func
		Err      error
	}{
		{
			R:        strings.NewReader("ABC\n"),
			Expected: "ABC",
			F: func(p []byte) ([]byte, error) {
				return p, nil
			},
			Err: nil,
		},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		proc := New(c.R, &buf, c.F)
		err := proc.Run()
		if err != c.Err {
			t.Fatalf("got %v, want %v", err, c.Err)
		}
		if buf.String() != c.Expected {
			t.Fatalf("got %v, want %v", buf.String(), c.Expected)
		}
	}
}
