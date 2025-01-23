package parallel

import (
	"bytes"
	"context"
	"fmt"
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
		{
			R:        strings.NewReader("ABC\nABC\nABC\n"),
			Expected: "ABCABCABC",
			F: func(p []byte) ([]byte, error) {
				return p, nil
			},
			Err: nil,
		},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		proc := New(c.R, &buf, c.F)
		err := proc.Run(context.Background())
		if err != c.Err {
			t.Fatalf("got %v, want %v", err, c.Err)
		}
		if buf.String() != c.Expected {
			t.Fatalf("got %v (%d), want %v (%d)",
				buf.String(),
				len(buf.Bytes()),
				c.Expected,
				len(c.Expected))
		}
	}
}

func TestProcParallel(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		f       Func
		wantErr bool
	}{{
		name:  "simple passthrough",
		input: "hello\nworld\n",
		f: func(b []byte) ([]byte, error) {
			return b, nil
		},
	}, {
		name:  "worker error",
		input: "hello\nworld\n",
		f: func(b []byte) ([]byte, error) {
			return nil, fmt.Errorf("worker error")
		},
		wantErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			r := strings.NewReader(tt.input)
			var w bytes.Buffer

			p := New(r, &w, tt.f)
			err := p.Run(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Run [%v]: got %v, want %v",
					tt.name,
					err,
					tt.wantErr)
			}
		})
	}
}
