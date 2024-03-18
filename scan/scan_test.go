package scan

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestProcessor(t *testing.T) {
	data := `123 XXX 456 XXX 789 XXX `
	r := strings.NewReader(data)
	var buf bytes.Buffer
	p := New(r, &buf, func(p []byte) ([]byte, error) {
		// this processor will just duplicate the input, e.g. turn "123 " into "123 123 ", etc.
		return append(p, p...), nil
	})
	p.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF {
			return 0, nil, io.EOF
		}
		if len(data) >= 4 {
			return 4, data[:4], nil
		}
		return 0, nil, nil
	})
	p.BatchSize = 1
	err := p.Run()
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	var mustContain = []string{
		"123 123",
		"XXX XXX",
		"456 456",
		"789 789",
	}
	// non-deterministic, e.g. 123 123 XXX XXX XXX XXX 789 789 XXX XXX 456 456
	result := buf.String()
	if len(result) != 2*len(data) {
		t.Fatalf("expected len %d, got %d", 2*len(data), len(result))
	}
	for _, v := range mustContain {
		if !strings.Contains(result, v) {
			t.Fatalf("missing %v in result", v)
		}
	}
}
