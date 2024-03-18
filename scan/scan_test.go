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
	result := buf.String()
	for _, v := range mustContain {
		if !strings.Contains(result, v) {
			t.Fatalf("missing %v in result", v)
		}
	}
}
