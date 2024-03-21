package record

import (
	"bufio"
	"log"
	"strings"
	"testing"
)

func TestIndexOpeningTag(t *testing.T) {
	ts := &TagSplitter{Tag: "a", MaxBytesApprox: 1}
	r := strings.NewReader("<a>1</a><a>2</a>")
	s := bufio.NewScanner(r)
	s.Split(ts.Split)
	for s.Scan() {
		log.Printf("token: %s", s.Text())
	}
}

func TestIndexClosingTag(t *testing.T) {}
