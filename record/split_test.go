package record

import (
	"bufio"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestSplit(t *testing.T) {
	var cases = []struct {
		about     string
		r         io.Reader
		tag       string
		batchSize int
		result    []string
		err       error
	}{
		{
			about:     "empty string, empty tag",
			r:         strings.NewReader(""),
			tag:       "",
			batchSize: 1,
			result:    nil,
			err:       nil,
		},
		{
			about:     "a single valid XML snippet",
			r:         strings.NewReader("<a>hello</a>"),
			tag:       "a",
			batchSize: 1,
			result:    []string{"<a>hello</a>"},
			err:       nil,
		},
		{
			about:     "two valid XML elements",
			r:         strings.NewReader("<a>hello</a><a>hi</a>"),
			tag:       "a",
			batchSize: 1,
			result:    []string{"<a>hello</a>", "<a>hi</a>"},
			err:       nil,
		},
		{
			about:     "one, nested",
			r:         strings.NewReader("<a><b>hello</b></a>"),
			tag:       "a",
			batchSize: 1,
			result:    []string{"<a><b>hello</b></a>"},
			err:       nil,
		},
		{
			about:     "one, nested, same tag",
			r:         strings.NewReader("<a><a>hello</a></a>"),
			tag:       "a",
			batchSize: 1,
			result:    []string{"<a><a>hello</a></a>"}, // TODO
			err:       ErrNestedTagsNotImplemented,
		},
	}
	for _, c := range cases {
		var (
			s      = bufio.NewScanner(c.r)
			ts     = NewTagSplitter(c.tag)
			result []string
		)
		ts.BatchSize = c.batchSize
		s.Split(ts.Split)
		for s.Scan() {
			result = append(result, s.Text())
		}
		if s.Err() != c.err {
			t.Errorf("got %v, want %v", s.Err(), c.err)
		}
		if s.Err() == nil && !reflect.DeepEqual(result, c.result) {
			t.Errorf("got (%d) %v, want (%d) %v", len(result), result, len(c.result), c.result)
		}
	}
}
