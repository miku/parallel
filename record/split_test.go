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
			result:    []string{"<a><a>hello</a></a>"},
			err:       ErrNestedTagsWithSameNameNotImplemented, // TODO
		},
		{
			about:     "three tags, batch size 2",
			r:         strings.NewReader("<a>1</a><a>2</a><a>3</a>"),
			tag:       "a",
			batchSize: 2,
			result:    []string{"<a>1</a><a>2</a>", "<a>3</a>"},
			err:       nil,
		},
		{
			about:     "four tags, batch size 2, noise",
			r:         strings.NewReader("<a>1</a><a>2</a><a>3</a><x></x><a>4</a>"),
			tag:       "a",
			batchSize: 2,
			result:    []string{"<a>1</a><a>2</a>", "<a>3</a><a>4</a>"},
			err:       nil,
		},
		{
			about:     "single matching tag, noise",
			r:         strings.NewReader("<a>1</a><a>2</a><a>3</a><x>X</x><a>4</a>"),
			tag:       "x",
			batchSize: 2,
			result:    []string{"<x>X</x>"},
			err:       nil,
		},
		{
			about:     "no matching tag at all",
			r:         strings.NewReader("<a>1</a><a>2</a><a>3</a><x></x><a>4</a>"),
			tag:       "z",
			batchSize: 2,
			result:    nil,
			err:       nil,
		},
		{
			about:     "prefix tag name",
			r:         strings.NewReader(`<PubmedArticles><PubmedArticle>1</PubmedArticle></PubmedArticles>`),
			tag:       "PubmedArticle",
			batchSize: 1,
			result:    []string{`<PubmedArticle>1</PubmedArticle>`},
			err:       nil,
		},
		{
			about:     "works with attributes",
			r:         strings.NewReader(`<a z="ok">1</a>`),
			tag:       "a",
			batchSize: 1,
			result:    []string{`<a z="ok">1</a>`},
			err:       nil,
		},
		{
			about:     "more attributes",
			r:         strings.NewReader(`NNN <a>1</a> NNN <a k="v">2</a> NNN`),
			tag:       "a",
			batchSize: 1,
			result:    []string{`<a>1</a>`, `<a k="v">2</a>`},
			err:       nil,
		},
		{
			about: "newlines",
			r: strings.NewReader(`NNN
				<a>1
</a> NNN <a k="v">
2
</a>NNN`),
			tag:       "a",
			batchSize: 1,
			result: []string{`<a>1
</a>`, `<a k="v">
2
</a>`},
			err: nil,
		},
	}
	for _, c := range cases {
		var (
			s      = bufio.NewScanner(c.r)
			ts     = NewTagSplitter(c.tag)
			result []string
		)
		ts.BatchSize = c.batchSize
		// s.Split(ts.Split)
		s.Split(ts.SplitZ)
		for s.Scan() {
			result = append(result, s.Text())
		}
		if s.Err() != c.err {
			t.Errorf("[%s] got %v, want %v", c.about, s.Err(), c.err)
		}
		if s.Err() == nil && !reflect.DeepEqual(result, c.result) {
			t.Errorf("[%s] got (%d) %v, want (%d) %v", c.about, len(result), result, len(c.result), c.result)
		}
	}
}
