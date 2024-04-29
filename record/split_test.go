package record

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func TestSplit(t *testing.T) {
	var cases = []struct {
		doc                   string
		tagSplitter           *TagSplitter
		input                 string
		expectedResultBatches []string
		err                   error
	}{
		{
			doc:                   "empty input",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "",
			expectedResultBatches: nil,
			err:                   nil,
		},
		{
			doc:                   "single element",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "<a>1</a>",
			expectedResultBatches: []string{"<a>1</a>"},
			err:                   nil,
		},
		{
			doc:                   "broken element",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "<a>1",
			expectedResultBatches: nil,
			err:                   nil,
		},
		{
			doc:                   "two elements",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "<a>1</a><a>2</a>",
			expectedResultBatches: []string{"<a>1</a><a>2</a>"},
			err:                   nil,
		},
		{
			doc:                   "two elements, small batch size",
			tagSplitter:           &TagSplitter{Tag: "a", MaxBytesApprox: 1},
			input:                 "<a>1</a><a>2</a>",
			expectedResultBatches: []string{"<a>1</a>", "<a>2</a>"},
			err:                   nil,
		},
		{
			doc:                   "two elements, noise, small batch size",
			tagSplitter:           &TagSplitter{Tag: "a", MaxBytesApprox: 1},
			input:                 "<a>1</a><a>2</a><b></b><a>3</a>",
			expectedResultBatches: []string{"<a>1</a>", "<a>2</a>", "<a>3</a>"},
			err:                   nil,
		},
		{
			doc:                   "two elements, plus noise",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "<a>1</a>  <a>2</a>   HELLO!",
			expectedResultBatches: []string{"<a>1</a><a>2</a>"},
			err:                   nil,
		},
		{
			doc:                   "prefix matches",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 "<aa>1</aa>  <a>2</a>   HELLO!",
			expectedResultBatches: []string{"<a>2</a>"},
			err:                   nil,
		},
		{
			doc:                   "tag with attributes",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 `<a x="1">1</a>  <a>2</a>   HELLO!`,
			expectedResultBatches: []string{`<a x="1">1</a><a>2</a>`},
			err:                   nil,
		},
		{
			doc:                   "garbled input 1",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 `<a|`,
			expectedResultBatches: nil,
			err:                   nil,
		},
		{
			doc:                   "garbled input 2",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 `</a>...<a>`,
			expectedResultBatches: nil,
			err:                   ErrGarbledInput,
		},
		{
			doc:                   "tag missing",
			tagSplitter:           &TagSplitter{},
			input:                 `</a>...<a>`,
			expectedResultBatches: nil,
			err:                   ErrTagRequired,
		},
		{
			doc:                   "nested elements",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 `<a><a></a></a>`,
			expectedResultBatches: []string{`<a><a></a></a>`},
			err:                   nil,
		},
		{
			doc:                   "many elements",
			tagSplitter:           &TagSplitter{Tag: "a"},
			input:                 `<a>..</a><a>..</a><a>..</a><a>..</a><a>..</a><a>..</a>`,
			expectedResultBatches: []string{`<a>..</a><a>..</a><a>..</a><a>..</a><a>..</a><a>..</a>`},
			err:                   nil,
		},
	}
	for _, c := range cases {
		s := bufio.NewScanner(strings.NewReader(c.input))
		s.Split(c.tagSplitter.Split)
		var result []string
		for s.Scan() {
			result = append(result, s.Text())
		}
		if s.Err() != c.err {
			t.Fatalf("[%s] got %v, want %v", c.doc, s.Err(), c.err)
		}
		if !reflect.DeepEqual(result, c.expectedResultBatches) {
			t.Fatalf("[%s] got (%d) %v, want (%d) %v",
				c.doc, len(result), result, len(c.expectedResultBatches), c.expectedResultBatches)
		}
	}
}

func BenchmarkTagSplitter(b *testing.B) {
	data := `
	....................<a>................
	.......................................
	..............<a></a>..................
	.......................................
	.......................................
	.......................................
	<a>...</a>...............<a>....</a>...
	.......................................
	.......................................
	.......................................
	...................................</a>
	`
	ts := TagSplitter{Tag: "a", MaxBytesApprox: 8}
	s := bufio.NewScanner(strings.NewReader(data))
	s.Split(ts.Split)
	for n := 0; n < b.N; n++ {
		var count int
		for s.Scan() {
			_ = s.Text()
			count++
		}
	}
}
