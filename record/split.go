package record

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"sync"
)

const (
	// defaultMaxBytes is the default approximate batch size
	defaultMaxBytes = 16777216
	// internalBufferPruneLimit is the minumum number of bytes kept in the buffer; note
	// that this limits the size of an XML that can be processed by this routines
	internalBufferPruneLimit = 1048576
)

var (
	ErrTagRequired              = errors.New("tag required")
	ErrGarbledInput             = errors.New("likely gabled input")
	ErrNestedTagsNotImplemented = errors.New("nested tags with the same name not implemented yet")
)

// TagSplitter splits input on XML elements. It will batch content up to
// approximately MaxBytesApprox bytes. It is guaranteed that each batch
// contains at least one complete element content.
type TagSplitter struct {
	// Tag to split on. Nested tags with the same name are not supperted
	// currently (they will cause an error).
	Tag string
	// MaxBytesApprox is the approximate number of bytes in a batch. A batch
	// will always contain at least one element, which may exceed this number
	// significantly. By default, we use 16M per batch.
	MaxBytesApprox uint
	// buf is the internal scratch space that is used to find a complete
	// element. This buffer will grow as large as required to accomodate a tag.
	buf []byte
	// batch is the staging space to write complete tags to and its size will
	// be approximate limited by MaxBytesApprox.
	batch bytes.Buffer
	// done signals when there is nothing more to return.
	done bool
	// once for initializing the opening and closing tag byte slices; the
	// closing tag to look for (this does not change); opening tags variants,
	// e.g. '<a>', and '<a '; previously, these were assembled as needed, but
	// it may help a tiny bit to not recompute them all the time.
	once        sync.Once
	closingTag  []byte
	openingTag1 []byte
	openingTag2 []byte
}

// maxBytes returns the maximum byte size per batch.
func (s *TagSplitter) maxBytes() int {
	if s.MaxBytesApprox == 0 {
		return defaultMaxBytes
	} else {
		return int(s.MaxBytesApprox)
	}
}

// pruneBuf shrinks the internal buffer, if possible. The internal buffer shall
// never be larger than 16K or twice the size of the byte slice passed to Split
// (whichever is larger). The byte slice passed to Split is typically "getconf
// PAGE_SIZE" on Linux.
//
// Currently, the median buffer size is about 3K.
//
//	In [6]: df = pd.read_csv("buffersize.tsv")
//	In [7]: df.describe()
//	Out[7]:
//
//	count 3701472.000
//	mean     3770.982
//	std      3641.797
//	min         0.000
//	25%      1561.000
//	50%      3126.000
//	75%      5048.000
//	max    289179.000
func (s *TagSplitter) pruneBuf(data []byte) {
	// If the data passed is too small, we want to accumulate at least a
	// certain number of bytes, they could accomodate an XML tag (e.g. 16K).
	if L := slices.Max([]int{2 * len(data), internalBufferPruneLimit}); len(s.buf) < L {
		return
	}
	k := int(len(s.buf) / 2)
	s.buf = s.buf[k:]
}

func (s *TagSplitter) ensureTags() {
	if len(s.closingTag) == 0 {
		s.closingTag = []byte("</" + s.Tag + ">")
	}
	if len(s.openingTag1) == 0 {
		s.openingTag1 = []byte("<" + s.Tag + ">")
	}
	if len(s.openingTag2) == 0 {
		s.openingTag2 = []byte("<" + s.Tag + " ")
	}
}

// Split accumulates one or more XML element contents and returns a batch of
// them as a token. This can be used for downstream XML parsing, where the
// consumer expects a valid tag.
func (s *TagSplitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if s.Tag == "" {
		return 0, nil, ErrTagRequired
	}
	if s.done {
		return len(data), nil, io.EOF
	}
	s.once.Do(func() {
		s.ensureTags()
	})
	s.buf = append(s.buf, data...)
	for {
		// If batch accumulated enough bytes, actually return a token.
		if s.batch.Len() >= s.maxBytes() {
			b := s.batch.Bytes()
			s.batch.Reset()
			return len(data), b, nil
		}
		n, err := s.copyContent(&s.batch)
		if err != nil {
			return 0, nil, err
		}
		if n == 0 {
			if atEOF {
				s.done = true
				if s.batch.Len() == 0 {
					return len(data), nil, nil
				}
				// return the rest of the batch, completely
				return len(data), s.batch.Bytes(), nil
			} else {
				// we did not make any progress and we are not done yet, try to
				// prune buffer
				s.pruneBuf(data)
				return len(data), nil, nil
			}
		}
	}
	return 0, nil, nil
}

// copyContent reads at most one element content from the internal buffer and
// writes it to the given writer. If no complete element has been found in the
// internal buffer, zero is returned. This may fail, if the content is invalid
// XML or if it contains nested tags of the same name.
func (s *TagSplitter) copyContent(w io.Writer) (n int, err error) {
	var start, end, last int
	if start = s.indexOpeningTag(s.buf); start == -1 {
		return 0, nil
	}
	if end = s.indexClosingTag(s.buf); end == -1 {
		return 0, nil
	}
	if end < start {
		return 0, ErrGarbledInput
	}
	last = end + len(s.Tag) + 3
	// sanity check, TODO: fix this w/ a stack
	if s.indexOpeningTag(s.buf[start+1:end]) != -1 {
		return 0, ErrNestedTagsNotImplemented
	}
	n, err = w.Write(s.buf[start:last])
	s.buf = s.buf[last:] // TODO: optimize this, ringbuffer?
	return
}

// indexOpeningTag returns the index of the first opening tag in data, or -1;
// cf. https://www.w3.org/TR/REC-xml/#sec-starttags
func (s *TagSplitter) indexOpeningTag(data []byte) int {
	// TODO: this seems to be a bigger bottleneck
	// (https://i.imgur.com/fYzN2mq.png) that I originally thought. Average
	// size of data is about 3K.
	u := bytes.Index(data, s.openingTag1)
	v := bytes.Index(data, s.openingTag2)
	if u == -1 && v == -1 {
		return -1
	}
	if v == -1 {
		return u
	}
	if u == -1 {
		return v
	}
	return slices.Min([]int{u, v})

}

// indexClosingTag returns the index of the first closing tag in data or -1.
func (s *TagSplitter) indexClosingTag(data []byte) int {
	return bytes.Index(data, s.closingTag)
}
