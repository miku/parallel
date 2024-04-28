package record

import (
	"bytes"
	"errors"
	"index/suffixarray"
	"io"
	"sort"
	"sync"
)

const (
	// defaultMaxBytes is the default approximate batch size
	defaultMaxBytes = 16777216
	// internalBufferPruneLimit is the number of bytes kept in the buffer; this
	// mostly keep the internal buffer from growing w/o limits when no tag is
	// found in the stream.
	internalBufferPruneLimit = 16384      // bytes
	maxBufSize               = 1073741824 // 1GB (please send me real-world XML where an element exceeds 1GB -- I think they exist)
)

var (
	ErrTagRequired              = errors.New("tag required")
	ErrGarbledInput             = errors.New("likely gabled input")
	ErrNestedTagsNotImplemented = errors.New("nested tags with the same name not implemented yet")
	ErrMaxBufSizeExceeded       = errors.New("max buf size exceeded (data may not be valid xml)")

	errOpenTagNotFound = errors.New("open tag not found")
)

// TagSplitter splits input on XML elements. It will batch content up to
// approximately MaxBytesApprox bytes. It is guaranteed that each batch
// contains at least one complete element content.
type TagSplitter struct {
	// Tag to split on. Nested tags with the same name are not supported
	// currently (they will cause an error).
	Tag string
	// MaxBytesApprox is the approximate number of bytes in a batch. A batch
	// will always contain at least one element, which may exceed this number.
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
// not be larger twice the size of the byte slice passed to Split, but at least
// 16K. The byte slice passed to Split is typically of size "getconf PAGE_SIZE"
// on Linux.
//
// Currently, the median buffer size while running over pubmed JATS XML is
// about 3KB.
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
	// certain number of bytes, so they could accomodate an XML tag.
	L := 2 * len(data)
	if internalBufferPruneLimit > L {
		L = internalBufferPruneLimit
	}
	if len(s.buf) < L {
		return
	}
	k := int(len(s.buf) / 2)
	s.buf = s.buf[k:]
}

// ensureTags set tag values to search for in the stream.
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
		if s.batch.Len() >= s.maxBytes() {
			// Return token, if we hit batch threshold.
			b := s.batch.Bytes()
			s.batch.Reset()
			return len(data), b, nil
		}
		n, err := s.copyContent(&s.batch)
		switch {
		case err == errOpenTagNotFound:
			// Keep the internal buffer from growing, but only if we do not
			// find an opening tag. Searching for a closing tag means we are
			// inside a tag and we may want to search on.
			s.pruneBuf(data)
		case err != nil:
			return len(data), nil, err
		}
		if n == 0 {
			if atEOF {
				s.done = true
				if s.batch.Len() == 0 {
					return len(data), nil, nil
				}
				// Return the rest of the batch, completely.
				return len(data), s.batch.Bytes(), nil
			} else {
				return len(data), nil, nil
			}
		}
	}
}

// copyContent reads at most one element content from the internal buffer and
// writes it to the given writer. Returns the number of bytes read, e.g. zero
// if no complete element has been found in the internal buffer. This may fail
// on invalid XML or very large XML elements.
func (s *TagSplitter) copyContent(w io.Writer) (n int, err error) {
	if len(s.buf) > maxBufSize {
		return 0, ErrMaxBufSizeExceeded
	}
	index := suffixarray.New(s.buf)
	// We can treat both tags the same, as they have the same length,
	// accidentally.
	ot1 := index.Lookup(s.openingTag1, -1)
	ot2 := index.Lookup(s.openingTag2, -1)
	openingTagIndices := append(ot1, ot2...)
	if len(openingTagIndices) == 0 {
		return 0, errOpenTagNotFound
	}
	closingTagIndices := index.Lookup(s.closingTag, -1)
	if len(closingTagIndices) == 0 {
		return 0, nil
	}
	var start, end, last int
	if len(openingTagIndices) == 1 && len(closingTagIndices) == 1 {
		start = openingTagIndices[0]
		end = closingTagIndices[0]
		if end < start {
			return 0, ErrGarbledInput
		}
		last = end + len(s.Tag) + 3 // TODO: assumes </...>
	} else {
		sort.Ints(openingTagIndices)
		sort.Ints(closingTagIndices)
		start, end = findMatchingTags(openingTagIndices, closingTagIndices)
		if end < start {
			return 0, ErrGarbledInput
		}
		if start == -1 {
			// no matching tag found
			return 0, nil
		}
		last = end + len(s.Tag) + 3 // TODO: assumes </...>
	}
	n, err = w.Write(s.buf[start:last])
	s.buf = s.buf[last:] // TODO: optimize this, ringbuffer?
	return
}

// findMatchingTags returns the indices of matching opening and close tags. The
// opening tag used is always the first one. Returns [-1, -1] if no matching
// closing tag exists.
func findMatchingTags(opening []int, closing []int) (int, int) {
	if len(opening) == 0 || len(closing) == 0 {
		return -1, -1
	}
	var i, j, size int
	for {
		if j == len(closing) {
			return -1, -1
		}
		if i < len(opening) && opening[i] < closing[j] {
			size++
			i++
		} else {
			size--
			if size == 0 {
				return opening[0], closing[j]
			}
			j++
		}
	}
}
