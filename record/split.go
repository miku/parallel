package record

import (
	"bytes"
	"errors"
	"io"
	"slices"
)

const defaultMaxBytes = 16777216

var (
	ErrTagRequired              = errors.New("tag required")
	ErrGarbledInput             = errors.New("likely gabled input")
	ErrNestedTagsNotImplemented = errors.New("nested tags with the same name not implemented yet")
)

// TagSplitter splits input on XML elements. It will batch content up to
// approximately MaxBytesApprox bytes. It is guaranteed that each batch
// contains at least one complete element content.
type TagSplitter struct {
	Tag            string
	MaxBytesApprox uint         // max bytes to read per batch, approximately, 16MB by default
	buf            []byte       // temporary storage
	batch          bytes.Buffer // collected content
	done           bool         // are we done processing
}

func (s *TagSplitter) maxBytes() int {
	if s.MaxBytesApprox == 0 {
		return defaultMaxBytes
	} else {
		return int(s.MaxBytesApprox)
	}
}

// pruneBuf shrinks the internal buffer, if possible.
func (s *TagSplitter) pruneBuf(data []byte) {
	L := slices.Max([]int{2 * len(data), 16384})
	if len(s.buf) < L {
		return
	}
	k := int(len(s.buf) / 2)
	s.buf = s.buf[k:]
}

func (s *TagSplitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if s.Tag == "" {
		return 0, nil, ErrTagRequired
	}
	if s.done {
		return 0, nil, io.EOF
	}
	s.buf = append(s.buf, data...)
	for {
		if s.batch.Len() >= s.maxBytes() {
			b := s.batch.Bytes()
			s.batch.Reset()
			return len(data), b, nil
		}
		n, err := s.copyContent(&s.batch)
		if err != nil {
			return 0, nil, err
		}
		// if no content has been found for a few iterations, then drop parts
		// of the internal buffer
		if n == 0 {
			if atEOF {
				s.done = true
				if s.batch.Len() == 0 {
					return len(data), nil, nil
				}
				return len(data), s.batch.Bytes(), nil
			} else {
				return len(data), nil, nil
			}
			// we did not make any progress, try to prune buffer
			s.pruneBuf(data)
		}
	}
	return 0, nil, nil
}

// copyContent reads of a single element content from the internal buffer and
// writes it to the given writer. To determine whether content has been
// written, test for non-zero n.
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
	// sanity check, TODO: fix this
	if s.indexOpeningTag(s.buf[start+1:end]) != -1 {
		return 0, ErrNestedTagsNotImplemented
	}
	n, err = w.Write(s.buf[start:last])
	s.buf = s.buf[last:] // TODO: optimize this
	return
}

// https://www.w3.org/TR/REC-xml/#sec-starttags
func (s *TagSplitter) indexOpeningTag(data []byte) int {
	t := "<" + s.Tag + ">"
	v := bytes.Index(data, []byte(t))
	if v == -1 {
		t = "<" + s.Tag + " "
		v = bytes.Index(data, []byte(t))
	}
	return v
}

// indexClosingTag returns the index of the next closing tag in a given byte
// slice.
func (s *TagSplitter) indexClosingTag(data []byte) int {
	return bytes.Index(data, []byte("</"+s.Tag+">"))
}
