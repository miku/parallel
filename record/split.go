package record

import (
	"bytes"
	"errors"
	"io"
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

func (s *TagSplitter) openingTag() []byte {
	return []byte("<" + s.Tag + ">") // or whitespace
}

func (s *TagSplitter) closingTag() []byte {
	return []byte("</" + s.Tag + ">")
}

func (s *TagSplitter) maxBytes() int {
	if s.MaxBytesApprox == 0 {
		return defaultMaxBytes
	} else {
		return int(s.MaxBytesApprox)
	}
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
		if n == 0 {
			if atEOF {
				s.done = true
				b := s.batch.Bytes()
				if len(b) == 0 {
					return len(data), nil, nil
				} else {
					return len(data), b, nil
				}
			} else {
				return len(data), nil, nil
			}
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
	var prefix = "<" + s.Tag
	var v = bytes.Index(data, []byte(prefix))
	if len(data) <= v+len(prefix) {
		return -1
	}
	if data[v+len(prefix)] == ' ' || data[v+len(prefix)] == '>' {
		return v
	}
	return -1
}

// indexClosingTag returns the index of the next closing tag in a given byte
// slice.
func (s *TagSplitter) indexClosingTag(data []byte) int {
	return bytes.Index(data, []byte("</"+s.Tag+">"))
}
