package record

import (
	"bytes"
	"io"
)

// TagSplitter splits a stream on a given XML element.
type TagSplitter struct {
	tag       []byte       // XML tag to split on
	opening   []byte       // the opening tag to look for
	closing   []byte       // the closing tag to look for
	batchSize int          // number of elements to collect in one batch
	pos       int          // current read position within data
	in        bool         // whether we are inside the tag or not
	buf       bytes.Buffer // read buffer
	count     int          // the number of elements in the buffer so far
}

// NewTagSplitter returns a TagSplitter for a given XML element name, given as string.
func NewTagSplitter(tag string) *TagSplitter {
	return &TagSplitter{
		tag:       []byte(tag),
		opening:   append(append([]byte("<"), []byte(tag)...), []byte(">")...), // TODO: respect namespaces
		closing:   append(append([]byte("</"), []byte(tag)...), []byte(">")...),
		batchSize: 100,
	}
}

// Split finds elements in the stream and will accumulate them up to a given batch size.
func (ts *TagSplitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	defer func() {
		ts.pos = 0
	}()
	if atEOF {
		// at the end, just return the rest; we do not care, if there is a
		// proper end tag, that is the problem of the calling code
		ts.buf.Write(data)
		return len(data), ts.buf.Bytes(), io.EOF
	}
	for {
		if ts.batchSize == ts.count {
			ts.count = 0
			b := ts.buf.Bytes()
			ts.buf.Reset()
			return ts.pos, b, nil
		}
		if ts.in {
			v := bytes.Index(data[ts.pos:], ts.closing)
			if v == -1 {
				// current tag exceeds data, so write all and exit Split
				if n, err := ts.buf.Write(data[ts.pos:]); err != nil {
					return n, nil, err
				}
				return len(data), nil, nil
			} else {
				// end tag found, write and increase counter
				ts.buf.Write(data[ts.pos : ts.pos+v])
				ts.buf.Write(data[ts.pos+v : ts.pos+v+len(ts.closing)])
				ts.in = false
				ts.count++
				ts.pos = ts.pos + v + len(ts.closing)
			}
		} else {
			// search for the next opening tag
			v := bytes.Index(data[ts.pos:], ts.opening)
			if v == -1 {
				// nothing found in rest of data, move on
				return len(data), nil, nil
			} else {
				// found start tag
				ts.in = true
				ts.pos = ts.pos + v
			}
		}
	}
}
