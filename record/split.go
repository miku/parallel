package record

import (
	"bytes"
	"errors"
	"io"
)

var ErrNestedTagsWithSameNameNotImplemented = errors.New("nested tags not implemented")

// TagSplitter splits a stream on a given XML element.
type TagSplitter struct {
	BatchSize int          // number of elements to collect in one batch
	tag       []byte       // XML tag to split on
	opening   []byte       // the opening tag to look for
	closing   []byte       // the closing tag to look for
	pos       int          // current read position within data
	in        bool         // whether we are inside the tag or not
	buf       bytes.Buffer // read buffer
	count     int          // the number of elements in the buffer so far
	done      bool         // signals that we are done processing
}

// NewTagSplitter returns a TagSplitter for a given XML element name, given as string.
func NewTagSplitter(tag string) *TagSplitter {
	return &TagSplitter{
		BatchSize: 100,
		tag:       []byte(tag),
		opening:   []byte("<" + tag), // additional check required; next char must be '>' or whitespace
		closing:   []byte("</" + tag + ">"),
	}
}

// Split finds elements in the stream and will accumulate them up to a given batch size.
func (ts *TagSplitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	defer func() {
		ts.pos = 0
	}()
	if ts.done {
		return 0, nil, io.EOF
	}
	if atEOF {
		// at the end, just return the rest; we do not care, if there is a
		// proper end tag, that is the problem of the calling code
		//
		// if we return io.EOF, Scan() would stop immediately, hence we set
		// done to true and return in the subsequent call, only
		ts.done = true
		if len(data) == 0 && ts.buf.Len() == 0 {
			return 0, nil, nil
		} else {
			ts.buf.Write(data)
			return len(data), ts.buf.Bytes(), nil
		}
	}
	for {
		if ts.BatchSize == ts.count {
			if bytes.Count(ts.buf.Bytes(), ts.opening) != bytes.Count(ts.buf.Bytes(), ts.closing) {
				return 0, nil, ErrNestedTagsWithSameNameNotImplemented
			}
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
				if _, err = ts.buf.Write(data[ts.pos : ts.pos+v]); err != nil {
					return 0, nil, err
				}
				if _, err = ts.buf.Write(data[ts.pos+v : ts.pos+v+len(ts.closing)]); err != nil {
					return 0, nil, err
				}
				ts.in = false
				ts.count++
				ts.pos = ts.pos + v + len(ts.closing)
			}
		} else {
			for {
				// search for the next opening tag
				v := bytes.Index(data[ts.pos:], ts.opening)
				if v == -1 {
					// nothing found in rest of data, move on
					return len(data), nil, nil
				} else {
					k := v + 2
					if data[k] == ' ' || data[k] == '\t' || data[k] == '>' {
						// found start tag
						ts.in = true
						ts.pos = ts.pos + v
						break
					} else {
						ts.pos = ts.pos + 1
					}
				}

			}
		}
	}
}
