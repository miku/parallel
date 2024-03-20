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
	buf       bytes.Buffer // accumulated batch data
	scratch   bytes.Buffer // intermediate read buffer
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

func (ts *TagSplitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	defer func() {
		ts.pos = 0
	}()
	if ts.done {
		return 0, nil, io.EOF
	}
	if atEOF {
		// we assume that we do not have to care about exact batch counts at
		// this point, but we can just return whatever is left
		ts.done = true
		if ts.scratch.Len() > 0 {
			_, _ = io.Copy(&ts.buf, &ts.scratch)
		}
		if len(data) == 0 && ts.buf.Len() == 0 {
			return 0, nil, nil
		} else {
			if _, err := ts.buf.Write(data); err != nil {
				return 0, nil, err
			}
			return len(data), ts.buf.Bytes(), nil
		}
	}
	// Possible cases. There may be iterations of split that do not return a
	// token at all.
	//
	// | start | end | action                             |
	// |-------|-----|------------------------------------|
	// | 0     | 0   | move on                            |
	// | 1     | 0   | put data in scratch buffer         |
	// | 0     | 1   | collect scratch and data (count++) |
	// | 1     | 1   | collect data (count++)             |
	// | N     | N   | ...                                |
	for {
		if ts.BatchSize == ts.count {
			b := ts.buf.Bytes()
			ts.buf.Reset()
			ts.count = 0
			return ts.pos, b, nil
		}
		if ts.in {
			// find closing tag
			v := bytes.Index(data[ts.pos:], ts.closing)
			if v == -1 {
				// we need to keep the data from the previous iteration around
				_, _ = ts.scratch.Write(data[ts.pos:])
				return len(data), nil, nil
			} else {
				// found a closing tag
				end := ts.pos + v + len(ts.closing)
				// we do not support nested tags of the same name just yet, so we fail
				if ts.openingTagCount(append(ts.scratch.Bytes(), data[ts.pos:end]...)) > 1 {
					return 0, nil, ErrNestedTagsWithSameNameNotImplemented
				}
				// if we have anything in the scratch buffer, add that first
				if ts.scratch.Len() > 0 {
					_, _ = io.Copy(&ts.buf, &ts.scratch)
					ts.scratch.Reset()
				}
				_, _ = ts.buf.Write(data[ts.pos:end])
				ts.pos = end
				ts.count++
				ts.in = false
			}
		} else {
			// find opening tag
			v := ts.openingTagIndex(data[ts.pos:])
			if v == -1 {
				return len(data), nil, nil
			} else {
				ts.pos = ts.pos + v
				ts.in = true
			}
		}
	}
}

// openingTagCount counts the number of opening tags found in data.
func (ts *TagSplitter) openingTagCount(data []byte) int {
	return bytes.Count(data, append(ts.opening, '>')) + bytes.Count(data, append(ts.opening, ' '))
}

// openingTagIndex returns the index of the opening tag in data, or -1.
func (ts *TagSplitter) openingTagIndex(data []byte) (index int) {
	index = bytes.Index(data, append(ts.opening, '>'))
	if index == -1 {
		// make sure, we do not have a prefix
		return bytes.Index(data, append(ts.opening, ' '))
	} else {
		return index
	}
}
