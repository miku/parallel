package parallel

import (
	"bufio"
	"io"
)

// Func is a generic processing function.
type Func func([]byte) ([]byte, error)

func New(r io.Reader, w io.Writer, f Func) *Proc {
	return &Proc{r: r, w: w, f: f}
}

// Proc wraps a bufio.Scanner and a processing function and will process
// found tokens in parallel. All output will be written to a given writer.
type Proc struct {
	r          io.Reader
	s          *bufio.Scanner
	w          io.Writer
	f          Func
	Size       int
	NumWorkers int
}

func (p *Proc) Run() error {
	// setup workers
	return nil
}
