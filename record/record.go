// Package scan accepts a bufio.SplitFunc and generalizes batches to non-line oriented input, e.g. XML.
package record

import (
	"bufio"
	"bytes"
	"io"
	"runtime"
	"sync"
)

// Processor can process lines in parallel.
type Processor struct {
	BatchSize  int
	SplitFunc  bufio.SplitFunc
	NumWorkers int
	Verbose    bool
	R          io.Reader
	W          io.Writer
	F          func([]byte) ([]byte, error)
}

// NewProcessor creates a new line processor.
func NewProcessor(r io.Reader, w io.Writer, f func([]byte) ([]byte, error)) *Processor {
	return &Processor{
		BatchSize:  100,
		NumWorkers: runtime.NumCPU(),
		R:          r,
		W:          w,
		F:          f,
	}
}

// Split set the SplitFunc to be used to identify records.
func (p *Processor) Split(f bufio.SplitFunc) {
	p.SplitFunc = f
}

// Run starts the workers, crunching through the input.
func (p *Processor) Run() error {
	// wErr signals a worker or writer error. If an error occurs, the items in
	// the queue are still process, just no items are added to the queue. There
	// is only one way to toggle this, from nil to non-nil, so we don't care
	// about synchronisation.
	var wErr error
	// worker takes []byte batches from a channel queue, executes f and sends
	// the result to the out channel.
	worker := func(queue chan []byte, out chan []byte, f func([]byte) ([]byte, error), wg *sync.WaitGroup) {
		defer wg.Done()
		for batch := range queue {
			r, err := f(batch)
			if err != nil {
				wErr = err
			}
			out <- r
		}
	}
	// writer buffers writes
	writer := func(w io.Writer, bc chan []byte, done chan bool) {
		bw := bufio.NewWriter(w)
		for b := range bc {
			if _, err := bw.Write(b); err != nil {
				wErr = err
			}
		}
		if err := bw.Flush(); err != nil {
			wErr = err
		}
		done <- true
	}
	var (
		queue = make(chan []byte)
		out   = make(chan []byte)
		done  = make(chan bool)
		wg    sync.WaitGroup
	)
	// start worker and writer goroutines
	go writer(p.W, out, done)
	wg.Add(p.NumWorkers)
	for i := 0; i < p.NumWorkers; i++ {
		go worker(queue, out, p.F, &wg)
	}
	// setup scanner with custom split function
	scanner := bufio.NewScanner(p.R)
	scanner.Split(p.SplitFunc)
	// batch and number of elements put into batch, we do not distinguish
	// items; could also limit the size; TODO
	var (
		buf bytes.Buffer
		i   int
	)
	for scanner.Scan() {
		if i == p.BatchSize {
			// To avoid checking on each loop, we only check for worker or
			// write errors here.
			if wErr != nil {
				break
			}
			b := make([]byte, buf.Len())
			copy(b, buf.Bytes())
			queue <- b
			buf.Reset()
			i = 0
		}
		buf.Write(scanner.Bytes())
		i++
	}
	queue <- buf.Bytes() // no other modification
	close(queue)
	wg.Wait()
	close(out)
	<-done
	return wErr
}
