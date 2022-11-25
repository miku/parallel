// Package parallel implements helpers for fast processing of line oriented inputs.
package parallel

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"runtime"
	"sync"
	"time"
)

// Version of library.
const Version = "0.1.0"

// BytesBatch is a slice of byte slices.
type BytesBatch struct {
	b [][]byte
}

// NewBytesBatch creates a new BytesBatch with a given capacity.
func NewBytesBatch() *BytesBatch {
	return NewBytesBatchCapacity(0)
}

// NewBytesBatchCapacity creates a new BytesBatch with a given capacity.
func NewBytesBatchCapacity(cap int) *BytesBatch {
	return &BytesBatch{b: make([][]byte, 0, cap)}
}

// Add adds an element to the batch.
func (bb *BytesBatch) Add(b []byte) {
	bb.b = append(bb.b, b)
}

// Reset empties this batch.
func (bb *BytesBatch) Reset() {
	bb.b = nil
}

// Size returns the number of elements in the batch.
func (bb *BytesBatch) Size() int {
	return len(bb.b)
}

// Slice returns a slice of byte slices.
func (bb *BytesBatch) Slice() [][]byte {
	b := make([][]byte, len(bb.b))
	for i := 0; i < len(bb.b); i++ {
		b[i] = bb.b[i]
	}
	return b
}

// SimpleTransformerFunc converts bytes to bytes.
type SimpleTransformerFunc func([]byte) []byte

// TransformerFunc takes a slice of bytes and returns a slice of bytes and a
// an error. A common denominator of functions that transform data.
type TransformerFunc func([]byte) ([]byte, error)

// ToTransformerFunc takes a simple transformer and wraps it so it can be used in
// places where a TransformerFunc is expected.
func ToTransformerFunc(f SimpleTransformerFunc) TransformerFunc {
	return func(b []byte) ([]byte, error) {
		return f(b), nil
	}
}

// Processor can process lines in parallel.
type Processor struct {
	BatchSize       int
	RecordSeparator byte
	NumWorkers      int
	SkipEmptyLines  bool
	Verbose         bool
	R               io.Reader
	W               io.Writer
	F               TransformerFunc
}

// New is a preferred way to create a new parallel processor.
var New = NewProcessor

// NewProcessor creates a new line processor.
func NewProcessor(r io.Reader, w io.Writer, f TransformerFunc) *Processor {
	return &Processor{
		BatchSize:       10000,
		RecordSeparator: '\n',
		NumWorkers:      runtime.NumCPU(),
		SkipEmptyLines:  true,
		R:               r,
		W:               w,
		F:               f,
	}
}

// RunWorkers allows to quickly set the number of workers.
func (p *Processor) RunWorkers(numWorkers int) error {
	p.NumWorkers = numWorkers
	return p.Run()
}

// Run starts the workers, crunching through the input.
func (p *Processor) Run() error {
	// wErr signals a worker or writer error. If an error occurs, the items in
	// the queue are still process, just no items are added to the queue. There
	// is only one way to toggle this, from false to true, so we don't care
	// about synchronisation.
	var wErr error
	// worker takes []byte batches from a channel queue, executes f and sends the result to the out channel.
	worker := func(queue chan [][]byte, out chan []byte, f TransformerFunc, wg *sync.WaitGroup) {
		defer wg.Done()
		for batch := range queue {
			for _, b := range batch {
				r, err := f(b)
				if err != nil {
					wErr = err
				}
				out <- r
			}
		}
	}
	// writer buffers writes.
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
		queue   = make(chan [][]byte)
		out     = make(chan []byte)
		done    = make(chan bool)
		total   int64
		started = time.Now()
	)
	var wg sync.WaitGroup
	go writer(p.W, out, done)
	for i := 0; i < p.NumWorkers; i++ {
		wg.Add(1)
		go worker(queue, out, p.F, &wg)
	}
	batch := NewBytesBatchCapacity(p.BatchSize)
	br := bufio.NewReader(p.R)
	for {
		b, err := br.ReadBytes(p.RecordSeparator)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(b)) == 0 && p.SkipEmptyLines {
			continue
		}
		batch.Add(b)
		if batch.Size() == p.BatchSize {
			if p.Verbose {
				log.Printf("parallel: dispatched %d lines (%0.2f lines/s)", total, float64(total)/time.Since(started).Seconds())
			}
			total += int64(p.BatchSize)
			// To avoid checking on each loop, we only check for worker or write errors here.
			if wErr != nil {
				break
			}
			queue <- batch.Slice()
			batch.Reset()
		}
	}
	queue <- batch.Slice()
	batch.Reset()
	close(queue)
	wg.Wait()
	close(out)
	<-done
	return wErr
}
