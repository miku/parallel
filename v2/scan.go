package parallel

import (
	"bufio"
	"io"
	"runtime"
	"sync"
)

// Func is a generic processing function.
type Func func([]byte) ([]byte, error)

// Result is a processing result. If Err is not nil, a processing error occured
// and B may be empty.
type Result struct {
	B   []byte
	Err error
}

func New(r io.Reader, w io.Writer, f Func) *Proc {
	proc := &Proc{r: r, w: w, f: f}
	proc.Size = 16777216
	proc.NumWorkers = runtime.NumCPU()
	return proc
}

// Proc wraps a bufio.Scanner and a processing function and will process
// found tokens in parallel. All output will be written to a given writer.
type Proc struct {
	r          io.Reader
	w          io.Writer
	f          Func
	Size       int
	NumWorkers int
	batch      chan []byte
	resultC    chan Result
	done       chan bool
	wg         sync.WaitGroup
}

func (p *Proc) worker(queue chan []byte, resultC chan Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for blob := range queue {
		b, err := p.f(blob)
		r := Result{B: b, Err: err}
		resultC <- r
	}
}

func (p *P) writer(resultC chan Result, done chan bool) {
	for blob := range resultC {
		p.w.Write(blob)
	}
}

func (p *Proc) Run() error {
	p.queue = make(chan []byte)
	p.resultC = make(chan []byte)
	p.done = make(chan bool)

	for i := 0; i < p.NumWorkers; i++ {
		p.wg.Add(1)
		go worker(queue, resultC, &wg)
	}

	var (
		scanner = bufio.NewScanner(p.r)
		batch   = make([]byte, p.Size)
		i       int
	)
	for scanner.Scan() {
		b := scanner.Bytes()
		k := i + len(b)
		if k < len(batch) {
			_ = copy(batch[i:], b)
		} else {
			// pass to goroutine
		}
		i = i + len(b)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
