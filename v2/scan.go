// Package parallel/v2 implements parallel batch processing of records (e.g.
// lines) read from a stream.
package parallel

import (
	"bufio"
	"io"
	"runtime"
	"sync"
)

const defaultBatchSize = 16777216

// Func is a generic processing function.
type Func func([]byte) ([]byte, error)

var blobPool = sync.Pool{
	New: func() any {
		b := make([]byte, defaultBatchSize, defaultBatchSize)
		return b
	},
}

// Result is a processing result. If Err is not nil, a processing error occured
// and B may be empty.
type Result struct {
	B   []byte
	Err error
}

func New(r io.Reader, w io.Writer, f Func) *Proc {
	proc := &Proc{r: r, w: w, f: f}
	proc.Size = defaultBatchSize // 16MB
	proc.NumWorkers = runtime.NumCPU()
	return proc
}

// Proc wraps a bufio.Scanner and a processing function and will process
// found tokens in parallel. All output will be written to a given writer.
type Proc struct {
	r io.Reader
	w io.Writer
	// f is a function that parses a blob of data an returns a blob of data.
	// This may already be a single item or a list of items. In the latter case
	// it is the task of the processing function to do further parsing
	f Func
	// Size is the batch size in bytes, default is 16MB, so with NumCPU number
	// of threads a 64 core machine would end up using about 1GB of RAM
	Size int
	// NumWorkers is the number of threads
	NumWorkers int

	// queue is the channel to pass batch of data to a worker
	queue chan []byte
	// resultC forwards results to a sink, Result will contain a result and any
	// error
	resultC chan Result
	// done signals completion of the sink processing
	done chan bool
	// wg will wait on all workers
	wg sync.WaitGroup
	// mu protects the error slice
	mu sync.Mutex
	// errors collects any error that happened during processing
	errors []error
}

// worker can process a blob of bytes with the given Func. If a processing
// function returns an error this worker will wind down.
func (p *Proc) worker(ctx context.Cancel) {
	defer p.wg.Done()
	select {
	case <-ctx.Done():
	case blob := <-p.queue:
		b, err := p.f(blob)
		r := Result{
			B:   b,
			Err: err,
		}
		p.resultC <- r
		if err != nil {
			p.mu.Lock()
			p.errors = append(p.errors, err)
			p.mu.Unlock()
		}
		blob = nil
		blobPool.Put(blob)
	default:
		break
	}

	for blob := range p.queue {
		b, err := p.f(blob)
		r := Result{
			B:   b,
			Err: err,
		}
		p.resultC <- r
		if err != nil {
			p.mu.Lock()
			p.errors = append(p.errors, err)
			p.mu.Unlock()
		}
		blob = nil
		blobPool.Put(blob)
	}
}

// writer collects results and writes it to the setup write.
func (p *Proc) writer() {
	for r := range p.resultC {
		if r.Err != nil {
			continue
		}
		_, _ = p.w.Write(r.B)
	}
	p.done <- true
}

// Run start the workers and begins reading and processing data.
func (p *Proc) Run() error {
	p.queue = make(chan []byte)
	p.resultC = make(chan Result)
	p.done = make(chan bool)
	go p.writer()
	p.wg.Add(p.NumWorkers)
	for i := 0; i < p.NumWorkers; i++ {
		go p.worker()
	}
	var (
		scanner = bufio.NewScanner(p.r)
		batch   = blobPool.Get().([]byte)
		i       int
	)
	for scanner.Scan() {
		b := scanner.Bytes()
		k := i + len(b)
		if k > len(batch) {
			p.queue <- batch[:i]
			batch = blobPool.Get().([]byte)
			i = 0
		}
		_ = copy(batch[i:], b)
		i = i + len(b)
		if len(p.errors) > 0 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		p.queue <- batch[:i]
	}
	close(p.queue)
	p.wg.Wait()
	close(p.resultC)
	<-p.done
	return nil
}
