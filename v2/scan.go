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
	proc.Size = 16777216 // 16MB
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
	queue      chan []byte
	resultC    chan Result
	done       chan bool
	wg         sync.WaitGroup
}

// worker can process a blob of bytes with the given Func.
func (p *Proc) worker() {
	defer p.wg.Done()
	for blob := range p.queue {
		// TODO: a fast line splitter, with SWAR, then apply F on each line
		b, err := p.f(blob)
		r := Result{
			B:   b,
			Err: err,
		}
		p.resultC <- r
		if err != nil {
			break
		}
		blobPool.Put(blob)
	}
}

// writer collects results and writes it to the setup write.
func (p *Proc) writer() {
	for blob := range p.resultC {
		p.w.Write(blob.B)
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
		if k > cap(batch) {
			p.queue <- batch[:i]
			batch = blobPool.Get().([]byte)
			batch = batch[:0]
			i = 0
		}
		_ = copy(batch[i:], b)
		i = i + len(b)
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
