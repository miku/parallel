// Uppercases each line. Order of lines is not preserved.
//
//     $ echo hello | go run examples/uppercase.go
//     HELLO
//
//     $ time go run examples/uppercase.go < /some/9GB.file > throwaway
//
//     real    1m24.319s
//     user    2m3.868s
//     sys     0m16.632s
//
//     $ time go run examples/uppercase.go < /some/130G.file > throwaway
//
//     real     42m18.826s
//     user    114m52.623s
//     sys      25m23.491s

package main

import (
	"bytes"
	"log"
	"os"

	"github.com/miku/parallel"
)

func main() {
	p := parallel.NewProcessor(os.Stdin, os.Stdout, parallel.ToTransformerFunc(bytes.ToUpper))
	p.BatchSize = 100000
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
