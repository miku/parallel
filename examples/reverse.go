// reverses lines.
//
//     $ echo "hello" | go run examples/reverse.go
//     olleh
package main

import (
	"bytes"
	"log"
	"os"

	"github.com/miku/parallel"
)

// reverse reverses the bytes in a slice in place. Assumes values end with a newline.
func reverse(b []byte) ([]byte, error) {
	b = bytes.TrimSpace(b)
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	b = append(b, '\n')
	return b, nil
}

func main() {
	p := parallel.NewProcessor(os.Stdin, os.Stdout, reverse)
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
