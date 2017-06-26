// Example for a JSON filter.
//
//    $ make fixtures/large.ldj
//    ...
//    $ cat fixtures/large | go run examples/attrin.go
//    ...
//
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime"

	"github.com/miku/parallel"
)

// MarshalEnd marshals a value and appends a the given bytes at the end.
func MarshalEnd(v interface{}, end []byte) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return b, err
	}
	b = append(b, end...)
	return b, err
}

func main() {
	numWorkers := flag.Int("w", runtime.NumCPU(), "number of workers")
	flag.Parse()
	p := parallel.NewProcessor(os.Stdin, os.Stdout, func(b []byte) ([]byte, error) {
		// Ignore empty lines.
		if len(bytes.TrimSpace(b)) == 0 {
			return nil, nil
		}
		// Use an anonymous throwaway struct.
		var entry struct {
			Identifier int `json:"id"`
		}
		if err := json.Unmarshal(b, &entry); err != nil {
			return nil, err
		}
		// Keep documents which have even identifiers.
		if entry.Identifier%2 == 0 {
			return MarshalEnd(entry, []byte("\n"))
		}
		return nil, nil
	})
	if err := p.RunWorkers(*numWorkers); err != nil {
		log.Fatal(err)
	}
}
