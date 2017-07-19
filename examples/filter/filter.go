// Example for a JSON filter.
//
//    $ go run examples/attr.go
//    {"name":"B","id":2}
//    {"name":"D","id":4}
//
package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/miku/parallel"
)

var input = `
{"name": "A", "id": 1}
{"name": "B", "id": 2}
{"name": "C", "id": 3}
{"name": "D", "id": 4}
{"name": "E", "id": 5}
`

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
	r := strings.NewReader(input)
	p := parallel.NewProcessor(r, os.Stdout, func(b []byte) ([]byte, error) {
		// Use an anonymous throwaway struct.
		var entry struct {
			Name       string `json:"name"`
			Identifier int    `json:"id"`
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
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
