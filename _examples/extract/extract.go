// Extract a value from a JSON document. Toy example, so top-level keys only.
// Sifting through 2M documents (9GB) takes about 150s. The ubertool jq takes 321s.
//
//    $ cat some.json | go run examples/value.go -key somekey
//    key1
//    key2
//    ...
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

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
	key := flag.String("key", "", "extract values for this key")
	flag.Parse()

	p := parallel.NewProcessor(os.Stdin, os.Stdout, func(b []byte) ([]byte, error) {
		// Unmarshal into generic map.
		m := make(map[string]interface{})
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		v, ok := m[*key]
		if ok {
			switch val := v.(type) {
			case string:
				result := []byte(val)
				result = append(result, '\n')
				return result, nil
			default:
				result := []byte(fmt.Sprintf("%v", val))
				result = append(result, '\n')
				return result, nil
			}
		}
		return nil, nil
	})
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
