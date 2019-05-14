# parallel

Process lines in parallel.

This package helps to increase the performance of command line applications,
that transform data and where data is read in a mostly line orientied fashion.

Note: The *order* of the input lines is not preserved in the output.

The main type is a
[parallel.Processor](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/processor.go#L68-L76),
which reads from an [io.Reader](https://golang.org/pkg/io/#Reader), applies a
function to each input line (separated by a newline by default) and writes the
result to an [io.Writer](https://golang.org/pkg/io/#Writer).

The [transformation function](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/processor.go#L56-L58) takes a byte slice and therefore does not assume
any specific format, so the input may be plain lines, CSV, newline delimited
JSON or similar line oriented formats. The output is just bytes and can again
assume any format.

An example for a simple transformation that does nothing:

```go
func Noop(b []byte) ([]byte, error) {
	return b, nil
}
```

We can connect this function to IO and let it run:

```go
p := parallel.NewProcessor(os.Stdin, os.Stdout, Noop)
if err := p.Run(); err != nil {
	log.Fatal(err)
}
```

That's all the setup needed. For details and self contained programs, see [examples](https://github.com/miku/parallel/tree/master/examples).

The processer expects a
[parallel.TransformerFunc](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/processor.go#L56-L58).
There are some functions, that take a byte slice and and return a byte slice,
but do not return an error (an example would be [bytes.ToUpper](https://golang.org/pkg/bytes/#ToUpper)). These functions can be turned into a TransformerFunc with a simple [helper](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/processor.go#L60-L66):

```go
p := parallel.NewProcessor(os.Stdin, os.Stdout, parallel.ToTransformerFunc(bytes.ToUpper))
if err := p.Run(); err != nil {
	log.Fatal(err)
}
```

## Full Example

```go
// Uppercases each line. Order of lines is not preserved.
//
//     $ printf "hello\nhi\n" | go run examples/uppercase.go
//     HELLO
//     HI

package main

import (
	"bytes"
	"log"
	"os"

	"github.com/miku/parallel"
)

func main() {
	// Setup input, output and business logic.
	p := parallel.NewProcessor(os.Stdin, os.Stdout, func(b []byte) ([]byte, error) {
		return bytes.ToUpper(b), nil
	})

	// Start processing with parallel workers.
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
```

* More examples: https://github.com/miku/parallel/tree/master/examples

# Adjusting the processor

The processor has a few attributes, that can be adjusted prior to running:

```go
p := parallel.NewProcessor(os.Stdin, os.Stdout, parallel.ToTransformerFunc(bytes.ToUpper))

// Adjust processor options.
p.NumWorkers = 4          // number of workers (default to runtime.NumCPU())
p.BatchSize = 10000       // how many records to batch, before sending to a worker
p.RecordSeparator = '\n'  // record separator (must be a byte at the moment)

if err := p.Run(); err != nil {
	log.Fatal(err)
}
```

The default should be ok for a lot of use cases. Batches are kept in memory, so
higher batch sizes will need more memory but will decrease the coordination
overhead. Sometimes, a batch size of one can be [useful
too](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/examples/fetchall.go#L166).

# Random performance data point

Combining parallel with a fast JSON library, such as
[jsoniter](https://github.com/json-iterator/go), one can process up to 100000
JSON documents (of about 1K in size) per second. Here is an [example
snippet](https://gist.github.com/miku/62f64de2016dc38186e21270715e8016#file-main-go).
