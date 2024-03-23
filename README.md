# parallel

Process lines or records in parallel.

This package helps to increase the performance of command line filters, that
transform data and where data is read in a line or record oriented fashion.

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

An example for the identity transform:

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

That's all the setup needed. For details and self contained programs, see
[examples](https://github.com/miku/parallel/tree/master/examples).

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

The defaults should work for most cases. Batches are kept in memory, so
higher batch sizes will need more memory but will decrease the coordination
overhead. Sometimes, a batch size of one can be [useful
too](https://github.com/miku/parallel/blob/fa00b8c221050cc7a84a666f124c9a8c9f0cd471/examples/fetchall.go#L166).

# Record support

It is possible to parallelize record oriented data, too. There is a
[record.Processor](https://github.com/miku/parallel/blob/11f067737e71ef854339f14b25b83c2194234311/record/record.go#L25-L35)
additionally takes a
[Split](https://github.com/miku/parallel/blob/11f067737e71ef854339f14b25b83c2194234311/record/record.go#L37-L40)
function, that is passed internally to a
[bufio.Scanner](https://pkg.go.dev/bufio#Scanner), which will parse the input
and will concatenate a number of records into a batch, which is then passed to
the conversion function.

The [bufio](https://pkg.go.dev/bufio) package contains a number of split
functions, like [ScanWords](https://pkg.go.dev/bufio#ScanWords) and others.
Originally, we implemented record support for fast XML processing. For that, we
added a
[TagSplitter](https://github.com/miku/parallel/blob/11f067737e71ef854339f14b25b83c2194234311/record/split.go#L28-L55)
which can split input on XML tags.


# Random performance data point

Combining parallel with a fast JSON library, such as
[jsoniter](https://github.com/json-iterator/go), one can process up to 100000
JSON documents (of about 1K in size) per second. Here is an [example
snippet](https://gist.github.com/miku/62f64de2016dc38186e21270715e8016#file-main-go).
