SHELL = /bin/bash

.PHONY: test
test:
	go test -cover ./...

uppercase: examples/uppercase/uppercase.go
	go build -o $@ $<

fixtures/large.ldj:
	python fixtures/large.py > $@

.PHONY: clean
clean:
	rm -f uppercase
	rm -f fixtures/large.ldj
