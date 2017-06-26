SHELL = /bin/bash

uppercase: examples/uppercase.go
	go build -o $@ $<

fixtures/large.ldj:
	python fixtures/large.py > $@

clean:
	rm -f uppercase
	rm -f fixtures/large.ldj
