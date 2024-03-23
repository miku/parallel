package parallel

import (
	"reflect"
	"testing"
)

func TestBytesBatch(t *testing.T) {
	bb := NewBytesBatch()
	if bb.Size() != 0 {
		t.Fatalf("got %d, want 0", bb.Size())
	}
	bb.Add([]byte("."))
	if bb.Size() != 1 {
		t.Fatalf("got %d, want 1", bb.Size())
	}
	bb.Add([]byte("."))
	expected := [][]byte{
		[]byte("."),
		[]byte("."),
	}
	if !reflect.DeepEqual(expected, bb.Slice()) {
		t.Fatalf("got %v, want %v", bb.Slice(), expected)
	}
}
