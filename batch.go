package parallel

// BytesBatch is a slice of byte slices.
type BytesBatch struct {
	b [][]byte
}

// NewBytesBatch creates a new BytesBatch with a given capacity.
func NewBytesBatch() *BytesBatch {
	return NewBytesBatchCapacity(0)
}

// NewBytesBatchCapacity creates a new BytesBatch with a given capacity.
func NewBytesBatchCapacity(cap int) *BytesBatch {
	return &BytesBatch{b: make([][]byte, 0, cap)}
}

// Add adds an element to the batch.
func (bb *BytesBatch) Add(b []byte) {
	bb.b = append(bb.b, b)
}

// Reset empties this batch.
func (bb *BytesBatch) Reset() {
	bb.b = nil
}

// Size returns the number of elements in the batch.
func (bb *BytesBatch) Size() int {
	return len(bb.b)
}

// Slice returns a slice of byte slices.
func (bb *BytesBatch) Slice() [][]byte {
	b := make([][]byte, len(bb.b))
	for i := 0; i < len(bb.b); i++ {
		b[i] = bb.b[i]
	}
	return b
}
