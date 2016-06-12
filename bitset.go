package bloom

import (
	"github.com/willf/bitset"
)

// bitsetStorage is a struct representing the Bitset backend for the bloom filter.
type bitsetStorage struct {
	store *bitset.BitSet
	queue []uint
	size  uint
}

// NewBitsetStorage creates a Bitset backend storage to be used with the bloom filter.
func NewBitsetStorage(size uint) *bitsetStorage {
	b := make([]uint, 0)
	return &bitsetStorage{bitset.New(size), b, size}
}

// Append appends the bit, which is to be saved, to the queue.
func (s *bitsetStorage) Append(bit uint) {
	s.queue = append(s.queue, bit)
}

// Save pushes the bits from the queue to the storage backend, assigning the value 1 in the process.
func (s *bitsetStorage) Save() {
	for _, bit := range s.queue {
		s.store.Set(bit)
	}
}

// Exists checks if the given bit exists in the Bitset backend.
func (s *bitsetStorage) Exists(bit uint) (ret bool, err error) {
	ret = s.store.Test(bit)

	return
}
