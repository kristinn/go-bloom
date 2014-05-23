package bloom

import (
	"github.com/kristinn/bitset"
)

type bitsetStorage struct {
	store *bitset.BitSet
	queue []uint
	size  uint
}

func NewBitsetStorage(size uint) *bitsetStorage {
	b := make([]uint, 0)
	return &bitsetStorage{bitset.New(size), b, size}
}

func (s *bitsetStorage) Append(bitLocations *[]uint) {
	s.queue = append(s.queue, *bitLocations...)
}

func (s *bitsetStorage) Save() {
	for _, bit := range s.queue {
		s.store.Set(bit)
	}
}
