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

func (s *bitsetStorage) Append(bit uint) {
	s.queue = append(s.queue, bit)
}

func (s *bitsetStorage) Save() {
	for _, bit := range s.queue {
		s.store.Set(bit)
	}
}

func (s *bitsetStorage) Exists(bit uint) (ret bool, err error) {
	ret = s.store.Test(bit)

	return
}
