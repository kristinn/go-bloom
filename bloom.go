package bloom

import (
	"encoding/binary"
	"fmt"
	"github.com/kristinn/redigo/redis"
	"hash"
	"hash/fnv"
	"math"
	"sync"
)

type bloomFilter struct {
	filters []filter
}

type filter struct {
	size       uint
	storage    storage
	hasher     hash.Hash64
	multiplier uint
}

func NewBitset(size, hashIter uint) *bloomFilter {
	filters := filterSetup(size, hashIter)

	for index, filter := range filters {
		filter.storage = NewBitsetStorage(filter.size)
		filters[index] = filter
	}

	return &bloomFilter{filters}
}

func NewRedis(pool *redis.Pool, key string, size, hashIter uint) (*bloomFilter, error) {
	filters := filterSetup(size, hashIter)

	bloom := bloomFilter{filters}

	var err error
	for index, filter := range bloom.filters {
		filter.storage, err = NewRedisStorage(pool, fmt.Sprintf("%s.%d", key, filter.multiplier), filter.size)
		if err != nil {
			return &bloom, err
		}
		bloom.filters[index] = filter
	}

	return &bloom, nil
}

func filterSetup(size, hashIter uint) (filters []filter) {
	partitionSize := math.Ceil(float64(size) / float64(hashIter))

	var k uint
	hasher := fnv.New64()
	for k = 0; k < hashIter; k++ {
		filters = append(filters, filter{uint(partitionSize), nil, hasher, k + 1})
	}

	return
}

func (b *bloomFilter) Append(value []byte) {
	for _, f := range b.filters {
		a, b := f.hashValue(&value)
		f.storage.Append((a + b*f.multiplier) % f.size)
	}
}

func (b *bloomFilter) Save() {
	var wg sync.WaitGroup
	for _, f := range b.filters {
		wg.Add(1)
		go func(f filter) {
			defer wg.Done()

			f.storage.Save()
		}(f)
	}

	wg.Wait()
}

func (b *bloomFilter) Exists(value []byte) (exists bool, err error) {
	for _, f := range b.filters {
		a, b := f.hashValue(&value)
		exists, err = f.storage.Exists((a + b*f.multiplier) % f.size)
		if !exists {
			return
		}
	}

	exists = true
	return
}

func (f *filter) hashValue(value *[]byte) (a, b uint) {
	f.hasher.Reset()
	f.hasher.Write(*value)
	sum := f.hasher.Sum(nil)

	a = uint(binary.BigEndian.Uint32(sum[0:4]))
	b = uint(binary.BigEndian.Uint32(sum[4:8]))

	return
}
