/*
Bloom filter with Bitset and Redis backend support.

Speeding things up with:

- Partitioned bloom filters

- Utilizing the same cheap hash function every time, but still with good results (http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/rsa.pdf)
*/
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

// bloomFilter holds all the storage filters.
type bloomFilter struct {
	filters []filter
}

// filter represents each and every storage filter. Each hash iteration (k) = 1 storage filter.
type filter struct {
	size       uint
	storage    storage
	hasher     hash.Hash64
	multiplier uint
}

// NewBitset creates and returns a new bloom filter using Bitset as a backend.
func NewBitset(size, hashIter uint) *bloomFilter {
	filters := filterSetup(size, hashIter)

	for index, filter := range filters {
		filter.storage = NewBitsetStorage(filter.size)
		filters[index] = filter
	}

	return &bloomFilter{filters}
}

// NewRedis creates and returns a new bloom filter using Redis as a backend.
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

// filterSetup is a helper function to generate the required number of filters (hash iterations -> k).
func filterSetup(size, hashIter uint) (filters []filter) {
	partitionSize := math.Ceil(float64(size) / float64(hashIter))

	var k uint
	hasher := fnv.New64()
	for k = 0; k < hashIter; k++ {
		filters = append(filters, filter{uint(partitionSize), nil, hasher, k + 1})
	}

	return
}

// Append is used to append a value to the queue.
func (b *bloomFilter) Append(value []byte) {
	for _, f := range b.filters {
		a, b := f.hashValue(&value)
		f.storage.Append((a + b*f.multiplier) % f.size)
	}
}

// Save takes care of saving the values from the queue to the correct backend.
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

// Exists checks if the given value is in the bloom filter or not. False positives might occur.
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

// hashValue takes care of hashing the value that is being stored in the bloom filter.
func (f *filter) hashValue(value *[]byte) (a, b uint) {
	f.hasher.Reset()
	f.hasher.Write(*value)
	sum := f.hasher.Sum(nil)

	a = uint(binary.BigEndian.Uint32(sum[0:4]))
	b = uint(binary.BigEndian.Uint32(sum[4:8]))

	return
}
