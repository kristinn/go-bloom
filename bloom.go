package bloom

import (
	"fmt"
	"github.com/kristinn/redigo/redis"
	"hash"
	"hash/fnv"
	"math"
)

type bloomFilter struct {
	filters []filter
}

type filter struct {
	size       uint
	storage    interface{}
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
	for k = 0; k < hashIter; k++ {
		filters = append(filters, filter{uint(partitionSize), nil, fnv.New64(), k + 1})
	}

	return
}
