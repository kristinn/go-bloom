package bloom

import (
	"github.com/kristinn/redigo/redis"
)

// redisStorage is a struct representing the Redis backend for the bloom filter.
type redisStorage struct {
	pool  *redis.Pool
	key   string
	size  uint
	queue []uint
}

// NewRedisStorage creates a Redis backend storage to be used with the bloom filter.
func NewRedisStorage(pool *redis.Pool, key string, size uint) (*redisStorage, error) {
	var err error

	store := redisStorage{pool, key, size, make([]uint, 0)}

	conn := store.pool.Get()
	defer conn.Close()
	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return &store, err
	}

	if !exists {
		if err := store.init(); err != nil {
			return &store, err
		}
	}

	return &store, nil
}

// init takes care of settings every bit to 0 in the Redis bitset.
func (s *redisStorage) init() (err error) {
	conn := s.pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	var i uint
	for i = 0; i < s.size; i++ {
		conn.Send("SETBIT", s.key, i, 0)
	}

	_, err = conn.Do("EXEC")

	return
}

// Append appends the bit, which is to be saved, to the queue.
func (s *redisStorage) Append(bit uint) {
	s.queue = append(s.queue, bit)
}

// Save pushes the bits from the queue to the storage backend, assigning the value 1 in the process.
func (s *redisStorage) Save() {
	conn := s.pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	for _, bit := range s.queue {
		conn.Send("SETBIT", s.key, bit, 1)
	}

	conn.Do("EXEC")
}

// Exists checks if the given bit exists in the Redis backend.
func (s *redisStorage) Exists(bit uint) (ret bool, err error) {
	conn := s.pool.Get()
	defer conn.Close()

	bitValue, err := redis.Int(conn.Do("GETBIT", s.key, bit))
	if err != nil {
		return
	}
	return bitValue == 1, err
}
