package bloom

import (
	"github.com/kristinn/redigo/redis"
)

type redisStorage struct {
	pool  *redis.Pool
	key   string
	size  uint
	queue []uint
}

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

func (s *redisStorage) Append(bit uint) {
	s.queue = append(s.queue, bit)
}

func (s *redisStorage) Save() {
	conn := s.pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	for _, bit := range s.queue {
		conn.Send("SETBIT", s.key, bit, 1)
	}

	conn.Do("EXEC")
}

func (s *redisStorage) Exists(bit uint) (ret bool, err error) {
	conn := s.pool.Get()
	defer conn.Close()

	bitValue, err := redis.Int(conn.Do("GETBIT", s.key, bit))
	if err != nil {
		return
	}
	return bitValue == 1, err
}
