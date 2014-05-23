package bloom

import (
	"github.com/kristinn/redigo/redis"
	"testing"
)

func newRedisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", ":6379")
}

func TestRedisInit(t *testing.T) {
	pool := redis.NewPool(newRedisConnection, 5)

	_, err := NewRedis(pool, "redis-init-test", 15000, 7)
	if err != nil {
		t.Fatal(err)
	}

	conn := pool.Get()
	defer conn.Close()

	conn.Do("FLUSHALL")
}

func TestRedisSave(t *testing.T) {
	pool := redis.NewPool(newRedisConnection, 5)

	r, err := NewRedis(pool, "redis-save-test", 15000, 7)
	if err != nil {
		t.Fatal(err)
	}

	r.Append([]byte("afi"))
	r.Save()

	exists, err := r.Exists([]byte("afi"))
	if !exists {
		t.Fatal("afi should exist in the Redis backend")
	}
	if err != nil {
		t.Fatal(err)
	}

	exists, err = r.Exists([]byte("amma"))
	if exists {
		t.Fatal("amma shouldn't exist in the Redis backend")
	}
	if err != nil {
		t.Fatal(err)
	}

	conn := pool.Get()
	defer conn.Close()

	conn.Do("FLUSHALL")
}

func TestBitsetSave(t *testing.T) {
	b := NewBitset(15000, 7)

	b.Append([]byte("afi"))
	b.Save()

	exists, err := b.Exists([]byte("afi"))
	if !exists {
		t.Fatal("afi should exist in the Bitset backend")
	}
	if err != nil {
		t.Fatal(err)
	}

	exists, err = b.Exists([]byte("amma"))
	if exists {
		t.Fatal("amma shouldn't exist in the Bitset backend")
	}
	if err != nil {
		t.Fatal(err)
	}
}
