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

// TODO: Finish.
func TestRedisSave(t *testing.T) {
	pool := redis.NewPool(newRedisConnection, 5)

	_, err := NewRedis(pool, "redis-save-test", 15000, 7)
	if err != nil {
		t.Fatal(err)
	}
}
