package goworker

import (
	"errors"
	"net/url"
	"time"

	"github.com/fzzy/radix/redis"
	"github.com/youtube/vitess/go/pools"
)

var (
	errorInvalidScheme = errors.New("Invalid Redis database URI scheme.")
	pool               *pools.ResourcePool
)

type redisConn struct {
	Client *redis.Client
}

func (r *redisConn) Cmd(cmd string, args ...interface{}) *redis.Reply {
	return r.Client.Cmd(cmd, args...)
}

func (r *redisConn) Close() {
	r.Client.Close()
}

func newRedisFactory(uri string) pools.Factory {
	return func() (pools.Resource, error) {
		return redisConnFromUri(uri)
	}
}

func newRedisPool(uri string, capacity int, maxCapacity int, idleTimout time.Duration) *pools.ResourcePool {
	return pools.NewResourcePool(newRedisFactory(uri), capacity, maxCapacity, idleTimout)
}

func redisConnFromUri(uriString string) (*redisConn, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, err
	}

	var network string
	var host string
	var password string
	var db string

	switch uri.Scheme {
	case "redis":
		network = "tcp"
		host = uri.Host
		if uri.User != nil {
			password, _ = uri.User.Password()
		}
		if len(uri.Path) > 1 {
			db = uri.Path[1:]
		}
	case "unix":
		network = "unix"
		host = uri.Path
	default:
		return nil, errorInvalidScheme
	}

	conn, err := redis.Dial(network, host)
	if err != nil {
		return nil, err
	}

	if password != "" {
		if conn.Cmd("AUTH", password).Err != nil {
			conn.Close()
			return nil, err
		}
	}

	if db != "" {
		if conn.Cmd("SELECT", db).Err != nil {
			conn.Close()
			return nil, err
		}
	}

	return &redisConn{conn}, nil
}
