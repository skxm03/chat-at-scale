package redis

import (
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrEmptyAddr       = errors.New("redis address cannot be empty")
	ErrInvalidPoolSize = errors.New("redis pool size must be greater than 0")
	ErrInvalidDB       = errors.New("redis db index cannot be negative")
)

// Config holds redis connection parameters and pool configuration.
type Config struct {
	Addr            string
	Password        string
	DB              int
	PoolSize        int
	MinIdleConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
}

// DefaultConfig provides recommended production defaults for high-throughput connection pooling.
func DefaultConfig() Config {
	return Config{
		Addr:            "localhost:6379",
		Password:        "",
		DB:              0,
		PoolSize:        50,
		MinIdleConns:    10,
		MaxIdleConns:    25,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
		DialTimeout:     3 * time.Second,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
		PoolTimeout:     4 * time.Second,
	}
}

// Validate checks configuration sanity.
func (c Config) Validate() error {
	if c.Addr == "" {
		return ErrEmptyAddr
	}
	if c.PoolSize <= 0 {
		return ErrInvalidPoolSize
	}
	if c.DB < 0 {
		return ErrInvalidDB
	}
	return nil
}

// ToOptions converts the Config into go-redis Options.
func (c Config) ToOptions() *goredis.Options {
	return &goredis.Options{
		Addr:            c.Addr,
		Password:        c.Password,
		DB:              c.DB,
		PoolSize:        c.PoolSize,
		MinIdleConns:    c.MinIdleConns,
		MaxIdleConns:    c.MaxIdleConns,
		ConnMaxIdleTime: c.ConnMaxIdleTime,
		ConnMaxLifetime: c.ConnMaxLifetime,
		DialTimeout:     c.DialTimeout,
		ReadTimeout:     c.ReadTimeout,
		WriteTimeout:    c.WriteTimeout,
		PoolTimeout:     c.PoolTimeout,
	}
}

