package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	PoolSize    int32

	Lease     time.Duration
	Heartbeat time.Duration
	Reap      time.Duration
	Poll      time.Duration

	JobMin time.Duration
	JobMax time.Duration

	DisableFencing bool
	UnsafeClaim    bool
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("bad %s=%q: %v", key, v, err)
	}

	return n
}

func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("bad %s=%q: %v", key, v, err)
	}

	return d
}

func envBool(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func Load() Config {
	c := Config{
		DatabaseURL:    env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/jobs"),
		ListenAddr:     env("LISTEN_ADDR", ":8080"),
		PoolSize:       int32(envInt("POOL_SIZE", 0)),
		Lease:          envDur("LEASE", 10*time.Second),
		Heartbeat:      envDur("HEARTBEAT", 3*time.Second),
		Reap:           envDur("REAP_INTERVAL", 2*time.Second),
		Poll:           envDur("POLL_INTERVAL", 200*time.Millisecond),
		JobMin:         envDur("JOB_MIN", 50*time.Millisecond),
		JobMax:         envDur("JOB_MAX", 150*time.Millisecond),
		DisableFencing: envBool("DISABLE_FENCING"),
		UnsafeClaim:    envBool("UNSAFE_CLAIM"),
	}

	if c.Heartbeat <= 0 || c.Heartbeat >= c.Lease {
		log.Fatal("HEARTBEAT must be positive and smaller than LEASE")
	}

	if c.Lease <= 0 || c.JobMin < 0 || c.JobMax < c.JobMin ||
		c.Poll <= 0 || c.Reap <= 0 {
		log.Fatal("invalid duration configuration")
	}

	return c
}
