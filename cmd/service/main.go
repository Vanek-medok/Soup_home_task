package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"job-queue/internal/api"
	"job-queue/internal/config"
	"job-queue/internal/database"
	"job-queue/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("service stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: service api|worker")
	}

	mode := os.Args[1]

	if mode != "api" && mode != "worker" {
		return fmt.Errorf("unknown mode: %s", mode)
	}

	cfg := config.Load()

	if cfg.PoolSize == 0 {
		if mode == "api" {
			cfg.PoolSize = 20
		} else {
			cfg.PoolSize = 4
		}
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stop()

	pool, err := database.Connect(
		ctx,
		cfg.DatabaseURL,
		cfg.PoolSize,
	)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	if cfg.DisableFencing {
		log.Println("WARNING: DISABLE_FENCING enabled")
	}

	if cfg.UnsafeClaim {
		log.Println("WARNING: UNSAFE_CLAIM enabled")
	}

	switch mode {
	case "api":
		return api.Run(ctx, cfg, pool)

	case "worker":
		worker.Run(ctx, cfg, pool)
	}

	return nil
}
