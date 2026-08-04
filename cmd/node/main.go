// Command node runs a single store process.
//
// It serves the whole Part 1 API. Part 2 adds exactly three things: a node id, a
// partition range it will accept, and GET /kv to list its own keys. Nothing else
// about the node changed when the system became a cluster, because each key
// lives on exactly one node — so per-key atomicity stayed local and sharding
// needed no new concurrency code.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"codakv/internal/config"
	"codakv/internal/node"
	"codakv/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadNode()
	if err != nil {
		return err
	}

	kv, err := store.New(cfg.PartitionCount)
	if err != nil {
		return err
	}

	svc := node.NewService(kv, node.Config{
		NodeID:         cfg.NodeID,
		PartitionCount: cfg.PartitionCount,
		Owned:          cfg.Owned,
		MaxValueBytes:  cfg.MaxValueBytes,
		MaxKeyBytes:    cfg.MaxKeyBytes,
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           node.NewHandler(svc, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// No WriteTimeout: GET /kv streams an unbounded keyspace, and a deadline
		// here would truncate a long listing mid-stream.
	}

	log.Info("node starting",
		"node", cfg.NodeID,
		"addr", cfg.ListenAddr,
		"partitions", cfg.PartitionCount,
		"owned", cfg.Owned.String(),
		"maxValueBytes", cfg.MaxValueBytes,
		"maxKeyBytes", cfg.MaxKeyBytes,
	)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		log.Info("shutting down", "signal", sig.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
