// Command proxy runs the stateless router in front of the store nodes.
//
// It holds a routing table and nothing else — no keys, no locks, no versions —
// which is why it can be replicated freely and why losing one costs only the
// requests in flight.
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
	"codakv/internal/proxy"
	"codakv/internal/routing"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadProxy()
	if err != nil {
		return err
	}

	// The same routing.RangeFor the nodes use, so neither side can drift: a node
	// only needs to know which slot it occupies, never where its peers are.
	table, err := routing.NewTable(cfg.NodeIDs, cfg.PartitionCount)
	if err != nil {
		return err
	}

	caller := proxy.NewCaller(cfg.Addresses, cfg.PartitionCount, cfg.RequestTimeout)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           proxy.NewHandler(table, caller, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// No WriteTimeout: GET /kv streams an unbounded keyspace and a deadline
		// would truncate a long listing mid-stream.
	}

	for id, r := range table.Ranges() {
		log.Info("route", "node", id, "address", cfg.Addresses[id], "partitions", r.String())
	}
	log.Info("proxy starting",
		"addr", cfg.ListenAddr,
		"partitions", cfg.PartitionCount,
		"nodes", len(cfg.NodeIDs),
		"requestTimeout", cfg.RequestTimeout,
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
