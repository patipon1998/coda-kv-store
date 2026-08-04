// Package config loads process configuration from the environment. Both
// binaries use it so that defaults and validation live in one place.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"codakv/internal/routing"
)

// Defaults. PartitionCount is 1024 rather than 256 because the extra memory is
// negligible (~80 bytes per empty partition) while it keeps placement skew under
// 5% out to roughly a hundred nodes; 256 starts to skew past ~25.
const (
	DefaultPartitionCount = 1024
	DefaultMaxValueBytes  = 1 << 20 // 1 MiB — ruling R6
	DefaultMaxKeyBytes    = 1 << 10 // 1 KiB — ruling R8
	DefaultRequestTimeout = 5 * time.Second
)

// Node is a store node's configuration.
type Node struct {
	NodeID         string
	ListenAddr     string
	PartitionCount int

	// Owned is the partition range this node accepts writes for. Requests for
	// anything outside it are refused with 421, which turns a proxy
	// misconfiguration into a loud error instead of silent data loss.
	Owned routing.Range

	MaxValueBytes int64
	MaxKeyBytes   int
}

// LoadNode reads a node's configuration from the environment.
//
// Partition ownership comes from NODE_INDEX and NODE_COUNT via
// routing.RangeFor, which the proxy also calls — so neither side can drift from
// the other, and the node needs to know only which slot it occupies, never where
// its peers are. OWNED_PARTITIONS overrides that when set, both for uneven
// placement and to make the misrouting demo possible without code changes.
func LoadNode() (Node, error) {
	cfg := Node{
		NodeID:         env("NODE_ID", "node-1"),
		ListenAddr:     env("LISTEN_ADDR", ":8081"),
		PartitionCount: DefaultPartitionCount,
		MaxValueBytes:  DefaultMaxValueBytes,
		MaxKeyBytes:    DefaultMaxKeyBytes,
	}

	var err error
	if cfg.PartitionCount, err = envInt("PARTITION_COUNT", DefaultPartitionCount); err != nil {
		return Node{}, err
	}
	if !routing.ValidPartitionCount(cfg.PartitionCount) {
		return Node{}, fmt.Errorf("PARTITION_COUNT must be a positive power of two, got %d",
			cfg.PartitionCount)
	}

	maxValue, err := envInt("MAX_VALUE_BYTES", DefaultMaxValueBytes)
	if err != nil {
		return Node{}, err
	}
	cfg.MaxValueBytes = int64(maxValue)

	if cfg.MaxKeyBytes, err = envInt("MAX_KEY_BYTES", DefaultMaxKeyBytes); err != nil {
		return Node{}, err
	}

	if override := os.Getenv("OWNED_PARTITIONS"); override != "" {
		r, err := routing.ParseRange(override)
		if err != nil {
			return Node{}, fmt.Errorf("OWNED_PARTITIONS: %w", err)
		}
		if r.Hi > uint64(cfg.PartitionCount) {
			return Node{}, fmt.Errorf("OWNED_PARTITIONS %s exceeds PARTITION_COUNT %d",
				override, cfg.PartitionCount)
		}
		cfg.Owned = r
		return cfg, nil
	}

	index, err := envInt("NODE_INDEX", 0)
	if err != nil {
		return Node{}, err
	}
	count, err := envInt("NODE_COUNT", 1)
	if err != nil {
		return Node{}, err
	}
	if cfg.Owned, err = routing.RangeFor(index, count, cfg.PartitionCount); err != nil {
		return Node{}, err
	}
	return cfg, nil
}

// Proxy is the router's configuration.
type Proxy struct {
	ListenAddr     string
	PartitionCount int

	// NodeIDs is the cluster in order; position determines each node's partition
	// range, so the order must match the nodes' own NODE_INDEX values.
	NodeIDs []string
	// Addresses maps a node id to its base URL.
	Addresses map[string]string

	RequestTimeout time.Duration
}

// LoadProxy reads the proxy's configuration from the environment.
//
// NODES is an ordered, comma-separated list of id=url pairs, for example:
//
//	NODES=node-1=http://node-1:8081,node-2=http://node-2:8081
func LoadProxy() (Proxy, error) {
	cfg := Proxy{
		ListenAddr:     env("LISTEN_ADDR", ":8081"),
		PartitionCount: DefaultPartitionCount,
		RequestTimeout: DefaultRequestTimeout,
		Addresses:      map[string]string{},
	}

	var err error
	if cfg.PartitionCount, err = envInt("PARTITION_COUNT", DefaultPartitionCount); err != nil {
		return Proxy{}, err
	}
	if !routing.ValidPartitionCount(cfg.PartitionCount) {
		return Proxy{}, fmt.Errorf("PARTITION_COUNT must be a positive power of two, got %d",
			cfg.PartitionCount)
	}

	raw := os.Getenv("NODES")
	if strings.TrimSpace(raw) == "" {
		return Proxy{}, fmt.Errorf("NODES is required, e.g. node-1=http://node-1:8081,node-2=...")
	}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		id, url, found := strings.Cut(pair, "=")
		id, url = strings.TrimSpace(id), strings.TrimSpace(url)
		if !found || id == "" || url == "" {
			return Proxy{}, fmt.Errorf("NODES entry %q is not id=url", pair)
		}
		if _, dup := cfg.Addresses[id]; dup {
			return Proxy{}, fmt.Errorf("NODES lists %q twice", id)
		}
		cfg.NodeIDs = append(cfg.NodeIDs, id)
		cfg.Addresses[id] = strings.TrimRight(url, "/")
	}
	if len(cfg.NodeIDs) == 0 {
		return Proxy{}, fmt.Errorf("NODES is empty")
	}

	if raw := os.Getenv("REQUEST_TIMEOUT"); raw != "" {
		if cfg.RequestTimeout, err = time.ParseDuration(raw); err != nil {
			return Proxy{}, fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
