package config

import (
	"testing"

	"codakv/internal/routing"
)

func TestLoadNodeDefaults(t *testing.T) {
	cfg, err := LoadNode()
	if err != nil {
		t.Fatalf("LoadNode with a clean environment: %v", err)
	}

	if cfg.NodeID != "node-1" {
		t.Errorf("NodeID %q, want node-1", cfg.NodeID)
	}
	if cfg.ListenAddr != ":8081" {
		t.Errorf("ListenAddr %q, want :8081", cfg.ListenAddr)
	}
	if cfg.PartitionCount != DefaultPartitionCount {
		t.Errorf("PartitionCount %d, want %d", cfg.PartitionCount, DefaultPartitionCount)
	}
	// A lone node owns everything.
	if cfg.Owned.Lo != 0 || cfg.Owned.Hi != uint64(DefaultPartitionCount) {
		t.Errorf("Owned %v, want the whole keyspace", cfg.Owned)
	}
}

// The property that matters: a node derives its range from its own index and the
// cluster size, using the same function the proxy's table uses. Neither side can
// drift, and the node never learns where its peers are.
func TestLoadNodeDerivesRangeFromIndex(t *testing.T) {
	const partitions = 1024

	for index := 0; index < 3; index++ {
		t.Setenv("NODE_INDEX", itoa(index))
		t.Setenv("NODE_COUNT", "3")
		t.Setenv("PARTITION_COUNT", itoa(partitions))

		cfg, err := LoadNode()
		if err != nil {
			t.Fatalf("index %d: %v", index, err)
		}

		want, err := routing.RangeFor(index, 3, partitions)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Owned != want {
			t.Errorf("index %d owns %v, want %v", index, cfg.Owned, want)
		}
	}
}

// The override is the misrouting demo lever: point a node at the wrong range and
// it starts refusing keys with 421, no code change required.
func TestLoadNodeOwnedPartitionsOverride(t *testing.T) {
	t.Setenv("PARTITION_COUNT", "1024")
	t.Setenv("NODE_INDEX", "2")
	t.Setenv("NODE_COUNT", "3")
	t.Setenv("OWNED_PARTITIONS", "0-341")

	cfg, err := LoadNode()
	if err != nil {
		t.Fatal(err)
	}
	want := routing.Range{Lo: 0, Hi: 342}
	if cfg.Owned != want {
		t.Errorf("Owned %v, want %v — the override should beat NODE_INDEX", cfg.Owned, want)
	}
}

func TestLoadNodeRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"partition count not a power of two", map[string]string{"PARTITION_COUNT": "1000"}},
		{"zero partitions", map[string]string{"PARTITION_COUNT": "0"}},
		{"partition count not a number", map[string]string{"PARTITION_COUNT": "many"}},
		{"index beyond cluster size", map[string]string{"NODE_INDEX": "5", "NODE_COUNT": "3"}},
		{"negative index", map[string]string{"NODE_INDEX": "-1", "NODE_COUNT": "3"}},
		{"zero nodes", map[string]string{"NODE_COUNT": "0"}},
		{"unparseable override", map[string]string{"OWNED_PARTITIONS": "not-a-range"}},
		{"override beyond the keyspace", map[string]string{
			"PARTITION_COUNT": "16", "OWNED_PARTITIONS": "0-99"}},
		{"backwards override", map[string]string{"OWNED_PARTITIONS": "500-100"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			if _, err := LoadNode(); err == nil {
				t.Error("configuration accepted, want an error")
			}
		})
	}
}

func TestLoadProxy(t *testing.T) {
	t.Setenv("NODES", "node-1=http://a:8081,node-2=http://b:8081,node-3=http://c:8081/")
	t.Setenv("PARTITION_COUNT", "1024")

	cfg, err := LoadProxy()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"node-1", "node-2", "node-3"}
	if len(cfg.NodeIDs) != len(want) {
		t.Fatalf("parsed %d nodes, want %d", len(cfg.NodeIDs), len(want))
	}
	// Order is load-bearing: a node's position fixes its partition range, so it
	// must line up with that node's NODE_INDEX.
	for i, id := range want {
		if cfg.NodeIDs[i] != id {
			t.Errorf("node %d is %q, want %q", i, cfg.NodeIDs[i], id)
		}
	}
	if got := cfg.Addresses["node-3"]; got != "http://c:8081" {
		t.Errorf("trailing slash not trimmed: %q", got)
	}
	if cfg.RequestTimeout != DefaultRequestTimeout {
		t.Errorf("RequestTimeout %v, want %v", cfg.RequestTimeout, DefaultRequestTimeout)
	}
}

func TestLoadProxyRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"NODES missing", map[string]string{}},
		{"NODES blank", map[string]string{"NODES": "   "}},
		{"entry is not id=url", map[string]string{"NODES": "node-1"}},
		{"empty id", map[string]string{"NODES": "=http://a:8081"}},
		{"empty url", map[string]string{"NODES": "node-1="}},
		{"duplicate id", map[string]string{"NODES": "n1=http://a,n1=http://b"}},
		{"partition count not a power of two", map[string]string{
			"NODES": "n1=http://a", "PARTITION_COUNT": "1000"}},
		{"unparseable timeout", map[string]string{
			"NODES": "n1=http://a", "REQUEST_TIMEOUT": "soon"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			if _, err := LoadProxy(); err == nil {
				t.Error("configuration accepted, want an error")
			}
		})
	}
}

func TestLoadProxyCustomTimeout(t *testing.T) {
	t.Setenv("NODES", "node-1=http://a:8081")
	t.Setenv("REQUEST_TIMEOUT", "250ms")

	cfg, err := LoadProxy()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RequestTimeout.String() != "250ms" {
		t.Errorf("RequestTimeout %v, want 250ms", cfg.RequestTimeout)
	}
}

// The proxy's table and each node's derived range must agree for every
// partition. A disagreement would be silent misrouting rather than a crash.
func TestProxyTableAgreesWithNodeRanges(t *testing.T) {
	const partitions = 1024
	ids := []string{"node-1", "node-2", "node-3"}

	t.Setenv("NODES", "node-1=http://a,node-2=http://b,node-3=http://c")
	t.Setenv("PARTITION_COUNT", itoa(partitions))

	proxyCfg, err := LoadProxy()
	if err != nil {
		t.Fatal(err)
	}
	table, err := routing.NewTable(proxyCfg.NodeIDs, proxyCfg.PartitionCount)
	if err != nil {
		t.Fatal(err)
	}

	for i, id := range ids {
		t.Setenv("NODE_INDEX", itoa(i))
		t.Setenv("NODE_COUNT", itoa(len(ids)))
		nodeCfg, err := LoadNode()
		if err != nil {
			t.Fatal(err)
		}
		for p := nodeCfg.Owned.Lo; p < nodeCfg.Owned.Hi; p++ {
			if owner := table.NodeForPartition(p); owner != id {
				t.Fatalf("node %s claims partition %d, but the proxy routes it to %s", id, p, owner)
			}
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
