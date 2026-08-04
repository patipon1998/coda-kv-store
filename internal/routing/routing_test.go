package routing

import (
	"fmt"
	"hash/fnv"
	"testing"
)

// K1: the hash must be deterministic across processes and restarts. The inline
// FNV-1a implementation exists only to avoid an allocation per call, so it has
// to agree with the standard library exactly.
func TestPartitionMatchesStdlibFNV(t *testing.T) {
	keys := []string{"", "a", "user:42", "ไทย🔑", "a/b", "key with space",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

	for _, k := range keys {
		h := fnv.New64a()
		_, _ = h.Write([]byte(k))
		want := h.Sum64() & 1023
		if got := Partition(k, 1024); got != want {
			t.Errorf("Partition(%q) = %d, stdlib FNV-1a gives %d", k, got, want)
		}
	}
}

// K1: same key, same partition, every time.
func TestPartitionIsStable(t *testing.T) {
	const key = "user:42"
	first := Partition(key, 1024)
	for i := 0; i < 1000; i++ {
		if got := Partition(key, 1024); got != first {
			t.Fatalf("Partition(%q) drifted: %d then %d", key, first, got)
		}
	}
}

func TestPartitionWithinBounds(t *testing.T) {
	for _, count := range []int{1, 2, 16, 256, 1024, 16384} {
		for i := 0; i < 5000; i++ {
			p := Partition(fmt.Sprintf("key-%d", i), count)
			if p >= uint64(count) {
				t.Fatalf("Partition returned %d, outside [0,%d)", p, count)
			}
		}
	}
}

// K3: a good hash spreads keys evenly. Balance was never the argument against
// modulo — resharding was — so this guards the hash, not the placement scheme.
func TestPartitionDistribution(t *testing.T) {
	const (
		keys  = 100_000
		nodes = 3
	)
	counts := make([]int, nodes)
	table, err := NewTable([]string{"node-1", "node-2", "node-3"}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	index := map[string]int{"node-1": 0, "node-2": 1, "node-3": 2}

	for i := 0; i < keys; i++ {
		counts[index[table.NodeFor(fmt.Sprintf("user:%d", i))]]++
	}

	want := keys / nodes
	for i, got := range counts {
		if diff := float64(got-want) / float64(want); diff > 0.10 || diff < -0.10 {
			t.Errorf("node %d holds %d keys, want ~%d (%.1f%% off)", i, got, want, diff*100)
		}
	}
}

func TestValidPartitionCount(t *testing.T) {
	valid := []int{1, 2, 4, 256, 1024, 16384}
	invalid := []int{0, -1, 3, 100, 1000}

	for _, n := range valid {
		if !ValidPartitionCount(n) {
			t.Errorf("ValidPartitionCount(%d) = false, want true", n)
		}
	}
	for _, n := range invalid {
		if ValidPartitionCount(n) {
			t.Errorf("ValidPartitionCount(%d) = true, want false", n)
		}
	}
}

// The whole point of RangeFor: node and proxy derive identical ranges from the
// same inputs, so they cannot drift.
func TestRangeForCoversEveryPartitionExactlyOnce(t *testing.T) {
	for _, nodeCount := range []int{1, 2, 3, 5, 7, 16} {
		for _, partitionCount := range []int{16, 256, 1024} {
			seen := make([]int, partitionCount)
			for i := 0; i < nodeCount; i++ {
				r, err := RangeFor(i, nodeCount, partitionCount)
				if err != nil {
					t.Fatalf("RangeFor(%d,%d,%d): %v", i, nodeCount, partitionCount, err)
				}
				for p := r.Lo; p < r.Hi; p++ {
					seen[p]++
				}
			}
			for p, n := range seen {
				if n != 1 {
					t.Fatalf("nodes=%d partitions=%d: partition %d owned by %d nodes",
						nodeCount, partitionCount, p, n)
				}
			}
		}
	}
}

// Remainder spreads across low-indexed nodes, so the largest and smallest ranges
// differ by at most one partition.
func TestRangeForIsBalanced(t *testing.T) {
	const partitionCount = 1024
	for _, nodeCount := range []int{3, 5, 7, 13} {
		var lo, hi uint64 = ^uint64(0), 0
		for i := 0; i < nodeCount; i++ {
			r, err := RangeFor(i, nodeCount, partitionCount)
			if err != nil {
				t.Fatal(err)
			}
			lo = min(lo, r.Count())
			hi = max(hi, r.Count())
		}
		if hi-lo > 1 {
			t.Errorf("nodeCount=%d: range sizes span %d..%d, want a spread of at most 1", nodeCount, lo, hi)
		}
	}
}

func TestRangeForRejectsBadInput(t *testing.T) {
	cases := []struct{ index, nodeCount, partitionCount int }{
		{0, 0, 1024},  // no nodes
		{-1, 3, 1024}, // negative index
		{3, 3, 1024},  // index == nodeCount
		{0, 3, 1000},  // partitionCount not a power of two
		{0, 3, 0},     // zero partitions
	}
	for _, c := range cases {
		if _, err := RangeFor(c.index, c.nodeCount, c.partitionCount); err == nil {
			t.Errorf("RangeFor(%d,%d,%d) succeeded, want error", c.index, c.nodeCount, c.partitionCount)
		}
	}
}

func TestRangeContains(t *testing.T) {
	r := Range{Lo: 10, Hi: 20}
	for _, p := range []uint64{10, 15, 19} {
		if !r.Contains(p) {
			t.Errorf("Range{10,20}.Contains(%d) = false, want true", p)
		}
	}
	for _, p := range []uint64{0, 9, 20, 100} {
		if r.Contains(p) {
			t.Errorf("Range{10,20}.Contains(%d) = true, want false", p)
		}
	}
}

// ParseRange is the OWNED_PARTITIONS override — the misroute demo lever.
func TestParseRange(t *testing.T) {
	ok := []struct {
		in   string
		want Range
	}{
		{"0-85", Range{0, 86}},
		{"86-170", Range{86, 171}},
		{"7", Range{7, 8}},
		{"  0 - 3 ", Range{0, 4}},
	}
	for _, c := range ok {
		got, err := ParseRange(c.in)
		if err != nil {
			t.Errorf("ParseRange(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseRange(%q) = %v, want %v", c.in, got, c.want)
		}
	}

	bad := []string{"", "  ", "abc", "5-2", "-", "1-x", "x-1"}
	for _, in := range bad {
		if _, err := ParseRange(in); err == nil {
			t.Errorf("ParseRange(%q) succeeded, want error", in)
		}
	}
}

// A round trip through String and ParseRange must be lossless, since one is what
// operators read and the other is what they type.
func TestRangeStringRoundTrip(t *testing.T) {
	for _, r := range []Range{{0, 86}, {86, 171}, {171, 256}, {7, 8}} {
		got, err := ParseRange(r.String())
		if err != nil {
			t.Fatalf("ParseRange(%q): %v", r.String(), err)
		}
		if got != r {
			t.Errorf("round trip of %v via %q gave %v", r, r.String(), got)
		}
	}
}

// K2: every key routes to the node whose range holds its partition — the
// property the node's 421 check relies on.
func TestTableAgreesWithRangeFor(t *testing.T) {
	ids := []string{"node-1", "node-2", "node-3"}
	table, err := NewTable(ids, 1024)
	if err != nil {
		t.Fatal(err)
	}

	ranges := make([]Range, len(ids))
	for i := range ids {
		ranges[i], _ = RangeFor(i, len(ids), 1024)
	}

	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("user:%d", i)
		p := Partition(key, 1024)
		owner := table.NodeFor(key)

		for j, id := range ids {
			if ranges[j].Contains(p) {
				if owner != id {
					t.Fatalf("key %q in partition %d: table says %s, RangeFor says %s", key, p, owner, id)
				}
			} else if owner == id {
				t.Fatalf("key %q in partition %d: table says %s but that range is %v", key, p, id, ranges[j])
			}
		}
	}
}

func TestNewTableRejectsBadInput(t *testing.T) {
	if _, err := NewTable(nil, 1024); err == nil {
		t.Error("NewTable with no nodes succeeded, want error")
	}
	if _, err := NewTable([]string{"n1"}, 1000); err == nil {
		t.Error("NewTable with non-power-of-two partition count succeeded, want error")
	}
	if _, err := NewTable([]string{"n1", "n2", "n3"}, 2); err == nil {
		t.Error("NewTable with more nodes than partitions succeeded, want error")
	}
}

func BenchmarkPartition(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Partition("user:42", 1024)
	}
}
