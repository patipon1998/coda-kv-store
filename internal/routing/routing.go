// Package routing maps keys to partitions, and partitions to the node that owns
// them. It is deliberately dependency-free and is shared by both binaries: the
// proxy uses it to decide where to send a request, and the node uses it to
// reject requests for partitions it does not own.
//
// Sharing one implementation is the point. If the two sides computed partitions
// differently they would disagree silently, and a write would land somewhere no
// read would ever look.
package routing

import (
	"fmt"
	"strconv"
	"strings"
)

// FNV-1a 64-bit parameters.
//
// FNV-1a is chosen over hash/maphash because maphash seeds itself randomly per
// process: fine for in-process striping, fatal here, since two nodes must agree
// on where a key lives. Implemented inline rather than via hash/fnv to avoid an
// allocation per call; routing_test.go asserts the two agree.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// Partition returns the partition a key belongs to.
//
// partitionCount must be a power of two — see ValidPartitionCount — which lets
// the modulo become a mask.
func Partition(key string, partitionCount int) uint64 {
	h := fnvOffset64
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= fnvPrime64
	}
	return h & uint64(partitionCount-1)
}

// ValidPartitionCount reports whether n is usable as a partition count: positive
// and a power of two.
func ValidPartitionCount(n int) bool {
	return n > 0 && n&(n-1) == 0
}

// Range is a half-open span of partitions, [Lo, Hi).
type Range struct {
	Lo uint64
	Hi uint64
}

// RangeFor returns the partitions owned by the node at the given index, given
// the cluster size. Both the node and the proxy call this, so neither can drift
// from the other: a node needs only to know which slot it occupies, never where
// its peers live.
//
// Any remainder is spread across the low-indexed nodes rather than dumped on the
// last one, which keeps the largest and smallest ranges within one partition of
// each other.
func RangeFor(index, nodeCount, partitionCount int) (Range, error) {
	if nodeCount <= 0 {
		return Range{}, fmt.Errorf("routing: nodeCount must be positive, got %d", nodeCount)
	}
	if index < 0 || index >= nodeCount {
		return Range{}, fmt.Errorf("routing: index %d out of range for nodeCount %d", index, nodeCount)
	}
	if !ValidPartitionCount(partitionCount) {
		return Range{}, fmt.Errorf("routing: partitionCount must be a power of two, got %d", partitionCount)
	}

	base := partitionCount / nodeCount
	extra := partitionCount % nodeCount

	// Nodes below `extra` take one additional partition each.
	lo := index*base + min(index, extra)
	size := base
	if index < extra {
		size++
	}
	return Range{Lo: uint64(lo), Hi: uint64(lo + size)}, nil
}

// Contains reports whether the partition falls inside the range.
func (r Range) Contains(partition uint64) bool {
	return partition >= r.Lo && partition < r.Hi
}

// Count returns how many partitions the range covers.
func (r Range) Count() uint64 { return r.Hi - r.Lo }

func (r Range) String() string {
	if r.Hi == r.Lo {
		return "empty"
	}
	return fmt.Sprintf("%d-%d", r.Lo, r.Hi-1)
}

// ParseRange reads the inclusive "lo-hi" form used by the OWNED_PARTITIONS
// override, and returns the equivalent half-open Range. A bare "N" means the
// single partition N.
func ParseRange(s string) (Range, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Range{}, fmt.Errorf("routing: empty partition range")
	}

	lo, hi, found := strings.Cut(s, "-")
	if !found {
		hi = lo
	}

	loN, err := strconv.ParseUint(strings.TrimSpace(lo), 10, 64)
	if err != nil {
		return Range{}, fmt.Errorf("routing: bad range %q: %w", s, err)
	}
	hiN, err := strconv.ParseUint(strings.TrimSpace(hi), 10, 64)
	if err != nil {
		return Range{}, fmt.Errorf("routing: bad range %q: %w", s, err)
	}
	if hiN < loN {
		return Range{}, fmt.Errorf("routing: range %q ends before it starts", s)
	}
	return Range{Lo: loN, Hi: hiN + 1}, nil
}

// Table maps every partition to the id of the node that owns it. The proxy holds
// one; nodes never do.
type Table struct {
	partitionCount int
	owner          []string // indexed by partition
	nodeIDs        []string // stable, in cluster order
}

// NewTable builds a table by giving each node in nodeIDs the range RangeFor
// assigns to its position.
func NewTable(nodeIDs []string, partitionCount int) (*Table, error) {
	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("routing: no nodes configured")
	}
	if !ValidPartitionCount(partitionCount) {
		return nil, fmt.Errorf("routing: partitionCount must be a power of two, got %d", partitionCount)
	}
	if len(nodeIDs) > partitionCount {
		return nil, fmt.Errorf("routing: %d nodes cannot share %d partitions", len(nodeIDs), partitionCount)
	}

	owner := make([]string, partitionCount)
	for i, id := range nodeIDs {
		r, err := RangeFor(i, len(nodeIDs), partitionCount)
		if err != nil {
			return nil, err
		}
		for p := r.Lo; p < r.Hi; p++ {
			owner[p] = id
		}
	}
	return &Table{
		partitionCount: partitionCount,
		owner:          owner,
		nodeIDs:        append([]string(nil), nodeIDs...),
	}, nil
}

// NodeFor returns the id of the node holding the given key.
func (t *Table) NodeFor(key string) string {
	return t.owner[Partition(key, t.partitionCount)]
}

// NodeForPartition returns the id of the node holding the given partition.
func (t *Table) NodeForPartition(p uint64) string { return t.owner[p] }

// NodeIDs returns the configured nodes, in cluster order.
func (t *Table) NodeIDs() []string { return append([]string(nil), t.nodeIDs...) }

// PartitionCount returns the partition count the table was built with.
func (t *Table) PartitionCount() int { return t.partitionCount }

// Ranges returns each node's owned range, for diagnostics and the proxy's
// /routing endpoint.
func (t *Table) Ranges() map[string]Range {
	out := make(map[string]Range, len(t.nodeIDs))
	for i, id := range t.nodeIDs {
		r, err := RangeFor(i, len(t.nodeIDs), t.partitionCount)
		if err != nil {
			continue // unreachable: NewTable already validated these
		}
		out[id] = r
	}
	return out
}
