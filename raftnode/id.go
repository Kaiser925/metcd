package raftnode

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	tsLen     = 5 * 8
	cntLen    = 8
	suffixLen = tsLen + cntLen
)

type Generator struct {
	// high order 2 bytes
	prefix uint64
	// low order 6 bytes
	suffix uint64
}

func NewGenerator(memberID uint16, now time.Time) *Generator {
	prefix := uint64(memberID) << suffixLen
	unixMilli := uint64(now.UnixNano()) / uint64(time.Millisecond/time.Nanosecond)
	suffix := lowbit(unixMilli, tsLen) << cntLen
	return &Generator{
		prefix: prefix,
		suffix: suffix,
	}
}

// Next generates a id that is unique.
func (g *Generator) Next() uint64 {
	suffix := atomic.AddUint64(&g.suffix, 1)
	id := g.prefix | lowbit(suffix, suffixLen)
	return id
}

func lowbit(x uint64, n uint) uint64 {
	return x & (math.MaxUint64 >> (64 - n))
}
