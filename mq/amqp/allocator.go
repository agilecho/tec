package amqp

import (
	"bytes"
	"fmt"
	"math/big"
)

const (
	free      = 0
	allocated = 1
)

type allocator struct {
	pool *big.Int
	last int
	low  int
	high int
}

func newAllocator(low, high int) *allocator {
	return &allocator{
		pool: big.NewInt(0),
		last: low,
		low:  low,
		high: high,
	}
}

func (a allocator) String() string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "allocator[%d..%d]", a.low, a.high)

	for low := a.low; low <= a.high; low++ {
		high := low
		for a.reserved(high) && high <= a.high {
			high++
		}

		if high > low+1 {
			fmt.Fprintf(b, " %d..%d", low, high-1)
		} else if high > low {
			fmt.Fprintf(b, " %d", high-1)
		}

		low = high
	}

	return b.String()
}

func (a *allocator) next() (int, bool) {
	wrapped := a.last

	for ; a.last <= a.high; a.last++ {
		if a.reserve(a.last) {
			return a.last, true
		}
	}

	a.last = a.low

	for ; a.last < wrapped; a.last++ {
		if a.reserve(a.last) {
			return a.last, true
		}
	}

	return 0, false
}

func (a *allocator) reserve(n int) bool {
	if a.reserved(n) {
		return false
	}
	a.pool.SetBit(a.pool, n-a.low, allocated)
	return true
}

func (a *allocator) reserved(n int) bool {
	return a.pool.Bit(n-a.low) == allocated
}

func (a *allocator) release(n int) {
	a.pool.SetBit(a.pool, n-a.low, free)
}
