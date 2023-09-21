package main

import (
	"sync"
	"time"
)

type FlightTimeStats struct {
	m       sync.Mutex
	buckets SortedMap[uint64, uint64]
	count   uint64
	Average time.Duration
	Median  time.Duration
}

func NewFlightTimeStats() FlightTimeStats {
	return FlightTimeStats{
		buckets: NewSortedMap[uint64, uint64](),
	}
}

func (f *FlightTimeStats) Count() uint64 {
	f.m.Lock()
	defer f.m.Unlock()
	return f.count
}

func (f *FlightTimeStats) Add(duration time.Duration) {
	// Add to bucket for fast median calculation
	durAsInt := uint64(duration.Milliseconds())
	f.m.Lock()
	defer f.m.Unlock()
	val := f.buckets.m[durAsInt]
	f.buckets.Put(durAsInt, val+1)

	// Avg calculation
	f.count++
	f.Average += (duration - f.Average) / time.Duration(f.count)
}

func (f *FlightTimeStats) calcMedian() {
	f.m.Lock()
	defer f.m.Unlock()
	middle := f.count / 2
	var count uint64
	node := f.buckets.keys
	for node != nil {
		count += f.buckets.m[node.value]
		if count >= middle {
			f.Median = time.Duration(node.value) * time.Millisecond
			return
		}
		node = node.next
	}
}
