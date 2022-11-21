package main

import "time"

type FlightTimeStatistics struct {
	Outbox        FlightTimeStats
	Sent          FlightTimeStats
	DeliveryEvent FlightTimeStats
	ReceivedEvent FlightTimeStats
	Received      FlightTimeStats
}

func (f *FlightTimeStatistics) calcMedians() {
	//f.Outbox.calcMedian()
	f.Sent.calcMedian()
	f.DeliveryEvent.calcMedian()
	f.ReceivedEvent.calcMedian()
	f.Received.calcMedian()
}

type FlightTimeStats struct {
	buckets SortedMap[int64, int64]
	count   int64
	Average time.Duration
	Median  time.Duration
}

func (f *FlightTimeStats) Add(duration time.Duration) {
	// Add to bucket for fast median calculation
	durAsInt := duration.Milliseconds()
	val := f.buckets.m[durAsInt]
	f.buckets.Put(durAsInt, val+1)

	// Avg calculation
	f.count++
	f.Average += (duration - f.Average) / time.Duration(f.count)
}

func (f *FlightTimeStats) calcMedian() {
	middle := f.count / 2
	var count int64
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
