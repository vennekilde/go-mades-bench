package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

type MessageTracker struct {
	m        sync.Mutex
	Messages map[string]*MessageIdent
}

func NewMessageTracker() *MessageTracker {
	return &MessageTracker{
		Messages: map[string]*MessageIdent{},
	}
}

func (m *MessageTracker) Add(id string, msg *MessageIdent) {
	m.m.Lock()
	defer m.m.Unlock()
	m.Messages[id] = msg
}

func (m *MessageTracker) Get(id string) *MessageIdent {
	m.m.Lock()
	defer m.m.Unlock()
	return m.Messages[id]
}

func (m *MessageTracker) Delete(msgID string) {
	m.m.Lock()
	defer m.m.Unlock()
	delete(m.Messages, msgID)
}

type MessageIdent struct {
	ID          uint64
	Created     time.Time
	Count       uint8
	EventsFlags int
}

type Tracker struct {
	Start       time.Time
	End         time.Time
	PayloadSize int
	Trackables  []*Trackable
}

func (t *Tracker) SetStart(start time.Time) {
	t.Start = start
	for _, trackable := range t.Trackables {
		trackable.Start = start
	}
}

func (t *Tracker) calcMedians() {
	for _, trackable := range t.Trackables {
		trackable.Stats.calcMedian()
	}
}

func (t *Tracker) AwaitEnd() {
	// Assume last tracker is the final tracker that should complete
	completedLock := &t.Trackables[len(t.Trackables)-1].CompletedLock
	// When able to lock the completedLock mutex, it means the process has completed
	completedLock.Lock()
	defer completedLock.Unlock()
	t.End = time.Now()
	// Initiate graceful finish
	t.GracefulFinish()
}

func (t *Tracker) GracefulFinish() {
	for {
		lastCount := t.eventCount()
		log.Println("waiting a second for any out of order messages...")
		time.Sleep(time.Second)
		count := t.eventCount()
		if lastCount == count {
			// Break loop, as no changes occurred the past second
			return
		}
	}
}

func (t *Tracker) eventCount() uint64 {
	var c uint64
	for _, trackable := range t.Trackables {
		c += trackable.Stats.Count()
	}
	return c
}

func (t *Tracker) BenchResults(w io.Writer) {
	t.calcMedians()

	_, _ = w.Write([]byte("==============================================================================\n"))
	_, _ = w.Write([]byte(fmt.Sprintf("Started at       : %s\n", t.Start.Format(time.RFC3339))))

	var duration time.Duration
	if !t.End.IsZero() {
		_, _ = w.Write([]byte(fmt.Sprintf("Ended at         : %s\n", t.End.Format(time.RFC3339))))
		duration = t.End.Sub(t.Start)
	} else {
		duration = time.Since(t.Start)
	}
	_, _ = w.Write([]byte(fmt.Sprintf("Duration         : %s\n", duration.String())))
	_, _ = w.Write([]byte(fmt.Sprintf("Msg size         : %d bytes\n\n", t.PayloadSize)))
	_, _ = w.Write([]byte("=== Statistics\n"))

	for _, trackable := range t.Trackables {
		trackable.BenchResults(w)
	}

	_, _ = w.Write([]byte("==============================================================================\n"))
}

type Trackable struct {
	m                  sync.Mutex
	Name               string
	Stats              FlightTimeStats
	Limit              uint64
	Completed          bool
	CompletedLock      sync.Mutex
	Start              time.Time
	End                time.Time
	EstimatedEventSize uint64
}

func NewTrackable() *Trackable {
	t := &Trackable{
		Stats: NewFlightTimeStats(),
	}
	t.CompletedLock.Lock()
	return t
}

func (t *Trackable) Add(duration time.Duration) {
	t.Stats.Add(duration)
	t.Stats.m.Lock()
	if t.Stats.count >= t.Limit && !t.Completed {
		t.End = time.Now()
		t.Completed = true
		t.CompletedLock.Unlock()
	}
	t.Stats.m.Unlock()
}

func (t *Trackable) Duration() time.Duration {
	if t.End.IsZero() {
		return time.Since(t.Start)
	}
	return t.End.Sub(t.Start)
}

func (t *Trackable) BenchResults(w io.Writer) {
	column1Len := 18
	column2Len := 19
	column3Len := 12
	column4Len := 20
	column5Len := 20
	column6Len := 25

	t.m.Lock()
	t.Stats.m.Lock()

	c := t.Stats.count
	duration := t.Duration()
	msgsPerSec := float64(c) / duration.Seconds()
	throughputData := float64(c) / duration.Seconds() * float64(t.EstimatedEventSize) / 1000000
	avg := t.Stats.Average
	median := t.Stats.Median

	t.Stats.m.Unlock()
	t.m.Unlock()

	_, _ = w.Write([]byte(t.Name))
	_, _ = w.Write(columnSpacer(column1Len, len(t.Name)))

	msgsPerSecStr := fmt.Sprintf(": %.3f msgs/s", msgsPerSec)
	_, _ = w.Write([]byte(msgsPerSecStr))
	_, _ = w.Write(columnSpacer(column2Len, len(msgsPerSecStr)))

	throughputDataStr := fmt.Sprintf("%.3f mb/s", throughputData)
	_, _ = w.Write([]byte(throughputDataStr))
	_, _ = w.Write(columnSpacer(column3Len, len(throughputDataStr)))

	cStr := fmt.Sprintf("%d/%d msgs", c, t.Limit)
	_, _ = w.Write([]byte(cStr))
	_, _ = w.Write(columnSpacer(column4Len, len(cStr)))

	durationStr := fmt.Sprintf("duration: %s", duration.Round(time.Millisecond))
	_, _ = w.Write([]byte(durationStr))
	_, _ = w.Write(columnSpacer(column5Len, len(durationStr)))

	avgStr := fmt.Sprintf("flight time: %.3fs (avg)", avg.Seconds())
	_, _ = w.Write([]byte(avgStr))
	_, _ = w.Write(columnSpacer(column6Len, len(avgStr)))

	medianStr := fmt.Sprintf("%.3fs (median)\n", median.Seconds())
	_, _ = w.Write([]byte(medianStr))
}

func columnSpacer(minLen int, usedLen int) []byte {
	spaceLen := minLen - usedLen
	if spaceLen <= 0 {
		spaceLen = 1
	}
	return bytes.Repeat([]byte{' '}, spaceLen)
}
