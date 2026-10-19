package util

import "sync"

type LamportClock struct {
	timestamp uint32
	mu        sync.Mutex
}

func NewLamportClock() *LamportClock {
	return &LamportClock{}
}

func (this *LamportClock) Incr() {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.timestamp++
}

func (this *LamportClock) Update(other_timestamp uint32) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.timestamp = max(this.timestamp, other_timestamp) + 1
}

func (this *LamportClock) GetTimestamp() uint32 {
	return this.timestamp
}
