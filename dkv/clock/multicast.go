package clock

import (
	"sort"
	"sync"
)

type MulticastMessage struct {
	Timestamp uint64
	Key       uint64
	Value     string
}

type MulticastQueue struct {
	Lock  sync.RWMutex
	Queue map[uint64][]MulticastMessage
}

func NewMulticastQueue() *MulticastQueue {
	return &MulticastQueue{
		Queue: make(map[uint64][]MulticastMessage),
	}
}

func (queue *MulticastQueue) Enqueue(key uint64, msg MulticastMessage) {
	queue.Lock.Lock()
	defer queue.Lock.Unlock()
	if _, ok := queue.Queue[key]; !ok {
		queue.Queue[key] = make([]MulticastMessage, 0)
	}
	queue.Queue[key] = append(queue.Queue[key], msg)

	sort.Slice(queue.Queue[key], func(i, j int) bool {
		return queue.Queue[key][i].Timestamp < queue.Queue[key][j].Timestamp
	})

}

func (queue *MulticastQueue) Dequeue(key uint64) {
	queue.Lock.Lock()
	defer queue.Lock.Unlock()
	if _, ok := queue.Queue[key]; !ok {
		return
	}
	queue.Queue[key] = queue.Queue[key][:0]

}
