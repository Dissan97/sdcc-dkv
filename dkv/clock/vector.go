package clock

import (
	"sync"
)

type VectorClock struct {
	mu     sync.RWMutex
	values map[string]uint64
	Id     string
}

func NewVectorClock(id string) *VectorClock {
	mp := make(map[string]uint64)
	mp[id] = 0
	return &VectorClock{
		values: mp,
		Id:     id,
	}
}

func (vc *VectorClock) Inc() {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.values[vc.Id]++
}

func (vc *VectorClock) Update(clock *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	for node, ts := range clock.values {
		if ts > vc.values[node] {
			vc.values[node] = ts
		}
	}
	vc.values[vc.Id]++
}

func (vc *VectorClock) NewNode(id string, actualValue uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.values[id] = actualValue
}

func (vc *VectorClock) RemoveNode(id string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	delete(vc.values, id)
}

func (vc *VectorClock) Value() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.values
}
