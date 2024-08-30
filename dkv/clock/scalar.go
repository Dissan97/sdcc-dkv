package clock

import (
	"sync/atomic"
)

type ScalarClock struct {
	Timestamp uint64 `json:"key"`
}

func NewScalar() *ScalarClock {
	return &ScalarClock{0}
}

func (sc *ScalarClock) Inc() {
	atomic.AddUint64(&sc.Timestamp, 1)
}

func (sc *ScalarClock) Update(otherScalar uint64) {

	old := sc.Value()

	if otherScalar >= old {
		if atomic.CompareAndSwapUint64(&sc.Timestamp, old, otherScalar) {
			return
		}
	} else {
		atomic.AddUint64(&sc.Timestamp, 1)
	}
}

func (sc *ScalarClock) Value() uint64 {
	return atomic.LoadUint64(&sc.Timestamp)
}
func (sc *ScalarClock) Serialize() uint64 {
	return sc.Value()
}

// Deserialize from int64
func (sc *ScalarClock) Deserialize(value uint64) {
	atomic.StoreUint64(&sc.Timestamp, value)
}
