package causal_order

import (
	"log"
	"sync"
)

type MessageCausalMulticast struct {
	Key         string
	Value       string
	VectorClock map[string]uint64
	Guid        string
	Deliverable bool
	Operation   string
}

type MulticastQueue struct {
	Queue         []*MessageCausalMulticast
	ExpectedClock map[string]uint64
	TimeLock      *sync.RWMutex
	Lock          sync.RWMutex
}

// Init Initialize the multicast queue
func (mq *MulticastQueue) Init(vc map[string]uint64, vcLock *sync.RWMutex) {
	mq.Queue = make([]*MessageCausalMulticast, 0)
	mq.ExpectedClock = vc
	mq.TimeLock = vcLock
	log.Println("MulticastQueue initialized with replica GUIDs:", vc)
}

// IsCausallyReady Check if the message is causally ready for delivery by comparing vector clocks
func (mq *MulticastQueue) IsCausallyReady(theMessage *MessageCausalMulticast) bool {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	// Using a standard for loop to get the index
	for i := 0; i < len(mq.Queue); i++ {
		msg := mq.Queue[i]
		if msg.Key == theMessage.Key && msg.Value == theMessage.Value {

			firstCheck := false
			secondCheck := true
			mq.TimeLock.RLock()
			firstCheck = msg.VectorClock[msg.Guid] == (mq.ExpectedClock[msg.Guid] + 1)
			for k, v := range mq.ExpectedClock {
				if k != msg.Guid {
					if msg.VectorClock[k] > v {
						secondCheck = false
						break
					}
				}
			}
			mq.TimeLock.RUnlock()
			if firstCheck && secondCheck {
				log.Printf("key=%s meet criteria for causally orderd", theMessage.Key)
				mq.Queue = append(mq.Queue[:i], mq.Queue[i+1:]...)
				return true
			}

		}
	}

	return false
}

// Enqueue a message into the multicast queue
func (mq *MulticastQueue) Enqueue(msg *MessageCausalMulticast) {
	mq.Lock.Lock()
	mq.Queue = append(mq.Queue, msg)
	mq.Lock.Unlock()
	log.Println("Message queued key=", msg.Key, "vector clock=", msg.VectorClock)
}

func (mq *MulticastQueue) Dequeue(msg *MessageCausalMulticast) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	for i := 0; i < len(mq.Queue); i++ {
		if mq.Queue[i].Key == msg.Key && mq.Queue[i].Value == msg.Value {
			mq.Queue = append(mq.Queue[:i], mq.Queue[i+1:]...)
			return
		}
	}
}
