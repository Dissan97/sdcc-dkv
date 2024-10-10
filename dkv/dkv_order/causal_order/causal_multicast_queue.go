package causal_order

import (
	"fmt"
	"sync"
)

type MessageCausalMulticast struct {
	Key         string
	Value       string
	VectorClock map[string]uint64
	SenderGuid  string
	Operation   string
}

type MulticastQueue struct {
	Queue map[string]*MessageCausalMulticast
	Lock  sync.RWMutex
}

func (msg *MessageCausalMulticast) GetKey() string {
	return fmt.Sprintf("%s:%v", msg.Key, msg.VectorClock)
}

func (mq *MulticastQueue) Init() {
	mq.Queue = make(map[string]*MessageCausalMulticast)
}

func (mq *MulticastQueue) AddMessage(message *MessageCausalMulticast) {
	key := message.GetKey()
	mq.Lock.Lock()
	defer mq.Lock.Unlock()
	mq.Queue[key] = message
}

func (mq *MulticastQueue) RemoveMessage(message *MessageCausalMulticast) {
	key := message.GetKey()
	mq.Lock.Lock()
	defer mq.Lock.Unlock()
	delete(mq.Queue, key)
}

func (mq *MulticastQueue) CheckDeliverable(key string, Vc map[string]uint64,
	lock *sync.RWMutex) (bool, error) {

	mq.Lock.RLock()
	msg, ok := mq.Queue[key]
	mq.Lock.RUnlock()
	if !ok {
		return false, fmt.Errorf("message not exist")
	}
	return IsCausallyReady(msg, Vc, lock), nil

}

func IsCausallyReady(msg *MessageCausalMulticast, Vc map[string]uint64,
	lock *sync.RWMutex) bool {
	senderGuid := msg.SenderGuid
	lock.RLock()
	defer lock.RUnlock()

	if msg.VectorClock[senderGuid] != Vc[senderGuid]+1 {
		return false
	}

	for k, v := range Vc {
		if k != senderGuid {
			if msg.VectorClock[k] > v {
				return false
			}
		}
	}
	return true
}
