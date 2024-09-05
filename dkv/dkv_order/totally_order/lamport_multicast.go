package totally_order

import (
	"log"
	"sort"
	"strconv"
	"sync"
)

type MessageMulticast struct {
	Key         string
	Value       string
	Timestamp   uint64
	Guid        string
	Nodes       map[string]bool //node1 -> hasAck? //node -> node2 -> hashAck?
	Deliverable bool
}

type MessageAck struct {
	Message *MessageMulticast
	Sender  string // hex.ToString([]guid)
}

type MulticastQueue struct {
	Queue        []*MessageMulticast
	QueueMap     map[string]*MessageMulticast
	MinTimestamp uint64
	Lock         sync.RWMutex
}

func (mq *MulticastQueue) Init() {
	mq.Queue = make([]*MessageMulticast, 0)
	mq.QueueMap = make(map[string]*MessageMulticast)
}

func (mq *MulticastQueue) getKey(msg *MessageMulticast) string {
	return msg.Guid + ":" + strconv.FormatUint(msg.Timestamp, 10)
}
func (mq *MulticastQueue) sort() {
	sort.Slice(mq.Queue, func(i, j int) bool {
		if mq.Queue[i].Guid < mq.Queue[j].Guid {
			return mq.Queue[i].Timestamp < mq.Queue[j].Timestamp
		}
		return mq.Queue[i].Timestamp < mq.Queue[j].Timestamp
	})
}
func (mq *MulticastQueue) Enqueue(msg *MessageMulticast) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()
	key := mq.getKey(msg)

	if _, ok := mq.QueueMap[key]; ok {
		log.Println("message already queued")
		return
	}

	mq.Queue = append(mq.Queue, msg)
	mq.sort()
	mq.QueueMap[mq.getKey(msg)] = msg
	mq.MinTimestamp = mq.Queue[0].Timestamp
}

func (mq *MulticastQueue) ManageAckForTheQueue(msg *MessageAck) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	key := mq.getKey(msg.Message)

	if target, ok := mq.QueueMap[key]; ok {
		target.Nodes[msg.Sender] = true
		size := len(target.Nodes)
		count := 0
		for _, b := range target.Nodes {
			if b {
				count++
			}
		}
		if count == size {
			target.Deliverable = true
		}

		return
	}
	log.Println("message not queued adding")
	msg.Message.Nodes[msg.Sender] = true
	mq.Queue = append(mq.Queue, msg.Message)
	mq.sort()
	mq.QueueMap[key] = msg.Message
	mq.MinTimestamp = mq.Queue[0].Timestamp
}

func (mq *MulticastQueue) Dequeue() *MessageMulticast {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	if len(mq.Queue) < 1 {
		return nil
	}

	mq.sort()

	msg := mq.Queue[0]

	mq.Queue = mq.Queue[1:]
	delete(mq.QueueMap, mq.getKey(msg))
	return msg

}

func (mq *MulticastQueue) GetMaxPriorityMessage() (*MessageMulticast, bool) {
	mq.Lock.RLock()
	defer mq.Lock.RUnlock()
	return mq.Queue[0], mq.Queue[0].Deliverable
}

func (mq *MulticastQueue) ForceRemove(args *MessageMulticast) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()
	key := mq.getKey(args)
	delete(mq.QueueMap, key)
	for i, msg := range mq.Queue {
		if key == mq.getKey(msg) {
			mq.Queue = append(mq.Queue[:i], mq.Queue[i+1:]...)
		}
	}

}
