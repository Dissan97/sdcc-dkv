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
	Nodes       map[string]bool // node1 -> hasAck?
	Deliverable bool
	Operation   string
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

// Init initializes the MulticastQueue by setting up the queue and map structures.
func (mq *MulticastQueue) Init() {
	mq.Queue = make([]*MessageMulticast, 0)
	mq.QueueMap = make(map[string]*MessageMulticast)
	log.Println("MulticastQueue initialized")
}

// getKey generates a unique key for the message based on its GUID and timestamp.
func (mq *MulticastQueue) getKey(msg *MessageMulticast) string {
	return msg.Key + ":" + msg.Guid + ":" + strconv.FormatUint(msg.Timestamp, 10)
}

// sort orders the messages in the queue by timestamp and GUID to ensure total ordering.
func (mq *MulticastQueue) sort() {
	sort.Slice(mq.Queue, func(i, j int) bool {
		if mq.Queue[i].Timestamp == mq.Queue[j].Timestamp {
			return mq.Queue[i].Guid < mq.Queue[j].Guid
		}
		return mq.Queue[i].Timestamp < mq.Queue[j].Timestamp
	})
	log.Println("MulticastQueue sorted by timestamp and GUID")
}

// Enqueue adds a new message to the queue and ensures it is sorted.
func (mq *MulticastQueue) Enqueue(msg *MessageMulticast) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	key := mq.getKey(msg)

	// Check if message is already in the queue
	if _, ok := mq.QueueMap[key]; ok {
		log.Println("Message already queued:", key)
		return
	}

	// Add message to the queue and queue map
	mq.Queue = append(mq.Queue, msg)
	mq.sort()
	mq.QueueMap[key] = msg
	mq.MinTimestamp = mq.Queue[0].Timestamp
	log.Printf("Message enqueued with key: %s, total messages in queue: %d\n", key, len(mq.Queue))
}

// ManageAckForTheQueue updates the acknowledgment status for a message in the queue.
// If all acknowledgments are received, it marks the message as deliverable.
func (mq *MulticastQueue) ManageAckForTheQueue(msg *MessageAck) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	key := mq.getKey(msg.Message)

	// Update acknowledgment status if message is in the queue
	if target, ok := mq.QueueMap[key]; ok {
		target.Nodes[msg.Sender] = true
		size := len(target.Nodes)
		count := 0
		for _, b := range target.Nodes {
			if b {
				count++
			}
		}

		// Mark message as deliverable if all ack are received
		if count == size {
			target.Deliverable = true
			log.Printf("Message with key: %s is now deliverable\n", key)
		} else {
			log.Printf("Acknowledgment received from %s for message key: %s. Total acks: %d/%d\n", msg.Sender, key, count, size)
		}
		return
	}

	// If message is not in the queue, add it
	log.Println("Message not queued, adding:", key)
	msg.Message.Nodes[msg.Sender] = true
	mq.Queue = append(mq.Queue, msg.Message)
	mq.sort()
	mq.QueueMap[key] = msg.Message
	mq.MinTimestamp = mq.Queue[0].Timestamp
	log.Printf("New message enqueued with key: %s after acknowledgment\n", key)
}

// GetMaxPriorityMessage returns the message with the highest priority (lowest timestamp)
// and whether it is deliverable or not.
func (mq *MulticastQueue) GetMaxPriorityMessage() (*MessageMulticast, bool) {
	mq.Lock.RLock()
	defer mq.Lock.RUnlock()

	// Check if the queue has any messages
	if len(mq.Queue) == 0 {
		log.Println("No messages in queue to get priority")
		return nil, false
	}

	log.Printf("Max priority message retrieved with key: %s, deliverable: %t\n", mq.getKey(mq.Queue[0]), mq.Queue[0].Deliverable)
	return mq.Queue[0], mq.Queue[0].Deliverable
}

// RemoveMessage removes a message from the queue based on the message key.
func (mq *MulticastQueue) RemoveMessage(args *MessageMulticast) {
	mq.Lock.Lock()
	defer mq.Lock.Unlock()

	key := mq.getKey(args)
	delete(mq.QueueMap, key)

	// Remove the message from the queue slice
	for i, msg := range mq.Queue {
		if key == mq.getKey(msg) {
			mq.Queue = append(mq.Queue[:i], mq.Queue[i+1:]...)
			break
		}
	}
}
