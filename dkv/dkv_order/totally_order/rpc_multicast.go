package totally_order

import (
	"fmt"
	"log"
	"net/rpc"
	"sdcc_dkv/replica"
	"sync"
	"sync/atomic"
	"time"
)

type RpcMulticast struct {
	Queue                  *MulticastQueue
	CurrentReplica         *replica.Replica
	Timestamp              uint64
	TimeLock               sync.Mutex
	MaxRetries             int
	Wait                   time.Duration
	globalCounterMulticast uint64
	globalCounterReceive   uint64
}

type MessageUpdate struct {
	Key   string
	Value string
}

func (rm *RpcMulticast) Init(replica *replica.Replica, multicastRetries int, multicastWait time.Duration) {
	rm.Queue = new(MulticastQueue)
	rm.Queue.Init()
	rm.Timestamp = 0
	rm.CurrentReplica = replica
	rm.MaxRetries = multicastRetries
	rm.Wait = multicastWait
	rm.globalCounterMulticast = 0
	rm.globalCounterReceive = 0

}

func (rm *RpcMulticast) timeSend() uint64 {
	rm.TimeLock.Lock()
	defer rm.TimeLock.Unlock()
	rm.Timestamp += 1
	return rm.Timestamp
}

func (rm *RpcMulticast) timeReceive(otherClock uint64) {
	rm.TimeLock.Lock()
	defer rm.TimeLock.Unlock()
	clk := max(rm.Timestamp, otherClock)
	rm.Timestamp = clk + 1
}

// Multicast receive request from client
// assuming communication fifo and reliable
func (rm *RpcMulticast) Multicast(args *MessageUpdate, reply *bool) error {
	//add msg to the queue
	counter := atomic.AddUint64(&rm.globalCounterMulticast, 1)
	log.Printf("[#%d]: requested update multicast\n", counter)

	logicTime := rm.timeSend()

	nodesToContact := make(map[string]bool)
	curr := rm.CurrentReplica
	for i := 0; i < curr.ReplicationFactor; i++ {
		_, index := curr.LookupNode(args.Key)
		nodesToContact[curr.SortedKeys[(index+i)%len(curr.SortedKeys)]] = false
	}

	msg := &MessageMulticast{
		Key:         args.Key,
		Value:       args.Value,
		Timestamp:   logicTime,
		Guid:        rm.CurrentReplica.Node.Guid,
		Nodes:       nodesToContact,
		Deliverable: false,
	}

	// multicast all the nodes

	var wg sync.WaitGroup
	wg.Add(len(msg.Nodes))
	allSuccess := int32(0)
	for k := range msg.Nodes {
		go func(wg *sync.WaitGroup, node string, allSuccess *int32) {
			defer wg.Done()
			rm.CurrentReplica.Lock.RLock()
			defer rm.CurrentReplica.Lock.RUnlock()
			log.Printf("sending request to %s:%s\n", curr.Replicas[node].Hostname, curr.Node.MulticastPort)
			client, err := rm.dialWithRetries(curr.Replicas[node].Hostname + ":" + curr.Node.MulticastPort)
			if err != nil {
				log.Printf("[#%d] multicast request to %s:%s failed error:%s\n", counter, curr.Replicas[node].Hostname,
					curr.Node.MulticastPort, err.Error())
				atomic.AddInt32(allSuccess, -1)
				return
			}

			var reply bool
			err = client.Call("RpcMulticast.Receive", msg, &reply)
			if err != nil {
				log.Println("call RpcMulticast.Receive: ", err)
				atomic.StoreInt32(allSuccess, -1)
				return
			}

			err = client.Close()
			if err != nil {
				return
			}
		}(&wg, k, &allSuccess)
	}

	wg.Wait()
	if atomic.LoadInt32(&allSuccess) != 0 {
		*reply = false
		return nil
	}
	log.Printf("[#%d]call multicast update done all synchronized\n", counter)
	*reply = true
	return nil
}

// Receive function that receive the ack message
func (rm *RpcMulticast) Receive(args *MessageMulticast, reply *bool) error {
	counter := atomic.AddUint64(&rm.globalCounterReceive, 1)
	log.Printf("[#%d] receive requested for %s\n", counter, rm.Queue.getKey(args))
	rm.timeReceive(args.Timestamp)
	// (1) enqueue
	rm.Queue.Enqueue(args)
	// (2) sending ack to other process

	ackSender := rm.CurrentReplica.Node.Guid
	ackArgs := &MessageAck{
		Message: args,
		Sender:  ackSender,
	}
	// this part may be synchronized
	for guid := range args.Nodes {
		err := rm.sendAck(guid, ackArgs /*, &wg*/)
		if err != nil {
			*reply = false
			rm.Queue.ForceRemove(args)
			return nil
		}
	}

	// let deliver to application if the message is the first and if received all the ack
	for i := 0; i < rm.MaxRetries; i++ {
		targetMessage, deliverable := rm.Queue.GetMaxPriorityMessage()
		if rm.Queue.getKey(targetMessage) == rm.Queue.getKey(args) && deliverable {
			log.Printf("[#%d]: receive all ack received, message is deliverable\n", counter)
			rm.Queue.Dequeue()
			*reply = true
			return nil
		}
		time.Sleep(rm.Wait)
	}
	log.Printf("[#%d]: receive ack missing, message is not deliverable\n", counter)
	rm.Queue.ForceRemove(args)
	*reply = false
	return nil

}

func storeData() {
	log.Println("start store data")
}

func (rm *RpcMulticast) sendAck(guid string, ackArgs *MessageAck) error /*, wg *sync.WaitGroup)*/ {
	//defer wg.Done()
	curr := rm.CurrentReplica
	rm.timeSend()
	curr.Lock.RLock()
	defer curr.Lock.RUnlock()

	client, err := rm.dialWithRetries(curr.Replicas[guid].Hostname + ":" + curr.Replicas[guid].MulticastPort)

	if err != nil {
		fmt.Println("*RpcMulticast.sendAck Error dialing:", guid, err)
		return err
	}

	var reply bool
	err = client.Call("*RpcMulticast.ReceiveAck", ackArgs, &reply)
	if err != nil {
		fmt.Println("Error calling *RpcMulticast.ReceiveAck:", guid, err)
		return err
	}

	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (rm *RpcMulticast) ReceiveAck(args *MessageAck, reply *bool) error {
	counter := atomic.AddUint64(&rm.globalCounterReceive, 1)
	log.Printf("[#%d]: received ack for %s\n", counter, rm.Queue.getKey(args.Message))
	rm.timeReceive(args.Message.Timestamp)
	rm.Queue.ManageAckForTheQueue(args)
	*reply = true
	return nil
}

func (rm *RpcMulticast) dialWithRetries(host string) (*rpc.Client, error) {
	var err error
	var client *rpc.Client
	for i := 0; i < rm.MaxRetries; i++ {
		client, err = rpc.Dial("tcp", host)
		if err != nil {
			time.Sleep(rm.Wait)
			continue
		}
		break
	}
	return client, err
}

type UpdateMessage struct {
	Key           string
	Value         string
	SelectedNodes []string
}
