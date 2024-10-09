package causal_order

import (
	"fmt"
	"log"
	"sdcc_dkv/data"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RpcCausalMulticast Assuming leader process the process which manage the key following ordered node are replicas
type RpcCausalMulticast struct {
	Queue                  *MulticastQueue
	CurrentReplica         *replica.Replica
	VectorClock            map[string]uint64 // group vectorClocks
	TimeLock               sync.RWMutex
	globalCounterMulticast uint64
	globalCounterReceive   uint64
}

type MessageUpdate struct {
	Key       string
	Value     string
	Operation string
}

type MessageAck struct {
	Message *MessageCausalMulticast
	Sender  string
}

func (rpcMulticast *RpcCausalMulticast) Init(replica *replica.Replica) {
	rpcMulticast.CurrentReplica = replica
	rpcMulticast.CurrentReplica.Lock.RLock()
	defer rpcMulticast.CurrentReplica.Lock.RUnlock()
	rpcMulticast.globalCounterMulticast = 0
	rpcMulticast.globalCounterReceive = 0

	// Initialize vector clock for each replica
	rpcMulticast.VectorClock = make(map[string]uint64)
	log.Printf("Start init vector clock\n")
	logMessage := "Vector clock: "
	for _, guid := range replica.SortedKeys {
		rpcMulticast.VectorClock[guid] = 0
		logMessage += fmt.Sprintf("{guid:%s, clk:%d}\n", guid, rpcMulticast.VectorClock[guid])
	}
	log.Printf("RpcCausalMulticast initialized %s", logMessage)

	rpcMulticast.Queue = new(MulticastQueue)
	rpcMulticast.Queue.Init(rpcMulticast.VectorClock, &rpcMulticast.TimeLock)
}

// Multicast receive request from client
func (rpcMulticast *RpcCausalMulticast) Multicast(args *MessageUpdate, reply *bool) error {

	rpcMulticast.CurrentReplica.SimulateLatency()
	log.Printf("requested multicast for key=%s", args.Key)
	operation := strings.ToLower(args.Operation)
	if operation == "del" || operation == "delete" || operation == "put" {
		defer rpcMulticast.logOperation(args, reply)
	} else {
		log.Printf("Multicast operation not recognized: %s", operation)
		*reply = false
		return nil
	}
	curr := rpcMulticast.CurrentReplica

	// now handle the operation

	vc := rpcMulticast.timeSend()

	msg := &MessageCausalMulticast{
		Key:         args.Key,
		Value:       args.Value,
		VectorClock: vc,
		Guid:        curr.Node.Guid,
		Deliverable: false,
		Operation:   operation,
	}

	var wg sync.WaitGroup
	wg.Add(len(vc))
	allSuccess := int32(0)

	for k := range vc {

		go func(wg *sync.WaitGroup, guid string, allSuccess *int32) {
			defer wg.Done()
			rpcMulticast.CurrentReplica.Lock.RLock()
			defer rpcMulticast.CurrentReplica.Lock.RUnlock()
			log.Printf("Sending multicast request to targetNode %s", guid)
			client, err := rpcMulticast.CurrentReplica.DialWithRetries(curr.Replicas[guid].Hostname +
				":" + curr.Node.MulticastPort)
			defer utils.CloseClient(client)
			if err != nil {
				log.Printf("Multicast request to targetNode %s failed with error: %s",
					curr.Node.Guid, err.Error())
				atomic.AddInt32(allSuccess, -1)
				return
			}
			var reply bool
			err = client.Call("RpcCausalMulticast.Receive", msg, &reply)
			if err != nil {
				log.Printf("Call to RpcCausalMulticast.Receive failed for targetNode %s: %s", guid, err.Error())
				atomic.StoreInt32(allSuccess, -1)
				return
			}
			if !reply {
				atomic.AddInt32(allSuccess, -1)
				log.Printf("Multicast request to targetNode %s replied %v", guid, reply)
				return
			}

			log.Printf("Multicast request to targetNode %s succeeded", guid)
		}(&wg, k, &allSuccess)

	}
	wg.Wait()
	if atomic.LoadInt32(&allSuccess) != 0 {
		log.Printf("Multicast request failed for one or more nodes")
		*reply = false
		return nil
	}

	log.Printf("Multicast request completed successfully storing "+
		"{VectorClock: %s, key=%s, value=%s}\n", rpcMulticast.getVectorString(), msg.Key, msg.Value)
	*reply = false
	return nil
}

func (rpcMulticast *RpcCausalMulticast) logOperation(args *MessageUpdate, reply *bool) {
	if *reply == true {
		values := ""
		if strings.ToLower(args.Operation) == "put" {
			values = fmt.Sprintf("(key=%s, value=%s)", args.Key, args.Value)
		} else {
			values = fmt.Sprintf("(key=%s)", args.Key)
		}
		log.Printf("completing %s with %s\n", args.Operation, values)
	}

}

// Receive function for handling incoming multicast messages
func (rpcMulticast *RpcCausalMulticast) Receive(args *MessageCausalMulticast, reply *bool) error {

	rpcMulticast.CurrentReplica.SimulateLatency()
	counter := atomic.AddUint64(&rpcMulticast.globalCounterReceive, 1)
	log.Printf("[#%d]: Received multicast message for key: %s", counter, args.Key)
	log.Printf("got message guidGroup=%s which leadername=%s", args.Guid,
		rpcMulticast.CurrentReplica.Replicas[args.Guid].Hostname)
	// (1) Enqueue the received message
	rpcMulticast.Queue.Enqueue(args)
	log.Printf("[#%d]: Enqueued message with key: %s", counter, args.Key)

	// check deliverable if deliverable deliver message
	log.Printf("check for causally ordered")
	for i := 0; i < rpcMulticast.CurrentReplica.RetryDial; i++ {
		if rpcMulticast.Queue.IsCausallyReady(args) {
			rpcMulticast.timeReceive(args)
			*reply = rpcMulticast.deliverOperation(args)
			rpcMulticast.Queue.Dequeue(args)
			return nil
		}
		time.Sleep(time.Duration(i+1) * rpcMulticast.CurrentReplica.RetryWait)
	}
	rpcMulticast.Queue.Dequeue(args)
	*reply = false
	return nil
}

func (rpcMulticast *RpcCausalMulticast) timeSend() map[string]uint64 {
	rpcMulticast.TimeLock.RLock()
	defer rpcMulticast.TimeLock.RUnlock()

	ret := make(map[string]uint64)
	for k, v := range rpcMulticast.VectorClock {
		ret[k] = v
		if k == rpcMulticast.CurrentReplica.Node.Guid {
			ret[k]++
		}
	}
	log.Printf("msg vector clock: %v", ret)
	return ret
}
func (rpcMulticast *RpcCausalMulticast) timeReceive(msg *MessageCausalMulticast) {
	rpcMulticast.TimeLock.Lock()
	defer rpcMulticast.TimeLock.Unlock()
	for k := range rpcMulticast.VectorClock {
		actual := rpcMulticast.VectorClock[k]
		rpcMulticast.VectorClock[k] = max(actual, msg.VectorClock[k])
	}
	log.Printf("timeReceive: my actual vector clock: %v\n", rpcMulticast.VectorClock)
}

func (rpcMulticast *RpcCausalMulticast) getVectorString() string {
	return fmt.Sprint(rpcMulticast.VectorClock[rpcMulticast.CurrentReplica.Node.Guid])
}

func (rpcMulticast *RpcCausalMulticast) getTimestampFromVc(vc map[string]uint64) string {

	ret := "["
	index := 0
	for index = 0; index < len(rpcMulticast.CurrentReplica.SortedKeys)-1; index++ {
		guid := rpcMulticast.CurrentReplica.SortedKeys[index]
		ret += fmt.Sprintf("%s, %d, ", guid, vc[guid])
	}
	guid := rpcMulticast.CurrentReplica.SortedKeys[index]
	ret += fmt.Sprintf("%s, %d]", guid, vc[guid])
	return ret
}

func (rpcMulticast *RpcCausalMulticast) deliverOperation(args *MessageCausalMulticast) bool {
	timestamp := rpcMulticast.getTimestampFromVc(args.VectorClock)
	log.Printf("request deliver operation for key=%s and timestamp=%s\n", args.Key, timestamp)

	if args.Operation == "put" {
		rpcMulticast.CurrentReplica.DataStore.Put(
			args.Key,
			data.Value{
				Timestamp: timestamp,
				Val:       args.Value,
			})
		return true
	} else if args.Operation == "delete" || args.Operation == "del" {
		ret := rpcMulticast.CurrentReplica.DataStore.Del(args.Key)
		if ret.Timestamp != data.GetDefaultValue().Timestamp && ret.Val != data.GetDefaultValue().Val {
			return true
		}
	}
	return false
}
func (rpcMulticast *RpcCausalMulticast) GetRequestByNodes(key *string, ret *data.Value) error {
	rpcMulticast.CurrentReplica.SimulateLatency()
	log.Printf("Get requested for key=%s\n", *key)
	*ret = rpcMulticast.CurrentReplica.DataStore.Get(*key)
	log.Printf("Get response for key=%s, (timestamp=%s, value=%s)\n", *key, ret.Timestamp, ret.Val)
	return nil
}
