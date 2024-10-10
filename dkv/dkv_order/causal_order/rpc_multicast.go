package causal_order

import (
	"fmt"
	"log"
	"sdcc_dkv/data"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strings"
	"sync"
	"time"
)

type BufferMessage struct {
	Message *MessageCausalMulticast
	Counter int
}

// RpcCausalMulticast Assuming leader process the process which manage the key following ordered node are replicas
type RpcCausalMulticast struct {
	Queue            *MulticastQueue
	CurrentReplica   *replica.Replica
	VectorClock      map[string]uint64 // group vectorClocks
	TimeLock         sync.RWMutex
	BufferedMessages map[string]*BufferMessage
	BufferLock       sync.RWMutex
}

type MessageUpdate struct {
	Key       string
	Value     string
	Operation string
}

func (rpcMulticast *RpcCausalMulticast) Init(replica *replica.Replica) {
	rpcMulticast.CurrentReplica = replica
	rpcMulticast.CurrentReplica.Lock.RLock()
	defer rpcMulticast.CurrentReplica.Lock.RUnlock()

	// Initialize vector clock for each replica
	rpcMulticast.VectorClock = make(map[string]uint64)
	log.Printf("Start init vector clock actual sorted nodes %v\n", replica.SortedKeys)

	for i := 0; i < len(replica.SortedKeys); i++ {
		guid := replica.SortedKeys[i]
		rpcMulticast.VectorClock[guid] = 0
	}

	log.Printf("RpcCausalMulticast initialized vector clock %v", rpcMulticast.VectorClock)
	rpcMulticast.Queue = new(MulticastQueue)
	rpcMulticast.Queue.Init()
	rpcMulticast.BufferedMessages = make(map[string]*BufferMessage)
}

// Multicast receive request from client
func (rpcMulticast *RpcCausalMulticast) Multicast(args *MessageUpdate, reply *bool) error {
	rpcMulticast.CurrentReplica.SimulateLatency()
	log.Printf("requested casusal multicast for %v\n", args)

	// update of clock copy of it to avoid concurrency issue
	vc := rpcMulticast.timeSend()

	// create the message
	msg := &MessageCausalMulticast{
		Key:         args.Key,
		Value:       args.Value,
		VectorClock: vc,
		SenderGuid:  rpcMulticast.CurrentReplica.Node.Guid,
		Operation:   args.Operation,
	}

	// can deliver the message
	*reply = rpcMulticast.deliverOperation(msg)

	if *reply {
		log.Printf("message delivered %v\n", args)
		bufferKey := msg.GetKey()
		// buffering message if there is someone failed
		rpcMulticast.BufferLock.Lock()
		rpcMulticast.BufferedMessages[bufferKey] = &BufferMessage{
			Message: msg,
			Counter: 0,
		}
		rpcMulticast.BufferLock.Unlock()
		go rpcMulticast.ContactOtherNodes(msg)
	} else {
		log.Printf("cannot deliver the message %v\n", args)
	}
	return nil
}

func (rpcMulticast *RpcCausalMulticast) ContactOtherNodes(msg *MessageCausalMulticast) {
	contactSize := len(rpcMulticast.CurrentReplica.SortedKeys) - 1
	guids := make([]string, contactSize)
	i := 0
	key := msg.GetKey()
	rpcMulticast.CurrentReplica.Lock.RLock()
	for _, guid := range rpcMulticast.CurrentReplica.SortedKeys {
		if msg.SenderGuid != guid {
			guids[i] = guid
			i++
		}
	}
	rpcMulticast.CurrentReplica.Lock.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(contactSize)
	for i = 0; i < contactSize; i++ {
		go rpcMulticast.doCall(msg, key, guids[i], &wg)
	}
	wg.Wait()
	rpcMulticast.BufferLock.Lock()
	if rpcMulticast.BufferedMessages[key].Counter == contactSize {
		log.Printf("No need for BufferedMessage %v\n", msg)
		delete(rpcMulticast.BufferedMessages, key)
	}
	rpcMulticast.BufferLock.Unlock()
}

func (rpcMulticast *RpcCausalMulticast) doCall(msg *MessageCausalMulticast, key, guid string, wg *sync.WaitGroup) {

	node := rpcMulticast.CurrentReplica.Replicas[guid]
	hostname := node.Hostname + ":" + node.MulticastPort
	client, err := rpcMulticast.CurrentReplica.DialWithRetries(hostname)
	if err != nil {
		log.Printf("cannot connect to replica %v\n", err)
		return
	}

	var replay bool
	err = client.Call("RpcCausalMulticast.Receive", msg, &replay)
	if err != nil {
		log.Printf("cannot call receive from replica %v\n", err)
		utils.CloseClient(client)
		return
	}
	utils.CloseClient(client)
	if !replay {
		log.Printf("receive from replica %v\n", replay)
		rpcMulticast.doCall(msg, key, guid, wg)
	} else {
		rpcMulticast.BufferLock.Lock()
		rpcMulticast.BufferedMessages[key].Counter++
		rpcMulticast.BufferLock.Unlock()
		wg.Done()
	}
	return
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
	log.Printf("received called for message %v\n", args)
	// check already causally ready
	*reply = false
	if args == nil {
		log.Printf("args cannot be nil")
		return nil
	}
	if IsCausallyReady(args, rpcMulticast.VectorClock, &rpcMulticast.TimeLock) {
		log.Printf("message is ready to deliver %v\n", args)
		*reply = rpcMulticast.deliverOperation(args)
		if *reply {
			log.Printf("message delivered %v\n", args)
			rpcMulticast.timeReceive(args)
		} else {
			log.Printf("cannot deliver the message %v\n", args)
		}
		return nil
	}

	// message not ready
	rpcMulticast.Queue.AddMessage(args)
	key := args.GetKey()

	var ok bool
	var err error
	for i := 0; i < rpcMulticast.CurrentReplica.RetryDial; i++ {
		log.Printf("[#%d] wating to syncrhonize %v", i, args)
		if ok, err = rpcMulticast.Queue.CheckDeliverable(key, rpcMulticast.VectorClock, &rpcMulticast.TimeLock); ok {
			log.Printf("message now is ready to deliver %v\n", args)
			*reply = rpcMulticast.deliverOperation(args)
			if *reply {
				log.Printf("message delivered %v\n", args)
				rpcMulticast.timeReceive(args)
			}
			break
		}

		if err != nil {
			log.Printf("not in queue message %s\n", args.GetKey())
			rpcMulticast.CurrentReplica.Lock.RLock()
			sender := rpcMulticast.CurrentReplica.Replicas[args.SenderGuid]
			rpcMulticast.CurrentReplica.Lock.RUnlock()
			client, err := rpcMulticast.CurrentReplica.DialWithRetries(sender.Hostname + ":" + sender.MulticastPort)
			if err != nil {
				log.Printf("cannot connect to replica %v\n", err)
				*reply = false
				return nil
			}
			var ret MessageCausalMulticast
			err = client.Call("RpcCausalMulticast.PullMessage", args.GetKey(), &ret)
			if err != nil {
				log.Printf("cannot pull message from replica %v\n", err)
				*reply = false
				utils.CloseClient(client)
				return nil
			}
			utils.CloseClient(client)
			args = &ret
			rpcMulticast.Queue.AddMessage(args)
			continue
		}

		time.Sleep(time.Duration(i<<1) * rpcMulticast.CurrentReplica.RetryWait)
	}
	rpcMulticast.Queue.RemoveMessage(args)
	return nil
}

func (rpcMulticast *RpcCausalMulticast) PullMessage(args *string, reply *MessageCausalMulticast) error {
	log.Printf("pulling message %v\n", args)
	rpcMulticast.BufferLock.RLock()
	defer rpcMulticast.BufferLock.RUnlock()

	msg, ok := rpcMulticast.BufferedMessages[*args]
	if !ok {
		reply = nil
	}
	reply = msg.Message

	return nil
}

func (rpcMulticast *RpcCausalMulticast) timeSend() map[string]uint64 {
	rpcMulticast.TimeLock.Lock()
	defer rpcMulticast.TimeLock.Unlock()
	ret := make(map[string]uint64)
	for k := range rpcMulticast.VectorClock {
		if k == rpcMulticast.CurrentReplica.Node.Guid {
			rpcMulticast.VectorClock[k]++
		}
		ret[k] = rpcMulticast.VectorClock[k]
	}
	return ret
}
func (rpcMulticast *RpcCausalMulticast) timeReceive(msg *MessageCausalMulticast) {
	rpcMulticast.TimeLock.Lock()
	defer rpcMulticast.TimeLock.Unlock()
	for k := range msg.VectorClock {
		rpcMulticast.VectorClock[k] = max(rpcMulticast.VectorClock[k], msg.VectorClock[k])
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
