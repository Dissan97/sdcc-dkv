package totally_order

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

type RpcSequentialMulticast struct {
	Queue          *MulticastQueue
	CurrentReplica *replica.Replica
	Timestamp      uint64
	TimeLock       sync.Mutex
}

type MessageUpdate struct {
	Operation string
	Key       string
	Value     string
}

// Init Initialize the RpcSequentialMulticast structure
func (rpcMulticast *RpcSequentialMulticast) Init(replica *replica.Replica) {
	rpcMulticast.Queue = new(MulticastQueue)
	rpcMulticast.Queue.Init()
	rpcMulticast.Timestamp = 0
	rpcMulticast.CurrentReplica = replica
	log.Println("RpcSequentialMulticast initialized")
}

// timeSend Increment local timestamp for sending a message (ensures total order)
func (rpcMulticast *RpcSequentialMulticast) timeSend() uint64 {
	rpcMulticast.TimeLock.Lock()
	defer rpcMulticast.TimeLock.Unlock()
	rpcMulticast.Timestamp += 1
	log.Printf("Incremented timestamp to %d for sending message", rpcMulticast.Timestamp)
	return rpcMulticast.Timestamp
}

// timeReceive Adjust local timestamp upon receiving a message
func (rpcMulticast *RpcSequentialMulticast) timeReceive(otherClock uint64) {
	rpcMulticast.TimeLock.Lock()
	defer rpcMulticast.TimeLock.Unlock()
	clk := max(rpcMulticast.Timestamp, otherClock)
	rpcMulticast.Timestamp = clk + 1
	log.Printf("Updated timestamp to %d after receiving message with clock %d", rpcMulticast.Timestamp, otherClock)
}

// Multicast receive request from client
func (rpcMulticast *RpcSequentialMulticast) Multicast(args *MessageUpdate, reply *bool) error {

	rpcMulticast.CurrentReplica.SimulateLatency()

	operation := strings.ToLower(args.Operation)
	if operation == "del" || operation == "delete" || operation == "put" {
		defer rpcMulticast.handleOperation(args, reply)
	} else {
		log.Printf("Multicast operation not recognized: %s", operation)
		*reply = false
		return nil
	}

	log.Printf("Requested update multicast for key: %s", args.Key)

	// Get logical time for the message
	logicTime := rpcMulticast.timeSend()

	// Select nodes to multicast the message
	nodesToContact := make(map[string]bool)
	curr := rpcMulticast.CurrentReplica
	ringSize := len(curr.SortedKeys)
	for i := 0; i < ringSize; i++ {
		_, index := curr.LookupNode(args.Key)
		nodesToContact[curr.SortedKeys[(index+i)%ringSize]] = false
	}

	msg := &MessageMulticast{
		Key:         args.Key,
		Value:       args.Value,
		Timestamp:   logicTime,
		Guid:        rpcMulticast.CurrentReplica.Node.Guid,
		Nodes:       nodesToContact,
		Deliverable: false,
		Operation:   operation,
	}

	log.Println("message to deliver for multicast", msg)

	// Multicast the message to all selected nodes
	var wg sync.WaitGroup
	wg.Add(len(msg.Nodes))
	allSuccess := int32(0)

	for k := range msg.Nodes {
		go func(wg *sync.WaitGroup, node string, allSuccess *int32) {
			defer wg.Done()
			rpcMulticast.CurrentReplica.Lock.RLock()
			log.Printf("Sending multicast request to node %s", node)
			client, err := rpcMulticast.CurrentReplica.DialWithRetries(curr.Replicas[node].Hostname +
				":" + curr.Node.MulticastPort)
			rpcMulticast.CurrentReplica.Lock.RUnlock()
			if err != nil {
				log.Printf("Multicast request to node %s failed with error: %s",
					node, err.Error())
				atomic.AddInt32(allSuccess, -1)
				return
			}
			defer utils.CloseClient(client)

			var reply bool
			err = client.Call("RpcSequentialMulticast.Receive", msg, &reply)
			if err != nil {
				log.Printf("Call to RpcSequentialMulticast.Receive failed for node %s: %s",
					node, err.Error())
				atomic.StoreInt32(allSuccess, -1)
				return
			}

			log.Printf("Multicast request to node %s succeeded", node)
		}(&wg, k, &allSuccess)
	}

	wg.Wait()
	log.Printf("Multicast: All receive call are done...")
	if atomic.LoadInt32(&allSuccess) != 0 {
		log.Printf("Multicast request failed for=%s one or more nodes", args.Key)
		*reply = false
		return nil
	}
	log.Printf("Multicast request completed successfully storing "+
		"{timestamp: %d, key=%s, value=%s}\n", msg.Timestamp, msg.Key, msg.Value)

	*reply = true
	return nil
}

// Receive function for handling incoming multicast messages
func (rpcMulticast *RpcSequentialMulticast) Receive(args *MessageMulticast, reply *bool) error {

	rpcMulticast.CurrentReplica.SimulateLatency()

	log.Printf("Received multicast message for key: %s", args.Key)

	// Update local clock
	rpcMulticast.timeReceive(args.Timestamp)

	// (1) Enqueue the received message
	rpcMulticast.Queue.Enqueue(args)
	log.Printf("Enqueued message with key: %s", args.Key)

	// (2) Send acknowledgment to other nodes
	ackSender := rpcMulticast.CurrentReplica.Node.Guid
	ackArgs := &MessageAck{
		Message: args,
		Sender:  ackSender,
	}
	successSendAck := int32(0)
	wg := &sync.WaitGroup{}
	wg.Add(len(args.Nodes))
	for guid := range args.Nodes {
		go func(guid string, ackArgs *MessageAck) {
			defer wg.Done()
			err := rpcMulticast.sendAck(guid, ackArgs)
			if err != nil {
				log.Printf("Failed to send acknowledgment to node %s", guid)
				rpcMulticast.Queue.RemoveMessage(args)
				atomic.AddInt32(&successSendAck, -1)
			}
		}(guid, ackArgs)
	}
	wg.Wait()
	if atomic.LoadInt32(&successSendAck) != 0 {
		log.Printf("Receive some ackknowledgement was not sent for %s", args.Key)
		rpcMulticast.Queue.RemoveMessage(args)
		*reply = false
		return nil
	}
	// Check if the message is deliverable
	for i := 0; i < rpcMulticast.CurrentReplica.RetryDial; i++ {
		targetMessage, deliverable := rpcMulticast.Queue.GetMaxPriorityMessage()
		if rpcMulticast.Queue.getKey(targetMessage) == rpcMulticast.Queue.getKey(args) && deliverable {
			log.Printf("All acknowledgments received, message %s is deliverable", args.Key)
			rpcMulticast.Queue.RemoveMessage(targetMessage)
			log.Printf("Dequeued message with key: %s ready to deliver", args.Key)
			*reply = false
			if args.Operation == "put" {
				rpcMulticast.CurrentReplica.DataStore.Put(
					targetMessage.Key,
					data.Value{
						Timestamp: fmt.Sprintf("%d", targetMessage.Timestamp),
						Val:       targetMessage.Value,
					})

				*reply = true
			} else if args.Operation == "delete" || args.Operation == "del" {
				ret := rpcMulticast.CurrentReplica.DataStore.Del(targetMessage.Key)
				if ret.Timestamp != data.GetDefaultValue().Timestamp && ret.Val != data.GetDefaultValue().Val {
					*reply = true
				}
			}

			return nil
		}
		time.Sleep(time.Duration(i+1) * rpcMulticast.CurrentReplica.RetryWait)
	}

	// If delivery is not possible after retries
	log.Printf("Acknowledgments missing, message %s not deliverable", args.Key)
	rpcMulticast.Queue.RemoveMessage(args)
	*reply = false
	return nil
}

// sendAck Send acknowledgment to the specified node
func (rpcMulticast *RpcSequentialMulticast) sendAck(guid string, ackArgs *MessageAck) error {

	curr := rpcMulticast.CurrentReplica
	curr.Lock.RLock()
	replicaInfo := curr.Replicas[guid]
	curr.Lock.RUnlock()

	log.Printf("Sending acknowledgment to node %s", guid)

	client, err := rpcMulticast.CurrentReplica.DialWithRetries(replicaInfo.Hostname + ":" + replicaInfo.MulticastPort)
	if err != nil {
		log.Printf("Error dialing node %s for acknowledgment: %s", guid, err.Error())
		return err
	}
	defer utils.CloseClient(client)

	var reply bool
	err = client.Call("RpcSequentialMulticast.ReceiveAck", ackArgs, &reply)
	if err != nil {
		log.Printf("Error calling RpcSequentialMulticast.ReceiveAck on node %s: %s", guid, err.Error())
		return err
	}

	log.Printf("Acknowledgment successfully sent to node %s", guid)
	return nil
}

// ReceiveAck Handle received acknowledgment
func (rpcMulticast *RpcSequentialMulticast) ReceiveAck(args *MessageAck, reply *bool) error {

	rpcMulticast.CurrentReplica.SimulateLatency()
	log.Printf("Received acknowledgment for message key: %s", args.Message.Key)
	rpcMulticast.Queue.ManageAckForTheQueue(args)
	*reply = true
	return nil
}

func (rpcMulticast *RpcSequentialMulticast) handleOperation(args *MessageUpdate, reply *bool) {
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

func (rpcMulticast *RpcSequentialMulticast) GetRequestByNodes(key *string, ret *data.Value) error {
	rpcMulticast.CurrentReplica.SimulateLatency()
	log.Printf("Get requested for key=%s\n", *key)
	*ret = rpcMulticast.CurrentReplica.DataStore.Get(*key)
	log.Printf("Get response for key=%s, (timestamp=%s, value=%s)\n", *key, ret.Timestamp, ret.Val)
	return nil
}

type UpdateMessage struct {
	Key           string
	Value         string
	SelectedNodes []string
}
