package multicast

import (
	"bytes"
	"errors"
	"log"
	"net/rpc"
	"sdcc_dkv/data"
	"sdcc_dkv/replica"
	"sort"
	"sync"
	"time"
)

const (
	RpcMulticastAck    = "MulticastTORpc.MulticastAck"
	RpcMulticastUpdate = "MulticastTORpc.MulticastUpdate"
)

type TotallyOrderedMessage struct {
	Key       string
	Value     data.Value
	Timestamp uint64
	Id        []byte
	Ack       uint
	VNodes    []*replica.VNode
}

type TotallyOrdered struct {
	Rep              *replica.Replica
	Queue            []TotallyOrderedMessage
	Mu               sync.RWMutex
	HighestTimestamp uint64
}

func NewMTOQueue(rep *replica.Replica) *TotallyOrdered {
	return &TotallyOrdered{
		Rep:              rep,
		Queue:            make([]TotallyOrderedMessage, 0),
		HighestTimestamp: 0,
	}
}

func (mto *TotallyOrdered) requestToEnter(msg *TotallyOrderedMessage) {

	mto.Mu.Lock()
	defer mto.Mu.Unlock()
	mto.Queue = append(mto.Queue, *msg)
	sort.Slice(mto.Queue, func(i, j int) bool {
		if mto.Queue[i].Timestamp == mto.Queue[j].Timestamp {
			return bytes.Compare(mto.Queue[i].Id, mto.Queue[j].Id) < 0
		}
		return mto.Queue[i].Timestamp < mto.Queue[j].Timestamp
	})
	mto.HighestTimestamp = mto.Queue[len(mto.Queue)-1].Timestamp
}

type TORpc struct {
	MTO        *TotallyOrdered
	MaxRetries int
}

type Ack struct {
	Timestamp uint64
	Id        []byte
	Key       string
}

func (mtoRpc *TORpc) MulticastAck(msg *TotallyOrderedMessage, reply *bool) error {
	mtoRpc.MTO.Mu.RLock()
	defer mtoRpc.MTO.Mu.RUnlock()
	log.Println("node: "+mtoRpc.MTO.Rep.Node.Hostname, "Calling MulticastAck")
	for i := range mtoRpc.MTO.Queue {
		if mtoRpc.MTO.Queue[i].Timestamp == msg.Timestamp && mtoRpc.MTO.Queue[i].Key == msg.Key {
			*reply = true
			return nil
		}
	}
	*reply = false
	return errors.New(RpcMulticastAck)
}

func (mtoRpc *TORpc) waitAck(theNode *replica.VNode, msg *TotallyOrderedMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	args := &Ack{
		Timestamp: msg.Timestamp,
		Id:        msg.Id,
		Key:       msg.Key,
	}
	var reply bool
	for j := 0; j < mtoRpc.MaxRetries; j++ {
		log.Println("node: "+mtoRpc.MTO.Rep.Node.Hostname, "Calling waitAck")
		client, err := rpc.DialHTTP("tcp", theNode.Hostname+":"+theNode.ControlPort)

		if err != nil {
			log.Println(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err = client.Call(RpcMulticastAck, &args, &reply)

		if err != nil {
			err := client.Close()
			if err != nil {
				log.Println(err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err = client.Close()
		if err != nil {
			log.Println(err)
		}
		break
	}
}

func (mtoRpc *TORpc) MulticastUpdate(msg *TotallyOrderedMessage, reply *TotallyOrderedMessage) error {
	mtoRpc.MTO.requestToEnter(msg)
	var err error
	err = errors.New("receive failed")
	log.Println("node: "+mtoRpc.MTO.Rep.Node.Hostname, "Calling MulticastUpdate")
	var wg sync.WaitGroup
	for i := range msg.VNodes {
		if !bytes.Equal(msg.VNodes[i].Guid, mtoRpc.MTO.Rep.Node.Guid) {
			wg.Add(1)
			go mtoRpc.waitAck(msg.VNodes[i], msg, &wg)
		}
	}

	wg.Wait()

	err = errors.New("receive failed")
	reply = nil

	if mtoRpc.MTO.Queue[0].Ack == mtoRpc.MTO.Rep.ReplicationFactor {
		*reply = mtoRpc.MTO.Queue[0]
		err = nil
	}

	return err
}
