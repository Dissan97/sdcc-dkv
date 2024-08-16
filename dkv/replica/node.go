package replica

import (
	"errors"
	"sdcc_dkv/clock"
	"sdcc_dkv/data"
	"sdcc_dkv/utils"
	"sort"
	"sync"
)

const (
	Causal     = "CAUSAL"
	Sequential = "SEQUENTIAL"
)

type VNode struct {
	Hostname    string
	ControlPort string
	DataPort    string
	Id          uint64
	SC          *clock.ScalarClock
	VC          *clock.VectorClock
	OpMode      string
	active      int
}

type Replica struct {
	Node         VNode
	Replicas     map[uint64]*VNode
	CurrentIndex int
	SortedKeys   []uint64
	Lock         sync.RWMutex
	Data         *data.Store

	ReplicationFactor uint
}

func NewNode(hostname string, controlPort, dataPort, op string) *VNode {
	id := utils.HashKey(hostname)

	return &VNode{
		Hostname:    hostname,
		ControlPort: controlPort,
		DataPort:    dataPort,
		Id:          id,
		SC:          clock.NewScalar(),
		VC:          clock.NewVectorClock(id),
		OpMode:      op,
		active:      1,
	}
}

func NewReplica(node *VNode, replication uint) *Replica {

	sk := make([]uint64, 0)
	sk = append(sk, node.Id)
	replica := &Replica{
		Node:              *node,
		Replicas:          make(map[uint64]*VNode),
		SortedKeys:        sk,
		Data:              data.NewStore(),
		ReplicationFactor: replication,
	}

	replica.Replicas[node.Id] = node
	return replica
}

func (replica *Replica) Join(node *VNode) (map[uint64]*VNode, error) {

	if node == nil {
		return nil, errors.New("node is nil")
	}

	replica.Lock.Lock()
	defer replica.Lock.Unlock()

	if replica.Node.OpMode != node.OpMode {
		return nil, errors.New("node is not operating logic as Replica node")
	}

	if _, ok := replica.Replicas[node.Id]; ok {
		return nil, errors.New("node exist")
	}

	replica.Replicas[node.Id] = node
	replica.SortedKeys = append(replica.SortedKeys, node.Id)
	sort.Slice(replica.SortedKeys, func(i, j int) bool { return replica.SortedKeys[i] < replica.SortedKeys[j] })

	for i := 0; i < len(replica.SortedKeys); i++ {
		if replica.SortedKeys[i] == node.Id {
			replica.CurrentIndex = i
			break
		}
	}

	return replica.Replicas, nil
}

func (replica *Replica) Leave(node *VNode) error {
	if node != nil {
		replica.Lock.Lock()
		defer replica.Lock.Unlock()
		delete(replica.Replicas, node.Id)
		index := sort.Search(len(replica.SortedKeys), func(i int) bool { return replica.SortedKeys[i] >= node.Id })
		if index < len(replica.SortedKeys) && replica.SortedKeys[index] == node.Id {
			replica.SortedKeys = append(replica.SortedKeys[:index], replica.SortedKeys[index+1:]...)
		}
		return nil
	}
	return errors.New("node is nil")
}

func (replica *Replica) LookupNode(key uint64) (*VNode, int) {
	if len(replica.Replicas) == 0 {
		return nil, 0
	}

	index := sort.Search(len(replica.SortedKeys), func(i int) bool { return replica.SortedKeys[i] >= key })

	// If the index is equal to the length, wrap around to the first Node
	if index == len(replica.SortedKeys) {
		index = 0
	}

	nodeKey := replica.SortedKeys[index]
	return replica.Replicas[nodeKey], index
}

func (replica *Replica) GetReplicas() map[uint64]*VNode {
	replica.Lock.RLock()
	defer replica.Lock.RUnlock()
	return replica.Replicas
}
