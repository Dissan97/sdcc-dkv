package replica

import (
	"bytes"
	"encoding/hex"
	"errors"
	"log"
	"sdcc_dkv/clock"
	"sdcc_dkv/data"
	"sdcc_dkv/utils"
	"sort"
	"sync"
	"time"
)

type VNode struct {
	Hostname    string
	ControlPort string
	DataPort    string
	Guid        []byte
	SC          *clock.ScalarClock
	VC          *clock.VectorClock
	OpMode      string
	active      int
}

type Replica struct {
	Node              VNode
	Replicas          map[string]*VNode // Use string keys for easier map operations
	CurrentIndex      uint
	SortedKeys        [][]byte
	Lock              sync.RWMutex
	Data              *data.Store
	ReplicationFactor uint
}

func NewNode(hostname string, controlPort, dataPort, op string) *VNode {
	id := utils.HashKey(hostname)

	return &VNode{
		Hostname:    hostname,
		ControlPort: controlPort,
		DataPort:    dataPort,
		Guid:        id,
		SC:          clock.NewScalar(),
		VC:          clock.NewVectorClock(hex.EncodeToString(id)),
		OpMode:      op,
		active:      1,
	}
}

func NewReplica(node *VNode, replication uint) *Replica {

	sk := make([][]byte, 0)
	sk = append(sk, node.Guid)
	replica := &Replica{
		Node:              *node,
		Replicas:          make(map[string]*VNode),
		SortedKeys:        sk,
		Data:              data.NewStore(),
		ReplicationFactor: replication,
	}

	replica.Replicas[hex.EncodeToString(node.Guid)] = node
	return replica
}

func (rep *Replica) Join(node *VNode) (map[string]*VNode, error) {
	if node == nil {
		return nil, errors.New("node is nil")
	}

	rep.Lock.Lock()
	defer rep.Lock.Unlock()

	if rep.Node.OpMode != node.OpMode {
		return nil, errors.New("node is not operating logic as Replica node")
	}

	// Convert the byte slice key to a string for map operations
	nodeGuidStr := hex.EncodeToString(node.Guid)

	if _, ok := rep.Replicas[nodeGuidStr]; ok {
		return nil, errors.New("node exists")
	}

	rep.Replicas[nodeGuidStr] = node
	rep.SortedKeys = append(rep.SortedKeys, node.Guid)

	// Sort the byte slices lexicographically
	sort.Slice(rep.SortedKeys, func(i, j int) bool {
		return bytes.Compare(rep.SortedKeys[i], rep.SortedKeys[j]) < 0
	})

	// Find the index of the newly added node
	for i := 0; i < len(rep.SortedKeys); i++ {
		if bytes.Equal(rep.SortedKeys[i], node.Guid) {
			rep.CurrentIndex = uint(i)
			break
		}
	}

	return rep.Replicas, nil
}

func (rep *Replica) Leave(node *VNode) error {
	if node == nil {
		return errors.New("node is nil")
	}

	rep.Lock.Lock()
	defer rep.Lock.Unlock()

	// Remove the node from the Replicas map
	delete(rep.Replicas, hex.EncodeToString(node.Guid))

	// Find the index of the node in the SortedKeys slice using bytes.Compare
	index := sort.Search(len(rep.SortedKeys), func(i int) bool {
		return bytes.Compare(rep.SortedKeys[i], node.Guid) >= 0
	})

	// Check if the node was found in the SortedKeys slice
	if index < len(rep.SortedKeys) && bytes.Equal(rep.SortedKeys[index], node.Guid) {
		// Remove the node's GUID from the SortedKeys slice
		rep.SortedKeys = append(rep.SortedKeys[:index], rep.SortedKeys[index+1:]...)
	}

	return nil
}

func (rep *Replica) LookupNode(key []byte) (*VNode, int) {
	if len(rep.Replicas) == 0 {
		return nil, 0
	}

	// Perform a binary search to find the appropriate index
	index := sort.Search(len(rep.SortedKeys), func(i int) bool {
		return bytes.Compare(rep.SortedKeys[i], key) >= 0
	})

	// If the index is equal to the length, wrap around to the first Node
	if index == len(rep.SortedKeys) {
		index = 0
	}

	nodeKey := rep.SortedKeys[index]
	return rep.Replicas[hex.EncodeToString(nodeKey)], index
}

func (rep *Replica) GetReplicas() map[string]*VNode {
	rep.Lock.RLock()
	defer rep.Lock.RUnlock()
	return rep.Replicas
}

func (rep *Replica) LogDaemon() {
	for {
		time.Sleep(5 * time.Second)
		logMsg := "Active replicas\n"
		for i := range rep.SortedKeys {
			hexStr := hex.EncodeToString(rep.SortedKeys[i])
			logMsg += "\t[hostname:" + rep.Replicas[hexStr].Hostname + ", id:" + hex.EncodeToString(rep.Replicas[hexStr].Guid) + "]\n"
		}
		log.Println(logMsg[:len(logMsg)-1])

	}
}
