package replica

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"sdcc_dkv/data"
	"sort"
	"sync"
	"time"
)

type VNode struct {
	Hostname      string
	Guid          string
	OverlayPort   string
	MulticastPort string
	DataPort      string
}

func (vn *VNode) Init(hostname, guid, overlayPort, multicastPort, dataPort string) {
	vn.Hostname = hostname
	vn.Guid = guid
	vn.OverlayPort = overlayPort
	vn.MulticastPort = multicastPort
	vn.DataPort = dataPort

}

type Replica struct {
	Node              *VNode
	Replicas          map[string]*VNode
	CurrentIndex      int
	SortedKeys        []string
	Lock              sync.RWMutex
	DataStore         *data.Store
	ReplicationFactor int
	HashFunc          func() hash.Hash
	HashBit           int
	Latency           time.Duration
}

func (rep *Replica) Init(hostname, multicastPort, overlayPort, dataPort, hashFunc string, rf, latency int) {

	var hf func() hash.Hash
	if hashFunc == "md5" {
		hf = md5.New
	} else if hashFunc == "sha1" {
		hf = sha1.New
	} else if hashFunc == "sha256" {
		hf = sha256.New
	} else {
		log.Fatal("unsupported hash function: " + hashFunc)
	}

	node := new(VNode)
	node.Init(hostname, hex.EncodeToString(hf().Sum([]byte(hostname))), overlayPort, multicastPort, dataPort)
	sk := make([]string, 1)
	sk[0] = node.Guid
	datastore := new(data.Store)
	datastore.Init()
	rep.Node = node
	rep.Replicas = make(map[string]*VNode)
	rep.CurrentIndex = 0
	rep.SortedKeys = sk
	rep.DataStore = datastore
	rep.ReplicationFactor = rf
	rep.HashFunc = hf
	rep.HashBit = hf().Size()
	rep.Latency = time.Duration(latency) * time.Millisecond
	rep.Replicas[node.Guid] = node

}

func (rep *Replica) Info() {
	sortedNodes := ""
	rep.Lock.RLock()
	for i := range rep.SortedKeys {
		sortedNodes += fmt.Sprintf("%s, ", rep.SortedKeys[i])
	}
	rep.Lock.RUnlock()
	msg := fmt.Sprintf("node information\n"+
		"node: {\n"+
		"\tHost:%s,\n"+
		"\tMulticastPort:%s,\n"+
		"\tOverlayPort: %s,\n"+
		"\tDataPort: %s,\n"+
		"\tGuid:%s"+
		"\n}\n"+
		"current index: %d\n"+
		"sorted nodes: [%s]\n",
		rep.Node.Hostname,
		rep.Node.MulticastPort,
		rep.Node.OverlayPort,
		rep.Node.DataPort,
		rep.Node.Guid,
		rep.CurrentIndex,
		sortedNodes[:len(sortedNodes)-2])
	log.Print(msg)
}

func (rep *Replica) Join(hostname, oPort, mPort string) error {
	if hostname == "" {
		return errors.New("node is empty")
	}
	guid := hex.EncodeToString(rep.HashFunc().Sum([]byte(hostname)))
	rep.Lock.Lock()
	defer rep.Lock.Unlock()

	if _, ok := rep.Replicas[guid]; ok {
		return errors.New("node exists")
	}
	node := &VNode{Hostname: hostname, Guid: guid, OverlayPort: oPort, MulticastPort: mPort}
	rep.Replicas[guid] = node
	rep.SortedKeys = append(rep.SortedKeys, node.Guid)

	sort.Slice(rep.SortedKeys, func(i, j int) bool {
		return rep.SortedKeys[i] < rep.SortedKeys[j]
	})

	for i := 0; i < len(rep.SortedKeys); i++ {
		if rep.SortedKeys[i] == node.Guid {
			rep.CurrentIndex = i
			break
		}
	}

	return nil
}

func (rep *Replica) Leave(hostname string) error {
	if hostname == "" {
		return errors.New("hostname is empty")
	}
	guid := hex.EncodeToString(rep.HashFunc().Sum([]byte(hostname)))
	rep.Lock.Lock()
	defer rep.Lock.Unlock()

	delete(rep.Replicas, guid)

	index := sort.Search(len(rep.SortedKeys), func(i int) bool {
		return rep.SortedKeys[i] >= guid
	})

	if index < len(rep.SortedKeys) && rep.SortedKeys[index] == guid {
		rep.SortedKeys = append(rep.SortedKeys[:index], rep.SortedKeys[index+1:]...)
	}

	return nil
}

func (rep *Replica) LookupNode(key string) (*VNode, int) {
	rep.Lock.RLock()
	defer rep.Lock.RUnlock()
	if len(rep.Replicas) == 0 {
		return nil, 0
	}
	index := sort.Search(len(rep.SortedKeys), func(i int) bool {
		return rep.SortedKeys[i] >= key
	})
	if index == len(rep.SortedKeys) {
		index = 0
	}
	nodeKey := rep.SortedKeys[index]
	return rep.Replicas[nodeKey], index
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
			hexStr := rep.SortedKeys[i]
			logMsg += "\t[hostname:" + rep.Replicas[hexStr].Hostname + ", id:" + rep.Replicas[hexStr].Guid + "]\n"
		}
		log.Println(logMsg[:len(logMsg)-1])

	}
}
