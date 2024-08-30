package dkvNet

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sdcc_dkv/replica"
)

const (
	JoinOp     = "NodeRpc.JoinNet"
	LeaveOp    = "NodeRpc.LeaveNet"
	GetNode    = "NodeRpc.LookupNode"
	PutRequest = "NodeRpc.PutRequest"
	GetRequest = "NodeRpc.GetRequest"
	DelRequest = "NodeRpc.DelRequest"
)

type NodeRpc struct {
	Rep *replica.Replica
}

type JoinNetArgs struct {
	N *replica.VNode
}

type JoinNetReply struct {
	Success  bool
	Replicas map[string]*replica.VNode
}

type LeaveNetArgs struct {
	Node *replica.VNode
}

type LeaveNetReply struct {
	Success bool
}

type GetNodeArgs struct {
	Key []byte
}

type GetNodeReply struct {
	Node *replica.VNode
}

type PutRequestArgs struct {
	Key   []byte
	Value string
}
type PutRequestReply struct {
	Node    *replica.VNode
	Success bool
}
type DelRequestArgs struct {
	Key uint64
}
type DelRequestReply struct {
	Success bool
}

type GetRequestArgs struct {
	Key uint64
}
type GetRequestReply struct {
	Value   string
	Success bool
}

func (nRpc *NodeRpc) JoinNet(args *JoinNetArgs, reply *JoinNetReply) error {
	log.Println(JoinOp, "called")

	//joining the net
	replicas, err := nRpc.Rep.Join(args.N)
	if err != nil {
		return err
	}

	// notify the targets
	nRpc.Rep.Lock.RLock()
	defer nRpc.Rep.Lock.RUnlock()

	for _, node := range replicas {
		go func(nT, node *replica.VNode) {
			client, err := rpc.DialHTTP("tcp", nT.Hostname+":"+nT.ControlPort)
			if err != nil {
				log.Println("notify ", JoinOp, err)
			}
			var dummy JoinNetReply
			args := JoinNetArgs{}
			args.N = node
			err = client.Call(JoinOp, &args, &dummy)

		}(args.N, node)
	}

	reply.Success = true
	reply.Replicas = replicas
	return nil
}

func (nRpc *NodeRpc) LeaveNet(args *LeaveNetArgs, reply *LeaveNetArgs) error {
	return nil
}

func (nRpc *NodeRpc) LookupNode(args *GetNodeArgs, reply *GetNodeReply) error {
	log.Println(GetNode, "called")
	reply.Node, _ = nRpc.Rep.LookupNode(args.Key)
	if reply.Node == nil {
		return errors.New(GetNode + " Node not found")
	}
	return nil
}

// todo adjust this synchronization
func (nRpc *NodeRpc) PutRequest(args *PutRequestArgs, reply *PutRequestReply) error {
	vNode, index := nRpc.Rep.LookupNode(args.Key)
	if vNode == nil {
		reply.Success = false
		return errors.New(PutRequest + " Node not found")
	}
	nRpc.Rep.Lock.RLock()
	defer nRpc.Rep.Lock.RUnlock()
	size := len(nRpc.Rep.Replicas)

	if bytes.Equal(vNode.Guid, nRpc.Rep.Node.Guid) {
		for i := 0; i < int(nRpc.Rep.ReplicationFactor); i++ {
			//forward request to the others and wait response circular ring
			v := nRpc.Rep.SortedKeys[(index+i)%size]
			fmt.Println(v)
		}
	}
	return nil
}

func (nRpc *NodeRpc) GetRequest(args *GetNodeArgs, reply *GetNodeReply) error {
	log.Println(GetRequest, "called")
	return nil
}

func (nRpc *NodeRpc) DelRequest(args *DelRequestArgs, reply *DelRequestReply) error {
	log.Println(DelRequest, "called")
	return nil
}
