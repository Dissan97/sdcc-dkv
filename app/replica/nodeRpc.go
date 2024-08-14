package replica

import (
	"errors"
	"fmt"
	"log"
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
	R *Replica
}

type JoinNetArgs struct {
	N *VNode
}

type JoinNetReply struct {
	Success  bool
	Replicas map[uint64]*VNode
}

type LeaveNetArgs struct {
	Node *VNode
}

type LeaveNetReply struct {
	Success bool
}

type GetNodeArgs struct {
	Key uint64
}

type GetNodeReply struct {
	Node *VNode
}

type PutRequestArgs struct {
	Key   uint64
	Value string
}
type PutRequestReply struct {
	Node    *VNode
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
	replicas, err := nRpc.R.Join(args.N)
	if err != nil {
		return err
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
	reply.Node, _ = nRpc.R.LookupNode(args.Key)
	if reply.Node == nil {
		return errors.New(GetNode + " Node not found")
	}
	return nil
}

// todo adjust this synchronization
func (nRpc *NodeRpc) PutRequest(args *PutRequestArgs, reply *PutRequestReply) error {
	vNode, index := nRpc.R.LookupNode(args.Key)
	if vNode == nil {
		reply.Success = false
		return errors.New(PutRequest + " Node not found")
	}
	nRpc.R.Lock.RLock()
	defer nRpc.R.Lock.RUnlock()
	size := len(nRpc.R.Replicas)

	if vNode.Id == nRpc.R.Node.Id {
		for i := 0; i < int(nRpc.R.ReplicationFactor); i++ {
			//forward request to the others and wait response circular ring
			v := nRpc.R.SortedKeys[(index+i)%size]
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
