package overlay

import (
	"log"
	"sdcc_dkv/replica"
)

const (
	JoinRingService  = "RpcRing.JoinRing"
	LeaveRingService = "RpcRing.LeaveRing"
)

type RpcRing struct {
	Replica *replica.Replica
}

func (rpcRing *RpcRing) Init(replica *replica.Replica) {
	rpcRing.Replica = replica
}

type JoinRingArgs struct {
	Hostname      string
	OverlayPort   string
	MulticastPort string
	DataPort      string
}

func (rpcRing *RpcRing) JoinRing(args *JoinRingArgs, reply *bool) error {
	log.Println(rpcRing.Replica.Node.Hostname, "joinRing method called")
	err := rpcRing.Replica.Join(args.Hostname, args.OverlayPort, args.MulticastPort, args.DataPort)
	if err != nil {
		*reply = false
	}
	return err
}

type LeaveRingArgs struct {
	Hostname string
}

func (rpcRing *RpcRing) LeaveRing(args *LeaveRingArgs, reply *bool) error {
	err := rpcRing.Replica.Leave(args.Hostname)
	if err != nil {
		*reply = false
	}
	return err
}
