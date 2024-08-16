package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strings"
)

//TODO Implement MulticastTotally Ordered Node.Multicast Node.Rpc

func main() {

	nodeId := flag.String("node", "localhost", "the node addr of the container")
	controlPort := flag.String("port", "9000", "the control port of the container")
	dataPort := flag.String("dataPort", "8080", "the data port of the container")
	clk := flag.String("clk", "SCALAR", "the clk time of the container")
	replicationFactor := flag.Uint("replicationFactor", 3, "the replication factor of the datastore")
	joinNode := flag.String("joinNode", "localhost", "the join node addr of the container")

	flag.Parse()
	fmt.Println("joinNode:", *joinNode)
	*clk = strings.ToUpper(*clk)

	if *clk != replica.Sequential && *clk != replica.Causal {
		log.Println("clk must be SEQUENTIAL or CAUSAL\nSEQUENTIAL CHOSEN")
		*clk = replica.Sequential
	}

	if *replicationFactor < 3 {
		log.Println("replication factor must be greater then 3\nREPLICATION FACTOR SET TO 3")
		*replicationFactor = 3
	}

	node := replica.NewNode(*nodeId+":", *controlPort, *dataPort, *clk)

	rep := replica.NewReplica(node, *replicationFactor)

	log.Println(
		"Vnode started\nhostname:", node.Hostname, "\ndataPort:", *dataPort,
		"\ncontrolPort:", *controlPort,
	)

	go startHttpServices(rep)

	//Join Operation

	var reply replica.JoinNetReply
	args := replica.JoinNetArgs{}
	args.N = node

	client, err := rpc.DialHTTP("tcp", *joinNode+":"+*controlPort)

	if err != nil {
		log.Fatal("rpc.DialHttp to "+*joinNode+":"+*controlPort+" error=", err)
	}

	err = client.Call(replica.JoinOp, &args, &reply)
	if err != nil {
		log.Fatal(*joinNode+":"+*controlPort+".JoinOp error=", err)
	}

	log.Println("node:", node.Hostname, "got reply from ", *joinNode+":"+*controlPort+" reply=", reply.Success)

	replicas := reply.Replicas

	for _, r := range replicas {
		if r.Id != node.Id {
			_, err := rep.Join(r)
			if err != nil {
				log.Println("rep.Join", err)
			}
		}
	}
	i := 0
	log.Println("Retrieving this replicas now add to mine known replicas")
	for _, r := range rep.GetReplicas() {
		log.Println(i, "hostname: ", r.Hostname, "id: ", r.Id)
		i++
	}

	getArgs := replica.GetNodeArgs{
		Key: utils.HashKey("my_key:9000"),
	}
	var getReply replica.GetNodeReply
	err = client.Call(replica.GetNode, &getArgs, &getReply)
	if err != nil {
		log.Fatal("call GETNODE:", err)
	}
	fmt.Println("key: wanted=", getArgs.Key)
	fmt.Println(getReply.Node.Hostname, getReply.Node.Id)

}

func startHttpServices(rep *replica.Replica) {

	nodeRpc := new(replica.NodeRpc)
	nodeRpc.R = rep
	err := rpc.Register(nodeRpc)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	go func() {
		for {
			l, err := net.Listen("tcp", rep.Node.Hostname+":"+rep.Node.ControlPort)
			if err != nil {
				log.Fatal("listen", err)
			}
			err = http.Serve(l, nil)
			if err != nil {
				log.Println("serve", err)
			}
		}
	}()

	mux := replica.NewMuxServer(rep)
	err = http.ListenAndServe(rep.Node.Hostname+":"+rep.Node.DataPort, mux)
	if err != nil {
		log.Fatal("ListenAndServe", err)
	}

}
