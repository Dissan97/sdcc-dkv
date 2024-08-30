package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sdcc_dkv/dkvNet"
	"sdcc_dkv/multicast"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strings"
	"time"
)

//TODO Implement MulticastTotally Ordered Node.Multicast Node.Rpc

func main() {

	nodeId := flag.String("node", "localhost", "the node addr of the container")
	controlPort := flag.String("port", "9000", "the control port of the container")
	dataPort := flag.String("dataPort", "8080", "the data port of the container")
	clk := flag.String("clk", utils.Sequential, "the clk time of the container")
	replicationFactor := flag.Uint("replicationFactor", 3, "the replication factor of the datastore")
	joinNode := flag.String("joinNode", "localhost", "the join node addr of the container")

	flag.Parse()
	fmt.Println("joinNode:", *joinNode)
	*clk = strings.ToUpper(*clk)

	if *clk != utils.Sequential && *clk != utils.Causal {
		log.Println("clk must be SEQUENTIAL or CAUSAL\nSEQUENTIAL CHOSEN")
		*clk = utils.Sequential
	}

	if *replicationFactor < 3 {
		log.Println("replication factor must be greater then 3\nREPLICATION FACTOR SET TO 3")
		*replicationFactor = 3
	}

	node := replica.NewNode(*nodeId, *controlPort, *dataPort, *clk)

	rep := replica.NewReplica(node, *replicationFactor)

	log.Println(
		"Vnode started\nhostname:", node.Hostname, "\ndataPort:", *dataPort,
		"\ncontrolPort:", *controlPort,
	)
	go rep.LogDaemon()
	go startHttpServices(rep)

	//Join Operation

	var reply dkvNet.JoinNetReply
	args := dkvNet.JoinNetArgs{}
	args.N = node
	var err error = nil
	var client *rpc.Client

	if *joinNode != *nodeId {

		for i := 0; i < 5; i++ {
			client, err = rpc.DialHTTP("tcp", *joinNode+":"+*controlPort)

			if err == nil {
				break
			}
			log.Println("rpc.DialHttp to "+*joinNode+":"+*controlPort+" error=", err)
			time.Sleep(100 * time.Millisecond)
		}
		if client == nil {
			log.Fatal("rpc.DialHTTP to "+*joinNode+":"+*controlPort+" error=", err)
		}
		for {
			err = client.Call(dkvNet.JoinOp, &args, &reply)
			if err == nil {
				break
			}
			if errors.Is(err, errors.New("node exists")) {
				log.Println("node exists")
				break
			}
			log.Println("problem in calling", dkvNet.JoinOp, "error=", err)
			time.Sleep(100 * time.Millisecond)
		}

		if err != nil {
			log.Fatal("rpc call failed", dkvNet.JoinOp, "error=", err)
		}
	}

	log.Println("node:", node.Hostname, "got reply from ", *joinNode+":"+*controlPort+" reply=", reply.Success)
	for {
		time.Sleep(5 * time.Second)
		n, _ := rep.LookupNode([]byte("ciao mondo"))

		log.Println("me=", node.Hostname, "wanted=", n.Hostname)
	}
	select {}

}

func startHttpServices(rep *replica.Replica) {

	nodeRpc := new(dkvNet.NodeRpc)
	if rep.Node.OpMode == "SCALAR" {
		multicastRpc := new(multicast.TORpc)
		multicastRpc.MTO = multicast.NewMTOQueue(rep)
		err := rpc.Register(multicastRpc)
		if err != nil {
			log.Fatal("rpc register error=", err)
		}
	}
	nodeRpc.Rep = rep
	err := rpc.Register(nodeRpc)
	if err != nil {
		log.Fatal("rpc register error=", err)
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

	mux := dkvNet.NewMuxServer(rep)
	err = http.ListenAndServe(rep.Node.Hostname+":"+rep.Node.DataPort, mux)
	if err != nil {
		log.Fatal("ListenAndServe", err)
	}

}
