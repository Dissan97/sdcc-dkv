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

//todo

func main() {

	nodeId := flag.String("node", "localhost", "the node addr of the container")
	controlPort := flag.String("port", "9000", "the control port of the container")
	dataPort := flag.String("dataPort", "8080", "the data port of the container")
	clk := flag.String("clk", "SCALAR", "the clk time of the container")
	replicationFactor := flag.Uint("replicationFactor", 3, "the replication factor of the datastore")
	flag.Parse()

	*clk = strings.ToUpper(*clk)

	if *clk != replica.Sequential && *clk != replica.Causal {
		log.Println("clk must be SEQUENTIAL or CAUSAL\nSEQUENTIAL CHOSEN")
		*clk = replica.Sequential
	}

	if *replicationFactor < 3 {
		log.Println("replication factor must be greater then 3\nREPLICATION FACTOR SET TO 3")
		*replicationFactor = 3
	}

	log.Println("nodeId:", *nodeId)
	log.Println("controlPort:", *controlPort)
	log.Println("dataPort:", *dataPort)

	node := replica.NewNode(*nodeId+":"+*controlPort, *clk)

	fmt.Println("VNode.Hostname:", node.Hostname)
	fmt.Println("VNode.Id:", node.Id)
	rep := replica.NewReplica(node, *replicationFactor)

	if *nodeId == "localhost" {
		myKey := utils.HashKey("my_key")
		fmt.Println("my_key:", myKey)
		nodeRpc := new(replica.NodeRpc)
		nodeRpc.R = rep
		err := rpc.Register(nodeRpc)
		if err != nil {
			return
		}
		rpc.HandleHTTP()
		go func() {
			for {
				l, err := net.Listen("tcp", node.Hostname)
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
		err = http.ListenAndServe(":"+*dataPort, mux)
		if err != nil {
			log.Fatal("ListenAndServe", err)
		}
		select {}
	}

	var reply replica.JoinNetReply
	args := replica.JoinNetArgs{}
	args.N = node

	client, err := rpc.DialHTTP("tcp", "localhost:"+*controlPort)

	if err != nil {
		log.Fatal("dialing:", err)
	}

	err = client.Call(replica.JoinOp, &args, &reply)
	if err != nil {
		log.Fatal("call joinOp:", err)
	}

	fmt.Println("reply.success:", reply.Success)
	replicas := reply.Replicas

	for _, r := range replicas {
		if r.Id != node.Id {
			_, err := rep.Join(r)
			if err != nil {
				fmt.Println("rep.Join", err)
			}
		}
	}
	i := 0
	for _, r := range rep.GetReplicas() {
		fmt.Println(i, "hostname: ", r.Hostname, "id: ", r.Id)
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
