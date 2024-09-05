package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sdcc_dkv/data_http"
	"sdcc_dkv/dkv_order/totally_order"
	"sdcc_dkv/overlay"
	"sdcc_dkv/replica"
	"strings"
	"sync"
	"time"
)

func main() {

	hostname := flag.String("hostname", "localhost", "the hostname of the server")
	overlayPort := flag.String("o_port", "9000", "the port of the server for overlay")
	multicastPort := flag.String("m_port", "10000", "the port of the server for dkv_order")
	dataPort := flag.String("d_port", "8080", "the port of the server for data")
	opMode := flag.String("mode", "scalar", "the operation mode")
	hashFunction := flag.String("hash", "sha1", "the hash function")
	replicationFactor := flag.Int("rf", 3, "the replication factor")
	nodesToContact := flag.String("nto_contact", "localhost:9000", "nodes to contact to form the network")
	nodeType := flag.String("type", "client", "the node type to use")
	multicastRetries := flag.Int("m_retries", 20, "the number of times to retry multicast")
	multicastWait := flag.Int("m_wait", 50, "how many milliseconds to wait between retry multicast")
	simulatedLatency := flag.Int("fake_latency", 0, "simulating network latency")
	flag.Parse()

	if *nodeType == "aclient" {
		runClient()
		return
	}
	log.Printf("Server started with this properties\n"+
		"hostname=%s\n"+
		"dkv_order port=%s\n"+
		"overlay port=%s\n"+
		"operation mode=%s\n"+
		"data port=%s\n"+
		"replication factor=%d\n"+
		"nodes to contatct=[%s]\n", *hostname, *multicastPort, *overlayPort, *opMode,
		*dataPort, *replicationFactor, *nodesToContact)

	newReplica := new(replica.Replica)
	foo := 0
	log.Println(foo)
	newReplica.Init(*hostname, *multicastPort, *overlayPort, *dataPort, *hashFunction,
		*replicationFactor, *simulatedLatency)

	newReplica.Info()

	rpc.HandleHTTP()
	readyChan := make(chan bool)
	go startHttp(newReplica)
	go startMulticast(newReplica, *opMode, readyChan,
		*multicastRetries, time.Duration(*multicastWait)*time.Millisecond)
	startRing(newReplica, *nodesToContact, readyChan)

}

type KeyValue struct {
	Key   string
	Value string
}

func runClient() {

	file, err := os.Open("resources/testData.json")
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal("file close error", err)
		}
	}(file)

	// Read the file's contents
	byteValue, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	// Unmarshal the JSON array into a slice of Person structs
	var jData []KeyValue
	err = json.Unmarshal(byteValue, &jData)
	if err != nil {
		log.Fatal(err)
	}
	sum := int64(0)
	var reply bool

	hosts := []string{"localhost:10000", "localhost:10001", "localhost:10002"}
	wg := sync.WaitGroup{}
	wg.Add(len(hosts))
	for i := 0; i < 10; i++ {
		go func(group *sync.WaitGroup) {
			defer group.Done()
			for _, d := range jData {

				msg := &totally_order.MessageUpdate{
					Key:   d.Key,
					Value: d.Value,
				}
				h := rand.Int() % 3
				client, err := rpc.Dial("tcp", hosts[h])
				if err != nil {
					log.Fatal(err)
				}
				start := time.Now().UnixMicro()
				err = client.Call("RpcMulticast.Multicast", msg, &reply)
				if err != nil {
					log.Fatal(hosts[h], "Call error ", err)
				}
				if reply == false {
					log.Printf("cannot perform put for key: %s, %s\n", d.Key, d.Value)
				}
				end := time.Now().UnixMicro()
				log.Printf("request for key %s took: %f milliseconds\n", d.Key, float64(end-start)/float64(1000))
				sum += end - start
				i += 1
				err = client.Close()
				if err != nil {
					return
				}
				//time.Sleep(1 * time.Second)
			}
			log.Println("took average", (float64(sum)/float64(i))/float64(1000), "milliseconds")
		}(&wg)
	}
	wg.Wait()
}
func startRing(newReplica *replica.Replica, contact string, readyChan chan bool) {

	others := strings.Split(contact, ",")

	//registering rpc service
	ringRpc := new(overlay.RpcRing)
	ringRpc.Init(newReplica)
	err := rpc.Register(ringRpc)
	if err != nil {
		log.Fatal(newReplica.Node.Hostname, "cannot register rpc service: ", err)
	}

	listener, err := net.Listen("tcp", newReplica.Node.Hostname+":"+newReplica.Node.OverlayPort)
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(listener)
	if err != nil {
		log.Fatal(newReplica.Node.Hostname, "cannot listen on overlay port: ", newReplica.Node.OverlayPort, err)
	}
	var wg sync.WaitGroup
	wg.Add(len(others))
	for i := range others {
		go func(node string, wg *sync.WaitGroup) {
			defer wg.Done()
			host := strings.Split(node, ":")
			log.Println("contacting node", host[0]+":"+host[1])
			var err error
			var client *rpc.Client
			for j := 0; j < 3; j++ {
				client, err = rpc.Dial("tcp", host[0]+":"+host[1])
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				if err != nil {
					log.Fatal("dial error cannot contact the client after 3 retries", err)
				}
				break
			}
			log.Println("dial gone right")
			args := &overlay.JoinRingArgs{
				Hostname:      newReplica.Node.Hostname,
				OverlayPort:   newReplica.Node.OverlayPort,
				MulticastPort: newReplica.Node.MulticastPort}

			var reply bool
			err = client.Call(overlay.JoinRingService, args, &reply)
			if err != nil {
				log.Println(newReplica.Node.Hostname, "cannot join ring: ", err)
			}
			err = client.Close()
			if err != nil {
				return
			}
		}(others[i], &wg)

	}
	go func() {
		wg.Wait()
		readyChan <- true
	}()

	log.Println(newReplica.Node.Hostname, "ring manager started at addr", listener.Addr().String())
	for {
		accept, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go rpc.ServeConn(accept)

	}

}

func startMulticast(newReplica *replica.Replica, opMode string, ready chan bool, multicastRetries int,
	multicastWait time.Duration) {
	if <-ready {
		log.Println("multicast service ready")
	}
	newReplica.Info()
	var err error
	if opMode == "scalar" {
		service := new(totally_order.RpcMulticast)
		service.Init(newReplica, multicastRetries, multicastWait)
		err = rpc.Register(service)
	} else if opMode == "vector" {
		return
	} else {
		log.Fatal("Operation mode must be scalar or vector")
	}
	if err != nil {
		log.Fatal("cannot start multicast service ", opMode, err)
	}

	listener, err := net.Listen("tcp", newReplica.Node.Hostname+":"+newReplica.Node.MulticastPort)
	if err != nil {
		log.Fatal("cannot listen for multicast synchronization")
	}
	log.Println(newReplica.Node.Hostname, "multicast service started on", listener.Addr().String())

	for {
		accept, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go rpc.ServeConn(accept)

	}
}

func startHttp(newReplica *replica.Replica) {
	httpHandler := new(data_http.Handler)
	httpHandler.Init(newReplica)
	httpHandler.StartListening()
}
