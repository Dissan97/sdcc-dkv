package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"sdcc_dkv/data_http"
	"sdcc_dkv/dkv_order/causal_order"
	"sdcc_dkv/dkv_order/totally_order"
	"sdcc_dkv/overlay"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strings"
	"sync"
	"time"
)

func main() {
	// Parse command line flags
	hostname := flag.String("hostname", "localhost", "the hostname of the server")
	overlayPort := flag.String("o_port", "9000", "the port of the server for overlay")
	multicastPort := flag.String("m_port", "10000", "the port of the server for dkv_order")
	dataPort := flag.String("d_port", "8080", "the port of the server for data")
	opMode := flag.String("mode", utils.Sequential, "the operation mode")
	hashFunction := flag.String("hash", "md5", "the hash function")
	nodesToContact := flag.String("nto_contact", "localhost:9000", "nodes to contact to form the network")
	retryDial := flag.Int("r_dial", 20, "the number of times to retry dial")
	retryWait := flag.Int("r_wait", 50, "how many milliseconds to wait between retry dial")
	simulateLatency := flag.Int("simulate_latency", 0, "simulating network latency")
	flag.Parse()

	// Log server properties
	log.Printf("Server started with the following properties:\n"+
		"hostname=%s\n"+
		"dkv_order port=%s\n"+
		"overlay port=%s\n"+
		"operation mode=%s\n"+
		"data port=%s\n"+
		"nodes to contact=[%s]\n", *hostname, *multicastPort, *overlayPort, *opMode,
		*dataPort, *nodesToContact)

	// Initialize new replica and log initialization
	newReplica := new(replica.Replica)
	newReplica.Init(*hostname, *multicastPort, *overlayPort, *dataPort, *hashFunction,
		*simulateLatency, *retryDial, *retryWait, *opMode)

	newReplica.Info()

	// Register RPC and start HTTP and multicast services
	rpc.HandleHTTP()
	readyChan := make(chan bool)
	go startHttp(newReplica) // Start HTTP server
	go startMulticast(newReplica, *opMode, readyChan)
	startRing(newReplica, *nodesToContact, readyChan) // Start the ring of nodes
}

// startRing initializes the node's ring for overlay communication
func startRing(newReplica *replica.Replica, contacts string, readyChan chan bool) {
	others := strings.Split(contacts, ",")

	// Registering RPC service for the ring
	ringRpc := new(overlay.RpcRing)
	ringRpc.Init(newReplica)
	err := rpc.Register(ringRpc)
	if err != nil {
		log.Fatal(newReplica.Node.Hostname, "cannot register rpc service: ", err)
	}

	// Start listening on the overlay port
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
			log.Println("Contacting node", host[0]+":"+host[1])
			var err error
			var client *rpc.Client
			// Retry connection to the node
			for j := 0; j < 3; j++ {
				client, err = rpc.Dial("tcp", host[0]+":"+host[1])
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				break // Connection successful, exit loop
			}
			if err != nil {
				log.Fatal("Dial error: cannot contacts the client after 3 retries", err)
			}
			log.Println("Dial successful")

			// Prepare arguments for joining the ring
			args := &overlay.JoinRingArgs{
				Hostname:      newReplica.Node.Hostname,
				OverlayPort:   newReplica.Node.OverlayPort,
				MulticastPort: newReplica.Node.MulticastPort,
				DataPort:      newReplica.Node.DataPort}

			var reply bool
			err = client.Call(overlay.JoinRingService, args, &reply)
			if err != nil {
				log.Println(newReplica.Node.Hostname, "cannot join ring: ", err)
			}
			err = client.Close()
			if err != nil {
				log.Println("Client close error:", err)
				return
			}
		}(others[i], &wg)

	}
	go func() {
		wg.Wait() // Wait for all join operations to complete
		for {     // ensure that all nodes has joined the network
			newReplica.Lock.RLock()
			if len(newReplica.Replicas) == (len(others) + 1) {
				readyChan <- true
				newReplica.Lock.RUnlock()
				return
			}
			newReplica.Lock.RUnlock()
		}
	}()

	log.Println(newReplica.Node.Hostname, "ring manager started at addr", listener.Addr().String())
	for {
		accept, err := listener.Accept()
		time.Sleep(newReplica.Latency) // Simulate latency
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go rpc.ServeConn(accept) // Serve the RPC connection
	}
}

// startMulticast initializes the multicast service for the replica
func startMulticast(newReplica *replica.Replica, opMode string, ready chan bool) {
	if <-ready {
		log.Println("Multicast service is ready")
	}
	newReplica.Info()
	var err error
	if opMode == utils.Sequential {
		service := new(totally_order.RpcSequentialMulticast)
		service.Init(newReplica)
		err = rpc.Register(service) // Register the multicast service
	} else if opMode == utils.Causal {
		service := new(causal_order.RpcCausalMulticast)
		service.Init(newReplica)
		err = rpc.Register(service)
	} else {
		log.Fatalf("Operation mode: %s not supported\n supported=<%s>", opMode,
			utils.Sequential+" | "+utils.Causal)
	}
	if err != nil {
		log.Fatal("Cannot register multicast service: ", err)
	}
	// Listen for multicast connections
	multicastListener, err := net.Listen("tcp", newReplica.Node.Hostname+":"+newReplica.Node.MulticastPort)
	defer func(multicastListener net.Listener) {
		err := multicastListener.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(multicastListener)
	if err != nil {
		log.Fatal(newReplica.Node.Hostname, "cannot listen on multicast port: ", newReplica.Node.MulticastPort, err)
	}
	log.Println(newReplica.Node.Hostname, "multicast manager started at addr", multicastListener.Addr().String())
	for {
		accept, err := multicastListener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go rpc.ServeConn(accept) // Serve the RPC connection
	}
}

// startHttp initializes the HTTP server for the replica
func startHttp(newReplica *replica.Replica) {
	httpHandler := new(data_http.Handler)
	httpHandler.Init(newReplica)
	httpHandler.StartListening()
}
