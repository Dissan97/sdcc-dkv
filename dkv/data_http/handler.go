package data_http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"net/rpc"
	"sdcc_dkv/data"
	"sdcc_dkv/dkv_order/totally_order"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
)

type Handler struct {
	router *mux.Router
	rep    *replica.Replica
}

type Response struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
}

type GetDelRequest struct {
	Key string `json:"key"`
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (handler *Handler) Init(rep *replica.Replica) {
	handler.router = mux.NewRouter()
	handler.rep = rep
	handler.router.HandleFunc("/api/datastore", handler.GetRequest).Methods("GET")
	handler.router.HandleFunc("/api/datastore", handler.PutRequest).Methods("PUT")
	handler.router.HandleFunc("/api/datastore", handler.DeleteRequest).Methods("DELETE")
}

func (handler *Handler) StartListening() {
	log.Printf("starting on http://%s:%s\n", handler.rep.Node.Hostname, handler.rep.Node.DataPort)
	if err := http.ListenAndServe(handler.rep.Node.Hostname+":"+handler.rep.Node.DataPort, handler.router); err != nil {
		log.Fatalf("error starting server on %s:%s error: %s\n", handler.rep.Node.Hostname,
			handler.rep.Node.DataPort, err)
	}
}

func (handler *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.router.ServeHTTP(w, r)
}

func (handler *Handler) jsonResponse(w http.ResponseWriter, status int, message string) {
	response := Response{
		Message: message,
		Status:  status,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}
}

func (handler *Handler) forwardGet(key, retMessage string) (string, bool) {
	curr := handler.rep

	// Get the index for the key and determine which nodes to contact
	nodeSize := len(curr.SortedKeys)
	nodesToContact := make([]string, nodeSize)
	_, index := curr.LookupNode(key)

	for i := 0; i < nodeSize; i++ {

		node := curr.Replicas[curr.SortedKeys[(index+i)%len(curr.SortedKeys)]]
		if curr.Node.Guid != node.Guid {
			nodesToContact[i] = node.Hostname + ":" + node.MulticastPort
		}
	}

	// Iterate through the nodes and send the GET request to each node
	for _, node := range nodesToContact {

		client, err := handler.rep.DialWithRetries(node)
		if err != nil {
			log.Printf("error connecting to node %s: %s\n", node, err)
			continue
		}
		var resultGet data.Value
		serviceMethod := fmt.Sprintf("Rpc%sMulticast.GetRequestByNodes", curr.OperationMode)
		err = client.Call(serviceMethod, &key, &resultGet)
		if err != nil {
			log.Printf("error calling rpc %s on node %s error: %s\n", serviceMethod, node, err)
			continue
		}

		err = client.Close()
		if err != nil {
			log.Printf("error closing rpc %s on node %s error: %s\n", serviceMethod, node, err)
		}

		if resultGet.Timestamp != data.GetDefaultValue().Timestamp && resultGet.Val != data.GetDefaultValue().Val {
			log.Printf("forwarder found on node %s with key: %s {timestamp=%s, value=%s}\n", key, node,
				resultGet.Timestamp, resultGet.Val)
			retMessage = fmt.Sprintf("{timestamp: %s, value:%s}", resultGet.Timestamp, resultGet.Val)
			return retMessage, true
		}

	}

	return retMessage, false
}

func (handler *Handler) GetRequest(w http.ResponseWriter, r *http.Request) {
	handler.rep.SimulateLatency()
	status := http.StatusNotFound
	log.Println("GET request received")

	// Extract key from URL query parameters
	key := r.URL.Query().Get("key")
	if key == "" {
		handler.jsonResponse(w, http.StatusBadRequest, "Key is missing in the GET request")
		return
	}

	log.Printf("GET request for key: %s", key)

	handler.rep.DataStore.Lock.RLock()
	defer handler.rep.DataStore.Lock.RUnlock()

	ret := handler.rep.DataStore.Get(key)
	responseMessage := fmt.Sprintf("{timestamp: %s, value:%s}", ret.Timestamp, ret.Val)
	log.Printf("GET response for key: %s on my storage %s", key, responseMessage)
	if ret.Timestamp == "Not exists" && ret.Val == "" {
		var exists bool
		if responseMessage, exists = handler.forwardGet(key, responseMessage); exists {
			log.Printf("GET request for key %s returned a value for %s", key, responseMessage)
			status = http.StatusOK
		}
	} else {
		status = http.StatusOK
	}
	log.Printf("return this to client: %s", responseMessage)
	handler.jsonResponse(w, status, responseMessage)
}

func (handler *Handler) PutRequest(w http.ResponseWriter, r *http.Request) {
	handler.rep.SimulateLatency()
	log.Println("PUT request received")

	// Decode the PUT request body to get key and value
	var putReq PutRequest
	err := json.NewDecoder(r.Body).Decode(&putReq)
	if err != nil {
		handler.jsonResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	log.Printf("PUT request for key: %s, value: %s", putReq.Key, putReq.Value)

	err = handler.contactRpcNode(putReq.Key, putReq.Value, "put")
	if err != nil {
		handler.jsonResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.jsonResponse(w, http.StatusOK, fmt.Sprintf("PUT request executed for key: %s, value: %s",
		putReq.Key, putReq.Value))
}

func (handler *Handler) DeleteRequest(w http.ResponseWriter, r *http.Request) {
	handler.rep.SimulateLatency()
	log.Println("DELETE request received")

	// Extract key from URL query parameters
	key := r.URL.Query().Get("key")
	if key == "" {
		handler.jsonResponse(w, http.StatusBadRequest, "Key is missing in the DELETE request")
		return
	}

	log.Printf("DELETE request for key: %s", key)

	err := handler.contactRpcNode(key, "", "delete")
	if err != nil {
		handler.jsonResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.jsonResponse(w, http.StatusOK, fmt.Sprintf("DELETE request executed for key: %s", key))
}

func (handler *Handler) contactRpcNode(key, value, operation string) error {
	vNode, _ := handler.rep.LookupNode(key)
	client, err := handler.rep.DialWithRetries(vNode.Hostname + ":" + vNode.MulticastPort)
	if err != nil {
		log.Printf("error contacting rpc node %s: %s", key, err)
		return err
	}
	callFunction := ""
	if handler.rep.OperationMode == utils.Sequential {
		callFunction = "RpcSequentialMulticast."
	} else if handler.rep.OperationMode == utils.Causal {
		callFunction = "RpcCausalMulticast."
	} else {
		return fmt.Errorf("invalid operation mode: %s", handler.rep.OperationMode)
	}
	callFunction += "Multicast"
	args := &totally_order.MessageUpdate{Key: key, Value: value, Operation: operation}
	var reply bool
	err = client.Call(callFunction, args, &reply)
	defer func(client *rpc.Client) {
		err = client.Close()
		if err != nil {
			log.Printf("error in closing client: %s", err)
		}
	}(client)
	if err != nil {
		log.Printf("error contacting rpc node %s: %s", key, err)
		return err
	}
	return err
}
