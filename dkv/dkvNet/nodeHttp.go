package dkvNet

import (
	"encoding/json"
	"fmt"
	"github.com/gosimple/slug"
	"log"
	"net/http"
	"net/rpc"
	"sdcc_dkv/data"
	"sdcc_dkv/multicast"
	"sdcc_dkv/replica"
	"sdcc_dkv/utils"
	"strconv"
)

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func InternalServerErrorHandler(w http.ResponseWriter) {
	w.WriteHeader(http.StatusInternalServerError)
	_, err := w.Write([]byte("500 Internal Server Error"))
	if err != nil {
		return
	}
}

func (handler *ReplicaHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		get(w, r, handler.Rep)
		return
	case http.MethodDelete:
		remove(w, r, handler.Rep)
		return
	case http.MethodPut:
	case http.MethodPost:
		put(w, r, handler.Rep)
		return
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func put(w http.ResponseWriter, r *http.Request, rep *replica.Replica) {
	var kv KeyValue

	// Decode the request body into the KeyValue struct
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w)
		return
	}

	// Convert the key to an uint64 using slug.Make
	key := slug.Make(kv.Key)

	// Convert the value using slug.Make
	value := slug.Make(kv.Value)
	fmt.Println("hello world", key, value)

	// Find the appropriate node for the key
	// if node != me forward
	node, index := rep.LookupNode(utils.HashKey(key))
	if node == nil {
		log.Println("VNode not found")
		InternalServerErrorHandler(w)
		return
	}
	var formalValue data.Value
	if rep.Node.OpMode == utils.Sequential {
		vNodes := make([]*replica.VNode, rep.ReplicationFactor)
		for i := 0; uint(i) < rep.ReplicationFactor; i++ {
			vNodes[i] = rep.Replicas[string(rep.SortedKeys[(index+i)%len(rep.SortedKeys)])]
		}
		old := node.SC.Value()
		node.SC.Update(old + 1)

		formalValue = data.Value{
			Timestamp: strconv.FormatUint(old, 10),
			Value:     value,
		}

		seqMsg := multicast.TotallyOrderedMessage{
			Key:       key,
			Value:     formalValue,
			Timestamp: old + 1,
			Id:        node.Guid,
			Ack:       1,
			VNodes:    vNodes,
		}

		var reply multicast.TotallyOrderedMessage

		client, err := rpc.DialHTTP("tcp", node.Hostname+":"+node.ControlPort)

		if err != nil {
			log.Fatal("rpc.DialHttp to "+node.Hostname+":"+node.ControlPort+" error=", err)
		}
		//todo handle error
		err = client.Call(multicast.RpcMulticastUpdate, &seqMsg, &reply)
		if err != nil {
			log.Println(node.Hostname+":"+node.ControlPort+"clock.RpcMulticastUpdate error=", err)
			InternalServerErrorHandler(w)
			return
		}

	}

	rep.Data.Put(key, formalValue)

	// Prepare the response
	response := KeyValue{
		Key:   kv.Key,
		Value: kv.Value,
	}

	// Set the Content-Type header to application/json
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Set status code to 201 Created

	// Encode the response as JSON and write it to the ResponseWriter
	if err := json.NewEncoder(w).Encode(response); err != nil {
		InternalServerErrorHandler(w)
		return
	}
}

func remove(w http.ResponseWriter, r *http.Request, replica *replica.Replica) {
	var kv KeyValue

	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w)
		return
	}

	fmt.Println("Get rec:", kv.Key, kv.Value)
	key := slug.Make(kv.Key)

	val := replica.Data.Del(key)
	if val.Timestamp == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	response := KeyValue{
		Key:   kv.Key,
		Value: val.Timestamp + ":" + val.Value,
	}

	// Set the content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// Encode the KeyValue struct as JSON and write it to the ResponseWriter
	if err := json.NewEncoder(w).Encode(response); err != nil {
		InternalServerErrorHandler(w)
		return
	}
}

func get(w http.ResponseWriter, r *http.Request, replica *replica.Replica) {
	var kv KeyValue

	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w)
		return
	}

	fmt.Println("Get rec:", kv.Key, kv.Value)
	key := slug.Make(kv.Key)

	value := replica.Data.Get(key)
	if value.Timestamp == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	response := KeyValue{
		Key:   kv.Key,
		Value: value.Timestamp + ":" + value.Value,
	}

	// Set the content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// Encode the KeyValue struct as JSON and write it to the ResponseWriter
	if err := json.NewEncoder(w).Encode(response); err != nil {
		InternalServerErrorHandler(w)
		return
	}
}

type ReplicaHttpHandler struct {
	Rep *replica.Replica
}

func NewMuxServer(replica *replica.Replica) *http.ServeMux {
	handler := new(ReplicaHttpHandler)
	handler.Rep = replica
	mux := http.NewServeMux()
	mux.Handle("/api/datastore", handler)
	mux.Handle("/api/datastore/", handler)
	return mux
}
