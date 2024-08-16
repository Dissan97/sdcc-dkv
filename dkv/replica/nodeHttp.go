package replica

import (
	"encoding/json"
	"fmt"
	"github.com/gosimple/slug"
	"log"
	"net/http"
	"strconv"
)

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func InternalServerErrorHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("500 Internal Server Error"))
}

func (replica *Replica) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		get(w, r, replica)
		return
	case http.MethodDelete:
		del(w, r, replica)
		return
	case http.MethodPut:
	case http.MethodPost:
		put(w, r, replica)
		return
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func put(w http.ResponseWriter, r *http.Request, replica *Replica) {
	var kv KeyValue

	// Decode the request body into the KeyValue struct
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w, r)
		return
	}

	// Convert the key to a uint64 using slug.Make
	key, err := strconv.ParseUint(slug.Make(kv.Key), 10, 64)
	if err != nil {
		log.Println("Error")
		InternalServerErrorHandler(w, r)
		return
	}

	// Convert the value using slug.Make
	value := slug.Make(kv.Value)
	fmt.Println("hello world", key, value)

	// Find the appropriate node for the key
	node, _ := replica.LookupNode(key)
	if node == nil {
		log.Println("VNode not found")
		InternalServerErrorHandler(w, r)
		return
	}

	//todo forward to another replica if needed (CHAT GPT DON'T WORRY ABOUT THIS NOW)

	//fmt.Println("my hostname:", replica.Node.Hostname, "my id:", replica.Node.Id)
	//fmt.Println("real hostname: ", node.Hostname, "real id", node.Id)

	// Store the key-value pair in the replica's data store
	replica.Data.Put(key, value)

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
		InternalServerErrorHandler(w, r)
		return
	}
}

func del(w http.ResponseWriter, r *http.Request, replica *Replica) {
	var kv KeyValue

	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w, r)
		return
	}

	fmt.Println("Get rec:", kv.Key, kv.Value)
	key, err := strconv.ParseUint(slug.Make(kv.Key), 10, 64)

	if err != nil {
		log.Println("error: ", err)
		InternalServerErrorHandler(w, r)
		return
	}

	value := replica.Data.Del(key)
	if value == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	response := KeyValue{
		Key:   kv.Key,
		Value: value,
	}

	// Set the content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// Encode the KeyValue struct as JSON and write it to the ResponseWriter
	if err := json.NewEncoder(w).Encode(response); err != nil {
		InternalServerErrorHandler(w, r)
		return
	}
}

func get(w http.ResponseWriter, r *http.Request, replica *Replica) {
	var kv KeyValue

	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		InternalServerErrorHandler(w, r)
		return
	}

	fmt.Println("Get rec:", kv.Key, kv.Value)
	key, err := strconv.ParseUint(slug.Make(kv.Key), 10, 64)

	if err != nil {
		log.Println("error: ", err)
		InternalServerErrorHandler(w, r)
		return
	}

	value := replica.Data.Get(key)
	if value == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	response := KeyValue{
		Key:   kv.Key,
		Value: value,
	}

	// Set the content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// Encode the KeyValue struct as JSON and write it to the ResponseWriter
	if err := json.NewEncoder(w).Encode(response); err != nil {
		InternalServerErrorHandler(w, r)
		return
	}
}

/*


 */

func NewMuxServer(replica *Replica) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/api/datastore", replica)
	mux.Handle("/api/datastore/", replica)
	return mux
}
