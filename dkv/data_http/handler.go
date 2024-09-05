package data_http

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sdcc_dkv/replica"
)

type Handler struct {
	router *mux.Router
	rep    *replica.Replica
}

type Response struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
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

func (handler *Handler) GetRequest(w http.ResponseWriter, r *http.Request) {
	handler.jsonResponse(w, http.StatusOK, "GET request received")
}

func (handler *Handler) PutRequest(w http.ResponseWriter, r *http.Request) {
	handler.jsonResponse(w, http.StatusOK, "PUT request received")
}

func (handler *Handler) DeleteRequest(w http.ResponseWriter, r *http.Request) {
	handler.jsonResponse(w, http.StatusOK, "DELETE request received")
}
