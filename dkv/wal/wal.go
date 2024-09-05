package wal

import (
	"log"
	"os"
	"sync"
	"time"
)

type StateWal struct {
	Operation string
	Key       string
	Value     string
	Timestamp string
}

type AppendOnlyQueue struct {
	StateWalQueue []StateWal
	Lock          sync.Mutex
	LogFile       *os.File
}

func (queue *AppendOnlyQueue) Init(hostname string) {
	queue.StateWalQueue = make([]StateWal, 0)
	var err error
	queue.LogFile, err = os.Create("log/" + hostname + "-WalLog.log")
	if err != nil {
		log.Fatal("cannot create append only log", err)
	}
}
func (queue *AppendOnlyQueue) daemonWork() {
	queue.Lock.Lock()
	size := len(queue.StateWalQueue)
	for i := 0; i < size; i++ {
		_, err := queue.LogFile.Write([]byte(queue.StateWalQueue[0].Operation + "," + queue.StateWalQueue[0].Value))
		if err != nil {
			log.Println("write error", err)
		}
		queue.StateWalQueue = queue.StateWalQueue[1:]
	}
	queue.Lock.Unlock()
	err := queue.LogFile.Sync()
	if err != nil {
		log.Println(" file sync error", err)
	}
}

func (queue *AppendOnlyQueue) daemon(done chan bool) {
	for {
		queue.daemonWork()
		select {
		case <-done:
			return
		default:
			time.Sleep(5 * time.Second)
		}

	}
}

// AppendOperation called for put and delete request
func (queue *AppendOnlyQueue) AppendOperation(wal StateWal) {
	queue.Lock.Lock()
	defer queue.Lock.Unlock()
	queue.StateWalQueue = append(queue.StateWalQueue, wal)
}

func (queue *AppendOnlyQueue) Start() chan bool {
	done := make(chan bool)
	go queue.daemon(done)
	return done
}
