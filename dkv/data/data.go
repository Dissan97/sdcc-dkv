package data

import (
	"fmt"
	"log"
	"sync"
)

var defaultValue = Value{
	Timestamp: "Not exists",
	Val:       "",
}

type Value struct {
	Timestamp string
	Val       string
}

type Store struct {
	dataStore map[string]Value // [md5(key)]value
	Lock      sync.RWMutex
}

func (store *Store) Init() {
	store.dataStore = make(map[string]Value)
}

func (store *Store) Put(key string, value Value) {
	fmt.Println("put called")
	store.Lock.Lock()
	defer store.Lock.Unlock()

	store.dataStore[key] = value

	return
}

func (store *Store) Get(key string) Value {

	store.Lock.RLock()
	defer store.Lock.RUnlock()
	log.Println("DataStore.Get")
	if ret, ok := store.dataStore[key]; ok {
		log.Println("found key: ", key, "ret:", ret)
		return ret
	}
	log.Println("DataStore.Get: key not found", store.dataStore[key])
	return defaultValue
}

func (store *Store) Del(key string) Value {
	store.Lock.Lock()
	defer store.Lock.Unlock()
	if ret, ok := store.dataStore[key]; ok {
		delete(store.dataStore, key)
		return ret
	}

	return GetDefaultValue()
}

func (store *Store) GetMap() map[string]Value {
	return store.dataStore
}
func GetDefaultValue() Value {
	return defaultValue
}
