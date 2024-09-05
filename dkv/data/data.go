package data

import (
	"fmt"
	"sync"
)

type Value struct {
	Timestamp string
	Value     string
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

	if ret, ok := store.dataStore[key]; ok {
		return ret
	}

	return Value{}
}

func (store *Store) Del(key string) Value {
	store.Lock.Lock()
	defer store.Lock.Unlock()
	if ret, ok := store.dataStore[key]; ok {
		delete(store.dataStore, key)
		return ret
	}

	return Value{
		Timestamp: ""}
}

func (store *Store) GetMap() map[string]Value {
	return store.dataStore
}
