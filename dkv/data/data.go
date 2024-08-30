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
	Mu        sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		dataStore: make(map[string]Value),
	}
}

func (s *Store) Put(key string, value Value) {
	fmt.Println("put called")
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.dataStore[key] = value

	return
}

func (s *Store) Get(key string) Value {

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if ret, ok := s.dataStore[key]; ok {
		return ret
	}

	return Value{}
}

func (s *Store) Del(key string) Value {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if ret, ok := s.dataStore[key]; ok {
		delete(s.dataStore, key)
		return ret
	}

	return Value{
		Timestamp: ""}
}
