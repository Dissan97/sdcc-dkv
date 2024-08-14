package data

import (
	"fmt"
	"sync"
)

type Store struct {
	dataStore map[uint64]string // [md5(key)]value
	Mu        sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		dataStore: make(map[uint64]string),
	}
}

func (s *Store) Put(key uint64, value string) {
	fmt.Println("put called")
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.dataStore[key] = value

	return
}

func (s *Store) Get(key uint64) string {

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if ret, ok := s.dataStore[key]; ok {
		return ret
	}

	return ""
}

func (s *Store) Del(key uint64) string {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if ret, ok := s.dataStore[key]; ok {
		delete(s.dataStore, key)
		return ret
	}

	return ""
}
