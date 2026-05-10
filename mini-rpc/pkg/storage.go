package minirpc

import "sync"

// infra
type Storage struct {
	rwMu sync.RWMutex
	data map[string]string
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[string]string),
	}
}

func (s *Storage) Set(key string, value string) {
	s.rwMu.Lock()
	// unlock when the method returns
	defer s.rwMu.Unlock()
	s.data[key] = value
}

func (s *Storage) Get(key string) (string, bool) {
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}
