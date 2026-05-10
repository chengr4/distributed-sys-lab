package minirpc

import (
	"sync"
	"testing"
)

func TestStorageBasic(t *testing.T) {
	s := NewStorage()
	s.Set("k1", "v1")
	val, ok := s.Get("k1")
	if !ok || val != "v1" {
		t.Errorf("Basic read/write failed: got %s", val)
	}
}

func TestStorageConcurrency(t *testing.T) {
	s := NewStorage()
	var wg sync.WaitGroup

	for i := range 100 {
		// add one the wg counter (add one task)
		wg.Add(1)
		// A concurrent task
		go func(currI int) {
			// wg counter -= 1
			defer wg.Done()
			s.Set("key", "value")
		}(i)
	}

	for range 100 {
		wg.Go(func() {
			s.Get("key")
		})
	}

	// main goroutine wait here until the counter == 0
	wg.Wait()
}
