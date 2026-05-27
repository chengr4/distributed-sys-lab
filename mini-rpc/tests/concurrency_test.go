package tests

import (
	"fmt"
	"mini-rpc/pkg"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

func TestAddConcurrentCalls(t *testing.T) {
	port := "12381"
	go minirpc.StartServer(port)
	time.Sleep(200 * time.Millisecond)

	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	const numRequests = 50
	var wg sync.WaitGroup
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func(val int) {
			defer wg.Done()
			args := &minirpc.AddArgs{Num1: val, Num2: 1}
			reply := &minirpc.AddReply{}
			err := client.Call("KVService.Add", args, reply)
			if err != nil {
				t.Errorf("Concurrent call failed: %v", err)
			}
			if reply.Result != val+1 {
				t.Errorf("Result mismatch: expected %d, got %d", val+1, reply.Result)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Successfully completed %d concurrent requests", numRequests)
}

func TestStoreConcurrency(t *testing.T) {
	port := "12382"
	_, err := minirpc.StartServer(port)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	// Allow server to start
	time.Sleep(200 * time.Millisecond)

	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	const numItems = 100
	var wg sync.WaitGroup

	// 1. Concurrent Writes: Store numItems unique keys
	t.Logf("Starting %d concurrent writes...", numItems)
	wg.Add(numItems)
	for i := 0; i < numItems; i++ {
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", idx)
			val := fmt.Sprintf("val-%d", idx)
			args := &minirpc.StoreArgs{Name: key, Value: val}
			reply := &minirpc.StoreReply{}
			err := client.Call("KVService.Store", args, reply)
			if err != nil {
				t.Errorf("Store failed for %s: %v", key, err)
				return
			}
			if !reply.Success {
				t.Errorf("Store reply not success for %s: %s", key, reply.Message)
			}
		}(i)
	}
	wg.Wait()

	// 2. Concurrent Reads: Read numItems keys back
	t.Logf("Starting %d concurrent reads...", numItems)
	wg.Add(numItems)
	for i := 0; i < numItems; i++ {
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", idx)
			expectedVal := fmt.Sprintf("val-%d", idx)
			args := &minirpc.ReadArgs{Name: key}
			reply := &minirpc.ReadReply{}
			err := client.Call("KVService.Read", args, reply)
			if err != nil {
				t.Errorf("Read failed for %s: %v", key, err)
				return
			}
			if !reply.Success {
				t.Errorf("Read reply not success for %s: %s", key, reply.Message)
				return
			}
			if reply.Value != expectedVal {
				t.Errorf("Value mismatch for %s: expected %s, got %s", key, expectedVal, reply.Value)
			}
		}(i)
	}
	wg.Wait()

	// 3. Mixed Workload: Concurrent Store and Read on same keys
	t.Logf("Starting mixed workload...")
	const mixedOps = 200
	wg.Add(mixedOps)
	for i := 0; i < mixedOps; i++ {
		go func(idx int) {
			defer wg.Done()
			key := "shared-key"
			if idx%2 == 0 {
				// Write
				val := fmt.Sprintf("val-%d", idx)
				args := &minirpc.StoreArgs{Name: key, Value: val}
				reply := &minirpc.StoreReply{}
				client.Call("KVService.Store", args, reply)
			} else {
				// Read
				args := &minirpc.ReadArgs{Name: key}
				reply := &minirpc.ReadReply{}
				client.Call("KVService.Read", args, reply)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("Store Concurrency Integration Test Completed Successfully.")
}
