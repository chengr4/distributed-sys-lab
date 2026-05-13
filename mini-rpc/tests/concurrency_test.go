package tests

import (
	"mini-rpc/pkg"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCalls(t *testing.T) {
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

	for i := range numRequests {
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
