package main

import (
	"net/rpc"
	"testing"
	"time"
	"mini-rpc/pkg"
)

func TestServerStartup(t *testing.T) {
	// start the server in the background
	go main()

	// put current groutine to sleep for a short time to allow the server to start up
	time.Sleep(500 * time.Millisecond)

	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	args := &minirpc.GetTimeArgs{}
	reply := &minirpc.GetTimeReply{}

	err = client.Call("KVService.GetTime", args, reply)
	if err != nil {
		t.Errorf("Failed to call RPC: %v", err)
	}
	if !reply.Success {
		t.Error("Expected success, got failure")
	}
}
