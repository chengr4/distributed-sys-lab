package main

import (
	"net/rpc"
	"testing"
	"time"
)

func TestChain(t *testing.T) {
	portA := "12351"
	portB := "12352"
	portC := "12353"

	go startServer(portA)
	go startServer(portB)
	go startServer(portC)

	// Wait for servers to start
	time.Sleep(200 * time.Millisecond)

	// main thread connects to A
	handleA, err := rpc.Dial("tcp", "localhost:"+portA)
	if err != nil {
		t.Fatalf("Failed to connect to server A: %v", err)
	}
	defer handleA.Close()

	var replyA SetNextNodeReply

	// use main node to  set A -> B
	err = handleA.Call("KVService.SetNextNode", &SetNextNodeArgs{NextNodeAddr: "localhost:" + portB}, &replyA)
	if err != nil || !replyA.Success {
		t.Fatalf("Failed to set next node: %v, %s", err, replyA.Message)
	}

	handleB, err := rpc.Dial("tcp", "localhost:"+portB)
	if err != nil {
		t.Fatalf("Failed to connect to server B: %v", err)
	}
	defer handleB.Close()

	var replyB SetNextNodeReply

	// use main node to set B -> C
	err = handleB.Call("KVService.SetNextNode", &SetNextNodeArgs{NextNodeAddr: "localhost:" + portC}, &replyB)
	if err != nil || !replyB.Success {
		t.Fatalf("Failed to set next node: %v, %s", err, replyB.Message)
	}

	// Chaining request
	// Client -> A -> B -> C (compute) -> B -> A -> Client
	var addReply AddReply
	addArgs := &AddArgs{Num1: 5, Num2: 7}

	err = handleA.Call("KVService.Add", addArgs, &addReply)
	if err != nil {
		t.Fatalf("Failed to call Add: %v", err)
	}

	expected := 12
	if addReply.Result != expected {
		t.Fatalf("Expected result %d, got %d", expected, addReply.Result)
	}

	t.Logf("Successfully got result (A -> B -> C): %d", addReply.Result)
}
