package tests

import (
	"net/rpc"
	"testing"
	"time"
	"mini-rpc/pkg"
)

func TestChain(t *testing.T) {
	portA := "12351"
	portB := "12352"
	portC := "12353"

	// Now we can call StartServer because it's in the minirpc package!
	go minirpc.StartServer(portA)
	go minirpc.StartServer(portB)
	go minirpc.StartServer(portC)

	// Wait for servers to start
	time.Sleep(200 * time.Millisecond)

	// main thread connects to A
	handleA, err := rpc.Dial("tcp", "localhost:"+portA)
	if err != nil {
		t.Fatalf("Failed to connect to server A: %v", err)
	}
	defer handleA.Close()

	var replyA minirpc.SetNextNodeReply

	// use main node to set A -> B
	err = handleA.Call("KVService.SetNextNode", &minirpc.SetNextNodeArgs{NextNodeAddr: "localhost:" + portB}, &replyA)
	if err != nil || !replyA.Success {
		t.Fatalf("Failed to set next node for A: %v, %s", err, replyA.Message)
	}

	// use main node to set B -> C
	handleB, err := rpc.Dial("tcp", "localhost:"+portB)
	if err != nil {
		t.Fatalf("Failed to connect to server B: %v", err)
	}
	defer handleB.Close()

	var replyB minirpc.SetNextNodeReply
	err = handleB.Call("KVService.SetNextNode", &minirpc.SetNextNodeArgs{NextNodeAddr: "localhost:" + portC}, &replyB)
	if err != nil || !replyB.Success {
		t.Fatalf("Failed to set next node for B: %v, %s", err, replyB.Message)
	}

	// Chaining request: Client -> A -> B -> C (compute) -> B -> A -> Client
	var addReply minirpc.AddReply
	addArgs := &minirpc.AddArgs{Num1: 5, Num2: 7}

	err = handleA.Call("KVService.Add", addArgs, &addReply)
	if err != nil {
		t.Fatalf("Chain Add call failed: %v", err)
	}

	expected := 12
	if addReply.Result != expected {
		t.Fatalf("Chain result mismatch: expected %d, got %d", expected, addReply.Result)
	}

	t.Logf("Successfully got result (A -> B -> C): %d", addReply.Result)
}
