package tests

import (
	"mini-rpc/pkg"
	"net"
	"net/rpc"
	"testing"
	"time"
)

func TestForwardingTimeout(t *testing.T) {
	portA := "12361"
	portBlackHole := "12362"

	go minirpc.StartServer(portA)

	// Start a black hole server that accepts connections but does nothing
	// no listener.Accept() here
	ln, err := net.Listen("tcp", ":"+portBlackHole)
	if err != nil {
		t.Fatalf("Failed to start black hole server: %v", err)
	}
	defer ln.Close()

	time.Sleep(200 * time.Millisecond)

	argsNext := minirpc.SetNextNodeArgs{NextNodeAddr: "localhost:" + portBlackHole}
	var replyNext minirpc.SetNextNodeReply
	remotehandleA, err := rpc.Dial("tcp", "localhost:"+portA)
	err = remotehandleA.Call("KVService.SetNextNode", &argsNext, &replyNext)
	if err != nil || !replyNext.Success {
		t.Fatalf("Failed to set next node: %v, reply: %v", err, replyNext)
	}

	startNow := time.Now()
	var addReply minirpc.AddReply
	err = remotehandleA.Call("KVService.Add", &minirpc.AddArgs{Num1: 1, Num2: 2}, &addReply)
	duration := time.Since(startNow)
	
	// Verification
	if err == nil {
		t.Fatalf("[FAILURE] Expected timeout error, but call succeeded with result: %v", addReply)
	}

	// We expect the error to be a timeout error
	t.Logf("[SUCCESS] Correctly captured expected timeout error: %v", err)
	t.Logf("[STATS] Request duration: %v", duration)

	// Validate the duration is within an expected window (3s threshold + some buffer)
	if duration < 3*time.Second {
		t.Errorf("[FAILURE] Timed out too early: %v (expected >= 3s)", duration)
	}
	if duration > 5*time.Second {
		t.Errorf("[FAILURE] Timed out too late: %v (expected <= 5s)", duration)
	}
}
