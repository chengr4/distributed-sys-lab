package tests

import (
	"bytes"
	"mini-rpc/pkg"
	"strings"
	"testing"
	"time"
)

// TestDualRoleSelfCall verifies that a single process can act as both
// an RPC server and a CLI client simultaneously (Dual Role).
func TestDualRoleSelfCall(t *testing.T) {
	port := "12345"

	// 1. Start the RPC Server in the background using the library
	go func() {
		if err := minirpc.StartServer(port); err != nil {
			t.Logf("Background server stopped: %v", err)
		}
	}()

	// Give the server a moment to bind to the port
	time.Sleep(200 * time.Millisecond)

	// 2. Simulate User Interaction via CLI
	input := "dial localhost:" + port + "\ngetTime\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := minirpc.NewCLI(in, out, &minirpc.RPCDialer{DefaultTimeout: 1 * time.Second})
	cli.Run()

	// 3. Verify the output
	output := out.String()

	if !strings.Contains(output, "Successfully connected") {
		t.Errorf("CLI failed to connect. Output:\n%s", output)
	}

	if !strings.Contains(output, "Server time") {
		t.Errorf("CLI failed to call getTime. Output:\n%s", output)
	}

	t.Logf("Dual Role Test Passed.")
}
