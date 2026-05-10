package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestDualRoleSelfCall verifies that a single process can act as both
// an RPC server and a CLI client simultaneously (Dual Role).
func TestDualRoleSelfCall(t *testing.T) {
	// Use a specific port for integration test to avoid conflicts
	port := "12345"

	// 1. Start the RPC Server in the background
	// We use a goroutine to simulate the background server behavior
	go func() {
		// Note: startServer contains an infinite loop
		if err := startServer(port); err != nil {
			// If it fails immediately (e.g., port in use), we log it
			t.Logf("Background server stopped: %v", err)
		}
	}()

	// Give the server a moment to bind to the port
	time.Sleep(200 * time.Millisecond)

	// 2. Simulate User Interaction via CLI
	// We simulate the following scenario:
	// - Dial the local server
	// - Call getTime to verify connectivity
	// - Exit
	input := "dial localhost:" + port + "\ngetTime\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	// Initialize CLI with injected mock input/output
	cli := NewCLI(in, out)
	cli.Run()

	// 3. Verify the output
	output := out.String()
	
	// Check if Dial was successful
	if !strings.Contains(output, "Successfully connected") {
		t.Errorf("CLI failed to connect to its own server. Output:\n%s", output)
	}

	// Check if RPC call 'getTime' returned a result
	if !strings.Contains(output, "Server time") {
		t.Errorf("CLI failed to perform RPC call to its own server. Output:\n%s", output)
	}

	if !strings.Contains(output, "Goodbye!") {
		t.Errorf("CLI failed to exit gracefully. Output:\n%s", output)
	}

	t.Logf("Dual Role Test Passed. Combined Output:\n%s", output)
}
