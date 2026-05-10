package minirpc

import (
	"bytes"
	"strings"
	"testing"
)

func TestCLIBasicFlow(t *testing.T) {
	// Test case: Input an unknown command, then exit
	input := "hello\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out)
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Unknown command") {
		t.Errorf("Expected output to contain 'Unknown command', got: %q", output)
	}
	if !strings.Contains(output, "Goodbye!") {
		t.Errorf("Expected output to contain 'Goodbye!', got: %q", output)
	}
}

func TestCLIDialFailure(t *testing.T) {
	// Test case: Attempt to connect to a non-existent address
	input := "dial localhost:9999\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out)
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Dial failed") {
		t.Errorf("Expected dial failure message, got: %q", output)
	}
}

func TestCLINoConnectionWarning(t *testing.T) {
	// Test case: Attempt to call getTime without a connection
	input := "getTime\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out)
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Please execute 'dial'") {
		t.Errorf("Expected connection prompt message, got: %q", output)
	}
}
