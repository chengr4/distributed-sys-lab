package minirpc

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCLIBasicFlow(t *testing.T) {
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

func TestCLIAddValidation(t *testing.T) {
	input := "add hello world\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out)
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Error: Both arguments must be integers") {
		t.Errorf("Expected validation error for non-integer inputs, got: %q", output)
	}
}

func TestCLINoConnectionWarning(t *testing.T) {
	commands := []string{
		"getTime",
		"add 1 2",
		"read key",
		"store k v",
		"setNextNode localhost:8001",
	}

	for _, cmd := range commands {
		input := cmd + "\nexit\n"
		in := strings.NewReader(input)
		out := &bytes.Buffer{}
		cli := NewCLI(in, out)
		cli.Run()

		output := out.String()
		if !strings.Contains(output, "Please execute 'dial'") {
			t.Errorf("Command %q should show connection warning, got: %q", cmd, output)
		}
	}
}

type MockTimeoutRequester struct{}

func (m *MockTimeoutRequester) CallRemote(serviceMethod string, args interface{}, reply interface{}) error {
	return errors.New("RPC call timed out")
}

func TestCLIRemoteTimeout(t *testing.T) {
	in := strings.NewReader("add 1 2\nexit\n")
	out := &bytes.Buffer{}
	mock := &MockTimeoutRequester{}

	cli := NewCLI(in, out)
	cli.remoteRequester = mock
	cli.Run()

	if !strings.Contains(out.String(), "RPC call timed out") {
		t.Errorf("Expected timeout message, got: %q", out.String())
	}
}
