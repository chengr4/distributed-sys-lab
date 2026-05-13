package minirpc

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// MockRemoteRequester is a unified mock for RemoteRequester using function injection.
type MockRemoteRequester struct {
	DoCall func(serviceMethod string, args interface{}, reply interface{}) error
}

func (m *MockRemoteRequester) CallRemote(serviceMethod string, args interface{}, reply interface{}) error {
	if m.DoCall != nil {
		return m.DoCall(serviceMethod, args, reply)
	}
	return nil
}

func TestCLIBasicFlow(t *testing.T) {
	input := "hello\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out, &MockDialer{})
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

	// Create a dialer that always fails
	mockDialer := &MockDialerWithErr{Err: errors.New("connection refused")}
	cli := NewCLI(in, out, mockDialer)
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Dial failed") {
		t.Errorf("Expected dial failure message, got: %q", output)
	}
}

type MockDialerWithErr struct {
	Err error
}

func (m *MockDialerWithErr) Dial(addr string) (RemoteRequester, error) {
	return nil, m.Err
}

func TestCLIAddValidation(t *testing.T) {
	input := "add hello world\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}

	cli := NewCLI(in, out, &MockDialer{})
	cli.Run()

	output := out.String()
	if !strings.Contains(output, "Error: Both arguments must be integers") {
		t.Errorf("Expected validation error for non-integer inputs, got: %q", output)
	}
}

func TestCLINoConnectionMessage(t *testing.T) {
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
		cli := NewCLI(in, out, &MockDialer{})
		cli.Run()

		output := out.String()
		// Now it should show the RPC adapter's error instead of a manual check
		if !strings.Contains(output, "RPC client is not initialized") {
			t.Errorf("Command %q should show connection error, got: %q", cmd, output)
		}
	}
}

func TestCLIStoreSentence(t *testing.T) {
	input := "store mykey this is a long sentence\nexit\n"
	in := strings.NewReader(input)
	out := &bytes.Buffer{}
	var capturedValue string

	mock := &MockRemoteRequester{
		DoCall: func(method string, args interface{}, reply interface{}) error {
			if method == "KVService.Store" {
				storeArgs := args.(*StoreArgs)
				capturedValue = storeArgs.Value
				r := reply.(*StoreReply)
				r.Message = "Success"
			}
			return nil
		},
	}

	cli := NewCLI(in, out, &MockDialer{})
	cli.remoteRequester = mock
	cli.Run()

	expected := "this is a long sentence"
	if capturedValue != expected {
		t.Errorf("Expected captured value to be %q, got %q", expected, capturedValue)
	}
}

func TestCLIRemoteTimeout(t *testing.T) {
	in := strings.NewReader("add 1 2\nexit\n")
	out := &bytes.Buffer{}
	mock := &MockRemoteRequester{
		DoCall: func(method string, args interface{}, reply interface{}) error {
			return errors.New("RPC call timed out")
		},
	}

	cli := NewCLI(in, out, &MockDialer{})
	cli.remoteRequester = mock
	cli.Run()

	if !strings.Contains(out.String(), "RPC call timed out") {
		t.Errorf("Expected timeout message, got: %q", out.String())
	}
}
